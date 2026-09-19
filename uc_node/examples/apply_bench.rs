// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `apply_bench` — the FSM-side hop in isolation (M14a's lag barrier).
//!
//! Hop-isolation harness (CLAUDE.md "Finding a performance bottleneck"): a
//! FAKE node — the cnc page, `log.buf` and the per-id rings, leader flags set
//! — plus a driver thread that appends frames through `uc_log::Appender` and
//! plays archive + consensus (`durable = commit = append`, published every
//! `--batch` frames), paced so `append − min(applied) ≤ --window`; N
//! `uc_service` attaches apply a raw-tier counting state machine. No sink:
//! the egress broadcast never blocks its producer. The number is applied
//! frames/s per FSM — the apply hop alone, with the M14a barrier in the loop.
//!
//! SMOKE on a dev box (`docs/notes/dev-box-not-a-bench.md`): compare ladders
//! and ratios (N=1 vs N=2/4/8, bounded vs lockstep, this tree vs `main`), never
//! absolutes against a bar. Never point `--root` at `/tmp` (RAM-backed).
//!
//! ```text
//! cargo run -p uc_node --release --example apply_bench -- --root /home/claude/apply-bench --fsms 2 --mode bounded --secs 6
//! cargo run -p uc_node --release --example apply_bench -- --root /home/claude/apply-bench --fsms 2 --mode lockstep --secs 6
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use kv_store::KvSm;
use kv_store::bench::{KvBTree, KvHash};
use kv_store::wire as kv_wire;
use uc_log::buffer::{AppendError, Appender, LogBuffer};
use uc_log::cnc::{CncMeta, CncPage, unpack_service_status};
use uc_node::ServicesConfig;
use uc_protocol::ring::{BroadcastRing, SpscRing};
use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER};
use uc_protocol::v2::frame::{HEADER_LEN, align_frame_len};
use uc_service::{ApplyCtx, RawStateMachine, ServiceBuilder, ServiceConfig};

const APP: &str = "apply-bench";
const MIB: u64 = 1 << 20;
const MAX_MSG: u32 = 64 << 10;
const MAX_PAYLOAD: u32 = 256;

#[derive(Parser)]
#[command(about = "Isolated apply-hop bench: N FSMs tail one fake node's log buffer")]
struct Args {
    /// Instance dir root (wiped each run). Real disk, never /tmp.
    #[arg(long)]
    root: PathBuf,
    /// How many FSMs attach (ids 0..N), all declared.
    #[arg(long, default_value_t = 1)]
    fsms: u8,
    /// `bounded` or `lockstep`.
    #[arg(long, default_value = "bounded")]
    mode: String,
    /// Bounded lag in bytes (default buffer/4, the node's default).
    #[arg(long)]
    lag: Option<u64>,
    #[arg(long, default_value_t = 6)]
    secs: u64,
    #[arg(long, default_value_t = 1)]
    warmup_secs: u64,
    /// Command payload bytes (≤ 256).
    #[arg(long, default_value_t = 64)]
    payload: usize,
    /// Log buffer size in MiB (power of two).
    #[arg(long, default_value_t = 64)]
    buffer_mib: u64,
    /// Frames appended between two durable/commit publishes (the fake archive's block).
    #[arg(long, default_value_t = 64)]
    batch: u64,
    /// Driver pacing: never let `append - min(applied)` exceed this (default buffer/2).
    #[arg(long)]
    window: Option<u64>,
    /// Which state machine every row runs: `raw` (the counting baseline),
    /// `kv` (`examples/kv`'s `KvSm` on `Arc<BTreeMap>`), `kv-btree` or
    /// `kv-hash` (the same PUT path over `std` maps). The kv arms are fed
    /// real `PUT` frames whose key cycles over `--keys` distinct keys.
    #[arg(long, default_value = "raw")]
    sm: String,
    /// Distinct keys the kv arms cycle over (8-byte keys, `n % keys`).
    #[arg(long, default_value_t = 100_000)]
    keys: u64,
}

/// Bytes of a kv `PUT` frame that are not the value: format byte, op byte,
/// `key_len: u16`, then the 8-byte key the driver patches per frame.
const KV_PUT_HEAD: usize = 4 + KV_KEY_LEN;
const KV_KEY_LEN: usize = 8;

/// The `PUT` frame template the driver patches: key bytes at
/// `KV_PUT_HEAD - KV_KEY_LEN..KV_PUT_HEAD`, the value fills the rest of
/// `payload_len`. Encoded by `examples/kv`'s own encoder so the bytes are
/// exactly what a client sends.
fn kv_put_template(payload_len: usize) -> Vec<u8> {
    debug_assert!(payload_len >= KV_PUT_HEAD, "main() guards --payload first");
    let value = vec![0x42u8; payload_len - KV_PUT_HEAD];
    let f = kv_wire::encode_put(&[0u8; KV_KEY_LEN], &value);
    debug_assert_eq!(f.len(), payload_len);
    f
}

#[inline]
fn kv_patch_key(frame: &mut [u8], n: u64, keys: u64) {
    frame[KV_PUT_HEAD - KV_KEY_LEN..KV_PUT_HEAD].copy_from_slice(&(n % keys).to_le_bytes());
}

/// Raw-tier counter: no decode, no allocation — the cheapest legal SM, so the
/// hop's own cost (the barrier, the loop, the egress publish) is what shows.
///
/// The frame counter lives in the `TaggedRaw` wrapper since the `--sm` arms
/// landed (every arm needs one), so this SM only stamps the position and
/// publishes an 8-byte response: per frame that is the same three operations
/// the pre-`--sm` `RawCount` did (count, stamp, publish), split across the
/// two structs rather than doubled.
struct RawCount {
    last: Option<u64>,
}

impl RawStateMachine for RawCount {
    const NAME: &'static str = "raw";

    fn apply(&mut self, ctx: &mut ApplyCtx, _cmd: &[u8], out: &mut Vec<u8>) {
        self.last = Some(ctx.position);
        out.extend_from_slice(&ctx.position.to_le_bytes());
    }
    fn query(&self, _q: &[u8], out: &mut Vec<u8>) {
        out.extend_from_slice(&self.last.unwrap_or(0).to_le_bytes());
    }
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
}

/// FSM identity: attach finds a service's row by name now, so N instances of
/// the same `RawCount` logic each need a DISTINCT declared name to occupy N
/// distinct rows. `RawCount` is raw-tier (`RawStateMachine` directly, not
/// `StateMachine`), so `uc_service::Tagged` — which only forwards the typed
/// tier — cannot wrap it; this is that same forwarding shape, hand-written
/// for the raw tier, local to this bench (spec §3.3 "one type, one row" is
/// for a real service; a hop-isolation harness legitimately runs one type at
/// every row).
///
/// Generic over the inner SM since the `--sm` arms landed; the wrapper
/// counts frames itself so the sweep check below works for every arm
/// (`KvSm` answers an empty query with a decode error, not a count).
struct TaggedRaw<const ROW: u8, S: RawStateMachine> {
    inner: S,
    frames: u64,
}
impl<const ROW: u8, S: RawStateMachine> RawStateMachine for TaggedRaw<ROW, S> {
    const NAME: &'static str = uc_service::tagged::TAGGED_NAMES[ROW as usize];
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        self.frames += 1;
        self.inner.apply(ctx, cmd, out)
    }
    /// An empty query answers the wrapper's own frame count; anything else
    /// is the inner SM's (the kv arms' `DIGEST`, which the end-of-run
    /// content check uses).
    fn query(&self, q: &[u8], out: &mut Vec<u8>) {
        if q.is_empty() {
            out.extend_from_slice(&self.frames.to_le_bytes());
        } else {
            self.inner.query(q, out)
        }
    }
    fn last_applied(&self) -> Option<u64> {
        self.inner.last_applied()
    }
}

/// Object-safe view over a `Service<TaggedRaw<ROW>>` for an arbitrary ROW —
/// the const generic can't be a runtime value, so the N spawned services
/// (one per row, `ROW` fixed by a `match` at spawn time) are stored behind
/// this trait rather than in a homogeneously-typed `Vec`.
trait BenchService {
    fn query_frames(&self) -> u64;
    /// The kv arms' `DIGEST` answer: `(len, digest)`, or `None` if the inner
    /// SM did not answer `ST_OK` (the raw arm has no such query).
    fn query_kv(&self) -> Option<(u64, u64)>;
    fn stop(self: Box<Self>);
}
impl<const ROW: u8, S: RawStateMachine> BenchService for uc_service::Service<TaggedRaw<ROW, S>> {
    fn query_frames(&self) -> u64 {
        let mut out = Vec::new();
        self.query_raw(&[], &mut out);
        u64::from_le_bytes(out[..8].try_into().unwrap())
    }
    fn query_kv(&self) -> Option<(u64, u64)> {
        let mut out = Vec::new();
        self.query_raw(&kv_wire::encode_digest(), &mut out);
        if out.len() < 25 || out[0] != kv_wire::ST_OK {
            return None;
        }
        let len = u64::from_le_bytes(out[1..9].try_into().unwrap());
        let digest = u64::from_le_bytes(out[9..17].try_into().unwrap());
        Some((len, digest))
    }
    fn stop(self: Box<Self>) {
        (*self).stop();
    }
}

fn unix_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// The machine-readable result line `scripts/apply_ab.sh` parses.
///
/// Extracted from `main` so the runner's parser and this printer cannot
/// drift: `apply_json_line_shape_is_pinned` below pins the exact bytes, and
/// the script's `--selftest` stubs print that same shape. `min_rate` — the
/// slowest FSM's applied frames/s — is the hop number the A/B compares;
/// `pace_stalls` (Ruling Q9) is the driver-is-the-limiter guard: the driver
/// spins on `min_applied` only when it is running ahead of the FSMs, so a
/// driver that never stalls IS the limiter and the run's `min_rate` is
/// measuring the driver, not the apply hop; the rest is provenance for the
/// run.
#[allow(clippy::too_many_arguments)]
fn render_apply_json(
    fsms: u8,
    mode: &str,
    lag: u64,
    payload: usize,
    frame: u64,
    secs: f64,
    min_rate: f64,
    driver_rate: f64,
    pace_stalls: u64,
    per: &[(f64, u64)],
) -> String {
    let per_json: Vec<String> = per
        .iter()
        .enumerate()
        .map(|(i, p)| format!("{{\"fsm\":{i},\"rate\":{:.0},\"lag_waits\":{}}}", p.0, p.1))
        .collect();
    format!(
        "APPLY-JSON {{\"fsms\":{fsms},\"mode\":\"{mode}\",\"lag\":{lag},\"payload\":{payload},\"frame\":{frame},\"secs\":{secs:.2},\"min_rate\":{min_rate:.0},\"driver_rate\":{driver_rate:.0},\"pace_stalls\":{pace_stalls},\"per\":[{}]}}",
        per_json.join(",")
    )
}

fn min_applied(cnc: &CncPage, fsms: u8) -> u64 {
    (0..fsms)
        .map(|id| cnc.service_slot(id as usize).applied.load_acquire())
        .min()
        .unwrap_or(0)
}

fn main() -> anyhow::Result<()> {
    let a = Args::parse();
    anyhow::ensure!(
        !a.root.starts_with("/tmp"),
        "--root must not be under /tmp (RAM-backed, no swap)"
    );
    anyhow::ensure!((1..=8).contains(&a.fsms), "--fsms must be 1..=8");
    anyhow::ensure!(
        a.payload as u32 <= MAX_PAYLOAD,
        "--payload must be <= {MAX_PAYLOAD}"
    );
    let lockstep = match a.mode.as_str() {
        "bounded" => false,
        "lockstep" => true,
        m => anyhow::bail!("--mode must be bounded|lockstep, got {m}"),
    };
    let buffer_bytes = a.buffer_mib * MIB;
    let lag = if lockstep {
        0
    } else {
        a.lag.unwrap_or(buffer_bytes / 4)
    };
    let window = a.window.unwrap_or(buffer_bytes / 2);
    let frame = align_frame_len(HEADER_LEN + a.payload) as u64;

    // ---- the fake node ----
    // Take a real `InstanceDir` (it creates `journal/` and `state/` and takes
    // `instance.lock`, which no service ever touches — a service locks only
    // `service.<id>.lock`) so every IPC path below comes from the SAME
    // accessors the real node uses. That is what keeps this harness from
    // drifting off the layout again: `svc_sched.<row>.ring` was added to
    // `uc_service::attach` and missed here, and the bench died on every start
    // until 2026-09-07.
    let _ = std::fs::remove_dir_all(&a.root);
    let dir = uc_node::InstanceDir::acquire(&a.root)?;
    let meta = CncMeta {
        node_id: 0,
        instance_id: rand::random::<u128>(),
        app_id: APP.into(),
        buffer_bytes,
        max_payload: MAX_PAYLOAD,
        // FSM identity: declare `fsm0..fsm<N-1>` (the `Tagged`/`TaggedRaw`
        // naming convention) so each of the N `TaggedRaw<ROW>` instances
        // below finds its row by name.
        services: ServicesConfig::tagged(a.fsms).service_names(),
    };
    let cnc = CncPage::create_file(&dir.cnc_path(), &meta)?;
    let mask = (1u64 << a.fsms) - 1;
    cnc.store_services_declared(mask);
    cnc.store_fsm_lag_bytes(lag);
    cnc.status()
        .flags
        .store_release(NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE);
    cnc.status().leader_hint.store_release(0);
    cnc.status().node_heartbeat_ns.store_release(unix_ns());
    let buffer = Arc::new(LogBuffer::create_file(
        &dir.log_path(),
        buffer_bytes,
        Arc::clone(&cnc),
        MAX_PAYLOAD as usize,
    )?);
    let mut rings = Vec::new();
    for id in 0..a.fsms {
        std::fs::create_dir_all(dir.snapshot_dir_for(id))?;
        let q = SpscRing::create(&dir.svc_query_ring_for(id), MIB, MAX_MSG)
            .map_err(|e| anyhow::anyhow!("svc_query ring: {e}"))?;
        let e = BroadcastRing::create(&dir.egress_service_for(id), 4 * MIB, MAX_MSG)
            .map_err(|e| anyhow::anyhow!("egress ring: {e}"))?;
        // The per-row `svc_sched` SPSC (service → node) that log time and
        // timers added: `uc_service::attach` OPENS it, so a fake node that
        // does not create it fails every attach with
        // `ring error: io: No such file or directory`. Same geometry as the
        // real node's (`uc_node::node`: `SpscRing::create(.., MIB, MAX_MSG)`).
        // The raw counter never schedules a timer, so nothing is ever written
        // and no drain is needed — the consumer half is held only to keep the
        // mapping alive for the run.
        let s = SpscRing::create(&dir.svc_sched_ring_for(id), MIB, MAX_MSG)
            .map_err(|e| anyhow::anyhow!("svc_sched ring: {e}"))?;
        rings.push((q, e, s));
    }

    // ---- the FSMs ----
    // FSM identity: attach finds its row by name, and the row (a const
    // generic on `TaggedRaw`) can't be a runtime value — `id` selects which
    // monomorphization to spawn.
    let mut services: Vec<Box<dyn BenchService>> = Vec::new();
    let kv_arm = a.sm != "raw";
    if kv_arm {
        anyhow::ensure!(
            a.payload >= KV_PUT_HEAD,
            "--payload must be >= {KV_PUT_HEAD} for the kv arms"
        );
        anyhow::ensure!(a.keys >= 1, "--keys must be >= 1");
    }
    // One monomorphization per (row, arm): `ROW` is a const generic and the
    // arm picks the inner type, so both are fixed here at spawn time.
    macro_rules! spawn_rows {
        ($cfg:expr, $mk:expr, $id:expr) => {{
            let make = $mk;
            let b: Box<dyn BenchService> = match $id {
                0 => Box::new(
                    ServiceBuilder::new(
                        $cfg,
                        TaggedRaw::<0, _> {
                            inner: make(),
                            frames: 0,
                        },
                    )
                    .start()?,
                ),
                1 => Box::new(
                    ServiceBuilder::new(
                        $cfg,
                        TaggedRaw::<1, _> {
                            inner: make(),
                            frames: 0,
                        },
                    )
                    .start()?,
                ),
                2 => Box::new(
                    ServiceBuilder::new(
                        $cfg,
                        TaggedRaw::<2, _> {
                            inner: make(),
                            frames: 0,
                        },
                    )
                    .start()?,
                ),
                3 => Box::new(
                    ServiceBuilder::new(
                        $cfg,
                        TaggedRaw::<3, _> {
                            inner: make(),
                            frames: 0,
                        },
                    )
                    .start()?,
                ),
                4 => Box::new(
                    ServiceBuilder::new(
                        $cfg,
                        TaggedRaw::<4, _> {
                            inner: make(),
                            frames: 0,
                        },
                    )
                    .start()?,
                ),
                5 => Box::new(
                    ServiceBuilder::new(
                        $cfg,
                        TaggedRaw::<5, _> {
                            inner: make(),
                            frames: 0,
                        },
                    )
                    .start()?,
                ),
                6 => Box::new(
                    ServiceBuilder::new(
                        $cfg,
                        TaggedRaw::<6, _> {
                            inner: make(),
                            frames: 0,
                        },
                    )
                    .start()?,
                ),
                7 => Box::new(
                    ServiceBuilder::new(
                        $cfg,
                        TaggedRaw::<7, _> {
                            inner: make(),
                            frames: 0,
                        },
                    )
                    .start()?,
                ),
                _ => unreachable!("--fsms is bounds-checked to 1..=8 above"),
            };
            b
        }};
    }
    for id in 0..a.fsms {
        let cfg = ServiceConfig::new(&a.root, APP);
        let svc: Box<dyn BenchService> = match a.sm.as_str() {
            "raw" => spawn_rows!(cfg, || RawCount { last: None }, id),
            "kv" => spawn_rows!(cfg, KvSm::default, id),
            "kv-btree" => spawn_rows!(cfg, KvBTree::default, id),
            "kv-hash" => spawn_rows!(cfg, KvHash::default, id),
            m => anyhow::bail!("--sm must be raw|kv|kv-btree|kv-hash, got {m}"),
        };
        services.push(svc);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while (0..a.fsms)
        .any(|id| !unpack_service_status(cnc.service_slot(id as usize).status.load_acquire()).1)
    {
        anyhow::ensure!(
            Instant::now() < deadline,
            "FSMs did not all attach within 10 s"
        );
        std::thread::sleep(Duration::from_millis(1));
    }

    // ---- the driver: appender + fake archive/consensus, paced by the slowest FSM ----
    let stop = Arc::new(AtomicBool::new(false));
    let appended = Arc::new(AtomicU64::new(0));
    let driver = {
        let buffer = Arc::clone(&buffer);
        let cnc = Arc::clone(&cnc);
        let stop = Arc::clone(&stop);
        let appended = Arc::clone(&appended);
        let (fsms, batch, payload_len) = (a.fsms, a.batch, a.payload);
        let (kv_arm, keys) = (kv_arm, a.keys);
        std::thread::Builder::new()
            .name("apply-bench-driver".into())
            .spawn(move || {
                let mut app = Appender::new(buffer, 1, 0);
                let mut payload = if kv_arm {
                    kv_put_template(payload_len)
                } else {
                    vec![0x42u8; payload_len]
                };
                let mut n = 0u64;
                let mut stalls = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    if kv_arm {
                        kv_patch_key(&mut payload, n, keys);
                    }
                    match app.append(0, n as u32, &payload) {
                        Ok(_) => {
                            n += 1;
                            if n.is_multiple_of(batch) {
                                let p = app.position();
                                let c = cnc.counters();
                                c.durable.store_release(p);
                                c.commit.store_release(p);
                                appended.store(n, Ordering::Relaxed);
                                // Pace on the slowest FSM: the apply hop is the
                                // thing under test, never the driver.
                                while !stop.load(Ordering::Relaxed)
                                    && p - min_applied(&cnc, fsms) > window
                                {
                                    stalls += 1;
                                    std::hint::spin_loop();
                                }
                            }
                        }
                        Err(AppendError::WouldOverrun) => std::thread::yield_now(),
                        Err(e) => panic!("append: {e:?}"),
                    }
                }
                let p = app.position();
                cnc.counters().durable.store_release(p);
                cnc.counters().commit.store_release(p);
                appended.store(n, Ordering::Relaxed);
                stalls
            })?
    };

    // ---- measure ----
    let snap = |cnc: &CncPage| -> Vec<(u64, u64)> {
        (0..a.fsms)
            .map(|id| {
                let s = cnc.service_slot(id as usize);
                (s.applied.load_acquire(), s.lag_waits.load_acquire())
            })
            .collect()
    };
    std::thread::sleep(Duration::from_secs(a.warmup_secs));
    let t0 = Instant::now();
    let before = snap(&cnc);
    let appended0 = appended.load(Ordering::Relaxed);
    std::thread::sleep(Duration::from_secs(a.secs));
    let after = snap(&cnc);
    let appended1 = appended.load(Ordering::Relaxed);
    let elapsed = t0.elapsed().as_secs_f64();
    stop.store(true, Ordering::Relaxed);
    let stalls = driver.join().expect("driver");

    println!(
        "== apply_bench: {} FSM(s), sm={} keys={} mode={} lag={} payload={} frame={} batch={} window={} secs={:.2} (SMOKE, not a gate) ==",
        a.fsms, a.sm, a.keys, a.mode, lag, a.payload, frame, a.batch, window, elapsed
    );
    let mut per = Vec::new();
    for id in 0..a.fsms as usize {
        let frames = (after[id].0 - before[id].0) / frame;
        let rate = frames as f64 / elapsed;
        let waits = after[id].1 - before[id].1;
        per.push((rate, waits));
        println!(
            "fsm={id} applied_frames/s={rate:.0} MB/s={:.1} lag_waits={waits}",
            rate * frame as f64 / 1e6
        );
    }
    let min_rate = per.iter().map(|p| p.0).fold(f64::MAX, f64::min);
    let driver_rate = (appended1 - appended0) as f64 / elapsed;
    println!("driver appended_frames/s={driver_rate:.0} pace_stalls={stalls}");
    println!("hop: min applied_frames/s={min_rate:.0}");
    println!(
        "{}",
        render_apply_json(
            a.fsms,
            &a.mode,
            lag,
            a.payload,
            frame,
            elapsed,
            min_rate,
            driver_rate,
            stalls,
            &per,
        )
    );

    // The SM's own count: proves the cursor sweep applied every frame (not
    // just advanced past them). `total` covers warmup too, so compare against
    // the total applied bytes / frame.
    let mut content_ok = true;
    for (id, s) in services.iter().enumerate() {
        let sm_frames = s.query_frames();
        let swept = cnc.service_slot(id).applied.load_acquire() / frame;
        println!(
            "fsm={id} sm_frames={sm_frames} swept_frames={swept}{}",
            if sm_frames == swept { "" } else { " MISMATCH" }
        );
        // Content check for the kv arms: every frame was a PUT to key
        // `n % keys`, so the map must hold exactly min(frames, keys) entries.
        // Counting apply calls cannot see a frame that decoded to
        // BAD_REQUEST (the cursor still moves); the map's length can.
        if kv_arm {
            let want = sm_frames.min(a.keys);
            match s.query_kv() {
                Some((len, digest)) if len == want && digest != 0 => {
                    println!("fsm={id} kv_len={len} kv_digest={digest:#x} (content OK)");
                }
                got => {
                    content_ok = false;
                    println!("fsm={id} kv content CHECK FAILED: want len={want}, got {got:?}");
                }
            }
        }
    }
    for s in services {
        s.stop();
    }
    drop(rings);
    drop(dir); // releases `instance.lock` before the dir goes away
    let _ = std::fs::remove_dir_all(&a.root);
    anyhow::ensure!(content_ok, "a kv arm did not hold the state its frames put");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{KV_KEY_LEN, KV_PUT_HEAD, kv_patch_key, kv_put_template, render_apply_json};
    use kv_store::wire::{Command, decode_command};

    /// The driver's patched template must decode, through `examples/kv`'s
    /// own decoder, as a PUT of the cycled key with the value filling the
    /// rest of `--payload` — otherwise the kv arms measure a BAD_REQUEST
    /// path, not a map insert.
    #[test]
    fn kv_put_template_decodes_as_put_of_the_patched_key() {
        let mut f = kv_put_template(64);
        assert_eq!(f.len(), 64);
        kv_patch_key(&mut f, 100_007, 100_000);
        match decode_command(&f) {
            Ok(Command::Put { key, value }) => {
                assert_eq!(key, &7u64.to_le_bytes());
                assert_eq!(key.len(), KV_KEY_LEN);
                assert_eq!(value.len(), 64 - KV_PUT_HEAD);
            }
            other => panic!("not a PUT: {other:?}"),
        }
    }

    /// Pins the exact bytes of the line `scripts/apply_ab.sh` parses.
    ///
    /// The runner reads `min_rate` out of this line, and its `--selftest`
    /// stubs print a hand-written copy of this shape. If this assertion is
    /// ever edited, the script's parser AND its stubs must be edited in the
    /// same commit — that coupling is the whole point of the pin.
    #[test]
    fn apply_json_line_shape_is_pinned() {
        let line = render_apply_json(
            2,
            "bounded",
            16_777_216,
            64,
            96,
            8.0,
            1_234_567.4,
            2_345_678.6,
            42,
            &[(1_234_567.4, 11), (2_345_678.6, 0)],
        );
        assert_eq!(
            line,
            "APPLY-JSON {\"fsms\":2,\"mode\":\"bounded\",\"lag\":16777216,\"payload\":64,\
             \"frame\":96,\"secs\":8.00,\"min_rate\":1234567,\"driver_rate\":2345679,\
             \"pace_stalls\":42,\
             \"per\":[{\"fsm\":0,\"rate\":1234567,\"lag_waits\":11},\
             {\"fsm\":1,\"rate\":2345679,\"lag_waits\":0}]}"
        );
    }
}
