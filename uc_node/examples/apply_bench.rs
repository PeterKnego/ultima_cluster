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
use uc_log::buffer::{AppendError, Appender, LogBuffer};
use uc_log::cnc::{CncMeta, CncPage, unpack_service_status};
use uc_protocol::ring::{BroadcastRing, SpscRing};
use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER};
use uc_protocol::v2::frame::{HEADER_LEN, align_frame_len};
use uc_service::{RawStateMachine, ServiceBuilder, ServiceConfig};

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
}

/// Raw-tier counter: no decode, no allocation — the cheapest legal SM, so the
/// hop's own cost (the barrier, the loop, the egress publish) is what shows.
struct RawCount {
    frames: u64,
    last: Option<u64>,
}

impl RawStateMachine for RawCount {
    fn apply(&mut self, position: u64, _cmd: &[u8], out: &mut Vec<u8>) {
        self.frames += 1;
        self.last = Some(position);
        out.extend_from_slice(&self.frames.to_le_bytes());
    }
    fn query(&self, _q: &[u8], out: &mut Vec<u8>) {
        out.extend_from_slice(&self.frames.to_le_bytes());
    }
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
}

fn unix_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
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
    let _ = std::fs::remove_dir_all(&a.root);
    std::fs::create_dir_all(a.root.join("journal"))?;
    std::fs::create_dir_all(a.root.join("state"))?;
    let meta = CncMeta {
        node_id: 0,
        instance_id: rand::random::<u128>(),
        app_id: APP.into(),
        buffer_bytes,
        max_payload: MAX_PAYLOAD,
    };
    let cnc = CncPage::create_file(&a.root.join("cnc2.dat"), &meta)?;
    let mask = (1u64 << a.fsms) - 1;
    cnc.store_services_declared(mask);
    cnc.store_fsm_lag_bytes(lag);
    cnc.status()
        .flags
        .store_release(NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE);
    cnc.status().leader_hint.store_release(0);
    cnc.status().node_heartbeat_ns.store_release(unix_ns());
    let buffer = Arc::new(LogBuffer::create_file(
        &a.root.join("log.buf"),
        buffer_bytes,
        Arc::clone(&cnc),
        MAX_PAYLOAD as usize,
    )?);
    let mut rings = Vec::new();
    for id in 0..a.fsms {
        std::fs::create_dir_all(a.root.join("snapshots").join(id.to_string()))?;
        let q = SpscRing::create(&a.root.join(format!("svc_query.{id}.ring")), MIB, MAX_MSG)
            .map_err(|e| anyhow::anyhow!("svc_query ring: {e}"))?;
        let e = BroadcastRing::create(
            &a.root.join(format!("egress_service.{id}.broadcast")),
            4 * MIB,
            MAX_MSG,
        )
        .map_err(|e| anyhow::anyhow!("egress ring: {e}"))?;
        rings.push((q, e));
    }

    // ---- the FSMs ----
    let mut services = Vec::new();
    for id in 0..a.fsms {
        let cfg = ServiceConfig::new(&a.root, APP).service_id(id);
        services.push(
            ServiceBuilder::new(
                cfg,
                RawCount {
                    frames: 0,
                    last: None,
                },
            )
            .start()?,
        );
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
        std::thread::Builder::new()
            .name("apply-bench-driver".into())
            .spawn(move || {
                let mut app = Appender::new(buffer, 1);
                let payload = vec![0x42u8; payload_len];
                let mut n = 0u64;
                let mut stalls = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    match app.append(0, n, &payload) {
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
        "== apply_bench: {} FSM(s), mode={} lag={} payload={} frame={} batch={} window={} secs={:.2} (SMOKE, not a gate) ==",
        a.fsms, a.mode, lag, a.payload, frame, a.batch, window, elapsed
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
    let per_json: Vec<String> = per
        .iter()
        .enumerate()
        .map(|(i, p)| format!("{{\"fsm\":{i},\"rate\":{:.0},\"lag_waits\":{}}}", p.0, p.1))
        .collect();
    println!(
        "APPLY-JSON {{\"fsms\":{},\"mode\":\"{}\",\"lag\":{},\"payload\":{},\"frame\":{},\"secs\":{:.2},\"min_rate\":{:.0},\"driver_rate\":{:.0},\"per\":[{}]}}",
        a.fsms,
        a.mode,
        lag,
        a.payload,
        frame,
        elapsed,
        min_rate,
        driver_rate,
        per_json.join(",")
    );

    // The SM's own count: proves the cursor sweep applied every frame (not
    // just advanced past them). `total` covers warmup too, so compare against
    // the total applied bytes / frame.
    for (id, s) in services.iter().enumerate() {
        let mut out = Vec::new();
        s.query_raw(&[], &mut out);
        let sm_frames = u64::from_le_bytes(out[..8].try_into().unwrap());
        let swept = cnc.service_slot(id).applied.load_acquire() / frame;
        println!(
            "fsm={id} sm_frames={sm_frames} swept_frames={swept}{}",
            if sm_frames == swept { "" } else { " MISMATCH" }
        );
    }
    for s in services {
        s.stop();
    }
    drop(rings);
    let _ = std::fs::remove_dir_all(&a.root);
    Ok(())
}
