// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Hop-1 sink: a **node-shaped process over a REAL instance dir** — the same
//! well-known shmem files `uc2_node` creates (`cnc2.dat`, `ingress.ring`,
//! `query.ring`, both egress broadcasts), a cnc page marked as a serving
//! leader, and one single-writer busy-poll agent that pops every ingress /
//! query record and publishes exactly one position-keyed `MSG_V2_RESPONSE`
//! for it.
//!
//! There is no log buffer, no consensus, no durability and no service: the
//! payload is discarded and the `position` is synthesised by advancing the
//! aligned frame length a real log would have consumed. That is the point —
//! this is "a node with an infinitely fast backend", so anything measured
//! against it is hop 1 (the client/edge → shmem path) alone.
//!
//! Parks forever; the orchestrator kills it. Prints `READY` on its own line
//! once the files exist and the serving flags are set.

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};

use uc2_log::cnc::{CncMeta, CncPage};
use uc_protocol::ring::{
    BroadcastRing, MpscConsumer, MpscRing, RingError, RingHeader, RingWaitHandle,
};
use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER};
use uc_protocol::v2::frame::{HEADER_LEN, align_frame_len};
use uc_protocol::v2::ipc::{FLAG_V2_IS_QUERY, MSG_V2_RESPONSE};

/// Same per-record ceiling `uc2_node::create_rings` gives every ring.
const MAX_MSG: u32 = 64 << 10;
/// Same default the real node publishes (`uc2_node`'s `default_admission_bytes`).
const ADMISSION_BYTES: u64 = 256 * 1024;
const MIB: u64 = 1 << 20;

#[derive(clap::Args)]
pub struct Args {
    /// Instance dir to (re)create. Wiped of any stale cnc/ring files first.
    #[arg(long)]
    pub instance_dir: PathBuf,
    #[arg(long, default_value = "hop-bench")]
    pub app_id: String,
    #[arg(long, default_value_t = 0)]
    pub node_id: u32,
    /// Published `cnc.meta().max_payload` — the door an attaching `Engine`
    /// inherits when its own `max_payload` is `None`.
    #[arg(long, default_value_t = 512)]
    pub max_payload: u32,
    /// Ingress + query ring capacity (power of two).
    #[arg(long, default_value_t = 64 * MIB)]
    pub ingress_bytes: u64,
    /// Egress broadcast capacity (power of two).
    #[arg(long, default_value_t = 64 * MIB)]
    pub egress_bytes: u64,
    /// Response body length, after the mandatory 8-byte LE position prefix.
    #[arg(long, default_value_t = 8)]
    pub response_body: usize,
    /// What the single drain thread does when both rings came up empty.
    /// `yield` (default, and what the smoke ladder measures) = 64 spins then
    /// `yield_now`; `spin` = never leave the core; `park` = 64 spins then
    /// arm + futex-park on the ingress ring's wake word for `--park-us`.
    /// The REAL node busy-spins by design — this knob exists to attribute a
    /// collapse to the sink's idle policy, not to propose one.
    #[arg(long, default_value = "yield")]
    pub idle: IdlePolicy,
    /// Park budget for `--idle park`, in microseconds.
    #[arg(long, default_value_t = 50)]
    pub park_us: u64,
    /// Report per-second MPSC hole telemetry alongside the pop rate:
    /// `holes` (cumulative `MpscConsumer::holes_skipped`), `hol` (polls that
    /// returned `Ok(None)` with `claim_position > consumer_position` — the
    /// consumer head-of-line behind exactly one claimed-but-uncommitted slot)
    /// and `empty` (polls that returned `Ok(None)` on a genuinely empty ring).
    /// OFF by default: the two extra Acquire loads sit on the `Ok(None)` path,
    /// which is hot when the ring is starved, so leaving it on would perturb
    /// the very number the ladder reports.
    #[arg(long, default_value_t = false)]
    pub hole_stats: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum IdlePolicy {
    Spin,
    Yield,
    Park,
}

/// Read-only view of a ring file's [`RingHeader`], for the `--hole-stats`
/// observer. `uc_protocol` exposes `RingHeader` with public atomics but no
/// position accessor on `MpscConsumer`; rather than add one to production ring
/// code for a bench, the sink maps the same file a second time, read-only, and
/// reads the two counters it needs.
struct HeaderView {
    _mmap: memmap2::Mmap,
    header: *const RingHeader,
}

impl HeaderView {
    fn open(path: &std::path::Path) -> anyhow::Result<Self> {
        let f = std::fs::File::open(path)?;
        // SAFETY: the ring file is a fixed-layout shared mapping whose first
        // `size_of::<RingHeader>()` bytes are exactly that struct; this second
        // mapping is read-only and only ever loads its atomics.
        let mmap = unsafe { memmap2::Mmap::map(&f)? };
        anyhow::ensure!(mmap.len() >= std::mem::size_of::<RingHeader>(), "ring file too short");
        let header = mmap.as_ptr().cast::<RingHeader>();
        Ok(Self { _mmap: mmap, header })
    }

    /// `true` when `claim_position > consumer_position` — i.e. an `Ok(None)`
    /// observed right now is head-of-line behind a claimed slot, not an empty
    /// ring.
    fn head_of_line(&self) -> bool {
        // SAFETY: `header` points into `_mmap`, alive for `self`'s lifetime.
        let h = unsafe { &*self.header };
        h.claim_position.load(std::sync::atomic::Ordering::Acquire)
            > h.consumer_position.load(std::sync::atomic::Ordering::Acquire)
    }
}

/// "Random enough" instance id — a fresh one per boot invalidates any stale
/// attachment, exactly as the real node's `rand::random::<u128>()` does.
fn rand_instance_id() -> u128 {
    let ns = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(1);
    let pid = std::process::id() as u128;
    (ns ^ 0xA5A5_5A5A_A5A5_5A5A_u128).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ (pid << 64)
}

fn now_wall_ns() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

pub fn run(a: Args) -> anyhow::Result<()> {
    std::fs::create_dir_all(&a.instance_dir)?;
    for f in [
        "cnc2.dat",
        "ingress.ring",
        "query.ring",
        "svc_query.ring",
        "egress_service.broadcast",
        "egress_node.broadcast",
    ] {
        let _ = std::fs::remove_file(a.instance_dir.join(f));
    }

    let meta = CncMeta {
        node_id: a.node_id,
        instance_id: rand_instance_id(),
        app_id: a.app_id.clone(),
        buffer_bytes: a.ingress_bytes,
        max_payload: a.max_payload,
    };
    let cnc = CncPage::create_file(&a.instance_dir.join("cnc2.dat"), &meta)
        .map_err(|e| anyhow::anyhow!("create cnc page: {e}"))?;

    // The four client-facing rings. Sizes are this bench's own (deliberately
    // fatter than the real node's 4 MiB / 1 MiB, so the SINK is never the
    // thing being measured); `MAX_MSG` matches the real node exactly.
    MpscRing::create(&a.instance_dir.join("ingress.ring"), a.ingress_bytes, MAX_MSG)
        .map_err(|e| anyhow::anyhow!("create ingress ring: {e}"))?;
    MpscRing::create(&a.instance_dir.join("query.ring"), a.ingress_bytes, MAX_MSG)
        .map_err(|e| anyhow::anyhow!("create query ring: {e}"))?;
    BroadcastRing::create(&a.instance_dir.join("egress_service.broadcast"), a.egress_bytes, MAX_MSG)
        .map_err(|e| anyhow::anyhow!("create egress_service ring: {e}"))?;
    BroadcastRing::create(&a.instance_dir.join("egress_node.broadcast"), a.egress_bytes, MAX_MSG)
        .map_err(|e| anyhow::anyhow!("create egress_node ring: {e}"))?;

    // Publish "serving leader" so `SendHalf::can_serve()` / `leader_hint()`
    // and the edge's leader watch both see one.
    let status = cnc.status();
    status.term.store_release(1);
    status.leader_hint.store_release(a.node_id as u64);
    status.flags.store_release(NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE);
    status.node_heartbeat_ns.store_release(now_wall_ns());
    cnc.store_admission_bytes(ADMISSION_BYTES);

    // Node-side halves: consume both client → node MPSC rings, produce onto
    // the service egress broadcast (the ring the real SERVICE writes
    // responses to — that is where a client's matcher looks first).
    let (_ingress_producer, mut ingress) = MpscRing::open(&a.instance_dir.join("ingress.ring"))
        .map_err(|e| anyhow::anyhow!("open ingress ring: {e}"))?
        .into_split();
    let (_query_producer, mut query) = MpscRing::open(&a.instance_dir.join("query.ring"))
        .map_err(|e| anyhow::anyhow!("open query ring: {e}"))?
        .into_split();
    let mut egress = BroadcastRing::open(&a.instance_dir.join("egress_service.broadcast"))
        .map_err(|e| anyhow::anyhow!("open egress_service ring: {e}"))?
        .producer();

    println!("READY");
    use std::io::Write;
    std::io::stdout().flush()?;

    // ------------------------------------------------------------ hot loop
    let mut payload_buf: Vec<u8> = Vec::with_capacity(MAX_MSG as usize);
    let mut resp_buf: Vec<u8> = vec![0u8; 8 + a.response_body];
    // Synthetic log position: advance by the aligned frame length a real log
    // buffer would have consumed for this record.
    let mut position: u64 = 0;
    let mut popped: u64 = 0;
    let mut popped_at_last_report: u64 = 0;
    let mut idle: u32 = 0;
    let mut last_report = Instant::now();
    let mut last_heartbeat = Instant::now();

    // `--hole-stats` observers (one per MPSC ring) and their counters.
    let views: Option<[HeaderView; 2]> = if a.hole_stats {
        Some([
            HeaderView::open(&a.instance_dir.join("ingress.ring"))?,
            HeaderView::open(&a.instance_dir.join("query.ring"))?,
        ])
    } else {
        None
    };
    let mut hol_polls: u64 = 0;
    let mut empty_polls: u64 = 0;
    let (mut hol_at_last, mut empty_at_last) = (0u64, 0u64);
    let mut holes_at_last: u64 = 0;

    // `--idle park` parks on the ingress ring's wake word: an MPSC producer
    // bumps the commit count and `signal()`s once per commit, and only
    // syscalls when `waiters > 0`.
    let park_handle: Option<RingWaitHandle> =
        if a.idle == IdlePolicy::Park { Some(ingress.wait_handle()) } else { None };
    let park_budget = Duration::from_micros(a.park_us);

    loop {
        let mut did = false;
        for (idx, (ring, is_query)) in
            [(&mut ingress as &mut MpscConsumer, false), (&mut query as &mut MpscConsumer, true)]
                .into_iter()
                .enumerate()
        {
            // Bounded batch per ring so neither starves the other.
            for _ in 0..256 {
                match ring.try_read(&mut payload_buf) {
                    Ok(Some(rec)) => {
                        did = true;
                        popped += 1;
                        let frame_len = align_frame_len(HEADER_LEN + payload_buf.len()) as u64;
                        resp_buf[..8].copy_from_slice(&position.to_le_bytes());
                        position += frame_len;
                        let flags = if is_query { FLAG_V2_IS_QUERY } else { 0 };
                        // Never drop a response: retry a Full broadcast.
                        loop {
                            match egress.write(MSG_V2_RESPONSE, flags, rec.header_extra, &resp_buf)
                            {
                                Ok(()) => break,
                                Err(RingError::Full) => std::thread::yield_now(),
                                Err(e) => {
                                    return Err(anyhow::anyhow!("egress write: {e}"));
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        // The one place the two `Ok(None)` causes can be told
                        // apart: `try_read` collapses "ring empty" and "head
                        // of line behind exactly one claimed slot" into the
                        // same return, so the observer re-reads the header.
                        if let Some(v) = views.as_ref() {
                            if v[idx].head_of_line() {
                                hol_polls += 1;
                            } else {
                                empty_polls += 1;
                            }
                        }
                        break;
                    }
                    Err(e) => {
                        // A torn/oversized record is a bug in the driver, not
                        // something this sink can repair — but do not die on
                        // it either; count it as popped and move on.
                        eprintln!("dummy-node: ring read error: {e}");
                        break;
                    }
                }
            }
        }

        if did {
            idle = 0;
        } else {
            idle += 1;
            match a.idle {
                IdlePolicy::Spin => std::hint::spin_loop(),
                IdlePolicy::Yield => {
                    if idle > 64 {
                        std::thread::yield_now();
                        idle = 0;
                    }
                }
                IdlePolicy::Park => {
                    if idle > 64 {
                        if let Some(h) = park_handle.as_ref() {
                            let seq = h.current_seq();
                            h.arm();
                            // Re-check after arming: a commit between the last
                            // `try_read` and `arm()` would otherwise be a lost
                            // wakeup (the `PARK_CEIL` backstop bounds it, but
                            // at 2 ms that IS the result on this bench).
                            if h.current_seq() == seq {
                                h.park(seq, park_budget);
                            }
                            h.disarm();
                        }
                        idle = 0;
                    }
                }
            }
        }

        // Housekeeping, off the per-record path.
        if last_heartbeat.elapsed() >= Duration::from_millis(100) {
            status.node_heartbeat_ns.store_release(now_wall_ns());
            last_heartbeat = Instant::now();
        }
        if last_report.elapsed() >= Duration::from_secs(1) {
            let elapsed = last_report.elapsed().as_secs_f64();
            let rate = (popped - popped_at_last_report) as f64 / elapsed;
            let holes = ingress.holes_skipped() + query.holes_skipped();
            let mut line = format!(
                "dummy-node: popped={popped} resp/s={rate:.0} holes={holes} \
                 holes/s={:.0}",
                (holes - holes_at_last) as f64 / elapsed
            );
            holes_at_last = holes;
            if a.hole_stats {
                line.push_str(&format!(
                    " hol/s={:.0} empty/s={:.0}",
                    (hol_polls - hol_at_last) as f64 / elapsed,
                    (empty_polls - empty_at_last) as f64 / elapsed,
                ));
                hol_at_last = hol_polls;
                empty_at_last = empty_polls;
            }
            println!("{line}");
            let _ = std::io::stdout().flush();
            popped_at_last_report = popped;
            last_report = Instant::now();
        }
    }
}
