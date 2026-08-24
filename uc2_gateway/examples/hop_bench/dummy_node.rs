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
use uc_protocol::ring::{BroadcastRing, MpscConsumer, MpscRing, RingError};
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

    loop {
        let mut did = false;
        for (ring, is_query) in
            [(&mut ingress as &mut MpscConsumer, false), (&mut query as &mut MpscConsumer, true)]
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
                    Ok(None) => break,
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
            if idle > 64 {
                std::thread::yield_now();
                idle = 0;
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
            println!("dummy-node: popped={popped} resp/s={rate:.0}");
            let _ = std::io::stdout().flush();
            popped_at_last_report = popped;
            last_report = Instant::now();
        }
    }
}
