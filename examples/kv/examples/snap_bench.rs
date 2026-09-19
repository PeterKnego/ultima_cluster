// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `snap_bench` — the snapshot side of a kv state store, in isolation.
//!
//! `uc_node`'s `apply_bench --sm kv|kv-btree|kv-hash` prices a store per
//! WRITE. This prices the same three stores per SNAPSHOT, through the real
//! `SnapshotStateMachine` calls and nothing else (no node, no rings): the
//! `freeze` stall the apply thread pays, the off-thread stream's time and
//! bytes, the write penalty while a frozen handle is alive (`KvSm`'s
//! `Arc<BTreeMap>` deep-copies the whole map on the FIRST write after a freeze
//! — `Arc::make_mut` — which is where its O(1) `freeze` moves the O(n) cost;
//! the `live_write_max_ns` column is that spike), the write cost once the
//! handle is dropped, `install_snapshot`
//! time, and peak RSS growth across the live snapshot (the peak is reset
//! before each live window via `/proc/self/clear_refs`; where that is
//! unavailable the column is `null`).
//!
//! SMOKE on a dev box (`docs/notes/dev-box-not-a-bench.md`): ratios between
//! arms, never absolutes against a bar.
//!
//! ```text
//! cargo run -p kv_store --release --example snap_bench -- --keys 1000000 --reps 3
//! ```

use std::io::Write;
use std::time::Instant;

use clap::Parser;
use kv_store::bench::{KvBTree, KvHash};
use kv_store::{KvSm, wire};
use uc_service::{ApplyCtx, RawStateMachine, SnapshotStateMachine};

#[derive(Parser)]
#[command(about = "Snapshot-side cost of examples/kv's store vs std-map twins")]
struct Args {
    /// Distinct 8-byte keys in the store.
    #[arg(long, default_value_t = 1_000_000)]
    keys: u64,
    /// Value bytes per key (apply_bench's 64 B PUT carries 52).
    #[arg(long, default_value_t = 52)]
    value_bytes: usize,
    /// Overwrites in each steady-state write window.
    #[arg(long, default_value_t = 500_000)]
    writes: u64,
    #[arg(long, default_value_t = 3)]
    reps: u32,
    /// Comma-separated: ordmap, btree, hash.
    #[arg(long, default_value = "ordmap,btree,hash")]
    arms: String,
}

/// One arm's numbers for one rep.
#[derive(Default, Debug)]
struct Row {
    fill_ns: f64,
    steady_ns: f64,
    steady_max_ns: u64,
    freeze_ns: u64,
    stream_idle_ms: f64,
    bytes: u64,
    live_write_ns: f64,
    live_write_max_ns: u64,
    live_writes: u64,
    stream_live_ms: f64,
    after_ns: f64,
    restore_ms: f64,
    hwm_delta_kb: Option<i64>,
}

/// A sink that counts bytes and drops them: the stream's own cost, no disk.
struct Count(u64);
impl Write for Count {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0 += b.len() as u64;
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Peak RSS so far, from `/proc/self/status`. `None` if unreadable: the
/// column then prints `null`, never a plausible number.
fn vm_hwm_kb() -> Option<i64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    s.lines()
        .find_map(|l| l.strip_prefix("VmHWM:"))
        .and_then(|v| v.trim().trim_end_matches(" kB").trim().parse().ok())
}

/// Resets the peak (Linux ≥ 4.0: writing `5` to `clear_refs`), so a delta
/// measured across the live window is this window's growth and not the
/// process's earlier high-water mark — reps share one process. `false` if
/// the reset is unavailable, in which case the column is `null`.
fn reset_vm_hwm() -> bool {
    std::fs::write("/proc/self/clear_refs", b"5").is_ok()
}

/// The PUT frame for key `k`, value `vb` bytes, as a client would send it.
fn put_frame(k: u64, vb: usize) -> Vec<u8> {
    wire::encode_put(&k.to_le_bytes(), &vec![0x42u8; vb])
}

/// Drives `S` through the real trait calls. `pos` is the running log
/// position (strictly increasing; a user frame never applies at 0).
fn run_arm<S: RawStateMachine + SnapshotStateMachine + Default>(a: &Args, name: &str) -> Row
where
    S::SnapshotHandle: Send + 'static,
{
    let mut r = Row::default();
    let mut sm = S::default();
    let mut pos = 1u64;
    let mut out = Vec::with_capacity(16);
    let mut frame = put_frame(0, a.value_bytes);
    let patch = |f: &mut [u8], k: u64| f[4..12].copy_from_slice(&k.to_le_bytes());
    let write = |sm: &mut S, k: u64, pos: &mut u64, out: &mut Vec<u8>, frame: &mut Vec<u8>| {
        patch(frame, k);
        out.clear();
        let mut ctx = ApplyCtx::for_sm::<S>(*pos);
        sm.apply(&mut ctx, frame, out);
        *pos += 96;
    };

    // fill
    let t = Instant::now();
    for k in 0..a.keys {
        write(&mut sm, k, &mut pos, &mut out, &mut frame);
    }
    r.fill_ns = t.elapsed().as_nanos() as f64 / a.keys as f64;

    // steady overwrites, no handle alive — timed per write exactly as the
    // live window below is, so the two differ only by the concurrent stream
    let t = Instant::now();
    let mut steady_max = 0u64;
    for i in 0..a.writes {
        let t1 = Instant::now();
        write(&mut sm, i % a.keys, &mut pos, &mut out, &mut frame);
        steady_max = steady_max.max(t1.elapsed().as_nanos() as u64);
    }
    r.steady_ns = t.elapsed().as_nanos() as f64 / a.writes as f64;
    r.steady_max_ns = steady_max;

    // freeze + live stream on another thread while this thread keeps writing.
    // This freeze is not timed; `freeze_ns` comes from the idle capture below
    // on the same-sized map, so the live window's clock starts after it.
    let hwm0 = if reset_vm_hwm() { vm_hwm_kb() } else { None };
    let (h, _) = sm.freeze().expect("freeze");
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let streamer = {
        let done = done.clone();
        std::thread::spawn(move || {
            let t = Instant::now();
            let mut sink = Count(0);
            S::stream_snapshot(h, &mut sink).expect("stream");
            let ms = t.elapsed().as_secs_f64() * 1e3;
            done.store(true, std::sync::atomic::Ordering::Release);
            (ms, sink.0)
        })
    };
    let mut n = 0u64;
    let mut max = 0u64;
    let t = Instant::now();
    while !done.load(std::sync::atomic::Ordering::Acquire) {
        let t1 = Instant::now();
        write(&mut sm, n % a.keys, &mut pos, &mut out, &mut frame);
        max = max.max(t1.elapsed().as_nanos() as u64);
        n += 1;
    }
    let live_total = t.elapsed().as_nanos() as f64;
    let (ms, bytes) = streamer.join().expect("streamer");
    let live_bytes = bytes;
    r.stream_live_ms = ms;
    r.live_writes = n;
    r.live_write_ns = if n > 0 { live_total / n as f64 } else { 0.0 };
    r.live_write_max_ns = max;
    r.hwm_delta_kb = hwm0.zip(vm_hwm_kb()).map(|(before, after)| after - before);

    // freeze + idle stream (writer waits), also captures the image for restore
    let t = Instant::now();
    let (h, p) = sm.freeze().expect("freeze");
    r.freeze_ns = t.elapsed().as_nanos() as u64;
    let mut image = Vec::new();
    let t = Instant::now();
    S::stream_snapshot(h, &mut image).expect("stream");
    r.stream_idle_ms = t.elapsed().as_secs_f64() * 1e3;
    r.bytes = image.len() as u64;
    assert_eq!(
        live_bytes, r.bytes,
        "live stream wrote a different image size"
    );
    let _ = p;

    // writes after the handle is gone
    let t = Instant::now();
    for i in 0..a.writes {
        write(&mut sm, i % a.keys, &mut pos, &mut out, &mut frame);
    }
    r.after_ns = t.elapsed().as_nanos() as f64 / a.writes as f64;

    // restore into a fresh SM, tagged one frame past the image's cursor
    let mut fresh = S::default();
    let t = Instant::now();
    let got = fresh
        .install_snapshot(pos, &mut std::io::Cursor::new(&image))
        .expect("install");
    r.restore_ms = t.elapsed().as_secs_f64() * 1e3;
    assert_eq!(got, pos);

    println!(
        "SNAP-JSON {{\"arm\":\"{name}\",\"keys\":{},\"value_bytes\":{},\"fill_ns\":{:.0},\"steady_ns\":{:.0},\"steady_max_ns\":{},\"freeze_ns\":{},\"stream_idle_ms\":{:.2},\"bytes\":{},\"live_write_ns\":{:.0},\"live_write_max_ns\":{},\"live_writes\":{},\"stream_live_ms\":{:.2},\"after_ns\":{:.0},\"restore_ms\":{:.2},\"hwm_delta_kb\":{}}}",
        a.keys,
        a.value_bytes,
        r.fill_ns,
        r.steady_ns,
        r.steady_max_ns,
        r.freeze_ns,
        r.stream_idle_ms,
        r.bytes,
        r.live_write_ns,
        r.live_write_max_ns,
        r.live_writes,
        r.stream_live_ms,
        r.after_ns,
        r.restore_ms,
        r.hwm_delta_kb.map_or("null".to_string(), |v| v.to_string())
    );
    r
}

fn main() {
    let a = Args::parse();
    for rep in 0..a.reps {
        for arm in a.arms.split(',') {
            eprintln!("rep {rep} arm {arm}");
            match arm {
                "ordmap" => {
                    run_arm::<KvSm>(&a, "ordmap");
                }
                "btree" => {
                    run_arm::<KvBTree>(&a, "btree");
                }
                "hash" => {
                    run_arm::<KvHash>(&a, "hash");
                }
                other => panic!("unknown arm {other}"),
            }
        }
    }
}
