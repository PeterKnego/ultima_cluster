// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M1 gate: solo append+record+fsync throughput (spec §9: >= 1M msgs/s @ 64B).
//!
//! Usage: cargo run -p uc2_log --release --example m1_gate -- <journal_dir> \
//!            [secs=10] [payload=64] [buffer_mib=512] [buffer_path=/dev/shm/uc2-m1-gate.buf]
//!
//! Layout mirrors deployment: buffer file on tmpfs (/dev/shm — no writeback
//! I/O), journal on the real disk (<journal_dir> — put it on NVMe).
//! Appender runs on the main thread (stand-in for the consensus agent);
//! the archive agent busy-spins on its own thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use uc2_log::agent::{AgentRunner, IdleStrategy};
use uc2_log::archive::{Archive, ArchiveConfig};
use uc2_log::buffer::{AppendError, Appender, LogBuffer};
use uc2_log::counters::LogCounters;

fn main() {
    let mut args = std::env::args().skip(1);
    let journal_dir = args
        .next()
        .expect("usage: m1_gate <journal_dir> [secs] [payload] [buffer_mib] [buffer_path]");
    let secs: u64 = args.next().map(|s| s.parse().unwrap()).unwrap_or(10);
    let payload_len: usize = args.next().map(|s| s.parse().unwrap()).unwrap_or(64);
    let buffer_mib: u64 = args.next().map(|s| s.parse().unwrap()).unwrap_or(512);
    let buffer_path = args
        .next()
        .unwrap_or_else(|| "/dev/shm/uc2-m1-gate.buf".to_string());
    // Optional message cap (0/unset = unlimited). Only used to bound the
    // append-only journal's on-disk growth when the gate is run on a small /
    // quota'd scratch filesystem (e.g. a sandbox tmpfs) where a full-duration
    // run would exhaust the disk before the deadline. Unset on the real fleet.
    let max_msgs: u64 = std::env::var("UC2_M1_MAX_MSGS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    std::fs::create_dir_all(&journal_dir).unwrap();
    let counters = Arc::new(LogCounters::new());
    let mut archive = Archive::open(ArchiveConfig::new(&journal_dir)).unwrap();
    counters.prime(archive.recovered_position());
    let buffer = Arc::new(
        LogBuffer::create_file(
            buffer_path.as_ref(),
            buffer_mib * 1024 * 1024,
            Arc::clone(&counters),
            1024 * 1024,
        )
        .unwrap(),
    );
    assert_eq!(
        archive.recovered_position(),
        0,
        "use a fresh journal dir for the gate"
    );

    let blocks = Arc::new(AtomicU64::new(0));
    let blocks_c = Arc::clone(&blocks);
    let buf_c = Arc::clone(&buffer);
    let agent = AgentRunner::spawn("uc2-archive", IdleStrategy::BusySpin, move || {
        match archive.do_work(&buf_c) {
            Ok(true) => {
                blocks_c.store(archive.blocks_recorded(), Ordering::Relaxed);
                true
            }
            Ok(false) => false,
            Err(e) => panic!("archive: {e}"),
        }
    })
    .unwrap();

    let payload = vec![0xa5u8; payload_len];
    let mut appender = Appender::new(Arc::clone(&buffer), 1);
    let start = Instant::now();
    let deadline = start + Duration::from_secs(secs);
    let mut appended = 0u64;
    let mut overruns = 0u64;
    let mut next_report = start + Duration::from_secs(1);
    while Instant::now() < deadline {
        for _ in 0..1024 {
            match appender.append(1, appended, &payload) {
                Ok(_) => appended += 1,
                Err(AppendError::WouldOverrun) => {
                    overruns += 1;
                    std::hint::spin_loop();
                }
                Err(e) => panic!("{e}"),
            }
        }
        let now = Instant::now();
        if now >= next_report {
            next_report = now + Duration::from_secs(1);
            eprintln!(
                "t={:>3}s appended={} durable_lag={}B",
                start.elapsed().as_secs(),
                appended,
                counters.append.load_acquire() - counters.durable.load_acquire(),
            );
        }
        if max_msgs != 0 && appended >= max_msgs {
            break;
        }
    }
    let elapsed = start.elapsed();
    // drain: wait for the archive to catch up, then stop it
    while counters.durable.load_acquire() < counters.append.load_acquire() {
        std::thread::yield_now();
    }
    agent.stop();

    let nblocks = blocks.load(Ordering::Relaxed);
    let bytes = counters.durable.load_acquire();
    let rate = appended as f64 / elapsed.as_secs_f64();
    println!("== uc2 M1 gate ==");
    println!("payload            {payload_len} B  (96 B framed at 64 B)");
    println!("duration           {:.2} s", elapsed.as_secs_f64());
    println!("appended           {appended} msgs");
    println!("rate               {:.0} msgs/s", rate);
    println!(
        "recorded+fsynced   {bytes} B ({:.1} MB/s)",
        bytes as f64 / elapsed.as_secs_f64() / 1e6
    );
    println!(
        "blocks (=fsyncs)   {nblocks} ({:.0}/s, avg {:.0} KiB)",
        nblocks as f64 / elapsed.as_secs_f64(),
        bytes as f64 / nblocks.max(1) as f64 / 1024.0
    );
    println!("overrun stalls     {overruns}");
    println!(
        "GATE (>=1M msgs/s @64B): {}",
        if payload_len == 64 && rate >= 1_000_000.0 {
            "PASS"
        } else {
            "CHECK"
        }
    );
    let _ = std::fs::remove_file(&buffer_path);
}
