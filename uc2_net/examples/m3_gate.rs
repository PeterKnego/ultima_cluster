// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M3 gate: static-leader commit pipeline (spec §9 GO/NO-GO: >= 400k
//! committed/s, p50 <= 1 ms, fsync on, 3 nodes).
//!
//! Local (single host, loopback, all three nodes in-process):
//!   cargo run -p uc2_net --release --example m3_gate -- local <journal_root> \
//!       [secs=10] [payload=64] [admission_mib=4] [buffer_mib=256]
//!
//! Fleet (one process per host; start followers first):
//!   m3_gate follower <bind_addr> <journal_dir> <leader_addr> [buffer_mib]
//!   m3_gate leader <bind_addr> <journal_dir> <f1_addr> <f2_addr> \
//!       [secs=10] [payload=64] [admission_mib=4] [buffer_mib=256]
//!
//! Journal dirs MUST be on a real filesystem (dev sandbox: /home/claude/...,
//! NEVER /tmp — RAM-backed tmpfs). UC2_M3_MAX_BYTES caps the appended stream.
//!
//! MEASUREMENT: committed/s = messages / ONE wall clock around load + drain
//! (drain = leader commit reaches the stream end — every counted message is
//! quorum-fsync'd; the M1 accounting lesson). Commit latency is sampled every
//! SAMPLE_EVERY appends: (position, Instant) pairs resolved when the commit
//! counter passes them; p50/p99/max over the samples. ADMISSION CONTROL is a
//! position window vs commit (spec §7): append stalls when
//! append - commit > admission budget — leader-local counters only, so it
//! works identically cross-host (this closes M2's fleet sent-pacing wedge).

use std::collections::VecDeque;
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::Ordering;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use uc2_log::agent::{AgentRunner, IdleStrategy};
use uc2_log::archive::{Archive, ArchiveConfig};
use uc2_log::buffer::{AppendError, Appender, LogBuffer};
use uc2_log::counters::LogCounters;
use uc2_log::region::Region;
use uc2_net::fault::FaultSocket;
use uc2_net::receiver::{FollowerConfig, FollowerReceiver, FollowerStats, LeaderReceiver};
use uc2_net::sender::{Sender, SenderConfig, SenderStats};

const TERM: u32 = 1;
const MAX_PAYLOAD: usize = 1024;
const SAMPLE_EVERY: u64 = 1024;

fn buffer(mib: usize) -> Arc<LogBuffer> {
    let counters = Arc::new(LogCounters::new());
    Arc::new(LogBuffer::new(Region::heap_zeroed(mib << 20), counters, MAX_PAYLOAD))
}

fn archive_agent(name: &str, b: &Arc<LogBuffer>, dir: &str) -> AgentRunner {
    std::fs::create_dir_all(dir).unwrap();
    let mut archive = Archive::open(ArchiveConfig::new(dir)).unwrap();
    let b = Arc::clone(b);
    AgentRunner::spawn(name, IdleStrategy::BusySpin, move || {
        archive.do_work(&b).expect("archive fail-stop")
    })
    .unwrap()
}

fn follower_node(
    name: &str,
    sock: FaultSocket,
    leader: SocketAddr,
    journal_dir: &str,
    buffer_mib: usize,
) -> (Arc<LogBuffer>, Arc<FollowerStats>, Vec<AgentRunner>) {
    let b = buffer(buffer_mib);
    let cfg = FollowerConfig::new(TERM, leader);
    let mut rx = FollowerReceiver::new(Arc::clone(&b), sock, cfg);
    let stats = rx.stats();
    let rxa =
        AgentRunner::spawn(&format!("{name}-rx"), IdleStrategy::BusySpin, move || rx.do_work())
            .unwrap();
    let ara = archive_agent(&format!("{name}-ar"), &b, journal_dir);
    (b, stats, vec![rxa, ara])
}

fn leader_node(
    raw: UdpSocket,
    followers: Vec<SocketAddr>,
    journal_dir: &str,
    buffer_mib: usize,
) -> (Arc<LogBuffer>, Arc<SenderStats>, Vec<AgentRunner>) {
    let b = buffer(buffer_mib);
    let recv = raw.try_clone().unwrap();
    let send = FaultSocket::from_socket(raw).unwrap();
    let (tx, rx) = mpsc::sync_channel(4096);
    let mut sender = Sender::new(Arc::clone(&b), send, followers, 3, rx, SenderConfig::new(TERM));
    let stats = sender.stats();
    let txa = AgentRunner::spawn("leader-tx", IdleStrategy::BusySpin, move || sender.do_work())
        .unwrap();
    let mut lr = LeaderReceiver::new(recv, tx, TERM).unwrap();
    let lra =
        AgentRunner::spawn("leader-ctrl", IdleStrategy::BusySpin, move || lr.do_work()).unwrap();
    let ara = archive_agent("leader-ar", &b, journal_dir);
    (b, stats, vec![txa, lra, ara])
}

fn max_bytes_cap() -> u64 {
    std::env::var("UC2_M3_MAX_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&v| v > 0)
        .unwrap_or(u64::MAX)
}

struct LoadResult {
    end: u64,
    msgs: u64,
    latencies_ns: Vec<u64>,
}

/// Append until `secs` elapse (on the shared clock) or the byte cap, pacing
/// by the ADMISSION WINDOW: append - commit <= budget (spec §7). Samples
/// commit latency every SAMPLE_EVERY appends.
fn drive_load(lb: &Arc<LogBuffer>, secs: u64, payload: usize, budget: u64, clock: Instant) -> LoadResult {
    let body = vec![0u8; payload];
    let mut a = Appender::new(Arc::clone(lb), TERM);
    let max_bytes = max_bytes_cap();
    let mut msgs = 0u64;
    let mut pending: VecDeque<(u64, Instant)> = VecDeque::new();
    let mut latencies_ns: Vec<u64> = Vec::with_capacity(1 << 20);
    let drain = |pending: &mut VecDeque<(u64, Instant)>, lat: &mut Vec<u64>, commit: u64| {
        while pending.front().is_some_and(|&(p, _)| p <= commit) {
            let (_, t) = pending.pop_front().unwrap();
            lat.push(t.elapsed().as_nanos() as u64);
        }
    };
    while clock.elapsed().as_secs() < secs && a.position() < max_bytes {
        match a.append(1, msgs, &body) {
            Ok(_) => {
                msgs += 1;
                if msgs.is_multiple_of(SAMPLE_EVERY) {
                    pending.push_back((a.position(), Instant::now()));
                }
            }
            Err(AppendError::WouldOverrun) => std::thread::yield_now(),
            Err(e) => panic!("{e}"),
        }
        // admission window vs commit (leader-local; works cross-host)
        loop {
            let commit = lb.counters().commit.load_acquire();
            drain(&mut pending, &mut latencies_ns, commit);
            if a.position() - commit <= budget {
                break;
            }
            std::thread::yield_now();
        }
    }
    let end = a.position();
    // commit drain: every appended byte quorum-fsync'd before the clock stops
    let t = Instant::now();
    loop {
        let commit = lb.counters().commit.load_acquire();
        drain(&mut pending, &mut latencies_ns, commit);
        if commit >= end {
            break;
        }
        assert!(t.elapsed() < Duration::from_secs(300), "commit drain stuck at {commit} < {end}");
        std::thread::yield_now();
    }
    LoadResult { end, msgs, latencies_ns }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("local") => local(&args[1..]),
        Some("leader") => leader_role(&args[1..]),
        Some("follower") => follower_role(&args[1..]),
        _ => {
            eprintln!("usage: m3_gate local|leader|follower ... (see file header)");
            std::process::exit(2);
        }
    }
}

fn local(args: &[String]) {
    let root = args.first().expect("usage: m3_gate local <journal_root> ...").clone();
    let secs: u64 = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(10);
    let payload: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(64);
    let admission_mib: u64 = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(4);
    let buffer_mib: usize = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(256);

    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1s = FaultSocket::bind("127.0.0.1:0").unwrap();
    let f2s = FaultSocket::bind("127.0.0.1:0").unwrap();
    let (a1, a2) = (f1s.local_addr().unwrap(), f2s.local_addr().unwrap());
    let (f1b, _f1st, f1a) =
        follower_node("f1", f1s, leader_addr, &format!("{root}/f1"), buffer_mib);
    let (f2b, _f2st, f2a) =
        follower_node("f2", f2s, leader_addr, &format!("{root}/f2"), buffer_mib);
    let (lb, lst, la) = leader_node(raw, vec![a1, a2], &format!("{root}/leader"), buffer_mib);

    println!("== uc2 M3 gate (local loopback) ==");
    println!(
        "payload {payload} B, admission {admission_mib} MiB, buffers {buffer_mib} MiB x3, {secs} s"
    );

    let (p, pl) = (Arc::clone(&lb), Arc::clone(&f1b));
    let progress_start = Instant::now();
    let printer = AgentRunner::spawn("printer", IdleStrategy::Sleep(Duration::from_secs(1)), {
        let mut last = (0u64, 0u64);
        move || {
            let now = (p.counters().commit.load_acquire(), pl.counters().durable.load_acquire());
            println!(
                "t={:>3}s  commit +{:>6.1} MB/s  f1 durable +{:>6.1} MB/s  inflight {:>9} B",
                progress_start.elapsed().as_secs(),
                (now.0 - last.0) as f64 / 1e6,
                (now.1 - last.1) as f64 / 1e6,
                p.counters().append.load_acquire() - now.0,
            );
            last = now;
            false
        }
    })
    .unwrap();

    let clock = Instant::now();
    let mut res = drive_load(&lb, secs, payload, admission_mib << 20, clock);
    let full = clock.elapsed().as_secs_f64();
    printer.stop();

    res.latencies_ns.sort_unstable();
    let (p50, p99, pmax) = (
        percentile(&res.latencies_ns, 0.50),
        percentile(&res.latencies_ns, 0.99),
        res.latencies_ns.last().copied().unwrap_or(0),
    );
    let committed_per_s = res.msgs as f64 / full;
    use Ordering::Relaxed as R;
    println!("== uc2 M3 gate ==");
    println!(
        "stream               {} B ({} msgs) committed in {full:.2} s (drain-inclusive)",
        res.end, res.msgs
    );
    println!("committed/s          {committed_per_s:>9.0}");
    println!(
        "commit latency       p50 {:.3} ms  p99 {:.3} ms  max {:.3} ms  ({} samples)",
        p50 as f64 / 1e6,
        p99 as f64 / 1e6,
        pmax as f64 / 1e6,
        res.latencies_ns.len()
    );
    println!(
        "sender               dgrams {}  commit_gossips {}  flow_stalls {}  overruns {}",
        lst.datagrams.load(R),
        lst.commit_gossips.load(R),
        lst.flow_stalls.load(R),
        lst.overruns.load(R),
    );
    let pass = committed_per_s >= 400_000.0 && p50 as f64 / 1e6 <= 1.0 && lst.overruns.load(R) == 0;
    println!(
        "GATE (>=400k committed/s, p50 <= 1 ms, fsync on): {}",
        if pass { "PASS" } else { "FAIL" }
    );
    for a in f1a.into_iter().chain(f2a).chain(la) {
        a.stop();
    }
    let _ = (f1b, f2b);
    if !pass {
        std::process::exit(1);
    }
}

/// Fleet follower: runs until killed, printing durable/commit progress.
fn follower_role(args: &[String]) {
    let bind = args.first().expect("bind addr");
    let journal = args.get(1).expect("journal dir");
    let leader: SocketAddr = args.get(2).expect("leader addr").parse().unwrap();
    let buffer_mib: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(256);
    let sock = FaultSocket::bind(bind.as_str()).unwrap();
    let (b, st, _agents) = follower_node("follower", sock, leader, journal, buffer_mib);
    let mut last = 0u64;
    loop {
        std::thread::sleep(Duration::from_secs(1));
        let c = b.counters();
        let d = c.durable.load_acquire();
        println!(
            "durable {:>7.1} MB/s (at {d})  commit {}  ap_sent {}  naks {}",
            (d - last) as f64 / 1e6,
            c.commit.load_acquire(),
            st.append_positions_sent.load(Ordering::Relaxed),
            st.naks_sent.load(Ordering::Relaxed),
        );
        last = d;
    }
}

/// Fleet leader: identical measurement to local mode — commit and the
/// latency samples are leader-local, so the cross-host numbers are the real
/// gate numbers (unlike M2, nothing here needs remote counters).
fn leader_role(args: &[String]) {
    let bind = args.first().expect("bind addr");
    let journal = args.get(1).expect("journal dir");
    let f1: SocketAddr = args.get(2).expect("f1 addr").parse().unwrap();
    let f2: SocketAddr = args.get(3).expect("f2 addr").parse().unwrap();
    let secs: u64 = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(10);
    let payload: usize = args.get(5).map(|s| s.parse().unwrap()).unwrap_or(64);
    let admission_mib: u64 = args.get(6).map(|s| s.parse().unwrap()).unwrap_or(4);
    let buffer_mib: usize = args.get(7).map(|s| s.parse().unwrap()).unwrap_or(256);
    let raw = UdpSocket::bind(bind.as_str()).unwrap();
    let (lb, lst, agents) = leader_node(raw, vec![f1, f2], journal, buffer_mib);
    let clock = Instant::now();
    let mut res = drive_load(&lb, secs, payload, admission_mib << 20, clock);
    let full = clock.elapsed().as_secs_f64();
    res.latencies_ns.sort_unstable();
    let (p50, p99) =
        (percentile(&res.latencies_ns, 0.50), percentile(&res.latencies_ns, 0.99));
    let committed_per_s = res.msgs as f64 / full;
    use Ordering::Relaxed as R;
    println!(
        "leader: {} msgs committed in {full:.2} s = {committed_per_s:.0}/s; p50 {:.3} ms p99 {:.3} ms; gossips {} overruns {}",
        res.msgs,
        p50 as f64 / 1e6,
        p99 as f64 / 1e6,
        lst.commit_gossips.load(R),
        lst.overruns.load(R),
    );
    println!(
        "GATE (>=400k committed/s, p50 <= 1 ms): {}",
        if committed_per_s >= 400_000.0 && p50 as f64 / 1e6 <= 1.0 { "PASS" } else { "FAIL" }
    );
    for a in agents {
        a.stop();
    }
}
