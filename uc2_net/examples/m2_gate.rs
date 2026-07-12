// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M2 gate: replication stream throughput (spec §9: >= 100 MB/s per
//! follower, durable positions keeping pace, resilient to 0.1-1% loss).
//!
//! Local (single host, loopback, all three nodes in-process):
//!   cargo run -p uc2_net --release --example m2_gate -- local <journal_root> \
//!       [secs=10] [payload=64] [loss_ppm=0] [buffer_mib=256]
//!
//! Fleet (one process per host; start followers first):
//!   m2_gate follower <bind_addr> <journal_dir> <leader_addr> [buffer_mib]
//!   m2_gate leader <bind_addr> <journal_dir> <f1_addr> <f2_addr> \
//!       [secs=10] [payload=64] [loss_ppm=0] [buffer_mib=256]
//!
//! Journal dirs MUST be on a real filesystem (on the dev sandbox:
//! /home/claude/..., NEVER /tmp — RAM-backed tmpfs). Buffers are heap.
//! UC2_M2_MAX_BYTES caps the appended stream (bounded runs on small disks).
//!
//! Headline = drain-inclusive durable rate: ONE wall clock around load +
//! drain (every byte fsync'd on every node before the clock stops) — the M1
//! gate's accounting lesson (docs/benchmarks/uc2-m1-gate-2026-07-09.md).

use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use uc2_log::agent::{AgentRunner, IdleStrategy};
use uc2_log::archive::{Archive, ArchiveConfig};
use uc2_log::buffer::{AppendError, Appender, LogBuffer};
use uc2_log::cnc::{CncMeta, CncPage};
use uc2_log::region::Region;
use uc2_net::fault::{FaultConfig, FaultSocket};
use uc2_net::receiver::{FollowerConfig, FollowerReceiver, FollowerStats, LeaderReceiver};
use uc2_net::sender::{Sender, SenderConfig, SenderStats};

const TERM: u32 = 1;
const MAX_PAYLOAD: usize = 1024;

fn buffer(mib: usize) -> Arc<LogBuffer> {
    let cnc = CncPage::heap(&CncMeta {
        node_id: 0,
        instance_id: 0,
        app_id: "test".into(),
        buffer_bytes: (mib as u64) << 20,
        max_payload: MAX_PAYLOAD as u32,
    });
    Arc::new(LogBuffer::new(Region::heap_zeroed(mib << 20), cnc, MAX_PAYLOAD))
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
    let cfg = FollowerConfig::new(leader);
    let mut rx =
        FollowerReceiver::new(Arc::clone(&b), sock, cfg, Arc::new(AtomicU32::new(TERM)));
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
    loss_ppm: u32,
    buffer_mib: usize,
) -> (Arc<LogBuffer>, Arc<SenderStats>, Vec<AgentRunner>) {
    let b = buffer(buffer_mib);
    let recv = raw.try_clone().unwrap();
    let mut send = FaultSocket::from_socket(raw).unwrap();
    send.set_faults(FaultConfig {
        seed: 20_260_710,
        drop_per_million: loss_ppm,
        ..Default::default()
    });
    let (tx, rx) = mpsc::sync_channel(4096);
    let term = Arc::new(AtomicU32::new(TERM));
    let mut sender = Sender::new(
        Arc::clone(&b),
        send,
        followers,
        3,
        rx,
        SenderConfig::new(TERM),
        Arc::clone(&term),
    );
    let stats = sender.stats();
    let txa = AgentRunner::spawn("leader-tx", IdleStrategy::BusySpin, move || sender.do_work())
        .unwrap();
    let mut lr = LeaderReceiver::new(recv, tx, term).unwrap();
    let lra =
        AgentRunner::spawn("leader-ctrl", IdleStrategy::BusySpin, move || lr.do_work()).unwrap();
    let ara = archive_agent("leader-ar", &b, journal_dir);
    (b, stats, vec![txa, lra, ara])
}

fn max_bytes_cap() -> u64 {
    std::env::var("UC2_M2_MAX_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&v| v > 0)
        .unwrap_or(u64::MAX)
}

/// Pacing floor for the appender. Task 9 carry-fix: in `local` mode the
/// follower log buffers are in-process, so we pace the leader's append against
/// the SLOWEST live follower's DURABLE frontier — never letting the leader's
/// ring tail scroll above where a follower might still NAK. (The old
/// `sent`-based floor let `sent` reach `follower_durable + CAP`, so `append`
/// climbed to `follower_durable + CAP + cap/2`; the ring bottom was then
/// `cap/2` ABOVE `follower_durable`, and a single dropped datagram left a NAK
/// gap that had already scrolled out of the leader's ring → `serve_nak`
/// Overrun → permanent wedge with no replay session until M4.)
///
/// On the fleet the leader cannot see follower counters cross-host, so the
/// `leader` role passes `&[]` here and falls back to `sent`; real fleet loss
/// runs must therefore use generous buffers (admission-control-vs-commit that
/// closes this gap lands in M3).
fn pace_floor(lb: &Arc<LogBuffer>, followers: &[Arc<LogBuffer>]) -> u64 {
    if followers.is_empty() {
        lb.counters().sent.load_acquire()
    } else {
        followers.iter().map(|f| f.counters().durable.load_acquire()).min().unwrap()
    }
}

/// Append until `secs` elapse (measured on the shared clock) or the byte cap
/// is hit, pacing against `pace_floor` (never build more than half a buffer of
/// backlog — the M2 stand-in for admission control). Returns (end, msgs).
fn drive_load(
    lb: &Arc<LogBuffer>,
    followers: &[Arc<LogBuffer>],
    secs: u64,
    payload: usize,
    clock: Instant,
) -> (u64, u64) {
    let cap = lb.capacity();
    let max_bytes = max_bytes_cap();
    let body = vec![0u8; payload];
    let mut a = Appender::new(Arc::clone(lb), TERM);
    let mut msgs = 0u64;
    while clock.elapsed().as_secs() < secs && a.position() < max_bytes {
        match a.append(1, msgs, &body) {
            Ok(_) => msgs += 1,
            Err(AppendError::WouldOverrun) => std::hint::spin_loop(),
            Err(e) => panic!("{e}"),
        }
        while a.position() > pace_floor(lb, followers) + cap / 2 {
            // Yield (not spin) while waiting on a slower consumer: on a
            // core-starved host, spinning here would burn a core the followers
            // need to advance durable. On a non-oversubscribed host this is
            // equivalent (the wait is rare and short).
            std::thread::yield_now();
        }
    }
    (a.position(), msgs)
}

fn await_durable(b: &Arc<LogBuffer>, end: u64, what: &str) {
    let t = Instant::now();
    while b.counters().durable.load_acquire() < end {
        assert!(t.elapsed() < Duration::from_secs(300), "{what} drain stuck");
        std::hint::spin_loop();
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("local") => local(&args[1..]),
        Some("leader") => leader_role(&args[1..]),
        Some("follower") => follower_role(&args[1..]),
        _ => {
            eprintln!("usage: m2_gate local|leader|follower ... (see file header)");
            std::process::exit(2);
        }
    }
}

fn local(args: &[String]) {
    let root = args.first().expect("usage: m2_gate local <journal_root> ...").clone();
    let secs: u64 = args.get(1).map(|s| s.parse().unwrap()).unwrap_or(10);
    let payload: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(64);
    let loss_ppm: u32 = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(0);
    let buffer_mib: usize = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(256);

    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let leader_addr = raw.local_addr().unwrap();
    let f1s = FaultSocket::bind("127.0.0.1:0").unwrap();
    let f2s = FaultSocket::bind("127.0.0.1:0").unwrap();
    let (a1, a2) = (f1s.local_addr().unwrap(), f2s.local_addr().unwrap());
    let (f1b, f1st, f1a) = follower_node("f1", f1s, leader_addr, &format!("{root}/f1"), buffer_mib);
    let (f2b, f2st, f2a) = follower_node("f2", f2s, leader_addr, &format!("{root}/f2"), buffer_mib);
    let (lb, lst, la) =
        leader_node(raw, vec![a1, a2], &format!("{root}/leader"), loss_ppm, buffer_mib);

    println!("== uc2 M2 gate (local loopback) ==");
    println!("payload {payload} B, loss {loss_ppm} ppm, buffers {buffer_mib} MiB x3, {secs} s");

    // per-second progress: instantaneous rebuilt rate per follower
    let (p1, p2) = (Arc::clone(&f1b), Arc::clone(&f2b));
    let progress_start = Instant::now();
    let printer = AgentRunner::spawn("printer", IdleStrategy::Sleep(Duration::from_secs(1)), {
        let mut last = (0u64, 0u64);
        move || {
            let now = (p1.counters().append.load_acquire(), p2.counters().append.load_acquire());
            println!(
                "t={:>3}s  f1 +{:>6.1} MB/s  f2 +{:>6.1} MB/s",
                progress_start.elapsed().as_secs(),
                (now.0 - last.0) as f64 / 1e6,
                (now.1 - last.1) as f64 / 1e6,
            );
            last = now;
            false // idle (sleep 1 s) every cycle
        }
    })
    .unwrap();

    // ONE wall clock around load + drain (drain-inclusive headline)
    let clock = Instant::now();
    let followers = [Arc::clone(&f1b), Arc::clone(&f2b)];
    let (end, msgs) = drive_load(&lb, &followers, secs, payload, clock);
    await_durable(&lb, end, "leader");
    await_durable(&f1b, end, "f1");
    await_durable(&f2b, end, "f2");
    let full = clock.elapsed().as_secs_f64();
    printer.stop();

    let rate_mbs = end as f64 / full / 1e6;
    use Ordering::Relaxed as R;
    println!("== uc2 M2 gate ==");
    println!("stream               {end} B ({msgs} msgs) in {full:.2} s (drain-inclusive)");
    println!("per-follower durable {rate_mbs:>7.1} MB/s   ({:.0} msgs/s)", msgs as f64 / full);
    println!(
        "sender               dgrams {}  naks_served {}  flow_stalls {}  overruns {}  heartbeats {}",
        lst.datagrams.load(R),
        lst.naks_served.load(R),
        lst.flow_stalls.load(R),
        lst.overruns.load(R),
        lst.heartbeats.load(R),
    );
    for (n, st) in [("f1", &f1st), ("f2", &f2st)] {
        println!(
            "  {n}: naks_sent {}  dropped dup {} overrun {} stale {} malformed {}",
            st.naks_sent.load(R),
            st.dropped_dup.load(R),
            st.dropped_overrun.load(R),
            st.dropped_stale_term.load(R),
            st.dropped_malformed.load(R),
        );
    }
    let naks = f1st.naks_sent.load(R) + f2st.naks_sent.load(R);
    let pass = rate_mbs >= 100.0
        && lst.overruns.load(R) == 0
        && (loss_ppm == 0 || (naks > 0 && lst.naks_served.load(R) > 0));
    let loss_note = if loss_ppm > 0 { ", loss recovered via NAK" } else { "" };
    println!("GATE (>=100 MB/s per follower{loss_note}): {}", if pass { "PASS" } else { "FAIL" });

    for a in f1a.into_iter().chain(f2a).chain(la) {
        a.stop();
    }
    if !pass {
        std::process::exit(1);
    }
}

/// Fleet follower: runs until killed, printing rebuilt/durable progress.
/// (Follower counters aren't visible cross-host until cnc lands in M5, so
/// the fleet verdict is read off these consoles.)
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
        let now = c.append.load_acquire();
        println!(
            "rebuilt {:>7.1} MB/s  contiguous {now}  durable_lag {}  naks_sent {}",
            (now - last) as f64 / 1e6,
            now - c.durable.load_acquire(),
            st.naks_sent.load(Ordering::Relaxed),
        );
        last = now;
    }
}

/// Fleet leader: drives the load, drains its OWN durable, prints sender
/// stats, then lingers briefly so followers can NAK the tail before exit.
///
/// NOTE: the leader cannot see follower counters cross-host, so `drive_load`
/// gets `&[]` and paces against `sent` (see `pace_floor`). Under injected loss
/// on the fleet this can wedge if the buffer is too small — use generous
/// buffers until M3's admission-control-vs-commit closes the gap.
fn leader_role(args: &[String]) {
    let bind = args.first().expect("bind addr");
    let journal = args.get(1).expect("journal dir");
    let f1: SocketAddr = args.get(2).expect("f1 addr").parse().unwrap();
    let f2: SocketAddr = args.get(3).expect("f2 addr").parse().unwrap();
    let secs: u64 = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(10);
    let payload: usize = args.get(5).map(|s| s.parse().unwrap()).unwrap_or(64);
    let loss_ppm: u32 = args.get(6).map(|s| s.parse().unwrap()).unwrap_or(0);
    let buffer_mib: usize = args.get(7).map(|s| s.parse().unwrap()).unwrap_or(256);
    let raw = UdpSocket::bind(bind.as_str()).unwrap();
    let (lb, lst, agents) = leader_node(raw, vec![f1, f2], journal, loss_ppm, buffer_mib);
    let clock = Instant::now();
    let (end, msgs) = drive_load(&lb, &[], secs, payload, clock);
    await_durable(&lb, end, "leader");
    let full = clock.elapsed().as_secs_f64();
    use Ordering::Relaxed as R;
    println!("leader: {end} B ({msgs} msgs) appended+durable in {full:.2} s");
    println!(
        "sender: dgrams {}  naks_served {}  flow_stalls {}  overruns {}",
        lst.datagrams.load(R),
        lst.naks_served.load(R),
        lst.flow_stalls.load(R),
        lst.overruns.load(R),
    );
    std::thread::sleep(Duration::from_secs(5)); // tail-NAK settle window
    for a in agents {
        a.stop();
    }
}
