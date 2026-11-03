//! netfault scenarios: three REAL node processes on one host under one-kind,
//! one-link, one-direction network faults from `testing/netfault`, watched by
//! the WGL checker. Design:
//! `docs/superpowers/specs/2026-11-03-uc2-netfault-scenarios-design.md` §4;
//! plan `docs/superpowers/plans/2026-11-03-uc2-netfault-nightly.md`.
//!
//! Every scenario: boot, attach on `lo`, warm up, workers on, install the
//! rules (always `--out` and `--peer`: on `lo` a datagram passes both hooks),
//! a 5 s fault window, read `stats` and assert the rule FIRED, detach (which
//! heals everything), a 5 s heal window, then safety (linearizable) and the
//! scenario's liveness. The scenarios share one attachment, so they take
//! `NETFAULT_LOCK` and the nightly command runs them with `--test-threads=1`.
#![cfg(feature = "netfault-tests")]

use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use uc_lincheck::history::{Entry, History};

mod common;
use common::*;

const N: usize = 3;
const IPS: [Ipv4Addr; N] = [
    Ipv4Addr::new(127, 0, 0, 2),
    Ipv4Addr::new(127, 0, 0, 3),
    Ipv4Addr::new(127, 0, 0, 4),
];
const FAULT_WINDOW: Duration = Duration::from_secs(5);
const HEAL_WINDOW: Duration = Duration::from_secs(5);
const CATCH_UP: Duration = Duration::from_secs(10);
const N_WORKERS: u32 = 3;
const THROTTLE: Duration = Duration::from_millis(15);

/// A booted three-node cluster, nodes on `127.0.0.2/3/4`, roles known.
#[allow(dead_code, reason = "kill/restart (Scenario B) land in the next task")]
struct Cluster {
    _tmp: tempfile::TempDir,
    dirs: Arc<Vec<PathBuf>>,
    addrs: Vec<SocketAddr>,
    members_str: String,
    node_procs: Vec<Option<Reap>>,
    svc_procs: Vec<Option<Reap>>,
    crypto: Option<CrashtestCrypto>,
    crypto_on: bool,
    roles: Roles,
}

impl Cluster {
    /// The exact spawn sequence of `leader_node_sigkill_recovery_multi_once`
    /// (hard_crash.rs), on distinct loopback addresses.
    fn boot() -> Cluster {
        let tmp = tempdir();
        let addrs: Vec<SocketAddr> = IPS.iter().map(|ip| free_addr_on(*ip)).collect();
        let members: Vec<(u32, SocketAddr)> =
            (0..N as u32).map(|i| (i, addrs[i as usize])).collect();
        let members_str = members_arg(&members);
        let crypto_on = crypto_from_env();
        let crypto =
            crypto_on.then(|| provision_crypto(tmp.path(), &(0..N as u32).collect::<Vec<_>>()));
        let mut dirs: Vec<PathBuf> = Vec::with_capacity(N);
        let mut node_procs: Vec<Option<Reap>> = Vec::with_capacity(N);
        for i in 0..N as u32 {
            let d = tmp.path().join(format!("n{i}"));
            std::fs::create_dir_all(&d).unwrap();
            node_procs.push(Some(spawn_node_multi(
                &d,
                i,
                addrs[i as usize],
                &members_str,
                crypto_args(&crypto, i),
            )));
            dirs.push(d);
        }
        for d in &dirs {
            wait_for_ready(d, Duration::from_secs(10));
        }
        let svc_procs: Vec<Option<Reap>> = dirs.iter().map(|d| Some(spawn_service(d))).collect();
        let roles = roles(&dirs, 30);
        if crypto_on {
            assert_crypto_epoch_active(&dirs[roles.leader], Duration::from_secs(10));
        }
        eprintln!(
            "[netfault] leader=n{} f1=n{} f2=n{} crypto={crypto_on}",
            roles.leader, roles.f1, roles.f2
        );
        Cluster {
            _tmp: tmp,
            dirs: Arc::new(dirs),
            addrs,
            members_str,
            node_procs,
            svc_procs,
            crypto,
            crypto_on,
            roles,
        }
    }

    fn port(&self, idx: usize) -> String {
        self.addrs[idx].port().to_string()
    }

    fn ip(&self, idx: usize) -> String {
        self.addrs[idx].ip().to_string()
    }

    fn workload(&self, seed: u64) -> Workload {
        let history = Arc::new(History::default());
        let last_seen = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        warmup_write_multi(&self.dirs, &history, &last_seen);
        let handles = spawn_workers_multi(
            &self.dirs, &history, &stop, &last_seen, seed, THROTTLE, N_WORKERS,
        );
        Workload {
            history,
            stop,
            handles,
        }
    }
}

fn crypto_args(crypto: &Option<CrashtestCrypto>, id: u32) -> Option<(&Path, &Path)> {
    crypto
        .as_ref()
        .map(|m| (m.key_paths[&id].as_path(), m.allowlist_path.as_path()))
}

/// The shmem workers of `hard_crash.rs`, recording one history.
struct Workload {
    history: Arc<History>,
    stop: Arc<AtomicBool>,
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl Workload {
    fn ok(&self) -> usize {
        History::ok_count(&self.history.snapshot())
    }

    fn finish(self) -> Vec<Entry> {
        self.stop.store(true, Ordering::Relaxed);
        join_workers(self.handles);
        Arc::try_unwrap(self.history)
            .ok()
            .expect("sole history owner")
            .into_entries()
    }
}

/// Polls `pred` every 50 ms; panics with `msg` once `timeout` has passed.
fn wait_until(timeout: Duration, mut pred: impl FnMut() -> bool, msg: &str) {
    let deadline = Instant::now() + timeout;
    while !pred() {
        assert!(Instant::now() < deadline, "{msg}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ----------------------------------------------------------------- Scenario A

/// Spec §4 A: every NAK F1 sends toward L is dropped, and one DATA datagram
/// in ten from L to F1 is lost, so F1 detects loss it can never repair by
/// NAK. L and F2 are a quorum, so commit keeps advancing; after heal F1's
/// durable position must reach L's commit-at-heal within 10 s.
#[test]
fn nak_loss_on_one_link() {
    let _g = NETFAULT_LOCK.lock().unwrap();
    shorten_client_timeout();
    let c = Cluster::boot();
    let nf = Netfault::attach("lo");
    let w = c.workload(0x4E41_4B01);
    let (l, f1) = (c.roles.leader, c.roles.f1);
    let nak = format!("{} nak peer={} out", c.port(f1), c.ip(l));
    let data = format!("{} data peer={} out", c.port(l), c.ip(f1));
    nf.run(&[
        "block",
        "--out",
        "--kind",
        "nak",
        "--peer",
        &c.ip(l),
        &c.port(f1),
    ]);
    nf.run(&[
        "rule",
        "--out",
        "--kind",
        "data",
        "--loss",
        "10",
        "--peer",
        &c.ip(f1),
        &c.port(l),
    ]);
    let ok_at_fault = w.ok();
    std::thread::sleep(FAULT_WINDOW);
    let ok_in_fault = w.ok();
    let (commit_at_heal, _) = cnc_positions(&c.dirs[l]);
    let (nak_dropped, data_dropped) = (nf.counter(&nak, "dropped"), nf.counter(&data, "dropped"));
    eprintln!(
        "[nak_loss_on_one_link] nak_dropped={nak_dropped} data_dropped={data_dropped} \
         ok_at_fault={ok_at_fault} ok_in_fault={ok_in_fault} commit_at_heal={commit_at_heal}"
    );
    assert!(nak_dropped > 0, "the NAK rule counted nothing: {nak}");
    assert!(
        data_dropped > 0,
        "the DATA loss rule counted nothing: {data}"
    );
    drop(nf); // detach = heal
    assert!(
        ok_in_fault > ok_at_fault,
        "commit did not advance during the fault (L+F2 are a quorum)"
    );
    wait_until(
        CATCH_UP,
        || cnc_positions(&c.dirs[f1]).1 >= commit_at_heal,
        "F1's durable position did not reach L's commit-at-heal within 10 s of heal",
    );
    std::thread::sleep(HEAL_WINDOW);
    let entries = w.finish();
    eprintln!(
        "[nak_loss_on_one_link] ops={} ok={}",
        entries.len(),
        History::ok_count(&entries)
    );
    assert_linearizable(&entries, "netfault_nak_loss", "run0");
}
