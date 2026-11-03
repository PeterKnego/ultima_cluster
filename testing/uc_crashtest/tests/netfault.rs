//! netfault scenarios: three REAL node processes on one host under one-kind,
//! one-link, one-direction network faults from `testing/netfault`, watched by
//! the WGL checker. Design:
//! `docs/superpowers/specs/2026-11-03-uc2-netfault-scenarios-design.md` §4;
//! plan `docs/superpowers/plans/2026-11-03-uc2-netfault-nightly.md`.
//!
//! Every scenario: boot, attach on `lo`, warm up, workers on, install the
//! rules (always `--out` and `--peer`: on `lo` a datagram passes both hooks),
//! a 5 s fault window, read `stats` and record whether the rule FIRED, detach
//! (which heals everything), a 5 s heal window, then — in this order — the
//! history is checked (a `Violation` is always the first line read, spec §7)
//! and only then the recorded fired/liveness checks are asserted. The
//! scenarios share one attachment, so they take the netfault lock and the
//! nightly command runs them with `--test-threads=1`.
#![cfg(feature = "netfault-tests")]

use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use uc_lincheck::history::{Entry, History, Outcome};
use uc_lincheck::model::Op;

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
struct Cluster {
    _tmp: tempfile::TempDir,
    dirs: Arc<Vec<PathBuf>>,
    addrs: Vec<SocketAddr>,
    members_str: String,
    node_procs: Vec<Option<Reap>>,
    svc_procs: Vec<Option<Reap>>,
    crypto: Option<CrashtestCrypto>,
    roles: Roles,
}

impl Cluster {
    /// The exact spawn sequence of `leader_node_sigkill_recovery_multi_once`
    /// (hard_crash.rs), on distinct loopback addresses.
    fn boot() -> Cluster {
        let tmp = tempdir();
        let addrs: Vec<SocketAddr> = IPS.iter().map(|ip| free_addr_on(*ip)).collect();
        // A rule's key is (peer, port): two nodes on one port number would
        // over-match each other's rules.
        for i in 0..N {
            for j in (i + 1)..N {
                assert_ne!(
                    addrs[i].port(),
                    addrs[j].port(),
                    "nodes {i} and {j} share a port; re-run"
                );
            }
        }
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
            roles,
        }
    }

    fn port(&self, idx: usize) -> String {
        self.addrs[idx].port().to_string()
    }

    fn ip(&self, idx: usize) -> String {
        self.addrs[idx].ip().to_string()
    }

    /// SIGKILLs node `idx` and then its service (node first, then service:
    /// the external-supervisor contract of `node_sigkill_recovery`).
    fn kill_node_and_service(&mut self, idx: usize) {
        self.node_procs[idx] = None;
        self.svc_procs[idx] = None;
    }

    /// Respawns node `idx` on its old address, waits for a fresh cnc
    /// instance, then its service.
    fn restart(&mut self, idx: usize) {
        let old_id = open_cnc(&self.dirs[idx]).and_then(|c| c.try_instance_id());
        self.node_procs[idx] = Some(spawn_node_multi(
            &self.dirs[idx],
            idx as u32,
            self.addrs[idx],
            &self.members_str,
            crypto_args(&self.crypto, idx as u32),
        ));
        wait_for_fresh_cnc_instance(&self.dirs[idx], old_id, Duration::from_secs(10));
        self.svc_procs[idx] = Some(spawn_service(&self.dirs[idx]));
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

/// The shmem workers of `hard_crash.rs`, recording one history. Dropping it
/// stops the workers, so a panicking scenario does not leave them running
/// into the next one.
struct Workload {
    history: Arc<History>,
    stop: Arc<AtomicBool>,
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl Workload {
    fn ok(&self) -> usize {
        History::ok_count(&self.history.snapshot())
    }

    /// Linearizable reads that completed `Ok` so far.
    fn ok_reads(&self) -> usize {
        self.history
            .snapshot()
            .iter()
            .filter(|e| matches!(e.op, Op::Read) && matches!(e.outcome, Outcome::Ok(_)))
            .count()
    }

    fn finish(mut self) -> Vec<Entry> {
        self.stop.store(true, Ordering::Relaxed);
        join_workers(std::mem::take(&mut self.handles));
        let history = Arc::clone(&self.history);
        drop(self);
        Arc::try_unwrap(history)
            .ok()
            .expect("sole history owner")
            .into_entries()
    }
}

impl Drop for Workload {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// The fired and liveness checks of a scenario, recorded as they happen and
/// asserted only AFTER the history has been checked, so a checker
/// `Violation` is never hidden behind a missed counter or a timeout.
#[derive(Default)]
struct Checks(Vec<String>);

impl Checks {
    fn that(&mut self, ok: bool, msg: impl Into<String>) {
        if !ok {
            self.0.push(msg.into());
        }
    }

    fn assert_all(self, scenario: &str) {
        assert!(
            self.0.is_empty(),
            "[{scenario}] {} check(s) failed (history was linearizable):\n  {}",
            self.0.len(),
            self.0.join("\n  ")
        );
    }
}

/// Polls `pred` every 50 ms until it holds or `timeout` passes.
fn wait_until(timeout: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while !pred() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    true
}

/// The scenario's tail, in the order spec §7 ranks a red: detach already
/// done, heal window, history checked, then the recorded checks.
fn conclude(w: Workload, ck: Checks, scenario: &str) {
    std::thread::sleep(HEAL_WINDOW);
    let entries = w.finish();
    eprintln!(
        "[{scenario}] ops={} ok={}",
        entries.len(),
        History::ok_count(&entries)
    );
    assert_linearizable(&entries, &format!("netfault_{scenario}"), "run0");
    ck.assert_all(scenario);
}

// ----------------------------------------------------------------- Scenario A

/// Spec §4 A: every NAK F1 sends toward L is dropped, and one DATA datagram
/// in ten from L to F1 is lost, so F1 detects loss it can never repair by
/// NAK. L and F2 are a quorum, so commit keeps advancing; after heal F1's
/// durable position must reach L's commit-at-heal within 10 s.
#[test]
fn nak_loss_on_one_link() {
    let _g = netfault_lock();
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
    let mut ck = Checks::default();
    ck.that(
        nak_dropped > 0,
        format!("the NAK rule counted nothing: {nak}"),
    );
    ck.that(
        data_dropped > 0,
        format!("the DATA loss rule counted nothing: {data}"),
    );
    ck.that(
        c.roles.leader == roles(&c.dirs, 5).leader,
        "the leader changed during the fault window: the rules named a stale topology",
    );
    drop(nf); // detach = heal
    ck.that(
        ok_in_fault > ok_at_fault,
        "commit did not advance during the fault (L+F2 are a quorum)",
    );
    ck.that(
        wait_until(CATCH_UP, || cnc_positions(&c.dirs[f1]).1 >= commit_at_heal),
        "F1's durable position did not reach L's commit-at-heal within 10 s of heal",
    );
    conclude(w, ck, "nak_loss_on_one_link");
}

// ----------------------------------------------------------------- Scenario B

/// Spec §4 B: F1's VOTE datagrams never leave (toward F2 or L); everything
/// else does, and votes ARRIVING at F1 are untouched. Then L is SIGKILLed.
/// F2 as candidate needs F1's vote, which cannot arrive; F1 as candidate
/// needs F2's, which can — so the cluster must elect, and must elect F1.
///
/// Whether the block is EXERCISED is a race between the two followers'
/// election timers: F1 casts a vote toward F2 only if F2 campaigns. So a
/// 1 ms delay rule counts F2's REQUEST_VOTEs toward F1, and the exact claim
/// is: every one of them was answered (a grant or a rejection, both VOTE
/// datagrams — `uc_consensus::election::handle_request_vote`) and every
/// answer was dropped. When F2 never asked, the scenario says so.
///
/// One precondition the kill cannot guarantee: if it lands between L's
/// sends to the two followers and F2 holds more bytes than F1, F2 refuses
/// F1's request (votes rank on `(last_term, last_durable)`) and F1's grants
/// are blocked, so no election can complete until heal. That is the
/// protocol behaving, not a defect, and a timeout names it.
#[test]
fn votes_silenced() {
    let _g = netfault_lock();
    shorten_client_timeout();
    let mut c = Cluster::boot();
    let nf = Netfault::attach("lo");
    let w = c.workload(0x564F_5445);
    let (l, f1, f2) = (c.roles.leader, c.roles.f1, c.roles.f2);
    let vote_f2 = format!("{} vote peer={} out", c.port(f1), c.ip(f2));
    let asks = format!("{} request-vote peer={} out", c.port(f2), c.ip(f1));
    nf.run(&[
        "block",
        "--out",
        "--kind",
        "vote",
        "--peer",
        &c.ip(f2),
        &c.port(f1),
    ]);
    nf.run(&[
        "block",
        "--out",
        "--kind",
        "vote",
        "--peer",
        &c.ip(l),
        &c.port(f1),
    ]);
    nf.run(&[
        "rule",
        "--out",
        "--kind",
        "request-vote",
        "--delay",
        "1",
        "--peer",
        &c.ip(f1),
        &c.port(f2),
    ]);
    // The rules are live when bpftool returns; a beat of load under them
    // before the kill.
    std::thread::sleep(Duration::from_millis(300));
    c.kill_node_and_service(l);
    let election = try_await_single_leader_among(&c.dirs, &[f1, f2], 10);
    let (vote_dropped, f2_asked) = (
        nf.counter(&vote_f2, "dropped"),
        nf.counter(&asks, "delayed"),
    );
    let (_, f1_durable) = cnc_positions(&c.dirs[f1]);
    let (_, f2_durable) = cnc_positions(&c.dirs[f2]);
    eprintln!(
        "[votes_silenced] killed=n{l} election={election:?} (f1=n{f1} f2=n{f2}) f2_asked={f2_asked} \
         vote_dropped={vote_dropped} durable f1={f1_durable} f2={f2_durable}"
    );
    let mut ck = Checks::default();
    if f2_asked > 0 {
        ck.that(
            vote_dropped > 0,
            format!(
                "F2 asked F1 for its vote {f2_asked} time(s) and no answer was dropped: {vote_f2}"
            ),
        );
    } else {
        eprintln!(
            "[votes_silenced] F2 never campaigned (F1's timer fired first): the block had nothing to drop"
        );
    }
    match election {
        Ok(winner) => {
            ck.that(
                winner == f1,
                format!(
                    "n{winner} won although F1 (n{f1}) is the only node that can collect a second \
                     vote — a vote reached F2 by a path the rule does not cover"
                ),
            );
            let ok_at_election = w.ok();
            c.restart(l);
            let rejoined = try_await_single_leader_among(&c.dirs, &[l, f1, f2], 20);
            ck.that(rejoined.is_ok(), format!("after L's restart: {rejoined:?}"));
            std::thread::sleep(Duration::from_secs(2));
            let ok_after = w.ok();
            ck.that(
                ok_after > ok_at_election,
                format!("ok count did not grow after the election (stuck at {ok_at_election})"),
            );
        }
        Err(e) if f2_durable > f1_durable => ck.that(
            false,
            format!(
                "{e}; F2 held more bytes than F1 at the kill (durable f1={f1_durable} \
                 f2={f2_durable}), so F2 refuses F1's request and F1's grants are blocked: no \
                 election can complete until heal — a PRECONDITION miss, not a UC defect; re-run"
            ),
        ),
        Err(e) => ck.that(
            false,
            format!("{e} with F1 not behind F2 (durable f1={f1_durable} f2={f2_durable}): a liveness finding"),
        ),
    }
    drop(nf);
    conclude(w, ck, "votes_silenced");
}

// ----------------------------------------------------------------- Scenario C

/// Spec §4 C: everything F1 sends toward L is held 20-50 ms and reordered;
/// L's traffic toward F1 is untouched. F2's reports arrive on time, so the
/// quorum is never held by the slow one: commit and linearizable reads keep
/// completing.
#[test]
fn one_direction_slow() {
    let _g = netfault_lock();
    shorten_client_timeout();
    let c = Cluster::boot();
    let nf = Netfault::attach("lo");
    let w = c.workload(0x534C_4F57);
    let (l, f1) = (c.roles.leader, c.roles.f1);
    let slow = format!("{} any peer={} out", c.port(f1), c.ip(l));
    nf.run(&[
        "rule",
        "--out",
        "--delay",
        "20",
        "--jitter",
        "30",
        "--peer",
        &c.ip(l),
        &c.port(f1),
    ]);
    let (ok_at_fault, reads_at_fault) = (w.ok(), w.ok_reads());
    std::thread::sleep(FAULT_WINDOW);
    let (ok_in_fault, reads_in_fault) = (w.ok(), w.ok_reads());
    let delayed = nf.counter(&slow, "delayed");
    eprintln!(
        "[one_direction_slow] delayed={delayed} ok={ok_at_fault}->{ok_in_fault} \
         ok_reads={reads_at_fault}->{reads_in_fault}"
    );
    let mut ck = Checks::default();
    ck.that(
        delayed > 0,
        format!("the delay rule counted nothing: {slow}"),
    );
    ck.that(
        c.roles.leader == roles(&c.dirs, 5).leader,
        "the leader changed during the fault window: the rules named a stale topology",
    );
    drop(nf);
    ck.that(
        ok_in_fault > ok_at_fault,
        "commit did not advance while F1's reports were slow",
    );
    ck.that(
        reads_in_fault > reads_at_fault,
        "no linearizable read completed while F1's reports were slow (F2's acks are prompt)",
    );
    conclude(w, ck, "one_direction_slow");
}

// ----------------------------------------------------------------- Scenario D

/// Spec §4 D: one DATA in ten from L to F1 is lost AND every repair L sends
/// F1 — DATA starting below the flow's frontier — is dropped. F1 NAKs, L
/// repairs, the repair never arrives; F1 falls behind by exactly the lost
/// datagrams until heal. This is also the first live check that netfault's
/// `retransmit` classification sees UC's repairs as repairs.
#[test]
fn repairs_dropped() {
    let _g = netfault_lock();
    shorten_client_timeout();
    let c = Cluster::boot();
    let nf = Netfault::attach("lo");
    let w = c.workload(0x5245_5052);
    let (l, f1) = (c.roles.leader, c.roles.f1);
    let data = format!("{} data peer={} out", c.port(l), c.ip(f1));
    let repair = format!("{} retransmit peer={} out", c.port(l), c.ip(f1));
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
    nf.run(&[
        "block",
        "--out",
        "--kind",
        "retransmit",
        "--peer",
        &c.ip(f1),
        &c.port(l),
    ]);
    let ok_at_fault = w.ok();
    std::thread::sleep(FAULT_WINDOW);
    let ok_in_fault = w.ok();
    let (commit_at_heal, _) = cnc_positions(&c.dirs[l]);
    let (_, f1_durable_at_heal) = cnc_positions(&c.dirs[f1]);
    let (data_dropped, repair_dropped) =
        (nf.counter(&data, "dropped"), nf.counter(&repair, "dropped"));
    eprintln!(
        "[repairs_dropped] data_dropped={data_dropped} repair_dropped={repair_dropped} \
         ok={ok_at_fault}->{ok_in_fault} commit_at_heal={commit_at_heal} \
         f1_durable_at_heal={f1_durable_at_heal}"
    );
    let mut ck = Checks::default();
    ck.that(
        data_dropped > 0,
        format!("the DATA loss rule counted nothing: {data}"),
    );
    ck.that(
        repair_dropped > 0,
        format!("the retransmit rule counted nothing: {repair}"),
    );
    ck.that(
        f1_durable_at_heal < commit_at_heal,
        format!(
            "F1 did not fall behind during the fault (durable {f1_durable_at_heal} vs commit \
             {commit_at_heal}): the repairs reached it"
        ),
    );
    ck.that(
        c.roles.leader == roles(&c.dirs, 5).leader,
        "the leader changed during the fault window: the rules named a stale topology",
    );
    drop(nf);
    ck.that(
        ok_in_fault > ok_at_fault,
        "commit did not advance during the fault (L+F2 are a quorum)",
    );
    ck.that(
        wait_until(CATCH_UP, || cnc_positions(&c.dirs[f1]).1 >= commit_at_heal),
        "F1's durable position did not reach L's commit-at-heal within 10 s of heal",
    );
    conclude(w, ck, "repairs_dropped");
}
