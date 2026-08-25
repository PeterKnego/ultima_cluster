// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M12a Task 11 — the **remote lincheck capstone** (spec
//! `docs/superpowers/specs/2026-08-22-uc2-m12-adoptable-design.md` §4.5–§4.6
//! row 1): three node processes, three `Sessioned` service processes, three
//! **gateway** processes, and four `RemoteClient`s driving a CAS register
//! through the framed TCP remote protocol while the current leader's node is
//! SIGKILLed over and over. Assert the recorded history is WGL-linearizable
//! and that **no acknowledged write was lost**.
//!
//! ## What this proves that nothing else does
//!
//! `uc2_gateway/tests/failover.rs` already crashes a leader under a pipelined
//! client — but in-process, with `Edge`s and `Node`s as handles in one test
//! binary, one client, and a scripted single failover. This is the
//! out-of-process form: **nine real OS processes**, real `kill -9`, repeated
//! failovers under continuous concurrent load, and the answer adjudicated by
//! the untouched `uc-lincheck` checker rather than by a hand-written
//! "highest acknowledged" invariant.
//!
//! **What it deliberately does NOT cover: the SDK's pipelining.** Every worker
//! here is strictly `submit` → `wait`, one request in flight at a time, so the
//! concurrency this history records is exactly the four workers — and that is
//! a requirement, not an oversight: an op has to be a single interval with a
//! single outcome to be a linearizability history entry at all, and the
//! per-op re-send delta the envelope-off recording depends on is only
//! attributable to one op if only one is outstanding. Deep pipelining across a
//! failover (200 writes in flight, credits, `EXPIRED`-freedom under a full
//! window) is `failover.rs`'s job; the two tests are complements.
//!
//! Three edges are in the loop the whole time, and one of them dies on every
//! chaos cycle: when a node's shmem instance restarts underneath its edge, the
//! edge latches faulted, refuses new connections forever, and the
//! `uc2-crashtest-gateway` process exits 1 — exactly what the shipped
//! `uc2-gateway` daemon does under
//! `packaging/systemd/uc2-gateway.service`'s `Restart=on-failure`. The test's
//! supervisor loop respawns any gateway whose process has exited, which is the
//! systemd contract standing in for systemd. **The gateways are never killed
//! by the chaos thread**; every gateway respawn in a run is the product
//! deciding to die, and the count is reported.
//!
//! ## Classification discipline
//!
//! Same rule as `hard_crash.rs`: an outcome is `Ok` only when a `RESPONSE`
//! came back, and every error — `Expired`, `Unknown`, `TimedOut`, `Closed`,
//! `Io`, `Frame` — is `Indeterminate` and is **never retried by the worker**.
//! The client already re-sends internally (that is its promise), and a second
//! retry layer on top would manufacture the duplicate-apply violation this
//! test exists to catch. Two errors are treated as harness bugs and panic:
//! `HelloRefused` (wrong `app_id`/protocol — a wiring mistake) and
//! `PayloadTooLarge`.
//!
//! ## The two variants, and why they record differently
//!
//! - **envelope ON** (`remote_lin_envelope_on`, the default posture): the
//!   edges prepend the 16-byte `client_id ++ seq` session header and the
//!   services run `Sessioned<RegisterSm>`, so a re-send is exactly-once by
//!   construction. Every `Ok` is trustworthy, so this variant additionally
//!   runs the **no-acked-write-lost oracle** below, and requires
//!   `stats().expired == 0`.
//! - **envelope OFF** (`remote_lin_envelope_off`): raw pass-through, and the
//!   spec is explicit that this mode is at-least-once ("with it off, re-sent
//!   writes are reported as possibly duplicated", §4.5). A re-sent mutation
//!   that had in fact already applied applies a **second time**, which is a
//!   real second effect, not a bookkeeping artefact — no checker can call a
//!   history with a hidden second effect linearizable, so recording such an
//!   op as a single `Ok` would make this test fail for a reason that is the
//!   documented contract rather than a defect. It is therefore recorded the
//!   way the contract reads: a mutation whose client re-sent it during the
//!   call is recorded `Indeterminate`, plus one extra `Indeterminate` entry
//!   modelling the possible duplicate. Read ops need no such treatment (a
//!   duplicated query has no effect). Anything the envelope-off run still
//!   asserts — liveness, no `Expired`, linearizability modulo those
//!   duplicates — is asserted with full teeth.
//!
//! ## The no-acked-write-lost oracle (envelope ON)
//!
//! Every `Ok` mutation is recorded with the log `position` the edge reported,
//! and the register's final linearizable value must be the value written by
//! the last of them. One wrinkle makes this less trivial than "max position
//! wins": a `REPLAYED` response is answered from the dedup cache at the
//! position of the **re-send**, while the write itself happened at the
//! (unknown, strictly earlier) position of the original apply. So a replayed
//! mutation's effective position is only bounded above, and the sound
//! statement is: the final value is the last FRESH mutation's value, unless
//! some replayed mutation whose re-send landed after it in fact applied later.
//! A second widening is needed for the mirror-image reason: a mutation whose
//! ticket ran out of budget was never REFUSED, so the node may commit it
//! afterwards — possibly after the final read is issued — and its value is a
//! candidate too. The result is a set, almost always of size one; its size is
//! printed on every run, so the day it stops being one is visible.
//!
//! Gated behind `hard-crash-tests`: real processes, real SIGKILLs.
#![cfg(feature = "hard-crash-tests")]

use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use uc2_client::Client;
use uc2_log::cnc::CncPage;
use uc2_remote::{Consistency, RemoteClient, RemoteConfig, RemoteError, RemoteResponse, RemoteStats};
use uc_lincheck::checker::{Verdict, check_register};
use uc_lincheck::history::{Entry, History, Outcome};
use uc_lincheck::model::{Op, RegResp};
use uc_lincheck::register::{Cmd, CmdResp};

mod common;
use common::*;

const GATEWAY_BIN: &str = env!("CARGO_BIN_EXE_uc2-crashtest-gateway");

/// Cluster size. Three voters: one can be down for the whole restart window
/// and the remaining two are still a quorum, so a serving leader exists
/// throughout and the workload is expected to make progress *during* the
/// chaos, not merely to survive it.
const N: usize = 3;
/// Concurrent remote clients. Four against a three-member cluster guarantees
/// at least two share an edge.
const WORKERS: u32 = 4;
/// How long the workload runs (per test).
const LOAD: Duration = Duration::from_secs(20);
/// Per-op pacing, as in `lin_v2.rs`: keeps the recorded history a couple of
/// thousand entries rather than a couple of hundred thousand, which is what
/// makes the WGL search tractable — see `CHECKER_STACK` for the other half
/// of that story.
const THROTTLE: Duration = Duration::from_millis(40);
/// How often the leader's node+service are SIGKILLed and respawned.
const CHAOS_PERIOD: Duration = Duration::from_secs(3);
/// How often the gateway supervisor looks for an exited edge. Must be well
/// under `CHAOS_PERIOD` — a faulted edge is a gateway that has to come back
/// before the next kill, not after it.
const SUPERVISE_TICK: Duration = Duration::from_millis(200);
/// Ceiling on the modelled duplicates of ONE envelope-off mutation.
///
/// The count that matters is the op's re-send DELTA, not a flat one: the
/// client re-sends every unanswered request on every reconnect, so an op that
/// straddled two failovers can be written three times and applied three
/// times, and a model that admits only two applies would make the checker
/// report a `Violation` the product did not commit. (Run 1 of the first local
/// pass: 151 re-sends against 16 re-sent mutations — multi-re-send is the
/// norm, not the corner.) So `phantoms = min(delta, MAX_PHANTOM_DUPLICATES)`.
///
/// The cap exists only to bound the WGL search — every `Indeterminate`
/// mutation is eligible from its invoke to the end of the history, so each one
/// widens it. A delta above the cap is therefore counted (`phantom_cap_hits`,
/// printed at the end alongside the run's `max_resend_delta`) rather than
/// silently truncated: it is the one condition under which an envelope-off
/// `Violation` might be the model's fault instead of the cluster's, and it has
/// to be visible to be ruled out.
///
/// 8, and both bounds of that choice were measured rather than guessed:
///
/// - **Why more than a handful.** At a cap of 4 the cap was hit 3-5 times in
///   every envelope-off run, and a hit is exactly the case that could turn a
///   sound cluster into a red test.
/// - **Why not the raw delta.** The observed maximum delta is up to ~54
///   measured — but a re-send is not an apply. Most of those writes are
///   refused by an edge that cannot serve (REDIRECT) or land on a node that
///   dies before committing;
///   an apply needs the frame to actually reach a leader that commits it, and
///   with 6 kills in a 20 s window an op cannot plausibly do that more than a
///   handful of times. Phantoms that model impossible applies are pure cost:
///   every one is an `Indeterminate` mutation eligible from its invoke to the
///   end of the history, and raising the cap to 16 DOUBLED the run (81 s vs
///   41 s) for no additional modelling truth.
///
/// So 8 covers realistic duplication with room to spare, and the residual —
/// a delta above 8 — is counted rather than hidden.
const MAX_PHANTOM_DUPLICATES: u64 = 8;

/// Stack for the thread the WGL check runs on.
///
/// `uc-lincheck`'s search is RECURSIVE and one frame deep per linearized op,
/// so its stack need grows with the history: at ~3.3k entries it overflows a
/// default 8 MiB thread stack and the process *aborts* — which, in a test
/// that owns nine child processes, also means no destructor runs and every
/// one of them is orphaned. Giving the checker its own generous stack is the
/// fix; the checker itself is the shared, untouched adjudicator and is not
/// something this test gets to change.
const CHECKER_STACK: usize = 256 << 20;

/// A tempdir on the ext4 target volume, never `/tmp` (RAM-backed tmpfs with
/// no swap on the dev box — see CLAUDE.md "Local box").
fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-remote-lin-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

// ------------------------------------------------------------------ codec
//
// The remote protocol carries opaque bytes: the gateway never looks inside a
// command, so the test has to speak the same codec the service's typed
// adapter does (bincode/serde, standard config) itself.

fn enc(c: &Cmd) -> Vec<u8> {
    bincode::serde::encode_to_vec(c, bincode::config::standard()).expect("encode cmd")
}

fn dec(b: &[u8]) -> CmdResp {
    bincode::serde::decode_from_slice(b, bincode::config::standard()).expect("decode resp").0
}

fn read_query() -> Vec<u8> {
    bincode::serde::encode_to_vec((), bincode::config::standard()).expect("encode query")
}

fn dec_read(b: &[u8]) -> Option<u64> {
    bincode::serde::decode_from_slice(b, bincode::config::standard()).expect("decode read").0
}

// ------------------------------------------------------------- addressing

/// A free loopback UDP address for a node's replication socket (bind, learn
/// the port, release — the child process binds it for real).
fn free_udp_addr() -> SocketAddr {
    let s = UdpSocket::bind("127.0.0.1:0").expect("bind udp probe");
    let a = s.local_addr().unwrap();
    drop(s);
    a
}

/// A free loopback TCP address for an edge's listener. The whole node-id →
/// gateway map has to be known before the first edge starts (it is what
/// `REDIRECT` names), so the ports are reserved this way up front — and the
/// same port is reused on every respawn, which is why `TcpListener::bind`'s
/// `SO_REUSEADDR` matters here.
fn free_tcp_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind tcp probe");
    l.local_addr().unwrap()
}

fn members_arg(members: &[(u32, SocketAddr)]) -> String {
    members.iter().map(|(id, a)| format!("{id}@{a}")).collect::<Vec<_>>().join(",")
}

/// Open `dir`'s cnc page read-only. `None` on any transient failure (the file
/// is being recreated by a restarting node), which every caller treats as
/// "try again", never as an error.
fn open_cnc(dir: &Path) -> Option<Arc<CncPage>> {
    CncPage::open_file(&dir.join("cnc2.dat"), APP_ID).ok()
}

/// The index of a node whose cnc page says it is a serving leader, if any.
///
/// Unlike `hard_crash.rs`'s `await_single_leader_multi` this does **not**
/// assert that at most one node claims leadership: a SIGKILLed node's cnc
/// page freezes with whatever flags its agents last wrote, so a dead ex-leader
/// keeps advertising `LEADER | CAN_SERVE` until its replacement process
/// recreates the page. Under continuous chaos that is a normal, expected
/// reading, not split brain.
fn find_leader(dirs: &[PathBuf]) -> Option<usize> {
    use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER};
    let want = NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE;
    (0..dirs.len())
        .find(|&i| open_cnc(&dirs[i]).is_some_and(|c| c.status().flags.load_acquire() & want == want))
}

/// Wait for some node to be a serving leader. Used at boot and after the
/// chaos has stopped, both times with every node process alive.
fn await_leader(dirs: &[PathBuf], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(i) = find_leader(dirs) {
            return i;
        }
        assert!(Instant::now() < deadline, "no serving leader among {} nodes in {secs}s", dirs.len());
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Wait until a node respawned on the SAME instance dir has finished its
/// WHOLE boot sequence and presents a fresh `instance_id`.
///
/// `Client::connect` — not a raw cnc read — is what makes this a real
/// readiness barrier: it opens and validates every IPC artifact in sequence
/// (the cnc page plus each ring, magic-checked). The cnc page is created
/// FIRST, so a fresh `instance_id` alone leaves a window in which the rings
/// do not exist yet, and a service spawned inside that window dies at startup
/// with `ring error: No such file or directory` — taking that member's apply
/// path with it for the rest of the run, which is how a linearizable read
/// ends up unanswerable 60 s after the chaos stopped. (Observed: run 3 of the
/// first 3x local pass. `common::wait_for_ready`'s doc calls this window out;
/// `hard_crash.rs`'s cnc-only variant gets away with it because it restarts a
/// node far less often.)
///
/// Returns `false` on timeout instead of panicking: this runs on the chaos
/// thread, where a panic would poison the rig mutex and turn a slow restart
/// into a hang rather than a failed assertion. The caller counts the failures
/// and the test asserts on the count.
fn await_fresh_instance(dir: &Path, old: Option<u128>, timeout: Duration) -> bool {
    // No pre-kill id means there is nothing to be fresh RELATIVE TO: a node's
    // instance dir keeps its cnc page and rings after the process dies, so a
    // `Client::connect` would happily validate the STALE files and this would
    // report a restart that has not happened — and the service spawned behind
    // it would attach to a dead node for the rest of the run. Refuse instead;
    // the caller counts it and the test asserts the count is zero.
    let Some(old) = old else { return false };
    let old = Some(old);
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(c) = Client::connect(dir, APP_ID) {
            let id = c.instance_id();
            c.shutdown();
            if Some(id) != old {
                return true;
            }
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

// -------------------------------------------------------------------- rig

/// Every process the test owns, plus what it takes to respawn any of them.
struct Rig {
    dirs: Vec<PathBuf>,
    udp: Vec<SocketAddr>,
    node_members: String,
    gw: Vec<SocketAddr>,
    gw_members: String,
    /// Envelope on ⇒ services run `Sessioned<RegisterSm>` and edges add the
    /// session header. The two must agree.
    envelope: bool,
    nodes: Vec<Option<Reap>>,
    svcs: Vec<Option<Reap>>,
    gws: Vec<Option<Reap>>,
    /// Gateways respawned because their process EXITED on its own — the
    /// `is_faulted` contract firing. Never incremented by a kill: the chaos
    /// thread does not touch gateway processes.
    gw_respawns: u64,
    /// Services respawned because their process exited on its own. Expected
    /// to be 0 now that the restart barrier is `Client::connect`-based, but
    /// supervised (and reported) anyway: an unsupervised service that dies
    /// once silently disarms one member's apply path for the whole run, and
    /// a supervisor is what a real deployment has.
    svc_respawns: u64,
}

/// Has this child exited (or is the slot empty)? Reaps it if so.
fn exited(slot: &mut Option<Reap>) -> bool {
    match slot.as_mut() {
        Some(r) => r.0.try_wait().ok().flatten().is_some(),
        None => true,
    }
}

fn spawn_node_member(dir: &Path, id: u32, bind: SocketAddr, members: &str) -> Reap {
    let child = Command::new(NODE_BIN)
        .arg("--instance-dir")
        .arg(dir)
        .arg("--app-id")
        .arg(APP_ID)
        .arg("--id")
        .arg(id.to_string())
        .arg("--bind")
        .arg(bind.to_string())
        .arg("--members")
        .arg(members)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn node {id}: {e}"));
    Reap(child)
}

impl Rig {
    fn spawn_gateway(&self, i: usize) -> Reap {
        let mut cmd = Command::new(GATEWAY_BIN);
        cmd.arg("--instance-dir")
            .arg(&self.dirs[i])
            .arg("--app-id")
            .arg(APP_ID)
            .arg("--listen")
            .arg(self.gw[i].to_string())
            .arg("--members")
            .arg(&self.gw_members);
        if !self.envelope {
            cmd.arg("--no-envelope");
        }
        let child = cmd
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn gateway {i}: {e}"));
        Reap(child)
    }

    /// Respawn every gateway whose process has exited. This is systemd's job
    /// in production (`Restart=on-failure`), and standing in for it is part
    /// of what the capstone asserts: a gateway that faults must be replaceable
    /// by a fresh process against the same instance dir, with no client
    /// having to be told anything beyond "your connection dropped".
    fn supervise(&mut self) {
        for i in 0..N {
            if exited(&mut self.gws[i]) {
                self.gws[i] = None; // reap before rebinding the port
                self.gws[i] = Some(self.spawn_gateway(i));
                self.gw_respawns += 1;
            }
            if exited(&mut self.svcs[i]) {
                self.svcs[i] = None;
                self.svcs[i] = Some(spawn_service_with(&self.dirs[i], self.envelope));
                self.svc_respawns += 1;
            }
        }
    }

    /// Hard-crash node `i` and its service, then bring both back on the same
    /// instance dir, address and id. Node before service in both directions:
    /// a node's read barrier can block awaiting its service, and a service
    /// never self-heals across a node restart (the v2 external-supervisor
    /// contract, restated in `hard_crash.rs`).
    ///
    /// Returns `false` if the restarted node never presented a fresh cnc
    /// instance in time.
    fn kill_and_restart(&mut self, i: usize) -> bool {
        // Read the id to be fresh RELATIVE TO before anything dies (see
        // `await_fresh_instance`). A short retry covers a torn read of a page
        // being rewritten; in practice this always succeeds first try, because
        // `find_leader` just read the same page.
        let mut old = None;
        for _ in 0..20 {
            old = open_cnc(&self.dirs[i]).and_then(|c| c.try_instance_id());
            if old.is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        self.nodes[i] = None; // SIGKILL + reap
        self.svcs[i] = None;
        self.nodes[i] = Some(spawn_node_member(&self.dirs[i], i as u32, self.udp[i], &self.node_members));
        let fresh = await_fresh_instance(&self.dirs[i], old, Duration::from_secs(15));
        self.svcs[i] = Some(spawn_service_with(&self.dirs[i], self.envelope));
        fresh
    }
}

/// `Mutex::lock` that survives a poisoned lock. A panic on the chaos thread
/// is reported by joining it, not by wedging the main thread's cleanup.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

// ----------------------------------------------------------------- oracle

/// One acknowledged mutation: where the edge said it landed, what it set the
/// register to, and whether it was answered from the session dedup cache.
#[derive(Clone, Copy, Debug)]
struct Mutation {
    position: u64,
    value: u64,
    replayed: bool,
}

/// The value a mutation would set if it committed and took effect (a CAS that
/// commits and FAILS sets nothing; treating `new` as its value is a superset,
/// which is the safe direction for a candidate set).
fn mutation_value(op: &Op) -> Option<u64> {
    match op {
        Op::Write(v) => Some(*v),
        Op::Cas { new, .. } => Some(*new),
        Op::Read => None,
    }
}

/// Every value the register may legally hold once the workload is over.
///
/// Three sources, and each is a bound rather than a certainty for a different
/// reason:
///
/// 1. The last exactly-known (FRESH) acked mutation by position — the answer
///    in the ordinary case, and the only member of the set on a clean run.
/// 2. `replayed` acked mutations whose re-send landed after (1). A replayed
///    response is answered from the dedup cache at the position of the
///    RE-SEND, while the write itself happened at the (unknown, strictly
///    earlier) position of the original apply — so such a mutation is only
///    bounded above and stays a candidate for "last".
/// 3. `indeterminate` mutations (`indet_values`): a request whose ticket gave
///    up at its 15 s budget has NOT been refused — the node can still commit
///    it afterwards, including after the final read is issued. Excluding
///    these is what turns a perfectly legal late commit into a false
///    accusation that the product lost an acknowledged write.
///
/// Only the LAST effective mutation decides the final value, so no replay of
/// the sequence is needed — just the set of values that could be it.
fn expected_final_values(mutations: &[Mutation], indet_values: &[u64]) -> Vec<u64> {
    let last_fresh = mutations.iter().filter(|m| !m.replayed).max_by_key(|m| m.position);
    let Some(f) = last_fresh else {
        // No fresh mutation at all (only possible if literally every ack was a
        // replay): fall back to every acknowledged value, plus the
        // indeterminate ones.
        let mut out: Vec<u64> = mutations.iter().map(|m| m.value).collect();
        out.extend_from_slice(indet_values);
        out.sort_unstable();
        out.dedup();
        return out;
    };
    let mut out = vec![f.value];
    out.extend(
        mutations.iter().filter(|m| m.replayed && m.position > f.position).map(|m| m.value),
    );
    out.extend_from_slice(indet_values);
    out.sort_unstable();
    out.dedup();
    out
}

// ---------------------------------------------------------------- workers

/// Connect a `RemoteClient` over `members`, retrying while the edges are
/// still coming up.
fn connect_remote(members: &[String], client_id: u64, timeout: Duration) -> RemoteClient {
    let deadline = Instant::now() + timeout;
    loop {
        let cfg = RemoteConfig {
            app_id: APP_ID.into(),
            members: members.to_vec(),
            client_id: Some(client_id),
            // Generous: one op's budget has to cover a leader SIGKILL, the
            // election that follows, the edge's own request timeout and every
            // re-send those imply. Anything less and the test measures its own
            // impatience instead of the cluster.
            request_timeout: Duration::from_secs(15),
            connect_timeout: Duration::from_secs(2),
            max_inflight: 16,
            ..Default::default()
        };
        match RemoteClient::connect(cfg) {
            Ok(c) => return c,
            Err(e) => {
                assert!(Instant::now() < deadline, "client {client_id} could not connect: {e:?}");
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

/// Classify one request outcome. `Ok` is only ever a real `RESPONSE`.
/// Panics on the two errors that can only be a harness/wiring bug.
fn resolve(r: Result<RemoteResponse, RemoteError>, what: &str) -> Option<RemoteResponse> {
    match r {
        Ok(resp) => Some(resp),
        Err(e @ (RemoteError::HelloRefused { .. } | RemoteError::PayloadTooLarge)) => {
            panic!("{what}: harness/wiring error, not a cluster outcome: {e:?}")
        }
        // Expired | Unknown | TimedOut | Closed | Io | Frame | NoMembersReachable:
        // "may or may not have committed", and NEVER retried here.
        Err(_) => None,
    }
}

struct WorkerOut {
    stats: RemoteStats,
    ok: u64,
    indeterminate: u64,
    /// Indeterminate ops that were MUTATIONS. These are the ones that can
    /// still commit after the ticket gave up, so they are what the acked-write
    /// oracle has to widen its candidate set for.
    indeterminate_mutations: u64,
    /// Envelope-off ops whose re-send delta exceeded [`MAX_PHANTOM_DUPLICATES`]
    /// — see that constant.
    phantom_cap_hits: u64,
    /// The largest per-op re-send delta seen. Reported so the cap can be
    /// judged against measurement rather than taste.
    max_resend_delta: u64,
}

#[allow(clippy::too_many_arguments)]
fn worker(
    id: u32,
    members: Vec<String>,
    envelope: bool,
    seed: u64,
    history: Arc<History>,
    last_seen: Arc<AtomicU64>,
    mutations: Arc<Mutex<Vec<Mutation>>>,
    indeterminate_values: Arc<Mutex<Vec<u64>>>,
    stop: Arc<AtomicBool>,
) -> WorkerOut {
    // A fixed per-worker `client_id`: it is the key the edge's session dedup
    // is per, so a re-send after a reconnect has to assert the same identity
    // to be recognised as a re-send at all.
    let client = connect_remote(&members, 1 + id as u64, Duration::from_secs(30));
    let mut rng = StdRng::seed_from_u64(seed ^ (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
    let (mut ok, mut indeterminate) = (0u64, 0u64);
    let (mut indeterminate_mutations, mut phantom_cap_hits) = (0u64, 0u64);
    let mut max_resend_delta = 0u64;

    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(THROTTLE);
        let resends_before = client.stats().resends;

        let (op, outcome, is_mutation) = match rng.random_range(0..3u8) {
            0 => {
                let v = rng.random_range(1..1000u64);
                let inv = history.invoke();
                let r = client.submit(&enc(&Cmd::Write(v))).and_then(|t| t.wait());
                match resolve(r, "write") {
                    Some(resp) => {
                        assert_eq!(dec(&resp.bytes), CmdResp::WriteAck, "write {v}");
                        last_seen.store(v, Ordering::Relaxed);
                        if envelope {
                            lock(&mutations).push(Mutation {
                                position: resp.position,
                                value: v,
                                replayed: resp.replayed,
                            });
                        }
                        (Op::Write(v), (inv, Outcome::Ok(RegResp::Ack)), true)
                    }
                    None => (Op::Write(v), (inv, Outcome::Indeterminate), true),
                }
            }
            1 => {
                let inv = history.invoke();
                let r = client.query(&read_query(), Consistency::Linearizable).and_then(|t| t.wait());
                match resolve(r, "read") {
                    Some(resp) => {
                        let v = dec_read(&resp.bytes);
                        if let Some(x) = v {
                            last_seen.store(x, Ordering::Relaxed);
                        }
                        (Op::Read, (inv, Outcome::Ok(RegResp::Value(v))), false)
                    }
                    None => (Op::Read, (inv, Outcome::Indeterminate), false),
                }
            }
            _ => {
                // A recently-seen value as `old` most of the time (so some
                // CASes succeed), a random one otherwise (so some fail).
                let old = if rng.random_bool(0.7) {
                    last_seen.load(Ordering::Relaxed)
                } else {
                    rng.random_range(1..1000u64)
                };
                let new = rng.random_range(1..1000u64);
                let inv = history.invoke();
                let r = client.submit(&enc(&Cmd::Cas { old, new })).and_then(|t| t.wait());
                match resolve(r, "cas") {
                    Some(resp) => match dec(&resp.bytes) {
                        CmdResp::CasResult(b) => {
                            if b {
                                last_seen.store(new, Ordering::Relaxed);
                                if envelope {
                                    lock(&mutations).push(Mutation {
                                        position: resp.position,
                                        value: new,
                                        replayed: resp.replayed,
                                    });
                                }
                            }
                            (Op::Cas { old, new }, (inv, Outcome::Ok(RegResp::CasOk(b))), true)
                        }
                        other => panic!("cas returned a non-cas response: {other:?}"),
                    },
                    None => (Op::Cas { old, new }, (inv, Outcome::Indeterminate), true),
                }
            }
        };

        let (inv, outcome) = outcome;
        match outcome {
            Outcome::Ok(_) => ok += 1,
            Outcome::Indeterminate => {
                indeterminate += 1;
                if is_mutation {
                    indeterminate_mutations += 1;
                    // It may yet commit — after the ticket gave up, even after
                    // this test's final read is issued. The value it would set
                    // therefore belongs in the acked-write oracle's candidate
                    // set (a CAS that commits and FAILS sets nothing, so
                    // including `new` unconditionally is a superset, which is
                    // the safe direction).
                    if envelope && let Some(v) = mutation_value(&op) {
                        lock(&indeterminate_values).push(v);
                    }
                }
            }
        }
        // The re-send DELTA for THIS op: a worker has at most one request in
        // flight, so every re-send counted across the call belongs to it.
        let delta = client.stats().resends.saturating_sub(resends_before);
        max_resend_delta = max_resend_delta.max(delta);
        let dup = if is_mutation { delta } else { 0 };
        // Only meaningful with the envelope OFF: that is the only mode where
        // the delta becomes phantoms, so it is the only mode where truncating
        // it can make the model unfaithful.
        if !envelope && dup > MAX_PHANTOM_DUPLICATES {
            phantom_cap_hits += 1;
        }
        record(&history, id, op, inv, outcome, envelope, dup);
    }

    let stats = client.stats();
    client.shutdown();
    WorkerOut {
        stats,
        ok,
        indeterminate,
        indeterminate_mutations,
        phantom_cap_hits,
        max_resend_delta,
    }
}

/// Record one completed op. `resend_delta` is the envelope-OFF at-least-once
/// case (see the module doc): a mutation the client had to write `1 + delta`
/// times may have been APPLIED that many times, so it is recorded as an
/// `Indeterminate` op plus one `Indeterminate` phantom per re-send — up to
/// [`MAX_PHANTOM_DUPLICATES`] — rather than as the single definite op it is
/// not. `resend_delta == 0` (and every envelope-ON op) is recorded exactly as
/// observed.
fn record(
    history: &History,
    id: u32,
    op: Op,
    inv: u64,
    outcome: Outcome,
    envelope: bool,
    resend_delta: u64,
) {
    if envelope || resend_delta == 0 {
        history.record(id, op, inv, outcome);
        return;
    }
    history.record(id, op.clone(), inv, Outcome::Indeterminate);
    for _ in 0..resend_delta.min(MAX_PHANTOM_DUPLICATES) {
        history.record(id, op.clone(), inv, Outcome::Indeterminate);
    }
}

// ------------------------------------------------------------------ chaos

#[derive(Debug, Default)]
struct ChaosReport {
    kills: u64,
    no_leader_ticks: u64,
    restart_timeouts: u64,
}

// ------------------------------------------------------------------ check

/// Run the checker on a thread with [`CHECKER_STACK`] instead of the test
/// harness's default 8 MiB.
fn check_deep(entries: &[Entry]) -> Verdict {
    std::thread::scope(|s| {
        std::thread::Builder::new()
            .stack_size(CHECKER_STACK)
            .spawn_scoped(s, || check_register(entries))
            .expect("spawn checker thread")
            .join()
            .expect("checker thread")
    })
}

fn assert_linearizable(entries: &[Entry], tag: &str) {
    match check_deep(entries) {
        Verdict::Linearizable => eprintln!("[remote_lin] {tag}: Linearizable"),
        Verdict::Inconclusive => {
            // A budget-exhausted search is not an answer: accepting it would
            // let this capstone pass while proving nothing. Same call as
            // `lin_v2.rs` makes at every one of its six check sites.
            panic!(
                "remote_lin {tag}: checker Inconclusive — the WGL search hit its visited-state \
                 budget and adjudicated nothing; raise the budget / lower the op target (raise \
                 THROTTLE, shorten LOAD)"
            )
        }
        Verdict::Violation => {
            let path =
                PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("remote_lin_{tag}.txt"));
            let mut s = String::new();
            for e in entries {
                s.push_str(&format!("{e:?}\n"));
            }
            let _ = std::fs::write(&path, s);
            eprintln!("[remote_lin] history ({} entries) dumped to {}", entries.len(), path.display());
            panic!("remote_lin history NOT linearizable (tag {tag})");
        }
    }
}

// ------------------------------------------------------------------- test

#[test]
fn remote_lin_envelope_on() {
    remote_lin_once(1, true);
}

#[test]
fn remote_lin_envelope_off() {
    remote_lin_once(2, false);
}

/// The two variants run one at a time. Each boots NINE busy-polling
/// processes (3 nodes x 4 agents, 3 services, 3 edges) and cargo runs the
/// tests in one binary on parallel threads; overlapping them on a 4-vCPU CI
/// runner would measure the runner rather than the cluster.
static SERIALIZE: Mutex<()> = Mutex::new(());

fn remote_lin_once(seed: u64, envelope: bool) {
    let _serialized = lock(&SERIALIZE);
    let tag = if envelope { "envelope_on" } else { "envelope_off" };
    let started = Instant::now();
    let root = tempdir();

    // --- 1. Addresses first: every node needs the whole member map, and
    // every edge needs the whole gateway map, before any of them starts.
    let udp: Vec<SocketAddr> = (0..N).map(|_| free_udp_addr()).collect();
    let node_members = members_arg(&(0..N as u32).map(|i| (i, udp[i as usize])).collect::<Vec<_>>());
    let gw: Vec<SocketAddr> = (0..N).map(|_| free_tcp_addr()).collect();
    let gw_members = members_arg(&(0..N as u32).map(|i| (i, gw[i as usize])).collect::<Vec<_>>());

    // --- 2. Nodes, then services, then edges.
    let mut dirs = Vec::with_capacity(N);
    let mut nodes: Vec<Option<Reap>> = Vec::with_capacity(N);
    for (i, &bind) in udp.iter().enumerate() {
        let d = root.path().join(format!("n{i}"));
        std::fs::create_dir_all(&d).unwrap();
        nodes.push(Some(spawn_node_member(&d, i as u32, bind, &node_members)));
        wait_for_ready(&d, Duration::from_secs(20));
        dirs.push(d);
    }
    let svcs: Vec<Option<Reap>> = dirs.iter().map(|d| Some(spawn_service_with(d, envelope))).collect();

    let mut rig = Rig {
        dirs: dirs.clone(),
        udp,
        node_members,
        gw: gw.clone(),
        gw_members,
        envelope,
        nodes,
        svcs,
        gws: (0..N).map(|_| None).collect(),
        gw_respawns: 0,
        svc_respawns: 0,
    };
    for i in 0..N {
        rig.gws[i] = Some(rig.spawn_gateway(i));
    }
    // The initial three edges are not "respawns" — only the product deciding
    // to die is, and `supervise_gateways` is what counts those.
    rig.gw_respawns = 0;

    let leader0 = await_leader(&dirs, 30);
    eprintln!("[remote_lin] {tag}: cluster up, leader = n{leader0}");

    let gw_addrs: Vec<String> = gw.iter().map(|a| a.to_string()).collect();
    let history = Arc::new(History::default());
    let last_seen = Arc::new(AtomicU64::new(0));
    let mutations = Arc::new(Mutex::new(Vec::<Mutation>::new()));
    // Values of mutations whose ticket never resolved — candidates for the
    // final value because the node may commit them after the fact.
    let indeterminate_values = Arc::new(Mutex::new(Vec::<u64>::new()));
    let stop = Arc::new(AtomicBool::new(false));

    // --- 3. Warm-up write, recorded as history entry 0.
    //
    // The WGL model starts at `None` (never written); a warm-up write that
    // was NOT recorded would leave a phantom value later reads observe but
    // the checker cannot account for (a false `Violation`). Same fix as
    // `hard_crash.rs::warmup_write`.
    {
        // ONE attempt, no retry loop. A retried warm-up would be a second
        // possible apply of the same logical write with only one history
        // entry to account for it — the very thing this test refuses to do
        // anywhere else. It does not need a retry loop: no chaos is running
        // yet, `await_leader` above has already confirmed a serving leader,
        // and the client's own 15 s budget absorbs a redirect or a reconnect
        // internally. If it still fails, the rig is broken and the test says
        // so immediately.
        let warm = connect_remote(&gw_addrs, 1 + WORKERS as u64, Duration::from_secs(30));
        let inv = history.invoke();
        let resp = warm
            .submit(&enc(&Cmd::Write(1)))
            .and_then(|t| t.wait())
            .unwrap_or_else(|e| panic!("warm-up write did not commit: {e:?}"));
        assert_eq!(dec(&resp.bytes), CmdResp::WriteAck);
        history.record(WORKERS, Op::Write(1), inv, Outcome::Ok(RegResp::Ack));
        last_seen.store(1, Ordering::Relaxed);
        if envelope {
            lock(&mutations).push(Mutation {
                position: resp.position,
                value: 1,
                replayed: resp.replayed,
            });
        }
        warm.shutdown();
    }

    // --- 4. Workers, then chaos.
    let rig = Arc::new(Mutex::new(rig));
    let handles: Vec<_> = (0..WORKERS)
        .map(|w| {
            // Each worker starts on a different edge (`members[0]` is the
            // first dial), so the load is spread across all three from the
            // outset rather than piling onto whichever one is listed first.
            let members: Vec<String> =
                (0..N).map(|k| gw_addrs[(w as usize + k) % N].clone()).collect();
            let (history, last_seen, mutations, indeterminate_values, stop) = (
                Arc::clone(&history),
                Arc::clone(&last_seen),
                Arc::clone(&mutations),
                Arc::clone(&indeterminate_values),
                Arc::clone(&stop),
            );
            std::thread::spawn(move || {
                worker(
                    w,
                    members,
                    envelope,
                    seed,
                    history,
                    last_seen,
                    mutations,
                    indeterminate_values,
                    stop,
                )
            })
        })
        .collect();

    let chaos = {
        let (rig, stop, dirs) = (Arc::clone(&rig), Arc::clone(&stop), dirs.clone());
        std::thread::spawn(move || {
            let mut report = ChaosReport::default();
            let mut next_kill = Instant::now() + CHAOS_PERIOD;
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(SUPERVISE_TICK);
                // Systemd's job, every tick: a gateway that exited (faulted,
                // or could not attach to a node mid-restart) comes back.
                lock(&rig).supervise();
                if Instant::now() < next_kill {
                    continue;
                }
                next_kill = Instant::now() + CHAOS_PERIOD;
                match find_leader(&dirs) {
                    Some(li) => {
                        report.kills += 1;
                        if !lock(&rig).kill_and_restart(li) {
                            report.restart_timeouts += 1;
                        }
                    }
                    None => report.no_leader_ticks += 1,
                }
            }
            report
        })
    };

    std::thread::sleep(LOAD);
    stop.store(true, Ordering::Relaxed);

    let mut outs = Vec::with_capacity(WORKERS as usize);
    for h in handles {
        match h.join() {
            Ok(o) => outs.push(o),
            Err(e) => std::panic::resume_unwind(e),
        }
    }
    let report = match chaos.join() {
        Ok(r) => r,
        Err(e) => std::panic::resume_unwind(e),
    };

    // --- 5. Let the cluster settle, with the supervisor still running: the
    // last kill may have been moments ago, and the final read below needs a
    // leader and a live edge to reach it through.
    let settle = Instant::now() + Duration::from_secs(30);
    let leader_final = loop {
        lock(&rig).supervise();
        if let Some(i) = find_leader(&dirs) {
            break i;
        }
        assert!(Instant::now() < settle, "cluster never re-converged on a leader after the chaos");
        std::thread::sleep(Duration::from_millis(100));
    };

    // --- 6. The final linearizable read, recorded as the last history entry.
    let final_value = {
        let c = connect_remote(&gw_addrs, 1 + WORKERS as u64 + 1, Duration::from_secs(60));
        let inv = history.invoke();
        let deadline = Instant::now() + Duration::from_secs(60);
        let v = loop {
            lock(&rig).supervise();
            match c.query(&read_query(), Consistency::Linearizable).and_then(|t| t.wait()) {
                Ok(r) => break dec_read(&r.bytes),
                Err(e) => {
                    assert!(Instant::now() < deadline, "final linearizable read never answered: {e:?}");
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        };
        history.record(WORKERS, Op::Read, inv, Outcome::Ok(RegResp::Value(v)));
        c.shutdown();
        v
    };

    // --- 7. Report, then assert.
    let entries = Arc::try_unwrap(history).ok().expect("sole history owner").into_entries();
    let ok = History::ok_count(&entries);
    let totals = outs.iter().fold(RemoteStats::default(), |mut a, o| {
        a.redirects += o.stats.redirects;
        a.leader_changes += o.stats.leader_changes;
        a.reconnects += o.stats.reconnects;
        a.resends += o.stats.resends;
        a.retries += o.stats.retries;
        a.unknown += o.stats.unknown;
        a.expired += o.stats.expired;
        a.refused_members += o.stats.refused_members;
        a.max_credits_seen = a.max_credits_seen.max(o.stats.max_credits_seen);
        a
    });
    let (gw_respawns, svc_respawns) = {
        let r = lock(&rig);
        (r.gw_respawns, r.svc_respawns)
    };
    let worker_ok: u64 = outs.iter().map(|o| o.ok).sum();
    let worker_indef: u64 = outs.iter().map(|o| o.indeterminate).sum();
    let indef_muts: u64 = outs.iter().map(|o| o.indeterminate_mutations).sum();
    let cap_hits: u64 = outs.iter().map(|o| o.phantom_cap_hits).sum();
    let max_delta: u64 = outs.iter().map(|o| o.max_resend_delta).max().unwrap_or(0);
    let muts = lock(&mutations).clone();
    let indet_values = lock(&indeterminate_values).clone();
    let replayed = muts.iter().filter(|m| m.replayed).count();

    // Everything the rig owns dies HERE, before the WGL search: nine
    // busy-polling processes must not be competing with the checker for a
    // 4-vCPU runner, and the window in which an abort (see `CHECKER_STACK`)
    // could orphan them shrinks to nothing. `try_unwrap` is the proof that
    // nothing else still holds the rig — the chaos thread has been joined.
    drop(Arc::try_unwrap(rig).ok().expect("sole rig owner"));

    eprintln!(
        "[remote_lin] {tag}: entries={} ok={ok} worker_ok={worker_ok} \
         worker_indeterminate={worker_indef} kills={} restart_timeouts={} no_leader_ticks={} \
         gateway_respawns={gw_respawns} service_respawns={svc_respawns} \
         final_leader=n{leader_final} final_value={final_value:?} \
         acked_mutations={} replayed={replayed} indeterminate_mutations={indef_muts} \
         phantom_cap_hits={cap_hits} max_resend_delta={max_delta} \
         client_stats={totals:?} elapsed={:?}",
        entries.len(),
        report.kills,
        report.restart_timeouts,
        report.no_leader_ticks,
        muts.len(),
        started.elapsed(),
    );

    // Anti-vacuity: the faults really happened, and the clients really had to
    // deal with them. A run where the leader was never killed, or where no
    // client ever heard about it, proves nothing.
    assert!(report.kills >= 3, "only {} leader kills — the chaos barely ran", report.kills);
    assert_eq!(report.restart_timeouts, 0, "a restarted node never presented a fresh cnc instance");
    assert!(
        totals.redirects + totals.leader_changes >= 1,
        "no client was ever told the cluster moved: {totals:?}"
    );
    // Every gateway whose node was killed must have faulted and exited (the
    // `is_faulted` contract), and the supervisor must have brought it back.
    // `+ 1`: the last kill can land moments before the load window closes,
    // and the edge only polls its own faulted flag every 100 ms, so the very
    // last fault is allowed to go unobserved. Every earlier one is not.
    assert!(
        gw_respawns + 1 >= report.kills,
        "only {gw_respawns} gateway respawns for {} leader kills — an edge whose node instance \
         restarted under it must latch faulted and exit",
        report.kills
    );

    // The Task 9 not-serving latch + probe-before-flush make an EXPIRED
    // response impossible: a re-send can only miss the session cache if the
    // edge accepted a LATER seq than one it had refused. If this ever fires,
    // it is a product finding — do not loosen it.
    assert_eq!(
        totals.expired, 0,
        "{tag}: writes reported EXPIRED — the accepted-SUBMITs-are-a-prefix invariant broke: {totals:?}"
    );

    // Liveness: the remote path is slower than the shmem one and every chaos
    // cycle costs each worker an in-flight op, so the bar is 70% rather than
    // lin_v2's 80%.
    assert!(
        ok as u64 * 100 >= entries.len() as u64 * 70,
        "liveness: only {ok}/{} ops completed Ok (<70%)",
        entries.len()
    );

    assert_linearizable(&entries, tag);

    // --- 8. No acknowledged write was lost (envelope ON only: with the
    // envelope off an acknowledgement is not exactly-once, so there is no
    // "the" last acked write to check against — see the module doc).
    if envelope {
        assert!(!muts.is_empty(), "no acknowledged mutation at all");
        let candidates = expected_final_values(&muts, &indet_values);
        eprintln!(
            "[remote_lin] {tag}: acked-write oracle candidates={candidates:?} (ambiguous={}, \
             from {} acked mutations + {} indeterminate)",
            candidates.len() - 1,
            muts.len(),
            indet_values.len(),
        );
        let v = final_value.expect("the register was written");
        assert!(
            candidates.contains(&v),
            "an acknowledged write was LOST: the register reads {v}, but the last write that \
             could legally be the final one set one of {candidates:?} ({} acked mutations, \
             {replayed} of them replayed; {} indeterminate mutations folded in)",
            muts.len(),
            indet_values.len(),
        );
    }

}
