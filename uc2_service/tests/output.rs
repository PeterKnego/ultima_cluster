// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Task 12 capstone: `OutputHandler` is leader-only and at-least-once. A
//! single node + service submit 20 `Add(1)`; the installed `Recorder` returns
//! one `Retryable` on its very first call (pinning the retry-with-backoff
//! path) then succeeds for the rest. The node durably persists the progress
//! marker. The service is then hard-crashed (`svc.crash()` — no graceful
//! teardown) and a FRESH incarnation attaches with a fresh `Recorder`
//! (sharing the same `seen` sink): five more `Add(1)` land, and every position
//! above the marker is delivered again — the at-least-once contract — with no
//! committed position ever SKIPPED (`is_contiguous_positions`, walked against
//! the journal via `TailReader`).

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use uc2_client::Client;
use uc2_log::cnc::CncPage;
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};
use uc2_service::{NoopOutput, OutputError, OutputHandler, ServiceBuilder, ServiceConfig, StateMachine};
use uc_protocol::v2::frame::{self, FRAME_TYPE_MESSAGE, HEADER_LEN, align_frame_len};
use ultima_journal::TailReader;

/// `cargo test` runs every `#[test]` fn in this file as a separate OS thread
/// within the SAME process by default. `output_thread_spawns_only_for_a_real_handler`
/// asserts on process-wide thread PRESENCE (`/proc/self/task`), so it must not
/// run concurrently with the capstone test below (which also spawns a
/// `uc2-output` thread) — this lock serializes the two.
static TEST_SERIAL: Mutex<()> = Mutex::new(());

// ------------------------------------------------------------- the state machine

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Cmd {
    Add(u64),
}

/// A running total; the response is the total AFTER applying.
#[derive(Default)]
struct CountSm {
    total: u64,
    last_applied: Option<u64>,
}

impl StateMachine for CountSm {
    type Command = Cmd;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, position: u64, cmd: Cmd) -> u64 {
        let Cmd::Add(n) = cmd;
        self.total += n;
        self.last_applied = Some(position);
        self.total
    }

    fn query(&self, _q: ()) -> u64 {
        self.total
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

// --------------------------------------------------------------- the handler

/// Records every position it is asked to handle into a shared sink. Its very
/// first call (guarded by `fail_first`) returns `Retryable` once, pinning the
/// output agent's bounded-backoff retry path; every other call (including the
/// retried one) succeeds and records the position.
struct Recorder {
    seen: Arc<Mutex<Vec<u64>>>,
    fail_first: AtomicBool,
}

impl OutputHandler<CountSm> for Recorder {
    async fn on_committed(
        &self,
        position: u64,
        _cmd: &Cmd,
        _state: &CountSm,
    ) -> Result<(), OutputError> {
        if self.fail_first.swap(false, Ordering::SeqCst) {
            return Err(OutputError::Retryable("pinned first-call failure".into()));
        }
        self.seen.lock().unwrap().push(position);
        Ok(())
    }
}

// --------------------------------------------------------------------- harness

fn start_single_node(dir: &Path, app_id: &str) -> Node {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Node::start(NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: app_id.into(),
        buffer_bytes: 1 << 20,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
        purge: uc2_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc2_node::CryptoConfig::Disabled,
        services: uc2_node::ServicesConfig::default(),
    })
    .unwrap()
}

fn cfg(dir: &Path, app_id: &str) -> ServiceConfig {
    ServiceConfig::new(dir, app_id)
}

fn open_cnc(dir: &Path, app_id: &str) -> Arc<CncPage> {
    CncPage::open_file(&dir.join("cnc2.dat"), app_id).unwrap()
}

fn wait_until(mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(Instant::now() < deadline, "condition never held");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Named OS threads currently alive in this process (Linux `/proc/self/task`),
/// via `/proc/<tid>/comm` — the name `AgentRunner::spawn` gives its thread.
/// Used to pin the non-negotiable "spawn only for a real handler" contract
/// without exposing a test-only accessor on `Service`.
fn thread_names() -> Vec<String> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir("/proc/self/task") else { return out };
    for entry in entries.flatten() {
        if let Ok(comm) = std::fs::read_to_string(entry.path().join("comm")) {
            out.push(comm.trim().to_string());
        }
    }
    out
}

// ------------------------------------------------------------------------ test

/// The builder's default path (no `.output_handler` call, `O = NoopOutput`)
/// must NOT spawn the output thread — spawning a thread that can only ever
/// run a no-op duty cycle would be pure waste. Installing a real handler
/// (`Recorder`) DOES spawn it. Pins the "spawn only for a real handler"
/// contract directly (thread presence/absence), independent of the capstone
/// test above (which only ever exercises the real-handler path).
#[test]
fn output_thread_spawns_only_for_a_real_handler() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let node = start_single_node(dir.path(), "spawn");
    wait_until(|| node.can_serve());

    let before = thread_names();
    assert!(
        !before.iter().any(|n| n == "uc2-output"),
        "no output thread before any service attaches: {before:?}"
    );

    let svc_noop =
        ServiceBuilder::new(cfg(dir.path(), "spawn"), CountSm::default()).start().unwrap();
    assert!(
        !thread_names().iter().any(|n| n == "uc2-output"),
        "default (NoopOutput) builder path must not spawn the output thread"
    );
    svc_noop.stop();

    let seen = Arc::new(Mutex::new(Vec::<u64>::new()));
    let svc_real = ServiceBuilder::new(cfg(dir.path(), "spawn"), CountSm::default())
        .output_handler(Recorder { seen, fail_first: AtomicBool::new(false) })
        .start()
        .unwrap();
    // A NEW thread's name (`pthread_setname_np`) is set by the thread ITSELF
    // early in its startup, not by the parent's `spawn` call — so there is a
    // short window right after `start()` returns where the thread exists but
    // hasn't named itself yet. Poll rather than check once immediately.
    wait_until(|| thread_names().iter().any(|n| n == "uc2-output"));
    svc_real.stop();

    node.stop();
}

/// M12a Task 2+3 review carry-over: `is_noop_output` recognizes not just the
/// bare `NoopOutput` (the builder's default, covered above) but ALSO
/// `TypedOutput<NoopOutput>` — what `.output_handler(NoopOutput)` produces —
/// so an EXPLICIT call installing the typed no-op handler must also skip the
/// output thread spawn, not just the implicit default-builder path. Same
/// thread-presence technique as `output_thread_spawns_only_for_a_real_handler`
/// above, serialized on the same `TEST_SERIAL` lock.
#[test]
fn output_handler_explicit_noop_spawns_no_thread() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let node = start_single_node(dir.path(), "explicit-noop");
    wait_until(|| node.can_serve());

    let before = thread_names();
    assert!(
        !before.iter().any(|n| n == "uc2-output"),
        "no output thread before any service attaches: {before:?}"
    );

    let svc = ServiceBuilder::new(cfg(dir.path(), "explicit-noop"), CountSm::default())
        .output_handler(NoopOutput)
        .start()
        .unwrap();
    assert!(
        !thread_names().iter().any(|n| n == "uc2-output"),
        "explicit .output_handler(NoopOutput) (routed through TypedOutput<NoopOutput>) \
         must not spawn the output thread"
    );
    svc.stop();

    node.stop();
}

#[test]
fn output_runs_leader_only_at_least_once_across_service_restart() {
    let _serial = TEST_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    // Walk every `MESSAGE` frame position archived in the journal (via
    // `TailReader`, the same read-only primitive `uc2_service::replay` uses)
    // and check that `sorted` — the deduped positions the `Recorder` ever saw
    // across BOTH incarnations — covers every one of them from its own first
    // entry onward. At-least-once permits duplicates (already folded out by
    // `dedup`) but never a SKIP. Note the anchor: the walk starts at
    // `sorted[0]`, so this proves "no gap after the FIRST seen position" —
    // in general it would not catch a skip BEFORE the first delivery. That
    // earliest-skip case is covered in THIS test anyway: incarnation 1
    // starts from marker 0 and delivers from the true first committed
    // position, so `sorted[0]` IS the journal's first MESSAGE position. A
    // closure (not a free fn) so it can capture `dir` without threading it
    // through the call site.
    let is_contiguous_positions = |sorted: &[u64]| -> bool {
        if sorted.is_empty() {
            return true;
        }
        let first = sorted[0];
        let reader = TailReader::open(&dir.path().join("journal")).unwrap();
        let mut archived = Vec::new();
        reader
            .scan(|_seq, base, payload| {
                let mut off = 0usize;
                while off + HEADER_LEN <= payload.len() {
                    let hdr = frame::read_header(&payload[off..]);
                    let total = hdr.length as usize;
                    let aligned = align_frame_len(total);
                    if total < HEADER_LEN || off + aligned > payload.len() {
                        break;
                    }
                    let pos = base + off as u64;
                    if hdr.frame_type == FRAME_TYPE_MESSAGE && pos >= first {
                        archived.push(pos);
                    }
                    off += aligned;
                }
                true
            })
            .unwrap();
        archived.sort_unstable();
        archived.as_slice() == sorted
    };
    let node = start_single_node(dir.path(), "out");
    wait_until(|| node.can_serve());
    let seen = Arc::new(Mutex::new(Vec::<u64>::new()));
    let svc = ServiceBuilder::new(cfg(dir.path(), "out"), CountSm::default())
        .output_handler(Recorder { seen: seen.clone(), fail_first: AtomicBool::new(true) })
        .start()
        .unwrap();
    let client = Client::connect(dir.path(), "out").unwrap();
    for _ in 0..20 {
        let _: u64 = client.submit(&Cmd::Add(1)).unwrap();
    }
    wait_until(|| seen.lock().unwrap().len() >= 20); // Retryable on the first call retried, then all delivered
    // The node's consensus duty cycle samples+persists `output_completed` on
    // its OWN schedule (Task 12): the first-ever change persists immediately
    // (no floor wait), so this is normally instantaneous, but under system
    // load the consensus thread's next cycle can simply not have RUN yet —
    // poll rather than assert once immediately after the proxy condition
    // above (same reasoning as the `is_contiguous_positions` poll below).
    let cnc = open_cnc(dir.path(), "out");
    wait_until(|| cnc.status().output_progress.load_acquire() > 0);
    let marker0 = cnc.status().output_progress.load_acquire();
    assert!(marker0 > 0, "node persisted the progress marker");
    // hard service restart: marker floors the replay; positions >= marker re-delivered (at-least-once)
    svc.crash();
    let svc2 = ServiceBuilder::new(cfg(dir.path(), "out"), CountSm::default())
        .output_handler(Recorder { seen: seen.clone(), fail_first: AtomicBool::new(false) })
        .start()
        .unwrap();
    for _ in 0..5 {
        let _: u64 = client.submit(&Cmd::Add(1)).unwrap();
    }
    // `>=`, not the brief's `>`: frames are contiguous, so whenever the marker
    // happens to fully catch up to the pre-crash frontier before the crash,
    // the FIRST new submit's own position is numerically EQUAL to `marker0`
    // (frame N's end == frame N+1's start). A strict `>` then excludes it,
    // making "5 of the 5 new submits" permanently impossible (only 4 ever
    // qualify) — a genuine hang, reproduced directly, not a hypothetical.
    // `>=` matches this test's OWN preceding comment ("positions >= marker0
    // re-delivered"); see the task report for the full repro.
    wait_until(|| seen.lock().unwrap().iter().filter(|&&p| p >= marker0).count() >= 5);
    // That count is still only a coarse proxy: because `marker0` legitimately
    // lags (the 100 ms persist floor), `svc2` redelivers the WHOLE `(marker0,
    // pre-crash frontier]` range on top of the 5 new submits — often more than
    // 5 positions on its own — so the count condition can trip before the
    // output thread has actually caught up to the last of the 5 NEW commits
    // (already committed/archived by the time `client.submit` returns, just
    // not yet delivered). Poll for the real finishing condition (full
    // contiguous coverage) rather than asserting once right after the proxy —
    // same final check, just given the bounded time to actually converge.
    wait_until(|| {
        let mut sorted = seen.lock().unwrap().clone();
        sorted.sort_unstable();
        sorted.dedup();
        is_contiguous_positions(&sorted)
    });
    let mut sorted = seen.lock().unwrap().clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert!(is_contiguous_positions(&sorted), "no committed position skipped");
    svc2.stop();
    node.stop();
}
