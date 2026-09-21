// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Plan B2 T4 (FSM upgrade lifecycle spec §3 S4 steps 4–5): the PINNED
//! install at attach, and the four refusals that guard it.
//!
//! Every test shares one shape: a single node with `RegisterSm` (`VERSION =
//! 0`) declared at row 0, five writes `0..5`, a coordinated instant at **P**
//! (so `snap-<P>.ultsnap` holds v1's true state, `value = Some(4)`), then a
//! two-frame tail above P opening with `Cas { old: 4, new: 99 }`. That CAS is
//! the whole experiment: it succeeds only against v1's true state at P, so
//! the register read back after the swap says which path ran —
//!
//! * `Some(99)`  — the pinned install: v1's artifact at P, then the tail
//!   recomputed under v2's own `apply`;
//! * `Some(8)`   — the §2.3 counterfactual: v2 replayed `[0, P)` itself, so
//!   `Write(4)` stored `2·4 = 8`, the CAS found `8 != 4` and failed.
//!
//! The two are asserted against each other (`…without_a_pin…` is the control),
//! which is what makes "the artifact really was installed" evidence rather
//! than a hope.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use uc_log::cnc::CncPage;
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig, PurgePolicy};
use uc_protocol::ring::{MpscProducer, MpscRing, RingError};
use uc_protocol::v2::cnc::CNC_SVC_STATUS_ATTACHED;
use uc_protocol::v2::frame::{self, FRAME_TYPE_MESSAGE, HEADER_LEN, align_frame_len};
use uc_protocol::v2::ipc::{MSG_V2_SUBMIT, extra_client};
use uc_service::snapshots::EnvelopeError;
use uc_service::{
    ApplyCtx, RawStateMachine, Service, ServiceBuilder, ServiceConfig, ServiceError, StateMachine,
};

use uc_lincheck::register::{Cmd as RegCmd, DoublingRegisterSm, RegisterSm};

/// A small ring and small segments, as `uc_diffreplay/tests/reconstruction.rs`
/// sizes them: nothing here needs to scroll the ring, but keeping the whole
/// instance dir small keeps these seven node bring-ups cheap.
const BUFFER_BYTES: usize = 1 << 16;
const SEGMENT_BYTES: u64 = 16 * 1024;
const CLIENT_ID: u32 = 7;

/// `RegisterSm` takes the trait's default `VERSION`; `DoublingRegisterSm`
/// declares 2. Read from the types rather than written as literals so a
/// change to either is a compile-time relocation, not a silently wrong pin.
const V1: u32 = <RegisterSm as StateMachine>::VERSION;
const V2: u32 = <DoublingRegisterSm as StateMachine>::VERSION;

/// v1 writes `0..WRITES`; its register at P therefore holds `LAST_WRITE`.
const WRITES: u64 = 5;
const LAST_WRITE: u64 = WRITES - 1;
/// The post-P tail's first frame: one CAS keyed on v1's TRUE state at P (see
/// the module doc).
const CAS_NEW: u64 = 99;
/// The tail's second frame: a CAS that matches under NEITHER path, so it
/// changes no outcome. It is there so the tail is two frames and the last of
/// them starts strictly ABOVE P — the shape
/// [`a_durable_sm_above_the_origin_is_rewound_to_it`] needs (the frame at
/// exactly P is the first one, since P is an exclusive frontier).
const NEVER_MATCHES: u64 = 12_345;
/// What v2 computes for itself when it replays `[0, P)` — `Write(4)` doubled.
const COUNTERFACTUAL: u64 = 2 * LAST_WRITE;

// --------------------------------------------------------------------- harness

fn tempdir() -> tempfile::TempDir {
    tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap()
}

fn cfg(dir: &Path, app_id: &str) -> ServiceConfig {
    ServiceConfig::new(dir, app_id)
}

fn open_cnc(dir: &Path, app_id: &str) -> Arc<CncPage> {
    CncPage::open_file(&dir.join("cnc2.dat"), app_id).unwrap()
}

fn open_ingress(dir: &Path) -> MpscProducer {
    let ring = MpscRing::open(&dir.join("ingress.ring")).unwrap();
    let (prod, _consumer) = ring.into_split();
    prod
}

fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !f() {
        assert!(Instant::now() < deadline, "{what}: condition never held");
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn start_node(dir: &Path, app_id: &str) -> Node {
    let bind: SocketAddr = "127.0.0.1:0".parse().unwrap();
    Node::start(NodeConfig {
        id: 0,
        members: vec![(0, bind)],
        bind,
        instance_dir: dir.to_path_buf(),
        app_id: app_id.into(),
        buffer_bytes: BUFFER_BYTES,
        max_payload: 256,
        admission_bytes_default: 256 * 1024,
        settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),
        force_jumbo_frames: false,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 1,
        faults: FaultConfig::default(),
        purge: PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: SEGMENT_BYTES,
        crypto: uc_node::CryptoConfig::Disabled,
        services: uc_node::ServicesConfig::single(<RegisterSm as StateMachine>::NAME),
    })
    .unwrap()
}

/// Submit one `Cmd` through the real ingress ring (no response awaited — this
/// works whether or not a service is attached).
fn submit(prod: &MpscProducer, local_seq: u32, cmd: &RegCmd) {
    let payload = bincode::serde::encode_to_vec(cmd, bincode::config::standard()).unwrap();
    let extra = extra_client(CLIENT_ID, local_seq);
    for attempt in 0.. {
        match prod.try_write(MSG_V2_SUBMIT, 0, extra, &payload) {
            Ok(()) => return,
            Err(RingError::Full) => {
                assert!(attempt < 200_000, "ingress ring never drained");
                std::thread::sleep(Duration::from_micros(50));
            }
            Err(e) => panic!("submit failed: {e}"),
        }
    }
}

/// `append == commit == durable`, held stable — the pipeline is drained.
/// (`uc_service/tests/reconstruction.rs`'s `wait_commit_covers_all` minus its
/// `append > 4096` floor: this fixture writes six tiny frames, not 2000.)
fn wait_drained(node: &Node) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = 0u64;
    let mut stable_since = Instant::now();
    loop {
        let c = node.counters();
        let (append, commit, durable) = (
            c.append.load_acquire(),
            c.commit.load_acquire(),
            c.durable.load_acquire(),
        );
        if append > 0 && append == commit && append == durable {
            if append == last {
                if stable_since.elapsed() > Duration::from_millis(200) {
                    return;
                }
            } else {
                last = append;
                stable_since = Instant::now();
            }
        } else {
            last = 0;
            stable_since = Instant::now();
        }
        assert!(
            Instant::now() < deadline,
            "commit never covered all submits (append={append} commit={commit} durable={durable})"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// `uc2ctl snapshot`, in process: command an instant and return **P**.
fn command_instant(node: &Node) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match node.command_snapshot(false) {
            Ok(p) => return p,
            Err(uc_node::SnapshotRefusal::Retry) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(e) => panic!("uc2ctl snapshot refused: {e}"),
        }
    }
}

/// The row's published applied frontier has reached `min(commit, durable)` —
/// the apply loop has finished reconstructing and caught up.
fn wait_service_caught_up(cnc: &CncPage) {
    wait_until("service caught up", || {
        let c = cnc.counters();
        let target = c.commit.load_acquire().min(c.durable.load_acquire());
        target > 0 && cnc.service_slot(0).applied.load_acquire() >= target
    });
}

fn query_v1(svc: &Service<RegisterSm>) -> Option<u64> {
    svc.query(())
}

fn query_v2(svc: &Service<DoublingRegisterSm>) -> Option<u64> {
    svc.query(())
}

/// The shared fixture: a live node whose row 0 holds v1's artifact at
/// [`Fixture::p`] and one CAS frame above it. The v1 service has stopped, so
/// `service.0.lock` is free for the attach under test.
struct Fixture {
    dir: tempfile::TempDir,
    app: &'static str,
    node: Node,
    p: u64,
}

impl Fixture {
    fn new(app: &'static str) -> Fixture {
        let dir = tempdir();
        let node = start_node(dir.path(), app);
        wait_until("node can serve", || node.can_serve());

        let svc1 = ServiceBuilder::new(cfg(dir.path(), app), RegisterSm::default())
            .start_with_snapshots()
            .unwrap();
        let prod = open_ingress(dir.path());
        for v in 0..WRITES {
            submit(&prod, v as u32 + 1, &RegCmd::Write(v));
        }
        wait_drained(&node);
        let cnc = open_cnc(dir.path(), app);
        wait_service_caught_up(&cnc);
        assert_eq!(query_v1(&svc1), Some(LAST_WRITE), "v1's state before P");

        let p = command_instant(&node);
        let art = artifact_path(dir.path(), p);
        wait_until("row 0 published its artifact", || art.is_file());

        // The tail above P: one CAS that only v1's true state at P satisfies,
        // then one that nothing satisfies (see `NEVER_MATCHES`).
        submit(
            &prod,
            WRITES as u32 + 1,
            &RegCmd::Cas {
                old: LAST_WRITE,
                new: CAS_NEW,
            },
        );
        submit(
            &prod,
            WRITES as u32 + 2,
            &RegCmd::Cas {
                old: NEVER_MATCHES,
                new: 0,
            },
        );
        wait_drained(&node);
        wait_service_caught_up(&cnc);
        assert_eq!(query_v1(&svc1), Some(CAS_NEW), "v1 applied its own tail");
        svc1.stop();

        Fixture { dir, app, node, p }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    fn cnc(&self) -> Arc<CncPage> {
        open_cnc(self.path(), self.app)
    }

    fn artifact(&self) -> std::path::PathBuf {
        artifact_path(self.path(), self.p)
    }

    /// Write the row's pin words directly on the page. Legitimate here: the
    /// node's `uc2-cluster` agent only stores pin words for a row its FSM
    /// state actually pins (`publish_view`'s `if let Some(p) =
    /// st.pin_for(row)`), and no test in this file commits an `UpgradePin`
    /// command — so the single-writer rule on those four words holds.
    fn pin(&self, from: u32, to: u32) {
        self.cnc()
            .service_slot(0)
            .status
            .store_pin(self.p, from, to);
    }

    fn stop(self) {
        self.node.stop();
    }
}

/// Row 0's artifact for the instant at `p`.
fn artifact_path(dir: &Path, p: u64) -> std::path::PathBuf {
    dir.join("snapshots")
        .join("0")
        .join(format!("snap-{p}.ultsnap"))
}

// --------------------------------------------------------------------- tests

/// Spec §3 S4 step 5: the pin names the version that may serve this row, and
/// the binary that is not it is refused BY NAME — before a single slot word
/// is written.
#[test]
fn a_stale_binary_is_refused_by_name_after_the_pin() {
    let f = Fixture::new("pin-stale");
    let cnc = f.cnc();
    let applied_before = cnc.service_slot(0).applied.load_acquire();
    f.pin(V1, V2);

    let err = ServiceBuilder::new(cfg(f.path(), f.app), RegisterSm::default())
        .start_with_snapshots()
        .err()
        .expect("refused");
    assert!(
        matches!(
            err,
            ServiceError::PinnedVersionMismatch {
                row: 0,
                pinned: V2,
                mine: V1,
                ..
            }
        ),
        "{err}"
    );
    assert_eq!(
        cnc.service_slot(0).status.load_acquire() & CNC_SVC_STATUS_ATTACHED,
        0,
        "nothing was written to the slot"
    );
    assert_eq!(
        cnc.service_slot(0).applied.load_acquire(),
        applied_before,
        "a refused attach does not republish `applied`"
    );
    f.stop();
}

/// Spec §3 S4 step 4: the pinned version installs the artifact at the origin
/// and recomputes the tail under its OWN apply. `Some(99)` is the CAS
/// succeeding against v1's true state at P — see the module doc.
#[test]
fn the_pinned_version_installs_the_origin_unconditionally_and_recomputes_the_tail() {
    let f = Fixture::new("pin-install");
    f.pin(V1, V2);

    let svc2 = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start_with_snapshots()
        .unwrap();
    let cnc = f.cnc();
    wait_service_caught_up(&cnc);
    assert_eq!(
        query_v2(&svc2),
        Some(CAS_NEW),
        "the artifact at P was installed, then the tail recomputed under v2"
    );
    svc2.stop();
    f.stop();
}

/// The control that makes the assertion above evidence: the SAME swap with no
/// pin computes the §2.3 counterfactual instead.
#[test]
fn the_same_swap_without_a_pin_computes_the_counterfactual() {
    let f = Fixture::new("pin-none");

    let svc2 = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start_with_snapshots()
        .unwrap();
    let cnc = f.cnc();
    wait_service_caught_up(&cnc);
    assert_eq!(
        query_v2(&svc2),
        Some(COUNTERFACTUAL),
        "unpinned, v2 replays [0, P) itself: Write(4) stores 8 and the CAS fails"
    );
    svc2.stop();
    f.stop();
}

/// Spec §3 S4 step 4, the "unconditionally" clause: a DURABLE state machine
/// that already carries state above the origin is REWOUND to it, and
/// recomputes the tail exactly as its fresh peers do.
///
/// The above-P instance is built the way a real one arises — v2's own apply
/// over the archived frames from genesis, i.e. the counterfactual state an
/// unpinned swap would have persisted — so the assertion below is "the pinned
/// path overrode a durable disagreeing state", not "a fresh SM was installed
/// into".
#[test]
fn a_durable_sm_above_the_origin_is_rewound_to_it() {
    let f = Fixture::new("pin-rewind");
    let cnc = f.cnc();
    let end = {
        let c = cnc.counters();
        c.commit.load_acquire().min(c.durable.load_acquire())
    };

    let mut durable = DoublingRegisterSm::default();
    replay_from_genesis(&mut durable, &f.path().join("journal"), end);
    assert!(
        StateMachine::last_applied(&durable) > Some(f.p),
        "precondition: the durable SM must sit ABOVE the origin (last_applied={:?}, P={})",
        StateMachine::last_applied(&durable),
        f.p
    );
    assert_eq!(
        StateMachine::query(&durable, ()),
        Some(COUNTERFACTUAL),
        "precondition: it carries the counterfactual, not v1's history"
    );

    f.pin(V1, V2);
    let svc2 = ServiceBuilder::new(cfg(f.path(), f.app), durable)
        .start_with_snapshots()
        .unwrap();
    wait_service_caught_up(&cnc);
    assert_eq!(
        query_v2(&svc2),
        Some(CAS_NEW),
        "the install rewound the durable SM to the origin and the tail was recomputed"
    );
    svc2.stop();
    f.stop();
}

/// A pinned row MUST install, and only `start_with_snapshots` carries the
/// capability — a plain `start()` on a pinned row is refused by name rather
/// than silently replaying under the new version.
#[test]
fn a_pinned_row_started_without_snapshots_is_refused() {
    let f = Fixture::new("pin-nosnap");
    f.pin(V1, V2);

    let err = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start()
        .err()
        .expect("refused");
    assert!(
        matches!(err, ServiceError::PinRequiresSnapshots { row: 0, .. }),
        "{err}"
    );
    f.stop();
}

/// The set at the origin was pruned or never fetched: named, with the path,
/// instead of falling back to some other artifact or to genesis.
#[test]
fn a_pinned_origin_with_no_artifact_is_refused() {
    let f = Fixture::new("pin-missing");
    std::fs::remove_file(f.artifact()).unwrap();
    f.pin(V1, V2);

    let err = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start_with_snapshots()
        .err()
        .expect("refused");
    assert!(
        matches!(err, ServiceError::PinnedArtifactMissing { row: 0, .. }),
        "{err}"
    );
    f.stop();
}

/// Plan B2 erratum: the pinned install cross-checks the artifact against the
/// pin's `from`, not against `S::VERSION`. A pin naming a `from` no artifact
/// on this node was built by is refused by name.
#[test]
fn a_pinned_artifact_built_by_the_wrong_version_is_refused() {
    let f = Fixture::new("pin-wrongfrom");
    f.pin(3, V2);

    let err = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start_with_snapshots()
        .err()
        .expect("refused");
    assert!(
        matches!(
            err,
            ServiceError::MistaggedSnapshot {
                source: EnvelopeError::VersionMismatch {
                    built: V1,
                    expected: 3
                },
                ..
            }
        ),
        "{err}"
    );
    f.stop();
}

/// `PinRead::Contended` is "could not read", never "no pin": a reader that
/// must DECIDE refuses rather than attaching unpinned off a half-published
/// triple.
#[test]
fn a_contended_pin_read_is_refused_not_ignored() {
    let f = Fixture::new("pin-contended");
    // Hold the seqlock open: the odd bump plus `from`/`to`, with the origin
    // store and the closing bump left undone.
    f.cnc()
        .service_slot(0)
        .status
        .store_pin_begin_for_test(V1, V2);

    let err = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start_with_snapshots()
        .err()
        .expect("refused");
    assert!(
        matches!(err, ServiceError::PinUnreadable { row: 0 }),
        "{err}"
    );

    // And it is not a permanent wedge: finishing the store makes the SAME
    // attach take the pinned path.
    f.cnc()
        .service_slot(0)
        .status
        .store_pin_finish_for_test(f.p);
    let svc2 = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start_with_snapshots()
        .unwrap();
    let cnc = f.cnc();
    wait_service_caught_up(&cnc);
    assert_eq!(query_v2(&svc2), Some(CAS_NEW));
    svc2.stop();
    f.stop();
}

// ------------------------------------------------- the durable-SM stand-in

/// Walk the archived log `[0, end)` and apply every MESSAGE frame through
/// `sm`'s own `apply`, with the RECORDED header values — the same dispatch
/// `uc_service::replay` and `uc_diffreplay::drive` do, minus the live-rejoin
/// machinery. This is how the above-origin durable instance is built: it IS
/// what an unpinned v2 attach computes for itself.
fn replay_from_genesis<S: RawStateMachine>(sm: &mut S, journal_dir: &Path, end: u64) {
    let reader = uc_journal::TailReader::open(journal_dir).unwrap();
    let identity = S::IDENTITY;
    let mut resp = Vec::with_capacity(256);
    reader
        .scan_from(0, |_seq, base, block| {
            let mut off = 0usize;
            while off + HEADER_LEN <= block.len() {
                let hdr = frame::read_header(&block[off..]);
                let total = hdr.length as usize;
                let aligned = align_frame_len(total);
                if total < HEADER_LEN || off + aligned > block.len() {
                    break;
                }
                let pos = base + off as u64;
                if pos.saturating_add(aligned as u64) > end {
                    return false;
                }
                if hdr.frame_type == FRAME_TYPE_MESSAGE && Some(pos) > sm.last_applied() {
                    let mut ctx = ApplyCtx::new(pos, identity)
                        .with_time(hdr.time_ns)
                        .with_term(hdr.leadership_term_id);
                    resp.clear();
                    sm.apply(&mut ctx, &block[off + HEADER_LEN..off + total], &mut resp);
                }
                off += aligned;
            }
            true
        })
        .unwrap();
}
