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

use uc_log::cnc::{AdminReq, CncPage, PinRead};
use uc_net::fault::FaultConfig;
use uc_node::{Node, NodeConfig, PurgePolicy};
use uc_protocol::ring::{MpscProducer, MpscRing, RingError};
use uc_protocol::v2::cnc::ADMIN_OP_UPGRADE_PIN;
use uc_protocol::v2::cnc::CNC_SVC_STATUS_ATTACHED;
use uc_protocol::v2::frame::{self, FRAME_TYPE_MESSAGE, HEADER_LEN, align_frame_len};
use uc_protocol::v2::ipc::{MSG_V2_SUBMIT, extra_client};
use uc_protocol::v2::upgrade::{UpgradePin, encode_upgrade_pin};
use uc_service::snapshots::{EnvelopeError, SnapshotStore, verify_snapshot_envelope};
use uc_service::{
    ApplyCtx, RawStateMachine, Service, ServiceBuilder, ServiceConfig, ServiceError,
    SnapshotStateMachine, StateMachine,
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

/// v1 writes `Write(i % MODULUS)`; every `writes` count is a multiple of
/// `MODULUS`, so its register at P always holds `LAST_WRITE`.
const MODULUS: u64 = 5;
const LAST_WRITE: u64 = MODULUS - 1;
/// The PURGING fixture's shape: small journal segments so a few hundred KiB
/// of log rolls many of them and the purge below P is real; `PURGE_FILLERS`
/// frames above P then scroll the 64 KiB ring past P.
const PURGE_SEGMENT_BYTES: u64 = 8 * 1024;
const PURGE_WRITES: u64 = 4_000;
const PURGE_FILLERS: u64 = 2_000;
const PURGE_EXTRA_INSTANTS: u64 = 400;
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

/// Bounded wait that REPORTS rather than panics — for a condition that is
/// allowed not to hold (a skipped instant publishes no artifact).
fn wait_for(limit: Duration, mut f: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + limit;
    while !f() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

fn start_node(dir: &Path, app_id: &str, purge: PurgePolicy, segment_bytes: u64) -> Node {
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
        purge,
        learners: Vec::new(),
        journal_segment_bytes: segment_bytes,
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
    /// The pin [`Fixture::pin_at`] last faked, for [`Fixture::assert_pin_held`].
    faked_pin: std::cell::Cell<Option<(u64, u32, u32)>>,
}

/// How a [`Fixture`] is built. [`Spec::small`] is the purge-off shape every
/// refusal test uses; [`Spec::purging`] is spec S4's production posture.
struct Spec {
    purge: PurgePolicy,
    segment_bytes: u64,
    /// `Write(i % MODULUS)` this many times before the instant. A multiple of
    /// `MODULUS`, so v1's register at P is always `LAST_WRITE`.
    writes: u64,
    /// Extra never-matching CAS frames after the tail's first two, to scroll
    /// the live ring past P — so the pinned attach's follower OVERRUNS and
    /// the reconstruction path (`uc_service::replay`) runs.
    fillers: u64,
    /// Extra coordinated instants commanded after the row's last write, each
    /// appending a header-only `SNAPSHOT` frame the row does NOT apply. They
    /// push the instant **P** far above the row's last applied MESSAGE, which
    /// is what puts the artifact's internal cursor below the purge floor —
    /// the shape in which `replay_into`'s gap guard actually fires after a
    /// pinned install. See `a_pinned_attach_converges_on_a_purging_cluster`.
    extra_instants: u64,
}

impl Spec {
    fn small() -> Spec {
        Spec {
            purge: PurgePolicy::Disabled,
            segment_bytes: SEGMENT_BYTES,
            writes: MODULUS,
            fillers: 0,
            extra_instants: 0,
        }
    }

    fn purging() -> Spec {
        Spec {
            purge: PurgePolicy::BelowSnapshot { slack_bytes: 0 },
            segment_bytes: PURGE_SEGMENT_BYTES,
            writes: PURGE_WRITES,
            fillers: PURGE_FILLERS,
            extra_instants: PURGE_EXTRA_INSTANTS,
        }
    }
}

impl Fixture {
    fn new(app: &'static str) -> Fixture {
        Fixture::build(app, Spec::small())
    }

    fn build(app: &'static str, spec: Spec) -> Fixture {
        let (f, svc1) = Fixture::build_with_v1(app, spec);
        svc1.stop();
        f
    }

    /// [`Fixture::build`] with the v1 service still ATTACHED and handed back,
    /// for the tests that commit a REAL pin while v1 runs. Since #33 that
    /// v1 stops at the pin record ([`wait_stopped_at_the_pin`]); before it,
    /// v1 kept applying past the pin (spec §3 S4's window, now closed).
    fn build_with_v1(app: &'static str, spec: Spec) -> (Fixture, Service<RegisterSm>) {
        let dir = tempdir();
        let node = start_node(dir.path(), app, spec.purge, spec.segment_bytes);
        wait_until("node can serve", || node.can_serve());

        let svc1 = ServiceBuilder::new(cfg(dir.path(), app), RegisterSm::default())
            .start()
            .unwrap();
        let prod = open_ingress(dir.path());
        let mut seq = 0u32;
        for i in 0..spec.writes {
            seq += 1;
            submit(&prod, seq, &RegCmd::Write(i % MODULUS));
        }
        wait_drained(&node);
        let cnc = open_cnc(dir.path(), app);
        wait_service_caught_up(&cnc);
        assert_eq!(query_v1(&svc1), Some(LAST_WRITE), "v1's state before P");

        // The instant. `extra_instants` of them are commanded first: each is a
        // header-only `SNAPSHOT` frame the row skips, so they lift P away from
        // the row's last applied MESSAGE without changing its state.
        let mut p = command_instant(&node);
        for _ in 0..spec.extra_instants {
            p = command_instant(&node);
        }
        // An instant whose builder was busy publishes nothing (`freeze` is
        // skipped, `SNAPSHOT_SKIPPED_BUSY`), so keep commanding until one
        // lands rather than waiting forever on a skipped one — bounded, so a
        // row that never publishes fails here with the position it was stuck
        // on instead of hanging the suite.
        let instant_deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let art = artifact_path(dir.path(), p);
            if wait_for(Duration::from_millis(500), || art.is_file()) {
                break;
            }
            assert!(
                Instant::now() < instant_deadline,
                "row 0 published no artifact for any commanded instant \
                 (last P={p}); every freeze was skipped busy"
            );
            p = command_instant(&node);
        }
        if !matches!(spec.purge, PurgePolicy::Disabled) {
            // The complete set at P moves the durable snapshot floor, which
            // commands the purge; the archive acks by advancing its first
            // base (the persist is throttled, so this is a wait, not a poll).
            wait_until("purge advanced the archive floor", || {
                node.archive_first_base() > 0
            });
        }

        // The tail above P: one CAS that only v1's true state at P satisfies,
        // then never-matching ones (see `NEVER_MATCHES`) — at least one, so
        // the tail's LAST frame starts strictly above P.
        seq += 1;
        submit(
            &prod,
            seq,
            &RegCmd::Cas {
                old: LAST_WRITE,
                new: CAS_NEW,
            },
        );
        for _ in 0..=spec.fillers {
            seq += 1;
            submit(
                &prod,
                seq,
                &RegCmd::Cas {
                    old: NEVER_MATCHES,
                    new: 0,
                },
            );
        }
        wait_drained(&node);
        wait_service_caught_up(&cnc);
        assert_eq!(query_v1(&svc1), Some(CAS_NEW), "v1 applied its own tail");
        if spec.fillers > 0 {
            let append = node.counters().append.load_acquire();
            assert!(
                append > p + BUFFER_BYTES as u64,
                "the ring must have scrolled PAST P so the pinned attach's \
                 follower overruns: append={append}, P={p}, capacity={BUFFER_BYTES}"
            );
        }
        (
            Fixture {
                dir,
                app,
                node,
                p,
                faked_pin: std::cell::Cell::new(None),
            },
            svc1,
        )
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

    /// Write the row's pin words directly on the page. Legitimate here: no
    /// test in this file commits an `UpgradePin` command through this path,
    /// so the node's FSM holds no pin for the row.
    ///
    /// The node republishes EVERY row's pin and running words from COMMITTED
    /// cluster state on every cluster publish (`ClusterAgent::publish_view`),
    /// so a faked pin must be written AFTER the last cluster record: a
    /// `SnapshotReport` (or any other record) that commits later wipes it,
    /// and the attach then reads "unpinned, running v1" and refuses with
    /// `VersionMismatch`. [`Fixture::pin_at`] therefore waits for the cluster
    /// FSM to be quiet first ([`Fixture::wait_cluster_quiet`]), and
    /// [`Fixture::assert_pin_held`] re-checks the slot right before a start.
    fn pin(&self, from: u32, to: u32) {
        self.pin_at(self.p, from, to);
    }

    /// [`Fixture::pin`] at an origin other than the fixture's instant.
    fn pin_at(&self, origin: u64, from: u32, to: u32) {
        self.wait_cluster_quiet();
        self.cnc()
            .service_slot(0)
            .status
            .store_pin(origin, from, to);
        self.faked_pin.set(Some((origin, from, to)));
    }

    /// Block until no cluster record can still be in flight: the log is
    /// drained (`append == commit == durable`, so any accepted record is
    /// committed), every set in the committed catalog is agreed (its reports
    /// have all been applied), and the cluster agent's walk has consumed to
    /// commit — all held across a short settle.
    fn wait_cluster_quiet(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut quiet_since: Option<(Instant, u64)> = None;
        loop {
            let c = self.node.counters();
            let (append, commit, durable) = (
                c.append.load_acquire(),
                c.commit.load_acquire(),
                c.durable.load_acquire(),
            );
            let view = self.node.cluster_view();
            let agreed = view.snapshot_inner().catalog.iter().all(|e| e.is_agreed());
            let walked = view.consumed.load(std::sync::atomic::Ordering::Acquire);
            let quiet = append > 0
                && append == commit
                && append == durable
                && agreed
                && walked >= commit
                && self.cnc().cluster_applied() <= walked;
            match (quiet, quiet_since) {
                (true, Some((since, at))) if at == commit => {
                    if since.elapsed() > Duration::from_millis(200) {
                        return;
                    }
                }
                (true, _) => quiet_since = Some((Instant::now(), commit)),
                (false, _) => quiet_since = None,
            }
            assert!(
                Instant::now() < deadline,
                "cluster FSM never went quiet (append={append} commit={commit} \
                 durable={durable} walked={walked} agreed={agreed})"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    /// Assert the slot still reads the pin [`Fixture::pin_at`] faked — call it
    /// right before `ServiceBuilder::start()`, so a pin wiped by a late
    /// cluster publish fails HERE, named, rather than as a downstream
    /// `VersionMismatch`. A no-op when this fixture faked no pin.
    fn assert_pin_held(&self) {
        if let Some((origin, from, to)) = self.faked_pin.get() {
            assert_eq!(
                self.cnc().service_slot(0).status.pin(),
                PinRead::Pinned { origin, from, to },
                "the faked pin was wiped before the attach: the node republishes \
                 the row view from committed state"
            );
        }
    }

    fn stop(self) {
        self.node.stop();
    }
}

/// `uc2ctl upgrade pin`, in process: stage the 20-byte `UpgradePin` record at
/// `<instance_dir>/upgrade.pending` and submit admin op 10 through the cnc
/// admin band (these test nodes run the filesystem admin policy, so there is
/// no auth line). The twin of `uc_diffreplay/tests/common::pin_row`.
///
/// [`Fixture::pin`] pokes the slot words instead, which is enough for a
/// SERVICE-side test; this exists for the one test whose subject is the
/// NODE's behaviour under a pin — the floor hold reads the COMMITTED view,
/// not the cnc words.
///
/// Only two answers are races and only those two are retried: status 2
/// (single-in-flight) and reason 54 `pin_no_set` (the set's position is
/// published a moment after the artifact lands) or 61 `pin_origin_not_agreed`
/// (the catalog agrees it a moment after that). Anything else fails here,
/// named.
fn pin_via_admin(dir: &Path, cnc: &CncPage, row: u8, from: u32, to: u32, origin: u64) {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut bytes = Vec::new();
    encode_upgrade_pin(
        &UpgradePin {
            row,
            from,
            to,
            origin,
        },
        &mut bytes,
    );
    let (id, ip, port) = uc_node::staged_digest(&bytes);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let pending = dir.join(uc_node::UPGRADE_PENDING_FILE);
        let tmp = dir.join(format!("{}.tmp", uc_node::UPGRADE_PENDING_FILE));
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp)
                .unwrap();
            f.write_all(&bytes).unwrap();
            f.sync_all().unwrap();
        }
        std::fs::rename(&tmp, &pending).unwrap();

        let seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0) + 1;
        cnc.write_admin_req(&AdminReq {
            seq,
            nonce: seq,
            op: ADMIN_OP_UPGRADE_PIN,
            id,
            ip,
            port,
        });
        let resp_deadline = Instant::now() + Duration::from_secs(15);
        let resp = loop {
            if let Some(r) = cnc.read_admin_resp(seq) {
                break r;
            }
            assert!(
                Instant::now() < resp_deadline,
                "admin response timed out for seq {seq}"
            );
            std::thread::yield_now();
        };
        let racy = resp.status == 2
            || resp.reason == uc_node::REASON_PIN_NO_SET
            || resp.reason == uc_node::REASON_PIN_ORIGIN_NOT_AGREED;
        if resp.status == 0 || !racy || Instant::now() >= deadline {
            assert_eq!(
                resp.status, 0,
                "upgrade pin refused: status={} reason={}",
                resp.status, resp.reason
            );
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// `uc2ctl settings apply`, in process: commit one replicated `Settings`
/// record (`retain_sets = 2`, every other key at its genesis default). It is a
/// cluster record that needs no attached service, which is what makes it a
/// deterministic stand-in for a late `SnapshotReport`: the leader accepts and
/// appends it, and it commits (and the agent republishes the row view)
/// asynchronously. Returns once the leader has ACCEPTED it, not once it is
/// applied.
fn append_a_settings_record(dir: &Path, cnc: &CncPage) {
    use std::io::Write as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    let mut settings = uc_protocol::v2::settings::Settings::genesis_default();
    settings.retain_sets = 2;
    let mut bytes = Vec::new();
    uc_protocol::v2::settings::encode_settings(&settings, &mut bytes);
    let (id, ip, port) = uc_node::staged_digest(&bytes);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let pending = dir.join(uc_node::SETTINGS_PENDING_FILE);
        let tmp = dir.join(format!("{}.tmp", uc_node::SETTINGS_PENDING_FILE));
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&tmp)
                .unwrap();
            f.write_all(&bytes).unwrap();
            f.sync_all().unwrap();
        }
        std::fs::rename(&tmp, &pending).unwrap();
        let seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0) + 1;
        cnc.write_admin_req(&AdminReq {
            seq,
            nonce: seq,
            op: uc_protocol::v2::cnc::ADMIN_OP_SETTINGS_APPLY,
            id,
            ip,
            port,
        });
        let resp_deadline = Instant::now() + Duration::from_secs(15);
        let resp = loop {
            if let Some(r) = cnc.read_admin_resp(seq) {
                break r;
            }
            assert!(
                Instant::now() < resp_deadline,
                "admin response timed out for seq {seq}"
            );
            std::thread::yield_now();
        };
        if resp.status == 0 {
            return;
        }
        // status 2 is single-in-flight: the only answer that is a race.
        assert!(
            resp.status == 2 && Instant::now() < deadline,
            "settings apply refused: status={} reason={}",
            resp.status,
            resp.reason
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Row 0's artifact for the instant at `p`.
fn artifact_path(dir: &Path, p: u64) -> std::path::PathBuf {
    dir.join("snapshots")
        .join("0")
        .join(format!("snap-{p}.ultsnap"))
}

/// #33 spec §7.2: a service still attached when a pin to another LINE
/// commits stops at exactly the pin record — its apply thread fail-stops
/// (`version_superseded`), so `Service::stop` would re-raise that panic.
/// Wait for the stop, then drop the handle (which joins without re-raising).
fn wait_stopped_at_the_pin(svc: Service<RegisterSm>) {
    wait_until("v1 stopped at the pin record (#33)", || !svc.is_alive());
    drop(svc);
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

    f.assert_pin_held();
    let err = ServiceBuilder::new(cfg(f.path(), f.app), RegisterSm::default())
        .start()
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

    f.assert_pin_held();
    let svc2 = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start()
        .unwrap();
    let cnc = f.cnc();
    wait_service_caught_up(&cnc);
    assert_eq!(
        query_v2(&svc2),
        Some(CAS_NEW),
        "the artifact at P was installed, then the tail recomputed under v2"
    );
    assert_eq!(
        svc2.pinned(),
        Some((f.p, V1, V2)),
        "this incarnation reports the pin it installed under"
    );
    svc2.stop();
    f.stop();
}

/// Purge is the production posture of spec §3 S4, and it is the posture that
/// puts the pinned attach on the reconstruction path: the ring has scrolled
/// past the origin, so the follower overruns immediately and
/// `uc_service::replay` runs — with the journal purged below the instant.
///
/// Two things have to hold for that to converge, and neither did before the
/// T4 review fix:
///
/// * the follower must resume at the ORIGIN, not at the artifact's internal
///   cursor: the tag is an exclusive frontier, so everything below it IS the
///   artifact;
/// * when the gap guard does fire, the artifact at the PINNED ORIGIN must be
///   installable by this binary — it was built by the pin's `from`, and the
///   unpinned same-version rule (plan B2 T3) would refuse it and fail-stop
///   the apply thread.
#[test]
fn a_pinned_attach_converges_on_a_purging_cluster() {
    let f = Fixture::build("pin-purge", Spec::purging());
    assert!(
        f.node.archive_first_base() > 0,
        "precondition: the journal is purged below P"
    );
    // The TRIGGER, asserted rather than assumed: the gap guard fires only
    // because the artifact's own internal cursor — where `install_snapshot`
    // leaves the state machine, i.e. the row's last applied MESSAGE below P —
    // sits BELOW the journal's first retained base. That is what `Spec
    // ::purging`'s `extra_instants` buy; without it the pinned attach would
    // converge through the ordinary tail replay and prove nothing about the
    // gap. The purge behind the floor is asynchronous and throttled, so this
    // is a bounded WAIT, not a spot read — on a loaded box the floor is at P
    // well before the segments below it are gone.
    let cursor = {
        let mut v1 = RegisterSm::default();
        let mut art = std::fs::File::open(f.artifact()).unwrap();
        verify_snapshot_envelope(&mut art, f.p, Some(V1)).unwrap();
        SnapshotStateMachine::install_snapshot(&mut v1, f.p, &mut art).unwrap();
        StateMachine::last_applied(&v1).unwrap_or(0)
    };
    wait_until(
        "the journal's first base passed the artifact's cursor (the gap)",
        || f.node.archive_first_base() > cursor,
    );
    f.pin(V1, V2);

    f.assert_pin_held();
    let svc2 = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start()
        .unwrap();
    let cnc = f.cnc();
    wait_service_caught_up(&cnc);
    assert_eq!(
        query_v2(&svc2),
        Some(CAS_NEW),
        "a pinned attach on a purging cluster converges on v1's history, \
         not the counterfactual"
    );
    assert_eq!(svc2.pinned(), Some((f.p, V1, V2)));
    svc2.stop();
    f.stop();
}

/// Regression for the CI flake of run 38038671733: `Fixture::pin` fakes the
/// pin on the cnc page, and the node's cluster agent rewrites every row's pin
/// words from COMMITTED state on each publish. A cluster record that commits
/// after the poke (there, a late `SnapshotReport`; here a `Settings` record
/// accepted just before it) wiped the pin, and the attach then read
/// "unpinned, running v1" and refused with `VersionMismatch`. The fixture now
/// waits for the cluster FSM to be quiet before it pokes; this test appends
/// a record IMMEDIATELY before `pin` and fails if that wait ever regresses.
#[test]
fn a_faked_pin_survives_a_cluster_record_committing_just_before_it() {
    let f = Fixture::new("pin-late-record");
    append_a_settings_record(f.path(), &f.cnc());
    f.pin(V1, V2);
    f.assert_pin_held();
    let svc2 = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start()
        .unwrap();
    wait_service_caught_up(&f.cnc());
    assert_eq!(svc2.pinned(), Some((f.p, V1, V2)));
    svc2.stop();
    f.stop();
}

/// The control that makes the assertion above evidence: the SAME swap with no
/// pin computes the §2.3 counterfactual instead.
///
/// #33 (spec §7.1): an unpinned attach of a binary off the row's running line
/// is now refused BY NAME — `RegisterSm`'s attach recorded 0.0.0, and
/// `DoublingRegisterSm` is 0.2.0 — so the unpinned swap can no longer run
/// through attach at all. The counterfactual it would have computed is
/// computed directly instead (v2's `apply` over the journal from genesis to
/// the same frontier, as [`a_durable_sm_above_the_origin_is_rewound_to_it`]
/// builds its precondition), so `Some(CAS_NEW)` above still stands against
/// `Some(COUNTERFACTUAL)` here.
#[test]
fn the_same_swap_without_a_pin_computes_the_counterfactual() {
    let f = Fixture::new("pin-none");
    let cnc = f.cnc();

    let err = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start()
        .err()
        .expect("an unpinned off-line swap is refused at attach (#33)");
    assert!(
        matches!(
            err,
            ServiceError::VersionMismatch { row: 0, running, mine, .. }
                if running == V1 && mine == V2
        ),
        "{err}"
    );
    assert_eq!(
        cnc.service_slot(0).status.load_acquire() & CNC_SVC_STATUS_ATTACHED,
        0,
        "nothing was written to the slot"
    );

    let end = {
        let c = cnc.counters();
        c.commit.load_acquire().min(c.durable.load_acquire())
    };
    let mut v2 = DoublingRegisterSm::default();
    replay_from_genesis(&mut v2, &f.path().join("journal"), end);
    assert_eq!(
        StateMachine::query(&v2, ()),
        Some(COUNTERFACTUAL),
        "unpinned, v2 replays [0, P) itself: Write(4) stores 8 and the CAS fails"
    );
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
    f.assert_pin_held();
    let svc2 = ServiceBuilder::new(cfg(f.path(), f.app), durable)
        .start()
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

// #67 retired `a_pinned_row_started_without_snapshots_is_refused`: it
// started a pinned row with the snapshot-less `start()`, which no longer
// exists — every `ServiceBuilder::start()` carries the install capability, so
// the SDK cannot produce `PinRequiresSnapshots`. The refusal is still
// `attach`'s backstop for an attach without the capability and is covered
// there, at unit level:
// `attach::tests::a_pinned_row_attached_without_the_install_capability_is_refused`.

/// The set at the origin was pruned or never fetched: named, with the path,
/// instead of falling back to some other artifact or to genesis.
#[test]
fn a_pinned_origin_with_no_artifact_is_refused() {
    let f = Fixture::new("pin-missing");
    std::fs::remove_file(f.artifact()).unwrap();
    f.pin(V1, V2);

    f.assert_pin_held();
    let err = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start()
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
///
/// #33 D3 (spec §7.3): that cross-check is by LINE, so "no artifact was built
/// by `from`" means "none was built on `from`'s major.minor". The artifact
/// here was built by `V1` (0.0.0); `from` is 1.0.0, a different line. (This
/// test used `from = 3` — 0.0.3, the same line as 0.0.0 since #33, which
/// [`a_pinned_artifact_built_by_a_patch_of_from_is_installed`] now covers.)
#[test]
fn a_pinned_artifact_built_by_the_wrong_version_is_refused() {
    let f = Fixture::new("pin-wrongfrom");
    let off_line = uc_protocol::identity::pack_version(1, 0, 0);
    assert!(!uc_protocol::identity::same_line(off_line, V1));
    f.pin(off_line, V2);

    f.assert_pin_held();
    let err = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start()
        .err()
        .expect("refused");
    assert!(
        matches!(
            err,
            ServiceError::MistaggedSnapshot {
                source: EnvelopeError::VersionMismatch {
                    built: V1,
                    expected
                },
                ..
            } if expected == off_line
        ),
        "{err}"
    );
    f.stop();
}

/// #33 D3 (spec §7.3): patch builds of one line share the artifact format, so
/// a pin whose `from` is another PATCH of the artifact's builder installs it —
/// origin artifacts on different nodes may come from different patch builds
/// of `from`'s line. `Some(CAS_NEW)` is the install-then-recompute signature
/// (module doc).
#[test]
fn a_pinned_artifact_built_by_a_patch_of_from_is_installed() {
    let f = Fixture::new("pin-patchfrom");
    let patch = V1 + 3; // 0.0.3: same major.minor as V1 (0.0.0), other patch
    assert!(patch != V1 && uc_protocol::identity::same_line(patch, V1));
    f.pin(patch, V2);

    f.assert_pin_held();
    let svc2 = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start()
        .expect("a same-line artifact installs");
    let cnc = f.cnc();
    wait_service_caught_up(&cnc);
    assert_eq!(
        query_v2(&svc2),
        Some(CAS_NEW),
        "the artifact at P was installed, then the tail recomputed under v2"
    );
    drop(svc2);
    f.stop();
}

/// #33 ruling R17 (spec D3, patch is free): a pin names a LINE, not a
/// build. A PATCH build of the pin's `to` attaches to the pinned row and
/// takes the pinned path — installs the origin, recomputes the tail — rather
/// than being refused, so a patch release of the pinned version can roll
/// node by node after the pin. A build on another line is still refused by
/// name.
#[test]
fn a_patch_build_of_the_pinned_to_installs_the_origin() {
    let f = Fixture::new("pin-patchto");
    let to_patch = V2 + 5; // 0.2.5: DoublingRegisterSm (0.2.0)'s line, other patch
    assert!(to_patch != V2 && uc_protocol::identity::same_line(to_patch, V2));
    f.pin(V1, to_patch);

    // Off-line (V1 = 0.0.0 against the pinned 0.2.x): refused by name.
    f.assert_pin_held();
    let err = ServiceBuilder::new(cfg(f.path(), f.app), RegisterSm::default())
        .start()
        .err()
        .expect("an off-line build is refused");
    assert!(
        matches!(
            err,
            ServiceError::PinnedVersionMismatch {
                row: 0,
                pinned,
                mine: V1,
                ..
            } if pinned == to_patch
        ),
        "{err}"
    );

    // Same line, other patch: admitted, and it installs the origin.
    let svc2 = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start()
        .expect("a patch build of the pinned `to` attaches");
    let cnc = f.cnc();
    wait_service_caught_up(&cnc);
    assert_eq!(
        query_v2(&svc2),
        Some(CAS_NEW),
        "the artifact at P was installed, then the tail recomputed under v2"
    );
    assert_eq!(svc2.pinned(), Some((f.p, V1, to_patch)));
    drop(svc2);
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
        .start()
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
        .start()
        .unwrap();
    let cnc = f.cnc();
    wait_service_caught_up(&cnc);
    assert_eq!(query_v2(&svc2), Some(CAS_NEW));
    svc2.stop();
    f.stop();
}

/// Spec §3 S4's other shape, and plan B2 T5's fix: between the pin and the
/// old binary's stop, `from` KEEPS APPLYING, so a cadence instant can leave a
/// LATER artifact on disk — one built by `from`, not by the pinned `to`. The
/// gap guard must then prefer the PINNED ORIGIN over that newer artifact.
/// Picking the newest (what it did before T5) walks straight into the
/// unpinned same-version rule (plan B2 T3), and the apply thread of a service
/// that attached successfully fail-stops with
/// `MistaggedSnapshot { VersionMismatch { built: V1, expected: V2 } }`.
///
/// **Why the later artifact is published here rather than by a second
/// `command_instant`.** A real later instant completes a real SET, which
/// advances this node's snapshot floor past the origin — and the journal
/// purge that follows the floor takes `first` past the origin with it. That
/// leaves the origin unable to cover the gap at all, which is a DIFFERENT
/// hazard (S4 with the purge floor already above the origin) and not the one
/// this fix is about. The state under test — a newer artifact on disk while
/// the origin is still at or above the purge floor — is what the
/// floor-persist throttle and a not-yet-complete set at P2 produce on a live
/// node, and it is reached here deterministically instead: the artifact is a
/// real v1 image published through the row's OWN [`SnapshotStore`], with the
/// framework's own envelope and `from`'s version stamp. Nothing is
/// hand-written.
#[test]
fn a_pinned_attach_prefers_the_origin_over_a_later_artifact() {
    let f = Fixture::build("pin-later-art", Spec::purging());
    assert!(
        f.node.archive_first_base() > 0,
        "precondition: the journal is purged below P"
    );
    let cnc = f.cnc();
    let target = {
        let c = cnc.counters();
        c.commit.load_acquire().min(c.durable.load_acquire())
    };

    // v1's later cadence instant: a genuine v1 image (the artifact at the
    // origin, read back through the framework's envelope check), republished
    // at a position in the tail above the origin.
    let p2 = f.p + HEADER_LEN as u64;
    assert!(
        p2 < target,
        "P2={p2} must sit inside the tail (target={target})"
    );
    let store = SnapshotStore::open(f.path(), 0).unwrap();
    let mut v1 = RegisterSm::default();
    let mut art = std::fs::File::open(f.artifact()).unwrap();
    verify_snapshot_envelope(&mut art, f.p, Some(V1)).unwrap();
    SnapshotStateMachine::install_snapshot(&mut v1, f.p, &mut art).unwrap();
    let (handle, _) = SnapshotStateMachine::freeze(&v1).unwrap();
    store
        .publish(p2, V1, |w| {
            <RegisterSm as SnapshotStateMachine>::stream_snapshot(handle, w)
        })
        .unwrap();
    assert_eq!(
        store.newest(target).unwrap().map(|(pos, _)| pos),
        Some(p2),
        "precondition: the NEWEST covering artifact is the later one, not the origin"
    );

    f.pin(V1, V2);
    f.assert_pin_held();
    let svc2 = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start()
        .unwrap();
    wait_service_caught_up(&cnc);
    assert_eq!(
        query_v2(&svc2),
        Some(CAS_NEW),
        "the gap guard must cover the gap with the PINNED origin, not with the \
         newer artifact `from` left behind"
    );
    assert_eq!(svc2.pinned(), Some((f.p, V1, V2)));
    assert!(
        store.path_for(p2).is_file(),
        "the later artifact is left alone, not consumed or deleted"
    );
    svc2.stop();
    f.stop();
}

/// Plan B2 T5 (fix round): purge on, a real `uc2ctl upgrade pin`, and then a
/// real cadence instant at **P2 > origin**.
///
/// Before #33 this instant was taken while v1 was STILL APPLYING — the window
/// spec §3 S4 described, in which `from` kept applying between the pin and
/// the swap and built `snap-P2` itself, so the node's floor hold
/// (`Consensus::hold_floor_for_pins`) and the service's preference for the
/// pinned origin over `snap-P2` were both load-bearing here. #33 (spec §7.2)
/// CLOSES that window: an attached v1 stops at exactly the pin record, so
/// `from` builds nothing above it. What this test now pins:
///
/// * v1 stops at the pin (it does not keep applying);
/// * the instant at P2 therefore finds no row-0 builder — no `snap-P2`, no
///   complete set at P2, and the journal the pinned attach replays from
///   stays (`archive_first_base <= origin`);
/// * the pinned attach converges (`Some(CAS_NEW)`).
///
/// The origin-over-a-later-`from`-artifact preference stays covered by
/// [`a_pinned_attach_prefers_the_origin_over_a_later_artifact`], which
/// constructs that artifact by hand.
///
/// The pin goes in through admin op 10 rather than [`Fixture::pin`] because
/// only a COMMITTED pin is a version record the apply loop acts on, and the
/// node's hold reads the committed view.
#[test]
fn a_pinned_attach_survives_a_cadence_instant_after_the_pin() {
    let (f, svc1) = Fixture::build_with_v1("pin-cadence", Spec::purging());
    let origin = f.p;
    let cnc = f.cnc();
    assert!(
        f.node.archive_first_base() > 0,
        "precondition: the journal is purged below the origin"
    );

    pin_via_admin(f.path(), &cnc, 0, V1, V2, origin);
    wait_until("the pin reached the row's slot words", || {
        cnc.service_slot(0).status.pin()
            == uc_log::cnc::PinRead::Pinned {
                origin,
                from: V1,
                to: V2,
            }
    });
    wait_stopped_at_the_pin(svc1);

    // The cadence instant, with v1 stopped at the pin: nothing on row 0 can
    // build `snap-P2`, so the set at P2 never completes on this node.
    let p2 = command_instant(&f.node);
    assert!(p2 > origin, "P2={p2} must sit above the origin={origin}");
    // Give the floor tick every chance to move (it is throttled to 100 ms and
    // the purge behind it is asynchronous).
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        !artifact_path(f.path(), p2).is_file(),
        "a stopped `from` must not build snap-P2 above its pin"
    );
    assert!(
        f.node.snapshot_set_position() < p2,
        "no complete set at P2 without a row-0 artifact"
    );
    assert!(
        f.node.archive_first_base() <= origin,
        "the journal the pinned attach replays from must stay: \
         archive_first_base={} origin={origin} P2={p2}",
        f.node.archive_first_base()
    );

    let svc2 = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start()
        .unwrap();
    wait_service_caught_up(&cnc);
    assert_eq!(
        query_v2(&svc2),
        Some(CAS_NEW),
        "a pinned attach converges after an instant above the pin"
    );
    assert_eq!(svc2.pinned(), Some((origin, V1, V2)));
    svc2.stop();
    f.stop();
}

/// Plan B2 (final review I4): the pinned arm replaces the published `applied`
/// with the ORIGIN — a number off the cnc page, not the state machine's — so
/// it gets the SAME drift bound the unpinned arm has always had.
///
/// The state under test is reachable: a store-only `uc2ctl snapshot fetch`
/// (admin op 9) can leave an artifact ABOVE this node's durable frontier, and
/// a pin naming it would otherwise publish `applied` above `durable`, which
/// the node's floor hold reads. The artifact here is a genuine v1 image
/// republished at that position through the row's own [`SnapshotStore`] —
/// nothing hand-written — so the install itself succeeds and the refusal is
/// the drift check, not an envelope check.
#[test]
fn a_pinned_origin_above_the_durable_frontier_is_a_drift_refusal() {
    let f = Fixture::new("pin-drift");
    let cnc = f.cnc();
    let applied_before = cnc.service_slot(0).applied.load_acquire();
    let frontier = cnc.counters().durable.load_acquire();
    let origin = frontier + BUFFER_BYTES as u64;

    let store = SnapshotStore::open(f.path(), 0).unwrap();
    let mut v1 = RegisterSm::default();
    let mut art = std::fs::File::open(f.artifact()).unwrap();
    verify_snapshot_envelope(&mut art, f.p, Some(V1)).unwrap();
    SnapshotStateMachine::install_snapshot(&mut v1, f.p, &mut art).unwrap();
    let (handle, _) = SnapshotStateMachine::freeze(&v1).unwrap();
    store
        .publish(origin, V1, |w| {
            <RegisterSm as SnapshotStateMachine>::stream_snapshot(handle, w)
        })
        .unwrap();

    f.pin_at(origin, V1, V2);
    f.assert_pin_held();
    let err = ServiceBuilder::new(cfg(f.path(), f.app), DoublingRegisterSm::default())
        .start()
        .err()
        .expect("refused");
    assert!(
        matches!(err, ServiceError::Drift { service, journal }
                 if service == origin && journal < origin),
        "{err}"
    );
    assert_eq!(
        cnc.service_slot(0).applied.load_acquire(),
        applied_before,
        "a refused attach does not republish `applied`"
    );
    f.stop();
}

/// Plan B2 (final review C1): a service that attaches in the node's BOOT
/// WINDOW must not silently take the unpinned path.
///
/// `uc_service::attach` uses `services_declared != 0` as "this page is
/// ready"; the pin words are republished by `ClusterAgent::new`'s
/// `publish_view`, which the node constructs hundreds of lines further into
/// `Node::start` (log-buffer open, journal recovery, artifact recovery). With
/// the declared set stored first, an attach landing in that window reads
/// `PinRead::NoPin` on a page whose row IS pinned and replays from genesis
/// under the new binary — the §2.3 counterfactual, with no refusal and no log
/// line. The node therefore stores the declared set LAST.
///
/// The test watches the page from a second thread and samples the pin words
/// at the exact instant the declared set becomes visible. It re-opens the
/// file every turn and ignores every page still carrying the PREVIOUS boot's
/// `instance_id`, so it can only ever report the restarted node's own page.
///
/// It lives here rather than in `uc_node` because the subject is
/// attach-visible state and because a committed pin needs a complete snapshot
/// set at the origin — i.e. this file's fixture (a node, a v1 service, an
/// instant and admin op 10). Scope: the pin must be in the cluster artifact
/// the restart recovers from. A pin committed ABOVE that artifact is
/// [`a_pin_above_the_recovered_artifact_is_published_before_the_declared_set`
/// ]'s subject — plan B3 T5 moved the declared-set store into the consensus
/// pass to cover it, and this test is what keeps the boot ordering it still
/// relies on honest.
#[test]
fn a_restarted_node_publishes_the_pin_before_the_declared_set() {
    let (f, svc1) = Fixture::build_with_v1("pin-bootorder", Spec::small());
    let Fixture {
        dir, app, node, p, ..
    } = f;
    let origin = p;
    let cnc = open_cnc(dir.path(), app);

    // A REAL pin: the boot-time republish reads the cluster FSM, not the slot
    // words, so `Fixture::pin` would prove nothing here.
    pin_via_admin(dir.path(), &cnc, 0, V1, V2, origin);
    wait_until("the pin reached the row's slot words", || {
        cnc.service_slot(0).status.pin()
            == uc_log::cnc::PinRead::Pinned {
                origin,
                from: V1,
                to: V2,
            }
    });
    // …and it must be INSIDE the artifact the restart recovers from: one more
    // instant, above the pin's own frame, freezes the cluster FSM with it.
    let p2 = command_instant(&node);
    let cluster_art = dir
        .path()
        .join("snapshots")
        .join("cluster")
        .join(format!("snap-{p2}.ultcluster"));
    wait_until("the cluster artifact above the pin", || {
        cluster_art.is_file()
    });

    let old_instance = cnc.try_meta().expect("meta").instance_id;
    drop(cnc);
    wait_stopped_at_the_pin(svc1);
    node.stop();

    // The watcher: spin until a page with a NEW `instance_id` publishes a
    // nonzero declared set, then sample that same page's pin words. Bounded,
    // and it reports rather than panics off-thread.
    let cnc_path = dir.path().join("cnc2.dat");
    let watcher = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(page) = CncPage::open_file(&cnc_path, app) {
                let fresh = page
                    .try_meta()
                    .is_some_and(|m| m.instance_id != old_instance);
                if fresh && page.services_declared() != 0 {
                    return Some(page.service_slot(0).status.pin());
                }
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::yield_now();
        }
    });

    let node2 = start_node(dir.path(), app, PurgePolicy::Disabled, SEGMENT_BYTES);
    let sampled = watcher.join().expect("watcher panicked");
    let expected = uc_log::cnc::PinRead::Pinned {
        origin,
        from: V1,
        to: V2,
    };
    assert_eq!(
        sampled,
        Some(expected),
        "the pin words must already be published when the declared set turns \
         on — an attach passing the booting gate any earlier reads NoPin"
    );
    // And the same state is what an attach sees once `Node::start` returns.
    let cnc2 = open_cnc(dir.path(), app);
    assert_eq!(cnc2.service_slot(0).status.pin(), expected);
    node2.stop();
}

/// Plan B3 T5: the half of the boot race B2's C1 reorder explicitly did NOT
/// cover — a pin committed **above** this node's newest cluster artifact.
///
/// The test above pins and then takes one more instant, so the pin is inside
/// the artifact `ClusterAgent::recover` loads and the pin words are on the
/// page before `Node::start` returns. Here there is no instant after the pin:
/// the restart recovers a cluster FSM that does not hold it, and the pin only
/// lands once the `uc2-cluster` agent has replayed the log above the artifact
/// — some passes AFTER `Node::start` returned. A service attaching in that
/// window used to read `PinRead::NoPin` and replay `[0, P)` from genesis
/// under the new binary: the §2.3 counterfactual, silently.
///
/// T5 closes it by publishing the declared set — the word every attacher
/// reads as "this node is ready" — from the consensus pass rather than from
/// `Node::start`, on the first pass where the node knows its leader AND its
/// cluster FSM has consumed the log up to commit. Two things are asserted:
///
/// * the ORDER, by the same watcher the test above uses: at the instant the
///   declared set turns on, the row's pin words already hold the pin;
/// * the CONSEQUENCE: a v2 service started the moment `Node::start` returns
///   waits the gap out (`ServiceConfig::boot_wait`, `NodeBooting` internally)
///   and then takes the pinned path — `Some(CAS_NEW)`, not the
///   counterfactual, with the pin reported on the handle.
#[test]
fn a_pin_above_the_recovered_artifact_is_published_before_the_declared_set() {
    let (f, svc1) = Fixture::build_with_v1("pin-abovecluster", Spec::small());
    let Fixture {
        dir, app, node, p, ..
    } = f;
    let origin = p;
    let cnc = open_cnc(dir.path(), app);

    // A REAL pin, and NO instant after it: the newest cluster artifact is the
    // one at `origin`, which predates the pin's own frame.
    pin_via_admin(dir.path(), &cnc, 0, V1, V2, origin);
    wait_until("the pin reached the row's slot words", || {
        cnc.service_slot(0).status.pin()
            == uc_log::cnc::PinRead::Pinned {
                origin,
                from: V1,
                to: V2,
            }
    });
    let cluster_art = dir
        .path()
        .join("snapshots")
        .join("cluster")
        .join(format!("snap-{origin}.ultcluster"));
    assert!(
        cluster_art.is_file(),
        "precondition: the artifact the restart recovers from is the one at the instant, taken BEFORE the pin was committed"
    );

    let old_instance = cnc.try_meta().expect("meta").instance_id;
    drop(cnc);
    wait_stopped_at_the_pin(svc1);
    node.stop();

    // The watcher: spin until a page with a NEW `instance_id` publishes a
    // nonzero declared set, then sample that same page's pin words.
    let cnc_path = dir.path().join("cnc2.dat");
    let watcher = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(page) = CncPage::open_file(&cnc_path, app) {
                let fresh = page
                    .try_meta()
                    .is_some_and(|m| m.instance_id != old_instance);
                if fresh && page.services_declared() != 0 {
                    return Some(page.service_slot(0).status.pin());
                }
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::yield_now();
        }
    });

    let node2 = start_node(dir.path(), app, PurgePolicy::Disabled, SEGMENT_BYTES);
    // IMMEDIATELY, the way a co-restarting service does: the declared set is
    // not on the page yet, so this attach waits it out.
    let svc2 = ServiceBuilder::new(cfg(dir.path(), app), DoublingRegisterSm::default())
        .start()
        .expect("the boot wait outlasts the node's join");

    let sampled = watcher.join().expect("watcher panicked");
    let expected = uc_log::cnc::PinRead::Pinned {
        origin,
        from: V1,
        to: V2,
    };
    assert_eq!(
        sampled,
        Some(expected),
        "a pin committed ABOVE the recovered artifact must also be published before the declared set — the gate is the cluster FSM reaching commit, not the boot ordering"
    );

    let cnc2 = open_cnc(dir.path(), app);
    wait_service_caught_up(&cnc2);
    assert_eq!(
        svc2.pinned(),
        Some((origin, V1, V2)),
        "the attach acted on the pin"
    );
    assert_eq!(
        query_v2(&svc2),
        Some(CAS_NEW),
        "v1's artifact at the origin, then the tail under v2 — not the \
         counterfactual v2 would compute by replaying from genesis"
    );
    svc2.stop();
    node2.stop();
}

// ------------------------------------------------------- pin completion

/// Every `install_snapshot` position [`CountingV2`] has seen, in order. Only
/// [`a_completed_pin_restarts_from_the_completion_set_with_one_install`]
/// builds a `CountingV2`, so no other test writes here.
static COUNTED_INSTALLS: std::sync::Mutex<Vec<u64>> = std::sync::Mutex::new(Vec::new());

/// [`DoublingRegisterSm`] (same `NAME`, same `VERSION` — the pin's `to`)
/// that records every `install_snapshot` position in [`COUNTED_INSTALLS`],
/// so a test can tell one install from two.
#[derive(Default)]
struct CountingV2(DoublingRegisterSm);

impl StateMachine for CountingV2 {
    const NAME: &'static str = <DoublingRegisterSm as StateMachine>::NAME;
    const VERSION: u32 = V2;
    type Command = RegCmd;
    type Response = <DoublingRegisterSm as StateMachine>::Response;
    type Query = ();
    type QueryResponse = Option<u64>;

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: RegCmd) -> Self::Response {
        StateMachine::apply(&mut self.0, ctx, cmd)
    }
    fn query(&self, q: ()) -> Option<u64> {
        StateMachine::query(&self.0, q)
    }
    fn last_applied(&self) -> Option<u64> {
        StateMachine::last_applied(&self.0)
    }
}

impl SnapshotStateMachine for CountingV2 {
    type SnapshotHandle = <DoublingRegisterSm as SnapshotStateMachine>::SnapshotHandle;

    fn freeze(&self) -> Result<(Self::SnapshotHandle, u64), uc_service::SnapshotError> {
        self.0.freeze()
    }
    fn stream_snapshot(
        h: Self::SnapshotHandle,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc_service::SnapshotError> {
        DoublingRegisterSm::stream_snapshot(h, dst)
    }
    fn install_snapshot(
        &mut self,
        p: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc_service::SnapshotError> {
        COUNTED_INSTALLS.lock().unwrap().push(p);
        self.0.install_snapshot(p, src)
    }
}

/// Pin completion (snapshot-lifecycle errata, "pin completion (as built)",
/// rulings C1–C3): purge on, a real pin, the pinned version attached and
/// caught up, then an instant ABOVE the pin record that agrees. That set
/// completes the pin, so the floor and the purge behind it move past the
/// origin. A restart of the upgraded service must then start from the
/// completion set — exactly ONE install, at the set, with the correct state.
///
/// Before the fix the restart re-ran the pinned install at the origin
/// unconditionally; with the journal above the origin purged, the replay's
/// gap guard then installed the newer set as well — two installs, origin
/// first.
#[test]
fn a_completed_pin_restarts_from_the_completion_set_with_one_install() {
    let (f, svc1) = Fixture::build_with_v1("pin-complete", Spec::purging());
    let origin = f.p;
    let cnc = f.cnc();
    pin_via_admin(f.path(), &cnc, 0, V1, V2, origin);
    wait_until("the pin reached the row's slot words", || {
        cnc.service_slot(0).status.pin()
            == uc_log::cnc::PinRead::Pinned {
                origin,
                from: V1,
                to: V2,
            }
    });
    wait_stopped_at_the_pin(svc1);
    let record_pos = match cnc.service_slot(0).status.row_view() {
        uc_log::cnc::RowRead::View { record_pos, .. } => record_pos,
        uc_log::cnc::RowRead::Contended => panic!("row view contended"),
    };
    assert!(record_pos > origin, "the pin record sits above its origin");

    let svc2 = ServiceBuilder::new(cfg(f.path(), f.app), CountingV2::default())
        .start()
        .unwrap();
    wait_service_caught_up(&cnc);
    assert_eq!(svc2.query(()), Some(CAS_NEW), "the pinned attach converged");

    // The completion instant: above the pin record, built by `to`.
    let deadline = Instant::now() + Duration::from_secs(60);
    let p2 = loop {
        let p = command_instant(&f.node);
        assert!(p > record_pos, "P2={p} above the pin record {record_pos}");
        if wait_for(Duration::from_millis(500), || {
            artifact_path(f.path(), p).is_file()
        }) {
            break p;
        }
        assert!(Instant::now() < deadline, "no instant above the pin built");
    };
    wait_until("the set at P2 agreed", || {
        f.node
            .cluster_view()
            .snapshot_inner()
            .catalog
            .iter()
            .any(|e| e.position == p2 && e.is_agreed())
    });
    wait_until("the purge passed the origin", || {
        f.node.archive_first_base() > origin
    });

    svc2.stop();
    COUNTED_INSTALLS.lock().unwrap().clear();
    let svc3 = ServiceBuilder::new(cfg(f.path(), f.app), CountingV2::default())
        .start()
        .unwrap();
    wait_service_caught_up(&cnc);
    assert_eq!(
        svc3.query(()),
        Some(CAS_NEW),
        "the restart reproduces the upgraded row's state"
    );
    assert_eq!(
        *COUNTED_INSTALLS.lock().unwrap(),
        vec![p2],
        "a completed pin restarts from the completion set, once \
         (origin={origin}, record_pos={record_pos}, P2={p2})"
    );
    // Ruling C3: the completed pin is released — the origin is an ordinary
    // set again, and retention prunes it.
    wait_until("retention pruned the completed pin's origin", || {
        !artifact_path(f.path(), origin).is_file()
    });
    svc3.stop();
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
