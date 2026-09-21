// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The service attach sequence (spec §7, v1 task14 discipline). ORDER IS
//! LOAD-BEARING — see the numbered steps.

use std::sync::Arc;
use std::sync::Mutex;

use uc_log::buffer::LogBuffer;
use uc_log::cnc::{CncPage, PinRead, pack_service_status, unpack_service_status};
use uc_log::reader::LogFollower;
use uc_protocol::ring::{BroadcastRing, SpscRing};
use uc_protocol::v2::cnc::CNC_SVC_STATUS_SNAPSHOT_CAPABLE;

use crate::apply::{ApplyState, InstallFn};
use crate::config::{ServiceConfig, ServiceError};
use crate::egress::Egress;
use crate::snapshots::SnapshotStore;
use crate::traits::RawStateMachine;

/// The pieces the builder needs after a successful attach: the apply agent's
/// state (moved into its thread) plus the handles the `Service` keeps.
pub(crate) struct Attached<S: RawStateMachine> {
    pub(crate) apply_state: ApplyState<S>,
    /// The log buffer, held separately (not just inside `apply_state.follower`)
    /// so the builder can hand the output agent (Task 12) its OWN independent
    /// [`LogFollower`] over the same buffer — apply and output are two distinct
    /// readers with two distinct cursors.
    pub(crate) buffer: Arc<LogBuffer>,
    pub(crate) cnc: Arc<CncPage>,
    pub(crate) instance_id: u128,
    /// This incarnation's service epoch (the post-bump value).
    pub(crate) epoch: u64,
    /// Shared poison flag (see [`ApplyState::poisoned`]) — the `Service`
    /// handle keeps a clone so `is_alive` can report a poisoned incarnation.
    pub(crate) poisoned: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// FSM identity: the row this attach landed on, found by `S::IDENTITY.name`.
    pub(crate) service_id: u8,
    /// M14a: `service.<row>.lock`, held for the service's life (dropped last,
    /// released by the OS on any exit) — enforces one process per row.
    pub(crate) _lock: std::fs::File,
    /// Plan B2 T4: the install capability the caller handed in, given back
    /// untouched so `start_with_snapshots` can reuse the SAME closure for the
    /// apply thread's [`crate::apply::SnapshotRestore`] — attach borrows it
    /// for the pinned install and owns none of it.
    pub(crate) install: Option<InstallFn<S>>,
    /// Plan B2 T4: the pin this attach acted on, `(origin, from, to)` —
    /// `Some` ONLY when the artifact at `origin` was actually installed here.
    /// A row with no pin, or an attach that refused, yields `None`.
    pub(crate) pin: Option<(u64, u32, u32)>,
}

/// M14a: the one path every service-side slot access takes.
pub(crate) fn slot(cnc: &CncPage, id: u8) -> &uc_log::cnc::ServiceSlot {
    cnc.service_slot(id as usize)
}

/// M14a Task 7: the lag mode this incarnation runs under, computed from the
/// page's RAW `services_declared` (NOT the effective/folded declared-set
/// mask `attach()` uses for the per-id gate and `ApplyState.declared`) — see
/// the call site's comment for why the fold must not happen here.
pub(crate) fn lag_mode_for(cnc: &CncPage) -> crate::lag::LagMode {
    crate::lag::mode_from_page(cnc.services_declared(), cnc.fsm_lag_bytes())
}

/// Plan B3 T5: is this page a node that has NOT yet joined its cluster?
///
/// Names on line 7 with no declared set. `create_file` publishes the header
/// (names included) at the very start of `Node::start`; the declared set is
/// published by the consensus pass, on the first pass where the node knows
/// its leader and its cluster FSM has consumed the log up to commit
/// (`uc_node::Consensus::maybe_publish_declared`). Between the two, every
/// word an attacher depends on — the lag policy, and the row's upgrade PIN
/// above all — may still be missing or stale.
///
/// No configured node publishes this pair in steady state and no harness page
/// has names (`ServicesConfig::none_for_tests` declares nothing and names
/// nothing), so it is unambiguous.
pub(crate) fn node_booting(
    declared: u64,
    names: &[Option<uc_protocol::identity::FsmName>],
) -> bool {
    declared == 0 && names.iter().any(Option::is_some)
}

/// Plan B3 T5: wait out [`ServiceConfig::boot_wait`] for a node that is still
/// joining its cluster, polling every [`BOOT_POLL`].
///
/// Called by [`ServiceBuilder::start`](crate::ServiceBuilder::start) and
/// [`start_with_snapshots`](crate::ServiceBuilder::start_with_snapshots)
/// BEFORE [`attach`], because `attach` takes the state machine by value and
/// so cannot be retried. The refusal it returns on timeout is `attach`'s own,
/// by name.
///
/// The page is re-opened every turn rather than held: a node restarting
/// underneath this wait rewrites `cnc2.dat`, and a mapping taken before that
/// says nothing about the node that is now booting.
pub(crate) fn wait_out_node_boot(cfg: &ServiceConfig) -> Result<(), ServiceError> {
    if cfg.boot_wait.is_zero() {
        return Ok(());
    }
    let path = cfg.instance_dir.join("cnc2.dat");
    let deadline = std::time::Instant::now() + cfg.boot_wait;
    loop {
        // A torn page (a node rewriting it in place) reads as booting rather
        // than as an error: `try_meta` is the same belt `attach` uses.
        let booting = match CncPage::open_file(&path, &cfg.app_id) {
            Ok(cnc) => {
                cnc.try_meta().is_none()
                    || node_booting(cnc.services_declared(), &cnc.service_names())
            }
            // Anything else — no page, wrong app_id, a bad header — is a real
            // refusal `attach` will raise properly a moment from now, with
            // the same error. Not something to spin on.
            Err(_) => return Ok(()),
        };
        if !booting {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            return Err(ServiceError::NodeBooting);
        }
        std::thread::sleep(BOOT_POLL);
    }
}

/// How often [`wait_out_node_boot`] looks at the page. Short enough that the
/// common case (a service and its node started together) costs one or two
/// turns, long enough that a full `boot_wait` is 500 map-and-read turns, not
/// a spin.
const BOOT_POLL: std::time::Duration = std::time::Duration::from_millis(20);

/// Run the 6-step attach. Steps 1–5 here; step 6 (spawn the threads) is the
/// builder's job, after this returns.
///
/// `install` is `Some` only from
/// [`ServiceBuilder::start_with_snapshots`](crate::ServiceBuilder::start_with_snapshots)
/// — the one path that has `S: SnapshotStateMachine` in scope — and is what
/// makes this row snapshot-CAPABLE: the capability bit rides the SAME status
/// store as the attached bit (coordinated-snapshot spec §5.2). Folding it in
/// there rather than OR-ing it afterwards leaves no window in which the node
/// can see this row attached-but-not-capable and refuse an instant
/// (`48 snapshot_unsupported`) on a row that is about to be capable.
///
/// Plan B2 T4 (spec §3 S4 steps 4–5) adds the row's upgrade PIN to the
/// sequence, between the lock and step 4. Order is load-bearing:
///
/// 1. the row is found by name (it names the lock file and the slot),
/// 2. `service.<row>.lock` is taken — so two racing attaches cannot both
///    pass the pin read and both install into the same row,
/// 3. the pin is read through the seqlock reader and every refusal is
///    decided,
/// 4. the pinned install runs, rewinding the state machine to the origin,
///
/// and only then does step 4 below publish `applied`. Nothing is written to
/// the slot before the pin decision, so a refused attach leaves the row
/// exactly as it found it.
pub(crate) fn attach<S: RawStateMachine>(
    cfg: &ServiceConfig,
    sm: S,
    install: Option<InstallFn<S>>,
) -> Result<Attached<S>, ServiceError> {
    let dir = &cfg.instance_dir;
    let snapshot_capable = install.is_some();

    // 1. Open + validate the cnc page (magic/crc/version/app_id). Capture the
    //    node's per-boot instance_id (a fresh id invalidates a stale attach).
    let cnc = CncPage::open_file(&dir.join("cnc2.dat"), &cfg.app_id)?;
    // A restarting node rewrites this page in place, so it can tear between
    // `open_file`'s validation and this decode — refuse, never panic.
    let meta = cnc.try_meta().ok_or(uc_log::cnc::CncError::BadHeader)?;
    let instance_id = meta.instance_id;

    // 1b. Find our row BY NAME (spec §4.3). A harness page (`none_for_tests`:
    // `services_declared == 0` and no names declared) rings row 0 for
    // whoever attaches — the pre-M14c multi-service-oblivious contract.
    let names = cnc.service_names();
    let raw_declared = cnc.services_declared();
    let any_named = names.iter().any(Option::is_some);
    // Names with no declared set is a node that has not yet JOINED its
    // cluster (plan B3 T5, [`node_booting`]) — the declared set is published
    // by the consensus pass, not at boot. Refuse rather than fall into the
    // harness arm below, which would fix `LagMode::Off` for this attachment's
    // life. The node stores `fsm_lag_bytes` BEFORE any agent runs, so a
    // nonzero declared set also proves the lag policy is already published —
    // and, since B3, that every committed `UpgradePin` has been applied here
    // and republished onto this row's pin words.
    //
    // `ServiceBuilder::start` has already waited `cfg.boot_wait` out
    // ([`wait_out_node_boot`]), so reaching this line means the node is
    // genuinely not ready — or the caller asked for no wait at all.
    if node_booting(raw_declared, &names) {
        return Err(ServiceError::NodeBooting);
    }
    let harness = raw_declared == 0 && !any_named;
    let row: u8 = if harness {
        0
    } else {
        cnc.row_of(&S::IDENTITY.name).ok_or_else(|| {
            let declared: Vec<String> = names
                .iter()
                .flatten()
                .map(|n| n.as_str().to_string())
                .collect();
            let name = S::IDENTITY.name.as_str().to_string();
            if declared.is_empty() {
                ServiceError::UnknownFsmNoNames { name }
            } else {
                ServiceError::UnknownFsm { name, declared }
            }
        })?
    };
    // M14a: the declared-set gate. `0` on the page is a harness node
    // (`ServicesConfig::none_for_tests`), which rings FSM 0 only.
    let declared = match cnc.services_declared() {
        0 => 1,
        d => d,
    };
    // M14a Task 7: the lag mode this incarnation runs under, read once at
    // attach (the page's `services_declared`/`fsm_lag_bytes` are boot-once —
    // see the cnc layout doc). `lag_mode_for` deliberately reads the RAW page
    // value, NOT the folded `declared` mask above: `mode_from_page`'s
    // `(0, _) => Off` arm is what recognizes a harness node
    // (`ServicesConfig::none_for_tests`, `services_declared == 0` on the
    // page) — folding `0` to `1` first would make an undeclared page
    // indistinguishable from a genuine one-FSM cluster and route it through
    // `Bounded`/`Lockstep` instead of `Off`. `ApplyState.declared` (below, for
    // `lag::floor`) still gets the FOLDED mask, since `floor` needs a real
    // bit to range over either way.
    let lag_mode = lag_mode_for(&cnc);
    // 1c. M14a: one process per id. Exclusive flock, held for the service's
    // life (the OS releases it on any exit), mirroring the node's
    // `instance.lock`.
    let lock_path = dir.join(format!("service.{}.lock", row));
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;
    fs2::FileExt::try_lock_exclusive(&lock).map_err(|_| ServiceError::AlreadyAttached {
        name: S::IDENTITY.name.as_str().to_string(),
        row,
    })?;

    // 1d. Plan B2 (spec §3 S4 steps 4–5): the row's PIN, read through the
    //     seqlock reader ONLY, and decided BEFORE any slot word is written.
    //     Under `service.<row>.lock` (just taken above), so the decision and
    //     the install that follows it are serialised against any other
    //     attach to this row.
    let s = slot(&cnc, row);
    let pin = match s.status.pin() {
        PinRead::NoPin => None,
        // NOT "no pin": a half-published triple that a reader silently read
        // as unpinned would skip an install the cluster requires. Transient,
        // so the refusal says to retry.
        PinRead::Contended => return Err(ServiceError::PinUnreadable { row }),
        PinRead::Pinned { origin, from, to } => {
            if to != S::VERSION {
                return Err(ServiceError::PinnedVersionMismatch {
                    name: S::IDENTITY.name.as_str().to_string(),
                    row,
                    origin,
                    pinned: to,
                    mine: S::VERSION,
                });
            }
            Some((origin, from, to))
        }
    };
    // UNCONDITIONAL install (step 4): the artifact at the origin, built by
    // the pin's `from`, replaces whatever state this state machine holds — a
    // DURABLE state machine already above the origin is rewound to it and
    // recomputes the tail under THIS version, exactly as its fresh peers do.
    // There is no "already caught up, skip it" arm on purpose: the prefix
    // below the origin was computed by `from`'s `apply`, and this binary's
    // may mean something different by the same recorded command (spec §2.3).
    let mut sm = sm;
    if let Some((origin, from, to)) = pin {
        let Some(install_fn) = install.as_ref() else {
            return Err(ServiceError::PinRequiresSnapshots {
                name: S::IDENTITY.name.as_str().to_string(),
                row,
                origin,
            });
        };
        let store = SnapshotStore::open(dir, row)?;
        let path = store.path_for(origin);
        let mut file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(ServiceError::PinnedArtifactMissing {
                    row,
                    origin,
                    path: path.display().to_string(),
                });
            }
            Err(e) => return Err(e.into()),
        };
        // The envelope is cross-checked against the PIN's `from`, not against
        // `S::VERSION` — this is the sanctioned crossing of a version
        // boundary, and the artifact is required to be the one `from` built
        // (the unpinned path in `replay.rs` requires `S::VERSION` instead).
        let env = crate::snapshots::verify_snapshot_envelope(&mut file, origin, Some(from))
            .map_err(|e| ServiceError::MistaggedSnapshot {
                path: path.display().to_string(),
                source: e,
            })?;
        let installed = (install_fn)(&mut sm, origin, &mut file)
            .map_err(|e| ServiceError::Replay(format!("pinned install at {origin}: {e}")))?;
        // Two post-install checks on the TRAIT contract, because nothing
        // downstream can catch either: the tag is an EXCLUSIVE frontier, so a
        // cursor left AT or above `origin` swallows the frame starting at
        // `origin`, and a cursor left at `None` restarts the whole replay
        // from genesis under THIS version — the counterfactual this install
        // exists to avoid. (`None >= Some(_)` is false, so the `None` case
        // needs its own clause.)
        let cursor = sm.last_applied();
        if installed != origin || cursor.is_none() || cursor >= Some(origin) {
            return Err(ServiceError::Replay(format!(
                "pinned install at {origin} left the state machine at {cursor:?} \
                 (returned {installed}); install_snapshot must land at the tag \
                 with its cursor strictly below it"
            )));
        }
        eprintln!(
            "uc_service: row {row} pinned install of snap-{origin} \
             (from {from:#010x} to {to:#010x}, artifact built by {:#010x})",
            env.version
        );
    }

    // 2. Open the log buffer file (read-only in spirit: the service only ever
    //    uses the read APIs; a v2.x hardening may map PROT_READ). Its max_claim
    //    margin must match the node's, so take max_payload from the cnc header.
    let buffer = Arc::new(LogBuffer::open_file(
        &dir.join("log.buf"),
        Arc::clone(&cnc),
        meta.max_payload as usize,
    )?);

    // 3. Egress producer (service→everyone responses) + svc_query consumer
    //    (node→service queries; drained by Task 11). M14a: named for this
    //    process's row, found by name above.
    let egress_ring = BroadcastRing::open(&dir.join(format!("egress_service.{}.broadcast", row)))
        .map_err(|e| ServiceError::Ring(e.to_string()))?;
    let egress = Egress::new(egress_ring.producer());
    let svc_query_ring = SpscRing::open(&dir.join(format!("svc_query.{}.ring", row)))
        .map_err(|e| ServiceError::Ring(e.to_string()))?;
    let (_svc_query_producer, svc_query) = svc_query_ring.into_split();

    // Time-and-timers §4.4: the service→node schedule ring; this process is the
    // producer, the node's consensus agent the consumer.
    let svc_sched_ring = SpscRing::open(&dir.join(format!("svc_sched.{}.ring", row)))
        .map_err(|e| ServiceError::Ring(e.to_string()))?;
    let (svc_sched, _svc_sched_consumer) = svc_sched_ring.into_split();

    // 4. Publish the applied frontier. Over-reporting above the journal
    //    frontier is a drift (wrong/stale SM) — refuse. Under-reporting is
    //    safe (the apply loop's idempotent-skip re-applies nothing already
    //    seen). For M5 we publish `last_applied` (the position, not the frame
    //    end); the idempotent-skip makes the distinction harmless and Task 9's
    //    replay recomputes the true byte cursor.
    //
    //    Drift bound = the archive DURABLE frontier (`counters().durable`), not
    //    `commit` (Task 8 review). In any correct run the apply loop only ever
    //    advances `last_applied` up to `min(commit, durable) <= durable`, and
    //    the journal (archive) only guarantees replay availability up to
    //    `durable`. So `last_applied > durable` cannot arise from this cluster's
    //    history — it can only be a PERSISTENT SM carried in from a different (or
    //    newer) instance dir. Refuse rather than replay off a phantom cursor the
    //    journal can never satisfy. `unwrap_or(0)` folds the fresh-SM case in:
    //    `0 > durable` is never true, so a fresh SM never drifts.
    //
    //    M6 Task 5 note: this bound is UNCHANGED by purge. `durable` remains the
    //    right upper bound — a purge only raises the journal's LOWER floor
    //    (`first_base`); the below-floor case is handled downstream by the apply
    //    thread's gap guard (snapshot install or `SnapshotRequired`), not here.
    let last_applied = sm.last_applied();
    let frontier = cnc.counters().durable.load_acquire();
    if last_applied.unwrap_or(0) > frontier {
        return Err(ServiceError::Drift {
            service: last_applied.unwrap_or(0),
            journal: frontier,
        });
    }
    // The follower resumes from `last_applied` (a frame START); the apply loop's
    // idempotent-skip re-walks that one frame harmlessly, and if the live ring
    // has already scrolled past it the first `next_batch` returns `Overrun` and
    // the SAME journal-replay mechanism (Task 9) reconstructs + rejoins. Exactly
    // one rejoin mechanism — try-live-then-replay — covers both a caught-up
    // reattach and a fresh SM (`None -> 0`) on a long-scrolled ring.
    // Plan B2 T4 (review fix): after a PINNED install the follower resumes at
    // the ORIGIN, not at the artifact's internal cursor. The artifact tag is
    // an EXCLUSIVE frontier — everything below it IS the artifact — so the
    // frames in `(cursor, origin)` are already reflected in the installed
    // image and replaying them is not just wasted work: resuming below the
    // origin is what hands the reconstruction path a `start_pos` under the
    // purge floor, and its gap guard then re-installs the very artifact we
    // just installed. `uc_service::replay`'s own post-install path does the
    // same thing (`start_pos = installed; cursor = installed`).
    //
    // The SM's own cursor is deliberately left where `install_snapshot` put
    // it (strictly below the origin), so the apply loop's idempotency guard
    // dispatches the frame that starts exactly AT the origin.
    let start_pos = match pin {
        Some((origin, _, _)) => origin,
        None => last_applied.unwrap_or(0),
    };
    // …and the pinned `start_pos` gets the SAME drift bound the unpinned one
    // was just given. `last_applied` was checked above, but the pinned arm
    // replaces it with the ORIGIN, which is not the state machine's number at
    // all: it comes off the cnc page. A store-only `uc2ctl snapshot fetch`
    // (spec §5, admin op 9) can leave an artifact ABOVE this node's durable
    // frontier, and a pin naming it would publish `applied` above `durable` —
    // which the node's floor hold reads, so it must be bounded like every
    // other published `applied`.
    if start_pos > frontier {
        return Err(ServiceError::Drift {
            service: start_pos,
            journal: frontier,
        });
    }
    // `s` is the same slot reference taken for the pin read in step 1d.
    s.applied.store_release(start_pos);
    // Status: attached, incarnation += 1 (the prior life's value survives a
    // crash on the same page; a node restart zeroes it with the page).
    let (_, _, incarnation) = unpack_service_status(s.status.load_acquire());
    // Coordinated-snapshot spec §5.2: the capability bit lives in the free
    // 9..31 band of the SAME word `pack_service_status` fills (row in 0..8,
    // attached at bit 8, incarnation at 32..64), so OR-ing it in disturbs
    // neither the incarnation band nor the attached bit. The service process
    // is the word's only writer, and `Service::stop` re-packs it without the
    // bit — a detached row is not capable, which is exactly right.
    let capable = if snapshot_capable {
        CNC_SVC_STATUS_SNAPSHOT_CAPABLE
    } else {
        0
    };
    s.status
        .store_release(pack_service_status(row, true, incarnation.wrapping_add(1)) | capable);
    // cnc 3.1: the attaching service's declared version, for observability
    // (`ServiceStatusLine::version`) — written once, here, alongside status.
    s.status.store_version(S::VERSION);
    // 5. Bump the epoch AFTER applied, AcqRel — the discipline the node's
    //    capture-recheck bracket relies on (unchanged, now per slot).
    let epoch = s.epoch.fetch_add(1) + 1;

    let poisoned = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let follower = LogFollower::new(Arc::clone(&buffer), start_pos);
    let apply_state = ApplyState {
        poisoned: Arc::clone(&poisoned),
        follower,
        sm: Arc::new(Mutex::new(sm)),
        cnc: Arc::clone(&cnc),
        egress,
        resp_buf: Vec::with_capacity(256),
        journal_dir: dir.join("journal"),
        svc_query,
        svc_sched,
        announce_pending: true,
        was_leader: false,
        pending: std::collections::HashMap::new(),
        table_last: std::collections::HashMap::new(),
        needs_replay: false,
        replay_wait: None,
        replay_stalled: None,
        instance_id,
        instance_mismatch_streak: 0,
        my_epoch: epoch,
        service_id: row,
        // Plan B2 T4 (review fix): the reconstruction path needs the pin too —
        // the artifact at the pinned ORIGIN was built by the pin's `from`, so
        // the gap guard's same-version rule (plan B2 T3) has to make an
        // exception for exactly that one artifact. See `replay::replay_into`.
        pin,
        lag_mode,
        declared,
        lag_waiting: false,
        // M6 Task 3: only `start_with_snapshots` installs a real trigger
        // (it needs `S: SnapshotStateMachine`, a bound `attach` doesn't
        // carry) — it overwrites this field on the `Attached` this function
        // returns, before spawning the apply thread.
        snapshot_trigger: None,
        // M6 Task 5: likewise, only `start_with_snapshots` installs the
        // below-floor reconstruction capability; a plain `start()` leaves it
        // `None`, so a purged-below gap fail-stops with `SnapshotRequired`.
        snapshot_restore: None,
    };

    Ok(Attached {
        apply_state,
        buffer,
        cnc,
        instance_id,
        epoch,
        poisoned,
        service_id: row,
        _lock: lock,
        install,
        pin,
    })
}

#[cfg(test)]
mod tests {
    use super::lag_mode_for;
    use crate::lag::LagMode;
    use uc_log::cnc::{CncMeta, CncPage};

    fn page() -> std::sync::Arc<CncPage> {
        CncPage::heap(&CncMeta {
            node_id: 1,
            instance_id: 1,
            app_id: "attach-test".into(),
            buffer_bytes: 1 << 20,
            max_payload: 256,
            services: [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES],
        })
    }

    // Review fix (fix round 1): `lag_mode_for` must read the RAW
    // `services_declared` value, not the effective/folded declared-set mask
    // `attach()` computes for its per-id gate — a fresh/`none_for_tests` page
    // (`services_declared == 0`) is a harness node and must attach as
    // `LagMode::Off`, never `Lockstep`/`Bounded`, regardless of whatever
    // `fsm_lag_bytes` happens to hold.
    #[test]
    fn undeclared_page_is_off_even_with_a_nonzero_lag_bound() {
        let p = page();
        assert_eq!(p.services_declared(), 0, "fresh page: nothing declared");
        p.store_fsm_lag_bytes(1 << 18); // a real bound, NOT lockstep's 0
        assert_eq!(lag_mode_for(&p), LagMode::Off);
    }

    // The lockstep-collision case named in the review: an undeclared page
    // whose `fsm_lag_bytes` also happens to be 0 must still read as `Off`,
    // not `Lockstep` — this is exactly the bug the fold-before-mode_from_page
    // ordering introduced (harmless when the bound was nonzero, silent when
    // it was 0 too).
    #[test]
    fn undeclared_page_is_off_not_lockstep_when_lag_bytes_is_also_zero() {
        let p = page();
        assert_eq!(p.services_declared(), 0);
        assert_eq!(p.fsm_lag_bytes(), 0);
        assert_eq!(lag_mode_for(&p), LagMode::Off);
    }

    // ---- the boot gap (2026-09-10 review of the cnc meta() fix) ----

    struct CountSm;
    impl crate::traits::RawStateMachine for CountSm {
        const NAME: &'static str = "count";
        fn apply(&mut self, _ctx: &mut crate::ApplyCtx, _cmd: &[u8], _out: &mut Vec<u8>) {}
        fn query(&self, _q: &[u8], _out: &mut Vec<u8>) {}
        fn last_applied(&self) -> Option<u64> {
            None
        }
    }

    /// Real disk under the cargo target tree (CLAUDE.md's scratch rule).
    fn scratch() -> tempfile::TempDir {
        let base = std::env::current_exe()
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        tempfile::tempdir_in(base).unwrap()
    }

    /// A page exactly as a booting node leaves it between `create_file` (a
    /// complete, crc-valid header with names on line 7) and its
    /// `store_services_declared`: the declared word still reads 0.
    fn file_page(
        dir: &std::path::Path,
        names: &[&str],
        declared: Option<u64>,
    ) -> std::sync::Arc<CncPage> {
        let mut services = [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES];
        for (i, n) in names.iter().enumerate() {
            services[i] = Some(uc_protocol::identity::FsmName::parse(n).unwrap());
        }
        let page = CncPage::create_file(
            &dir.join("cnc2.dat"),
            &CncMeta {
                node_id: 1,
                instance_id: 7,
                app_id: "boot-gap".into(),
                buffer_bytes: 1 << 20,
                max_payload: 256,
                services,
            },
        )
        .unwrap();
        if let Some(d) = declared {
            page.store_services_declared(d);
        }
        page
    }

    fn try_attach(dir: &std::path::Path) -> Option<crate::config::ServiceError> {
        let cfg = crate::config::ServiceConfig::new(dir, "boot-gap");
        super::attach(&cfg, CountSm, None).err()
    }

    /// Names on line 7 with `services_declared == 0` is a page no configured
    /// node ever publishes and no harness page ever has: it is the node
    /// mid-boot. Attaching then would fix `LagMode::Off` for the service's
    /// life (`lag_mode_for`'s harness arm), so it must be refused by name.
    #[test]
    fn names_present_with_declared_zero_is_refused_as_booting() {
        let dir = scratch();
        let _page = file_page(dir.path(), &["count"], None);
        match try_attach(dir.path()) {
            Some(crate::config::ServiceError::NodeBooting) => {}
            other => panic!("expected NodeBooting, got {other:?}"),
        }
    }

    /// The discriminator is exactly that pair. The same page with the
    /// declared word published is a real one-FSM node (it fails later, on
    /// the rings this scratch dir does not have — anything but `NodeBooting`),
    /// and a harness page (no names, declared 0) is not booting either.
    #[test]
    fn a_published_declared_set_or_a_harness_page_is_not_booting() {
        let dir = scratch();
        let _page = file_page(dir.path(), &["count"], Some(0b1));
        assert!(
            !matches!(
                try_attach(dir.path()),
                Some(crate::config::ServiceError::NodeBooting)
            ),
            "a published declared set is not the boot gap"
        );

        let dir = scratch();
        let _page = file_page(dir.path(), &[], None);
        assert!(
            !matches!(
                try_attach(dir.path()),
                Some(crate::config::ServiceError::NodeBooting)
            ),
            "a harness page (no names, declared 0) is not the boot gap"
        );
    }
}
