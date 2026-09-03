// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The service attach sequence (spec §7, v1 task14 discipline). ORDER IS
//! LOAD-BEARING — see the numbered steps.

use std::sync::Arc;
use std::sync::Mutex;

use uc_log::buffer::LogBuffer;
use uc_log::cnc::{CncPage, pack_service_status, unpack_service_status};
use uc_log::reader::LogFollower;
use uc_protocol::ring::{BroadcastRing, SpscRing};

use crate::apply::ApplyState;
use crate::config::{ServiceConfig, ServiceError};
use crate::egress::Egress;
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

/// Run the 6-step attach. Steps 1–5 here; step 6 (spawn the threads) is the
/// builder's job, after this returns.
pub(crate) fn attach<S: RawStateMachine>(
    cfg: &ServiceConfig,
    sm: S,
) -> Result<Attached<S>, ServiceError> {
    let dir = &cfg.instance_dir;

    // 1. Open + validate the cnc page (magic/crc/version/app_id). Capture the
    //    node's per-boot instance_id (a fresh id invalidates a stale attach).
    let cnc = CncPage::open_file(&dir.join("cnc2.dat"), &cfg.app_id)?;
    let meta = cnc.meta();
    let instance_id = meta.instance_id;

    // 1b. Find our row BY NAME (spec §4.3). A harness page (`none_for_tests`:
    // `services_declared == 0` and no names declared) rings row 0 for
    // whoever attaches — the pre-M14c multi-service-oblivious contract.
    let names = cnc.service_names();
    let harness = cnc.services_declared() == 0 && names.iter().all(Option::is_none);
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
    let start_pos = last_applied.unwrap_or(0);
    let s = slot(&cnc, row);
    s.applied.store_release(start_pos);
    // Status: attached, incarnation += 1 (the prior life's value survives a
    // crash on the same page; a node restart zeroes it with the page).
    let (_, _, incarnation) = unpack_service_status(s.status.load_acquire());
    s.status
        .store_release(pack_service_status(row, true, incarnation.wrapping_add(1)));
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
        needs_replay: false,
        instance_id,
        instance_mismatch_streak: 0,
        my_epoch: epoch,
        service_id: row,
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
}
