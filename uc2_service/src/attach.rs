// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The service attach sequence (spec §7, v1 task14 discipline). ORDER IS
//! LOAD-BEARING — see the numbered steps.

use std::sync::Arc;
use std::sync::Mutex;

use uc2_log::buffer::LogBuffer;
use uc2_log::cnc::CncPage;
use uc2_log::reader::LogFollower;
use uc_protocol::ring::{BroadcastRing, SpscRing};

use crate::apply::ApplyState;
use crate::config::{ServiceConfig, ServiceError};
use crate::egress::Egress;
use crate::traits::StateMachine;

/// The pieces the builder needs after a successful attach: the apply agent's
/// state (moved into its thread) plus the handles the `Service` keeps.
pub(crate) struct Attached<S: StateMachine> {
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
}

/// Run the 6-step attach. Steps 1–5 here; step 6 (spawn the threads) is the
/// builder's job, after this returns.
pub(crate) fn attach<S: StateMachine>(
    cfg: &ServiceConfig,
    sm: S,
) -> Result<Attached<S>, ServiceError> {
    let dir = &cfg.instance_dir;

    // 1. Open + validate the cnc page (magic/crc/version/app_id). Capture the
    //    node's per-boot instance_id (a fresh id invalidates a stale attach).
    let cnc = CncPage::open_file(&dir.join("cnc2.dat"), &cfg.app_id)?;
    let meta = cnc.meta();
    let instance_id = meta.instance_id;

    // 2. Open the log buffer file (read-only in spirit: the service only ever
    //    uses the read APIs; a v2.x hardening may map PROT_READ). Its max_claim
    //    margin must match the node's, so take max_payload from the cnc header.
    let buffer =
        Arc::new(LogBuffer::open_file(&dir.join("log.buf"), Arc::clone(&cnc), meta.max_payload as usize)?);

    // 3. Egress producer (service→everyone responses) + svc_query consumer
    //    (node→service queries; drained by Task 11).
    let egress_ring = BroadcastRing::open(&dir.join("egress_service.broadcast"))
        .map_err(|e| ServiceError::Ring(e.to_string()))?;
    let egress = Egress::new(egress_ring.producer());
    let svc_query_ring =
        SpscRing::open(&dir.join("svc_query.ring")).map_err(|e| ServiceError::Ring(e.to_string()))?;
    let (_svc_query_producer, svc_query) = svc_query_ring.into_split();

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
    let last_applied = sm.last_applied();
    let frontier = cnc.counters().durable.load_acquire();
    if last_applied.unwrap_or(0) > frontier {
        return Err(ServiceError::Drift { service: last_applied.unwrap_or(0), journal: frontier });
    }
    // The follower resumes from `last_applied` (a frame START); the apply loop's
    // idempotent-skip re-walks that one frame harmlessly, and if the live ring
    // has already scrolled past it the first `next_batch` returns `Overrun` and
    // the SAME journal-replay mechanism (Task 9) reconstructs + rejoins. Exactly
    // one rejoin mechanism — try-live-then-replay — covers both a caught-up
    // reattach and a fresh SM (`None -> 0`) on a long-scrolled ring.
    let start_pos = last_applied.unwrap_or(0);
    cnc.service().service_applied.store_release(start_pos);

    // 5. Bump the service epoch — the incarnation marker — AFTER step 4, with
    //    AcqRel (fetch_add), so a barrier reader that captures
    //    epoch→applied→epoch sees a consistent snapshot. This is the
    //    bump-once-at-attach point; it happens before the thread is READY.
    let epoch = cnc.service().service_epoch.fetch_add(1) + 1;

    let follower = LogFollower::new(Arc::clone(&buffer), start_pos);
    let apply_state = ApplyState {
        follower,
        sm: Arc::new(Mutex::new(sm)),
        cnc: Arc::clone(&cnc),
        egress,
        journal_dir: dir.join("journal"),
        svc_query,
        needs_replay: false,
        instance_id,
        instance_mismatch_streak: 0,
        my_epoch: epoch,
    };

    Ok(Attached { apply_state, buffer, cnc, instance_id, epoch })
}
