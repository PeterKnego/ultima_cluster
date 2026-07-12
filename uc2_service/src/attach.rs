// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The service attach sequence (spec §7, v1 task14 discipline). ORDER IS
//! LOAD-BEARING — see the numbered steps.

use std::sync::Arc;
use std::sync::RwLock;

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
    let last_applied = sm.last_applied();
    if let Some(la) = last_applied {
        let frontier = cnc.counters().durable.load_acquire();
        if la > frontier {
            return Err(ServiceError::Drift { service: la, journal: frontier });
        }
    }
    let start_pos = last_applied.unwrap_or(0);
    cnc.service().service_applied.store_release(start_pos);

    // 5. Bump the service epoch — the incarnation marker — AFTER step 4, with
    //    AcqRel (fetch_add), so a barrier reader that captures
    //    epoch→applied→epoch sees a consistent snapshot. This is the
    //    bump-once-at-attach point; it happens before the thread is READY.
    let epoch = cnc.service().service_epoch.fetch_add(1) + 1;

    let follower = LogFollower::new(buffer, start_pos);
    let apply_state = ApplyState {
        follower,
        sm: RwLock::new(sm),
        cnc: Arc::clone(&cnc),
        egress,
        svc_query,
        needs_replay: false,
    };

    Ok(Attached { apply_state, cnc, instance_id, epoch })
}
