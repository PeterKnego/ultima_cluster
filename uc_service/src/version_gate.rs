// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! #33 spec §7.2: at a version record (CLUSTER kind 4 or 6) for its own row,
//! a service defers to the cluster FSM's verdict — it waits until the
//! `uc2-cluster` agent has applied the record, then reads the row view. It
//! never decides from the record bytes: the cluster FSM may REFUSE a record,
//! and a refused record must change nothing (spec D7).

use uc_log::cnc::{CncPage, RowRead};
use uc_protocol::identity::{VersionDisplay, same_line};
use uc_protocol::v2::frame::{ClusterKind, FrameHeader, align_frame_len, read_cluster_prefix};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    Continue,
    Stop { running: u32 },
}

/// Pure decision once the agent has applied past `rec_end`:
/// - `record_pos < rec_end` → the record was refused → Continue;
/// - `record_pos == rec_end` and ours → Continue;
/// - otherwise (not ours, or a LATER record already superseded it) → Stop.
///
/// `None` = the view is contended; the caller retries.
///
/// The "later record" case stops even when the row reads this binary's line
/// again (a pin to v2 at R1, a pin back to v1 at R2): the service is AT R1,
/// and whatever R1 set, every frame between R1 and R2 is not this binary's
/// to apply unless R1 named its line — which the view no longer says. A stop
/// there is always safe: the restart attaches with `attach_record_pos ≥ R2`.
pub(crate) fn verdict(mine: u32, rec_end: u64, view: RowRead) -> Option<Verdict> {
    let RowRead::View {
        running,
        record_pos,
        ..
    } = view
    else {
        return None;
    };
    let running = running.unwrap_or(0);
    Some(
        if record_pos < rec_end || (record_pos == rec_end && same_line(mine, running)) {
            Verdict::Continue
        } else {
            Verdict::Stop { running }
        },
    )
}

/// The arm's wait budget (ruling R13): spin this many times, then yield
/// [`WAIT_YIELDS`] times, then give up for this cycle with
/// [`Gate::Pending`]. The agent applies at commit and this frame is already
/// committed here, so the common wait ends inside the spins; the whole budget
/// is on the order of one short apply cycle (a few µs of spinning plus 64
/// scheduler yields — tens to a few hundred µs), never a sleep.
const WAIT_SPINS: u32 = 1024;
/// See [`WAIT_SPINS`].
const WAIT_YIELDS: u32 = 64;

/// What the arm decided about one `CLUSTER` frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(crate) enum Gate {
    /// Not a version record for this row, already decided, refused, or
    /// accepted and on this binary's line: apply on.
    Pass,
    /// The record superseded this binary's line: stop at exactly `pos`.
    Stop { running: u32 },
    /// The `uc2-cluster` agent has not applied the record (or the view stayed
    /// contended) within the wait budget. The caller rewinds to `pos`,
    /// publishes `applied = pos` and ends the cycle; the next cycle re-walks
    /// the record (R13 — a wait must never starve the runner's stop check).
    Pending,
}

/// The apply loop's arm (out of line — M14a). `pos` is the frame START.
#[inline(never)]
pub(crate) fn on_cluster_frame(
    cnc: &CncPage,
    row: u8,
    mine: u32,
    decided_to: u64,
    pos: u64,
    hdr: &FrameHeader,
    payload: &[u8],
) -> Gate {
    // The same bytes the cluster agent hands `ClusterFsm::apply`, prefix
    // included; both version bodies start with the row byte.
    let Some((kind, body)) = read_cluster_prefix(payload) else {
        return Gate::Pass;
    };
    if !matches!(kind, ClusterKind::UpgradePin | ClusterKind::RowGenesis)
        || body.first() != Some(&row)
    {
        return Gate::Pass;
    }
    // A frame END, the unit `running_record_pos` is kept in (the cluster
    // agent computes the same end for the ctx it applies under).
    let rec_end = pos + align_frame_len(hdr.length as usize) as u64;
    if rec_end <= decided_to {
        return Gate::Pass; // decided by this incarnation's attach (spec §7.1)
    }
    let (mut spins, mut yields) = (0u32, 0u32);
    loop {
        // `cluster_applied` is stored (Release) AFTER the row words, so a
        // reader that sees it at or past `rec_end` sees the row as of then.
        if cnc.cluster_applied() >= rec_end
            && let Some(v) = verdict(
                mine,
                rec_end,
                cnc.service_slot(row as usize).status.row_view(),
            )
        {
            return match v {
                Verdict::Continue => Gate::Pass,
                Verdict::Stop { running } => Gate::Stop { running },
            };
        }
        // Never sleep on a live peer (CLAUDE.md, M14a): spin, then yield,
        // then hand the cycle back (R13).
        if spins < WAIT_SPINS {
            spins += 1;
            std::hint::spin_loop();
        } else if yields < WAIT_YIELDS {
            yields += 1;
            std::thread::yield_now();
        } else {
            return Gate::Pending;
        }
    }
}

/// R13, the live arm's `Pending`: rewind the follower to the record and
/// publish `applied = pos` — every frame before it applied, nothing at or
/// after it. Out of line and cold so the per-frame arm stays a type test,
/// one call and a small match.
#[cold]
#[inline(never)]
pub(crate) fn rewind_to_record(cursor: &mut u64, cnc: &CncPage, row: u8, pos: u64) {
    *cursor = pos;
    crate::attach::slot(cnc, row).applied.store_release(pos);
}

/// The named fail-stop message (spec §7.2).
pub(crate) fn stop_message(name: &str, running: u32, at: u64, mine: u32) -> String {
    format!(
        "version_superseded: row {name:?} moved to {} at position {at}; this binary ({}) \
         stopped there — restart it as a {}.{}.x build",
        VersionDisplay(running),
        VersionDisplay(mine),
        running >> 24,
        (running >> 16) & 0xff,
    )
}

/// Fail-stop at a superseding version record: the `version_superseded`
/// event, then the named panic — the apply thread's existing fail-stop idiom
/// (a supervisor restarts the process; the next attach reads a
/// `running_record_pos` at or past `at`, so the record is behind it and the
/// restart cannot stop here again). The caller has already published
/// `applied = at`.
#[cold]
#[inline(never)]
pub(crate) fn stop_fail(name: &str, running: u32, at: u64, mine: u32) -> ! {
    let running_s = VersionDisplay(running).to_string();
    let mine_s = VersionDisplay(mine).to_string();
    uc_obs::obs_event!(
        Error,
        "version_superseded",
        row_name = name,
        running = running_s.as_str(),
        mine = mine_s.as_str(),
        position = at,
    );
    panic!("{}", stop_message(name, running, at, mine))
}

/// The whole stop, out of line so the apply loop's arm stays a type test and
/// two calls: publish `applied = at` (every frame before the record applied,
/// nothing after), clear the slot's ATTACHED bit, release the SM guard (a
/// fail-stop must not poison the SM mutex the query path locks), then
/// [`stop_fail`].
///
/// Ruling R18: this stop is DELIBERATE, not a crash, so it clears ATTACHED
/// as `Service::stop` does (incarnation kept; a fresh attach bumps it) —
/// but ONLY that bit: `SNAPSHOT_CAPABLE` stays, so `uc2ctl snapshot` is not
/// refused 48 on a row stopped at a pin. Left set, the slot would read as a wedged live service — stale
/// heartbeat, attached bit on — and `Uc2ServiceWedged` would page on every
/// upgrade whose new build takes longer than its `for:` to attach. Cleared,
/// the row honestly reads absent until the new build takes the slot.
#[cold]
#[inline(never)]
pub(crate) fn stop_at_record<S: crate::traits::RawStateMachine>(
    guard: std::sync::MutexGuard<'_, S>,
    cnc: &CncPage,
    row: u8,
    running: u32,
    at: u64,
) -> ! {
    let slot = crate::attach::slot(cnc, row);
    slot.applied.store_release(at);
    // Clear ATTACHED and nothing else: the incarnation (a fresh attach bumps
    // it) and SNAPSHOT_CAPABLE stay, so the node can still command an
    // instant while the row sits stopped at the record. This apply thread is
    // the status word's writer while attached, with ONE exception (final
    // review M8): `Service::stop` on the owning thread also clears ATTACHED,
    // by a plain store rather than a read-modify-write, and the two may race
    // on this word. The race is harmless: both write ATTACHED clear with the
    // same incarnation, so either order leaves a detached row; the only
    // difference is whether SNAPSHOT_CAPABLE survives (this store keeps it,
    // `stop`'s drops it), and a stopped service is detached either way.
    let w = slot.status.load_acquire();
    slot.status
        .store_release(w & !uc_protocol::v2::cnc::CNC_SVC_STATUS_ATTACHED);
    drop(guard);
    stop_fail(S::IDENTITY.name.as_str(), running, at, S::VERSION)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_log::cnc::RowRead;
    use uc_protocol::identity::pack_version;
    const V1: u32 = pack_version(1, 0, 0);
    const V2: u32 = pack_version(2, 0, 0);
    fn view(running: Option<u32>, record_pos: u64) -> RowRead {
        RowRead::View {
            pin: None,
            running,
            record_pos,
        }
    }

    #[test]
    fn verdict_continues_past_a_refused_record() {
        // record_pos < rec_end: the record at rec_end was refused (Review Focus 4).
        assert_eq!(
            verdict(V1, 1280, view(Some(V1), 640)),
            Some(Verdict::Continue)
        );
    }
    #[test]
    fn verdict_continues_when_the_accepted_record_is_ours() {
        assert_eq!(
            verdict(pack_version(1, 0, 3), 1280, view(Some(V1), 1280)),
            Some(Verdict::Continue)
        );
    }
    #[test]
    fn verdict_stops_when_the_accepted_record_is_not_ours() {
        assert_eq!(
            verdict(V1, 1280, view(Some(V2), 1280)),
            Some(Verdict::Stop { running: V2 })
        );
    }
    #[test]
    fn verdict_stops_when_a_later_record_superseded_this_one() {
        // Review Focus 1: R1 (to v2) at 1280, R2 (back to v1) at 1920; a v1
        // service reaching R1 must stop even though running reads v1 again.
        assert_eq!(
            verdict(V1, 1280, view(Some(V1), 1920)),
            Some(Verdict::Stop { running: V1 })
        );
    }
    #[test]
    fn verdict_on_a_contended_view_is_retry() {
        assert_eq!(verdict(V1, 1280, RowRead::Contended), None);
    }

    // ---- on_cluster_frame over a real (heap) cnc page ----

    use uc_log::cnc::{CncMeta, CncPage};
    use uc_protocol::v2::frame::{CLUSTER_BODY_PREFIX_LEN, FRAME_TYPE_CLUSTER, HEADER_LEN};

    const ROW: u8 = 0;
    /// A `RowGenesis` frame: 32 B header + 8 B prefix + 8 B body = 48 B,
    /// aligned to 64 — so a frame starting at `START` ends at `START + 64`.
    const START: u64 = 1216;
    const END: u64 = START + 64;

    fn page() -> std::sync::Arc<CncPage> {
        CncPage::heap(&CncMeta {
            node_id: 1,
            instance_id: 7,
            app_id: "t".into(),
            buffer_bytes: 1 << 20,
            max_payload: 256,
            services: [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES],
        })
    }

    fn cluster_frame(kind: ClusterKind, body: &[u8]) -> (FrameHeader, Vec<u8>) {
        let mut payload = vec![0u8; CLUSTER_BODY_PREFIX_LEN];
        payload[0] = kind as u8;
        payload.extend_from_slice(body);
        let hdr = FrameHeader {
            length: (HEADER_LEN + payload.len()) as u32,
            frame_type: FRAME_TYPE_CLUSTER,
            flags: 0,
            leadership_term_id: 1,
            client_id: 0,
            seq: 0,
            time_ns: 0,
        };
        (hdr, payload)
    }

    fn genesis(row: u8, version: u32) -> (FrameHeader, Vec<u8>) {
        let mut b = vec![row, 0, 0, 0];
        b.extend_from_slice(&version.to_le_bytes());
        cluster_frame(ClusterKind::RowGenesis, &b)
    }

    #[test]
    fn a_superseding_genesis_stops_once_the_agent_has_applied_it() {
        let p = page();
        p.service_slot(0).status.store_row_view(None, Some(V2), END);
        p.store_cluster_applied(END);
        let (h, pl) = genesis(ROW, V2);
        assert_eq!(
            on_cluster_frame(&p, ROW, V1, 0, START, &h, &pl),
            Gate::Stop { running: V2 }
        );
        // The same record for a matching line build is not a stop.
        assert_eq!(
            on_cluster_frame(&p, ROW, pack_version(2, 0, 9), 0, START, &h, &pl),
            Gate::Pass
        );
    }

    #[test]
    fn records_that_are_not_this_rows_versions_return_at_once() {
        // Nothing is published on the page: a wait would never end, so each
        // of these must decide without one.
        let p = page();
        let (h, pl) = genesis(1, V2);
        assert_eq!(
            on_cluster_frame(&p, ROW, V1, 0, START, &h, &pl),
            Gate::Pass,
            "other row"
        );
        let (h, pl) = cluster_frame(ClusterKind::Settings, &[0u8; 40]);
        assert_eq!(
            on_cluster_frame(&p, ROW, V1, 0, START, &h, &pl),
            Gate::Pass,
            "other kind"
        );
        let (h, pl) = genesis(ROW, V2);
        assert_eq!(
            on_cluster_frame(&p, ROW, V1, END, START, &h, &pl),
            Gate::Pass,
            "at or below attach_record_pos: decided by the attach"
        );
        assert_eq!(
            on_cluster_frame(&p, ROW, V1, 0, START, &h, &[]),
            Gate::Pass,
            "no prefix"
        );
    }

    /// R13: the arm waits a BOUNDED budget, then answers `Pending`; a caller
    /// that re-asks once the agent has applied the record gets the verdict.
    #[test]
    fn the_arm_waits_a_bounded_budget_then_decides_on_a_later_ask() {
        let p = page();
        let (h, pl) = genesis(ROW, V2);
        let t0 = std::time::Instant::now();
        assert_eq!(
            on_cluster_frame(&p, ROW, V1, 0, START, &h, &pl),
            Gate::Pending,
            "the agent never advanced"
        );
        assert!(
            t0.elapsed() < std::time::Duration::from_millis(500),
            "the budget is bounded: {:?}",
            t0.elapsed()
        );
        let writer = {
            let p = std::sync::Arc::clone(&p);
            std::thread::spawn(move || {
                std::thread::sleep(std::time::Duration::from_millis(50));
                // The agent's order: row words first, then cluster_applied.
                p.service_slot(0).status.store_row_view(None, Some(V2), END);
                p.store_cluster_applied(END);
            })
        };
        let (mut pendings, t0) = (0u32, std::time::Instant::now());
        let gate = loop {
            match on_cluster_frame(&p, ROW, V1, 0, START, &h, &pl) {
                Gate::Pending => pendings += 1,
                g => break g,
            }
            assert!(
                t0.elapsed() < std::time::Duration::from_secs(5),
                "never decided"
            );
        };
        assert_eq!(gate, Gate::Stop { running: V2 });
        assert!(pendings >= 1, "the 50 ms wait outlasts one budget");
        writer.join().unwrap();
    }

    #[test]
    fn a_restart_past_a_superseded_record_does_not_stop_there_again() {
        // Review Focus 2: R1 at END was superseded by R2 at END + 640, which
        // moved the row back to this binary's line. An incarnation that
        // attached AFTER R2 (attach_record_pos = R2's end) walks past R1;
        // one that attached before it stops at R1.
        let p = page();
        let r2 = END + 640;
        p.service_slot(0).status.store_row_view(None, Some(V1), r2);
        p.store_cluster_applied(r2);
        let (h, pl) = genesis(ROW, V2);
        assert_eq!(
            on_cluster_frame(&p, ROW, V1, r2, START, &h, &pl),
            Gate::Pass
        );
        assert_eq!(
            on_cluster_frame(&p, ROW, V1, 0, START, &h, &pl),
            Gate::Stop { running: V1 }
        );
    }

    #[test]
    fn a_refused_record_is_passed_even_with_an_older_view() {
        // cluster_applied is past the record but record_pos is older: the
        // FSM refused it, and a refused record changes nothing (D7).
        let p = page();
        p.service_slot(0).status.store_row_view(None, Some(V1), 640);
        p.store_cluster_applied(END + 4096);
        let (h, pl) = genesis(ROW, V2);
        assert_eq!(
            on_cluster_frame(&p, ROW, V1, 640, START, &h, &pl),
            Gate::Pass
        );
    }

    #[test]
    fn the_stop_message_names_row_versions_position_and_remedy() {
        let m = stop_message("kv", pack_version(2, 1, 0), 4096, pack_version(2, 0, 3));
        assert_eq!(
            m,
            "version_superseded: row \"kv\" moved to 2.1.0 at position 4096; this binary \
             (2.0.3) stopped there — restart it as a 2.1.x build"
        );
    }
}
