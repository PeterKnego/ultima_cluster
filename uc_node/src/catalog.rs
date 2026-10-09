// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The catalog's query layer (catalog spec §4.3-§4.4): pure, total functions
//! over the replicated set list (`ClusterViewInner::catalog`, oldest first)
//! plus [`SoftTable`], a node's SOFT per-node table of advertised
//! [`Holdings`]. The soft table is never replicated and never persisted —
//! gossip-shaped, rebuilt purely from inbound `STATUS` datagrams as they
//! arrive, so a restart starts it empty and a node that stops advertising
//! simply ages out of [`SoftTable::live`].
//!
//! This module is pure: no I/O, no clock reads. `now_ns` and `stale_ns` /
//! `timeout_ns` are always inputs, never sampled in here — the caller (the
//! node, `uc2ctl`, a test) supplies them. Every function here is total: an
//! unlisted position, an empty `sets` slice, or a row index past
//! `CNC_MAX_SERVICES` all answer "nothing", never panic.
//!
//! Consumed by: Task 8 (a node's effective floor), Task 9 (the leader fills
//! `SoftTable` from inbound `STATUS`), Task 10 (gauges), Task 12 (e2e tests)
//! and later `uc2ctl`.

use std::collections::BTreeMap;

use uc_consensus::election::NodeId;
use uc_protocol::identity::same_line;
use uc_protocol::v2::catalog::{CLUSTER_ROW, RowVerdict, SetEntry, SetState};
use uc_protocol::v2::datagram::Holdings;
use uc_protocol::v2::upgrade::SnapshotReport;

/// One node's last-advertised [`Holdings`] plus when it was recorded, for
/// [`SoftTable::live`]'s staleness check.
pub struct SoftEntry {
    pub holdings: Holdings,
    pub last_seen_ns: u64,
}

/// The soft per-node table (catalog spec §4.4): what every node last said it
/// holds. Keyed by [`NodeId`] so a later `record` for the same node simply
/// replaces its entry — there is no history, only the latest advertisement.
#[derive(Default)]
pub struct SoftTable {
    pub by_node: BTreeMap<NodeId, SoftEntry>,
}

impl SoftTable {
    /// Record (or replace) `node`'s advertised holdings as of `now_ns`.
    pub fn record(&mut self, node: NodeId, h: Holdings, now_ns: u64) {
        self.by_node.insert(
            node,
            SoftEntry {
                holdings: h,
                last_seen_ns: now_ns,
            },
        );
    }

    /// Entries not yet stale: `now_ns - last_seen_ns <= stale_ns`
    /// (saturating, so a `now_ns` before `last_seen_ns` never underflows).
    pub fn live(&self, now_ns: u64, stale_ns: u64) -> impl Iterator<Item = (NodeId, &Holdings)> {
        self.by_node.iter().filter_map(move |(&id, e)| {
            if now_ns.saturating_sub(e.last_seen_ns) <= stale_ns {
                Some((id, &e.holdings))
            } else {
                None
            }
        })
    }
}

/// `row`'s packed version within `e` — `None` for a row index at or past
/// `CNC_MAX_SERVICES` (never a panic; the caller treats "no such row" as "no
/// match") and `None` for a row whose verdict is `Unreported`: such a row
/// built nothing at P, and its `version` field is the default `0` — which is
/// also a legitimate packed version, so reading it would let an unreported
/// row match `agreed_for(row, 0)`. `CLUSTER_ROW` (255) reads the cluster
/// artifact's entry under the same rule.
fn row_version(e: &SetEntry, row: u8) -> Option<u32> {
    let r = if row == CLUSTER_ROW {
        &e.cluster
    } else {
        e.rows.get(row as usize)?
    };
    (r.verdict != RowVerdict::Unreported).then_some(r.version)
}

/// A read-only view over the catalog's set list plus the soft table, with
/// the inputs a pure query needs (spec §4.3's query set). Built fresh by the
/// caller for each query — it borrows, never owns, `sets` and `soft`.
pub struct CatalogQuery<'a> {
    pub sets: &'a [SetEntry],
    /// The content hash of the catalog `sets` is
    /// (`ClusterView::catalog_version`) — what a holder's
    /// `Holdings.catalog_position` must equal; the name predates the ruling.
    pub catalog_position: u64,
    pub soft: &'a SoftTable,
    pub now_ns: u64,
    pub stale_ns: u64,
}

impl CatalogQuery<'_> {
    /// `sets`'s index for `p`, or `None` if `p` is not a listed position —
    /// total over an empty `sets` slice.
    fn index_of(&self, p: u64) -> Option<usize> {
        self.sets.iter().position(|e| e.position == p)
    }

    /// The newest `is_agreed()` entry at or below `at_most`, or `None` if
    /// there is none (an empty catalog, or every entry above `at_most`).
    pub fn newest_agreed(&self, at_most: u64) -> Option<u64> {
        self.sets
            .iter()
            .filter(|e| e.is_agreed() && e.position <= at_most)
            .map(|e| e.position)
            .max()
    }

    /// Agreed entries whose `row`'s version is the SAME LINE as `version`
    /// (major.minor match, patch free — `same_line`); `row == CLUSTER_ROW`
    /// reads the cluster artifact's entry instead of a declared row.
    pub fn agreed_for(&self, row: u8, version: u32) -> Vec<u64> {
        self.sets
            .iter()
            .filter(|e| e.is_agreed())
            .filter_map(|e| {
                let v = row_version(e, row)?;
                same_line(v, version).then_some(e.position)
            })
            .collect()
    }

    /// Nodes that count as holding the set at `p`: live in the soft table,
    /// advertising the same `catalog_position` — the content hash of the
    /// catalog (`ClusterView::catalog_version`); the name predates the
    /// ruling — this query is against, with
    /// bit `i` of `sets_held` set, `i` = `p`'s index in `sets`. `p` not
    /// listed, or an index at or past bit 63, answers empty — never a shift
    /// panic.
    pub fn holders(&self, p: u64) -> Vec<NodeId> {
        let Some(i) = self.index_of(p) else {
            return Vec::new();
        };
        if i >= 64 {
            return Vec::new();
        }
        let bit = 1u64 << i;
        self.soft
            .live(self.now_ns, self.stale_ns)
            .filter(|(_, h)| h.catalog_position == self.catalog_position && h.sets_held & bit != 0)
            .map(|(id, _)| id)
            .collect()
    }

    /// Live nodes whose advertised journal span covers `[p, q]`:
    /// `journal_first <= p && durable >= q`.
    pub fn journal_covers(&self, p: u64, q: u64) -> Vec<NodeId> {
        self.soft
            .live(self.now_ns, self.stale_ns)
            .filter(|(_, h)| h.journal_first <= p && h.durable >= q)
            .map(|(id, _)| id)
            .collect()
    }

    /// Positions of `Commanded` entries whose instant is older than
    /// `timeout_ns` (saturating, so a timeout past `u64::MAX - time_ns`
    /// never wraps into "not stalled").
    pub fn stalled(&self, timeout_ns: u64) -> Vec<u64> {
        self.sets
            .iter()
            .filter(|e| {
                e.state == SetState::Commanded && e.time_ns.saturating_add(timeout_ns) < self.now_ns
            })
            .map(|e| e.position)
            .collect()
    }

    /// Every `(position, row)` whose verdict is `Diverged` or `NoMajority`,
    /// rows `0..CNC_MAX_SERVICES` then the cluster row as `CLUSTER_ROW`
    /// (255), in position order (the catalog's own order — oldest first).
    pub fn diverged(&self) -> Vec<(u64, u8)> {
        let mut out = Vec::new();
        for e in self.sets {
            for (row, r) in e.rows.iter().enumerate() {
                if matches!(r.verdict, RowVerdict::Diverged | RowVerdict::NoMajority) {
                    out.push((e.position, row as u8));
                }
            }
            if matches!(
                e.cluster.verdict,
                RowVerdict::Diverged | RowVerdict::NoMajority
            ) {
                out.push((e.position, CLUSTER_ROW));
            }
        }
        out
    }

    /// Spans `(a, b]` between consecutive agreed entries that nobody can
    /// rebuild: no live node holds `b`'s set complete, and no live node's
    /// journal spans `[a, b]`.
    pub fn coverage_gaps(&self) -> Vec<(u64, u64)> {
        let agreed: Vec<u64> = self
            .sets
            .iter()
            .filter(|e| e.is_agreed())
            .map(|e| e.position)
            .collect();
        agreed
            .windows(2)
            .filter_map(|w| {
                let (a, b) = (w[0], w[1]);
                if self.holders(b).is_empty() && self.journal_covers(a, b).is_empty() {
                    Some((a, b))
                } else {
                    None
                }
            })
            .collect()
    }

    /// §4.4: the newest agreed set among those `node` advertises holding
    /// (per [`holders`](Self::holders)) — `None` when `node` holds nothing
    /// agreed (an empty catalog, or a voter on a learner-only cluster), in
    /// which case the caller falls back.
    pub fn effective_floor(&self, node: NodeId) -> Option<u64> {
        self.sets
            .iter()
            .filter(|e| e.is_agreed())
            .filter(|e| self.holders(e.position).contains(&node))
            .map(|e| e.position)
            .max()
    }
}

/// Snapshot-lifecycle spec §4: one row's start set — `position == 0` = none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StartSet {
    pub position: u64,
    /// The packed version that built the row's artifact (`RowEntry.version`).
    pub version: u32,
}

/// Snapshot-lifecycle spec §4.1: row `row`'s start set — the NEWEST entry
/// that (1) is agreed, (2) has `rows[row]` Agreed (a row the set did not
/// report is not eligible for it), (3) this node holds complete on disk
/// (`held`, the consensus agent's `holdings_held`), and (4) sits at or below
/// `frontier = min(commit, durable)`. An empty catalog answers none.
///
/// The second value is the lowest position of a set that passes (1)-(3)
/// but sits ABOVE `frontier` — when the frontier reaches it the answer
/// changes, so the caller recomputes then (spec §4.3); `u64::MAX` = none.
pub fn start_set_for(row: u8, sets: &[SetEntry], held: &[u64], frontier: u64) -> (StartSet, u64) {
    let mut wait_above = u64::MAX;
    for e in sets.iter().rev() {
        let Some(r) = e.rows.get(row as usize) else {
            break;
        };
        if !e.is_agreed() || r.verdict != RowVerdict::Agreed || !held.contains(&e.position) {
            continue;
        }
        if e.position > frontier {
            wait_above = wait_above.min(e.position);
            continue;
        }
        return (
            StartSet {
                position: e.position,
                version: r.version,
            },
            wait_above,
        );
    }
    (StartSet::default(), wait_above)
}

/// Plan ruling P1: whom to ask for a set, in order. Known holders first —
/// `live_holders` (the leader's soft table; empty on a follower, which has
/// none) and `builders` ([`builders_at`]) — then every other member; each
/// tier learners first, then lowest node id (spec §6). Never `self_id`, no
/// duplicates. A member that turns out not to hold the set answers nothing
/// and costs one fetch timeout.
pub fn fetch_candidates(
    self_id: NodeId,
    live_holders: &[NodeId],
    builders: &[NodeId],
    learners: &[NodeId],
    voters: &[NodeId],
) -> Vec<NodeId> {
    let order = |v: &mut Vec<NodeId>| {
        v.sort_unstable();
        v.dedup();
        v.sort_by_key(|id| (!learners.contains(id), *id));
    };
    let mut known: Vec<NodeId> = live_holders
        .iter()
        .chain(builders)
        .copied()
        .filter(|&id| id != self_id)
        .collect();
    order(&mut known);
    let mut rest: Vec<NodeId> = learners
        .iter()
        .chain(voters)
        .copied()
        .filter(|id| *id != self_id && !known.contains(id))
        .collect();
    order(&mut rest);
    known.extend(rest);
    known
}

/// Plan ruling P1: the nodes that BUILT the set at `p` — reporters, in the
/// committed `SnapshotReport` records at `p`, whose hash equals the catalog's
/// row hash there. Replicated, so a follower knows them too. Empty when the
/// catalog does not list `p` or the records have moved past it.
pub fn builders_at(reports: &[SnapshotReport], sets: &[SetEntry], p: u64) -> Vec<NodeId> {
    let Some(e) = sets.iter().find(|e| e.position == p) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for r in reports.iter().filter(|r| r.position == p) {
        let Some(row) = e.rows.get(r.row as usize) else {
            continue;
        };
        for &(id, h, _) in &r.hashes {
            if h == row.hash && !out.contains(&id) {
                out.push(id);
            }
        }
    }
    out
}

/// Review focus 5 / plan ruling P12: how many nodes reported the instant at
/// `p` (the widest committed row record there); `0` when none is held.
pub fn reporters_at(reports: &[SnapshotReport], p: u64) -> usize {
    reports
        .iter()
        .filter(|r| r.position == p)
        .map(|r| r.hashes.len())
        .max()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::identity::pack_version;
    use uc_protocol::v2::catalog::*;

    const OK: RowEntry = RowEntry {
        version: 0x0102_0000,
        hash: 1,
        verdict: RowVerdict::Agreed,
        size: 0,
    };
    fn agreed(p: u64) -> SetEntry {
        let mut e = SetEntry::commanded(p, SetKind::Full, p);
        e.rows[0] = OK;
        e.cluster = OK;
        e.state = SetState::Complete;
        e
    }
    fn diverged(p: u64) -> SetEntry {
        let mut e = agreed(p);
        e.rows[0].verdict = RowVerdict::Diverged;
        e
    }
    fn agreed_v(p: u64, version: u32) -> SetEntry {
        let mut e = agreed(p);
        e.rows[0].version = version;
        e
    }

    /// Snapshot-lifecycle spec §4.1: the newest entry that is agreed, whose
    /// row is Agreed, that this node holds, at or below min(commit, durable).
    #[test]
    fn start_set_eligibility_table() {
        let sets = [agreed_v(1000, 7), agreed_v(2000, 8), agreed_v(3000, 9)];
        // agreed and held
        assert_eq!(
            start_set_for(0, &sets, &[1000, 2000, 3000], u64::MAX),
            (
                StartSet {
                    position: 3000,
                    version: 9
                },
                u64::MAX
            )
        );
        // not held -> the newest held one
        assert_eq!(start_set_for(0, &sets, &[1000], u64::MAX).0.position, 1000);
        // above min(commit, durable) -> the older one, and the frontier to watch
        assert_eq!(
            start_set_for(0, &sets, &[1000, 2000, 3000], 2500),
            (
                StartSet {
                    position: 2000,
                    version: 8
                },
                3000
            )
        );
        // row unreported in the set -> not eligible for that row
        assert_eq!(
            start_set_for(1, &sets, &[1000, 2000, 3000], u64::MAX).0,
            StartSet::default()
        );
        // a diverged set is skipped
        let with_div = [agreed_v(1000, 7), diverged(2000)];
        assert_eq!(
            start_set_for(0, &with_div, &[1000, 2000], u64::MAX)
                .0
                .position,
            1000
        );
        // a Commanded entry is skipped
        let cmd = [
            agreed_v(1000, 7),
            SetEntry::commanded(2000, SetKind::Full, 0),
        ];
        assert_eq!(
            start_set_for(0, &cmd, &[1000, 2000], u64::MAX).0.position,
            1000
        );
        // empty catalog -> none
        assert_eq!(
            start_set_for(0, &[], &[1000], u64::MAX),
            (StartSet::default(), u64::MAX)
        );
    }

    /// Review focus 1: a set still being fetched is not in the held list
    /// until its LAST artifact is renamed into place (the completion edge),
    /// so the publisher names the older held set - never a half-written one.
    #[test]
    fn start_set_skips_a_set_that_is_still_being_fetched() {
        let sets = [agreed_v(1000, 7), agreed_v(2000, 7)];
        let held_while_fetching_2000 = [1000];
        assert_eq!(
            start_set_for(0, &sets, &held_while_fetching_2000, u64::MAX)
                .0
                .position,
            1000
        );
    }
    fn holding(catalog_position: u64, sets_held: u64, first: u64, durable: u64) -> Holdings {
        Holdings {
            catalog_position,
            sets_held,
            journal_first: first,
            durable,
            commit: durable,
            ..Default::default()
        }
    }
    fn q<'a>(sets: &'a [SetEntry], soft: &'a SoftTable) -> CatalogQuery<'a> {
        CatalogQuery {
            sets,
            catalog_position: 42,
            soft,
            now_ns: 10_000,
            stale_ns: 900,
        }
    }

    #[test]
    fn newest_agreed_skips_diverged_and_commanded_sets() {
        let sets = [
            agreed(1000),
            diverged(2000),
            SetEntry::commanded(2500, SetKind::Full, 0),
            agreed(3000),
        ];
        let soft = SoftTable::default();
        let c = q(&sets, &soft);
        assert_eq!(c.newest_agreed(u64::MAX), Some(3000));
        assert_eq!(
            c.newest_agreed(2999),
            Some(1000),
            "2000 diverged, 2500 commanded"
        );
        assert_eq!(c.newest_agreed(999), None);
    }

    #[test]
    fn holders_requires_live_and_matching_catalog_position() {
        let sets = [agreed(1000), agreed(2000)];
        let mut soft = SoftTable::default();
        soft.record(1, holding(42, 0b10, 0, 5000), 9_500); // live, holds 2000
        soft.record(2, holding(42, 0b10, 0, 5000), 9_000); // stale: 10_000 - 9_000 > 900
        soft.record(3, holding(41, 0b10, 0, 5000), 9_900); // wrong catalog position
        soft.record(4, holding(42, 0b01, 0, 5000), 9_900); // holds 1000 only
        let c = q(&sets, &soft);
        assert_eq!(c.holders(2000), vec![1]);
        assert_eq!(c.holders(1000), vec![4]);
        assert_eq!(c.holders(3000), Vec::<NodeId>::new(), "not a listed set");
    }

    #[test]
    fn journal_covers_is_first_le_p_and_durable_ge_q() {
        let sets = [agreed(1000)];
        let mut soft = SoftTable::default();
        soft.record(1, holding(42, 0, 1000, 2000), 9_900);
        soft.record(2, holding(42, 0, 1001, 2000), 9_900);
        soft.record(3, holding(42, 0, 1000, 1999), 9_900);
        let c = q(&sets, &soft);
        assert_eq!(c.journal_covers(1000, 2000), vec![1]);
    }

    #[test]
    fn stalled_names_commanded_sets_past_the_timeout() {
        let sets = [
            SetEntry::commanded(100, SetKind::Full, 1_000),
            SetEntry::commanded(200, SetKind::Full, 9_800),
            agreed(300),
        ];
        let soft = SoftTable::default();
        let c = q(&sets, &soft); // now_ns = 10_000
        assert_eq!(
            c.stalled(500),
            vec![100],
            "200 is only 200 ns old; 300 is complete"
        );
    }

    #[test]
    fn diverged_lists_every_non_agreed_row_including_the_cluster() {
        let mut e = agreed(1000);
        e.cluster.verdict = RowVerdict::NoMajority;
        let sets = [e, diverged(2000), agreed(3000)];
        let soft = SoftTable::default();
        assert_eq!(
            q(&sets, &soft).diverged(),
            vec![(1000, CLUSTER_ROW), (2000, 0)]
        );
    }

    #[test]
    fn coverage_gaps_are_spans_nobody_can_rebuild() {
        let sets = [agreed(1000), agreed(2000), agreed(3000)];
        let mut soft = SoftTable::default();
        soft.record(1, holding(42, 0b100, 2500, 3500), 9_900); // holds 3000; journal [2500, 3500]
        let c = q(&sets, &soft);
        // (1000, 2000]: nobody holds 2000 and no journal covers it → gap
        // (2000, 3000]: node 1 holds 3000 → covered
        assert_eq!(c.coverage_gaps(), vec![(1000, 2000)]);
    }

    #[test]
    fn effective_floor_is_what_this_node_holds() {
        let sets = [agreed(1000), agreed(2000)];
        let mut soft = SoftTable::default();
        soft.record(1, holding(42, 0b01, 0, 9000), 9_900);
        soft.record(2, holding(42, 0b00, 0, 9000), 9_900);
        let c = q(&sets, &soft);
        assert_eq!(c.newest_agreed(u64::MAX), Some(2000), "the cluster floor");
        assert_eq!(c.effective_floor(1), Some(1000), "node 1 holds only 1000");
        assert_eq!(
            c.effective_floor(2),
            None,
            "a voter on a learner-only cluster"
        );
    }

    #[test]
    fn agreed_for_matches_the_line_not_the_patch() {
        let sets = [agreed(1000), agreed(2000)];
        let soft = SoftTable::default();
        let c = q(&sets, &soft);
        assert_eq!(c.agreed_for(0, pack_version(1, 2, 7)), vec![1000, 2000]);
        assert_eq!(c.agreed_for(0, pack_version(1, 3, 0)), Vec::<u64>::new());
    }

    /// A row that never reported at P has `version = 0` in the entry — and
    /// `0` is a legitimate packed version — so an UNREPORTED row must never
    /// match `agreed_for(row, 0)`: it built nothing at P.
    #[test]
    fn an_unreported_row_never_matches_agreed_for() {
        let sets = [agreed(1000), agreed(2000)];
        assert_eq!(sets[0].rows[1].verdict, RowVerdict::Unreported);
        assert_eq!(sets[0].rows[1].version, 0);
        let soft = SoftTable::default();
        let c = q(&sets, &soft);
        assert_eq!(
            c.agreed_for(1, 0),
            Vec::<u64>::new(),
            "row 1 never reported"
        );
        // A reported row at version 0 still matches.
        let mut zero = agreed(3000);
        zero.rows[0].version = 0;
        let sets = [zero];
        let c = q(&sets, &soft);
        assert_eq!(c.agreed_for(0, 0), vec![3000]);
        // And the cluster row likewise: reported → read, unreported → None.
        assert_eq!(c.agreed_for(CLUSTER_ROW, 0x0102_0000), vec![3000]);
        let mut e = agreed(4000);
        e.cluster = RowEntry::default();
        assert_eq!(row_version(&e, CLUSTER_ROW), None);
        assert_eq!(row_version(&e, 0), Some(0x0102_0000));
        assert_eq!(row_version(&e, 200), None, "past CNC_MAX_SERVICES");
    }

    /// Boundaries of the time-based queries: `live` is inclusive at
    /// `now - last_seen == stale_ns`; `stalled` is strict, so an instant at
    /// exactly `time_ns + timeout == now` is NOT stalled yet.
    #[test]
    fn live_and_stalled_boundaries() {
        let mut soft = SoftTable::default();
        soft.record(1, holding(42, 0, 0, 0), 10_000 - 900); // exactly stale_ns old
        soft.record(2, holding(42, 0, 0, 0), 10_000 - 901);
        let live: Vec<NodeId> = soft.live(10_000, 900).map(|(id, _)| id).collect();
        assert_eq!(live, vec![1], "inclusive at the boundary");
        let sets = [
            SetEntry::commanded(100, SetKind::Full, 9_500), // 9_500 + 500 == now
            SetEntry::commanded(200, SetKind::Full, 9_499),
        ];
        let c = q(&sets, &soft);
        assert_eq!(c.stalled(500), vec![200], "at the boundary: not stalled");
    }

    /// `newest_agreed(at_most)` is inclusive: a set AT `at_most` answers.
    #[test]
    fn newest_agreed_is_inclusive_at_its_bound() {
        let sets = [agreed(1000), agreed(2000)];
        let soft = SoftTable::default();
        let c = q(&sets, &soft);
        assert_eq!(c.newest_agreed(2000), Some(2000));
        assert_eq!(c.newest_agreed(1999), Some(1000));
        assert_eq!(c.newest_agreed(1000), Some(1000));
    }

    /// `coverage_gaps`' journal escape: a span nobody holds the newer set of
    /// is still NOT a gap while some live node's journal covers it.
    #[test]
    fn coverage_gaps_escape_through_a_covering_journal() {
        let sets = [agreed(1000), agreed(2000)];
        let mut soft = SoftTable::default();
        soft.record(1, holding(42, 0, 1000, 2000), 9_900); // holds nothing, journal [1000, 2000]
        let c = q(&sets, &soft);
        assert_eq!(c.holders(2000), Vec::<NodeId>::new());
        assert_eq!(c.coverage_gaps(), Vec::<(u64, u64)>::new());
        // One byte short on either end and the gap reappears.
        let mut soft = SoftTable::default();
        soft.record(1, holding(42, 0, 1001, 2000), 9_900);
        let c = q(&sets, &soft);
        assert_eq!(c.coverage_gaps(), vec![(1000, 2000)]);
    }

    /// Plan ruling P1: known holders (live soft entries, then builders) first,
    /// learners first then lowest id; then every other member the same way;
    /// never self; no duplicates.
    #[test]
    fn fetch_candidates_put_known_holders_first_learners_first_then_everyone_else() {
        // self = 1; learners 4, 5; voters 0..=3; live holder 3; builders 4 and self.
        let c = fetch_candidates(1, &[3], &[4, 1], &[5, 4], &[0, 1, 2, 3]);
        assert_eq!(c, vec![4, 3, 5, 0, 2]);
        assert_eq!(
            fetch_candidates(0, &[], &[], &[], &[0]),
            Vec::<NodeId>::new(),
            "a solo node has nobody"
        );
    }

    /// Plan ruling P1: the builders of the set at P are the reporters whose
    /// hash matches the catalog's row hash at P.
    #[test]
    fn builders_are_the_reporters_of_the_catalogued_hash() {
        let mut e = agreed(1000);
        e.rows[0].hash = 7;
        let reports = [SnapshotReport {
            row: 0,
            position: 1000,
            hashes: vec![(0, 7, 1), (2, 8, 1), (3, 7, 1)],
        }];
        assert_eq!(builders_at(&reports, &[e.clone()], 1000), vec![0, 3]);
        assert_eq!(
            builders_at(&reports, &[e], 2000),
            Vec::<NodeId>::new(),
            "no entry at 2000"
        );
    }

    /// Review focus 5: a set reported by exactly one node is visible as such.
    #[test]
    fn reporters_at_counts_the_reporters_of_one_instant() {
        let reports = [
            SnapshotReport {
                row: 0,
                position: 1000,
                hashes: vec![(4, 7, 1)],
            },
            SnapshotReport {
                row: 1,
                position: 900,
                hashes: vec![(0, 7, 1), (1, 7, 1)],
            },
        ];
        assert_eq!(reporters_at(&reports, 1000), 1);
        assert_eq!(reporters_at(&reports, 900), 2);
        assert_eq!(reporters_at(&reports, 5), 0);
    }
}
