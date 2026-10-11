//! Tier retention (spec §7): which sets and journal chunks a writer may
//! delete from the tier under the replicated policy
//! (`Settings::{backup_retain_sets, backup_retain_age_secs}`), what an
//! incomplete upgrade pin protects, and the deletion order.
//!
//! **What retention can never move.** The tier's `through` (the chain end,
//! [`super::chain::watermark_of`]) equals the reach of the NEWEST complete
//! set: any set whose reach passes the newest set's position has a chain
//! that covers it, and the greedy step from a covered point reaches the end
//! of the covered run. Retention never deletes the newest set, and deletes
//! only chunks whose `to` lies at or below the OLDEST kept set — none of
//! which the newest set's chain steps through (a step from cursor `c` uses
//! a chunk with `c < to`). So the reported `through`, and the writer's
//! `progress` behind the node's purge cap, never move backward.
//!
//! **Order (spec §7.2).** A set's `SET` label goes first, so the set stops
//! counting at once (a listing without the label reads it as incomplete,
//! never as a start); then its objects; then its directory; then the
//! chunks. Every step is idempotent — a missing file is `Ok` — so any
//! writer may run it, twice, or after a crash in the middle.
use super::TierError;
use super::layout::{SET_FILE, SetLabel, chunk_object, set_dir, set_label};
use super::target::BackupTarget;
use crate::cluster_fsm::{ClusterView, ClusterViewInner};
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

/// The replicated retention policy; `0` = no limit on that axis, both `0`
/// = keep everything.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetentionPolicy {
    pub keep_sets: u16,
    pub keep_age_secs: u64,
}

impl RetentionPolicy {
    /// The committed policy as the cluster FSM last published it.
    pub fn from_view(v: &ClusterView) -> RetentionPolicy {
        RetentionPolicy {
            keep_sets: v.backup_retain_sets.load(Ordering::Acquire),
            keep_age_secs: v.backup_retain_age_secs.load(Ordering::Acquire),
        }
    }

    /// Spec §7.2 rule 1: with both limits `0`, nothing is ever deletable.
    pub fn keeps_all(&self) -> bool {
        self.keep_sets == 0 && self.keep_age_secs == 0
    }
}

/// Positions of protected sets and the protected journal spans [from, to).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Protection {
    pub sets: Vec<u64>,
    pub spans: Vec<(u64, u64)>,
}

/// Spec §7.2 rule 3: for every row whose newest pin is incomplete (ruling
/// C1, [`crate::catalog::pin_complete`]) protect the pin's origin set, the
/// rollback set — the newest tier set strictly below the pin's record
/// position — and the journal span `[rollback set, record position)`.
/// `tier_sets` are the tier's complete set positions. A row with no
/// running record reads `record_pos = u64::MAX` (the pin never completes):
/// its rollback set is then the newest tier set.
pub fn protection(inner: &ClusterViewInner, tier_sets: &[u64]) -> Protection {
    let rows: BTreeSet<u8> = inner.pins.iter().map(|p| p.row).collect();
    let mut prot = Protection::default();
    for row in rows {
        let Some(gate) = crate::catalog::pin_gate(&inner.pins, &inner.running, row) else {
            continue;
        };
        if crate::catalog::pin_complete(&inner.catalog, row, &gate) {
            continue;
        }
        prot.sets.push(gate.origin);
        if let Some(r) = tier_sets
            .iter()
            .copied()
            .filter(|&p| p < gate.record_pos)
            .max()
        {
            prot.sets.push(r);
            prot.spans.push((r, gate.record_pos));
        }
    }
    prot.sets.sort_unstable();
    prot.sets.dedup();
    prot
}

/// Spec §7.2: the complete sets (`sets`, any order) retention may delete,
/// ascending. A set is deletable iff a limit is set, it is not the newest
/// set, it is not protected, and it lies outside EVERY non-zero limit — not
/// among the `keep_sets` newest, and older than `keep_age_secs` by log time
/// against the newest set's log time (the tier's own clock, never the wall
/// clock). The two limits keep the UNION of what each keeps.
pub fn deletable_sets(sets: &[SetLabel], p: &RetentionPolicy, prot: &Protection) -> Vec<u64> {
    if p.keeps_all() || sets.is_empty() {
        return Vec::new();
    }
    let mut sorted: Vec<&SetLabel> = sets.iter().collect();
    sorted.sort_by_key(|s| s.position);
    let n = sorted.len();
    let newest = sorted[n - 1];
    let max_age_ns = p.keep_age_secs.saturating_mul(1_000_000_000);
    sorted[..n - 1]
        .iter()
        .enumerate()
        .filter(|&(i, s)| {
            let outside_count = p.keep_sets == 0 || i < n.saturating_sub(p.keep_sets as usize);
            let outside_age =
                p.keep_age_secs == 0 || newest.time_ns.saturating_sub(s.time_ns) > max_age_ns;
            outside_count && outside_age && !prot.sets.contains(&s.position)
        })
        .map(|(_, s)| s.position)
        .collect()
}

/// Spec §7.2: the chunks retention may delete — those lying WHOLLY at or
/// below `oldest_kept` (`to <= oldest_kept`; a chunk straddling it is kept,
/// ruling PF1 allows overlaps) and overlapping no protected span.
pub fn deletable_chunks(
    chunks: &[(u64, u64)],
    oldest_kept: u64,
    prot: &Protection,
) -> Vec<(u64, u64)> {
    chunks
        .iter()
        .copied()
        .filter(|&(from, to)| {
            to <= oldest_kept && !prot.spans.iter().any(|&(a, b)| from < b && to > a)
        })
        .collect()
}

/// What a retention pass deleted — counted as it goes, so a pass that
/// fails midway still reports what it already removed. A set counts once
/// its `SET` label is gone (it stops counting in the tier at that moment).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Deleted {
    pub sets: u64,
    pub chunks: u64,
    /// Incomplete set directories cleared (crash residue).
    pub incomplete: u64,
    /// The lowest complete set deleted (`0` = none).
    pub oldest_set: u64,
}

impl Deleted {
    pub fn any(&self) -> bool {
        self.sets + self.chunks + self.incomplete > 0
    }
}

/// Remove one set directory: `SET` first, then the rest
/// ([`remove_set_rest`]). Missing files are `Ok`.
fn remove_set(t: &dyn BackupTarget, p: u64) -> Result<(), TierError> {
    t.delete(&set_label(p))?;
    remove_set_rest(t, p)
}

/// Every entry of a set directory but `SET`, then the directory (if empty).
fn remove_set_rest(t: &dyn BackupTarget, p: u64) -> Result<(), TierError> {
    let dir = set_dir(p);
    for name in t.list(&dir)? {
        if name != SET_FILE {
            t.delete(&format!("{dir}/{name}"))?;
        }
    }
    t.delete_dir_if_empty(&dir)
}

/// [`apply`], counting into `done` as each step lands.
pub fn apply_counted(
    t: &dyn BackupTarget,
    sets: &[u64],
    chunks: &[(u64, u64)],
    done: &mut Deleted,
) -> Result<(), TierError> {
    for &p in sets {
        t.delete(&set_label(p))?;
        done.sets += 1;
        if done.oldest_set == 0 || p < done.oldest_set {
            done.oldest_set = p;
        }
        remove_set_rest(t, p)?;
    }
    for &(from, to) in chunks {
        t.delete(&chunk_object(from, to))?;
        done.chunks += 1;
    }
    Ok(())
}

/// Delete `sets` then `chunks`, in the spec §7.2 order: per set its `SET`
/// label first, then every other object in its directory, then the
/// directory itself (if empty); the chunks last. Idempotent.
pub fn apply(t: &dyn BackupTarget, sets: &[u64], chunks: &[(u64, u64)]) -> Result<(), TierError> {
    apply_counted(t, sets, chunks, &mut Deleted::default())
}

/// Ruling R10a: plan retention only once this node's committed cluster view
/// has walked (`ClusterView::consumed`) at least to the newest COMPLETE set
/// in the tier. A lagging view may not yet hold the pin (or the settings)
/// that protects a set another writer already copied; skip the pass until
/// it catches up. An empty tier is trivially caught up.
pub fn view_caught_up(consumed: u64, tier_sets: &[SetLabel]) -> bool {
    tier_sets.iter().all(|s| s.position <= consumed)
}

/// Spec §7: would retention delete `cand` again the moment it landed in
/// the tier? The policy and pin protection over the tier's complete sets
/// plus `cand`. The writer never copies such a set — whether it lies below
/// the oldest set or above a protected one — so a deleted set does not come
/// straight back; a set the policy would KEEP is still (back)filled.
pub fn recopy_blocked(
    tier: &[SetLabel],
    cand: &SetLabel,
    p: &RetentionPolicy,
    inner: &ClusterViewInner,
) -> bool {
    if p.keeps_all() {
        return false;
    }
    let mut sets: Vec<SetLabel> = tier
        .iter()
        .filter(|s| s.position != cand.position)
        .cloned()
        .collect();
    sets.push(cand.clone());
    let positions: Vec<u64> = sets.iter().map(|s| s.position).collect();
    deletable_sets(&sets, p, &protection(inner, &positions)).contains(&cand.position)
}

/// Incomplete set directories (no `SET`) strictly below the oldest kept
/// complete set — a deletion interrupted between its `SET` and its
/// directory, never a start. `None` (no complete set) = none.
pub fn stale_incomplete(incomplete: &[u64], oldest_kept: Option<u64>) -> Vec<u64> {
    let Some(o) = oldest_kept else {
        return Vec::new();
    };
    incomplete.iter().copied().filter(|&p| p < o).collect()
}

/// One retention pass over the tier (spec §7.2): list it, skip unless the
/// view has caught up (R10a), then delete the policy's sets (`SET` first),
/// the aged incomplete set directories below the oldest kept set (older
/// than `incomplete_max_age` by [`BackupTarget::newest_entry_age`], so a
/// lagging writer's in-flight copy is never raced) and the chunks wholly
/// below the oldest kept set. `Ok(false)` = skipped for R10a.
pub fn run_pass(
    t: &dyn BackupTarget,
    p: &RetentionPolicy,
    inner: &ClusterViewInner,
    consumed: u64,
    incomplete_max_age: std::time::Duration,
    done: &mut Deleted,
) -> Result<bool, TierError> {
    let l = super::chain::list_tier(t)?;
    if !view_caught_up(consumed, &l.sets) {
        return Ok(false);
    }
    let positions: Vec<u64> = l.sets.iter().map(|s| s.position).collect();
    let prot = protection(inner, &positions);
    let (sets, chunks) = plan(&l.sets, &l.chunks, p, &prot);
    let oldest_kept = positions
        .iter()
        .copied()
        .filter(|q| !sets.contains(q))
        .min();
    let mut residue = Vec::new();
    for q in stale_incomplete(&l.incomplete_sets, oldest_kept) {
        if t.newest_entry_age(&set_dir(q))?
            .is_some_and(|age| age >= incomplete_max_age)
        {
            residue.push(q);
        }
    }
    apply_counted(t, &sets, &[], done)?;
    for q in residue {
        remove_set(t, q)?;
        done.incomplete += 1;
    }
    apply_counted(t, &[], &chunks, done)?;
    Ok(true)
}

/// One retention pass's decision over a listing: the sets and chunks to
/// delete. Empty when the policy keeps everything.
pub fn plan(
    sets: &[SetLabel],
    chunks: &[(u64, u64)],
    p: &RetentionPolicy,
    prot: &Protection,
) -> (Vec<u64>, Vec<(u64, u64)>) {
    if p.keeps_all() {
        return (Vec::new(), Vec::new());
    }
    let del = deletable_sets(sets, p, prot);
    let oldest_kept = sets
        .iter()
        .map(|s| s.position)
        .filter(|q| !del.contains(q))
        .min();
    let chunks = oldest_kept.map_or_else(Vec::new, |o| deletable_chunks(chunks, o, prot));
    (del, chunks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_fsm::ClusterState;
    use std::fs::File;
    use std::sync::Mutex;
    use uc_protocol::identity::pack_version;
    use uc_protocol::v2::catalog::{RowEntry, RowVerdict, SetEntry, SetKind, SetState};
    use uc_protocol::v2::upgrade::{RowRunning, UpgradePin};

    const S: u64 = 1_000_000_000;

    fn label(position: u64, time_secs: u64) -> SetLabel {
        SetLabel {
            position,
            kind: SetKind::Full,
            time_ns: time_secs * S,
            rows: Vec::new(),
            cluster_hash: 1,
            cluster_size: 1,
        }
    }

    /// Sets at 100, 200, … 500, one log-second apart (t = 1..=5).
    fn five() -> Vec<SetLabel> {
        (1..=5).map(|i| label(i * 100, i)).collect()
    }

    fn pol(keep_sets: u16, keep_age_secs: u64) -> RetentionPolicy {
        RetentionPolicy {
            keep_sets,
            keep_age_secs,
        }
    }

    fn none() -> Protection {
        Protection::default()
    }

    #[test]
    fn defaults_keep_all() {
        assert!(deletable_sets(&five(), &pol(0, 0), &none()).is_empty());
        let (s, c) = plan(&five(), &[(0, 100), (100, 200)], &pol(0, 0), &none());
        assert!(s.is_empty() && c.is_empty(), "both 0 deletes nothing");
    }

    #[test]
    fn count_only() {
        assert_eq!(
            deletable_sets(&five(), &pol(2, 0), &none()),
            vec![100, 200, 300]
        );
        assert_eq!(deletable_sets(&five(), &pol(5, 0), &none()), vec![]);
        assert_eq!(deletable_sets(&five(), &pol(9, 0), &none()), vec![]);
    }

    #[test]
    fn age_only() {
        // newest at t=5; a set strictly MORE than 2 s older goes: t=1, t=2.
        assert_eq!(deletable_sets(&five(), &pol(0, 2), &none()), vec![100, 200]);
        // the input order does not matter
        let mut rev = five();
        rev.reverse();
        assert_eq!(deletable_sets(&rev, &pol(0, 2), &none()), vec![100, 200]);
    }

    #[test]
    fn both_limits_keep_the_union() {
        // count keeps {400, 500}; age (1 s) keeps {400, 500}; union = same.
        assert_eq!(
            deletable_sets(&five(), &pol(2, 1), &none()),
            vec![100, 200, 300]
        );
        // count keeps {500}; age (3 s) keeps {200..500}: union {200..500}.
        assert_eq!(deletable_sets(&five(), &pol(1, 3), &none()), vec![100]);
        // count keeps {300, 400, 500}; age (1 s) keeps {400, 500}: union {300..}.
        assert_eq!(deletable_sets(&five(), &pol(3, 1), &none()), vec![100, 200]);
    }

    #[test]
    fn the_newest_is_never_deleted() {
        // keep_sets = 0 + a tiny age: every older set goes, never the newest.
        let sets = [label(100, 1), label(200, 1_000)];
        assert_eq!(deletable_sets(&sets, &pol(0, 1), &none()), vec![100]);
        // a lone set survives any policy
        assert!(deletable_sets(&[label(7, 0)], &pol(0, 1), &none()).is_empty());
        // even "protected" bookkeeping aside, the newest is not in the list
        let (s, _) = plan(&five(), &[], &pol(1, 0), &none());
        assert!(!s.contains(&500));
    }

    #[test]
    fn a_protected_set_is_kept() {
        let prot = Protection {
            sets: vec![200],
            spans: vec![],
        };
        assert_eq!(
            deletable_sets(&five(), &pol(1, 0), &prot),
            vec![100, 300, 400]
        );
    }

    const TO: u32 = pack_version(2, 0, 0);
    const ROW: u8 = 0;

    fn inner_with_pin(origin: u64, record_pos: u64) -> ClusterViewInner {
        let mut inner = ClusterView::new(&ClusterState::genesis_empty()).snapshot_inner();
        inner.pins = vec![UpgradePin {
            row: ROW,
            from: pack_version(1, 0, 0),
            to: TO,
            origin,
        }];
        inner.running[ROW as usize] = Some(RowRunning {
            row: ROW,
            version: TO,
            record_pos,
        });
        inner
    }

    fn agreed_on(p: u64, version: u32) -> SetEntry {
        let ok = RowEntry {
            version,
            hash: 1,
            verdict: RowVerdict::Agreed,
            size: 0,
        };
        let mut e = SetEntry::commanded(p, SetKind::Full, p);
        e.rows[ROW as usize] = ok;
        e.cluster = ok;
        e.state = SetState::Complete;
        e
    }

    /// Spec §7.2 rule 3: an incomplete pin protects its origin, the newest
    /// tier set strictly below its record and the journal between them;
    /// a completion set (agreed on `to`'s line, above the record) lifts it.
    #[test]
    fn pin_protection_holds_until_completion_then_lifts() {
        // origin 200, the pin's record at 350; tier sets 100..500.
        let mut inner = inner_with_pin(200, 350);
        inner.catalog = vec![agreed_on(200, pack_version(1, 0, 0))];
        let tier: Vec<u64> = five().iter().map(|s| s.position).collect();
        let prot = protection(&inner, &tier);
        assert_eq!(prot.sets, vec![200, 300], "origin + rollback set");
        assert_eq!(prot.spans, vec![(300, 350)]);
        // keep_sets = 1 would delete 100..400; the pin keeps 200 and 300.
        assert_eq!(deletable_sets(&five(), &pol(1, 0), &prot), vec![100, 400]);

        // A set on the OLD line above the record does not complete it…
        inner.catalog.push(agreed_on(400, pack_version(1, 0, 0)));
        assert_eq!(protection(&inner, &tier).sets, vec![200, 300]);
        // …nor does one on `to`'s line BELOW the record…
        inner.catalog.push(agreed_on(300, TO));
        assert_eq!(protection(&inner, &tier).sets, vec![200, 300]);
        // …an agreed set on `to`'s line above the record does: lifted.
        inner.catalog.push(agreed_on(500, TO));
        assert_eq!(protection(&inner, &tier), Protection::default());
    }

    /// A row with no running record never completes: the rollback set is
    /// the newest tier set and the span runs to the end.
    #[test]
    fn a_pin_without_a_running_record_protects_the_newest_and_on() {
        let mut inner = inner_with_pin(200, 0);
        inner.running[ROW as usize] = None;
        let prot = protection(&inner, &[100, 200, 300]);
        assert_eq!(prot.sets, vec![200, 300]);
        assert_eq!(prot.spans, vec![(300, u64::MAX)]);
        // no pins at all: nothing protected
        let plain = ClusterView::new(&ClusterState::genesis_empty()).snapshot_inner();
        assert_eq!(protection(&plain, &[100]), Protection::default());
    }

    #[test]
    fn deletable_chunks_respects_spans_and_the_oldest_kept_set() {
        let chunks = [(0, 100), (100, 250), (150, 300), (250, 400), (300, 450)];
        // oldest kept = 300: (0,100), (100,250), (150,300) lie wholly at or
        // below it; (250,400) straddles it and is kept.
        assert_eq!(
            deletable_chunks(&chunks, 300, &none()),
            vec![(0, 100), (100, 250), (150, 300)]
        );
        // a protected span [200, 260) keeps every chunk overlapping it
        let prot = Protection {
            sets: vec![],
            spans: vec![(200, 260)],
        };
        assert_eq!(deletable_chunks(&chunks, 300, &prot), vec![(0, 100)]);
        // a span touching a chunk's end only (half-open) does not keep it
        let touch = Protection {
            sets: vec![],
            spans: vec![(100, 120)],
        };
        assert_eq!(
            deletable_chunks(&chunks, 300, &touch),
            vec![(0, 100), (150, 300)]
        );
        // nothing below the oldest kept set
        assert!(deletable_chunks(&chunks, 50, &none()).is_empty());
    }

    #[test]
    fn plan_deletes_chunks_only_below_the_oldest_kept_set() {
        let chunks = [(100, 250), (250, 400), (400, 520)];
        let (s, c) = plan(&five(), &chunks, &pol(2, 0), &none());
        assert_eq!(s, vec![100, 200, 300]);
        // oldest kept = 400: (100,250) and (250,400) go; (400,520) carries
        // the chain on.
        assert_eq!(c, vec![(100, 250), (250, 400)]);
        // the reach from the oldest kept set is untouched
        let left: Vec<(u64, u64)> = chunks.iter().copied().filter(|x| !c.contains(x)).collect();
        assert_eq!(
            crate::tier::chain::reach_from(&left, 400),
            crate::tier::chain::reach_from(&chunks, 400)
        );
    }

    /// Age-only: a set whose log time is NEWER than the newest set's (a
    /// clock oddity across leaders) reads as age 0 and is kept.
    #[test]
    fn a_set_newer_by_log_time_than_the_newest_is_kept() {
        let sets = [label(100, 1), label(200, 50), label(300, 10)];
        assert_eq!(deletable_sets(&sets, &pol(0, 5), &none()), vec![100]);
    }

    /// Ruling R10a: plan only once the view has walked to the tier's
    /// newest complete set.
    #[test]
    fn retention_waits_for_the_view_to_reach_the_newest_tier_set() {
        assert!(view_caught_up(0, &[]), "an empty tier is caught up");
        let sets = [label(100, 1), label(500, 2)];
        assert!(!view_caught_up(499, &sets));
        assert!(view_caught_up(500, &sets));
        assert!(view_caught_up(9_000, &sets));
    }

    /// Fix round 1 / IMPORTANT 1: the copy filter blocks a set retention
    /// would delete again — below the oldest set AND above a protected one
    /// — and lets through what the policy keeps.
    #[test]
    fn the_copy_filter_does_not_recopy_a_set_deleted_above_a_protected_one() {
        // The pin test's state after one pass with keep_sets = 1: 200
        // (origin) and 300 (rollback) protected, 100 and 400 deleted.
        let mut inner = inner_with_pin(200, 350);
        inner.catalog = vec![agreed_on(200, pack_version(1, 0, 0))];
        let tier = [label(200, 2), label(300, 3), label(500, 5)];
        let p = pol(1, 0);
        assert!(recopy_blocked(&tier, &label(400, 4), &p, &inner), "above");
        assert!(recopy_blocked(&tier, &label(100, 1), &p, &inner), "below");
        assert!(!recopy_blocked(&tier, &label(600, 6), &p, &inner), "newer");
        // keep-everything: nothing is blocked (backfill as before)
        assert!(!recopy_blocked(&tier, &label(400, 4), &pol(0, 0), &inner));
        // an age policy that KEEPS an older set lets it be backfilled
        let plain = ClusterView::new(&ClusterState::genesis_empty()).snapshot_inner();
        let t2 = [label(300, 3), label(500, 5)];
        assert!(!recopy_blocked(&t2, &label(100, 1), &pol(0, 10), &plain));
        assert!(recopy_blocked(&t2, &label(100, 1), &pol(0, 2), &plain));
    }

    /// A pin whose rollback set IS the oldest kept set, with chunks
    /// straddling it: the straddlers and the span stay, the chain from the
    /// rollback set is whole, and only chunks wholly below it go.
    #[test]
    fn a_pins_rollback_set_as_the_oldest_kept_keeps_its_straddling_chunks() {
        let inner = inner_with_pin(300, 350); // origin = rollback = 300
        let sets = [label(100, 1), label(200, 2), label(300, 3), label(400, 4)];
        let prot = protection(&inner, &[100, 200, 300, 400]);
        assert_eq!(prot.sets, vec![300]);
        assert_eq!(prot.spans, vec![(300, 350)]);
        let chunks = [
            (100, 200),
            (200, 300),
            (250, 300),
            (150, 320),
            (280, 310),
            (300, 420),
        ];
        let (del, gone) = plan(&sets, &chunks, &pol(1, 0), &prot);
        assert_eq!(del, vec![100, 200]);
        assert_eq!(gone, vec![(100, 200), (200, 300), (250, 300)]);
        let left: Vec<(u64, u64)> = chunks
            .iter()
            .copied()
            .filter(|c| !gone.contains(c))
            .collect();
        let reach = crate::tier::chain::reach_from;
        assert_eq!(reach(&left, 300), reach(&chunks, 300));
        assert_eq!(reach(&left, 400), reach(&chunks, 400));
    }

    /// A fake target that records every mutating call in order.
    struct Recording {
        log: Mutex<Vec<String>>,
        dirs: Vec<(String, Vec<String>)>,
    }

    impl BackupTarget for Recording {
        fn put_if_absent(
            &self,
            _: &str,
            _: &mut dyn FnMut(&mut File) -> Result<u64, TierError>,
        ) -> Result<crate::tier::target::Put, TierError> {
            unreachable!("retention never writes")
        }
        fn list(&self, dir: &str) -> Result<Vec<String>, TierError> {
            Ok(self
                .dirs
                .iter()
                .find(|(d, _)| d == dir)
                .map(|(_, v)| v.clone())
                .unwrap_or_default())
        }
        fn open(&self, _: &str) -> Result<Option<File>, TierError> {
            Ok(None)
        }
        fn delete(&self, path: &str) -> Result<(), TierError> {
            self.log.lock().unwrap().push(format!("rm {path}"));
            Ok(())
        }
        fn delete_dir_if_empty(&self, dir: &str) -> Result<(), TierError> {
            self.log.lock().unwrap().push(format!("rmdir {dir}"));
            Ok(())
        }
    }

    #[test]
    fn apply_removes_set_first_then_files_dir_then_chunks() {
        let d = set_dir(100);
        let t = Recording {
            log: Mutex::new(Vec::new()),
            // a listing that still shows SET (eventually-consistent store)
            dirs: vec![(
                d.clone(),
                vec![
                    "SET".into(),
                    "cluster.ultcluster".into(),
                    "row-0.ultsnap".into(),
                ],
            )],
        };
        apply(&t, &[100], &[(0, 100)]).unwrap();
        assert_eq!(
            *t.log.lock().unwrap(),
            vec![
                format!("rm {}", set_label(100)),
                format!("rm {d}/cluster.ultcluster"),
                format!("rm {d}/row-0.ultsnap"),
                format!("rmdir {d}"),
                format!("rm {}", chunk_object(0, 100)),
            ]
        );
    }

    /// A target whose deletes start failing after `ok` of them.
    struct Failing {
        ok: Mutex<usize>,
    }

    impl BackupTarget for Failing {
        fn put_if_absent(
            &self,
            _: &str,
            _: &mut dyn FnMut(&mut File) -> Result<u64, TierError>,
        ) -> Result<crate::tier::target::Put, TierError> {
            unreachable!()
        }
        fn list(&self, _: &str) -> Result<Vec<String>, TierError> {
            Ok(Vec::new())
        }
        fn open(&self, _: &str) -> Result<Option<File>, TierError> {
            Ok(None)
        }
        fn delete(&self, _: &str) -> Result<(), TierError> {
            let mut ok = self.ok.lock().unwrap();
            if *ok == 0 {
                return Err(TierError::Refused("target down".into()));
            }
            *ok -= 1;
            Ok(())
        }
        fn delete_dir_if_empty(&self, _: &str) -> Result<(), TierError> {
            Ok(())
        }
    }

    /// A pass that fails midway still counts what it already deleted.
    #[test]
    fn a_failed_apply_counts_the_deletions_already_made() {
        // both SET deletes land, then one chunk, then the second fails.
        let t = Failing { ok: Mutex::new(3) };
        let mut done = Deleted::default();
        let e = apply_counted(&t, &[100, 200], &[(0, 100), (100, 200)], &mut done);
        assert!(e.is_err());
        assert_eq!(
            done,
            Deleted {
                sets: 2,
                chunks: 1,
                incomplete: 0,
                oldest_set: 100
            }
        );
    }

    fn scratch() -> tempfile::TempDir {
        let root = std::env::var("CARGO_TARGET_TMPDIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_node_tests")
            });
        std::fs::create_dir_all(&root).unwrap();
        tempfile::Builder::new()
            .prefix("uc2-tier-")
            .tempdir_in(&root)
            .unwrap()
    }

    /// Age every entry of a set directory, then the directory, by `by`.
    fn age_dir(dir: &std::path::Path, by: std::time::Duration) {
        let when = std::time::SystemTime::now() - by;
        for ent in std::fs::read_dir(dir).unwrap() {
            File::options()
                .write(true)
                .open(ent.unwrap().path())
                .unwrap()
                .set_modified(when)
                .unwrap();
        }
        File::open(dir).unwrap().set_modified(when).unwrap();
    }

    /// Fix round 1 / IMPORTANT 2: a set directory whose `SET` is gone (a
    /// deletion interrupted midway) below the oldest kept set is cleared by
    /// one pass once it is old; a fresh one, or one above the oldest kept
    /// set, is left alone.
    #[test]
    fn a_pass_clears_aged_incomplete_set_dirs_below_the_oldest_kept_set() {
        let dir = scratch();
        let t = crate::tier::target::FsTarget::new(dir.path());
        let hour = std::time::Duration::from_secs(3600);
        for p in [300u64, 400] {
            let sd = dir.path().join(set_dir(p));
            std::fs::create_dir_all(&sd).unwrap();
            std::fs::write(sd.join("SET"), label(p, p).render()).unwrap();
        }
        for p in [100u64, 200, 500] {
            let sd = dir.path().join(set_dir(p));
            std::fs::create_dir_all(&sd).unwrap();
            std::fs::write(sd.join("row-0.ultsnap"), b"x").unwrap();
        }
        age_dir(&dir.path().join(set_dir(100)), hour * 2);
        age_dir(&dir.path().join(set_dir(500)), hour * 2);
        let inner = ClusterView::new(&ClusterState::genesis_empty()).snapshot_inner();
        let mut done = Deleted::default();
        // keep-everything still clears residue
        assert!(run_pass(&t, &pol(0, 0), &inner, u64::MAX, hour, &mut done).unwrap());
        assert_eq!(done.incomplete, 1);
        assert_eq!((done.sets, done.chunks), (0, 0));
        assert!(
            !dir.path().join(set_dir(100)).exists(),
            "aged + below: cleared"
        );
        assert!(dir.path().join(set_dir(200)).exists(), "fresh: kept");
        assert!(
            dir.path().join(set_dir(500)).exists(),
            "above oldest kept: kept"
        );
        let l = crate::tier::chain::list_tier(&t).unwrap();
        assert_eq!(l.incomplete_sets, vec![200, 500]);
    }

    /// R10a through the pass: a view short of the newest tier set skips.
    #[test]
    fn a_pass_is_skipped_while_the_view_lags_the_tier() {
        let dir = scratch();
        let t = crate::tier::target::FsTarget::new(dir.path());
        for p in [100u64, 200] {
            let sd = dir.path().join(set_dir(p));
            std::fs::create_dir_all(&sd).unwrap();
            std::fs::write(sd.join("SET"), label(p, p).render()).unwrap();
        }
        let inner = ClusterView::new(&ClusterState::genesis_empty()).snapshot_inner();
        let mut done = Deleted::default();
        let hour = std::time::Duration::from_secs(3600);
        assert!(!run_pass(&t, &pol(1, 0), &inner, 199, hour, &mut done).unwrap());
        assert!(!done.any());
        assert!(run_pass(&t, &pol(1, 0), &inner, 200, hour, &mut done).unwrap());
        assert_eq!(done.sets, 1);
        assert!(!dir.path().join(set_dir(100)).exists());
    }

    /// On a real filesystem target: idempotent, a missing file is `Ok`, and
    /// the listing no longer counts the set once its `SET` is gone.
    #[test]
    fn apply_is_idempotent_on_a_filesystem_target() {
        let root = std::env::var("CARGO_TARGET_TMPDIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_node_tests")
            });
        std::fs::create_dir_all(&root).unwrap();
        let dir = tempfile::Builder::new()
            .prefix("uc2-tier-")
            .tempdir_in(&root)
            .unwrap();
        let t = crate::tier::target::FsTarget::new(dir.path());
        for p in [100u64, 200] {
            let sd = dir.path().join(set_dir(p));
            std::fs::create_dir_all(&sd).unwrap();
            std::fs::write(sd.join("row-0.ultsnap"), b"x").unwrap();
            std::fs::write(sd.join("SET"), label(p, p).render()).unwrap();
        }
        std::fs::create_dir_all(dir.path().join("journal")).unwrap();
        std::fs::write(dir.path().join(chunk_object(100, 200)), b"c").unwrap();
        apply(&t, &[100], &[(100, 200)]).unwrap();
        apply(&t, &[100], &[(100, 200)]).unwrap();
        assert!(!dir.path().join(set_dir(100)).exists());
        assert!(!dir.path().join(chunk_object(100, 200)).exists());
        let l = crate::tier::chain::list_tier(&t).unwrap();
        assert_eq!(l.sets.iter().map(|s| s.position).collect::<Vec<_>>(), [200]);
        assert!(l.incomplete_sets.is_empty());
    }
}
