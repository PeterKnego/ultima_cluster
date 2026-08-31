// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Term-map reconciliation (spec §M4): the pure, exhaustively-testable core of
//! log truncation. A follower that receives the leader's term map compares it
//! against its own history and decides, byte-for-byte, how far its local log is
//! still valid.
//!
//! A **term map** is an ascending `Vec<(term, base)>`: term `t`'s bytes begin at
//! byte position `base`. Positions are globally monotone across the cluster, so
//! two maps that share an entry share every byte below that entry's successor.
//!
//! ## Data-stamped term recording (the safety contract)
//!
//! Our own map records a term **only when we actually accepted that term's
//! data** — never by adopting an entry the leader gossiped. Concretely, an entry
//! `(t, b)` appears in our own map iff either we opened term `t` ourselves as
//! leader (base = our durable at the time), or we streamed and durably wrote
//! bytes stamped term `t` starting at `b` (the `DataTermObserved` path in the
//! election SM). Gossip never grows our map.
//!
//! This is what makes truncation sound. Consider a healed ex-leader whose own
//! map is `[(1,0)]` at durable 3000 (it wrote bytes `[2000,3000)` under its own
//! failed term but only ever stamped them as term 1). The real cluster opened
//! term 2 at position 2000. The leader ships `[(1,0),(2,2000)]`. Because our own
//! map *lacks* a term-2 stamp below our durable, those `[2000,3000)` bytes are
//! provably ours-and-divergent, not the term-2 leader's — so we truncate to
//! 2000. Had we adopted the gossiped `(2,2000)` (the old, unsafe rule), we would
//! have recorded our stale divergent bytes as the term-2 leader's data — a
//! split-history that could then be served or used as vote credentials. A leader
//! entry we lack below our durable therefore **proves** divergence.
//!
//! ## [`reconcile`] rule (pure over `(own, own_durable, leader)`)
//!
//! - Find `k`, the length of the longest common prefix of the two maps.
//! - `valid_up_to = own_durable`, clamped down to:
//!   - `own[k].base` when `k < own.len()` — our first entry beyond the common
//!     prefix is a history the leader never certified (a conflicting term or an
//!     overhang past a shorter leader map);
//!   - `leader[k].base` when `k < leader.len()` **and** `leader[k].base <
//!     own_durable` — a term the leader certified below our durable that our own
//!     data-stamped map lacks, i.e. proven divergence.
//! - `new_map` = **our own surviving entries only** (own entries with
//!   `base < valid_up_to`). The leader's entries are never adopted here; our map
//!   grows solely through our own leadership or `DataTermObserved`.
//!
//! The caller (the election SM) derives the action from the [`Outcome`]:
//! `valid_up_to < own_durable` ⇒ truncate; else if `new_map` shrank/changed ⇒
//! persist. [`Reconcile::NoCommonPrefix`] means the leader's shipped suffix
//! begins strictly beyond our entire history — incremental reconciliation is
//! impossible and M6's snapshot install is the real answer; M4 surfaces it
//! loudly (the sim asserts it is unreachable at `<= MAX_TERM_MAP_WIRE_ENTRIES`
//! terms).

/// Cap on the number of term-map entries shipped on the wire (the leader
/// piggybacks the last `MAX_TERM_MAP_WIRE_ENTRIES` on its commit-gossip
/// cadence). A follower whose divergence predates this window falls off the
/// incremental path and needs a snapshot (M6) — surfaced as
/// [`Reconcile::NoCommonPrefix`].
pub const MAX_TERM_MAP_WIRE_ENTRIES: usize = 64;

/// The result of comparing our term map against the leader's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// The byte position up to which our local log is still valid. Truncation
    /// is required iff `valid_up_to < own_durable`.
    pub valid_up_to: u64,
    /// The reconciled term map: our own surviving entries (base strictly below
    /// `valid_up_to`). Never contains an adopted leader entry.
    pub new_map: Vec<(u32, u64)>,
}

/// Outcome of [`reconcile`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconcile {
    /// Histories share a prefix. See [`Outcome`] for the surviving bound and the
    /// reconciled map.
    Ok(Outcome),
    /// No common entry — the leader's shipped window begins strictly beyond
    /// our whole byte range (`leader[0].base > own_durable`). Incremental
    /// reconciliation is impossible; a snapshot install (M6) is the answer.
    /// Since the 2026-08-16 alignment fix this is the GENUINE purged-prefix
    /// signal only: a window that merely slid past our map's FRONT (lifetime
    /// terms > `MAX_TERM_MAP_WIRE_ENTRIES`) aligns inside our history and
    /// reconciles normally.
    NoCommonPrefix,
}

/// Compare our `own` term map (bounded by `own_durable`) against the `leader`'s.
///
/// Both maps are ascending `(term, base)` slices. See the module docs for the
/// rules; the unit tests are the binding contract.
pub fn reconcile(own: &[(u32, u64)], own_durable: u64, leader: &[(u32, u64)]) -> Reconcile {
    // A leader with no map tells us nothing — our history is clean as-is.
    if leader.is_empty() {
        return Reconcile::Ok(Outcome {
            valid_up_to: own_durable,
            new_map: own.to_vec(),
        });
    }

    // ALIGN the leader's shipped window inside our (full) map before prefix
    // matching. The leader ships only the last `MAX_TERM_MAP_WIRE_ENTRIES`
    // entries (`term_map_wire_tail`), so once a cluster's LIFETIME leadership
    // count exceeds the window, `leader[0]` is not our entry 0 — it is some
    // entry `j` in the middle of our map. The 2026-08-16 acked-write-loss
    // hunt found the previous index-aligned match (`own[k] == leader[k]` from
    // k = 0) declaring `NoCommonPrefix` against every HEALTHY follower the
    // moment the window slid (own[0]=(1,0) vs leader[0]=(term_N, base_N)),
    // wiping followers in a loop, starving elections of durable credentials,
    // and ultimately truncating committed bytes cluster-wide. Terms are
    // strictly ascending in both maps, so the alignment point is unique.
    let j = match own.iter().position(|&e| e == leader[0]) {
        Some(j) => j,
        None => {
            // The leader's window start is not in our history.
            if !own.is_empty() && leader[0].1 > own_durable {
                // Window begins beyond our whole byte range: it has slid past
                // our history (the genuine purged-prefix case) — incremental
                // repair is impossible.
                return Reconcile::NoCommonPrefix;
            }
            if own.is_empty() {
                // Fresh node: nothing of ours to invalidate; our map grows
                // only when we actually stream data.
                return Reconcile::Ok(Outcome {
                    valid_up_to: own_durable,
                    new_map: Vec::new(),
                });
            }
            // The window starts INSIDE our byte range but our data-stamped map
            // never observed that term there: the bytes we hold at/above the
            // window base belong to some other term — proven divergence.
            // Additionally clamp at our first entry claiming a term >= the
            // window's first term (a same-term/different-base conflict proves
            // divergence from that entry's base onward).
            let mut cut = own_durable.min(leader[0].1);
            if let Some(&(_, base)) = own.iter().find(|&&(t, _)| t >= leader[0].0) {
                cut = cut.min(base);
            }
            let mut new_map: Vec<(u32, u64)> = Vec::new();
            for &(term, base) in own {
                if base < cut {
                    new_map.push((term, base));
                }
            }
            return Reconcile::Ok(Outcome {
                valid_up_to: cut,
                new_map,
            });
        }
    };

    // Longest common run from the alignment point (entries equal in both term
    // and base). `k` counts matches within the WINDOW; the shared prefix in
    // own-map coordinates is `own[..j + k]` (entries below the window are our
    // honest observations of history the leader simply did not ship — absence
    // from the window is not contradiction).
    let mut k = 0;
    while j + k < own.len() && k < leader.len() && own[j + k] == leader[k] {
        k += 1;
    }

    // Our bytes are valid up to our durable, clamped down at the first point of
    // divergence beyond the common run:
    //   - our own first uncertified entry (conflict or overhang), and/or
    //   - a leader term below our durable that our data-stamped map LACKS
    //     (proven divergence — the bytes there are ours, not that term's).
    let mut valid_up_to = own_durable;
    if j + k < own.len() {
        valid_up_to = valid_up_to.min(own[j + k].1);
    }
    if k < leader.len() && leader[k].1 < own_durable {
        valid_up_to = valid_up_to.min(leader[k].1);
    }
    let k = j + k; // shared-prefix length in own-map coordinates, used below.

    // The reconciled map is our own surviving entries only — never an adopted
    // leader entry (reconcile never grows the map — that is `DataTermObserved`'s
    // job). The COMMON PREFIX is kept unconditionally: a legitimate zero-byte
    // frontier entry (base == durable) that the leader shares must survive.
    // Beyond the prefix, entries survive only below `valid_up_to`. In a CLEAN
    // outcome every beyond-prefix own entry has base >= durable (else the
    // own-side clamp above would have fired), i.e. it is a zero-byte PHANTOM
    // from a term the leader's history contradicts (e.g. we won a term,
    // persisted the map entry, and crashed before the NewTerm frame fsynced).
    // Keeping such a phantom is unsafe: once genuine data streams past its
    // base, the next reconcile's own-side clamp would spuriously truncate
    // genuine (possibly committed) bytes at the phantom's base.
    // (Loop form rather than an iterator chain: verifiability-friendly for the
    // Lean/Aeneas pipeline — see docs/superpowers/specs/2026-07-16-uc2-lean-proofs-design.md §6.)
    let mut new_map: Vec<(u32, u64)> = Vec::with_capacity(own.len());
    new_map.extend_from_slice(&own[..k]);
    for &(term, base) in &own[k..] {
        if base < valid_up_to {
            new_map.push((term, base));
        }
    }

    Reconcile::Ok(Outcome {
        valid_up_to,
        new_map,
    })
}

/// Mutation tooth (never in a default build): the PRE-2026-08-16 kernel,
/// verbatim from `2102584^` — an index-aligned prefix match from `k = 0` with
/// the old `NoCommonPrefix` rule (`leader[0].base > own[0].base`). Once a
/// cluster's lifetime term count exceeds `MAX_TERM_MAP_WIRE_ENTRIES`, the
/// leader's shipped window no longer starts at a healthy follower's entry 0,
/// so this declares `NoCommonPrefix` against every healthy follower — the
/// acked-write-loss bug the aligned [`reconcile`] fixed. Kept so the sim's
/// `window_slide_with_index_aligned_reconcile_wipes_healthy_followers` red
/// twin can prove the sim catches it; selected through
/// `ElectionSm::set_mutate_index_aligned_reconcile`.
#[cfg(feature = "mutation-testing")]
pub fn reconcile_index_aligned(
    own: &[(u32, u64)],
    own_durable: u64,
    leader: &[(u32, u64)],
) -> Reconcile {
    if leader.is_empty() {
        return Reconcile::Ok(Outcome {
            valid_up_to: own_durable,
            new_map: own.to_vec(),
        });
    }
    let mut k = 0;
    while k < own.len() && k < leader.len() && own[k] == leader[k] {
        k += 1;
    }
    if k == 0 && !own.is_empty() && leader[0].1 > own[0].1 {
        return Reconcile::NoCommonPrefix;
    }
    let mut valid_up_to = own_durable;
    if k < own.len() {
        valid_up_to = valid_up_to.min(own[k].1);
    }
    if k < leader.len() && leader[k].1 < own_durable {
        valid_up_to = valid_up_to.min(leader[k].1);
    }
    let mut new_map: Vec<(u32, u64)> = Vec::with_capacity(own.len());
    new_map.extend_from_slice(&own[..k]);
    for &(term, base) in &own[k..] {
        if base < valid_up_to {
            new_map.push((term, base));
        }
    }
    Reconcile::Ok(Outcome {
        valid_up_to,
        new_map,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_outcome_drops_beyond_prefix_phantom_frontier_entry() {
        // Deposed ex-leader that crashed AFTER persisting its term-2 map
        // entry but BEFORE the NewTerm frame fsynced: map [(1,0),(2,5000)],
        // durable exactly 5000 (zero term-2 bytes). Term 3 opened at 5000.
        // Reconcile is CLEAN (nothing to truncate: valid_up_to == durable)
        // but the phantom (2,5000) MUST be dropped — keeping it would make a
        // LATER reconcile (after genuine term-3 data streams past 5000)
        // spuriously truncate committed bytes at the phantom's base.
        match reconcile(&[(1, 0), (2, 5000)], 5000, &[(1, 0), (3, 5000)]) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, 5000); // clean — no truncation
                assert_eq!(o.new_map, vec![(1, 0)]); // phantom dropped
            }
            other => panic!("expected Ok, got {other:?}"),
        }
        // and the SHARED frontier entry (in the common prefix) survives:
        let m = [(1, 0), (2, 5000)];
        match reconcile(&m, 5000, &m) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, 5000);
                assert_eq!(o.new_map, m.to_vec()); // k == 2: kept unconditionally
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[test]
    fn identical_histories_are_clean() {
        // Same map on both sides: valid to our durable, map unchanged.
        let m = [(1, 0), (3, 4096)];
        match reconcile(&m, 8000, &m) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, 8000); // == own_durable ⇒ clean
                assert_eq!(o.new_map, vec![(1, 0), (3, 4096)]);
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[test]
    fn divergent_own_tail_truncates_at_own_divergent_base() {
        // common: (1,0). We opened term 2 at 4096 (a leader that never won
        // quorum); real history went term 3 at 4096. Our term-2 tail diverges.
        let own = [(1, 0), (2, 4096)];
        let leader = [(1, 0), (3, 4096)];
        match reconcile(&own, 6000, &leader) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, 4096); // < durable ⇒ truncate
                assert_eq!(o.new_map, vec![(1, 0)]);
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[test]
    fn own_overhang_beyond_leader_truncates_at_own_next_base() {
        // We opened term 2 at 5000 as a leader that never won quorum; the real
        // cluster never had term 2 (leader map stops at term 1). Our [5000,
        // 6000) term-2 bytes are uncertified — truncate to 5000, keep (1,0).
        let own = [(1, 0), (2, 5000)];
        let leader = [(1, 0)];
        match reconcile(&own, 6000, &leader) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, 5000);
                assert_eq!(o.new_map, vec![(1, 0)]);
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    /// F4 behind-follower-clean: the follower's OWN map already carries the
    /// term-2 stamp (it streamed and durably wrote term-2 bytes, recording the
    /// term via `DataTermObserved`). Reconciling against the identical leader
    /// map is clean — no truncation, map unchanged. This is the case that the
    /// old (unsafe) code conflated with the ex-leader-divergent case below;
    /// data-stamping is exactly what tells them apart.
    #[test]
    fn behind_follower_with_stamped_term_is_clean() {
        let own = [(1, 0), (2, 2000)];
        let leader = [(1, 0), (2, 2000)];
        match reconcile(&own, 3000, &leader) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, 3000); // == durable ⇒ clean
                assert_eq!(o.new_map, vec![(1, 0), (2, 2000)]);
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    /// F4 SCENARIO A — the safety pin. A healed ex-leader whose own map is
    /// `[(1,0)]` at durable 3000 (it wrote `[2000,3000)` under its own failed
    /// term, only ever stamping them as term 1). The real cluster opened term 2
    /// at 2000. Because our data-stamped map LACKS a term-2 entry below our
    /// durable, those bytes are provably ours-and-divergent: truncate to 2000,
    /// keep only `[(1,0)]`. The old code adopted the gossiped `(2,2000)` and
    /// returned `valid_up_to == 3000` — recording stale divergent bytes as the
    /// term-2 leader's data (split-history). RED against the pre-fix code.
    #[test]
    fn ex_leader_divergent_truncates_at_leaders_uncovered_base() {
        let own = [(1, 0)];
        let leader = [(1, 0), (2, 2000)];
        match reconcile(&own, 3000, &leader) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, 2000); // < durable ⇒ truncate
                assert_eq!(o.new_map, vec![(1, 0)]);
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[test]
    fn entry_at_the_bound_is_not_a_divergence() {
        // Leader opened term 2 exactly at our durable: our own map lacks it, but
        // the term-2 base (3000) is NOT below our durable (3000), so no byte of
        // ours is attributed to it — clean, no truncation. Our map does not grow
        // here (that happens when we actually stream term-2 data).
        let own = [(1, 0)];
        let leader = [(1, 0), (2, 3000)];
        match reconcile(&own, 3000, &leader) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, 3000); // clean, no truncation
                assert_eq!(o.new_map, vec![(1, 0)]); // (2,3000) not adopted
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    /// F4 Minor: same base, different term. `own=[(5,0)]`, `leader=[(6,0)]` —
    /// the whole history diverges at position 0, but position 0 is shared ground
    /// (both begin there), so this is NOT `NoCommonPrefix`; it is a truncate to
    /// 0. The leader's term-6 base (0) sits below our durable and our map lacks
    /// it ⇒ everything above 0 is ours-and-divergent.
    #[test]
    fn same_base_different_term_truncates_to_zero() {
        match reconcile(&[(5, 0)], 4096, &[(6, 0)]) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, 0);
                assert_eq!(o.new_map, vec![]);
            }
            other => panic!("expected Ok (truncate to 0), got {other:?}"),
        }
    }

    #[test]
    fn windowed_leader_map_aligns_against_full_own_map() {
        // THE 2026-08-16 acked-write-loss regression. A healthy follower's
        // FULL map vs a leader whose shipped window slid (lifetime terms >
        // MAX_TERM_MAP_WIRE_ENTRIES): the window's first entry is our entry
        // j > 0, not our entry 0. The old index-aligned match returned
        // NoCommonPrefix here — wiping every healthy follower in a loop and
        // eventually truncating committed bytes cluster-wide. Alignment must
        // find the window inside our history and reconcile CLEAN.
        let own: Vec<(u32, u64)> = (0..80u32).map(|i| (i + 1, i as u64 * 1000)).collect();
        let leader: Vec<(u32, u64)> = own[16..].to_vec(); // window of the last 64
        let durable = 80_000;
        match reconcile(&own, durable, &leader) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, durable, "healthy follower must not truncate");
                assert_eq!(
                    o.new_map, own,
                    "full map survives, below-window entries kept"
                );
            }
            other => panic!("expected clean Ok, got {other:?}"),
        }
    }

    #[test]
    fn windowed_alignment_still_cuts_at_real_divergence() {
        // Same windowed shape, but our tail genuinely diverges: we hold a
        // term-70 entry at base 69_000 where the leader's window shows term
        // 71 opening at 68_000 (< our durable). The own-run clamp must cut
        // at the first mismatched entry exactly as in the unwindowed case.
        let mut own: Vec<(u32, u64)> = (0..70u32).map(|i| (i + 1, i as u64 * 1000)).collect();
        own.push((71, 70_000)); // divergent frontier entry (our own term 71)
        let mut leader: Vec<(u32, u64)> = own[16..70].to_vec();
        leader.push((72, 69_500)); // leader's term 72 opened below our durable
        match reconcile(&own, 71_000, &leader) {
            Reconcile::Ok(o) => {
                // Divergence at our (71, 70_000) vs leader (72, 69_500):
                // leader-side clamp fires at 69_500.
                assert_eq!(o.valid_up_to, 69_500);
                assert!(
                    o.new_map
                        .iter()
                        .all(|&(_, b)| b < 69_500 || o.new_map.len() == 70)
                );
            }
            other => panic!("expected Ok with cut, got {other:?}"),
        }
    }

    #[test]
    fn window_start_inside_our_bytes_but_unknown_term_cuts_there() {
        // The window's first entry names a term our data-stamped history
        // never observed, at a base BELOW our durable: the bytes we hold from
        // that base on are provably not that term's — truncate there instead
        // of wiping the whole log (the old code wiped).
        let own = [(1u32, 0u64), (2, 2000)];
        let leader = [(40u32, 4000u64), (41, 9000)];
        match reconcile(&own, 5000, &leader) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, 4000);
                assert_eq!(o.new_map, vec![(1, 0), (2, 2000)]);
            }
            other => panic!("expected Ok cut at window start, got {other:?}"),
        }
    }

    #[test]
    fn no_common_prefix_is_surfaced() {
        // Leader shipped only a suffix (window slid past our history): its first
        // entry begins far beyond our first byte, no overlap.
        let own = [(1, 0)];
        let leader = [(40, 1 << 20), (41, 2 << 20)];
        assert!(matches!(
            reconcile(&own, 5000, &leader),
            Reconcile::NoCommonPrefix
        ));
    }

    #[test]
    fn empty_own_map_reconciles_clean_at_durable_zero() {
        // Fresh follower, no history, no bytes: nothing of ours to invalidate,
        // nothing to record (our map grows only when we actually stream data).
        match reconcile(&[], 0, &[(1, 0), (2, 5000)]) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, 0);
                assert_eq!(o.new_map, vec![]);
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }
}
