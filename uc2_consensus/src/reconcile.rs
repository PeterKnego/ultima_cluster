// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Term-map reconciliation (spec §M4): the pure, exhaustively-testable core of
//! log truncation. A follower that receives the leader's term map compares it
//! against its own history and decides, byte-for-byte, how far its local log is
//! still valid — and which term-map entries it may adopt.
//!
//! A **term map** is an ascending `Vec<(term, base)>`: term `t`'s bytes begin at
//! byte position `base`. Positions are globally monotone across the cluster, so
//! two maps that share an entry share every byte below that entry's successor.
//!
//! [`reconcile`] is pure over `(own, own_durable, leader)`:
//!
//! - It finds the longest common prefix of the two maps.
//! - `valid_up_to` = our durable, clamped down to the base of our first entry
//!   *beyond* the common prefix (a term/history the leader never certified —
//!   whether it conflicts with the leader's entry at that index or simply
//!   overhangs a shorter leader map). The leader side never clamps us *down*:
//!   bytes below our durable that the leader attributes to a newer term were
//!   streamed to us by that term's leader, so they are valid and the entry is
//!   adopted.
//! - `new_map` = the shared prefix, plus every leader entry beyond it whose
//!   `base < valid_up_to` (an entry *at* the bound covers zero of our surviving
//!   bytes, so it is not adopted yet).
//!
//! The caller (the election SM) derives the action from the [`Outcome`]:
//! `valid_up_to < own_durable` ⇒ truncate; else if `new_map` grew ⇒ persist the
//! adopted map; else nothing. [`Reconcile::NoCommonPrefix`] means the leader's
//! shipped suffix begins beyond our entire history — incremental reconciliation
//! is impossible and M6's snapshot install is the real answer; M4 surfaces it
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
    /// The reconciled term map: the shared prefix plus every adopted leader
    /// entry (base strictly below `valid_up_to`).
    pub new_map: Vec<(u32, u64)>,
}

/// Outcome of [`reconcile`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconcile {
    /// Histories share a prefix. See [`Outcome`] for the surviving bound and the
    /// reconciled map.
    Ok(Outcome),
    /// No common entry — the leader's shipped suffix begins beyond our history.
    /// Incremental reconciliation is impossible; a snapshot install (M6) is the
    /// answer. The sim/harness prove this is unreachable at `<=
    /// MAX_TERM_MAP_WIRE_ENTRIES` terms.
    NoCommonPrefix,
}

/// Compare our `own` term map (bounded by `own_durable`) against the `leader`'s.
///
/// Both maps are ascending `(term, base)` slices. See the module docs for the
/// rules; the unit tests are the binding contract.
pub fn reconcile(own: &[(u32, u64)], own_durable: u64, leader: &[(u32, u64)]) -> Reconcile {
    // A leader with no map tells us nothing — treat our history as clean.
    if leader.is_empty() {
        return Reconcile::Ok(Outcome { valid_up_to: own_durable, new_map: own.to_vec() });
    }

    // Fresh follower: no history, no bytes. Common prefix is trivially empty;
    // valid_up_to is our durable (0 for a fresh node); adopt leader entries
    // that cover the bytes we hold (base < valid_up_to).
    if own.is_empty() {
        let new_map = leader.iter().copied().filter(|(_, base)| *base < own_durable).collect();
        return Reconcile::Ok(Outcome { valid_up_to: own_durable, new_map });
    }

    // Longest common prefix (entries equal in both term and base).
    let mut k = 0;
    while k < own.len() && k < leader.len() && own[k] == leader[k] {
        k += 1;
    }

    // No shared entry: the leader's earliest shipped entry begins beyond our
    // first entry with no overlap — its window has slid past our history.
    if k == 0 && leader[0].1 > own[0].1 {
        return Reconcile::NoCommonPrefix;
    }

    // Our bytes are valid up to our durable, clamped to the base of our first
    // entry beyond the common prefix — that entry (a conflicting term, or an
    // overhang past a shorter leader map) is a history the leader never had.
    let mut valid_up_to = own_durable;
    if k < own.len() {
        valid_up_to = valid_up_to.min(own[k].1);
    }

    // The reconciled map: shared prefix + adopted leader entries. An entry with
    // base == valid_up_to covers zero of our surviving bytes, so require `<`.
    let mut new_map: Vec<(u32, u64)> = own[..k].to_vec();
    new_map.extend(leader[k..].iter().copied().filter(|(_, base)| *base < valid_up_to));

    Reconcile::Ok(Outcome { valid_up_to, new_map })
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn behind_follower_is_clean_and_adopts_covering_entries() {
        // We hold 3000 bytes recorded as term 1; leader history has term 2 at
        // 2000: our bytes [2000, 3000) were streamed by the term-2 leader — the
        // entry covers our bytes, so adopt it. No byte is invalid (valid_up_to
        // == durable), but the map grows.
        let own = [(1, 0)];
        let leader = [(1, 0), (2, 2000)];
        match reconcile(&own, 3000, &leader) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, 3000);
                assert_eq!(o.new_map, vec![(1, 0), (2, 2000)]);
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[test]
    fn entry_at_the_bound_is_not_adopted() {
        // Leader opened term 2 exactly at our durable: we hold zero bytes of
        // term 2, so the entry (base == valid_up_to) is not adopted yet.
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

    #[test]
    fn no_common_prefix_is_surfaced() {
        // Leader shipped only a suffix (window slid past our history): its first
        // entry begins far beyond our first byte, no overlap.
        let own = [(1, 0)];
        let leader = [(40, 1 << 20), (41, 2 << 20)];
        assert!(matches!(reconcile(&own, 5000, &leader), Reconcile::NoCommonPrefix));
    }

    #[test]
    fn empty_own_map_adopts_leader_prefix_below_durable() {
        // Fresh follower, no history, no bytes: nothing to adopt yet.
        match reconcile(&[], 0, &[(1, 0), (2, 5000)]) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, 0);
                assert_eq!(o.new_map, vec![]);
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[test]
    fn empty_own_map_with_streamed_bytes_adopts_covering_entries() {
        // Fresh follower that has streamed 6000 bytes but not yet recorded any
        // term-map entry: adopt every leader entry whose base is below what we
        // hold (an entry at the bound covers nothing).
        match reconcile(&[], 6000, &[(1, 0), (2, 4000), (3, 6000)]) {
            Reconcile::Ok(o) => {
                assert_eq!(o.valid_up_to, 6000);
                assert_eq!(o.new_map, vec![(1, 0), (2, 4000)]); // (3,6000) at bound
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }
}
