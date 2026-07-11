// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Persisted per-node consensus state (spec §6): the vote record and the
//! term map (the RecordingLog analog), each a rotating two-slot
//! `StableValue`. Both stores are DURABLE ON RETURN (`Notifier::wait`) —
//! the vote's persist-before-answer contract and the term map's
//! open-term-before-serving contract both depend on that, so this module
//! never exposes a fire-and-forget store.

use std::path::Path;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use ultima_journal::{JournalError, StableValue, StableValueConfig, StableValueError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VoteRecord {
    pub term: u32,
    /// NodeId of the candidate voted for in `term`.
    pub voted_for: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TermMapEntry {
    pub term: u32,
    /// Absolute stream position where this leadership term begins.
    pub base: u64,
}

pub type TermMap = Vec<TermMapEntry>;

pub struct NodeState {
    vote: StableValue<VoteRecord>,
    term_map: StableValue<TermMap>,
    /// Cached copies (`StableValue::load` clones the cached value, but reads on
    /// the consensus hot path go through this single lock; the term map and the
    /// vote share it so a recovery read is one lock, not two).
    cache: Mutex<(Option<VoteRecord>, TermMap)>,
}

impl NodeState {
    /// Open (create if absent) `vote.state` + `term_map.state` in `dir`, seeding
    /// the read cache from whatever survived the last run.
    pub fn open(dir: &Path) -> Result<Self, StableValueError> {
        let vote = StableValue::open(StableValueConfig::new(dir.join("vote.state")))?;
        let term_map = StableValue::open(StableValueConfig::new(dir.join("term_map.state")))?;
        let v = vote.load()?;
        let m = term_map.load()?.unwrap_or_default();
        Ok(Self { vote, term_map, cache: Mutex::new((v, m)) })
    }

    pub fn vote(&self) -> Option<VoteRecord> {
        self.cache.lock().unwrap().0
    }

    pub fn term_map(&self) -> TermMap {
        self.cache.lock().unwrap().1.clone()
    }

    /// Durable on return — the caller may answer the vote request only after
    /// this returns `Ok`.
    pub fn store_vote(&self, v: VoteRecord) -> Result<(), StableValueError> {
        self.vote.store(&v)?.wait().map_err(durability_error)?;
        self.cache.lock().unwrap().0 = Some(v);
        Ok(())
    }

    /// Durable on return — the new term exists before the leader acts in it.
    pub fn store_term_map(&self, m: &TermMap) -> Result<(), StableValueError> {
        self.term_map.store(m)?.wait().map_err(durability_error)?;
        self.cache.lock().unwrap().1 = m.clone();
        Ok(())
    }
}

/// Map a `Notifier::wait()` failure into a `StableValueError`.
///
/// `wait()` yields a `JournalError`, and there is no `From<JournalError> for
/// StableValueError` (and `ultima_journal` is out of scope to change here). In
/// `Durability::Consistent` mode — the only mode `StableValue` uses — a `wait()`
/// failure reflects a durable-write / fsync I/O failure, so `Io` is the closest
/// honest variant. (In practice `StableValue::store` fsyncs synchronously and
/// returns an already-resolved `Notifier::done()`, so this path is never taken;
/// it exists to keep the durable-on-return contract total.)
fn durability_error(e: JournalError) -> StableValueError {
    StableValueError::Io(std::io::Error::other(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vote_and_term_map_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let s = NodeState::open(dir.path()).unwrap();
            assert_eq!(s.vote(), None);
            assert!(s.term_map().is_empty());
            s.store_vote(VoteRecord { term: 3, voted_for: 2 }).unwrap();
            s.store_term_map(&vec![
                TermMapEntry { term: 1, base: 0 },
                TermMapEntry { term: 3, base: 4096 },
            ])
            .unwrap();
        }
        // "restart": reopen from the same dir
        let s = NodeState::open(dir.path()).unwrap();
        assert_eq!(s.vote(), Some(VoteRecord { term: 3, voted_for: 2 }));
        assert_eq!(
            s.term_map(),
            vec![TermMapEntry { term: 1, base: 0 }, TermMapEntry { term: 3, base: 4096 }]
        );
    }

    #[test]
    fn store_vote_overwrites_previous_term() {
        let dir = tempfile::tempdir().unwrap();
        let s = NodeState::open(dir.path()).unwrap();
        s.store_vote(VoteRecord { term: 1, voted_for: 0 }).unwrap();
        s.store_vote(VoteRecord { term: 2, voted_for: 1 }).unwrap();
        assert_eq!(s.vote(), Some(VoteRecord { term: 2, voted_for: 1 }));
    }
}
