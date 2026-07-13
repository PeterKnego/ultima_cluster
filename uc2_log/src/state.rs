// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Persisted per-node consensus state (spec §6): the vote record, the
//! term map (the RecordingLog analog), and the output-progress marker
//! (Task 12), each a rotating two-slot `StableValue`. All three stores are
//! DURABLE ON RETURN (`Notifier::wait`) — the vote's persist-before-answer
//! contract and the term map's open-term-before-serving contract both depend
//! on that, so this module never exposes a fire-and-forget store. The
//! output-progress marker does not strictly need durable-on-return (a
//! persistence lag only widens at-least-once replay — see
//! `uc2_node::node::Consensus::maybe_persist_output_progress`), but reuses the
//! same durable `StableValue` primitive for consistency.

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMember {
    pub id: u32,
    pub ip: u32,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredConfig {
    pub version: u64,
    pub voters: Vec<StoredMember>,
    pub learners: Vec<StoredMember>,
    pub tombstones: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigRecord {
    pub position: u64,      // frame-END effect point; 0 = genesis
    pub config: StoredConfig,
    pub prev_position: u64,
    pub prev: StoredConfig,
}

pub type TermMap = Vec<TermMapEntry>;

/// Cached consensus state: vote, term map, output progress, snapshot floor, config record.
type CacheState = (Option<VoteRecord>, TermMap, u64, u64, Option<ConfigRecord>);

pub struct NodeState {
    vote: StableValue<VoteRecord>,
    term_map: StableValue<TermMap>,
    output_progress: StableValue<u64>,
    /// M6 Task 4: the durable snapshot floor — the position of the newest
    /// service-built snapshot the node has validated (`<= durable`) and
    /// committed to. Purge never drops journal below it. Same `StableValue`
    /// primitive + increase-only-high-water-mark discipline as
    /// `output_progress` (see `store_snapshot_floor` / the node's
    /// `maybe_persist_snapshot_floor`).
    snapshot: StableValue<u64>,
    /// M7 Task 2: the durable cluster membership configuration record —
    /// contains both the current and previous config, forming a one-level
    /// history. One-in-flight suffices because a new config is only proposable
    /// after the previous one commits, and committed entries cannot be
    /// truncated, so at most one config entry is ever truncation-exposed.
    config: StableValue<ConfigRecord>,
    /// Cached copies (`StableValue::load` clones the cached value, but reads on
    /// the consensus hot path go through this single lock; the term map, the
    /// vote, the output-progress marker, the snapshot floor, and the config
    /// record share it so a recovery read is one lock, not five).
    cache: Mutex<CacheState>,
}

impl NodeState {
    /// Open (create if absent) `vote.state` + `term_map.state` +
    /// `output_progress.state` + `snapshot.state` + `config.state` in `dir`,
    /// seeding the read cache from whatever survived the last run.
    pub fn open(dir: &Path) -> Result<Self, StableValueError> {
        let vote = StableValue::open(StableValueConfig::new(dir.join("vote.state")))?;
        let term_map = StableValue::open(StableValueConfig::new(dir.join("term_map.state")))?;
        let output_progress =
            StableValue::open(StableValueConfig::new(dir.join("output_progress.state")))?;
        let snapshot = StableValue::open(StableValueConfig::new(dir.join("snapshot.state")))?;
        let config = StableValue::open(StableValueConfig::new(dir.join("config.state")))?;
        let v = vote.load()?;
        let m = term_map.load()?.unwrap_or_default();
        let op = output_progress.load()?.unwrap_or(0);
        let snap = snapshot.load()?.unwrap_or(0);
        let c = config.load()?;
        Ok(Self {
            vote,
            term_map,
            output_progress,
            snapshot,
            config,
            cache: Mutex::new((v, m, op, snap, c)),
        })
    }

    pub fn vote(&self) -> Option<VoteRecord> {
        self.cache.lock().unwrap().0
    }

    pub fn term_map(&self) -> TermMap {
        self.cache.lock().unwrap().1.clone()
    }

    /// The last durably-persisted output-progress marker (Task 12); `0` if the
    /// output loop has never advanced it (or this is a fresh instance dir).
    pub fn output_progress(&self) -> u64 {
        self.cache.lock().unwrap().2
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

    /// Durable on return — persist the output-progress marker (Task 12). A
    /// stale (lagging) reader between this write and a crash only widens the
    /// next incarnation's at-least-once replay window; never a correctness
    /// issue (see the module doc).
    pub fn store_output_progress(&self, v: u64) -> Result<(), StableValueError> {
        self.output_progress.store(&v)?.wait().map_err(durability_error)?;
        self.cache.lock().unwrap().2 = v;
        Ok(())
    }

    /// The last durably-persisted snapshot floor (M6 Task 4); `0` if no snapshot
    /// has ever been validated (or this is a fresh instance dir).
    pub fn snapshot_floor(&self) -> u64 {
        self.cache.lock().unwrap().3
    }

    /// Durable on return — persist the snapshot floor (M6 Task 4). Caller
    /// (`Consensus::maybe_persist_snapshot_floor`) enforces the increase-only
    /// high-water-mark and the `<= durable` validation BEFORE calling this, for
    /// the exact reason the output-progress marker does: the cnc page is
    /// recreated fresh every boot, so a naive "value changed" persist would
    /// regress the durable floor to a lower live value on the first cycle and
    /// (unlike output-progress, which is only at-least-once slack) that would be
    /// a SAFETY bug — a purge floor must never move backwards. This method is
    /// the plain durable store; the guard lives at the one call site.
    pub fn store_snapshot_floor(&self, v: u64) -> Result<(), StableValueError> {
        self.snapshot.store(&v)?.wait().map_err(durability_error)?;
        self.cache.lock().unwrap().3 = v;
        Ok(())
    }

    /// The last durably-persisted cluster membership configuration record
    /// (M7 Task 2); `None` if no config has been recorded (a fresh instance
    /// must seed genesis before any configuration state is durable).
    pub fn config_record(&self) -> Option<ConfigRecord> {
        self.cache.lock().unwrap().4.clone()
    }

    /// Durable on return — persist the cluster membership configuration record
    /// (M7 Task 2). A one-level history (current and previous) suffices because
    /// a new config is only proposable after the previous one commits, and
    /// committed entries cannot be truncated — so at most one config entry is
    /// ever truncation-exposed and thus at risk of reverting.
    pub fn store_config_record(&self, r: &ConfigRecord) -> Result<(), StableValueError> {
        self.config.store(r)?.wait().map_err(durability_error)?;
        self.cache.lock().unwrap().4 = Some(r.clone());
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

    #[test]
    fn output_progress_defaults_to_zero_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let s = NodeState::open(dir.path()).unwrap();
            assert_eq!(s.output_progress(), 0, "fresh instance dir marker defaults to 0");
            s.store_output_progress(4096).unwrap();
            assert_eq!(s.output_progress(), 4096);
        }
        // "restart": reopen from the same dir.
        let s = NodeState::open(dir.path()).unwrap();
        assert_eq!(s.output_progress(), 4096, "durable marker survives reopen");
    }

    #[test]
    fn snapshot_floor_defaults_to_zero_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let s = NodeState::open(dir.path()).unwrap();
            assert_eq!(s.snapshot_floor(), 0, "fresh instance dir floor defaults to 0");
            s.store_snapshot_floor(8192).unwrap();
            assert_eq!(s.snapshot_floor(), 8192);
        }
        // "restart": reopen from the same dir — the floor is a durable value.
        let s = NodeState::open(dir.path()).unwrap();
        assert_eq!(s.snapshot_floor(), 8192, "durable snapshot floor survives reopen");
    }

    #[test]
    fn snapshot_floor_is_independent_of_output_progress() {
        let dir = tempfile::tempdir().unwrap();
        let s = NodeState::open(dir.path()).unwrap();
        s.store_output_progress(4096).unwrap();
        s.store_snapshot_floor(8192).unwrap();
        assert_eq!(s.output_progress(), 4096);
        assert_eq!(s.snapshot_floor(), 8192, "separate StableValues, separate cache slots");
    }

    #[test]
    fn config_record_defaults_none_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let genesis = StoredConfig {
            version: 0,
            voters: vec![StoredMember { id: 1, ip: 0x0a000001, port: 19100 }],
            learners: vec![],
            tombstones: vec![],
        };
        {
            let s = NodeState::open(dir.path()).unwrap();
            assert_eq!(s.config_record(), None, "fresh dir: no record until the node seeds genesis");
            let r = ConfigRecord {
                position: 0,
                config: genesis.clone(),
                prev_position: 0,
                prev: genesis.clone(),
            };
            s.store_config_record(&r).unwrap();
            assert_eq!(s.config_record(), Some(r));
        }
        let s = NodeState::open(dir.path()).unwrap();
        let r = s.config_record().expect("survives reopen");
        assert_eq!(r.config, genesis);
        assert_eq!(r.position, 0);
    }
}
