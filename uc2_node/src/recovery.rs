// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M11 Task 4: offline access to the node's REAL boot-time `ConfigRecord`
//! recovery — genesis-seed -> T5-carry revert -> forward re-derivation, see
//! `crate::node::recover_config_record`'s doc for the full algorithm — for
//! `uc2ctl force-single-member` and any other offline tooling. This module
//! never reimplements that logic: [`recovered_config`] opens exactly what a
//! real boot opens (the exclusive instance flock, the journal `Archive`,
//! `NodeState`) and calls `crate::node::recover_config_record` directly, so
//! the config it reports is always the SAME config a real boot of the same
//! instance dir would adopt.
//!
//! [`force_single_member`] is the quorum-loss recovery tool: given a durable
//! survivor's instance dir and its own node id, it writes a NEW `ConfigRecord`
//! naming that node the SOLE voter.
//!
//! - `position`/`prev_position` are pinned at the recovered durable frontier
//!   (`durable`), so the record survives the T5-carry revert on the
//!   survivor's OWN next boot (`recover_config_record` only reverts when
//!   `rec.position > durable` — pinning at exactly `durable` means that
//!   never holds).
//! - `version` is one past the recovered version — it beats every archived
//!   `FRAME_TYPE_CONFIG` frame at or below `durable` (`rederive_config`
//!   already folded everything up to `durable` into `recovered.version`,
//!   and nothing exists above `durable` by definition of that being the
//!   journal's own recovered frontier), so this node's own next boot
//!   re-adopts the forced record rather than something older.
//! - `voters` is exactly the one survivor, at its EXISTING address from the
//!   recovered config (never a caller-supplied address — the point is to
//!   trust only what was already durable).
//! - `learners` is empty and `tombstones` passes `recovered.tombstones`
//!   through UNCHANGED — dropped peers are never tombstoned here (Global
//!   Constraints, plan `docs/superpowers/plans/2026-08-20-uc2-m11-survivable-cluster.md`):
//!   they wipe-and-rejoin later as fresh ids/learners, exactly like any other
//!   voluntary membership change.
//!
//! Vote and term-map state are left untouched: quorum-of-1 falls out of
//! `uc2_consensus::election::ElectionSm` simply reading the adopted
//! one-voter config on the survivor's next boot, not from any
//! force-specific consensus code path.
//!
//! No manual backup of `config.state` is needed before this overwrite:
//! `ultima_journal::StableValue`'s two-slot rotation already keeps the
//! previous generation on disk (`stable_value.rs`'s `pick_slot` — one slot
//! is always intact), so the PRE-force config record survives as the older
//! of the two slots. This module provides no tool to read that slot back —
//! the quorum-loss procedure is a one-way door by design, which is exactly
//! what the CLI's data-loss statement says before it writes.

use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;

use uc2_log::archive::{Archive, ArchiveConfig};
use uc2_log::state::{ConfigRecord, NodeState, StoredConfig, StoredMember};

use crate::ipc::InstanceDir;
use crate::node::recover_config_record;

/// The config a real boot of this instance dir would adopt, plus the
/// journal's recovered durable frontier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredConfig {
    pub version: u64,
    pub voters: Vec<(u32, SocketAddr)>,
    pub learners: Vec<(u32, SocketAddr)>,
    pub tombstones: Vec<u32>,
    pub durable: u64,
}

/// What [`force_single_member`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForceReport {
    pub old_version: u64,
    pub new_version: u64,
    pub durable: u64,
    /// Every id dropped from the recovered voters/learners set (i.e.
    /// everyone but the survivor) — NOT tombstoned, see the module doc.
    pub dropped_peers: Vec<u32>,
}

fn addr_from_pair(ip: u32, port: u16) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from(ip.to_be_bytes()), port))
}

fn members_from_stored(members: &[StoredMember]) -> Vec<(u32, SocketAddr)> {
    members.iter().map(|m| (m.id, addr_from_pair(m.ip, m.port))).collect()
}

fn to_io<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::other(e.to_string())
}

/// Take the instance's exclusive flock (refusing with an error if a node is
/// currently running), open the journal `Archive` and `NodeState` exactly as
/// boot does, and run the real `recover_config_record`. Returns the still-
/// held `InstanceDir` (and the open `NodeState`) along with the recovered
/// record — [`recovered_config`] drops the lock immediately (a pure read);
/// [`force_single_member`] keeps holding it through its own refusal checks
/// and its write, so the whole read-check-write sequence runs under ONE
/// flock acquisition, not two (no window for a node to start in between).
fn recover_locked(instance_dir: &Path) -> io::Result<(InstanceDir, NodeState, ConfigRecord, u64)> {
    let instance = InstanceDir::acquire(instance_dir)
        .map_err(|e| io::Error::other(format!("a node is running: {e}")))?;

    let archive_cfg = ArchiveConfig::new(instance.journal_dir());
    let archive = Archive::open(archive_cfg).map_err(to_io)?;
    let durable = archive.recovered_position();
    let state = NodeState::open(&instance.state_dir()).map_err(to_io)?;

    // No genesis members/learners to seed with: this tool only ever operates
    // on an instance dir that has already booted at least once (a truly
    // fresh, never-booted dir has no meaningful "recover the effective
    // config" question to answer — `force_single_member` would have nothing
    // real to narrow). The genesis-seed fallback inside
    // `recover_config_record` is dead code on that path (it only fires when
    // `state.config_record()` is `None`).
    let rec = recover_config_record(&state, &archive, durable, &[], &[])?;
    Ok((instance, state, rec, durable))
}

/// Read-only intent: the effective config a real boot of `instance_dir`
/// would adopt right now. Refuses if a node currently holds the instance
/// flock.
pub fn recovered_config(instance_dir: &Path) -> io::Result<RecoveredConfig> {
    let (_instance, _state, rec, durable) = recover_locked(instance_dir)?;
    Ok(RecoveredConfig {
        version: rec.config.version,
        voters: members_from_stored(&rec.config.voters),
        learners: members_from_stored(&rec.config.learners),
        tombstones: rec.config.tombstones,
        durable,
    })
}

/// Quorum-loss recovery: force `instance_dir` to adopt a single-voter config
/// naming `node_id` as the sole member — see the module doc for the exact
/// record-construction rules. Refuses (no write happens) if:
/// - a node currently holds the instance flock ("a node is running"),
/// - `node_id` is tombstoned in the recovered config,
/// - `node_id` is not a voter or learner in the recovered config.
pub fn force_single_member(instance_dir: &Path, node_id: u32) -> io::Result<ForceReport> {
    let (_instance, state, rec, durable) = recover_locked(instance_dir)?;
    let recovered = rec.config;

    if recovered.tombstones.contains(&node_id) {
        return Err(io::Error::other(format!(
            "node {node_id} is tombstoned in the recovered cluster config (v{}); \
             a tombstoned id can never be forced back into the cluster",
            recovered.version
        )));
    }

    let survivor = recovered
        .voters
        .iter()
        .chain(recovered.learners.iter())
        .find(|m| m.id == node_id)
        .cloned();
    let Some(survivor) = survivor else {
        return Err(io::Error::other(format!(
            "node {node_id} is not a member (voter or learner) of the recovered cluster \
             config (v{})",
            recovered.version
        )));
    };

    let dropped_peers: Vec<u32> = recovered
        .voters
        .iter()
        .chain(recovered.learners.iter())
        .map(|m| m.id)
        .filter(|&id| id != node_id)
        .collect();

    let forced = StoredConfig {
        version: recovered.version + 1,
        voters: vec![survivor],
        learners: Vec::new(),
        tombstones: recovered.tombstones,
    };
    let new_rec = ConfigRecord {
        position: durable,
        prev_position: durable,
        config: forced.clone(),
        prev: forced,
    };
    state.store_config_record(&new_rec).map_err(to_io)?;

    Ok(ForceReport {
        old_version: recovered.version,
        new_version: new_rec.config.version,
        durable,
        dropped_peers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Scratch on real disk (`CARGO_TARGET_TMPDIR`), never `/tmp` — RAM-backed
    /// tmpfs with no swap on this box (CLAUDE.md); same pattern as
    /// `crate::node`'s `crypto_scratch_dir` (a lib unit test doesn't get
    /// `CARGO_TARGET_TMPDIR` via the `env!` macro the way an integration test
    /// binary does, so this reads it at runtime instead).
    fn scratch_dir(tag: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::var("CARGO_TARGET_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc2_node_tests")
            })
            .join("uc2-node-recovery")
            .join(format!("{tag}-{seq}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn stored_member(id: u32, a: SocketAddr) -> StoredMember {
        match a.ip() {
            std::net::IpAddr::V4(v4) => StoredMember { id, ip: u32::from(v4), port: a.port() },
            std::net::IpAddr::V6(_) => panic!("ipv4 only"),
        }
    }

    fn seed_config(dir: &Path, version: u64, voters: &[(u32, SocketAddr)], tombstones: Vec<u32>) {
        std::fs::create_dir_all(dir.join("state")).unwrap();
        let cfg = StoredConfig {
            version,
            voters: voters.iter().map(|(id, a)| stored_member(*id, *a)).collect(),
            learners: Vec::new(),
            tombstones,
        };
        let rec = ConfigRecord { position: 0, config: cfg.clone(), prev_position: 0, prev: cfg };
        NodeState::open(&dir.join("state")).unwrap().store_config_record(&rec).unwrap();
    }

    #[test]
    fn addr_from_pair_roundtrips_addr_to_pair() {
        let a: SocketAddr = "127.0.0.1:4242".parse().unwrap();
        let m = stored_member(1, a);
        assert_eq!(addr_from_pair(m.ip, m.port), a);
    }

    #[test]
    fn recovered_config_reports_a_pre_seeded_record() {
        let dir = scratch_dir("recovered-config-basic");
        let addr: SocketAddr = "127.0.0.1:59921".parse().unwrap();
        seed_config(&dir, 3, &[(1, addr)], vec![9]);
        let rc = recovered_config(&dir).expect("recovered_config");
        assert_eq!(rc.version, 3);
        assert_eq!(rc.voters, vec![(1, addr)]);
        assert_eq!(rc.tombstones, vec![9]);
        assert_eq!(rc.durable, 0);
    }
}
