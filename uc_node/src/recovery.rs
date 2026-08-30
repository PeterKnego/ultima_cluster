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
//! # `recovered_config` must never persist (fix round 1, Critical 1)
//!
//! `recover_config_record` is boot logic, not read-only logic: it has TWO
//! internal paths that CALL `state.store_config_record` via its own `seed()`
//! helper, persisting a fabricated, empty, zero-voter genesis record —
//! (a) no record exists yet (`state.config_record().is_none()`), and (b) the
//! "doubly-ahead" compounding-crash window (`rec.position > durable` AND
//! `rec.prev_position > durable` — two config adoptions durably persisted in
//! the same crash, before any archive catch-up, leaving nothing genuine left
//! to revert to). Both are correct for a real BOOT (a fresh instance dir
//! SHOULD genesis-seed; a doubly-ahead crash SHOULD fall back to a fresh seed
//! and let the cluster's real leader re-teach the node) — but this module's
//! whole contract is "read-only intent, refuse if a node is running", and
//! silently persisting a fabricated empty config over a crashed survivor's
//! real (if momentarily unreadable) membership would violate that even
//! though nothing was RUNNING at the time. Worse, it is a ONE-WAY hazard:
//! once ANY record exists (even an empty one), the absent-record seed path
//! can never fire again on a later, correct call — the fabricated record
//! would be permanent.
//!
//! [`recover_locked`] therefore inspects `state.config_record()` itself,
//! BEFORE ever calling `recover_config_record`, and refuses outright on
//! either case rather than letting the real function's seed path run. Both
//! [`recovered_config`] and [`force_single_member`]/[`plan_force_single_member`]
//! share this one path, so neither can persist anything when they refuse —
//! see `force_config.rs`'s `*_without_persisting` regression tests (the
//! reviewer's empirical probe, made permanent).
//!
//! # One entry point, one lock (fix round 1, Important 6)
//!
//! [`plan_force_single_member`] does the ENTIRE read-check-plan sequence
//! under a single [`InstanceDir::acquire`] and keeps holding that lock (via
//! the returned [`PlannedForce`]) until [`PlannedForce::commit`] either
//! writes or is dropped. A caller that needs to print the data-loss
//! statement before writing (`uc2ctl`) calls `plan_force_single_member` once,
//! prints [`PlannedForce::data_loss_statement`], then calls `commit` — NO
//! second `recover_locked`/flock cycle in between, so there is no window for
//! a node to start between the print and the write. [`force_single_member`]
//! is the one-shot convenience wrapper (`plan(...)?.commit()`) for callers
//! that don't need the pre-write print (tests, other tooling).
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
//! `uc_consensus::election::ElectionSm` simply reading the adopted
//! one-voter config on the survivor's next boot, not from any
//! force-specific consensus code path.
//!
//! # The two-slot rotation is not always a one-generation guarantee (fix round 1, Minor 7)
//!
//! No SEPARATE manual backup of `config.state` is taken before the forced
//! write: `uc_journal::StableValue`'s two-slot rotation
//! (`stable_value.rs`'s `pick_slot` — one slot is always intact) already
//! keeps the previously-stored generation on disk. In the COMMON case (the
//! recovered record's `position <= durable`, so `recover_config_record`
//! reads it without reverting) this call performs exactly ONE store — the
//! forced write — so the record that was on disk immediately before this
//! call IS the older of the two slots afterward. But when a SINGLE (not
//! doubly-ahead — that case now refuses, see above) T5-carry revert fires
//! inside `recover_config_record`, that revert ITSELF persists the reverted
//! record before this call makes its own forced write — TWO stores in one
//! call. The two-slot rotation only ever retains ONE prior generation, so in
//! that case the slot left over after this call holds the REVERTED record,
//! not the record that was truly on disk at the START of this call (that one
//! was already superseded by the revert's own store, before the forced write
//! ever ran). This module provides no tool to read either slot back in any
//! case — the quorum-loss procedure is a one-way door by design, which is
//! exactly what the CLI's data-loss statement says before it writes.

use std::io;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::Path;

use uc_log::archive::{Archive, ArchiveConfig};
use uc_log::state::{ConfigRecord, NodeState, StoredConfig, StoredMember};

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

/// What [`force_single_member`]/[`PlannedForce::commit`] did.
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

/// The exact quorum-loss data-loss statement `uc2ctl force-single-member`
/// prints BEFORE writing (M11 plan, Task 4 brief). Lifted here as the one
/// source of truth — fix round 1, Important 3 — so the CLI cannot drift from
/// what `uc_node/tests/force_config.rs` pins byte-for-byte.
pub fn data_loss_statement(node_id: u32, durable: u64, dropped_peers: &[u32]) -> String {
    format!(
        "forcing node {node_id} to a single-member cluster at durable position {durable}: any \
         write acknowledged by the old quorum but not held in this node's journal is LOST; \
         peers {dropped_peers:?} are dropped from the config and must be wiped and rejoined as \
         fresh learners."
    )
}

/// Take the instance's exclusive flock (refusing with an error if a node is
/// currently running), open the journal `Archive` and `NodeState` exactly as
/// boot does, and run the real `recover_config_record` — but ONLY after
/// proving neither of its two FABRICATING paths can fire (see the module
/// doc's "`recovered_config` must never persist" section); refuses instead.
///
/// This rules out the fabrication hazard, but NOT every write
/// `recover_config_record` can make: two genuine, non-fabricating,
/// idempotent boot-semantic persists remain reachable even after the guard
/// above — the single T5-carry revert (`node.rs`'s `recover_config_record`,
/// the `state.store_config_record(&rec)` call after reverting to `rec.prev`,
/// fired when `rec.position > durable` but `rec.prev_position <= durable` —
/// the doubly-ahead case above is excluded, this is the ordinary single-crash
/// case) and the forward re-derive fold (`rederive_config`'s own
/// `state.store_config_record(&rederived)`, fired when the archive holds a
/// `FRAME_TYPE_CONFIG` frame not yet folded into the persisted record). Both
/// recover REAL, previously-adopted data (a prior config generation, or an
/// archived frame) rather than inventing anything — the hazard this module
/// guards against — so [`recovered_config`] is read-only in CONTRACT (never
/// fabricates, never loses data) but not a literal zero-write guarantee.
///
/// Returns the still-held `InstanceDir` (and the open `NodeState`) along
/// with the recovered record — [`recovered_config`] drops the lock
/// immediately; [`plan_force_single_member`] keeps holding it all the way
/// through [`PlannedForce::commit`], so the whole read-check-plan-write
/// sequence runs under ONE flock acquisition.
fn recover_locked(instance_dir: &Path) -> io::Result<(InstanceDir, NodeState, ConfigRecord, u64)> {
    let instance = InstanceDir::acquire(instance_dir)
        .map_err(|e| io::Error::other(format!("a node is running: {e}")))?;

    // `preallocate_segments: false`, exactly like `backup.rs::verify_artifact`'s
    // `ArchiveConfig` construction (see that function's doc comment on its
    // `ArchiveConfig` for the full rationale — cited here rather than
    // repeated): the default (`true`) re-preallocates the active segment up
    // to `segment_size_bytes` (64 MiB) on EVERY open, an artifact-scale
    // mutation this offline, read-mostly tool has no business making on a
    // possibly disk-pressured survivor.
    //
    // Checked directly against `uc_journal::journal::mod::Journal::open`
    // (fix round 1, Important 2) rather than assumed: the background
    // `SegmentPipeline` that pre-creates the NEXT segment's temp file is
    // gated on the SAME flag (`let pipeline = if
    // config.preallocate_segments { Some(SegmentPipeline::spawn(...)) } else
    // { None };`), and the active segment's own re-preallocation-on-open is
    // gated on it too (`if config.preallocate_segments { ... }` around the
    // fallocate call). With `false`, NEITHER runs — no re-preallocation, no
    // pipeline thread, no `seg-prealloc.*.tmp` temp file is ever created, so
    // there is nothing to suppress or clean up beyond setting the flag. A
    // torn active-segment tail still heals (a physical shrink-only
    // truncate, same as `verify_artifact`) — all this tool needs; it never
    // appends.
    let archive_cfg =
        ArchiveConfig { preallocate_segments: false, ..ArchiveConfig::new(instance.journal_dir()) };
    let archive = Archive::open(archive_cfg).map_err(to_io)?;
    let durable = archive.recovered_position();
    let state = NodeState::open(&instance.state_dir()).map_err(to_io)?;

    match state.config_record() {
        None => {
            return Err(io::Error::other(
                "instance dir has no durable config record yet — not a previously-booted node; \
                 force-single-member only operates on an already-initialized instance (no config \
                 record was written; the dir/state-file skeleton InstanceDir::acquire and \
                 NodeState::open create either way is not a config record)",
            ));
        }
        Some(ref rec) if rec.position > durable && rec.prev_position > durable => {
            return Err(io::Error::other(format!(
                "doubly-ahead crash window: the durable config record's current position ({}) \
                 AND its previous position ({}) both exceed the recovered archive frontier \
                 ({durable}) — nothing genuine is left to revert to, and force-single-member \
                 refuses rather than falling back to an empty seed (no config record was \
                 written); wipe this instance dir and rejoin it as a fresh id instead",
                rec.position, rec.prev_position
            )));
        }
        Some(_) => {}
    }

    let rec = recover_config_record(&state, &archive, durable, &[], &[])?;
    Ok((instance, state, rec, durable))
}

/// Read-only intent: the effective config a real boot of `instance_dir`
/// would adopt right now. Refuses (writing nothing) if a node currently
/// holds the instance flock, if no config record exists yet, or on the
/// doubly-ahead crash window — see [`recover_locked`].
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

/// A computed, not-yet-written quorum-loss force — everything a caller needs
/// to print the data-loss statement BEFORE writing, obtained under the SAME
/// flock acquisition [`PlannedForce::commit`] writes under (fix round 1,
/// Important 6: no second lock/open/recover cycle in between). Dropping a
/// `PlannedForce` without calling `commit` writes nothing and simply
/// releases the flock.
pub struct PlannedForce {
    node_id: u32,
    durable: u64,
    dropped_peers: Vec<u32>,
    old_version: u64,
    new_version: u64,
    new_rec: ConfigRecord,
    // Held only to keep the flock alive from `plan` through `commit`; never
    // read directly.
    _instance: InstanceDir,
    state: NodeState,
}

impl PlannedForce {
    /// The durable position the forced record is pinned at.
    pub fn durable(&self) -> u64 {
        self.durable
    }

    /// Every id being dropped from the config (not tombstoned — see the
    /// module doc).
    pub fn dropped_peers(&self) -> &[u32] {
        &self.dropped_peers
    }

    /// The exact data-loss statement for this planned force — see
    /// [`data_loss_statement`].
    pub fn data_loss_statement(&self) -> String {
        data_loss_statement(self.node_id, self.durable, &self.dropped_peers)
    }

    /// Write the forced record — the ONLY step that mutates anything. Still
    /// under the same flock `plan_force_single_member` acquired.
    pub fn commit(self) -> io::Result<ForceReport> {
        self.state.store_config_record(&self.new_rec).map_err(to_io)?;
        Ok(ForceReport {
            old_version: self.old_version,
            new_version: self.new_version,
            durable: self.durable,
            dropped_peers: self.dropped_peers,
        })
    }
}

/// Plan (but do not yet write) a quorum-loss force of `instance_dir` onto a
/// single-voter config naming `node_id` as the sole member — see the module
/// doc for the exact record-construction rules. Refuses (nothing is read
/// into a plan, nothing is written) if:
/// - a node currently holds the instance flock ("a node is running"),
/// - no config record exists yet, or the doubly-ahead crash window applies
///   (see [`recover_locked`]),
/// - `node_id` is tombstoned in the recovered config,
/// - `node_id` is not a voter or learner in the recovered config.
pub fn plan_force_single_member(instance_dir: &Path, node_id: u32) -> io::Result<PlannedForce> {
    let (instance, state, rec, durable) = recover_locked(instance_dir)?;
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
    let new_version = forced.version;
    let new_rec = ConfigRecord {
        position: durable,
        prev_position: durable,
        config: forced.clone(),
        prev: forced,
    };

    Ok(PlannedForce {
        node_id,
        durable,
        dropped_peers,
        old_version: recovered.version,
        new_version,
        new_rec,
        _instance: instance,
        state,
    })
}

/// One-shot quorum-loss force: plan then immediately commit under the SAME
/// lock (no gap). For callers that don't need to print anything between the
/// read and the write (tests, non-interactive tooling); `uc2ctl` uses
/// [`plan_force_single_member`] directly so it can print the data-loss
/// statement first.
pub fn force_single_member(instance_dir: &Path, node_id: u32) -> io::Result<ForceReport> {
    plan_force_single_member(instance_dir, node_id)?.commit()
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
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_node_tests")
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

    /// Fix round 1, Critical 1 regression, `recovered_config` half: a
    /// totally fresh, never-booted instance dir has no config record. Before
    /// the fix, `recover_config_record`'s genesis-seed path would fire and
    /// PERSIST an empty, zero-voter version-0 record — this asserts the
    /// refusal AND that `state/config.state` is byte-identical before and
    /// after the (refused) call.
    #[test]
    fn recovered_config_refuses_an_uninitialized_dir_without_persisting() {
        let dir = scratch_dir("recovered-config-uninitialized");
        std::fs::create_dir_all(dir.join("state")).unwrap();
        // Materialize state/*.state (create-if-absent, no value stored) the
        // same way `recover_locked` itself would, to capture a genuine
        // "before" byte image of an untouched, freshly-created config.state.
        drop(NodeState::open(&dir.join("state")).unwrap());
        let before = std::fs::read(dir.join("state").join("config.state")).unwrap();

        let err = recovered_config(&dir).expect_err("a fresh dir has no config record yet");
        assert!(
            err.to_string().to_lowercase().contains("no durable config record"),
            "unexpected message: {err}"
        );

        let after = std::fs::read(dir.join("state").join("config.state")).unwrap();
        assert_eq!(before, after, "recovered_config must not write anything when it refuses");
        assert!(
            NodeState::open(&dir.join("state")).unwrap().config_record().is_none(),
            "no record must have been persisted"
        );
    }

    /// Same regression, `force_single_member` half — the demonstrated
    /// failure path (a not-a-member refusal used to fire AFTER an empty
    /// genesis record had already been persisted).
    #[test]
    fn force_refuses_an_uninitialized_dir_without_persisting() {
        let dir = scratch_dir("force-uninitialized");
        std::fs::create_dir_all(dir.join("state")).unwrap();
        drop(NodeState::open(&dir.join("state")).unwrap());
        let before = std::fs::read(dir.join("state").join("config.state")).unwrap();

        let err = force_single_member(&dir, 5).expect_err("a fresh dir has no config record yet");
        assert!(
            err.to_string().to_lowercase().contains("no durable config record"),
            "unexpected message: {err}"
        );

        let after = std::fs::read(dir.join("state").join("config.state")).unwrap();
        assert_eq!(before, after, "force_single_member must not write anything when it refuses");
        assert!(NodeState::open(&dir.join("state")).unwrap().config_record().is_none());
    }

    /// Fix round 1, Critical 1 regression: the doubly-ahead compounding-crash
    /// window (`recover_config_record`'s own doc: two adoptions durably
    /// persisted before any archive catch-up in the same crash) must refuse,
    /// not silently fall back to an empty seed that would overwrite the
    /// crashed survivor's real membership.
    #[test]
    fn force_refuses_the_doubly_ahead_crash_window_without_persisting() {
        let dir = scratch_dir("force-doubly-ahead");
        let addr: SocketAddr = "127.0.0.1:59931".parse().unwrap();
        std::fs::create_dir_all(dir.join("state")).unwrap();
        // durable == 0 on a fresh, empty journal; both `position` and
        // `prev_position` sit above that.
        let cfg = StoredConfig {
            version: 5,
            voters: vec![stored_member(1, addr)],
            learners: Vec::new(),
            tombstones: Vec::new(),
        };
        let prev_cfg = StoredConfig { version: 4, ..cfg.clone() };
        let rec = ConfigRecord { position: 200, config: cfg, prev_position: 100, prev: prev_cfg };
        NodeState::open(&dir.join("state")).unwrap().store_config_record(&rec).unwrap();
        let before = std::fs::read(dir.join("state").join("config.state")).unwrap();

        let err = force_single_member(&dir, 1)
            .expect_err("must refuse the doubly-ahead crash window, not fall back to a seed");
        assert!(err.to_string().to_lowercase().contains("doubly-ahead"), "unexpected message: {err}");

        let after = std::fs::read(dir.join("state").join("config.state")).unwrap();
        assert_eq!(before, after, "force_single_member must not write anything when it refuses");
    }

    /// Fix round 1, Important 3: the CLI's data-loss statement must be
    /// tested against the exact copy the brief specifies, not just eyeballed.
    #[test]
    fn data_loss_statement_matches_the_exact_wording_the_brief_specifies() {
        let msg = data_loss_statement(2, 128, &[0, 1]);
        assert_eq!(
            msg,
            "forcing node 2 to a single-member cluster at durable position 128: any write \
             acknowledged by the old quorum but not held in this node's journal is LOST; peers \
             [0, 1] are dropped from the config and must be wiped and rejoined as fresh learners."
        );
    }

    /// Fix round 1, Important 6: `plan_force_single_member` + `commit` must
    /// produce the exact same result as the one-shot `force_single_member`
    /// (the latter is now just `plan(...)?.commit()`), and the plan's own
    /// reported `durable`/`dropped_peers` must match what ends up in the
    /// committed `ForceReport`.
    #[test]
    fn plan_then_commit_matches_the_one_shot_force() {
        let dir = scratch_dir("plan-then-commit");
        let addrs: Vec<SocketAddr> =
            (0..3).map(|_| "127.0.0.1:0".parse().unwrap()).collect::<Vec<_>>();
        let voters: Vec<(u32, SocketAddr)> = (0..3u32).map(|i| (i, addrs[i as usize])).collect();
        seed_config(&dir, 7, &voters, Vec::new());

        let planned = plan_force_single_member(&dir, 1).expect("plan");
        assert_eq!(planned.durable(), 0);
        let mut dropped = planned.dropped_peers().to_vec();
        dropped.sort_unstable();
        assert_eq!(dropped, vec![0, 2]);
        let statement = planned.data_loss_statement();
        assert!(statement.starts_with("forcing node 1 to a single-member cluster"));

        let report = planned.commit().expect("commit");
        assert_eq!(report.old_version, 7);
        assert_eq!(report.new_version, 8);
    }
}
