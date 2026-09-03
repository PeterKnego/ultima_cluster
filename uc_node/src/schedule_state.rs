// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The node's durable schedule-table record and the staged-file digest
//! (time-and-timers spec §5, plan 2).
//!
//! Two small, independent pieces:
//!
//! * [`ScheduleRecord`] + [`open`]/[`load`]/[`store`] — `state/schedules.state`,
//!   a rotating two-slot [`StableValue`] holding the newest ADOPTED table
//!   (its frame-END position, the frame's stamp, and the encoded bytes
//!   verbatim). It is a CACHE with one job: let a restarted node re-arm its
//!   heaps before the log has replayed anything, so a `DailyAt` entry does
//!   not go silent for a day after a bounce. It is deliberately NOT part of
//!   [`crate::backup`]'s `STATE_FILES` set — an artifact taken before this
//!   feature existed must stay valid, and the backup/restore path copies the
//!   whole `state/` directory anyway, so the record travels with a backup by
//!   construction.
//! * [`schedule_digest`] — the ten bytes of the staged file's SHA-256 that
//!   ride an `ADMIN_OP_SCHEDULE_APPLY` request's `(id, ip, port)` fields.
//!   The table itself is far too large for the admin line, so `uc2ctl`
//!   stages it as `<instance_dir>/schedules.pending` and SIGNS its digest;
//!   the node reads the file back and refuses unless the digest matches, so
//!   the file the operator signed is the file the cluster adopts.
//!
//! `pub` (and re-exported from the crate root) because `uc2ctl` — which
//! already depends on `uc_node` — must compute the identical digest and read
//! the same record back for `schedule show`. It stays here rather than in
//! `uc_protocol`: that crate is a `core`-friendly, dependency-light leaf with
//! no `sha2` and no `serde`-derived state records.

use std::path::Path;

use serde::{Deserialize, Serialize};
use uc_journal::{StableValue, StableValueConfig, StableValueError};
use uc_protocol::v2::schedule::{ScheduleTable, encode_schedule_table};

/// The staged table file an admin client writes under the instance directory
/// before sending `ADMIN_OP_SCHEDULE_APPLY`. Relative to `<instance_dir>`,
/// NOT to `state/` — it is a request payload, not durable node state, and the
/// node deletes it once the table is appended.
pub const SCHEDULE_PENDING_FILE: &str = "schedules.pending";

/// The durable record's file name under `<instance_dir>/state/`.
pub const SCHEDULE_STATE_FILE: &str = "schedules.state";

/// The newest schedule table this node has ADOPTED. Mirrors
/// [`uc_log::state::ConfigRecord`]'s shape and discipline (persist before the
/// in-memory effect), but lives in its own `StableValue` rather than inside
/// `NodeState`: the record is optional, node-local, and irrelevant to
/// consensus safety, and `NodeState`'s single cache lock is on the consensus
/// hot path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleRecord {
    /// Frame-END position of the `FRAME_TYPE_SCHEDULE_TABLE` frame that
    /// carried this table. The idempotency key: a re-observed frame at or
    /// below this is a no-op.
    pub position: u64,
    /// The frame's log-time stamp. Recorded for diagnostics only — boot
    /// arming reads the LOG's clock (`cnc.log_time_ns()`), never this, so a
    /// long downtime catches up by one tick instead of replaying a backlog.
    pub time_ns: u64,
    /// The encoded table, exactly as it appeared on the wire
    /// (`uc_protocol::v2::schedule::encode_schedule_table`). Kept as bytes so
    /// the record's format is the frozen wire format and nothing here can
    /// drift from it.
    pub table: Vec<u8>,
    /// ONE level of history — the record this one superseded, with its own
    /// `prev` cleared. Exactly [`uc_log::state::ConfigRecord`]'s discipline
    /// and for the same reason: a table frame that a truncation drops must
    /// revert to a predecessor rather than survive claiming a position the
    /// log no longer backs, and one level suffices because a new table is
    /// only appliable once the previous one has COMMITTED (the
    /// single-in-flight rule in `Consensus::apply_schedule_table`), and
    /// committed frames are never truncated — so at most one table frame is
    /// ever truncation-exposed.
    pub prev: Option<Box<ScheduleRecord>>,
}

impl ScheduleRecord {
    /// The predecessor this record superseded, its own history exhausted
    /// (nothing below a reverted record is recoverable). `None` when there is
    /// no predecessor — the node then holds no table at all, for which
    /// [`ScheduleRecord::empty`] is the durable representation.
    pub fn reverted(self) -> Option<ScheduleRecord> {
        self.prev.map(|p| ScheduleRecord { prev: None, ..*p })
    }

    /// The canonical "no table adopted" record: position 0, stamp 0, and an
    /// encoded EMPTY table. A `StableValue` cannot be cleared, so this is
    /// what a revert with no predecessor stores — and boot arming then
    /// disarms every row (an empty entry set per row) instead of re-arming a
    /// table the log no longer holds.
    pub fn empty() -> ScheduleRecord {
        let mut table = Vec::new();
        encode_schedule_table(
            &ScheduleTable {
                entries: Vec::new(),
            },
            &mut table,
        );
        ScheduleRecord {
            position: 0,
            time_ns: 0,
            table,
            prev: None,
        }
    }
}

/// Open (create if absent) `<dir>/schedules.state`, where `dir` is the
/// instance directory's `state/` subdirectory.
pub fn open(dir: &Path) -> Result<StableValue<ScheduleRecord>, StableValueError> {
    StableValue::open(StableValueConfig::new(dir.join(SCHEDULE_STATE_FILE)))
}

/// The last stored record, or `None` on a node that has never adopted a
/// table (including a fresh instance directory).
pub fn load(sv: &StableValue<ScheduleRecord>) -> Result<Option<ScheduleRecord>, StableValueError> {
    sv.load()
}

/// Read `<instance_dir>/state/schedules.state` directly, for a caller (`uc2ctl
/// schedule show`, `uc2ctl status`'s `schedule_position=`) that only needs
/// one snapshot of the record rather than a `StableValue` to keep open.
/// `instance_dir` is the INSTANCE directory (not its `state/` subdirectory —
/// this joins it), matching every other `uc2ctl`-facing path in this crate.
///
/// Safe beside a running node: `StableValue::open` takes no file lock and
/// caches both slots at open, so this is a plain read, not a contended
/// attach. Deliberately NOT `uc_node::backup::open_state_readonly` — that
/// helper is private (not even `pub(crate)`) and scoped to the
/// backup/restore artifact path, not a general read of a live instance
/// directory's `state/`.
pub fn read_record(instance_dir: &Path) -> Result<Option<ScheduleRecord>, StableValueError> {
    let sv = open(&instance_dir.join("state"))?;
    load(&sv)
}

/// Durable on return, exactly like `NodeState::store_config_record` — the
/// caller may take the in-memory effect only after this returns `Ok`.
pub fn store(sv: &StableValue<ScheduleRecord>, r: &ScheduleRecord) -> Result<(), StableValueError> {
    sv.store(r)?
        .wait()
        // `Notifier::wait` yields a `JournalError`; `StableValue`'s own error
        // type has no variant for it (same seam `uc_log::state` bridges).
        .map_err(|e| StableValueError::Io(std::io::Error::other(e.to_string())))?;
    Ok(())
}

/// The first TEN bytes of SHA-256 over `bytes`, read little-endian as an
/// admin request's `(id, ip, port)` fields — 80 bits of collision resistance
/// against an operator staging one file and signing another, which is all
/// those three fields have room for.
///
/// FROZEN: `uc2ctl` computes this over the file it stages and the node
/// recomputes it over the file it read. Changing the byte selection or the
/// endianness makes every apply refuse with
/// [`crate::node::REASON_SCHEDULE_DIGEST`].
pub fn schedule_digest(bytes: &[u8]) -> (u32, u32, u16) {
    use sha2::{Digest, Sha256};
    let h = Sha256::digest(bytes);
    (
        u32::from_le_bytes(h[0..4].try_into().expect("4 bytes")),
        u32::from_le_bytes(h[4..8].try_into().expect("4 bytes")),
        u16::from_le_bytes(h[8..10].try_into().expect("2 bytes")),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scratch directory on REAL DISK, never `/tmp` (RAM-backed tmpfs with
    /// no swap on the dev box — CLAUDE.md). `CARGO_TARGET_TMPDIR` is set only
    /// for integration-test binaries and these are inline unit tests in the
    /// lib target, so this falls back to a package-relative `target/`
    /// directory — the same helper shape `audit.rs`'s tests use.
    fn tempdir() -> tempfile::TempDir {
        let root = std::env::var("CARGO_TARGET_TMPDIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_node_tests")
            });
        assert!(
            !root.starts_with("/tmp"),
            "test scratch must not live on tmpfs: {}",
            root.display()
        );
        std::fs::create_dir_all(&root).expect("scratch root");
        tempfile::Builder::new()
            .prefix("uc2-sched-")
            .tempdir_in(&root)
            .expect("tempdir")
    }

    /// FROZEN: the digest is the first TEN bytes of SHA-256 over the staged
    /// bytes, read little-endian as the admin request's `(id, ip, port)`
    /// fields. Pinned against the canonical `SHA-256("abc")` vector
    /// `ba7816bf 8f01cfea 414140de 5dae2223 …`, so `uc2ctl` and the node can
    /// never drift: they must compute the same three numbers or every apply
    /// is refused with `REASON_SCHEDULE_DIGEST`.
    #[test]
    fn digest_is_the_first_ten_bytes_of_sha256_le() {
        let (id, ip, port) = schedule_digest(b"abc");
        assert_eq!(id, 0xbf16_78ba, "bytes 0..4 LE");
        assert_eq!(ip, 0xeacf_018f, "bytes 4..8 LE");
        assert_eq!(port, 0x4141, "bytes 8..10 LE");
        // Any other bytes give a different triple (the whole point).
        assert_ne!(schedule_digest(b"abd"), (id, ip, port));
        assert_ne!(schedule_digest(b""), (id, ip, port));
    }

    #[test]
    fn record_roundtrips_through_a_stable_value() {
        let dir = tempdir();
        let sv = open(dir.path()).expect("open");
        assert_eq!(load(&sv).expect("load"), None, "nothing stored yet");
        let rec = ScheduleRecord {
            position: 4096,
            time_ns: 1_767_225_600_000_000_000,
            table: vec![1, 2, 3, 4, 5],
            prev: Some(Box::new(ScheduleRecord {
                position: 2048,
                time_ns: 1_767_225_500_000_000_000,
                table: vec![9, 9],
                prev: None,
            })),
        };
        store(&sv, &rec).expect("store");
        assert_eq!(load(&sv).expect("load"), Some(rec.clone()));
        // A second open of the SAME file recovers it (the boot path).
        drop(sv);
        let sv = open(dir.path()).expect("reopen");
        assert_eq!(load(&sv).expect("load"), Some(rec));
        assert!(dir.path().join(SCHEDULE_STATE_FILE).is_file());
    }

    /// `read_record` is `uc2ctl`'s (`schedule show`, `status`'s
    /// `schedule_position=`) entry point — joins `state/` onto the INSTANCE
    /// dir itself (unlike `open`/`load` above, which already take the
    /// `state/` subdirectory), and works as a plain read beside an open
    /// `StableValue` on the same file (no lock taken).
    #[test]
    fn read_record_reads_the_instance_dirs_state_subdirectory() {
        let dir = tempdir();
        let state_dir = dir.path().join("state");
        std::fs::create_dir_all(&state_dir).expect("state dir");
        assert_eq!(
            read_record(dir.path()).expect("read"),
            None,
            "nothing stored yet"
        );

        let sv = open(&state_dir).expect("open");
        let rec = ScheduleRecord {
            position: 128,
            time_ns: 9,
            table: vec![1, 2, 3],
            prev: None,
        };
        store(&sv, &rec).expect("store");
        // read_record works alongside the still-open StableValue, not just
        // after it's dropped — a running node holds this open the whole time.
        assert_eq!(read_record(dir.path()).expect("read"), Some(rec));
    }

    /// The one-level revert a truncation runs (plan-2 fix round 1): prev is
    /// promoted, its own history is exhausted, and a record with no prev
    /// reverts to nothing — for which `empty()` is the durable stand-in
    /// (`StableValue` has no clear), and which decodes as a table with zero
    /// entries so every row disarms.
    #[test]
    fn reverted_promotes_one_level_and_exhausts_the_history() {
        let older = ScheduleRecord {
            position: 2048,
            time_ns: 7,
            table: vec![9, 9],
            prev: None,
        };
        let newer = ScheduleRecord {
            position: 4096,
            time_ns: 9,
            table: vec![1],
            prev: Some(Box::new(older.clone())),
        };
        let back = newer.reverted().expect("one level of history");
        assert_eq!(back, older, "prev promoted verbatim, its own prev cleared");
        assert_eq!(back.clone().reverted(), None, "history exhausted");

        let e = ScheduleRecord::empty();
        assert_eq!((e.position, e.time_ns, e.prev.clone()), (0, 0, None));
        let table = uc_protocol::v2::schedule::decode_schedule_table(&e.table)
            .expect("the empty record's bytes are a decodable table");
        assert!(table.entries.is_empty(), "zero entries: every row disarms");
    }
}
