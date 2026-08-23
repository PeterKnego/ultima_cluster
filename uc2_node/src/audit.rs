// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M12b (spec §5.3): the admin audit log — `<instance_dir>/audit.jsonl`.
//!
//! Every admin request the node answers is recorded here **before** its
//! answer is published on the cnc response line, so an operator reading the
//! file after the fact sees every decision the node made, including the ones
//! it refused. One `\n`-terminated JSON object per line, `O_APPEND`, one
//! `sync_data` per record:
//!
//! ```json
//! {"ts_ns":1755600000000000000,"event":"admin_op","actor":"ops-alice","origin":"local","op":1,"op_name":"add_learner","id":4,"addr":"10.0.0.4:9100","seq":12,"nonce":880,"outcome":"accepted","reason":0,"config_version":7}
//! ```
//!
//! Key order is fixed and is a contract, exactly as for
//! [`crate::obs::log`] — the two share one formatter
//! ([`crate::obs::log::format_line_at`]), so the audit line and the `info`
//! mirror this module emits to the obs sink can never drift in escaping or
//! field order. The audit line carries no `"level"` key (a `level` on a
//! record that is always the same severity is noise); the obs mirror does.
//!
//! **`accepted` means proposed and appended, not committed.** For a request
//! the leader accepts, the record is written after `propose_config` +
//! `append_config_frame` have already put the config change in the log, and
//! before the response line is published. The record therefore precedes the
//! commit of the change it describes. That ordering is deliberate: the
//! guarantee this file offers is "no answer was given that is not on disk
//! here", not "everything here reached quorum". A change that was appended
//! and then truncated away by a later leader appears here as `accepted`; the
//! `config_adopted` obs event and `uc2ctl status` are what report the
//! committed world.
//!
//! **No rotation.** Admin operations are operator-rate — a busy cluster
//! makes tens of them a year, and each record is ~200 bytes. The file is
//! append-only for the life of the instance directory; an operator who wants
//! it truncated does so deliberately, offline. See
//! `docs/reference/instance-directory.md`.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

use crate::obs::log::{Field, FieldValue, LogLevel, format_line_at};

/// The file name inside the instance directory.
pub const AUDIT_FILE: &str = "audit.jsonl";

/// What the node decided about an admin request. Mirrors the wire `status`
/// on the cnc admin response line: `Accepted` = 0, `Refused` = 1 (carrying
/// the wire `reason`), `Retry` = 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditOutcome {
    /// Proposed and appended (see the module doc: not necessarily committed).
    Accepted,
    /// Refused, with the wire `reason` code the caller was given.
    Refused(u32),
    /// Answered `retry` — no leader known, a superseded forward, a role
    /// change under an in-flight forward, or a momentarily full ring.
    Retry,
}

impl AuditOutcome {
    fn as_str(self) -> &'static str {
        match self {
            AuditOutcome::Accepted => "accepted",
            AuditOutcome::Refused(_) => "refused",
            AuditOutcome::Retry => "retry",
        }
    }

    /// The wire `reason` that accompanies this outcome (0 unless refused).
    pub fn reason(self) -> u32 {
        match self {
            AuditOutcome::Refused(r) => r,
            _ => 0,
        }
    }

    /// Build the outcome from the wire `(status, reason)` pair the node is
    /// about to publish, so the record can never disagree with the answer.
    pub fn from_wire(status: u32, reason: u32) -> AuditOutcome {
        match status {
            0 => AuditOutcome::Accepted,
            2 => AuditOutcome::Retry,
            _ => AuditOutcome::Refused(reason),
        }
    }
}

/// Where the request the node is answering came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditOrigin {
    /// Written into this node's own cnc admin band by an admin client.
    Local,
    /// Forwarded to this (leader) node by a peer as a `ConfigProposal`
    /// datagram — the peer already authenticated it locally.
    Forwarded,
}

impl AuditOrigin {
    fn as_str(self) -> &'static str {
        match self {
            AuditOrigin::Local => "local",
            AuditOrigin::Forwarded => "forwarded",
        }
    }
}

/// The wire op codes, as `wire_to_config_op` decodes them. An op outside
/// 1..=5 is recorded as `"unknown"` rather than dropped — a malformed
/// request is exactly the kind of thing an audit log exists to keep.
pub fn op_name(op: u32) -> &'static str {
    match op {
        1 => "add_learner",
        2 => "promote",
        3 => "demote",
        4 => "remove_learner",
        5 => "remove_voter",
        _ => "unknown",
    }
}

/// One audit record. Borrowed throughout — a record is formatted and written
/// synchronously at the call site, never queued.
#[derive(Debug)]
pub struct AuditRecord<'a> {
    /// Unix nanoseconds (`crate::obs::metrics::now_unix_ns`).
    pub ts_ns: u64,
    /// The admin key name that signed the request under
    /// [`uc2_crypto::admin::AdminPolicy::Hmac`]; `"filesystem"` under
    /// `Filesystem` (nothing was authenticated — the directory permissions
    /// were the boundary); `"peer:<id>"` for a proposal a peer forwarded.
    pub actor: &'a str,
    pub origin: AuditOrigin,
    /// The raw wire op code.
    pub op: u32,
    /// `op_name(op)`.
    pub op_name: &'static str,
    /// The target node id.
    pub id: u32,
    /// The target address as `(ip, port)`, rendered `"ip:port"`; `None`
    /// (rendered `null`) for the ops that carry no address.
    pub addr: Option<(u32, u16)>,
    /// The admin band sequence number this answer belongs to. `0` on a
    /// forwarded proposal recorded at the leader: `seq` is the *requesting*
    /// node's local band sequence and the wire proposal does not carry it —
    /// the `nonce` is what ties the two records together.
    pub seq: u64,
    pub nonce: u64,
    pub outcome: AuditOutcome,
    /// The wire `reason` published with the answer.
    pub reason: u32,
    /// The config version published with the answer (the new one on
    /// `accepted`, the current one otherwise).
    pub config_version: u64,
}

/// The append-only admin audit file. Opened once at node start and owned by
/// the consensus agent (the only writer).
#[derive(Debug)]
pub struct AuditLog {
    file: File,
    path: PathBuf,
}

impl AuditLog {
    /// Open (creating if absent) `<instance_dir>/audit.jsonl` for append.
    /// Never truncates: a restart continues the same file.
    pub fn open(instance_dir: &Path) -> std::io::Result<AuditLog> {
        let path = instance_dir.join(AUDIT_FILE);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(AuditLog { file, path })
    }

    /// The file's path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one record and `sync_data` it, then mirror it to the obs sink
    /// at `info`. Returns `Err` if the record did not reach the disk — the
    /// caller must then refuse the request rather than answer it unrecorded
    /// (see `REASON_AUDIT_FAILED`).
    ///
    /// The mirror is emitted only on a successful write, and from the same
    /// field slice as the file line, so "there is an `admin_op` line in the
    /// log" always implies "there is an `admin_op` record on disk".
    pub fn record(&mut self, r: &AuditRecord<'_>) -> std::io::Result<()> {
        debug_assert!(
            r.outcome.reason() == 0 || r.outcome.reason() == r.reason,
            "audit outcome {:?} disagrees with wire reason {}",
            r.outcome,
            r.reason
        );
        let addr = r.addr.map(|(ip, port)| format!("{}:{}", Ipv4Addr::from(ip), port));
        let fields = fields(r, addr.as_deref());
        let line = format_line_at(r.ts_ns as u128, None, "admin_op", &fields);
        // One `write_all` under `O_APPEND`: the offset is taken by the kernel
        // at write time, so records from a concurrent appender (there is
        // none today — the consensus agent is the only writer — but a
        // `uc2ctl` verb could become one) interleave whole, never mid-line.
        // A write that FAILS partway can still leave a line without its
        // trailing `\n`; that is the recognizable signature of the failure
        // this `?` reports, and the request it belongs to is refused rather
        // than answered. A reader should treat a final line with no newline
        // as truncated.
        self.file.write_all(line.as_bytes())?;
        self.file.sync_data()?;
        crate::obs::log::emit(LogLevel::Info, "admin_op", &fields);
        Ok(())
    }
}

/// The record's fields, in the one order both the file line and the obs
/// mirror use. `addr` is `null` when the op carries no address.
fn fields<'a>(r: &'a AuditRecord<'a>, addr: Option<&'a str>) -> [Field<'a>; 11] {
    [
        Field { key: "actor", value: FieldValue::Str(r.actor) },
        Field { key: "origin", value: FieldValue::Str(r.origin.as_str()) },
        Field { key: "op", value: FieldValue::U64(r.op as u64) },
        Field { key: "op_name", value: FieldValue::Str(r.op_name) },
        Field { key: "id", value: FieldValue::U64(r.id as u64) },
        Field {
            key: "addr",
            value: match addr {
                Some(s) => FieldValue::Str(s),
                None => FieldValue::Null,
            },
        },
        Field { key: "seq", value: FieldValue::U64(r.seq) },
        Field { key: "nonce", value: FieldValue::U64(r.nonce) },
        Field { key: "outcome", value: FieldValue::Str(r.outcome.as_str()) },
        Field { key: "reason", value: FieldValue::U64(r.reason as u64) },
        Field { key: "config_version", value: FieldValue::U64(r.config_version) },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `tempfile::tempdir()` (the in-crate unit-test convention;
    /// `CARGO_TARGET_TMPDIR` exists only for integration tests). Safe against
    /// the "no heavy artifacts on tmpfs" rule: every file here is a handful
    /// of ~200-byte lines.
    fn tempdir() -> tempfile::TempDir {
        tempfile::Builder::new().prefix("uc2-audit-").tempdir().expect("tempdir")
    }

    fn rec(seq: u64) -> AuditRecord<'static> {
        AuditRecord {
            ts_ns: 1_755_600_000_000_000_000,
            actor: "ops-alice",
            origin: AuditOrigin::Local,
            op: 1,
            op_name: op_name(1),
            id: 4,
            addr: Some((u32::from_be_bytes([10, 0, 0, 4]), 9100)),
            seq,
            nonce: 880,
            outcome: AuditOutcome::Accepted,
            reason: 0,
            config_version: 7,
        }
    }

    #[test]
    fn the_line_is_byte_exact_for_a_known_record() {
        let dir = tempdir();
        let mut a = AuditLog::open(dir.path()).unwrap();
        a.record(&rec(12)).unwrap();
        let text = std::fs::read_to_string(a.path()).unwrap();
        assert_eq!(
            text,
            "{\"ts_ns\":1755600000000000000,\"event\":\"admin_op\",\"actor\":\"ops-alice\",\
             \"origin\":\"local\",\"op\":1,\"op_name\":\"add_learner\",\"id\":4,\
             \"addr\":\"10.0.0.4:9100\",\"seq\":12,\"nonce\":880,\"outcome\":\"accepted\",\
             \"reason\":0,\"config_version\":7}\n"
        );
    }

    #[test]
    fn an_addressless_op_records_a_null_addr_and_its_refusal_reason() {
        let dir = tempdir();
        let mut a = AuditLog::open(dir.path()).unwrap();
        let mut r = rec(3);
        r.op = 3;
        r.op_name = op_name(3);
        r.addr = None;
        r.outcome = AuditOutcome::Refused(20);
        r.reason = 20;
        a.record(&r).unwrap();
        let text = std::fs::read_to_string(a.path()).unwrap();
        assert!(text.contains(r#""op":3,"op_name":"demote","id":4,"addr":null"#), "{text}");
        assert!(text.contains(r#""outcome":"refused","reason":20"#), "{text}");
    }

    #[test]
    fn opening_twice_appends_rather_than_truncating() {
        let dir = tempdir();
        {
            let mut a = AuditLog::open(dir.path()).unwrap();
            a.record(&rec(1)).unwrap();
        }
        {
            let mut a = AuditLog::open(dir.path()).unwrap();
            a.record(&rec(2)).unwrap();
        }
        let text = std::fs::read_to_string(dir.path().join(AUDIT_FILE)).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "the second open truncated the file: {text}");
        assert!(lines[0].contains(r#""seq":1"#), "{text}");
        assert!(lines[1].contains(r#""seq":2"#), "{text}");
    }

    #[test]
    fn unknown_ops_are_recorded_not_dropped() {
        assert_eq!(op_name(0), "unknown");
        assert_eq!(op_name(6), "unknown");
        assert_eq!(op_name(2), "promote");
        assert_eq!(op_name(4), "remove_learner");
        assert_eq!(op_name(5), "remove_voter");
    }

    #[test]
    fn outcomes_come_from_the_wire_answer() {
        assert_eq!(AuditOutcome::from_wire(0, 0), AuditOutcome::Accepted);
        assert_eq!(AuditOutcome::from_wire(1, 20), AuditOutcome::Refused(20));
        assert_eq!(AuditOutcome::from_wire(2, 0), AuditOutcome::Retry);
        assert_eq!(AuditOutcome::from_wire(1, 20).reason(), 20);
        assert_eq!(AuditOutcome::Retry.reason(), 0);
    }

    /// A record that cannot reach the disk must surface as `Err`, so the
    /// caller refuses the request rather than answering it unrecorded. The
    /// failure is forced with a read-only descriptor: an unwritable
    /// *directory* would only break `open`, since an already-open append fd
    /// keeps working however the directory's mode later changes.
    #[test]
    fn a_record_that_cannot_be_written_is_an_error() {
        let dir = tempdir();
        let path = dir.path().join(AUDIT_FILE);
        std::fs::write(&path, b"").unwrap();
        let mut a = AuditLog { file: File::open(&path).unwrap(), path: path.clone() };
        let err = a.record(&rec(1)).expect_err("a read-only fd must not silently swallow a record");
        assert!(
            matches!(err.kind(), std::io::ErrorKind::Other | std::io::ErrorKind::PermissionDenied)
                || err.raw_os_error() == Some(9),
            "unexpected error: {err:?}"
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "", "nothing was recorded");
    }

    /// `open` on a directory this process cannot write is a hard error — the
    /// node refuses to start rather than run unauditable. Skipped as root,
    /// which ignores the mode bits.
    #[test]
    fn opening_in_an_unwritable_directory_fails() {
        if unsafe { libc::geteuid() } == 0 {
            eprintln!("skipping: running as root, mode bits do not apply");
            return;
        }
        let dir = tempdir();
        let sub = dir.path().join("ro");
        std::fs::create_dir(&sub).unwrap();
        let mut perms = std::fs::metadata(&sub).unwrap().permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            perms.set_mode(0o500);
        }
        std::fs::set_permissions(&sub, perms.clone()).unwrap();
        let r = AuditLog::open(&sub);
        // Restore before asserting so the tempdir can always be cleaned up.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut p = perms;
            p.set_mode(0o700);
            std::fs::set_permissions(&sub, p).unwrap();
        }
        assert!(r.is_err(), "an unwritable instance directory must refuse to open the audit log");
    }
}
