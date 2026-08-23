// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Structured JSON-lines log core.
//!
//! One record per line, `\n`-terminated, each line valid JSON:
//!
//! ```json
//! {"ts_ns":1755600000000000000,"level":"info","event":"became_leader","node":0,"term":3}
//! ```
//!
//! `ts_ns` is unix nanoseconds from [`SystemTime::now`]. Keys appear in a
//! fixed order: `ts_ns`, `level`, `event`, then the caller's fields in call
//! order — this is a machine log, not a display format, so key order is a
//! stable contract rather than a cosmetic choice. Use [`crate::obs_event!`]
//! at call sites rather than [`emit`] directly; it turns bare key = value
//! pairs into [`Field`]s without an intermediate allocation.
//!
//! [`format_line_at`] is the formatter both this module and the M12b admin
//! audit log ([`crate::audit`]) render through. `admin_op` — one record per
//! admin request the node answers, carrying `actor`, `origin`, `op`,
//! `op_name`, `id`, `addr`, `seq`, `nonce`, `outcome`, `reason` and
//! `config_version` — is emitted here at `info` as the *mirror* of the line
//! `<instance_dir>/audit.jsonl` already holds; the file, not this stream, is
//! the record of record.

use std::fmt;
use std::io::Write;
use std::str::FromStr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::SystemTime;

/// Log severity, ordered `Error < Warn < Info` so a numerically higher level
/// is "more verbose"; [`emit`] filters a call out when its level is
/// numerically greater than the current global [`level`].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub enum LogLevel {
    Error = 0,
    Warn = 1,
    #[default]
    Info = 2,
}

impl LogLevel {
    fn as_json_str(self) -> &'static str {
        match self {
            LogLevel::Error => "error",
            LogLevel::Warn => "warn",
            LogLevel::Info => "info",
        }
    }

    fn from_u8(v: u8) -> LogLevel {
        match v {
            0 => LogLevel::Error,
            1 => LogLevel::Warn,
            _ => LogLevel::Info,
        }
    }
}

impl FromStr for LogLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "error" => Ok(LogLevel::Error),
            "warn" => Ok(LogLevel::Warn),
            "info" => Ok(LogLevel::Info),
            _ => Err(format!("log.level must be one of error|warn|info, got \"{s}\"")),
        }
    }
}

/// Process-wide level filter. `Relaxed` is sufficient: this gates whether a
/// record is formatted at all, not any data the record carries, so a stale
/// read only costs (or saves) one log line around a concurrent `set_level`.
static LEVEL: AtomicU8 = AtomicU8::new(LogLevel::Info as u8);

/// Set the global level filter; [`emit`] calls at a numerically higher
/// (more verbose) level than this are dropped before formatting.
pub fn set_level(l: LogLevel) {
    LEVEL.store(l as u8, Ordering::Relaxed);
}

/// The current global level filter.
pub fn level() -> LogLevel {
    LogLevel::from_u8(LEVEL.load(Ordering::Relaxed))
}

/// One field's value. `Str` is JSON-escaped; the numeric and bool variants
/// are written verbatim (JSON numbers/booleans need no escaping).
pub enum FieldValue<'a> {
    U64(u64),
    I64(i64),
    Bool(bool),
    /// JSON `null` — a field that is structurally present but has no value
    /// for this record (the audit log's `addr` on an op that carries none).
    /// Has no `From` impl on purpose: [`crate::obs_event!`] call sites pass
    /// values, and a null is built explicitly.
    Null,
    Str(&'a str),
}

impl From<u64> for FieldValue<'_> {
    fn from(v: u64) -> Self {
        FieldValue::U64(v)
    }
}

impl From<i64> for FieldValue<'_> {
    fn from(v: i64) -> Self {
        FieldValue::I64(v)
    }
}

impl From<bool> for FieldValue<'_> {
    fn from(v: bool) -> Self {
        FieldValue::Bool(v)
    }
}

impl<'a> From<&'a str> for FieldValue<'a> {
    fn from(v: &'a str) -> Self {
        FieldValue::Str(v)
    }
}

/// One `key: value` pair attached to a record, in the order it should
/// appear on the line. `key` is `&'static str` — call sites pass literals
/// (via [`crate::obs_event!`]'s `stringify!`), never a runtime string.
pub struct Field<'a> {
    pub key: &'static str,
    pub value: FieldValue<'a>,
}

enum Sink {
    Stderr,
    Capture(Arc<Mutex<Vec<u8>>>),
}

static SINK: OnceLock<Mutex<Sink>> = OnceLock::new();

fn sink() -> &'static Mutex<Sink> {
    SINK.get_or_init(|| Mutex::new(Sink::Stderr))
}

/// Test-only: swap the sink to an in-memory buffer and return it. Records
/// emitted after this call accumulate in the returned buffer instead of
/// going to stderr, until [`stderr_for_tests`] swaps the sink back.
pub fn capture_for_tests() -> Arc<Mutex<Vec<u8>>> {
    let buf = Arc::new(Mutex::new(Vec::new()));
    *sink().lock().unwrap() = Sink::Capture(buf.clone());
    buf
}

/// Test-only: swap the sink back to stderr.
pub fn stderr_for_tests() {
    *sink().lock().unwrap() = Sink::Stderr;
}

/// Append `s`, JSON-escaped, to `out` — without the surrounding quotes.
/// Escapes `"`, `\`, and control characters below `0x20` (as `\u00XX`,
/// lowercase hex, four digits); every other byte passes through unchanged.
fn push_json_escaped(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => {
                use fmt::Write as _;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
}

/// Format one record as its `\n`-terminated JSON line, without emitting it.
/// `ts_ns` is the caller's timestamp and `level` is optional — a record with
/// no level renders `{"ts_ns":…,"event":…}` with the level key absent
/// entirely.
///
/// This is the one formatter in the crate: [`emit`] renders through it, and
/// so does [`crate::audit::AuditLog::record`], so the JSON escaping and the
/// key-order contract cannot drift between the log stream and the audit file.
pub(crate) fn format_line_at(
    ts_ns: u128,
    level: Option<LogLevel>,
    event: &'static str,
    fields: &[Field<'_>],
) -> String {
    let mut line = String::with_capacity(128);
    line.push_str(r#"{"ts_ns":"#);
    line.push_str(&ts_ns.to_string());
    if let Some(level) = level {
        line.push_str(r#","level":""#);
        line.push_str(level.as_json_str());
        line.push('"');
    }
    line.push_str(r#","event":""#);
    push_json_escaped(&mut line, event);
    line.push('"');
    for field in fields {
        line.push(',');
        line.push('"');
        line.push_str(field.key);
        line.push_str("\":");
        match &field.value {
            FieldValue::U64(v) => line.push_str(&v.to_string()),
            FieldValue::I64(v) => line.push_str(&v.to_string()),
            FieldValue::Bool(v) => line.push_str(if *v { "true" } else { "false" }),
            FieldValue::Null => line.push_str("null"),
            FieldValue::Str(v) => {
                line.push('"');
                push_json_escaped(&mut line, v);
                line.push('"');
            }
        }
    }
    line.push('}');
    line.push('\n');
    line
}

/// Emit one structured log record, subject to the global [`level`] filter.
/// Prefer [`crate::obs_event!`] at call sites over calling this directly.
pub fn emit(level: LogLevel, event: &'static str, fields: &[Field<'_>]) {
    if level > self::level() {
        return;
    }
    let ts_ns = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let line = format_line_at(ts_ns, Some(level), event, fields);

    match &mut *sink().lock().unwrap() {
        Sink::Stderr => {
            let _ = std::io::stderr().lock().write_all(line.as_bytes());
        }
        Sink::Capture(buf) => buf.lock().unwrap().extend_from_slice(line.as_bytes()),
    }
}

/// Emit a structured log record: `obs_event!(Info, "became_leader", node =
/// id as u64, term = term as u64)`. Values are `u64`, `i64`, `bool`, or
/// `&str`, dispatched to [`FieldValue`] by `From`; keys become `&'static
/// str` via `stringify!`, so they must be identifiers, not expressions.
#[macro_export]
macro_rules! obs_event {
    ($lvl:ident, $event:expr $(, $key:ident = $val:expr)* $(,)?) => {
        $crate::obs::log::emit(
            $crate::obs::log::LogLevel::$lvl,
            $event,
            &[$($crate::obs::log::Field {
                key: stringify!($key),
                value: $crate::obs::log::FieldValue::from($val),
            }),*],
        )
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn a_record_is_one_valid_json_line_with_fields_in_order() {
        let _g = TEST_LOCK.lock().unwrap();
        let buf = capture_for_tests();
        emit(LogLevel::Info, "became_leader", &[
            Field { key: "node", value: FieldValue::U64(0) },
            Field { key: "term", value: FieldValue::U64(3) },
        ]);
        let s = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(s.ends_with('\n'));
        assert!(s.contains(r#""level":"info","event":"became_leader","node":0,"term":3}"#), "{s}");
        assert!(s.starts_with(r#"{"ts_ns":"#));
        stderr_for_tests();
    }

    #[test]
    fn the_level_filter_suppresses_below_threshold() {
        let _g = TEST_LOCK.lock().unwrap();
        let buf = capture_for_tests();
        set_level(LogLevel::Error);
        emit(LogLevel::Info, "noise", &[]);
        // Not an emptiness assertion: the capture buffer is a process-global
        // sink and `TEST_LOCK` only serializes THIS file's own tests, so a
        // concurrent node-test emission could land a line here and make an
        // is_empty() check flaky. Assert on content instead: the suppressed
        // event specifically must not appear.
        {
            let s = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
            assert!(!s.contains(r#""event":"noise""#), "{s}");
        }
        emit(LogLevel::Error, "kept", &[]);
        {
            let s = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
            assert!(s.contains(r#""event":"kept""#), "{s}");
        }
        set_level(LogLevel::Info);
        stderr_for_tests();
    }

    #[test]
    fn string_values_are_escaped() {
        let _g = TEST_LOCK.lock().unwrap();
        let buf = capture_for_tests();
        emit(LogLevel::Warn, "e", &[Field { key: "msg", value: FieldValue::Str("a\"b\\c\nd") }]);
        let s = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(s.contains("\"msg\":\"a\\\"b\\\\c\\u000ad\""), "{s}");
        stderr_for_tests();
    }

    #[test]
    fn the_macro_expands_to_emit() {
        let _g = TEST_LOCK.lock().unwrap();
        let buf = capture_for_tests();
        crate::obs_event!(Info, "config_adopted", node = 1u64, version = 7u64, pending = false);
        let s = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(s.contains(r#""event":"config_adopted","node":1,"version":7,"pending":false"#), "{s}");
        stderr_for_tests();
    }
}
