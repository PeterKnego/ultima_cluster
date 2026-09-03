// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc2ctl schedule apply <file.toml>` / `uc2ctl schedule show`
//! (time-and-timers plan 2, spec §5).
//!
//! `apply` is a three-step flow: parse the operator's TOML into a
//! [`uc_protocol::v2::schedule::ScheduleTable`] (resolving each `fsm = "…"`
//! name to its identity hash via the node's own cnc name lines, so a typo'd
//! or stale FSM name is refused HERE, before anything is staged); encode it
//! and stage it as `<instance_dir>/schedules.pending` (temp name, fsync,
//! rename — so the node never reads a half-written file); then send
//! `ADMIN_OP_SCHEDULE_APPLY` through the same signed-request channel every
//! other `uc2ctl` mutating verb uses, carrying the staged file's
//! [`uc_node::schedule_digest`] in the request's `(id, ip, port)` fields —
//! the node recomputes the identical digest over the file IT reads back, so
//! the file an operator signed is the file the cluster adopts (see
//! `uc_node::schedule_state`'s module doc).
//!
//! `show` reads back the newest ADOPTED table from durable node state
//! (`uc_node::read_record`, NOT the staged file — that one is consumed by a
//! successful apply) and renders it, resolving each entry's `identity_hash`
//! back to a name through the SAME cnc name lines `apply` used to resolve
//! forward.
//!
//! A refused or timed-out `apply` deliberately leaves the staged file in
//! place: the node only deletes `schedules.pending` on a successful append
//! (`Node::apply_schedule_table`), so a retry needs nothing restaged.

use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use serde::Deserialize;

use uc_protocol::v2::cnc::ADMIN_OP_SCHEDULE_APPLY;
use uc_protocol::v2::schedule::{
    MAX_SCHEDULE_ENTRIES, SCHEDULE_ENTRY_LEN, SCHEDULE_HEADER_LEN, ScheduleEntry, ScheduleRule,
    ScheduleTable, decode_schedule_table, encode_schedule_table,
};

use crate::CommonArgs;

const NS_PER_SEC: u64 = 1_000_000_000;

// ---------------------------------------------------------------- TOML shape

#[derive(Debug, Deserialize)]
struct ScheduleFile {
    #[serde(default, rename = "schedule")]
    entries: Vec<ScheduleFileEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScheduleFileEntry {
    fsm: String,
    id: u64,
    every: Option<String>,
    anchor: Option<String>,
    at: Option<String>,
    once: Option<String>,
}

// ---------------------------------------------------------------- errors

#[derive(Debug, thiserror::Error)]
pub enum ScheduleFileError {
    #[error("parsing schedule TOML: {0}")]
    Toml(#[from] toml::de::Error),
    #[error(
        "entry {index} (fsm={fsm:?} id={id}): unknown fsm {fsm:?} — not one of this cluster's \
         declared rows"
    )]
    UnknownFsm { index: usize, fsm: String, id: u64 },
    #[error(
        "entry {index} (fsm={fsm:?} id={id}): duplicate of entry {first_index} — one (fsm, id) \
         pair may appear only once"
    )]
    Duplicate {
        index: usize,
        first_index: usize,
        fsm: String,
        id: u64,
    },
    #[error("entry {index} (fsm={fsm:?} id={id}): specify exactly one of every/at/once")]
    NotExactlyOne { index: usize, fsm: String, id: u64 },
    #[error("entry {index} (fsm={fsm:?} id={id}): anchor requires every")]
    AnchorWithoutEvery { index: usize, fsm: String, id: u64 },
    #[error("entry {index} (fsm={fsm:?} id={id}): every requires anchor")]
    EveryWithoutAnchor { index: usize, fsm: String, id: u64 },
    #[error("entry {index} (fsm={fsm:?} id={id}): bad duration {value:?}: {detail}")]
    BadDuration {
        index: usize,
        fsm: String,
        id: u64,
        value: String,
        detail: String,
    },
    #[error("entry {index} (fsm={fsm:?} id={id}): bad time {value:?}: {detail}")]
    BadTime {
        index: usize,
        fsm: String,
        id: u64,
        value: String,
        detail: String,
    },
    #[error("entry {index} (fsm={fsm:?} id={id}): every period must be > 0")]
    ZeroPeriod { index: usize, fsm: String, id: u64 },
    #[error("too many schedule entries: {count} > {MAX_SCHEDULE_ENTRIES}")]
    TooManyEntries { count: usize },
    #[error("encoded schedule table is {len} bytes, exceeding the {ceiling}-byte ceiling")]
    TooLarge { len: usize, ceiling: usize },
}

/// TOML text -> a validated [`ScheduleTable`], resolving each entry's `fsm`
/// name to an identity hash via `resolve` (the cluster's declared cnc rows —
/// `run_apply` below builds this from `cnc.service_names()`, per the
/// controller ruling amending this task's brief).
pub fn parse_table(
    toml_text: &str,
    resolve: impl Fn(&str) -> Option<u64>,
) -> Result<ScheduleTable, ScheduleFileError> {
    let file: ScheduleFile = toml::from_str(toml_text)?;
    let mut entries = Vec::with_capacity(file.entries.len());
    // (fsm name, timer id) pairs already accepted, for the duplicate check —
    // named by the string the operator wrote rather than the resolved hash
    // so the error message can quote it back.
    let mut seen: Vec<(String, u64)> = Vec::with_capacity(file.entries.len());

    for (index, e) in file.entries.iter().enumerate() {
        let Some(identity_hash) = resolve(&e.fsm) else {
            return Err(ScheduleFileError::UnknownFsm {
                index,
                fsm: e.fsm.clone(),
                id: e.id,
            });
        };
        if let Some(first_index) = seen
            .iter()
            .position(|(fsm, id)| *fsm == e.fsm && *id == e.id)
        {
            return Err(ScheduleFileError::Duplicate {
                index,
                first_index,
                fsm: e.fsm.clone(),
                id: e.id,
            });
        }

        let set_count = [e.every.is_some(), e.at.is_some(), e.once.is_some()]
            .iter()
            .filter(|b| **b)
            .count();
        if set_count != 1 {
            return Err(ScheduleFileError::NotExactlyOne {
                index,
                fsm: e.fsm.clone(),
                id: e.id,
            });
        }

        let rule = if let Some(every) = &e.every {
            let Some(anchor) = &e.anchor else {
                return Err(ScheduleFileError::EveryWithoutAnchor {
                    index,
                    fsm: e.fsm.clone(),
                    id: e.id,
                });
            };
            let period_ns =
                parse_duration_ns(every).map_err(|detail| ScheduleFileError::BadDuration {
                    index,
                    fsm: e.fsm.clone(),
                    id: e.id,
                    value: every.clone(),
                    detail,
                })?;
            if period_ns == 0 {
                return Err(ScheduleFileError::ZeroPeriod {
                    index,
                    fsm: e.fsm.clone(),
                    id: e.id,
                });
            }
            let anchor_ns = parse_rfc3339(anchor).map_err(|detail| ScheduleFileError::BadTime {
                index,
                fsm: e.fsm.clone(),
                id: e.id,
                value: anchor.clone(),
                detail,
            })?;
            ScheduleRule::Every {
                period_ns,
                anchor_ns,
            }
        } else if let Some(at) = &e.at {
            if e.anchor.is_some() {
                return Err(ScheduleFileError::AnchorWithoutEvery {
                    index,
                    fsm: e.fsm.clone(),
                    id: e.id,
                });
            }
            let secs_of_day =
                parse_time_of_day(at).map_err(|detail| ScheduleFileError::BadTime {
                    index,
                    fsm: e.fsm.clone(),
                    id: e.id,
                    value: at.clone(),
                    detail,
                })?;
            ScheduleRule::DailyAt { secs_of_day }
        } else {
            let once = e
                .once
                .as_ref()
                .expect("set_count == 1: once is the remaining option");
            if e.anchor.is_some() {
                return Err(ScheduleFileError::AnchorWithoutEvery {
                    index,
                    fsm: e.fsm.clone(),
                    id: e.id,
                });
            }
            let at_ns = parse_rfc3339(once).map_err(|detail| ScheduleFileError::BadTime {
                index,
                fsm: e.fsm.clone(),
                id: e.id,
                value: once.clone(),
                detail,
            })?;
            ScheduleRule::Once { at_ns }
        };

        seen.push((e.fsm.clone(), e.id));
        entries.push(ScheduleEntry {
            identity_hash,
            timer_id: e.id,
            rule,
        });
    }

    if entries.len() > MAX_SCHEDULE_ENTRIES {
        return Err(ScheduleFileError::TooManyEntries {
            count: entries.len(),
        });
    }
    let table = ScheduleTable { entries };
    let mut buf = Vec::new();
    encode_schedule_table(&table, &mut buf);
    // Fix round 1, Minor 3: unreachable under the current frozen 33-byte
    // per-entry wire format — the `TooManyEntries` check above always fires
    // first, since encoded length is a deterministic function of entry
    // count. Kept as a guard against a future format change that makes an
    // individual entry's encoded size vary.
    let ceiling = SCHEDULE_HEADER_LEN + MAX_SCHEDULE_ENTRIES * SCHEDULE_ENTRY_LEN;
    if buf.len() > ceiling {
        return Err(ScheduleFileError::TooLarge {
            len: buf.len(),
            ceiling,
        });
    }
    Ok(table)
}

// ---------------------------------------------------------------- duration / time parsing

/// `<n>(ns|us|ms|s|m|h|d)` -> nanoseconds.
fn parse_duration_ns(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let split = s
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("no unit in {s:?} (want ns|us|ms|s|m|h|d)"))?;
    let (num, unit) = s.split_at(split);
    if num.is_empty() {
        return Err(format!("no number in {s:?}"));
    }
    let n: u64 = num
        .parse()
        .map_err(|e| format!("bad number {num:?} in {s:?}: {e}"))?;
    let mult: u64 = match unit {
        "ns" => 1,
        "us" => 1_000,
        "ms" => 1_000_000,
        "s" => NS_PER_SEC,
        "m" => 60 * NS_PER_SEC,
        "h" => 3_600 * NS_PER_SEC,
        "d" => 86_400 * NS_PER_SEC,
        other => {
            return Err(format!(
                "unknown unit {other:?} in {s:?} (want ns|us|ms|s|m|h|d)"
            ));
        }
    };
    n.checked_mul(mult)
        .ok_or_else(|| format!("duration overflow: {s:?}"))
}

/// The inverse of [`parse_duration_ns`], for `schedule show`: the LARGEST
/// unit that divides `ns` exactly, else plain nanoseconds. Not required to
/// reproduce the exact string an operator wrote — only to round-trip through
/// [`parse_duration_ns`] to the same nanosecond count.
fn render_duration_ns(ns: u64) -> String {
    const UNITS: &[(u64, &str)] = &[
        (86_400 * NS_PER_SEC, "d"),
        (3_600 * NS_PER_SEC, "h"),
        (60 * NS_PER_SEC, "m"),
        (NS_PER_SEC, "s"),
        (1_000_000, "ms"),
        (1_000, "us"),
    ];
    for (mult, suffix) in UNITS {
        if ns != 0 && ns.is_multiple_of(*mult) {
            return format!("{}{suffix}", ns / mult);
        }
    }
    format!("{ns}ns")
}

/// `HH:MM[:SS]` -> seconds of day (0..86_400).
fn parse_time_of_day(s: &str) -> Result<u32, String> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() < 2 || parts.len() > 3 {
        return Err(format!("expected HH:MM[:SS], got {s:?}"));
    }
    let hh: u32 = parts[0].parse().map_err(|_| format!("bad hour in {s:?}"))?;
    let mm: u32 = parts[1]
        .parse()
        .map_err(|_| format!("bad minute in {s:?}"))?;
    let ss: u32 = if parts.len() == 3 {
        parts[2]
            .parse()
            .map_err(|_| format!("bad second in {s:?}"))?
    } else {
        0
    };
    if hh > 23 || mm > 59 || ss > 59 {
        return Err(format!("time out of range: {s:?}"));
    }
    Ok(hh * 3600 + mm * 60 + ss)
}

fn render_time_of_day(secs_of_day: u32) -> String {
    format!(
        "{:02}:{:02}:{:02}",
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60
    )
}

/// `YYYY-MM-DDTHH:MM:SSZ` -> unix nanoseconds. No date crate in this tree
/// (CLAUDE.md/plan-2 ruling) — hand-parsed, and rejects anything else by
/// name rather than trying to be lenient.
fn parse_rfc3339(s: &str) -> Result<u64, String> {
    let b = s.as_bytes();
    let shape_ok = b.len() == 20
        && b[4] == b'-'
        && b[7] == b'-'
        && b[10] == b'T'
        && b[13] == b':'
        && b[16] == b':'
        && b[19] == b'Z'
        && b[0..4].iter().all(u8::is_ascii_digit)
        && b[5..7].iter().all(u8::is_ascii_digit)
        && b[8..10].iter().all(u8::is_ascii_digit)
        && b[11..13].iter().all(u8::is_ascii_digit)
        && b[14..16].iter().all(u8::is_ascii_digit)
        && b[17..19].iter().all(u8::is_ascii_digit);
    if !shape_ok {
        return Err(format!(
            "expected YYYY-MM-DDTHH:MM:SSZ (UTC only), got {s:?}"
        ));
    }
    let year: i64 = s[0..4].parse().unwrap();
    let month: u32 = s[5..7].parse().unwrap();
    let day: u32 = s[8..10].parse().unwrap();
    let hour: u32 = s[11..13].parse().unwrap();
    let minute: u32 = s[14..16].parse().unwrap();
    let second: u32 = s[17..19].parse().unwrap();
    if !(1..=12).contains(&month) {
        return Err(format!("month out of range in {s:?}"));
    }
    if day < 1 || day > days_in_month(year, month) {
        return Err(format!("day out of range in {s:?}"));
    }
    if hour > 23 || minute > 59 || second > 59 {
        return Err(format!("time out of range in {s:?}"));
    }
    let days = days_from_civil(year, month, day);
    let secs = days
        .checked_mul(86_400)
        .and_then(|d| d.checked_add(hour as i64 * 3600))
        .and_then(|d| d.checked_add(minute as i64 * 60))
        .and_then(|d| d.checked_add(second as i64))
        .ok_or_else(|| format!("date overflow in {s:?}"))?;
    if secs < 0 {
        return Err(format!("date before the Unix epoch: {s:?}"));
    }
    Ok(secs as u64 * NS_PER_SEC)
}

/// The inverse of [`parse_rfc3339`], for `schedule show`.
fn render_rfc3339(ns: u64) -> String {
    let secs = ns / NS_PER_SEC;
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// The proleptic Gregorian leap rule: divisible by 4, except centuries not
/// divisible by 400 (so 2000 is a leap year, 1900 and 2100 are not).
fn is_leap_year(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

/// Days in `m` (`1..=12`) of year `y`, respecting [`is_leap_year`] for
/// February. Used to bound `parse_rfc3339`'s `day` field — without this a
/// day of `29..=31` is silently accepted for every month (`2026-02-30`
/// would otherwise roll over to March).
fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(y) {
                29
            } else {
                28
            }
        }
        _ => 0, // unreachable: `month` is already range-checked to 1..=12
    }
}

/// Howard Hinnant's `days_from_civil` — proleptic Gregorian, days relative to
/// the Unix epoch (1970-01-01 = 0). `m` in `1..=12`, `d` in `1..=31`.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = (m as i64 + 9) % 12; // [0, 11]: Mar=0 .. Feb=11
    let doy = (153 * mp + 2) / 5 + d as i64 - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

/// The inverse of [`days_from_civil`] (same source).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

// ---------------------------------------------------------------- apply

/// `uc2ctl schedule apply <file>`: parse, stage, sign, send. See the module
/// doc for the three-step flow.
pub fn apply(common: &CommonArgs, file: &Path) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(file)
        .map_err(|e| anyhow::anyhow!("reading {}: {e}", file.display()))?;
    let cnc = crate::open(common)?;
    let names = cnc.service_names();
    let resolve = |name: &str| {
        names
            .iter()
            .flatten()
            .find(|n| n.as_str() == name)
            .map(|n| n.hash())
    };
    let table = parse_table(&text, resolve).map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut bytes = Vec::new();
    encode_schedule_table(&table, &mut bytes);

    let pending_path = common.instance_dir.join(uc_node::SCHEDULE_PENDING_FILE);
    let tmp_path = common
        .instance_dir
        .join(format!("{}.tmp", uc_node::SCHEDULE_PENDING_FILE));
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp_path)
            .map_err(|e| anyhow::anyhow!("staging {}: {e}", tmp_path.display()))?;
        f.write_all(&bytes)
            .map_err(|e| anyhow::anyhow!("staging {}: {e}", tmp_path.display()))?;
        f.sync_all()
            .map_err(|e| anyhow::anyhow!("fsync {}: {e}", tmp_path.display()))?;
    }
    std::fs::rename(&tmp_path, &pending_path)
        .map_err(|e| anyhow::anyhow!("staging {}: {e}", pending_path.display()))?;

    let (id, ip, port) = uc_node::schedule_digest(&bytes);
    let resp = crate::signed_admin_request(
        common,
        ADMIN_OP_SCHEDULE_APPLY,
        id,
        ip,
        port,
        "schedule position",
    )
    .map_err(|e| anyhow::anyhow!("{e} (staged file kept at {})", pending_path.display()))?;

    match resp.status {
        0 => {
            println!("applied: position={}", resp.version);
            Ok(())
        }
        1 => {
            println!(
                "refused: {} (schedule position {}) — staged file kept at {}",
                crate::reason_str(resp.reason),
                resp.version,
                pending_path.display()
            );
            anyhow::bail!("refused: {}", crate::reason_str(resp.reason));
        }
        2 => {
            println!(
                "retry: leader unknown or a previous table is still uncommitted (schedule \
                 position {}) — staged file kept at {}, try again",
                resp.version,
                pending_path.display()
            );
            anyhow::bail!("retry: try again");
        }
        other => anyhow::bail!("unrecognized response status {other}"),
    }
}

// ---------------------------------------------------------------- show

/// `uc2ctl schedule show`: the newest ADOPTED table, from durable node state
/// (`uc_node::read_record`), with each entry's `identity_hash` resolved back
/// to a name through the cnc page's declared rows.
pub fn show(common: &CommonArgs) -> anyhow::Result<()> {
    let Some(record) = uc_node::read_record(&common.instance_dir)
        .map_err(|e| anyhow::anyhow!("reading schedule state: {e}"))?
    else {
        println!("no schedule table adopted");
        return Ok(());
    };

    let table = decode_schedule_table(&record.table)
        .ok_or_else(|| anyhow::anyhow!("durable schedule record failed to decode"))?;

    let cnc = crate::open(common)?;
    let names = cnc.service_names();
    let name_of = |hash: u64| -> String {
        names
            .iter()
            .flatten()
            .find(|n| n.hash() == hash)
            .map(|n| n.as_str().to_string())
            .unwrap_or_else(|| format!("0x{hash:016x}"))
    };

    println!("position={} time_ns={}", record.position, record.time_ns);
    for e in &table.entries {
        let rule = match e.rule {
            ScheduleRule::Every {
                period_ns,
                anchor_ns,
            } => format!(
                "every {} anchor {}",
                render_duration_ns(period_ns),
                render_rfc3339(anchor_ns)
            ),
            ScheduleRule::DailyAt { secs_of_day } => {
                format!("at {}", render_time_of_day(secs_of_day))
            }
            ScheduleRule::Once { at_ns } => format!("once {}", render_rfc3339(at_ns)),
        };
        println!(
            "fsm={} id={} rule={rule}",
            name_of(e.identity_hash),
            e.timer_id
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_both_rules_resolves_names_and_refuses_by_name() {
        let resolve = |n: &str| match n {
            "orders" => Some(0xabc),
            "kv" => Some(0xdef),
            _ => None,
        };
        let t = parse_table(
            r#"
[[schedule]]
fsm = "orders"
id = 1
every = "1h"
anchor = "2026-01-01T00:00:00Z"
[[schedule]]
fsm = "kv"
id = 2
at = "14:00"
[[schedule]]
fsm = "kv"
id = 3
once = "2026-01-01T00:00:00Z"
"#,
            resolve,
        )
        .unwrap();
        assert_eq!(t.entries.len(), 3);
        assert_eq!(t.entries[0].identity_hash, 0xabc);
        assert_eq!(
            t.entries[0].rule,
            ScheduleRule::Every {
                period_ns: 3_600_000_000_000,
                anchor_ns: 1_767_225_600_000_000_000
            }
        );
        assert_eq!(
            t.entries[1].rule,
            ScheduleRule::DailyAt {
                secs_of_day: 50_400
            }
        );
        assert_eq!(
            t.entries[2].rule,
            ScheduleRule::Once {
                at_ns: 1_767_225_600_000_000_000
            }
        );
        let e = parse_table(
            "[[schedule]]\nfsm = \"nope\"\nid = 1\nevery = \"1s\"\nanchor = \"2026-01-01T00:00:00Z\"\n",
            resolve,
        )
        .unwrap_err();
        assert!(e.to_string().contains("nope"), "{e}");
        let e = parse_table(
            "[[schedule]]\nfsm = \"kv\"\nid = 1\nevery = \"1s\"\nat = \"14:00\"\n",
            resolve,
        )
        .unwrap_err();
        assert!(e.to_string().contains("exactly one of"), "{e}");
        let e = parse_table(
            "[[schedule]]\nfsm = \"kv\"\nid = 1\nonce = \"2026-01-01T00:00:00Z\"\nat = \"14:00\"\n",
            resolve,
        )
        .unwrap_err();
        assert!(e.to_string().contains("exactly one of"), "{e}");
        let e = parse_table(
            "[[schedule]]\nfsm = \"kv\"\nid = 1\nevery = \"0s\"\nanchor = \"2026-01-01T00:00:00Z\"\n",
            resolve,
        )
        .unwrap_err();
        assert!(e.to_string().contains("period"), "{e}");
    }

    #[test]
    fn duplicate_fsm_id_pair_is_refused_by_name() {
        let resolve = |n: &str| match n {
            "kv" => Some(0xdef),
            _ => None,
        };
        let e = parse_table(
            "[[schedule]]\nfsm = \"kv\"\nid = 1\nat = \"14:00\"\n\
             [[schedule]]\nfsm = \"kv\"\nid = 1\nat = \"15:00\"\n",
            resolve,
        )
        .unwrap_err();
        assert!(e.to_string().contains("duplicate"), "{e}");
    }

    #[test]
    fn more_than_max_entries_is_refused_by_name() {
        let resolve = |_: &str| Some(1u64);
        let mut toml_text = String::new();
        for i in 0..(MAX_SCHEDULE_ENTRIES + 1) {
            toml_text.push_str(&format!(
                "[[schedule]]\nfsm = \"kv\"\nid = {i}\nonce = \"2026-01-01T00:00:00Z\"\n"
            ));
        }
        let e = parse_table(&toml_text, resolve).unwrap_err();
        assert!(e.to_string().contains("too many"), "{e}");
    }

    #[test]
    fn anchor_without_every_is_refused_by_name() {
        let resolve = |_: &str| Some(1u64);
        let e = parse_table(
            "[[schedule]]\nfsm = \"kv\"\nid = 1\nat = \"14:00\"\nanchor = \"2026-01-01T00:00:00Z\"\n",
            resolve,
        )
        .unwrap_err();
        assert!(e.to_string().contains("anchor"), "{e}");
    }

    #[test]
    fn every_without_anchor_is_refused_by_name() {
        let resolve = |_: &str| Some(1u64);
        let e = parse_table(
            "[[schedule]]\nfsm = \"kv\"\nid = 1\nevery = \"1s\"\n",
            resolve,
        )
        .unwrap_err();
        assert!(e.to_string().contains("anchor"), "{e}");
    }

    #[test]
    fn duration_parser_covers_every_unit_and_rejects_garbage() {
        assert_eq!(parse_duration_ns("1ns"), Ok(1));
        assert_eq!(parse_duration_ns("1us"), Ok(1_000));
        assert_eq!(parse_duration_ns("1ms"), Ok(1_000_000));
        assert_eq!(parse_duration_ns("1s"), Ok(1_000_000_000));
        assert_eq!(parse_duration_ns("2m"), Ok(120_000_000_000));
        assert_eq!(parse_duration_ns("1h"), Ok(3_600_000_000_000));
        assert_eq!(parse_duration_ns("2d"), Ok(172_800_000_000_000));
        assert!(parse_duration_ns("1w").is_err(), "unknown unit");
        assert!(parse_duration_ns("h").is_err(), "no number");
        assert!(parse_duration_ns("5").is_err(), "no unit");
        assert!(parse_duration_ns("").is_err(), "empty");
        // Fix round 1, Minor 2: overflows u64 nanoseconds rather than
        // wrapping — 99_999_999_999 days * 86_400_000_000_000 ns/day is
        // far past u64::MAX.
        assert!(
            parse_duration_ns("99999999999d").is_err(),
            "overflow must error, not wrap"
        );
        // round-trips through the show-side renderer
        for s in ["7d", "3h", "45m", "20s", "500ms", "250us", "999ns"] {
            let ns = parse_duration_ns(s).unwrap();
            assert_eq!(parse_duration_ns(&render_duration_ns(ns)), Ok(ns), "{s}");
        }
    }

    #[test]
    fn time_of_day_parser_accepts_hhmm_and_hhmmss_and_rejects_out_of_range() {
        assert_eq!(parse_time_of_day("14:00"), Ok(50_400));
        assert_eq!(parse_time_of_day("00:00:00"), Ok(0));
        assert_eq!(parse_time_of_day("23:59:59"), Ok(86_399));
        assert!(parse_time_of_day("24:00").is_err());
        assert!(parse_time_of_day("12:60").is_err());
        assert!(parse_time_of_day("12").is_err());
        assert!(parse_time_of_day("12:00:00:00").is_err());
        for s in ["00:00:00", "14:00:00", "23:59:59"] {
            let secs = parse_time_of_day(s).unwrap();
            assert_eq!(render_time_of_day(secs), s);
        }
    }

    #[test]
    fn rfc3339_round_trips_and_rejects_non_utc_forms() {
        assert_eq!(
            parse_rfc3339("2026-01-01T00:00:00Z"),
            Ok(1_767_225_600_000_000_000)
        );
        assert_eq!(
            render_rfc3339(1_767_225_600_000_000_000),
            "2026-01-01T00:00:00Z"
        );
        for (s, secs) in [
            ("1970-01-01T00:00:00Z", 0i64),
            ("1970-01-01T00:00:01Z", 1),
            ("2000-02-29T00:00:00Z", 951_782_400), // leap day
            ("2026-09-03T12:34:56Z", 1_788_438_896),
            ("2099-12-31T23:59:59Z", 4_102_444_799),
        ] {
            let ns = parse_rfc3339(s).unwrap();
            assert_eq!(ns, secs as u64 * NS_PER_SEC, "{s}");
            assert_eq!(render_rfc3339(ns), s, "round trip {s}");
        }
        assert!(parse_rfc3339("2026-01-01 00:00:00Z").is_err(), "needs T");
        assert!(
            parse_rfc3339("2026-01-01T00:00:00+02:00").is_err(),
            "UTC only"
        );
        assert!(parse_rfc3339("2026-01-01T00:00:00").is_err(), "needs Z");
        assert!(parse_rfc3339("2026-13-01T00:00:00Z").is_err(), "bad month");
        assert!(parse_rfc3339("2026-01-32T00:00:00Z").is_err(), "bad day");
        assert!(parse_rfc3339("2026-01-01T24:00:00Z").is_err(), "bad hour");
        assert!(parse_rfc3339("not-a-date").is_err());
    }

    /// Fix round 1, Important 1: `day` was only bounded to `1..=31`, so
    /// `2026-02-30`/`2026-04-31`/a non-leap `2026-02-29` silently rolled
    /// over into the following month instead of being refused.
    #[test]
    fn rfc3339_rejects_calendrically_invalid_dates() {
        for s in [
            "2026-02-30T00:00:00Z", // Feb never has 30 days
            "2026-04-31T00:00:00Z", // Apr has 30 days
            "2026-02-29T00:00:00Z", // 2026 is not a leap year
            "1900-02-29T00:00:00Z", // century, not divisible by 400: not a leap year
        ] {
            assert!(parse_rfc3339(s).is_err(), "{s} must be rejected");
        }
        for s in [
            "2024-02-29T00:00:00Z", // ordinary leap year
            "2000-02-29T00:00:00Z", // century divisible by 400: a leap year
        ] {
            assert!(parse_rfc3339(s).is_ok(), "{s} must be accepted");
        }
    }
}
