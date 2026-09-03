// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The replicated schedule table (time-and-timers spec §5, plan 2): the
//! frozen wire body of a `FRAME_TYPE_SCHEDULE_TABLE` frame and the pure
//! recurrence arithmetic every node runs identically. `core`-only.

pub const MAX_SCHEDULE_ENTRIES: usize = 32;
pub const SCHEDULE_HEADER_LEN: usize = 8;
pub const SCHEDULE_ENTRY_LEN: usize = 33;
const SCHEDULE_VERSION: u32 = 1;
const NS_PER_SEC: u64 = 1_000_000_000;
const NS_PER_DAY: u64 = 86_400 * NS_PER_SEC;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleRule {
    /// Every `period_ns` from `anchor_ns` (occurrences: anchor, anchor+p, …).
    Every { period_ns: u64, anchor_ns: u64 },
    /// Once a day at `secs_of_day` UTC (occurrences: k·day + secs).
    DailyAt { secs_of_day: u32 },
    /// One fixed deadline; after it, nothing (the entry parks).
    Once { at_ns: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScheduleEntry {
    pub identity_hash: u64,
    pub timer_id: u64,
    pub rule: ScheduleRule,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleTable {
    pub entries: Vec<ScheduleEntry>,
}

impl ScheduleRule {
    /// First occurrence strictly after `t_ns` (saturating); `None` when
    /// nothing follows — only a `Once` at or before `t_ns`.
    pub const fn next_after(&self, t_ns: u64) -> Option<u64> {
        match *self {
            ScheduleRule::Every {
                period_ns,
                anchor_ns,
            } => {
                if t_ns < anchor_ns {
                    return Some(anchor_ns);
                }
                // Saturating throughout — a period of 1 at the top of the
                // range makes the quotient itself `u64::MAX`, so even the
                // `+ 1` overflows (found by `uc_protocol_schedule_table`).
                let k = ((t_ns - anchor_ns) / period_ns).saturating_add(1);
                Some(anchor_ns.saturating_add(k.saturating_mul(period_ns)))
            }
            ScheduleRule::DailyAt { secs_of_day } => {
                let off = secs_of_day as u64 * NS_PER_SEC;
                let day = t_ns / NS_PER_DAY;
                let today = day.saturating_mul(NS_PER_DAY).saturating_add(off);
                Some(if today > t_ns {
                    today
                } else {
                    (day + 1).saturating_mul(NS_PER_DAY).saturating_add(off)
                })
            }
            ScheduleRule::Once { at_ns } => {
                if t_ns < at_ns {
                    Some(at_ns)
                } else {
                    None
                }
            }
        }
    }

    /// Latest occurrence at or before `t_ns`; `None` before the first.
    pub const fn latest_at_or_before(&self, t_ns: u64) -> Option<u64> {
        match *self {
            ScheduleRule::Every {
                period_ns,
                anchor_ns,
            } => {
                if t_ns < anchor_ns {
                    return None;
                }
                Some(anchor_ns + (t_ns - anchor_ns) / period_ns * period_ns)
            }
            ScheduleRule::DailyAt { secs_of_day } => {
                let off = secs_of_day as u64 * NS_PER_SEC;
                let day = t_ns / NS_PER_DAY;
                // `base <= t_ns` by construction, but `base + off` can
                // OVERFLOW in the last day of the u64 range (found by
                // `uc_protocol_schedule_table`: `t_ns` near `u64::MAX` with a
                // large `secs_of_day`). An occurrence that overflows is by
                // definition after `t_ns`, so it falls through to yesterday's
                // — which cannot overflow, being at most `t_ns`.
                let base = day * NS_PER_DAY;
                if u64::MAX - base >= off && base + off <= t_ns {
                    Some(base + off)
                } else if day == 0 {
                    None
                } else {
                    Some((day - 1) * NS_PER_DAY + off)
                }
            }
            ScheduleRule::Once { at_ns } => {
                if at_ns <= t_ns {
                    Some(at_ns)
                } else {
                    None
                }
            }
        }
    }

    /// Spec §5 one-tick catch-up: the latest missed occurrence if any is
    /// newer than what was delivered, else the next one; `None` = parked
    /// (a `Once` already delivered).
    pub const fn arm(&self, last_delivered_ns: Option<u64>, log_time_ns: u64) -> Option<u64> {
        let latest = self.latest_at_or_before(log_time_ns);
        match (latest, last_delivered_ns) {
            (Some(o), None) => Some(o),
            (Some(o), Some(l)) if o > l => Some(o),
            (_, Some(l)) => self.next_after(l),
            (None, None) => self.next_after(log_time_ns),
        }
    }
}

pub fn encode_schedule_table(t: &ScheduleTable, out: &mut Vec<u8>) {
    out.extend_from_slice(&SCHEDULE_VERSION.to_le_bytes());
    out.extend_from_slice(&(t.entries.len() as u16).to_le_bytes());
    out.extend_from_slice(&[0, 0]);
    for e in &t.entries {
        out.extend_from_slice(&e.identity_hash.to_le_bytes());
        out.extend_from_slice(&e.timer_id.to_le_bytes());
        let (kind, a, b) = match e.rule {
            ScheduleRule::Every {
                period_ns,
                anchor_ns,
            } => (1u8, period_ns, anchor_ns),
            ScheduleRule::DailyAt { secs_of_day } => (2u8, secs_of_day as u64, 0),
            ScheduleRule::Once { at_ns } => (3u8, at_ns, 0),
        };
        out.push(kind);
        out.extend_from_slice(&a.to_le_bytes());
        out.extend_from_slice(&b.to_le_bytes());
    }
}

/// Total on any input (see the module doc's refusal list): `None` on a short
/// buffer, a version ≠ 1, `count > 32`, a length ≠ `8 + 33·count`, an unknown
/// kind, `period_ns == 0`, `secs_of_day >= 86_400`, a non-zero `b` on a
/// `DailyAt`/`Once`, or a duplicate `(identity_hash, timer_id)`.
pub fn decode_schedule_table(buf: &[u8]) -> Option<ScheduleTable> {
    if buf.len() < SCHEDULE_HEADER_LEN {
        return None;
    }
    let version = u32::from_le_bytes(buf[0..4].try_into().ok()?);
    let count = u16::from_le_bytes(buf[4..6].try_into().ok()?) as usize;
    if version != SCHEDULE_VERSION || count > MAX_SCHEDULE_ENTRIES {
        return None;
    }
    if buf.len() != SCHEDULE_HEADER_LEN + count * SCHEDULE_ENTRY_LEN {
        return None;
    }
    let mut entries = Vec::with_capacity(count);
    for i in 0..count {
        let o = SCHEDULE_HEADER_LEN + i * SCHEDULE_ENTRY_LEN;
        let u = |s: usize| u64::from_le_bytes(buf[o + s..o + s + 8].try_into().unwrap());
        let (identity_hash, timer_id, kind, a, b) = (u(0), u(8), buf[o + 16], u(17), u(25));
        let rule = match kind {
            1 if a > 0 => ScheduleRule::Every {
                period_ns: a,
                anchor_ns: b,
            },
            2 if a < 86_400 && b == 0 => ScheduleRule::DailyAt {
                secs_of_day: a as u32,
            },
            3 if b == 0 => ScheduleRule::Once { at_ns: a },
            _ => return None,
        };
        if entries
            .iter()
            .any(|e: &ScheduleEntry| e.identity_hash == identity_hash && e.timer_id == timer_id)
        {
            return None;
        }
        entries.push(ScheduleEntry {
            identity_hash,
            timer_id,
            rule,
        });
    }
    Some(ScheduleTable { entries })
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: u64 = 60 * 60 * 1_000_000_000;
    const DAY: u64 = 24 * H;

    /// FROZEN wire layout: header(8) ‖ entries × 33. Never change these bytes.
    #[test]
    fn table_codec_pins_bytes_and_is_total() {
        let t = ScheduleTable {
            entries: vec![
                ScheduleEntry {
                    identity_hash: 0x0102_0304_0506_0708,
                    timer_id: 7,
                    rule: ScheduleRule::Every {
                        period_ns: H,
                        anchor_ns: 5,
                    },
                },
                ScheduleEntry {
                    identity_hash: 9,
                    timer_id: 8,
                    rule: ScheduleRule::DailyAt {
                        secs_of_day: 14 * 3600,
                    },
                },
                ScheduleEntry {
                    identity_hash: 9,
                    timer_id: 9,
                    rule: ScheduleRule::Once { at_ns: 42 },
                },
            ],
        };
        let mut b = Vec::new();
        encode_schedule_table(&t, &mut b);
        assert_eq!(b.len(), SCHEDULE_HEADER_LEN + 3 * SCHEDULE_ENTRY_LEN);
        assert_eq!(&b[0..4], &1u32.to_le_bytes(), "version 1");
        assert_eq!(&b[4..6], &3u16.to_le_bytes(), "count");
        assert_eq!(&b[6..8], &[0, 0], "reserved");
        assert_eq!(
            &b[8..16],
            &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01],
            "hash LE"
        );
        assert_eq!(&b[16..24], &7u64.to_le_bytes());
        assert_eq!(b[24], 1, "kind Every");
        assert_eq!(&b[25..33], &H.to_le_bytes());
        assert_eq!(&b[33..41], &5u64.to_le_bytes());
        assert_eq!(b[41 + 16], 2, "kind DailyAt");
        assert_eq!(b[74 + 16], 3, "kind Once");
        assert_eq!(
            &b[74 + 17..74 + 25],
            &42u64.to_le_bytes(),
            "once: a = at_ns"
        );
        assert_eq!(&b[74 + 25..74 + 33], &0u64.to_le_bytes(), "once: b = 0");
        assert_eq!(decode_schedule_table(&b), Some(t.clone()));
        // totality
        assert_eq!(decode_schedule_table(&b[..7]), None, "short header");
        assert_eq!(
            decode_schedule_table(&b[..b.len() - 1]),
            None,
            "length mismatch"
        );
        let mut v = b.clone();
        v[0] = 2;
        assert_eq!(decode_schedule_table(&v), None, "version");
        let mut k = b.clone();
        k[24] = 4;
        assert_eq!(decode_schedule_table(&k), None, "unknown kind (3 is Once)");
        let mut z = b.clone();
        z[25..33].copy_from_slice(&0u64.to_le_bytes());
        assert_eq!(decode_schedule_table(&z), None, "period 0");
        let mut d = b.clone();
        d[41 + 17..41 + 25].copy_from_slice(&86_400u64.to_le_bytes());
        assert_eq!(decode_schedule_table(&d), None, "secs_of_day out of range");
        let mut ob = b.clone();
        ob[74 + 25..74 + 33].copy_from_slice(&1u64.to_le_bytes());
        assert_eq!(decode_schedule_table(&ob), None, "once: b must be 0");
        let mut dup = t.clone();
        dup.entries.push(dup.entries[0]);
        let mut db = Vec::new();
        encode_schedule_table(&dup, &mut db);
        assert_eq!(decode_schedule_table(&db), None, "duplicate (hash, id)");
        let big = ScheduleTable {
            entries: (0..33)
                .map(|i| ScheduleEntry {
                    identity_hash: 1,
                    timer_id: i,
                    rule: ScheduleRule::Every {
                        period_ns: 1,
                        anchor_ns: 0,
                    },
                })
                .collect(),
        };
        let mut bb = Vec::new();
        encode_schedule_table(&big, &mut bb);
        assert_eq!(decode_schedule_table(&bb), None, "33 entries refused");
        assert_eq!(MAX_SCHEDULE_ENTRIES, 32);
        const {
            assert!(
                SCHEDULE_HEADER_LEN + MAX_SCHEDULE_ENTRIES * SCHEDULE_ENTRY_LEN <= 1312,
                "fits the crypto-on payload ceiling"
            );
        }
    }

    #[test]
    fn every_rule_arithmetic() {
        let r = ScheduleRule::Every {
            period_ns: H,
            anchor_ns: 10 * H,
        };
        assert_eq!(
            r.next_after(0),
            Some(10 * H),
            "before the anchor: the anchor"
        );
        assert_eq!(r.next_after(10 * H), Some(11 * H), "strictly after");
        assert_eq!(r.next_after(10 * H + 1), Some(11 * H));
        assert_eq!(r.latest_at_or_before(9 * H), None);
        assert_eq!(r.latest_at_or_before(10 * H), Some(10 * H));
        assert_eq!(r.latest_at_or_before(12 * H + 5), Some(12 * H));
        // arm: one-tick catch-up
        assert_eq!(
            r.arm(None, 12 * H + 5),
            Some(12 * H),
            "missed ticks collapse to the latest"
        );
        assert_eq!(
            r.arm(Some(12 * H), 12 * H + 5),
            Some(13 * H),
            "already delivered: the next"
        );
        assert_eq!(
            r.arm(Some(11 * H), 12 * H + 5),
            Some(12 * H),
            "one behind: the latest, once"
        );
        assert_eq!(
            r.arm(None, 0),
            Some(10 * H),
            "before the anchor: the anchor"
        );
        assert_eq!(r.next_after(u64::MAX - 1), Some(u64::MAX), "saturates");
    }

    #[test]
    fn once_rule_arithmetic() {
        let r = ScheduleRule::Once { at_ns: 5 * H };
        assert_eq!(r.next_after(0), Some(5 * H));
        assert_eq!(r.next_after(5 * H - 1), Some(5 * H));
        assert_eq!(r.next_after(5 * H), None, "nothing follows a once");
        assert_eq!(r.latest_at_or_before(5 * H - 1), None);
        assert_eq!(r.latest_at_or_before(9 * H), Some(5 * H));
        assert_eq!(r.arm(None, 0), Some(5 * H), "in the future: the deadline");
        assert_eq!(r.arm(None, 9 * H), Some(5 * H), "missed: fires once, late");
        assert_eq!(r.arm(Some(5 * H), 9 * H), None, "delivered: parked");
        assert_eq!(
            r.arm(Some(4 * H), 9 * H),
            Some(5 * H),
            "re-applied with a newer deadline than the delivered one: fires"
        );
    }

    /// Totality at the top of the range (found by the
    /// `uc_protocol_schedule_table` fuzz target): the naive
    /// `day * NS_PER_DAY + secs_of_day` overflows in the last day of the u64
    /// range. Every rule kind must answer, not panic, for any `t_ns`.
    #[test]
    fn the_arithmetic_is_total_at_the_top_of_the_range() {
        for secs in [0u32, 1, 50_400, 86_399] {
            let r = ScheduleRule::DailyAt { secs_of_day: secs };
            // An answer, not a panic — and a real occurrence of the rule
            // (`secs` past a day boundary), never above `t_ns`.
            let l = r.latest_at_or_before(u64::MAX).unwrap();
            assert_eq!(l % NS_PER_DAY, secs as u64 * NS_PER_SEC, "{secs}: {l}");
            let _ = r.next_after(u64::MAX);
            let _ = r.arm(None, u64::MAX);
            let _ = r.arm(Some(u64::MAX), u64::MAX);
        }
        let e = ScheduleRule::Every {
            period_ns: 1,
            anchor_ns: 0,
        };
        assert_eq!(e.latest_at_or_before(u64::MAX), Some(u64::MAX));
        assert_eq!(e.next_after(u64::MAX), Some(u64::MAX), "saturates");
        let o = ScheduleRule::Once { at_ns: u64::MAX };
        assert_eq!(o.latest_at_or_before(u64::MAX), Some(u64::MAX));
        assert_eq!(o.next_after(u64::MAX), None);
    }

    #[test]
    fn daily_rule_arithmetic() {
        let r = ScheduleRule::DailyAt {
            secs_of_day: 14 * 3600,
        };
        let d0_14 = 14 * H;
        assert_eq!(r.next_after(0), Some(d0_14));
        assert_eq!(r.next_after(d0_14), Some(DAY + d0_14), "strictly after");
        assert_eq!(r.latest_at_or_before(d0_14 - 1), None);
        assert_eq!(r.latest_at_or_before(DAY + d0_14 + 1), Some(DAY + d0_14));
        assert_eq!(
            r.arm(None, 3 * DAY + 1),
            Some(2 * DAY + d0_14),
            "latest past occurrence, once"
        );
        assert_eq!(
            r.arm(Some(2 * DAY + d0_14), 3 * DAY + 1),
            Some(3 * DAY + d0_14)
        );
    }
}
