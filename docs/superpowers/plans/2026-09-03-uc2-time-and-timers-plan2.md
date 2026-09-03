# Time and timers, plan 2 (the replicated schedule table) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An operator applies a TOML schedule table with one signed `uc2ctl schedule apply` request; the leader appends it as a `SCHEDULE_TABLE` log frame; every node adopts it from the log, persists it, and materialises each entry's next deadline into its per-row timer heap; the leader fires table ticks as `TIMER` frames with `FLAG_TIMER_TABLE` set; `Timed<S>` delivers each tick exactly once via its `table_last` map; recurrence is computed from the tick just fired, never from a clock, so every node agrees.

**Architecture:** The table rides the same three mechanisms plan 1 and M7 already proved: the archive agent's header walk (which adopts CONFIG frames today) gains a second observation kind; `uc_node::timers::RowTimers` gains table entries with a rule and a next deadline, fired through the same `fire_due_timers` pass with the table flag; the service's `Timed<S>` wrapper reports table ticks consumed with a distinct op so followers advance their entries from the log. The admin request line is 64 bytes of fixed fields, so the encoded table is staged in a file under the instance dir and the signed request carries an 80-bit digest of it — the HMAC already covers those fields. Apply is leader-only (a follower refuses by name; there is nothing to forward but the digest).

**Tech Stack:** Rust 1.96 workspace (MSRV 1.89); `uc_protocol` (core-only leaf), `uc_log`, `uc_service`, `uc_node`, `uc_ctl` (+ `toml`/`serde` for the file), `uc_lincheck`; `fuzz/`; `packaging/prometheus/uc2-alerts.yml`.

**Spec:** `docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md` §5 (binding), §4.5–§4.6 (the heap and `Timed`), §6–§7, §11 items 7–10. Plan 1 is on `main` (f31700d); this plan builds on its as-built surfaces, listed under Global Constraints. Task 0 records the deltas this plan discovered against §5.

## Global Constraints

- **Whole workspace green after every task**: `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo clippy -p uc_service --features apply-profile --all-targets -- -D warnings`, `cargo clippy -p uc_gateway --features test-util --all-targets -- -D warnings`, `cargo test --workspace --exclude uc_node`, `cargo test -p uc_node --lib --test smoke --test failover --test learner --test purge_safety --test query_barrier --test admin_auth --test daemon_refusals --test timers --test services`; after Tasks 1 and 7, `(cd fuzz && RUSTFLAGS="--cfg fuzzing" cargo +nightly check)`.
- **Still the unreleased 2.11.0 flag day**: `uc_protocol::version::CURRENT` stays `0.7.0`, `CNC_V2_VERSION` stays 3.1; a new frame type and a new sched-ring op are additions to layouts that have not shipped.
- **Frozen after they ship**: `FRAME_TYPE_SCHEDULE_TABLE = 6`; the table body codec (Task 1); `SchedOp::TableConsumed = 4`; admin `op = 6` (`schedule_apply`) with its reason codes; `MAX_SCHEDULE_ENTRIES = 32`. Each pinned by a test whose comment says so.
- **Plan-1 surfaces this plan builds on (as built, not as the spec first wrote them)**: `FLAG_TIMER_TABLE = 0x01` on a TIMER frame; `TimerEvent.table`; `Timed::table_last: BTreeMap<u64, u64>` with the rule "deliver iff `deadline > table_last[id]`, then record"; `RowTimers { hash, pending, heap, in_flight }` with `peek_due/take_in_flight/rearm/consumed`; `fire_due_timers` (global deadline order, `TIMERS_PER_PASS = 64`, hold-clients rule, `append_timer(&body, flags)`); `drain_sched_rings` decoding `SchedRecord { op, timer_id, deadline_ns }`; the archive's `observe_terms` walk with `config_observations: Vec<(end_pos, payload)>` handed to the consensus agent over `cfg_obs_tx` and drained in `do_work` step 1c; `Appender::append_config(term, payload) -> Result<frame END, _>`; the admin path (`AdminReq { seq, nonce, op: u32, id: u32, ip: u32, port: u16 }` at cnc 3584, `AdminAuth` HMAC over `app_id ‖ instance_id ‖ seq ‖ nonce ‖ op ‖ id ‖ ip ‖ port ‖ expiry_ns`, `handle_admin` → `verify_admin` → `propose_and_append(op, id, ip, port)` → `audit_admin` → `write_admin_reply(seq, status, reason, version)`; status 0 = accepted, 1 = refused, 2 = retry; `audit::op_name(op)`).
- **Determinism**: every node computes the same next deadline from the same fired deadline and the same rule; the node never consults a wall clock to advance a table entry. The only clock-driven choice is which ticks are DUE (the leader pass, as today).
- **Ordering** (spec §4.3) is unchanged: table ticks are appended by the same pass logic as programmatic timers, stamped with their deadline.
- **Payload ceiling**: the encoded table must fit one frame — `MAX_SCHEDULE_ENTRIES = 32` × 33 B + 8 B header = 1064 B < 1312 (crypto-on ceiling). A file that encodes larger is refused by `uc2ctl` before any request is written.
- **Fleet spend is user-gated.** Never write scratch to `/tmp`.
- Commit subjects: `type(scope): imperative summary`. Every new or changed test is **watched red first**.

---

## File structure

| file | responsibility | task |
|---|---|---|
| `docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md` §5 | as-built errata: 32 hash-keyed entries, the staged-file + signed-digest apply, leader-only apply, table-tick semantics (no re-arm, one-tick catch-up), `TableConsumed` | 0 |
| `uc_protocol/src/v2/frame.rs`, `uc_protocol/src/v2/schedule.rs` (new), `uc_protocol/src/v2/mod.rs`, `uc_protocol/src/v2/ipc.rs` | `FRAME_TYPE_SCHEDULE_TABLE`; `ScheduleRule`/`ScheduleEntry`/`ScheduleTable` + the frozen codec + the pure next-occurrence arithmetic; `SchedOp::TableConsumed`; `ADMIN_OP_SCHEDULE_APPLY` | 1 |
| `uc_log/src/buffer.rs`, `uc_log/src/archive.rs` | `Appender::append_schedule_table`; the archive's `table_observations: Vec<(end_pos, time_ns, payload)>` + take/retain | 2 |
| `uc_node/src/timers.rs` | `TableEntry`, `RowTimers::{adopt_table, clear_table, table_fired, table_delivered}`, `peek_due` returning the kind | 3 |
| `uc_node/src/node.rs`, `uc_node/src/schedule_state.rs` (new) | adoption (leader at append, followers from the archive), `state/schedules.state` (`StableValue<ScheduleRecord>`), boot arming, fire with the flag + advance, `TableConsumed` drain, the admin op (staged file, digest check, validation, append, audit, reply), metrics + obs events | 4 |
| `uc_service/src/traits.rs`, `uc_service/src/timed.rs`, `uc_service/src/apply.rs` | `ApplyCtx::consumed_table`; `Timed` reports table ticks with `TableConsumed` and announces `table_delivered()` after attach/replay | 5 |
| `uc_ctl/src/main.rs`, `uc_ctl/src/schedule.rs` (new), `uc_ctl/Cargo.toml` | `uc2ctl schedule apply <file.toml>` (parse, validate against the cnc name lines, encode, stage, sign, send, decode the reply) and `uc2ctl schedule show` | 6 |
| `uc_node/tests/timers.rs`, `uc_node/tests/admin_auth.rs`, `uc_lincheck/src/timer.rs`, `uc_node/tests/lin_v2.rs`, `fuzz/…` | end-to-end table tests; the capstone's table clause; fuzz target `uc_protocol_schedule_table` | 7 |
| `docs/…`, `RELEASES.md`, `docs/releases.md`, `CLAUDE.md`, `packaging/prometheus/uc2-alerts.yml`, the gate doc | docs sweep, runbook, attack-surface entry, release bullet, gate rows | 8 |

---

### Task 0: Spec as-built errata for §5

**Files:**
- Modify: `docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md` §5 (and one line in §4.5)

The tree recon (2026-09-03) found five places where §5 as written cannot be built as stated. Record them so the spec stays the binding record.

- [ ] **Step 1: §5, the payload bullet.** Replace the "Payload: bincode of `ScheduleTable { entries: Vec<Entry> }`, **≤ 64 entries** …" bullet with:

```markdown
- Payload: a hand-laid, bounded, total codec (`uc_protocol::v2::schedule`,
  the same style as `v2::config`'s), **≤ 32 entries**, each keyed by the
  FSM's **identity hash** rather than its name (`hash u64 ‖ id u64 ‖ kind u8
  ‖ a u64 ‖ b u64` = 33 B; 32 entries + an 8-byte header = 1064 B, inside
  the 1312 B crypto-on ceiling). Names appear only in the operator's TOML;
  `uc2ctl` resolves each against the node's cnc name lines and refuses an
  undeclared one before any request is written. Two rules only: `every`
  (`period_ns`, `anchor_ns`) and `at` (`secs_of_day`, UTC).
```

- [ ] **Step 2: §5, how the table reaches the leader.** After the first paragraph add:

```markdown
**How the bytes reach the leader.** The cnc admin request line is 64
bytes of fixed fields (`seq, nonce, op, id, ip, port`), and the HMAC
covers exactly those fields — a table cannot ride it. `uc2ctl schedule
apply` therefore (1) encodes the table, (2) writes the bytes to
`<instance_dir>/schedules.pending` (0600, fsync, rename), and (3) writes
an admin request with `op = 6` whose `id ‖ ip ‖ port` fields carry the
first 80 bits of SHA-256 over the encoded bytes. The node verifies the
request as it does every admin op, reads the staged file, recomputes the
digest, and refuses by name on a mismatch (reason `schedule_digest`).
Under `[admin] auth = "hmac"` the payload is thereby authenticated
through the signed digest; under the filesystem policy it is trusted the
way every admin request already is. **Apply is leader-only**: a follower
answers `status = 2` with the leader hint, as it does when it cannot
forward — there is nothing to forward but the digest, and the leader
cannot read the follower's file.
```

- [ ] **Step 3: §5, table-tick semantics.** Replace the "Table timers go through the same heap …" bullet with:

```markdown
- Table timers go through the same heap (§4.5) and the same TIMER frame
  with `flags` bit 0 set; the FSM sees them in the same `on_timer` with
  `ev.table = true`. Exactly-once is the `table_last` rule (§4.6). Three
  differences from a programmatic instance, all deliberate:
  - **The node advances the entry at append** (`next = rule.next_after
    (fired)`), so a leader keeps a table on schedule without waiting for
    its service; a follower advances on the service's `TableConsumed
    (id, deadline)` report, so a new leader starts from what its own
    service last delivered. A leader whose service lagged may re-fire ticks
    the old leader already fired; `Timed` drops them (at-least-once, as
    §4.5).
  - **No re-arm on leadership loss.** A table tick whose frame was
    truncated is not fired again; the next tick is. (A programmatic
    instance IS re-armed, because it has no successor.)
  - **One-tick catch-up.** When an entry is armed — at adoption, at boot
    from `state/schedules.state`, or when the service announces its
    `table_last` — its next deadline is the LATEST occurrence at or below
    the log's clock if that occurrence is newer than the last delivered
    one, else the first occurrence after it. A cluster that was down for an
    hour with a one-second rule fires one tick on recovery, not 3 600.
```

- [ ] **Step 4: §4.5, one sentence.** After "Entries leave the heap when the leader appends them, or when the service reports one consumed." append: "Plan 2's table entries are the exception: they never leave, they advance (§5)."

- [ ] **Step 5: §5, persistence line.** After "persists it as a `StableValue` in `state/schedules.state`" add: "(`ScheduleRecord { position, time_ns, table }`; at boot the node re-arms every entry from the record and the recovered log clock before its service attaches — an FSM with no attached service still ticks on the leader; the tick is dropped on delivery only if no service ever attaches, which is the operator's problem to see through `uc2_timers_pending` and the attach gauges)".

- [ ] **Step 6: Commit** `docs(spec): time and timers §5 — as-built errata from the plan-2 recon (32 hash-keyed entries, staged-file apply, table-tick semantics)`.

---

### Task 1: `uc_protocol` — the frame type, the table codec, the recurrence arithmetic, the two new codes

**Files:**
- Create: `uc_protocol/src/v2/schedule.rs`
- Modify: `uc_protocol/src/v2/mod.rs` (add `pub mod schedule;`), `uc_protocol/src/v2/frame.rs` (constant after `FRAME_TYPE_TIMER`, and `frame_type_codes_are_stable`), `uc_protocol/src/v2/ipc.rs` (`SchedOp::TableConsumed = 4`, `read_sched_record` accepts `4`, the pinned-bytes test)
- Test: `uc_protocol/src/v2/schedule.rs` `mod tests`

**Interfaces:**
- Produces (all `core`-only; `alloc::vec::Vec` is already used by `v2::config` — follow it):
  - `pub const FRAME_TYPE_SCHEDULE_TABLE: u8 = 6;`
  - `pub const MAX_SCHEDULE_ENTRIES: usize = 32; pub const SCHEDULE_ENTRY_LEN: usize = 33; pub const SCHEDULE_HEADER_LEN: usize = 8;` (header = `version: u32 ‖ count: u16 ‖ reserved: u16`, version fixed at `1`)
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum ScheduleRule { Every { period_ns: u64, anchor_ns: u64 }, DailyAt { secs_of_day: u32 } }` (kind byte `1` / `2`; `a ‖ b` = `period_ns ‖ anchor_ns` or `secs_of_day as u64 ‖ 0`)
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub struct ScheduleEntry { pub identity_hash: u64, pub timer_id: u64, pub rule: ScheduleRule }`
  - `#[derive(Debug, Clone, PartialEq, Eq)] pub struct ScheduleTable { pub entries: Vec<ScheduleEntry> }`
  - `pub fn encode_schedule_table(t: &ScheduleTable, out: &mut Vec<u8>)`; `pub fn decode_schedule_table(buf: &[u8]) -> Option<ScheduleTable>` — total: `None` on a short buffer, a version ≠ 1, `count > 32`, a length ≠ `8 + 33·count`, an unknown kind, `period_ns == 0`, `secs_of_day >= 86_400`, or a duplicate `(identity_hash, timer_id)`.
  - `impl ScheduleRule { pub const fn next_after(&self, t_ns: u64) -> u64 }` — the first occurrence strictly after `t_ns`; `pub const fn latest_at_or_before(&self, t_ns: u64) -> Option<u64>` — the latest occurrence `≤ t_ns` (`None` if before the anchor); `pub const fn arm(&self, last_delivered_ns: Option<u64>, log_time_ns: u64) -> u64` — the one-tick catch-up rule of spec §5: `match latest_at_or_before(log_time_ns) { Some(o) if Some(o) > last_delivered_ns => o, _ => next_after(last_delivered_ns.unwrap_or(log_time_ns)) }` (for `DailyAt`, "the anchor" is Unix epoch day 0). All saturating, all `u64`.
  - `uc_protocol::v2::ipc::SchedOp::TableConsumed = 4`.
  - `pub const ADMIN_OP_SCHEDULE_APPLY: u32 = 6;` (in `uc_protocol/src/v2/cnc.rs` beside the admin-line docs) and the reason codes the node will use: `ADMIN_REASON_SCHEDULE_DIGEST = 40`, `ADMIN_REASON_SCHEDULE_MISSING = 41`, `ADMIN_REASON_SCHEDULE_DECODE = 42`, `ADMIN_REASON_SCHEDULE_UNKNOWN_FSM = 43` (check the existing reason-code range in `uc_node/src/audit.rs` and pick the next free block if 40 collides — say so in the report).
- Consumed by: Tasks 2, 3, 4, 5, 6, 7.

- [ ] **Step 1: Write the failing tests** in `uc_protocol/src/v2/schedule.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    const H: u64 = 60 * 60 * 1_000_000_000;
    const DAY: u64 = 24 * H;

    /// FROZEN wire layout: header(8) ‖ entries × 33. Never change these bytes.
    #[test]
    fn table_codec_pins_bytes_and_is_total() {
        let t = ScheduleTable {
            entries: alloc::vec![
                ScheduleEntry { identity_hash: 0x0102_0304_0506_0708, timer_id: 7, rule: ScheduleRule::Every { period_ns: H, anchor_ns: 5 } },
                ScheduleEntry { identity_hash: 9, timer_id: 8, rule: ScheduleRule::DailyAt { secs_of_day: 14 * 3600 } },
            ],
        };
        let mut b = alloc::vec::Vec::new();
        encode_schedule_table(&t, &mut b);
        assert_eq!(b.len(), SCHEDULE_HEADER_LEN + 2 * SCHEDULE_ENTRY_LEN);
        assert_eq!(&b[0..4], &1u32.to_le_bytes(), "version 1");
        assert_eq!(&b[4..6], &2u16.to_le_bytes(), "count");
        assert_eq!(&b[6..8], &[0, 0], "reserved");
        assert_eq!(&b[8..16], &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01], "hash LE");
        assert_eq!(&b[16..24], &7u64.to_le_bytes());
        assert_eq!(b[24], 1, "kind Every");
        assert_eq!(&b[25..33], &H.to_le_bytes());
        assert_eq!(&b[33..41], &5u64.to_le_bytes());
        assert_eq!(b[41 + 16], 2, "kind DailyAt");
        assert_eq!(decode_schedule_table(&b), Some(t.clone()));
        // totality
        assert_eq!(decode_schedule_table(&b[..7]), None, "short header");
        assert_eq!(decode_schedule_table(&b[..b.len() - 1]), None, "length mismatch");
        let mut v = b.clone(); v[0] = 2; assert_eq!(decode_schedule_table(&v), None, "version");
        let mut k = b.clone(); k[24] = 3; assert_eq!(decode_schedule_table(&k), None, "kind");
        let mut z = b.clone(); z[25..33].copy_from_slice(&0u64.to_le_bytes()); assert_eq!(decode_schedule_table(&z), None, "period 0");
        let mut d = b.clone(); d[41 + 17..41 + 25].copy_from_slice(&86_400u64.to_le_bytes()); assert_eq!(decode_schedule_table(&d), None, "secs_of_day out of range");
        let mut dup = t.clone(); dup.entries.push(dup.entries[0]);
        let mut db = alloc::vec::Vec::new(); encode_schedule_table(&dup, &mut db);
        assert_eq!(decode_schedule_table(&db), None, "duplicate (hash, id)");
        let big = ScheduleTable { entries: (0..33).map(|i| ScheduleEntry { identity_hash: 1, timer_id: i, rule: ScheduleRule::Every { period_ns: 1, anchor_ns: 0 } }).collect() };
        let mut bb = alloc::vec::Vec::new(); encode_schedule_table(&big, &mut bb);
        assert_eq!(decode_schedule_table(&bb), None, "33 entries refused");
        assert_eq!(MAX_SCHEDULE_ENTRIES, 32);
        assert!(SCHEDULE_HEADER_LEN + MAX_SCHEDULE_ENTRIES * SCHEDULE_ENTRY_LEN <= 1312, "fits the crypto-on payload ceiling");
    }

    #[test]
    fn every_rule_arithmetic() {
        let r = ScheduleRule::Every { period_ns: H, anchor_ns: 10 * H };
        assert_eq!(r.next_after(0), 10 * H, "before the anchor: the anchor");
        assert_eq!(r.next_after(10 * H), 11 * H, "strictly after");
        assert_eq!(r.next_after(10 * H + 1), 11 * H);
        assert_eq!(r.latest_at_or_before(9 * H), None);
        assert_eq!(r.latest_at_or_before(10 * H), Some(10 * H));
        assert_eq!(r.latest_at_or_before(12 * H + 5), Some(12 * H));
        // arm: one-tick catch-up
        assert_eq!(r.arm(None, 12 * H + 5), 12 * H, "missed ticks collapse to the latest");
        assert_eq!(r.arm(Some(12 * H), 12 * H + 5), 13 * H, "already delivered: the next");
        assert_eq!(r.arm(Some(11 * H), 12 * H + 5), 12 * H, "one behind: the latest, once");
        assert_eq!(r.arm(None, 0), 10 * H, "before the anchor: the anchor");
        assert_eq!(r.next_after(u64::MAX - 1), u64::MAX, "saturates");
    }

    #[test]
    fn daily_rule_arithmetic() {
        let r = ScheduleRule::DailyAt { secs_of_day: 14 * 3600 };
        let d0_14 = 14 * H;
        assert_eq!(r.next_after(0), d0_14);
        assert_eq!(r.next_after(d0_14), DAY + d0_14, "strictly after");
        assert_eq!(r.latest_at_or_before(d0_14 - 1), None);
        assert_eq!(r.latest_at_or_before(DAY + d0_14 + 1), Some(DAY + d0_14));
        assert_eq!(r.arm(None, 3 * DAY + 1), 2 * DAY + d0_14, "latest past occurrence, once");
        assert_eq!(r.arm(Some(2 * DAY + d0_14), 3 * DAY + 1), 3 * DAY + d0_14);
    }
}
```

In `ipc.rs`'s `sched_record_pins_literal_bytes_and_rejects_bad_ops`: add `(SchedOp::TableConsumed, 4u8)` to the valid loop and change the "op 4 is not a record" assertion to op `5`. In `frame.rs`'s `frame_type_codes_are_stable`: `assert_eq!(FRAME_TYPE_SCHEDULE_TABLE, 6);`.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc_protocol schedule && cargo test -p uc_protocol ipc && cargo test -p uc_protocol frame`
Expected: compile errors (module missing; `TableConsumed` unknown; `FRAME_TYPE_SCHEDULE_TABLE` unknown).

- [ ] **Step 3: Implement** `uc_protocol/src/v2/schedule.rs` (header per the crate's other v2 modules; `extern crate alloc` is already in the crate — check `v2/config.rs`'s imports and mirror them):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The replicated schedule table (time-and-timers spec §5, plan 2): the
//! frozen wire body of a `FRAME_TYPE_SCHEDULE_TABLE` frame and the pure
//! recurrence arithmetic every node runs identically. `core`-only.

use alloc::vec::Vec;

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
    /// First occurrence strictly after `t_ns` (saturating).
    pub const fn next_after(&self, t_ns: u64) -> u64 {
        match *self {
            ScheduleRule::Every { period_ns, anchor_ns } => {
                if t_ns < anchor_ns {
                    return anchor_ns;
                }
                let k = (t_ns - anchor_ns) / period_ns + 1;
                anchor_ns.saturating_add(k.saturating_mul(period_ns))
            }
            ScheduleRule::DailyAt { secs_of_day } => {
                let off = secs_of_day as u64 * NS_PER_SEC;
                let day = t_ns / NS_PER_DAY;
                let today = day.saturating_mul(NS_PER_DAY).saturating_add(off);
                if today > t_ns { today } else { (day + 1).saturating_mul(NS_PER_DAY).saturating_add(off) }
            }
        }
    }
    /// Latest occurrence at or before `t_ns`; `None` before the first.
    pub const fn latest_at_or_before(&self, t_ns: u64) -> Option<u64> {
        match *self {
            ScheduleRule::Every { period_ns, anchor_ns } => {
                if t_ns < anchor_ns {
                    return None;
                }
                Some(anchor_ns + (t_ns - anchor_ns) / period_ns * period_ns)
            }
            ScheduleRule::DailyAt { secs_of_day } => {
                let off = secs_of_day as u64 * NS_PER_SEC;
                let day = t_ns / NS_PER_DAY;
                let today = day * NS_PER_DAY + off;
                if today <= t_ns { Some(today) } else if day == 0 { None } else { Some((day - 1) * NS_PER_DAY + off) }
            }
        }
    }
    /// Spec §5 one-tick catch-up: the latest missed occurrence if any is
    /// newer than what was delivered, else the next one.
    pub const fn arm(&self, last_delivered_ns: Option<u64>, log_time_ns: u64) -> u64 {
        let latest = self.latest_at_or_before(log_time_ns);
        match (latest, last_delivered_ns) {
            (Some(o), None) => o,
            (Some(o), Some(l)) if o > l => o,
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
            ScheduleRule::Every { period_ns, anchor_ns } => (1u8, period_ns, anchor_ns),
            ScheduleRule::DailyAt { secs_of_day } => (2u8, secs_of_day as u64, 0),
        };
        out.push(kind);
        out.extend_from_slice(&a.to_le_bytes());
        out.extend_from_slice(&b.to_le_bytes());
    }
}

/// Total on any input (see the module doc's refusal list).
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
            1 if a > 0 => ScheduleRule::Every { period_ns: a, anchor_ns: b },
            2 if a < 86_400 && b == 0 => ScheduleRule::DailyAt { secs_of_day: a as u32 },
            _ => return None,
        };
        if entries.iter().any(|e: &ScheduleEntry| e.identity_hash == identity_hash && e.timer_id == timer_id) {
            return None;
        }
        entries.push(ScheduleEntry { identity_hash, timer_id, rule });
    }
    Some(ScheduleTable { entries })
}
```

`const fn` with `match` guards and `Option` is fine on 1.89; if `if`-guards in a `const fn` `match` are refused, drop `const` from `arm` only (say so). Add `FRAME_TYPE_SCHEDULE_TABLE` to `frame.rs` with a doc comment in the style of `FRAME_TYPE_CONFIG` ("payload = `v2::schedule::encode_schedule_table` bytes; appended by a serving leader on a verified `schedule_apply` admin request; adopted at append (leader) / at durable recording (follower, archive scan); the apply layer skips it"). Add `TableConsumed = 4` to `SchedOp` and the `4 =>` arm to `read_sched_record`. Add the admin constants to `cnc.rs`.

- [ ] **Step 4: Run** `cargo fmt --all && cargo clippy -p uc_protocol --all-targets -- -D warnings && cargo test -p uc_protocol && (cd fuzz && RUSTFLAGS="--cfg fuzzing" cargo +nightly check)` — the existing `uc_protocol_sched_record` fuzz target must still compile (it round-trips any decoded op).
Expected: PASS.

- [ ] **Step 5: Commit** `feat(uc_protocol): schedule table — FRAME_TYPE_SCHEDULE_TABLE, frozen 33-byte-entry codec, Every/DailyAt arithmetic with one-tick arm, SchedOp::TableConsumed, admin op 6 (tests: table_codec_pins_bytes_and_is_total, every_rule_arithmetic, daily_rule_arithmetic)`.

---

### Task 2: `uc_log` — append the table; the archive observes it

**Files:**
- Modify: `uc_log/src/buffer.rs` (beside `append_config`), `uc_log/src/archive.rs` (`Archive` fields, `observe_terms`, the take/retain pair beside `take_config_observations`)
- Test: both files' `mod tests`

**Interfaces:**
- Produces:
  - `Appender::append_schedule_table(&mut self, term: u32, payload: &[u8]) -> Result<u64, AppendError>` — a `FRAME_TYPE_SCHEDULE_TABLE` frame, `client_id = seq = 0`, `flags = 0`, stamped like every frame (`max(now, last)`), returning the frame **END** position (same convention as `append_config`: the adoption effect point).
  - `Archive::take_table_observations(&mut self) -> Vec<(u64, u64, Vec<u8>)>` — `(frame_end_position, time_ns, payload)` for every `FRAME_TYPE_SCHEDULE_TABLE` frame recorded since the last call; `Archive::retain_table_observations(&mut self, unsent: Vec<_>)` mirrors the config pair.
- Consumed by: Task 4.

- [ ] **Step 1: Write the failing tests.** `buffer.rs` `mod tests` (uses the module's `buf()` helper and the `headers()` helper Task 3 of plan 1 added):

```rust
    #[test]
    fn append_schedule_table_is_a_stamped_type_6_frame_returning_the_end() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 4, 0);
        a.set_now(1_000);
        let end = a.append_schedule_table(4, b"table-bytes").unwrap();
        assert_eq!(end, 64, "32 header + 11 payload -> aligned 64; END returned");
        let s = b.recordable_slice(0, 64).unwrap();
        let h = read_header(s);
        assert_eq!(h.frame_type, FRAME_TYPE_SCHEDULE_TABLE);
        assert_eq!((h.client_id, h.seq, h.flags), (0, 0, 0));
        assert_eq!(h.time_ns, 1_000);
        assert_eq!(&s[HEADER_LEN..HEADER_LEN + 11], b"table-bytes");
        assert_eq!(a.last_stamp(), 1_000);
    }
```

`archive.rs` `mod tests` (copy the setup of `archive_publishes_the_highest_recorded_stamp_and_never_lowers_it`):

```rust
    #[test]
    fn archive_observes_schedule_table_frames_with_end_position_and_stamp() {
        // setup as in the stamp test above: buffer b, cnc c, archive over a temp journal
        let mut a = Appender::new(Arc::clone(&b), 1, 0);
        a.set_now(500);
        a.append(1, 1, b"x").unwrap();                       // [0, 64)
        a.set_now(700);
        let end = a.append_schedule_table(1, b"tbl").unwrap(); // [64, 128)
        assert_eq!(end, 128);
        archive.do_work(&b).unwrap();
        let obs = archive.take_table_observations();
        assert_eq!(obs, vec![(128, 700, b"tbl".to_vec())]);
        assert!(archive.take_table_observations().is_empty(), "drained");
        archive.retain_table_observations(obs.clone());
        assert_eq!(archive.take_table_observations(), obs, "retained comes back");
        assert!(archive.take_config_observations().is_empty(), "not confused with CONFIG");
    }
```

- [ ] **Step 2: Run to verify they fail** — `cargo test -p uc_log schedule_table` → compile errors.

- [ ] **Step 3: Implement.** `append_schedule_table` is `append_config` with `FRAME_TYPE_SCHEDULE_TABLE` in the header (copy the body; same wrap/overrun/commit/counter discipline, same `PayloadTooLarge` check against `max_payload`, same END return). In `archive.rs`, add `table_observations: Vec<(u64, u64, Vec<u8>)>` (init empty; **not** reset by `truncate_to` — the consensus agent's own durable-position check discards a stale one, exactly as for CONFIG), and in `observe_terms`'s loop, after the CONFIG branch:

```rust
            if h.frame_type == FRAME_TYPE_SCHEDULE_TABLE {
                let payload_start = off + HEADER_LEN;
                let payload_end = off + h.length as usize;
                self.table_observations.push((
                    base + off as u64 + aligned as u64,
                    h.time_ns,
                    block[payload_start..payload_end].to_vec(),
                ));
            }
```

plus `take_table_observations`/`retain_table_observations` modelled on the config pair (`std::mem::take`; retain = prepend the unsent ones).

- [ ] **Step 4: Run** `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p uc_log` → PASS.

- [ ] **Step 5: Commit** `feat(uc_log): append_schedule_table (frame END, stamped) and the archive's table observations (tests: append_schedule_table_is_a_stamped_type_6_frame_returning_the_end, archive_observes_schedule_table_frames_with_end_position_and_stamp)`.

---

### Task 3: `RowTimers` — table entries that advance instead of leaving

**Files:**
- Modify: `uc_node/src/timers.rs`
- Test: its `mod tests`

**Interfaces:**
- Produces on `RowTimers`:
  - `pub fn adopt_table(&mut self, entries: &[(u64 /*id*/, ScheduleRule)], log_time_ns: u64)` — replaces this row's table: entries not in the new list are dropped (and their heap entries lazily discarded); a kept id keeps its `last_delivered`; every entry's `next` is recomputed with `rule.arm(last_delivered, log_time_ns)`.
  - `pub fn table_fired(&mut self, id: u64, deadline_ns: u64)` — leader, after a successful append: `next = rule.next_after(deadline_ns)`, re-pushed; nothing goes to `in_flight`.
  - `pub fn table_delivered(&mut self, id: u64, deadline_ns: u64)` — from the service's `TableConsumed` (follower path, and the post-attach announce): `last_delivered = max(last, deadline)`; if `next <= deadline` then `next = rule.next_after(deadline)` and re-push.
  - `peek_due(&mut self, now) -> Option<(u64, u64, bool /*table*/)>` — the heap is shared; the third field says which kind the head is (a table entry is stale in the heap when its deadline ≠ `entry.next`, exactly as a programmatic one is stale when ≠ `pending[id]`).
  - `take_in_flight` is unchanged and must NOT be called for a table head; `rearm` ignores table entries by construction (they are never in `in_flight`).
  - `pub fn table_len(&self) -> usize`; `pending_len()` now counts programmatic + table.
  - A heap key that cannot collide between the two kinds: use `Reverse<(u64 /*deadline*/, u64 /*id*/, bool /*table*/)>`.
- Consumed by: Task 4.

- [ ] **Step 1: Write the failing tests**:

```rust
    #[test]
    fn table_entries_advance_on_fire_and_never_enter_in_flight() {
        use uc_protocol::v2::schedule::ScheduleRule;
        let mut t = RowTimers::new(1);
        let r = ScheduleRule::Every { period_ns: 100, anchor_ns: 1_000 };
        t.adopt_table(&[(7, r)], 1_250); // log clock 1_250: latest missed occurrence 1_200 (one-tick catch-up)
        assert_eq!(t.peek_due(1_199), None);
        assert_eq!(t.peek_due(1_200), Some((7, 1_200, true)));
        t.table_fired(7, 1_200);
        assert_eq!(t.in_flight_len(), 0, "table ticks are never in flight");
        assert_eq!(t.peek_due(1_299), None);
        assert_eq!(t.peek_due(1_300), Some((7, 1_300, true)), "advanced from the fired deadline, not the clock");
        assert_eq!(t.rearm(), 0);
        assert_eq!(t.table_len(), 1);
    }

    #[test]
    fn table_delivered_advances_a_follower_and_adopt_keeps_last_delivered() {
        use uc_protocol::v2::schedule::ScheduleRule;
        let r = ScheduleRule::Every { period_ns: 100, anchor_ns: 0 };
        let mut f = RowTimers::new(1);
        f.adopt_table(&[(7, r)], 0);
        assert_eq!(f.peek_due(u64::MAX), Some((7, 0, true)), "first occurrence at the anchor");
        f.table_delivered(7, 300); // the log delivered ticks up to 300
        assert_eq!(f.peek_due(u64::MAX), Some((7, 400, true)));
        f.table_delivered(7, 100); // an old report never moves it back
        assert_eq!(f.peek_due(u64::MAX), Some((7, 400, true)));
        // re-adoption of the same id keeps last_delivered and re-arms from the clock
        f.adopt_table(&[(7, r), (8, r)], 950);
        assert_eq!(f.peek_due(u64::MAX), Some((7, 900, true)), "one-tick catch-up above last_delivered=300");
        assert_eq!(f.table_len(), 2);
        // an id dropped from the table disappears
        f.adopt_table(&[(8, r)], 950);
        f.table_fired(8, 900);
        assert_eq!(f.peek_due(u64::MAX), Some((8, 1_000, true)));
        assert_eq!(f.table_len(), 1);
    }

    #[test]
    fn programmatic_and_table_share_the_heap_in_deadline_order() {
        use uc_protocol::v2::schedule::ScheduleRule;
        let mut t = RowTimers::new(1);
        t.schedule(1, 500);
        t.adopt_table(&[(1, ScheduleRule::Every { period_ns: 1_000, anchor_ns: 400 })], 0); // same id 1: distinct kinds
        assert_eq!(t.peek_due(1_000), Some((1, 400, true)));
        t.table_fired(1, 400);
        assert_eq!(t.peek_due(1_000), Some((1, 500, false)));
        t.take_in_flight(1, 500);
        assert_eq!(t.peek_due(1_000), None);
        assert_eq!(t.pending_len(), 1, "the table entry (next 1_400) still counts as pending");
    }
```

- [ ] **Step 2: Run to verify they fail** — `cargo test -p uc_node --lib timers` → compile errors.

- [ ] **Step 3: Implement.** Add to `RowTimers`:

```rust
struct TableEntry { rule: ScheduleRule, next: u64, last_delivered: Option<u64> }
// field:
table: HashMap<u64, TableEntry>,
// heap element becomes Reverse<(u64, u64, bool)>
```

`peek_due` checks staleness per kind: `table == false` → `pending.get(&id) == Some(&dl)`; `table == true` → `self.table.get(&id).map(|e| e.next) == Some(dl)`. `adopt_table`: retain `last_delivered` for kept ids, drop others, `next = rule.arm(last_delivered, log_time_ns)`, push. `table_fired`: `if let Some(e) = self.table.get_mut(&id) && e.next == deadline_ns { e.next = e.rule.next_after(deadline_ns); heap.push(...) }`. `table_delivered`: `e.last_delivered = max(..)`; `if e.next <= deadline_ns { e.next = e.rule.next_after(deadline_ns); push }`. `pending_len = pending.len() + table.len()`. Existing tests must stay green (they use the `(id, dl)` shape of `peek_due` — update their destructuring to the triple).

- [ ] **Step 4: Run** `cargo fmt --all && cargo clippy -p uc_node --all-targets -- -D warnings && cargo test -p uc_node --lib timers` → PASS (7 tests).

- [ ] **Step 5: Commit** `feat(uc_node): RowTimers table entries — adopt/arm, advance on fire, advance on delivered, shared heap by kind (tests: table_entries_advance_on_fire_and_never_enter_in_flight, table_delivered_advances_a_follower_and_adopt_keeps_last_delivered, programmatic_and_table_share_the_heap_in_deadline_order)`.

---

### Task 4: `uc_node` — adoption, persistence, boot arming, firing with the flag, the admin op, observability

**Files:**
- Create: `uc_node/src/schedule_state.rs` (`ScheduleRecord`, the `StableValue` open/load/store helpers, the digest)
- Modify: `uc_node/src/node.rs` (archive thread: feed `take_table_observations` over a new `tbl_obs_tx` channel, retained like config; `Consensus` fields; `do_work` step 1c: drain `tbl_obs_rx` → `adopt_table_frame(position, time_ns, payload)`; `fire_due_timers`: the table branch; `drain_sched_rings`: `TableConsumed`; `handle_admin`: the `ADMIN_OP_SCHEDULE_APPLY` arm; boot: load the record and arm; `publish_timers_pending` unchanged), `uc_node/src/audit.rs` (`op_name(6) = "schedule_apply"`, reason names), `uc_node/src/obs/metrics.rs` + `obs/mod.rs` (`uc2_schedule_table_position` gauge, `uc2_schedule_entries` gauge, `uc2_schedule_apply_refused_total` counter), `uc_node/src/lib.rs`
- Test: `uc_node/tests/timers.rs` (Task 7 carries the end-to-end tests; this task's own tests are the unit tests in `schedule_state.rs` and one `services.rs`-style boot test)

**Interfaces:**
- `pub(crate) struct ScheduleRecord { pub position: u64, pub time_ns: u64, pub table: Vec<u8> /* encoded */ }` (serde, like `ConfigRecord`), stored in `state/schedules.state` via `StableValue<ScheduleRecord>` (`StableValueConfig::new(path)`; open on `Node::start` beside the other state files — find where `config.state` is opened and mirror it).
- `pub(crate) fn schedule_digest(bytes: &[u8]) -> (u32, u32, u16)` — the first 10 bytes of SHA-256 over `bytes` as `(id, ip, port)` LE (use the SHA-256 the admin HMAC path already depends on — `grep -n "sha2\|hmac" uc_node/Cargo.toml uc_crypto/Cargo.toml`).
- `pub(crate) const SCHEDULE_PENDING_FILE: &str = "schedules.pending";`
- `Consensus::adopt_table_frame(&mut self, position: u64, time_ns: u64, payload: &[u8])`: decode (fail-stop on `None`, like CONFIG — the block is CRC-covered); if `position <= self.schedule_position` return (idempotent, by position); store `ScheduleRecord`; for each declared row: `adopt_table(entries for that row's hash, time_ns)`; set `schedule_position`; obs event `schedule_table_adopted { position, entries }`.
- Boot: after the timers are constructed, `if let Some(rec) = load` → `adopt_table_frame`-like arming with `log_time_ns = self.cnc.log_time_ns()` (the recovered clock, Task 3 of plan 1) — NOT the record's `time_ns` (one-tick catch-up from the log's clock, spec §5).
- Leader append: `append_schedule_table_frame(&mut self, table: &ScheduleTable) -> Result<u64, AppendError>` encodes, appends via `append_schedule_table(term, &bytes)`, then adopts at append (`adopt_table_frame(end, self.appender.last_stamp(), &bytes)` — the appender's last stamp IS the frame's stamp).
- `fire_due_timers`: when `peek_due` says `table == true`, append with `flags = FLAG_TIMER_TABLE`, then `t.table_fired(id, dl)` instead of `take_in_flight`; counters as today.
- `drain_sched_rings`: `SchedOp::TableConsumed => t.table_delivered(r.timer_id, r.deadline_ns)`.
- `handle_admin`, new arm for `req.op == ADMIN_OP_SCHEDULE_APPLY` after `verify_admin`: leader-only (follower: `status = 2`, reason `0`, no forward — reuse the "no leader / cannot forward" reply shape but skip the forward); read `<instance_dir>/schedules.pending` (missing → `1`/`SCHEDULE_MISSING`), check `schedule_digest(&bytes) == (req.id, req.ip, req.port)` (else `1`/`SCHEDULE_DIGEST`), decode (else `1`/`SCHEDULE_DECODE`), every `identity_hash` must be a declared row's hash (else `1`/`SCHEDULE_UNKNOWN_FSM`), append (`WouldOverrun` → `2`), reply `0` with `version = position`; audit every outcome through `audit_admin` with the op name `schedule_apply` (the `AuditedReq` carries `id/ip/port`, which here are the digest — fine, it is what was signed); delete the staged file after a successful append.
- Metrics: `uc2_schedule_table_position` (gauge: the adopted frame END, 0 = none), `uc2_schedule_entries` (gauge), `uc2_schedule_apply_refused_total` (counter, `ObsSources` field). `CONTRACT_SERIES` updated; `uc2ctl status` prints `schedule_position=` (Task 6).

- [ ] **Step 1: Write the failing tests.** `uc_node/src/schedule_state.rs` `mod tests`: `digest_is_the_first_ten_bytes_of_sha256_le` (pin against a known vector: SHA-256 of `b"abc"` starts `ba7816bf 8f01cfea 414140de`, so `id = 0xbf1678ba`, `ip = 0xeacf018f`, `port = 0x4041`), `record_roundtrips_through_a_stable_value` (temp dir under the cargo target tree). `uc_node/tests/services.rs`: `a_node_reloads_its_schedule_record_at_boot_and_arms_from_the_log_clock` — start a node, write a `ScheduleRecord` for a declared row through the crate's `pub(crate)`... — not reachable from an integration test; instead make this a `#[cfg(test)]` unit test in `node.rs` if the two in-module constructors allow (they do: `node.rs` has `mod tests` with `on_collapsed` helpers), or defer the boot-arming assertion to Task 7's end-to-end restart test (preferred: Task 7's test restarts a node with a live table and asserts ticks resume without a flood). Choose the latter and say so.

- [ ] **Step 2: Run to verify they fail** — `cargo test -p uc_node --lib schedule_state` → compile errors.

- [ ] **Step 3: Implement** in the order: `schedule_state.rs` → `Consensus` fields (`schedule_position: u64`, `schedule_state: StableValue<ScheduleRecord>`, `tbl_obs_rx`, `schedule_refused: Arc<AtomicU64>`) in both constructors → archive thread feed (copy the config loop: `for obs in archive.take_table_observations() { if tbl_obs_tx.send(obs).is_err() { break; } }`) → step 1c drain (after the config drain, with the same `position > durable` plausibility skip) → boot arming → `append_schedule_table_frame` → `fire_due_timers` table branch → `drain_sched_rings` arm → `handle_admin` arm → audit names → metrics.

For the `fire_due_timers` change the shape is:

```rust
            let Some((row, id, dl, table)) = best else { hold = false; break; };
            // ...
            let flags = if table { FLAG_TIMER_TABLE } else { 0 };
            match app.append_timer(&body, flags) {
                Ok((position, stamp)) => {
                    if table { t.table_fired(id, dl); } else { t.take_in_flight(id, dl); }
                    // counters + timer_late as today
```

- [ ] **Step 4: Run** the whole-workspace set (Global Constraints) — every existing test stays green (no table is ever applied by an existing test).

- [ ] **Step 5: Commit** `feat(uc_node): schedule table — adopted from the log (leader at append, followers from the archive), persisted in state/schedules.state, armed at boot from the log clock, fired with FLAG_TIMER_TABLE and advanced at append; schedule_apply admin op (staged file + signed digest, leader-only); metrics (tests: digest_is_the_first_ten_bytes_of_sha256_le, record_roundtrips_through_a_stable_value)`.

---

### Task 5: `uc_service` — `Timed` reports table ticks distinctly and announces `table_last`

**Files:**
- Modify: `uc_service/src/traits.rs` (`ApplyCtx::consumed_table` `pub(crate)`; `take_sched_records` emits `TableConsumed`; `RawStateMachine::table_delivered(&self) -> Vec<(u64, u64)>` provided, default empty), `uc_service/src/timed.rs` (`on_timer` calls `ctx.consumed_table` when `ev.table`; override `table_delivered`), `uc_service/src/session.rs` (`Sessioned` forwards `table_delivered`), `uc_service/src/apply.rs` (the announce step also writes one `TableConsumed` per `table_delivered()` entry)
- Test: `uc_service/tests/timed.rs`

**Interfaces:**
- `ApplyCtx::consumed_table(&mut self, id: u64, deadline_ns: u64)` → a `SchedRecord { op: TableConsumed, .. }` from `take_sched_records`.
- `RawStateMachine::table_delivered(&self) -> Vec<(u64, u64)>` — `Timed` returns its `table_last` map (sorted by id); the announce step sends it so a node rebuilds `last_delivered` for every table entry from the service that actually delivered them.
- Consumed by: Task 4 (already drains `TableConsumed`), Task 7.

- [ ] **Step 1: Write the failing tests** in `uc_service/tests/timed.rs` (reuse `Rec`, `ctx`, `ev`; add `tev(id, dl)` = `TimerEvent { id, deadline_ns: dl, table: true }`):

```rust
#[test]
fn table_ticks_deliver_strictly_increasing_deadlines_and_report_table_consumed() {
    let mut t = Timed::new(Rec::default());
    let mut c = ctx(64, 1_000);
    t.on_timer(&mut c, tev(5, 1_000));
    assert_eq!(t.inner().fired, vec![(64, 5, 1_000)]);
    let recs = c.take_sched_records_for_test(); // see note
    assert_eq!(recs.len(), 1);
    assert_eq!((recs[0].op, recs[0].timer_id, recs[0].deadline_ns), (SchedOp::TableConsumed, 5, 1_000));
    let mut c = ctx(128, 1_000);
    t.on_timer(&mut c, tev(5, 1_000)); // duplicate (a re-fire): dropped, still reported
    assert_eq!(t.inner().fired.len(), 1);
    assert_eq!(c.take_sched_records_for_test()[0].op, SchedOp::TableConsumed);
    let mut c = ctx(192, 900);
    t.on_timer(&mut c, tev(5, 900)); // an OLDER tick after a newer one: dropped
    assert_eq!(t.inner().fired.len(), 1);
    t.on_timer(&mut ctx(256, 2_000), tev(5, 2_000));
    assert_eq!(t.inner().fired.len(), 2);
    assert_eq!(t.table_delivered(), vec![(5, 2_000)]);
    // programmatic and table ids do not interfere
    t.apply(&mut ctx(300, 2_000), b"s5@3000", &mut Vec::new());
    assert_eq!(t.pending(), vec![(5, 3_000)]);
    assert_eq!(t.table_delivered(), vec![(5, 2_000)]);
}
```

(`take_sched_records` is `pub(crate)`; expose a `#[doc(hidden)] pub fn take_sched_records_for_test` on `ApplyCtx` or move this test into `uc_service/src/timed.rs`'s `mod tests` — prefer the in-crate unit test; adjust the imports accordingly.) Plus a snapshot round-trip extension: after install, `table_delivered()` equals the original's.

- [ ] **Step 2: Run to verify it fails** — `cargo test -p uc_service timed` → compile errors.

- [ ] **Step 3: Implement.** `traits.rs`: `consumed_table` pushes into a second private list (or the same list with a kind); `take_sched_records` maps it to `SchedOp::TableConsumed`; provided `fn table_delivered(&self) -> Vec<(u64, u64)> { Vec::new() }` on `RawStateMachine` (NOT forwarded by the blanket impl). `timed.rs`: in `on_timer`, replace the unconditional `ctx.consumed(..)` with `if ev.table { ctx.consumed_table(ev.id, ev.deadline_ns) } else { ctx.consumed(ev.id, ev.deadline_ns) }`; `fn table_delivered(&self) -> Vec<(u64,u64)> { self.table_last.iter().map(|(&i,&d)| (i,d)).collect() }`. `session.rs`: forward. `apply.rs` announce step: after the `Schedule` records, one `SchedRecord { op: TableConsumed, timer_id, deadline_ns }` per `sm.table_delivered()` entry.

- [ ] **Step 4: Run** `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p uc_service --features apply-profile --all-targets -- -D warnings && cargo test -p uc_service` → PASS.

- [ ] **Step 5: Commit** `feat(uc_service): Timed reports table ticks as TableConsumed and announces table_last after attach/replay (test: table_ticks_deliver_strictly_increasing_deadlines_and_report_table_consumed)`.

---

### Task 6: `uc2ctl schedule apply` and `schedule show`

**Files:**
- Create: `uc_ctl/src/schedule.rs` (TOML → `ScheduleTable`; name resolution; the staged file; the request; the reply)
- Modify: `uc_ctl/src/main.rs` (`Cmd::Schedule(ScheduleArgs)` with subcommands `Apply { file }` / `Show`; wire into `main`'s dispatch; `status` prints `schedule_position=`), `uc_ctl/Cargo.toml` (`toml` + `serde` if not already deps — check; `sha2` via the same crate the HMAC signing uses)
- Test: `uc_ctl/src/schedule.rs` `mod tests` (parsing + validation + encoding), `uc_node/tests/admin_auth.rs` (Task 7 runs the request end to end)

**Interfaces:**
- TOML per spec §5: `[[schedule]] fsm = "<name>" id = <u64> every = "<dur>" anchor = "<rfc3339>"` OR `at = "HH:MM[:SS]"`. Durations: `<n>(ns|us|ms|s|m|h|d)`; RFC 3339 parsed with the crate the tree already uses for timestamps (`grep -rn "chrono\|time = " Cargo.toml uc_*/Cargo.toml`; if none, accept only the `YYYY-MM-DDTHH:MM:SSZ` form and parse it by hand — the plan does not add a date crate for one field).
- `pub fn parse_table(toml_text: &str, resolve: impl Fn(&str) -> Option<u64>) -> Result<ScheduleTable, ScheduleFileError>` — `resolve` maps an FSM name to its identity hash via the node's cnc name lines (`cnc.service_slot(row).identity.name()/hash()` for each declared row); errors by name: unknown fsm, duplicate `(fsm, id)`, both/neither of `every`/`at`, bad duration, bad time, > 32 entries, encoded length > the ceiling.
- `pub fn apply(common: &CommonArgs, file: &Path) -> anyhow::Result<()>`: parse → encode → write `<instance_dir>/schedules.pending` (write to a temp name, fsync, rename; mode 0600) → `schedule_digest` (the same function as the node's; put it in `uc_protocol::v2::schedule` as `pub fn digest_fields(bytes) -> (u32, u32, u16)` so both sides share it — move Task 4's helper there) → build `AdminReq { op: ADMIN_OP_SCHEDULE_APPLY, id, ip, port, .. }` and send it through the existing signed-request helper (`uc_ctl/src/main.rs` ~416-490: `write_admin_req` + `read_admin_resp` + the HMAC line) → print `applied: position=<version>` or the refusal by reason name (extend the reason-name table with the four new codes).
- `pub fn show(common: &CommonArgs)`: read `state/schedules.state` through `uc_node::backup`'s `open_state_readonly::<ScheduleRecord>` (make `ScheduleRecord` `pub` in `uc_node` and re-export it) and print one line per entry (`fsm=<name via cnc> id=<id> rule=<every …|at …>`), or `no schedule table adopted`.

- [ ] **Step 1: Write the failing tests** in `uc_ctl/src/schedule.rs`:

```rust
    #[test]
    fn parses_both_rules_resolves_names_and_refuses_by_name() {
        let resolve = |n: &str| match n { "orders" => Some(0xabc), "kv" => Some(0xdef), _ => None };
        let t = parse_table(r#"
[[schedule]]
fsm = "orders"
id = 1
every = "1h"
anchor = "2026-01-01T00:00:00Z"
[[schedule]]
fsm = "kv"
id = 2
at = "14:00"
"#, resolve).unwrap();
        assert_eq!(t.entries.len(), 2);
        assert_eq!(t.entries[0].identity_hash, 0xabc);
        assert_eq!(t.entries[0].rule, ScheduleRule::Every { period_ns: 3_600_000_000_000, anchor_ns: 1_767_225_600_000_000_000 });
        assert_eq!(t.entries[1].rule, ScheduleRule::DailyAt { secs_of_day: 50_400 });
        let e = parse_table("[[schedule]]\nfsm = \"nope\"\nid = 1\nevery = \"1s\"\nanchor = \"2026-01-01T00:00:00Z\"\n", resolve).unwrap_err();
        assert!(e.to_string().contains("nope"), "{e}");
        let e = parse_table("[[schedule]]\nfsm = \"kv\"\nid = 1\nevery = \"1s\"\nat = \"14:00\"\n", resolve).unwrap_err();
        assert!(e.to_string().contains("either"), "{e}");
        let e = parse_table("[[schedule]]\nfsm = \"kv\"\nid = 1\nevery = \"0s\"\nanchor = \"2026-01-01T00:00:00Z\"\n", resolve).unwrap_err();
        assert!(e.to_string().contains("period"), "{e}");
    }
```

(1_767_225_600 is 2026-01-01T00:00:00Z as Unix seconds — verify with `date -u -d 2026-01-01T00:00:00Z +%s` before committing.)

- [ ] **Step 2: Run to verify it fails** — `cargo test -p uc_ctl schedule` → compile errors.

- [ ] **Step 3: Implement** per the interfaces; the clap shape mirrors `AddLearner(AddLearnerArgs)` with a nested `#[command(subcommand)]`.

- [ ] **Step 4: Run** `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p uc_ctl && cargo run -p uc_ctl -- schedule --help` → PASS; the help text lists `apply` and `show`.

- [ ] **Step 5: Commit** `feat(uc_ctl): schedule apply (TOML → staged file → signed digest request) and schedule show (test: parses_both_rules_resolves_names_and_refuses_by_name)`.

---

### Task 7: End-to-end tests, the capstone's table clause, the fuzz target

**Files:**
- Modify: `uc_node/tests/timers.rs` (two tests), `uc_node/tests/admin_auth.rs` (one test), `uc_lincheck/src/timer.rs` (the oracle's table clause + `TimerSm` records `table` on `FiredRec`), `uc_node/tests/lin_v2.rs` (the capstone applies a table with a 150 ms rule to row 1 before the churn), `fuzz/fuzz_targets/uc_protocol_schedule_table.rs` (new), `fuzz/Cargo.toml`, `fuzz/src/seeds.rs`, `fuzz/src/bin/seed_corpus.rs`, `fuzz/README.md`, `.github/workflows/nightly.yml` (`FUZZ_GROUPS` fifth leg gains the target: still ≤ 4 per leg), `docs/VERIFICATION.md` (row + count)

**Tests to write (each watched red — say against what):**

1. `uc_node/tests/timers.rs::a_schedule_table_ticks_exactly_once_per_deadline_and_advances_from_the_tick` — single node, `Timed<ClockSm>`; apply a table (`every = 200ms`, anchor = now rounded down) through the in-process admin path (write the staged file + `write_admin_req` as `admin_auth.rs` does; `AdminPolicy::Filesystem`); wait for ≥ 5 fires; assert every fire has `table == true` (extend `ClockSm::Fired` with `table: bool`), deadlines strictly increase by exactly 200 ms, each `time_ns == deadline_ns` (on time), no duplicates, and `uc2_schedule_entries`/the cnc `timers_pending` count ≥ 1. Red: with the `fire_due_timers` table branch stubbed.
2. `uc_node/tests/timers.rs::a_restarted_node_resumes_the_table_with_one_catch_up_tick` — same, then stop the node and service, sleep 1 s (5 ticks' worth), restart both on the same dir; assert the first post-restart fire's deadline is the LATEST missed occurrence (one tick, not five) and ticks continue; assert the record was reloaded (`uc2_schedule_table_position` > 0 immediately after start, via `/metrics` or the `uc2ctl status` line). Red: with boot arming disabled.
3. `uc_node/tests/admin_auth.rs::schedule_apply_is_signed_digest_checked_leader_only_and_audited` — under `AdminPolicy::Hmac`: a correctly signed request with a matching digest is accepted (`status 0`, audit line `op=schedule_apply outcome=accepted`); a request whose digest does not match the staged bytes is refused (`reason = SCHEDULE_DIGEST`, audited as refused); the same request on a follower answers `status 2`; a request naming an undeclared FSM hash is refused (`SCHEDULE_UNKNOWN_FSM`). Red: each branch by commenting out its check.
4. Capstone: `TimerSm` gains `table: bool` on `FiredRec`; `assert_timer_report` gains clause (7): table fires per `(id)` have strictly increasing deadlines, each `deadline == rule.next_after(previous)` for the rule the test applied (pass the rule into the checker), and `time_ns >= deadline`; `two_fsm_timer_churn_under_failover` applies a 150 ms `every` table to row 1 (leader-only apply at start) and asserts ≥ 20 table fires with the clause, replication-equivalent across nodes. Red: with `table_fired` not advancing.
5. Fuzz: `uc_protocol_schedule_table` (decode → encode → decode equality; `arm`/`next_after` total on the decoded rules for a fuzzed `t`), seeds: the two-entry table, a 32-entry table, short, bad kind, duplicate.

- [ ] **Step 1–3**: write, run red, implement per the list; the capstone and crashtest runs record seeds/wall time/fire counts.
- [ ] **Step 4: Run** the Global Constraints set + `cargo test -p uc_node --test timers -- --test-threads=1` + `cargo test -p uc_node --test lin_v2 two_fsm_timer_churn_under_failover -- --nocapture` + `scripts/fuzz_smoke.sh --min-runs 1000 30 uc_protocol_schedule_table` (check the script's argument order first).
- [ ] **Step 5: Commit** `test: schedule table end to end (exactly-once ticks, one-tick catch-up after restart, signed/digest-checked/leader-only apply), the capstone's table clause, fuzz target uc_protocol_schedule_table`.

---

### Task 8: Docs, release bullet, runbook, attack surface, gate rows

**Files:**
- Modify: `docs/reference/wire-protocol.md` (frame type 6 + the body), `docs/reference/limits.md` (32 entries, the 1064 B body), `docs/reference/uc2ctl.md` (`schedule apply/show`), `docs/ops/uc2-runbook.md` (the staged file, the reason codes, `state/schedules.state`, `uc2ctl status`'s `schedule_position=`), `docs/how-to/monitor-a-cluster.md` (the three families), `docs/security/attack-surface.md` (the staged file is a local-write surface authenticated by the signed 80-bit digest under `hmac`, trusted under the filesystem policy like every admin op; the frame decoder is total and fuzzed), `docs/notes/uc2-log-time-and-timers-explained.md` (a "the schedule table" section: the one-tick catch-up, why the node advances at append, why a truncated tick is not re-fired), `RELEASES.md` + `docs/releases.md` (the third time bullet: the replicated schedule table), `docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md` (row e: a 32-entry table with 100 ms rules on one FSM, bar = `uc2_timers_late_total == 0` after warm-up and throughput within row a's resolution; result cell empty), `packaging/prometheus/uc2-alerts.yml` (`Uc2ScheduleTableDiverged`: `count(count_values("p", uc2_schedule_table_position)) > 1` for 60 s — every node must hold the same adopted position), `CLAUDE.md` ("Standing facts": the table; "Next up": plan 2 done), `docs/BACKLOG.md` (item 2a → done, pointer to the plan), the spec (§11 plan-2 items marked as-built)
- The sweep check: every constant/name in the docs exists in the tree; every link resolves.

- [ ] **Step 1–3**: write; `ls` every linked path; grep every named constant.
- [ ] **Step 4: Commit** `docs: the replicated schedule table — reference, runbook, attack surface, explainer section, release bullet, gate row e, alert`.

---

## Self-review (run before handing this plan over)

**Spec coverage (§5 as amended by Task 0).** Admin-applied TOML → Task 6; leader appends frame type 6 → Tasks 1, 2, 4; every node adopts from the archive walk → Tasks 2, 4; persisted in `state/schedules.state` → Task 4; applying replaces the whole table → Task 3 (`adopt_table` drops absent ids) + Task 4; entries hash-keyed, ≤ 32, two rules → Task 1; next deadline from the fired deadline → Tasks 1, 3, 4; first deadline from the frame's own stamp (and the one-tick catch-up) → Tasks 1, 3, 4; same heap, same TIMER frame with the flag, `ev.table` → Tasks 3, 4, 5 (+ plan 1's `Timed`); exactly-once via `table_last` → plan 1 + Task 5's `TableConsumed`; leader-only apply with the staged file + digest → Tasks 4, 6; observability → Tasks 4, 8; tests → Task 7; docs/release → Task 8. §10's "`node.toml [schedules]` as a boot-time convenience" stays a door.

**Placeholders.** Task 4's `handle_admin` arm and Task 6's `apply` are described against named existing helpers rather than reproduced (the admin path is 300 lines of existing code the executor reads); every other code step has code. Task 7 lists each test's assertions and its red condition.

**Type consistency.** `ScheduleRule::{next_after, latest_at_or_before, arm}` (Task 1) are what `RowTimers` (Task 3) and the capstone clause (Task 7) call. `RowTimers::{adopt_table, table_fired, table_delivered}` and the triple-returning `peek_due` (Task 3) are what `fire_due_timers`/`drain_sched_rings`/`adopt_table_frame` (Task 4) call. `SchedOp::TableConsumed` (Task 1) is what `ApplyCtx::consumed_table` emits (Task 5) and `drain_sched_rings` maps to `table_delivered` (Task 4). `ADMIN_OP_SCHEDULE_APPLY` + the four reason codes (Task 1) are what `handle_admin` (Task 4), `audit::op_name` (Task 4) and `uc2ctl` (Task 6) use. `digest_fields` lives in `uc_protocol::v2::schedule` and is shared by Tasks 4 and 6. `ScheduleRecord` is `pub` in `uc_node` for Task 6's `show`.
