# Time and timers, plan 1 (log time + the scheduler primitive) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every log frame carries a leader-stamped, monotone `time_ns`; a state machine reads it as `ctx.time_ns`, schedules timers with `ctx.schedule(id, at)`, and receives them through `on_timer` as deadline-stamped, in-order `TIMER` frames — exactly once per scheduled instance under failover, restart and snapshot install.

**Architecture:** The 32-byte frame header is relaid (the two `u64` ids carried 32 bits each; the freed 8 bytes become `time_ns`) so the wire cost is zero. `uc_log::Appender` owns the clamp `stamp = max(now, last_stamp)` for every frame type and a new `append_timer`; the archive agent carries the last recorded stamp into one cnc word the next leader seeds from. Every node keeps a per-row timer heap fed by a new service→node SPSC ring; only the leader fires, at the top of its pass, before that pass's client frames. The service-side `Timed<S>` wrapper turns the node's at-least-once firing into exactly-once delivery from log content alone. Consensus, elections and replication are untouched.

**Tech Stack:** Rust 1.96 workspace (MSRV 1.89); `uc_protocol` (core-only leaf), `uc_log`, `uc_service`, `uc_node`, `uc_ctl`, `uc_lincheck`, `uc_sim`; `fuzz/` (separate workspace, nightly); `packaging/prometheus/uc2-alerts.yml`.

**Spec:** `docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md` (binding). Plan 1 = spec §11 items 1–6: §3 (the timestamp), §4 (the primitive), §6–§9 for what plan 1 ships; §5 (the schedule table) is plan 2 and is **not** in this plan except for the fields that keep its snapshot/frame formats stable (`FLAG_TIMER_TABLE`, `TimerEvent.table`, `Timed`'s `table_last` map). Read the spec end-to-end before Task 0. Task 0 records the as-built deltas the tree recon (2026-09-03) found.

## Global Constraints

- **Whole workspace green after every task**: `cargo fmt --all` (enforced by CI: `cargo fmt --all -- --check`), `cargo clippy --workspace --all-targets -- -D warnings`, `cargo clippy -p uc_service --features apply-profile --all-targets -- -D warnings`, `cargo clippy -p uc_gateway --features test-util --all-targets -- -D warnings`, `cargo test --workspace --exclude uc_node`, `cargo test -p uc_node --lib --test smoke --test failover --test learner --test purge_safety --test query_barrier --test admin_auth --test daemon_refusals` (the CI `test` job, `.github/workflows/ci.yml:44-63`). `fuzz/` is outside the workspace: after Task 1, Task 4 and Task 13, `(cd fuzz && cargo +nightly check)` must pass.
- **Wire and cnc are the pending, unreleased `2.11.0` flag day** (spec §9): `uc_protocol::version::CURRENT` stays `0.7.0` and `CNC_V2_VERSION` stays `3.1` — both are unreleased, so their layouts change in place. No compatibility shim for `2.10.0`'s header; a `2.10.0` node in this cluster stalls (the standing flag-day rule).
- **Frozen after they ship**: the header layout (`OFF_CLIENT_ID=12, OFF_SEQ=16, OFF_RESERVED1=20, OFF_TIME_NS=24`), `FRAME_TYPE_TIMER=5`, the 24-byte timer body, `MSG_V2_SCHED=8` and its 17-byte record, `CNC_OFF_LOG_TIME_NS=4048`, `CNC_SVC_OFF_TIMERS_PENDING=488`. Each is pinned by a test whose comment says so.
- **The clamp is the appender's, not any caller's** (spec §3.2): every frame type — MESSAGE, NEW_TERM, CONFIG, PADDING, TIMER — is stamped inside `uc_log::Appender`; no caller passes a stamp. The clamp is non-strict (`max`, equal stamps allowed). Position is the order; time is never a tie-breaker.
- **Ordering rule (spec §4.3), the property Task 3's test pins**: within one leader pass, every due timer is appended before any client frame, stamped `max(deadline, last_stamp)`; if the per-pass timer bound is hit, no client frame is appended that pass.
- **Node layer at-least-once, service layer exactly-once** (spec §4.5–§4.6). The node never persists timer state and never reads a frame payload; the `Timed<S>` wrapper decides delivery from its own log-derived maps.
- **Every `apply` impl in the tree compiles unchanged**: `ApplyCtx` gains fields behind `#[non_exhaustive]`; `on_timer` is a provided method with a no-op default on both tiers.
- **`query` receives no time** (spec §3.3).
- **Fleet spend is user-gated** (Task 14's gate run). Local numbers are smoke, never a gate. Never write scratch to `/tmp`.
- Commit subjects: `type(scope): imperative summary`, as in `git log --oneline -30`.
- Every new or changed test is **watched red first** (the step says what to revert or stub); the task's commit message names the test.

---

## File structure

| file | responsibility | task |
|---|---|---|
| `docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md` | as-built errata (§3.3 request shape, §4.8 re-announce, §6 pending word, §8 sim scope, seed note) | 0 |
| `uc_protocol/src/v2/frame.rs` | header relayout (`client_id`/`seq`/`time_ns`), `FRAME_TYPE_TIMER`, `FLAG_TIMER_TABLE`, `TimerBody` codec, layout tests | 1 |
| `uc_log/src/buffer.rs`, `uc_net/src/sender.rs` (tests), `uc_node/tests/smoke.rs`, `uc_service/src/{apply,egress}.rs`, `fuzz/{fuzz_targets/uc_protocol_log_frame.rs,src/seeds.rs}` | every `FrameHeader` user follows the rename | 1 |
| `uc_protocol/src/v2/cnc.rs`, `uc_log/src/cnc.rs` | `CNC_OFF_LOG_TIME_NS` (page 1, 4048) and `CNC_SVC_OFF_TIMERS_PENDING` (slot line 7, +488); accessors; offset pins | 2 |
| `uc_log/src/buffer.rs`, `uc_log/src/archive.rs` | `Appender` stamps every frame (`set_now`, seed, clamp), `append(u32,u32,..)`, `append_timer`; archive carries `time_ns` → cnc; the pass-order property test | 3 |
| `uc_protocol/src/v2/ipc.rs`, `uc_node/src/ipc.rs`, `uc_node/src/node.rs` (`create_rings`), `uc_service/src/attach.rs` | `MSG_V2_SCHED` + `SchedRecord` codec; `svc_sched.<row>.ring` created by the node (consumer kept), opened by the service (producer) | 4 |
| `uc_service/src/traits.rs`, `apply.rs`, `replay.rs`, `session.rs`, `tagged.rs`, `lib.rs` | `ApplyCtx` fields/methods, `TimerEvent`, `on_timer` on both tiers + forwarding, build sites fill `time_ns`/`term`, TIMER delivery, sched records written, re-announce | 5 |
| `uc_service/src/timed.rs` (new), `lib.rs` | `Timed<S>`: pending map, exactly-once filter, `consumed`, snapshot, `TimerSource` | 6 |
| `uc_node/src/timers.rs` (new), `lib.rs` | `RowTimers`: pending/heap/in-flight, `schedule`/`cancel`/`consumed`/`pop_due`/`rearm`; unit tests | 7 |
| `uc_node/src/node.rs` | wall clock per pass, `set_now`, `drain_sched_rings`, `fire_due_timers` before client drains, hold-clients rule, re-arm on both leader-exit paths, seed at `on_collapsed`, `timers_pending` publish, `TimerStats`, obs events | 8 |
| `uc_node/src/obs/{metrics,mod}.rs`, `uc_ctl/src/main.rs`, `packaging/prometheus/uc2-alerts.yml` | `uc2_timers_*`, `uc2_log_time_ns`, `uc2_log_time_lag_seconds`, `CONTRACT_SERIES`, alert rule, `uc2ctl status` | 9 |
| `uc_node/tests/timers.rs` (new) | end-to-end: schedule → fire → `on_timer` on one node; late + exactly-once across a leader change | 10 |
| `uc_lincheck/src/timer.rs` (new), `uc_node/tests/lin_v2.rs`, `uc_node/tests/lincheck_v2/mod.rs`, `examples/uc_crashtest/src/bin/uc_crashtest-service.rs`, `examples/uc_crashtest/tests/hard_crash.rs` | `TimerSm`; the two-FSM capstone with timer churn under failover; the hard-crash scenario | 11 |
| `uc_sim/src/timers.rs` (new), `uc_sim/src/lib.rs`, `uc_sim/tests/scenarios.rs` | the leader-pass model + the ordering invariant, seeded | 12 |
| `fuzz/fuzz_targets/uc_protocol_timer_frame.rs`, `fuzz/fuzz_targets/uc_protocol_sched_record.rs` (new), `fuzz/Cargo.toml`, `fuzz/src/bin/seed_corpus.rs`, `fuzz/src/seeds.rs`, `fuzz/README.md`, `.github/workflows/nightly.yml`, `docs/VERIFICATION.md` | two fuzz targets, matrix group, verification record | 13 |
| `RELEASES.md`, `docs/releases.md`, `docs/reference/{wire-protocol,cnc-page,limits,semver-policy}.md`, `docs/ops/uc2-runbook.md`, `docs/how-to/upgrade-a-cluster.md`, `docs/security/attack-surface.md`, `docs/notes/uc2-log-time-and-timers-explained.md` (new), `docs/benchmarks/uc2-time-and-timers-gate-<date>.md` (new), the M14 and identity specs (errata), `docs/BACKLOG.md` | docs sweep, explainer, release writeup, gate doc | 14 |

---

### Task 0: Spec as-built errata

**Files:**
- Modify: `docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md` (§3.2, §3.3, §4.4, §4.5, §4.8, §6, §8, §11)

Recon of the tree (2026-09-03) found six things the spec's text does not say. Record them so the spec stays the binding record; each is a short amendment in place, not a rewrite.

- [ ] **Step 1: §3.3, the request shape.** Replace the `TimerReq` sentence after the code block with:

```markdown
`TimerReq` is `Schedule { id, at_ns } | Cancel { id }` — the two things a state
machine may ask. `consumed` is not a variant: `Timed` reports it through a
`pub(crate)` method that pushes into a private list, and the apply loop takes
both lists at once as wire records (`ApplyCtx::take_sched_records() ->
Vec<SchedRecord>`, `uc_protocol::v2::ipc::SchedRecord`). Nothing outside the
crate can forge a `consumed`.
```

- [ ] **Step 2: §3.2, the seed after a collapse.** Append to the "Seed at leader open" bullet:

```markdown
  After the leader-open collapse the archive has cut its journal to `base`, but
  `log_time_ns` may still hold the stamp of a frame above `base` that the cut
  discarded. Seeding from it is **monotone-safe**: the new leader's first stamps
  are at or above what any replica could have seen, never below. A stamp that is
  slightly ahead of wall time for one pass is the same "late" case §4.3 already
  accepts; a stamp that goes backwards would not be. The archive never lowers the
  word.
```

- [ ] **Step 3: §4.4, the node side is new code.** Append to the ring paragraph:

```markdown
`svc_sched` is the first per-row ring the node **consumes** — `svc_query`'s
consumer half is dropped at creation (`node.rs` `create_rings`) — so the
consensus agent's drain of it is new code beside `drain_query_ring`, not a
refactor of it. The node keeps `Vec<Option<SpscConsumer>>` by row, the shape
`svc_query` uses for its producers.
```

- [ ] **Step 4: §4.8, re-announce is one flag, three arming points.** Replace the "Re-announce" bullet's first sentence with:

```markdown
- **Re-announce.** The apply loop carries `announce_pending: bool`, set at
  attach, and set again whenever `replay_into` returns (which is also the only
  path a snapshot install takes, so install is covered without a second hook).
  When the flag is set, the top of the next `apply_cycle` asks the wrapper for
  its pending set — `trait TimerSource { fn pending_timers(&self) -> Vec<(u64,
  u64)> }`, implemented by `Timed`, a no-op default for everything else — and
  writes one `Schedule` record per entry before delivering any frame.
```

- [ ] **Step 5: §6, the pending-count word and the ring-full counter.** Replace the last paragraph ("**cnc:** …") with:

```markdown
**cnc:** two words, both inside the unreleased cnc `3.1`. `log_time_ns` is page
1 offset `4048` (the third word of the boot-once `4032` line; `4032`/`4040` are
written once before publish and never again, so the archive agent is the line's
only live writer). `timers_pending` is slot line 7 offset `+488` (the word after
`identity_hash`; line 7's writer is the node, and the consensus agent is the
node). Offsets are pinned in both `uc_protocol` and `uc_log`. `uc2_timers_pending`
and `uc2ctl status` read the slot word; the fired/late/re-armed counters are
process-local atomics in `ObsSources`. `uc2_sched_ring_full_total` is **not**
exported in plan 1: it would need a service-written word and the reserved slot
bytes are not spent here; the service counts it in a log record instead.
```

- [ ] **Step 6: §8, the sim tier's real shape.** Replace the "Sim tier" paragraph with:

```markdown
**Sim tier.** `uc_sim`'s world has no frames — a command is a 96-byte append
counter and the only per-position fact is the term map — so the §4.3 invariant
cannot be a world-level check without inventing a frame model the world does not
need. It is instead a **pure model of the leader pass** in `uc_sim::timers`:
a virtual clock, random client appends and timer deadlines, leader changes with
lagging and leading clocks, driven by the same seeded RNG, asserting the §4.3
property and the clamp on every step. Stamping and firing touch neither
`CommitTracker` nor `ElectionSm`, so the Lean model, conformance vectors and
loom models are re-run as regression only. The "leaders die mid-fire" scenario
runs against real code in the `lin_v2` capstone (§8, Capstones), where it
belongs.
```

- [ ] **Step 7: §11 plan 1 item 4.** Append: "Re-arm runs on both leader-exit paths: `Action::BecomeFollower` (which already drops the appender) and `halt()` (removed from the cluster). The pending ingress payloads carry across a role flip by design and are not touched."

- [ ] **Step 8: Commit**

```bash
git add docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md
git commit -m "docs(spec): time and timers — as-built errata from the tree recon"
```

---

### Task 1: Frame header relayout, `FRAME_TYPE_TIMER`, the timer body codec

**Files:**
- Modify: `uc_protocol/src/v2/frame.rs` (whole file: constants 15-22, struct 43-50, codec 60-99, tests 101-159)
- Modify (mechanical rename, listed so nothing is missed): `uc_log/src/buffer.rs:576-577,620-626,674-680,743-749,768-774` and its tests (`832-833, 982, 1091, 1172-1181, 1221`), `uc_log/src/archive.rs` tests (`1506-1512, 1526-1532, 1656-1689`), `uc_log/src/reader.rs:192,205,262`, `uc_log/src/writer.rs:119`, `uc_log/tests/buffer_file.rs:47`, `uc_net/src/sender.rs:1784,2062-2068,2301`, `uc_service/src/egress.rs:47-62`, `uc_service/src/apply.rs:396-397`, `uc_node/src/node.rs:3517,3745` (widening casts go away), `uc_node/tests/smoke.rs:241-271`, `fuzz/fuzz_targets/uc_protocol_log_frame.rs:18`, `fuzz/src/seeds.rs:764-773`
- Test: `uc_protocol/src/v2/frame.rs` `mod tests`

**Interfaces:**
- Produces:
  - `pub const OFF_CLIENT_ID: usize = 12; pub const OFF_SEQ: usize = 16; pub const OFF_RESERVED1: usize = 20; pub const OFF_TIME_NS: usize = 24;` (`OFF_SESSION_ID`/`OFF_CORRELATION_ID` are deleted)
  - `pub const FRAME_TYPE_TIMER: u8 = 5; pub const FLAG_TIMER_TABLE: u8 = 0x01;`
  - `pub struct FrameHeader { pub length: u32, pub frame_type: u8, pub flags: u8, pub leadership_term_id: u32, pub client_id: u32, pub seq: u32, pub time_ns: u64 }`
  - `pub const TIMER_BODY_LEN: usize = 24; #[derive(Debug, Clone, Copy, PartialEq, Eq)] pub struct TimerBody { pub identity_hash: u64, pub timer_id: u64, pub deadline_ns: u64 }`
  - `pub fn write_timer_body(buf: &mut [u8], b: &TimerBody)` (panics if `buf.len() < 24`, like `write_header_except_length`), `pub fn read_timer_body(buf: &[u8]) -> Option<TimerBody>` (`None` if shorter than 24 — total on any input)
- Consumed by: Tasks 3, 5, 8, 13.

- [ ] **Step 1: Replace the layout and codec tests** in `uc_protocol/src/v2/frame.rs` `mod tests` (delete `header_roundtrip_except_length` and `field_offsets_do_not_overlap`; keep `alignment_math`; extend `frame_type_codes_are_stable`):

```rust
    /// FROZEN layout (spec §3.1). Never change these.
    #[test]
    fn field_offsets_are_the_relaid_layout() {
        // length(4) type(1) flags(1) rsvd(2) term(4) client_id(4) seq(4) rsvd(4) time_ns(8) = 32
        assert_eq!(OFF_LENGTH, 0);
        assert_eq!(OFF_TYPE, 4);
        assert_eq!(OFF_FLAGS, 5);
        assert_eq!(OFF_RESERVED0, 6);
        assert_eq!(OFF_TERM_ID, 8);
        assert_eq!(OFF_CLIENT_ID, 12);
        assert_eq!(OFF_SEQ, 16);
        assert_eq!(OFF_RESERVED1, 20);
        assert_eq!(OFF_TIME_NS, 24);
        assert_eq!(HEADER_LEN, 32);
        assert_eq!(FRAME_ALIGNMENT, 32);
    }

    #[test]
    fn header_roundtrip_except_length_pins_the_bytes() {
        let mut buf = [0xAAu8; HEADER_LEN];
        let h = FrameHeader {
            length: 0,
            frame_type: FRAME_TYPE_MESSAGE,
            flags: 0x5a,
            leadership_term_id: 7,
            client_id: 0x0102_0304,
            seq: 0x0506_0708,
            time_ns: 0x1122_3344_5566_7788,
        };
        write_header_except_length(&mut buf, &h);
        assert_eq!(&buf[0..4], &[0xAA; 4], "length is the commit word: untouched");
        assert_eq!(&buf[6..8], &[0, 0], "reserved0 written as zero");
        assert_eq!(&buf[12..16], &[0x04, 0x03, 0x02, 0x01], "client_id LE at 12");
        assert_eq!(&buf[16..20], &[0x08, 0x07, 0x06, 0x05], "seq LE at 16");
        assert_eq!(&buf[20..24], &[0, 0, 0, 0], "reserved1 written as zero");
        assert_eq!(&buf[24..32], &0x1122_3344_5566_7788u64.to_le_bytes(), "time_ns LE at 24");
        let out = read_header(&buf);
        assert_eq!(out.frame_type, FRAME_TYPE_MESSAGE);
        assert_eq!(out.flags, 0x5a);
        assert_eq!(out.leadership_term_id, 7);
        assert_eq!(out.client_id, 0x0102_0304);
        assert_eq!(out.seq, 0x0506_0708);
        assert_eq!(out.time_ns, 0x1122_3344_5566_7788);
    }

    #[test]
    fn frame_type_codes_are_stable() {
        assert_eq!(FRAME_TYPE_MESSAGE, 1);
        assert_eq!(FRAME_TYPE_PADDING, 2);
        assert_eq!(FRAME_TYPE_NEW_TERM, 3);
        assert_eq!(FRAME_TYPE_CONFIG, 4);
        assert_eq!(FRAME_TYPE_TIMER, 5);
        assert_eq!(FLAG_TIMER_TABLE, 0x01);
    }

    /// FROZEN: the 24-byte TIMER body (spec §4.2).
    #[test]
    fn timer_body_roundtrip_and_short_input_is_none() {
        let b = TimerBody { identity_hash: 0xdead_beef_cafe_f00d, timer_id: 42, deadline_ns: 1_700_000_000_000_000_000 };
        let mut buf = [0u8; TIMER_BODY_LEN];
        write_timer_body(&mut buf, &b);
        assert_eq!(&buf[0..8], &b.identity_hash.to_le_bytes());
        assert_eq!(&buf[8..16], &42u64.to_le_bytes());
        assert_eq!(&buf[16..24], &b.deadline_ns.to_le_bytes());
        assert_eq!(read_timer_body(&buf), Some(b));
        assert_eq!(read_timer_body(&buf[..23]), None);
        assert_eq!(read_timer_body(&[]), None);
        let mut longer = [7u8; 40];
        longer[..24].copy_from_slice(&buf);
        assert_eq!(read_timer_body(&longer), Some(b), "trailing bytes are ignored");
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_protocol frame`
Expected: compile errors (`client_id`, `seq`, `time_ns`, `FRAME_TYPE_TIMER`, `TimerBody` unknown).

- [ ] **Step 3: Implement** in `uc_protocol/src/v2/frame.rs` — replace lines 15-22 and 43-50, extend the codec, add the body codec:

```rust
pub const OFF_LENGTH: usize = 0; // u32 LE — TOTAL frame length (header + payload); 0 = uncommitted
pub const OFF_TYPE: usize = 4; // u8
pub const OFF_FLAGS: usize = 5; // u8
pub const OFF_RESERVED0: usize = 6; // u16 — reserved, written as zero
pub const OFF_TERM_ID: usize = 8; // u32 LE — leadership_term_id
pub const OFF_CLIENT_ID: usize = 12; // u32 LE — the submitting client (0 for node-originated frames)
pub const OFF_SEQ: usize = 16; // u32 LE — the client's local sequence (0 for node-originated frames)
pub const OFF_RESERVED1: usize = 20; // u32 — reserved, written as zero
pub const OFF_TIME_NS: usize = 24; // u64 LE — leader-stamped ns since the Unix epoch; non-decreasing along the log

/// Scheduled timer fired by the leader (time-and-timers spec §4.2): a 24-byte
/// body ([`TimerBody`]); `client_id`/`seq` are 0; `time_ns` is the deadline
/// unless the frame is late (`time_ns > deadline_ns`). Delivered to exactly the
/// FSM whose identity hash it names; every other apply loop skips it.
pub const FRAME_TYPE_TIMER: u8 = 5;
/// `flags` bit 0 on a TIMER frame: fired from the replicated schedule table
/// (plan 2), not from a state machine's `schedule` call.
pub const FLAG_TIMER_TABLE: u8 = 0x01;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub length: u32,
    pub frame_type: u8,
    pub flags: u8,
    pub leadership_term_id: u32,
    pub client_id: u32,
    pub seq: u32,
    pub time_ns: u64,
}

pub fn write_header_except_length(buf: &mut [u8], h: &FrameHeader) {
    buf[OFF_TYPE] = h.frame_type;
    buf[OFF_FLAGS] = h.flags;
    buf[OFF_RESERVED0..OFF_RESERVED0 + 2].copy_from_slice(&[0, 0]);
    buf[OFF_TERM_ID..OFF_TERM_ID + 4].copy_from_slice(&h.leadership_term_id.to_le_bytes());
    buf[OFF_CLIENT_ID..OFF_CLIENT_ID + 4].copy_from_slice(&h.client_id.to_le_bytes());
    buf[OFF_SEQ..OFF_SEQ + 4].copy_from_slice(&h.seq.to_le_bytes());
    buf[OFF_RESERVED1..OFF_RESERVED1 + 4].copy_from_slice(&[0, 0, 0, 0]);
    buf[OFF_TIME_NS..OFF_TIME_NS + 8].copy_from_slice(&h.time_ns.to_le_bytes());
}

pub fn read_header(buf: &[u8]) -> FrameHeader {
    FrameHeader {
        length: u32::from_le_bytes(buf[OFF_LENGTH..OFF_LENGTH + 4].try_into().unwrap()),
        frame_type: buf[OFF_TYPE],
        flags: buf[OFF_FLAGS],
        leadership_term_id: u32::from_le_bytes(buf[OFF_TERM_ID..OFF_TERM_ID + 4].try_into().unwrap()),
        client_id: u32::from_le_bytes(buf[OFF_CLIENT_ID..OFF_CLIENT_ID + 4].try_into().unwrap()),
        seq: u32::from_le_bytes(buf[OFF_SEQ..OFF_SEQ + 4].try_into().unwrap()),
        time_ns: u64::from_le_bytes(buf[OFF_TIME_NS..OFF_TIME_NS + 8].try_into().unwrap()),
    }
}

/// The TIMER frame body: fixed, 24 bytes, three LE `u64`s.
pub const TIMER_BODY_LEN: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimerBody {
    /// `FsmIdentity::hash()` of the FSM this timer belongs to.
    pub identity_hash: u64,
    /// The FSM's own id for the timer.
    pub timer_id: u64,
    /// What was asked for; compare with the header's `time_ns` for lateness.
    pub deadline_ns: u64,
}

pub fn write_timer_body(buf: &mut [u8], b: &TimerBody) {
    buf[0..8].copy_from_slice(&b.identity_hash.to_le_bytes());
    buf[8..16].copy_from_slice(&b.timer_id.to_le_bytes());
    buf[16..24].copy_from_slice(&b.deadline_ns.to_le_bytes());
}

/// Total on any input: `None` when shorter than [`TIMER_BODY_LEN`]; longer
/// input is accepted and the tail ignored (a committed frame is trusted; the
/// length check is what keeps this decoder safe on a fuzzed slice).
pub fn read_timer_body(buf: &[u8]) -> Option<TimerBody> {
    if buf.len() < TIMER_BODY_LEN {
        return None;
    }
    let u = |o: usize| u64::from_le_bytes(buf[o..o + 8].try_into().unwrap());
    Some(TimerBody { identity_hash: u(0), timer_id: u(8), deadline_ns: u(16) })
}
```

Keep `read_header`'s existing caller-guard doc comment (the fuzz target's `len >= HEADER_LEN` contract). Update the file's layout comment block if there is one above the constants.

- [ ] **Step 4: Follow the rename through the tree.** Every site in the **Files** list: `session_id` → `client_id` (`u32`), `correlation_id` → `seq` (`u32`), add `time_ns: 0` to every `FrameHeader { .. }` literal that is not inside `uc_log::Appender` (the appender's stamps land in Task 3; until then it writes `time_ns: 0`). Specifically:
  - `uc_log/src/buffer.rs:574-577`: `pub fn append(&mut self, client_id: u32, seq: u32, payload: &[u8])`; the four `FrameHeader` literals gain `time_ns: 0` for now.
  - `uc_service/src/egress.rs:47-53`: `pub(crate) fn publish(&mut self, client_id: u32, seq: u32, position: u64, resp: &[u8])`; line 57 becomes `let extra = extra_client(client_id, seq);` (the `as u32` casts go). Update the doc comment at line 9.
  - `uc_service/src/apply.rs:396-397`: `st.egress.publish(hdr.client_id, hdr.seq, pos, &st.resp_buf);`.
  - `uc_node/src/node.rs:3745`: `app.append(client_id, local_seq, payload)`. Line 3517: `app.append(0, self.next_corr, payload)` with `next_corr` retyped `u32` at its declaration (`grep -n "next_corr" uc_node/src/node.rs`, the field and its `0` init) and `self.next_corr = self.next_corr.wrapping_add(1)` at 3518/3520 — correlation is by value within a bounded window (the client's own sequence wraps the same way, `uc_client/src/slots.rs` invariant 4).
  - `uc_node/tests/smoke.rs:270-271`: `assert_eq!(hdr.client_id, 7); assert_eq!(hdr.seq, 1);`.
  - `fuzz/fuzz_targets/uc_protocol_log_frame.rs:18`: `let _ = (h.length, h.frame_type, h.flags, h.leadership_term_id, h.client_id, h.seq, h.time_ns);`. `fuzz/src/seeds.rs:764-773`: `client_id: 7, seq: 11, time_ns: 0`.
  - Test literals in `uc_log` and `uc_net`: rename fields, add `time_ns: 0`; assertions on `session_id`/`correlation_id` values become assertions on `client_id`/`seq` with the same numbers.

- [ ] **Step 5: Run the workspace and the fuzz check**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p uc_protocol -p uc_log -p uc_net -p uc_service && cargo test -p uc_node --test smoke && (cd fuzz && cargo +nightly check)`
Expected: all PASS; `frame` tests green including `header_roundtrip_except_length_pins_the_bytes` and `timer_body_roundtrip_and_short_input_is_none`.

- [ ] **Step 6: Commit**

```bash
git add uc_protocol uc_log uc_net uc_service uc_node fuzz
git commit -m "feat(uc_protocol): relay the frame header — client_id/seq u32, time_ns u64; FRAME_TYPE_TIMER + TimerBody codec (tests: field_offsets_are_the_relaid_layout, header_roundtrip_except_length_pins_the_bytes, timer_body_roundtrip_and_short_input_is_none)"
```

---

### Task 2: cnc words — `log_time_ns` (page 1, 4048) and `timers_pending` (slot line 7, +488)

**Files:**
- Modify: `uc_protocol/src/v2/cnc.rs` (after `CNC_OFF_FSM_LAG_BYTES`, line 250; after `CNC_SVC_OFF_IDENTITY_HASH`, line 297; the offset test block starting ~587)
- Modify: `uc_log/src/cnc.rs` (`ServiceIdentityLine`, lines 205-217; the `fsm_lag_bytes` accessor pair, 637-649; tests `cnc_offsets_match_protocol_constants` 1061 and `services_declared_and_fsm_lag_roundtrip_and_offset_pin` 1596)
- Test: both files' `mod tests`

**Interfaces:**
- Produces:
  - `uc_protocol::v2::cnc::CNC_OFF_LOG_TIME_NS: usize = 4048` (+ `const _` asserts it is `CNC_OFF_FSM_LAG_BYTES + 8` and `< 4096`)
  - `uc_protocol::v2::cnc::CNC_SVC_OFF_TIMERS_PENDING: usize = 488` (+ assert `== CNC_SVC_OFF_IDENTITY_HASH + 8`)
  - `uc_log::cnc::CncPage::log_time_ns(&self) -> u64`, `store_log_time_ns(&self, v: u64)` (Acquire/Release, bare `AtomicU64` on the shared line, exactly `fsm_lag_bytes`'s pattern)
  - `uc_log::cnc::ServiceIdentityLine::timers_pending(&self) -> u64`, `store_timers_pending(&self, v: u64)`
- Consumed by: Task 3 (archive writes `log_time_ns`), Task 8 (node reads the seed, writes `timers_pending`), Task 9 (metrics, `uc2ctl`).

- [ ] **Step 1: Write the failing tests.** In `uc_protocol/src/v2/cnc.rs`'s offset test block add:

```rust
        // time-and-timers spec §6 (FROZEN): the archive's last recorded stamp,
        // third word of the boot-once 4032 line.
        assert_eq!(CNC_OFF_LOG_TIME_NS, 4048);
        assert_eq!(CNC_OFF_LOG_TIME_NS, CNC_OFF_FSM_LAG_BYTES + 8);
        // per-row pending-timer count, the word after identity_hash on line 7.
        assert_eq!(CNC_SVC_OFF_TIMERS_PENDING, 488);
        assert_eq!(CNC_SVC_OFF_TIMERS_PENDING, CNC_SVC_OFF_IDENTITY_HASH + 8);
```

In `uc_log/src/cnc.rs` add to `cnc_offsets_match_protocol_constants` (after the `CNC_OFF_FSM_LAG_BYTES` line):

```rust
        assert_eq!(cnc::CNC_OFF_LOG_TIME_NS, 4048);
        assert_eq!(
            std::mem::offset_of!(ServiceIdentityLine, timers_pending),
            cnc::CNC_SVC_OFF_TIMERS_PENDING - cnc::CNC_SVC_OFF_NAME
        );
```

and a new test beside `services_declared_and_fsm_lag_roundtrip_and_offset_pin`:

```rust
    #[test]
    fn log_time_and_timers_pending_roundtrip_and_offset_pin() {
        let page = CncPage::heap(&test_meta());
        assert_eq!(page.log_time_ns(), 0, "fresh page: no stamp yet");
        page.store_log_time_ns(1_700_000_000_000_000_123);
        assert_eq!(page.log_time_ns(), 1_700_000_000_000_000_123);
        let raw = page.page();
        assert_eq!(
            u64::from_le_bytes(raw[4048..4056].try_into().unwrap()),
            1_700_000_000_000_000_123,
            "offset pin: log_time_ns lives at 4048"
        );
        assert_eq!(page.fsm_lag_bytes(), 0, "the neighbouring boot-once word is untouched");
        let slot = page.service_slot(2);
        assert_eq!(slot.identity.timers_pending(), 0);
        slot.identity.store_timers_pending(17);
        assert_eq!(slot.identity.timers_pending(), 17);
        let raw = page.page();
        let base = cnc::CNC_OFF_SERVICE_SLOTS + 2 * cnc::CNC_SERVICE_SLOT_STRIDE + cnc::CNC_SVC_OFF_TIMERS_PENDING;
        assert_eq!(
            u64::from_le_bytes(raw[base..base + 8].try_into().unwrap()),
            17,
            "offset pin: timers_pending lives at slot +488"
        );
    }
```

(`test_meta()` is the module's existing helper; `CNC_OFF_SERVICE_SLOTS` and `CNC_SERVICE_SLOT_STRIDE` are the existing page-2 constants — check their exact names with `grep -n "CNC_OFF_SERVICE_SLOTS\|CNC_SERVICE_SLOT_STRIDE" uc_protocol/src/v2/cnc.rs` and use those.)

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc_protocol cnc && cargo test -p uc_log cnc`
Expected: compile errors (`CNC_OFF_LOG_TIME_NS`, `timers_pending` unknown).

- [ ] **Step 3: Implement.** `uc_protocol/src/v2/cnc.rs`, after line 250:

```rust
/// Time-and-timers spec §3.2/§6: the highest `time_ns` the archive agent has
/// recorded — the seed a new leader clamps its first stamps against, and the
/// `uc2_log_time_ns` gauge. Third word of the 4032 line; `4032`/`4040` are
/// written once before the page is published, so the archive agent is this
/// line's only live writer. Writer: archive agent; init = 0.
pub const CNC_OFF_LOG_TIME_NS: usize = 4048;
const _: () = assert!(CNC_OFF_LOG_TIME_NS == CNC_OFF_FSM_LAG_BYTES + 8);
const _: () = assert!(CNC_OFF_LOG_TIME_NS + 8 <= 4096);
```

after line 297:

```rust
/// Time-and-timers spec §6: this row's pending-timer count, the word after
/// `identity_hash` on line 7 (node-written, like the rest of the line; the
/// consensus agent refreshes it once per pass). Reader: `/metrics`, `uc2ctl`.
pub const CNC_SVC_OFF_TIMERS_PENDING: usize = 488;
const _: () = assert!(CNC_SVC_OFF_TIMERS_PENDING == CNC_SVC_OFF_IDENTITY_HASH + 8);
```

`uc_log/src/cnc.rs`: reshape the line (the pad shrinks by one word) and add accessors:

```rust
#[repr(C)]
pub struct ServiceIdentityLine {
    name: [u8; cnc::CNC_SVC_NAME_LEN],
    hash: AtomicU64,
    timers_pending: AtomicU64,
    _pad: [u64; 2],
}
impl ServiceIdentityLine {
    pub fn name(&self) -> Option<FsmName> {
        FsmName::from_padded(&self.name)
    }
    pub fn hash(&self) -> u64 {
        self.hash.load(Ordering::Acquire)
    }
    /// Pending timers for this row (time-and-timers spec §6); node-written.
    pub fn timers_pending(&self) -> u64 {
        self.timers_pending.load(Ordering::Acquire)
    }
    pub fn store_timers_pending(&self, v: u64) {
        self.timers_pending.store(v, Ordering::Release)
    }
}
const _: () = assert!(std::mem::size_of::<ServiceIdentityLine>() == 64);
const _: () = assert!(
    std::mem::offset_of!(ServiceIdentityLine, timers_pending)
        == cnc::CNC_SVC_OFF_TIMERS_PENDING - cnc::CNC_SVC_OFF_NAME
);
```

and beside `fsm_lag_bytes` (line 649):

```rust
    /// The archive agent's last recorded frame stamp (time-and-timers §3.2).
    pub fn log_time_ns(&self) -> u64 {
        // SAFETY: 4048 is 8-aligned and 4048 + 8 <= CNC_PAGE_LEN.
        unsafe {
            (*(self.region.ptr_at(CNC_OFF_LOG_TIME_NS) as *const AtomicU64)).load(Ordering::Acquire)
        }
    }
    pub fn store_log_time_ns(&self, v: u64) {
        // SAFETY: as `log_time_ns`.
        unsafe {
            (*(self.region.ptr_at(CNC_OFF_LOG_TIME_NS) as *const AtomicU64)).store(v, Ordering::Release)
        }
    }
```

Add `CNC_OFF_LOG_TIME_NS` to the `use uc_protocol::v2::cnc::{..}` list at the top of `uc_log/src/cnc.rs`, and the two words to the layout comment at the top of that file (lines 1-30) and to the line-7 comment above `ServiceIdentityLine`.

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p uc_protocol cnc && cargo test -p uc_log cnc`
Expected: PASS, including `log_time_and_timers_pending_roundtrip_and_offset_pin`.

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/v2/cnc.rs uc_log/src/cnc.rs
git commit -m "feat(cnc): log_time_ns at 4048 and per-row timers_pending at slot +488, pinned in both crates (test: log_time_and_timers_pending_roundtrip_and_offset_pin)"
```

---

### Task 3: The appender stamps every frame; `append_timer`; the archive carries the stamp; the pass-order property

**Files:**
- Modify: `uc_log/src/buffer.rs` (`Appender` 548-565, `append` 574-628, `append_new_term` 642-680, `append_config` 700-745, `write_padding` 763-777; tests from 795)
- Modify: `uc_log/src/archive.rs` (`Archive` fields ~125-150, `do_work` 302-341, `observe_terms` 341-378)
- Modify: `uc_node/src/node.rs:5438` (the one production `Appender::new`), test helpers `node.rs:8296,8320`, `uc_service/src/apply.rs:876` (test)
- Test: `uc_log/src/buffer.rs` `mod tests`, `uc_log/src/archive.rs` `mod tests`

**Interfaces:**
- Produces:
  - `Appender::new(buffer: Arc<LogBuffer>, leadership_term_id: u32, seed_stamp: u64) -> Self` — `now_ns` and `last_stamp` both start at `seed_stamp`
  - `Appender::set_now(&mut self, now_ns: u64)` — the once-per-pass clock reading; `Appender::last_stamp(&self) -> u64`
  - `Appender::append(&mut self, client_id: u32, seq: u32, payload: &[u8]) -> Result<u64, AppendError>` (stamps `max(now, last)`)
  - `Appender::append_timer(&mut self, body: &TimerBody, flags: u8) -> Result<(u64, u64), AppendError>` — `(frame_start, stamp)`; stamps `max(body.deadline_ns, last)`
  - `append_new_term`, `append_config`, padding: stamped `max(now, last)`; signatures unchanged
  - `Archive` tracks `last_time_ns` and stores it to `cnc.store_log_time_ns` after every recorded block; never lowers it
- Consumed by: Task 8.

- [ ] **Step 1: Write the failing tests** in `uc_log/src/buffer.rs` `mod tests` (the module's `buf()` helper builds the buffer; `read_header` reads a frame back from `b.recordable_slice(pos, len)`):

```rust
    fn headers(b: &LogBuffer, end: u64) -> Vec<FrameHeader> {
        let s = b.recordable_slice(0, end as usize).unwrap();
        let mut out = Vec::new();
        let mut off = 0usize;
        while off + HEADER_LEN <= s.len() {
            let h = read_header(&s[off..]);
            if h.length == 0 { break; }
            out.push(h);
            off += align_frame_len(h.length as usize);
        }
        out
    }

    #[test]
    fn every_frame_type_is_stamped_and_the_clamp_never_goes_backwards() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 3, 1_000);
        a.append_new_term().unwrap(); // stamped with the seed
        a.set_now(5_000);
        a.append(1, 1, b"x").unwrap();
        a.set_now(4_000); // the clock stepped back: the stamp must hold at 5_000
        a.append(1, 2, b"y").unwrap();
        a.append_config(3, b"cfg").unwrap();
        a.set_now(6_000);
        a.append(1, 3, b"z").unwrap();
        let hs = headers(&b, a.position());
        let stamps: Vec<u64> = hs.iter().map(|h| h.time_ns).collect();
        assert_eq!(stamps, vec![1_000, 5_000, 5_000, 5_000, 6_000]);
        assert_eq!(a.last_stamp(), 6_000);
        assert!(hs.iter().all(|h| h.frame_type != FRAME_TYPE_PADDING || h.time_ns > 0));
    }

    #[test]
    fn append_timer_stamps_the_deadline_and_marks_late_by_clamp() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 3, 0);
        a.set_now(100);
        a.append(1, 1, b"a").unwrap();
        let body = TimerBody { identity_hash: 9, timer_id: 42, deadline_ns: 150 };
        let (pos, stamp) = a.append_timer(&body, 0).unwrap();
        assert_eq!(stamp, 150, "on time: stamped with the deadline");
        let s = b.recordable_slice(pos, 64).unwrap();
        let h = read_header(s);
        assert_eq!(h.frame_type, FRAME_TYPE_TIMER);
        assert_eq!((h.client_id, h.seq, h.flags), (0, 0, 0));
        assert_eq!(h.length as usize, HEADER_LEN + TIMER_BODY_LEN);
        assert_eq!(read_timer_body(&s[HEADER_LEN..]), Some(body));
        // late: the log is already past the deadline
        a.set_now(1_000);
        a.append(1, 2, b"b").unwrap();
        let late = TimerBody { identity_hash: 9, timer_id: 43, deadline_ns: 500 };
        let (_, stamp) = a.append_timer(&late, FLAG_TIMER_TABLE).unwrap();
        assert_eq!(stamp, 1_000, "late: clamped to last_stamp, deadline kept in the body");
        let hs = headers(&b, a.position());
        assert_eq!(hs.last().unwrap().flags, FLAG_TIMER_TABLE);
        let stamps: Vec<u64> = hs.iter().map(|h| h.time_ns).collect();
        assert!(stamps.windows(2).all(|w| w[0] <= w[1]), "{stamps:?}");
    }

    /// Spec §4.3, pinned at the appender: drive random passes of
    /// (set_now, due timers first, then client frames) and assert no frame
    /// before a TIMER carries a stamp above its deadline unless the TIMER is
    /// late.
    #[test]
    fn pass_order_property_no_earlier_frame_exceeds_an_on_time_deadline() {
        let (b, _c) = buf();
        let mut a = Appender::new(Arc::clone(&b), 1, 0);
        let mut rng = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || { rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; rng };
        let mut now = 1_000u64;
        let mut pending: Vec<(u64, u64)> = Vec::new(); // (id, deadline)
        let mut id = 0u64;
        'passes: for _ in 0..40 {
            now += next() % 300;
            a.set_now(now);
            // schedule a few timers for the near future or the past
            for _ in 0..(next() % 3) {
                id += 1;
                pending.push((id, now.saturating_sub(200) + next() % 600));
            }
            pending.sort_by_key(|p| p.1);
            while let Some(&(tid, dl)) = pending.first() {
                if dl > now { break; }
                pending.remove(0);
                if a.append_timer(&TimerBody { identity_hash: 1, timer_id: tid, deadline_ns: dl }, 0).is_err() { break 'passes; }
            }
            for _ in 0..(next() % 4) {
                if a.append(1, 1, b"c").is_err() { break 'passes; }
            }
        }
        let hs = headers(&b, a.position());
        assert!(hs.len() > 20, "the buffer filled too early to mean anything: {}", hs.len());
        for (i, h) in hs.iter().enumerate() {
            if h.frame_type != FRAME_TYPE_TIMER { continue; }
            let body = read_timer_body(&b.recordable_slice(hs[..i].iter().map(|x| align_frame_len(x.length as usize) as u64).sum(), 64).unwrap()[HEADER_LEN..]).unwrap();
            let late = h.time_ns > body.deadline_ns;
            if !late {
                assert!(hs[..i].iter().all(|e| e.time_ns <= body.deadline_ns), "frame before an on-time timer stamped past its deadline: {:?} vs {body:?}", &hs[..i]);
            }
            assert!(hs[i..].iter().all(|e| e.time_ns >= h.time_ns), "a later frame stamped below the timer");
        }
    }
```

The buffer's overrun gate stops the run once `CAP` (4096) bytes are written — durable never advances in this test — so the loop breaks out and the assertions run over whatever was written; `hs.len() > 20` guards against a degenerate run. Raise `CAP` locally if the bound is hit before 20 frames.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc_log buffer`
Expected: compile errors (`Appender::new` arity, `set_now`, `append_timer` unknown).

- [ ] **Step 3: Implement the appender.** In `uc_log/src/buffer.rs`:

```rust
pub struct Appender {
    buffer: Arc<LogBuffer>,
    pos: u64,
    cached_durable: u64,
    leadership_term_id: u32,
    /// The pass's clock reading (`set_now`); a stamp is `max(now_ns, last_stamp)`.
    now_ns: u64,
    /// The last stamp written — the log's clock never goes backwards.
    last_stamp: u64,
}

impl Appender {
    /// `seed_stamp`: the highest stamp already on the log (the cnc `log_time_ns`
    /// word at leader open; `0` on a fresh cluster). Every stamp this appender
    /// writes is `>= seed_stamp`.
    pub fn new(buffer: Arc<LogBuffer>, leadership_term_id: u32, seed_stamp: u64) -> Self {
        let pos = buffer.cnc.counters().append.load_acquire();
        let cached_durable = buffer.cnc.counters().durable.load_acquire();
        Self { buffer, pos, cached_durable, leadership_term_id, now_ns: seed_stamp, last_stamp: seed_stamp }
    }
    /// Once per leader pass (time-and-timers spec §3.2).
    pub fn set_now(&mut self, now_ns: u64) {
        self.now_ns = now_ns;
    }
    pub fn last_stamp(&self) -> u64 {
        self.last_stamp
    }
    #[inline]
    fn stamp(&mut self) -> u64 {
        let s = self.now_ns.max(self.last_stamp);
        self.last_stamp = s;
        s
    }
    #[inline]
    fn stamp_at(&mut self, want: u64) -> u64 {
        let s = want.max(self.last_stamp);
        self.last_stamp = s;
        s
    }
```

In `append`, `append_new_term`, `append_config`: compute `let time_ns = self.stamp();` **before** the header write (the padding frame written inside the same call uses the same value: pass it into `write_padding(off, pad_len, time_ns)` — change that helper to take the stamp rather than calling `stamp()` on `&self`). Each `FrameHeader { .. }` literal gets `client_id`, `seq`, `time_ns` (`0, 0, time_ns` for the node-originated types). Then add, modelled on `append_config` (same wrap/overrun discipline, returns the frame START like `append`):

```rust
    /// Append a TIMER frame (time-and-timers spec §4.2/§4.3). Stamped with the
    /// deadline, clamped to `last_stamp` — so `stamp > body.deadline_ns` means
    /// the timer is late. Returns `(frame_start, stamp)`.
    pub fn append_timer(&mut self, body: &TimerBody, flags: u8) -> Result<(u64, u64), AppendError> {
        let total = HEADER_LEN + TIMER_BODY_LEN;
        let aligned = align_frame_len(total) as u64;
        let b = &self.buffer;
        let off = b.offset(self.pos);
        let to_end = b.capacity - off as u64;
        let pad = if aligned > to_end { to_end } else { 0 };
        let end = self.pos + pad + aligned;
        if end > self.cached_durable + b.capacity {
            self.cached_durable = b.cnc.counters().durable.load_acquire();
            if end > self.cached_durable + b.capacity {
                return Err(AppendError::WouldOverrun);
            }
        }
        let time_ns = self.stamp_at(body.deadline_ns);
        if pad > 0 {
            self.write_padding(off, pad as u32, time_ns);
        }
        let foff = if pad > 0 { 0 } else { off };
        // SAFETY: single writer; the slot [foff, foff+aligned) is ours until the commit word.
        unsafe {
            let dst = std::slice::from_raw_parts_mut(b.region.ptr_at(foff), aligned as usize);
            frame::write_header_except_length(
                dst,
                &FrameHeader {
                    length: 0,
                    frame_type: FRAME_TYPE_TIMER,
                    flags,
                    leadership_term_id: self.leadership_term_id,
                    client_id: 0,
                    seq: 0,
                    time_ns,
                },
            );
            frame::write_timer_body(&mut dst[HEADER_LEN..], body);
        }
        b.commit_word(foff).store(total as u32, Ordering::Release);
        let start = self.pos + pad;
        self.pos = end;
        b.cnc.counters().append.store_release(self.pos);
        Ok((start, time_ns))
    }
```

Copy the exact wrap/commit/counter sequence from `append` (lines 574-628) rather than from this sketch where the two differ — `append` is the source of truth for the buffer discipline; only the header fields, the body write and the stamp are new. Import `TimerBody`, `TIMER_BODY_LEN`, `FRAME_TYPE_TIMER`, `write_timer_body` from `uc_protocol::v2::frame`.

- [ ] **Step 4: Fix the three other `Appender::new` callers**: `uc_node/src/node.rs:5438` becomes `Appender::new(Arc::clone(&self.buffer), open.term, self.cnc.log_time_ns())` followed by `appender.set_now(self.pass_now_ns);` — `pass_now_ns` does not exist until Task 8; for this task use `appender.set_now(wall_now_ns())` with a private `fn wall_now_ns() -> u64` (`SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)`) added next to `now_ns` (node.rs:3481), which Task 8 then reuses. The two test helpers (`node.rs:8296,8320`) and `uc_service/src/apply.rs:876` pass `0`.

- [ ] **Step 5: The archive carries the stamp.** In `uc_log/src/archive.rs` add `last_time_ns: u64` to `Archive` (init `0` beside `last_observed_term`), and in `observe_terms`'s loop, after the term branch:

```rust
            if h.frame_type != FRAME_TYPE_PADDING && h.time_ns > self.last_time_ns {
                self.last_time_ns = h.time_ns;
            }
```

In `do_work`, immediately after `self.observe_terms(slice, base);` (line 333):

```rust
        // Time-and-timers spec §3.2: the highest recorded stamp, the seed for
        // the next leader and the `uc2_log_time_ns` gauge. Never lowered.
        buffer.cnc.store_log_time_ns(self.last_time_ns);
```

`truncate_to` does **not** reset `last_time_ns` (Task 0 step 2 says why). Add to `archive.rs`'s tests, beside the existing term-observation test that appends frames through a real `Appender` and runs `do_work` (`grep -n "term_observations\|observe" uc_log/src/archive.rs | head`; copy that test's setup):

```rust
    #[test]
    fn archive_publishes_the_highest_recorded_stamp_and_never_lowers_it() {
        // setup copied from the term-observation test above: buffer + cnc + Archive over a temp journal
        let mut a = Appender::new(Arc::clone(&b), 1, 0);
        a.set_now(700);
        a.append(1, 1, b"a").unwrap();
        a.set_now(900);
        a.append(1, 2, b"b").unwrap();
        archive.do_work(&b).unwrap();
        assert_eq!(c.log_time_ns(), 900);
        c.store_log_time_ns(1_500); // e.g. a value left by a previous leader life
        a.set_now(1_000);
        a.append(1, 3, b"c").unwrap();
        archive.do_work(&b).unwrap();
        assert_eq!(c.log_time_ns(), 1_500, "the archive never lowers the word");
    }
```

(For the second assertion the archive's own `last_time_ns` is 1_000 and the store must be `max(word, last_time_ns)`: implement the store as `let cur = buffer.cnc.log_time_ns(); if self.last_time_ns > cur { buffer.cnc.store_log_time_ns(self.last_time_ns) }` — the archive agent is the single writer, so read-then-write is race-free.)

- [ ] **Step 6: Run**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p uc_log && cargo test -p uc_node --lib --test smoke --test failover && cargo test -p uc_service`
Expected: PASS; the three new buffer tests and the archive test green.

- [ ] **Step 7: Commit**

```bash
git add uc_log uc_node/src/node.rs uc_service/src/apply.rs
git commit -m "feat(uc_log): every frame stamped max(now, last) inside the Appender; append_timer stamps the deadline; archive carries log_time_ns (tests: every_frame_type_is_stamped_and_the_clamp_never_goes_backwards, append_timer_stamps_the_deadline_and_marks_late_by_clamp, pass_order_property_no_earlier_frame_exceeds_an_on_time_deadline, archive_publishes_the_highest_recorded_stamp_and_never_lowers_it)"
```

---

### Task 4: `MSG_V2_SCHED`, the 17-byte `SchedRecord`, and the `svc_sched.<row>.ring` on both sides

**Files:**
- Modify: `uc_protocol/src/v2/ipc.rs` (module doc 1-38; constants after `MSG_V2_BAD_SERVICE` line 59; helpers after `write_query_payload` ~100)
- Modify: `uc_node/src/ipc.rs` (after `egress_service_for`, line 94, + its test at 140-144)
- Modify: `uc_node/src/node.rs` (`create_rings` 5673-5732: the stale-unlink loop 5699-5702, the per-row loop 5716-5724, the return tuple; `struct Consensus` field beside `svc_query` 1879; the init literal 1316; the second constructor ~6646)
- Modify: `uc_service/src/attach.rs` (lines 134-142 open the producer; `ApplyState` literal 196-222), `uc_service/src/apply.rs` (`ApplyState` field beside `svc_query`, ~203)
- Test: `uc_protocol/src/v2/ipc.rs` `mod tests`; `uc_node/src/ipc.rs` test; `uc_node/tests/services.rs` (one new test)

**Interfaces:**
- Produces:
  - `pub const MSG_V2_SCHED: u16 = 8;` `pub const SCHED_RECORD_LEN: usize = 17;`
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum SchedOp { Schedule = 1, Cancel = 2, Consumed = 3 }`
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub struct SchedRecord { pub op: SchedOp, pub timer_id: u64, pub deadline_ns: u64 }`
  - `pub fn write_sched_record(r: &SchedRecord) -> [u8; SCHED_RECORD_LEN]`, `pub fn read_sched_record(buf: &[u8]) -> Option<SchedRecord>` (total: `None` on short input or an unknown op byte)
  - `uc_node::ipc::InstanceDir::svc_sched_ring_for(&self, id: u8) -> PathBuf` (`svc_sched.<id>.ring`)
  - `create_rings` returns an additional `Vec<Option<SpscConsumer>>` (by row); `Consensus.svc_sched: Vec<Option<SpscConsumer>>`
  - `ApplyState.svc_sched: SpscProducer` (service side)
- Consumed by: Tasks 5, 8, 13.

- [ ] **Step 1: Write the failing tests.** `uc_protocol/src/v2/ipc.rs` `mod tests`:

```rust
    /// FROZEN: op(1) ++ timer_id(8, LE) ++ deadline_ns(8, LE) = 17.
    #[test]
    fn sched_record_pins_literal_bytes_and_rejects_bad_ops() {
        let r = SchedRecord { op: SchedOp::Schedule, timer_id: 0x0102_0304_0506_0708, deadline_ns: 0x1122_3344_5566_7788 };
        let b = write_sched_record(&r);
        assert_eq!(b[0], 1);
        assert_eq!(&b[1..9], &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
        assert_eq!(&b[9..17], &[0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11]);
        assert_eq!(read_sched_record(&b), Some(r));
        for (op, code) in [(SchedOp::Cancel, 2u8), (SchedOp::Consumed, 3u8)] {
            let r = SchedRecord { op, timer_id: 1, deadline_ns: 2 };
            assert_eq!(write_sched_record(&r)[0], code);
            assert_eq!(read_sched_record(&write_sched_record(&r)), Some(r));
        }
        let mut bad = b;
        bad[0] = 0;
        assert_eq!(read_sched_record(&bad), None, "op 0 is not a record");
        bad[0] = 4;
        assert_eq!(read_sched_record(&bad), None, "op 4 is not a record");
        assert_eq!(read_sched_record(&b[..16]), None);
        assert_eq!(MSG_V2_SCHED, 8);
    }
```

`uc_node/src/ipc.rs` test (beside line 140):

```rust
        assert_eq!(d.svc_sched_ring_for(0), dir.path().join("svc_sched.0.ring"));
        assert_eq!(d.svc_sched_ring_for(7), dir.path().join("svc_sched.7.ring"));
```

`uc_node/tests/services.rs` (uses the module's `config`, `names`, `start_service`, `wait_until`, `tempdir` helpers):

```rust
#[test]
fn node_creates_one_sched_ring_per_declared_row_and_the_service_opens_it() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), names(&["count", "fsm1"], None))).unwrap();
    wait_until("serving", || node.can_serve());
    assert!(dir.path().join("svc_sched.0.ring").is_file());
    assert!(dir.path().join("svc_sched.1.ring").is_file());
    assert!(!dir.path().join("svc_sched.2.ring").exists(), "undeclared rows get no ring");
    let svc = start_service::<CountSm>(dir.path());
    // attach succeeded ⇒ the producer half opened; nothing is written yet
    svc.stop();
    node.stop();
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc_protocol ipc && cargo test -p uc_node --lib ipc && cargo test -p uc_node --test services node_creates_one_sched_ring`
Expected: compile errors / the ring files do not exist.

- [ ] **Step 3: Implement the codec** in `uc_protocol/src/v2/ipc.rs` (add to the module doc's ring list: `svc_sched.<id>.ring` (SPSC, service → node): [`MSG_V2_SCHED`] — payload a [`SchedRecord`]):

```rust
/// `svc_sched.<id>.ring` (SPSC, service → node, time-and-timers spec §4.4):
/// a schedule, cancel or consumed request for that row's timers. `header_extra`
/// is unused (zero).
pub const MSG_V2_SCHED: u16 = 8;

pub const SCHED_RECORD_LEN: usize = 17;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SchedOp {
    Schedule = 1,
    Cancel = 2,
    /// `Timed` delivered (or dropped) this instance; the node clears it.
    Consumed = 3,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedRecord {
    pub op: SchedOp,
    pub timer_id: u64,
    /// `0` for `Cancel`.
    pub deadline_ns: u64,
}

pub fn write_sched_record(r: &SchedRecord) -> [u8; SCHED_RECORD_LEN] {
    let mut out = [0u8; SCHED_RECORD_LEN];
    out[0] = r.op as u8;
    out[1..9].copy_from_slice(&r.timer_id.to_le_bytes());
    out[9..17].copy_from_slice(&r.deadline_ns.to_le_bytes());
    out
}

/// Total: `None` on a short slice or an op byte outside `1..=3`.
pub fn read_sched_record(buf: &[u8]) -> Option<SchedRecord> {
    if buf.len() < SCHED_RECORD_LEN {
        return None;
    }
    let op = match buf[0] {
        1 => SchedOp::Schedule,
        2 => SchedOp::Cancel,
        3 => SchedOp::Consumed,
        _ => return None,
    };
    Some(SchedRecord {
        op,
        timer_id: u64::from_le_bytes(buf[1..9].try_into().unwrap()),
        deadline_ns: u64::from_le_bytes(buf[9..17].try_into().unwrap()),
    })
}
```

- [ ] **Step 4: Node side.** `uc_node/src/ipc.rs`:

```rust
    /// Time-and-timers §4.4: the service→node schedule ring for row `id`.
    pub fn svc_sched_ring_for(&self, id: u8) -> PathBuf {
        self.root.join(format!("svc_sched.{id}.ring"))
    }
```

`create_rings`: add `stale.push(dir.svc_sched_ring_for(id));` in the unlink loop; in the per-row loop after the `svc_query` create:

```rust
        let sched = SpscRing::create(&dir.svc_sched_ring_for(id), MIB, MAX_MSG).map_err(to_io)?;
        let (_producer, consumer) = sched.into_split();
        svc_sched[id as usize] = Some(consumer);
```

with `let mut svc_sched: Vec<Option<SpscConsumer>> = (0..CNC_MAX_SERVICES).map(|_| None).collect();` declared beside `svc_query`, returned as a sixth tuple element, stored in a new `Consensus` field `svc_sched: Vec<Option<SpscConsumer>>` (next to `svc_query`, line 1879) and threaded through both constructors (1316 and ~6646 — the second constructor's `create_rings` call site takes the same tuple). Update the doc comment at 5658-5662 (per id: `svc_query` 1 MiB + `svc_sched` 1 MiB + broadcast 4 MiB + snapshots → 6 MiB per id) and `docs/reference/instance-directory.md`'s per-row reservation line (grep `5 MiB` there).

- [ ] **Step 5: Service side.** `uc_service/src/attach.rs` after line 142:

```rust
    // Time-and-timers §4.4: the service→node schedule ring; this process is the
    // producer, the node's consensus agent the consumer.
    let svc_sched_ring = SpscRing::open(&dir.join(format!("svc_sched.{}.ring", row)))
        .map_err(|e| ServiceError::Ring(e.to_string()))?;
    let (svc_sched, _svc_sched_consumer) = svc_sched_ring.into_split();
```

and `svc_sched,` in the `ApplyState { .. }` literal; `pub(crate) svc_sched: SpscProducer,` beside `svc_query` in `apply.rs`'s `ApplyState`. (`_svc_sched_consumer` is dropped like `_svc_query_producer`.)

- [ ] **Step 6: Run**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p uc_protocol ipc && cargo test -p uc_node --lib && cargo test -p uc_node --test services && cargo test -p uc_service && (cd fuzz && cargo +nightly check)`
Expected: PASS incl. `sched_record_pins_literal_bytes_and_rejects_bad_ops` and `node_creates_one_sched_ring_per_declared_row_and_the_service_opens_it`.

- [ ] **Step 7: Commit**

```bash
git add uc_protocol/src/v2/ipc.rs uc_node/src/ipc.rs uc_node/src/node.rs uc_node/tests/services.rs uc_service/src/attach.rs uc_service/src/apply.rs docs/reference/instance-directory.md
git commit -m "feat(ipc): MSG_V2_SCHED + SchedRecord; svc_sched.<row>.ring created by the node, opened by the service (tests: sched_record_pins_literal_bytes_and_rejects_bad_ops, node_creates_one_sched_ring_per_declared_row_and_the_service_opens_it)"
```

---

### Task 5: `ApplyCtx` time/term/schedule, `TimerEvent`, `on_timer`, TIMER delivery, sched records, re-announce

**Files:**
- Modify: `uc_service/src/traits.rs` (`ApplyCtx` 27-55; `StateMachine` 63-90; `RawStateMachine` 98-116; the blanket impl 121-147)
- Modify: `uc_service/src/session.rs:166-278` (`Sessioned` forwards `on_timer`), `uc_service/src/tagged.rs:18-37` (`Tagged` forwards), `uc_service/src/lib.rs:68-77` (exports)
- Modify: `uc_service/src/apply.rs` (`ApplyState` ~203: `announce_pending: bool`; `apply_cycle` 269: the announce step; the frame loop 373-450: fill `time_ns`/`term`, the TIMER branch, sched writes; the post-replay site 451-483)
- Modify: `uc_service/src/replay.rs:168-172` (fill `time_ns`/`term`; TIMER branch in replay too), `uc_service/src/attach.rs` (`announce_pending: true` in the literal)
- Test: `uc_service/tests/raw_contract.rs`, `uc_service/tests/apply.rs`

**Interfaces:**
- Produces (public):
  - `ApplyCtx { pub position: u64, pub time_ns: u64, pub term: u32, .. }`; `ApplyCtx::new(position, identity)` (time 0, term 0); `with_time(self, u64) -> Self`; `with_term(self, u32) -> Self`; `schedule(&mut self, id: u64, at_ns: u64)`; `cancel(&mut self, id: u64)`; `timers(&self) -> &[TimerReq]`
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum TimerReq { Schedule { id: u64, at_ns: u64 }, Cancel { id: u64 } }`
  - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub struct TimerEvent { pub id: u64, pub deadline_ns: u64, pub table: bool }` with `pub fn late(&self, ctx: &ApplyCtx) -> bool`
  - provided `fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {}` on `StateMachine` and `RawStateMachine`; the blanket impl forwards; `Sessioned` and `Tagged` forward
  - `pub trait TimerSource { fn pending_timers(&self) -> Vec<(u64, u64)> { Vec::new() } }` with a blanket `impl<S: RawStateMachine> TimerSource for S`? — **no**: a blanket would collide with `Timed`'s impl. Instead `RawStateMachine` gets a second provided method `fn pending_timers(&self) -> Vec<(u64, u64)> { Vec::new() }` (documented: "framework hook; only wrappers override it"). Task 6 overrides it in `Timed`.
- Produces (crate-private): `ApplyCtx::consumed(&mut self, id, deadline_ns)`, `ApplyCtx::take_sched_records(&mut self) -> Vec<SchedRecord>`
- Consumed by: Tasks 6, 10, 11.

- [ ] **Step 1: Write the failing tests.** `uc_service/tests/raw_contract.rs` (this file already builds `ApplyCtx::new(pos, Sm::IDENTITY)` for single-SM unit tests; add):

```rust
#[test]
fn ctx_carries_time_and_term_and_collects_requests_in_order() {
    let mut ctx = ApplyCtx::for_sm::<EchoSm>(64).with_time(1_234).with_term(9);
    assert_eq!((ctx.position, ctx.time_ns, ctx.term), (64, 1_234, 9));
    ctx.schedule(7, 5_000);
    ctx.cancel(3);
    ctx.schedule(7, 6_000);
    assert_eq!(
        ctx.timers(),
        &[
            TimerReq::Schedule { id: 7, at_ns: 5_000 },
            TimerReq::Cancel { id: 3 },
            TimerReq::Schedule { id: 7, at_ns: 6_000 },
        ]
    );
    let ev = TimerEvent { id: 7, deadline_ns: 1_000, table: false };
    assert!(ev.late(&ctx), "stamp 1_234 > deadline 1_000");
    let on_time = TimerEvent { id: 7, deadline_ns: 1_234, table: false };
    assert!(!on_time.late(&ctx));
}

struct TimerRecorder {
    seen: Vec<(u64, u64, u64, u32)>, // (position, id, deadline, term)
    last: Option<u64>,
}
impl RawStateMachine for TimerRecorder {
    const NAME: &'static str = "timer-recorder";
    fn apply(&mut self, ctx: &mut ApplyCtx, _cmd: &[u8], _out: &mut Vec<u8>) {
        self.last = Some(ctx.position);
    }
    fn query(&self, _q: &[u8], _out: &mut Vec<u8>) {}
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
        self.seen.push((ctx.position, ev.id, ev.deadline_ns, ctx.term));
        self.last = Some(ctx.position);
    }
}

#[test]
fn on_timer_defaults_to_a_noop_and_wrappers_forward_it() {
    // default: EchoSm does not override on_timer; calling it is a no-op that compiles
    let mut echo = EchoSm::default();
    let mut ctx = ApplyCtx::for_sm::<EchoSm>(96).with_time(5);
    RawStateMachine::on_timer(&mut echo, &mut ctx, TimerEvent { id: 1, deadline_ns: 5, table: false });
    // Sessioned forwards and advances its own last_applied
    let mut s = Sessioned::new(TimerRecorder { seen: vec![], last: None }, SessionConfig::default());
    let mut ctx = ApplyCtx::new(128, <Sessioned<TimerRecorder> as RawStateMachine>::IDENTITY).with_time(7).with_term(2);
    s.on_timer(&mut ctx, TimerEvent { id: 42, deadline_ns: 7, table: false });
    assert_eq!(s.last_applied(), Some(128));
    assert_eq!(s.inner().seen, vec![(128, 42, 7, 2)]);
}
```

(`EchoSm` is the file's existing typed test SM; if its name differs, use that file's typed SM. `Sessioned::inner()` exists since the identity plan.)

`uc_service/tests/apply.rs` — an end-to-end test that writes a TIMER frame onto the real log through the node and asserts `on_timer` ran, using the module's `node_config`, `wait_until`, `open_ingress`/`write_submit` helpers. Because no node-side scheduler exists until Task 8, this test drives the frame through `Node::append_timer_for_test` — **add that test-only hook in this task** (`#[doc(hidden)] pub fn append_timer_for_test(&self, body: TimerBody) -> Result<(), String>` on `uc_node::Node`, forwarding a `NodeCmd`-style message to the consensus agent exactly as `Node::submit` forwards a payload — copy `submit`'s channel path and add a `TimerForTest(TimerBody)` variant to the same enum that calls `app.append_timer(&body, 0)` in the leader's `drain_ingress`). Then:

```rust
#[derive(Default)]
struct TimerCountSm {
    fired: Vec<(u64, u64, u64)>, // (position, id, time_ns)
    last: Option<u64>,
}
impl StateMachine for TimerCountSm {
    const NAME: &'static str = "svc-test";
    type Command = u8;
    type Response = u64; // the stamp the command was applied at
    type Query = ();
    type QueryResponse = Vec<(u64, u64, u64)>;
    fn apply(&mut self, ctx: &mut ApplyCtx, _cmd: u8) -> u64 {
        self.last = Some(ctx.position);
        ctx.time_ns
    }
    fn query(&self, _q: ()) -> Vec<(u64, u64, u64)> {
        self.fired.clone()
    }
    fn last_applied(&self) -> Option<u64> { self.last }
    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
        self.fired.push((ctx.position, ev.id, ctx.time_ns));
        self.last = Some(ctx.position);
    }
}

#[test]
fn timer_frame_is_delivered_to_the_named_fsm_only_and_responses_carry_time() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::start(node_config(dir.path(), "svc-test")).unwrap();
    wait_until(|| node.can_serve());
    let svc = ServiceBuilder::new(ServiceConfig::new(dir.path(), "svc-test"), TimerCountSm::default()).start().unwrap();
    let hash = <TimerCountSm as RawStateMachine>::IDENTITY.hash();
    node.append_timer_for_test(TimerBody { identity_hash: hash ^ 1, timer_id: 1, deadline_ns: 1 }).unwrap(); // foreign: skipped
    node.append_timer_for_test(TimerBody { identity_hash: hash, timer_id: 2, deadline_ns: 1 }).unwrap();
    wait_until(|| svc.query(()).len() == 1);
    let fired = svc.query(());
    assert_eq!(fired[0].1, 2, "only the frame naming this FSM's hash was delivered: {fired:?}");
    assert!(fired[0].2 > 0, "the frame carries a stamp: {fired:?}");
    // a client command applied after the timer carries a stamp >= the timer's
    let client = uc_client::Client::connect(dir.path(), "svc-test").unwrap();
    let stamp: u64 = client.submit(&7u8).unwrap();
    assert!(stamp >= fired[0].2, "monotone: {stamp} < {}", fired[0].2);
    client.shutdown();
    svc.stop();
    node.stop();
}
```

(`Client::connect(dir, app_id)` and `submit` are what `uc_node/tests/services.rs` uses; the module's `node_config` sets the app id the second argument names.)

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc_service --test raw_contract ctx_carries && cargo test -p uc_service --test apply timer_frame`
Expected: compile errors (`with_time`, `TimerReq`, `TimerEvent`, `on_timer`, `append_timer_for_test` unknown).

- [ ] **Step 3: Implement `ApplyCtx` and the traits** in `uc_service/src/traits.rs`:

```rust
use uc_protocol::v2::ipc::{SchedOp, SchedRecord};

/// A request a state machine made during one apply (time-and-timers §3.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimerReq {
    Schedule { id: u64, at_ns: u64 },
    Cancel { id: u64 },
}

/// A fired timer, as delivered to `on_timer` (time-and-timers §4.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimerEvent {
    pub id: u64,
    pub deadline_ns: u64,
    /// Fired from the replicated schedule table (plan 2), not from `schedule`.
    pub table: bool,
}
impl TimerEvent {
    /// The leader could not place this timer at its deadline (spec §4.3's
    /// post-failover case): `ctx.time_ns > deadline_ns`.
    pub fn late(&self, ctx: &ApplyCtx) -> bool {
        ctx.time_ns > self.deadline_ns
    }
}

#[non_exhaustive]
#[derive(Debug)]
pub struct ApplyCtx {
    /// The frame's absolute byte position (the idempotency key).
    pub position: u64,
    /// The frame's leader stamp: ns since the Unix epoch, non-decreasing along
    /// the log, identical on every replica (time-and-timers §3). "Now".
    pub time_ns: u64,
    /// The frame's `leadership_term_id`.
    pub term: u32,
    identity: FsmIdentity,
    timers: Vec<TimerReq>,
    consumed: Vec<(u64, u64)>,
}

impl ApplyCtx {
    pub fn new(position: u64, identity: FsmIdentity) -> ApplyCtx {
        ApplyCtx { position, time_ns: 0, term: 0, identity, timers: Vec::new(), consumed: Vec::new() }
    }
    pub fn for_sm<S: RawStateMachine>(position: u64) -> ApplyCtx {
        ApplyCtx::new(position, S::IDENTITY)
    }
    /// Test builder; the apply loop sets the field from the frame header.
    pub fn with_time(mut self, time_ns: u64) -> ApplyCtx {
        self.time_ns = time_ns;
        self
    }
    pub fn with_term(mut self, term: u32) -> ApplyCtx {
        self.term = term;
        self
    }
    pub fn identity(&self) -> FsmIdentity {
        self.identity
    }
    pub fn ids(&self) -> IdGen {
        IdGen::new(self.position, self.identity)
    }
    /// Ask for `on_timer(id)` at `at_ns` (log time). Re-scheduling a pending id
    /// replaces its deadline. Deterministic: an output of apply, replayed
    /// identically on every replica (time-and-timers §4.4).
    pub fn schedule(&mut self, id: u64, at_ns: u64) {
        self.timers.push(TimerReq::Schedule { id, at_ns });
    }
    pub fn cancel(&mut self, id: u64) {
        self.timers.push(TimerReq::Cancel { id });
    }
    /// What this apply has asked so far, in order (read by `Timed`).
    pub fn timers(&self) -> &[TimerReq] {
        &self.timers
    }
    /// `Timed` only: this instance was delivered or dropped; the node may clear it.
    pub(crate) fn consumed(&mut self, id: u64, deadline_ns: u64) {
        self.consumed.push((id, deadline_ns));
    }
    /// Apply loop only: drain both lists as wire records, requests first.
    pub(crate) fn take_sched_records(&mut self) -> Vec<SchedRecord> {
        let mut out = Vec::with_capacity(self.timers.len() + self.consumed.len());
        for r in self.timers.drain(..) {
            out.push(match r {
                TimerReq::Schedule { id, at_ns } => SchedRecord { op: SchedOp::Schedule, timer_id: id, deadline_ns: at_ns },
                TimerReq::Cancel { id } => SchedRecord { op: SchedOp::Cancel, timer_id: id, deadline_ns: 0 },
            });
        }
        for (id, dl) in self.consumed.drain(..) {
            out.push(SchedRecord { op: SchedOp::Consumed, timer_id: id, deadline_ns: dl });
        }
        out
    }
}
```

On `StateMachine` (after `last_applied`):

```rust
    /// A timer this FSM scheduled (or the schedule table fired) has reached
    /// its position on the log. `ctx.time_ns` is the frame's stamp — the
    /// deadline unless `ev.late(ctx)`. Advance `last_applied` from
    /// `ctx.position` exactly as in `apply`. Default: ignore timers.
    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
        let _ = (ctx, ev);
    }
```

On `RawStateMachine` the same `on_timer`, plus:

```rust
    /// Framework hook (time-and-timers §4.8): the pending instances a wrapper
    /// holds, re-announced to the node after attach and after replay. Only
    /// `Timed` overrides it; a bare state machine has none.
    fn pending_timers(&self) -> Vec<(u64, u64)> {
        Vec::new()
    }
```

Blanket impl: `fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) { StateMachine::on_timer(self, ctx, ev) }` (do **not** forward `pending_timers`; a typed SM has none). Update the `StateMachine` doc comment ("no clock" → "no clock of its own: `ctx.time_ns` is the log's").

`session.rs`, inside `impl<S: RawStateMachine> RawStateMachine for Sessioned<S>`:

```rust
    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
        self.max_pos_seen = Some(ctx.position).max(self.max_pos_seen);
        self.inner.on_timer(ctx, ev)
    }
    fn pending_timers(&self) -> Vec<(u64, u64)> {
        self.inner.pending_timers()
    }
```

`tagged.rs`, inside `impl StateMachine for Tagged<ROW, S>`: `fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) { self.0.on_timer(ctx, ev) }`. `lib.rs`: add `TimerEvent, TimerReq` to the `traits` re-export and to the "promised surface" doc line.

- [ ] **Step 4: The apply loop.** `uc_service/src/apply.rs`. Add `pub(crate) announce_pending: bool` to `ApplyState` (set `true` in `attach.rs`'s literal). At the top of `apply_cycle` (after the poisoned check, before the `loop`):

```rust
    if st.announce_pending {
        st.announce_pending = false;
        let pending = st.sm.lock().unwrap().pending_timers();
        let recs: Vec<SchedRecord> = pending
            .into_iter()
            .map(|(id, dl)| SchedRecord { op: SchedOp::Schedule, timer_id: id, deadline_ns: dl })
            .collect();
        write_sched(&mut st.svc_sched, &recs);
    }
```

with, at module level:

```rust
/// Write schedule records to the node; a full ring is transient (the node
/// drains every pass), so spin like the egress path does, and count it.
fn write_sched(prod: &mut SpscProducer, recs: &[SchedRecord]) {
    for r in recs {
        let bytes = write_sched_record(r);
        let mut spins = 0u32;
        loop {
            match prod.try_write(MSG_V2_SCHED, 0, [0; 8], &bytes) {
                Ok(()) => break,
                Err(RingError::Full) => {
                    spins += 1;
                    if spins == 1_000 {
                        crate::obs_event!(Warn, "sched_ring_full", spins = spins as u64);
                    }
                    std::thread::yield_now();
                }
                Err(e) => panic!("svc_sched ring fail-stop: {e}"),
            }
        }
    }
}
```

(`obs_event!` is available through `uc_obs`; if `uc_service` does not depend on it, use the crate's existing logging path — `grep -n "obs_event\|eprintln" uc_service/src/apply.rs` — and keep the counter as a `static AtomicU64` for a later export.)

In the frame loop replace the single `if` at line 381 with:

```rust
                    let above = Some(pos) > sm.last_applied();
                    if hdr.frame_type == FRAME_TYPE_MESSAGE && above {
                        // (existing body, with:)
                        let mut ctx = ApplyCtx::new(pos, S::IDENTITY)
                            .with_time(hdr.time_ns)
                            .with_term(hdr.leadership_term_id);
                        sm.apply(&mut ctx, payload, &mut st.resp_buf);
                        // (existing egress publish, profile hooks …)
                        let recs = ctx.take_sched_records();
                        if !recs.is_empty() {
                            write_sched(&mut st.svc_sched, &recs);
                        }
                    } else if hdr.frame_type == FRAME_TYPE_TIMER && above {
                        if let Some(body) = read_timer_body(payload)
                            && body.identity_hash == S::IDENTITY.hash()
                        {
                            let mut ctx = ApplyCtx::new(pos, S::IDENTITY)
                                .with_time(hdr.time_ns)
                                .with_term(hdr.leadership_term_id);
                            sm.on_timer(
                                &mut ctx,
                                TimerEvent {
                                    id: body.timer_id,
                                    deadline_ns: body.deadline_ns,
                                    table: hdr.flags & FLAG_TIMER_TABLE != 0,
                                },
                            );
                            let recs = ctx.take_sched_records();
                            if !recs.is_empty() {
                                write_sched(&mut st.svc_sched, &recs);
                            }
                        }
                    }
```

(`sm` is the `MutexGuard` over `st.sm`; `st.svc_sched` is a different field, so the borrows are disjoint as `st.resp_buf`'s already is.) After the post-replay block (line 480, `st.lag_waiting = false;`) add `st.announce_pending = true;`.

`replay.rs:168-172`: fill `time_ns`/`term` the same way, and add the TIMER branch there too — replay must deliver timers the live loop would have (the guard `Some(pos) > guard.last_applied()` and the hash check identical; `on_timer` requests during replay are dropped: `let _ = ctx.take_sched_records();` — the re-announce after replay covers them, which is the whole point of §4.8).

- [ ] **Step 5: The test hook on `Node`.** In `uc_node/src/node.rs`, find the enum the in-process `submit` path pushes onto `ingress_rx` (`grep -n "ingress_tx\|ingress_rx" uc_node/src/node.rs | head`). If it carries `Vec<u8>` payloads directly, change it to `enum Ingress { Payload(Vec<u8>), TimerForTest(TimerBody) }` (the `submit` path wraps in `Payload`); in `drain_ingress`, a `TimerForTest(b)` arm calls `app.append_timer(&b, 0)` with the same `WouldOverrun` hold-back as `try_append`. Add:

```rust
    /// Test-only (plan Task 5): append a TIMER frame as the leader would, so a
    /// service test can drive `on_timer` before the node-side scheduler exists.
    #[doc(hidden)]
    pub fn append_timer_for_test(&self, body: TimerBody) -> Result<(), String> {
        self.ingress_tx.send(Ingress::TimerForTest(body)).map_err(|e| e.to_string())
    }
```

- [ ] **Step 6: Run**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo clippy -p uc_service --features apply-profile --all-targets -- -D warnings && cargo test -p uc_service && cargo test -p uc_node --lib --test smoke --test services`
Expected: PASS incl. `ctx_carries_time_and_term_and_collects_requests_in_order`, `on_timer_defaults_to_a_noop_and_wrappers_forward_it`, `timer_frame_is_delivered_to_the_named_fsm_only_and_responses_carry_time`. Watched red: revert the `else if … FRAME_TYPE_TIMER` branch and the `apply.rs` test must hang on its `wait_until` (10 s) — record that in the commit message.

- [ ] **Step 7: Commit**

```bash
git add uc_service uc_node/src/node.rs
git commit -m "feat(uc_service): ApplyCtx time_ns/term/schedule/cancel, TimerEvent, provided on_timer on both tiers, TIMER delivery by identity hash, sched records to the node, re-announce after attach/replay (tests: ctx_carries_time_and_term_and_collects_requests_in_order, on_timer_defaults_to_a_noop_and_wrappers_forward_it, timer_frame_is_delivered_to_the_named_fsm_only_and_responses_carry_time)"
```

---

### Task 6: `Timed<S>` — exactly-once delivery from log content alone

**Files:**
- Create: `uc_service/src/timed.rs`
- Modify: `uc_service/src/lib.rs` (`pub mod timed;`, `pub use timed::Timed;`)
- Test: `uc_service/tests/timed.rs` (new)

**Interfaces:**
- Produces: `pub struct Timed<S> { .. }` with `Timed::new(inner: S) -> Timed<S>`, `inner(&self) -> &S`, `pending(&self) -> Vec<(u64, u64)>` (sorted by id); `impl<S: RawStateMachine> RawStateMachine for Timed<S>` (forwards `NAME`/`VERSION`; `apply` forwards then updates the maps from `ctx.timers()`; `on_timer` filters; `pending_timers` returns the pending map); `impl<S: SnapshotStateMachine> SnapshotStateMachine for Timed<S>` (blob = bincode `TimedImage { pending: BTreeMap<u64,u64>, table_last: BTreeMap<u64,u64> }`, length-prefixed, ahead of the inner artifact — exactly `Sessioned`'s shape)
- Consumed by: Tasks 10, 11.

- [ ] **Step 1: Write the failing tests** in `uc_service/tests/timed.rs`:

```rust
use uc_service::{ApplyCtx, RawStateMachine, SnapshotStateMachine, Timed, TimerEvent};

#[derive(Default)]
struct Rec {
    fired: Vec<(u64, u64, u64)>, // (position, id, deadline)
    last: Option<u64>,
}
impl RawStateMachine for Rec {
    const NAME: &'static str = "rec";
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], _out: &mut Vec<u8>) {
        // cmd: b"s<id>@<at>" schedules, b"c<id>" cancels
        let s = std::str::from_utf8(cmd).unwrap();
        if let Some(rest) = s.strip_prefix('s') {
            let (id, at) = rest.split_once('@').unwrap();
            ctx.schedule(id.parse().unwrap(), at.parse().unwrap());
        } else if let Some(id) = s.strip_prefix('c') {
            ctx.cancel(id.parse().unwrap());
        }
        self.last = Some(ctx.position);
    }
    fn query(&self, _q: &[u8], _out: &mut Vec<u8>) {}
    fn last_applied(&self) -> Option<u64> { self.last }
    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
        self.fired.push((ctx.position, ev.id, ev.deadline_ns));
        self.last = Some(ctx.position);
    }
}

fn ctx(pos: u64, t: u64) -> ApplyCtx {
    ApplyCtx::for_sm::<Timed<Rec>>(pos).with_time(t)
}
fn ev(id: u64, dl: u64) -> TimerEvent {
    TimerEvent { id, deadline_ns: dl, table: false }
}

#[test]
fn delivers_a_pending_instance_exactly_once_and_reports_consumed() {
    let mut t = Timed::new(Rec::default());
    let mut c = ctx(64, 100);
    t.apply(&mut c, b"s7@500", &mut Vec::new());
    assert_eq!(t.pending(), vec![(7, 500)]);
    assert_eq!(t.pending_timers(), vec![(7, 500)]);
    // the schedule request is still in ctx for the apply loop to forward
    assert_eq!(c.timers().len(), 1);
    let mut c = ctx(128, 500);
    t.on_timer(&mut c, ev(7, 500));
    assert_eq!(t.inner().fired, vec![(128, 7, 500)]);
    assert!(t.pending().is_empty());
    assert_eq!(t.last_applied(), Some(128));
    // duplicate (a re-fire after leadership loss): dropped, still consumed, still advances
    let mut c = ctx(192, 500);
    t.on_timer(&mut c, ev(7, 500));
    assert_eq!(t.inner().fired.len(), 1, "dropped");
    assert_eq!(t.last_applied(), Some(192));
}

#[test]
fn reschedule_replaces_and_cancel_wins_over_a_fire_already_on_the_log() {
    let mut t = Timed::new(Rec::default());
    t.apply(&mut ctx(64, 100), b"s7@500", &mut Vec::new());
    t.apply(&mut ctx(96, 100), b"s7@900", &mut Vec::new());
    assert_eq!(t.pending(), vec![(7, 900)], "replaced");
    t.on_timer(&mut ctx(128, 500), ev(7, 500));
    assert!(t.inner().fired.is_empty(), "the stale instance (7, 500) is not pending");
    t.apply(&mut ctx(160, 600), b"c7", &mut Vec::new());
    assert!(t.pending().is_empty());
    t.on_timer(&mut ctx(224, 900), ev(7, 900));
    assert!(t.inner().fired.is_empty(), "cancel wins");
}

#[test]
fn a_bare_state_machine_gets_every_frame_but_timed_filters() {
    let mut bare = Rec::default();
    bare.on_timer(&mut ApplyCtx::for_sm::<Rec>(1).with_time(1), ev(1, 1));
    bare.on_timer(&mut ApplyCtx::for_sm::<Rec>(2).with_time(1), ev(1, 1));
    assert_eq!(bare.fired.len(), 2, "at-least-once without the wrapper");
}
```

plus a snapshot round trip: implement `SnapshotStateMachine for Rec` in the test (freeze = bincode of `fired` + `last`, install = decode), then `Timed::freeze` → `stream_snapshot` into a `Vec<u8>` → a fresh `Timed::new(Rec::default())` `install_snapshot(pos, &mut &bytes[..])` → `pending()` equal to the original's and a subsequent `on_timer` for a pending instance delivered, for a consumed one dropped.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc_service --test timed`
Expected: compile error (`Timed` unknown).

- [ ] **Step 3: Implement** `uc_service/src/timed.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `Timed<S>`: exactly-once timer delivery (time-and-timers spec §4.6).
//!
//! The node fires timers **at least once** (it re-arms in-flight instances on
//! leadership loss). This wrapper keeps the pending set the inner state
//! machine asked for — rebuilt from the log on replay, carried in the
//! snapshot — and delivers a `TIMER` frame to the inner `on_timer` only if
//! its `(id, deadline)` is still pending. Every replica decides identically
//! because the decision reads nothing but committed frames.

use std::collections::BTreeMap;

use crate::config::SnapshotError;
use crate::traits::{ApplyCtx, RawStateMachine, SnapshotStateMachine, TimerEvent, TimerReq};

const MAX_IMAGE_LEN: u64 = 1 << 26;

pub struct Timed<S> {
    inner: S,
    pending: BTreeMap<u64, u64>,
    /// Plan 2: last delivered deadline per table id. Carried in the image now
    /// so the snapshot format does not change when the table lands.
    table_last: BTreeMap<u64, u64>,
    max_pos_seen: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct TimedImage {
    pending: BTreeMap<u64, u64>,
    table_last: BTreeMap<u64, u64>,
}

impl<S> Timed<S> {
    pub fn new(inner: S) -> Timed<S> {
        Timed { inner, pending: BTreeMap::new(), table_last: BTreeMap::new(), max_pos_seen: None }
    }
    pub fn inner(&self) -> &S {
        &self.inner
    }
    pub fn pending(&self) -> Vec<(u64, u64)> {
        self.pending.iter().map(|(&i, &d)| (i, d)).collect()
    }
    fn absorb(&mut self, reqs: &[TimerReq]) {
        for r in reqs {
            match *r {
                TimerReq::Schedule { id, at_ns } => {
                    self.pending.insert(id, at_ns);
                }
                TimerReq::Cancel { id } => {
                    self.pending.remove(&id);
                }
            }
        }
    }
}

impl<S: RawStateMachine> RawStateMachine for Timed<S> {
    const NAME: &'static str = S::NAME;
    const VERSION: u32 = S::VERSION;

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        self.max_pos_seen = Some(ctx.position).max(self.max_pos_seen);
        let before = ctx.timers().len();
        self.inner.apply(ctx, cmd, out);
        let reqs: Vec<TimerReq> = ctx.timers()[before..].to_vec();
        self.absorb(&reqs);
    }

    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
        self.max_pos_seen = Some(ctx.position).max(self.max_pos_seen);
        let deliver = if ev.table {
            self.table_last.get(&ev.id).is_none_or(|&last| ev.deadline_ns > last)
        } else {
            self.pending.get(&ev.id) == Some(&ev.deadline_ns)
        };
        if deliver {
            if ev.table {
                self.table_last.insert(ev.id, ev.deadline_ns);
            } else {
                self.pending.remove(&ev.id);
            }
            let before = ctx.timers().len();
            self.inner.on_timer(ctx, ev);
            let reqs: Vec<TimerReq> = ctx.timers()[before..].to_vec();
            self.absorb(&reqs);
        }
        ctx.consumed(ev.id, ev.deadline_ns);
    }

    fn query(&self, q: &[u8], out: &mut Vec<u8>) {
        self.inner.query(q, out)
    }

    fn last_applied(&self) -> Option<u64> {
        self.inner.last_applied().max(self.max_pos_seen)
    }

    fn pending_timers(&self) -> Vec<(u64, u64)> {
        self.pending()
    }
}
```

`is_none_or` is Rust 1.82+, inside MSRV 1.89. The `SnapshotStateMachine` impl copies `Sessioned`'s (`session.rs:293-381`) line for line with `TimedImage` in place of `TableImage` and `MAX_IMAGE_LEN` in place of `MAX_TABLE_BLOB_LEN`; `install_snapshot` resets `max_pos_seen = Some(installed)`. `absorb` after `on_timer` handles an FSM that re-schedules itself from inside its timer handler (the recurring-timer pattern).

- [ ] **Step 4: Run**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p uc_service`
Expected: PASS incl. the four `timed` tests.

- [ ] **Step 5: Commit**

```bash
git add uc_service/src/timed.rs uc_service/src/lib.rs uc_service/tests/timed.rs
git commit -m "feat(uc_service): Timed<S> — exactly-once timer delivery from the log-derived pending set, consumed reports, snapshot image (tests: delivers_a_pending_instance_exactly_once_and_reports_consumed, reschedule_replaces_and_cancel_wins_over_a_fire_already_on_the_log, a_bare_state_machine_gets_every_frame_but_timed_filters, snapshot round trip)"
```

---

### Task 7: `uc_node::timers::RowTimers` — the per-row heap, pure and unit-tested

**Files:**
- Create: `uc_node/src/timers.rs`
- Modify: `uc_node/src/lib.rs` (`pub(crate) mod timers;` — or `pub mod` if the crate exposes internals for tests the way `preflight` is; follow the neighbouring `mod` lines)
- Test: `uc_node/src/timers.rs` `mod tests`

**Interfaces:**
- Produces:
  - `pub struct RowTimers { .. }` with `RowTimers::new(identity_hash: u64) -> Self`, `hash(&self) -> u64`, `schedule(&mut self, id: u64, deadline_ns: u64)`, `cancel(&mut self, id: u64)`, `consumed(&mut self, id: u64, deadline_ns: u64)`, `peek_due(&mut self, now_ns: u64) -> Option<(u64, u64)>` (`(id, deadline)` of the earliest due entry, lazily discarding stale heap entries; does not remove it), `take_in_flight(&mut self, id: u64, deadline_ns: u64)` (pending → in-flight after a successful append), `rearm(&mut self) -> usize` (in-flight → pending, returns how many), `pending_len(&self) -> usize`, `in_flight_len(&self) -> usize`
  - `pub struct TimerStats { pub fired: [AtomicU64; 8], pub late: [AtomicU64; 8], pub rearmed: [AtomicU64; 8] }` (`Default`; shared with `/metrics` via `Arc`)
- Consumed by: Tasks 8, 9.

- [ ] **Step 1: Write the failing tests** at the bottom of the new file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_replace_cancel_and_due_order_across_ids() {
        let mut t = RowTimers::new(0xabc);
        t.schedule(1, 500);
        t.schedule(2, 300);
        t.schedule(1, 400); // replace
        assert_eq!(t.pending_len(), 2);
        assert_eq!(t.peek_due(299), None);
        assert_eq!(t.peek_due(300), Some((2, 300)));
        t.cancel(2);
        assert_eq!(t.peek_due(1_000), Some((1, 400)), "stale heap entry for (2,300) and (1,500) skipped");
        t.take_in_flight(1, 400);
        assert_eq!(t.peek_due(1_000), None);
        assert_eq!((t.pending_len(), t.in_flight_len()), (0, 1));
    }

    #[test]
    fn consumed_clears_in_flight_on_the_leader_and_pending_on_a_follower() {
        let mut leader = RowTimers::new(1);
        leader.schedule(9, 100);
        assert_eq!(leader.peek_due(100), Some((9, 100)));
        leader.take_in_flight(9, 100);
        leader.consumed(9, 100);
        assert_eq!((leader.pending_len(), leader.in_flight_len()), (0, 0));

        let mut follower = RowTimers::new(1);
        follower.schedule(9, 100);
        follower.consumed(9, 100); // never fired here; the log delivered it
        assert_eq!(follower.pending_len(), 0);
        assert_eq!(follower.peek_due(u64::MAX), None);

        let mut stale = RowTimers::new(1);
        stale.schedule(9, 200); // re-scheduled after the fire the consumed refers to
        stale.consumed(9, 100);
        assert_eq!(stale.pending_len(), 1, "a consumed for an older instance leaves the new one");
    }

    #[test]
    fn rearm_moves_in_flight_back_and_they_fire_again() {
        let mut t = RowTimers::new(1);
        t.schedule(4, 50);
        t.schedule(5, 60);
        t.take_in_flight(4, 50);
        t.take_in_flight(5, 60);
        assert_eq!(t.rearm(), 2);
        assert_eq!(t.peek_due(100), Some((4, 50)));
        t.take_in_flight(4, 50);
        assert_eq!(t.peek_due(100), Some((5, 60)));
        assert_eq!(t.rearm(), 1, "only the still in-flight one");
    }

    #[test]
    fn reschedule_of_an_in_flight_id_supersedes_it() {
        let mut t = RowTimers::new(1);
        t.schedule(7, 10);
        t.take_in_flight(7, 10);
        t.schedule(7, 20); // the FSM re-armed it from on_timer before consumed arrived
        assert_eq!(t.in_flight_len(), 0, "the old instance can no longer be re-armed");
        assert_eq!(t.peek_due(20), Some((7, 20)));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_node --lib timers`
Expected: compile error (module missing).

- [ ] **Step 3: Implement** `uc_node/src/timers.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Per-row timer heap (time-and-timers spec §4.5). Kept on EVERY node from the
//! row's service `svc_sched` records; only the leader pops by time. No
//! persistence: the heap is a cache of what the service knows and converges
//! from the service's re-announce after a restart. At-least-once by design —
//! `rearm` after a leadership loss may fire an instance twice; `Timed<S>` on
//! the service side drops the duplicate.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::atomic::AtomicU64;

use uc_protocol::v2::cnc::CNC_MAX_SERVICES;

pub struct RowTimers {
    hash: u64,
    /// id → deadline of the pending instance (one per id).
    pending: HashMap<u64, u64>,
    /// (deadline, id), lazily deleted: an entry whose deadline no longer
    /// matches `pending[id]` is stale and skipped.
    heap: BinaryHeap<Reverse<(u64, u64)>>,
    /// Appended by this node as leader, not yet reported consumed.
    in_flight: HashMap<u64, u64>,
}

impl RowTimers {
    pub fn new(identity_hash: u64) -> Self {
        Self { hash: identity_hash, pending: HashMap::new(), heap: BinaryHeap::new(), in_flight: HashMap::new() }
    }
    pub fn hash(&self) -> u64 {
        self.hash
    }
    pub fn schedule(&mut self, id: u64, deadline_ns: u64) {
        self.in_flight.remove(&id); // a newer instance supersedes an in-flight one
        self.pending.insert(id, deadline_ns);
        self.heap.push(Reverse((deadline_ns, id)));
    }
    pub fn cancel(&mut self, id: u64) {
        self.pending.remove(&id);
        self.in_flight.remove(&id);
    }
    pub fn consumed(&mut self, id: u64, deadline_ns: u64) {
        if self.in_flight.get(&id) == Some(&deadline_ns) {
            self.in_flight.remove(&id);
        }
        if self.pending.get(&id) == Some(&deadline_ns) {
            self.pending.remove(&id);
        }
    }
    /// Earliest pending instance with `deadline <= now_ns`, or `None`.
    pub fn peek_due(&mut self, now_ns: u64) -> Option<(u64, u64)> {
        while let Some(Reverse((dl, id))) = self.heap.peek().copied() {
            if self.pending.get(&id) != Some(&dl) {
                self.heap.pop(); // stale
                continue;
            }
            return if dl <= now_ns { Some((id, dl)) } else { None };
        }
        None
    }
    /// After the leader appended `(id, deadline)`: pending → in-flight.
    pub fn take_in_flight(&mut self, id: u64, deadline_ns: u64) {
        if self.pending.get(&id) == Some(&deadline_ns) {
            self.pending.remove(&id);
            self.heap.pop(); // it was the head `peek_due` returned
            self.in_flight.insert(id, deadline_ns);
        }
    }
    /// Leadership lost: every in-flight instance is pending again.
    pub fn rearm(&mut self) -> usize {
        let n = self.in_flight.len();
        for (id, dl) in self.in_flight.drain() {
            self.pending.insert(id, dl);
            self.heap.push(Reverse((dl, id)));
        }
        n
    }
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }
    pub fn in_flight_len(&self) -> usize {
        self.in_flight.len()
    }
}

/// Process-local counters `/metrics` renders per row (spec §6).
#[derive(Default)]
pub struct TimerStats {
    pub fired: [AtomicU64; CNC_MAX_SERVICES],
    pub late: [AtomicU64; CNC_MAX_SERVICES],
    pub rearmed: [AtomicU64; CNC_MAX_SERVICES],
}
```

`take_in_flight`'s `heap.pop()` relies on the caller having just called `peek_due` (which leaves the matching entry at the head); assert that in a `debug_assert_eq!` on the popped value.

- [ ] **Step 4: Run**

Run: `cargo test -p uc_node --lib timers`
Expected: PASS, four tests.

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/timers.rs uc_node/src/lib.rs
git commit -m "feat(uc_node): RowTimers — per-row pending/heap/in-flight with lazy deletion and re-arm (tests: schedule_replace_cancel_and_due_order_across_ids, consumed_clears_in_flight_on_the_leader_and_pending_on_a_follower, rearm_moves_in_flight_back_and_they_fire_again, reschedule_of_an_in_flight_id_supersedes_it)"
```

---

### Task 8: The leader pass — clock once, timers before clients, re-arm on leader exit, seed, pending word

**Files:**
- Modify: `uc_node/src/node.rs`: `struct Consensus` (1835; fields beside `svc_query` 1879 and `service_last_epoch` 2056), both constructors (1316/1354 and ~6646/6711), `do_work` (2230-2449), `on_collapsed` (5420-5455), `Action::BecomeFollower` (5009-5045), `halt` (5304-5321), `publish_status` (~3460-3478), `Node::observability` (1664)
- Modify: `uc_node/src/obs/mod.rs:42-59` (`ObsSources.timer_stats: Arc<TimerStats>`)
- Test: `uc_node/src/node.rs` unit tests are impractical for the agent; the end-to-end tests are Task 10. This task's own checks are the existing suites plus `cargo clippy`.

**Interfaces:**
- Produces on `Consensus`: `pass_now_ns: u64`, `timers: Vec<Option<RowTimers>>` (by row; `Some` for declared rows, hash from `services.name_of(row).unwrap().hash()`), `svc_sched: Vec<Option<SpscConsumer>>` (Task 4), `timer_stats: Arc<TimerStats>`; `const TIMERS_PER_PASS: usize = 64; const SCHED_DRAIN_PER_CYCLE: usize = 256;`; `fn drain_sched_rings(&mut self) -> bool`, `fn fire_due_timers(&mut self) -> (bool, bool)` (`(did, hold_clients)`), `fn rearm_timers(&mut self)`, `fn publish_timers_pending(&self)`
- Consumed by: Tasks 9, 10.

- [ ] **Step 1: Fields and construction.** Add the three fields to `struct Consensus` and initialise them in both constructor literals: `pass_now_ns: 0`, `timer_stats: Arc::new(TimerStats::default())` (the `Node` keeps a clone for `observability()`), and

```rust
            timers: (0..CNC_MAX_SERVICES as u8)
                .map(|row| services.name_of(row).map(|n| RowTimers::new(n.hash())))
                .collect(),
```

(`services` is the `ServicesConfig` the constructor already holds for `create_rings`; `FsmName::hash()` is `uc_protocol::identity::FsmName::hash`.)

- [ ] **Step 2: The pass.** In `do_work`, right after `self.publish_service_mins();` (line 2238) and before step 0:

```rust
        // Time-and-timers spec §3.2/§4.3 — ONE wall-clock read per pass. The
        // appender clamps every stamp to max(now, last), so this is the only
        // place the log's clock advances. Sched rings drain first so a timer
        // scheduled by the service this pass can fire this pass.
        let now_wall = wall_now_ns();
        self.pass_now_ns = now_wall;
        if let Some(app) = self.appender.as_mut() {
            app.set_now(now_wall);
        }
        did |= self.drain_sched_rings();
```

Replace steps 3 and 3b (lines 2361-2370) with:

```rust
        // 3. Fire due timers BEFORE any client frame of this pass (spec §4.3).
        let serving = self.leader_flag.load(Ordering::Relaxed) && self.sm.can_serve();
        let mut hold_clients = false;
        if serving {
            let (d, hold) = self.fire_due_timers();
            did |= d;
            hold_clients = hold;
        }
        // 3a/3b. Client frames — skipped entirely when the timer bound was hit,
        // so no client frame can land between two due timers.
        if serving && !hold_clients {
            did |= self.drain_ingress();
        }
        if !(serving && hold_clients) {
            did |= self.drain_ingress_ring(serving);
        }
```

Add, near `drain_ingress`:

```rust
const TIMERS_PER_PASS: usize = 64;
const SCHED_DRAIN_PER_CYCLE: usize = 256;

fn wall_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

impl Consensus {
    /// Every role: absorb the services' schedule/cancel/consumed records.
    fn drain_sched_rings(&mut self) -> bool {
        let mut did = false;
        let mut err: Option<(RingError, &'static str)> = None;
        for row in 0..CNC_MAX_SERVICES {
            let (Some(cons), Some(t)) = (self.svc_sched[row].as_mut(), self.timers[row].as_mut()) else {
                continue;
            };
            let mut buf = Vec::new();
            for _ in 0..SCHED_DRAIN_PER_CYCLE {
                match cons.try_read(&mut buf) {
                    Ok(Some(rec)) => {
                        did = true;
                        if rec.msg_type != MSG_V2_SCHED {
                            continue;
                        }
                        if let Some(r) = read_sched_record(&buf) {
                            match r.op {
                                SchedOp::Schedule => t.schedule(r.timer_id, r.deadline_ns),
                                SchedOp::Cancel => t.cancel(r.timer_id),
                                SchedOp::Consumed => t.consumed(r.timer_id, r.deadline_ns),
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        err = Some((e, "svc_sched"));
                        break;
                    }
                }
            }
        }
        if let Some((e, ring)) = err {
            self.ring_error_fail_stop(&e, ring);
        }
        did
    }

    /// Leader only. Returns `(did, hold_clients)`: `hold_clients` is true when
    /// the per-pass bound was hit or the buffer would overrun — either way no
    /// client frame may be appended this pass (spec §4.3).
    fn fire_due_timers(&mut self) -> (bool, bool) {
        let now = self.pass_now_ns;
        let Some(app) = self.appender.as_mut() else {
            return (false, false);
        };
        let mut did = false;
        for _ in 0..TIMERS_PER_PASS {
            // earliest due instance across all rows — global deadline order,
            // so two rows' timers never clamp each other into "late"
            let mut best: Option<(usize, u64, u64)> = None; // (row, id, deadline)
            for (row, slot) in self.timers.iter_mut().enumerate() {
                if let Some(t) = slot
                    && let Some((id, dl)) = t.peek_due(now)
                    && best.is_none_or(|(_, _, bdl)| dl < bdl)
                {
                    best = Some((row, id, dl));
                }
            }
            let Some((row, id, dl)) = best else {
                return (did, false);
            };
            let t = self.timers[row].as_mut().unwrap();
            let body = TimerBody { identity_hash: t.hash(), timer_id: id, deadline_ns: dl };
            match app.append_timer(&body, 0) {
                Ok((position, stamp)) => {
                    t.take_in_flight(id, dl);
                    did = true;
                    self.timer_stats.fired[row].fetch_add(1, Ordering::Relaxed);
                    let late = stamp > dl;
                    if late {
                        self.timer_stats.late[row].fetch_add(1, Ordering::Relaxed);
                    }
                    crate::obs_event!(
                        Info,
                        "timer_fired",
                        node = self.id as u64,
                        row = row as u64,
                        timer_id = id,
                        deadline_ns = dl,
                        time_ns = stamp,
                        position = position,
                        late = late
                    );
                }
                Err(AppendError::WouldOverrun) => return (did, true),
                Err(AppendError::PayloadTooLarge) => unreachable!("a 24-byte body never exceeds max_payload"),
            }
        }
        (did, true) // bound hit: hold the clients this pass
    }

    /// Leadership lost: every in-flight instance is pending again (spec §4.5).
    fn rearm_timers(&mut self) {
        for (row, slot) in self.timers.iter_mut().enumerate() {
            if let Some(t) = slot {
                let n = t.rearm();
                if n > 0 {
                    self.timer_stats.rearmed[row].fetch_add(n as u64, Ordering::Relaxed);
                    crate::obs_event!(Info, "timers_rearmed", node = self.id as u64, row = row as u64, count = n as u64);
                }
            }
        }
    }

    fn publish_timers_pending(&self) {
        for (row, slot) in self.timers.iter().enumerate() {
            if let Some(t) = slot {
                self.cnc.service_slot(row).identity.store_timers_pending(t.pending_len() as u64);
            }
        }
    }
}
```

(`Option::is_none_or` and let-chains are inside the pinned toolchain; if `let` chains are not enabled at the MSRV job's 1.89, use nested `if let` — the tree already uses let-chains at `node.rs:3682` and `apply.rs`, so they are fine.) `self.timers` and `self.appender` are disjoint fields; if the borrow checker objects to `app` living across the `self.timers` loop, take the appender out with `let mut app = self.appender.take().unwrap();` and put it back before returning.

- [ ] **Step 3: Re-arm on both leader-exit paths.** In `Action::BecomeFollower` (line 5016, right after `self.appender = None;`) and in `halt()` (after `self.leader_flag.store(false, ..)`, line 5306): `self.rearm_timers();`.

- [ ] **Step 4: Seed at leader open.** `on_collapsed` line 5438 (Task 3 already made it `Appender::new(.., self.cnc.log_time_ns())` + `set_now(wall_now_ns())`); change the `set_now` argument to `self.pass_now_ns`.

- [ ] **Step 5: Publish the pending word.** In `publish_status` (step 6), after `self.publish_ring_holes();` (line 3478): `self.publish_timers_pending();`.

- [ ] **Step 6: Expose the stats.** `ObsSources` gains `pub timer_stats: Arc<TimerStats>`; `Node::observability()` (1664) fills it from the clone the `Node` keeps (add a `timer_stats: Arc<TimerStats>` field to `Node` beside whatever it keeps for `truncations`/`wipes`).

- [ ] **Step 7: Run**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p uc_node --lib --test smoke --test failover --test learner --test purge_safety --test query_barrier --test admin_auth --test daemon_refusals --test services && cargo test -p uc_service`
Expected: PASS (behaviour unchanged for every existing test: no service schedules anything yet, so `fire_due_timers` returns `(false, false)` every pass).

- [ ] **Step 8: Commit**

```bash
git add uc_node/src/node.rs uc_node/src/obs/mod.rs
git commit -m "feat(uc_node): one clock read per pass; due timers fire before client frames (hold clients at the bound); sched rings drained every role; re-arm on BecomeFollower and halt; seed from log_time_ns; timers_pending published"
```

---

### Task 9: Observability — metrics, contract series, the alert rule, `uc2ctl status`

**Files:**
- Modify: `uc_node/src/obs/metrics.rs` (`CONTRACT_SERIES` 34+; the per-row block around 731-740; `ServiceRow`/`service_rows` 195-243)
- Modify: `packaging/prometheus/uc2-alerts.yml` (after `Uc2ServicePinnedAtLagBound`, line 141)
- Modify: `uc_ctl/src/main.rs` (status: the `services:` line 582 and the per-row `println!` 603-612)
- Test: `uc_node/tests/obs_http.rs` (the contract-coverage test picks the new names up from `CONTRACT_SERIES`; add one assertion), `uc_ctl` unit test for the status line if one exists (`grep -n "fn status" uc_ctl/src/main.rs`)

**Interfaces:**
- Produces metric families: `uc2_timers_pending{service,row}` (gauge, from the slot word), `uc2_timers_fired_total{service,row}`, `uc2_timers_late_total{service,row}`, `uc2_timers_rearmed_total{service,row}` (counters, from `ObsSources.timer_stats`), `uc2_log_time_ns` (gauge, the page-1 word), `uc2_log_time_lag_seconds` (gauge; leader only — rendered `0` on followers, with the help text saying so)
- Alert: `Uc2LogTimeFrozen`: `expr: uc2_log_time_lag_seconds > 5 and on(instance) uc2_node_is_leader == 1` (use the existing leader-flag series name — `grep -n '"uc2_.*leader' uc_node/src/obs/metrics.rs`), `for: 30s`, severity warning, summary "log time on the leader is {{ $value }}s behind wall time — its clock stepped backwards, or the appender is stalled".

- [ ] **Step 1: Write the failing test.** In `uc_node/tests/obs_http.rs`, beside the existing contract-coverage test, add:

```rust
#[test]
fn timer_and_log_time_families_are_in_the_contract() {
    for name in ["uc2_timers_pending", "uc2_timers_fired_total", "uc2_timers_late_total", "uc2_timers_rearmed_total", "uc2_log_time_ns", "uc2_log_time_lag_seconds"] {
        assert!(uc_node::obs::metrics::CONTRACT_SERIES.contains(&name), "{name}");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p uc_node --test obs_http timer_and_log_time`
Expected: FAIL (names absent).

- [ ] **Step 3: Implement.** `ServiceRow` gains `timers_pending: u64` (read `slot.identity.timers_pending()` in `service_rows`) and `fired/late/rearmed: u64` (read `s.timer_stats.fired[id].load(Relaxed)` etc.). After the identity-hash gauge:

```rust
    push_service_labeled(out, "uc2_timers_pending", "Pending scheduled timers for this row on this node (time-and-timers spec §6); every node holds the same set, the leader fires it.", "gauge", &rows, |r| r.timers_pending);
    push_service_labeled(out, "uc2_timers_fired_total", "TIMER frames this node appended as leader for the row.", "counter", &rows, |r| r.fired);
    push_service_labeled(out, "uc2_timers_late_total", "Fired timers whose stamp exceeded their deadline (post-failover or scheduled in the past).", "counter", &rows, |r| r.late);
    push_service_labeled(out, "uc2_timers_rearmed_total", "In-flight timers moved back to pending on a leadership loss; each may fire again (the service drops the duplicate).", "counter", &rows, |r| r.rearmed);
```

and two node-level gauges (beside `uc2_fsm_lag_bytes`):

```rust
    let log_time = s.cnc.log_time_ns();
    push_gauge(&mut out, "uc2_log_time_ns", "The highest leader stamp the archive has recorded: the log's clock, identical on every replica once caught up (time-and-timers spec §3).", log_time);
    let lag_s = if is_leader && log_time > 0 { now.saturating_sub(log_time) / 1_000_000_000 } else { 0 };
    push_gauge(&mut out, "uc2_log_time_lag_seconds", "Leader only (0 elsewhere): wall clock minus the log's clock. Grows when the leader's clock stepped backwards (stamps hold until wall time catches up) or nothing is being appended. Alert: Uc2LogTimeFrozen.", lag_s);
```

(`push_gauge` — use whatever the file's existing unlabeled-gauge helper is called; `is_leader` and `now` are already computed in the encoder for other families — check the surrounding code.) Add all six names to `CONTRACT_SERIES` in render order. The alert rule per the interface above. `uc2ctl status`: the `services:` line gains `log_time=<rfc3339 or 0>` (format with the crate's existing time formatting, or plain ns if none) and each row line gains `timers_pending={}` from `s.identity.timers_pending()`.

- [ ] **Step 4: Run**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test -p uc_node --test obs_http && cargo test -p uc_ctl && cargo run -p uc_node --release --example m10_alerts -- --check packaging/prometheus/uc2-alerts.yml` (if `m10_alerts` has a rules-lint mode — `grep -n "check\|lint" uc_node/examples/m10_alerts.rs | head`; otherwise `promtool check rules` if installed, else skip and say so in the commit).
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/obs uc_node/tests/obs_http.rs packaging/prometheus/uc2-alerts.yml uc_ctl/src/main.rs
git commit -m "feat(obs): uc2_timers_{pending,fired,late,rearmed}, uc2_log_time_ns, uc2_log_time_lag_seconds, Uc2LogTimeFrozen; uc2ctl status shows log time and pending timers (test: timer_and_log_time_families_are_in_the_contract)"
```

---

### Task 10: End-to-end — a timer schedules, fires, and is delivered; late + exactly-once across a leader change

**Files:**
- Create: `uc_node/tests/timers.rs`
- Modify: `.github/workflows/ci.yml:60-63` (add `--test timers` to the fast `uc_node` list)
- Reuses: `uc_node/tests/services.rs` helpers (`config`, `names`, `start_service`, `open_cnc`, `wait_until`, `serialize`, `tempdir` — copy them into the new file or move them to a `tests/common` module if one exists; `uc_node/tests/failover.rs:239` `spawn_cluster(n)` for the multi-node case)

**Interfaces:** consumes everything above; produces the two tests below.

- [ ] **Step 1: Write the failing tests.**

```rust
use std::time::{Duration, Instant};
use uc_service::{ApplyCtx, StateMachine, Timed, TimerEvent};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum Cmd { At { id: u64, in_ms: u64 }, Cancel { id: u64 } }
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct Fired { position: u64, id: u64, deadline_ns: u64, time_ns: u64, late: bool }

#[derive(Default)]
struct ClockSm { fired: Vec<Fired>, stamps: Vec<(u64, u64)>, last: Option<u64> }
impl StateMachine for ClockSm {
    const NAME: &'static str = "clock";
    type Command = Cmd;
    type Response = u64; // the stamp the command was applied at
    type Query = ();
    type QueryResponse = (Vec<Fired>, Vec<(u64, u64)>);
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Cmd) -> u64 {
        match cmd {
            Cmd::At { id, in_ms } => ctx.schedule(id, ctx.time_ns + in_ms * 1_000_000),
            Cmd::Cancel { id } => ctx.cancel(id),
        }
        self.stamps.push((ctx.position, ctx.time_ns));
        self.last = Some(ctx.position);
        ctx.time_ns
    }
    fn query(&self, _: ()) -> Self::QueryResponse { (self.fired.clone(), self.stamps.clone()) }
    fn last_applied(&self) -> Option<u64> { self.last }
    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: TimerEvent) {
        self.fired.push(Fired { position: ctx.position, id: ev.id, deadline_ns: ev.deadline_ns, time_ns: ctx.time_ns, late: ev.late(ctx) });
        self.stamps.push((ctx.position, ctx.time_ns));
        self.last = Some(ctx.position);
    }
}

#[test]
fn a_scheduled_timer_fires_at_its_deadline_in_order_and_once() {
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), names(&["clock"], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc = start_service_with(dir.path(), Timed::new(ClockSm::default()));
    let client = Client::connect(dir.path(), APP).unwrap();
    let t0: u64 = client.submit(&Cmd::At { id: 1, in_ms: 200 }).unwrap();
    let _: u64 = client.submit(&Cmd::At { id: 2, in_ms: 50 }).unwrap();
    let _: u64 = client.submit(&Cmd::Cancel { id: 2 }).unwrap();
    // keep the log moving so stamps around the deadline exist
    let deadline = Instant::now() + Duration::from_millis(600);
    while Instant::now() < deadline {
        let _: u64 = client.submit(&Cmd::At { id: 99, in_ms: 10_000 }).unwrap();
        std::thread::sleep(Duration::from_millis(5));
    }
    wait_until("timer 1 fired", || svc.query(()).0.iter().any(|f| f.id == 1));
    let (fired, stamps) = svc.query(());
    let f1: Vec<_> = fired.iter().filter(|f| f.id == 1).collect();
    assert_eq!(f1.len(), 1, "exactly once: {fired:?}");
    assert_eq!(f1[0].deadline_ns, t0 + 200_000_000);
    assert!(!f1[0].late, "{f1:?}");
    assert_eq!(f1[0].time_ns, f1[0].deadline_ns);
    assert!(fired.iter().all(|f| f.id != 2), "cancelled: {fired:?}");
    // §4.3: every frame before the timer is stamped <= deadline, every frame after >= it
    let (before, after): (Vec<_>, Vec<_>) = stamps.iter().partition(|(p, _)| *p < f1[0].position);
    assert!(before.iter().all(|(_, t)| *t <= f1[0].deadline_ns), "{before:?}");
    assert!(after.iter().all(|(_, t)| *t >= f1[0].deadline_ns), "{after:?}");
    assert!(stamps.windows(2).all(|w| w[0].1 <= w[1].1), "monotone: {stamps:?}");
    let cnc = open_cnc(dir.path());
    wait_until("pending word", || cnc.service_slot(0).identity.timers_pending() >= 1); // id 99s
    client.shutdown();
    svc.stop();
    node.stop();
}

#[test]
fn a_timer_in_flight_at_a_leader_change_fires_late_and_is_delivered_once() {
    // 3 nodes via failover.rs's spawn_cluster pattern, each with a Timed<ClockSm> service;
    // schedule id 5 for +300 ms on the leader, immediately kill_and_restart the leader
    // (the existing helper), then wait until every surviving service reports id 5 fired.
    // Assert on EVERY node: fired.filter(id==5).len()==1, identical `position`, and either
    // (late == false && time_ns == deadline) or (late == true && time_ns > deadline);
    // and that uc2_timers_rearmed_total on the old leader (from its /metrics or the
    // TimerStats handle) is >= 0 — the test cannot force the re-arm race, so it asserts
    // the outcome (once, same position everywhere), not the path.
}
```

Fill the second test from `uc_node/tests/failover.rs`'s cluster helpers (`spawn_cluster`, the leader lookup, the kill/restart helper — names at `failover.rs:239-320`), attaching a `Timed<ClockSm>` service to each node's instance dir with the `start_service_with` helper (a variant of `services.rs`'s `start_service` that takes a constructed SM). Both tests use `Client` from `uc_client` as `services.rs` does.

- [ ] **Step 2: Run to verify they fail** — with Task 8's `fire_due_timers` body replaced by `return (false, false)`, the first test must time out on `wait_until("timer 1 fired")`. Restore it.

- [ ] **Step 3: Run**

Run: `cargo test -p uc_node --test timers -- --test-threads=1`
Expected: PASS, both.

- [ ] **Step 4: Commit**

```bash
git add uc_node/tests/timers.rs .github/workflows/ci.yml
git commit -m "test(uc_node): timers end to end — schedule/cancel/fire in order and once on one node; late + once across a leader change (tests: a_scheduled_timer_fires_at_its_deadline_in_order_and_once, a_timer_in_flight_at_a_leader_change_fires_late_and_is_delivered_once; watched red with fire_due_timers stubbed)"
```

---

### Task 11: `TimerSm` in `uc_lincheck`; the two-FSM capstone with timer churn; the hard-crash scenario

**Files:**
- Create: `uc_lincheck/src/timer.rs`; modify `uc_lincheck/src/lib.rs` (`pub mod timer;`)
- Modify: `uc_node/tests/lin_v2.rs` (new test `two_fsm_timer_churn_under_failover`), `uc_node/tests/lincheck_v2/mod.rs` (a `spawn_timer_workers` beside `spawn_workers2` at 2029; an `FsmSet::TwoTimed` variant or a `start_cfg` path that attaches `Timed<TimerSm>` at row 1 — follow how `FsmSet::Two` attaches `Tagged<1, RegisterSm>`)
- Modify: `examples/uc_crashtest/src/bin/uc_crashtest-service.rs` (`--timer` flag → `Timed<TimerSm>`; `TimerSm::NAME` is `"timer"`), `examples/uc_crashtest/tests/common/mod.rs` (`spawn_service_timer(dir)`, and `spawn_node_with_services(dir, "register,timer", ..)`), `examples/uc_crashtest/tests/hard_crash.rs` (new `two_fsm_timer_service_sigkill`)
- Modify: `.github/workflows/nightly.yml` (`capstones` job runs the new `lin_v2` test by name if it lists tests; `crashtest` job likewise)

**Interfaces:**
- Produces: `uc_lincheck::timer::{TimerCmd, TimerResp, TimerSm}`: `TimerCmd::{Schedule { id, in_ns }, Cancel { id }}`, `TimerResp::Stamp(u64)`; `TimerSm` implements `StateMachine` (`NAME = "timer"`, `Query = ()`, `QueryResponse = Vec<FiredRec>` with `FiredRec { position, id, deadline_ns, time_ns }`) and `SnapshotStateMachine` (bincode of the whole struct, like `RegisterSm`'s M6 impl). Not persisting beyond that, like `RegisterSm`.
- The capstone's oracle: after the run, every node's `Timed<TimerSm>` query returns the **same** `Vec<FiredRec>` (replication equivalence, the `two_fsm_oracle_bites` pattern), each `(id, deadline)` appears at most once, and the §4.3 property holds over each node's own `(position, time_ns)` series (the SM records every apply's stamp too). `check_register` still adjudicates FSM 0.

- [ ] **Step 1: `TimerSm`** — write it first with its own unit test (`apply` schedules relative to `ctx.time_ns`; `on_timer` records; `last_applied` advances from both), watched red by asserting a fired record before `on_timer` is implemented.

- [ ] **Step 2: The capstone.** Copy `linearizable_under_failover_v2` (`lin_v2.rs:100-192`) into `two_fsm_timer_churn_under_failover`: `ClusterCfg { services: <the variant that puts Timed<TimerSm> at row 1>, .. }`; run `spawn_workers` (register, FSM 0) and a new `spawn_timer_workers` (FSM 1: each worker loops `Schedule { id: worker<<32 | n, in_ns: 5..200 ms }` and occasionally `Cancel` of its previous id, `submit_to(1, ..)`); the fault loop is the existing one (`kill_and_restart_leader`, `crash_and_restart_leader_service`). At the end: stop workers, wait until every node's FSM 1 `applied` equals the leader's, then query each node's `Timed<TimerSm>` and assert the oracle above; then `check_register(&entries)` as today. Budget and liveness gates as in the copied test.

- [ ] **Step 3: The hard-crash scenario.** Mirror `two_fsm_service_sigkill` (`hard_crash.rs`) with the row-1 service being `Timed<TimerSm>` (`--timer`), SIGKILL-ing that service mid-load three times, and the same oracle as step 2 on the recovered services' query output.

- [ ] **Step 4: Run**

Run: `cargo test -p uc_lincheck && cargo test -p uc_node --test lin_v2 two_fsm_timer_churn_under_failover -- --nocapture && cargo test -p uc_crashtest --features hard-crash-tests two_fsm_timer_service_sigkill -- --nocapture`
Expected: PASS. Record the seed and wall time in the commit message. Dev-box runs are smoke; the nightly runs are the record.

- [ ] **Step 5: Commit**

```bash
git add uc_lincheck uc_node/tests examples/uc_crashtest .github/workflows/nightly.yml
git commit -m "test(capstone): TimerSm; two-FSM timer churn under failover (exactly-once, replication-equivalent, §4.3 order) and the SIGKILL scenario (tests: two_fsm_timer_churn_under_failover, two_fsm_timer_service_sigkill)"
```

---

### Task 12: The leader-pass model in `uc_sim` — the §4.3 invariant, seeded

**Files:**
- Create: `uc_sim/src/timers.rs`; modify `uc_sim/src/lib.rs` (`pub mod timers;`)
- Modify: `uc_sim/tests/scenarios.rs` (one seeded test)

**Interfaces:**
- Produces: `uc_sim::timers::{PassModel, Frame, Kind}`: `PassModel::new(seed_stamp: u64)`; `set_now(now)`; `schedule(id, deadline)`; `pass(&mut self)` (fires due timers in deadline order then appends `k` client frames, `k` chosen by the caller); `leader_change(new_seed: u64)` (models the clamp seed: `last_stamp = max(last_stamp, new_seed)`); `frames(&self) -> &[Frame]` with `Frame { kind: Kind, stamp: u64, deadline: Option<u64> }`; `check(&self) -> Result<(), String>` — the §4.3 invariant and monotonicity, the same predicate Task 3's appender test uses, expressed once here as the reference.

- [ ] **Step 1: Test** in `uc_sim/tests/scenarios.rs`:

```rust
#[test]
fn leader_pass_model_keeps_timers_in_order_across_leader_changes() {
    for seed in 1..=64u64 {
        let mut rng = SmallRng::seed_from_u64(seed); // or the crate's existing seeded RNG
        let mut m = uc_sim::timers::PassModel::new(0);
        let mut now = 1_000_000u64;
        for step in 0..2_000 {
            now += rng.gen_range(0..50_000);
            if rng.gen_range(0..10) == 0 {
                // a new leader whose clock lags or leads by up to 1 s
                let skew: i64 = rng.gen_range(-1_000_000_000..1_000_000_000);
                now = (now as i64 + skew).max(0) as u64;
                m.leader_change(m.last_stamp());
            }
            m.set_now(now);
            for _ in 0..rng.gen_range(0..3) {
                m.schedule(step as u64 * 8 + rng.gen_range(0..8), now.saturating_sub(100_000) + rng.gen_range(0..400_000));
            }
            m.pass(rng.gen_range(0..4));
        }
        m.check().unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        assert!(m.frames().iter().any(|f| f.kind == uc_sim::timers::Kind::Timer), "seed {seed} fired nothing");
    }
}
```

- [ ] **Step 2: Implement** the model as the spec's §4.3 steps verbatim (read → fire due in deadline order stamped `max(deadline, last)` → clients stamped `max(now, last)`), and `check()` as: stamps non-decreasing; for every timer frame `i` with `stamp == deadline`, all `j < i` have `stamp_j <= deadline`; for every timer frame, all `j > i` have `stamp_j >= stamp_i`. Watch it red by making `pass` append clients before timers.

- [ ] **Step 3: Run** `cargo test -p uc_sim leader_pass_model` → PASS; then the regression tiers unchanged: `cargo test -p uc_sim`, `RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --release --test loom_mpsc --test loom_broadcast`, and the conformance pair from CLAUDE.md (`conform_gen` + `lake exe conform`) — record their outputs in the commit message; they must be unchanged, which is the "consensus untouched" claim made verifiable.

- [ ] **Step 4: Commit** `git commit -m "test(uc_sim): leader-pass model pins the §4.3 ordering invariant across 64 seeds and leader changes (test: leader_pass_model_keeps_timers_in_order_across_leader_changes); loom/conform re-run unchanged"`.

---

### Task 13: Fuzz targets, the nightly matrix group, `docs/VERIFICATION.md`

**Files:**
- Create: `fuzz/fuzz_targets/uc_protocol_timer_frame.rs`, `fuzz/fuzz_targets/uc_protocol_sched_record.rs`
- Modify: `fuzz/Cargo.toml` (two `[[bin]]` blocks, `test/doc/bench = false`), `fuzz/src/seeds.rs` (+ `seeds::uc_protocol_timer_frame()`, `seeds::uc_protocol_sched_record()`; extend `uc_protocol_log_frame()` with a `07-timer` seed), `fuzz/src/bin/seed_corpus.rs` (two `write_target` calls), `fuzz/README.md` (table rows), `.github/workflows/nightly.yml:283-287` (`FUZZ_GROUPS`: add both to a **new fifth leg** — the comment says four targets per leg max), `docs/VERIFICATION.md` §7 (table rows; "fifteen" → "seventeen"; the legs count in §7 Method and §10)

- [ ] **Step 1: Targets.**

```rust
// fuzz/fuzz_targets/uc_protocol_timer_frame.rs
#![no_main]
use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::frame::*;
fuzz_target!(|data: &[u8]| {
    if data.len() < HEADER_LEN { return; }
    let h = read_header(data);
    let _ = (h.client_id, h.seq, h.time_ns);
    let body = &data[HEADER_LEN..];
    if let Some(b) = read_timer_body(body) {
        let mut out = [0u8; TIMER_BODY_LEN];
        write_timer_body(&mut out, &b);
        assert_eq!(read_timer_body(&out), Some(b));
        let _ = h.time_ns > b.deadline_ns; // the lateness predicate is total
    }
});
```

```rust
// fuzz/fuzz_targets/uc_protocol_sched_record.rs
#![no_main]
use libfuzzer_sys::fuzz_target;
use uc_protocol::v2::ipc::*;
fuzz_target!(|data: &[u8]| {
    if let Some(r) = read_sched_record(data) {
        assert_eq!(read_sched_record(&write_sched_record(&r)), Some(r));
    }
});
```

- [ ] **Step 2: Wire them** per `fuzz/README.md` "Adding a target" (steps 1-5 there). Run: `(cd fuzz && cargo +nightly check) && scripts/fuzz_smoke.sh 30 --min-runs 1000 uc_protocol_timer_frame uc_protocol_sched_record`. Expected: both targets run ≥ 1000 execs in 30 s, no crash.

- [ ] **Step 3: `docs/VERIFICATION.md`** — two table rows in §7 ("`uc_protocol_timer_frame` — the TIMER body the apply loop decodes from a committed frame; guarded by length, total on any slice"; "`uc_protocol_sched_record` — the 17-byte service→node schedule record the consensus agent decodes from a shared-memory ring any local process can write"), the count and legs updated, and in §2/§3 one sentence each pointing at Task 12's model and Task 11's capstone.

- [ ] **Step 4: Commit** `git commit -m "fuzz: uc_protocol_timer_frame + uc_protocol_sched_record; fifth nightly leg; VERIFICATION.md rows"`.

---

### Task 14: Docs sweep, the explainer, the release writeup, the gate doc

**Files:**
- Create: `docs/notes/uc2-log-time-and-timers-explained.md`, `docs/benchmarks/uc2-time-and-timers-gate-<date>.md` (bars pre-committed, no run)
- Modify: `RELEASES.md` (the pending `2.11.0` section gains two feature bullets: log time; timers), `docs/releases.md` (same entry), `docs/reference/wire-protocol.md` "Log frames" (the relaid header, `TIMER` type + body, the stamp rule), `docs/reference/cnc-page.md` (`4048 log_time_ns`, slot `+488 timers_pending`), `docs/reference/limits.md` (`TIMERS_PER_PASS`, one instance per `(fsm, id)`), `docs/reference/semver-policy.md` (the `2.11.0` column: header relayout, new frame type, additive SDK surface), `docs/how-to/upgrade-a-cluster.md` (the `0.7.0` entry now also lists the header), `docs/ops/uc2-runbook.md` (cnc decode of the two words; `uc2ctl status` fields; `svc_sched.<row>.ring` in the instance-dir layout), `docs/reference/instance-directory.md` (per-row reservation), `docs/security/attack-surface.md` (the TIMER body and the sched record as decoders on untrusted bytes; the ring as a local-write surface), `docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md` (as-built erratum: the first per-FSM frame in a broadcast log, delivery by identity hash), `docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md` (§3.4: "the FSM has no clock" → points at `ctx.time_ns`; §11: "will follow as their own wire release" → "shipped in the same `2.11.0` flag day"), `docs/BACKLOG.md` (a line for plan 2, the schedule table), `CLAUDE.md` ("Standing facts": the header, `on_timer`, `Timed`, the two cnc words; "Next up")
- Modify: `QUICKSTART.md` if it shows an `apply` — add a two-line `on_timer` example only if it already shows a full SM; otherwise leave it.

- [ ] **Step 1: The explainer** — the "time is data on the tape" text from the design discussion, in seven numbered parts (leader writes the clock; FSM reads the tape; schedule is a note in state; leader places the wake-up at the right spot with the §4.3 timeline; replicas receive it by playing the tape; the two safety nets; snapshots carry the list), then "what Aeron does" (the verified facts from spec §2), then the failure-mode table from spec §7.

- [ ] **Step 2: The gate doc skeleton** — rows and bars, pre-committed before any run: (a) `m14_fleet_gate.py` rows a/b/e with `Timed<..>` services attached and no timers scheduled — bar: within the harness's same-source rebuild resolution as measured by `scripts/hop1_ab.sh` on the rig **that day** (write the number in before the run); (b) the same with one FSM scheduling 1 000 timers/s — bar: throughput within the same resolution, `uc2_timers_late_total == 0` after warm-up; (c) precision: `time_ns − deadline_ns` over ≥ 10 000 on-time fires — bar: p99 ≤ 2 × the measured consensus-pass length on the rig (measure the pass first with `uc2_*` cycle metrics or a one-off probe; write it down before the run). The run itself is user-gated.

- [ ] **Step 3: Sweep** the files above; `grep -rn "session_id\|correlation_id" docs/ README.md QUICKSTART.md` must return only historical gate docs.

- [ ] **Step 4: Commit** `git commit -m "docs: log time and timers — explainer, reference/how-to/runbook sweep, 2.11.0 release bullets, gate doc with pre-committed bars, spec errata (M14, identity)"`.

---

## Self-review (run before handing this plan over)

**Spec coverage.** §3.1 → Task 1; §3.2 → Task 3 (+ Task 8 seed/`set_now`); §3.3 → Task 5; §4.1 → recorded in the spec, no task; §4.2 → Tasks 1, 3; §4.3 → Task 8 (the pass), Task 3 (property at the appender), Task 12 (model), Task 10 (end to end); §4.4 → Task 4 + Task 5 (`write_sched`, re-announce); §4.5 → Tasks 7, 8; §4.6 → Task 6; §4.7 → Task 5; §4.8 → Task 6 (snapshot) + Task 5 (announce flag); §5 → plan 2 (only `FLAG_TIMER_TABLE`, `TimerEvent.table`, `table_last` land here); §6 → Tasks 2, 8, 9 (with Task 0's erratum on the ring-full counter); §7 → Tasks 10, 11 exercise the rows that are testable in-process; §8 → Tasks 3, 6, 7, 10, 11, 12, 13; §9 → Task 14 (+ Global Constraints for the version decision); §10 → nothing to build.

**Placeholders.** Every code step has code. Steps that say "copy X's setup" name the exact file and line of X. Task 10's second test and Task 11's steps 2-3 describe the construction against named helpers rather than reproducing 200-line harnesses; the executor reads those helpers.

**Type consistency.** `Appender::new(buffer, term, seed_stamp)` (Task 3) is what Tasks 3 step 4 and 8 step 4 call. `append_timer(&TimerBody, flags: u8) -> Result<(u64, u64), AppendError>` (Task 3) is what Task 5's test hook and Task 8's `fire_due_timers` call. `SchedRecord { op: SchedOp, timer_id, deadline_ns }` (Task 4) is what `ApplyCtx::take_sched_records` (Task 5), `write_sched` (Task 5) and `drain_sched_rings` (Task 8) use. `RawStateMachine::pending_timers` (Task 5) is what `Timed` overrides (Task 6) and the announce step reads (Task 5). `RowTimers::{schedule, cancel, consumed, peek_due, take_in_flight, rearm, pending_len}` (Task 7) are the only methods Task 8 calls. `TimerStats.{fired, late, rearmed}` (Task 7) are what Task 8 increments and Task 9 renders. `CncPage::log_time_ns/store_log_time_ns` and `ServiceIdentityLine::timers_pending/store_timers_pending` (Task 2) are what Tasks 3, 8, 9 use.
