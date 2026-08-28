# M14c — Client hot-path fix, wire 0.6.0 snapshot stream, per-service observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close M14b's measured client-hop cost, make a snapshot session carry one artifact per declared FSM (wire 0.5.0 → 0.6.0), and give operators per-service metrics, two proven alerts, a per-service `uc2ctl status` table and attach/detach transition events — so a multi-FSM cluster can be joined, watched and diagnosed per FSM.

**Architecture:** Three workstreams in a fixed order. (1) The client hot path: commit the single-ring fast path in `SlotTable::resolve`, then bisect the rate loss M14a-style with a committed dev-box A/B runner — one variant per suspect, exact binaries back to back, keep what measures. (2) The snapshot transfer plane: a session stays one `SnapSession`/`SnapIntake` but becomes a stream of artifacts — one `SNAP_BEGIN` per declared id (ascending, naming the id, its newest artifact position and length, and the sender's declared set) followed by that artifact's chunks; chunk offsets are stream-global so `SNAP_NAK` repair is unchanged; the receiver writes each artifact under `snapshots/<id>/`, refuses a `layout = 0` (0.5.0) or mismatched-declared-set BEGIN by name, and adopts the floor only when every declared id's artifact has landed; each FSM installs its own artifact and tail-replays as today. (3) Observability: labelled twins of the service families via the existing `push_labeled` (`service="<id>"`), five new families, two alert rules each proven by `m10_alert_fire.sh`, a `uc2ctl status` per-service table read off the cnc page, and `service_attached`/`service_detached` events from the node's per-cycle service scan.

**Tech Stack:** Rust 2024 (workspace edition), stable 1.96.0 pinned / MSRV 1.89; `uc_protocol` datagram codec; `uc2_net` reliable-UDP snapshot session with the seeded fault layer for tests; `uc2_node` cnc page-2 service slots; Prometheus exposition + `promtool` rule tests; `hop_bench` for the client A/B; `cargo-fuzz` on nightly for the new `SNAP_BEGIN` seed.

**Spec:** `docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md` — **§14 (2026-08-28 amendments) is binding and wins over earlier sections**; this plan implements §14.2 (client hot path), §14.3 (= §7.3 as designed, incl. the §3.4 correction), §14.4 (= §9). **Landed before this plan:** M14a (`main` 6111257) and M14b (`main` 4347bc2 — the M14b plan's execution record lists the deferred minors; the ones on the client hot path are Task 1/2's business, the rest stay deferred). **Not in this plan:** the §12 capstones (M14c′), the fleet gate and release writeup (M14d), a datagram header version field, a remote-protocol service selector.

## Deviations from the spec, for the reviewer

1. **`SNAP_BEGIN_FIXED_LEN` is 34, not §7.3's 35** — §14.3 reuses the zero pad `[4..8]` for `layout`/`service_id` and inserts only `services_declared: u64`. §14 already records this.
2. **A 0.5.0 sender's `SNAP_BEGIN` is dropped by the length check, not by the `layout` byte.** A 0.5.0 body is 26 + config bytes; `read_snap_begin_body` returns `None` below 34, and a 0.5.0 body with ≥ 8 bytes of config parses as a 0.6.0 body whose `layout` byte is the pad's zero — so the `layout == 0` refusal is the defensive second line, and the spec's "peer wire 0.5.0" refusal counter counts both paths (the receiver counts a too-short BEGIN there too). Stated so nobody expects a version handshake that does not exist (§14.3's §3.4 correction).
3. **The snapshot source returns `None` if any declared id's newest artifact file is missing on disk**, not only at floor 0. The floor is `min(snapshot_pos)`, so every declared id has *reported* an artifact at or above it, but a file can be gone (retention keeps newest-2; a crash between the slot write and the publish) — shipping a partial set would strand the joiner's other FSMs below an adopted floor, so the sender declines the session and the follower keeps NAKing until the next snapshot round. Logged once per decline.
4. **`uc2_service_attached{service}` is derived from the slot's `status` word, `uc2_service_lag_bytes{service}` is `commit − applied` saturating at 0** (a service can report `applied` past the sampled `commit` within one cycle).
5. **`service_detached` is inferred from heartbeat age crossing the same threshold the `Uc2ServiceWedged` alert uses**, not from a service-side goodbye — a `kill -9` sends none; the event says what the node can know.
6. **The client A/B numbers are dev-box smoke and set no bar.** Task 2's decision rule is relative (variant vs the previous kept tree, non-overlapping `--reps 6` ranges); the fleet gate (M14d) measures the shipped code.
7. **The receiver keeps every announced artifact in flight (`parts: Vec<SnapPart>`), not one `current`.** The sender rotates to artifact k+1 when artifact k's last chunk has been *sent* (§14.3), so under loss artifact k still has gaps when BEGIN(k+1) lands; one `Rebuilt` over the whole stream and per-artifact rename when the contiguous frontier passes its range keeps repair artifact-agnostic. (Task 5.)
8. **The sender re-sends the current artifact's BEGIN on a 20 ms cadence** until that artifact's first chunk is acknowledged by the stream advancing — a lost BEGIN would otherwise stall the session to the 30 s timeout (the receiver cannot NAK for an artifact it was never told about). A duplicate BEGIN is a no-op at the receiver. This also closes the same hole for artifact 0 on 0.5.0. (Task 4.)
9. **The snapshot source and the receiver's mask use `ring_ids()`/`ring_mask()`, not `ids()`/`declared()`**, so a `ServicesConfig::none_for_tests()` harness node offers and expects `{0}` (M14a's harness rule) — otherwise every node-only test, including the existing single-FSM learner join, would never open a session. Identical to `declared` for any real node. (Task 6.)
10. **The four M10 service aggregates and their labelled twins share one family block** (one `# HELP`/`# TYPE` per name — a second header is a Prometheus parse error), so "the aggregate" in PromQL is `{service=""}` and `sum()` over the family double counts; `Uc2ServiceWedged`, the dashboard and `m10_alert_fire.sh`'s selector are pinned to the aggregate. `uc2_service_epoch`'s aggregate stays FSM 0's epoch (M14a). (Task 7/8.)
11. **The `m10_alerts` scenarios for the two new rules skip `wait_ready`** — `/readyz` keys on the page-1 min-over-declared heartbeat, which an absent or sleeping FSM holds stale by construction; they use `await_stable_leader` instead. (Task 8.)

## Global Constraints

- MSRV **1.89**; `cargo clippy --workspace --all-targets -- -D warnings` clean after **every** task; `x.is_multiple_of(n)` rather than `x % n == 0`. **`--all-targets` does not compile feature-gated tests**: any task that adds an enum variant or changes a public signature must also build `cargo test -p uc2-crashtest --features hard-crash-tests --no-run` and fix the matches there (M14b's lesson).
- **Never write scratch or test artifacts to `/tmp`** (RAM-backed, no swap). Integration tests use `tempdir_in(env!("CARGO_TARGET_TMPDIR"))`; the A/B runner's instance dirs and every bench artifact live under `/home/claude/` (real disk).
- **Private `CARGO_TARGET_DIR`** for every measurement and for the proof stack (`~/.cache/cargo-target` is shared across worktrees); `/home/claude/cargo-target-uc2-m14a` (warm, this worktree) and `/home/claude/cargo-target-uc2-m14-main` (warm, the `main` checkout) exist. Bench binaries are copied out of the target dir and checksummed before a run.
- **Rate bars are fleet-only.** Every number this plan records is dev-box smoke; no task moves or sets a bar.
- **Wire (spec §14.3):** `SNAP_BEGIN` 0.6.0 body = `[0..4] session:u32 · [4] layout:u8 = 1 · [5] service_id:u8 · [6..8] zero · [8..16] snapshot_pos:u64 · [16..24] total_len:u64 · [24..32] services_declared:u64 · [32..34] config_len:u16 · [34..] config`; `SNAP_BEGIN_FIXED_LEN = 34`; `version::CURRENT = 0.6.0` (documentary — no receive path checks it); the 16-byte datagram header, `DATA`/`NAK`/`SNAP_CHUNK`/`SNAP_NAK`/`AppendPosition`/`TermMap`/admin bodies, the log frame, the ring framing (`ULTRNG2`) and `CNC_V2_VERSION` (3.0) are untouched. Chunk offsets are stream-global; `SNAP_NAK` semantics unchanged.
- **Session rule (spec §14.3):** one session per join; artifacts ascending by id, one per declared id; the receiver adopts the floor (`min` over received positions into the existing `incoming_snapshot_pos` cell) only when `received == services_declared`; refusals `peer wire 0.5.0` and `declared-set mismatch` are named and counted.
- **`uc2_service` is not modified** (each FSM already discovers and installs its own artifact from `snapshots/<id>/`). `uc2_consensus`, `uc2_crypto`, `uc2_remote` untouched; the remote protocol stays v1; `uc2_gateway` only if a match must grow.
- **Metrics (spec §14.4):** labels via the existing `push_labeled` with `service="<id>"`; unlabeled aggregate names keep their names and mean "slowest FSM"; every new family is in `CONTRACT_SERIES`; alert names `Uc2ServiceAbsent`, `Uc2ServicePinnedAtLagBound`, both with a `for: 30s` window and an `m10_alerts` scenario.
- Public API additions (`uc2_net` `SnapshotSet`/`SnapArtifact`, `uc2_node` counters) land in `docs/reference/semver-policy.md` in Task 10.
- Commit after every task with a conventional message; one task, one commit (a fix round may add one).

## File Structure

| File | Create/Modify | Responsibility |
|---|---|---|
| `uc2_client/src/slots.rs` | Modify | T1: single-ring fast path in `resolve` (no `received` RMW when `expected == bit`); invariants 7/8; tests. |
| `uc2_client/src/engine.rs` | Modify | T2: only the bisection variants that measure (v1 `handle_fan_in_piece` out of line, v2 `send_with_prefix`, v3 `poll` loop shape). |
| `scripts/hop1_ab.sh`, `docs/benchmarks/uc2-m14c-client-hop-2026-08-28.md` | Create | T2: the committed dev-box A/B runner and its record (smoke, never a gate). |
| `uc_protocol/src/v2/datagram.rs`, `uc_protocol/src/version.rs`, `docs/reference/wire-protocol.md` | Modify | T3: `SNAP_BEGIN` 0.6.0 body (34 B, `layout`/`service_id`/`services_declared`), `CURRENT` 0.6.0 (documentary). |
| `fuzz/src/seeds.rs`, `fuzz/README.md`, `fuzz/corpus/uc_protocol_datagram/` | Modify | T3: the `14-snap-begin-v2` seed. |
| `uc2_net/src/sender.rs` | Modify | T3 (interim literal), T4: `SnapshotSet`/`SnapArtifact`/`SnapshotSource`, the artifact stream with stream-global offsets, BEGIN resend. |
| `uc2_net/src/receiver.rs` | Modify | T3 (interim literals), T5: per-id `SnapIntake` (`parts: Vec<SnapPart>`), adopt-on-complete, the two refusal counters on `FollowerStats`, `set_snapshot_intake(root, own_declared, incoming)`. |
| `uc2_net/tests/snapshot_session.rs` | Modify | T4/T5: multi-artifact transport tests (loss on both artifacts, refusals). |
| `uc2_node/src/services.rs`, `uc2_node/src/ipc.rs`, `uc2_node/src/node.rs` | Modify | T6: `ring_mask()`, `snapshot_root()`, the per-id source closure + intake wiring, `snapshot_session_refusals()`; T9: `SERVICE_STALE_NS`, `note_service_transitions`, attach/detach events. |
| `uc2_node/tests/learner.rs` | Modify | T6: the two-FSM below-floor join. |
| `docs/how-to/upgrade-a-cluster.md`, `docs/reference/semver-policy.md` | Modify | T6: the 0.6.0 flag-day step; T10: API rows. |
| `uc2_node/src/obs/metrics.rs` | Modify | T7: labelled service families (`push_gauge_with_services`), five new families, the two refusal counters; `CONTRACT_SERIES` 65 → 72. |
| `packaging/prometheus/uc2-alerts.yml`, `uc2_node/examples/m10_alerts.rs`, `scripts/m10_alert_fire.sh`, `packaging/grafana/uc2-dashboard.json` | Modify | T8: `Uc2ServiceAbsent`, `Uc2ServicePinnedAtLagBound`, their scenarios, the adjudicator hookup, `Uc2ServiceWedged` pinned to the aggregate, dashboard rows. |
| `uc2ctl/src/main.rs`, `uc2ctl/tests/status_services.rs`, `uc2_node/src/obs/http.rs`, `uc2_node/tests/services.rs` | Modify/Create | T9: the per-service `status` table; the stale-threshold pin; the attach/detach event test. |
| `docs/how-to/monitor-a-cluster.md`, `docs/how-to/diagnose-a-node.md`, `docs/ops/uc2-runbook.md`, `docs/reference/uc2ctl.md`, `docs/VERIFICATION.md` | Modify | T10. |

---

### Task 1: The single-ring fast path — `resolve` stops touching `received`

M14b's exact-binary A/B put hop 1 at **−4.2 % resp/s** against M14a's tip with p90 2 → 3 µs (the M14b plan's post-execution addendum, `main` 4347bc2). A scratch build that skipped `received.fetch_or` for a single-ring request restored p90 to 2 µs but not the rate. This task commits the tail win, which is free and provable in unit tests; Task 2 chases the rate.

**Why it is sound.** For a request whose `expected` names exactly one ring, that one piece both opens and closes the set: nothing accumulates in `received`, and nothing reads it (`received` is written in `claim` phase 2 at `slots.rs:135` and read only inside `resolve` — verified by `grep -n received uc2_client/src/slots.rs`). The exactly-once gate is the completing owner CAS at `slots.rs:212-218`, not the `fetch_or`: a duplicate delivery for the same generation finds the slot already `FREE` (early `Miss` at `slots.rs:168-170`) or re-owned by a later generation (`seq as u32 != wire_seq`, `slots.rs:172-174`), and even a concurrent second `resolve` loses the CAS and returns `Miss`. So the `fetch_or`'s duplicate check at `slots.rs:199-201` is redundant for a single-ring request — it is a per-response atomic RMW on a shared cache line bought for nothing.

**Files:**
- Modify `uc2_client/src/slots.rs` — module-doc invariant 7 (line 20) and invariant 8 (line 22); `resolve` body (lines 164–221, specifically the `if let Some(r) = ring` block at 183–209); a new `#[cfg(test)]` accessor beside `set_next_seq_for_tests` (lines 278–281); tests (`mod tests` 289–668 — extend `second_resolve_is_a_miss_exactly_once` at 305–311, add three).
- No other file changes. `uc2_client`'s public API is untouched; `uc2_service` and `uc2_node` are not in this workstream.

**Interfaces:**

Consumes (all exist today):
```rust
// uc2_client/src/slots.rs:41-61
pub(crate) enum Resolve {
    Won { user_data: u64, fan_in: bool, first: bool },
    Partial { first: bool },
    KindMismatch,
    WrongRing,
    Miss,
}
// uc2_client/src/slots.rs:109-116
pub(crate) fn claim(&self, user_data: u64, kind: ReqKind, deadline_ns: u64, expected: u8, fan_in: bool) -> Result<u64, ClaimError>;
// uc2_client/src/slots.rs:164
pub(crate) fn resolve(&self, wire_seq: u32, expect_kind: Option<ReqKind>, ring: Option<u8>) -> Resolve;
// uc2_client/src/slots.rs:226
pub(crate) fn slot_index(&self, wire_seq: u32) -> usize;
// uc2_client/src/slots.rs:279
#[cfg(test)] pub(crate) fn set_next_seq_for_tests(&self, v: u64);
```

Produces:
```rust
// signature UNCHANGED — this is a body-only change
pub(crate) fn resolve(&self, wire_seq: u32, expect_kind: Option<ReqKind>, ring: Option<u8>) -> Resolve;
// new, test-only: the raw `received` word of the slot a wire seq maps to.
#[cfg(test)] pub(crate) fn received_for_tests(&self, wire_seq: u32) -> u8;
```

- [ ] **Step 1: Add the test-only accessor and write the failing tests.** Insert after `set_next_seq_for_tests` (`slots.rs:278-281`):

```rust
    /// The raw `received` word of the slot `wire_seq` maps to. Test-only:
    /// it is read AFTER a completion (freeing a slot does not clear the
    /// word — only `claim` phase 2 does), which is exactly how the
    /// single-ring fast path is observed.
    #[cfg(test)]
    pub(crate) fn received_for_tests(&self, wire_seq: u32) -> u8 {
        self.slots[(wire_seq as usize) & self.mask].received.load(Ordering::Relaxed)
    }
```

Append these three tests at the end of `mod tests`, after `a_single_ring_fan_in_completes_as_a_fan_in` (which ends at `slots.rs:667`):

```rust
    /// The fast path, stated as an invariant rather than as a timing claim:
    /// a request awaiting ONE ring never writes `received` at all. (The word
    /// survives the free — only `claim` phase 2 resets it — so reading it
    /// after the completion is a faithful witness.)
    #[test]
    fn a_single_ring_resolve_never_touches_received() {
        let t = SlotTable::new(8, 0);
        let seq = t.claim(0x5A, ReqKind::Submit, u64::MAX, 0b1, false).unwrap();
        assert_eq!(
            t.resolve(seq as u32, Some(ReqKind::Submit), Some(0)),
            Resolve::Won { user_data: 0x5A, fan_in: false, first: true },
            "a set of one: its only piece is also its first"
        );
        assert_eq!(t.received_for_tests(seq as u32), 0, "no fetch_or on the single-ring path");
        assert_eq!(t.inflight(), 0);
    }

    /// The control: a MULTI-ring request still accumulates, because its
    /// completion condition IS `received == expected`.
    #[test]
    fn a_multi_ring_resolve_still_accumulates_received() {
        let t = SlotTable::new(8, 0);
        let seq = t.claim(0x5B, ReqKind::Submit, u64::MAX, 0b11, true).unwrap();
        assert_eq!(t.resolve(seq as u32, Some(ReqKind::Submit), Some(1)), Resolve::Partial { first: true });
        assert_eq!(t.received_for_tests(seq as u32), 0b10, "the partial piece is recorded");
        assert_eq!(
            t.resolve(seq as u32, Some(ReqKind::Submit), Some(0)),
            Resolve::Won { user_data: 0x5B, fan_in: true, first: false }
        );
        assert_eq!(t.received_for_tests(seq as u32), 0b11);
    }

    /// The exactly-once gate for a single-ring request is the owner CAS, not
    /// the `received` bit: a duplicate delivery must still be `Miss` exactly
    /// once, and must not resurrect or double-decrement the slot.
    #[test]
    fn a_single_ring_duplicate_is_a_miss_and_the_window_stays_closed() {
        let t = SlotTable::new(8, 0);
        let seq = t.claim(0x5C, ReqKind::Submit, u64::MAX, 0b1, false).unwrap();
        assert!(matches!(t.resolve(seq as u32, None, Some(0)), Resolve::Won { .. }));
        assert_eq!(t.inflight(), 0);
        for _ in 0..3 {
            assert_eq!(t.resolve(seq as u32, None, Some(0)), Resolve::Miss, "duplicate delivery");
        }
        assert_eq!(t.inflight(), 0, "a duplicate must not double-decrement inflight");
    }
```

Also extend the existing `second_resolve_is_a_miss_exactly_once` (`slots.rs:305-311`) so it pins the window too — replace its body with:

```rust
    #[test]
    fn second_resolve_is_a_miss_exactly_once() {
        let t = SlotTable::new(8, 0);
        let seq = t.claim(1, ReqKind::Submit, u64::MAX, 0b1, false).unwrap();
        assert!(matches!(t.resolve(seq as u32, None, Some(0)), Resolve::Won { .. }));
        assert_eq!(t.resolve(seq as u32, None, Some(0)), Resolve::Miss, "duplicate must not double-complete");
        // The single-ring fast path removes the `received`-bit duplicate
        // check; the owner CAS is what makes this a Miss.
        assert_eq!(t.inflight(), 0);
    }
```

- [ ] **Step 2: Run the new tests and state the failure.**

```bash
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo test -p uc2_client --lib slots::
```

Expect `a_single_ring_resolve_never_touches_received` to FAIL with
`assertion `left == right` failed: no fetch_or on the single-ring path / left: 1 / right: 0`
(today's `resolve` runs `slot.received.fetch_or(bit, …)` at `slots.rs:198` for every ring delivery). The other three tests pass already — they are the regression fence, and stating that up front is the point: the fast path must not move them.

- [ ] **Step 3: Implement the fast path.** Replace `resolve` (`slots.rs:164-221`) in full:

```rust
    pub(crate) fn resolve(&self, wire_seq: u32, expect_kind: Option<ReqKind>, ring: Option<u8>) -> Resolve {
        let slot = &self.slots[(wire_seq as usize) & self.mask];
        let owner = slot.owner.load(Ordering::Acquire);
        if owner == FREE || owner == RESERVED {
            return Resolve::Miss;
        }
        let seq = owner - 1;
        if seq as u32 != wire_seq {
            return Resolve::Miss; // stale generation
        }
        if let Some(expect) = expect_kind
            && slot.kind.load(Ordering::Relaxed) != expect as u8
        {
            return Resolve::KindMismatch; // leave the slot for the real answer
        }
        let expected = slot.expected.load(Ordering::Relaxed);
        // False for a ring-less terminal: no piece is pushed, so the flag is
        // irrelevant there (see `Resolve::Won`'s doc).
        let mut first = false;
        if let Some(r) = ring {
            debug_assert!(r < 8);
            let bit = 1u8 << r;
            if expected & bit == 0 {
                return Resolve::WrongRing; // not ours to answer; slot untouched
            }
            if expected == bit {
                // SINGLE-RING FAST PATH (the common request: `try_submit`,
                // `try_submit_to`, every query). The set has one member, so
                // this piece both opens and closes it: `received` would only
                // ever hold `bit`, and nothing reads it — so skip the atomic
                // RMW entirely. Exactly-once is unaffected: the gate is the
                // completing `compare_exchange(owner, FREE)` below, and a
                // duplicate delivery for this generation is caught above
                // (slot already FREE, or re-owned by a later generation —
                // invariant 4), or loses that CAS. Measured: p90 3 µs → 2 µs
                // (M14b addendum), the whole tail cost of a response.
                first = true;
            } else {
                // MULTI-RING (fan-in) only. `received` is mutated only here,
                // and never races a concurrent re-claim of this word:
                // `release` — the only cross-thread freer of a slot — fires
                // only when this generation's ring write FAILED (engine.rs's
                // `finish_write`), meaning no response frame for it was ever
                // sent; a `resolve` call in flight for this generation is
                // proof a frame DID arrive, so no concurrent
                // `release`/re-claim of THIS generation can be racing it
                // (module doc invariant 7). So a plain fetch_or is exact: a
                // repeated bit is a duplicate delivery on that ring.
                let prev = slot.received.fetch_or(bit, Ordering::AcqRel);
                if prev & bit != 0 {
                    return Resolve::Miss;
                }
                // The slot table — not a seq comparison — decides where a
                // generation starts: `prev == 0` means nothing has answered
                // this claim yet, so this piece opens a fresh set.
                first = prev == 0;
                if (prev | bit) != expected {
                    return Resolve::Partial { first }; // more rings still to answer
                }
            }
        }
        let user_data = slot.user_data.load(Ordering::Relaxed);
        let fan_in = slot.fan_in.load(Ordering::Relaxed);
        if slot
            .owner
            .compare_exchange(owner, FREE, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Resolve::Miss; // lost the race to sweep/another delivery
        }
        self.inflight.fetch_sub(1, Ordering::AcqRel);
        Resolve::Won { user_data, fan_in, first }
    }
```

- [ ] **Step 4: Run the whole `uc2_client` suite and state the pass.**

```bash
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo test -p uc2_client
```

Expect all green, including the four tests above, the 40 000-claim `concurrent_exactly_once_stress` (`slots.rs:406-563` — it resolves single-ring claims from four threads while a sweeper races, so it exercises exactly the case where the CAS, not the bit, has to be the gate), and `uc2_client/tests/engine_synthetic.rs`. Three assertions there are the ones worth naming because they read the counters the fast path could have moved, and must not change:
`engine_synthetic.rs:246` (`duplicates == 1` after a single-ring duplicate — now reached via `Resolve::Miss` from the freed-slot check instead of the repeated bit, same arm at `engine.rs:804-806`), `engine_synthetic.rs:484` (`duplicates == 1` after a late fan-in piece) and `engine_synthetic.rs:498` (`wrong_ring == 1` — the `expected & bit == 0` check still runs BEFORE the fast path).

- [ ] **Step 5: Rewrite module-doc invariants 7 and 8.** Replace `slots.rs:20` (invariant 7) with:

```
//! 7. `expected` (the ring bitmask a request awaits), `received` (the bitmask of rings that have answered so far) and `fan_in` (whether this request was issued as a fan-in, stored as a separate `AtomicBool`) are written in claim phase 2, under the RESERVED word — invisible to readers until the phase-3 publish. Thereafter `received` is touched by MULTI-RING (fan-in) requests only: `resolve` skips the `fetch_or` when `expected == bit` (a single-ring request — the overwhelmingly common one), because a set of one is opened and closed by its only piece, so the word would only ever hold that bit and nothing reads it. Exactly-once does not rest on `received` in either case: the gate is the single owner CAS (invariant 3), and a duplicate delivery for a completed single-ring generation is caught by the FREE/stale-seq checks at the top of `resolve` or loses that CAS. For the multi-ring case the `fetch_or` never races a concurrent re-claim of the same word: `release` is the ONLY cross-thread freer of a slot (it runs on the send thread, inside `engine.rs`'s `finish_write` — `resolve`/`sweep`/`drain_abort` are all poll-thread-only), and `release(seq)` is called only when that seq's ring write FAILED, meaning no response frame for that generation was ever transmitted and so no `resolve` call for it can be in flight to race the free. A `resolve` that is in flight therefore always targets a generation no concurrent `release` can be freeing, so the matching claim-time reset of `received` (phase 2, under RESERVED) and `resolve`'s `fetch_or` on it are always the same generation, never back-to-back generations racing across the free. This, plus invariant 4 (a stale wire_seq collision needs a 2^32-outstanding gap), also covers reading `expected`/`fan_in` from a newer generation: that read is confined to the same impossible window, and even there the completing `compare_exchange(owner, FREE)` would fail (return `Miss`), so a stale read is never acted on. `resolve` completes a slot (the single owner CAS) only when the last expected bit lands in `received` or a ring-less (`ring: None`) terminal answer arrives, whichever comes first.
```

Replace `slots.rs:22` (invariant 8) with:

```
//! 8. `resolve` reports `first` — whether the delivery it just recorded was the FIRST ring piece of this generation. For a multi-ring request that is `received == 0` before the `fetch_or`; for a single-ring request it is `true` by construction (a set of one has nothing preceding its only piece), decided without reading `received` at all. This exists because invariant 4's outstanding-gap argument protects the SLOT (freed and re-claimed under a fresh `expected`/`received`), not the engine-side fan-in piece buffer indexed by the same slot index: a partial fan-in ended by a ring-less terminal or the deadline sweep leaves pieces behind, and the generation 2^32 requests later at that index carries the SAME u32 wire seq — so a seq comparison cannot tell the two apart, while `first` (decided here, by the slot table) always can. The buffer resets on `first`, never on a seq change.
```

- [ ] **Step 6: Clippy and commit.**

```bash
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo clippy --workspace --all-targets -- -D warnings
git add uc2_client/src/slots.rs
git commit -m "perf(client): single-ring fast path in resolve — the owner CAS is the exactly-once gate, so skip received.fetch_or when expected == bit (p90 3 -> 2 us; rate unchanged, see M14c task 2)"
```

Expect clippy clean. No new enum variant, no match site anywhere else, so the `--all-targets` blind spot for feature-gated tests (`examples/uc2-crashtest/tests/*.rs` under `hard-crash-tests`) does not apply here. **No bench in this task** — Task 2 measures the fast path together with the hot-body variants, on one harness, in one sitting.

---

### Task 2: The hot-body bisection — a committed A/B runner, three variants, one bench doc

The rate loss survives the fast path, so it is the grown hot body — M14a's codegen lesson (`docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md`, "The fix, measured": an inline wait ladder cost 9 % at N=1 on a path N=1 never executes; out of line it cost 1.5 %). Method: one variant per suspect, exact binaries A/B'd back to back on an idle box, keep what measures.

> **Smoke, never a gate.** Every number this task produces is a dev-box A/B ratio. Rate bars are fleet-only (`CLAUDE.md`, "Benchmarking discipline"; `docs/notes/dev-box-not-a-bench.md`) and M14d owns the fleet gate. **This task sets no bar** and must not be cited as one. The same dip measured 7× on this box spanned 0–18 % against a 10 % bar — which is why the decision rule below wants non-overlapping ranges over 6 reps, not a single pair.

**Files:**
- Create `scripts/hop1_ab.sh` (new, executable) — the committed A/B runner.
- Create `docs/benchmarks/uc2-m14c-client-hop-2026-08-28.md` (new) — the record, shaped like the M14a apply-hop doc.
- Modify `uc2_client/src/engine.rs` — v1: the `MSG_V2_RESPONSE` arm of `handle_record` (lines 743–809) plus a new out-of-line function after `handle_record` (which ends at line 862); v2: `SendHalf::send`'s prefix arm (lines 443–453) plus a new method in `impl SendHalf` (starts line 407); v3: `PollHalf::poll`'s ring loop (lines 668–670). **Only the variants that measure survive to the commit.**
- Not modified: `uc2_service`, `uc2_node`, `uc_protocol`, `uc2_gateway` (the harness itself is untouched — see the note in Step 2 on why `hop_bench` differs by four lines between the two trees and why that is not a confound).

**Interfaces:**

Consumes (verified, all exist):
```rust
// uc2_client/src/engine.rs:730-736
fn handle_record(shared: &Shared, fanin: &mut [FanIn], ring_id: Option<u8>, rec: &RecordHeader, buf: &[u8], cb: &mut impl FnMut(Completion<'_>)) -> usize;
// uc2_client/src/engine.rs:409-419  (already carries #[allow(clippy::too_many_arguments)] at :408)
fn send(&self, ring: &MpscProducer, msg_type: u16, flags: u16, kind: ReqKind, user_data: u64, bytes: &[u8], expected: u8, fan_in: bool, prefix: Option<u8>) -> Result<(), SubmitError>;
// uc2_client/src/engine.rs:660
pub fn poll(&mut self, mut cb: impl FnMut(Completion<'_>)) -> usize;
// uc2_client/src/engine.rs:280
fn push_piece(&mut self, first: bool, position: u64, ring: u8, body: &[u8]);
// uc2_client/src/engine.rs:702-709
fn drain_ring(ring: &mut BroadcastConsumer, ring_id: Option<u8>, shared: &Shared, buf: &mut Vec<u8>, fanin: &mut [FanIn], cb: &mut impl FnMut(Completion<'_>)) -> usize;
// uc2_client/src/engine.rs:241  — the field v3 reshapes access to
egress_services: Vec<(u8, BroadcastConsumer)>,
// uc_protocol/src/ring/mpsc.rs:234
pub fn try_write(&self, msg_type: u16, flags: u16, header_extra: [u8; 8], payload: &[u8]) -> Result<(), RingError>;
// uc_protocol/src/v2/ipc.rs:71 / :95
pub fn extra_client(client_id: u32, local_seq: u32) -> [u8; 8];
pub fn write_query_payload(service_id: u8, query: &[u8], out: &mut Vec<u8>);
```

Produces:
```rust
// engine.rs, v1 — free function placed after `handle_record`
#[allow(clippy::too_many_arguments)]
#[inline(never)]
fn handle_fan_in_piece(shared: &Shared, fanin: &mut [FanIn], wire_seq: u32, ring: u8, position: u64, body: &[u8], resolved: Resolve, cb: &mut impl FnMut(Completion<'_>)) -> usize;
// engine.rs, v2 — method in `impl SendHalf`
#[inline(never)]
fn send_with_prefix(&self, ring: &MpscProducer, msg_type: u16, flags: u16, extra: [u8; 8], id: u8, bytes: &[u8]) -> Result<(), RingError>;
```
plus `scripts/hop1_ab.sh` (CLI: `--sink BIN --a BIN --b BIN [--reps N] [--secs S] [--root DIR]`).

- [ ] **Step 1: Write and commit-stage the A/B runner.** Create `scripts/hop1_ab.sh` with exactly this content, then `chmod +x scripts/hop1_ab.sh`:

```bash
#!/usr/bin/env bash
# UC v2 — hop-1 A/B: two `hop_bench` DRIVER binaries against ONE fixed sink.
#
# SMOKE, NEVER A GATE. Rate bars are fleet-only (CLAUDE.md "Benchmarking
# discipline"; docs/notes/dev-box-not-a-bench.md). This script produces a
# RATIO between two binaries measured back to back on an idle box; it never
# produces a number to compare against a bar, and a red run here is not a
# regression until the fleet says so.
#
# The measured hop is client Engine -> ingress ring -> node -> egress
# broadcast -> Engine, with `dummy-node` standing in for the node (an
# infinitely fast backend). Only the DRIVER differs between the two sides:
# --sink is one fixed binary used for every single run, so a sink-side
# codegen difference can never leak into the delta.
#
# BUILD DISCIPLINE (required, see CLAUDE.md "Benchmarking discipline"):
# ~/.cache/cargo-target is shared by the main checkout and every worktree, so
# another checkout's build silently replaces your binaries mid-measurement.
# Build every side with its own private CARGO_TARGET_DIR, COPY the binary out
# to a stable path, and record its sha256 — then pass the copies here. Never
# point --a/--b at a live target dir.
#
# Usage:
#   scripts/hop1_ab.sh --sink BIN --a BIN --b BIN [--reps N] [--secs S] [--root DIR]
#
#   --sink  hop_bench binary used for `dummy-node` (fixed for every run)
#   --a     hop_bench binary used as driver A (the baseline)
#   --b     hop_bench binary used as driver B (the candidate)
#   --reps  A/B pairs to run (default 6). Even reps run A then B, odd reps
#           run B then A, so a warm-up or thermal drift cannot favour a side.
#   --secs  seconds per run (default 6)
#   --root  scratch dir for the instance dir and logs (default $HOME/m14c-ab).
#           MUST be on real disk — never /tmp (RAM-backed, no swap).
set -euo pipefail

SINK=""; DRIVER_A=""; DRIVER_B=""; REPS=6; SECS=6; ROOT="$HOME/m14c-ab"
PAYLOAD=64; INFLIGHT=4096; ENGINES=1; APP_ID="hop-ab"

while [ $# -gt 0 ]; do
    case "$1" in
        --sink) SINK="$2"; shift 2 ;;
        --a) DRIVER_A="$2"; shift 2 ;;
        --b) DRIVER_B="$2"; shift 2 ;;
        --reps) REPS="$2"; shift 2 ;;
        --secs) SECS="$2"; shift 2 ;;
        --root) ROOT="$2"; shift 2 ;;
        -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done
for v in SINK DRIVER_A DRIVER_B; do
    [ -n "${!v}" ] || { echo "--${v,,} is required (see --help)" >&2; exit 2; }
done
for b in "$SINK" "$DRIVER_A" "$DRIVER_B"; do
    [ -x "$b" ] || { echo "not an executable: $b" >&2; exit 2; }
done
case "$ROOT" in /tmp/*|/tmp) echo "--root must not be under /tmp (RAM-backed, no swap)" >&2; exit 2 ;; esac

mkdir -p "$ROOT"
TSV="$ROOT/runs.tsv"
: > "$TSV"

echo "== hop-1 A/B (SMOKE, not a gate) — reps=$REPS secs=$SECS payload=${PAYLOAD}B inflight=$INFLIGHT engines=$ENGINES"
echo "   sink   $(sha256sum "$SINK" | cut -c1-12)  $SINK"
echo "   A      $(sha256sum "$DRIVER_A" | cut -c1-12)  $DRIVER_A"
echo "   B      $(sha256sum "$DRIVER_B" | cut -c1-12)  $DRIVER_B"

run_one() { # $1 = side label (A|B), $2 = driver binary, $3 = rep number
    local side="$1" bin="$2" rep="$3" sink_pid waited=0 out
    rm -rf "$ROOT/instance"
    mkdir -p "$ROOT/instance"
    "$SINK" dummy-node --instance-dir "$ROOT/instance" --app-id "$APP_ID" \
        >"$ROOT/sink.log" 2>&1 &
    sink_pid=$!
    until grep -q '^READY$' "$ROOT/sink.log" 2>/dev/null; do
        sleep 0.1
        waited=$((waited + 1))
        if [ "$waited" -gt 300 ]; then
            kill "$sink_pid" 2>/dev/null || true
            echo "sink never printed READY (30 s); see $ROOT/sink.log" >&2
            exit 1
        fi
        kill -0 "$sink_pid" 2>/dev/null || { echo "sink died; see $ROOT/sink.log" >&2; exit 1; }
    done
    out="$("$bin" engine-load --instance-dir "$ROOT/instance" --app-id "$APP_ID" \
        --secs "$SECS" --payload "$PAYLOAD" --inflight "$INFLIGHT" --engines "$ENGINES")"
    kill "$sink_pid" 2>/dev/null || true
    wait "$sink_pid" 2>/dev/null || true
    printf '%s\n' "$out" | sed -n 's/^RESULT //p' | python3 -c '
import json, sys
rep, side, tsv = sys.argv[1], sys.argv[2], sys.argv[3]
d = json.loads(sys.stdin.readline())
print("rep %2s  %s  %12.0f resp/s   p50 %.3f ms  p90 %.3f ms  p99 %.3f ms  lost %d"
      % (rep, side, d["responses_per_sec"], d["p50_ms"], d["p90_ms"], d["p99_ms"], d["lost"]))
open(tsv, "a").write("%s\t%f\t%f\n" % (side, d["responses_per_sec"], d["p90_ms"]))
' "$rep" "$side" "$TSV"
}

for rep in $(seq 1 "$REPS"); do
    if [ $((rep % 2)) -eq 1 ]; then
        run_one A "$DRIVER_A" "$rep"
        run_one B "$DRIVER_B" "$rep"
    else
        run_one B "$DRIVER_B" "$rep"   # reversed order: drift cannot favour a side
        run_one A "$DRIVER_A" "$rep"
    fi
done

python3 - "$TSV" <<'PY'
import sys
rows = {"A": [], "B": []}
for line in open(sys.argv[1]):
    side, rate, p90 = line.split("\t")
    rows[side].append((float(rate), float(p90)))
print("\n== summary (SMOKE — a ratio, not a gate)")
stat = {}
for side in ("A", "B"):
    r = sorted(x[0] for x in rows[side])
    p = sorted(x[1] for x in rows[side])
    stat[side] = (sum(r) / len(r), r[0], r[-1])
    print("   %s  n=%d  mean %.0f  min %.0f  max %.0f resp/s   p90 median %.3f ms"
          % (side, len(r), stat[side][0], stat[side][1], stat[side][2], p[len(p) // 2]))
delta = (stat["B"][0] - stat["A"][0]) / stat["A"][0] * 100.0
overlap = not (stat["B"][1] > stat["A"][2] or stat["A"][1] > stat["B"][2])
print("   B vs A: %+.2f %%   ranges %s" % (delta, "OVERLAP" if overlap else "disjoint"))
print("   (dev-box smoke; keep a variant only on a disjoint, repeatable delta)")
PY
```

- [ ] **Step 2: Build the three fixed binaries and record their checksums.** Three trees, three private target dirs, three copies. Nothing moves any `HEAD` and no worktree is added or removed.

```bash
mkdir -p /home/claude/m14c-ab/bin

# (1) main 4347bc2 — the M14b tip. Supplies BOTH the fixed sink and driver A.
#     The main checkout is already on main; do not touch its HEAD.
cd /home/claude/ultima/ultima_cluster
git rev-parse --short HEAD                     # expect 4347bc2
CARGO_TARGET_DIR=/home/claude/cargo-target-m14c-main \
  cargo build --release -p uc2_gateway --example hop_bench
cp /home/claude/cargo-target-m14c-main/release/examples/hop_bench /home/claude/m14c-ab/bin/hb-main

# (2) main 3a7f9a5 — M14a's tip, the recovery target. Extracted read-only with
#     `git archive` (no worktree, no HEAD move, no checkout).
mkdir -p /home/claude/m14c-a0-tree
git -C /home/claude/ultima/ultima_cluster archive 3a7f9a5 | tar -x -C /home/claude/m14c-a0-tree
cd /home/claude/m14c-a0-tree
CARGO_TARGET_DIR=/home/claude/cargo-target-m14c-a0 \
  cargo build --release -p uc2_gateway --example hop_bench
cp /home/claude/cargo-target-m14c-a0/release/examples/hop_bench /home/claude/m14c-ab/bin/hb-m14a

# (3) the branch tree AFTER Task 1 — driver B for the first pair.
cd /home/claude/ultima/ultima_cluster/.claude/worktrees/uc2-multi-service
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a \
  cargo build --release -p uc2_gateway --example hop_bench
cp /home/claude/cargo-target-uc2-m14a/release/examples/hop_bench /home/claude/m14c-ab/bin/hb-t1

sha256sum /home/claude/m14c-ab/bin/*            # paste into the bench doc
```

Two facts to state in the doc rather than discover later. (a) `uc2_gateway/examples/hop_bench/engine_load.rs` differs by exactly four lines between 3a7f9a5 and 4347bc2 (`git diff 3a7f9a5 HEAD -- uc2_gateway/examples/hop_bench/engine_load.rs`): the mandatory `Outcome::Responses(_)` and `Outcome::BadService { .. }` arms added to an exhaustive match on a path a bench never takes. The harness is otherwise byte-identical, so `hb-m14a` vs `hb-main` is a `uc2_client` comparison. (b) `hb-main` is the sink for **every** run in this task, including runs of `hb-m14a` — that is what makes the sides comparable.

- [ ] **Step 3: The two reference pairs.** Establish where Task 1 landed and re-measure M14a's tip in this session, so the residual in Step 7 is an in-session number rather than a cross-session quote.

```bash
cd /home/claude/ultima/ultima_cluster/.claude/worktrees/uc2-multi-service
# R0: how far below M14a's tip the current tip sits, on this box, today.
scripts/hop1_ab.sh --sink /home/claude/m14c-ab/bin/hb-main \
  --a /home/claude/m14c-ab/bin/hb-m14a --b /home/claude/m14c-ab/bin/hb-main \
  --reps 6 --root /home/claude/m14c-ab | tee /home/claude/m14c-ab/r0-m14a-vs-main.log
# R1: Task 1 (the fast path) vs main.
scripts/hop1_ab.sh --sink /home/claude/m14c-ab/bin/hb-main \
  --a /home/claude/m14c-ab/bin/hb-main --b /home/claude/m14c-ab/bin/hb-t1 \
  --reps 6 --root /home/claude/m14c-ab | tee /home/claude/m14c-ab/r1-main-vs-t1.log
```

Expect R0 to reproduce the M14b addendum's shape (`hb-main` a few percent below `hb-m14a`; the addendum's 17 pairs read ≈ 5.93 M vs ≈ 5.68 M, −4.2 %). Expect R1 to show p90 back at 2 µs on side B with the rate within noise of A — that is precisely the M14b scratch-build result, now committed. **If R0 does not reproduce a negative delta**, stop and record that: the premise of Steps 4–6 is that the loss is measurable on this box today, and chasing an unmeasurable delta with codegen variants is how plausible-but-wrong stories get shipped (CLAUDE.md, "Measurement refutes plausible-but-wrong stories").

Box hygiene for every run in Steps 3–6: nothing else on the box (no `cargo build` in another checkout, no test suite, no other agent session), and the same conditions for every rep.

- [ ] **Step 4: Variant v1 — the fan-in arms out of line.** The suspect: `handle_record`'s `MSG_V2_RESPONSE` branch is 67 lines (`engine.rs:743-809`) and every one of them is in the poll loop's body, while a single-ring response only ever executes the first `Resolve::Won` arm. Replace `engine.rs:759-808` (the `match shared.table.resolve(...)` expression) with:

```rust
            match shared.table.resolve(wire_seq, Some(delivered), Some(ring)) {
                // THE HOT BODY: one ring, one response, nothing buffered.
                // Everything else lives in `handle_fan_in_piece`, out of
                // line, so it costs nothing here (M14a's codegen lesson:
                // code in a hot loop's body costs even on paths that never
                // run — docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md).
                Resolve::Won { user_data, fan_in: false, .. } => {
                    shared.stats.responses.fetch_add(1, Ordering::Relaxed);
                    cb(Completion {
                        user_data,
                        position: Some(position),
                        outcome: Outcome::Response(&buf[8..]),
                    });
                    1
                }
                other => handle_fan_in_piece(
                    shared,
                    fanin,
                    wire_seq,
                    ring,
                    position,
                    &buf[8..],
                    other,
                    &mut *cb,
                ),
            }
```

and insert this after `handle_record` ends (today at `engine.rs:862`, before `fn maintenance` at `:866`):

```rust
/// The cold arms of `handle_record`'s `MSG_V2_RESPONSE` branch — everything a
/// single-ring response never reaches: a fan-in piece (`Partial`, or the
/// closing `Won { fan_in: true }`), a response from a ring the request did
/// not expect, a stale cross-generation collision, a duplicate.
///
/// `#[inline(never)]` on purpose. The caller is the poll loop's body and the
/// single-ring `Won` arm is the only path a normal client takes; M14a
/// measured a wait ladder costing 9 % at N=1 through codegen alone, on an arm
/// N=1 never executed (`docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md`).
/// Keep the hot body small. Returns 1 if a completion was emitted, else 0.
#[allow(clippy::too_many_arguments)]
#[inline(never)]
fn handle_fan_in_piece(
    shared: &Shared,
    fanin: &mut [FanIn],
    wire_seq: u32,
    ring: u8,
    position: u64,
    body: &[u8],
    resolved: Resolve,
    cb: &mut impl FnMut(Completion<'_>),
) -> usize {
    match resolved {
        Resolve::Won { user_data, fan_in: true, first } => {
            // The last piece: buffer it beside the earlier ones, emit the
            // whole set ordered by id, then drop the pieces (the `Bytes`
            // refcounts travelled to the caller by value). `first` is true
            // when this piece is also the generation's only one (a
            // single-declared-FSM `try_submit_all`).
            let f = &mut fanin[shared.table.slot_index(wire_seq)];
            f.push_piece(first, position, ring, body);
            f.parts.sort_by_key(|p| p.0);
            shared.stats.responses.fetch_add(1, Ordering::Relaxed);
            cb(Completion {
                user_data,
                position: Some(f.position),
                outcome: Outcome::Responses(&f.parts),
            });
            f.parts.clear();
            1
        }
        Resolve::Partial { first } => {
            fanin[shared.table.slot_index(wire_seq)].push_piece(first, position, ring, body);
            0
        }
        Resolve::WrongRing => {
            // A sibling FSM answering a request that named another ring (or a
            // fan-in generation that never expected it).
            shared.stats.wrong_ring.fetch_add(1, Ordering::Relaxed);
            0
        }
        Resolve::KindMismatch => {
            // T14: stale cross-generation collision — drop, count, leave the
            // slot for the real answer.
            shared.stats.kind_mismatch.fetch_add(1, Ordering::Relaxed);
            0
        }
        Resolve::Miss => {
            shared.stats.duplicates.fetch_add(1, Ordering::Relaxed);
            0
        }
        // The caller handles the single-ring terminal inline and never routes
        // it here; answered identically rather than by `unreachable!` so a
        // future caller cannot turn a refactor into a panic on the hot path.
        Resolve::Won { user_data, fan_in: false, .. } => {
            shared.stats.responses.fetch_add(1, Ordering::Relaxed);
            cb(Completion { user_data, position: Some(position), outcome: Outcome::Response(body) });
            1
        }
    }
}
```

Build, copy, measure both axes:

```bash
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo build --release -p uc2_gateway --example hop_bench
cp /home/claude/cargo-target-uc2-m14a/release/examples/hop_bench /home/claude/m14c-ab/bin/hb-v1
scripts/hop1_ab.sh --sink /home/claude/m14c-ab/bin/hb-main \
  --a /home/claude/m14c-ab/bin/hb-t1 --b /home/claude/m14c-ab/bin/hb-v1 \
  --reps 6 --root /home/claude/m14c-ab | tee /home/claude/m14c-ab/r2-t1-vs-v1.log   # the DECISION
scripts/hop1_ab.sh --sink /home/claude/m14c-ab/bin/hb-main \
  --a /home/claude/m14c-ab/bin/hb-main --b /home/claude/m14c-ab/bin/hb-v1 \
  --reps 6 --root /home/claude/m14c-ab | tee /home/claude/m14c-ab/r2b-main-vs-v1.log # the doc row
```

Apply the decision rule (Step 7) now: if v1 is not kept, `git checkout -- uc2_client/src/engine.rs` before Step 5, and the "previous kept tree" binary for Step 5 stays `hb-t1`.

- [ ] **Step 5: Variant v2 — `send`'s prefix path out of line.** The suspect: `send` is one function for submits and queries, and the query arm (`engine.rs:445-452`) carries a `RefCell` borrow, `write_query_payload` and a second `try_write` call site inside the body a submit runs. Replace `engine.rs:443-453` with:

```rust
        let write_result = match prefix {
            // HOT: a submit writes the caller's slice straight through.
            None => ring.try_write(msg_type, flags, extra, bytes),
            // COLD (queries only): assemble `id ++ bytes` first, out of line
            // so the scratch borrow and the assembly never appear in `send`'s
            // body (M14a's codegen lesson).
            Some(id) => self.send_with_prefix(ring, msg_type, flags, extra, id, bytes),
        };
```

and add this method to `impl SendHalf`, immediately after `send` (which ends at `engine.rs:455`) and before `expect_one` (`:458`):

```rust
    /// `send`'s prefixed (query) write, kept out of the hot body: one
    /// `try_write` takes one slice, so assemble `service_id ++ bytes` in this
    /// half's scratch first. `SendHalf` is `!Sync`, so the `RefCell` is never
    /// contended.
    #[inline(never)]
    fn send_with_prefix(
        &self,
        ring: &MpscProducer,
        msg_type: u16,
        flags: u16,
        extra: [u8; 8],
        id: u8,
        bytes: &[u8],
    ) -> Result<(), RingError> {
        let mut scratch = self.scratch.borrow_mut();
        write_query_payload(id, bytes, &mut scratch);
        ring.try_write(msg_type, flags, extra, &scratch)
    }
```

Build, copy to `hb-v2`, and run the same two A/Bs — `--a` for the decision run is the previous **kept** binary (`hb-v1` if v1 was kept, else `hb-t1`), `--a` for the doc run is always `hb-main`. Logs `r3-prev-vs-v2.log` / `r3b-main-vs-v2.log`.

**Revert discipline for Steps 4–6.** `git checkout -- uc2_client/src/engine.rs` would also undo a kept earlier variant, so do not use it. Instead, keep the accepted state in a file outside the repo and restore from that: after each adjudication that KEEPS a variant, run `cp uc2_client/src/engine.rs /home/claude/m14c-ab/engine-kept.rs`; to reject a variant, run `cp /home/claude/m14c-ab/engine-kept.rs uc2_client/src/engine.rs` (seed it once from the Task-1 tree before Step 4: `cp uc2_client/src/engine.rs /home/claude/m14c-ab/engine-kept.rs`). Confirm each revert with `git diff --stat uc2_client/src/engine.rs`.

- [ ] **Step 6: Variant v3 — `poll`'s ring loop.** The suspect: `poll` walks a heap `Vec<(u8, BroadcastConsumer)>` (`engine.rs:241`, loop at `:668-670`) on every duty cycle, and the overwhelmingly common attach — every pre-M14b client, the gateway, and this bench — has exactly one entry. Chosen shape: **keep the `Vec`, hoist the one-element case with a slice pattern** (not a fixed `[Option<…>; 8]` array, which would enlarge `PollHalf` by seven idle `BroadcastConsumer`s and change `wait_handle`'s indexing at `:692`). Replace `engine.rs:668-670` with:

```rust
        // The single-FSM attach is the hot shape (every pre-M14b client, the
        // gateway, this bench): drain its one ring directly, so the hot body
        // carries no iterator setup over a heap Vec. N > 1 keeps the loop.
        if let [(id, ring)] = egress_services.as_mut_slice() {
            emitted += drain_ring(ring, Some(*id), shared, buf, fanin, &mut cb);
        } else {
            for (id, ring) in egress_services.iter_mut() {
                emitted += drain_ring(ring, Some(*id), shared, buf, fanin, &mut cb);
            }
        }
```

Build, copy to `hb-v3`, run the same two A/Bs (`--a` = previous kept binary; `--a` = `hb-main`), logs `r4-prev-vs-v3.log` / `r4b-main-vs-v3.log`. Revert if not kept.

- [ ] **Step 7: Adjudicate, revert the rest, run the correctness gate.** The rule, applied identically to all three:

> **Keep** a variant iff its delta vs the previous kept tree is **≥ +1 %** over `--reps 6` **and** the two sides' min–max ranges are disjoint (the runner prints `disjoint`/`OVERLAP`). Otherwise **revert** it — an overlapping range on this box means "not measured", not "small win". Record every variant's number either way, kept or not: a null result is the finding for that suspect.

Then, with only the kept variants in the tree:

```bash
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo test -p uc2_client
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo test -p uc2_node --test services --test query_barrier
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo clippy --workspace --all-targets -- -D warnings
```

Expect all green. `uc2_client` covers the slot table and `engine_synthetic.rs`'s per-ring matching, fan-in ordering and counter assertions (the only tests that can see v1's re-routing); `services` and `query_barrier` are the `uc2_node` end-to-end tests that drive the query prefix v2 moves and the multi-ring poll loop v3 reshapes. If v1 changed no behaviour it must change no test — a red here is a real defect in the refactor, not a flake, and is the reason these runs come after the measurements rather than instead of them.

**The residual.** Compute the kept tree's position against `hb-m14a` from R0 and the kept deltas, and re-measure it directly if any variant was kept:

```bash
cp /home/claude/cargo-target-uc2-m14a/release/examples/hop_bench /home/claude/m14c-ab/bin/hb-kept
scripts/hop1_ab.sh --sink /home/claude/m14c-ab/bin/hb-main \
  --a /home/claude/m14c-ab/bin/hb-m14a --b /home/claude/m14c-ab/bin/hb-kept \
  --reps 6 --root /home/claude/m14c-ab | tee /home/claude/m14c-ab/r5-m14a-vs-kept.log
```

If the kept tree still sits **more than 1.5 % below** `hb-m14a`, record that as the residual, in the doc, in those words — an honest unexplained remainder, with the suspects that were measured and refuted named. Do not keep adding variants to close it; the M14a precedent is that the hot body's remaining cost is real work (per-ring matching, the wider `Resolve`, the fan-in buffer) that M14b bought deliberately, and M14d's fleet gate is where it is adjudicated against the whole chain (M13 measured the fleet chain as cluster-bound: 1.75 M/s into a 3-node cluster vs 2.44 M/s against a dummy node, so a few percent of client hop may be masked end to end — real per core either way).

- [ ] **Step 8: Write the bench doc.** Create `docs/benchmarks/uc2-m14c-client-hop-2026-08-28.md`, shaped like `docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md`:

- **Header:** date; the three trees and their commits (`main` 4347bc2, `main` 3a7f9a5, the branch at the Task-1 commit); the box; "a private `CARGO_TARGET_DIR` per tree"; the sha256 prefixes from Step 2.
- **Smoke banner** (blockquote), the M14a doc's wording: dev-box numbers are never compared to a bar (`docs/notes/dev-box-not-a-bench.md`); what is recorded is ratios from one hop measured alone.
- **"What the harness isolates":** `hop_bench engine-load` → `dummy-node` over a real instance dir (cnc page + rings, no log buffer, no consensus, no service); one engine, one sender thread and one poll thread, `--wait yield`; inflight 4096, payload 64 B, 6 s; a fresh sink per run; the sink is `hb-main` for every run so only the driver differs; the four-line `engine_load.rs` difference between 3a7f9a5 and 4347bc2 (exhaustive-match arms on a path a bench never takes) noted as a non-confound.
- **Results table**, one row per A/B, columns `A | B | A mean resp/s | B mean resp/s | delta % | ranges (disjoint/overlap) | p90 A / p90 B`: R0 (`hb-m14a` vs `hb-main`), R1 (`hb-main` vs `hb-t1`), R2/R2b (v1), R3/R3b (v2), R4/R4b (v3), R5 (`hb-m14a` vs `hb-kept`). Fill every cell from the runner's summary lines in `/home/claude/m14c-ab/*.log`; do not round away the min/max, they are what the disjointness claim rests on.
- **Findings**, numbered, in the M14a doc's voice: (1) what the fast path did — tail only, and why (the RMW is the tail, the body is the rate); (2) which variants measured and which were null, each named with its number, including a plain statement for each null that the suspect was refuted rather than "no change observed"; (3) the residual, if any, in the words of Step 7.
- **"What this changes":** the kept variants, the fact that the client hop is a per-core cost the fleet chain may mask, and that M14d's fleet gate is the adjudicator.
- **Reproduce:** the exact Step 2 build commands and one `scripts/hop1_ab.sh` invocation, verbatim.

- [ ] **Step 9: Commit.**

```bash
git add scripts/hop1_ab.sh docs/benchmarks/uc2-m14c-client-hop-2026-08-28.md
git add uc2_client/src/engine.rs            # ONLY if at least one variant was kept
git status --porcelain                       # expect exactly the files above, nothing else
git commit -m "perf(client): hop-1 hot-body bisection — <kept variants, one clause each>; committed A/B runner + dev-box smoke record"
```

Write the kept variants into the message (e.g. `out-of-line fan-in arms in handle_record`), and if none measured, say so plainly (`no variant measured; suspects recorded`) and drop `uc2_client/src/engine.rs` from the `git add` list — the runner and the doc still ship, because the null result is the finding. Expect `git status --porcelain` to list only those files: the three build trees, the copied binaries and every log live under `/home/claude/` (real disk, never `/tmp`) and are outside the repo by construction. `/home/claude/m14c-a0-tree` is an extracted archive, not a worktree — `git worktree list` must still show exactly the three entries it showed before this task.

---

### Task 3: `SNAP_BEGIN` 0.6.0 — `layout` / `service_id` / `services_declared` (+ `CURRENT`, fuzz)

**Files:**
- Modify `uc_protocol/src/v2/datagram.rs` — `SNAP_BEGIN_FIXED_LEN` (line 159), the `SnapBeginBody` doc block (226–232), the struct (233–238), `write_snap_begin_body` (240–251), `read_snap_begin_body` (252–268), the `DGRAM_KIND_SNAP_BEGIN`/`DGRAM_KIND_SNAP_DONE` kind docs (143, 150), unit test `snap_begin_body_roundtrips_and_pins_layout` (673–692)
- Modify `uc_protocol/src/version.rs` — the `CURRENT` doc comment block (ending line 54) and `pub const CURRENT` (line 55)
- Modify `uc2_net/src/sender.rs` (the one `SnapBeginBody { .. }` literal, line 1105–1109) and `uc2_net/src/receiver.rs` (the SNAP_DONE echo literal, 1578–1586; the three in-module test literals at 3956–3959, 4038–4046, 4092–4095) — **interim field values only**, replaced properly in Tasks 4 and 5
- Modify `fuzz/src/seeds.rs` — the `10-snap-begin-config` seed (112–121) and a new seed after `13-config-reply` (145); `fuzz/README.md` — the `uc_protocol_datagram` row (line 155); regenerate `fuzz/corpus/uc_protocol_datagram/`
- Modify `docs/reference/wire-protocol.md` — the version table (line 13) and the cnc-vs-wire note (lines 16–19)

**Interfaces:**

Consumes (exists in the tree):
```rust
// uc_protocol/src/v2/datagram.rs:159, 233, 240, 253 (today, 0.5.0)
pub const SNAP_BEGIN_FIXED_LEN: usize = 26;
pub struct SnapBeginBody { pub session: u32, pub snapshot_pos: u64, pub total_len: u64, pub config: Vec<u8> }
pub fn write_snap_begin_body(buf: &mut [u8], b: &SnapBeginBody);
pub fn read_snap_begin_body(buf: &[u8]) -> Option<SnapBeginBody>;
// uc_protocol/src/version.rs:55
pub const CURRENT: ProtocolVersion = ProtocolVersion::new(0, 5, 0);
```

Produces (Tasks 4–6 depend on exactly these):
```rust
pub const SNAP_BEGIN_FIXED_LEN: usize = 34;
/// The 0.6.0 body layout discriminator carried at byte 4.
pub const SNAP_BEGIN_LAYOUT_V2: u8 = 1;
pub struct SnapBeginBody {
    pub session: u32,
    pub layout: u8,
    pub service_id: u8,
    pub snapshot_pos: u64,
    pub total_len: u64,
    pub services_declared: u64,
    pub config: Vec<u8>,
}
pub fn write_snap_begin_body(buf: &mut [u8], b: &SnapBeginBody);   // writes b.layout verbatim
pub fn read_snap_begin_body(buf: &[u8]) -> Option<SnapBeginBody>;  // any layout value; None below 34 B
pub const CURRENT: ProtocolVersion = ProtocolVersion::new(0, 6, 0);
```

**Two decisions to state in the commit message, both deliberate:**

1. **`write_snap_begin_body` writes `b.layout` verbatim rather than hardcoding `1`.** Every production caller passes `SNAP_BEGIN_LAYOUT_V2`; writing the field is strictly more general and is what lets Task 5's receiver-refusal test forge a well-formed 34-byte `layout == 0` body with the real encoder instead of hand-assembling bytes.
2. **A 0.5.0 sender's datagram never reaches the `layout` check.** A 0.5.0 `SNAP_BEGIN` body is 26 bytes plus its config; `read_snap_begin_body` returns `None` below 34, so the *length* check drops it first and the receiver never sees a body at all. The `layout == 0` refusal (Task 5) is therefore **defensive**: it fires only on a body that is 0.6.0-*shaped* (≥ 34 B) yet carries `layout = 0` — a 0.5.0 sender whose 26-byte fixed part plus an ≥ 8-byte config happens to reach 34 bytes, which is exactly the real M7 case (`config` is always non-empty on a configured cluster). Both halves are pinned by tests below.

- [ ] **Step 1: Write the failing tests**

Replace `snap_begin_body_roundtrips_and_pins_layout` (`uc_protocol/src/v2/datagram.rs:673–692`) with:

```rust
    #[test]
    fn snap_begin_body_roundtrips_and_pins_layout() {
        assert_eq!(SNAP_BEGIN_FIXED_LEN, 34, "0.6.0 fixed part (spec §14.3)");
        let b = SnapBeginBody {
            session: 0x0A0B_0C0D,
            layout: SNAP_BEGIN_LAYOUT_V2,
            service_id: 2,
            snapshot_pos: 0x1000,
            total_len: 300 * 1024,
            services_declared: 0b101,
            config: vec![],
        };
        let mut buf = vec![0u8; SNAP_BEGIN_FIXED_LEN];
        write_snap_begin_body(&mut buf, &b);
        assert_eq!(read_snap_begin_body(&buf), Some(b));
        // session=0x0A0B0C0D -> LE [0x0D,0x0C,0x0B,0x0A]; layout=1; service_id=2;
        // [6..8] zero; snapshot_pos=0x1000 -> LE [0,0x10,0,0,0,0,0,0];
        // total_len=307200=0x0004_B000 -> LE [0x00,0xB0,0x04,0,0,0,0,0];
        // services_declared=0b101 -> LE [5,0,0,0,0,0,0,0]; config_len=0 -> LE [0,0].
        assert_eq!(
            &buf[..],
            &[
                0x0D, 0x0C, 0x0B, 0x0A, 1, 2, 0, 0, 0x00, 0x10, 0, 0, 0, 0, 0, 0, 0x00, 0xB0,
                0x04, 0, 0, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );
        // Short buffer is rejected (caller drops the datagram).
        assert_eq!(read_snap_begin_body(&buf[..SNAP_BEGIN_FIXED_LEN - 1]), None);
    }

    /// A wire-0.5.0 sender's SNAP_BEGIN is dropped by the LENGTH check, before
    /// `layout` is even looked at: its fixed part is 26 bytes. The `layout`
    /// refusal on the receiving node (M14c, `uc2_net`) is therefore defensive —
    /// it catches a body that is 0.6.0-SHAPED (>= 34 B, which a 0.5.0 body with
    /// an 8-byte-or-longer config reaches) yet carries layout 0.
    #[test]
    fn a_wire_050_shaped_snap_begin_is_too_short_and_a_layout_zero_body_decodes() {
        // The exact 26 bytes a 0.5.0 `write_snap_begin_body` produced.
        let legacy: [u8; 26] = [
            0x0D, 0x0C, 0x0B, 0x0A, 0, 0, 0, 0, 0x00, 0x10, 0, 0, 0, 0, 0, 0, 0x00, 0xB0, 0x04,
            0, 0, 0, 0, 0, 0, 0,
        ];
        assert_eq!(read_snap_begin_body(&legacy), None, "26 bytes is below the 0.6.0 fixed part");
        // 34 bytes with layout 0 DOES decode — the reader is total and hands the
        // discriminator to the caller, which is what decides (spec §14.3).
        let b = SnapBeginBody {
            session: 1,
            layout: 0,
            service_id: 0,
            snapshot_pos: 4096,
            total_len: 64,
            services_declared: 0,
            config: vec![],
        };
        let mut buf = vec![0u8; SNAP_BEGIN_FIXED_LEN];
        write_snap_begin_body(&mut buf, &b);
        let got = read_snap_begin_body(&buf).expect("a 34-byte body always decodes");
        assert_eq!(got.layout, 0);
        assert_eq!(got, b);
    }

    /// `config` still rides at the end and its length is still re-checked
    /// against the buffer actually received.
    #[test]
    fn snap_begin_config_rides_past_the_fixed_part() {
        let cfg = vec![0x11u8, 0x22, 0x33, 0x44];
        let b = SnapBeginBody {
            session: 7,
            layout: SNAP_BEGIN_LAYOUT_V2,
            service_id: 1,
            snapshot_pos: 8192,
            total_len: 1 << 20,
            services_declared: 0b11,
            config: cfg.clone(),
        };
        let mut buf = vec![0u8; SNAP_BEGIN_FIXED_LEN + cfg.len()];
        write_snap_begin_body(&mut buf, &b);
        assert_eq!(&buf[32..34], &[4, 0], "config_len at [32..34]");
        assert_eq!(read_snap_begin_body(&buf), Some(b));
        // Truncated config: refused, not silently short-read.
        assert_eq!(read_snap_begin_body(&buf[..buf.len() - 1]), None);
    }
```

And in `uc_protocol/src/version.rs`'s test module, add:

```rust
    #[test]
    fn current_is_the_m14c_snapshot_wire() {
        assert_eq!(CURRENT, ProtocolVersion::new(0, 6, 0));
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo test -p uc_protocol --lib`
Expected: compile errors — `cannot find value SNAP_BEGIN_LAYOUT_V2 in this scope`, and `struct SnapBeginBody has no field named layout` (also `service_id`, `services_declared`) at the three new test literals.

- [ ] **Step 3: Implement the 0.6.0 body**

In `uc_protocol/src/v2/datagram.rs`, replace line 159 with:

```rust
/// Fixed part of a [`SnapBeginBody`] (wire 0.6.0, M14c). 0.5.0's was 26; the
/// 0.6.0 body reuses the old 4-byte pad for `layout` + `service_id` and
/// inserts an 8-byte `services_declared` word before `config_len`. A 0.5.0
/// body is therefore *shorter* than this and is dropped by
/// [`read_snap_begin_body`]'s length check.
pub const SNAP_BEGIN_FIXED_LEN: usize = 34;

/// The value [`SnapBeginBody::layout`] carries on wire 0.6.0. `0` is what a
/// 0.5.0 sender's pad byte reads as — see [`read_snap_begin_body`].
pub const SNAP_BEGIN_LAYOUT_V2: u8 = 1;
```

Replace the doc block + struct + both functions (226–268) with:

```rust
/// Opens (and, echoed back, acks) one artifact of a snapshot session.
///
/// **M14c / wire 0.6.0.** A session is a *stream of artifacts* — one BEGIN per
/// declared FSM, ascending by id, each followed by that artifact's chunks;
/// chunk offsets are stream-global, so `SNAP_NAK` repair is byte-identical to
/// 0.5.0 (spec §14.3). `session` scopes chunk/NAK traffic to one transfer;
/// `layout` is the body discriminator (`SNAP_BEGIN_LAYOUT_V2` on 0.6.0);
/// `service_id` names which FSM's artifact this is; `snapshot_pos` is the
/// artifact's tag `S`; `total_len` is THAT artifact's file size (the receiver
/// pre-sizes its `.part` to it); `services_declared` is the sender's declared
/// FSM bitmask, which the receiver compares against its own and which tells it
/// how many artifacts complete the session; `config` is the length-prefixed
/// encoded config (M7, empty for M6), identical on every BEGIN of a session.
///
/// LE: session 0..4, layout 4, service_id 5, 6..8 zero (u64 alignment for
/// `snapshot_pos`), snapshot_pos 8..16, total_len 16..24,
/// services_declared 24..32, config_len u16 32..34, config bytes 34...
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapBeginBody {
    pub session: u32,
    pub layout: u8,
    pub service_id: u8,
    pub snapshot_pos: u64,
    pub total_len: u64,
    pub services_declared: u64,
    pub config: Vec<u8>,
}

/// Encode a snap-begin body. `layout` is written verbatim — production callers
/// pass [`SNAP_BEGIN_LAYOUT_V2`]; a test forging a legacy-discriminator body
/// passes 0.
pub fn write_snap_begin_body(buf: &mut [u8], b: &SnapBeginBody) {
    buf[0..4].copy_from_slice(&b.session.to_le_bytes());
    buf[4] = b.layout;
    buf[5] = b.service_id;
    buf[6..8].fill(0);
    buf[8..16].copy_from_slice(&b.snapshot_pos.to_le_bytes());
    buf[16..24].copy_from_slice(&b.total_len.to_le_bytes());
    buf[24..32].copy_from_slice(&b.services_declared.to_le_bytes());
    buf[32..34].copy_from_slice(&(b.config.len() as u16).to_le_bytes());
    if !b.config.is_empty() {
        buf[34..34 + b.config.len()].copy_from_slice(&b.config);
    }
}

/// Decode a snap-begin body, or `None` if the buffer is shorter than
/// [`SNAP_BEGIN_FIXED_LEN`] or than the `config_len` it declares (the caller
/// drops a malformed datagram).
///
/// **Total for every `layout` value, including 0.** Deciding what an unknown
/// discriminator means is the receiving node's job, not the decoder's: it
/// counts a named refusal (`peer wire 0.5.0`) and drops the session, which is
/// diagnosable, where a silent `None` here would be indistinguishable from a
/// truncated datagram.
pub fn read_snap_begin_body(buf: &[u8]) -> Option<SnapBeginBody> {
    if buf.len() < SNAP_BEGIN_FIXED_LEN {
        return None;
    }
    let config_len = u16::from_le_bytes(buf[32..34].try_into().ok()?) as usize;
    if buf.len() < SNAP_BEGIN_FIXED_LEN + config_len {
        return None;
    }
    Some(SnapBeginBody {
        session: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
        layout: buf[4],
        service_id: buf[5],
        snapshot_pos: u64::from_le_bytes(buf[8..16].try_into().unwrap()),
        total_len: u64::from_le_bytes(buf[16..24].try_into().unwrap()),
        services_declared: u64::from_le_bytes(buf[24..32].try_into().unwrap()),
        config: buf[34..34 + config_len].to_vec(),
    })
}
```

Extend the kind docs at line 143 and 150:

```rust
/// leader → peer: opens one artifact of a session. Body = [`SnapBeginBody`];
/// header `position` = 0. M14c: one BEGIN per declared FSM, ascending by id.
pub const DGRAM_KIND_SNAP_BEGIN: u8 = 12;
```
```rust
/// peer → leader: EVERY artifact of the session is complete (echoes the last
/// artifact's [`SnapBeginBody`] as the ack).
pub const DGRAM_KIND_SNAP_DONE: u8 = 15;
```

- [ ] **Step 4: Bump `CURRENT` and keep the workspace compiling**

`uc_protocol/src/version.rs` — append to the comment block above line 55, then change the constant:

```rust
// 0.6.0 (M14c): `DGRAM_KIND_SNAP_BEGIN`'s body carries the per-FSM fields a
// multi-service snapshot session needs — `layout`, `service_id` and
// `services_declared` — growing its fixed part from 26 to 34 bytes
// (`SNAP_BEGIN_FIXED_LEN`). Every other datagram, the 16-byte header included,
// is byte-identical to 0.5.0, so a mixed 0.5.0/0.6.0 cluster replicates and
// elects normally; only a snapshot session between mixed versions goes wrong,
// and only the 0.6.0 side can detect it (the `layout` byte). **This constant
// is documentary and is not itself checked on any receive path** — the flag
// day rests on the standing operational rule (upgrade all nodes together,
// `docs/how-to/upgrade-a-cluster.md`), not on a version gate.
pub const CURRENT: ProtocolVersion = ProtocolVersion::new(0, 6, 0);
```

Now fix the five `SnapBeginBody { .. }` literals outside `uc_protocol` with **interim** values so the workspace builds; Tasks 4 and 5 replace them with real ones.

`uc2_net/src/sender.rs:1106-1109` →
```rust
        write_snap_begin_body(
            &mut body,
            &SnapBeginBody {
                session,
                layout: SNAP_BEGIN_LAYOUT_V2,
                service_id: 0,          // Task 4: the artifact's id
                snapshot_pos,
                total_len,
                services_declared: 1,   // Task 4: the source's declared mask
                config: config.to_vec(),
            },
        );
```
(add `SNAP_BEGIN_LAYOUT_V2` to the `uc_protocol::v2::datagram` import list at `uc2_net/src/sender.rs:28-30`.)

`uc2_net/src/receiver.rs:1578-1586` (the SNAP_DONE echo) →
```rust
        write_snap_begin_body(
            &mut d[DATAGRAM_HEADER_LEN..],
            &SnapBeginBody {
                session: intake.session,
                layout: SNAP_BEGIN_LAYOUT_V2,
                service_id: 0,          // Task 5: the last artifact's id
                snapshot_pos: intake.snapshot_pos,
                total_len: intake.total_len,
                services_declared: 1,   // Task 5: this node's own mask
                config: vec![], // the DONE ack carries no config — only SNAP_BEGIN ships it
            },
        );
```
(add `SNAP_BEGIN_LAYOUT_V2` to the import list at `uc2_net/src/receiver.rs:38-45`.)

The three in-module receiver tests (3956, 4038, 4092) each gain `layout: SNAP_BEGIN_LAYOUT_V2, service_id: 0, services_declared: 1,` in their literal — same shape; Task 5 rewrites them.

- [ ] **Step 5: Run the tests**

Run: `CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo test -p uc_protocol --lib && CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo test -p uc2_net`
Expected: all pass. `uc2_net`'s snapshot tests still pass because the receiver ignores the new fields at this point and the body simply grew by 8 bytes on both sides.

- [ ] **Step 6: Fuzz seed + corpus + README**

In `fuzz/src/seeds.rs`, update the `10-snap-begin-config` literal (line 119) to the new fields:

```rust
    write_snap_begin_body(
        &mut b,
        &SnapBeginBody {
            session: 7,
            layout: SNAP_BEGIN_LAYOUT_V2,
            service_id: 0,
            snapshot_pos: 8192,
            total_len: 1 << 20,
            services_declared: 0b1,
            config,
        },
    );
```

and add, immediately before `seeds` is returned (after line 145):

```rust
    // M14c / wire 0.6.0: a MULTI-service SNAP_BEGIN — a non-zero `service_id`,
    // a multi-bit `services_declared`, and no config, so the decoder's fixed
    // part is exercised at exactly `SNAP_BEGIN_FIXED_LEN` (the 10- seed covers
    // the config-carrying variable-length path).
    let mut b = vec![0u8; SNAP_BEGIN_FIXED_LEN];
    write_snap_begin_body(
        &mut b,
        &SnapBeginBody {
            session: 9,
            layout: SNAP_BEGIN_LAYOUT_V2,
            service_id: 2,
            snapshot_pos: 65536,
            total_len: 300 * 1024,
            services_declared: 0b101,
            config: vec![],
        },
    );
    seeds.push(Seed::fixed("14-snap-begin-v2", datagram(DGRAM_KIND_SNAP_BEGIN, 0, 3, &b)));
```

**Name deviation, state it in the commit:** the brief asked for `11-snap-begin-v2`, but `11-` is already taken by `11-snap-nak` (`fuzz/src/seeds.rs:125`, corpus file `fuzz/corpus/uc_protocol_datagram/11-snap-nak`) and the `NN-<name>` prefix is this corpus's ordering convention. `14-` is the next free index.

Regenerate and update the README row:

```bash
cd fuzz && cargo +nightly run --bin seed-corpus
```
Expected: `10-snap-begin-config` rewritten (8 bytes longer — `Regen::Always`), `14-snap-begin-v2` created. Commit both corpus files.

`fuzz/README.md:155` → append to the `uc_protocol_datagram` cell: `Since wire 0.6.0 that includes SNAP_BEGIN's layout discriminator, per-FSM id and declared-set word (`read_snap_begin_body` is total for every layout value).`

- [ ] **Step 7: Reference doc + verify the proof surface is untouched**

`docs/reference/wire-protocol.md:13` → `| `version::CURRENT` | `0.6.0` |`; replace the parenthetical at line 18–19 with: `the UDP datagram format moved to 0.6.0 in M14c, when `SNAP_BEGIN` grew its per-FSM fields — every other datagram is byte-identical to 0.5.0.`

Confirm the Lean/conformance tier does not model this body:
```bash
grep -rn -i "snap_begin\|SNAP_BEGIN" proofs/ docs/VERIFICATION.md
```
Expected: no output — the Lean model and the conformance vectors cover election/commit safety, not the snapshot transport, so nothing there needs regenerating. Note that in the commit message so the next reader does not re-derive it.

- [ ] **Step 8: Clippy + commit**

```bash
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo clippy --workspace --all-targets -- -D warnings
git add uc_protocol/src/v2/datagram.rs uc_protocol/src/version.rs \
        uc2_net/src/sender.rs uc2_net/src/receiver.rs \
        fuzz/src/seeds.rs fuzz/README.md fuzz/corpus/uc_protocol_datagram \
        docs/reference/wire-protocol.md
git commit -m "feat(protocol)!: wire 0.6.0 — SNAP_BEGIN carries layout/service_id/services_declared (fixed part 26 → 34)"
```

---

### Task 4: sender — one session, a stream of N artifacts

**Files:**
- Modify `uc2_net/src/sender.rs` — `SnapshotSource` (line 97–107), `SnapSession` (109–128), the session fields on `Sender` (270–277), `try_open_snap_session` (947–976), `drive_snap_session` (978–1051), `send_snap_chunk` (1053–1090), `send_snap_begin` (1092–1115), the `SNAP_DGRAMS_PER_CYCLE`/timeout constants (57–68), the T17 test fixtures (2572–2651, 2800–2835)
- Modify `uc2_net/tests/snapshot_session.rs` — the `snapshot_source` in `build()` (lines 96–101) and `tempfile::tempdir()` → `tempdir_in` (93, 133)

**Interfaces:**

Consumes (verified): `uc_protocol::v2::datagram::{SNAP_BEGIN_FIXED_LEN = 34, SNAP_BEGIN_LAYOUT_V2, SnapBeginBody { session, layout, service_id, snapshot_pos, total_len, services_declared, config }, write_snap_begin_body}` (Task 3); `Sender::assemble_snap(&mut self, peer: SocketAddr, position: u64, kind: u8, payload: &[u8]) -> bool` (`uc2_net/src/sender.rs:1152`); `CtrlMsg::SnapNak { from, session, offset, length }` / `CtrlMsg::SnapDone { from, session }` (`:83-86`, handlers `:573-595` — **unchanged**, they key on `(peer, session)` only).

Produces:
```rust
// uc2_net::sender
/// One FSM's newest durable artifact, as offered to a snapshot session.
pub struct SnapArtifact { pub service_id: u8, pub snapshot_pos: u64, pub path: PathBuf, pub len: u64 }
/// The whole set a session ships: one artifact per declared FSM, ASCENDING by
/// `service_id`, non-empty, every `len > 0`.
pub struct SnapshotSet { pub services_declared: u64, pub config: Vec<u8>, pub artifacts: Vec<SnapArtifact> }
pub type SnapshotSource = Arc<dyn Fn() -> Option<SnapshotSet> + Send + Sync>;
pub fn Sender::set_snapshot_source(&mut self, src: SnapshotSource);  // signature unchanged, type changed
```

**Ordering decision (state it in the commit):** the two-artifact **transport** tests live in **Task 5**, not here, because they assert receiver-side facts (`snapshots/<id>/` paths, per-artifact rename, the declared-set refusals) that only exist once the intake is per-id. Task 4's own proof is a **sender-side** unit test on the datagram stream (two BEGINs, correct ids/bases, no chunk spanning an artifact boundary), and Task 4 keeps `uc2_net/tests/snapshot_session.rs` green with a one-artifact set.

**Design note for the implementer — why a BEGIN is re-sent.** The sender advances to artifact *k+1* when artifact *k*'s last chunk has been *sent*, not acked (spec §14.3), so a lost BEGIN has to be self-healing: without a resend, a dropped BEGIN(*k*) makes the receiver drop every chunk of artifact *k* (it cannot place them), the receiver cannot NAK for an artifact it was never told about, and the session stalls until `SNAP_SESSION_TIMEOUT_NS` (30 s) — which under the 20 % loss the tests inject is not a corner case. A duplicate BEGIN is a no-op at the receiver (same session + id + pos), so re-sending it is free of side effects, exactly like a re-sent chunk. This also fixes the same hole for artifact 0 on 0.5.0.

- [ ] **Step 1: Write the failing test**

In `uc2_net/src/sender.rs`'s test module, beside `sender_without_crypto_and_snapshot_source` (line 2625), add a two-artifact fixture and its test:

```rust
    /// Two artifacts, ids 0 and 2, 2048 B and 3000 B — deliberately not a
    /// multiple of the MTU budget, so the boundary clamp is exercised.
    fn sender_with_two_artifacts() -> (Sender, Fake, tempfile::TempDir) {
        let (mut s, f, dir) = sender_without_crypto();
        let p0 = dir.path().join("snap-2048.ultsnap");
        let p2 = dir.path().join("snap-4096.ultsnap");
        std::fs::write(&p0, vec![0xA1u8; 2048]).unwrap();
        std::fs::write(&p2, vec![0xB2u8; 3000]).unwrap();
        s.set_snapshot_source(Arc::new(move || {
            Some(SnapshotSet {
                services_declared: 0b101,
                config: t17_config_bytes(),
                artifacts: vec![
                    SnapArtifact { service_id: 0, snapshot_pos: 2048, path: p0.clone(), len: 2048 },
                    SnapArtifact { service_id: 2, snapshot_pos: 4096, path: p2.clone(), len: 3000 },
                ],
            })
        }));
        (s, f, dir)
    }

    #[test]
    fn a_session_ships_one_begin_per_artifact_and_never_spans_a_boundary() {
        let (mut s, f, _dir) = sender_with_two_artifacts();
        let addr: SocketAddr = "127.0.0.1:59991".parse().unwrap();
        s.on_nak(addr, 0, 96); // below the ring floor → upgrades to a session
        let mut begins: Vec<SnapBeginBody> = Vec::new();
        let mut chunks: Vec<(u64, usize)> = Vec::new(); // (stream offset, payload len)
        for _ in 0..12 {
            s.do_work();
            while let Some(d) = f.recv_raw() {
                let h = read_datagram_header(&d).unwrap();
                match h.kind {
                    DGRAM_KIND_SNAP_BEGIN => {
                        begins.push(read_snap_begin_body(&d[DATAGRAM_HEADER_LEN..]).unwrap())
                    }
                    DGRAM_KIND_SNAP_CHUNK => {
                        chunks.push((h.position, d.len() - DATAGRAM_HEADER_LEN))
                    }
                    _ => {}
                }
            }
        }
        assert_eq!(begins.len(), 2, "one BEGIN per artifact: {begins:?}");
        assert_eq!((begins[0].service_id, begins[0].snapshot_pos, begins[0].total_len), (0, 2048, 2048));
        assert_eq!((begins[1].service_id, begins[1].snapshot_pos, begins[1].total_len), (2, 4096, 3000));
        for b in &begins {
            assert_eq!(b.layout, SNAP_BEGIN_LAYOUT_V2);
            assert_eq!(b.services_declared, 0b101, "the declared mask rides EVERY begin");
            assert_eq!(b.session, begins[0].session, "one session for the whole stream");
            assert_eq!(b.config, t17_config_bytes(), "config rides every begin unchanged");
        }
        // Stream-global offsets, contiguous over [0, 5048), and no datagram
        // straddles the 2048 boundary (the receiver writes one datagram into
        // exactly one `.part`).
        chunks.sort_unstable();
        chunks.dedup();
        let mut want = 0u64;
        for &(off, len) in &chunks {
            assert_eq!(off, want, "chunks fill the stream contiguously: {chunks:?}");
            assert!(
                off >= 2048 || off + len as u64 <= 2048,
                "chunk [{off}, {}) spans the artifact boundary at 2048",
                off + len as u64
            );
            want = off + len as u64;
        }
        assert_eq!(want, 5048, "the whole 2048 + 3000 byte stream was sent");
    }
```

`sender_without_crypto` and `t17_config_bytes` already exist in that module (`:2565`, `:2625`'s body); the new fixture reuses them. Add `SNAP_BEGIN_LAYOUT_V2`, `read_datagram_header`, `read_snap_begin_body`, `DGRAM_KIND_SNAP_BEGIN` to the test module's imports if absent.

- [ ] **Step 2: Run to verify it fails**

Run: `CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo test -p uc2_net --lib sender::`
Expected: compile error — `cannot find struct SnapshotSet` / `SnapArtifact` in this scope; the existing fixtures at 2618/2646/2830 also fail to compile once the type changes (fixed in Step 3).

- [ ] **Step 3: Implement the source types and the session**

Replace `SnapshotSource` (`uc2_net/src/sender.rs:97-107`) with:

```rust
/// M14c: one FSM's newest durable snapshot artifact, as offered to a session.
#[derive(Debug, Clone)]
pub struct SnapArtifact {
    pub service_id: u8,
    pub snapshot_pos: u64,
    pub path: PathBuf,
    pub len: u64,
}

/// M14c (spec §7.3/§14.3): everything one snapshot session ships — one
/// artifact per declared FSM, **ascending by `service_id`, non-empty, every
/// `len > 0`**, plus the declared bitmask and the config the session carries.
/// A source that cannot honour those invariants must return `None`: the
/// session is refused (the peer re-NAKs) rather than opened half-formed.
#[derive(Debug, Clone)]
pub struct SnapshotSet {
    /// The sender's declared FSM mask; rides every `SNAP_BEGIN` and is what
    /// the receiver compares against its own (`declared-set mismatch`).
    pub services_declared: u64,
    /// M7: the encoded `ConfigRecord.config` at ship time — see below.
    pub config: Vec<u8>,
    pub artifacts: Vec<SnapArtifact>,
}

/// M6 Task 6 / M14c: the newest durable snapshot SET the node is willing to
/// ship. The node wires this to each declared FSM's `SnapshotStore` filtered by
/// its PERSISTED floor marker (never a half-written file). `None` = nothing
/// shippable (the NAK stays an overrun). M7 Task 6: `config` is the
/// `v2::config::encode_config` bytes of the CURRENT `ConfigRecord.config` at
/// ship time — carried in every `SNAP_BEGIN` so a below-floor joiner adopts the
/// leader's membership alongside its lineage. Over-delivery (shipping to a peer
/// whose config is already current) is safe: the receiver adopts by fiat only
/// on a genuine install, and adoption is idempotent by version.
pub type SnapshotSource = Arc<dyn Fn() -> Option<SnapshotSet> + Send + Sync>;
```

Replace `SnapSession` (109–128) with:

```rust
/// One artifact inside an in-flight outbound session. `base` is its first
/// byte's STREAM-GLOBAL offset (the session is one concatenated byte stream
/// with artifact boundaries announced by the BEGINs), so `SNAP_NAK` repair is
/// byte-identical to 0.5.0.
struct SnapPart {
    service_id: u8,
    snapshot_pos: u64,
    base: u64,
    len: u64,
    file: std::fs::File,
    /// When this artifact's `SNAP_BEGIN` was last put on the wire; `None` =
    /// never. Re-sent on the [`SNAP_BEGIN_RESEND_NS`] cadence — see
    /// `drive_snap_session`.
    begun_ns: Option<u64>,
}

/// One in-flight outbound snapshot transfer (M6 Task 6; M14c: N artifacts). At
/// most one at a time — a second requester waits; sessions are rare by
/// construction (only a peer whose NAK fell below the purge floor triggers one).
struct SnapSession {
    peer: SocketAddr,
    session: u32,
    /// Rides every `SNAP_BEGIN`; the receiver refuses a session whose mask
    /// differs from its own.
    services_declared: u64,
    /// Ascending by `service_id`, contiguous in `base`.
    parts: Vec<SnapPart>,
    /// Sum of the artifacts' lengths — the stream's byte space.
    stream_len: u64,
    /// Next sequential STREAM offset to ship (the contiguous fill cursor).
    cursor: u64,
    /// Peer-requested missing STREAM ranges (repair), served before the cursor.
    naks: VecDeque<(u64, u32)>,
    last_activity_ns: u64,
    /// M7 Task 6: the encoded `ConfigRecord.config` at the moment this session
    /// opened (from the `SnapshotSource` closure) — carried in every `SNAP_BEGIN`.
    config: Vec<u8>,
}

impl SnapSession {
    /// Index of the artifact containing stream offset `at`, or `None` past EOF.
    fn part_at(&self, at: u64) -> Option<usize> {
        self.parts.iter().position(|p| at >= p.base && at < p.base + p.len)
    }
}
```

Add beside `SNAP_SESSION_TIMEOUT_NS` (line 68):

```rust
/// A session re-sends the `SNAP_BEGIN` of the artifact it is currently working
/// on no more often than this. A lost BEGIN would otherwise strand every chunk
/// of that artifact (the receiver cannot place bytes for an artifact it was
/// never told about, and cannot NAK for one either) until the 30 s session
/// timeout. A duplicate BEGIN is a no-op at the receiver, so this costs at most
/// one datagram per 20 ms per session.
const SNAP_BEGIN_RESEND_NS: u64 = 20_000_000;
```

- [ ] **Step 4: Implement open / drive / chunk / begin**

Replace `try_open_snap_session` (947–976):

```rust
    fn try_open_snap_session(&mut self, to: SocketAddr) -> bool {
        if self.snap.is_some() {
            return false;
        }
        let Some(src) = self.snapshot_source.clone() else {
            return false;
        };
        let Some(set) = src() else {
            return false;
        };
        if set.artifacts.is_empty() {
            return false;
        }
        // Build the parts eagerly: every file must open and every invariant
        // must hold before a single datagram goes out, so a half-formed set
        // stays an overrun (the peer re-NAKs) rather than a session the
        // receiver can never complete.
        let mut parts = Vec::with_capacity(set.artifacts.len());
        let mut base = 0u64;
        let mut prev_id: Option<u8> = None;
        for a in &set.artifacts {
            if a.len == 0 || prev_id.is_some_and(|p| p >= a.service_id) {
                return false; // empty artifact, or not strictly ascending
            }
            prev_id = Some(a.service_id);
            let Ok(file) = std::fs::File::open(&a.path) else {
                return false;
            };
            parts.push(SnapPart {
                service_id: a.service_id,
                snapshot_pos: a.snapshot_pos,
                base,
                len: a.len,
                file,
                begun_ns: None,
            });
            base += a.len;
        }
        let sid = self.snap_session_seq.wrapping_add(1);
        self.snap_session_seq = sid;
        self.snap = Some(SnapSession {
            peer: to,
            session: sid,
            services_declared: set.services_declared,
            parts,
            stream_len: base,
            cursor: 0,
            naks: VecDeque::new(),
            last_activity_ns: self.base.elapsed().as_nanos() as u64,
            config: set.config,
        });
        self.stats.snap_sessions.fetch_add(1, Ordering::Relaxed);
        true
    }
```

Replace `drive_snap_session` (978–1051):

```rust
    /// Advance the in-flight snapshot session by at most [`SNAP_DGRAMS_PER_CYCLE`]
    /// chunk datagrams: make sure this cycle's target artifact has a live
    /// `SNAP_BEGIN`, then serve peer repair NAKs, then fill the cursor
    /// sequentially. Abandons the session after [`SNAP_SESSION_TIMEOUT_NS`] with
    /// no progress. Returns `true` iff it did work.
    fn drive_snap_session(&mut self) -> bool {
        let Some(mut sess) = self.snap.take() else {
            return false;
        };
        let now = self.base.elapsed().as_nanos() as u64;
        if now.saturating_sub(sess.last_activity_ns) >= SNAP_SESSION_TIMEOUT_NS {
            // Abandoned (peer died, or its DONE was lost): drop the session; the
            // slot frees for the next requester. `self.snap` stays `None`.
            return true;
        }

        // Which artifact this cycle's first datagram targets: the head repair
        // NAK's, else the cursor's, else the last one (the stream is fully sent
        // and we are waiting for the DONE — keep its BEGIN alive).
        let target = sess
            .naks
            .front()
            .and_then(|&(off, _)| sess.part_at(off))
            .or_else(|| sess.part_at(sess.cursor))
            .unwrap_or(sess.parts.len() - 1);

        let mut did = false;
        let first_ever = sess.parts[target].begun_ns.is_none();
        let stale = sess.parts[target]
            .begun_ns
            .is_none_or(|at| now.saturating_sub(at) >= SNAP_BEGIN_RESEND_NS);
        if stale {
            let p = &sess.parts[target];
            let (peer, session, service_id, pos, len) =
                (sess.peer, sess.session, p.service_id, p.snapshot_pos, p.len);
            let declared = sess.services_declared;
            // M8 (Task 17): `begun_ns` latches only on a datagram that actually
            // reached the wire. A seal failure (no session with this peer yet)
            // must leave the artifact un-begun so the NEXT cycle retries the
            // BEGIN — latching it unconditionally would ship chunks a receiver
            // with no intake for them can only drop.
            if self.send_snap_begin(peer, session, service_id, pos, len, declared, &sess.config) {
                sess.parts[target].begun_ns = Some(now);
                did = true;
            } else if first_ever {
                // Nothing in this artifact can make progress until the peer has
                // its BEGIN; keep the slot and retry next cycle. `false` (no
                // work done) deliberately: a session whose peer has no key yet
                // must not keep the agent's duty loop hot, and
                // `last_activity_ns` is left un-refreshed so the session is
                // abandoned on the ordinary `SNAP_SESSION_TIMEOUT_NS` path if
                // the link never comes up.
                self.snap = Some(sess);
                return false;
            }
        }

        let mut emitted = 0usize;
        // Repair NAKs first (the peer is blocked on these).
        while emitted < SNAP_DGRAMS_PER_CYCLE {
            let Some((offset, length)) = sess.naks.pop_front() else {
                break;
            };
            let n = self.send_snap_chunk(&mut sess, offset, true);
            if n == 0 {
                break; // outside every artifact / read error — drop the request
            }
            if (n as u32) < length {
                // Range spans multiple datagrams (or an artifact boundary):
                // re-queue the remainder.
                sess.naks.push_front((offset + n as u64, length - n as u32));
            }
            emitted += 1;
            did = true;
        }
        // Then sequential cursor fill, artifact by artifact.
        while emitted < SNAP_DGRAMS_PER_CYCLE && sess.cursor < sess.stream_len {
            let at = sess.cursor;
            let Some(i) = sess.part_at(at) else {
                break;
            };
            if sess.parts[i].begun_ns.is_none() {
                // Crossed into the next artifact: its BEGIN goes out first, at
                // the top of the next cycle.
                break;
            }
            let n = self.send_snap_chunk(&mut sess, at, false);
            if n == 0 {
                break;
            }
            sess.cursor += n as u64;
            emitted += 1;
            did = true;
        }

        if did {
            sess.last_activity_ns = now;
        }
        self.snap = Some(sess);
        did
    }
```

Replace the body of `send_snap_chunk` (1056–1090) — everything above `let budget` stays as documented, the seek/read/EOF handling becomes artifact-relative:

```rust
    /// Read one MTU-sized chunk at STREAM offset `offset` from whichever
    /// artifact contains it and ship it as a SNAP_CHUNK (header `position` =
    /// the stream offset). A datagram never spans an artifact boundary — the
    /// receiver writes one datagram into exactly one `.part`. Returns bytes
    /// sent (0 past the stream / read error).
    fn send_snap_chunk(&mut self, sess: &mut SnapSession, offset: u64, is_nak: bool) -> usize {
        let Some(i) = sess.part_at(offset) else {
            return 0;
        };
        // M8 (Task 17): `- crypto_overhead()`. A sealed chunk adds the 8-byte
        // counter and the 16-byte tag, so without this the datagram overruns
        // `mtu` by exactly `CRYPTO_OVERHEAD` on every full chunk.
        let budget = self.cfg.mtu - DATAGRAM_HEADER_LEN - self.cfg.crypto_overhead();
        let part_end = sess.parts[i].base + sess.parts[i].len;
        let want = ((part_end - offset) as usize).min(budget);
        let in_file = offset - sess.parts[i].base;
        let mut buf = vec![0u8; want];
        if sess.parts[i].file.seek(SeekFrom::Start(in_file)).is_err() {
            return 0;
        }
        if sess.parts[i].file.read_exact(&mut buf).is_err() {
            return 0;
        }
        if !self.assemble_snap(sess.peer, offset, DGRAM_KIND_SNAP_CHUNK, &buf) {
            // Sealed-or-dropped: never a cleartext fallback. Reported as 0
            // bytes sent, which leaves the sequential cursor exactly where it
            // was (retried next cycle) and drops a repair request (the peer's
            // snapshot NAK timer re-fires) — the same shape a lost datagram
            // already takes on this path.
            return 0;
        }
        let _ = self.sock.send_to(&self.scratch, sess.peer);
        self.stats.snap_chunks.fetch_add(1, Ordering::Relaxed);
        if is_nak {
            self.stats.snap_chunk_naks.fetch_add(1, Ordering::Relaxed);
        }
        want
    }
```

Replace `send_snap_begin` (1097–1115):

```rust
    /// Ship one artifact's SNAP_BEGIN (header `position` = 0; body carries
    /// session / layout / service_id / pos / len / declared / config). Returns
    /// `false` if the datagram could not be sealed and was therefore dropped
    /// (M8 Task 17) — the caller must NOT latch the artifact as begun.
    #[allow(clippy::too_many_arguments)]
    fn send_snap_begin(
        &mut self,
        peer: SocketAddr,
        session: u32,
        service_id: u8,
        snapshot_pos: u64,
        total_len: u64,
        services_declared: u64,
        config: &[u8],
    ) -> bool {
        let mut body = vec![0u8; SNAP_BEGIN_FIXED_LEN + config.len()];
        write_snap_begin_body(
            &mut body,
            &SnapBeginBody {
                session,
                layout: SNAP_BEGIN_LAYOUT_V2,
                service_id,
                snapshot_pos,
                total_len,
                services_declared,
                config: config.to_vec(),
            },
        );
        if !self.assemble_snap(peer, 0, DGRAM_KIND_SNAP_BEGIN, &body) {
            return false;
        }
        let _ = self.sock.send_to(&self.scratch, peer);
        true
    }
```

- [ ] **Step 5: Update the three existing sender fixtures**

`sender_with_crypto_and_established_session` (2618), `sender_without_crypto_and_snapshot_source` (2646) and the fixture at 2830 each close over `Some((4096, snap_path.clone(), total, t17_config_bytes()))`. Each becomes:

```rust
        s.set_snapshot_source(Arc::new(move || {
            Some(SnapshotSet {
                services_declared: 0b1,
                config: t17_config_bytes(),
                artifacts: vec![SnapArtifact {
                    service_id: 0,
                    snapshot_pos: 4096,
                    path: snap_path.clone(),
                    len: total,
                }],
            })
        }));
```

`a_snapshot_begin_is_sealed_so_its_carried_config_cannot_be_forged` (2710) keeps `assert_eq!(begins.len(), 1)`: `snap_datagrams` (2653) drives exactly 4 `do_work` calls with no sleeps, so `SNAP_BEGIN_RESEND_NS` (20 ms) cannot elapse — the resend does not perturb it. Add to that test, after the existing assertions:

```rust
        assert_eq!(body.layout, SNAP_BEGIN_LAYOUT_V2);
        assert_eq!(body.service_id, 0);
        assert_eq!(body.services_declared, 0b1);
```

- [ ] **Step 6: Keep the integration harness compiling and green**

`uc2_net/tests/snapshot_session.rs:96-101` →

```rust
    let snapshot_source: uc2_net::sender::SnapshotSource = Arc::new(move || {
        Some(uc2_net::sender::SnapshotSet {
            services_declared: 0b1,
            config: Vec::new(),
            artifacts: vec![uc2_net::sender::SnapArtifact {
                service_id: 0,
                snapshot_pos: SNAP_POS,
                path: snap_path.clone(),
                len: src_len,
            }],
        })
    });
```

Also change both `tempfile::tempdir().unwrap()` calls (lines 93 and 133) to
`tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap()` — this file predates the rule and `/tmp` here is RAM-backed with no swap (CLAUDE.md, "Local box").

- [ ] **Step 7: Run**

```bash
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo test -p uc2_net
```
Expected: `a_session_ships_one_begin_per_artifact_and_never_spans_a_boundary` passes; the two `snapshot_session.rs` tests still pass (one artifact, receiver still writing flat paths — Task 5 moves them); every T17 crypto test passes.

- [ ] **Step 8: Clippy + commit**

```bash
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo clippy --workspace --all-targets -- -D warnings
git add uc2_net/src/sender.rs uc2_net/tests/snapshot_session.rs
git commit -m "feat(net): snapshot sender ships a stream of N artifacts (SnapshotSet, stream-global offsets, self-healing BEGIN)"
```

---

### Task 5: receiver — per-id intake, floor on the complete set, two named refusals

**Files:**
- Modify `uc2_net/src/receiver.rs` — `FollowerStats` (366–465, two new counters), `SnapIntake` (474–495), the `FollowerReceiver` snapshot fields (549–564), the constructor defaults (717–723), `set_snapshot_intake` (824–838), `snap_begin` (1480–1523), `snap_chunk` (1525–1547), `snap_complete` (1549–1600), `snap_upkeep` (1640–1679), and the three in-module tests (3944–4008, 4021–4076, 4079–4118)
- Modify `uc2_net/tests/snapshot_session.rs` — `build()` (76–155), `final_path` (177–179), the two existing tests (182–219), plus three new tests

**Interfaces:**

Consumes: `SnapBeginBody` + `SNAP_BEGIN_LAYOUT_V2` + `SNAP_BEGIN_FIXED_LEN = 34` (Task 3); `Rebuilt::{new(start), insert(start, end) -> bool, contiguous() -> u64, first_gap() -> Option<(u64,u64)>}` (`uc2_net/src/rebuild.rs:23,33,28,74`); `NakTimer::{new(cfg, seed), poll(Option<(u64,u64)>, now_ns) -> Option<(u64,u64)>}` (`:111,126`); `IncomingSnapshotSignal = (Arc<AtomicU64>, Arc<Mutex<Vec<u8>>>)` (`:472`); `FollowerReceiver::stats() -> Arc<FollowerStats>` (used at `uc2_node/src/node.rs:996`).

Produces:
```rust
// uc2_net::receiver::FollowerStats — the two named refusals (spec §14.3),
// read by uc2_node (Task 6) and exported as metrics in the observability workstream.
pub snap_refused_legacy_peer: AtomicU64,        // "peer wire 0.5.0"
pub snap_refused_declared_mismatch: AtomicU64,  // "declared-set mismatch"
// signature change: the intake takes the snapshots/ ROOT and this node's own mask
pub fn FollowerReceiver::set_snapshot_intake(
    &mut self,
    snap_root: PathBuf,
    own_declared: u64,
    incoming: Option<IncomingSnapshotSignal>,
);
```

**Design correction to the brief — `parts: Vec<SnapPart>`, not `current: Option<(u8, u64, u64, u64)>`.** The sender rotates to artifact *k+1* when artifact *k*'s last chunk has been **sent**, not received (spec §14.3, and Task 4 implements exactly that). Under loss, artifact *k* therefore still has gaps when BEGIN(*k+1*) arrives. With a single `current`, those repair chunks would land in the wrong file or be dropped, artifact *k* would never be renamed, and `received` would never reach `services_declared` — the joiner stranded, which is the one outcome the design forbids. Keeping every announced artifact (at most `CNC_MAX_SERVICES` = 8, each a path plus one open `.part`) makes the repair path artifact-agnostic: `got` is one `Rebuilt` over the whole stream, `first_gap()` names the first hole wherever it is, and any artifact whose range the contiguous frontier has passed is renamed. The last entry of `parts` is "the current artifact"; `announced_len` is the brief's `stream_base + artifact_len` generalised.

- [ ] **Step 1: Write the failing tests**

Rewrite `uc2_net/tests/snapshot_session.rs`'s harness to take an artifact list. Replace the `SNAP_POS`/`SNAP_LEN` constants (35–36) and `write_snapshot_file` (56–64) with:

```rust
const SNAP_LEN: usize = 300 * 1024;

/// Deterministic snapshot payload for `id` (so both ends can be byte-compared,
/// and the two artifacts of a stream are distinguishable).
fn snapshot_bytes(id: u8) -> Vec<u8> {
    (0..SNAP_LEN).map(|i| (i.wrapping_mul(31).wrapping_add(7 + id as usize)) as u8).collect()
}

/// The artifact position FSM `id` publishes in these tests.
fn snap_pos(id: u8) -> u64 {
    64 * 1024 + id as u64 * 4096
}

fn write_snapshot_file(dir: &Path, id: u8) -> PathBuf {
    let path = dir.join(format!("snap-{id}-{}.ultsnap", snap_pos(id)));
    std::fs::write(&path, snapshot_bytes(id)).unwrap();
    path
}
```

In `build`, take `ids: &[u8]` and build the set + intake (replacing lines 96–101 and 142):

```rust
fn build(faults: FaultConfig, ids: &[u8]) -> Harness {
    ...
    let leader_dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    let mut declared = 0u64;
    let mut artifacts = Vec::new();
    for &id in ids {
        declared |= 1 << id;
        let path = write_snapshot_file(leader_dir.path(), id);
        let len = std::fs::metadata(&path).unwrap().len();
        artifacts.push(uc2_net::sender::SnapArtifact {
            service_id: id,
            snapshot_pos: snap_pos(id),
            path,
            len,
        });
    }
    let snapshot_source: uc2_net::sender::SnapshotSource = Arc::new(move || {
        Some(uc2_net::sender::SnapshotSet {
            services_declared: declared,
            config: Vec::new(),
            artifacts: artifacts.clone(),
        })
    });
    ...
    follower.set_snapshot_intake(follower_snap_dir.clone(), declared, None);
    ...
}
```
(`follower_snap_dir` is now the `snapshots/` ROOT; the receiver creates `snapshots/<id>/` itself. Keep the existing `create_dir_all` of the root.)

Replace `final_path` (177–179) and update the two existing tests:

```rust
    fn final_path(&self, id: u8) -> PathBuf {
        self.follower_snap_dir.join(id.to_string()).join(format!("snap-{}.ultsnap", snap_pos(id)))
    }
```

`below_floor_nak_upgrades_to_snapshot_session_and_file_transfers_exactly` → `build(FaultConfig::default(), &[0])`, `h.final_path(0)`, `snapshot_bytes(0)`.
`snapshot_session_survives_chunk_loss_via_snap_nak` → same, `&[0]`.

Then add the three new tests:

```rust
#[test]
fn a_two_artifact_stream_lands_in_per_id_dirs_under_chunk_loss() {
    // Drop 20% of datagrams in BOTH directions: chunks from BOTH artifacts are
    // lost → the follower NAKs the stream-global gaps → repair chunks fill
    // them, wherever in the stream they fall. Both files complete.
    let faults = FaultConfig { drop_per_million: 200_000, seed: 42, ..FaultConfig::default() };
    let mut h = build(faults, &[0, 2]);
    h.trigger();
    h.pump_until("both artifacts transferred under loss", |h| {
        h.final_path(0).exists() && h.final_path(2).exists()
    });

    assert_eq!(std::fs::read(h.final_path(0)).unwrap(), snapshot_bytes(0), "FSM 0's artifact");
    assert_eq!(std::fs::read(h.final_path(2)).unwrap(), snapshot_bytes(2), "FSM 2's artifact");
    assert!(
        !h.follower_snap_dir.join("1").exists(),
        "an undeclared id gets no directory"
    );
    assert_eq!(
        h.leader_send.stats().snap_sessions.load(Ordering::Relaxed),
        1,
        "ONE session carries the whole set"
    );
    assert!(
        h.leader_send.stats().snap_chunk_naks.load(Ordering::Relaxed) > 0,
        "the SNAP_NAK repair path must have run under 20% loss"
    );
    // No `.part` survives a completed session.
    for id in [0u8, 2] {
        let dir = h.follower_snap_dir.join(id.to_string());
        let parts: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".part"))
            .collect();
        assert!(parts.is_empty(), "{dir:?} still holds a .part");
    }
}

#[test]
fn a_layout_zero_begin_is_refused_as_a_wire_050_peer() {
    let mut h = build(FaultConfig::default(), &[0]);
    let st = h.follower.stats();
    h.forge_begin(0, 0b1); // layout 0 = a 0.5.0-shaped body
    h.pump_until("the legacy-layout refusal is counted", |_| {
        st.snap_refused_legacy_peer.load(Ordering::Relaxed) > 0
    });
    assert_eq!(st.snap_refused_declared_mismatch.load(Ordering::Relaxed), 0, "not the other refusal");
    assert!(!h.follower_snap_dir.join("0").exists(), "no intake, no directory, no .part");
}

#[test]
fn a_mismatched_declared_set_refuses_the_session() {
    let mut h = build(FaultConfig::default(), &[0]); // the follower's own mask is 0b1
    let st = h.follower.stats();
    h.forge_begin(uc_protocol::v2::datagram::SNAP_BEGIN_LAYOUT_V2, 0b11);
    h.pump_until("the declared-set refusal is counted", |_| {
        st.snap_refused_declared_mismatch.load(Ordering::Relaxed) > 0
    });
    assert_eq!(st.snap_refused_legacy_peer.load(Ordering::Relaxed), 0, "not the other refusal");
    assert!(!h.follower_snap_dir.join("0").exists(), "no intake, no directory, no .part");
}
```

with this helper on `Harness` (the follower is cleartext here, so a plain socket suffices):

```rust
    /// Send a hand-built SNAP_BEGIN straight at the follower — the only way to
    /// exercise a refusal, since our own sender never emits one.
    fn forge_begin(&self, layout: u8, services_declared: u64) {
        use uc_protocol::v2::datagram::{
            DATAGRAM_HEADER_LEN, DGRAM_KIND_SNAP_BEGIN, DatagramHeader, SNAP_BEGIN_FIXED_LEN,
            SnapBeginBody, write_datagram_header, write_snap_begin_body,
        };
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN + SNAP_BEGIN_FIXED_LEN];
        write_datagram_header(
            &mut d,
            &DatagramHeader {
                position: 0,
                leadership_term_id: TERM,
                kind: DGRAM_KIND_SNAP_BEGIN,
                flags: 0,
                key_epoch: 0,
            },
        );
        write_snap_begin_body(
            &mut d[DATAGRAM_HEADER_LEN..],
            &SnapBeginBody {
                session: 99,
                layout,
                service_id: 0,
                snapshot_pos: snap_pos(0),
                total_len: 64,
                services_declared,
                config: vec![],
            },
        );
        let s = UdpSocket::bind("127.0.0.1:0").unwrap();
        s.send_to(&d, self.follower_addr).unwrap();
    }
```

`pump_until`'s closure takes `&Harness`; the two refusal tests ignore it and read the `Arc<FollowerStats>` captured before the pump.

- [ ] **Step 2: Run to verify they fail**

Run: `CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo test -p uc2_net --test snapshot_session`
Expected: compile errors — `set_snapshot_intake` takes 2 arguments but 3 were supplied; `no field snap_refused_legacy_peer on type FollowerStats`.

- [ ] **Step 3: Implement the counters and the intake state**

Append to `FollowerStats` (before the closing brace at 465):

```rust
    /// M14c (spec §14.3): a `SNAP_BEGIN` arrived whose `layout` byte is not
    /// [`SNAP_BEGIN_LAYOUT_V2`] — the named refusal **`peer wire 0.5.0`**. The
    /// session is dropped. NOTE this is the *defensive* half: a genuine 0.5.0
    /// body is only 26 bytes plus its config and is usually dropped by
    /// `read_snap_begin_body`'s length check before it ever gets here; this
    /// fires when a 0.5.0 body happens to reach 34 bytes (an 8-byte-or-longer
    /// carried config — i.e. every configured cluster). The follower keeps
    /// NAKing; the operator sees the counter and finishes the flag day.
    pub snap_refused_legacy_peer: AtomicU64,
    /// M14c (spec §8, §14.3): a `SNAP_BEGIN` arrived whose `services_declared`
    /// differs from this node's own declared mask (or that names a service id
    /// outside it) — the named refusal **`declared-set mismatch`**. The session
    /// is dropped: installing a set that does not cover this node's FSMs would
    /// strand one below an adopted floor. Declared sets must match cluster-wide.
    pub snap_refused_declared_mismatch: AtomicU64,
```

Replace `SnapIntake` (474–495) with:

```rust
/// One artifact inside an inbound session. `base` is its first byte's
/// STREAM-GLOBAL offset — the session is one concatenated byte stream whose
/// artifact boundaries the BEGINs announce, so the repair path is unchanged.
struct SnapPart {
    service_id: u8,
    snapshot_pos: u64,
    base: u64,
    len: u64,
    /// Taken (dropped) just before the rename, mirroring the 0.5.0 discipline.
    file: Option<std::fs::File>,
    part_path: PathBuf,
    final_path: PathBuf,
    done: bool,
}

/// M6 Task 6 / M14c: one in-flight INBOUND snapshot transfer — a stream of one
/// artifact per declared FSM. Chunks land at their stream offset in the
/// pre-sized `.part` of whichever artifact contains that offset; one `Rebuilt`
/// over the stream's byte space tracks contiguity + gaps (NAK'd like the main
/// stream). Each artifact is fsync'd + atomically renamed the moment the
/// contiguous frontier passes its end; the FLOOR is adopted only once EVERY
/// declared id has landed, so no FSM is ever stranded below an adopted floor.
struct SnapIntake {
    peer: SocketAddr,
    session: u32,
    /// From the session's first `SNAP_BEGIN`; equals this node's own mask (a
    /// difference is refused before an intake ever opens).
    services_declared: u64,
    /// Bit `i` set ⇔ id `i`'s artifact is complete and renamed.
    received: u64,
    /// Announced artifacts, ascending, contiguous in `base`.
    parts: Vec<SnapPart>,
    /// Sum of the announced artifacts' lengths — how far the stream is known
    /// to run (it grows as later BEGINs arrive).
    announced_len: u64,
    /// Contiguity over `[0, announced_len)` STREAM offsets.
    got: Rebuilt,
    nak: NakTimer,
    /// M7 Task 6: the encoded `ConfigRecord.config` carried in `SNAP_BEGIN`
    /// (identical on every BEGIN of a session; taken from the first).
    config: Vec<u8>,
}
```

On `FollowerReceiver`, rename the field doc and add the mask (549–564):

```rust
    /// M6 Task 6 / M14c: the snapshots ROOT (`instance_dir/snapshots`) for
    /// inbound transfers; each artifact lands under `<root>/<service_id>/`.
    /// `None` = this node never receives snapshots (no intake).
    snap_dir: Option<PathBuf>,
    /// M14c: this node's own declared FSM mask, compared against every
    /// session's `services_declared`. Set by `set_snapshot_intake`.
    own_declared: u64,
```
Constructor default (beside `snap_dir: None` at 717): `own_declared: 0,`.

Replace `set_snapshot_intake` (832–838):

```rust
    /// M6 Task 6 / M14c: enable INBOUND snapshot transfers. `snap_root` is the
    /// `snapshots/` directory the per-id `.part`/final artifacts land under
    /// (`<root>/<id>/`); `own_declared` is this node's declared FSM bitmask,
    /// which every session's `SNAP_BEGIN` must match (`declared-set mismatch`);
    /// `incoming` (if set) is `(position, config)`: the position cell receives
    /// each COMPLETED session's floor — the MINIMUM over the received artifact
    /// positions — for the consensus agent to adopt as an archive floor, and
    /// (M7 Task 6) the config cell receives that session's carried
    /// `SNAP_BEGIN.config` bytes for the agent's `adopt_snapshot_config`
    /// handler. Without this call kinds 12/13 are ignored (a node that never
    /// joins below a floor never receives snapshots).
    pub fn set_snapshot_intake(
        &mut self,
        snap_root: PathBuf,
        own_declared: u64,
        incoming: Option<IncomingSnapshotSignal>,
    ) {
        self.snap_dir = Some(snap_root);
        self.own_declared = own_declared;
        if let Some((pos, config)) = incoming {
            self.incoming_snapshot_pos = Some(pos);
            self.incoming_snapshot_config = Some(config);
        }
    }
```

- [ ] **Step 4: Implement begin / chunk / publish / complete / upkeep**

Add a free helper above `impl FollowerReceiver` and replace `snap_begin` (1483–1523):

```rust
/// Open the `.part` for one announced artifact under `<root>/<id>/`. Free
/// function so it borrows neither the receiver nor the intake.
fn open_snap_part(root: &Path, b: &SnapBeginBody, base: u64) -> Option<SnapPart> {
    let dir = root.join(b.service_id.to_string());
    std::fs::create_dir_all(&dir).ok()?;
    let part_path = dir.join(format!("incoming-{}.part", b.snapshot_pos));
    let final_path = dir.join(format!("snap-{}.ultsnap", b.snapshot_pos));
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(&part_path)
        .ok()?;
    file.set_len(b.total_len).ok()?;
    Some(SnapPart {
        service_id: b.service_id,
        snapshot_pos: b.snapshot_pos,
        base,
        len: b.total_len,
        file: Some(file),
        part_path,
        final_path,
        done: false,
    })
}
```

```rust
    /// Begin (or extend) an inbound snapshot transfer: pre-size this artifact's
    /// `.part` and start tracking it. A duplicate BEGIN for an artifact already
    /// announced in this session is a no-op; a BEGIN for a different session
    /// replaces a stale one. Two named refusals drop the session outright.
    fn snap_begin(&mut self, from: SocketAddr, b: SnapBeginBody) {
        let Some(root) = self.snap_dir.clone() else {
            return; // this node does not receive snapshots
        };
        if b.layout != SNAP_BEGIN_LAYOUT_V2 {
            // "peer wire 0.5.0" — a body whose discriminator we do not speak.
            self.stats.snap_refused_legacy_peer.fetch_add(1, Ordering::Relaxed);
            self.snap_intake = None;
            return;
        }
        // The id must be inside the mask, and the mask must be OURS. A shift by
        // an id >= 64 is not representable — `checked_shl` folds that into the
        // same refusal rather than panicking in debug.
        let bit = 1u64.checked_shl(b.service_id as u32).unwrap_or(0);
        if b.services_declared != self.own_declared || b.services_declared & bit == 0 {
            self.stats.snap_refused_declared_mismatch.fetch_add(1, Ordering::Relaxed);
            self.snap_intake = None;
            return;
        }
        if b.total_len == 0 {
            return;
        }
        if let Some(cur) = self.snap_intake.as_mut()
            && cur.peer == from
            && cur.session == b.session
        {
            if cur
                .parts
                .iter()
                .any(|p| p.service_id == b.service_id && p.snapshot_pos == b.snapshot_pos)
            {
                return; // duplicate BEGIN — already announced (the sender re-sends)
            }
            // The next artifact of the SAME session: it starts where the
            // announced stream currently ends.
            let Some(part) = open_snap_part(&root, &b, cur.announced_len) else {
                return;
            };
            cur.announced_len += part.len;
            cur.parts.push(part);
            self.stats.datagrams.fetch_add(1, Ordering::Relaxed);
            return;
        }
        // A new session (or one replacing a stale one).
        let Some(part) = open_snap_part(&root, &b, 0) else {
            return;
        };
        let announced_len = part.len;
        self.snap_intake = Some(SnapIntake {
            peer: from,
            session: b.session,
            services_declared: b.services_declared,
            received: 0,
            parts: vec![part],
            announced_len,
            got: Rebuilt::new(0),
            nak: NakTimer::new(self.snap_nak_cfg, self.snap_seed ^ b.session as u64),
            config: b.config,
        });
        self.stats.datagrams.fetch_add(1, Ordering::Relaxed);
    }
```

Replace `snap_chunk` (1526–1547) and add the publish step:

```rust
    /// Land one snapshot chunk at its STREAM offset, in whichever announced
    /// artifact contains it; publish any artifact the contiguous frontier has
    /// now passed.
    fn snap_chunk(&mut self, from: SocketAddr, offset: u64, payload: &[u8]) {
        let Some(intake) = self.snap_intake.as_mut() else {
            return;
        };
        if intake.peer != from || payload.is_empty() {
            return;
        }
        let Some(end) = offset.checked_add(payload.len() as u64) else {
            return;
        };
        // A chunk always sits inside ONE artifact (the sender never spans a
        // boundary). Anything else — past the announced stream, or for a BEGIN
        // we have not seen — is dropped; the sender re-sends that BEGIN on its
        // own cadence and the peer re-NAKs the bytes.
        let Some(i) = intake
            .parts
            .iter()
            .position(|p| offset >= p.base && end <= p.base + p.len)
        else {
            return;
        };
        let at = offset - intake.parts[i].base;
        let Some(file) = intake.parts[i].file.as_mut() else {
            return; // already renamed — a duplicate repair chunk
        };
        if file.seek(SeekFrom::Start(at)).is_err() || file.write_all(payload).is_err() {
            return;
        }
        intake.got.insert(offset, end);
        self.snap_publish_complete_parts();
    }

    /// fsync + atomically rename every announced artifact the contiguous
    /// frontier has now covered end to end, then — once EVERY declared id has
    /// landed — complete the session. A torn `.part` is never renamed, so a
    /// reader (the service's gap guard, or AdoptFloor) only ever sees a
    /// complete artifact.
    fn snap_publish_complete_parts(&mut self) {
        let Some(intake) = self.snap_intake.as_mut() else {
            return;
        };
        let contiguous = intake.got.contiguous();
        for p in intake.parts.iter_mut() {
            if p.done || contiguous < p.base + p.len {
                continue;
            }
            let Some(file) = p.file.take() else {
                continue;
            };
            if file.sync_all().is_err() {
                p.file = Some(file);
                return;
            }
            drop(file);
            if std::fs::rename(&p.part_path, &p.final_path).is_err() {
                return;
            }
            p.done = true;
            intake.received |= 1u64 << p.service_id; // id < 64: checked in `snap_begin`
        }
        if intake.received != intake.services_declared {
            return; // the set is incomplete — no floor is adopted yet
        }
        self.snap_complete();
    }
```

Replace `snap_complete`'s head and its position/DONE handling (1552–1600) — the ack echoes the LAST artifact, and the floor is the minimum:

```rust
    /// Every artifact of the session is renamed: ack with SNAP_DONE, publish the
    /// carried config, and signal the floor — the MINIMUM over the received
    /// positions, which is exactly the node floor the leader shipped from, so
    /// every FSM's own artifact sits at or above it.
    fn snap_complete(&mut self) {
        let Some(intake) = self.snap_intake.take() else {
            return;
        };
        let Some(last) = intake.parts.last() else {
            return;
        };
        let floor = intake.parts.iter().map(|p| p.snapshot_pos).min().unwrap_or(0);
        // Ack: echo the last artifact's SnapBeginBody as SNAP_DONE so the leader
        // closes its session (it keys on `(peer, session)` alone).
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN + SNAP_BEGIN_FIXED_LEN];
        write_datagram_header(
            &mut d,
            &DatagramHeader {
                position: 0,
                leadership_term_id: self.term.load(Ordering::Relaxed),
                kind: DGRAM_KIND_SNAP_DONE,
                flags: 0,
                key_epoch: 0,
            },
        );
        write_snap_begin_body(
            &mut d[DATAGRAM_HEADER_LEN..],
            &SnapBeginBody {
                session: intake.session,
                layout: SNAP_BEGIN_LAYOUT_V2,
                service_id: last.service_id,
                snapshot_pos: last.snapshot_pos,
                total_len: last.len,
                services_declared: intake.services_declared,
                config: vec![], // the DONE ack carries no config — only SNAP_BEGIN ships it
            },
        );
        // M8 (T17): sealed or dropped. A dropped DONE costs only the leader's
        // session slot (it times out); the local artifacts are already renamed,
        // so the install below must proceed either way.
        self.seal_and_send(intake.peer, DGRAM_KIND_SNAP_DONE, &mut d);
        // M7 Task 6: publish the carried config BEFORE the position signal (see
        // the field docs for the ordering argument).
        if let Some(cell) = &self.incoming_snapshot_config {
            *cell.lock().unwrap() = intake.config.clone();
        }
        if let Some(slot) = &self.incoming_snapshot_pos {
            slot.store(floor, Ordering::Release);
        }
        self.snap_adopt_pending = Some(floor);
        self.stats.datagrams.fetch_add(1, Ordering::Relaxed);
    }
```

Replace `snap_upkeep`'s gap computation (1644–1652) — everything below it is unchanged:

```rust
        let contiguous = intake.got.contiguous();
        let announced_len = intake.announced_len;
        let gap = intake.got.first_gap().or({
            if contiguous < announced_len {
                Some((contiguous, announced_len))
            } else {
                None
            }
        });
```

Add `use std::path::Path;` if the module does not already import it (it imports `PathBuf` at the top).

- [ ] **Step 5: Update the three in-module receiver tests**

Each of `the_snapshot_intakes_snap_nak_and_snap_done_are_sealed` (3944), `an_unsealed_snap_begin_is_refused_now_that_t17_landed` (4021) and `an_unsealed_snap_chunk_is_refused_now_that_t17_landed` (4079):

- `r.set_snapshot_intake(dir.path().to_path_buf(), None);` → `r.set_snapshot_intake(dir.path().to_path_buf(), 0b1, None);`
- `tempfile::tempdir()` → `tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR"))`
- every `SnapBeginBody { .. }` literal gains `layout: SNAP_BEGIN_LAYOUT_V2, service_id: 0, services_declared: 0b1,` (replacing Task 3's interim `services_declared: 1` — same value, now meaningful)
- in the first test, the completion assertion becomes
  `assert!(dir.path().join("0").join("snap-4096.ultsnap").exists(), "the sealed session actually completed end to end");`
  and add `assert_eq!(done.service_id, 0); assert_eq!(done.services_declared, 0b1);` beside the existing `done.session` check.

- [ ] **Step 6: Run**

```bash
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo test -p uc2_net
```
Expected: all five `snapshot_session` tests pass (the two single-artifact ones now reading `snapshots/0/`), the two refusal tests count exactly their own counter, and every in-module crypto/T17 test passes.

If `a_two_artifact_stream_lands_in_per_id_dirs_under_chunk_loss` times out, the first thing to check is BEGIN loss: assert on `h.follower.stats().datagrams` and print which of the two `final_path`s is missing — a missing *second* file with a complete first one means the artifact-2 BEGIN never landed and Task 4's `SNAP_BEGIN_RESEND_NS` path is not firing (target selection falls through to `parts.len() - 1`).

- [ ] **Step 7: Clippy + commit**

```bash
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo clippy --workspace --all-targets -- -D warnings
git add uc2_net/src/receiver.rs uc2_net/tests/snapshot_session.rs
git commit -m "feat(net): per-FSM snapshot intake — snapshots/<id>/, floor adopted only on the complete set, two named refusals"
```

---

### Task 6: node wiring + the two-FSM learner join + flag-day docs

**Files:**
- Modify `uc2_node/src/services.rs` — add `ring_mask()` beside `ring_ids()` (lines 87–93)
- Modify `uc2_node/src/ipc.rs` — add `snapshot_root()` beside `snapshot_dir_for` (lines 92–95)
- Modify `uc2_node/src/node.rs` — the snapshot wiring block (906–938), `set_snapshot_intake` (980–983), a public accessor beside `crypto_stats` (1450–1458)
- Modify `uc2_node/tests/learner.rs` — the existing `fresh_learner_joins_a_purged_leader_via_snapshot_session` (338–455, one added store) and a new test after it
- Modify `docs/how-to/upgrade-a-cluster.md` (new section after line 214's `## Control-page change in 2.8.0` block, i.e. before `## Where to go next` at 238) and `docs/reference/semver-policy.md` (line 136)

**Interfaces:**

Consumes (verified): `uc2_net::sender::{SnapshotSet, SnapArtifact, SnapshotSource}` and `FollowerReceiver::set_snapshot_intake(PathBuf, u64, Option<IncomingSnapshotSignal>)` (Tasks 4, 5); `FollowerStats::{snap_refused_legacy_peer, snap_refused_declared_mismatch}` (Task 5); `ServicesConfig::{declared() -> u64, ids(), ring_ids(), is_declared(u8)}` (`uc2_node/src/services.rs:74-93`); `CncPage::service_slot(usize).snapshot_pos.load_acquire()` (used at `services.rs:146`); `InstanceDir::snapshot_dir_for(u8)` (`uc2_node/src/ipc.rs:93`); `Node::route_drops: Arc<FollowerStats>` — the SAME `Arc` the receiver bumps (`node.rs:996, 451`), already surfaced by `crypto_stats()` (`:1456`).

Produces:
```rust
// uc2_node::services
impl ServicesConfig { pub fn ring_mask(&self) -> u64; }   // {0} for a none_for_tests node
// uc2_node::ipc
impl InstanceDir { pub fn snapshot_root(&self) -> PathBuf; }
// uc2_node
impl Node {
    /// (peer wire 0.5.0, declared-set mismatch) — the observability workstream's metric source.
    pub fn snapshot_session_refusals(&self) -> (u64, u64);
}
```

**Two decisions to state in the commit:**

1. **The source closure walks `ring_ids()`, not `ids()`, and puts `ring_mask()` on the wire.** M14a's standing harness rule is that a page whose `services_declared` reads `0` is a harness node treated as `{0}` — and `ServicesConfig::none_for_tests()` is what every node-only test in the tree uses, including the existing single-FSM learner-join test. With `ids()` a harness node would offer an empty set, never open a session, and that test would fail; `ring_ids()` yields `{0}` there and is *identical* to `declared` for any real node (`ring_ids() == declared` whenever `from_ids` built it). Both ends use the same rule, so the masks compare equal.
2. **`None` when the node floor is 0 OR any declared id's newest file is missing.** Floor 0 means nothing has snapshotted (the joiner is served by journal replay from 0 — spec §14.3's "moot" case). A missing file is the sharper one: the set must be *complete*, because the receiver adopts the floor only when every declared id has landed. Shipping a partial set would open a session that can never complete, the joiner would sit below a floor it can never adopt, and the leader's one session slot would be held for 30 s at a time. Refusing keeps it an overrun, the peer re-NAKs, and the next attempt sees the file.

- [ ] **Step 1: Write the failing test**

In `uc2_node/tests/learner.rs`, add after the existing join test (line 455) — read that test end to end first; this one mirrors its shape and adds two real FSMs on both sides:

```rust
/// A snapshot-capable RAW state machine (bytes in, bytes out — no serde, so the
/// test can submit plain byte payloads through `Node::submit` exactly as the
/// single-FSM join test above does). `freeze` pins `(total, last_applied)`.
#[derive(Default)]
struct SumSm {
    total: u64,
    last: Option<u64>,
}

impl uc2_service::RawStateMachine for SumSm {
    fn apply(&mut self, position: u64, cmd: &[u8], out: &mut Vec<u8>) {
        if cmd.len() >= 8 {
            self.total = self.total.wrapping_add(u64::from_le_bytes(cmd[..8].try_into().unwrap()));
        }
        self.last = Some(position);
        out.extend_from_slice(&self.total.to_le_bytes());
    }
    fn query(&self, _q: &[u8], out: &mut Vec<u8>) {
        out.extend_from_slice(&self.total.to_le_bytes());
    }
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
}

impl uc2_service::SnapshotStateMachine for SumSm {
    type SnapshotHandle = Vec<u8>;
    fn freeze(&self) -> Result<(Vec<u8>, u64), uc2_service::SnapshotError> {
        let pos = self.last.unwrap_or(0);
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.total.to_le_bytes());
        buf.extend_from_slice(&pos.to_le_bytes());
        Ok((buf, pos))
    }
    fn stream_snapshot(
        handle: Vec<u8>,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc2_service::SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
    }
    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc2_service::SnapshotError> {
        let mut buf = Vec::new();
        src.read_to_end(&mut buf)?;
        assert!(buf.len() >= 16, "a SumSm artifact is 16 bytes");
        self.total = u64::from_le_bytes(buf[..8].try_into().unwrap());
        self.last = Some(position);
        Ok(position)
    }
}

fn start_sum_service(dir: &Path, app: &str, id: u8) -> uc2_service::Service<SumSm> {
    let cfg = uc2_service::ServiceConfig::new(dir, app)
        .service_id(id)
        .snapshot_policy(uc2_service::SnapshotPolicy { interval_bytes: 256 * 1024 });
    uc2_service::ServiceBuilder::new(cfg, SumSm::default())
        .start_with_snapshots()
        .expect("service start")
}

/// M14c (spec §7.3/§14.3): a fresh learner joins a PURGED **two-FSM** leader.
/// One session carries BOTH artifacts (one `SNAP_BEGIN` per declared id, chunk
/// offsets stream-global); the learner writes each to `snapshots/<id>/`, adopts
/// the floor only once both landed, and each of its FSMs installs its OWN
/// artifact and tail-replays. The first test anywhere that combines two FSMs
/// with a below-floor join.
#[test]
fn fresh_learner_joins_a_purged_two_fsm_leader_and_both_fsms_converge() {
    let _g = serialize();
    let dir = tempfile::Builder::new()
        .prefix("uc2-learner-2fsm-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    const SEG: u64 = 64 * 1024;
    let app = "learner-join-2fsm";

    let v_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let l_sock = UdpSocket::bind("127.0.0.1:0").unwrap();
    let v_addr = v_sock.local_addr().unwrap();
    let l_addr = l_sock.local_addr().unwrap();
    let members = vec![(0u32, v_addr)];
    let learners = vec![(1u32, l_addr)];

    let cfg = |id: NodeId, sock_addr: SocketAddr, d: PathBuf| NodeConfig {
        id,
        members: members.clone(),
        learners: learners.clone(),
        bind: sock_addr,
        instance_dir: d,
        app_id: app.into(),
        buffer_bytes: 1 << 18, // small ring: the learner's NAK from 0 falls below it
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 50_000_000,
        election_timeout_max_ns: 100_000_000,
        seed: 0xC0FFEE ^ id as u64,
        faults: FaultConfig::default(),
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 4096 },
        journal_segment_bytes: SEG,
        crypto: uc2_node::CryptoConfig::Disabled,
        services: uc2_node::ServicesConfig::from_ids(&[0, 1], None).unwrap(),
    };

    let v_dir = dir.path().join("v0");
    let voter = Node::start_with_socket(cfg(0, v_addr, v_dir.clone()), v_sock).expect("start voter");
    let _v0 = start_sum_service(&v_dir, app, 0);
    let _v1 = start_sum_service(&v_dir, app, 1);
    await_until(30, "voter serves", || voter.can_serve());

    // Drive well past one snapshot interval per FSM so both slots publish a
    // position and the node floor (their min) leaves the journal's first
    // segment behind.
    for i in 0u64..24000 {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        loop {
            match voter.submit(p.clone()) {
                Ok(()) => break,
                Err(_) => std::thread::yield_now(),
            }
        }
    }
    await_until(30, "voter quiesced", || {
        let c = voter.counters();
        let a = c.append.load_acquire();
        a > 0 && c.commit.load_acquire() == a && c.durable.load_acquire() == a
    });

    let v_cnc = CncPage::open_file(&v_dir.join("cnc2.dat"), app).expect("open voter cnc");
    await_until(30, "both FSMs published a snapshot", || {
        v_cnc.service_slot(0).snapshot_pos.load_acquire() > SEG
            && v_cnc.service_slot(1).snapshot_pos.load_acquire() > SEG
    });
    await_until(30, "voter purged its prefix", || voter.archive_first_base() > 0);
    let first_base = voter.archive_first_base();
    let frontier = voter.counters().append.load_acquire();
    let commit = voter.counters().commit.load_acquire();

    // A FRESH learner joins with no prior state — and with its own two FSMs.
    let l_dir = dir.path().join("l1");
    let learner =
        Node::start_with_socket(cfg(1, l_addr, l_dir.clone()), l_sock).expect("start learner");
    let _l0 = start_sum_service(&l_dir, app, 0);
    let _l1 = start_sum_service(&l_dir, app, 1);

    await_until(60, "learner caught up across the purged prefix", || {
        learner.counters().durable.load_acquire() >= frontier
            && learner.counters().commit.load_acquire() >= frontier
    });
    assert!(
        learner.archive_first_base() >= first_base,
        "the learner must have adopted the shipped snapshot floor, not replayed from 0"
    );

    // Both artifacts landed, each in its OWN directory.
    for id in [0u8, 1] {
        let d = l_dir.join("snapshots").join(id.to_string());
        let installed: Vec<_> = std::fs::read_dir(&d)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                let n = e.file_name();
                let n = n.to_string_lossy();
                n.starts_with("snap-") && n.ends_with(".ultsnap")
            })
            .collect();
        assert!(!installed.is_empty(), "learner {d:?} holds no installed artifact");
    }

    // And both learner FSMs reached the leader's commit — each installed its own
    // artifact and tail-replayed the retained window.
    let l_cnc = CncPage::open_file(&l_dir.join("cnc2.dat"), app).expect("open learner cnc");
    await_until(60, "both learner FSMs applied to the leader's commit", || {
        l_cnc.service_slot(0).applied.load_acquire() >= commit
            && l_cnc.service_slot(1).applied.load_acquire() >= commit
    });
    assert_eq!(
        learner.snapshot_session_refusals(),
        (0, 0),
        "matching declared sets and a 0.6.0 peer: neither refusal may fire"
    );
    assert!(!learner.is_leader(), "a learner never leads");
}
```

Add the imports this needs at the top of `learner.rs`: `use std::path::Path;` (the file already imports `PathBuf`) and nothing else — `uc2_service` items are named through their full paths above. `uc2_service` is already a dev-dependency of `uc2_node` (`uc2_node/tests/services.rs:14` imports it).

- [ ] **Step 2: Run to verify it fails**

Run: `CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo test -p uc2_node --test learner`
Expected: compile error — `no method named snapshot_session_refusals found for struct Node`. (After Step 4 it will then fail at runtime on the two-artifact assertions until the wiring lands, because the node's source closure still offers one artifact.)

- [ ] **Step 3: Add the two small accessors**

`uc2_node/src/services.rs`, beside `ring_ids` (after line 93):

```rust
    /// [`ring_ids`](Self::ring_ids) as a bitmask — what the snapshot session
    /// puts on the wire and compares (M14c, spec §14.3). Identical to
    /// [`declared`](Self::declared) for any node built by `from_ids`; `{0}` for
    /// a `none_for_tests` harness node, matching M14a's standing rule that a
    /// page whose `services_declared` reads 0 is treated as `{0}`.
    pub fn ring_mask(&self) -> u64 {
        if self.declared == 0 { 1 } else { self.declared }
    }
```

`uc2_node/src/ipc.rs`, beside `snapshot_dir_for` (after line 95):

```rust
    /// M14c: the snapshots ROOT (`snapshots/`), which holds one `<id>/`
    /// directory per declared FSM. The inbound snapshot intake is wired to this
    /// and picks the per-id subdirectory from each `SNAP_BEGIN`.
    pub fn snapshot_root(&self) -> PathBuf {
        self.root.join("snapshots")
    }
```

- [ ] **Step 4: Wire the node**

`uc2_node/src/node.rs:906-938` — replace the `snap_dir` block and the source closure:

```rust
        // M6 Task 6 / M14c: snapshot session wiring. `snap_root` holds one
        // `snapshots/<id>/` per declared FSM (created in `create_rings`);
        // `incoming_snapshot` is the node-internal signal the receiver raises on
        // a COMPLETED inbound transfer — with N artifacts that is the minimum
        // over the set, so no FSM is ever stranded below an adopted floor.
        let snap_root = instance.snapshot_root();
        let _ = std::fs::create_dir_all(&snap_root);
```
(keep `incoming_snapshot`, `incoming_snapshot_config` and `prime_generation` exactly as they are)

```rust
        // Offer ONLY files at or above the node's durable floor: a session ships
        // fully-published artifacts (rename-atomic, and each id's marker is
        // written only after its own rename — `uc2_service::builder_agent`).
        let src_cnc = Arc::clone(&cnc);
        let src_root = snap_root.clone();
        let src_services = cfg.services;
        // M7 Task 6: the same cell `Action::ConfigAdopted`'s exec arm refreshes —
        // ships whatever config is CURRENT at the moment a peer's NAK opens a
        // session, never a boot-time snapshot of it.
        let src_config_bytes = Arc::clone(&config_bytes);
        sender.set_snapshot_source(Arc::new(move || {
            let floor = src_cnc.snapshots().node_snapshot_floor.load_acquire();
            if floor == 0 {
                return None; // nothing has snapshotted: the joiner replays from 0
            }
            let mut artifacts = Vec::new();
            for id in src_services.ring_ids() {
                let pos = src_cnc.service_slot(id as usize).snapshot_pos.load_acquire();
                if pos == 0 {
                    return None;
                }
                let path = src_root.join(id.to_string()).join(format!("snap-{pos}.ultsnap"));
                // A declared id whose newest artifact is missing (a retention
                // race, a hand-edited dir) makes the SET incomplete — and the
                // receiver adopts the floor only on a complete set, so a partial
                // ship would strand the joiner below a floor it can never adopt
                // AND hold the leader's single session slot for 30 s. Refuse:
                // the NAK stays an overrun, the peer re-NAKs, the next attempt
                // sees the file.
                let len = std::fs::metadata(&path).ok()?.len();
                artifacts.push(SnapArtifact { service_id: id, snapshot_pos: pos, path, len });
            }
            if artifacts.is_empty() {
                return None;
            }
            Some(SnapshotSet {
                services_declared: src_services.ring_mask(),
                config: src_config_bytes.lock().unwrap().clone(),
                artifacts,
            })
        }));
```

`uc2_node/src/node.rs:980-983` →

```rust
        receiver.set_snapshot_intake(
            snap_root.clone(),
            cfg.services.ring_mask(),
            Some((Arc::clone(&incoming_snapshot), Arc::clone(&incoming_snapshot_config))),
        );
```

Import `use uc2_net::sender::{SnapArtifact, SnapshotSet};` alongside the existing `uc2_net::sender` imports at the top of `node.rs`.

Add the accessor beside `crypto_stats` (after line 1458):

```rust
    /// M14c (spec §14.3, §9): the two named snapshot-session refusals this node
    /// counted — `(peer wire 0.5.0, declared-set mismatch)`. Both drop the
    /// session; the follower keeps NAKing, so a non-zero value means a joiner is
    /// stuck and the fleet is mixed-version or mis-declared. The observability
    /// workstream exports these; this accessor is the single source it reads.
    pub fn snapshot_session_refusals(&self) -> (u64, u64) {
        (
            self.route_drops.snap_refused_legacy_peer.load(Ordering::Relaxed),
            self.route_drops.snap_refused_declared_mismatch.load(Ordering::Relaxed),
        )
    }
```

- [ ] **Step 5: Fix the existing single-FSM join test**

`uc2_node/tests/learner.rs:452` currently publishes the floor by storing only the page-1 aggregate:

```rust
    cnc.snapshots().service_snapshot_pos.store_release(floor);
```

The new source closure reads each id's **slot**, so add the slot write the real service would have done (a `none_for_tests` node never aggregates, which is why the page-1 store stays):

```rust
    // M14c: the source closure ships each declared id's own newest artifact, so
    // the test must publish the SLOT the service owns as well as the page-1
    // aggregate the node would normally derive from it (a `none_for_tests` node
    // publishes no aggregates — `publish_service_mins` returns early).
    cnc.service_slot(0).snapshot_pos.store_release(floor);
    cnc.snapshots().service_snapshot_pos.store_release(floor);
```

That test already writes its artifact to `v_dir/snapshots/0/` (line 449) and the learner's intake now lands it at `l_dir/snapshots/0/`, so nothing else in it moves.

- [ ] **Step 6: Run**

```bash
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo test -p uc2_node --test learner
```
Expected: both join tests pass. If the two-FSM one fails, the discriminating checks, in order: (a) `learner.snapshot_session_refusals()` non-zero → the masks disagree (`ring_mask` vs what the sender put on the wire); (b) `l_dir/snapshots/1/` empty while `0/` is populated → the second BEGIN never landed (Task 4's resend); (c) both directories populated but a learner FSM's `applied` stuck → the service-side install, i.e. `replay_into`'s gap guard (`uc2_service/src/replay.rs:87-100`) not finding a covering artifact — check that the artifact position is `>= ` the learner's `archive_first_base` (it must be: the floor is the min over the set, and each id's own position is `>= ` that min).

Then the fuller sweep this task touches:
```bash
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo test -p uc2_node --test services --test purge_safety --test lifecycle
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo test -p uc2_net
```

- [ ] **Step 7: Flag-day docs**

`docs/how-to/upgrade-a-cluster.md` — insert before `## Where to go next` (line 238), in the shape of the two sections above it:

```markdown
## Wire change in 2.8.0: `SNAP_BEGIN` carries every FSM's snapshot (0.6.0)

M14c moves the node-to-node wire from `0.5.0` to `0.6.0`. One datagram
changes: `SNAP_BEGIN`, which opens a snapshot session. Its body grew from 26
to 34 fixed bytes and now names *which* FSM's artifact is being shipped
(`service_id`), the sender's declared FSM set (`services_declared`), and a
layout discriminator — because a session now carries **one artifact per
declared FSM**, not one artifact. `DATA`, `NAK`, `APPEND_POSITION`,
`TERM_MAP`, the 16-byte header and every admin datagram are byte-identical to
`0.5.0`.

**This is a whole-cluster flag day, on the same terms as every prior one.**
A mixed `0.5.0`/`0.6.0` cluster replicates and elects normally — which is
precisely why it is dangerous: the damage is confined to snapshot sessions,
so it surfaces later, when a learner joins or a node falls below the purge
floor, not at upgrade time. A `0.5.0` receiver handed a `0.6.0` `SNAP_BEGIN`
misreads its config length and drops or mis-adopts the carried membership; a
`0.6.0` receiver refuses the session by name. Nothing in the header enforces
this (`version::CURRENT` is documentary and has no caller on any receive
path) — the rule is operational: **stop every node, swap, start every node.**
`scripts/uc2_flag_day.sh` does exactly that and needs no new flags.

Two named, counted refusals on the receiving node tell you a cluster is
mixed or mis-declared instead of leaving a joiner silently stuck:

| refusal | meaning | fix |
|---|---|---|
| `peer wire 0.5.0` | a `SNAP_BEGIN` arrived whose layout byte is not the `0.6.0` value | finish the flag day: some node is still on `0.5.0` |
| `declared-set mismatch` | the sender's `[services] ids` differ from this node's | make `[services] ids` identical on every node, then restart the odd one out |

Both drop the session; the joining node keeps NAKing, so the cluster is
stalled-but-safe until the mismatch is fixed — never half-installed.

The snapshot **directory layout is unchanged from 2.8.0's own layout**:
artifacts already live in `snapshots/<service-id>/` (M14a). No migration, no
rollback step beyond restarting the old binaries together.
```

`docs/reference/semver-policy.md:136` → `` currently `0.6.0` — see [wire protocol](wire-protocol.md)). ``

- [ ] **Step 8: Clippy + commit**

```bash
CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a cargo clippy --workspace --all-targets -- -D warnings
git add uc2_node/src/services.rs uc2_node/src/ipc.rs uc2_node/src/node.rs \
        uc2_node/tests/learner.rs \
        docs/how-to/upgrade-a-cluster.md docs/reference/semver-policy.md
git commit -m "feat(node): ship one artifact per declared FSM in a snapshot session; two-FSM learner join; 0.6.0 flag-day docs"
```

---

### Task 7: The labelled per-FSM metric families

**Files:**
- Modify `uc2_node/src/obs/metrics.rs`:
  - imports (15–23): add `uc2_log::cnc::unpack_service_status`, add `CNC_MAX_SERVICES` to the `uc_protocol::v2::cnc` list
  - `CONTRACT_SERIES` (34–100): remove the four scattered `uc2_service_*` entries (53, 54, 57, 65), insert a nine-entry service block after `"uc2_commit_bytes"` (52)
  - new helpers after `push_labeled` (144–152)
  - `render_prometheus` (161): call the new block where the service band is today (269–311), move `let now = now_unix_ns();` (348) up above it, delete the old `uc2_service_heartbeat_age_seconds` push (357–362)
  - test module (626–774): fixture `synthetic_sources` (637–676) declares FSM 0; three new tests after `peer_slots_export_only_occupied_with_labels` (731–739)
- Test: `uc2_node/src/obs/metrics.rs`'s own `mod tests` (626) — `every_contract_series_is_present` (680–686) and `series_present` (696–700) are reused unchanged.
- **Not modified:** `uc2_node/examples/m10_gate.rs:294–320` iterates `CONTRACT_SERIES` and prints `CONTRACT_SERIES.len()`, so the coverage gate row picks the five new families up for free — verify by reading, change nothing.

**Interfaces:**

Consumes (all verified in the tree):
- `uc2_log::cnc::CncPage::services_declared(&self) -> u64` (`uc2_log/src/cnc.rs:517`), `fsm_lag_bytes(&self) -> u64` (532), `service_slot(&self, i: usize) -> &ServiceSlot` (506, panics on `i >= 8`).
- `uc2_log::cnc::ServiceSlot` (`uc2_log/src/cnc.rs:170–179`): `status, applied, epoch, output_completed, snapshot_pos, heartbeat_ns, lag_waits, reserved`, each a `PaddedAtomicU64` with `load_acquire()`.
- `uc2_log::cnc::unpack_service_status(v: u64) -> (u8, bool, u32)` (`uc2_log/src/cnc.rs:205`) — `(service_id, attached, incarnation)`.
- `uc_protocol::v2::cnc::CNC_MAX_SERVICES: usize = 8` (`uc_protocol/src/v2/cnc.rs:268`).
- `push_family_header(out, name, help, ty)` (`metrics.rs:102`), `push_gauge` (115), `push_gauge_f64` (123), `push_counter` (131), `push_labeled(out, name, help, ty, &[(String, u64)])` (144 — five params, the peer band's mechanism at 364–406).
- `now_unix_ns() -> u64` (`metrics.rs:26`).

Produces (later tasks depend on these exact names):
- Nine metric families, rendered in this order: `uc2_service_applied_bytes`, `uc2_service_epoch`, `uc2_service_snapshot_pos_bytes`, `uc2_service_heartbeat_age_seconds` (each = the existing unlabeled aggregate **plus** one `service="<id>"` sample per declared id, in ONE family block), then `uc2_service_attached`, `uc2_service_lag_bytes`, `uc2_service_lag_waits_total` (labelled only), then `uc2_services_declared` and `uc2_fsm_lag_bytes` (plain gauges).
- `struct ServiceRow` + `fn service_rows(&ObsSources, commit: u64, now: u64) -> Vec<ServiceRow>` and `fn push_service_families(out: &mut String, s: &ObsSources, commit: u64, now: u64)`, private to `metrics.rs`.
- `CONTRACT_SERIES.len() == 70` (was 65).

**Design notes the implementer must not re-decide:**

1. **One family block, aggregate and labels together.** `uc2_service_applied_bytes` and `uc2_service_applied_bytes{service="0"}` are samples of the *same* Prometheus family; a scrape that emits two `# HELP` lines for one name is rejected by the Prometheus text parser. So the aggregate and its labelled twins share a single header, emitted by `push_gauge_with_services`. Consequence for queries (Task 8/Task 10 both depend on it): `sum(uc2_service_applied_bytes)` double counts, so "the aggregate" is `{service=""}` and "per FSM" is `{service!=""}`.
2. **Rows come from the declared bitmask on the page, not from occupied slots.** Unlike the peer band (which skips `id_and_role == 0`), a declared id renders even when nothing ever attached: `uc2_service_attached{service="1"} 0` is the whole point — you cannot alert on a series that is absent. A harness page (`services_declared == 0`, `ServicesConfig::none_for_tests`) renders the headers with no labelled samples, which is exactly what `push_labeled` already does for zero peers.
3. `uc2_service_epoch`'s **aggregate stays FSM 0's epoch** (what M14a made it, `metrics.rs:277–282`); only its labelled twins are per-id. It is not a `min`.

- [ ] **Step 1: RED — the per-FSM sample test**

Add to `uc2_node/src/obs/metrics.rs`'s `mod tests`, after `peer_slots_export_only_occupied_with_labels` (ends 739). Extend the test-module import at 630 to `use uc2_log::cnc::{CncMeta, CncPage, pack_id_and_role, pack_service_status};`.

```rust
    /// M14c (spec §9): one labelled sample per DECLARED id — including an id
    /// nothing has ever attached to, which must read `attached 0` rather
    /// than vanish (an absent series is not alertable).
    #[test]
    fn per_fsm_families_render_one_sample_per_declared_id() {
        let s = synthetic_sources();
        s.cnc.store_services_declared(0b101); // ids 0 and 2; id 1 NOT declared
        s.cnc.store_fsm_lag_bytes(65_536);
        s.cnc.counters().commit.store_release(10_000);
        let s0 = s.cnc.service_slot(0);
        s0.status.store_release(pack_service_status(0, true, 3));
        s0.applied.store_release(9_000);
        s0.epoch.store_release(7);
        s0.snapshot_pos.store_release(4_096);
        s0.lag_waits.store_release(12);
        // id 2: declared, never attached — every field stays zero.
        let text = render_prometheus(&s);
        assert!(text.contains(r#"uc2_service_applied_bytes{service="0"} 9000"#), "{text}");
        assert!(text.contains(r#"uc2_service_epoch{service="0"} 7"#), "{text}");
        assert!(text.contains(r#"uc2_service_snapshot_pos_bytes{service="0"} 4096"#), "{text}");
        assert!(text.contains(r#"uc2_service_attached{service="0"} 1"#), "{text}");
        assert!(text.contains(r#"uc2_service_attached{service="2"} 0"#), "{text}");
        assert!(text.contains(r#"uc2_service_lag_bytes{service="0"} 1000"#), "{text}");
        assert!(text.contains(r#"uc2_service_lag_bytes{service="2"} 10000"#), "{text}");
        assert!(text.contains(r#"uc2_service_lag_waits_total{service="0"} 12"#), "{text}");
        assert!(text.contains(r#"uc2_service_heartbeat_age_seconds{service="2"}"#), "{text}");
        assert!(text.contains("\nuc2_services_declared 5\n"), "{text}");
        assert!(text.contains("\nuc2_fsm_lag_bytes 65536\n"), "{text}");
        assert!(!text.contains(r#"service="1""#), "id 1 is not declared: {text}");
    }

    /// The four M10 aggregates keep their bare names (now "slowest FSM") and
    /// share ONE family header with their labelled twins — two `# HELP`
    /// lines for one family is a scrape Prometheus rejects.
    #[test]
    fn the_aggregates_keep_their_bare_names_in_one_family_block() {
        let s = synthetic_sources();
        s.cnc.store_services_declared(0b11);
        s.cnc.service().service_applied.store_release(1_234);
        let text = render_prometheus(&s);
        assert!(text.contains("\nuc2_service_applied_bytes 1234\n"), "{text}");
        for name in [
            "uc2_service_applied_bytes",
            "uc2_service_epoch",
            "uc2_service_snapshot_pos_bytes",
            "uc2_service_heartbeat_age_seconds",
        ] {
            assert_eq!(
                text.matches(&format!("# TYPE {name} ")).count(),
                1,
                "exactly one family header for {name}: {text}"
            );
        }
    }

    /// `commit - applied` saturates: a slot that reports past this scrape's
    /// commit sample (two independent atomics, read microseconds apart)
    /// reads 0, never a wrapped 18-exabyte lag.
    #[test]
    fn service_lag_bytes_saturates_when_applied_is_past_commit() {
        let s = synthetic_sources();
        s.cnc.store_services_declared(0b1);
        s.cnc.counters().commit.store_release(500);
        s.cnc.service_slot(0).applied.store_release(900);
        assert!(
            render_prometheus(&s).contains(r#"uc2_service_lag_bytes{service="0"} 0"#),
            "{}",
            render_prometheus(&s)
        );
    }
```

```bash
export CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a
cargo test -p uc2_node --lib obs::metrics 2>&1 | tail -20
```
Expected: three failures, each `assertion failed: text.contains(...)` — the strings `uc2_service_applied_bytes{service="0"} 9000`, `uc2_services_declared`, `uc2_service_lag_bytes{service="0"} 0` are nowhere in the render (only `uc2_service_applied_bytes 0` is). `the_aggregates_keep_their_bare_names_in_one_family_block` passes already (one header each, no twins yet) — that is fine, it is the regression guard for step 3.

- [ ] **Step 2: the row gatherer and the three push helpers**

In `metrics.rs`, after `push_labeled` (ends 152), add:

```rust
/// M14c (spec §9): one row per DECLARED service id, gathered in a single
/// pass over page 2 so every per-FSM family renders from the same snapshot.
///
/// Unlike the peer band (which skips unoccupied slots), a declared id gets a
/// row even when nothing has ever attached to it: an absent FSM must show up
/// as `uc2_service_attached{service="k"} 0`, not as a missing series —
/// `Uc2ServiceAbsent` cannot alert on a series that is not there. A harness
/// page (`services_declared == 0`) yields no rows, and the families then
/// render as headers alone, exactly like a node with no occupied peer slots.
struct ServiceRow {
    /// Pre-formatted label body, no braces: `service="0"`.
    labels: String,
    attached: u64,
    applied: u64,
    epoch: u64,
    snapshot_pos: u64,
    /// `commit - applied`, saturating: the two counters are independent
    /// atomics read microseconds apart, so `applied > commit` is a normal
    /// racy snapshot, not an error.
    lag_bytes: u64,
    lag_waits: u64,
    heartbeat_age: f64,
}

fn service_rows(s: &ObsSources, commit: u64, now: u64) -> Vec<ServiceRow> {
    let declared = s.cnc.services_declared();
    let mut rows = Vec::new();
    for id in 0..CNC_MAX_SERVICES as u8 {
        if declared & (1u64 << id) == 0 {
            continue;
        }
        let slot = s.cnc.service_slot(id as usize);
        let (_, attached, _) = unpack_service_status(slot.status.load_acquire());
        let applied = slot.applied.load_acquire();
        let hb = slot.heartbeat_ns.load_acquire();
        rows.push(ServiceRow {
            labels: format!("service=\"{id}\""),
            attached: attached as u64,
            applied,
            epoch: slot.epoch.load_acquire(),
            snapshot_pos: slot.snapshot_pos.load_acquire(),
            lag_bytes: commit.saturating_sub(applied),
            lag_waits: slot.lag_waits.load_acquire(),
            heartbeat_age: now.saturating_sub(hb) as f64 / 1e9,
        });
    }
    rows
}

/// One family block carrying BOTH the unlabeled aggregate sample and one
/// labeled sample per declared FSM. They are the same metric FAMILY, so they
/// must share a single `# HELP`/`# TYPE` pair — a second header for a name
/// already seen in the same scrape is a parse error on Prometheus's side.
/// The query-side consequence, documented in `monitor-a-cluster.md`:
/// `sum(<name>)` double counts, so "the aggregate" is `<name>{service=""}`
/// and "per FSM" is `<name>{service!=""}`.
fn push_gauge_with_services(
    out: &mut String,
    name: &str,
    help: &str,
    aggregate: u64,
    rows: &[ServiceRow],
    pick: impl Fn(&ServiceRow) -> u64,
) {
    push_family_header(out, name, help, "gauge");
    out.push_str(name);
    out.push(' ');
    out.push_str(&aggregate.to_string());
    out.push('\n');
    for r in rows {
        out.push_str(name);
        out.push('{');
        out.push_str(&r.labels);
        out.push_str("} ");
        out.push_str(&pick(r).to_string());
        out.push('\n');
    }
}

/// [`push_gauge_with_services`] for an `f64` family (the heartbeat ages).
fn push_gauge_f64_with_services(
    out: &mut String,
    name: &str,
    help: &str,
    aggregate: f64,
    rows: &[ServiceRow],
    pick: impl Fn(&ServiceRow) -> f64,
) {
    push_family_header(out, name, help, "gauge");
    out.push_str(name);
    out.push(' ');
    out.push_str(&aggregate.to_string());
    out.push('\n');
    for r in rows {
        out.push_str(name);
        out.push('{');
        out.push_str(&r.labels);
        out.push_str("} ");
        out.push_str(&pick(r).to_string());
        out.push('\n');
    }
}

/// A per-FSM family with no aggregate twin (`attached`, `lag_bytes`,
/// `lag_waits_total`): header plus one labeled sample per declared id.
fn push_service_labeled(
    out: &mut String,
    name: &str,
    help: &str,
    ty: &str,
    rows: &[ServiceRow],
    pick: impl Fn(&ServiceRow) -> u64,
) {
    let samples: Vec<(String, u64)> =
        rows.iter().map(|r| (r.labels.clone(), pick(r))).collect();
    push_labeled(out, name, help, ty, &samples);
}
```

Add the imports (15–23):

```rust
use uc2_log::cnc::unpack_service_status;
use uc_protocol::v2::cnc::{
    CNC_MAX_PEER_SLOTS, CNC_MAX_SERVICES, CNC_PEER_ROLE_LEARNER, CNC_PEER_ROLE_VOTER,
    NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER,
};
```

- [ ] **Step 3: `push_service_families` and the render-order move**

Still in `metrics.rs`, after `push_service_labeled`:

```rust
/// M14c (spec §9): every per-FSM family, aggregates included, as one
/// contiguous block. `commit` and `now` are this scrape's single samples,
/// threaded in so the block is consistent with the rest of the render.
///
/// The aggregates are page 1's `min` over the declared ids, published once
/// per cycle by the node (`crate::services::service_mins` /
/// `Consensus::publish_service_mins`) — they now mean "the slowest FSM".
/// `uc2_service_epoch`'s aggregate is the exception: it is FSM 0's epoch
/// (M14a retired page 1's `service_epoch`), not a min.
fn push_service_families(out: &mut String, s: &ObsSources, commit: u64, now: u64) {
    let rows = service_rows(s, commit, now);
    let service = s.cnc.service();
    let snapshots = s.cnc.snapshots();
    let status = s.cnc.status();

    push_gauge_with_services(
        out,
        "uc2_service_applied_bytes",
        "Position the service state machine has applied through (unlabeled = the SLOWEST declared FSM; one labeled sample per declared FSM).",
        service.service_applied.load_acquire(),
        &rows,
        |r| r.applied,
    );
    push_gauge_with_services(
        out,
        "uc2_service_epoch",
        "Service incarnation counter, bumped each attach (unlabeled = FSM 0's, the M10 series; one labeled sample per declared FSM).",
        s.cnc.service_slot(0).epoch.load_acquire(),
        &rows,
        |r| r.epoch,
    );
    push_gauge_with_services(
        out,
        "uc2_service_snapshot_pos_bytes",
        "Position of the newest complete service-built snapshot, 0 = none (unlabeled = the min over declared FSMs, which is the purge floor).",
        snapshots.service_snapshot_pos.load_acquire(),
        &rows,
        |r| r.snapshot_pos,
    );
    push_gauge_f64_with_services(
        out,
        "uc2_service_heartbeat_age_seconds",
        "Seconds since a service heartbeat was last stamped, unlabeled = the stalest declared FSM (a never-written heartbeat reads as a huge age, by design).",
        now.saturating_sub(status.service_heartbeat_ns.load_acquire()) as f64 / 1e9,
        &rows,
        |r| r.heartbeat_age,
    );
    push_service_labeled(
        out,
        "uc2_service_attached",
        "1 if this declared FSM's slot has the ATTACHED bit set. A declared FSM that never started reads 0 here and holds admission closed.",
        "gauge",
        &rows,
        |r| r.attached,
    );
    push_service_labeled(
        out,
        "uc2_service_lag_bytes",
        "commit - this FSM's applied position (saturating). Pinned at uc2_fsm_lag_bytes means this FSM is pacing the cluster.",
        "gauge",
        &rows,
        |r| r.lag_bytes,
    );
    push_service_labeled(
        out,
        "uc2_service_lag_waits_total",
        "Times this FSM's apply loop waited at the lag barrier for a sibling.",
        "counter",
        &rows,
        |r| r.lag_waits,
    );
    push_gauge(
        out,
        "uc2_services_declared",
        "Bitmask of declared service ids (bit k = id k). Must match cluster-wide; a mismatch refuses snapshot sessions.",
        s.cnc.services_declared(),
    );
    push_gauge(
        out,
        "uc2_fsm_lag_bytes",
        "The configured FSM lag bound in bytes; 0 means lockstep.",
        s.cnc.fsm_lag_bytes(),
    );
}
```

In `render_prometheus`: keep `let service = s.cnc.service();` and `let service_applied = service.service_applied.load_acquire();` (269–270, `service_applied` still feeds `uc2_apply_lag_bytes` at 331), then **replace** the `uc2_service_applied_bytes` push (271–276) and the `uc2_service_epoch` push (277–282) with:

```rust
    // M14c (spec §9): the whole per-FSM band — the four M10 aggregates (now
    // "slowest FSM") each with their labelled twins, plus attached/lag/
    // lag_waits/declared/fsm_lag. `now` moves up from the heartbeat block
    // below so one clock sample covers both.
    let now = now_unix_ns();
    push_service_families(&mut out, s, commit, now);
```

Then delete the `uc2_service_snapshot_pos_bytes` push (297–305, keeping `let snapshots = s.cnc.snapshots();` at 297 and the two pushes that follow it), delete `let now = now_unix_ns();` (348) and `let service_hb = status.service_heartbeat_ns.load_acquire();` (350, now read inside `push_service_families`), and delete the `uc2_service_heartbeat_age_seconds` push (357–362). `uc2_node_heartbeat_age_seconds` (351–356) stays exactly as it is, now reading the moved-up `now`.

Finally, `CONTRACT_SERIES` (34–100): delete `"uc2_service_applied_bytes"` (53), `"uc2_service_epoch"` (54), `"uc2_service_snapshot_pos_bytes"` (57) and `"uc2_service_heartbeat_age_seconds"` (65), and insert after `"uc2_commit_bytes"` (52):

```rust
    // M14c (spec §9): the per-FSM band. The first four are the M10
    // aggregates, which now also carry one `service="<id>"` sample per
    // declared id in the SAME family block.
    "uc2_service_applied_bytes",
    "uc2_service_epoch",
    "uc2_service_snapshot_pos_bytes",
    "uc2_service_heartbeat_age_seconds",
    "uc2_service_attached",
    "uc2_service_lag_bytes",
    "uc2_service_lag_waits_total",
    "uc2_services_declared",
    "uc2_fsm_lag_bytes",
```

- [ ] **Step 4: the fixture declares FSM 0, then GREEN**

In `synthetic_sources` (637), after `cnc.store_free_disk_bytes(1);` (655):

```rust
        // M14c: declare FSM 0 and a lag bound so the per-service families
        // render at least one LABELED sample in the base fixture —
        // `every_contract_series_is_present` would otherwise be satisfied by
        // the bare family header alone (see `series_present`'s own doc on
        // why vacuous presence checks are the hazard here).
        cnc.store_services_declared(0b1);
        cnc.store_fsm_lag_bytes(1 << 20);
```

```bash
cargo test -p uc2_node --lib obs:: 2>&1 | tail -20
```
Expected: `test result: ok.` — the three new tests pass, and `every_contract_series_is_present` now covers 70 names (it reads `CONTRACT_SERIES` directly, so nothing to update). `derived_lags_saturate_and_saturation_divides` (718–730) still passes: it drives page 1's `service_applied`, which the aggregate still reads.

- [ ] **Step 5: prove the exporter's own contract test and the gate harness agree**

```bash
cargo test -p uc2_node --lib 2>&1 | tail -5
cargo test -p uc2_node --test obs_http 2>&1 | tail -5
cargo build -p uc2_node --release --example m10_gate 2>&1 | tail -3
```
Expected: all green; the example builds (it uses `CONTRACT_SERIES` and `CONTRACT_SERIES.len()`, both still in scope, so its coverage row now demands 70/70 with no source change).

- [ ] **Step 6: clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add uc2_node/src/obs/metrics.rs
git commit -m "feat(m14c): per-FSM metric families — service={id} twins for applied/epoch/snapshot_pos/heartbeat_age, plus attached/lag_bytes/lag_waits_total/services_declared/fsm_lag_bytes (CONTRACT_SERIES 65 -> 70)"
```

---


- [ ] **Step 12 (addendum, from the wire workstream's seam): the two snapshot-refusal counters**

Task 5 adds `snap_refused_legacy_peer` and `snap_refused_declared_mismatch` to `uc2_net::receiver::FollowerStats`, and `ObsSources.receiver: Arc<FollowerStats>` already exists (`uc2_node/src/obs/mod.rs:41`). Render them exactly the way `uc2_reports_unattested_total` is rendered (`metrics.rs:434-436`, `push_counter` on a relaxed load), directly after it:

```rust
    push_counter(
        &mut out,
        "uc2_snapshot_refused_legacy_peer_total",
        "Snapshot sessions refused because the sender's SNAP_BEGIN was a 0.5.0 body (too short or layout 0): the fleet is mixed-version; upgrade every node (spec §14.3).",
        s.receiver.snap_refused_legacy_peer.load(Ordering::Relaxed),
    );
    push_counter(
        &mut out,
        "uc2_snapshot_refused_declared_set_total",
        "Snapshot sessions refused because the sender's declared service set differs from this node's [services] ids — a joiner is stuck until the sets match (spec §8).",
        s.receiver.snap_refused_declared_mismatch.load(Ordering::Relaxed),
    );
```

Add both names to `CONTRACT_SERIES` beside `"uc2_reports_unattested_total"` (line 73). **`CONTRACT_SERIES.len()` becomes 72, not 70** — update the count this task's earlier step asserted (search the tests you added for `70`). Test, beside the per-FSM one:

```rust
    /// M14c (spec §14.3): the two named snapshot-session refusals render as
    /// counters off the receiver stats the node already shares with /metrics.
    #[test]
    fn snapshot_refusal_counters_render_from_receiver_stats() {
        let s = synthetic_sources();
        s.receiver.snap_refused_legacy_peer.fetch_add(3, Ordering::Relaxed);
        s.receiver.snap_refused_declared_mismatch.fetch_add(1, Ordering::Relaxed);
        let text = render_prometheus(&s);
        assert!(text.contains("uc2_snapshot_refused_legacy_peer_total 3\n"), "{text}");
        assert!(text.contains("uc2_snapshot_refused_declared_set_total 1\n"), "{text}");
    }
```

Run `cargo test -p uc2_node --lib obs::metrics` — expected PASS (fold this into the task's single commit; the `git add` list is unchanged).


### Task 8: The two alert rules, their `m10_alerts` scenarios, the adjudicator hookup, and the dashboard

**Files:**
- Modify `packaging/prometheus/uc2-alerts.yml` (82 lines, `groups: [{name: uc2, interval: 15s}]`): `Uc2ServiceWedged`'s expr (20–24) gains `{service=""}`; two new rules appended after `Uc2DiskLow` (78–82)
- Modify `uc2_node/examples/m10_alerts.rs` (1079 lines): `ALL_SCENARIOS` (52–64), `run_scenario`'s match (162–181), `make_config` (961–989) + a `spawn_cluster_with_services` beside `spawn_cluster` (994–1029), two new scenario functions after `scenario_service_wedged` (ends ~528)
- Modify `scripts/m10_alert_fire.sh` (540 lines): `RULE_META` (222–237), `build_Uc2ServiceWedged` (269–279), two new builders, `RULE_BUILDERS` (407–424)
- Modify `packaging/grafana/uc2-dashboard.json`: panel 9's target B (`"expr": "uc2_service_heartbeat_age_seconds"`, line 197) and four new panels appended after panel 10
- Test: `scripts/m10_alert_fire.sh` itself is the test (it shells `promtool test rules` per rule and exits nonzero on any FAIL); `promtool check rules` is the cheap pre-check. The repo keeps **no** checked-in promtool unit-test file — the per-rule YAML is generated into `$OUT/_promtool_tests/<Alert>.yml` by the script's Python step (`write_test_yaml`, 477–500). Do not add one.

**Interfaces:**

Consumes:
- Task 7's families: `uc2_service_attached{service}`, `uc2_service_lag_bytes{service}`, `uc2_fsm_lag_bytes` (plain).
- `uc2_node::{FsmLag, ServicesConfig}` — `ServicesConfig::from_ids(&[u8], Option<FsmLag>) -> Result<Self, String>` (`uc2_node/src/services.rs:44`), `FsmLag::Bounded(u64)` (19). Both are re-exported from the crate root (`uc2_node/tests/services.rs:13` imports them that way).
- `uc2_service::{ServiceBuilder, ServiceConfig, StateMachine}` — already imported by `m10_alerts.rs:45`; `ServiceConfig::new(dir, APP).service_id(id)` is the per-FSM form (`uc2_node/tests/services.rs:139`).
- `SeriesFile::record_round(&mut self, instance: &str, body: &str, families: &[&str])` (`m10_alerts.rs:207`), `scrape(SocketAddr) -> String` (277), `await_stable_leader(&[NodeH], u64) -> usize` (1059), `NodeH::{n, obs_addr, stop, instance_dir}` (906–945), `Disclosure` (184–190), `NoopSm` (470–481), `seed_for(usize) -> u64` (957).
- The script's `select(rows, name, filt)` (115–121) compares `r["labels"].get(k) == v`, so **`{"service": None}` selects the sample that has no `service` label** — no code change needed for that idiom.
- `add_hold_last(r, row, metric, for_secs)` (173–186), `total_for(for_secs, range_secs=0, margin=60)` (168), `new_rule(severity, labels_from)` (210).

Produces:
- Alerts `Uc2ServiceAbsent` (critical) and `Uc2ServicePinnedAtLagBound` (warning).
- Scenarios `service_absent` and `fsm_pinned`, each writing `<name>.series`.
- `spawn_cluster_with_services(scratch_root, label, n, admission_bytes, services) -> (TempDir, Vec<NodeH>)`.

**Why these two scenarios are constructible as REAL clusters (both `state: "real"`), and why they hold still:**
- *service_absent*: a node declaring `{0, 1}` with only FSM 0 attached publishes `services_declared = 0b11`, so `uc2_service_attached{service="1"}` renders `0` for the whole capture. It never changes — nothing can attach it.
- *fsm_pinned*: with `fsm_lag = Bounded(8 KiB)` and `admission_bytes = 256 KiB`, the FSM door (`append - min_applied <= fsm_lag`, `uc2_node/src/node.rs:394` + 3313–3320) is the binding one, and this node's own durable report is capped at `min_applied + fsm_lag` (`crate::services::report_ceiling`, `uc2_node/src/services.rs:206`). On a **single-node** cluster commit *is* that report, so under sustained load `commit - applied_1` settles at **exactly 8192** — the alert's `>=` (not `>`) is required by that arithmetic, and the captured series is a flat 8192 rather than a jittery value that `hold_last` might sample below the bound.
- *fsm_pinned* drives load with `Node::submit(vec![0u8; 64])` (the raw in-process path `scenario_leader_isolated` already uses) while typed state machines are attached. That is sound: the typed blanket impl decodes with `bincode::serde::decode_from_slice::<S::Command, _>` (`uc2_service/src/traits.rs:72`), which for `Command = ()` consumes zero bytes and ignores the remainder — no fail-stop, and the response encodes back to zero bytes.
- **Neither scenario may call `wait_ready`** (`m10_alerts.rs:315`): `/readyz` returns 503 while page 1's service heartbeat is stale (`uc2_node/src/obs/http.rs:256–259`), and page 1's heartbeat is the `min` over declared FSMs — an absent or sleeping FSM 1 holds it at 0/stale forever. Use `await_stable_leader` (which keys on `can_serve`) instead. `scenario_service_wedged` calls `wait_ready` *before* stopping its service, which is why it can.

- [ ] **Step 1: the rules (RED at the script level)**

Append to `packaging/prometheus/uc2-alerts.yml`, after `Uc2DiskLow` (ends 82):

```yaml
  - alert: Uc2ServiceAbsent
    # M14c: rendered for every DECLARED id, so an FSM that never started is a
    # 0 sample rather than a missing series — that is what makes it
    # alertable at all. An absent declared FSM holds min(applied) still,
    # which closes the admission door and caps this node's durable report at
    # the lag bound: the cluster stalls by design until it attaches.
    expr: uc2_service_attached == 0
    for: 30s
    labels: { severity: critical }
    annotations: { summary: "declared FSM {{ $labels.service }} is not attached on {{ $labels.instance }} — admission is closed and this node's report is capped at the lag bound" }
  - alert: Uc2ServicePinnedAtLagBound
    # Bounded mode only: uc2_fsm_lag_bytes == 0 means lockstep, where being
    # "at the bound" is the normal steady state. `>=` not `>`: the node's own
    # report ceiling caps commit at exactly min(applied) + fsm_lag, so a
    # genuinely pinned FSM sits ON the bound and never past it. The
    # on(instance) group_left join is the same idiom Uc2PeerLagging uses to
    # compare a per-peer series against the node-scalar admission window.
    expr: (uc2_service_lag_bytes >= on(instance) group_left uc2_fsm_lag_bytes) and on(instance) uc2_fsm_lag_bytes > 0
    for: 30s
    labels: { severity: warning }
    annotations: { summary: "FSM {{ $labels.service }} on {{ $labels.instance }} is pinned at the fsm_lag bound — it is pacing the whole cluster" }
```

And change `Uc2ServiceWedged`'s expr (21) to select the aggregate explicitly:

```yaml
    expr: uc2_service_heartbeat_age_seconds{service=""} > 5 and uc2_node_heartbeat_age_seconds < 3
```

(M14c added labelled samples to that family. The `and` would already have dropped them — the label sets differ, so nothing matches `uc2_node_heartbeat_age_seconds` — but relying on that is a trap for the next edit. `{service=""}` matches series where the label is absent, i.e. the aggregate.)

```bash
promtool check rules packaging/prometheus/uc2-alerts.yml
scripts/m10_alert_fire.sh --out /home/claude/m14c-alerts 2>&1 | tail -25
```
Expected: `check rules` prints `SUCCESS: 16 rules found`. The script then aborts **before** any cluster runs, with `error: 2 alert(s) in .../uc2-alerts.yml have no RULE_BUILDERS entry: ['Uc2ServiceAbsent', 'Uc2ServicePinnedAtLagBound']` (the completeness cross-check at 431–452, exit 1). That refusal is the RED for steps 2–4.

- [ ] **Step 2: the harness gains a services-aware cluster builder**

In `uc2_node/examples/m10_alerts.rs`, give `make_config` (961) a `services` parameter — it has exactly one caller, `spawn_cluster` (1014). **The `#[allow]` is required, not cosmetic:** `clippy::too_many_arguments` fires above seven, `make_config` is at seven today, and CI runs `-D warnings`.

```rust
// One more knob than clippy likes; every one of them varies per scenario
// and a struct here would just be `NodeConfig` again.
#[allow(clippy::too_many_arguments)]
fn make_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    instance_dir: PathBuf,
    seed: u64,
    addr: SocketAddr,
    buffer_bytes: usize,
    admission_bytes: u64,
    services: uc2_node::ServicesConfig,
) -> NodeConfig {
```
and replace the last field with `services,`.

Then rename the body of `spawn_cluster` into a services-taking form and keep the old name as a wrapper:

```rust
/// Bind every node's socket first (so the full member map is known before
/// any agent runs), then start each on its pre-bound socket with a real obs
/// HTTP server attached. Every declared FSM's rings and `snapshots/<id>/`
/// are created by the node at boot, whether or not anything attaches.
fn spawn_cluster_with_services(
    scratch_root: &Path,
    label: &str,
    n: usize,
    admission_bytes: u64,
    services: uc2_node::ServicesConfig,
) -> (tempfile::TempDir, Vec<NodeH>) {
    let dir = tempfile::Builder::new()
        .prefix(&format!("m10-{label}-"))
        .tempdir_in(scratch_root)
        .expect("tempdir");

    let socks: Vec<UdpSocket> =
        (0..n).map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind")).collect();
    let members: Vec<(NodeId, SocketAddr)> =
        socks.iter().enumerate().map(|(i, s)| (i as NodeId, s.local_addr().unwrap())).collect();

    let mut nodes = Vec::with_capacity(n);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = dir.path().join(format!("n{i}"));
        let cfg = make_config(
            i as NodeId,
            members.clone(),
            instance_dir.clone(),
            seed_for(i),
            addr,
            RING_BYTES,
            admission_bytes,
            services,
        );
        let node = Node::start_with_socket(cfg, sock).expect("start");
        let obs =
            ObsServer::serve(node.observability(), "127.0.0.1:0".parse().unwrap()).expect("bind obs");
        nodes.push(NodeH { addr, instance_dir, node: Some(node), obs: Some(obs) });
    }
    (dir, nodes)
}

/// The default declared set (`{0}`, lag bound = buffer/4) — every pre-M14c
/// scenario.
fn spawn_cluster(
    scratch_root: &Path,
    label: &str,
    n: usize,
    admission_bytes: u64,
) -> (tempfile::TempDir, Vec<NodeH>) {
    spawn_cluster_with_services(
        scratch_root,
        label,
        n,
        admission_bytes,
        uc2_node::ServicesConfig::default(),
    )
}
```

- [ ] **Step 3: the two scenarios**

Add in `m10_alerts.rs` between `scenario_service_wedged` (which ends just above 522) and `scenario_leader_isolated`'s doc comment (522–527, `fn` at 528) — or equivalently at the end of the scenario run; order in the file is presentation only:

```rust
// ----------------------------------------------------------- scenario 12

/// Uc2ServiceAbsent — **real**. A node declaring `{0, 1}` with only FSM 0
/// ever attached. FSM 1's slot stays unattached for the whole capture, and
/// the exporter renders it as `uc2_service_attached{service="1"} 0` because
/// the per-FSM band iterates the DECLARED bitmask, not occupied slots.
///
/// No `wait_ready` here: `/readyz` keys on page 1's service heartbeat, which
/// is the `min` over declared FSMs, so an absent FSM 1 holds this node at
/// 503 forever by design. `await_stable_leader` (can_serve) is the right
/// gate — the node is serving, it is admission that is shut.
fn scenario_service_absent(scratch_root: &Path) -> (SeriesFile, Disclosure) {
    let services = uc2_node::ServicesConfig::from_ids(&[0, 1], None).expect("declared set");
    let (_dir, mut nodes) =
        spawn_cluster_with_services(scratch_root, "svc-absent", 1, 256 * 1024, services);
    await_stable_leader(&nodes, 20);

    let instance_dir = nodes[0].instance_dir.clone();
    let svc0 = ServiceBuilder::new(ServiceConfig::new(&instance_dir, APP).service_id(0), NoopSm)
        .start()
        .expect("FSM 0 attaches");
    // FSM 1 is deliberately never started.
    let addr = nodes[0].obs_addr();

    let mut sf = SeriesFile::new();
    for _ in 0..6 {
        sf.record_round(
            "n0",
            &scrape(addr),
            &["uc2_service_attached", "uc2_services_declared"],
        );
        thread::sleep(Duration::from_millis(500));
    }
    svc0.stop();
    nodes[0].stop();

    (
        sf,
        Disclosure {
            scenario: "service_absent",
            rules: &["Uc2ServiceAbsent"],
            state: "real",
            method: "real single-node cluster declaring services.ids = [0, 1]; FSM 0 attaches, \
                     FSM 1 is never started. 6 real scrapes of the node's /metrics all read \
                     uc2_service_attached{service=\"1\"} 0 (and service=\"0\" 1) — the per-FSM \
                     band renders a row for every DECLARED id, so the absent FSM is a 0 sample, \
                     not a missing series."
                .into(),
        },
    )
}

// ----------------------------------------------------------- scenario 13

/// FSM 1's stand-in for `fsm_pinned`: 20 ms per apply, so it can never keep
/// up with the load loop and the lag barrier pins the whole node to it.
struct SlowSm;
impl StateMachine for SlowSm {
    type Command = ();
    type Response = ();
    type Query = ();
    type QueryResponse = ();
    fn apply(&mut self, _position: u64, _cmd: ()) {
        thread::sleep(Duration::from_millis(20));
    }
    fn query(&self, _q: ()) {}
    fn last_applied(&self) -> Option<u64> {
        None
    }
}

/// Uc2ServicePinnedAtLagBound — **real**. A node declaring `{0, 1}` with
/// `fsm_lag = Bounded(8 KiB)` and a 256 KiB admission window, so the FSM
/// door — not the admission window — is what stops the log. FSM 0 is a
/// no-op, FSM 1 sleeps 20 ms per record. Under sustained load this node's
/// own durable report is capped at `min(applied) + fsm_lag`
/// (`services::report_ceiling`), and on a single-node cluster commit IS that
/// report, so `commit - applied_1` settles at exactly 8192 = the bound and
/// stays there. That flatness is what makes the rule's `>=` adjudicable
/// under `hold_last` dilation.
///
/// Disclosure worth reading twice: FSM 1's heartbeat also ages while it
/// sleeps inside `apply`, so a real deployment in this state would fire
/// `Uc2ServiceWedged` too. That is honest — an FSM this slow IS wedged from
/// the cluster's point of view — but only `Uc2ServicePinnedAtLagBound` is
/// adjudicated from this capture.
fn scenario_fsm_pinned(scratch_root: &Path) -> (SeriesFile, Disclosure) {
    let services = uc2_node::ServicesConfig::from_ids(&[0, 1], Some(uc2_node::FsmLag::Bounded(8192)))
        .expect("declared set");
    let (_dir, mut nodes) =
        spawn_cluster_with_services(scratch_root, "fsm-pinned", 1, 256 * 1024, services);
    await_stable_leader(&nodes, 20);

    let instance_dir = nodes[0].instance_dir.clone();
    let svc0 = ServiceBuilder::new(ServiceConfig::new(&instance_dir, APP).service_id(0), NoopSm)
        .start()
        .expect("FSM 0 attaches");
    let svc1 = ServiceBuilder::new(ServiceConfig::new(&instance_dir, APP).service_id(1), SlowSm)
        .start()
        .expect("FSM 1 attaches");

    let addr = nodes[0].obs_addr();
    let families = ["uc2_service_lag_bytes", "uc2_fsm_lag_bytes", "uc2_service_attached"];
    let mut sf = SeriesFile::new();
    for _ in 0..12 {
        // Keep the backlog past the bound: submits are refused (`Full`) the
        // moment the FSM door shuts, which is the state being captured.
        let round_deadline = Instant::now() + Duration::from_millis(450);
        while Instant::now() < round_deadline {
            let _ = nodes[0].n().submit(vec![0u8; 64]);
        }
        sf.record_round("n0", &scrape(addr), &families);
        thread::sleep(Duration::from_millis(50));
    }
    svc1.stop();
    svc0.stop();
    nodes[0].stop();

    (
        sf,
        Disclosure {
            scenario: "fsm_pinned",
            rules: &["Uc2ServicePinnedAtLagBound"],
            state: "real",
            method: "real single-node cluster, services.ids = [0, 1], fsm_lag = 8 KiB, admission \
                     window 256 KiB (so the FSM door binds first). FSM 1 sleeps 20 ms per apply; \
                     the node's report ceiling caps commit at min(applied) + fsm_lag, so 12 real \
                     scrapes read uc2_service_lag_bytes{service=\"1\"} flat at the 8192-byte \
                     bound while uc2_fsm_lag_bytes reads 8192."
                .into(),
        },
    )
}
```

Register both: `ALL_SCENARIOS` (52–64) gains `"service_absent"` and `"fsm_pinned"` at the end; `run_scenario`'s match (166–181) gains

```rust
        "service_absent" => scenario_service_absent(scratch_root),
        "fsm_pinned" => scenario_fsm_pinned(scratch_root),
```

```bash
export CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a
cargo run -p uc2_node --release --example m10_alerts -- --scenario service_absent --out /home/claude/m14c-alerts 2>&1 | tail -5
cargo run -p uc2_node --release --example m10_alerts -- --scenario fsm_pinned --out /home/claude/m14c-alerts 2>&1 | tail -5
grep -E 'uc2_service_attached|uc2_service_lag_bytes|uc2_fsm_lag_bytes' /home/claude/m14c-alerts/*.series
```
Expected: two `scenario=… state=real` lines and two `.series` files; `uc2_service_attached{service="1",instance="n0"}` all zeros, `uc2_service_lag_bytes{service="1",instance="n0"}` flat `8192`, `uc2_fsm_lag_bytes{instance="n0"}` `8192`. If the lag row reads *less* than 8192 in the last sample, the load loop is not keeping the backlog past the bound — raise the per-round busy window, do not weaken the rule.

- [ ] **Step 4: the adjudicator hookup**

In `scripts/m10_alert_fire.sh`, add to `RULE_META` (222–237):

```python
    "Uc2ServiceAbsent": {"severity": "critical", "real": True, "scenario": "service_absent"},
    "Uc2ServicePinnedAtLagBound": {"severity": "warning", "real": True, "scenario": "fsm_pinned"},
```

Change `build_Uc2ServiceWedged`'s LHS selection (271) to pin the aggregate now that the family also carries labelled samples:

```python
    svc_row = select(rows, "uc2_service_heartbeat_age_seconds", {"service": None})
```

(`select` compares `r["labels"].get(k) == v`, so `None` means "this label is absent" — the aggregate row. The rule's own `{service=""}` selector matches the same series.)

Add the two builders next to it:

```python
def build_Uc2ServiceAbsent():
    rows = load_scenario("service_absent")
    row = select(rows, "uc2_service_attached", {"service": "1"})
    r = new_rule("critical", labels_from=row)  # == 0 keeps every label
    add_hold_last(r, row, "uc2_service_attached", 30)
    r["eval_time"] = total_for(30)[0]
    return r


def build_Uc2ServicePinnedAtLagBound():
    rows = load_scenario("fsm_pinned")
    lag_row = select(rows, "uc2_service_lag_bytes", {"service": "1"})
    bound_row = select(rows, "uc2_fsm_lag_bytes", {})
    # group_left keeps the LHS's `service` label; `and on(instance)` keeps it too.
    r = new_rule("warning", labels_from=lag_row)
    add_hold_last(r, lag_row, "uc2_service_lag_bytes", 30)
    add_hold_last(r, bound_row, "uc2_fsm_lag_bytes", 30)
    r["eval_time"] = total_for(30)[0]
    return r
```

and to `RULE_BUILDERS` (407–424):

```python
    "Uc2ServiceAbsent": build_Uc2ServiceAbsent,
    "Uc2ServicePinnedAtLagBound": build_Uc2ServicePinnedAtLagBound,
```

```bash
scripts/m10_alert_fire.sh --out /home/claude/m14c-alerts 2>&1 | tail -25
```
Expected: 16 verdict lines, all `PASS`, including
`PASS rule=Uc2ServiceAbsent scenario=service_absent state=real` and
`PASS rule=Uc2ServicePinnedAtLagBound scenario=fsm_pinned state=real`,
plus the `dilate rule=… policy=hold_last …` disclosure lines for the three new input series. Script exits 0. (Needs `promtool` on `PATH` or `$HOME/.local/bin`; the script names the download if it is missing.)

- [ ] **Step 5: dashboard rows**

In `packaging/grafana/uc2-dashboard.json`, panel 9's target B (line 197) becomes the aggregate explicitly:

```json
        {
          "refId": "B",
          "datasource": { "type": "prometheus", "uid": "${DS_PROMETHEUS}" },
          "expr": "uc2_service_heartbeat_age_seconds{service=\"\"}",
          "legendFormat": "service (slowest) {{instance}}"
        }
```

Append after panel 10 (the last element of `panels`):

```json
    {
      "id": 11,
      "type": "row",
      "title": "Services (per FSM)",
      "gridPos": { "h": 1, "w": 24, "x": 0, "y": 29 },
      "collapsed": false,
      "panels": []
    },
    {
      "id": 12,
      "type": "timeseries",
      "title": "Per-FSM apply lag vs the bound",
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 30 },
      "datasource": { "type": "prometheus", "uid": "${DS_PROMETHEUS}" },
      "fieldConfig": {
        "defaults": { "unit": "bytes", "custom": { "drawStyle": "line", "fillOpacity": 10 } },
        "overrides": []
      },
      "targets": [
        {
          "refId": "A",
          "datasource": { "type": "prometheus", "uid": "${DS_PROMETHEUS}" },
          "expr": "uc2_service_lag_bytes",
          "legendFormat": "fsm {{service}} {{instance}}"
        },
        {
          "refId": "B",
          "datasource": { "type": "prometheus", "uid": "${DS_PROMETHEUS}" },
          "expr": "uc2_fsm_lag_bytes > 0",
          "legendFormat": "bound {{instance}}"
        }
      ]
    },
    {
      "id": 13,
      "type": "timeseries",
      "title": "Per-FSM heartbeat age",
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 30 },
      "datasource": { "type": "prometheus", "uid": "${DS_PROMETHEUS}" },
      "fieldConfig": {
        "defaults": { "unit": "s", "custom": { "drawStyle": "line", "fillOpacity": 10 } },
        "overrides": []
      },
      "targets": [
        {
          "refId": "A",
          "datasource": { "type": "prometheus", "uid": "${DS_PROMETHEUS}" },
          "expr": "uc2_service_heartbeat_age_seconds{service!=\"\"}",
          "legendFormat": "fsm {{service}} {{instance}}"
        }
      ]
    },
    {
      "id": 14,
      "type": "stat",
      "title": "Every declared FSM attached",
      "gridPos": { "h": 4, "w": 6, "x": 0, "y": 38 },
      "datasource": { "type": "prometheus", "uid": "${DS_PROMETHEUS}" },
      "fieldConfig": { "defaults": { "unit": "none" }, "overrides": [] },
      "targets": [
        {
          "refId": "A",
          "datasource": { "type": "prometheus", "uid": "${DS_PROMETHEUS}" },
          "expr": "min(uc2_service_attached)",
          "instant": true
        }
      ]
    },
    {
      "id": 15,
      "type": "stat",
      "title": "Declared sets agreeing",
      "gridPos": { "h": 4, "w": 6, "x": 6, "y": 38 },
      "datasource": { "type": "prometheus", "uid": "${DS_PROMETHEUS}" },
      "fieldConfig": { "defaults": { "unit": "none" }, "overrides": [] },
      "targets": [
        {
          "refId": "A",
          "datasource": { "type": "prometheus", "uid": "${DS_PROMETHEUS}" },
          "expr": "count(count_values(\"v\", uc2_services_declared))",
          "instant": true
        }
      ]
    }
```

(Panel 15 is the spec §8 "declared sets differ across nodes" query, verbatim: `1` = every node declares the same set, `> 1` = drift. It has no alert rule — §8 documents it as "must match", enforced on the snapshot path, alertable here.)

```bash
python3 -c "import json;d=json.load(open('packaging/grafana/uc2-dashboard.json'));print(len(d['panels']),[p['id'] for p in d['panels']])"
```
Expected: `15 [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]` — valid JSON, no duplicate ids.

- [ ] **Step 6: clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add packaging/prometheus/uc2-alerts.yml packaging/grafana/uc2-dashboard.json uc2_node/examples/m10_alerts.rs scripts/m10_alert_fire.sh
git commit -m "feat(m14c): Uc2ServiceAbsent + Uc2ServicePinnedAtLagBound, both proven to fire from real two-FSM scenarios; dashboard per-FSM rows; ServiceWedged pinned to the aggregate sample"
```

---

### Task 9: `uc2ctl status`'s per-service table, and the attach/detach transition records

**Files:**
- Modify `uc2ctl/src/main.rs`: `use uc2_log::cnc::{AdminAuth, AdminReq, CncPage}` (80) gains `unpack_service_status`; `run_status` (495–553) gains the services block between the `log:` line (523) and `members:` (524)
- Create `uc2ctl/tests/status_services.rs`
- Modify `uc2_node/src/services.rs`: add `SERVICE_STALE_NS` next to `fsm_lag_eff` (193–201)
- Modify `uc2_node/src/obs/http.rs`: `HEARTBEAT_STALE_NS` (39) gets a cross-reference + a pinning test in `mod tests` (299)
- Modify `uc2_node/src/node.rs`: import (19); `struct Consensus` fields near `last_flags` (1915); both constructors (`last_flags: 0,` at 1298 and 5998); `publish_service_mins` (2713–2734); `publish_status` (3092–3096); new `note_service_transitions`
- Test: `uc2_node/tests/services.rs` (607 lines) — one new test using the file's existing `serialize()`/`tempdir()`/`config()`/`ids()`/`start_service()` helpers (18–67, 139–141)

**Interfaces:**

Consumes:
- `uc2_log::cnc::unpack_service_status(u64) -> (u8, bool, u32)` (`uc2_log/src/cnc.rs:205`), `pack_service_status(u8, bool, u32) -> u64` (198).
- `CncPage::{services_declared, fsm_lag_bytes, service_slot}` (`uc2_log/src/cnc.rs:517/532/506`); `uc_protocol::v2::cnc::CNC_MAX_SERVICES` — already imported by `uc2ctl/src/main.rs:82` and `uc2_node/src/node.rs:35`.
- `ServicesConfig::ids(&self) -> impl Iterator<Item = u8>` (`uc2_node/src/services.rs:84`), ascending, declared only.
- `uc2_node::obs::log::{capture_for_tests, stderr_for_tests}` (`uc2_node/src/obs/log.rs:152/159`) and `crate::obs_event!` (247) — `key = value` pairs where keys are identifiers and values are `u64`/`i64`/`bool`/`&str`.
- `uc2_service::attach` bumps the slot epoch once per incarnation (`uc2_service/src/attach.rs:160–162`: status stored with `incarnation + 1`, then `epoch.fetch_add(1) + 1`); `Service::stop()` clears the ATTACHED bit (asserted by `uc2_node/tests/services.rs:172–174`).
- `Node::start_with_socket(NodeConfig, UdpSocket)` (`uc2_node/src/node.rs:508`).

Produces:
- `uc2_node::services::SERVICE_STALE_NS: u64 = 3_000_000_000`.
- `Consensus::note_service_transitions(&mut self)` and the fields `service_last_epoch: [u64; CNC_MAX_SERVICES]`, `service_was_live: [bool; CNC_MAX_SERVICES]`, `last_wall_ns: u64`.
- `[log]` events `service_attached` / `service_detached`, both with fields `node`, `service`, `epoch`.
- `uc2ctl status` stdout gains a `services:` header line plus one `  id=… ` line per declared id.

**One deliberate widening of the spec's wording, for the reviewer:** §14.4 says the detach event fires "on the heartbeat aging past the wedged threshold". The predicate implemented here is `attached_bit && heartbeat_fresh`, so an *orderly* `Service::stop` (which clears the bit) is reported on the next duty cycle instead of 3 s later, while a SIGKILLed service — bit still set, heartbeat frozen — is still reported by the ageing heartbeat exactly as specified. Strictly more coverage, one predicate, and it is what makes the test below deterministic in under a second rather than racing a 3 s timer.

- [ ] **Step 1: RED — the `uc2ctl status` table test**

Create `uc2ctl/tests/status_services.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M14c (spec §9): `uc2ctl status` prints a per-service table off page 2.
//!
//! Starts a real node IN-PROCESS declaring `{0, 1}` and writes FSM 0's slot
//! by hand — the slot band's writer is the service process, and this test
//! deliberately does not need one: `uc2ctl` reads the page, and the page is
//! what is under test. FSM 1 is left untouched (declared, never attached),
//! which is the row an operator most needs to see.

use std::net::{SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use uc2_log::cnc::{CncPage, pack_service_status};
use uc2_net::fault::FaultConfig;
use uc2_node::{
    CryptoConfig, DEFAULT_JOURNAL_SEGMENT_BYTES, FsmLag, Node, NodeConfig, PurgePolicy,
    ServicesConfig,
};

const APP: &str = "ctlsvc";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_uc2ctl")
}

fn make_config(instance_dir: PathBuf, addr: SocketAddr, services: ServicesConfig) -> NodeConfig {
    NodeConfig {
        id: 0,
        members: vec![(0, addr)],
        learners: Vec::new(),
        bind: addr,
        instance_dir,
        app_id: APP.into(),
        buffer_bytes: 1 << 22,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: 0x5150_1234_ABCD_0F0F,
        faults: FaultConfig::default(),
        purge: PurgePolicy::Disabled,
        journal_segment_bytes: DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: CryptoConfig::Disabled,
        services,
    }
}

#[test]
fn status_prints_one_row_per_declared_fsm_including_an_absent_one() {
    let root = tempfile::Builder::new()
        .prefix("uc2ctl-svc-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
    let addr = sock.local_addr().unwrap();
    let dir = root.path().join("n0");
    let services = ServicesConfig::from_ids(&[0, 1], Some(FsmLag::Bounded(8192))).unwrap();
    let node = Node::start_with_socket(make_config(dir.clone(), addr, services), sock).unwrap();

    let deadline = Instant::now() + Duration::from_secs(20);
    while !node.can_serve() {
        assert!(Instant::now() < deadline, "node never became leader/serving");
        std::thread::sleep(Duration::from_millis(10));
    }

    // Stand in for FSM 0's service process: attached, incarnation 1, epoch 1,
    // applied 4096, one snapshot at 2048, a heartbeat stamped just now.
    let cnc = CncPage::open_file(&dir.join("cnc2.dat"), APP).expect("open cnc");
    let s0 = cnc.service_slot(0);
    s0.applied.store_release(4096);
    s0.snapshot_pos.store_release(2048);
    s0.epoch.store_release(1);
    s0.heartbeat_ns.store_release(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64,
    );
    s0.status.store_release(pack_service_status(0, true, 1));

    let out = Command::new(bin())
        .args(["status", "--instance-dir", dir.to_str().unwrap(), "--app-id", APP])
        .output()
        .expect("spawn uc2ctl");
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    assert_eq!(out.status.code(), Some(0), "status must succeed: {stdout}");

    assert!(stdout.contains("services: declared=[0, 1] fsm_lag=8192 bytes"), "{stdout}");
    assert!(
        stdout.contains("id=0 attached=true epoch=1 incarnation=1 applied=4096"),
        "{stdout}"
    );
    assert!(stdout.contains("snapshot_pos=2048"), "{stdout}");
    // The declared-but-absent FSM must still get a row — it is the row that
    // explains a stalled cluster.
    assert!(
        stdout.contains("id=1 attached=false epoch=0 incarnation=0 applied=0"),
        "{stdout}"
    );
    assert!(stdout.contains("heartbeat_age=never"), "{stdout}");
    // The pre-existing sections are untouched.
    assert!(stdout.contains("config: version="), "{stdout}");
    assert!(stdout.contains("members:"), "{stdout}");

    node.stop();
}
```

```bash
export CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a
cargo test -p uc2ctl --test status_services 2>&1 | tail -20
```
Expected: FAIL — `assertion failed: stdout.contains("services: declared=[0, 1] fsm_lag=8192 bytes")`, with the printed stdout showing only `config:`/`role:`/`log:`/`members:`.

- [ ] **Step 2: GREEN — the `run_status` block**

In `uc2ctl/src/main.rs`, extend the import at 80:

```rust
use uc2_log::cnc::{AdminAuth, AdminReq, CncPage, unpack_service_status};
```

and insert into `run_status` immediately after `println!("log: commit={commit} durable={durable} append={append}");` (523):

```rust
    // M14c (spec §9): the per-service table, straight off page 2 of the page
    // this command already opened. One row per DECLARED id (the bitmask at
    // cnc 4032), including ids nothing has attached to — a declared-but-
    // absent FSM holds min(applied) still, which closes the admission door
    // and caps this node's durable report, so it is exactly the row that
    // explains a stalled cluster. A harness page (declared == 0) prints the
    // header and no rows.
    let declared = cnc.services_declared();
    let fsm_lag = cnc.fsm_lag_bytes();
    let ids: Vec<u8> =
        (0..CNC_MAX_SERVICES as u8).filter(|i| declared & (1u64 << i) != 0).collect();
    let lag_desc =
        if fsm_lag == 0 { "lockstep".to_string() } else { format!("{fsm_lag} bytes") };
    println!("services: declared={ids:?} fsm_lag={lag_desc}");
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    for id in ids {
        let s = cnc.service_slot(id as usize);
        let (_, attached, incarnation) = unpack_service_status(s.status.load_acquire());
        let applied = s.applied.load_acquire();
        let hb = s.heartbeat_ns.load_acquire();
        // A never-stamped heartbeat is `never`, not a 55-year age: the
        // slot is zeroed at node boot and only a service ever writes it.
        let age = if hb == 0 {
            "never".to_string()
        } else {
            format!("{:.3}s", now_ns.saturating_sub(hb) as f64 / 1e9)
        };
        println!(
            "  id={id} attached={attached} epoch={} incarnation={incarnation} \
             applied={applied} lag={} snapshot_pos={} heartbeat_age={age}",
            s.epoch.load_acquire(),
            commit.saturating_sub(applied),
            s.snapshot_pos.load_acquire(),
        );
    }
```

(`CNC_MAX_SERVICES` is already in the `uc_protocol::v2::cnc` import at 82; `SystemTime`/`UNIX_EPOCH` at 75.)

```bash
cargo test -p uc2ctl --test status_services 2>&1 | tail -10
cargo test -p uc2ctl 2>&1 | tail -10
```
Expected: both green — `admin_auth_bin.rs`'s status assertions (`config: version=1`, `id=102 role=learner`) are unaffected; its nodes use `ServicesConfig::none_for_tests()`, so they print `services: declared=[] fsm_lag=lockstep` and no rows.

- [ ] **Step 3: RED — the transition-record test**

Add to `uc2_node/tests/services.rs` (end of file):

```rust
/// M14c (spec §9): the `[log]` transition records name each FSM's arrival
/// and departure. Attach is keyed on the slot's epoch (bumped once per
/// incarnation by `uc2_service::attach`); departure is keyed on liveness =
/// ATTACHED bit AND a fresh heartbeat, so an orderly `stop()` is reported on
/// the next duty cycle and a killed service is reported once its heartbeat
/// ages past `services::SERVICE_STALE_NS`.
#[test]
fn attaching_and_stopping_an_fsm_emits_the_transition_records() {
    let _g = serialize();
    let buf = uc2_node::obs::log::capture_for_tests();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 1], None))).unwrap();
    wait_until("serving", || node.can_serve());

    let svc1 = start_service(dir.path(), 1);
    wait_until("service_attached record for FSM 1", || {
        let t = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
        t.lines().any(|l| {
            l.contains(r#""event":"service_attached""#)
                && l.contains(r#""service":1"#)
                && l.contains(r#""epoch":1"#)
        })
    });

    svc1.stop();
    wait_until("service_detached record for FSM 1", || {
        let t = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
        t.lines().any(|l| {
            l.contains(r#""event":"service_detached""#) && l.contains(r#""service":1"#)
        })
    });

    // FSM 0 was never started: it must not be reported as attaching or
    // departing — the events are edges, not a per-cycle status dump.
    let t = String::from_utf8_lossy(&buf.lock().unwrap()).into_owned();
    assert!(
        !t.lines().any(|l| l.contains(r#""event":"service_attached""#)
            && l.contains(r#""service":0"#)),
        "{t}"
    );

    node.stop();
    uc2_node::obs::log::stderr_for_tests();
}
```

```bash
cargo test -p uc2_node --test services attaching_and_stopping 2>&1 | tail -20
```
Expected: FAIL — `timeout waiting for service_attached record for FSM 1` (the `wait_until` helper's panic, 30 s deadline). No such event exists yet.

- [ ] **Step 4: GREEN — the staleness bar**

In `uc2_node/src/services.rs`, after `fsm_lag_eff` (ends 201):

```rust
/// M14c (spec §9): how stale a declared FSM's heartbeat may get before the
/// node calls it gone and emits `service_detached`. Deliberately the SAME
/// 3 s bar `obs::http`'s `HEARTBEAT_STALE_NS` applies to `/readyz` — a
/// service readiness already refuses to count is a service the transition
/// log should have named — and pinned equal to it by a unit test in
/// `obs::http`. They are separate constants because they answer different
/// questions (serve traffic? / say so in the log); if you move one, decide
/// about the other rather than discovering the drift later.
pub const SERVICE_STALE_NS: u64 = 3_000_000_000;
```

In `uc2_node/src/obs/http.rs`, amend the doc on `HEARTBEAT_STALE_NS` (37–39) with `/// Pinned equal to [`crate::services::SERVICE_STALE_NS`] (M14c) by
/// `the_readiness_bar_and_the_detach_bar_agree` below.` and add to `mod tests` (299):

```rust
    /// M14c: readiness and the `service_detached` transition record use the
    /// same 3 s staleness bar. Two constants, one number, one test.
    #[test]
    fn the_readiness_bar_and_the_detach_bar_agree() {
        assert_eq!(HEARTBEAT_STALE_NS, crate::services::SERVICE_STALE_NS);
    }
```

- [ ] **Step 5: GREEN — the node-side edge detector**

In `uc2_node/src/node.rs`, extend the import at 19:

```rust
use uc2_log::cnc::{AdminAuth, AdminReq, AdminResp, CncMeta, CncPage, unpack_service_status};
```

Add to `struct Consensus`, immediately after `last_flags: u64,` (1915):

```rust
    /// M14c (spec §9): per-declared-id edge state for the `[log]` transition
    /// records. `service_last_epoch[i]` is the epoch the last
    /// `service_attached` was emitted for; `service_was_live[i]` whether id
    /// `i` was last seen attached AND stamping. Indexed BY SERVICE ID, not
    /// densely — declared sets are sparse (`{0, 3}` is legal).
    service_last_epoch: [u64; CNC_MAX_SERVICES],
    service_was_live: [bool; CNC_MAX_SERVICES],
    /// M14c: the wall-clock ns `publish_status` last stamped into
    /// `node_heartbeat_ns`, reused by `note_service_transitions` so the
    /// transition check costs no second clock read per duty cycle. One cycle
    /// stale, which is immaterial against a 3 s bar; `0` until the first
    /// `publish_status`, which reads as "everything fresh" and therefore can
    /// never emit a spurious detach at boot.
    last_wall_ns: u64,
```

In BOTH constructors, after `last_flags: 0,` (1298 and 5998):

```rust
            service_last_epoch: [0; CNC_MAX_SERVICES],
            service_was_live: [false; CNC_MAX_SERVICES],
            last_wall_ns: 0,
```

In `publish_status`, after `status.node_heartbeat_ns.store_release(now_ns);` (3096):

```rust
        self.last_wall_ns = now_ns;
```

In `publish_service_mins` (2713), as the last statement of the function (after the `service_heartbeat_ns` store, 2731–2733):

```rust
        self.note_service_transitions();
```

and add the method immediately after `publish_service_mins`:

```rust
    /// M14c (spec §9): emit `service_attached` / `service_detached` on the
    /// edges of each declared FSM's liveness.
    ///
    /// Out of line deliberately. M14a measured a wait ladder inlined into a
    /// hot loop's body costing 9 % at N=1 — on a path N=1 never executes —
    /// through codegen alone; `do_work` calls `publish_service_mins` first
    /// thing every cycle and everything downstream (the door, the report
    /// ceiling, both persisters) reads what it publishes, so that body stays
    /// small.
    ///
    /// Live = the slot's ATTACHED bit is set AND its heartbeat is fresher
    /// than [`crate::services::SERVICE_STALE_NS`]. That one predicate covers
    /// both exits: an orderly `Service::stop` clears the bit (reported next
    /// cycle), and a SIGKILLed service leaves the bit set, so only the
    /// ageing heartbeat can report it (~3 s). Attach is keyed on the epoch,
    /// which `uc2_service::attach` bumps once per incarnation, so a
    /// stop/start pair emits `service_detached` then `service_attached` with
    /// the new epoch.
    #[inline(never)]
    fn note_service_transitions(&mut self) {
        let now_ns = self.last_wall_ns;
        for id in self.services.ids() {
            let i = id as usize;
            let slot = self.cnc.service_slot(i);
            let epoch = slot.epoch.load_acquire();
            let (_, attached, _) = unpack_service_status(slot.status.load_acquire());
            let live = attached
                && now_ns.saturating_sub(slot.heartbeat_ns.load_acquire())
                    < crate::services::SERVICE_STALE_NS;
            if epoch > self.service_last_epoch[i] {
                self.service_last_epoch[i] = epoch;
                self.service_was_live[i] = true;
                crate::obs_event!(
                    Info,
                    "service_attached",
                    node = self.id as u64,
                    service = id as u64,
                    epoch = epoch
                );
            } else if self.service_was_live[i] && !live {
                self.service_was_live[i] = false;
                crate::obs_event!(
                    Info,
                    "service_detached",
                    node = self.id as u64,
                    service = id as u64,
                    epoch = epoch
                );
            }
        }
    }
```

```bash
cargo test -p uc2_node --test services 2>&1 | tail -12
cargo test -p uc2_node --lib obs::http 2>&1 | tail -5
```
Expected: every test in `services.rs` passes (13 + the new one), and the http pin test passes.

- [ ] **Step 6: full suite for the two touched crates, clippy, commit**

```bash
cargo test -p uc2_node --test obs_log 2>&1 | tail -6
cargo test -p uc2_node --lib 2>&1 | tail -4
cargo clippy --workspace --all-targets -- -D warnings
git add uc2ctl/src/main.rs uc2ctl/tests/status_services.rs uc2_node/src/services.rs uc2_node/src/obs/http.rs uc2_node/src/node.rs uc2_node/tests/services.rs
git commit -m "feat(m14c): uc2ctl status per-FSM table (id/attached/epoch/applied/lag/snapshot_pos/heartbeat age) + service_attached/service_detached transition records on the liveness edge"
```

---

### Task 10: Documentation

**Files:**
- Modify `docs/how-to/monitor-a-cluster.md`: the family count (58), a new per-FSM subsection after the holes-counters passage (~85), two rows in the alert table (119–133), a note after the leader-authoritative paragraph (135)
- Modify `docs/how-to/diagnose-a-node.md`: a new section after "Is the node alive, and is its service alive?" (44–49)
- Modify `docs/ops/uc2-runbook.md`: the "Observing a cluster" bullet (55–60) and a `Look-up` entry (103–124)
- Modify `docs/reference/uc2ctl.md`: `status`'s output-fields table (131–142)
- Modify `docs/reference/semver-policy.md`: a bullet after the M14b one (63–75)
- Modify `docs/VERIFICATION.md`: §9 (516–541) gains an alert-proof paragraph; `Reproducing everything` (614–650) gains the command
- **Do not touch** `README.md:65` or `docs/benchmarks/uc2-m10-gate-2026-08-20.md` — "62/62 metric families" there is a recorded M10 fleet result, not a live count.

**Interfaces:** consumes Task 7's family names, Task 8's alert names and dashboard panels, Task 9's `uc2ctl status` output shape and event names. Produces no code.

- [ ] **Step 1: `monitor-a-cluster.md`**

Line 58: `The full series contract — 65 families —` becomes `The full series contract — 70 families —`.

After the `IngressRingWedged` paragraph (ends ~89) and before `## Install the alert rules`, add:

```markdown
### The per-FSM families (M14)

A node runs one FSM per declared service id (`[services] ids`), and every
service family carries a `service="<id>"` label per declared id:
`uc2_service_applied_bytes`, `uc2_service_epoch`,
`uc2_service_snapshot_pos_bytes`, `uc2_service_heartbeat_age_seconds`,
`uc2_service_attached`, `uc2_service_lag_bytes` (= `commit − applied`),
`uc2_service_lag_waits_total`. Two node-scalar gauges describe the set
itself: `uc2_services_declared` (the bitmask — bit *k* = id *k*) and
`uc2_fsm_lag_bytes` (the lag bound; **0 means lockstep**).

Two shapes to know before writing a query:

- **The first four families also carry an unlabeled sample**, which is the
  M10 series and now means **the slowest declared FSM** (page 1's `min` over
  the declared ids — the number the purge floor, the admission door and
  `/readyz` all key on). Aggregate and per-FSM samples live in the same
  family, so `sum(uc2_service_applied_bytes)` double counts. Say
  `{service=""}` for the aggregate and `{service!=""}` for the per-FSM rows.
- **A declared FSM that has never attached still renders a row**, reading
  `uc2_service_attached{service="k"} 0` with zeros beside it. That is
  deliberate: you cannot alert on a series that is absent, and "declared but
  never started" is the state that silently closes admission cluster-wide.

Declared sets must match across nodes (spec §8). There is no alert rule for
drift, because it is a query over the fleet rather than a per-node
condition — `count(count_values("v", uc2_services_declared))` is `1` on a
healthy cluster and `> 1` the moment two nodes disagree. The dashboard ships
it as the "Declared sets agreeing" stat.
```

In the alert table (after the `Uc2DiskLow` row, 133), add:

```markdown
| `Uc2ServiceAbsent` | a declared FSM's `uc2_service_attached` has read 0 for 30s — it was never started, or it stopped. Admission is closed and this node's durable report is capped at the lag bound, so the cluster stalls by design until it attaches | critical |
| `Uc2ServicePinnedAtLagBound` | a declared FSM's `uc2_service_lag_bytes` has sat at or above `uc2_fsm_lag_bytes` for 30s in bounded mode — that FSM is pacing the whole cluster | warning |
```

After the leader-authoritative paragraph (135), append:

```markdown
`Uc2ServiceWedged` selects the aggregate explicitly
(`uc2_service_heartbeat_age_seconds{service=""}`) — the same family now
carries a labelled sample per FSM, and the rule is about the node's slowest
one. `Uc2ServiceAbsent` and `Uc2ServicePinnedAtLagBound` are per-FSM: they
fire once per offending `service` label, on whichever node declares it.
```

- [ ] **Step 2: `diagnose-a-node.md` — the two playbooks**

After "Is the node alive, and is its service alive?" (ends 49), insert:

```markdown
## Which FSM is holding the cluster up?

Since M14 a node runs one FSM per declared id, and the slowest one paces
everything: page 1's service band is the `min` over declared ids, the
admission door is `append − min(applied) ≤ fsm_lag`, and this node's durable
report is capped at `min(applied) + fsm_lag`. So a single sick FSM stalls
commits cluster-wide — by design, and visibly.

Start with `uc2ctl status`, which prints the whole band without a scrape:

```text
services: declared=[0, 1] fsm_lag=8192 bytes
  id=0 attached=true epoch=3 incarnation=3 applied=1048576 lag=0 snapshot_pos=1040384 heartbeat_age=0.004s
  id=1 attached=false epoch=0 incarnation=0 applied=0 lag=1048576 snapshot_pos=0 heartbeat_age=never
```

Read it in this order:

1. **`attached=false`** on a declared id (`Uc2ServiceAbsent`) — that FSM's
   process is not running, or it refused to attach. Check the service's own
   logs for `ServiceNotDeclared` (its `--service-id` is not in this node's
   `[services] ids`) or the `service.<id>.lock` refusal (two processes, one
   id). `heartbeat_age=never` distinguishes "never started since this node
   booted" from "was running, stopped".
2. **`attached=true` with a stale `heartbeat_age`** (`Uc2ServiceWedged`) —
   the apply loop is wedged inside `apply()`, not the cluster. The
   `[log]` records say which: `service_attached` then no `service_detached`
   means it is still holding its slot.
3. **`lag` pinned at `fsm_lag`** (`Uc2ServicePinnedAtLagBound`) — that FSM is
   running, just slower than the log. Nothing is broken; the cluster is being
   paced to it, which is what a bound buys you. Either make that FSM faster
   or accept the rate. Raising `fsm_lag` buys latency headroom, not
   throughput, and it is refused above `buffer_bytes / 2`.

`uc2_service_lag_waits_total{service}` tells you the converse: an FSM whose
wait counter climbs is the one *being* paced, i.e. a victim, not the cause.
The cause is the id with the largest `uc2_service_lag_bytes`.

The transition records name arrivals and departures explicitly:
`{"event":"service_attached","node":0,"service":1,"epoch":4}` and
`{"event":"service_detached","node":0,"service":1,"epoch":4}`. Departure is
edge-triggered on either the slot's ATTACHED bit clearing (an orderly stop,
reported within a duty cycle) or the heartbeat ageing past 3 s (a killed
process — nothing clears the bit for it).
```

- [ ] **Step 3: runbook + `uc2ctl` reference**

`docs/ops/uc2-runbook.md`, the "Observing a cluster" bullet (55–60): append a
second bullet

```markdown
- [Diagnose a node → Which FSM is holding the cluster up?](../how-to/diagnose-a-node.md#which-fsm-is-holding-the-cluster-up)
  — the per-FSM band (`uc2ctl status`'s services table, the
  `service="<id>"` metric families, `Uc2ServiceAbsent` /
  `Uc2ServicePinnedAtLagBound`, and the `service_attached`/`service_detached`
  records).
```

and in `Look-up` (103–124), after the `uc2ctl` entry:

```markdown
- [Monitor a cluster → The per-FSM families](../how-to/monitor-a-cluster.md#the-per-fsm-families-m14)
  — which metric families carry a `service` label, what the unlabeled
  aggregate means now, and the declared-set drift query.
```

`docs/reference/uc2ctl.md`, `status`'s output-fields table (134–142), after
the `members` row:

```markdown
| `services` | the declared id list (cnc 4032's bitmask) and the lag policy — `fsm_lag=lockstep` or `fsm_lag=<N> bytes` (cnc 4040). A node started for a harness (`ServicesConfig::none_for_tests`) prints an empty list and no rows |
| per-FSM rows | one line per **declared** id, attached or not: `attached` (the slot's ATTACHED bit), `epoch` (incarnations since this node booted), `incarnation` (the status word's counter), `applied`, `lag` (`commit − applied`), `snapshot_pos`, `heartbeat_age` (`never` if that FSM has not stamped since boot) |
```

and amend the first sentence of `### status` (123–125) to end `…, per-member peer-slot observability, the per-declared-FSM service table (M14), and the leader/serving flags.`

- [ ] **Step 4: `semver-policy.md`**

After the M14b bullet (ends 75):

```markdown
- **M14c adds the per-FSM observability surface**, all additive:
  `uc2_service_attached`, `uc2_service_lag_bytes`,
  `uc2_service_lag_waits_total`, `uc2_services_declared` and
  `uc2_fsm_lag_bytes` as new metric families; a `service="<id>"` sample per
  declared id on `uc2_service_applied_bytes`, `uc2_service_epoch`,
  `uc2_service_snapshot_pos_bytes` and `uc2_service_heartbeat_age_seconds`;
  the `Uc2ServiceAbsent` and `Uc2ServicePinnedAtLagBound` rules; the
  `service_attached`/`service_detached` `[log]` records; and a `services:`
  section in `uc2ctl status`'s output. Nothing was renamed or removed. Two
  consequences worth flagging even though neither is breaking under this
  policy: a query that assumed one sample per `uc2_service_*` family now
  sees several, so `sum(...)` double counts unless it says `{service=""}`
  (the shipped rules and dashboard were updated); and a scraper of
  `uc2ctl status` stdout sees new lines between `log:` and `members:`.
  The metric series contract is not itself in the promised-surface table —
  `uc2_node::obs` is listed as not promised — but it is treated as an
  operator interface in practice: families are added, not renamed.
```

- [ ] **Step 5: `VERIFICATION.md`**

At the end of §9 (after the hop-isolation paragraph, ends 541), add:

```markdown
**Alert rules are proven to fire, not just to parse.**
`scripts/m10_alert_fire.sh` builds or breaks a real cluster per rule
(`uc2_node/examples/m10_alerts.rs`), scrapes each node's *real* `/metrics`
HTTP endpoint once a second, time-dilates the captured samples onto a
synthetic timeline sized to that rule's `for:` clause, and lets
`promtool test rules` adjudicate — one `PASS`/`FAIL` line per shipped rule,
with the dilation policy disclosed at runtime and every scenario labelled
`real` or `synthetic`. A rule that ships without an adjudication entry fails
the run before any cluster starts. All 16 rules are covered; the two M14c
per-FSM rules (`Uc2ServiceAbsent`, `Uc2ServicePinnedAtLagBound`) are backed
by `real` two-FSM scenarios — a declared FSM that never attaches, and one
whose apply loop is slow enough that the node's own report ceiling pins
`commit − applied` exactly at the lag bound.
```

and in `Reproducing everything` (614–650), after the `scripts/elle_mutation.sh` line:

```bash
# Alert rules — every shipped rule fired against a real cluster (needs promtool)
scripts/m10_alert_fire.sh
```

- [ ] **Step 6: link check + commit**

```bash
grep -rn "which-fsm-is-holding-the-cluster-up\|the-per-fsm-families-m14" docs/ | grep -v superpowers
grep -rn "65 families" docs/ | grep -v superpowers   # must be empty
```
Expected: the two anchors resolve to the headings added in steps 1–2 (GitHub slugs: `## Which FSM is holding the cluster up?` → `which-fsm-is-holding-the-cluster-up`; `### The per-FSM families (M14)` → `the-per-fsm-families-m14`), and no stale family count remains.

```bash
git add docs/how-to/monitor-a-cluster.md docs/how-to/diagnose-a-node.md docs/ops/uc2-runbook.md docs/reference/uc2ctl.md docs/reference/semver-policy.md docs/VERIFICATION.md
git commit -m "docs(m14c): per-FSM metric families and their two shapes, the two alert playbooks, uc2ctl status rows, semver additive note, alert-firing proof in VERIFICATION"
```

---

### Task 11: The local proof stack (smoke, not a gate)

Runs things and records what it saw; no code. Mirrors the M14b plan's Task 9
(`docs/superpowers/plans/2026-08-27-uc2-m14b-query-routing-and-fan-in.md:1723–1762`)
with the additions this workstream earns: the hard-crash tier explicitly, the
alert-firing script, and the datagram fuzz target. Use the warm private target
dir (`~/.cache/cargo-target` is shared with the main checkout and every other
worktree — a concurrent build there silently swaps your binaries). Every
command in the **foreground**, each with a `timeout`. Scratch goes to real
disk under `/home/claude`, never `/tmp`.

- [ ] **Step 1: the workspace suite**

```bash
export CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a
cargo clippy --workspace --all-targets -- -D warnings
timeout 2400 cargo test --workspace --no-fail-fast 2>&1 | tail -40
```
Expected: clippy clean; every binary `0 failed`. `uc2ctl`'s new `status_services` and `uc2_node`'s new `services::attaching_and_stopping_an_fsm_emits_the_transition_records` are in this run.

- [ ] **Step 2: the capstones, the hard-crash tier, the heavy sim**

```bash
timeout 900 cargo test -p uc2_node --test lin_v2 2>&1 | tail -12
timeout 900 cargo test -p uc2_node --test lin_partition_v2 2>&1 | tail -12
timeout 900 cargo test -p uc2-crashtest --features hard-crash-tests 2>&1 | tail -12
timeout 1800 cargo test -p uc2_sim --release --features sim-heavy 2>&1 | tail -8
```
Expected: every capstone `Linearizable`; the heavy sim tier green. The
hard-crash tier is called out explicitly because `--all-targets` does **not**
compile feature-gated tests: `examples/uc2-crashtest/tests/*.rs` under
`hard-crash-tests` never sees a `cargo clippy --workspace --all-targets` run,
so this is the only place a break there surfaces.

- [ ] **Step 3: the alert rules actually fire**

```bash
command -v promtool || ls "$HOME/.local/bin/promtool"
timeout 1800 scripts/m10_alert_fire.sh --out /home/claude/m14c-alerts 2>&1 | tail -30
```
Expected: 16 `PASS rule=…` lines, exit 0, including
`PASS rule=Uc2ServiceAbsent scenario=service_absent state=real` and
`PASS rule=Uc2ServicePinnedAtLagBound scenario=fsm_pinned state=real`.
If `promtool` is absent the script exits 2 with the download hint — install it
rather than skipping this step; it is the only proof the two new rules fire.
Afterwards: `rm -rf /home/claude/m14c-alerts` (the scenarios leave instance
dirs under `_scratch`).

- [ ] **Step 4: fuzz smoke on the datagram decoder**

```bash
scripts/fuzz_smoke.sh 60 --min-runs 10000 uc_protocol_datagram
git clean -fdq fuzz/corpus; git checkout -- fuzz/Cargo.lock 2>/dev/null || true
git status --short
```
Expected: `PASS` with ≥ 10 000 runs; tree clean afterwards. (Needs nightly +
`cargo-fuzz`; `fuzz/` is outside the workspace and has its own lockfile.)

- [ ] **Step 5: the live-scrape coverage row**

```bash
timeout 900 cargo run -p uc2_node --release --example m10_gate -- --root /home/claude/m14c-smoke 2>&1 | tail -20
rm -rf /home/claude/m14c-smoke
```
Expected: the `1 /metrics coverage` verdict reads `all 70 CONTRACT_SERIES
families present on the leader's scrape; >=1 occupied per-peer series
confirmed`. Record any other verdict as smoke — this box is small and noisy,
and rate bars are fleet-only (M14d).

- [ ] **Step 6: record the run**

```bash
git commit --allow-empty -m "test(m14c-obs): local proof stack — workspace suite, lin capstones, hard-crash, sim-heavy, m10_alert_fire (16/16 rules fire), datagram fuzz smoke, m10_gate coverage 70/70 (numbers are smoke, not a gate)"
```

---

## Self-review

**Spec coverage (§14 is binding):**

| §14 clause | Task |
|---|---|
| §14.2 single-ring fast path committed; bisection v1/v2/v3 A/B'd exact-binary, keep what measures, numbers recorded, no bar | T1, T2 (+ the bench doc) |
| §14.3 one session per join, one BEGIN per declared id ascending, stream-global offsets, `SNAP_NAK` unchanged | T4 (sender), T5 (receiver) |
| §14.3 receiver adopts the floor only when `received == services_declared`; per-id `.part` + rename | T5 |
| §14.3 layout 34 / `layout` byte / `services_declared`; `CURRENT` 0.6.0 documentary; fuzz seed | T3 |
| §14.3 refusals `peer wire 0.5.0` + `declared-set mismatch`, named + counted | T5 (counters), T6 (accessor), T7 (metrics) |
| §14.3 "no snapshot for an id" moot; source declines on a missing file | T6 (deviation 3) |
| §14.3 §3.4 correction stated in the docs | T3 (`wire-protocol.md`), T6 (`upgrade-a-cluster.md`) |
| §14.3 acceptance: two-artifact stream with loss, refusals, two-FSM learner join, datagram pin 34, fuzz seed | T5, T6, T3 |
| §14.4 labelled twins via `push_labeled`, aggregates keep names, five new families, `CONTRACT_SERIES` | T7 |
| §14.4 two alerts with `m10_alerts` scenarios proven by `m10_alert_fire.sh`; dashboard | T8 |
| §14.4 `uc2ctl status` per-service table; `service_attached`/`service_detached` events | T9 |
| §14.4 docs; semver additive | T10 |
| Proof stack incl. the feature-gated crashtest, `m10_alert_fire.sh`, datagram fuzz smoke | T11 |

**Placeholder scan:** grepped for `TBD`, `TODO`, `similar to Task`, `add appropriate`, `fill in`, `implement later` — none.

**Type consistency across the seams:** `SnapBeginBody { session, layout, service_id, snapshot_pos, total_len, services_declared, config }` and `SNAP_BEGIN_LAYOUT_V2` (T3) are consumed verbatim by T4/T5; `SnapshotSet { services_declared, config, artifacts: Vec<SnapArtifact { service_id, snapshot_pos, path, len }> }` and `SnapshotSource` (T4) by T6; `FollowerReceiver::set_snapshot_intake(PathBuf, u64, Option<IncomingSnapshotSignal>)` and `FollowerStats::{snap_refused_legacy_peer, snap_refused_declared_mismatch}` (T5) by T6 and T7 (the addendum reads them off `ObsSources.receiver`, which already exists); `Resolve::{Won { user_data, fan_in, first }, Partial { first }, ..}` (M14b) by T1/T2; T7's family names by T8 (rules), T9 (status table wording) and T10; `SERVICE_STALE_NS == HEARTBEAT_STALE_NS` pinned by T9's test.

**Three facts worth re-checking during execution:**
1. T4 rotates artifacts on *send*; T5's `parts: Vec<SnapPart>` exists because of that. If a future change makes the sender wait for the last chunk's `SNAP_NAK`-silence before rotating, `parts` can shrink to one — not before.
2. T6's source uses `ring_ids()`/`ring_mask()` so `none_for_tests()` harness nodes still ship `{0}`; the receiver compares against its own `ring_mask()`. Both ends must keep using the same fold.
3. T7 renders aggregate + labelled twins in ONE family block; every PromQL in T8/T10 that means "the aggregate" must say `{service=""}` — `sum()` over the family double counts.
