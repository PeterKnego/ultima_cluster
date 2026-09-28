# UC2 Mandatory Snapshots (#67) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Snapshot support becomes a compile-time requirement of `ServiceBuilder::start()`, with a codec-neutral `WholeStateSnapshot` helper that makes it cheap for any FSM whose whole state can be encoded to bytes.

**Architecture:**
1. Add the helper trait with a blanket `SnapshotStateMachine` impl. The SDK owns the frame, the cursor, and the exclusive-frontier check; the app owns the encoding.
2. Migrate every snapshot-less FSM in the tree to be snapshot-capable. During this step each FSM still starts through the existing `start_with_snapshots()`, so every task compiles.
3. Fold `start_with_snapshots()` into `start()` and delete the snapshot-less path.

**Tech Stack:** Rust 1.96 (pinned) / MSRV 1.89; crate `uc_service` (SDK) plus its in-tree callers.

**Spec:** `docs/superpowers/specs/2026-09-28-uc2-mandatory-snapshots-design.md`. Read it first. Decisions D1–D5 bind every task.

## Global Constraints

- One `ServiceBuilder::start()` requiring `S: SnapshotStateMachine`. `start_with_snapshots()` is removed, with no deprecation shim (D1, D2).
- The helper is codec-neutral: `encode_state(&self) -> Result<Vec<u8>, SnapshotError>` and `decode_state(&mut self, &[u8]) -> Result<(), SnapshotError>`. `uc_service` imports no codec for it (D3).
- The helper is opt-in by trait impl, never automatic (D4).
- Helper frame: `cursor_present u8 ‖ cursor u64 LE ‖ app bytes`, inside the unchanged `ULTSNAP2` envelope.
- After `decode_state`, `self.last_applied()` must equal the recorded cursor. Otherwise return `SnapshotError::Codec("decode_state did not restore last_applied (encode it with the state)")` (D5).
- A recorded cursor above the tag gives `SnapshotError::Codec("mis-tagged: cursor above tag")`.
- There is no node, wire or cnc change. The node's refusal 48 and `PinRequiresSnapshots` stay.
- The typed-tier codec is untouched: it stays serde + bincode (SBE is a separate spec).
- Every task ends green on `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --all -- --check`. Before any push, also run the MSRV gate: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings`.
- Tests put instance dirs under `env!("CARGO_TARGET_TMPDIR")`. Scratch files go under `$HOME/scratch/`, never `/tmp`.
- No `Co-Authored-By` or any other attribution trailer in commits.
- `scripts/harness/*.rs` are frozen, out-of-workspace harness copies. Do not edit them.

## Review Focus

1. **An app forgets to encode `last_applied`.** Install must fail with the D5 message, not double-apply. Pinned by `install_refuses_a_decode_that_drops_the_cursor` (Task 1).
2. **A fresh FSM that has applied nothing (`last_applied() == None`) is frozen.** The round trip must restore `None`, not `Some(0)`. Pinned by `a_fresh_fsm_round_trips_none` (Task 1).
3. **A helper-based FSM wrapped in `Sessioned<S>` or `Timed<S>`.** It must compile and round-trip through the wrapper's own snapshot impl. Pinned by `wrappers_compose_with_the_helper` (Task 1).
4. **A helper-based FSM joins a purged cluster below the floor.** It must install and converge. Pinned by `a_helper_fsm_learner_joins_a_purged_leader` (Task 4).
5. **An FSM without snapshot support calls `start()`.** It must fail to compile. Pinned by the `compile_fail` doctest (Task 5).

---

## File Structure

| file | change |
|---|---|
| `uc_service/src/snapshots.rs` | `WholeStateSnapshot` trait, the blanket `SnapshotStateMachine` impl, frame helpers, unit tests |
| `uc_service/src/lib.rs` | re-export `WholeStateSnapshot`; (Task 5) fold `start_with_snapshots` into `start` |
| `uc_service/tests/whole_state_snapshot.rs` (new) | coherence and wrapper tests (a separate crate) |
| test/example FSMs in `uc_service/tests`, `uc_client/tests`, `uc_gateway/{tests,examples}`, `uc_node/{tests,examples}`, `testing/uc_crashtest`, `examples/counter` | gain snapshot support (Tasks 2–4) |
| every `.start_with_snapshots()` call | renamed `.start()` (Task 5) |
| docs (Task 6) | contract, how-tos, semver policy, #33 spec pointer |

---

### Task 1: The `WholeStateSnapshot` helper

**Files:**
- Modify: `uc_service/src/snapshots.rs` (append the trait, blanket impl and tests)
- Modify: `uc_service/src/lib.rs:~71-80` (re-export)
- Create: `uc_service/tests/whole_state_snapshot.rs`

**Interfaces:**
- Produces: `pub trait uc_service::WholeStateSnapshot: RawStateMachine { fn encode_state(&self) -> Result<Vec<u8>, SnapshotError>; fn decode_state(&mut self, bytes: &[u8]) -> Result<(), SnapshotError>; }` and `impl<S: WholeStateSnapshot> SnapshotStateMachine for S { type SnapshotHandle = Vec<u8>; … }`.

- [ ] **Step 1: Read** the trait contract at `uc_service/src/traits.rs:~440-520`: `freeze`, `stream_snapshot`, `install_snapshot`, the exclusive-frontier rule, and the provided `project()`. Then read one hand-written impl: `uc_lincheck/src/register.rs:76-125`.

- [ ] **Step 2: Write the failing unit tests.** Append them to `uc_service/src/snapshots.rs`'s test module.

```rust
#[cfg(test)]
mod whole_state_tests {
    use super::*;
    use crate::{ApplyCtx, RawStateMachine, SnapshotStateMachine};

    /// A raw FSM whose state is one u64; `forget_cursor` makes decode_state
    /// drop last_applied on purpose (Review Focus 1).
    #[derive(Default)]
    struct Sum { total: u64, last: Option<u64>, forget_cursor: bool }
    impl RawStateMachine for Sum {
        const NAME: &'static str = "sum";
        fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
            self.total += cmd.len() as u64;
            self.last = Some(ctx.position);
            out.clear();
        }
        fn query(&self, _q: &[u8], out: &mut Vec<u8>) { out.clear(); out.extend_from_slice(&self.total.to_le_bytes()); }
        fn last_applied(&self) -> Option<u64> { self.last }
    }
    impl WholeStateSnapshot for Sum {
        fn encode_state(&self) -> Result<Vec<u8>, SnapshotError> {
            let mut b = self.total.to_le_bytes().to_vec();
            b.push(self.last.is_some() as u8);
            b.extend_from_slice(&self.last.unwrap_or(0).to_le_bytes());
            Ok(b)
        }
        fn decode_state(&mut self, b: &[u8]) -> Result<(), SnapshotError> {
            if b.len() != 17 { return Err(SnapshotError::Codec("sum state".into())); }
            self.total = u64::from_le_bytes(b[0..8].try_into().unwrap());
            if !self.forget_cursor {
                self.last = (b[8] == 1).then(|| u64::from_le_bytes(b[9..17].try_into().unwrap()));
            }
            Ok(())
        }
    }

    fn freeze_bytes(s: &Sum) -> (Vec<u8>, u64) {
        let (h, pos) = s.freeze().unwrap();
        let mut out = Vec::new();
        Sum::stream_snapshot(h, &mut out).unwrap();
        (out, pos)
    }

    #[test]
    fn freeze_then_install_round_trips_state_and_cursor() {
        let s = Sum { total: 7, last: Some(4096), forget_cursor: false };
        let (bytes, pos) = freeze_bytes(&s);
        assert_eq!(pos, 4096);
        let mut t = Sum::default();
        // the tag is the instant P, an exclusive frontier at or above the cursor
        assert_eq!(t.install_snapshot(4160, &mut &bytes[..]).unwrap(), 4160);
        assert_eq!((t.total, t.last), (7, Some(4096)));
    }

    #[test]
    fn a_fresh_fsm_round_trips_none() {
        let (bytes, pos) = freeze_bytes(&Sum::default());
        assert_eq!(pos, 0);
        let mut t = Sum { total: 9, last: Some(1), forget_cursor: false };
        t.install_snapshot(64, &mut &bytes[..]).unwrap();
        assert_eq!((t.total, t.last), (0, None));
    }

    #[test]
    fn install_refuses_a_cursor_above_the_tag() {
        let (bytes, _) = freeze_bytes(&Sum { total: 1, last: Some(8192), forget_cursor: false });
        let err = Sum::default().install_snapshot(4096, &mut &bytes[..]).unwrap_err();
        assert!(err.to_string().contains("mis-tagged: cursor above tag"), "{err}");
    }

    #[test]
    fn install_refuses_a_decode_that_drops_the_cursor() {
        let (bytes, _) = freeze_bytes(&Sum { total: 1, last: Some(4096), forget_cursor: false });
        let mut t = Sum { forget_cursor: true, ..Default::default() };
        let err = t.install_snapshot(4160, &mut &bytes[..]).unwrap_err();
        assert!(err.to_string().contains("decode_state did not restore last_applied"), "{err}");
    }

    #[test]
    fn install_refuses_a_truncated_frame() {
        for n in 0..9 {
            let err = Sum::default().install_snapshot(64, &mut &[1u8; 9][..n]).unwrap_err();
            assert!(err.to_string().contains("whole-state frame"), "{n}: {err}");
        }
    }

    #[test]
    fn equal_state_freezes_to_equal_bytes() {
        let a = Sum { total: 3, last: Some(640), forget_cursor: false };
        let b = Sum { total: 3, last: Some(640), forget_cursor: true };
        assert_eq!(freeze_bytes(&a).0, freeze_bytes(&b).0);
    }
}
```

- [ ] **Step 3: Run the tests and confirm they fail.** Run `cargo test -p uc_service --lib whole_state_tests 2>&1 | tail -5`. Expected: compile errors naming `WholeStateSnapshot`.

- [ ] **Step 4: Implement.** Append to `uc_service/src/snapshots.rs`, adding imports as the compiler asks (`RawStateMachine` and `SnapshotStateMachine` from `crate::traits`, `SnapshotError` from `crate::config`).

```rust
/// Snapshot support for an FSM whose whole state can be written to bytes (#67).
///
/// The app owns the encoding (any codec — bincode today, SBE or a
/// hand-rolled layout tomorrow); the SDK owns everything else: the handle,
/// streaming, the cursor, and the exclusive-frontier check.
///
/// **Cost.** `encode_state` runs as `freeze`: on the apply thread with the
/// SM lock held, so each snapshot pauses apply for as long as encoding takes —
/// proportional to state size. For large state implement
/// [`SnapshotStateMachine`] directly (a persistent map pins in O(1)), or take
/// snapshots as standby instants on a learner so voters never pause.
///
/// **Determinism.** `encode_state` must turn equal states into equal bytes:
/// replicas' artifact hashes are compared (`SnapshotReport`). Never iterate a
/// `HashMap` into the bytes.
///
/// **The cursor.** `decode_state` must restore the state's own
/// `last_applied` — encode it with the rest of the state. The SDK records the
/// cursor at freeze and refuses an install whose `decode_state` did not bring
/// it back, because a lost cursor makes the apply loop re-apply frames the
/// snapshot already contains.
pub trait WholeStateSnapshot: RawStateMachine {
    /// Encode the whole state, including `last_applied`.
    fn encode_state(&self) -> Result<Vec<u8>, SnapshotError>;
    /// Replace the whole state from bytes `encode_state` produced.
    fn decode_state(&mut self, bytes: &[u8]) -> Result<(), SnapshotError>;
}

/// `cursor_present u8 ‖ cursor u64 LE`, ahead of the app's bytes.
const WHOLE_STATE_HEADER_LEN: usize = 9;

impl<S: WholeStateSnapshot> SnapshotStateMachine for S {
    type SnapshotHandle = Vec<u8>;

    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        let cursor = self.last_applied();
        let app = self.encode_state()?;
        let mut out = Vec::with_capacity(WHOLE_STATE_HEADER_LEN + app.len());
        out.push(cursor.is_some() as u8);
        out.extend_from_slice(&cursor.unwrap_or(0).to_le_bytes());
        out.extend_from_slice(&app);
        Ok((out, cursor.unwrap_or(0)))
    }

    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn std::io::Write) -> Result<(), SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
    }

    fn install_snapshot(&mut self, position: u64, src: &mut dyn std::io::Read) -> Result<u64, SnapshotError> {
        let mut buf = Vec::new();
        src.read_to_end(&mut buf)?;
        if buf.len() < WHOLE_STATE_HEADER_LEN || buf[0] > 1 {
            return Err(SnapshotError::Codec("whole-state frame".into()));
        }
        let recorded = (buf[0] == 1)
            .then(|| u64::from_le_bytes(buf[1..9].try_into().expect("8 bytes")));
        if recorded.unwrap_or(0) > position {
            return Err(SnapshotError::Codec("mis-tagged: cursor above tag".into()));
        }
        self.decode_state(&buf[WHOLE_STATE_HEADER_LEN..])?;
        if self.last_applied() != recorded {
            return Err(SnapshotError::Codec(
                "decode_state did not restore last_applied (encode it with the state)".into(),
            ));
        }
        Ok(position)
    }
}
```

Re-export it from `uc_service/src/lib.rs` beside the other `pub use` lines: `pub use crate::snapshots::WholeStateSnapshot;`.

If the blanket impl conflicts with an existing impl, the compiler reports E0119. The expected sites are `timed.rs:120` and `session.rs:318`. If that happens, stop and report it as BLOCKED with the exact error; do not restructure the traits.

- [ ] **Step 5: Run the unit tests and confirm they pass.** Run `cargo test -p uc_service --lib whole_state_tests`. Expected: 6 passed.

- [ ] **Step 6: Write the coherence and wrapper test.** Create `uc_service/tests/whole_state_snapshot.rs`. It is a separate crate, which is what coherence needs.

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! #67: the helper's blanket impl coexists with hand-written impls and with
//! the SDK's generic snapshot wrappers.

use uc_service::{ApplyCtx, RawStateMachine, Sessioned, SnapshotError, SnapshotStateMachine, Timed, WholeStateSnapshot};

#[derive(Default)]
struct Helper { v: u64, last: Option<u64> }
impl RawStateMachine for Helper {
    const NAME: &'static str = "helper";
    fn apply(&mut self, ctx: &mut ApplyCtx, _c: &[u8], out: &mut Vec<u8>) { self.v += 1; self.last = Some(ctx.position); out.clear(); }
    fn query(&self, _q: &[u8], out: &mut Vec<u8>) { out.clear(); }
    fn last_applied(&self) -> Option<u64> { self.last }
}
impl WholeStateSnapshot for Helper {
    fn encode_state(&self) -> Result<Vec<u8>, SnapshotError> {
        let mut b = self.v.to_le_bytes().to_vec();
        b.push(self.last.is_some() as u8);
        b.extend_from_slice(&self.last.unwrap_or(0).to_le_bytes());
        Ok(b)
    }
    fn decode_state(&mut self, b: &[u8]) -> Result<(), SnapshotError> {
        self.v = u64::from_le_bytes(b[0..8].try_into().unwrap());
        self.last = (b[8] == 1).then(|| u64::from_le_bytes(b[9..17].try_into().unwrap()));
        Ok(())
    }
}

/// A hand-written impl in the same crate as a helper-based one (D4: opt-in).
#[derive(Default)]
struct Manual { last: Option<u64> }
impl RawStateMachine for Manual {
    const NAME: &'static str = "manual";
    fn apply(&mut self, ctx: &mut ApplyCtx, _c: &[u8], out: &mut Vec<u8>) { self.last = Some(ctx.position); out.clear(); }
    fn query(&self, _q: &[u8], out: &mut Vec<u8>) { out.clear(); }
    fn last_applied(&self) -> Option<u64> { self.last }
}
impl SnapshotStateMachine for Manual {
    type SnapshotHandle = ();
    fn freeze(&self) -> Result<((), u64), SnapshotError> { Ok(((), self.last.unwrap_or(0))) }
    fn stream_snapshot(_h: (), _d: &mut dyn std::io::Write) -> Result<(), SnapshotError> { Ok(()) }
    fn install_snapshot(&mut self, p: u64, _s: &mut dyn std::io::Read) -> Result<u64, SnapshotError> { Ok(p) }
}

fn assert_snapshot_capable<S: SnapshotStateMachine>() {}

#[test]
fn both_kinds_are_snapshot_capable() {
    assert_snapshot_capable::<Helper>();
    assert_snapshot_capable::<Manual>();
}

#[test]
fn wrappers_compose_with_the_helper() {
    assert_snapshot_capable::<Timed<Helper>>();
    assert_snapshot_capable::<Sessioned<Helper>>();
    assert_snapshot_capable::<Timed<Manual>>();
}
```

Check the real wrapper type parameters before writing the test. If `Timed` wraps only the typed tier (`Timed<S: StateMachine>`), use a typed helper FSM for that line and say so in the report. Also check the constructors and exports with `grep -n "pub struct Timed\|pub struct Sessioned\|impl<S" uc_service/src/timed.rs uc_service/src/session.rs | head`.

Add one round trip through `Sessioned<Helper>`: construct it the way `uc_service/src/session.rs`'s own tests do, then freeze → stream → install and compare `last_applied()`. This is Review Focus 3.

- [ ] **Step 7: Run** `cargo test -p uc_service --test whole_state_snapshot` and `cargo test -p uc_service`. Expected: all pass. Then run clippy and fmt.

- [ ] **Step 8: Commit.** `git commit -am "service(#67): WholeStateSnapshot — codec-neutral snapshot helper with an SDK-owned cursor check"` (also `git add` the new test file).

---

### Task 2: Migrate the SDK-side test FSMs (`uc_service`, `uc_client`, `uc_gateway`)

**Files:** every file in `uc_service/tests/`, `uc_client/tests/`, `uc_gateway/tests/` and `uc_gateway/examples/m12_gate.rs` that calls `ServiceBuilder::…start()` on an FSM without snapshot support. The candidates, by `.start()` count, are:
- `uc_service/tests/{reconstruction,output,query,apply,pinned_attach}.rs`
- `uc_client/tests/{pipelined,roundtrip}.rs`
- `uc_gateway/tests/{credits_wire,credits,roundtrip,common/mod}.rs`
- `uc_gateway/examples/m12_gate.rs`

**Interfaces:**
- Consumes: `uc_service::WholeStateSnapshot` (Task 1).
- Produces: every FSM these files attach is snapshot-capable and started with `.start_with_snapshots()`. Task 5 renames the call.

Some `.start()` matches may be on other types. Only `ServiceBuilder` calls count.

- [ ] **Step 1: List the real sites.** Run `grep -n "\.start()" <files>` and read each call. List every FSM type that is started with `.start()` and implements neither `SnapshotStateMachine` nor the helper.

- [ ] **Step 2: Give each such FSM a `WholeStateSnapshot` impl.**
  - Encode every field of the FSM, including its `last_applied`.
  - Typed FSMs already depend on `bincode` and `serde`, so they may encode a tuple of their fields with `bincode::serde::encode_to_vec(…, bincode::config::standard())`.
  - Raw FSMs encode their integers with `to_le_bytes`, as in Task 1's `Sum`.
  - Within one test crate, put a shared impl or macro in that crate's `tests/common` module when two or more files attach the same FSM shape.
  - Switch the call to `.start_with_snapshots()`.
  - Do not change what any test asserts.

- [ ] **Step 3: Run the affected crates' tests.** Run `cargo test -p uc_service -p uc_client -p uc_gateway`. If a test's behaviour changes, it is most likely because the row is now snapshot-capable and so answers an instant it used to refuse. Stop, report which test and why, and do not edit its assertion.

- [ ] **Step 4: Run the m12 smoke, direct arm.** Use the smallest args documented in `uc_gateway/examples/m12_gate.rs`'s module doc and confirm non-zero throughput. The gateway arm is broken before this work (#69); do not run it.

- [ ] **Step 5: Run clippy and fmt, then commit.** `git commit -am "test(#67): SDK-side test FSMs are snapshot-capable"`.

---

### Task 3: Migrate `uc_node` tests and examples, and the crashtest service

**Files:**
- `uc_node/tests/`: `services.rs`, `backup.rs`, `crypto_cluster.rs`, `query_barrier.rs`, `lincheck_v2/mod.rs`, `timers.rs`, `obs_http.rs`, `jumbo.rs`, `crypto_adversarial.rs`, `admin_auth.rs`
- `uc_node/examples/`: `apply_bench.rs`, `m5_gate.rs`, `read_profile.rs`, `m7_gate.rs`, `m10_gate.rs`, `m10_alerts.rs`
- `testing/uc_crashtest/src/bin/uc_crashtest-service.rs`

**Interfaces:**
- Consumes: `WholeStateSnapshot` (Task 1).

- [ ] **Step 1: List the real `ServiceBuilder::start()` sites** in these files, as in Task 2 Step 1.

- [ ] **Step 2: Migrate each FSM** exactly as in Task 2 Step 2, then switch each call to `.start_with_snapshots()`.
  - `uc_node/tests` is one test crate per file, so share impls only within a file.
  - `apply_bench.rs` (`RawCount`): add the impl only. The spec's claim that no A/B is owed rests on the harness commanding no instant. Confirm that with `grep -n "snapshot\|SNAPSHOT\|instant" uc_node/examples/apply_bench.rs`, and quote the result in the report. If it does command an instant, stop and report.

- [ ] **Step 3: Run the tests.**
  - `cargo test -p uc_node --no-fail-fast` in the background with bounded polls. It is long.
  - `cargo test -p uc_crashtest --features hard-crash-tests`.
  - Apply the same "behaviour change → stop and report" rule as Task 2 Step 3.

- [ ] **Step 4: Run the gate smokes #33 fixed** with their smallest documented args, and confirm each starts and moves traffic: m5, m6, m7, m10_gate probes, and `read_profile`. m6 is included even though it may already use snapshots.

- [ ] **Step 5: Run clippy (including `-p uc_crashtest --features hard-crash-tests`) and fmt, then commit.** `git commit -am "test(#67): uc_node tests/examples and the crashtest service are snapshot-capable"`.

---

### Task 4: `examples/counter` uses the helper, and a helper-based learner joins a purged cluster

> Spec §7.4 names `examples/counter` for the end-to-end join; `uc_node`'s tests cannot depend on an example crate, so the join is proven with a helper-based FSM in `uc_node/tests/learner.rs` and `counter` gets its own snapshot round-trip test — the same claim, split across the two crates that can each test it.

**Files:**
- Modify: `examples/counter/src/lib.rs` (`CounterSm`), `examples/counter/src/bin/counter-service.rs`, `examples/counter/src/bin/counter-single.rs`
- Modify: `uc_node/tests/learner.rs` (a new test beside `fresh_learner_joins_a_purged_leader_via_snapshot_session`, ~:686)

**Interfaces:**
- Consumes: `WholeStateSnapshot`.

- [ ] **Step 1: Write the failing test.** Add a `counter` lib unit test in `examples/counter/src/lib.rs`.

```rust
#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use uc_service::SnapshotStateMachine;

    #[test]
    fn counter_round_trips_value_and_cursor() {
        let c = CounterSm { value: -5, last_applied: Some(640) };
        let (h, pos) = c.freeze().unwrap();
        let mut bytes = Vec::new();
        CounterSm::stream_snapshot(h, &mut bytes).unwrap();
        let mut d = CounterSm::default();
        d.install_snapshot(pos + 64, &mut &bytes[..]).unwrap();
        assert_eq!((d.value, d.last_applied), (-5, Some(640)));
    }
}
```

Run `cargo test -p counter --lib snapshot_tests`. Expected: a compile error, because there is no `SnapshotStateMachine` for `CounterSm`.

- [ ] **Step 2: Implement.** In `examples/counter/src/lib.rs`, the comment is teaching material, so keep it plain:

```rust
/// Snapshots are required (#67). A counter's whole state is two numbers, so
/// the simple helper fits: encode the state, decode it back, and the SDK
/// handles the rest. A state machine with a large state should implement
/// `SnapshotStateMachine` itself instead — see `docs/reference/state-machine-contract.md`.
impl uc_service::WholeStateSnapshot for CounterSm {
    fn encode_state(&self) -> Result<Vec<u8>, uc_service::SnapshotError> {
        bincode::serde::encode_to_vec((self.value, self.last_applied), bincode::config::standard())
            .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))
    }
    fn decode_state(&mut self, bytes: &[u8]) -> Result<(), uc_service::SnapshotError> {
        let ((value, last_applied), _): ((i64, Option<u64>), _) =
            bincode::serde::decode_from_slice(bytes, bincode::config::standard())
                .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))?;
        self.value = value;
        self.last_applied = last_applied;
        Ok(())
    }
}
```

In both bins, change the call to `.start_with_snapshots()`. If `CounterSm`'s fields are private and the test module needs them, the test lives in the same module, so it has access.

- [ ] **Step 3: Run** `cargo test -p counter`. Expected: pass.

- [ ] **Step 4: Add the helper-based learner join test.** In `uc_node/tests/learner.rs`, add `a_helper_fsm_learner_joins_a_purged_leader`, which is a copy of `fresh_learner_joins_a_purged_leader_via_snapshot_session` (~:686-…).
  - Use a local FSM with the same logic as that test's `SumSm` that implements `WholeStateSnapshot` instead of the hand-written trait. Read `SumSm` at ~:880-935 and reuse its apply/query/last_applied logic. The only difference is the snapshot path.
  - Keep every assertion of the original: the learner converges below the floor and its value matches.
  - Run `cargo test -p uc_node --test learner a_helper_fsm_learner_joins`. Expected: pass.
  - Then force a failure to confirm the test has teeth. Make the helper's `decode_state` skip the cursor, run the test, and confirm it FAILS through the D5 check. Restore it. Record both runs in the report.

- [ ] **Step 5: Run** `cargo test -p uc_node --test learner` and `examples/kv/scripts/kvcluster.sh`'s test only if counter's bins are part of it (check first). Then run clippy and fmt, and commit: `git commit -am "examples(#67): counter uses WholeStateSnapshot; a helper FSM learner joins a purged leader"`.

---

### Task 5: One `start()`: fold `start_with_snapshots` in and delete the snapshot-less path

**Files:**
- Modify: `uc_service/src/lib.rs:~207-420` (`start`, `start_with_snapshots`)
- Modify: every `.start_with_snapshots()` call in the workspace (~73 + those added in Tasks 2–4)

**Interfaces:**
- Produces: `pub fn start(self) -> Result<Service<S>, ServiceError> where S: SnapshotStateMachine`. No `start_with_snapshots` remains.

- [ ] **Step 1: Write the failing compile-fail doctest.** Put it on `ServiceBuilder` or `start`:

````rust
/// A state machine without snapshot support does not start (#67):
///
/// ```compile_fail
/// use uc_service::{ApplyCtx, RawStateMachine, ServiceBuilder, ServiceConfig};
/// #[derive(Default)]
/// struct NoSnap;
/// impl RawStateMachine for NoSnap {
///     const NAME: &'static str = "nosnap";
///     fn apply(&mut self, _c: &mut ApplyCtx, _cmd: &[u8], out: &mut Vec<u8>) { out.clear(); }
///     fn query(&self, _q: &[u8], out: &mut Vec<u8>) { out.clear(); }
///     fn last_applied(&self) -> Option<u64> { None }
/// }
/// let _ = ServiceBuilder::new(ServiceConfig::new("/nonexistent", "a"), NoSnap).start();
/// ```
````

Run `cargo test -p uc_service --doc`. Expected: this doctest FAILS, because today's `start()` compiles for `NoSnap`, and a `compile_fail` doctest fails when the code compiles. Record that failure.

- [ ] **Step 2: Implement.**
  - Delete today's `start()` body.
  - Rename `start_with_snapshots` to `start`, keeping its `where S: SnapshotStateMachine` bound and its body.
  - Rewrite the doc comment. Drop the "M6 Task 3 controller-resolved deviation" paragraph about opting in. State that snapshot support is required, name the two ways to provide it (`WholeStateSnapshot` or `SnapshotStateMachine`), and keep the capability-bit paragraph.
  - Delete the Plan B2 T4 comment that described the snapshot-less `None` path.
  - The `attach(&cfg, sm, Some(install))` call is now the only one. If `attach`'s `install: Option<…>` parameter has no other caller that passes `None`, leave the signature alone. Changing it is not in scope; note it in the report.

- [ ] **Step 3: Rename every caller.** Run `grep -rln "start_with_snapshots" --include=*.rs --include=*.md . | grep -v target`. Replace the call in every `.rs` file outside `scripts/harness/`. Leave `.md` hits to Task 6.

- [ ] **Step 4: Run the tests.**
  - `cargo test -p uc_service --doc`: the compile_fail doctest now passes.
  - `cargo build --workspace --all-targets`.
  - `cargo test --workspace --no-fail-fast` in the background with bounded polls.
  - `cargo test -p uc_crashtest --features hard-crash-tests`.
  - Rebuild the diffreplay fixtures (`cargo build -p uc_lincheck --features replay-bin --bin register-replay && cargo build -p uc_diffreplay`), then run `cargo test -p uc_diffreplay --test pin_verify -- --test-threads=1`.

- [ ] **Step 5: Run clippy on stable and on MSRV 1.89, plus with hard-crash-tests, and fmt. Then commit.** `git commit -am "service(#67): start() requires snapshot support; start_with_snapshots removed"`.

---

### Task 6: Docs

**Files:**
- `docs/reference/state-machine-contract.md`
- the how-tos and tutorials found by the grep below
- `docs/reference/semver-policy.md`
- `docs/how-to/upgrade-an-application.md`
- `docs/notes/uc2-row-running-version-explained.md`
- the #33 spec's errata block

- [ ] **Step 1: Find every statement to change.** Run:

```bash
grep -rn "start_with_snapshots\|start()\|optional\|without snapshots\|snapshot-capable\|snapshots are" docs README.md docs/QUICKSTART.md --include=*.md | grep -v superpowers
```

List each hit that says snapshots are optional, shows `start_with_snapshots`, or describes a snapshot-less row. Leave historical release notes (`RELEASES.md`, `docs/releases.md`) as they are.

- [ ] **Step 2: Update `state-machine-contract.md`.**
  - Snapshot support is required.
  - There are two ways to provide it. Show the full `WholeStateSnapshot` example (`CounterSm`'s impl), and point to the full `SnapshotStateMachine` trait for large state.
  - State the helper's cost: the pause under the apply lock, proportional to state size, and the standby-instant alternative.
  - Encoding must be deterministic (the `HashMap` warning).
  - Explain the cursor check.

- [ ] **Step 3: Rewrite every other hit from Step 1.**
  - In `upgrade-an-application.md`, drop the "a row that cannot snapshot cannot change version" caveat.
  - In `uc2-row-running-version-explained.md` §"why only a pin", add one sentence: #67 made snapshots mandatory.
  - In `semver-policy.md`, add a line recording the SDK API change.
  - In the #33 spec's errata block, add a bullet: "#67 (2026-09-28) made snapshot support a compile-time requirement; the genesis-pin rejection (§3.1) stands."

- [ ] **Step 4: Run** `scripts/check_doc_links.py`. Expected: 0 errors.

- [ ] **Step 5: Commit.** `git commit -am "docs(#67): snapshots are required; the WholeStateSnapshot helper"`. The `uc_starter` template repo's how-to is out of scope; name it in the PR body as a follow-up.

---

### Task 7: Proof stack

**Files:** none. This task produces evidence only, and any fix goes back to its owning task's pattern.

- [ ] **Step 1: Run the full stack** in the background with bounded polls. Logs go to `$HOME/scratch/rv67/`.

```bash
cargo build -p uc_lincheck --features replay-bin --bin register-replay && cargo build -p uc_diffreplay
cargo test --workspace --no-fail-fast
cargo test -p uc_node --test lin_v2
cargo test -p uc_node --test lin_partition_v2
cargo test -p uc_crashtest --features hard-crash-tests
cargo test -p uc_diffreplay --test pin_verify -- --test-threads=1
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p uc_crashtest --all-targets --features hard-crash-tests -- -D warnings
CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all -- --check
scripts/check_doc_links.py
```

- [ ] **Step 2: Report.** Give each command's result with its log path and last lines. Commit only if a fix was needed, and name the task it belongs to.
