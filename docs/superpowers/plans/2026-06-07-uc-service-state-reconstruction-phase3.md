# Service-State Reconstruction — Phase 3 (Contract Flip + Parity + Capstone Proof) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close out the service-state reconstruction feature: re-enable a reconstruction-aware node-side `last_applied` cross-check, revert the lincheck `RegisterSm` to plain in-memory, and prove end-to-end that a non-persisting in-memory SM survives both node-kill and service-crash failover (lincheck capstone), then consolidate the 4-phase feature into a canonical task doc.

**Architecture:** Phases 1/2a/2b already ship reconstruction (mid-life reattach replay + bidirectional snapshot install + async build). Phase 3 removes the two scaffolds that masked it — the skipped cross-check and the self-persisting test SM — and adds the capstone proof. The cross-check uses **upper-bound sanity (Option A, user-approved):** a service that reports `last_applied` ABOVE the node's known log frontier is impossible (corruption / wrong incarnation) → refuse; a service at-or-below is reconstructed, not refused.

**Tech Stack:** Rust, openraft 0.10, `ultima_journal` (`Journal::last_seq()`), shmem cnc (`ServiceStatus.last_applied`/`service_epoch` atomics), the WGL lincheck harness (`uc_node/tests/lincheck/`).

**Branch:** `feat/service-reconstruction-phase3` (already created off `main`).

---

## Background facts the implementer needs (verified)

- **Cross-check timing works at `new()`.** `runtime/builder.rs` calls `wait_for_service_ready(...)` (line ~128) BEFORE `ShmemAdaptedStateMachine::new(...)` (line ~155), so by `new()` the service is `Ready` and has published `ServiceStatus.last_applied`. Read it with the existing `service_last_of(service_status_ptr)` helper (`state_machine_shmem.rs:88`).
- **Upper bound at startup** = `journal.last_seq().unwrap_or(0)` — the highest appded log index the node durably holds. A service cannot have applied beyond it. `journal` is already a param of `new()` and a field of `ShmemInner`.
- **Upper bound at reattach** = `up_to` (the node's live frontier / parked entry index) inside `drive_catchup`.
- **The original cross-check is incompatible with reconstruction.** `state_machine.rs:113-148` refuses on `(user=None, framework=Some)` and `(Some(u), Some(f)) if u != f`. A fresh in-memory service reports `0/None` while the framework is at `Some(N)` — the legitimate reconstruction case. Do NOT re-enable that predicate; use the upper-bound predicate below.
- **`ClusterError::DriftDetected { user: Option<u64>, framework: Option<u64> }`** (`error.rs:13`) is the refuse error at `new()`. `drive_catchup` returns `io::Error`, so map the same condition to `io::Error::other(...)` there (its errors are node-fatal, which IS the intended "refuse").
- **`RegisterSm` self-persistence was added in `162b7ad`;** the Phase 2b trait methods (`freeze`/`stream_snapshot`/`install_snapshot`, `type SnapshotHandle = Vec<u8>`) were added later in `b4d8d2a` and MUST be kept. The original plain in-memory `apply` (commit `ce96bf9`) had **no idempotency guard** — the guard came with persistence in `162b7ad`, so the revert removes it too.
- **The capstone already enables both faults** (`lin_register.rs:212` `fault_rng.random_bool(0.5)` picks node-kill vs service-crash). Phase 3 does not re-add faults; it proves they still pass once `RegisterSm` no longer self-persists.

---

## File Structure

- `uc_node/src/runtime/reconstruct.rs` — add the pure `service_not_ahead` guard + unit tests (lives beside `plan_replay`; same module that owns the reconstruction decision).
- `uc_node/src/raft/state_machine_shmem.rs` — call the guard in `new()` (replace the skip block) and in `drive_catchup` (before `plan_replay`).
- `uc_node/tests/lincheck/register_sm.rs` — strip persistence → plain in-memory; keep Phase 2b trait surface; drop persistence unit tests.
- `uc_node/tests/lincheck/cluster.rs` — `spawn_service` uses `RegisterSm::default()`; fix comments.
- `uc_node/tests/lin_register.rs` — fix stale "persists its own state" comments; capstone is the proof.
- `uc_service/src/state_machine.rs` + `CLAUDE.md` — document `last_applied()` is load-bearing; update the "service crash"/"in-memory SMs lose state"/"safe purge" statements.
- `docs/tasks/task14_service_state_reconstruction.md` — NEW canonical consolidation of phases 1/2a/2b/3.

---

## Task 1: Re-enable the node-side `last_applied` cross-check (upper-bound, Option A)

**Files:**
- Modify: `uc_node/src/runtime/reconstruct.rs` (add guard + tests)
- Modify: `uc_node/src/raft/state_machine_shmem.rs` (call at `new()` ~lines 229-235; call in `drive_catchup` before `plan_replay` ~line 753)

- [ ] **Step 1: Write the failing unit test for the pure guard**

In `uc_node/src/runtime/reconstruct.rs`, inside `mod tests`, add:

```rust
#[test]
fn service_not_ahead_ok_when_at_or_below_frontier() {
    assert!(service_not_ahead(0, 5).is_ok()); // fresh in-memory service
    assert!(service_not_ahead(5, 5).is_ok()); // exactly caught up
    assert!(service_not_ahead(3, 5).is_ok()); // behind → reconstructed
    assert!(service_not_ahead(0, 0).is_ok()); // cold start, empty log
}

#[test]
fn service_not_ahead_refuses_when_above_frontier() {
    // Service claims to have applied an index the node never logged: corruption
    // / wrong incarnation. Refuse with both values for the operator.
    assert_eq!(service_not_ahead(6, 5), Err(DriftBound { service_last: 6, frontier: 5 }));
    assert_eq!(service_not_ahead(1, 0), Err(DriftBound { service_last: 1, frontier: 0 }));
}
```

- [ ] **Step 2: Run it to verify it fails to compile (function not defined)**

Run: `cargo test -p uc_node --lib runtime::reconstruct 2>&1 | head`
Expected: compile error — `service_not_ahead` / `DriftBound` not found.

- [ ] **Step 3: Implement the pure guard**

In `reconstruct.rs` (above `mod tests`):

```rust
/// Upper-bound divergence signal: the reattached/booting service reports a
/// `last_applied` (`service_last`) STRICTLY ABOVE the node's known log
/// `frontier` (the journal tail at startup, or the live apply frontier at
/// reattach). That is impossible in correct operation — the service only ever
/// applies entries this node delivered — so it indicates corruption or a service
/// from a different incarnation/cluster. A service at-or-below the frontier is
/// the normal reconstruction case and is NOT a divergence.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DriftBound {
    pub(crate) service_last: u64,
    pub(crate) frontier: u64,
}

/// Refuse iff the service is ahead of the node's log frontier. See [`DriftBound`].
pub(crate) fn service_not_ahead(service_last: u64, frontier: u64) -> Result<(), DriftBound> {
    if service_last > frontier {
        Err(DriftBound { service_last, frontier })
    } else {
        Ok(())
    }
}
```

- [ ] **Step 4: Run the unit tests to verify they pass**

Run: `cargo test -p uc_node --lib runtime::reconstruct 2>&1 | tail -5`
Expected: PASS (existing `plan_replay` tests + the 2 new ones).

- [ ] **Step 5: Wire the guard into `new()` — replace the skip block**

In `state_machine_shmem.rs`, the current block (~lines 229-235) is:

```rust
        if loaded_last_applied.is_some() {
            tracing::warn!(
                framework_last_applied = ?loaded_last_applied,
                "shmem mode: skipping user/framework last_applied cross-check \
                 (deferred until cnc-sub-mmap MPSC attach lands)"
            );
        }
```

Replace it with the upper-bound cross-check (Phase 3). `journal` and `service_status_ptr` are in scope as `new()` params:

```rust
        // Phase 3 cross-check (upper-bound / Option A). By the time the node builds
        // the adapter, the builder has already awaited `wait_for_service_ready`, so
        // the service has published `ServiceStatus.last_applied`. The service can
        // only have applied entries this node logged, so a reported index ABOVE the
        // journal tail is impossible — corruption or a wrong incarnation. Refuse.
        // A service at-or-below the tail (incl. a fresh in-memory service at 0) is
        // the normal reconstruction case and is allowed through; reconstruction (the
        // apply/reattach path) brings it up to the node frontier.
        let service_last = service_last_of(service_status_ptr);
        let log_tail = journal.last_seq().unwrap_or(0);
        if let Err(d) = crate::runtime::reconstruct::service_not_ahead(service_last, log_tail) {
            return Err(crate::ClusterError::DriftDetected {
                user: Some(d.service_last),
                framework: Some(d.frontier),
            });
        }
        if loaded_last_applied.is_some() {
            tracing::debug!(
                framework_last_applied = ?loaded_last_applied,
                service_last,
                log_tail,
                "shmem startup cross-check passed (service at-or-below log tail)"
            );
        }
```

- [ ] **Step 6: Wire the guard into `drive_catchup` — before `plan_replay`**

In `state_machine_shmem.rs::drive_catchup`, locate where `service_last` is obtained and `plan_replay(service_last, up_to, last_purged)` is called (the `match ... ReplayPlan` around line 740-754). Immediately BEFORE the `plan_replay` call, add:

```rust
        // Phase 3 cross-check at reattach: the live node frontier (`up_to`) is the
        // upper bound. A reattached service reporting an index above it claims state
        // the node has not applied — refuse (drive_catchup errors are node-fatal,
        // which IS the intended refusal). At-or-below is replayed/reconstructed.
        if let Err(d) = crate::runtime::reconstruct::service_not_ahead(service_last, up_to) {
            return Err(io::Error::other(format!(
                "service reattach drift: service last_applied={} > node frontier={}",
                d.service_last, d.frontier
            )));
        }
```

NOTE: this tightens the previously-tolerated `service_last > up_to` case (which `plan_replay`'s `service_last >= up_to` clamp silently absorbed). The legitimate `service_last == up_to` case (re-confirm the parked entry) still flows through `plan_replay` unchanged. If `service_last` is not already a local binding at that point, read it via `service_last_of(ss_ptr)` (the same value the reattach branch already uses).

- [ ] **Step 7: Build + clippy**

Run: `cargo clippy -p uc_node --all-targets -- -D warnings 2>&1 | tail -3`
Expected: clean.

- [ ] **Step 8: Run the reconstruction integration suites (must stay green)**

Run: `cargo test -p uc_node --test reconstruct_reattach --test reconstruct_snapshot -- --test-threads=1 2>&1 | grep "test result"`
Expected: all PASS (the cross-check never fires for legitimate reconstruction — services come back at-or-below the frontier).

- [ ] **Step 9: Commit**

```bash
git add uc_node/src/runtime/reconstruct.rs uc_node/src/raft/state_machine_shmem.rs
git commit -m "feat(reconstruct): re-enable node-side last_applied cross-check (upper-bound)

Phase 3. Refuse only when the service reports last_applied ABOVE the node's
log frontier (journal tail at startup, live frontier at reattach) — the one
impossible/corruption case. A service at-or-below (incl. fresh in-memory at 0)
is the normal reconstruction case, not drift. Replaces the skipped cross-check
left from Phase 1. Pure guard service_not_ahead() + unit tests."
```

---

## Task 2: Revert `RegisterSm` to plain in-memory

**Files:**
- Modify: `uc_node/tests/lincheck/register_sm.rs`
- Modify: `uc_node/tests/lincheck/cluster.rs`
- Modify: `uc_node/tests/lin_register.rs`

- [ ] **Step 1: Rewrite `register_sm.rs` as plain in-memory (keep the Phase 2b trait surface)**

Replace the whole file with the version below. Removed: `data_dir` field, `persist()`, the reload in `new()`, `STATE_FILE`, the idempotency guard, and the persistence unit tests. Kept: `Cmd`/`CmdResp`, `value`/`last_applied`, `freeze`/`stream_snapshot`/`install_snapshot` (Phase 2b), `query`, `last_applied`.

```rust
//! The replicated CAS-register state machine the lincheck capstone runs. Mirrors
//! the `Counter` test SM shape in m2/m3. `Read` is a Query; `Write`/`Cas` are
//! Commands.
//!
//! This SM is **plain in-memory** — it persists NOTHING. That is deliberate: it
//! is the proof object for service-state reconstruction. When the service crashes
//! and restarts, it comes back empty (value=None); the node reconstructs it from
//! the replicated log (mid-life reattach replay, or snapshot-install + tail replay
//! when the gap is below the purge boundary). The lincheck capstone exercises both
//! node-kill and service-crash faults against this non-persisting SM and asserts
//! linearizability — see docs/tasks/task14_service_state_reconstruction.md.

use std::io::{Read as IoRead, Write as IoWrite};

use serde::{Deserialize, Serialize};
use uc_service::{SnapshotError, StateMachine};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Cmd {
    Write(u64),
    Cas { old: u64, new: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CmdResp {
    WriteAck,
    CasResult(bool),
}

#[derive(Default)]
pub struct RegisterSm {
    value: Option<u64>,
    last_applied: Option<u64>,
}

impl StateMachine for RegisterSm {
    type Command = Cmd;
    type Response = CmdResp;
    type Query = (); // Read
    type QueryResponse = Option<u64>;
    type SnapshotHandle = Vec<u8>;

    fn apply(&mut self, log_index: u64, cmd: Cmd) -> CmdResp {
        let resp = match cmd {
            Cmd::Write(v) => {
                self.value = Some(v);
                CmdResp::WriteAck
            }
            Cmd::Cas { old, new } => {
                if self.value == Some(old) {
                    self.value = Some(new);
                    CmdResp::CasResult(true)
                } else {
                    CmdResp::CasResult(false)
                }
            }
        };
        self.last_applied = Some(log_index);
        resp
    }
    fn query(&self, _q: ()) -> Option<u64> {
        self.value
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
    fn freeze(&self) -> Result<(Vec<u8>, u64), SnapshotError> {
        let buf = bincode::serde::encode_to_vec(
            (self.value, self.last_applied),
            bincode::config::standard(),
        )
        .map_err(|e| SnapshotError::Codec(e.to_string()))?;
        Ok((buf, self.last_applied.unwrap_or(0)))
    }
    fn stream_snapshot(handle: Vec<u8>, dst: &mut dyn IoWrite) -> Result<(), SnapshotError> {
        dst.write_all(&handle)?;
        Ok(())
    }
    fn install_snapshot(&mut self, src: &mut dyn IoRead) -> Result<u64, SnapshotError> {
        let mut buf = Vec::new();
        src.read_to_end(&mut buf)?;
        let ((v, la), _) = bincode::serde::decode_from_slice::<(Option<u64>, Option<u64>), _>(
            &buf,
            bincode::config::standard(),
        )
        .map_err(|e| SnapshotError::Codec(e.to_string()))?;
        self.value = v;
        self.last_applied = la;
        Ok(la.unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_then_cas_in_memory() {
        let mut sm = RegisterSm::default();
        assert_eq!(sm.apply(1, Cmd::Write(7)), CmdResp::WriteAck);
        assert_eq!(sm.apply(2, Cmd::Cas { old: 7, new: 9 }), CmdResp::CasResult(true));
        assert_eq!(sm.apply(3, Cmd::Cas { old: 7, new: 1 }), CmdResp::CasResult(false));
        assert_eq!(sm.query(()), Some(9));
        assert_eq!(sm.last_applied(), Some(3));
    }

    #[test]
    fn freeze_install_roundtrip() {
        let mut sm = RegisterSm::default();
        sm.apply(1, Cmd::Write(42));
        let (handle, idx) = sm.freeze().unwrap();
        assert_eq!(idx, 1);
        let mut bytes = Vec::new();
        RegisterSm::stream_snapshot(handle, &mut bytes).unwrap();
        let mut restored = RegisterSm::default();
        assert_eq!(restored.install_snapshot(&mut std::io::Cursor::new(bytes)).unwrap(), 1);
        assert_eq!(restored.query(()), Some(42));
        assert_eq!(restored.last_applied(), Some(1));
    }
}
```

- [ ] **Step 2: Update `cluster.rs::spawn_service` to use `RegisterSm::default()`**

In `uc_node/tests/lincheck/cluster.rs`, change the `spawn_service` body (~lines 100-103):

```rust
    // Service-side SM persists its state to `data_dir` (see register_sm.rs) so a
    // service-only restart recovers — the node does not replay history into a
    // reconnecting service.
    ServiceBuilder::new(cfg, RegisterSm::new(data_dir.to_owned()))
```

to:

```rust
    // Service-side SM is plain in-memory (register_sm.rs persists nothing). A
    // service-only restart therefore comes back EMPTY; the node reconstructs it
    // from the replicated log — that recovery is exactly what the capstone proves.
    ServiceBuilder::new(cfg, RegisterSm::default())
```

Leave `ServiceConfig.data_dir`/`svc_data_dir` as-is (the service framework still uses its own instance dir; only the SM stopped writing there).

- [ ] **Step 3: Fix stale persistence comments in `cluster.rs` and `lin_register.rs`**

In `cluster.rs`, update the comment at the node-restart site (~line 416 "rejoin via persisted data_dir" is still true for the NODE — leave it) but fix any comment claiming the SERVICE SM persists. In `lin_register.rs`, rewrite the capstone doc comment block (~lines 145-207) that says "RegisterSm persists its own state" / "recovers from disk" to state that the in-memory SM is **reconstructed by the node** (mid-life reattach replay; snapshot-install + tail replay below purge). Keep the fault description (both kinds, quorum-preserving, recovery between faults).

- [ ] **Step 4: Build + clippy the test target**

Run: `cargo clippy -p uc_node --all-targets -- -D warnings 2>&1 | tail -3`
Expected: clean (no unused `data_dir`/`PathBuf`/`IoRead` warnings; remove now-unused imports if the compiler flags them).

- [ ] **Step 5: Run the register_sm unit tests**

Run: `cargo test -p uc_node --test lin_register --lib 2>&1 | grep "test result" || cargo test -p uc_node --test lin_register register_sm 2>&1 | tail -5`
(NOTE: `register_sm.rs` is a module under the `lin_register`/`lincheck` test crate; the unit tests run as part of that test binary. If they are not picked up standalone, they run inside Task 3's capstone build.)
Expected: the 2 new unit tests PASS.

- [ ] **Step 6: Commit**

```bash
git add uc_node/tests/lincheck/register_sm.rs uc_node/tests/lincheck/cluster.rs uc_node/tests/lin_register.rs
git commit -m "test(lincheck): revert RegisterSm to plain in-memory (drop self-persistence)

Phase 3. RegisterSm now persists nothing and has no idempotency guard — it is
the proof object for reconstruction. A service-only restart comes back empty and
is reconstructed by the node (reattach replay / snapshot-install + tail). Keeps
the Phase 2b freeze/stream_snapshot trait surface. Reverts the 162b7ad workaround;
service-side SM built via RegisterSm::default()."
```

---

## Task 3: Lincheck capstone proof (both faults, in-memory SM, multi-seed)

**Files:**
- Modify (if needed): `uc_node/tests/lin_register.rs`

- [ ] **Step 1: Run the capstone on the default seed**

Run: `cargo test -p uc_node --test lin_register -- --test-threads=1 2>&1 | tail -20`
Expected: all tests PASS, including `fault_roundtrip_keeps_serving` and the capstone, with `Verdict::Linearizable`. This is the headline proof: a non-persisting in-memory SM survives BOTH node-kill and service-crash failover because the node reconstructs it.

- [ ] **Step 2: Run the capstone across multiple seeds (stability)**

Run:
```bash
for s in 4359 1 88888 7 42; do
  echo "=== seed $s ==="
  LIN_SEED=$s cargo test -p uc_node --test lin_register the_capstone_test_name -- --exact --nocapture 2>&1 \
    | grep -E "faults=|Linearizable|VIOLATION|test result"
done
```
(Replace `the_capstone_test_name` with the actual `#[tokio::test]` capstone fn name in `lin_register.rs`.)
Expected: every seed reports `Linearizable` and `test result: ok`. If any seed shows a VIOLATION, STOP — that is a real reconstruction bug exposed by the now-honest in-memory SM; debug with `superpowers:systematic-debugging` (the `/tmp/lincheck_history_<seed>.txt` dump is written on violation).

- [ ] **Step 3: If a flake appears (Inconclusive / timing), adjust only knobs, not scope**

If the checker returns `Inconclusive`, lower `target_ops`/`n_workers` per the in-file guidance; do not weaken faults. Re-run Step 2. Record the seeds that passed in the commit message.

- [ ] **Step 4: Commit (only if `lin_register.rs` changed; otherwise note proof in Task 5 doc)**

```bash
git add uc_node/tests/lin_register.rs
git commit -m "test(lincheck): capstone proves in-memory SM survives both faults

Phase 3. With RegisterSm plain in-memory, the capstone (node-kill + service-crash,
seeded 50/50) stays Linearizable across seeds <list> — reconstruction, not
self-persistence, is what makes service-crash failover correct."
```

---

## Task 4: Documentation — trait + CLAUDE.md

**Files:**
- Modify: `uc_service/src/state_machine.rs` (trait doc)
- Modify: `CLAUDE.md`

- [ ] **Step 1: Strengthen the `last_applied()` trait doc**

In `uc_service/src/state_machine.rs`, the `last_applied()` doc (~lines 37-45) already says it must agree with the framework at startup. Add that it is **load-bearing for reconstruction**: the node reads the service's reported `last_applied` (via the cnc `ServiceStatus.last_applied` atomic) to decide the replay range on reattach and to run the startup cross-check; an SM that under-reports it will be over-replayed (relying on apply idempotency by `log_index`), and one that OVER-reports it (above the node's log frontier) is refused as drift.

- [ ] **Step 2: Update CLAUDE.md reconstruction statements**

In `CLAUDE.md`:
- The line "Service crash → node keeps replicating, voluntarily transfers leadership if leader, resumes apply when service reconnects." → extend: on reconnect the node **reconstructs** the (possibly fresh in-memory) service to the node's apply frontier — replaying `(service_last_applied, frontier]` from the journal, or installing a snapshot + tail-replaying when the gap is below the purge boundary.
- Any statement that "in-memory SMs lose state on service restart" (e.g. in the architecture/storage notes) → correct to: in-memory SMs are reconstructed on reattach/cold-start; they no longer lose state across a service-only restart.
- Add to the storage/snapshot notes that **log purge is now backed by real service snapshots** (Phase 2a/2b) — the node drives the service to BUILD a real snapshot before openraft purges, so a below-purge reattach is served by snapshot-install.

- [ ] **Step 3: Build (doctest-safe) + clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings 2>&1 | tail -2`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add uc_service/src/state_machine.rs CLAUDE.md
git commit -m "docs: last_applied() is load-bearing; CLAUDE.md reconstruction + safe-purge"
```

---

## Task 5: Consolidate the feature into a canonical task doc

**Files:**
- Create: `docs/tasks/task14_service_state_reconstruction.md`
- Modify: `docs/tasks/task12_linearizability_harness.md` (fix Finding #2)

- [ ] **Step 1: Write `docs/tasks/task14_service_state_reconstruction.md`**

A self-contained record (per CLAUDE.md "canonical permanent record" rule) folding in the essential rationale from the spec + 4 phase plans so it does not depend on the superpowers artifacts. Cover, with the real commit SHAs:
- **Problem:** in-memory SMs (and any non-self-persisting SM) lost state on a service-only restart; the node parked apply mid-stream on a persistent SPSC cursor, so a fresh service missed `(0, cursor]`.
- **Model:** channel A = service publishes `last_applied` into cnc `ServiceStatus.last_applied` before READY; `service_epoch` bump = reattach signal; node self-drives `drive_catchup` over `(service_last, frontier]`; below-purge ⇒ snapshot-install + tail.
- **Phase 1 (`9ba1c5e`):** mid-life reattach replay (epoch + cursor reset + gate).
- **Phase 2a (`3b7179e`):** functional bidirectional snapshot path (`snapshot.ring`/`snapshot_resp.ring` + `snapshot.region`; BUILD/BUILT/INSTALL/INSTALLED frames); real snapshot persisted ⇒ safe purge; the degenerate-snapshot/reattach race fix.
- **Phase 2b (`f9294eb` + fix `7ae478f`):** async build (`freeze`/`stream_snapshot` trait; off-`inner` round-trip; `service_epoch` race guard); the freeze-index-vs-frontier double-apply regression and its journal-`log_id_at` fix.
- **Phase 3 (this branch):** upper-bound cross-check; RegisterSm reverted to in-memory; capstone proof (both faults, linearizable across seeds).
- **Trait contract:** `freeze`/`stream_snapshot`/`install_snapshot`/`last_applied`; `last_applied()` load-bearing.
- **Known limits (carried forward):** snapshot_loop no-nack liveness gap; in-`apply()` errors are node-fatal not service-scoped; hard mid-apply `kill -9` of the service not yet exercised (faults are graceful shutdowns).

Per CLAUDE.md, LEAVE the `docs/superpowers/specs/*` and `docs/superpowers/plans/*` artifacts in place (do not delete).

- [ ] **Step 2: Fix task12 Finding #2**

In `docs/tasks/task12_linearizability_harness.md`, update Finding #2 (the self-persisting-RegisterSm note from `162b7ad`): the workaround is removed in Phase 3; the in-memory `RegisterSm` now survives service-crash via reconstruction (link to task14). Keep the historical finding but mark it resolved.

- [ ] **Step 3: Commit**

```bash
git add docs/tasks/task14_service_state_reconstruction.md docs/tasks/task12_linearizability_harness.md
git commit -m "docs(task14): consolidate service-state reconstruction (phases 1-3); task12 Finding #2 resolved"
```

---

## Final verification (before finishing the branch)

- [ ] `cargo clippy --workspace --all-targets -- -D warnings` — clean.
- [ ] `cargo test -p uc_node --test reconstruct_reattach --test reconstruct_snapshot -- --test-threads=1` — green.
- [ ] `cargo test -p uc_node --test drift_detection -- --test-threads=1` — green (non-shmem cross-check unaffected).
- [ ] `cargo test -p uc_node --test lin_register -- --test-threads=1` — green; capstone Linearizable.
- [ ] `cargo test -p uc_node --test m3_service_crash -- --test-threads=1` — green (known to be occasionally timing-flaky; retry in isolation).
- [ ] Then invoke `superpowers:finishing-a-development-branch` (the user merges each phase to main locally).

## Self-Review notes (spec coverage)

- spec §8 "re-enable cross-check" → Task 1 (reconstruction-aware upper-bound, per user Option A).
- spec §8 "revert RegisterSm" → Task 2.
- spec §8 "`last_applied()` load-bearing" + CLAUDE.md → Task 4.
- spec §8 "CLAUDE.md service-crash / in-memory / safe-purge" → Task 4 Step 2.
- spec §10 "lincheck capstone, both faults, linearizable across seeds" → Task 3.
- spec §11 success criteria (in-memory survives restart+cold-start; safe purge; cross-check re-enabled; workaround removed) → Tasks 1-3 + capstone.
- CLAUDE.md "consolidate into docs/tasks/taskXX" → Task 5.
