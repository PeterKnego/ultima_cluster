# Known openraft-alpha issues observed

Tracking notes on openraft `0.10.0-alpha.*` rough edges we've hit, with an
assessment of whether each is an openraft bug (reportable upstream) or our
usage/contract. Not yet filed upstream — see status per item. Current pin:
`0.10.0-alpha.21` (see `Cargo.toml`).

## 1. Apply-worker drain debug-assert (`sm/worker.rs:214`) — intermittent

`#[cfg(debug_assertions)]` invariant `assert_eq!(end - 1, got_last_index)` in
openraft's state-machine worker, asserting that our `RaftStateMachine::apply`
fully drained the entry stream it was handed. It intermittently panics a node's
apply-worker task during apply/convergence — at bootstrap and (more often) during
post-partition leader election. **Not a linearizability violation** (our WGL
checker never reports `Violation`); a debug-only sanity panic.

- **Our side looks correct.** `state_machine_shmem.rs::apply` returns `Ok(())`
  only after the `while let Some(item) = entries.next().await` loop exhausts the
  stream (`state_machine.rs` embedded apply likewise); every early exit is an
  `Err` via `?`, on which openraft never reaches the assert. So on the path where
  the assert fires (apply returned `Ok`), our impl provably drained — pointing at
  openraft's own `end`/`got_last_index` counting in certain bootstrap/convergence
  interleavings.
- **Status: reportable, but needs a clean repro.** Intermittent, and markedly
  rarer on alpha.21 (0 occurrences across 9 `lin_partition` runs + 4 feature-on
  capstone runs, vs ~40% of leader-isolation runs on alpha.20 — alpha.21's
  responder-drain fix may have reduced it). Filing would need instrumentation
  capturing `end`, `got_last_index`, and the batch's entry types when it trips.
- **Workarounds in tree:** `start_3()` bounded boot-retry and the `lin_partition.rs`
  scenario-level retry (task15); the m3 convergence flake (task04-deferred) is
  likely the same assert.

## 2. `build_snapshot` returning `Err` is node-FATAL

In openraft 0.10 a `build_snapshot` error shuts the node down. For the
degenerate-empty-snapshot/reattach case we had to return a *non-advancing*
snapshot instead of `Err` (task14, Phase 2a). More a sharp edge / design choice
than a bug. **Status: minor; low-priority ergonomics feedback at most.**

## 3. `initialize()` returns after FLUSH, not COMMIT

`Raft::initialize()` returns once the init membership log is flushed, not
committed, so an immediately following `add_learner` races the in-progress
membership change and fails with `ChangeMembershipError::InProgress`. We work
around it with retry+backoff in `runtime/builder.rs`. Deterministic and easy to
describe. **Status: cleanest thing to report upstream — a doc clarification or
small API note would help others.**

## 4. m3 convergence / fallback flake (task04-deferred)

Intermittent election-convergence failure in `m3_service_crash` (and kin). Likely
the same as #1 (apply-worker assert during convergence) or a sibling election
timing issue; not independently characterized. **Status: deferred; revisit if #1
is filed.**

---

**Not openraft:** a teardown-time `SIGSEGV` occasionally seen after an in-process
cluster test's logic completes is almost certainly our shmem/quinn shutdown path,
not openraft — tracked on our side, not here.

**Decision (2026-06-09):** note these and keep moving; do not file upstream yet.
#1 is the only one with real upstream value and it needs a reproducer first.
