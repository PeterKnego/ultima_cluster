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

---

## Handoff: filing issue #1 upstream (next session)

To be picked up in a separate session. Goal: produce a minimal reproducer for the
`sm/worker.rs:214` apply-drain debug-assert, then file it on the openraft repo
(https://github.com/databendlabs/openraft).

**Get a repro (it's intermittent — instrument, then loop):**
1. Add diagnostics at the assert site. Vendor/patch openraft locally (e.g.
   `[patch.crates-io]` to a local checkout of `0.10.0-alpha.21`) and change
   `core/sm/worker.rs:213-215` from the bare `assert_eq!(end - 1, got_last_index…)`
   to log first: `eprintln!("DRAIN-MISMATCH end={end} got_last={} ", got_last_index.load(...))`
   then keep the assert. Also log the batch: in the stream `.map()` just above
   (~line 198), print each `entry.log_id.index` and payload variant
   (Blank/Normal/Membership) as it's pulled.
2. Reproduce most reliably on **alpha.20** (the flake was ~40% on the
   leader-isolation scenario there; alpha.21 is much rarer). Either temporarily pin
   back to alpha.20 for the hunt, or loop alpha.21 many times:
   `for i in $(seq 1 50); do cargo test -p uc_node --features fault-injection --test lin_partition leader_isolation_elects_new_leader -- --test-threads=1 --nocapture 2>&1 | grep -E "DRAIN-MISMATCH|sm/worker.rs:214"; done`
   (the `start_3` boot-retry + scenario-retry will mask the *test outcome*, but the
   `eprintln!` fires whenever the underlying assert condition occurs).
3. Capture: openraft version, the `end` vs `got_last_index` values, and the batch's
   `(index, payload-variant)` list for the offending apply call. Note the scenario
   (post-partition leader election / bootstrap).

**What to assert in the report:** our `RaftStateMachine::apply`
(`uc_node/src/raft/state_machine_shmem.rs:501` and `state_machine.rs:201`) returns
`Ok(())` ONLY after the `while let Some(item) = entries.next().await` loop exhausts
the stream — every early exit is an `Err` via `?`, on which openraft never reaches
the assert. So when the assert fires, apply returned `Ok` having drained, which
means `got_last_index` should equal `end - 1`. The mismatch therefore looks like an
openraft-side `end`/`got_last_index` accounting discrepancy in certain
election/bootstrap interleavings — ask the maintainers to confirm whether it's an
openraft invariant bug or a `apply`-contract nuance we're missing.

**Also worth filing (cheaper, deterministic):** issue #3 above — `initialize()`
returning after FLUSH not COMMIT, causing `add_learner` to race
`ChangeMembershipError::InProgress`. A one-paragraph doc clarification request; no
repro hunt needed (see `runtime/builder.rs` retry-backoff workaround).
