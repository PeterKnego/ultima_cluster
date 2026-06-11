# Known openraft-alpha issues observed

Tracking notes on openraft `0.10.0-alpha.*` rough edges we've hit, with an
assessment of whether each is an openraft bug or our usage/contract — see status
per item. Current pin: `0.10.0-alpha.21` (see `Cargo.toml`). Headline: #1 (the
apply-drain debug-assert) was filed (openraft#1780), ruled a storage bug on our
side, and FIXED in `ultima_journal` (`ultima_db` `1de711a`).

## 1. Apply-worker drain debug-assert (`sm/worker.rs:214`) — intermittent

**FILED 2026-06-10: https://github.com/databendlabs/openraft/issues/1780 — RULED
A STORAGE BUG ON OUR SIDE (correct assert). FIXED 2026-06-11 in `ultima_journal`
(`ultima_db` commit `1de711a`).**

`#[cfg(debug_assertions)]` invariant `assert_eq!(end - 1, got_last_index)` in
openraft's state-machine worker, asserting that our `RaftStateMachine::apply`
fully drained the entry stream it was handed. It intermittently panicked a node's
apply-worker task during apply/convergence — at bootstrap and (more often) during
post-partition leader election. Not a linearizability violation in our debug-mode
test runs (the assert panics and the run is discarded/retried); but in a release
build (assert compiled out) the same short read would silently skip an apply while
openraft advances `last_applied` past it → **state divergence**. So it was a real
latent correctness bug, not a cosmetic one.

- **Maintainer ruling (drmingdrmer, 2026-06-10): the assert is CORRECT; it caught
  a storage-contract violation on our side.** Our earlier analysis (that the assert
  was stricter than the `try_get_log_entries` short-read allowance) was WRONG.
  `RaftLogStorage::append()` requires every appended entry to be **readable the
  instant `append()` returns — before the flush callback, even before durability**
  (openraft may apply committed entries whose local flush is still in flight). So by
  the time apply reads `since..end`, the log *contains* every entry and the reader
  MUST return all of them. The `try_get_log_entries` boundary tolerance covers only
  entries the log does **not** contain (purged at the front / not-yet-appended past
  the tail) — NOT appended-but-not-yet-visible entries. Our "read-after-flush
  visibility window" is exactly what the `append()` contract forbids.
- **Root cause (confirmed in `ultima_journal`):** `Journal::append()` only validated
  monotonicity and **enqueued** the record to the bg writer thread, which updated
  `last_seq`/`segments` later; `read`/`read_range`/`last_seq` saw only that persisted
  state. So a read between `append()` returning and the writer processing got a
  short read missing the tail entry — precisely the contract violation.
- **Fix (`ultima_db` `1de711a`, recipe option A — synchronous in-memory tail):**
  `append()` now publishes the record into a `pending: BTreeMap<seq,(meta,payload)>`
  and advances `last_seq`/`first_seq` under the state lock *before returning*;
  `read`/`read_range` overlay `pending` on the durable segments; the bg writer evicts
  each seq from `pending` the instant it lands in a segment (under the same lock, so
  a seq is in a segment XOR pending). Mutation-checked regression test
  (`append_is_immediately_readable_before_flush`) appends N without awaiting the
  flush and asserts all read paths see them immediately, both durability modes.
- **Cluster validation:** leader-isolation (the ~40%-on-alpha.20 offender) ran 20/20
  clean post-fix with **0** `sm/worker.rs:214` hits and **0** scenario-retries fired.
  (Corroborating; the journal mutation test is the hard proof.)
- **The in-tree retries are now belt-and-suspenders, not load-bearing:** `start_3()`
  boot-retry + `lin_partition.rs` scenario-retry (task15) remain as cheap insurance
  (and still cover any unrelated convergence hiccup); they should no longer fire for
  this cause. The m3 convergence flake (task04-deferred) was likely the same bug and
  should now be gone too — worth re-checking.
- **Remaining upstream (optional):** the maintainer invited a doc-clarification PR
  ("direction 3") tightening the `try_get_log_entries` wording to say the boundary
  tolerance covers only purged/not-yet-appended entries, cross-referencing the
  `append()` readability requirement. Not yet submitted.

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

## 4. m3 convergence / fallback flake (task04-deferred) — RESOLVED 2026-06-11

Intermittent election-convergence failure in `m3_service_crash` (and kin) —
historically "passes ~8/8 in retries". It was the same as #1 (the read-after-append
visibility bug surfacing during convergence). **Re-checked after the `1de711a` fix:
`m3_service_crash` 25/25 clean and `m3_three_node_shmem` 10/10 clean, with 0
`sm/worker.rs:214` apply-assert hits across all 35 runs.** Considered resolved by the
journal fix. (A rare flake can't be proven absent by 35 runs, but the suspected cause
— the apply-assert — did not fire once where it previously did.)

---

**Not openraft:** a teardown-time `SIGSEGV` occasionally seen after an in-process
cluster test's logic completes is almost certainly our shmem/quinn shutdown path,
not openraft — tracked on our side, not here.

**Status (2026-06-11):** #1 filed (openraft#1780), ruled a storage bug on our side,
and FIXED in `ultima_journal` (`1de711a`). #2 unchanged (minor). #3 still open and
is the only remaining upstream item worth a (small, optional) doc-clarification PR.
The earlier "next-session handoff: get a repro + file #1" is complete — superseded
by the fix above.
