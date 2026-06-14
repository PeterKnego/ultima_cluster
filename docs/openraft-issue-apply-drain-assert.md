GitHub issue for databendlabs/openraft. FILED 2026-06-10: https://github.com/databendlabs/openraft/issues/1780
Reproducer branch (pushed to fork): https://github.com/PeterKnego/openraft/tree/fix/apply-drain-assert-short-read

---

**Title:** Apply-worker debug-assert panics on a contract-compliant short read from `entries_stream`

---

## Summary

The state-machine worker's apply path contains a `debug_assertions`-only invariant:

```rust
// openraft/src/core/sm/worker.rs — Worker::apply()
self.state_machine.apply(Box::pin(strm)).await.sto_apply(last.clone())?;

#[cfg(debug_assertions)]
{
    assert_eq!(end - 1, got_last_index.load(std::sync::atomic::Ordering::Relaxed));
}
```

`got_last_index` is the index of the last entry pulled from the stream returned by
`RaftLogReader::entries_stream(since..end)`. The assert requires the stream to yield an
entry at `end - 1` (`= last.index()`).

This is **stricter than the documented contract of `RaftLogReader::try_get_log_entries`**,
which the default `entries_stream` is built on:

> If the log doesn't contain all the requested entries, return the existing entries.
> **The absence of an entry is tolerated only at the beginning or end of the range.**
> Missing entries within the range (i.e., holes) are not permitted and should result in an error.

So a **contract-compliant** log reader may legally return a short read that omits the entry
at the end of the range. When it does, a fully-draining `RaftStateMachine::apply` returns
`Ok`, but `got_last_index < end - 1`, and the apply worker **panics** — crashing the
state-machine worker task and wedging the node — even though neither the state machine nor
the log reader violated its documented contract.

## Observed impact

Intermittent apply-worker panics during bootstrap and post-partition leader election, when a
freshly-flushed final committed entry is observed late by the reader handle (a read-after-flush
visibility window in the storage implementation). Not a linearizability violation; the node
simply dies on the debug assert. Storage impls with eventually-consistent read/write paths hit
this; a strictly-consistent in-memory store does not.

## Reproduction

Two deterministic reproducers (gated on `debug_assertions`, since the assert is debug-only):

- **Unit** — `openraft/src/core/sm/worker.rs`, `mod apply_drain_assert_tests`: a log reader
  that omits the last entry of the requested range + a fully-draining state machine →
  `apply` panics at the assert (`left: 2, right: 1`). A control test with a full read applies
  cleanly.
- **End-to-end** — `tests/tests/apply_drain_assert_e2e_test.rs`: a real single-node `Raft`
  whose store performs one legal short read on the apply path; a normal `client_write` then
  trips the assert in the running apply worker (`left: 2, right: 0`).

Branch with both tests: <https://github.com/PeterKnego/openraft/tree/fix/apply-drain-assert-short-read>

```
cargo test -p openraft --lib apply_drain_assert_tests
cargo test -p tests   --test apply_drain_assert_e2e_test
```

Both are mutation-checked: disabling the assert makes both fail, confirming they bind to this
exact invariant.

## The real question (why this isn't just "remove the assert")

The assert is over-strict *relative to the `entries_stream` API contract*, but it is also
guarding a genuine correctness requirement: the entries in `[since, end-1]` are **committed**,
so they must exist and be readable. On a short read, the SM never applied the committed entry
at `end-1`, yet the worker reports `ApplyResult.last_applied = last` to `RaftCore`. **Simply
relaxing/removing the assert would make openraft believe an entry is applied when it isn't —
silent under-apply / state divergence**, which is worse than the panic.

So the apply path is using a short-read-*tolerant* read API for data it requires to be
*complete*, and handles the resulting mismatch with a debug panic.

## Possible directions (seeking maintainer guidance)

1. **Fail cleanly instead of panicking:** treat a short read on the committed apply range as a
   `StorageError` (loud, no data loss; `RaftCore` can retry/go fatal) rather than a debug
   `assert_eq!`.
2. **Retry the apply read:** if a short read on the apply range is considered a transient
   visibility condition, re-fetch `entries_stream(since..end)` until complete.
3. **Clarify the contract:** document that, unlike general `try_get_log_entries` reads, the
   apply read requires the full `[since, end)` range (committed entries) and that a short read
   there is a storage violation.

Happy to turn the reproducer branch into a PR for whichever direction you prefer.

## Version

Present on current `main` and `0.10.0-alpha.21`. (`#[cfg(debug_assertions)]` only.)
