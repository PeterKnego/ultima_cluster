# Task 10: Eventual Log Durability (Aeron-style)

## Summary

`uc_node` now stores the Raft log in an `ultima_journal::Journal` opened (by default)
with `Durability::Eventual`: an append is acknowledged to openraft at the **page-cache
write**, with fsync running asynchronously in the journal's background writer — off the
commit critical path. Cluster durability is provided by **quorum replication** (an entry
commits only when a quorum holds it). This mirrors Aeron Archive's default
`fileSyncLevel=0` (verified: Aeron's `RecordingWriter`/`Catalog` call `force()` only at
sync level >= 1; the consensus log commits on quorum append-position). Configurable via
`NodeConfig.log_durability`; `Consistent` (fsync-before-ack, power-loss safe) is opt-in.

## Durability model

| Mode | Ack when | On-disk fsync | Survives process crash | Survives power loss | Aeron analog |
|------|----------|---------------|------------------------|---------------------|--------------|
| Eventual (default) | page-cache write | background (journal idle-fsync) | yes | only if not a quorum simultaneously | fileSyncLevel=0 |
| Consistent | after fsync | inline, before ack | yes | yes | fileSyncLevel>=1 |

A client-acknowledged write is lost only if a quorum of nodes suffer power loss / kernel
panic within the un-fsynced window. The metadata `StableValue`s (vote, committed, ...) stay
`Consistent` — they are cold-path and safety-critical (a forgotten vote risks split-brain).

## Implementation

- `JournalLogStorage::open_with_durability(dir, durability)`; `open(dir)` defaults Eventual.
- `NodeConfig.log_durability` (default Eventual) threaded via `NodeBuilder`.
- Append path unchanged: the existing `on_complete` ack fires at the page-cache write in
  Eventual, at fsync in Consistent.
- Recovery clamp (`recovery::reconcile`): a power loss can leave a fsynced `committed`
  ahead of the eventual log's recovered tail; reconcile clamps `committed` down to the
  durable floor `max(last_seq, last_purged)` (preserving snapshot-durable commits).
  Lowering committed is safe — the node re-learns commit from the leader.
- Observability: `JournalLogStorage::durability_lag()` = `last_seq - durable_seq` (the
  un-fsynced window).
- Probe `JournalFsynced` renamed `JournalDurable` (Eventual acks pre-fsync).

## Measurement

Storage: `ext4` at `/`; `Linux 7.0.0-15-generic x86_64`.
Micro-measurement (`measure_append_ack_latency_by_mode`), single-entry `blocking_append`,
median latency:

| mode | append-ack median |
|------|-------------------|
| Consistent | 34 µs |
| Eventual | 31 µs |

This isolates the **log-append** durability cost on the changed path. The gap here is
storage-dependent: on tmpfs / fast SSD the fsync cost is low so the difference is modest;
on real disk it is large — the prior `aeron-vs-uc-commit-path-benchmark` design attributed
the full single-node commit floor (p50 ~= 36 ms) to the "journal group-commit window
(~38 ms/committed entry)".

## Scope & next levers (honest)

This removes the **log-append** fsync from the commit critical path. A commit round-trip
still incurs a `save_committed` fsync, and the output path a per-record `output_progress`
fsync — both kept `Consistent` here (out of scope). Validating the full commit-path effect
needs the `aeron-vs-uc` harness (single-node tmpfs vs real-disk, in-flight concurrency
sweep); if the ~38 ms window only partly collapses, `committed` / `output_progress` are the
next candidates.

Note: `ultima_journal::JournalConfig::new` still defaults to `Consistent`; the Eventual
default is applied at the `JournalLogStorage::open` layer (uc_node), so the two layers have
intentionally different defaults.
