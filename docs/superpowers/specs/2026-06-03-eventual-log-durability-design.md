# Design: Eventual Log Durability for `uc_node` (Aeron-style)

**Date:** 2026-06-03
**Status:** Draft for review
**Crate:** `ultima_cluster` / `uc_node` (Raft log over `ultima_journal`)

## Problem

`uc_node` stores the Raft log in an `ultima_journal::Journal` opened with
`Durability::Consistent` (`uc_node/src/raft/log_storage.rs::open`). openraft's `IOFlushed`
ack is chained on that journal's **post-fsync** notifier, so **an `fsync` sits on the commit
critical path** of every append batch.

The prior `aeron-vs-uc-commit-path-benchmark` design measured the full single-node commit
path at **p50 ≈ 36 ms, ~100 rt/s**, **Raft-commit-dominated by the "journal group-commit
window (~38 ms/committed entry)"** — a floor it names the **#1 optimization target** that
has "never been optimized, only flagged."

Aeron Archive — the reference SMR log store — defaults to **`fileSyncLevel=0`: no fsync at
all** on log data or catalog. It writes via `FileChannel.write()` into the OS page cache, and
Aeron Cluster commits an entry when a **quorum has it in page cache** (the append-position
protocol). Durability = replication quorum; this survives process crash but not simultaneous
power loss. Power-loss safety is an opt-in `fileSyncLevel=2`.

This design moves UC's Raft log to the same model: acknowledge an append at the **page-cache
write** (Eventual), with fsync running asynchronously in the journal's background writer,
off the commit critical path. Durability is provided by quorum replication.

## Goals

- Make Raft-log durability **configurable**, defaulting to **Eventual** (Aeron
  `fileSyncLevel=0` model): ack on page-cache write, background fsync, quorum durability.
- Keep `Consistent` (fsync-before-ack, power-loss safe) available as opt-in.
- Preserve correctness across **process crashes** (the page cache survives) and handle the
  one new **power-loss** edge the log-only scope introduces (the `committed` inversion).
- Quantify the commit-path effect with a baseline-first measurement.

## Non-Goals

- Relaxing durability of the Raft **metadata** `StableValue`s (`vote`, `committed`,
  `last_purged`, `last_applied`, `snapshot_meta`). They stay `Consistent`: cold-path (election
  / snapshot frequency, ~free to fsync) and safety-critical — a forgotten `vote` on power loss
  can cause double-voting / split-brain, a worse failure than losing recent log entries.
- Relaxing the per-record `output_progress` fsync (a separate hot path; already at-least-once
  + idempotent-by-`log_index`, so a good future candidate, but out of scope here).
- A 3-level (0/1/2) sync enum. The journal only distinguishes "fsync vs not," so Aeron levels
  1 and 2 both map to `Consistent`; the doc notes the mapping. We reuse
  `ultima_journal::Durability` directly.
- Forcing a tighter fsync cadence than the journal's existing background idle-fsync
  (approach B) — deferred (YAGNI).

## Architecture

The change is concentrated in three places — `NodeConfig` (the knob), `log_storage.rs::open`
(apply it to the journal only), and `recovery.rs` (repair the inversion). The append path is
unchanged: its existing `on_complete` chaining already fires the `IOFlushed` ack at the
page-cache-write boundary in Eventual mode and at fsync in Consistent mode.

### Component 1: config knob (`NodeConfig`)

Add a field to `NodeConfig` (`uc_node/src/config.rs:50`, the struct `NodeBuilder::new(config, sm)` already holds):

```rust
/// Durability for the Raft log journal.
///
/// `Eventual` (default) acks an append at the page-cache write, with fsync off the
/// commit critical path — durability via quorum replication; survives process
/// crash, **not** simultaneous quorum power loss. This is Aeron's
/// `fileSyncLevel=0` model. `Consistent` fsyncs before ack (power-loss safe; Aeron
/// `fileSyncLevel>=1`), at the cost of per-commit fsync latency.
pub log_durability: ultima_journal::Durability,
```

Default is `Durability::Eventual` (set in `NodeConfig`'s `Default`/constructor).
`NodeBuilder::build()` passes it through:

```rust
let log_storage = JournalLogStorage::open(&self.config.data_dir, self.config.log_durability)?;
```

### Component 2: `JournalLogStorage::open`

Signature gains the parameter; only the **journal's** durability changes. All `StableValue`s
stay `Durability::Consistent`.

```rust
pub fn open(data_dir: &Path, log_durability: Durability) -> Result<Self, ClusterError> {
    std::fs::create_dir_all(data_dir.join("journal"))?;
    let journal = Arc::new(Journal::open(JournalConfig {
        dir: data_dir.join("journal"),
        segment_size_bytes: SEGMENT_SIZE_BYTES,
        durability: log_durability,          // was hard-coded Durability::Consistent
    })?);
    // vote / committed / last_purged / last_applied / snapshot_meta / output_progress:
    // unchanged — all Durability::Consistent.
    ...
}
```

All other callers of `open` (tests under `uc_node/tests/`, e.g. `log_storage_open.rs`,
`drift_detection.rs`) pass an explicit durability (use `Durability::Consistent` where they
assert immediate on-disk state, or `Eventual` where they exercise the new default).

### Component 3: append path (unchanged logic + probe rename)

`append` keeps `notifier.on_complete(callback)` on the final entry. In Eventual the notifier
resolves after the buffered page-cache write (Aeron semantics); in Consistent, after fsync.
No logic change.

**Cosmetic fix:** the probe stamped inside that callback is
`uc_protocol::probes::Checkpoint::JournalFsynced`. In Eventual it fires at the page-cache
write, not fsync, so rename it to `JournalDurable` (or document the dual meaning) so latency
telemetry is not misleading. Update the enum + the one stamp site.

### Component 4: recovery clamp (the inversion repair)

`committed` is a `StableValue` (still fsynced) and can be persisted **ahead** of the Eventual
log's recovered tail after a power loss: the leader acks an append at page-cache write,
reaches quorum, advances + fsyncs `committed`, then power-loses before the journal's
background fsync flushes that entry. On reboot `committed.index` can exceed
`journal.last_seq()`, violating openraft's `committed <= last_log` invariant. (`last_applied`
is only persisted at snapshot install, so it is not exposed this way; `committed` is the one
at risk.)

Rename `recovery::assert_consistent` → `recovery::reconcile` (it now repairs, not only
asserts). Keep the existing checks as asserts (still genuine corruption):
`last_seq >= last_purged.index` and `output_progress <= last_applied`. Add the repair:

```rust
// Eventual-log / Consistent-committed inversion: a power loss can leave a fsynced
// `committed` ahead of the journal's recovered (page-cache-lost) tail. Clamp committed
// down to the durable log tail; the node re-learns the true commit from the leader via
// normal Raft catch-up. Lowering committed is always safe.
let last_seq = storage.journal.last_seq().unwrap_or(0);
if let Some(c) = storage.committed.load().map_err(recovery_err)?
    && c.index > last_seq
{
    match storage.last_log_id_at(last_seq).map_err(recovery_err)? {
        Some(id) => storage.committed.store(&id).map_err(recovery_err)?.wait().map_err(recovery_err)?,
        None => storage.committed.clear().map_err(recovery_err)?.wait().map_err(recovery_err)?,
    }
}
```

`last_log_id_at(seq)` is a small helper extracted from the existing `get_log_state` logic
(read the record at `seq`, bincode-decode, return its `log_id`); when `seq == 0` / empty log
it returns `None`. `reconcile` is called from `NodeBuilder::build()` exactly where
`assert_consistent` is today.

## Durability model (documentation deliverable)

A durability-model section in the task doc + the `NodeConfig::log_durability` doc comment,
mirroring Aeron's README:

| Mode | Ack when | On-disk fsync | Survives process crash | Survives power loss | Aeron analog |
|------|----------|---------------|------------------------|---------------------|--------------|
| **Eventual** (default) | page-cache write | background (journal idle-fsync) | yes | only if not a quorum simultaneously | `fileSyncLevel=0` |
| **Consistent** | after fsync | inline, before ack | yes | yes | `fileSyncLevel>=1` |

Cluster durability under Eventual is provided by quorum replication: an entry is committed
only when a quorum has it (in page cache). A client-acknowledged write is lost only if a
quorum of nodes suffer power loss / kernel panic within the un-fsynced window.

## Observability

Add a gauge for the Eventual window: **durability lag** =
`journal.last_seq() − journal.durable_seq()` ("entries written but not yet fsynced"), using
task28's `Journal::durable_seq()` watermark. This makes the un-fsynced window observable and
is the health signal for the mode. (The existing `uc_journal_fsync_duration_seconds` metric
still applies — fsync now runs on the background writer.)

## Measurement (baseline-first)

Use the existing single-node commit-path fixture (`uc_autobench` / `shmem-e2e`, the source of
the ~36 ms p50). Measure **Consistent (baseline) → Eventual**, reporting p50/p99 commit
latency and achieved throughput (sweeping in-flight concurrency, per the prior benchmark's
note that group-commit batching sets the throughput knee).

**Honest scope note:** a commit round-trip also incurs a `save_committed` fsync (and outputs
incur `output_progress` fsyncs). Log-Eventual removes the **log-append** fsync from the
critical path; the `save_committed` fsync remains (kept Consistent by the log-only decision).
The measured win may therefore be **partial** — the measurement quantifies it and tells us
whether `committed` / `output_progress` (out of scope here) are the next levers. We report the
actual decomposition rather than assume a full collapse of the ~38 ms window.

## Testing

- **Default + flow-through:** `NodeConfig` defaults `log_durability = Eventual`;
  `JournalLogStorage::open` opens the journal Eventual; a test asserts the journal's mode via
  behavior (see below). All existing cluster tests stay green under the new default
  (`three_node_cluster`, replication, divergence, `client_retry`) — quorum still provides
  cross-restart consistency.
- **Recovery clamp:** construct a data dir where `committed.index = N` (write via the
  StableValue) but the journal is truncated to `last_seq < N`; run `reconcile`; assert
  `committed` is clamped to `last_log_id_at(last_seq)` (or cleared if empty) and the node
  opens and serves. A companion case: `committed.index <= last_seq` is left untouched.
- **Process-crash recovery (existing crash/restart tests):** unchanged — page-cache writes
  survive a process crash, so all acked entries recover.
- **Eventual durability behavior:** append in Eventual mode; assert `durable_seq()` lags
  `last_seq()` immediately after append and catches up after the journal idle-fsync (mirrors
  ultima_journal's task28 tests); the durability-lag gauge reflects this.
- **Consistent opt-in:** a node configured `Consistent` behaves as today (a test that asserts
  `durable_seq() == last_seq()` right after an acked append).

## Deliverables

- `uc_node/src/config.rs`: `log_durability` field on `NodeConfig` + default.
  `uc_node/src/runtime/builder.rs`: pass it through to `JournalLogStorage::open`.
- `uc_node/src/raft/log_storage.rs`: `open` signature; journal durability from config;
  `last_log_id_at` helper (extracted from `get_log_state`).
- `uc_node/src/runtime/recovery.rs`: `assert_consistent` → `reconcile` + the committed clamp.
- `uc_protocol/src/probes.rs`: rename `JournalFsynced` → `JournalDurable` (+ the stamp site).
- Observability: durability-lag gauge.
- `uc_node/tests/`: clamp test, Eventual/Consistent behavior tests; update `open` call sites.
- `docs/tasks/task10_eventual_log_durability.md`: durability-model doc + before/after
  commit-path measurement.
- `cargo clippy -- -D warnings` clean; workspace tests green.

## Open risks / notes

- **Partial win:** as above, `save_committed` remains a per-commit fsync; the measurement may
  show the ~38 ms window only partly collapses. That is itself a useful finding and points at
  the next levers (out of scope here).
- **openraft `committed > last_log` handling:** we clamp proactively at startup before
  openraft reads `committed`, so openraft never observes the inversion. If openraft also
  tolerates it internally, the clamp is belt-and-suspenders; either way it is safe.
- **Default change is a durability-semantics change.** It is configurable and documented;
  operators needing power-loss durability set `log_durability = Consistent`.
