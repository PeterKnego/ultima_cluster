# Task 14 — Service-State Reconstruction

Canonical record for the service-state reconstruction feature (Phases 1–3). It
makes the service's user state machine — including a **non-persisting in-memory
SM** — recoverable after a service crash or a node restart, driven by the node
from the replicated log, and makes reads linearizable via a ReadIndex barrier.

Design history (retained, not required reading): `docs/superpowers/specs/2026-06-06-uc-service-state-reconstruction-design.md`
and `docs/superpowers/plans/2026-06-0*-*.md`.

## Problem

`uc_service` holds the user's `StateMachine`; `uc_node` holds Raft + the log. The
two communicate over shmem SPSC rings. The node's `apply()` parks on the apply
ring waiting for the service. Originally:

- A service-only restart brought back a **fresh in-memory SM** (empty), but the
  node's apply cursor had advanced — the service silently missed `(0, cursor]`.
- A **node restart** recovered the node from its journal/snapshot, but the fresh
  service only received the entries openraft replayed; if the log had been purged
  below committed, the snapshot'd prefix never reached the service.
- Reads were served straight from the service with only a bare leader check, so a
  freshly-(re)attached/empty service could answer **stale** (a linearizability
  violation), and there was no read-index barrier (the `client_dispatcher.rs` M5
  TODO).

Cold-start (no snapshot) was already handled by openraft re-applying
`(durable_applied, committed]` on boot; the durable `last_applied` only advances
at snapshot cadence. The real gaps were mid-life reattach, below-purge recovery,
and linearizable reads.

## Model

- **Channel A:** the service publishes its `StateMachine::last_applied()` into the
  cnc `ServiceStatus.last_applied` atomic, and bumps `ServiceStatus.service_epoch`,
  before flipping `Ready`. An epoch change = a new service incarnation (reattach).
- **Reconstruction decision** (`runtime/reconstruct.rs::plan_replay`): given
  `(service_last, node_frontier, last_purged)` →
  `Replay{from,to}` (journal replay) or `NeedsSnapshot` (below purge → install a
  snapshot, then tail-replay).
- **drive_catchup** (`raft/state_machine_shmem.rs`): runs the plan under the
  apply lock, reusing the apply rings; on `NeedsSnapshot` it installs the node's
  on-disk/in-memory snapshot into the service then tail-replays.

## Phases

- **Phase 1 — mid-life reattach** (`9ba1c5e`). Epoch-change detection +
  `SpscConsumer::discard_backlog()` (cursor reset) + node-driven `drive_catchup`
  replaying `(service_last, up_to]`. Proven by `tests/reconstruct_reattach.rs`
  (in-memory `CounterSm` → survives a service-only restart).
- **Phase 2a — functional snapshot path** (`3b7179e`). Dedicated
  `service/snapshot.ring` + `snapshot_resp.ring` SPSC pair and a
  `service/snapshot.region` file carry BUILD (service→node) and INSTALL
  (node→service); frames `BUILD_SNAPSHOT`/`SNAPSHOT_BUILT`/`INSTALL_SNAPSHOT`/
  `SNAPSHOT_INSTALLED`. Node `build_snapshot` drives the live service → a real
  snapshot persisted to disk, so **log purge is safe**. Fixed a degenerate-empty
  snapshot/reattach data-loss race (return a non-advancing snapshot; returning
  `Err` from `build_snapshot` is FATAL in openraft 0.10).
- **Phase 2b — async build** (`f9294eb`, regression fix `7ae478f`). Trait gains
  `type SnapshotHandle: Send`, `freeze() -> (handle, index)` (O(1) MVCC pin),
  `stream_snapshot(handle, dst)`; the service freezes under a brief read lock then
  streams off-thread. Node `build_snapshot` keeps `inner` FREE during the BUILD
  round-trip (3 scopes; reattach guarded by `service_epoch`). Regression fixed:
  the snapshot must be labeled with the freeze-time `built_index` (resolved from
  the journal via `log_id_at`), not the scope-1 frontier, or tail-replay
  double-applies.
- **Phase 3 — contract flip, parity, read barrier, gap fix** (this task; commits
  below). Detailed next.

## Phase 3 details

- **Upper-bound `last_applied` cross-check** (`1bd9fd5`, `2b6eb20`). At node
  startup the service is already `Ready`, so the node reads `service_last_applied`
  and refuses (`DriftDetected`) only if it exceeds the journal tail
  (`journal.last_seq()`); a service at-or-below is the normal reconstruction case.
  `reconstruct::service_not_ahead` (pure, unit-tested) is also enforced at
  reattach in `drive_catchup`.
- **`StateMachine::last_applied()` is load-bearing** — documented in the trait and
  CLAUDE.md.
- **Read-gate driver redesign** (`ab47c8e`). A first read-gate that held an extra
  adapter clone in the query path reproducibly **froze** the 3-node cluster
  (isolated to a *third* live adapter clone via `mem::forget`; 2 = fine, 3 =
  freeze; root mechanism not fully pinned). Redesigned to avoid it: the
  (previously dead) `NodeHandle` shmem adapter slot now holds only the shutdown
  flag, and the repurposed 2nd clone is owned by a **proactive reconcile-driver
  task** — net live-clone count stays 2. The read-gate (`ReconcileGate`) holds
  only lightweight signals (no adapter clone).
- **ReadIndex linearizable read barrier** (`5d41fbe`). Linearizable reads call
  `Raft::ensure_linearizable(ReadPolicy::ReadIndex)` (confirm leadership via a
  quorum, wait until openraft applied to the read index, return `read_log_id`),
  then `ShmemQueryLink::submit` waits until the **service** has caught up to that
  index before serving. Finishes the M5/M4 read TODO. No adapter clone / no
  `inner` lock in the read path (freeze-safe).
- **Reconstruct on a prefix GAP, not just an epoch change** (`aa010e3`). Root
  cause of node-restart stale reads (confirmed by tracing): a node restart whose
  log was purged below committed replays only the post-snapshot *tail* to the
  fresh service via the normal apply path — the snapshot prefix was never
  installed. Now `apply()` triggers `drive_catchup` when
  `service_caught_up_to + 1 < log_index` (a gap), and the driver's
  `ensure_reconciled` triggers when `service_caught_up_to < frontier`; both route
  to `NeedsSnapshot` which installs the prefix. `service_caught_up_to` advances
  only when the service is genuinely at `log_index` (the Normal arm fills it;
  Blank/Membership advance only when already contiguous) so a term-blank can't
  mask an unfilled gap.

## Trait contract (current)

`apply` (sync, deterministic) · `query` · `last_applied` (load-bearing) ·
`type SnapshotHandle: Send` + `freeze`/`stream_snapshot` · `install_snapshot`.
`StoreStateMachine` (ultima_db) implements freeze/stream via `snapshot_stream`.

## Tests

- `tests/reconstruct_reattach.rs` — in-memory SM survives a service-only restart;
  and a READ immediately after restart (no intervening write) observes the
  reconstructed value (read-gate + ReadIndex barrier).
- `tests/reconstruct_snapshot.rs` — below-purge reattach reconstructed via
  snapshot-install + tail replay; concurrent-build epoch-stability.
- `tests/lin_register.rs` — the WGL lincheck capstone (node-kill + service-crash,
  heavy concurrent churn). Runs a **self-persisting** `RegisterSm` (see Known
  limitations); Linearizable across seeds 4359/1/88888/7/42/13/999.

## Known limitations

- **Residual in-memory reconstruction race under heavy concurrent fault churn.**
  With a *non-persisting* SM, the lincheck capstone still occasionally produces a
  stale read under sustained concurrent node-kill + service-crash + leadership
  churn: tracing shows the read-gate passing (`service_caught_up_to >=
  read_index`, `epoch == reconciled`) while the service answers empty — i.e.
  either `service_caught_up_to` is briefly wrong on a node, or a stale leader
  serves across a transition. The deterministic cold-start and single-fault cases
  are fixed; this rare multi-fault interleaving is **deferred**. The capstone
  therefore runs a self-persisting `RegisterSm` (durable state across a node
  restart sidesteps it); in-memory reconstruction is proven by the focused
  `reconstruct_*` tests. Resolving it likely needs node-disambiguated tracing and
  tightening the cross-node read-readiness signal.
- `snapshot_loop` has no nack-on-error frame (a failed build/install logs + skips;
  the node's await only bails on shutdown). Rare; deferred.
- In-`apply()` reconstruction errors are node-fatal (openraft contract), not
  service-scoped.
- A hard `kill -9` of the service mid-apply is not exercised (faults are graceful
  shutdowns).
