# Task 12 — Linearizability Test Harness (failover)

A CI-runnable, seeded linearizability regression test that drives a concurrent
CAS-register workload through repeated leader kill/restart failovers, records a
real-time history, and proves it linearizable with an in-repo Wing-Gong-Lowe
(WGL) checker. Test-only: everything lives under `uc_node/tests/lincheck/` +
`uc_node/tests/lin_register.rs`, with **one** production change that the harness
surfaced (see "Findings").

## Why

The suite had good *scenario* tests (elect, replicate, failover, snapshot
install, service crash, client retry) but no test of the core SMR guarantee:
that every client observes a single, real-time-consistent history **under
failover**. Failover is exactly where naive SMR breaks — stale reads after a
leader change, lost updates during a transfer, a CAS that appears to succeed
twice. This harness is that guard.

## What it is

Module `uc_node/tests/lincheck/`, five focused units; the pure ones carry no
cluster/tokio deps so they unit-test in milliseconds:

- `model.rs` — the sequential spec. `Model` trait (`init`/`step`) +
  `RegisterModel` (`State = Option<u64>`; `Op = Write|Read|Cas`;
  `Resp = Ack|Value|CasOk`). Pure.
- `history.rs` — recording. `Entry{client,op,invoke,ret,outcome}`,
  `Outcome = Ok(RegResp) | Indeterminate`, and a thread-safe `History` whose
  real-time order comes from a single global `AtomicU64` stamped at `invoke()`
  and at `record()`: `A` precedes `B` iff `A.ret < B.invoke`. Deterministic;
  avoids clock jitter. Pure types + recorder.
- `checker.rs` — the WGL linearizability search (below). Pure, generic over
  `Model`.
- `register_sm.rs` — `RegisterSm`, the replicated SM the cluster actually runs
  (`uc_service::StateMachine`): `Command = Write|Cas`, `Response = WriteAck|
  CasResult`, `Query = ()`, `QueryResponse = Option<u64>`. **Self-persisting**:
  it writes `(value, last_applied)` durably to its `data_dir` on every apply
  (fsync before returning the response, so the framework only ever acks durable
  state), reloads it on startup, and guards on `log_index` so recovery replays
  are idempotent (CAS is not). This mirrors the production `StoreStateMachine`
  contract (persist to `ultima_db`) and is what lets a *service-only* restart
  recover — uc does not replay history into a reconnecting service (Finding #2).
  The degenerate node-side SM is `RegisterSm::default()` (no `data_dir`, never
  persists; in shmem mode it doesn't apply app data).
- `cluster.rs` — `LinCluster`, a 3-node shmem fault harness (one
  `instance_dir`/`data_dir`/`svc_data_dir` per node). All methods take `&self`
  with an internal `tokio::sync::Mutex<Vec<Node>>` + `Arc<Client>` per node so
  concurrent workers and the fault scheduler share it; leader polling uses the
  **sync** `Client::current_leader()` on cloned `Arc<Client>`s so the nodes lock
  is never held across an await. `start_3`, `submit_cmd`, `read` (linearizable
  via `query_linearizable`), `wait_for_stable_leader`, `kill_and_restart_leader`,
  `crash_and_restart_leader_service`, `shutdown`.

`uc_node/tests/lin_register.rs` wires it: the checker/model/history unit tests,
a `smoke_3node_submit_read`, a `fault_roundtrip_keeps_serving` liveness check,
and the capstone `linearizable_under_failover`.

All cluster tests run on the **default multi-thread** tokio runtime — the
multi-node shmem boot deadlocks under `current_thread` (it blocks internally on
the service-ready handshake during `NodeBuilder::start()`).

## The WGL checker

Standard Wing-Gong-Lowe linearization-point search (the algorithm behind
Knossos/Porcupine), generic over `Model`. Maintain the model `State` + the
not-yet-linearized set; repeatedly pick a **frontier op** (its `invoke` precedes
the minimum `ret` of the remaining ops — real-time-eligible), `step` it, and for
an `Ok(observed)` op require `resp == observed`; backtrack on dead-ends.
**Memoize** `(remaining-bitset, state)` — but only after a *fully-explored*
dead-end, never a budget-truncated one, so a memo hit always means a real dead
end (a subtle soundness point: caching a budget-truncated subtree would let a
later path get a false "unlinearizable").

Indeterminate ops: `ret = u64::MAX` (linearizable any time at/after `invoke`),
**optional** (the search may drop them ≡ never committed), and response
**unconstrained** when placed. Indeterminate *reads* carry no information →
dropped before the search; only indeterminate *mutations* enter as optional
unconstrained effects.

A **visited-state budget** (`DEFAULT_BUDGET = 5_000_000`) returns `Inconclusive`
rather than a false `Linearizable` from a truncated search. This is the
no-false-pass guard, and it fired during bring-up (see Bounds).

Six pure unit tests feed known-good/known-bad histories (sequential, stale read,
double-applied CAS, concurrent overlap, an indeterminate write that may be
present-or-absent, and one that must have happened) and assert `Linearizable` vs
`Violation` — validating the checker independent of the cluster.

## Fault model — leader node-kill+restart AND service-crash+restart

The capstone injects one quorum-preserving fault at a time (seeded 50/50 choice),
waiting for a stable leader between faults:

- `kill_and_restart_leader`: gracefully shut down the leader's node **and**
  service; survivors (2/3) re-elect; then restart the node against its
  **persisted raft `data_dir`** (rejoins under the same `node_id`, no membership
  change) but a **fresh shmem `instance_dir`** (the control/ring files are
  volatile per-process IPC, not cluster state; a stale `cnc.dat` would race the
  service handshake), with a fresh service + reconnected client (a restarted
  node has a new `instance_id`, invalidating the old client). Teardown is
  node-first, then service (Finding #3).
- `crash_and_restart_leader_service`: gracefully shut down only the leader's
  **service**; the node stays up, its service-watcher transfers leadership; a
  fresh service is started on the same `svc_data_dir`.

Both faults are linearizable-safe because `RegisterSm` **persists its own state**
(see "What it is"). A node restart re-applies the committed log into the fresh
service, whose `log_index` guard makes the replay idempotent; a service restart
reloads durable state from `svc_data_dir`. This is the production
`StoreStateMachine` contract — uc does not replay history into a reconnecting
service, so the SM must own its durability (Finding #2).

Note: faults are **graceful** shutdowns (`shutdown().await`), so the in-flight
apply completes before teardown. A true hard crash (e.g. `kill -9`) of a service
mid-apply — where an entry is read off the SPSC ring but not acked — is **not**
exercised yet; see Deferred.

## Findings (surfaced by building the harness)

**1. Recovery false-positive: `output_progress > last_applied` (FIXED in
production).** The M5 startup invariant in `uc_node/src/runtime/recovery.rs`
treated `output_progress > last_applied` as fatal "data dir corrupt". But the
durable `last_applied` StableValue only advances at snapshot install
(`SnapshotPolicy::LogsSinceLast`, default 5000), whereas `output_progress` is
fsynced per committed output. So **any** shmem node restart against a persisted
`data_dir` before the first snapshot tripped a false-positive panic — the
applied entries are in the log and openraft re-applies them on startup (the
invariant compared against the stale durable marker, not the post-replay
in-memory `last_applied`; in fact the live apply path reads `last_applied` from
raft metrics, so no skip was ever possible). Fix: clamp `output_progress` down
to the durable `last_applied`; after replay the output dispatcher re-drives
`on_committed` for the gap (at-least-once, idempotent by the `log_index`
contract), so nothing is skipped. Surviving corruption checks (`last_seq >=
last_purged`, the committed-clamp floor) still catch genuine log/snapshot
corruption. Cost note: a cold restart before the first snapshot can re-drive up
to ~5000 outputs — correct, but a follow-up could persist `last_applied` more
often to bound it. Regression tests:
`reconcile_clamps_output_progress_ahead_of_last_applied`,
`reconcile_leaves_output_progress_below_last_applied_untouched`.

**2. A reconnecting service is NOT rebuilt — the SM must persist its own state
(documented design, NOT a bug; resolved by making `RegisterSm` persistent).**
When a service crashes and a fresh service reconnects to a still-running node, uc
does not replay history or install a snapshot into it
(`uc_service/src/runtime/service.rs` passes the user SM straight to the apply
loop; `uc_node/src/raft/state_machine_shmem.rs` never queries the service's
`last_applied`). The service resumes applying only **new** entries. This is fine
for the production `StoreStateMachine` (state is durable in `ultima_db`) but means
a purely in-memory SM returns with amnesia. An early version of `RegisterSm` was
in-memory, and the capstone caught the resulting violation: a linearizable read
returned `None` (empty register) long after the register had been written —
because a reconnecting service served fresh/empty state.

**Resolution:** `RegisterSm` was made **self-persisting** (writes `(value,
last_applied)` per apply, fsync-before-ack, reload on startup, `log_index`
idempotency guard) — mirroring the `StoreStateMachine` contract. With that, the
capstone exercises **both** node-kill+restart and service-crash+restart and stays
linearizable across seeds. The contract worth stating loudly: **a custom
`StateMachine` must persist its own durable state to survive a service-only
restart** — uc does not reconstruct it for you. (Making uc reconstruct a
reconnecting service from the log — the deferred `ServiceReady{last_applied}`
cross-check — would make in-memory SMs first-class; see Deferred.)

**Update (task14, service-state reconstruction):** uc now DOES reconstruct a
reconnecting/fresh service from the log (mid-life reattach replay; snapshot-install
+ tail-replay below purge; reconstruct on epoch-change OR prefix-gap), with a
ReadIndex read barrier. So in-memory SMs are first-class — proven by the
`reconstruct_reattach` (incl. read-after-restart) and `reconstruct_snapshot` tests.
The lincheck capstone here **keeps the self-persisting `RegisterSm`** because a
rare in-memory reconstruction race remains under heavy concurrent fault churn
(documented in `docs/tasks/task14_service_state_reconstruction.md` → Known
limitations); self-persistence sidesteps it and keeps the capstone green.

**3. Node shutdown hangs if the service is torn down first while a client write
is in-flight (worked around in the harness; possible uc robustness follow-up).**
Killing a leader under concurrent load surfaced a deadlock: if `service.shutdown`
runs before `node.shutdown` while a worker has an in-flight `client_write`, the
node's apply loop blocks forever awaiting an `apply_resp` from the now-dead
service, so `node.shutdown` never returns. (The standalone `fault_roundtrip`
test, with no concurrent in-flight ops, never hit this.) The harness fixes it by
ordering teardown **node-first, then service** (raft shutdown drains/cancels the
in-flight apply while the service is still alive). Whether `node.shutdown` should
itself be robust to a dead service + in-flight write is a possible production
follow-up; the existing `LinCluster::shutdown` and `m3` paths use service-first
order safely only because they have no concurrent in-flight ops.

## Bounds & determinism

Capstone defaults: seed `0x1107` (override via `LIN_SEED`), `n_workers = 3`,
`target_ops = 800`, a per-worker throttle of ~1 op / 60 ms, faults injected until
the op target is reached (~18 leader failovers, ~50 s wall-clock). The WGL search
cost is dominated by the concurrency window and the number of indeterminate
mutations (each adds an optional drop-branch), so a modest worker count is the
key lever — at 8 workers / 1200 ops the checker hit the visited-state budget and
returned `Inconclusive` (the guard working as designed).

The throttle is load-bearing: a single `kill_and_restart_leader` takes ~5 s of
recovery, during which unthrottled workers drive the survivors hard enough to hit
an 800-op target in a *single* failover window. Throttling to ~46 ops per
failover spreads the workload across ~18 failovers with a bounded, checker-
friendly history. These bounds complete reliably (verified across multiple seeds)
while exercising many leader changes — ample to surface a stale-read /
lost-update / double-applied-CAS bug.

Outcomes: `Linearizable` → pass; `Violation` → fail, dump the full history to
`/tmp/lincheck_history_<seed>.txt` (the checker is deterministic on a captured
history, so the dump is the reproducible debugging artifact even though the real
tokio/shmem/openraft interleaving is not bit-reproducible); `Inconclusive` →
fail loudly (lower bounds). A **liveness gate** requires ≥80% of ops to complete
`Ok`, distinguishing a cluster-progress failure from a linearizability bug.

## Running it

```bash
cargo test -p uc_node --test lin_register                       # all lincheck tests
cargo test -p uc_node --test lin_register linearizable_under_failover -- --test-threads=1
LIN_SEED=12345 cargo test -p uc_node --test lin_register linearizable_under_failover -- --test-threads=1
```

The pure checker/model unit tests run in milliseconds on every `cargo test`; the
capstone takes ~1–2 minutes.

## Deferred / future work

- **Hard service crash (`kill -9` mid-apply).** Faults today are *graceful*
  shutdowns, so the in-flight apply completes before teardown. A true crash that
  drops an entry already read off the SPSC apply ring but not yet acked is not
  exercised; whether uc re-drives that entry on reconnect (vs. the node stalling
  awaiting its ack) is untested. Needs an abrupt task-abort fault primitive.
- **uc-side service-state reconstruction** — make the node rebuild a reconnecting
  service from the log (drive replay from the already-defined
  `ServiceReady{last_applied}` handshake; snapshot-install for purged ranges).
  This would make purely in-memory SMs first-class (no per-SM persistence
  needed) and subsumes the manual persistence in `RegisterSm`. Bigger, own spec.
- **Linearizability with the real `StoreStateMachine`** (ultima_db adapter)
  rather than the test `RegisterSm` — maximal production realism; heavier test.
- **Network partition / quorum-loss / packet loss** — needs a lossy QUIC test
  transport; deferred from the original design.
- **Deterministic simulation (sim-clock + sim-net)** — the cluster runs on real
  tokio/shmem/openraft; full DST is out of scope.
- **Bound the restart output-replay cost** (Finding #1) by persisting
  `last_applied` more frequently than snapshot cadence.
- **Violation witness** — the checker returns `Verdict::Violation` without a
  minimal sub-history; localization today is via the dumped history.
- **`node.shutdown` robustness** (Finding #3) — make node shutdown tolerate a
  dead service with an in-flight client write, instead of relying on teardown
  order.
