# Design: Linearizability Test Harness for `ultima_cluster`

**Date:** 2026-06-05
**Status:** Draft for review
**Scope:** A CI-runnable, seeded linearizability regression test that drives a concurrent CAS-register workload through repeated leader-kill/restart and service-crash failovers, records a history, and proves it linearizable with an in-repo Wing-Gong-Lowe checker. Test-only; no production code changes.

## Problem

`ultima_cluster` has good *scenario* tests (elect, replicate, failover, snapshot install, service crash, client retry) but **no linearizability check** — the one test that directly exercises the core SMR guarantee (every client sees a single, real-time-consistent history) under failover. Failover is exactly where naive SMR breaks: stale reads after a leader change, lost updates during a transfer, a CAS that appears to succeed twice. This harness adds that guard.

## Scope decisions (from brainstorming)

- **Goal:** a CI-runnable **seeded regression test** (bounded, permanent suite guard), not an exploratory soak tool.
- **Model:** a single **CAS register** (`read` / `write(v)` / `cas(old,new)`) — the canonical Jepsen single-object model, maximal signal for stale-read / lost-update bugs.
- **Checker:** an **in-repo Rust Wing-Gong-Lowe (WGL)** checker, generic over a `Model` trait. No external toolchain.
- **Placement:** a **test-local, self-contained** module (`uc_node/tests/lincheck/`) + one test file. Zero production surface; no refactor of existing tests.
- **Faults:** **leader kill+restart** and **service crash+restart**, both in-process and quorum-preserving. Network partition / quorum-loss / full DST are **out of scope** (deferred — partition needs a lossy QUIC transport).

## Non-Goals

- Network partition, packet loss/reorder, quorum-loss windows. (Need a lossy QUIC test transport — deferred.)
- Deterministic simulation testing (sim-clock + sim-network). The cluster runs on real tokio/shmem/openraft.
- Multi-key KV / counter models (the `Model` trait makes them trivial later; v1 is the register only).
- Any change to production crates (`uc_node`/`uc_service`/`uc_client`/`uc_protocol`). Test-only.

## Architecture

Module `uc_node/tests/lincheck/` (declared `mod lincheck;` from the test file), five focused units; the pure ones carry no cluster/tokio deps so they unit-test in isolation.

### `model.rs` — sequential spec (pure)
```rust
pub trait Model {
    type State: Clone + Eq + std::hash::Hash;
    type Op: Clone;
    type Resp: Eq + Clone;
    fn init() -> Self::State;
    fn step(state: &Self::State, op: &Self::Op) -> (Self::State, Self::Resp);
}
```
`RegisterModel`: `State = Option<u64>`; `Op = Write(u64) | Read | Cas{old:u64,new:u64}`; `Resp = Ack | Value(Option<u64>) | CasOk(bool)`. `step`: `Write(v)→(Some(v),Ack)`; `Read→(s, Value(s))`; `Cas{old,new}→ if s==Some(old) {(Some(new),CasOk(true))} else {(s,CasOk(false))}`.

### `register_sm.rs` — the replicated SM (`uc_service::StateMachine`)
The *real* SM the cluster runs. `Command = Write(u64) | Cas{old:u64,new:u64}`; `Response = WriteAck | CasResult(bool)`; `Query = Read`; `QueryResponse = Option<u64>`. `apply(log_index, cmd)` mutates a single `Option<u64>` and records `last_applied`; `query(Read)` returns the value; `build/install_snapshot` (de)serialize the `Option<u64>` + last_applied. Mirrors the existing `Counter` test SM shape.

### `history.rs` — recording (pure types + a recorder)
```rust
pub enum Op { Write(u64), Read, Cas { old: u64, new: u64 } }
pub enum Outcome { Ok(RegResp), Indeterminate }   // RegResp mirrors RegisterModel::Resp
pub struct Entry { pub client: u32, pub op: Op, pub invoke: u64, pub ret: u64, pub outcome: Outcome }
pub struct History { /* Mutex<Vec<Entry>> + global AtomicU64 seq */ }
```
Real-time order via a single global `AtomicU64` `fetch_add` at invoke and at return: `A` precedes `B` iff `A.ret < B.invoke`. (Deterministic; avoids clock jitter under the single-thread runtime.)

### `checker.rs` — WGL linearizability search (pure, generic over `Model`)
`check::<M: Model>(entries) -> Verdict` where `Verdict = Linearizable | Violation(Vec<Entry>) | Inconclusive`.

### `cluster.rs` — 3-node shmem fault harness
Spawns 3 nodes + 3 services + 3 clients (one `instance_dir` per node), modeled on `m2_multi_node::spawn_3_node_cluster` + `m3_service_crash`. Holds per-node `NodeConfig` + `Option<NodeHandle/Service/Client>` so faults can `take()`/restart. Provides `submit_to_leader`, `kill_and_restart_leader`, `crash_and_restart_leader_service`, `wait_for_stable_leader`.

### `tests/lin_register.rs` — the integration test
Wires it: spawn cluster → run seeded workload + seeded fault scheduler → collect history → `assert linearizable`.

## History recording & outcome classification

Worker loop per op: `inv = SEQ.fetch_add(1)`; fire op against the leader; `ret = SEQ.fetch_add(1)`; classify result; record-or-retry.

Classification of `ClientError`:

| Result | Disposition |
|---|---|
| `Ok(resp)` | record `Outcome::Ok(resp)` (must linearize with exactly this response) |
| `Timeout`, `ResponseOverwritten` | record `Outcome::Indeterminate` (may/may-not have committed; response unobserved) |
| `NodeStalled`, `ServiceStalled` | record `Outcome::Indeterminate` (died mid-flight) |
| `NotLeader{hint}`, `BackpressureFull`, `ShutDown` | **DidNotExecute → not recorded; retry** against the (new) leader |
| `Submission`/`Decode` | DidNotExecute (request-side; won't occur with `u64` ops) |

Policy: DidNotExecute → retry (never reached the log). Indeterminate → record, **never retry** (a blind retry of a possibly-committed `cas`/`write` could double-apply; the checker handles the in-limbo op). Reads use `query_linearizable`.

## The WGL checker

Standard Wing-Gong-Lowe linearization-point search. Maintain model `State` + the not-yet-linearized set. Repeatedly pick a **frontier op** (its `invoke` precedes the minimum `ret` of the remaining ops — real-time-eligible), `model.step` it, and for an `Ok(observed)` op require `resp' == observed`; lift on match, unlift/backtrack on dead-end. **Memoize** visited `(linearized-bitset, state)` to prune (the optimization behind Knossos/Porcupine).

**Indeterminate ops:** `ret = u64::MAX` (linearizable any time at/after `invoke`); **optional** (the search may leave them un-linearized ≡ never committed); when linearized their **response is unconstrained**. **Indeterminate reads carry no information** (unknown value, no state change) → the checker **drops them**; only indeterminate *mutations* (write/cas) enter the search as optional unconstrained effects. Success = all `Ok` ops linearized + a consistent subset of indeterminate mutations.

**Bounds & no-false-pass:** worst-case exponential, so cap total ops (~≤ 2000) and in-flight concurrency (~≤ 8) — the concurrent window (the exponential factor) stays small while length scales ~linearly with the memo. A **visited-state budget** returns `Inconclusive` if exceeded — never a silent `Linearizable` from a truncated search.

**Failure output:** `Violation` returns the minimal offending sub-history; the test prints it + the seed and dumps the full history to a file for offline re-checking.

## Fault harness (3-node shmem, quorum-preserving)

- **Route-to-leader:** `submit_to_leader(op)` finds the leader via `client.current_leader()` → that node's client → submit; on `NotLeader{hint}` switch to the hinted node; on `NodeStalled` re-poll survivors. (This is the DidNotExecute retry path.)
- **Leader kill + restart:** `take()`+shutdown the leader's node **and** service (graceful); quorum (2/3) holds, survivors re-elect. After a delay, **restart** the node with the same node_id + persisted `data_dir` (rejoins as follower) + a fresh service; **reconnect that node's client** (a restarted node has a new `instance_id`, invalidating the old client).
- **Service crash + restart:** `take()`+shutdown the *leader's service* only; the node's service-watcher transfers leadership (the `m3` path); restart a fresh service on the same `instance_dir`.
- **Seeded scheduler:** a dedicated task; on a cadence (every K ops) it picks fault 1 or 2 from the seeded RNG, applies it, and **waits for a stable leader among survivors before the next fault**. Invariant: **at most one node-or-service down at a time** → quorum never lost → the workload keeps progressing.

**Verification points for implementation** (flagged, not assumed):
1. A killed voter, restarted at the same node_id/data_dir, **rejoins and the cluster commits while it is down** *without* a membership change. (`m4_client_leader_failover` instead `remove_node`d the dead voter; we rejoin. If openraft stalls without removal, fall back to remove + re-add on restart.)
2. The exact `ClientError` returned on an `instance_id` mismatch after a node restart, to trigger the client reconnect.

## The test (CI shape)

`#[tokio::test(flavor = "current_thread")]` (shmem requirement), file-local `CLUSTER_SERIAL` guard. **Seed** from `LIN_SEED` env (default fixed constant → deterministic CI; sweep locally). Bounds: ~1–2 k ops, 8 workers, fault every ~150–250 ops, ~30–60 s budget.

Outcomes:
- **Linearizable** → pass.
- **Violation** → fail; print minimal sub-history + seed; dump full history.
- **Inconclusive** (budget exceeded) → fail loudly (lower bounds).
- **Liveness gate:** require ≥ ~80 % of ops to complete `Ok`; otherwise the cluster never progressed → fail with a distinct "harness/cluster" message (not a linearizability bug).

**Determinism — honest framing:** seeded *workload + fault schedule* (reproducible choices), but real tokio/shmem/openraft means the exact interleaving is **not** bit-reproducible across runs. The **checker is deterministic on a captured history**, so the dumped history is the reproducible debugging artifact. Full DST deferred.

**Always-on cheap guards:** pure unit tests in `checker.rs`/`model.rs` feed known-good and known-bad histories (stale read, double-applied cas, an indeterminate op that must be allowed) and assert `Linearizable` vs `Violation` — validating the checker independent of the cluster, in milliseconds. The cluster test is the integration capstone.

## Deliverables

- `uc_node/tests/lincheck/{mod,model,register_sm,history,checker,cluster}.rs`
- `uc_node/tests/lin_register.rs`
- Checker/model unit tests (in `checker.rs`/`model.rs`).
- No production-crate changes. `cargo test` green; `cargo clippy --all-targets -- -D warnings` clean.

## Open risks / notes

- **Checker correctness** is the highest risk — mitigated by the pure known-good/known-bad unit tests *before* trusting it on cluster output.
- **Restart-rejoin** (verification point 1) — if openraft needs the dead voter removed to progress, fall back to membership change on restart; either way keep all-quorum.
- **Flakiness** — the liveness gate + bounded faults (one at a time, wait-for-recovery) keep the cluster progressing; the `Inconclusive` budget prevents false passes; timing non-determinism means a failure is reproduced via the dumped history, not by re-running the seed.
- **Runtime cost** — heavier than a unit test (~tens of seconds); feature-gate later if it strains CI.
