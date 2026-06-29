# SyncCore teardown investigation — root cause is a test-harness cycle, not a product bug (2026-06-29)

Followed the fleet-hang hypothesis that SyncCore leaks a busy-spinning consensus thread on
teardown. Systematic debugging (reproduce → instrument → root-cause) found a **real leak in
a reproduction harness, but no bug in UC**, and it was **not** the fleet hang.

## Reproduction & confirmation

A `thread_leak` bin (openraft `benchmarks/minimal`, since removed) created + dropped a
single-node cluster in a loop and counted `/proc/self/task`:

- **SyncCore, plain drop (no `shutdown()`):** +2 OS threads per iteration (consensus thread
  + durability consumer), never reclaimed — the consensus loop idle-spins forever.
- **SyncCore, explicit `shutdown()`:** stable thread count — clean teardown.
- **RaftCore, plain drop:** stable (its core is a tokio *task*, not an OS thread).

So `shutdown()` tears SyncCore down correctly; plain drop does not.

## Root cause (instrumented, definitive)

The SyncCore consensus loop exits only when `tx_shutdown` is **sent** (`shutdown()`) or
**dropped** (the `Arc<RaftInner>` holding it is freed). Probing `Arc::strong_count(&inner)`
showed it **stays pinned** after `drop(raft); drop(router)` — so the dropped-sender path
never fires and the loop spins forever.

The pin is a **reference cycle in the bench-minimal `Router`**, which is *both* the
`RaftNetworkFactory` *and* the holder of the cluster's `Raft` handles:

```
consensus std::thread → SyncCore → RaftCore.network_factory (= a Router clone)
    → Router.table → Raft handle → Arc<RaftInner> → tx_shutdown   ⟸ the loop waits on this
```

RaftCore has the *same* cycle but tolerates it: its core is a tokio task that the runtime
cancels at teardown, which drops `RaftCore` → breaks the cycle → `RaftInner` frees. SyncCore's
consensus thread is a detached `std::thread` the runtime cannot cancel, so the cycle (and the
idle-spin) persist.

## Why UC is unaffected

- **No cycle.** UC's `QuicRaftNetworkFactory` / `UdpRaftNetworkFactory` hold connection state
  (peer pool, endpoint, config) — **never a `Raft` handle**. So on node drop, `RaftInner`
  frees, `tx_shutdown` drops, and the consensus thread exits. Verified by reading both
  factories.
- **And UC calls `shutdown()`.** `uc-node-launch` calls `node.shutdown()` on ctrl-c; UC never
  drops a live node without it (service reconstruction replays the SM; it does not recreate the
  openraft node).

So the leak cannot occur in UC, and it was therefore **not** the fleet hang.

## Implications

1. **No product fix is required.** The leak is a property of an in-process test router that
   strong-references its own nodes through the network factory + a forgotten `shutdown()`.
2. **Contract (now explicit):** any in-process harness that builds SyncCore nodes must call
   `Raft::shutdown()` on teardown (or avoid a network-factory→node reference cycle). This is
   the same contract RaftCore has; RaftCore is just far more forgiving when it's violated.
3. **Latent fragility (optional hardening):** a detached busy-spin OS thread that can be
   orphaned by a reference cycle is more fragile than RaftCore's parked, runtime-cancelled
   task. A future hardening could tie the consensus thread's lifetime to the runtime (e.g. a
   bridge-task drop-guard that signals the loop to stop), so it dies at runtime teardown like
   RaftCore. Not needed for correctness in UC; deferred.

## The fleet hang remains unexplained

This was the leading hypothesis for the 2026-06-29 fleet hang (`synccore-fleet-2026-06-29.md`)
and it is now **ruled out**. The fleet's `uc-node-launch` shuts down gracefully and has no
cycle. The hang (all 3 nodes unresponsive to sudo mid-rep-batch) needs separate investigation
— candidates: SyncCore's higher steady-state CPU (busy-spin) saturating the shared box under
sustained sweep load, the iterate relaunch transiently overlapping old+new nodes, or an
AWS/instance issue. None confirmed.
