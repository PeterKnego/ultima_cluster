# SyncCore 3c — disruptor-native replication + input ring — Design

**Date:** 2026-06-29
**Status:** Proposed — awaiting review
**Repo of work:** openraft fork `PeterKnego/openraft` branch `sync-core` (continues 3d + the completion-as-notification redesign)
**Predecessors:** `2026-06-28-synccore-phase3-decisions.md` (the deferred 3c A/B fork), `2026-06-28-synccore-disruptor-pipeline-design.md` (target architecture), `2026-06-28-synccore-completion-notification-redesign-design.md` (3d redesign), `docs/benchmarks/synccore-latency-injected-2026-06-29.md` (microbench evidence).

## Decision: B (disruptor-native), full pipeline — bounded by the async/quinn boundary

The deferred 3c A-vs-B fork is resolved **B**: reimplement replication as busy-spin
**disruptor consumers** (the fully disruptor-native option / full throughput ceiling), and
add the **disruptor input ring** — not A (reuse openraft's replication tasks reactor-free).

A grounding constraint discovered from the source map (`ReplicationCore` + the consensus
input plumbing) bounds what "full pipeline" can mean **under quinn (3c)**:

- **Sync producers** (durability consumer, the new network consumers, eventually the apply
  consumer) are busy-spin threads — they publish to a disruptor ring natively.
- **Async producers** — the public `Raft` API (`client_write`) **and inbound RPCs** (the QUIC
  server calls `raft.append_entries()`/`vote()`) — are tokio tasks that `await tx_api.send`
  for backpressure. A disruptor `publish` is a blocking spin; calling it from a tokio task
  blocks a worker. And quinn (async, reactor-driven) cannot be polled reactor-free, so the
  architecture-doc "network consumer polls inbound datagrams" is a **3e (UDP)** capability.

So a *single* fully-unified input ring is **gated on 3e**. In 3c the consensus core pulls a
disruptor ring for everything **sync**, and keeps tokio channels for the **async** API/RPC
path. This is the honest bound; full collapse happens at 3e.

## Target shape (3c)

```
   clients / QUIC-server (async) ──tokio mpsc rx_api──┐
   tick + sm-worker apply (async) ──tokio mpsc────────┤   (try_recv each loop)
                                                       ▼
                          ┌──────── CONSENSUS CORE (sync, pinned) ────────┐
                          │ loop { poller.poll(input_ring);               │
                          │        try_recv(rx_api); try_recv(rx_notif);  │
                          │        engine.handle_*; for c in pop_command: │
                          │          dispatch(c) }                         │
                          └───┬───────────────┬───────────────▲───────────┘
              durability ring │   per-peer    │  send rings    │ INPUT RING (disruptor, MPSC)
                              ▼   send rings   ▼                │ {network acks | io-done}
                     DURABILITY consumer   per-peer NETWORK consumers
                       (exists, 3b.2)        (NEW, busy-spin, hybrid-reactor quinn)
                              │                   │
                              └── io-done ────────┴── acks/results ──▶ INPUT RING
```

The win: the **hot** sync path (replication acks + io-done) becomes a disruptor ring instead
of tokio mpsc (the `hi-perf-cmp` study: disruptor beats channels); replication leaves
RaftCore entirely (the milestone that fully severs RaftCore).

## Stage 3c.1 — replication as per-peer busy-spin network consumers

The bulk of B, and the riskiest code (~60% new — the replication executor). Suite-guarded.

**Topology: one busy-spin network-consumer thread per follower** (not single-multiplexed).
Under quinn each RPC is an async round-trip driven via reactor-free `block_on` (hybrid
reactor); a single thread `block_on`-ing peers sequentially would serialize replication. Per-
peer threads preserve today's parallelism (2–4 threads typical). Single-multiplexed (one
socket, peer table) is a **3e/UDP** optimization (non-blocking `sendto`), deferred. The fleet
diag (2026-06-29) confirmed thread count at these scales is not a saturation concern.

**Transport — two disruptor ring classes:**
- **Per-peer send ring** (consensus → peer consumer): the engine's dispatched ops for that
  target — `Replicate{req}`, `Heartbeat`, `Vote`/`PreVote`, `ReplicateSnapshot`,
  `TransferLeader`. `SyncCore::run_command`'s currently-delegated arms publish here instead
  of `self.core.run_command` (which spawns tasks). `RebuildReplicationStreams` /
  `CloseReplicationStreams` become peer-table management (add/remove a peer consumer +
  its ring).
- **Ack path** (peer consumers → consensus core): the reproduced ack contract. **In 3c.1
  acks are published via the existing `tx_notification` channel** (the loop already drains it
  through `process_notification`), so 3c.1 stays suite-green without depending on the ring.
  **3c.2 moves this onto the disruptor input ring** (peer consumers become MPSC producers into
  it, sharing it with the durability io-done feed). Keeping the ack transport swap in 3c.2
  isolates the executor reimplementation (3c.1) from the input-path rewrite (3c.2).

**The reimplemented per-peer executor** owns, reactor-free, what `ReplicationCore` owns
today (mapped from `replication/mod.rs` + helpers):
- per-peer `matching`/progress + `remote_matched`;
- the **inflight queue** (last_log_id + send-time; `drain_acked` for RTT/heartbeat timing);
- **payload selection**: log-id-range vs `LogsSince` vs zero-length heartbeat, driven by the
  send-ring op + `committed`;
- entry reads via the **`GatedLogReader`** vended by the durability consumer (the reader-vend
  `std::sync::mpsc` side-channel stays — risk 6);
- the RPC: `network.stream_append(req_stream, option)` driven by `block_on` (hybrid reactor),
  response-stream handling, `backoff` on RPC error;
- the **snapshot** sub-state machine (`network.full_snapshot`, chunked, cancel/replace);
- emitting the **exact ack contract** to the input ring.

**Reproduced ack contract** (must match the map exactly):
- `ReplicationProgress{progress, inflight_id}` — on match (`Ok(Ok(Some(matching)))`,
  `inflight_id` set when entries sent; not emitted when `matching.is_none()`), on conflict
  (`Ok(Err(conflict))`, always, may have `inflight_id=None`), and on RPC error (`Err(string)`,
  only when `inflight_id.is_some()`).
- `HeartbeatProgress{stream_id, sending_time, target}` — after any timed RPC round-trip that
  `drain_acked` confirms (log replication, heartbeat, snapshot success).
- `HigherVote{target, higher, leader_vote}` — on a peer returning a higher vote → step down.
- `StorageError{error}` — on a storage error reading a snapshot → fatal.
- vote path: `VoteResponse`/`PreVoteResponse{target, resp, candidate_vote}` on vote replies;
  no notification on vote RPC transport failure (a failure is not a grant).

**Risk handling:**
- **Risk 2 (readability):** entry reads go through the `GatedLogReader`, which blocks until the
  durability consumer's `io_submitted` watermark covers the range — cannot ship not-yet-
  readable entries. No extra gate.
- **Risk 3 (FIFO + inflight_id):** each peer consumer publishes its `ReplicationProgress`
  slots to the input ring **strictly in network-response order**; the engine matches
  `(target, inflight_id)` exactly as today. A peer consumer must not reorder its own slots.
- **Risk 4 (snapshot cancel/replace):** the peer consumer holds a `snapshot-in-progress`
  sub-state; a newer `ReplicateSnapshot`/`Replicate` op cancels-and-replaces it; the snapshot
  stream checks the cancel + send-ring between chunks so a slow snapshot doesn't stall the
  peer's drain.
- **Risk 6 (reader-vend):** unchanged — peer consumers request `GatedLogReader`s from the
  durability consumer via the existing `log_reader_request_tx` side-channel.

## Stage 3c.2 — disruptor input ring (sync feed) + EventPoller pull

- The consensus core pulls the **input ring** via the disruptor `EventPoller` for the sync
  events: network acks (3c.1) + **io-done** published **directly by the durability consumer**
  (which **removes the `io_completion_forwarder` tokio task** and its 1µs batch — a real
  simplification). The loop: `poller.poll()` → `engine.handle_notification` → `pop_command`
  drain → dispatch.
- **Residual tokio channels, `try_recv`'d alongside** (the async boundary, until 3e):
  `rx_api` (client writes + inbound QUIC RPCs — preserves async backpressure) and the
  notification channel for tick + sm-worker apply-progress (apply hop is 3e).
- So the loop pulls **three sources** (input ring via poller; `rx_api`; residual notif
  channel). The balancer/budget logic from 3d carries over.
- **Risk 1 (many async producers):** unaddressed by design intent — the async API/RPC
  producers stay on `tx_api` (tokio mpsc, native async backpressure); they do NOT publish to
  the ring in 3c. Closing this (single ring) is 3e.
- **Risk 5 (linearizable read barrier):** `GetLinearizer` arrives via `rx_api` (async path),
  is processed by the loop, emits a heartbeat (ReadIndex), and returns via oneshot — unchanged
  from 3d; it does not sit behind network-consumer ring slots.

## Sequencing

1. **3c.1** — per-peer network consumers + send rings + reimplemented executor; **acks via the
   existing `tx_notification` channel**; `run_command` delegated arms → publish; peer-table
   lifecycle. Biggest, riskiest; suite green before moving on. This severs RaftCore's
   replication.
2. **3c.2** — introduce the disruptor input ring; move network acks **and** durability io-done
   onto it (durability consumer publishes io-done directly); consensus core `EventPoller`-pulls
   the input ring; drop `io_completion_forwarder`. Suite green.
3. **Measure** — `commit_latency` (still primary) + a denoised fleet A/B once the bench-infra
   ansible flakiness (see `synccore-fleet-2026-06-29.md`) is fixed.

## Testing

- **openraft 180-suite green at every step** — `cargo test -p tests --features sync-core`
  (replication / membership / snapshot / election tests are the correctness oracle for the
  reimplemented executor + ack contract) + 495 lib; default RaftCore path unaffected; clippy
  clean both feature states.
- **UC linearizability + partition suites** — `uc_node` lincheck capstone + `lin_partition`
  (behind `fault-injection`) — the ultimate check that reimplemented replication preserves
  linearizability under churn + network faults. Run before declaring 3c done.
- Extend the `sync_durability` consumer unit test family with a network-consumer ack-contract
  unit test (a stub `RaftNetworkV2` driving the per-peer executor, asserting the emitted
  `ReplicationProgress`/`HeartbeatProgress`/`HigherVote` sequence for match/conflict/error).

## Risks carried

- **Reproducing the ack contract exactly** is the make-or-break; the 180-suite + lincheck are
  the guard, but subtle `inflight_id`/FIFO/`matching.is_none()` mistakes corrupt quorum state.
- **Snapshot cancel/replace** reactor-free is subtle (long-running, must not stall the ring).
- **Per-peer thread count** grows with cluster size; fine at v1 scale, revisit at 3e
  (single-multiplexed UDP consumer).
- **block_on driving quinn** must complete reactor-free under the hybrid reactor — already
  true for the delegated path (it runs on tokio); here the peer consumer thread enters the
  runtime context like the consensus thread.
- **Fork divergence** from upstream grows further; the 180-suite keeps it honest.

## Next

On approval → writing-plans to decompose 3c.1 then 3c.2 into bite-sized, suite-guarded tasks,
executed subagent-driven (same as 3a/3b/3d-redesign).
