# SyncCore disruptor pipeline (Phase 3b) — Design

**Date:** 2026-06-28
**Status:** Proposed — awaiting review
**Repo:** openraft fork `PeterKnego/openraft` branch `sync-core` (continues Phase 3a)
**Predecessors:** `docs/superpowers/plans/2026-06-27-synccore-sync-transformation.md` (Phase 3a, done), `docs/openraft-busyspin-runtime-feasibility.md`, `docs/benchmarks/floor-decomposition-2026-06-25.md`

## Decisions locked

1. **Ring transport: `disruptor-rs`** — chosen on evidence (the `hi-perf-cmp` handoff-vs-disruptor study: disruptor beats a lean hand-rolled ring ~17–33%, gap widens with burst, both crush channels; plus multi-consumer + dependency barriers + event-poller fit a multi-stage pipeline).
2. **Fully-synchronous, reactor-free I/O consumers** — no tokio reactor, no futex scheduler in the hot path. The consensus core and the I/O stages are synchronous loops on pinned threads, coordinated by disruptor rings.

## The key design tension and its resolution

**Tension:** Phase 3a's whole validation strategy is *reuse openraft's 180-test suite* by keeping the public `Raft` async API and the async `RaftLogStorage`/`RaftNetworkV2`/`RaftStateMachine` trait seams (the suite's memstore + in-memory router are *async* trait impls). But "fully-synchronous, reactor-free" sounds like *removing* those async traits — which would break test-reuse.

**Resolution — keep the async trait seam, drive it reactor-free.** The async storage/network *futures* in a reactor-free impl (memstore, in-memory router, a direct-`fdatasync` journal, a blocking-`sendto` UDP transport) **complete via synchronous operations — they never `.await` a reactor**. So a trivial **`block_on` with no tokio runtime** (a busy-poll-to-`Ready` on a dedicated consumer thread) drives them to completion. There is no reactor, no futex park — yet the trait seam stays, so the suite's async impls keep working AND UC's existing adapters keep working. "Reactor-free synchronous" is a property of *how we drive* the futures (no runtime) + *what the impls do* (sync syscalls), not of removing `async` from the trait signatures.

**Consequence:** the reactor-free **architecture** comes first and is suite-guarded against the existing async impls. The **UDP transport swap** (below) is a later, benchmarked *backend* change *inside* the network consumer — not a precondition, and not a test-reuse risk.

## Target architecture

```
            ┌──────────────────────── CONSENSUS CORE (pinned, sync) ───────────────────────┐
clients ───▶│ input ring: {client cmd | inbound RPC | RPC ack | io-done | tick}            │
            │ loop { ev = poller.poll(); engine.handle_*(ev); for c in pop_command():       │
            │          dispatch(c) }                                                          │
            └───┬───────────────┬────────────────────────┬───────────────────────┬──────────┘
   apply ring   │   durability ring         per-peer net ring(s)        (Respond/Watch: inline, sync)
                ▼               ▼                        ▼
        APPLY consumer   DURABILITY consumer     NETWORK consumer(s)   ── all reactor-free, busy-spin
        (RaftStateMachine (RaftLogStorage         (RaftNetworkV2 send;  ── drive the async trait via
         .apply, sync)     .append + fdatasync)    recv inbound)          no-runtime block_on
                │               │                        │
                └── responses ──┴──── io-done ───────────┴── acks/inbound ──▶ back to input ring
```

- **Consensus core** — one pinned thread. Owns the `Engine` (from Phase 3a SyncCore). Consumes the input ring via the disruptor **`EventPoller`** (it must *pull*, because it batches Engine drives and publishes outputs — not a managed per-event closure). Drives `engine.handle_*`, drains `pop_command()`, and **dispatches each command**: pure-state commands (`Respond`, `ReplicateCommitted`, `UpdateIOProgress`) execute **inline** (already synchronous); I/O commands **publish to the relevant consumer ring**.
- **Durability consumer** — pinned, managed disruptor consumer. Reads `AppendEntries`/`SaveVote`/`SaveCommitted`/`Purge`/`Truncate` events; drives `RaftLogStorage` (reactor-free `block_on`); publishes the `IOFlushed`/completion back to the input ring (this is how the Engine learns the log is durable — preserves openraft's existing IO-completion semantics).
- **Network consumer(s)** — pinned, busy-spin. Bidirectional: drains the send ring (outgoing AppendEntries/Vote/heartbeat/snapshot via `RaftNetworkV2`, or later direct `sendto`) **and** polls inbound datagrams (non-blocking recv), publishing inbound RPCs + acks to the input ring. Replication-stream lifecycle (`RebuildReplicationStreams`/`CloseReplicationStreams`) becomes **consumer/peer-table management**, not tokio task spawning.
- **Apply consumer** — pinned, managed. Reads apply commands; calls `RaftStateMachine::apply` (sync); publishes responses.

### How openraft's task-spawning commands map (the hard part)

openraft's `RaftCore` executes several commands by **spawning tokio tasks** (per-peer `ReplicationCore`, snapshot transmitter, parallel vote senders, heartbeat workers). These are exactly what the disruptor model replaces:

| Command | RaftCore today | Disruptor pipeline |
|---|---|---|
| `Replicate{target}` | send to per-peer ReplicationCore task | publish to target's network ring |
| `RebuildReplicationStreams` | spawn per-peer tasks | add/configure per-peer network-consumer entries |
| `CloseReplicationStreams` | drop tasks | remove peer entries |
| `BroadcastHeartbeat` | heartbeat workers | publish heartbeat to all peer rings |
| `SendVote`/`SendPreVote` | spawn parallel vote RPCs | publish vote-req to all peer rings; acks → input ring |
| `ReplicateSnapshot{target}` | spawn snapshot transmitter | network consumer streams snapshot to target |
| `BroadcastTransferLeader` | broadcast | publish to peer rings |

Inbound RPC responses/acks that RaftCore today receives via `tx_notify` become input-ring events published by the network consumer. **No tokio tasks; the per-peer "stream" is a peer entry in a busy-spin network consumer.**

## Staged decomposition (each milestone suite-guarded, 180/0)

Built incrementally so the suite stays green and the *architecture* lands before the *transport optimization*:

- **3b.1 — SyncCore owns `run_command` (relocate, no rings yet).** Move the per-command match into SyncCore. Execute storage/apply/respond/pure-sync commands directly; **delegate the task-spawning (replication/snapshot/vote/heartbeat) commands to RaftCore** for now (single-node fully self-driven; multi-node still uses RaftCore's replication). This is the reviewer-flagged "first change that breaks RaftCore-untouched" — it needs RaftCore's I/O executor *factored out*, not reached into. Suite green (single-node + multi-node via delegated replication).
- **3b.2 — reactor-free I/O driver + durability & apply consumers.** Introduce the no-runtime `block_on` driver and move log-append/fsync and apply onto ring-fed durability/apply consumer threads. Storage/SM still the async traits (reactor-free). Suite green.
- **3c — network consumers (replication off RaftCore).** Replace the delegated replication/vote/heartbeat task-spawning with busy-spin network consumers driving `RaftNetworkV2` (still the trait; the in-memory test router for the suite). Per-peer lifecycle = peer-table management. Suite green — this is the milestone that fully severs RaftCore.
- **3d — synchronous input + disruptor-rs wiring + pinning.** Replace the async `select!` input with the disruptor input ring + `EventPoller`; pin the consensus core; busy-spin. The full pipeline is now reactor-free/sync. Suite green.
- **3e — UDP transport backend + measure.** Swap the UC network consumer's backend from quinn to a lean UDP datapath (Raft-tolerant of loss; fragmentation for large batches via a lean reliability shim or UC's task16 core). **Benchmark UDP-vs-quinn end-to-end in-pipeline** (task16 showed the edge is network-dependent — validate, don't assume). Re-run `busyspin-commit-bench` + a multi-node bench vs the floor decomposition.

> Test-reuse note: 3b.1–3d keep openraft's async trait impls (the suite drives them). The UDP swap (3e) is a UC-side network backend behind the same `RaftNetworkV2` seam, benchmarked separately — it does not affect the openraft suite.

## Risks

- **Replication choreography (3c)** is the subtlety-dense core — the Engine assumes commands complete and feed results back as events; getting the inbound-ack/IO-done feedback ordering wrong breaks linearizability. The 180-suite (replication, membership, snapshot) + UC's lincheck/partition suites are the guard.
- **`disruptor-rs` integration:** no closed-signal (add a sentinel/closed-flag for shutdown); `publish` blocks on a full ring (size output rings for the consensus core's burst; the consensus core must not stall on a slow consumer — bound or drop-with-backpressure per ring class).
- **Fork maintenance:** SyncCore + the run_command extraction diverge further from upstream; the suite keeps them honest but rebasing on new alphas costs more.
- **UDP transport (3e):** loss-tolerance ok for Raft (idempotent AppendEntries + retries); large-batch fragmentation + (optional) TLS are real work; end-to-end edge is network-dependent — hence benchmarked, not assumed.

## Open sub-decisions (for review)

- **Network consumer granularity:** one busy-spin consumer multiplexing all peers (single UDP socket, peer table) vs one consumer thread per peer. Lean to **single multiplexed consumer** (fewer pinned cores; one socket); revisit if a hot peer needs isolation.
- **Where SyncCore's run_command extraction lives:** a new `run_command` on SyncCore mirroring RaftCore's, or factor RaftCore's executor into a shared sync-friendly unit both call. Lean to **mirror-then-diverge** (consistent with Phase 3a), accepting the tracked duplication until 3c severs RaftCore.

## Next

On approval, decompose **3b.1** into a bite-sized plan (writing-plans) and execute subagent-driven, the same way Phase 3a ran.
