# SyncCore minimal-3d commit→apply microbench (2026-06-28)

**What:** First realized measurement of the Model-B SyncCore work — the minimal "3d"
synchronous consensus loop (openraft fork `sync-core` branch, commit `324d5bd5`) vs the
async `RaftCore`, on an in-memory cluster. This is the "primary, cheap" measurement the
phase-3 decisions doc deferred until 3d unblocked it.

**Harness:** `openraft/benchmarks/minimal` `commit_latency` bin (new). In-memory log
store + state machine + in-process network (openraft's own minimal bench crate); a single
client drives `client_write(ClientRequest{})` sequentially (inflight=1) for the latency
number, or N concurrent sequential clients for throughput. A/B by building the bench twice
(`--features sync-core` flips the cluster onto SyncCore). No fsync, no real network, no UC
integration — this isolates the **pure consensus software choreography**.

**Box:** 4-core dev box. n=50k (latency) / 100k (throughput), 5k warmup discarded.

## Headline

The minimal 3d spike **does not net-beat RaftCore on an in-memory store** — but the
attribution is the actual result, and it is mostly *positive* for Model B:

| variant (single-node, inflight=1) | p50 (µs) | vs RaftCore |
|---|---|---|
| **RaftCore** (async, append inline + fire-and-forget) | **~27** | — |
| **3b.2** (async loop + off-thread durability consumer) | **~53** | +96% |
| **3d** (synchronous loop + off-thread durability) | **~35** | +30% |

| variant (single-node, conc=64) | throughput (op/s) | vs RaftCore |
|---|---|---|
| **RaftCore** | **~1.05M** | — |
| **3b.2** (async loop) | **~586k** | −44% |
| **3d** (sync loop) | **~330–540k** | −50…−68% |

(Robust across 3 reps; the inflight-1 gap survives dropping `--server-workers` to 2, so it
is not an oversubscription artifact. Note the `n=2000` smoke earlier showed SyncCore
*faster* — that was a RaftCore warmup artifact; at n=50k RaftCore settles to ~27µs.)

## What it means

1. **The 3d sync loop delivers its predicted micro-win.** With the *same* off-thread
   durability architecture, the synchronous busy-spin loop is **−34% latency vs the async
   loop** (53→35µs). That is precisely the Model-B thesis isolated: re-polling a completion
   (`block_on`, never-park) beats a futex park (`.await` on a tokio reactor) on the
   consensus→durability→consensus round-trip.

2. **The residual gap to RaftCore is the durability *thread hop*, not the sync loop.**
   RaftCore appends **inline** on the consensus task (no hop) and **fire-and-forget** (the
   `IOFlushed` callback returns completion as a later notification — it never waits). 3b.2
   moved the append onto a consumer thread and the spike currently **waits** for it
   (uniform-await, the Task-1 readability decision). For an *instant* in-memory append, the
   cross-thread ring + wait is pure overhead RaftCore doesn't pay. Off-thread durability
   only pays when the I/O is expensive enough to overlap — which the in-memory harness
   cannot exhibit.

3. **Under saturation the busy-wait is actively harmful.** The throughput axis inverts the
   latency story: at conc=64, 3d falls *below* even 3b.2. Two compounding causes, both
   expected:
   - the `block_on` **busy-wait serializes** the consensus thread — it cannot drain the
     64-client backlog while spinning on one op's completion (RaftCore's fire-and-forget
     append never blocks the loop);
   - the **busy-spin pegs a core** that the (also busy-spin) durability consumer + tokio
     I/O workers need — on a 4-core box this is self-defeating oversubscription.

## Conclusion / next

The spike validated the *foundation* (sync loop, suite 180/0) and now the *measurement*
confirms the decisions-doc prediction with numbers: **the naive whole-loop `block_on`
busy-wait is load-bearing-bad, and the off-thread-durability win is invisible without
expensive overlappable I/O.** The deferred next steps are therefore not polish — they are
required for any net win:

- **Completion-as-notification redesign** (fold in deferred 3b.2 Tasks 2–3 + readability
  gate): make append fire-and-forget again and feed I/O completions back as *later loop
  inputs* so the consensus thread does useful work instead of busy-waiting. This is what
  should turn the −34%-vs-async micro-win into a net-vs-RaftCore win and fix the throughput
  collapse.
- **Core pinning / no oversubscription** for the busy-spin threads (4-core box penalizes
  busy-spin under load; Model A hit the same wall).
- **A real-I/O setting** (UC fsync + QUIC) to actually value off-thread durability — the
  in-memory microbench structurally cannot. Gated on verifying UC's adapters complete
  reactor-free under `block_on`.

The in-memory commit→apply microbench is retained (`commit_latency`) as the cheap
regression/iteration harness for the redesign — the redesign's success criterion is
**3d-redesigned ≥ RaftCore at inflight=1 AND under concurrency** on this same harness.
