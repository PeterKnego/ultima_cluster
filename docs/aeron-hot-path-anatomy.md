# Aeron Cluster hot-path anatomy — how 800k msg/s @ p50 0.4 ms (with fsync) happens

Source-level map of Aeron's per-message hot path (Aeron checkout `be83d5d4de`, Java tree),
produced 2026-07-02 after the parity scorecard measured Aeron Cluster at ≥800k msg/s,
p50 0.38 ms / p99 0.44 ms *with* per-block fdatasync (`file.sync.level=1`), vs UC's ~56k.
Each section ends with the UC contrast. §7 derives the optimization plan skeleton.

TL;DR — three design moves produce the numbers, and none of them is "faster code":

1. **Consensus is a control plane, not a data plane.** Log replication is a raw byte-stream
   fan-out done by the media driver's sender thread (multi-destination-cast of the log term
   buffer). The consensus module never sends entries and never acks batches: followers
   gossip a monotonic *durable position* (coalesced, per duty-cycle, 200 ms heartbeat
   floor), the leader computes commit = majority-ranked position, and gossips that back.
   At 800k msg/s the consensus-control traffic is a few thousand tiny messages/sec.
2. **Batching is structural, never timer-based.** No linger anywhere. Batches form from
   backlog at every stage: the sender's TermScanner packs whatever complete frames exist
   (≤MTU: 14×96 B messages/datagram), the archive recorder block-writes whatever
   accumulated since its last poll (≤1 MiB) and fsyncs once per block, position counters
   update with hysteresis (per-datagram, per-poll, per-window/4). Under load, batch size
   grows automatically; at idle, latency is one poll cadence.
3. **A pipeline of single-writer polling Agents over shared-mmap buffers.** ~8 duty-cycle
   threads per node (driver 3, archive 3, consensus 1, service 1), each a single-writer
   loop; every handoff is a term buffer with release/acquire frame-length framing (exactly
   UC's ring discipline) read in place — zero locks, zero wakeups, zero syscalls on the
   intra-host path, ~3 userland copies end-to-end.

The consensus-module thread touches each message exactly **once** (one `offer` into the log
term buffer). Everything else — replication, durability, apply, egress — happens on other
Agents reading the same shared memory, coordinated only by monotonic position counters.

---

## 1. The messaging data plane (what a 64 B message costs)

Frame = 32 B header + payload, 32-aligned → **96 B** for a 64 B message.

**Publish** (`ConcurrentPublication.offer`, ConcurrentPublication.java:154): one volatile
load of pub-lmt (flow-control gate), one **XADD** on the per-term tail counter (claims a
disjoint range — no CAS loop, no lock), header written with a negative-length release
store, payload `putBytes` (64 B copy), then a release store of the positive frame length —
the commit point that makes the frame visible to every scanner. `tryClaim` is the zero-copy
variant (caller writes directly into the term buffer). `ExclusivePublication` (used for the
cluster log) drops even the XADD — plain store, single writer. **Zero syscalls, zero
allocation per message.**

**Send** (driver sender thread, NetworkPublication.java:826): `TermScanner` walks completed
frames from snd-pos and packs **up to one MTU (1408 B = 14 messages)** into one
`DatagramChannel.write` — one syscall per datagram, snd-pos release-stored once per
datagram. Bound = flow-control window (snd-lmt from receiver Status Messages, sent every
window/4 ≈ 32 KiB, ~2.4k SMs/s at 77 MB/s — not per message).

**Receive** (driver receiver thread): one `receive` syscall per datagram,
`TermRebuilder.insert` copies the datagram into the image term buffer at its
offset-addressed slot (out-of-order safe, duplicate-free), release-stores the frame length,
bumps rcv-hwm once per datagram. Loss → NAK (event-driven) → sender re-scans the term
buffer and retransmits. **Retransmit needs no storage read-back — the term buffer IS the
retransmit buffer.**

**Subscribe** (`Image.poll`, Image.java:340): reads frames **in place** in the shared-mmap
term buffer (zero copy), up to fragmentLimit per poll, sub-pos release-stored **once per
poll**.

**IPC**: publisher and subscriber map the same term buffer; no driver data involvement at
all; nobody wakes anybody — pure polling; the conductor refreshes pub-lmt with window/8
hysteresis.

At 800k×64 B cross-host: 800k XADDs, **~57k send + ~57k recv syscalls** (14:1 MTU packing),
~57k position stores, ~2.4k status messages, 0 NAKs. That's the entire data plane.

**UC contrast:** UC's shmem rings have the same framing discipline (atomic-after-write
length prefix) — this layer was never the gap (task18/aeron investigation: copying refuted,
ring hop ~29 ns busy / ~9 µs futex). What UC lacks is everything below.

## 2. Ingress → log on the leader (the consensus thread's per-message work)

`IngressAdapter.poll` → `ConsensusModuleAgent.onIngressMessage`
(ConsensusModuleAgent.java:783): role/term guard, session hash-lookup, then
`LogPublisher.appendMessage` (LogPublisher.java:115) = **one gather `offer`**
(session-header + payload, single copy) onto the log `ExclusivePublication`. That is the
consensus module's **entire** per-message duty. No quorum math, no RPC, no counters, no
queueing — per-message consensus cost ≈ one term-buffer append.

**UC contrast:** on UC's consensus thread each entry is touched repeatedly: api-batch
channel enqueue → engine command processing → journal append → **replication read-back**
(per-peer streams re-read entries via `limited_get_log_entries` — from the journal or entry
cache) → apply dispatch through `sm::Worker` → per-message oneshot completion. The single
thread does O(5–7) touches per entry; Aeron's does 1.

## 3. Replication = the fabric, acks = position gossip

- The leader's log channel is **MDC (multi-destination-cast)**: one `ExclusivePublication`,
  one destination per follower (ConsensusModuleAgent.java:1595-1636). The **driver's sender
  thread** does the fan-out from the same term buffer the append wrote. No AppendEntries,
  no per-batch request/response, no storage read on the replication path.
- Followers subscribe to the log stream like any subscription; their **archive records it**
  (SourceLocation.REMOTE), and `RecordingPos` — advanced only **after** the block write
  (+fsync at sync.level≥1) — is the follower's durable position.
- **Follower → leader**: `AppendPosition` (tiny SBE message) sent only when the recorded
  position advanced since the last send, floor 200 ms (ConsensusModuleAgent.java:2686,2701)
  — i.e., once per follower duty-cycle batch covering thousands of messages.
- **Leader**: per duty cycle, ranks member positions (majority-th highest,
  ClusterMember.java:867), bounded by its own durable position → `CommitPosition` gossiped
  to members only when it advanced (:2846). Commit therefore waits for a **quorum of
  fsync'd positions** — real Raft durability semantics — at a cost amortized to
  ~duty-cycle rate.

**UC contrast:** openraft replication is per-batch RPC choreography: engine decides a
range → per-peer stream reads entries back from storage → AppendEntries over QUIC →
follower engine ingests → journal append+fsync → ack response → leader engine
update_matching → commit → notify. The measured cost: ~0.52 ms of the 0.70 ms replication
bucket is this choreography, not the wire (floor-decomposition §3b). Aeron's equivalent
control loop runs a handful of times per millisecond *total*, independent of message rate.

## 4. Durability (why fsync is free at 800k)

The archive recorder (dedicated agent) `blockPoll`s the log image: each poll delivers the
contiguous run of complete frames since last time (≤1 MiB, ≤1 term), one positional
`FileChannel.write` from the mapped term buffer (no intermediate copy), then — at
sync.level 1 — **one `force(false)` per block** (RecordingWriter.java:131-140). Segments
are 128 MiB, preallocated with `setLength` at open (one dir-force per ~1.7 s at 77 MB/s).
fsync frequency = block rate, which *falls* as load rises (bigger blocks). Level 0 = same
writes, zero forces.

**UC contrast:** UC's journal group commit + fdatasync is the **same design** (and the
prealloc work matched the segment preallocation). This is why fsync is throughput-free in
both systems (scorecard confirmed). The floors differ (+0.4 ms UC vs +36 µs Aeron) because
UC's *commit path serializes more stages* around the sync, not because the sync strategy
differs. No work needed here.

## 5. Apply and egress

The service is its own Agent (own process/thread) that **polls the committed log
directly**: `BoundedLogAdapter.poll(commitPosition.get())` — bounded by a shared-memory
counter, reading the log in place (leader: a *spy* subscription on its own log publication;
follower: the replicated stream). Apply = the user callback inline on the service thread.
Egress = `session.offer` onto a per-session response publication (followers compute but
don't send). No apply channel, no per-entry handoff, no response oneshot — the *log itself*
is the apply queue and the *position counter* is the only coordination.

**UC contrast:** UC's embedded apply path is: openraft `sm::Worker` task receives an apply
command batch → `AdaptedStateMachine::apply` under a mutex → bincode decode → user apply →
bincode encode → per-message oneshot wake of the submit future. Two async hops + per-message
waker + codec, where Aeron has a poll of shared memory. (Shmem mode adds the service
process rings + output/replay machinery on top.)

## 6. Where the 13×/14× actually lives (measured + mapped)

| mechanism | Aeron | UC today | measured cost |
|---|---|---|---|
| per-message consensus-thread touches | 1 (log offer) | ~5–7 (engine, journal, repl read-back, apply dispatch, oneshot) | UC plateau 54–56k on 1 thread, same on 8/16 vCPU |
| replication transport | driver MDC from term buffer | per-batch RPC + storage read-back | 0.52 ms choreography vs 0.18 ms wire |
| replication ack | coalesced durable-position gossip | per-RPC response through engine | part of the 0.52 ms |
| batching | structural (backlog-formed) | timer linger (2 ms) + group commit | linger 5→2 ms was the single biggest shipped win |
| apply | service polls committed log in place | channel hops + mutex + codecs + oneshot | base bucket 0.86 ms incl. commit→apply ~0.67 ms |
| wakeups | none (polling agents) | futex/task wakes per hop | +0.68 ms floor just from multi_thread scheduling |
| durability | block write+fsync, position after | group commit fdatasync | equivalent — free on both |

## 7. Derived UC optimization plan (skeleton)

Ordered by leverage; each maps to an existing fork/UC asset.

- **P1 — Replication as a data stream + position gossip** (attacks the 0.52 ms choreography
  bucket and the per-entry consensus-thread work). Stream journal bytes to followers as a
  continuous flow (the fork's per-peer stream consumers — SyncCore 3c — already sever
  replication from the core and hold per-peer sessions); replace per-batch ack RPC with a
  monotonic durable-position report; commit = ranked quorum position in the engine. This is
  a semantic change to the openraft fork (position-based protocol between our own nodes),
  not upstreamable — effectively completing the divorce that 3c started.
- **P2 — No storage read-back on replication.** Replicate from the bytes at append time
  (the journal write buffer / the already-built EntryCache as a term-buffer analog), with
  journal read only for lagging followers. The EntryCache (built, merge-ready,
  embedded-effective) is the asset; the change is feeding replication from the append path
  instead of `limited_get_log_entries`.
- **P3 — Kill the timer linger: backlog-formed batching.** Batch = drain-whatever-is-queued
  at every stage (submit ring → engine input; engine → journal already is group commit;
  journal → replication scanner). At low load, latency = poll cadence (µs), not 2 ms; at
  high load, batches grow automatically. Expected: the floor's linger component vanishes
  with no throughput cost — the Aeron mechanism proves the design.
- **P4 — Apply = poll the committed log.** The service/apply stage becomes an independent
  poller of the journal bounded by a commit watermark (embedded: same process; shmem: the
  service already has the journal-replay machinery — make it the *primary* path, with the
  response keyed by log position on the broadcast ring instead of per-message oneshots).
  This is SyncCore 3e's "apply inline" taken to its logical end.
- **P5 — Compose the agent pipeline.** SyncCore already built the shape: sync consensus
  loop + reactor-free durability consumer + per-peer network consumers + disruptor input
  ring, busy-spin executor available. P1–P4 turn those stages into a pure
  counter-coordinated pipeline; then the consensus thread's per-message work approaches
  Aeron's (one staging append), and throughput stops being bounded by one thread doing
  everything.
- **Non-goals** (already at parity): fsync strategy (§4), ring/framing discipline (§1),
  overload robustness (admission control shipped).

Realistic expectations: P3 alone is a floor play (‑2 ms at low load). P1+P2+P4 attack both
the 13× latency (choreography + hops) and the 14× throughput (consensus-thread touches).
None is a knob; all are fork/architecture work with the foundations already validated
(task19, 3c consumers, EntryCache, busy-spin executor, this week's correctness gates).

---

*Method: three parallel source surveys over `/home/claude/ultima/aeron` (data plane;
consensus module; archive + client), key files: ConcurrentPublication/ExclusivePublication,
NetworkPublication/TermScanner/TermRebuilder, ConsensusModuleAgent, LogPublisher/LogAdapter,
ClusterMember.quorumPosition, RecordingWriter/RecordingSession, ClusteredServiceAgent/
BoundedLogAdapter, AeronCluster. Line references as of Aeron `be83d5d4de`. The
aeron-benchmarks LoadTestRig itself is not in that checkout; its echo-service semantics were
confirmed via the in-tree EchoService analog. Numbers quoted from
`aeron-parity-scorecard-2026-07-02.md` and the floor-decomposition/knee-attribution docs.*
