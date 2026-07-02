# Aeron Cluster hot-path anatomy — how 800k msg/s @ p50 0.4 ms (with fsync) happens

A source-level map of Aeron Cluster's per-message hot path, written for a reader who knows
SMR/Raft concepts (log, leader/follower, commit index, apply, quorum) but not Aeron.
Produced 2026-07-02 (Aeron checkout `be83d5d4de`, Java tree) after the parity scorecard
measured Aeron Cluster at ≥800k msg/s, p50 0.38 ms / p99 0.44 ms *with* per-block fdatasync
(`file.sync.level=1`), vs UC's ~56k msg/s. Section 8 derives a UC optimization plan.

**TL;DR — the numbers come from three design moves, none of which is "faster code":**

1. **Consensus is a control plane, not a data plane.** Log entries are never sent by the
   consensus code at all — replication is a raw byte-stream fan-out done by Aeron's
   messaging layer, and followers acknowledge with a coalesced "my durable log position is
   now X" message a few thousand times per second, regardless of message rate. There is no
   AppendEntries, no per-batch ack, and the leader's consensus thread touches each message
   exactly once.
2. **Batching is structural, never timer-based.** There is no linger delay anywhere.
   Batches form automatically from backlog at every stage; under load they grow, at idle
   latency is just a polling loop's reaction time.
3. **The node is a pipeline of single-writer polling threads over shared memory.** ~8
   threads, each looping over its own stage, handing work to the next via shared-memory
   buffers and monotonic position counters. No locks, no wakeups, no syscalls on the
   intra-host path, ~3 copies end to end.

---

## 1. Aeron in five concepts (the primer)

Aeron is a messaging library first; the cluster (SMR) part is built *on top of* the
messaging part, and that layering is the whole story. The pieces:

- **Media driver** — a separate always-running component (own process or embedded) that
  owns all networking. Applications never touch sockets; they exchange data with the
  driver through shared memory. In the default "dedicated" mode the driver runs **three
  threads**: a *conductor* (admin/bookkeeping), a *sender* (drains outgoing buffers to
  UDP), and a *receiver* (reassembles incoming datagrams into buffers).
- **Term buffer** — the core data structure: a large memory-mapped ring of fixed-size
  segments ("terms") that a *Publication* (sender handle) appends framed messages into and
  a *Subscription/Image* (receiver handle) reads from **in place**. A frame becomes visible
  to readers via a release-store of its length word — the same atomic-after-write framing
  discipline UC's shmem rings use. Everything in Aeron — client↔driver, driver↔driver
  recording, cluster log — is a term-buffer stream.
- **Counters** — progress and flow control are communicated exclusively through
  shared-memory position counters (how far the publisher may write, how far the sender has
  sent, how far a subscriber has read...). Nothing signals anything; interested parties
  poll counters.
- **Agents** — every active component is an "Agent": a single-writer duty-cycle loop
  (`doWork()`) run by one thread with a configurable idle strategy (busy-spin at the
  latency-sensitive end). No thread pools, no async runtimes, no tasks.
- **Archive** — a recording service: it *subscribes* to any stream like a normal consumer
  and writes the raw term-buffer bytes to disk files, exposing a "recorded up to position
  X" counter. This is how anything in Aeron becomes durable — including the cluster log.

One node, put together — four components, ~8 polling threads, every handoff through
shared-memory term buffers and position counters (no queues, no wakeups):

```
┌─ MEDIA DRIVER ──┐ ┌─ ARCHIVE ───────┐ ┌─ CONSENSUS MODULE ─┐ ┌─ SERVICE ─────────┐
│  conductor      │ │  conductor      │ │  1 thread:         │ │  1 thread:        │
│  sender    ◀────┼─┼── recorder      │ │   poll ingress,    │ │   poll committed  │
│  receiver       │ │  replayer       │ │   append to log,   │ │   log, apply user │
│  (3 threads)    │ │  (3 threads)    │ │   position gossip, │ │   state machine,  │
│                 │ │                 │ │   commit, election │ │   offer responses │
└───────┬─────────┘ └────────┬────────┘ └─────────┬──────────┘ └─────────┬─────────┘
        │                    │                    │                      │
        ▼                    ▼                    ▼                      ▼
╔═══════════════════════════════════════════════════════════════════════════════════╗
║   shared-memory term buffers (ingress / log / egress) + position counters (mmap)   ║
╚═══════════════════════════════════════════════════════════════════════════════════╝
```

**Rosetta table for the SMR reader:**

| Raft/SMR concept | Aeron Cluster realization |
|---|---|
| the replicated log | one term-buffer *stream* ("the log channel"), leader-published |
| AppendEntries RPC | none — followers simply *subscribe* to the log stream; the media driver fans it out |
| log persistence | each node's Archive records the log stream to segment files |
| follower matchIndex | `AppendPosition`: a tiny control message carrying the follower's *recorded-durable* position, sent coalesced |
| leader commitIndex | `CommitPosition`: leader ranks members' durable positions, takes the majority-th, gossips it back; also exposed as a local counter |
| apply loop | the service is its own Agent that polls the log stream directly, bounded by the commit counter |
| state machine responses | the service offers responses onto per-client egress streams (leader only) |
| election/terms | a separate `Election` state machine on the consensus thread — idle at steady state |

Keep one number in mind for scale: a 64 B message is framed with a 32 B header and
32-byte-aligned → **96 B on the wire and in every buffer**.

## 2. The life of one message (leader, steady state)

Plain-language walkthrough; every step below has a deep-dive section later. First the
**data path** — what happens *per message* (step numbers match the list below; `═▶` is a
UDP hop by the media driver, `─▶` is shared memory on one host):

```
 client              leader                 leader                LEADER LOG
 app     ①          ingress      ②        consensus    ②         term buffer
 thread ────▶ terms ═════▶ terms ────▶ thread (one append) ────▶ ●●●●●●●●●●●
                                                                  │  │  │  │
              ┌───────────────── ③ driver sender fans out ────────┘  │  │  │
              │                    (MDC; NAK retransmits             │  │  │
              ▼                     re-read THIS buffer)             │  │  │
        follower log terms ×N                             ④ archive ─┘  │  │
              │ (each follower runs its own ④ and ⑦)        recorder:   │  │
              ▼                                       block-write ≤1MiB │  │
        follower archive: write+fsync → durable pos    + 1 fsync/block  │  │
                                                       → RecordingPos   │  │
                                          ⑦ service thread ─────────────┘  │
                                            polls log in place,           ...
                                            bounded by commit counter,
                                            applies user state machine
                                                 │ ⑧ response
 client                                          ▼
 app     ◀════ egress terms ◀─────────── response offer (leader only)
 thread   (poll in place)
```

Steps ⑤ (position gossip) and ⑥ (commit) are deliberately absent from this picture —
they are the **control plane**, run per *duty cycle* rather than per message, and are drawn
in §5. Step by step:

1. **Client → cluster.** The client library prepends a small session header and appends the
   message into its *ingress* term buffer (one atomic fetch-and-add to claim space, copy
   the bytes, one release-store to publish — no syscall). The client's media driver packs
   it, with its neighbors, into a UDP datagram to the leader (~14 such messages fit one
   datagram).
2. **Leader ingress.** The leader's driver receiver thread rebuilds the datagram into the
   ingress term buffer. The **consensus module** — a single Agent thread — polls that
   buffer, validates the session, and does its *entire* per-message consensus duty: **one
   append of the message onto the log stream's term buffer.** No quorum math, no RPC, no
   queueing. That's the whole per-message cost on the consensus thread.
3. **Replication happens in the fabric.** The log stream has each follower registered as a
   destination ("multi-destination-cast": one publication, N endpoints). The *driver's
   sender thread* — not consensus code — packs log frames MTU-full and sends them to every
   follower. If a datagram is lost, the follower's driver notices the gap and sends a NAK;
   the sender re-reads the frames *from the same term buffer* and retransmits. The log
   never gets re-read from disk or from the consensus module for replication.
4. **Durability, everywhere, off-thread.** On the leader and on each follower, the local
   Archive subscribes to the log stream and block-writes whatever has accumulated since its
   last poll (up to 1 MiB per write) to a preallocated 128 MiB segment file, then — at
   `file.sync.level=1` — issues **one fdatasync per block**. Only after write(+sync) does
   it advance its "recorded position" counter. fsync frequency scales with *block* rate,
   not message rate: the more load, the bigger the blocks.
5. **Acknowledgement = position gossip.** Each follower's consensus thread watches its own
   recorded-position counter and, once per duty cycle in which it advanced (with a 200 ms
   heartbeat floor), sends the leader one small `AppendPosition` message — implicitly
   acknowledging *every* message up to that durable position. At 800k msg/s this is a few
   thousand control messages per second total.
6. **Commit.** Once per duty cycle, the leader ranks all members' durable positions, takes
   the majority-th highest (bounded by its own durable position) — that's the commit index
   — and, when it advanced, gossips `CommitPosition` to members and bumps a local counter.
   Note the semantics: **commit waits for a quorum of fsync'd positions** — full Raft
   durability — at a cost amortized to the duty-cycle rate.
7. **Apply.** The service (own Agent, own thread — the leader's service reads its own log
   stream via a loopback "spy" subscription) polls the log **in place**, bounded by the
   commit counter, and invokes the user's state-machine callback. No apply queue, no
   handoff: the log *is* the apply queue and the commit counter is the only coordination.
8. **Respond.** The service offers the response onto that client's egress stream (followers
   run the same apply but their offers are no-ops); the driver sends it; the client's
   poller picks it up in place.

Count what scaled with the 800k: term-buffer appends, datagram packing, block writes.
Count what didn't: consensus control traffic (kHz), commit computation (per duty cycle),
fsyncs (per block), flow-control updates (per window fraction). That asymmetry is the
design — a funnel in which every stage coalesces the one above it, with no timer anywhere:

```
 800,000 /s   messages: term-buffer appends (one XADD + release-store each)
    │
    ▼  ÷14        TermScanner packs complete frames MTU-full (1408 B)
  ~57,000 /s  datagrams: one send + one recv syscall, one position store each
    │
    ▼  ÷10…100    archive recorder drains whatever accumulated (≤1 MiB blocks)
  ~600–6,000 /s  file writes — and ONE fdatasync per block (sync.level=1)
    │
    ▼  coalesced per duty cycle / window fraction / 200 ms floor
  ~1,000–3,000 /s  control messages: AppendPosition, CommitPosition, flow-control
```

The deeper the backlog, the *fewer* expensive operations per message. Load makes the
funnel more efficient, which is why the latency curve stays flat to 800k.

## 3. Deep dive: the messaging data plane

*(what one 64 B message costs the messaging layer; classes in `aeron-client`/`aeron-driver`)*

**Publish** (`ConcurrentPublication.offer`, ConcurrentPublication.java:154): one volatile
read of the flow-control limit counter, one atomic fetch-and-add (XADD) on the term's tail
counter to claim 96 B (no CAS loop — concurrent producers get disjoint ranges), header
written with a negative-length release-store ("in progress"), 64 B payload copy, then a
release-store of the positive frame length — the commit point that makes the frame visible
to all readers. A `tryClaim` variant lets the caller write payload directly into the term
buffer (zero copy). The cluster log uses `ExclusivePublication` (single writer) which
replaces even the XADD with a plain store. **Zero syscalls, zero allocation per message.**

**Send** (driver sender thread, NetworkPublication.java:826): a scanner walks completed
frames and packs **up to one MTU (1408 B = 14 × 96 B messages) into one
`DatagramChannel.write`** — one syscall per *datagram*, one position-counter store per
datagram. The send window is bounded by flow control: the receiver reports its progress
via a Status Message every quarter-window (~32 KiB), i.e. ~2.4k times/s at 77 MB/s — not
per message.

**Receive** (driver receiver thread): one `receive` syscall per datagram; frames are copied
into the receiving term buffer at their *position-addressed offset* (so out-of-order and
duplicate datagrams are handled trivially), and published with the same release-store
framing. Loss triggers a NAK; retransmission re-reads the sender's term buffer.

**Consume** (`Image.poll`, Image.java:340): the handler is called with pointers **into the
shared term buffer** (zero copy), up to a fragment limit per poll, and the reader's
position counter is stored **once per poll**, not per message.

**Same-host (IPC)**: publisher and subscriber map the *same* term buffer; the driver's data
path is not involved at all; nobody wakes anybody — pure polling.

At 800k × 64 B cross-host this totals: 800k XADDs, **~57k send and ~57k receive syscalls**
(the 14:1 MTU packing), ~57k position stores, ~2.4k status messages, 0 NAKs.

**UC contrast:** UC's shmem rings use the same atomic-after-write framing — this layer was
never our gap (the 2026-06-21 investigation measured ring hops at ~29 ns busy-spun; copying
refuted). What UC lacks is the layering *above*, next sections.

## 4. Deep dive: ingress → log (the consensus thread)

`IngressAdapter.poll` → `ConsensusModuleAgent.onIngressMessage`
(ConsensusModuleAgent.java:783): check "am I leader, is the term current", one hash-map
session lookup, then `LogPublisher.appendMessage` (LogPublisher.java:115) = **one gather
append** (session header + payload, the one copy on this thread) onto the log stream.
That is the consensus module's entire per-message duty.

**UC contrast:** on UC's consensus thread each entry is touched ~5–7 times: api-batch
channel enqueue → engine command processing → journal append → *replication read-back*
(per-peer streams re-read entries from storage via `limited_get_log_entries`) → apply
dispatch through openraft's state-machine worker → per-message response oneshot. This is
the direct mechanical reason UC plateaus at ~54–56k on one consensus thread (measured
identical on 8 and 16 vCPU) while Aeron's consensus thread shrugs at 800k.

## 5. Deep dive: replication and commit

The two planes, on a 3-node cluster — the data plane carries every message but involves no
consensus code; the control plane involves the consensus threads but carries no messages:

```
 DATA PLANE — per message, driver threads only          (at 800k msg/s: 800k msgs)
 ═══════════════════════════════════════════════════════════════════════════════
                            log stream (MDC fan-out; NAK/retransmit inside)
   leader log terms ═══════════════════▶ follower A log terms ─▶ archive ─▶ fsync
                    ╚══════════════════▶ follower B log terms ─▶ archive ─▶ fsync


 CONTROL PLANE — per duty cycle, consensus threads      (at 800k msg/s: ~kHz total)
 ─────────────────────────────────────────────────────────────────────────────────
   follower A ── AppendPosition(durable pos = X) ──▶ ┌────────┐
   follower B ── AppendPosition(durable pos = Y) ──▶ │ leader │
                                                     └───┬────┘
        commit = majority-th of rank(X, Y, own durable)  │
   follower A ◀───────── CommitPosition(commit) ─────────┤
   follower B ◀───────── CommitPosition(commit) ─────────┘
```

- The log is published once with **MDC — multi-destination-cast** — one publication whose
  frames the *driver's sender thread* transmits to every registered follower endpoint
  (ConsensusModuleAgent.java:1595-1636). Consensus code sends nothing per message.
- Followers are plain subscribers to the log stream; their Archives record it; the
  **recorded position advances only after write(+fsync at sync.level≥1)**
  (RecordingSession.java:236-239) — so it is a *durable* position.
- **Follower → leader**: `AppendPosition` (a fixed ~30 B control message) sent only when
  the recorded position advanced since the last send, floor one per 200 ms
  (ConsensusModuleAgent.java:2686,2701). One message acknowledges everything up to that
  position — the Raft matchIndex, coalesced.
- **Leader**: once per duty cycle, rank member positions and take the majority-th highest
  (ClusterMember.java:867) bounded by the leader's own durable position; if it advanced,
  gossip `CommitPosition` and bump the local commit counter (:2818-2873).

**UC contrast:** openraft replication is per-batch RPC choreography — engine decides a
range, per-peer stream reads the entries back from storage, AppendEntries over QUIC,
follower engine ingests, fsyncs, responds, leader engine updates matching, commits,
notifies. Our replication probe measured ~0.52 ms of the 0.70 ms replication bucket as this
choreography (the wire RTT is only ~0.18 ms). Aeron's equivalent control loop runs a few
thousand times per second *in total*, independent of message rate.

## 6. Deep dive: durability, apply, egress

**Durability**: the Archive recorder (a dedicated Agent) polls the log image for whatever
contiguous complete frames accumulated (≤1 MiB, never crossing a term), does one positional
`FileChannel.write` **directly from the mapped term buffer** (no intermediate copy), then at
sync.level 1 one `force(false)` — an fdatasync — per block (RecordingWriter.java:131-140).
Segments are 128 MiB, preallocated at open. fsync *frequency falls as load rises* (bigger
blocks). **This is the same design as UC's journal group commit + fdatasync + preallocation
— our scorecard confirmed fsync is throughput-free for both systems.** The floors differ
(+0.4 ms UC vs +36 µs Aeron eventual→durable) because UC serializes more *stages* around
the sync, not because the sync strategy differs.

**Apply**: the service Agent polls the committed log **in place**
(`BoundedLogAdapter.poll(commitPosition.get())`, ClusteredServiceAgent.java:262) and calls
the user callback inline. Leader and follower run identical apply; only the leader's
response offers actually send (followers' are mocked). No apply channel, no per-entry
handoff, no completion futures — the log is the queue, the counter is the coordination.

**UC contrast:** UC embedded apply = openraft sm-worker task receives a command batch →
adapter mutex → bincode decode → user apply → bincode encode → per-message oneshot wake of
the awaiting submit future. Two async hops plus a per-message waker where Aeron has a poll.
(Shmem mode adds the service-process rings and the output/replay machinery on top.)

## 7. Where the measured 13× / 14× gap lives

The single-picture version — what the one consensus thread does per message:

```
 Aeron:  poll ingress ─▶ append to log ─▶ done                            (1 touch;
                                                                           everything else
                                                                           happens on other
                                                                           threads, keyed by
                                                                           position counters)

 UC:     api-batch channel ─▶ engine command ─▶ journal append ─▶ replication
         read-back (per-peer streams re-read entries from storage) ─▶ ack
         processing ─▶ commit bookkeeping ─▶ apply dispatch (sm-worker channel)
         ─▶ per-message response oneshot                                  (~5–7 touches,
                                                                           mostly on/through
                                                                           the same thread)
```

| mechanism | Aeron | UC today | our measurement |
|---|---|---|---|
| per-message consensus-thread touches | 1 (log append) | ~5–7 (engine, journal, repl read-back, apply dispatch, oneshot) | UC plateau 54–56k on 1 thread, same on 8/16 vCPU |
| replication transport | driver fan-out from the log buffer | per-batch RPC + storage read-back | 0.52 ms choreography vs 0.18 ms wire |
| replication ack | coalesced durable-position gossip | per-RPC response through the engine | inside the 0.52 ms |
| batching | structural (backlog-formed) | timer linger (2 ms) + group commit | linger 5→2 ms was our single biggest shipped throughput win |
| apply | service polls committed log in place | channel hops + mutex + codecs + oneshot | base bucket 0.86 ms incl. commit→apply ~0.67 ms |
| wakeups | none (polling Agents) | futex/task wakes per hop | +0.68 ms floor just from a multi_thread runtime |
| durability | block write + fsync-per-block | journal group commit | equivalent — throughput-free on both |

## 8. Derived UC optimization plan (skeleton)

Ordered by leverage; each maps to an existing fork/UC asset.

- **P1 — Replication as a data stream + position gossip.** Stream journal bytes to
  followers as a continuous flow; replace per-batch ack RPCs with a monotonic
  durable-position report; commit = ranked quorum position in the engine. The fork's
  SyncCore 3c per-peer stream consumers already severed replication from the core — this
  completes that divorce with a position-based protocol between our own nodes (fork-only,
  not upstreamable). Attacks the 0.52 ms choreography bucket *and* the per-entry
  consensus-thread work.
- **P2 — No storage read-back on replication.** Feed replication from the bytes at append
  time (the already-built EntryCache is our term-buffer analog), with journal reads only
  for lagging followers.
- **P3 — Kill the timer linger: backlog-formed batching.** Batch = drain-whatever-is-queued
  at every stage (submit ring → engine input; engine → journal is already group commit;
  journal → replication scanner). Low load: latency = poll cadence, not 2 ms. High load:
  batches grow automatically. Aeron is the existence proof that this loses nothing.
- **P4 — Apply = poll the committed log.** The apply stage becomes an independent poller of
  the journal bounded by a commit watermark, with responses keyed by log position on the
  broadcast ring instead of per-message oneshots — SyncCore 3e's "apply inline" taken to
  its logical end.
- **P5 — Compose the agent pipeline.** SyncCore already built the shape (sync consensus
  loop, reactor-free durability consumer, per-peer network consumers, disruptor input ring,
  busy-spin executor). P1–P4 turn those stages into a pure counter-coordinated pipeline; the
  consensus thread's per-message work then approaches Aeron's single staging append.
- **Non-goals** (already at parity): fsync strategy (§6), ring/framing discipline (§3),
  overload robustness (admission control, purge slack, wedge fix — shipped this week).

Realistic expectations: P3 alone is a latency-floor play (removes the linger term). P1+P2+P4
attack both the ~13× latency gap (choreography + hops) and the ~14× throughput gap
(consensus-thread touches). None is a knob; all are fork/architecture work with foundations
already validated (task19 SyncCore, 3c consumers, EntryCache, busy-spin executor, this
week's correctness gates).

---

*Method: three parallel source surveys over `/home/claude/ultima/aeron` (data plane;
consensus module; archive + client). Key classes: ConcurrentPublication /
ExclusivePublication, NetworkPublication / TermScanner / TermRebuilder,
ConsensusModuleAgent, LogPublisher / LogAdapter, ClusterMember.quorumPosition,
RecordingWriter / RecordingSession, ClusteredServiceAgent / BoundedLogAdapter, AeronCluster.
Line references as of Aeron `be83d5d4de`. The aeron-benchmarks LoadTestRig is not in that
checkout; its echo-service semantics were confirmed via the in-tree EchoService analog.
Numbers quoted from `benchmarks/aeron-parity-scorecard-2026-07-02.md` and the
floor-decomposition / knee-attribution docs.*
