# Architecture

How `ultima_cluster` works, for someone who knows roughly what Raft is and has
not read the design specs.

The canonical specs are in [`docs/superpowers/specs/`](/docs/superpowers/specs), and
they are the authority where this document and they disagree. But they are dated
design records written for an implementer mid-build — this document is the way in.

---

## What it is

`ultima_cluster` is a **State Machine Replication application server**. You write
a deterministic state machine; it runs your state machine on every node in a
cluster, applying the same commands in the same order, and survives node failure
without losing acknowledged writes.

That is the same job Raft does. What differs is the shape.

## Background: state machine replication

*Skip to [The one idea](#the-one-idea) if you already know SMR.*

The model ([Wikipedia](https://en.wikipedia.org/wiki/State_machine_replication))
is simple to state: instead of replicating *data*, replicate *the sequence of
commands*. Every replica starts in the same state and applies the same commands
in the same order, so every replica ends in the same state — no diffing, no
merging, no conflict resolution.

That reduces fault tolerance to a single problem: **agreeing on the order.** That
is what consensus protocols like Raft and Paxos do, and it is all they do.
Determinism does the rest.

What you get from it:

- **Strong consistency without a distributed transaction protocol.** Ordering is
  the only agreement needed.
- **Trivial failover.** Every replica already holds the complete state, so a new
  leader takes over immediately — there is no state transfer on the failure path.
- **A replayable history.** The command log *is* the system of record, which makes
  audit, recovery, and rebuild-from-scratch fall out for free.

The price is a hard constraint: **`apply` must be deterministic.** No clocks, no
random numbers, no map-iteration order, no I/O, no ambient state. Two replicas
that disagree by one bit have silently forked, and no consensus layer can detect
it for you.

### When you'd reach for it

The natural fit is a modest amount of state that must be *exactly* right, mutated
by a high rate of small commands: matching engines and order books, exchange and
trading systems, control planes, metadata and configuration stores, sequencers,
coordination services. It is the model behind ZooKeeper, etcd, Aeron Cluster, and
the LMAX-style trading architectures.

### When you would not

- **Large state.** The whole state lives in memory on every node and must be
  snapshottable. Bulk storage wants a replicated database, not SMR.
- **Nondeterministic work.** If `apply` needs to call a service, read a clock, or
  consult anything ambient, the model does not hold. Push that work to an
  `OutputHandler` (leader-only, at-least-once) or out of the system entirely.
- **Write scaling.** Every node applies every command, and ordering runs through
  one leader. SMR buys consistency and failover, never write throughput that
  scales with node count — adding nodes makes it *more* durable, not faster.
- **Eventual consistency is sufficient.** Then this is a great deal of machinery
  for a guarantee you are not using.

## The one idea

In a textbook Raft implementation, consensus sits *in the data path*. Every entry
flows through the consensus module: it is appended, wrapped in an `AppendEntries`
RPC, sent, acknowledged, matched against `nextIndex`/`matchIndex`, committed. The
consensus thread touches each entry five to seven times, and that is the ceiling —
the throughput plateau is identical on 8 and 16 vCPUs, because the bottleneck is
one thread doing per-message work.

`ultima_cluster` inverts this, following Aeron's design:

> **Replication is a byte-stream fan-out of the log itself. Consensus is a control
> plane that runs at single-digit kHz regardless of message rate.**

The log is a shared-memory ring buffer. The sender agent scans it and blasts
MTU-sized datagrams at the followers — it does not know or care what a "message"
is; it is streaming bytes at their absolute offsets. Followers write those bytes
into the same offsets in their own buffer. Acknowledgement is not per-entry: each
follower periodically gossips *the position it has made durable*, and the leader
takes the majority-th highest of those positions as the commit point.

The consensus thread touches each message exactly once, on append. Everything
after that is byte plumbing.

This is where the measured numbers come from: **1.64 M responses/s at p50 0.600 ms**
end-to-end through the SDK, with fsync on and linearizable reads, on a 3-host
`c6id.2xlarge` fleet. The predecessor built on `openraft` capped around 56 k/s on
matched hardware — a gap that three weeks of systematic elimination showed to be
architectural rather than tuning (~73% of the commit floor was async choreography
and IPC; only ~27% was physical fsync and wire time).

## Positions, not indices

The single most important consequence: **there is no log index.** There are
absolute `u64` byte positions, monotonic forever.

An entry is not "index 4,712." It is "the frame at byte 3,211,264." This is what
makes the byte-stream fan-out possible — a datagram is *self-locating*, carrying
the stream position of its first byte, so it can be written straight into the
right offset without consulting any per-entry state. Retransmission is just
re-reading the buffer at a position. Recovery is a binary search over journal
block base positions.

The Raft concepts survive, re-expressed:

| Raft | Here |
|---|---|
| Log index | Absolute byte position |
| `matchIndex` per follower | Gossiped durable position per follower |
| Commit index | Majority-th highest reported durable position, clamped to the leader's own |
| Term | Leadership term = `(term_id, base_position)` |
| `RecordingLog` / log metadata | The **term map** — an fsync'd record of term history |

## Process shape

```
[client process]  ──shmem──▶  [uc2_node]  ◀──reliable UDP──▶  [uc2_node on peer host]
                                  ▲
                                  │ shmem (file-backed log buffer + cnc page)
                                  ▼
                             [uc2_service]   ← your StateMachine lives here
```

Three process roles share an instance directory. The separation costs nothing,
because the service does not receive entries — it **polls the shared log buffer
in place**. There is no apply ring and no per-entry handoff across the process
boundary.

Running the service as a thread inside the node process is a configuration flag,
not a different architecture: coordination is entirely counters in shared memory,
so it is the same code with a different `mmap`.

## The node: four single-writer polling agents

Plain `std::thread`s with a configurable idle strategy. All coordination happens
through position counters in an mmap'd 4 KiB **`cnc.dat` page** — no channels, no
locks, no wakeups on the hot path.

| Agent | Job |
|---|---|
| **consensus** | The only "brain." Polls the client ingress ring, validates the session, performs **one append** per message. Drains control messages. Once per duty cycle, ranks reported durable positions and advances commit. Runs the election state machine |
| **sender** | Scans the log buffer from `sent_position`, packs complete frames MTU-full, fans the identical datagram out to every follower (one scan, N sends, `sendmmsg`). Serves NAK retransmits by re-reading the buffer |
| **receiver** | Receives datagrams. Log frames are written at their position offset; control frames go to consensus over an SPSC ring |
| **archive** | Polls the buffer from `durable_position`, block-writes ≤1 MiB to `ultima_journal`, one `fdatasync` per block, then advances the durable counter. **The only fsync site in the system** |

Each counter has exactly one writer, which is what lets the fast path use plain
stores plus a single release-store commit word. Each also sits on its own
64-byte stride within the page: four agents storing to four counters on one
cache line would contend through the coherence protocol even though they never
touch the same field, so the padding buys back the independence the
single-writer rule is there to provide.

## The log buffer and durability

**The buffer** is one mmap'd power-of-2 ring per node (default ~512 MiB) in the
instance directory; the byte offset is `position & (size − 1)`. Exactly one writer
per node depending on role — the consensus agent appends when leader, the receiver
writes frames at their offsets when follower. Because writes are position-addressed,
duplicate and reordered datagrams are idempotent by construction.

**Frames** carry a 32-byte header — length (the atomic-after-write commit word),
type/flags, `leadership_term_id`, `session_id`, `correlation_id` — followed by a
32-byte-aligned payload. Padding frames absorb the wrap, so no frame straddles the
buffer end.

**The archive** records *blocks, not messages*: one journal record per ≤1 MiB
frame-aligned block, one CRC per block, one `fdatasync` per block. The archive
agent's poll batching **is** the group commit — batching is structural, formed
from backlog at every stage, never timer-based. There is no linger anywhere in the
system.

**Commit means quorum-fsync'd.** A position is committed when a majority of nodes
have recorded it durably, not merely received it.

**The overrun rule — one hard gate, everything else degrades.** The appender may
never overwrite bytes the archive has not yet recorded. That is the single hard
backpressure point, surfaced at the ingress door as admission control. Every other
lagging reader degrades gracefully: a follower NAKing below the buffer tail is
upgraded to a journal replay session; a lagging service switches to journal replay
and rejoins the live buffer when caught up. **The ring is a fast-path cache over
the journal, never a correctness dependency for readers.**

## Replication data plane

**Send.** Frames are packed MTU-full (1408 B default) and the *identical* datagram
goes to every follower. Datagrams are self-locating: the header carries the stream
position of the first byte plus the `leadership_term_id`. When idle, low-rate
heartbeats carry the append position.

**Receive.** Frames land at `position & mask`. A stale `leadership_term_id` is
dropped on an exact header match. The archive records only the contiguous prefix,
so a hole in the stream can never fool durability.

**Loss → NAK.** A gap that persists past a short randomized delay (~1 RTT)
triggers `NAK(position, length)`; the sender retransmits by re-reading the log
buffer. **The log buffer is the retransmit buffer** — there is no separate
retransmission queue.

**Flow control is quorum-paced**, deliberately not min-paced. Followers advertise
their contiguous-rebuilt position and receive window; the sender's limit is the
**quorum-th order statistic** over those windows. A slow follower therefore never
stalls a commit the quorum could legally advance — it recovers by NAK or is
demoted to a replay session.

**Replay sessions — one mechanism, three uses.** A bounded, separately paced
journal-read stream with its own session id, used for (a) a follower that has
fallen below the buffer, (b) a learner or new node joining, (c) post-election
catch-up. It hands off to the live stream once within buffer range, and it is the
only replication path that reads storage — steady state never touches the disk for
replication.

The control plane rides the same UDP socket: `AppendPosition`, `CommitPosition`,
`RequestVote`/`Vote`, NAK, and status, as fixed-size little-endian frames demuxed
by the receiver.

## Control plane

**Steady state is two message types, per duty cycle, never per message.** A
follower sends `AppendPosition(term_id, durable_pos)` when its durable position
advances (with a 100 ms heartbeat floor). Once per duty cycle the leader computes
commit as the majority-th highest of the reported durable positions, bounded by its
own, stores it to the cnc commit counter — **that store is the apply notification** —
and gossips `CommitPosition`. Followers apply up to `min(commit, local contiguous
durable)`.

**Elections are Raft's safety core, expressed over positions**, and live entirely
inside `uc2_consensus` as a pure, synchronous, deterministic state machine: no I/O,
no threads, no clock. Time is injected. The agent performs the I/O; the state
machine only emits actions.

- A vote is granted iff the term is new, no conflicting vote exists for that term
  (the vote is persisted to a `StableValue` *before* answering), and the
  candidate's `(last_term, durable_position)` is lexicographically at least ours.
  Only durable positions count — a crash discards the non-durable tail anyway.
- A new leader sets `base_position` to its **own durable position**, discarding any
  local bytes beyond it, then appends a **NewTerm no-op frame and waits for it to
  commit before serving anything** (Raft §5.4.2). *The commit advance itself is
  clamped to the NewTerm frame's position* — see Finding #6b in
  [`VERIFICATION.md`](/docs/VERIFICATION.md) for why that clamp exists and what happened
  before it did.
- Reconciliation is follower-side: the leader ships its term map, a diverged
  follower truncates to the last common `(term, base)` prefix — only ever
  uncommitted bytes, by vote and commit safety — then catches up by NAK or replay.

**Linearizable reads** are a ReadIndex analog: capture the commit position C,
confirm leadership with a nonce'd heartbeat round, wait for `service_applied ≥ C`,
run the query, then validate with the service seqlock (epoch unchanged across the
query). That last check closes a TOCTOU against the service crashing mid-query.
Snapshot-consistency reads skip the barrier entirely.

## The apply path and the SDK

**Apply is the service polling the log.** The service mmaps the buffer read-only
plus the cnc page, and runs:

```
while service_applied < min(commit, contiguous_durable):
    read frame → user's apply(position, cmd) → advance counter
```

There is exactly **one deliberate copy**, at the apply boundary: the payload is
copied out of the mapped ring into `Bytes` and then validated against the append
position seqlock-style, re-reading on wrap-over. Borrowed views into an
overwritable ring would be unsound, and at KV payload sizes the copy measured as
noise.

**Responses bypass the node.** The client stamps `(session_id, correlation_id)`;
consensus carries them in the frame header; the service echoes them plus the
position directly onto an egress broadcast ring. There are no per-message
oneshots — the client matcher correlates off the ring.

### What you implement

- **`StateMachine`** — synchronous, deterministic `apply(position, cmd)` and
  `query`. No I/O, no clock, no ambient state. `AppCommand` is `Bytes`; reads are
  typed rather than closures. `StateMachine` is one of two tiers a service can
  implement; see
  [the state-machine contract reference](reference/state-machine-contract.md)
  for the raw bytes-in/bytes-out tier underneath it and when to reach for it
  directly.
- **`SnapshotStateMachine`** *(optional)* — enables journal purge. A node below the
  purge floor (crashed service, fresh learner, cold start) converges by snapshot
  install plus tail replay, never by reading a purged prefix.
- **`OutputHandler`** *(optional)* — async, leader-only side effects, at-least-once,
  with the position as idempotency key. The service advances an `output_completed`
  counter, the node persists it periodically, and a leadership transition replays
  `(marker, commit]`. Monotonic persistence can only widen the replay, so
  at-least-once holds.

`uc2_service` ships an optional adapter for
[`ultima-db`](https://crates.io/crates/ultima-db) behind the `ultima_db` feature,
if you want a transactional store as your state machine rather than hand-rolling
one.

## Membership

Single-server reconfiguration (M7): promote, demote, add, or remove **one** member
at a time, live, under load, via the `uc2ctl` admin tool. Joint consensus is not
needed for these operations — adjacent configurations differ by one member, so
majorities always intersect.

**Hard cap: 8 total members** (voters plus learners), bounded by the cnc
observability band. One node per instance directory.

## Wire security

Optional authenticated and encrypted node-to-node UDP (M8), **off by default**. A
cluster runs either all-encrypted or all-cleartext — flag day, no mixed mode.

Each node holds an X25519 static keypair, with peers authorized by an allowlist
re-read at runtime (so adding a node needs no restart). Noise `IK` establishes
per-peer pairwise keys; a rotating cluster group key seals the byte-identical
fan-out traffic, so the leader seals once and sends N times. The 16-byte datagram
header stays cleartext and is authenticated as AES-256-GCM associated data;
overhead is 24 bytes per datagram.

Threat model: a network-path adversary. **Explicitly out of model:** a compromised
host, and a malicious cluster member — the group key is symmetric, so any holder
can forge fan-out traffic as any node. See runbook §11.

## Crates

| Crate | Role |
|---|---|
| `uc_protocol` | Wire spec — cnc page layout, datagram/frame formats, lock-free rings. `no_std`; the multi-language gate |
| `uc2_log` | Log buffer runtime and archive agent — journal recording, snapshots, purge floor |
| `uc2_net` | Reliable-UDP sender/receiver agents, NAK repair, flow control, snapshot sessions, fault injection |
| `uc2_consensus` | Pure-sync safety core — commit tracking, elections, term maps, truncation. No I/O, no threads, no clock |
| `uc2_sim` | Deterministic simulation, invariants, fuzz |
| `uc2_node` | Composition — agent wiring, IPC surface, read barrier, gate harnesses |
| `uc2_service` | Service SDK — `StateMachine` traits, apply agent, reconstruction |
| `uc2_client` | Sync client SDK — submit, linearizable and snapshot queries, response matcher |
| `uc-lincheck` | WGL linearizability checker, history recorder, register model |
| `ultima_journal` | Segmented append journal and atomic `StableValue`s |

## Design lineage

The predecessor (`uc_node`, built on `openraft`) was correct and gate-green, and
structurally capped roughly 13–14× behind Aeron Cluster on matched hardware and
durability. The decision to rebuild rather than tune rested on measurement, not
taste; the retired v1 record is kept in [`docs/tasks/`](/docs/tasks) as a
negative-results archive.

Three moves were adopted wholesale from Aeron's design — "port the design, not the
code":

1. Consensus is a control plane; replication is a byte-stream fan-out.
2. Batching is structural, formed from backlog at every stage — never timer-based.
3. The node is a pipeline of single-writer polling agents coordinated exclusively
   by shared-memory position counters.

## Where to go next

- **[`VERIFICATION.md`](/docs/VERIFICATION.md)** — what is proved, what is checked, what
  is only bug-hunted, and how to reproduce each.
- **[`docs/ops/uc2-runbook.md`](/docs/ops/uc2-runbook.md)** — instance directory layout,
  durability requirements, decoding the cnc page, enabling purge, live
  reconfiguration, wire crypto setup.
- **[`docs/benchmarks/`](/docs/benchmarks)** — every milestone gate record, each with
  its pass/fail rule committed before the run.
- **[`docs/superpowers/specs/`](/docs/superpowers/specs)** — the canonical design specs.
