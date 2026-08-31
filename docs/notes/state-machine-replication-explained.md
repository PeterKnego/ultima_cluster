# State machine replication, explained

*The one idea `ultima_cluster` (UC) is built on. No prior knowledge of Raft,
Paxos or consensus assumed.*

Already know SMR? The part worth reading is
[What UC adds](#what-uc-adds-to-the-picture).

## The idea in one paragraph

State machine replication is a way to make several machines behave as one
reliable machine.

You start with a **deterministic program**: the same input sequence always
gives the same output and the same internal state. You then run a copy of
that program on several servers. The servers first agree on **one single
ordered list of input commands**, usually with a consensus protocol like Raft
or Paxos. Each server then applies the commands from that list, in the same
order, to its own copy. Because the program is deterministic, all copies stay
identical.

The result is that a server can answer a query from its own copy, and if some
servers fail the others continue with the correct state. **The only thing the
servers must agree on is the order of the inputs** — not the state itself, and
not the output.

![Clients submit commands; the nodes run a consensus protocol to agree one
ordered log; each replica then applies that log independently and they all
reach the same state](../images/smr-overview.png)

## Why that last sentence is the whole trick

It is worth sitting on, because it is where the leverage comes from.

Agreeing on *state* is expensive and awkward. States are large, they differ
in complicated ways, and reconciling two divergent ones means diffing,
merging, and inventing a conflict-resolution policy that is usually wrong in
some corner.

Agreeing on *order* is a much smaller problem. An order is a sequence of
opaque commands. The protocol does not need to know what a command means, how
big the state is, or what the program does with it. So the hard,
general-purpose, formally-verified machinery — Raft, Paxos — is solved once,
in a layer that is completely ignorant of your application. Determinism then
does the rest for free: same start, same inputs, same order ⇒ same state, with
no further communication.

That is why the arrow in the diagram from the log to the replicas has no
arrows coming *back*. After the order is fixed, the replicas never talk to
each other again. They cannot disagree, so they have nothing to say.

What you get out of it:

- **Strong consistency without a distributed transaction protocol.** Ordering
  is the only agreement needed.
- **Trivial failover.** Every replica already holds the complete state, so a
  new leader takes over immediately — there is no state transfer on the
  failure path.
- **A replayable history.** The command log *is* the system of record, which
  makes audit, recovery and rebuild-from-scratch fall out for free.

## The price: `apply` must be deterministic

This is a hard constraint, not a style guide. Inside the function that applies
a command there must be:

- no clock — `SystemTime::now()` differs on every machine;
- no randomness;
- no I/O, no network calls, no ambient configuration;
- no iteration over a `HashMap` whose order is seeded per process;
- nothing that depends on how much memory or how many cores the host has.

Two replicas that disagree by one bit have **silently forked**, and no
consensus layer below them can detect it — it agreed on the order, and the
order was fine. This is the failure mode SMR trades for; everything else it
gives you is downstream of taking it seriously.

If you need a clock or a network call, it does not go in `apply`. Either the
value is decided *before* the command enters the log (so it is an input, and
every replica sees the same one), or the effect happens *after* apply, on one
node only — see [`OutputHandler`](../ARCHITECTURE.md#what-you-implement).

## When to reach for it

The natural fit is **a modest amount of state that must be exactly right,
mutated by a high rate of small commands**: matching engines and order books,
exchange and trading systems, control planes, metadata and configuration
stores, sequencers, coordination services. It is the model behind ZooKeeper,
etcd, Aeron Cluster and the LMAX-style trading architectures.

## When not to

- **Large state.** The whole state lives in memory on every node and must be
  snapshottable. Bulk storage wants a replicated database, not SMR.
- **Nondeterministic work.** If the work genuinely needs a clock, a service
  call, or anything ambient in the middle of applying, the model does not
  hold.
- **Write scaling.** Every node applies every command, and ordering runs
  through one leader. SMR buys consistency and failover, never write
  throughput that scales with node count — adding nodes makes the system
  *more durable*, not faster.
- **Eventual consistency is sufficient.** Then this is a great deal of
  machinery for a guarantee you are not using.

## Where the diagram simplifies

The picture above is the **logical** view, and it is the right one for
understanding the model. Two things it flattens, which matter as soon as you
look at real code:

**"One ordered log" is not one object.** There is no shared log sitting
between the nodes. Every node holds its *own copy* of the log, and the
consensus protocol is precisely the machinery that makes those copies agree on
a common prefix. Drawing it once is what makes the idea legible; expecting to
find it as a thing in the system is what makes the code confusing.

**A replica is not automatically a separate box from a node.** In the diagram
the consensus nodes and the replicas are drawn as two rows to separate the two
*jobs*. Whether they are separate processes is an implementation choice —
in UC they are (see below), in many systems they are not.

## What UC adds to the picture

UC is an SMR application server: you write the deterministic program, it
provides everything else in the diagram. Three ways the real thing differs
from the sketch:

**The two jobs are two processes.** The consensus node (`uc2-node`) and your
state machine (`uc_service`, the "replica") run as separate processes on the
same host, talking over shared memory rather than a network hop. That is what
lets your state machine crash and restart without taking consensus down with
it, and vice versa.

```text
[client process]  ──shmem──▶  [uc_node]  ◀──reliable UDP──▶  [uc_node on peer host]
                                  ▲
                                  │ shmem (file-backed log buffer + cnc page)
                                  ▼
                             [uc_service]   ← your StateMachine lives here
```

**The log is addressed by byte position, not by index.** Commands are not
"entry 4 712"; they are "the frame at byte 3 211 264". That is what allows
replication to be a byte-stream fan-out instead of a per-entry RPC, and it is
the single biggest structural difference from a textbook Raft. See
[Positions, not indices](../ARCHITECTURE.md#positions-not-indices).

**Reads are not free just because every replica has the state.** "Any server
can answer a query" is true of the *model*; a real system still has to say
*which* answer you get. A follower's copy can lag, so UC offers a
**linearizable** read that goes through a quorum barrier (you see everything
acknowledged before your read) and a cheaper snapshot read that does not. The
choice is yours per query, and the barrier is the reason a follower's answer
can be trusted.

**Several state machines per log.** Since 2.8.0 one agreed log can feed up to
eight independent state machines per node — same order, different programs.
See [multi-service](uc2-m14-multi-service-explained.md).

## Where to go next

- [Architecture](../ARCHITECTURE.md) — how UC implements all of the above,
  written for someone who now knows what SMR is.
- [Quickstart](../QUICKSTART.md) — a running three-node cluster in a few
  minutes.
- [Core principles](../../CORE_PRINCIPLES.md) — the correctness, resiliency
  and performance commitments this model is in service of.
- [Verification](../VERIFICATION.md) — how the claim "the replicas cannot
  diverge" is actually checked, rather than asserted.
