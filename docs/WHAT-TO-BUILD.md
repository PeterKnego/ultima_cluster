# What you can build with ultima_cluster

`ultima_cluster` (UC) is a state machine replication (SMR) server. UC runs one
deterministic program, a finite state machine (FSM), on each node of a cluster.
Each node applies the same commands in the same order, so each node holds the
same state. If the leader fails, a new leader takes its place, and the cluster
loses no committed write. This page is a catalog of applications for SMR. For
each application, the page tells you whether UC supports it now.

[State machine replication, explained](notes/state-machine-replication-explained.md)
tells you what SMR is. This page does not repeat it.

## The fit test

If a problem has these three properties, it fits SMR:

1. **Only the commands decide the state.** The same commands in the same
   order always make the same state. Nothing else is allowed to change the state:
   not a host clock, a random value, an input or output operation (I/O) or a
   direct write.
2. **The order of the commands changes the result.** Two buyers for the last
   seat get different results in a different order. Thus the nodes need one
   agreed order. You do not make this order: UC puts each command into one
   committed sequence.
3. **Each node can hold the whole state, make a snapshot of it and send it.**
   Each node keeps a full copy of the state. A node that joins or recovers gets
   the state as a snapshot. Thus you must be able to make a snapshot of the
   state and send it in an acceptable time.

   For the performance that is usually the reason to use SMR, the state must
   also fit in memory. The state can be on disk instead, in the own store of
   the FSM or in memory that the operating system (OS) pages out. This costs
   latency, mostly on reads. A write can stay in memory and go to disk later,
   because the log already holds it durably. A read that misses memory waits
   for the disk.

Under these conditions, you get strong consistency and fast failover. The
logic is also simple to reason about: it runs on one thread and uses no locks.

Most applications that fit have one of two shapes:

- **A small, critical core that many requests compete for.** Examples are a
  limit, a claim, a balance and a lease.
- **A stream where one canonical order makes all downstream work simpler.**

If a problem has neither shape, SMR costs more than it gives.

## How to read a verdict

Each entry ends with one of three verdicts:

- ✅ **Fits today.** UC supplies all that the core of the application needs.
- 🔧 **Fits, with work that you supply.** UC supplies the core. The entry
  names the part that you build around it.
- ❌ **Not a fit.** A stated limit of UC prevents it. The entry names the
  limit.

A verdict is about UC as it ships now, not about SMR in general.
[Limits](reference/limits.md) contains the limits that the verdicts refer to.

## Core applications

### 1. Matching engines and exchanges

This is the classic application. LMAX and Aeron Cluster use this design.
The order of events is critical, the logic is deterministic, and the order book
fits in memory.

**On UC:** the order book is one FSM. Risk and audit can be sibling FSMs on the
same log (see the next entry). Fills go out through the output handler. Snapshot
reads give the current state of the book. [`examples/kv`](../examples/kv) is the
worked example of the shape of an FSM.
**Verdict:** ✅ fits today.

### 2. Risk, margin and liquidation engines

The position and margin state must agree with the stream of fills.

**On UC:** run risk as a second FSM on the log of the matching engine. The two
FSMs apply the same commands in the same order, so you have nothing to
reconcile. The lag bound keeps risk within a known distance of the matching
engine.
**Verdict:** ✅ fits today.

### 3. Ledgers and balance systems

Payments, wallets and double-entry accounting must never spend the same money
two times. A total order of debits and credits prevents this.

**On UC:** one FSM holds the balances. `Sessioned` makes sure that a transfer
that a client sends again applies only once. Holds expire on log time and
timers. Diff replay rebuilds any past state for a dispute.
**Verdict:** ✅ fits today.

### 4. Coordination services

These services supply locks, leader election, service discovery and
configuration. Examples are ZooKeeper, etcd, Chubby and Consul. The state is
small, and correctness is much more important than throughput.

**On UC:** leases and lock timeouts run on log time. A linearizable read tells
you which client holds a lock now. The output handler sends notifications. UC
does not supply a general client application programming interface (API) with
watches, sessions and many languages. The client software development kit
(SDK) of UC is for Rust. Other languages use the
[remote protocol](reference/remote-protocol.md), which has a specification for
new implementations.
**Verdict:** 🔧 fits, with work that you supply: the client API.

### 5. Metadata and control planes

Examples are the Kafka KRaft controller, the Ceph monitors, the Kubernetes
control plane (through etcd) and HDFS NameNode high availability. The data
plane scales out. A small replicated core holds the authoritative metadata.

**On UC:** the metadata is one FSM. UC uses this design itself: an internal
[cluster FSM](notes/uc2-cluster-fsm-explained.md) holds the membership,
schedules and settings of UC.
**Verdict:** ✅ fits today.

### 6. Sharded replicated databases

Each shard is its own consensus group, as in CockroachDB, TiKV, Spanner and
YugabyteDB. Replication in each partition gives linearizable writes. Shards
give scale.

**On UC:** one cluster is one log, so one shard is one UC cluster. UC does not
supply the route of requests to shards, the rebalance of shards or
transactions across shards.
**Verdict:** 🔧 fits, with work that you supply: the shard layer.

### 7. Sequencers

Global ID generation, total-order broadcast and the sequencer pattern. Other
services use the sequencer as their source of truth.

**On UC:** the log is a sequencer. `IdGen` makes IDs without coordination.
Sibling FSMs or the output handler send the ordered stream to consumers.
**Verdict:** ✅ fits today.

### 8. Reservation and inventory systems

Examples are ticket sales, seat reservations, auctions and the rate control of
an advertisement budget. Many requests compete for a limited resource. Each
node must resolve them the same way.

**On UC:** the inventory is one FSM. A hold expires on a timer. An auction
closes at a deadline in log time, so each node closes it at the same bid.
**Verdict:** ✅ fits today.

### 9. Workflow engines and durable job schedulers

Temporal keeps each shard of workflow history as a replicated FSM, in effect.
This gives exactly-once state transitions and clean recovery.

**On UC:** the workflow state and its timers are in one FSM. The schedule table
supplies recurrent jobs. The activities are the calls to the outside world, and
they run through the output handler. The output handler is at-least-once, so
each activity must be idempotent.
**Verdict:** 🔧 fits, with work that you supply: idempotent activities and the
worker protocol.

### 10. Strongly consistent quotas and rate limiters

Use SMR when an approximate count is not acceptable, for example a limit that
affects billing.

**On UC:** the counter and its window are in one FSM. The window moves forward
on log time.
**Verdict:** ✅ fits today.

### 11. Authoritative game simulation

Deterministic lockstep and server-authoritative worlds. A replay of the input
log builds the world again.

**On UC:** the world is one FSM, and a timer on log time advances it one tick
at a time. Diff replay is the replay tool.
**Verdict:** ✅ fits today. The input of each tick must fit the payload
ceiling (see below).

### 12. Byzantine fault tolerance (BFT) and blockchain systems

Tendermint and HotStuff use SMR with a Byzantine fault model. This is the same
idea against a stronger adversary.

**On UC:** UC tolerates nodes that crash and nodes that a partition isolates.
It does not tolerate nodes that lie. A malicious cluster member is
[out of the threat model of UC](security/threat-model.md#5-out-of-model).
**Verdict:** ❌ not a fit.

### 13. Caches with strict updates

When the local node can answer reads, a replicated cache is a strong design.

**On UC:** the local node answers a snapshot read without a consensus round.
The cost is the same as a lookup in the memory of a plain cache. Only writes
and invalidations go through the log, so each node evicts in the same order.
If a stale answer is not acceptable, use a linearizable read.
**Verdict:** ✅ fits today.

## By domain

### Trading and finance

- **Pre-trade risk gateways.** Credit, position and keystroke-error limits must
  agree across all sessions of one account. SMR makes the check and the update
  into one step. ✅
- **Financial Information eXchange (FIX) and order gateways with hot
  failover.** Replicate the session state and the sequence numbers. Then a
  standby can take over without gaps, duplicate orders or a large burst of
  resent messages. You supply the FIX session layer. 🔧
- **Price index and oracle services.** A sequenced service gets feeds and
  publishes a median or mark price. Thus each consumer sees the same price at
  the same log position. Feed values enter as commands, never as reads of
  outside data. ✅
- **Market data ticker plants.** One total order for all feeds, and a
  replicated last-value cache. The snapshots of this cache agree exactly with
  the incremental stream. You supply high-rate multicast fan-out to many
  consumers. 🔧
- **Clearing, netting and settlement.** The results must be identical and
  auditable. Diff replay rebuilds any past state for a dispute. ✅
- **Telecom online charging and prepaid balances.** This is a ledger with
  holds, at high rates. Holds expire on log time. ✅
- **Sports betting.** The odds, the liability limit of each market and the
  acceptance of bets need one order. When odds change under load, this order
  is most important. ✅

### On-chain and crypto

- **Order-book chains** (Hyperliquid, dYdX v4). These chains run a matching
  engine as a Byzantine replicated FSM. ❌ UC has no Byzantine fault model.
- **Rollup sequencers.** A sequencer with a single operator orders transactions
  before settlement on the layer-1 chain. This fits ✅. A decentralized
  sequencer, where the sequencers do not trust each other, does not fit ❌.
- **Bridges and custody coordination.** A set of signers must agree which
  withdrawals to release, and one signer can be dishonest. ❌ This is out of
  the threat model.

### Infrastructure and platforms

- **Message queues and stream logs** (RabbitMQ quorum queues, NATS JetStream,
  Redpanda). The order and the acknowledgement state fit. UC does not supply
  message bodies above the payload ceiling or a log for each partition. 🔧
- **Timestamp oracles** (the Placement Driver of TiDB). A replicated service
  gives timestamps in one global order. Log time and `IdGen` supply this. ✅
- **Secrets and public key infrastructure (PKI)** (Vault integrated storage).
  Versions, leases and revocations must never diverge. Leases run on log time.
  UC encrypts the link between nodes only if
  [wire crypto](how-to/encrypt-node-traffic.md) is on. The client link is plain
  Transmission Control Protocol (TCP). 🔧
- **Cluster schedulers** (Nomad). Two leaders must never place the same work.
  The placement is one FSM, and the launch goes out through the output
  handler. ✅
- **Cluster state for search engines and databases** (Elasticsearch, InfluxDB
  meta nodes). ✅ for the metadata. You supply the data plane.
- **Schema registries and feature-flag stores.** No consumer can see version
  N+1 before version N. The order of the log gives this guarantee. ✅
- **Distributed transaction coordinators.** A replicated log of two-phase
  commit (2PC) decisions removes the case where a coordinator blocks. ✅ for
  the decision log. You supply the participant protocol.

### Application backends

- **Uniqueness constraints** for user names, email addresses, domains and
  license keys. Each is a small claim that many requests compete for, with
  exactly one winner. ✅
- **Session and token revocation.** "Revoke all sessions" takes effect in the
  same order on each node. A linearizable read refuses the old token. ✅
- **Server-authoritative collaborative editing.** One ordered log of
  operations for each document, as an alternative to conflict-free replicated
  data types (CRDTs). Each operation must fit the payload ceiling. Many
  documents need many documents in one log, or many clusters. 🔧
- **The order of chat messages in each channel.** A canonical message order
  and exact read markers. The channels share the one log of a cluster. Above
  the rate of one cluster, you must use shards. 🔧
- **Dispatch and assignment** of drivers, couriers and tickets. One decision
  for each resource prevents double assignment. ✅
- **Matchmaking and lobbies.** Small queues that many requests compete for, and
  that must survive a server failure. ✅
- **The write side of event sourcing.** Command query responsibility
  segregation (CQRS) divides a system into a command side and a query side. The
  command side is a replicated FSM by definition. Read
  models are sibling FSMs or consumers of the output handler. ✅

### Safety-critical and embedded systems

SMR comes from this domain: Software Implemented Fault Tolerance (SIFT) and the
work that led to the Byzantine generals paper. But UC is a Linux server
product. It needs a [multi-core Linux host](how-to/size-a-host.md). It claims no
real-time bound and no safety certification. It has no Byzantine fault model.

- **Flight control and avionics.** ❌
- **Railway interlocking and industrial control.** ❌
- **Spacecraft computers** (triple modular redundancy with votes). ❌

## Poor fits

For all SMR systems:

- **No order between items.** Independent work that runs in parallel gets
  nothing from a consensus round.
- **Very large state.** Each node holds all of the state. A new or recovered
  node gets it as a snapshot. If you cannot make a snapshot of the state and
  send it in an acceptable time, it does not fit, wherever the state is. State
  that you can send but that is larger than memory fits at a cost. Each read
  that misses memory waits for the disk, and `apply` runs on one thread.
- **Nondeterministic logic.** External calls and random values break
  replication. Move them to the edge of the system and log their *results* as
  commands. For UC, a clock is not on this list: use log time.
- **Disposable caches that are cheap to rebuild.** If order and warm failover
  are not important, cache-aside is simpler.
- **Wide-area replication on a path where latency is critical.** Each write
  pays a quorum round trip across the distance.
- **Write scale.** More nodes make a cluster more durable, but not faster.

The hard limits of UC, all in [Limits](reference/limits.md):

| Limit | Result |
|---|---|
| **8 members** in a cluster, **8 FSMs** on a log | Above these numbers, use more clusters. |
| **One log** in each cluster | UC has no built-in shards. A shard is a cluster, and you supply the work across shards. |
| **Command payload ≤ 1344 bytes** (≤ 8896 bytes on a proven jumbo-frame path), **never divided into chunks** | The application divides large values. Or it stores them in a different place and sends a reference. This limit does not apply to responses. |
| **The remote path reaches only FSM row 0** | A client on a different host reaches one FSM. Only a client on the host of a node reaches the sibling FSMs. |
| **Linux only** (x86-64, aarch64) | UC runs on no other host OS. |
| **The link between client and gateway is plain TCP** | It has no Transport Layer Security (TLS) and no client authentication. Keep the port private, or put a proxy in front of it. |
| **The client SDK is for Rust** | Other languages implement the specified [remote protocol](reference/remote-protocol.md). |
| **Crash faults, not Byzantine faults** | UC trusts each member. |

## Design rules, and where UC implements them

1. **Keep the FSM pure.** Put I/O, time and random values into the command log
   as inputs. On UC, `apply` is synchronous and does no I/O by construction.
   `ctx.time_ns` and `IdGen` replace the clock and the ID generator. Side
   effects go out through the output handler.
2. **Separate the sequencer from the application logic.** Then several
   consumers can share one ordered stream, for example risk, market data and
   audit. On UC, these consumers are several FSMs on one log.
3. **Plan snapshots and log compaction from the start.** Recovery time is
   usually the real constraint. On UC, implement `SnapshotStateMachine` and
   turn on purge. Use `--standby` instants, so that a snapshot never stops
   commit.
4. **Batch and pipeline commands through consensus.** Throughput comes from
   the amortized cost of the quorum round trip, not from a concurrent FSM. UC
   batches replication itself. Keep many requests in flight, not one request
   for each caller
   ([what the shape of a client costs](benchmarks/uc2-remote-client-shapes-2026-09-18.md)).
5. **Shard when one log becomes the bottleneck.** Keep operations across shards
   rare. On UC, a shard is a cluster.
6. **Plan the upgrade path from the first version.** The version of an FSM is
   an input to its state, as a command is. On UC, declare `const VERSION` and
   pin the origin at each upgrade. Then use diff replay to prove the change,
   as the [software development life cycle (SDLC) standard](reference/application-sdlc.md)
   tells you.

## Where to go next

- [Build an application](tutorials/build-an-application.md): from design to
  upgrade, with a replicated key-value store as the worked example.
- [State-machine contract](reference/state-machine-contract.md): what your
  code must implement.
- [Benchmarks](BENCHMARKS.md): measured throughput and latency, and the gate
  that gave each number.
