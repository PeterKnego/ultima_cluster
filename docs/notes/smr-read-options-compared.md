# Reading data in a state machine replication system: the options compared

**Audience:** anyone choosing, reviewing or explaining a read path for an SMR
system — UC's or another. Assumes you know what SMR is; if not, start with
[State machine replication, explained](state-machine-replication-explained.md).
**Status:** explanatory survey. UC's own read path is described normatively by
the v2 design spec (§7) and the code (`uc_node/src/node.rs`); the "What UC
does" section below says where it sits on this map and cites the code it was
checked against (2026-10-07). If they disagree, the code wins and this note is
stale.

---

## Why reading is a design problem at all

Every replica holds the full state, so "any server can answer a query" is true
of the model. A real system still has to say *which* answer you get: a
follower's copy can lag, and a leader that was deposed a moment ago may not
know it yet. Every read option below is a different answer to two questions —
*how do I know this state is current enough?* and *who pays to find out?*

The options fall into three families:

1. **Through the log** — the read gets a slot in the total order like a write.
2. **Leader-confirmed** — the leader proves it is still the leader, then reads
   locally.
3. **Leaderless or replica-local** — a replica answers by itself, with a
   guarantee anywhere from linearizable to "some committed prefix".

## The comparison

### At a glance

| # | Option | Consistency | Read latency | Leader load | Assumes clocks? | Write-path cost |
|---|---|---|---|---|---|---|
| A | Read as command | Linearizable | Full commit + fsync | High | No | Uses log bandwidth |
| B | ReadIndex (leader) | Linearizable | 1 RTT | Medium | No | None |
| C | Follower ReadIndex | Linearizable | 1 RTT + hop | Low | No | None |
| D | Leader lease | Linearizable\* | Local | Medium | **Yes** | None |
| E | Quorum leases | Linearizable\* | Local (many replicas) | Low | **Yes** | Writes wait on lease holders |
| F | Paxos quorum read | Linearizable | Quorum fan-out | None | No | None |
| G | Invalidation (Hermes / CRAQ) | Linearizable | Local | None | No | Writes reach **all** replicas |
| H | Safe-time read | Snapshot → linearizable with commit-wait | Local (+ wait) | None | Usually | Commit-wait |
| I | Session token | Read-your-writes / monotonic | Local (+ apply wait) | None | No | None |
| J | Bounded staleness | Staleness ≤ bound | Local | None | If bound is in time | None |
| K | Local / eventual | Committed prefix | Local | None | No | None |
| L | Read at position P | Exact at P | Local | None | No | Retention cost |
| M | External projection | Eventual | Read store's latency | None | No | Export pipeline |

### How each works, and what it costs

| # | Option | How it works | Trade-off |
|---|---|---|---|
| A | Read as command | The query is appended to the log like a write. It is answered when applied at its position. | Simplest to prove, since there is no separate read path. Every read pays a write's price and grows the journal, snapshots and replay. |
| B | ReadIndex (leader) | The leader notes the current commit position. One heartbeat round to a quorum proves it is still leader. It waits until applied ≥ that position, then queries locally. | No clock assumption and no disk write. Each read costs a quorum round trip, which can be shared across reads (see UC below). The leader also runs every query. |
| C | Follower ReadIndex | A follower asks the leader for its read position (the leader runs B's quorum check). The follower waits for its own apply to reach it and answers locally. | Spreads query work across replicas. Costs an extra hop, and the follower must check the term hasn't changed mid-read. |
| D | Leader lease | After a quorum heartbeat, the leader assumes no rival can be elected for `lease < election_timeout − drift`. It reads locally until the lease runs out. | Fastest linearizable read. A GC pause, VM stall or clock jump can let a deposed leader serve stale data, and nothing detects it. |
| E | Quorum leases | A quorum grants read leases to several replicas. Each lease holder reads locally. Writes must reach every holder or wait for the leases to expire. | Local linearizable reads in many places, at the price of slower writes. Same clock risk as D. |
| F | Paxos quorum read | The client reads a majority and takes the highest accepted position. If that write isn't applied yet, it waits or retries at one replica. | Leaves the leader out of reads. Every read fans out to a quorum, and contention with in-flight writes causes retries. |
| G | Invalidation (Hermes / CRAQ) | Writes mark or version a key on all replicas before committing. A replica with a clean copy answers locally. A dirty copy waits, or asks the tail in CRAQ. | Local linearizable reads everywhere. Writes need all replicas rather than a quorum, so one slow node stalls writes. |
| H | Safe-time read | Each replica publishes a time below which nothing can change. A read at time t waits until safe time ≥ t, then reads the state at t. | Consistent snapshots from any replica. Needs synchronized clocks, or pays commit-wait to make the read current. |
| I | Session token | The client carries the position of its last write or read. Any replica answers once applied ≥ token. | Cheap and gives the guarantee users notice. Not linearizable across clients: you may miss another client's write. |
| J | Bounded staleness | A replica serves locally if its lag is under a bound, in time or positions. Otherwise it waits or redirects. | Caps how old an answer can be. Still not monotonic when you switch replicas, and a time bound needs clocks. |
| K | Local / eventual | Query whatever the replica has applied right now. | Cheapest possible read. The answer can be arbitrarily old, and switching replicas can move you backwards. |
| L | Read at position P | Query a retained snapshot or MVCC version as of committed position P. | Deterministic and repeatable on every replica, which suits audits and diffs. Not current, and keeping old versions costs storage. |
| M | External projection | The state machine exports changes to a separate read store, such as a search index or warehouse, and clients query that. | Moves read load out of the cluster and allows different query models. Eventual, and you own a second system and its pipeline. |

\* Correct only while clock drift and pause bounds actually hold.

**Reading the trade-off column.** Options fall into three groups by cost. Some
pay **per read** (A, B, F). Some pay **per write** (E, G, and H's commit-wait).
The rest pay in **assumptions**: clocks for D and E, staleness for I through M.
No option escapes all three. Choosing a read path is choosing which cost your
workload tolerates best.

## Two traps every option shares

1. **"Committed" is not "applied".** B, C, F, H and I all have to wait until the
   state machine has *applied* up to the read position before querying it. The
   commit counter alone does not tell you the state reflects it.
2. **The replica can change while it answers.** A follower can learn of a new
   term mid-read, or a service process can crash and rebuild between the
   barrier and the query. Either way the answer can come from a different state
   than the one the barrier certified. The fix is a check after capture:
   re-read the term or epoch and discard the answer if it moved.

## What UC does

Checked against the code on 2026-10-07:

- **Linearizable reads are B: ReadIndex on the leader only.** The query drain
  admits a linearizable read only on a serving leader and answers anything else
  with `NOT_LEADER` (`uc_node/src/node.rs`, the `FLAG_V2_LINEARIZABLE` branch of
  the query drain). The leader samples the commit counter as the read position,
  runs a `READ_PROBE`/`READ_PROBE_ACK` quorum round, waits for the service to
  apply up to that position, and only then forwards the query. UC has **no
  follower ReadIndex (C)**.
- **Probe rounds are batched across reads** ("Rung A",
  `uc_node/src/read_round.rs`). Many waiting reads share one probe round, so
  each read's share of the round's cost tends to zero under load. A read is
  certified by *round order* (the round's sequence number when the read was
  admitted), not by comparing positions. Comparing positions was rejected as
  unsafe, because in a quiet period a round confirmed before the read arrived
  can show the same position. The full argument is in
  [The linearizable read barrier, explained](uc2-read-barrier-explained.md).
- **Trap 2 is closed explicitly.** A follower acks a probe only if the term still
  matches, and the answer is checked again after capture. A service-epoch check
  accepts the answer only if the service did not restart during the query.
- **Snapshot reads are K.** A read without the linearizable flag skips the
  barrier and goes straight to the service with no epoch check (same drain,
  `node.rs`). The client picks per query: `Consistency::Linearizable` or
  `Consistency::Snapshot` (`uc_client/src/engine.rs`).
  **A note on the name:** UC's "snapshot read" is option K, a local read of
  whatever the node's service has applied right now. It is called a snapshot
  because the answer reflects one consistent applied position (a committed
  prefix), but nobody chooses that position and nothing ties it to the
  present. It is not H (a snapshot at a chosen time), not L (a retained version
  at a chosen position), and unrelated to UC's snapshot artifacts (`SNAPSHOT`
  instants, `ULTSNAP2` files). Wherever this note says "snapshot read" about UC,
  read "K".
- **A (read as command) is available to any application, with no framework
  support needed.** Define a command whose `apply` leaves the state untouched
  and returns the data as its `Response`. Apply runs only up to
  `min(commit, durable)`, and the response returns over the egress broadcast
  keyed by position. So the answer cannot reach the client before the command
  commits, and the read is linearizable at its own position in the order. The
  price is the full write path: admission, replication, quorum fsync, and a
  permanent place in the journal, re-applied by every replica and every replay.
  The query must also fit the command payload ceiling. Use it only when the
  read must be ordered relative to specific writes (for example, a read whose
  result feeds `on_committed`) or must be recorded in the log for audit. For a
  plain current read, the batched barrier is far cheaper.
- **There is no leader lease (D), deliberately.** It was designed as "Rung B" in
  `docs/superpowers/specs/2026-07-24-uc2-leader-lease-design.md` and then
  discharged. According to that brief's resolution block, re-measuring after
  Rung A (`docs/benchmarks/uc2-read-profile-2026-07-26-after-rung-a.md`) left
  the barrier's cost inside run noise on a LAN. A lease would add a clock-drift
  assumption to read safety and buy nothing measurable. It would come back only
  for a WAN or cross-region deployment, where each probe is a full wide-area
  round trip, and only as its own project. Keeping it out preserves a UC-wide
  property: **clocks drive liveness, never safety.**
- **Not offered today:** session tokens (I), time-addressed reads (H; `query`
  gets no `time_ns` by design) and historical reads at a position (L). UC
  addresses everything by byte position, so I would be cheap to build: the token
  is a position the client already learns from its write's response.

## Would another read path be faster in UC? (measured 2026-10-07)

Rung A removed the barrier's cost: linearizable and snapshot reads are equal at
plateau on the leader. So no certification scheme (a lease included) can win
throughput on a LAN. The remaining lever is **where the query runs.**
`docs/benchmarks/uc2-read-spread-2026-10-07.md` measured the upper bound for
moving reads to followers: snapshot reads on all 3 nodes vs on the leader only.

**What was tested: UC snapshot reads only (option K, local reads), not follower ReadIndex.**
UC has no follower ReadIndex to test; a follower answers a linearizable read
with `NOT_LEADER`. Every client sent snapshot reads: on the leader alone in one
arm, and on all 3 nodes at once in the other (follower clients use the
harness's `--any-role` flag). A snapshot read skips both costs a linearizable
read pays: there is no quorum barrier, and no wait for the service to apply up
to the read position. That makes the run an **upper bound** for moving reads to
followers, chosen on purpose: if spread snapshot reads did not scale, no
follower read path could.

- **Proven: snapshot read capacity scales with the number of nodes answering.**
  Spread ÷ leader-only = **2.74** read-only and **3.65** under 20k writes/s,
  against a pre-committed 2.4 bar. The query work is per node and does not
  funnel through the leader.
- **Not proven: that follower ReadIndex (C) would scale the same way.** It adds
  two costs this run did not include:
  1. *A leader round per batch*: the follower asks the leader for a read
     position, which the leader confirms with a quorum probe round. Batched
     rounds cost about nothing on the leader (Rung A), so this probably does
     not cap capacity, but that is inference.
  2. *An apply wait on the follower*: it must apply up to the leader's commit
     position before answering. Under write load that adds replication lag to
     every read's latency. It need not cut throughput, since the follower
     applies those writes anyway, but nobody has measured it.

  Session tokens (I) carry only the apply wait.
- **So "reads scale with nodes" holds today for snapshot reads only.**
  Linearizable reads in UC are leader-only, so their capacity is one node's.
  They would scale only if follower ReadIndex were built, and this run puts the
  ceiling for that at about 2.7–3.6× on 3 nodes, minus whatever those costs
  turn out to be.
- **Side finding: a read-only service runs at a fraction of its read
  capacity.** The service answers queries on its apply thread (at most 64 per
  cycle), but query work does not count as progress for the thread's idle
  ladder. A service with no writes therefore sleeps 50 µs between batches,
  which caps it near 600k reads/s per node. A local smoke run (dev box, not
  fleet data) measured 599k reads/s read-only by default vs 3.76M with
  `UC2_APPLY_IDLE=spin`, and 3.32M with writes. This affects every read path
  on every node, linearizable included. It is recorded in the bench doc
  (§5.4); no fix has been made.

## Choosing, in short

- **Default linearizable read:** ReadIndex, with probe rounds batched across
  reads. Running it on followers (C) is worth adding only when the leader's
  query CPU, not the probe, becomes the limit.
- **Most application reads:** session tokens. Read-your-writes at local cost is
  the guarantee users actually notice.
- **Leases:** only with a monotonic clock, a deliberate safety margin, and an
  accepted risk that a long pause breaks safety without anyone noticing.
- **Read as command:** only when the read must sit at a fixed point in the
  order relative to writes, for example because its result feeds a side effect.
  In UC this is a normal command arm, not a framework feature.

## Further reading

- Ongaro, *Consensus: Bridging Theory and Practice* (2014), §6.4 — ReadIndex
  and leases in Raft.
- Moraru, Andersen, Kaminsky, "Paxos Quorum Leases" (SoCC 2014).
- Charapko, Ailijiang, Demirbas, "Linearizable Quorum Reads in Paxos"
  (HotStorage 2019).
- Katsarakis et al., "Hermes: A Fast, Fault-Tolerant and Linearizable
  Replication Protocol" (ASPLOS 2020).
- Terrace, Freedman, "Object Storage on CRAQ" (USENIX ATC 2009).
- Corbett et al., "Spanner: Google's Globally-Distributed Database" (OSDI 2012).
- Terry et al., "Session Guarantees for Weakly Consistent Replicated Data"
  (PDIS 1994).
- In-tree: [The linearizable read barrier, explained](uc2-read-barrier-explained.md),
  `docs/superpowers/specs/2026-07-26-uc2-rung-a-batch-probe-design.md`.
