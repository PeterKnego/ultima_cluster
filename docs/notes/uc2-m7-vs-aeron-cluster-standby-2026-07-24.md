# M7 live reconfiguration vs. Aeron Cluster Standby

**Date:** 2026-07-24
**Status:** analysis note (comparison + forward-looking sketch)
**Related:** `docs/superpowers/specs/2026-07-13-uc2-reconfig-design.md` §1
("Why not port Aeron's design"), `docs/benchmarks/uc2-m7-gate-2026-07-13.md`
**Source:** Aeron premium docs — Cluster Standby overview
(https://aeron.io/premium-docs/aeron-cluster-standby/standby-overview.html)

## Headline

These solve the same operational problem — "keep serving while a box dies or
the topology changes" — with **opposite architectural philosophies**. M7 does
membership change *inside* the consensus protocol. Aeron's production answer
(Cluster Standby) deliberately moves replacement *outside* it.

## What each one is

**M7 (UC v2.1)** — single-server membership change, config-as-log-entry
(Raft §4.2.2, adapted to byte positions). A membership change is a
`FRAME_TYPE_CONFIG=4` frame the leader appends *in-stream*: it occupies
positions, replicates as bytes, is archived and CRC-covered, and takes effect
at commit. Voters and learners are all full consensus participants (learners
just aren't counted in quorum yet). Promote/demote/add/remove **one change at a
time, under load, no restarts**, via the `uc2ctl` admin CLI.

**Aeron Cluster Standby** — a premium warm-standby DR component. Standby nodes
are **explicitly non-consensus members**: they don't vote, don't participate in
elections, and *don't apply back-pressure to the live log*. They asynchronously
pull the log via the archive (backup-query → replay channels), drive their own
service copies, and can snapshot independently. Promotion to a real cluster
member happens through a **TransitionModule** that *stops the standby and starts
a fresh ConsensusModule* — plus out-of-band steps like DNS/name-resolution
repointing.

## Side-by-side

| Dimension | **M7 reconfiguration** | **Aeron Cluster Standby** |
|---|---|---|
| **Core idea** | Membership *is* consensus state (log entry, committed, truncation-safe) | Replication is *out of band*; standbys sit beside consensus |
| **Node role** | Voter or learner — both are protocol members; learner is one `promote` from voting | Non-consensus replicator; never votes, never back-pressures the leader |
| **Replication coupling** | Synchronous — a promoted voter's durable position gates commit (quorum order-statistic) | Asynchronous — standby may lag; **data loss on failover is inherent** |
| **Membership authority** | The log. `ClusterConfig{version, voters, learners, tombstones}` in a durable `StableValue`, re-derived from the journal scan on boot | Boot config lists consensus nodes; standby has a `clusterMemberId` but it's decorative (no vote) |
| **How a dead box is replaced** | In-protocol: `add-learner` → catch up → `promote` → `remove-voter <dead>` (3 committed changes), online | TransitionModule stops standby / starts ConsensusModule + external DNS repoint. A *cluster mutation event*, not a live protocol op |
| **Topology change (3⇄5)** | First-class: two add+promote (or demote+remove) pairs, one in flight | Not what it's for. Standby count is a DR/read-scaling knob, not a consensus resize |
| **Cross-region / multi-site** | Not a goal — trusted-network posture, same voting set spans hosts | **Primary purpose.** "Warm DR where log data is replicated to another region/DC," daisy-chained standbys |
| **Snapshots** | Node-built (M6), used for below-floor learner catch-up | Standby can snapshot *off* the live cluster — offload heavy/slow services (query, persistent egress); cluster can even recover from a newer standby snapshot |
| **Consistency during transition** | Strong throughout — single-server overlap guarantees adjacent-config quorums intersect; truncation-revert keeps adopted config = durable frontier | Async → possible loss; "some action needs to be taken outside the system to resolve that situation" |
| **Failover latency** | Leader self-removal → normal election, ~200 ms class (measured) | Promotion is a heavier orchestrated transition, not sub-second |
| **Fleet-proven** | Yes — 5-host AWS gate: worst transition dip 4.7% (<10% bar), self-removal 3.22s, zero loss/divergence | Aeron's own *in-protocol* dynamic join was deprecated (~1.41) and removed (1.42, 2023) as never production-quality — Standby is the retreat |

## The key architectural divergence

This is the whole story, and the M7 spec (§1) names it explicitly:

- Open-source Aeron once shipped **dynamic join** — passive members +
  log-recorded membership events, which is *structurally the same design as M7*
  — then **deprecated and removed it** because the integration surface
  (membership × snapshots × truncation × recovery) never reached production
  quality.
- Aeron's production answer, **Cluster Standby, moves replacement out of the
  consensus protocol entirely**: async replication + an explicit non-voting role
  + stop-and-restart-as-a-member promotion. That sidesteps the hard interaction
  surface by never letting membership live in the log.
- **M7 takes the opposite bet.** It keeps membership *in* the log and pays for
  the hard surface with the sim (`uc2_sim` inv6–inv9 + counterfactual-red pins),
  the WGL lincheck capstones, the SIGKILL crashtest, and truncation-revert as a
  first-class design element (§5). The spec frames Aeron's retreat as "an
  engineering warning about the integration surface, not about the math" — and
  M7 is "the first place v2 deliberately exceeds the reference."

Concretely, M7 has machinery Standby doesn't need because it never faces the
problem: **truncation-revert** (a config frame below a truncation point
atomically reverts to `prev`), **fresh-forever tombstoned NodeIds** (removed ids
can never reappear — kills zombie identity), and **rebuild-at-boundary** of the
quorum machinery (`CommitTracker`, `follower_slot`, flow-control split) at each
adopted config. Standby avoids all of it by keeping membership static and
replication async.

## Where they're complementary (not competing)

They're not the same feature, so "which is better" is the wrong question:

- **A UC learner ≈ an Aeron standby's *good* properties, minus the async gap.**
  A UC learner is replicated-to and never counted in quorum — the same "receive
  the log, don't disturb consensus" role. But a UC learner *does* apply
  flow-control back-pressure once promoted and is synchronous, so it can't be
  pushed to another region cheaply the way a standby can, and it can't be a
  lag-tolerant read replica.
- **What UC v2 does *not* have that Standby does:** async cross-region DR
  replication, non-back-pressuring read/query offload, snapshot-taking off the
  live path, daisy-chained replication. Genuine gaps if you want geo-DR or heavy
  read fan-out. UC's posture is explicitly trusted-network, same-region voting
  set.
- **What Standby does *not* give you that M7 does:** true online,
  strongly-consistent membership change and cluster resize with zero committed
  loss and ~200 ms failover — no external DNS dance, no stop/restart-as-member.

**Bottom line:** M7 is *online strongly-consistent reconfiguration* — the thing
Aeron tried in-protocol, pulled, and replaced with an out-of-band DR product.
Cluster Standby is *async warm-standby DR + read/snapshot offload* — a different
capability UC v2 hasn't built (it would land as async cross-region learners), and
a plausible future item if geo-DR ever enters scope.

## Forward-looking sketch: an "async cross-region learner" (UC's Standby analog)

### The key realization: today's learner is already ~80% of a Standby

A UC v2 learner already has the two properties that define Aeron's non-consensus
role, and it got them "for free" from the M6/M7 design:

- **It never back-pressures the leader.** `FlowControl` keeps voters and learners
  in two lists; `limit()` is the quorum-th order statistic over **voters only**
  (`uc2_net/src/flow.rs:76-83`), and a learner's advert is stored as bare
  `contiguous` and never consulted (`flow.rs:50-56`). Unit-proven:
  `learner_status_never_moves_the_limit` (`flow.rs:132-149`). A lagging learner
  cannot stall the send cursor.
- **It never gates commit.** `rebuild_membership` sets `members = voter_ids()`
  only, and `follower_slot` returns `None` for a learner, so its `Report` never
  reaches the `CommitTracker` (`uc2_consensus/src/election.rs:1382-1401`,
  `1250-1262`).
- **It self-heals when it falls behind** over the *same* reliable-UDP machinery
  as a voter: shallow NAK off the ring (`sender.rs:588-601`), deep-NAK replay
  from the journal when >1 buffer behind (`sender.rs:602-676`), and a snapshot
  session when it drops below the purge floor (`sender.rs:684-771` /
  `receiver.rs:816-937`). SNAP_BEGIN even carries the leader's `ConfigRecord`
  so the joiner adopts membership.

So a learner is **already async and lag-tolerant in the ways that matter.** It is
*not* a Standby only because of four missing capabilities. Each maps to a
specific, narrow seam.

### Gap 1 — Cross-region reach (transport/trust), the real blocker

Today's posture is trusted-network, same-region: the known-source guard trusts
seed-config addresses and drops everything else (`node.rs:2119-2124`,
`derive_peer_maps` at `node.rs:3016-3042`). Shipping the log across regions/DCs
needs **wire-crypto** (already the named next-after-M7 item) plus NAT/routing
tolerance on the reliable-UDP path. *This is the gating dependency* — nothing
below is safe to expose over a WAN without it. No consensus-core change.

### Gap 2 — Stale/bounded-staleness reads off the learner (the payoff feature)

Aeron's headline Standby win is running a query/egress service *off* the standby
without touching the live cluster. UC's shape for this:

- A cross-region learner runs its **own `uc2_service` copy** applying its local
  (lagging) committed positions — the apply agent already polls
  `min(commit, durable)` in the log buffer in place. That's a read replica for
  free, *if* the read path doesn't demand leader contact.
- Today linearizable reads go through the `READ_PROBE`/`ACK` quorum barrier —
  a WAN round-trip a standby should never pay. Add an explicit **stale-read /
  bounded-staleness query mode** that serves from the learner's local applied
  state and returns its lag (commit-position delta) so the caller can bound it.
  This is a new `Query` route, not a change to the linearizable path — the
  framework already routes linearizable vs. snapshot reads by type.

**Effort:** moderate, additive. **Risk:** low — it's a new, clearly-labeled
non-linearizable path; the strong path is untouched.

### Gap 3 — Daisy-chaining (learner-as-relay), to bound WAN fan-out

Aeron lets a standby replicate from another standby. UC's fan-out is
leader→followers, and a learner's own sender currently gets an **empty solo
fan-out** (`node.rs:573-587`). The `Sender` machinery is already generic — it
serves NAKs from its ring (`serve_nak`), deep-NAKs from its journal
(`serve_nak_from_journal`), and opens snapshot sessions (`try_open_snap_session`)
without caring who the downstream is. So a **relay learner** = give its sender a
downstream fan-out of further-downstream learners and let them NAK against it.
One WAN stream from the leader to a regional relay, then intra-region fan-out.

**Effort:** moderate. **Risk:** medium — a relay must serve consistent bytes
from its *own* frontier (it can only relay what it has durably), and the config
frame in SNAP_BEGIN must reflect the relay's adopted config. Wants its own sim
arm.

### Gap 4 — DR promotion / failover semantics (the genuinely hard part)

Aeron's TransitionModule stops the standby and starts a ConsensusModule + an
external DNS repoint, and openly accepts async data loss ("some action needs to
be taken outside the system"). UC's tension:

- **In-region promotion already exists** (`promote`), but you would *not* promote
  a lagging cross-region learner into the quorum — the promote precondition
  (learner durable ≥ commit − slack) correctly refuses until it's caught up
  (`election.rs:1406-1418`; spec §6). That's the right guard for a *live* member,
  and it's exactly why a WAN-lagged learner shouldn't join the voting set.
- **True DR failover** (primary region gone, remote region forms a new cluster)
  means promoting from a log that is *behind* — i.e. accepting a truncation /
  divergence against writes the dead primary had committed but not shipped. UC's
  strong model has no in-protocol answer for that by construction; it would be an
  **operator-driven seed-config bootstrap of a new cluster** from the remote
  learners' frontier, with the same "reconcile externally" caveat Aeron carries.
  Tombstones + config-version give a clean *identity* story for the new cluster;
  they do **not** absolve the data-divergence reconciliation.

**Effort:** high, and partly a *product/semantics* decision, not just code. This
is where UC would be adopting Aeron's async-DR trade-off (bounded loss) that M7
deliberately does not make.

### Suggested shape if this is ever taken up

1. **Prereq:** wire-crypto (Gap 1) — hard dependency, do first.
2. **Phase A — read replica:** stale-read query mode (Gap 2). Highest value,
   lowest risk, mostly additive; delivers the "query/egress offload" win alone.
3. **Phase B — relay:** learner-as-relay fan-out (Gap 3) for WAN efficiency; new
   sim arm for relay-frontier consistency.
4. **Phase C — DR failover (Gap 4):** only if geo-DR-with-bounded-loss becomes an
   explicit product goal; treat as a *separate* consistency posture from the
   strong core, documented as such, with an operator runbook for external
   reconciliation.

**Net:** UC does not need a parallel "Standby" subsystem the way Aeron built one.
The learner role is already the right primitive; a Standby is a learner with
(a) crypto-secured WAN transport, (b) a stale-read mode, and optionally (c) relay
fan-out — with DR failover as a deliberate, separately-scoped weakening of the
consistency model, not a free extension of it.
