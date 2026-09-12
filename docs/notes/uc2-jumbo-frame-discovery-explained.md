# Jumbo frames and path-MTU discovery, explained

*Written 2026-09-12 for the jumbo-frame work (plans 1 and 2), which ships in
`2.12.0` — unreleased as this is written. Spec:
`docs/superpowers/specs/2026-09-10-uc2-jumbo-frame-discovery-design.md`; read
its two "Errata … as built" sections alongside the body, because several of
the behaviours below are errata rather than spec text. This note carries the
argument in plain language.*

The operator's task guide is
[Run a cluster on jumbo frames](../how-to/jumbo-frames.md).

## The problem in one sentence

One command must fit one datagram, the datagram size was a source constant
sized for a 1500 B Ethernet path (`MTU_DEFAULT = 1408`), and the clouds UC is
deployed on carry roughly six times that — so the command payload ceiling was
1344 B (crypto off) or 1312 B (crypto on) on a fabric that would have carried
8896 B.

Raising `MTU_DEFAULT` would have been a one-line change and the wrong one:
every cluster whose paths carry only 1500 B would then be sending datagrams
its own network cannot deliver. The ceiling has to follow what the paths
actually carry, which means measuring them.

## Why the ceiling is one value, cluster-wide, forever

The tempting design is per-node: each node discovers its own MTU and sizes
its own door. It is unsound twice over.

- **A command is admitted on one node and shipped by another.** A client on
  node A submits at A's ceiling; the leader B has to replicate it to every
  member. If B's path to C is narrower, the command was admitted and can
  never be replicated — it sits above commit forever.
- **A frame in the log is a permanent obligation.** Once a large frame is
  committed, *every future leader* must be able to ship it to *every future
  member*: as live `DATA`, as a NAK repair, and as the tail a snapshot
  session replays after installing an artifact. A member behind a narrower
  path can never receive it, and there is no mechanism to un-append it.

So the sound value is the minimum over **all pairs** — including
follower-to-follower paths no leader ever sends on today, because tomorrow's
leader might — and it can only ever go **up** within a cluster's life. That
is the whole shape of the feature: a replicated, monotone number that every
node applies at the same commit position.

The number lives where every other cluster-wide policy lives since `2.11.0`:
the replicated `Settings` record inside [the cluster
FSM](uc2-cluster-fsm-explained.md), as `datagram_mtu`. It is **not
operator-writable** — `uc2ctl settings apply` refuses the key and
`node.toml`'s `[settings]` has no such field — because an operator's number
is a claim and discovery's number is a measurement.

## Why do-not-fragment, and what it costs

A probe is only evidence if it arrived whole. Without the do-not-fragment bit
the kernel splits an oversize datagram, the peer's IP stack reassembles it,
and the ack says "8960 carried" about a path that carries 1500 — discovery
would over-report on exactly the networks it exists to protect. So the
replication socket sets `IP_MTU_DISCOVER = IP_PMTUDISC_DO` (IPv4), or
`IPV6_MTU_DISCOVER = IPV6_PMTUDISC_DO` plus `IPV6_DONTFRAG` (IPv6), once at
bind.

DF is also the right posture for the data plane, independently of probing.
UDP fragmentation is all-or-nothing: one lost fragment loses the whole
datagram, and the loss surfaces as an unexplained NAK storm rather than as a
size problem. With DF, an oversize send fails locally with `EMSGSIZE`, is
counted (`uc2_send_emsgsize_total`), and alerts (`Uc2PathBelowMtu`).

Two consequences are worth stating plainly, because both are visible to
operators:

- **A path below 1436 B (IPv4) or 1456 B (IPv6) now fails by name.** Such a
  path used to "work" by fragmenting UC's 1408 B baseline datagrams. It no
  longer does. The baseline is UC's contract — below it a full 32-entry
  schedule table does not fit one frame — so failing loudly is the honest
  outcome, but it is a behaviour change on any network that was quietly
  fragmenting.
- **A node runs on Linux (or Android) only.** The three socket options are
  exposed by `libc` for those targets alone, and running without DF would
  make the ladder unsound rather than merely less informative. So
  `uc_net::sockopt::set_dont_fragment` returns
  `io::ErrorKind::Unsupported` everywhere else and `Node::start` propagates
  it: **a macOS or BSD box cannot run a `uc2-node`**, by a named refusal at
  bind. The client and service crates are unaffected.

## The ladder: three fixed rungs, not a search

Discovery tests a fixed ladder of UC datagram sizes,
`RUNGS = [1408, 8832, 8960]` (`uc_protocol::v2::datagram`):

| rung | why it exists | ceiling, crypto off | ceiling, crypto on |
|---|---|---|---|
| 1408 | `MTU_DEFAULT`, a 1500 B Ethernet path — the baseline every cluster starts from | 1344 | 1312 |
| 8832 | GCP (8896 B) over IPv4, and both clouds over IPv6 — `JUMBO_MIN_RUNG`, what `force_jumbo_frames` demands | 8768 | 8736 |
| 8960 | AWS (9001 B) over IPv4 — `MTU_BOUND`, the largest datagram UC will ever send | 8896 | 8864 |

Every ceiling is `payload_ceiling(rung, crypto_on)`: the rung, less the 16 B
datagram header, less crypto's 24 B (8 B counter + 16 B GCM tag) when it is
on, floored to `FRAME_ALIGNMENT = 32`, less the 32 B frame header. Nothing in
that arithmetic is new; jumbo only made the input a variable.

A binary search would find a larger number on some paths. Three reasons it
was not chosen:

- **The value is replicated, so it must be validatable.** `is_rung` is a
  closed-set check the leader's `validate` and every follower's apply can
  make. An arbitrary discovered integer cannot be distinguished from a forged
  or corrupted one.
- **Determinism.** Two nodes probing the same path must reach the same
  answer, and a test must be able to state the expected answer exactly. The
  in-process proofs assert the rung is exactly 8832 and the ceiling exactly
  8768, not "something around 8800".
- **There is nothing to win.** The rungs are chosen to sit just under the two
  fabrics that exist; the next useful number is a one-line addition to
  `RUNGS` when a fabric justifies it.

## How a rung becomes the cluster's rung

1. **Every node probes every peer.** A `PROBE` (datagram kind 24) is padded
   to exactly `rung` bytes. The responder credits it **only if the received
   length equals the claimed rung** — a truncated or reassembled arrival is
   not proof — and answers with a `PROBE_ACK` (kind 25) carrying
   `rung ‖ own_min_rung`. Both kinds are `Scope::Pairwise`, so under wire
   crypto they are sealed per peer.
2. **A node's own minimum** is the smallest `verified` rung over its
   configured peers, where an unresolved peer contributes `0`. A node
   therefore advertises a jumbo minimum only once it has heard from *every*
   peer at that rung. That number rides on every ack it sends, which is how
   the leader learns about paths it is not on.
3. **The leader's table** holds `min(verified[m], advertised[m])` per member,
   and once *every* current member (voters and learners alike — replication
   reaches learners too) has an entry, and the table's minimum exceeds the
   committed rung, the leader appends one `CLUSTER kind = 3` Settings frame
   raising `datagram_mtu`. One cluster command at a time, like every other.
4. **Every node applies it at commit.** The cluster FSM stores
   `max(committed, incoming)` — monotone in the state machine, not merely in
   the proposer — and the consensus agent then moves three doors together:
   the sender's datagram budget, the appender's payload door, and the cnc
   page's live `payload_ceiling` word at offset **3984** that every client and
   the gateway edge reads per submit. So a client attached *before* the raise
   sees it without reattaching (`payload_ceiling_adopted` records the move).

The `Settings` record grew one `u32` for this (`SETTINGS_LEN = 33`,
`SETTINGS_VERSION = 2`). A version-1 record — the 29 B `2.11.0` shape — still
decodes, reading `datagram_mtu = 0` as "baseline", which is why this flag day
needs no instance-directory wipe even though committed `CLUSTER` frames and a
cluster artifact survive it.

## The monotone rule, and the two prices it charges

Monotone is the only safe direction (the log's frames are permanent
obligations), but it forces two behaviours that surprise people.

**A solo cluster never raises** (errata 4). `ProbeTable::table_min(&[])`
answers `None`, not `Some(MTU_BOUND)`: an empty member set is *no evidence*,
not universal evidence. A one-node cluster has measured nothing, and the
commit would be irreversible. The failure it avoids is concrete: start one
node, `add-learner`, promote — the grow-from-one path — on an ordinary
1500 B network. Had the solo node committed 8960, its snapshot chunks to the
new learner would be cut at the 8960 B budget and never arrive, and a wipe
would be the only remedy. So discovery begins when the first peer joins, and
a one-node cluster keeps the 1344/1312 ceiling.

One consequence runs the other way and belongs in the operator's head:
`own_min_rung` answers `MTU_BOUND` for an empty peer map (there is nothing to
compare), so **`force_jumbo_frames` passes immediately on a solo node** —
there is no peer whose path could be narrow. The flag cannot deliver its
promise on a one-node cluster; it is a multi-node guarantee.

**A jumbo commit is a one-way door for future members, too.** Nobody chooses
it and nothing un-chooses it: on a fabric that carries jumbo frames the cluster
commits as soon as two members have probed each other, and from that moment a
node whose path to some member is narrower cannot ever join — it fail-stops
`path_below_committed_mtu`, by design, because the log already holds frames it
cannot receive. A cross-region learner over 1500 B peering, a DR site behind a
tunnel: not addable later, for the life of the cluster, with a new cluster as
the only remedy. The only opt-out the design offers is to keep one narrow path
in the cluster from the start, which holds the whole cluster at the baseline
(the minimum is over all pairs) and keeps every future member admissible. There
is no way to have both.

**A rung never comes back down.** There is no re-probe-and-lower path. A path
that degrades below the committed rung is an *outage*, not a new ceiling:
`uc2_send_emsgsize_total` climbs and `Uc2PathBelowMtu` fires, and a node
restarting into that state refuses to join by name
(`path_below_committed_mtu`). Lowering would mean the log holds frames the
cluster has decided it cannot carry, which is not a state with a remedy.

## The rejoin reset

A member that comes back after a long absence would otherwise wait out its
peers' 30 s backoff before anyone re-probed it. So a `PROBE` *received* from a
peer we had backed off on resets our cadence toward that peer to the fast
schedule — 1 s, five attempts (errata 2, `ProbeTable::on_peer_seen`). A peer
still inside its fast ladder, and a resolved peer, are no-ops: two nodes
booting together must not keep resetting each other. The practical effect is
that a rejoining member's raise lands in seconds.

## What a permanently narrow cluster looks like

This is the reading most likely to be misdiagnosed as a leak, so it is worth
stating exactly (errata 1).

Probing a peer stops only when **both** halves are settled: our `verified`
rung for it is the top rung **and** its advertised minimum has caught up.
Until then the node keeps re-probing — at the rung it has already verified,
plus anything above it. The reason is that an ack's `own_min_rung` is the
*only* channel through which a peer's own minimum reaches us; there is no
gossip of that fact, so the only way to refresh it is to ask again.

On a cluster with one permanently narrow path, therefore, **every node keeps
probing every peer at the 30 s cadence forever**: two datagrams per 30 s per
peer on a 1408-capped path, three when the refresh rung is appended. That is
an accepted cost, not a bug, and `uc2_probe_sent_total` climbing slowly
forever is the expected shape.

The healthy narrow reading is:

| series | on a narrow cluster | on a resolved jumbo cluster |
|---|---|---|
| `uc2_datagram_mtu_bytes` | 1408 on every node | the proven rung, identical everywhere |
| `uc2_probe_min_mtu_bytes` | **1408 — equal to the committed rung** | equal to the rung |
| `uc2_probe_sent_total` | rising slowly, forever | flat once every peer resolves |
| `uc2_send_emsgsize_total` | `0` | `0` |

One counting rule belongs with that table: **probes the kernel refused are
not in `uc2_probe_sent_total`.** That counter counts datagrams that left the
host; a round that was refused for size — or that found no pairwise crypto
session yet — is counted separately and deliberately unexported, so "discovery
traffic emitted" stays readable on exactly the narrow path where it matters.
(Spec §9's parenthetical says otherwise and is superseded; the erratum is
recorded in the spec.)

`Uc2MtuDiscoveryStalled` keys on `uc2_probe_min_mtu_bytes >
uc2_datagram_mtu_bytes` for 60 s. The alert fires when *this* node has proven
more than the cluster has committed — i.e. some *other* member is the one
holding discovery back. Whether it fires on a legitimately narrow cluster
depends on **where** the narrowness is. A narrow **member** — one host whose
interface MTU is low — never fires it, because that member is a peer of every
other node, so it pins every node's own minimum too and the two gauges agree
everywhere. A single narrow **path** between two members does fire it,
permanently, on every node that is not on that path. A–B narrow (a bad switch
port, a tunnel, one peering leg) with A–C and B–C jumbo leaves C's own minimum
at the jumbo rung while the committed rung stays at the baseline — a cluster at
its correct rung, with C alerting every 60 s forever. Fix the link, or silence
the rule for that node. (`uc2_probe_min_mtu_bytes` reads `0` while any peer is
unresolved, and also on a node with no peers at all: `0` means "nothing is
proven", one encoding for one state.)

## The two startup gates

Both gates exist because a node that cannot carry what the cluster carries
must not pretend otherwise. They differ in who asks for them and in what
"not yet" means.

| | `force_jumbo_frames = true` (spec §6) | the join gate (spec §5.4) |
|---|---|---|
| who turns it on | the operator, per host (env: `UC2_FORCE_JUMBO_FRAMES`) | nobody — it arms itself whenever a node learns of a committed rung above the baseline it has not proven |
| what it demands | every peer proves `JUMBO_MIN_RUNG = 8832` | the voters this node has proven the **committed** rung to form a quorum with it (a joining learner: one proven voter) |
| while pending | holds `can_serve` false and answers `/readyz` with 503 — in **any** role, not just leader; a held LEADER also appends no client command, fires no timer and confirms no linearizable read, so the hold is not merely advisory | the same; `uc2_jumbo_gate_pending` is `1` |
| on proof | `jumbo_gate_passed` (`gate = force`), serving begins | `jumbo_gate_passed` (`gate = join`, with `proven_voters`/`voters`/`self_vote`), serving begins |
| on failure | after `JUMBO_GATE_WINDOW = 30 s`, fail-stop: `jumbo_path_too_narrow` (a peer answered below the rung) or `jumbo_peer_silent` (no peer answer at all — worded as the liveness fact it is) | fail-stop `path_below_committed_mtu` the moment a peer is PROVEN narrow, naming the peer and both rungs — no window |
| on nothing proven either way | (the silent case is a failure above) | HOLDS — silence, a peer still mid-ladder, or an outlived ack — with no window, until a quorum of voters has proven the rung; says so every 30 s (`jumbo_join_gate_holding`, warn) and raises `Uc2JumboGateHeld` after 5 min, in any role |

**The join gate's refusal has no window, silence never refuses, and the pass
is a quorum.** The join gate fail-stops only on a peer that *answered* below
the committed rung **and** has spent its fast probe ladder; it passes once the
voters it has proven the rung to form a quorum with it; it holds on anything
else. Each part matters:

- `verified == 0` is silence — a member that is down, slow, or still
  replaying a cold start. Nothing is known about its path, so it is never
  "narrow" — and it is not something a timer can turn into a verdict either,
  so the gate does not wait *on it*: it waits for a **quorum**. Self plus one
  proven voter on three voters, self plus two on four or five; a joining
  learner has no vote and needs no quorum for anything, so it passes on one
  proven voter; a learner peer's proof counts for nothing, as its ack counts
  for nothing at commit. Why that is the right condition: proof is a probe
  ack over the same UDP plane replication uses, from a voter, and durable
  reports and votes are pairwise-sealed exactly like probes, so a node that
  cannot get a probe ack from a quorum of voters cannot get their commit acks
  or votes either — and serving is leader-only in UC, so a timed pass would
  let it do nothing. Why it is not silent: a holding gate writes
  `jumbo_join_gate_holding` every 30 s with the terms it is short of and the
  members short of the rung, and `Uc2JumboGateHeld` fires after five minutes
  in any role — a held follower or learner is as visible as a held leader.
  Why it needs no
  timer to stay available: the dead-host outage that once argued for one
  (one dead voter out of three holding every restarted survivor at `/readyz`
  503 until it came back — the shape that left the `lin_v2` capstones, which
  kill the leader and then wait for a serving survivor, with no servable node)
  cannot happen, because the survivors *are* the quorum; the restarted one
  proves the rung to the other within a probe round (up to the 30 s slow
  cadence if its fast ladder was already spent). Two earlier iterations
  were availability bugs in opposite directions — refusing on silence
  crash-looped a restarted survivor, holding on *every* peer made a dead host
  an outage — and the unproven pass that briefly replaced them gave up the
  spec's promise: a previously-proven member whose path degraded while it was
  down joined and served, leaving a mislabelled lagging-peer alert where a
  named refusal belonged. Under the quorum rule that member finds every voter
  answering it at the baseline, spends its ladder, and refuses by name.
  `uc2ctl remove <dead-id>` is accepted
  while a gate is pending — admin handling keys on the leader flag, not on the
  gate. A peer ANSWERING below the rung is discovery in flight, resolved
  within its ladder — not a hold of its own; only the missing quorum holds.
- **An ack that has been outlived proves nothing.** The refusal needs a CURRENT
  answer, so every round of an unresolved peer carries one datagram at the rung
  already verified (the refresh rung) and `narrow_peers` requires an ack within
  the last round or two. A peer that answered the baseline once and then stopped
  answering altogether — a killed member, or one restarted under wire crypto,
  whose stale pairwise session means every sealed probe to it is dropped — looks
  exactly like a 1408-carrying path otherwise, and §5.4 fail-stopped a healthy
  node on loopback for it before this rule existed.
- `verified` *below* the committed rung is also the ordinary **mid-ladder**
  state of a perfectly healthy peer: the ladder starts at 1408 and
  `verified` advances ack by ack, so `verified == 1408` with the jumbo rungs
  still in flight is what discovery looks like while it is working. The fast
  ladder having run out (five attempts, ~5 s) is what turns "not proven yet"
  into "tried and failed".

An earlier iteration refused immediately on a narrow answer and fail-stopped
healthy nodes; under `systemd Restart=on-failure` that is a crash loop, which
is strictly worse than not serving. The predicate now lives on
`ProbeTable::narrow_peers`, beside the cadence it depends on.

One case deserves its own sentence because the refusal cannot tell it apart
from a narrow peer: **a host whose own interface MTU is too small**. Its
kernel refuses the larger probes outright with `EMSGSIZE`, and such a refusal
**spends** the probe's attempt rather than being refunded — otherwise that
host would never reach the fast-ladder threshold and would pend forever at
503 instead of refusing by name. So the most likely misconfiguration (one
host's `ip link` MTU left unset) reaches `path_below_committed_mtu` after one
fast ladder, and the refusal text names both possible causes.

## The developer trap

A dev box's cluster runs over loopback, whose MTU is 65 536. Discovery
resolves at the top rung, a 4 KB command works, and the same command fails on
a 1500 B production path. So the notification has to fire **on success**:

> `command_over_standard_ceiling`: command is 2048 B, above the standard
> 1312 B ceiling. This cluster carries it … every deployment will need
> jumbo-frame support on all node paths: set `force_jumbo_frames = true` in
> `node.toml` so a cluster without it refuses to start instead of failing at
> submit.

One warn-level `uc_obs` line, **once per client**, at submit in both client
tiers — `uc_client::Engine` (which knows the live ceiling and prints it) and
`uc_remote` (which does not: protocol v1 advertises no ceiling, so that
clause is omitted). The threshold is `MAX_PAYLOAD_DEFAULT` = 1312 B, the
crypto-on baseline, which holds on any cluster. The node's own view of the
same fact is `uc2_commands_over_standard_total`, counted where frames are
appended — leader-only by construction, so read it summed across the fleet.

Two sharp edges in that warning, both real:

- **A jumbo command relayed through `uc2-gateway` emits the line in the
  gateway's log too**, because the edge submits through `uc_client::Engine`
  like any other local client. An operator reading gateway logs may
  misattribute it to the relay; it is the *client's* command that is over the
  standard ceiling.
- **`uc_remote` measures the bare command** while the node's door sees the
  command plus 16 bytes when the edge's session envelope is on. A command
  within 16 B of the threshold can therefore cross it at the node without
  ever warning at the remote client.

## The security posture, in one paragraph

`PROBE`/`PROBE_ACK` are `Scope::Pairwise`, so with `[crypto].enabled = true`
an off-path or on-path attacker cannot forge one. With crypto **off** the
node↔node plane has no source authentication at all — which the [threat
model](../security/threat-model.md) already states, and the two probe kinds are
handled *ahead* of the receiver's term filter on purpose (a path is a path
whatever the term), so not even the term is an obstacle — and jumbo adds a new
flavour of the existing forged-datagram class: a forged `PROBE_ACK` claiming
the top rung can walk the committed rung up to a size the paths do not carry,
and because the rung is monotone and persists in the cluster artifact, **the
damage survives every restart**. There is no downgrade path. The remedy is
the one the threat model already names: enable wire crypto on any network
where an attacker can inject datagrams.

## What this deliberately does not do

- **Fragment a command across datagrams.** A command above the discovered
  ceiling is refused, never split.
- **Lower the rung, or re-probe after a path degrades.** See above: that is
  an outage.
- **Let an operator choose the rung.** Rejected in the design: the operator
  would have to know the number, and a mis-set node on one narrow path is a
  silent replication failure.
- **Advertise the ceiling over the remote protocol.** `RemoteClient` warns on
  the standard threshold alone; a protocol v2 is a separate backlog item.

## Where to go next

- [Run a cluster on jumbo frames](../how-to/jumbo-frames.md) — the operator's
  guide: enabling it on a cloud fabric, verifying it, `force_jumbo_frames`,
  and what each refusal means.
- [The cluster FSM, explained](uc2-cluster-fsm-explained.md) — where the
  replicated `Settings` record lives and how a `CLUSTER` command is applied.
- [Wire protocol](../reference/wire-protocol.md) — the `PROBE`/`PROBE_ACK`
  bodies, the `Settings` v2 layout, and the flag-day statement.
- [Limits](../reference/limits.md) — the ceiling rows, stated as a ladder.
- [Upgrade a cluster](../how-to/upgrade-a-cluster.md) — the `2.12.0` flag day.
