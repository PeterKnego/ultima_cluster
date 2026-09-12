# Run a cluster on jumbo frames

Raise the command payload ceiling from ~1.3 KB to ~8.8 KB by giving the nodes
a fabric that carries jumbo frames. UC discovers the rest: there is no MTU
key in `node.toml`, and nothing to set per node.

| datagram rung | command ceiling, crypto off | crypto on |
|---|---|---|
| 1408 B (the baseline every cluster starts from) | 1344 B | 1312 B |
| 8832 B (GCP over IPv4; both clouds over IPv6) | 8768 B | 8736 B |
| 8960 B (AWS over IPv4) | 8896 B | 8864 B |

Why it works this way, and why the value is cluster-wide and monotone:
[Jumbo frames and path-MTU discovery,
explained](../notes/uc2-jumbo-frame-discovery-explained.md). This page is the
task.

## Before you start: this is a one-way door

The committed rung is **monotone** and there is no opt-out — no config key
lowers it, and no operator command un-commits it. On a fabric that carries
jumbo frames, the upgrade therefore commits the cluster to 8832/8960 B as soon
as two members have probed each other, with no further decision from you.

From that moment, **no member on a narrower path can ever join**. A node whose
path to some member answers below the committed rung fail-stops with
`path_below_committed_mtu` ([§6](#6-when-a-node-refuses-to-join)) — by design:
the log already holds frames it cannot receive. A cross-region learner behind
1500 B peering, a DR site over a tunnel, a laptop on a VPN — none of them can
be added later, for the life of the cluster, and the only remedy is a new
cluster.

If you know you will need a narrower member eventually, **keep one narrow path
in the cluster from the start** — that is the only opt-out the design offers.
The minimum over all pairs is what gets committed, so one member on a 1500 B
path holds the whole cluster at the baseline
([§4](#4-when-the-cluster-stays-at-the-baseline)) and every future member can
still join. There is no way to get a jumbo ceiling *and* keep narrow members
admissible.

## Before you start: a node runs on Linux

Discovery depends on setting the do-not-fragment bit on the replication
socket, and `libc` exposes `IP_MTU_DISCOVER` / `IPV6_MTU_DISCOVER` /
`IPV6_DONTFRAG` on **Linux and Android only**. Without DF the kernel would
fragment a probe, the peer would reassemble it, and the ack would claim a size
the path does not carry — discovery would over-report on exactly the networks
it exists to protect. So rather than run with an unsound ladder, `uc2-node`
**refuses to start on any other OS**, by name, at bind:

```
do-not-fragment is not available on this OS; path-MTU discovery would
over-report the path (jumbo spec §4.3)
```

This is not jumbo-specific and not opt-out: since `2.12.0` a macOS or BSD box
cannot run a node at all. The client (`uc_client`, `uc_remote`) and service
crates are unaffected — only the node binary binds that socket. Develop
against a Linux VM or container.

## 1. Give the hosts a jumbo fabric

UC does not trust a configured MTU; it measures the path. Your job stops at
the infrastructure:

- **AWS** — a VPC carries up to 9001 B between instances. Use an instance type
  and driver combination that supports it, and set the interface:
  `sudo ip link set dev ens5 mtu 9001` (make it persistent the way your distro
  does). UC lands on the 8960 B rung.
- **GCP** — a VPC network's MTU is a network property, settable up to 8896 B;
  set the network's MTU *and* the interface to match. UC lands on the 8832 B
  rung.

Check both ends carry it before involving UC, with DF set so the kernel does
not quietly fragment the test:

```sh
# -s 8960 puts 8988 B on the wire (8960 + 8 ICMP + 20 IPv4) — exactly what
# UC's 8960 B top-rung datagram occupies. -M do sets do-not-fragment.
ping -M do -s 8960 -c 3 10.0.0.2
```

A path that leaves the VPC — an internet gateway, a VPN, a tunnel, some
peering configurations — may carry far less than the interface claims.
Discovery measures each pair separately and the committed rung is the minimum
over all of them, so one such path keeps the whole cluster at the baseline.
That is the intended outcome, not a failure: see
[§4](#4-when-the-cluster-stays-at-the-baseline).

## 2. Change nothing in UC's config

There is no MTU key. `max_payload` is **retired** as of `2.12.0` and refused
by name at startup:

```
max_payload is no longer configurable (2.12.0): the command payload ceiling is
discovered from the path MTU between nodes and committed cluster-wide. Delete
the line. To REQUIRE jumbo frames, set force_jumbo_frames = true instead.
```

Delete it from every `node.toml` before the flag day ([Upgrade a
cluster](upgrade-a-cluster.md)). The ceiling is a replicated setting
(`datagram_mtu` inside the cluster's `Settings` record), so it is also not
something `uc2ctl settings apply` can write — an operator file carrying
`datagram_mtu` is refused as an unknown key.

Restart the cluster on the new binaries. Every node probes every peer on boot
and on every membership change; the leader commits the minimum once every
member has answered.

## 3. Verify

**The node's own view** — `uc2ctl status` prints one ceiling line:

```
ceiling: 8864 B (rung 8960, discovered)
ceiling: 1344 B (rung 1408, baseline)
```

`discovered` means a rung above the baseline is committed. One wrinkle worth
knowing: `status` reads the committed rung out of this node's newest **cluster
artifact**, and the snapshot cadence defaults to `0` (instants are commanded,
not automatic), so a cluster that raised its rung may have no artifact to read
it from yet. In that case the line infers the rung from the live ceiling and
says so:

```
ceiling: 8864 B (rung >1408, discovered — inferred from the ceiling; no
committed cluster artifact to read the rung from)
```

If the ceiling is *below* what the printed rung allows, the line says
`— capped by this node's own max_payload, not by the rung`: the binding half
is this node's buffer bound, not the cluster's rung.

**The cluster's view** — `/metrics`, once `[metrics]` is configured:

| series | healthy on a jumbo cluster |
|---|---|
| `uc2_datagram_mtu_bytes` | the same rung on every node (8960 or 8832) |
| `uc2_payload_ceiling_bytes` | the matching ceiling; the same value clients read from the cnc page |
| `uc2_probe_min_mtu_bytes` | equal to the rung on every node (`0` = nothing proven yet, or no peers) |
| `uc2_probe_sent_total` / `uc2_probe_acked_total` | flat once every peer has resolved |
| `uc2_send_emsgsize_total` | **0**, always |
| `uc2_commands_over_standard_total` | however many commands over 1312 B this leader has appended |
| `uc2_jumbo_gate_pending` | **0** — `1` means this node is held by a startup gate (§5, §6) and is not serving |

`uc2ctl settings show` prints the committed record including `datagram_mtu`,
and each discovery commit is in `audit.jsonl` as a `settings_apply` with
`source = "discovery"` and `actor = "node"` — the one audit line no admin
request produced. The node also logs `datagram_mtu_proposed` (leader) and
`payload_ceiling_adopted` (every node) as the rung moves.

Expect the raise within a few seconds of the last node starting; the probe
ladder retries every 1 s for its first five attempts.

## 4. When the cluster stays at the baseline

`uc2_datagram_mtu_bytes = 1408` after the cluster has settled means some path
did not prove a jumbo rung. Read `uc2_probe_min_mtu_bytes` on **every** node:
the node (or nodes) reporting `1408` is the one whose path to some peer is
narrow; a node reporting `0` has a peer that has not answered at all.

Two readings are normal and must not be chased:

- **`uc2_probe_sent_total` rising forever on a narrow cluster.** Probing a
  peer stops only when the top rung is verified *and* that peer's advertised
  minimum has caught up, and an ack is the only channel for the second fact.
  So a permanently narrow cluster re-probes at the 30 s cadence indefinitely:
  two to three datagrams per 30 s per peer. Accepted cost, not a leak.
- **A one-node cluster never raises.** An empty member set is no evidence, so
  a solo node keeps the 1344/1312 ceiling and discovery starts when the first
  peer joins.

`Uc2MtuDiscoveryStalled` (`uc2_probe_min_mtu_bytes > uc2_datagram_mtu_bytes`
for 60 s) is the alert for "this node has proven more than the cluster has
committed" — i.e. some *other* member is holding discovery back. Whether a
cluster that cannot beat the baseline fires it depends on **where** the
narrowness is:

- **A narrow member** — one host whose interface MTU is low — never fires it.
  That member is a peer of every other node, so it pins every node's own
  minimum too and the two gauges agree everywhere.
- **A narrow path between two members** does fire it, permanently, on every
  node that is not on that path. A–B narrow (a bad switch port, a tunnel, one
  peering leg) with A–C and B–C jumbo leaves C's own minimum at the jumbo rung
  while the committed rung stays at the baseline — a cluster at its correct
  rung, with C alerting every 60 s forever. Fix the link, or silence the rule
  for that node.

## 5. Require a jumbo path at startup

If your application's commands do not fit the standard ceiling, a cluster
without jumbo support should refuse to start rather than serve and then refuse
those commands one at a time. That is the flag:

```toml
force_jumbo_frames = true    # default false; env: UC2_FORCE_JUMBO_FRAMES=1
```

(The env override accepts `1`/`true`/`0`/`false` only; anything else is a
named refusal rather than a silent `false`.)

With it set, the node starts its agents, replicates and votes as usual, but
**holds serving** — `can_serve` stays false and `/readyz` answers 503 in any
role, leader or follower — until every configured peer has proven the
8832 B rung. Then it logs `jumbo_gate_passed` and serves.

If 30 s (`JUMBO_GATE_WINDOW`, not configurable) elapses first, the node
fail-stops with exit code 1 and one of two named refusals — **unless this node
has already learned a committed jumbo rung**. Once it has (from its own
cluster artifact at boot, the archive replay, or an installed snapshot), the
join gate (§6) takes precedence: it holds until a quorum of voters has proven
the committed rung and never fail-stops on silence, so a silent peer is then a
hold, not `jumbo_peer_silent`. The precedence is per node and starts when the
rung is learned: a forced node that has not yet reached the `CLUSTER` frame
in its replay, or a fresh joiner still waiting on its snapshot session, runs
the force gate's rule and its 30 s window until then.

| refusal | what it means | what to do |
|---|---|---|
| `jumbo_path_too_narrow` | the peer **answered**, and its best acked rung is below 8832 — the refusal names the first offender's member id and carried rung, and the log line lists all of them | raise the interface/path MTU to at least 8832 B end to end, or unset the flag to run at the discovered rung |
| `jumbo_peer_silent` | the peer never acked at any rung, so nothing is known about its path | a **liveness** problem, not an MTU one: start the member, or fix what is dropping UC's traffic to it, then restart this node |

Two caveats:

- **On a one-node cluster the gate passes immediately**, because there is no
  peer whose path could be narrow. It is a multi-node guarantee; do not read a
  passing solo node as proof of a jumbo fabric.
- A gated node — leader or not — raises `Uc2JumboGateHeld` after five
  minutes, and `Uc2LeaderNotServing` excludes a pending gate, so a gated
  leader raises exactly that one alert.
  `uc2_datagram_mtu_bytes` / `uc2_probe_min_mtu_bytes` say how far
  discovery got.

## 6. When a node refuses to join

Independent of the flag, and not configurable: once a cluster has **committed**
a jumbo rung, a node whose path to some member answers *below* it fail-stops
at startup rather than joining.

```
consensus fatal (fail-stop): PathBelowCommittedMtu peer=0 committed=8960
carried=1408 — … either the path between them is narrower than 8960 B, or
THIS host's own interface MTU cannot carry it (the kernel refused the larger
probes for size). Check `ip link` on both ends and the VPC/subnet MTU, then
restart; the rung is monotone and will not come back down.
```

Three things to know about it:

- **The remedy is the path, never a wipe.** The log already holds frames this
  node cannot receive; nothing in its instance directory is wrong. Fix the
  MTU — on the peer's path *or on this host's own interface*, which the node
  cannot distinguish, hence the two-cause wording — and restart.
- **Silence never refuses, and never passes either — the gate passes on a
  quorum.** A member that is down, slow, or still replaying answers nothing,
  and nothing is known about its path — so it is never "narrow". What ends
  the hold is proof from enough voters: the gate passes once the voters this
  node's probes have proven the committed rung to form a **quorum with this
  node** — self plus one on three voters, self plus two on four or five. A
  joining **learner** has no vote and needs no quorum for anything (its
  frames come from the leader, a voter), so it passes on **one** proven
  voter; a learner peer's proof counts for nothing, as its ack counts for
  nothing at commit. The pass is logged as `jumbo_gate_passed` (`gate =
  join`) with `proven_voters`, `voters` and `self_vote` on the record. The
  one voter a learner proves need not be the leader, whose path is the one
  its frames actually cross — a learner proven to another voter serves
  `/readyz` 200 with the leader's path untested if the leader is silent; if
  the leader is answering below the rung, the refusal catches it. There
  is **no timer**, and none is needed: proof is a probe ack over the same UDP
  plane replication uses, from a voter, and durable reports and votes are
  pairwise-sealed exactly like probes, so a node that cannot get a probe ack
  from a quorum of voters cannot get their commit acks or votes either, and
  serving is leader-only — a timed pass would let it do nothing. The outage
  that once argued for a timer — one dead voter out of three holding every
  restarted survivor at `/readyz` 503 until it came back — cannot happen
  under this rule, because the two survivors *are* the quorum: the restarted
  one proves the rung to the other within a probe round (1 s on the fast
  ladder, up to the 30 s slow cadence once that ladder is spent) and serves.
  **A hold that does not clear has a voice**: every 30 s the node logs
  `jumbo_join_gate_holding` (warn) naming the committed rung, the quorum terms
  it is short of and every member short of the rung, and after five minutes
  `Uc2JumboGateHeld` fires — in any role, so a held follower or learner is as
  visible as a held leader. The earlier iterations were the two availability
  bugs this rule was built to avoid (refusing on silence crash-looped a
  restarted survivor; holding on *every* peer made a dead host an outage), and
  the unproven pass that briefly replaced them gave up the spec's promise: a
  previously-proven member whose path degraded while it was down — a shrunk
  interface MTU, the likeliest misconfiguration — joined and served, leaving a
  mislabelled `Uc2PeerLagging` where a named refusal belonged. Under the
  quorum rule that member finds every voter answering it at the baseline,
  spends its ladder, and refuses by name (§6's remedy).
- **A peer ANSWERING below the rung is discovery in flight, not a hold of
  its own.** The baseline ack has landed and the jumbo rungs have not; only
  the missing quorum holds serving, so a mid-ladder peer outside a proven
  quorum delays nothing. It resolves one way or the other within a probe
  ladder (five attempts, ~5 s): either the jumbo ack lands, or the ladder
  runs out and the node refuses by name.
- **A peer that answered once and then went quiet is silent, not narrow.** The
  refusal needs CURRENT evidence: every probe round carries one datagram at the
  rung the path is already known to carry, and a peer is only refused while it
  keeps answering that one while the larger rungs go unanswered. A killed
  member, a restarted one (under wire crypto its pairwise session is stale, so
  every sealed probe to it is dropped), or a path that went away all stop
  answering altogether, and none of them is a narrow path.
- **A dead member does not have to be waited out.** `uc2ctl remove <dead-id>`
  is accepted while a gate is pending — admin handling keys on the leader flag,
  not on the gate — and removing the member takes it out of discovery's minimum.

`Uc2PathBelowMtu` (`increase(uc2_send_emsgsize_total[5m]) > 0`) is the
runtime face of the same condition on a *running* node: a path degraded below
the committed rung, and the rung cannot be lowered. Fix the path.

## 7. The dev-time check

A dev box's cluster runs over loopback (MTU 65 536), so discovery reaches the
top rung and an oversize command succeeds locally, then fails on a 1500 B
production path. Two guards:

**The warning.** The first time any client submits a command above the
standard 1312 B ceiling, it emits one warn-level record, once per client:

```json
{"ts_ns":…,"level":"warn","event":"command_over_standard_ceiling","len":2048,
 "standard":1312,"ceiling":8864,"remedy":"this cluster carries it, but every
 deployment will need jumbo-frame support on all node paths: set
 force_jumbo_frames = true in node.toml so a cluster without it refuses to
 start instead of failing at submit"}
```

(One line in the real file; wrapped here. The `ceiling` field is the shmem
tier's only — `uc_remote` omits it, because protocol v1 advertises none.)

Grep your logs for `command_over_standard_ceiling` in CI. Two things to know
when you do:

- A command relayed through `uc2-gateway` emits the line **in the gateway's
  log too**, because the edge submits through `uc_client::Engine` like any
  other local client. It is the client's command that is oversize, not
  anything the relay did.
- `uc_remote` measures the **bare** command, while the node's door sees the
  command plus the 16-byte session envelope when the edge has sessions on. A
  command within 16 B of the threshold can therefore cross it at the node
  without warning at the remote client. Protocol v1 gives the client no way to
  know whether the envelope is on.

**The hard pin.** On the `Engine` tier, set the client door to the standard
ceiling so an oversize command is an error rather than a warning:

```rust
use uc_client::{Engine, EngineConfig};
use uc_protocol::v2::datagram::MAX_PAYLOAD_DEFAULT;

let cfg = EngineConfig {
    // Pin the door: refuse anything a 1500 B-path cluster could not carry.
    max_payload: Some(MAX_PAYLOAD_DEFAULT),   // 1312
    ..Default::default()
};
let (send, poll) = Engine::attach(dir, "myapp", cfg)?;
```

`max_payload: None` (the default) follows the node's live ceiling per submit,
which is what production wants. The blocking `Client` and `PipelinedClient`
tiers do not expose the knob — they always follow the node — so on those tiers
the warning line is the signal to fail your build on.

The node-side counterpart is `uc2_commands_over_standard_total`: nonzero means
this deployment now depends on jumbo-frame support.

## Where to go next

- [Jumbo frames and path-MTU discovery, explained](../notes/uc2-jumbo-frame-discovery-explained.md)
  — the design argument, the errata, and the security note.
- [Upgrade a cluster](upgrade-a-cluster.md#wire--cnc-change-in-2120-jumbo-frames-080-cnc-32)
  — the `2.12.0` flag day: no wipe, delete `max_payload`, the DF behaviour
  change.
- [Monitor a cluster](monitor-a-cluster.md) — where the eight series and the
  two alert rules sit among the rest.
- [Limits](../reference/limits.md#hard-limits) — the ceiling rows as a ladder.
- [Threat model](../security/threat-model.md) — why a jumbo cluster on an
  untrusted network wants `[crypto].enabled = true`.
