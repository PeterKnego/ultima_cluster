# UC v2 — Jumbo frames: path-MTU discovery and a replicated payload ceiling

**Date:** 2026-09-10
**Status:** design brainstormed in chat 2026-09-10 in seven sections, each
approved by the maintainer as presented ("go"). Next: the implementation plan.
**Baseline:** local `main` at `f46db43`. `v2.11.0` is tagged and published
(wire `0.7.0`, cnc `3.1`). Nothing under `uc_protocol` has moved since the
tag, so `2.12.0` is not yet a flag day; this design makes it one.
**Requested by:** the maintainer, 2026-09-10 ("we have a hard limit on max
command size based on max mtu: ~1300B. since jumbo frames are available on
target clouds (aws, gcp), UC should be able to check at startup the max MTU
between nodes and set the max size accordingly"), with two additions in the
same conversation: an optional `force_jumbo_frames` knob that turns a
shortfall into a startup failure, and a developer-facing notification when a
command exceeds the standard ceiling. Anticipated by `CLAUDE.md`'s "Next up"
paragraph ("`2.12.0` … is the natural home for jumbo frames") and by
`docs/BACKLOG.md` item 1, which lists the ceiling as one of the two questions
a real workload would settle.
**Release:** `2.12.0`, together with the monotonic log clock already on
`main`. Wire `0.7.0` → `0.8.0`, cnc `3.1` → `3.2`.

## 1. Goal and locked decisions

Today one command must fit one UDP datagram, and the datagram budget is a
source constant: `MTU_DEFAULT = 1408` in `uc_protocol::v2::datagram`, sized
to clear a 1500 B Ethernet path. That fixes the command payload ceiling at
1344 B with wire crypto off and 1312 B with it on (`docs/security/
attack-surface.md` §3). The main deploy targets carry far more — AWS VPCs
9001 B, GCP VPCs up to 8896 B — and UC leaves it on the table.

Locked in the brainstorm:

1. **Discovery, not configuration.** UC measures the path MTU between nodes
   and derives the ceiling from what every path actually carries. There is
   no knob that states the number by hand; the existing `max_payload` key in
   `node.toml` is refused by name.
2. **One value for the whole cluster**, owned by the leader and replicated
   through the cluster FSM's Settings record. The reasons are §2.
3. **Safe by default.** A cluster starts at today's 1408 B baseline and rises
   only when every member has proven a larger path. A cluster on a 1500 B
   path behaves exactly as it does today; nothing an operator does now
   breaks.
4. **`force_jumbo_frames = true`** makes discovery a startup gate: a node
   that cannot prove a jumbo path to every configured member fails to start,
   by name, with two distinct refusals for "answered too small" and "never
   answered".
5. **The socket sets do-not-fragment.** An oversize datagram fails locally
   and is counted, instead of fragmenting silently.
6. **A developer notification** fires at submit time when a command exceeds
   the standard ceiling, even when the cluster carries it — because a dev
   box's loopback carries everything.

## 2. The constraint that shapes the design

Three facts from the code decide the shape.

**The receiver already accepts any size.** `uc_net`'s receiver recv buffer is
64 KiB (`receiver.rs:1281`). Raising the datagram budget changes no header
and no layout; a node parses a 9 KB datagram today. Discovery is purely a
sender-side and agreement problem.

**The ceiling must be one value cluster-wide, settled before the first
large frame is appended.** Two reasons:

- A client on node A submits a command at A's ceiling. The leader B must
  send it to every member. If B's path to C is narrower, the command is
  admitted and cannot be replicated.
- Once a large frame is in the log, every future leader must be able to
  ship it to every member — as live DATA, as a NAK repair, and as the tail
  a snapshot session replays. A member behind a narrower path can never
  receive it.

So the sound value is the minimum over **all node pairs**, including
follower-to-follower paths the leader never sends on, and it can only ever
go up within a cluster's life.

**Nothing sets DF today.** `grep` finds no `IP_MTU_DISCOVER`,
`IPV6_DONTFRAG` or socket-option call under `uc_net/src` or `uc_node/src`.
A probe that does not set DF would be answered on a 1500 B path after the
kernel fragmented it, and would report success for a size the path does not
carry. Both 2026-08-02 briefs (`2026-08-02-uc2-envelope-map-brief.md` §7.2,
and hi-perf-cmp's `2026-08-02-network-payload-streaming-design.md`) called
for a DF probe for exactly this reason.

## 3. Approaches considered

**A. An operator knob (`mtu = 8960` in `node.toml`), default 1408.** The
smallest change: plumb a config key into preflight and `SenderConfig.mtu`,
which already exists and is never set. Rejected by the maintainer: the
operator has to know the number, and a mis-set node on one narrower path is
a silent replication failure. Kept as the mental model for what the
discovered value replaces.

**B. A one-shot probe at startup, decided locally per node.** Matches the
words "check at startup", but fails on every cold start where peers come up
in sequence (each node decides before its peers answer), and cannot give the
cluster one value (§2). Rejected.

**C. Leader-driven discovery, replicated ceiling — chosen.** Every node
probes its peers with a DF ladder and answers its peers' probes. The leader
aggregates, and when every member has answered it commits the minimum
through the existing replicated Settings record. Restarts recover the
committed value from the cluster artifact, so boot order does not matter.
`force_jumbo_frames` layers a startup gate on top for operators who need
the guarantee before serving.

## 4. Wire

### 4.1 Rungs

Discovery tests a fixed ladder of **UC datagram sizes** (the UDP payload),
not a binary search: deterministic, testable, one const to extend.

| rung | fits (IPv4, 28 B IP+UDP) | fits (IPv6, 48 B) | ceiling crypto-off | ceiling crypto-on |
|---|---|---|---|---|
| 1408 | 1500 B Ethernet (1472) | 1500 B (1452) | 1344 | 1312 |
| 8832 | GCP 8896 (8868) | GCP 8896 (8848), AWS 9001 (8953) | 8768 | 8736 |
| 8960 | AWS 9001 (8973) | — | 8896 | 8864 |

Ceilings are `max_payload_for_mtu(rung)` with and without `CRYPTO_OVERHEAD`;
the const fn exists since 2026-09-08 and this design adds nothing to its
arithmetic beyond a crypto-aware sibling, `payload_ceiling(rung, crypto_on)`,
that the runtime readers in §7 and §9 call. The 8832 rung exists so that GCP over IPv4 and both clouds over
IPv6 land on a jumbo rung; the 8960 rung is AWS over IPv4, the fleet's
case. `RUNGS: [u32; 3] = [1408, 8832, 8960]` lives in `uc_protocol::v2::
datagram`; `MTU_DEFAULT` stays 1408 and is `RUNGS[0]`; `MTU_BOUND =
RUNGS[last]` is the largest datagram UC will ever send.

**"Jumbo" for `force_jumbo_frames` means the 8832 rung or better** — the
lowest rung both clouds carry on both address families.

### 4.2 Two new datagram kinds

- **`DGRAM_KIND_PROBE = 24`.** Body: `rung: u32` followed by zero padding so
  the whole datagram is exactly `rung` bytes long (with crypto on, the
  sealed length is `rung`; the cleartext body is `rung` less the crypto
  overhead). A responder credits a probe **only if the received length
  equals `rung`** — a truncated or reassembled arrival is not a proof.
- **`DGRAM_KIND_PROBE_ACK = 25`.** Body: `rung: u32 ‖ own_min_rung: u32`,
  8 bytes. `rung` is the probe being acknowledged; `own_min_rung` is the
  responder's own verified minimum over **its** configured peers (§5.3), so
  the leader learns every pair's result without a second exchange.

Both are `Scope::Pairwise` in `uc_crypto::Transport::scope_of`, like every
one-to-one control kind. Under wire crypto a forged ack cannot raise the
ceiling; with crypto off the plane has no integrity anyway, which the threat
model already states. A probe cannot be sent before the pairwise session
exists; the retry cadence (§5.1) absorbs the handshake.

Wire `0.7.0` → `0.8.0`. A `0.7.0` peer counts kinds 24/25 as unknown and
drops them; a mixed cluster therefore never raises its ceiling, which is
safe, but mixed clusters are unsupported regardless (flag-day rule).

### 4.3 Do-not-fragment

The replication socket sets `IP_MTU_DISCOVER = IP_PMTUDISC_DO` (IPv4) or
`IPV6_DONTFRAG = 1` plus `IPV6_MTU_DISCOVER = IPV6_PMTUDISC_DO` (IPv6) once
at bind, via `libc` (already a workspace dependency; `uc_net` gains it). Two
consequences:

- A datagram larger than the route MTU fails immediately with `EMSGSIZE`
  and never leaves the host. A datagram larger than a downstream hop is
  dropped there, and the kernel's path-MTU cache makes the *next* send fail
  with `EMSGSIZE` if the hop's ICMP reaches us. Either way a probe is
  unacked and a data send is a counted failure, never a silent fragment.
- **One behaviour change:** a path below 1436 B (IPv4) or 1456 B (IPv6)
  that works today by fragmenting the 1408 B baseline will now fail by name
  (`uc2_send_emsgsize_total` rises, `Uc2PathBelowMtu` fires — §9). The
  baseline is UC's contract: below it a full schedule table does not fit.

Probe sends that fail with `EMSGSIZE` are expected on a narrow path and
count in `uc2_probe_sent_total` only, never in `uc2_send_emsgsize_total`.

## 5. Discovery

### 5.1 The prober (every node)

Each node keeps a per-peer entry `{verified: u32, attempts: u32}` for every
configured member other than itself — the recovered membership at boot, and
the FSM's committed membership as it changes (new members are added with
`verified = 0`, removed members dropped). `verified` is the largest rung
whose probe was acked; a peer that has never acked reads `0`.

Cadence: on boot and on every membership change, one probe per rung per
unresolved peer. Retry every **1 s** for the first five attempts, then every
**30 s**, until the peer's `verified == MTU_BOUND` (resolved; probing stops
for that peer). Loss of a single big probe therefore costs one second, and a
permanently narrow path costs two datagrams per 30 s per peer.

Probes ride the sender agent's pass (it owns the socket's send side and the
pairwise seal path); acks are handled by the receiver agent, which updates
the peer entry and, if the ack's `own_min_rung` is present, records it as
the peer's advertised minimum (§5.3). The receiver also **answers** probes:
on a `PROBE` whose received length equals its `rung`, it queues a
`PROBE_ACK` for the sender agent to seal and send.

### 5.2 A node's own minimum

`own_min_rung = min over configured peers of verified`, where an unresolved
peer contributes `0`. It is what the node puts in every ack it sends, what
`uc2_probe_min_mtu_bytes` exports, and what §5.4 and §6 compare against.
Because an unresolved peer pins it to `0`, a node advertises a jumbo minimum
only when it has heard from **every** peer at that rung or better.

### 5.3 The leader's table and the commit rule

The leader (only) also keeps `advertised: u32` per member from the latest
ack's `own_min_rung`, and computes per member
`table[m] = min(verified[m], advertised[m])`.

**Commit rule**, evaluated once per consensus pass while leading: if every
current member (voters and learners, excluding self) has a table entry, and
`min over table > committed_rung`, append a `CLUSTER kind = 3` Settings
frame that is the committed Settings with `datagram_mtu` raised to that
minimum. Single-in-flight applies as for every CLUSTER command (retry next
pass while one is above commit). The value is monotone by construction here
and in the FSM (§5.5); a table minimum below the committed rung is a
degraded path and is reported (§9), never acted on.

A learner counts. Replication reaches learners, so a learner behind a
narrower path would receive nothing above its rung.

### 5.4 Restart and join

When a node knows a committed rung `R > MTU_DEFAULT` — from the cluster
artifact in its instance dir at boot, or from the artifact a snapshot
session installs — it compares `own_min_rung` against `R` once its ladder
has settled (five attempts, ~5 s, or the 30 s gate window under
`force_jumbo_frames`). `own_min_rung < R` is a startup fail-stop,
**`PathBelowCommittedMtu { peer, committed, carried }`**, naming the first
peer that fell short. The remedy is the path, and the how-to says so. A
joiner behind a narrower path therefore never joins, which is the only
outcome compatible with §2: the log already holds frames it cannot receive.

### 5.5 Settings record, version 2

`uc_protocol::v2::settings::Settings` gains `datagram_mtu: u32` (`0` =
baseline, i.e. `MTU_DEFAULT`). Layout, frozen once shipped: version `u32`
@0, `fsm_lag_bytes` `u64` @4, `admission_bytes` `u64` @12,
`snapshot_interval_bytes` `u64` @20, `snapshot_target` `u8` @28,
`datagram_mtu` `u32` @29; `SETTINGS_LEN = 33`, `SETTINGS_VERSION = 2`.

- **Decode accepts version 1** and maps it to `datagram_mtu = 0`. This is
  the first flag day in which a cluster artifact and committed CLUSTER
  frames persist across the upgrade (the cluster FSM shipped in `2.11.0`),
  so `upgrade-a-cluster` must not require a wipe. Encode always writes v2.
- **Validation** (`ClusterFsm::validate`, leader's door): `datagram_mtu`
  must be `0` or a member of `RUNGS`; anything else is refused with reason
  47 `settings_bounds`.
- **The FSM keeps the rung monotone.** On applying a Settings record the FSM
  stores `datagram_mtu = max(committed, incoming)`. Deterministic across
  replicas, and it is what protects the rung from `uc2ctl settings apply`,
  which encodes a whole record from an operator file whose absent keys mean
  `0`: the operator's file cannot lower the rung, and it does not need to
  know it.
- **Not operator-writable.** `uc_ctl::settings::SettingsFile` gains no
  `datagram_mtu` key; its `deny_unknown_fields` refuses one. Likewise
  `node.toml`'s `[settings]` genesis section.

`uc2ctl settings show` prints `datagram_mtu` with its meaning (`1408
(baseline)` or `8960 (discovered)`).

## 6. `force_jumbo_frames`

A top-level `node.toml` key, `bool`, default `false`, with the env override
`UC2_FORCE_JUMBO_FRAMES` in the existing override table.

When true, the node runs its agents as usual but holds `can_serve` false and
does not answer readiness until the gate passes. The gate passes when
`own_min_rung >= 8832` (§4.1). It fails when the **30 s** window
(`JUMBO_GATE_WINDOW`, a constant, not configurable) elapses first, and the
node fail-stops through the daemon's existing named-exit path
(`uc2-node` exit code 1 plus an `obs_event`), with one of two refusals:

- **`JumboPathTooNarrow { peer, carried }`** — the peer answered, but its
  best acked rung is below 8832. `carried` is that rung.
- **`JumboPeerSilent { peer, waited_secs }`** — the peer never acked any
  rung within the window. This is a liveness fact, not an MTU fact, and is
  worded as one.

If several peers fall short the refusal names the first by member id and
the log line lists all of them. §5.4's `PathBelowCommittedMtu` still applies
under the flag and takes precedence when a committed rung exists.

The gate is post-bind by necessity: probing needs the socket, and under
wire crypto it needs the pairwise handshake the agents run. It is therefore
not a `PreflightError` but a startup fail-stop, the same posture as the
ENOSPC path. Only `max_payload`'s retirement (§7.4) is a preflight refusal.

Why the flag exists at all (maintainer, 2026-09-10): an application whose
commands exceed the standard ceiling must not be deployable onto a cluster
that cannot carry them. Without the flag such a cluster starts, serves, and
refuses those commands one by one at submit; with it the cluster refuses to
start and the operator fixes the infrastructure first.

## 7. Runtime plumbing

### 7.1 The live rung

The cluster agent stores the committed `datagram_mtu` (or `MTU_DEFAULT` for
`0`) into one `AtomicU32` owned by the node, `live_rung`, at every Settings
apply. Three readers:

- **The sender's datagram budget** becomes `live_rung - DATAGRAM_HEADER_LEN
  - crypto_overhead`, replacing `cfg.mtu` at its four budget sites
  (`sender.rs:988, 1193, 1263, 1634`: DATA packing, two NAK-serve paths,
  snapshot chunks). `run`/`scratch` are sized at `MTU_BOUND`.
- **A single frame larger than the live budget is sent alone**, in one
  datagram bounded by `MTU_BOUND`, never packed with others. Such a frame
  exists in the log only because a committed rung admitted it, so the path
  is proven; this closes the window in which a new leader's cluster agent
  has not yet applied a Settings commit the old leader already acted on. The
  `Sender::new` assert ("a max-size frame must fit one datagram") checks
  the log buffer's bound against `MTU_BOUND`.
- **The leader's ingress door and `Appender::append`** refuse a payload
  above `payload_ceiling(live_rung, crypto_on)`. The appender's check
  goes from a field read to one `Relaxed` load per append; §10 measures it
  rather than assuming.

### 7.2 The log buffer's bound

`LogBuffer::new(region, cnc, max_payload)`'s parameter becomes the **bound**,
`max_payload_for_mtu(MTU_BOUND)` = 8864 in production, and keeps its role in
the capacity assert (≥ 4× max claim: 71 680 B, trivially met by the 64 MiB
default) and as the header's `max_payload` word. Tests that build tiny
buffers keep passing a small bound. `MIN_FSM_LAG_BYTES` stays derived from
`MTU_DEFAULT` — the cluster-wide floor is still one baseline frame, and a
host clamps up to its own one-frame floor at the point of use as today. The
`SNAP_BEGIN` body-budget assert stays against `MTU_DEFAULT`, since a
`SNAP_BEGIN` must fit the smallest rung.

### 7.3 The cnc page: a live ceiling word, cnc 3.2

`CNC_OFF_PAYLOAD_CEILING = 3984`, `u64`, the third word of the 3968 line
(the two hole counters occupy 3968/3976; the line has 48 free bytes, and
page 1's last line is full). Writer: the cluster agent at Settings apply;
init at boot: the baseline ceiling for this node's crypto mode. Readers:
`uc_client::Engine` per submit (one `Acquire` load, replacing the
attach-time copy of the header's `max_payload`, which is now the bound) and
`uc_gateway`'s edge per submit (`edge.rs:1328`, same replacement). A client
attached before the raise therefore sees it. `CNC_V2_VERSION` → 3.2; a 3.1
attacher refuses by version as the 3.0→3.1 note describes. Offsets are
pinned in both `uc_protocol` and `uc_log` with the usual assertion tests.

### 7.4 What is retired

- **`max_payload` in `node.toml`** — `PreflightError::MaxPayloadRetired`,
  refused by name pointing at discovery and at `force_jumbo_frames`, the
  same posture as `admission_bytes`'s move in `2.11.0`. The m9 fleet gate's
  `payload-over-mtu` refusal row becomes `max-payload-retired`, still
  asserting the refusal names `max_payload`.
- **`PayloadExceedsMtu` / `PayloadTooSmallForScheduleTable`** — both were
  checks on the operator's number; with no number there is nothing to
  check. The schedule-table floor is already a `const _` assert against
  `MAX_PAYLOAD_DEFAULT` in `datagram.rs`.
- **The hard-coded `1344` in `uc_remote/src/engine.rs:131-217`** (inflight
  buffer sizing) becomes the bound's crypto-off ceiling, 8896, so a remote
  client can pipeline max-size jumbo commands.

`MTU_DEFAULT`, `MAX_PAYLOAD_DEFAULT` and the `== 1312` assert stay; they are
the baseline every cluster starts from.

## 8. Developer notification

The trap: a dev box's cluster runs over loopback, whose MTU is 65 536, so
discovery lands on the top rung and a 4 KB command succeeds locally, then
fails on a 1500 B production path. The notification must fire on success.

- **Where.** At submit, in `uc_client::Engine` and in `uc_remote::
  RemoteClient` — the one place every command's byte length is known before
  it enters the ring, in both tiers (the typed tier has already encoded).
- **What.** The first time a client sends a command above
  `MAX_PAYLOAD_DEFAULT` (1312 B, the crypto-on baseline, which holds on any
  cluster), it emits one warn-level `uc_obs` line, once per client, and
  increments `commands_over_standard`. `uc_client` gains `uc_obs` as a
  dependency (a dependency-free leaf, so the "small dep set" posture holds).
  Wording:

  > `command_over_standard_ceiling`: command is 2048 B, above the standard
  > 1312 B ceiling. This cluster carries it (discovered ceiling 8864 B), but
  > every deployment will need jumbo-frame support on all node paths. Set
  > `force_jumbo_frames = true` in `node.toml` so a cluster without it
  > refuses to start instead of failing at submit.

  The shmem client knows the live ceiling and prints it; `RemoteClient` does
  not (protocol v1 advertises none) and omits that clause.
- **When the cluster cannot carry it.** The existing `PayloadTooLarge`
  (client) and `RETRY_PAYLOAD_TOO_LARGE` (edge) refusals gain the same
  remedy text. Today they state only the number.
- **Hard fail in tests.** `ClientConfig::max_payload: Option<usize>` already
  exists as an override; pinning it to `MAX_PAYLOAD_DEFAULT` in CI turns any
  oversize command into an error. The how-to documents this as the dev-time
  check.

A declared `MAX_COMMAND_BYTES` on the state-machine trait, checked at
service attach, was considered and rejected: on a fresh cluster the ceiling
is still the baseline at attach time, so the check would race discovery.

## 9. Observability

Metrics (all on the existing `/metrics` endpoint):

| series | kind | meaning |
|---|---|---|
| `uc2_datagram_mtu_bytes` | gauge | the committed rung this node applies (1408 until discovery commits) |
| `uc2_payload_ceiling_bytes` | gauge | `payload_ceiling(live_rung, crypto_on)`, the same value as cnc 3984 |
| `uc2_probe_min_mtu_bytes` | gauge | this node's `own_min_rung` (§5.2); `0` while any peer is unresolved |
| `uc2_probe_sent_total` / `uc2_probe_acked_total` | counters | ladder activity, includes EMSGSIZE'd probes in `sent` |
| `uc2_send_emsgsize_total` | counter | non-probe sends refused by the kernel for size; must be 0 on a healthy cluster |
| `uc2_commands_over_standard_total` | counter | frames appended above `MAX_PAYLOAD_DEFAULT` — the ops-side view of §8 |

Two alert rules (23 → 25 in `packaging/prometheus/uc2-alerts.yml`, each with
a `RULE_BUILDERS` scenario in `scripts/m10_alert_fire.sh` so its
completeness cross-check keeps passing):

- **`Uc2MtuDiscoveryStalled`** — `uc2_probe_min_mtu_bytes >
  uc2_datagram_mtu_bytes` for 60 s on any node: this node has proven more
  than the cluster has committed, so some member is holding discovery back
  (silent, or narrower).
- **`Uc2PathBelowMtu`** — `increase(uc2_send_emsgsize_total[5m]) > 0`: a
  path has degraded below the committed rung (or below the baseline, §4.3).
  This is the runtime face of the outage mode §5.4 refuses at startup.

`uc2ctl status` shows `ceiling: 8864 B (rung 8960, discovered)` or `ceiling:
1312 B (rung 1408, baseline)`, and `commands over standard: N`. Discovery
commits are visible as ordinary `CLUSTER` Settings frames and are audited
as `settings_apply` with `source = "discovery"` (today's operator applies
carry `source = "operator"`).

## 10. Proof

**Unit (`uc_protocol`, `uc_net`, `uc_node`):** probe/ack encode/decode and
the exact-length rule; Settings v2 layout frozen, v1 decodes to
`datagram_mtu = 0`, v2 rejects a non-rung; the FSM's `max()` rule; the
leader's commit rule (all members present, min > committed, learners
count, an unresolved peer pins `own_min_rung` to 0); the ladder's cadence
and stop condition; the sender's "oversize frame goes alone" rule; a
`PreflightError::MaxPayloadRetired` refusal by name.

**Fault layer.** `uc_net::fault::FaultConfig` gains `max_datagram: Option<
usize>`: a send above it is dropped, the way a DF'd datagram vanishes at a
narrow hop. With it an in-process three-node test (`uc_node/tests/`) proves:
(a) discovery lands on exactly the capped rung on every node; (b) the
ceiling never rises while one member is silent; (c) under
`force_jumbo_frames` a capped path yields `JumboPathTooNarrow` naming the
peer, and a never-started peer yields `JumboPeerSilent`; (d) a restart
against a committed rung above the cap yields `PathBelowCommittedMtu`;
(e) a client attached before the raise sees the new ceiling through cnc 3984
and a 4 KB command round-trips after it.

**Fuzz.** One new target, `uc_protocol_probe`, over probe/ack decode and the
Settings v1/v2 decoder (24 targets). Both decoders are pure and join the
Miri tier for free.

**Sim.** `uc_sim` does not model datagram size and this design adds nothing
there; the rung is ordinary Settings data whose FSM-versus-kernel invariants
`inv12` already sweeps. Stated here so `docs/VERIFICATION.md` records the
gap honestly: the path-MTU logic is proven by the fault-layer test and the
fleet, not the sim.

**Fleet gate** — `docs/benchmarks/uc2-jumbo-frame-discovery-gate-<run date>.md`,
bars pre-committed here, honest-failure protocol as always:

| row | arm | bar |
|---|---|---|
| a | AWS, 3 voters, interface MTU 9001 as provisioned | every node reports `uc2_datagram_mtu_bytes = 8960` within 10 s of the last node's start, 3 of 3 reps |
| b | 1500 B path (interface MTU forced to 1500 by ansible on the same fleet) | rung stays 1408 on every node; `uc2_send_emsgsize_total = 0` throughout; 64 B throughput paired delta against the base tree within −3 %, pair count fixed from the base tree's observed spread per the 2026-08-31 lesson, minimum 5 pairs |
| c | the envelope-map brief's §6 disposition, run as written on the AWS arm | jumbo soak plateau ≥ 15 % over standard **and** the jumbo 64 B rung within −3 % of standard (throughput and p99) — this decides whether the runbook *recommends* jumbo, not whether the feature ships |
| d | force gate on the 1500 B arm; then one node held down on the 9001 arm | all three refuse `JumboPathTooNarrow` within 30 s naming a peer; the two live nodes refuse `JumboPeerSilent` naming the third |
| e | appender relaxed load | `apply_bench`-style isolated A/B is the wrong harness (the appender is leader ingress); `m5_gate` on the fleet, standard arm, paired against the base tree — reported, no bar: rate bars for a one-load change cannot be resolved by this rig (the identity gate's row a straddled the same bar), and the number is recorded for the next gate to pair against |
| f | client-hop cost of the per-submit cnc load | `scripts/hop1_ab.sh` with its same-source rebuild control, dev-box **smoke**, reported with the control's resolution, no bar |

Row c carries a path-MTU blackhole probe before the arm, as the brief
requires: with §4.3 in place that probe is UC's own ladder, and a jumbo arm
whose nodes report `uc2_datagram_mtu_bytes = 1408` after 30 s aborts the
arm loudly.

## 11. Docs

Written before the tag, per the release rule:

- **New:** `docs/notes/uc2-jumbo-frame-discovery-explained.md` (the
  explainer: why one value, why DF, the ladder, the monotone rule, the dev
  trap) and `docs/how-to/jumbo-frames.md` (enable on AWS/GCP, verify with
  `uc2ctl status` and the two gauges, `force_jumbo_frames`, the CI pin,
  what `PathBelowCommittedMtu` means and the remedy).
- **Rewritten statements:** `CLAUDE.md`'s "Command payload ceiling"
  standing fact (a rung table, not one number, and the flag-day sentence
  updated: raising the ceiling is no longer a source change);
  `docs/reference/limits.md` rows 27–30; `docs/security/attack-surface.md`
  §3; `docs/reference/configuration.md` (`max_payload` → retired,
  `force_jumbo_frames` added, `[settings]` note); `docs/reference/
  wire-protocol.md` (kinds 24/25, DF, `0.8.0`, the ceiling paragraphs at
  242/299/392); `docs/reference/cnc-page.md` (3984, cnc 3.2);
  `docs/reference/remote-protocol.md` § payload; `docs/reference/
  semver-policy.md` (the `2.12.0` flag-day paragraph, Settings v1 accepted
  on read); `docs/how-to/upgrade-a-cluster.md` (a `2.12.0` section: no
  wipe, delete `max_payload`, the DF behaviour change);
  `packaging/node.example.toml`; `docs/ops/uc2-runbook.md` (status output,
  the two alerts); `docs/VERIFICATION.md` (the fault-layer tier and the sim
  gap); `docs/BACKLOG.md` item 1 (the ceiling question is answered by
  discovery; the remote-protocol-v2 question stands).
- **Release:** `RELEASES.md` section for `2.12.0` (this feature and the log
  clock, the fixed bugs already on `main`), `docs/releases.md` entry.

## 12. Out of scope

- **Fragmenting a command across datagrams.** Still the brief's "regime 3"
  feature, still unjustified. A command above the discovered ceiling is
  refused, never split.
- **Lowering the rung at runtime**, or re-probing after a path degrades. A
  degraded path is an outage (`Uc2PathBelowMtu`); the log's frames are
  permanent facts about what every member must carry.
- **A configurable rung set or gate window.** Both are constants; a fourth
  rung is a one-line change when a fabric justifies it.
- **Making the rung the operator's choice.** Rejected in §3.
- **A remote-protocol advertisement of the ceiling.** `RemoteClient` warns on
  the standard threshold alone; a protocol v2 is a separate backlog item.

## Errata (plan 1, as built)

1. **§5.1's stop condition is incomplete.** As shipped, probing a peer does
   not stop merely when `verified == MTU_BOUND`: it stops only when
   `verified == MTU_BOUND` **and** that peer's advertised minimum
   (`own_min_rung`, §5.2/§5.3) has also caught up to the top rung. Until
   both hold, the node keeps re-probing that peer — at its already-verified
   rung, not from scratch — because an ack's `own_min_rung` is the *only*
   way the leader learns a peer's own minimum: the leader has no other
   channel to that fact, so it can only learn it from acks to its own
   probes. On a cluster with one permanently narrow path this means every
   node keeps probing every peer at the 30 s cadence forever (2–3
   datagrams per 30 s per peer) — an accepted cost, not a bug.
2. **A rejoin case §5.1 does not mention.** A `PROBE` received from a peer
   the node had backed off on (i.e. was probing at the 30 s cadence)
   resets the node's cadence toward that peer back to the 1 s/five-attempt
   schedule. This is what makes a rejoining member's MTU raise land in
   seconds rather than waiting out the full 30 s backoff window.
3. **§7.2's `MIN_FSM_LAG_BYTES` framing needs a correction.** The lockstep
   `fsm_lag` one-frame floor follows the **live, discovered** ceiling, not
   the fixed `MTU_DEFAULT`-derived `MIN_FSM_LAG_BYTES` bound described
   there. `MIN_FSM_LAG_BYTES` (1376 B) remains the cluster-wide floor the
   leader's `validate` refuses below — it must hold at the worst case,
   every cluster's baseline rung — but a host's own one-frame clamp at the
   point of use (`uc_node::services::fsm_lag_from_setting`) reads that
   host's live `payload_ceiling`, not a fixed `max_payload`. On a jumbo
   cluster this means the per-host clamp can go **up** relative to
   `MIN_FSM_LAG_BYTES` (as far as 8928 B at the top rung), never down. See
   `uc_protocol::v2::settings::MIN_FSM_LAG_BYTES`'s doc comment for the
   as-built wording.
4. **§5.3: an empty member set yields no commit — a solo cluster stays at the
   baseline; discovery starts when the first peer joins.**
   `ProbeTable::table_min(&[])` returns `None`, not `Some(MTU_BOUND)`. An
   empty set is *no evidence*, not universal evidence: a one-node cluster has
   measured nothing, and the rung is monotone in the FSM, so a top-rung commit
   on zero measurements is irreversible. It would break the grow-from-one path
   — start one node, `add-learner`, promote — on any standard-MTU network,
   because the joiner's snapshot chunks would be cut at the leader's 8960 B
   budget and never arrive, leaving a wipe as the only remedy.
   (`own_min_rung()` is unchanged: it still answers `MTU_BOUND` for an empty
   peer map, which is why §5.1's boot ordering seeds the peer set before either
   agent is given the table.)
5. **§7.1/§7.3 name the wrong agent as the writer.** They say the cluster
   agent writes `live_rung` and the cnc word at 3984; as built the
   **consensus** agent writes both, in `Consensus::refresh_from_view` — the
   cluster agent publishes the view and the consensus agent reads it, which is
   what keeps the appender's door and the door's writer on one thread.
   `docs/reference/cnc-page.md` already documents it that way.
