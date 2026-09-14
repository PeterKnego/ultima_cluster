# Rolling upgrades — what it would take to make the wire and the cnc page backward compatible

*Written 2026-09-13 against the `2.12.0` tree (wire `0.8.0`, cnc `3.2`;
released the same day, tag `v2.12.0`). The question: what would it take for `ultima_cluster` to be upgraded
one node at a time instead of on a flag day? Repo evidence is cited by
`path:line` (line numbers as of the tree this was written on) or commit; external claims cite the first-party page they were read from.
Anything not checked is marked "not verified". Status: an assessment, not a
spec — the recommendation at the end is a starting position for
`docs/BACKLOG.md` item 3, which already names this gap.*

## The shape of the problem

A rolling upgrade is not "the new binary tolerates the old datagrams". It
is three separate promises, and UC keeps none of them today:

1. **Two wire versions on one UDP plane at once.** Every kind every node
   emits must be either understood or *safely ignored* by every other node,
   for the whole window.
2. **Two writers in one log.** The leader changes during the window, so the
   log — persisted in the journal, replayed by every service on restart,
   shipped as opaque byte runs in `DATA` datagrams — holds frames stamped by
   both versions. `0.7.0`'s header relayout (`48a2bc7`, same flag day as
   `0bb7c65`) is a change no rolling scheme absorbs without a per-frame or
   per-term layout tag, because a `0.6.0` frame *parses* on a `0.7.0` node and
   means something else (`docs/reference/wire-protocol.md:284-286`).
3. **One cluster FSM, two decoders.** Since `2.11.0` cluster data is applied
   at commit by every node (`uc_node/src/cluster_fsm.rs:349-359`). A record
   the old binary cannot decode is refused with reason 42 *and `applied`
   still advances* — so a mixed cluster does not stall on it, it **silently
   diverges** (the new nodes adopt the record, the old ones drop it). This is
   the hazard specific to UC's design, and the reason a committed "cluster
   wire level" is the only workable shape (§5, Level 2).

The version constant itself enforces nothing: `uc_protocol/src/version.rs:32-46`
says so in its own words ("NOT on any live enforcement path … `CURRENT`
documents the semver of the wire *datagram* protocol but does not itself
enforce anything"), and `CURRENT = 0.8.0` at `:81` has no caller outside the
file. The one version that IS gated is the cnc word, at local IPC attach
(`uc_log/src/cnc.rs:486`).

## What the docs already say

- `docs/reference/semver-policy.md:210-226` — "A change to either is a
  **flag day**: every node in a cluster is stopped and restarted on the new
  version together. Mixed-version operation is not supported and is not made
  safe by the version numbers agreeing on a major." `:262-265` repeats it for
  `2.12.0`.
- `docs/how-to/upgrade-a-cluster.md:8-20` — "**There is no rolling or
  partial mode.** This is not a conservative default — it follows directly
  from wire protocol 0.5.0's content-attested durable reports … a
  mixed-version cluster **stalls commits** rather than making an unsound
  one." `:236-244` and `:267-270` give the same-host order (stop clients,
  gateway, services, node; start node first).
- `docs/reference/cnc-page.md:7-9` — "Offsets … do not change within a wire
  protocol major version. New fields are added in the reserved band." (The
  rule is stated; its one-directional consequence is not — §3e.)
- `docs/BACKLOG.md:185-216` — **item 3, "Rolling upgrades and leadership
  transfer"**, ranked, cost "high". Quotes the deferral from
  `docs/superpowers/specs/2026-08-19-uc2-production-readiness-design.md:27`:
  "A one-version-skew negotiation window is real design work — the
  negotiated floor becomes consensus-relevant state — and is explicitly out
  of scope here." The FSM-identity spec (`…/2026-09-02-uc2-fsm-identity-design.md:516-526`)
  reserves the second half of the mechanism: "the log-stamped half, the
  validator, and the rolling-upgrade semantics that follow from them are
  **backlog item 3** … The carrier for that half is a term-boundary log
  event, not `SNAP_BEGIN`."
- `docs/notes/uc2-twelve-factor-assessment.md:98-102` — the flag-day rule is
  named as the residue behind factors 5 and 10, and defended: "a rolling
  deploy that traded that for parity would be the wrong trade."
- The v2 design spec (`docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md:266`)
  takes no position on upgrades; its only version statement is the IPC-entry
  check.

The deferral's reason — "the negotiated floor becomes consensus-relevant
state" — has been answered since by the cluster FSM: the jumbo rung IS a
negotiated, committed, monotone floor, applied as FSM state at commit, not
kernel state (`docs/superpowers/specs/2026-09-10-uc2-jumbo-frame-discovery-design.md:205-262`).
The consensus kernel and the Lean model are untouched by it. That is the
precedent this note builds on.

## The compatibility surfaces today

| # | surface | where | what governs mixed versions today | past changes (class) | rolling-tolerable? |
|---|---|---|---|---|---|
| a | 16 B UDP datagram header | `uc_protocol/src/v2/datagram.rs:104-110` (`position u64`, `term u32`, `kind u8`, `flags u8`, `key_epoch u16`) | no version field. `flags` is written `0` at every send site (`uc_net/src/sender.rs:993,1322,…`) and **never read** (no `h.flags` reader outside the codec). Unknown kinds: `on_datagram` pre-matches probes and consensus kinds (`uc_net/src/receiver.rs:1969-1990`), then the term filter (`:1991-1995`), then `_ => {}` (`:2279`) — silently ignored, **no counter**. Bodies: 13 readers tolerate trailing bytes (`buf.len() < LEN`, e.g. `datagram.rs:185,426,645`); 3 are exact (`SNAP_REQUEST :584`, `SNAP_REDIRECT :615`, `TERM_MAP :720`). | 0.2.0 kinds 16/17 (additive); 0.4.0 `key_epoch` took a reserved slot + kinds 18–20 (reserved-slot use + additive); 0.5.0 `APPEND_POSITION` gained a body, absent = unattested (`receiver.rs:293-296`) (body extension with semantics); 0.8.0 kinds 24/25 (additive) | additive kinds: yes already. Body growth: yes if the old meaning stays acceptable — 0.5.0's did not (unattested = uncounted = stalled), so it was a flag day in disguise. A spare byte exists: `flags`. |
| b | 32 B log frame header | `uc_protocol/src/v2/frame.rs:15-23`; two reserved slots, `u16 @6` and `u32 @20`, written as zero (`:131,135`) | no layout field. `read_header` is `#[inline(always)]` and not total (`:158-171`). Parsed by the apply loop (`uc_service/src/apply.rs:560-612`: MESSAGE, TIMER, SNAPSHOT arms; **any other type falls through and is skipped**), the archive walk (`uc_log/src/archive.rs:169,448,454,465`), the cluster agent (`uc_node/src/cluster_agent.rs:407`), and the receiver, which checks only `PADDING` in a DATA run (`uc_net/src/receiver.rs:384`) | 0.2.0 `CONFIG` type 4 (additive type); 2.11.0 `CLUSTER` reuses 4, `TIMER` 5, `SNAPSHOT` 7 (additive); **`48a2bc7` relayout**: `session_id u64 @16`/`correlation_id u64 @24` → `client_id u32 @12`, `seq u32 @16`, `time_ns u64 @24` (`git show 0bb7c65^:uc_protocol/src/v2/frame.rs:19-22` vs today) | additive types: yes already (skipped by every walker). Relayout: **no** — and the archive seeds the log clock from `time_ns` (`archive.rs:454`), which on a pre-relayout frame is the old `correlation_id`. Needs a layout tag (§5, Level 3). |
| c | wire-crypto envelope | `uc_crypto/src/seal.rs:4,52`: the 16 B header is AAD; Noise `IK` payload = `node_id ‖ boot_salt`, **fixed 20 B, any other length refused** (`uc_crypto/src/handshake.rs:115-120`) | no version or capability in the handshake; `HS_BUF_LEN` has ~4x headroom (`:122-124`). Crypto is all-or-nothing per cluster (`docs/reference/semver-policy.md`, CLAUDE.md) | 0.4.0 introduced it | a handshake payload extension is itself a flag day (fixed length refused). The AAD covering the header is a feature: a version byte in the header is authenticated for free. Crypto-off clusters never handshake, so the handshake cannot be the carrier of a version exchange. |
| d | snapshot session | `SNAP_BEGIN` fixed 120 B, `layout` byte: `V2_RETIRED = 1`, `V3_RETIRED = 2`, `V4 = 3` (`datagram.rs:263-282`) | the one explicit discriminator: `b.layout != V4` → `snap_refused_legacy_peer`, session dropped (`receiver.rs:2295-2306`). Also refuses by **per-row service `VERSION` inequality** (`receiver.rs:2364-2385`) | 0.6.0 26→34 B, 0.7.0 34→122→120 B (body relayouts, each tagged) | the tag makes refusal *named*, not *compatible*: there is one accepted layout. Making it a window (`layout ∈ [min, max]`, sender picks by level) is the ordinary Level-2 work. The per-row version equality means a rolling **service** upgrade also refuses snapshot sessions between hosts on different `const VERSION`s — a policy to revisit (FSM-identity spec §7's "validator"). |
| e | cnc page (`cnc2.dat`) | `uc_protocol/src/v2/cnc.rs:52` (8 KiB), `:64` (`3.2`), `version_compatible :503-509`; sole caller `uc_log/src/cnc.rs:486`; attachers `uc_service/src/attach.rs:76`, `uc_client/src/engine.rs:399` (the gateway rides `uc_client::Engine`), `uc_ctl/src/main.rs:822` | `same major && page.minor <= attacher.minor`: a **newer attacher tolerates an older page; an older attacher refuses a newer page**. Whether a newer attacher *works* on the older page depends on the field: 3.2's `payload_ceiling` reads 0 as "header bound" (`cnc.rs:60-63`), 3.1's names do not exist on a 3.0 page → "refuses by name" (`cnc.rs:55-59`) | 2.0 (`fa10920`); 3.0 4→8 KiB + slot band (page growth, major); 3.1 slot line 7 + a status word (reserved-line use, minor); 3.2 one word at 3984 (reserved-band, minor) | 3.2-style: yes, today, in the attacher-first direction. 3.1-style: no (new field required, no zero meaning). 3.0-style: no (page length). Same-host only; `uc2-service@.service` is `BindsTo=uc2-node.service` (`packaging/systemd/uc2-service@.service:6-7`), so a node restart restarts the host's services anyway — the page is **not on the rolling path**; it matters for embedded/long-lived clients only. |
| f | replicated records in the log | `CLUSTER` body `kind u8 ‖ reserved [u8;7] ‖ payload` (`frame.rs:68-100`); Settings `SETTINGS_VERSION = 2`, decoder accepts `(1, 29 B)` and `(2, 33 B)` only, v1 → `datagram_mtu = 0` (`uc_protocol/src/v2/settings.rs:9-17,110-119`); schedule `SCHEDULE_VERSION = 1` strict (`schedule.rs:11`); membership (`config.rs`) has no codec version; cluster image `v1` (`cluster_image.rs:43`); `config.state`/vote/floor via `StableValue` (`uc_journal/src/stable_value.rs:186-190`, exact `format_ver`); journal segments exact `format_ver` (`uc_journal/src/journal/segment.rs:304-312`); snapshot envelope `ULTSNAP1 ‖ P` magic-only (`uc_service/src/snapshots.rs:58-62`) | **the one forward-compat idiom in the tree is Settings v1-on-read** — but it is backward-only: an unknown version → `None` → reason 42, `applied` advances (`cluster_fsm.rs:352-359`). Journal/StableValue/cluster image are node-local files: never mixed *within* a node; only the cluster image crosses hosts (the snapshot session's `service_id = 255` artifact) | Settings v1→v2 (versioned extension) | the record layer is the *easiest* to make compatible (a version word already leads every record) and the *most dangerous* to get wrong (silent divergence). Rule: no node may append a record shape above the committed level (§5). Node-local formats need a per-node migration/`MinSupported` story, not a wire one. |
| g | admin plane | request line at cnc 3584: `seq, nonce, op u32, id, ip, port` (`cnc.rs:134-140`); ops 1–9 (`uc_node/src/audit.rs:149-161`); refusals 1–12, 20–24, 40–51 (`uc_node/src/node.rs:378,410-522`); follower→leader forwarding as `CONFIG_PROPOSAL` (`node.rs:2929,7582`; body `op u32` at `datagram.rs:284-290`) | unknown op → `wire_to_config_op` `None` (`node.rs:9647-9660`) → `REASON_MALFORMED_OP = 11`. A new op forwarded to an old leader is refused by name; `uc2ctl` is same-host and version-gated by the cnc word | ops 6–9 added (additive) | yes already: additive ops, named refusal. Ops 6–9 are **leader-local by design** (staged file), so an old leader never sees a forwarded new op anyway. |
| h | per-row IPC rings | `RingHeader` = magic + geometry, **no version word** (`uc_protocol/src/ring/common.rs:104-127`); `ULTRNG2\0` for MPSC (`uc_protocol/src/magic.rs:11`), `ULTRNG\0\0` for SPSC/Broadcast; record kinds `MSG_V2_*` 1–8 (`uc_protocol/src/v2/ipc.rs:42-65`) | magic mismatch on attach (`common.rs:604-622`); record kind is a `u16` any consumer can skip | M13 MPSC re-magic (format change, magic-gated); `MSG_V2_SCHED = 8` (additive) | same-host and volatile ("recreated at boot", `upgrade-a-cluster.md:249-250`): covered by the host restart. Not on the rolling path. |

Two ancillary facts. The remote protocol (`uc_remote/src/frame.rs:19,52,107-113`)
carries `version u16` in every 24 B header and the edge refuses anything but
an exact match (`uc_gateway/src/edge.rs:1157-1159`, `HELLO_REFUSED_VERSION`):
it is the in-tree precedent for a **named** version refusal, not for
negotiation — there is no min/max window. And `uc_sim` has no wire layer at
all: it drives `ElectionSm` over typed events (`uc_sim/src/world.rs:1-4,50-51`),
so "mixed-version worlds" would not be a sim change but a codec-level
corpus plus a two-binary crashtest (§5).

### What the history classifies to

| bump | commit | class | absorbed by Level 1 | by Level 2 | by Level 3 |
|---|---|---|---|---|---|
| wire 0.2.0 | `50c4ca8` | additive kinds 16/17 + additive frame type 4 | kinds yes; the type only if an old node may ignore membership (it may not — needs a level) | yes | yes |
| wire 0.3.0 | `a3bbcb3` | a cnc reserved-band word, tagged as a wire bump (the lines were not yet independent) | n/a | n/a | n/a |
| wire 0.4.0 | `c269ad6` | reserved-slot use + kinds 18–20 + a cluster mode | no while crypto is on (old nodes cannot open sealed datagrams); the mode is a flag day by policy | crypto-off → crypto-on stays a flag day by choice | same |
| wire 0.5.0 | `48fc96c` | body extension whose *absence* changes quorum counting | no — the old meaning was made unacceptable on purpose | yes, at the cost of running the pre-fix semantics until the level is raised (§6) | same |
| wire 0.6.0 | `126836d` | `SNAP_BEGIN` body relayout, tagged | no | yes (sender picks layout by level; receiver keeps a window) | same |
| wire 0.7.0 | `0bb7c65` + `48a2bc7` | `SNAP_BEGIN` relayout **and the frame-header relayout** | no | `SNAP_BEGIN` yes; the header **no** | yes, per-term layout |
| wire 0.8.0 | `d4f297f` | additive kinds 24/25, Settings v2 | yes (already true: a 0.7.0 peer drops the probes, `version.rs:75-80`) | yes | yes |
| cnc 3.0 | `f58f3c2` | page growth 4→8 KiB, major | no | no — page growth stays a same-host restart | same |
| cnc 3.1 | `58d9c01` | reserved-line use with a required field | no (attacher refuses by name) | n/a — same-host | same |
| cnc 3.2 | `82d903f` | reserved-band word, `0 = absent` | **yes, already** (attacher-first) | n/a | same |

## External precedents (first-party sources)

| system | mechanism | the one thing that transfers | source |
|---|---|---|---|
| etcd | leader computes cluster version = min(member versions), `major.minor`, monotone; a member below it panics at join ("invalid downgrade; server version is lower than determined cluster version"); explicit `downgrade enable` lowers it one minor | **the leader-computed committed minimum**, exported as a gauge (`etcd_cluster_version{cluster_version="3.5"}`) | https://etcd.io/docs/v3.5/upgrades/upgrade_3_5/ ("operates with the protocol of the lowest common version"); https://raw.githubusercontent.com/etcd-io/etcd/main/server/etcdserver/version/monitor.go; …/version/downgrade.go; https://etcd.io/docs/v3.5/metrics/etcd-metrics-latest.txt |
| CockroachDB | replicated `version` setting; a binary supports `[MinSupported, Latest]`; `IsActive(v)` gates every incompatible path; finalize is one-way (`SET CLUSTER SETTING version`, `cluster.preserve_downgrade_option` holds it) | **named version gates consulted at the point of use**, raised only when every node's binary supports the target ("cannot upgrade to %s: node running %s") | https://docs.cockroachlabs.com/docs/stable/upgrade-cockroach-version; https://raw.githubusercontent.com/cockroachdb/cockroach/master/pkg/clusterversion/clusterversion.go; …/setting.go |
| Kubernetes | no runtime mechanism; a published skew policy (apiserver instances within one minor; kubelet never newer, up to three older; upgrade apiserver first; never skip minors) | **the contract shape**: "N reads N−1" per release, enforced by process and a test matrix, not by code | https://kubernetes.io/releases/version-skew-policy/ |
| Kafka | per-connection `ApiVersions` min/max per API (KIP-35); tagged fields add data without a bump (KIP-482); the cluster floor `inter.broker.protocol.version` → KRaft `metadata.version` as a `FeatureLevelRecord` in the metadata log: roll binaries at the old level, raise once; "If a broker encounters an unsupported metadata.version, it should unregister itself and terminate" | **the operator-raised floor record in the replicated log** — the exact shape of UC's jumbo rung | https://cwiki.apache.org/confluence/display/KAFKA/KIP-35+-+Retrieving+protocol+version; https://kafka.apache.org/43/design/protocol; https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields; https://cwiki.apache.org/confluence/display/KAFKA/KIP-778%3A+KRaft+to+KRaft+Upgrades |
| Aeron | one `version` byte per data frame (`CURRENT_VERSION = 0`, offset 4); a mismatched frame is dropped and counted (`invalidPackets`), never negotiated; CnC file checked on semver **major** at attach; Cluster stamps `appVersion` into every `NewLeadershipTermEvent` and snapshot, validated by a pluggable `AppVersionValidator` (default: major equality) | **the version in the header, drop-and-count** (a named refusal, ~free) and **the version stamped at term boundaries in the log** (the carrier backlog item 3 already names) | https://github.com/aeron-io/aeron/wiki/Transport-Protocol-Specification; https://raw.githubusercontent.com/aeron-io/aeron/master/aeron-driver/src/main/java/io/aeron/driver/media/UdpChannelTransport.java; …/aeron-client/src/main/java/io/aeron/CncFileDescriptor.java; …/aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModule.java. Local mirror: `../aeron-go/aeron/logbuffer/DataFrameHeader.go:21`, `../aeron-go/aeron/counters/counters.go:35,148` (aeron-go checks the cnc version for exact equality, stricter than Java's major) |
| Cassandra | per-connection handshake negotiates the highest common messaging version in `[minimum_version, current_version]`; every serializer from `minimum` up stays live, chosen per endpoint | the fully decentralised alternative — no floor at all — and why UC should not take it: N serializers selected per destination, a handshake RTT per peer, and a retransmit buffer that must be re-encodable per peer, which is where it collides with the log-buffer-as-retransmit-buffer design | https://raw.githubusercontent.com/apache/cassandra/trunk/src/java/org/apache/cassandra/net/MessagingService.java; …/net/HandshakeProtocol.java; https://raw.githubusercontent.com/apache/cassandra/trunk/NEWS.txt |
| Raft (Ongaro) | membership change only; no mention of software versions (0 hits for upgrade/protocol version in the thesis PDF) | only the observation that every system above replicates its version the way Raft replicates a configuration: a committed entry whose effect is deferred to commit | https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf, ch. 4 opening |

The Kafka upgrade page's literal rolling steps and an Aeron Cluster
rolling-upgrade procedure could not be fetched (404 / JS shell) — **not
verified**; Aeron's site claims "rolling upgrades … zero downtime" with no
procedure behind it (https://aeron.io/aeron-open-source/).

## The option space

Costs are stated against three yardsticks: code, proof surface, and the hot
path — with CLAUDE.md's standing lesson that code in a hot loop's body costs
even on untaken paths, and that the apply loop's per-frame callees are
`#[inline(always)]` for that reason (`frame.rs:152-157`).

| level | what it is | code | proof / test | hot path | absorbs |
|---|---|---|---|---|---|
| **0** — contract only | a documented "0.x+1 reads 0.x" skew rule with no mechanism | docs only | none | none | nothing, and it cannot even be *detected*: with `CURRENT` unchecked, a mis-skewed cluster is indistinguishable from a healthy one until commit stalls (0.5.0) or state diverges (a CLUSTER record). Only meaningful as the preamble to Level 1. |
| **1** — additive discipline + explicit rules | (i) unknown datagram kinds dropped **and counted** (`dropped_unknown_kind`, before the term filter); (ii) "a body grows only at its tail; a reader takes the prefix it knows" — relax the three exact readers or document them as fixed-length forever; (iii) the datagram `flags` byte becomes `wire: u8` = the sender's minor, written by every sender (it is written as 0 today, so the field exists at zero cost); (iv) the frame header's reserved `u16 @6` becomes `layout`, written as 0; (v) cnc: every new word must have a `0 = absent` reading (3.2's rule, not 3.1's) and the swap order "attachers before node" is written down | small; `uc_protocol` + one receiver counter + `wire-protocol.md`/`cnc-page.md` rules | a golden-datagram corpus in `uc_protocol` tests (every kind at every shipped version, decoded by the current readers) | one byte write per datagram (already written), one compare per unknown kind | 0.8.0-shaped bumps; cnc 3.2-shaped bumps. Not 0.5.0, not any relayout. |
| **2** — committed cluster wire level (the etcd / CockroachDB / Kafka shape, UC's jumbo pattern) | the leader keeps per-member `wire_max` **from the `wire` byte of datagrams it already receives** (heartbeat replies, reports — no new kind; authenticated by the AAD when crypto is on, `seal.rs:52`), commits `wire_level = min over members` as a Settings v3 field or a `CLUSTER kind = 4`, monotone in the FSM like `datagram_mtu` (`cluster_fsm.rs:377-379`); every **send-side** choice that has an old and a new shape is gated `if level >= L`; receivers accept the whole window `[wire_min, wire_max]` of their binary; a node with `wire_max < level` refuses to start by name (the `PathBelowCommittedMtu` analog, spec §5.4), as does one with `wire_min > level`; `uc2_wire_level` gauge, `uc2ctl status` line, join refusal | the jumbo feature's size and shape: a leader table, a commit rule, a FSM field, two refusals, one gauge; plus the discipline that every future wire change has two encoders until `wire_min` passes it | (1) the corpus, extended pairwise (old encoders × new decoders and the reverse, via `git show <tag>:` sources or a vendored snapshot); (2) a `uc_protocol_compat` fuzz target; (3) a **two-binary crashtest**: `examples/uc_crashtest` already spawns real node processes — a scenario that runs N−1 from the previous release tarball and one from the tree, upgrades them one at a time under load, and asserts the history linearizable; (4) a fleet row reusing `m14_fleet_gate.py --base-tree` (`bench-infra/scripts/m14_fleet_gate.py:36` already ships two trees to a fleet): bars = commit never stalls beyond an election timeout during the roll, linearizable history, `dropped_unknown_kind` bounded; (5) sim: the kernel is untouched (the level is FSM state), so Lean and `uc_sim` need no change — an `inv13` "no node applies a record above the committed level" is optional | the DATA path is a byte-run copy and stays one; control-kind senders read one atomic; the receiver stores one byte per datagram into a per-peer slot (same class as `self.cfg.leader = from`, `receiver.rs:2018-2020`); the apply loop is untouched | 0.2.0, 0.5.0 (with the caveat in §6), 0.6.0, 0.7.0's `SNAP_BEGIN` half, 0.8.0, any future record-version bump. **Not** the frame-header relayout, not cnc page growth, not crypto on/off. |
| **3** — versioned log frame layout + shippable formats | a `layout` in the header, selected **per term, not per frame**: the `NEW_TERM` frame (empty body today, `frame.rs:32-36`) carries `wire_level` (Aeron's `appVersion` in `NewLeadershipTermEvent`; the FSM-identity spec's "term-boundary log event"), every reader switches decoder at a term boundary and the per-frame path keeps one decoder; the archive's clock seed (`archive.rs:454`) and `ctx.time_ns` become undefined (0) for frames below the level that introduced them, so an FSM that reads `time_ns` needs a defined pre-level value — which is the FSM-`VERSION` validator backlog item 3 describes; the cluster image and snapshot envelope get the Settings-v1 idiom; journal/`StableValue` `format_ver` stays exact and gains a per-node boot migration (they never cross hosts) | large: the apply loop, `FrameIter`, the archive walk, the cluster agent, the receiver's run validation, the snapshot builders; two decoders live until `wire_min` passes the layout | the same two-binary crashtest with a leader change mid-roll; the `uc_protocol_log_frame` fuzz target per layout; `lin_v2` under a mixed log | **per-frame** tagging is the design to refuse: a branch per frame in `apply_cycle` is exactly what cost 27 % in `2.11.0` before the inlining fix. **Per-term** hoists it out of the per-frame path: one decoder pointer swapped when the apply loop sees `NEW_TERM` (it skips that type today, `apply.rs:560-612`) | everything above plus a relayout of the 0.7.0 kind. The header *length* (32 B, `FRAME_ALIGNMENT`) stays fixed by choice. |
| cnc, separately | no new mechanism: `version_compatible` already admits a newer attacher on an older page (`cnc.rs:508`); codify "0 = absent" for every new word and the attacher-first swap; page growth and required fields stay a host restart, which `BindsTo` makes atomic anyway | docs + a test that every post-3.2 word tolerates 0 | an attach test against the previous page version | none | 3.2-shaped bumps. Not 3.0/3.1-shaped. |

## Cheap versus structurally expensive

Cheap, and true today or one small change away:

- Unknown datagram kinds (silently ignored, `receiver.rs:2279`) — add the counter.
- Unknown frame *types* (skipped by every walker) — already safe.
- Unknown admin ops (reason 11) and the leader-local ops 6–9.
- The datagram `flags` byte as a wire-minor field: written as zero today,
  never read, authenticated under AAD when crypto is on.
- The frame header's two reserved slots (`u16 @6`, `u32 @20`).
- A record-version window on the CLUSTER payloads: every record already
  leads with a version word; Settings already reads v1.
- The cnc page in the attacher-first direction.

Structurally expensive, each for a different reason:

- **The frame-header relayout.** Same length, different meaning, persisted,
  replayed, shipped opaque. Only a per-term layout (Level 3) absorbs it, and
  it drags `ctx.time_ns` semantics and the FSM version validator with it.
- **The 16-byte datagram header's length and the DATA run's self-locating
  contract.** Changing either changes what `position` means to every
  receiver; no negotiation makes that mixed-safe. Fixed by choice.
- **Crypto on/off.** A mode, not a version (`uc_net` cannot open sealed
  datagrams without a session). Stays all-or-nothing.
- **Safety fixes of the 0.5.0 kind.** Under a committed level the cluster runs
  the *pre-fix* semantics until the level is raised — etcd and CockroachDB
  accept exactly this ("operates with the protocol of the lowest common
  version"). The maintainer may still declare such a fix a flag day; the
  mechanism does not force the choice.
- **The FSM `const VERSION` equality on `SNAP_BEGIN`** (`receiver.rs:2364-2385`).
  A rolling *service* upgrade — user code, not UC — refuses snapshot sessions
  between mismatched hosts today. Aeron's answer is a pluggable validator
  defaulting to major-equality; UC's spec already reserved the field for one.

## Recommendation

**Target Level 2 for the next feature cycle, and use the next flag day to
reserve Level 3's carrier — so that flag day is the last one for the 0.x
line.** Concretely, in the flag day after `2.12.0` (wire `0.9.0`, cnc
unchanged):

1. `flags u8 @13` → `wire u8`: every sender writes `CURRENT.minor`
   (`0` = "0.8.0 or older"). Receivers store it per peer and count
   `dropped_wire_newer` on a value above their `wire_max`; nothing else
   changes on the receive path. The AAD already authenticates it.
2. `NEW_TERM` gains an 8-byte body `wire_level u32 ‖ reserved u32`, and the
   frame header's `u16 @6` becomes `layout` (written `0` = the `2.11.0`
   header). Old apply loops skip `NEW_TERM`; the archive never reads its body
   — additive on every walker.
3. Settings v3 adds `wire_level: u8`, monotone in the FSM, raised by the
   leader's commit rule once every member's last-seen `wire` byte is at or
   above the next level (the jumbo table, `spec §5.3`, with the header byte as
   the ack). The raise record is v3, which a `2.12.0` decoder refuses with 42
   — acceptable *only* because the commit rule guarantees no `2.12.0` binary
   is a member when it is appended; write that invariant down and test it.
4. A joiner whose `wire_max < wire_level` or `wire_min > wire_level` refuses
   to start by name (two new startup refusals); `uc2_wire_level` gauge;
   `uc2ctl status` prints it; `Uc2WireLevelBelowMembers` alert when the
   leader's table minimum exceeds the committed level for longer than a roll.
5. `dropped_unknown_kind` counter; the three exact body readers documented
   as fixed-length forever (they are unlikely to grow; leave them strict).
6. The rules go into `docs/reference/wire-protocol.md` (a body grows at its
   tail; a new kind is ignorable until gated; a new record version is appended
   only above its level; the header length and the DATA run contract are
   frozen) and `docs/reference/cnc-page.md` (every new word reads `0` as
   absent; attachers swap before the node).
7. Proof: the golden corpus and the two-binary crashtest ship *with* the
   flag day, against `2.12.0`'s release binary, so the first rolling upgrade
   (`0.9.0` → `0.10.0`) has its gate before it exists.

**Stays flag-day forever, by choice:** the major digit of both lines; the
16-byte datagram header length and the self-locating DATA contract; the
32-byte frame header length; crypto on/off; cnc page growth or a required
new cnc field (a same-host restart, atomic under `BindsTo`); and any safety
fix the maintainer declines to run in its pre-fix form during a roll.

**What this does not buy:** a rolling upgrade across the `48a2bc7`-style
relayout still needs Level 3's second half (per-term decoder selection in
every walker, and a defined `time_ns` for pre-level frames). Step 2 only
reserves its carrier. Do Level 3 when a header change is actually wanted,
not before — it is the one part of this whose cost lands in the apply loop.

## Method notes

Every `path:line` above was read in this tree on 2026-09-13; the commit
hashes were verified with `git log -1` (the frame-header relayout is
`48a2bc7`, 2026-09-03, separate from `0bb7c65` but in the same `2.11.0`
flag day). External quotes were fetched from the URLs shown; the etcd
"within one minor" rule appears in its upgrade prerequisite and downgrade
path, not as a literal join check in code (the join window is
`[MinClusterVersion, local]`, `server/etcdserver/cluster_util.go`). Sizing
words ("small", "the jumbo feature's size") are judgements, not estimates.
