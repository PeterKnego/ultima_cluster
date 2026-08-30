# UC v2.1 (M7) — dynamic reconfiguration: single-server membership change

**Date:** 2026-07-13
**Status:** approved design (brainstorm 2026-07-13); next step = implementation plan
**Baseline:** v2.0.0 (M1–M6 complete, v1 retired; see
`2026-07-09-uc-v2-aeron-shaped-smr-design.md` — the v2 spec penciled
"joint-consensus reconfiguration: v2.x", which this design revisits and
replaces with single-server change)

## 1. Goal and locked decisions

Make membership a **live, online operation**: replace a dead box and resize the
cluster (3⇄5) under load, without restarts and without violating any v2 safety
property. This closes v2.0's accepted capability regression ("static voting
set from config").

Decisions locked during the brainstorm:

| Decision | Choice |
|---|---|
| Operational scope | **Replace-a-box + resize**, composed from single-server changes (promote / demote / add-learner / remove), exactly **one change in flight** at a time. No arbitrary set-to-set changes. |
| Protocol mechanism | **Approach A: single-server change, config-as-log-entry** (Raft §4.2.2 adapted to the position-based core). Joint consensus rejected as YAGNI for this scope; frozen-window handoff rejected because it does not escape the hard part (a mid-window election still resolves membership from the log) and adds a deliberate write stall. |
| Admin ingress | **Local admin CLI via cnc** (`uc2ctl`, same-host like `m6_gate probe`); a follower node forwards the proposal to the leader over the existing UDP control plane. No new remote-admin surface before wire-crypto. |
| Node identity | **Fresh-forever NodeIds.** Removed ids are tombstoned in the config and can never reappear. Kills the zombie-identity bug class. |
| Acceptance gate | **Full fleet gate** (M1–M6 protocol) + the local proof stack. |

### Why not port Aeron's design

The reference implementation has **no working design to port**. Open-source
Aeron Cluster shipped "dynamic join" (passive members + log-recorded membership
events — structurally this design), then deprecated (~1.41) and removed it
(1.42, 2023) as never production quality; current OSS Aeron is static-membership
with offline procedures, and their production answer (premium Cluster Standby)
moves replacement *out* of the consensus protocol. Their retreat is an
engineering warning about the integration surface — membership × snapshots ×
truncation × recovery — not about the math. UC's bet: that surface is exactly
what `uc_sim` + the WGL capstones + the crashtest were built to hold. M7 is
the first place v2 deliberately exceeds the reference.

> **See also:** `docs/notes/uc2-m7-vs-aeron-cluster-standby-2026-07-24.md` — a
> full M7-vs-Aeron-Cluster-Standby comparison plus a sketch of what an "async
> cross-region learner" (UC's Standby analog) would take against the current
> design.

## 2. As-built baseline (what this design changes)

Membership today is 100 % boot config, zero durable state:
`NodeConfig.members: Vec<(NodeId, SocketAddr)>` + disjoint `learners`; the
durable set is vote / term_map / output_progress / snapshot floor only.
Membership is baked in at construction everywhere: `ElectionSm` takes a
slot-indexed members Vec (`follower_slot`), `CommitTracker::new(n−1, n)` sizes
the quorum ranking at build time, the sender peer list and flow-control
voter/learner split are fixed at startup, the known-source guard drops
non-member datagrams, and the cnc band caps at 8 peer slots. Learners already
have the right shape (`can_vote: false`, not in `members`, never counted, never
candidates); NoCommonPrefix wipe-and-rejoin and snapshot sessions exist (M6).

Frame types today: MESSAGE=1, PADDING=2, NEW_TERM=3. NewTerm frames are
appended in-stream by the leader and fed back as SM events; followers discover
them via the archive frame-scan (data-stamped term maps). Config frames ride
both paths unchanged.

## 3. Config model and durable state

```rust
pub struct ClusterConfig {
    pub version: u64,                        // +1 per change; genesis = 0
    pub voters:  Vec<(NodeId, SocketAddr)>,
    pub learners: Vec<(NodeId, SocketAddr)>,
    pub tombstones: Vec<NodeId>,             // fresh-forever: never reusable
}
```

- **Genesis:** today's boot config becomes config **version 0**. A joining
  node boots with an operator-supplied *seed* config; from then on the stream
  is authoritative.
- **Config as log entry:** a membership change is a **`FRAME_TYPE_CONFIG = 4`**
  frame appended by the leader in-stream — it occupies positions, replicates
  as bytes, is archived and CRC-covered like everything else. Body = the full
  new `ClusterConfig` (self-contained, never a delta) + the predecessor
  config's position for audit. Frame layout pinned with literal LE-byte tests
  (house style).
- **Durable record:** one new `StableValue<ConfigRecord>` alongside
  vote/term_map:

```rust
pub struct ConfigRecord {
    pub position: u64,        // where `config` took effect (frame end)
    pub config: ClusterConfig,
    pub prev_position: u64,   // exactly one level of history — sufficient
    pub prev: ClusterConfig,  //   because of the one-in-flight rule (§4)
}
```

  One level of history suffices: a new change is only proposable after the
  previous config entry is **committed**, and a committed entry can never be
  truncated — so at most one config entry is ever truncation-exposed.
- **Boot recovery:** load the durable `ConfigRecord`, then re-adopt any config
  frame found by the journal-suffix scan above `position` — the same scan that
  rebuilds the term map.

## 4. Quorum and election integration

- **Adoption timing (data-stamped):** the **leader adopts at append** (it must
  immediately count the new member set); a **follower adopts when the config
  frame is durably recorded**, discovered by the archive frame-scan — the M4
  discipline: never adopt what isn't durably yours.
- **Rebuild-at-boundary:** at adoption the node *rebuilds* the slot-indexed
  machinery rather than mutating it: new `CommitTracker(n′−1, n′)`,
  `follower_slot` remap, flow-control voter/learner split, sender peer list.
  Carried state: the last known durable `Report` per surviving member is
  re-fed; new members start unknown. Commit stays monotonic through the
  boundary (it lives in the cnc counter, not the tracker).
- **One-in-flight:** the leader refuses a proposal while
  `config_record.position > commit` (pending change not yet committed).
- **Elections across adjacent configs:** a candidate counts votes under *its*
  adopted config; the vote-grant rule (lexicographic `(last_term,
  last_durable)`, persist-vote-before-answer) is unchanged. Safety = the
  single-server overlap argument: adjacent configs differ by one member, so
  any majority of version v intersects any majority of v+1 — disjoint quorums
  cannot form.
- **The single-server-change precondition** (new leader must commit an entry
  in its own term before any config change — Ongaro's 2015 correction) is
  **already structural**: the M4 serving gate is "leader serves only after its
  NewTerm frame is quorum-committed", and that frame is the required
  own-term commit. Config proposals are accepted only from a serving leader.
- **Removed voters cannot disrupt:** at adoption the removed id leaves
  `members` and is tombstoned, so its Vote/Report datagrams are dropped by the
  known-source guard — v2's strict source filtering does the job Raft needs
  pre-vote for; zombies cannot force term inflation. A node that sees a config
  excluding itself **halts fail-stop** ("removed from cluster"); one that never
  sees it spins isolated (runbook: decommission the process).
- **Leader self-removal:** the leader proposes the config without itself,
  keeps leading until that entry **commits** (quorums counted among C_new
  members; bounded-by-own stays safe — the appender always holds everything it
  appended), then steps down and halts → normal election among the remaining
  voters (existing ~p50 202 ms failover class).

## 5. Truncation revert — the UC-specific hard part

Truncation is the existing archive `Truncate{to}` path (M5 truncation-epoch
machinery). New rule, joining the same latch/critical section:

- If `to < config_record.position`: atomically **revert** to `prev`
  (**persist-revert-before-truncate**, mirroring M5's
  persist-map-before-truncate ordering), then rebuild-at-boundary under the
  reverted config. (`position` is the frame-END effect point; truncation is
  frame-aligned, so any `to` strictly below it lands at or below the config
  frame's start and removes the frame. `to == position` preserves the frame —
  no revert.)
- After truncation + subsequent replication, a surviving config frame above
  the new frontier is re-adopted by the normal scan — adoption is idempotent
  by `version`.
- Invariant (sim inv8): after any truncation, the adopted config equals the
  config implied by the node's durable frontier.

## 6. Operations and the admin path

| Op | Leader-checked precondition | Effect |
|---|---|---|
| `add-learner id@addr` | id not tombstoned, not present; ≤ 8 total members | learner added; stream + (below-floor) snapshot session begin |
| `promote id` | learner's reported durable ≥ commit − slack (default: one admission window) — else refused with the measured gap | learner → voter |
| `demote id` | would not leave 0 voters | voter → learner |
| `remove-learner id` | is a learner | removed + tombstoned |
| `remove-voter id` | would not leave 0 voters | removed + tombstoned; if id == leader → §4 step-down path |

Structural invalids are refused. **Liveness judgment stays with the operator**:
the CLI warns when a member's report is stale ("removing a live voter while
node1 is dark leaves you stalled") but does not block — runbook territory.

**Recipes:** replace-a-box = `add-learner` → catch up → `promote` →
`remove-voter <dead>` (three committed changes). Resize 3→5 = two
add+promote pairs.

**Admin path:** a request/response slot pair in the cnc reserved band
(3456..4096 is free; one op at a time matches one-in-flight). `uc2ctl` (new
bin) writes `{op, id, addr, nonce}`; the local node's consensus agent polls
it. If follower, the node forwards over the existing control plane as new
datagram kinds **`CONFIG_PROPOSAL = 16` / `CONFIG_REPLY = 17`** to the leader
hint. Replies carry accepted/refused + reason code + new config version; the
nonce makes retries idempotent against the pending change. `uc2ctl status`
decodes the local cnc: config version, members with reported durables, pending
change.

## 7. Net layer, cnc, versioning

- **Sender:** fan-out gains/loses a destination at rebuild; M6 snapshot
  sessions serve a below-floor joiner unchanged.
- **Receiver/guards:** known-source set updates at rebuild; a joining node's
  bootstrap trust is its operator-supplied seed config (acceptable under the
  trusted-network posture).
- **Flow control:** a learner's advert stays observability-only until promote
  moves it into the quorum order statistic; the promote slack bounds how far a
  fresh voter can briefly drag the advertised limit — the transient the fleet
  gate's dip criterion measures.
- **cnc:** new observability fields in the reserved band: `config_version`,
  `pending_config`, + the admin slots. PeerSlots' `id_and_role` flips at
  adoption; slots reassign at rebuild. **Hard cap: 8 total members** (voters +
  learners, including transitional states), enforced at proposal.
- **Versioning:** FRAME_TYPE_CONFIG=4 + kinds 16/17 ⇒ **protocol version
  bump**. v2.0 nodes refuse mismatched versions at every entry (existing
  rule). Upgrade path: **rolling-restart all binaries to v2.1 first (still
  static), then reconfigure.** No mixed-version config changes.

## 8. Sim modeling and invariants

Per the M4 lesson ("sim models the ACTUAL mechanism"), the sim gets config
frames as data — adoption-on-durable, rebuild-at-boundary, revert-on-truncate
modeled mechanically. New invariants alongside inv1–5:

- **inv6 — config determinism:** a node's adopted config equals the config its
  durable frontier implies.
- **inv7 — quorum legality:** commit only advances via a quorum of the
  adopting node's config at that position.
- **inv8 — revert correctness:** after any truncation, adopted config
  re-equals the frontier's config.
- **inv9 — tombstone permanence:** a tombstoned id never re-enters.

**Counterfactual-red pins** (the C-1 discipline): delete the serving-gate
precondition → a crafted seed must produce a disjoint-quorum commit; delete
revert-on-truncate → inv8 must go red. **Fuzz arm:** a seeded driver injects
random config ops — including illegal ones (propose-during-pending must be
refused) — under crash/partition/truncation churn; 1000-seed heavy tier.

## 9. Test plan and fleet gate

1. **Unit:** CONFIG frame LE-byte pin; `ConfigRecord` roundtrip;
   precondition/tombstone logic; tracker rebuild-with-carried-reports remap.
2. **Sim:** §8.
3. **Integration** (`uc_node/tests/reconfig.rs`): add→promote e2e; demote;
   remove-voter incl. leader self-removal (step-down + re-election); crafted
   truncation-revert (divergent-leader shape); every refusal; joining-node
   bootstrap-from-seed.
4. **L3:** `lin_v2` gains a 4th fault arm — random legal config ops cycling a
   spare node mid-churn, WGL-checked, **non-vacuity asserted** (≥ 3 config
   entries committed during the run, else the test fails as vacuous).
5. **Crashtest:** SIGKILL a node mid-config-window, restart, converge; config
   recovered from the journal scan.
6. **Fleet gate** (5×`c6id.2xlarge`, orchestrator-driven, sustained load):
   - **replace-a-box live:** add learner on the 5th host → catch up → promote
     → remove a voter. Gate: commit-rate dip **< 10 %** across every
     transition window + zero read divergence (loadclient guard).
   - **resize 3→5→3**, same bars.
   - **leader self-removal:** existing failover bar (zero committed loss, gap
     in the ~200 ms class).

## 10. Milestone shape

**M7** in the v2 line: branch `uc2/m7-reconfig`, SDD tasks (plan will cut
~10–12: frame+durable, adoption+rebuild, truncation-revert, guards+halt, admin
path+uc2ctl, net/cnc, sim arm, integration, L3+crashtest, gate+docs), gate doc
`docs/benchmarks/uc2-m7-gate-<date>.md`, runbook §6 rewritten from "learner
add/remove" to the full ops table, README/CLAUDE.md scope lines updated
("joint-consensus reconfig v2.x" → shipped single-server reconfig), **tag
v2.1.0 on gate pass**. Protocol version bumps once, at the start.

## 11. Risks and mitigations

| Risk | Mitigation |
|---|---|
| The Aeron failure mode: membership × snapshot × truncation × recovery interactions rot | Sim models the actual mechanism (§8) with counterfactual-red pins; truncation-revert is a first-class design element (§5), not an afterthought |
| Rebuild-at-boundary loses quorum context | Carried reports re-fed; commit monotonic in cnc; integration test pins commit progress across a boundary |
| Fresh voter drags flow control after promote | Promote slack precondition bounds the transient; fleet gate measures it |
| Zombie removed nodes | Known-source guard + tombstones (structural); self-halt on seeing own removal |
| Operator foot-guns (removing live voters while another is dark) | CLI warns on stale reports but does not block; runbook |
| Mixed-version clusters | Protocol version bump + refuse-at-entry (existing); documented upgrade order |
