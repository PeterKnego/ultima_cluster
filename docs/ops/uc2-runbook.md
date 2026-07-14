# UC v2 operations runbook

Operator-facing reference for running, observing, and repairing a UC v2 cluster.
This is the counterpart to the design/task docs: it assumes the system works and
tells you how to see what it is doing and what to do when it misbehaves.

Scope: M1–M7 (consensus, log, snapshots, purge, learners, live single-server
reconfiguration). Everything here is observable through the shared **cnc
page** (`instance_dir/cnc2.dat`) — a fixed-layout 4 KiB control page every
process mmaps — plus the node's own stats accessors.

---

## 1. Instance directory layout

One node owns one instance directory. The service and clients attach to the same
directory (same host, shared memory). Default files:

| Path                              | Owner    | Purpose |
|-----------------------------------|----------|---------|
| `instance.lock`                   | node     | Exclusive `flock`. A second node on the same dir is **refused** (`AlreadyRunning`). Service/clients take a *shared* lock as a liveness probe. |
| `cnc2.dat`                        | node     | The 4 KiB control page (magic `UC2CNC\0\0`). All cross-process observability lives here — see §3. |
| `log.buf`                         | node     | The log ring buffer (`buffer_bytes`). Recreated fresh each boot. |
| `journal/`                        | node     | Segmented durable log (`ultima_journal`). Survives restarts; the source of truth for replay + purge. |
| `state/`                          | node     | Raft durables (`StableValue`s): vote, term-map, output-progress, snapshot floor. |
| `snapshots/`                      | service + node | `snap-<pos>.ultsnap` artifacts. The service **builds** them; the node **ships/installs** them (M6). `<pos>` is the absolute log byte position the snapshot represents. |
| `ingress.ring`                    | clients→node | MPSC submit ring. |
| `query.ring`                      | clients→node | Query submissions (linearizable + snapshot reads). |
| `svc_query.ring`                  | node→service | Forwarded queries. |
| `egress_service.broadcast`        | node→service | Apply/output stream to the service. |
| `egress_node.broadcast`           | node→clients | Submit responses broadcast to clients. |

Note: unlike v1, UC v2 does **not** use a `/dev/shm/ultima-{user}-{instance}`
discovery directory — every IPC file lives directly under the instance dir.

**Durability requirement — what must sit on a real disk.** `journal/` (the
replay/purge source of truth), `state/` (the vote + term-map `StableValue`s —
losing these across a machine restart lets a node re-vote in a term it already
voted in, i.e. a split-brain hazard, not just data loss — **M7 adds
`config.state` to this same durable set**: the `ConfigRecord` `StableValue`
holding the current + one-level-previous `ClusterConfig`, without which a
restarted node cannot re-adopt the right membership from a truncated log
prefix), and `snapshots/` must all survive a power loss. Only the rings,
`cnc2.dat`, and `log.buf` are volatile-safe (rebuilt/re-primed on boot). Since
`journal/`, `state/`, and `snapshots/` all live *under* the instance dir, the
simplest safe posture is:
**put the whole instance dir on a real filesystem, never on tmpfs** — an
instance dir on RAM makes every fsync a silent no-op and voids all durability
guarantees while everything appears to work. (Splitting rings-on-tmpfs from
durables-on-disk requires bind-mounting the durable subdirs; only do this
deliberately.) The fleet-gate orchestrator (`bench-infra/scripts/m6_fleet_gate.py`)
enforces this: it `stat -f`s the instance-dir parent on every host — local and
remote — and refuses to run on `tmpfs`/`ramfs`.

---

## 2. The bind-concrete-IP footgun

**Symptom:** the cluster elects a leader but followers never advance
`durable`/`commit`; the leader's per-peer `reported_durable` slots (§3) stay at
0; you may see the receiver's `append_pos_unknown_source` counter climbing.

**Cause:** a node was configured to bind `0.0.0.0` (or a wildcard) while its
advertised member address is a concrete IP. Datagrams then arrive from a source
address that does not match any entry in the member map, so the receiver cannot
attribute them to a peer and the consensus agent ignores the reports.

**Fix:** bind the **exact** address that appears in the member list. Every
node's `NodeConfig.bind` must equal its own `(id, addr)` entry in `members`. On a
multi-homed host pick the interface address the peers actually route to, and use
that identical value in both places.

---

## 3. Reading the cnc page

Attach read-only with `CncPage` (or `xxd` at the fixed offsets — the layout is
pinned in `uc_protocol::v2::cnc` and never drifts). Key fields:

### Node status (offset 704 band)

| Field | Offset | Meaning |
|-------|--------|---------|
| `term`              | 704  | current leadership term |
| `flags`             | 768  | bit0 = **leader**, bit1 = **can_serve**. A serving leader reads `0x03`. |
| `leader_hint`       | 832  | last known leader id; `u64::MAX` = unknown |
| `node_heartbeat_ns` | 896  | wall-clock ns; compare to your clock for node liveness |
| `service_heartbeat_ns` | 960 | service apply-loop liveness |

**Leader probe:** `flags & 0x03 == 0x03` at offset 768 means "this node is the
serving leader." `0x01` = elected but not yet serving (NewTerm frame not
quorum-committed). `0x00` = follower/learner.

### Log counters (offset 256 band)

`append` (256), `durable` (320), `sent` (384), `commit` (448),
`service_applied` (512). Steady state on a leader: `append ≥ durable ≥ commit`,
`service_applied` trailing `commit` by the apply lag.

### Snapshot / purge slots (M6)

| Field | Offset | Writer | Meaning |
|-------|--------|--------|---------|
| `service_snapshot_pos` | 1152 | service builder | newest COMPLETE on-disk snapshot; 0 = none |
| `node_snapshot_floor`  | 1216 | consensus       | node-side mirror of the above (the purge floor target) |
| `incoming_snapshot_pos`| 1280 | consensus       | newest inbound snapshot a below-floor joiner installed |
| `archive_first_base`   | 1344 | consensus       | the archive's **first retained** log position (the real purge floor) |

**Purge health check:** `archive_first_base ≤ node_snapshot_floor`. Purge is
allowed to drop everything below a snapshot; if `archive_first_base` climbs to
`node_snapshot_floor` the purge has caught up to the snapshot and is working. If
`archive_first_base` stays pinned at 0 while `node_snapshot_floor` advances,
purge is off or not running (expected when `PurgePolicy::Disabled`).

### Per-peer observability band (M6, offset 1408)

Eight fixed 256-byte slots (`CNC_OFF_PEER_SLOTS`, stride 256, max 8). Slot order:
voting followers first, then learners. Decode `peer_slot(i)`:

| Sub-field | Slot offset | Writer | Decode |
|-----------|-------------|--------|--------|
| `id_and_role`      | +0   | consensus (boot) | `id = raw >> 8`; `role = raw & 0xff` (1 = voter, 2 = learner). `raw == 0` = dormant slot. |
| `reported_durable` | +64  | consensus | newest durable position this peer has reported (per-peer replication lag = `commit − reported_durable`) |
| `advertised_limit` | +128 | sender    | the peer's flow-control ceiling (voter) or latest position (learner) |
| `naks_plus_replay` | +192 | (reserved) | RESERVED in M6 — aggregate NAK/replay counts live in the sender's stats; the per-peer split is deferred. Reads 0. |

A follower's whole band is dormant (all-zero) — only the leader publishes it.
**M7 note:** `id_and_role`'s slot count can legitimately SHRINK across a
demote/remove — every node zeros its own trailing slots on a shrinking
rebuild (a real bug this milestone's gate work caught and fixed: a stale slot
used to linger at the old, now out-of-range index, producing a ghost
duplicate entry for a still-live id).

### Config version / pending (M7, offset 3456)

| Field | Offset | Writer | Meaning |
|-------|--------|--------|---------|
| `config_version` | 3456 | consensus | this node's currently-adopted `ClusterConfig` version (genesis = 0) |
| `config_pending` | 3520 | consensus | nonzero while a proposed config change is appended but not yet past the truncation-revert risk window (`config_position > commit_seen`) |

The admin request/response slots (3584/3648 — one op in flight at a time,
matching the one-in-flight design rule) are the `uc2ctl`/`m7_gate probe`
channel described in §6 below; they are not meant to be decoded by hand.

---

## 4. Running node/service processes

The reference bins park on busy-spin agents and **ignore SIGTERM slowly**; when
running under systemd/`systemd-run`, set a short stop timeout so shutdown is
prompt:

```bash
systemd-run --unit uc2-node --property TimeoutStopSec=1 \
    /path/to/uc2-node --instance-dir /srv/uc2/n0 --bind 10.0.0.10:9100 ...
```

Backgrounding over `ssh host 'cmd &'` hangs the ssh session (the busy-spin
threads keep the pipe open). Use `systemd-run` (or `setsid` + redirected
stdio) for remote daemons.

---

## 5. Enabling purge

Purge is **OFF by default** (`PurgePolicy::Disabled`) — an unpurged cluster is
always safe; purge only becomes safe once snapshots back it. Checklist before
turning it on:

1. **Snapshot capability.** The service's state machine must implement
   `build_snapshot`/`install_snapshot` (the `ultima_db` `StoreStateMachine`
   adapter does). Without it, `service_snapshot_pos` never advances and there is
   no floor to purge below.
2. **Policy.** Set `PurgePolicy::BelowSnapshot { slack_bytes }`. `slack_bytes`
   keeps a retained tail below the snapshot floor so a slightly-behind follower
   still catches up via journal replay (not a full snapshot install) — size it
   to your worst-case follower lag.
3. **Watch the floors.** After enabling, confirm `archive_first_base` rises
   toward `node_snapshot_floor` (§3). If it lags forever, the archive purge is
   failing (check node logs — purge errors are logged and retried, never fatal).
4. **Below-floor joiners recover via snapshot.** A follower/learner whose NAK
   falls below `archive_first_base` is served a **snapshot session** (kinds
   12–15), then tail-replays. This is automatic; watch `incoming_snapshot_pos`
   on the joiner.

**Prefill decision (Decision #6 — rejected, with evidence):** a restarted node
does **not** prefill its send ring from the journal. A below-ring catch-up gap is
served on demand from the journal (deep-NAK replay); pinned by
`failover.rs::restarted_follower_below_ring_gap_is_served_from_journal_not_prefilled`
(asserts `replay_datagrams > 0` after a 5×-ring gap). Prefill would waste startup
work and memory for a path the NAK/replay machinery already covers.

---

## 6. Live cluster reconfiguration (M7)

M7 ships **single-server membership change**: promote / demote / add / remove
one member at a time, live, under load, via the `uc2ctl` admin CLI — no
restarts, no joint consensus (the design spec's §1 rationale: adjacent configs
differ by one member, so any majority of version *v* intersects any majority
of *v+1* — disjoint quorums cannot form; see
`docs/superpowers/specs/2026-07-13-uc2-reconfig-design.md`). Exactly **one
change is in flight at a time**; a second proposal is refused
(`ChangePending`) until the first commits. **Hard cap: 8 total members**
(voters + learners, including transitional states) — the cnc peer
observability band has exactly 8 slots.

A **learner** is still replicated-to but never counted (no vote, no quorum
slot, no flow-control window, no read-quorum ack) — but unlike pre-M7, a
learner is no longer boot-config-only: it is admitted and removed **live**,
recorded as a real log entry (`FRAME_TYPE_CONFIG`), and every node adopts it
from the replicated stream (a joining node's own `--members`/`--learners`
flags are only a *bootstrap-trust seed* for a genuinely fresh instance dir —
once any config frame exists, the stream is authoritative and a stale/edited
seed has no effect).

**Admin client single-writer rule:** at most ONE admin client may write the
admin band (`instance_dir/cnc2.dat` offset 3584) at a time — this covers
`uc2ctl` AND the `m7_gate` harness (which writes the band directly), or any
other direct `write_admin_req` caller. The admin slot uses seqlock semantics
for reader-writer safety (the consensus agent reads responses), but a seqlock
does NOT protect writer-vs-writer racing: two concurrent admin clients can
interleave field writes and compose a request neither sent (worst case: a
refused/nonsense operation, never data corruption — the node validates every
field). Operators: serialize admin clients (`uc2ctl`, `m7_gate`) per instance dir.

### `uc2ctl` — the five ops

```text
cargo run -p uc2_node --example uc2ctl -- add-learner   --instance-dir D --app-id A --id N --addr ip:port
cargo run -p uc2_node --example uc2ctl -- promote        --instance-dir D --app-id A --id N
cargo run -p uc2_node --example uc2ctl -- demote         --instance-dir D --app-id A --id N
cargo run -p uc2_node --example uc2ctl -- remove-learner --instance-dir D --app-id A --id N
cargo run -p uc2_node --example uc2ctl -- remove-voter   --instance-dir D --app-id A --id N
cargo run -p uc2_node --example uc2ctl -- status         --instance-dir D --app-id A
```

Point `--instance-dir` at **any** node in the cluster, not necessarily the
leader — a follower forwards the request to the leader hint over the existing
UDP control plane (kinds 16/17) and relays the reply back to the same
`uc2ctl` invocation. `uc2ctl` talks to the node purely through the cnc page's
admin request/response slots (the same reserved band `m7_gate`'s in-process
scenarios write directly); no new IPC ring, no new port.

| Op | Wire op | Precondition (leader-checked) | Effect |
|---|---|---|---|
| `add-learner id@addr` | 1 | id not tombstoned, not already present; ≤ 8 total members after | learner added; stream + (below-floor, if purged past it) snapshot session begins |
| `promote id` | 2 | id is a learner; its reported durable ≥ commit − slack (default: one admission window) | learner → voter |
| `demote id` | 3 | id is a voter; would not leave 0 voters; id is not the leader's own | voter → learner |
| `remove-learner id` | 4 | id is a learner | removed **and tombstoned** |
| `remove-voter id` | 5 | id is a voter; would not leave 0 voters | removed **and tombstoned**; if `id` is the current leader, it keeps serving until its own removal **commits**, then steps down (→ §8's failover class) |

### Reason codes (`uc2ctl`'s refusal output / wire `reason` field)

| Code | Reason | Meaning |
|---|---|---|
| 1 | `NotLeader` | the node handling the request (after any forward) isn't the leader — retry |
| 2 | `NotServing` | a serving leader hasn't yet committed an entry in its own term (M4 serving gate) |
| 3 | `ChangePending` | another config change is still in flight — wait for it to commit, then retry |
| 4 | `Tombstoned` | this id was permanently removed before; **fresh-forever ids never rejoin** |
| 5 | `AlreadyPresent` | add-learner on an id already in the cluster |
| 6 | `NotFound` | promote/demote/remove on an id not currently a member |
| 7 | `WrongRole` | promote a voter, or demote a learner |
| 8 | `ZeroVoters` | would leave the cluster with no voters |
| 9 | `TooManyMembers` | would exceed the 8-member cap |
| 10 | `NotCaughtUp` | learner too far behind commit to promote safely — refusal reports the measured gap |
| 11 | malformed/unknown op | node-level catch-all (not a `ProposeError`) — an op code the node doesn't recognize |
| 12 | `SelfDemote` | `demote` refused the leader's own id — see the recourse below |

A `status: 2` (`retry`) response (not a `ProposeError` — a CLI-level "leader
unknown yet, or the append ring was momentarily full") means: just try again.

### Recipes

**Replace a dead/dying box** (3 committed config changes):
1. `add-learner <new-id>@<new-addr>` — bring the new host up first.
2. Wait for it to catch up (`uc2ctl status` on any node shows the new id's
   `reported_durable` climbing toward `commit`; the `NotCaughtUp` refusal on
   step 3 also reports the live gap if you jump the gun).
3. `promote <new-id>`.
4. `remove-voter <dead-id>` — safe once the promoted replacement is a voter;
   do this **after** promoting, not before (removing first would run one
   member short during the promotion window for no reason).

> **Snapshot-pairing caveat:** under sustained write load, a fresh learner
> catching up purely via NAK/journal replay is a throughput-bounded race it
> can lose indefinitely — a sufficiently fast, unthrottled writer can outrun
> replay forever, not just slowly (this is not a hang: stopping the writer
> lets the same learner catch up completely, just slowly). **Pair
> reconfiguration with M6 snapshots/purge in any deployment with meaningful
> sustained write load**: a `SnapshotStateMachine` + purge policy lets a
> below-floor learner converge via snapshot install + tail replay instead of
> pure replay, removing the ceiling entirely. The `promote` precondition's
> `NotCaughtUp` refusal (reason 10) is the guard that surfaces this — it
> withholds promotion until the gap closes, so the failure mode is "promote
> never accepted", not a silent quorum hazard.

**Resize the cluster (e.g. 3 → 5)**: repeat `add-learner` + (catch-up) +
`promote` once per new voter — two independent single-server changes, not one
bulk operation. Shrinking back (5 → 3) is `demote` + `remove-learner` per
member being dropped, in either order relative to each other (they are
independent single-server changes too) but demote-then-remove per member (you
cannot remove a current voter directly — demote it to a learner first, then
remove the learner).

> **Snapshot-pairing caveat:** the same NAK/journal-replay ceiling applies to
> every `add-learner` in a resize — under sustained write load, a fresh
> learner catching up purely via replay can be outrun indefinitely by a
> sufficiently fast, unthrottled writer (not a hang: stopping the writer lets
> it catch up completely, just slowly). **Pair reconfiguration with M6
> snapshots/purge in any deployment with meaningful sustained write load** so
> each new learner converges via snapshot install + tail replay rather than
> racing pure replay. The `promote` precondition's `NotCaughtUp` refusal
> (reason 10) is the guard that surfaces this.

**Demoting the leader itself is refused (reason 12).** To turn a leader into
a learner: `remove-voter` its id (self-removal is supported — the leader
replicates its own removal, steps down when it commits), then `add-learner` a
FRESH id on that host (tombstoned ids never rejoin).

**Leader self-removal**: `remove-voter <the-leader's-own-id>`, run against
that same leader's own instance dir (or any node — it forwards). The leader
keeps serving until its own removal **commits**, then steps down; the
remaining voters elect a new leader (the existing ~200 ms failover class —
§8). Zero committed entries are lost across the handoff.

### Staleness warning (informational, never blocking)

`uc2ctl status` prints a per-member `reported_durable` and a `-- STALE: N
bytes behind commit` warning when a member's last reported durable trails
commit by more than one admission window — that member may be effectively
dark. **The CLI does not block on this.** Removing a live-but-stale voter can
stall the cluster (it can no longer ack the removal's own commit, and if it
was needed for quorum you've just made that quorum unreachable) — the
liveness judgment stays with the operator; read the warning before running
`remove-voter` on anything you haven't independently confirmed is actually
down.

### Removed-node decommission

A node that adopts a config excluding its own id **halts fail-stop
immediately** (heartbeat freezes; it never re-claims `LEADER`/`CAN_SERVE`;
zombie datagrams from its old address cannot disrupt the survivors — the
known-source guard drops anything not in the current member set). A halted
process does not exit on its own — it parks. Decommission it like any other
dead process (`systemctl stop`/kill the unit, power off the box); there is no
further protocol step. **Tombstone/fresh-id rule:** the removed id can
**never** rejoin under any circumstances, including reusing the same physical
host or address for a *different* id — `add-learner` on a tombstoned id is
permanently refused (`Tombstoned`). If you are replacing hardware, always
bring the replacement up with a **new** id, never the old one — **and a fresh,
empty instance directory.** A new id must never inherit an old id's on-disk
`journal/`/`state/` (in particular `state/config.state`, whose durable
`ConfigRecord` still carries the OLD id's now-stale tombstones/membership) —
reusing a directory across ids is undefined, not merely discouraged: the M7
gate-authoring work hit exactly this by reusing a freed local test-harness
directory for a new id and produced a node stuck at a random half-adopted
config version, sorted out only by wiping the directory before every reused
host slot's new generation (`bench-infra/scripts/m6_fleet_gate.py`'s
`reset_dir`).

**A removed node's binary refuses to restart on its old instance dir**
(`tombstoned in the recovered cluster config`) — this is the intended
decommission backstop, not an error to work around. Previously a restarted
removed node would boot as a permanently-idle zombie (the runtime
`HaltRemoved` latch is version-gated and never re-fires on boot, so a fresh
process never re-halted itself); `Node::start` now checks the just-recovered
config against its own id at construction and fails loudly instead, so an
orchestrator sees a failed unit rather than a healthy-looking idle one. If a
node was removed while its removal was still uncommitted and the cluster
later truncated it (rare; requires losing the removal's own quorum), the
wrongly-halted node's recourse is the same wipe-and-rejoin as below.

**Wipe-and-rejoin (NoCommonPrefix):** unrelated to reconfiguration — if a
rejoining node's log has no common prefix with the leader (the leader purged
past the divergence), the node truncates to 0 and rejoins from the snapshot
floor (`wipes()` increments). This is automatic and safe.

### Upgrade order (mixed-version clusters)

M7 bumps the protocol version once (`FRAME_TYPE_CONFIG=4` + admin datagram
kinds 16/17 are new wire surface; a v2.0 node refuses a mismatched protocol
version at every IPC/network entry — the existing rule, unchanged). **Roll
every binary (node + service + client + `uc2ctl`) to v2.1 FIRST, while the
cluster is still fully static** (issue zero config changes during the
rolling-restart window), **then** start reconfiguring. There is no supported
mixed-version config-change path — a v2.0 node in the member set during a
config change is not a tested or safe configuration.

---

## 7. Gate binaries

The `m*_gate` example binaries (`cargo run -p uc2_node --example m6_gate --
all --secs N`) run the milestone scenarios and **exit 1 on an honest FAIL** — a
zero exit is the pass signal, so they compose in CI. They guard `/tmp` (RAM
tmpfs with a quota) and want journals on a real disk; pass an explicit
`journal_root` on the ext4 volume for a fleet run.
