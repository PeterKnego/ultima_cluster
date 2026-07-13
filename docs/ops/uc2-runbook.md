# UC v2 operations runbook

Operator-facing reference for running, observing, and repairing a UC v2 cluster.
This is the counterpart to the design/task docs: it assumes the system works and
tells you how to see what it is doing and what to do when it misbehaves.

Scope: M1–M6 (consensus, log, snapshots, purge, learners). Everything here is
observable through the shared **cnc page** (`instance_dir/cnc2.dat`) — a
fixed-layout 4 KiB control page every process mmaps — plus the node's own stats
accessors.

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
| `state/`                          | node     | Raft durables (`StableValue`s): vote, term-map, committed, output-progress, snapshot floor. |
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
voted in, i.e. a split-brain hazard, not just data loss), and `snapshots/` must
all survive a power loss. Only the rings, `cnc2.dat`, and `log.buf` are
volatile-safe (rebuilt/re-primed on boot). Since `journal/`, `state/`, and
`snapshots/` all live *under* the instance dir, the simplest safe posture is:
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

## 6. Adding / removing a learner (a box)

A **learner** is replicated-to but never counted — no vote, no quorum slot, no
flow-control window, no read-quorum ack. Use it to add read capacity or stage a
new host without changing the voting quorum.

**Add a learner host:**

1. Bring up the new node with its own instance dir, its own `(id, addr)`, and a
   member map that lists the voters. Put the new id in `NodeConfig.learners`
   (NOT in `members`); its own id must **not** appear in `members`.
2. Add the same `(id, addr)` to every voter's `learners` list so they fan DATA /
   commit-gossip / term-maps out to it.
3. The learner catches up via ordinary NAK-replay — or, if the leader has
   purged below its join point, via a snapshot session (watch
   `incoming_snapshot_pos`). Confirm its `commit` converges to the cluster
   `commit`; killing it must never stall the leader's commit.

**Remove a learner:** drop it from the voters' `learners` lists and stop the
learner node. No config-change round is needed (a learner is not in any quorum),
and no election is triggered.

**Wipe-and-rejoin (NoCommonPrefix):** if a rejoining node's log has no common
prefix with the leader (the leader purged past the divergence), the node
truncates to 0 and rejoins from the snapshot floor (`wipes()` increments). This
is automatic and safe; it is the wipe half of reconciliation, not an error.

---

## 7. Gate binaries

The `m*_gate` example binaries (`cargo run -p uc2_node --example m6_gate --
all --secs N`) run the milestone scenarios and **exit 1 on an honest FAIL** — a
zero exit is the pass signal, so they compose in CI. They guard `/tmp` (RAM
tmpfs with a quota) and want journals on a real disk; pass an explicit
`journal_root` on the ext4 volume for a fleet run.
