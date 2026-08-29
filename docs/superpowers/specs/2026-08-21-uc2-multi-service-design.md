# UC v2 (M14) — multi-service, stage 1: one command log, N state machines

**Date:** 2026-08-21 (re-baselined 2026-08-26, see §0)
**Status:** approved design (brainstorm 2026-08-20/21); re-baselined on the
shipped M11–M13 tree; re-baseline **reviewed 2026-08-27** (§0 item 2 demoted
to a plan task — no deployments exist, so there is no compatibility question,
only the tool's correctness against the new layout). Next: the implementation
plan.
**Baseline:** `origin/main` 4fcad3c (M1–M13 complete, `v2.7.0`, wire 0.5.0,
cnc page 2.0, remote protocol v1, ingress ring `ULTRNG2`).

## 0. Re-baseline 2026-08-26 — what M11–M13 changed under this design

The brainstorm was held against 797bfbf (M1–M10, `v2.4.0`) and numbered this
work M13. The remote-path work took the M13 slot on 2026-08-24; this is
**M14**. M11 (survivable), M12a–d (adoptable) and M13 (remote path) have since
shipped and touch five things this spec relied on. Every item below is
corrected in place in the body; this section is the changelog for review.

| # | what shipped | effect on this spec | fixed in |
|---|---|---|---|
| 1 | M11 `free_disk_bytes` @ 3840, M12b `admin_auth` @ 3904, M13 hole counters @ 3968/3976 (`uc_protocol/src/v2/cnc.rs:152-227`) | the two page-1 fields this spec placed at 3840/3904 collide; the only free page-1 line is 4032..4096 | §3.2: both fields move to the 4032 line (u64 pair, one writer — the same pattern M13 used at 3968/3976); page 1's reserved band is thereby exhausted |
| 2 | M11 offline backup/verify/restore (`uc2_node/src/backup.rs`, `uc2ctl backup|verify-backup|restore`) copies `snapshots/` **flat**, filtered to `snap-<pos>.ultsnap` names, and verifies one coverage invariant (newest snapshot ≥ `journal_first_base`) | `snapshots/<id>/` subdirectories would be silently skipped by backup and restore, and `verify` has no per-FSM coverage form. **Review 2026-08-27: not a design item** — there are no deployments, so no old artifacts to stay compatible with; it is a mechanical plan task (teach the three tools the per-id layout) | new §7.5 (plan task, not a decision) |
| 3 | M12a remote protocol v1 (`uc2_remote/src/frame.rs`): `SUBMIT`/`QUERY` carry no service selector; the standing rule is that the remote protocol stays v1 | `submit_to`/`submit_all`/`query_*_on` have no remote form | §6.4: the remote path is FSM 0 in stage 1; a selector is a protocol-v2 item, out of scope |
| 4 | M13 client `Engine` is a `(SendHalf, PollHalf)` split with an all-atomic `Slot` (`uc2_client/src/slots.rs:44-49`); the gateway's grant budget is `EdgeConfig::max_inflight` less 1/8 (`uc2_gateway/src/edge.rs:207-209`), not the cnc admission field, and `SubmitError::Backpressure` already squeezes a connection's credits (`edge.rs:1357-1387`) | the fan-in buffer for `submit_all` cannot live in the atomic slot; FSM back-pressure must reach remote clients | §6.2 (fan-in buffer on the `PollHalf`), §5.2 (the new door term surfaces as today's `Backpressure`; no gateway change) |
| 5 | M11 boot reservation (`buffer_bytes` + 14 MiB of rings, fallocated; ENOSPC = named startup refusal) | every extra FSM adds 5 MiB of rings | §5.5 |

Also swept, no design change: `attach()` is `pub(crate)` — the public attach
surface is `ServiceBuilder::start()` / `start_with_snapshots()` (§4.1 now
names those); `ServiceConfig` is `{instance_dir, app_id, snapshot_policy}` and
gains `service_id`; `SnapshotStore::open(instance_dir)` gains the id;
`Sessioned<S>` keeps its client table inside the SM state (snapshotted), so it
is per-FSM for free (§4.5); the spec's `CNC_TOTAL_LEN` was never a real
constant — the page-size constant is `CNC_PAGE_LEN` (§3.1);
`SNAP_BEGIN_FIXED_LEN` is 26 today, so the +9 lands at 35 (§7.3); M12d's fuzz
targets already cover the three decoders this spec changes (§12); the M12
`upgrade-a-cluster.md` "restart a host's processes together" procedure is the
one the cnc flag day reuses (§13). One new failure mode fell out of item 2:
growing the declared set after purge has run is unrecoverable for the new id
(§8, §11).

## 1. Goal and locked decisions

Let one UC node host **N independent state machines (FSMs) over one replicated
command log** — Aeron Cluster's "several `ClusteredService`s per node" shape.
Each FSM is its own process with its own lifecycle, snapshots, output progress
and egress; every committed command is applied by every FSM; the client picks
whose response it wants. Today UC is structurally one FSM per node (one
`ServiceProgress` band on the cnc page, one apply agent, one egress ring).

Decisions locked during the brainstorm:

| Decision | Choice |
|---|---|
| Staging | **Stage 1 (this spec):** one log → N FSMs. **Stage 2 (future, not this spec):** N independent logs, each with its own FSM set, in one daemon — "N of stage 1 in parallel". Stage 1 must not close that door (§10). |
| Command delivery | **Broadcast** — every FSM applies every committed frame. No per-command routing to a subset, no service id in the log frame or on the wire. |
| Process model | **One process per FSM.** Each attaches to the instance dir with a `service_id`. In-process hosting of several FSMs is not provided (a user may spawn N attaches in one binary; nothing stops it, nothing helps it). |
| Response to the client | **Designated responder, chosen per request.** Every FSM publishes its response on its own egress ring; `submit` defaults to FSM 0's answer; `submit_to(cmd, id)` and `submit_all(cmd)` select one or fan in all. |
| FSM pacing (`fsm_lag`) | **Bounded with back-pressure, always.** One knob: `lockstep` (no FSM starts frame *k+1* until every FSM finished frame *k*) or a byte bound. **There is no unbounded mode** — an FSM slower than the log's sustained rate can never catch up from journal replay (replay is strictly slower than live apply), so "unbounded" is a silent death spiral, not graceful degradation. Default `buffer_bytes / 4`. |
| Cluster-wide back-pressure | **Q — quorum-gated durable reports.** A node reports `min(validated_frontier, min_applied + fsm_lag)` to the leader, so when a commit quorum's FSMs lag, commit stalls and the leader's admission door closes. Leader-local-only pacing (L) was rejected: it leaves follower FSMs unbounded and turns the same problem into a failover stall. |
| Declared set | **Static per node config** (`[services] ids = [...]`); change = restart with a new config. Must match cluster-wide; checked where it can be (snapshot transfer), exported so it can be alerted on. |
| Cap | **8 FSMs per log** (`CNC_MAX_SERVICES = 8`). Stage 2 multiplies logs, not slots. |
| Single-service compatibility | No `[services]` section ⇒ `{0}`, default lag. `counter-service` and every existing deployment behave as before after the flag-day cnc/wire bump. |
| Versioning | cnc page 2.0 → 3.0 (same-host flag day); wire 0.5.0 → 0.6.0 (`SNAP_BEGIN` payload only; cross-host flag day). |

### What does not change

`uc2_consensus`, `uc2_net`'s data/NAK/report planes (the receiver is untouched —
Q is a value the node already publishes for it), `uc2_crypto`, the log frame,
the UDP datagram header, elections, the archive, the Lean model, the sim's
safety invariants, the M12b admin plane (HMAC + audit log), the M12a remote
protocol (stays v1 — §6.4), the M13 ingress/query MPSC ring format
(`ULTRNG2`, per-record commit) and the gateway's grant budget (§5.2). The one
wire change is the snapshot-session `SNAP_BEGIN`
payload (one artifact per FSM, §7.3), so the wire protocol bumps
**0.5.0 → 0.6.0** — a flag day like every prior bump (a 0.5.0 peer refuses at
the version check; upgrade all nodes together). The same-host IPC surface
changes behind the cnc page version (§3.4).

## 2. Why this shape (and what Aeron does)

Aeron Cluster delivers every ingress message to every service; each service
tails the log on its own subscription image at its own pace; back-pressure is
structural (the log publication cannot advance past the slowest consumer's
position by more than the term length); service acks exist only for snapshot
and shutdown coordination; which service answers a client is application
convention. That is exactly UC's existing apply model — services poll the
shared log buffer in place, nobody dispatches — with two differences that
this design keeps deliberately:

1. **UC bounds the lag explicitly and lets the operator pick lockstep.**
   Aeron's bound is "one term buffer" and implicit. UC's `fsm_lag` is a
   first-class policy with lockstep as the degenerate value (= "one frame").
2. **UC makes the back-pressure a quorum property (Q).** A follower reports
   less than it holds when its FSMs lag; the leader counts commits on what is
   reported. Reporting less than you hold is always safe in Raft — it only
   delays commit — and elections rank on the node's own `(last_term,
   last_durable)`, never on reports, so vote freshness is unaffected.

The mechanism in UC terms:

```
log.buf (ring) ── every FSM tails it at its own cursor ──▶ FSM proc 0,1,…,N-1
                                                            │ each writes its own cnc slot
                                                            ▼
        cnc2.dat page 2: ServiceSlot[8] {applied, epoch, snapshot_pos, output_completed, heartbeat, …}
                                                            │
   FSM-side barrier:   before frame [p, p+len):  floor = min(slot.applied)
                       lockstep: wait while floor < p;  bounded: wait while p+len-floor > fsm_lag
   node-side (leader): admission door also requires  append - min_applied <= fsm_lag
   node-side (all):    AppendPosition report ceiling = min(validated_frontier, min_applied + fsm_lag)
```

## 3. Control page (`cnc2.dat`) and config

### 3.1 Page 2: `ServiceSlot[CNC_MAX_SERVICES]`

`cnc2.dat` grows from 4 KiB to **8 KiB** (`CNC_PAGE_LEN`: 4096 → 8192 —
`uc_protocol/src/v2/cnc.rs:46`; there is no separate total-length constant).
Page 1 (offsets 0..4096) keeps its byte layout exactly; every existing offset
test holds, including the M13 `offsets_do_not_overlap` assertion that the
3968 line ends at 4032 (`cnc.rs:568`). Page 2 (4096..8192) is
`ServiceSlot[8]`, stride 512 B (8 cache lines), **one writer per line**:

| line | offset in slot | field | width | writer |
|---|---|---|---|---|
| 0 | +0 | `status`: `service_id:u8 \| attached:u8 \| pad:u16 \| incarnation:u32` | 64 B line | service (attach/detach) |
| 1 | +64 | `applied` (position) | u64 | service apply agent |
| 2 | +128 | `epoch` | u64 | service (attach-time `fetch_add`, AcqRel) |
| 3 | +192 | `output_completed` (position) | u64 | service output agent |
| 4 | +256 | `snapshot_pos` (position) | u64 | service builder agent |
| 5 | +320 | `heartbeat_ns` | u64 | service apply agent |
| 6 | +384 | `lag_waits` (count) | u64 | service apply agent |
| 7 | +448 | reserved (zero) | — | — |

Constants `CNC_OFF_SERVICE_SLOTS = 4096`, `CNC_SERVICE_SLOT_STRIDE = 512`,
`CNC_MAX_SERVICES = 8`, per-field `CNC_SVC_OFF_*`, pinned in **both**
`uc_protocol::v2::cnc` and `uc2_log::cnc` with the same offset/size
const-asserts and unit tests the `PeerSlots` band has.

### 3.2 Page 1: legacy singular fields become node-written aggregates

| offset | field | new meaning | writer |
|---|---|---|---|
| 512 | `service_applied` | `min(slot.applied)` over declared ids | consensus agent |
| 576 | `service_epoch` | **retired**, held at 0 (readers move to the slot) | — |
| 640 | `output_completed` | `min(slot.output_completed)` | consensus agent |
| 960 | `service_heartbeat_ns` | `min(slot.heartbeat_ns)` (the oldest) | consensus agent |
| 1152 | `service_snapshot_pos` | `min(slot.snapshot_pos)` | consensus agent |

Computed once per consensus poll cycle (N acquire loads, a store per field only
when the value changed). Effect: `uc2ctl status`, the M10 metric families, the
dashboard and the purge-floor logic keep reading one number whose meaning is
now "the slowest FSM", and the single-writer rule per cnc line is preserved (the
writer changed from "the service" to "the node", it did not become shared).

New page-1 fields in the **last free line** of page 1 (4032..4096 — the
reserved band below it is used up: `admission_bytes` 3712, `seal_failures`
3776, M11 `free_disk_bytes` 3840, M12b `admin_auth` 3904, M13
`ingress_holes_skipped`/`query_holes_skipped` 3968/3976;
`docs/reference/cnc-page.md`):

| offset | field | writer |
|---|---|---|
| 4032 | `services_declared: u64` (bit *i* set ⇔ id *i* declared) | node at startup, once |
| 4040 | `fsm_lag_bytes: u64` (`0` ⇔ lockstep) | node at startup, once |

Both are plain `AtomicU64`s sharing one line with one writer — the pattern
M13 set with the 3968/3976 pair (`PaddedAtomicU64` cannot sit at +8;
`cnc.rs:204-227`). Services and clients read the declared set and the policy
**from the page**, not from the config file. **Page 1 is thereby full.** A
later page-level field grows the file by another 4 KiB behind the next cnc
major bump — the same flag day as this one, and cheap (the file is
version-gated and recreated at every node start); per-slot fields go in the
slot's reserved line 7.

### 3.3 Config (`node.toml`)

```toml
[services]
ids     = [0, 1, 2]     # absent section ⇒ [0]
fsm_lag = "16MiB"       # byte size, or "lockstep"; default: buffer_bytes / 4
```

Startup refusals (named, M9 style, before any file is created): empty `ids`,
duplicate id, id ≥ 8, unparsable `fsm_lag`, `fsm_lag >= buffer_bytes / 2`
(a "bounded" policy must provably keep FSMs on the ring in steady state:
the ring's overrun margin is `max_claim`, and half the ring is the conservative
bound that also leaves room for the leader's own admission window). The
existing `#[serde(deny_unknown_fields)]` stays; `[services]` is a new known
table.

### 3.4 Versioning

Two gates, both flag-day:

- **cnc page `CNC_V2_VERSION` 2.0 → 3.0** (same-host). Every same-host party
  (node, each service, clients, `uc2ctl`, the metrics endpoint) already refuses
  a page whose version differs; the ring record formats (§6.3) ride the same
  gate. Restart each host's node + services + clients as a unit.
- **Wire `version::CURRENT` 0.5.0 → 0.6.0** (cross-host), because `SNAP_BEGIN`'s
  payload gains per-service fields (§7.3). Header, `DATA`, `NAK`,
  `AppendPosition`, `TermMap`, admin datagrams are byte-identical to 0.5.0; a
  0.5.0 peer refuses a 0.6.0 datagram at the existing version check. Upgrade
  all nodes together (the standing rule since 0.5.0).

## 4. Service SDK (`uc2_service`)

### 4.1 Attach

`ServiceConfig` (today `{instance_dir, app_id, snapshot_policy}`,
`uc2_service/src/config.rs:12-18`) gains `service_id: u8` (default 0, builder
`.service_id(id)`). The public attach surface is `ServiceBuilder::start()` /
`start_with_snapshots()` (`uc2_service/src/lib.rs:123,195`); both run the
crate-private `attach()` (`attach.rs:40`), which becomes:

1. Open `cnc2.dat`; version/app_id/instance_id checks as today.
2. Refuse if bit `service_id` is clear in `services_declared` →
   `ServiceError::ServiceNotDeclared { id, declared }`.
3. Take `service.<id>.lock` (exclusive flock; mirrors `instance.lock`) →
   `ServiceError::AlreadyAttached { id }`.
4. Open `svc_query.<id>.ring` (consumer) and `egress_service.<id>.broadcast`
   (producer). The node creates both for every declared id at startup.
5. Drift check (`last_applied <= durable`) as today; store
   `slot[id].applied = last_applied`; write `slot[id].status`
   (`attached = 1`, `incarnation += 1`); then `slot[id].epoch.fetch_add(1)`
   (AcqRel) — the same epoch→applied→epoch discipline the barrier relies on.
6. Detach (clean shutdown): `attached = 0`; the lock is released by the OS on
   exit either way.

### 4.2 Apply loop and the lag barrier

`apply_cycle` is today's loop plus one step **before applying each frame**
at `[p, p + len)`:

```
floor = min over declared ids of slot[i].applied      // N acquire loads
if lockstep:  wait while floor < p                     // all FSMs finished frame k-1
else:         wait while p + len - floor > fsm_lag
```

Waiting is the same spin/yield the follower already uses for
`Batch::NotCommitted`; the heartbeat keeps ticking during a wait so a waiting
FSM is never mistaken for a dead one; `slot[id].lag_waits += 1` per wait
episode. The barrier reads only shmem (no node involvement), is role-agnostic
(runs identically on leader and followers), and **does not apply during
journal replay** — a replaying FSM is by definition the one holding `min`
down, so it never needs to wait; live FSMs wait for it.

Invariant the barrier gives: at any instant, for any two declared, attached
FSMs *a*, *b*: `applied_a − applied_b ≤ fsm_lag` (bounded) or
`applied_a − applied_b ≤ one frame` (lockstep). Under lockstep, `min(applied)
≥ frame end` is exactly "every FSM's response for this command exists".

**As-built errata (M14a):** the invariant above is a target cap on *live*
apply and does not cover journal replay. On a FOLLOWER whose FSM *k* is stuck
(slow SM, or itself mid-replay) while the cluster's quorum keeps committing, a
sibling FSM *j* that falls off the live ring (`Overrun`) rejoins via
`replay_into` to the archived frontier — not to `applied_k + fsm_lag` — so
`applied_j − applied_k` may exceed `fsm_lag` on that follower for as long as
the replay takes. This is safe (applying more of an already-committed log is
always sound; responses are leader-only, so a follower's over-eager FSM
publishes nothing early) and it is bounded on the leader itself, because the
leader's admission door enforces the barrier before a frame is even appended
— so the leader's own FSMs never see this excursion. The M14c slow-FSM oracle
must therefore sample the `fsm_lag` bound on the **leader only**; sampling it
on a follower mid-replay is expected to (harmlessly) trip.

**As-built errata (M14a, lockstep wait):** under lockstep the `Wait` is served out of line by `lockstep_wait` — spin re-planning, then yield (with a heartbeat refresh), and only then the agent's 50 µs sleep — because a lockstep FSM that sleeps on a live sibling stalls every sibling's next frame and the set cascades into sleeping in lockstep (measured 18 k frames/s before, 631 k after at N=2). A bounded `Wait` still goes straight to the sleep: it is `fsm_lag` bytes ahead of the slowest FSM, and spinning on that FSM's `applied` line slows it (−6 % at N=8 when tried). `lag_waits` counts ladder exhaustions (a stalled or dead sibling), not ladder spins.

Everything else in the loop is unchanged: `apply(position, cmd)` per frame,
`slot[id].applied.store_release(...)` after the frame, `Overrun` → journal
replay → rejoin.

### 4.3 Responses and queries

- Each FSM publishes `MSG_V2_RESPONSE` (and `MSG_V2_RETRY`) on **its own**
  `egress_service.<id>.broadcast`. Record `header_extra` stays
  `(client_id, local_seq)`; there is no service id in the record — the ring *is*
  the service id.
- The SDK drains `svc_query.<id>.ring`; payload `expected_epoch ++ query` as
  today (the node resolved the target when it picked the ring); the epoch
  check is against `slot[id].epoch`.

### 4.4 Per-service state

`snapshots/<id>/snap-<pos>.ultsnap` (`SnapshotStore::open(instance_dir, id)`
— today `open(instance_dir)`, `uc2_service/src/snapshots.rs:49`),
`state/output_progress.<id>.state`, `slot[id].snapshot_pos` (builder agent),
`slot[id].output_completed` (output agent), `slot[id].heartbeat_ns`.

### 4.5 `Sessioned<S>` (M12a exactly-once) per FSM

`Sessioned<S>` keeps its per-client dedup table inside the SM state
(`clients: BTreeMap<u64, ClientState>`, `uc2_service/src/session.rs:82`) and
bundles it into the snapshot (`session.rs:284-353`), so a `Sessioned` FSM is
per-FSM with no change: each FSM's table is rebuilt from the same log and is
identical across FSMs by determinism (same `client_id ++ seq` envelopes, same
FRESH/REPLAYED/EXPIRED verdicts). A cluster may mix `Sessioned` and plain FSMs
freely — the envelope is applied per FSM, not per log.

## 5. Node (`uc2_node`)

### 5.1 Aggregates

The consensus agent computes `min_applied`, `min_snapshot_pos`,
`min_output_completed`, `oldest_heartbeat` over **declared** ids each poll
cycle and publishes them to page 1 (§3.2). A declared-but-unattached id reads
`applied = 0` (fresh page) or its last life's value, and therefore holds the
min down — **intentional**: an absent FSM back-pressures writes (§8) rather
than being silently skipped.

### 5.2 Admission door (leader)

```
admission_open = (append - commit      <= admission_bytes)        // today
              && (append - min_applied <= fsm_lag_eff)            // new
fsm_lag_eff    = fsm_lag            (bounded)
               = max_claim          (lockstep: at most one frame past the FSMs)
```

Same door for linearizable-read admission (the existing `admission_open` call
sites — `fn admission_open(append, commit, budget)`, `uc2_node/src/node.rs:385`,
called at `node.rs:1459` and `node.rs:3218`; no new site). The FSM term is a
second predicate at the same sites, not a change to `admission_bytes`
(cnc 3712 keeps its meaning).

**Remote clients (M12a/M13).** The gateway derives its grant budget from its
own `EdgeConfig::max_inflight` (`uc2_gateway/src/edge.rs:207-209,769`), not
from the node's door, and already handles the local `Engine`'s
`SubmitError::Backpressure` by halving the connection's credits and re-trying
(`edge.rs:1357-1387`). A door closed by FSM lag must surface to the `Engine`
exactly as a door closed by `append − commit` does today, so FSM back-pressure
reaches remote clients through that existing path with **no gateway change**.
Plan check item: confirm which client-visible outcome a closed door produces
today (ring-full `Backpressure` vs `MSG_V2_RETRY` on `egress_node`) and that
the FSM term produces the same one — no new outcome is introduced.

### 5.3 Q — quorum-gated durable reports

Today `publish_validated_frontier` (`uc2_node/src/node.rs:4739-4746`) publishes
the `(validated_term, validated_frontier)` pair that the receiver clamps every
`AppendPosition` report to (term stored first, then position, both `Release`,
so a torn read fails the leader's attestation check — the safe direction). It
becomes:

```
ceiling = min(sm.validated_up_to(), min_applied + fsm_lag_eff)
term    = term_map.term_at(ceiling - 1)          // attest the byte below the report
store term (Release); store ceiling (Release)    // unchanged ordering
```

The receiver (`uc2_net`) is untouched. Properties:

- `ceiling ≤ validated_up_to` always — never attests content it hasn't
  validated (the 0.5.0 soundness argument is unchanged).
- Monotone except via truncation (same as the frontier today).
- Effect: if a commit quorum's FSMs lag by more than `fsm_lag`, the leader's
  `CommitTracker` cannot advance → `append − commit` grows → the leader's door
  closes → cluster-wide back-pressure. A lagging **minority** does not stall
  the cluster (as in any Raft): it falls to journal replay and recovers when
  load drops, or at failover, where the new leader's door stays closed until
  its own FSMs catch up (a bounded stall with no new appends meanwhile).
- Elections: `RequestVote{last_term, last_durable}` come from the node's own
  state (`ElectionSm`), not from reports — unchanged.
- Flow control: the leader's quorum-paced window is an order statistic over
  reported positions; a node with lagging FSMs therefore looks like a slow
  follower and is paced out of the window when it is in the minority — the
  intended direction. (Check item for the plan: confirm no leader-side logic
  treats a low report as "needs snapshot" — NAK/deep-replay are
  follower-initiated, so none is expected.)

### 5.4 Query routing and the read barrier

- `query.ring` (`MSG_V2_QUERY`) payload becomes `service_id:u8 ++ query bytes`
  (`header_extra` unchanged). `drain_query_ring` forwards to
  `svc_query.<id>.ring` (an array of N `SpscProducer`s, one per declared id);
  an undeclared id is answered with `MSG_V2_BAD_SERVICE` (new constant) on
  `egress_node.broadcast`, keyed by the client pair.
- `advance_pending_reads` carries `service_id` per pending read and checks
  `slot[id].epoch / slot[id].applied` (epoch-stable, `applied ≥ commit_at`)
  instead of the singular band. The quorum probe round (`read_round.rs`) is
  unchanged — it certifies a commit position, which is service-agnostic, so
  one round serves reads for any FSM.
- `can_serve` / `/readyz`: unchanged.

### 5.5 Rings and files created by the node

For every declared id: `svc_query.<id>.ring` (SPSC), `egress_service.<id>.broadcast`,
`snapshots/<id>/` (directory). `instance.lock`, `ingress.ring`, `query.ring`,
`egress_node.broadcast`, `log.buf`, `journal/`, `state/`, `audit.jsonl` as
today. The legacy `svc_query.ring` / `egress_service.broadcast` names are
**not** created (a cnc-2.0 service would fail the version check first anyway).

**Boot reservation (M11).** The IPC files are fallocated at creation, so
ENOSPC is a named startup refusal (`docs/reference/instance-directory.md`).
Today's footprint is `buffer_bytes` + 14 MiB of rings (~78 MiB at the
defaults). Each declared id adds `svc_query.<id>.ring` (1 MiB) +
`egress_service.<id>.broadcast` (4 MiB) — the sizes at
`uc2_node/src/node.rs:5015,5019` — so the reservation becomes
**`buffer_bytes` + 14 MiB + 5 MiB × (N − 1)** (+4 KiB for page 2): ~113 MiB at
N = 8 with the default buffer. `instance-directory.md`'s "free space needed
before boot" row gets the formula.

## 6. Client SDK (`uc2_client`) and IPC records

### 6.1 Engine

`Engine::attach` (M13: returns the `(SendHalf, PollHalf)` split,
`uc2_client/src/engine.rs:203-222,253`) reads `services_declared` from the
page and opens `egress_service.<id>.broadcast` for every declared id;
`PollHalf::poll` round-robins N service rings + `egress_node` (today exactly
two, `engine.rs:267-268,447-448`). `client_id` allocation (`next_client_id`)
stays node-wide.

### 6.2 Slot table and API

Each `Slot` (all-atomic today: `owner`/`user_data`/`deadline_ns`/`kind`,
`uc2_client/src/slots.rs:44-49`) gains `expected: AtomicU8` and
`received: AtomicU8` bitmasks. The fan-in buffer for `submit_all` does **not**
live in the slot (which must stay lock-free and fixed-size): it is a
`PollHalf`-owned `Vec<Option<Bytes>>` per slot index — the `PollHalf` is the
single owner of completion, so it needs no synchronisation — and is handed to
the callback whole when `received == expected`.

| call | `expected` | completion |
|---|---|---|
| `try_submit(user_data, cmd)` / `PipelinedClient::submit` / `Client::submit` | `{0}` | FSM 0's response |
| `try_submit_to(user_data, id, cmd)` / `submit_to` | `{id}` | FSM `id`'s response |
| `try_submit_all(user_data, cmd)` / `submit_all` | declared set | `Vec<(u8, Response)>` ordered by id (`Ticket<Vec<(u8, R)>>` on the blocking layers) |
| `try_query` / `query_snapshot(q)` / `query_linearizable(q)` | `{0}` | as today |
| `query_*_on(id, q)` | `{id}` | as today |

(`Engine` names are the M13 `try_*` family, `engine.rs:324,330`; the blocking
names are `PipelinedClient`'s, `uc2_client/src/pipelined.rs:154-181`, mirrored
by the `Client` shim, `client.rs:102-115`.) A response arriving on ring *r*
for a slot with bit *r* clear in `expected` is dropped; completion fires when
`received == expected`. Under lockstep all N arrive (nearly) together; under
bounded, as each FSM gets there. The `ReqKind` kind-check belt and the
generation discipline are unchanged. `submit_to`/`submit_all`/`query_*_on`
with an undeclared id fail locally (`ClientError::ServiceNotDeclared`) before
touching a ring.

### 6.3 Record formats (all gated by cnc 3.0)

| ring | type | payload |
|---|---|---|
| `ingress.ring` | `MSG_V2_SUBMIT` | unchanged |
| `query.ring` | `MSG_V2_QUERY` | **`service_id:u8 ++ query`** |
| `svc_query.<id>.ring` | `MSG_V2_SVC_QUERY` | unchanged (`expected_epoch ++ query`) |
| `egress_service.<id>.broadcast` | `MSG_V2_RESPONSE` / `MSG_V2_RETRY` | unchanged |
| `egress_node.broadcast` | `MSG_V2_NOT_LEADER` / `MSG_V2_RETRY` / **`MSG_V2_BAD_SERVICE`** (new; payload `service_id:u8`) | |

The log frame header (`OFF_RESERVED1` included) and the UDP datagram header
are **not** touched; the only datagram payload that changes is `SNAP_BEGIN`
(§7.3). The MPSC ring record framing (M13 `ULTRNG2`, per-record commit) is
untouched — the `service_id` byte is inside the `MSG_V2_QUERY` payload.

### 6.4 Remote clients (`uc2_remote` / `uc2_gateway`): FSM 0 in stage 1

The remote protocol v1 (`uc2_remote/src/frame.rs`: `SUBMIT` = 4, `QUERY` = 5;
`PROTOCOL_VERSION = 1`) carries no service selector in any request frame, and
the standing rule (CLAUDE.md, `docs/reference/semver-policy.md`) is that it
stays v1. In stage 1 the edge relays every `SUBMIT`/`QUERY` through the local
`Engine`'s default calls, so **a remote client always gets FSM 0's answer**
(`submit`/`query_*` semantics); `submit_to`/`submit_all`/`query_*_on` are
shmem-only. A remote selector is a protocol-v2 item (one byte in the request
frame's flags/header, plus `RESPONSE` fan-in) — out of scope (§11), and the
edge needs no change for stage 1 beyond attaching to a cnc-3.0 page.

## 7. Snapshots, purge, output, replay — per FSM

### 7.1 Snapshots and the purge floor

Each FSM snapshots on its own policy into `snapshots/<id>/` and writes
`slot[id].snapshot_pos`. The purge floor is the page-1 aggregate
`min(snapshot_pos)` over declared ids, so `maybe_persist_snapshot_floor` and
`PurgePolicy::BelowSnapshot` are unchanged in code and the journal is never
purged past the slowest FSM's snapshot — no FSM can be stranded below the floor
by a faster sibling. A declared FSM that has never snapshotted holds the floor
at 0 (= purge inert), visible via `uc2_service_snapshot_pos_bytes{service}`.
`state/snapshot.state` stays one scalar (it is the floor).

### 7.2 Output handlers

Per FSM, leader-only, at-least-once, contract unchanged. Durable marker
`state/output_progress.<id>.state`; `slot[id].output_completed`; page-1
aggregate = min. A `Permanent` failure in FSM 1 advances only FSM 1's marker.

### 7.3 Inbound snapshot transfer (learner join / below-floor node)

A snapshot session carries **one artifact per declared id**. `SNAP_BEGIN`'s
*payload* gains `service_id:u8` and `services_declared:u64` after the existing
fixed fields (`SNAP_BEGIN_FIXED_LEN`: 26 → 35, `uc_protocol/src/v2/datagram.rs:159`;
the datagram header is untouched) — this is the 0.5.0 → 0.6.0 wire bump
(§3.4). The receiver refuses
the session if the sender's declared set differs from its own
(`declared-set mismatch`, a named refusal; see §8). Each artifact lands in
`snapshots/<id>/`; floor adoption uses the min over the received set; each
follower FSM installs its own artifact and tail-replays (today's mechanism,
per id).

### 7.4 Replay

`replay_into` is unchanged per FSM: own `last_applied`, own store, own
below-floor gap guard (`SnapshotRequired` → install own snapshot). Concurrent
replays read the journal concurrently (read-only readers; the archive is the
only writer — already the case for node-side deep-NAK replay running beside
the service).

### 7.5 Offline backup / verify / restore (M11) — per FSM

M11's `backup_instance` / `verify_artifact` / `restore_artifact`
(`uc2_node/src/backup.rs:373,433,570`; `uc2ctl backup | verify-backup |
restore`) copy `journal/` and `state/` by globbing the directory — so
`state/output_progress.<id>.state` is covered as-is — but copy `snapshots/`
**flat, filtered to `snap-<pos>.ultsnap` names** (`backup.rs:391`,
`scan_snapshots` `:649-667`), and `verify` checks one coverage invariant,
`newest_snapshot ≥ journal_first_base` (`:486-492`). Unchanged, a backup would
silently drop every `snapshots/<id>/` directory. Stage 1 therefore:

- `backup`/`restore` copy `snapshots/<id>/` for every id directory present
  (backup is offline and config-blind: it takes what is on disk, not the
  declared set), keeping the per-directory `snap-*.ultsnap` filter.
- `verify`'s coverage invariant becomes **per id**: for every `snapshots/<id>/`
  present, `newest(id) ≥ journal_first_base`, else
  `BackupError::Hole { service: id }`. The artifact-wide "newest snapshot" in
  the `MANIFEST` becomes a per-id list; `check_manifest` compares per id.
- `restore` keeps its `TargetNotEmpty` refusal; the declared-set consequence
  of restoring an artifact with ids `{0,1}` under a config declaring
  `{0,1,2}` is the §8 row "declared set grew after purge ran".
- `docs/how-to/back-up-a-cluster.md` gains the per-FSM statement; the M11
  backup round-trip test runs once with two ids (§12).

## 8. Failure modes

| situation | behaviour |
|---|---|
| FSM *k* crashes | `slot.attached = 0`, heartbeat ages; others continue up to the bound then wait; leader admission closes at the bound; on a follower, Q lowers its report — if a quorum of nodes has a stuck FSM, commit stalls. Re-attach: epoch bump, resume from `last_applied` (ring or journal). |
| FSM *k* permanently slower than the log | bounded: it paces the cluster (writes run at FSM *k*'s rate) — the intended outcome; visible as `lag_waits` on the others and `uc2_service_lag_bytes{service=k}` pinned near the bound. |
| Declared FSM never started | identical to "crashed at t=0": admission closed until it attaches. Alert: `uc2_service_attached == 0 for 30s`. |
| Undeclared id attaches | refused (`ServiceNotDeclared`). |
| Two processes, same id | refused (`service.<id>.lock`). |
| Declared sets differ across nodes | live path: nothing breaks (sets are node-local; the log carries no service id). Snapshot sessions are refused at `SNAP_BEGIN`. Exported as `uc2_services_declared` (bitmask) so `count(count_values("v", uc2_services_declared)) > 1` alerts. Documented as "must match". |
| Node restart (new `instance_id`) | each FSM sees the mismatch at its slot and re-attaches — today's path, per id. |
| Lockstep and one FSM dies | everything stops at the next frame; alert fires; that is the contract lockstep buys. |
| `fsm_lag` too large for the ring | refused at startup (`>= buffer_bytes/2`). |
| Declared set **grew** after purge ran (id *k* added to a cluster whose journal no longer starts at 0 — including a restore of an artifact that lacks `snapshots/<k>/`) | FSM *k* cannot build its state: broadcast SMR means a new FSM replays from genesis, no sibling's snapshot is of any use (different SM), and the prefix is gone. Its attach ends in `replay_into`'s below-floor guard (`SnapshotRequired`, none available) — a named, per-FSM refusal, and the other FSMs are unaffected. Meanwhile `min(applied)` over declared ids holds admission closed (§5.1), so the operator sees the cluster stall by design until the set is restored. **Rule:** a new id can only be added while the journal is intact from 0 (purge disabled or never fired) — documented in `run-a-cluster.md` and §11. |
| Client attached before all FSMs | rings exist (node creates them); requests targeting an unattached FSM simply wait (bounded by the client's own timeout) — same as a restarting single service today. |

## 9. Observability

M10 families gain a `service` label (the page-1 aggregates keep the unlabeled
names, now meaning "slowest FSM"):
`uc2_service_applied_bytes{service}`, `uc2_service_epoch{service}`,
`uc2_service_snapshot_pos_bytes{service}`, `uc2_service_heartbeat_age_seconds{service}`,
`uc2_service_attached{service}`, `uc2_service_lag_bytes{service}` (= `commit − applied`),
`uc2_service_lag_waits_total{service}`, `uc2_services_declared` (bitmask),
`uc2_fsm_lag_bytes` (0 = lockstep). `/readyz` unchanged (`can_serve`).
Two new alert rules, both proven to fire via `scripts/m10_alert_fire.sh`:
*declared FSM absent* and *FSM pinned at the lag bound*. `uc2ctl status`
(one of the twelve M7–M12 subcommands, `uc2ctl/src/main.rs:254-301`) prints
a per-service table (id, attached, epoch, applied, lag, snapshot_pos,
heartbeat age); `uc2ctl verify-backup` reports the per-id coverage (§7.5).
The `[log]` transition records gain `service_attached` / `service_detached`
events.

## 10. Stage-2 door (not built here)

Stage 2 = N independent logs, each with its own FSM set, in one daemon:
`[[logs]]` in `node.toml` → N instance dirs, N UDP ports, N `Node` instances in
one process. Stage 1 keeps that open by: (a) the config parser treating today's
top-level log settings as "the single implicit log"; (b) adding no
process-global singletons for services — all new state hangs off `Node` /
`InstanceDir`; (c) `uc2ctl` already taking `--instance-dir`.

## 11. Out of scope

Per-command routing to a subset of FSMs; cross-FSM transactions; dynamic
add/remove of a service id at runtime; adding an id after purge has run (§8 —
a structural property of broadcast SMR, not a stage-1 gap); unbounded lag;
in-process hosting helpers; a service selector on the remote protocol
(protocol v2, §6.4); stage 2.

## 12. Test plan and acceptance

**Unit.** Page-2 offset/size tests in both `uc_protocol` and `uc2_log`; the
lag-barrier predicate (lockstep/bounded) as a pure function with a table; the
admission door with the FSM term; the Q `(term, ceiling)` pair
(`term_at(ceiling − 1)`, `ceiling ≤ validated_up_to`); client slot-table mask
completion (drop-outside-mask, fan-in ordering); config refusals.

**`uc2_sim`.** New invariant: *a node's report never exceeds its validated
frontier and never decreases except via truncation*. New scenario: FSM
progress modelled as a per-node "apply ceiling"; assert commit stalls iff a
quorum is capped (liveness sanity) while every existing safety invariant holds.

**In-process capstones.** `lin_v2` gains `two_fsm`: two `RegisterSm` FSMs
(ids 0, 1) under failover + purge/snapshot churn, in **both** lockstep and
bounded modes; per-FSM histories checked by the untouched `uc-lincheck`
checker; plus a replication-equivalence oracle — `submit_all` responses must
agree across FSMs (same deterministic SM, same log ⇒ identical responses). A
*slow-FSM* variant (FSM 1 sleeps in `apply`) asserts the sampled bound
`applied_0 − applied_1 ≤ fsm_lag` and that throughput converges to FSM 1's
rate rather than diverging. `lin_partition_v2` runs once with two FSMs.

**Hard-crash (`uc2-crashtest`).** `kill -9` FSM 1 mid-load, restart, assert
linearizable for both FSMs; `kill -9` the node with two FSMs attached.

**M11 backup round trip.** `backup → verify → restore → boot` with two ids and
purge enabled, asserting both `snapshots/<id>/` trees survive and `verify`
reports a `Hole { service }` when one id's newest snapshot is deleted from the
artifact (§7.5).

**Elle.** The clean tier runs once with two FSMs (otherwise unchanged).

**Fuzz / Miri (M12d tier, `docs/VERIFICATION.md`).** The three decoders this
spec changes already have targets: `uc_protocol_cnc` (extend to page-2 slot
reads and the 4032 pair), `uc_protocol_datagram` (already calls
`read_snap_begin_body` — the 35-byte fixed part is covered the moment the
reader changes), `ring_mpsc_record` (the query-ring record; the
`service_id ++ query` split is a new decode step to fuzz), and
`uc2_node_toml` (`[services]` parses under the existing target). Miri stays
where it is (pure decoders). `VERIFICATION.md` rows updated.

**Lean / conformance.** No model change (consensus untouched; Q is below the
model's abstraction — the model already admits any report ≤ durable). Recorded
in the gate doc.

**Fleet gate (`docs/benchmarks/uc2-m14-gate-<date>.md`, driver
`bench-infra/scripts/m14_fleet_gate.py`, user-approved run).**
A real non-noop FSM pair (`counter` + a deliberately ~2× slower variant) on the
5-host fleet. Pre-committed bars: bounded-mode throughput converges to the slow
FSM's rate within a stated tolerance; zero divergence between FSMs' responses;
FSM-kill time-to-recover ≤ 15 s (M9's bar); lockstep cost measured and
*reported*, not barred. Local runs are smoke only (dev box is not a bench).

## 13. Risks and mitigations

| risk | mitigation |
|---|---|
| Q interacts with flow control / leader-side heuristics in a way the design missed | the sim scenario + `lin_partition_v2` with two FSMs; explicit plan check item (§5.3) before code |
| Lockstep barrier cost across processes | measured in isolation the day M14a merged (`uc2_node/examples/apply_bench`, `docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md`): the first implementation slept 50 µs on every wait (18 k frames/s); the shipped `lockstep_wait` ladder never sleeps on a live sibling (631 k / 583 k / 458 k frames/s at N = 2 / 4 / 8 on the dev box, bounded unaffected); the remaining ~1.6 µs/frame is the N-way cross-core handshake and is what the fleet row measures; lockstep is opt-in, bounded is the default |
| Per-service egress rings multiply client polling work | N ≤ 8; the engine already polls two rings; measured in the M5-style smoke |
| cnc flag day surprises operators | named refusals on version mismatch (already in place); the M12 `how-to/upgrade-a-cluster.md` already has the exact same-host procedure ("Ring format change in 2.7.0: restart a host's processes together", written for `ULTRNG2`) — this release adds a cnc-3.0 section in the same shape plus the wire-0.6.0 whole-cluster step; `RELEASES.md` section per the CLAUDE.md release rule; `run-a-cluster.md` gets the declared-set-before-purge rule (§8) |
| M11 backup silently drops per-FSM snapshot dirs if §7.5 is missed in the plan | the backup round-trip test (§12) is written **before** the `snapshots/<id>/` move lands, so it fails red on the flat copier first |
| Declared sets drift across nodes | refused on the snapshot path, alertable on the live path; documented |

## 14. M14c design amendments (2026-08-28) — what execution of M14a/M14b taught, and the M14c cut

M14a (`main` 6111257) and M14b (`main` 4347bc2) are in. Their execution records (at the end of each plan doc) hold the as-built rulings; this section records what M14c changes in *this* design and how the remaining work is cut. Where it contradicts an earlier section, this section wins.

### 14.1 The cut

- **M14c** (this section's scope, one plan): (1) the client hot-path cost M14b measured, fixed first; (2) §7.3 — N artifacts per snapshot session, wire 0.6.0; (3) §9 — observability. In that order: the fix precedes anything that measures, the wire change is the one flag day and gets its own review cycle, the labelled metrics are what the proof plans will read.
- **M14c2** (its own plan, after M14c): the §12 capstones — `lin_v2 two_fsm` (lockstep, bounded, slow-FSM oracle), `lin_partition_v2` with two FSMs, the two hard-crash scenarios, the elle clean tier with two FSMs. They are written against the finished node because the learner-join and purge/snapshot-churn paths they prove are exactly what §7.3 changes.
- **M14d** unchanged: fleet gate + release writeup.

### 14.2 Client hot path (new workstream; not in the original design)

M14b's exact-binary A/B (`hop_bench engine-load` → `dummy-node`, dev box, 17 pairs) put hop 1 at **−4.2 % resp/s vs M14a's tip**, reproducibly, with p90 2 → 3 µs. Skipping `received.fetch_or` for a single-ring request (`expected == bit`; the completing CAS is already the exactly-once gate) restores the tail but not the rate, so the rate loss is the grown hot body — M14a's codegen lesson. M14c: commit the fast path; then bisect M14a-style, one variant per suspect, A/B'd back to back on the same harness: (v1) the fan-in arms of `handle_record`'s `MSG_V2_RESPONSE` branch moved out of line (`#[inline(never)]`), leaving the single-ring `Won` path as the hot body; (v2) `send`'s prefix path out of line; (v3) `poll`'s ring loop shape. Keep what measures; record every number in the plan's execution record. Target: hop 1 inside the box's repeat noise of `main`. No bar — rate bars are fleet-only (M14d).

**As-built errata (M14c, 2026-08-28 — the −4.2 % premise is refuted):** the
M14b measurement this section is built on **did not reproduce**. Rebuilding
the identical two commits and A/B-ing the exact binaries back to back
(`scripts/hop1_ab.sh`, 6 reps, alternated order, fixed sink) read **−0.30 %**,
**+0.31 %** and **−0.05 %** across three configurations — all with overlapping
ranges — and a control that A/B'd **the same commit built twice** manufactured
**+1.02 %**, larger than the effect being hunted. So this box does not resolve
1 % on this harness, and the bisection (v1/v2/v3) was **stopped before it
started**: recording three "refuted" variants would have been a claim the
instrument cannot support. What *is* kept is Task 1's fast path — skipping
`received.fetch_or` for a single-ring request — because it restores the
**tail** exactly as designed (p90 3 → 2 µs) at a rate delta of −0.05 %,
OVERLAP. The new standing rule, and the reason the premise fell: **a
same-source rebuild control before any binary-to-binary perf claim** — build
the same commit twice, A/B those two binaries, and treat anything smaller than
that control's spread as unmeasurable on this box. Full record:
`docs/benchmarks/uc2-m14c-client-hop-2026-08-28.md`; runner:
`scripts/hop1_ab.sh`. A ~1 % client-hop question belongs on the fleet (M14d),
not on the dev box.

**As-built errata (M14c, 2026-08-28 — two deliberate strictnesses in §14.3/§14.4).**
Both are as-built on purpose; neither is a bug to "fix":
1. **The receiver checks `services_declared` on EVERY `SNAP_BEGIN`**, not
   "on the first" as §14.3 says. Every BEGIN carries the mask, and a session
   whose later BEGIN disagrees with its first is exactly the mixed/forged case
   the `declared-set mismatch` refusal exists for — checking once would admit
   the artifacts that arrive after the check.
2. **`service_detached` also fires when the slot's ATTACHED bit clears**, not
   only when the heartbeat ages past the wedged threshold. An orderly service
   stop clears the bit immediately; waiting out the heartbeat threshold to say
   so would delay the event by the stale window and make an orderly stop look
   like a wedge.

### 14.3 §7.3 as designed — one session per join, a stream of N artifacts

What is already per-id (M14a): `snapshots/<id>/` directories, `SnapshotStore::open(dir, id)` (each FSM builds, discovers and installs its *own* artifact; the node signals nothing), `slot[id].snapshot_pos`, and the purge floor `min(snapshot_pos)` over declared ids. What is still single-artifact: the transfer plane — the sender's `SnapSession` (one at a time, `snapshot_source` closure returns FSM 0's file at the node floor), the receiver's `SnapIntake` (one `snap_dir`, one `incoming_snapshot_pos`), and the node-wide floor-adoption cell.

**Design.** A snapshot session stays one `SnapSession`/`SnapIntake`, but is a *stream of artifacts*: for every declared id, ascending, one `SNAP_BEGIN` (naming the id, that id's newest artifact position and length) followed by that artifact's chunks; the sender advances to the next id when the current artifact's last chunk has gone out; **chunk offsets are stream-global** (the session is one concatenated byte stream with artifact boundaries announced by the BEGINs), so `SNAP_NAK` repair is byte-identical to today. The receiver writes each artifact to `snapshots/<id>/incoming-<pos>.part` and renames it on completion, tracks `received: u64` against the BEGIN's `services_declared`, and adopts the floor — `min` over the received positions, into the existing `incoming_snapshot_pos` cell — **only when `received == services_declared`**, so no FSM is ever stranded below an adopted floor. Each FSM then installs its own artifact and tail-replays: today's per-id path, untouched.

**The "declared id with no snapshot" case is moot.** The floor is `min(snapshot_pos)`; an id that has never snapshotted holds it at 0; the sender's source closure returns `None` at floor 0; no session opens and the joiner is served by journal replay from 0. Whenever a session *does* open, every declared id has an artifact at or above the floor (its own `snapshot_pos ≥ min`), and the sender ships each id's newest. (The §8 "declared set grew after purge" row is a different situation and stands.)

**`SNAP_BEGIN` layout (0.6.0).** The 0.5.0 body is `session:u32 [0..4] · pad [4..8] · snapshot_pos:u64 [8..16] · total_len:u64 [16..24] · config_len:u16 [24..26] · config [26..]`. 0.6.0 reuses the pad and inserts one word: `[4] layout:u8 = 1 · [5] service_id:u8 · [6..8] zero · [8..16] snapshot_pos · [16..24] total_len · [24..32] services_declared:u64 · [32..34] config_len · config [34..]` — **`SNAP_BEGIN_FIXED_LEN` 26 → 34** (§7.3 said 35; it appended nine bytes after the fixed part and did not reuse the pad). `services_declared` rides every BEGIN of a session; the receiver checks it on the first.

**Refusals (named, counted, on the receiving node):** `layout == 0` → `peer wire 0.5.0` (a 0.5.0 sender in a mixed cluster); `services_declared ≠ own declared` → `declared-set mismatch` (§8 row unchanged). Both drop the session; the follower keeps NAKing and the operator sees the counter.

**§3.4 correction — the wire has no version gate.** The 16-byte datagram header is `position · leadership_term_id · kind · flags · key_epoch`; `uc_protocol::version::CURRENT` is documentary and has no caller on any receive path (its own doc says so). §3.4's "a 0.5.0 peer refuses a 0.6.0 datagram at the existing version check" is therefore false. The truth: DATA/NAK/AppendPosition/TermMap/admin are byte-identical across 0.5.0 and 0.6.0, so a mixed cluster replicates and elects normally; only a snapshot session between mixed versions goes wrong, and only the **0.6.0** side can detect it (the `layout` byte). A 0.5.0 receiver of a 0.6.0 BEGIN misreads `config_len` and drops or mis-adopts config — the flag day is real and rests on the standing operational rule (upgrade all nodes together, `how-to/upgrade-a-cluster.md`), stated plainly rather than attributed to a check that does not exist. `CURRENT` still becomes 0.6.0 (the record of what the wire means). Adding a real header version field is out of scope (the header is full; it would be its own flag day).

**Acceptance (M14c):** `uc2_net/tests/snapshot_session.rs` gains a two-artifact stream with chunk loss repaired by `SNAP_NAK` and a `layout = 0` / mismatched-set refusal; `uc2_node/tests/learner.rs` gains a fresh learner joining a purged two-FSM leader and both FSMs converging (the first test anywhere that combines two FSMs with a below-floor join); `fuzz/src/seeds.rs` gains a 0.6.0 `SNAP_BEGIN` seed (`uc_protocol_datagram` already decodes it); the datagram unit tests pin 34.

### 14.4 §9 as designed

Labels via the existing `push_labeled` (`service="<id>"`) — the peer-slot band's mechanism, not a new one. The unlabeled aggregate names (`uc2_service_applied_bytes`, `_epoch`, `_snapshot_pos_bytes`, `_heartbeat_age_seconds`) keep their names and now mean "slowest FSM"; each gains a labelled twin per declared id. New families: `uc2_service_attached{service}` (0/1 from slot status), `uc2_service_lag_bytes{service}` (= commit − applied), `uc2_service_lag_waits_total{service}` (the slot's `lag_waits`), `uc2_services_declared` (the bitmask), `uc2_fsm_lag_bytes` (0 = lockstep). All in `CONTRACT_SERIES`, so `every_contract_series_is_present` and the `m10_gate` live scrape cover them. Alerts (`packaging/prometheus/uc2-alerts.yml`): `Uc2ServiceAbsent` — `uc2_service_attached == 0 for 30s`; `Uc2ServicePinnedAtLagBound` — `uc2_service_lag_bytes >= uc2_fsm_lag_bytes for 30s` and `uc2_fsm_lag_bytes > 0` (bounded mode only) — each with an `m10_alerts` scenario so `scripts/m10_alert_fire.sh` proves it fires. `uc2ctl status` prints a per-service table (id, attached, epoch, applied, lag, snapshot_pos, heartbeat age) from the cnc page it already opens (`service_slot(id)`). `service_attached` / `service_detached` `obs_event!`s (with `service`, `epoch`) emitted from the node's per-cycle `publish_service_mins` on an epoch bump and on the heartbeat aging past the wedged threshold. Dashboard rows for the labelled families. Semver: additive.

### 14.5 Out of scope for M14c

The capstones (M14c2), the fleet gate and release writeup (M14d), a datagram header version field, a remote-protocol service selector (§11), and the M14b deferrals that are not on the hot path (listed in the M14b plan's execution record).

## 15. M14d design (2026-08-29) — the fleet gate and the 2.8.0 release

M14a (`main` 6111257), M14b (4347bc2) and M14c (b3f1053) are in. This section
fixes what §12's one paragraph left open — the harness, the topology, the
tolerance, the exact rule behind every bar — and scopes the release. Where it
contradicts §12 or §14.1, this section wins. Bars are committed here, before
any fleet run, per the honest-failure protocol (M7–M13).

### 15.1 The cut, amended

- **M14d** (this section, one plan): `bench-infra/scripts/m14_fleet_gate.py` +
  the `m12_gate` harness extension, the gate doc
  `docs/benchmarks/uc2-m14-gate-<date>.md`, and the `2.8.0` release writeup.
  Tagging and the crates.io publish stay user steps
  (`docs/how-to/cut-a-release.md` §4, §6).
- **M14c2 moves *after* the release** (ruling 2026-08-29, reversing §14.1's
  order): the §12 capstones (`lin_v2 two_fsm`, `lin_partition_v2` with two
  FSMs, the two hard-crash scenarios, the elle two-FSM tier) and the M14c
  plan's "Deferred to M14c2" list land as a proof-only `2.8.1`. `2.8.0`
  therefore ships multi-service with the coverage VERIFICATION §11 states
  today — unit, in-process integration on one node and a 3-node cluster, the
  M14b sim scenario, the fuzz seeds — **and says so** in the gate doc, in
  VERIFICATION §11 and in the release notes. This is a disclosed gap, not a
  claim.
- Out of scope, unchanged from §14.5: a datagram header version field, a
  remote-protocol service selector (§11), the M14a/M14b deferred minors.

### 15.2 Topology

Four `c6id.2xlarge` (8 vCPU each, 32 vCPU), `us-east-1`, one placement
group, NVMe journals, fsync on — the M13 shape. Roles: `hosts[0..3]` three
voters, `hosts[3]` the learner (idle until row f). **The measuring client is
the direct `Engine`, which is shmem-attached and therefore runs on the
leader host** — exactly as the M12 and M13 direct arms did
(`m12_fleet_gate.py:421-437`). A separate client host was drafted on
2026-08-29 and withdrawn the same day (errata: this paragraph); the
account's 48-vCPU quota leaves room for a fifth host if a remote-path row
is ever added, but no row in §15.4 needs one. Rows a–e use exactly M13's
voter shape, so row a's N=1 number is measured on the same host shape as
M13's full-stack direct arm; the client binary and its windowing changed
since, so the M13 number is context, not a bar (errata 2026-08-29, per
CLAUDE.md's same-source-rebuild lesson).

### 15.3 Harness: `m12_gate` extended, driven by `m14_fleet_gate.py`

**No new binary.** `uc2_gateway/examples/m12_gate.rs` already has the `node`
/ `service` / `edge` / direct-client roles the M12 and M13 fleet drivers
launch as `systemd-run` transient units. M14d adds:

- `node --services <mask> --fsm-lag lockstep|bounded:<bytes>` → the
  `ServicesConfig` the node boots with (today hard-coded to
  `ServicesConfig::default()` at `m12_gate.rs:428`). Absent flags keep the
  default (`{0}`, `Bounded(buffer_bytes / 4)`), so every existing arm is
  byte-for-byte unchanged.
- `node --purge` → `PurgePolicy::BelowSnapshot { slack_bytes: 0 }` (as `m6_gate`'s node
  role), needed by row f only; and the `node` role's stats line gains
  `Node::snapshot_session_refusals()` beside `reports_unattested`, so row f's
  refusal counters come out of a gate node without a `[metrics]` endpoint.
- `service --service-id <id> --work-spin <K>` → `ServiceConfig::service_id`,
  and a `SpinCountSm`: `CountSm`'s typed twin whose `apply` runs a
  fixed-iteration integer loop of `K` rounds before returning the count. The
  loop's result is consumed through `std::hint::black_box` and **never
  reaches the response**, so the response is the count and only the count;
  `K` changes cost, not output. The typed tier is deliberate (it is the
  quickstart's tier and the one the M12 gate rated); `--raw-sm` stays
  available and is not part of any M14 row. Unit test: for any `K`, the
  response stream of `SpinCountSm` equals `CountSm`'s on the same input.
- The direct-client role submits with `submit_all` (waits for every declared
  FSM's response) when the declared mask has more than one bit, and after
  each arm runs the divergence check of row c through
  `query_linearizable_on(id)` per declared id, against every voter.

`m14_fleet_gate.py` reuses `m6_fleet_gate.build_fleet_hosts` (count=4 —
errata 2026-08-29, following §15.2's withdrawal of the fifth host),
`m12_fleet_gate`'s `systemd-run` bring-up, sync and teardown, and M13's
`--selftest` pattern: every verdict function is pure over recorded numbers
and the selftest replays a canned row set through them, locally, with no
fleet. Bars are module-level constants, printed beside each verdict as
`GATE-JSON` lines; the exit code is the verdict.

**The slow FSM, made precise.** §12 says "a deliberately ~2× slower
variant". `counter`'s apply costs tens of nanoseconds and is nowhere near the
limiter (M13 measured the fleet chain cluster-bound at ~1.75 M ops/s), so
"2× slower than `counter`" would change nothing. The intended meaning, fixed
here: **the slow FSM's solo apply rate is ≈ half the N=1 cluster rate**, so
that it — not consensus, not the client — is the bottleneck in row b. `K` is
calibrated by a preliminary arm (`calib`: FSM 0 alone running `SpinCountSm`
at a ladder of `K`, 12 s arms with an 8 s window, like every rate arm; pick
the `K` whose rate is nearest 0.5× row a's N=1 rate) and recorded in the gate
doc. Errata 2026-08-29: the picked rung must land inside [0.35, 0.65] × N=1,
or the run FAILS at calibration — a ladder that never made the FSM the
limiter would let row b pass vacuously. Row b's bar is a ratio against
slow-solo measured *with that same `K` in the same run*, so the bar does not
depend on the calibration landing exactly.

**What "rate" means, everywhere below**: client-observed completed
operations per second over the arm's steady window (the middle 8 s of a 12 s
arm; the leading 2 s and trailing 2 s discarded — M9's window rule), envelope
on, one direct-client process on the leader host at `m12_gate`'s direct-client
defaults (`--inflight 4096`, 64-byte payload — the M13 sizing). Positions and bytes are the node's view;
ops are the client's; the gate rates ops.

### 15.4 The rows and their bars

| row | arm(s) | rule | bar |
|---|---|---|---|
| **a** — equal-speed pair | `n1`: `{0}` counter. `n2eq`: `{0,1}` both counter, bounded default | `rate(n2eq) / rate(n1)` | **≥ 0.90** |
| **b** — bounded convergence | `slow1`: `{0}` `SpinCountSm(K)`. `pair`: `{0,1}` = counter + `SpinCountSm(K)`, bounded default | `rate(pair) / rate(slow1)` | **within [0.90, 1.10]** (ruling 2026-08-29: ±10 %) |
| **c** — zero divergence | after **every** arm above and in d–f | on every voter (and the learner in f), for every declared id: `query_linearizable_on(id)` → count; all counts equal each other **and** the client's completed-op count; per-FSM `applied` bytes on `uc2ctl status` equal across ids on each host | **any mismatch = FAIL** |
| **d** — FSM kill | `pair` under load; at t0 `SIGKILL` FSM 1's unit on the **leader** host; `systemd-run` it again immediately (procedure re-specified 2026-08-29 after run 1: both FSMs run with `--snapshot-interval-bytes 33554432` **and the arm runs `PurgePolicy::BelowSnapshot`** — without purge the restart installs nothing and replays the whole journal — and the measuring client submits to **FSM 0 only** instead of fan-in) | M9's recovery rule (`m9_fleet_gate.py:343-379`): recovered = first 2 s window at ≥ 80 % of the pre-kill 8 s baseline whose end is within 15 s of t0, confirmed by the next window; **and** `uc2ctl status` on that host shows `service 1` attached with `lag ≤ bound` by the same deadline | **≤ 15 s** (M9's bar) |
| **e** — lockstep cost | `n2eq-ls` and `pair-ls`: rows a/b's pairs with `--fsm-lag lockstep` | `rate(n2eq-ls) / rate(n2eq)` and `rate(pair-ls) / rate(pair)` | **reported, not barred** (§12) |
| **f** — two-FSM learner join, wire 0.6.0 | `pair` with `PurgePolicy::BelowSnapshot` on the voters, under load; `uc2ctl add-learner` a learner on `hosts[3]` declared `{0,1}` | the learner's snapshot session carries **two** artifacts (`layout = SNAP_BEGIN_LAYOUT_V2 = 1`, `services_declared = 0b11`): after the join the learner holds a complete artifact under both `snapshots/0/` and `snapshots/1/`, and `uc2ctl status` on it shows `snapshot_pos > 0` for both ids; the learner reaches both voters' `applied` within M6's `JOIN_BUDGET = 60 s`; row c's check passes on the learner; the receiving node's `Node::snapshot_session_refusals()` pair — printed by the `node` role beside `reports_unattested` — reads **(0, 0)** (`peer wire 0.5.0`, `declared-set mismatch`) on every node | **converges inside 60 s with zero refusals**, and ≥ 1 snapshot install observed on the learner (`snapshot_installed` in its node log); the join time is reported |
| **g** — correctness tiers | CI at the gated commit | `ci.yml` green (workspace tests, clippy, deny, fuzz smoke); the most recent `nightly.yml` at or after the gated commit green (capstones, sim-heavy, crashtest, loom, miri) | **green**, and the doc states the M14c2 deferral in §15.1's words |

**Erratum, 2026-08-29 (after fleet run 1).** Row d's *procedure* — not its
bar — was re-specified after run 1 FAILed: the arm now runs a 32 MiB snapshot
policy on both FSMs **together with purge** (a `SnapshotPolicy` shortens a
restart only with purge — reconstruction installs the newest artifact only when
the journal no longer covers the start position, `uc2_service/src/replay.rs:73-78`),
and the measuring client submits to FSM 0 only, because run 1's snapshot-less
restart made the attach clause a full-journal-replay clock and its fan-in
client's 30 s `request_timeout` pinned the rate clause at 0 regardless of FSM
1's recovery (`docs/benchmarks/uc2-m14-gate-2026-08-29.md`,
"Re-specification — applied 2026-08-29 (run 2)"; run 1's FAIL stays recorded).

Why these and not others: the remote path is FSM-0-only in 2.8.0
(`docs/reference/limits.md`) and unchanged by M14 at the wire, so M13's rows
a–d stand and are **not re-run**; the per-FSM metric families and both M14c
alerts are proven to fire by `scripts/m10_alert_fire.sh` in CI and are not a
fleet question; the `[services]` named refusals are `daemon_refusals` tests.
A fleet gate is for what only a fleet can measure — rates across real
hosts, a kill on a real host, a snapshot stream over a real network.

Reading a FAIL: row a or b below the bar is diagnosed before any re-run
(harness defect vs. product property — both recorded, the bar kept); row c
is a **consensus or apply defect** and blocks the release outright; row f's
refusal counters non-zero is a wire-0.6.0 defect and blocks likewise.

### 15.5 Facts the gate doc must state

The `K` chosen and the calibration ladder; every arm's rate with its window;
the leader's identity per arm; the row-c counts per host per id; row d's
timeline (M9's `INFO recovery timeline` format) and the observed
`service_detached` → `service_attached` log lines on the leader host; row f's per-id artifact
lengths on the learner, join time and both refusal counters; the CI and nightly
run ids for row g; the commit gated; and the M14c2 deferral, verbatim.

### 15.6 The 2.8.0 release

Per `docs/how-to/cut-a-release.md` §1 and CLAUDE.md "Release documentation",
**before the tag**, in this order:

1. **Version**: `Cargo.toml` `2.7.0 → 2.8.0` (+ intra-workspace pins);
   literal-string sweep (`README.md` lines 33/37/38/89, `packaging/compose.yml`,
   `Dockerfile` comments, `QUICKSTART.md`, `run-a-cluster.md`);
   `SECURITY.md` supported line → `2.8.x`. Wire `0.6.0` and cnc `3.0` are
   already in the code (126836d, f58f3c2) — nothing to bump there.
2. **Writeup**: a `2.8.0` section atop `RELEASES.md` — feature bullets
   (multi-service `[services]`; per-FSM routing + client fan-in; the wire
   0.6.0 snapshot stream; per-FSM observability + the two alerts; `uc2ctl
   status` per-FSM table; per-FSM backup), a **Fixed** bullet (the M14c
   `SNAP_NAK` slot pinning, a405e71; the apply-loop lockstep sleep, 80a37a8),
   a **Performance** bullet (the M14 gate, the two hop docs), the **Upgrade
   consequence** paragraph (0.6.0 flag day + cnc 3.0 same-host restart;
   `upgrade-a-cluster.md` §"Wire change in 2.8.0" already written). The
   matching `docs/releases.md` entry. **A new explainer**
   `docs/notes/uc2-m14-multi-service-explained.md` (one log → N FSMs, the
   lag barrier and the quorum-gated report ceiling in plain language, why
   lockstep costs what it costs) — CLAUDE.md requires every feature bullet
   to link a detailed doc and none exists for the mechanism.
3. **Sweep** for statements 2.8.0 invalidates: `docs/reference/limits.md`'s
   "unreleased" qualifiers; `upgrade-a-cluster.md`'s dating; CLAUDE.md's
   project-status block (version, the M14 table row, wire `0.6.0`, the cnc
   page is 8 KiB in two places, "Next up"); `docs/VERIFICATION.md`'s header
   ("current as of M12d") and §11 (the M14c2 deferral, stated); the M14b
   plan's three "rustdoc-only behaviours to carry into the release writeup".
4. **Security posture refresh** (the 2026-08-29 review): `attack-surface.md`
   (cnc row 4 KiB → 8 KiB + page-2 band; a row for
   `uc_protocol::v2::ipc::split_query_payload`, local-only, fuzzed via
   `ring_mpsc_record`; the `SNAP_BEGIN` row's per-id on-disk consequence);
   `threat-model.md` §5 (one stalled FSM on a quorum of hosts is a
   cluster-scope liveness lever; `service.<id>.lock` is a same-uid squat
   point); `self-assessment.md` (F7 = a405e71; §4 gains the multi-artifact
   intake state machine; §5's tier table carries the M14c2 caveat; a
   "revised for 2.8.0" line, the M12d dating kept as history).
5. **Nightly**: the 2026-08-28 scheduled `nightly.yml` run failed; its cause
   is diagnosed and recorded (fixed, or named as a known flake with the
   memory's evidence) before the rc tag.
6. **Tag path**: `v2.8.0-rc.1` → `cosign verify-blob` as a stranger
   (cut-a-release §3, §5) → `v2.8.0`. Both user steps; the plan ends with the
   release-smoke evidence and the two commands.

### 15.7 Acceptance

M14d is done when: the harness extension's unit tests are green (spin-SM
determinism; `--services`/`--fsm-lag` parse and refuse like `node.toml`);
`m14_fleet_gate.py --selftest` passes; the gate doc with **all bars committed
first** is on `main`; the fleet run is recorded in that doc with every row's
verdict and §15.5's facts, FAILs diagnosed; and the 15.6 writeup is on
`main` with the rc tag's verification evidence — the `v2.8.0` tag itself
being the user's step.
