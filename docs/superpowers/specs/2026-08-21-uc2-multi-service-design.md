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
| Lockstep barrier cost across processes | measured on the fleet; lockstep is opt-in, bounded is the default |
| Per-service egress rings multiply client polling work | N ≤ 8; the engine already polls two rings; measured in the M5-style smoke |
| cnc flag day surprises operators | named refusals on version mismatch (already in place); the M12 `how-to/upgrade-a-cluster.md` already has the exact same-host procedure ("Ring format change in 2.7.0: restart a host's processes together", written for `ULTRNG2`) — this release adds a cnc-3.0 section in the same shape plus the wire-0.6.0 whole-cluster step; `RELEASES.md` section per the CLAUDE.md release rule; `run-a-cluster.md` gets the declared-set-before-purge rule (§8) |
| M11 backup silently drops per-FSM snapshot dirs if §7.5 is missed in the plan | the backup round-trip test (§12) is written **before** the `snapshots/<id>/` move lands, so it fails red on the flat copier first |
| Declared sets drift across nodes | refused on the snapshot path, alertable on the live path; documented |
