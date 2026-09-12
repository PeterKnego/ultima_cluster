# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

`ultima_cluster` (UC) is a **Rust-native State Machine Replication
application server**. This is **UC v2**; the v1 stack (an
`openraft`-based design) is retired and its crates deleted — v2 owns
consensus, elections, and transport directly. Do not reintroduce `openraft`
or `quinn`/QUIC. The crate names `uc_node`/`uc_service`/`uc_client` were
v1's and were banned for that reason; **that ban is lifted** — the v2 crates
took those names in the `uc2_*` → `uc_*` rename (see `RELEASES.md`), so a
pre-rename commit or doc naming them means the deleted v1 crate, not this
code.

**Current version: `2.11.0`** — tagged 2026-09-08 at `ff0f5b6`: FSM identity,
log time and timers, the replicated schedule table, the cluster FSM, and
coordinated snapshot instants, one flag day (wire `0.6.0` → `0.7.0`, cnc
`3.0` → `3.1`). All 13 crates published to crates.io the same
day (`cut-a-release.md` §6, `uc_service` now before `uc_node`), so `2.11.0`
is the newest crates.io version. Both fleet gates ran 2026-09-07/08 and every row is
recorded in its gate doc with no bar moved: three PASS, two honest FAIL (timer
precision — bar since restated; the coordinated-snapshot arm's introduction
cost, since absorbed), four inconclusive (rate bars an order of magnitude below
the rig's variance — the driver now judges paired deltas). The known issue
shipped as recorded — `CncPage::meta()` can panic on a page a restarting node
rewrites — and is **fixed for `2.12.0`** (`meta()` replaced by a
fallible `try_meta()`, plus the boot-gap attach refusal found reviewing it;
`docs/BACKLOG.md` § Shipped). `2.10.0` was one log stream, `UC2_*` env
overrides, `uc_obs`, the `ultima_db` removal and the Broadcast-ring
memory-ordering fix; `2.9.0` the `uc_*` crate rename.

**Pending: `2.12.0` — NOT tagged, NOT published, neither gate run.** Two
features on one flag day, wire `0.7.0` → `0.8.0` and cnc `3.1` → `3.2`:
**jumbo-frame discovery** (the command payload ceiling is measured from the
paths between nodes and committed cluster-wide, monotone — `RUNGS = [1408,
8832, 8960]`, `PROBE`/`PROBE_ACK` kinds 24/25, the live ceiling word at cnc
offset 3984, `Settings` v2's `datagram_mtu`, `max_payload` retired from
`node.toml` and refused by name, the optional `force_jumbo_frames` gate, a
below-the-committed-rung join refusal, and do-not-fragment on the replication
socket — which makes a node **Linux/Android-only**, refused by name
elsewhere) and the **monotonic log clock** (`uc_node::log_clock`; a backward
wall step is smeared at 500 ppm instead of freezing the log's clock, and the
consensus pass takes one clock read instead of two — no flag-day surface of
its own). Both fleet gates are pre-committed and **UNRUN**
(`docs/benchmarks/uc2-jumbo-frame-discovery-gate-2026-09-12.md`,
`uc2-log-clock-gate-2026-09-08.md`) — quote no number from either. The
workspace version is still `2.11.0`: the bump, the tag and the publish all
happen in `docs/how-to/cut-a-release.md`, not in the feature work. The
release writeup is in `RELEASES.md` and `docs/releases.md`; the explainer is
`docs/notes/uc2-jumbo-frame-discovery-explained.md`, the how-to
`docs/how-to/jumbo-frames.md`, and the spec
`docs/superpowers/specs/2026-09-10-uc2-jumbo-frame-discovery-design.md` (read
its two "Errata … as built" sections before the body — seven errata, and the
three an operator is most likely to misread are the forever-climbing
`uc2_probe_sent_total` on a narrow cluster, the solo cluster that never
raises, and the join gate that never refuses a silent peer — it holds until a
quorum of voters has proven the rung, with no timer).
**M14c2 is the last feature milestone; milestones M1–M14 are all complete**, each
closed by a fleet-proven gate doc under `docs/benchmarks/` (bars are
pre-committed before any run; a miss is recorded as FAIL and keeps the bar —
the honest-failure protocol). M14c2 is **proof-only**: no new feature, no wire
or cnc change — its record is the two-FSM capstones (`docs/VERIFICATION.md`
§11) and the lockstep envelope bench doc; the pinned-fleet-rig validation run
is the one open item. The per-milestone history that used to
live in this section is in `RELEASES.md` (user-facing), `docs/releases.md` (the
engineering record), and the gate docs; this section keeps only the map and
the standing facts that bind new work.

| milestone | release | what it shipped | gate doc (`docs/benchmarks/`) |
|---|---|---|---|
| M1–M6 | v2.0.0 | the v2 core: log+archive, replication, commit pipeline, elections, end-to-end SDK, snapshots/learners/purge | `uc2-m{4,5,6}-gate-*` |
| M7 | v2.1.0 | live single-server reconfiguration (promote/demote/add/remove via `uc2ctl`, one at a time, under load) | `uc2-m7-gate-2026-07-13` |
| M8 | v2.3.0 rollup | opt-in node↔node wire crypto (Noise IK + AES-256-GCM, flag-day) | `uc2-m8-gate-2026-07-29` |
| wire 0.5.0 | v2.3.0 rollup | content-attested durable reports — a consensus safety fix (commit ranking becomes a CONTENT quorum) | `docs/notes/uc2-term-map-window-loss-explained.md` |
| M9 | v2.3.0 | deployable node: `uc2-node` daemon, TOML config, named startup refusals, systemd | `uc2-m9-gate-2026-08-19` |
| M10 | v2.4.0 | observable cluster: `/metrics` `/healthz` `/readyz`, alert rules, dashboard | `uc2-m10-gate-2026-08-20` |
| M11 | v2.5.0 | survivable cluster: offline backup/verify/restore, quorum-loss recovery, ENOSPC fail-stop | `uc2-m11-gate-2026-08-20` |
| M12a–d | v2.6.0 | adoptable cluster: gateway kit + remote client, admin authn/audit, packaging/publishing, security posture + fuzz tier | `uc2-m12-gate-2026-08-22` |
| M13 | v2.7.0 | remote path at the cluster's speed: per-record MPSC ring (no publish convoy), Engine-shaped remote client, edge grant budget | `uc2-m13-gate-2026-08-24` |
| M14 | v2.8.0 | multi-service: one log → N FSMs (bounded/lockstep lag, per-FSM routing + fan-in, 0.6.0 snapshot stream, per-FSM observability) | `uc2-m14-gate-2026-08-29` |
| M14c2 | v2.8.1 | two-FSM proof pass; lockstep envelope; fleet pinning | proof-only, no fleet gate — `docs/VERIFICATION.md` §11 (`uc2-m14c2-lockstep-oversubscription-2026-08-30` is dev-box smoke, not a gate doc) |

(Tag state: `v2.2.0` was never tagged — M8 and wire 0.5.0 rolled into
`v2.3.0`; `v2.6.0` shipped as `v2.6.0-rc.1` only and is superseded by
`v2.7.0`, with no final `v2.6.0` tag. The ordered crates.io publish ran for
the first time on 2026-08-30 with `2.9.0` — all 12 crates went live under
their `uc_*` names, and `2.10.0` published **13** (`uc_obs` joined) on
2026-08-31; `docs/how-to/cut-a-release.md` §6 is the procedure. Its
rate-limit note is now measured on both runs: crates.io limits **new crate
names** hard and new *versions* barely at all, so `2.9.0`'s twelve new
names took 62 minutes and `2.10.0`'s one took 59 seconds.)

Next up, now that `2.11.0` is tagged and published: (1) ~~bound the three
unbounded waits in `examples/uc_crashtest/tests/remote_lin.rs`~~ — DONE
2026-09-12 (`common::join_within`, a 30 s `Reap::drop`; the 58-minute hang of
2026-09-08 is still unexplained, it just fails with a name now); (2) **run the
two `2.12.0` fleet gates and cut the release** — the
jumbo gate's six rows (`bench-infra/scripts/jumbo_gate.py`; rows a/b/d need a
fleet whose interface MTU ansible can force between 9001 and 1500, row c is
the soak that decides whether the runbook *recommends* jumbo, rows e/f carry
no bar) and the log clock's A/B, then `docs/how-to/cut-a-release.md`; the
feature code, the docs and the writeup are all written; (3) a fleet re-run of
the time-and-timers rows a/b/e under the paired statistic, and of row c under
its restated bar. The cluster FSM / coordinated snapshots spec is
`docs/superpowers/specs/2026-09-05-uc2-cluster-fsm-and-coordinated-snapshot-design.md`
(read its two "Errata … as built" sections before the body); its three plans
(`docs/superpowers/plans/2026-09-06-uc2-cluster-fsm-plan1.md`,
`…-coordinated-snapshot-plan2.md`, and the plan-3 retirement-and-proof pass
pinned by `uc_node/tests/retired.rs`) all shipped in `2.11.0`; explainer
`docs/notes/uc2-cluster-fsm-explained.md`.

Already on `main` and in the same flag day: **FSM identity**
(`docs/BACKLOG.md` § Shipped, taken up 2026-09-01;
spec `docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md`, plan
`docs/superpowers/plans/2026-09-02-uc2-fsm-identity.md`) —
**shipped in `2.11.0`.** All ten
plan tasks (T0–T10) are done: identity in code (`const NAME` + `const
VERSION`); the row keeps its cluster-wide meaning but a service finds it
by name; `SNAP_BEGIN` 0.7.0 carries hashes + versions per row and refuses
by name; cnc 3.1; `ApplyCtx` replaces the bare `position` apply parameter;
`IdGen` for deterministic IDs; disk, rings and the client engine untouched
(the placement-independent variant was cut, spec §2.1); explainer
`docs/notes/uc2-fsm-identity-and-deterministic-ids-explained.md`; gate doc
`docs/benchmarks/uc2-fsm-identity-gate-2026-09-02.md` (ran 2026-09-07:
rows b and j PASS, e reported, a not adjudicable — the base tree straddles the
same bar on that rig). Shipped in `2.11.0`; still-open residuals are in
`docs/BACKLOG.md`.

Also on `main` and in the same `2.11.0` flag day: **time and timers**
(spec `docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md`),
**all three plans IMPLEMENTED** — plan 1
(`docs/superpowers/plans/2026-09-03-uc2-time-and-timers-plan1.md`, T0–T14),
leader-stamped log time and a deterministic scheduler; plan 2
(`docs/superpowers/plans/2026-09-03-uc2-time-and-timers-plan2.md`, T0–T8), the
**replicated schedule table** (spec §5): the since-retired `FRAME_TYPE_SCHEDULE_TABLE = 6`,
`uc2ctl schedule apply/show`, adoption through the archive walk, and table
ticks that fire through the same heap and the same `TIMER` frame; and plan 3
(`docs/superpowers/plans/2026-09-03-uc2-schedule-table-in-snapshot.md`, T0–T5,
spec §5 errata), which puts that table on the **snapshot session**
(`SNAP_TABLE`, datagram kind 21) so a below-floor joiner installs it before it
can serve or lead. **Plan 3 is superseded by the cluster FSM** — `SNAP_TABLE`
is retired before shipping and the table rides the cluster artifact instead —
so read plan 3 as the reasoning that motivated the cluster FSM, not as what
ships. Requested by
the maintainer 2026-09-02, not a ranked backlog item; shipped in `2.11.0`.
Explainer `docs/notes/uc2-log-time-and-timers-explained.md` (plans 2 and 3
are its "The schedule table" section); gate doc
`docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md` (ran 2026-09-07/08:
g PASS; c and f honest FAIL; a/b/e inconclusive against a bar an order of
magnitude below the rig's variance; d verdict-bearing only after its baseline
was given a `pace_stalls` guard — within resolution at N=1; h's all-nodes arm
reported at ~164 ms freeze / 0.0 s commit gap, its standby arm unresolved. Four
bar rulings followed and are implemented; no bar was moved. Row d is an
isolated `apply_bench` A/B because, unlike identity, this work touches two hot
loops). The other ranked directions, each with the
doc that first recorded it, are in `docs/BACKLOG.md` (update its line when
an item is taken up or dropped). `2.10.0` shipped 2026-08-31 (tag
`v2.10.0`, all 13 crates published; the release-evidence table is at the
top of `docs/releases.md`). It left two items open, neither a blocker:
`nightly.yml` has never run on the tag commit, and `uc2-gateway` shipped
without a `--version` flag while `uc2-node` and `uc2ctl` have one — the
second is **fixed on `main`** (clap emits `--version` only under
`#[command(version)]`; a unit test now guards it) and lands in the next
release. M14c2 is done — the two-FSM capstones
(`lin_v2 two_fsm*`, `lin_partition_v2`, the two hard-crash scenarios, the
Elle `quiet_two_fsm` pass), the lockstep verdict (an operating-envelope
fact, not a defect) and the `--pin` fleet rig shipped as `2.8.1`. The rig's
validation run RAN 2026-08-31 and **pinning was not adopted** — pinned
spread 14.3 % against a pre-committed < 5 % bar, and it costs 9.4 % of mean
throughput; it does remove the worst mode (47.7 % → 14.3 %), so placement is
one cause among others. `--pin` stays opt-in
(`docs/benchmarks/uc2-m14c2-fleet-pinning-2026-08-30.md`). Row e is still
un-re-measured. A follow-on core-count sweep (2026-08-31,
`docs/benchmarks/uc2-node-core-count-sweep-2026-08-31.md`) answers "how many
cores does a node need" — **4, one per polling agent, flat past 5** on the
DIRECT shmem path — (`docs/benchmarks/uc2-regime-probe-2026-08-31.md`). A "two stable regimes"
reading of that sweep was **refuted the same day** by a 16-arm per-second
timeline probe: it is one broad distribution with a long low tail, no arm
transitions, and **pinning tightens p50 spread 31x** rather than removing a
second state. Two standing lessons: fix a spread bar's rep count from observed
arm-to-arm variance (n=4 cannot tell width from tail), and check whether a
driver passes `m12_gate`'s `--warmup-secs`/`--measure-secs` steady window
before comparing its rates with another's. **`m14_fleet_gate.py` does**
(`WARMUP_SECS, MEASURE_SECS = 2, 8`; only rows d and f opt out, deliberately —
the per-completion `done_ns` Vec would reach hundreds of MB and its doubling
memcpy could land inside row d's 2 s recovery window), so the M14 gate's rows
a/b/e are steady-window numbers. **Every other driver does not**, including
`m14_core_sweep.py`, so the 2026-08-31 core-count sweep and regime-probe rates
include a 3-5 % warmup climb — harmless for the shape/ratio conclusions those
docs draw, but not comparable head-to-head with a gate row. First
candidate for the next minor: the twelve-factor hygiene items postponed
out of M14c2 — env-var overrides for deploy-varying config keys (#3) and
one log stream (#11); the release-ledger line (#5) is process, not code
(`docs/notes/uc2-twelve-factor-assessment.md`).

### Standing facts that bind new work

- **The wire protocol SHIPPED is 0.7.0** (`2.11.0`, cnc `3.1`; next bullet);
  **`0.8.0` + cnc `3.2` are implemented and UNRELEASED** in the pending
  `2.12.0` (two pairwise datagram kinds, 24/25, and one cnc word at 3984 — no
  layout change either side, so a `0.7.0` peer drops the probes and such a
  cluster simply never raises its ceiling).
  Before 0.7.0, `0.6.0` changed `SNAP_BEGIN` only; a `0.5.0` sender's session is refused by name,
  so a mixed cluster stalls a joiner rather than installing half a set); the
  node↔node wire and the `cnc.dat` page layout are **flag days, never
  mixed-version** — a 0.4.0 peer's durable report reads as unattested and is
  not counted, so a mixed cluster stalls commits rather than making unsound
  ones; upgrade all nodes together. The client↔gateway remote protocol is
  separate and stays v1. What is API vs. what is flag-day:
  `docs/reference/semver-policy.md`.
- **`2.11.0` (tagged 2026-09-08, `ff0f5b6`) is five features on one flag
  day** (FSM identity, log time and timers plan 1, the replicated schedule
  table, the cluster FSM, and coordinated snapshot instants): wire `0.6.0` →
  `0.7.0` and cnc `3.0` → `3.1`. The sub-bullets below are what each shipped.
  - **FSM identity.** `SNAP_BEGIN` carries per-row identity hashes +
    versions, compared positionally, refused by name (replaces the
    `services_declared` bitmask); cnc slot line 7 = row name + hash,
    node-written at boot, and status line word 1 = the attached service's
    packed version; `[services] names` (not `ids`) is **required** in
    `node.toml` — the same explicit-choice posture `[crypto]`/`[admin]`
    have had since 2.6.0, and `ids` is refused by name pointing at
    `names`; identity lives **in code**, a required `const NAME` (+
    optional `const VERSION`) on the state-machine trait, not in
    deployment config; the apply signature is now `apply(&mut self, ctx:
    &mut ApplyCtx, cmd, out)`.
  - **Log time and timers (plan 1).** The 32-byte frame header is
    **relaid**: `client_id: u32` @12, `seq: u32` @16, `time_ns: u64` @24
    (the two `u64` id fields were only ever half-filled, which paid for the
    stamp; header size and the payload ceiling are unchanged). The leader
    reads its clock **once per pass** and stamps every frame
    `max(now, last)` inside `uc_log::Appender`, so the log's time never
    goes backwards; `ctx.time_ns` is the FSM's deterministic "now" and
    `query` gets none. Since the (unreleased) `2.12.0` that read is
    `CLOCK_MONOTONIC` plus a sampled epoch offset (`uc_node::log_clock`): a
    backward wall step is smeared at 500 ppm, never frozen — spec
    `docs/superpowers/specs/2026-09-08-uc2-monotonic-log-clock-design.md`.
    `FRAME_TYPE_TIMER = 5` (24-byte body
    `identity_hash ‖ timer_id ‖ deadline_ns`) is the **first per-FSM frame
    in a broadcast log**: delivered only to the FSM whose hash it names,
    skipped by every other apply loop but still counted as a yielded frame
    for lag/lockstep. Due timers are fired **before** the pass's client
    frames, stamped with the deadline, bounded by `TIMERS_PER_PASS = 64`
    (at the bound the pass appends no client frames at all). Node layer is
    **at-least-once** (in-flight instances re-armed on `BecomeFollower` and
    `halt`); `uc_service::Timed<S>` makes delivery exactly-once from the
    log-derived pending set. New SDK surface, all additive:
    `ApplyCtx::{time_ns, term, schedule, cancel, timers}`, a **provided**
    `on_timer(&mut self, ctx, ev)` on both tiers, `TimerEvent::late(ctx)`.
    Two more cnc words in the same page version: `log_time_ns` at page 1
    offset `4048` (archive-agent-written, never lowered — a new leader seeds
    its clamp from it; the node seeds the word itself at boot from
    `Archive::recovered_log_time_ns()`, a journal walk at `open`, so the
    clamp survives a restart and only a fresh instance dir starts from wall
    time) and per-row `timers_pending` at slot line 7 `+488`
    (consensus-agent-written each pass). One new per-row IPC ring,
    `svc_sched.<row>.ring` (SPSC, service → node, 1 MiB, `MSG_V2_SCHED`),
    the first per-row ring the **node consumes**; it takes the per-row
    reservation from 5 to 6 MiB. Only `timer_late` and (retired) `timers_rearmed`
    were logged
    (no per-fire record: `uc_obs` has no Debug level and a `stderr`
    write per timer on the consensus agent was rejected).
  - **The replicated schedule table (plan 2, since retired — see below).**
    The retired `FRAME_TYPE_SCHEDULE_TABLE = 6` carried the whole table: an 8-byte header
    plus `count × 33` bytes, `MAX_SCHEDULE_ENTRIES = 32` → 1064 B, inside the
    1312 B crypto-on ceiling. Three rules — `every {period_ns, anchor_ns}`,
    `at {secs_of_day}` (daily, UTC) and `once {at_ns}`, which **parks** after
    firing (stays in the table as delivered, so re-applying the same file does
    not re-fire it; changing its time or id does). `uc2ctl schedule apply
    <file.toml>` encodes, stages `<instance_dir>/schedules.pending` (0600,
    fsync, rename) and signs the first 80 bits of its SHA-256 in admin op 6's
    `id ‖ ip ‖ port` fields — so a 64-byte admin line authenticates a
    1064-byte payload. **Leader-only** (the staged file is node-local: a
    follower answers retry, never forwards) and **single-in-flight** (the
    leader answers retry while the previous table frame is above commit, which
    is what made one level of the (now retired) `ScheduleRecord.prev` enough). Refusals
    `40 schedule_digest` / `41 schedule_missing` / `42 schedule_decode` /
    `43 schedule_unknown_fsm`; audited as `schedule_apply`. Ticks fire as
    `TIMER` frames with `FLAG_TIMER_TABLE`; a truncated table tick is **not**
    re-armed; and a due entry fires at the **latest** occurrence at or before
    the leader's clock (`RowTimers::table_fire_deadline`) — one catch-up tick
    after downtime, never a backlog. `Timed<S>` dedups on `table_last`. Metrics
    `uc2_schedule_table_position` / `uc2_schedule_entries` /
    `uc2_schedule_apply_refused_total`, alert `Uc2ScheduleTableDiverged`.
    **Three parts of plan 2 and ALL of plan 3 were superseded before shipping,
    in the same flag day** — read the cluster-FSM sub-bullet below
    for what actually ships. (a) `FRAME_TYPE_SCHEDULE_TABLE = 6` is retired and
    reserved; the table is a `CLUSTER kind = 2` payload. (b) The plan-2
    adoption path — leader-at-append / followers-from-the-archive-walk,
    persisted in `state/schedules.state`, reverted on truncation to
    `ScheduleRecord.prev` (retired along with the rest of this record),
    and the follower's `TableConsumed` advance — is gone: **every**
    node applies the table at COMMIT in the cluster FSM, there is no durable
    record and nothing to revert, and only the leader holds a heap to advance.
    Single-in-flight is still one command, but now spans all three `CLUSTER`
    kinds rather than tables alone. (c) Plan 3's `SNAP_TABLE` (datagram kind
    21) is retired and reserved; the table rides the cluster FSM's artifact
    (`service_id = 255`) instead, and `schedule_table_adopted`'s `source` is
    the single value `cluster_fsm`, not `log`/`boot`/`snapshot`.
    **Documented limits**: one possible duplicate tick per entry after a
    promotion (`Timed` drops it), no timezones and no cron. Plan 2's
    crash-between-record-and-persist window and plan 3's two ship-side windows
    (a restarted node under-shipping; a wiped node's position-0 record, ruling
    R7) are **CLOSED by the cluster FSM** — the next sub-bullet — which is why
    that feature exists; `SNAP_TABLE`, `state/schedules.state`,
    `ScheduleRecord`/`prev`/revert and `shippable_schedule` are all retired.
  - **The cluster FSM.** Cluster data — membership, the schedule table and
    the new replicated settings record — lives in one **internal** state
    machine (`uc_node::cluster_fsm`, `const NAME = "uc_cluster"`), applied at
    **commit** by a fifth polling agent `uc2-cluster` and snapshotted into its
    own artifact `snapshots/cluster/snap-<pos>.ultcluster`. `FRAME_TYPE_CLUSTER
    = 4` (reusing `CONFIG`'s number) carries `kind: u8 ‖ reserved [u8; 7] ‖
    payload`, kinds `1 = Membership`, `2 = ScheduleTable`, `3 = Settings`;
    the retired `FRAME_TYPE_SCHEDULE_TABLE = 6` and the retired
    `DGRAM_KIND_SNAP_TABLE = 21` (retired before shipping) are
    reserved. The snapshot session ships the
    cluster artifact under `service_id = 255`, last and outside the declared
    mask, and a below-floor joiner installs it BEFORE its floor advances — so
    `SnapBeginBody.config` is gone and `SNAP_BEGIN` is fixed-length at 120 B,
    layout **V4** (`SNAP_BEGIN_LAYOUT_V4 = 3`; V3 = 2 reserved). Membership has
    **two readers on purpose**: the consensus kernel keeps its durable-time
    shadow in `state/config.state` (Raft §4.1 — the newest config in the log,
    committed or not) and the FSM is the snapshot authority; `uc_sim`'s
    **inv12** sweeps that the FSM's membership is always a committed prefix of
    the kernel's. Settings (`fsm_lag`, `admission_bytes`,
    `snapshot_interval_bytes`, `snapshot_target`) are replicated: `[settings]`
    in `node.toml` **seeds genesis only**, `uc2ctl settings apply <file.toml>`
    changes them (admin op **7**, refusals **44–47**, audited `settings_apply`),
    `uc2ctl settings show` reads the committed artifact; the old top-level
    `admission_bytes` and `[services] fsm_lag` are **refused by name**; every
    replicated value is CLAMPED at the point of use; only the reserved
    `u64::MAX` sentinels (`admission_bytes`, `snapshot.interval_bytes`) and an
    `fsm_lag` byte bound below one max-size frame are refused at the door,
    with reason 47.
    Single-in-flight now spans all three kinds. The timer heap is
    **LEADER-ONLY** — a follower's service writes no `svc_sched` record and
    exports `timers_pending = 0`, a demotion discards the heap, and a new
    leader re-announces on the leader flag's rising edge;
    `uc2_timers_rearmed_total` is retired. The `uc_` FSM-name prefix is
    **reserved**. `uc_node` now depends on `uc_service`, which flips the
    crates.io publish order. There is no `state/schedules.state`, no
    `ScheduleRecord`/`prev`/revert and no `ScheduleShip` — both retired. Explainer
    `docs/notes/uc2-cluster-fsm-explained.md`; spec
    `docs/superpowers/specs/2026-09-05-uc2-cluster-fsm-and-coordinated-snapshot-design.md`
    (§5, coordinated snapshot instants, is that spec's plan 2 — the next
    sub-bullet). A pre-final pass closed four of the gaps plan 1 first left:
    the §9 gauges `uc2_cluster_fsm_position` (the FSM's consumed position, a
    per-node stall reading — NOT a fleet-wide constant, so no alert keys on
    it) and `uc2_settings_position`; the fifth `uc2_agent_alive` sample
    (`agent="cluster"`, which `/healthz`/`/readyz` and the daemon's fail-stop
    loop now cover too); the `uc_node_cluster_artifact` fuzz target; and
    `snapshots/cluster/` in `uc2ctl backup`/`verify-backup`/`restore`
    (`MANIFEST` is `uc2-backup-v3`). **The one recorded gap left**:
    `schedule show`/`settings show`/`status` read the artifact, so they say
    "no cluster artifact yet" until the first instant completes.
  - **Coordinated and standby snapshot instants (spec §5, plan 2).**
    `FRAME_TYPE_SNAPSHOT = 7` is header-only — the frame's END position **P**
    IS the instant — and broadcast: every declared row and the cluster FSM
    freeze at P, and a node holds the **complete set at P** when every row's
    `snap-<P>.ultsnap` plus `snapshots/cluster/snap-<P>.ultcluster` exist.
    That set is **committed by construction** (a row freezes only after
    applying to P, apply is gated on `min(commit, durable)`, committed bytes
    are never truncated), which is what lets the ship gate become "the
    complete set at my floor" with no counter. The retired `SnapshotPolicy` /
    `ServiceConfig::snapshot_policy` are **DELETED**; `start_with_snapshots`
    is the whole opt-in and sets `CNC_SVC_STATUS_SNAPSHOT_CAPABLE = 1 << 9`.
    Header flag `FLAG_SNAPSHOT_STANDBY = 0x01` (the byte `FLAG_TIMER_TABLE`
    rides in) makes an instant learners-only; a node reads its role from
    `NODE_FLAG_LEARNER = 4` in the cnc node-flags word. Return path:
    `DGRAM_KIND_SNAP_REQUEST = 22` / `SNAP_REDIRECT = 23`, driven by
    `uc2ctl snapshot fetch` (admin op **9**, node-local, never forwarded),
    taken **store-only** — nothing installed, the floor adopted through the
    ordinary completeness path. Admin op **8** = `uc2ctl snapshot
    [--standby]`; refusals **48 `snapshot_unsupported`** (naming the row),
    **49 `snapshot_no_learner`**, **50 `snapshot_above_durable`**. New cnc
    slot word `freeze_ns` at line 7 `+496` (**service**-written, unlike the
    rest of that line). Every artifact now carries a framework-owned 16-byte
    envelope, `ULTSNAP1 ‖ P` — the tag is an **EXCLUSIVE** frontier, so
    `install_snapshot(P)` returns `position` and must NOT report P from
    `last_applied()`, and no payload-side check can catch a mis-tag;
    pre-envelope artifacts are refused by name (clear a dev box's
    `snapshots/` once). **Retention is node-owned and delete-only** — both
    per-writer `retain_newest(2)` pruners are retired, because only the node can
    see a *set*. A replayed span acts on its LAST `SNAPSHOT` frame (ruling
    P10). 8 metric series + `Uc2SnapshotStalled` /
    `Uc2StandbySnapshotStalled` / `Uc2SnapshotSetDiverged`;
    `snapshot_session_refusals()` is a 5-tuple. The instant gauge is split by
    ruling P13: `uc2_snapshot_instant_position` counts FULL instants only and
    `uc2_snapshot_standby_instant_position` is learner-only, so a healthy
    `target = learners` cluster (whose voter leader's own set never completes
    without `uc2ctl snapshot fetch`) does not read as a dead FSM. The
    freeze cost is real and documented, not hidden: a freeze on a quorum
    stalls commit at `P + fsm_lag` until the slowest ends, which is the whole
    reason `--standby` exists. Explainer
    `docs/notes/uc2-cluster-fsm-explained.md` § Instants; spec errata
    "(plan 2, as built)".
  - **The relayout is the sharper half of this flag day.** Every prior wire
    bump was caught by a length check, so a mixed cluster stalled. A relaid
    header is the *same length*: a `0.6.0` peer's frames parse and mean
    something different. Stop every node before starting any node.
  - See the "Next up" paragraph above,
    `docs/notes/uc2-fsm-identity-and-deterministic-ids-explained.md`,
    `docs/notes/uc2-log-time-and-timers-explained.md` (its "The schedule
    table" section for plans 2 and 3),
    `docs/notes/uc2-cluster-fsm-explained.md`, and
    `docs/reference/semver-policy.md`'s
    FSM-identity carve-out (ships as the next minor, `2.11.0`, not `3.0.0`,
    per the maintainer's decision). `Uc2LogTimeFrozen` and
    `Uc2ScheduleTableDiverged` have `RULE_BUILDERS` entries in
    `scripts/m10_alert_fire.sh` since `e8e3a25`, backed by the
    `log_time_frozen` and `schedule_diverged` scenarios, so its completeness
    cross-check passes and the M10 gate's row 4 can be re-run as written —
    run locally 2026-09-07: **23/23 rules fire** under promtool, the five
    2.11.0 rules on synthetic sources; not yet on a fleet.
- **Wire crypto is opt-in and OFF by default**, all-encrypted or
  all-cleartext per cluster (no mixed mode). Threat model: a network-path
  adversary; out of model: a compromised host or a malicious member — the
  fan-out group key is symmetric, so any holder can forge fan-out traffic as
  any node (a documented residual). UC seals its own reliable-UDP transport;
  no QUIC.
- **`[crypto]` and `[admin]` are explicit config choices since 2.6.0** — a
  `node.toml` without both refuses to start by name (a per-host edit, not a
  wire flag day; see `docs/how-to/upgrade-a-cluster.md`). Admin requests are
  HMAC-SHA256-signed, with an append-only, fsync-per-record audit log
  (`<instance_dir>/audit.jsonl`). Residual: a follower forwards an
  authenticated admin request to the leader over the node↔node UDP plane, so
  `[admin] auth = "hmac"` authenticates cluster-wide only when paired with
  `[crypto].enabled = true`.
- **Command payload ceiling: discovered per cluster** — 1344 B crypto-off /
  1312 B crypto-on at the 1408 B baseline rung every cluster starts from, up
  to 8896 / 8864 at the 8960 B rung once every path has proven it (`2.12.0`,
  spec `docs/superpowers/specs/2026-09-10-uc2-jumbo-frame-discovery-design.md`).
  The arithmetic is unchanged (`payload_ceiling(rung, crypto)`); what moved is
  that the rung is committed cluster data, not a source constant, and
  `max_payload` is no longer a `node.toml` key. One command must fit one
  datagram, and `MTU_DEFAULT = 1408` (`uc_protocol::v2::datagram`) is sized
  to clear a 1500 B Ethernet path without IP fragmentation; `MTU_BOUND =
  8960` is the top of the ladder `RUNGS = [1408, 8832, 8960]`. The arithmetic
  (`docs/security/attack-surface.md` §3) is pure transport geometry: 1408
  less the 16 B datagram header less the 32 B frame header, floored to
  `FRAME_ALIGNMENT = 32` → 1344; crypto's `CRYPTO_OVERHEAD = 24` (8 B
  counter + 16 B GCM tag) takes the next aligned step down → 1312; the same
  arithmetic at the 8960 B top rung gives 8896 / 8864. (Aeron, whose 1408 UC
  inherited, lands on the same 1344 — it is the top `MESSAGE_LENGTH` in
  `aeron-io/benchmarks`.) The rung a cluster runs at is discovered (a
  do-not-fragment `PROBE`/`PROBE_ACK` pair up the ladder, committed through
  the replicated Settings record, monotone — never lowers) and lives in the
  cnc page's live `payload_ceiling` word, not a source constant;
  `max_payload` is retired from `node.toml`, refused by name. `bincode` is
  `NoLimit`; the typed tier's decode is bounded by the payload cap and
  serde's 1 MiB pre-allocation cap, not by the codec.
- **Purge is OFF by default** (`PurgePolicy::Disabled`), and since
  `2.11.0` a purge floor moves only on a **complete snapshot set**
  at one commanded instant (`uc2ctl snapshot`, or the replicated
  `snapshot_interval_bytes` cadence — `0`, the default, means no cadence). The
  `/metrics`/`/healthz`/`/readyz` endpoint exists only when `[metrics]` is
  configured; readiness keys on `can_serve`, never the leader flag; the
  peer-slot metric band is leader-authoritative (followers export zeros).
- **Instance dirs reserve ~78 MiB at boot** (the IPC backing files are
  fallocated, not sparse, so a full disk is a named startup refusal instead
  of a SIGBUS mid-run); a node that cannot reserve it refuses to start.
  ~79 MiB since `2.11.0`, where `svc_sched.<row>.ring`
  takes the per-row cost from 5 to 6 MiB (that ring is written and drained
  only while a node LEADS, since the cluster FSM).
- **M13 mechanics worth knowing**: the MPSC ingress ring commits per record
  (ring magic `ULTRNG2` — a same-host restart re-initialises the ring; a
  dead producer's hole is skipped and counted, cnc offsets 3968/3976);
  `RemoteClient` is a thin blocking layer over `RemoteEngine`'s send/poll
  halves; the gateway holds a global grant budget (the Engine window less
  1/8 headroom, divided across live connections). The M12 "collapse past the
  admission window" diagnosis was wrong — the cause was a ring publish
  convoy; see `docs/notes/uc2-m13-mpsc-publish-convoy-explained.md` before
  trusting any pre-2.7.0 gateway sizing advice.
- **M14 mechanics worth knowing**: ≤ 8 FSMs, id 0 mandatory and
  remote-reachable; lag policy per node must match cluster-wide (checked on
  the snapshot path); one stalled FSM on a quorum of hosts stalls commit by
  design (report ceiling); `service.<id>.lock` per FSM.
- **13 publishable crates, versioned in lockstep** with the tag and the
  image; `uc_sim`, `uc_lincheck` and the example crates are
  `publish = false`. Publishing is manual and ordered
  (`docs/how-to/cut-a-release.md` §6) — and the order **flipped** in
  `2.11.0`: `uc_node` now depends on `uc_service` (the
  cluster FSM implements the same traits a user's state machine does), so
  `uc_service` publishes first; `uc_service`'s dev-dependency on `uc_node`
  stays a dev-only cycle under the existing unversioned idiom; `deny.toml` + `cargo-deny` run in CI
  (one documented ignore: RUSTSEC-2025-0141, `bincode` unmaintained, no
  patched version exists). Docker/compose/ghcr/cosign are CI-only; aarch64
  binaries are built but never executed in CI — but the full correctness
  stack (workspace + lin capstones + hard-crash) first ran and passed on
  real ARM hardware 2026-08-31
  (`docs/benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md`).
- **Security posture**: `docs/security/{threat-model,attack-surface,self-assessment}.md`
  + root `SECURITY.md` (supported = latest minor; GitHub private
  vulnerability reporting). The whole proof surface is mapped in
  `docs/VERIFICATION.md` — sim, lincheck/crashtest capstones, Elle, Lean
  proofs + conformance, loom (log-buffer frame visibility, the MPSC ring's
  per-record commit, and the Broadcast ring's seqlock read barrier — the last
  found and fixed a real weak-memory defect when it was written, 2026-08-31),
  **24** fuzz targets (15 before `2.11.0`, which added eight; `uc_protocol_probe`
  is the pending `2.12.0`'s), Miri (pure decoders + `uc_remote`'s
  Vec-backed SPSC internals; the mmap'd IPC rings are out of Miri's reach).
- **`cargo fmt` is ENFORCED** since 2026-08-31: `cargo fmt --all -- --check`
  is the first step of `ci.yml`'s `test` job, so workspace drift is zero and
  stays zero. The long deferral (3 393 hunks by the end) was discharged as one
  mechanical commit once `fix/remaining-flakes` landed — history before that
  commit is unformatted, so `git blame` across it needs `-w`. `fuzz/` is
  outside the workspace and `--all` does not reach it.

Canonical documents, in order:

0. `docs/notes/state-machine-replication-explained.md` — what SMR *is*, and
   the single source for the concept (`README.md` and `docs/ARCHITECTURE.md`
   both point here rather than restating it; keep it that way). Read it if
   you are explaining the model to anyone, or writing prose that describes it.
1. `docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md` — the
   canonical v2 design spec; read it end-to-end before substantial work.
   Later milestones have their own specs beside it (M7 reconfig, M8 wire
   crypto, M12 adoptable, M13 remote path — each amended with as-built
   errata where execution diverged from the draft).
2. `docs/benchmarks/uc2-m*-gate-*.md` — the per-milestone gate docs (the
   permanent record for v2; the `taskNN` docs under `docs/tasks/` are v1-era
   history).
3. `docs/ops/uc2-runbook.md` — operational runbook (instance-dir layout, cnc
   decode, purge enablement, live reconfiguration ops).
4. Storage primitives: `../ultima_db/docs/tasks/task26_journal.md` —
   `uc_journal`'s design notes, which live in the sibling repo for historical
   reasons (the crate itself is in-tree). **UC has no `ultima-db` dependency**
   since 2026-08-31; the workspace's only durability primitive is
   `uc_journal`, and a service brings its own state machine.

## Build & Test Commands

MSRV is 1.89 (`rust-version` in the root `Cargo.toml`'s `[workspace.package]`
— see that field's comment for how it was probed; CI's `msrv` job runs
`cargo clippy --workspace --all-targets --locked -- -D warnings` directly
against a 1.89.0 toolchain, not just `check`). Local dev, the rest of CI, and releases
build on the newer stable pinned in `rust-toolchain.toml` (currently 1.96.0;
rustup auto-installs it). To bump the pin: `rustup toolchain install <ver>
--profile minimal --component rustfmt --component clippy`, update `channel`
in `rust-toolchain.toml`, then run the full local proof stack before
committing — the MSRV floor is a separate, deliberate decision, not moved by
this.

```bash
cargo build --workspace                          # build all workspace crates
cargo test                                       # in-process integration + sim tests (default)
cargo test -p uc_node --test lin_v2             # WGL linearizability capstone (failover + purge/snapshot churn)
cargo test -p uc_node --test lin_partition_v2   # network-partition / quorum-loss linearizability
cargo test -p uc_crashtest --features hard-crash-tests   # spawn real node+service procs; SIGKILL mid-load, assert linearizable
cargo clippy --workspace --all-targets -- -D warnings     # lint (must pass with zero warnings)
cargo run -p uc_node --release --example m5_gate # throughput gate harness (see the gate doc)
cargo run -p uc_node --release --example m6_gate -- all --secs 6 --cycles 5   # snapshots/learners/purge gate
cargo run -p uc_node --release --example m7_gate -- all --secs 6             # live reconfig gate (replace/resize/self-removal)
cargo run -p uc_ctl -- status --instance-dir D --app-id A  # M7 admin CLI: add/promote/demote/remove/status
scripts/fuzz_smoke.sh 60 --min-runs 10000         # fuzz regression gate: every target, 60s each (needs nightly + cargo-fuzz)
(cd fuzz && cargo +nightly fuzz run uc_protocol_datagram -- -max_total_time=600)  # hunt one target
scripts/elle_check.sh                            # elle consistency tier: 5 list-append passes, both models (needs java+jq)
scripts/elle_mutation.sh                         # elle mutation testing: control clean + 3 injected consensus bugs caught
(cd proofs && lake exe cache get && lake build)   # Lean proofs: model + theorems + conform checker (needs elan)
cargo run -p uc_consensus --release --example conform_gen -- --out $HOME/.cache/uc2-conform/vectors.jsonl --count 100000 --seed 1 && (cd proofs && lake exe conform $HOME/.cache/uc2-conform/vectors.jsonl)  # model<->Rust conformance
RUSTFLAGS="--cfg loom" cargo test -p uc_protocol --release --test loom_mpsc  # MPSC ring loom model (also --test loom_broadcast; log buffer: -p uc_log --test loom_frame)
python3 bench-infra/scripts/m13_hop_bench.py --selftest  # M13 gate row arithmetic, no fleet/ssh
```

`fuzz/` is a `cargo-fuzz` crate **outside the workspace** (the root manifest
excludes it; it has its own `[workspace]` and lockfile), so `cargo
build/test/clippy --workspace` never sees it and it needs the nightly
toolchain plus `cargo install cargo-fuzz`. `scripts/fuzz_smoke.sh [--min-runs
N] [SECS] [TARGET…]` is the regression gate CI runs (`--min-runs 10000`
against 600 s per target); `fuzz/README.md` covers adding a target,
regenerating the corpus, and `tmin`/`cmin`.

The elle scripts write histories to `$HOME/.cache/uc2-elle*` (disk) — never
override `ELLE_DIR`/`ELLE_MUT_DIR` to `/tmp` (often RAM-backed tmpfs → OOM; see
"Local scratch" below). Nightly CI runs the clean tier (`elle` job); the weekly
`elle-weekly.yml` runs the mutation tier.

Cross-host fleet gates run via `bench-infra/` (terraform + ansible
provisioning); each milestone has its own driver under `bench-infra/scripts/`
— `m6_fleet_gate.py` (`--m7` for the M7 scenarios) through
`m9`/`m10`/`m11`/`m12_fleet_gate.py`, and `m13_hop_bench.py`, whose
`--arms gate` adjudicated the M13 bars on the fleet and whose `--selftest`
checks the row arithmetic locally.

Workspace crates:

- `uc_protocol` — wire spec; `core`-friendly data types (`version`, `magic`,
  `error_codes`) plus the lock-free ring buffers (`ring`:
  SPSC/MPSC/Broadcast — the MPSC ring commits per record since M13, so no
  producer ever waits on another)
  and the v2 wire spec (`v2`): the `cnc.dat` 8 KiB (cnc 3.0: page 2 is the
  per-service slot band) page layout, the self-locating
  UDP datagram header, and per-message frame layouts. Multi-language gate.
- `uc_log` — the log buffer + archive. File-backed shared log buffer (readers
  poll positions in place, bounded by the commit counter) and the archive agent
  that records ≤1 MiB blocks into `uc_journal` (the retransmit + recovery
  store). Owns snapshot builder + below-floor reconstruction primitives.
- `uc_net` — own reliable-UDP transport (no QUIC): sender/receiver polling
  agents, NAK-based retransmit off the log buffer, quorum-paced flow control,
  snapshot sessions. A seeded fault layer drives the sim.
- `uc_crypto` — **M8 wire crypto (opt-in, off by default)**: pure-sync,
  socket-free crypto plane for node↔node UDP. Noise `IK` handshake (`snow`,
  X25519), per-peer pairwise keys + a rotating cluster group key, AES-256-GCM
  seal/open over the datagram envelope (16-byte header authenticated as AAD),
  RFC-6479 anti-replay, and the `SharedTransport`/`SendHalf`/`ReceiveHalf`
  split that keeps the per-datagram hot path off a lock. `uc_net` calls it at
  two seams; `uc_node` owns config, handshake routing, and key rotation.
- `uc_obs` — the structured JSON-lines log record format (`emit`, the
  `obs_event!` macro, the level filter, and the single `format_line_at`
  formatter the admin audit file also renders through). A dependency-free
  leaf so it can sit under every daemon: `uc2-gateway` must not depend on
  `uc_node`, and both emit the same records. Purpose-named on purpose —
  it is not a `common`/`util` dumping ground, and a crate rename is a
  major version now that the 2.9.0 carve-out is spent.
- `uc_consensus` — pure-sync Raft-safety core over **byte positions**:
  `CommitTracker` (quorum-th highest committed position), `ElectionSm`
  (lexicographic `(last_term, last_durable)` vote, data-stamped term map,
  truncation). No async, no I/O — driven deterministically by the sim.
- `uc_sim` — virtual-time deterministic world + safety invariants + seeded
  fuzz. The gate that proves consensus safety without hardware.
- `uc_node` — the node binary + library. Wires the single-writer polling
  agents (consensus / sender / receiver / archive, plus **`uc2-cluster`**
  since `2.11.0`), the `cnc.dat` page, the ingress ring, and
  the linearizable-read barrier. Owns elections, truncation, and — since the
  cluster FSM — the cluster's own replicated state (`cluster_fsm.rs`,
  `cluster_agent.rs`), which is why it now depends on `uc_service`.
- `uc_service` — service-side SDK. **M12a: two tiers.** `RawStateMachine`
  (bytes-in/bytes-out, the core contract) or the typed `StateMachine` (sync
  `apply`/`query`), which gets `RawStateMachine` for free via a blanket impl —
  a type implements exactly one of the two. Optionally `SnapshotStateMachine`
  (M6 purge) + `RawOutputHandler`/`OutputHandler` (async, leader-only,
  `TypedOutput` adapts the latter onto the former). `uc_service::session::
  Sessioned<S>` wraps either tier for exactly-once-over-a-remote-hop: a
  16-byte `client_id ++ seq` envelope, a 1-byte FRESH/REPLAYED/EXPIRED tag,
  replicated `SessionConfig` enforced at snapshot install. The apply agent
  polls committed positions in the log buffer; reconstruction replays the
  journal or installs a snapshot + tail-replays.
- `uc_client` — sync local-shmem input-client SDK. Small dep set (no transport,
  no consensus); matcher over the broadcast response ring.
- `uc_remote` — **M12a**: the remote wire protocol (protocol v1: framed TCP,
  credit-gated flow control, `REDIRECT`/`LEADER_CHANGED`/`RETRY`) and
  `RemoteClient`, the pipelined, redirect-following, re-sending Rust
  implementation of it — for clients that cannot attach to shmem directly.
  **M13**: rebuilt as the `RemoteEngine` split halves
  (`RemoteSendHalf`/`RemotePollHalf`, lock-free SPSC internals, count-based
  admission); `RemoteClient` remains as a thin blocking layer on top.
- `uc_gateway` — **M12a**: `Edge`, a per-node TCP front door that terminates
  `uc_remote` traffic and relays it over the local `uc_client::Engine`;
  ships as the `uc2-gateway` binary + `gateway.toml` + a systemd unit.
  **M13**: a global outstanding-grant budget — the sum of per-connection
  credits never exceeds the node's admission window.
- `uc_lincheck` — test/verification library: WGL linearizability `checker`, op
  `history` recorder, `model`, and the in-memory CAS-`register` SM
  (`Cmd`/`CmdResp`/`RegisterSm: uc_service::StateMachine`). One source of truth
  shared by the in-process lincheck capstone (`uc_node/tests/lin_v2.rs`) and the
  multi-process hard-crash test.
- `examples/uc_crashtest` — multi-process test harness: reference bins (node +
  service halves over a shared instance_dir) + the hard-crash tests behind the
  `hard-crash-tests` feature. The real `kill -9` path for reconstruction validation.
- `uc_journal` — segmented append journal + `StableValue`. In-tree workspace
  member (moved in from `ultima_db`; full history preserved).

## Local scratch: keep heavy artifacts off `/tmp`

On many Linux dev boxes `/tmp` is `tmpfs` — RAM-backed — and swap may be small
or absent. Anything written there (including the agent scratchpad at
`/tmp/claude-*/`) then consumes resident RAM, and large test outputs
(multi-tens-of-thousands-of-event elle histories, journal segments, load-test
dumps) race the busy-spin node clusters and `cargo` release builds for the
same pool until the kernel `SIGKILL`s the biggest process (exit 137/143) —
which manifests as tests dying mid-run or the Claude Code harness itself
getting torn down ("previous process exited"). This has happened repeatedly
on developer machines; avoid it structurally rather than by assuming the
current machine is big enough (do not encode a particular box's size,
mounts, or free space here — this file is shared):

- **Write test/scratch artifacts to real disk, under `$HOME/scratch/`** — NOT
  `/tmp`, and NOT the home directory itself. One sweepable root is the whole
  point: an unnamed "somewhere under `$HOME`" is how a home dir accumulates a
  hundred loose `fix*.py`/`*.log` files and multi-GB stray cargo target dirs,
  with no way to tell scratch from work. A stray target dir belongs in
  `~/.cache/cargo-target-<name>` (see "Benchmarking discipline"), never in
  `$HOME`. For the elle harness set `ELLE_DIR=$HOME/scratch/elle-out`, never
  the default `/tmp/uc2-elle`. Check with `findmnt /tmp` if unsure what backs
  it.
- Test **instance dirs / journals already go to the cargo target tree** via
  `env!("CARGO_TARGET_TMPDIR")` (the `tempdir()` helper in the test suites) —
  keep it that way; do not `tempdir()` under `/tmp`.
- Keep generated histories small (cap op targets), bound `elle-cli`'s JVM heap
  (`-Xmx`), and `rm -rf` scratch between runs to reclaim RAM.

## Benchmarking discipline

Perf **rate bars are fleet-only** (`bench-infra/`); a local run is **smoke**,
never a gate — never move a bar because a dev-box run went red. A dev box is
noisy whatever its size (busy-spin agents contend for the scheduler): on one,
the same dip measured 7× spanned 0–18% against a 10% bar.

The cargo target dir (`~/.cache/cargo-target`) is **shared by the main
checkout and every worktree**, so another checkout's build can silently swap
your binaries mid-measurement. For any measurement or proof stack run from a
worktree, set a private `CARGO_TARGET_DIR=<path>` and verify binary
provenance before trusting a number.

## Finding a performance bottleneck

UC's SMR is a chain of hops; throughput is bounded by the slowest. Don't
micro-optimize blindly — **isolate each hop, measure it alone** (realistic
stand-ins at the boundaries: dummy sink, dummy upstream, raw driver), and
compare against the whole-chain number: the hop whose solo throughput ≈ the
whole-chain throughput is the limiter, and optimizing any faster hop measures
null end-to-end. Two refinements from M13:

- A whole-system **collapse** can be an emergent pathology under a stress
  dimension, not any hop's slow steady state — sweep the stress axes
  (concurrency, inflight), and **reproduce the collapse in the smallest
  isolated hop** before believing a causal story.
- **Measurement refutes plausible-but-wrong stories.** M12 blamed the credit
  budget because the collapse "appeared past the admission window"; the
  isolation matrix (window large *and* small, sink with *no* window, load
  inside the envelope) proved it was an ingress-ring publish convoy —
  "consistent with the symptom" is not "the cause."

Two more from M14a's apply-hop isolation (`docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md`):

- **Code in a hot loop's body costs even on paths that never run.** A wait
  ladder added inline to the apply loop's `Wait` arm cost 9 % at N=1 — a path
  N=1 never executes — through codegen alone; out of line it cost 1.5 %.
  A/B the *exact binaries* back to back on an idle box before attributing a
  delta to the change's semantics, and keep the hot body small.
- **A barrier wait must never sleep on a live peer.** One lockstep FSM in a
  50 µs sleep stalls every sibling's next frame, their ladders exhaust, and
  the set cascades into sleeping in lockstep (18 k frames/s); the yield budget
  has to exceed *any* plausible handshake, not the common one, and spinning
  on a slow peer's line only slows that peer (−6 % bounded at N=8).
- **Exact binaries are not enough — rebuild the same source twice first.**
  M14b's client-hop A/B read −4.2 % on one binary pair (17 pairs, no
  overlap); fresh builds of the same two commits read ±0.3 %, and two
  builds of the *same* commit differed by 1 %. Before attributing a delta to
  code, measure the harness's build-to-build resolution with a same-source
  rebuild control, and only trust deltas outside it (`scripts/hop1_ab.sh`,
  `docs/benchmarks/uc2-m14c-client-hop-2026-08-28.md`).

One more from the 2.11.0 apply-hop regression (2026-09-07, the time-and-timers
gate doc's row d ledger):

- **A hot loop's callees fall out of line when the CALLER grows.** Nothing in
  the frame loop's own arms explained the last 12 % of a 27 % loss: LLVM had
  stopped inlining the loop's per-frame callees (`FrameIter::next`,
  `read_header`, `Egress::publish`, `BroadcastProducer::write`) once
  `apply_cycle` outgrew its inlining budget — each became a GOT-indirect call
  per frame with its result round-tripping the stack. The per-frame callees
  are `#[inline(always)]` now, which pins them regardless of caller size. The
  `apply-profile` rdtsc probes could NOT see this (they read per-frame parity
  while the plain A/B read −12 %): when the profile and the rate disagree,
  read the machine code — `objdump -d -C` on the runner's kept
  `apply_bench.{a,b}` binaries, `readelf -rW` to resolve `call *slot(%rip)`
  targets, and compare the per-frame path's call list arm against arm.

Harness models: `uc_gateway/examples/hop_bench` (client/edge/node hops),
`uc_node/examples/apply_bench` (the FSM hop alone), `scripts/hop1_ab.sh`
(the client hop A/B, with a same-source rebuild control); worked example
`docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md`; the convoy mechanism
`docs/notes/uc2-m13-mpsc-publish-convoy-explained.md`.

## Architecture overview

UC is a State Machine Replication application server. Three process roles;
same-host inter-process traffic via shared memory, cross-host traffic via UC's
own reliable-UDP transport between nodes:

```
[client process]      ──shmem──▶  [uc_node]  ◀──reliable-UDP──▶  [uc_node on peer host]
                                      ▲
                                      │ shmem (file-backed log buffer + cnc page)
                                      ▼
                                 [uc_service]
```

Each node is **five single-writer polling agents** (four before
`2.11.0`), counter-coordinated (no
locks on the hot path): **consensus** (commit tracking + elections), **sender**
and **receiver** (reliable-UDP replication + NAK repair), **archive** (record
the log buffer into `uc_journal` in ≤1 MiB blocks), and **`uc2-cluster`**
(apply the cluster FSM's `CLUSTER` commands at commit, publish its view, write
its artifact). All coordination is
through atomic counters in the `cnc.dat` page and monotonic byte **positions**
(the absolute-offset analog of a Raft log index); `apply` is keyed on position.

- `uc_node` owns consensus, log durability, snapshot transport, leader election,
  and the cluster's own replicated state (the cluster FSM).
- `uc_service` owns the user's deterministic business logic (`apply`, `query`)
  and side-effecting `on_committed` (leader-only, at-least-once).
- Client processes translate external requests into Commands and submit via shmem.

The shmem layer is a fixed-layout `cnc.dat` 8 KiB (cnc 3.0: page 2 is the
per-service slot band) control page (`uc_protocol::v2::cnc`,
offsets pinned in both `uc_protocol` and `uc_log` so they never drift) plus the
file-backed log buffer and per-stream ring buffers under an instance directory.
Ring buffers are lock-free; SPSC for service↔node, MPSC for clients→node,
Broadcast for node→clients (position-keyed responses bypass the node via an
egress broadcast).

Storage primitives:
- Log buffer: `uc_log` file-backed ring; the appender never overwrites bytes not
  yet recorded (one hard overrun gate); all other readers degrade to journal replay.
- Archive / recovery: `uc_journal::Journal` (segmented append, group commit,
  CRC per block; block seq = block index, meta = base position).
- Durable state: `uc_journal::StableValue<T>` (rotating two-slot atomic value)
  for vote, term map, snapshot floor, output progress, cluster-config record (config.state).
- App state + snapshots: the user's `StateMachine`. M6 snapshots use the
  `SnapshotStateMachine` capability; the artifact's PAYLOAD bytes are entirely
  the service's own business — UC ships no store and prescribes no snapshot
  encoding, but since `2.11.0` it does own a 16-byte
  `ULTSNAP1 ‖ P` envelope ahead of them, and the artifact tag is an
  **exclusive** frontier. `uc_lincheck`'s `RegisterSm`/`ListAppendSm` are the
  worked examples. **When** a snapshot happens is no longer the service's
  choice: it is a coordinated instant on the log (`SNAPSHOT` frame, type 7),
  not a per-service byte counter.

Replication is reliable-UDP: the log buffer doubles as the retransmit buffer, a
receiver that falls behind sends NAKs repaired from the buffer (or, below the
purge floor, upgraded to a snapshot session), and flow control is a quorum
order-statistic over follower durable positions. A follower more than one buffer
behind is served from the journal (deep-NAK replay), never prefilled.

Commit / apply pipeline (steady state):
- Client writes a submit frame into the ingress MPSC ring (admission window at the door).
- The leader appends to the log buffer, replicates via the sender agent, and the
  consensus agent advances the commit counter when a quorum's durable positions cross it.
- The service's apply agent polls `min(commit, durable)` in the log buffer in
  place and calls `state_machine.apply(ctx, cmd)` (the position lives on the
  ctx since 2.11.0), publishing the response
  to the egress broadcast (position-keyed) for the client's matcher.

Snapshots + purge (M6, **OFF by default** — `PurgePolicy::Disabled`): a
service-built snapshot lets a node drop the journal prefix below the snapshot
floor (`PurgePolicy::BelowSnapshot { slack_bytes }`). A node that has fallen
below the floor — a crashed-and-restarted service, a fresh **learner**
(replicated-to, never counted in quorum), a cold-started node — converges by
**installing a snapshot + tail-replaying**, never by reading the purged prefix.
`NoCommonPrefix` = wipe-and-rejoin.

Linearizable reads go through a `READ_PROBE`/`ACK` quorum barrier, wait for the
service to catch up to the read position, and use a follower header-term check +
capture-recheck + a service-epoch backstop (accept the answer only if the service
didn't restart during the query) to close the TOCTOU against a crashing service.

Correctness is proven at three levels: the deterministic sim (`uc_sim`, safety
invariants + seeded fuzz), the WGL lincheck capstones (`uc_node/tests/lin_v2.rs`
under failover AND purge/snapshot churn; `lin_partition_v2.rs` under
partition/quorum-loss — all driving the untouched `uc_lincheck` checker), and the
multi-process SIGKILL crashtest (`examples/uc_crashtest`).

## Code conventions

- **`uc_protocol` core types stay `core`-friendly.** `version`/`magic`/`error_codes`
  import nothing outside `core`; the ring buffers need `std::sync::atomic` + `memmap2`.
  No `tokio` in the protocol layer.
- **Apply is sync, deterministic, no I/O.** The trait signature enforces it:
  `fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Self::Command) -> Self::Response`
  (2.11.0 replaced the bare `position: u64` parameter with `ApplyCtx`, which
  carries `position`, the deterministic `time_ns`, `term`, and the timer
  handles `schedule`/`cancel`). No `async`, and no AMBIENT clock or randomness
  — 2.11.0 did not weaken this, it replaced both with replicated substitutes:
  `ctx.time_ns` is the leader's stamp carried on the frame (every replica
  applies the same value), and `IdGen` derives ids from the position, so
  reaching for `SystemTime::now()` or an RNG inside `apply` is still the
  divergence bug it always was. Non-negotiable for SMR correctness. `position`
  (the absolute byte offset) is the idempotency key.
- **Consensus is pure-sync.** `uc_consensus` (CommitTracker, ElectionSm) has no
  async and no I/O — it is driven by the node's polling agents and the sim. Safety
  logic lands there so the sim can adjudicate it deterministically.
- **`output_handler` is async, leader-only, retryable.** Returns
  `Result<(), OutputError>` where `Retryable` retries while leader and `Permanent`
  advances the durable (increase-only) progress marker anyway.
- **Reads are typed `Query` / `QueryResponse`, not closures.** The IPC boundary
  doesn't carry closures; the framework routes linearizable vs. snapshot reads.
- **`AppCommand = bytes::Bytes` end-to-end.** Refcounted; flows from the log
  buffer through apply without intermediate copies.
- **Per-record framing uses an atomic-after-write length prefix.** Reader sees
  length=0 → record not yet committed → spin/yield. Standard torn-record protection.
- **cnc page offsets are pinned in BOTH `uc_protocol` and `uc_log`** with
  offset-assertion tests, and must never drift. Add fields in the reserved band.
- **Snapshot `freeze`/`install_snapshot` are keyed on `position`.** `install_snapshot`
  takes the target position and rejects a mis-tagged artifact.
- **One node per instance directory** — an exclusive flock prevents accidental
  coexistence; service and clients take a shared lock as a liveness probe.
- **`app_id` + `instance_id` + `protocol_version` checked at every IPC entry.**
  Wrong `app_id` = wrong cluster; changed `instance_id` = node restart since last
  attach; protocol mismatch = refuse.

## Feature Development Workflow

Using superpowers (brainstorming, writing-plans, executing-plans) during feature
development is fine — the generated plans/notes under `docs/superpowers/` are
working artifacts. For v2, the per-milestone record is the **gate doc**
(`docs/benchmarks/uc2-mX-gate-*.md`) plus the runbook and the retained superpowers
plan; there is **no `docs/tasks/` consolidation** for v2 (that pattern was v1-era).

**Leave the corresponding superpowers artifacts (`docs/superpowers/plans/*.md`,
`docs/superpowers/specs/*.md`) in place.** Do NOT delete them as part of finishing
a feature — they are retained as historical scaffolding; the maintainer removes
them manually if ever.

## Release documentation (required for every release)

The root **`RELEASES.md`** is the user-facing release document; `docs/releases.md`
is the deep per-release engineering record behind it. **Every new release adds a
new section at the top of `RELEASES.md`** (latest first), structured as:

1. one bullet per **feature**, briefly explained, each linking to a separate
   detailed doc (how-to / reference / `docs/notes/` explainer) — **write the
   detailed doc if it does not exist yet**;
2. one optional bullet for **fixed bugs**, with links to the docs that cover
   them (if they exist);
3. one optional bullet for **performance** results, with links to the gate /
   benchmark docs (if they exist).

Do this — plus the matching `docs/releases.md` entry and a sweep of
QUICKSTART / how-to / reference for statements the release invalidated —
**before tagging**, so the tag contains the writeup. README's "Scope and
limits" section stays a pointer to `RELEASES.md` plus the standing limits, not
a parallel prose copy of the release history.

## Pointers to dependent crates

- `uc_journal/` — segmented append journal + `StableValue`. In-tree workspace
  member (moved in from `ultima_db`; full history preserved). Design notes:
  `../ultima_db/docs/tasks/task26_journal.md`.
- **`ultima-db` is no longer a dependency of any kind** (removed
  2026-08-31). It had been an optional dep behind `uc_service`'s non-default
  `ultima_db` feature, carrying a `StoreStateMachine` adapter that nothing in
  the tree used except its own roundtrip test — no binary, example, gate
  harness or capstone ever built it, and `snapshot_stream` appeared nowhere
  outside the adapter. Removing it dropped three crates from `Cargo.lock`
  (`ultima-db`, `dashmap`, `hashbrown`), four CI/nightly/docs steps, and
  `uc_service`'s only crates.io coupling. Do not reintroduce it: a service
  supplies its own `StateMachine`, and UC prescribes no store.
