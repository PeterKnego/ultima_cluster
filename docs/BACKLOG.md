# Backlog — candidate directions after M14

*Written 2026-09-01 against `v2.10.0`. Status: a ranked list of options, not
a plan. Nothing here is scheduled; the maintainer picks. Every item cites the
document that first recorded it, so the reasoning can be re-checked rather
than re-derived. When an item is taken up it gets a spec under
`docs/superpowers/specs/` and a gate doc under `docs/benchmarks/`, and its
line here is updated to point at them. When an item is dropped, say why
here rather than deleting it — the "Deprioritized" section of
`docs/superpowers/specs/2026-08-01-uc2-formal-roadmap.md` is the model.*

## Where this list comes from

M8–M14 turned the v2 engine into a deployable product (`RELEASES.md`). What
the record does not contain is a user: the only in-tree service is the
`examples/counter` crate, `docs/reference/remote-protocol.md` still describes
itself as "the page a non-Rust port implements from", and no port exists.
Every remaining gap the docs record is either an accepted residual
(`docs/reference/limits.md`, `docs/security/self-assessment.md`) or a
deferral waiting for a reason to matter. The ranking below follows from
that: the first direction is the one that supplies the reason.

## Ranked directions

### 1. Dogfood — a real service plus a second-language client

Build one non-trivial reference service in-tree (a sessioned key-value
store or an order book), and a remote client in a second language (Go or
Python) written from `docs/reference/remote-protocol.md` alone, as that page
invites. Run both through the gate discipline.

- **Why first:** it exercises the whole M8–M14 surface the way a user would,
  and it settles two questions the docs cannot settle on their own:
  - whether the **command payload ceiling** (≤ 1344 B crypto-off / ≤ 1312 B
    crypto-on, one command per datagram — `docs/security/attack-surface.md`
    §3, `CLAUDE.md` standing facts) is a real adoption blocker. Moving it is
    a wire flag day (fragmented commands, jumbo frames, or an OS-bypass
    fabric), so it needs a workload to justify it.
  - whether the **remote protocol needs a v2**: `SUBMIT`/`QUERY` carry no
    FSM selector, so a remote client reaches only FSM 0
    (`docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md` §11 "Out
    of scope"; `docs/releases.md` 2.8.0 entry).
- **Cost:** moderate. **Output:** a backlog grounded in use, plus the two
  decisions above.
- **Status 2026-09-01: brainstormed and PARKED** — decisions (KV store,
  Go in a separate repo, docs-sufficiency bar, clean-room build, op set,
  wire format, state machine) are recorded in
  `docs/superpowers/specs/2026-09-01-uc2-dogfood-kv-and-go-client-brief.md`;
  the maintainer paused to add features to UC first. Resume from that
  brief's "Where the design stopped".

### 2. FSM identity — name the state machine, not the slot

*Added 2026-09-01; taken up the same day (brainstorm in progress).*

M14 identifies an FSM only by its slot number. Attach validates `app_id`
and the per-boot `instance_id` (`uc_service/src/attach.rs`), then checks
that the numeric `service_id` is in the node's declared set — and nothing
else. Two nodes agree on the *set of slot numbers*
(`docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md`, "Declared
set" and "Command delivery" rows) and never on what logic each slot holds.
The slot is placement; nothing states identity. An FSM name is the per-FSM
analog of `app_id`: a declared identity checked at every boundary that
today checks only the number.

- **What it binds:** attach refusal by name (service's declared name vs. the
  node's slot→name map); the cluster-wide agreement check the snapshot path
  already runs for the declared set and lag policy; snapshot artifacts
  (install rejects a foreign FSM's artifact the way it rejects a mis-tagged
  position); query routing by name instead of slot, closing the wrong-slot
  read hazard (queries are slot-routed: `query.ring` payload is
  `service_id:u8 ++ query`, spec §5.4); and deterministic ID derivation.
- **First consumer:** a deterministic ID utility in `uc_service` — the same
  series of IDs on every replica, per `(position, FSM identity, ordinal
  within this apply)`, stateless so a snapshot-installed replica and a
  journal-replayed one agree by construction. The identity is what keeps
  the IDs placement-independent; without it the utility would need a
  hand-rolled domain tag that this work would then replace.
- **What it does not do:** a name says two replicas *intend* to be the same
  FSM; it does not verify they run the same code.
- **Cost:** moderate. Config + attach + a cnc slot-band field (reserved
  line 7; same-host, recreated per boot — confirm against
  `docs/reference/semver-policy.md` whether that is a cnc version bump or
  a flag day) + snapshot header + client/gateway name resolution.
- **Status 2026-09-02: IMPLEMENTED on branch `uc2/fsm-identity`, release on
  hold** — spec `docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md`
  ("named rows": identity in code, `const NAME` + `const VERSION`; the row
  keeps its cluster-wide meaning and a service finds it by name; `SNAP_BEGIN`
  0.7.0 carries hashes + versions per row, compared positionally and refused
  by name; cnc 3.1; `IdGen`; disk/rings/client engine untouched; the
  placement-independent variant was cut by the spec's §2.1 comparison
  table). Plan `docs/superpowers/plans/2026-09-02-uc2-fsm-identity.md`
  (T0–T10), all tasks done: code (`uc_protocol`/`uc_service`/`uc_node`/
  `uc_client`), harnesses/capstones by name, docs + explainer + gate-doc
  skeleton. Not yet released — no version bump, no tag, no fleet run; more
  changes are planned on this branch before a release. Explainer:
  `docs/notes/uc2-fsm-identity-and-deterministic-ids-explained.md`.

### 2a. Time and timers — DONE (both plans), release on hold

*Added 2026-09-03, when plan 1 was implemented; plan 2 followed the same day.
Kept here as the record of a finished item rather than deleted (see this
page's preamble). Not a ranked item on the
2026-09-01 list: leader-stamped log time and a deterministic scheduler were
requested by the maintainer directly on 2026-09-02 and specced beside FSM
identity, which is why this line sits under item 2 rather than getting a
number of its own.*

**Plan 1 is IMPLEMENTED on the same branch as FSM identity, release on
hold.** Spec:
`docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md`; plan:
`docs/superpowers/plans/2026-09-03-uc2-time-and-timers-plan1.md` (T0–T14, all
tasks done). What shipped: a leader-written `time_ns` in every frame header
(the header was relaid to pay for it, so the payload ceiling is unchanged),
the `max(now, last)` clamp, `ApplyCtx::{time_ns, term, schedule, cancel}`, a
provided `on_timer` on both tiers, `FRAME_TYPE_TIMER` with deadline-stamped
in-order placement, the per-row node heap with re-arm on leadership loss,
`uc_service::Timed<S>` for exactly-once delivery, one new per-row IPC ring
(`svc_sched.<row>.ring`), two cnc words, six metric families and one alert
rule. Explainer:
`docs/notes/uc2-log-time-and-timers-explained.md`; gate skeleton:
`docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md` (bars committed, no
run). It rides the same unreleased `2.11.0` flag day as FSM identity.

**Plan 2, the replicated schedule table, is IMPLEMENTED too** — same branch,
same unreleased `2.11.0` flag day, release still on hold. Plan:
`docs/superpowers/plans/2026-09-03-uc2-time-and-timers-plan2.md` (T0–T8, all
tasks done). What shipped: `FRAME_TYPE_SCHEDULE_TABLE = 6` with a frozen,
total, fuzzed codec (`MAX_SCHEDULE_ENTRIES = 32`, 33-byte entries, 1064 B
full — always one datagram); three rules (`every` from an anchor, `at` daily
UTC, and `once`, which parks in the table after firing); `uc2ctl schedule
apply <file.toml>` staging `<instance_dir>/schedules.pending` and signing its
SHA-256 digest into admin op 6, leader-only and single-in-flight, with four
named refusals and an audit record; adoption through the archive's header
walk exactly as CONFIG takes, persisted in `state/schedules.state` with one
level of `prev` to revert to on truncation and re-armed at boot from the log
clock; table ticks firing as `TIMER` frames with `FLAG_TIMER_TABLE`, advanced
at append on the leader and on `TableConsumed` on a follower, with one-tick
catch-up at fire time; `Timed`'s `table_last` dedup; three metric families,
two records and the `Uc2ScheduleTableDiverged` alert. Explainer section:
`docs/notes/uc2-log-time-and-timers-explained.md#the-schedule-table`; gate row
e in `docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md`.

Three execution rulings amended spec §5 (recorded there as as-built errata):
the one-tick catch-up moved from arm time to fire time; single-in-flight apply
plus `ScheduleRecord.prev` and revert-on-truncation; and `once` as a third
rule kind that parks rather than leaving the table.

**Plan 3, the schedule table on the snapshot session, is IMPLEMENTED too** —
same branch, same unreleased `2.11.0` flag day. Plan:
`docs/superpowers/plans/2026-09-03-uc2-schedule-table-in-snapshot.md` (T0–T5,
all done), against the same spec §5's as-built errata. What shipped:
`DGRAM_KIND_SNAP_TABLE = 21` with a total, fuzzed codec (body `session ‖
position ‖ time_ns ‖ table_len ‖ table`, ≤ 1086 B, `position == 0` iff the
table is empty); the leader sending it after **every** `SNAP_BEGIN` of a
session, gated on its own commit counter; a receiver that withholds
`SNAP_DONE` until the table arrives, drops strays
(`uc2_snapshot_table_stray_total`, latched per episode) and publishes
table → config → floor; a fiat install (`Consensus::install_snapshot_table`,
`prev: None`) that runs before the floor advances; `schedule_table_adopted`
gaining a `source` field and `snapshot_installed` a `table_position`. It
closes the first bullet below and adds the two after it.

**One execution ruling, R7 (2026-09-03), amends the ship gate.** Task 5's
writeup review found that `shippable_schedule` passed a position-`0` record
through with its body, which the wire's frozen `(position == 0)` ⇔
`(table_len == 0)` rule refuses — and because a session completes only once
its table arrives, that would have STALLED the joiner on every re-send rather
than failing loudly. Fixed in `c87fd4a`: a record at position `0`, or one
whose bytes will not decode or decode to no entries, ships as `(0, 0, [])`.
The deliberate consequence is the third bullet below — a wiped node's kept
table is local-only. The R7 vectors ride the existing
`the_snapshot_session_ships_only_a_committed_schedule_table`.

**What is left under this feature** — each documented in
`docs/reference/limits.md` and the explainer's "Known limits of the table",
none a blocker, none scheduled:

- ~~**The table is not carried in the snapshot stream.**~~ — CLOSED
  2026-09-03 by plan 3,
  `docs/superpowers/plans/2026-09-03-uc2-schedule-table-in-snapshot.md`
  (spec §5's as-built errata). The judgement that closing it needed a wire
  change was right; what changed is that `2.11.0` was already an unreleased
  flag day, so the change cost nothing extra. It is a new datagram kind
  rather than a `SNAP_BEGIN` field: `SNAP_TABLE` (21, ≤ 1086 B) sent after
  **every** `SNAP_BEGIN` of a session, withheld `SNAP_DONE` until it arrives,
  installed by fiat before the floor advances — so a below-floor joiner holds
  the cluster's table before it can serve a read or win an election.
- **A restarted node under-ships the table for one window** (plan 3 residual
  a). The cnc commit counter is not primed at boot, so `shippable_schedule`'s
  commit gate cannot yet clear the node's own record: it offers the one-level
  `prev`, or nothing, until the first commit advance. That is the safe
  direction — no uncommitted table is ever handed on — but a joiner served
  inside that window can end up with an older table, or none until the next
  `uc2ctl schedule apply`, if the frame is below the shipper's own floor.
  Priming the counter at boot, or seeding the gate from the durable record,
  would close it.
- **A wiped node's kept table does not propagate by snapshot** (plan 3
  residual b, in its post-R7 form). A node whose newest shippable record sits
  at position `0` ships `(0, 0, [])`, so a joiner it serves installs **no
  table** and learns the real one from the next table frame or the next
  `uc2ctl schedule apply`. Two records have that shape: the `to == 0` wipe
  record (`revert_schedule_below` keeps the table body at position 0 so a
  wiped node keeps ticking) and the canonical no-table record
  (`ScheduleRecord::empty`, whose bytes are an 8-byte encoded *empty* table
  rather than zero bytes); `shippable_schedule` maps both, plus any record
  whose bytes will not decode or decode to no entries, onto "no table".
  This is deliberate, not a gap: position `0` means the table is **unanchored
  in the log**, so the wipe keep-alive is a local fiat that keeps one node
  ticking until the next frame, not a cluster fact a joiner should record — a
  joiner given it would hold a table no position backs, which is the
  divergence `Uc2ScheduleTableDiverged` exists to catch. Closing it properly
  means a wiped node re-anchoring its own table, which is a re-apply, not a
  ship-seam change.
- **One crash window loses one adoption.** A node that dies between the
  archive recording a table frame and the consensus agent persisting
  `state/schedules.state` comes back without it: there is no journal re-scan
  for type-6 frames on the recovery path. Sub-millisecond, same symptom, same
  remedy (re-apply). A recovery-path scan would close it.
- **A restart may re-append one tick per entry.** Boot arming has no delivered
  set until the service attaches and announces its `table_last`, so a
  restarted node may append the latest occurrence of every entry once — a
  parked `once` included. `Timed` drops it; a state machine without the
  wrapper sees it, which is the at-least-once trade it already accepted.
- **No timezones and no cron syntax.** `at` is UTC. "02:00 local, with DST"
  is not expressible — a timezone database is replicated state that must agree
  on every node and across every upgrade. Cron-style rules are a possible
  fourth `kind` byte; the codec has room.
- ~~**Two alert rules have no `m10_alert_fire.sh` builder**~~ — CLOSED in the
  final fix wave (2026-09-03). `Uc2LogTimeFrozen` and
  `Uc2ScheduleTableDiverged` now have builders backed by two new
  `m10_alerts` scenarios (`log_time_frozen`, `schedule_diverged`), both
  synthetic-state / real-exporter in `identity_drift`'s shape and disclosed
  by name, so the M10 gate's row 4 can be re-run as written.
- **`append_schedule_table` duplicates `append_config`'s body deliberately**
  (ruling R8). `uc_log::Appender` now carries four specialised append bodies;
  the shared writer that would collapse them touches the hot `append` path,
  and M14a's inline-ladder lesson is that code added to a hot loop's body
  costs even on the arms that never execute (9 % at N=1, from codegen alone).
  So the follow-up is "extract it **with an A/B** against `apply_bench` and
  the client hop, on rebuilt-same-source controls", not "deduplicate".
- **`node.toml [schedules]` as a boot-time convenience** stays the door spec
  §10 left open: a per-host section that simply calls the admin op at startup.
  Deliberately not the primary form — it turns a schedule edit into a rolling
  edit plus a leader change.

### 3. Rolling upgrades and leadership transfer

The two operations items `docs/superpowers/specs/2026-08-19-uc2-production-readiness-design.md`
deferred by name:

- **Version negotiation / upgrade window.** Today every node↔node wire or
  `cnc.dat` change is a cluster-wide flag day
  (`docs/how-to/upgrade-a-cluster.md`, `docs/reference/semver-policy.md`).
  The spec's reason for deferring: a negotiated floor becomes
  consensus-relevant state, which is real design work, not a script.
- **Leadership transfer.** A planned leader stop costs one election
  timeout. Needs a new protocol message (a Raft `TimeoutNow` analog); the
  spec calls it "a consensus change wearing an operations hat; gets its own
  spec or none at all."
- **Crypto-on-by-default** was parked "revisit at M12, not before" in the
  same spec and never revisited; it belongs in this milestone.
- **FSM version — the static half shipped with FSM identity (implemented on
  `uc2/fsm-identity`, release on hold); see spec §7**
  (`docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md` §7):
  `const VERSION` per FSM in Aeron's packed-semver layout, attach-written to
  the cnc slot, exported, carried per row on `SNAP_BEGIN` and
  equality-checked. What is left for THIS item, with Aeron as the
  comparator (read from source): the log-stamped half — the leader writes
  the version into a term-boundary log event and snapshot markers, every
  module and service validates its static value against it through a
  pluggable validator (Aeron's default: major-equality), fail-stop — and
  the rolling-upgrade semantics that follow. The carrier is a term-boundary
  log event, not `SNAP_BEGIN`.
- **Why:** the flag-day rule is the limit an operator hits first. This is
  the gap between "deployable" and "operable at scale".
- **Cost:** high — both items touch consensus and are a wire flag day
  themselves.

### 4. Geo — async cross-region learner with a stale-read mode

`docs/notes/uc2-m7-vs-aeron-cluster-standby-2026-07-24.md` found that a
UC learner is already most of an Aeron Cluster Standby, and sketched a
phased shape: (prereq) wire crypto → Phase A stale-read query mode off a
learner → Phase B learner-as-relay → Phase C DR failover as a *separately
scoped* consistency weakening.

- **Status of the prerequisite:** met — wire crypto shipped in M8
  (`v2.3.0`).
- **Why:** the largest capability gap against the stated comparator; Phase A
  is mostly additive and low-risk per the note.
- **Cost:** moderate for Phase A; Phase C is a product decision before it
  is code.

### 5. Verification debt

`docs/VERIFICATION.md` §11 and `docs/superpowers/specs/2026-08-01-uc2-formal-roadmap.md`
record what is not proved:

- **`leader_completeness`** — the roadmap's HIGHEST-priority task (F-UC-1,
  ≈ 7–12 S2-equivalents), not started. The joint-induction blueprint is in
  the phase-2 memo; the sole named open theorem in the corpus.
- **The Lean model collapses the durable counter's two readers into one**
  (issue #7). A real acked-write-loss bug lived in exactly that gap and was
  found from the Rust side. Proofs composed over that lemma are weaker than
  they look until the split lands.
- **SPSC and the futex layer have no loom model**; MPSC and Broadcast do,
  and the Broadcast model found a real weak-memory defect the day it was
  written (2026-08-31). The mmap itself is outside loom and Miri; a
  Vec-backed variant for Miri "has not been built, and that trade-off is
  recorded rather than resolved".
- **aarch64 tests in CI.** Binaries are built, tests never run; the full
  stack has passed on Graviton exactly once
  (`docs/benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md`). A one-time
  pass is a data point, not a regression gate.
- **Term-map follow-ons** from
  `docs/notes/uc2-term-map-window-loss-explained.md`: commit-floor anchoring
  of the wire window, election credential floors for wiped nodes, a
  persisted commit watermark (the truncation-below-commit defence forgets
  state across reboot).
- **Why:** every proof gate so far found exactly one real bug; this is the
  direction most likely to find the next one.

### 6. Performance, round three

All framed by the docs as characterisation, not defects:

- **One remote connection on Graviton is 0.498× direct against a 0.5×
  bar** — `docs/benchmarks/uc2-m13-remote-on-arm-2026-09-01.md` ("FAIL by
  0.2 %", on c6id-era bars; the c6id gate itself remains PASS).
- **M14 gate row e has never been re-measured**, and the pinned rig's
  residual 14.3 % spread is undiagnosed
  (`docs/benchmarks/uc2-m14c2-fleet-pinning-2026-08-30.md`,
  `docs/VERIFICATION.md` §11).
- Recorded follow-ons: sharded per-client ingress and demand-weighted
  credits (`docs/superpowers/specs/2026-08-24-uc2-m13-remote-path-design.md`
  §8 "Follow-ons (not M13)"), service-side raw passthrough and per-slot response-buffer
  reuse (`docs/superpowers/specs/2026-08-13-uc2-pipelined-client-design.md`
  §10 "Out of scope / deferred"), the Rung B time-based leader lease
  (`docs/superpowers/specs/2026-07-24-uc2-leader-lease-design.md`,
  discharged for the LAN goal; only for WAN reads).
- **Why not first:** no user is asking for more than the current ceiling;
  every number here is a fleet characterisation with its caveats disclosed.

### 7. External review

`docs/security/self-assessment.md` §4 "What an external review should focus on" ranks seven
areas for outside eyes, led by the pre-auth UDP dispatch with crypto OFF and
the `snow` handshake state machine under interleaved malformed messages.
Cheap relative to the surface it covers; pairs with direction 1, since a
reviewer wants a workload to attack.

## Small items, worth doing regardless of direction

- **Release-mode bounds guard in `read_frame_validated`**
  (`uc_log/src/buffer.rs`): the check ahead of the `unsafe` slice read is
  `debug_assert!` only. Deferred in the M8 release notes as pre-existing
  code (`docs/releases.md`, v2.3.0 "Deferred / follow-up"). The code's own
  safety analysis says it is reachable only via a corrupted commit word or a
  mid-frame position, so this is hygiene, not a live defect — and a one-line
  check.
- **`nightly.yml` has never run on the `v2.10.0` tag commit**
  (`docs/releases.md`, release-evidence table).
- **`uc2-gateway --version`** — fixed on `main` after the tag, lands in the
  next release (`docs/releases.md`).
- **Leader self-send `seal_failures` wart** — the encrypted leader's
  self-addressed position report fails to seal and counts; harmless,
  suppression deferred since M8 (`docs/releases.md`, v2.3.0).
- **Minter-local epoch collision** after leader change — transient DATA
  loss, NAK-repaired, "a nice-to-have, not a safety break"
  (`docs/notes/uc2-m8-formal-methods-followups.md`).

## Accepted residuals — listed so they are not re-proposed

These are decisions with a reason, not forgotten work. Do not reopen one
without a new argument:

- The four wire-crypto residuals (cleartext headers; a removed node keeps
  decryption until the next rotation; any group-key holder can forge fan-out
  traffic; no compromised-host story) —
  `docs/superpowers/specs/2026-07-28-uc2-wire-crypto-design.md` §7,
  `docs/reference/limits.md`.
- Admin HMAC is cluster-wide only with `[crypto].enabled = true` (the
  kind-16 forward plane) — closing it is a wire change
  (`docs/notes/uc2-admin-authentication.md`).
- The typed tier's pre-commit query decode fail-stops on a malformed frame —
  documented, not changed, in M12d (`docs/security/self-assessment.md` §3).
- `bincode` unmaintained (RUSTSEC-2025-0141) — no patched version exists;
  one documented `deny.toml` ignore.
- Twelve-factor #6 (stateless processes) is opposed by design; #5's release
  ledger and #8's "simple" horizontal scale are partial by the nature of a
  consensus system (`docs/notes/uc2-twelve-factor-assessment.md`).
- `--pin` stays opt-in: pinned spread 14.3 % against a < 5 % bar, and it
  costs 9.4 % of mean throughput
  (`docs/benchmarks/uc2-m14c2-fleet-pinning-2026-08-30.md`).
- Lockstep-mode collapse under CPU oversubscription is an operating-envelope
  fact, not a defect
  (`docs/benchmarks/uc2-m14c2-lockstep-oversubscription-2026-08-30.md`,
  `docs/reference/limits.md`).
