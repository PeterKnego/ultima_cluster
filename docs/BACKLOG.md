# Backlog — candidate directions after M14

## Bound the three unbounded waits in `remote_lin.rs`

**Added 2026-09-08**, from the fleet-gate follow-up (see `docs/releases.md`,
"Known issue at release"). The worker `join()`, the chaos `join()` and
`Reap::drop`'s `kill(); wait()` in `examples/uc_crashtest/tests/remote_lin.rs`
are the only waits in that test body without a deadline, and so the only places
a 60-minute stall can live. They are why the 2026-09-08 nightly spent its whole
budget and was cancelled instead of failing fast — that run took every other
nightly job's evidence down with it. Bounding them is a **diagnosability** fix:
the next occurrence fails with a location instead of a cancellation.

This was the second half of the `CncPage::meta()` item; the first half is
fixed — see the [Shipped](#shipped-since-this-list-was-written) section's
first entry. Fixing the panic does **not** explain the hang, and was never claimed
to — that is exactly why this half stays open.

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

### 2. Schedule-table and timer follow-ons

See [Shipped](#shipped-since-this-list-was-written) below.

*What is left of items 2 and 2a after both shipped in `2.11.0`. Every bullet
is a KNOWN, documented residual, not a defect: each is stated in
`docs/reference/limits.md` and reachable by a user, and each has a remedy or a
reason it is deliberate. Kept as a numbered item so it stays a candidate
direction rather than a footnote to a finished one. The references that used
to point at "§ 2a" for these residuals now point here.*

- ~~**A restarted node under-ships the table for one window**~~ (plan 3
  residual a) — **CLOSED by the cluster FSM (2.11 pending), plan 1.** The
  cause was structural: the table was shipped by a **live read** of the
  shipping node's memory, gated on the cnc commit counter, which is
  deliberately not primed at boot. Both the read and the gate are gone. The
  table is a record inside the internal cluster FSM, and the snapshot session
  carries that FSM's own **artifact** (`service_id = 255`), which is committed
  by construction and durable across a restart. Pinned by
  `uc_node/tests/learner.rs::a_joiner_served_by_a_leader_restarted_before_its_first_commit_advance_still_installs_the_table`.
  → [The cluster FSM, explained](notes/uc2-cluster-fsm-explained.md)
- ~~**A wiped node's kept table does not propagate by snapshot**~~ (plan 3
  residual b, post-R7) — **CLOSED by the same change.** There is no wipe
  keep-alive to propagate: `ScheduleRecord` (retired) and
  `shippable_schedule` (retired) are gone — as is `revert_schedule_below` —
  and the position-0 encoding rule they needed went with them. The table is FSM state applied at **commit**, so an
  uncommitted frame is never applied and a truncated one never existed as far
  as the FSM is concerned — there is nothing to revert and nothing to keep
  alive at an unanchored position.
  → [The cluster FSM, explained](notes/uc2-cluster-fsm-explained.md)
- ~~**One crash window loses one adoption**~~ — **CLOSED by the same change.**
  There is no `state/schedules.state` to crash between recording and
  persisting; the `uc2-cluster` agent recovers by replaying the journal above
  its artifact, exactly as every other FSM does.
- **A promotion may re-append one tick per entry.** Still open, in a slightly
  different shape: the timer heap is leader-only since plan 1, so a newly
  promoted leader arms the table from the cluster FSM's view and has no
  delivered set until its service announces its `table_last`. It may append
  the latest occurrence of every entry once — a parked `once` included.
  `Timed` drops it; a state machine without the wrapper sees it, which is the
  at-least-once trade it already accepted.
- **No timezones and no cron syntax.** `at` is UTC. "02:00 local, with DST"
  is not expressible — a timezone database is replicated state that must agree
  on every node and across every upgrade. Cron-style rules are a possible
  fourth `kind` byte; the codec has room.
- ~~**`append_schedule_table` (retired) duplicated `append_config`'s body**~~ (ruling
  R8) — **moot**: the cluster FSM collapsed both into one `append_cluster`
  with a kind byte, because there is one frame type now. The underlying rule
  stands for any future append body: extract **with an A/B** against
  `apply_bench` and the client hop, on rebuilt-same-source controls, never by
  inspection — M14a's inline-ladder lesson is that code added to a hot loop's
  body costs even on the arms that never execute (9 % at N=1, from codegen
  alone).
- **`node.toml [schedules]` as a boot-time convenience** stays the door spec
  §10 left open: a per-host section that simply calls the admin op at startup.
  Deliberately not the primary form — it turns a schedule edit into a rolling
  edit plus a leader change.
- **New in the cluster-FSM shape, and worth a line each.**
  `uc2ctl schedule show` / `settings show` / `status`'s `schedule_position=`
  read the newest **cluster artifact**, a file beside the running node, so
  they lag the live view and say "no cluster artifact yet" until the first
  snapshot instant completes; a live reading needs the
  response-on-the-egress-broadcast path the design left to a phase 2. (Three
  other residuals listed here were closed by plan 1's pre-final pass:
  `uc2ctl backup` now carries `snapshots/cluster/`;
  `uc2_cluster_fsm_position` and `uc2_settings_position` are exported; and
  `uc2_agent_alive` carries the fifth agent.)
- **New with coordinated snapshots (plan 2), and deliberate.**
  A voter's purge floor waits for an operator after a `--standby` instant:
  **automatic standby replication** — a voter pulling a learner's set without
  `uc2ctl snapshot fetch` — needs the learner's "complete at P" to be visible
  cluster-wide, which is a fourth cluster-FSM command and a design of its own.
  Aeron's open-source half defers it the same way. Two smaller ones beside it:
  the redirect's **sending** side emits no log record (it leaves `uc_net`,
  which carries no logging dependency by design — its witness is the leader's
  `snap_redirects` counter), and the two new session-refusal counters
  (`uc2_snapshot_refused_position_total`,
  `uc2_snapshot_refused_fetch_expired_total`) are rendered but not yet listed
  in `CONTRACT_SERIES`.
- **Why:** none of these blocks anyone today, which is exactly why the list is
  worth keeping — a residual nobody wrote down becomes a surprise. The three
  struck-through entries above are the ones a user actually met by accident,
  and they are closed; what is left is conveniences and observability.
- **Cost:** low, and what remains is two items: a live reading for
  `schedule show` / `settings show` / `status`, which needs the
  response-on-the-egress-broadcast path (spec §13 phase 2), and automatic
  standby replication, which needs a fourth `CLUSTER` kind (spec §13).

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
- **Snapshot-session probe counters are sender-local** — `snap_redirects`
  and `snap_request_unknown_peer` (the member gate on `SNAP_REQUEST`, plan 2)
  live in `SenderStats` with no `/metrics` family, `Node::` accessor or obs
  record, so an operator cannot see a peer probing for sets. One gauge each,
  read at scrape; found by plan 2's final re-review.
- **Leader self-send `seal_failures` wart** — the encrypted leader's
  self-addressed position report fails to seal and counts; harmless,
  suppression deferred since M8 (`docs/releases.md`, v2.3.0).
- **Minter-local epoch collision** after leader change — transient DATA
  loss, NAK-repaired, "a nice-to-have, not a safety break"
  (`docs/notes/uc2-m8-formal-methods-followups.md`).
- **Alert on `uc2_log_clock_smear_ns`** (a smear above N seconds for M
  minutes), with its `scripts/m10_alert_fire.sh` builder and scenario —
  recorded 2026-09-08 by the log-clock spec's errata bullet 9.

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


## Shipped since this list was written

Kept per this page's preamble — an item that is taken up gets its line updated
rather than deleted, so the reasoning stays re-checkable. The last two are in
`2.11.0` and the first in `2.12.0`; what remains open from them is item 2
above, plus the `remote_lin.rs` waits at the top of this page.

### `CncPage::meta()` must not panic on a page a live writer re-initialised — FIXED for `2.12.0`

Taken up and fixed 2026-09-09, after `2.11.0` shipped it as a recorded known
issue. `meta()` `.expect()`ed a valid header on an mmap another process owns
and re-initialises on restart, so `uc2ctl`, a client attach, a service attach
or the gateway could PANIC instead of erroring. `open_file` validates at open
and documents the right posture ("a typed error, never a panic"), but a shared
mapping changes under the reader afterwards, so open-time validation cannot
close it.

Fixed by **replacing** `meta()` with `try_meta() -> Option<CncMeta>` rather
than adding a twin beside it — permitted in a minor because
[the semver policy](reference/semver-policy.md) lists `uc_log`'s `cnc` module
as not promised, and preferred because a surviving `meta()` is a loaded call
the next production caller can reach for. Six call sites, not the five this
list named: `uc_gateway/src/edge.rs` reads `max_payload` the same way and was
missed. The three attach doors answer `None` with `CncError::BadHeader`;
`uc2ctl` refuses admin signing by name and skips its leftover-page
cross-check. Pinned by a test that forces the torn window deterministically
instead of waiting on the ~1-in-6 crashtest race.

Record: `docs/releases.md`, "Known issue at release" → its **FIXED after the
tag** paragraph.

### FSM identity — name the state machine, not the slot (was item 2)

Taken up 2026-09-01, IMPLEMENTED 2026-09-02, merged to `main` 2026-09-04.
Identity lives **in code** — a required `const NAME` plus an optional
`const VERSION` on the state-machine trait — and the row keeps its
cluster-wide meaning while a service finds it by name. `SNAP_BEGIN` 0.7.0
carries hashes and versions per row, compared positionally and refused by
name; cnc 3.1; `ApplyCtx` replaced the bare `position` apply parameter;
`IdGen` gives deterministic ids. The placement-independent variant was cut by
the spec's §2.1 comparison table.

Spec `docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md` · plan
`docs/superpowers/plans/2026-09-02-uc2-fsm-identity.md` (T0–T10) · explainer
[`docs/notes/uc2-fsm-identity-and-deterministic-ids-explained.md`](notes/uc2-fsm-identity-and-deterministic-ids-explained.md)
· how-to [Schedule work inside a state machine](how-to/schedule-work-in-a-service.md)
· gate `docs/benchmarks/uc2-fsm-identity-gate-2026-09-02.md` (bars committed,
**rows not yet run**).

### Time and timers, and the replicated schedule table (was item 2a)

Requested by the maintainer directly on 2026-09-02 — never a ranked item on
the 2026-09-01 list — and specced beside FSM identity, which is why it sat
under item 2. **All three plans are implemented** and merged to `main`:
leader-stamped log time plus a deterministic scheduler (plan 1); the
replicated schedule table, the since-retired `FRAME_TYPE_SCHEDULE_TABLE = 6` and
`uc2ctl schedule apply/show` (plan 2); and that table on the snapshot session,
`SNAP_TABLE` datagram kind 21, so a below-floor joiner installs it before it
can serve or lead (plan 3).

**And superseded again by coordinated snapshot instants** (the same spec's
plan 2, done on the `uc2+coordinated-snapshot-plan2` branch): the cluster
artifact plan 1 wrote on a bridging trigger is now written at a commanded
instant, so a "set" is one log position rather than a lowest-common-floor, and
the now-retired `SnapshotPolicy`'s per-service byte interval is deleted with it. Standby
instants (`uc2ctl snapshot --standby`) and the `uc2ctl snapshot fetch` return
path come with it —
[`docs/notes/uc2-cluster-fsm-explained.md` § Instants](notes/uc2-cluster-fsm-explained.md#instants-one-position-one-set).

**Superseded before shipping, in the same unreleased `2.11.0`**: the cluster
FSM took over both carries; the retired `FRAME_TYPE_SCHEDULE_TABLE = 6` and
`SNAP_TABLE` kind 21 are both **retired** here. The frame becomes
`CLUSTER kind = 2` and the table rides
the cluster FSM's own snapshot artifact under `service_id = 255` instead, which
is what actually closed the limitation plan 2 shipped with (plan 3's own two
ship-side residuals went with it; see the struck bullets under item 2 above).
Both numbers are reserved so they are never reassigned. Read plans 2 and 3 as
the reasoning that motivated the cluster FSM, not as what ships:
[`docs/notes/uc2-cluster-fsm-explained.md`](notes/uc2-cluster-fsm-explained.md).

One item recorded here as open has since **closed**: `Uc2LogTimeFrozen` and
`Uc2ScheduleTableDiverged` gained `m10_alert_fire.sh` builders in the final
fix wave (2026-09-03), backed by the `log_time_frozen` and `schedule_diverged`
scenarios, so the M10 gate's row 4 can be re-run as written.

Specs `docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md` ·
plans `2026-09-03-uc2-time-and-timers-plan{1,2}.md` and
`2026-09-03-uc2-schedule-table-in-snapshot.md` · explainer
[`docs/notes/uc2-log-time-and-timers-explained.md`](notes/uc2-log-time-and-timers-explained.md)
· how-tos [Schedule work inside a state machine](how-to/schedule-work-in-a-service.md)
and [Run work on a schedule](how-to/run-work-on-a-schedule.md) · gate
`docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md` (bars committed,
**rows not yet run**; row d has no runner).

