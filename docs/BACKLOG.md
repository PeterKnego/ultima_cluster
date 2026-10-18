# Backlog — candidate directions after M14

*Written 2026-09-01 against `v2.10.0`. Status: a ranked list of options, not
a plan. Nothing here is scheduled; the maintainer picks. Every item cites the
document that first recorded it, so the reasoning can be re-checked rather
than re-derived. When an item is taken up it gets a spec under
`docs/superpowers/specs/` and a gate doc under `docs/benchmarks/`, and its
line here is updated to point at them. An item that ships, or stops
being relevant, is removed (last purge 2026-10-15): `RELEASES.md`,
`docs/releases.md` and git keep the record.*

## Where this list comes from

M8–M14 turned the v2 engine into a deployable product (`RELEASES.md`). What
the record does not contain is an outside user: the in-tree services are the
`examples/counter` teaching crate and the `examples/kv` dogfood store,
`docs/reference/remote-protocol.md` still describes itself as "the page a
non-Rust port implements from", and no port exists.
Every remaining gap the docs record is either an accepted residual
(`docs/reference/limits.md`, `docs/security/self-assessment.md`) or a
deferral waiting for a reason to matter. The ranking below follows from
that: the first direction is the one that supplies the reason.

## Ranked directions

### 1. Dogfood — a real service plus a second-language client

The in-tree reference service and the two-track experience assessment are
done (`examples/kv`, the wayfinder map
[#16](https://github.com/PeterKnego/ultima_cluster/issues/16), closed
2026-09-18), and jumbo-frame discovery (`2.12.0`) answered the payload-ceiling
question. What remains open:

- **A remote client in a second language** (Go or Python), written from
  `docs/reference/remote-protocol.md` alone, as that page invites — the
  wayfinder map scoped it out.
- **The remote protocol's FSM selector and ceiling.** `SUBMIT`/`QUERY` still
  carry no FSM selector, so a remote client reaches only FSM 0
  (`docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md` §11 "Out
  of scope"), and the protocol advertises no payload ceiling, so a `uc_remote`
  client warns on the standard 1312 B threshold alone; the read-your-writes
  protocol v2 added neither.

### 2. Schedule-table and timer follow-ons

*What is left of items 2 and 2a after both shipped in `2.11.0`. Every bullet
is a KNOWN, documented residual, not a defect: each is stated in
`docs/reference/limits.md` and reachable by a user, and each has a remedy or a
reason it is deliberate. Kept as a numbered item so it stays a candidate
direction rather than a footnote to a finished one. The references that used
to point at "§ 2a" for these residuals now point here.*

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
- **`node.toml [schedules]` as a boot-time convenience** stays the door spec
  §10 left open: a per-host section that simply calls the admin op at startup.
  Deliberately not the primary form — it turns a schedule edit into a rolling
  edit plus a leader change.
- **New in the cluster-FSM shape, and worth a line each.**
  `uc2ctl status`'s `schedule_position=` still reads the newest **cluster
  artifact**, a file beside the running node, so it lags the live view and
  reads `none` until the first snapshot instant completes (`schedule show`,
  `settings show` and `upgrade show` read the live cluster view since
  project 4).
- **New with coordinated snapshots (plan 2), and deliberate.** The
  redirect's **sending** side emits no log record (it leaves `uc_net`,
  which carries no logging dependency by design — its witness is the leader's
  `snap_redirects` counter).
- **Why:** none of these blocks anyone today, which is exactly why the list is
  worth keeping — a residual nobody wrote down becomes a surprise.
- **Cost:** low, and what remains is one item: a live reading for `uc2ctl
  status`'s `schedule_position=`. (Automatic standby replication shipped with
  the snapshot lifecycle's `auto_fetch`: every node fetches each new agreed
  set in the background — [Learner-only
  clusters](how-to/bound-journal-growth.md#learner-only-clusters-voters-fetch-and-purge-by-default).)

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
- **FSM version** — the static half shipped with FSM identity (`2.11.0`,
  `docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md` §7), and the
  committed per-row running version with the pin (`2.13.0` and
  [#33](https://github.com/PeterKnego/ultima_cluster/issues/33)). What is left
  is the rolling-upgrade semantics: a committed per-row level, so two versions
  of one row can apply side by side on purpose —
  [#66](https://github.com/PeterKnego/ultima_cluster/issues/66).
- **Why:** the flag-day rule is the limit an operator hits first. This is
  the gap between "deployable" and "operable at scale".
- **Cost:** high — both items touch consensus and are a wire flag day
  themselves.
- **Researched 2026-09-13/14, tracked as
  [#31](https://github.com/PeterKnego/ultima_cluster/issues/31)** — the
  version-negotiation item only. `docs/notes/uc2-rolling-upgrade-compatibility-assessment.md`
  inventories every compatibility surface and recommends a committed,
  monotone cluster **wire level** in the Settings record (the jumbo-rung
  shape; the kernel and Lean model untouched), with the next flag day
  reserving the carriers so it is the last one of the 0.x line for additive
  changes. `docs/notes/uc2-aeron-cluster-upgrade-model.md` is the
  comparator: Aeron Cluster has no supported rolling upgrade and no
  committed level. Leadership transfer is not covered by either note.

### 4. Geo — async cross-region learner with a stale-read mode

`docs/notes/uc2-m7-vs-aeron-cluster-standby-2026-07-24.md` found that a
UC learner is already most of an Aeron Cluster Standby, and sketched a
phased shape: (prereq) wire crypto → Phase A stale-read query mode off a
learner → Phase B learner-as-relay → Phase C DR failover as a *separately
scoped* consistency weakening.

- **Status of the prerequisite:** met — wire crypto shipped in M8
  (`v2.3.0`).
- **Status of Phase A:** mostly done. A learner answers snapshot reads (the
  query path never checked the node's role) and, since `2.14.0`,
  read-your-writes reads. What remains of Phase A is a staleness bound on a
  learner's answer; Phases B and C are untouched
  ([UC against Aeron Premium](notes/uc2-vs-aeron-premium-2026-09-19.md) §1).
- **Why:** Standby is the comparator's one architectural premium component;
  relay and warm DR are what is left of the gap.
- **Cost:** low for the staleness bound, moderate for Phase B; Phase C is a
  product decision before it is code.

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
- **The service-time record (2026-09-16,
  `docs/benchmarks/uc2-service-time-2026-09-16.md`)** measured the commit
  path's own round trip for the first time: shipped p50 123 µs on
  c6id.2xlarge, of which the apply agent's 50 µs idle sleep is a bimodal
  second mode (77 µs at p90, removed by `UC2_APPLY_IDLE=spin`), thread
  placement 41 µs of mean, fsync 27 µs, and the wire at most 33.5 µs.
  **Candidate that follows from it:** a
  paced open-loop mode for `m12_gate client-direct` so "p99 at N msg/s
  offered" can be stated in the same terms Adaptive's cluster benchmarks
  use. Kernel bypass and the net-decomp brief's WIRE(P) instrument are
  declined by the same record (the whole round trip is 33.5 µs).
- **Time-based hot-path latency, as an option (recorded 2026-10-18).**
  Every hot-path reading on `/metrics` is a position or a counter — the two
  lag gauges are `append − commit` and `commit − applied` in bytes — and the
  only duration histograms are timer lateness and the snapshot freeze. No
  node measures submit→commit, commit→apply or a single frame's apply in
  time, and the gateway measures no remote round trip. The frame header's
  leader stamp (`time_ns`, 2.11.0) is the natural source: the apply loop
  could record `now − hdr.time_ns` per frame, and the archive agent a
  per-fsync duration, into a fixed-bucket histogram. **The cost is the
  decision**: a clock read and a bucket increment in the apply body is
  exactly the shape the 2.11.0 regression taught us to A/B on `apply_bench`
  before trusting (`CLAUDE.md` § Finding a performance bottleneck), and
  sampling (every Nth frame, or once per pass on the pass's last frame)
  is the likely compromise. The cheap, counter-only parts are filed
  separately and do not depend on this decision: a frame count
  ([#171](https://github.com/PeterKnego/ultima_cluster/issues/171)), archive
  counters plus an `append − durable` gauge
  ([#172](https://github.com/PeterKnego/ultima_cluster/issues/172)), and a
  `/metrics` on `uc2-gateway`
  ([#173](https://github.com/PeterKnego/ultima_cluster/issues/173)). A
  service that wants per-command latency today can compute it in its own
  `apply` from `ctx.time_ns` and its own clock, at its own cost.
- **Why not first:** no user is asking for more than the current ceiling;
  every number here is a fleet characterisation with its caveats disclosed.
- **Inspect `ABTRDA3` (recorded 2026-09-16)** —
  <https://github.com/ASherjil/ABTRDA3>, a C++20 kernel-bypass L2 library
  with one four-call ring API (`acquire`/`commit`, `tryReceive`/`release`)
  over five interchangeable backends: Solarflare `ef_vi` (CTPIO), DPDK,
  mlx5 verbs (`RAW_PACKET` QP), AF_XDP (zero-copy + busy-poll) and
  `PACKET_MMAP`. The transport is a template parameter chosen from TOML, so
  the same reflector loop is measured on every backend. Its
  `docs/Benchmarks.md` is eleven 24 h one-frame-in-flight RTT soaks on one
  isolated-core i9-11900K rig (`rdtscp`, HdrHistogram, ports DAC-looped, no
  switch): `ef_vi` 1.866 µs median / 3.306 µs max over 44.3 B samples;
  verbs 2.426 µs and DPDK 3.5–3.6 µs on ConnectX-4 Lx and X2522; AF_XDP
  6.2 µs on mlx5 and 10.5 µs on i40e (DPDK on i40e sits at 9.0 µs, the
  silicon floor); a consumer I225-V at 13.4 µs with the flattest tail in
  the campaign. Its `docs/Known_driver_issues.md` is a symptom → mechanism
  → evidence catalogue (stock `i40e` silently no-ops AF_XDP busy-poll; a
  gapless `SCHED_FIFO` poll strands RX under NAPI deferral; `igc` cannot
  ping-pong AF_XDP at all; the 2.5GBASE-T PHY costs more than 1 GbE). What
  to take from it for UC: (a) the measurement discipline — 24 h soaks,
  P99.999 as the headline, warm-up discarded, IRQ steering proven rather
  than assumed — against our own fleet-gate method; (b) whether an AF_XDP or
  DPDK path under `uc_net`'s reliable-UDP could shave the per-hop wire cost
  the fleet gates pay through kernel sockets, and what fraction of UC's
  commit latency that is; (c) the driver-defect list as a checklist before
  any bypass experiment on the c6id fleet (ENA has none of these NICs, so
  the numbers do not transfer — the method does). Isolated transport
  ladders belong in the sibling `hi-perf-cmp` grid; only a UC-budget-share
  measurement belongs here.

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
- **Auto-fetch can re-download an agreed set that is never noted held.** An
  operator `uc2ctl snapshot fetch` with no position can store a newer,
  not-yet-agreed set; a later auto-fetch of the newest agreed set below it
  then lands without moving `stored_set_pos` (`fetch_max`), times out after
  60 s and retries on the backoff ladder. It stops once the newer set agrees
  (normally within ~5 s) and persists only if that set diverged, which
  `Uc2SnapshotSetDiverged` already alerts. Snapshot lifecycle final review
  M3, 2026-10-10 (the Task 9 ledger had called it unreachable).
- **Project 3: the backup tier — one edge left open.** 3a and 3b are merged
  (PRs #100 and #107, not yet released;
  [how-to](how-to/back-up-to-a-tier.md),
  [3a spec](superpowers/specs/2026-10-10-uc2-backup-tier-design.md),
  [3b spec](superpowers/specs/2026-10-12-uc2-backup-tier-3b-design.md)).
  - **3b: retention's view gate, the origin-set edge** (ruling R10a, deferred
    by the 3a final review, 2026-10-11). Retention runs only once the
    writer's view has consumed past the tier's newest complete set, on the
    reasoning that a pin whose rollback set is in the tier has its record
    below that set. A pin whose record lies ABOVE the newest tier set is not
    covered by that argument: in the narrow window before the view applies
    the record, a pass does not see the pin. Today that pin can protect only
    the newest set, which is never deleted; the edge is a set that lands in
    the tier inside that window. Narrow and unobserved — settle it (gate on a
    later position, or show the window empty) before nodes delete on the
    tier's word. **Still open:** 3b shipped with this edge unsettled: nodes
    now hold purge on the tier's word; the window remains narrow and unobserved.
- **Compressing the node's own journal (idea, unmeasured).** The backup tier
  compresses what it writes off-node (spec
  `superpowers/specs/2026-10-10-uc2-backup-tier-design.md` D11); the node's
  live journal stays raw because the archive agent writes it on the commit
  path and the sender resends straight from it. If node disk ever binds,
  measure the archive hop alone with and without per-block zstd before
  deciding — it would also change the on-disk block format (a flag day).
  Recorded 2026-10-10.
- **`auto_fetch_candidates` pairs a catalog with a separately-read version.**
  `uc_node/src/node.rs`'s `auto_fetch_candidates` takes the catalog from the
  `ClusterViewInner` it is handed but loads `ClusterView::catalog_version`
  from its own atomic, which `ClusterView::publish` stores after the catalog.
  A read between the two pairs holders' `sets_held` bit indices with the
  wrong set list — the same two-read race the cluster view fixed by deriving
  the version from the catalog it renders. Candidate fix:
  `catalog_version_of(&inner.catalog)`. Found in the cluster-view final
  review, 2026-10-15; not reproduced.
- **Admin writes over HTTP (for a GUI).** The cluster view is read-only; a GUI
  that also changes the cluster needs a write path that does not exist.
  Open questions: how a remote request is authenticated (the admin band's
  HMAC signature is bound to a local `cnc2.dat` and its `instance_id`, so a
  remote signed request needs its own canonical form); replay protection
  across a network hop (`seq` monotonicity is per node); TLS (the metrics
  listener is cleartext and unauthenticated by design); and CORS — whether a
  browser GUI talks to the nodes directly or a GUI backend polls them, which
  is the shape the read surface already assumes. Taken from the cluster-view
  spec §9.
- **The `uc_service::apply` replay forward-progress guard has no counter.**
  Plan B3 T5 added a guard that hands the cycle back when a replay pass
  fails to advance the cursor (it used to spin forever, which also hung
  `Service::stop`'s join). Since the plan's final review that episode is no
  longer silent — it re-enters the replay with the gap forced, so the row
  either installs a covering artifact or fail-stops by name — but the
  episode itself is still reported only by an `eprintln!`: no metric family,
  no obs record, because `uc_service` has no `uc_obs` dependency and no
  status word for it. A row that waits there legitimately (its covering
  artifact is above the apply target) is visible only in the service's own
  stderr. Recorded 2026-09-21.
- **Widen `replay_into`'s gap test.**
  `uc_service/src/replay.rs`'s gap guard is `first > start_pos` with `first
  = reader.first_meta().unwrap_or(0)`, which cannot detect the shape a
  below-floor joiner presents: cursor 0, a stale journal segment based at 0
  (so `first = 0`), and a live buffer base above it. The plan B3 final review
  (F1) routed around it — a replay pass that cannot advance re-enters with
  the gap forced — but the root cause is that `first` is read from the
  journal's own first block rather than from something that knows the
  frontier (the log buffer's base, or the node's archive floor). Note what
  the detour rests on: with the TRUE `first`, the covering-artifact test
  `s_pos >= first` proves the artifact meets the journal's retained tail;
  with the forced (synthetic) `first` it proves only "above the cursor", so
  no-hole-between-artifact-and-tail is currently an ARCHIVE invariant
  (`purge_before`, `truncate_to` and `adopt_floor` all keep the journal
  contiguous or wipe it) rather than a local check. Deriving `first` from
  the frontier restores the local check, makes the detour unnecessary and
  removes a whole class of
  "the journal answers a question it cannot answer". Not changed in the fix
  wave because it is a change to the guard every reconstruction path runs
  through. Recorded 2026-09-21 (`.superpowers/sdd/2026-09-21-uc2-live-snapshot-reports/final-review.md`,
  Important 1).
  **Still open after #77 (2026-10-07):** the detour also fired on a healthy
  row whose next frame straddled `min(commit, durable)` — commit can sit
  mid-frame — and fail-stopped it at the first ring lap. #77 fixed that one
  misfire (`Replay::AwaitCommit`: a stop at a frame the journal holds is
  never a gap); the widening itself is unchanged.
- **`uc_gateway/tests/credits_wire.rs`'s latch test no longer exercises a
  declared node.**
  `a_connection_told_not_serving_is_never_served_later_on_the_same_socket`
  pins that an edge which answered "not serving" never serves that socket
  afterwards. Plan B3's readiness gate collapses the "the node exists but
  cannot serve" window on a single-voter cluster, so the test was re-shaped
  onto an UNDECLARED harness node (`common::start_single_node_full(…,
  declared = false)`) — a page with no names on line 7, the one shape the
  gate does not apply to. The latch is still pinned, but no longer against a
  node a client could really attach to: re-shape it around a **learner** (which
  declares, and never serves), so the window is real again. A
  branch-introduced coverage loss, recorded 2026-09-21.
- **`commit_source` as an enum.** `uc_consensus/src/election.rs` carries the
  provenance of the current commit position as a `&'static str` compared by
  value (`"gossip"`, `"leader"`, …). It is read in obs records and in tests;
  an enum with a `Display` would make a typo a compile error and let the
  stale-value class of bug (a documented value that no longer exists) be
  caught by the match. Ledgered by plan B3's T5 review, recorded 2026-09-21.
- **`pin-verify` against a real cluster, and against a learner.**
  `uc2-diffreplay pin-verify` (plan C) proves S4's refusal and the pinned
  install's artifact path on **one** node — a single voter in a scratch
  instance dir — so it says nothing about the step the upgrade how-to leans
  on hardest: "confirm the pin is visible on EVERY node before you stop
  anything". A multi-node rig would let the mode judge a pin that has
  committed on some nodes and not others, and a learner arm would cover the
  row shape that declares but never serves. Recorded 2026-09-21 in
  `docs/VERIFICATION.md` § "What `pin_verify.rs` does NOT verify".
- **A spurious `service_detached` record right after every attach.**
  `uc_node::node`'s `note_service_transitions` (~`uc_node/src/node.rs:5708`)
  decides whether a row is `live` from its `heartbeat_ns` word, and the
  sample after an attach can still read `0` there — so `live` is false, the
  `service_was_live` latch set at the attach edge clears, and that sample
  emits a `service_detached` record about a service that never detached,
  microseconds after its `service_attached`. Harmless to the node, but it
  makes the obs stream lie about the one transition an operator watches
  during an upgrade window.
  Found by plan C T2 (the `pin-verify` rig watches exactly these
  transitions); ticket-worthy. Recorded 2026-09-21.
- **`uc_diffreplay` has never compiled with `--no-default-features
  --all-targets`.** Pre-existing — its test targets call `Corpus::export`,
  gated behind the `export` feature, with no `required-features` line — but
  **plan C widened it**, and the widened form is what a fixer will meet.
  Measured on this HEAD the first errors are `unresolved import
  uc_diffreplay::live` ×2 and `cannot find live in uc_diffreplay` ×8:
  `tests/common/mod.rs` now `pub use`s `live::*`, and `common` is compiled
  into **every** test target in the directory, so the fix is
  `required-features = ["export", "pin-verify"]` on *every* target, not one
  key on the two that call `export` — plus the wider clippy run added to CI.
  Only the `--lib --bins` form of that run is in the proof stack today.
  Found by plan C T2, cause and fix corrected by plan C's final review,
  recorded 2026-09-21.
- **Build `uc_starter` against the in-tree crates in `ci.yml`** (`[patch.crates-io]`), so an SDK break is caught on the PR that makes it — deferred until `uc_starter` is public (a private checkout needs a secret). Spec `docs/superpowers/specs/2026-09-24-uc-starter-design.md` §8. On publication: link it from README.md, docs/QUICKSTART.md §7 and docs/tutorials/build-an-application.md.
- **A backward wall-clock step on the leader stalls the heartbeat-staleness
  predicate for as long as the smear takes to retire it, not for the step's
  own size.** `maybe_append_row_genesis` (`uc_node/src/node.rs`) — and the
  existing service-liveness predicate it shares the comparison with — judge
  freshness as `pass_now_ns.saturating_sub(heartbeat_ns) <
  SERVICE_STALE_NS` (3 s, `uc_node/src/services.rs`). `heartbeat_ns` is
  stamped from the real wall clock (`unix_ns()`, `uc_service/src/apply.rs`),
  but `pass_now_ns` is the leader's smeared, monotonic LOG clock
  (`uc_node::log_clock`, `2.12.0`): a backward step is never frozen but
  retired at `SMEAR_PPM = 500` parts per million of monotonic time, so a
  step bigger than `SERVICE_STALE_NS` leaves `pass_now_ns` reading ahead of
  every fresh heartbeat for `step_ns / 0.0005` of real time — around 100
  minutes to retire a 3 s step, not 3 s. Every reader of this predicate
  (row genesis, and whatever else keys "is this row's service alive" off
  the same comparison) waits that long. Candidate fix: read the wall clock
  for the staleness test only when a row is unversioned (row genesis is a
  one-time bring-up cost, not a steady-state one, so paying a real
  `SystemTime::now()` read there does not reintroduce the ambient-clock
  problem the log clock exists to avoid on the steady apply path). Recorded
  2026-09-28.

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
