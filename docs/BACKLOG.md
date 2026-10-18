# Backlog — candidate directions after M14

*Written 2026-09-01 against `v2.10.0`. Status: a ranked list of options, not
a plan. Nothing here is scheduled; the maintainer picks. Every item cites the
document that first recorded it, so the reasoning can be re-checked rather
than re-derived. When an item is taken up it gets a spec under
`docs/superpowers/specs/` and a gate doc under `docs/benchmarks/`, and its
line here is updated to point at them. An item that ships, or stops
being relevant, is removed (last purge 2026-10-17): `RELEASES.md`,
`docs/releases.md` and git keep the record. A finding with a file and a
fix shape is a GitHub issue, not a small item here.*

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

- **`leader_completeness`** — the roadmap's HIGHEST-priority task (F-UC-1)
  and the sole named open theorem in the corpus. Not untouched: the closure
  arc banked 2026-07-19 reduced it to one named obligation, the `canon`
  invariant, with its assembly, crux, consumer interface, antitonicity and a
  machine-checked satisfiability witness all landed, and the hypothesis that
  had made it conditional discharged since. What remains is the joint /
  well-founded induction the corpus's standard shape does not give
  (`docs/VERIFICATION.md` § "What is not proved", estimated 3–4 tasks) — and
  the next bullet, which has to land first for the result to mean what it
  says.
- **The Lean model collapses the durable counter's two readers into one**
  (issue #7). A real acked-write-loss bug lived in exactly that gap and was
  found from the Rust side. Proofs composed over that lemma are weaker than
  they look until the split lands.
- **SPSC and the futex layer have no loom model**; MPSC, Broadcast, the log
  buffer's frame visibility and its write-reserve seqlock (`loom_reserve`,
  from the #78 fix, in `nightly.yml` since #116) do, and the Broadcast model
  found a real weak-memory defect the day it was written (2026-08-31). The
  mmap itself is outside loom and Miri; a Vec-backed variant for Miri "has
  not been built, and that trade-off is recorded rather than resolved".
- **aarch64 tests in CI.** The release binaries are built on a native
  aarch64 runner (`release.yml`'s `ubuntu-24.04-arm` leg), but no workflow
  runs a test there; the full stack has passed on Graviton exactly once
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
- **The apply hop lost ground on Ice Lake, and the ring CRC is most of
  it.** The #33 apply-hop A/B
  (`docs/benchmarks/uc2-row-running-version-apply-ab-2026-10-17.md`) is an
  honest FAIL with the bar kept: #33 itself costs 2.5 % at N=1 under
  layout-randomised linking, and the merges after it a further 4.2 % at
  both N — Ice Lake only, the same binaries 2 % faster on Zen 5, mechanism
  unidentified ([#149](https://github.com/PeterKnego/ultima_cluster/issues/149)).
  The profile behind it put 61 % of the hop in the egress ring's per-record
  CRC-32, in the ring format since `v2.0.0`
  ([#150](https://github.com/PeterKnego/ultima_cluster/issues/150)). Both
  are issues; the line is here so this direction's ranking can weigh them.
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

A finding with a file, a mechanism and a fix shape is a GitHub issue, not a
line here (`docs/agents/issue-tracker.md`): this section is for ideas whose
open questions come before any code. On 2026-10-17 the seventeen specified
items it had accumulated moved to issues
[#119](https://github.com/PeterKnego/ultima_cluster/issues/119) through
[#135](https://github.com/PeterKnego/ultima_cluster/issues/135), each with
its full text, and the same day's re-check found the condition the third
had been waiting on already met (`uc_starter` has been public since
2026-09-24), so it is
[#153](https://github.com/PeterKnego/ultima_cluster/issues/153); what stays
is the two that are still ideas.

- **Compressing the node's own journal (idea, unmeasured).** The backup tier
  compresses what it writes off-node (spec
  `superpowers/specs/2026-10-10-uc2-backup-tier-design.md` D11); the node's
  live journal stays raw because the archive agent writes it on the commit
  path and the sender resends straight from it. If node disk ever binds,
  measure the archive hop alone with and without per-block zstd before
  deciding — it would also change the on-disk block format (a flag day).
  Recorded 2026-10-10.

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
