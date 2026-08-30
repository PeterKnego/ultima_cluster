# Dispatch brief: nightly flake hunt (fresh session)

**Status:** QUEUED 2026-08-16 — not started. This brief is the session's
starting context; read it with the memory topics it cites before touching
anything.
**Mission:** return nightly CI to green by closing or honestly de-rating
the intermittent family that failed 6 of the 8 nightlies Aug 9-16 — with
the house pre-committed-n discipline throughout (fix n BEFORE coding;
template: the receiver-frontier soak, memory `receiver-frontier-soak`).

## Evidence at queue time (2026-08-16)

Nightlies (schedule runs, repo PeterKnego/ultima_cluster): Aug 9 F, 10 F,
11 P, 12 F, 13 P, 14 F, 15 F, 16 F → **6/8 failing, elevated** vs the
documented per-test rates. Push CI, elle, elle-weekly, lean-proofs,
sim-heavy, loom: all green throughout. The three failed nights that were
diagnosed (run ids for log retrieval):

| night | run id | failing test | class |
|---|---|---|---|
| Aug 16 | 31925505297 | `resize_3_to_5_to_3` (uc_node/tests/reconfig.rs:1192, "node never adopted its own removal", 20 s deadline) | reconfig adoption-timeout; documented "pre-existing 2/4 on loaded baseline" (memory `pre-existing-test-flakes`) |
| Aug 15 | 31862981617 | `node_sigkill_recovery` (examples/uc_crashtest/tests/hard_crash.rs:582) | crashtest recovery timeout. The two `apply.rs:385` panics in its log are the DESIGNED fail-stop (instance_id changed → service fail-stop) — expected SIGKILL noise, do NOT chase them |
| Aug 14 | 31771653976 | `sigkill_mid_config_window` (hard_crash.rs:746) | documented ~5%/run = the crypto-OFF floor (memory `m8-crypto-reconfig-open`) |

Aug 9/10/12 failures NOT yet diagnosed — Phase 0 classifies them.

Timeline note: Aug 14/15 nightlies ran BEFORE the pipelined-client merge
(`c3011e2`); Aug 16 was the first on it and failed in a path the client
work never touched. Nothing implicates that arc.

## Pre-committed method

**Phase 0 — classify everything.** Pull all 6 failed runs' logs, one row
per failure: test, assertion line, which deadline tripped. If new family
members appear, add them to the table before choosing targets.

**Phase 1 — reproduce under CI-like contention, n fixed first.** CI
runners are ~2-core and noisy. For each target test, PRE-COMMIT n (e.g.
n=50) and run two arms on this box: unconstrained, and constrained to two
cores (`taskset -c 0,1`, plus a background load if needed to mimic runner
noise). Record rates. A test that reproduces ONLY under the 2-core
constraint is a contention-liveness question; one that reproduces
unconstrained is a real intermittent worth full systematic-debugging.
Artifacts to real disk, never `/tmp` (tmpfs rule).

**Phase 2 — per-test verdict, one class at a time (do not blast all
three).** Two legal outcomes only:
- **Liveness/consensus bug**: escalate to a real fix with a directed test
  (house standard); a flake in reconfig adoption or crash recovery may be
  a genuine liveness hole — treat memory `archive-cursor-vs-second-primer`
  and `receiver-frontier-soak` as prior art for what these hunts find.
- **Deadline too tight for contended runners**: fix = raise the specific
  deadline WITH measured before/after rates at the pre-committed n —
  never an unmeasured bump.

**Standing cross-reference that outranks convenience:** memory
`receiver-frontier-soak` carries an OPEN acked-write-loss signal (witness
fired on an unmutated run, ~0.7%/run). Any crashtest failure in this hunt
must be checked against that witness before being filed as "timeout
flake" — a loss masquerading as a flake is the one outcome this brief
exists to not miss. Related caution: memory `elle-tooth-oracle-expiry`
("ask what was detecting it before" applies to any deadline you raise).

## Exit criteria

- Rate table (per test: CI rate observed, local rates both arms, n).
- Per-test verdict (bug fixed | deadline re-rated with data | reclassified
  and escalated), each a commit.
- **Three consecutive green nightlies** after the fixes land, then update
  memory `pre-existing-test-flakes` (close what's closed, keep what's
  honestly still open) and delete/annotate this brief's QUEUED status.

---

## Phase 0 RESULT (2026-08-16, session start) + Phase 1 pre-commitment

Phase 0 reclassified the campaign. Complete table:

| night | failure | class |
|---|---|---|
| Aug 9 | elle-crypto failover: **`false|incompatible-order`** (4,684 events) | **SAFETY SIGNAL** |
| Aug 10 | elle-crypto failover: **`false|incompatible-order`** (13,630 events) | **SAFETY SIGNAL** |
| Aug 9 | resize_3_to_5_to_3 + restart_of_removed_node_refuses_to_start (reconfig.rs:1192/1663) | liveness timeout |
| Aug 12 | linearizable_under_failover_with_crypto @ mod.rs:618 = "no survivor leader within Ns" | liveness timeout (crypto arm) |
| Aug 14 | sigkill_mid_config_window | liveness timeout |
| Aug 15 | node_sigkill_recovery (apply.rs:385 panics = designed fail-stop, noise) | liveness timeout |
| Aug 16 | resize_3_to_5_to_3 | liveness timeout |

**Priority ruling: the elle-crypto `incompatible-order` outranks everything**
— a serializability-violation verdict on unmutated histories, twice, and
possibly the same underlying event as the OPEN acked-write-loss witness
(memory `receiver-frontier-soak`, ~0.7%/run, open since 2026-08-03). The
timeout family waits.

Facts pinned: CI seed is the DEFAULT (0x1107 = 4359 printed) every night —
nondeterminism is real-time scheduling, not seed; no golden repro exists.
No CI artifacts were uploaded (histories lost); local reproduction
required. CI apparent hit rate: 2 of ~8 nightly crypto runs ≈ 25%.

**Phase 1 pre-commitment (elle-crypto failover repro):**
- Exact CI shape: `UC2_CRYPTO=1 ELLE_TARGET_OPS=8000 ELLE_BUDGET_SECS=300
  scripts/elle_check.sh failover`, release build, default seed, fresh
  `ELLE_DIR` per attempt under `/home/claude/elle-hunt/` (real disk).
- **n = 12 attempts, FIXED**, arm A = unconstrained (this box is 4-core,
  same nominal shape as ubuntu-latest). P(0 hits | true rate 25%) ≈ 3%.
- Decide: ≥1 `incompatible-order` (either model) → REPRODUCED; preserve
  the full ELLE_DIR + elle-cli verbose JSON (both models) for that attempt
  — the anomaly detail IS the analysis payload — and move to history
  analysis. 0/12 → NOT reproduced at this rate; re-plan EXPLICITLY
  (2-core taskset arm next); no silent extension.
- Passing attempts' dirs are deleted; failing attempts kept whole.

**Phase 1 re-plan (pre-registered branch taken, 2026-08-16):** the first
n=12 ran the literal CI env but the 8,000-op TARGET binds on this fast box
(8 s, 3 fault ticks/attempt) while the 300 s BUDGET binds on slow CI
runners (246 ticks) — ~1/80th the fault exposure per attempt; those 12
passes are void as evidence (harness lesson recorded: equalize
EXPOSURE — fault windows — not op counts). Amended protocol, n fixed
before running: `ELLE_TARGET_OPS=1000000` (unreachable → budget binds →
~250 fault ticks/attempt, matching CI), `ELLE_BUDGET_SECS=300`;
**arm A: n=8 unconstrained; arm B: n=8 under `taskset -c 0,1`** (CI-speed
op rate AND wider per-op vulnerability windows). Decide: ≥1
`incompatible-order` in either arm → reproduced (keep full dir + verbose
anomaly JSON). 0/16 → next re-plan is explicit (2-vCPU cloud VM, or land
CI artifact-upload and wait for the next nightly hit).

**Phase 1 status update (2026-08-16):** REPRODUCED on the amended
protocol's first attempt (round 2, arm A, attempt 1):
`strong-serializable false|G-single-item-realtime`, serializable clean —
stale linearizable reads (2.2 s and 9.5 s of staleness) right after
serving resumed post-failover. The CI `incompatible-order` is the
stronger cousin of the same signal. Two follow-on facts from the same
round: (a) attempts 2-3 died SIGBUS — root-caused to `truncate(true)` on
mmap-shared files at boot (cnc + all rings), fixed on main `4f544dd`
(punch-hole helper, hammer test); (b) the elle checker at `-Xmx2g` OOMs
on ~450 MB histories → empty verdict misread as FAIL; runner now uses
6g and classifies empty verdicts INVALID.

## ACKED-WRITE-LOSS WITNESS CLOSED (2026-08-18)

The standing open item this brief was told to outrank convenience for
(memory `receiver-frontier-soak`, open since 2026-08-03) is closed on its
own pre-committed n.

- **Protocol, fixed before launch:** control arm = `elle_mut_vote_order`
  with the mutation feature compiled in and `UC2_MUTATION` UNSET (inert), so
  every partition/heal cycle runs the SHIPPED vote-order check with
  `CommittedTruncationWitness` armed. **n = 450**, chosen so that zero hits
  excludes the ~0.7 %/run pre-fix rate at one-sided 95 %.
- **Result: 0 hits / 449 valid runs** (450 launched, 1 errored) →
  **0.668 %/run** upper bound, excluding both pre-fix rates (11.3 %/run
  archive-fail-stop, ~0.7 %/run unmutated witness). Root cause was the
  term-map window chain fixed earlier in this arc.
- **Shape deviation, recorded not hidden:** `ELLE_KEYS=200`,
  `ELLE_WORKERS=2`. The July shape now OOM-kills on this box (4.5 GB max RSS
  per run) because post-fix throughput is ~8x higher and the list-append
  values balloon before the fault minimum is met. Keys/workers change the op
  MIX, not the fault exposure the witness feeds on — still 10 partition/heal
  cycles per run, RSS 1.0 GB.
- **One observation kept, NOT a hit:** run 428 failed with `no single
  serving leader within 20s` — a post-heal reconvergence timeout, no
  truncation and no elle-invalid verdict. Its log was then LOST to a cleanup
  glob (`rm -rf .../run*`) that also matched the kept file — only the tally
  line and the failure text quoted here survive. Keep preserved evidence
  outside the directory a cleanup targets.
  1/428 = 0.23 %/run, same family as the Aug 12 nightly `mod.rs:618`
  timeout, which predates this arc. Telling "pre-existing" from "introduced
  by the validated-frontier gating" needs ~1300 runs per arm at that rate;
  the nightlies are the cheaper ongoing signal.

## LAST FLAKE CLOSED: `resize_3_to_5_to_3` (2026-08-17, main 834fc73)

The final nightly failure, and it was **not** contention. A removed node
adopts its OWN removal only if the removal frame reaches it before the leader
stops replicating to it — and the leader stops the moment the removal commits.
Caught with a diagnostic on the 12th attempt: the frozen node sat at
`cfgv=5 durable=130592` while the leader ran on to `cfgv=8 durable=192960`. It
had never received the bytes carrying its own removal and never would.

That race is INTENDED. Continuing to ship cluster data to a decommissioned
node would be the worse behaviour, and the design says so in its own risk
table (spec 2026-07-13): *"Known-source guard + tombstones (structural);
self-halt on seeing own removal"* — structural first, self-halt conditional.
The test had asserted the conditional half as a 20 s guarantee.

Fix: report adoption as best effort within a bounded window (it lands in
**57/60** runs — the flake rate, now stated in the open instead of hidden in
an assertion), then assert the GUARANTEE unconditionally — every survivor
drops both removed ids from its peer band, so no survivor addresses them.
Uses this suite's existing settle-poll-then-assert helpers; a first cut that
asserted the band instantly failed **27/60**, because `publish_peer_band` runs
inline with config adoption.

Measured: **0/60** after (5/86 = 5.8% before; P(0/60 | 5.8%) ~ 2.7%), full
reconfig suite 0/12 runs failed, workspace green.

Nightly `31993025198` (head `48fc96c`, the 0.5.0 commit) had already shown
`elle` and `elle-crypto` GREEN — the original safety signal — with this test
as its only failure. The exit criterion is now three consecutive green
nightlies plus the witness re-soak.

## RESIDUE CLOSED: content attestation, wire protocol 0.5.0

The architectural call below was taken: **attest content in the report.**
`DGRAM_KIND_APPEND_POSITION` gains an 8-byte body carrying `durable_term` —
the term the sender attributes to the byte below its reported position — and
the leader declines any report that disagrees with its own term map. Equal
terms at the same position imply identical prefixes (Log Matching), so this
is precisely Raft's `(index, term)` pair and it turns commit ranking from a
POSITION quorum into a CONTENT quorum. Header and `cnc.dat` unchanged; a
0.4.0 peer's header-only report reads as unattested and is simply not
counted, so a mixed cluster stalls commits rather than making unsound ones.

**Attestation alone did NOT close it** (still 6 rewinds/run). The last hole
was in the previous round's own frontier-extension rule: it extended the
validated frontier whenever `durable` advanced, assuming bytes arriving after
a clean reconcile come from the current leader. They need not — the archive
also records buffer content accepted under an EARLIER term, which a later
leader can contradict. Requiring the frontier bytes to belong to the current
term (our map's last term == `current_term`) separates "streaming from this
term's leader" from "catching up on a deposed leader's tail".

**Result: rewinds 11 -> 0 per 300 s storm** (three runs), 0 acked-write
violations, cuts still occur (8-11/run) but never below the applied frontier
— which is exactly the invariant the tripwire exists to assert.

**Test integrity:** two sim red-twins stopped firing because attestation
independently defends the bug they inject (unguarded intake reopen). Rather
than delete or weaken them, `SimConfig::attest_reports` ablates attestation
so each twin still proves the guard it names is load-bearing. Lean: the model
already ASSUMED this property (`sendReport` carries `hgate : reconciled`), so
Rust is now strictly more conservative than the modelled transition and every
safety theorem still covers it; the fidelity note is recorded in
`ProtocolCommit.lean`. 3037 jobs green, 100k conformance vectors clean.

## QUORUM-PLANE RESIDUE (superseded by the section above): four fixes

Chasing the `prov=("gossip")` rewind found four independent defects, each
fixed with a test (commit `a1f17e4`):

1. **`CommitTracker` slots were high-water marks.** A follower's durable
   REGRESSES on every truncation (reconcile cut, wipe, restart onto a
   shorter journal) — constantly under churn — so the leader kept ranking a
   quorum that no longer existed. Slots now take the latest report. Same for
   M7's carried `last_reports`; the receiver also re-reports immediately
   after a truncation instead of waiting out `append_pos_floor_ns`.
2. **Term observations were delivered lossily** (`try_send`, on a comment's
   claim they are "re-derivable from commit gossip" — they are not:
   `observe_terms` scans each block exactly once). A storm overflowed the
   channel; the map then permanently lacked terms whose bytes the node held,
   so every later reconcile read the leader's newer entries as divergence.
   Now retained/retried, and buffered across the SM's truncating latch.
3. **The SM's `durable` could run a duty cycle AHEAD of its own term map**
   (the archive publishes the counter inside `do_work`, hands observations
   over afterwards). `refresh_durable` now clamps to the observation
   frontier, so `durable` and the map describe the same prefix.
4. **The commit-validation latch was a BOOLEAN.** A follower that reconciled
   clean could still take a gossiped commit far past anything validated
   (31 KB in the captured case) and apply a deposed leader's bytes. It is now
   a POSITION (`validated_up_to`), lowered by any unconfirmed term boundary,
   and `AppendPosition` reports are clamped to it — making the leader's
   ranking a CONTENT quorum rather than a POSITION quorum.

**Measured: rewinds 11 -> 4-7 per 300 s storm; 0 acked-write violations
throughout. NOT closed.** Four principled fixes each moved the number
without eliminating it — the systematic-debugging rule for that pattern is
to stop patching and question the architecture, so this stops here.

**Architectural finding (maintainer decision):** *a byte position is not a
content identity.* UC commits POSITIONS and validates CONTENT only at
term-map granularity, which is too coarse when leaders churn faster than
maps propagate. Closing this properly likely means attesting content in the
report itself — a term-or-digest stamp for the acknowledged range — which
touches the wire protocol (another version bump) and is a design call, not
a session patch. Everything above is defence in depth beneath that.

**Behaviour changes to know about:** (a) each rig run now shows exactly one
wipe-and-rejoin (previously zero) — clamped reports let a lagging node fall
past the 64-entry window and take the legitimate `NoCommonPrefix` path; with
M6 snapshots on this is a snapshot install, not a wipe. (b) The sim pin
`raw_m3_forged_report_phantom_commit_is_caught` was STRENGTHENED: a one-shot
forged report is now self-correcting (the sender's next honest report
overwrites it — under high-water slots it latched forever), while a
SUSTAINED forgery still trips inv7. Both halves are asserted.

## RESIDUE CLOSED: the apply/gossip race (2026-08-16, same day)

The race the rewind tripwire exposed is now fixed rather than merely
surfaced. **Mechanism:** commit gossip carries a POSITION ONLY. A follower
that adopts a new term holds a tail no leader has validated yet (the
term-map reconcile arrives on a later datagram, or a lost one). Accepting
the new leader's commit position blessed OUR bytes at positions the NEW
timeline owns, so the service applied a deposed leader's content there
(elle `incompatible-order`); when the reconcile cut finally landed it was
BENEATH the applied cursor. Raft closes this with the AppendEntries
prevLogIndex/prevLogTerm match gating `commitIndex`; UC's equivalent
evidence is the term-map reconcile, so the commit advance now waits for
it.

**Fix** (`uc_consensus::ElectionSm`, the safety core, so the sim
adjudicates it): an `awaiting_reconcile` commit-validation latch, armed on
adopting a strictly higher term (mirroring the node's existing data-plane
latch + intake-gate close, derived at boot from the same recovered state:
`vote_term > map_term`), released when this term's leader map reconciles
clean OR its truncation acks, and dropped on becoming leader. Gossip
arriving while latched is HELD in `deferred_commit` and replayed as one
`AdvanceCommit` on release — bounded by ONE gossip round, because the
leader ships `ShipTermMap` alongside every `GossipCommit` on both the
commit-advance path and the idle floor.

**Model finding (third of a kind):** `ProtocolCommit.lean` §10 lists this
exact plane as a documented omission — *"(a) commit gossip / follower
`commit_seen` (decision 4 — YAGNI: leader completeness is about the
leader's hist)"*. As with the windowed map (`ProtocolData` decision 7),
the bug lived precisely in the erased distinction. The fix needs no Lean
change (the plane is unmodeled); modeling the follower commit plane is
now a proofs-arc item, not a YAGNI.

**Acceptance pre-commitment (fixed before running):** (1) rig n=8 x 300 s
— 0 violations, 0 wipes, 0 consensus deaths AND **0 tripwires** (the new
metric: 13-27/run before this fix, 0 after it in a single pilot run);
(2) elle failover x3 both models; (3) `resize_3_to_5_to_3` n=20 — the
latch could delay config adoption, so it must not exceed the measured
5.8% baseline materially; (4) lin_v2, lin_partition_v2, hard-crash,
workspace, clippy, conformance 100k, `lake build`. Any miss → back to
analysis, no goalpost moves.

### Race-fix acceptance RESULT — pre-committed bar MISSED, reported as written

The pre-committed bar was **0 tripwires**; the measured campaign gave
**6-14/run** (n=8 x 300 s), against 13-27/run before the latch. Recorded
as a MISS, not re-scored: the latch closes the path it names (directed
unit test `a_new_terms_gossip_cannot_bless_an_unvalidated_tail`, and 0
acked-write violations / 0 wipes across the campaign) but a SECOND,
distinct defect keeps the rewind alive. The new `UC2_TRUNC_TRACE`
provenance field names it: every surviving cut-below-commit carries
`prov=("gossip", …)` — a follower validated cleanly against leader A,
took A's gossiped commit C, and a LATER leader B then truncated below C.
So either A committed C without a surviving quorum, or B is missing
committed bytes. That is the quorum/leader-completeness plane (the
Figure-8 / #6b family), not the apply path — **OPEN, proofs-arc scope**.
Severity bound: no acked write was lost in 8 x 300 s, and followers never
publish responses, so the divergence is contained to a node that then
poisons itself.

Two harness/robustness defects surfaced while accepting this, both fixed:
(a) the tripwire's PANIC had no supervisor in-process — the apply thread
died mid-pass, the node silently stopped applying, and the panic re-raised
at teardown (2 of 3 `lin_partition_v2` runs, 1 of 3 elle passes — neither
a consistency anomaly). Fixed by POISONING instead of panicking (stop
applying, RETRY every read, keep the heartbeat) plus a real supervisor
(`AgentRunner::is_finished`, `Service::is_alive`,
`LinClusterV2::supervise_services`, `respawns=` in elle summaries).
(b) the term-map slot clamp had to become SIZE-driven: entry width is
value-dependent under bincode varints, and the race fix's ~15x throughput
gain pushed terms/positions into wider varint classes, overflowing the
count-based clamp (`got: 4085` vs limit 4079).

Final battery on the complete change set: rig 3/3 (0 violations, 0 wipes,
9-14 poisonings = the open residue, self-healing via respawn); elle 3/3
both models; lin_v2 7/7; lin_partition_v2 3/3 x 7; hard-crash 4/4;
reconfig 1/20 (baseline 5.8%); workspace exit 0; clippy clean; Lean 3037
jobs; conformance 100k vectors.

## ROOT CAUSE FOUND + FIX (2026-08-16)

The safety signal, the acked-write-loss witness, and (likely) the whole
liveness-timeout family are ONE bug. Chain, each step evidence-backed
(rig runs 1-8 in /home/claude/stale-hunt/, evidence dir kept, trace):

1. **Reconcile's common-prefix match was INDEX-aligned** while the leader
   ships a WINDOWED map (`term_map_wire_tail`, last 64 entries). After a
   cluster's 65th lifetime leadership term, `own[0]=(1,0)` never equals
   `leader[0]`, so `reconcile` returned `NoCommonPrefix` against every
   HEALTHY follower → wipe-and-rejoin (`Truncate{to:0}`). The old doc
   claim "unreachable at <= MAX_TERM_MAP_WIRE_ENTRIES terms" was tested
   only where window == full map. The Lean model erased exactly this
   distinction (ProtocolData.lean decision 7: "full-map gossip
   (simplification)") — bugs live in the distinctions a model erases.
2. **Wipe loop**: a wiped follower refills from 0 via paced deep-NAK,
   rebuilds old map entries from replayed frames, and the next gossip
   re-wipes it (window still above its bridge): 179 full wipes in 42 s
   (UC2_TRUNC_TRACE, run8), each discarding up to ~570 KB of bytes its
   own cnc commit mirror showed committed.
3. **Loss**: kills landing while a quorum is mid-wipe/refill elect among
   amnesiacs (honest low durable credentials), rebasing the cluster below
   commit; the rebooted full node adopts and truncates its committed tail
   (evidence: term-103 data 603104..655392 present in all three buffers
   as ghost frames; all journals+term maps rebased; terms 89-123 absent).
   ~52 KB / 1,191 acked writes lost in run 1's capture; commit counter
   (655328) never rewound, immortal via gossip.
4. **Divergence server**: the service never observes the rewind
   (`applied` monotone, `durable` primed under it) — it idles serving
   OLD-timeline answers through the refill, then RESUMES applying
   new-timeline bytes on top: merged-timeline SMs = elle
   `incompatible-order` (serializable!) + `G-single-item-realtime`.

**Fix shipped (this branch):** (a) reconcile aligns the leader's window
INSIDE the follower's full map by (term,base) before prefix-matching —
`NoCommonPrefix` is now the genuine purged-prefix signal only; a window
starting inside our bytes at an unknown term cuts there instead of
wiping. 3 directed regression tests. (b) service log-rewind tripwire:
`durable < applied cursor` on a matching instance id = fail-stop (the
stale-serving/timeline-merge window closes). Deferred hardening (follow
up in the proofs arc): commit-floor anchoring of the wire window,
election credential floor for wiped nodes, persisted commit watermark
so truncation-below-commit stays visible across reboots.

**Acceptance pre-commitment (FIXED before running):** rig
`stale_read_hunt` n=8 x 300 s, crypto ON, kill 500 ms, UC2_TRUNC_TRACE=1.
Decide: 0/8 violations AND zero RECONCILE-to-0 cuts on healthy followers
→ CONFIRMED (vs pre-fix 6/6 + 1/1). Any violation → back to Phase 1, no
goalpost moves. Then elle failover x3 (arm A shape): expect 3/3 PASS both
models (pre-fix valid-attempt rate 3 FAIL / 1 PASS). Then the full local
proof stack (workspace tests, clippy, lin_v2, lin_partition_v2, sim,
conformance) before any push.

**Acceptance round 1 result (partial, alignment fix only) + TWO MORE
DEFECTS UNMASKED:** 4 runs: 0 violations, 0 wipes (target metrics clean),
but 3/4 runs died on failure modes the wipe loop had been masking:
(1) `term-map persist fail-stop: PayloadTooLarge` — the persisted term
map overflows its ~4 KiB StableValue slot at ~340 lifetime terms
(minutes under churn; CI's Aug 12 `no survivor leader within 20s` at
mod.rs:618 is this assert's downstream) → FIXED: persisted copy clamps
to newest 300 entries (boot re-derives the full map from journal frames;
SM seeds from the re-derivation).
(2) The new service rewind tripwire fires on a REAL PRE-EXISTING
apply/gossip race: commit gossip is position-only, so a follower holding
a divergent old tail can apply its OLD content at positions the NEW
timeline's gossiped commit has blessed, BEFORE the (slower, term-map-
cadence) reconcile cut lands. Pre-fix this silently served wrong answers
(part of the elle anomaly mass); post-fix it fail-stops the service
incarnation (correct but loud). The underlying race is OPEN — proofs-arc
scope (commit-plane contract: the apply bound needs a content-validated
frontier, cf. Raft's follower commitIndex clamp to last-validated
AppendEntries). Recorded in memory `reconcile-window-alignment-loss`.
**Acceptance round 2 (RESTARTED, both fixes):** same protocol; success =
0 violations + 0 healthy-follower wipes + 0 consensus-thread deaths;
service-tripwire fail-stops are EXPECTED (documented open race) and heal
at the next nemesis cycle on that node.

**Acceptance round 2 RESULT: 8/8 PASS** — 0 violations, 0 wipes, 0
consensus deaths across 1,332 leader kills (tripwires 13-27/run = the
open apply/gossip race, as expected). Lean full build green (3037 jobs,
no sorries); conformance 100k vectors zero divergence.

**Elle acceptance amendment (recorded before rerun):** the first elle x3
at `ELLE_TARGET_OPS=1M` died SIGKILL (OOM) in GENERATION all 3 attempts —
post-fix throughput is several times higher (no wipe stalls), so a 300 s
budget now builds a far larger in-memory history than any pre-fix run,
and attempts overlapped the 7-worker Lean build. Amended to
`ELLE_TARGET_OPS=80000` (pre-fix-scale op volume, bounding recorder
memory; ~130 fault ticks/run x 3 runs keeps total exposure at CI scale)
and `ELLE_JAVA_XMX=3g` (2 g handled larger pre-fix histories). The decide
rule is unchanged: 3/3 PASS under both models.

**Gate results (2026-08-16, post-fix):** rig 8/8 PASS; elle failover 3/3
PASS both models (85-87 fault windows, 318k ops/run — post-fix generation
is ~8x pre-fix); Lean `lake build` green (3037 jobs, no sorries);
conformance 100k vectors zero divergence; clippy clean; lin_v2 7/7 and
lin_partition_v2 7/7 unloaded; workspace suite green EXCEPT
`resize_3_to_5_to_3` (the documented pre-existing reconfig flake, memory
`pre-existing-test-flakes`, measured 2/4 on a loaded baseline). Because
the fix touches term maps, that flake is re-measured HEAD vs the pre-fix
baseline (`4f544dd`) at a pre-committed n=6 each rather than assumed
unrelated; a HEAD rate materially above baseline blocks the push.

**Reconfig-flake control (HEAD vs pre-fix `4f544dd` worktree), rate table:**

| arm | first n=6 | n=20 | decisive n=60 | cumulative |
|---|---|---|---|---|
| HEAD (fixed) | 1/6 | 2/20 | 2/60 | **5/86 (5.8%)** |
| baseline `4f544dd` | 0/6 | 0/20 | 5/60 | **5/86 (5.8%)** |

Verdict: **no detectable regression** — identical rates. The interim
3/26-vs-0/26 was small-sample noise; the pre-committed response to a weak
one-sided signal was to widen n, not to argue it away (decide rule was
"HEAD ≥5 failures against baseline 0 blocks the push"; the decisive arm
put baseline HIGHER). Signature is always the documented one:
`reconfig.rs:1192 node 60 never adopted its own removal (v6)`, a 20 s
busy-spin deadline on a removed learner — contention-sensitive, and it
stays OPEN as the pre-existing flake it was (memory
`pre-existing-test-flakes`). Cheap repro: ~0.5 s per pass, 20 s per fail.

**Second amendment (the 80k cap was the WRONG shape):** capped runs bind
in ~39 s with 14 fault ticks and ~14 lifetime terms — they never reach
the >64-term regime the fix guards, so they test nothing. And post-fix
throughput is ~8x pre-fix (84k ops/39 s vs 72k/300 s — the wipe loop was
also a throughput disaster), which makes per-key lists (and therefore
history memory) explode quadratically at fixed key count: the 3 g checker
OOMed on a 168k-event history. Final gate shape: keep the FULL 300 s
budget (242 ticks, 300+ terms — the actual bug regime), bound memory by
workload geometry instead: `ELLE_KEYS=200` (spreads appends, shorter
lists, smaller reads), `ELLE_WORKERS=2` (halves op rate),
`ELLE_JAVA_XMX=6g`. Decide rule still 3/3 PASS both models.

**Directed-rig pre-commitment (stale-read root cause, 2026-08-16):**
`uc_node/tests/stale_read_hunt.rs` (ignored; hunt tool): 3-node crypto-ON
cluster, 1 writer acking monotone register writes, 2 linearizable readers
asserting every read ≥ the acked frontier snapshot taken BEFORE invoke,
nemesis = elle's failover mix (alternating leader kill+restart /
leader-service-only crash) every 500 ms. **n = 6 runs × 300 s, FIXED**,
concurrent with elle hunt round 4 (contention noted; it mimics CI noise).
Decide: ≥1 violation → root-cause payload (cnc dump + nemesis
correlation) and move to fix; 0/6 → mechanism needs elle's op shape —
next re-plan explicit: port the frontier witness INTO the elle harness
as an online assert.
