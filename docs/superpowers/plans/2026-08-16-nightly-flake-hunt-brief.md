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
| Aug 16 | 31925505297 | `resize_3_to_5_to_3` (uc2_node/tests/reconfig.rs:1192, "node never adopted its own removal", 20 s deadline) | reconfig adoption-timeout; documented "pre-existing 2/4 on loaded baseline" (memory `pre-existing-test-flakes`) |
| Aug 15 | 31862981617 | `node_sigkill_recovery` (examples/uc2-crashtest/tests/hard_crash.rs:582) | crashtest recovery timeout. The two `apply.rs:385` panics in its log are the DESIGNED fail-stop (instance_id changed → service fail-stop) — expected SIGKILL noise, do NOT chase them |
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

**Directed-rig pre-commitment (stale-read root cause, 2026-08-16):**
`uc2_node/tests/stale_read_hunt.rs` (ignored; hunt tool): 3-node crypto-ON
cluster, 1 writer acking monotone register writes, 2 linearizable readers
asserting every read ≥ the acked frontier snapshot taken BEFORE invoke,
nemesis = elle's failover mix (alternating leader kill+restart /
leader-service-only crash) every 500 ms. **n = 6 runs × 300 s, FIXED**,
concurrent with elle hunt round 4 (contention noted; it mimics CI noise).
Decide: ≥1 violation → root-cause payload (cnc dump + nemesis
correlation) and move to fix; 0/6 → mechanism needs elle's op shape —
next re-plan explicit: port the frontier witness INTO the elle harness
as an online assert.
