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
