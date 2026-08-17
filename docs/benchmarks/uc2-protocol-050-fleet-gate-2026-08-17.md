# Fleet gate: wire protocol 0.5.0 (content-attested durable reports)

**Status:** protocol pre-committed 2026-08-17, BEFORE provisioning. Numbers
land below only after the run; nothing in the decide rules moves afterwards.

## What is being validated

`DGRAM_KIND_APPEND_POSITION` now carries an 8-byte body with `durable_term`
(the term the sender attributes to the byte below its reported position), and
the leader DECLINES a report whose attestation disagrees with its own term map
(`Node::reports_unattested`). Local evidence is strong — the directed rig went
from 11 log-rewinds per 300 s kill storm to 0, elle 3/3, capstones green, Lean
3037 jobs, 100k conformance vectors. Two questions remain that only a real
cross-host fleet can answer:

1. **Cost.** Does the per-report map lookup plus 8 wire bytes show up at rate?
2. **False declines.** On real links (loss, reordering, cross-host skew), does
   a HONEST follower's report get declined often enough to hurt — a liveness
   risk this change introduces that the LAN-local rig cannot exercise?

Not being tested: mixed-version operation. 0.5.0 is a deliberate flag day (a
0.4.0 peer's header-only report reads as unattested and is never counted, so a
mixed cluster stalls commits). That behaviour is by design and asserted by unit
tests; deliberately provisioning a split-version fleet buys nothing here.

## Fleet shape

Reuses the existing orchestrators — no new fleet code beyond the node roles
now emitting `reports_unattested` to their unit logs (the counter is
process-local, so the cnc-reading `probe` role cannot see it).

- **Arm A — M5 throughput**, 3 × c6id.2xlarge (the SAME shape as the
  2026-08-15 M5 engine re-run, so its numbers are comparable):
  `bench-infra/scripts/m5_fleet_gate.py`.
- **Arm B — M6 scenarios**, 4 hosts: `m6_fleet_gate.py` (learner join, purge
  cycles). Exercises a catching-up replica's reports — the most likely place
  for an honest decline — under real cross-host conditions.

## Decide rules (FIXED before the run)

**Arm A (cost).** PASS iff the gate binary's own bar prints `RESULT: PASS` on
≥1 point (rule 1, unchanged from the M5 gate) AND the best sustained rate is
within **15%** of the 2026-08-15 engine re-run's 1.47 M responses/s. The 15%
band is the measured fleet-to-fleet variance from that same arc (a control
re-run of the OLD client on a fresh fleet came in 10.2% low), NOT a number
chosen to fit whatever comes back. A miss is reported as a miss and
investigated before any tag.

**Arm B (correctness).** PASS iff every scenario the orchestrator runs passes
its own documented bar (dip < 10%, reconstruction < 10 s, zero divergence,
zero acked-write loss).

**Attestation health (both arms), the metric this gate exists for.**
`reports_unattested` is read from every node's unit log at the end of each
arm. Expected shape: 0 in steady state; small bounded bursts around leader
changes and learner catch-up are legitimate (a follower's map catches up a
gossip round later). **FAIL if any node's counter climbs monotonically while
the cluster is quiescent**, or if a scenario's throughput dip correlates with
a climbing counter — either means honest reports are being declined.

## Cost discipline

Real terraform state is `bench-infra/terraform/` (the repo root holds an empty
decoy). Destroy immediately after collection and verify with
`terraform -chdir=terraform state list` printing nothing.

## Results

_(filled in after the run)_
