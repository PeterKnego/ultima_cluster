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

## Results — 2026-08-17, both arms PASS

Fleet destroyed immediately after collection; `terraform state list` empty.

### Arm A — cost (3 × c6id.2xlarge, us-east-1, single AZ, placement group)

| admission | inflight W | responses/s | p50 | p90 | p99 | gate |
|---|---|---|---|---|---|---|
| 64 KiB | 4096 | 947,159 | 4.065 ms | 4.657 ms | 5.186 ms | FAIL |
| 128 KiB | 4096 | 1,575,562 | 2.494 ms | 2.798 ms | 3.344 ms | FAIL |
| 256 KiB | 4096 | 1,955,519 | 1.801 ms | 1.977 ms | 2.202 ms | FAIL |
| 64 KiB | 1024 | 1,026,274 | 0.900 ms | 0.990 ms | 2.187 ms | PASS |
| 128 KiB | 1024 | 1,484,358 | 0.649 ms | 0.742 ms | 0.903 ms | PASS |
| **256 KiB** | **1024** | **1,478,833** | **0.653 ms** | 0.757 ms | 0.905 ms | **PASS** |
| 64 KiB | 512 | 894,942 | 0.524 ms | 0.609 ms | 0.828 ms | PASS |

(The W=4096 rows fail the gate's LATENCY bar, not its throughput bar — the
same shape as every previous M5 run; they are the deep-pipelining points.)

**RULE 1: PASS.** **Cost verdict: PASS, and the margin is not close** —
1,478,833 rps @ p50 0.653 ms against the 2026-08-15 engine re-run's
1,475,268-floor comparison point of ~1.47 M @ 0.655 ms. That is **+0.6 %**,
inside a 15 % band drawn for ~10 % fleet variance. The per-report term-map
lookup and 8 extra wire bytes do not register at 1.5 M responses/s.

### Arm B — correctness under churn (4 hosts)

- **learner-join: PASS** — joined in 1.31 s (budget 60 s); baseline 299,771
  commits/s, during-join 311,734 commits/s, **dip 0.0 %** (gate < 10 %).
- **purge-cycle: PASS** — 5 cycles, every follower reconstruction converged,
  worst **6.04 s** (gate < 10 s), purge confirmed fired.

### Attestation health — the metric this gate exists for

`reports_unattested` read from every node's unit log after both arms:

| node0 | node1 | node2 | node3 (learner) |
|---|---|---|---|
| 0 | 0 | 0 | 0 |

**Zero declined reports on every host, including the joining learner** — the
one place a lagging term map could plausibly have produced false declines on
real cross-host links. Nothing was near the FAIL condition (a monotonic climb
on a quiescent cluster), so the liveness risk this change introduces did not
materialise at fleet scale.

### Incidental fix

Provisioning any 4-host fleet failed for the WHOLE cluster: Ansible's
`aeron_port_base` / `uc_raft_port` maps were keyed `node0..node2` (a leftover
from the 3-host Aeron parity arc), so a 4th host raised
`'dict' has no attribute 'node3'` on every node. Extended through `node4`, so
the 5-host M7 fleet gate works too.

### Standing caveat

0.5.0 remains a **flag day**. This gate ran a single-version fleet, which is
the supported configuration; a mixed 0.4.0/0.5.0 cluster stalls commits by
design (a header-only report reads as unattested and is never counted) and was
deliberately not provisioned.
