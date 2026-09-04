# The M13 remote rows on Graviton — the per-connection toll grew, the aggregate toll shrank

*Ran 2026-09-01. Voters 3 × `c9gd.2xlarge` (Graviton, 8 cores no SMT, one
placement group, us-east-1a); client hosts `c6i.2xlarge` (x86, 8 vCPU, off
the measured path — the edge and cluster are ARM). Driver
`bench-infra/scripts/m13_hop_bench.py --arms gate --gate-client-hosts 2`,
10 s rungs, payload 64 B, per-connection inflight 1024. Zero lost responses
on every remote rung of every run.*

Closes the "not re-measured on modern hosts" caveat in
[`uc2-arch-sweep-c8id-vs-c9gd-2026-08-31`](uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md).

**This is a characterization, not a gate.** The driver adjudicates the
pre-committed *c6id-era* bars from
[`uc2-m13-gate-2026-08-24`](uc2-m13-gate-2026-08-24.md), and their verdicts
are reported verbatim below — but the bars encode 2020-x86 expectations, and
a miss here is information about modern hardware, not a regression (the
c6id gate itself remains PASS on its own fleet).

## Result (final clean run, 2 client hosts)

| row | measured | vs direct | c6id-era (for the ratio) | c6id bar → verdict here |
|---|---|---|---|---|
| G — direct `Engine` on the leader | **3.41 M/s** @ p50 0.235 ms | 1× | 1.75 M/s | (reference) |
| a — ONE remote connection | **1.70 M/s** @ p50 0.645 ms | **0.498×** | 1.08 M/s = 0.617× | ≥ 0.5× → **FAIL by 0.2 %** |
| b — best aggregate rung (n=8) | **3.10 M/s** | **0.908×** | 1.46 M/s = 0.836× | ≥ 0.75× → **PASS** |
| c — ladder monotone | n=16 is 0.73× of n=8 | — | monotone | ≥ 0.8× rung-to-rung → **FAIL** (client-bound, below) |
| d — engines → dummy node (ARM ingress alone) | peak **6.53 M/s** at 4 engines | — | — | PASS |

Two findings, opposite directions:

1. **The per-connection toll roughly doubled on modern hardware.** Direct
   got 1.95× faster than the c6id generation (3.41 vs 1.75 M/s) but a single
   remote connection only 1.57× faster (1.70 vs 1.08 M/s), so the ratio
   fell 0.617× → 0.498×. One TCP connection's serialize/credit/syscall path
   does not scale with the cluster behind it. (Measured twice: 0.452× and
   0.498× across the two sessions below — the client host was at ~40 % CPU
   both times, so this is the path, not the driver.)
2. **The aggregate toll shrank**: 0.836× → 0.908× at n=8. With enough
   connections the edge relays at near-direct rate on Graviton — and in one
   run (direct reference 3.00 M/s on that leader) the aggregate measured
   **1.066× — above direct**, because the direct arm's `engine-load` shares
   the leader's 8 cores with the node while remote load arrives from
   outside. On modern hosts "remote costs throughput" is true per
   connection and nearly false in aggregate.

## The client-host story (why `--gate-client-hosts` exists)

The check-first run used ONE 8-vCPU client host: its CPU hit **98.5 %** at
the n=8 rung — which was also the only non-monotone rung — and ~88 % at
n=16. The aggregate was measuring the client, not the path. With two client
hosts (each rung's N connections split evenly, rates summed, per-host CPU
recorded per point), n=8's binding disappeared (41 %/83 %) and the aggregate
rose from 2.54 to 3.10 M/s. **n=16 remains client-bound even at two hosts**
(one host at 99.8 %) — row c's dip is that, not an edge collapse; a third
client host would be needed to chart n=16 honestly, and the recorded
per-host CPU is the evidence either way. Rule of thumb from these runs:
budget one 8-vCPU x86 client host per ~1.5 M resp/s of offered remote load.

Unexplained residue, recorded not theorized: the two client hosts (same
`c6i.2xlarge` type, one Spot one On-Demand) showed a persistent ~2×
host-CPU asymmetry at equal per-host rates (e.g. 41 % vs 83 % at n=8),
across both runs.

## Method notes for reproduction

- Fleet: `terraform.tfvars` with `instance_type = "c9gd.2xlarge"`,
  `client_instance_type = "c6i.2xlarge"`, `node_count = 5`,
  `voter_count = 3`; driver `--nodes 5 --arms gate --gate-client-hosts 2`.
- Client hosts on EBS-only types need the (now default) **64 GB root
  volume** — the AMI's ~8 GB root fills mid-provision and wedges rsync;
  NVMe-bearing types never hit this because `/opt/bench` mounts the
  instance store.
- Spot for client hosts works (`client_spot = true`) once the account has
  the `AWSServiceRoleForEC2Spot` service-linked role (one-time admin
  `aws iam create-service-linked-role --aws-service-name spot.amazonaws.com`)
  — but us-east-1a x86 spot was shallow on the day (c6id reclaimed after
  5 min, c5 unavailable), so this record's second client ran On-Demand.
  Existing one-time Spot instances can never change type/market in place
  (`lifecycle ignore_changes` guards this).
- The edge's TCP listener is awaited before the first rung (a startup race
  voided row a in one session; fixed in the driver).
- Both sessions' full `HOP-JSON` ladders (including the discarded
  race-voided rung) are in the job logs; the tables above are the complete
  clean run plus the check-first run's client-CPU evidence.

Destroyed after: 13 resources, `state list` empty, 0 instances by direct
API query.
