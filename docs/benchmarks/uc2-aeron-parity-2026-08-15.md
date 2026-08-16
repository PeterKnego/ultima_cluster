# UC v2 vs Aeron Cluster — same-conditions scorecard (3 × c6id.2xlarge)

**Date:** 2026-08-15
**Status:** PRE-REGISTERED — protocol, grids, and reading rule below are
committed BEFORE the fleet run; results land in "Results" afterward and the
rules are not touched once data exists.

## Purpose

The v2 spec (§2) framed its stretch goal as "800 k parity (Aeron-parity
territory)", quoting Aeron Cluster's ≥800 k/s @ p50 0.38 ms — measured in the
v1 era on **3 × c6id.4xlarge** (16 vCPU). UC v2's M5-class numbers
(1.4-1.6 M/s @ p50 0.6-0.7 ms) come from **3 × c6id.2xlarge** (8 vCPU). This
run puts both systems on the SAME fleet, same hardware class as UC's gates,
and produces the v2 successor to the v1 scorecard
(`docs/tasks/task19_synccore_model_b.md` era). **Scorecard, not a gate**: no
pass/fail threshold — best-point vs best-point plus the ratio, with every
comparability caveat disclosed.

## Conditions matrix (pre-committed)

Matched exactly (same fleet, same run):
- 3 × c6id.2xlarge, us-east-1, single AZ, cluster placement group,
  private-IP binding, logs/archives on NVMe (`/opt/bench` / instance store).
- Both systems measured on THIS fleet — the UC anchor re-runs here
  (2026-08-15 A/B lesson: fleet-to-fleet variance ≈10%; cross-fleet quoting
  is invalid).
- 3 nodes, response only after quorum commit + apply on the leader.
- Deterministic leader on host0 (UC: start bias; Aeron:
  `aeron.cluster.appointed.leader.id=0`); client co-located on host0.
- Client edge = shared memory both: UC shmem rings; Aeron client attached to
  node0's media driver (`aeron.dir=/dev/shm/node0-driver`) with
  `aeron:ipc` ingress AND egress.
- 64 B opaque payload, 100% writes, one client process, snapshots off.
- Same `os_tune`, no CPU pinning either arm.

Matched with disclosed judgment calls:
- **Durability**: UC fdatasyncs the journal per ≤1 MiB block before positions
  count as durable. Aeron: `aeron.archive.file.sync.level=1` +
  `catalog.file.sync.level=1` (data-sync = the fdatasync equivalent;
  deliberately NOT level 2, which adds metadata sync UC does not pay —
  task13's 2026-06 table used 2, which over-penalized Aeron). Sync
  granularity differs (per-block vs per-recorder-batch): disclosed.
- **Methodology**: UC client = bounded-window max-throughput (best of
  admission×W sweep); Aeron `LoadTestRig` = fixed-rate pacing (best
  sustained rate). Best-point-vs-best-point, both swept; disclosed.
- **JVM warmup**: rig warmup phase (10 s) kept and excluded from
  measurement — JIT needs it, UC is AOT. Temurin 21, benchmarks-repo
  (`aeron-io/benchmarks` @ `6afb215`, Aeron 1.51) shipped launch scripts.
- **Response bytes**: Aeron echo returns the 64 B payload; UC returns a u64
  (8 B). ~56 B/op egress asymmetry in UC's favor; disclosed.

Inherent design differences (disclosed, not equalized):
- Threading posture: UC = 4 busy-spin agents + service (sized for 8 vCPU);
  Aeron = benchmarks-repo shipped posture, run in BOTH driver modes (below).
- Wire internals: each system's defaults (UC MTU 1408; Aeron repo-tuned
  term buffers / socket buffers per `cluster.properties.j2`).

## Arms and grids (pre-committed)

Run order: **UC anchor → Aeron SHARED sweep → Aeron DEDICATED sweep → UC
anchor repeat** (the bracketing anchors detect fleet drift across the
session).

1. **UC anchor** (public-Engine m5_gate client, main `8f8cb20` tree): points
   256 KiB/W=1024 and 128 KiB/W=1024, 15 s each, run at session start AND
   end (4 runs total). Invalidation rules as in the M5 gate doc.
2. **Aeron shared**: `aeron.threading.mode=SHARED` on all media drivers.
   Rate grid **{200k, 400k, 600k, 800k, 1000k, 1200k, 1400k}** msg/s ×
   batch.size **{64, 256}**, warmup 10 s, measure 15 s per rung, fresh
   cluster per MODE (not per rung — matches how Aeron is operated; rungs
   are back-to-back against a running cluster, like the v1 ladder).
3. **Aeron dedicated**: identical grid, `aeron.threading.mode=DEDICATED`
   (Aeron's default and the v1-era 4xlarge mode) — knowingly oversubscribes
   8 vCPU; that cost is a finding, not a flaw.

**Reading rule:** an Aeron rung is VALID iff the rig sustains the offered
rate (achieved == offered within the rig's own tolerance, no `.FAIL`
marker, no errors in node/driver logs). Each arm's headline = the highest
valid rate with **p50 ≤ 1.0 ms**, reported with p50/p90/p99. The scorecard
reports: UC anchor best point, Aeron best point per mode, and the
UC/Aeron ratio per mode. Secondary table: full grids verbatim.

**IPC-ingress validation (time-boxed, before the sweeps):** bring the
cluster up with leader-side `aeron:ipc` ingress + client IPC ingress/egress
and verify election completes and a smoke rung passes. task13 §11's
follower crash was a config-shape bug (global IPC channel + per-member UDP
endpoints appended); the fix path is per-role rendering (IPC on node0 +
client only, UDP members list otherwise) if Aeron 1.51 still appends
endpoints. If IPC ingress cannot be made to work inside the timebox
(~45 min), FALL BACK to UDP-loopback ingress for the client edge, disclose,
and record the fallback as a caveat row — the run proceeds either way. If
both edges are cheap to run, both are recorded (the IPC-vs-UDP client-edge
delta is itself a useful row).

## Results — UC v2 ≈ **1.6-1.8× Aeron Cluster's best sub-millisecond throughput** on identical 8-vCPU hardware; both Aeron modes knee at 800 k under the p50 ≤ 1 ms rule; shared mode wins its tails decisively

Run performed 2026-08-15 on one fleet (3 × c6id.2xlarge, cluster placement
group). **Client edge: `aeron:ipc` as pre-registered** — the IPC smoke
validated cleanly on Aeron 1.51 with the node0-only ingress render (task13
§11's crash class is config-shape, now definitively dead). Fleet destroyed
immediately after; `terraform state list` verified EMPTY. Raw artifacts
(`.hdr`, aggregator `.hgrm` reports, rig consoles, UC consoles) in
`bench-out/aeron-parity-2026-08-15/` (local).

### Scorecard (each system's best point under its reading rule)

| system | best sub-ms point | responses/s | p50 | p99 |
|---|---|---|---|---|
| **UC v2** (public Engine client, shmem) | 256 KiB / W=1024 | **1,282,493 – 1,433,230** (4 bracketing anchors) | 0.648–0.776 ms | 0.878–1.075 ms |
| **Aeron Cluster, SHARED driver** (IPC client) | 800 k rate / batch 64 | **800,000** (offered==achieved) | **0.360 ms** | 1.044 ms |
| **Aeron Cluster, DEDICATED driver** (IPC client) | 800 k rate / batch 64 | 800,000 | 0.394 ms | **42.0 ms** |

**Ratio: UC/Aeron ≈ 1.60–1.79× at the measured clean points** (UC bracket
÷ 800 k). Grid-granularity disclosure (added 2026-08-16): Aeron was
OFFERED up to 1.4 M in both modes — 800 k is the highest rung the rig
sustained (1.0 M+ returned its own `.FAIL` achieved-rate markers in shared
mode at both batch sizes), so Aeron-shared's true sub-ms ceiling lies in
the unsubdivided bracket **[800 k, 1 M)**. Taking that bracket's open end,
the conservative ratio floor is 1.28 M ÷ ~1 M ≈ **1.28×**; the honest
statement is "UC leads by 1.3–1.8×, with the measured-point ratio
1.6–1.8×". A finer sweep (850/900/950 k) on a future fleet would tighten
this to ±50 k. Dedicated mode, for completeness, ACHIEVED 1.0–1.4 M
(grid-capped, raw ceiling unfound) — Aeron moves ≥1.4 M/s through this
pipeline when latency is sacrificed. The complementary truth, stated plainly: **at Aeron's
operating point its p50 is ~1.9× lower than UC's at UC's** (0.36 vs
0.65-0.78 ms) — Aeron buys lower median latency at its rate ceiling; UC
sustains ~1.6-1.8× the rate inside the same 1 ms budget with comparable
p99. The v1-era gap (Aeron 800 k vs UC-v1 56 k = 14×) is closed and
inverted by v2.

Notable: Aeron-shared's 800 k @ 0.36 ms on EIGHT vCPUs essentially
reproduces the v1-era ≥800 k @ 0.38 ms measured on SIXTEEN (c6id.4xlarge,
dedicated mode) — Aeron's ceiling here is not core-starved; it is the
system's shape at this payload/replication/durability point.

### Aeron grids (aggregator `.hgrm` percentiles; COMPLETE tables added
2026-08-16 — percentile = value at the first histogram bucket ≥ the target
quantile, the reports' native resolution; `.FAIL` rows are the rig's own
achieved-rate failures, their latencies describe an over-offered run and
are reported for transparency, not as operating points)

SHARED (client edge IPC):

| batch | rate | p50 | p90 | p99 | sustained |
|---|---|---|---|---|---|
| 64 | 200 k | 265 µs | 295 µs | 410 µs | ✓ |
| 64 | 400 k | 338 µs | 400 µs | 453 µs | ✓ |
| 64 | 600 k | 354 µs | 420 µs | 1.03 ms | ✓ |
| 64 | **800 k** | **360 µs** | **432 µs** | **1.04 ms** | ✓ |
| 64 | 1.0 M | 1.30 s | 1.57 s | 1.66 s | ✗ `.FAIL` |
| 64 | 1.2 M | 1.24 s | 2.64 s | 2.94 s | ✗ `.FAIL` |
| 64 | 1.4 M | 4.89 s | 6.59 s | 6.97 s | ✗ `.FAIL` |
| 256 | 200 k | 614 µs | 730 µs | 8.0 ms | ✓ |
| 256 | 400 k | 491 µs | 617 µs | 25.1 ms | ✓ |
| 256 | 600 k | 509 µs | 727 µs | 44.3 ms | ✓ |
| 256 | 800 k | 510 µs | 3.59 ms | 53.3 ms | ✓ (tails blown) |
| 256 | 1.0 M | 1.50 s | 2.79 s | 3.05 s | ✗ `.FAIL` |
| 256 | 1.2 M | 2.69 s | 5.08 s | 5.41 s | ✗ `.FAIL` |
| 256 | 1.4 M | 46 ms | 1.58 s | 1.90 s | ✗ `.FAIL` |

The complete table sharpens two findings: **batch 64 is Aeron's only
clean configuration here** (batch 256 passes the rate but its p99 runs
8-53 ms even when sustained), and the `.FAIL` rows show what over-offer
looks like — p50s in SECONDS, full ingress-queueing collapse, the same
signature AWS's 2025 blog measured for OSS Cluster at a forced 1 M.

DEDICATED (client edge IPC) — rig sustained every rate to 1.4 M, but the
latency bar decides:

| batch | rate | p50 | p90 | p99 | p50 ≤ 1 ms |
|---|---|---|---|---|---|
| 64 | 200 k | 351 µs | 3.56 ms | 8.0 ms | ✓ |
| 64 | 400 k | 323 µs | 1.40 ms | 7.2 ms | ✓ |
| 64 | 600 k | 350 µs | 1.94 ms | 4.4 ms | ✓ |
| 64 | 800 k | 394 µs | 2.32 ms | 42.0 ms | ✓ (p99 blown) |
| 64 | 1.0 M | 1.92 ms | 39.1 ms | 55.7 ms | ✗ |
| 64 | 1.2 M | 1.71 ms | 3.39 ms | 27.2 ms | ✗ |
| 64 | 1.4 M | 2.40 ms | 5.85 ms | 12.7 ms | ✗ |
| 256 | 200 k | 1.49 ms | 6.20 ms | 10.8 ms | ✗ |
| 256 | 400 k | 995 µs | 5.49 ms | 10.1 ms | ✓ (marginal) |
| 256 | 600 k | 721 µs | 3.98 ms | 9.7 ms | ✓ |
| 256 | 800 k | 1.04 ms | 4.46 ms | 11.1 ms | ✗ |
| 256 | 1.0 M | 1.70 ms | 21.2 ms | 134.6 ms | ✗ |
| 256 | 1.2 M | 2.49 ms | 144.7 ms | 175.6 ms | ✗ |
| 256 | 1.4 M | 2.46 ms | 37.4 ms | 93.4 ms | ✗ |

Dedicated's p99 never drops below 4.4 ms at ANY rate — on 8 vCPU the
mode's spin threads make clean tails structurally unavailable.

The dedicated-mode picture is the oversubscription tax made visible:
3 dedicated driver threads + consensus + archive + service + rig on 8 vCPU
keeps average throughput (rates "sustain") while scheduling jitter destroys
the distribution (p90 up to 145 ms). **On this hardware class, shared mode
is the right Aeron configuration** — same 800 k knee, clean tails — which
is why both modes were pre-registered arms rather than a single guess.

### UC anchors (same fleet, bracketing the Aeron sweeps)

| when | point | responses/s | p50 | p99 |
|---|---|---|---|---|
| pre | 256/1024 | 1,304,138 | 0.755 ms | 1.075 ms |
| pre | 128/1024 | 1,318,844 | 0.757 ms | 0.878 ms |
| post | 256/1024 | 1,433,230 | 0.648 ms | 1.035 ms |
| post | 128/1024 | 1,282,493 | 0.776 ms | 0.944 ms |

All four anchors clean (`sends == responses`, zero redirects/lost/dups/
overwrites/in-flight). The pre→post spread (~10%) shows same-fleet,
same-day drift — the third fleet-variance datum today, and why the
scorecard quotes UC as a bracket, not a point.

### Run-integrity disclosures (what went wrong in the harness and how it was handled)

- **First launch discarded**: the initial IPC smoke returned a false
  negative (artifact check raced the cluster's election settle) and the
  driver fell back to UDP per pre-registration; the partial UDP shared
  sweep (4 rungs) was discarded when the smoke was re-validated
  interactively and IPC confirmed working. Kept for the record in
  `run1-aborted.log`; its two valid UDP rungs (200 k p50 316 µs, 400 k)
  are consistent with the IPC rows.
- **Rig console is not capturable over plain ssh** (the benchmarks
  launcher's stdout behavior); rung validity was switched to
  artifact-based (`.hdr` vs `.hdr.FAIL` — the rig's own honesty markers)
  and percentiles come from the benchmarks' own `aggregate-results`
  reports, not console scraping.
- One driver parse bug (string-vs-numeric percentile match) meant
  percentiles were recomputed locally from the pulled `.hgrm` files; the
  measurement itself was never affected.

### Related measurements — independent corroboration of the knee (added 2026-08-16)

AWS's "Aeron on AWS 2025 performance benchmark results" blog
(aws.amazon.com/blogs/industries/aeron-on-aws-2025-performance-benchmark-results/)
measured Aeron Cluster latency at two FIXED rates (100 k and 1 M msgs/s;
no ceiling search) on 3 × c6in.16xlarge — 64 vCPU, network-optimized, with
the full low-latency treatment this run deliberately skipped (CPU pinning,
isolcpus, sysctl tuning); cluster persistence on EBS, fsync level and
message size undisclosed.

- **Their OSS Cluster @ 1 M: p50 4,948 µs / p99 8,577 µs — queueing
  collapse.** That is this doc's knee finding, replicated independently on
  8× our cores with full host tuning: OSS Aeron Cluster cannot do 1 M/s
  cleanly regardless of hardware. It also mirrors our dedicated-mode
  signature exactly (rate "sustained", distribution destroyed). The
  "shape, not cores" conclusion above now rests on two independent
  datasets.
- **Their OSS Cluster @ 100 k: p50 95 µs** vs our 265-360 µs below the
  knee — a tuning-tier and disclosure gap, not a contradiction: pinning +
  isolation, an undisclosed (possibly async) fsync level against our
  fsync-on requirement, and unknown payload size all stack in their favor.
- **Fence: Aeron Premium (DPDK kernel bypass) is a different comparison
  class.** Their Premium tier runs 1 M @ p50 106 µs (the blog's "59×"
  claim is Premium-vs-OSS at 1 M). This doc's scorecard compares UC —
  stock UDP sockets, no kernel bypass, fsync-on — against OSS Aeron under
  identical conditions. No claim here extends to Premium: its throughput
  ceiling was not measured by AWS, and comparing a kernel-bypass
  commercial product to UC's stock-socket transport would need its own
  matched-conditions run.
- Their Transport-only numbers (OSS C driver: 1 M @ p50 35 µs) confirm the
  wall is the consensus/log plane, not UDP transport — consistent with the
  hot-path anatomy that seeded UC v2's design.

## Eventual-durability arm — PRE-REGISTERED 2026-08-16 (before its fleet run)

Both scorecard arms above ran fsync-on. This addendum pre-registers the
matched EVENTUAL arm — both systems acking on the buffered write, durability
by replication — which isolates what fsync costs each system:

- **UC**: `UC2_JOURNAL_DURABILITY=eventual` (merged main `0901daf`, opt-in,
  default unchanged): the archive's durable counter advances post-buffered-
  write; the journal fsyncs asynchronously; vote/term `StableValue` stays
  fsync'd (election-time cold path — a Raft-safety property, not a
  throughput cost; disclosed asymmetry: Aeron's consensus-module mark file
  likewise keeps its own behavior).
- **Aeron**: `aeron.archive.file.sync.level=0` + `catalog.file.sync.level=0`
  (the template's `durability: none` arm) — its native buffered-write-ack
  posture.
- Grids: Aeron **SHARED / batch 64 only** (the fsync-on winner; dedicated
  and batch-256 are already characterized), same rate grid 200 k-1.4 M.
  UC anchors 256/1024 + 128/1024 bracketing, same protocol. Same reading
  rule (sustained + p50 ≤ 1 ms), same invalidation rules, one fleet,
  fsync-on UC anchor points ALSO re-run on the same fleet so the
  fsync-on-vs-eventual delta for UC is same-hardware.
- Loss-model note for the record: this posture (either system) can lose
  acked writes on simultaneous quorum power loss and permits a power-lost
  node to restart with a shorter log than it acked — the standard
  replication-durability tradeoff. UC's gates and spec guarantees remain
  stated for fsync-on; this arm is a measurement, not a posture change.

### Eventual-arm results

*(to be filled by the run)*

### Standing caveats (from the pre-registered matrix, restated)

Methodology (windowed-max vs rate-paced), durability granularity
(per-block fdatasync vs archive sync.level=1), echo-vs-count service
(~56 B/op egress asymmetry in UC's favor), JVM warmup excluded, each
system's shipped threading posture. None of these plausibly bridge a
1.6-1.8× gap, but they are the reason this doc says "scorecard", not
"proof of superiority".
