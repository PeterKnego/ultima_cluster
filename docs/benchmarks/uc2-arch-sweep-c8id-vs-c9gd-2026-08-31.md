# CPU-architecture sweep: c8id (Intel Xeon 6) vs c9gd (Graviton) — METHOD PRE-COMMIT

*Method committed 2026-08-31 BEFORE any c9gd run, per the honest-failure
protocol: the arms, rep counts and reported statistics below are fixed now;
results are appended, never selected.*

## Question

Does UC's throughput/latency profile on the direct shmem path carry across
CPU architectures at a fixed instance shape — and does the "4 cores, one per
polling agent" answer from
[`uc2-node-core-count-sweep-2026-08-31`](uc2-node-core-count-sweep-2026-08-31.md)
hold off x86? This is user-facing hardware guidance, not a gate: **no
pass/fail bar**, but everything reported is pre-declared here.

## Arms

| | Intel arm (baseline, reused) | Graviton arm (new) |
|---|---|---|
| type | `c8id.4xlarge` | `c9gd.4xlarge` |
| CPU | Xeon 6975P-C, 3.9 GHz, **8 cores × 2 SMT** | Graviton, 3.3 GHz, **16 cores, no SMT** |
| RAM / disk | 32 GiB / 950 GB instance NVMe | 32 GiB / 950 GB instance NVMe |
| fleet | 3 voters, one placement group, us-east-1 | same |

The instance *shape* matches exactly (vCPU count, RAM, NVMe); the **SMT
asymmetry does not** — 16 vCPU is 8 physical cores on c8id and 16 on c9gd.
That is a property of the hardware being compared, not a defect of the
method: pinned arms w1..w6 use whole physical cores on both (so "w4" = 4
real cores on each), while "unpinned" means all-8-cores on Intel and
all-16 on Graviton. Both readings are stated with the results.

## Protocols (identical to the 2026-08-31 c8id runs)

Driver `bench-infra/scripts/m14_core_sweep.py`, direct shmem path (no
gateway), payload 64 B, inflight 4096, 12 s arms, service pinned core 6,
client core 7, reps interleaved. New `--topology 16x1` declares the
Graviton layout; the driver verifies it against `lscpu` on every voter and
refuses to run on a mismatch (same fail-closed posture as before).

1. **Core-count sweep**: widths 1..6 + unpinned, **3 reps** (21 arms) —
   mirrors the c8id sweep.
2. **Spread probe**: w6 + unpinned, **8 reps each**, `--timeline` on
   (16 arms) — mirrors the regime probe; 8 reps because that run showed
   n=4 cannot distinguish distribution width from a tail.

Reported per protocol, pre-declared: mean rate + [min..max] per width;
all p50s sorted; p50/rate spans pinned vs unpinned; per-second timeline
warmup climb. Rates include the 3–5 % warmup climb (this driver has no
steady-window support) — comparable arm-to-arm and with the c8id baseline
docs, **not** with M14 gate rows a/b/e.

## Gate before any bench arm: first-ever aarch64 execution

aarch64 binaries have been *built* in CI since M12d but **never executed
anywhere**. The lock-free rings (SPSC/MPSC/Broadcast) and the log buffer's
atomic-after-write framing run on a weakly-ordered memory model for the
first time — the exact class where x86's TSO hides defects (the
Broadcast-ring seqlock bug loom found in 2.10.0 is this class). So, in
order, on a c9gd voter before any measurement:

1. `cargo test --workspace` (release profile for the ring/loom-adjacent suites' timing),
2. `cargo test -p uc_node --test lin_v2` and `--test lin_partition_v2`,
3. `cargo test -p uc_crashtest --features hard-crash-tests`.

**A failure stops the bench** and becomes the finding.

## Baseline rows reused (Intel arm — measured 2026-08-31, cited not re-run)

From the regime probe (main `04ee1d8`): w6 pinned n=8 rate 3.04 M [2.95 ..
3.12], p50 1.264 ms [1.251 .. 1.281]; unpinned n=8 rate 2.68 M [2.45 ..
2.99], p50 0.917 ms [0.361 .. 1.290]; p50 span 0.030 ms pinned vs 0.929 ms
unpinned. From the core sweep (main `2737805`): plateau at w4, no gain past
w5. *Provenance caveat*: the c9gd arm builds current `main` (a few
docs/test-only commits later); same-source rebuild resolution is ~1 %
(`uc2-m14c-client-hop-2026-08-28`), well under any plausible arch delta.

## Results

### Correctness gate: PASS — with one real finding

First-ever aarch64 execution (c9gd.4xlarge node0, rustc 1.96.0 aarch64, the
repo toolchain pin, release profile): workspace **1449 passed / 0 failed**,
`lin_v2` 14/14 (171 s), `lin_partition_v2` 8/8, hard-crash 6/6 (real
SIGKILL). Two deviations, both adjudicated:

1. **`failover::contested_first_election_first_block_truncation_recovers`
   failed 10/10 isolated** — NOT the known x86 workspace-only flake (that one
   is green isolated). Instrumentation showed every attempt losing the test's
   isolate-before-ship race: the leader's NewTerm replicated and committed
   (`commit=64` vs `NEW_TERM=32`) before the partition landed, 264/264
   attempts, where x86 wins ~50/50. **A test-construction defect, not a
   product defect** — elections, replication and commit all behaved
   correctly. Fixed by construction: `PartitionHandle::block_except_election`
   (a kind-selective muzzle in `uc_net`'s fault layer — election datagrams
   pass, all else drops, unit-tested) plus a designated term-1 winner with an
   80–120 ms election band. Red→green on ARM: 0/10 → **10/10**; x86 workspace
   re-run 1449/0 (and the previously-documented x86 whole-workspace flake of
   this test did not reproduce in that run — the deterministic construction
   plausibly retires it).
2. **`remote_lin_envelope_off`** failed once, mode = checker `Inconclusive`
   — the documented open flake (ledger: ~2/9 on x86); an ARM re-rep went
   Inconclusive at the 5 M default budget (180 indeterminate vs the 117–142
   x86 baseline band) then **Linearizable** at 24 M of the 50 M escalated
   budget. Same behavior as x86, no violation.

No weak-memory defect surfaced: the lock-free rings, seqlock reads and
atomic-after-write framing all held on real weakly-ordered hardware.

### Spread probe — the cross-arch table (n=8 per config, 12 s arms, zero lost)

| | c8id.4xlarge (Intel, 8c×2SMT) | c9gd.4xlarge (Graviton, 16c) |
|---|---|---|
| rate, w6 pinned | 3.04 M [2.95 .. 3.12] | 3.04 M [2.94 .. 3.13] |
| rate, unpinned | 2.68 M [2.45 .. 2.99] | **3.79 M [3.55 .. 4.17]** |
| p50, w6 pinned | 1.264 ms [1.251 .. 1.281] | **0.291 ms [0.282 .. 0.299]** |
| p50, unpinned | 0.917 ms [0.361 .. 1.290] | 0.382 ms [0.273 .. 0.900] |
| p50 span, pinned vs unpinned | 0.030 vs 0.929 ms (31×) | 0.017 vs 0.627 ms (37×) |
| p99, w6 pinned | *not recorded*¹ | 0.912 ms [0.880 .. 0.931] |
| p99, unpinned | *not recorded*¹ | 0.646 ms [0.383 .. 1.002]² |
| warmup climb (first 3 s → last 3 s) | +3.2 % / +5.1 % | **−0.6 % / −0.3 %** |

(Intel column: `uc2-regime-probe-2026-08-31`, measured the previous day, same
driver and protocol. ¹ The Intel run's POINT lines carried p99 but the doc
published only rate/p50 and the raw logs were not retained — an absence, not
a failed measurement; one cross-metric comparison still stands: Graviton's
pinned **p99** (0.912 ms) is below Intel's pinned **p50** (1.264 ms) at equal
throughput. ² Unpinned p99s split into a 0.38–0.46 ms group (5 arms) and a
~1.00 ms group (3 arms, values 0.999/1.001/1.002 — suspiciously exactly 1 ms,
possibly a timer-granularity mode); the ~1 ms p99 group does NOT coincide
with the one slow-p50 arm — two of its three members have fast p50s. Recorded,
not explained.)

Three findings:

1. **Pinned-to-6-cores throughput is identical across architectures**
   (3.04 M/s on both — treat the exactness as coincidence, the equality as
   real), but **unpinned inverts**: Intel *loses* 12 % to a long slow tail
   when unpinned, Graviton *gains* 25 % over its own pinned rate — with 16
   real cores and no SMT siblings, the scheduler roaming beats any 6-core
   straitjacket. Pinning on this host costs 20 % of throughput and buys the
   same thing it bought on Intel: variance (p50 span 37× tighter). The
   pinning story generalizes as **"pinning trades throughput for variance",
   not "pin on N cores"** — and the trade's price is topology-dependent.
2. **p50 latency is ~4.3× lower on Graviton at equal throughput** (0.291 vs
   1.264 ms, w6 pinned). The low-latency mode Intel only reaches in its
   fast-tail unpinned arms (0.36 ms) is roughly where Graviton sits *all the
   time* (0.27–0.35 ms in 7/8 unpinned arms; one 0.90 ms tail arm shows the
   tail mechanism exists there too, milder).
3. **The 3–5 % warmup climb is an Intel artifact, not a harness property** —
   Graviton arms are flat-to-slightly-declining. The comparability caveat
   ("non-steady-window drivers include the climb") still binds Intel numbers
   but adds ~nothing on this host. Levels are still set in second 0 and held
   (no mid-run transitions), matching the Intel timeline finding.

### Core-count sweep on c9gd (widths ×3 reps, interleaved, zero lost)

| arm | cores | mean resp/s | [min .. max] | spread | vs w1 |
|---|---|---|---|---|---|
| w1 | 1 | 2,647,399 | [2,615,820 .. 2,688,811] | 2.8 % | 1.00× |
| w2 | 2 | 3,233,637 | [3,165,191 .. 3,309,974] | 4.5 % | 1.22× |
| w3 | 3 | 3,270,233 | [3,237,000 .. 3,296,467] | 1.8 % | 1.24× |
| w4 | 4 | 3,024,960 | [2,991,043 .. 3,072,472] | 2.7 % | 1.14× |
| w5 | 5 | 3,068,695 | [3,024,868 .. 3,119,401] | 3.1 % | 1.16× |
| w6 | 6 | 3,088,217 | [3,058,860 .. 3,129,282] | 2.3 % | 1.17× |
| unpinned | all 16 | 3,437,529 | [3,171,225 .. 3,665,534] | 14.4 % | 1.30× |

**The Intel core-count answer does not carry over.** On c8id the node needed
4 cores (one per polling agent) and was flat past 5. On c9gd the plateau is
at **2–3 cores** — one Neoverse-V3 core runs the whole four-agent node at
2.65 M/s, 87 % of the six-core rate — and w4–w6 sit a consistent ~6 % *below*
w2–w3, outside every per-width spread (no causal story offered; the arms are
interleaved so session drift cannot produce it). Unpinned-over-16 beats every
pinned width. Practical guidance for this host class: **don't pin, and don't
budget four cores per node — two suffice at this operating point.**

### Scope and caveats

- One payload (64 B), one inflight (4096), **direct shmem path** — the
  client is `uc_client::Engine` on the leader host; no gateway, no crypto;
  3-voter placement-group fleet, us-east-1, 2026-08-31/09-01. A remote
  client through a gateway measured 0.62× direct (one connection) / 0.84×
  (N=16 aggregate) on the c6id generation
  ([M13 gate](uc2-m13-gate-2026-08-24.md)); that ratio has NOT been
  re-measured on these hosts.
- The Graviton tree carried one local fix over the Intel baseline's commit:
  the failover-test construction + fault-layer muzzle (test-only + fault
  layer; nothing on the measured path). Same-source rebuild resolution is
  ~1 % (`uc2-m14c-client-hop-2026-08-28`) — irrelevant at these deltas.
- "Unpinned" spans different hardware on each arm (all 8 vs all 16 cores) —
  that asymmetry IS the product being compared, per the method section.
- Rates are 12 s whole-arm (no steady window). On Intel that embeds a
  +3–5 % climb; on Graviton the climb measured −0.6 %/−0.3 %, so the c9gd
  numbers are effectively steady-state as-is. Not comparable to M14 gate
  rows a/b/e.
- Raw logs: 21-arm sweep + 16-arm probe with per-second timelines and
  `SUMMARY-JSON`, job dir `c9gd-sweep.log`/`c9gd-probe.log` (transcribed
  here in full as the tables above).

## Reproducing

Needs your own cloud account (fleet runs cost real money) or any three Linux
hosts you already have. `bench-infra/README.md` covers the control-machine
toolchain; credentials go in a gitignored `bench-infra/.env`, host/type
choices in `terraform.tfvars` (start from `example.aws.tfvars`).

```bash
cd bench-infra
# terraform.tfvars: cloud = "aws", instance_type = "c9gd.4xlarge", node_count = 3
# (the AMI architecture derives from the instance type automatically)
make up-uc

# correctness first — on one host, the ARM gate this doc describes:
#   cargo test --release --workspace && the lin_v2 / lin_partition_v2 /
#   uc_crashtest --features hard-crash-tests tiers (docs/how-to/reproduce-a-result.md)

python3 scripts/m14_core_sweep.py --fleet --topology 16x1     --widths 1,2,3,4,5,6 --reps 3 --secs 12          # 21-arm core sweep
python3 scripts/m14_core_sweep.py --fleet --no-sync --topology 16x1     --widths 6 --reps 8 --secs 12 --timeline         # 16-arm spread probe

make destroy && terraform -chdir=terraform state list   # must print nothing
```

For the Intel arm use `instance_type = "c8id.4xlarge"` and `--topology 8x2`.
On hosts of any other core layout the driver refuses to run until the layout
is added to `TOPOLOGIES` — that is the fail-closed pin-map check working, not
a bug. Hosts you provisioned yourself (no terraform) are passed as
`--hosts pub1/priv1,pub2/priv2,pub3/priv3` and need the ansible `provision.yml`
prep (rust toolchain under `/opt/bench/.cargo`, the tree at `/opt/bench/uc`,
instance dirs on a real filesystem — never tmpfs). Compare against the tables
above per `docs/how-to/reproduce-a-result.md`: match the hardware class or
state which way your difference cuts, and fix rep counts before reading
spreads (n=4 cannot tell a distribution's width from a tail).
