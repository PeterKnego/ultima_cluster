# How much of SMR latency is the network, how much is IPC, and how much is the stack

*Written 2026-09-16 from three sources: UC's own benchmark record (v1 and
v2), Adaptive's published Aeron numbers (the Google Cloud guide of February
2024, the AWS blog of February 2026, and the `aeron-io/benchmarks` harness),
and the ABTRDA3 bare-metal NIC campaign recorded in `docs/BACKLOG.md` § 6.
Every number below is quoted from the cited document. The one measurement
this note prompted, the same-day service-time run, is
`docs/benchmarks/uc2-service-time-2026-09-16.md` and is quoted in § 1e.*

## The short answer

In a single cloud placement group, the wire round trip is a few tens of
microseconds. A tuned SMR stack with no fsync adds roughly the same again on
top of it. Everything above ~100 µs in a published SMR latency is either
queueing (the client's window divided by the throughput) or the stack's own
choreography: fsync, thread hand-offs, and consensus bookkeeping.

- **UC v1 measured this directly** (2026-06-25): 10 % wire, 18 % fsync,
  ~73 % software structure, on a 1.88 ms floor.
- **UC v2 had never measured it until this note prompted the run.** Every
  v2 latency in the gate docs was a saturation point that Little's Law
  predicts to three figures. The 2026-09-16 service-time run (§ 1e) now
  puts the shipped path at ~115 µs p50 with fsync on, of which the wire is
  at most 29 %, fsync 27 µs, and the service's idle sleep a second mode
  worth 77 µs at p90.
- **Aeron's published numbers contain the decomposition implicitly**,
  because Adaptive benchmarks the transport alone and the cluster on the
  same hosts. The cluster's cost over the transport is ~55–65 µs at p50 on
  AWS in 2025, with archive sync almost certainly at level 0 (page cache).
- **Kernel bypass buys little RTT and a lot of tail.** At 100 k msg/s on
  AWS, DPDK moved Aeron's transport p50 by −3 µs (the wrong way) and its p99
  by 3 µs. At 1 M msg/s it is the difference between an 8.5 ms and a 143 µs
  cluster p99. The win is the absence of syscalls and interrupts under load,
  not a faster packet.

## 1. What UC's own record says

### 1a. The v1 floor decomposition (the only direct measurement)

`git show 7d62e2f:docs/benchmarks/floor-decomposition-2026-06-25.md`, 3 ×
c6id.2xlarge, QUIC, 64 B, inflight 1, layered black-box subtraction:

| bucket | derivation | p50 | share of 1.88 ms |
|---|---|--:|--:|
| base (IPC rings + openraft commit→apply + apply) | 1-node eventual | 0.861 ms | 46 % |
| replication (QUIC RTT + remote append to majority) | 3-node − 1-node | 0.704 ms | 37 % |
| fsync (local NVMe journal) | consistent − eventual | 0.331 ms | 18 % |

Raw LAN ping between the nodes was 0.186 ms, so of the replication bucket
only ~0.19 ms was wire and ~0.52 ms was openraft's async choreography.
Re-cut by nature: **~73 % software/structural, ~27 % physical (fsync +
wire)**, and the wire alone ~10 %. The doc's verdict: "NOT wire-bound … busy-
poll on the network was always going to be null."

### 1b. The transport A/Bs that agreed with it

- `docs/tasks/task17_inter_node_latency.md`, commit `0b3cf90`: "the network
  was never the bottleneck — fsync/IPC dwarf RTT." Cross-host busy-poll of
  the datapaths (Phase B) was a measured null.
- `docs/tasks/task16_inter_node_udp_transport.md` § 6.6, 3 × c7i.4xlarge in
  a placement group, single inflight, 64 B: UC-QUIC p50 0.091 ms, UC-UDP
  0.098 ms, Aeron echo on the same fleet p50 0.047 ms / p99 0.066 ms. On
  Hetzner's LAN the same probe read ~0.31–0.34 ms, so the placement group is
  ~3× faster than a commodity LAN. Transport choice (QUIC vs UDP) flipped
  sign between the two networks and was called "not universal".
- `docs/tasks/task18_busyspin_ring_consumers.md` § 4a: busy-spinning the
  intra-host ring consumers removed ~40 µs per commit and measured **null**
  on a fleet whose floor was 0.85–1.4 ms: "~3–5 % of a millisecond-scale
  commit and invisible under the noise."

### 1c. What IPC costs, in isolation

- Futex wake ~8.8 µs, busy-spin hand-off ~29 ns, a ~300× gap
  (`git show 369fc4f:docs/benchmarks/aeron-vs-uc-threading-copying-2026-06-21.md`).
  Copies at KV sizes are 4–40 ns and "a near-non-issue".
- Replacing poll-sleep with futex wakeups on v1's four commit-path rings cut
  the inflight-1 floor 5.02 → 1.08 ms, a 4.6× win
  (`docs/tasks/task11_event_driven_ring_wakeups.md`). IPC latency matters
  enormously **when it is a sleep**, and hardly at all once it is a spin.
- v2's shmem client hop alone does 2.76–2.80 M resp/s at p50 0.001 ms
  (`docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md`): "Hop 1 is ≥ 2× the
  cluster's own commit ceiling; it is not where any of the remote path's
  throughput goes."
- The two real v2 IPC defects were both scheduling pathologies, not latency:
  the MPSC publish convoy under CPU oversubscription (M13, 30–100× collapse,
  fixed by per-record commit) and the lockstep yield storm (M14c2, ~880×
  collapse, recorded as an operating-envelope fact).

### 1d. v2 is not network-bound at its throughput ceiling

`docs/benchmarks/uc2-m12-gate-2026-08-22.md`, "Network budget
characterization", 4 × c6id.2xlarge: peak 1.42 M resp/s drove 3.21 Gbps and
393 k pkt/s, "roughly a quarter of the instance's ~12.5 Gbps burst ceiling";
replication is batched at ~0.28 packets and ~275 B per committed command;
the TCP RTT floor on that hardware is p50 35.8 µs (hi-perf-cmp). "The
~1.4 M/s ceiling is software (the single apply thread / consensus), not the
NIC."

### 1e. The gap, closed the same day: the v2 service-time number

Until this note was written, every v2 latency figure had been taken at a
client window that binds:

| fleet / doc | throughput | p50 | note |
|---|--:|--:|---|
| 3 × c6id.2xlarge, M5 gate | 1.64 M/s | 0.600 ms | W=1024, fsync on |
| 3 × c6id.2xlarge, parity 2026-08-15 | 1.28–1.43 M/s | 0.65–0.78 ms | W=1024, fsync on |
| 4 × c6id.2xlarge, M12, inflight 256 | 518 k/s | 0.472 ms | 256/518 k = 0.494 ms |
| 3 × c9gd.2xlarge (Graviton), M13-on-ARM | 3.41 M/s | 0.235 ms | direct engine |

None of these is a service time. **The measurement ran on 2026-09-16**
(`docs/benchmarks/uc2-service-time-2026-09-16.md`, 4 × c6id.2xlarge,
closed loop at inflight 1, five arms):

| posture | mean | p50 | p90 |
|---|--:|--:|--:|
| shipped (fsync on, unpinned, apply agent sleeps 50 µs) | 166 µs | 123 µs | 219 µs |
| pinned | 124 µs | 109 µs | 194 µs |
| pinned + apply agent spins (`UC2_APPLY_IDLE=spin`) | 111 µs | 105 µs | 117 µs |
| pinned + spin + fsync off the path (eventual, 1 ms) | 85 µs | 77 µs | 98 µs |

Raw UDP round trip on the same fleet: 33.5 µs p50. So the wire is at most
44 % of the no-fsync path and at most 29 % of the shipped p50; fsync is
27 µs; the service's idle sleep is a second mode worth 77 µs at p90; and
thread placement is 41 µs of mean. The net-decomp brief's kernel-bypass
clause needed the wire to be ≥ 80 µs absolute — the whole round trip is
33.5 µs, so that clause is closed by the bound without building the
instrument. The brief's cheap-ladder clause (syscall share) stays open.

## 2. What Aeron's published numbers say

### 2a. The `aeron-io/benchmarks` repo publishes no results

It is a harness. Verified 2026-09-16: no `results/`, no CSV or PNG, no
releases, wiki uninitialised. What it does fix is the method every Adaptive
number below was taken with:

- RTT, not one-way: the client stamps `nanoTime` into the payload, the
  cluster's `EchoClusteredService` offers the same buffer straight back on
  the session, and the client records `now − stamp` on egress.
- HdrHistogram at 3 significant digits, 60 one-second measurement windows
  after 30 warm-up seconds, three runs, `BusySpinIdleStrategy` everywhere,
  media driver `DEDICATED` with conductor `spin` and sender/receiver `noop`,
  `aeron.dir` on `/dev/shm`, every thread pinned by name with `taskset`,
  every process wrapped in `numactl`.
- Default sweep: rates 501 k / 301 k / 101 k paired with lengths 32 / 288 /
  1344 B, burst 1, MTU 1408.
- **`--file-sync-level` defaults to 0** in `remote-cluster-benchmarks`
  (`file_sync_levels=(0)`), and `low-latency-archive.properties` sets
  `aeron.archive.file.sync.level=0`. Level 0 is `pwrite` into the page cache
  with no sync. The Google Cloud guide's worked example invokes the script
  with `--file-sync-level 0` explicitly (p. 18). The AWS blog does not state
  the level it used.

### 2b. Google Cloud, February 2024 (c3-highcpu-88, compact placement, one rack)

Round trip at 100 k × 288 B msg/s, µs:

| | transport p50 | transport p99 | cluster p50 | cluster p99 | cluster − transport, p50 |
|---|--:|--:|--:|--:|--:|
| Java (OSS, BSD sockets) | 32 | 57 | 85 | 109 | 53 |
| C (OSS) | 32 | 73 | 89 | 112 | 57 |
| C + DPDK (Premium) | 13 | 18 | 30 | 36 | 17 |
| C + ATS encryption | 36 | 146 | 99 | 130 | 63 |
| C + ATS + DPDK | 15 | 41 | 37 | 45 | 22 |

Max throughput under a 1 ms p99 ceiling: transport Java 800 k, C-DPDK
4.7 M; cluster Java 250 k, C 200 k, C-DPDK 2.2 M. The multi-zone appendix
(three zones, one region) puts the cluster at p50 776–830 µs and p99
872–1 159 µs for every variant: once the wire is ~0.8 ms, bypass is worth
~25 % at p99 and nothing at p50.

### 2c. AWS, published February 2026 (c6in.16xlarge, cluster placement group, EBS, Ubuntu 22.04)

The January 2026 AWS guide PDF in `.superpowers/` contains only setup
instructions and two topology figures; the numbers are on
<https://aws.amazon.com/blogs/industries/aeron-on-aws-2025-performance-benchmark-results/>.
Message size is not stated on the page. Round trip, µs:

| 100 k msg/s | transport p50 / p99 / p99.9 | cluster p50 / p99 / p99.9 | cluster − transport, p50 |
|---|---|---|--:|
| OSS Java | 21 / 32 / 46 | 95 / 136 / 197 | 74 |
| OSS C | 22 / 43 / 60 | 97 / 139 / 203 | 75 |
| Premium (DPDK) | 24 / 29 / 31 | 76 / 98 / 106 | 52 |
| Premium + ATS | 25 / 30 / 32 | 79 / 102 / 114 | 54 |

| 1 M msg/s | transport p50 / p99 / p99.9 | cluster p50 / p99 / p99.9 |
|---|---|---|
| OSS Java | 30 / 57 / 206 | 3 301 / 8 479 / 9 306 |
| OSS C | 35 / 84 / 413 | 4 948 / 8 577 / 8 987 |
| Premium (DPDK) | 30 / 39 / 43 | 106 / 143 / 158 |
| Premium + ATS | 31 / 40 / 45 | 122 / 166 / 202 |

The blog's own framing: OSS improved 70 % (transport) and 64 % (cluster)
since the previous publication "thanks to enhancements in Aeron and AWS
infrastructure, including updated instance types, networking, and the
benchmark harness itself"; Premium now leads OSS by 33 % / 29 % at 100 k.
The methodology also changed: the 1 ms-ceiling max-throughput search was
dropped in favour of fixed 100 k and 1 M rates.

### 2d. Reading Aeron's decomposition

Three things fall out of putting 2b and 2c side by side.

1. **The cluster costs ~50–75 µs over the transport at p50**, on both
   clouds, in both years, with or without bypass. That is consensus,
   sequencing, the archive write (level 0), the service echo, and egress.
   It is the same order as the wire itself.
2. **Bypass barely moves the transport RTT on modern AWS.** At 100 k, DPDK's
   transport p50 is 24 µs against Java's 21. Its effect is on the tail
   (p99.9 31 vs 46) and on what happens at 1 M msg/s, where OSS's cluster
   p50 goes to 3.3–4.9 ms while Premium holds 106 µs. That collapse is the
   same knee UC's parity doc found for OSS Aeron on 8-vCPU hosts
   (`docs/benchmarks/uc2-aeron-parity-2026-08-15.md`: "OSS Aeron Cluster
   cannot do 1 M/s cleanly regardless of hardware").
3. **The published cluster p50 is consistent with archive sync level 0.**
   UC's parity fleet measured OSS Aeron at level 0 at p50 84–120 µs against
   265–354 µs at level 1 (fdatasync), on 3 × c6id.2xlarge with no pinning.
   Adaptive's 85–97 µs on tuned, pinned, isolated hosts sits inside the
   level-0 band. Nothing on the AWS page contradicts that, and the harness
   default is 0.

## 3. Where bare-metal NIC numbers fit

ABTRDA3 (`docs/BACKLOG.md` § 6) measures the layer below all of the above:
one frame in flight, two ports of one NIC cabled back to back, no switch,
no consensus, no persistence. Its 24 h medians are 1.9 µs (ef_vi CTPIO),
2.4 µs (verbs), 3.5–3.6 µs (DPDK on X2522 / ConnectX-4), 6.2 µs (AF_XDP on
mlx5), 9.0 µs (DPDK on i40e), 13.4 µs (igc at 1 GbE). Two readings for UC:

- **The spread between transports on the same silicon is single-digit
  microseconds.** DPDK vs AF_XDP is ~3 µs; ef_vi vs DPDK is 1.6 µs. A cloud
  placement-group RTT is 20–50 µs and a cluster round trip ~100 µs, so the
  transport choice is a 3–5 % effect on the cluster number, which is what
  Aeron's 100 k rows show and what UC's task18 predicted.
- **The NIC silicon and the PHY dominate the bare wire**: DPDK on i40e is
  9 µs because the i40e is 9 µs; the I225-V at 2.5 GbE is 3.75 µs slower
  than at 1 GbE because of the 802.3bz line code. None of this is reachable
  from an ENA-backed EC2 instance, so ABTRDA3's absolute numbers do not
  transfer; its methodology (24 h soaks, P99.999 headline, IRQ steering
  proven by read-back) does.

## 4. What this means for UC, and what is missing

- **Do not expect a transport change to move UC's throughput.** M12 shows
  the NIC at a quarter of its capacity at UC's ceiling, and every isolated
  measurement (task17, task18, M13 hop 1, M12) points at the single
  consensus/apply thread and the client's own hot path.
- **A transport change can move UC's latency floor by at most 33 µs**, the
  raw round trip, and in practice by a fraction of that. Measured
  2026-09-16 (§ 1e): the wire is at most 29 % of the shipped p50. Aeron's
  own pattern agreed in advance (the wire is ~20–30 % of a tuned, level-0
  cluster round trip). The levers that are UC's to pull are the apply
  agent's idle sleep (77 µs at p90, one env var), thread placement (41 µs
  of mean), and fsync (27 µs, a durability decision).
- **Like-for-like with Aeron, at last.** Aeron's published cluster p50 at
  100 k msg/s is 85–97 µs OSS and 76 µs Premium on tuned 64-vCPU hosts,
  almost certainly at archive sync level 0. UC's matching arm (no fsync on
  the path, pinned, spinning apply) is 77 µs on an 8-vCPU host; UC's shipped
  posture with fsync on is 112–123 µs. Same band; the gap between UC's
  shipped number and Aeron's published one is fsync plus the sleep's second
  mode, not the transport.
