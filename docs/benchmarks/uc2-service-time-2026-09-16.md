# UC v2 service time — closed-loop round trip through commit + apply + response

**Date:** 2026-09-16 (pre-registered before the fleet run; **RAN the same
day**, re-run with the ladder default 2026-09-17 — §4.5; 4 × `c6id.2xlarge`, us-east-1 cluster placement group, 55 points,
0 lost, driver exit 0; raw output in `bench-out/service-time-2026-09-16/`).
**Status:** measurement row, **no bar**. Nothing here passes or fails; the
output is a number UC has never published and the three subtractions it
makes possible.
**Driver:** `bench-infra/scripts/service_time_gate.py --fleet`.
**Why now:** `docs/notes/uc2-network-and-ipc-share-of-smr-latency.md`
(2026-09-16) put UC's benchmark record beside Aeron's and found that every
published UC v2 latency is a saturation point (`p50 = window ÷ throughput`,
`docs/notes/uc2-latency-throughput-explained.md`), while Aeron's 100 k msg/s
rows and ABTRDA3's one-in-flight soaks are service times. Comparing the two
kinds of number is comparing queueing against the path. This run produces
the path.

## 1. What is measured

On a fresh 3-voter cluster (typed `CountSm`, envelope off, 64 B payload,
crypto off — the M5/M12 shape), the direct shmem client on the leader host
runs **one command in flight**: submit, wait for the response, submit the
next. Each response has crossed the full commit path — ingress ring → leader
append → DATA to both followers → follower `fdatasync` → `APPEND_POSITION`
back → commit → apply on the leader's service → egress broadcast → client
matcher. The round-trip histogram at inflight 1 is the service time. A short
inflight ladder (1, 2, 4, 8, 16, 64, 256) shows where queueing takes over,
tying the number back to the M12 row-1 ladder that started at 1 but whose
inflight-1 point was never reported.

Five arms, fixed order, one fresh cluster each:

| arm | durability | pinned | apply idle | isolates |
|---|---|---|---|---|
| A | consistent (fdatasync per block) | no | `Sleep(50 µs)` (shipped default) | the shipped posture, as an operator gets it |
| B | consistent | `PIN_MAP_C6ID_2XL` | `Sleep(50 µs)` | A−B = thread placement / SMT-sibling noise |
| C | consistent | pinned | `UC2_APPLY_IDLE=spin` | B−C = the service's idle sleep on the response path |
| D | eventual, 1 ms interval | pinned | spin | C−D = the quorum `fdatasync` on the commit path |
| A′ | consistent | no | `Sleep(50 µs)` | drift bracket: A repeated last |

What is left in arm D is the wire round trip plus every agent's duty-cycle
latency. Splitting those two is the job of
`docs/superpowers/specs/2026-08-02-uc2-net-decomp-brief.md`'s skew-free
WIRE(P) instrument, which this run does not build. This run bounds it: a raw
UDP ping-pong (`hi-perf-cmp`'s `network-rtt-udp`, 64 B, one in flight,
built on the hosts) between two cluster hosts is measured before and after
the arms, and the report states one raw round trip as a share of arm D's
p50. That share is an **upper bound** on what any faster transport could
recover from the service time.

Reps: three at inflight 1 and 2 per arm (the median is the arm's number,
the spread is reported), one at each higher rung. 20 s per point, 2 s
warm-up inside the client's window.

## 2. Two knobs this run needed, both opt-in, both off by default

- **`UC2_APPLY_IDLE`** (new, `uc_service`): the apply agent's idle strategy
  — `spin`, `yield`, or `sleep:<micros>`; unset keeps the shipped
  `Sleep(50 µs)`; anything else is refused by name. The apply agent is the
  only agent in the commit path that sleeps (every `uc_node` agent yields),
  so at low load its sleep plus the kernel's timer slack sits directly on
  every response. Arm C measures exactly that. The knob is a per-process env
  override in the same family as `UC2_JOURNAL_DURABILITY`, because pegging
  a core per service is a capacity decision, not cluster state.
- **`m12_gate client-direct --poll-idle spin`** (harness only): the client's
  poll thread spins instead of sleeping 20 µs on an empty poll. Every arm
  runs with it, so the harness's own sleep never lands in the number — a
  harness that sleeps on the response path measures itself (the M14a
  lesson, `docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md`).

Neither changes any default. Row 1 of the M12 gate, every M14 arm, and the
release binaries behave byte-for-byte as before.

## 3. Threats, stated before the run

1. **The client observes from the leader host.** The direct client shares
   the leader's 8 vCPUs with the node's five agents and the service. Arm B
   pins it to the fourth core's two threads (`client: "3,7"`), disjoint
   from the node (`0,1,4,5`) and service (`2`). Arm A does not, and its
   A−B delta is the price.
2. **A three-node quorum commits on the faster follower.** The service time
   is the faster of two round trips, not the mean. Reported as is; the
   net-decomp brief's attribution rule (§4.2) is what would name which.
3. **fdatasync on instance-store NVMe** (`c6id`): arm D removes it from the
   path by making it a 1 ms background cadence; the parity doc's
   eventual-arm tuning (`docs/benchmarks/uc2-aeron-parity-2026-08-15.md`,
   interval re-test 2026-08-17) is why 1 ms, not the 50 ms default.
4. **Fleet-day drift.** A′ brackets A. If A′'s inflight-1 p50 differs from
   A's by more than the arms' intended deltas, the subtractions are
   reported with that caveat, not sharpened.
5. **This is the ops-bound, 64 B regime only.** Nothing here speaks for
   large payloads.
6. **Local smoke is not evidence.** The in-process harness on the dev box
   verified wiring only: with the shipped posture its inflight-1 p50 sat at
   one quorum `fdatasync` of the box's consumer NVMe (a raw probe put one
   sync at ~3 ms), fell to sub-millisecond with eventual durability, and
   fell again with `UC2_APPLY_IDLE=spin` — the arms separate in the
   intended order. Those values are the box's, not the fleet's, and do not
   appear below.

## 4. Results

Fleet: 4 × `c6id.2xlarge` (8 vCPU, instance-store NVMe), us-east-1 cluster
placement group, Ubuntu per `bench-infra` ansible, tree = the working tree
this doc was committed from (rsync mode). Leader was n0 in every arm. Driver
log: `bench-out/service-time-2026-09-16/run.log`; per-point JSON:
`points.jsonl`; arm medians: `summary.json`.

### 4.1 Raw UDP round trip, same fleet

`network-rtt-udp`, 64 B, one in flight, responder on n1, client on n0,
both unpinned, 200 000 samples after 20 000 warm-up:

| when | p50 | p99 | mean |
|---|--:|--:|--:|
| before the arms | 33.5 µs | 43.3 µs | 33.9 µs |
| after the arms | 33.3 µs | 40.9 µs | 33.6 µs |

Stable to 0.2 µs across the ~70-minute run. This agrees with the
hi-perf-cmp TCP figure the M12 gate doc cites for the same instance class
(p50 35.8 µs).

### 4.2 The ladder, all arms

Driver report block, verbatim (p50/p90/p99/max in ms; `B/cmd`, `pkt/cmd`
are the leader's NIC tx per committed command):

```
  arm inflight rep     resp/s      p50      p90      p99      max  lost   B/cmd  pkt/cmd
    A        1   1       5762    0.209    0.222    0.328    8.430     0   661.7    6.095
    A        1   2       6037    0.123    0.219    0.289   17.842     0   652.7    6.011
    A        1   3       6149    0.117    0.218    0.325   17.154     0   632.1    5.822
    A        2   1      10599    0.216    0.222    0.329   22.020     0   559.4    4.699
    A        2   2      10226    0.216    0.222    0.341   23.167     0   590.2    4.946
    A        2   3      10208    0.216    0.225    0.542    7.516     0   573.5    4.845
    A        4   1      17884    0.220    0.234    0.554    6.541     0   529.1    4.000
    A        8   1      34467    0.220    0.321    0.354    6.185     0   404.5    2.245
    A       16   1      60016    0.232    0.341    0.408   24.904     0   349.9    1.338
    A       64   1     174253    0.355    0.385    0.518   14.565     0   281.1    0.440
    A      256   1     572232    0.428    0.502    1.203   73.400     0   273.1    0.245
    B        1   1       8131    0.109    0.187    0.222    3.164     0   657.7    6.057
    B        1   2       7979    0.109    0.209    0.225   17.138     0   640.4    5.898
    B        1   3       8048    0.109    0.194    0.223   21.463     0   652.4    6.008
    B        2   1      13356    0.111    0.217    0.286   17.678     0   548.2    4.427
    B        2   2      13398    0.112    0.216    0.229   17.531     0   541.1    4.341
    B        2   3      13537    0.111    0.216    0.229   14.107     0   537.2    4.217
    B        4   1      22757    0.213    0.220    0.324    5.554     0   457.4    3.058
    B        8   1      37801    0.217    0.229    0.330   10.641     0   399.0    2.159
    B       16   1      67457    0.222    0.325    0.439   19.333     0   337.0    1.154
    B       64   1     191908    0.345    0.358    0.471   13.083     0   286.9    0.416
    B      256   1     644701    0.358    0.450    0.565   11.944     0   268.3    0.221
    C        1   1       9028    0.105    0.117    0.204   13.353     0   659.7    6.075
    C        1   2       8973    0.105    0.121    0.175   21.086     0   652.0    6.003
    C        1   3       8851    0.105    0.117    0.176   20.709     0   655.0    6.031
    C        2   1      15423    0.118    0.160    0.218   20.005     0   606.8    5.473
    C        2   2      15653    0.116    0.162    0.219   13.492     0   621.4    5.490
    C        2   3      15237    0.118    0.162    0.225   10.404     0   644.1    5.881
    C        4   1      26879    0.145    0.186    0.249   10.338     0   513.5    3.972
    C        8   1      47162    0.155    0.209    0.268   22.364     0   407.8    2.364
    C       16   1      85042    0.175    0.234    0.285   11.469     0   342.6    1.280
    C       64   1     210533    0.288    0.312    0.414   14.410     0   276.7    0.305
    C      256   1     707095    0.335    0.378    0.496   14.418     0   266.6    0.215
    D        1   1      12098    0.077    0.098    0.131   11.919     0   655.9    6.038
    D        1   2      11674    0.076    0.099    0.159   14.909     0   664.0    6.113
    D        1   3      11808    0.078    0.098    0.150   37.487     0   646.7    5.954
    D        2   1      20324    0.096    0.114    0.185   18.711     0   543.7    4.322
    D        2   2      20393    0.097    0.114    0.181   15.696     0   565.9    4.655
    D        2   3      20879    0.096    0.113    0.167   13.976     0   562.8    4.634
    D        4   1      37852    0.098    0.132    0.182   12.886     0   467.3    3.288
    D        8   1      69864    0.104    0.147    0.207   10.191     0   385.2    2.000
    D       16   1     129633    0.113    0.153    0.218   10.207     0   332.2    1.144
    D       64   1     396764    0.147    0.216    0.308    8.204     0   284.1    0.395
    D      256   1    1004861    0.217    0.351    0.462   10.346     0   276.9    0.163
   A2        1   1       6280    0.112    0.218    0.326    8.946     0   659.5    6.074
   A2        1   2       6361    0.113    0.221    0.297   31.048     0   666.9    6.142
   A2        1   3       6831    0.109    0.217    0.228   10.060     0   646.7    5.956
   A2        2   1      10839    0.217    0.221    0.329   19.087     0   561.9    4.605
   A2        2   2      10616    0.217    0.223    0.329   19.366     0   564.8    4.617
   A2        2   3      11005    0.216    0.226    0.326    6.337     0   529.4    4.196
   A2        4   1      20094    0.218    0.228    0.332    5.992     0   471.3    3.223
   A2        8   1      34867    0.220    0.328    0.342   22.774     0   387.0    1.971
   A2       16   1      60925    0.229    0.335    0.393   24.527     0   341.4    1.256
   A2       64   1     178956    0.352    0.373    0.492   13.296     0   288.0    0.431
   A2      256   1     592191    0.416    0.497    0.899   10.117     0   276.4    0.211
```

The inflight-256 points tie this fleet to the record: arm A's 572 k/s at
p50 0.428 ms sits beside the M12 row-1 point of 2026-08-24 (518 k/s at
0.472 ms, same instance class, same driver family).

### 4.3 Service time at inflight 1 and the three subtractions

Median of three reps per arm. The closed-loop **mean** is `1 / resp/s`
(one command in flight, so the rate is the reciprocal of the mean round
trip) and is reported beside the percentiles because two of the effects
below are bimodal and a p50 hides them.

| arm | posture | mean | p50 | p90 | p99 | p50 spread over reps |
|---|---|--:|--:|--:|--:|--:|
| A | shipped: fsync on, unpinned, apply sleeps 50 µs | 166 µs | 123 µs | 219 µs | 325 µs | 92 µs |
| B | + pinned | 124 µs | 109 µs | 194 µs | 223 µs | 0 |
| C | + apply agent spins (`UC2_APPLY_IDLE=spin`) | 111 µs | 105 µs | 117 µs | 176 µs | 0 |
| D | + fsync off the path (eventual, 1 ms) | 85 µs | 77 µs | 98 µs | 150 µs | 2 µs |
| A′ | = A, run last | 157 µs | 112 µs | 218 µs | 297 µs | 4 µs |

Subtractions:

| term | mean | p50 | p90 | reading |
|---|--:|--:|--:|---|
| A − B, thread placement | 41 µs | 14 µs | 25 µs | the shipped posture pays this in variance more than in median: arm A's three reps spread 92 µs at p50 (209 / 123 / 117), arm B's spread 0 |
| B − C, the apply agent's idle sleep | 13 µs | 4 µs | **77 µs** | bimodal by construction: a commit that lands while the agent sleeps waits out the sleep plus timer slack (~100 µs), one that lands while it is awake does not. Arms A and B show the two modes at ~110 and ~215 µs; arm C has one mode |
| C − D, the quorum `fdatasync` | 27 µs | 28 µs | 19 µs | one block sync on instance-store NVMe, on the faster follower |
| A − A′, drift | 9 µs | 11 µs | 1 µs | inside every effect above |

### 4.4 Reading

1. **UC v2's shipped service time is ~110–125 µs at p50 and ~160 µs mean,
   fsync on, on an untuned 8-vCPU host.** No prior doc had this number. It
   is not a saturation artefact: the ladder shows p50 flat to inflight 8 and
   queueing taking over only past 16.
2. **The path under the shipped posture is bimodal, and the second mode is
   the service's idle sleep.** Every A/B/A′ rep has p90 ≈ 220 µs against a
   p50 of ≈ 110–120 µs; arm C collapses p90 to 117 µs with one env var.
   `UC2_APPLY_IDLE=spin` buys 77 µs at p90 and 13 µs of mean for one pegged
   core per service. That is the strongest single lever in this run and it
   is a deploy-time choice, not code.
3. **fdatasync is 27 µs of the path, not the floor.** Eventual durability
   moves p50 from 105 to 77 µs. The v1 decomposition had fsync at 18 % of a
   1.88 ms floor (0.33 ms, EBS-era); on instance-store NVMe it is ~25 % of
   a 0.11 ms path. Same share, twenty times smaller in absolute terms.
4. **The wire bound.** One raw UDP round trip is 33.5 µs. Arm D's 77 µs p50
   contains exactly one such round trip (DATA out, `APPEND_POSITION` back
   from the faster follower), so the wire is **at most 44 %** of the
   no-fsync path and at most **29 %** of the shipped p50 (33.5 / 115). The
   other 43 µs of arm D is agent duty-cycle latency across four hops (ingress
   → consensus → sender; receiver → archive → report; receiver → consensus
   → commit; apply → egress → matcher) and the client's own poll.
5. **Against the net-decomp brief's clause (K).** The brief's kernel-bypass
   clause needs WIRE ≥ 80 µs absolute *and* ≥ 20 % of p50. The 20 % leg may
   hold (the bound is 29–44 %), but the 80 µs leg cannot: the entire wire
   round trip is 33.5 µs, so no transport can recover 80 µs from this path.
   On this fleet class, **(K) is closed by the bound without building the
   instrument**: kernel bypass is declined, with the number. Clause (L), the
   cheap ladder, turns on the syscall share, which this run does not
   measure; it stays open. The brief's thresholds were never ratified and
   are cited here as the standing draft.
6. **Like-for-like with Aeron.** Adaptive's published Aeron Cluster p50 at
   100 k msg/s is 85–97 µs (OSS, c6in.16xlarge 2025 / c3-highcpu-88 2024,
   tuned and pinned, archive sync almost certainly level 0) and 76 µs
   (Premium DPDK, AWS 2025). UC's comparable arm is D, 77 µs p50 on an
   8-vCPU c6id.2xlarge; UC's shipped posture with fsync on is 112–123 µs.
   The systems sit in the same band once the durability posture is matched,
   and the difference between UC's shipped number and Aeron's published one
   is mostly fsync (27 µs) and the apply sleep's second mode, not the
   transport.
7. **Throughput moved too, at the top of the ladder.** Inflight 256: A
   572 k/s, B 645 k/s, C 707 k/s, D 1.00 M/s at p50 0.217 ms with 0 lost.
   The spin-apply and eventual arms are not just latency arms; the M5
   gate's 1.64 M/s was at W = 1024 and is not comparable, but a 1 M/s point
   inside 0.25 ms p50 is new.

**Threats, answered.** (1) The client-on-leader-host cost is arm A − B.
(2) The faster follower gates commit; the report says so and does not
claim a per-follower split. (3) Eventual at 1 ms: p99 150 µs, max 37 ms in
one rep; the 1 ms lumps did not regress the tail the way the 50 ms default
did in the parity doc. (4) Drift 9–11 µs, smaller than every effect
claimed. (5) 64 B only. (6) No dev-box number above.

### 4.5 Re-run with the ladder as the default — 2026-09-17

The reading in §4.4 (2) led to a product change the same week: the apply
agent's idle strategy became a spin → yield → sleep ladder (commit
`2be3d6f`; `uc_log::IdleStrategy::Backoff`, `uc_service::APPLY_IDLE` = spin
2 000 / yield 1 024 / sleep 50 µs, no ramp). Its gate was two-sided: the
apply hop under load, where the agent should never idle, and this record's
low-load arms, where the second mode lives.

**Apply hop, dev-box A/B** (`scripts/apply_ab.sh 425eb2c 2be3d6f`, bounded,
one FSM, four runs per arm; a ratio, never a number): base 21.21 M
frames/s, head 21.91 M, head-rebuilt 21.93 M — **+3.3 %**, against a
rebuild resolution of 0.10 % and a worst sem of 0.10 %, driver-bound guard
clear. "Outside resolution", in the faster direction: the paced driver
still lets the agent idle briefly whenever it catches the appender, and the
ladder spins through those gaps. Recorded as a dev-box reading, not a
claim.

**Low-load arms, fleet re-run.** Fresh 4 × `c6id.2xlarge` fleet (different
physical placement: raw UDP round trip 26.7 µs p50 before and 26.8 after,
against 33.5 on 2026-09-16), arms A, C and A′ only, same driver, same
reps. The driver's arm-A label still reads "sleep-apply"; the binary
carried the ladder default.

| arm | posture | 2026-09-16 (flat sleep) mean / p50 / p90 / p99 | 2026-09-17 (ladder) mean / p50 / p90 / p99 |
|---|---|---|---|
| A | shipped, unpinned | 166 / 123 / 219 / 325 µs | **114 / 105 / 118 / 265 µs** |
| C | pinned + `UC2_APPLY_IDLE=spin` | 111 / 105 / 117 / 176 µs | 111 / 103 / 118 / 191 µs |
| A′ | = A, run last | 157 / 112 / 218 / 297 µs | 117 / 105 / 123 / 249 µs |

Inflight 256, zero lost: A 632 k/s at p50 0.351 ms (572 k / 0.428 the day
before), C 725 k/s at 0.330 ms (707 k / 0.335).

Reading:

1. **The second mode is gone under the shipped posture.** Unpinned, no env
   var, arm A's p90 fell from 219 to 118 µs and its mean from 166 to
   114 µs. p50 moved 18 µs, which is the sleep's share of the median plus a
   little placement.
2. **Arm C is the cross-fleet anchor.** It did not have the sleep on either
   day and reads within 2 µs at p50 and 1 µs at p90 across the two fleets,
   so the 7 µs shorter wire today is not what moved A. The comparison
   between the two A rows is the ladder, to within that anchor's spread.
3. **What the env var still buys over the default is the tail, and it is
   placement, not idling.** Today A and C agree at p50 and p90; C's p99 is
   191 against A's 265, and C is the pinned arm. `UC2_APPLY_IDLE=spin` on
   an unpinned service is not expected to close that gap; pinning is.
4. **Drift bracket** 0 µs at p50, 5 µs at p90: inside every claim above.

Threat 6 restated for this run: the dev-box shape check of the same change
halved this box's inflight-1 p50 (146 → 72 µs, fsync off the path); it is
listed as wiring proof and no local number is in the tables.

Raw output: `bench-out/service-time-2026-09-17-ladder/`.

## 5. What the number feeds

- The Aeron comparison in `docs/notes/uc2-network-and-ipc-share-of-smr-latency.md`
  gets a like-for-like row: UC's inflight-1 p50 against Aeron's 100 k
  msg/s p50 (95 µs OSS, 76 µs Premium on c6in.16xlarge; 85 µs OSS on
  c3-highcpu-88), with the fsync posture of each stated.
- Arm C's delta decided the apply agent's default: it is the spin → yield →
  sleep ladder since `2be3d6f` (§4.5), and `UC2_APPLY_IDLE` remains the
  per-service override.
- Arm D's wire share, with the raw RTT, is the prior the net-decomp brief's
  clause (K-lat) will be tested against, and says whether building that
  instrument is worth a fleet session of its own.
