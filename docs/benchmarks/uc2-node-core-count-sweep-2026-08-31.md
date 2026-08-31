# How many cores does a UC node need? — 4, and a bistability nobody knew about

*Ran 2026-08-31 on `main` `2737805`. 3 × `c8id.4xlarge` (Intel Xeon 6975P-C,
8 physical cores × 2 SMT, 1 socket), us-east-1, one placement group. 21 arms,
12 s each, **zero lost responses**. Driver:
`bench-infra/scripts/m14_core_sweep.py`.*

**Answer: 4 physical cores** — one per polling agent — with no measurable gain
past 5. **But the more important result is accidental:** the harness has two
stable operating regimes that differ 5× in latency, and which one an arm lands
in is independent of core count. That bistability, not thread placement, is
the better candidate for the fleet variance that
[`uc2-m14c2-fleet-pinning`](uc2-m14c2-fleet-pinning-2026-08-30.md) was chasing.

## Why this run exists

The pinning run left one point on a curve nobody had drawn: constraining the
node to 2 physical cores cost 9.4 % of mean throughput. Nobody knew which side
of the knee that sat on.

## Method

Hold hardware, binary, workload and service/client placement **fixed**; vary
only the number of physical cores the node's four polling agents may use. An
instance-size sweep would confound cores with cache, memory bandwidth, NIC and
CPU generation at once; a pin-width sweep on one host changes exactly one
thing.

| arm | node `CPUAffinity` | cores |
|---|---|---|
| w1 | `0,8` | 1 |
| w2 | `0,1,8,9` | 2 |
| w3 | `0,1,2,8,9,10` | 3 |
| w4 | `0,1,2,3,8,9,10,11` | 4 |
| w5 | `0,1,2,3,4,8,9,10,11,12` | 5 |
| w6 | `0,1,2,3,4,5,8,9,10,11,12,13` | 6 |
| unpinned | — | all 8 |

Service fixed on core 6, client on core 7, every arm. Each width gets **both
SMT threads** of each core, so "3 cores" is three whole cores, never six
half-shared ones — otherwise the sweep would measure SMT contention rather
than core count. 3 reps, interleaved (w1..w6, unpinned, repeat) so session
drift lands on every width equally.

**Sibling layout verified at run time**, not assumed: `lscpu -e=CPU,CORE` on
all three voters gave `CORE 0..7 0..7`, i.e. siblings `(i, i+8)`. The driver
refuses to run (`SystemExit`) if it does not match, because a wrong map
silently measures something other than what it claims.

**Scope: this is the DIRECT shmem path — there is no gateway.**
`start_cluster` starts only `node` and `service` units; the client is
`client-direct --instance-dir …`, attaching to the leader's shared memory in
the foreground. Nothing here sizes a host that terminates remote traffic,
where an `Edge` and its per-connection threads would want cores of their own.
The co-located client also occupies a core a production node would not spend.

## The two regimes

Sorting all 21 arms by p50 splits them cleanly, with the largest gap in the
whole set — **0.872 ms** — sitting in the middle and nothing inside it:

```
0.322 0.337 0.338 0.348 0.352 0.356 0.368     <- 7 arms, "shallow"
                    ( 0.872 ms gap )
1.228 1.261 1.274 1.278 1.294 1.367 1.381
1.537 1.555 1.604 1.692 1.702 1.750           <- 14 arms, "deep"
```

| | p50 | rate | behaviour |
|---|---|---|---|
| **deep** (14/21) | 1.228 – 1.750 ms | 2.28 – 3.05 M | window near-full; **scales with cores** |
| **shallow** (7/21) | 0.322 – 0.368 ms | 2.29 – 2.52 M | short queue; **flat regardless of cores** |

Shallow appeared at w3, w4, w6 and unpinned — so it is **not** a function of
core count. w6 produced both regimes on consecutive reps.

**Shallow is not "degraded".** At w1 the two regimes deliver the same
throughput (2.31 M deep vs 2.35 M shallow) with **5× different latency**.
Shallow only loses once the node has ≥ 4 cores, where deep buys ~3.0 M by
running a much deeper queue and paying ~4× the latency for it. These are two
operating points, not a good and a bad one.

## The core-count curve (deep regime only)

Averaging across regimes is meaningless — it manufactures a 27 % "spread" at
w4 out of two different states. Deep-regime arms only:

| node cores | mean resp/s | n | arms (M) |
|---|---|---|---|
| 1 | 2,312,709 | 3 | 2.28  2.32  2.33 |
| 2 | 2,535,046 | 3 | 2.50  2.56  2.54 |
| 3 | 2,615,789 | 2 | 2.70  2.53 |
| **4** | **2,944,156** | 2 | 3.03  2.86 |
| **5** | **3,022,271** | 3 | 3.00  3.02  3.05 |
| 6 | 3,011,065 | 1 | 3.01 |
| unpinned | — | **0** | — |

Rises w1 → w4, then flat. w4 and w5 differ by 2.6 % while w4's own two samples
differ by 5.7 %, so **they are not distinguishable**; w6 adds nothing. The
node runs exactly four agent threads
(`uc2-{consensus,sender,receiver,archive}`, all `IdleStrategy::Yield` — they
return the core when idle but never idle under saturation), and the curve
flattens where the threads run out. **One physical core per agent.**

## Limits, stated

- **Unpinned drew zero deep-regime arms** (3/3 shallow), so the practically
  interesting comparison — scheduler vs a pin map, saturated — is simply
  absent from this run.
- **w6's deep number is n=1.** The plateau's right-hand end is one arm.
- **The regime split cost the design.** 3 reps per width were planned; the
  deep regime got 3, 3, 2, 2, 3, 1, 0. Interleaving guards against drift, not
  against a bistability nobody knew was there. Any repeat should detect the
  regime per arm and keep sampling until each width has n deep arms.
- **One session, one host type, direct path only.** Absolute rates are not
  comparable to the `c6id.2xlarge` pinning run — different CPU generation.

## What was ruled out along the way

**The admission-window theory, refuted without spending fleet time.** The
shallow regime's implied concurrency (~800 against a 4096 window) suggested
`--admission-kib 256` as the cap. It cannot be: both regimes ran with the
identical setting, and the deep regime sustains ~3,900 concurrent under it.
`admission_bytes` gates `append - commit` (bytes awaiting commit); client
concurrency spans commit **and** apply **and** the response hop. Different
windows; one does not bound the other.

**A live lead, unconfirmed:** `uc_client/src/client.rs:42` hardcodes
`MAX_INFLIGHT: u32 = 1024` on the blocking `Client` path, and the shallow
regime's concurrency sits just under it. But the harness reports using the
`Engine`, so it has not been established that this constant is even reachable
here. Recorded as a lead, not a finding.

## Consequences

1. **`uc2-m14c2-fleet-pinning`'s residual 14.3 % spread has a better
   candidate than "undiagnosed":** a regime split with a ~25 % rate gap.
   Pinning went shallow 4/18 vs unpinned 3/3 in this run — suggestive that
   pinning makes the deep regime more likely, which would explain how it cut
   the spread without addressing the cause. **n=3 for unpinned; do not build
   on this.**
2. **Any future fleet measurement should record p50 per arm and split by
   regime** before averaging. Every spread statistic taken across a mixed
   sample is inflated by the split, not by the variable under test.
3. **Sizing guidance for the direct path:** 4 cores for the node, plus one per
   attached FSM, plus one for a co-located client. Nothing here covers the
   gateway path.

## Reproducing

```bash
cd bench-infra && make up-uc            # 3x c8id.4xlarge; edit instance_type/node_count
python3 scripts/m14_core_sweep.py --fleet --no-sync \
    --widths 1,2,3,4,5,6 --reps 3 --hosts <pub/priv x3>
cd bench-infra && make destroy && terraform -chdir=terraform state list   # must be empty
```

This run's fleet was destroyed immediately: 11 resources, `state list` empty,
0 EC2 instances tagged `uc-bench` per a direct API query.

**Aside worth recording:** `terraform.tfvars`' note that the account's
us-east-1 on-demand vCPU limit is 32 is **stale** — 3 × c8id.4xlarge (48 vCPU)
applied without complaint.
