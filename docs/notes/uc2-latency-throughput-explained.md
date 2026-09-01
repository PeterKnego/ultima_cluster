# Latency and throughput in UC, explained

**Audience:** anyone about to quote a UC performance number, tune an
admission or inflight window, or argue that a faster network / disk / codec
would make the cluster faster.
**Status:** explanatory. The normative record is the gate docs
(`docs/benchmarks/uc2-m5-gate-2026-07-12.md`,
`uc2-m12-gate-2026-08-22.md`, `uc2-m13-hop-bench-2026-08-24.md`) and
`docs/releases.md`; if this note and those disagree, they win and this note
is stale. Every number below is quoted from one of them, with the run's own
hardware — none is re-measured here, and none is current-release verified.

---

## The one-sentence version

UC's latency and throughput are not two independent dials: they are bound by
Little's Law through the concurrency in flight, so **a UC "performance
number" is a point on a curve, and quoting one without the other is
meaningless.**

## The law, and the fact that it actually holds here

Little's Law says `L = λ × W`: items in the system = arrival rate × time
each spends there. Rearranged for our purposes, **latency = concurrency ÷
throughput**.

The M5 fleet gate measured the fit to three significant figures
(`uc2-m5-gate-2026-07-12.md:385-390`, 3 × c6id.2xlarge, single AZ, cluster
placement group, fsync on), at a fixed client window of 4096:

| offered W | responses/s | W ÷ resp/s | observed p50 |
|---:|---:|---:|---:|
| 4096 | 1,078,422 | 3.80 ms | 3.789 ms |
| 4096 | 1,850,954 | 2.21 ms | 2.212 ms |
| 4096 | 2,143,233 | 1.91 ms | 1.902 ms |

No residual. The latency at those points is **not** a property of any hop —
it is the client's own window divided by the rate at which the pipeline
drains it.

## Two regimes, and why the window is a cap and not a target

The subtlety that trips people up: `W` in the law is the concurrency
*actually in the system*, while `--inflight` and `admission_bytes` are
**caps**. A cap that never binds contributes nothing.

**Regime 1 — below saturation.** The window is inert and latency is service
time. Hop 1 measured alone (`uc2-m13-hop-bench-2026-08-24.md:87-92`, one
`Engine` through the ingress ring and egress broadcast to a dummy node):

| engines | inflight | resp/s | p50 |
|---:|---:|---:|---:|
| 1 | 256 | 2,762,390 | 0.001 ms |
| 1 | 1024 | 2,762,815 | 0.001 ms |
| 1 | 4096 | 2,801,828 | 0.001 ms |

A 16× change in the window moved neither throughput nor latency, because
offered load never filled it. Here the hop is genuinely 2.8 M/s at a
microsecond.

**Regime 2 — at saturation.** The window binds, and latency becomes
`window ÷ throughput` exactly as the M5 table shows. Same hop, two engines
fanning in: 4,485,522 resp/s at p50 1.792 ms — which the gate doc annotates
as "Little's Law over two 4096 windows" (`:97`).

**Independently corroborated.** Adaptive's own Aeron Cluster numbers show
the same shape on other hardware: OSS at p50 95 µs at 100 k msg/s and p50
4,948 µs at 1 M — ten times the load for 52x the latency, which is a knee
crossing, not a service-time change
(`uc2-aeron-parity-2026-08-15.md:230-260` records this as "replicated
independently on 8x our cores with full host tuning"). Two labs, two
clouds, two implementations, one knee.

The transition between the regimes is the only interesting operating point,
and it sits at the **bandwidth-delay product**. M5 computes it: 1.6 M/s ×
0.6 ms ≈ 980 in flight, and `W = 1024` is where the pipeline runs at full
rate inside the 1 ms budget (`:394-397`).

## The anti-pattern this predicts, measured

If you push the window past the BDP you buy latency and no throughput. If
the hop is *also* CPU-bound, you can buy latency and **negative**
throughput. Hop 3 alone, one real `RemoteClient` against a sink that answers
instantly (`uc2-m13-hop-bench-2026-08-24.md:126,131`):

| conns | inflight | resp/s | p50 |
|---:|---:|---:|---:|
| 1 | 1024 | 170,999 | 3.973 ms |
| 1 | 4096 | 154,708 | **22.725 ms** |

Four times the concurrency, 5.7× the latency, 10% *less* throughput. There
is no tuning story in which that window is the right one.

## Why the two decouple at all: batching

A design that sent one datagram per command per follower would have its
throughput pinned at `1/RTT` per follower, and the wire would be the only
lever that mattered. UC is not that design. From the M12 network-budget
measurement (`docs/releases.md:1350-1358`): at peak, 1,424,941 resp/s drove
401.0 MB/s (3.21 Gbps) and 392,556 pkt/s — about a quarter of the
instance's ~12.5 Gbps ceiling — "because replication is batched to ~0.28
packets / ~281 bytes per committed command rather than a datagram per
command per follower."

**That 0.28 is the whole decoupling.** One round trip carries many commands,
so throughput ≈ batch ÷ RTT while per-command latency ≈ RTT + queueing. The
same shape appears on the durability axis: one fdatasync covers a whole
≤1 MiB block, so the batch amortizes the *throughput* cost of the guarantee
without weakening it — see
[`uc2-fsync-batching-vs-eventual-explained.md`](uc2-fsync-batching-vs-eventual-explained.md).

## Where the limits actually are

**Throughput: software, not the network.** The M12 conclusion is explicit
(`docs/releases.md:1359-1361`): "The ~1.4M/s ceiling is software (the single
apply thread / consensus), not the network." The NIC was at ~25% of its
ceiling at peak and under 10% at the p99 < 1 ms point.

**Latency: queueing, not the wire.** The M12 gate anchors this against an
external reference (`uc2-m12-gate-2026-08-22.md:542-544`): the wire RTT
floor on that hardware is **TCP p50 35.8 µs / p99 45.2 µs** (measured in the
sibling `hi-perf-cmp` grid, same instance type and placement), and therefore
"none of these p99 figures are RTT-bound; they are queueing (inflight ÷
throughput) as Little's Law predicts." Set against M5's best point, the wire
is roughly **6% of a 600 µs p50**.

**But latency is on the commit path structurally.** Commit means
quorum-fsync'd: `uc_consensus/src/commit.rs:5-6` ranks
`{leader's own durable} ∪ {reported follower durables}` and bounds the
result by the leader's own durable, and under the default
`Durability::Consistent` the durable counter advances only after that
block's fdatasync returns (`uc_log/src/archive.rs:298-301`). The archive is
its own polling agent, so **no appending thread ever blocks on fsync** — but
"non-blocking for a thread" is not "off the latency path". A commit can
never be earlier than the quorum-th fdatasync.

## The two knobs

| knob | where | default | what it does |
|---|---|---|---|
| `admission_bytes` | node, server side | 256 KiB (`uc_node/src/config_file.rs:259-261`) | caps un-committed bytes admitted at the door |
| client inflight | client / driver | harness-set | caps outstanding ops per client |

They are not redundant. M5 swept both because at `W = 4096` *every*
admission setting failed the latency bar — "at that offered concurrency the
dominant queue is the client's own inflight window, not the admission
window" (`:390-392`). Underneath it the admission model still shows:
throughput rose 64 → 128 → 256 KiB (1.10M → 1.62M → 1.64M/s at W = 1024)
and saturated past 128 KiB (`:398-400`).

Practical rule: **set the client window near the BDP** (`throughput ×
target latency`), then size admission to the point where throughput
saturates. Anything above the BDP is pure latency.

## How to read — and not misread — a UC number

1. **Never quote throughput and latency from different rows.** 2.14 M/s @
   1.9 ms and 1.64 M/s @ 0.600 ms are the same system at two offered loads.
   **And the ceiling's percentile is part of the number.** Adaptive publish
   Aeron OSS Cluster at 800 k/s under a p50 bar and 250 k/s under a p99 bar
   — a 3.2x spread on one product, from the bar definition alone. UC's own
   M5 bracket is a p50 ≤ 1 ms bar whose p99 runs 0.878-1.075 ms, so part of
   it would not survive a p99 bar either. Say which percentile, always.
2. **A latency number without its window is uninterpretable.** It could be
   service time (regime 1) or queue occupancy (regime 2), and those have
   nothing to do with each other.
3. **"Would a faster X help?" is a budget-share question.** Before
   attributing anything to the wire, the disk, or the codec, get that
   component's share of the measured latency. The 35.8 µs RTT figure exists
   precisely so that question has an answer for the network; the equivalent
   for fsync is a `Consistent` vs `Eventual` A/B at the same operating
   point.
4. **A collapse is not a latency story.** Both times a UC ladder collapsed,
   the cause was an emergent pathology (an MPSC publish convoy — see
   [`uc2-m13-mpsc-publish-convoy-explained.md`](uc2-m13-mpsc-publish-convoy-explained.md)),
   not a hop getting slower. Sweep the stress axis and reproduce it in the
   isolated hop.
5. **To raise throughput, attack the serial software hops** (apply thread,
   consensus) — per CLAUDE.md's hop-isolation rule, optimizing any faster
   hop measures null end-to-end.

## What this note does not claim

- **There is no v2 service-time decomposition.** No document breaks a commit
  into its RTT / fdatasync / IPC terms. Every latency number in the record
  is an end-to-end measurement at a stated offered load. Claims of the form
  "UC's floor is mostly X" are unsupported until someone runs that
  measurement.
- **The rates above are each from one run on that run's hardware**, mostly
  c6id.2xlarge in 2026-07/08 at earlier releases. The *shape* — the law, the
  two regimes, the knobs — is what this note is about; the absolute rates
  are not current-release verified.
- **Warmup caveat.** `m14_fleet_gate.py` uses the `--warmup-secs` /
  `--measure-secs` steady window (rows d and f deliberately opt out); most
  other drivers do not, so their published rates include a 3–5% warmup
  climb. Fine for shapes and ratios, not for head-to-head comparison with a
  gate row.

## See also

- `docs/benchmarks/uc2-m5-gate-2026-07-12.md` — the Little's-law finding and
  the two-knob sweep.
- `docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md` — per-hop isolation; the
  two regimes side by side.
- `docs/benchmarks/uc2-m12-gate-2026-08-22.md` — the network budget and the
  RTT reference floor.
- [`uc2-fsync-batching-vs-eventual-explained.md`](uc2-fsync-batching-vs-eventual-explained.md)
  — the durability axis of the same batching argument.
- CLAUDE.md, "Finding a performance bottleneck" — the hop-isolation method
  this note supplies the arithmetic for.
