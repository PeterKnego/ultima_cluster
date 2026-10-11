# UC v2 — Read types under rising write load on Graviton (c9gd), writes pushed to the ceiling

**Date:** 2026-10-11
**Status:** questions and bars pre-committed before any run (this section is
fixed once data exists; results are appended below it, never edited into it).
**Kind:** INSTRUMENT, not a gate. It ships or refuses nothing.
**Context:** the README's headline write figure (3.8 M responses/s, p99 ≤ 1 ms)
was measured on 3 × `c9gd.4xlarge`
(`uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md`), while every read run on record —
`uc2-read-spread-2026-10-07.md`, `uc2-read-types-under-writes-2026-10-10.md`,
`uc2-query-drain-progress-2026-10-10.md` — ran on 3 × `c6id.2xlarge`, where the
same software writes at 1.4–1.5 M/s. The highest write rate any read run
reached was ~199 k/s, the ceiling of the harness's single paced writer thread.
So "how many reads alongside how many writes" has only ever been answered on
the older fleet, and only up to 200 k writes/s. This run puts the read matrix
on the Graviton fleet and pushes writes as far as they go.

## 1. Arms and rungs

Harness: `uc_node/examples/read_profile.rs` (`client --mode lin|snap|ryw`,
`--any-role` for followers), driven by
`bench-infra/scripts/m6_fleet_gate.py --read-profile --rp-matrix`. The arms
are the 2026-10-10 five:

| Arm | What runs |
|---|---|
| `lin/leader` | linearizable reads on the leader (quorum barrier) |
| `snap/leader` | snapshot reads on the leader |
| `ryw/leader` | read-your-writes reads on the leader |
| `snap/spread` | snapshot reads on all 3 nodes at once |
| `ryw/spread` | read-your-writes reads on all 3 nodes at once |

**The read-your-writes token** is the attached node's own commit position at
send time, the freshest token a real client could carry (as in the 2026-10-10
run): the hardest case for the mode.

**Write load** comes from the leader-host client in every arm. Two harness
changes made for this run, both in the tree the hosts build:

- `--write-threads N`: the target rate is split across N writer threads, each
  with its own client id and its own ingress producer. The 2026-10-10 run had
  one thread, whose pacing loop is the ceiling somewhere above 200 k/s.
- `--write-unpaced`: every writer thread submits as fast as the ingress ring
  accepts, yielding on a full ring. The accepted rate IS the result. The
  driver spells this rung `max` in `--rp-write-rates`.

**Rungs:** `--rp-write-rates 0,20000,50000,100000,200000,500000,1000000,2000000,max`
with `--rp-write-threads 4` for every rung that writes. The first five rungs
are the 2026-10-10 ladder (now driven by 4 threads instead of 1, which at
≤ 200 k/s changes nothing measurable: each thread sleeps between its paced
submits either way); the next three are new; `max` is the ceiling. Writes
actually accepted per second are recorded per rung, and so is the fraction of
attempts dropped on a full ring — a dropped write never reached the node.

**Shape:** 1,024 reads in flight per reading host, 20 s per rung, fresh 3-node
cluster per rung, 2 reps per (arm, write rate) with the arm order rotated,
3 × `c9gd.4xlarge` (16-core Graviton, no SMT, instance NVMe) in one placement
group, Ubuntu 24.04 arm64. 9 rates × 5 arms × 2 reps = 90 rungs. The tree
includes the query-drain-progress fix (`uc_service/src/apply.rs`, main since
PR #97), so the 0-writes column is a real baseline, not the idle-sleep cap the
2026-10-10 doc had to discount.

## 2. Questions and bars

**Q1 — the write ceiling with reads alongside (reported, no bar).** For each
arm: **committed** writes/s at the `max` rung — the cluster's commit counter
advance over the send window divided by the frame size (the harness reads the
cnc commit word at the window's start and end; the frame size is checked at a
paced rung, where committed bytes ÷ accepted writes must give one constant) —
beside the ring-accepted writes/s, the fraction of attempts dropped on a full
ring, and the highest paced rung whose accepted rate reached ≥ 95 % of its
target. *Amended before the run, after a 5 s smoke of the `max` rung showed
ring-accepted rates (4.6–5.9 M/s) that the cluster could not plausibly have
committed: a write "accepted" by the ingress ring can still be turned away at
the node's admission window, and this harness discards write responses, so
only the commit counter says what was committed. The accepted number stays
in the record as what the writer did; the committed number is the answer.* This is the number the README's 3.8 M/s is compared against — that
figure came from a pipelined `uc_client::Engine` with 4,096 writes in flight
and no reads; this harness's writer is an unwindowed `try_write` loop, so the
two are not the same instrument, and a lower ceiling here is expected. What is
new is the reads/s standing beside it.

**Q2 — what read-your-writes buys.** At 0 writes/s, `ryw/spread` reads/s ÷
`lin/leader` reads/s. Bar: **≥ 2.0** (the 2026-10-10 bar, unchanged). Reported
at every write rate; judged at 0.

**Q3 — fast-path cost.** At 0 writes/s, `ryw/leader` reads/s ÷ `snap/leader`
reads/s. Bar: **≥ 0.90** (unchanged). Reported at every rate.

**Q4 — starvation under writes (descriptive, per arm).** An arm is **starved**
at a write rate when any of these holds against the same arm at 0 writes/s:
- read throughput below **25 %** of its 0-writes value;
- retries + redirects at **≥ 5 %** of resolved reads on any host;
- worst-host p99 latency at **≥ 10×** its 0-writes p99.

Reported: for each arm, the lowest write rate (accepted, not targeted) at
which it is starved, or "not starved up to the highest rate reached". The
`max` rung is included: it is where starvation, if it exists, should appear.

**Q5 — the cross-fleet factor (reported, no bar).** At the 200 k rung, each
arm's reads/s here ÷ the same arm on `c6id.2xlarge` in
`uc2-read-types-under-writes-2026-10-10.md` §4.1 (lin/leader 1,928,922;
snap/leader 2,621,373; ryw/leader 2,774,136; snap/spread 9,516,282; ryw/spread
8,932,002). The arch sweep measured the write path at 2.5× between these
fleets; this says whether reads move the same way.

**Validity.** A rung is invalid only when a client crashes or two hosts serve
(an invalid rung is reported and never substituted). Retries, dropped writes
and a sagging in-flight depth are results, not invalidity. A missing rep makes
that cell's verdict INCONCLUSIVE. A paced rung whose accepted rate is below 95 %
of its target is reported at its accepted rate and flagged as writer-bound.
Q4's "accepted, not targeted" write rate means the committed rate from Q1.

## 3. What this does not measure

- Linearizable reads on followers (not implemented).
- Real tokens: real clients carry older tokens than "commit right now", so
  their read-your-writes reads wait less than this run's.
- The pipelined-engine write ceiling (the README figure): see Q1.
- Reads with the writer on a separate host: the leader host runs the node, the
  service, the reading client and the 4 writer threads. A client-bound rung
  shows as a depth below its target.
- Any x86 number: this is one fleet, one architecture.

## 4. Results

Run 2026-10-11, 18:33–20:14 UTC, 3 × `c9gd.4xlarge` (16 cores each,
aarch64, no SMT), us-east-1, one placement group, driven from branch
`bench/read-matrix-c9gd` at `c1276d9f` (the hosts built an rsync'd copy of
that tree; `--help` on each host showed the new writer flags and the binary
carried the `commit_bytes_per_sec` field before the run). Command:

```
m6_fleet_gate.py --fleet --read-profile --rp-matrix --rp-readers 1024 --rp-secs 20 \
  --rp-reps 2 --rp-write-rates 0,20000,50000,100000,200000,500000,1000000,2000000,max \
  --rp-write-threads 4
```

Raw data: `uc2-read-types-under-writes-c9gd-2026-10-11-rungs.jsonl` (all 90
rungs). Host 0 won every election, so "leader host" is host 0 throughout.
Fleet destroyed by a watcher the moment the driver exited; teardown verified
by `terraform state list` returning 0 entries and an EC2 `describe_instances`
showing no non-terminated instance (§4.6).

**Validity:** 90/90 rungs valid, both reps present in every cell, 0 retries
and 0 redirects on every host of every rung. Every paced target was reached:
the worst arm's committed ÷ target is 1.000 at 20 k–1 M and 0.998 at 2 M.
Two cells are client-bound (depth below 90 %): snap/spread at 500 k, rep 2
(61 %) and at 0 writes, rep 1 (92 %); they are lower bounds on the nodes and
stay in the record.

**Frame size check (Q1's method):** at every paced rung the commit advance ÷
accepted writes is 128 B (2.6 MB/s at 20 k, 25.6 at 200 k, 128.0 at 1 M,
255.5 at 2 M), so committed writes/s below is `commit_bytes_per_sec ÷ 128`.

### 4.1 Reads/s (mean of 2 reps; per rep in brackets)

| writes/s committed | lin/leader | snap/leader | ryw/leader | snap/spread | ryw/spread |
|---:|---:|---:|---:|---:|---:|
| 0 | 3.88M [3.88 3.88] | 7.05M [7.24 6.86] | 5.87M [6.01 5.73] | 20.73M [20.90 20.56] | 16.87M [16.64 17.11] |
| 20 k | 3.72M [3.61 3.82] | 6.19M [6.25 6.14] | 5.19M [5.45 4.92] | 19.76M [20.17 19.35] | 16.74M [16.56 16.93] |
| 50 k | 3.21M [3.40 3.01] | 5.61M [5.48 5.74] | 4.47M [4.67 4.28] | 19.32M [18.90 19.74] | 16.01M [16.05 15.97] |
| 100 k | 3.16M [3.04 3.28] | 6.06M [5.93 6.19] | 4.75M [4.49 5.01] | 19.88M [19.90 19.86] | 16.34M [16.17 16.52] |
| 200 k | 3.13M [3.03 3.23] | 5.69M [5.78 5.59] | 4.88M [4.97 4.79] | 19.90M [19.97 19.83] | 16.49M [16.25 16.74] |
| 500 k | 3.23M [3.39 3.07] | 6.10M [6.10 6.11] | 5.04M [5.04 5.04] | 19.72M [19.44 20.01] | 16.84M [16.47 17.21] |
| 1 M | 3.10M [3.00 3.21] | 6.22M [6.13 6.32] | 4.82M [4.78 4.87] | 19.44M [19.51 19.37] | 16.34M [16.12 16.56] |
| 2 M | 2.89M [2.91 2.86] | 5.07M [5.23 4.91] | 4.60M [4.62 4.58] | 17.74M [17.84 17.63] | 15.11M [14.97 15.26] |
| `max` | 1.14M [1.15 1.13] | 0.84M [0.64 1.04] | 1.20M [0.63 1.77] | 7.27M [5.46 9.08] | 6.59M [5.97 7.22] |

Worst-host p99 at every paced rung, every arm: **0.21–0.47 ms** (p50
0.14–0.37 ms). The leader arms' rep-to-rep spread is up to ±6 % (ryw/leader
at 20 k: 5.45 vs 4.92), the spread arms' ±2 %, except at `max` (§4.3).

### 4.2 The `max` rung: writes as far as they go (Q1)

Four unpaced writer threads on the leader host, 20 s, mean of 2 reps:

| arm | committed writes/s | ring-accepted | attempts dropped | reads/s | p50 | worst-host p99 (reps) |
|---|---:|---:|---:|---:|---:|---:|
| lin/leader | **4.64M** | 4.64M | 46.1 % | 1.14M | 0.88 ms | 1.23 ms [1.23 1.21] |
| snap/leader | **4.67M** | 4.67M | 44.5 % | 0.84M | 0.92 ms | 1.47 ms [1.67 1.27] |
| ryw/leader | **4.73M** | 4.72M | 44.8 % | 1.20M | 0.69 ms | 1.27 ms [1.44 1.10] |
| snap/spread | **5.27M** | 5.26M | 33.2 % | 7.27M | 0.89 ms | [685.8 1.11] |
| ryw/spread | **4.95M** | 4.94M | 42.1 % | 6.59M | 0.94 ms | [12.9 1.25] |

Committed equals ring-accepted in every arm (the admission window turned
nothing away); the 33–46 % of attempts dropped are the writers finding the
ingress ring full, which is the only back-pressure this harness has. So the
cluster's write ceiling with reads alongside, on this instrument, is
**4.6–5.3 M committed writes/s** — 590–675 MB/s of log, quorum-durable —
above the README's 3.8 M/s, which a pipelined `uc_client::Engine` measured
with no reads on the same fleet type. The two instruments differ (§2, Q1),
so this is not a new headline for the README; it is a floor under it.

### 4.3 Verdicts

| Question | Measured (per rep) | Bar | Verdict |
|---|---|---|---|
| Q1 write ceiling | 4.6–5.3 M committed/s with reads; every paced rung to 2 M reached | reported | §4.2 |
| Q2 ryw/spread ÷ lin/leader @0 | 4.35 (4.29 · 4.41) | ≥ 2.0 | **PASS** |
| Q3 ryw/leader ÷ snap/leader @0 | 0.832 (0.829 · 0.836) | ≥ 0.90 | **FAIL** — bar kept |
| Q4 starvation | leader arms starve only at `max` (reads 12–29 % of 0-writes); spread arms starve only at `max`, on the p99 clause | §2 | **no arm starved up to 2 M committed writes/s** |
| Q5 cross-fleet @200 k | lin 1.62×, snap/leader 2.17×, ryw/leader 1.76×, snap/spread 2.09×, ryw/spread 1.85× | reported | §4.5 |

Q2 at every rate: 4.35 (0), 4.51 (20 k), 4.99 (50 k), 5.17 (100 k), 5.27
(200 k), 5.22 (500 k), 5.27 (1 M), 5.24 (2 M), 5.78 (`max`).

**Q3 misses, in both reps, at every paced rate.** ryw/leader ÷ snap/leader is
0.83, 0.84, 0.80, 0.78, 0.86, 0.83, 0.78, 0.91 from 0 to 2 M writes/s — a
read-your-writes read on the leader costs about 15–20 % more than a snapshot
read here. The 2026-10-10 c6id run read 0.94–1.10 for the same ratio (its
0-writes cell was the idle cap; with writes it was a real reading), so the
gap is specific to this fleet or to the post-idle-cap code, and this run
cannot tell which. It is not the token wait: at 0 writes the token is already
applied when the read is admitted, so the extra cost is on the fast path — the
node's min-position check per query, the client's token bookkeeping, or
both. The bar stays; the next step is an A/B of that fast path, not a
re-measurement. (At `max` the ratio inverts to 1.42 because snap/leader's
reads collapse further than ryw/leader's, which is noise between two
starved arms, not a result.)

**Q4 detail.** Against its own 0-writes value, every arm holds ≥ 73 % of its
reads up to 2 M committed writes/s (lin/leader 0.75, snap/leader 0.72 at
2 M; the spread arms 0.86 and 0.90), worst-host p99 stays within 2.2× of its
0-writes value, and no host ever retried. At `max` the leader-host arms fall
to 12–29 % of their read-only rate (the leader host's service is applying
4.6–4.7 M frames/s and answers queries in the gaps), and the spread arms keep
35–39 % with the followers doing the reading. The spread arms' `max` p99 is
**bimodal**: rep 1 read 686 ms (snap/spread) and 12.9 ms (ryw/spread) on a
follower, rep 2 read 1.1 and 1.2 ms. A follower whose service falls behind
the flood answers its reads late; which follower, and whether, differs per
boot. That is one rep each way, so it is recorded, not explained.

### 4.4 What the run says

1. **Two million committed writes a second beside fifteen million
   read-your-writes reads a second, p99 under half a millisecond.** At 2 M
   writes/s (255 MB/s of quorum-durable log) ryw/spread served 15.1 M reads/s
   at worst-host p99 0.34 ms, snap/spread 17.7 M at 0.24 ms, and the leader
   alone 2.9–5.1 M depending on read type at ≤ 0.47 ms. The question this run
   was asked — "3 M writes and 9 M read-your-writes reads at once?" — is
   answered yes with margin on reads and no on writes at that latency: 2 M
   is the highest paced rung, and the ceiling (§4.2) sits at 4.6–5.3 M with
   reads at 1.1–7.3 M and p99 in the 1–1.5 ms band on the leader.
2. **Reads are flat across a hundredfold rise in write load.** From 20 k to
   2 M writes/s the spread arms lose 8–10 % and the leader arms 11–22 %. The
   "applies all committed frames before answering queries" ordering costs
   reads nothing measurable until the service is saturated.
3. **Read-your-writes on all nodes gives 4.4–5.8× the capacity of
   linearizable reads on the leader** (Q2), at every write rate.
4. **Read-your-writes on the leader is not free here** (Q3): 0.78–0.91 of a
   snapshot read. Recorded as a miss; see §4.3 for what it is not.
5. **Graviton reads 1.6–2.2× faster than the 2020 x86 fleet** (Q5), less
   than the 2.5× the write path showed in the arch sweep; the linearizable
   path gains least (1.62×), the snapshot paths most (2.1–2.2×).
6. **Client-bound cells are rarer than on c6id.** Only two cells fell below
   90 % depth; one client per host at 1,024 in flight keeps up with ~6.6 M
   reads/s per host here.

### 4.5 Cross-fleet, same arm, 200 k writes/s

| arm | c9gd.4xlarge | c6id.2xlarge (2026-10-10) | ratio |
|---|---:|---:|---:|
| lin/leader | 3.13M | 1.93M | 1.62× |
| snap/leader | 5.69M | 2.62M | 2.17× |
| ryw/leader | 4.88M | 2.77M | 1.76× |
| snap/spread | 19.90M | 9.52M | 2.09× |
| ryw/spread | 16.49M | 8.93M | 1.85× |

The c6id numbers predate the idle-cap fix but were measured with writes, so
the cap was not in play for them; the comparison is like for like.

### 4.6 Teardown

Destroy ran from a watcher that waited for the driver to exit (20:14:23 UTC):
`terraform plan -destroy` → 11 to destroy → `apply`; `terraform state list`
empty; `describe_instances` showed 0 non-terminated instances. Cost: one
provision + 90 rungs + destroy on 3 × `c9gd.4xlarge`, about 1 h 50 min of
fleet time end to end.
