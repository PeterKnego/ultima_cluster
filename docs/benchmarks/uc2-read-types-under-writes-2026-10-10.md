# UC v2 — Read types compared, and how reads behave under rising write load

**Date:** 2026-10-10
**Status:** questions and bars pre-committed before any run (this section is
fixed once data exists; results are appended below it, never edited into it).
**Kind:** INSTRUMENT, not a gate. It ships or refuses nothing.
**Context:** read-your-writes reads merged in PR #88 (spec
`docs/superpowers/specs/2026-10-08-uc2-read-your-writes-design.md`). No run so
far measured read-your-writes reads themselves: the read-spread run
(`uc2-read-spread-2026-10-07.md`) measured snapshot reads before the feature
existed, and the client-hop A/B measured writes only. The service applies all
committed frames before it answers queries (at most 64 per apply cycle), so
writes take precedence over reads; this run also measures what that does to
each read type as writes rise.

## 1. Arms

Harness: `uc_node/examples/read_profile.rs` (`client --mode lin|snap|ryw`,
`--any-role` for followers), driven by
`bench-infra/scripts/m6_fleet_gate.py --read-profile --rp-matrix`.

| Arm | What runs |
|---|---|
| `lin/leader` | linearizable reads on the leader (quorum barrier) |
| `snap/leader` | snapshot reads on the leader |
| `ryw/leader` | read-your-writes reads on the leader |
| `snap/spread` | snapshot reads on all 3 nodes at once |
| `ryw/spread` | read-your-writes reads on all 3 nodes at once |

There is no `lin/spread` arm: a follower answers a linearizable read with
`NOT_LEADER`.

**The read-your-writes token** is the attached node's own commit position at
send time: the freshest token a real client could carry (it has just seen a
write commit). On the leader that makes a read-your-writes read
"linearizable minus the quorum round"; on a follower it forces the read to
wait for that node to apply everything it knows is committed. Real clients
carry older tokens and wait less, so this is the hardest case for the mode.

**Write load:** `--rp-write-rates 0,20000,50000,100000,200000` writes/s from
the leader-host client in every arm (identical across arms). The writes
actually accepted per second are recorded per rung, since one paced writer
thread may not reach the higher targets.

**Shape:** 1,024 reads in flight per reading host, 20 s per rung, fresh
3-node cluster per rung, 2 reps per (arm, write rate) with the arm order
rotated, 3 × `c6id.2xlarge` in one placement group (the shape of the 2026-07-26
read profile and the 2026-10-07 read-spread run).

## 2. Questions and bars

**Q1 — fast-path cost.** At 0 writes/s, `ryw/leader` reads/s ÷ `snap/leader`
reads/s. Bar: **≥ 0.90** means read-your-writes costs about what a snapshot
read costs when nothing is waiting.

**Q2 — what the feature buys.** At 0 writes/s, `ryw/spread` reads/s ÷
`lin/leader` reads/s. Bar: **≥ 2.0** means read-your-writes on all nodes
gives at least twice the read capacity of the linearizable path. Reported at
every write rate; judged at 0.

**Q3 — starvation under writes (descriptive, per arm).** For each arm, an arm
is **starved** at a write rate when any of these holds against the same arm
at 0 writes/s:
- read throughput below **25 %** of its 0-writes value;
- retries + redirects at **≥ 5 %** of resolved reads on any host;
- worst-host p99 latency at **≥ 10×** its 0-writes p99.

Reported: for each arm, the lowest write rate at which it is starved, or
"not starved up to the highest rate reached". Write rates the writer did not
actually reach are reported as reached, not as targeted.

**Validity.** A rung is invalid only when a client crashes or two hosts serve
(an invalid rung is reported and never substituted). Retries and a sagging
in-flight depth are results here, not invalidity. A missing rep makes that
cell's verdict INCONCLUSIVE.

## 3. What this does not measure

- Linearizable reads on followers (not implemented).
- Real tokens: real clients carry older tokens than "commit right now", so
  their read-your-writes reads wait less than this run's.
- The read-only idle-ladder cap recorded in `uc2-read-spread-2026-10-07.md`
  §5.4 (the service sleeps between query batches when there are no writes) is
  still in the code; the 0-writes column measures it, it is not removed.
- The client side is one process per host; a client-bound rung shows as a
  depth below its target.

## 4. Results

Run 2026-10-10, 19:00–19:56 UTC, 3 × `c6id.2xlarge`, us-east-1, placement
group, driven by `m6_fleet_gate.py --read-profile --rp-matrix` from this
branch. Raw data: `uc2-read-types-under-writes-2026-10-10-rungs.jsonl` (all 50
rungs). **Provenance:** the hosts built a rsync'd working tree (no `.git`, so
no recorded SHA). It contained `fcf8f963`'s harness, because every rung ran
`--mode ryw` and recorded `writes_per_sec`, which exist only from that commit;
`bab226bd` (a clippy allow + fmt) changes no behaviour. Host 0 won every
election, so "leader host" is host 0 throughout. Fleet destroyed after the
run; teardown verified by `terraform state list` returning 0 entries and an EC2
`describe_instances` showing no non-terminated instances.

**Validity:** 50/50 rungs valid (no client crash, exactly one leader in every
rung), both reps present in every cell, 0 retries and 0 redirects on every host
of every rung. Every write target was reached: 19,999–20,000, 49,998–49,999,
99,996–99,998 and 198,323–199,333 writes/s accepted.

**The driver prints no verdict for `--rp-matrix`.** Its closing line ("the
verdict is the `decide` block above") and its `sweep:` header are left over
from the lin/snap ladder mode; `run_read_matrix` never calls `decide`. The
verdicts below are computed by hand from the rung file against §2's bars,
unchanged.

### 4.1 Reads/s (mean of 2 reps)

| writes/s | lin/leader | snap/leader | ryw/leader | snap/spread | ryw/spread |
|---:|---:|---:|---:|---:|---:|
| 0 | 547,195 | 618,766 | 647,589 | 1,800,264 | 1,755,929 |
| 20k | 2,102,610 | 2,313,799 | 2,546,074 | 9,499,679 | 8,931,717 |
| 50k | 2,194,543 | 2,403,581 | 2,522,367 | 9,029,186 | 8,298,876 |
| 100k | 2,102,151 | 2,843,258 | 2,680,885 | 9,628,793 | 9,136,237 |
| 200k | 1,928,922 | 2,621,373 | 2,774,136 | 9,516,282 | 8,932,002 |

Worst-host p99 stays between 0.44 and 1.00 ms in every rung with writes, and
between 1.70 and 2.01 ms at 0 writes. Rep-to-rep spread on the leader arms
reaches about ±18 % in a cell (snap/leader at 50k: 1.97M vs 2.84M); the spread
arms stay within about ±4 %.

### 4.2 Verdicts

| Question | Measured (per rep) | Bar | Verdict |
|---|---|---|---|
| Q1 ryw/leader ÷ snap/leader @0 | 1.047 (0.998 · 1.101) | ≥ 0.90 | **PASS**, with the caveat in §4.3 |
| Q2 ryw/spread ÷ lin/leader @0 | 3.21 (3.18 · 3.24) | ≥ 2.0 | **PASS** |
| Q3 starvation, every arm | none starved | §2 | **not starved up to ~199k writes/s**, all five arms |

Q2 at every write rate: 3.21 (0), 4.25 (20k), 3.78 (50k), 4.35 (100k),
4.63 (200k).

Q3 detail: against its own 0-writes value, every arm's reads/s ran
**3.5–5.4× higher** (mean of reps) with writes, not lower (worst single
rep: snap/leader at 50k, ×3.18); worst-host p99 ran at 0.25–0.51× its 0-writes p99; retries were
0 %. No starvation clause comes near firing.

### 4.3 The 0-writes column is the idle-sleep cap, so Q1 and Q3 are weak tests

Every arm reads 3.5–5.4× faster *with* writes than without. That is the
read-only idle cap recorded in `uc2-read-spread-2026-10-07.md` §5.4, still in
the code (§3): `apply_cycle` (`uc_service/src/apply.rs`) does not count answered
queries as progress, so a service with no writes sleeps between query batches
and serves about 550–650k reads/s per node whatever the read type. This run
reproduces it on every arm: 547k–648k on the leader, and 549k–650k per host in
both spread arms.

What that does to the bars:

- **Q1** passes, but at 0 writes both arms sit on the same sleep-bound ceiling,
  so the ratio measures the cap, not the read paths. With writes, where the
  cap is gone, ryw/leader ÷ snap/leader reads 1.10, 1.05, 0.94, 1.06 at 20k,
  50k, 100k and 200k: the same conclusion (read-your-writes on the leader costs
  about what a snapshot read costs), but that is a descriptive reading, not the
  pre-committed bar.
- **Q2** is the least affected: both of its arms are capped, and it passes with
  margin both at 0 writes (3.21) and everywhere the cap is gone (3.78–4.63).
- **Q3** compares each arm against a depressed baseline, so "no starvation"
  is guaranteed by construction and says little. A fairer descriptive reading
  is each arm at 200k against itself at 20k (not a bar): reads/s ×0.92
  (lin/leader), ×1.13 (snap/leader), ×1.09 (ryw/leader), ×1.00 (snap/spread),
  ×1.00 (ryw/spread); worst-host p99 ×1.39, ×0.98, ×1.11, ×1.41, ×0.94.
  Only lin/leader loses throughput as writes rise tenfold, and only 8 %.

### 4.4 What the run says

1. **Read-your-writes reads cost about what snapshot reads cost.** On the
   leader, ryw/leader ÷ snap/leader is 0.94–1.10 across all write rates, inside
   the leader arms' rep-to-rep noise. Spread across 3 nodes, ryw/spread runs at
   0.92–0.98 of snap/spread, with the freshest possible token (§1), a token a
   real client cannot beat.
2. **Read-your-writes on all nodes gives 3.2–4.6× the read capacity of
   linearizable reads on the leader** (Q2), and 1.15–1.44× even on the leader
   alone (ryw/leader ÷ lin/leader), the quorum round it skips.
3. **Writes up to ~199k/s do not starve any read type.** The "applies all
   committed frames before answering queries" ordering did not show up as a
   cost at these rates: reads/s is flat from 20k to 200k writes/s on every arm
   but lin/leader (−8 %), and no host ever retried.
4. **Followers serve more than the leader in the spread arms.** With writes,
   followers answer 2.8–3.6M reads/s each and the leader host 2.4–3.1M (it also
   runs the writer). Per node, spread reads match or beat the solo leader arm,
   so spreading reads adds capacity in proportion to node count, the same
   finding as the 2026-10-07 run.
5. **Client-bound cells.** snap/spread at 50k–200k writes/s ran with a minimum
   in-flight depth as low as 43 % on some host (5 of its 6 reps at 43–89 %) (§3: a client-bound rung shows as a
   depth below its target). Those cells are lower bounds on node capacity.
   Every other cell held ≥ 93 %.
6. **The idle cap is the largest effect in the run.** A read-only node runs
   3.5–5.4× slower than the same node under any write load. Fixing it (count
   answered queries as apply progress) is outside this instrument. A
   per-process workaround already exists: `UC2_APPLY_IDLE=spin` (or `yield`)
   replaces the default backoff ladder (`uc_service/src/lib.rs`, since
   2026-09-17), at the cost of a core per service. This run left it unset
   (the default ladder, 50 µs sleep rung), as a deployment would; the
   2026-10-07 local smoke showed `spin` lifting read-only reads about 6×.

## Follow-up (2026-10-10, appended after the results above)

The read-only idle cap is fixed on branch `fix/query-drain-progress`:
`apply_cycle` now counts answered queries as progress. Fleet A/B in
[`uc2-query-drain-progress-2026-10-10.md`](uc2-query-drain-progress-2026-10-10.md):
read-only reads rose 3.8–5.6× on every arm. The numbers above were measured
before the fix and stand as recorded.

## Follow-up (2026-10-11, appended after the results above)

The same five arms ran on 3 × `c9gd.4xlarge` with write rungs up to 2 M/s and
an unpaced ceiling:
[`uc2-read-types-under-writes-c9gd-2026-10-11.md`](uc2-read-types-under-writes-c9gd-2026-10-11.md).
Reads there run 1.6–2.2× the 200 k column here; no read type starved up to
2 M committed writes/s; and the leader-side read-your-writes ÷ snapshot ratio
read 0.83 against the same ≥ 0.90 bar, recorded as a miss. The numbers above
stand as recorded.
