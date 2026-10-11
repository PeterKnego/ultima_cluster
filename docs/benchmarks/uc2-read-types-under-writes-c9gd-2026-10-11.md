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
