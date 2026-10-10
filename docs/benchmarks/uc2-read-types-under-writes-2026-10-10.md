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

_Not yet run._
