# UC v2 — Read spread: does snapshot read capacity scale with the nodes that answer?

**Date:** 2026-10-07
**Status:** bar pre-committed before any run (this section is fixed once data
exists; results are appended below it, never edited into it).
**Kind:** INSTRUMENT, not a gate. No feature ships or is refused on it; it
decides whether follower ReadIndex or session-token reads are worth designing.
**Context:** `docs/notes/smr-read-options-compared.md` ("What UC does"), and
`docs/benchmarks/uc2-read-profile-2026-07-26-after-rung-a.md`, which found
linearizable and snapshot reads equal at plateau on the leader — so the
barrier no longer limits reads, and the next lever is *where* the query runs.

## 1. Question

UC answers a linearizable read only on the leader. Two read paths would move
query work onto followers: **follower ReadIndex** (still linearizable) and
**session-token reads** (read-your-writes). Both are worth building for
throughput only if read capacity actually grows when more nodes answer.

UC already answers **snapshot** reads on every node: the query drain forwards
a non-linearizable read to the local service before its leader check
(`uc_node/src/node.rs`, the `FLAG_V2_LINEARIZABLE` branch of the query drain).
A snapshot read pays neither the barrier nor a wait for the read position. So
snapshot reads spread over all nodes are the **upper bound** for both
candidates, measurable with no product change.

## 2. Arms

Harness: `uc_node/examples/read_profile.rs` (`client --mode snap`, plus a new
`--any-role` flag so a client can load a follower), driven by
`bench-infra/scripts/m6_fleet_gate.py --read-profile --rp-spread`.

- **leader:** one snapshot client on the leader's host, `R` reads in flight.
- **spread:** one snapshot client on **each** of the 3 hosts, concurrently,
  `R` reads in flight each.

Both arms: a fresh 3-node cluster per rung; `R ∈ {64, 256, 1024}` per host;
write mix `w ∈ {0, 20000}` writes/s, issued by the leader-host client only in
both arms (so the write load is identical); 20 s measured per rung; **2 reps**
per cell, arm order alternated per rep. Fleet: 3 × `c6id.2xlarge`, us-east-1,
the same class as the 2026-07-26 read profile.

**Why 2 reps.** The 2026-07-26 runs reproduced a snapshot plateau across two
fleets within 0.3 % (mixed) and 7.6 % (read-only). The verdict bands below are
60 % of the ideal ratio apart, so two reps per cell resolve them; the reps are
reported individually so their spread is visible.

## 3. Statistic and bar

**S(w) = mean over reps of the spread arm's total reads/s ÷ mean over reps of
the leader arm's reads/s, at R = 1024 per host** (the 2026-07-26 plateau rung),
judged separately for each write mix. The ideal is 3.0.

| S | verdict | meaning |
|---|---|---|
| S ≥ 2.4 (≥ 80 % of ideal) | **SCALES** | read capacity is per node; follower ReadIndex / session tokens could lift read throughput by up to about S× |
| 1.5 ≤ S < 2.4 | **PARTIAL** | something shared limits reads; the per-host numbers say which host lags, and that is the next question |
| S < 1.5 | **DOES NOT SCALE** | moving reads to followers would not raise capacity; neither candidate is worth building for throughput |

**Validity, per client:** exit 0 (which already means no unresolved reads at
the end), retries + redirects ≤ 5 % of resolved reads, mean in-flight depth
≥ 90 % of `R`, and exactly one serving leader after the window. A rung with
any invalid client is INVALID and never substituted; a missing plateau rep
makes that mix's verdict INCONCLUSIVE.

## 4. What this does not measure

- The **cost** either candidate adds. Follower ReadIndex adds a read-position
  request to the leader (batchable) and a wait for the follower to apply up to
  it, which under write load adds replication lag to every read. Session
  tokens add the apply wait only. Both make S an upper bound, not a forecast.
- **Latency** is reported per host but not judged.
- Clients share a host with a node and a service in both arms, so per-host CPU
  contention is the same shape in both; the 3 clients' windows start within
  about a second of each other (separate ssh sessions), which this doc accepts
  as noise against 20 s windows.

## 5. Results

Run 2026-10-07 on `bench/read-spread` @ `0ae3e6e` (`origin/main` `cc5b064` plus
the two harness commits), 3 × `c6id.2xlarge`, us-east-1, placement group.
Raw data: `uc2-read-spread-2026-10-07-rungs.jsonl` (the 19 valid rungs). Host 0
won every election, so "leader host" is host 0 throughout. Fleet destroyed after
the run; teardown verified by `terraform state list` returning 0 entries and an
EC2 `describe_instances` showing no live or terminating instances.

### 5.1 Verdict

| write mix | leader arm @1024 (reps) | spread arm @1024 (reps) | **S** | verdict |
|---|---|---|---:|---|
| read-only | 647,973 · 583,073 | 1,681,205 · 1,688,975 | **2.74** | **SCALES** |
| 20k writes/s | 2,233,338 · 2,868,107 | 9,113,874 · 9,485,676 | **3.65** | **SCALES** |

Both mixes clear the 2.4 bar; both plateau cells have both reps valid. **Snapshot
read capacity is per node.** Moving reads onto followers (follower ReadIndex or
session tokens) has up to about 2.7× (read-only) to 3.6× (mixed) of capacity
headroom on 3 nodes, before the costs §4 lists.

### 5.2 Per host

Read-only, @1024: every host answers 551k–574k r/s in the spread arm, leader and
followers alike (p50 1.82–1.89 ms). The leader arm's solo host read 648k and 583k,
so S < 3.0 here is the solo host running about 10 % above its spread-arm self,
not a follower deficit. The same per-node ceiling (~550k) appears at 256 and 64
readers, and matches the ~540k read-only plateau of
`uc2-read-profile-2026-07-26-after-rung-a.md`.

Mixed, @1024: the leader host reads 2.58M–2.66M r/s in the spread arm, the
followers 2.63M–3.83M (p50 0.26–0.38 ms). Followers out-run the leader host:
it also runs the writer client and does the leader's replication work. S > 3.0
is that asymmetry, not measurement error: the leader arm is bounded by the
busiest host, the spread arm adds two less-loaded ones.

### 5.3 Validity

**19 of 24 rungs valid; the 5 invalid rungs are all mixed-arm, at 64 or 256
readers, all on the same clause:** host 0's client held mean in-flight depth
44–226 against targets of 64/256 (`leader` r64 rep2, r256 rep1; `spread` r64
rep2, r256 rep1 and rep2). One such log shows why: the client completed 2.35M
snapshot reads/s at p50 0.116 ms while holding 226 of 256 in flight. At that rate
one client send loop cannot refill its window: **the client, not the node, is the
limit at low depth under write load.** The clause exists to keep exactly those
rungs out of the record, and none is substituted. No rung failed any other
clause: zero retries, zero redirects, no unresolved reads, one serving leader
after every window. Neither plateau cell is affected.

A driver flaw found during the run: when the first host's client was invalid,
the driver returned before writing the other hosts' logs, so the follower logs of
the invalid spread rungs were not kept. Fixed after the run (every host's log is
written before validation); no valid rung lost data.

### 5.4 Side finding: the read-only ceiling looks like an idle-ladder artifact

Reads run **4–5× faster per node under 20k writes/s than with no writes** (about
2.6M vs 0.55M r/s per host at 1024; p50 0.34–0.46 ms vs 1.58–1.89 ms). The
2026-07-26 doc recorded the same inversion and left it unexplained (§4 there).
The code suggests a mechanism. **It is NOT measured here**: no run isolated it.

- The service answers queries on its apply thread: `drain_queries` runs once per
  `apply_cycle`, at most `QUERY_DRAIN_PER_CYCLE = 64` per cycle
  (`uc_service/src/apply.rs`).
- `apply_cycle` returns `progressed` from applied frames only. Query work does not
  set it, so a cycle that answered 64 queries but applied nothing counts as
  **idle** to the agent runner.
- The apply agent's idle strategy is `APPLY_IDLE = Backoff { spins: 2000, yields:
  1024, sleep: 50 µs }` (`uc_service/src/lib.rs`). Under read-only load the idle
  streak never resets, so after about 3 000 cycles every cycle sleeps.
- At ≤ 64 answers per cycle and a 50 µs sleep that the kernel's default timer
  slack stretches toward ~100 µs, the per-node ceiling is about 64 / 100 µs ≈
  640k r/s. That is the observed 550k–650k.
- Under write load every applied frame resets the streak, and the ladder's awake
  window covers the gap between frames, so the thread never sleeps and answers
  far more queries per second.

If this holds, counting answered queries as progress (or a per-cycle drain bound
that matches the load) would lift read-only capacity per node by several times,
on the leader as well as followers. That is a cheaper lever than either new read
path, and it applies to linearizable reads too, since they are answered by the
same drain. Testing it is one A/B: the same read-only rung with
`UC2_APPLY_IDLE=spin` on the service, or with the progress fix.

### 5.5 Disposition

- **Follower ReadIndex and session-token reads: the capacity case is made**
  (S = 2.74 / 3.65 against a 2.4 bar). Whether either is worth building now
  depends on their costs (§4) and on whether anything needs more than one node's
  read capacity. That is a design question for the maintainer, not a run.
- **The idle-ladder finding should be tested first.** If the read-only ceiling is
  the ladder, fixing it raises single-node read capacity for every read path,
  and that changes how much headroom the follower paths are buying.
- Cost: one provision + 24 rungs + destroy on 3 × `c6id.2xlarge` (wall time not recorded precisely).
