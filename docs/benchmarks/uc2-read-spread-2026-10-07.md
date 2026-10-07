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

_Not yet run._
