# Journal prealloc fill strategy — end-to-end cluster A/B (submitted→persisted)

**Date:** 2026-06-21
**Hardware:** AWS 3× `c6id.4xlarge`, us-east-1, placement group, journal on local NVMe (`/opt/bench`, ext4).
**Cluster:** UC-only 3-node (`dist_3node`), `Durability::Consistent`, `inflight=8`, preallocation ON.
**Follow-up to:** `docs/benchmarks/journal-prealloc-fill-ab-2026-06-21.md` (the microbench A/B that shipped
`FallocateZeroRange` as the default) and the recommendation there to check the win end-to-end.
**Runbook basis:** `uc_autobench/scripts/prealloc-commit-ab.md` (adapted for the 3-way fill A/B).

## TL;DR — verdict

**The journal fill-strategy win is NULL end-to-end — fully masked at the cluster level.** The depth-1
microbench win (`full` 2.90 ms p99 → `fallocate` 0.175 ms) does **not** surface in the cluster's
`submitted→persisted` distribution: P50/P90/P99 are statistically identical across `full`, `paced`, and
`fallocate` (~153 / 211 / 245 µs), because group commit at `inflight=8` amortizes the per-commit barrier.
This **confirms the runbook's central prediction** and is consistent with the prior fdatasync and
segment-preallocation e2e NULL results — the commit path is dominated by `api_batch_linger` (~6.5 ms) +
replication, with the journal stage a small slice.

The shipped default flip to `FallocateZeroRange` remains justified by the **microbench depth-1 latency
win + the +43 % preallocated-throughput gain** — not by any end-to-end cluster latency change. No e2e
regression observed.

## Primary metric — `submitted→persisted` (leader journal append+fsync stage, µs)

From openraft's `runtime-stats` `log_stages` (cumulative per node-launch), last dump per pass.
Interleaved A/B/C ×2 for host-drift control.

| pass | fill | P50 | P90 | P99 | P99.9 |
|---|---|---|---|---|---|
| a1 | full | 153 | 213 | 245 | 212992 (~213 ms) |
| a2 | full | 151 | 210 | 243 | 212992 |
| b1 | paced | 153 | 211 | 240 | 229376 (~229 ms) |
| b2 | paced | 155 | 212 | 241 | 212992 |
| c1 | fallocate | 163 | 210 | 243 | **384** |
| c2 | fallocate | 164 | 211 | 265 | 212992 |

(`212992`/`229376` are openraft log-scale histogram **bucket boundaries** — i.e. "an outlier landed in
the ~213–229 ms bucket", not a precise value.)

### Reading the table

- **P50–P99: NULL across all three strategies** (~151–164 / 210–213 / 240–265 µs). The 2.90 ms depth-1
  microbench tail is not even visible here — group commit amortizes it away, exactly as predicted.
- **P99.9: a ~213 ms outlier that is intermittent and strategy-INDEPENDENT.** It appears in 5 of 6
  passes — under `full`, `paced`, AND `fallocate`. `fallocate`'s `c1` (384 µs) was the lone stall-free
  window; its `c2` hit 213 ms like the rest. **The interleaved design caught this**: a single
  `fallocate` pass (c1) would have looked like a decisive e2e win, but the second pass (c2) falsified it.
- Therefore the ~213 ms P99.9 stall is **not** the journal background-fill contention (the microbench
  root cause) — `fallocate` does essentially no background I/O yet still shows it. It is a different,
  intermittent leader-path event. The stats show `PurgeLog: 92` and `SM::BuildSnapshot: 92` over the
  run, so log compaction / snapshot-build (or a periodic fs/checkpoint stall) is the likely source — a
  separate lead, out of scope here.

### Why the microbench win vanishes end-to-end

- `proposed→received` P50 ≈ **6521 µs** (the `api_batch_linger` window) dwarfs the entire
  `submitted→persisted` stage (~153 µs P50). The journal fsync is a few percent of the commit path.
- At `inflight=8`, multiple commits coalesce into each group-commit fsync, so the per-commit barrier
  (which the fill strategy governs) is paid once per batch, not per commit — the depth-1 serial regime
  the microbench measures never occurs under cluster load.

## Secondary metric — end-to-end submit→response

Only the **final pass (c2)** sweep CSV survived (the per-pass `bench-out/dist` fetch path was
misconfigured in the loop; node0's `/opt/bench/results/uc_sweep.csv` is overwritten each pass). That
sample shows the cluster **saturated** at `inflight=8`: achieved ≈ 620–1180 ops/s against offered rates
of 1k–20k, so the reported p50/p99 are seconds-scale queueing artifacts, not per-commit latencies — not
usable for an A/B comparison. This does not affect the verdict: with the primary `submitted→persisted`
NULL across strategies, the downstream end-to-end latency is necessarily non-differentiating too. (A
clean e2e secondary would require sub-saturation rates + fixing the CSV collection; not worth the fleet
cost given the primary is decisive.)

## Method / setup

- Measurement-only branch `profile/raftcore-stats-fill-ab` off `main`: openraft `runtime-stats` feature
  + `Node::runtime_stats_display()` + a 5 s `RAFT_RUNTIME_STATS` dump loop in `uc-node-launch`, plus
  `UC_JOURNAL_PREALLOC_FILL` threaded through the `run` role (group var + export). **Kept off `main`**
  per the runbook (measurement instrument, not product code).
- ultima_db path-dep shipped to the fleet as a clean `origin/main` `git archive` snapshot (the sibling
  repo is being edited by another session — avoided capturing its WIP and never touched it).
- 6 interleaved passes `bench.yml -e durability=consistent -e inflight=8 -e uc_journal_prealloc=1
  -e uc_journal_prealloc_fill={full|paced|fallocate}`; `submitted→persisted` scraped from node0 stderr
  after each pass (before the next restart).

## Bottom line

End-to-end, the fill strategy is in the noise — confirming the journal p99 fix is a journal-local /
single-node-latency win, masked in the cluster by linger + replication + group commit (the same reason
the fdatasync and segment-prealloc A/Bs read NULL e2e). The shipped `FallocateZeroRange` default stands
on its microbench latency + throughput merits. Separate lead surfaced: an intermittent ~213 ms
`submitted→persisted` P99.9 stall, strategy-independent, likely log-purge/snapshot-build — worth its own
investigation if cluster tail latency becomes a target.
