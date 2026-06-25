# Commit-path floor decomposition — where the ~1 ms goes

**Date:** 2026-06-25.
**Question:** after task11 (event-driven wakeups) collapsed the commit floor to ~1 ms, and after a
string of µs-scale optimizations all came back NULL end-to-end (O1 busy-spin / task18, task17 Phase B
busy-poll, journal prealloc, fdatasync), *what is the millisecond actually made of?* This decides
whether any further perf work is worth it and which lever to pull.

**Verdict:** the floor is **~73% software/structural** (openraft async commit→apply + 3-process IPC +
openraft replication pipeline) and only **~27% physical** (NVMe fsync + wire RTT). Micro-optimizations
were correctly null — they nibble the 27%. The only lever that moves the floor is **structural**
(co-locate node+service / tune-or-replace the openraft duty-cycle), with a cheaper *openraft-internal
tuning* probe available first (see §4).

---

## 1. Method — layered e2e-delta (no probes)

The existing per-stage probe harness (`uc-bench-probes`, `attribution-bench`) is **single-process,
in-process, single-node, `current_thread`, eventual-durability** — not a faithful fleet proxy (no
replication, cooperative scheduling, no fsync, and a process-local clock that can't join across the 3
fleet processes). True per-stage fleet probes = the deferred task09 "Phase 4" cross-process,
clock-aligned build. Instead we used **black-box subtraction** (task13's method, refreshed on NVMe at
linger=0), which needs zero probe code: measure unloaded e2e p50 across a 2×2 where each config adds
exactly one layer.

| Config | adds | isolates |
|---|---|---|
| `1node_eventual` | — | **base**: IPC ring hops + openraft commit→apply cycle + apply (no fsync, no replication) |
| `1node_consistent` | local fsync | **fsync** = `1node_consistent − 1node_eventual` |
| `3node_eventual` | replication | **replication** = `3node_eventual − 1node_eventual` |
| `3node_consistent` | both | **cross-check** ≈ base + fsync + replication |

**Floor = unloaded.** At `inflight=1` the max achievable rate is `1/latency` (~1000/s at a 1 ms floor),
so the rate ladder must stay **below** that or the driver backlogs and p50 explodes. Used `inflight=1`,
`rates=200,500`; rate=200 is the authoritative (least-queued) floor.

**Setup:** AWS 3× `c6id.2xlarge` (8 vCPU, local **NVMe** instance store at the journal `--data-dir`),
single-AZ cluster placement group, us-east-1, QUIC, `UC_API_BATCH_LINGER_MS=0`, 64 B payload. Built from
branch `prototype/o1-busyspin-apply-consumer` via bench-infra rsync. The 1-node arms use the new
`single_node=true` knob (node0 bootstraps as a 1-node raft, peer set = itself). Raw data:
`bench-out/floor-decomp/fleet/floor_fleet.csv`.

> Instance note: dropped from `c6id.4xlarge` to `c6id.2xlarge` to fit the account's 32 on-demand vCPU
> limit (3×16=48 exceeded it; 3×8=24 fits). The floor is measured at `inflight=1` (not CPU-bound), and
> fsync (same NVMe SSD class) + RTT (same placement group) are preserved, so the decomposition is
> unaffected. Absolute floor here (~1.9 ms) runs a touch higher than the 4xlarge A/B (~0.85–1.4 ms),
> consistent with the smaller host adding scheduling latency — the **ratios** are the result.

## 2. Results (p50, inflight=1, linger=0)

| config | rate=200 p50 | rate=500 p50 |
|---|---:|---:|
| 1node_eventual | 0.861 ms | 0.956 ms |
| 1node_consistent | 1.192 ms | 1.144 ms |
| 3node_eventual | 1.565 ms | 1.522 ms |
| 3node_consistent | 1.883 ms | 1.439 ms |

Using rate=200 (cleanest):

| Bucket | derivation | p50 | % of 1.88 ms floor |
|---|---|---:|---:|
| **base** (IPC + openraft commit→apply + apply) | 1node_eventual | **0.861 ms** | **46%** |
| **replication** (QUIC RT + remote append to majority) | 3node_evt − 1node_evt | **0.704 ms** | **37%** |
| **fsync** (local NVMe journal durable) | 1node_cons − 1node_evt | **0.331 ms** | **18%** |
| total (cross-check) | base+fsync+repl = **1.896** vs measured **1.883** | | **✓ within 0.7%** |

The cross-check (predicted 1.896 vs measured 1.883 ms) validates that the layers compose additively.

## 3. The key cut — software vs physical

A raw LAN ping between nodes (private IP) is **0.186 ms round-trip** (0.171/0.186/0.207 min/avg/max).
But the **replication bucket is 0.704 ms** — so only ~0.19 ms is wire and **~0.52 ms (74% of
replication) is openraft replication-pipeline software** (serialization + append-entries through the
async task/channel machinery), not network. Re-bucketing the ~1.88 ms floor by *nature*:

| Nature | components | p50 | % |
|---|---|---:|---:|
| **Software / structural** | base 0.86 + replication-software 0.52 | **~1.37 ms** | **~73%** |
| **Physical I/O + wire** | NVMe fsync 0.33 + wire RTT 0.19 | **~0.52 ms** | **~27%** |

The base 0.86 ms is consistent with task11's finding that the post-wakeup single-node floor is dominated
by `commit_to_apply_enq` (~672 µs) — openraft's internal commit→apply handoff.

## 4. So what — where this points

- **NOT fsync-bound** (18%): the inline-fsync / SeqWatermark work (handoff-tax doc) is low-value at the
  cluster level — confirms the journal A/B nulls.
- **NOT wire-bound** (10%): physical RTT is ~0.19 ms; busy-poll on the network was always going to be
  null (task17 Phase B, confirmed).
- **~73% is openraft-async + 3-process-IPC structure.** This is *why* every µs-scale micro-opt has been
  null: they nibble the 27%. To move the floor you must attack the structure.

**Two levers, cheap-first:**
1. **openraft-internal tuning (cheaper, do first).** The ~0.52 ms replication-software and the ~0.86 ms
   base are both openraft-internal, not I/O. Probe: replication pipelining (`UC_PIPELINE_DEPTH`, task17),
   apply batching, and the commit→apply duty-cycle. A targeted win here needs no rewrite. The 0.52 ms
   replication-software (≈2.7× the wire RTT) is the most suspicious single number — start there.
2. **Structural rewrite (expensive, gate on #1).** Co-locate node+service (drop the IPC hops) and/or
   replace the openraft async duty-cycle with a polling model (Aeron-style). Rewrite-class; only justified
   if #1 stalls and sub-ms latency / 2× throughput becomes a product goal.

If neither is pursued: the honest canonical verdict is **UC commit floor ≈ 1–2 ms, ~73% structural, and
that is the design point** — stop filing µs-scale edges.

## 5. Reproduce

```bash
cd bench-infra && make up-uc        # 3x c6id (NVMe). c6id.2xlarge fits the 32-vCPU on-demand limit.
cd ansible
common="-e aeron_enabled=false -e uc_api_batch_linger_ms=0 -e inflight=1"
ladder='{"rate_ladder":[200,500]}'
ansible-playbook bench.yml $common -e "$ladder" -e single_node=true -e durability=none        # 1node_eventual
ansible-playbook bench.yml $common -e "$ladder" -e single_node=true -e durability=consistent  # 1node_consistent
ansible-playbook bench.yml $common -e "$ladder" -e durability=none                            # 3node_eventual
ansible-playbook bench.yml $common -e "$ladder" -e durability=consistent                      # 3node_consistent
# results: bench-out/dist/<ts>/node0/uc_sweep.csv (config column = <n>node_<dura>); make destroy
```

A local sandbox dry-run (`bench-infra/scripts/run_floor_decomp_local.sh`) validates the harness
mechanics but cannot resolve the deltas (4 vCPU running 3×(node+service) co-located, no real network).
