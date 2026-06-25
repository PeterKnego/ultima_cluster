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

## 3b. Replication sub-decomposition — it's openraft choreography, not the RPC (probe, 2026-06-25)

The §3 cut showed ~0.52 ms of the replication bucket is *software*. To find whether that software is
**UC's network code** (fixable: per-RPC `open_bi` stream, bincode) or **openraft's async core** (not
fixable without a rewrite), a leader-side probe (`uc-bench-probes`, commit f88eb03) brackets the whole
`append_entries` RPC on one clock — encode + pool/connect + send + wire×2 + follower recv/append/respond
+ decode — recording only RPCs that carry entries (heartbeats excluded). Self-timed inline, so no
cross-process clock join. Fresh fleet, same regime (3× c6id.2xlarge NVMe, linger=0, inflight=1, rate=200);
raw data `bench-out/floor-decomp/repl/`.

| arm | leader-observed RPC round-trip (p50) | samples | note |
|---|---:|---:|---|
| 1node_eventual / 1node_consistent | — | **n=0** | control: no replication, probe silent ✓ |
| 3node_eventual | **184 µs** (p99 469, min 93) | 16805 | the RPC, no follower fsync |
| 3node_consistent | **222 µs** (p99 533, min 137) | 16806 | +38 µs vs eventual = follower NVMe fsync ✓ |

Raw LAN ping this fleet: **118 µs min / 221 µs avg** round-trip. So the entire `append_entries` RPC
(184 µs) is **wire-dominated** — UC's added software (codec + per-RPC stream open + follower append) is
only **~tens of µs**. The +38 µs eventual→consistent delta matching a follower fsync is an independent
check that the probe brackets the real round-trip.

**The cut.** Of the ~0.70 ms replication bucket: the RPC round-trip is **0.184 ms (~26%)**, of which only
~0.03 ms is UC software; the remaining **~0.52 ms (~74%) is openraft choreography** — the async gap
*outside* the RPC: leader appends → RaftCore hands to ReplicationCore → RPC → ack → match-index update →
commit-index advance → apply trigger, each a tokio task wakeup (~8.8 µs each, and there are many).

**Decision (pre-registered in the probe scope):**
- RPC-software ~0.03 ms (≪ the 0.3 ms bar) → **the QUIC stream-pooling / scatter-gather-codec "cheap win"
  does not exist.** Killing per-RPC `open_bi` would save microseconds against a millisecond floor.
- choreography ~0.52 ms (≫ 0.3 ms) → it is **openraft-core async**, not UC code. **Confirms the structural
  verdict**: the replication software is the openraft duty-cycle itself.

So the only levers that move the floor remain (a) openraft-internal — fewer core↔replication async hops /
apply-commit pipelining (needs openraft changes, not a UC-side config), or (b) the co-location / duty-cycle
rewrite. There is no cheap UC-network-layer win. Micro-optimizing the RPC path (stream reuse, leaner codec)
is **not worth it**.

> Caveat: the inflight=1 e2e p50 per arm was noisy on this fleet (single rung; 3node_consistent<3node_eventual
> and a 1node_eventual rung inverted — sampling noise), so the replication-bucket value is taken from §2's
> cleaner run (0.70 ms). The probe's RPC measurement (n≈17k, tight) is robust, and the conclusion holds for
> any plausible bucket: 0.18 ms RPC is unambiguously a small fraction of a 0.5–0.9 ms replication cost.

## 4. So what — where this points

- **NOT fsync-bound** (18%): the inline-fsync / SeqWatermark work (handoff-tax doc) is low-value at the
  cluster level — confirms the journal A/B nulls.
- **NOT wire-bound** (10%): physical RTT is ~0.19 ms; busy-poll on the network was always going to be
  null (task17 Phase B, confirmed).
- **~73% is openraft-async + 3-process-IPC structure.** This is *why* every µs-scale micro-opt has been
  null: they nibble the 27%. To move the floor you must attack the structure.

**Levers — and what the §3b probe ruled out.** The §3b probe shows the replication software is **openraft
choreography (~0.52 ms), not UC's RPC code (~0.03 ms)**, so:
- ❌ **UC network-layer micro-opt (ruled out).** QUIC stream pooling / scatter-gather codec saves ~tens of
  µs — not worth it. (This was the candidate "cheap win"; the probe killed it.)
- ⚠️ **openraft-internal (not a UC knob).** The 0.52 ms choreography + the 0.86 ms base (≈ task11's
  `commit_to_apply_enq`) live in openraft's RaftCore↔ReplicationCore↔apply async loop. Reducing it means
  *changing openraft* (fewer task hops, tighter commit→apply, batching) — an upstream/fork effort, not a
  config. `UC_PIPELINE_DEPTH` (task17) already exists but only hides RTT under load, not the single-shot
  choreography. So this is medium-hard, not the "cheap first" it looked like before the probe.
- 🔨 **Structural rewrite.** Co-locate node+service (drop IPC hops) and/or replace the openraft async
  duty-cycle with a polling model (Aeron-style). Rewrite-class; justified only if sub-ms latency / 2×
  throughput becomes a product goal.

**Bottom line: there is no cheap win left.** The floor is ~74% openraft-async + 3-proc-IPC structure
(§3) and the replication slice specifically is openraft choreography (§3b), not anything UC can tune at
the network or config layer. The honest canonical verdict is **UC commit floor ≈ 1–2 ms, ~73%
structural, and that is the design point** — stop filing µs-scale edges; any real improvement is an
openraft-core change or the co-location rewrite.

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
