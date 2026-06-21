# Aeron vs UC — threading-handoff & data-copying investigation

**Date:** 2026-06-21
**Type:** comparative investigation — analysis + microbenchmark-validated opportunities (no fixes implemented)
**Spec:** `docs/superpowers/specs/2026-06-21-aeron-vs-uc-threading-copying-design.md`
**Plan:** `docs/superpowers/plans/2026-06-21-aeron-vs-uc-threading-copying.md`

Each finding is tagged **confidence** (`sandbox-validated` / `needs-fleet-confirmation` / `hypothesis`)
and **horizon** (`in-place tweak` / `refactor` / `long-horizon rewrite`).

---

## 1 Gap framing

### Headline numbers (AWS 3× c6id.4xlarge, placement group, durability=none, 64 B payload)

Source: `docs/benchmarks/aeron-vs-uc-parity-2026-06-21.md` (commit `e10a648`).

| metric | Aeron | UC | ratio |
|---|---|---|---|
| p50 latency (rate 100, zero queueing) | ~0.11 ms (~80 µs) | ~8 ms | ~100× |
| p99.9 latency (steady) | sub-ms | seconds past the knee | — |
| sustained throughput | 20k+ msg/s (flat) | ~10k msg/s (saturates) | ~2× |

### What the gap actually is (critical — sets the whole investigation's target)

The parity doc is explicit, and the census must respect it:

1. **The ~100× p50 gap is dominated by the deliberate 5 ms `UC_API_BATCH_LINGER_MS`, not the wire.**
   At rate 100 (one msg / 10 ms, no queueing) UC is still ~8 ms ≈ 5 ms linger + ~2.7 ms Raft
   replication + shmem IPC. This is a throughput-batching tradeoff, not a transport deficit. A
   `linger=0` run (not yet done) is the honest latency floor.
2. **The comparison is not like-for-like:** Aeron's `LoadTestRig` measures raw open-loop cluster-message
   RTT; UC's `commit-path-load` measures the full client→submit→linger-batch→replicate→apply→response
   path. The fair, transport-independent targets are the **throughput ceiling** (Aeron ~2×) and the
   **per-commit pipeline cost beneath the linger**.
3. **QUIC ≈ UDP on this fleet** — transport is not the lever (consistent with task16/task17).

**Therefore this investigation targets the per-commit pipeline cost and the throughput ceiling, not the
linger and not the wire.** The two axes — thread-handoff wakeups and payload copies — are exactly the
per-commit pipeline costs that sit *under* the linger and that *cap throughput* (every wakeup and copy is
work done per commit that limits how many commits/sec one core can push). This is a **shallow-pipeline
latency + throughput-ceiling** story, to be confirmed by the census in §2.

### Prior-work map (settled — do not re-litigate; see §guardrails)

- **Network transport** (task16 UDP, task17 pipelined append + Phase B busy-poll): worked; cross-host
  busy-poll concluded **negative** ("network was never the bottleneck — fsync/IPC dwarf RTT"). Any
  busy-poll finding here must target an **intra-host** hop and say why it differs.
- **Log storage** (journal preallocation / fdatasync / fill-strategy; `docs/wal-journal-handoff-tax-2026-06-21.md`):
  the storage cross-thread handoff (~32 µs, two wakeups/commit) is already documented, with the WAL
  inline-fsync spike and the journal `SeqWatermark` route as existing proposals. This investigation
  **cites and extends** those; its novel contribution is the **IPC ring hops** (client↔node↔service).
- **Group-commit** already amortizes the handoff under load (~2.9 µs/entry at depth). Every finding states
  which regime it helps: **serial/shallow**, **loaded**, or **both**.

---

## 2 UC commit-path census (hops + copies)

_TBD — Task 2._

---

## 3 Aeron core pattern catalog

_TBD — Task 3._

---

## 4 Aeron Cluster commit-path census

_TBD — Task 4._

---

## 5 Microbenchmark results

_TBD — Tasks 5–7._

---

## 6 Prioritized opportunities

_TBD — Task 8._

---

## 7 Synthesis

_TBD — Task 8._
