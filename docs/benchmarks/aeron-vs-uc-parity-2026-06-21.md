# Aeron vs UC parity sweep — AWS c6id, non-durable

**Date:** 2026-06-21
**Hardware:** AWS 3× `c6id.4xlarge`, us-east-1, placement group.
**Config:** `durability=none` (both non-durable: UC eventual / Aeron `fileSyncLevel=0`), payload 64 B,
rate ladder 100→20k msg/s, UC `inflight=128`, **UC `api_batch_linger_ms=5`**, Aeron ingress=UDP
(its native cluster transport). UC inter-node transport A/B'd: **QUIC** and **UDP**.
**Harness:** `bench-infra` parity sweep (`make up` + `bench.yml` ×2 transports); Aeron via aeron-benchmarks
`LoadTestRig` (`master`). UC built against a clean `origin/main` ultima_db snapshot (the working copy was
mid-edit by another session).

## Results (latency in ms; one accumulated HdrHistogram per rung)

### UC-QUIC vs Aeron
| rate | UC ach | UC p50 | UC p99 | UC p99.9 | Aer p50 | Aer p99 | Aer p99.9 |
|---|---|---|---|---|---|---|---|
| 100 | 100 | 8.14 | 8.83 | 8.87 | 0.11 | 0.34 | 1.41 |
| 500 | 500 | 8.87 | 13.77 | 67.24 | 0.09 | 0.23 | 1.51 |
| 1000 | 1000 | 9.01 | 33.16 | 69.27 | 0.08 | 0.23 | 0.44 |
| 2000 | 2000 | 8.75 | 64.32 | 77.59 | 0.09 | 0.21 | 0.22 |
| 5000 | 4998 | 9.91 | 88.87 | 98.70 | 0.08 | 0.22 | 0.23 |
| 10000 | **7809** | 1296 | 2806 | 2812 | 0.08 | 0.22 | 0.22 |
| 20000 | **10452** | 4073 | 8900 | 9144 | 0.08 | 0.22 | 0.24 |

### UC-UDP vs Aeron
| rate | UC ach | UC p50 | UC p99 | UC p99.9 | Aer p50 | Aer p99 | Aer p99.9 |
|---|---|---|---|---|---|---|---|
| 100 | 100 | 7.28 | 8.00 | 8.04 | 0.11 | 0.27 | 0.44 |
| 500 | 500 | 7.62 | 14.11 | 67.63 | 0.09 | 0.22 | 0.23 |
| 1000 | 1000 | 10.46 | 34.41 | 70.19 | 0.08 | 0.22 | 0.22 |
| 2000 | 1999 | 9.23 | 64.98 | 77.07 | 0.08 | 0.21 | 0.22 |
| 5000 | 4997 | 10.20 | 90.11 | 99.48 | 0.08 | 0.21 | 0.23 |
| 10000 | **8063** | 825 | 2408 | 2412 | 0.08 | 0.22 | 0.24 |
| 20000 | **10478** | 4068 | 8850 | 9093 | 0.08 | 0.21 | 0.22 |

## Findings

1. **Aeron is ~100× lower p50 and ~2× higher throughput in this config.** Aeron holds ~**80 µs** p50 and
   sub-ms p99.9 *flat* through 20k msg/s (it sustains the full offered rate: 200k samples @ 20k). UC
   saturates at **~10k msg/s** (achieved 7.8–10.5k at the top rungs) and its latency explodes past the
   knee (seconds).

2. **UC's p50 floor is the `api_batch_linger`, not the network.** At rate 100 (one msg per 10 ms, zero
   queueing) UC is still ~8 ms p50 vs Aeron's 0.11 ms — that ~8 ms ≈ the **5 ms `UC_API_BATCH_LINGER_MS`**
   + Raft replication (~2.7 ms) + shmem IPC. This is a deliberate throughput-batching tradeoff, not a
   transport cost. A `UC_API_BATCH_LINGER_MS=0` run would slash UC p50 (at some throughput cost) — the
   honest low-latency comparison, not done here.

3. **UC QUIC ≈ UDP on this fleet.** The two transports are within noise across the ladder; UDP is
   marginally better at the saturation knee (8063 vs 7809 achieved @ 10k target). Neither changes the
   linger-bound p50 or the ~10k/s ceiling. Consistent with task16's "the transport edge is
   network-dependent" — on this c6id placement group it's a wash (vs task16's earlier AWS result where
   QUIC edged UDP).

## Caveats (important)

- **Not a like-for-like latency comparison.** Aeron's `LoadTestRig` measures raw cluster-message RTT
  (open-loop, no app-level batching); UC's `commit-path-load` measures the full client→submit→**linger-batch**
  →replicate→apply→response path. The 100× p50 gap is dominated by the 5 ms linger + the SMR pipeline, not
  a raw wire-speed deficit. The fair comparisons are: **throughput ceiling** (Aeron ~2× here) and a
  **linger=0 UC latency** run (not measured).
- **Single run per config** (no interleave). Aeron's numbers are flat/stable across rungs (low variance);
  UC's saturation knee (7.8–10.5k) can drift run-to-run, so treat the ceiling as ~10k ± noise.
- `durability=none` isolates transport+pipeline from fsync (matched: UC eventual / Aeron
  `fileSyncLevel=0`). A `durability=consistent` parity would add per-commit fsync to both and is a
  separate run.

## Bottom line

On raw cluster latency + throughput, **Aeron substantially leads** (~80 µs vs ~8 ms p50; ~20k vs ~10k/s)
— as expected: UC is a durable SMR application server with a deliberate batching linger, not a bare
low-latency messaging layer. UC's headline latency is **linger-bound** (tunable) rather than
transport-bound, and **QUIC vs UDP makes no material difference here**. To chase the gap meaningfully:
(a) re-run with `UC_API_BATCH_LINGER_MS=0` for the true latency floor, and (b) investigate the ~10k/s
throughput ceiling (where the open-loop driver backs up) — neither pursued in this run.

## Artifacts
`bench-out/dist/20260621T161744Z/` (QUIC) and `bench-out/dist/20260621T162148Z/` (UDP): per-node
`uc_sweep.csv` + `aeron_rung_*.hdr` (HdrHistogram compressed-log; decode with `hdrh` /
`/tmp/parse_ab.py`) + `manifest.txt`.
