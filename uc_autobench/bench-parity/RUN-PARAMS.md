# Matched run-params sheet — aeron vs ultima_cluster

The single source of truth for making the two latency sweeps comparable. Every
parameter below is verified against source (flag names / property names are exact).

Companion to `docs/tasks/task13_aeron_vs_uc_commit_path.md` (the canonical comparison
record); the matched Aeron config lives in `aeron-cluster-ipc/`.

## The golden rule

**Change exactly one thing across the two systems: the system.** Pick one durability
posture and one storage medium for an entire sweep, hold every parameter in the table
identical, and only then compare histograms. If `achieved_rate` ≠ `target_rate` on
either side, the run is invalid (the sender couldn't keep up) — fix and rerun, don't
compare.

## Topology (both)

- 3 nodes, quorum = 2.
- Client co-located with the leader, **shared-memory client↔node**:
  - ultima: `uc_client` over shmem (only mode).
  - aeron: `aeron:ipc` ingress+egress, client shares node0's driver — see
    `aeron-cluster-ipc/` (node0 = `appointed.leader.id`).
- Intra-node = network: ultima **QUIC**, aeron **UDP**. *(This is the axis under test —
  do not try to equalize it.)*
- State machine = **echo / no-op** on both (ultima: echo `StateMachine`, NOT ultima_db;
  aeron: `EchoClusteredService`).

## Canonical parameters

| Parameter | Value | ultima (`commit-path-load.rs`) | aeron (`io.aeron.benchmarks.*` / property) |
|---|---|---|---|
| Rate ladder (msgs/s) | `100,500,1000,2000,5000,10000,20000` | `--rates` (sweeps all in one run) | `message.rate` — **one run per rung** |
| Measurement window | 5 s per rung | `--window-secs 5.0` | `iterations=5` (iterations == seconds) |
| Warmup window | 2 s per rung, discarded | `--warmup-secs 2.0` | `warmup.iterations=2` + `warmup.message.rate=<rung>` |
| Pacing / burst | per-message (no burst) | (per-message by design) | `batch.size=1` |
| Payload bytes | 64 | `--payload-bytes 64` | `message.length=64` |
| In-flight | see *Asymmetries* | `--inflight 128` (for compared points) | (none — open-loop unbounded) |
| Idle / spin | busy-spin both | tokio current_thread | `idle.strategy=busyspin` (set for latency runs) |
| Histogram | ns, 3 sig figs | built-in (1 ns–600 s, 3 sf) | `LoadTestRig` HdrHistogram (set output unit = ns) |
| Output | CSV / hgrm | `--out bench-out/uc.csv` | `output.file` |

aeron `iterations`/`message.rate` semantics verified in `LoadTestRig.send()`:
`stopTimeNs = start + iterations*1s`, `totalMessages = iterations * messageRate`.
So `iterations=5` = a 5-second window at every rung, matching ultima's `--window-secs 5`.

## Durability posture — the trap to avoid

**Defaults are mismatched:** ultima_journal defaults to `Durability::Consistent`
(fsync per group commit); aeron archive defaults to `file.sync.level=0` (no fsync).
Comparing the defaults silently compares "fsync vs no-fsync." Force one posture for the
whole sweep. Ideally report both as separate sweeps.

| Posture | ultima | aeron |
|---|---|---|
| **Durable (fsync)** | journal `Durability::Consistent` | `aeron.archive.file.sync.level=1` + `aeron.archive.catalog.file.sync.level=1` |
| **Non-durable** | journal `Durability::Eventual` | `aeron.archive.file.sync.level=0` (default) |

> ultima's durability is set at **cluster launch** (the journal `Config.durability` on
> each node), *not* a `commit-path-load` flag. Launch the 3-node cluster in the chosen
> posture before attaching the load driver.

**Storage medium must also match.** The durable log lives on disk for both (ultima
instance dir; aeron `aeron.archive.dir = cluster/nodeX/archive`). Put both on the *same
physical disk* for a sweep (or both on tmpfs for a pure-CPU sweep — label it via
ultima's `--config single_disk` / `single_tmpfs`). Note aeron's `aeron.dir` in `/dev/shm`
is only the IPC driver buffers, not the durable log — that stays as-is.

## Asymmetries and how they're handled

- **In-flight cap.** aeron's `LoadTestRig` is a pure open-loop sender with *no* in-flight
  cap (bounded only by cluster flow control). ultima caps at `--inflight`. For the
  head-to-head, run ultima at an in-flight high enough never to bind — use `128` and
  **verify `achieved_rate ≈ target_rate`** in the CSV; if it falls short at the top
  rungs, raise it (rule of thumb: `inflight ≥ target_rate × p99_seconds`). The smaller
  `--inflight` values (1,8,32) are ultima-only diagnostics — do not compare them to aeron.
- **Payload accounting.** ultima `--payload-bytes` is the KV value size; aeron
  `message.length` is the whole message payload (8-byte timestamp + 8-byte checksum
  live inside it). Both set to 64 is the honest match; note the framing differs by a few
  bytes — irrelevant at 64 B, worth a footnote.
- **Idle strategy.** aeron's spin loop is explicit (`idle.strategy`); ultima rides the
  tokio current-thread reactor. Use busy-spin on aeron for latency runs and keep the
  ultima driver on a dedicated core; accept this as a residual methodology difference.

## Run commands

### aeron — one run per rung (durable posture shown)
```bash
cd uc_autobench/bench-parity/aeron-cluster-ipc
export AERON_SCRIPT_HOME=/path/to/aeron-benchmarks/scripts/aeron
# Per-rung overrides (repeat for 100,500,...,20000):
RATE=2000
JVM_OPTS="\
 -Dio.aeron.benchmarks.message.rate=${RATE} \
 -Dio.aeron.benchmarks.warmup.message.rate=${RATE} \
 -Dio.aeron.benchmarks.iterations=5 \
 -Dio.aeron.benchmarks.warmup.iterations=2 \
 -Dio.aeron.benchmarks.batch.size=1 \
 -Dio.aeron.benchmarks.message.length=64 \
 -Dio.aeron.benchmarks.idle.strategy=busyspin \
 -Daeron.archive.file.sync.level=1 \
 -Daeron.archive.catalog.file.sync.level=1 \
 -Dio.aeron.benchmarks.output.file=aeron_rate${RATE}.hgrm" \
 ./start_cluster.sh
```

### ultima — one run sweeps the whole ladder (durable posture)
```bash
# 1. Launch a 3-node cluster with journal Durability::Consistent, app_id uc-bench-3node,
#    instance dir on the same disk as aeron's archive. (node/journal launch config.)
# 2. Attach the open-loop driver:
cargo run -p uc_autobench --release --bin commit-path-load -- \
  --connect /path/to/uc-bench-3node/instance \
  --app-id uc-bench-3node \
  --config single_disk \
  --rates 100,500,1000,2000,5000,10000,20000 \
  --inflight 128 \
  --payload-bytes 64 \
  --window-secs 5.0 --warmup-secs 2.0 \
  --out bench-out/uc_durable.csv \
  --hgrm-dir bench-out/hgrm        # one .hgrm per (rate,inflight), µs-scaled
```

Both sides emit the **same** HdrHistogram text format (µs values, 5 ticks/half-
distance): ultima via `--hgrm-dir`, aeron via `output.time.unit=MICROSECONDS` (its
default). Drop both `.hgrm` files on
<https://hdrhistogram.github.io/HdrHistogram/plotFiles.html> to overlay them.

For the **non-durable** sweep: aeron `file.sync.level=0`, ultima cluster launched with
`Durability::Eventual`; everything else identical.

## Pre-run parity checklist

- [ ] Same host(s); for cross-host, same NIC/links; client on the leader's host.
- [ ] 3 nodes both; aeron node0 is appointed leader and co-located with the client.
- [ ] Echo SM both (ultima NOT pointed at ultima_db).
- [ ] Durability posture identical (table above) — **not the mismatched defaults**.
- [ ] Same storage medium / physical disk for the durable log.
- [ ] payload = 64, batch/burst = 1, window = 5 s, warmup = 2 s, both.
- [ ] ultima `--inflight 128`; confirm `achieved_rate ≈ target_rate` at every rung.
- [ ] Both histograms in ns, 3 sig figs; export both to `.hgrm` for one overlay chart.
