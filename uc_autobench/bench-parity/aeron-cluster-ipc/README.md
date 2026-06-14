# Aeron Cluster — IPC-ingress config, matched to ultima_cluster

A drop-in variant of the aeron-benchmarks `cluster_localhost` sample that moves the
**client↔node** hop from UDP to **shared memory** (`aeron:ipc`), so the only
remaining differences vs `ultima_cluster` are the consensus/transport internals we
actually want to measure.

## Parity matrix

| Axis | ultima_cluster | aeron (this config) | Matched? |
|---|---|---|---|
| Client → node | shmem (`uc_client`) | `aeron:ipc` ingress + egress, shared driver | ✅ both shmem |
| Node ↔ node | QUIC | UDP (`replication.channel`) | ⚠️ both network, different stack — **this is the X-axis** |
| Nodes / quorum | 3 | 3 | ✅ |
| State machine | echo/no-op | `EchoClusteredService` | ✅ (use ultima's echo SM, not ultima_db) |
| Load model | open-loop fixed rate | open-loop fixed rate (`LoadTestRig`) | ✅ |
| Latency stat | `now − intended_send`, ns HdrHistogram | `now − timestamp`, ns HdrHistogram | ✅ identical method |
| Pacing | per-message | `batch.size=1` | ✅ |
| Payload | `--payload-bytes` | `message.length` (set equal) | set equal |
| Durability | ultima_journal fsync policy | aeron archive fsync policy | **must set equal** (see below) |

What's left after matching is exactly: **QUIC vs raw-UDP, ultima_journal vs aeron
archive, and ultima's (openraft-based) consensus vs aeron-consensus.** That's the
comparison.

## What changed vs the shipped sample

- `cluster.properties`: `ingress.channel` → `aeron:ipc`; `ingress.endpoints` **removed**
  (rejected for IPC, `AeronCluster.java:1466`); `appointed.leader.id=0` added.
- `client.properties`: `aeron.dir` → `/dev/shm/node0-driver` (share leader's driver);
  `egress.channel` → `aeron:ipc`; `batch.size=1`.
- `start_cluster.sh`: the separate **client media driver is not launched**.
- node{0,1,2}.properties: unchanged.

## Why node0 must be the appointed leader

IPC ingress reaches only the co-located node's driver. A *follower* responds to a
session-connect with a redirect to the leader's **UDP** ingress endpoint
(`ConsensusModuleAgent.onSessionConnect` → `sessionManager.onSessionConnect(... role,
ingressEndpoints)`). With IPC there is no endpoint to follow, so the client can only
be served if its co-located node is the leader. `appointed.leader.id=0` pins that.

> Caveat: this measures **steady-state on a fixed leader**. It does not exercise
> leader-change/failover (a co-located IPC client cannot fail over to a remote
> leader). If you want failover numbers, that's a separate scenario.

## Durability — set this equal or the comparison is invalid

fsync/group-commit dominates tail latency. Decide one posture and apply to both:
- **aeron**: archive sync level (`aeron.archive.file.sync.level` / catalog sync) —
  set the same on all nodes.
- **ultima**: ultima_journal fsync/group-commit policy.
Run the whole sweep with both either durable (fsync) or both non-durable. Never mix.

## Run

```bash
# 1. Build aeron-benchmarks (./gradlew clean deployTar) and point AERON_SCRIPT_HOME
#    at its scripts/aeron dir (required — that checkout lives outside this repo).
export AERON_SCRIPT_HOME=/path/to/aeron-benchmarks/scripts/aeron

# 2. Start cluster + run client (blocks, prints RTT HdrHistogram at the end).
./start_cluster.sh

# 3. Sweep the rate to build a latency-vs-throughput curve; match the rate ladder
#    you use for ultima's commit-path-load.rs. Set message.length == ultima payload.
```

## Cross-host (the fair distributed run)

Replace `localhost` in `cluster.properties` `aeron.cluster.members` with real node
IPs, run one node per host, and keep the **client on node0's host sharing
`/dev/shm/node0-driver`**. Then: client edge = shmem (both), intra-node = real
network (UDP here / QUIC for ultima). That is the apples-to-apples distributed point.

## Histogram overlay

`LoadTestRig` writes an HdrHistogram (`.hgrm`-style percentile distribution). Add a
small `.hgrm` exporter to the Rust side (the `hdrhistogram` crate supports interval
log / percentile output) so aeron + ultima plot on one chart.
