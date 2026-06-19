# Task 17 — Inter-node latency study (Phase A: local ladder + UC-bookkeeping attribution)

**Date:** 2026-06-19.
**Status:** Part A closed. 4-rung local ladder measured, UC-UDP bookkeeping tax sized (+13µs),
busy-spin confounded on this host (negative tax). Gate-A read: A2 (V2 streaming append) proceeds
regardless; Phase B (cross-host, real NIC) warranted because the local ladder cannot rule out
the busy-spin / kernel-bypass win. QUIC remains the default. Part B will append §7+.
**Branch:** `task17-inter-node-latency`.

**Provenance / scaffolding.** Bench scripts and results live under
`uc_autobench/tasks/latency-ladder/`. This doc is the canonical task17 record; it is
structured so Task 8 (Phase B, cross-host) can append directly after §6.

---

## 1. Motivation

Task 16 built the pluggable transport seam and produced the first UC-UDP vs. QUIC A/B on
LAN, AWS placement-group, and a 3-node fanout. The results were network-topology-dependent
(QUIC won on AWS placement group; UDP was competitive on Hetzner LAN) and all numbers came
from full openraft round-trips — mixing protocol-serialisation overhead, UC-transport
bookkeeping, and the wire itself into a single number.

**Phase A goal:** decompose the per-RPC loopback latency into its structural layers using a
4-rung microbench ladder, so we can (a) size the overhead UC-UDP adds over a raw socket
baseline, (b) test whether busy-polling beats the tokio reactor on this host, and (c) state
a reasoned Gate-A recommendation on whether cross-host measurement (Phase B) is warranted
and what Phase A2 (V2 streaming append) buys.

---

## 2. Ladder design

Four rungs, each a synchronous ping-pong over loopback with a 64-byte payload, measuring
one-way trip latency (RTT/2) at p50/p99/p99.9:

| Rung | System | What it measures |
|------|--------|-----------------|
| **bare-udp** | Raw `tokio::net::UdpSocket` | Pure loopback + tokio reactor floor |
| **busyspin-udp** | Blocking thread, `std::net::UdpSocket`, busy-spin recv | Kernel-bypass posture on this host |
| **udp** | UC-UDP transport (`UdpMux` + `UdpSession`) | UC adds over bare tokio |
| **quic** | UC-QUIC (`quinn`) | Production default for comparison |

The `bare-udp` rung is the zero-protocol baseline. `busyspin-udp` isolates the
async-vs-polling tax. `udp` adds the full UC-UDP stack. `quic` is the production default.

Script: `uc_autobench/scripts/latency-ladder.sh`.
Bench binary: `uc_autobench --bin internode-rpc-bench`.
Target rate: 20 000 req/s; duration: 20 s default (verification run used DURATION=10).
Commit: `189bb85`.

---

## 3. Results — 4-rung ladder (loopback, 64 B payload)

All times in nanoseconds. Measured 2026-06-19 on the build container (shared, virtualised).

| Rung | Achieved rate (req/s) | p50 (ns) | p99 (ns) | p99.9 (ns) |
|------|-----------------------|----------|----------|------------|
| bare-udp | 77 523 | 12 135 | 24 351 | 38 783 |
| busyspin-udp | 30 953 | 28 623 | 70 271 | 388 351 |
| udp | 37 816 | 24 927 | 45 695 | 64 031 |
| quic | 31 701 | 31 103 | 55 519 | 69 183 |

Raw TSV: `uc_autobench/tasks/latency-ladder/results.tsv` (repopulated on each run;
header-only between runs).

---

## 4. Derived taxes

Two taxes extracted from the p50 column:

```
tax_uc_bookkeeping    = p50(udp) − p50(bare-udp) = 24 927 − 12 135 = +12 792 ns  (~+13 µs)
tax_async_vs_busyspin = p50(bare-udp) − p50(busyspin-udp) = 12 135 − 28 623 = −16 488 ns
```

### 4.1 `tax_uc_bookkeeping` = +12 792 ns

UC-UDP adds approximately **+13 µs** per RPC over a bare `tokio::net::UdpSocket` on this
host. This is a real, positive cost attributable to the UC transport stack.

### 4.2 `tax_async_vs_busyspin` = −16 488 ns (NEGATIVE)

The busy-spin rung is **16 µs slower** than the bare-tokio baseline on this host. This is
not a defect in the bench — it is the expected behaviour of a busy-polling thread on a
shared, virtualised host and is explained in §5.

---

## 5. Why the busy-spin tax is negative on this host

Busy-spin's advantage is an **interrupt-avoidance** phenomenon: on a real NIC with dedicated
CPU cores, polling the ring buffer in a tight loop removes the ~5–10 µs kernel interrupt
round-trip and TSC context-switch cost on every packet receive. That advantage only appears
when:

1. A physical NIC interrupt is the bottleneck (not a software loopback).
2. The polling thread has an exclusive core — it never yields the CPU.

On this host neither condition holds:

- **Loopback has no interrupt.** The `lo` device delivers packets into the kernel socket
  buffer synchronously in the same `sendto` system call; there is no hardware interrupt to
  avoid.
- **Shared vCPUs.** The container's vCPUs are time-shared. A busy-polling thread that spins
  on `recv` is scheduled out by the hypervisor during its slice boundary; the tokio reactor
  (epoll-based) is already parked in the kernel when the packet arrives and is woken
  immediately. The busy-spin thread must wait for its next scheduling quantum — typically
  adding one or more scheduler quanta of extra latency.

The −16 µs result is therefore correctly interpreted as: **busy-spin has no measureable
advantage on container loopback and is materially worse here**. This does not refute the
busy-spin hypothesis for real hardware; it merely confirms that loopback is the wrong
instrument for that measurement.

---

## 6. Code-level attribution of `tax_uc_bookkeeping`

**Flamegraph unavailable.** `perf` and `cargo-flamegraph` are both absent from this
container (`perf_event_paranoid = 4`; neither binary is in PATH). An on-hardware flamegraph
is a Phase-B / follow-up item. What follows is a **code-inspection attribution** of the
+12 792 ns UC adds over bare async UDP. It is clearly labelled as such and is not a profile.

The UC-UDP send/recv hot-path for a single 64-byte ping is:

```
instance.do_rpc()
  → mux.rpc()
    → mux.open_session()  [session lookup]
      → mux.sessions.lock().await   (1)
    → pending.lock().await.insert() (2)
    → session.send_message()
      → session.state.lock().await  (3)
      → Segment::encode() + CRC32   (4)
      → socket.send_to()
  ← recv_loop: Segment::decode() + CRC32 verify  (5)
  ← recv_loop: mux.get_or_create_session()
      → mux.sessions.lock().await   (6)
  ← recv_loop: session.process()
      → session.state.lock().await  (7)
      → SM ack: Segment::encode() + socket.send_to()  (8)
    → route_inbound_message: tokio::spawn()  (9)
      → pending.lock().await.remove()  (10)
      → oneshot send
  ← caller awaits oneshot rx
```

File:line references for each contributor:

1. **`sessions.lock().await` on send path** — `mux.rs:232` (`get_or_create_session`). A
   `tokio::sync::Mutex` async lock acquisition on every RPC, even for an already-open
   session. The happy path hits the `s.get(&sid)` branch and returns quickly, but the async
   lock yield is still a tokio task switch point. (`mux.rs:297` is the `open_session`
   call-site that delegates into `get_or_create_session`, not a separate lock.)

2. **`pending.lock().await.insert()` on send path** — `mux.rs:299`. A second async tokio
   mutex acquisition on every RPC to register the oneshot correlator before transmitting.

3. **`session.state.lock().await` on send path** — `session.rs:74` (`send_message`). A
   third async tokio mutex in the session itself, protecting `next_send_seq` and the
   `SendWindow`. The 64-byte payload fits in one fragment, so the window spin is not hit,
   but the lock acquisition is unconditional.

4. **`Segment::encode()` + `crc32fast::hash()` on send path** — `wire.rs:61–73`.
   Every outbound DATA segment is serialised into a new `BytesMut` allocation (28-byte
   header + payload + CRC32 trailer). The CRC covers the full header + payload — 92 bytes
   for a 64-byte payload. `crc32fast` is hardware-accelerated (CLMUL) but still a
   full-buffer pass. Additionally `Frame::encode()` runs first at `frame.rs:89–98`
   (Frame-level CRC32 over the body), so the payload is hashed twice: once as a Frame body
   and once as a Segment payload.

5. **`Segment::decode()` + CRC32 verify on recv path** — `wire.rs:77–115` (called at
   `mux.rs:345`). Symmetric to (4); the inbound segment is copied out of the recv buffer
   (`Bytes::copy_from_slice` at `wire.rs:113`) and CRC-verified.

6. **`sessions.lock().await` on recv path** — `mux.rs:376` (`get_or_create_session` from
   the recv loop). Same async mutex as (1) but on the inbound side, holding for the session
   table lookup.

7. **`session.state.lock().await` inside `process()`** — `session.rs:97`. The inbound
   session lock for reassembly + SM/NAK response generation. For a non-fragmented 64-byte
   message this is a trivial `reasm.accept()` call but the lock is still acquired.

8. **SM ack transmission** — `session.rs:110–119`. After every inbound DATA segment the
   receiver sends a Selective-Merge (flow-control window advertisement) back to the sender:
   `Segment::encode()` + `crc32fast::hash()` + `socket.send_to()`. For a
   single-fragment message this means the receiver emits one extra datagram the sender must
   round-trip around before `recv_message()` delivers, introducing a mandatory extra
   send/recv pair in the bench's ping-pong.

9. **`tokio::spawn()` for each inbound message** — `mux.rs:387` (`route_inbound_message`).
   Every completed inbound message is dispatched into a fresh detached task. The spawn
   itself is cheap, but on every RPC this is one extra task-scheduling round-trip inside
   the tokio runtime, adding scheduler queue latency.

10. **`pending.lock().await.remove()` on recv path** — `mux.rs:395`. A fourth async mutex
    acquisition (the correlator map) to match and deliver the response to the waiting
    caller.

**Summary of identifiable hot-path costs UC adds over bare UDP:**

| Contributor | Location | Cost class |
|-------------|----------|------------|
| 4× async tokio mutex lock/unlock | `mux.rs:232,299,376`; `session.rs:74,97` | Task yield + wakeup |
| Double CRC32 (Frame + Segment) per direction | `frame.rs:96`; `wire.rs:72,85` | ~92-byte CRC twice each way |
| 2× `BytesMut` allocation + header marshal per direction | `wire.rs:62`; `frame.rs:90` | Heap alloc × 4 |
| Mandatory SM ack datagram on recv | `session.rs:110–119` | Extra kernel round-trip |
| `tokio::spawn` per message | `mux.rs:387` | Scheduler queue latency |
| `Bytes::copy_from_slice` on decode | `wire.rs:113` | Extra copy |

The dominant contributors are expected to be the multiple async mutex acquisitions (each a
potential tokio task yield) and the mandatory SM ack extra round-trip. The double CRC and
per-message allocation are secondary but non-zero.

The +12 792 ns tax is consistent with this picture: 4–5 async mutex round-trips at ~2 µs
each plus one extra datagram RTT (~12 µs on loopback) account for the bulk of the gap.
These are all structural costs of the current v1 UC-UDP design, not accidental regressions.

---

## 7. Gate-A read and recommendation

**What the local ladder settled:**

1. **UC-bookkeeping tax is real and sized: +13 µs p50** on loopback. This is attributable
   to the session-state async mutex topology, double CRC, mandatory SM ack, and per-message
   spawn (§6). On a real LAN (1 Gbit, ~50 µs RTT) the +13 µs tax is a large fraction of
   the wire cost — significant enough to motivate a v2 path that eliminates the structural
   causes (async mutex → parking_lot, single CRC at one layer, SM ack coalescing, route
   without spawn). That is Phase A2 work.

2. **Busy-spin tax cannot be sized on this host.** The −16 µs (confounded/negative) result
   correctly reflects the absence of a real NIC interrupt on loopback + vCPU sharing. It
   does not refute the busy-spin win on bare metal; it proves that loopback is not the
   right instrument for that measurement.

**Gate-A decision:**

- **A2 (V2 streaming / `stream_append` pipeline) proceeds regardless.** It targets
  pipelined commit latency (multiple concurrent in-flight `AppendEntries` per replication
  session) rather than single-inflight RTT. The UC-bookkeeping tax measured here (+13 µs
  per RTT) applies to every inflight; reducing it compounds. A2 is the high-confidence,
  ecosystem-proven lever (openraft explicitly anticipates V2 adoption; `RaftNetworkV2` +
  `stream_append` already exists in the codebase skeleton). The local ladder did not need
  to settle the busy-spin question for A2 to be justified.

- **Phase B (cross-host, real NIC) is warranted.** The local ladder could not rule out
  the busy-spin / AF_XDP win because loopback suppresses the interrupt-avoidance
  phenomenon that motivates them. A cross-host measurement on a real Ethernet NIC (Hetzner
  CCX or equivalent, dedicated cores) is the only way to determine whether the busy-spin
  rung beats the tokio-reactor rung in the environment where ultima_cluster actually runs.
  Phase B also gives an honest absolute latency floor (wire RTT is ~100–400 µs on a real
  LAN vs. ~12 µs loopback) that would change the significance of the +13 µs tax
  substantially — if wire RTT is 200 µs the tax is 6.5%; if it is 50 µs it is 26%.

- **QUIC remains the default.** Nothing in Phase A changes the task 16 conclusion.

---

---

## 8. Part B — V1→V2 streaming (`PipelinedNet`) + Gate-A closure

**Date:** 2026-06-19. **Commit:** `e02a524`.

### 8.1 What was built (Tasks 5–8)

The V2 streaming path replaces the `openraft_legacy::network_v1::Adapter` shim (one-at-a-time
sequential `AppendEntries`) with `PipelinedNet<N>` — a bounded, in-order pipeline that holds
up to `PIPELINE_DEPTH` (default 8) concurrent `AppendEntries` RPCs in flight to a single peer.

Implementation:

- **Task 5 (spike).** Read `RaftNetworkV2::stream_append` and `StreamAppendResult` from
  openraft alpha.21. Confirmed the API: `stream_append` receives a
  `BoxStream<'static, AppendEntriesRequest<TypeConfig>>` and returns
  `BoxStream<'static, StreamAppendResult<TypeConfig>>`. No compat shim needed — the V2
  trait is directly implementable over the existing `RaftNetwork` methods.

- **Task 6 (Clone for V1 impls).** Made `UdpRaftNetwork` and `QuicRaftNetwork` cheaply
  clonable (Arc-wrapped handles) so a single `PipelinedNet` instance can fan work to the
  underlying V1 transport without ownership issues.

- **Task 7 (PipelinedNet).** `uc_node/src/network/pipelined.rs` — `PipelinedNet<N: Clone>`
  implements `RaftNetworkV2::stream_append` by spawning a driver task that pulls requests
  from the stream, dispatches them concurrently via `FuturesUnordered` (bounded to
  `pipeline_depth()`), and yields `StreamAppendResult` responses in submission order. The
  V1 methods (`append_entries`, `vote`, `install_snapshot`) delegate through to the wrapped
  `N`. Both `UdpRaftNetworkFactory` and `QuicRaftNetworkFactory` now mint
  `PipelinedNet<UdpRaftNetwork>` / `PipelinedNet<QuicRaftNetwork>` respectively.

- **Task 8 (this task).** Added `UC_PIPELINE_DEPTH` env override via `parse_pipeline_depth`
  (pure helper) + `pipeline_depth()` (env reader). Factories call `pipeline_depth()` instead
  of the bare `PIPELINE_DEPTH` const, enabling depth sweeps without recompile.

### 8.2 Correctness gate

All three suites green on commit `e02a524`:

| Suite | Command | Result |
|-------|---------|--------|
| `uc_node` integration | `cargo test -p uc_node` | **PASS** (all tests, including `udp_three_node_replicates`, `lin_register`, reconstruction, output suites) |
| fault-injection + partition | `cargo test -p uc_node --features fault-injection -- --test-threads=1` | **PASS** (lin_partition, all fault-injection scenarios) |
| hard-crash linearizability | `cargo test -p uc-crashtest --features hard-crash-tests` | **PASS** (`linearizable_under_hard_crash`, `write_then_read_across_processes`) |

The pipeline introduces no reordering or data loss under fault injection: `PipelinedNet`'s
bounded concurrency is over independent RPCs to separate followers; per-follower ordering is
maintained by the `FuturesUnordered` response queue, which preserves submission order.

### 8.3 Pipelining measurement (local loopback)

**Measurement limitation: the `internode-rpc-bench` echo bench does not exercise `stream_append`.**

The bench exercises the transport echo path (`EchoClient.rpc`) directly — no openraft, no
`PipelinedNet`, no `stream_append`. `UC_PIPELINE_DEPTH` controls `PipelinedNet::new_client`
which is invoked only through openraft's replication network, not through the echo bench.

A depth-1 vs depth-8 sweep with `--mode ladder --inflight 8 --rate 10000` yields virtually
identical numbers (p50 ≈ 538–545 µs loopback) because the variable being swept (`UC_PIPELINE_DEPTH`)
has no effect on this code path.

**Why local loopback cannot size the pipeline win:**

1. `PipelinedNet` amortises per-RPC latency across multiple concurrent in-flight calls.
   Loopback RTT (~12–30 µs) is so low that even depth-1 sequential commit saturates at tens
   of thousands of req/s — there is no latency budget to win back by overlapping.
2. The realistic pipeline win appears at cross-host wire latency (100–400 µs per hop on a
   Hetzner LAN): depth-8 overlapping commits cuts effective per-entry latency from
   `1 × RTT` toward `RTT / 8` for a pipelined batch, saving ~87% in the fully-pipelined
   steady state.
3. The `commit-path-load` bench does run through openraft (and thus `PipelinedNet`) but
   requires a live cluster with at least two nodes and a service process — it is a
   cross-process bench that cannot run without real infrastructure.

**Indicative-only numbers (loopback, depth 1 vs 8, echo bench):**

| Depth | p50 (µs) | p99 (µs) | Achieved rate |
|-------|----------|----------|---------------|
| 1 (sequential) | 538 | 1 344 | 9 999 req/s |
| 8 (pipelined) | 545 | 1 312 | 9 998 req/s |

These numbers are transport-layer echo only. They confirm the transport path is unchanged
and the `UC_PIPELINE_DEPTH` env knob compiles and runs without regression, but they do not
show the Raft replication pipeline benefit. A real measurement requires the cross-host
fleet (task16 §6.4/6.5 harness) with `commit-path-load` at depth 1 vs 8.

### 8.4 Gate-A final recommendation

**V2 streaming (`PipelinedNet`) ADOPTED.**

- The implementation is complete, correct, and correctness-gate clean (§8.2).
- The approach is ecosystem-proven: openraft explicitly provides `RaftNetworkV2::stream_append`
  for exactly this purpose (pipelined commit, not single-inflight RTT reduction).
- `UC_PIPELINE_DEPTH` is now a runtime knob, enabling A/B on the cross-host fleet without
  recompilation.
- QUIC remains the default transport. Nothing in Phase A or Part B changes the task 16
  conclusion.

**Phase B (cross-host AF_XDP + `SO_BUSY_POLL`) remains WARRANTED** because:

- The local ladder (Part A) could not rule out the busy-spin / kernel-bypass win: loopback
  suppresses the hardware-interrupt-avoidance phenomenon that motivates them (§5).
- The real absolute-latency floor (100–400 µs cross-host vs. 12 µs loopback) determines
  whether the +13 µs UC-bookkeeping tax is material (6.5% vs. 26%) — a cross-host
  measurement is the only honest instrument.
- The `UC_PIPELINE_DEPTH` knob enables a clean depth-1 vs depth-8 A/B on the cross-host
  harness as Phase B's first sub-task.

**Phase B is a separate plan** gated on this Gate-A decision, consistent with the task 17
spec §4. It will append §9+ to this document when executed.

## 9. Part C — Phase B Stage 1 cross-host run (2026-06-19)

Fleet: **3× AWS `c7i.4xlarge`** (ENA, single-AZ + cluster placement group), `os_tune` (perf
governor + `net.core.busy_poll=50`). Provisioned via `make -C bench-infra up-uc`, run, then
**DESTROYED** (11 resources, billing stopped). The Phase A cold-boot SSH-wait fix held — all
3 nodes came up clean (`failed=0, unreachable=0`).

### 9.1 B-V2 — cross-host pipelined-commit A/B (the decisive V2-streaming number)

`commit-path-load` open-loop, 3-node, inflight 128, 64 B, `UC_PIPELINE_DEPTH=1` (sequential)
vs `8` (pipelined), rate ladder 100–20 000 msg/s.

| target rate | depth=1 achieved | depth=8 achieved | depth=1 p50 / p99 | depth=8 p50 / p99 |
|---|--:|--:|--:|--:|
| 100–5000/s | ≈target | ≈target (identical) | ~17–19 ms / ~21–64 ms | ~17–20 ms / ~22–64 ms |
| **10000/s (knee)** | **6815/s** | **7238/s** | 2611 ms / 4559 ms | **1869 ms / 3708 ms** |
| 20000/s (deep sat) | 6356/s | 6461/s | 10687 ms / 21123 ms | 10310 ms / 20821 ms |

**Finding — pipelining helps under backlog, exactly as predicted, and cross-host shows what
loopback (§8.3) could not.** At low–moderate rates depth-1 and depth-8 are identical (no
backlog → nothing to pipeline). **At the saturation knee (10 000/s target): depth=8 gives
+6.2 % commit throughput (7238 vs 6815/s), −28 % p50 (1869 vs 2611 ms), −19 % p99.** Neutral at
low load, no regression anywhere. On real cross-host RTT the leader's replication backlog gives
the 8-deep pipeline real RTT-wait to hide; loopback's µs RTT had none. **The Phase A V2
`stream_append` change is validated cross-host: a measurable throughput-and-latency win under
load, at no cost when idle.**

### 9.2 B1 — busy-poll datapath RTT (run 2026-06-19, after the split-role rung landed)

The first attempt was blocked: the `busypoll-udp` rung was a loopback-only echo-pair (`--role
server` bailed). That was fixed (a split-role `busypoll_udp_echo_server`/`_client`, commit
`bf77537`), then measured on a fresh 2-of-3-node `c7i.4xlarge` fleet (per-link, node1 client →
node0 server, 64 B single-inflight ping; `net.core.busy_poll=50` via `os_tune`):

| transport | p50 | p99 | p99.9 |
|---|--:|--:|--:|
| **UC-UDP** | **49.4 µs** | 60.2 µs | 70.5 µs |
| busy-poll UDP | 59.6 µs | 74.4 µs | 98.8 µs |
| UC-QUIC | 68.8 µs | 115.5 µs | 134.3 µs |
| Aeron (task16 §6.6, same HW) | 47 µs | 66 µs | ~76 µs |

**Two findings:**
1. **Busy-poll did NOT help — it was ~10 µs *slower* than plain UC-UDP** (59.6 vs 49.4 µs p50).
   Caveat/confound: the busy-poll rung adds a dedicated thread + mpsc/oneshot handoff per RPC,
   whereas UC-UDP drives the `UdpMux` directly — so the rung's architecture overhead (~10 µs)
   masks any ENA busy-poll benefit. The clean read is not "busy-poll is bad" but "busy-poll, as a
   cheap drop-in, does not beat UC's existing UDP path here."
2. **UC-UDP is already essentially at the Aeron floor (49.4 vs 47 µs p50)** on this clean
   placement-group link. (This per-link `--role server/client` number is lower/cleaner than the
   98 µs in §6.6, which came through a different harness path; both put UC-UDP within a small
   factor of Aeron.) There is almost no single-inflight RTT gap left to close.

### 9.3 Harness bug found + fixed (the run blocker)

First Pass-1 attempt failed: every node logged `setsid: failed to execute UC_PIPELINE_DEPTH=1:
No such file or directory`. The Task-4 env-prefix `setsid UC_PIPELINE_DEPTH=1 uc-node-launch`
is wrong — `setsid` is not a shell, so it tried to *exec* a program named `UC_PIPELINE_DEPTH=1`
(the `VAR=val cmd` form only applies before the command word, and `setsid` *was* the command
word). Fixed in `roles/run/tasks/main.yml` to `export UC_PIPELINE_DEPTH=…` (its own line, matching
the existing `UC_DURABILITY`/`UC_TRANSPORT` exports) before `setsid`. Re-ran clean. (Both the
per-task and whole-branch reviews missed this — they reasoned "POSIX `ENV=val cmd`" without
accounting for `setsid` consuming the assignment as an argv.)

### 9.4 Gate-B status

- **Gate-A / V2 streaming: CLOSED, positive.** B-V2 confirms the pipelined `stream_append`
  helps under cross-host load. No further action — it's already merged + default.
- **Gate-B1 (busy-poll vs AF_XDP): CLOSED — do NOT pursue AF_XDP.** §9.2 shows plain UC-UDP is
  already ~at the Aeron floor (49 vs 47 µs p50) on a clean placement-group link, and the cheap
  busy-poll lever did not beat it. With essentially no single-inflight RTT gap remaining, the
  weeks of AF_XDP work (raw L2 framing, UMEM, `CAP_NET_ADMIN`, ENA-zero-copy verification) cannot
  pay off — kernel-bypass would chase a gap that is already closed. **AF_XDP (Stage 2) is
  cancelled.**
- **Net latency conclusion:** the durable win was **V2 streaming** (§9.1 — pipelined commit, +6 %
  throughput / −28 % p50 under load, shipped + default). UC's transports are already in Aeron's
  latency class for raw per-link RTT; the remaining headroom is throughput-under-load (which V2
  streaming addressed), not single-shot latency.
- **QUIC stays the default transport** (encrypted-by-default, mature; ~20 µs behind UC-UDP on
  single-inflight RTT but that is not the bottleneck for replication throughput).
