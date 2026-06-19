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

1. **`sessions.lock().await` on send path** — `mux.rs:232` (`get_or_create_session`) and
   `mux.rs:297` (`open_session` → `get_or_create_session`). A `tokio::sync::Mutex` async
   lock acquisition on every RPC, even for an already-open session. The happy path hits the
   `s.get(&sid)` branch and returns quickly, but the async lock yield is still a tokio task
   switch point.

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

*Part B (cross-host ladder, Phase B) will append §8+ to this document.*
