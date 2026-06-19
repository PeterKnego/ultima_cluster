# Design — Inter-node latency: phased path toward Aeron-class

**Date:** 2026-06-19.
**Status:** Spec. A **two-phase, measurement-gated** effort to move UC inter-node latency toward Aeron's
class. Phase A is a high-confidence, mostly-local win (V2 streaming + understanding the userspace gap);
Phase B is exploratory cross-host work with kernel-bypass / busy-poll datapaths.
**Context:** Follows task16 (inter-node UDP transport + QUIC-vs-UDP A/B). The AWS run (task16 §6.6)
produced the first Aeron floor and an un-explained gap: **Aeron 47µs p50 vs UC ~91–98µs p50**, ~8× p99.

---

## 1. Problem & goal

task16 §6.6 (AWS placement group, 64 B, cross-host) measured Aeron **47µs p50 / 66µs p99** (open-loop
10 K/s, busy-spin threads pinned to cores 1–4) against UC-QUIC **91µs / 530µs** and UC-UDP
**98µs / 557µs** (single-inflight, no pinning). The comparison is tuned-vs-untuned, so the raw delta
overstates any protocol-level difference — but the gap is real and unexplained.

**Goal:** determine, **with measured numbers at each gate**, whether UC can reach Aeron's latency class,
and by which lever. Two phases, cheapest-and-most-proven first. **Each phase ends at a decision gate —
no phase proceeds on reasoning alone; it proceeds on numbers.** QUIC stays the default transport until
evidence says otherwise (task16 verdict unchanged).

## 2. Landscape — candidate levers and precedent (context, not verdict)

- **UC's UDP transport is async `tokio`** (`tokio::net::UdpSocket`, async `Mutex` on the hot path in
  `uc_node/src/network/udp/session.rs`, `tokio::time::sleep` ticker). It already ports Aeron's **wire
  mechanics** (NAK retransmit, flow-control window, fragmentation) — so the gap is the **runtime model +
  per-message bookkeeping**, not the packet format.
- **Aeron** = busy-spin idle strategy on pinned cores, lock-free SPSC, `tryClaim` zero-copy, pre-touched
  mmap. Source of its 47µs / tight-p99 profile.
- **openraft examples** = all async-tokio RPC; the one latency lever present is `RaftNetworkV2`
  **bidirectional streaming append** (`NetStreamAppend`).
- **Databend-meta** (largest production openraft user) = async gRPC + protobuf, **streaming `AppendV002`**
  (64-item buffer) + cached per-peer connection + `TCP_NODELAY`. **No busy-spin / kernel-bypass / UDP
  anywhere.** The production answer to "make replication fast" is *pipelined streaming + hygiene*.
- **New Rust-native datapaths** (this rescope): **AF_XDP via `xsk-rs`** (kernel-bypass, copy→zero-copy)
  and **kernel UDP + `SO_BUSY_POLL`** (busy-poll the NAPI ring, keep the kernel stack). Both are
  **real-NIC phenomena** — see §4.

**Levers, ranked cheapest/most-proven → most-exotic:**
1. **V2 streaming append + connection hygiene** — Phase A. What openraft's example *and* Databend do.
   Transport-agnostic (helps QUIC and UDP). UC likely lacks it (adapts V1 via `net.into_v2()`).
2. **`SO_BUSY_POLL` kernel UDP** — Phase B. Cheap partial win; keeps UDP semantics; real-NIC only.
3. **AF_XDP / `xsk-rs`** — Phase B. Bigger kernel-bypass win, Rust-native; copy→zero-copy; you own the
   eth/IP/UDP framing; real-NIC only.

(Adopting Aeron's C media driver via `aeron-rs` is explicitly *not* pursued — AF_XDP is the lighter,
Rust-native kernel-bypass path, and §2 shows no production openraft user runs a busy-spin C driver.)

---

## 3. Phase A — V2 streaming + understand the userspace gap (local, high-confidence)

### A1. Local latency ladder (free) — size the *userspace* buckets

The async-vs-busy-spin (userspace) and UC-bookkeeping costs reproduce on **loopback** (task16 §6.3 saw
UC-UDP lose to QUIC on loopback purely from in-band ticker overhead). Measure locally, free. All rungs:
same box, loopback, 64 B, single-inflight RTT, equal sample counts, p50/p99/p99.9.

| Rung | What | Isolates |
|---|---|---|
| 1 | minimal `tokio::net::UdpSocket` echo, no protocol | async-runtime + epoll + syscall floor |
| 2 | minimal blocking `std::net::UdpSocket` busy-recv loop, pinned core, no protocol | userspace busy-spin floor |
| 3 | UC-UDP loopback — `internode-rpc-bench --transport udp --role both` | rung 1 + UC bookkeeping (async-mutex, ticker, NAK/flow-ctrl/CRC) |
| 4 | UC-QUIC loopback — same harness | reference |

Derived: `(1)−(2)` = userspace async-vs-busy-spin tax; `(3)−(1)` = UC bookkeeping tax. Plus a
**flamegraph of rung 3** to confirm where the bookkeeping µs go (predicted: `state.lock().await`
async-mutex round-trips + the in-band ticker). Rungs 1–2 are ≈50–100-line throwaway harnesses under
`uc_autobench/`.

### A2. V2 streaming append — the most important near-term lever

Single-inflight ping measures **per-RPC RTT** — exactly what pipelining hides. Real Raft commit is
pipelined, so per-RPC RTT may not predict commit latency.

1. **Verify** UC's append path: confirm it builds the network via `net.into_v2()` (task16) ⇒ V1 per-RPC
   append, *not* streaming. Read `uc_node/src/network/` + the openraft adapter wiring.
2. **Adopt `RaftNetworkV2` streaming append** (`NetStreamAppend`-style): leader streams many
   AppendEntries without waiting per-response. This is transport-agnostic — it sits at the openraft
   `RaftNetwork` layer above both the QUIC and UDP `ClusterTransport`s, so both benefit. Include the
   cheap hygiene Databend uses: persistent per-peer connection reuse (UC already has one
   connection/peer-pair) and `TCP_NODELAY`/equivalent where applicable.
3. **Measure** pipelined commit latency before/after (multi-entry / fan-out path, not single-inflight) —
   the metric that actually gates Raft throughput.

V2 streaming is highest-priority because it is cheap, ecosystem-proven (openraft + Databend), and
targets the *right metric*. It may close enough of the practical gap to make Phase B unnecessary.

### Gate A (decision)

With A1 + A2 numbers in hand: does V2 streaming + the ladder findings close enough of the
*commit-latency* gap that the exotic datapaths aren't worth it? **Proceed to Phase B only if the
userspace ladder shows a large async-vs-busy-spin tax AND sub-50µs is a product requirement that V2
streaming did not satisfy.**

---

## 4. Phase B — cross-host Rust datapaths (real-NIC, exploratory)

**Why cross-host only:** both Phase B approaches are **real-NIC phenomena** whose benefit does *not*
appear on loopback. `SO_BUSY_POLL` has no NAPI ring to poll on `lo`; AF_XDP has no DMA/interrupt to
bypass on `lo` (and the local container likely lacks an XDP-capable NIC). So Phase B runs **cross-host
on AWS** (ENA NICs — which support AF_XDP including zero-copy), measured against UC-UDP / UC-QUIC /
Aeron on identical hardware. This absorbs the "full empirical" cloud phase deferred earlier.

### B1. `SO_BUSY_POLL` kernel UDP (cheap partial win)

Normal kernel UDP socket with `SO_BUSY_POLL` (+ `SO_PREFER_BUSY_POLL`, `net.core.busy_poll`/`busy_read`
sysctls): busy-poll the NAPI ring on recv, removing interrupt → softirq → wakeup latency while keeping
full UDP semantics. With tokio this likely needs a **dedicated blocking-recv thread** (tokio's epoll
reactor doesn't busy-poll natively). Burns a core. Measure the cross-host delta vs plain UC-UDP.

### B2. AF_XDP via `xsk-rs` (kernel-bypass)

NIC driver delivers raw L2 frames into a memory-mapped UMEM ring in userspace, bypassing the kernel
UDP/IP stack. For a fixed 2-node bench: **static MAC/IP, no ARP**; implement minimal eth/IP/UDP framing
+ checksums. Validation ordering (as researched):
1. **Copy / generic mode first** — works on ~any NIC; validate correctness (frames round-trip, framing
   correct) before chasing performance.
2. **`XDP_ZEROCOPY`** — only after confirming the NIC driver supports it (verify on AWS ENA). NIC DMAs
   directly into UMEM, no copy — the real latency win.
   Measure cross-host at each stage vs UC/Aeron.

If either B1 or B2 wins decisively, it becomes a **new `ClusterTransport` implementation** (the task16
seam already supports plugging one in). Phase B harnesses are bench-only until that decision.

### Gate B (decision)

Which datapath (if any) to adopt as a production `ClusterTransport`, with measured cross-host p50/p99
justifying the added complexity (AF_XDP privileges/framing; busy_poll core burn).

---

## 5. Shared measurement methodology

- Harness: `internode-rpc-bench` (single-inflight *and* pipelined/fan-out) + `aeron-echo-baseline.sh`
  for the Aeron reference. p50/p99/p99.9, equal sample counts, fixed 64 B (plus 1024 B where relevant).
- **Fix the task16 harness rough edges first** (needed for Phase B cloud runs): `build_aeron` role
  should `chown` `/opt/bench/aeron-deploy` to the run user; `download_results` runs on node0 and can't
  scp to the control box (read results from the streamed log or fetch the tarball manually).
- Phase B core-isolation: `os_tune` on an `up-fanout`-class fleet so busy-poll / AF_XDP / Aeron compare
  on equal pinning.

## 6. Deliverables

- **Phase A:** ladder numbers + flamegraph; the V1-vs-V2 finding; **V2 streaming append implemented**
  with its measured pipelined-commit improvement. Written up in `docs/tasks/task17_inter_node_latency.md`.
- **Phase B (if Gate A warrants):** `SO_BUSY_POLL` + AF_XDP (copy, then zero-copy) prototype harnesses;
  cross-host numbers vs UC/Aeron; an adopt / no-adopt recommendation per datapath, and a
  `ClusterTransport` impl if one wins.

## 7. Success criteria

- A1 ladder run locally (4 rungs, p50/p99/p99.9, two derived taxes) + rung-3 flamegraph.
- UC's append path definitively identified V1 vs V2; **V2 streaming adopted** and its commit-latency
  effect measured.
- Gate A decision recorded (proceed to Phase B or stop, justified by numbers).
- If Phase B runs: cross-host numbers for `SO_BUSY_POLL` and AF_XDP (copy + zero-copy) with an
  adopt/no-adopt call per datapath.

## 8. Out of scope / non-goals

- Changing the default transport before evidence (QUIC stays default).
- Adopting Aeron's C media driver (`aeron-rs`) — AF_XDP is the chosen Rust-native kernel-bypass path.
- Adopting any Phase B datapath without its cross-host gate-B numbers.
- AF_XDP framing beyond a fixed 2-node static config unless/until B2 is adopted as a transport.

## 9. Risks

- **AF_XDP complexity:** raw L2 framing, UMEM lifecycle, `CAP_NET_ADMIN`/root, an XDP program to load;
  zero-copy is NIC/driver-dependent (verify ENA support before relying on it).
- **busy_poll:** burns a core; tokio integration likely needs a dedicated recv thread; benefit is
  NIC/driver-dependent.
- **V2 streaming:** openraft alpha API churn (pin per task16); the streaming sub-trait wiring must not
  regress the linearizability suites — re-run them on the transport flip.
- **Measurement validity:** Phase B must compare on equal core-isolation, else it repeats task16 §6.6's
  tuned-vs-untuned apples-to-oranges.
