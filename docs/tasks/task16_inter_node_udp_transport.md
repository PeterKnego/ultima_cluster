# Task 16 — Inter-node UDP transport (Aeron-style reliable unicast) + QUIC-vs-UDP A/B

**Date:** 2026-06-17.
**Status:** Closed. Transport built, proven linearizable (failover / partition / 10% loss /
hard-crash) over UDP, transport-isolated microbench landed, QUIC-vs-UDP A/B captured. QUIC
remains the default.
**Branch:** `feat/inter-node-udp-transport`.

**Provenance / scaffolding.** Design rationale and the full phasing live in the retained
superpowers artifacts —
`docs/superpowers/specs/2026-06-17-inter-node-udp-transport-design.md` and
`docs/superpowers/plans/2026-06-17-inter-node-udp-transport.md` — kept as historical
design history per CLAUDE.md. This doc folds in the essential rationale so it stands on its
own; the spec/plan are not required reading to understand what shipped.

---

## 1. What this is & why

`ultima_cluster` carries cross-host consensus traffic over **QUIC** (`quinn`): one
persistent connection per peer-pair, a fresh bidirectional stream per RPC, TLS by default.
QUIC buys reliability + ordered streams + congestion control + encryption. For a **trusted
LAN with a small fixed peer set** that bundle also carries cost the replication path may not
need: the TLS handshake, conservative congestion control, per-stream state, and userspace
QUIC-stack overhead.

This task builds a **second inter-node transport — an Aeron-style reliable-unicast UDP
channel, implemented from scratch in-tree — behind a pluggable transport seam**, so we can
A/B it against QUIC on the same harness and **choose the best transport with measurements
rather than assertions**. QUIC stays the default and its code path is byte-for-byte
unchanged.

Scope is **cross-host node↔node only**. Same-host service↔node and client→node traffic
stays on the shmem rings (task08 / task11). task13 benchmarked Aeron *IPC* (same-host) as
the transport floor; this is the cross-host sibling — Aeron's *UDP* path as the reference.

**Reliability model — we lean on openraft (see §4).** openraft already retries failed RPCs
(re-sends `AppendEntries` from the matched index, re-issues votes, re-streams snapshots). So
the channel does **not** need to be a never-breaking stream. It must recover *ordinary*
packet loss efficiently (so a single dropped datagram doesn't force a full-RPC retransmit),
but a *truly* failed session may surface an error and let openraft's failure detector + retry
take over. We optimize the common case; we do not reimplement Aeron's full bulletproofing.

---

## 2. The transport seam

The only contract openraft imposes is `RaftNetworkFactory<TypeConfig>` (outbound) plus an
inbound listener. Both halves were previously hardcoded in the builder. We introduced a
**trait-based seam** so adding a transport never means editing consensus/builder wiring —
future kernel-bypass stacks (DPDK, io_uring, RDMA verbs), OpenUCX, etc. plug in by
implementing one trait.

`uc_node/src/network/transport.rs`:

```rust
#[allow(async_fn_in_trait)]
pub trait ClusterTransport {
    type Factory: RaftNetworkFactory<TypeConfig>;
    type Server;
    async fn build_factory(&self, ctx: &TransportCtx) -> Result<Self::Factory, NetworkError>;
    async fn spawn_server<SM>(&self, ctx: &TransportCtx, raft: Raft<TypeConfig, SM>)
        -> Result<Self::Server, NetworkError>
    where SM: RaftStateMachine<TypeConfig>;
}
```

`TransportCtx` carries the shared inputs every transport needs (`node_id`, `listen_addr`,
`app_id`, `data_dir` for certs/keys, and an optional `fault_table` under the
`fault-injection` feature). The methods are `async` (build/spawn both await socket binds);
the `async_fn_in_trait` lint is allowed because the trait is internal — only
`QuicTransport`/`UdpTransport` impl it and only the builder calls it, on the runtime thread,
so the auto-trait `Send`-flexibility the lint guards is not needed.

The config selector, parallel to the existing `TlsConfig`:

```rust
// config.rs
pub enum Transport {
    Quic,                // default — today's path, byte-for-byte unchanged
    Udp(UdpTuning),      // new: Aeron-style reliable unicast
}
```

`builder.rs` is transport-agnostic: it resolves `config.transport` to a concrete
`ClusterTransport`, calls `build_factory` + `spawn_server`, and never names QUIC or UDP. A
runtime-selected `TransportServer { Quic(..) | Udp(..) }` handle gives a uniform
`shutdown().await` / `local_addr()`.

**Caveat documented, not solved:** kernel-bypass stacks often want to own the runtime/poll
loop and don't speak `tokio::net`. `ClusterTransport` deliberately makes no tokio-socket
assumption — it only promises "give me a factory + a server"; how each transport drives I/O
is its own business.

**Module reshape** (`uc_node/src/network/`):

```
network/
  transport.rs    ClusterTransport, TransportCtx, TransportServer
  frame.rs        shared, unchanged (RPC Frame / MessageType / request_id correlator)
  codec.rs        shared, unchanged (encode/decode the 3 RPC classes)
  fault.rs        extended: block/partition + drop / delay, used by BOTH transports
  quic/           client.rs server.rs instance.rs factory.rs tls.rs transport_impl.rs
  udp/            wire.rs fragment.rs reassembly.rs send_window.rs session.rs
                  mux.rs instance.rs factory.rs server.rs transport_impl.rs
```

Both transports reuse `frame.rs`/`codec.rs` for the RPC request/response payload — the UDP
work is the **reliable channel underneath** the frame, not a new RPC encoding. Keeping the
`udp/` file surface parallel to `quic/` is what makes the A/B apples-to-apples.

---

## 3. The UDP channel design (`udp/`)

**One shared `tokio::net::UdpSocket` per process** (mirroring the single QUIC client
endpoint). A single receive-dispatch loop demultiplexes inbound datagrams to per-peer
**sessions** keyed by `session_id`.

### 3.1 Wire segments (`wire.rs`)

A fixed **28-byte little-endian header** + optional payload + a trailing CRC32 over the whole
thing:

```
version u8 | seg_type u8 | flags u8 | _pad u8 | session_id u32 | seq u64 | arg u64 |
payload_len u32 | payload[payload_len] | crc32 u32
```

Six segment types: `Data`, `Nak`, `Sm` (status message — ack + window), `Heartbeat`, `Hello`,
`HelloAck`. `flags` carries `FLAG_BEGIN` / `FLAG_END` for fragmentation. `arg` is the
type-specific scalar (e.g. NAK count, SM window). CRC32 (`crc32fast`) covers the full header +
payload so a corrupt datagram is dropped at decode.

### 3.2 Flat per-session sequence

Each session has a monotonic **`u64` send seq** — no `term_id` / `term_offset` /
`initial_term_id` algebra, no 3-term rotation. There is no shared log buffer to size or zero,
so the receiver tracks only a contiguous-received cursor + a reassembly buffer.

### 3.3 NAK range-retransmit (`send_window.rs`, `reassembly.rs`)

The sender retains unacked `Data` segments in an in-memory **send window** (`send_window.rs`),
bounded by the flow-control credit; a segment acked past the window is evicted. The receiver
gap-detects on seq discontinuity and emits `Nak(start, count)` covering a whole contiguous
gap; the sender resends that range from the window, with a `nak_linger_ms` (default 10 ms)
linger/dedup so a duplicate NAK doesn't double-send. Unicast NAK delay ≈ 0.

### 3.4 Receiver-window flow control

The receiver advertises `highest_contiguous_seq + window` via `Sm`, on the periodic SM timer
(default 200 ms) and on consumption advance. Default window **128 KiB** (`UdpTuning`-
configurable). The SM doubles as the receiver→sender keepalive. (v1 note: the sender does not
yet *honor* the SM-advertised window for true back-pressure — both ends default to the same
128 KiB, so the credit never binds in practice; honoring it is deferred, §7.)

### 3.5 MTU fragmentation (`fragment.rs`, `reassembly.rs`)

RPC `Frame`s larger than `mtu − header` split into `BEGIN…(middle)…END` `Data` segments and
reassemble per-session into a `BytesMut` before the completed frame is handed up. This covers
batched `AppendEntries` (the `max_payload_entries` knob can make large bodies) and
`InstallSnapshot` chunks. Default MTU **1408** (`UdpTuning`-configurable). The 3-node steady-
state test exercises multi-MTU fragment/reassemble end-to-end.

### 3.6 Per-session ticker, handshake & liveness (`mux.rs`, `session.rs`)

A per-session ticker, spawned once on session insert, fires the periodic maintenance:
re-NAK still-open gaps, re-advertise the SM window, and emit a `Heartbeat` (the idle
sender→receiver keepalive). `MissedTickBehavior::Delay` prevents a NAK burst after the task is
starved. First contact does a trivial `Hello`/`HelloAck` carrying `app_id` /
`protocol_version` (mismatch → refuse, matching the existing IPC-entry posture). A peer silent
past `session_timeout_ms` (default 5 s) is a dead peer → transport error → openraft.

### 3.7 Per-process session epoch — the key correctness fix (Task 13b)

The session id is derived from the address pair, but a node that **restarts on the same
address** would otherwise re-derive the **same** `session_id` while resetting its send seq to
0 — and survivors' stale `Reassembler` (which drops `seq < next`) would silently discard
*everything* from the "new" node. The cluster wedges. This is not a safety violation (the
channel fails closed), but it makes UDP node-restart non-functional, and it is exactly what
the lincheck failover test (leader kill + restart) needs.

The fix (`mux.rs`):

```
sid = session_id_for(local, peer) ^ per_process_epoch
```

`epoch` is a random `u32` minted at `UdpMux::bind`. The **initiator** XORs its epoch in and
stamps the wire id; the **receiver adopts the id from the wire**. A restart mints a fresh
epoch → a fresh `session_id` → survivors create a brand-new session and the stale reassembler
is sidestepped. **No wire-format change.**

### 3.8 Clean socket release on shutdown (Task 13b)

The mux holds the **sole strong `Arc<UdpSocket>`**; every session `SocketTx` and the
recv-loop hold a **`Weak`**. `shutdown()` `take()`s the socket, aborts + joins the recv loop
and all per-session tickers/route tasks, and clears the handler/registry — so the fd is freed
before any rebind on the same address, and a post-shutdown straggler RPC upgrades its `Weak`,
finds `None`, and silently drops its datagram (it cannot re-pin a dead fd). This closes the
`EADDRINUSE`-on-restart leak that the original implementation had (ticker tasks held strong
`Arc`s and kept the fd alive past shutdown).

### 3.9 RaftNetwork + server on top

Thin, because the channel does the hard part and the RPC encoding is shared:

- **`UdpRaftNetwork`** (`udp/instance.rs`) impls openraft's V1 `RaftNetwork` (wrapped by the
  same `Adapter`→V2 the QUIC path uses), mirroring `quic/instance.rs`: encode body via
  `codec`, get-or-open the per-peer session, send the request `Frame`, await the correlated
  response. `Frame.request_id` is the correlator — multiple in-flight RPCs multiplex over one
  session and demux by `request_id` (QUIC got this free via a fresh bi-stream per RPC; here
  the mux does the real demux work).
- **`spawn_udp_server`** (`udp/server.rs`) runs the dispatch loop; a reassembled request
  `Frame` dispatches into `raft.append_entries / vote / install_snapshot` (dispatch body
  identical to `quic/server.rs`) and the response goes back on the same session.
- **`UdpRaftNetworkFactory`** (`udp/factory.rs`) holds the shared mux + a node-wide
  `request_id` counter; `new_client` defers connection like QUIC. On session-fatal error it
  surfaces `RPCError` so openraft retries; the next attempt re-handshakes.

### 3.10 Aeron mapping — what was ported vs deliberately dropped

Source-verified against Aeron `master` (`io.aeron.protocol.*Flyweight`, `Configuration.java`,
`LossDetector`/`RetransmitHandler`) and the wiki Transport Protocol Spec. **Ported
(simplified):** range-based NAK loss recovery; receiver-driven flow control via SMs; MTU
fragmentation (BEGIN/END flags); liveness via cheap periodic beats + a longer death timeout.
Defaults adopted: MTU 1408, window 128 KiB, SM 200 ms, NAK linger 10 ms, death timeout 5 s.

**Deliberately dropped** (these exist for Aeron's multicast-capable, zero-GC,
separate-media-driver design — they are not protocol requirements for trusted-LAN unicast):

- **3-term rotating mmap log buffers + `(initial_term_id, term_id, term_offset)` position
  algebra** → replaced by a flat `u64` seq + an in-memory retain window (no shared buffer to
  size/zero).
- **Media-driver/client process split, shmem log buffers, conductor duty cycle, counters
  file** → the channel lives inside `uc_node`.
- **Multicast** (`OptimalMulticastDelayGenerator` RFC-5401 NAK backoff, group tags, TTL,
  late-joiner SETUP re-request) → unicast point-to-point needs none.
- **Congestion control** (pluggable cubic) → static window for v1 (LAN + small fixed peer
  set).
- **SETUP-frame geometry negotiation** → replaced by the trivial `Hello`/`HelloAck`
  validating `app_id` + `protocol_version` (we have no mmap buffers to size).
- **32-byte FRAME_ALIGNMENT + PAD frames, RTT-measurement frames, reserved value,
  EOS/REVOKED, error/resolution frames** → alignment is for atomic in-place shared-buffer
  reads we don't do; RTT frames tune timers Raft already owns.

---

## 4. Leaning on openraft (the reliability boundary)

The channel is **not bulletproof Aeron** — that is by design. The division of labor:

- The channel recovers **ordinary loss** via NAK range-retransmit from the send window,
  cheaply, without forcing a full-RPC resend. This is the common case and is where the
  efficiency win lives.
- A **truly failed session** (peer silent past the death timeout, send-window error, decode
  failure) surfaces as an `RPCError`. openraft's failure detector + retry then take over: it
  re-sends `AppendEntries` from the matched index, re-issues the vote, or re-streams the
  snapshot. The next attempt re-handshakes a fresh session.

So we optimize the steady state and let consensus own the rare hard failure, rather than
reimplementing Aeron's full never-breaking-stream machinery. The correctness suites (§5) are
what prove the boundary is drawn in a safe place: under drop / delay / partition / kill-9 the
cluster stays linearizable, the channel either recovers or fails closed, and openraft's retry
makes the system whole.

---

## 5. Correctness

An in-tree reliability layer must be *proven*, reusing the existing verification machinery
rather than inventing new tests. Everything below ran green; the QUIC paths are unchanged
throughout.

**Fault layer.** `fault.rs` was extended additively from block/partition to also do
**`drop(p)` and `delay(d)`**, all behind the non-default `fault-injection` feature, applied in
the recv path of *both* transports (`should_drop(roll < loss)`; `heal()` clears block + loss +
delay). The fault table is threaded from the factory via `set_fault_injection` and an
`addr → node` map, so the same WGL harness drives both transports.

| Suite | Transport | Result |
|---|---|---|
| 3-node steady-state replication (multi-MTU fragmentation) | UDP | leader election + `AppendEntries` fragment/reassemble + convergence — **PASS** |
| Channel unit tests (`udp/`) | UDP | 21/21 — gap→NAK→retransmit, fragment/reassemble across MTU, send-window dup/evict, real loopback RPC round-trip, ticker re-NAK |
| Lincheck capstone, leader **kill + restart** (`lin_register.rs`) | UDP | **Linearizable** under failover — incl. the node-restart path the §3.7 epoch fix unblocked; independent re-run green |
| Partition suite (`lin_partition.rs`): minority / leader-isolated / quorum-loss | UDP | 3/3 — no split-brain, no stale read on a partitioned-away node, clean failure under lost quorum |
| **Linearizable under 10% packet loss** | UDP | **629 ops all-Ok, WGL-Linearizable** — NAK retransmit proven under real segment loss |
| Multi-process hard-crash (`kill -9` mid-load) | UDP | **PASS** — service-reconstruction linearizable over UDP |

The QUIC capstone / partition / hard-crash all stay green (the env-switch is additive; QUIC
remains default). `assert_linearizable` always runs *before* reporting Ok, so a `Violation`
panic propagates past the timing-retry and can never be masked by retries; progress + minimum-
op (30-op) guards prevent a vacuous pass.

**Reproduce** (feature-gated, single-threaded):

```bash
UC_TRANSPORT=udp cargo test -p uc_node --features fault-injection -- --test-threads=1
cargo test -p uc-crashtest --features hard-crash-tests   # multi-process kill -9
```

---

## 6. Measured QUIC-vs-UDP A/B

### 6.1 The instruments

- **`internode-rpc-bench`** (`uc_autobench`, Task 19) — a transport-isolated microbench: an
  open-loop, coordinated-omission-free driver hammers `AppendEntries`-shaped RPCs node→node
  over a uniform `EchoClient::rpc` shim (`udp_echo_pair` / `quic_echo_pair` in
  `uc_node::network::bench_support`), records an HDR histogram, and emits the **same 13-column
  CSV schema task13 uses** so the curves overlay. This isolates the *transport RPC* path — not
  the full commit path. It is the fair, apples-to-apples transport measure.
- **End-to-end ladder** — `uc_autobench/scripts/run-uc-Nnode.sh` (arbitrary N) +
  `UC_TRANSPORT={quic,udp}` threaded through `uc-node-launch`, plus the `bench-infra` Ansible
  knobs (`transport`, `netem_loss_pct`, `netem_delay_ms`) for a realistic cross-host LAN run
  with `tc netem` loss/latency. This is the full-stack number the operator runs cross-host.

CSV schema (ns for the latency columns):
`system, config, workload, payload_bytes, inflight, target_rate, achieved_rate, p50_ns, p99_ns, p99_9_ns, p99_99_ns, max_ns, count`.

### 6.2 Microbench results (loopback, transport-isolated)

All rows actually measured on this host with `internode-rpc-bench --release`, loopback,
`rpc-echo` workload. Latencies converted to ms.

**64 B payload, inflight 8, 5000 req/s, 3 s:**

| transport | achieved/s | p50 | p99 | p99.9 | p99.99 | max | count |
|---|--:|--:|--:|--:|--:|--:|--:|
| **UDP** | 4999.6 | 0.807 ms | 2.501 ms | 3.281 ms | 3.879 ms | 3.977 ms | 15000 |
| **QUIC** | 4998.1 | 0.634 ms | 1.359 ms | 2.435 ms | 3.478 ms | 3.674 ms | 14996 |

**1024 B payload, inflight 8, 2000 req/s, 3 s:**

| transport | achieved/s | p50 | p99 | p99.9 | p99.99 | max | count |
|---|--:|--:|--:|--:|--:|--:|--:|
| **UDP** | 1999.4 | 0.701 ms | 1.998 ms | 2.210 ms | 2.308 ms | 2.308 ms | 5999 |
| **QUIC** | 1999.3 | 0.795 ms | 2.116 ms | 2.210 ms | 2.290 ms | 2.290 ms | 6000 |

Raw CSV rows (for the record):

```
udp-rpc,loopback,rpc-echo,64,8,5000,4999.6,807423,2500607,3280895,3878911,3977215,15000
quic-rpc,loopback,rpc-echo,64,8,5000,4998.1,634367,1358847,2435071,3477503,3674111,14996
udp-rpc,loopback,rpc-echo,1024,8,2000,1999.4,701439,1997823,2209791,2308095,2308095,5999
quic-rpc,loopback,rpc-echo,1024,8,2000,1999.3,794623,2115583,2209791,2289663,2289663,6000
```

### 6.3 Verdict (honest)

**On loopback, the result is mixed and payload-dependent, and QUIC currently has the edge on
the small-payload case.** At 64 B / 5000 req/s, QUIC is faster on both p50 (0.634 vs
0.807 ms) and p99 (1.36 vs 2.50 ms). At 1024 B / 2000 req/s the two are within noise and UDP
is even marginally ahead (p50 0.70 vs 0.80 ms; p99 2.00 vs 2.12 ms).

Why UDP is *not* an automatic win on loopback: our UDP mux runs an **in-band per-session
ticker** (heartbeat / SM / NAK-retry) and flow-control bookkeeping on the same datagram path,
whereas `quinn` is a mature, heavily optimized stack. On loopback — where there is **no real
network latency and effectively no loss** — none of UDP's leaner-path advantages (no TLS
handshake, no congestion control, no per-stream state) can pay off, while its per-RPC ticker
overhead is fully exposed. This is the **real transport** measured fairly, so the small-
payload p99 gap is a true datapoint, not an artifact — but loopback systematically flatters
the stack with the most machinery (QUIC) and penalizes the one optimizing for a network that
isn't there (UDP).

**Crucially, the loopback result does NOT settle the LAN question.** On loopback there is no
RTT, no jitter, and no packet loss — precisely the regime where UDP's leaner path (no
handshake, no CC ramp, NAK-only recovery of the rare drop) could win, and where QUIC's
conservative congestion control could cost. The A/B that decides the default is the
**cross-host LAN run**, which the operator runs with the same instruments plus the netem
knobs:

```bash
UC_TRANSPORT=quic N=3 bash uc_autobench/scripts/run-uc-Nnode.sh
UC_TRANSPORT=udp  N=3 bash uc_autobench/scripts/run-uc-Nnode.sh
# cross-host, with injected loss/latency, via bench-infra Ansible:
#   group_vars: transport, netem_loss_pct, netem_delay_ms, iface
```

(The heavy multi-node end-to-end ladder was not run cross-host in this session — it is the
operator's run on real hardware; the loopback ladder runs locally but understates the
network and so is not the deciding measure either.)

**Recommendation: keep QUIC as the default.** Nothing measured here justifies changing it —
QUIC wins the small-payload loopback case and is encrypted-by-default. UDP is proven correct
and is competitive at larger payloads; whether it wins on a real LAN is an open question to be
settled by the cross-host A/B above **before** the default is reconsidered. Don't oversell the
loopback numbers in either direction.

### 6.4 Cross-host results (REAL LAN — the deciding A/B, run 2026-06-18)

Ran on **2× Hetzner `ccx13`** (2 dedicated vCPU), node↔node over the **private network**
(`enp7s0`), single-inflight `ping` mode (sequential RTT — the latency-bound regime), 64 B,
symmetric `tc netem` applied per leg on both hosts. This is the cross-host run §6.3 deferred to
the operator, executed via the split-role harness (`internode-rpc-bench --role server` on
node0, `--role client` on node1; `make up-ping` → sweep → `make destroy`).

| netem (per leg) | UDP p50 / p99 | QUIC p50 / p99 |
|---|--:|--:|
| **clean** | **0.305 ms / 0.413 ms** | 0.339 ms / 0.467 ms |
| +1 ms (~2 ms RTT) | 2.42 ms / 3.97 ms | 2.49 ms / 3.08 ms |
| +5 ms (~10 ms RTT) | 10.76 ms / 11.58 ms | 10.83 ms / 13.10 ms |
| 1 % loss | *(no row — harness gap)* | 0.356 ms / **28.5 ms** |
| +5 ms, 1 % loss | *(no row)* | 10.78 ms / **59 ms** |

**Findings:**
- **Clean LAN: UDP wins** — p50 305 vs 339 µs (~10 % lower) *and* p99 413 vs 467 µs. The
  leaner-path advantage loopback could not show **does** appear once there's a real network.
  This **inverts the loopback small-payload verdict** (§6.3) for the latency-bound, single-
  inflight case.
- **Under added delay: ~even** — link delay dominates and the transport difference washes out
  (both ≈2.4 ms at +1 ms, ≈10.8 ms at +5 ms; QUIC slightly better p99 at +1 ms).
- **Under loss: incomplete on the UDP side — a HARNESS gap, not a transport defect.** The
  single-inflight echo-`ping` has no retry layer, so the first loss-hit RPC errors and aborts
  the run → no UDP row. The UDP *transport* recovers loss fine in the actual cluster
  (linearizable under 10 % loss, §5) because openraft retries; the raw ping doesn't. QUIC's
  ping survives loss but with a severe p99 tail (28–59 ms — head-of-line + retransmit). To
  measure UDP-under-loss fairly the ping client needs a per-RPC timeout-and-count (or use the
  open-loop `ladder` mode, which tolerates drops). Tracked as harness follow-up.

**Aeron baseline: deferred (infeasible on the cheap fleet).** The canonical Aeron RTT harness
(`remote-echo-benchmarks` → `echo-server` / `echo-client`) requires **≥4 isolated busy-spin CPU
cores per host** (driver conductor/sender/receiver + load-rig/echo) — which does not fit
2-vCPU `ccx13`. Launchers were identified on the live dist and `aeron_echo_launcher` corrected
to `echo-server`; a fair Aeron floor needs a ≥4-dedicated-core instance (e.g. `ccx33`) — a
future run.

**Updated verdict:** the cross-host clean-LAN result is the first evidence UDP's design pays
off where it is meant to (latency-bound, real network), and it inverts the loopback small-
payload finding. It is **not yet a mandate to switch the default**: one fleet, one payload,
single-inflight, no Aeron floor, and the loss axis unmeasured for UDP. **Keep QUIC default**;
the UDP-favorable LAN signal warrants a deeper cross-host run (throughput/inflight sweep + a
loss-tolerant ping + the Aeron baseline on dedicated cores) before the default is reconsidered.

### 6.5 Cross-host FAN-OUT / Raft-replication results (run 2026-06-18, 3× ccx33)

A fan-out mode (`internode-rpc-bench --mode fanout --connect a,b --quorum K`) models a Raft
leader replicating a log entry to N followers and committing on majority ack: the leader fires
concurrent RPCs to all followers each round and records the **K-of-N quorum latency** (K=1 =
3-node commit = faster follower; K=2 = all-acks). Run on a **3× ccx33** fleet (8 dedicated
vCPU — sized for the Aeron baseline) over the private network, node0 leader → node1+node2,
64 B, symmetric netem on all three.

| netem (per leg) | udp-fanout K=1 / K=2 | quic-fanout K=1 / K=2 |
|---|--:|--:|
| **clean** | **p50 257 µs** / —* | p50 319 µs / 324 µs |
| +1 ms | 2.34 ms / 2.39 ms | 2.42 ms / 2.44 ms |
| +5 ms | 10.6 ms / 10.7 ms | 10.7 ms / 10.7 ms |
| 1 % loss | p50 316 µs, p99 755 µs (105 rounds) | p50 329 µs, p99 28.6 ms (K=2; 2213 rounds) |

\* one transient miss (udp clean K=2); the impaired K=2 cells succeeded.

**Findings:**
- **Clean: UDP fan-out commit latency (K=1) 257 µs beats QUIC 319 µs (~20 %)** — the same UDP
  win as the per-link ping (§6.4), now in the replicate-to-2-followers model. K=2 (all-acks)
  is marginally above K=1, as expected (slower follower).
- **Added delay: ~even** — link delay dominates; transport difference washes out.
- **Loss is nuanced:** the loss-tolerant ping fix means UDP now produces rows under loss. UDP
  holds a **tighter tail** (p99 755 µs) but completes **far fewer rounds** (105 vs QUIC's 2213
  in the window) — NAK + 1 s-timeout recovery is slower *per round* than QUIC's in the raw,
  un-pipelined ping; QUIC completes many rounds but with a brutal p99 tail (28.6 ms). In the
  real cluster, openraft's pipelining/retry hides this per-round cost (the transport stays
  linearizable under 10 % loss, §5).

**Aeron per-link baseline: attempted, not completed.** On ccx33 the Aeron echo got
substantially wired — channels overridden to the private IPs, all required `LoadTestRig`
parameters resolved (`message.rate`/`message.length`/`iterations`/`warmup`/`batch.size`/
`output.file`), `BusySpinIdleStrategy`, and the `echo-server` (EchoNode) confirmed alive on the
follower — but the cross-host Aeron **connection never established** (`awaitConnected` 60 s
timeout). The remaining blocker is Aeron's cross-host channel/driver wiring (likely the client
sharing the node's existing media driver and/or the non-canonical port/endpoint setup), which
is exactly what the upstream **`remote-echo-benchmarks` orchestrator** manages — but that needs
its full ~20-variable env (CPU-core pinning per driver/app thread + `*_DESTINATION_CHANNEL` /
`*_SOURCE_CHANNEL` for both ends). Wiring that orchestrator into the harness is the right way to
get a canonical Aeron floor and is left as a dedicated follow-up; an ad-hoc `echo-server` +
`echo-client` launch is not sufficient. Note: a *true* Aeron leader→2-follower **quorum**
number would additionally require custom Aeron-client Java (publish-to-N + quorum logic) — the
benchmarks echo is point-to-point only.

**Verdict (updated): keep QUIC default.** The fan-out result reinforces §6.4 — UDP wins
latency-bound commit RTT on a clean LAN (257 vs 319 µs) — but the loss behavior (slow per-round
recovery in the raw ping), the still-missing Aeron floor, and the single-inflight/one-payload
scope all argue against changing the default on this evidence. The harness (split-role ping +
fan-out + netem + the `up-fanout FANOUT_INSTANCE_TYPE=` knob) is now in place to run the deeper
comparison cheaply.

---

## 7. What's deferred / future work

- **Congestion control** — static window for v1 (LAN assumption); the config seam + frame set
  leave room to add a pluggable strategy.
- **Multicast** — not built; unicast point-to-point only.
- **UDP encryption** — v1 is plaintext for the trusted-LAN A/B; QUIC stays the
  encrypted-by-default option. A DTLS-or-similar story is a later phase if UDP wins the LAN
  A/B.
- **Honoring the SM-advertised window** — the receiver advertises a flow-control window but
  the sender does not yet hold at it for true back-pressure (both ends default to 128 KiB, so
  the credit never binds). Real flow-control fidelity is deferred until a workload exercises
  it.
- **Session eviction** — per-session tickers are detached and there is no idle-session
  eviction in v1; a long-idle session's ticker (and its share of the registry) lingers until
  process exit. Documented, bounded in tests; an idle-timeout evictor is future work.
- **Reducing UDP's per-RPC overhead** — the small-payload loopback gap (§6.3) is the in-band
  ticker + flow-control bookkeeping. If the cross-host A/B shows UDP otherwise winning, moving
  the ticker off the hot path / batching SM updates is the lever to close it.
- **Cross-host LAN A/B** — the deciding measurement (§6.3) is the operator's to run on real
  hardware via `run-uc-Nnode.sh` + the `bench-infra` netem knobs.

Design scaffolding retained per CLAUDE.md:
`docs/superpowers/specs/2026-06-17-inter-node-udp-transport-design.md` and
`docs/superpowers/plans/2026-06-17-inter-node-udp-transport.md`.
