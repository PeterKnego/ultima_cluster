# Inter-node UDP transport (Aeron-style reliable unicast) — design

**Date:** 2026-06-17
**Branch:** `feat/inter-node-udp-transport`
**Status:** Design — approved, pending implementation plan.

## 1. Goal & motivation

`ultima_cluster` carries cross-host consensus traffic over QUIC (`quinn`) today: one
persistent connection per peer-pair, a fresh bidirectional stream per RPC, TLS by
default. QUIC gives reliability + ordered streams + congestion control + encryption,
but for a **trusted LAN with a small fixed peer set** it also carries cost we may not
need on the latency-critical replication path: the TLS handshake, conservative
congestion control, per-stream state, and userspace QUIC-stack overhead.

This feature builds a second inter-node transport — an **Aeron-style reliable-unicast
UDP channel, implemented from scratch in-tree** — behind a pluggable transport seam,
so we can A/B it against QUIC on the same harness and **choose the best approach with
measurements rather than assertions.** QUIC remains the default and is untouched.

Three concrete targets (from the brainstorming brief):

1. Add a reliable-UDP transport, grounded in how Aeron's UDP media driver works.
2. Extend the benchmark harness to run arbitrary multi-node tests and A/B transports.
3. Add a `uc_autobench` task that isolates and improves **inter-node** network perf.

**Reliability model.** openraft already retries failed RPCs (it re-sends
`AppendEntries` from the matched index, re-issues votes, re-streams snapshots). So the
transport does *not* need to be a never-breaking stream — it must recover ordinary
packet loss efficiently (so we don't force full-RPC retransmits on every dropped
datagram), but a truly failed session may surface an error and let openraft's failure
detector + retry take over. We optimize the common case; we do not reimplement Aeron's
full bulletproofing.

This sits alongside the same-host story: same-host service↔node and client→node
traffic stays on shmem rings (task08/task11); only **cross-host node↔node** traffic is
in scope here. task13 already benchmarked Aeron *IPC* (same-host) as a transport floor;
this is the cross-host sibling — Aeron's *UDP* path as the reference.

## 2. Aeron reference (what we port vs. what we drop)

Source-verified against Aeron `master` (the `io.aeron.protocol.*Flyweight` classes,
`Configuration.java`, `FrameDescriptor.java`, `LossDetector.java`,
`RetransmitHandler.java`) and the Aeron wiki Transport Protocol Specification. Aeron
frames are little-endian.

**Aeron mechanisms we port (simplified):**

- **Range-based NAK loss recovery.** Receiver detects a gap (a frame whose
  length-prefix stays unwritten *after* a frame that did arrive), sends a NAK carrying
  a `(start, length)` range — one NAK covers a whole contiguous gap — and the sender
  retransmits the range. Aeron's unicast NAK delay is ~1µs (near-zero); a resend
  "lingers" ~10 ms to suppress duplicate NAKs for the same range.
- **Receiver-driven flow control.** The receiver advertises a consumption position +
  a **receiver window** via Status Messages (SMs); the sender's send limit =
  `consumption_position + receiver_window`. Aeron default window =
  `min(128 KiB, termLength/2)` (128 KiB derives from a 10 Gbps × 100µs LAN
  bandwidth-delay product). SMs sent on a 200 ms periodic timer, on ≥25%-window
  consumption advance, and on term rotation. A receiver silent for 5 s is dropped.
- **MTU fragmentation.** Default MTU 1408 B (= 44×32). Messages above `MTU − 32` (1376 B
  payload) split into BEGIN…(middle)…END fragments (flag bits `0x80` BEGIN, `0x40` END,
  `0xC0` unfragmented), reassembled at the receiver by a per-session builder that
  validates offset continuity.
- **Liveness via cheap periodic beats + a longer death timeout.** Sender heartbeats
  (zero-length DATA frames, ~100 ms when idle) and receiver SMs (200 ms) each act as a
  one-direction keepalive; hard-death thresholds are ~100× larger (5–10 s).

**Aeron architecture details we deliberately drop** (they exist for Aeron's
multicast-capable, zero-GC, separate-media-driver design — not protocol requirements):

- **3-term rotating mmap log buffer + position algebra.** Aeron keys everything on
  `(initial_term_id, term_id, term_offset)` with `position = ((termId − initialTermId)
  << log2(termLength)) + termOffset`, and rotates 3 power-of-two terms (active / dirty
  / pre-zeroed) so the wait-free publish path never stalls on zeroing. We have no shared
  log buffer to size or zero → replace with a flat monotonic `u64` sequence per session
  and an in-memory retain window.
- **Media-driver/client process split, shmem log buffers, conductor duty cycle,
  counters file.** Pure single-process artifacts — the channel lives inside `uc_node`.
- **Multicast everything** — `OptimalMulticastDelayGenerator` (RFC 5401 randomized NAK
  backoff), min/max multicast flow control, group tags, TTL, late-joiner SETUP
  re-request. Unicast point-to-point needs none.
- **Congestion control** (pluggable cubic strategy). LAN + fixed small peer set →
  static window for v1.
- **SETUP-frame geometry negotiation.** Aeron's SETUP communicates term length /
  initial term id / MTU so a receiver can allocate matching mmap buffers. We have no
  such buffers → replace with a trivial one-time handshake carrying `app_id` +
  `protocol_version` + `session_id` + agreed max-datagram size (reusing the existing
  IPC-entry validation posture).
- **32-byte FRAME_ALIGNMENT + PAD frames, RTT-measurement frames, reserved value,
  EOS/REVOKED flags, error/resolution frames.** Alignment is for atomic in-place reads
  of the shared buffer; RTT frames tune timers Raft already owns.

Reference URLs are listed in §10.

## 3. Architecture — the transport seam

The only contract openraft imposes is `RaftNetworkFactory<TypeConfig>` (outbound) plus
an inbound listener. Today both halves are hardcoded in `uc_node/src/runtime/builder.rs`
(`QuicRaftNetworkFactory::new` at ~line 388, `spawn_server` at ~line 404).

We introduce a **trait-based transport seam** so adding a transport never requires
editing consensus/builder wiring — future kernel-bypass stacks (DPDK, io_uring, RDMA
verbs), OpenUCX, etc. plug in by implementing one trait.

```rust
/// A pluggable inter-node transport. Bundles both halves openraft needs:
/// an outbound RaftNetworkFactory and an inbound listener.
pub trait ClusterTransport: Send + Sync + 'static {
    type Factory: RaftNetworkFactory<TypeConfig>;
    type Server: Send;   // RAII listener handle with async shutdown()

    /// Build the outbound factory. Shared sockets/endpoints are created here.
    fn build_factory(&self, ctx: &TransportCtx) -> Result<Self::Factory, NetworkError>;

    /// Spawn the inbound listener dispatching into the local Raft instance.
    fn spawn_server<SM: RaftStateMachine<TypeConfig>>(
        &self, ctx: &TransportCtx, raft: Raft<TypeConfig, SM>,
    ) -> Result<Self::Server, NetworkError>;
}

/// Shared inputs every transport needs.
pub struct TransportCtx {
    pub node_id: NodeId,
    pub listen_addr: SocketAddr,
    pub app_id: String,
    pub data_dir: PathBuf,        // certs/keys for transports that want them
    #[cfg(feature = "fault-injection")]
    pub fault_table: Option<Arc<fault::FaultTable>>,
}
```

Config selector, parallel to the existing `TlsConfig`:

```rust
// config.rs
pub enum Transport {
    Quic,                // default — today's path, byte-for-byte unchanged
    Udp(UdpTuning),      // new: Aeron-style reliable unicast
    // future: Custom(Arc<dyn ...>) / a registry for out-of-tree transports
}
```

`builder.rs` becomes transport-agnostic: resolve `config.transport` to a concrete
`ClusterTransport`, call `build_factory` + `spawn_server`, never naming QUIC or UDP.

**Caveat documented, not solved here:** kernel-bypass stacks often want to own the
runtime/poll loop and don't speak `tokio::net`. `ClusterTransport` deliberately makes no
tokio-socket assumption — it only promises "give me a factory + a server"; how each
transport drives I/O is its own business. The shared `frame.rs` / `codec.rs` stay the
reusable RPC-payload layer above all transports.

**Module reshape** (`uc_node/src/network/`):

```
network/
  mod.rs          NetworkError, ClusterTransport, TransportCtx, Transport resolution,
                  shared frame/codec re-exports
  frame.rs        shared, unchanged (Frame / MessageType / request_id correlator)
  codec.rs        shared, unchanged (encode/decode the 3 RPC classes)
  fault.rs        extended: drop / delay / reorder, used by BOTH transports
  quic/           today's client.rs server.rs instance.rs factory.rs tls.rs, moved
  udp/            new: wire.rs channel.rs session.rs factory.rs instance.rs server.rs
```

Both transports reuse `frame.rs`/`codec.rs` for the RPC request/response payload — the
UDP work is the *reliable channel underneath* the frame, not a new RPC encoding.

## 4. The reliable-unicast UDP channel (`udp/`)

**One shared `tokio::net::UdpSocket` per process** (mirrors the single QUIC client
endpoint). A single receive-dispatch loop demultiplexes inbound datagrams to per-peer
**sessions** keyed by `session_id`.

### 4.1 Wire frames (`wire.rs`)

Compact ~16-byte common header, little-endian: `len: u32`, `version: u8`, `type: u8`,
`flags: u8`, `_pad: u8`, `session_id: u32`, `seq: u64` (exact layout finalized in the
plan; body CRC reused from `frame.rs` posture). Four frame types:

| Type | Carries | Purpose |
|---|---|---|
| `DATA` | `seq`, BEGIN/END flags, payload fragment | one fragment of a framed RPC message |
| `NAK` | `(start_seq, count)` | range gap report → triggers retransmit |
| `SM` (ack/window) | `highest_contiguous_seq`, `window` | flow-control credit + receiver→sender keepalive |
| `HEARTBEAT` | current `seq` | sender→receiver keepalive when idle |

Plus a one-time `HELLO`/handshake (carried as a DATA flag or a 5th type — decided in the
plan) carrying `app_id` + `protocol_version` + `session_id` + max-datagram size.

### 4.2 Flat sequence model

Each session has a monotonic `u64` send seq. No `term_id`/`term_offset`/`initial_term_id`
algebra and no 3-term rotation — there is no shared log buffer to size or zero. A
contiguous-received cursor + a reassembly buffer is all the receiver tracks.

### 4.3 NAK retransmit

Sender retains unacked DATA frames in an in-memory window (`BTreeMap<u64, Bytes>` or a
ring), bounded by the flow-control credit. Receiver gap-detects on seq discontinuity and
sends `NAK(start, count)`; sender resends the range from the window, with a ~10 ms
linger/dedup so a duplicate NAK doesn't double-send. Unicast NAK delay ≈ 0. A frame
acked past the window is evicted.

### 4.4 Flow control

Receiver advertises `highest_contiguous_seq + window` via SM, sent on a 200 ms periodic
timer and on ≥25%-window consumption advance. Sender holds at the advertised limit
(back-pressure: the send call yields/queues rather than overrunning). Default window
128 KiB, `UdpTuning`-configurable. A peer silent past the death timeout → the session is
torn down and the in-flight RPC errors, handing the failure to openraft.

### 4.5 Fragmentation

RPC `Frame`s above `mtu − header` split into BEGIN…(middle)…END DATA fragments,
reassembled per-session into a `BytesMut` before the completed `Frame` is handed up.
Covers batched `AppendEntries` (the `max_payload_entries` knob can produce large bodies)
and `InstallSnapshot` chunks. Default MTU 1408, `UdpTuning`-configurable.

### 4.6 Handshake & liveness

No Aeron SETUP geometry. First contact does a trivial handshake validating `app_id` +
`protocol_version` (mismatch → refuse, matching the existing IPC-entry posture) and
agreeing `session_id` + max datagram. Thereafter idle HEARTBEAT (sender) + SM (receiver)
are the two-direction keepalive; a long death timeout surfaces a dead peer as a
transport error.

## 5. RaftNetwork + server on top (`udp/instance.rs`, `udp/server.rs`, `udp/factory.rs`)

Thin, because the channel does the hard part and the RPC encoding is shared.

- **`UdpRaftNetwork`** impl openraft's V1 `RaftNetwork` (wrapped by the same `Adapter`→V2
  the QUIC path uses), mirroring `quic/instance.rs`: encode body via `codec`, get-or-open
  the per-peer session from a shared pool, send the request `Frame`, await the correlated
  response. The existing `Frame.request_id` is the correlator — multiple in-flight RPCs
  multiplex over one ordered session and demux by `request_id` (QUIC got this for free
  via a fresh bi-stream per RPC; here it does real work).
- **`spawn_udp_server`** runs the receive-dispatch loop; a reassembled request `Frame`
  dispatches into `raft.append_entries / vote / install_snapshot` (the `dispatch()` body
  is identical to `quic/server.rs`), and the response `Frame` goes back on the same
  session.
- **`UdpRaftNetworkFactory`** holds the shared socket + session pool + app_id;
  `new_client` defers connection exactly like QUIC (the trait can't return `Result`). On
  session-fatal error: compare-and-evict the pooled session (same pattern as
  `quic/instance.rs`), surface `RPCError` so openraft retries; the next attempt
  re-handshakes.

Keeping the surface parallel to the QUIC files makes the A/B apples-to-apples.

## 6. Correctness & fault injection

An in-tree reliability layer must be *proven*, reusing existing verification machinery:

- **Extend `fault.rs`** (today block/partition) with `drop(p)`, `delay(d)`, `reorder`
  hooks, gated by the `fault-injection` feature, applied in **both** transports' send
  paths. Deterministic + seedable.
- **Run existing suites against UDP** by flipping the transport field: the lincheck
  capstone (`uc_node/tests/lin_register.rs`), the partition suite
  (`uc_node/tests/lin_partition.rs`), and the multi-process hard-crash test
  (`examples/uc-crashtest`). Bar: stays **linearizable** under drop/delay/reorder +
  partition + kill-9.
- **Targeted channel unit tests** (`udp/`): gap→NAK→retransmit recovery;
  fragmentation/reassembly round-trip across MTU boundaries; flow-control back-pressure
  (a slow receiver is never overrun); duplicate/dedup; out-of-order delivery.
- **netem (`tc`)** in `bench-infra` for realistic cross-host loss/latency on perf runs —
  layered with the in-process fault layer (deterministic correctness + realistic perf).

## 7. Benchmark harness extension (target 2) + autobench A/B (target 3)

- **Generalize `uc-node-launch` + `run-uc-3node.sh` to arbitrary N**, plus a
  `--transport quic|udp` flag threaded via env (the existing `UC_DURABILITY` /
  `UC_MAX_PAYLOAD_ENTRIES` pattern → a new `UC_TRANSPORT`). One script spins up N nodes on
  either transport. `bench-infra` Ansible `group_vars` gets the same knob so real
  multi-host runs A/B cleanly.
- **New `uc_autobench` inter-node network microbench (target 3)** — isolates the
  *transport RPC* path, not the full commit path: a driver that hammers
  AppendEntries-shaped RPCs node→node and records HDR latency/throughput per transport,
  emitting the **same CSV schema task13 uses** so curves overlay. Swept over payload size
  × in-flight × injected loss. This is the instrument that lets us *choose the best
  approach* (QUIC vs UDP). The existing `commit-path-load` end-to-end ladder also runs
  under both transports for the full-stack number.
- **Deliverable:** a QUIC-vs-UDP decomposition writeup in the task doc, in task13's style.

## 8. Phasing

One feature branch (`feat/inter-node-udp-transport`), one spec, phased so each phase
lands green and the QUIC path is never broken.

| Phase | Deliverable |
|---|---|
| A | `ClusterTransport` trait + `TransportCtx` + `Transport` config enum; move QUIC under `quic/`; `builder.rs` transport-agnostic. **QUIC still default; full test suite green.** |
| B | UDP channel core (`wire.rs` / `channel.rs` / `session.rs`): frames, flat seq, NAK retransmit, flow control, fragmentation, handshake/heartbeat. Channel unit tests. |
| C | `UdpRaftNetwork` + `spawn_udp_server` + `UdpRaftNetworkFactory` on top; UDP passes the in-process lincheck capstone. |
| D | Extend `fault.rs` (drop/delay/reorder); run partition + hard-crash suites under UDP. |
| E | N-node bench harness + `--transport` knob; netem in `bench-infra`. |
| F | `uc_autobench` inter-node microbench + QUIC-vs-UDP A/B writeup → consolidate into `docs/tasks/task16_inter_node_udp_transport.md`. |

Superpowers spec/plan artifacts retained per CLAUDE.md (not auto-deleted).

## 9. Non-goals (v1)

- Congestion control (static window; LAN assumption) and multicast — deferred; the
  config seam and frame set leave room to add them later.
- Encryption on the UDP path — v1 is plaintext for trusted-LAN A/B; QUIC remains the
  encrypted-by-default option. (A DTLS-or-similar story is a later phase if UDP wins.)
- Replacing QUIC as default — QUIC stays default until measurements justify otherwise.
- Kernel-bypass / OpenUCX transports — only the seam is built to accommodate them; no
  such transport ships here.

## 10. Aeron source references

- Transport Protocol Specification — https://github.com/aeron-io/aeron/wiki/Transport-Protocol-Specification
- Flow and Congestion Control — https://github.com/aeron-io/aeron/wiki/Flow-and-Congestion-Control
- Design Overview — https://github.com/aeron-io/aeron/wiki/Design-Overview
- `DataHeaderFlyweight` (32-byte data header) — https://github.com/aeron-io/aeron/blob/master/aeron-client/src/main/java/io/aeron/protocol/DataHeaderFlyweight.java
- `NakFlyweight` / `StatusMessageFlyweight` / `SetupFlyweight` — same `io/aeron/protocol/` dir
- `LossDetector` / `RetransmitHandler` — https://github.com/aeron-io/aeron/blob/master/aeron-driver/src/main/java/io/aeron/driver/
- `Configuration.java` (MTU 1408, window 128 KiB, 200 ms SM, NAK/linger constants) — `aeron-driver`
- `LogBufferDescriptor` / `FrameDescriptor` (position formula, 32-byte alignment) — `aeron-client`

**Freshness note:** Aeron defaults are version-dependent (e.g. retransmit linger
60 ms→10 ms at 1.44.0). Pin an Aeron version before citing exact numbers as ours.
