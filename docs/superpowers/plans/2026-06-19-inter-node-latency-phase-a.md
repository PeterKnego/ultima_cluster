# Inter-node Latency Phase A Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Quantify UC's inter-node latency gap vs Aeron on a local ladder, and adopt openraft `RaftNetworkV2` pipelined `stream_append` to hide per-RPC latency under real replication.

**Architecture:** Two task groups. **A1 (ladder):** add two throwaway echo rungs (bare-tokio-UDP, bare-busy-spin-UDP) to the existing `internode-rpc-bench` harness, run all four rungs on loopback in `ping` mode, derive the async-vs-busy-spin and UC-bookkeeping taxes, flamegraph the UC-UDP rung. **A2 (V2 streaming):** replace the per-peer `net.into_v2()` openraft `Adapter` with a UC type that impls `RaftNetworkV2` directly and overrides `stream_append` to run an **ordered, bounded-depth pipeline** of AppendEntries over the existing request-id-correlated UDP/QUIC transports; measure pipelined commit latency; re-run the correctness suites.

**Tech Stack:** Rust, tokio, openraft `0.10.0-alpha.21`, `hdrhistogram`, `futures-util` (`StreamExt::buffered`), `core_affinity` (new dev-dep for the busy-spin rung), `cargo flamegraph` / `perf`.

## Global Constraints

- openraft pinned at **`0.10.0-alpha.21`** (exact; do not bump — see `docs/openraft-known-issues.md`).
- `uc_protocol` stays `no_std`-friendly (`core` only) — none of this work touches it.
- Apply is sync/deterministic; this work is purely the inter-node network layer — do not touch the apply path.
- New bench rungs are **throwaway harness code under `uc_autobench` / `bench_support.rs`** — not production transport code.
- A2 must not regress linearizability: the lincheck capstone + partition + hard-crash suites must pass on the change.
- `cargo clippy --workspace -- -D warnings` must stay clean.
- QUIC remains the default transport; A2 changes the *append driving*, not the default.

---

## Task Group A1 — Local latency ladder

### Task 1: Add the `bare-tokio-udp` echo rung

**Files:**
- Modify: `uc_node/src/network/bench_support.rs` (add `EchoClientInner`/`EchoServerInner` variants + constructors)
- Modify: `uc_autobench/src/bin/internode-rpc-bench.rs` (transport dispatch, ~lines 448–464 and ~325–328)
- Test: `uc_node/src/network/bench_support.rs` (inline `#[tokio::test]`)

**Interfaces:**
- Consumes: existing `EchoClient { async fn rpc(&self, body: Bytes) -> Result<Bytes, NetworkError> }`, `EchoServer { fn local_addr(&self) -> Result<SocketAddr,_>; async fn shutdown(self) }`, and the `Frame`/`MessageType` wire types (`uc_node/src/network/frame.rs`).
- Produces: `pub async fn bare_tokio_udp_echo_pair() -> Result<(EchoClient, EchoServer), NetworkError>` and a `"bare-udp"` transport string accepted by the bench.

- [ ] **Step 1: Write the failing test** (in `bench_support.rs`)

```rust
#[tokio::test]
async fn bare_tokio_udp_echo_roundtrips() {
    let (client, server) = bare_tokio_udp_echo_pair().await.unwrap();
    let resp = client.rpc(bytes::Bytes::from_static(b"ping-payload")).await.unwrap();
    assert_eq!(&resp[..], b"ping-payload");
    server.shutdown().await;
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test -p uc_node bare_tokio_udp_echo_roundtrips`
Expected: FAIL — `bare_tokio_udp_echo_pair` not found.

- [ ] **Step 3: Implement the rung.** Add to the `EchoClientInner` enum (near lines 37–47) and `EchoServerInner` (near 96–102):

```rust
// EchoClientInner:
BareTokioUdp { sock: std::sync::Arc<tokio::net::UdpSocket>, peer: std::net::SocketAddr },
// EchoServerInner:
BareTokioUdp { handle: tokio::task::JoinHandle<()>, local: std::net::SocketAddr },
```

Add the constructor + the server loop (a bare echo: recv datagram, send same bytes back — NO Frame, NO protocol, this is the raw async-socket floor):

```rust
pub async fn bare_tokio_udp_echo_pair() -> Result<(EchoClient, EchoServer), NetworkError> {
    let srv = tokio::net::UdpSocket::bind("127.0.0.1:0").await.map_err(NetworkError::from)?;
    let local = srv.local_addr().map_err(NetworkError::from)?;
    let handle = tokio::spawn(async move {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match srv.recv_from(&mut buf).await {
                Ok((n, from)) => { let _ = srv.send_to(&buf[..n], from).await; }
                Err(_) => break,
            }
        }
    });
    let cli = tokio::net::UdpSocket::bind("127.0.0.1:0").await.map_err(NetworkError::from)?;
    cli.connect(local).await.map_err(NetworkError::from)?;
    let client = EchoClient { inner: EchoClientInner::BareTokioUdp { sock: std::sync::Arc::new(cli), peer: local } };
    let server = EchoServer { inner: EchoServerInner::BareTokioUdp { handle, local } };
    Ok((client, server))
}
```

Add the `EchoClient::rpc` arm (raw send/recv, echo compares bytes — no request-id needed at single-inflight):

```rust
EchoClientInner::BareTokioUdp { sock, .. } => {
    sock.send(&body).await.map_err(NetworkError::from)?;
    let mut buf = vec![0u8; 64 * 1024];
    let n = sock.recv(&mut buf).await.map_err(NetworkError::from)?;
    Ok(bytes::Bytes::copy_from_slice(&buf[..n]))
}
```

Add `EchoServer::shutdown`/`local_addr` arms (`handle.abort()` on shutdown; return `local`).

- [ ] **Step 4: Run the test to confirm it passes**

Run: `cargo test -p uc_node bare_tokio_udp_echo_roundtrips`
Expected: PASS.

- [ ] **Step 5: Wire the bench dispatch.** In `internode-rpc-bench.rs`, add a `"bare-udp"` arm alongside `"udp"`/`"quic"` (both the pair path ~448–464 and any server/client paths ~325–328), mapping system label `"bare-udp-rpc"` → `bare_tokio_udp_echo_pair()`.

- [ ] **Step 6: Smoke-run the rung in ping mode**

Run: `cargo run -p uc_autobench --bin internode-rpc-bench --release -- --role both --transport bare-udp --mode ping --duration 5 --payload 64`
Expected: one CSV row, `system=bare-udp-rpc`, non-zero p50/p99, count > 0.

- [ ] **Step 7: Commit**

```bash
git add uc_node/src/network/bench_support.rs uc_autobench/src/bin/internode-rpc-bench.rs
git commit -m "feat(bench): bare tokio-UDP echo rung (async-socket latency floor)"
```

### Task 2: Add the `bare-busyspin-udp` echo rung

**Files:**
- Modify: `uc_node/src/network/bench_support.rs`
- Modify: `uc_node/Cargo.toml` (add `core_affinity = "0.8"` under `[dev-dependencies]` or a `bench` feature — match how the crate already gates bench-only deps; if none, use `[dev-dependencies]` and gate the rung behind `#[cfg(any(test, feature = "bench"))]` consistent with existing `bench_support` gating)
- Test: inline `#[tokio::test]`

**Interfaces:**
- Produces: `pub async fn bare_busyspin_udp_echo_pair() -> Result<(EchoClient, EchoServer), NetworkError>`, transport string `"busyspin-udp"`. This rung uses a **blocking `std::net::UdpSocket` in a busy-recv loop on a dedicated `std::thread` pinned to a core** for BOTH sides — the userspace busy-spin floor. The client side still exposes the async `rpc()` surface by handing the request to the pinned thread over a oneshot.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn bare_busyspin_udp_echo_roundtrips() {
    let (client, server) = bare_busyspin_udp_echo_pair().await.unwrap();
    let resp = client.rpc(bytes::Bytes::from_static(b"spin-payload")).await.unwrap();
    assert_eq!(&resp[..], b"spin-payload");
    server.shutdown().await;
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p uc_node bare_busyspin_udp_echo_roundtrips`
Expected: FAIL — function not found.

- [ ] **Step 3: Implement.** Server: a `std::thread` pinned via `core_affinity::set_for_current(core)` running a non-blocking `std::net::UdpSocket` (`set_nonblocking(true)`) in a `loop { match sock.recv_from {Ok=>send_to; Err(WouldBlock)=>spin_loop()} }`. Client: a pinned `std::thread` owning a non-blocking socket; `rpc()` sends the request over a `tokio::sync::mpsc` to that thread, the thread busy-sends/busy-recvs and replies over a `oneshot`. Show the full server thread + the client thread + the `rpc` arm (busy-poll with `std::hint::spin_loop()`), each ≤40 lines. Use `core_affinity::get_core_ids()` and pin server to id[0], client to id[1] (fall back to unpinned if <2 cores).

- [ ] **Step 4: Run to confirm pass**

Run: `cargo test -p uc_node bare_busyspin_udp_echo_roundtrips`
Expected: PASS.

- [ ] **Step 5: Wire dispatch** for `"busyspin-udp"` (system label `"busyspin-udp-rpc"`), same call sites as Task 1 Step 5.

- [ ] **Step 6: Smoke-run**

Run: `cargo run -p uc_autobench --bin internode-rpc-bench --release -- --role both --transport busyspin-udp --mode ping --duration 5 --payload 64`
Expected: one CSV row, `system=busyspin-udp-rpc`, p50 measurably **lower** than the bare-tokio rung (this is the whole point).

- [ ] **Step 7: Commit**

```bash
git add uc_node/src/network/bench_support.rs uc_node/Cargo.toml uc_autobench/src/bin/internode-rpc-bench.rs
git commit -m "feat(bench): bare busy-spin UDP echo rung (userspace busy-spin floor)"
```

### Task 3: Ladder runner + derived taxes

**Files:**
- Create: `uc_autobench/scripts/latency-ladder.sh`
- Create: `uc_autobench/tasks/latency-ladder/results.tsv` (header only)

**Interfaces:**
- Consumes: the four transport strings `bare-udp`, `busyspin-udp`, `udp`, `quic` in `--mode ping`.
- Produces: a TSV with one row per rung and a printed summary of `tax_async_vs_busyspin = p50(bare-udp) − p50(busyspin-udp)` and `tax_uc_bookkeeping = p50(udp) − p50(bare-udp)`.

- [ ] **Step 1: Write the runner.** A bash script (`set -euo pipefail`) that builds release once, then loops the 4 transports running `internode-rpc-bench --role both --mode ping --duration 20 --payload 64`, appends each CSV row (prefixed with the rung name) to the TSV, and at the end greps the four p50 columns and prints the two derived taxes. Show the full script.

- [ ] **Step 2: Run it**

Run: `bash uc_autobench/scripts/latency-ladder.sh`
Expected: 4 rows in the TSV; printed `tax_async_vs_busyspin` and `tax_uc_bookkeeping` in ns, both positive.

- [ ] **Step 3: Commit**

```bash
git add uc_autobench/scripts/latency-ladder.sh uc_autobench/tasks/latency-ladder/results.tsv
git commit -m "feat(bench): 4-rung local latency ladder + derived taxes"
```

### Task 4: Flamegraph the UC-UDP rung + record findings

**Files:**
- Create: `docs/tasks/task17_inter_node_latency.md` (the decision memo — start it here, A2 appends)

**Interfaces:** none (analysis output).

- [ ] **Step 1: Capture a flamegraph** of rung 3 under sustained load.

Run: `cargo flamegraph -p uc_autobench --bin internode-rpc-bench -- --role both --transport udp --mode ping --duration 30 --payload 64`
(If `cargo flamegraph` is unavailable, use `perf record -g -- <same bin/args>` then `perf report`.)
Expected: an SVG/`perf` report; identify the top self-time frames on the send/recv path.

- [ ] **Step 2: Write the memo's Part A.** In `task17_inter_node_latency.md`, record: the 4-rung ladder table (p50/p99/p99.9), the two derived taxes, and the flamegraph's top contributors (confirm/refute the prediction that `state.lock().await` async-mutex round-trips + the in-band ticker dominate `tax_uc_bookkeeping`). State the **Gate A** read: is the async-vs-busy-spin tax large enough to motivate Phase B?

- [ ] **Step 3: Commit**

```bash
git add docs/tasks/task17_inter_node_latency.md
git commit -m "docs(task17): Part A — local latency ladder + UC-UDP flamegraph findings"
```

---

## Task Group A2 — V2 streaming (pipelined `stream_append`)

> Replaces the openraft `Adapter` (`net.into_v2()`) with a UC type implementing `RaftNetworkV2` directly and overriding `stream_append` to pipeline. UC's transports already correlate responses by request-id, so concurrent in-flight AppendEntries is wire-safe. openraft drives one `stream_append` per peer session (`replication/mod.rs:259`) and **requires responses yielded in input order** — so the pipeline is *ordered* and bounded-depth (`futures_util::StreamExt::buffered(DEPTH)`).

### Task 5: Confirm the openraft streaming contract (read-only spike)

**Files:** none (write findings into the memo in Task 8).

- [ ] **Step 1: Read and record the exact types** from `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/openraft-0.10.0-alpha.21/`:
  - `RaftNetworkV2::stream_append` signature — `src/network/v2/network.rs` (~L120–140).
  - `StreamAppendResult<C>` definition (the `Ok` item type of the response stream) — grep `pub struct StreamAppendResult` / `pub enum StreamAppendResult`. Record its fields and how an `AppendEntriesResponse<C>` maps into it.
  - Confirm `ReplicationCore` requires **in-order** responses (`src/replication/mod.rs` ~L287–341 drains the stream and matches responses to sent requests in order).
- [ ] **Step 2:** Write a 4-line note in the memo stating the exact `stream_append` signature, the `StreamAppendResult` shape, and "responses must be yielded in request order, bounded pipeline depth is the lever." No commit (folded into Task 8).

### Task 6: Make the per-peer network cheaply cloneable

**Files:**
- Modify: `uc_node/src/network/udp/instance.rs` (UdpRaftNetwork)
- Modify: `uc_node/src/network/quic/instance.rs` (QuicRaftNetwork)
- Test: inline unit test per file

**Interfaces:**
- Produces: `impl Clone for UdpRaftNetwork` and `impl Clone for QuicRaftNetwork` — cloning duplicates only the cheap shared handles (`Arc<UdpMux>` / `Arc<Endpoint>` + per-peer addr + the shared atomic request-id counter), NOT any owned buffers. Pipelining needs to launch N concurrent `append_entries` from one `&mut self`, which requires owning `Clone` handles.

- [ ] **Step 1: Write the failing test** (UDP shown; mirror for QUIC):

```rust
#[tokio::test]
async fn udp_raft_network_clone_shares_mux() {
    // build a UdpRaftNetwork over a test UdpMux; clone it; both clones target the same peer
    // and share the same Arc<UdpMux> (Arc::ptr_eq on the mux handle).
    let net = test_udp_raft_network();
    let net2 = net.clone();
    assert!(std::sync::Arc::ptr_eq(net.mux_arc(), net2.mux_arc()));
}
```
(Add a `#[cfg(test)] fn mux_arc(&self) -> &Arc<UdpMux>` accessor if none exists.)

- [ ] **Step 2: Run, expect FAIL** (no `Clone`). Run: `cargo test -p uc_node udp_raft_network_clone_shares_mux`.

- [ ] **Step 3: Derive/implement `Clone`** on both networks (all fields are already `Arc`/`Copy`/cheap — add `#[derive(Clone)]` or a manual impl that clones the `Arc`s and the addr). Confirm the request-id counter is an `Arc<AtomicU64>` shared across clones (so concurrent requests get distinct ids); if it's a bare `AtomicU64`, wrap it in `Arc`.

- [ ] **Step 4: Run, expect PASS** for both UDP and QUIC tests.

- [ ] **Step 5: Commit**

```bash
git add uc_node/src/network/udp/instance.rs uc_node/src/network/quic/instance.rs
git commit -m "refactor(net): make per-peer Raft networks cheaply Clone (Arc handles) for pipelining"
```

### Task 7: Override `stream_append` with an ordered bounded pipeline

**Files:**
- Create: `uc_node/src/network/pipelined.rs` (the `PipelinedNet<N>` wrapper)
- Modify: `uc_node/src/network/mod.rs` (mod decl + `PIPELINE_DEPTH` const, default 8, overridable via `NodeConfig`)
- Modify: `uc_node/src/network/udp/factory.rs:76` and `uc_node/src/network/quic/factory.rs:98` (return the pipelined type instead of `net.into_v2()`)
- Test: `uc_node/src/network/pipelined.rs` inline test

**Interfaces:**
- Consumes: `UdpRaftNetwork`/`QuicRaftNetwork` (now `Clone`, Task 6) and their existing V1 `RaftNetwork::append_entries`.
- Produces: `pub struct PipelinedNet<N> { inner: N, depth: usize }` that `impl RaftNetworkV2<TypeConfig>` by delegating `append_entries`/`vote`/`full_snapshot`/`transfer_leader` to `inner` (via the same logic the `Adapter` used) and **overriding `stream_append`** to map the input request stream through `inner.clone().append_entries(req)` with `StreamExt::buffered(depth)` to preserve order while keeping `depth` requests in flight. Factories return `PipelinedNet<UdpRaftNetwork>` / `PipelinedNet<QuicRaftNetwork>` as `Self::Network`.

- [ ] **Step 1: Write the failing test** — a streaming pipeline test against a mock that records concurrency:

```rust
#[tokio::test]
async fn stream_append_pipelines_in_order_bounded() {
    // mock inner: append_entries sleeps 20ms, records max concurrent in-flight via an AtomicUsize
    // feed 8 requests through PipelinedNet{depth:4}.stream_append(...)
    // assert: responses arrive in input order; observed max concurrency == 4 (not 1, not 8)
}
```

- [ ] **Step 2: Run, expect FAIL** (no `PipelinedNet`). Run: `cargo test -p uc_node stream_append_pipelines_in_order_bounded`.

- [ ] **Step 3: Implement `PipelinedNet`.** Delegate the unary methods to `inner` (copy the `Adapter`'s delegation — it just forwards to the V1 impl). Override `stream_append` (signature confirmed in Task 5):

```rust
fn stream_append<'s, S>(&'s mut self, input: S, option: RPCOption)
    -> BoxFuture<'s, Result<BoxStream<'s, Result<StreamAppendResult<TypeConfig>, RPCError<TypeConfig>>>, RPCError<TypeConfig>>>
where S: Stream<Item = AppendEntriesRequest<TypeConfig>> + OptionalSend + Unpin + 'static {
    let inner = self.inner.clone();
    let depth = self.depth;
    Box::pin(async move {
        let strm = input
            .map(move |req| { let mut n = inner.clone(); async move { n.append_entries(req, option.clone()).await } })
            .buffered(depth)                          // ORDERED, ≤depth in flight
            .map(|res| res.map(into_stream_append_result)); // AppendEntriesResponse -> StreamAppendResult (per Task 5)
        Ok(Box::pin(strm) as BoxStream<'_, _>)
    })
}
```
Implement `into_stream_append_result` per the `StreamAppendResult` shape recorded in Task 5.

- [ ] **Step 4: Run, expect PASS.** Run: `cargo test -p uc_node stream_append_pipelines_in_order_bounded`.

- [ ] **Step 5: Repoint the factories.** Replace `net.into_v2()` at `udp/factory.rs:76` and `quic/factory.rs:98` with `PipelinedNet::new(net, depth)` and change each `type Network = Adapter<...>` to `type Network = PipelinedNet<...>`.

- [ ] **Step 6: Build + clippy.** Run: `cargo build -p uc_node && cargo clippy -p uc_node -- -D warnings`. Expected: clean.

- [ ] **Step 7: Commit**

```bash
git add uc_node/src/network/pipelined.rs uc_node/src/network/mod.rs uc_node/src/network/udp/factory.rs uc_node/src/network/quic/factory.rs
git commit -m "feat(net): pipelined RaftNetworkV2::stream_append (ordered, bounded depth) over UDP+QUIC"
```

### Task 8: Correctness gate + pipelined-latency measurement

**Files:**
- Modify: `docs/tasks/task17_inter_node_latency.md` (append Part B)

- [ ] **Step 1: Run the linearizability + fault suites** on the change (the pipeline must not reorder/drop under faults):

Run: `cargo test -p uc_node` then `cargo test -p uc_node --features fault-injection -- --test-threads=1` then `cargo test -p uc-crashtest --features hard-crash-tests`
Expected: all pass (lincheck capstone, partition, hard-crash all green).

- [ ] **Step 2: Measure pipelined commit latency** — compare `fanout` mode (multi-follower, the pipelined commit path) before/after by toggling `PIPELINE_DEPTH` (1 = old sequential, 8 = pipelined):

Run: `PIPELINE_DEPTH=1 ... --mode fanout ...` vs `PIPELINE_DEPTH=8 ... --mode fanout ...` (loopback 3-node in-process, or note it needs the cross-host fleet for a real number).
Expected: depth-8 shows lower commit p50/p99 under load, or — if loopback can't show it — an explicit note that the real number needs the task16 §6.6 cross-host harness.

- [ ] **Step 3: Write Part B + the Gate-A recommendation** in the memo: V1→V2 streaming done, measured effect, and whether the ladder (Part A) + streaming (Part B) close enough of the gap that Phase B is/ isn't warranted. Route back to the spec's goal options.

- [ ] **Step 4: Commit**

```bash
git add docs/tasks/task17_inter_node_latency.md
git commit -m "docs(task17): Part B — pipelined stream_append measured + Gate A recommendation"
```

---

## Self-Review (done at authoring)

- **Spec coverage:** §3-A1 ladder → Tasks 1–4; §3-A2 V2 streaming → Tasks 5–8; Gate A → Task 8 Step 3. §4 Phase B is deliberately a separate plan (gated). §5 harness reuse → Tasks 1–3 reuse `internode-rpc-bench`. ✓
- **Placeholder scan:** the one soft spot is Task 7 Step 3's `into_stream_append_result` and the exact `StreamAppendResult` mapping — deliberately gated behind Task 5's read-and-record spike so the executor has the real type before writing it (not a placeholder, a sequenced dependency). All other steps carry concrete code/commands.
- **Type consistency:** `EchoClient::rpc`/`EchoServer::shutdown`/`local_addr` used consistently across Tasks 1–3; `PipelinedNet<N>` / `stream_append` / `StreamAppendResult` consistent across Tasks 6–8; `Clone` (Task 6) is the prerequisite the pipeline (Task 7) consumes.

## Execution Handoff

Phase B (cross-host AF_XDP + `SO_BUSY_POLL`) is intentionally **not** in this plan — it is gated on the Gate-A decision (Task 8) and gets its own plan if warranted.
