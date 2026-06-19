# Split-role busy-poll rung Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the `busypoll-udp` bench rung cross-host `--role server` / `--role client` support (it is currently loopback-only `--role both`), unblocking the deferred Phase B B1 measurement.

**Architecture:** The busy-poll echo logic (a dedicated `core_affinity`-pinned blocking-recv thread with `SO_BUSY_POLL`) already lives inside `busypoll_udp_echo_pair`. Extract its two halves into standalone `busypoll_udp_echo_server(listen)` / `busypoll_udp_echo_client(connect)` constructors — exactly how `udp_echo_server`/`udp_echo_client`/`udp_echo_pair` are structured — then reimplement the pair in terms of them and wire the two `internode-rpc-bench` dispatch sites that currently bail.

**Tech Stack:** Rust, `std::net::UdpSocket` + `libc` `SO_BUSY_POLL`, `core_affinity`, tokio mpsc/oneshot.

## Global Constraints

- openraft pinned `0.10.0-alpha.21` (untouched — this is bench code only).
- Throwaway BENCH harness code in `uc_node/src/network/bench_support.rs` — NOT production transport.
- Must build default AND `--features fault-injection`; `cargo clippy -p uc_node -- -D warnings` clean.
- DRY: the refactored `busypoll_udp_echo_pair` MUST call the two new constructors (mirror `udp_echo_pair`), not duplicate the thread bodies.
- Commit only `uc_node/src/network/bench_support.rs` + `uc_autobench/src/bin/internode-rpc-bench.rs`; never `git add -A` (pre-existing dirty `uc_autobench/tasks/netping/results.tsv` stays untouched).
- Out of scope: the cross-host B1 fleet run; any production transport change.

---

## Task 1: Split-role busy-poll constructors + dispatch wiring

**Files:**
- Modify: `uc_node/src/network/bench_support.rs` (extract two constructors from `busypoll_udp_echo_pair` at ~L545–644; the `EchoClientInner::BusyPollUdp`/`EchoServerInner::BusyPollUdp` variants + `set_so_busy_poll` at ~L515 are unchanged)
- Modify: `uc_autobench/src/bin/internode-rpc-bench.rs` (the `use` list ~L30–32; the `--role server` match ~L331; the `--role client`/`both` `busypoll-udp` arm ~L484–488)
- Test: inline `#[tokio::test]` in `bench_support.rs`

**Interfaces:**
- Consumes: `EchoClient`/`EchoServer`, the `BusyPollUdp` inner variants, `set_so_busy_poll`, `core_affinity`, and the loopback `udp_echo_pair` pattern (L249–254) as the template.
- Produces: `pub async fn busypoll_udp_echo_server(listen: SocketAddr) -> Result<EchoServer, NetworkError>` and `pub async fn busypoll_udp_echo_client(connect: SocketAddr) -> Result<EchoClient, NetworkError>`; the dispatch now accepts `--role server`/`--role client` for `busypoll-udp`.

- [ ] **Step 1: Write the failing split-role test** (in `bench_support.rs`, next to the existing `busypoll_udp_echo_roundtrips` test):

```rust
#[tokio::test]
async fn busypoll_udp_split_role_roundtrips() {
    // Build server and client SEPARATELY (the cross-host path), not via the pair.
    let server = busypoll_udp_echo_server("127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = server.local_addr().unwrap();
    let client = busypoll_udp_echo_client(addr).await.unwrap();
    let resp = client.rpc(bytes::Bytes::from_static(b"split-busypoll")).await.unwrap();
    assert_eq!(&resp[..], b"split-busypoll");
    server.shutdown().await;
}
```

- [ ] **Step 2: Run it, expect FAIL** — `cargo test -p uc_node busypoll_udp_split_role_roundtrips` → `busypoll_udp_echo_server`/`busypoll_udp_echo_client` not found.

- [ ] **Step 3: Extract `busypoll_udp_echo_server(listen)`.** Move the server half of the current `busypoll_udp_echo_pair` (bind, `set_so_busy_poll`, `local_addr`, `stop_tx`/`stop_flag`, the `server_thread` with its read-timeout recv→echo loop) into a new function, parameterizing the bind address with `listen` and pinning to the FIRST core (preserving the pair's `server_core = core_ids.first()`):

```rust
/// Bind a busy-poll UDP echo server on `listen` (BLOCKING socket + SO_BUSY_POLL,
/// dedicated core-pinned blocking-recv thread echoing each datagram back to its
/// sender). Cross-host responder for `--role server`. Pairs with
/// [`busypoll_udp_echo_client`]. On a kernel/NIC without SO_BUSY_POLL it logs and
/// continues (still round-trips, just without the busy-poll benefit).
pub async fn busypoll_udp_echo_server(listen: SocketAddr) -> Result<EchoServer, NetworkError> {
    let srv_sock = std::net::UdpSocket::bind(listen).map_err(NetworkError::Io)?;
    if let Err(e) = set_so_busy_poll(&srv_sock, 50) {
        eprintln!("busypoll-udp: SO_BUSY_POLL not supported on server socket ({}); continuing without busy-poll", e);
    }
    let local = srv_sock.local_addr().map_err(NetworkError::Io)?;
    let server_core = core_affinity::get_core_ids().unwrap_or_default().first().copied();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let stop_flag = std::sync::Arc::new(std::sync::Mutex::new(Some(stop_rx)));
    let server_thread = std::thread::spawn(move || {
        if let Some(core) = server_core {
            core_affinity::set_for_current(core);
        }
        let _ = srv_sock.set_read_timeout(Some(std::time::Duration::from_millis(50)));
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            match srv_sock.recv_from(&mut buf) {
                Ok((n, from)) => {
                    let _ = srv_sock.send_to(&buf[..n], from);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    if let Ok(mut guard) = stop_flag.lock()
                        && let Some(ref mut rx) = *guard
                        && rx.try_recv().is_ok()
                    {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    Ok(EchoServer {
        inner: EchoServerInner::BusyPollUdp { stop_tx, thread: server_thread, local },
    })
}
```

- [ ] **Step 4: Extract `busypoll_udp_echo_client(connect)`.** Move the client half (the `req_tx`/`req_rx` mpsc, the `client_thread` that binds + `connect`s + busy-polls), parameterizing the target with `connect` and pinning to the SECOND core (preserving the pair's `client_core = core_ids.get(1)`):

```rust
/// Build a busy-poll UDP echo client that `rpc`s to `connect` (BLOCKING socket +
/// SO_BUSY_POLL on a dedicated core-pinned thread; the async `rpc()` hands each
/// request to that thread via mpsc and awaits a oneshot reply). Cross-host driver
/// for `--role client`. Pairs with [`busypoll_udp_echo_server`].
pub async fn busypoll_udp_echo_client(connect: SocketAddr) -> Result<EchoClient, NetworkError> {
    let client_core = core_affinity::get_core_ids().unwrap_or_default().get(1).copied();
    let (req_tx, mut req_rx) =
        tokio::sync::mpsc::channel::<(bytes::Bytes, tokio::sync::oneshot::Sender<bytes::Bytes>)>(16);
    let client_thread = std::thread::spawn(move || {
        if let Some(core) = client_core {
            core_affinity::set_for_current(core);
        }
        let bind: &str = if connect.is_ipv6() { "[::]:0" } else { "0.0.0.0:0" };
        let cli_sock = std::net::UdpSocket::bind(bind).expect("client bind");
        if let Err(e) = set_so_busy_poll(&cli_sock, 50) {
            eprintln!("busypoll-udp: SO_BUSY_POLL not supported on client socket ({}); continuing without busy-poll", e);
        }
        cli_sock.connect(connect).expect("client connect");
        let mut buf = vec![0u8; 64 * 1024];
        while let Some((body, reply_tx)) = req_rx.blocking_recv() {
            let _ = cli_sock.send(&body);
            let echoed = match cli_sock.recv(&mut buf) {
                Ok(n) => bytes::Bytes::copy_from_slice(&buf[..n]),
                Err(_) => bytes::Bytes::new(),
            };
            let _ = reply_tx.send(echoed);
        }
    });
    Ok(EchoClient {
        inner: EchoClientInner::BusyPollUdp { tx: req_tx, thread: client_thread },
        next_id: AtomicU64::new(1),
    })
}
```
(Note: the loopback pair bound the client to `127.0.0.1:0`; binding the wildcard in the target's address family — as `udp_echo_client` does — is correct for both loopback and cross-host.)

- [ ] **Step 5: Reimplement the pair in terms of the constructors** (replace the entire current `busypoll_udp_echo_pair` body, mirroring `udp_echo_pair` at L249–254):

```rust
pub async fn busypoll_udp_echo_pair() -> Result<(EchoClient, EchoServer), NetworkError> {
    let server = busypoll_udp_echo_server("127.0.0.1:0".parse().unwrap()).await?;
    let server_addr = server.local_addr()?;
    let client = busypoll_udp_echo_client(server_addr).await?;
    Ok((client, server))
}
```

- [ ] **Step 6: Run both tests, expect PASS** — `cargo test -p uc_node busypoll_udp` (runs `busypoll_udp_echo_roundtrips` via the refactored pair AND the new `busypoll_udp_split_role_roundtrips`). Both pass.

- [ ] **Step 7: Wire dispatch in `internode-rpc-bench.rs`.**
  - Add `busypoll_udp_echo_server, busypoll_udp_echo_client` to the `use uc_node::network::bench_support::{...}` import list (~L30–32).
  - `--role server` match (~L331): replace `"busypoll-udp" => anyhow::bail!("busypoll-udp does not support --role server; use --role both"),` with `"busypoll-udp" => busypoll_udp_echo_server(args.listen).await?,` (mirroring the `"udp" => udp_echo_server(args.listen).await?` arm two lines above).
  - `--role client`/`both` arm (~L484–488): change the `"busypoll-udp"` arm so `--role client` uses `busypoll_udp_echo_client(first_connect()?)` and `--role both` keeps `busypoll_udp_echo_pair()` — mirror exactly how the `"udp"` arm splits client-vs-both (read that arm; do NOT keep the `if args.role == "client" { bail }`).

- [ ] **Step 8: Build both configs + clippy + smoke.**
  - `cargo build -p uc_node && cargo build -p uc_node --features fault-injection && cargo clippy -p uc_node -- -D warnings && cargo clippy -p uc_autobench -- -D warnings` (all clean).
  - Cross-host smoke on loopback (two processes): start a server, then a client against it:
    ```bash
    BIN=$(cargo metadata --format-version 1 --no-deps | python3 -c "import json,sys;print(json.load(sys.stdin)['target_directory'])")/release/internode-rpc-bench
    cargo build -p uc_autobench --bin internode-rpc-bench --release
    "$BIN" --role server --transport busypoll-udp --listen 127.0.0.1:9400 & SRV=$!
    sleep 1
    "$BIN" --role client --transport busypoll-udp --connect 127.0.0.1:9400 --mode ping --duration 3 --payload 64
    kill $SRV 2>/dev/null
    ```
    Expected: the client prints one CSV row `system=busypoll-udp-ping`, count>0 (proves split-role works as two processes).

- [ ] **Step 9: Commit**

```bash
git add uc_node/src/network/bench_support.rs uc_autobench/src/bin/internode-rpc-bench.rs
git commit -m "feat(bench): split-role busypoll-udp (server/client) for cross-host B1"
```

---

## Self-Review (done at authoring)

- **Spec coverage:** the approved design's two constructors → Steps 3–4; DRY pair refactor → Step 5; dispatch wiring (both bail sites + import) → Step 7; split-role test → Steps 1–6. All covered in one task.
- **Placeholder scan:** all steps carry complete code; the one "read the `udp` arm and mirror it" in Step 7 points at concrete existing code (the `udp` client/both arm) rather than inventing a shape — the surrounding `udp` arm IS the template, and the exact server-arm edit is given verbatim.
- **Type consistency:** `busypoll_udp_echo_server(SocketAddr)->EchoServer` / `busypoll_udp_echo_client(SocketAddr)->EchoClient` match the `udp_echo_server`/`udp_echo_client` signatures and the `BusyPollUdp` inner variants used by `EchoServer::shutdown`/`local_addr` and `EchoClient::rpc` (unchanged).

## Execution Handoff

One task; after it, the cross-host B1 run is a separate cheap 2-node fleet session (not in this plan).
