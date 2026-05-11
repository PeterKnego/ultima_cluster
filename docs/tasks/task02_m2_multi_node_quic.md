# Task 02 — M2: Multi-Node + QUIC Inter-Node Transport

**Status:** Shipped 2026-05-11 (merge commit on `main`).
**Branch:** `feat/m2-multi-node-quic` (merged) — 25 commits.
**Workspace:** `ultima_cluster/`.

## Goal

Replace M1's `NoopNetwork` placeholder with a real `RaftNetwork` impl over QUIC (`quinn`), wire `BootstrapConfig::Peers` + membership-change APIs on `NodeHandle`, and prove the result with a 3-node cluster test suite (election, replication, leader failover, snapshot install on new follower, membership change).

Also addresses three M1-review-blocker fixes uncovered between M1 ship and M2 start:
- `get_log_state` recovers real `node_id` (the M1 placeholder was wrong).
- `install_snapshot` durable across restarts.
- User vs framework `last_applied` cross-check at startup.

## Scope

**In M2:**
- Phase 0: three M1 cleanups (Tasks 1-3).
- TLS infrastructure with self-signed cert generation in `uc_node::network::tls` (Task 4).
- Wire framing in `uc_node::network::frame` — fixed 14-byte header + variable body + CRC32 trailer; `MessageType` enum; 16 MiB body size cap; sync + async decode (Task 5).
- RPC body codecs in `uc_node::network::codec` — bincode for the 6 openraft RPC body types (Task 6).
- QUIC server in `uc_node::network::server` — listener + per-stream lockstep dispatch (Task 7).
- QUIC client in `uc_node::network::client` — `PeerConn` with one bi-stream per RPC (Approach A) (Task 8).
- `QuicRaftNetworkFactory` + `QuicRaftNetwork` in `uc_node::network::{factory, instance}` — lazy-connect-on-request, shared `Endpoint`, Arc::ptr_eq compare-and-evict on failure, respects `RPCOption::hard_ttl()` (Task 9).
- Wired into `NodeBuilder::start`; `NoopNetwork` removed (Task 10).
- `BootstrapConfig::Peers` implementation + `NodeHandle::add_learner / change_membership / remove_node` (Task 11).
- 5 multi-node integration tests (Tasks 12-16).
- Polish (Task 17).

**Deferred to M3-M5:**
- `uc_protocol` ring buffers + `cnc.dat` layout (M3).
- `uc_service` shmem split + service-process bootstrap (M3).
- Snapshot region mmap transport (replaces `Cursor<Vec<u8>>`) (M5).
- `uc_client` real implementation (M4).
- `OutputHandler` wiring (M5).
- Real CA-validated TLS (`TlsConfig::Files`) — M2 ships only `TlsConfig::SelfSigned` with an `AcceptAnything` verifier (M5).
- Prometheus exporter (M5).
- Persistent per-class streams optimization (vs. one-bi-stream-per-RPC) (post-M5 perf).
- Operator-rotation of TLS keys (M5).

## Architecture

### Process model

Unchanged from M1: one `uc_node` process per host, generic over `S: StateMachine`. M3 introduces the shmem split; M2 still has the user's state machine embedded.

The QUIC inter-node transport runs on `tokio`. One persistent `quinn::Endpoint` per node (for outbound + inbound), shared across all peer connections. One QUIC connection per peer-pair, lazy-established on first RPC. Each RPC opens a fresh bidirectional stream — the server processes frames in lockstep order per stream.

### Crate dependencies added in M2

Workspace deps appended:
```toml
quinn = { version = "0.11", default-features = false, features = ["runtime-tokio", "rustls-ring"] }
rustls = { version = "0.23", default-features = false, features = ["ring", "tls12", "std"] }
rcgen = { version = "0.13", default-features = false, features = ["pem", "ring"] }
rustls-pemfile = "2"
```

Plus `crc32fast = "1"` direct dep on `uc_node` (already in the lockfile via `ultima_journal`; consistent codec choice).

### Per-node disk layout (M2 additions)

```
{node.data_dir}/
├── journal/                       # (M1) raft log
├── vote.state                     # (M1) StableValue<Vote>
├── committed.state                # (M1) StableValue<LogId> for committed
├── output_progress.state          # (M1) StableValue<u64> for at-least-once output
├── membership.state               # (M1) reserved (unused in M1; will land in M3)
├── last_purged.state              # (M1) StableValue<LogId> for last_purged
├── tls.crt                        # (M2) self-signed cert PEM
├── tls.key                        # (M2) self-signed PKCS#8 key PEM
├── last_applied.state             # (M2) StableValue<LogId> after install_snapshot
├── snapshot_meta.state            # (M2) StableValue<StoredSnapshotMeta>
└── snapshot_{index}.bin           # (M2) snapshot bytes per install
```

`StoredSnapshotMeta { last_log_id, last_membership, bytes_filename }` is a serde struct pointing at the snapshot bytes file. Together with `last_applied.state` it forms the durable record of an installed snapshot.

## Network layer

### Module map

```
uc_node/src/network/
├── mod.rs           # public re-exports + NetworkError enum
├── tls.rs           # self-signed cert generation + rustls configs
├── frame.rs         # Frame + MessageType + encode/decode + read_async (16 MiB cap)
├── codec.rs         # bincode encode/decode wrappers for 6 RPC body types
├── server.rs        # spawn_server + ServerHandle + per-stream RPC dispatch
├── client.rs        # PeerConn (one bi-stream per RPC, send.finish() signals EOF)
├── factory.rs       # QuicRaftNetworkFactory + PeerPool type alias
└── instance.rs      # QuicRaftNetwork (RaftNetwork impl + get_or_connect + evict)
```

### Wire framing

Each frame on a QUIC stream:

```
msg_type        u8     (MessageType enum)
flags           u8     (bit 0: is_response)
request_id      u64    (correlator; defensively verified)
body_len        u32    (length of body in bytes, capped at 16 MiB)
body            (variable)
body_crc32      u32    (CRC over body)
```

Header is 14 bytes fixed. CRC covers body only. `MAX_BODY_LEN = 16 MiB` bounds attacker-controlled allocations from forged headers.

### MessageType discriminants

```
AppendEntriesReq  = 1   AppendEntriesResp  = 2
VoteReq           = 3   VoteResp           = 4
InstallSnapshotReq= 5   InstallSnapshotResp= 6
Handshake         = 10  HandshakeAck       = 11
```

(`Handshake`/`HandshakeAck` are reserved for M3 cnc handshake on the IPC boundary; not used by the QUIC layer.)

### RPC codecs

All 6 openraft body types are bincode-encoded with `bincode::config::standard()`. Each gets an encode/decode wrapper (e.g., `encode_vote_req` / `decode_vote_req`). Errors map to `NetworkError::Decode`. No scatter-gather zero-copy in M2 — correctness first; deferred to post-M5 perf work.

### QUIC server

`spawn_server(listen_addr, server_cfg, raft) -> ServerHandle`:
- Binds `quinn::Endpoint::server` to `listen_addr`.
- Spawns an accept loop on a tokio task.
- Per inbound connection: spawn a per-connection task that loops `accept_bi()`.
- Per inbound bi-stream: spawn a per-stream task that reads `Frame`, calls `dispatch`, writes response frame, then loops. `Frame::read_async` returning `UnexpectedEof` cleanly ends the loop (this is how `send.finish()` from the client cleanly closes a stream after one RPC).
- `dispatch` decodes the body, calls `raft.append_entries(req)` / `raft.vote(req)` / `raft.install_snapshot(req)` on the local openraft instance, encodes the response, writes it.
- Non-request `msg_type` → `NetworkError::Decode`.
- `ConnectionError::{ApplicationClosed, ConnectionClosed, LocallyClosed}` end the per-connection loop quietly.

`ServerHandle::shutdown()` closes the endpoint and awaits the accept task.

### QUIC client

`PeerConn::request(msg_type, body, response_type, timeout)`:
- Opens a fresh bidirectional stream via `connection.open_bi()`.
- Writes the encoded request frame.
- Calls `send.finish()` to signal EOF — the server's next `Frame::read_async` returns `UnexpectedEof` and ends the per-stream task cleanly.
- Awaits the response frame on `recv` with the caller-provided `timeout` (typically `RPCOption::hard_ttl()`).
- Validates `msg_type` and `request_id` (defense-in-depth — with one-RPC-per-stream the request_id always matches).
- Returns `response.body`.

Each RPC pays one stream open/close. QUIC streams are cheap (no handshake; a small control frame), so this is acceptable for M2 timings (250ms heartbeat → ~12 streams/sec on a 3-node cluster). Persistent per-class streams is a post-M5 perf optimization.

### `RaftNetwork` implementation

`QuicRaftNetwork` stores `target: NodeId`, `peer_addr: SocketAddr`, shared `endpoint: quinn::Endpoint`, `client_cfg: Arc<rustls::ClientConfig>`, shared `pool: PeerPool`, and `app_id: String`. Each RPC method (`append_entries` / `vote` / `install_snapshot`):

1. Encode body via `codec`.
2. `get_or_connect()` — fast path returns the cached `PeerConn` for `target`; slow path connects through the shared endpoint and inserts into the pool.
3. `conn.request(msg_type, body, response_type, option.hard_ttl())`.
4. On Ok: decode the response body.
5. On Err: `evict(&conn)` — only removes the entry if `Arc::ptr_eq` matches (avoids removing a replacement connection from a concurrent caller). Then surface `RPCError::Network`; openraft retries on its own backoff.

`PeerPool = Arc<Mutex<HashMap<NodeId, PeerConn>>>`. The benign cache race ("two concurrent callers both miss and connect") is documented and harmless — loser's `PeerConn` drops cleanly because `quinn::Connection` is Arc-internal.

### TLS

`TlsConfig::SelfSigned` is the only M2 mode. On first start:
- `tls::generate_self_signed(app_id) -> (cert_pem, key_pem)` via rcgen. Cert SAN includes `app_id`, `ultima_cluster`, and `localhost`.
- Writes `tls.crt` and `tls.key` to `data_dir`.

On every start:
- `tls::load_or_init(data_dir, app_id)` reads existing files or generates fresh ones.
- `tls::build_server_config(cert, key)` produces `Arc<rustls::ServerConfig>` (no client auth).
- `tls::build_client_config()` produces `Arc<rustls::ClientConfig>` with an `AcceptAnything` `ServerCertVerifier` (because peers won't have each other's self-signed certs in any trust store).

The crypto provider is installed once per process via `OnceLock` calling `rustls::crypto::ring::default_provider().install_default()`.

## Storage adapter changes

### `get_log_state` real `node_id` recovery (Task 1)

M1's `get_log_state` synthesized `CommittedLeaderId::new(last_term, 0)` with `node_id=0`. The Task 10 M1 review claimed this was safe because openraft's default `leader_id_std` mode discards `node_id`. **That was wrong.** openraft 0.9.24's actual default is `leader_id_adv` (the `single-term-leader` feature is opt-in, OFF by default), and `LeaderId<NID> { term, node_id }` participates in lexicographic ordering during vote comparisons.

The fix: when `last_seq` is present, bincode-decode the entry at `last_seq` from the journal and use its real `LogId::leader_id`. Cost: one extra record decode at startup — negligible. Sanity-checked via `debug_assert_eq!` that the journal's `meta` (storing the term) matches the decoded entry's term.

### Durable `install_snapshot` (Task 2)

M1's `install_snapshot` only updated in-memory state. M2 makes it durable:

1. Write `snapshot_{idx}.bin` to disk and fsync.
2. Write `snapshot_meta.state` (`StoredSnapshotMeta { last_log_id, last_membership, bytes_filename }`) — this is the durable pointer to the snapshot bytes.
3. Write `last_applied.state` (the `LogId<NodeId>` represented by the snapshot).
4. Mutate the user's state machine (`sm.install_snapshot(reader)`).
5. Update the adapter's in-memory metadata.

**Ordering rationale** (post-final-review fix): `snapshot_meta_sv` is written BEFORE `last_applied_sv`. Crash analysis:
- Crash after bytes write, before snapshot_meta: meta is None on restart → no snapshot loaded → user SM stays default. last_applied is None. Consistent.
- Crash after snapshot_meta, before last_applied: meta points at bytes file on restart → bytes loaded → user SM at N (via `sm.install_snapshot`). `loaded_last_applied = sm.last_applied()` returns Some(N). Cross-check passes. Consistent.
- Crash after last_applied write: complete state. Consistent.

On startup recovery, `AdaptedStateMachine::new` loads `snapshot_meta.state`. If `Some`, reads the bytes file with **fail-hard** semantics (no `unwrap_or_default()`; a missing/unreadable bytes file surfaces as `ClusterError::Recovery`). The bytes are then replayed into the user's `sm.install_snapshot` — failure here propagates as `ClusterError::Recovery` (not the M1 `.expect()` panic).

### User vs framework `last_applied` cross-check (Task 3)

`AdaptedStateMachine::new` now returns `Result<Self, ClusterError>`. After loading durable state, it compares user's `sm.last_applied()` against the framework's `loaded_last_applied`:

| user_la | framework_la | Outcome |
|---|---|---|
| `None` | `None` | ok |
| `Some(N)` | `Some(N)` | ok |
| `Some(U)` | `Some(F)` where U≠F | `DriftDetected { user, framework }` |
| `Some(U)` | `None` | `DriftDetected { user, framework: None }` |
| `None` | `Some(F)`, `current_snapshot.is_some()` | warn-log, accept framework |
| `None` | `Some(F)`, `current_snapshot.is_none()` | `DriftDetected { user: None, framework }` |

The `(None, Some)` branch is permissive only when a snapshot was actually loaded — otherwise it's silent corruption (framework claims state at N but no snapshot exists to install).

`ClusterError::DriftDetected { user: Option<u64>, framework: Option<u64> }` is a new error variant.

## Bootstrap and membership

### `BootstrapConfig::Peers` (Task 11)

Min-id bootstrap pattern. Each peer in the `PeerSeed` list specifies its own node_id + raft_addr.

**Bootstrapper** (the peer with the lowest node_id):
1. `raft.initialize({self_id: NodeAddr})` — idempotent, swallows `InitializeError::NotAllowed` on restart.
2. For each non-self peer, `raft.add_learner(peer.id, peer.node, blocking=true)`. On Ok: insert peer.id into a `promotable` set. On Err: warn-log; do NOT abort.
3. `raft.change_membership(promotable, retain=false)` — promotes only peers that became learners. Avoids the "configured set has unreachable peers → change_membership rejected" trap.

**Non-bootstrappers** (other peers):
- Log info and idle. Their QUIC server is already up; they wait to be reached by the bootstrapper's `add_learner` RPC.

This is the standard split-brain-safe Raft bootstrap pattern. If two nodes with `Peers` config but different `min_id` start simultaneously, only one bootstraps; the other waits.

### `NodeHandle::add_learner / change_membership / remove_node` (Task 11)

```rust
pub async fn add_learner(&self, node_id: NodeId, raft_addr: SocketAddr) -> Result<(), ClusterError>;
pub async fn change_membership(&self, voters: BTreeSet<NodeId>) -> Result<(), ClusterError>;
pub async fn remove_node(&self, node_id: NodeId) -> Result<(), ClusterError>;
```

`remove_node` reads current voters from `raft.metrics().borrow().membership_config.voter_ids()`, drops the target, and calls `change_membership(remaining, retain=false)`.

## openraft 0.9.24 idiosyncrasies discovered in M2

Adding to M1's list:

| Surface | Reality |
|---|---|
| `RaftNetwork` trait | Same `#[add_async_trait]` pattern as `RaftLogStorage` (native async fn). `RPCError<NodeId, NodeAddr, RaftError<NodeId, E>>` is the error shape; `E` is variant-specific (`()` for vote/append, `InstallSnapshotError` for install_snapshot). |
| `RaftNetworkFactory::new_client` | Cannot return `Result` — lazy-connect-on-request is the workaround. |
| `Raft::vote / append_entries / install_snapshot` | Direct methods for server-side RPC dispatch. |
| `Raft::add_learner(id, node, blocking)` | `blocking=true` waits for the learner's log to catch up before returning. |
| `Raft::change_membership(into_changemembers, retain)` | `BTreeSet<NodeId>` impls `Into<ChangeMembers<...>>`. `retain=false` means dropped nodes are removed (not retained as learners). |
| `Raft::metrics()` | Returns `tokio::sync::watch::Receiver<RaftMetrics<...>>`. `.borrow()` is sync; drop before any `.await`. `metrics.membership_config.voter_ids()` yields owned `NodeId` (not `&NodeId`). |
| `RaftError::APIError(InitializeError::NotAllowed(_))` | Pattern-match this variant to swallow re-initialization on restart. |
| `RaftError::APIError(ClientWriteError::ForwardToLeader(f))` | `f.leader_id: Option<NodeId>` plus `f.leader_node: Option<Node>`. |
| `LeaderId` default | `leader_id_adv` (NOT `leader_id_std`) — node_id IS load-bearing for vote comparisons. The M1 Task 10 review's claim about `leader_id_std` being the default was incorrect; `single-term-leader` is opt-in. |
| `RPCOption::hard_ttl()` | Returns `Duration` (not `Option<Duration>`). |
| `SnapshotMeta::signature()` | Available; used in `StorageIOError::read_snapshot(Some(meta.signature()), ...)`. |

## quinn 0.11 / rustls 0.23 / rcgen 0.13 idiosyncrasies

| Surface | Reality |
|---|---|
| `quinn::Endpoint::connect_with(config, addr, server_name)` | The right method for per-call config; `connect` uses the endpoint's default. Stable across 0.11.x. |
| `quinn::SendStream::finish() -> Result<(), ClosedStream>` | Synchronous (not async). Signals EOF; subsequent `read_async` on the peer's recv yields `UnexpectedEof`. |
| `quinn::SendStream::write_all` | Inherent method on quinn 0.11 — no `AsyncWriteExt` import needed. |
| `quinn::TransportConfig::max_idle_timeout(Some(IdleTimeout))` | `TryFrom<Duration> for IdleTimeout` exists; 30s is well within range. |
| `quinn::crypto::rustls::QuicServerConfig::try_from(rustls::ServerConfig)` | Adapter for rustls→quinn TLS. Same for `QuicClientConfig::try_from(rustls::ClientConfig)`. |
| `rustls::ClientConfig::builder().dangerous().with_custom_certificate_verifier(...)` | The `dangerous()` builder is mandatory for `AcceptAnything`-style verifiers. |
| `rustls::crypto::CryptoProvider::install_default(ring::default_provider())` | Required once per process before building configs. M2 uses `OnceLock`; ignore the duplicate-install error. |
| `rcgen::CertificateParams::new(Vec<String>)` | SANs as DNS-name strings. `KeyPair::generate() -> Result<KeyPair, Error>`. `params.self_signed(&key_pair) -> Result<Certificate, Error>`. `cert.pem() -> String`. |
| `rustls_pemfile::pkcs8_private_keys(reader)` | Iterator of `Result<PrivatePkcs8KeyDer, _>`. Wrap with `PrivateKeyDer::Pkcs8(...)`. |

## Tests

29 tests in workspace (was 11 at M1 merge; +18 in M2):

| File | Tests | Notes |
|---|---|---|
| `uc_protocol::version::tests` | 4 | (M1) pack roundtrip, compat cases |
| `uc_node tls::tests` | 3 | (M2) cert gen, init creates files, idempotent reload |
| `uc_node tests/drift_detection.rs` | 2 | (M2) drift detected for (Some, Some) unequal + (Some, None) |
| `uc_node tests/log_storage_open.rs` | 6 | (M1+M2) reopen empty, vote round-trip, committed round-trip, append+read, purge_retains_higher_indices, snapshot_meta_survives_reopen |
| `uc_node tests/frame_roundtrip.rs` | 7 | (M2) encode/decode empty + with body + corrupted CRC + unknown msg_type + read_async + oversized body_len + vote_req_roundtrip |
| `uc_node tests/m1_single_node.rs` | 2 | (M1) single-node submit/query + restart-with-state (now runs over real QUIC) |
| `uc_node tests/m2_multi_node.rs` | 5 | **(M2 capstone)** 3-node election + replication + leader_failover + snapshot_install_on_new_follower + membership_change_remove_node |

The 5 capstone multi-node tests exercise the full QUIC stack: TLS handshake, frame encode/decode, RPC dispatch, member discovery, vote, append, install_snapshot.

## Bugs caught during the two-stage subagent review

Seven real bugs caught and fixed during review (would have shipped otherwise):

1. **Task 1 — `get_log_state` synthetic node_id was wrong** (Critical). Audit revealed the openraft default is `leader_id_adv`, not `leader_id_std`. M1's `node_id=0` synthesis would have caused vote comparison bugs in M2 multi-node.
2. **Task 2 — `install_snapshot` mutated user-sm before durable writes** (Critical). A disk write failure would leave adapter metadata and user-sm in inconsistent states. Reordered.
3. **Task 2 — Startup snapshot replay silently swallowed errors** (Critical). `let _ = sm.install_snapshot(...)` would silently fail; framework would report state at N while user SM was at default. Replaced with `?` propagation via `ClusterError::Recovery`.
4. **Task 5 — Frame `body_len` unbounded** (Important). u32 from wire → 4 GiB allocation DoS. Added 16 MiB cap.
5. **Task 9 — `evict()` could remove a healthy replacement connection** (Important). Concurrent failures + reconnects could evict the wrong one. Fixed via `Arc::ptr_eq` compare-and-evict.
6. **Task 9 — `RPCOption::hard_ttl()` ignored** (Important). PeerConn used a hardcoded 10s timeout. Plumbed through.
7. **Task 17 (final review) — `install_snapshot` crash-window** (Important). Write order was `last_applied_sv` before `snapshot_meta_sv` — a crash between them left disk inconsistent. Plus `unwrap_or_default()` on the bytes file silently substituted `[]` on read failure. Plus the `(None, Some)` cross-check branch was too permissive. Fixed: reorder writes (meta first), fail-hard on bytes-file read, gate the permissive cross-check branch on `current_snapshot.is_some()`.

## Notable design decisions

- **Approach A for client streams** (one bi-stream per RPC) vs. a multiplexed-single-stream alternative. Forced by Task 7's lockstep server. QUIC stream open/close is cheap; persistent per-class streams is a deferred perf optimization. Documented inline.
- **Shared client `Endpoint`** (one UDP socket per process, not per peer). Forced by quinn idiom. `PeerConn::connect` takes `&Endpoint` as an arg; the factory holds it.
- **Lazy-connect-on-request** because `RaftNetworkFactory::new_client` can't return `Result`. Each RPC method calls `get_or_connect` first.
- **Min-id bootstrap with partial-failure tolerance**: only peers that successfully became learners are promoted to voters in the final `change_membership` call. Avoids the "configured set has unreachable peers → change_membership rejected" trap.
- **Lockstep server dispatch** (read → call raft → write → loop). Per-stream sequential. Simple to reason about; the per-RPC stream model from the client makes this exactly the right shape.
- **Write `snapshot_meta_sv` before `last_applied_sv`** in install_snapshot. Crash analysis above explains why this ordering preserves consistency.

## Follow-ups tracked for later milestones

- **M3 follow-up**: `query_snapshot<F: FnOnce(&S) -> R>` API surface in `NodeHandle` will change when M3 introduces the shmem service split (the closure can't cross the IPC boundary). M2 callers writing tests against this signature will need migration.
- **M3 follow-up**: `AdaptedStateMachine`'s `Arc<tokio::Mutex<Inner<S>>>` disappears in favor of "publish to apply ring → consume response from apply_resp ring."
- **M5 hardening**: replace `AcceptAnything` `ServerCertVerifier` with real CA validation. Add `TlsConfig::Files` option. Operator-rotation of TLS keys.
- **M5 cleanup**: orphan snapshot bytes files. `install_snapshot` writes `snapshot_{idx}.bin` but never deletes prior files. `TODO(M5)` comment in source.
- **Test gap**: `membership_change_remove_node` deliberately removes a non-leader. The leader-removal path triggers a leadership transfer first — untested in M2.
- **Test gap**: cold-restart of a node that has a persisted snapshot. Exercises the I1 fix path (snapshot_meta before last_applied).
- **Documentation gap**: `uc_service::StateMachine::last_applied()` contract — the framework expects the user SM to durably persist this value. Counter test SM doesn't (in-memory only); fine for tests since openraft re-applies committed entries through `apply()` on restart, rebuilding state.
- **Apply path doesn't durably persist `last_applied_sv`** — only `install_snapshot` does. After many normal applies + crash, framework's persisted value is stale. The cross-check would flag this as `DriftDetected` if the user SM reports `last_applied = Some(N)` and framework reports `None`. Currently relies on openraft's replay-through-apply to bring the user SM back up.
- **`app_id` validation**: `NodeConfig::validate` checks only length + non-emptiness. `rcgen` treats `app_id` as a DNS-label SAN, so a value with non-DNS characters fails cert generation with `NetworkError::Cert(...)` instead of `ClusterError::Config(...)`. Tighten validation.
- **Server-side stream-task noise on client timeout**: a long `raft.append_entries` followed by a client timeout would warn-log on the server. Cosmetic.

## Build/test/lint

```bash
cargo build --workspace
cargo test --workspace        # 29 tests
cargo clippy --workspace --all-targets -- -D warnings     # zero warnings
cargo doc --workspace --no-deps                           # builds; 6 doc warnings (intra-doc links, code-block markers)
```

## Pointers

- Canonical design: `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md` (Section 7: Network layer; covers M1-M5).
- M1 record: `docs/tasks/task01_m1_embedded_single_node.md`.
- openraft 0.9.24 source (cargo registry cache) — `RaftNetwork` / `RaftLogStorage` / `RaftStateMachine` trait shapes; `leader_id_adv.rs` for the default leader_id mode.
- quinn 0.11 / rustls 0.23 / rcgen 0.13 docs.
