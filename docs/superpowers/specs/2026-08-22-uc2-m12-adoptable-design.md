# UC v2 — M12 "Adoptable cluster": design

*Umbrella design for the last milestone of the production-readiness arc
(`2026-08-19-uc2-production-readiness-design.md` §7). M12 is decomposed here
into four sub-milestones, each with its own implementation plan; this
document fixes the cross-cutting decisions and the design of each
sub-milestone so the plans do not re-litigate them. Status: design approved
in conversation 2026-08-22; written spec pending user review.*

## 0. Why M12, and what "adoptable" means

M9 produced a process that starts and stops; M10 let *you* operate it; M11 let
an *ops team* operate it. M12 is for **a stranger**: someone who is not the
author downloads a release, runs three nodes on three hosts, points their own
state machine and their own remote clients at it, and operates it — without a
Rust toolchain and without reading the source. The four things standing in
the way are, in the order they bite an adopter:

1. **Clients must be co-located with a node** (the only client ingress is the
   shmem ring; writes on a follower return `NotLeader{hint}` the caller
   cannot act on across hosts).
2. **The admin plane has no access control** (anyone who can write the
   instance directory can remove voters; no audit record) and the deployment
   makes two security choices (`[crypto]` absent = cleartext) silently.
3. **There are no release artifacts** (no `release.yml`, no image, no
   signatures) and **no version identity** (workspace `0.1.0`, tags `v2.5.0`,
   nothing published, no stated API promise).
4. **The security posture is disclosed only in `VERIFICATION.md` §10**, and
   the decoders that parse untrusted bytes are not fuzzed.

## 1. Decisions

| Decision | Choice |
|---|---|
| Structure | **Umbrella spec, four sub-milestones, gateway first**: M12a gateway kit + state-machine contract → M12b admin authn/audit + explicit-choice config → M12c packaging/publishing/hygiene → M12d posture + review-ready package. Each merges to `main` as it lands; **one tag, `v2.6.0`, when all four are done.** |
| Gateway shape | **C — a gateway kit**: a library over `uc2_client::Engine` (leader discovery, redirect, remote flow control, exactly-once envelope) plus a thin reference binary. Not a fixed daemon with a UC-owned protocol (B), not "every user rebuilds it" (A). **D — remote ingress in the node itself (what Aeron Cluster does)** is the better end-state but is consensus-agent work with its own client-auth story; it gets its own spec later, and C's library is written transport-agnostic so it becomes D's client SDK. |
| Non-leader write | **Redirect**, never forward. Edge stays stateless; clients reconnect. A forwarding mode is a documented seam, not a deliverable. |
| Exactly-once across the remote hop | **Session-ordered protocol + FSM-side dedup adapter** (`Sessioned<S>`, the Raft-paper client-session model): the only scheme sound across edge crashes and failover. Raw pass-through (Aeron parity) stays available. |
| Remote flow control | **Receiver-driven explicit credits** (Aeron Status-Message model): `HELLO_OK{credits}`, credit piggybacked on every `RESPONSE`, standalone `STATUS` when idle or when credits reopen. TCP push-back is the backstop only. `RETRY` is a state signal with a `retry_after` hint, never a load signal. |
| Reference protocol | **Framed TCP, pipelined, opaque payload**, in `uc2_remote`. No HTTP shim, no gRPC. Plain TCP; posture stated (operator's TLS terminator if needed). |
| State-machine contract | **Two tiers, user's choice**: a new raw bytes-in/bytes-out `RawStateMachine` core and today's typed serde/bincode `StateMachine` as a blanket adapter on top. Decided by the 2026-08-22 codec spike (`docs/notes/2026-08-22-codec-budget-spike.md`): with `Vec<u8>`-typed commands the apply thread is decode-bound (56–85 % of its cycles); the format is not the cost (SBE == raw); the typing and the per-frame allocation are. No SBE dependency in UC. |
| Admin credential | **Named HMAC-SHA256 keys in `[admin]`**; request tag in a new 64-byte cnc line inside the reserved band; `audit.jsonl` under the instance dir. `auth = "none"` is the deliberate legacy opt-out. |
| Explicit-choice config | `[crypto]` and `[admin]` go from absent-means-off to **absent-means-refuse-to-start**. Stated upgrade consequence. |
| Crypto default | **Stays OFF**; the operator must write `enabled = false` to get it. Flipping blind without the M8 fleet ratio is premature. |
| Versioning | **Lockstep**: `workspace.package.version = "2.6.0"` = tag = every crate; public crates published to crates.io; `rust-version` stated; toolchain pinned. Consequence accepted: a breaking change to the promised surface is `3.0.0`. |
| Packaging | **GitHub Releases (x86_64 + aarch64 linux-gnu, `SHA256SUMS`, SBOM) + `ghcr.io` image, both signed with cosign keyless (GitHub OIDC).** A `release-smoke` CI job proves the no-toolchain quickstart from the fresh artifacts before anything is published. |
| External security review | **Review-ready package + self-assessment is the deliverable**; the external review is a separate, user-scheduled step (like fleet gates). |
| `cargo fmt` | One-shot + `--check` gate in M12c **only if no long-lived branch is open then**; otherwise deferred and said so. |
| Bars | **Fleet-only.** Local runs are smoke. Never move a bar because a dev-box run went red. |

## 2. Inherited non-goals (production-readiness §3, restated)

No consensus change; no wire-protocol change; no cnc layout change outside
the reserved band (`3904..4096`); no mixed-version operation; no
leadership-transfer protocol; no sharding; no dynamic loading of state
machines; **no new remote-admin surface** — the gateway carries no admin
operations, admin stays local through the cnc slot, now authenticated; no
weakening of any gate.

Added for M12: **no remote ingress inside the node** (shape D) and **no TLS
on the remote client link** in this release.

## 3. Cross-cutting decisions in detail

### 3.1 The state-machine contract: two tiers

> **Amendment (Task 13, as-built).** The plan below sketches a `Typed<S>`
> wrapper (`impl<S: StateMachine> RawStateMachine for Typed<S>`). What
> shipped is a **blanket impl directly on `S`** —
> `impl<S: StateMachine> RawStateMachine for S`, `uc2_service/src/traits.rs`
> — with no `Typed<S>` newtype at all: a typed `StateMachine` simply *is* a
> `RawStateMachine`, not something wrapped into one. `ServiceBuilder::new`
> therefore accepts either tier directly (no separate `::raw` constructor);
> `.output_handler(typed)` installs a typed `OutputHandler` through the
> `TypedOutput<O>` adapter, `.raw_output_handler(raw)` installs a
> `RawOutputHandler` directly. Everything else below (the codec, the
> byte-identity promise, the measured cost split) shipped as written. See
> `docs/reference/state-machine-contract.md` for the as-built reference.

Today `uc2_service::StateMachine` is typed (`Command/Response/Query/
QueryResponse: Serialize + DeserializeOwned`) and the framework does one
bincode-standard decode per command at the apply boundary
(`uc2_service/src/apply.rs`, also `replay.rs`, `output.rs`) and one encode
per response (`egress.rs`). The transport is bytes end-to-end; only the
trait forces the codec. The spike measured that cost at 56–85 % of the apply
thread's cycles for `Vec<u8>`-typed commands (~1.5 ns/payload byte), dropping
to 15–21 % with `bytes::Bytes` fields (byte-identical wire); SBE measured
equal to a hand-laid raw frame. Hence:

```rust
/// Core contract. The framework passes the frame payload straight through
/// and reuses `out` across calls: zero decode, zero allocation in steady state.
pub trait RawStateMachine: Send + 'static {
    fn apply(&mut self, position: u64, cmd: &[u8], out: &mut Vec<u8>);
    fn query(&self, q: &[u8], out: &mut Vec<u8>);
    fn last_applied(&self) -> Option<u64>;
}

/// Today's typed trait, signature unchanged. Blanket-adapted directly onto
/// the core trait (as-built: no `Typed<S>` wrapper — see the amendment
/// above) — does exactly today's bincode-standard encode/decode.
pub trait StateMachine: Send + 'static { /* as today */ }
impl<S: StateMachine> RawStateMachine for S { /* bincode standard */ }
```

- `SnapshotStateMachine` is already byte streams; it gains a `Raw` bound
  variant only if the compiler needs it (plan decides).
- `ServiceBuilder::new(cfg, sm)` accepts either tier directly (`S:
  RawStateMachine`; a typed `sm` satisfies this through the blanket impl
  above, so there is no separate `::raw` constructor — as-built, see the
  amendment above). Every existing state machine (`counter`,
  `RegisterSm`, `CountSm`, the `ultima_db` store adapter) compiles unchanged;
  a **wire byte-identity test** (old `StateMachine` encode == `Typed<S>`
  encode for the same value) guards `2.5.0` clients.
- `egress.publish` stops allocating per response in both tiers (reused
  buffer; `position ++ bytes` written into the ring directly).
- Docs: typed users type blobs as `bytes::Bytes`/`serde_bytes`; raw users may
  use SBE/flatbuffers/hand-laid frames directly — UC takes no such dependency.
- The client tiers mirror this already (`Engine::try_submit(&[u8])` is raw;
  `PipelinedClient`/`Client` are typed).
- The `apply-profile` feature (uc2_service, off by default, zero-cost when
  off) stays as the measuring tool; the M12a fleet gate re-runs the M5 ladder
  with it on, for a raw `CountSm` and the typed one, and publishes the codec
  share.

### 3.2 Version identity and the semver promise

`workspace.package.version = "2.6.0"`; every internal path dependency gains
`version = "2.6.0"` (publish requires it); `ultima_journal` joins the
workspace version; `uc2ctl` drops `publish = false`; `examples/*` and
`uc-lincheck` stay unpublished. All eleven names are free on crates.io
(checked 2026-08-22). `rust-version` = the lowest stable that passes (≥ 1.88,
`ultima-db 0.1.1`'s floor; edition 2024 needs ≥ 1.85); `rust-toolchain.toml`
pins that stable; CI gains an `msrv` job.

`docs/reference/semver-policy.md` names the promised surface:
`RawStateMachine`, `StateMachine`, `SnapshotStateMachine`, `OutputHandler`,
`Sessioned`, `NodeConfig` + `node.toml`, `gateway.toml`, the three client
tiers, the `uc2_remote` protocol (its own `version` field, v1), `uc2ctl`
verbs and exit codes. Everything else is `#[doc(hidden)]` or documented as
internal. Wire protocol (`version::CURRENT`) and the cnc page version stay
flag-day (policy unchanged, restated in the same doc). A breaking change to
the promised surface is `3.0.0`.

### 3.3 Explicit-choice configuration

`[crypto]` gains a required `enabled: bool`; `[admin]` is new with a required
`auth`. **Absent section = named startup refusal** (`ConfigError::
CryptoChoiceRequired`, `AdminChoiceRequired`). `packaging/node.example.toml`
ships both sections uncommented with the posture comment. Upgrade
consequence, stated in `upgrade-a-cluster.md` alongside M11's 78 MiB
reservation: a `node.toml` from M9–M11 refuses to start until the two
choices are written down. Test fixtures and gates choose explicitly.

## 4. M12a — Gateway kit and the state-machine contract

> **As built (M12a).** M12a shipped as Tasks 1–12 on `uc2/m12-adoptable`;
> `docs/benchmarks/uc2-m12-gate-2026-08-22.md` is the acceptance-gate record
> (every §8 row, local smoke numbers, the facts a fleet re-run must state).
> The amendments below this line correct this section's sketch against what
> actually shipped; §4.1–§4.6's prose is otherwise as written.

### 4.1 Crates

- **`uc2_remote`** — the remote wire protocol (codec, frame types, constants)
  and the Rust remote client (`RemoteClient`: connect, pipelined submit/query,
  credit accounting, redirect following, re-send after failover). Deps:
  `bytes`, `serde` (for typed convenience only), std TCP. No shmem stack —
  this is what a polyglot port re-implements.
- **`uc2_gateway`** — the edge library (`Edge`) over `uc2_client::Engine`,
  plus `src/bin/uc2-gateway.rs`, the reference binary driven by
  `gateway.toml`.
- **`uc2_service::session`** — `Sessioned<S>` (no new dependencies).

### 4.2 Remote protocol (`uc2_remote`, protocol v1)

Frame: `u32 len | u8 type | u8 flags | u16 version | u64 client_id | u64 seq
| payload`. Payload is **opaque command/query/response bytes** — the gateway
never interprets them (the raw tier makes this literally true).

| Type | Direction | Meaning |
|---|---|---|
| `HELLO` / `HELLO_OK` | c→e / e→c | protocol version, `app_id` check, client's asserted `client_id`; `HELLO_OK{credits, leader_node_id, leader_addr}` |
| `SUBMIT` | c→e | a write; `seq` monotonic per client |
| `QUERY` | c→e | flag `linearizable` or `snapshot`; served locally on any member |
| `RESPONSE` | e→c | flags `FLAG_IS_QUERY`/`FLAG_REPLAYED`/`FLAG_EXPIRED`/`FLAG_ENVELOPED` (`FLAG_REPLAYED`/`FLAG_EXPIRED` lifted off the `Sessioned` 1-byte tag); carries `credits` |
| `STATUS{acked_seq, credits}` | e→c | standalone credit/liveness frame: on a timer when idle, immediately when credits reopen |
| `REDIRECT{leader_node_id, addr}` | e→c | this edge's node is not serving writes; go there |
| `RETRY{reason, retry_after_us}` | e→c | state signal: `RETRY_NOT_SERVING=1` (no leader hint yet), `RETRY_INSTANCE_RESTART=2` (reserved; the reference edge signals this via `LEADER_CHANGED{unknown}`+close instead), `RETRY_SERVICE_UNAVAILABLE=3` (the local `Engine` backpressured past the request's timeout), `RETRY_PAYLOAD_TOO_LARGE=4` (terminal — the client must not resend) |
| `UNKNOWN` | e→c | the edge's `Engine` timed the slot out: may or may not have committed |
| `LEADER_CHANGED{leader_node_id, addr}` | e→c | pushed to every connection on a leader-watch transition |
| `PING` / `PONG` | c→e / e→c | client liveness: the client pings when idle past `ping_interval` (default 1 s); it declares the connection dead and fails over after `dead_after` (default 3 s) with nothing received at all |

`HELLO_REFUSED` carries `HELLO_REFUSED_APP_ID=1`, `HELLO_REFUSED_VERSION=2`
(both the client's problem — every member answers the same way), or
`HELLO_REFUSED_FAULTED=3` (the edge's own problem: its node's shmem instance
restarted underneath it and it will never serve again — try a different
member).

`client_id` is a client-chosen random `u64`, stable for the client's
lifetime (persisted by the client if it wants dedup to survive its own
restart); `seq` starts at 1 and is monotonic per client.

**Flow control (receiver-driven credits).** The client may have at most
`credits` unanswered `seq`s beyond `acked_seq`. The edge sizes credits from
the `Engine` inflight window it has left, shared across its connections, and
shrinks them when `Engine` reports `Backpressure` — pressure is signalled
before frames leave the client. A client that ignores credits is stopped by
the edge ceasing to read its socket (TCP window closes): the backstop, not
the mechanism. No frame is ever accepted and then bounced for capacity.

### 4.3 The edge (`uc2_gateway::Edge`)

- One `Engine` per edge process: a `SendHalf` per acceptor thread, one
  `PollHalf` driver thread. Connection table keyed by `user_data =
  conn_idx << 32 | local_corr`.
- **Leader watch**: polls `SendHalf::can_serve()` and `leader_hint()` (a
  cluster `NodeId`) off the cnc page; on transition pushes `LEADER_CHANGED` to
  every connection. The node-id→address map is **static, in `gateway.toml`**
  (`[[members]] node_id = 1, gateway = "host1:9100"`) — the cnc page carries
  ids and roles but no addresses, and a static member list is Aeron's
  `ingressEndpoints` string; no node or cnc change.
- **Redirect**: `SUBMIT` while `!can_serve` → `REDIRECT` to
  `members[leader_hint]`, or `RETRY{not_serving}` if the hint is unknown.
  Queries are answered locally regardless of role (snapshot from the local
  replica; linearizable through the local node's read barrier).
- **Envelope** (`session_envelope = true`, default): the edge prepends a fixed
  16-byte LE header `client_id, seq` to the opaque command bytes and lifts the
  1-byte `Sessioned` tag off the response into `RESPONSE` flags. `false` =
  raw pass-through (Aeron parity; dedup is the application's).
- Error handling: `app_id` mismatch → `HELLO` refused; payload > the node's
  `max_payload` → refused before touching the ring; `Backpressure` → credits
  shrink (no message); `InstanceRestart` → all connections get
  `LEADER_CHANGED{unknown}` and are closed (clients reconnect, re-`HELLO`);
  edge death → clients reconnect per the member map. The edge holds no
  durable state.
- **As built: the per-connection not-serving latch.** A connection that is
  told once — `REDIRECT` or `RETRY{not_serving}` — that this node cannot
  take writes is told the same thing for every later `SUBMIT` on that
  connection, even if the node starts serving a microsecond later
  (`Conn::latch_not_serving`). Invariant: **the set of `SUBMIT`s a
  connection gets accepted is always a prefix of what it sent.** Without
  this, `Sessioned`'s FRESH/REPLAYED/EXPIRED classification breaks — a
  write accepted after an earlier one on the same connection was refused
  would leave a gap the dedup table cannot classify.
- **As built: the faulted-exit contract.** Once `InstanceRestart` fires, the
  edge latches `faulted` permanently — new `HELLO`s get
  `HELLO_REFUSED_FAULTED` — and the `uc2-gateway` daemon polls `is_faulted`
  and exits `1`, so `Restart=on-failure` brings up a fresh gateway against
  the new node instance rather than serve a permanently faulted edge
  forever.
- `gateway.toml`: `[local] instance_dir, app_id, listen`, `[[members]]`,
  `session_envelope`, `max_inflight`, `status_interval_ms`. Named startup
  refusals like `uc2-node`. `packaging/systemd/uc2-gateway.service` and
  `gateway.example.toml`.

### 4.4 `Sessioned<S>` — exactly-once at the raw layer

`impl<S: RawStateMachine> RawStateMachine for Sessioned<S>`:
- `apply`: peel the 16-byte header; if `seq` ≤ client's highest applied and
  inside the window → write a 1-byte `TAG_REPLAYED=1` ‖ cached response; if
  older than the window → `TAG_EXPIRED=2` alone (no bytes follow); else
  delegate, cache `(seq → response)`, write `TAG_FRESH=0` ‖ response. Per
  client: highest applied `seq` + a bounded window of responses (**as
  built**: `SessionConfig { window: usize (default 4096), max_clients: usize
  (default 65536), max_bytes: usize (default 256 MiB) }` — not tied to
  `max_inflight`); clients are evicted deterministically by
  `(last_seen_pos, client_id)`, oldest first, on either budget. **As built:
  `SessionConfig` is part of the replicated contract** — every replica must
  run identical values, and `install_snapshot` refuses a snapshot whose
  embedded config disagrees with the live node's rather than silently
  retuning it. A `seq` gap (client skipped numbers) is applied as fresh —
  ordering within a session is the protocol's job.
- `query` and `last_applied` delegate.
- Snapshot: `SnapshotHandle = (dedup blob, S::SnapshotHandle)`;
  `stream_snapshot` writes a length-prefixed dedup blob (config + table) then
  delegates; `install_snapshot` reads it off the same `src`, refuses on a
  config mismatch, then delegates — one artifact, one position tag.
- Works over a typed `StateMachine` too (via the blanket impl onto
  `RawStateMachine`, §3.1's amendment), so typed users get it by wrapping —
  no separate typed `Sessioned` exists.

### 4.5 Failover promises (what `RemoteClient` guarantees)

Every `SUBMIT` ends in exactly one of `RESPONSE` / `REDIRECT` / `RETRY` /
`UNKNOWN` / connection loss. The SDK follows `REDIRECT` and `LEADER_CHANGED`
(reconnect, re-`HELLO`, credits reset), re-sends unanswered `seq`s in order.
With the envelope on, a re-send is safe by construction (`fresh` /
`replayed` / `expired` are all well-defined; `expired` surfaces as a hard
"outcome unknowable" error). With it off, re-sent writes are reported as
"possibly duplicated". `UNKNOWN` is resolved by the re-send. Nothing queues
durably anywhere.

**As built, two mechanisms not in the original sketch:**
- **Probe-before-flush.** A freshly (re)connected client does not flush its
  whole pending window immediately — it writes exactly one request and
  waits for proof the far end will actually serve (a `RESPONSE`, or a
  `STATUS` whose `acked_seq` covers it) before releasing the rest. Also
  acts on `HELLO_OK`'s named leader before flushing, so a pipelined window
  is flushed at the real leader rather than redirected frame by frame. See
  `docs/notes/uc2-gateway-shapes-and-flow-control.md` for why this mattered.
- **`PING`/`PONG` liveness.** `RemoteConfig::ping_interval` (default 1 s)
  sends a `PING` when nothing has been written for that long;
  `RemoteConfig::dead_after` (default 3 s, must exceed `ping_interval` —
  not validated today) declares the connection dead and fails over when
  nothing at all has been received in that window.

### 4.6 Tests and gate

1. **Remote lincheck capstone** (CI, hard-crash style): 3 nodes + 3 edges,
   `RemoteClient`s pipelining through `uc2_remote`, leader SIGKILLed
   repeatedly; `uc-lincheck` asserts linearizable and **zero acked writes
   lost**; run with the envelope on (default) and off.
2. `Sessioned` unit/property tests: replay, expiry, gap, LRU eviction,
   snapshot round-trip of the dedup table, position tag.
3. Credit accounting tests (client never exceeds credits; edge shrinks under
   `Backpressure`; standalone `STATUS` reopens); redirect + leader watch
   under a forced election.
4. Typed-adapter wire byte-identity test; all existing suites unchanged.
5. **Codec share** (`m5_gate` with `apply-profile` on, raw and typed
   `CountSm`) and **gateway cost vs direct `Engine`** (a new `m12_gate`
   example): local runs are smoke; the fleet run states the numbers; proposed
   bar ≥ 0.8× direct-`Engine` throughput at equal inflight, finalised in the
   plan with its rationale.

## 5. M12b — Admin authentication, audit, explicit-choice config

> **As built (M12b).** M12b shipped as Tasks 1–7 on `uc2/m12b-admin-auth`
> (HEAD `cca681d`); `docs/benchmarks/uc2-m12-gate-2026-08-22.md` row 4 and
> its "M12b facts" section are the acceptance-gate record. The amendments
> below correct this section's sketch against what actually shipped;
> §5.1/§5.3/§5.4's prose is otherwise as written. Names below match code
> exactly: `AdminAuth`, `AdminKey`, `AdminPolicy`, `AdminMessage`, `sign`/
> `verify` (`uc2_crypto::admin`); `StartOpts`, `Node::start_with`,
> `REASON_AUTH_*`/`REASON_AUDIT_FAILED`, `verify_admin`, `handle_admin`
> (`uc2_node::node`); `AuditLog`/`AuditRecord`/`AuditOutcome`/`AuditOrigin`
> (`uc2_node::audit`); `AdminSection`/`AdminKeyEntry`/`AdminAuthMode`,
> `ConfigError::{CryptoChoiceRequired, AdminChoiceRequired}`
> (`uc2_node::config_file`).
>
> **§5.2 deviation, ruled: no `(seq, nonce)` replay ring.** The sketch below
> proposed one; the shipped design refuses it on the same grounds without
> one. The 64-byte auth line's tag already covers `seq`
> (`AdminMessage::canonical_bytes`), and the consensus agent's
> `handle_admin` only ever reads the admin-req slot when its `seq` is
> greater than `last_admin_seq` (`read_admin_req(self.last_admin_seq)`), so
> a captured request can never be re-presented at its original `seq` — it
> is never even read a second time — and re-presenting it at a higher `seq`
> changes the canonical bytes the tag was computed over, which fails
> `verify`. A node restart resets `last_admin_seq` to 0, which looks like it
> reopens the window, but the tag also covers `instance_id`
> (`CncPage::meta().instance_id`), which is re-randomized on every restart
> — so a pre-restart capture cannot be replayed post-restart either. A ring
> would therefore never refuse anything these two checks (the seqlock
> cursor, plus `instance_id` in the signed bytes) do not already refuse;
> what remains genuinely unbounded without a ring is the *delay* window for
> a live, correctly-sequenced, never-yet-applied request, and `expiry_ns`
> (checked against `now` with a `2 * ttl` upper bound too, so a
> forward-dated expiry cannot extend the window either) is what bounds
> that. Test: `uc2_node/tests/admin_auth.rs::a_replayed_request_cannot_be_re_presented`
> captures a signed request's exact bytes, replays them verbatim after a
> subsequent request has advanced `seq`, and asserts no second effect and
> no second audit record.
>
> **§5.2 reason codes, as shipped** (`uc2_node::node`, disjoint from
> `uc2_consensus::config::ProposeError`'s 1–10/12 and the node's own
> `REASON_MALFORMED_OP = 11`): `REASON_AUTH_MISSING = 20` (the auth line was
> all-zero — an unsigned request against an `Hmac` policy), `REASON_AUTH_BAD_TAG
> = 21` (a known key, but the HMAC does not verify — wrong key, tampering,
> or a stale auth line uc2ctl failed to clear), `REASON_AUTH_EXPIRED = 22`
> (`expiry_ns <= now`, or `expiry_ns > now + 2 * ttl`), `REASON_AUTH_UNKNOWN_KEY
> = 23` (`key_name_hash` matches no loaded key). The sketch's
> `auth_replay`/`auth_unconfigured` names do not exist — replay is refused
> by the no-ring argument above (folding into `auth_bad_tag`/`auth_missing`
> depending on what changed), and there is no "unconfigured" case distinct
> from `Filesystem` (which reads the auth line at all only under `Hmac`).
> One further code the sketch did not anticipate: `REASON_AUDIT_FAILED = 24`
> — the audit record could not be written (a full or failing disk), so the
> request is refused rather than answered unrecorded, even on the accepted
> path where the config change may already be appended (`uc2_node::audit`'s
> module doc: "accepted" means proposed and appended, not committed;
> "recorded" means "the record reached disk before the answer did," and 24
> is what happens when it cannot).
>
> **§5.2, verify-first on leader AND follower; the kind-16 residual.**
> `handle_admin` calls `verify_admin` before any role check — a follower
> never forwards an unauthenticated proposal, matching the sketch. What the
> sketch's "the peer plane's trust is whatever `[crypto]` says, as today"
> undersold: the forwarded `ConfigProposal` (kind 16) rides that peer plane
> by wire, not the admin band, so the leader cannot re-verify the
> operator's HMAC tag against it — it can only attest to which peer
> forwarded it (`peer:<id>`, from `addr_to_id`). `on_config_proposal`'s
> membership guard drops a kind-16 datagram whose source address resolves
> to no current member before any work runs (mitigation shipped, M12b
> review), but with `[crypto].enabled = false` a network-path adversary
> able to spoof a member's UDP source address can still inject a proposal.
> **`[admin] auth = "hmac"` authenticates cluster-wide only paired with
> `[crypto].enabled = true`** — stated in
> `docs/reference/configuration.md#admin-authentication`,
> `docs/how-to/change-cluster-membership.md`, and
> `docs/how-to/encrypt-node-traffic.md`.
>
> **§5.3, the shipped record shape** differs from the sketch's `args{id,
> addr}` — the actual fields are flat, matching `uc2_node::audit::AuditRecord`:
> `{ts_ns, event:"admin_op", actor, origin, op, op_name, id, addr, seq,
> nonce, outcome, reason, config_version}`. `addr` is `null` for ops that
> carry no address (`promote`/`demote`/`remove-*`); `actor` is the signing
> key's name under `Hmac`, `"filesystem"` under `Filesystem`
> (`ACTOR_UNVERIFIED = "unverified"` — not `"filesystem"` — when
> authentication itself failed, since neither the claimed key name nor
> `"filesystem"` would be honest there), or `"peer:<id>"` for a leader
> recording a peer-forwarded proposal. `seq` is `0` on a leader's
> `forwarded`-origin record (the requesting node's admin-band `seq` is
> local to it and the wire proposal does not carry it; `nonce` is the join
> key between the two nodes' records for the same change).
>
> **`AdminPolicy` is not a `NodeConfig` field.** It lives on a new
> `StartOpts { socket: Option<UdpSocket>, admin: AdminPolicy }`, passed to
> the one real constructor `Node::start_with(cfg, opts)` — both fields are
> live process resources (a bound socket; loaded key material), not values
> a `Clone`-able, TOML-mirroring config struct should carry. `Node::start`
> and `Node::start_with_socket` are thin wrappers that pass
> `StartOpts::default()`, whose `admin` is `AdminPolicy::Filesystem` — the
> pre-M12b posture, unchanged for every library caller (in-process tests,
> gates, harnesses). Only the `uc2-node` daemon binary
> (`uc2_node/src/bin/uc2-node.rs`) builds a live `AdminPolicy` from
> `[admin]` and calls `Node::start_with` directly.

### 5.1 Credential

```toml
[admin]
auth = "hmac"                 # or "none": filesystem access is the boundary (WARN at boot)
keys = [{ name = "ops-alice", key_path = "/etc/uc2/admin/alice.key" }]
request_ttl_ms = 30000
```
Key file: 32 random bytes, mode `0600`, owned by the daemon user (refused by
name otherwise — same rule as `[crypto].key_path`). `uc2ctl gen-admin-key
PATH` creates one. Absent `[admin]` → `ConfigError::AdminChoiceRequired`.

### 5.2 Signed request — existing slot + reserved band, no protocol change

`uc2ctl --admin-key PATH [--admin-key-name NAME]` (name defaults to the file
stem) writes the existing 32-byte `AdminReq` (`seq, nonce, op, id, ip, port`
at `CNC_OFF_ADMIN_REQ = 3584`) plus a new 64-byte line `CNC_OFF_ADMIN_AUTH =
3904`: `tag[32] @0 = HMAC-SHA256(key, app_id ‖ instance_id ‖ seq ‖ nonce ‖
op ‖ id ‖ ip ‖ port ‖ expiry_ns)`, `expiry_ns u64 @32`, `key_name_hash u64
@40` (FNV-1a 64 of the name), 16 bytes reserved; published under the same
seqlock discipline as the request (auth line written before `req.seq`'s
release store). The `instance_id` binding defeats replay against a restarted
node; `expiry` bounds the window; the node keeps a small ring of recent
`(seq, nonce)` to refuse replays inside it. New dependency: RustCrypto `hmac`
(in-family with the existing `sha2`/`hkdf`). Read-only `status` and the
offline verbs (`backup`, `verify-backup`, `restore`, `force-single-member`)
need no key.

Node side: `handle_admin` verifies **first**; unknown key / bad tag / expired
/ replayed / unconfigured → `status = 1` with new reason codes
(`auth_missing`, `auth_bad_tag`, `auth_expired`, `auth_replay`,
`auth_unconfigured`). A follower verifies locally before forwarding (kind 16);
the peer plane's trust is whatever `[crypto]` says, as today. One HMAC per
admin op — off the hot path.

### 5.3 Audit log

`<instance_dir>/audit.jsonl`, append-only (`O_APPEND`, fsync per record),
written **before** the response line is published so a refused request is
always recorded:
`{ts_ns, actor: "ops-alice"|"filesystem", origin: local|forwarded, op,
args{id,addr}, seq, nonce, outcome: accepted|refused|retry, reason,
config_version}`. Mirrored as an `obs_event!` at `info`. No rotation
(documented; admin ops are rare). `uc2ctl audit --instance-dir D`
pretty-prints. The committing leader records forwarded proposals with
`origin: forwarded`.

### 5.4 `[crypto].enabled`

`CryptoSection { enabled: bool (required), key_path, allowlist_path,
rotation… }`; `enabled = true` requires the paths as today; absent section →
`ConfigError::CryptoChoiceRequired`.

### 5.5 Docs and tests

Docs state plainly: `app_id` is a wrong-cluster guard, not a credential;
`auth = "none"` means anyone who can write the instance directory can remove
voters. Tests: HMAC vector; expiry / replay / unknown-key / permission
refusals; absence refusals by name; CI integration: signed op accepted +
audited, unsigned refused + audited, replay refused; `auth = "none"` path
unchanged; `m7_gate`, the reconfig suites and `bench-infra` choose a mode
explicitly. Upgrade note with the two new startup refusals.

## 6. M12c — Packaging, publishing, hygiene

- **Supply chain**: `cargo-deny` (advisories, licenses, bans — incl. exactly
  one AES-GCM implementation in the graph) and a CycloneDX SBOM attached to
  releases; dead `quinn`/`rustls`/`rcgen`/`rustls-pemfile` workspace deps
  removed; `rust-version` + pinned toolchain + `msrv` CI job (§3.2).
- **Publishing**: `cargo publish --dry-run` of the whole DAG
  (`ultima_journal`, `uc_protocol` → `uc2_log`/`uc2_crypto` → `uc2_net`/
  `uc2_consensus` → `uc2_node`/`uc2_client` → `uc2_service`/`uc2_remote`/
  `uc2_gateway` → `uc2ctl`) as a CI job; the real publish is a manual step at
  tag time with the maintainer's token.
- **`release.yml`** on tag `v*`: matrix `x86_64-unknown-linux-gnu` +
  `aarch64-unknown-linux-gnu`; `cargo build --release --locked` of `uc2-node`,
  `uc2ctl`, `uc2-gateway`, `counter-service`, `counter-client`; strip;
  `uc2-<ver>-<target>.tar.gz` + `SHA256SUMS` + SBOM; **cosign keyless**
  (`permissions: id-token: write`, `cosign sign-blob`, bundles attached);
  GitHub Release; image `ghcr.io/peterknego/uc2:<ver>` on
  `gcr.io/distroless/cc` with the binaries + `packaging/`, `cosign sign` on
  the digest. A **`release-smoke`** job runs before publish: pulls the fresh
  tarball into a bare `ubuntu` container without Rust and runs the binary
  quickstart (3 nodes + gateway + counter on one host, a remote client
  submit) — the "no toolchain" gate row, proven on every tag.
- **Quickstart + packaging**: `docs/QUICKSTART.md` rewritten artifacts-first
  (download, `cosign verify-blob`, `uc2ctl gen-admin-key`, three `node.toml`s,
  systemd or `packaging/compose.yml`, `uc2-gateway`, remote client); the
  cargo path becomes a "from source" section. `uc2-gateway.service`,
  `gateway.example.toml`, `compose.yml` under `packaging/`.
- **`cargo fmt`**: one-shot + `--check` gate if no long-lived branch is open
  when M12c reaches it; otherwise deferred, stated in the gate doc.

## 7. M12d — Security posture and the review-ready package

- README **Security posture** section, in its own words: cleartext by
  default means the `uc_protocol::v2` decoders parse untrusted bytes with
  nothing in front of them; a malicious member can forge fan-out traffic
  (symmetric group key); the remote client link is plain TCP with no
  authentication in this release; `app_id` is not a credential; admin needs
  an HMAC key unless the operator chose `"none"`.
- README **Scope/limits** (production-readiness §9): 8 members max; one node
  per instance dir; one state machine per cluster; all fleet measurements
  single-AZ; **command payload ≤ ~1.3 KB** (one datagram, `MTU_DEFAULT =
  1408`, not configurable — the spike surfaced this).
- `docs/security/threat-model.md`, `attack-surface.md` (UDP datagram header
  and frame decoders, `AppendPositionBody`, snapshot-session framing, cnc
  admin slot + auth line, gateway frame decoder, TOML config, `/metrics`
  HTTP) and `self-assessment.md`.
- **Fuzz targets** (`cargo-fuzz`, `fuzz/`, nightly toolchain) for each
  decoder above; a `fuzz` job in `nightly.yml` (~10 min per target, corpus
  committed). **Miri** over rings/cnc accessors attempted in the same job;
  if mmap blocks it, the note says so and a `Vec`-backed ring variant is the
  fallback — attempted, not promised.
- External review: the package is the deliverable; the review is a
  separate, user-scheduled step tracked like a fleet gate (gate row:
  "package prepared; external review pending").

## 8. Acceptance gate (`docs/benchmarks/uc2-m12-gate-<date>.md`)

| Row | Proof |
|---|---|
| Gateway follows the leader across failover; zero acked writes lost | remote lincheck capstone, CI |
| Gateway throughput cost vs direct `Engine` | `m12_gate`, fleet; bar ≥ 0.8× (plan finalises) |
| Codec share on the apply thread at the M5 ladder, raw vs typed | `m5_gate` + `apply-profile`, fleet |
| Admin op without a credential refused + audited; with one accepted + audited; replay refused | CI integration test |
| Quickstart cluster from published artifacts with no toolchain | `release-smoke`, CI, every tag |
| Artifacts and image verifiable | `cosign verify-blob` / `cosign verify` in `release-smoke` |
| Crates publishable | `cargo publish --dry-run` DAG, CI; published manually at tag |
| Decoder fuzz job green | `nightly.yml` `fuzz` |
| Security package present | threat model, attack surface, self-assessment in tree |
| External review | separate, user-scheduled; row reads "pending" until done |

Release documentation per `CLAUDE.md`: new section atop `RELEASES.md`,
`docs/releases.md` entry, QUICKSTART/how-to/reference sweep, explainer notes
(gateway shapes + flow control; two-tier state-machine contract; admin
authn), all before the `v2.6.0` tag.

## 9. Risks

| Risk | Mitigation |
|---|---|
| The gateway accretes correctness obligations | Locked stateless; dedup lives in `Sessioned` inside the FSM; `Engine`'s slot correlation is the only exactly-once boundary on the edge. |
| The two-tier contract breaks `2.5.0` state machines or clients | Typed trait signature unchanged; blanket adapter; wire byte-identity test; every existing suite runs unchanged. |
| Explicit-choice config breaks existing deployments | Named refusals; upgrade note; example config ships both choices; fixtures updated. |
| Credits mis-sized stall or overrun clients | Property tests; edge shrinks credits before `Backpressure`, TCP backstop behind. |
| cosign keyless / ghcr permissions surprise at tag time | `release.yml` runs on a pre-release tag (`v2.6.0-rc.1`) first; `release-smoke` gates publish. |
| `Miri` cannot run over mmap | Attempted, not promised; the fallback is stated. |
| Scope creep | Four sub-milestones, each independently shippable; M12d can shrink without invalidating a–c. |

## 10. Plans

One implementation plan per sub-milestone, written when it starts:
`docs/superpowers/plans/2026-08-22-uc2-m12a-gateway-kit.md` first. The
others follow the decisions here; if a plan finds a reason to deviate, this
spec is amended, not silently bypassed.

## References

- `docs/superpowers/specs/2026-08-19-uc2-production-readiness-design.md` §7–§11
- `docs/notes/2026-08-22-codec-budget-spike.md` (and `hi-perf-cmp`
  `docs/notes/2026-08-22-uc-kv-codec-ladder.md`)
- Aeron Cluster client (`aeron-go/cluster/client/aeron_cluster.go`): ingress
  endpoints, `SessionEvent{REDIRECT}`, `NewLeaderEvent`, Status-Message flow
  control — the shape D reference and the credit model.
