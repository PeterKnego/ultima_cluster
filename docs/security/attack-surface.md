<!-- SPDX-License-Identifier: Apache-2.0 -->
# Attack surface

Every place `ultima_cluster` parses bytes it did not write, what reaches it,
what bounds it, and which fuzz target covers it. The
[threat model](threat-model.md) says who the adversaries are; this page is the
inventory. [Self-assessment](self-assessment.md) records what was found in it.

"Fuzz target" names a real target in [`fuzz/fuzz_targets/`](/fuzz) — there are
fourteen, each run for 600 s per night with an asserted execution floor
([VERIFICATION §7](/docs/VERIFICATION.md#7-fuzzing--decoders-total-on-untrusted-bytes),
[`fuzz/README.md`](/fuzz/README.md)).

---

## 1. The inventory

| Surface | Entry (file: function) | Reachable by | Authenticated? | Length / shape guards | Fuzz target | Notes |
|---|---|---|---|---|---|---|
| **UDP datagram header** | `uc_protocol/src/v2/datagram.rs: read_datagram_header`, via `uc2_net/src/receiver.rs: Receiver::poll` → `crypto_admit` → `on_datagram` | anyone who can send a packet to the node's UDP port | crypto ON: yes, AEAD with the header as AAD. crypto OFF: **no** | `n >= DATAGRAM_HEADER_LEN` pre-guard *and* the reader is total (returns `Option`) | `uc_protocol_datagram` | The first code an unauthenticated packet reaches. With crypto off it is also the last line of defence. |
| **Datagram body readers** (kinds 3, 4, 5, 7, 8, 9, 10, 12, 14, 16, 17) | same file: `read_nak_body`, `read_status_body`, `read_append_position_body`, `read_request_vote_body`, `read_vote_body`, `read_term_map_body`, `read_read_probe_body`, `read_snap_begin_body`, `read_snap_nak_body`, `read_config_proposal_body`, `read_config_reply_body` | as above | as above | each checks its own fixed `*_BODY_LEN` and returns `Option`; `read_term_map_body` additionally caps at `MAX_TERM_MAP_WIRE_ENTRIES = 64` and against the caller's `out` slice | `uc_protocol_datagram` | Five of these were caller-guarded-only until M12d (finding F1, §3 of the self-assessment). |
| **`AppendPositionBody`** (wire 0.5.0 content attestation) | `datagram.rs: read_append_position_body` (`APPEND_POSITION_BODY_LEN = 8`) | any peer, or anyone spoofing one with crypto off | crypto ON only | fixed 8-byte body; a header-only (0.4.0) report reads as **unattested and is not counted** | `uc_protocol_datagram` | The term the sender attributes to the byte below its reported position. A disagreeing report is declined by the leader — the field turns commit ranking from a position quorum into a content quorum. |
| **Snapshot-session framing** (`SNAP_BEGIN` 12, `SNAP_CHUNK` 13, `SNAP_NAK` 14, `SNAP_DONE` 15) | `datagram.rs: read_snap_begin_body` (`SNAP_BEGIN_FIXED_LEN = 26` + a variable config tail), `read_snap_nak_body` | any peer | crypto ON: yes — **all** SNAP kinds are sealed since M8 T17 | fixed prefix + a bounded config tail; total readers | `uc_protocol_datagram` | A forged `SNAP_BEGIN` would carry attacker-chosen application state *and* membership into a joining node; the M8 fall-through that closes it is pinned by `an_unsealed_snap_begin_is_refused_now_that_t17_landed`. |
| **Noise `IK` handshake** (kinds 18 `HS_INIT`, 19 `HS_RESP`) | `uc2_crypto/src/handshake.rs: Peers::on_message`, routed by `receiver.rs: crypto_admit`'s `Scope::Unsealed` arm | anyone who can reach the UDP port, when crypto is **on** | **no — this is the pre-auth surface**; it is what creates a session | `snow`'s own message-length checks; the allowlist rejects an unknown static; claims-our-own-id is an explicit early refusal; an allowlist-reload rate limit driven by `now_ns` | `uc2_crypto_handshake` | The most valuable target in the set: turning crypto on *adds* this parser to the pre-auth surface. |
| **AEAD envelope** | `uc2_crypto/src/seal.rs: open_in_place`, `open_detached` | any sender of a sealed datagram | this **is** the authentication; the framing arithmetic runs before the tag is verified | `buf.len() >= DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD` (24 = 8-byte counter + 16-byte tag) before any split | `uc2_crypto_open` | Anti-replay is RFC-6479 over the per-sender counter. |
| **Group-key message** (kind 20 `HS_KEY`) | `uc2_crypto/src/group.rs: GroupPlane::on_key_message` | a peer whose identity resolved (pairwise-sealed) | yes | two distinct message shapes share the kind, so the decoder must disambiguate hostile input | `uc2_crypto_group_key` | The group key is symmetric — see the threat model §5. |
| **cnc control page** | `uc_protocol/src/v2/cnc.rs: read_cnc_header`, `read_cnc_app_id` | any local process that can read the instance directory | no — file permissions are the control | 4 KiB fixed layout, magic + version gate, offsets pinned in two crates with assertion tests | `uc_protocol_cnc` | Only magic-checked, not integrity-protected: that is exactly why admin binding is taken from boot-time state instead (finding F4). |
| **cnc admin slot + auth line** | `uc2_log/src/cnc.rs: CncPage::read_admin_req` (seqlock, `CNC_OFF_ADMIN_REQ`) and `read_admin_auth` (`CNC_OFF_ADMIN_AUTH = 3904`, 64 bytes) → `uc2_node/src/node.rs: Consensus::verify_admin` | any local process that can write the instance directory | **yes with `[admin] auth = "hmac"`**: HMAC-SHA256 tag, `expiry_ns`, `seq > last_admin_seq`, and `instance_id`/`app_id` bound from boot-time state. `auth = "none"` = filesystem permissions only | fixed 64-byte line; acquire-load of the seqlock commit word so a torn write is never observed | `uc2_crypto_admin` (property target over the signed-tag layout) | The tag covers `seq`, so no separate replay ring exists — the reasoning is in the spec's M12b "As built" amendment. |
| **`audit.jsonl`** | `uc2_node/src/audit.rs: AuditLog::record` (append + `fsync`, before the answer is published); read offline by `uc2ctl audit` | written by the node; read by an operator | n/a (append-only file, not a parser of adversarial input) | one JSON object per line; a partial tail line is the crash signature, not an input | — | Anyone who can write the instance directory can truncate it. It is a record for an operator, not tamper-evident against a local attacker. |
| **Log frame header** | `uc_protocol/src/v2/frame.rs: read_header` (`HEADER_LEN = 32`, 32-byte alignment) | the archive/replay path and the apply reader; bytes originate from a peer's replication stream | inherits the datagram plane's answer | **deliberately caller-guarded** (`len >= HEADER_LEN`) — a hot-path reader; the fuzz target drives it *behind* the real caller's guard so it pins the contract rather than pretending it is absent | `uc_protocol_log_frame` | The atomic-after-write length prefix (0 = uncommitted) is the torn-record protection. |
| **Gateway TCP frames** | `uc2_remote/src/frame.rs: decode_header` (24-byte header) + seven body decoders (`Hello`, `HelloOk`, `HelloRefused`, `ResponseMeta`, `Status`, `Leader`, `Retry`) via `uc2_gateway/src/conn.rs` | **anyone who can open a TCP socket to the gateway port** | **no.** No client credential, no TLS in this release | `len > MAX_FRAME_LEN (1 MiB)` → `FrameError::TooLong` at decode, before allocation; `HELLO` must precede credits; per-connection credit window; `max_connections` (default 1024); a 1 s socket `WRITE_TIMEOUT`; `request_timeout` on outstanding completions | `uc2_remote_frame` | The most exposed surface in the system. `FramedConn::read_frame`'s accumulate loop is *not* fuzzed (VERIFICATION §7) — the decoder is. |
| **`node.toml`** | `uc2_node/src/config_file.rs: parse_str` | the operator (local file) | n/a | `serde(deny_unknown_fields)` on every section, then `preflight::check_semantics` — every named startup refusal | `uc2_node_toml` | Not an adversary surface; it is the surface where an operator mistake becomes a named refusal instead of a broken cluster. |
| **`gateway.toml`** | `uc2_gateway/src/config_file.rs: parse_str` (runs `EdgeConfig::validate` itself) | the operator | n/a | same | `uc2_gateway_toml` | |
| **`/metrics`, `/healthz`, `/readyz`** | `uc2_node/src/obs/http.rs: route` (fuzzed through the `cfg(any(test, fuzzing))` seam `route_raw`) | **anyone who can reach the metrics port** | **no — unauthenticated by design** | request buffer capped at `REQUEST_CAP = 4096` bytes; a connection deadline; only the request line is parsed; non-`GET` and unknown paths are 404; the status set is exactly `{200, 404, 503}` | `uc2_node_http` | The bind address is the only control. See §2. |
| **Journal segments and records** | `ultima_journal` segment header + record decoder, reached on every crash recovery | a local process with write access to the instance directory; otherwise, disk corruption | no | CRC per block, length checks, tail-preserving recovery | `ultima_journal_record` | **CRC is integrity, not authenticity.** A local attacker who can write the directory is equivalent to host compromise (threat model §5). |
| **`StableValue` slots** (vote, term map, snapshot floor, output progress, config) | `ultima_journal::stable_value` | as above | no | rotating two-slot write, CRC per slot | `ultima_journal_stable_value` | Corruption here is a consensus-safety input, not merely data loss. |
| **Session envelope** | `uc2_service/src/session.rs: Sessioned::{apply, install_snapshot}` | any remote client, through the gateway | no (the envelope is a dedup key, not a credential — `client_id` is client-chosen) | `SESSION_HEADER_LEN = 16` (`client_id ++ seq`); a shorter command is `TAG_EXPIRED`, never a panic; snapshot install bounded with `take(len)` after finding F3; replicated `SessionConfig` enforces client-count and byte budgets with LRU eviction | `uc2_service_session` | Two real findings lived here (F2, F3). A hostile client can churn `client_id`s to force eviction — that is a liveness effect on *other* clients' dedup, not a correctness one. |
| **Typed state-machine tier (bincode)** | `uc2_service/src/traits.rs`: the blanket `impl<S: StateMachine> RawStateMachine for S` — `bincode::serde::decode_from_slice::<S::Command>(cmd, bincode::config::standard())` | a remote client's opaque command bytes, **after** they are committed | no | **`bincode::config::standard()` is `Configuration<LittleEndian, Varint, NoLimit>` — no decode byte limit is configured.** What bounds it in practice: (a) the input slice is at most `max_payload` (≤ 1344 bytes, §3), (b) serde's `size_hint::cautious` caps a single container pre-allocation at 1 MiB, (c) a decode failure is `.expect("corrupt committed frame (fail-stop)")` | **not fuzzed here** — see the note | Fuzzing bincode would be fuzzing an external crate (VERIFICATION §7's "does not cover"). The honest consequence to state: with a typed state machine and untrusted remote clients, malformed bytes become *committed* bytes and every replica fail-stops identically on apply. That is the documented "committed bytes are trusted" stance ([state-machine contract](/docs/reference/state-machine-contract.md)); the answer for genuinely untrusted input is the **raw tier**, which hands you the bytes and lets you reject them. |

## 2. Bind-address guidance

Three listening sockets, three different answers. `packaging/node.example.toml`
and `packaging/gateway.example.toml` carry the same advice inline.

| Port | What it is | Expose to |
|---|---|---|
| **Node UDP** (`bind`, e.g. `10.0.0.10:9100`) | the replication and consensus plane | **the other nodes, and nothing else.** It must be the node's real address — identical to its own `members` entry, never a wildcard, or preflight refuses to start. With `[crypto].enabled = false` anyone who can reach this port can inject consensus traffic; with it on, an unlisted static key gets no session. Put it on a private subnet or a security group that admits only the member addresses. |
| **Gateway TCP** (`9200`+) | the remote client front door | **your application tier.** Unauthenticated and unencrypted in this release: treat reachability as authorization. Do not expose it to the internet; if clients are remote, terminate authentication and TLS in a proxy in front of it. |
| **Metrics HTTP** (`[metrics] bind`, e.g. `127.0.0.1:9600`) | `/metrics`, `/healthz`, `/readyz` | **loopback, or a private address your scraper can reach.** Unauthenticated by design — the endpoint discloses positions, terms, role and peer state. Absent `[metrics]` means no endpoint at all. |

## 3. The command payload ceiling

A command travels in **one datagram** — the node does not fragment frames. With
`MTU_DEFAULT = 1408` (not configurable) and `DATAGRAM_HEADER_LEN = 16`, the
frame header at `HEADER_LEN = 32` rounded up to `FRAME_ALIGNMENT = 32`:

- **crypto off — `max_payload` ≤ 1344 bytes**
- **crypto on — `max_payload` ≤ 1312 bytes** (`CRYPTO_OVERHEAD = 24`: an 8-byte
  counter plus a 16-byte GCM tag)

`uc2_node::preflight::check_semantics` computes exactly this and refuses to
start with `PayloadExceedsMtu`, naming the number of bytes the configured value
would need. The default in `packaging/node.example.toml` is 512.

## 4. What this page deliberately does not list

- **`bincode` internals and the `ultima-db` `snapshot_stream` adapter** — both
  are external crates reached through the seams above. They are a dependency
  posture question (`deny.toml`, the SBOM shipped with each release), not a
  target here.
- **Stateful sequences.** Every fuzz target is one decoder on one input.
  Sequences that drive the receiver's dispatch across terms, epochs and
  sessions belong to the simulation and the linearizability capstones
  ([VERIFICATION §2, §3](/docs/VERIFICATION.md)).
- **The IPC ring buffers' `unsafe` code.** Covered by loom for interleavings
  and offset-pin tests for layout, and by neither for undefined behaviour —
  Miri cannot map a file-backed region. Stated in
  [VERIFICATION §11](/docs/VERIFICATION.md#11-what-is-not-verified).
