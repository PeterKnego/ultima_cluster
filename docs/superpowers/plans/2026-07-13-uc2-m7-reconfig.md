# UC v2 M7 — dynamic reconfiguration (single-server membership change) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** membership becomes a live, online operation — replace a dead box and resize 3⇄5 under load via single-server changes recorded as log entries, with the config × truncation × snapshot × recovery surface held by the sim + lincheck + crashtest stack (the surface Aeron retreated from).

**Architecture:** a membership change is a `FRAME_TYPE_CONFIG = 4` frame appended in-stream by the leader (adopt-at-append) and adopted by followers when durably recorded (the archive frame-scan that data-stamps term maps also yields config observations); at adoption every slot-indexed structure is REBUILT (fresh `CommitTracker`, `follower_slot` remap, sender fan-out + flow control via a `CtrlMsg::SetPeers`), with the last durable report per surviving member carried over; one change in flight (a new proposal is refused until the previous config frame commits) makes exactly one level of durable config history (`ConfigRecord { cur, prev }`) sufficient, and truncation strictly below the config's frame-end position reverts to `prev` (persist-revert-before-truncate, inside the existing truncation-epoch latch); removed ids are tombstoned forever, and v2's known-source guards drop a removed node's traffic at the door (the job Raft needs pre-vote for); operators drive the five ops through a same-host cnc admin slot (`uc2ctl`), with follower→leader forwarding as datagram kinds 16/17.

**Tech Stack:** Rust workspace (edition 2024), `uc_protocol` v2 (core-only wire codecs), `uc_log` (StableValue `ConfigRecord`, archive config-scan), `uc_consensus` (dep-free `ClusterConfig` + `ElectionSm` dynamic membership), `uc_net` (SetPeers rebuild, kinds 16/17), `uc_node` (adoption wiring, admin poll, uc2ctl), `uc_sim` (inv6–9 + config fuzz), `uc_lincheck` (checker unchanged — again).

## Global Constraints

Copied from the spec (`docs/superpowers/specs/2026-07-13-uc2-reconfig-design.md`) and house rules:

- **Scope:** promote / demote / add-learner / remove-learner / remove-voter, **exactly one change in flight**. No arbitrary set-to-set changes, no joint consensus.
- **Adopt-at-append (leader) / adopt-at-durable (follower, via archive scan).** A node's quorum arithmetic always uses its adopted config; safety = single-server overlap (any majority of version v intersects any majority of v+1).
- **The single-server-change precondition is the serving gate:** config proposals are accepted ONLY from a serving leader (`can_serve() == true`, i.e. its NewTerm frame committed in its own term). Never weaken this — it is the Ongaro-2015 fix, structurally.
- **One level of config history is sufficient and load-bearing:** proposals are refused while the previous config frame is uncommitted, and committed frames cannot be truncated — so at most one config entry is ever truncation-exposed. If you find yourself needing deeper history, the one-in-flight rule is broken somewhere.
- **Truncation revert:** `to < config_record.position` ⇒ revert to `prev` (persist-revert-BEFORE-truncate, inside the existing `Action::Truncate` exec ordering: persist map → persist config revert → `truncate_to` → prime → ack). `to == position` preserves the frame — no revert. Positions are frame-END effect points; truncation is frame-aligned.
- **Wipe (`Truncate{to:0}`) keeps the OPERATIONAL config** (config-by-fiat, same authority argument as `adopt_snapshot_lineage`): the durable record is reset to `{position: 0, config: current, prev: current, prev_position: 0}` and the refill re-stamps real positions. Snapshot installs adopt the leader's config-at-floor shipped in the extended `SNAP_BEGIN`.
- **Fresh-forever NodeIds:** tombstoned ids never re-enter either list; enforced in `ClusterConfig::apply` (pure function — every caller shares it).
- **Hard cap 8 total members** (voters + learners, incl. transitional), enforced at proposal — the cnc PeerSlots band is the limit.
- **Protocol version bumps once** (minor +1) with FRAME_TYPE_CONFIG=4 + kinds 16/17 + the SNAP_BEGIN extension + new cnc offsets. v2.0 nodes refuse at entry (existing rule). Upgrade order documented: all binaries first, then reconfigure.
- **Removed-node behavior:** on adopting a config excluding itself a node halts fail-stop; a leader removing itself keeps leading until the config entry COMMITS, then halts (step-down = existing election takes over).
- **Every cnc counter has exactly one writer;** new slots land in 3456..4096 with the writer named at the offset constant. `uc_protocol` codecs stay core-only (no `SocketAddr` — addrs are `(u32 ipv4, u16 port)` LE on the wire and in `uc_consensus`; only `uc_node` converts).
- **Apply stays sync/deterministic/no-I/O**; CONFIG frames are consensus-plane — the apply layer skips every non-MESSAGE frame type already (no service change).
- **clippy `--workspace --all-targets -- -D warnings` stays clean** (CI enforces it now). Denied-lints history: `manual_is_multiple_of`, `int_plus_one`, `collapsible_if` (let-chains).
- **Journals/instance dirs never on `/tmp` for load runs** (RAM tmpfs; the orchestrator enforces fs-type). Unit tests with tiny buffers may use `tempfile::tempdir()`; journal-bearing integration tests use `tempdir_in(env!("CARGO_TARGET_TMPDIR"))` (failover.rs pattern).
- **Implementers stage ONLY their own files** (never `git add -A` — a `__pycache__` got committed that way once); branch `uc2/m7-reconfig`. `Cargo.lock` staged explicitly and named when touched.
- **Honest gates:** binaries print the bar and `exit(1)` on FAIL; in-process runs are smoke, never the gate; "Task 11 complete" ≠ "M7 gate passed" (the fleet run is a separate user-approved step, M1–M6 precedent).
- **Sim-first discipline:** Task 4 (sim) lands BEFORE the node wiring (Task 5). If the sim can't model a mechanism mechanically, the mechanism is wrong — redesign, don't stub.

## As-built anchor map (read these before your task)

| Seam | Where |
|---|---|
| Frame types + header | `uc_protocol/src/v2/frame.rs` (MESSAGE=1, PADDING=2, NEW_TERM=3; 32 B header; `write_header_except_length`/`read_header`) |
| Datagram kinds | `uc_protocol/src/v2/datagram.rs` (1–15 used; body codec pattern = `SnapBeginBody` + `write_/read_snap_begin_body`) |
| cnc offsets | `uc_protocol/src/v2/cnc.rs` (PeerSlots end at 3456; page 4096) + accessors in `uc_log/src/cnc.rs` |
| Durables | `uc_log/src/state.rs` `NodeState` (vote/term_map/output_progress/snapshot StableValues + one cache Mutex) |
| Election SM | `uc_consensus/src/election.rs` (`ElectionSm`, `Event`, `Action`, `follower_slot` at :810, `become_leader` :658, `adopt_term` :677, truncating latch in `step` :337, `adopt_snapshot_lineage` :556) |
| Commit ranking | `uc_consensus/src/commit.rs` (`CommitTracker::new(n_followers, cluster_size)`, `on_durable(slot, durable)`, `advance(own)`, `reset_reports()`) |
| Node wiring | `uc_node/src/node.rs` (`NodeConfig` :106, construction :391–546, `do_work` steps 1–10 :972, `feed_net` :1613, `exec` :1758, `send(to, kind, position, term, body)` :1941) |
| Leader append | `uc_log/src/buffer.rs` `append_new_term` :427 (mirror for `append_config`) |
| Archive scan | `uc_log/src/archive.rs` term stamping :270–290, `take_term_observations` :289, `truncate_to` :293+ |
| Sender control | `uc_net/src/sender.rs` `CtrlMsg` :75, `with_learners` :245, `set_peer_slots` :298 |
| Flow control | `uc_net/src/flow.rs` (`new`, `on_status`, `limit`) |
| Net events | `uc_net/src/receiver.rs` `NetEvent` :55 (+ `kind_idx` — extend BOTH) |
| Sim | `uc_sim/src/world.rs` (`World`, `SimConfig`, `Msg`, `inject_*`, `crash/restart`), `uc_sim/src/invariants.rs` (`InvariantChecker`) |
| Integration helpers | `uc_node/tests/failover.rs` (`spawn_cluster_ring`, `NodeH`, `make_config_ring`, `DEFAULT_RING`) |
| L3 harness | `uc_node/tests/lincheck_v2/mod.rs` (`ClusterCfg`, `start_cfg`, fault arms in `lin_v2.rs`) |
| Fleet orchestrator | `bench-infra/scripts/m6_fleet_gate.py` (LocalHost/SshHost, probe/loadclient pattern, `assert_durable_fs`) |

---

### Task 1: Wire layer — CONFIG frame, config codec, kinds 16/17, cnc offsets, version bump

**Files:**
- Create: `uc_protocol/src/v2/config.rs`
- Modify: `uc_protocol/src/v2/frame.rs` (add `FRAME_TYPE_CONFIG`)
- Modify: `uc_protocol/src/v2/datagram.rs` (kinds 16/17 + bodies; extend `SnapBeginBody`)
- Modify: `uc_protocol/src/v2/cnc.rs` (offsets 3456..3776)
- Modify: `uc_protocol/src/v2/mod.rs` (export `config`)
- Modify: `uc_protocol/src/version.rs` (minor +1)
- Modify: `uc_log/src/cnc.rs` (accessors + offset-pin test extension)

**Interfaces:**
- Produces: `uc_protocol::v2::frame::FRAME_TYPE_CONFIG: u8 = 4`
- Produces: `uc_protocol::v2::config::{WireConfig, WireMember, encode_config(&WireConfig, &mut Vec<u8>), decode_config(&[u8]) -> Option<WireConfig>, MAX_MEMBERS: usize = 8}` where `WireMember { id: u32, ip: u32, port: u16 }` and `WireConfig { version: u64, prev_position: u64, voters: Vec<WireMember>, learners: Vec<WireMember>, tombstones: Vec<u32> }`
- Produces: `DGRAM_KIND_CONFIG_PROPOSAL: u8 = 16`, `DGRAM_KIND_CONFIG_REPLY: u8 = 17`, `ConfigProposalBody { nonce: u64, op: u32, id: u32, ip: u32, port: u16 }`, `ConfigReplyBody { nonce: u64, status: u32, reason: u32, version: u64 }` + `write_/read_` fns (SnapBegin pattern)
- Produces: `SnapBeginBody` gains `config: Vec<u8>` (the encoded `WireConfig` at the floor; length-prefixed u16 after `total_len` — old field offsets unchanged)
- Produces cnc constants (all u64 lines, 64-B stride): `CNC_OFF_CONFIG_VERSION = 3456` (writer: consensus agent), `CNC_OFF_CONFIG_PENDING = 3520` (writer: consensus agent), `CNC_OFF_ADMIN_REQ = 3584` (writer: uc2ctl; layout within the line: seq u64 @+0 — written LAST/release, nonce u64 @+8, op u32 @+16, id u32 @+20, ip u32 @+24, port u32 @+28), `CNC_OFF_ADMIN_RESP = 3648` (writer: consensus agent; seq u64 @+0 echoes the request seq — written LAST/release, status u32 @+8, reason u32 @+12, version u64 @+16). Const-assert `3648 + 64 <= 4096`.
- Produces `uc_log::cnc::CncPage` accessors: `config_version()`, `config_pending()`, `admin_req_*()` / `write_admin_req(..)`, `admin_resp_*()` / `write_admin_resp(..)` (seqlock discipline: readers check seq; writers write fields then seq with release — mirror the existing PeerSlot accessor style).

- [ ] **Step 1: failing tests for the config codec + frame type + kinds**

In `uc_protocol/src/v2/config.rs` (new file, tests module):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> WireConfig {
        WireConfig {
            version: 3,
            prev_position: 4096,
            voters: vec![
                WireMember { id: 1, ip: u32::from_be_bytes([10, 0, 0, 1]), port: 19100 },
                WireMember { id: 2, ip: u32::from_be_bytes([10, 0, 0, 2]), port: 19100 },
                WireMember { id: 3, ip: u32::from_be_bytes([10, 0, 0, 3]), port: 19100 },
            ],
            learners: vec![WireMember { id: 5, ip: u32::from_be_bytes([10, 0, 0, 5]), port: 19100 }],
            tombstones: vec![4],
        }
    }

    #[test]
    fn config_roundtrips() {
        let c = sample();
        let mut buf = Vec::new();
        encode_config(&c, &mut buf);
        assert_eq!(decode_config(&buf), Some(c));
    }

    #[test]
    fn config_wire_layout_is_pinned_le() {
        // version=3, prev_position=4096, counts (voters=3, learners=1, tombs=1)
        let c = sample();
        let mut buf = Vec::new();
        encode_config(&c, &mut buf);
        assert_eq!(&buf[0..8], &3u64.to_le_bytes());          // version
        assert_eq!(&buf[8..16], &4096u64.to_le_bytes());      // prev_position
        assert_eq!(&buf[16..18], &3u16.to_le_bytes());        // n_voters
        assert_eq!(&buf[18..20], &1u16.to_le_bytes());        // n_learners
        assert_eq!(&buf[20..22], &1u16.to_le_bytes());        // n_tombstones
        // first voter entry: id u32 | ip u32 | port u16 = 10 bytes
        assert_eq!(&buf[22..26], &1u32.to_le_bytes());
        assert_eq!(&buf[26..30], &u32::from_be_bytes([10, 0, 0, 1]).to_le_bytes());
        assert_eq!(&buf[30..32], &19100u16.to_le_bytes());
        assert_eq!(buf.len(), 22 + 4 * 10 + 4); // header + 4 members + 1 tombstone u32
    }

    #[test]
    fn decode_rejects_truncated_and_oversized() {
        let mut buf = Vec::new();
        encode_config(&sample(), &mut buf);
        assert_eq!(decode_config(&buf[..buf.len() - 1]), None, "truncated");
        let mut big = sample();
        big.voters = (0..9)
            .map(|i| WireMember { id: i, ip: 0, port: 0 })
            .collect();
        let mut b2 = Vec::new();
        encode_config(&big, &mut b2);
        assert_eq!(decode_config(&b2), None, "over MAX_MEMBERS refused at decode too");
    }
}
```

In `frame.rs` tests extend `frame_type_codes_are_stable`:

```rust
        assert_eq!(FRAME_TYPE_CONFIG, 4);
```

In `datagram.rs` tests (mirror `snap_begin_body_roundtrips_and_pins_layout`):

```rust
    #[test]
    fn config_proposal_and_reply_bodies_roundtrip_and_pin_layout() {
        let p = ConfigProposalBody { nonce: 7, op: 2, id: 5, ip: 0x0a000005, port: 19100 };
        let mut buf = [0u8; CONFIG_PROPOSAL_BODY_LEN];
        write_config_proposal_body(&mut buf, &p);
        assert_eq!(&buf[0..8], &7u64.to_le_bytes());
        assert_eq!(&buf[8..12], &2u32.to_le_bytes());
        assert_eq!(&buf[12..16], &5u32.to_le_bytes());
        assert_eq!(&buf[16..20], &0x0a000005u32.to_le_bytes());
        assert_eq!(&buf[20..22], &19100u16.to_le_bytes());
        assert_eq!(read_config_proposal_body(&buf), Some(p));

        let r = ConfigReplyBody { nonce: 7, status: 0, reason: 0, version: 4 };
        let mut buf = [0u8; CONFIG_REPLY_BODY_LEN];
        write_config_reply_body(&mut buf, &r);
        assert_eq!(&buf[0..8], &7u64.to_le_bytes());
        assert_eq!(&buf[16..24], &4u64.to_le_bytes());
        assert_eq!(read_config_reply_body(&buf), Some(r));
    }

    #[test]
    fn kind_codes_16_17_are_stable() {
        assert_eq!(DGRAM_KIND_CONFIG_PROPOSAL, 16);
        assert_eq!(DGRAM_KIND_CONFIG_REPLY, 17);
    }
```

In `cnc.rs` (uc_protocol) extend the offset-pin test:

```rust
        assert_eq!(CNC_OFF_CONFIG_VERSION, 3456);
        assert_eq!(CNC_OFF_CONFIG_PENDING, 3520);
        assert_eq!(CNC_OFF_ADMIN_REQ, 3584);
        assert_eq!(CNC_OFF_ADMIN_RESP, 3648);
```

- [ ] **Step 2: run to verify failures**

Run: `cargo test -p uc_protocol 2>&1 | tail -5`
Expected: compile errors — `FRAME_TYPE_CONFIG`, `config` module, kinds not defined.

- [ ] **Step 3: implement**

`uc_protocol/src/v2/config.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Cluster-config wire codec (M7, spec 2026-07-13): the payload of a
//! `FRAME_TYPE_CONFIG` frame, the snapshot-session config carry, and the
//! durable record's canonical byte form. Core-only: addresses are
//! `(ipv4 u32, port u16)` — `SocketAddr` conversion happens in `uc_node`.
//!
//! Layout (all LE):
//!   version u64 | prev_position u64 | n_voters u16 | n_learners u16 |
//!   n_tombstones u16 | voters[n]{id u32, ip u32, port u16} |
//!   learners[n]{..} | tombstones[n]{u32}

/// Hard cap: voters + learners (incl. transitional states) — the cnc
/// PeerSlots band has 8 slots. Enforced at proposal AND at decode.
pub const MAX_MEMBERS: usize = 8;

pub const CONFIG_FIXED_LEN: usize = 22;
pub const MEMBER_LEN: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireMember {
    pub id: u32,
    pub ip: u32,   // IPv4, network-order value stored as a plain u32
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireConfig {
    pub version: u64,
    /// Frame-end position of the PREDECESSOR config entry (0 for genesis) —
    /// audit trail; the durable record keeps the authoritative prev.
    pub prev_position: u64,
    pub voters: Vec<WireMember>,
    pub learners: Vec<WireMember>,
    pub tombstones: Vec<u32>,
}

pub fn encode_config(c: &WireConfig, out: &mut Vec<u8>) {
    out.extend_from_slice(&c.version.to_le_bytes());
    out.extend_from_slice(&c.prev_position.to_le_bytes());
    out.extend_from_slice(&(c.voters.len() as u16).to_le_bytes());
    out.extend_from_slice(&(c.learners.len() as u16).to_le_bytes());
    out.extend_from_slice(&(c.tombstones.len() as u16).to_le_bytes());
    for m in c.voters.iter().chain(c.learners.iter()) {
        out.extend_from_slice(&m.id.to_le_bytes());
        out.extend_from_slice(&m.ip.to_le_bytes());
        out.extend_from_slice(&m.port.to_le_bytes());
    }
    for t in &c.tombstones {
        out.extend_from_slice(&t.to_le_bytes());
    }
}

pub fn decode_config(buf: &[u8]) -> Option<WireConfig> {
    if buf.len() < CONFIG_FIXED_LEN {
        return None;
    }
    let version = u64::from_le_bytes(buf[0..8].try_into().ok()?);
    let prev_position = u64::from_le_bytes(buf[8..16].try_into().ok()?);
    let nv = u16::from_le_bytes(buf[16..18].try_into().ok()?) as usize;
    let nl = u16::from_le_bytes(buf[18..20].try_into().ok()?) as usize;
    let nt = u16::from_le_bytes(buf[20..22].try_into().ok()?) as usize;
    if nv + nl > MAX_MEMBERS {
        return None; // structural invalid — refuse at the wire, too
    }
    let need = CONFIG_FIXED_LEN + (nv + nl) * MEMBER_LEN + nt * 4;
    if buf.len() != need {
        return None;
    }
    let mut off = CONFIG_FIXED_LEN;
    let mut member = |off: &mut usize| -> WireMember {
        let id = u32::from_le_bytes(buf[*off..*off + 4].try_into().unwrap());
        let ip = u32::from_le_bytes(buf[*off + 4..*off + 8].try_into().unwrap());
        let port = u16::from_le_bytes(buf[*off + 8..*off + 10].try_into().unwrap());
        *off += MEMBER_LEN;
        WireMember { id, ip, port }
    };
    let voters = (0..nv).map(|_| member(&mut off)).collect();
    let learners = (0..nl).map(|_| member(&mut off)).collect();
    let mut tombstones = Vec::with_capacity(nt);
    for _ in 0..nt {
        tombstones.push(u32::from_le_bytes(buf[off..off + 4].try_into().ok()?));
        off += 4;
    }
    Some(WireConfig { version, prev_position, voters, learners, tombstones })
}
```

`frame.rs` — after `FRAME_TYPE_NEW_TERM`:

```rust
/// Cluster-config entry (M7, spec 2026-07-13): payload =
/// `v2::config::encode_config` bytes. Appended by a serving leader; adopted
/// at append (leader) / at durable recording (follower, archive scan).
/// Replicated/archived/replayed like any frame; the apply layer skips every
/// non-MESSAGE type, so services never see it.
pub const FRAME_TYPE_CONFIG: u8 = 4;
```

`datagram.rs` — kinds + bodies (constants after `DGRAM_KIND_SNAP_DONE`, bodies mirroring the SnapBegin codec):

```rust
/// M7: follower→leader forwarded membership proposal (`uc2ctl` wrote the
/// local admin slot on a non-leader). Body = `ConfigProposalBody`.
pub const DGRAM_KIND_CONFIG_PROPOSAL: u8 = 16;
/// M7: leader→follower reply for a forwarded proposal. Body = `ConfigReplyBody`.
pub const DGRAM_KIND_CONFIG_REPLY: u8 = 17;

pub const CONFIG_PROPOSAL_BODY_LEN: usize = 22;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigProposalBody {
    pub nonce: u64,
    /// `uc_consensus::config::ConfigOp` discriminant (1..=5).
    pub op: u32,
    pub id: u32,
    pub ip: u32,
    pub port: u16,
}

pub fn write_config_proposal_body(buf: &mut [u8], b: &ConfigProposalBody) {
    buf[0..8].copy_from_slice(&b.nonce.to_le_bytes());
    buf[8..12].copy_from_slice(&b.op.to_le_bytes());
    buf[12..16].copy_from_slice(&b.id.to_le_bytes());
    buf[16..20].copy_from_slice(&b.ip.to_le_bytes());
    buf[20..22].copy_from_slice(&b.port.to_le_bytes());
}

pub fn read_config_proposal_body(buf: &[u8]) -> Option<ConfigProposalBody> {
    if buf.len() < CONFIG_PROPOSAL_BODY_LEN {
        return None;
    }
    Some(ConfigProposalBody {
        nonce: u64::from_le_bytes(buf[0..8].try_into().ok()?),
        op: u32::from_le_bytes(buf[8..12].try_into().ok()?),
        id: u32::from_le_bytes(buf[12..16].try_into().ok()?),
        ip: u32::from_le_bytes(buf[16..20].try_into().ok()?),
        port: u16::from_le_bytes(buf[20..22].try_into().ok()?),
    })
}

pub const CONFIG_REPLY_BODY_LEN: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigReplyBody {
    pub nonce: u64,
    /// 0 = accepted; 1 = refused (see `reason`); 2 = retry (leader unknown/changed).
    pub status: u32,
    /// `uc_consensus::config::ProposeError` discriminant when refused.
    pub reason: u32,
    /// New config version when accepted; current version otherwise.
    pub version: u64,
}

pub fn write_config_reply_body(buf: &mut [u8], b: &ConfigReplyBody) {
    buf[0..8].copy_from_slice(&b.nonce.to_le_bytes());
    buf[8..12].copy_from_slice(&b.status.to_le_bytes());
    buf[12..16].copy_from_slice(&b.reason.to_le_bytes());
    buf[16..24].copy_from_slice(&b.version.to_le_bytes());
}

pub fn read_config_reply_body(buf: &[u8]) -> Option<ConfigReplyBody> {
    if buf.len() < CONFIG_REPLY_BODY_LEN {
        return None;
    }
    Some(ConfigReplyBody {
        nonce: u64::from_le_bytes(buf[0..8].try_into().ok()?),
        status: u32::from_le_bytes(buf[8..12].try_into().ok()?),
        reason: u32::from_le_bytes(buf[12..16].try_into().ok()?),
        version: u64::from_le_bytes(buf[16..24].try_into().ok()?),
    })
}
```

`SnapBeginBody` extension — add a trailing length-prefixed config blob WITHOUT moving existing offsets (`session@0..4, snapshot_pos@8..16, total_len@16..24` stay; new: `config_len u16 @24..26, config bytes @26..`). Update `SNAP_BEGIN_BODY_LEN` to a `SNAP_BEGIN_FIXED_LEN = 26` + document that the body is now variable-length; update `write_/read_snap_begin_body` to carry `config: Vec<u8>` and update the existing pin test's expectations for the two new bytes (the M6 pin asserts offsets of the FIXED fields — keep those assertions identical, add `config_len` assertions).

cnc constants (`uc_protocol/src/v2/cnc.rs`), after the PeerSlots block:

```rust
/// M7 — adopted cluster-config version. Writer: consensus agent.
pub const CNC_OFF_CONFIG_VERSION: usize = 3456;
/// M7 — 1 while a config change is uncommitted (pending), else 0.
/// Writer: consensus agent.
pub const CNC_OFF_CONFIG_PENDING: usize = 3520;
/// M7 — admin REQUEST line (writer: uc2ctl, same-host). seq u64 @+0 is the
/// commit word — the admin writes the fields, then seq, with release; the
/// consensus agent acts on seq > last-seen. Fields: nonce u64 @+8, op u32
/// @+16, id u32 @+20, ip u32 @+24, port u32 @+28.
pub const CNC_OFF_ADMIN_REQ: usize = 3584;
/// M7 — admin RESPONSE line (writer: consensus agent). seq u64 @+0 echoes the
/// request seq (written LAST, release); status u32 @+8, reason u32 @+12,
/// version u64 @+16.
pub const CNC_OFF_ADMIN_RESP: usize = 3648;

const _: () = assert!(CNC_OFF_ADMIN_RESP + 64 <= CNC_PAGE_LEN);
```

`version.rs`: bump the CURRENT protocol version's **minor** by 1 (same major). Keep `MIN_COMPATIBLE` unchanged unless it equals CURRENT — the compat rule (`same major, other.minor <= ours`) already makes v2.0 nodes refuse a v2.1 peer.

`uc_log/src/cnc.rs`: add accessors following the existing PeerSlot style — `config_version()/store_config_version(v)`, `config_pending()/store_config_pending(bool)`, `read_admin_req() -> Option<AdminReq>` (returns `None` unless seq > the caller-supplied last-seen; struct `AdminReq { seq: u64, nonce: u64, op: u32, id: u32, ip: u32, port: u16 }`), `write_admin_req(&AdminReq)` (fields then seq, release), `read_admin_resp(seq) -> Option<AdminResp>`, `write_admin_resp(&AdminResp)`. Extend the `cnc.rs` layout-pin test with the four new offsets.

- [ ] **Step 4: run tests**

Run: `cargo test -p uc_protocol && cargo test -p uc_log cnc`
Expected: PASS (incl. the extended pin tests).

- [ ] **Step 5: clippy + commit**

Run: `cargo clippy -p uc_protocol -p uc_log --all-targets -- -D warnings`

```bash
git add uc_protocol/src/v2/config.rs uc_protocol/src/v2/frame.rs uc_protocol/src/v2/datagram.rs \
        uc_protocol/src/v2/cnc.rs uc_protocol/src/v2/mod.rs uc_protocol/src/version.rs uc_log/src/cnc.rs
git commit -m "feat(uc_protocol,uc_log): M7 wire layer — CONFIG frame type, config codec, kinds 16/17, admin cnc band, version minor bump"
```

---

### Task 2: Durable `ConfigRecord` in `NodeState`

**Files:**
- Modify: `uc_log/src/state.rs`

**Interfaces:**
- Produces (serde structs for `StableValue`):

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMember { pub id: u32, pub ip: u32, pub port: u16 }

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredConfig {
    pub version: u64,
    pub voters: Vec<StoredMember>,
    pub learners: Vec<StoredMember>,
    pub tombstones: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigRecord {
    pub position: u64,      // frame-END effect point; 0 = genesis
    pub config: StoredConfig,
    pub prev_position: u64,
    pub prev: StoredConfig,
}
```

- Produces `NodeState` additions: field `config: StableValue<ConfigRecord>` (file `config.state`), cache slot, `pub fn config_record(&self) -> Option<ConfigRecord>`, `pub fn store_config_record(&self, r: &ConfigRecord) -> Result<(), StableValueError>` (durable on return — adoption and revert both depend on it).

- [ ] **Step 1: failing test**

```rust
    #[test]
    fn config_record_defaults_none_and_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let genesis = StoredConfig {
            version: 0,
            voters: vec![StoredMember { id: 1, ip: 0x0a000001, port: 19100 }],
            learners: vec![],
            tombstones: vec![],
        };
        {
            let s = NodeState::open(dir.path()).unwrap();
            assert_eq!(s.config_record(), None, "fresh dir: no record until the node seeds genesis");
            let r = ConfigRecord {
                position: 0,
                config: genesis.clone(),
                prev_position: 0,
                prev: genesis.clone(),
            };
            s.store_config_record(&r).unwrap();
            assert_eq!(s.config_record(), Some(r));
        }
        let s = NodeState::open(dir.path()).unwrap();
        let r = s.config_record().expect("survives reopen");
        assert_eq!(r.config, genesis);
        assert_eq!(r.position, 0);
    }
```

- [ ] **Step 2: run to verify failure** — `cargo test -p uc_log config_record` → compile error.

- [ ] **Step 3: implement** — add the structs above; add the `config` StableValue to `NodeState::open` (`dir.join("config.state")`); widen the cache tuple to include `Option<ConfigRecord>` (the module's single-lock pattern — extend, don't add a second Mutex); accessors mirror `store_snapshot_floor` (durable-on-return via `.wait().map_err(durability_error)`).

- [ ] **Step 4: run** — `cargo test -p uc_log state` → PASS (old tests untouched).

- [ ] **Step 5: commit**

```bash
git add uc_log/src/state.rs
git commit -m "feat(uc_log): durable ConfigRecord StableValue — cur+prev, one level (one-in-flight makes it sufficient)"
```

---

### Task 3: `uc_consensus` — `ClusterConfig`, `ConfigOp`, and dynamic membership in `ElectionSm`

**Files:**
- Create: `uc_consensus/src/config.rs`
- Modify: `uc_consensus/src/election.rs`
- Modify: `uc_consensus/src/lib.rs` (export `config`)

**Interfaces:**
- Produces `uc_consensus::config`:

```rust
pub type Addr = (u32, u16); // (ipv4, port) — SocketAddr conversion is uc_node's job

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    pub version: u64,
    pub voters: Vec<(NodeId, Addr)>,
    pub learners: Vec<(NodeId, Addr)>,
    pub tombstones: Vec<NodeId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigOp {
    AddLearner { id: NodeId, addr: Addr }, // wire op = 1
    PromoteLearner { id: NodeId },         // 2
    DemoteVoter { id: NodeId },            // 3
    RemoveLearner { id: NodeId },          // 4
    RemoveVoter { id: NodeId },            // 5
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProposeError {
    NotLeader,        // wire reason = 1
    NotServing,       // 2  (serving gate = the single-server-change precondition)
    ChangePending,    // 3  (one in flight)
    Tombstoned,       // 4
    AlreadyPresent,   // 5
    NotFound,         // 6
    WrongRole,        // 7  (promote a voter / demote a learner)
    ZeroVoters,       // 8
    TooManyMembers,   // 9  (MAX_MEMBERS = 8)
    NotCaughtUp { gap: u64 }, // 10
}

impl ClusterConfig {
    pub fn genesis(voters: Vec<(NodeId, Addr)>, learners: Vec<(NodeId, Addr)>) -> Self;
    /// PURE single-server transition — every proposal path shares it.
    pub fn apply(&self, op: ConfigOp) -> Result<ClusterConfig, ProposeError>;
    pub fn voter_ids(&self) -> Vec<NodeId>;
    pub fn is_voter(&self, id: NodeId) -> bool;
    pub fn is_learner(&self, id: NodeId) -> bool;
    pub fn contains(&self, id: NodeId) -> bool;   // voter or learner
    pub fn op_code(op: &ConfigOp) -> u32;         // wire discriminants above
    pub fn reason_code(e: &ProposeError) -> u32;
}
```

- Produces `ElectionSm` additions:
  - `Event::ConfigObserved { position: u64, config: ClusterConfig }` — frame-END position; fed by the leader append path AND the follower archive scan AND boot recovery. Idempotent for `config.version <= adopted version`. **Data-plane event: latched out during truncation** (config frames re-observed after resume), EXCEPT it is also how post-revert re-adoption happens — the latch allow-list is unchanged; re-observation lands after `Truncated` releases the latch.
  - `Action::ConfigAdopted { position: u64, config: ClusterConfig, prev_position: u64, prev: ClusterConfig }` — the node must: persist the `ConfigRecord`, rebuild the net layer (SetPeers), update cnc, and halt if excluded.
  - `Action::HaltRemoved` — this node is not in the adopted config (and not mid-self-removal-as-leader): fail-stop.
  - `pub fn propose_config(&mut self, op: ConfigOp) -> Result<ClusterConfig, ProposeError>` — leader-only, serving-only, one-in-flight; `PromoteLearner` additionally checks the learner's last report (see below). Returns the NEW config for the node to append; the SM does NOT adopt here — adoption follows the `ConfigObserved` fed back by the append path (single adoption path).
  - `pub fn config(&self) -> &ClusterConfig`, `pub fn config_position(&self) -> u64`, `pub fn config_pending(&self) -> bool` (`config_position > commit_seen`).
  - `pub fn last_report(&self, id: NodeId) -> Option<u64>` — from the carried-report cache (used by promote precondition + `uc2ctl status`).
  - Constructor change: `ElectionConfig` gains `pub config: ClusterConfig` and `pub config_position: u64` (recovered); `members`/`can_vote` become DERIVED (`members = config.voter_ids()`, `can_vote = config.is_voter(id)`) — keep the existing fields as private derived state so the 1250-line body stays unchanged except where noted. The old `ElectionConfig.members`/`can_vote` fields are REMOVED (callers migrate in Task 5; sim in Task 4; unit tests in this task).
  - Rebuild-at-boundary (private `fn adopt_config(&mut self, position: u64, cfg: ClusterConfig, out: &mut Vec<Action>)`): refresh derived `members`/`can_vote`; `self.tracker = CommitTracker::new(n'-1, n')`; re-feed carried reports for surviving members via `follower_slot`; leader keeps role (unless removed — see self-removal); follower stays follower. On self-exclusion: if leader and `config_pending()` (own removal in flight) → keep serving until commit (Task 8 handles the step-down); else emit `Action::HaltRemoved`.
  - Carried reports: new field `last_reports: Vec<(NodeId, u64)>` updated on every `Event::Report` (upsert max), pruned to config members at rebuild.

- [ ] **Step 1: failing unit tests** (in `config.rs` tests + `election.rs` tests)

```rust
    // config.rs
    #[test]
    fn apply_enforces_every_precondition() {
        let g = ClusterConfig::genesis(
            vec![(1, (1, 1)), (2, (2, 2)), (3, (3, 3))],
            vec![],
        );
        // add + promote + demote + remove round trip
        let c1 = g.apply(ConfigOp::AddLearner { id: 5, addr: (5, 5) }).unwrap();
        assert_eq!(c1.version, 1);
        assert!(c1.is_learner(5));
        let c2 = c1.apply(ConfigOp::PromoteLearner { id: 5 }).unwrap();
        assert!(c2.is_voter(5));
        let c3 = c2.apply(ConfigOp::DemoteVoter { id: 5 }).unwrap();
        assert!(c3.is_learner(5));
        let c4 = c3.apply(ConfigOp::RemoveLearner { id: 5 }).unwrap();
        assert!(!c4.contains(5));
        assert!(c4.tombstones.contains(&5));
        // tombstone permanence
        assert_eq!(
            c4.apply(ConfigOp::AddLearner { id: 5, addr: (5, 5) }),
            Err(ProposeError::Tombstoned)
        );
        // structural refusals
        assert_eq!(g.apply(ConfigOp::AddLearner { id: 1, addr: (9, 9) }), Err(ProposeError::AlreadyPresent));
        assert_eq!(g.apply(ConfigOp::PromoteLearner { id: 1 }), Err(ProposeError::WrongRole));
        assert_eq!(g.apply(ConfigOp::RemoveVoter { id: 9 }), Err(ProposeError::NotFound));
        let solo = ClusterConfig::genesis(vec![(1, (1, 1))], vec![]);
        assert_eq!(solo.apply(ConfigOp::RemoveVoter { id: 1 }), Err(ProposeError::ZeroVoters));
        assert_eq!(solo.apply(ConfigOp::DemoteVoter { id: 1 }), Err(ProposeError::ZeroVoters));
        // 8-cap
        let mut big = g.clone();
        for i in 10..15u32 {
            big = big.apply(ConfigOp::AddLearner { id: i, addr: (i, 1) }).unwrap();
        }
        assert_eq!(
            big.apply(ConfigOp::AddLearner { id: 20, addr: (20, 1) }),
            Err(ProposeError::TooManyMembers)
        );
    }

    // election.rs tests — house harness style (drive step() directly)
    #[test]
    fn adopt_config_rebuilds_tracker_and_carries_reports() {
        // 3 voters {1,2,3}, self=1 becomes leader, followers report; then adopt
        // v1 adding voter 5 → commit still advances with carried reports, and 5's
        // report starts counting only after it arrives.
        // ... (build via the existing make_leader test helper pattern; assert
        // commit advances identically pre/post adoption for the same reports,
        // and follower_slot(5) is Some after adoption.)
    }

    #[test]
    fn propose_refused_unless_serving_and_no_pending() {
        // candidate → NotLeader; leader pre-NewTerm-commit → NotServing;
        // leader with adopted-but-uncommitted config → ChangePending.
    }

    #[test]
    fn promote_requires_caught_up_learner() {
        // leader with commit=100_000, learner 5 last_report=10_000, slack=32_768
        // → Err(NotCaughtUp{gap: 57_232}); after Report{5, 90_000} → Ok.
    }

    #[test]
    fn config_observed_is_idempotent_and_monotone_by_version() {
        // feeding the same ConfigObserved twice = one adoption; a LOWER version
        // after a higher one is ignored.
    }

    #[test]
    fn removed_follower_emits_halt() {
        // follower 3 adopts a config without id 3 → Action::HaltRemoved.
    }

    #[test]
    fn nonmember_vote_and_report_stay_dropped_after_adoption() {
        // remove voter 3; its Report/Vote/RequestVote no longer influence
        // tracker/votes (extends the existing forged-report/membership tests).
    }
```

Write these as REAL tests following the existing `election.rs` test-module helpers (there are extensive tests below line 900 — reuse their construction pattern; the promote-slack test drives `propose_config(ConfigOp::PromoteLearner{..})` with an explicit `slack` argument — see Step 3 signature note).

- [ ] **Step 2: run to verify failures** — `cargo test -p uc_consensus` → compile errors.

- [ ] **Step 3: implement**

`config.rs`: the pure type + `apply` exactly as the Interfaces block; `apply` clones, bumps `version`, moves/inserts/removes, pushes tombstones on removals, checks in this order: tombstone → presence/role → zero-voters → cap.

`election.rs` changes, surgically:

1. `use crate::config::{ClusterConfig, ConfigOp, ProposeError};`
2. `ElectionConfig`: replace `pub members: Vec<NodeId>` + `pub can_vote: bool` with `pub config: ClusterConfig` + `pub config_position: u64`. In `new()`: `let members_ids = cfg.config.voter_ids(); let can_vote = cfg.config.is_voter(cfg.id);` then the existing asserts run against the derived values (learner-not-in-members holds by `ClusterConfig` construction). Store `config`, `config_position`, `last_reports: Vec::new()`.
3. `Event::Report` handler: before the leader-slot branch, upsert `last_reports` (`entry max durable`) for ANY current-term member report.
4. New `Event::ConfigObserved { position, config }` arm (data-plane — the truncating latch in `step()` already drops it since it's not in the allow-list):

```rust
            Event::ConfigObserved { position, config } => {
                if config.version <= self.config.version {
                    return; // idempotent / stale re-observation
                }
                self.adopt_config(position, config, out);
            }
```

5. `adopt_config` (private):

```rust
    fn adopt_config(&mut self, position: u64, cfg: ClusterConfig, out: &mut Vec<Action>) {
        let prev = std::mem::replace(&mut self.config, cfg);
        let prev_position = self.config_position;
        self.config_position = position;
        self.members = self.config.voter_ids();
        self.can_vote = self.config.is_voter(self.id);
        // Rebuild-at-boundary: fresh tracker sized to the new voting set, then
        // re-feed the last durable report per SURVIVING member so commit ranking
        // does not restart from zero. Commit itself is monotonic in the cnc
        // counter — a rebuild can only pause, never regress it.
        let n = self.members.len();
        self.tracker = CommitTracker::new(n - 1, n);
        self.last_reports.retain(|(id, _)| self.members.contains(id) && *id != self.id);
        for &(id, durable) in &self.last_reports {
            if let Some(slot) = self.follower_slot(id) {
                self.tracker.on_durable(slot, durable);
            }
        }
        out.push(Action::ConfigAdopted {
            position,
            config: self.config.clone(),
            prev_position,
            prev: prev.clone(),
        });
        // Self-exclusion: a follower halts now; a leader removing itself keeps
        // serving until this entry COMMITS (the node executes the step-down —
        // see `self_removal_committed`), because C_new must be replicated by a
        // leader that still exists.
        if !self.config.contains(self.id) && !matches!(self.role, Role::Leader) {
            out.push(Action::HaltRemoved);
        }
    }
```

6. `propose_config`:

```rust
    /// Leader-only membership proposal (M7). `slack` = max catch-up gap a
    /// learner may have and still be promoted (the node passes its admission
    /// window). Returns the NEW config for the node to append as a
    /// FRAME_TYPE_CONFIG frame; adoption happens via the ConfigObserved the
    /// append path feeds back — one adoption path for leader and follower.
    pub fn propose_config(
        &mut self,
        op: ConfigOp,
        slack: u64,
    ) -> Result<ClusterConfig, ProposeError> {
        if !matches!(self.role, Role::Leader) {
            return Err(ProposeError::NotLeader);
        }
        if !self.serving {
            return Err(ProposeError::NotServing); // the single-server-change precondition
        }
        if self.config_pending() {
            return Err(ProposeError::ChangePending); // one in flight
        }
        if let ConfigOp::PromoteLearner { id } = op {
            let reported = self
                .last_reports
                .iter()
                .find(|(rid, _)| *rid == id)
                .map(|(_, d)| *d)
                .unwrap_or(0);
            let target = self.commit_seen.saturating_sub(slack);
            if reported < target {
                return Err(ProposeError::NotCaughtUp { gap: target - reported });
            }
        }
        self.config.apply(op)
    }

    pub fn config_pending(&self) -> bool {
        self.config_position > self.commit_seen
    }
```

7. `become_leader`: after `tracker.reset_reports()`, ALSO clear `last_reports` (stale-term reports must not certify promote preconditions either).

- [ ] **Step 4: run** — `cargo test -p uc_consensus` → all PASS (old + new).

- [ ] **Step 5: clippy + commit**

```bash
cargo clippy -p uc_consensus --all-targets -- -D warnings
git add uc_consensus/src/config.rs uc_consensus/src/election.rs uc_consensus/src/lib.rs
git commit -m "feat(uc_consensus): ClusterConfig + ConfigOp + dynamic membership — ConfigObserved adoption, rebuild-at-boundary w/ carried reports, propose_config behind the serving gate"
```

Note: `uc_node` and `uc_sim` will NOT compile until Tasks 4–5 migrate them off `ElectionConfig.members`. That is expected mid-branch; each task's own crate tests gate it. Do NOT run `--workspace` gates between Tasks 3 and 5.

---

### Task 4: Sim — config frames, inv6–9, counterfactual-red pins, fuzz arm

**Files:**
- Modify: `uc_sim/src/world.rs`
- Modify: `uc_sim/src/invariants.rs`
- Modify: `uc_sim/tests/scenarios.rs`

**Interfaces:**
- Consumes: Task 3's `Event::ConfigObserved`, `Action::{ConfigAdopted, HaltRemoved}`, `propose_config(op, slack)`, `ClusterConfig`, `ConfigOp`, `ProposeError`.
- Produces (for scenario tests): `World::propose_config(node: usize, op: ConfigOp) -> Result<u64, ProposeError>` (returns the new version; models the leader append: enqueues the config frame into the leader's stream so followers observe it at durable), `World::node_config_version(node: usize) -> u64`, `World::halted_removed(node: usize) -> bool`, `SimConfig` gains `initial_learners: Vec<usize>` and `serving_gate_disabled: bool` (counterfactual, default false), `revert_on_truncate_disabled: bool` (counterfactual, default false).

- [ ] **Step 1: migrate the sim's SM construction** to `ElectionConfig { config: ClusterConfig::genesis(...), config_position: 0, .. }` (all nodes get synthetic addrs `(node_idx as u32, 1)` — the sim never opens sockets). Run `cargo test -p uc_sim` → existing scenarios must stay green BEFORE any new modeling (pure migration commit).

```bash
git add uc_sim/src/world.rs
git commit -m "refactor(uc_sim): migrate to ClusterConfig-based ElectionConfig (pure migration, scenarios green)"
```

- [ ] **Step 2: model config frames mechanically.** In `world.rs`: a config frame is data occupying positions in the modeled stream (like the NewTerm modeling); a follower node feeds `Event::ConfigObserved{position: frame_end, config}` when its durable crosses the frame end; the leader feeds it at append. `Action::ConfigAdopted` in the sim's action handler: record the node's `(version, position, prev)` in per-node sim state (the sim's mirror of the durable record); on `Action::Truncate` exec, if `to < config_position` and `!revert_on_truncate_disabled` → revert the mirror to prev (the sim models the NODE's obligation). `Action::HaltRemoved` → mark node halted (it stops being scheduled, like `crash` but permanent).

- [ ] **Step 3: failing invariant tests (inv6–9), then implement the checks** in `invariants.rs`:

```rust
    // inv6 — config determinism: node's adopted config version == the version
    //        implied by its durable frontier (recompute from the injected
    //        frame ledger the World keeps).
    // inv7 — quorum legality: every AdvanceCommit the checker sees must be
    //        certified by reports from a quorum of the ADOPTING node's config
    //        at that position (extend on_advance_commit with the config ledger).
    // inv8 — revert correctness: after on_truncate, adopted version ==
    //        version-at-frontier (checked in the same hook).
    // inv9 — tombstone permanence: no adopted config ever re-lists a
    //        tombstoned id (checked on every adoption).
```

Each invariant gets a unit test in `invariants.rs` proving it CATCHES a violation (the `inv2_catches_*` pattern — construct the bad state by hand, assert `Err`).

- [ ] **Step 4: scenario tests** in `scenarios.rs`:

```rust
    #[test]
    fn add_promote_demote_remove_cycle_under_faults() { /* 5-node world, run the
        full cycle with crashes+partitions between steps; all invariants green;
        halted_removed(removed) eventually true */ }

    #[test]
    fn propose_during_pending_is_refused() { /* second propose_config before the
        first commits → Err(ChangePending) */ }

    #[test]
    fn truncation_below_config_frame_reverts() { /* partition the leader after
        it appends a config frame replicated to a minority; new leader elected
        without it; heal; the minority truncates → reverted version; then
        re-adopts whatever the new leader's stream carries; inv6/inv8 green */ }

    #[test]
    fn counterfactual_no_serving_gate_produces_disjoint_quorum_commit() {
        /* serving_gate_disabled=true + a crafted seed: leader of C_old crashes
           right after proposing; a new leader proposes immediately (gate off)
           → the checker must catch an inv7 violation. Assert Err — this test
           proves the precondition is load-bearing (counterfactual-red). */
    }

    #[test]
    fn counterfactual_no_revert_breaks_inv8() {
        /* revert_on_truncate_disabled=true + the truncation scenario above →
           assert the run returns an inv8 violation. */
    }

    #[cfg(feature = "sim-heavy")]
    #[test]
    fn fuzz_heavy_config_churn() { /* 1000 seeds: random legal + illegal ops
        (illegal must be refused, never adopted) under crash/partition/
        truncation churn; all invariants green on every seed */ }
```

- [ ] **Step 5: run** — `cargo test -p uc_sim && cargo test -p uc_sim --features sim-heavy` → PASS.

- [ ] **Step 6: clippy + commit**

```bash
cargo clippy -p uc_sim --all-targets -- -D warnings
git add uc_sim/src/world.rs uc_sim/src/invariants.rs uc_sim/tests/scenarios.rs
git commit -m "feat(uc_sim): config-change modeling — inv6-9, counterfactual-red serving-gate + revert pins, config-churn fuzz arm"
```

---

### Task 5: Node wiring — leader append, follower scan adoption, rebuild, cnc publish

**Files:**
- Modify: `uc_log/src/buffer.rs` (add `append_config`)
- Modify: `uc_log/src/archive.rs` (config observations in the frame scan)
- Modify: `uc_net/src/sender.rs` (`CtrlMsg::SetPeers` + fan-out/flow rebuild)
- Modify: `uc_node/src/node.rs` (genesis seed, `ConfigObserved` feed, `exec` arms, conversions)

**Interfaces:**
- Consumes: Task 1 codecs, Task 2 `ConfigRecord`, Task 3 events/actions.
- Produces: `Appender::append_config(&mut self, term: u32, payload: &[u8]) -> Result<u64, AppendError>` (mirror `append_new_term` :427 with a payload; frame type CONFIG, returns frame-END position).
- Produces: `Archive::take_config_observations(&mut self) -> Vec<(u64, Vec<u8>)>` — `(frame_end_position, payload_bytes)` for every CONFIG frame durably recorded since last call (detected in the same scan loop as term stamping :270–290; store the payload slice copy).
- Produces: `CtrlMsg::SetPeers { followers: Vec<SocketAddr>, learners: Vec<SocketAddr>, cluster_size: usize }` — sender swaps fan-out lists, rebuilds `FlowControl::new(...)` re-feeding the last `Status` per surviving addr (keep a `last_status: HashMap<SocketAddr,(u64,u32)>` in the sender loop), and calls the existing `set_peer_slots` refresh.
- Produces in `uc_node`: `fn wire_to_cluster_config(w: &WireConfig) -> ClusterConfig`, `fn cluster_to_wire(c: &ClusterConfig, prev_position: u64) -> WireConfig`, `fn addr_of((ip, port): Addr) -> SocketAddr` (Ipv4).

- [ ] **Step 1: `append_config` + failing buffer test** (mirror the `append_new_term` test in `buffer.rs`: append a config frame, read it back via the reader, assert type/term/payload).

- [ ] **Step 2: archive scan + failing test** — record a block containing a CONFIG frame; `take_config_observations()` returns `[(frame_end, payload)]`; a PADDING/MESSAGE-only block returns `[]`. Implement inside the existing scan loop:

```rust
            if h.frame_type == FRAME_TYPE_CONFIG {
                let payload_start = off + HEADER_LEN;
                let payload_end = off + h.length as usize;
                self.config_observations.push((
                    base + off as u64 + align_frame_len(h.length as usize) as u64,
                    block[payload_start..payload_end].to_vec(),
                ));
            }
```

(frame-END = aligned end — the effect point; matches `ConfigRecord.position` semantics.)

- [ ] **Step 3: `CtrlMsg::SetPeers` + sender test** — existing sender unit-test style: build a sender with 2 followers, send `SetPeers` adding a third, assert the fan-out list length + flow limit re-derives from re-fed Status.

- [ ] **Step 4: node wiring.** In `uc_node/src/node.rs`:
  1. **Genesis seed** (construction, after `NodeState::open`): if `state.config_record()` is `None`, build genesis from `cfg.members`/`cfg.learners` (version 0, position 0) and `store_config_record`. Then construct `ElectionSm` from the RECORD (not the raw NodeConfig): `ElectionConfig { config: record_to_cluster(&rec.config), config_position: rec.position, .. }`. `NodeConfig.members`/`learners` keep their meaning as the SEED (doc update on the fields: "seed config — authoritative only for a fresh instance dir; the durable ConfigRecord + stream own it afterwards").
  2. **do_work step 1c** (after the existing 1b term-observation drain): drain `archive.take_config_observations()` → decode → `self.feed(Event::ConfigObserved { position, config })`; a decode failure is fail-stop (`panic!("corrupt CONFIG frame at {position}")` — CRC-covered bytes, so this is a bug, not line noise).
  3. **Leader append path**: a new `fn append_config_frame(&mut self, new_cfg: &ClusterConfig) -> u64` — encode via `cluster_to_wire(new_cfg, self.sm.config_position())`, `appender.append_config(term, &bytes)`, then feed `Event::ConfigObserved { position, config: new_cfg.clone() }` (adopt-at-append; the archive will re-observe it durably — idempotent by version).
  4. **`exec` arm**:

```rust
            Action::ConfigAdopted { position, config, prev_position, prev } => {
                // Persist BEFORE any behavioral change (crash between persist
                // and rebuild = recovery re-adopts from the record: safe).
                let rec = ConfigRecord {
                    position,
                    config: cluster_to_stored(&config),
                    prev_position,
                    prev: cluster_to_stored(&prev),
                };
                self.state.store_config_record(&rec).expect("config persist fail-stop");
                // Rebuild the net layer.
                let followers: Vec<SocketAddr> = config.voters.iter()
                    .filter(|(id, _)| *id != self.id)
                    .map(|(_, a)| addr_of(*a)).collect();
                let learners: Vec<SocketAddr> = config.learners.iter()
                    .filter(|(id, _)| *id != self.id)
                    .map(|(_, a)| addr_of(*a)).collect();
                let _ = self.sender_ctrl.send(CtrlMsg::SetPeers {
                    followers: followers.clone(),
                    learners,
                    cluster_size: config.voters.len(),
                });
                // Refresh the node's own routing + observability.
                self.rebuild_peer_maps(&config);      // id_to_addr / addr_to_id / peers / learner_ids
                self.publish_peer_band();             // slots reassign
                self.cnc.store_config_version(config.version);
                self.cnc.store_config_pending(true);  // cleared when commit crosses `position` (step 6 below)
            }
            Action::HaltRemoved => {
                eprintln!("node {}: removed from cluster config — halting", self.id);
                self.halt_removed = true;             // do_work returns; agent parks/exits
            }
```

  5. **Pending flag maintenance** (do_work, with the commit poll): when `commit >= sm.config_position()` and pending flag set → `store_config_pending(false)`.
  6. `rebuild_peer_maps` — extract the construction block at :431–446 into a method used by both construction and adoption.

- [ ] **Step 5: migrate remaining `ElectionConfig` construction sites** (node.rs tests, `m4_gate`/`m5_gate`/`m6_gate` examples construct nodes via `NodeConfig` only — verify with `cargo build -p uc_node --all-targets`).

- [ ] **Step 6: run** — `cargo test -p uc_log -p uc_net -p uc_node --lib` then `cargo test -p uc_node --test smoke --test failover --test learner --test purge_safety --test query_barrier` → all green (no behavior change for static clusters: genesis record == old static wiring).

- [ ] **Step 7: clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add uc_log/src/buffer.rs uc_log/src/archive.rs uc_net/src/sender.rs uc_node/src/node.rs
git commit -m "feat(uc2): config adoption wiring — append_config, archive config-scan, CtrlMsg::SetPeers rebuild, persist-then-rebuild exec, cnc publish"
```

---

### Task 6: Truncation revert + boot re-adoption + snapshot config carry

**Files:**
- Modify: `uc_node/src/node.rs` (`exec` `Action::Truncate` arm :1867, boot recovery, snapshot install path)
- Modify: `uc_net/src/sender.rs` + `uc_net/src/receiver.rs` (SNAP_BEGIN config carry)
- Modify: `uc_consensus/src/election.rs` (`adopt_snapshot_config`)

**Interfaces:**
- Consumes: `ConfigRecord` (Task 2), `SnapBeginBody.config` (Task 1).
- Produces: `ElectionSm::adopt_snapshot_config(&mut self, position: u64, config: ClusterConfig)` — config-by-fiat on snapshot install (mirror of `adopt_snapshot_lineage` :556: same authority argument, same gating by the node on `durable < floor`); sets cur=prev=config, positions = (floor, floor), rebuilds derived state, NO `ConfigAdopted` action (the node does the persist+rebuild inline on the install path).

- [ ] **Step 1: failing SM test** — a follower with config v2 at position P; `Truncated{epoch, to}` with `to < P` after the node reverted the record… **note the division of labor:** the SM's mirror must also revert. Add to the SM: on `Event::Truncated` (matching epoch), if `to < self.config_position` → revert `self.config`/`config_position` to the prev the SM retains (add `prev_config: ClusterConfig, prev_position: u64` fields maintained by `adopt_config`), refresh derived members/can_vote, rebuild tracker (empty reports — post-truncation reports re-arrive), and emit `Action::ConfigAdopted` with the REVERTED config so the node persists + rebuilds through the one existing path. Test: adopt v1 then feed matching `Truncated{to: below}` → config back to v0 + `ConfigAdopted{v0}` emitted; `to == position` → no revert.

- [ ] **Step 2: node ordering test + implementation.** In `exec` `Action::Truncate` (:1867), BEFORE calling `archive.truncate_to`: if `to < state.config_record().map_or(0, |r| r.position)` → build the reverted record (`prev` promoted to cur, `prev` duplicated) and `store_config_record` — **persist-revert-before-truncate**. (The SM's own revert on the `Truncated` ack then re-emits `ConfigAdopted`, whose persist is an idempotent overwrite of the same record.) Wipe special case: when `to == 0`, persist `{position: 0, config: CURRENT, prev: CURRENT, prev_position: 0}` (config-by-fiat — global constraint) instead of promoting prev. Integration-style unit test in node.rs tests: craft the M5-style truncation exec and assert record state across it.

- [ ] **Step 3: boot re-adoption test + implementation.** Recovery already replays archive scans for term observations; ensure the SAME boot path drains `take_config_observations` from the recovery scan so a config frame above `record.position` re-adopts (idempotent by version). Test: write config frame → crash before persist (simulate: build node A, append+adopt, copy the instance dir BEFORE the record persist by constructing the record-less state manually) — simpler deterministic form: delete `config.state` from a stopped instance dir, reboot, assert the record is rebuilt from the journal scan to the same version.

- [ ] **Step 4: snapshot carry.** Sender: populate `SnapBeginBody.config` from the node-provided snapshot source (extend `SnapshotSource` to `Option<(u64, PathBuf, u64, Vec<u8>)>` — the encoded config at/below the floor, produced from the CURRENT record; the leader's committed config is ≥ any config the floor could imply, and adopt-by-version idempotence makes over-delivery safe). Receiver: on `SNAP_BEGIN`, stash the config bytes with the session; on install completion the node decodes + calls `adopt_snapshot_config(floor, cfg)` + persists the fiat record. Extend the existing M6 learner below-floor test (`uc_node/tests/learner.rs` snapshot-session case) to assert the joiner's `config_version` equals the leader's after install.

- [ ] **Step 5: run** — `cargo test -p uc_consensus -p uc_net && cargo test -p uc_node --test learner --test failover` → green.

- [ ] **Step 6: clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add uc_consensus/src/election.rs uc_node/src/node.rs uc_net/src/sender.rs uc_net/src/receiver.rs
git commit -m "feat(uc2): config truncation-revert (persist-revert-before-truncate), boot re-adoption from journal scan, snapshot-session config carry"
```

---

### Task 7: Admin path — cnc slots, node poll, kinds 16/17 forwarding, `uc2ctl`

**Files:**
- Create: `uc_node/examples/uc2ctl.rs`
- Modify: `uc_node/src/node.rs` (do_work step 11: admin poll; `feed_net` arms for kinds 16/17)
- Modify: `uc_net/src/receiver.rs` (`NetEvent::{ConfigProposal, ConfigReply}` + `kind_idx` + demux)

**Interfaces:**
- Consumes: Task 1 admin cnc accessors + body codecs; Task 3 `propose_config`.
- Produces: `NetEvent::ConfigProposal { from: SocketAddr, body: ConfigProposalBody }`, `NetEvent::ConfigReply { body: ConfigReplyBody }`.
- Produces: `uc2ctl` subcommands: `add-learner --id N --addr IP:PORT`, `promote --id N`, `demote --id N`, `remove-learner --id N`, `remove-voter --id N`, `status` — all take `--instance-dir D --app-id A`; mutating commands write the admin request line (fresh seq = old seq + 1, random nonce), poll the response line for the echoed seq (timeout 10 s), print accepted/refused + reason string, **exit 1 on refusal/timeout**. `status` decodes config version/pending, PeerSlots (id/role/reported durable), leader flags — and prints a **staleness warning** per member whose reported durable is > one admission window behind commit ("removing a live voter while nodeX is dark leaves you stalled") — warn, never block.

- [ ] **Step 1: receiver demux + failing test** — extend the receiver's kind demux (the READ_PROBE pattern) to surface kinds 16/17 as `NetEvent`s; unit test: feed the raw datagram bytes, assert the event. Extend `NET_EVENT_KINDS`/`kind_idx` (positional observability array — append, never reorder).

- [ ] **Step 2: node admin poll + forwarding.** do_work step 11 (after step 10):

```rust
        // 11. Admin slot (M7): at most one request per cycle. Leader: propose +
        // append + reply. Follower: forward to the leader hint as kind 16; the
        // reply (kind 17) is written back to the response line when it arrives.
        if let Some(req) = self.cnc.read_admin_req(self.last_admin_seq) {
            self.last_admin_seq = req.seq;
            self.handle_admin(req);
        }
```

`handle_admin`: decode op → if leader: `sm.propose_config(op, self.admission_bytes)`; on Ok: `append_config_frame` + reply `{status: 0, version}` to the response line; on Err: reply `{status: 1, reason}`. If follower with a leader hint: remember `(seq, nonce)` in a 1-slot pending map and `send(leader_addr, DGRAM_KIND_CONFIG_PROPOSAL, ..)`; on `NetEvent::ConfigReply` matching the pending nonce → write the response line. No hint → reply `{status: 2 /*retry*/}`. Leader side `NetEvent::ConfigProposal`: same propose path, reply via kind 17 to `from`. **Nonce dedup:** leader keeps the last `(nonce, reply)` and re-sends the stored reply on a repeat nonce (retry-idempotent while the change is pending).

- [ ] **Step 3: `uc2ctl`** — clap bin in `uc_node/examples/uc2ctl.rs` (house style: `m6_gate probe` — `CncPage::open_file`, app_id check). Mutating flow: read current req seq, write fields+seq (the Task 1 accessor enforces field-then-seq/release), poll response ≤10 s.

- [ ] **Step 4: integration test** (in `uc_node/tests/reconfig.rs`, started here, extended in Task 9): 3-voter loopback cluster via `spawn_cluster_ring`; write an `add-learner` request into the LEADER's cnc via `CncPage` directly (the uc2ctl codepath minus the bin), assert: response accepted, config_version becomes 1 on all three nodes within a deadline, PeerSlots show the learner id with role=learner. Repeat via a FOLLOWER's cnc → forwarded, same outcome.

- [ ] **Step 5: run** — `cargo test -p uc_net && cargo test -p uc_node --test reconfig` → green. Build the bin: `cargo build -p uc_node --example uc2ctl`.

- [ ] **Step 6: clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add uc_net/src/receiver.rs uc_node/src/node.rs uc_node/examples/uc2ctl.rs uc_node/tests/reconfig.rs
git commit -m "feat(uc2): admin path — cnc request/response slots, leader propose+append, follower forwarding (kinds 16/17), uc2ctl CLI"
```

---

### Task 8: Leader self-removal, removed halt, joining-node bootstrap

**Files:**
- Modify: `uc_consensus/src/election.rs` (self-removal completion)
- Modify: `uc_node/src/node.rs` (halt semantics; join docs)
- Modify: `uc_node/tests/reconfig.rs`

**Interfaces:**
- Produces: `Action::StepDownRemoved` — emitted by the SM when (a) it is leader, (b) its adopted config excludes it, and (c) commit has crossed `config_position` (the entry is committed). Node exec: log, stop leading (feed nothing — just halt the process the same way `HaltRemoved` does; the remaining voters elect normally).

- [ ] **Step 1: failing SM test** — leader proposes `RemoveVoter{self}`; adoption keeps it leading (`can_serve()` still true, no Halt); feed reports until commit crosses the config frame → `Action::StepDownRemoved` emitted exactly once.

- [ ] **Step 2: implement** — in `rank_leader` (where commit advances), after the serving-gate check: if `!self.config.contains(self.id)` and `self.commit_seen >= self.config_position` → emit `Action::StepDownRemoved` once (guard with a `stepped_down: bool`). Note the tracker at this point ranks C_new followers only (rebuild already excluded self from... **careful**: `CommitTracker::new(n-1, n)` assumes self occupies a slot-exempt position; when self is NOT in members, followers = ALL n voters. In `adopt_config`, size the tracker `CommitTracker::new(n_followers, n)` where `n_followers = members minus self if self is a member else members`; `follower_slot` already skips only `m == self.id`, which is absent — verify with a unit test that commit advances via a quorum of C_new while the removed leader still appends.)

- [ ] **Step 3: node halt semantics** — `Action::{HaltRemoved, StepDownRemoved}` both set `halt_removed = true`; the agent loop exits do_work permanently, `publish_status` clears CAN_SERVE/LEADER flags, the process stays up but inert (fail-stop posture; the integration test asserts the flags drop and a new leader emerges).

- [ ] **Step 4: integration tests** (reconfig.rs):
  - `leader_self_removal_hands_off`: 3 voters under a write load thread; remove the leader via its own admin slot; assert: change accepted, old leader's cnc drops LEADER+CAN_SERVE, a new leader serves, committed writes never regress (read the loadclient-style monotonic guard from m6_gate's pattern), total gap bounded (< 5 s in-process).
  - `removed_follower_halts_and_zombie_cannot_disrupt`: remove a live follower; assert it halts; keep its socket sending forged Votes/Reports at the cluster (reuse the forged-report test technique from node.rs tests) → terms don't inflate (`current_term` stable across 2 s).
  - `joining_node_boots_from_stale_seed`: start a 3-voter cluster, run add-learner for id 5, THEN boot node 5 with a seed listing only the ORIGINAL 3 voters (stale seed) → it must adopt v1 from the stream and appear in peer bands; then promote it and assert quorum works with 4 voters (kill one other voter; cluster still commits).

- [ ] **Step 5: run** — `cargo test -p uc_consensus && cargo test -p uc_node --test reconfig` → green.

- [ ] **Step 6: clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add uc_consensus/src/election.rs uc_node/src/node.rs uc_node/tests/reconfig.rs
git commit -m "feat(uc2): leader self-removal (step-down after commit), removed-node fail-stop halt, stale-seed join bootstrap"
```

---

### Task 9: Integration suite — the remaining spec §9.3 scenarios

**Files:**
- Modify: `uc_node/tests/reconfig.rs`

**Interfaces:** consumes everything above; uses `failover.rs` helpers (`spawn_cluster_ring`, `NodeH::crash/restart`, `make_config_ring`).

- [ ] **Step 1: write the remaining scenarios as failing/green tests:**
  - `full_replace_a_box_recipe`: add-learner → wait catch-up (poll PeerSlot reported durable) → promote → remove-voter(dead one crashed first) → cluster of 3 new-set voters commits; every intermediate version visible in `config_version`.
  - `resize_3_to_5_to_3`: two add+promote pairs; then two demote+remove pairs; commit advances throughout; final voter set = original 3.
  - `every_refusal_surfaces`: during-pending (propose twice fast), not-caught-up (promote immediately after add under load), tombstone reuse (remove then re-add same id), zero-voters (remove down to 1 then try again — wait: min is ≥1 voter, removing the last voter refused), promote-a-voter (WrongRole), 9th member (TooManyMembers). Assert exact `reason` codes on the reply line.
  - `truncation_revert_e2e`: craft the divergent-leader shape from `failover.rs`'s heal-with-truncation test, with the divergent tail CONTAINING a config frame (partition leader appends config to a minority, majority elects, heals) → the minority node truncates, reverts, then re-adopts the majority's config; assert `config_version` + journal record consistency.
  - `crash_mid_pending_recovers`: SIGKILL-free in-process variant — `NodeH::crash()` a follower after the leader appends a config but before it commits; restart it; it re-adopts from journal/stream; version converges.

- [ ] **Step 2: run** — `cargo test -p uc_node --test reconfig -- --test-threads=1` (cluster tests serialize by convention) → all green.

- [ ] **Step 3: commit**

```bash
git add uc_node/tests/reconfig.rs
git commit -m "test(uc2): reconfig integration suite — replace recipe, resize 3-5-3, refusal matrix, truncation-revert e2e, crash-mid-pending"
```

---

### Task 10: L3 lincheck fault arm + crashtest scenario

**Files:**
- Modify: `uc_node/tests/lincheck_v2/mod.rs` (config-op driver + spare-node pool)
- Modify: `uc_node/tests/lin_v2.rs` (4th capstone)
- Modify: `examples/uc_crashtest/tests/hard_crash.rs` (mid-config-window SIGKILL)

**Interfaces:** consumes the admin path (drive ops via the leader's cnc like Task 7's test); `uc_lincheck` checker/history/model UNCHANGED (the invariant of every milestone).

- [ ] **Step 1: harness support** — `ClusterCfg` gains `spare_node: bool`; `LinClusterV2` gains `fn random_config_op(&mut self, rng: &mut StdRng) -> bool` cycling a spare id through add-learner → promote → demote → remove-learner (each step only when the previous committed — poll `config_pending`), returning whether an op was submitted; counter `config_ops_committed`.

- [ ] **Step 2: the capstone** in `lin_v2.rs`:

```rust
#[test]
fn linearizable_under_reconfig_churn() {
    // Failover-capstone workers + WGL oracle; fault mix gains a 4th arm:
    // 1-in-4 → random_config_op (the others: kill leader / crash follower
    // service / partition). Seeds 0x1107 / 7 / 99, budget <= 120 s/seed.
    // NON-VACUITY: assert!(cluster.config_ops_committed >= 3,
    //   "vacuous: reconfig churn never actually reconfigured");
}
```

- [ ] **Step 3: crashtest** — extend `hard_crash.rs` with `sigkill_mid_config_window`: drive an add-learner via the leader's cnc, SIGKILL the leader NODE process between append and commit (tight timing loop — accept whichever side the race lands on, both are valid histories), restart, assert: cluster converges to ONE of {v0, v1} on all nodes (never a mix after convergence), history stays Linearizable.

- [ ] **Step 4: run** — `cargo test -p uc_node --test lin_v2 linearizable_under_reconfig_churn` (then the full lin_v2), `cargo test -p uc_crashtest --features hard-crash-tests` → green.

- [ ] **Step 5: commit**

```bash
git add uc_node/tests/lincheck_v2/mod.rs uc_node/tests/lin_v2.rs examples/uc_crashtest/tests/hard_crash.rs
git commit -m "test(uc2): L3 reconfig-churn capstone (non-vacuity >=3 committed ops) + SIGKILL-mid-config-window crashtest"
```

---

### Task 11: Gate harness, fleet orchestrator, docs

**Files:**
- Create: `uc_node/examples/m7_gate.rs` (m6_gate pattern: `node|service|learner|probe|loadclient` roles + in-process `all` smoke; probe JSON gains `config_version`/`config_pending`)
- Modify: `bench-infra/scripts/m6_fleet_gate.py` → add `--m7` scenarios (or `m7_fleet_gate.py` importing its host classes — prefer extending: the host classes, fs-guard, and loadclient logic are shared)
- Create: `docs/benchmarks/uc2-m7-gate-2026-07-XX.md` (correctness proofs + smoke verdict + fleet placeholder, M6 doc structure)
- Modify: `docs/ops/uc2-runbook.md` §6 (full ops table: the five ops, recipes, staleness warning, upgrade order, halt/decommission)
- Modify: `README.md` + `CLAUDE.md` (scope lines: single-server reconfig shipped; 8-member cap; protocol minor bump)

**Interfaces:** consumes `uc2ctl`-equivalent admin writes (the orchestrator drives ops via `m7_gate ctl ...` subcommand or ssh'd `uc2ctl`), `probe` fields.

- [ ] **Step 1: `m7_gate` in-process `all` smoke** — 3 voters + 1 spare host slot + services + background load (m6_gate's driver): scenario A = full replace-a-box cycle; scenario B = resize 3→5→3; scenario C = leader self-removal. PASS criteria in-process: all transitions commit, reads monotonic, no quorum stall > 5 s; commit-dip printed as informational (fleet-only gate, M6 precedent).

- [ ] **Step 2: orchestrator scenarios** (fleet thresholds from the spec): `scenario_replace_a_box` (dip < 10 % across each transition window, measured over fixed 5 s windows like M6's learner-join; zero loadclient divergence), `scenario_resize_3_5_3` (same bars), `scenario_leader_self_removal` (zero committed loss; gap in the failover class — assert new leader serving < 10 s, report the measured gap). `--local` mode must PASS end-to-end before the fleet run (M6 discipline). Reuse `assert_durable_fs` unchanged.

- [ ] **Step 3: gate doc + runbook + README/CLAUDE.md** — M6 doc structure verbatim: what the gate measures, correctness proofs (sim tables incl. counterfactual-red results, lin_v2 seeds table, crashtest), smoke verdict marked NOT-the-gate, **Fleet result: PENDING placeholder**, and the explicit note that the fleet run is a separate user-approved step. Runbook §6 rewrite + §1 note that `config.state` joined the durable set. README scope section: static→dynamic sentence + 8-member cap. CLAUDE.md: scope line + `uc2ctl` in the commands block.

- [ ] **Step 4: run everything the CI gates run** — `cargo build --workspace --all-targets && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace` and `python3 bench-infra/scripts/m6_fleet_gate.py --local --m7` → PASS.

- [ ] **Step 5: commit**

```bash
git add uc_node/examples/m7_gate.rs bench-infra/scripts/m6_fleet_gate.py \
        docs/benchmarks/uc2-m7-gate-*.md docs/ops/uc2-runbook.md README.md CLAUDE.md
git commit -m "gate(uc2): m7_gate harness + fleet orchestrator scenarios (replace/resize/self-removal) + M7 gate doc (fleet PENDING) + runbook/README/CLAUDE.md"
```

**After Task 11:** merge `uc2/m7-reconfig` to main only when the whole local suite is green; the FLEET run (5×`c6id.2xlarge`) is a separate user-approved step, and `v2.1.0` is tagged only after its PASS lands in the gate doc.

---

## Self-review (executed at plan-writing time)

- **Spec coverage:** §3 config model → T1/T2; §4 quorum/election (adoption, rebuild, one-in-flight, serving gate, removed voters, self-removal) → T3/T5/T8; §5 truncation revert (incl. `to == position` boundary + wipe fiat + snapshot carry) → T6; §6 ops + admin + recipes + staleness warning → T7 (+T9 refusal matrix); §7 net/cnc/versioning → T1/T5 (version bump in T1); §8 sim inv6–9 + counterfactual pins + fuzz → T4; §9 test plan → T9/T10/T11; §10 milestone shape → T11 + the post-T11 note. No spec requirement without a task.
- **Type consistency:** `ConfigOp`/`ProposeError` discriminants match the wire `op`/`reason` codes (T1 ↔ T3 tables); `ConfigRecord` fields match T2 ↔ T5 exec; `propose_config(op, slack)` arity consistent in T3/T7; frame-END position semantics stated identically in T1 (scan), T3 (`ConfigObserved`), T6 (revert boundary).
- **Known mid-branch break:** Tasks 3–5 intentionally break `uc_sim`/`uc_node` compilation until migration lands — called out in T3's note; workspace-wide gates resume at T5 step 7.
