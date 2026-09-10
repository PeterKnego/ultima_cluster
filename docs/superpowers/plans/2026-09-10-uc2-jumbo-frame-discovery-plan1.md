# Jumbo frames — Implementation Plan (plan 1 of 2: discovery and the replicated ceiling)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every node probes the path MTU to its peers with a do-not-fragment ladder, the leader commits the cluster-wide minimum through the replicated Settings record, and the command payload ceiling — the sender's datagram budget, the appender's door, and the number clients read from the cnc page — follows that committed value instead of a source constant.

**Architecture:** Two new pairwise datagram kinds, `PROBE = 24` (padded to the rung it tests) and `PROBE_ACK = 25` (carries the responder's own minimum). A `uc_net::probe::ProbeTable` shared by the sender agent (which sends probes on a 1 s / 30 s cadence before its leader-role gate), the receiver agent (which answers probes with its own seal path and records acks), and the consensus agent (which, while leading, commits `min over members` once every member has answered). `Settings` goes to version 2 with a `datagram_mtu: u32` field that the cluster FSM keeps monotone; the node derives a live rung from the committed view and pushes it into the sender's budget, the log buffer's payload door, and a new live cnc word at 3984 that `uc_client` and the gateway edge read per submit. The socket sets DF, so an oversize send fails by name instead of fragmenting. `max_payload` leaves `node.toml`; `NodeConfig::max_payload` becomes the *bound* (the top rung's ceiling), and tests keep passing small bounds with small buffers.

**Tech Stack:** Rust 1.96 workspace (MSRV 1.89); `uc_protocol` (core-only leaf), `uc_crypto`, `uc_log`, `uc_net` (gains `libc`), `uc_node`, `uc_client`, `uc_gateway`, `uc_ctl`; docs.

**Spec:** `docs/superpowers/specs/2026-09-10-uc2-jumbo-frame-discovery-design.md` — §2, §3C, §4 (all), §5 (5.1–5.3, 5.5), §7 (all), §10's unit and fault-layer tiers and its in-process test items (a), (b), (e). **Plan 2** covers §5.4 (restart/join fail-stop), §6 (`force_jumbo_frames`), §8 (developer notification), §9 (metrics, alerts, status, audit source), the `uc_remote` literal, the fuzz target, §10's fleet gate and §11's docs sweep. Plan 2 is written after this plan lands so its names come from real code.

## Global Constraints

- **Whole workspace green after every task**: `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo clippy -p uc_service --features apply-profile --all-targets -- -D warnings`, `cargo clippy -p uc_gateway --features test-util --all-targets -- -D warnings`, `cargo clippy -p uc_crashtest --features hard-crash-tests --all-targets -- -D warnings` (feature-gated targets escape `--workspace`), `cargo test --workspace --exclude uc_node`, `cargo test -p uc_node --lib --test smoke --test failover --test learner --test purge_safety --test query_barrier --test admin_auth --test daemon_refusals --test timers --test services --test reconfig`; `(cd fuzz && RUSTFLAGS="--cfg fuzzing" cargo +nightly check)` after Tasks 0 and 1 (the `uc_protocol_settings` target must still build).
- **This plan opens the `2.12.0` flag day**: Task 0 moves `uc_protocol::version::CURRENT` to `0.8.0`; Task 5 moves `CNC_V2_VERSION` to `3.2`. The workspace version stays `2.11.0` until the release procedure. Every doc line that says "0.7.0 is the shipped wire" stays true — `2.11.0` IS shipped; the docs say `0.8.0` / cnc `3.2` are *pending*.
- **Frozen once shipped, each pinned by a test whose comment says so**: `RUNGS = [1408, 8832, 8960]`; `DGRAM_KIND_PROBE = 24`, `DGRAM_KIND_PROBE_ACK = 25`; the probe body (`rung: u32 LE @0`, zero padding to the rung length) and the ack body (`rung: u32 @0 ‖ own_min_rung: u32 @4`, `PROBE_ACK_BODY_LEN = 8`); Settings v2 (`datagram_mtu: u32 @29`, `SETTINGS_LEN = 33`, `SETTINGS_VERSION = 2`, v1 accepted on decode); `CNC_OFF_PAYLOAD_CEILING = 3984`.
- **`MTU_DEFAULT` stays 1408 and every derived constant keeps its value**: `MAX_PAYLOAD_DEFAULT == 1312`, `MIN_FSM_LAG_BYTES == 1376`, the `SNAP_BEGIN` budget assert against `MTU_DEFAULT`. The baseline is what every cluster starts from.
- **The consensus hot loop is not the place for new work.** The leader's commit rule runs at most once per 100 ms (a timestamp check per pass, nothing else). The appender's door is one `Relaxed` atomic load replacing a field read — and nothing more is added to `append`.
- **Probes never inflate `seal_failures`**: a probe that cannot be sealed yet (no pairwise session) counts in `probe_unsent`, never in the M8 counter an alert keys on.
- **Determinism**: `ClusterFsm::apply` reads nothing but its own state and the command; the monotone rule is `max(committed, incoming)` inside `apply`, so every replica computes the same value.
- **Surfaces this plan builds on (as built)**: `uc_protocol::v2::datagram::{DatagramHeader, write_datagram_header, read_datagram_header, DATAGRAM_HEADER_LEN, MTU_DEFAULT, max_payload_for_mtu, MAX_PAYLOAD_DEFAULT}` (`datagram.rs:16-44, 632-679`), the `NakBody` body pattern (`:681-706`); `uc_protocol::v2::crypto::CRYPTO_OVERHEAD` (`crypto.rs:34`); `uc_protocol::v2::frame::{HEADER_LEN, FRAME_ALIGNMENT, align_frame_len, CLUSTER_BODY_PREFIX_LEN}`; `uc_protocol::v2::settings` (`settings.rs:1-47`, encode/decode `:82-116`, frozen test `:128`); `uc_protocol::v2::cluster_image` (`SETTINGS_LEN` at `:29, :54, :166`); `uc_protocol::v2::cnc::{CNC_OFF_INGRESS_HOLES_SKIPPED = 3968, CNC_OFF_QUERY_HOLES_SKIPPED = 3976, CNC_V2_VERSION}` and `offsets_do_not_overlap` (`cnc.rs:646`); `uc_log::cnc::CncPage::{ingress_holes_skipped, store_ingress_holes_skipped}` as the bare-`AtomicU64` accessor pattern (`uc_log/src/cnc.rs:761-811`) and its pin test (`:1692`); `uc_log::LogBuffer::{new(region, cnc, max_payload), max_payload(), max_claim(), read_run_validated}` (`buffer.rs:101-189, 485`) and `Appender::{append, append_cluster}` (`:608, :759`); `uc_crypto::Transport::scope_of` (`transport.rs:289-346`); `uc_net::fault::{FaultConfig, FaultSocket::send_to}` (`fault.rs:41-74, 206-269`); `uc_net::sender::{Sender, SenderConfig, SenderStats, CtrlMsg, Sender::do_work, assemble_snap}` (`sender.rs:89-137, 329-459, 806-1066, 1736-1782`), the four budget sites (`:988, :1193, :1263, :1634`); `uc_net::receiver::{FollowerReceiver, FollowerStats, on_datagram, do_work, seal_and_send, set_sender_route}` (`receiver.rs:495-600, 900-960, 1618-1656, 1848-1862, 1863-1923`); `uc_net/tests/common/mod.rs::{buffer, spawn_leader, spawn_follower, TERM}`; `uc_node::node::{Node::start_with, NodeConfig, rebuild_net_for_config, refresh_from_view, append_cluster_frame, validate_cluster_command, apply_settings, do_work's leader hook}` (`node.rs:948-1019, 180-255, 4420-4465, 5731-5838, 5592-5621, 7041-7085, 3482-3529`), `addr_of`, `id_to_addr`; `uc_node::cluster_fsm::{ClusterFsm::{validate_replicated, apply}, ClusterState, ClusterView::{publish, to_state}, ClusterRefusal::SettingsBounds}` (`cluster_fsm.rs:72-90, 253-303, 345-374, 476-577`); `uc_node::config_file::{NodeConfigFile, the admission_bytes refusal, ENV_OVERRIDES}` (`config_file.rs:232-288, 580-599, 406-426`); `uc_node::preflight::{PreflightError, check_semantics}` (`preflight.rs:31-160`); `uc_client::engine::{EngineConfig::max_payload, Shared, attach, the submit check}` (`engine.rs:74-85, 203-217, 341-357, 452-469`); `uc_gateway::edge::{Edge::start, Shared, the wire_len check}` (`edge.rs:776-830, 1316-1333`); `uc_ctl::settings::{parse_settings, show}` (`uc_ctl/src/settings.rs:44-100, 194-212`); `uc_node/tests/query_barrier.rs::{make_config, spawn_cluster, await_single_leader}` (`:66-165`).
- **Never write scratch to `/tmp`.** Test instance dirs go through `CARGO_TARGET_TMPDIR`. Fleet spend is user-gated (nothing here runs on a fleet).
- Commit subjects: `type(scope): imperative summary`. Every new or changed test is **watched red first**; the commit body says how.

---

## File structure

| file | responsibility | task |
|---|---|---|
| `uc_protocol/src/v2/datagram.rs` | `RUNGS`, `MTU_BOUND`, `JUMBO_MIN_RUNG`, `is_rung`, `payload_ceiling`, kinds 24/25, probe + ack bodies | 0 |
| `uc_protocol/src/version.rs`, `uc_crypto/src/transport.rs` | wire `0.8.0`; kinds 24/25 named `Pairwise` | 0 |
| `uc_protocol/src/v2/settings.rs`, `uc_protocol/src/v2/cluster_image.rs` | Settings v2 (`datagram_mtu`), v1 accepted on decode; the image accepts a v1 or v2 settings tail | 1 |
| `uc_node/src/cluster_fsm.rs`, `uc_ctl/src/settings.rs` | `datagram_mtu` validated as a rung, kept monotone in `apply`, published on the view; `settings show` prints it; the file refuses it | 1 |
| `uc_net/src/sockopt.rs` (new), `uc_net/src/fault.rs`, `uc_net/Cargo.toml`, `uc_node/src/node.rs` (bind) | `set_dont_fragment`; `FaultConfig::max_datagram`; the node sets DF after bind | 2 |
| `uc_net/src/probe.rs` (new) | `ProbeTable`: per-peer verified/advertised, cadence, `own_min_rung`, `table_min`, `due` | 3 |
| `uc_net/src/sender.rs`, `uc_net/src/receiver.rs`, `uc_net/tests/probe.rs` (new) | sender sends due probes before its role gate; receiver answers probes and records acks, wire-length check; loopback proof with `max_datagram` | 4 |
| `uc_protocol/src/v2/cnc.rs`, `uc_log/src/cnc.rs`, `uc_log/src/buffer.rs` | `CNC_OFF_PAYLOAD_CEILING = 3984`, cnc `3.2`, accessors + pins; `LogBuffer::{payload_ceiling, set_payload_ceiling}` read by `Appender` | 5 |
| `uc_node/src/node.rs`, `uc_node/src/config_file.rs`, `uc_node/src/preflight.rs`, `uc_net/src/sender.rs` (budget), `bench-infra/scripts/m9_fleet_gate.py` | live rung + ceiling from the view; `ProbeTable` wired at boot and on membership; the leader's commit rule; the sender's live budget; `max_payload` key retired; MTU preflight checks removed; m9 refusal row renamed | 6 |
| `uc_client/src/engine.rs`, `uc_gateway/src/edge.rs` | per-submit read of the live ceiling word | 7 |
| `uc_node/tests/jumbo.rs` (new) | 3-node in-process proof: capped rung, silent member holds the ceiling, an early-attached client sees the raise | 8 |
| `docs/reference/{wire-protocol,cnc-page,configuration,semver-policy}.md`, `packaging/node.example.toml`, `CLAUDE.md` | the statements this plan makes false | 9 |

---

### Task 0: Rungs, `payload_ceiling`, kinds 24/25, probe bodies, wire `0.8.0`

**Files:**
- Modify: `uc_protocol/src/v2/datagram.rs` (after `MAX_PAYLOAD_DEFAULT`'s asserts at `:46-56`; after `DGRAM_KIND_SNAP_REDIRECT` at `:433`)
- Modify: `uc_protocol/src/version.rs:65-76, :112-114`
- Modify: `uc_crypto/src/transport.rs:289-346`

**Interfaces:**
- Produces: `uc_protocol::v2::datagram::{RUNGS: [u32; 3], MTU_BOUND: usize, JUMBO_MIN_RUNG: u32, is_rung(u32) -> bool, payload_ceiling(rung: usize, crypto_on: bool) -> usize, DGRAM_KIND_PROBE, DGRAM_KIND_PROBE_ACK, PROBE_RUNG_LEN: usize = 4, write_probe_rung(&mut [u8], u32), read_probe_rung(&[u8]) -> Option<u32>, PROBE_ACK_BODY_LEN: usize = 8, ProbeAckBody { rung: u32, own_min_rung: u32 }, write_probe_ack_body, read_probe_ack_body}`.

- [ ] **Step 1: Write the failing tests** (append inside `datagram.rs`'s `mod tests`)

```rust
    /// FROZEN once shipped (jumbo spec §4.1): the ladder, the bound, and the
    /// ceilings the table in the spec promises for each rung.
    #[test]
    fn rungs_and_ceilings_are_pinned() {
        assert_eq!(RUNGS, [1408, 8832, 8960]);
        assert_eq!(RUNGS[0] as usize, MTU_DEFAULT);
        assert_eq!(MTU_BOUND, 8960);
        assert_eq!(JUMBO_MIN_RUNG, 8832);
        assert!(is_rung(1408) && is_rung(8832) && is_rung(8960));
        assert!(!is_rung(0) && !is_rung(1500) && !is_rung(9001));
        // crypto-off / crypto-on ceilings, spec §4.1 table
        assert_eq!(payload_ceiling(1408, false), 1344);
        assert_eq!(payload_ceiling(1408, true), 1312);
        assert_eq!(payload_ceiling(8832, false), 8768);
        assert_eq!(payload_ceiling(8832, true), 8736);
        assert_eq!(payload_ceiling(8960, false), 8896);
        assert_eq!(payload_ceiling(8960, true), 8864);
        // The crypto-on figure at the baseline IS today's default.
        assert_eq!(payload_ceiling(MTU_DEFAULT, true), MAX_PAYLOAD_DEFAULT);
        assert_eq!(payload_ceiling(MTU_DEFAULT, true), max_payload_for_mtu(MTU_DEFAULT));
    }

    /// FROZEN once shipped (jumbo spec §4.2): kind numbers and both bodies,
    /// with absolute wire pins like `control_bodies_roundtrip`.
    #[test]
    fn probe_kinds_and_bodies_are_pinned() {
        assert_eq!(DGRAM_KIND_PROBE, 24);
        assert_eq!(DGRAM_KIND_PROBE_ACK, 25);
        assert_eq!(PROBE_RUNG_LEN, 4);
        assert_eq!(PROBE_ACK_BODY_LEN, 8);
        let mut b = [0u8; PROBE_RUNG_LEN];
        write_probe_rung(&mut b, 8832);
        assert_eq!(b, [0x80, 0x22, 0, 0]); // 8832 = 0x2280 LE
        assert_eq!(read_probe_rung(&b), Some(8832));
        assert_eq!(read_probe_rung(&b[..3]), None);
        let a = ProbeAckBody {
            rung: 8960,
            own_min_rung: 1408,
        };
        let mut buf = [0u8; PROBE_ACK_BODY_LEN];
        write_probe_ack_body(&mut buf, &a);
        assert_eq!(read_probe_ack_body(&buf), Some(a));
        assert_eq!(buf, [0, 0x23, 0, 0, 0x80, 0x05, 0, 0]); // 8960 = 0x2300, 1408 = 0x0580
        assert_eq!(read_probe_ack_body(&buf[..7]), None);
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc_protocol rungs_and_ceilings_are_pinned probe_kinds_and_bodies_are_pinned`
Expected: compile error, `RUNGS` / `DGRAM_KIND_PROBE` not found.

- [ ] **Step 3: Implement** — after the `MAX_PAYLOAD_DEFAULT` asserts (`datagram.rs:56`):

```rust
/// Jumbo spec §4.1: the fixed ladder of UC datagram sizes discovery tests,
/// smallest first. `RUNGS[0]` is `MTU_DEFAULT`, the baseline every cluster
/// starts from; 8832 fits GCP (8896 B) over IPv4 and both clouds over IPv6;
/// 8960 fits AWS (9001 B) over IPv4. FROZEN once shipped: a new fabric adds
/// a rung, it never renumbers one.
pub const RUNGS: [u32; 3] = [1408, 8832, 8960];
/// The largest datagram UC will ever send — `RUNGS`' last entry. Sizes the
/// sender's scratch buffers and bounds a single frame.
pub const MTU_BOUND: usize = RUNGS[RUNGS.len() - 1] as usize;
/// What `force_jumbo_frames` demands: the lowest rung both clouds carry on
/// both address families (spec §4.1).
pub const JUMBO_MIN_RUNG: u32 = 8832;
const _: () = assert!(RUNGS[0] as usize == MTU_DEFAULT);
const _: () = assert!(RUNGS[0] < RUNGS[1] && RUNGS[1] < RUNGS[2]);

/// Is `v` one of the ladder's rungs? The only values `Settings::datagram_mtu`
/// may carry besides `0`.
pub const fn is_rung(v: u32) -> bool {
    let mut i = 0;
    while i < RUNGS.len() {
        if RUNGS[i] == v {
            return true;
        }
        i += 1;
    }
    false
}

/// The command payload ceiling at a given datagram size, for either crypto
/// mode — [`max_payload_for_mtu`]'s crypto-aware sibling (that one is the
/// crypto-ON figure, used where a single crypto-safe default is wanted).
pub const fn payload_ceiling(rung: usize, crypto_on: bool) -> usize {
    let overhead = if crypto_on {
        crate::v2::crypto::CRYPTO_OVERHEAD
    } else {
        0
    };
    let budget = rung - DATAGRAM_HEADER_LEN - overhead;
    let aligned = budget & !(crate::v2::frame::FRAME_ALIGNMENT - 1);
    aligned - crate::v2::frame::HEADER_LEN
}
const _: () = assert!(payload_ceiling(MTU_DEFAULT, true) == MAX_PAYLOAD_DEFAULT);
```

After `DGRAM_KIND_SNAP_REDIRECT` (`datagram.rs:433`):

```rust
/// Jumbo spec §4.2: a path-MTU probe. Body = `rung: u32 LE` followed by zero
/// padding so the WHOLE datagram (sealed length, with crypto on) is exactly
/// `rung` bytes. A responder credits it only if the received length equals
/// `rung` — a fragmented-and-reassembled or truncated arrival is not a proof.
/// `Scope::Pairwise`. Header `position` is unused (zero).
pub const DGRAM_KIND_PROBE: u8 = 24;
/// Jumbo spec §4.2: the answer. Body = [`ProbeAckBody`]: the rung being
/// acknowledged plus the responder's OWN verified minimum over its peers, so
/// the leader learns every pair's result without a second exchange.
/// `Scope::Pairwise`.
pub const DGRAM_KIND_PROBE_ACK: u8 = 25;

/// The fixed prefix of a `PROBE` body: the rung, `u32 LE`. Everything after it
/// is padding and carries nothing.
pub const PROBE_RUNG_LEN: usize = 4;

pub fn write_probe_rung(buf: &mut [u8], rung: u32) {
    buf[0..4].copy_from_slice(&rung.to_le_bytes());
}

/// The rung a `PROBE` body claims, or `None` if the body is shorter than
/// [`PROBE_RUNG_LEN`]. The LENGTH check against that claim is the receiver's.
pub fn read_probe_rung(buf: &[u8]) -> Option<u32> {
    if buf.len() < PROBE_RUNG_LEN {
        return None;
    }
    Some(u32::from_le_bytes(buf[0..4].try_into().unwrap()))
}

pub const PROBE_ACK_BODY_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProbeAckBody {
    /// The rung of the probe this acknowledges.
    pub rung: u32,
    /// The responder's own verified minimum over ITS configured peers
    /// (spec §5.2); `0` while any of them is unresolved.
    pub own_min_rung: u32,
}

pub fn write_probe_ack_body(buf: &mut [u8], b: &ProbeAckBody) {
    buf[0..4].copy_from_slice(&b.rung.to_le_bytes());
    buf[4..8].copy_from_slice(&b.own_min_rung.to_le_bytes());
}

pub fn read_probe_ack_body(buf: &[u8]) -> Option<ProbeAckBody> {
    if buf.len() < PROBE_ACK_BODY_LEN {
        return None;
    }
    Some(ProbeAckBody {
        rung: u32::from_le_bytes(buf[0..4].try_into().unwrap()),
        own_min_rung: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
    })
}
```

`uc_protocol/src/version.rs`: above `CURRENT` add the `0.8.0` paragraph and change the constant; update the pin test.

```rust
// 0.8.0 (jumbo frames, spec §4.2): two new pairwise datagram kinds,
// `DGRAM_KIND_PROBE = 24` (a path-MTU probe padded to the rung it tests) and
// `DGRAM_KIND_PROBE_ACK = 25` (the rung acknowledged plus the responder's own
// verified minimum). No existing layout changes. A 0.7.0 peer counts both as
// unknown kinds and drops them, so a mixed cluster never raises its ceiling —
// safe, and unsupported anyway (flag-day rule). Ships in `2.12.0`.
pub const CURRENT: ProtocolVersion = ProtocolVersion::new(0, 8, 0);
```

```rust
    #[test]
    fn current_is_the_jumbo_frame_wire() {
        assert_eq!(CURRENT, ProtocolVersion::new(0, 8, 0));
    }
```

`uc_crypto/src/transport.rs`, in `scope_of`'s `Pairwise` arm, after `| DGRAM_KIND_CONFIG_REPLY`:

```rust
            // Jumbo spec §4.2: a probe is one-to-one, and its ack must be
            // authenticated so a forged ack cannot raise the ceiling. Named
            // here (the catch-all already says Pairwise) to keep the inventory.
            | DGRAM_KIND_PROBE
            | DGRAM_KIND_PROBE_ACK => Scope::Pairwise,
```

Add both to the `use uc_protocol::v2::datagram::{…}` import at the top of `transport.rs`. Add a scope test next to the existing `scope_of` tests: `assert_eq!(Transport::scope_of(DGRAM_KIND_PROBE), Scope::Pairwise); assert_eq!(Transport::scope_of(DGRAM_KIND_PROBE_ACK), Scope::Pairwise);`.

- [ ] **Step 4: Run** `cargo test -p uc_protocol && cargo test -p uc_crypto && (cd fuzz && RUSTFLAGS="--cfg fuzzing" cargo +nightly check)` — expected all PASS. Grep the tree for other pins of the old wire version: `grep -rn "0, 7, 0" --include=*.rs . | grep -v target/` must return only `seal.rs:434` (a byte vector unrelated to the version).

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/v2/datagram.rs uc_protocol/src/version.rs uc_crypto/src/transport.rs
git commit -m "feat(uc_protocol, uc_crypto): jumbo — rung ladder, payload_ceiling, PROBE/PROBE_ACK kinds 24/25, wire 0.8.0"
```

---

### Task 1: Settings v2 — `datagram_mtu`, monotone in the FSM, on the view, shown by `uc2ctl`

**Files:**
- Modify: `uc_protocol/src/v2/settings.rs:10-12, 60-116, 128-150`
- Modify: `uc_protocol/src/v2/cluster_image.rs:29, 54, 160-168`
- Modify: `uc_node/src/cluster_fsm.rs:253-303 (validate), 345-374 (apply), 476-577 (view)`
- Modify: `uc_ctl/src/settings.rs:194-212 (show)`, its tests

**Interfaces:**
- Produces: `Settings { …, datagram_mtu: u32 }`; `SETTINGS_VERSION = 2`, `SETTINGS_LEN = 33`, `SETTINGS_LEN_V1 = 29`; `decode_settings` accepts v1 (→ `datagram_mtu = 0`) and v2; `ClusterView::datagram_mtu: AtomicU32`; `ClusterRefusal::SettingsBounds("datagram_mtu")` (reason 47).

- [ ] **Step 1: Failing tests** — in `settings.rs` replace `settings_layout_is_frozen`'s length/version asserts and add two tests:

```rust
    #[test]
    fn settings_layout_is_frozen() {
        // FROZEN once shipped (cluster-FSM spec §6/§7, jumbo spec §5.5):
        // version u32 @0, fsm_lag_bytes u64 @4, admission_bytes u64 @12,
        // snapshot_interval_bytes u64 @20, target u8 @28, datagram_mtu u32 @29.
        assert_eq!(SETTINGS_VERSION, 2);
        assert_eq!(SETTINGS_LEN, 33);
        assert_eq!(SETTINGS_LEN_V1, 29);
        assert_eq!(FSM_LAG_LOCKSTEP, u64::MAX);
        assert_eq!(MIN_FSM_LAG_BYTES, 1376);
        let s = Settings {
            fsm_lag_bytes: 16 << 20,
            admission_bytes: 4 << 20,
            snapshot_interval_bytes: 1 << 30,
            snapshot_target: Target::Learners,
            datagram_mtu: 8960,
        };
        let mut out = Vec::new();
        encode_settings(&s, &mut out);
        assert_eq!(out.len(), SETTINGS_LEN);
        assert_eq!(&out[0..4], &2u32.to_le_bytes());
        assert_eq!(out[28], 1);
        assert_eq!(&out[29..33], &8960u32.to_le_bytes());
        assert_eq!(decode_settings(&out), Some(s));
    }

    /// Jumbo spec §5.5: the first flag day in which a cluster artifact and
    /// committed CLUSTER frames persist across the upgrade. A v1 record
    /// decodes, with the new field at its baseline meaning.
    #[test]
    fn a_version_1_record_decodes_with_datagram_mtu_zero() {
        let mut v1 = Vec::new();
        v1.extend_from_slice(&1u32.to_le_bytes());
        v1.extend_from_slice(&(16u64 << 20).to_le_bytes());
        v1.extend_from_slice(&(4u64 << 20).to_le_bytes());
        v1.extend_from_slice(&(1u64 << 30).to_le_bytes());
        v1.push(1);
        assert_eq!(v1.len(), SETTINGS_LEN_V1);
        let s = decode_settings(&v1).expect("v1 decodes");
        assert_eq!(s.datagram_mtu, 0);
        assert_eq!(s.fsm_lag_bytes, 16 << 20);
        assert_eq!(s.snapshot_target, Target::Learners);
        // A v1 header with a v2 length (or the reverse) is refused: the
        // length is exact per version.
        v1.extend_from_slice(&[0, 0, 0, 0]);
        assert!(decode_settings(&v1).is_none());
    }
```

Update `decode_is_total_and_refuses_unknown_version_and_target`: the `v2[0] = 2` line becomes `v2[0] = 3` (3 is the unknown version now). Update `genesis_default_means_derive_at_use` with `assert_eq!(d.datagram_mtu, 0);`.

In `cluster_fsm.rs` tests (next to the existing settings tests near `:1000`):

```rust
    /// Jumbo spec §5.5: `datagram_mtu` must be 0 or a rung.
    #[test]
    fn settings_datagram_mtu_must_be_a_rung() {
        let fsm = ClusterFsm::new(ClusterState::genesis_empty(), Vec::new());
        let mut s = Settings::genesis_default();
        s.datagram_mtu = 1500;
        assert_eq!(
            fsm.validate_replicated(&ClusterCommand::Settings(s)),
            Err(ClusterRefusal::SettingsBounds("datagram_mtu"))
        );
        s.datagram_mtu = 8832;
        assert!(fsm.validate_replicated(&ClusterCommand::Settings(s)).is_ok());
        s.datagram_mtu = 0;
        assert!(fsm.validate_replicated(&ClusterCommand::Settings(s)).is_ok());
    }

    /// Jumbo spec §5.5: the FSM keeps the rung monotone — an operator's
    /// `settings apply` (whose absent keys encode as 0) cannot lower it.
    #[test]
    fn settings_apply_never_lowers_datagram_mtu() {
        let mut fsm = ClusterFsm::new(ClusterState::genesis_empty(), Vec::new());
        let mut out = Vec::new();
        let mut raise = Settings::genesis_default();
        raise.datagram_mtu = 8960;
        apply_settings_at(&mut fsm, 64, &raise, &mut out);
        assert_eq!(out, [0]);
        assert_eq!(fsm.state().settings.datagram_mtu, 8960);
        // An operator record with datagram_mtu = 0 and a new admission value.
        let mut op = Settings::genesis_default();
        op.admission_bytes = 4096;
        apply_settings_at(&mut fsm, 128, &op, &mut out);
        assert_eq!(out, [0]);
        assert_eq!(fsm.state().settings.admission_bytes, 4096, "the operator's field landed");
        assert_eq!(fsm.state().settings.datagram_mtu, 8960, "the rung did not move");
        // A lower rung is likewise kept at the max.
        let mut lower = op;
        lower.datagram_mtu = 8832;
        apply_settings_at(&mut fsm, 192, &lower, &mut out);
        assert_eq!(fsm.state().settings.datagram_mtu, 8960);
    }

    fn apply_settings_at(fsm: &mut ClusterFsm, end: u64, s: &Settings, out: &mut Vec<u8>) {
        let mut payload = Vec::new();
        let kind = ClusterFsm::encode_command(&ClusterCommand::Settings(*s), &mut payload);
        let mut frame = Vec::new();
        uc_protocol::v2::frame::write_cluster_prefix(&mut frame, kind);
        frame.extend_from_slice(&payload);
        let mut ctx = uc_service::ApplyCtx::new(end, ClusterFsm::IDENTITY);
        fsm.apply(&mut ctx, &frame, out);
    }
```

(If the existing tests already have a helper that applies an encoded command — grep `fn apply_cmd` / `write_cluster_prefix` in the test module — reuse it and drop `apply_settings_at`. `Settings` is `Copy`; if it is not, clone.)

In `uc_ctl/src/settings.rs` tests: a file naming `datagram_mtu` is refused by `deny_unknown_fields`:

```rust
    /// Jumbo spec §5.5: the rung is leader-owned; an operator file cannot name it.
    #[test]
    fn datagram_mtu_is_not_an_operator_key() {
        let e = parse_settings("datagram_mtu = 8960\n").unwrap_err();
        assert!(e.to_string().contains("datagram_mtu"), "{e}");
    }
```

- [ ] **Step 2: Run to verify they fail** — `cargo test -p uc_protocol settings` (compile error: no field `datagram_mtu`), `cargo test -p uc_node --lib settings_` and `cargo test -p uc_ctl datagram_mtu` (compile errors).

- [ ] **Step 3: Implement**

`settings.rs`:

```rust
/// Encoding version, first word of the payload. `2` since the jumbo flag day
/// (`datagram_mtu`); a reader ACCEPTS `1` (the `2.11.0` shape, mapped to
/// `datagram_mtu = 0`) because cluster artifacts and committed frames written
/// by `2.11.0` persist across the upgrade — jumbo spec §5.5.
pub const SETTINGS_VERSION: u32 = 2;
/// The exact encoded length of a version-2 record — no trailing bytes.
pub const SETTINGS_LEN: usize = 4 + 8 + 8 + 8 + 1 + 4;
/// The exact length of a version-1 record, accepted on decode only.
pub const SETTINGS_LEN_V1: usize = 4 + 8 + 8 + 8 + 1;
```

Add to `Settings`:

```rust
    /// Jumbo spec §5.5: the committed datagram rung (`uc_protocol::v2::
    /// datagram::RUNGS`), `0` = the baseline `MTU_DEFAULT`. Leader-owned:
    /// written by discovery, never by an operator file; the FSM keeps it
    /// monotone (`max(committed, incoming)`).
    pub datagram_mtu: u32,
```

`genesis_default` gets `datagram_mtu: 0`. `encode_settings` appends `out.extend_from_slice(&s.datagram_mtu.to_le_bytes());`. `decode_settings`:

```rust
pub fn decode_settings(buf: &[u8]) -> Option<Settings> {
    if buf.len() < 4 {
        return None;
    }
    let version = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    let datagram_mtu = match (version, buf.len()) {
        (1, SETTINGS_LEN_V1) => 0,
        (2, SETTINGS_LEN) => u32::from_le_bytes(buf[29..33].try_into().unwrap()),
        _ => return None,
    };
    let fsm_lag_bytes = u64::from_le_bytes(buf[4..12].try_into().unwrap());
    let admission_bytes = u64::from_le_bytes(buf[12..20].try_into().unwrap());
    let snapshot_interval_bytes = u64::from_le_bytes(buf[20..28].try_into().unwrap());
    let snapshot_target = match buf[28] {
        0 => Target::All,
        1 => Target::Learners,
        _ => return None,
    };
    Some(Settings {
        fsm_lag_bytes,
        admission_bytes,
        snapshot_interval_bytes,
        snapshot_target,
        datagram_mtu,
    })
}
```

`cluster_image.rs`: import `SETTINGS_LEN_V1` too; `MIN_IMAGE_LEN` uses `SETTINGS_LEN_V1`; the tail check becomes

```rust
    // The settings record is self-versioned and exact-length per version
    // (`settings::decode_settings`): the remainder must be exactly one v1 or
    // one v2 record. A 2.11.0 artifact carries v1 — jumbo spec §5.5.
    let rest = body.len().checked_sub(o)?;
    if rest != SETTINGS_LEN && rest != SETTINGS_LEN_V1 {
        return None;
    }
    let settings = &body[o..];
```

Add a `cluster_image` test that encodes parts with a 29-byte v1 settings blob and asserts `decode` returns `settings.len() == 29` and `decode_settings(parts.settings).unwrap().datagram_mtu == 0`; and that a 31-byte tail is refused.

`cluster_fsm.rs` `validate_replicated`, Settings arm, first check:

```rust
                // Jumbo spec §5.5: 0 (baseline) or a ladder rung, nothing else.
                if s.datagram_mtu != 0 && !uc_protocol::v2::datagram::is_rung(s.datagram_mtu) {
                    return Err(ClusterRefusal::SettingsBounds("datagram_mtu"));
                }
```

`apply`, Settings arm:

```rust
            ClusterCommand::Settings(s) => {
                // Jumbo spec §5.5: the rung is monotone in the FSM itself, so
                // an operator record (absent keys = 0) cannot lower it, and
                // every replica computes the same value.
                let keep = self.state.settings.datagram_mtu.max(s.datagram_mtu);
                self.state.settings = s;
                self.state.settings.datagram_mtu = keep;
                self.state.settings_position = ctx.position;
            }
```

`ClusterView`: add `pub datagram_mtu: AtomicU32` (init 0; `publish` stores `st.settings.datagram_mtu` before `position`; `to_state` reads it). Add `use std::sync::atomic::AtomicU32` if absent.

`uc_ctl/src/settings.rs` `show`: extend the `println!` with `datagram_mtu={} ({})` where the second value is `"baseline"` for `0` and `"discovered"` otherwise; add `datagram_mtu: 0` wherever the tests build a `Settings` literal. `parse_settings` builds its `Settings` with `datagram_mtu: 0`.

Grep the tree for every other `Settings {` literal and add `datagram_mtu: 0`: `grep -rn "snapshot_target:" --include=*.rs . | grep -v target/`.

- [ ] **Step 4: Run** `cargo test -p uc_protocol && cargo test -p uc_node --lib && cargo test -p uc_ctl && cargo test -p uc_node --test services --test reconfig && (cd fuzz && RUSTFLAGS="--cfg fuzzing" cargo +nightly check)` — all PASS. Also run `cargo test -p uc_sim` (inv12 sweeps membership; settings encoding changes must not disturb it).

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/v2/settings.rs uc_protocol/src/v2/cluster_image.rs uc_node/src/cluster_fsm.rs uc_ctl/src/settings.rs
git commit -m "feat(uc_protocol, uc_node, uc_ctl): Settings v2 — datagram_mtu, monotone in the FSM, v1 accepted on decode"
```

---

### Task 2: Do-not-fragment on the socket; a `max_datagram` fault knob

**Files:**
- Create: `uc_net/src/sockopt.rs`
- Modify: `uc_net/src/lib.rs` (add `pub mod sockopt;`), `uc_net/Cargo.toml` (add `libc = { workspace = true }`)
- Modify: `uc_net/src/fault.rs:41-74, 206-215`
- Modify: `uc_node/src/node.rs:952-960`

**Interfaces:**
- Produces: `uc_net::sockopt::set_dont_fragment(&UdpSocket) -> io::Result<()>`; `FaultConfig::max_datagram: usize` (default `usize::MAX`; a send longer than it is dropped before the seeded rolls, so it consumes no RNG draw).

- [ ] **Step 1: Failing tests**

`uc_net/src/sockopt.rs` (whole file, tests included):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Socket options the replication socket needs and `std` does not expose.
//!
//! Jumbo spec §4.3: with do-not-fragment set, a datagram larger than the
//! route MTU fails locally with `EMSGSIZE` and never leaves the host, and
//! one larger than a downstream hop is dropped there — so a probe that is
//! acked was carried WHOLE, and a data send that does not fit is a counted
//! failure rather than a silent fragment (one lost fragment loses the whole
//! datagram, which surfaces as a mystery NAK storm).

use std::io;
use std::net::UdpSocket;
use std::os::fd::AsRawFd;

/// Set DF on `sock` for its address family: `IP_MTU_DISCOVER =
/// IP_PMTUDISC_DO` for IPv4, `IPV6_MTU_DISCOVER = IPV6_PMTUDISC_DO` plus
/// `IPV6_DONTFRAG = 1` for IPv6.
pub fn set_dont_fragment(sock: &UdpSocket) -> io::Result<()> {
    let fd = sock.as_raw_fd();
    let v6 = sock.local_addr()?.is_ipv6();
    if v6 {
        setsockopt(fd, libc::IPPROTO_IPV6, libc::IPV6_MTU_DISCOVER, libc::IPV6_PMTUDISC_DO)?;
        setsockopt(fd, libc::IPPROTO_IPV6, libc::IPV6_DONTFRAG, 1)?;
    } else {
        setsockopt(fd, libc::IPPROTO_IP, libc::IP_MTU_DISCOVER, libc::IP_PMTUDISC_DO)?;
    }
    Ok(())
}

/// Read back `IP_MTU_DISCOVER` / `IPV6_MTU_DISCOVER` (tests, diagnostics).
pub fn mtu_discover(sock: &UdpSocket) -> io::Result<libc::c_int> {
    let fd = sock.as_raw_fd();
    let (level, name) = if sock.local_addr()?.is_ipv6() {
        (libc::IPPROTO_IPV6, libc::IPV6_MTU_DISCOVER)
    } else {
        (libc::IPPROTO_IP, libc::IP_MTU_DISCOVER)
    };
    let mut v: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `v` and `len` are valid for the duration of the call and sized
    // for a c_int option.
    let rc = unsafe {
        libc::getsockopt(fd, level, name, &mut v as *mut _ as *mut libc::c_void, &mut len)
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(v)
}

fn setsockopt(fd: i32, level: libc::c_int, name: libc::c_int, value: libc::c_int) -> io::Result<()> {
    // SAFETY: `value` outlives the call; the length matches its type.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            &value as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn df_is_set_on_a_v4_socket() {
        let s = UdpSocket::bind("127.0.0.1:0").unwrap();
        set_dont_fragment(&s).unwrap();
        assert_eq!(mtu_discover(&s).unwrap(), libc::IP_PMTUDISC_DO);
    }

    #[test]
    fn df_is_set_on_a_v6_socket() {
        let Ok(s) = UdpSocket::bind("[::1]:0") else {
            return; // no IPv6 loopback on this box: nothing to assert
        };
        set_dont_fragment(&s).unwrap();
        assert_eq!(mtu_discover(&s).unwrap(), libc::IPV6_PMTUDISC_DO);
    }

    // DF's effect on an oversize send cannot be shown on loopback (MTU 65536:
    // a datagram over it is refused with EMSGSIZE with or without DF). The
    // behaviour is proven on the fleet's 1500 B arm (spec §10 row b); these
    // tests pin only that the option is set.
}
```

`uc_net/src/fault.rs` test (in its `mod tests`):

```rust
    /// Jumbo spec §10: a capped "path" drops anything longer, the way a DF'd
    /// datagram vanishes at a narrow hop — deterministically, consuming no RNG
    /// draw, so a seeded run's drop/dup/reorder sequence is unchanged.
    #[test]
    fn max_datagram_drops_longer_sends_without_touching_the_rng() {
        let rx = UdpSocket::bind("127.0.0.1:0").unwrap();
        rx.set_nonblocking(true).unwrap();
        let to = rx.local_addr().unwrap();
        let mut tx = FaultSocket::bind("127.0.0.1:0").unwrap();
        tx.set_faults(FaultConfig {
            max_datagram: 1408,
            ..FaultConfig::default()
        });
        tx.send_to(&vec![1u8; 1408], to).unwrap(); // at the cap: passes
        tx.send_to(&vec![2u8; 1409], to).unwrap(); // over: dropped
        tx.send_to(&vec![3u8; 8960], to).unwrap(); // over: dropped
        let got = recv_all(&FaultSocket::from_socket(rx).unwrap(), 1);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].len(), 1408);
    }
```

(`recv_all` exists in `fault.rs`'s tests at `:335`; it takes a `&FaultSocket`.)

- [ ] **Step 2: Run to verify they fail** — `cargo test -p uc_net sockopt` (no module), `cargo test -p uc_net max_datagram` (no field).

- [ ] **Step 3: Implement** — `uc_net/Cargo.toml`: `libc = { workspace = true }` under `[dependencies]`. `lib.rs`: `pub mod probe;` is Task 3; add `pub mod sockopt;` now. `FaultConfig`:

```rust
    /// Jumbo spec §10: a send longer than this is dropped whole, the way a
    /// DF'd datagram is lost at a hop narrower than it — the in-process stand-in
    /// for a small-MTU path. `usize::MAX` (the default) = no cap. Checked
    /// before the seeded rolls so it consumes no RNG draw.
    pub max_datagram: usize,
```

Default `max_datagram: usize::MAX`. In `send_to`, after the partition block and before the `drop_per_million` roll:

```rust
        if buf.len() > self.cfg.max_datagram {
            return Ok(()); // lost at a narrow hop (jumbo spec §10)
        }
```

`uc_node/src/node.rs` `start_with`, right after `let self_addr = sock.local_addr()?;`:

```rust
        // Jumbo spec §4.3: do-not-fragment on the one node socket (all three
        // FaultSocket clones share it). An oversize send fails with EMSGSIZE
        // instead of fragmenting silently; a probe that is acked was carried
        // whole. Applied to an injected test socket too — the tests are where
        // the DF behaviour is proven.
        uc_net::sockopt::set_dont_fragment(&sock)?;
```

- [ ] **Step 4: Run** `cargo test -p uc_net && cargo test -p uc_node --test smoke --test failover --test query_barrier` — all PASS (loopback carries every size the tests send today).

- [ ] **Step 5: Commit**

```bash
git add uc_net/Cargo.toml uc_net/src/lib.rs uc_net/src/sockopt.rs uc_net/src/fault.rs uc_node/src/node.rs Cargo.lock
git commit -m "feat(uc_net, uc_node): jumbo — DF on the replication socket; FaultConfig::max_datagram models a narrow path"
```

---

### Task 3: `ProbeTable` — the per-peer ledger, cadence, own minimum, the leader's table minimum

**Files:**
- Create: `uc_net/src/probe.rs`
- Modify: `uc_net/src/lib.rs` (`pub mod probe;`)

**Interfaces:**
- Produces:

```rust
pub struct ProbeCadence { pub fast_ns: u64, pub fast_attempts: u32, pub slow_ns: u64 }
impl Default for ProbeCadence  // 1 s, 5, 30 s (spec §5.1)
pub struct ProbeTable { /* Mutex<Inner> */ }
pub struct PeerProbe { pub verified: u32, pub advertised: u32, pub attempts: u32, pub next_due_ns: u64 }
impl ProbeTable {
    pub fn new(cadence: ProbeCadence) -> Arc<ProbeTable>;
    pub fn set_peers(&self, peers: &[SocketAddr]);          // add new (zeros, due now), drop absent
    pub fn peers(&self) -> Vec<SocketAddr>;
    pub fn get(&self, peer: SocketAddr) -> Option<PeerProbe>;
    pub fn due(&self, now_ns: u64) -> Vec<(SocketAddr, Vec<u32>)>;  // peers due now, with the rungs above `verified`; bumps attempts + next_due
    pub fn on_ack(&self, from: SocketAddr, rung: u32, own_min_rung: u32); // verified = max(verified, rung) if rung is a rung; advertised = own_min_rung
    pub fn own_min_rung(&self) -> u32;                        // min over verified; 0 if any peer unresolved; MTU_BOUND if no peers
    pub fn table_min(&self, members: &[SocketAddr]) -> Option<u32>; // Some(min over min(verified, advertised)) iff every member present with both > 0
    pub fn unsent(&self) -> u64; pub fn note_unsent(&self);   // probes that could not be sealed (no session yet)
}
```

- [ ] **Step 1: Write the file with its failing tests**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Path-MTU discovery state (jumbo spec §5.1–§5.3), shared by the sender
//! agent (sends due probes), the receiver agent (records acks, answers
//! probes) and the consensus agent (the leader's commit rule). Not a hot
//! path: a handful of updates per peer per process lifetime, so one mutex.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use uc_protocol::v2::datagram::{MTU_BOUND, RUNGS, is_rung};

/// Spec §5.1: retry every `fast_ns` for the first `fast_attempts`, then every
/// `slow_ns`, until the peer's top rung is verified.
#[derive(Debug, Clone, Copy)]
pub struct ProbeCadence {
    pub fast_ns: u64,
    pub fast_attempts: u32,
    pub slow_ns: u64,
}

impl Default for ProbeCadence {
    fn default() -> Self {
        Self {
            fast_ns: 1_000_000_000,
            fast_attempts: 5,
            slow_ns: 30_000_000_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PeerProbe {
    /// The largest rung this node's probe to the peer was acked at; 0 = none.
    pub verified: u32,
    /// The peer's own minimum, from its latest ack (spec §5.2); 0 = unknown
    /// or the peer itself is unresolved.
    pub advertised: u32,
    pub attempts: u32,
    pub next_due_ns: u64,
}

pub struct ProbeTable {
    cadence: ProbeCadence,
    peers: Mutex<HashMap<SocketAddr, PeerProbe>>,
    unsent: AtomicU64,
}

impl ProbeTable {
    pub fn new(cadence: ProbeCadence) -> Arc<ProbeTable> {
        Arc::new(ProbeTable {
            cadence,
            peers: Mutex::new(HashMap::new()),
            unsent: AtomicU64::new(0),
        })
    }

    /// Replace the peer set: a new peer starts unresolved and due now; a peer
    /// no longer in `peers` is forgotten (spec §5.1, membership change).
    pub fn set_peers(&self, peers: &[SocketAddr]) {
        let mut g = self.peers.lock().unwrap();
        g.retain(|a, _| peers.contains(a));
        for &p in peers {
            g.entry(p).or_default();
        }
    }

    pub fn peers(&self) -> Vec<SocketAddr> {
        self.peers.lock().unwrap().keys().copied().collect()
    }

    pub fn get(&self, peer: SocketAddr) -> Option<PeerProbe> {
        self.peers.lock().unwrap().get(&peer).copied()
    }

    /// The peers whose next probe is due at `now_ns`, each with the rungs
    /// still above its `verified`. Bumps `attempts` and schedules the next
    /// due time, so a caller that sends what it is handed needs nothing else.
    pub fn due(&self, now_ns: u64) -> Vec<(SocketAddr, Vec<u32>)> {
        let mut out = Vec::new();
        let mut g = self.peers.lock().unwrap();
        for (&addr, p) in g.iter_mut() {
            if p.verified as usize >= MTU_BOUND || now_ns < p.next_due_ns {
                continue;
            }
            let rungs: Vec<u32> = RUNGS.iter().copied().filter(|&r| r > p.verified).collect();
            p.attempts += 1;
            let step = if p.attempts <= self.cadence.fast_attempts {
                self.cadence.fast_ns
            } else {
                self.cadence.slow_ns
            };
            p.next_due_ns = now_ns + step;
            out.push((addr, rungs));
        }
        out
    }

    /// An ack from `from`: `rung` was carried whole (only a ladder rung
    /// counts — a forged or garbled value is ignored), and the peer's own
    /// minimum is `own_min_rung`. An ack from an unknown address is ignored.
    pub fn on_ack(&self, from: SocketAddr, rung: u32, own_min_rung: u32) {
        if !is_rung(rung) {
            return;
        }
        let mut g = self.peers.lock().unwrap();
        if let Some(p) = g.get_mut(&from) {
            p.verified = p.verified.max(rung);
            p.advertised = own_min_rung;
        }
    }

    /// Spec §5.2: min over peers of `verified`; 0 while any peer is
    /// unresolved; `MTU_BOUND` for a node with no peers (a solo cluster's
    /// path is loopback).
    pub fn own_min_rung(&self) -> u32 {
        let g = self.peers.lock().unwrap();
        if g.is_empty() {
            return MTU_BOUND as u32;
        }
        g.values().map(|p| p.verified).min().unwrap_or(0)
    }

    /// Spec §5.3: the leader's table minimum over `members` — `Some` only when
    /// every member has an entry whose `verified` and `advertised` are both
    /// non-zero (every pair answered). An empty `members` is `Some(MTU_BOUND)`.
    pub fn table_min(&self, members: &[SocketAddr]) -> Option<u32> {
        let g = self.peers.lock().unwrap();
        let mut min = MTU_BOUND as u32;
        for m in members {
            let p = g.get(m)?;
            if p.verified == 0 || p.advertised == 0 {
                return None;
            }
            min = min.min(p.verified).min(p.advertised);
        }
        Some(min)
    }

    pub fn note_unsent(&self) {
        self.unsent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn unsent(&self) -> u64 {
        self.unsent.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn a(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn fast() -> ProbeCadence {
        ProbeCadence {
            fast_ns: 10,
            fast_attempts: 2,
            slow_ns: 100,
        }
    }

    #[test]
    fn a_new_peer_is_due_now_with_every_rung_then_follows_the_cadence() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1)]);
        let d = t.due(0);
        assert_eq!(d, vec![(a(1), vec![1408, 8832, 8960])]);
        assert!(t.due(5).is_empty(), "not due again before fast_ns");
        assert_eq!(t.due(10).len(), 1); // attempt 2 (still fast)
        assert!(t.due(25).is_empty(), "attempt 3 is on the slow cadence");
        assert_eq!(t.due(120).len(), 1);
    }

    #[test]
    fn an_ack_raises_verified_and_narrows_the_next_probe() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1)]);
        t.due(0);
        t.on_ack(a(1), 8832, 0);
        assert_eq!(t.get(a(1)).unwrap().verified, 8832);
        assert_eq!(t.due(10), vec![(a(1), vec![8960])]);
        t.on_ack(a(1), 8960, 8832);
        assert!(t.due(1_000).is_empty(), "resolved: probing stops");
        // A lower late ack never lowers; a non-rung is ignored.
        t.on_ack(a(1), 1408, 8832);
        t.on_ack(a(1), 1500, 8832);
        assert_eq!(t.get(a(1)).unwrap().verified, 8960);
        // An ack from a stranger is ignored.
        t.on_ack(a(9), 8960, 8960);
        assert!(t.get(a(9)).is_none());
    }

    #[test]
    fn own_min_is_zero_while_any_peer_is_unresolved() {
        let t = ProbeTable::new(fast());
        assert_eq!(t.own_min_rung(), MTU_BOUND as u32, "no peers: loopback");
        t.set_peers(&[a(1), a(2)]);
        assert_eq!(t.own_min_rung(), 0);
        t.on_ack(a(1), 8960, 0);
        assert_eq!(t.own_min_rung(), 0);
        t.on_ack(a(2), 8832, 0);
        assert_eq!(t.own_min_rung(), 8832);
    }

    #[test]
    fn table_min_needs_every_member_verified_and_advertising() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1), a(2)]);
        let members = [a(1), a(2)];
        assert_eq!(t.table_min(&members), None);
        t.on_ack(a(1), 8960, 8960);
        assert_eq!(t.table_min(&members), None, "a(2) silent");
        t.on_ack(a(2), 8960, 0);
        assert_eq!(t.table_min(&members), None, "a(2) itself unresolved");
        t.on_ack(a(2), 8960, 8832);
        assert_eq!(t.table_min(&members), Some(8832), "a(2)'s own path is the min");
        // A member not in the table (never set as a peer) blocks the rule.
        assert_eq!(t.table_min(&[a(1), a(2), a(3)]), None);
        assert_eq!(t.table_min(&[]), Some(MTU_BOUND as u32));
    }

    #[test]
    fn set_peers_forgets_removed_and_keeps_known() {
        let t = ProbeTable::new(fast());
        t.set_peers(&[a(1), a(2)]);
        t.on_ack(a(1), 8960, 8960);
        t.set_peers(&[a(1), a(3)]);
        assert_eq!(t.get(a(1)).unwrap().verified, 8960);
        assert!(t.get(a(2)).is_none());
        assert_eq!(t.get(a(3)).unwrap(), PeerProbe::default());
    }
}
```

- [ ] **Step 2: Run to verify red** — comment the whole `impl ProbeTable` block out first and run `cargo test -p uc_net probe` to see the tests fail to compile; then restore it. (The commit body records this.)

- [ ] **Step 3: Run** `cargo test -p uc_net probe` — 5 PASS. `cargo clippy -p uc_net --all-targets -- -D warnings`.

- [ ] **Step 4: Commit**

```bash
git add uc_net/src/probe.rs uc_net/src/lib.rs
git commit -m "feat(uc_net): jumbo — ProbeTable: per-peer ledger, cadence, own minimum, the leader's table minimum"
```

---

### Task 4: The sender probes, the receiver answers and records — proven over loopback with a capped path

**Files:**
- Modify: `uc_net/src/sender.rs` (imports; `Sender` fields `:461-560`; `do_work` before the role gate `:869`; new `send_due_probes`, `assemble_probe`; `SenderStats` gets `probes_sent`)
- Modify: `uc_net/src/receiver.rs` (imports; `FollowerReceiver` fields; `do_work` `:1633` sets `last_wire_len`; `on_datagram` `:1872` early branch; new `on_probe`; `FollowerStats` gets `probes_answered`, `probe_acks`, `probes_wrong_length`)
- Create: `uc_net/tests/probe.rs`

**Interfaces:**
- Consumes: `ProbeTable` (Task 3); `write_probe_rung`, `read_probe_rung`, `ProbeAckBody`, `write_probe_ack_body`, `read_probe_ack_body`, `DGRAM_KIND_PROBE`, `DGRAM_KIND_PROBE_ACK` (Task 0).
- Produces: `Sender::set_probe_table(&mut self, Arc<ProbeTable>)`, `FollowerReceiver::set_probe_table(&mut self, Arc<ProbeTable>)`; `SenderStats::probes_sent`; `FollowerStats::{probes_answered, probe_acks, probes_wrong_length}`.

- [ ] **Step 1: The failing integration test** — `uc_net/tests/probe.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Jumbo spec §10 (fault-layer tier): discovery lands on exactly the rung a
//! capped path carries, and on the top rung when nothing caps it.

mod common;

use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use uc_log::agent::{AgentRunner, IdleStrategy};
use uc_net::fault::{FaultConfig, FaultSocket};
use uc_net::probe::{ProbeCadence, ProbeTable};
use uc_net::receiver::{FollowerConfig, FollowerReceiver};
use uc_net::sender::{Sender, SenderConfig};
use uc_protocol::v2::datagram::MTU_BOUND;

/// One node: a sender (always-follower here, so it only probes) and a
/// receiver on the same socket, sharing one table.
struct Peer {
    addr: SocketAddr,
    table: Arc<ProbeTable>,
    _agents: Vec<uc_log::agent::AgentHandle>,
}

fn spawn_peer(name: &str, faults: FaultConfig) -> Peer {
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr = raw.local_addr().unwrap();
    let mut send_sock = FaultSocket::from_socket(raw.try_clone().unwrap()).unwrap();
    let mut recv_sock = FaultSocket::from_socket(raw).unwrap();
    send_sock.set_faults(faults);
    recv_sock.set_faults(faults);
    let table = ProbeTable::new(ProbeCadence {
        fast_ns: 20_000_000, // 20 ms
        fast_attempts: 5,
        slow_ns: 200_000_000,
    });
    let buffer = common::buffer();
    let (_ctrl_tx, ctrl_rx) = mpsc::sync_channel(64);
    let term = Arc::new(AtomicU32::new(common::TERM));
    let role = Arc::new(AtomicBool::new(false)); // never leads: probes only
    let mut sender = Sender::new(
        Arc::clone(&buffer),
        send_sock,
        Vec::new(),
        1,
        ctrl_rx,
        SenderConfig::new(common::TERM),
        Arc::clone(&term),
        role,
    );
    sender.set_probe_table(Arc::clone(&table));
    let mut receiver = FollowerReceiver::new(
        buffer,
        recv_sock,
        FollowerConfig::new(addr),
        term,
        common::unrouted_consensus(),
    );
    receiver.set_probe_table(Arc::clone(&table));
    let tx = AgentRunner::spawn(&format!("{name}-tx"), IdleStrategy::Yield, move || sender.do_work()).unwrap();
    let rx = AgentRunner::spawn(&format!("{name}-rx"), IdleStrategy::Yield, move || receiver.do_work()).unwrap();
    Peer { addr, table, _agents: vec![tx, rx] }
}

fn await_verified(p: &Peer, peer: SocketAddr, want: u32, secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let got = p.table.get(peer).map(|e| e.verified).unwrap_or(0);
        if got == want {
            return;
        }
        assert!(Instant::now() < deadline, "verified to {peer} = {got}, wanted {want}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn an_uncapped_loopback_path_verifies_the_top_rung_both_ways() {
    let a = spawn_peer("a", FaultConfig::default());
    let b = spawn_peer("b", FaultConfig::default());
    a.table.set_peers(&[b.addr]);
    b.table.set_peers(&[a.addr]);
    await_verified(&a, b.addr, MTU_BOUND as u32, 5);
    await_verified(&b, a.addr, MTU_BOUND as u32, 5);
    assert_eq!(a.table.own_min_rung(), MTU_BOUND as u32);
    // b's ack carried its own minimum, so a's table knows b's view too.
    let deadline = Instant::now() + Duration::from_secs(5);
    while a.table.get(b.addr).unwrap().advertised != MTU_BOUND as u32 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(a.table.table_min(&[b.addr]), Some(MTU_BOUND as u32));
}

#[test]
fn a_path_capped_at_8832_verifies_exactly_8832() {
    let cap = FaultConfig {
        max_datagram: 8832,
        ..FaultConfig::default()
    };
    let a = spawn_peer("a", cap);
    let b = spawn_peer("b", cap);
    a.table.set_peers(&[b.addr]);
    b.table.set_peers(&[a.addr]);
    await_verified(&a, b.addr, 8832, 5);
    await_verified(&b, a.addr, 8832, 5);
    // Give the ladder two more attempts: 8960 must never land.
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(a.table.get(b.addr).unwrap().verified, 8832);
    assert_eq!(a.table.own_min_rung(), 8832);
}

/// The cap applies to what THIS side sends. A narrow path in one direction
/// only still caps the pair's minimum: a's probes to b are dropped above
/// 1408, so b credits a at 1408 and advertises 1408 back.
#[test]
fn an_asymmetric_cap_shows_up_in_the_advertised_minimum() {
    let a = spawn_peer(
        "a",
        FaultConfig {
            max_datagram: 1408,
            ..FaultConfig::default()
        },
    );
    let b = spawn_peer("b", FaultConfig::default());
    a.table.set_peers(&[b.addr]);
    b.table.set_peers(&[a.addr]);
    await_verified(&b, a.addr, MTU_BOUND as u32, 5); // b → a is wide
    await_verified(&a, b.addr, 1408, 5); // a → b is narrow
    let deadline = Instant::now() + Duration::from_secs(5);
    while b.table.get(a.addr).unwrap().advertised != 1408 {
        assert!(Instant::now() < deadline, "b never learned a's minimum");
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(b.table.table_min(&[a.addr]), Some(1408));
}
```

(`common::buffer()` and `common::unrouted_consensus()` exist in `uc_net/tests/common/mod.rs`. Check the exact type name `AgentRunner::spawn` returns — grep `pub fn spawn` in `uc_log/src/agent.rs` — and use it for `_agents`.)

- [ ] **Step 2: Run to verify red** — `cargo test -p uc_net --test probe`: compile error, `set_probe_table` not found.

- [ ] **Step 3: Sender side** — imports: `use uc_protocol::v2::datagram::{DGRAM_KIND_PROBE, PROBE_RUNG_LEN, write_probe_rung}` and `use crate::probe::ProbeTable`. Fields on `Sender`:

```rust
    /// Jumbo spec §5.1: the discovery ledger shared with the receiver and
    /// the node. `None` = this sender does not probe (harness senders).
    probe: Option<Arc<ProbeTable>>,
```

(init `probe: None` in the constructor.) `SenderStats` gets `pub probes_sent: AtomicU64,`. Setter:

```rust
    /// Jumbo spec §5.1: probe the peers in `table` on its cadence. The table's
    /// peer set is the node's to maintain (`ProbeTable::set_peers`).
    pub fn set_probe_table(&mut self, table: Arc<ProbeTable>) {
        self.probe = Some(table);
    }
```

In `do_work`, immediately before `let leader_role = self.role.load(Ordering::Relaxed);`:

```rust
        // Jumbo spec §5.1: probes go out whatever this node's role — a
        // follower's path matters as much as the leader's — so this runs
        // BEFORE the leader-role gate below.
        if self.send_due_probes() {
            did = true;
        }
```

The two new methods:

```rust
    /// One pass of the ladder: every peer the table says is due gets one
    /// PROBE per rung still above its verified size. Pairwise-sealed like a
    /// snapshot chunk; a probe that cannot be sealed yet (no session) is
    /// counted in the table's `unsent`, never in `seal_failures`.
    fn send_due_probes(&mut self) -> bool {
        let Some(table) = self.probe.clone() else {
            return false;
        };
        let now = self.now_ns();
        let due = table.due(now);
        if due.is_empty() {
            return false;
        }
        let overhead = DATAGRAM_HEADER_LEN + self.cfg.crypto_overhead();
        for (peer, rungs) in due {
            for rung in rungs {
                let body_len = rung as usize - overhead;
                let mut body = vec![0u8; body_len];
                write_probe_rung(&mut body, rung);
                if self.assemble_probe(peer, &body) {
                    let _ = self.sock.send_to(&self.scratch, peer);
                    self.stats.probes_sent.fetch_add(1, Ordering::Relaxed);
                } else {
                    table.note_unsent();
                }
            }
        }
        true
    }

    /// `assemble_snap` for a PROBE, except that a seal failure is the
    /// table's `unsent`, not `seal_failures` (a probe before the handshake
    /// completes is expected, not a defect an alert should see).
    fn assemble_probe(&mut self, peer: SocketAddr, body: &[u8]) -> bool {
        self.scratch.clear();
        self.scratch.resize(DATAGRAM_HEADER_LEN, 0);
        write_datagram_header(
            &mut self.scratch,
            &DatagramHeader {
                position: 0,
                leadership_term_id: self.term.load(Ordering::Relaxed),
                kind: DGRAM_KIND_PROBE,
                flags: 0,
                key_epoch: 0,
            },
        );
        self.scratch.extend_from_slice(body);
        if self.crypto.is_none() {
            return true;
        }
        let Some(&peer_id) = self.peer_ids.get(&peer) else {
            return false;
        };
        let crypto = self.crypto.as_mut().expect("checked Some just above");
        let now_ns = crypto.now_ns();
        crypto
            .seal(DGRAM_KIND_PROBE, Some(peer_id), &mut self.scratch, now_ns)
            .is_ok()
    }
```

`body_len` uses `PROBE_RUNG_LEN` only through `write_probe_rung`; add `debug_assert!(body_len >= PROBE_RUNG_LEN)`.

- [ ] **Step 4: Receiver side** — imports: `use uc_protocol::v2::datagram::{DGRAM_KIND_PROBE, DGRAM_KIND_PROBE_ACK, PROBE_ACK_BODY_LEN, ProbeAckBody, read_probe_ack_body, read_probe_rung, write_probe_ack_body}` and `use crate::probe::ProbeTable`. Fields on `FollowerReceiver`:

```rust
    /// Jumbo spec §5.1: the discovery ledger (acks land here; probes are
    /// answered with `own_min_rung` from here). `None` = harness receiver.
    probe: Option<Arc<ProbeTable>>,
    /// The WIRE length of the datagram `on_datagram` is currently handling —
    /// the sealed length, before `crypto_admit` opened it. A PROBE is credited
    /// only if this equals the rung it claims (spec §4.2).
    last_wire_len: usize,
```

`FollowerStats` gets `pub probes_answered: AtomicU64, pub probe_acks: AtomicU64, pub probes_wrong_length: AtomicU64,`. Setter `pub fn set_probe_table(&mut self, table: Arc<ProbeTable>)`. In `do_work`, set `self.last_wire_len = n;` before `if let Some(len) = self.crypto_admit(...)`. In `on_datagram`, right after `let Some(h) = read_datagram_header(d) else { … };` and BEFORE `is_consensus_kind`:

```rust
        // Jumbo spec §4.2: probes are term-independent (a path is a path
        // whoever leads) and never touch the data plane, so they are handled
        // before the term filter below.
        if matches!(h.kind, DGRAM_KIND_PROBE | DGRAM_KIND_PROBE_ACK) {
            self.on_probe(h.kind, &d[DATAGRAM_HEADER_LEN..], from);
            return;
        }
```

```rust
    fn on_probe(&mut self, kind: u8, body: &[u8], from: SocketAddr) {
        let Some(table) = self.probe.clone() else {
            return; // no ledger: a harness receiver ignores probes
        };
        if kind == DGRAM_KIND_PROBE {
            let Some(rung) = read_probe_rung(body) else {
                self.stats.dropped_malformed.fetch_add(1, Ordering::Relaxed);
                return;
            };
            if self.last_wire_len != rung as usize {
                // Truncated, reassembled, or lying: not a proof of the path.
                self.stats.probes_wrong_length.fetch_add(1, Ordering::Relaxed);
                return;
            }
            let ack = ProbeAckBody {
                rung,
                own_min_rung: table.own_min_rung(),
            };
            let mut d = vec![0u8; DATAGRAM_HEADER_LEN + PROBE_ACK_BODY_LEN];
            write_datagram_header(
                &mut d,
                &DatagramHeader {
                    position: 0,
                    leadership_term_id: self.term.load(Ordering::Relaxed),
                    kind: DGRAM_KIND_PROBE_ACK,
                    flags: 0,
                    key_epoch: 0,
                },
            );
            write_probe_ack_body(&mut d[DATAGRAM_HEADER_LEN..], &ack);
            if self.seal_and_send(from, DGRAM_KIND_PROBE_ACK, &mut d) {
                self.stats.probes_answered.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            let Some(ack) = read_probe_ack_body(body) else {
                self.stats.dropped_malformed.fetch_add(1, Ordering::Relaxed);
                return;
            };
            table.on_ack(from, ack.rung, ack.own_min_rung);
            self.stats.probe_acks.fetch_add(1, Ordering::Relaxed);
        }
    }
```

Check `seal_and_send`'s crypto path: `transport.seal_pairwise_control(kind, peer_id, d)` — confirm it accepts any `Pairwise` kind (grep its match, `uc_crypto/src/transport.rs`); if it has a kind allow-list, add `DGRAM_KIND_PROBE_ACK` to it.

- [ ] **Step 5: Run** `cargo test -p uc_net --test probe` — 3 PASS; `cargo test -p uc_net` (every existing sender/receiver test still green: a `None` table changes nothing); `cargo clippy -p uc_net --all-targets -- -D warnings`.

- [ ] **Step 6: Commit**

```bash
git add uc_net/src/sender.rs uc_net/src/receiver.rs uc_net/tests/probe.rs
git commit -m "feat(uc_net): jumbo — the sender probes on the table's cadence, the receiver answers and records acks; loopback proof with a capped path"
```

---

### Task 5: cnc 3.2 — the live ceiling word at 3984; the log buffer's live payload door

**Files:**
- Modify: `uc_protocol/src/v2/cnc.rs:59 (version), after :260 (new const), offsets_do_not_overlap (:646-755), :786 (version pin)`
- Modify: `uc_log/src/cnc.rs` (accessor pair after `store_query_holes_skipped` `:811`; pin tests near `:1732`; layout test near `:1210`)
- Modify: `uc_log/src/buffer.rs:78-143 (field + accessors), :608, :759 (the two doors)`

**Interfaces:**
- Produces: `uc_protocol::v2::cnc::CNC_OFF_PAYLOAD_CEILING = 3984`; `CNC_V2_VERSION = (3 << 24) | (2 << 16)`; `uc_log::cnc::CncPage::{payload_ceiling() -> u64, store_payload_ceiling(u64)}`; `uc_log::LogBuffer::{payload_ceiling() -> usize, set_payload_ceiling(usize)}` — `max_payload()` is now documented as the BOUND.

- [ ] **Step 1: Failing tests**

`uc_protocol/src/v2/cnc.rs`, in `offsets_do_not_overlap` after the `4032` assertions:

```rust
        // Jumbo (cnc 3.2, FROZEN): the live command payload ceiling, third
        // word of the 3968 line — a line whose other two words are on-change
        // diagnostics, so a client reading this per submit never false-shares
        // against a hot writer (the 4032 line's log_time_ns is rewritten every
        // frame).
        assert_eq!(CNC_OFF_PAYLOAD_CEILING, 3984);
        assert_eq!(CNC_OFF_PAYLOAD_CEILING, CNC_OFF_QUERY_HOLES_SKIPPED + 8);
        const { assert!(CNC_OFF_PAYLOAD_CEILING + 8 <= CNC_OFF_INGRESS_HOLES_SKIPPED + 64) };
```

Version pin at `:786`: `assert_eq!(CNC_V2_VERSION, (3 << 24) | (2 << 16));` with a comment "cnc 3.2: the live payload ceiling word (jumbo)".

`uc_log/src/cnc.rs`, next to `ingress_holes_skipped_roundtrip_and_offset_pin`:

```rust
    #[test]
    fn payload_ceiling_roundtrip_and_offset_pin() {
        let page = CncPage::heap(&test_meta());
        assert_eq!(page.payload_ceiling(), 0, "fresh page: the node stores it at boot");
        page.store_payload_ceiling(8864);
        assert_eq!(page.payload_ceiling(), 8864);
        let raw = page.page();
        assert_eq!(
            u64::from_le_bytes(raw[3984..3992].try_into().unwrap()),
            8864,
            "offset pin: the value must live at 3984 exactly"
        );
    }
```

and in the layout test: `assert_eq!(cnc::CNC_OFF_PAYLOAD_CEILING, 3984);`.

`uc_log/src/buffer.rs` tests (near the `max_payload: 256` fixtures at `:994`):

```rust
    /// Jumbo spec §7.1/§7.2: `max_payload` is the BOUND (buffer sizing); the
    /// live ceiling starts equal to it and the appender refuses above the
    /// CEILING, not the bound.
    #[test]
    fn the_appender_refuses_above_the_live_ceiling_not_the_bound() {
        let (buf, cnc) = test_buffer_and_cnc(256); // whatever the fixture is named
        assert_eq!(buf.payload_ceiling(), 256, "ceiling starts at the bound");
        let mut app = Appender::new(Arc::clone(&buf));
        assert!(app.append(1, 1, &[0u8; 200]).is_ok());
        buf.set_payload_ceiling(128);
        assert_eq!(
            app.append(1, 2, &[0u8; 200]).unwrap_err(),
            AppendError::PayloadTooLarge
        );
        assert!(app.append(1, 3, &[0u8; 128]).is_ok());
        assert_eq!(
            app.append_cluster(1, ClusterKind::Settings, &[0u8; 121]).unwrap_err(),
            AppendError::PayloadTooLarge,
            "8 B prefix + 121 > 128"
        );
        buf.set_payload_ceiling(256);
        assert!(app.append(1, 4, &[0u8; 200]).is_ok());
        let _ = cnc;
    }
```

(Use the file's existing fixture that builds a `LogBuffer` with `max_payload = 256` — the test at `:994-1004` shows its shape; name it accordingly.)

- [ ] **Step 2: Run to verify red** — `cargo test -p uc_protocol cnc`, `cargo test -p uc_log payload_ceiling`: missing const / methods.

- [ ] **Step 3: Implement**

`uc_protocol/src/v2/cnc.rs` after `CNC_OFF_QUERY_HOLES_SKIPPED`'s asserts:

```rust
/// Jumbo spec §7.3 (cnc 3.2): the LIVE command payload ceiling in bytes —
/// `payload_ceiling(committed rung, crypto)` — what a client may submit right
/// now. Third `u64` of the 3968 line (3968 and 3976 are the hole counters).
/// Writer: the consensus agent, on change only (at boot, and when the
/// committed `Settings::datagram_mtu` moves); readers: every attached client
/// and the gateway edge, one `Acquire` load per submit. Shares the line
/// legitimately: all three words are written a handful of times per
/// process lifetime, so there is no hot writer to false-share against —
/// unlike the 4032 line, whose `log_time_ns` moves every frame. The header's
/// `max_payload` (offset 112) is now the BOUND the buffer is sized for, not
/// the door.
pub const CNC_OFF_PAYLOAD_CEILING: usize = 3984;
const _: () = assert!(CNC_OFF_PAYLOAD_CEILING == CNC_OFF_QUERY_HOLES_SKIPPED + 8);
const _: () = assert!(CNC_OFF_PAYLOAD_CEILING + 8 <= CNC_OFF_INGRESS_HOLES_SKIPPED + 64);
```

Version: `pub const CNC_V2_VERSION: u32 = (3 << 24) | (2 << 16);` with the doc extended: "3.2 (jumbo): the live payload ceiling word at 3984; a 3.1 attacher refuses by version, a 3.2 attacher on a 3.1 page reads 0 there and treats it as the header bound." Update the layout comment block at the top (`3456 … band` line) to name `payload_ceiling`.

`uc_log/src/cnc.rs` (import `CNC_OFF_PAYLOAD_CEILING`):

```rust
    /// Jumbo spec §7.3: the live command payload ceiling. Bare `AtomicU64` at
    /// 3984 — third word of the 3968 line, same reasoning as its neighbours.
    pub fn payload_ceiling(&self) -> u64 {
        // SAFETY: offset 3984, size 8, 8-byte aligned (3984 % 8 == 0).
        let ptr = unsafe { self.region.ptr_at(CNC_OFF_PAYLOAD_CEILING) as *const AtomicU64 };
        unsafe { (*ptr).load(Ordering::Acquire) }
    }

    /// Writer: the consensus agent, on change only.
    pub fn store_payload_ceiling(&self, v: u64) {
        // SAFETY: offset 3984, size 8, 8-byte aligned. See the getter.
        let ptr = unsafe { self.region.ptr_at(CNC_OFF_PAYLOAD_CEILING) as *const AtomicU64 };
        unsafe { (*ptr).store(v, Ordering::Release) }
    }
```

`uc_log/src/buffer.rs`: field `payload_ceiling: AtomicUsize` on `LogBuffer`, initialised to `max_payload` in `new`; accessors:

```rust
    /// The BOUND: the largest payload this buffer is sized for (capacity ≥
    /// 4× its max claim). Since jumbo, NOT the door — see `payload_ceiling`.
    pub fn max_payload(&self) -> usize { self.max_payload }

    /// Jumbo spec §7.1: the LIVE door `Appender::append` refuses above —
    /// `min(bound, payload_ceiling(committed rung, crypto))`, stored by the
    /// node when the committed rung moves. Starts equal to the bound; the
    /// node lowers it to the baseline before any agent runs.
    pub fn payload_ceiling(&self) -> usize {
        self.payload_ceiling.load(Ordering::Relaxed)
    }

    pub fn set_payload_ceiling(&self, v: usize) {
        debug_assert!(v <= self.max_payload, "ceiling above the bound");
        self.payload_ceiling.store(v.min(self.max_payload), Ordering::Release);
    }
```

The two doors: `if payload.len() > self.buffer.payload_ceiling.load(Ordering::Relaxed)` in `append`; `if body_len > self.buffer.payload_ceiling.load(Ordering::Relaxed)` in `append_cluster`. Nothing else in `append` changes (Global Constraints: one load replaces one field read).

- [ ] **Step 4: Run** `cargo test -p uc_protocol && cargo test -p uc_log && cargo test -p uc_client && cargo test -p uc_service` — PASS. Every attach in the tree goes through `CncPage` version checks: `cargo test -p uc_node --test smoke --test services` must still pass (both sides of an attach are this tree).

- [ ] **Step 5: Commit**

```bash
git add uc_protocol/src/v2/cnc.rs uc_log/src/cnc.rs uc_log/src/buffer.rs
git commit -m "feat(uc_protocol, uc_log): jumbo — cnc 3.2 live payload-ceiling word at 3984; LogBuffer's appender door follows a live ceiling"
```

---

### Task 6: The node — live rung, wired probes, the leader's commit rule, the sender's live budget, `max_payload` retired

**Files:**
- Modify: `uc_node/src/node.rs` (`NodeConfig` doc `:194`; `start_with` `:1005-1019, 1320-1376`; `Node`/consensus struct fields near `:2812`; `rebuild_net_for_config` `:4420-4465`; `refresh_from_view` `:5731-5838`; `do_work` leader hook `:3506-3512`; new `maybe_commit_datagram_mtu`; accessors near `:2133`)
- Modify: `uc_net/src/sender.rs` (`Sender::live_mtu`, `set_live_mtu`, `budget()`; the four sites; `run`/`scratch` capacity)
- Modify: `uc_node/src/config_file.rs:243-244, 290-330, 580-599, 826-832`, its tests `:1590-1632`
- Modify: `uc_node/src/preflight.rs:31-84, 100-160`, its tests `:509-565`
- Modify: `bench-infra/scripts/m9_fleet_gate.py:262`

**Interfaces:**
- Consumes: `ProbeTable` (Task 3), `Sender::set_probe_table` / `FollowerReceiver::set_probe_table` (Task 4), `LogBuffer::set_payload_ceiling`, `CncPage::store_payload_ceiling` (Task 5), `ClusterView::datagram_mtu` (Task 1), `payload_ceiling`, `MTU_BOUND`, `RUNGS` (Task 0).
- Produces: `Sender::set_live_mtu(&mut self, Arc<AtomicUsize>)`; `Node::datagram_mtu(&self) -> u32` (the committed rung, 1408 for `0`), `Node::payload_ceiling(&self) -> usize`, `Node::probe_table(&self) -> Arc<ProbeTable>`; `PreflightError::MaxPayloadRetired`; `ConfigError::Invalid { field: "max_payload", .. }`.

- [ ] **Step 1: Failing tests**

`uc_node/src/config_file.rs` tests, next to the `admission_bytes` refusal test:

```rust
    /// Jumbo spec §7.4: the ceiling is DISCOVERED. A `max_payload` key is
    /// refused by name, pointing at discovery and at `force_jumbo_frames`.
    #[test]
    fn max_payload_is_refused_by_name_pointing_at_discovery() {
        let toml = format!("max_payload = 1312\n{MINIMAL}");
        let e = load_str(&toml).unwrap_err();
        assert!(matches!(e, ConfigError::Invalid { field: "max_payload", .. }), "{e}");
        assert!(e.to_string().contains("discover"), "{e}");
        assert!(e.to_string().contains("force_jumbo_frames"), "{e}");
    }

    /// Jumbo spec §7.2: without the key, `NodeConfig::max_payload` is the
    /// BOUND — the top rung's ceiling for this node's crypto mode.
    #[test]
    fn max_payload_bound_is_derived_from_the_top_rung_and_the_crypto_mode() {
        let (cfg, _) = load_str(MINIMAL).unwrap(); // MINIMAL has [crypto] enabled = false
        assert_eq!(cfg.max_payload, uc_protocol::v2::datagram::payload_ceiling(8960, false));
        assert_eq!(cfg.max_payload, 8896);
        let on = MINIMAL.replace("enabled = false", "enabled = true"); // adjust to MINIMAL's actual [crypto] shape
        let (cfg, _) = load_str(&on).unwrap();
        assert_eq!(cfg.max_payload, 8864);
    }
```

(Read `MINIMAL` in the test module to see how `[crypto]` is written, and adapt the `replace`; if enabling crypto in `MINIMAL` needs key material, build the crypto-on case with the smallest valid `[crypto]` section the loader accepts — the `admin_auth`/`crypto_cluster` tests show one.)

`uc_node/src/preflight.rs` tests: delete `the_mtu_refusal_reports_the_true_requirement_not_a_sentinel` and `the_mtu_budget_accounts_for_crypto_overhead` (the checks they test are gone); keep the `buffer_bytes / 4` test.

`uc_net/src/sender.rs` test (next to `mtu_budget_shrinks_by_the_crypto_overhead_so_sealed_datagrams_still_fit`):

```rust
    /// Jumbo spec §7.1: the packing budget follows the LIVE rung; a frame
    /// larger than that budget still goes out alone (read_run_validated
    /// always copies the first frame), bounded by MTU_BOUND.
    #[test]
    fn the_budget_follows_the_live_rung_and_an_oversize_frame_goes_alone() {
        let (mut s, fake) = sender_to(/* as the neighbouring tests build one, with a buffer whose max_payload is 8864 */);
        let live = Arc::new(AtomicUsize::new(1408));
        s.set_live_mtu(Arc::clone(&live));
        assert_eq!(s.budget(), 1408 - DATAGRAM_HEADER_LEN);
        live.store(8960, Ordering::Release);
        assert_eq!(s.budget(), 8960 - DATAGRAM_HEADER_LEN);
        // Append one 4 KB frame and many 64 B frames; at a 1408 live rung the
        // 4 KB frame is emitted alone in one datagram and the 64 B frames pack.
        live.store(1408, Ordering::Release);
        /* append via the test's Appender, run s.do_work() as leader, collect
           datagrams from `fake`, and assert: exactly one datagram is longer
           than 1408 and it holds exactly one frame; every other datagram is
           <= 1408. */
    }
```

Fill the elided parts from the neighbouring test's harness (`sender.rs:1951-2011`, `Fake` + `sender_to`); the assertion is what matters.

- [ ] **Step 2: Run to verify red** — `cargo test -p uc_node --lib max_payload_` (no refusal yet), `cargo test -p uc_net the_budget_follows` (no `set_live_mtu`).

- [ ] **Step 3: `uc_net` sender** — field `live_mtu: Option<Arc<AtomicUsize>>` (init `None`); `pub fn set_live_mtu(&mut self, v: Arc<AtomicUsize>)`;

```rust
    /// The datagram body budget this pass: the LIVE rung (jumbo spec §7.1)
    /// when the node wired one, else `cfg.mtu`, less the header and the
    /// crypto tag. `read_run_validated` copies a first frame even when it is
    /// larger than this, so a frame the committed ceiling admitted is sent
    /// alone, never split and never withheld.
    fn budget(&self) -> usize {
        let mtu = self
            .live_mtu
            .as_ref()
            .map(|m| m.load(Ordering::Relaxed))
            .unwrap_or(self.cfg.mtu);
        mtu - DATAGRAM_HEADER_LEN - self.cfg.crypto_overhead()
    }
```

Replace the four `let budget = self.cfg.mtu - DATAGRAM_HEADER_LEN - self.cfg.crypto_overhead();` lines with `let budget = self.budget();`. Size `run`/`scratch` with `Vec::with_capacity(MTU_BOUND)`. The `Sender::new` assert compares against `MTU_BOUND`, not `cfg.mtu`:

```rust
        assert!(
            align_frame_len(HEADER_LEN + buffer.max_payload())
                + DATAGRAM_HEADER_LEN
                + cfg.crypto_overhead()
                <= MTU_BOUND,
            "a max-size frame (+ crypto overhead, if enabled) must fit one datagram at the \
             top rung (MTU_BOUND) — the buffer's max_payload is the bound, jumbo spec §7.2"
        );
```

- [ ] **Step 4: `uc_node` config and preflight** — `config_file.rs`: `NodeConfigFile.max_payload` becomes `#[serde(default)] max_payload: Option<usize>` (accepted only to refuse by name, exactly like `admission_bytes`); delete `default_max_payload` and `default_max_payload_for_test`. After the `admission_bytes` refusal:

```rust
    // Jumbo spec §7.4: the ceiling is discovered from the path, not stated.
    if f.max_payload.is_some() {
        return Err(ConfigError::Invalid {
            field: "max_payload",
            detail: "max_payload is no longer configurable (2.12.0): the command payload \
                     ceiling is discovered from the path MTU between nodes and committed \
                     cluster-wide. Delete the line. To REQUIRE jumbo frames, set \
                     force_jumbo_frames = true instead."
                .into(),
        });
    }
```

Where `NodeConfig` is built (`:826`): `max_payload: uc_protocol::v2::datagram::payload_ceiling(uc_protocol::v2::datagram::MTU_BOUND, matches!(crypto, CryptoConfig::Enabled { .. })),` with the comment "the BOUND (spec §7.2): what the buffer is sized for; the live door is the committed rung's ceiling". `NodeConfig::max_payload`'s doc in `node.rs` becomes: "The payload BOUND this node's log buffer is sized for (jumbo spec §7.2) — `payload_ceiling(MTU_BOUND, crypto)` from `node.toml`; tests pass small values with small buffers. The live door is `min(this, payload_ceiling(committed rung, crypto))`."

`preflight.rs`: delete the `PayloadExceedsMtu` and `PayloadTooSmallForScheduleTable` variants and their check blocks (`:100-160`) and the now-unused imports (`MTU_DEFAULT`, `SCHEDULE_*`, `CRYPTO_OVERHEAD`, `align_frame_len`…). Keep `PayloadTooLarge` (bound vs `buffer_bytes / 4`). Grep `PayloadExceedsMtu\|PayloadTooSmallForScheduleTable` across `uc_node/src`, `uc_node/tests/daemon_refusals.rs`, `docs/` and remove or reword each hit (the docs hits are Task 9's).

`bench-infra/scripts/m9_fleet_gate.py:262`: `("max-payload-retired", "max_payload = 65536", "max_payload"),` — the row still asserts the refusal names the field.

- [ ] **Step 5: `uc_node` wiring** — in `start_with`:

```rust
        // Jumbo spec §5.1: one discovery ledger for the three agents.
        let probe_table = ProbeTable::new(ProbeCadence::default());
        let live_mtu = Arc::new(AtomicUsize::new(MTU_DEFAULT));
        // The door starts at the BASELINE, whatever the bound (spec §7.1),
        // before any agent runs; refresh_from_view raises it at commit.
        buffer.set_payload_ceiling(
            cfg.max_payload.min(payload_ceiling(MTU_DEFAULT, crypto_send.is_some())),
        );
        cnc.store_payload_ceiling(buffer.payload_ceiling() as u64);
```

(Place it after `buffer` and `cnc` exist and after `crypto_send` is known; `crypto_send.is_some()` is the crypto-on predicate the sender config already uses.) Then `sender.set_probe_table(Arc::clone(&probe_table)); sender.set_live_mtu(Arc::clone(&live_mtu));` before the sender agent spawns, `receiver.set_probe_table(Arc::clone(&probe_table));` before the receiver agent spawns, and seed the peer set at boot: `probe_table.set_peers(&sender_members);` (the same voters+learners-minus-self list `set_members` gets). Store `probe_table`, `live_mtu`, `crypto_on: bool` on the consensus struct and `probe_table` on `Node` (for tests).

`rebuild_net_for_config`: after computing `members`, `self.probe_table.set_peers(&members);`.

`refresh_from_view`, after the `admission_bytes` block:

```rust
        // Jumbo spec §7.1: the committed rung → the sender's budget, the
        // appender's door, the client-facing cnc word. `0` = baseline.
        let rung = match self.cluster_view.datagram_mtu.load(Ordering::Acquire) {
            0 => MTU_DEFAULT,
            r => r as usize,
        };
        if rung != self.live_mtu.load(Ordering::Relaxed) {
            let ceiling = self.max_payload.min(payload_ceiling(rung, self.crypto_on));
            // Order: door and cnc word first, then the sender's budget — a
            // client may see the wider door only once the appender takes it.
            self.buffer.set_payload_ceiling(ceiling);
            self.cnc.store_payload_ceiling(ceiling as u64);
            self.live_mtu.store(rung, Ordering::Release);
            crate::obs_event!(
                Info,
                "payload_ceiling_adopted",
                node = self.id as u64,
                datagram_mtu = rung as u64,
                ceiling = ceiling as u64
            );
        }
```

(`self.buffer` is the consensus agent's `Arc<LogBuffer>` — use whatever field name holds it, e.g. the one `read_run_validated`'s callers use; `self.max_payload` exists — `fsm_lag_from_setting` reads it.)

The leader's commit rule — in `do_work` next to `maybe_issue_cadence_snapshot()`:

```rust
        if serving && !hold_clients {
            did |= self.maybe_commit_datagram_mtu();
        }
```

```rust
    /// Jumbo spec §5.3: while leading, once every member (voters and
    /// learners, minus self) has answered, commit `min over the table` when
    /// it exceeds the committed rung. Evaluated at most every 100 ms — a
    /// timestamp compare per pass, nothing else on the hot path.
    fn maybe_commit_datagram_mtu(&mut self) -> bool {
        if self.pass_mono_ns < self.next_mtu_check_ns {
            return false;
        }
        self.next_mtu_check_ns = self.pass_mono_ns + 100_000_000;
        let view_position = self.cluster_view.position.load(Ordering::Acquire);
        if self.last_cluster_append > view_position {
            return false; // single-in-flight: a CLUSTER command is above commit
        }
        let committed = self.cluster_view.datagram_mtu.load(Ordering::Acquire);
        let membership = self.cluster_view.membership();
        let members: Vec<SocketAddr> = membership
            .voters
            .iter()
            .chain(membership.learners.iter())
            .filter(|(id, _)| *id != self.id)
            .map(|(_, a)| addr_of(*a))
            .collect();
        let Some(min) = self.probe_table.table_min(&members) else {
            return false; // someone has not answered yet
        };
        if min <= committed.max(MTU_DEFAULT as u32) {
            return false; // nothing to raise (a lower min is a degraded path — plan 2 reports it)
        }
        let mut settings = self.cluster_view.to_state().settings;
        settings.datagram_mtu = min;
        let cmd = ClusterCommand::Settings(settings);
        if self.validate_cluster_command(&cmd).is_err() {
            return false;
        }
        match self.append_cluster_frame(&cmd) {
            Ok(position) => {
                crate::obs_event!(
                    Info,
                    "datagram_mtu_proposed",
                    node = self.id as u64,
                    datagram_mtu = min as u64,
                    position = position
                );
                true
            }
            Err(_) => false, // WouldOverrun: next check
        }
    }
```

(`next_mtu_check_ns: u64` is a new field, init 0. `pass_mono_ns` is the pass's monotonic reading already used for the tick.) `Node` accessors:

```rust
    /// Jumbo: the committed datagram rung this node applies (1408 = baseline).
    pub fn datagram_mtu(&self) -> u32 {
        match self.cluster_view().datagram_mtu.load(Ordering::Acquire) {
            0 => MTU_DEFAULT as u32,
            r => r,
        }
    }
    /// Jumbo: the live command payload ceiling (the cnc 3984 word).
    pub fn payload_ceiling(&self) -> usize {
        self.cnc.payload_ceiling() as usize
    }
    pub fn probe_table(&self) -> Arc<ProbeTable> {
        Arc::clone(&self.probe_table)
    }
```

- [ ] **Step 6: Run** the Global Constraints list. Watch for `uc_node/tests/daemon_refusals.rs` (it may assert the old MTU refusal — reword to the new `max_payload` refusal), `uc_gateway/examples/m12_gate.rs` and `uc_node/examples/m5_gate.rs` (they set `NodeConfig.max_payload` to a literal — still valid: it is the bound). `cargo test -p uc_node --lib && cargo test -p uc_node --test smoke --test failover --test learner --test reconfig --test services --test daemon_refusals` — PASS.

- [ ] **Step 7: Commit**

```bash
git add uc_node/src/node.rs uc_node/src/config_file.rs uc_node/src/preflight.rs uc_net/src/sender.rs bench-infra/scripts/m9_fleet_gate.py uc_node/tests/daemon_refusals.rs
git commit -m "feat(uc_node, uc_net): jumbo — live rung from the committed view, probes wired, the leader's commit rule, max_payload retired"
```

---

### Task 7: Clients read the live ceiling

**Files:**
- Modify: `uc_client/src/engine.rs:74-85 (doc), 203-217 (Shared), 341-357 (attach), 452-469 (submit)`
- Modify: `uc_gateway/src/edge.rs:341-360 (Shared), 776-830 (start), 1316-1333 (check)`

**Interfaces:**
- Consumes: `CncPage::payload_ceiling()` (Task 5).
- Produces: `EngineConfig::max_payload: Option<usize>` keeps its meaning as an OVERRIDE; `None` now means "the node's LIVE ceiling, read per submit".

- [ ] **Step 1: Failing tests**

`uc_client` (in `engine.rs`'s tests or `uc_client/tests/`, wherever `attach` is exercised against a heap/temp cnc page):

```rust
    /// Jumbo spec §7.3: a client attached before the ceiling moved sees the
    /// new value — the door is the live cnc word, not the attach-time header.
    #[test]
    fn the_submit_door_follows_the_live_cnc_word() {
        /* build a node-less instance dir with a cnc page whose header
           max_payload = 8864 and payload_ceiling = 1312 (the fixture the
           other attach tests use; call page.store_payload_ceiling(1312)),
           attach an Engine with cfg.max_payload = None, then: */
        let e = send.send(/* 2000-byte command */).unwrap_err();
        assert!(matches!(e, SubmitError::PayloadTooLarge { len: 2000, max: 1312 }));
        page.store_payload_ceiling(8864);
        assert!(send.send(/* the same 2000-byte command */).is_ok());
        // An explicit override still wins in both directions.
    }
```

`uc_gateway`: the edge equivalent under `--features test-util` — an edge whose node page reads 1312 refuses a 2000 B frame with `RETRY_PAYLOAD_TOO_LARGE`, then accepts after `store_payload_ceiling(8864)` (follow the shape of the existing `PAYLOAD_TOO_LARGE` edge test — grep `RETRY_PAYLOAD_TOO_LARGE` in `uc_gateway/tests` and `edge.rs`'s test module).

- [ ] **Step 2: Run to verify red** — both fail: the door is the attach-time header value.

- [ ] **Step 3: Implement**

`uc_client/src/engine.rs`: `Shared.max_payload: Option<usize>` stays as the OVERRIDE only; `attach` stores `cfg.max_payload` (not `.or(meta.max_payload)`); the submit check becomes

```rust
        let max = match s.max_payload {
            Some(m) => m,
            // Jumbo spec §7.3: the node's LIVE ceiling, one Acquire load. A
            // 3.1-era page (0 here) falls back to the header bound.
            None => match s.cnc.payload_ceiling() {
                0 => s.header_max_payload,
                c => c as usize,
            },
        };
        if wire_len > max {
            return Err(SubmitError::PayloadTooLarge { len: wire_len, max });
        }
```

with `header_max_payload: usize` (from `meta.max_payload`) added to `Shared`. Update `EngineConfig::max_payload`'s doc: "`None` (the default) follows the attached node's LIVE ceiling (the cnc `payload_ceiling` word, read per submit), so a client attached before the cluster raised its ceiling sees the raise; `Some(n)` pins the door — the dev-time check the jumbo how-to describes."

`uc_gateway/src/edge.rs`: keep the `CncPage` (do not `drop(cnc)`), store `cnc: Arc<CncPage>` and `header_max_payload` in `Shared`, and the check reads `shared.live_max_payload()`:

```rust
    fn live_max_payload(&self) -> usize {
        match self.cnc.payload_ceiling() {
            0 => self.header_max_payload,
            c => c as usize,
        }
    }
```

Replace `shared.max_payload` at `:1328` (and any other reader) with `shared.live_max_payload()`.

- [ ] **Step 4: Run** `cargo test -p uc_client && cargo test -p uc_gateway --features test-util && cargo clippy -p uc_gateway --features test-util --all-targets -- -D warnings` — PASS.

- [ ] **Step 5: Commit**

```bash
git add uc_client/src/engine.rs uc_gateway/src/edge.rs uc_client/tests uc_gateway/tests
git commit -m "feat(uc_client, uc_gateway): jumbo — the submit door reads the live cnc ceiling per submit"
```

---

### Task 8: The in-process proof — three nodes, a capped path, a silent member, an early client

**Files:**
- Create: `uc_node/tests/jumbo.rs`

**Interfaces:**
- Consumes: `Node::{datagram_mtu, payload_ceiling, probe_table, cluster_view, can_serve, is_leader, start_with_socket}`; `FaultConfig::max_datagram`; `uc_client::Engine` (or the `RemoteClient`-free local client the other tests use).

- [ ] **Step 1: Write the test file** (copy `make_config`/`spawn_cluster`/`await_single_leader` from `uc_node/tests/query_barrier.rs:66-165`, with `faults` a parameter and `max_payload: 8864, buffer_bytes: 1 << 22`; register the same `CountSm` service those tests use, so a client can submit):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Jumbo spec §10, in-process items (a), (b), (e): discovery lands on the
//! capped rung on every node; the ceiling never rises while one member is
//! silent; a client attached before the raise sees it.

/* … harness copied from query_barrier.rs, parameterised by FaultConfig … */

fn await_rung(nodes: &[Node], want: u32, secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if nodes.iter().all(|n| n.datagram_mtu() == want) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "rungs {:?}, wanted {want} everywhere",
            nodes.iter().map(|n| n.datagram_mtu()).collect::<Vec<_>>()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn discovery_lands_on_the_capped_rung_on_every_node() {
    let c = spawn_cluster(3, FaultConfig { max_datagram: 8832, ..FaultConfig::default() });
    await_single_leader(&c.nodes, 10);
    await_rung(&c.nodes, 8832, 15);
    for n in &c.nodes {
        assert_eq!(n.payload_ceiling(), 8768, "crypto-off ceiling at 8832");
    }
}
```

Then:

```rust
#[test]
fn an_uncapped_loopback_cluster_reaches_the_top_rung() {
    let c = spawn_cluster(3, FaultConfig::default());
    await_single_leader(&c.nodes, 10);
    await_rung(&c.nodes, 8960, 15);
    for n in &c.nodes {
        assert_eq!(n.payload_ceiling(), 8864, "min(bound 8864, ceiling(8960, off) = 8896)");
    }
}

#[test]
fn the_ceiling_holds_at_baseline_while_one_member_is_silent() {
    // Bind all three sockets, start only two.
    let (dir, socks, members) = bind_members(3);
    let mut nodes = Vec::new();
    for i in 0..2 {
        nodes.push(start_node(&dir, i, &members, socks[i].try_clone().unwrap(), FaultConfig::default()));
    }
    await_single_leader(&nodes, 10);
    std::thread::sleep(Duration::from_secs(3)); // > the fast cadence's whole window
    for n in &nodes {
        assert_eq!(n.datagram_mtu(), 1408, "a silent member pins the ceiling");
        assert_eq!(n.payload_ceiling(), 1344, "crypto-off ceiling at the 1408 baseline");
    }
    nodes.push(start_node(&dir, 2, &members, socks[2].try_clone().unwrap(), FaultConfig::default()));
    await_rung(&nodes, 8960, 20);
}

#[test]
fn a_client_attached_before_the_raise_sees_it() {
    let c = spawn_cluster(3, FaultConfig::default());
    let leader = await_single_leader(&c.nodes, 10);
    // Attach BEFORE discovery completes: the door reads 1344 (crypto off).
    let (send, poll) = attach_client(&c.dirs[leader]);
    let big = vec![7u8; 4000];
    let early = send.send(/* big */);
    // Either refused (ceiling still 1344) or accepted (discovery already
    // landed) — what matters is the state AFTER the raise.
    await_rung(&c.nodes, 8960, 15);
    let ok = send.send(/* big again */);
    assert!(ok.is_ok(), "4000 B under a discovered ceiling of 8864: {ok:?}");
    /* poll for the response and assert the command applied */
    let _ = (early, poll);
}
```

Fill `bind_members`/`start_node`/`attach_client` from the harness you copied (`query_barrier.rs` attaches through `uc_client`; use its shape).

- [ ] **Step 2: Watch it red** — run once with Task 6's `maybe_commit_datagram_mtu` early-`return false` inserted at its top; every rung assertion must fail at 1408. Revert. Record in the commit body.

- [ ] **Step 3: Run** `cargo test -p uc_node --test jumbo` — 4 PASS. Then the full Global Constraints list.

- [ ] **Step 4: Commit**

```bash
git add uc_node/tests/jumbo.rs
git commit -m "test(uc_node): jumbo — three-node proof: capped rung, silent member holds baseline, early client sees the raise"
```

---

### Task 9: The reference statements this plan made false

**Files:**
- Modify: `docs/reference/wire-protocol.md:13-20, 51, 82-83` (version `0.8.0` pending; the kinds table gains 24/25 `pairwise`; `MTU_DEFAULT` row says "baseline rung; the ladder is `RUNGS`"); `:242-243, 299, 392` (the ceiling paragraphs: "1344/1312 at the baseline rung; up to 8896/8864 at the top rung once discovered")
- Modify: `docs/reference/cnc-page.md:19, 65-66` (version 3.2 pending; a `3984 payload_ceiling` row after 3976)
- Modify: `docs/reference/configuration.md:156-159, 231-241` (`max_payload` → "retired in 2.12.0, refused by name; the ceiling is discovered — see the jumbo explainer (plan 2)"; the three `max_payload` rows in the semantics table go)
- Modify: `docs/reference/semver-policy.md` (a "The pending `2.12.0` flag day" paragraph after the 2.11.0 one: wire `0.7.0` → `0.8.0`, cnc `3.1` → `3.2`, Settings v1 accepted on read so no wipe)
- Modify: `packaging/node.example.toml:75-91` (the `max_payload` block → a three-line note that it is discovered, and a commented `# force_jumbo_frames = false` placeholder is NOT added yet — plan 2 adds the key)
- Modify: `CLAUDE.md` "Command payload ceiling" standing fact: first sentence becomes "**Command payload ceiling: discovered per cluster** — 1344 B crypto-off / 1312 B crypto-on at the 1408 B baseline rung every cluster starts from, up to 8896 / 8864 at the 8960 B rung once every path has proven it (`2.12.0`, spec `docs/superpowers/specs/2026-09-10-uc2-jumbo-frame-discovery-design.md`). The arithmetic is unchanged (`payload_ceiling(rung, crypto)`); what moved is that the rung is committed cluster data, not a source constant, and `max_payload` is no longer a `node.toml` key."

- [ ] **Step 1: Make the edits.** Each page keeps its 2.11.0 facts as shipped and marks the new ones "2.12.0 pending", the way the 2.11.0 flag day was documented before its tag.

- [ ] **Step 2: Verify** `grep -rn "not configurable" docs/reference/limits.md docs/reference/wire-protocol.md` — `limits.md` still says the MTU is not configurable; leave `limits.md` to plan 2's sweep but add a one-line "(2.12.0 pending: discovered — see wire-protocol.md)" to its row 29 now so the two pages do not contradict. `git grep -n "PayloadExceedsMtu\|PayloadTooSmallForScheduleTable" -- docs` returns only historical release notes (`docs/releases.md`, gate docs), which stay as history.

- [ ] **Step 3: Commit**

```bash
git add docs/reference/wire-protocol.md docs/reference/cnc-page.md docs/reference/configuration.md docs/reference/semver-policy.md docs/reference/limits.md packaging/node.example.toml CLAUDE.md
git commit -m "docs: jumbo plan 1 — wire 0.8.0 kinds, cnc 3.2 word, max_payload retired, the ceiling is discovered"
```

---

## Self-review

**Spec coverage (plan 1's share).** §2 → Tasks 2, 5, 6 (one value, DF, receiver unchanged). §4.1 → Task 0 (`RUNGS`, `MTU_BOUND`, `JUMBO_MIN_RUNG`, `payload_ceiling`). §4.2 → Tasks 0, 4 (kinds, bodies, exact-length rule, pairwise scope, `0.8.0`). §4.3 → Task 2 (DF; the EMSGSIZE *counter* is plan 2's §9). §5.1 → Tasks 3, 4, 6 (cadence, prober before the role gate, responder, `set_peers` at boot and on membership). §5.2 → Task 3 (`own_min_rung`). §5.3 → Tasks 3, 6 (`table_min`, the commit rule, learners count, single-in-flight). §5.5 → Task 1 (v2, v1 accepted, validation as rung, monotone `max`, not operator-writable, `show`). §7.1 → Tasks 5, 6 (live rung, `budget()`, oversize-alone via `read_run_validated`'s first-frame rule — pinned by a test, the `Sender::new` assert against `MTU_BOUND`, the appender's one load). §7.2 → Tasks 5, 6 (bound, capacity assert unchanged, `MIN_FSM_LAG_BYTES` and the `SNAP_BEGIN` assert untouched — Global Constraints). §7.3 → Tasks 5, 7 (3984, cnc 3.2, per-submit reads, 3.1-page fallback). §7.4 → Task 6 (`max_payload` refused, the two preflight checks removed, m9 row; the `uc_remote` literal is plan 2). §10 unit + fault-layer + in-process (a), (b), (e) → Tasks 3, 4, 8. §11's reference pages this plan touches → Task 9. **Deferred to plan 2 by design:** §5.4, §6, §8, §9, `uc_remote`, the fuzz target, the fleet gate, the explainer/how-to/RELEASES sweep.

**Placeholder scan.** Task 6 Step 1's second config test and Task 7/8's tests name fixtures the implementer must read from the neighbouring tests (`MINIMAL`'s `[crypto]` shape, `sender_to`, `query_barrier.rs`'s attach); each names the file and line range to copy from and states the assertion in full, so the deliverable is unambiguous. No "TBD"/"handle edge cases".

**Type consistency.** `ProbeTable::{new(ProbeCadence) -> Arc<ProbeTable>, set_peers(&[SocketAddr]), due(u64) -> Vec<(SocketAddr, Vec<u32>)>, on_ack(SocketAddr, u32, u32), own_min_rung() -> u32, table_min(&[SocketAddr]) -> Option<u32>, note_unsent(), get(SocketAddr) -> Option<PeerProbe>}` is used identically in Tasks 3, 4, 6, 8. `payload_ceiling(rung: usize, crypto_on: bool) -> usize` in Tasks 0, 6, 8. `Settings::datagram_mtu: u32` and `ClusterView::datagram_mtu: AtomicU32` in Tasks 1, 6, 8. `CncPage::{payload_ceiling() -> u64, store_payload_ceiling(u64)}` in Tasks 5, 6, 7. `LogBuffer::{payload_ceiling() -> usize, set_payload_ceiling(usize)}` in Tasks 5, 6. `Sender::{set_probe_table, set_live_mtu(Arc<AtomicUsize>), budget()}` in Tasks 4, 6. `FollowerReceiver::set_probe_table` in Tasks 4, 6. `Node::{datagram_mtu() -> u32, payload_ceiling() -> usize, probe_table()}` in Tasks 6, 8.
