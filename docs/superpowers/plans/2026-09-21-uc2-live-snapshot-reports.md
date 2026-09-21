# Live snapshot-hash reports and the pins-authoritative gate (plan B3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make plan B1's `SnapshotReport` record *produced* on a live cluster — every node hashes each row artifact as it is built, reports `(row, P, hash)` to the leader, and the leader appends one `SnapshotReport` per `(row, P)` once a quorum has reported or a timeout elapses, so the cluster FSM's deterministic verdict, the `uc2_snapshot_hash_mismatch` gauge and `Uc2SnapshotHashDiverged` (all shipped in B1) light up for real. And close the boot window plan B2 deferred: a service cannot pass the node's readiness gate until the node has heard its leader and applied cluster state up to commit, so a pin committed above this node's artifact is never invisible to an attaching service.

**Architecture:** The row artifact is written by the *service* process's builder (`uc_service::builder_agent` → `SnapshotStore::publish`), not the node, so the hash is taken there, as the bytes stream (no extra I/O), and published in the row's cnc slot line 7 word `+504` (`artifact_hash`, service-written, stored before `snapshot_pos`). On the node's local set-complete edge the consensus agent reads each declared row's hash and sends a new pairwise datagram `SNAP_REPORT` (kind 26) to the leader; the leader (feeding itself directly) keeps one pending vector per row for the newest instant, and appends `ClusterCommand::SnapshotReport` through the existing single-in-flight cluster append when voters reporting reach quorum or 5 s pass — everything downstream (apply, verdict, gauge, event, alert, `uc2ctl upgrade show`) already exists. The readiness gate moves from `Node::start` into the consensus pass: `services_declared` is published once the node knows its leader and the cluster agent's view has reached the commit position; `ServiceBuilder` and `Client::connect` gain a bounded wait on `NodeBooting` so existing callers keep working.

**Tech Stack:** Rust 1.96 (MSRV 1.89); crates `uc_protocol`, `uc_crypto` (scope table), `uc_net` (receiver decode), `uc_node`, `uc_service`, `uc_log`, `uc_client`, `uc_ctl`; `sha2`.

**Spec:** `docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md` §6.5.2 (items 1–5 and "why through the log"), §2.5 (`SnapshotReport`), §11 item 13. Plan B1 (`docs/superpowers/plans/2026-09-20-uc2-upgrade-pin-and-snapshot-report.md`) shipped the record, verdict, gauge, event and alert; plan B2 (`…/2026-09-21-uc2-pinned-install-at-attach.md`) deferred the complete boot gate here (its C1 comment in `uc_node/src/node.rs` at the `store_services_declared` site).

## Global Constraints

- **Same flag day as B1/B2 (2.13.0, unshipped):** wire `0.9.0` gains one pairwise datagram kind, **`SNAP_REPORT = 26`**; the `0.9.0` doc entry says so. `CNC_V2_VERSION` stays `(3 << 24) | (3 << 16)`; the new cnc word rides it.
- **`SNAP_REPORT` body, 24 B exactly:** `row u8 @0 ‖ reserved [u8; 3] @1 ‖ node_id u32 @4 ‖ position u64 @8 ‖ hash u64 @16`, LE; reserved zero; `Scope::Pairwise`; sealed per destination like every pairwise kind. Header `position` unused (0).
- **The hash** is SHA-256 over the artifact's PAYLOAD bytes (everything after the 24-byte `ULTSNAP2` envelope — the envelope is identical across nodes anyway), truncated to its first 8 bytes as `u64` LE; `0` = no hash published. cnc slot line 7 `+504 artifact_hash` (`CNC_SVC_OFF_ARTIFACT_HASH = 504`), **service-written** by the builder agent, stored BEFORE `snapshot_pos` (so a node that `Acquire`-loads `snapshot_pos == P` sees the hash of the artifact at P), pinned in both crates' offset tests.
- **Who reports:** every node (voters and learners) for every declared row, on its own LOCAL set-complete edge (`snapshot_set_complete` with `source = "local"`, not `fetch`); the leader feeds its own report into its collector without a datagram. A follower sends to the leader hint's address; no hint ⇒ counted and skipped (`snapshot_report_unsent`).
- **Leader collection:** per row, ONLY the newest instant is pending (an older instant's late report is dropped); reports for an instant at or below the row's held report in the committed view are dropped; membership-checked `node_id` (a non-member is dropped); append when reporters that are VOTERS ≥ quorum, or `SNAP_REPORT_TIMEOUT_NS = 5 s` after the first report — whichever first; the append is `ClusterCommand::SnapshotReport { row, position, hashes }` with `hashes` from a `BTreeMap<u32, u64>` (sorted, non-empty by construction — B1's `encode_command` precondition); gated by `last_cluster_append > view_position` like every cluster append (retry next pass); the pending map is cleared on `BecomeFollower`/halt.
- **The gate:** `services_declared` is stored by the consensus pass, once per incarnation, when **(a)** the node knows a leader (it is the leader, or a leader hint is set) **and (b)** `cluster_view.position >= cnc.counters().commit`. `fsm_lag_bytes` stays where it is (before). Consequence, documented: a service or client cannot attach to a node that has not joined its cluster — `NodeBooting` until then. `ServiceBuilder::start*` and `uc_client::Client::connect` retry `NodeBooting` internally for a bounded `boot_wait` (default 10 s; `Duration::ZERO` = no wait), configurable on `ServiceConfig` / `EngineConfig`.
- **Apply stays deterministic;** nothing here enters an FSM's `apply`. No hot-loop change on the apply agent; the builder's hashing is on the builder thread.
- **No panic from bytes on the wire:** the body decoder is total (short/reserved → `None`), covered by the datagram fuzz target if it dispatches on kind.
- `cargo fmt --all -- --check`; workspace clippy `-D warnings` + the feature-gated runs (`uc_crashtest --features hard-crash-tests`, `uc_lincheck --features replay-bin`, `uc_service --features apply-profile`, `uc_gateway --features test-util`); fuzz crate builds; frozen numbers get tests. **No `RELEASES.md`/`docs/releases.md`/`CLAUDE.md` edits** (plan D). No `git stash`. Scratch under `$HOME/scratch/`.

### Errata against the spec text (decided while planning; Task 7 records them under §6.5.2)

1. **The hash is taken service-side, not node-side.** §6.5.2 item 1 says "`builder_agent` already streams the service's bytes … the node is the right party — a service should not grade its own image". The builder agent runs in the *service* process (`uc_service::builder_agent`); the node never streams a row artifact and only sees the file. Hashing where the bytes are written costs no I/O; a node-side re-read would cost the artifact's size per instant on a node agent. Under the threat model (a compromised host is out of scope) the two are equivalent.
2. **The body carries `node_id`**, so the leader can membership-check and key the vector without a reverse address lookup (the `READ_PROBE_ACK` shape).
3. **Learners report too** (§6.5.2 says "N nodes"); quorum is counted over voters only.
4. **`SNAP_REPORT_TIMEOUT_NS = 5 s`** is fixed (a leader-side constant), not a setting.
5. **The metrics-only complement (§6.5.2 "worth shipping as a complement") is dropped:** a per-instant hash needs the position as a label to be comparable, which is unbounded cardinality; B1's `uc2_snapshot_hash_mismatch` + `Uc2SnapshotHashDiverged` is the alerting surface and `uc2ctl status` prints `artifact_hash=` per row for a manual cross-node comparison.
6. **The cluster artifact (`service_id = 255`) is not reported** — `SnapshotReport.row < 8`; backlog.
7. **The pins-authoritative gate is the leader-and-commit condition above**, not a per-row cnc word: it makes "no pin" a statement about committed cluster state rather than about what this node's artifact happened to contain.

---

## File structure

| file | responsibility |
|---|---|
| `uc_protocol/src/v2/cnc.rs`, `uc_log/src/cnc.rs` | `+504 artifact_hash` (line 7), offset pins |
| `uc_service/src/snapshots.rs`, `builder_agent.rs`, `Cargo.toml` | hash while streaming; store the word |
| `uc_protocol/src/v2/datagram.rs`, `uc_crypto/src/transport.rs` | kind 26, body codec, scope |
| `uc_net/src/receiver.rs` | `NetEvent::SnapReport`, decode, routing |
| `uc_node/src/node.rs` | report on the set edge; leader collection + append; the gate; `Node::snapshot_report(row)` |
| `uc_service/src/lib.rs`, `config.rs`; `uc_client/src/client.rs`, `engine.rs` | bounded `NodeBooting` wait |
| `uc_ctl/src/main.rs` | `status` prints `artifact_hash=` |
| `uc_node/tests/learner.rs` (or a new `snapshot_reports.rs` reusing its harness) | multi-node e2e |
| docs | `wire-protocol.md`, `cnc-page.md`, `limits.md`, `monitor-a-cluster.md`, `uc2ctl.md`, `upgrade-a-cluster.md`, `uc2-cluster-fsm-explained.md`, `attack-surface.md`, VERIFICATION, spec errata |

---

### Task 1: The artifact hash — computed while streaming, published on line 7

**Files:**
- Modify: `uc_protocol/src/v2/cnc.rs` (after `CNC_SVC_OFF_FREEZE_NS` ~371; the layout comment ~322; the frozen-offsets test ~800), `uc_log/src/cnc.rs` (`ServiceIdentityLine` ~352-380: the `_pad: [u64; 1]` becomes `artifact_hash: AtomicU64`; accessors; `const _` offset assert), `uc_service/Cargo.toml` (`sha2 = { workspace = true }`), `uc_service/src/snapshots.rs` (`publish` returns `(PathBuf, u64)`; a `HashingWriter` adapter), `uc_service/src/builder_agent.rs` (store the hash BEFORE `snapshot_pos`; tests), `uc_ctl/src/main.rs` (`status` prints ` artifact_hash=0x{:016x}`), `docs/reference/cnc-page.md` (the `+504` row)
- Test: both cnc test modules; `uc_service` snapshots + builder tests; `uc_ctl`

**Interfaces:**
- `uc_protocol::v2::cnc::CNC_SVC_OFF_ARTIFACT_HASH: usize = 504`.
- `uc_log::cnc::ServiceIdentityLine::{artifact_hash() -> u64, store_artifact_hash(u64)}`.
- `uc_service::snapshots::artifact_hash_of(payload: &[u8]) -> u64` (pure: SHA-256, first 8 bytes LE) and `SnapshotStore::publish(pos, version, write) -> Result<(PathBuf, u64), SnapshotError>` — the `u64` is the hash of the payload bytes the `write` closure produced.

- [ ] **Step 1: Write the failing tests**

`uc_protocol/src/v2/cnc.rs` (the line-7 assertions):
```rust
        // plan B3: the row's artifact hash, service-written after freeze/stream.
        assert_eq!(CNC_SVC_OFF_ARTIFACT_HASH, 504);
        assert_eq!(CNC_SVC_OFF_ARTIFACT_HASH, CNC_SVC_OFF_FREEZE_NS + 8);
        const { assert!(CNC_SVC_OFF_ARTIFACT_HASH + 8 <= CNC_SERVICE_SLOT_STRIDE, "line 7 is now full") };
```
`uc_log/src/cnc.rs`: `artifact_hash()` is 0 at init, `store_artifact_hash(0xDEAD)` reads back, slots independent.
`uc_service/src/snapshots.rs`:
```rust
    #[test]
    fn artifact_hash_is_sha256_of_the_payload_truncated() {
        use sha2::{Digest, Sha256};
        let want = u64::from_le_bytes(Sha256::digest(b"payload")[..8].try_into().unwrap());
        assert_eq!(artifact_hash_of(b"payload"), want);
        assert_ne!(artifact_hash_of(b"payload"), artifact_hash_of(b"payloae"));
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        let (_path, h) = store.publish(4096, 1, |w| { w.write_all(b"pay")?; w.write_all(b"load")?; Ok(()) }).unwrap();
        assert_eq!(h, want, "the hash covers the payload as streamed, envelope excluded");
    }
```
`uc_service/src/builder_agent.rs`: in `a_successful_job_publishes_the_file_and_the_cnc_marker_then_clears_busy`, assert `cnc.service_slot(0).identity.artifact_hash() == artifact_hash_of(b"snapshot-bytes")`; add a test that the hash word is written before the marker (drive the builder and assert both; the ORDER is asserted structurally by reading the code — say so).

- [ ] **Step 2: Run to verify they fail** — `cargo test -p uc_protocol cnc && cargo test -p uc_log && cargo test -p uc_service snapshots builder_agent` → compile errors.

- [ ] **Step 3: Implement**

`snapshots.rs`:
```rust
/// SHA-256 over `payload`, first 8 bytes as `u64` LE — the per-artifact hash
/// every node reports for the live determinism check (spec §6.5.2). Over
/// the PAYLOAD only: the 24-byte envelope is identical across nodes anyway.
pub fn artifact_hash_of(payload: &[u8]) -> u64 { /* sha2 */ }

struct HashingWriter<'a> { inner: &'a mut dyn Write, hasher: sha2::Sha256 }
impl Write for HashingWriter<'_> { /* write → hasher.update + inner.write; flush → inner */ }
```
`publish` writes the envelope to the file directly, then wraps the file in `HashingWriter` for `write(&mut hw)`, finalises the digest, and returns `(final_path, hash)`. Every existing `publish` caller destructures the tuple (`builder_agent.rs`, the B2 tests in `uc_service/tests/pinned_attach.rs` that publish by hand — grep `\.publish(`).
`builder_agent.rs`: `Ok((_path, hash)) => { slot.identity.store_artifact_hash(hash); slot.snapshot_pos.store_release(pos); }` with the ordering comment. `uc_log` line 7: `artifact_hash: AtomicU64` in the `_pad` position (line stays 64 B), accessors Release/Acquire, the `const _` assert. `uc_ctl` `status`: ` artifact_hash=0x{:016x}` after `pinned_from=`. `cnc-page.md`: `| 504 | artifact_hash (line 7) — u64, SHA-256[..8] of the row's newest artifact payload | **service** (builder agent), stored BEFORE snapshot_pos — plan B3 |`; line 7 is now full.

- [ ] **Step 4: Run** — the four suites + `cargo build --workspace` + `cargo clippy -p uc_service -p uc_log -p uc_protocol -p uc_ctl --all-targets -- -D warnings`.

- [ ] **Step 5: Commit** — `cnc line 7 +504 artifact_hash: the builder hashes the payload as it streams and publishes it before snapshot_pos (plan B3 T1)`

---

### Task 2: `SNAP_REPORT` — kind 26, body codec, scope, receiver decode

**Files:**
- Modify: `uc_protocol/src/v2/datagram.rs` (after `DGRAM_KIND_PROBE_ACK` ~490; body codec beside `ProbeAckBody`; frozen test ~1360), `uc_crypto/src/transport.rs` (`scope_of` ~300-330: `| DGRAM_KIND_SNAP_REPORT` in the Pairwise arm with a one-line comment), `uc_net/src/receiver.rs` (`NetEvent::SnapReport { from: u32, row: u8, position: u64, hash: u64 }` with `kind_idx = 11`, `NET_EVENT_KINDS = 12`; decode in the snapshot-kind `match` beside `DGRAM_KIND_SNAP_REDIRECT` ~2264, routed to the consensus channel; the `is_consensus_kind` range is NOT widened — the kind is term-independent like the probes, so it must be handled before the stale-term drop; read `on_datagram` ~1955-1975 and place it where `SNAP_REDIRECT` is), `uc_node/src/node.rs` (`net_event_drops_by_kind` array is sized by the constant — verify nothing else hard-codes 11), `fuzz/fuzz_targets/uc_protocol_datagram.rs` (if it dispatches per kind, add the body decoder), `docs/reference/wire-protocol.md` (the kinds table row after 25; a `#### SNAP_REPORT body (wire 0.9.0, 2.13.0)` section modelled on the `PROBE`/`PROBE_ACK` one at ~200-230; the `0.9.0` line at ~13 gains "and one pairwise datagram kind, `SNAP_REPORT` (26)")
- Test: `uc_protocol` frozen numbers + codec; `uc_net` receiver decode test (model on the `SNAP_REDIRECT` decode test near `receiver.rs:7639`)

**Interfaces:**
```rust
pub const DGRAM_KIND_SNAP_REPORT: u8 = 26;
pub const SNAP_REPORT_BODY_LEN: usize = 24;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapReportBody { pub row: u8, pub node_id: u32, pub position: u64, pub hash: u64 }
pub fn write_snap_report_body(buf: &mut [u8], b: &SnapReportBody);      // buf.len() >= 24; reserved zero
pub fn read_snap_report_body(buf: &[u8]) -> Option<SnapReportBody>;      // exact 24, reserved zero, row < 8, position > 0
```

- [ ] **Step 1: Write the failing tests** — frozen: `DGRAM_KIND_SNAP_REPORT == 26`, `SNAP_REPORT_BODY_LEN == 24`, layout bytes (row @0, reserved @1..4 zero, node_id @4, position @8, hash @16), roundtrip, refusals (23 B, 25 B, reserved non-zero, row 8, position 0), `Transport::scope_of(26) == Scope::Pairwise`; receiver: a sealed-off (crypto-none) datagram of kind 26 with a valid body yields `NetEvent::SnapReport{..}` on the route channel; an invalid body yields nothing.
- [ ] **Step 2: fail.** **Step 3: implement** (the receiver arm mirrors `SNAP_REDIRECT`'s: decode, `route.try_send(ev)`, drop-count on a full channel). **Step 4:** `cargo test -p uc_protocol -p uc_crypto -p uc_net`, `cargo build --workspace`, fuzz build. **Step 5: Commit** — `wire: SNAP_REPORT (26), pairwise, 24-byte body; NetEvent::SnapReport (plan B3 T2)`.

---

### Task 3: Every node reports on its local set-complete edge

**Files:**
- Modify: `uc_node/src/node.rs` (`check_set_completeness` ~5943-6020: on the `source = "local"` edge; a new `fn send_snapshot_reports(&mut self, p: u64)`; `Consensus` fields `snapshot_reports_sent: Arc<AtomicU64>` / `..._unsent` for the two obs counters if the crate's pattern is atomics + metrics — follow `schedule_refused`), `uc_node/src/obs/metrics.rs` (`uc2_snapshot_reports_sent_total`, `uc2_snapshot_reports_unsent_total`, registered in `METRIC_NAMES`)
- Test: `node.rs` harness — how existing tests assert an outbound pairwise datagram: read the test for `on_read_probe` (~15698-15780) and the harness's outbox/`sent` accessor; mirror it.

**Interfaces:** `Consensus::send_snapshot_reports(p)`: for each `row in self.services.ids()` with `slot.snapshot_pos == p` and `slot.identity.artifact_hash() != 0`: body `{ row, node_id: self.id, position: p, hash }`; if this node is the leader → `self.on_snap_report(self.id, row, p, hash)` (Task 4's collector; until Task 4 lands, a stub that records into a `Vec` is fine — Task 4 replaces it); else → the leader hint's address (resolve the hint id through the member map the way `on_config_proposal`'s forward does — read ~7811-7830) → `self.send(addr, DGRAM_KIND_SNAP_REPORT, 0, term, &body)`; no hint → `unsent += 1`. Obs `snapshot_report_sent { node, row, position }` once per row per edge (Info) — it fires once per instant, not per pass.

- [ ] Steps: failing test (on the local edge with a leader hint set, one datagram per declared row is staged, body decodes to the row's hash; on the `fetch` edge nothing is sent; with no hint the unsent counter moves) → implement → `cargo test -p uc_node --lib snapshot_report` → commit `uc_node: report (row, P, hash) to the leader on the local set-complete edge (plan B3 T3)`.

---

### Task 4: The leader collects and appends

**Files:**
- Modify: `uc_node/src/node.rs` (`Consensus` fields: `pending_snapshot_reports: HashMap<u8, PendingSnapshotReport>`; `PendingSnapshotReport { position: u64, first_seen_ns: u64, hashes: BTreeMap<u32, u64> }`; `pub const SNAP_REPORT_TIMEOUT_NS: u64 = 5_000_000_000`; `fn on_snap_report(&mut self, from: u32, row: u8, position: u64, hash: u64)`; `fn maybe_append_snapshot_reports(&mut self)` called once per pass on the leader after the cluster-command gate check; `feed_net`'s `NetEvent::SnapReport` arm; clearing on `BecomeFollower`/halt beside `last_cluster_append`'s reset), `uc_node/src/obs/metrics.rs` (`uc2_snapshot_reports_appended_total`, `uc2_snapshot_reports_timed_out_total`)
- Test: `node.rs` harness (model: `settings_apply_is_single_in_flight_on_the_view_position` for the gate; the read-probe quorum tests for "distinct ackers / membership")

**Interfaces:** `on_snap_report`: drop if `from` is not a member (voter or learner) of `self.sm.config()`; drop if `position <= cluster_view.to_state().report_for(row).map(|r| r.position).unwrap_or(0)`; if a pending entry exists for `row` with a lower position, replace it (log `snapshot_report_superseded`); insert `hashes[from] = hash`, set `first_seen_ns` on creation from the pass clock. `maybe_append_snapshot_reports`: for each pending, `voters_reporting = hashes.keys().filter(is_voter).count()`, `quorum = voters/2 + 1`; if `voters_reporting >= quorum || now - first_seen >= SNAP_REPORT_TIMEOUT_NS` and `last_cluster_append <= view_position` → `append_cluster_frame(&ClusterCommand::SnapshotReport(SnapshotReport { row, position, hashes: map.into_iter().collect() }))`; on `Ok` remove the entry, obs `snapshot_report_appended { row, position, reporters, voters_reporting, quorum, by = "quorum" | "timeout" }`; on `WouldOverrun` keep it (next pass); at most ONE append per pass (single-in-flight).

- [ ] Steps: failing tests — quorum append (3 voters: two reports → appended with 2 hashes, sorted; the third arriving later is dropped as ≤ held once committed); timeout append (one report, advance the harness clock ≥ 5 s → appended with 1 hash, `by = timeout`); a non-member report dropped; a newer instant supersedes an older pending; a follower's `on_snap_report` is a no-op; step-down clears; the append respects the single-in-flight gate (a pending settings append above the view delays it) → implement → `cargo test -p uc_node --lib snapshot_report` → commit `uc_node: the leader collects SNAP_REPORTs per (row, P) and appends SnapshotReport on quorum or 5 s (plan B3 T4)`.

---

### Task 5: The pins-authoritative gate and the bounded `NodeBooting` wait

**Files:**
- Modify: `uc_node/src/node.rs` (remove `cnc.store_services_declared(..)` from `Node::start` ~2154; add `declared_published: bool` to `Consensus` and, in the pass right after `publish_status` ~4000: `if !self.declared_published && self.leader_known() && self.cluster_view.position.load(Acquire) >= self.cnc.counters().commit.load_acquire() { self.cnc.store_services_declared(self.services.declared()); self.declared_published = true; obs "services_declared_published" { node, commit, cluster_position } }`; `leader_known()` = `matches!(role, Leader) || cnc.status().leader_hint != 0` — read how the hint is stored at ~7811; the B2 C1 comment is rewritten to describe the new gate), `uc_service/src/config.rs` (`ServiceConfig::boot_wait: Duration`, default 10 s, builder setter), `uc_service/src/lib.rs` (`start`/`start_with_snapshots` loop on `Err(NodeBooting)` with 20 ms sleeps until `boot_wait` elapses, then return the error), `uc_client/src/engine.rs` + `client.rs` (`EngineConfig::boot_wait`, same loop in `Engine::attach`/`Client::connect`), `testing/uc_crashtest/src/bin/uc_crashtest-service.rs` (inherits via the builder — verify), docs later
- Test: `uc_service/tests/pinned_attach.rs` — extend the B2 boot-race test: commit a pin with NO instant after it (so it is ABOVE the recovered artifact), restart the node, attach immediately → the attach waits (`NodeBooting` internally) and then sees `Pinned` and installs; a node-level test that `services_declared` is 0 until the cluster agent's position reaches commit (harness: hold the view position below commit, assert 0; publish, assert nonzero); `uc_client` engine test for the wait.

- [ ] Steps: failing tests → implement → `cargo test -p uc_node --lib declared`, `cargo test -p uc_service --test pinned_attach`, `cargo test -p uc_client`, **the whole `uc_node` test suite and `uc_service` suite** (every test that starts a node then a service exercises the wait), `cargo test -p uc_crashtest --features hard-crash-tests` → commit `readiness: services_declared is published once the node knows its leader and the cluster view has reached commit; bounded NodeBooting wait in ServiceBuilder and Client::connect (plan B3 T5)`.

Ruling recorded here for the executor: if a single-node harness never sets a leader hint before it IS the leader, `leader_known` must include `Role::Leader`; if the node's commit counter is 0 before the first heartbeat, the condition `view >= 0` is trivially true — that is why (a) is required too.

---

### Task 6: End-to-end on a three-node cluster

**Files:**
- Create: `uc_node/tests/snapshot_reports.rs` (reuse `learner.rs`'s harness by copying `spawn_cluster_with_learner_services`, `Cluster`, `SumSm` and the instant helpers — or move them into `uc_node/tests/common/` if that module exists; do not `include!` a test file)
- Modify: `uc_node/src/node.rs` (`pub fn snapshot_report(&self, row: u8) -> Option<uc_protocol::v2::upgrade::SnapshotReport>` reading the cluster view — beside `cluster_snapshot_position()`)

- [ ] Tests: (1) `three_voters_agree`: 3 voters, one declared row with a snapshot-capable `SumSm` on each; writes; `command_snapshot` on the leader; wait until every node's `snapshot_report(0)` is `Some` with 3 hashes and `verdict().agreed`; `uc2_snapshot_hash_mismatch{row="0"}` renders 0 on each node's `/metrics` (or via the view). (2) `one_divergent_node_is_named`: same, but node 2's `SumSm` is a test variant whose `freeze()` appends its node id to the image (a deliberate nondeterminism) → the report's verdict names node 2 as the minority, the gauge reads 1 on every node, and the leader's obs stream has `snapshot_hash_diverged { row = 0, node = 2 }`. (3) `a_learner_reports_but_does_not_count_toward_quorum`: 2 voters + 1 learner; the append happens after the two voters report (learner's hash present in the vector or not, depending on arrival — assert the vector has ≥ 2 entries and that the append did not wait for the timeout).
- [ ] Commit `uc_node: three-node snapshot-report e2e — agreement, a named minority, learner reporting (plan B3 T6)`.

---

### Task 7: Docs, spec errata, proof stack

- Docs: `wire-protocol.md` (done in T2 — verify), `cnc-page.md` (T1 — verify; line 7 full), `limits.md` (the flag-day sentence: add kind 26 and `+504`; the line-7 "one free word" statement anywhere is gone), `monitor-a-cluster.md` (events `snapshot_report_sent`/`snapshot_report_appended`/`snapshot_report_superseded`/`services_declared_published`; counters `uc2_snapshot_reports_*`; the `uc2_snapshot_hash_mismatch` row now says reports are live), `uc2ctl.md` (`status` `artifact_hash=`; `upgrade show`'s verdict line is now populated on a live cluster), `upgrade-a-cluster.md` (the "confirm the pin on every node" step is now backed by the gate — a service cannot attach before its node has applied cluster state to commit; `NodeBooting` semantics and `boot_wait`; the how-to's manual `sha256sum` step is replaced by `uc2ctl upgrade show`), `docs/reference/state-machine-contract.md` + `configuration.md` (`boot_wait`), `docs/security/attack-surface.md` (kind 26 in the datagram inventory, sealed pairwise; a forged report needs the pairwise key), `uc2-cluster-fsm-explained.md` ("Pins and reports" → the live path), the spec's "Errata (plan B3, as built)" block under §6.5.2 with the seven errata, `VERIFICATION.md` (the e2e suite; the fuzz arm).
- Proof stack (paste every tail): fmt; workspace clippy + the four feature-gated runs; the two fixture builds; `cargo test --workspace`; `cargo test -p uc_node --test lin_v2`; `cargo test -p uc_crashtest --features hard-crash-tests`; `(cd fuzz && cargo +nightly fuzz build)`; and — because the gate changes boot ordering for every service — `cargo test -p uc_node --test lin_partition_v2`.
- Commit `docs: live snapshot reports (SNAP_REPORT, artifact_hash), the pins-authoritative readiness gate, spec errata as built (plan B3 T7)`.

---

## Self-review

**Spec coverage.** §6.5.2 item 1 (hash at stream time): T1, with erratum 1 (service-side). Item 2 (follower→leader datagram): T2 + T3. Item 3 (leader appends one record on quorum or timeout): T4. Item 4 (verdict at apply): B1, exercised live in T6. Item 5 (gauge/event/alert): B1; the metrics-only complement dropped (erratum 5). B2's deferred gate: T5 (erratum 7). §2.5's `SnapshotReport` row range excludes the cluster artifact (erratum 6, backlog).

**Placeholder scan.** T3 and T4 give the exact conditions and names but leave the harness accessor for "an outbound datagram" to the implementer with the model test named; T6 says copy-or-move the harness, never `include!`. No TBD/TODO.

**Type consistency.** `SnapReportBody { row: u8, node_id: u32, position: u64, hash: u64 }` (T2) ↔ `NetEvent::SnapReport { from, row, position, hash }` (T2) ↔ `on_snap_report(from, row, position, hash)` (T3/T4); `artifact_hash()` (T1) read in T3; `SnapshotReport { row, position, hashes: Vec<(u32, u64)> }` (B1) built from the `BTreeMap` in T4 and read by `Node::snapshot_report` in T6; `boot_wait: Duration` on both configs (T5).
