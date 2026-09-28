# UC2 Row Running Version (#33) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A row (one declared FSM) can never be applied by two versions at once: every row gets a committed running version, attach refuses a major.minor mismatch, and an already-attached service stops at exactly the position of a record that supersedes it.

**Architecture:** The running version is cluster-FSM state (like pins), set by an automatic `RowGenesis` record (new `CLUSTER` kind 6) and by the existing `UpgradePin` (kind 4). The cluster agent publishes it per row onto the cnc page under the existing pin seqlock (slot status line `+48`/`+56`), plus a page-1 word (`4056`) saying how far it has applied `CLUSTER` frames. Services read those words at attach and, in the apply loop, at every version record for their own row. The leader appends genesis automatically and admits no client frame until every declared row has a version.

**Tech Stack:** Rust 1.96 (pinned) / MSRV 1.89; crates `uc_protocol`, `uc_log`, `uc_node`, `uc_service`, `uc_ctl`; Prometheus rules under `packaging/prometheus/`.

**Spec:** `docs/superpowers/specs/2026-09-27-uc2-row-running-version-design.md` (revision 2). Read it before any task. Decisions D1–D7 and §3.1 (why pin only) bind every task.

## Global Constraints

- Wire `0.9.0` → `0.10.0` and cnc `3.3` → `3.4`: a flag day. No compatibility shim for mixed versions.
- `same_line(a, b)` ⇔ `a >> 16 == b >> 16` (equal major and minor, patch ignored). `0` is an ordinary version (D4). One helper, `uc_protocol::identity::same_line`, used everywhere.
- New refusal code **60 `version_already_set`**. Code 52 keeps its number and is renamed `row_undeclared`. Code 53 gains the `same_line(from, running)` clause.
- cnc offsets are pinned in BOTH `uc_protocol::v2::cnc` and `uc_log::cnc` with offset asserts; every new word reads `0` as absent.
- Hot-loop rule (CLAUDE.md "Finding a performance bottleneck"): the apply loop gains only a type test plus an `#[inline(never)]` call. Nothing else inline.
- A barrier wait never sleeps on a live peer (spin, then `yield_now`).
- Tests put instance dirs under `env!("CARGO_TARGET_TMPDIR")`; scratch goes under `$HOME/scratch/`, never `/tmp`.
- Every task ends green on `cargo clippy --workspace --all-targets -- -D warnings` and `cargo fmt --all -- --check`. Before any push, also run `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings`.
- No `Co-Authored-By` or any attribution trailer in commits.

## Review Focus

1. **Double record before a lagging service reaches the first one**: a v1 service still below record R1 (to v2) when R2 (back to v1) commits must stop at R1, not continue. That is `row_view.record_pos > rec_end` ⇒ Stop. Pinned by `verdict_stops_when_a_later_record_superseded_this_one` (Task 9).
2. **Service restart right after a stop**: the restarted old binary must be refused at attach, and a restarted matching binary must not stop again at the record it already passed (no stop loop). Pinned by `a_restart_after_the_stop_is_covered_by_attach` (Task 9) and e2e case (b) (Task 12).
3. **Follower service attaches before genesis commits**: running is absent at attach and the service proceeds, so the genesis record must stop it if its line differs. Pinned by e2e case (a) (Task 12), which attaches follower services before the leader's.
4. **Refused version record**: a pin the cluster FSM refuses must not stop any service. Pinned by `verdict_continues_past_a_refused_record` (Task 9), with the FSM-side refusal in Task 4.
5. **Harness page (nothing declared)**: `ServicesConfig::none_for_tests()` declares no rows, so the client gate must stay open and no genesis is ever proposed. Pinned by `ingress_gate_is_open_with_nothing_declared` (Task 7).

---

## File Structure

| file | change |
|---|---|
| `uc_protocol/src/identity.rs` | `same_line` |
| `uc_protocol/src/v2/frame.rs` | `ClusterKind::RowGenesis = 6` |
| `uc_protocol/src/v2/upgrade.rs` | `RowGenesis` record codec; `RowRunning` + running-list codec |
| `uc_protocol/src/version.rs` | `CURRENT = 0.10.0` |
| `uc_protocol/src/v2/cnc.rs` | cnc 3.4; `CNC_SVC_OFF_RUNNING_VERSION = 48`, `CNC_SVC_OFF_RUNNING_RECORD_POS = 56`, `CNC_OFF_CLUSTER_APPLIED = 4056` |
| `uc_protocol/src/v2/cluster_image.rs` | image v3 (`running` blob) |
| `uc_log/src/cnc.rs` | `ServiceStatusLine::{store_row_view, row_view}`, `RowRead`; `CncPage::{cluster_applied, store_cluster_applied}` |
| `uc_node/src/cluster_fsm.rs` | `ClusterState.running`, `ClusterCommand::RowGenesis`, refusal 60, pin `same_line` clause, image v3 freeze/install, `ClusterView.versioned` |
| `uc_node/src/cluster_agent.rs` | publish row views for every row; publish `cluster_applied` |
| `uc_node/src/node.rs` | `REASON_VERSION_ALREADY_SET`; `maybe_append_row_genesis`; client gate; pin door no-pin half |
| `uc_node/src/audit.rs` | `SOURCE_GENESIS`, audit-only op `row_genesis` |
| `uc_service/src/config.rs` | `ServiceError::VersionMismatch` |
| `uc_service/src/attach.rs` | version-before-status order; row-view read; `attach_record_pos` |
| `uc_service/src/version_gate.rs` (new) | pure `verdict` + `on_cluster_frame` |
| `uc_service/src/apply.rs` | the `FRAME_TYPE_CLUSTER` arm; `ApplyState.attach_record_pos` |
| `uc_service/src/snapshots.rs` | envelope version check → `same_line` |
| `uc_ctl/src/main.rs`, `uc_ctl/src/upgrade.rs` | status fields, reason 60/52, `upgrade show` running, pin `--from` default |
| `uc_node/src/obs/metrics.rs` | `uc2_row_running_version` |
| `packaging/prometheus/uc2-alerts.yml`, `uc_node/examples/m10_alerts.rs`, `scripts/m10_alert_fire.sh` | `Uc2RowVersionMismatch`; `Uc2ServiceVersionDrift` by line |
| `uc_node/tests/row_version.rs` (new) | the #33 repro and the e2e cases |
| docs (Task 13) | how-to, references, explainer |

---

### Task 0: The #33 repro, written first and watched failing

**Files:**
- Create: `uc_node/tests/row_version.rs`

**Interfaces:**
- Produces: the helpers `three_node_cluster`, `KvV1`, `KvV2` reused by Task 12.

Two FSM builds of one row named `kv`: `KvV1` (VERSION 1.0.0) answers `Put` and rejects anything else with a `BAD_REQUEST` byte, deterministically and without changing state. `KvV2` (2.0.0) also accepts `Append`. The leader runs v2 and the followers v1.

- [ ] **Step 1: Read the harness idiom** in `uc_node/tests/learner.rs:925-995` (in-process `SumSm`, `ServiceBuilder::new(cfg, sm).start_with_snapshots()`) and `uc_ctl/tests/status_services.rs:31-54` (`NodeConfig` for one node). The new test copies those shapes for three nodes on loopback, each with `ServicesConfig::from_names(&["kv"], None)`.

- [ ] **Step 2: Write the test**

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! #33: two builds of one row must never both apply the log. See
//! docs/superpowers/specs/2026-09-27-uc2-row-running-version-design.md.

use std::path::Path;
use uc_protocol::identity::pack_version;

/// Commands: tag byte 1 = Put(u64), tag byte 2 = Append(u64) (v2 only).
/// Response: 0 = ok, 0xBA = BAD_REQUEST. Query: the stored u64.
#[derive(Default)]
struct Kv {
    value: u64,
    last: Option<u64>,
}

macro_rules! kv_build {
    ($ty:ident, $ver:expr, $append:expr) => {
        #[derive(Default)]
        struct $ty(Kv);
        impl uc_service::RawStateMachine for $ty {
            const NAME: &'static str = "kv";
            const VERSION: u32 = $ver;
            fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
                out.clear();
                self.0.last = Some(ctx.position);
                let v = u64::from_le_bytes(cmd[1..9].try_into().unwrap());
                match cmd[0] {
                    1 => { self.0.value = v; out.push(0) }
                    2 if $append => { self.0.value += v; out.push(0) }
                    _ => out.push(0xBA),
                }
            }
            fn query(&self, _q: &[u8], out: &mut Vec<u8>) {
                out.clear();
                out.extend_from_slice(&self.0.value.to_le_bytes());
            }
            fn last_applied(&self) -> Option<u64> {
                self.0.last
            }
        }
        impl uc_service::SnapshotStateMachine for $ty {
            type SnapshotHandle = Vec<u8>;
            fn freeze(&self) -> Result<(Vec<u8>, u64), uc_service::SnapshotError> {
                let mut b = self.0.value.to_le_bytes().to_vec();
                b.extend_from_slice(&self.0.last.unwrap_or(0).to_le_bytes());
                Ok((b, self.0.last.unwrap_or(0)))
            }
            fn stream_snapshot(h: Vec<u8>, dst: &mut dyn std::io::Write) -> Result<(), uc_service::SnapshotError> {
                dst.write_all(&h).map_err(Into::into)
            }
            fn install_snapshot(&mut self, position: u64, src: &mut dyn std::io::Read) -> Result<u64, uc_service::SnapshotError> {
                let mut b = [0u8; 16];
                src.read_exact(&mut b)?;
                self.0.value = u64::from_le_bytes(b[..8].try_into().unwrap());
                self.0.last = Some(u64::from_le_bytes(b[8..].try_into().unwrap()));
                Ok(position)
            }
        }
    };
}
kv_build!(KvV1, pack_version(1, 0, 0), false);
kv_build!(KvV2, pack_version(2, 0, 0), true);

fn put(v: u64) -> Vec<u8> { let mut c = vec![1u8]; c.extend_from_slice(&v.to_le_bytes()); c }
fn append(v: u64) -> Vec<u8> { let mut c = vec![2u8]; c.extend_from_slice(&v.to_le_bytes()); c }

/// The #33 scenario. Before the fix: the leader's v2 acknowledges
/// `Append(5)`; the v1 followers apply it as BAD_REQUEST; after the leader
/// stops, a v1 follower leads and the value reads 10, not 15 — an
/// acknowledged write lost. After the fix: every v1 service is refused at
/// attach or stopped at the genesis record, and once v2 runs everywhere the
/// value reads 15.
#[test]
fn a_mixed_version_row_never_loses_an_acknowledged_write() {
    let c = three_node_cluster("rowver33");
    let leader = c.wait_leader();
    // Followers FIRST, so they are attached before the genesis record
    // commits (Review Focus 3).
    let mut followers_v1: Vec<_> = c.others(leader).map(|i| c.try_start::<KvV1>(i)).collect();
    let _leader_v2 = c.start::<KvV2>(leader);
    let client = c.client(leader);
    assert_eq!(client.submit(&put(10)).expect("put acked"), vec![0]);
    assert_eq!(client.submit(&append(5)).expect("append acked"), vec![0]);

    // Every v1 service is either refused or has stopped by now.
    for f in followers_v1.iter_mut() {
        assert!(f.is_refused_or_stopped_within(std::time::Duration::from_secs(10)),
            "a v1 service is still applying a row that runs 2.0: {f:?}");
    }
    // Bring v2 up on the followers, stop the old leader, read on the new one.
    let v2s: Vec<_> = c.others(leader).map(|i| c.start::<KvV2>(i)).collect();
    c.stop_node(leader);
    let new_leader = c.wait_leader();
    let got = c.client(new_leader).query_u64();
    assert_eq!(got, 15, "the acknowledged Append(5) was lost");
    drop(v2s);
}
```

The helper `three_node_cluster(app) -> Cluster` lives in the same file:
- It starts three `Node`s with `make_config` copied from `uc_ctl/tests/status_services.rs:31-54`, `members` listing all three loopback sockets and `services: ServicesConfig::from_names(&["kv"], None).unwrap()`.
- `Cluster` methods:
  - `wait_leader() -> usize`: polls `node.can_serve()`.
  - `others(i)`: the other two indices.
  - `start::<S>(i) -> uc_service::Service<S>`: `ServiceBuilder::new(ServiceConfig::new(dir_i, app), S::default()).start_with_snapshots().expect(..)`.
  - `try_start::<S>(i) -> Attempt<S>`: stores the `Result` so a refusal is observable.
  - `client(i) -> Client`: a `uc_client` submitter against node `i`'s instance dir, copied from the client setup in `uc_node/tests/services.rs`.
  - `stop_node(i)`.
- `Attempt::is_refused_or_stopped_within(d)` is true when the start returned `Err(_)`, or the service's `is_alive()` turns false within `d`.

- [ ] **Step 3: Run it on the unmodified tree and record the failure**

Run: `cargo test -p uc_node --test row_version a_mixed_version_row -- --nocapture 2>&1 | tail -20`

Expected: FAIL. Either the "a v1 service is still applying" assertion or `left: 10, right: 15`. Copy the failure lines into the commit message body.

- [ ] **Step 4: Mark it `#[ignore = "#33: un-ignored in Task 12"]`** so the tree stays green, and commit.

```bash
git add uc_node/tests/row_version.rs
git commit -m "test(#33): mixed-version row repro — fails on main

<paste the failure lines from Step 3>"
```

---

### Task 1: Protocol constants and the `RowGenesis` record

**Files:**
- Modify: `uc_protocol/src/identity.rs` (after `unpack_version`, ~line 158)
- Modify: `uc_protocol/src/v2/frame.rs:72-93`
- Modify: `uc_protocol/src/v2/upgrade.rs` (after `decode_upgrade_pin`, ~line 61)
- Modify: `uc_protocol/src/version.rs:84`
- Modify: `uc_protocol/src/v2/cnc.rs:53-66` (version), `:399` (after `CNC_SVC_OFF_PINNED_FROM`), `:298-314` (page-1 line 4032)

**Interfaces:**
- Produces:
  - `pub const fn same_line(a: u32, b: u32) -> bool`
  - `ClusterKind::RowGenesis` (= 6)
  - `pub struct RowGenesis { pub row: u8, pub version: u32 }`, `pub const ROW_GENESIS_LEN: usize = 8`
  - `encode_row_genesis(&RowGenesis, &mut Vec<u8>)`, `decode_row_genesis(&[u8]) -> Option<RowGenesis>`
  - `CNC_SVC_OFF_RUNNING_VERSION = 48`, `CNC_SVC_OFF_RUNNING_RECORD_POS = 56`, `CNC_OFF_CLUSTER_APPLIED = 4056`, `RUNNING_PRESENT: u64 = 1 << 32`

- [ ] **Step 1: Write the failing tests** (append to the existing `#[cfg(test)] mod tests` in each file)

`identity.rs`:
```rust
#[test]
fn same_line_ignores_patch_and_treats_zero_as_a_version() {
    assert!(same_line(pack_version(1, 4, 2), pack_version(1, 4, 9)));
    assert!(!same_line(pack_version(1, 4, 2), pack_version(1, 5, 2)));
    assert!(!same_line(pack_version(1, 4, 2), pack_version(2, 4, 2)));
    assert!(same_line(0, 0));
    assert!(!same_line(0, pack_version(1, 0, 0)));
    // raw small ints (the uc_lincheck fixtures' VERSION = 1/2/3) are 0.0.x:
    assert!(same_line(1, 3));
}
```

`upgrade.rs`:
```rust
#[test]
fn row_genesis_golden_bytes_and_strict_decode() {
    let g = RowGenesis { row: 3, version: pack_version(1, 2, 3) };
    let mut b = Vec::new();
    encode_row_genesis(&g, &mut b);
    assert_eq!(b, [3, 0, 0, 0, 0x03, 0x00, 0x02, 0x01]);
    assert_eq!(decode_row_genesis(&b), Some(g));
    let mut bad = b.clone(); bad[1] = 1;               // reserved non-zero
    assert_eq!(decode_row_genesis(&bad), None);
    let mut bad = b.clone(); bad[0] = CNC_MAX_SERVICES as u8; // row out of range
    assert_eq!(decode_row_genesis(&bad), None);
    assert_eq!(decode_row_genesis(&b[..7]), None);      // short
    let mut long = b.clone(); long.push(0);
    assert_eq!(decode_row_genesis(&long), None);        // exact length
}
```
(`upgrade.rs` already imports `CNC_MAX_SERVICES`; add `use crate::identity::pack_version;` inside the test module.)

`frame.rs` (in its test module, beside the existing `from_u8` test near line 362):
```rust
#[test]
fn cluster_kind_six_is_row_genesis() {
    assert_eq!(ClusterKind::from_u8(6), Some(ClusterKind::RowGenesis));
    assert_eq!(ClusterKind::from_u8(7), None);
}
```

`cnc.rs` (test module):
```rust
#[test]
fn cnc_3_4_row_version_words_sit_in_the_free_status_line_words() {
    assert_eq!(CNC_SVC_OFF_RUNNING_VERSION, 48);
    assert_eq!(CNC_SVC_OFF_RUNNING_RECORD_POS, 56);
    assert_eq!(CNC_OFF_CLUSTER_APPLIED, 4056);
    assert_eq!(CNC_V2_VERSION, (3 << 24) | (4 << 16));
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p uc_protocol same_line row_genesis cluster_kind_six cnc_3_4 2>&1 | tail -5`
Expected: compile errors naming `same_line`, `RowGenesis`, `CNC_SVC_OFF_RUNNING_VERSION`.

- [ ] **Step 3: Implement**

`identity.rs`, after `unpack_version`:
```rust
/// #33 (row running version, spec D3/D4): two packed versions are the SAME
/// LINE when major and minor agree; patch is free. `0` is an ordinary value
/// here — it equals only `0`. FROZEN: attach, the apply-loop version gate,
/// the cluster FSM's pin rule and the pinned install all decide on this.
pub const fn same_line(a: u32, b: u32) -> bool {
    a >> 16 == b >> 16
}
```

`frame.rs`: add `RowGenesis = 6,` to the enum and `6 => Some(ClusterKind::RowGenesis),` to `from_u8`.

`upgrade.rs`, after `decode_upgrade_pin`:
```rust
/// `row u8 @0 ‖ reserved [u8; 3] @1 ‖ version u32 @4` — exactly 8 bytes,
/// `CLUSTER kind = 6` (#33 spec §5.1). The leader's own attached version for
/// a row that has none yet: a recorded FACT, never an operator's change.
pub const ROW_GENESIS_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowGenesis {
    pub row: u8,
    pub version: u32,
}

pub fn encode_row_genesis(g: &RowGenesis, out: &mut Vec<u8>) {
    out.push(g.row);
    out.extend_from_slice(&[0, 0, 0]);
    out.extend_from_slice(&g.version.to_le_bytes());
}

/// Exact-length, reserved-zero, `row < CNC_MAX_SERVICES`.
pub fn decode_row_genesis(buf: &[u8]) -> Option<RowGenesis> {
    if buf.len() != ROW_GENESIS_LEN || buf[1..4] != [0, 0, 0] {
        return None;
    }
    let row = buf[0];
    if row as usize >= CNC_MAX_SERVICES {
        return None;
    }
    Some(RowGenesis {
        row,
        version: u32::from_le_bytes(buf[4..8].try_into().ok()?),
    })
}
```

`version.rs:84`: change `CURRENT` to `0.10.0`, keeping the existing constructor form on that line. Update the doc comment above it with one sentence: "0.10.0 (#33): `CLUSTER` kind 6 `RowGenesis`; a 0.9.0 peer refuses it as undecodable and diverges silently — flag day."

`cnc.rs`:
- Bump `CNC_V2_VERSION` to `(3 << 24) | (4 << 16)` and add a "3.4 (#33)" line to its doc listing the three words.
- After `CNC_SVC_OFF_PINNED_FROM`:
```rust
/// cnc 3.4 (#33 spec §5.2): the row's committed RUNNING version — bit 32
/// set ⇔ present, low 32 bits the packed version. `0` = absent (no record
/// yet). Written by the `uc2-cluster` agent under the `pin_seq` seqlock,
/// with the four pin words and `running_record_pos`.
pub const CNC_SVC_OFF_RUNNING_VERSION: usize = 48;
/// cnc 3.4: frame-END position of the row's last ACCEPTED version record
/// (genesis or pin); `0` = none. Same writer and seqlock as the word above.
pub const CNC_SVC_OFF_RUNNING_RECORD_POS: usize = 56;
/// Presence bit of [`CNC_SVC_OFF_RUNNING_VERSION`].
pub const RUNNING_PRESENT: u64 = 1 << 32;
```
- After `CNC_OFF_LOG_TIME_NS`, change the old `CNC_OFF_LOG_TIME_NS + 8 <= 4096` assert's neighbour to:
```rust
/// cnc 3.4 (#33 spec §5.2): the frame-END position the `uc2-cluster` agent
/// had applied `CLUSTER` frames up to when it last applied one (written
/// only after a batch that applied or installed something, AFTER the row
/// words — so a reader that sees `>= p` sees every row word as of `p`).
/// Fourth word of the 4032 line. Writer: cluster agent; init = 0.
pub const CNC_OFF_CLUSTER_APPLIED: usize = 4056;
const _: () = assert!(CNC_OFF_CLUSTER_APPLIED == CNC_OFF_LOG_TIME_NS + 8);
const _: () = assert!(CNC_OFF_CLUSTER_APPLIED + 8 <= 4096);
```

- [ ] **Step 4: Run to verify they pass**, then the whole crate: `cargo test -p uc_protocol 2>&1 | grep "test result"`. Expected: all ok. Any test pinning `CURRENT == 0.9.0` or cnc `3.3` must be updated to the new value. Search: `grep -rn "0, 9, 0\|3 << 16" uc_protocol uc_log`.

- [ ] **Step 5: Commit** — `git commit -am "protocol(#33): same_line, CLUSTER kind 6 RowGenesis, wire 0.10.0, cnc 3.4 words"`

---

### Task 2: cnc row view under the pin seqlock

**Files:**
- Modify: `uc_log/src/cnc.rs:180-337` (`ServiceStatusLine`, `PinRead`), page accessors near `log_time_ns` (~line 847)

**Interfaces:**
- Consumes: Task 1 offsets and `RUNNING_PRESENT`.
- Produces:
  - `ServiceStatusLine::store_row_view(&self, pin: Option<(u64, u32, u32)>, running: Option<u32>, record_pos: u64)`. Single writer, one seqlock round.
  - `ServiceStatusLine::row_view(&self) -> RowRead`
  - `pub enum RowRead { View { pin: Option<(u64, u32, u32)>, running: Option<u32>, record_pos: u64 }, Contended }`
  - `CncPage::cluster_applied(&self) -> u64`, `CncPage::store_cluster_applied(&self, v: u64)`
  - `pin()` / `store_pin()` are kept. `store_pin` now delegates to `store_row_view` so existing callers compile. It keeps the running words as they were: it re-reads them, since it is the single writer.

- [ ] **Step 1: Write the failing tests** (in `uc_log/src/cnc.rs`'s test module, beside the existing pin seqlock tests; find them with `grep -n "store_pin_begin_for_test" uc_log/src/cnc.rs`)

```rust
#[test]
fn row_view_round_trips_and_absent_reads_as_none() {
    let (_d, page) = test_page(); // the module's existing page fixture
    let s = &page.service_slot(2).status;
    assert_eq!(s.row_view(), RowRead::View { pin: None, running: None, record_pos: 0 });
    s.store_row_view(Some((4096, 7, 8)), Some(0), 5120);
    assert_eq!(s.row_view(), RowRead::View { pin: Some((4096, 7, 8)), running: Some(0), record_pos: 5120 });
    assert_eq!(s.pin(), PinRead::Pinned { origin: 4096, from: 7, to: 8 });
    // Some(0) is a recorded unversioned FSM, distinct from None (D4).
    s.store_row_view(None, None, 0);
    assert_eq!(s.row_view(), RowRead::View { pin: None, running: None, record_pos: 0 });
}

#[test]
fn row_view_is_contended_while_a_store_is_in_flight() {
    let (_d, page) = test_page();
    let s = &page.service_slot(0).status;
    s.store_pin_begin_for_test(1, 2); // leaves pin_seq ODD
    assert_eq!(s.row_view(), RowRead::Contended);
    s.store_pin_finish_for_test(64);
    assert!(matches!(s.row_view(), RowRead::View { .. }));
}

#[test]
fn cluster_applied_word_round_trips_at_4056() {
    let (_d, page) = test_page();
    assert_eq!(page.cluster_applied(), 0);
    page.store_cluster_applied(9000);
    assert_eq!(page.cluster_applied(), 9000);
}
```
If the module's fixture is not named `test_page`, use the one the existing pin tests use. Read the first pin test to find it.

- [ ] **Step 2: Run to verify they fail**: `cargo test -p uc_log row_view cluster_applied 2>&1 | tail -5`. Expected: compile errors.

- [ ] **Step 3: Implement**

In `ServiceStatusLine`, replace `_pad: [u64; 2]` with:
```rust
    running_version: AtomicU64,
    running_record_pos: AtomicU64,
```
Add these methods:
```rust
    /// #33 spec §5.2: publish the whole row view — the pin triple (origin 0 =
    /// none), the running version (`None` = no record yet) and the position
    /// of the last accepted version record — in ONE seqlock round. Single
    /// writer (`uc2-cluster`), as for `store_pin`.
    pub fn store_row_view(&self, pin: Option<(u64, u32, u32)>, running: Option<u32>, record_pos: u64) {
        let (origin, from, to) = pin.unwrap_or((0, 0, 0));
        let rv = running.map_or(0, |v| cnc::RUNNING_PRESENT | v as u64);
        self.pin_seq.fetch_add(1, Ordering::Release);
        self.pinned_version.store(to as u64, Ordering::Release);
        self.pinned_from.store(from as u64, Ordering::Release);
        self.upgrade_origin.store(origin, Ordering::Release);
        self.running_version.store(rv, Ordering::Release);
        self.running_record_pos.store(record_pos, Ordering::Release);
        self.pin_seq.fetch_add(1, Ordering::Release);
    }

    /// The row view as one consistent read, or `Contended` after 64 spins —
    /// the same discipline and the same "never fabricate" rule as [`Self::pin`].
    pub fn row_view(&self) -> RowRead {
        for _ in 0..64 {
            let s1 = self.pin_seq.load(Ordering::Acquire);
            if s1 & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let origin = self.upgrade_origin.load(Ordering::Acquire);
            let to = self.pinned_version.load(Ordering::Acquire) as u32;
            let from = self.pinned_from.load(Ordering::Acquire) as u32;
            let rv = self.running_version.load(Ordering::Acquire);
            let record_pos = self.running_record_pos.load(Ordering::Acquire);
            if s1 == self.pin_seq.load(Ordering::Acquire) {
                return RowRead::View {
                    pin: (origin != 0).then_some((origin, from, to)),
                    running: (rv & cnc::RUNNING_PRESENT != 0).then_some(rv as u32),
                    record_pos,
                };
            }
            std::hint::spin_loop();
        }
        RowRead::Contended
    }
```
Change `store_pin` into:
```rust
    pub fn store_pin(&self, origin: u64, from: u32, to: u32) {
        // Kept for existing callers; the single writer re-publishes its own
        // running words unchanged.
        let rv = self.running_version.load(Ordering::Acquire);
        let running = (rv & cnc::RUNNING_PRESENT != 0).then_some(rv as u32);
        let rp = self.running_record_pos.load(Ordering::Acquire);
        let pin = (origin != 0).then_some((origin, from, to));
        self.store_row_view(pin, running, rp);
    }
```
After `PinRead`:
```rust
/// #33: a consistent read of the whole row view. `Contended` has
/// [`PinRead::Contended`]'s meaning: a reader that must DECIDE treats it as
/// "could not read", never as "absent".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowRead {
    View { pin: Option<(u64, u32, u32)>, running: Option<u32>, record_pos: u64 },
    Contended,
}
```
Add offset asserts next to the existing ones:
```rust
const _: () = assert!(
    std::mem::offset_of!(ServiceStatusLine, running_version) == cnc::CNC_SVC_OFF_RUNNING_VERSION
);
const _: () = assert!(
    std::mem::offset_of!(ServiceStatusLine, running_record_pos)
        == cnc::CNC_SVC_OFF_RUNNING_RECORD_POS
);
```
Next to `log_time_ns` on `CncPage` (same `unsafe` pattern as lines 847-857, with its `// SAFETY:` comment copied and adapted):
```rust
    /// cnc 3.4 (#33): see `uc_protocol::v2::cnc::CNC_OFF_CLUSTER_APPLIED`.
    pub fn cluster_applied(&self) -> u64 {
        // SAFETY: as `log_time_ns` — an aligned, page-backed u64 at a pinned offset.
        unsafe { (*(self.region.ptr_at(CNC_OFF_CLUSTER_APPLIED) as *const AtomicU64)).load(Ordering::Acquire) }
    }
    pub fn store_cluster_applied(&self, v: u64) {
        // SAFETY: as above; single writer (the `uc2-cluster` agent).
        unsafe { (*(self.region.ptr_at(CNC_OFF_CLUSTER_APPLIED) as *const AtomicU64)).store(v, Ordering::Release) }
    }
```
Import `CNC_OFF_CLUSTER_APPLIED` in the `use` list at line 28. Add `assert_eq!(cnc::CNC_OFF_CLUSTER_APPLIED, 4056);` beside the `4048` assert at ~line 1401.

- [ ] **Step 4: Run**: `cargo test -p uc_log 2>&1 | grep "test result"`. Expected: all ok.
- [ ] **Step 5: Commit** — `git commit -am "cnc(#33): row view (running version + record pos) under the pin seqlock; cluster_applied word"`

---

### Task 3: Cluster image v3

**Files:**
- Modify: `uc_protocol/src/v2/cluster_image.rs:42-49` (version), `:76-90` (parts), `:117-140` (encode), `:154-247` (decode)
- Modify: `uc_protocol/src/v2/upgrade.rs` (running-list codec)

**Interfaces:**
- Produces:
  - `CLUSTER_IMAGE_VERSION = 3`
  - `ClusterImageParts.running: &[u8]`. Empty for v1/v2 images.
  - `pub struct RowRunning { pub row: u8, pub version: u32, pub record_pos: u64 }`, `ROW_RUNNING_LEN = 16`
  - `encode_running_list(&[RowRunning], &mut Vec<u8>)`, `decode_running_list(&[u8]) -> Option<Vec<RowRunning>>`. Entries are strictly increasing by row, and `row < CNC_MAX_SERVICES`.

- [ ] **Step 1: Failing tests**

In `upgrade.rs` tests:
```rust
#[test]
fn running_list_round_trips_and_rejects_unsorted_rows() {
    let l = vec![
        RowRunning { row: 0, version: pack_version(1, 0, 0), record_pos: 640 },
        RowRunning { row: 3, version: 0, record_pos: 1280 },
    ];
    let mut b = Vec::new();
    encode_running_list(&l, &mut b);
    assert_eq!(b.len(), 2 * ROW_RUNNING_LEN);
    assert_eq!(decode_running_list(&b), Some(l.clone()));
    let mut swapped = b[ROW_RUNNING_LEN..].to_vec();
    swapped.extend_from_slice(&b[..ROW_RUNNING_LEN]);
    assert_eq!(decode_running_list(&swapped), None);
    assert_eq!(decode_running_list(&b[..15]), None);
    assert_eq!(decode_running_list(&[]), Some(vec![]));
}
```
In `cluster_image.rs` tests:
```rust
#[test]
fn v3_image_round_trips_the_running_blob_and_v2_reads_empty() {
    let running = [7u8; 16];
    let p = ClusterImageParts {
        applied: 4096, table_position: 0, settings_position: 0,
        membership: b"m", table: b"t", settings: &settings_v2_bytes(),
        pins: &[], reports: &[], running: &running,
    };
    let mut img = Vec::new();
    encode_cluster_image(&p, &mut img).unwrap();
    assert_eq!(u32::from_le_bytes(img[8..12].try_into().unwrap()), 3);
    assert_eq!(decode_cluster_image(&img).unwrap().running, &running[..]);
    // A v2 image (the existing v2 fixture/encoder path) decodes with empty running.
    let v2 = v2_fixture_image(); // see Step 3 note
    assert_eq!(decode_cluster_image(&v2).unwrap().running, &[] as &[u8]);
}
```
`settings_v2_bytes()` and a v2 fixture may already exist in the test module. Look for the existing v1 `PLAN1_FIXTURE` at `cluster_image.rs:288`. If there is no v2 fixture, add `const PLAN_B1_V2_FIXTURE: &[u8]`, produced by running today's encoder once (before Step 3) and pasting the bytes as a literal. Do that first, while the encoder still writes v2.

- [ ] **Step 2: Run to fail**: `cargo test -p uc_protocol running_list v3_image 2>&1 | tail -5`.

- [ ] **Step 3: Implement**

`upgrade.rs`:
```rust
/// #33 spec §5.3: one entry per row that HAS a running version, strictly
/// increasing by row: `row u8 ‖ reserved [u8; 3] ‖ version u32 ‖ record_pos u64`.
pub const ROW_RUNNING_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowRunning { pub row: u8, pub version: u32, pub record_pos: u64 }

pub fn encode_running_list(l: &[RowRunning], out: &mut Vec<u8>) {
    for r in l {
        out.push(r.row);
        out.extend_from_slice(&[0, 0, 0]);
        out.extend_from_slice(&r.version.to_le_bytes());
        out.extend_from_slice(&r.record_pos.to_le_bytes());
    }
}

pub fn decode_running_list(buf: &[u8]) -> Option<Vec<RowRunning>> {
    if !buf.len().is_multiple_of(ROW_RUNNING_LEN) {
        return None;
    }
    let mut out: Vec<RowRunning> = Vec::new();
    for c in buf.chunks_exact(ROW_RUNNING_LEN) {
        if c[1..4] != [0, 0, 0] || c[0] as usize >= CNC_MAX_SERVICES {
            return None;
        }
        if out.last().is_some_and(|p| p.row >= c[0]) {
            return None;
        }
        out.push(RowRunning {
            row: c[0],
            version: u32::from_le_bytes(c[4..8].try_into().ok()?),
            record_pos: u64::from_le_bytes(c[8..16].try_into().ok()?),
        });
    }
    Some(out)
}
```
`cluster_image.rs`:
- `CLUSTER_IMAGE_VERSION = 3`, with a doc line: "3 (#33): a trailing length-prefixed `running` blob after `reports`".
- Add `pub running: &'a [u8],` to `ClusterImageParts`, with a doc: "empty for a v1/v2 image or a cluster with no running versions recorded".
- In `encode_cluster_image`, after the reports blob:
```rust
    let running_len = payload_len_prefix(p.running.len())?;
    out.extend_from_slice(&running_len.to_le_bytes());
    out.extend_from_slice(p.running);
```
- In `decode_cluster_image`:
  - Change the version gate to `if !(1..=CLUSTER_IMAGE_VERSION).contains(&version) { return None; }`.
  - The v1 branch returns an empty `running` as well.
  - In the non-v1 branch, after reading `reports`, and only when `version == 3`:
```rust
        let running = if version >= 3 {
            let nl = u32_at(o)? as usize;
            o += 4;
            let r = o.checked_add(nl).and_then(|end| body.get(o..end))?;
            o += nl;
            r
        } else {
            &body[body.len()..]
        };
```
  - Keep the `o != body.len()` exact-framing check after it, and return `running` in the parts. The tuple destructuring becomes four elements.

- [ ] **Step 4: Run**: `cargo test -p uc_protocol 2>&1 | grep "test result"`. Expected: ok. Fix callers the compiler names: `ClusterImageParts { .. }` literals in `uc_node/src/cluster_fsm.rs:559` and in tests. For now they pass `running: &[]`; Task 4 fills them.
- [ ] **Step 5: Commit** — `git commit -am "image(#33): cluster image v3 carries per-row running versions; v1/v2 read as empty"`

---

### Task 4: The cluster FSM holds the running version

**Files:**
- Modify: `uc_node/src/cluster_fsm.rs` (state :81-155, command :203-239, validate :326-424, encode :427-456, apply :463-500, query :502-515, freeze/install :543-641, view :650-830)

**Interfaces:**
- Consumes: `RowGenesis`, `RowRunning`, `same_line`, and the codecs from Tasks 1 and 3.
- Produces:
  - `ClusterState.running: [Option<RowRunning>; CNC_MAX_SERVICES]` (the `row` field inside equals the index)
  - `ClusterState::running_for(row) -> Option<RowRunning>`
  - `ClusterCommand::RowGenesis(RowGenesis)`
  - `ClusterRefusal::VersionAlreadySet` → code 60
  - `ClusterView.versioned: AtomicU8`: bit `r` set ⇔ row `r` has a running version
  - `ClusterView::running_for(row) -> Option<RowRunning>`, read under the inner lock
  - query op `6`: the running list

- [ ] **Step 1: Failing tests** (in the file's test module; reuse its existing helpers for building an FSM and applying a framed command — find the helper used by the existing pin tests, e.g. `grep -n "fn apply_cmd\|fn frame(" uc_node/src/cluster_fsm.rs`)

```rust
#[test]
fn genesis_sets_a_rows_running_version_once_and_refuses_60_after() {
    let mut f = fsm_with_rows(); // existing helper or ClusterFsm::new(ClusterState::genesis_empty(), vec![])
    let g = ClusterCommand::RowGenesis(RowGenesis { row: 1, version: pack_version(1, 0, 0) });
    assert_eq!(apply_at(&mut f, &g, 640), vec![0]);
    assert_eq!(f.state().running_for(1), Some(RowRunning { row: 1, version: pack_version(1, 0, 0), record_pos: 640 }));
    let g2 = ClusterCommand::RowGenesis(RowGenesis { row: 1, version: pack_version(2, 0, 0) });
    assert_eq!(apply_at(&mut f, &g2, 1280), vec![60]);
    assert_eq!(f.state().running_for(1).unwrap().version, pack_version(1, 0, 0), "a refused record changes nothing");
}

#[test]
fn a_pin_sets_running_and_must_start_from_the_running_line() {
    let mut f = fsm_with_rows();
    apply_at(&mut f, &ClusterCommand::RowGenesis(RowGenesis { row: 0, version: pack_version(1, 4, 2) }), 640);
    let off_line = UpgradePin { row: 0, from: pack_version(1, 3, 0), to: pack_version(2, 0, 0), origin: 512 };
    assert_eq!(apply_at(&mut f, &ClusterCommand::UpgradePin(off_line), 1280), vec![53]);
    let patch_from = UpgradePin { row: 0, from: pack_version(1, 4, 7), to: pack_version(2, 0, 0), origin: 512 };
    assert_eq!(apply_at(&mut f, &ClusterCommand::UpgradePin(patch_from), 1920), vec![0]);
    assert_eq!(f.state().running_for(0).unwrap(), RowRunning { row: 0, version: pack_version(2, 0, 0), record_pos: 1920 });
    // Rollback is just another pin, from the running line (spec §4.1).
    let back = UpgradePin { row: 0, from: pack_version(2, 0, 0), to: pack_version(1, 4, 2), origin: 1024 };
    assert_eq!(apply_at(&mut f, &ClusterCommand::UpgradePin(back), 2560), vec![0]);
}

#[test]
fn freeze_install_round_trips_running_and_a_v2_image_migrates_from_pins() {
    let mut f = fsm_with_rows();
    apply_at(&mut f, &ClusterCommand::RowGenesis(RowGenesis { row: 2, version: 7 }), 640);
    let (img, pos) = f.freeze().unwrap();
    let mut g = fsm_with_rows();
    g.install_snapshot(pos, &mut &img[..]).unwrap();
    assert_eq!(g.state().running, f.state().running);
    // v2 image with a pin for row 0 and nothing for row 1: row 0 migrates to
    // the pin's `to` at record_pos = applied; row 1 stays None.
    let img_v2 = v2_image_with_pin(UpgradePin { row: 0, from: 1, to: 2, origin: 512 }, 4096);
    let mut h = fsm_with_rows();
    h.install_snapshot(4096, &mut &img_v2[..]).unwrap();
    assert_eq!(h.state().running_for(0), Some(RowRunning { row: 0, version: 2, record_pos: 4096 }));
    assert_eq!(h.state().running_for(1), None);
}
```
`apply_at(f, cmd, end)` builds a `CLUSTER` body (prefix plus `ClusterFsm::encode_command`), sets `ctx.position = end`, calls `apply` and returns `out`. If an equivalent helper exists, use it and drop this one. `v2_image_with_pin` builds a v2 image by calling `encode_cluster_image` with a hand-written v2 byte layout. The simplest route is to reuse Task 3's `PLAN_B1_V2_FIXTURE` approach with one pin.

- [ ] **Step 2: Run to fail**: `cargo test -p uc_node --lib cluster_fsm 2>&1 | tail -5`.

- [ ] **Step 3: Implement**

- `ClusterState` gains:
```rust
    /// #33 spec §4.1: per row, the version it RUNS and the frame-END of the
    /// last accepted record that set it (genesis or pin). `None` = no record
    /// yet — distinct from `Some(version: 0)`, a recorded unversioned FSM.
    pub running: [Option<RowRunning>; CNC_MAX_SERVICES],
```
  `genesis()` sets `running: [None; CNC_MAX_SERVICES]`. Add:
```rust
    pub fn running_for(&self, row: u8) -> Option<RowRunning> {
        self.running.get(row as usize).copied().flatten()
    }
```
- `ClusterCommand::RowGenesis(RowGenesis)`.
- `ClusterRefusal::VersionAlreadySet`, with `reason_code` 60. Update the doc on `reason_code` to say "60 (#33)".
- `validate_replicated`:
```rust
            ClusterCommand::RowGenesis(g) => {
                if self.state.running_for(g.row).is_some() {
                    return Err(ClusterRefusal::VersionAlreadySet);
                }
                Ok(())
            }
```
  In the `UpgradePin` arm, replace `if p.from != cur.to { return Err(PinFromMismatch) }` with a check against the running version, which covers the pinned case too, since a pin sets running:
```rust
                if let Some(r) = self.state.running_for(p.row)
                    && !uc_protocol::identity::same_line(p.from, r.version)
                {
                    return Err(ClusterRefusal::PinFromMismatch);
                }
```
  Keep the `origin` monotonicity check against `pin_for` exactly as it is.
- `decode_command` / `encode_command`: add the `RowGenesis` arms using Task 1's codec.
- `apply`, in the accepted `match`:
```rust
            ClusterCommand::RowGenesis(g) => {
                self.state.running[g.row as usize] =
                    Some(RowRunning { row: g.row, version: g.version, record_pos: ctx.position });
            }
            ClusterCommand::UpgradePin(p) => {
                self.state.push_pin(p);
                self.state.running[p.row as usize] =
                    Some(RowRunning { row: p.row, version: p.to, record_pos: ctx.position });
            }
```
- `query`: `Some(6) => encode_running_list(&self.state.running.iter().flatten().copied().collect::<Vec<_>>(), out),`
- `freeze`: encode the running list into a `Vec` and pass `running: &running` in the parts.
- `install_snapshot`: after decoding pins:
```rust
        let mut running = [None; CNC_MAX_SERVICES];
        if parts.running.is_empty() {
            // v1/v2 image (#33 spec §5.3): a pinned row runs its newest pin's
            // `to`; the pin's own record position is not in the image, so use
            // `applied` — at or above it, which is all attach needs.
            for row in 0..CNC_MAX_SERVICES as u8 {
                if let Some(p) = pins.iter().rev().find(|p| p.row == row) {
                    running[row as usize] = Some(RowRunning { row, version: p.to, record_pos: parts.applied });
                }
            }
        } else {
            for r in decode_running_list(parts.running).ok_or_else(|| bad("cluster image running"))? {
                running[r.row as usize] = Some(r);
            }
        }
```
  and set `running` in the new `ClusterState`. A v3 image with no rows recorded also has an empty blob and takes the migration branch. That is harmless, because such an image also has no pins.
- `ClusterView`:
  - Add `pub versioned: AtomicU8` (init 0).
  - Add `running: [Option<RowRunning>; CNC_MAX_SERVICES]` to `ClusterViewInner`.
  - `publish` copies `running` under the lock, then stores `versioned`, the bitmask of `Some` rows, with Release, before `position`.
  - `to_state` fills `running` from inner.
  - Add:
```rust
    /// #33: one row's running version, under the inner lock (pin door, genesis).
    pub fn running_for(&self, row: u8) -> Option<RowRunning> {
        self.inner.lock().unwrap().running.get(row as usize).copied().flatten()
    }
```

- [ ] **Step 4: Run**: `cargo test -p uc_node --lib cluster_fsm cluster_agent 2>&1 | grep "test result"`. Expected: ok. Also update the stale doc at `cluster_fsm.rs:36-43`, which still explains the image version being `1`, to say 3.
- [ ] **Step 5: Commit** — `git commit -am "cluster_fsm(#33): per-row running version; RowGenesis (60); pins set running and start from its line; image v3"`

---

### Task 5: The cluster agent publishes row views and `cluster_applied`

**Files:**
- Modify: `uc_node/src/cluster_agent.rs:300-311` (`publish_view`), `:551-568` (do_work tail), `:596-612` (`install_from`), the `cluster_command_applied` event near `:506-513`

**Interfaces:**
- Consumes: `store_row_view`, `store_cluster_applied` (Task 2); `running_for` (Task 4).
- Produces: every declared row's words are republished on every view publish, and `cnc.cluster_applied()` is at least the frame-end of every applied `CLUSTER` frame.

- [ ] **Step 1: Failing test** (cluster_agent test module; model it on the existing pin-publish test, found with `grep -n "store_pin\|pin()" uc_node/src/cluster_agent.rs | tail`)

```rust
#[test]
fn an_applied_genesis_reaches_the_row_view_before_cluster_applied_moves() {
    let h = agent_harness(); // the module's existing fixture
    let end = h.append_cluster(&ClusterCommand::RowGenesis(RowGenesis { row: 0, version: pack_version(1, 0, 0) }));
    h.commit_to(end);
    h.run_until(|| h.cnc.cluster_applied() >= end);
    assert_eq!(
        h.cnc.service_slot(0).status.row_view(),
        RowRead::View { pin: None, running: Some(pack_version(1, 0, 0)), record_pos: end }
    );
}
```
Name the fixture methods after whatever the existing pin test uses.

- [ ] **Step 2: Run to fail**: `cargo test -p uc_node --lib cluster_agent 2>&1 | tail -5`.

- [ ] **Step 3: Implement**

```rust
    fn publish_view(&mut self) {
        let st = self.fsm.state();
        self.view.publish(st);
        for row in 0..uc_protocol::v2::cnc::CNC_MAX_SERVICES as u8 {
            let pin = st.pin_for(row).map(|p| (p.origin, p.from, p.to));
            let run = st.running_for(row);
            self.cnc.service_slot(row as usize).status.store_row_view(
                pin,
                run.map(|r| r.version),
                run.map_or(0, |r| r.record_pos),
            );
        }
    }
```
In `do_work`, inside `if applied_any { self.publish_view(); }`, add `self.cnc.store_cluster_applied(self.fsm.state().applied);` right after `publish_view()`. Add the same line after `publish_view()` in `install_from`. The spec wants it written only after a batch that applied or installed something, never per pass, to keep false sharing on the 4032 line rare. The existing `self.view.note_consumed(..)` stays last.

Extend the `cluster_command_applied` obs event path: on an accepted kind 6 or kind 4, also emit `row_version_recorded` with `row`, `version`, `position`, `source` (`"genesis"`/`"pin"`).

- [ ] **Step 4: Run**: `cargo test -p uc_node --lib 2>&1 | grep "test result"`. Expected: ok.
- [ ] **Step 5: Commit** — `git commit -am "cluster_agent(#33): publish every row's running version; cluster_applied after each applying batch"`

---

### Task 6: The leader appends genesis; the pin door learns the running version

**Files:**
- Modify: `uc_node/src/node.rs` — reason constants near `:546-558`; `do_work` near `:4096-4105`; new fn beside `maybe_commit_datagram_mtu` (`:6803`); `apply_upgrade_pin` no-pin half (`:9028-9032`)
- Modify: `uc_node/src/audit.rs:153-167`, `:231-235`
- Modify: `uc_service/src/attach.rs:441-445` (the store order)

**Interfaces:**
- Consumes: `ClusterView::{versioned, running_for}` (Task 4); `ClusterCommand::RowGenesis`.
- Produces: `REASON_VERSION_ALREADY_SET: u32 = 60`, `AUDIT_OP_ROW_GENESIS: u32 = 100`, `SOURCE_GENESIS: &str = "genesis"`, and `fn maybe_append_row_genesis(&mut self) -> bool`.

- [ ] **Step 1: Failing test** (in `uc_node/tests/row_version.rs`, alongside Task 0's ignored test; it reuses Task 0's harness)

```rust
#[test]
fn the_leader_records_its_attached_version_as_genesis() {
    let c = three_node_cluster("rowgen");
    let leader = c.wait_leader();
    let _s = c.start::<KvV2>(leader);
    c.wait(|| matches!(
        c.page(leader).service_slot(0).status.row_view(),
        uc_log::cnc::RowRead::View { running: Some(v), record_pos, .. }
            if v == pack_version(2, 0, 0) && record_pos > 0
    ));
    // Every node agrees (applied at commit everywhere).
    for i in 0..3 {
        c.wait(|| matches!(c.page(i).service_slot(0).status.row_view(),
            uc_log::cnc::RowRead::View { running: Some(v), .. } if v == pack_version(2, 0, 0)));
    }
    assert!(c.audit_lines(leader).iter().any(|l| l.contains("\"source\":\"genesis\"")));
}
```
Add `Cluster::{page(i) -> Arc<CncPage>, wait(pred), audit_lines(i) -> Vec<String>}` to the harness. `audit_lines` reads `<dir>/audit.jsonl`.

- [ ] **Step 2: Run to fail**: `cargo test -p uc_node --test row_version the_leader_records 2>&1 | tail -5`. Expected: timeout in `wait`.

- [ ] **Step 3: Implement**

`audit.rs`: add `pub const SOURCE_GENESIS: &str = "genesis";`. Add `pub const AUDIT_OP_ROW_GENESIS: u32 = 100;`, documented as audit-only: it never appears on the admin request line, and 100 keeps it far from real admin op numbers. Add `100 => "row_genesis",` to `op_name`.

`node.rs`:
```rust
/// #33 spec §4.1: a `RowGenesis` for a row that already has a running version.
pub const REASON_VERSION_ALREADY_SET: u32 = 60;
```
In `do_work`, after the `maybe_append_snapshot_reports` block, under the same gate:
```rust
        // 3a''''. #33 spec §6.1: record the leader's own attached version for
        // any declared row that has none. One mask test in steady state.
        if serving && !hold_clients {
            did |= self.maybe_append_row_genesis();
        }
```
The function, `#[inline(never)]`:
```rust
    #[inline(never)]
    fn maybe_append_row_genesis(&mut self) -> bool {
        let declared = self.services.declared() as u8;
        let versioned = self.cluster_view.versioned.load(Ordering::Acquire);
        let missing = declared & !versioned;
        if missing == 0 {
            return false;
        }
        let view_position = self.cluster_view.position.load(Ordering::Acquire);
        if self.last_cluster_append > view_position {
            return false; // single-in-flight
        }
        let now = crate::obs::metrics::now_unix_ns();
        for row in 0..8u8 {
            if missing & (1 << row) == 0 {
                continue;
            }
            let slot = self.cnc.service_slot(row as usize);
            // Status FIRST (Acquire), then the version word: attach stores the
            // version before the status word (Release), so ATTACHED here
            // implies the version below is this incarnation's (spec §6.1).
            let (_, attached, _) = unpack_service_status(slot.status.load_acquire());
            let fresh = now.saturating_sub(slot.heartbeat_ns.load_acquire())
                < crate::services::SERVICE_STALE_NS;
            if !attached || !fresh {
                continue;
            }
            let version = slot.status.version();
            let cmd = ClusterCommand::RowGenesis(RowGenesis { row, version });
            if self.validate_cluster_command(&cmd).is_err() {
                return false;
            }
            return match self.append_cluster_frame(&cmd) {
                Ok(position) => {
                    crate::obs_event!(Info, "row_version_genesis_proposed",
                        node = self.id as u64, row = row as u64,
                        version = version as u64, position = position);
                    self.audit_row_genesis(row, version, position);
                    true
                }
                Err(_) => false, // WouldOverrun: next pass
            };
        }
        false
    }
```
`audit_row_genesis` is a copy of `audit_datagram_mtu` (`node.rs:6868-6898`) with these fields changed: `op: AUDIT_OP_ROW_GENESIS`, `op_name: op_name(AUDIT_OP_ROW_GENESIS)`, `id: version`, `addr: None`, `config_version: position`, `detail: Some(format!("row={row}"))` (if `detail` takes `Option<&str>`, bind the string first), and `source: crate::audit::SOURCE_GENESIS`. Also add the doc paragraph explaining why it is written after the append (copy the mtu one's reasoning).

Pin door no-pin half (`node.rs:9028-9032`): wrap it so it only runs when the row has no running version. When the row has one, the FSM's `same_line` rule decides:
```rust
        if state.pin_for(pin.row).is_none() && state.running_for(pin.row).is_none() {
            let attached = self.cnc.service_slot(pin.row as usize).status.version();
            if pin.from != attached {
                return self.refuse_upgrade_pin(REASON_PIN_FROM_MISMATCH);
            }
        }
```

`uc_service/src/attach.rs:441-445`: swap the order so the version is stored first:
```rust
    // #33 spec §6.1: the version word BEFORE the status word — the leader's
    // genesis reads ATTACHED (Acquire) and then the version, so this order
    // (both Release) makes the version it reads this incarnation's.
    s.status.store_version(S::VERSION);
    s.status
        .store_release(pack_service_status(row, true, incarnation.wrapping_add(1)) | capable);
```

- [ ] **Step 4: Run**: `cargo test -p uc_node --test row_version the_leader_records && cargo test -p uc_node --lib 2>&1 | grep "test result"`. Expected: ok.
- [ ] **Step 5: Commit** — `git commit -am "node(#33): the leader appends RowGenesis for its attached version; audited; pin door defers to running"`

---

### Task 7: The client gate

**Files:**
- Modify: `uc_node/src/node.rs:7810-7825` (`drain_ingress_ring`), `:5926-5949` (`drain_ingress`), and a new field on the consensus struct (next to `last_cluster_append`, `:3363`)

**Interfaces:**
- Produces: `fn rows_versioned(&mut self) -> bool`. Latched: once every declared row is versioned, it stays true for the incarnation.

- [ ] **Step 1: Failing tests** (`uc_node/tests/row_version.rs`)

```rust
#[test]
fn clients_wait_until_every_declared_row_has_a_version() {
    let c = three_node_cluster("rowgate");
    let leader = c.wait_leader();
    let client = c.client(leader);
    // No service anywhere: the row has no version, so nothing commits.
    assert!(client.submit_timeout(&put(1), std::time::Duration::from_millis(500)).is_err());
    let _s = c.start::<KvV2>(leader);
    assert_eq!(client.submit(&put(1)).expect("admitted after genesis"), vec![0]);
}

#[test]
fn ingress_gate_is_open_with_nothing_declared() {
    // Review Focus 5: a harness page declares no rows — the gate must not hold.
    let c = one_node_harness("rowgate0"); // ServicesConfig::none_for_tests()
    let leader = c.wait_leader();
    let committed = c.commit_raw_frame(leader); // appends one client frame
    assert!(committed, "a harness node must still admit client frames");
}
```
`one_node_harness` / `commit_raw_frame` copy the no-service client-frame pattern already used in `uc_node/tests/smoke.rs`. Find it with `grep -n "none_for_tests" uc_node/tests/smoke.rs`.

- [ ] **Step 2: Run to fail**: the first test fails because the submit succeeds without a version (or hangs until the timeout, depending on the client; assert on whichever outcome makes it fail on the current tree).

- [ ] **Step 3: Implement**

Field: `rows_versioned_latched: bool` (init false). Also `next_version_gate_log_ns: u64` (init 0).
```rust
    /// #33 spec §6.2: true once every declared row has a running version.
    /// Latched: a committed version is never removed.
    fn rows_versioned(&mut self) -> bool {
        if self.rows_versioned_latched {
            return true;
        }
        let declared = self.services.declared() as u8;
        let missing = declared & !self.cluster_view.versioned.load(Ordering::Acquire);
        if missing == 0 {
            self.rows_versioned_latched = true;
            return true;
        }
        if self.pass_mono_ns >= self.next_version_gate_log_ns {
            self.next_version_gate_log_ns = self.pass_mono_ns + 5_000_000_000;
            let row = missing.trailing_zeros() as u64;
            let (_, attached, _) = unpack_service_status(
                self.cnc.service_slot(row as usize).status.load_acquire());
            crate::obs_event!(Info, "version_gate_waiting", node = self.id as u64,
                row = row, leader_service_attached = attached);
        }
        false
    }
```
In `drain_ingress_ring`, inside `if serving {`, as the first check of each iteration: `if !self.rows_versioned() { break; }`. In `drain_ingress`, at the top: `if !self.rows_versioned() { return false; }`.

- [ ] **Step 4: Run**: `cargo test -p uc_node --test row_version clients_wait ingress_gate && cargo test -p uc_node 2>&1 | grep -E "test result|FAILED"`.
  **Expected fallout:** tests that declare rows but submit before any service is attached on the leader will now wait. For each failure, confirm that it declares a row and submits before attaching. If so, move its service start before the first submit. This is the same node-then-service re-shaping 2.13.0 did, and the test's intent is unchanged. List every test touched in the commit message.
- [ ] **Step 5: Commit** — `git commit -am "node(#33): no client frame until every declared row has a running version"`

---

### Task 8: Attach refuses a version off the running line

**Files:**
- Modify: `uc_service/src/config.rs:160-212` (new variant)
- Modify: `uc_service/src/attach.rs:247-271` (after the pin decision), the `ApplyState` literal (`:450+`)
- Modify: `uc_service/src/apply.rs:270-300` (`ApplyState.attach_record_pos`)
- Modify: `uc_service/src/snapshots.rs:220-227` (envelope `same_line`), and the test at `:573-586`

**Interfaces:**
- Consumes: `row_view()` / `RowRead` (Task 2); `same_line`.
- Produces: `ServiceError::VersionMismatch { name: String, row: u8, running: u32, mine: u32 }`, `ServiceError::RowViewUnreadable { row: u8 }`, `ApplyState.attach_record_pos: u64`.

- [ ] **Step 1: Failing tests** (`uc_node/tests/row_version.rs`)

```rust
#[test]
fn attach_refuses_a_binary_off_the_running_line_by_name() {
    let c = three_node_cluster("rowattach");
    let leader = c.wait_leader();
    let _v2 = c.start::<KvV2>(leader);
    c.wait_versioned(leader, 0);
    let f = c.others(leader).next().unwrap();
    c.wait_versioned(f, 0);
    let err = c.try_start::<KvV1>(f).unwrap_err_string();
    assert!(err.contains("row 0") && err.contains("2.0.0") && err.contains("1.0.0"), "{err}");
    // Same line, different patch: admitted (D3).
    let _patch = c.start::<KvV2Patch>(f); // VERSION = pack_version(2, 0, 7)
}
```
Add `kv_build!(KvV2Patch, pack_version(2, 0, 7), true);` to Task 0's builds. Add `Cluster::wait_versioned(i, row)` (waits for `running: Some(_)`) and `Attempt::unwrap_err_string()`.

In `snapshots.rs`, update the existing test at `:573-586`, which uses raw 7 vs 8 (same line 0.0.x):
```rust
        // #33 D3: the envelope check is by LINE — patch builds share the format.
        assert_eq!(
            verify_snapshot_envelope(&mut r, 4096, Some(pack_version(1, 0, 9))).map(|e| e.version),
            Ok(pack_version(1, 0, 3))
        );
        assert!(matches!(
            verify_snapshot_envelope(&mut r2, 4096, Some(pack_version(1, 1, 0))),
            Err(EnvelopeError::VersionMismatch { .. })
        ));
```
The artifact fixture there must be built with `pack_version(1, 0, 3)` instead of `7`. Adjust that fixture and use a second reader `r2` over the same bytes.

- [ ] **Step 2: Run to fail**: `cargo test -p uc_node --test row_version attach_refuses; cargo test -p uc_service snapshots`.

- [ ] **Step 3: Implement**

`config.rs`:
```rust
    /// #33 spec §7.1: the row has a committed running version and this
    /// binary is not on its line (major.minor). Refused by name before any
    /// slot word is written; the only way to move a row is a pin.
    #[error(
        "row {row} ({name:?}) runs {running}; this binary is {mine} — install a \
         {running_line} build, or move the row to this version with `uc2ctl upgrade pin`",
        running = uc_protocol::identity::VersionDisplay(*running),
        mine = uc_protocol::identity::VersionDisplay(*mine),
        running_line = LineDisplay(*running)
    )]
    VersionMismatch { name: String, row: u8, running: u32, mine: u32 },
    /// The row view could not be read consistently (`RowRead::Contended`).
    /// Transient; retry the attach.
    #[error("row {row}: the running-version words could not be read consistently; retry")]
    RowViewUnreadable { row: u8 },
```
Add a small `struct LineDisplay(u32)` in `config.rs` whose `Display` prints `"{major}.{minor}.x"` via `unpack_version`.

`attach.rs`, right after the pin `match` (`:271`):
```rust
    // 1e. #33 spec §7.1: the row's running version. Absent → proceed (a
    //     genesis record is coming and the apply loop adjudicates it).
    let attach_record_pos = match s.status.row_view() {
        RowRead::Contended => return Err(ServiceError::RowViewUnreadable { row }),
        RowRead::View { running: Some(r), .. }
            if !uc_protocol::identity::same_line(S::VERSION, r) =>
        {
            return Err(ServiceError::VersionMismatch {
                name: S::IDENTITY.name.as_str().to_string(),
                row,
                running: r,
                mine: S::VERSION,
            });
        }
        RowRead::View { record_pos, .. } => record_pos,
    };
```
Add `attach_record_pos,` to the `ApplyState` literal, and the field to `ApplyState` with the doc: "#33: every version record at or below this frame-END was decided by this attach; the apply loop adjudicates only later ones."

`snapshots.rs:220`: `&& !uc_protocol::identity::same_line(env.version, want)`.

- [ ] **Step 4: Run** `cargo test -p uc_service && cargo test -p uc_node --test row_version attach_refuses && cargo test -p uc_diffreplay --test pin_verify -- --test-threads=1` (pin_verify's pinned-exact checks must stay green: the fixtures' versions 1/2/3 are all line 0.0).
- [ ] **Step 5: Commit** — `git commit -am "service(#33): attach refuses a version off the row's running line; envelope check by line"`

---

### Task 9: The apply loop stops at a superseding record

**Files:**
- Create: `uc_service/src/version_gate.rs`
- Modify: `uc_service/src/lib.rs` (`mod version_gate;`), `uc_service/src/apply.rs:564-647` (the arm)

**Interfaces:**
- Consumes: `ApplyState.attach_record_pos` (Task 8); `row_view`, `cluster_applied` (Task 2).
- Produces:
  - `pub(crate) enum Verdict { Continue, Stop { running: u32 } }`
  - `pub(crate) fn verdict(mine: u32, rec_end: u64, view: RowRead) -> Option<Verdict>`. `None` means the view is contended; retry.
  - `pub(crate) fn on_cluster_frame(cnc: &CncPage, row: u8, mine: u32, attach_record_pos: u64, pos: u64, hdr: &FrameHeader, payload: &[u8]) -> Option<u32>`
  - `pub(crate) fn stop_fail(name: &str, running: u32, at: u64, mine: u32) -> !`

- [ ] **Step 1: Failing unit tests** (in `version_gate.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use uc_log::cnc::RowRead;
    use uc_protocol::identity::pack_version;
    const V1: u32 = pack_version(1, 0, 0);
    const V2: u32 = pack_version(2, 0, 0);
    fn view(running: Option<u32>, record_pos: u64) -> RowRead {
        RowRead::View { pin: None, running, record_pos }
    }

    #[test]
    fn verdict_continues_past_a_refused_record() {
        // record_pos < rec_end: the record at rec_end was refused (Review Focus 4).
        assert_eq!(verdict(V1, 1280, view(Some(V1), 640)), Some(Verdict::Continue));
    }
    #[test]
    fn verdict_continues_when_the_accepted_record_is_ours() {
        assert_eq!(verdict(pack_version(1, 0, 3), 1280, view(Some(V1), 1280)), Some(Verdict::Continue));
    }
    #[test]
    fn verdict_stops_when_the_accepted_record_is_not_ours() {
        assert_eq!(verdict(V1, 1280, view(Some(V2), 1280)), Some(Verdict::Stop { running: V2 }));
    }
    #[test]
    fn verdict_stops_when_a_later_record_superseded_this_one() {
        // Review Focus 1: R1 (to v2) at 1280, R2 (back to v1) at 1920; a v1
        // service reaching R1 must stop even though running reads v1 again.
        assert_eq!(verdict(V1, 1280, view(Some(V1), 1920)), Some(Verdict::Stop { running: V1 }));
    }
    #[test]
    fn verdict_on_a_contended_view_is_retry() {
        assert_eq!(verdict(V1, 1280, RowRead::Contended), None);
    }
}
```
Also add, in `uc_node/tests/row_version.rs` (Review Focus 2):
```rust
#[test]
fn a_restart_after_the_stop_is_covered_by_attach() {
    // Attach records attach_record_pos = the record's end; a matching binary
    // restarted after a stop never re-adjudicates that record.
    let c = three_node_cluster("rowrestart");
    let leader = c.wait_leader();
    let s = c.start::<KvV2>(leader);
    c.wait_versioned(leader, 0);
    drop(s);
    let s2 = c.start::<KvV2>(leader);
    let client = c.client(leader);
    assert_eq!(client.submit(&put(3)).unwrap(), vec![0]);
    assert!(s2.is_alive(), "a matching restart must not stop at the genesis record");
}
```

- [ ] **Step 2: Run to fail**: `cargo test -p uc_service version_gate 2>&1 | tail -5`.

- [ ] **Step 3: Implement** `uc_service/src/version_gate.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! #33 spec §7.2: at a version record (CLUSTER kind 4 or 6) for its own row,
//! a service defers to the cluster FSM's verdict — it waits until the
//! `uc2-cluster` agent has applied the record, then reads the row view. It
//! never decides from the record bytes: the cluster FSM may REFUSE a record,
//! and a refused record must change nothing (spec D7).

use uc_log::cnc::{CncPage, RowRead};
use uc_protocol::identity::{VersionDisplay, same_line};
use uc_protocol::v2::frame::{ClusterKind, FrameHeader, align_frame_len, read_cluster_prefix};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    Continue,
    Stop { running: u32 },
}

/// Pure decision once the agent has applied past `rec_end`:
/// - `record_pos < rec_end` → the record was refused → Continue;
/// - `record_pos == rec_end` and ours → Continue;
/// - otherwise (not ours, or a LATER record already superseded it) → Stop.
///   `None` = the view is contended; the caller retries.
pub(crate) fn verdict(mine: u32, rec_end: u64, view: RowRead) -> Option<Verdict> {
    let RowRead::View { running, record_pos, .. } = view else {
        return None;
    };
    let running = running.unwrap_or(0);
    Some(if record_pos < rec_end || (record_pos == rec_end && same_line(mine, running)) {
        Verdict::Continue
    } else {
        Verdict::Stop { running }
    })
}

/// The apply loop's arm (out of line — M14a). `pos` is the frame START.
/// Returns `Some(running)` when the service must stop BEFORE any later frame
/// (the caller publishes `applied = pos` and fail-stops); `None` otherwise.
#[inline(never)]
pub(crate) fn on_cluster_frame(
    cnc: &CncPage,
    row: u8,
    mine: u32,
    attach_record_pos: u64,
    pos: u64,
    hdr: &FrameHeader,
    payload: &[u8],
) -> Option<u32> {
    let (kind, body) = read_cluster_prefix(payload)?;
    if !matches!(kind, ClusterKind::UpgradePin | ClusterKind::RowGenesis) || body.first() != Some(&row) {
        return None;
    }
    let rec_end = pos + align_frame_len(hdr.length as usize) as u64;
    if rec_end <= attach_record_pos {
        return None;
    }
    let mut spins = 0u32;
    loop {
        if cnc.cluster_applied() >= rec_end
            && let Some(v) = verdict(mine, rec_end, cnc.service_slot(row as usize).status.row_view())
        {
            return match v {
                Verdict::Continue => None,
                Verdict::Stop { running } => Some(running),
            };
        }
        // Never sleep on a live peer (CLAUDE.md, M14a): spin, then yield.
        spins += 1;
        if spins < 1024 { std::hint::spin_loop() } else { std::thread::yield_now() }
    }
}

/// The named fail-stop message (spec §7.2).
pub(crate) fn stop_message(name: &str, running: u32, at: u64, mine: u32) -> String {
    format!(
        "version_superseded: row {name:?} moved to {} at position {at}; this binary ({}) \
         stopped there — restart it as a {}.{}.x build",
        VersionDisplay(running), VersionDisplay(mine), running >> 24, (running >> 16) & 0xff,
    )
}
```
Check `read_cluster_prefix`'s input. It takes the CLUSTER *body*, which is what the apply loop's `payload` is for a CLUSTER frame, as the cluster agent passes it. Confirm with `grep -n "read_cluster_prefix" uc_node/src/cluster_agent.rs`.

In `apply.rs`'s frame loop, add after the `FRAME_TYPE_SNAPSHOT` arm:
```rust
                    } else if hdr.frame_type == FRAME_TYPE_CLUSTER {
                        // #33 spec §7.2: the type test and one out-of-line call.
                        if let Some(running) = crate::version_gate::on_cluster_frame(
                            &st.cnc, st.service_id, S::VERSION, st.attach_record_pos, pos, &hdr, payload,
                        ) {
                            crate::attach::slot(&st.cnc, st.service_id).applied.store_release(pos);
                            crate::version_gate::stop_fail(S::IDENTITY.name.as_str(), running, pos, S::VERSION);
                        }
                    }
```
`stop_fail` is a `#[cold] #[inline(never)] fn stop_fail(..) -> !` in `version_gate.rs`. It emits `uc_obs` `version_superseded` (Error level; fields `row_name`, `running`, `mine`, `position`) and then `panic!("{}", stop_message(..))`. This is the fail-stop idiom of `apply.rs:737-746`. Import `FRAME_TYPE_CLUSTER` at `apply.rs:24`. The published `applied = pos` means every frame before the record has been applied and nothing after it.

- [ ] **Step 4: Run** `cargo test -p uc_service version_gate && cargo test -p uc_node --test row_version`. Then un-ignore Task 0's test and run it:
Run: `cargo test -p uc_node --test row_version a_mixed_version_row -- --nocapture`
Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -am "service(#33): stop at exactly the position of a superseding version record"`

---

### Task 10: `uc2ctl`

**Files:**
- Modify: `uc_ctl/src/main.rs:617-711` (`reason_str`), the per-row `println!` in `run_status` (search `heartbeat_age={age} timers_pending=`)
- Modify: `uc_ctl/src/upgrade.rs:83-95` (`--from` default), `:149-211` (`show`)
- Test: `uc_ctl/tests/status_services.rs`

**Interfaces:**
- Consumes: `row_view`; query op 6 / `read_committed_upgrade` (extend it to return the running list).

- [ ] **Step 1: Failing tests**. In `status_services.rs`'s first test, after the existing asserts, write a row view on slot 0 and assert:
```rust
    cnc.service_slot(0).status.store_row_view(None, Some(pack_version(1, 4, 2)), 640);
    let stdout = run_status(&dir); // factor the existing Command block into this helper
    assert!(stdout.contains("running=1.4.2 running_pos=640"), "{stdout}");
    assert!(stdout.contains("row=1") && stdout.contains("running=none"), "{stdout}");
```
In `main.rs` unit tests:
```rust
#[test]
fn reason_str_names_60_and_52() {
    assert_eq!(reason_str(60), "version_already_set");
    assert_eq!(reason_str(52), "row_undeclared");
}
```
- [ ] **Step 2: Run to fail**: `cargo test -p uc_ctl`.
- [ ] **Step 3: Implement.**
  - `reason_str`: add `60 => "version_already_set"` and rename 52's string.
  - `run_status`, per row: read `s.status.row_view()`. Render `running=` as `VersionDisplay(v)` for `Some(v)`, `none` for `None`, and `?` for `Contended`. Render `running_pos=` from `record_pos`. Append both after `artifact_hash=`.
  - `upgrade.rs`, `--from` default: prefer `row_view().running` when present, else fall back to the attached version word (today's behaviour).
  - `show`: extend `uc_node::cluster_agent::read_committed_upgrade` to also return `state.running`. Per row, print `running=<ver> set_at=<pos> by=<genesis|pin>`. `by` is `pin` when the row's newest pin's `to` equals the running version, and `genesis` otherwise.
- [ ] **Step 4: Run**: `cargo test -p uc_ctl`.
- [ ] **Step 5: Commit** — `git commit -am "uc2ctl(#33): status and upgrade show report the running version; reason 60; pin --from defaults to it"`

---

### Task 11: Metrics and alerts

**Files:**
- Modify: `uc_node/src/obs/metrics.rs` (the `ServiceRow` struct ~`:337`, the row build `:396-445`, the family list `:65-130`, and the renderer beside `uc2_service_version` `:693`)
- Modify: `packaging/prometheus/uc2-alerts.yml:178-189`
- Modify: `uc_node/examples/m10_alerts.rs` (scenario list `:68`, dispatch `:202`, new scenario beside `scenario_version_drift` `:1333`)
- Modify: `scripts/m10_alert_fire.sh` (`:273` table, builders `:516`, map `:759`)

**Interfaces:**
- Produces: `uc2_row_running_version{service,row}` (packed; the row is omitted when absent) and alert `Uc2RowVersionMismatch`.

- [ ] **Step 1: Failing test** (metrics test module; model on the existing `uc2_service_version` rendering test, found with `grep -n "uc2_service_version" uc_node/src/obs/metrics.rs | tail -3`):
```rust
#[test]
fn row_running_version_renders_only_for_rows_that_have_one() {
    let src = synthetic_sources_named(0, Some(FsmName::parse("kv").unwrap()));
    src.cnc.store_services_declared(0b11);
    src.cnc.service_slot(0).status.store_row_view(None, Some(pack_version(1, 4, 2)), 640);
    let text = render(&src);
    assert!(text.contains(&format!("uc2_row_running_version{{service=\"kv\",row=\"0\"}} {}", pack_version(1, 4, 2))), "{text}");
    assert!(!text.contains("uc2_row_running_version{service=\"\",row=\"1\"}"), "{text}");
}
```
- [ ] **Step 2: Run to fail.**
- [ ] **Step 3: Implement.**
  - `ServiceRow.running: Option<u64>`, filled from `row_view()`.
  - Register the family name in the list at `:65-130`.
  - Render it with a variant of `push_service_labeled` that skips `None` rows. HELP text: "Packed version the row RUNS (#33) — committed by genesis or a pin; absent before the first record. Alert: Uc2RowVersionMismatch."
  - Alerts, in `uc2-alerts.yml`:
```yaml
  - alert: Uc2RowVersionMismatch
    # #33: an attached service whose major.minor differs from its row's
    # committed running version. It can only last until that service stops
    # at the superseding record, so a firing alert means a stuck stop.
    expr: floor(uc2_service_version / 65536) != on(instance, row) floor(uc2_row_running_version / 65536) and on(instance, row) uc2_service_attached == 1
    for: 1m
    labels: { severity: critical }
    annotations: { summary: "row {{ $labels.row }} on {{ $labels.instance }} runs a version off its committed line" }
```
    Change `Uc2ServiceVersionDrift`'s expr to compare lines, since patch builds may differ (D3). Update its comment accordingly:
    `count by (row) (count_values("line", floor(uc2_service_version / 65536) > 0) by (row)) > 1`.
    The attached-gauge metric name above must match the real one; check with `grep -n "\"uc2_service_attached\|uc_service_attached" uc_node/src/obs/metrics.rs`.
  - `m10_alerts.rs`: add `"row_version_mismatch"` to `ALL_SCENARIOS` and the dispatch. The scenario is `scenario_version_drift`'s one-source shape: declare row 0 `kv`, `store_version(pack_version(1,3,0))`, set the attached status bit via `store_release(pack_service_status(0, true, 1))`, and `store_row_view(None, Some(pack_version(1,2,0)), 640)`. Record the three families.
  - `m10_alert_fire.sh`: table entry `"Uc2RowVersionMismatch": {"severity": "critical", "real": False, "scenario": "row_version_mismatch"}`, and a builder mirroring `build_Uc2LogTimeFrozen`'s two-series `and` shape over the three series, held 60 s.
- [ ] **Step 4: Run**: `cargo test -p uc_node --lib obs && cargo run -p uc_node --example m10_alerts -- --scenario row_version_mismatch && scripts/m10_alert_fire.sh` (needs `promtool`; if it is absent, say so in the task report and do not claim the rule fires).
- [ ] **Step 5: Commit** — `git commit -am "obs(#33): uc2_row_running_version, Uc2RowVersionMismatch; version drift compares lines"`

---

### Task 12: End-to-end: the pin stops old services exactly

**Files:**
- Modify: `uc_node/tests/row_version.rs`

- [ ] **Step 1: Write the test** (the rest of spec §10.2; §10.1 is Task 0, now passing)

```rust
#[test]
fn a_committed_pin_stops_every_old_service_at_exactly_the_record() {
    let c = three_node_cluster("rowpin");
    let leader = c.wait_leader();
    let olds: Vec<_> = (0..3).map(|i| c.start::<KvV1>(i)).collect();
    let client = c.client(leader);
    for v in 0..200 { client.submit(&put(v)).unwrap(); }
    let origin = c.snapshot_instant(leader);          // admin op 8, wait for the complete set
    let pin_end = c.pin(leader, 0, pack_version(1, 0, 0), pack_version(2, 0, 0), origin); // admin op 10
    for (i, s) in olds.iter().enumerate() {
        c.wait(|| !s.is_alive());
        let slot = c.page(i).service_slot(0);
        // applied = the record's START (every earlier frame applied, nothing after)
        assert_eq!(slot.applied.load_acquire(), c.frame_start_of(pin_end, i),
            "node {i}'s v1 stopped somewhere other than the pin record");
    }
    let news: Vec<_> = (0..3).map(|i| c.start::<KvV2>(i)).collect();
    assert_eq!(client.submit(&append(1)).unwrap(), vec![0]);
    assert_eq!(c.client(leader).query_u64(), 200);    // 199 + 1, recomputed from the origin
    drop(news);
}
```
The harness gains:
- `snapshot_instant(i) -> u64`: issue admin op 8 and wait for `snapshot_set_position`.
- `pin(i, row, from, to, origin) -> u64`: stage `upgrade.pending` via `uc_node::cluster_fsm::UPGRADE_PENDING_FILE`, send admin op 10 through the cnc admin line, and return the reply's position.
- `frame_start_of(end, i)`: walk node `i`'s log to the frame whose end is `end`.

Copy the admin-request mechanics from `uc_ctl/tests/snapshot_bin.rs`, which drives ops 8/9 against an in-process node. The `wait(|| !s.is_alive())` covers the service's fail-stop panic. Assert the stop message separately by capturing `uc_obs` with the `OBS_CAPTURE_LOCK` pattern (`grep -rn OBS_CAPTURE_LOCK uc_node/src | head -3`) and checking for `version_superseded`.

- [ ] **Step 2: Run**: `cargo test -p uc_node --test row_version`. Expected: all pass, and none ignored.
- [ ] **Step 3: Commit** — `git commit -am "test(#33): a pin stops every old service at exactly the record; the #33 repro passes"`

---

### Task 13: Docs

**Files:** `docs/how-to/upgrade-an-application.md`, `docs/reference/uc2ctl.md`, `docs/reference/cnc-page.md`, `docs/reference/wire-protocol.md` (kind table `:355-363`, version `:13`), `docs/reference/semver-policy.md`, `docs/how-to/upgrade-a-cluster.md`, a new `docs/notes/uc2-row-running-version-explained.md`, and `docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md` §9.2.

- [ ] **Step 1:** In `upgrade-an-application.md`, add a first section, "The version rules", covering:
  - major.minor must match on every node, patch is free, and "patch" must not change replicated behaviour *or the snapshot format*;
  - `0` is a version;
  - every change of line is a pin;
  - the standby instant on a learner is the recommended origin;
  - what `VersionMismatch` and `version_superseded` mean and what to do about each;
  - that `uc2-diffreplay upgrade` is how to check that a patch really is a patch.

  Keep the existing pin procedure and add the "old services stop by themselves at the pin" step to it.
- [ ] **Step 2:** In `uc2ctl.md`, add the `status` table rows for `running=`/`running_pos=`, `upgrade show`'s running line, reason 60, and 52's new name. In `cnc-page.md`, add the 3.4 words (slot `+48`, `+56`; page `4056`). In `wire-protocol.md`, add kind 6 and version 0.10.0. In `semver-policy.md`, record the new wire and cnc minor versions. In `upgrade-a-cluster.md`, add the 0.10.0 / 3.4 flag day (stop every node before starting any), with no wipe needed since image v3 reads v1/v2.
- [ ] **Step 3:** Write the explainer `docs/notes/uc2-row-running-version-explained.md`, in plain language:
  - the #33 story;
  - why a row may never run two versions;
  - genesis, attach and the exact stop;
  - why only a pin (the adopt and genesis-pin rejections, with the storage and replay arithmetic from spec §3.1).

  Link it from the spec.
- [ ] **Step 4:** Add a line to the lifecycle spec's §9.2: "Closed by `2026-09-27-uc2-row-running-version-design.md` (#33) for the safety half; rolling application upgrades are #66."
- [ ] **Step 5:** Run `grep -rn "0\.9\.0\|cnc 3\.3\|cnc \`3\.3\`" docs/reference docs/how-to QUICKSTART.md README.md` and update any statement this release invalidates. Commit: `git commit -am "docs(#33): version rules, the running version, wire 0.10.0 / cnc 3.4"`.

---

### Task 14: Fuzz, hot loop, full proof stack

**Files:** `fuzz/fuzz_targets/uc_node_cluster_artifact.rs` and the cluster-command decode target (`ls fuzz/fuzz_targets | grep -i cluster`); no product code.

- [ ] **Step 1: Fuzz.** Extend the cluster-command target's kind table with 6, and the artifact target's seed corpus with one v3 image. Generate it by writing a unit test that freezes an FSM holding a genesis, and saving the bytes to `fuzz/corpus/uc_node_cluster_artifact/v3_running`. Run `scripts/fuzz_smoke.sh 60 --min-runs 10000 uc_node_cluster_artifact <cluster-command target>`. If nightly or cargo-fuzz is unavailable, say so rather than claim a pass.
- [ ] **Step 2: Apply hop A/B.** Build `apply_bench` at `origin/main` and at HEAD into private target dirs, plus a same-source rebuild control (`scripts/hop1_ab.sh` is the pattern; `uc_node/examples/apply_bench`). Report the delta against the control's spread. This is smoke only: any bar is fleet-only (CLAUDE.md, "Benchmarking discipline"). If the arm costs more than the control's spread, read the per-frame call list with `objdump -d -C` as CLAUDE.md describes before changing anything.
- [ ] **Step 3: Proof stack**, each command's result recorded in the PR body:

```bash
cargo build -p uc_lincheck --features replay-bin --bin register-replay && cargo build -p uc_diffreplay
cargo test --workspace --no-fail-fast
cargo test -p uc_node --test lin_v2
cargo test -p uc_node --test lin_partition_v2
cargo test -p uc_crashtest --features hard-crash-tests
cargo test -p uc_diffreplay --test pin_verify -- --test-threads=1
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p uc_crashtest --all-targets --features hard-crash-tests -- -D warnings
CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings
cargo fmt --all -- --check
```
- [ ] **Step 4:** Commit any fixture/corpus additions: `git commit -am "fuzz(#33): kind 6 and image v3 seeds"`.
