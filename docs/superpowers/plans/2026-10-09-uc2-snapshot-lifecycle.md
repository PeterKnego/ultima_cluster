# UC2 Snapshot Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every row starts from the newest agreed snapshot set its node holds (published on the cnc page, installed by the service when it moves the row forward), every node keeps its own copy of the newest agreed set by fetching it in the background when space allows, artifact sizes ride the reports into the catalog, and a pin's origin must be an agreed set.

**Architecture:** Sizes enter at the reporter (`SNAP_REPORT` 32 B, report entries `(node, hash, size)`), are folded by the cluster FSM into `RowEntry.size` (the majority hash's size) and summed per set; the cluster image moves to v5 and still reads v1–v4 with every size `0`. The consensus agent publishes a per-row **start set** into two new words on the row's `snapshot_pos` cnc line; the service reads them at attach and in overrun recovery and installs through the existing `SnapshotStore`/`ULTSNAP2` checks. A pure `AutoFetch` state machine (new `uc_node::auto_fetch`) decides when and from whom the consensus agent issues the existing store-only fetch; a replicated `Settings` v4 `auto_fetch` switch turns it off. The pin door gains refusal 61.

**Tech Stack:** Rust 1.96 (MSRV 1.89), `uc_protocol` LE codecs, `crc32fast`, the `uc2-cluster` and consensus agents, `uc_service` attach/replay, `cargo-fuzz`, `promtool`.

**Spec:** `docs/superpowers/specs/2026-10-09-uc2-snapshot-lifecycle-design.md` (read §3 decisions and the Review focus list first). Predecessor: `docs/superpowers/specs/2026-10-01-uc2-snapshot-catalog-design.md` — read its errata and the R37–R42 amendments before Tasks 6, 7 and 9.

## Global Constraints

- **No attribution or co-author trailer** on any commit (no `Co-Authored-By`, no "Generated with").
- Scratch files under `$HOME/scratch/`, **never `/tmp`**.
- Private target dir for every build and proof run in this plan: `export CARGO_TARGET_DIR=$HOME/.cache/cargo-target-lifecycle`.
- **Do not touch `CLAUDE.md`.**
- `cargo fmt --all -- --check` is enforced by CI; run `cargo fmt --all` before every commit.
- MSRV 1.89 gate before every push: `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings`.
- Everything below changes **inside the unreleased flag day**: wire stays `0.11.0`, cnc stays `3.4`; nothing on disk is cleared (spec §9).
- `SNAP_REPORT` datagram (kind 26) body: **24 → 32 B**, `size u64 @24` (the artifact's byte size; `0` = unknown).
- `CLUSTER` kind 5 `SnapshotReport` entries: **`(node_id u32, hash u64, size u64)`**, still strictly increasing by node id.
- `RowEntry` gains `size: u64` — the size reported with the **majority** hash; `ROW_ENTRY_LEN` **13 → 21**, `SET_ENTRY_LEN` **135 → 207**. `size = 0` means **unknown**.
- A set's total size = sum of its rows' and its cluster artifact's sizes.
- Cluster image **v5** carries the new layout; **v1–v4 images decode with every size `0`**. No wipe.
- `Settings` record **v4** adds `auto_fetch: bool`, **default `true`**; **v1–v3 records decode as `true`**; `[settings] auto_fetch` seeds genesis; `uc2ctl settings apply` changes it.
- cnc service slot (cnc 3.4, folded in): **`start_set_pos` u64 @ slot `+264`** (0 = none), **`start_set_version` @ slot `+272`** (low 32 = packed version that built row r's artifact). Writer: node consensus agent, on change. Readers: service apply thread at attach and overrun recovery. Written `version`, then `pos` (Release); read `pos`, `version`, `pos` again.
- Start set eligibility (spec §4.1): `e.is_agreed()` ∧ `e.rows[r].verdict == Agreed` ∧ held complete on disk (`holdings_held`) ∧ `e.position <= min(commit, durable)`. Empty catalog ⇒ start set `0`.
- Start rule (spec §5): pinned row → pin path, unchanged, with priority; else install when `start_set_pos > resume` and the version rule holds; else today's behaviour; install failure → log once by name, fall back to replay. Strict `>`: never rewind.
- Auto-fetch (spec §6): trigger = newest agreed set `N` not held ∧ `N <= durable`; only the newest; holder = live holders first, **learners first, then lowest node id, never self**; **stagger `node_id × 250 ms`** on the first attempt for a given `N`; retry on a refusal or the **60 s `FETCH_TIMEOUT_NS`** to the next holder, **backing off from 1 s, doubling to 30 s**; one fetch at a time per node through `start_fetch`/`PendingFetch`/`IntakeMode::StoreOnly`.
- Fetch space check (spec §7.3): **`free_bytes >= total + max(total/4, 1 GiB)`**, using the `uc2-holdings` probe's `preflight::free_disk_bytes` reading; a set of unknown size (`0`) is fetched without the check, with a log line. The headroom is a fixed default, not a setting.
- Counter **`uc2_snapshot_auto_fetch_total{outcome}`**, outcomes **`ok`, `refused`, `timeout`, `no_space`, `no_holder`**.
- Gauge **`uc2_snapshot_newest_agreed_bytes`** (0 when unknown or none). Alert **`Uc2SnapshotWontFit`** (the newest agreed set would fail §7.3's check on this node; fires on every node, learners and switch-off nodes included).
- Obs event **`snapshot_fetch_skipped_no_space`** (names the set, its size and the free bytes; once per set).
- Audit: the existing `snapshot_fetch` record (op 9) with **`actor = "auto"`**.
- Admin refusal **61 `pin_origin_not_agreed`** on admin op 10 when the origin is not an agreed catalog entry; on an **Empty** catalog (no `Complete` entry, catalog ruling R26) the pin is allowed with a log line. Refusals 52–60 unchanged.
- Fuzz seed regeneration after every encoding change (ruling R45): `(cd fuzz && cargo +nightly run --bin seed-corpus)`, then `git status` must show only intended corpus changes.

## Review Focus

From the spec's Review focus list; each line names the test that pins it and the task that owns it.

1. **A service restarting during the background fetch of the very set it would start from** must start from the older held set or replay, never a half-written artifact. → Task 7 `start_set_skips_a_set_that_is_still_being_fetched` (the publisher never names a set not yet in `holdings_held`); Task 8 `a_start_set_whose_artifact_is_missing_falls_back_to_replay` and `a_start_set_whose_envelope_is_bad_falls_back_to_replay`.
2. **A pinned row whose cluster's newest agreed set is newer than the pin origin** — the pin path must win. → Task 8 `a_pinned_row_never_takes_the_start_set` (attach-side gate) and `the_overrun_jump_is_refused_on_a_pinned_row_or_after_a_new_version_record`.
3. **`holders()` listing a node whose soft entry is stale** — the fetch must fail over, not wait out the full timeout repeatedly on a dead holder. → Task 9 `a_timed_out_holder_is_not_tried_again_before_every_other_candidate` and `a_lone_dead_holder_costs_one_timeout_per_backoff_ceiling`.
4. **Sizes on a set catalogued before this change (`0`)** — the check must not block it and the gauge must not alarm. → Task 9 `an_unknown_size_is_fetched_without_the_check`; Task 4 `the_newest_agreed_bytes_word_is_zero_when_any_size_is_unknown`; Task 10 `the_wont_fit_rule_needs_a_known_size` (the rule's `> 0` guard).
5. **A learner-only cluster with one learner and the switch on** — voters fetch and purge on a set agreed over one reporter; alert and docs must make that visible. → Task 9 `reporters_at_counts_the_reporters_of_one_instant` plus the `snapshot_fetch_single_reporter` warning; Task 13 e2e 1 asserts the warning is emitted; Task 14 documents it in `bound-journal-growth.md` and the runbook.

## Plan rulings (choices this plan makes where the spec is silent or ambiguous)

Recorded in the spec's errata block by Task 14.

- **P1 — Holder candidates on a follower.** `holders()` reads the soft table, which only the LEADER fills (`STATUS` goes follower → leader). On any node the candidate list is: live soft-table holders (leader only), then the set's **builders** (node ids in the committed `SnapshotReport` records at `N` whose hash matches the catalog's row hash), each tier ordered learners-first then lowest id; then every other member, learners first then lowest id; never self. A candidate that does not hold the set answers nothing and costs one timeout.
- **P2 — "Refused".** A holder that cannot serve sends no datagram (sender `open_snap_session` returns `false` silently), so `refused` counts what the node can see: the local issue failing (`issue_fetch` → `Err`, i.e. route full, unknown address). A silent non-answer is `timeout`.
- **P3 — Version rule for the start set.** Spec §5 writes `start_set_version == S::VERSION`; D5 says "keep today's version rule", and today's unpinned install compares by **line** (`same_line`, #33 D3). This plan uses `same_line(start_set_version, S::VERSION)` and the `ULTSNAP2` envelope check `verify_snapshot_envelope(.., Some(S::VERSION))`, which is also by line.
- **P4 — Install failure.** "Artifact missing or unreadable" (open fails, envelope fails) falls back to replay with one log line. An error from the state machine's own `install_snapshot` after the envelope verified is a fail-stop (`ServiceError::Replay`): the state machine may be half-mutated and replaying on top of it would be a silent wrong state.
- **P5 — Overrun jump guard.** In overrun recovery the jump is refused when the row's live view now carries a pin, or its `running_record_pos` is above the walk's `decided_to` — jumping over a version record committed since attach would skip the #33 exact stop.
- **P6 — Size measured.** `size` = the artifact FILE's length on disk (envelope included for a row; the bare image for the cluster) — what a fetch transfers and what the space check must budget. Measured with one `fs::metadata` per row on the completion edge (once per instant, never per pass); the cluster artifact's size is published by the `uc2-cluster` agent beside its hash (`ClusterArtifactHash`).
- **P7 — Majority size.** `RowEntry.size` = the **largest** size reported with the majority hash (a reporter whose `stat` failed reports `0`, which must not erase the size); `0` with no majority.
- **P8 — A node still building `N` does not fetch it.** Auto-fetch skips `N` while any attached declared row on this node has `snapshot_pos < N` and `applied < N` and the set is not a standby set on a voter (it will freeze at `N` itself). Avoids a fetch racing a local build of the same files.
- **P9 — Waiting is bounded.** While the trigger waits (above `durable`, or building) the decision is re-checked every 100 ms (`AUTO_FETCH_RECHECK_NS`), not every pass.
- **P10 — `no_holder`** = every candidate for `N` already tried (or none exists): the tried list is cleared and the next attempt waits the 30 s ceiling.
- **P11 — `no_space`** backs off on the same ladder as `refused`/`timeout`; the obs event is named once per set; the counter counts every skip.
- **P12 — Thin agreement.** When the set being fetched was reported by exactly one node (`reporters_at == 1`), the node emits `snapshot_fetch_single_reporter` (warn, once per set) — the visibility spec Review focus 5 asks for.
- **P13 — Pin door 61 is door-only** (in `Consensus::apply_upgrade_pin`, after 54), not a `ClusterRefusal`: the catalog can retire entries between append and apply, and refusing at apply would make an accepted request fail later.
- **P14 — Test seam.** `Node::set_free_bytes_for_test(u64)` (`#[doc(hidden)]`, `0` = off) makes the `uc2-holdings` probe report that figure; e2e 5 needs it.
- **P15 — Inherited straggler residual.** A new fetch clears the receiver's single parked expired-fetch slot (`last_expired_fetch`, fix round 3). Auto-fetch issues fetches more often than an operator, so it never issues one while `pending_fetch` is set and always waits at least the backoff floor (1 s) after a timeout. The residual is unchanged otherwise and named in the errata.

---

## File structure

| File | Responsibility | Task |
|---|---|---|
| `uc_protocol/src/v2/datagram.rs` | `SNAP_REPORT` body 32 B | 1 |
| `uc_protocol/src/v2/upgrade.rs` | report entries with sizes, unsized decoders, `majority_size` | 1 |
| `uc_net/src/receiver.rs` | `NetEvent::SnapReport.size` | 1 |
| `uc_protocol/src/v2/catalog.rs` | `RowEntry.size`, 21/207 B layout, unsized list decoder, `SetEntry::total_size` | 2 |
| `uc_protocol/src/v2/cluster_image.rs` | `CLUSTER_IMAGE_VERSION = 5`, `cluster_image_version` | 2 |
| `uc_protocol/src/v2/settings.rs` | `Settings` v4 `auto_fetch` | 3 |
| `uc_node/src/config_file.rs`, `uc_ctl/src/settings.rs` | `[settings] auto_fetch`, `settings show` | 3 |
| `uc_node/src/cluster_fsm.rs` | majority size fold, v1–v4 image install, view words `auto_fetch`, `catalog_newest_agreed_bytes` | 3, 4 |
| `uc_protocol/src/v2/cnc.rs`, `uc_log/src/cnc.rs` | start-set words, `SnapshotPosLine` | 5 |
| `uc_node/src/cluster_agent.rs`, `uc_node/src/node.rs` | sizes on the report path | 6 |
| `uc_node/src/catalog.rs` | `start_set_for`, `fetch_candidates`, `builders_at`, `reporters_at` | 7, 9 |
| `uc_node/src/node.rs` | start-set publisher, auto-fetch integration, pin door 61, test seam | 7, 9, 11 |
| `uc_service/src/start_set.rs` (new), `attach.rs`, `replay.rs`, `apply.rs`, `lib.rs` | the start rule | 8 |
| `uc_node/src/auto_fetch.rs` (new), `uc_node/src/lib.rs`, `uc_node/src/audit.rs` | the auto-fetch state machine, stats, `SOURCE_AUTO` | 9 |
| `uc_node/src/obs/{mod,metrics}.rs`, `packaging/prometheus/uc2-alerts.yml`, `scripts/m10_alert_fire.sh`, `uc_node/examples/m10_alerts.rs` | counter, gauge, alert | 10 |
| `uc_ctl/src/main.rs`, pin retry helpers | refusal 61 | 11 |
| `fuzz/src/seeds.rs`, `fuzz/corpus/**` | seeds | 12 |
| `uc_node/tests/catalog.rs`, `uc_node/tests/learner.rs` | e2e; fixtures pinned to `auto_fetch = false` | 9, 13 |
| docs (Task 14) | sweep + spec errata | 3, 10, 11, 14 |

---
### Task 1: Reports carry sizes — the `SNAP_REPORT` body and the `SnapshotReport` record

**Files:**
- Modify: `uc_protocol/src/v2/datagram.rs` (`SNAP_REPORT_BODY_LEN` ~:542, `SnapReportBody` ~:550, `write_snap_report_body`/`read_snap_report_body` ~:557–592, test `snap_report_kind_and_body_are_pinned` ~:1580)
- Modify: `uc_protocol/src/v2/upgrade.rs` (constants ~:75–82, `SnapshotReport` ~:88, `ids_strictly_increasing`, `encode_snapshot_report`, `decode_snapshot_report`, `verdict`, `decode_report_list` ~:319)
- Modify: `uc_net/src/receiver.rs` (`NetEvent::SnapReport` ~:260, the forward ~:2094, its test ~:4382)
- Modify: `uc_node/src/node.rs` (`PendingSnapshotReport` ~:3284, `on_snap_report` ~:7220, the record build in `maybe_append_snapshot_reports` ~:7460, `deliver_snapshot_reports` ~:6925, the `NetEvent::SnapReport` arm ~:9716)
- Modify (mechanical, compile-driven): every `SnapshotReport { .. hashes: vec![..] }` literal and every `(id, h)` destructure of `.hashes` in `uc_node/src/{cluster_fsm,cluster_agent,node}.rs`, `uc_node/src/obs/metrics.rs`, `uc_node/examples/m10_alerts.rs`, `uc_node/tests/snapshot_reports.rs`, `uc_ctl/src/upgrade.rs`
- Test: `datagram.rs`, `upgrade.rs` and `node.rs` `mod tests`

**Interfaces:**
- Produces (`uc_protocol::v2::datagram`): `pub const SNAP_REPORT_BODY_LEN: usize = 32;` `SnapReportBody { row: u8, node_id: u32, position: u64, hash: u64, size: u64 }`.
- Produces (`uc_protocol::v2::upgrade`): `SnapshotReport.hashes: Vec<(u32, u64, u64)>` = `(node_id, hash, size)`; `SNAPSHOT_REPORT_ENTRY_LEN = 20`; `SNAPSHOT_REPORT_ENTRY_LEN_UNSIZED = 12`; `pub fn decode_snapshot_report_unsized(buf: &[u8]) -> Option<SnapshotReport>`; `pub fn decode_report_list_unsized(buf: &[u8]) -> Option<Vec<SnapshotReport>>`; `pub fn majority_size(r: &SnapshotReport, v: &Verdict) -> u64`.
- Produces (`uc_net::receiver`): `NetEvent::SnapReport { from: u32, row: u8, position: u64, hash: u64, size: u64 }`.
- Produces (`uc_node` private): `Consensus::on_snap_report(&mut self, from: NodeId, row: u8, position: u64, hash: u64, size: u64)`; `PendingSnapshotReport.sizes: BTreeMap<u32, u64>`. Task 6 replaces the two `size: 0` placeholders this task leaves in `deliver_snapshot_reports`.

- [ ] **Step 1: Write the failing tests.** In `uc_protocol/src/v2/datagram.rs` `mod tests`:

```rust
    /// Snapshot-lifecycle spec §7.1: the body grows to 32 B with the
    /// artifact's byte size at @24. A 24-byte body (the unreleased draft of
    /// 0.11.0) is refused by length, like any mixed flag day.
    #[test]
    fn snap_report_body_is_32_bytes_and_carries_the_artifact_size() {
        assert_eq!(SNAP_REPORT_BODY_LEN, 32);
        let b = SnapReportBody {
            row: 1,
            node_id: 2,
            position: 4096,
            hash: 7,
            size: 0x0102_0304_0506_0708,
        };
        let mut buf = [0u8; SNAP_REPORT_BODY_LEN];
        write_snap_report_body(&mut buf, &b);
        assert_eq!(&buf[24..32], &0x0102_0304_0506_0708u64.to_le_bytes(), "size @24");
        assert_eq!(read_snap_report_body(&buf), Some(b));
        assert_eq!(read_snap_report_body(&buf[..24]), None, "a 24 B body is refused by length");
    }
```

In `uc_protocol/src/v2/upgrade.rs` `mod tests`:

```rust
    /// Snapshot-lifecycle spec §7.1: entries are `(node_id u32, hash u64,
    /// size u64)`; a v1–v4 cluster image's report blob still decodes, through
    /// the UNSIZED decoders, with every size 0.
    #[test]
    fn report_entries_carry_sizes_and_the_unsized_layout_reads_zero() {
        let r = SnapshotReport {
            row: 1,
            position: 4096,
            hashes: vec![(0, 7, 100), (2, 7, 100)],
        };
        let mut b = Vec::new();
        encode_snapshot_report(&r, &mut b).unwrap();
        assert_eq!(b.len(), SNAPSHOT_REPORT_HEADER_LEN + 2 * SNAPSHOT_REPORT_ENTRY_LEN);
        assert_eq!(&b[16 + 12..16 + 20], &100u64.to_le_bytes(), "size @12 of the first entry");
        assert_eq!(decode_snapshot_report(&b), Some(r.clone()));
        let mut old = b[..SNAPSHOT_REPORT_HEADER_LEN].to_vec();
        for e in b[SNAPSHOT_REPORT_HEADER_LEN..].chunks(SNAPSHOT_REPORT_ENTRY_LEN) {
            old.extend_from_slice(&e[..SNAPSHOT_REPORT_ENTRY_LEN_UNSIZED]);
        }
        assert_eq!(decode_snapshot_report(&old), None, "12-byte entries are refused on the live path");
        assert_eq!(
            decode_snapshot_report_unsized(&old),
            Some(SnapshotReport {
                hashes: vec![(0, 7, 0), (2, 7, 0)],
                ..r.clone()
            })
        );
        let mut list = Vec::new();
        encode_report_list(std::slice::from_ref(&r), &mut list).unwrap();
        assert_eq!(decode_report_list(&list), Some(vec![r.clone()]));
        let mut old_list = (old.len() as u32).to_le_bytes().to_vec();
        old_list.extend_from_slice(&old);
        assert_eq!(
            decode_report_list_unsized(&old_list).map(|l| l[0].hashes.clone()),
            Some(vec![(0, 7, 0), (2, 7, 0)])
        );
        assert_eq!(decode_report_list(&old_list), None, "the live list decoder refuses the old entry width");
    }

    /// Snapshot-lifecycle spec §7.2 + plan ruling P7: the size recorded is the
    /// one reported WITH the majority hash — the largest such, so one reporter
    /// that could not size its artifact (0) does not erase the size; none
    /// without a majority.
    #[test]
    fn majority_size_is_the_size_reported_with_the_majority_hash() {
        let r = SnapshotReport {
            row: 0,
            position: 64,
            hashes: vec![(0, 7, 100), (1, 7, 100), (2, 8, 999)],
        };
        assert_eq!(majority_size(&r, &verdict(&r)), 100);
        let one_unknown = SnapshotReport {
            hashes: vec![(0, 7, 0), (1, 7, 100)],
            ..r.clone()
        };
        assert_eq!(majority_size(&one_unknown, &verdict(&one_unknown)), 100);
        let split = SnapshotReport {
            hashes: vec![(0, 7, 100), (1, 8, 200)],
            ..r.clone()
        };
        assert_eq!(majority_size(&split, &verdict(&split)), 0, "no majority, no size");
    }
```

In `uc_node/src/node.rs` `mod tests`, beside `snapshot_reports_append_once_every_voter_has_reported`:

```rust
    /// Snapshot-lifecycle spec §7.1: the committed record carries each
    /// reporter's size beside its hash, in node-id order.
    #[test]
    fn a_collected_report_record_carries_each_reporters_size() {
        let _obs = obs_capture_lock();
        let mut h = harness_with_rows(&["a"]);
        drive_to_serving_leader(&mut h);
        h.cons.pass_mono_ns = 1_000;
        let p = 6048u64;
        h.cons.on_snap_report(2, 0, p, 0xA1, 40);
        h.cons.on_snap_report(0, 0, p, 0xA1, 40);
        h.cons.on_snap_report(1, 0, p, 0xA1, 0);
        assert!(h.cons.maybe_append_snapshot_reports());
        let end = h.cons.last_cluster_append;
        h.commit_through(end);
        assert_eq!(
            h.cons.cluster_view.to_state().report_for(0).map(|r| r.hashes.clone()),
            Some(vec![(0, 0xA1, 40), (1, 0xA1, 0), (2, 0xA1, 40)])
        );
    }
```

- [ ] **Step 2: Run, expect FAIL.**
Run: `cargo test -p uc_protocol snap_report_body_is_32 report_entries_carry_sizes majority_size`
Expected: FAIL to compile — `struct SnapReportBody has no field named size`, `cannot find function decode_snapshot_report_unsized`, `cannot find function majority_size`.

- [ ] **Step 3: Implement the datagram body.** In `datagram.rs` replace the constant, struct doc and both functions:

```rust
pub const SNAP_REPORT_BODY_LEN: usize = 32;

/// Plan B3: "row `row` froze its artifact at `position` with hash `hash`" —
/// the sending node's own id is `node_id`. Snapshot-lifecycle spec §7.1 adds
/// `size`: the artifact FILE's byte length on disk (plan ruling P6), `0` =
/// unknown. LE: row 0, reserved 1..4 (zero), node_id 4..8, position 8..16,
/// hash 16..24, size 24..32.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapReportBody {
    pub row: u8,
    pub node_id: u32,
    pub position: u64,
    pub hash: u64,
    pub size: u64,
}

pub fn write_snap_report_body(buf: &mut [u8], b: &SnapReportBody) {
    buf[0] = b.row;
    buf[1..4].fill(0);
    buf[4..8].copy_from_slice(&b.node_id.to_le_bytes());
    buf[8..16].copy_from_slice(&b.position.to_le_bytes());
    buf[16..24].copy_from_slice(&b.hash.to_le_bytes());
    buf[24..32].copy_from_slice(&b.size.to_le_bytes());
}
```

In `read_snap_report_body` keep every check and add the field to the returned struct: `size: u64::from_le_bytes(buf[24..32].try_into().unwrap()),`. Update `snap_report_kind_and_body_are_pinned`: `assert_eq!(SNAP_REPORT_BODY_LEN, 32);`, give `b` `size: 0x1112_1314_1516_1718`, and append the eight size bytes `0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11, // size` to the absolute wire pin array. Add `size: 9` to the struct literal in `snap_report_body_admits_row_255_only_beyond_the_declared_rows`.

- [ ] **Step 4: Implement the record.** In `upgrade.rs`:

```rust
/// `node_id u32 ‖ hash u64 ‖ size u64` (snapshot-lifecycle spec §7.1).
pub const SNAPSHOT_REPORT_ENTRY_LEN: usize = 20;
/// The entry before sizes existed, `node_id u32 ‖ hash u64`. Read ONLY out
/// of a v1–v4 cluster image's report blob (`decode_report_list_unsized`),
/// with every size `0` (unknown). Never on a `CLUSTER` frame.
pub const SNAPSHOT_REPORT_ENTRY_LEN_UNSIZED: usize = 12;
/// One entry per member at most — the leader collects one hash per node.
pub const MAX_SNAPSHOT_REPORT_NODES: usize = MAX_MEMBERS;
/// 16 + 8 × 20 = 176: inside the 1312 B crypto-on ceiling at the baseline rung.
pub const SNAPSHOT_REPORT_MAX_LEN: usize =
    SNAPSHOT_REPORT_HEADER_LEN + MAX_SNAPSHOT_REPORT_NODES * SNAPSHOT_REPORT_ENTRY_LEN;
```

Change the struct field doc and type:

```rust
    /// `(node_id, hash, size)`, strictly increasing by node id — the canonical
    /// order, so identical observations always encode identically. `size` is
    /// the artifact file's byte length (snapshot-lifecycle spec §7.1), `0` =
    /// unknown.
    pub hashes: Vec<(u32, u64, u64)>,
```

`ids_strictly_increasing(hashes: &[(u32, u64, u64)])` (body unchanged). In `encode_snapshot_report` the loop becomes:

```rust
    for (id, h, size) in &r.hashes {
        out.extend_from_slice(&id.to_le_bytes());
        out.extend_from_slice(&h.to_le_bytes());
        out.extend_from_slice(&size.to_le_bytes());
    }
```

Replace `decode_snapshot_report` with a shared body:

```rust
/// Exact framing: `count` must match the length, reserved must be zero,
/// and every rule `encode_snapshot_report` enforces holds on read too.
pub fn decode_snapshot_report(buf: &[u8]) -> Option<SnapshotReport> {
    decode_report_with(buf, SNAPSHOT_REPORT_ENTRY_LEN)
}

/// A report in the layout a v1–v4 cluster image stored (12-byte entries):
/// every size reads `0`. Only `ClusterFsm::install_snapshot` calls it.
pub fn decode_snapshot_report_unsized(buf: &[u8]) -> Option<SnapshotReport> {
    decode_report_with(buf, SNAPSHOT_REPORT_ENTRY_LEN_UNSIZED)
}

fn decode_report_with(buf: &[u8], entry_len: usize) -> Option<SnapshotReport> {
    if buf.len() < SNAPSHOT_REPORT_HEADER_LEN || buf[2..8] != [0; 6] {
        return None;
    }
    let row = buf[0];
    let n = buf[1] as usize;
    if !is_report_row(row)
        || n == 0
        || n > MAX_SNAPSHOT_REPORT_NODES
        || buf.len() != SNAPSHOT_REPORT_HEADER_LEN + n * entry_len
    {
        return None;
    }
    let position = u64::from_le_bytes(buf[8..16].try_into().ok()?);
    if position == 0 {
        return None;
    }
    let mut hashes = Vec::with_capacity(n);
    let mut o = SNAPSHOT_REPORT_HEADER_LEN;
    for _ in 0..n {
        let id = u32::from_le_bytes(buf[o..o + 4].try_into().ok()?);
        let h = u64::from_le_bytes(buf[o + 4..o + 12].try_into().ok()?);
        let size = if entry_len == SNAPSHOT_REPORT_ENTRY_LEN {
            u64::from_le_bytes(buf[o + 12..o + 20].try_into().ok()?)
        } else {
            0
        };
        hashes.push((id, h, size));
        o += entry_len;
    }
    if !ids_strictly_increasing(&hashes) {
        return None;
    }
    Some(SnapshotReport {
        row,
        position,
        hashes,
    })
}
```

In `verdict`, the three closures become `.map(|(_, h, _)| *h)`, `.filter(|(_, x, _)| x == h)`, `.filter(|(_, h, _)| *h != m).map(|(id, _, _)| *id)`. After `verdict` add:

```rust
/// Snapshot-lifecycle spec §7.2, plan ruling P7: the size recorded for a row
/// is the one reported WITH the majority hash — identical hashes imply
/// identical bytes. The largest such, so a reporter whose `stat` failed (`0`)
/// does not erase it; `0` when there is no majority.
pub fn majority_size(r: &SnapshotReport, v: &Verdict) -> u64 {
    let Some(m) = v.majority_hash else {
        return 0;
    };
    r.hashes
        .iter()
        .filter(|(_, h, _)| *h == m)
        .map(|(_, _, s)| *s)
        .max()
        .unwrap_or(0)
}
```

Replace `decode_report_list` with:

```rust
pub fn decode_report_list(buf: &[u8]) -> Option<Vec<SnapshotReport>> {
    decode_report_list_with(buf, decode_snapshot_report)
}

/// A v1–v4 cluster image's report blob (12-byte entries, sizes read `0`).
pub fn decode_report_list_unsized(buf: &[u8]) -> Option<Vec<SnapshotReport>> {
    decode_report_list_with(buf, decode_snapshot_report_unsized)
}

fn decode_report_list_with(
    buf: &[u8],
    one: fn(&[u8]) -> Option<SnapshotReport>,
) -> Option<Vec<SnapshotReport>> {
    let mut out = Vec::new();
    let mut o = 0;
    while o < buf.len() {
        let len = u32::from_le_bytes(buf.get(o..o + 4)?.try_into().ok()?) as usize;
        o += 4;
        let end = o.checked_add(len)?;
        out.push(one(buf.get(o..end)?)?);
        o = end;
    }
    Some(out)
}
```

Every existing test literal in `upgrade.rs` gains a third tuple element `0` (e.g. `hashes: vec![(0, 7)]` → `hashes: vec![(0, 7, 0)]`).

- [ ] **Step 5: Thread the size through `uc_net`.** In `receiver.rs` add `size: u64,` to `NetEvent::SnapReport` (after `hash`) and `size: b.size,` in the forward at ~:2095; add `size: 0,` to the test literal ~:4382.

- [ ] **Step 6: Thread the size through the node's collector.** In `node.rs`:
  - `PendingSnapshotReport` gains, after `hashes`:

```rust
    /// Snapshot-lifecycle spec §7.1: each reporter's artifact size, keyed like
    /// `hashes` (a re-sent datagram overwrites with the same value).
    sizes: BTreeMap<u32, u64>,
```

  - `on_snap_report` takes `size: u64` as its last parameter; the join arm becomes `list[i].hashes.insert(from, hash); list[i].sizes.insert(from, size); return;`; the new entry builds `let mut sizes = BTreeMap::new(); sizes.insert(from, size);` and passes `sizes,` in the `PendingSnapshotReport` literal.
  - In `maybe_append_snapshot_reports` the payload becomes:

```rust
                let hashes: Vec<(u32, u64, u64)> = pend
                    .hashes
                    .iter()
                    .filter(|(id, _)| self.sm.config().contains(**id))
                    .map(|(id, h)| (*id, *h, pend.sizes.get(id).copied().unwrap_or(0)))
                    .collect();
```

  - `deliver_snapshot_reports`: the in-process call becomes `self.on_snap_report(self.id, row, p, hash, 0);` and the datagram literal gains `size: 0,` — both marked `// Task 6 (snapshot lifecycle) passes the real size`.
  - The `NetEvent::SnapReport { from, row, position, hash, size }` arm passes `size`.
  - Every test call `h.cons.on_snap_report(a, b, c, d)` gains `, 0`; every `SnapReportBody { .. }` test literal gains `size: 0`.

- [ ] **Step 7: Fix the remaining compile errors mechanically.** `cargo build --workspace --all-targets 2>&1 | grep -E '^(error|  -->)'` lists every site: append `, 0` to each `hashes` tuple literal; change `|(id, h)|` destructures of `SnapshotReport::hashes` to `|(id, h, _)|` (in `cluster_agent.rs`, `cluster_fsm.rs`, `obs/metrics.rs`, `examples/m10_alerts.rs`, `tests/snapshot_reports.rs`, `uc_ctl/src/upgrade.rs`). The `cluster_fsm.rs` test helper `fn report(row: u8, position: u64, hashes: &[(u32, u64)])` changes its parameter to `&[(u32, u64, u64)]` (body unchanged: `hashes.to_vec()`), and each of its callers appends `, 0` to every tuple. Do not change any logic.

- [ ] **Step 8: Run, expect PASS.**
Run: `cargo test -p uc_protocol && cargo test -p uc_net --lib && cargo test -p uc_node --lib snapshot_report && cargo test -p uc_node --lib a_collected_report_record_carries`
Expected: PASS.

- [ ] **Step 9: Commit.**

```bash
cargo fmt --all
git commit -am "protocol(lifecycle): SNAP_REPORT body 32 B and report entries carry the artifact size"
```

---

### Task 2: The catalog stores sizes; cluster image v5

**Files:**
- Modify: `uc_protocol/src/v2/catalog.rs` (module doc :13–18, constants :55–63, `RowEntry` :103–108, `encode_row_entry`/`decode_row_entry` :156–177, `decode_set_entry` :190–223, `decode_set_list` :243–255, tests)
- Modify: `uc_protocol/src/v2/cluster_image.rs` (doc :19–23 and :62–66, `CLUSTER_IMAGE_VERSION`, new `cluster_image_version`)
- Modify (mechanical): `RowEntry { .. }` literals in `uc_node/src/cluster_fsm.rs` (2) and `uc_node/src/catalog.rs` (1) gain `size: 0`
- Test: both files' `mod tests`

**Interfaces:**
- Produces: `RowEntry { version: u32, hash: u64, verdict: RowVerdict, size: u64 }`; `ROW_ENTRY_LEN = 21`; `ROW_ENTRY_LEN_UNSIZED = 13`; `SET_ENTRY_LEN = 207`; `SET_ENTRY_LEN_UNSIZED = 135`; `pub fn decode_set_list_unsized(buf: &[u8]) -> Option<Vec<SetEntry>>`; `impl SetEntry { pub fn total_size(&self) -> u64 }`.
- Produces: `CLUSTER_IMAGE_VERSION = 5`; `pub fn cluster_image_version(buf: &[u8]) -> Option<u32>` (the version word of an image `decode_cluster_image` accepts).

- [ ] **Step 1: Write the failing tests.** In `catalog.rs` `mod tests`:

```rust
    /// Snapshot-lifecycle spec §7.2: a row entry is 21 B — `version u32 ‖ hash
    /// u64 ‖ verdict u8 ‖ size u64` — and a set entry 207 B.
    #[test]
    fn row_entries_carry_sizes_and_the_set_entry_is_207_bytes() {
        assert_eq!((ROW_ENTRY_LEN, SET_ENTRY_LEN), (21, 207));
        let mut e = SetEntry::commanded(4096, SetKind::Full, 77);
        e.rows[0] = RowEntry {
            version: 1,
            hash: 0xAB,
            verdict: RowVerdict::Agreed,
            size: 0x0102_0304_0506_0708,
        };
        let mut b = Vec::new();
        encode_set_list(&[e.clone()], &mut b).unwrap();
        assert_eq!(b.len(), 2 + SET_ENTRY_LEN);
        let row0 = 2 + 18;
        assert_eq!(&b[row0 + 13..row0 + 21], &0x0102_0304_0506_0708u64.to_le_bytes(), "size @13 of a row entry");
        assert_eq!(decode_set_list(&b), Some(vec![e]));
    }

    /// A v1–v4 image's catalog blob (13-byte row entries) decodes through the
    /// unsized decoder with every size 0 — "unknown", never refused.
    #[test]
    fn an_unsized_set_list_decodes_with_every_size_zero() {
        let mut e = SetEntry::commanded(4096, SetKind::Standby, 5);
        e.state = SetState::Complete;
        e.rows[0] = RowEntry { version: 1, hash: 9, verdict: RowVerdict::Agreed, size: 777 };
        e.cluster = RowEntry { version: 0, hash: 3, verdict: RowVerdict::Agreed, size: 55 };
        let mut sized = Vec::new();
        encode_set_list(&[e.clone()], &mut sized).unwrap();
        let mut old = sized[..2 + 18].to_vec();
        for r in sized[2 + 18..].chunks(ROW_ENTRY_LEN) {
            old.extend_from_slice(&r[..ROW_ENTRY_LEN_UNSIZED]);
        }
        assert_eq!(old.len(), 2 + SET_ENTRY_LEN_UNSIZED);
        assert_eq!(decode_set_list(&old), None, "the live decoder refuses the old width");
        let got = decode_set_list_unsized(&old).unwrap();
        assert_eq!(got[0].rows[0].size, 0);
        assert_eq!(got[0].cluster.size, 0);
        assert_eq!((got[0].rows[0].hash, got[0].cluster.hash), (9, 3));
        assert_eq!(got[0].total_size(), 0, "unknown");
    }

    /// Spec §7.2: a set's size is its reported rows' plus its cluster
    /// artifact's; any unknown (0) component makes the total unknown.
    #[test]
    fn total_size_sums_reported_rows_and_the_cluster_artifact() {
        let mut e = SetEntry::commanded(1, SetKind::Full, 0);
        e.state = SetState::Complete;
        e.rows[0] = RowEntry { version: 1, hash: 1, verdict: RowVerdict::Agreed, size: 100 };
        e.rows[3] = RowEntry { version: 1, hash: 1, verdict: RowVerdict::Agreed, size: 20 };
        e.cluster = RowEntry { version: 0, hash: 1, verdict: RowVerdict::Agreed, size: 3 };
        assert_eq!(e.total_size(), 123, "Unreported rows 1, 2, 4..7 contribute nothing");
        e.rows[3].size = 0;
        assert_eq!(e.total_size(), 0, "one unknown row makes the set unknown");
        e.rows[3].size = 20;
        e.cluster.verdict = RowVerdict::Unreported;
        assert_eq!(e.total_size(), 0, "no cluster artifact report: unknown");
    }
```

In `cluster_image.rs` `mod tests`:

```rust
    /// Snapshot-lifecycle spec §7.2: images are written v5; the version word
    /// of an accepted image is readable so the cluster FSM can pick the blob
    /// layout (v1–v4 report and catalog blobs are UNSIZED).
    #[test]
    fn images_are_v5_and_the_version_word_of_an_accepted_image_is_readable() {
        assert_eq!(CLUSTER_IMAGE_VERSION, 5);
        let (membership, table, settings) = genesis_parts();
        let parts = ClusterImageParts {
            applied: 300,
            table_position: 0,
            settings_position: 0,
            membership: &membership,
            table: &table,
            settings: &settings,
            pins: &[],
            reports: &[],
            running: &[],
            catalog: &[],
        };
        let mut img = Vec::new();
        encode_cluster_image(&parts, &mut img).unwrap();
        assert_eq!(cluster_image_version(&img), Some(5));
        let body_end = img.len() - 4;
        let mut v4 = img[..body_end].to_vec();
        v4[8..12].copy_from_slice(&4u32.to_le_bytes());
        let crc = crc32fast::hash(&v4);
        v4.extend_from_slice(&crc.to_le_bytes());
        assert_eq!(cluster_image_version(&v4), Some(4));
        assert_eq!(decode_cluster_image(&v4), Some(parts), "v4 frames identically");
        let mut bad = img.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert_eq!(cluster_image_version(&bad), None, "only an ACCEPTED image has a version");
    }
```

- [ ] **Step 2: Run, expect FAIL.**
Run: `cargo test -p uc_protocol catalog cluster_image`
Expected: FAIL to compile — `struct RowEntry has no field named size`, `cannot find function decode_set_list_unsized`, `cannot find function cluster_image_version`.

- [ ] **Step 3: Implement `catalog.rs`.** Constants and layout doc:

```rust
/// `version u32 ‖ hash u64 ‖ verdict u8 ‖ size u64` (snapshot-lifecycle
/// spec §7.2 appended `size`).
pub const ROW_ENTRY_LEN: usize = 4 + 8 + 1 + 8; // 21
/// The row entry a v1–v4 cluster image stored, without `size`. Read only by
/// [`decode_set_list_unsized`].
pub const ROW_ENTRY_LEN_UNSIZED: usize = 4 + 8 + 1; // 13
/// `position u64 ‖ kind u8 ‖ state u8 ‖ time_ns u64 ‖ rows[0..CNC_MAX_SERVICES]
/// ‖ cluster` — `9 * ROW_ENTRY_LEN` since `CNC_MAX_SERVICES == 8`.
pub const SET_ENTRY_LEN: usize = 8 + 1 + 1 + 8 + 9 * ROW_ENTRY_LEN; // 207
/// The set entry a v1–v4 cluster image stored.
pub const SET_ENTRY_LEN_UNSIZED: usize = 8 + 1 + 1 + 8 + 9 * ROW_ENTRY_LEN_UNSIZED; // 135

const _: () = assert!(SET_ENTRY_LEN == 207);
const _: () = assert!(SET_ENTRY_LEN_UNSIZED == 135);
```

Update the module doc's layout paragraph to the 21/207 B layout and add: "A v1–v4 cluster image stored 13 B row entries (135 B sets); [`decode_set_list_unsized`] reads those with every `size` 0." `RowEntry` gains, last:

```rust
    /// Snapshot-lifecycle spec §7.2: the artifact's byte size reported with
    /// the majority hash; `0` = unknown (a set catalogued before sizes).
    pub size: u64,
```

`encode_row_entry` appends `out.extend_from_slice(&r.size.to_le_bytes());`. Replace the row/set decoders with width-parameterised ones:

```rust
fn decode_row_entry(buf: &[u8], sized: bool) -> Option<RowEntry> {
    let version = u32::from_le_bytes(buf.get(0..4)?.try_into().ok()?);
    let hash = u64::from_le_bytes(buf.get(4..12)?.try_into().ok()?);
    let verdict = match *buf.get(12)? {
        0 => RowVerdict::Unreported,
        1 => RowVerdict::Agreed,
        2 => RowVerdict::Diverged,
        3 => RowVerdict::NoMajority,
        _ => return None,
    };
    let size = if sized {
        u64::from_le_bytes(buf.get(13..21)?.try_into().ok()?)
    } else {
        0
    };
    Some(RowEntry {
        version,
        hash,
        verdict,
        size,
    })
}

fn decode_set_entry(buf: &[u8], sized: bool) -> Option<SetEntry> {
    let (row_len, set_len) = if sized {
        (ROW_ENTRY_LEN, SET_ENTRY_LEN)
    } else {
        (ROW_ENTRY_LEN_UNSIZED, SET_ENTRY_LEN_UNSIZED)
    };
    if buf.len() != set_len {
        return None;
    }
    let position = u64::from_le_bytes(buf.get(0..8)?.try_into().ok()?);
    let kind = match *buf.get(8)? {
        0 => SetKind::Full,
        1 => SetKind::Standby,
        _ => return None,
    };
    let state = match *buf.get(9)? {
        0 => SetState::Commanded,
        1 => SetState::Complete,
        _ => return None,
    };
    let time_ns = u64::from_le_bytes(buf.get(10..18)?.try_into().ok()?);
    let mut rows = [RowEntry::default(); CNC_MAX_SERVICES];
    let mut o = 18;
    for row in &mut rows {
        *row = decode_row_entry(buf.get(o..o + row_len)?, sized)?;
        o += row_len;
    }
    let cluster = decode_row_entry(buf.get(o..o + row_len)?, sized)?;
    o += row_len;
    debug_assert_eq!(o, set_len);
    Some(SetEntry {
        position,
        kind,
        time_ns,
        state,
        rows,
        cluster,
    })
}

/// Exact framing: `count u16 ‖ count × SET_ENTRY_LEN`, nothing after. A
/// count above [`MAX_CATALOG_SETS`] is refused, and every per-entry enum byte
/// is bounds-checked.
pub fn decode_set_list(buf: &[u8]) -> Option<Vec<SetEntry>> {
    decode_set_list_with(buf, true)
}

/// A v1–v4 cluster image's catalog blob: `count u16 ‖ count ×
/// SET_ENTRY_LEN_UNSIZED`, every `size` read as `0` (unknown).
pub fn decode_set_list_unsized(buf: &[u8]) -> Option<Vec<SetEntry>> {
    decode_set_list_with(buf, false)
}

fn decode_set_list_with(buf: &[u8], sized: bool) -> Option<Vec<SetEntry>> {
    let set_len = if sized { SET_ENTRY_LEN } else { SET_ENTRY_LEN_UNSIZED };
    let count = u16::from_le_bytes(buf.get(0..2)?.try_into().ok()?) as usize;
    if count > MAX_CATALOG_SETS || buf.len() != 2 + count * set_len {
        return None;
    }
    let mut out = Vec::with_capacity(count);
    let mut o = 2;
    for _ in 0..count {
        out.push(decode_set_entry(buf.get(o..o + set_len)?, sized)?);
        o += set_len;
    }
    Some(out)
}
```

Add to `impl SetEntry`:

```rust
    /// Snapshot-lifecycle spec §7.2: the set's byte size — every REPORTED
    /// row's size plus the cluster artifact's. `0` = unknown: the cluster
    /// artifact is unreported, or any reported component's size is `0` (a set
    /// catalogued before sizes existed).
    pub fn total_size(&self) -> u64 {
        if self.cluster.verdict == RowVerdict::Unreported || self.cluster.size == 0 {
            return 0;
        }
        let mut total = self.cluster.size;
        for r in &self.rows {
            if r.verdict == RowVerdict::Unreported {
                continue;
            }
            if r.size == 0 {
                return 0;
            }
            total = total.saturating_add(r.size);
        }
        total
    }
```

In the existing tests, every `RowEntry { version, hash, verdict }` literal gains `size: 0` and `set_entry_layout_is_frozen`'s `row2` offset arithmetic already uses `ROW_ENTRY_LEN` (keep it). In `uc_node/src/cluster_fsm.rs` and `uc_node/src/catalog.rs` the three `RowEntry { .. }` literals gain `size: 0` (Task 4 replaces the FSM fold's).

- [ ] **Step 4: Implement `cluster_image.rs`.** Bump and document:

```rust
/// Bumped to 5 (snapshot-lifecycle spec §7.2): the `reports` blob's entries
/// and the `catalog` blob's row entries carry a `size u64`. The OUTER framing
/// is identical to v4 — the blobs are opaque here — so a v1–v4 image is still
/// ACCEPTED; its reader picks the blob layout from [`cluster_image_version`]
/// (`uc_node::cluster_fsm` decodes v1–v4 blobs with every size `0`).
pub const CLUSTER_IMAGE_VERSION: u32 = 5;
```

Extend the module doc after the v4 sentence: "Layout v5 (snapshot-lifecycle spec §7.2) changes no framing: it marks that the opaque `reports` and `catalog` blobs carry sizes." After `decode_cluster_image` add:

```rust
/// The version word of an image [`decode_cluster_image`] ACCEPTS, else
/// `None`. The cluster FSM reads it to choose the sized (v5) or unsized
/// (v1–v4) blob decoders; kept out of [`ClusterImageParts`] so a decoded image
/// still re-encodes to parts that compare equal (the fuzz target's property).
pub fn cluster_image_version(buf: &[u8]) -> Option<u32> {
    decode_cluster_image(buf)?;
    Some(u32::from_le_bytes(buf.get(8..12)?.try_into().ok()?))
}
```

`decode_cluster_image`'s `(1..=CLUSTER_IMAGE_VERSION).contains(&version)` accepts 5 unchanged; its `version >= 3` / `version >= 4` branches are unchanged (v5 has both blobs). Any existing test that hard-codes `4` as the written version word changes to `CLUSTER_IMAGE_VERSION`.

- [ ] **Step 5: Run, expect PASS.**
Run: `cargo test -p uc_protocol && cargo build --workspace --all-targets`
Expected: PASS; build clean.

- [ ] **Step 6: Commit.**

```bash
cargo fmt --all
git commit -am "protocol(lifecycle): catalog row entries carry sizes (21/207 B); cluster image v5"
```

---

### Task 3: `Settings` v4 — the replicated `auto_fetch` switch

**Files:**
- Modify: `uc_protocol/src/v2/settings.rs` (constants :9–24, `Settings` :63–96, `genesis_default`, `encode_settings`, `decode_settings`, the frame-fit assert, tests)
- Modify: `uc_protocol/src/v2/cluster_image.rs` (the two settings-length matches ~:232 and ~:255; the `use super::settings::{..}` line)
- Modify: `uc_node/src/cluster_fsm.rs` (`ClusterView` gains `auto_fetch: AtomicBool`; `new`, `publish`, `to_state`)
- Modify: `uc_node/src/config_file.rs` (`SettingsSection` ~:220, the `Settings` literal ~:830)
- Modify: `uc_ctl/src/settings.rs` (`SettingsFile` :50, `parse_settings` :112–123, `render_settings_line` :219–247)
- Modify (mechanical): every `Settings { .. }` literal (`grep -rn "retain_sets:" --include=*.rs .`): `uc_node/src/cluster_fsm.rs`, `uc_ctl/src/settings.rs`, `fuzz/src/seeds.rs`
- Docs: `docs/reference/configuration.md` `[settings]` table; `docs/reference/uc2ctl.md` `settings apply`/`settings show` samples
- Test: `settings.rs`, `cluster_fsm.rs`, `config_file.rs`, `uc_ctl/src/settings.rs` `mod tests`

**Interfaces:**
- Produces: `Settings.auto_fetch: bool` (genesis `true`; v1–v3 decode `true`); `SETTINGS_VERSION = 4`; `SETTINGS_LEN = 36`; `SETTINGS_LEN_V3 = 35`.
- Produces: `ClusterView.auto_fetch: AtomicBool` — Task 9 reads it once per pass.

- [ ] **Step 1: Write the failing tests.** In `settings.rs` `mod tests`:

```rust
    /// Snapshot-lifecycle spec §6: v4 appends `auto_fetch u8 @35` (0/1, any
    /// other byte refused); v1–v3 records decode with `auto_fetch = true`.
    #[test]
    fn settings_v4_round_trips_auto_fetch_and_v1_to_v3_read_true() {
        assert_eq!((SETTINGS_VERSION, SETTINGS_LEN, SETTINGS_LEN_V3), (4, 36, 35));
        assert!(Settings::genesis_default().auto_fetch, "default on");
        let s = Settings {
            auto_fetch: false,
            ..Settings::genesis_default()
        };
        let mut b = Vec::new();
        encode_settings(&s, &mut b);
        assert_eq!(b.len(), SETTINGS_LEN);
        assert_eq!(&b[0..4], &4u32.to_le_bytes());
        assert_eq!(b[35], 0, "auto_fetch @35");
        assert_eq!(decode_settings(&b), Some(s));
        let mut v3 = b[..SETTINGS_LEN_V3].to_vec();
        v3[0..4].copy_from_slice(&3u32.to_le_bytes());
        assert_eq!(decode_settings(&v3).map(|s| s.auto_fetch), Some(true));
        let mut v2 = b[..SETTINGS_LEN_V2].to_vec();
        v2[0..4].copy_from_slice(&2u32.to_le_bytes());
        assert_eq!(decode_settings(&v2).map(|s| s.auto_fetch), Some(true));
        let mut v1 = b[..SETTINGS_LEN_V1].to_vec();
        v1[0..4].copy_from_slice(&1u32.to_le_bytes());
        assert_eq!(decode_settings(&v1).map(|s| s.auto_fetch), Some(true));
        let mut bad = b.clone();
        bad[35] = 2;
        assert_eq!(decode_settings(&bad), None, "auto_fetch byte must be 0 or 1");
        let mut short = b[..SETTINGS_LEN_V3].to_vec();
        short[0..4].copy_from_slice(&4u32.to_le_bytes());
        assert_eq!(decode_settings(&short), None, "a v4 header on a v3 length");
    }
```

In `cluster_fsm.rs` `mod tests`:

```rust
    /// Snapshot-lifecycle spec §6: the view carries `auto_fetch`, and
    /// `to_state` returns it — a leader that re-proposes the committed record
    /// (the jumbo rung raise) must not silently turn the switch back on.
    #[test]
    fn the_view_publishes_auto_fetch_and_to_state_returns_it() {
        let mut st = fsm().state().clone();
        st.settings.auto_fetch = false;
        let v = ClusterView::new(&st);
        assert!(!v.auto_fetch.load(Ordering::Acquire));
        assert!(!v.to_state().settings.auto_fetch);
        st.settings.auto_fetch = true;
        v.publish(&st);
        assert!(v.to_state().settings.auto_fetch);
    }
```

In `config_file.rs` `mod tests`:

```rust
    /// Snapshot-lifecycle spec §6: `[settings] auto_fetch` seeds genesis;
    /// absent means `true`.
    #[test]
    fn settings_auto_fetch_seeds_genesis_and_defaults_to_true() {
        let (cfg, _) = load_str(&format!("{MINIMAL}\n[settings]\nauto_fetch = false\n")).unwrap();
        assert!(!cfg.settings_genesis.auto_fetch);
        let (cfg, _) = load_str(&format!("{MINIMAL}\n[settings]\n")).unwrap();
        assert!(cfg.settings_genesis.auto_fetch);
    }
```

In `uc_ctl/src/settings.rs` `mod tests`:

```rust
    /// Snapshot-lifecycle spec §6: `auto_fetch` is an operator key; absent
    /// means `true`; `settings show` prints it after `retain_sets`.
    #[test]
    fn auto_fetch_parses_defaults_to_true_and_shows() {
        assert!(!parse_settings("auto_fetch = false\n").unwrap().auto_fetch);
        assert!(parse_settings("").unwrap().auto_fetch);
        let mut s = Settings::genesis_default();
        s.auto_fetch = false;
        let line = render_settings_line(9, &s);
        assert!(line.contains("retain_sets=1 auto_fetch=false datagram_mtu="), "{line}");
    }
```

- [ ] **Step 2: Run, expect FAIL.**
Run: `cargo test -p uc_protocol settings_v4 && cargo test -p uc_ctl auto_fetch`
Expected: FAIL to compile — `no field auto_fetch on type Settings`, `cannot find value SETTINGS_LEN_V3`.

- [ ] **Step 3: Implement the record.** In `settings.rs`:

```rust
pub const SETTINGS_VERSION: u32 = 4;
/// The exact encoded length of a version-4 record — no trailing bytes.
pub const SETTINGS_LEN: usize = 4 + 8 + 8 + 8 + 1 + 4 + 2 + 1; // 36
/// The exact length of a version-3 record (`retain_sets`, no `auto_fetch`),
/// accepted on decode only.
pub const SETTINGS_LEN_V3: usize = 4 + 8 + 8 + 8 + 1 + 4 + 2; // 35
```

Extend the `SETTINGS_VERSION` doc: "`4` since the snapshot-lifecycle spec (`auto_fetch`); `1`–`3` decode with `auto_fetch = true`, the default." Add to `Settings` after `retain_sets`:

```rust
    /// Snapshot-lifecycle spec §6: every node keeps its own copy of the
    /// newest agreed snapshot set by fetching it in the background. `true`
    /// by default and for every v1–v3 record. Off, a voter on a learner-only
    /// cluster holds no set and never purges (spec §6, the documented trade).
    pub auto_fetch: bool,
```

`genesis_default` gains `auto_fetch: true,`. `encode_settings` appends `out.push(s.auto_fetch as u8);`. `decode_settings`'s match becomes:

```rust
    let (datagram_mtu, retain_sets, auto_fetch) = match (version, buf.len()) {
        (1, SETTINGS_LEN_V1) => (0, 1, true),
        (2, SETTINGS_LEN_V2) => (u32::from_le_bytes(buf[29..33].try_into().unwrap()), 1, true),
        (3, SETTINGS_LEN_V3) => (
            u32::from_le_bytes(buf[29..33].try_into().unwrap()),
            u16::from_le_bytes(buf[33..35].try_into().unwrap()),
            true,
        ),
        (4, SETTINGS_LEN) => (
            u32::from_le_bytes(buf[29..33].try_into().unwrap()),
            u16::from_le_bytes(buf[33..35].try_into().unwrap()),
            match buf[35] {
                0 => false,
                1 => true,
                _ => return None,
            },
        ),
        _ => return None,
    };
```

and the returned struct gains `auto_fetch`. Update `decode_settings`'s doc to name the four `(version, length)` pairs. Update `settings_layout_is_frozen`: `SETTINGS_VERSION == 4`, `SETTINGS_LEN == 36`, add `SETTINGS_LEN_V3 == 35`, give its literal `auto_fetch: false` and assert `out[35] == 0`. The other existing tests in this file that build a v3 record by truncating to 35 B now truncate to `SETTINGS_LEN_V3`.

- [ ] **Step 4: Cluster image settings lengths.** In `cluster_image.rs` change the import to `use super::settings::{SETTINGS_LEN, SETTINGS_LEN_V1, SETTINGS_LEN_V2, SETTINGS_LEN_V3};` and BOTH `match u32_at(o)?` arms to:

```rust
            1 => SETTINGS_LEN_V1,
            2 => SETTINGS_LEN_V2,
            3 => SETTINGS_LEN_V3,
            4 => SETTINGS_LEN,
            _ => return None,
```

Fix this file's tests that assumed `SETTINGS_LEN == 35` (the `a_v1_image_whose_33_byte_tail...` test resizes to `SETTINGS_LEN`; its doc's "it is 35 now" becomes "36 now").

- [ ] **Step 5: The view.** In `cluster_fsm.rs` `ClusterView` add after `retain_sets`:

```rust
    /// Snapshot-lifecycle spec §6: the committed `auto_fetch` switch — read
    /// by the consensus agent once per pass (one load, no lock), and by
    /// [`Self::to_state`], so a re-proposed record carries it unchanged.
    pub auto_fetch: AtomicBool,
```

`new`: `auto_fetch: AtomicBool::new(true),`; `publish`: `self.auto_fetch.store(st.settings.auto_fetch, Ordering::Release);` beside `retain_sets`; `to_state`'s `Settings` literal: `auto_fetch: self.auto_fetch.load(Ordering::Acquire),`. Every other `Settings { .. }` literal in the file gains `auto_fetch: true`.

- [ ] **Step 6: The operator keys.** `config_file.rs` `SettingsSection` gains:

```rust
    /// Snapshot-lifecycle spec §6: seeds the replicated `auto_fetch`. Absent
    /// means `true` (every node keeps its own copy of the newest agreed set).
    #[serde(default)]
    auto_fetch: Option<bool>,
```

and its `Settings` literal gains `auto_fetch: s.auto_fetch.unwrap_or(true),`. In `uc_ctl/src/settings.rs`, `SettingsFile` gains `auto_fetch: Option<bool>,`, `parse_settings`'s literal gains `auto_fetch: file.auto_fetch.unwrap_or(true),` (with a one-line comment: "absent means on — the spec's default"), and `render_settings_line`'s format becomes

```rust
    format!(
        "position={position} admission_bytes={} fsm_lag={fsm_lag} \
         snapshot_interval_bytes={} snapshot_target={target} retain_sets={} \
         auto_fetch={} datagram_mtu={} ({rung})",
        settings.admission_bytes,
        settings.snapshot_interval_bytes,
        settings.retain_sets,
        settings.auto_fetch,
        settings.datagram_mtu,
    )
```

with the field-ordering doc listing `auto_fetch` after `retain_sets` ("added before `datagram_mtu`'s suffix, as `retain_sets` was"). Fix the remaining `Settings` literals (`uc_ctl` tests, `fuzz/src/seeds.rs::uc_protocol_settings`'s `non_default`) with `auto_fetch: true` (the fuzz crate is outside the workspace: `(cd fuzz && cargo +nightly build --bin seed-corpus)` must compile).

- [ ] **Step 7: Docs.** `docs/reference/configuration.md` `### [settings]` table: add the row

```markdown
| `auto_fetch` | bool | `true` | Every node fetches the newest **agreed** snapshot set it does not hold, in the background, so it can purge below it and restart from it (snapshot lifecycle). `false`: on a learner-only cluster (`snapshot_target = "learners"`) voters then hold no set and never purge. Seeds genesis only; change it with `uc2ctl settings apply`. |
```

and extend the line-34 summary row's key list with `auto_fetch`. In `docs/reference/uc2ctl.md`, every `settings show` sample line gains ` auto_fetch=true` after `retain_sets=…`, and the `settings apply` key list names `auto_fetch` (bool, absent = `true`).

- [ ] **Step 8: Run, expect PASS.**
Run: `cargo test -p uc_protocol && cargo test -p uc_ctl && cargo test -p uc_node --lib config_file cluster_fsm`
Expected: PASS.

- [ ] **Step 9: Commit.**

```bash
cargo fmt --all
git commit -am "settings(lifecycle): Settings v4 auto_fetch, default on; [settings] auto_fetch; settings show"
```

---

### Task 4: The cluster FSM folds sizes; v1–v4 images install with size 0

**Files:**
- Modify: `uc_node/src/cluster_fsm.rs` (`fold_into_catalog` ~:270, `install_snapshot` ~:980–1060, `ClusterView` struct/`new`/`publish`)
- Test: its `mod tests`

**Interfaces:**
- Consumes: Task 1 `majority_size`, `decode_report_list_unsized`; Task 2 `cluster_image_version`, `decode_set_list_unsized`, `SetEntry::total_size`.
- Produces: `ClusterView.catalog_newest_agreed_bytes: AtomicU64` — the newest agreed set's `total_size()` (0 when unknown or none). Task 9 and Task 10 read it.

- [ ] **Step 1: Write the failing tests** (in `cluster_fsm.rs` `mod tests`; the catalog-test helpers `report(..)` — taking `&[(u32, u64, u64)]` since Task 1 — `genesis_row(..)`, `apply_at` and `fsm()` already exist there):

```rust
    /// Snapshot-lifecycle spec §7.2: the row entry records the size reported
    /// with the majority hash; the cluster row likewise.
    #[test]
    fn the_catalog_records_the_majority_hashs_size() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        f.on_snapshot_frame(4096, false, 1);
        assert_eq!(apply_at(&mut f, 4200, &report(0, 4096, &[(0, 7, 40), (1, 7, 40), (2, 8, 99)])), 0);
        assert_eq!(apply_at(&mut f, 4300, &report(CLUSTER_ROW, 4096, &[(0, 9, 300), (1, 9, 300), (2, 9, 300)])), 0);
        let e = &f.state().catalog[0];
        assert_eq!((e.rows[0].verdict, e.rows[0].size), (RowVerdict::Diverged, 40));
        assert_eq!(e.cluster.size, 300);
    }

    /// Snapshot-lifecycle spec §7.2: a v4 image (unsized report and catalog
    /// blobs) installs with every size 0 — no refusal, no wipe.
    #[test]
    fn a_v4_image_installs_with_every_size_zero() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        f.on_snapshot_frame(1000, false, 1);
        assert_eq!(apply_at(&mut f, 1100, &report(0, 1000, &[(0, 1, 40)])), 0);
        assert_eq!(apply_at(&mut f, 1110, &report(CLUSTER_ROW, 1000, &[(0, 2, 300)])), 0);
        f.set_consumed(1200);
        let (img, _) = f.freeze().unwrap();
        let v4 = rewrite_image_as_v4(&img);
        let mut g = fsm();
        assert_eq!(g.install_snapshot(1200, &mut &v4[..]).unwrap(), 1200);
        let e = &g.state().catalog[0];
        assert_eq!((e.rows[0].hash, e.rows[0].size), (1, 0));
        assert_eq!((e.cluster.hash, e.cluster.size), (2, 0));
        assert_eq!(g.state().report_for(0).map(|r| r.hashes.clone()), Some(vec![(0, 1, 0)]));
        assert!(e.is_agreed(), "agreement survives the migration; only sizes are unknown");
    }

    /// Review focus 4: the gauge word is 0 while the newest agreed set's size
    /// is unknown — a pre-lifecycle set never reads as "too big".
    #[test]
    fn the_newest_agreed_bytes_word_is_zero_when_any_size_is_unknown() {
        let mut f = fsm();
        genesis_row(&mut f, 0, 100);
        f.on_snapshot_frame(1000, false, 1);
        assert_eq!(apply_at(&mut f, 1100, &report(0, 1000, &[(0, 1, 40)])), 0);
        assert_eq!(apply_at(&mut f, 1110, &report(CLUSTER_ROW, 1000, &[(0, 2, 0)])), 0);
        let v = ClusterView::new(f.state());
        assert_eq!(v.catalog_newest_agreed_bytes.load(Ordering::Acquire), 0);
        f.on_snapshot_frame(2000, false, 2);
        assert_eq!(apply_at(&mut f, 2100, &report(0, 2000, &[(0, 1, 40)])), 0);
        assert_eq!(apply_at(&mut f, 2110, &report(CLUSTER_ROW, 2000, &[(0, 2, 300)])), 0);
        v.publish(f.state());
        assert_eq!(v.catalog_newest_agreed_bytes.load(Ordering::Acquire), 340);
    }

    /// Re-frame a v5 image as v4: the report and catalog blobs back to their
    /// unsized widths, the version word to 4, the CRC recomputed.
    fn rewrite_image_as_v4(img: &[u8]) -> Vec<u8> {
        use uc_protocol::v2::catalog::{ROW_ENTRY_LEN, ROW_ENTRY_LEN_UNSIZED};
        use uc_protocol::v2::cluster_image::{decode_cluster_image, encode_cluster_image};
        use uc_protocol::v2::upgrade::{
            SNAPSHOT_REPORT_ENTRY_LEN, SNAPSHOT_REPORT_ENTRY_LEN_UNSIZED,
            SNAPSHOT_REPORT_HEADER_LEN,
        };
        let parts = decode_cluster_image(img).unwrap();
        let mut reports = Vec::new();
        let mut o = 0;
        while o < parts.reports.len() {
            let len = u32::from_le_bytes(parts.reports[o..o + 4].try_into().unwrap()) as usize;
            let rec = &parts.reports[o + 4..o + 4 + len];
            let mut old = rec[..SNAPSHOT_REPORT_HEADER_LEN].to_vec();
            for e in rec[SNAPSHOT_REPORT_HEADER_LEN..].chunks(SNAPSHOT_REPORT_ENTRY_LEN) {
                old.extend_from_slice(&e[..SNAPSHOT_REPORT_ENTRY_LEN_UNSIZED]);
            }
            reports.extend_from_slice(&(old.len() as u32).to_le_bytes());
            reports.extend_from_slice(&old);
            o += 4 + len;
        }
        let mut catalog = parts.catalog[..2].to_vec();
        for set in parts.catalog[2..].chunks(18 + 9 * ROW_ENTRY_LEN) {
            catalog.extend_from_slice(&set[..18]);
            for r in set[18..].chunks(ROW_ENTRY_LEN) {
                catalog.extend_from_slice(&r[..ROW_ENTRY_LEN_UNSIZED]);
            }
        }
        let mut out = Vec::new();
        encode_cluster_image(
            &uc_protocol::v2::cluster_image::ClusterImageParts {
                reports: &reports,
                catalog: &catalog,
                ..parts
            },
            &mut out,
        )
        .unwrap();
        let body_end = out.len() - 4;
        out.truncate(body_end);
        out[8..12].copy_from_slice(&4u32.to_le_bytes());
        let crc = crc32fast::hash(&out);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }
```

- [ ] **Step 2: Run, expect FAIL.**
Run: `cargo test -p uc_node --lib cluster_fsm::tests::the_catalog_records cluster_fsm::tests::a_v4_image cluster_fsm::tests::the_newest_agreed_bytes`
Expected: FAIL — compile error `no field catalog_newest_agreed_bytes`; once added, `the_catalog_records_the_majority_hashs_size` fails `(Diverged, 0) != (Diverged, 40)` and `a_v4_image_installs…` fails with `SnapshotError::Codec("cluster image reports")`.

- [ ] **Step 3: Implement the fold.** In `fold_into_catalog` the `RowEntry` literal becomes:

```rust
        let entry = RowEntry {
            version: self.version_at(r.row, r.position),
            hash: v.majority_hash.unwrap_or(0),
            verdict,
            // Snapshot-lifecycle spec §7.2 / plan ruling P7.
            size: uc_protocol::v2::upgrade::majority_size(r, &v),
        };
```

- [ ] **Step 4: Implement the install.** In `install_snapshot`, right after `let parts = decode_cluster_image(&img)...`:

```rust
        // Snapshot-lifecycle spec §7.2: a v1–v4 image stored the report and
        // catalog blobs UNSIZED; they decode with every size 0 (unknown).
        let sized = uc_protocol::v2::cluster_image::cluster_image_version(&img)
            .ok_or_else(|| bad("cluster image"))?
            >= 5;
```

then

```rust
        let reports = if sized {
            decode_report_list(parts.reports)
        } else {
            uc_protocol::v2::upgrade::decode_report_list_unsized(parts.reports)
        }
        .ok_or_else(|| bad("cluster image reports"))?;
```

and

```rust
        let catalog = if parts.catalog.is_empty() {
            Vec::new()
        } else if sized {
            decode_set_list(parts.catalog).ok_or_else(|| bad("cluster image: catalog"))?
        } else {
            uc_protocol::v2::catalog::decode_set_list_unsized(parts.catalog)
                .ok_or_else(|| bad("cluster image: catalog"))?
        };
```

(If the variable holding the image bytes is not named `img`, use the name passed to `decode_cluster_image`.)

- [ ] **Step 5: The view word.** In `ClusterView` add after `catalog_diverged`:

```rust
    /// Snapshot-lifecycle spec §7.4: the newest AGREED set's total size
    /// ([`SetEntry::total_size`]); `0` when unknown or none —
    /// `uc2_snapshot_newest_agreed_bytes`, and the auto-fetch space check's
    /// input.
    pub catalog_newest_agreed_bytes: AtomicU64,
```

`new`: `catalog_newest_agreed_bytes: AtomicU64::new(0),`. In `publish`, before `catalog_version`:

```rust
        self.catalog_newest_agreed_bytes.store(
            st.catalog
                .iter()
                .rev()
                .find(|e| e.is_agreed())
                .map_or(0, SetEntry::total_size),
            Ordering::Release,
        );
```

- [ ] **Step 6: Run, expect PASS.**
Run: `cargo test -p uc_node --lib cluster_fsm`
Expected: PASS.

- [ ] **Step 7: Commit.**

```bash
cargo fmt --all
git commit -am "cluster_fsm(lifecycle): the catalog records the majority size; v1-v4 images install with size 0"
```

---

### Task 5: The start-set words on the cnc slot

**Files:**
- Modify: `uc_protocol/src/v2/cnc.rs` (slot table comment :340–358; new constants after `CNC_SVC_OFF_ARTIFACT_HASH`; tests)
- Modify: `uc_log/src/cnc.rs` (new `SnapshotPosLine`; `ServiceSlot.snapshot_pos` type; asserts beside :520; test `cnc_offsets_match_protocol_constants` ~:1414 and a new test)
- Docs: `docs/reference/cnc-page.md` (the service-slot table: two rows)
- Test: both files

**Interfaces:**
- Produces (`uc_protocol::v2::cnc`): `CNC_SVC_OFF_START_SET_POS: usize = 264`, `CNC_SVC_OFF_START_SET_VERSION: usize = 272`.
- Produces (`uc_log::cnc`): `pub struct SnapshotPosLine` with `load_acquire(&self) -> u64` / `store_release(&self, u64)` (the existing `snapshot_pos` word — every current call site compiles unchanged), `store_start_set(&self, pos: u64, version: u32)`, `start_set(&self) -> Option<(u64, u32)>`.

- [ ] **Step 1: Write the failing tests.** In `uc_protocol/src/v2/cnc.rs` `mod tests`:

```rust
    /// Snapshot-lifecycle spec §4.2 (cnc 3.4, folded in): the start-set pair
    /// sits on the `snapshot_pos` line, right after it.
    #[test]
    fn the_start_set_words_sit_on_the_snapshot_pos_line() {
        assert_eq!(CNC_SVC_OFF_START_SET_POS, 264);
        assert_eq!(CNC_SVC_OFF_START_SET_VERSION, 272);
        assert_eq!(CNC_SVC_OFF_START_SET_POS, CNC_SVC_OFF_SNAPSHOT_POS + 8);
        const { assert!(CNC_SVC_OFF_START_SET_VERSION + 8 <= CNC_SVC_OFF_HEARTBEAT_NS) };
        assert_eq!(CNC_V2_VERSION, (3 << 24) | (4 << 16), "still cnc 3.4");
    }
```

In `uc_log/src/cnc.rs` `mod tests`:

```rust
    /// Snapshot-lifecycle spec §4.2: the pair round-trips, `0` reads as "no
    /// start set", the raw bytes sit at slot +264/+272, and `snapshot_pos`
    /// is untouched by it.
    #[test]
    fn the_start_set_pair_round_trips_at_its_pinned_offsets() {
        let page = CncPage::heap(&test_meta());
        let slot = page.service_slot(3);
        assert_eq!(slot.snapshot_pos.start_set(), None);
        slot.snapshot_pos.store_release(4000);
        slot.snapshot_pos.store_start_set(4096, 0x0102_0003);
        assert_eq!(slot.snapshot_pos.start_set(), Some((4096, 0x0102_0003)));
        assert_eq!(slot.snapshot_pos.load_acquire(), 4000, "snapshot_pos is its own word");
        let raw = page.page();
        let base = cnc::CNC_OFF_SERVICE_SLOTS + 3 * cnc::CNC_SERVICE_SLOT_STRIDE;
        let word = |o: usize| u64::from_le_bytes(raw[base + o..base + o + 8].try_into().unwrap());
        assert_eq!(word(cnc::CNC_SVC_OFF_START_SET_POS), 4096);
        assert_eq!(word(cnc::CNC_SVC_OFF_START_SET_VERSION), 0x0102_0003);
        slot.snapshot_pos.store_start_set(0, 0);
        assert_eq!(slot.snapshot_pos.start_set(), None, "0 = none");
        assert_eq!(page.service_slot(2).snapshot_pos.start_set(), None, "slots are independent");
    }
```

and in `cnc_offsets_match_protocol_constants`, after the `snapshot_pos` assertion:

```rust
        assert_eq!(
            s0.snapshot_pos.start_set_pos_ptr() - s0_base,
            cnc::CNC_SVC_OFF_START_SET_POS
        );
        assert_eq!(
            s0.snapshot_pos.start_set_version_ptr() - s0_base,
            cnc::CNC_SVC_OFF_START_SET_VERSION
        );
```

- [ ] **Step 2: Run, expect FAIL.**
Run: `cargo test -p uc_protocol start_set_words && cargo test -p uc_log start_set`
Expected: FAIL to compile — `cannot find value CNC_SVC_OFF_START_SET_POS`, `no method named start_set`.

- [ ] **Step 3: Protocol constants.** In `uc_protocol/src/v2/cnc.rs`, add to the slot table comment after the `+256 snapshot_pos` line:

```text
//   +264 start_set_pos   u64 position (0 = none)              writer: node (consensus agent)
//   +272 start_set_version u64 (low 32 = packed version that built the artifact)  writer: node (consensus agent)
```

and after `CNC_SVC_OFF_ARTIFACT_HASH`:

```rust
/// Snapshot-lifecycle spec §4.2 (cnc 3.4, folded in before release): the
/// row's START SET — the newest agreed snapshot set this node holds at or
/// below `min(commit, durable)`; `0` = none. Node-written (consensus agent,
/// on change only), read by the service at attach and in overrun recovery.
/// Shares the `snapshot_pos` line: both writers are rare (once per instant /
/// per agreement) and the per-frame apply path never reads the line.
/// Published as a pair with [`CNC_SVC_OFF_START_SET_VERSION`] through
/// `uc_log::cnc::SnapshotPosLine::{store_start_set, start_set}`.
pub const CNC_SVC_OFF_START_SET_POS: usize = 264;
/// Low 32 bits: the packed version (`identity::pack_version`) that built the
/// row's artifact at `start_set_pos` — the catalog's `RowEntry.version`.
pub const CNC_SVC_OFF_START_SET_VERSION: usize = 272;
const _: () = assert!(CNC_SVC_OFF_START_SET_POS == CNC_SVC_OFF_SNAPSHOT_POS + 8);
const _: () = assert!(CNC_SVC_OFF_START_SET_VERSION == CNC_SVC_OFF_START_SET_POS + 8);
const _: () = assert!(CNC_SVC_OFF_START_SET_VERSION + 8 <= CNC_SVC_OFF_HEARTBEAT_NS);
```

- [ ] **Step 4: The line type.** In `uc_log/src/cnc.rs` (add `fence` to the `std::sync::atomic` import), before `ServiceSlot`:

```rust
/// The slot's `snapshot_pos` line (`+256..+320`): the service builder's
/// `snapshot_pos` word, then (snapshot-lifecycle spec §4.2) the node's
/// start-set pair. Two writers on one line, each owning its own words, both
/// rare. `load_acquire`/`store_release` keep their `PaddedAtomicU64` meaning
/// for `snapshot_pos`, so every existing call site is unchanged.
#[repr(C, align(64))]
pub struct SnapshotPosLine {
    snapshot_pos: AtomicU64,
    start_set_pos: AtomicU64,
    start_set_version: AtomicU64,
    _pad: [u8; 40],
}

impl SnapshotPosLine {
    #[inline]
    pub fn load_acquire(&self) -> u64 {
        self.snapshot_pos.load(Ordering::Acquire)
    }
    #[inline]
    pub fn store_release(&self, v: u64) {
        self.snapshot_pos.store(v, Ordering::Release)
    }
    /// Node-side (consensus agent): publish row's start set. `pos` is zeroed
    /// FIRST, then the version, then `pos` with Release — the
    /// `ClusterArtifactHash::publish` pattern — so a reader that sees the same
    /// non-zero `pos` on both sides of its version load read a version stored
    /// for that `pos`. `pos == 0` publishes "none".
    pub fn store_start_set(&self, pos: u64, version: u32) {
        self.start_set_pos.store(0, Ordering::Relaxed);
        fence(Ordering::Release);
        self.start_set_version
            .store(u64::from(version), Ordering::Relaxed);
        self.start_set_pos.store(pos, Ordering::Release);
    }
    /// Service-side: the start set `(pos, version)`, or `None` for none — or
    /// for a pair that would not read coherently in four tries (a publish in
    /// flight each time). `None` is always safe: the row replays (spec §5
    /// rule 3).
    pub fn start_set(&self) -> Option<(u64, u32)> {
        for _ in 0..4 {
            let p = self.start_set_pos.load(Ordering::Acquire);
            if p == 0 {
                return None;
            }
            let v = self.start_set_version.load(Ordering::Relaxed) as u32;
            fence(Ordering::Acquire);
            if self.start_set_pos.load(Ordering::Relaxed) == p {
                return Some((p, v));
            }
        }
        None
    }
    /// Offset pins for `cnc_offsets_match_protocol_constants`.
    #[doc(hidden)]
    pub fn start_set_pos_ptr(&self) -> usize {
        &self.start_set_pos as *const _ as usize
    }
    #[doc(hidden)]
    pub fn start_set_version_ptr(&self) -> usize {
        &self.start_set_version as *const _ as usize
    }
}
const _: () = assert!(std::mem::size_of::<SnapshotPosLine>() == 64);
const _: () = assert!(
    std::mem::offset_of!(SnapshotPosLine, start_set_pos)
        == cnc::CNC_SVC_OFF_START_SET_POS - cnc::CNC_SVC_OFF_SNAPSHOT_POS
);
const _: () = assert!(
    std::mem::offset_of!(SnapshotPosLine, start_set_version)
        == cnc::CNC_SVC_OFF_START_SET_VERSION - cnc::CNC_SVC_OFF_SNAPSHOT_POS
);
```

and change `pub snapshot_pos: PaddedAtomicU64,` in `ServiceSlot` to `pub snapshot_pos: SnapshotPosLine,`. The existing `offset_of!(ServiceSlot, snapshot_pos)` and size-512 asserts stay. Add `pub use` of `SnapshotPosLine` wherever `ServiceSlot` is re-exported (check `uc_log/src/lib.rs`).

- [ ] **Step 5: Docs.** `docs/reference/cnc-page.md` service-slot table: after `+256 snapshot_pos`, add `+264 start_set_pos` (node, consensus agent; "newest agreed set this node holds at or below `min(commit, durable)`, 0 = none") and `+272 start_set_version` ("low 32: packed version that built it"), and one sentence: "read by the service at attach and in overrun recovery (snapshot lifecycle); written version-then-position, read position-version-position".

- [ ] **Step 6: Run, expect PASS.**
Run: `cargo test -p uc_protocol cnc && cargo test -p uc_log && cargo build --workspace --all-targets`
Expected: PASS; every existing `snapshot_pos.load_acquire()`/`.store_release()` call site compiles unchanged.

- [ ] **Step 7: Commit.**

```bash
cargo fmt --all
git commit -am "cnc(lifecycle): start_set_pos/start_set_version on the snapshot_pos line (+264/+272)"
```

---
### Task 6: The node reports real sizes

**Files:**
- Modify: `uc_node/src/cluster_agent.rs` (`ClusterArtifactHash` :238–262; its three `publish` call sites :353, :754, :1065; test `the_artifact_hash_word_answers_only_for_its_position` :1095)
- Modify: `uc_node/src/node.rs` (`ReportedSet` :3239; `send_snapshot_reports` :6771; `cache_report_set` :6832; `cache_report_set_quiet` :6890; `deliver_snapshot_reports` :6925; `ReportSeeder::seed` :12030–12045; a new free fn `file_size` beside `hash_file_from` :12161)
- Test: `cluster_agent.rs` and `node.rs` `mod tests`

**Interfaces:**
- Consumes: Task 1 (`on_snap_report(.., size)`, `SnapReportBody.size`).
- Produces: `ClusterArtifactHash::publish(&self, pos: u64, hash: u64, size: u64)`; `ClusterArtifactHash::hash_and_size_at(&self, p: u64) -> Option<(u64, u64)>` (`hash_at` kept, now `self.hash_and_size_at(p).map(|(h, _)| h)`); report tuples everywhere in the node are `(row: u8, hash: u64, size: u64)`.

- [ ] **Step 1: Write the failing tests.** In `cluster_agent.rs` `mod tests`:

```rust
    /// Snapshot-lifecycle spec §7.1 / plan ruling P6: the cluster agent
    /// publishes the artifact's byte size beside its hash, under the same
    /// position word.
    #[test]
    fn the_artifact_word_carries_the_size_for_its_position_only() {
        let w = ClusterArtifactHash::default();
        assert_eq!(w.hash_and_size_at(100), None);
        w.publish(100, 0xAA, 4321);
        assert_eq!(w.hash_and_size_at(100), Some((0xAA, 4321)));
        assert_eq!(w.hash_at(100), Some(0xAA));
        w.publish(200, 0xBB, 99);
        assert_eq!(w.hash_and_size_at(100), None, "superseded");
        assert_eq!(w.hash_and_size_at(200), Some((0xBB, 99)));
    }
```

In `node.rs` `mod tests`, beside `the_completeness_report_includes_the_cluster_artifact_as_row_255`:

```rust
    /// Snapshot-lifecycle spec §7.1, plan ruling P6: the completion edge
    /// reports each artifact's FILE length — a row's envelope included, the
    /// cluster image whole — and the collected record carries them.
    #[test]
    fn completion_edge_reports_carry_artifact_file_sizes() {
        use uc_protocol::v2::upgrade::CLUSTER_ROW;
        let mut h = harness_with_rows(&["a"]);
        drive_to_serving_leader(&mut h);
        let p = 6048u64;
        let membership = h.cons.cluster_view.membership();
        install_cluster_artifact_for_test(&mut h, p, &membership);
        assert_eq!(h.cluster.take_snapshot().unwrap(), p);
        write_row_artifact(&h, 0, p, &[7u8; 57]);
        h.row_published_at(0, p, uc_service::snapshots::artifact_hash_of(&[7u8; 57]));
        h.cons.check_set_completeness();
        let cluster_len = std::fs::metadata(
            h.cons.cluster_snapshot_dir.join(format!("snap-{p}.ultcluster")),
        )
        .unwrap()
        .len();
        assert_eq!(h.cons.pending_reports_for(0)[0].sizes[&h.cons.id], 57);
        assert_eq!(h.cons.pending_reports_for(CLUSTER_ROW)[0].sizes[&h.cons.id], cluster_len);
        assert!(cluster_len > 0);
    }
```

- [ ] **Step 2: Run, expect FAIL.**
Run: `cargo test -p uc_node --lib the_artifact_word_carries_the_size completion_edge_reports_carry`
Expected: FAIL to compile — `no method named hash_and_size_at`; once it exists, the node test fails `0 != 57` (Task 1 left `size: 0`).

- [ ] **Step 3: The cluster artifact's size.** Replace `ClusterArtifactHash`:

```rust
pub struct ClusterArtifactHash {
    pos: AtomicU64,
    hash: AtomicU64,
    /// Snapshot-lifecycle spec §7.1 (plan ruling P6): the artifact's byte
    /// length — the image the agent wrote, whole. Same publish discipline as
    /// `hash`.
    size: AtomicU64,
}

impl ClusterArtifactHash {
    /// Publish `hash` and `size` as the artifact at `pos`'s.
    pub fn publish(&self, pos: u64, hash: u64, size: u64) {
        self.pos.store(0, Ordering::Relaxed);
        fence(Ordering::Release);
        self.hash.store(hash, Ordering::Relaxed);
        self.size.store(size, Ordering::Relaxed);
        self.pos.store(pos, Ordering::Release);
    }

    /// The `(hash, size)` of the artifact at `p`, or `None` when the published
    /// word is for another position (or a publish is in flight).
    pub fn hash_and_size_at(&self, p: u64) -> Option<(u64, u64)> {
        if p == 0 || self.pos.load(Ordering::Acquire) != p {
            return None;
        }
        let h = self.hash.load(Ordering::Relaxed);
        let s = self.size.load(Ordering::Relaxed);
        fence(Ordering::Acquire);
        (self.pos.load(Ordering::Relaxed) == p).then_some((h, s))
    }

    /// The hash of the artifact at `p` (see [`Self::hash_and_size_at`]).
    pub fn hash_at(&self, p: u64) -> Option<u64> {
        self.hash_and_size_at(p).map(|(h, _)| h)
    }
}
```

(keep its `#[derive(Default)]`). The three call sites pass `img.len() as u64` as the third argument; the existing test's `publish(100, 0xAA)` calls gain `, 0`.

- [ ] **Step 4: Sizes on the report path.** In `node.rs`:
  - Beside `hash_file_from`:

```rust
/// Snapshot-lifecycle spec §7.1 (plan ruling P6): an artifact's byte length
/// on disk, `0` (unknown) when it cannot be read. One `stat`.
fn file_size(path: &Path) -> u64 {
    std::fs::metadata(path).map_or(0, |m| m.len())
}
```

  - `ReportedSet.reports: [(u8, u64, u64); CNC_MAX_SERVICES + 1]` (doc: "`(row, hash, size)`"); `cache_report_set`, `cache_report_set_quiet` and `deliver_snapshot_reports` take `reports: &[(u8, u64, u64)]`; `cache_report_set_quiet`'s zero array becomes `[(0u8, 0u64, 0u64); CNC_MAX_SERVICES + 1]`.
  - In `send_snapshot_reports`, the row reports and the cluster row:

```rust
        let cluster = self.cluster_artifact_hash.hash_and_size_at(p);
        let snap_root = self.snap_root.clone();
        let row_reports = rows[..n].iter().filter_map(|&row| {
            let (at, hash, still_at) = {
                let slot = self.cnc.service_slot(row as usize);
                let at = slot.snapshot_pos.load_acquire();
                let hash = slot.identity.artifact_hash();
                (at, hash, slot.snapshot_pos.load_acquire())
            };
            // Snapshot-lifecycle spec §7.1: one `stat` per row, on this
            // once-per-instant edge only (never a pass).
            (at == p && still_at == p && hash != 0).then(|| {
                let path = snap_root
                    .join(row.to_string())
                    .join(format!("{SNAP_PREFIX}{p}{SNAP_SUFFIX}"));
                (row, hash, file_size(&path))
            })
        });
        let mut reports = [(0u8, 0u64, 0u64); CNC_MAX_SERVICES + 1];
        let mut m = 0usize;
        for r in row_reports.chain(cluster.map(|(h, s)| (CLUSTER_ROW, h, s))) {
            reports[m] = r;
            m += 1;
        }
```

  (keep every existing comment in the function; the two `let (at, hash, still_at)` comments move with the block).
  - `deliver_snapshot_reports`: `for &(row, hash, size) in reports`; the in-process call `self.on_snap_report(self.id, row, p, hash, size);`; the body literal `size,` — delete Task 1's two "Task 6" comments.
  - `ReportSeeder::seed`: the row loop becomes

```rust
            for (&row, path) in self.rows.iter().zip(&row_paths) {
                if let Some(h) = hash_row_artifact(path, p) {
                    set.reports[set.n] = (row, h, file_size(path));
                    set.n += 1;
                }
            }
```

  and the cluster arm `set.reports[set.n] = (CLUSTER_ROW, h, file_size(&cluster));`; its `ReportedSet` literal's array is `[(0u8, 0u64, 0u64); CNC_MAX_SERVICES + 1]`.
  - Fix the remaining `(u8, u64)` report-tuple test literals (`cargo build -p uc_node --all-targets` lists them) by appending a size.

- [ ] **Step 5: Run, expect PASS.**
Run: `cargo test -p uc_node --lib cluster_agent snapshot_report reoffer seed completion_edge`
Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
cargo fmt --all
git commit -am "node(lifecycle): reports carry artifact file sizes (row stat on the edge, cluster size from the agent)"
```

---

### Task 7: The start-set publisher

**Files:**
- Modify: `uc_node/src/catalog.rs` (new `StartSet`, `start_set_for`, tests)
- Modify: `uc_node/src/node.rs` (`Consensus` fields beside `holdings_held` :4127 and in BOTH struct literals — `Node::start`'s ~:2514 and the test harness's ~:13726; `do_work` after `check_set_completeness` :4172; dirty marks in `note_catalog_for_holdings` :7667, `note_set_held` :7698, the pruner's `holdings_held.retain` :7800; new `maybe_publish_start_sets`/`publish_start_sets`)
- Test: `catalog.rs` and `node.rs` `mod tests`

**Interfaces:**
- Consumes: Task 5 `SnapshotPosLine::store_start_set`.
- Produces: `pub struct StartSet { pub position: u64, pub version: u32 }` (`Default` = none); `pub fn start_set_for(row: u8, sets: &[SetEntry], held: &[u64], frontier: u64) -> (StartSet, u64)` — the second value is the lowest position of an otherwise-eligible set ABOVE `frontier` (`u64::MAX` = none), the point at which the answer can next change.
- Produces (private): `Consensus::maybe_publish_start_sets(&mut self)`, `start_sets_dirty: bool`, `start_set_recomputes: u64` (test-visible).

- [ ] **Step 1: Write the failing tests.** In `uc_node/src/catalog.rs` `mod tests` (the `agreed(p)`/`diverged(p)` helpers there set `rows[0]` and `cluster` Agreed):

```rust
    fn agreed_v(p: u64, version: u32) -> SetEntry {
        let mut e = agreed(p);
        e.rows[0].version = version;
        e
    }

    /// Snapshot-lifecycle spec §4.1: the newest entry that is agreed, whose
    /// row is Agreed, that this node holds, at or below min(commit, durable).
    #[test]
    fn start_set_eligibility_table() {
        let sets = [agreed_v(1000, 7), agreed_v(2000, 8), agreed_v(3000, 9)];
        // agreed and held
        assert_eq!(
            start_set_for(0, &sets, &[1000, 2000, 3000], u64::MAX),
            (StartSet { position: 3000, version: 9 }, u64::MAX)
        );
        // not held → the newest held one
        assert_eq!(start_set_for(0, &sets, &[1000], u64::MAX).0.position, 1000);
        // above min(commit, durable) → the older one, and the frontier to watch
        assert_eq!(
            start_set_for(0, &sets, &[1000, 2000, 3000], 2500),
            (StartSet { position: 2000, version: 8 }, 3000)
        );
        // row unreported in the set → not eligible for that row
        assert_eq!(start_set_for(1, &sets, &[1000, 2000, 3000], u64::MAX).0, StartSet::default());
        // a diverged set is skipped
        let with_div = [agreed_v(1000, 7), diverged(2000)];
        assert_eq!(start_set_for(0, &with_div, &[1000, 2000], u64::MAX).0.position, 1000);
        // a Commanded entry is skipped
        let cmd = [agreed_v(1000, 7), SetEntry::commanded(2000, SetKind::Full, 0)];
        assert_eq!(start_set_for(0, &cmd, &[1000, 2000], u64::MAX).0.position, 1000);
        // empty catalog → none
        assert_eq!(start_set_for(0, &[], &[1000], u64::MAX), (StartSet::default(), u64::MAX));
    }

    /// Review focus 1: a set still being fetched is not in the held list
    /// until its LAST artifact is renamed into place (the completion edge),
    /// so the publisher names the older held set — never a half-written one.
    #[test]
    fn start_set_skips_a_set_that_is_still_being_fetched() {
        let sets = [agreed_v(1000, 7), agreed_v(2000, 7)];
        let held_while_fetching_2000 = [1000];
        assert_eq!(start_set_for(0, &sets, &held_while_fetching_2000, u64::MAX).0.position, 1000);
    }
```

In `node.rs` `mod tests`:

```rust
    /// Snapshot-lifecycle spec §4.2–4.3: the consensus agent publishes the
    /// row's start set on its cnc slot, moves it when the frontier passes a
    /// newer held agreed set, and costs no recompute on a steady pass.
    #[test]
    fn the_start_set_is_published_and_recomputed_only_on_a_change() {
        let mut h = harness_with_rows(&["a"]);
        let mut st = h.cons.cluster_view.to_state();
        let mut a = agreed_entry(1000);
        a.rows[0].version = 7;
        let mut b = agreed_entry(2000);
        b.rows[0].version = 7;
        st.catalog = vec![a, b];
        // `refresh_from_view` acts only when the view's position moves.
        st.applied += 1;
        h.cons.cluster_view.publish(&st);
        h.cons.refresh_from_view();
        h.cons.note_set_held(1000);
        h.cons.note_set_held(2000);
        let cnc = Arc::clone(&h.cons.cnc);
        let c = cnc.counters();
        c.durable.store_release(1500);
        c.commit.store_release(1500);
        h.cons.maybe_publish_start_sets();
        let slot = cnc.service_slot(0);
        assert_eq!(slot.snapshot_pos.start_set(), Some((1000, 7)));
        let n = h.cons.start_set_recomputes;
        for _ in 0..1000 {
            h.cons.maybe_publish_start_sets();
        }
        assert_eq!(h.cons.start_set_recomputes, n, "a steady pass recomputes nothing");
        c.durable.store_release(2500);
        c.commit.store_release(2500);
        h.cons.maybe_publish_start_sets();
        assert_eq!(slot.snapshot_pos.start_set(), Some((2000, 7)), "the frontier passed 2000");
        assert_eq!(h.cons.start_set_recomputes, n + 1);
        st.catalog.clear();
        st.applied += 1;
        h.cons.cluster_view.publish(&st);
        h.cons.refresh_from_view();
        h.cons.maybe_publish_start_sets();
        assert_eq!(slot.snapshot_pos.start_set(), None, "an empty catalog publishes none");
    }
```

(If `PaddedAtomicU64` has no `store_release` on the counters in a test build, use the harness's existing counter setter — `grep -n "durable.store_release\|fn set_durable" uc_node/src/node.rs` — the effective-floor tests already move `durable`.)

- [ ] **Step 2: Run, expect FAIL.**
Run: `cargo test -p uc_node --lib start_set`
Expected: FAIL to compile — `cannot find function start_set_for`, `no method maybe_publish_start_sets`.

- [ ] **Step 3: The pure rule.** In `uc_node/src/catalog.rs`:

```rust
/// Snapshot-lifecycle spec §4: one row's start set — `position == 0` = none.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StartSet {
    pub position: u64,
    /// The packed version that built the row's artifact (`RowEntry.version`).
    pub version: u32,
}

/// Snapshot-lifecycle spec §4.1: row `row`'s start set — the NEWEST entry
/// that (1) is agreed, (2) has `rows[row]` Agreed (a row the set did not
/// report is not eligible for it), (3) this node holds complete on disk
/// (`held`, the consensus agent's `holdings_held`), and (4) sits at or below
/// `frontier = min(commit, durable)`. An empty catalog answers none.
///
/// The second value is the lowest position of a set that passes (1)–(3)
/// but sits ABOVE `frontier` — when the frontier reaches it the answer
/// changes, so the caller recomputes then (spec §4.3); `u64::MAX` = none.
pub fn start_set_for(row: u8, sets: &[SetEntry], held: &[u64], frontier: u64) -> (StartSet, u64) {
    let mut wait_above = u64::MAX;
    for e in sets.iter().rev() {
        let r = &e.rows[row as usize];
        if !e.is_agreed() || r.verdict != RowVerdict::Agreed || !held.contains(&e.position) {
            continue;
        }
        if e.position > frontier {
            wait_above = wait_above.min(e.position);
            continue;
        }
        return (
            StartSet {
                position: e.position,
                version: r.version,
            },
            wait_above,
        );
    }
    (StartSet::default(), wait_above)
}
```

- [ ] **Step 4: The publisher.** In `node.rs` `Consensus` add after `holdings_held`:

```rust
    /// Snapshot-lifecycle spec §4.2: the start set last published per row.
    start_sets: [crate::catalog::StartSet; CNC_MAX_SERVICES],
    /// Spec §4.3: set by every edge that can change a start set — a catalog
    /// change, a held-set change — and cleared by the recompute.
    start_sets_dirty: bool,
    /// The frontier at which a newer held agreed set becomes eligible
    /// (`u64::MAX` = none): the only reason a clean pass recomputes.
    start_set_wait_above: u64,
    /// Recomputes run (test-visible; the steady-pass witness).
    start_set_recomputes: u64,
```

initialised in both struct literals as `start_sets: [crate::catalog::StartSet::default(); CNC_MAX_SERVICES], start_sets_dirty: true, start_set_wait_above: u64::MAX, start_set_recomputes: 0,`. Set `self.start_sets_dirty = true;` at the end of `note_catalog_for_holdings`, inside `note_set_held` (before `publish_sets_held`), and right after the pruner's `self.holdings_held.retain(..)`. Add the methods:

```rust
    /// Snapshot-lifecycle spec §4.3: recompute only when the catalog or this
    /// node's held list changed, or the frontier passed a newer held agreed
    /// set. A steady pass is one `bool` test and one compare.
    #[inline]
    fn maybe_publish_start_sets(&mut self) {
        if !self.start_sets_dirty {
            if self.start_set_wait_above == u64::MAX {
                return;
            }
            let c = self.cnc.counters();
            let frontier = c.commit.load_acquire().min(c.durable.load_acquire());
            if frontier < self.start_set_wait_above {
                return;
            }
        }
        self.publish_start_sets();
    }

    /// Spec §4.2: write each declared row's start set to its cnc slot — only
    /// a row whose answer changed is written (`store_start_set`).
    #[cold]
    #[inline(never)]
    fn publish_start_sets(&mut self) {
        self.start_sets_dirty = false;
        self.start_set_recomputes += 1;
        let c = self.cnc.counters();
        let frontier = c.commit.load_acquire().min(c.durable.load_acquire());
        let inner = self.cluster_view.snapshot_inner();
        let mut rows = [0u8; CNC_MAX_SERVICES];
        let mut n = 0usize;
        for row in self.services.ids() {
            rows[n] = row;
            n += 1;
        }
        let mut wait = u64::MAX;
        for &row in &rows[..n] {
            let (s, w) =
                crate::catalog::start_set_for(row, &inner.catalog, &self.holdings_held, frontier);
            wait = wait.min(w);
            if s != self.start_sets[row as usize] {
                self.start_sets[row as usize] = s;
                self.cnc
                    .service_slot(row as usize)
                    .snapshot_pos
                    .store_start_set(s.position, s.version);
                crate::obs_event!(
                    Info,
                    "start_set_published",
                    node = self.id as u64,
                    row = row as u64,
                    position = s.position,
                    version = s.version as u64
                );
            }
        }
        self.start_set_wait_above = wait;
    }
```

In `do_work`, right after `self.check_set_completeness();`:

```rust
        // Snapshot-lifecycle spec §4: and the rows' START SETS — one `bool`
        // test on a steady pass.
        self.maybe_publish_start_sets();
```

- [ ] **Step 5: Run, expect PASS.**
Run: `cargo test -p uc_node --lib start_set catalog`
Expected: PASS.

- [ ] **Step 6: Commit.**

```bash
cargo fmt --all
git commit -am "node(lifecycle): publish each row's start set on its cnc slot"
```

---

### Task 8: The start rule in the service

**Files:**
- Create: `uc_service/src/start_set.rs`
- Modify: `uc_service/src/lib.rs` (`mod start_set;` beside `mod replay;`)
- Modify: `uc_service/src/attach.rs` (after the pinned-install block ~:365, and `start_pos` ~:439)
- Modify: `uc_service/src/replay.rs` (`ReplayInstant` gains `resume`; `replay_into` after `let mut start_pos = ..` ~:240)
- Modify: `uc_service/src/apply.rs` (`ReplayInstant { .. }` literal ~:757 passes `resume: st.follower.cursor`)
- Test: `uc_service/src/start_set.rs` `mod tests`

**Interfaces:**
- Consumes: Task 5 `SnapshotPosLine::start_set`; existing `SnapshotStore`, `verify_snapshot_envelope`, `InstallFn<S>` (`apply.rs:151`), `uc_log::cnc::RowRead`.
- Produces (crate-private): `pub(crate) fn start_set_permitted(attach_pin: Option<(u64, u32, u32)>, live: &RowRead, decided_to: u64) -> bool`; `pub(crate) fn install_start_set<S: RawStateMachine>(sm: &mut S, slot: &ServiceSlot, row: u8, resume: u64, frontier: u64, store: &SnapshotStore, install: &InstallFn<S>) -> Result<Option<u64>, ServiceError>` — `Ok(Some(p))` = installed at `p`, the caller resumes the follower at `p`; `Ok(None)` = today's behaviour.

- [ ] **Step 1: Write the failing tests.** Create `uc_service/src/start_set.rs` with only the test module first (the functions come in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read as _, Write as _};
    use std::sync::Arc;
    use uc_log::cnc::{CncMeta, CncPage};

    struct Sum {
        total: u64,
        last: Option<u64>,
    }
    impl crate::traits::RawStateMachine for Sum {
        const NAME: &'static str = "sum";
        fn apply(&mut self, ctx: &mut crate::ApplyCtx, cmd: &[u8], _out: &mut Vec<u8>) {
            self.total += cmd.len() as u64;
            self.last = Some(ctx.position);
        }
        fn query(&self, _q: &[u8], _out: &mut Vec<u8>) {}
        fn last_applied(&self) -> Option<u64> {
            self.last
        }
    }

    fn page() -> Arc<CncPage> {
        CncPage::heap(&CncMeta {
            node_id: 1,
            instance_id: 1,
            app_id: "start-set".into(),
            buffer_bytes: 1 << 20,
            max_payload: 256,
            services: [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES],
        })
    }

    fn scratch() -> tempfile::TempDir {
        let base = std::env::current_exe().unwrap().parent().unwrap().to_path_buf();
        tempfile::tempdir_in(base).unwrap()
    }

    /// `total ‖ cursor` payload; `install` restores both (cursor strictly
    /// below the tag, the exclusive-frontier contract).
    fn install() -> InstallFn<Sum> {
        Box::new(|sm: &mut Sum, pos: u64, src: &mut dyn std::io::Read| {
            let mut b = [0u8; 16];
            src.read_exact(&mut b)?;
            sm.total = u64::from_le_bytes(b[..8].try_into().unwrap());
            sm.last = Some(u64::from_le_bytes(b[8..].try_into().unwrap()));
            Ok(pos)
        })
    }

    fn publish(store: &SnapshotStore, pos: u64, version: u32, total: u64) {
        store
            .publish(pos, version, |w| {
                w.write_all(&total.to_le_bytes())?;
                w.write_all(&(pos - 32).to_le_bytes())?;
                Ok(())
            })
            .unwrap();
    }

    fn fresh() -> Sum {
        Sum { total: 0, last: None }
    }

    /// Spec §5 rule 2: ahead → install, resume at the set.
    #[test]
    fn a_start_set_ahead_of_resume_is_installed() {
        let (p, dir) = (page(), scratch());
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        publish(&store, 4096, 0, 77);
        let slot = p.service_slot(0);
        slot.snapshot_pos.store_start_set(4096, 0);
        let mut sm = fresh();
        assert_eq!(install_start_set(&mut sm, slot, 0, 0, 8192, &store, &install()).unwrap(), Some(4096));
        assert_eq!((sm.total, sm.last), (77, Some(4064)));
    }

    /// Spec §11 "overrun recovery jumping forward": the overrun path passes
    /// `resume = max(state machine position, follower cursor)`. A start set
    /// ahead of both is installed (the row jumps forward); the set a row
    /// already installed at attach — the follower sits AT it while the state
    /// machine's own cursor is strictly below it — is never installed twice.
    #[test]
    fn overrun_recovery_jumps_forward_and_never_reinstalls_the_attach_set() {
        let (p, dir) = (page(), scratch());
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        publish(&store, 4096, 0, 77);
        publish(&store, 8192, 0, 99);
        let slot = p.service_slot(0);
        // Attached at 4096: SM cursor 4064, follower cursor 4096.
        slot.snapshot_pos.store_start_set(4096, 0);
        let mut sm = Sum { total: 77, last: Some(4064) };
        let resume = 4064u64.max(4096);
        assert_eq!(install_start_set(&mut sm, slot, 0, resume, 16384, &store, &install()).unwrap(), None);
        // A newer start set published since: the overrun jumps to it.
        slot.snapshot_pos.store_start_set(8192, 0);
        assert_eq!(install_start_set(&mut sm, slot, 0, resume, 16384, &store, &install()).unwrap(), Some(8192));
        assert_eq!((sm.total, sm.last), (99, Some(8160)));
    }

    /// Spec §5: behind or equal → no install (strict `>`: never rewind).
    #[test]
    fn a_start_set_at_or_behind_resume_is_not_installed() {
        let (p, dir) = (page(), scratch());
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        publish(&store, 4096, 0, 77);
        let slot = p.service_slot(0);
        slot.snapshot_pos.store_start_set(4096, 0);
        for resume in [4096u64, 5000] {
            let mut sm = Sum { total: 5, last: Some(resume) };
            assert_eq!(install_start_set(&mut sm, slot, 0, resume, 8192, &store, &install()).unwrap(), None);
            assert_eq!(sm.total, 5, "untouched");
        }
    }

    /// Plan ruling P3: another LINE → replay; a patch build of the same line
    /// installs (the envelope check is by line too).
    #[test]
    fn a_start_set_built_by_another_line_is_not_installed() {
        use uc_protocol::identity::pack_version;
        let (p, dir) = (page(), scratch());
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        publish(&store, 4096, pack_version(1, 0, 0), 77);
        let slot = p.service_slot(0);
        slot.snapshot_pos.store_start_set(4096, pack_version(1, 0, 0));
        let mut sm = fresh();
        // `Sum::VERSION` is 0 — line 0.0, not 1.0.
        assert_eq!(install_start_set(&mut sm, slot, 0, 0, 8192, &store, &install()).unwrap(), None);
        assert_eq!(sm.total, 0);
    }

    /// Review focus 1 + spec §10 row 1: the artifact is gone (pruned between
    /// publish and install) → replay, by name, never an error.
    #[test]
    fn a_start_set_whose_artifact_is_missing_falls_back_to_replay() {
        let (p, dir) = (page(), scratch());
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        let slot = p.service_slot(0);
        slot.snapshot_pos.store_start_set(4096, 0);
        let mut sm = fresh();
        assert_eq!(install_start_set(&mut sm, slot, 0, 0, 8192, &store, &install()).unwrap(), None);
    }

    /// Review focus 1: a file at the name whose envelope does not verify (a
    /// torn or mis-tagged artifact) → replay; the state machine is untouched.
    #[test]
    fn a_start_set_whose_envelope_is_bad_falls_back_to_replay() {
        let (p, dir) = (page(), scratch());
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        std::fs::write(store.path_for(4096), b"not an envelope").unwrap();
        let slot = p.service_slot(0);
        slot.snapshot_pos.store_start_set(4096, 0);
        let mut sm = fresh();
        assert_eq!(install_start_set(&mut sm, slot, 0, 0, 8192, &store, &install()).unwrap(), None);
        assert_eq!((sm.total, sm.last), (0, None));
    }

    /// Spec §4.1(4) belt and braces: never above `min(commit, durable)`.
    #[test]
    fn a_start_set_above_the_frontier_is_not_installed() {
        let (p, dir) = (page(), scratch());
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        publish(&store, 4096, 0, 77);
        let slot = p.service_slot(0);
        slot.snapshot_pos.store_start_set(4096, 0);
        let mut sm = fresh();
        assert_eq!(install_start_set(&mut sm, slot, 0, 0, 4000, &store, &install()).unwrap(), None);
    }

    /// Plan ruling P4: the state machine's OWN install failing after the
    /// envelope verified is a fail-stop — it may be half-mutated.
    #[test]
    fn a_state_machine_install_error_is_a_fail_stop() {
        let (p, dir) = (page(), scratch());
        let store = SnapshotStore::open(dir.path(), 0).unwrap();
        publish(&store, 4096, 0, 77);
        let slot = p.service_slot(0);
        slot.snapshot_pos.store_start_set(4096, 0);
        let failing: InstallFn<Sum> =
            Box::new(|_, _, _| Err(std::io::Error::other("injected").into()));
        let mut sm = fresh();
        assert!(matches!(
            install_start_set(&mut sm, slot, 0, 0, 8192, &store, &failing),
            Err(ServiceError::Replay(_))
        ));
    }

    /// Review focus 2 + spec §5 rule 1: a pinned row never takes the start
    /// set, whatever the catalog's newest agreed set is.
    #[test]
    fn a_pinned_row_never_takes_the_start_set() {
        let unpinned = RowRead::View { pin: None, running: Some(0), record_pos: 100 };
        assert!(start_set_permitted(None, &unpinned, 100));
        assert!(!start_set_permitted(Some((4096, 0, 0)), &unpinned, 100), "pinned at attach");
    }

    /// Plan ruling P5: in overrun recovery the jump is refused when the row's
    /// live view now carries a pin, a version record committed above what the
    /// walk has decided, or the view is mid-publish.
    #[test]
    fn the_overrun_jump_is_refused_on_a_pinned_row_or_after_a_new_version_record() {
        let pinned_now = RowRead::View { pin: Some((8192, 0, 0)), running: Some(0), record_pos: 100 };
        assert!(!start_set_permitted(None, &pinned_now, 100));
        let newer_record = RowRead::View { pin: None, running: Some(0), record_pos: 9000 };
        assert!(!start_set_permitted(None, &newer_record, 8000));
        assert!(!start_set_permitted(None, &RowRead::Contended, 8000));
    }
}
```

- [ ] **Step 2: Run, expect FAIL.**
Run: `cargo test -p uc_service --lib start_set`
Expected: FAIL to compile — `cannot find function install_start_set`, `cannot find function start_set_permitted` (after adding `mod start_set;` to `lib.rs`).

- [ ] **Step 3: Implement the module** (above the tests in `start_set.rs`):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Snapshot-lifecycle spec §5: the START RULE. At attach and in overrun
//! recovery, a row installs the start set its node published on the cnc
//! slot (`+264/+272`) when that moves the row forward, then replays only the
//! tail. A pinned row takes its pin path instead (rule 1); every other case
//! is today's behaviour (rule 3), and a set that cannot be read falls back
//! to it by name (rule 4).

use uc_log::cnc::{RowRead, ServiceSlot};

use crate::apply::InstallFn;
use crate::config::ServiceError;
use crate::snapshots::{SnapshotStore, verify_snapshot_envelope};
use crate::traits::RawStateMachine;

/// Spec §5 rule 1 and plan ruling P5: may this row take a start set at all?
/// Never when it attached under a pin (the pin path has priority). In
/// overrun recovery also never when the row's LIVE view now names a pin, or
/// a version record whose frame END is above `decided_to` (jumping over it
/// would skip the #33 exact stop), or the view is mid-publish. At attach the
/// caller passes the view it just read and its own `record_pos`.
pub(crate) fn start_set_permitted(
    attach_pin: Option<(u64, u32, u32)>,
    live: &RowRead,
    decided_to: u64,
) -> bool {
    if attach_pin.is_some() {
        return false;
    }
    matches!(live, RowRead::View { pin: None, record_pos, .. } if *record_pos <= decided_to)
}

/// Spec §5 rule 2: install the row's start set when it is strictly ahead of
/// `resume` (where the row would otherwise resume), at or below `frontier`
/// (`min(commit, durable)`), and built on this binary's LINE (plan ruling
/// P3). `Ok(Some(p))`: installed — resume the follower AT `p` (the tag is an
/// exclusive frontier). `Ok(None)`: today's behaviour. A missing or
/// unverifiable artifact is `Ok(None)` with one line naming why (rule 4); an
/// error from the state machine's own install is a fail-stop (plan ruling
/// P4) — it may have half-mutated the state.
pub(crate) fn install_start_set<S: RawStateMachine>(
    sm: &mut S,
    slot: &ServiceSlot,
    row: u8,
    resume: u64,
    frontier: u64,
    store: &SnapshotStore,
    install: &InstallFn<S>,
) -> Result<Option<u64>, ServiceError> {
    let Some((pos, version)) = slot.snapshot_pos.start_set() else {
        return Ok(None);
    };
    if pos <= resume || pos > frontier {
        return Ok(None);
    }
    if !uc_protocol::identity::same_line(version, S::VERSION) {
        eprintln!(
            "uc_service: row {row} start set snap-{pos} was built by {version:#010x}, \
             this binary is {:#010x}; replaying instead",
            S::VERSION
        );
        return Ok(None);
    }
    let path = store.path_for(pos);
    let mut file = match std::fs::File::open(&path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!(
                "uc_service: row {row} start set {} unreadable ({e}); replaying instead",
                path.display()
            );
            return Ok(None);
        }
    };
    if let Err(e) = verify_snapshot_envelope(&mut file, pos, Some(S::VERSION)) {
        eprintln!(
            "uc_service: row {row} start set {} refused ({e}); replaying instead",
            path.display()
        );
        return Ok(None);
    }
    let installed = (install)(sm, pos, &mut file)
        .map_err(|e| ServiceError::Replay(format!("start-set install at {pos}: {e}")))?;
    let cursor = sm.last_applied();
    if installed != pos || cursor.is_none() || cursor >= Some(pos) {
        return Err(ServiceError::Replay(format!(
            "start-set install at {pos} left the state machine at {cursor:?} \
             (returned {installed}); install_snapshot must land at the tag \
             with its cursor strictly below it"
        )));
    }
    eprintln!("uc_service: row {row} started from snap-{pos} (start set; resume was {resume})");
    Ok(Some(pos))
}
```

In `uc_service/src/lib.rs` add `mod start_set;` after `mod replay;`.

- [ ] **Step 4: Wire attach.** In `attach.rs`, immediately after the pinned-install `if let Some((origin, from, to)) = pin { .. }` block:

```rust
    // Snapshot-lifecycle spec §5 rule 2: an UNPINNED row installs its start
    // set when it moves the row forward. Rule 1 (the pin path) already ran
    // above and wins; `start_set_permitted` re-states it for the reader.
    let mut start_set_at: Option<u64> = None;
    if let Some(install_fn) = install.as_ref()
        && crate::start_set::start_set_permitted(
            pin,
            &RowRead::View {
                pin,
                running,
                record_pos: attach_record_pos,
            },
            attach_record_pos,
        )
    {
        let frontier = {
            let c = cnc.counters();
            c.commit.load_acquire().min(c.durable.load_acquire())
        };
        let store = SnapshotStore::open(dir, row)?;
        // Read before the call: `&mut sm` is the first argument.
        let resume = sm.last_applied().unwrap_or(0);
        start_set_at = crate::start_set::install_start_set(
            &mut sm,
            s,
            row,
            resume,
            frontier,
            &store,
            install_fn,
        )?;
    }
```

(`RowRead` is already imported for the pin read; the `RowRead::View` field types are `pin: Option<(u64, u32, u32)>`, `running: Option<u32>`, `record_pos: u64` — the values destructured at step 1d.) The resume position becomes:

```rust
    let start_pos = match (pin, start_set_at) {
        (Some((origin, _, _)), _) => origin,
        (None, Some(at)) => at,
        (None, None) => last_applied.unwrap_or(0),
    };
```

and extend the comment above it with one sentence: "After a START-SET install (snapshot lifecycle §5) the same reasoning holds: resume AT the set's position."

- [ ] **Step 5: Wire overrun recovery.** In `replay.rs` `ReplayInstant` add:

```rust
    /// Snapshot-lifecycle spec §5: where the row would otherwise resume — the
    /// live follower's cursor. The start set is installed only when strictly
    /// ahead of BOTH this and the state machine's own position, so a row that
    /// just installed its start set at attach never installs it again here.
    pub resume: u64,
```

In `replay_into`, right after `let mut start_pos = guard.last_applied().unwrap_or(0);`:

```rust
    // Snapshot-lifecycle spec §5, overrun recovery: jump on the node's start
    // set before the journal scan, when it moves the row forward and the row
    // may take one (plan ruling P5 — no pin now, no newer version record).
    if let Some(r) = restore {
        let slot = crate::attach::slot(cnc, instant.service_id);
        if crate::start_set::start_set_permitted(
            instant.pin,
            &slot.status.row_view(),
            instant.decided_to,
        ) {
            let frontier = {
                let c = cnc.counters();
                c.commit.load_acquire().min(c.durable.load_acquire())
            };
            if let Some(at) = crate::start_set::install_start_set(
                &mut *guard,
                slot,
                instant.service_id,
                start_pos.max(instant.resume),
                frontier,
                &r.store,
                &r.install,
            )? {
                start_pos = at;
                cursor = at;
            }
        }
    }
```

In `apply.rs`'s `ReplayInstant { .. }` literal add `resume: st.follower.cursor,`.

- [ ] **Step 6: Run, expect PASS.**
Run: `cargo test -p uc_service`
Expected: PASS (the existing `pinned_attach`, `reconstruction` and `whole_state_snapshot` suites unchanged: on a harness node nothing publishes a start set, so `start_set()` reads `None`).

- [ ] **Step 7: Commit.**

```bash
cargo fmt --all
git commit -am "service(lifecycle): the start rule at attach and in overrun recovery"
```

---
### Task 9: Auto-fetch — the state machine, the holder choice, the space check

**Files:**
- Create: `uc_node/src/auto_fetch.rs`
- Modify: `uc_node/src/lib.rs` (`pub mod auto_fetch;`)
- Modify: `uc_node/src/catalog.rs` (`fetch_candidates`, `builders_at`, `reporters_at`, tests)
- Modify: `uc_node/src/audit.rs` (`SOURCE_AUTO`, a test)
- Modify: `uc_node/src/node.rs`:
  - `PendingFetch` :1036 gains `auto: bool`;
  - `start_fetch` :10424 splits into the learner door + `issue_fetch`; `poll_pending_fetch` feeds auto outcomes;
  - `Consensus` fields `auto_fetch`, `auto_fetch_stats`, `soft_wire`, `soft_stale_ns` (both struct literals);
  - `Node` fields `auto_fetch_stats`, `free_override`; `Node::soft_table` :2632 body moves into a free fn `soft_table_from_wire`; new `Node::set_free_bytes_for_test`;
  - `HoldingsProbe` :11869 gains `free_override`; its construction :1997 chains `.with_free_override(..)`;
  - `do_work`: `self.maybe_auto_fetch();` beside `self.poll_pending_fetch();` (:4520);
  - new `maybe_auto_fetch`, `auto_fetch_step`, `local_build_pending`, `auto_fetch_candidates`, `audit_auto_fetch`.
- Modify: `uc_node/tests/learner.rs` (every `settings_genesis: ..Settings::genesis_default()` → `auto_fetch: false`), `uc_node/tests/catalog.rs` (`a_killed_node_leaves_holders_after_the_stale_timeout` and `learner_only_voters_do_not_purge_until_they_fetch` run with `auto_fetch = false`)
- Test: `auto_fetch.rs`, `catalog.rs`, `audit.rs`, `node.rs` `mod tests`

**Interfaces:**
- Consumes: Task 3 `ClusterView.auto_fetch`; Task 4 `ClusterView.catalog_newest_agreed_bytes`; `ClusterView.catalog_agreed_position`, `CatalogQuery::holders`.
- Produces (`uc_node::auto_fetch`, `pub`):

```rust
pub const AUTO_FETCH_STAGGER_NS: u64 = 250_000_000;
pub const AUTO_FETCH_BACKOFF_MIN_NS: u64 = 1_000_000_000;
pub const AUTO_FETCH_BACKOFF_MAX_NS: u64 = 30_000_000_000;
pub const AUTO_FETCH_RECHECK_NS: u64 = 100_000_000;
pub const FETCH_HEADROOM_MIN_BYTES: u64 = 1 << 30;
pub fn fits(free_bytes: u64, total: u64) -> bool;
pub enum Outcome { Ok, Refused, Timeout, NoSpace, NoHolder }   // .label(), Outcome::ALL
pub struct AutoFetchStats;                                      // .bump(Outcome), .get(Outcome)
pub enum SpaceCheck { Fits, Unknown { first: bool }, NoSpace { first: bool } }
pub struct AutoFetch;  // new(NodeId), quiet(n, now), due(n, durable, building, now),
                       // check_space(total, free, now), pick(&[NodeId], now),
                       // on_result(Outcome, now), first_thin(), target()
```

- Produces (`uc_node::catalog`, `pub`): `fetch_candidates(self_id, live_holders, builders, learners, voters) -> Vec<NodeId>`; `builders_at(reports: &[SnapshotReport], sets: &[SetEntry], p: u64) -> Vec<NodeId>`; `reporters_at(reports: &[SnapshotReport], p: u64) -> usize`.
- Produces (`uc_node::audit`): `pub const SOURCE_AUTO: &str = "auto";`.
- Produces (`uc_node::Node`): `#[doc(hidden)] pub fn set_free_bytes_for_test(&self, v: u64)`; `auto_fetch_stats: Arc<AutoFetchStats>` (Task 10 wires it into `ObsSources`).

- [ ] **Step 1: Write the failing tests for the state machine.** Create `uc_node/src/auto_fetch.rs` containing only this test module for now:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    /// Spec §7.3: `free >= total + max(total / 4, 1 GiB)`, exactly.
    #[test]
    fn fits_is_exact_at_its_threshold() {
        assert!(fits(5 * GIB, 4 * GIB), "4 GiB needs 1 GiB headroom: 5 GiB fits");
        assert!(!fits(5 * GIB - 1, 4 * GIB), "one byte short");
        assert!(fits(10 * GIB, 8 * GIB), "8 GiB: headroom is total/4 = 2 GiB");
        assert!(!fits(10 * GIB - 1, 8 * GIB));
        assert!(fits(100 + GIB, 100), "a small set still needs the 1 GiB floor");
        assert!(!fits(100 + GIB - 1, 100));
        assert!(!fits(u64::MAX - 1, u64::MAX), "saturates, never wraps");
    }

    /// Spec §6: the first attempt for a given N waits node_id × 250 ms.
    #[test]
    fn the_first_attempt_is_staggered_by_node_id() {
        let mut a = AutoFetch::new(3);
        assert!(!a.due(1000, 2000, false, 0));
        assert!(!a.due(1000, 2000, false, 749_999_999));
        assert!(a.due(1000, 2000, false, 750_000_000));
        let mut z = AutoFetch::new(0);
        assert!(z.due(1000, 2000, false, 0), "node 0 does not wait");
    }

    /// Spec §6 trigger: a set ahead of this node's durable frontier waits,
    /// and so does one this node is still building (plan ruling P8); the
    /// wait is re-checked every AUTO_FETCH_RECHECK_NS, not every pass (P9).
    #[test]
    fn a_set_above_durable_or_still_being_built_waits() {
        let mut a = AutoFetch::new(0);
        assert!(!a.due(1000, 999, false, 0), "above durable");
        assert!(a.quiet(1000, AUTO_FETCH_RECHECK_NS - 1), "re-checked only after the recheck delay");
        assert!(!a.due(1000, 2000, true, AUTO_FETCH_RECHECK_NS), "still building");
        assert!(a.due(1000, 2000, false, 2 * AUTO_FETCH_RECHECK_NS));
    }

    /// Spec §6: a refusal or timeout moves to the NEXT holder, backing off
    /// 1 s, 2 s, 4 s …; when every candidate has been tried the outcome is
    /// no_holder and the next attempt waits the 30 s ceiling (plan ruling P10).
    #[test]
    fn a_refusal_or_timeout_moves_to_the_next_holder_with_doubling_backoff() {
        let mut a = AutoFetch::new(0);
        let c = [7, 8, 9];
        let mut t = 0;
        assert!(a.due(1000, 2000, false, t));
        assert_eq!(a.pick(&c, t), Some(7));
        a.on_result(Outcome::Timeout, t);
        assert!(!a.due(1000, 2000, false, t + AUTO_FETCH_BACKOFF_MIN_NS - 1));
        t += AUTO_FETCH_BACKOFF_MIN_NS;
        assert!(a.due(1000, 2000, false, t));
        assert_eq!(a.pick(&c, t), Some(8));
        a.on_result(Outcome::Refused, t);
        assert!(!a.due(1000, 2000, false, t + 2 * AUTO_FETCH_BACKOFF_MIN_NS - 1));
        t += 2 * AUTO_FETCH_BACKOFF_MIN_NS;
        assert!(a.due(1000, 2000, false, t));
        assert_eq!(a.pick(&c, t), Some(9));
        a.on_result(Outcome::Timeout, t);
        t += 4 * AUTO_FETCH_BACKOFF_MIN_NS;
        assert!(a.due(1000, 2000, false, t));
        assert_eq!(a.pick(&c, t), None, "every candidate tried: no_holder");
        assert!(!a.due(1000, 2000, false, t + AUTO_FETCH_BACKOFF_MAX_NS - 1));
        assert!(a.due(1000, 2000, false, t + AUTO_FETCH_BACKOFF_MAX_NS));
        assert_eq!(a.pick(&c, t + AUTO_FETCH_BACKOFF_MAX_NS), Some(7), "the list starts over");
    }

    /// Review focus 3: a holder whose soft entry is stale but still listed
    /// FIRST is not retried before every other candidate has had its turn.
    #[test]
    fn a_timed_out_holder_is_not_tried_again_before_every_other_candidate() {
        let mut a = AutoFetch::new(0);
        assert!(a.due(1000, 2000, false, 0));
        assert_eq!(a.pick(&[5, 6], 0), Some(5));
        a.on_result(Outcome::Timeout, 0);
        assert!(a.due(1000, 2000, false, AUTO_FETCH_BACKOFF_MIN_NS));
        assert_eq!(a.pick(&[5, 6], AUTO_FETCH_BACKOFF_MIN_NS), Some(6), "5 is still listed first; 6 goes next");
    }

    /// Review focus 3: a lone dead holder costs one timeout, then a 30 s
    /// ceiling — never a back-to-back series of 60 s timeouts.
    #[test]
    fn a_lone_dead_holder_costs_one_timeout_per_backoff_ceiling() {
        let mut a = AutoFetch::new(0);
        assert!(a.due(1000, 2000, false, 0));
        assert_eq!(a.pick(&[5], 0), Some(5));
        a.on_result(Outcome::Timeout, 60_000_000_000);
        let t = 60_000_000_000 + AUTO_FETCH_BACKOFF_MIN_NS;
        assert!(a.due(1000, 2000, false, t));
        assert_eq!(a.pick(&[5], t), None, "not 5 again straight away");
        assert!(a.quiet(1000, t + AUTO_FETCH_BACKOFF_MAX_NS - 1));
    }

    /// Spec §6 "only the newest": a newer agreed set moves the target, clears
    /// the tried list and restarts the stagger.
    #[test]
    fn chasing_the_newest_resets_the_tried_list() {
        let mut a = AutoFetch::new(0);
        assert!(a.due(1000, 9000, false, 0));
        assert_eq!(a.pick(&[5], 0), Some(5));
        a.on_result(Outcome::Timeout, 0);
        assert!(a.due(2000, 9000, false, 1), "a new target is due at once (node 0)");
        assert_eq!(a.target(), 2000);
        assert_eq!(a.pick(&[5], 1), Some(5), "5 is untried for the new target");
    }

    /// Review focus 4 + spec §7.3: an unknown size (0) is fetched without the
    /// check; the log line is named once per set.
    #[test]
    fn an_unknown_size_is_fetched_without_the_check() {
        let mut a = AutoFetch::new(0);
        assert!(a.due(1000, 9000, false, 0));
        assert_eq!(a.check_space(0, 0, 0), SpaceCheck::Unknown { first: true });
        assert_eq!(a.check_space(0, 0, 0), SpaceCheck::Unknown { first: false });
    }

    /// Spec §7.3 + plan ruling P11: no space → skipped, named once per set,
    /// and backed off; a newer set is named again.
    #[test]
    fn no_space_is_named_once_per_set_and_backs_off() {
        let mut a = AutoFetch::new(0);
        assert!(a.due(1000, 9000, false, 0));
        assert_eq!(a.check_space(4 * GIB, GIB, 0), SpaceCheck::NoSpace { first: true });
        assert!(a.quiet(1000, AUTO_FETCH_BACKOFF_MIN_NS - 1));
        assert!(a.due(1000, 9000, false, AUTO_FETCH_BACKOFF_MIN_NS));
        assert_eq!(a.check_space(4 * GIB, GIB, AUTO_FETCH_BACKOFF_MIN_NS), SpaceCheck::NoSpace { first: false });
        assert!(a.due(2000, 9000, false, 10 * AUTO_FETCH_BACKOFF_MIN_NS));
        assert_eq!(a.check_space(4 * GIB, GIB, 0), SpaceCheck::NoSpace { first: true });
        assert_eq!(a.check_space(4 * GIB, 6 * GIB, 0), SpaceCheck::Fits);
    }

    /// An `ok` resets the ladder.
    #[test]
    fn ok_resets_the_backoff() {
        let mut a = AutoFetch::new(0);
        assert!(a.due(1000, 9000, false, 0));
        assert_eq!(a.pick(&[5], 0), Some(5));
        a.on_result(Outcome::Timeout, 0);
        a.on_result(Outcome::Ok, 0);
        assert!(a.due(1000, 9000, false, 0), "no backoff after ok");
    }

    #[test]
    fn outcomes_have_the_spec_labels_and_count_independently() {
        assert_eq!(
            Outcome::ALL.map(Outcome::label),
            ["ok", "refused", "timeout", "no_space", "no_holder"]
        );
        let s = AutoFetchStats::default();
        s.bump(Outcome::NoSpace);
        s.bump(Outcome::NoSpace);
        s.bump(Outcome::Ok);
        assert_eq!((s.get(Outcome::NoSpace), s.get(Outcome::Ok), s.get(Outcome::Timeout)), (2, 1, 0));
    }
}
```

In `uc_node/src/catalog.rs` `mod tests`:

```rust
    /// Plan ruling P1: known holders (live soft entries, then builders) first,
    /// learners first then lowest id; then every other member the same way;
    /// never self; no duplicates.
    #[test]
    fn fetch_candidates_put_known_holders_first_learners_first_then_everyone_else() {
        // self = 1; learners 4, 5; voters 0..=3; live holder 3; builders 4 and self.
        let c = fetch_candidates(1, &[3], &[4, 1], &[5, 4], &[0, 1, 2, 3]);
        assert_eq!(c, vec![4, 3, 5, 0, 2]);
        assert_eq!(fetch_candidates(0, &[], &[], &[], &[0]), Vec::<NodeId>::new(), "a solo node has nobody");
    }

    /// Plan ruling P1: the builders of the set at P are the reporters whose
    /// hash matches the catalog's row hash at P.
    #[test]
    fn builders_are_the_reporters_of_the_catalogued_hash() {
        let mut e = agreed(1000);
        e.rows[0].hash = 7;
        let reports = [SnapshotReport { row: 0, position: 1000, hashes: vec![(0, 7, 1), (2, 8, 1), (3, 7, 1)] }];
        assert_eq!(builders_at(&reports, &[e.clone()], 1000), vec![0, 3]);
        assert_eq!(builders_at(&reports, &[e], 2000), Vec::<NodeId>::new(), "no entry at 2000");
    }

    /// Review focus 5: a set reported by exactly one node is visible as such.
    #[test]
    fn reporters_at_counts_the_reporters_of_one_instant() {
        let reports = [
            SnapshotReport { row: 0, position: 1000, hashes: vec![(4, 7, 1)] },
            SnapshotReport { row: 1, position: 900, hashes: vec![(0, 7, 1), (1, 7, 1)] },
        ];
        assert_eq!(reporters_at(&reports, 1000), 1);
        assert_eq!(reporters_at(&reports, 900), 2);
        assert_eq!(reporters_at(&reports, 5), 0);
    }
```

In `uc_node/src/audit.rs` `mod tests`:

```rust
    /// Snapshot-lifecycle spec §6: an auto-fetch is the existing
    /// `snapshot_fetch` record with `actor = "auto"`.
    #[test]
    fn an_auto_fetch_record_names_actor_auto() {
        let dir = tempdir();
        let mut a = AuditLog::open(dir.path()).unwrap();
        let mut r = rec(0);
        r.actor = "auto";
        r.op = 9;
        r.op_name = op_name(9);
        r.addr = None;
        r.nonce = 0;
        r.config_version = 4096;
        r.source = SOURCE_AUTO;
        a.record(&r).unwrap();
        let text = std::fs::read_to_string(a.path()).unwrap();
        assert!(text.contains(r#""actor":"auto""#), "{text}");
        assert!(text.contains(r#""op":9,"op_name":"snapshot_fetch""#), "{text}");
        assert!(text.ends_with(",\"detail\":null,\"source\":\"auto\"}\n"), "{text}");
    }
```

In `uc_node/src/node.rs` `mod tests`:

```rust
    /// Snapshot-lifecycle spec §6: the switch gates the whole path; a held
    /// newest set issues nothing.
    #[test]
    fn auto_fetch_runs_only_while_the_switch_is_on_and_the_newest_agreed_set_is_unheld() {
        use crate::auto_fetch::Outcome;
        let mut h = harness_with_rows(&["a"]);
        drive_to_serving_leader(&mut h);
        let mut st = h.cons.cluster_view.to_state();
        // A STANDBY set on this voter: nothing here will build it (plan
        // ruling P8), whatever the harness's slot says.
        let mut e = agreed_entry(1000);
        e.kind = uc_protocol::v2::catalog::SetKind::Standby;
        st.catalog = vec![e];
        st.settings.auto_fetch = false;
        h.cons.cluster_view.publish(&st);
        let cnc = Arc::clone(&h.cons.cnc);
        cnc.counters().durable.store_release(5000);
        let attempts = |h: &Harness| {
            u64::from(h.cons.pending_fetch.is_some())
                + h.cons.auto_fetch_stats.get(Outcome::Refused)
                + h.cons.auto_fetch_stats.get(Outcome::NoHolder)
        };
        for k in 0..3u64 {
            h.cons.pass_now_ns = 10_000_000_000 * (k + 1);
            h.cons.maybe_auto_fetch();
        }
        assert_eq!(attempts(&h), 0, "switch off: nothing");
        st.settings.auto_fetch = true;
        h.cons.cluster_view.publish(&st);
        // Two passes 10 s apart: the second is past any stagger (node id × 250 ms).
        for k in 3..5u64 {
            h.cons.pass_now_ns = 10_000_000_000 * (k + 1);
            h.cons.maybe_auto_fetch();
        }
        assert!(attempts(&h) >= 1, "switch on: the unheld newest agreed set is attempted");
        h.cons.pending_fetch = None;
        h.cons.note_set_held(1000);
        let before = attempts(&h);
        h.cons.pass_now_ns += 100_000_000_000;
        h.cons.maybe_auto_fetch();
        assert_eq!(attempts(&h), before, "held: nothing more");
    }

    /// Spec §6 visibility: a landed auto fetch counts `ok`, a deadline counts
    /// `timeout`; an operator fetch counts neither.
    #[test]
    fn a_landed_auto_fetch_counts_ok_and_a_deadline_counts_timeout() {
        use crate::auto_fetch::Outcome;
        let mut h = harness_with_rows(&["a"]);
        h.cons.pass_now_ns = 1_000;
        h.cons.pending_fetch = Some(PendingFetch {
            learner: 1,
            position: 1000,
            stored_before: 0,
            deadline_ns: 1_000_000,
            auto: true,
        });
        h.cons.stored_set_pos.store(1000, Ordering::Release);
        h.cons.poll_pending_fetch();
        assert_eq!(h.cons.auto_fetch_stats.get(Outcome::Ok), 1);
        h.cons.pending_fetch = Some(PendingFetch {
            learner: 1,
            position: 2000,
            stored_before: 1000,
            deadline_ns: 2_000,
            auto: true,
        });
        h.cons.pass_now_ns = 3_000;
        h.cons.poll_pending_fetch();
        assert_eq!(h.cons.auto_fetch_stats.get(Outcome::Timeout), 1);
        h.cons.pending_fetch = Some(PendingFetch {
            learner: 1,
            position: 3000,
            stored_before: 1000,
            deadline_ns: 2_000,
            auto: false,
        });
        h.cons.poll_pending_fetch();
        assert_eq!(h.cons.auto_fetch_stats.get(Outcome::Timeout), 1, "an operator fetch is not counted");
    }
```

- [ ] **Step 2: Run, expect FAIL.**
Run: `cargo test -p uc_node --lib auto_fetch fetch_candidates builders_are reporters_at an_auto_fetch_record a_landed_auto_fetch`
Expected: FAIL to compile — `cannot find function fits`, `cannot find type AutoFetch`, `cannot find function fetch_candidates`, `cannot find value SOURCE_AUTO`, `struct PendingFetch has no field auto`.

- [ ] **Step 3: Implement `uc_node/src/auto_fetch.rs`** (above the tests):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Snapshot-lifecycle spec §6–§7: the background AUTO-FETCH decision. Pure —
//! no I/O, no clock, no locks; the consensus agent feeds it the newest agreed
//! set, its durable frontier, the free-space reading, a candidate list and
//! the pass clock, and issues the existing store-only fetch when it says so.
//! One fetch at a time per node (`PendingFetch` is the node's), chasing the
//! newest agreed set only.

use std::sync::atomic::{AtomicU64, Ordering};

use uc_consensus::election::NodeId;

/// Spec §6: the first attempt for a given set waits `node_id × 250 ms`.
pub const AUTO_FETCH_STAGGER_NS: u64 = 250_000_000;
/// Spec §6: the retry ladder — 1 s, doubling, to 30 s.
pub const AUTO_FETCH_BACKOFF_MIN_NS: u64 = 1_000_000_000;
pub const AUTO_FETCH_BACKOFF_MAX_NS: u64 = 30_000_000_000;
/// Plan ruling P9: how often a WAITING decision (set above durable, or still
/// being built here) is looked at again — not every pass.
pub const AUTO_FETCH_RECHECK_NS: u64 = 100_000_000;
/// Spec §7.3: the headroom floor, a fixed default (not a setting).
pub const FETCH_HEADROOM_MIN_BYTES: u64 = 1 << 30;

/// Spec §7.3: `free_bytes >= total + max(total / 4, 1 GiB)`. Saturating: a
/// total near `u64::MAX` never fits.
pub fn fits(free_bytes: u64, total: u64) -> bool {
    free_bytes >= total.saturating_add((total / 4).max(FETCH_HEADROOM_MIN_BYTES))
}

/// Spec §6: `uc2_snapshot_auto_fetch_total{outcome}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Refused,
    Timeout,
    NoSpace,
    NoHolder,
}

impl Outcome {
    pub const ALL: [Outcome; 5] = [
        Outcome::Ok,
        Outcome::Refused,
        Outcome::Timeout,
        Outcome::NoSpace,
        Outcome::NoHolder,
    ];
    pub const fn label(self) -> &'static str {
        match self {
            Outcome::Ok => "ok",
            Outcome::Refused => "refused",
            Outcome::Timeout => "timeout",
            Outcome::NoSpace => "no_space",
            Outcome::NoHolder => "no_holder",
        }
    }
}

/// The counter family's storage — the consensus agent bumps it, `/metrics`
/// reads it at scrape (Relaxed both ways: a counter, not a gate).
#[derive(Debug, Default)]
pub struct AutoFetchStats {
    counts: [AtomicU64; 5],
}

impl AutoFetchStats {
    pub fn bump(&self, o: Outcome) {
        self.counts[o as usize].fetch_add(1, Ordering::Relaxed);
    }
    pub fn get(&self, o: Outcome) -> u64 {
        self.counts[o as usize].load(Ordering::Relaxed)
    }
}

/// Spec §7.3: the answer of [`AutoFetch::check_space`]. `first` is `true`
/// the first time a given set gets that answer — the caller names it once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpaceCheck {
    Fits,
    Unknown { first: bool },
    NoSpace { first: bool },
}

/// One node's auto-fetch decision state.
#[derive(Debug)]
pub struct AutoFetch {
    node_id: NodeId,
    /// The newest agreed set being chased (`0` = none yet).
    target: u64,
    /// Pass-clock ns before which nothing is attempted for `target`.
    next_attempt_ns: u64,
    /// The current rung of the 1 s → 30 s ladder (`0` = not backing off).
    backoff_ns: u64,
    /// Candidates already tried for `target`, in order (plan ruling P10).
    tried: Vec<NodeId>,
    no_space_named: bool,
    size_unknown_named: bool,
    thin_named: bool,
}

impl AutoFetch {
    pub fn new(node_id: NodeId) -> Self {
        AutoFetch {
            node_id,
            target: 0,
            next_attempt_ns: 0,
            backoff_ns: 0,
            tried: Vec::new(),
            no_space_named: false,
            size_unknown_named: false,
            thin_named: false,
        }
    }

    pub fn target(&self) -> u64 {
        self.target
    }

    /// The steady-pass test: nothing to do for `n` before the next attempt.
    #[inline]
    pub fn quiet(&self, n: u64, now_ns: u64) -> bool {
        n == self.target && now_ns < self.next_attempt_ns
    }

    /// Spec §6: is an attempt for the newest agreed set `n` due now? A new
    /// `n` resets the chase (tried list, ladder, once-per-set names) and
    /// starts the stagger. Waiting — `n` above `durable`, or still being
    /// built here — re-checks after [`AUTO_FETCH_RECHECK_NS`].
    pub fn due(&mut self, n: u64, durable: u64, building: bool, now_ns: u64) -> bool {
        if n != self.target {
            self.target = n;
            self.tried.clear();
            self.backoff_ns = 0;
            self.no_space_named = false;
            self.size_unknown_named = false;
            self.thin_named = false;
            self.next_attempt_ns = now_ns.saturating_add(u64::from(self.node_id) * AUTO_FETCH_STAGGER_NS);
        }
        if now_ns < self.next_attempt_ns {
            return false;
        }
        if n > durable || building {
            self.next_attempt_ns = now_ns.saturating_add(AUTO_FETCH_RECHECK_NS);
            return false;
        }
        true
    }

    /// Spec §7.3 before every attempt. A set of unknown size (`total == 0`)
    /// is fetched without the check (review focus 4); a set that does not fit
    /// backs off (plan ruling P11).
    pub fn check_space(&mut self, total: u64, free_bytes: u64, now_ns: u64) -> SpaceCheck {
        if total == 0 {
            let first = !self.size_unknown_named;
            self.size_unknown_named = true;
            return SpaceCheck::Unknown { first };
        }
        if fits(free_bytes, total) {
            return SpaceCheck::Fits;
        }
        let first = !self.no_space_named;
        self.no_space_named = true;
        self.back_off(now_ns);
        SpaceCheck::NoSpace { first }
    }

    /// The first candidate not yet tried for the target; `None` when every
    /// one has been (or there are none) — `no_holder`: the list starts over
    /// after the 30 s ceiling (plan ruling P10).
    pub fn pick(&mut self, candidates: &[NodeId], now_ns: u64) -> Option<NodeId> {
        match candidates.iter().copied().find(|c| !self.tried.contains(c)) {
            Some(c) => {
                self.tried.push(c);
                Some(c)
            }
            None => {
                self.tried.clear();
                self.backoff_ns = AUTO_FETCH_BACKOFF_MAX_NS;
                self.next_attempt_ns = now_ns.saturating_add(AUTO_FETCH_BACKOFF_MAX_NS);
                None
            }
        }
    }

    /// Spec §6: `ok` resets the ladder; `refused`/`timeout` back off (the next
    /// attempt picks the next untried candidate).
    pub fn on_result(&mut self, o: Outcome, now_ns: u64) {
        match o {
            Outcome::Ok => {
                self.backoff_ns = 0;
                self.tried.clear();
                self.next_attempt_ns = now_ns;
            }
            Outcome::Refused | Outcome::Timeout => self.back_off(now_ns),
            Outcome::NoSpace | Outcome::NoHolder => {}
        }
    }

    /// Plan ruling P12: `true` the first time per set.
    pub fn first_thin(&mut self) -> bool {
        let first = !self.thin_named;
        self.thin_named = true;
        first
    }

    fn back_off(&mut self, now_ns: u64) {
        self.backoff_ns = if self.backoff_ns == 0 {
            AUTO_FETCH_BACKOFF_MIN_NS
        } else {
            (self.backoff_ns * 2).min(AUTO_FETCH_BACKOFF_MAX_NS)
        };
        self.next_attempt_ns = now_ns.saturating_add(self.backoff_ns);
    }
}
```

and `pub mod auto_fetch;` in `uc_node/src/lib.rs`.

- [ ] **Step 4: Implement the catalog helpers.** In `uc_node/src/catalog.rs` (add `use uc_protocol::v2::upgrade::SnapshotReport;` and `use uc_protocol::v2::cnc::CNC_MAX_SERVICES;`):

```rust
/// Plan ruling P1: whom to ask for a set, in order. Known holders first —
/// `live_holders` (the leader's soft table; empty on a follower, which has
/// none) and `builders` ([`builders_at`]) — then every other member; each
/// tier learners first, then lowest node id (spec §6). Never `self_id`, no
/// duplicates. A member that turns out not to hold the set answers nothing
/// and costs one fetch timeout.
pub fn fetch_candidates(
    self_id: NodeId,
    live_holders: &[NodeId],
    builders: &[NodeId],
    learners: &[NodeId],
    voters: &[NodeId],
) -> Vec<NodeId> {
    let order = |v: &mut Vec<NodeId>| {
        v.sort_unstable();
        v.dedup();
        v.sort_by_key(|id| (!learners.contains(id), *id));
    };
    let mut known: Vec<NodeId> = live_holders
        .iter()
        .chain(builders)
        .copied()
        .filter(|&id| id != self_id)
        .collect();
    order(&mut known);
    let mut rest: Vec<NodeId> = learners
        .iter()
        .chain(voters)
        .copied()
        .filter(|id| *id != self_id && !known.contains(id))
        .collect();
    order(&mut rest);
    known.extend(rest);
    known
}

/// Plan ruling P1: the nodes that BUILT the set at `p` — reporters, in the
/// committed `SnapshotReport` records at `p`, whose hash equals the catalog's
/// row hash there. Replicated, so a follower knows them too. Empty when the
/// catalog does not list `p` or the records have moved past it.
pub fn builders_at(reports: &[SnapshotReport], sets: &[SetEntry], p: u64) -> Vec<NodeId> {
    let Some(e) = sets.iter().find(|e| e.position == p) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for r in reports
        .iter()
        .filter(|r| r.position == p && (r.row as usize) < CNC_MAX_SERVICES)
    {
        let want = e.rows[r.row as usize].hash;
        for &(id, h, _) in &r.hashes {
            if h == want && !out.contains(&id) {
                out.push(id);
            }
        }
    }
    out
}

/// Review focus 5 / plan ruling P12: how many nodes reported the instant at
/// `p` (the widest committed row record there); `0` when none is held.
pub fn reporters_at(reports: &[SnapshotReport], p: u64) -> usize {
    reports
        .iter()
        .filter(|r| r.position == p)
        .map(|r| r.hashes.len())
        .max()
        .unwrap_or(0)
}
```

- [ ] **Step 5: `SOURCE_AUTO`.** In `audit.rs` after `SOURCE_GENESIS`:

```rust
/// [`AuditRecord::source`] (and `actor`) for a `snapshot_fetch` the node
/// issued on its own — snapshot-lifecycle spec §6's background auto-fetch.
pub const SOURCE_AUTO: &str = "auto";
```

- [ ] **Step 6: The fetch issue path.** In `node.rs`: `PendingFetch` gains

```rust
    /// Snapshot-lifecycle spec §6: issued by auto-fetch (its outcome feeds
    /// `uc2_snapshot_auto_fetch_total`), not by `uc2ctl snapshot fetch`.
    auto: bool,
```

`start_fetch` keeps its learner door and delegates:

```rust
    fn start_fetch(&mut self, learner_id: NodeId, position: u64) -> Result<(), FetchRefusal> {
        if learner_id == self.id || !self.cluster_view.membership().is_learner(learner_id) {
            return Err(FetchRefusal::NotALearner);
        }
        self.issue_fetch(learner_id, position, false)
    }

    /// The body `start_fetch` always had, minus its learner door: auto-fetch
    /// may pull from any member (plan ruling P1) — the source's sender serves
    /// a `SNAP_REQUEST` whatever its role.
    fn issue_fetch(&mut self, from: NodeId, position: u64, auto: bool) -> Result<(), FetchRefusal> {
        if self.pending_fetch.is_some() {
            return Err(FetchRefusal::Retry);
        }
        if position > self.cnc.counters().durable.load_acquire() {
            return Err(FetchRefusal::AboveDurable);
        }
        let Some(&peer) = self.id_to_addr.get(&from) else {
            return Err(FetchRefusal::UnknownPeer);
        };
        if self
            .fetch_tx
            .try_send(SnapFetch {
                peer,
                position,
                mode: IntakeMode::StoreOnly,
            })
            .is_err()
        {
            return Err(FetchRefusal::Retry);
        }
        self.pending_fetch = Some(PendingFetch {
            learner: from,
            position,
            stored_before: self.stored_set_pos.load(Ordering::Acquire),
            deadline_ns: self.pass_now_ns.saturating_add(FETCH_TIMEOUT_NS),
            auto,
        });
        crate::obs_event!(
            Info,
            "snapshot_fetch_requested",
            node = self.id as u64,
            from = from as u64,
            position = position,
            actor = if auto { "auto" } else { "operator" }
        );
        Ok(())
    }
```

(keep `start_fetch`'s existing doc comment and its "A VOTER pulling a learner's set is not a joiner" comment on the `mode` line inside `issue_fetch`). In `poll_pending_fetch` add, inside the landed branch after `self.pending_fetch = None;`:

```rust
            if p.auto {
                self.auto_fetch_stats.bump(crate::auto_fetch::Outcome::Ok);
                self.auto_fetch.on_result(crate::auto_fetch::Outcome::Ok, self.pass_now_ns);
            }
```

and inside the deadline branch:

```rust
            if p.auto {
                self.auto_fetch_stats.bump(crate::auto_fetch::Outcome::Timeout);
                self.auto_fetch.on_result(crate::auto_fetch::Outcome::Timeout, self.pass_now_ns);
            }
```

- [ ] **Step 7: The soft table on any node, and the space seam.** Add beside `addr_of`:

```rust
/// Catalog spec §5.3: a `SoftTable` from the sender's address-keyed `STATUS`
/// map over `membership`. Only a LEADER's map is filled (`STATUS` goes to the
/// leader), so on a follower this is empty — plan ruling P1's reason for the
/// builders tier. Shared by `Node::soft_table` and the auto-fetch holder
/// choice.
fn soft_table_from_wire(
    wire: &SoftTableWire,
    membership: &uc_consensus::config::ClusterConfig,
) -> crate::catalog::SoftTable {
    let mut t = crate::catalog::SoftTable::default();
    for (id, addr) in membership.voters.iter().chain(membership.learners.iter()) {
        if let Some((h, at)) = wire.get(&addr_of(*addr)) {
            t.record(*id, *h, *at);
        }
    }
    t
}
```

and `Node::soft_table` becomes:

```rust
    pub fn soft_table(&self) -> crate::catalog::SoftTable {
        let membership = self.cluster_view.snapshot_inner().membership;
        let wire = self
            .soft_wire
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let mut t = soft_table_from_wire(&wire, &membership);
        // This node's own cache, stamped now: a node never sends itself a
        // `STATUS`, but it is as much a holder as any follower.
        let own = *self.holdings.lock().unwrap_or_else(|e| e.into_inner());
        t.record(self.node_id, own, unix_now_ns());
        t
    }
```

`HoldingsProbe` gains `free_override: Option<Arc<AtomicU64>>,` (`None` in `new`) and:

```rust
    /// Plan ruling P14: a test's stand-in for `statvfs` (`0` = off).
    fn with_free_override(mut self, cell: Arc<AtomicU64>) -> Self {
        self.free_override = Some(cell);
        self
    }
```

and in `probe` the free reading becomes:

```rust
        let free = match self.free_override.as_ref().map(|c| c.load(Ordering::Acquire)) {
            Some(v) if v != 0 => Some(v),
            _ => crate::preflight::free_disk_bytes(&self.instance_dir),
        };
```

In `Node::start`, before the probe is built: `let free_override = Arc::new(AtomicU64::new(0)); let auto_fetch_stats = Arc::new(crate::auto_fetch::AutoFetchStats::default());`; chain `.with_free_override(Arc::clone(&free_override))` after `.with_report_seeder(..)`; store both on `Node` (`free_override: Arc<AtomicU64>`, `auto_fetch_stats: Arc<crate::auto_fetch::AutoFetchStats>`), and add:

```rust
    /// Plan ruling P14 — TEST SEAM, not API: make this node's `uc2-holdings`
    /// probe report `v` free bytes instead of `statvfs` (`0` restores the real
    /// reading). Lets an end-to-end test exercise the spec §7.3 space check
    /// without filling a disk.
    #[doc(hidden)]
    pub fn set_free_bytes_for_test(&self, v: u64) {
        self.free_override.store(v, Ordering::Release);
    }
```

- [ ] **Step 8: The consensus-agent integration.** `Consensus` gains (both struct literals: `auto_fetch: crate::auto_fetch::AutoFetch::new(cfg.id)` — in the harness literal the harness's node id — `auto_fetch_stats: Arc::clone(&auto_fetch_stats)` / `Arc::new(Default::default())`, `soft_wire: Arc::clone(&soft_wire)` / `Arc::new(Mutex::new(SoftTableWire::new()))`, `soft_stale_ns: SOFT_STALE_FACTOR * cfg.election_timeout_max_ns`):

```rust
    /// Snapshot-lifecycle spec §6: the background fetch decision.
    auto_fetch: crate::auto_fetch::AutoFetch,
    /// `uc2_snapshot_auto_fetch_total{outcome}` — shared with `ObsSources`.
    auto_fetch_stats: Arc<crate::auto_fetch::AutoFetchStats>,
    /// The sender's `STATUS` map — a LEADER's live holders (plan ruling P1).
    soft_wire: Arc<Mutex<SoftTableWire>>,
    /// [`Node::soft_stale_ns`]'s value, for the holder query.
    soft_stale_ns: u64,
```

Methods:

```rust
    /// Snapshot-lifecycle spec §6: fetch the newest agreed set this node does
    /// not hold, in the background. Steady path (switch off, a fetch in
    /// flight, nothing agreed, waiting out a delay, or already held): a few
    /// loads and compares.
    #[inline]
    fn maybe_auto_fetch(&mut self) {
        if !self.cluster_view.auto_fetch.load(Ordering::Acquire) || self.pending_fetch.is_some() {
            return;
        }
        let n = self
            .cluster_view
            .catalog_agreed_position
            .load(Ordering::Acquire);
        if n == 0 || self.auto_fetch.quiet(n, self.pass_now_ns) || self.holdings_held.contains(&n) {
            return;
        }
        self.auto_fetch_step(n);
    }

    #[cold]
    #[inline(never)]
    fn auto_fetch_step(&mut self, n: u64) {
        use crate::auto_fetch::{Outcome, SpaceCheck};
        let now = self.pass_now_ns;
        let durable = self.cnc.counters().durable.load_acquire();
        let inner = self.cluster_view.snapshot_inner();
        let building = self.local_build_pending(n, &inner);
        if !self.auto_fetch.due(n, durable, building, now) {
            return;
        }
        let total = self
            .cluster_view
            .catalog_newest_agreed_bytes
            .load(Ordering::Acquire);
        let free = self
            .holdings
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .free_bytes;
        match self.auto_fetch.check_space(total, free, now) {
            SpaceCheck::NoSpace { first } => {
                self.auto_fetch_stats.bump(Outcome::NoSpace);
                if first {
                    crate::obs_event!(
                        Warn,
                        "snapshot_fetch_skipped_no_space",
                        node = self.id as u64,
                        position = n,
                        bytes = total,
                        free_bytes = free
                    );
                }
                return;
            }
            SpaceCheck::Unknown { first: true } => {
                crate::obs_event!(
                    Info,
                    "snapshot_fetch_size_unknown",
                    node = self.id as u64,
                    position = n
                );
            }
            SpaceCheck::Unknown { first: false } | SpaceCheck::Fits => {}
        }
        let candidates = self.auto_fetch_candidates(n, &inner);
        let Some(from) = self.auto_fetch.pick(&candidates, now) else {
            self.auto_fetch_stats.bump(Outcome::NoHolder);
            crate::obs_event!(
                Info,
                "snapshot_fetch_no_holder",
                node = self.id as u64,
                position = n
            );
            return;
        };
        if crate::catalog::reporters_at(&inner.reports, n) == 1 && self.auto_fetch.first_thin() {
            crate::obs_event!(
                Warn,
                "snapshot_fetch_single_reporter",
                node = self.id as u64,
                position = n
            );
        }
        match self.issue_fetch(from, n, true) {
            Ok(()) => self.audit_auto_fetch(from, n),
            Err(_) => {
                self.auto_fetch_stats.bump(Outcome::Refused);
                self.auto_fetch.on_result(Outcome::Refused, now);
            }
        }
    }

    /// Plan ruling P8: will this node build the set at `n` itself? A standby
    /// set on a voter — never; otherwise yes while any ATTACHED declared row
    /// has neither frozen at `n` nor applied past it.
    fn local_build_pending(&self, n: u64, inner: &ClusterViewInner) -> bool {
        let standby = inner
            .catalog
            .iter()
            .find(|e| e.position == n)
            .is_some_and(|e| e.kind == SetKind::Standby);
        if standby && !inner.membership.is_learner(self.id) {
            return false;
        }
        self.services.ids().any(|row| {
            let s = self.cnc.service_slot(row as usize);
            s.status.load_acquire() & CNC_SVC_STATUS_ATTACHED != 0
                && s.snapshot_pos.load_acquire() < n
                && s.applied.load_acquire() < n
        })
    }

    /// Plan ruling P1's candidate list for the set at `n`.
    fn auto_fetch_candidates(&self, n: u64, inner: &ClusterViewInner) -> Vec<NodeId> {
        let wire = self
            .soft_wire
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let soft = soft_table_from_wire(&wire, &inner.membership);
        let q = crate::catalog::CatalogQuery {
            sets: &inner.catalog,
            catalog_position: self.cluster_view.catalog_version.load(Ordering::Acquire),
            soft: &soft,
            now_ns: unix_now_ns(),
            stale_ns: self.soft_stale_ns,
        };
        let live = q.holders(n);
        let builders = crate::catalog::builders_at(&inner.reports, &inner.catalog, n);
        let learners: Vec<NodeId> = inner.membership.learners.iter().map(|(id, _)| *id).collect();
        let voters: Vec<NodeId> = inner.membership.voters.iter().map(|(id, _)| *id).collect();
        crate::catalog::fetch_candidates(self.id, &live, &builders, &learners, &voters)
    }

    /// Spec §6: the existing `snapshot_fetch` record (op 9), `actor = "auto"`
    /// — written AFTER the issue, like `audit_datagram_mtu`, for its reason:
    /// no request is waiting on an answer. `id` names the holder asked,
    /// `config_version` the position.
    fn audit_auto_fetch(&mut self, from: NodeId, position: u64) {
        let rec = AuditRecord {
            ts_ns: crate::obs::metrics::now_unix_ns(),
            actor: crate::audit::SOURCE_AUTO,
            origin: AuditOrigin::Local,
            op: 9,
            op_name: op_name(9),
            id: from,
            addr: None,
            seq: 0,
            nonce: 0,
            outcome: AuditOutcome::Accepted,
            reason: 0,
            config_version: position,
            detail: None,
            source: crate::audit::SOURCE_AUTO,
        };
        if let Err(e) = self.audit.record(&rec) {
            let err = e.to_string();
            crate::obs_event!(
                Error,
                "admin_audit_failed",
                node = self.id as u64,
                seq = 0u64,
                nonce = 0u64,
                op = 9u64,
                status = 0u64,
                err = err.as_str(),
            );
        }
    }
```

(Import `SetKind` and `CNC_SVC_STATUS_ATTACHED` where `node.rs` already imports catalog and cnc names.) In `do_work`, after `self.poll_pending_fetch();`:

```rust
        // Snapshot-lifecycle spec §6: background auto-fetch of the newest
        // agreed set — a few loads on the steady path.
        self.maybe_auto_fetch();
```

- [ ] **Step 9: Keep the manual-fetch tests on the manual path.** These tests pin behaviour from before auto-fetch existed (a voter holding nothing until an operator fetches); run them with the switch off:
  - `uc_node/tests/learner.rs`: every `settings_genesis: uc_protocol::v2::settings::Settings::genesis_default(),` becomes

```rust
        // Snapshot lifecycle: this suite pins the MANUAL §5.7 fetch path —
        // auto-fetch is `catalog.rs`'s to test.
        settings_genesis: uc_protocol::v2::settings::Settings {
            auto_fetch: false,
            ..uc_protocol::v2::settings::Settings::genesis_default()
        },
```

  - `uc_node/tests/catalog.rs`: in `a_killed_node_leaves_holders_after_the_stale_timeout` and `learner_only_voters_do_not_purge_until_they_fetch`, replace `opts("…", …)` with

```rust
        Opts {
            settings: Settings {
                auto_fetch: false,
                ..Settings::genesis_default()
            },
            ..opts("catalog-learner-only", true)
        }
```

  (keeping each test's own app name and purge flag).

- [ ] **Step 10: Run, expect PASS.**
Run: `cargo test -p uc_node --lib && cargo test -p uc_node --test learner -- --test-threads=1 && cargo test -p uc_node --test catalog -- --test-threads=1`
Expected: PASS.

- [ ] **Step 11: Commit.**

```bash
cargo fmt --all
git commit -am "node(lifecycle): background auto-fetch of the newest agreed set, with holder choice, backoff and the space check"
```

---

### Task 10: Metrics and the `Uc2SnapshotWontFit` alert

**Files:**
- Modify: `uc_node/src/obs/mod.rs` (`ObsSources` gains `snapshot_auto_fetch` ~:140; `for_tests` ~:233)
- Modify: `uc_node/src/obs/metrics.rs` (`CONTRACT_SERIES` ~:86–92; render beside `uc2_snapshot_fetched_position` ~:889; the four `ObsSources { .. }` test literals ~:1781/2464/2571/2671; the count test ~:2045 122 → 124; a new render test)
- Modify: `uc_node/src/node.rs` (`Node::observability` ~:3066)
- Modify: `uc_node/tests/obs_http.rs` (~:107), `uc_node/examples/m10_alerts.rs` (`synthetic_sources_named` ~:431; new scenario `snapshot_wont_fit`; `ALL_SCENARIOS`; dispatch)
- Modify: `packaging/prometheus/uc2-alerts.yml` (after `Uc2SnapshotSetDiverged` ~:361), `scripts/m10_alert_fire.sh` (RULES table ~:279; builder; RULE_BUILDERS ~:808)
- Docs: `docs/how-to/monitor-a-cluster.md` (family count :70; series table near :443; the alert section near :501; the alert table near :647)

**Interfaces:**
- Consumes: Task 9 `AutoFetchStats`, `Outcome::ALL`/`label`; Task 4 `ClusterView.catalog_newest_agreed_bytes`.
- Produces: `ObsSources.snapshot_auto_fetch: Arc<crate::auto_fetch::AutoFetchStats>`; series `uc2_snapshot_auto_fetch_total{outcome}` and `uc2_snapshot_newest_agreed_bytes`.

- [ ] **Step 1: Write the failing tests.** In `metrics.rs` `mod tests`: change the count to `124` in `the_contract_has_the_number_of_families_the_docs_state`, and add:

```rust
    /// Snapshot-lifecycle spec §6/§7.4: the auto-fetch counter renders all
    /// five outcomes (zero included — an absent label is not alertable) and
    /// the gauge reads the view's newest-agreed-bytes word.
    #[test]
    fn auto_fetch_outcomes_and_the_newest_agreed_bytes_render() {
        let src = synthetic_sources();
        src.snapshot_auto_fetch.bump(crate::auto_fetch::Outcome::NoSpace);
        src.cluster_view
            .catalog_newest_agreed_bytes
            .store(4096, Ordering::Release);
        let text = render_prometheus(&src);
        for (label, v) in [("ok", 0), ("refused", 0), ("timeout", 0), ("no_space", 1), ("no_holder", 0)] {
            assert!(
                text.contains(&format!("\nuc2_snapshot_auto_fetch_total{{outcome=\"{label}\"}} {v}\n")),
                "{label}: {text}"
            );
        }
        assert!(text.contains("# TYPE uc2_snapshot_auto_fetch_total counter"), "{text}");
        assert!(text.contains("\nuc2_snapshot_newest_agreed_bytes 4096\n"), "{text}");
    }
```

(`publish` overwrites the word from the state, so the test stores into the atomic AFTER building the sources and never republishes.) And, pinning review focus 4 on the shipped rule:

```rust
    /// Review focus 4: a set of unknown size (gauge 0) never fires
    /// Uc2SnapshotWontFit — the shipped rule carries the `> 0` guard.
    #[test]
    fn the_wont_fit_rule_needs_a_known_size() {
        let rules = include_str!("../../../packaging/prometheus/uc2-alerts.yml");
        let at = rules
            .find("alert: Uc2SnapshotWontFit")
            .expect("Uc2SnapshotWontFit ships");
        let end = rules[at..].find("annotations:").map_or(rules.len(), |e| at + e);
        assert!(
            rules[at..end].contains("uc2_snapshot_newest_agreed_bytes > 0"),
            "{}",
            &rules[at..end]
        );
    }
```

- [ ] **Step 2: Run, expect FAIL.**
Run: `cargo test -p uc_node --lib metrics`
Expected: FAIL — `no field snapshot_auto_fetch on ObsSources`; then the count `122 != 124`; `the_wont_fit_rule_needs_a_known_size` panics "Uc2SnapshotWontFit ships".

- [ ] **Step 3: Implement.** `ObsSources` gains, after `snapshot_fetched_position`:

```rust
    /// Snapshot-lifecycle spec §6: `uc2_snapshot_auto_fetch_total{outcome}`,
    /// the SAME allocation the consensus agent bumps.
    pub snapshot_auto_fetch: Arc<crate::auto_fetch::AutoFetchStats>,
```

`for_tests` and every `ObsSources { .. }` literal (metrics tests ×4, `obs_http.rs`, `m10_alerts.rs`) add `snapshot_auto_fetch: Arc::new(Default::default()),`; `Node::observability` adds `snapshot_auto_fetch: Arc::clone(&self.auto_fetch_stats),`. `CONTRACT_SERIES` gains, after `"uc2_snapshot_fetched_position",`:

```rust
    // Snapshot-lifecycle spec §6/§7.4: background auto-fetch and the
    // newest agreed set's size.
    "uc2_snapshot_auto_fetch_total",
    "uc2_snapshot_newest_agreed_bytes",
```

Render, right after the `uc2_snapshot_fetched_position` gauge:

```rust
    let auto_fetch_samples: Vec<(String, u64)> = crate::auto_fetch::Outcome::ALL
        .iter()
        .map(|o| (format!("outcome=\"{}\"", o.label()), s.snapshot_auto_fetch.get(*o)))
        .collect();
    push_labeled(
        out,
        "uc2_snapshot_auto_fetch_total",
        "Background fetches of the newest agreed snapshot set, by outcome (snapshot-lifecycle spec §6): ok (landed), refused (could not be issued), timeout (no answer in 60 s), no_space (skipped: the set would not fit with headroom — free < size + max(size/4, 1 GiB)), no_holder (every candidate tried; retried after 30 s).",
        "counter",
        &auto_fetch_samples,
    );
    push_gauge(
        out,
        "uc2_snapshot_newest_agreed_bytes",
        "The newest AGREED snapshot set's total size in bytes — its rows' and cluster artifact's (snapshot-lifecycle spec §7.4); 0 when no set is agreed or its size is unknown (a set catalogued before sizes existed). Alert: Uc2SnapshotWontFit.",
        s.cluster_view
            .catalog_newest_agreed_bytes
            .load(Ordering::Acquire),
    );
```

(If the render function's buffer is named `&mut out` at that point rather than `out`, match the surrounding calls.)

- [ ] **Step 4: The alert.** In `packaging/prometheus/uc2-alerts.yml`, after `Uc2SnapshotSetDiverged`:

```yaml
  - alert: Uc2SnapshotWontFit
    # Snapshot-lifecycle spec §7.4: the newest agreed snapshot set would fail
    # the auto-fetch space check on this node — free < size + max(size/4,
    # 1 GiB). Fires on EVERY node (learners that build rather than fetch, and
    # nodes with auto_fetch off, included): it warns before any download, and
    # a node that cannot hold the newest set cannot purge below it either.
    # The `> 0` guard keeps a set of unknown size (catalogued before sizes
    # existed) from alarming.
    expr: |
      uc2_snapshot_newest_agreed_bytes > 0
      and
      uc2_free_disk_bytes < uc2_snapshot_newest_agreed_bytes
        + clamp_min(uc2_snapshot_newest_agreed_bytes / 4, 1073741824)
    for: 5m
    labels: { severity: warning }
    annotations: { summary: "{{ $labels.instance }} cannot fit the newest agreed snapshot set with headroom — its auto-fetch is skipped (uc2_snapshot_auto_fetch_total{outcome=\"no_space\"}) and it will not purge below that set; free disk or shrink the state" }
```

In `scripts/m10_alert_fire.sh` add to the rules table after `Uc2SnapshotSetDiverged`:

```python
    "Uc2SnapshotWontFit": {"severity": "warning", "real": False, "scenario": "snapshot_wont_fit"},
```

the builder (beside `build_Uc2DiskLow`, which is the same two-series shape):

```python
def build_Uc2SnapshotWontFit():
    # Snapshot-lifecycle spec §7.4: two unlabeled per-node gauges, the
    # build_Uc2DiskLow shape — the newest agreed set's size held > 0 and the
    # free bytes held below size + max(size/4, 1 GiB).
    rows = load_scenario("snapshot_wont_fit")
    size_row = select(rows, "uc2_snapshot_newest_agreed_bytes", {})
    free_row = select(rows, "uc2_free_disk_bytes", {})
    r = new_rule("warning", labels_from=size_row)
    add_hold_last(r, size_row, "uc2_snapshot_newest_agreed_bytes", 300)
    add_hold_last(r, free_row, "uc2_free_disk_bytes", 300)
    r["eval_time"] = total_for(300)[0]
    return r
```

and `"Uc2SnapshotWontFit": build_Uc2SnapshotWontFit,` in `RULE_BUILDERS`. In `uc_node/examples/m10_alerts.rs` add `"snapshot_wont_fit",` to `ALL_SCENARIOS` after `"snapshot_set_diverged"`, the dispatch arm `"snapshot_wont_fit" => scenario_snapshot_wont_fit(),`, and:

```rust
/// Uc2SnapshotWontFit — **synthetic, disclosed**. Snapshot-lifecycle spec
/// §7.4: one synthetic `ObsSources` whose cluster view lists an AGREED set
/// whose rows and cluster artifact total 4 GiB, on a node reporting 2 GiB
/// free — below 4 GiB + max(1 GiB, 1 GiB). Producing it for real needs a
/// multi-gigabyte set on a nearly full disk, out of proportion to this
/// rule's share of the harness.
fn scenario_snapshot_wont_fit() -> (SeriesFile, Disclosure) {
    use uc_protocol::v2::catalog::{RowEntry, RowVerdict, SetEntry, SetKind, SetState};
    const GIB: u64 = 1 << 30;
    let src = synthetic_sources(0);
    let mut st = src.cluster_view.to_state();
    let mut set = SetEntry::commanded(8192, SetKind::Full, 0);
    set.state = SetState::Complete;
    set.rows[0] = RowEntry { version: 0, hash: 1, verdict: RowVerdict::Agreed, size: 4 * GIB - 4096 };
    set.cluster = RowEntry { version: 0, hash: 2, verdict: RowVerdict::Agreed, size: 4096 };
    st.catalog.push(set);
    st.applied = 8192;
    src.cluster_view.publish(&st);
    src.cnc.store_free_disk_bytes(2 * GIB);

    let srv = ObsServer::serve(src.clone(), "127.0.0.1:0".parse().unwrap()).expect("bind");
    let addr = srv.local_addr();
    let mut sf = SeriesFile::new();
    for _ in 0..3 {
        sf.record_round(
            "n0",
            &scrape(addr),
            &["uc2_snapshot_newest_agreed_bytes", "uc2_free_disk_bytes"],
        );
        thread::sleep(Duration::from_millis(200));
    }
    srv.stop();
    (
        sf,
        Disclosure {
            scenario: "snapshot_wont_fit",
            rules: &["Uc2SnapshotWontFit"],
            state: "synthetic",
            method: "one synthetic ObsSources whose cluster view lists an AGREED set at 8192 \
                     totalling 4 GiB (row 0 + cluster artifact) on a node reporting 2 GiB \
                     free; the exporter renders uc2_snapshot_newest_agreed_bytes from the \
                     real ClusterView::publish — Uc2SnapshotWontFit's size > 0 and free < \
                     size + max(size/4, 1 GiB) predicate."
                .into(),
        },
    )
}
```

Regenerate the captured scenario the way the existing ones were (`cargo run -p uc_node --example m10_alerts -- …` per the script's header) and run `scripts/m10_alert_fire.sh` — it fails fast if a shipped alert has no builder.

- [ ] **Step 5: Docs.** `docs/how-to/monitor-a-cluster.md`: the family count `122` → `124`; add two rows to the snapshot series table:

```markdown
| `uc2_snapshot_auto_fetch_total` | counter | `outcome` | background fetches of the newest agreed set: `ok`, `refused`, `timeout`, `no_space`, `no_holder` (snapshot lifecycle). A rising `no_space` means this node cannot hold the newest set — see `Uc2SnapshotWontFit`. Flat at 0 with `[settings] auto_fetch = false` |
| `uc2_snapshot_newest_agreed_bytes` | gauge | none | the newest agreed set's total size; 0 when none is agreed or its size is unknown (a set catalogued before sizes) |
```

an alert paragraph after the `Uc2SnapshotSetDiverged` section:

```markdown
`Uc2SnapshotWontFit` (warning, `for: 5m`): the newest agreed snapshot set
would not fit on this node with headroom — `uc2_free_disk_bytes` below its
size plus `max(size / 4, 1 GiB)`. It fires on every node, learners and
`auto_fetch = false` nodes included, BEFORE any download: auto-fetch skips
the set (`outcome="no_space"`, and one `snapshot_fetch_skipped_no_space` log
record per set), so the node does not purge below it. Free disk, or shrink
the state. A set of unknown size never fires it.
```

and the row `| Uc2SnapshotWontFit (snapshot lifecycle) | the newest agreed set would not fit with headroom on this node, for 5m — free disk | warning |` in the alert table.

- [ ] **Step 6: Run, expect PASS.**
Run: `cargo test -p uc_node --lib metrics && cargo test -p uc_node --test obs_http && cargo build -p uc_node --examples && scripts/m10_alert_fire.sh`
Expected: PASS; the script reports every rule (including `Uc2SnapshotWontFit`) firing under promtool.

- [ ] **Step 7: Commit.**

```bash
cargo fmt --all
git commit -am "obs(lifecycle): uc2_snapshot_auto_fetch_total, uc2_snapshot_newest_agreed_bytes, Uc2SnapshotWontFit"
```

---

### Task 11: The pin door — refusal 61 `pin_origin_not_agreed`

**Files:**
- Modify: `uc_node/src/node.rs` (constants :585–596; `apply_upgrade_pin` after the 54 check ~:10254; tests `upgrade_pin_door_refusals_by_name` ~:19306 and the band assertion ~:19596)
- Modify: `uc_node/src/lib.rs` (re-export list ~:87)
- Modify: `uc_ctl/src/main.rs` (`reason_str` ~:726; its test ~:1803)
- Modify (retry helpers): `uc_node/tests/catalog.rs` `pin` (~:748), `uc_node/tests/row_version.rs` (~:964), `uc_service/tests/pinned_attach.rs` (~:489), `uc_diffreplay/src/live.rs` (~:222)
- Docs: `docs/reference/uc2ctl.md` refusal table (~:880), `docs/how-to/upgrade-an-application.md` (step 2/3 and its refusal table ~:298), `docs/notes/uc2-cluster-fsm-explained.md` (table ~:485), `docs/reference/application-sdlc.md` (~:82)
- Test: `node.rs`, `uc_ctl/src/main.rs` `mod tests`

**Interfaces:**
- Produces: `pub const REASON_PIN_ORIGIN_NOT_AGREED: u32 = 61;` (re-exported from `uc_node`).

- [ ] **Step 1: Write the failing tests.** In `node.rs` `mod tests`, after `upgrade_pin_door_refusals_by_name`:

```rust
    /// Snapshot-lifecycle spec §8: a pin's origin must be an AGREED catalog
    /// entry (61), except on an Empty catalog, where the pin is allowed as
    /// before. Checked after 54, so a missing set still reads 54.
    #[test]
    fn upgrade_pin_origin_must_be_agreed_unless_the_catalog_is_empty() {
        use uc_protocol::v2::catalog::{SetKind, SetState};
        let mut h = harness_with_rows(&["a"]);
        drive_to_serving_leader(&mut h);
        h.cons
            .cnc
            .service_slot(0)
            .status
            .store_version(pack_version(1, 0, 0));
        h.cons.snapshot_set_position.store(4096, Ordering::Release);
        let pin = UpgradePin {
            row: 0,
            from: pack_version(1, 0, 0),
            to: pack_version(1, 1, 0),
            origin: 4096,
        };
        // A catalog with a Complete set elsewhere and 4096 still Commanded.
        let mut st = h.cons.cluster_view.to_state();
        let mut commanded = uc_protocol::v2::catalog::SetEntry::commanded(4096, SetKind::Full, 0);
        commanded.state = SetState::Commanded;
        st.catalog = vec![agreed_entry(2048), commanded];
        h.cons.cluster_view.publish(&st);
        stage_pin_for_test(&h, &pin);
        assert_eq!(
            sr(h.cons.apply_upgrade_pin_staged()),
            (1, REASON_PIN_ORIGIN_NOT_AGREED),
            "complete here, not yet agreed"
        );
        // Agreed now: accepted.
        st.catalog = vec![agreed_entry(2048), agreed_entry(4096)];
        h.cons.cluster_view.publish(&st);
        stage_pin_for_test(&h, &pin);
        assert_eq!(sr(h.cons.apply_upgrade_pin_staged()).0, 0, "agreed origin accepted");
    }

    #[test]
    fn upgrade_pin_on_an_empty_catalog_is_allowed_unchecked() {
        let mut h = harness_with_rows(&["a"]);
        drive_to_serving_leader(&mut h);
        h.cons
            .cnc
            .service_slot(0)
            .status
            .store_version(pack_version(1, 0, 0));
        h.cons.snapshot_set_position.store(4096, Ordering::Release);
        stage_pin_for_test(
            &h,
            &UpgradePin {
                row: 0,
                from: pack_version(1, 0, 0),
                to: pack_version(1, 1, 0),
                origin: 4096,
            },
        );
        assert_eq!(sr(h.cons.apply_upgrade_pin_staged()).0, 0);
    }
```

Extend the band assertion: add `REASON_PIN_ORIGIN_NOT_AGREED` to the tuple and `61` to the expected tuple. In `uc_ctl/src/main.rs` `mod tests`:

```rust
    /// Snapshot-lifecycle spec §8: 61 names the not-yet-agreed origin and
    /// tells the operator to retry.
    #[test]
    fn reason_str_names_61() {
        assert!(reason_str(61).starts_with("pin_origin_not_agreed"), "{}", reason_str(61));
        assert!(reason_str(61).contains("retry"));
    }
```

- [ ] **Step 2: Run, expect FAIL.**
Run: `cargo test -p uc_node --lib upgrade_pin && cargo test -p uc_ctl reason_str_names_61`
Expected: FAIL to compile — `cannot find value REASON_PIN_ORIGIN_NOT_AGREED`; `reason_str(61)` is `"unknown/malformed"`.

- [ ] **Step 3: Implement.** In `node.rs` after `REASON_VERSION_ALREADY_SET`:

```rust
/// Snapshot-lifecycle spec §8: the pin's origin is not an AGREED catalog
/// entry — the set may still be collecting reports (up to ~5 s after the
/// instant, `SNAP_REPORT_TIMEOUT_NS`), so `uc2ctl` says to retry. DOOR-ONLY
/// (plan ruling P13): an Empty catalog (no `Complete` entry, catalog ruling
/// R26) cannot check agreement, and the pin is then allowed with a log line.
pub const REASON_PIN_ORIGIN_NOT_AGREED: u32 = 61;
```

In `apply_upgrade_pin`, right after the 54 check:

```rust
        // 61 (snapshot-lifecycle spec §8): the origin must be AGREED — a pin
        // is a one-way door, and an unverified or diverged origin would spread
        // a possibly bad state to every node. An Empty catalog cannot answer;
        // the pin is allowed as before, and named.
        if state.catalog_empty() {
            crate::obs_event!(
                Info,
                "upgrade_pin_agreement_unchecked",
                node = self.id as u64,
                row = pin.row as u64,
                origin = pin.origin
            );
        } else if !state
            .catalog
            .iter()
            .any(|e| e.position == pin.origin && e.is_agreed())
        {
            return self.refuse_upgrade_pin(REASON_PIN_ORIGIN_NOT_AGREED);
        }
```

Add `REASON_PIN_ORIGIN_NOT_AGREED` to `uc_node/src/lib.rs`'s re-export list. In `uc_ctl/src/main.rs` `reason_str`, after `60 =>`:

```rust
        // Snapshot-lifecycle spec §8: door-only, after 54.
        61 => {
            "pin_origin_not_agreed (the set at --origin is complete here but the catalog has not agreed it yet — reports can take ~5 s after the instant; retry, and check uc2_catalog_agreed_position reaches it)"
        }
```

- [ ] **Step 4: Retry helpers.** Every in-tree pin driver that already retries through 54 retries through 61 too — the window is the same kind (a set published a moment before it is agreed):
  - `uc_node/tests/catalog.rs` `pin`: the assertion becomes `(reason == uc_node::REASON_PIN_NO_SET || reason == uc_node::REASON_PIN_ORIGIN_NOT_AGREED) && Instant::now() < deadline`.
  - `uc_node/tests/row_version.rs`, `uc_service/tests/pinned_attach.rs`, `uc_diffreplay/src/live.rs`: `let racy = resp.status == 2 || resp.reason == uc_node::REASON_PIN_NO_SET || resp.reason == uc_node::REASON_PIN_ORIGIN_NOT_AGREED;` and each doc comment naming "54 `pin_no_set`" adds "or 61 `pin_origin_not_agreed`".

- [ ] **Step 5: Docs.** `docs/reference/uc2ctl.md` refusal table, after 60:

```markdown
| 61 | `pin_origin_not_agreed` — `--origin` is a complete set on this node but the snapshot catalog has not AGREED it (every reporting node's hash must match; reports take up to ~5 s after the instant). Retry once `uc2_catalog_agreed_position` reaches it. A diverged origin never becomes agreed: take a new instant. On a cluster whose catalog is still empty (no complete set yet), the pin is allowed unchecked and the node logs `upgrade_pin_agreement_unchecked` |
```

`docs/how-to/upgrade-an-application.md`: in the "wait for the complete set at P" step add "**and** for the catalog to agree it (`uc2_catalog_agreed_position` reaches P) — the pin refuses `61 pin_origin_not_agreed` until then", and add the row `| 61 | pin_origin_not_agreed | … retry after uc2_catalog_agreed_position reaches P |` to its refusal table. `docs/notes/uc2-cluster-fsm-explained.md`'s refusal table: `| 61 | pin_origin_not_agreed | door | origin must be an agreed catalog set (Empty catalog: allowed, logged) |`. `docs/reference/application-sdlc.md`: "Refusals by name, 52–59" → "52–61", adding "`pin_origin_not_agreed` (61)".

- [ ] **Step 6: Run, expect PASS.**
Run: `cargo test -p uc_node --lib upgrade_pin && cargo test -p uc_ctl && cargo build -p uc_diffreplay && cargo test -p uc_node --test row_version -- --test-threads=1`
Expected: PASS.

- [ ] **Step 7: Commit.**

```bash
cargo fmt --all
git commit -am "node(lifecycle): pin door 61 pin_origin_not_agreed; retry helpers and docs"
```

---
### Task 12: Fuzz seeds for every changed encoding (ruling R45)

**Files:**
- Modify: `fuzz/src/seeds.rs` (`uc_protocol_datagram` ~:63–240, `uc_protocol_cluster_frame` ~:1367, `uc_protocol_cluster_image` ~:1444, `uc_protocol_settings` ~:1689; `uc_node_cluster_artifact` ~:1508 needs no edit — it is built by the real `freeze`, so it regenerates in the v5 layout)
- Modify: `fuzz/corpus/**` (regenerated, committed)

**Interfaces:**
- Consumes: Tasks 1–4 encodings.

- [ ] **Step 1: Add the seeds.** In `uc_protocol_datagram`, before `seeds`'s final return:

```rust
    // Snapshot-lifecycle spec §7.1: the 32-byte SNAP_REPORT body (size @24).
    {
        use uc_protocol::v2::datagram::{
            DGRAM_KIND_SNAP_REPORT, SNAP_REPORT_BODY_LEN, SnapReportBody, write_snap_report_body,
        };
        let mut b = [0u8; SNAP_REPORT_BODY_LEN];
        write_snap_report_body(
            &mut b,
            &SnapReportBody { row: 0, node_id: 2, position: 65536, hash: 0xA1B2, size: 4096 },
        );
        seeds.push(Seed::fixed("22-snap-report", datagram(DGRAM_KIND_SNAP_REPORT, 0, 3, &b)));
    }
```

In `uc_protocol_cluster_frame` add `use uc_protocol::v2::upgrade::{SnapshotReport, encode_snapshot_report};`, build

```rust
    // Snapshot-lifecycle spec §7.1: kind 5 with sized entries.
    let mut report_bytes = Vec::new();
    encode_snapshot_report(
        &SnapshotReport { row: 0, position: 4096, hashes: vec![(1, 0xAA, 512), (2, 0xAA, 512)] },
        &mut report_bytes,
    )
    .expect("a two-entry report encodes");
```

and append `Seed::fixed("08-snapshot-report-sized", prefixed(ClusterKind::SnapshotReport, &report_bytes)),` to the returned vector. In `uc_protocol_cluster_image`, after `v3_running`:

```rust
    // Snapshot-lifecycle spec §7.2: a v5 image whose catalog lists one agreed
    // set with sizes, and the same image re-framed as v4 (version word and
    // CRC only — the leaf treats the blobs as opaque).
    use uc_protocol::v2::catalog::{RowEntry, RowVerdict, SetEntry, SetKind, SetState, encode_set_list};
    let mut set = SetEntry::commanded(4096, SetKind::Full, 7);
    set.state = SetState::Complete;
    set.rows[0] = RowEntry { version: 0, hash: 1, verdict: RowVerdict::Agreed, size: 40 };
    set.cluster = RowEntry { version: 0, hash: 2, verdict: RowVerdict::Agreed, size: 300 };
    let mut catalog = Vec::new();
    encode_set_list(&[set], &mut catalog).expect("one set");
    let mut v5_catalog = Vec::new();
    encode_cluster_image(&ClusterImageParts { catalog: &catalog, ..parts }, &mut v5_catalog)
        .expect("genesis parts are well under u32::MAX");
    let mut v4 = v5_catalog[..v5_catalog.len() - 4].to_vec();
    v4[8..12].copy_from_slice(&4u32.to_le_bytes());
    let crc = crc32fast::hash(&v4);
    v4.extend_from_slice(&crc.to_le_bytes());
```

and append `Seed::fixed("24-cluster-image-v5-sized-catalog", v5_catalog), Seed::fixed("25-cluster-image-v4-frame", v4),`. (If `crc32fast` is not already a fuzz dependency, `grep crc32fast fuzz/Cargo.toml`; add it at the workspace's version if absent.) In `uc_protocol_settings`, after `v1`:

```rust
    // Snapshot-lifecycle spec §6: the v3 shape (35 B, no auto_fetch) is a
    // live corpus value — it decodes with auto_fetch = true.
    let v3 = {
        let mut v = genesis.clone();
        v.truncate(SETTINGS_LEN_V3);
        v[0] = 3;
        v
    };
```

and append `Seed::fixed("08-version-3", v3),`. The existing `03-bad-version` seed sets byte 0 to `3` on a 36-byte v4 record — still a refusal (v3 header on a v4 length); leave it.

- [ ] **Step 2: Regenerate and check the corpus.**
Run: `(cd fuzz && cargo +nightly run --bin seed-corpus) && git status --short fuzz/`
Expected: changed/new files ONLY under `fuzz/corpus/uc_protocol_datagram/`, `uc_protocol_cluster_frame/`, `uc_protocol_cluster_image/`, `uc_protocol_settings/`, `uc_node_cluster_artifact/` (and `uc_protocol_status_body/` only if its seeds embed a settings record — inspect any other path before committing; an unexplained change is a stop).

- [ ] **Step 3: Smoke the changed targets.**
Run: `scripts/fuzz_smoke.sh 30 --min-runs 1000 uc_protocol_datagram uc_protocol_cluster_frame uc_protocol_cluster_image uc_protocol_settings uc_node_cluster_artifact`
Expected: every target completes ≥ 1000 runs, no crash.

- [ ] **Step 4: Commit.**

```bash
git add fuzz/src/seeds.rs fuzz/corpus
git commit -m "fuzz(lifecycle): seeds for SNAP_REPORT 32 B, sized reports, image v5/v4, Settings v3; corpus regenerated"
```

---

### Task 13: End-to-end tests

**Files:**
- Modify: `uc_node/tests/catalog.rs` (helpers: `APPLIES`, `ObsCapture`, `metric_labeled`, `await_capable_on`; five tests; `learner_only_voters_do_not_purge_until_they_fetch` gains its switch-off counter assertions)
- Test: `cargo test -p uc_node --test catalog -- --test-threads=1`

**Interfaces:**
- Consumes: everything above; `Node::set_free_bytes_for_test` (Task 9), `SnapshotPosLine::start_set` (Task 5), `REASON_PIN_ORIGIN_NOT_AGREED` (Task 11), `SetEntry::total_size` (Task 2).

- [ ] **Step 1: Add the helpers** (beside `metric`, `floor` and the `SumSm` statics):

```rust
/// Snapshot-lifecycle e2e 3: frames each node's `SumSm` applied in this
/// process — how a restart's replay length is read.
static APPLIES: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];

/// One labeled sample, e.g. `metric_labeled(n, "uc2_snapshot_auto_fetch_total", "outcome=\"ok\"")`.
fn metric_labeled(node: &Node, name: &str, labels: &str) -> u64 {
    let text = uc_node::obs::metrics::render_prometheus(&node.observability());
    let prefix = format!("{name}{{{labels}}} ");
    text.lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("no {name}{{{labels}}} sample in:\n{text}"))
        .trim()
        .parse()
        .unwrap_or_else(|e| panic!("{name}{{{labels}}}: {e}"))
}

/// [`await_capable`] over the nodes in `idxs` only.
fn await_capable_on(c: &Cluster, idxs: &[usize], rows: &[usize]) {
    let pages: Vec<Arc<CncPage>> = idxs.iter().map(|&i| c.cnc(i)).collect();
    await_until(30, "the named nodes' rows published the capability bit", || {
        pages.iter().all(|p| {
            rows.iter().all(|&r| {
                p.service_slot(r).status.load_acquire() & CNC_SVC_STATUS_SNAPSHOT_CAPABLE != 0
            })
        })
    });
}

/// The process-global obs sink, held for a scope (`snapshot_reports.rs`'s
/// guard; the file's `serialize()` keeps captures from overlapping).
struct ObsCapture(Arc<Mutex<Vec<u8>>>);

impl ObsCapture {
    fn take() -> Self {
        Self(uc_node::obs::log::capture_for_tests())
    }
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap_or_else(|e| e.into_inner())).into_owned()
    }
}

impl Drop for ObsCapture {
    fn drop(&mut self) {
        uc_node::obs::log::stderr_for_tests();
    }
}

fn pin_bytes(row: u8, version: u32, origin: u64) -> Vec<u8> {
    use uc_protocol::v2::upgrade::{UpgradePin, encode_upgrade_pin};
    let mut bytes = Vec::new();
    encode_upgrade_pin(
        &UpgradePin {
            row,
            from: version,
            to: version,
            origin,
        },
        &mut bytes,
    );
    bytes
}
```

and in `SumSm::apply`, first line: `APPLIES[self.node].fetch_add(1, Ordering::Relaxed);`.

- [ ] **Step 2: Write e2e 1 and run it against a stubbed rule to see it fail.** Add:

```rust
/// Snapshot-lifecycle spec §11 e2e 1: a learner-only cluster with auto-fetch
/// ON (the default). After a standby instant every voter fetches the agreed
/// set in the background, holds it, and purges below it — no operator step.
/// The set was agreed over ONE reporter, and the voters say so (review
/// focus 5).
#[test]
fn learner_only_voters_auto_fetch_the_agreed_set_and_purge_below_it() {
    let _g = serialize();
    let obs = ObsCapture::take();
    let mut c = spawn(2, 1, opts("lifecycle-auto-fetch", true), |_| true);
    start_sums(&mut c);
    let leader = await_single_leader(&c, 30);
    let learner = 2usize;
    await_capable(&c, &[0]);
    submit_frames(c.node(leader), 6000);
    let p = command_standby_instant(c.node(leader));
    await_until(60, "the learner completed the standby set", || {
        c.node(learner).snapshot_set_position() >= p
    });
    await_agreed(&c, &[0, 1, 2], p);
    for v in [0usize, 1] {
        await_until(90, &format!("voter {v} auto-fetched the agreed set"), || {
            c.node(v).snapshot_set_position() >= p
        });
        assert!(holds_on_disk(c.dir(v), &[0], p), "voter {v} holds the set on disk");
    }
    submit_frames(c.node(leader), 6000);
    for v in [0usize, 1] {
        await_until(60, &format!("voter {v} purged below the fetched set"), || {
            c.node(v).archive_first_base() > 0
        });
        assert_eq!(floor(&c, v), p, "voter {v}'s floor is the agreed set it fetched");
        assert!(
            metric_labeled(c.node(v), "uc2_snapshot_auto_fetch_total", "outcome=\"ok\"") >= 1,
            "voter {v} counted its fetch"
        );
        let audit = std::fs::read_to_string(c.dir(v).join("audit.jsonl")).unwrap_or_default();
        assert!(
            audit
                .lines()
                .any(|l| l.contains("\"actor\":\"auto\"") && l.contains("\"op_name\":\"snapshot_fetch\"")),
            "voter {v}: no auto snapshot_fetch audit record:\n{audit}"
        );
    }
    assert!(
        obs.text().contains("snapshot_fetch_single_reporter"),
        "a set agreed over one learner must be named as such"
    );
    c.stop();
}
```

Red check: temporarily make `maybe_auto_fetch` return at its first line, run `cargo test -p uc_node --test catalog learner_only_voters_auto_fetch -- --test-threads=1` → FAIL at "voter 0 auto-fetched the agreed set"; restore it → PASS.

- [ ] **Step 3: e2e 2 — the switch off.** In `learner_only_voters_do_not_purge_until_they_fetch` (already on `auto_fetch = false` since Task 9), after the 10-second no-purge loop add:

```rust
    for v in [0usize, 1] {
        for outcome in ["ok", "refused", "timeout", "no_space", "no_holder"] {
            assert_eq!(
                metric_labeled(c.node(v), "uc2_snapshot_auto_fetch_total", &format!("outcome=\"{outcome}\"")),
                0,
                "voter {v}: auto_fetch = false attempts nothing ({outcome})"
            );
        }
    }
```

and extend its doc comment: "Snapshot-lifecycle spec §11 e2e 2: with `[settings] auto_fetch = false` voters on a learner-only cluster hold nothing and never purge — the documented trade of the switch." Red check: flip its `auto_fetch: false` to `true` → FAIL at "voter 0 holds nothing"; restore → PASS.

- [ ] **Step 4: e2e 3 — the restart starts from the local set.**

```rust
/// Snapshot-lifecycle spec §11 e2e 3: a voter's in-memory service restarted
/// after an agreed instant installs the node's START SET at attach and
/// replays only the tail — it does not climb from 0.
#[test]
fn a_restarted_in_memory_service_starts_from_the_local_set() {
    let _g = serialize();
    let mut c = spawn(3, 0, opts("lifecycle-start-set", false), |_| true);
    let leader = await_single_leader(&c, 30);
    let v = *c.voters().iter().find(|&&i| i != leader).unwrap();
    let mut v_svc = None;
    for i in c.running() {
        let s = start_sum(&c.nodes[i].instance_dir, c.app, i);
        if i == v {
            v_svc = Some(s);
        } else {
            c.svcs.push(stopper(s));
        }
    }
    await_capable(&c, &[0]);
    submit_frames(c.node(leader), 4000);
    let p = instant_until_complete(&c, leader, &[0, 1, 2]);
    await_agreed(&c, &[0, 1, 2], p);
    let version = <SumSm as uc_service::RawStateMachine>::VERSION;
    let page = c.cnc(v);
    await_until(30, "the node published its start set", || {
        page.service_slot(0).snapshot_pos.start_set() == Some((p, version))
    });
    submit_frames(c.node(leader), 500);
    v_svc.take().unwrap().stop();
    APPLIES[v].store(0, Ordering::Relaxed);
    let svc = start_sum(c.dir(v), c.app, v);
    assert!(
        page.service_slot(0).applied.load_acquire() >= p,
        "attach published applied below the start set {p}"
    );
    await_applied(&c, leader, &[v], &[0]);
    let replayed = APPLIES[v].load(Ordering::Relaxed);
    assert!(
        replayed < 2000,
        "the restart applied {replayed} frames — it replayed from 0 (≈4500) instead of the tail (≈500)"
    );
    c.svcs.push(stopper(svc));
    c.stop();
}
```

Red check: temporarily make `install_start_set` return `Ok(None)` at its top → FAIL with "replayed from 0"; restore → PASS.

- [ ] **Step 5: e2e 4 — the pin waits for agreement.**

```rust
/// Snapshot-lifecycle spec §11 e2e 4: a pin naming an origin that is
/// complete on the leader but not yet AGREED is refused 61; once the report
/// timeout appends the record (one voter's row never attaches, so agreement
/// waits out the 5 s collector timeout) the same pin is accepted.
#[test]
fn a_pin_on_a_not_yet_agreed_origin_is_refused_61_then_accepted() {
    let _g = serialize();
    let mut c = spawn(3, 0, opts("lifecycle-pin-agreed", false), |_| true);
    let leader = await_single_leader(&c, 30);
    let quiet = *c.voters().iter().find(|&&i| i != leader).unwrap();
    let attached: Vec<usize> = c.running().into_iter().filter(|&i| i != quiet).collect();
    for &i in &attached {
        let s = start_sum(&c.nodes[i].instance_dir, c.app, i);
        c.svcs.push(stopper(s));
    }
    await_capable_on(&c, &attached, &[0]);
    submit_frames(c.node(leader), 2000);
    let p = command_instant(c.node(leader));
    await_until(30, "the leader completed the set", || {
        c.node(leader).snapshot_set_position() >= p
    });
    let version = <SumSm as uc_service::RawStateMachine>::VERSION;
    let (status, reason, _) = admin_staged(
        c.dir(leader),
        &c.cnc(leader),
        uc_node::UPGRADE_PENDING_FILE,
        ADMIN_OP_UPGRADE_PIN,
        &pin_bytes(0, version, p),
    );
    assert_eq!(
        (status, reason),
        (1, uc_node::REASON_PIN_ORIGIN_NOT_AGREED),
        "complete on the leader, not yet agreed (the quiet voter has not reported)"
    );
    await_agreed(&c, &attached, p);
    let at = pin(&c, leader, 0, version, p);
    assert!(at > p, "the pin was appended once the origin agreed");
    c.stop();
}
```

(`ADMIN_OP_UPGRADE_PIN` comes from `uc_protocol::v2::cnc` — add it to the file's `use uc_protocol::v2::cnc::{..}` list if absent.) Red check: comment out the 61 block in `apply_upgrade_pin` → FAIL `(0, 0) != (1, 61)`; restore → PASS.

- [ ] **Step 6: e2e 5 — no room.**

```rust
/// Snapshot-lifecycle spec §11 e2e 5: a node whose free space is below the
/// §7.3 check skips the fetch (`no_space`), names it once, downloads nothing,
/// and its gauge reports the agreed set's (known) size.
#[test]
fn a_node_without_room_skips_the_fetch_names_it_and_reports_the_size() {
    let _g = serialize();
    let obs = ObsCapture::take();
    let mut c = spawn(2, 1, opts("lifecycle-no-space", true), |_| true);
    start_sums(&mut c);
    let leader = await_single_leader(&c, 30);
    let learner = 2usize;
    let starved = *c.voters().iter().find(|&&i| i != leader).unwrap();
    let starved_id = c.nodes[starved].id;
    c.node(starved).set_free_bytes_for_test(1);
    await_until(10, "the starved voter's probe reads the override", || {
        c.node(starved)
            .soft_table()
            .by_node
            .get(&starved_id)
            .is_some_and(|e| e.holdings.free_bytes == 1)
    });
    await_capable(&c, &[0]);
    submit_frames(c.node(leader), 3000);
    let p = command_standby_instant(c.node(leader));
    await_until(60, "the learner completed the standby set", || {
        c.node(learner).snapshot_set_position() >= p
    });
    await_agreed(&c, &[0, 1, 2], p);
    await_until(90, "the leader auto-fetched the set", || {
        c.node(leader).snapshot_set_position() >= p
    });
    await_until(30, "the starved voter counted a no_space skip", || {
        metric_labeled(c.node(starved), "uc2_snapshot_auto_fetch_total", "outcome=\"no_space\"") >= 1
    });
    assert_eq!(c.node(starved).snapshot_set_position(), 0, "no download on the starved voter");
    assert_eq!(
        metric_labeled(c.node(starved), "uc2_snapshot_auto_fetch_total", "outcome=\"ok\""),
        0
    );
    let total = entry(c.node(starved), p).unwrap().total_size();
    assert!(total > 0, "a set catalogued after this change has a known size");
    assert_eq!(metric(c.node(starved), "uc2_snapshot_newest_agreed_bytes"), total);
    let p_text = p.to_string();
    assert!(
        obs.text()
            .lines()
            .any(|l| l.contains("snapshot_fetch_skipped_no_space") && l.contains(&p_text)),
        "the skip is named with the set's position"
    );
    c.stop();
}
```

Red check: make `fits` return `true` unconditionally → FAIL "no download on the starved voter"; restore → PASS.

- [ ] **Step 7: Run the whole file.**
Run: `cargo test -p uc_node --test catalog -- --test-threads=1`
Expected: PASS, every test (the six catalog tests, the fixture test, the five lifecycle tests).

- [ ] **Step 8: Commit.**

```bash
cargo fmt --all
git commit -am "test(lifecycle): five end-to-end scenarios — auto-fetch on/off, start set, pin 61, no space"
```

---

### Task 14: Docs sweep and spec errata

**Files (each line names what to write):**
- `docs/how-to/upgrade-a-cluster.md`, section `## Wire change after 0.10.0: the snapshot catalog (0.11.0)` (~:1001): add a paragraph —

```markdown
The same flag day also carries the **snapshot lifecycle**: the `SNAP_REPORT`
datagram's body grows from 24 B to 32 B (the artifact's size), the
`SnapshotReport` record's entries become `(node, hash, size)`, the
replicated `Settings` record moves to v4 (`auto_fetch`, default on), and the
cluster image to v5 (catalog row entries carry sizes). A v1–v4 cluster image
still loads — its sets read with size 0 ("unknown"), which nothing refuses —
and a v1–v3 settings record reads `auto_fetch = true`. Nothing on disk is
cleared. A dev cluster built from `main` between the catalog merge and this
change must also stop every node: a 24-byte report is refused by length.
```

- `docs/reference/wire-protocol.md`: the `SNAP_REPORT` (kind 26) row → 32 B body with `size u64 @24`; the `CLUSTER` kind 5 row → 20-byte entries `node_id u32 ‖ hash u64 ‖ size u64`; the Settings row → v4, 36 B, `auto_fetch u8 @35`; the 0.11.0 bump list gains these three.
- `docs/reference/semver-policy.md`: the 0.11.0 flag-day block gains the same three lines.
- `docs/reference/configuration.md` and `docs/reference/uc2ctl.md`: already carry `auto_fetch` (Task 3) and 61 (Task 11); add to `uc2ctl.md`'s `audit` section: "a `snapshot_fetch` record with `actor` and `source` `auto` is the node's own background fetch (snapshot lifecycle), not an operator's".
- `docs/how-to/bound-journal-growth.md` (§ "What happens to a node that falls below the floor" and the learner-only paragraphs ~:78–104): add a section

```markdown
## Learner-only clusters: voters fetch and purge by default

With `[settings] snapshot_target = "learners"` (or `uc2ctl snapshot
--standby`) only learners freeze. Every node — voters included — then
fetches the newest **agreed** set in the background (`auto_fetch`, on by
default), holds it, and purges its journal below it; no `uc2ctl snapshot
fetch` is needed. Watch `uc2_snapshot_auto_fetch_total{outcome}` and
`Uc2SnapshotWontFit`: a node that cannot fit the set with headroom
(`free < size + max(size/4, 1 GiB)`) skips it and does not purge.

**One learner proves little.** A standby set is agreed over the learners
that reported it; with ONE learner that is one reporter, and every voter
then fetches and purges on a set nobody cross-checked. A node logs
`snapshot_fetch_single_reporter` (warn) once per such set. Run two learners
if the purge floor must rest on agreement.

**Turning it off** (`uc2ctl settings apply` with `auto_fetch = false`):
voters on a learner-only cluster then hold no set and never purge — the
journal grows until you run `uc2ctl snapshot fetch` on each voter.
```

- `docs/how-to/monitor-a-cluster.md`: done in Task 10; add one sentence under the auto-fetch row naming `snapshot_fetch_single_reporter`.
- `docs/ops/uc2-runbook.md`, snapshots section: a "Restarts start from the newest agreed set" paragraph — the start rule (spec §5: an unpinned row installs the node's start set when it is ahead, then replays only the tail; `uc_service: row N started from snap-P` on stderr), the cnc words (`+264`/`+272`, readable with the cnc decode), and "reading a refused fetch": `snapshot_fetch_requested actor=auto`, `snapshot_fetch_timeout`, `snapshot_fetch_no_holder`, `snapshot_fetch_skipped_no_space`, and the counter outcomes; plus the residual (plan ruling P15) in one sentence.
- `docs/reference/cnc-page.md`: done in Task 5.
- `docs/superpowers/specs/2026-10-01-uc2-snapshot-catalog-design.md` §12: under "Project 2", add "→ designed in `2026-10-09-uc2-snapshot-lifecycle-design.md` (no byte threshold: D1)".
- `docs/BACKLOG.md` ~:377 (the Project 2 line): mark it taken up, pointing at the lifecycle spec and this plan; note "#48 closes with this work".
- `docs/superpowers/specs/2026-10-09-uc2-snapshot-lifecycle-design.md`: append

```markdown
#### Errata (plan, 2026-10-09)

Choices the implementation plan
(`docs/superpowers/plans/2026-10-09-uc2-snapshot-lifecycle.md`) made where
this spec is silent, numbered as there:

- **P1** `holders()` reads the soft table, which only a LEADER fills; on any
  node the fetch candidates are live holders, then the set's builders (the
  committed reports' matching hashes), then every other member — each tier
  learners first, then lowest id.
- **P2** A holder that cannot serve sends nothing; `refused` counts only a
  fetch the node could not issue, a silent holder is `timeout`.
- **P3** The start set's version rule is by LINE (`same_line`), as today's
  unpinned install is (D5), not exact equality.
- **P4** A missing or unverifiable artifact falls back to replay; an error
  from the state machine's own install is a fail-stop.
- **P5** The overrun jump is refused when the row now carries a pin or a
  version record above what the walk decided.
- **P6** `size` is the artifact file's length on disk (envelope included).
- **P7** The recorded size is the largest reported with the majority hash.
- **P8** A node still building `N` (an attached row below it) does not fetch it.
- **P9–P11** Waiting re-checks every 100 ms; `no_holder` waits 30 s;
  `no_space` backs off on the same ladder.
- **P12** A set reported by exactly one node logs
  `snapshot_fetch_single_reporter` once.
- **P13** Refusal 61 is door-only.
- **P14** `Node::set_free_bytes_for_test` is a hidden test seam.
- **P15** A new fetch still clears the receiver's single expired-fetch slot
  (catalog fix round 3); auto-fetch never re-arms while a fetch is pending
  and waits at least 1 s after a timeout.
```

- [ ] **Step 1: Write every change above.**
- [ ] **Step 2: Check links.** Run: `python3 scripts/check_doc_links.py` — Expected: 0 errors.
- [ ] **Step 3: Commit.**

```bash
git add docs
git commit -m "docs(lifecycle): flag-day sizes and Settings v4, auto-fetch, the start rule, refusal 61; spec errata (plan)"
```

(`RELEASES.md` / `docs/releases.md` are NOT in this plan — owed at the release cut, beside the catalog's.)

---

### Task 15: Proof stack (evidence only, no commit)

Run with the private target dir (`export CARGO_TARGET_DIR=$HOME/.cache/cargo-target-lifecycle`), logging each to `$HOME/scratch/lifecycle-proof/NN-<name>.log`:

- [ ] 1. `cargo build --workspace`
- [ ] 2. `cargo build -p uc_lincheck --features replay-bin --bin register-replay && cargo build -p uc_diffreplay`
- [ ] 3. `cargo test --workspace`
- [ ] 4. `cargo test -p uc_node --test lin_v2`
- [ ] 5. `cargo test -p uc_node --test lin_partition_v2`
- [ ] 6. `cargo test -p uc_crashtest --features hard-crash-tests` (its restarts now take the start-set path)
- [ ] 7. `cargo test -p uc_diffreplay --test pin_verify -- --test-threads=1`
- [ ] 8. `cargo test -p uc_node --test catalog -- --test-threads=1` and `cargo test -p uc_node --test learner -- --test-threads=1`
- [ ] 9. `cargo clippy --workspace --all-targets -- -D warnings` and `cargo clippy -p uc_crashtest --all-targets --features hard-crash-tests -- -D warnings`
- [ ] 10. `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings`
- [ ] 11. `cargo fmt --all -- --check`
- [ ] 12. `python3 scripts/check_doc_links.py`
- [ ] 13. Fuzz seeds are current: `(cd fuzz && cargo +nightly run --bin seed-corpus) && git status --short fuzz/` — Expected: empty (Task 12 committed them).
- [ ] 14. `scripts/fuzz_smoke.sh 30 --min-runs 1000 uc_protocol_datagram uc_protocol_cluster_frame uc_protocol_cluster_image uc_protocol_settings uc_node_cluster_artifact`
- [ ] 15. `scripts/m10_alert_fire.sh` — every shipped rule, `Uc2SnapshotWontFit` included, fires.
- [ ] 16. Push the branch, then dispatch a nightly ON the branch (the CI runner exposes timing the dev box does not):

```bash
git push -u origin design/snapshot-lifecycle
gh workflow run nightly.yml --ref design/snapshot-lifecycle
gh run list --workflow nightly.yml --branch design/snapshot-lifecycle --limit 1
gh run watch "$(gh run list --workflow nightly.yml --branch design/snapshot-lifecycle --limit 1 --json databaseId -q '.[0].databaseId')"
```

Report every command's exit code and pass/fail counts verbatim, and the nightly run's URL and conclusion. Any red stops the plan: it is not "flaky" until it has been reproduced and named. After the merge (not on this branch): close #48 with a link to the merge commit.
