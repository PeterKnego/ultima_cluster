# Retirement and proof — Implementation Plan (plan 3 of 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every symbol spec §7 retires is gone from the tree; the cluster image codec is fuzzed from the `core`-only crate; the `apply_bench` A/B runner the time-and-timers gate has lacked since 2026-09-03 exists and adjudicates the new apply-loop arm; the three new gate rows have pre-committed bars; and the release writeup, explainer, verification record and standing facts describe what is actually built.

**Architecture:** Deletion is grep-driven and pinned by a test that greps the tree. The image codec moves from `uc_node::cluster_fsm` into `uc_protocol::v2::cluster_image` (core-only, no `std::io`) so the fuzz crate reaches it without depending on `uc_node`; `cluster_fsm` keeps the trait impls and calls the codec. The A/B runner is a sibling of `scripts/hop1_ab.sh` that drives `uc_node/examples/apply_bench` with the same same-source rebuild control, so row d of the timer gate and the new row f share one procedure.

**Tech Stack:** as plans 1–2; `scripts/` (bash + python3), `docs/benchmarks/`.

**Spec:** `docs/superpowers/specs/2026-09-05-uc2-cluster-fsm-and-coordinated-snapshot-design.md` §7, §11 (fuzz, gate rows, acceptance), §12, §14 item 3. **Plans 1 and 2 must be on `main` first.**

## Global Constraints

- **Whole workspace green after every task** — plan 1's command list, plus `scripts/fuzz_smoke.sh 30 --min-runs 1000 uc_protocol_cluster_image uc_protocol_cluster_frame uc_protocol_datagram` after Task 1.
- **The workspace version stays `2.11.0`; no tag is cut by this plan.** Tagging, `release.yml`, verification and crates.io are `docs/how-to/cut-a-release.md` §§2–6, run by the maintainer after the fleet gates.
- **Perf rate bars are fleet-only.** Task 2 builds the runner and **pre-commits** bars; it runs the rows locally as smoke only and records them as smoke. A fleet run is user-gated.
- **Nothing retired may survive under another name.** Task 0's pin test greps for the retired identifiers and the retired numbers' old names; a later plan that needs a reserved number re-documents it, it does not resurrect the symbol.

---

## File structure

| file | responsibility | task |
|---|---|---|
| the whole tree | every §7 retired symbol deleted; `uc_node/tests/retired.rs` pins it | 0 |
| `uc_protocol/src/v2/cluster_image.rs` (new), `uc_node/src/cluster_fsm.rs`, `fuzz/` | the image codec as a core-only leaf; `uc_protocol_cluster_image` target; `SNAP_BEGIN` V4 seeds | 1 |
| `scripts/apply_ab.sh` (new), `docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md` | the `apply_bench` A/B runner with a rebuild control; rows d, f, g, h with bars | 2 |
| `docs/VERIFICATION.md`, `docs/notes/uc2-cluster-fsm-explained.md`, `RELEASES.md`, `docs/releases.md`, `CLAUDE.md`, `docs/reference/{limits,semver-policy}.md`, `docs/how-to/upgrade-a-cluster.md`, `docs/BACKLOG.md`, `docs/benchmarks/uc2-fsm-identity-gate-2026-09-02.md` | the record | 3 |

---

### Task 0: retire every §7 symbol, and pin the retirement

**Files:**
- Delete or edit wherever `git grep` finds them: `FRAME_TYPE_CONFIG` (alias, if plan 1 left one), `FRAME_TYPE_SCHEDULE_TABLE` (keep only `FRAME_TYPE_SCHEDULE_TABLE_RETIRED`), `append_schedule_table`, `DGRAM_KIND_SNAP_TABLE` (keep `DGRAM_KIND_SNAP_TABLE_RETIRED`), `SnapTableBody`, `write_snap_table_body`, `read_snap_table_body`, `SNAP_TABLE_FIXED_LEN`, `SnapBeginBody.config`, `SNAP_BEGIN_LAYOUT_V2`, `SNAP_BEGIN_LAYOUT_V3` (V4 is the only accepted layout; keep the two constants only if a test forges a legacy body — then rename them `_RETIRED`), `schedule_state` (module, `ScheduleRecord`, `SCHEDULE_STATE_FILE`, `read_record`), `ScheduleShip`, `shippable_schedule`, `known_committed`, `install_snapshot_table`, `adopt_snapshot_config`'s config-cell read, `incoming_snapshot_config`, `incoming_snapshot_table`, `SnapshotPolicy`, `ServiceConfig::snapshot_policy`, `maybe_build_snapshot`, `SnapshotTrigger::{policy, last_snapshot_pos}`, `rearm_timers`, `RowTimers::rearm`, `timer_stats.rearmed`, `uc2_timers_rearmed_total`, the `timers_rearmed` obs record, `SnapshotStore::retain_newest` (the node prunes since plan 2), the plan-1 bridging trigger, `NodeConfig::admission_bytes` (renamed `_default` in plan 1 — confirm no caller uses the old name), `services.fsm_lag` in `node.toml` (refused; confirm no test fixture still writes it under `[services]`).
- Create: `uc_node/tests/retired.rs`

**Interfaces:** none new.

- [ ] **Step 1: Write the pin test**

```rust
//! Spec §7 / plan 3: the symbols this flag day retired must not come back
//! under their old names. A grep, not a compile check, so a re-introduction
//! in a comment, a doc or a script is caught too.
use std::process::Command;

const RETIRED: &[&str] = &[
    "FRAME_TYPE_CONFIG\\b", "FRAME_TYPE_SCHEDULE_TABLE\\b", "append_schedule_table",
    "DGRAM_KIND_SNAP_TABLE\\b", "SnapTableBody", "SNAP_TABLE_FIXED_LEN", "SNAP_BEGIN_LAYOUT_V3\\b",
    "schedule_state::", "ScheduleRecord\\b", "SCHEDULE_STATE_FILE", "ScheduleShip", "shippable_schedule",
    "known_committed", "install_snapshot_table", "incoming_snapshot_config", "incoming_snapshot_table",
    "SnapshotPolicy\\b", "maybe_build_snapshot", "rearm_timers", "uc2_timers_rearmed_total", "timers_rearmed",
    "retain_newest", "bridging_trigger",
];

#[test]
fn retired_symbols_are_gone_from_the_tree() {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/..");
    let mut hits = Vec::new();
    for pat in RETIRED {
        let out = Command::new("git")
            .args(["grep", "-nE", pat, "--", ":!docs/superpowers/", ":!docs/benchmarks/", ":!docs/releases.md", ":!RELEASES.md", ":!uc_node/tests/retired.rs", ":!docs/notes/"])
            .current_dir(root)
            .output()
            .unwrap();
        if out.status.success() {
            hits.push(format!("{pat}:\n{}", String::from_utf8_lossy(&out.stdout)));
        }
    }
    assert!(hits.is_empty(), "retired symbols still present:\n{}", hits.join("\n"));
}
```

(The excluded paths are the historical record — specs, plans, gate docs, release notes, explainers — which are allowed to *mention* what was retired.)

- [ ] **Step 2: Run it to see the current hits** — `cargo test -p uc_node --test retired -- --nocapture`. Expected: FAIL, listing every remaining occurrence. That list **is** the work list for Step 3.
- [ ] **Step 3: Delete each hit**, one commit per crate (`uc_protocol`, `uc_log`, `uc_net`, `uc_service`, `uc_node`, `uc_ctl`, docs/scripts), re-running the pin test after each. For the reserved numbers keep only the `_RETIRED` constants with a one-line doc comment each.
- [ ] **Step 4: Run** the full Global Constraints list. **Step 5: Final commit** — `chore: retire every symbol spec §7 names, pinned by uc_node/tests/retired.rs`.

---

### Task 1: the cluster image codec as a core-only leaf, fuzzed; V4 seeds

**Files:**
- Create: `uc_protocol/src/v2/cluster_image.rs` (`CLUSTER_IMAGE_MAGIC`, `CLUSTER_IMAGE_VERSION`, `pub struct ClusterImageParts<'a> { applied: u64, table_position: u64, membership: &'a [u8], table: &'a [u8], settings: &'a [u8] }`, `pub fn encode_cluster_image(p: &ClusterImageParts, out: &mut Vec<u8>)`, `pub fn decode_cluster_image(buf: &[u8]) -> Option<ClusterImageParts<'_>>` — total, CRC-checked, exact framing; the three inner payloads are returned as slices and decoded by the caller with the existing codecs)
- Modify: `uc_node/src/cluster_fsm.rs` (`freeze`/`install_snapshot` call the leaf; the byte layout is unchanged from plan 1 so existing artifacts still load — pin with a fixture test that installs a plan-1-era image byte string)
- Create: `fuzz/fuzz_targets/uc_protocol_cluster_image.rs`; seeds `21-cluster-image` (a valid encode of genesis parts), `22-cluster-image-bad-crc`; `fuzz/src/seeds.rs` gains `SNAP_BEGIN` V4 seeds `23-snap-begin-v4` and `24-snap-begin-v4-bad-layout`; `scripts/fuzz_smoke.sh` lists the new target
- Test: `cluster_image.rs` tests (roundtrip; every corruption of one byte is refused or decodes to the same parts — a CRC guarantees refusal)

**Interfaces:** as above. `crc32fast` is already a `uc_protocol` dependency? — **check** `uc_protocol/Cargo.toml`; if it is not, the leaf implements the CRC-32 table inline (it must stay `core`-friendly and dependency-free, like `identity.rs`), and `uc_node` stops using `crc32fast` for this.

- [ ] **Step 1: Failing tests** — `cluster_image_roundtrips_and_layout_is_frozen` (assert the magic at 0, version at 8, and that `encode` of fixed parts produces the byte string plan 1's `freeze` produced — captured once from a plan-1 build and pasted as a `const`), `every_single_byte_corruption_is_refused` (loop over positions, flip one bit, assert `None`).
- [ ] **Step 2: Run** → red. **Step 3: Implement**; move the codec out of `cluster_fsm.rs`; `cluster_fsm`'s tests keep passing unchanged. **Step 4:** fuzz target (`decode` → re-encode → `decode` equal), seeds, smoke list. **Step 5: Run** `cargo test -p uc_protocol -p uc_node --lib && scripts/fuzz_smoke.sh 30 --min-runs 1000 uc_protocol_cluster_image`. **Step 6: Commit** — `feat(uc_protocol): the cluster image codec as a core-only leaf, fuzzed; SNAP_BEGIN V4 seeds (spec §11)`.

---

### Task 2: the `apply_bench` A/B runner, and the gate rows

**Files:**
- Create: `scripts/apply_ab.sh` — the same shape as `scripts/hop1_ab.sh`: `apply_ab.sh <base-sha> <head-sha> [--fsms N] [--secs S] [--pairs K]`; builds **both** commits into private `CARGO_TARGET_DIR`s **and** rebuilds `<head-sha>` a second time into a third dir (the same-source rebuild control), runs `apply_bench --root … --fsms N --mode bounded --secs S` K times per binary interleaved (A, B, B′, B, A, B′, …), parses its rate line, and prints per-arm mean/p50/spread and the three pairwise deltas — **head vs base**, **head vs head′** (the resolution), and a verdict `within resolution` / `outside resolution` for the first delta against the second.
- Modify: `docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md` — row d gains its runner (the `not run: no runner` text is replaced by the procedure); new rows **f** (commanded instants under the throughput load: the apply-loop arm's cost, `apply_ab.sh <plan-1-merge-base> <plan-2-head>` at N=1 and N=2 bounded, bar = within the run's own rebuild resolution), **g** (a below-floor join with the shipper restarted mid-window: `learner.rs`'s residual test scaled to the fleet — time to converge, bar **≤ 60 s** and `snapshot_installed` observed, matching the FSM-identity gate's row j), **h** (freeze duration vs commit stall: a `CountSm` with a deliberately large state — a `Vec<u8>` of 256 MiB the FSM carries — under `m14_fleet_gate.py`'s row a load; command an all-nodes instant and record `uc2_snapshot_freeze_seconds_max` and the longest gap in `commit` advance during it; then a `--standby` instant and record both again; bar: the standby instant's commit gap is **≤ the pass length measured on the day**, i.e. no stall attributable to the instant, and the all-nodes instant's gap is **reported, no bar** — it is the number this row exists to produce).
- Modify: `docs/benchmarks/uc2-fsm-identity-gate-2026-09-02.md` — no new rows; a note that row j's join now installs the cluster artifact.

**Interfaces:** `apply_bench`'s output line format — read `uc_node/examples/apply_bench.rs` for the exact `RESULT`/rate line before writing the parser; if it prints no machine-readable line, add one (`RESULT {"frames_per_sec": …, "fsms": …, "mode": …}`) in this task and pin its shape with a unit test in the example.

- [ ] **Step 1:** Read `apply_bench.rs`'s output; add the `RESULT` line if absent (commit separately: `feat(apply_bench): machine-readable RESULT line`).
- [ ] **Step 2:** Write `scripts/apply_ab.sh` with a `--selftest` that fakes two binaries printing fixed rates and asserts the arithmetic (as `m13_hop_bench.py --selftest` does).
- [ ] **Step 3:** Run `scripts/apply_ab.sh --selftest`; then one real local pair (`--pairs 3 --secs 3`) as **smoke** — record its resolution number in the gate doc's row f as "dev-box smoke, not a gate".
- [ ] **Step 4:** Write rows d/f/g/h into the gate doc's bar table and results table (results: `not run — fleet, user-gated`), with the "record the resolution first" instruction row a already carries.
- [ ] **Step 5: Commit** — `feat(scripts): apply_ab.sh — the apply_bench A/B with a same-source rebuild control; gate rows d/f/g/h (spec §11)`.

---

### Task 3: the record

**Files:**
- `docs/VERIFICATION.md`: §2 gains inv11 and inv12 in the invariant table and the two red twins in the prose; §3's capstone table gains the plan-2 `timers.rs`/`learner.rs` rows; §5 gains the SIGKILL-mid-build scenario; §7's target table gains `uc_protocol_cluster_frame`, `uc_protocol_settings`, `uc_protocol_cluster_image` (count: 18 → 21); §11's "timer heap" and "leader pass is checked for ORDERING" bullets are rewritten (the ordering oracle still runs; the schedule table's source of truth is the cluster FSM; the heap is leader-only), and a new bullet states the one seam that survives — `state/config.state` as the kernel's durable-time shadow, with the inv12 pointer.
- `docs/notes/uc2-cluster-fsm-explained.md`: finalised (plan 1 wrote the stub; plan 2 added the instants section): the problem, the line, the cluster FSM, the two readers, the instant, standby, what was retired, what stays and why, Aeron's shape cited to the lines spec §2 cites.
- `RELEASES.md` and `docs/releases.md`: the 2.11.0 draft section gains the fourth feature bullet ("The cluster FSM and coordinated snapshots", linking the explainer, `configuration.md` `[settings]`, `uc2ctl.md`, `bound-journal-growth.md`, `run-work-on-a-schedule.md`), the SDK-break line (`SnapshotPolicy` removed; `admission_bytes`/`fsm_lag` moved) under the FSM-identity carve-out, and the release-evidence table gains rows for gate rows f/g/h (`pending — fleet`).
- `CLAUDE.md`: the "Standing facts" 2.11.0 entry gains a fourth sub-bullet (the cluster FSM: frame 4/6/7, kinds 21–23, ops 7–9, `NODE_FLAG_LEARNER`, slot bit 9, `uc_node → uc_service`, the leader-only heap, `[settings]`, the two closed residuals) and the "IN PREPARATION" paragraph is rewritten to "release blocked on the three fleet gates"; the crate list gains the dependency note; the workspace-crate description of `uc_node` names the fifth agent.
- `docs/reference/limits.md`: the under-ship and wiped-node rows are struck through with "closed by the cluster FSM (2.11.0)"; a new row for the freeze-on-a-quorum stall with the `P + fsm_lag` formula and the standby remedy.
- `docs/reference/semver-policy.md`: the FSM-identity carve-out paragraph names the `SnapshotPolicy` removal and the `node.toml` key moves as riding the same carve-out.
- `docs/how-to/upgrade-a-cluster.md`: the 2.11 section (still headed "pending" — plan 3 does not date it; the tag does) gains the `node.toml` edits an operator must make (`admission_bytes` and `services.fsm_lag` → `[settings]`; no `uc_`-prefixed names) and the instance-dir changes (`snapshots/cluster/`, no `schedules.state`).
- `docs/BACKLOG.md`: item 2's under-ship and wiped-node bullets → closed by name with the commit; the §13 door (automatic standby replication) is added as a bullet under item 2 with the reason it is deferred.

- [ ] **Step 1:** Write every file above. **Step 2:** link-check (paths and anchors). **Step 3:** `cargo test --workspace --doc`; `cargo test -p uc_node --test retired`. **Step 4: Commit** — `docs: the cluster FSM and coordinated snapshots — verification, explainer, release draft, standing facts, limits (spec §12, plan 3)`.

---

## Self-review

**Spec coverage (plan 3's share):** §7 → Task 0; §11 fuzz → Task 1; §11 gate rows → Task 2; §11 acceptance ("every retired symbol gone", "residuals closed by name", "the differential timer test still passes" — the last is in every plan's Global Constraints) → Tasks 0, 3; §12 → Task 3; §14 item 3 → all.

**Placeholder scan:** Task 2 Step 1 conditions on what `apply_bench` prints today — a read-then-act instruction with the fallback specified (add a `RESULT` line and pin it), not a gap. Task 1 conditions on whether `uc_protocol` already has `crc32fast` — both branches specified.

**Type consistency:** `ClusterImageParts`, `encode_cluster_image`/`decode_cluster_image` in Task 1 only; `apply_ab.sh`'s three deltas named identically in Task 2's script and gate-row text; the retired list in Task 0 matches spec §7 and plan 1/2's deletions.
