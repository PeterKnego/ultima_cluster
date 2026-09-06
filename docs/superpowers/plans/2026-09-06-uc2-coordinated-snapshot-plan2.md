# Coordinated and standby snapshot instants — Implementation Plan (plan 2 of 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every user FSM and the cluster FSM freeze at one leader-commanded log position P; a node's snapshot set is complete or nothing; the floor moves only to a complete set; the session ships "the set at my floor"; and a standby-flagged instant freezes only learners, with voters pulling the set on demand — so no coordinated freeze ever pauses commit on a quorum.

**Architecture:** `FRAME_TYPE_SNAPSHOT = 7` (empty body, `FLAG_SNAPSHOT_STANDBY` in the header's existing flags byte) is appended by the leader and acted on by every row's apply loop and by the cluster agent: after applying everything below the frame-end P, `freeze()` and hand the build to the existing builder thread, which stores `snapshot_pos = P`. The consensus agent polls what it already polls (every declared slot's `snapshot_pos`, plus the cluster agent's) and declares the set at P complete when they all equal P; only then does `node_snapshot_floor := P`. `snapshot_set_for` ships the artifacts **at the floor** — committed by construction, because a row freezes only after *applying* to P and apply is gated on `min(commit, durable)`. A standby instant is the same frame with a flag; the node publishes `NODE_FLAG_LEARNER` in the node-written flags word the service already reads `NODE_FLAG_LEADER` from, and only a learner's rows act on the flagged frame. Voters obtain a learner's set by `SNAP_REQUEST` (kind 22) in a **store-only** receive mode; the leader answers a below-floor NAK it cannot serve with `SNAP_REDIRECT` (kind 23) to a learner that can. Plan 1's bridging trigger is deleted.

**Tech Stack:** as plan 1, plus `uc_node/examples/m10_alerts.rs` and `scripts/m10_alert_fire.sh` for the two new alert rules.

**Spec:** `docs/superpowers/specs/2026-09-05-uc2-cluster-fsm-and-coordinated-snapshot-design.md` §5 (all subsections, §5.7 in particular), §6 (`snapshot.target`), §7, §8, §9, §10, §11, §14 item 2, §15 check 4. **Plan 1 (`2026-09-06-uc2-cluster-fsm-plan1.md`) must be on `main` first** — this plan consumes its `ClusterAgent`, `ClusterView`, `CLUSTER_ARTIFACT_ID`, the V4 `SNAP_BEGIN`, and the leader-only heap.

## Global Constraints

- **Whole workspace green after every task** — the same command list as plan 1's Global Constraints, plus `cargo test -p uc_crashtest --features hard-crash-tests` after Task 10 and `cargo run -p uc_node --example m10_alerts -- schedule_diverged snapshot_stalled snapshot_set_diverged` after Task 8.
- **Still the unreleased 2.11.0 flag day**: `CURRENT` stays `0.7.0`, cnc `3.1`, workspace `2.11.0`.
- **Frozen once shipped**: `FRAME_TYPE_SNAPSHOT = 7`; `FLAG_SNAPSHOT_STANDBY = 0x01` (header flags); `NODE_FLAG_LEARNER = 4` (the node flags word — **spec errata, Task 0**: not a per-row slot bit); `CNC_SVC_STATUS_SNAPSHOT_CAPABLE = 1 << 9` (the service-written slot status word); `DGRAM_KIND_SNAP_REQUEST = 22` (body `session u32 @0 ‖ position u64 @4`, 12 B); `DGRAM_KIND_SNAP_REDIRECT = 23` (body `session u32 @0 ‖ learner_id u32 @4 ‖ position u64 @8`, 16 B); `ADMIN_OP_SNAPSHOT = 8`, `ADMIN_OP_SNAPSHOT_FETCH = 9`; refusals `48 snapshot_unsupported`, `49 snapshot_no_learner`; the one-position rule for a V4 session. Each pinned by a test whose comment says so.
- **The apply hot loop gains exactly one arm** (`else if hdr.frame_type == FRAME_TYPE_SNAPSHOT`), out of line: the arm calls a `#[inline(never)] fn on_snapshot_frame(...)`. Plan 3's gate row A/Bs it against `apply_bench`; keep the inline body to the type test and the call (M14a).
- **Commit, apply and replication never wait on a snapshot.** No task may add a wait on `busy`, on a peer's `snapshot_pos`, or on a fetch to any agent's duty cycle. An instant that cannot complete is abandoned, counted, and superseded.
- **Surfaces this plan builds on (as built after plan 1)**: `uc_node::cluster_agent::ClusterAgent::{do_work, applied, snapshot_pos, take_snapshot}` and its `cluster_snapshot_pos: Arc<AtomicU64>`; `uc_node::cluster_fsm::ClusterView::{position, snapshot_interval_bytes, snapshot_target, membership()}`; `uc_net::sender::{SnapshotSet, SnapArtifact, CLUSTER_ARTIFACT_ID, try_open_snap_session(to)}` (`sender.rs:1155`), `SnapSession` (`:203`); `uc_net::receiver::{SnapIntake, snap_complete}` (`receiver.rs:2415`), `send_snap_nak` (`:2527`), `incoming_snapshot_pos`, `snap_adopt_pending`; `uc_service::apply::{apply_cycle, ApplyState, SnapshotTrigger { busy, tx, freeze }}` (`apply.rs:152–177`), the `TIMER` arm (`:480`), `builder_agent::{BuildJob, BuilderState, builder_cycle}` (`builder_agent.rs:30–90`), `snapshots::SnapshotStore::{newest, retain_newest}` (`snapshots.rs:72`, `:128`); `uc_node`'s `publish_status` (`node.rs:3832`), `publish_service_mins` (`:3280`, writes the page-1 `service_snapshot_pos` min at `:3302`), `maybe_persist_snapshot_floor` (`:3813`), `snapshot_set_for` (`:7659`, `SNAP_DECLINE_*` at `:348–352`), `handle_admin` (`:5412`); `uc_node/src/obs/metrics.rs` (the name list `:60–80`, `push_gauge`/`push_counter` `:430–470`); `packaging/prometheus/uc2-alerts.yml` (`Uc2ScheduleTableDiverged` at `:184`), `uc_node/examples/m10_alerts.rs` (`scenario_schedule_diverged` `:1392`), `scripts/m10_alert_fire.sh` (`RULE_BUILDERS` `:519`, `build_Uc2ScheduleTableDiverged` `:502`); `uc_node/tests/learner.rs` (`spawn_cluster_with_learner(n_voters, n_learners)` `:208`, `await_single_leader`, `await_serving_among`, `submit_n`); `uc_consensus::config::ClusterConfig::is_learner(id)` (`config.rs:158`).
- **Fleet spend is user-gated. Never write scratch to `/tmp`.**
- Commit subjects: `type(scope): imperative summary`. Every new or changed test is **watched red first**.

---

## File structure

| file | responsibility | task |
|---|---|---|
| spec §5.7 item 2, §7 | errata: the learner bit is `NODE_FLAG_LEARNER` in the node flags word | 0 |
| `uc_protocol/src/v2/{frame,cnc,datagram}.rs`, `fuzz/` | `FRAME_TYPE_SNAPSHOT`, `FLAG_SNAPSHOT_STANDBY`, `NODE_FLAG_LEARNER`, `CNC_SVC_STATUS_SNAPSHOT_CAPABLE`, ops 8/9, kinds 22/23 + bodies, seeds | 1 |
| `uc_log/src/buffer.rs` | `append_snapshot(term, flags) -> (end, stamp)` | 2 |
| `uc_service/src/{apply,lib,config,attach,builder_agent}.rs` | the `SNAPSHOT` arm; capability bit; `SnapshotPolicy` and `maybe_build_snapshot` deleted; `busy` → incomplete | 3 |
| `uc_node/src/cluster_agent.rs` | freeze at P on `SNAPSHOT`; the bridging trigger deleted | 4 |
| `uc_node/src/node.rs` (consensus) | last-commanded P, set completeness, floor := P, cadence, single-in-flight + supersession, `uc2ctl snapshot`, `NODE_FLAG_LEARNER`, retention, `snapshot_set_for` at the floor | 5 |
| `uc_net/src/{sender,receiver}.rs` | one-position rule; `SNAP_REQUEST` serving; store-only receive; `SNAP_REDIRECT` | 6 |
| `uc_ctl/src/snapshot.rs` (new), `main.rs`, `audit.rs` | `snapshot [--standby]`, `snapshot fetch`, `snapshot show` | 7 |
| `uc_node/src/obs/metrics.rs`, `packaging/prometheus/uc2-alerts.yml`, `examples/m10_alerts.rs`, `scripts/m10_alert_fire.sh` | five metrics, two alerts, two scenarios, two builders | 8 |
| `uc_sim/src/world.rs`, `tests/scenarios.rs` | inv11 set alignment; truncated-instant scenario | 9 |
| `uc_node/tests/{timers,learner}.rs`, `examples/uc_crashtest` | instants under load; standby; fetch; redirect; SIGKILL mid-build | 10 |
| docs | as spec §12 for plan 2's share | 11 |

---

### Task 0: spec errata — where the learner bit lives

**Files:** `docs/superpowers/specs/2026-09-05-uc2-cluster-fsm-and-coordinated-snapshot-design.md` §5.7 item 2 and §7's cnc row.

- [ ] **Step 1:** Replace §5.7 item 2's "`CNC_SVC_STATUS_LEARNER = 1 << 10` … per row" with: "The node publishes `NODE_FLAG_LEARNER = 4` in the node-written status flags word — the same word the service apply loop already reads `NODE_FLAG_LEADER` from once per cycle (`uc_service/src/apply.rs:421`) — set from the kernel's durable-time membership shadow in `publish_status` on every adoption. A per-row slot status bit was wrong twice over: the slot's status word is **service-written** (`cnc.rs:277`), and role is a node property, not a row property." Update §7's cnc row to "bit 9 (slot status, service-written) = snapshot-capable; `NODE_FLAG_LEARNER = 4` (node flags word)".
- [ ] **Step 2:** This errata was applied when the plan was written (the commit that added this plan); verify §5.7 item 2 and §7's cnc row read as Step 1 says and skip the commit if so.

---

### Task 1: the wire and cnc constants

**Files:**
- Modify: `uc_protocol/src/v2/frame.rs` (`FRAME_TYPE_SNAPSHOT`, `FLAG_SNAPSHOT_STANDBY`), `uc_protocol/src/v2/cnc.rs` (`NODE_FLAG_LEARNER`, `CNC_SVC_STATUS_SNAPSHOT_CAPABLE`, `ADMIN_OP_SNAPSHOT`, `ADMIN_OP_SNAPSHOT_FETCH`), `uc_protocol/src/v2/datagram.rs` (kinds 22/23, `SnapRequestBody`, `SnapRedirectBody`, codecs, `SNAP_REQUEST_BODY_LEN = 12`, `SNAP_REDIRECT_BODY_LEN = 16`)
- Modify: `fuzz/src/seeds.rs` (`19-snap-request`, `20-snap-redirect`), `fuzz/fuzz_targets/uc_protocol_datagram.rs` (two arms)
- Test: each file's tests module

**Interfaces:**
- Produces: the constants above; `pub struct SnapRequestBody { pub session: u32, pub position: u64 }`, `write_snap_request_body(buf, &b)`, `read_snap_request_body(buf) -> Option<SnapRequestBody>`; `pub struct SnapRedirectBody { pub session: u32, pub learner_id: u32, pub position: u64 }` and its pair. Both readers are total and exact-length.

- [ ] **Step 1: Write the failing tests**

```rust
// frame.rs
#[test]
fn snapshot_frame_type_and_standby_flag_are_frozen() {
    assert_eq!(FRAME_TYPE_SNAPSHOT, 7);
    assert_eq!(FLAG_SNAPSHOT_STANDBY, 0x01);
    assert_ne!(FLAG_SNAPSHOT_STANDBY & FLAG_TIMER_TABLE, 0, "same header byte, and the same bit is fine: the frame TYPE disambiguates");
}
// cnc.rs
#[test]
fn learner_flag_and_capability_bit_and_snapshot_ops_are_frozen() {
    assert_eq!(NODE_FLAG_LEARNER, 4);
    assert_eq!(NODE_FLAG_LEARNER & (NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE), 0);
    assert_eq!(CNC_SVC_STATUS_SNAPSHOT_CAPABLE, 1 << 9);
    assert_eq!(ADMIN_OP_SNAPSHOT, 8);
    assert_eq!(ADMIN_OP_SNAPSHOT_FETCH, 9);
}
// datagram.rs
#[test]
fn snap_request_and_redirect_bodies_roundtrip_and_are_exact_length() {
    assert_eq!((DGRAM_KIND_SNAP_REQUEST, DGRAM_KIND_SNAP_REDIRECT), (22, 23));
    let mut b = [0u8; SNAP_REQUEST_BODY_LEN];
    write_snap_request_body(&mut b, &SnapRequestBody { session: 7, position: 8192 });
    assert_eq!(read_snap_request_body(&b), Some(SnapRequestBody { session: 7, position: 8192 }));
    assert!(read_snap_request_body(&b[..11]).is_none());
    let mut r = [0u8; SNAP_REDIRECT_BODY_LEN];
    write_snap_redirect_body(&mut r, &SnapRedirectBody { session: 7, learner_id: 3, position: 8192 });
    assert_eq!(read_snap_redirect_body(&r).unwrap().learner_id, 3);
}
```

- [ ] **Step 2: Run to verify failure** — `cargo test -p uc_protocol snapshot_frame learner_flag snap_request` → compile errors.
- [ ] **Step 3: Implement** — constants with doc comments citing spec §5.1/§5.7; the two bodies mirror `SnapNakBody`'s codec shape (`datagram.rs` near `SNAP_NAK_BODY_LEN`). Fuzz: add `DGRAM_KIND_SNAP_REQUEST`/`_REDIRECT` arms to the datagram target's kind dispatch; seeds with valid bodies.
- [ ] **Step 4: Run** — `cargo test -p uc_protocol && (cd fuzz && RUSTFLAGS="--cfg fuzzing" cargo +nightly check)`.
- [ ] **Step 5: Commit** — `feat(uc_protocol): SNAPSHOT frame + standby flag, NODE_FLAG_LEARNER, capability bit, ops 8/9, SNAP_REQUEST/REDIRECT (spec §5, §7)`.

---

### Task 2: `append_snapshot`

**Files:** `uc_log/src/buffer.rs` (beside `append_timer` `:873`); tests.

**Interfaces:** `pub fn append_snapshot(&mut self, term: u32, flags: u8) -> Result<(u64, u64), AppendError>` — empty body, `frame_type = FRAME_TYPE_SNAPSHOT`, `flags` written verbatim, stamped `max(now, last_stamp)` like a client frame (it is not a timer: it takes the pass's `now`, never a deadline), returns `(frame_end, stamp)`.

- [ ] **Step 1: Failing test** — `append_snapshot_is_an_empty_stamped_frame_with_flags`: append with `FLAG_SNAPSHOT_STANDBY`, read back, assert type 7, `length == HEADER_LEN`, flags set, `time_ns == now`.
- [ ] **Step 2: Run** → compile error. **Step 3: Implement** by copying `append_timer`'s claim/write/commit shape with a zero-length body and the client clamp. **Step 4: Run** `cargo test -p uc_log`. **Step 5: Commit** — `feat(uc_log): append_snapshot — the empty, flagged SNAPSHOT frame (spec §5.1)`.

---

### Task 3: the service freezes at P; `SnapshotPolicy` goes; the capability bit

**Files:**
- Modify: `uc_service/src/apply.rs` (the arm; `on_snapshot_frame`; `SnapshotTrigger` loses `policy`/`last_snapshot_pos`; `maybe_build_snapshot` deleted; `ApplyState` gains `standby_ok: bool` cached per cycle from `NODE_FLAG_LEARNER`)
- Modify: `uc_service/src/config.rs` (`SnapshotPolicy` and `ServiceConfig::snapshot_policy` deleted), `uc_service/src/lib.rs` (`start_with_snapshots` sets the capability bit; the interval seeding at `:236–45` deleted), `uc_service/src/attach.rs` (slot status: capability bit OR'd in when the caller says so), `uc_service/src/builder_agent.rs` (unchanged in shape; the job's position is P)
- Test: `uc_service/src/apply.rs` tests; every caller of `snapshot_policy(...)`/`SnapshotPolicy` in the workspace (`uc_node/tests`, `examples/`, `uc_crashtest`, `uc_lincheck`) updated — grep `SnapshotPolicy`

**Interfaces:**
- Produces: in the apply loop, on `FRAME_TYPE_SNAPSHOT` at frame-end P: if `snapshot_trigger.is_none()` → ignore (not capable; spec §5.2); else if `hdr.flags & FLAG_SNAPSHOT_STANDBY != 0 && !st.is_learner` → ignore (a voter on a standby instant; §5.7); else if `busy` → `SNAPSHOT_SKIPPED_BUSY.fetch_add(1)` and ignore (the row is incomplete for this instant, §10); else `freeze()` and `tx.try_send((P, job))`. `ApplyState.is_learner` is read with `is_leader` from the same flags word each cycle. `start_with_snapshots` ORs `CNC_SVC_STATUS_SNAPSHOT_CAPABLE` into the slot's status word at attach (a `Release` store after the attach handshake, like the attached bit).

- [ ] **Step 1: Failing tests**

```rust
#[test]
fn a_snapshot_frame_freezes_at_its_frame_end_after_everything_below_it() {
    let (mut st, cnc, builder_rx) = apply_state_with_snapshots_for_test(CountSm::default());
    cnc.status().flags.store_release(NODE_FLAG_LEADER);
    let end_c = append_and_commit(&st, &[b"inc", b"inc"]);
    let end_s = append_snapshot_and_commit(&st, 0);
    apply_cycle(&mut st);
    let (pos, job) = builder_rx.try_recv().expect("a build job at P");
    assert_eq!(pos, end_s);
    let mut img = Vec::new();
    job(&mut img).unwrap();
    assert_eq!(CountSm::decode(&img).count, 2, "frozen AFTER the two incs below P");
    assert!(end_c < end_s);
}

#[test]
fn a_standby_instant_is_ignored_by_a_voter_and_taken_by_a_learner() {
    let (mut st, cnc, builder_rx) = apply_state_with_snapshots_for_test(CountSm::default());
    cnc.status().flags.store_release(0);                       // voter (follower)
    append_snapshot_and_commit(&st, FLAG_SNAPSHOT_STANDBY);
    apply_cycle(&mut st);
    assert!(builder_rx.try_recv().is_err(), "voter ignores a standby instant");
    cnc.status().flags.store_release(NODE_FLAG_LEARNER);
    append_snapshot_and_commit(&st, FLAG_SNAPSHOT_STANDBY);
    apply_cycle(&mut st);
    assert!(builder_rx.try_recv().is_ok(), "learner takes it");
}

#[test]
fn a_non_capable_service_ignores_the_frame_and_a_busy_one_counts_the_skip() {
    let (mut st, cnc, _rx) = apply_state_for_test(CountSm::default());     // plain start(): no trigger
    cnc.status().flags.store_release(NODE_FLAG_LEADER);
    append_snapshot_and_commit(&st, 0);
    apply_cycle(&mut st);                                                    // must not panic, must not build
    let (mut st, cnc, _rx) = apply_state_with_snapshots_for_test(CountSm::default());
    cnc.status().flags.store_release(NODE_FLAG_LEADER);
    st.snapshot_trigger.as_ref().unwrap().busy.store(true, Ordering::Release);
    let before = SNAPSHOT_SKIPPED_BUSY.load(Ordering::Relaxed);
    append_snapshot_and_commit(&st, 0);
    apply_cycle(&mut st);
    assert_eq!(SNAPSHOT_SKIPPED_BUSY.load(Ordering::Relaxed), before + 1);
}
```

- [ ] **Step 2: Run** → compile errors.
- [ ] **Step 3: Implement**

In `apply_cycle`, after the `TIMER` arm (`:480–500`):

```rust
} else if hdr.frame_type == FRAME_TYPE_SNAPSHOT {
    let end = pos + align_frame_len(hdr.length as usize) as u64;
    on_snapshot_frame(st, &mut sm, end, hdr.flags, is_leader_or_learner_flags);
}
```

```rust
/// Spec §5.2/§5.7. Out of line on purpose (M14a): the hot arm is one type
/// test and a call.
#[inline(never)]
fn on_snapshot_frame<S: RawStateMachine>(st: &mut ApplyState<S>, sm: &mut S, p: u64, flags: u8, node_flags: u64) {
    let Some(trig) = st.snapshot_trigger.as_mut() else { return; };
    if flags & FLAG_SNAPSHOT_STANDBY != 0 && node_flags & NODE_FLAG_LEARNER == 0 {
        return;
    }
    if trig.busy.load(Ordering::Acquire) {
        SNAPSHOT_SKIPPED_BUSY.fetch_add(1, Ordering::Relaxed);
        return;
    }
    match (trig.freeze)(sm) {
        Ok(job) => {
            trig.busy.store(true, Ordering::Release);
            if trig.tx.try_send((p, job)).is_err() {
                trig.busy.store(false, Ordering::Release);
                SNAPSHOT_SKIPPED_BUSY.fetch_add(1, Ordering::Relaxed);
            }
        }
        Err(e) => {
            SNAPSHOT_FREEZE_FAILED.fetch_add(1, Ordering::Relaxed);
            eprintln!("freeze at {p} failed: {e}");
        }
    }
}
```

(Match the existing `freeze`/`busy` handoff in `maybe_build_snapshot` `:627–700` exactly for the `FreezeFn` call shape, then delete that function, `SnapshotTrigger.policy`, `.last_snapshot_pos`, `SnapshotPolicy`, `ServiceConfig::snapshot_policy`, and the interval seeding in `start_with_snapshots`.) Read the node flags word once per cycle where `is_leader` is read (`:421`): `let node_flags = st.cnc.status().flags.load_acquire(); let is_leader = node_flags & NODE_FLAG_LEADER != 0;`. In `start_with_snapshots`, after attach: `slot(&cnc, service_id).status.fetch_or(CNC_SVC_STATUS_SNAPSHOT_CAPABLE, Ordering::AcqRel)` (or the equivalent read-modify-write the slot exposes).

- [ ] **Step 4: Run** — `cargo test -p uc_service && cargo test --workspace --exclude uc_node` (every `SnapshotPolicy` caller compiles again) and `cargo test -p uc_node --test purge_safety --test learner` (these drove snapshots by policy; convert them to command an instant through the leader — Task 5's `Node::command_snapshot()` — and until Task 5 lands, mark them `#[ignore = "plan 2 task 5"]` in this commit and un-ignore there).
- [ ] **Step 5: Commit** — `feat(uc_service): freeze at the SNAPSHOT frame's position; SnapshotPolicy goes; capability bit at attach (spec §5.2, §5.7)`.

---

### Task 4: the cluster agent freezes at P; the bridging trigger goes

**Files:** `uc_node/src/cluster_agent.rs`; tests.

**Interfaces:** in `do_work`'s frame loop, `FRAME_TYPE_SNAPSHOT` at frame-end P → if standby-flagged and this node is not a learner (read `cnc.status().flags & NODE_FLAG_LEARNER`), ignore; else `self.take_snapshot_at(P)` — `freeze()` returns `applied`, which equals P only if the cluster FSM applied nothing between the last `CLUSTER` frame and P; so `take_snapshot_at(p)` sets `self.fsm` state's `applied = p` before freezing (a `ClusterFsm::mark_applied(p)` that only advances) so the artifact is keyed by P. `bridging_trigger` and `set_declared_rows_for_test` deleted.

- [ ] **Step 1: Failing test** — `the_cluster_agent_freezes_at_a_snapshot_frames_position`: append `Settings` then `SNAPSHOT`, commit both, `do_work`, assert `snapshot_pos() == end_of_snapshot_frame` and the artifact recovers with the setting. And `a_standby_instant_is_ignored_unless_this_node_is_a_learner` (flags word 0 → no artifact; `NODE_FLAG_LEARNER` → artifact).
- [ ] **Step 2: Run** → red. **Step 3: Implement** as above; delete the bridging trigger and its test. **Step 4: Run** `cargo test -p uc_node --lib cluster_agent`. **Step 5: Commit** — `feat(uc_node): the cluster agent freezes at the instant; plan 1's bridging trigger goes (spec §5.2, §14)`.

---

### Task 5: the consensus agent — command, completeness, floor, cadence, retention, the learner flag

**Files:**
- Modify: `uc_node/src/node.rs`: `Consensus` gains `snapshot_last_commanded: u64`, `snapshot_last_commanded_bytes: u64` (append position when commanded), `snapshot_set_position: Arc<AtomicU64>`, `snapshot_instant_pub: Arc<AtomicU64>`, `snapshot_row_incomplete: [Arc<AtomicU64>; 8]`; `command_snapshot(&mut self, standby: bool) -> (u32, u32, u64)` (op 8's body); `check_set_completeness(&mut self)` per pass; `maybe_issue_cadence_snapshot(&mut self)` per pass on the leader; `publish_status` adds `NODE_FLAG_LEARNER` from `self.sm.config().is_learner(self.id)`; `maybe_persist_snapshot_floor` reads `snapshot_set_position` instead of the page-1 min; `publish_service_mins` keeps writing the page-1 min for observability only; `snapshot_set_for` ships the artifacts **at `node_snapshot_floor`** (every `SnapArtifact.snapshot_pos == floor`, the cluster artifact at the floor too — `SNAP_DECLINE_MISSING` if any is absent); retention: after a set at P completes, `SnapshotStore::retain_newest`-style pruning under `snapshots/<row>/` and `snapshots/cluster/` keeps P and anything newer, deletes older (the node owns pruning now — the service's `retain_newest(2)` at `snapshots.rs:119` is deleted in Task 3 or here); `handle_admin` dispatches ops 8 and 9 (9 is a node-local request: `self.request_fetch(from_learner_id, position)` — Task 6 sends the datagram)
- Modify: `uc_node/src/audit.rs` (`8 => "snapshot"`, `9 => "snapshot_fetch"`)
- Test: `node.rs` tests

**Interfaces:**
- Produces: `Node::command_snapshot(&self, standby: bool) -> Result<u64, SnapshotRefusal>` (for tests and `uc2ctl`); `Node::snapshot_set_position(&self) -> u64`; `Node::snapshot_instant_position(&self) -> u64`. Refusals: `48` if any declared row's slot status lacks `CNC_SVC_STATUS_SNAPSHOT_CAPABLE` (naming the row in the audit `detail`); `49` if `standby` and `!self.sm.config().learners.is_empty()` is false; `2` (`retry`) on a follower; **single in flight with supersession**: refuse `2` while `self.snapshot_last_commanded > snapshot_set_position` **unless** `append - snapshot_last_commanded_bytes >= interval` (cadence) or the request is an explicit `uc2ctl snapshot` (always supersedes); on supersession emit `snapshot_instant_abandoned` with the rows whose `snapshot_pos != last_commanded`.

- [ ] **Step 1: Failing tests**

```rust
#[test]
fn a_commanded_instant_completes_when_every_row_and_the_cluster_reach_it_and_moves_the_floor() {
    let mut h = harness_with_rows(&["a", "b"]);
    drive_to_serving_leader(&mut h);
    mark_capable(&h, 0); mark_capable(&h, 1);
    let p = h.cons.command_snapshot(false).unwrap();
    assert_eq!(h.cons.snapshot_instant_pub.load(Ordering::Relaxed), p);
    h.cons.do_work();
    assert_eq!(h.cons.snapshot_set_position.load(Ordering::Relaxed), 0, "incomplete: nobody froze yet");
    h.cnc.service_slot(0).snapshot_pos.store_release(p);
    h.cons.do_work();
    assert_eq!(h.cons.snapshot_set_position.load(Ordering::Relaxed), 0, "row 1 and the cluster still missing");
    h.cnc.service_slot(1).snapshot_pos.store_release(p);
    h.cluster_snapshot_pos.store(p, Ordering::Release);
    h.cons.do_work();
    assert_eq!(h.cons.snapshot_set_position.load(Ordering::Relaxed), p);
    h.advance_floor_timer();
    h.cons.do_work();
    assert_eq!(h.cnc.snapshots().node_snapshot_floor.load_acquire(), p, "floor := P on completion");
}

#[test]
fn an_incapable_row_refuses_the_command_by_name_and_a_standby_needs_a_learner() {
    let mut h = harness_with_rows(&["a"]);
    drive_to_serving_leader(&mut h);
    assert_eq!(h.cons.command_snapshot(false).unwrap_err().code(), 48);
    mark_capable(&h, 0);
    assert_eq!(h.cons.command_snapshot(true).unwrap_err().code(), 49, "no learner in the config");
}

#[test]
fn single_in_flight_supersedes_after_an_interval_and_counts_the_abandoned_rows() {
    let mut h = harness_with_rows(&["a"]);
    drive_to_serving_leader(&mut h);
    mark_capable(&h, 0);
    h.set_settings_interval(4096);
    let p1 = h.cons.command_snapshot(false).unwrap();
    assert_eq!(h.cons.command_snapshot(false).unwrap_err().code(), 2, "in flight");
    h.append_client_bytes(4096);
    let p2 = h.cons.command_snapshot(false).unwrap();
    assert!(p2 > p1);
    assert_eq!(h.cons.snapshot_row_incomplete[0].load(Ordering::Relaxed), 1, "row 0 never reached p1");
    assert!(h.log_contains("snapshot_instant_abandoned"));
}

#[test]
fn the_learner_flag_follows_the_kernels_shadow() {
    let mut h = harness();
    assert_eq!(h.cnc.status().flags.load_acquire() & NODE_FLAG_LEARNER, 0);
    demote_self_to_learner(&mut h);   // feed a ConfigObserved demoting id 1
    h.cons.do_work();
    assert_ne!(h.cnc.status().flags.load_acquire() & NODE_FLAG_LEARNER, 0);
}
```

- [ ] **Step 2: Run** → compile errors. **Step 3: Implement** per the Interfaces block; `check_set_completeness` is one loop over declared slots plus one load of `cluster_snapshot_pos`, all `Acquire`, run once per pass after `publish_service_mins`. Cadence: on the leader, `if interval > 0 && append - last_commanded_bytes >= interval { self.command_snapshot(target == Learners) }`. Retention runs on the consensus agent **off the hot path**: only inside the completion branch (rare), deleting files under each row's dir and `snapshots/cluster/` with position `< P`, best-effort, counted on error. Un-ignore Task 3's two tests and convert them to `command_snapshot`.
- [ ] **Step 4: Run** — `cargo test -p uc_node --lib && cargo test -p uc_node --test purge_safety --test learner --test timers --test admin_auth`.
- [ ] **Step 5: Commit** — `feat(uc_node): commanded instants — completeness, floor := P, cadence, supersession, retention, NODE_FLAG_LEARNER (spec §5.3–5.5, §5.7)`.

---

### Task 6: the session — one position, `SNAP_REQUEST` serving, store-only receive, `SNAP_REDIRECT`

**Files:**
- Modify: `uc_net/src/sender.rs`: `try_open_snap_session(to)` → `try_open_snap_session(to, at: Option<u64>)` (`None` = the source's set at its floor; `Some(p)` = the set at p from the source, refused if absent); a `SNAP_REQUEST` arm in the sender's receive path (the sender agent already handles `SNAP_NAK`; add `SNAP_REQUEST` beside it) calling `try_open_snap_session(from, Some(body.position))`; the one-position rule: every `SnapArtifact.snapshot_pos` in a set must equal the set's position or the set is refused (extend `set_is_valid`).
- Modify: `uc_net/src/receiver.rs`: `SnapIntake` gains `mode: IntakeMode { Install, StoreOnly }`; a `SNAP_BEGIN` whose `snapshot_pos` differs from the intake's first is refused (`snapshot_session_refusals.3` — a fourth slot; or reuse the identity slot with a distinct log reason `position_mismatch`); `snap_complete` in `StoreOnly` mode: write the artifacts (already done by the chunk path), send `SNAP_DONE`, do **not** publish `incoming_snapshot_pos`, do not set `snap_adopt_pending`; instead store `stored_set_pos.store(p)` for Task 5's completeness loop to pick up (the node treats a stored set exactly like a locally-produced one — every slot's `snapshot_pos` is set by the receiver writing the artifact into the row's dir and storing the marker, the same word the builder writes); a `SNAP_REDIRECT` arm: on receipt, if `self.snap_intake.is_none()`, send `SNAP_REQUEST { session: fresh, position }` to the learner's address (resolved through the peer map) and open an `Install`-mode intake keyed to it.
- Modify: `uc_node/src/node.rs`: the leader, on a below-floor NAK it cannot serve (`snapshot_set_for` declined `SNAP_DECLINE_MISSING` because the set at its floor is absent — the standby case), sends `SNAP_REDIRECT { learner_id, position: floor }` to the NAKing peer for the first learner whose last reported `snapshot_set_position` (a new field on the STATUS/report datagram? — **no new field**: the leader uses the learner it most recently commanded a standby instant to, and the position it commanded; a learner that has not completed answers the request with nothing and the joiner re-NAKs, as today); `request_fetch(learner_id, position)` (op 9's body) sends `SNAP_REQUEST` from this voter and opens a `StoreOnly` intake.
- Test: `uc_net` unit tests (one-position rule; request→session; store-only completion publishes no position); `uc_node/tests/learner.rs` in Task 10.

**Interfaces:** `IntakeMode`, `Receiver::open_store_only_intake(peer, session, position)`, `Receiver::stored_set_pos: Arc<AtomicU64>`; `Sender::on_snap_request(from, body)`; `Node::request_fetch(learner_id: NodeId, position: Option<u64>) -> Result<(), FetchRefusal>` (`position: None` = ask the learner for its newest complete set: `SNAP_REQUEST.position = 0` means "newest").

- [ ] **Step 1: Failing tests** (`uc_net`): `a_session_whose_begins_disagree_on_position_is_refused`; `a_snap_request_opens_a_session_for_the_set_at_that_position` (sender with a source holding sets at 4096 and 8192; request 4096 → BEGINs carry 4096); `store_only_completion_writes_artifacts_and_publishes_no_position` (receiver intake in `StoreOnly`, drive a two-artifact session to completion, assert files exist, `incoming_snapshot_pos` untouched, `stored_set_pos == p`).
- [ ] **Step 2: Run** → red. **Step 3: Implement** per the Interfaces block; the `SnapshotSource` closure gains the `at: Option<u64>` argument (`snapshot_set_for(…, at)` — `None` reads the floor; `Some(p)` reads `snapshots/<row>/snap-{p}.ultsnap` + `snapshots/cluster/snap-{p}.ultcluster`). **Step 4: Run** `cargo test -p uc_net -p uc_node --lib`. **Step 5: Commit** — `feat(uc_net,uc_node): one-position sets, SNAP_REQUEST serving, store-only receive, SNAP_REDIRECT (spec §5.6, §5.7)`.

---

### Task 7: `uc2ctl snapshot [--standby]`, `snapshot fetch`, `snapshot show`

**Files:** `uc_ctl/src/snapshot.rs` (new), `uc_ctl/src/main.rs` (`Cmd::Snapshot(SnapshotArgs)` with subcommands `Take { standby: bool }`, `Fetch { from: u32, position: Option<u64> }`, `Show`), `uc_ctl`'s reason table (48, 49), `uc_node/src/audit.rs`.

**Interfaces:** `snapshot` (default subcommand `take`) sends op 8 with `id = standby as u32`; prints `instant=<P>` on `0`, the leader hint on `2`, the reason name on refusal. `snapshot fetch --from <id> [--position P]` sends op 9 with `id = learner_id`, `ip`/`port` carrying `position` split as the digest fields do (`ip = (p >> 16) as u32`, `port = p as u16` — **or** simply `id = learner_id, ip = p as u32, port = (p >> 32) as u16` — choose one, document it in `uc2ctl.md`, pin it). `snapshot show` reads `snapshots/*/` and prints, per row and for `cluster`, the newest artifact position, and `set=<P>` for the newest position every row and the cluster share, `set=none` otherwise.

- [ ] **Step 1: Failing tests** — `uc_node/tests/admin_auth.rs`: a one-node cluster with a capable service: `uc_ctl::snapshot::take(false)` → `instant=P`; wait for `node.snapshot_set_position() == P`; `show` contains `set=P`. And `take(true)` on a cluster with no learner → the reason name `snapshot_no_learner`.
- [ ] **Step 2: Run** → red. **Step 3: Implement** by mirroring `uc_ctl/src/settings.rs`'s request path (no staged file: these ops carry everything in the 64-byte line). **Step 4: Run** `cargo test -p uc_ctl && cargo test -p uc_node --test admin_auth`. **Step 5: Commit** — `feat(uc_ctl): snapshot [--standby], snapshot fetch, snapshot show (spec §8)`.

---

### Task 8: metrics, alerts, scenarios, builders

**Files:** `uc_node/src/obs/metrics.rs` (five metrics), `uc_node/src/node.rs` (the `MetricsSnapshot` fields), `packaging/prometheus/uc2-alerts.yml` (two rules), `uc_node/examples/m10_alerts.rs` (`scenario_snapshot_stalled`, `scenario_snapshot_set_diverged`), `scripts/m10_alert_fire.sh` (two `RULE_BUILDERS`), `docs/how-to/monitor-a-cluster.md` (Task 11 writes the prose; here only the name list).

**Interfaces:**
- `uc2_snapshot_instant_position` (gauge, leader), `uc2_snapshot_set_position` (gauge, every node), `uc2_snapshot_row_incomplete_total{row}` (counter), `uc2_snapshot_fetched_position` (gauge), and `uc2_snapshot_freeze_seconds{row}` — **check first** whether `metrics.rs` renders histograms; it renders gauges and counters only (`:430–470`), so export `uc2_snapshot_freeze_seconds_max{row}` (gauge, reset per instant) and `uc2_snapshot_freeze_seconds_sum{row}`/`_count{row}` (counters) — three series that give a mean and a worst case without a histogram type. The service measures the freeze (`Instant` around `(trig.freeze)(sm)`) and publishes it in the slot's reserved band (one `u64` ns at slot `+496`, node-read).
- `Uc2SnapshotStalled`: `uc2_snapshot_instant_position - on(instance) uc2_snapshot_set_position > 0` held for `> 2 × the cadence interval` — expressed as "the instant position has advanced twice while the set position did not": `changes(uc2_snapshot_instant_position[30m]) >= 2 and changes(uc2_snapshot_set_position[30m]) == 0`, severity `warning`, annotation naming the remedy (`uc2ctl snapshot show` on the leader; look for the row whose `snapshot_pos` is behind). `Uc2SnapshotSetDiverged`: the `count_values` idiom over `uc2_snapshot_set_position`, `Uc2ScheduleTableDiverged`'s shape verbatim. Both scenarios are synthetic-state/real-exporter like `scenario_schedule_diverged`; both builders copy `build_Uc2ScheduleTableDiverged`'s shape.

- [ ] **Step 1: Failing check** — `scripts/m10_alert_fire.sh --check-complete` (the completeness cross-check) lists the two new rules as missing builders after the YAML is edited.
- [ ] **Step 2: Implement** all four files. **Step 3: Run** `cargo run -p uc_node --example m10_alerts -- snapshot_stalled snapshot_set_diverged && scripts/m10_alert_fire.sh --check-complete` (needs `promtool`; if absent locally, the check-complete step alone must pass). **Step 4: Commit** — `feat(uc_node): snapshot metrics; Uc2SnapshotStalled and Uc2SnapshotSetDiverged with scenarios and builders (spec §9)`.

---

### Task 9: sim — inv11 set alignment, and a truncated instant

**Files:** `uc_sim/src/world.rs`, `uc_sim/src/invariants.rs`, `uc_sim/tests/scenarios.rs`.

**Interfaces:** the world gains `snapshot_frames: Vec<SnapFrame { end: u64, term: u32, standby: bool }>` appended by the leader on a new `SimEvent::CommandSnapshot`, and per node `complete_sets: Vec<u64>` appended when the node's durable/commit have passed a `SnapFrame.end` (the sim has no rows; a set is "complete" when the node has applied to `end`); inv11: for any two nodes, one's `complete_sets` is a prefix of the other's, **excluding** positions above the shorter node's commit; and no `complete_sets` entry is above that node's commit. Scenario: `a_snapshot_frame_truncated_by_a_leader_change_never_becomes_a_complete_set` — command an instant, partition the leader before commit, let a new leader truncate it, assert no node ever lists that position and inv11 holds.

- [ ] **Step 1: Failing tests** (both). **Step 2: Run** → red. **Step 3: Implement**. **Step 4: Run** `cargo test -p uc_sim` and `--features sim-heavy` once. **Step 5: Commit** — `test(uc_sim): inv11 set alignment; a truncated instant is never a set (spec §11)`.

---

### Task 10: integration — instants under load, standby, fetch, redirect, SIGKILL mid-build

**Files:** `uc_node/tests/timers.rs`, `uc_node/tests/learner.rs`, `examples/uc_crashtest/tests/hard_crash.rs` (or wherever `two_fsm_service_sigkill` lives), `uc_node/tests/lin_v2.rs` (the churn helper).

- [ ] **Step 1: `timers.rs`** — `a_commanded_instant_freezes_every_row_at_one_position_under_load_and_ordering_holds`: two rows, `Timed<ClockSm>` and `TaggedSum`, a client load loop, `node.command_snapshot(false)` three times spaced by writes; assert each row's `snapshot_pos` sequence equals the cluster's and equals the instants; `check_frames` (the `uc_sim::timers` oracle, already a dev-dep) over the frames still holds; commit never stalled for longer than one pass between instants (assert `commit` advanced between each pair of instants).
- [ ] **Step 2: `learner.rs`** — `a_standby_instant_freezes_only_the_learner_and_voters_applied_keep_moving` (`spawn_cluster_with_learner(3, 1)`; command `standby`; assert every voter's rows' `snapshot_pos` unchanged and `applied` advancing under `submit_n`, while the learner's set completes); `a_voter_fetches_a_learners_set_store_only_and_its_floor_moves` (then `uc_ctl::snapshot::fetch(from = learner)` on a voter; assert the artifacts landed, the voter's FSMs' `applied` never rewound, `node_snapshot_floor == P`); `a_joiner_below_the_voters_floor_is_redirected_to_the_learner` (purge on; a fresh learner joins; assert its `snapshot_installed` came via a `SNAP_REQUEST` to the learner — an obs record `snapshot_redirected` on the leader).
- [ ] **Step 3: crashtest** — `snapshot_instant_abandoned_on_service_sigkill_mid_build_and_the_next_completes`: command an instant, SIGKILL the row's service while `busy`, respawn, assert `uc2_snapshot_row_incomplete_total{row} == 1`, command again, assert completion, assert the history is linearizable (the existing checker).
- [ ] **Step 4: `lin_v2`** — the purge/snapshot-churn capstone's churn helper commands instants instead of relying on per-service intervals (which no longer exist); run `cargo test -p uc_node --test lin_v2` once locally (dev-box smoke, not a gate).
- [ ] **Step 5: Run** all four; **Step 6: Commit** — `test(uc_node,uc_crashtest): instants under load, standby, fetch, redirect, SIGKILL mid-build (spec §11)`.

---

### Task 11: docs

**Files:** `docs/reference/{uc2ctl,wire-protocol,cnc-page,configuration,instance-directory,limits}.md`, `docs/how-to/{monitor-a-cluster,bound-journal-growth,change-cluster-membership,schedule-work-in-a-service,run-work-on-a-schedule}.md`, `docs/notes/uc2-cluster-fsm-explained.md` (the instants section), `docs/notes/uc2-m14-multi-service-explained.md` (the snapshot-set paragraph), `RELEASES.md`, `docs/releases.md`, `CLAUDE.md`, `docs/VERIFICATION.md`, `docs/BACKLOG.md`.

- [ ] **Step 1:** `bound-journal-growth.md` is the one that changes most: "snapshots first, then purging" becomes "command an instant (`uc2ctl snapshot`) or set `snapshot.interval_bytes`; purge follows the set"; the per-service `SnapshotPolicy` paragraph goes. `limits.md`: the freeze-on-a-quorum stall as a stated limit with the `P + fsm_lag` formula; `TIMERS_PER_PASS` unchanged. `monitor-a-cluster.md`: the five metrics and two alerts. `uc2ctl.md`: three verbs, ops 8/9, refusals 48/49. `wire-protocol.md`: frame 7 + flag, kinds 22/23, V4 one-position rule. `cnc-page.md`: `NODE_FLAG_LEARNER`, slot bit 9, slot `+496`.
- [ ] **Step 2:** link-check; `cargo test --workspace --doc`. **Step 3: Commit** — `docs: coordinated and standby snapshot instants (spec §12, plan 2)`.

---

## Self-review

**Spec coverage (plan 2's share):** §5.1 → Tasks 1, 2; §5.2 → Tasks 3, 4; §5.3 → Task 5; §5.4 → Task 5 (`snapshot_set_for` at the floor) + Task 6 (one-position rule); §5.5 → Task 5 (+ Task 7 for the verb); §5.6 → Task 6; §5.7 → Tasks 0, 1, 3, 4, 5, 6, 7, 10; §6 (`target`) → Task 5's cadence; §7 (plan-2 rows) → Task 1; §8 → Task 7; §9 → Task 8; §10 → Tasks 3 (busy → incomplete), 5 (supersession, abandoned record), 9 (truncated instant), 10 (SIGKILL); §11 (plan-2 tests) → Tasks 9, 10; §12 → Task 11; §15 check 4 → Task 6's store-only mode is the answer (a voter above P never installs, so the question does not arise).

**Placeholder scan:** Task 7 leaves one encoding choice open (how `position` rides the 64-byte admin line for op 9) with both options spelled out and an instruction to pin whichever is chosen — a decision, not a gap. Task 8's histogram fallback is decided in the text (three series). No "TBD".

**Type consistency:** `FLAG_SNAPSHOT_STANDBY`, `NODE_FLAG_LEARNER`, `CNC_SVC_STATUS_SNAPSHOT_CAPABLE`, `SNAPSHOT_SKIPPED_BUSY` used identically across Tasks 1, 3, 4, 5; `snapshot_set_position`/`snapshot_instant_pub`/`snapshot_row_incomplete` in Tasks 5, 8, 10; `IntakeMode::{Install, StoreOnly}`, `stored_set_pos`, `request_fetch` in Tasks 6, 7, 10; `try_open_snap_session(to, at)` in Task 6 only.
