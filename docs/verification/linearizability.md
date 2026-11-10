# Linearizability: the WGL capstones

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

A history of concurrent operations is linearizable when every operation can be given one instant between its call and its return, and the operations taken in that order behave like a single correct copy. The Wing-Gong-Lowe search, WGL for short, checks this by trying orderings of the recorded history until it finds one that works or proves that none exists.

This tier runs that check against a real cluster under faults. The evidence is a recorded history that the WGL checker accepts as linearizable. The checker lives in [`uc_lincheck/`](/uc_lincheck), and the capstone tests live in `uc_node/tests`.

## What a capstone does

The harness records a history of operations on a concurrent compare-and-swap register, a CAS register for short. The WGL search then checks that history for linearizability. While the workers run, the harness kills leaders, crashes services and partitions the network. In the M6 tier it also runs snapshot-backed purge underneath.

Nothing is simulated. The tests use real node and service agents. They replicate over real reliable-UDP on loopback. They use real instance directories.

## The two-FSM capstones

The two-FSM capstones shipped in `2.8.1`, as part of M14c2. Every node runs two attached finite state machines, FSMs for short. Each FSM sits in a row, one numbered state-machine slot on the node. FSM 0 is row 0 and FSM 1 is row 1. The same checker judges **one history per FSM**.

A second oracle sits on top. An oracle is the rule that decides pass or fail. This one checks **replication equivalence**. Every `submit_all`'s per-FSM answers must be byte-equal. A disagreement is counted. It is recorded as `Indeterminate` in both histories. It is never silently taken from FSM 0.

| test | file | what it drives |
|---|---|---|
| `two_fsm_bounded` | `uc_node/tests/lin_v2.rs` | Two FSMs at `FsmLag::Bounded(64 KiB)` under failover and purge/snapshot churn. |
| `two_fsm_lockstep` | `uc_node/tests/lin_v2.rs` | The same faults at `FsmLag::Lockstep`. |
| `two_fsm_slow` | `uc_node/tests/lin_v2.rs` | A fast FSM beside `Slow<RegisterSm, 200>`, bounded. The slow FSM takes 200 µs per apply. |
| `two_fsm_slow_lockstep` | `uc_node/tests/lin_v2.rs` | The fast and slow FSMs of `two_fsm_slow`, in lockstep. |
| `minority_partition_and_heal_two_fsm` | `uc_node/tests/lin_partition_v2.rs` | A minority partition, quorum loss and heal. Per-FSM WGL runs before and after. |
| `two_fsm_service_sigkill` | `testing/uc_crashtest/tests/hard_crash.rs` | See [hard crash](hard-crash.md). FSM 1's process is `SIGKILL`ed mid-load. |
| `two_fsm_node_sigkill` | `testing/uc_crashtest/tests/hard_crash.rs` | See [hard crash](hard-crash.md). The node and both services are killed together. |
| `two_fsm_timer_churn_under_failover` | `uc_node/tests/lin_v2.rs` | Added in `2.11.0`. Row 1 is `Timed<TimerSm>` under sustained schedule/cancel churn. Node kills force leader changes while instances are in flight. Every node's `fired`/`scheduled`/`cancelled` record must agree. That is replication equivalence. The records must also pass the shared `uc_lincheck::timer::assert_timer_report` oracle. |

Two more checks keep the two-FSM capstones honest. They add no coverage of their own. The first is a test that makes the equivalence oracle fail on purpose. The second is the slow-FSM oracle, together with what it does not show.

### The equivalence oracle is shown to bite

The test `two_fsm_oracle_bites` in `lin_v2.rs` runs the same equivalence oracle the capstones run. It is marked `#[should_panic(expected = "replication-equivalence violated")]`.

It puts FSM 1 behind `Corrupt<RegisterSm>`. Then it drives `lincheck_v2::spawn_workers2`. That is the capstones' own worker loop. Its `a != b` arm is what feeds `equiv_failures`. The test then makes the identical `equiv_failures == 0` assertion that `run_two_fsm` makes.

The test is deliberately not an inline, hand-rolled comparison. Weakening the workers' fan-in check has to stop this test panicking. An oracle that has never been made to fail is not evidence.

### The slow-FSM oracle, and what it does not show

The slow-FSM oracle is what `two_fsm_slow*` actually assert, beyond linearizability. It has two parts. First, at every 50 ms sample, the FSMs' separation stays inside the policy. Bounded means at most `fsm_lag`. Lockstep means at most one 288 B frame. Second, over the run's second half, their apply rates agree within 10 %. The measured ratio was 1.000 in both runs.

What the slow-FSM oracle does **not** show was measured on 2026-08-30. This was dev-box smoke, never a gate. Both FSMs progressed at the same rate. The separation never approached the bound.

- `two_fsm_slow` ran at `Bounded(64 KiB)`. It sampled `max_lag = 192 B of 65536`. That is 0.3 % of the bound, about 3.7 frames at about 52 B/frame. It ran at `rate0 = rate1 = 22 111 B/s`.
- `two_fsm_slow_lockstep` sampled `max_lag = 64 B of 288`. That is about 1.2 frames. It ran at `rate0 = rate1 = 22 227 B/s`.

At about 52 B/frame, those byte rates come to about 425 records/s. That is an order of magnitude under the roughly 5 000 applies/s ceiling of `Slow<RegisterSm, 200>`. So the **client loop** is the limiter here, not FSM 1. The lag policy never binds.

**These runs therefore do not exercise a bound-pinned state.** The slow-FSM oracle is evidence of **equal progress** across a heterogeneous pair. It is not evidence of the barrier's behaviour at the bound. The barrier at the bound is covered by `uc_service`'s `lag` unit tests and the apply-hop bench, not here.

## Timers and the schedule table, end to end

The timer and schedule-table tests below shipped in `2.11.0`. They are not linearizability capstones. Their oracle is direct assertion rather than a WGL search. But they give the same kind of evidence, on the same kind of subject. They run real node and service processes over a real instance directory on the cargo target volume. They drive the timer mechanics through the public SDK.

A "watched red" below means the test was deliberately broken to confirm it fails. That proves the test can detect the bug.

**Timers on a row.** Seven tests in `uc_node/tests/timers.rs` cover the timer mechanics.

- `a_scheduled_timer_fires_at_its_deadline_in_order_and_once`
- `the_log_time_seed_survives_a_restart_of_the_same_instance_dir`
- `the_timer_bound_holds_client_frames_for_a_pass`
- `a_timer_in_flight_at_a_leader_change_fires_late_and_is_delivered_once`
- `a_schedule_table_ticks_exactly_once_per_deadline_and_advances_from_the_tick`
- `a_restarted_node_resumes_the_table_with_one_catch_up_tick`
- `a_promoted_below_floor_joiner_keeps_the_schedule_ticking_when_it_leads`

Together they show the following.

- One timer fires once at its deadline, in pass order. A cancelled one never fires.
- The cnc log-time word survives a restart of the same instance dir.
- More than `TIMERS_PER_PASS` due at one instant holds every client frame for a pass. Those timers still fire on time.
- An instance in flight at a leader change is delivered exactly once. It may be late or on time. It is never delivered twice, and never at diverging positions.
- An applied schedule table ticks once per occurrence and advances from the tick. Its `once` entry parks.
- Five periods of downtime are caught up by **one** tick. The already-delivered `once` is not re-delivered.
- A joiner that was below the purge floor keeps the schedule ticking once it is promoted and leading.

**The leader-only timer heap.** The cluster FSM is the node's own internal state machine. `timers_pending_on_the_old_leader_fire_exactly_once_on_the_new_one_after_its_announce`, in `uc_node/tests/timers.rs`, is a cluster-FSM test of the **leader-only heap**, from cluster-FSM spec §4.9. Three nodes run. Two instances are armed on the leader. The leader is crashed with both in flight. Every follower's per-row `timers_pending` cnc word reads `0` across a settle window. The leader's word reads `2`. That is a live reading, republished every pass. The new leader's rising-edge **announce** rebuilds the whole set. It fires the set exactly once. Every surviving service's fired record is compared as a whole struct. So positions, stamps and lateness must agree. The test was watched red by inverting the §4.9 half in place.

**The schedule table on a joiner.** Three tests in `uc_node/tests/learner.rs` cover it.

- `a_fresh_learner_below_the_floor_installs_the_leaders_schedule_table`
- `a_leader_without_a_table_ships_none_and_the_joiner_installs_none`
- `a_joiner_served_by_a_leader_restarted_before_its_first_commit_advance_still_installs_the_table`

They run a real leader and joiner `Node` pair over loopback UDP, under purge. A joiner **below the purge floor** installs the cluster's schedule table. It takes it off the snapshot session's cluster artifact. A leader holding no table ships none. The joiner then installs none, rather than keeping a stale one. The last test covers the residual the cluster FSM exists to close. A leader **restarted before its first commit advance** still ships the committed table. That is because the artifact is committed by construction. It is not gated on a counter that is zeroed at boot.

**Coordinated snapshots.** A commanded instant is a `SNAPSHOT` frame placed on the log. Every declared row freezes at the frame's end position, which these tests call P. A learner is a node that is replicated to but never counted in a quorum. A standby instant freezes only the learners. Four tests cover coordinated snapshots.

- `a_commanded_instant_freezes_every_row_at_one_position_under_load_and_ordering_holds`, in `uc_node/tests/timers.rs`. One node runs **two** declared rows. One is a `Timed<ClockSm>`. The other is a raw byte-sink `SumSm`. They run under a real client load loop mixing commands and timers. Three commanded instants are taken. Each is asserted on four axes. First, both rows' `snapshot_pos`, the cluster artifact's position and `snapshot_set_position` all equal **one** P. Second, `snapshot_instants_abandoned()` is unchanged. So the set completed on the **first** attempt. Ruling P10 makes that bar reachable. Third, commit keeps moving past each P. So the freeze did not park the pipeline. Fourth, the whole log is re-walked through `uc_sim::timers::check_frames`. So the §4.3 timer ordering oracle runs over a log carrying `SNAPSHOT` frames. There is a documented limitation. Rule 5 of the §4.3 oracle is **vacuous** here. It keys on a pass-start stamp that a reader outside the node cannot see. Any boundary inferred from the frame sequence would be inferred from the very ordering rule 5 exists to check. Rules 1–4 are what this test genuinely runs. Rule 5 stays owned by the in-crate differential test.
- `a_standby_instant_freezes_only_the_learner_and_voters_applied_keep_moving`, in `uc_node/tests/learner.rs`. It runs 3 voters and 1 learner, with a **real** `SumSm` service on all four. A declared row with no service attached cannot freeze either way. So a faked fixture would assert nothing. The learner's set completes at P. Every voter's `snapshot_pos`, `snapshot_set_position` and `freeze_ns` are still `0`. So no voter ran `freeze()`. Every voter's `applied` advances past P under fresh load. That is asserted **after** that load, not before it.
- `a_voter_fetches_a_learners_set_store_only_and_its_floor_moves`, in `uc_node/tests/learner.rs`. It uses the standby test's fixture plus a standby instant. Then it calls `request_fetch` on a **follower** voter. The verb is node-local and never forwarded. The negative case runs first. A position above durable is `Err(AboveDurable)`. That is wire reason 50. Then both artifacts land by name under the voter's own instance dir. The voter's `applied` is sampled on every iteration of the completion wait, with a running high-water mark. So an install-then-replay could not hide inside one sample. Its floor moves. `snapshot_set_position` and `snapshot_fetched_position` both equal P. The persisted `node_snapshot_floor` follows. `snapshot_session_refusals()` stays `(0,0,0,0,0)`.
- `a_joiner_below_the_voters_floor_is_redirected_to_the_learner`, in `uc_node/tests/learner.rs`. One voter has its floor-set artifacts **deleted**. The test names it for what it is: a node restored from a backup taken before its own floor. A learner holds a standby set. A fresh joiner starts from 0, below the purged prefix. The leader's `snap_redirects` counter rises. The joiner records `snapshot_redirect_followed`, naming the learner and P. Then it records `snapshot_installed`. Its `archive_first_base` shows it adopted the shipped floor rather than replaying from 0. The **learner's** session counter is what moved. The load-bearing red is skipping the deletion. The joiner then converges off the leader's own intact set, and the redirect assertion fires.

**Refusals by name.** Two tests check that reserved names and retired keys are refused.

- `a_uc_prefixed_fsm_name_is_reserved_and_refused_by_name`, in `uc_node/tests/services.rs`, covers the `uc_` reservation. It is refused at the door, with the name in the message.
- `daemon_refuses_a_services_fsm_lag_pointing_at_settings_apply` and its siblings, in `uc_node/tests/daemon_refusals.rs`, run the **real `uc2-node` binary**. It refuses three configs by name, with exit 2. They are a top-level `admission_bytes`, a `[services] fsm_lag`, and `names = ["uc_cluster"]`. Each refusal points at `uc2ctl settings apply` or at the reservation. The binary **starts** with the same two keys under `[settings]`. That is observed as a bounded poll for its cnc page and its own `node_listening`/`stopped` records.

## One more pin: `snapshot_restart_installs_only_with_purge`

The pin `snapshot_restart_installs_only_with_purge` comes from the M14d row-d lesson. It lives in `lin_v2.rs`.

A snapshot shortens a service restart **only together with purge, and only once the live log buffer has wrapped past `start_pos`**. Below the wrap, a restart reads the still-live ring. It touches neither the journal nor a snapshot, whatever the purge posture.

Coordinated snapshot instants arrived in `2.11.0`. Since then, this test commands one instant in the middle of its writes. It no longer configures a per-service byte interval. So P is a frame end. Everything above it is the tail the restarted service must replay.

## The churn capstones command instants

The churn capstones changed how they take snapshots in `2.11.0`. They are `linearizable_under_purge_and_snapshot_churn`, its `_with_crypto` twin, and both two-FSM arms. They used to drive their purge with the now-retired `SnapshotPolicy { interval_bytes }`.

They now call `cluster.command_instant()` once per fault tick, before the fault. So an instant is commanded while the cluster is still whole. Commanding instants this way is best-effort by design. `command_instant` answers `None` on "no serving leader", and in a fault loop that is the normal case. The loops' existing `max_archive_first_base() > 0` gate adjudicates whether enough instants landed.

A genesis `snapshot_interval_bytes` cadence was rejected for these tests. That cadence re-bases to the leader's append frontier at every leader open. These capstones kill the leader roughly every 1.2 s. So the cadence would often never fire. And what it exercised would be the cadence, not the commanded instant the purge depends on.

## How to run it

```bash
cargo test --workspace          # includes the capstones
```
