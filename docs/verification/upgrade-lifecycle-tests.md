# The FSM upgrade lifecycle: what the tests prove

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

This page covers the tests behind the FSM upgrade lifecycle, the procedure for moving a state machine to a new version. The [simulation](simulation.md) has no cluster-FSM frames, so the evidence here is unit tests and integration tests. Some run in-crate. Others drive real nodes, real services, real UDP sockets and real child processes. Each plan of the feature has its own section. Plans C, B3 and #33 each close with what their tests do NOT verify.

## Terms used on this page

- A **row** is one slot for a state machine in the cluster. Each service attaches to one row.
- An **artifact** is the snapshot file a row writes when the cluster takes a coordinated snapshot instant.
- A **pin** is a committed cluster record that names the version a row must run from a given log position on.
- The **origin** is that position, a coordinated instant `P` whose complete snapshot set exists on every node.
- The **counterfactual** is what the new binary would compute without a pin, by replaying the log from genesis instead of installing the artifact at the origin.

## B1: cluster-FSM unit tests for pins and reports

The schedule table and the settings record are cluster-FSM data. Since `2.13.0` the upgrade pin and the snapshot report are too. The sim cannot reach any of this data, because it has no cluster-FSM frames at all. So their correctness is unit-tested in-crate instead.

### The replicated half

Five tests in `cluster_fsm.rs` cover the replicated half. They pin that pin validation reads replicated state only. They pin the bounded pin history per row, and that the newest report is held per row while a stale one is refused. They pin that pins and reports round-trip through the image, that an old image installs empty, and that the view republishes pins and reports.

- `pin_refusals_are_replicated_state_only`
- `pin_history_is_bounded_per_row`
- `a_report_is_held_newest_per_row_and_a_stale_one_is_refused`
- `pins_and_reports_ride_the_image_and_an_old_image_installs_empty`
- `the_view_publishes_pins_and_reports`

### The door half and the retention exemption

Four tests in `node.rs` cover the door half and the retention exemption. They pin that the door refuses a bad pin request by name. They pin that a pin is appended as a cluster frame and the cnc words follow at commit. They pin that only one pin is in flight at a time, keyed on the view position, and that retention keeps every pinned origin.

- `upgrade_pin_door_refusals_by_name`
- `an_upgrade_pin_is_appended_as_a_cluster_frame_and_the_words_follow_at_commit`
- `upgrade_pin_is_single_in_flight_on_the_view_position`
- `retention_keeps_every_pinned_origin`

### The floor hold

A pinned origin holds the purge floor until the pinned version attaches. A fix round found a residual gap in that floor hold, and two more `node.rs` tests come from it.

| test | what it proves |
|---|---|
| `the_floor_is_held_at_a_pinned_origin_until_the_pinned_version_attaches` | the hold latches at the pinned origin. It clears only once the row is attached at `to` **and** has replayed past the candidate floor. Being merely attached is not enough. |
| `a_detached_row_under_a_pin_still_holds_the_floor` | a row with no service running at all still holds the floor. "Not consumed" cannot distinguish an in-flight upgrade from one that never started. |

## B2: the pinned install at attach

Everything in B1 proves the pin is recorded, replicated and observable. It says nothing about what a service process actually DOES with a pin at attach. `uc_service/tests/pinned_attach.rs` covers that. It has 11 tests. Each one runs a real `Node` and a real `Service<S>` over an on-disk instance directory, with no sim.

### The pinned install and its control

Three tests cover the pinned install itself:

- `the_pinned_version_installs_the_origin_unconditionally_and_recomputes_the_tail`
- `a_durable_sm_above_the_origin_is_rewound_to_it`. A durable state machine already above the origin is rewound to it.
- `the_same_swap_without_a_pin_computes_the_counterfactual` is the control. The same swap with no pin computes the spec §2.3 counterfactual instead.

### The attach refusals

These are the four `ServiceError` attach refusals, plus the envelope's own version cross-check.

| test | refusal it proves |
|---|---|
| `a_stale_binary_is_refused_by_name_after_the_pin` | `PinnedVersionMismatch` |
| `a_contended_pin_read_is_refused_not_ignored` | `PinUnreadable` |
| `a_pinned_row_started_without_snapshots_is_refused` | `PinRequiresSnapshots` |
| `a_pinned_origin_with_no_artifact_is_refused` | `PinnedArtifactMissing` |
| `a_pinned_artifact_built_by_the_wrong_version_is_refused` | `EnvelopeError::VersionMismatch`, surfaced as `MistaggedSnapshot` |

### Snapshot timing and purge

| test | what it proves |
|---|---|
| `a_pinned_attach_prefers_the_origin_over_a_later_artifact` | the gap guard prefers the origin over a newer artifact |
| `a_pinned_attach_survives_a_cadence_instant_after_the_pin` | the attach survives a cadence instant that lands after the pin but before the stop |
| `a_pinned_attach_converges_on_a_purging_cluster` | the attach converges on a **purging** cluster. The next section explains its fixture. |

### Why the purging test is single-row

`a_pinned_attach_converges_on_a_purging_cluster` stands in for a two-row cluster. In that cluster one row is busy enough to purge while a pinned row stays quiet. The test itself is a single-node, single-row fixture. It drives `PURGE_EXTRA_INSTANTS = 400` coordinated instants past the pin's origin on an otherwise-idle row. Only then does the pinned binary attach. That is what actually exercises "the origin's artifact must still be retrievable after the floor has moved past it many times." A real two-row harness would add cross-row scheduling noise without changing what this assertion proves.

### The end-to-end companion

`uc_diffreplay/tests/reconstruction.rs`'s `a_real_pin_makes_the_default_purge_off_swap_install_the_origin` is the end-to-end companion. It sends a real admin `upgrade pin` request over the wire, through a running node. The request is answered by the same code path an operator's `uc2ctl` would drive. The test confirms that the pinned install and the §2.3 counterfactual disagree, `Some(4)` vs `Some(8)`. It does this on the DEFAULT purge-off configuration, not just the purging one above.

## C: the black-box pin-verify suite

Every test above links the service SDK into the test process. `uc2-diffreplay pin-verify` works differently. It is spec §6.2 part 2. It runs the app's real binaries as child processes against a real node. Then it judges what UC's own surfaces say.

The suite is `uc_diffreplay/tests/pin_verify.rs`. It holds five end-to-end cases of pin-verify on the `register-replay` fixture. Each case has a node and one service process per era. OLD is the binary of the era before the swap. NEW is the swapped-in binary. OLD is stopped at a position X above the origin P before the pin. Q is the end of the span that is replayed. The cases that get that far also run a real admin `upgrade pin`. They then run the app binary three more times: one `project` and two `replay`. The same-version case is refused before the pin. The stale-NEW case stops at the un-attached swap arm.

### What the suite proves

| test | what it proves |
|---|---|
| `the_refusal_marker_is_the_sdks_own_text` | pins the refusal marker to the Display text of `ServiceError::PinnedVersionMismatch`. The stale binary is **refused by name** after the pin. It exits non-zero, and its stderr carries that error's own phrase. |
| `an_in_memory_register_passes_and_demonstrates_the_counterfactual` | the swapped-in binary's LIVE state equals what replaying the span from the pinned **artifact** computes. It also differs from the genesis counterfactual. |
| `a_durable_register_is_rewound_to_the_origin_and_passes` | the same holds for a state machine that persisted past the origin and must be rewound to it. It runs over `uc_lincheck::register::Durable<S>`. |
| `the_install_marker_is_the_sdks_own_text` | pins the install marker to the SDK's own text. The next section explains that marker. |

The in-memory and durable cases are the empty and durable shapes that §6.2 asks for.

### What makes the rewind observable

That OLD was stopped above P is only the precondition for a rewind. The rewind itself is made observable by a named marker. Pin-verify mode requires NEW to have printed the SDK's own `pinned install of snap-P` line on its stderr. This is `SwapArm::install_logged`. It is the same class of evidence as the refusal marker, and `the_install_marker_is_the_sdks_own_text` pins it.

The durable case also runs NEW as `register-replay --double-cas`. That is `DoublingCasRegisterSm`, `VERSION = 0.3.0`. It is a version change that touches a history-PRESERVING command. This matters because a durable service's wrong path is not the genesis replay. Its wrong path is continuing from X with the state it persisted. With a last-write-wins register and a `Write`-only change, the artifact path and continue-from-X compute the same value. So state alone could not have told them apart.

### The teeth

The suite has three teeth. Each is a case that must not pass.

| test | what it proves |
|---|---|
| `a_new_binary_that_is_not_the_pinned_version_is_a_fail` | a NEW binary that is not on the pinned version's line is a FAIL rather than a pass. The line is major.minor. Since #33 R17, a patch build of `--to` is on the pinned line. |
| `a_pure_write_corpus_is_inconclusive_not_a_pass` | a span whose commands cannot tell the two paths apart is INCONCLUSIVE rather than a pass |
| `a_same_version_run_is_refused_before_the_pin` | a run whose OLD already IS the pinned version is refused before any pin is placed. That run's refusal arm could not have refused anything. |

### The evidence from one run

One run recorded this evidence in its own report:

- The positions were P=12864 < X=16160 < Q=16256.
- The artifact path and the live state both projected `value=Some(249)`.
- The genesis path projected `value=Some(398)`.

The durable case ran on the same span with `--double-cas`. It parts all three paths:

- Live and artifact both projected `value=Some(400)`.
- Genesis projected `value=Some(398)`.
- The continue-from-X value is what a skipped install would have left. It was `value=Some(249)`, measured on the `--double` build this case used before. It equals neither of the others.

### What `pin_verify.rs` does NOT verify

- **It is one node.** That node is a single voter in a scratch instance dir. So nothing here is evidence about a pin propagating across a cluster. Nor is it evidence about the "confirm the pin on every node" step, or about a node that has not applied the pin yet.
- **It cannot tell whether an arbitrary app's state machine is durable or in-memory.** The sequence exercises both shapes, because OLD is stopped at X > P before the pin. But the check is the same either way. Only the fixture pins each shape by construction.
- **`TIMER` frames in a corpus are skipped and counted, never re-submitted.** A node mints those. So a timer-driven divergence is outside what a span can demonstrate here.
- **The re-submission is not row-aware.** `live::message_frames` re-submits **every** `MESSAGE` frame in the span. It does so regardless of which row the recorded cluster routed it to, because it inherits `drive::walk_block`'s dispatch. On a corpus exported from a multi-FSM cluster, the mode feeds other rows' commands into `--row`. Both paths see the same bytes, so the comparison itself is unaffected. But the span being replayed is not the row's own history.
- **`uc2ctl upgrade show` is deliberately not consulted.** Its record lands one instant behind the artifact it reads. So this suite says nothing about that surface. The metrics and `upgrade show` readings are covered by the plan B1 and B3 tests on this page.

## B3: live snapshot-hash reports

Plans B1 and B2 left `SnapshotReport` recorded, replicated and observable. But nothing ever *produced* one. Plan B3 is the producer. Each of its four seams has its own proof.

### The hash

The hash is `uc_service::snapshots::artifact_hash_of`. The test `artifact_hash_is_sha256_of_the_payload_truncated` pins it against a hand-computed SHA-256. The same test drives a real `SnapshotStore::publish` through the streaming `HashingWriter` in two `write_all` calls. It asserts that the published hash equals the digest of the concatenation. So "the hash covers the payload as streamed, envelope excluded" is asserted, not asserted-about.

The cnc word beneath the hash is `+504 artifact_hash`. Its offset is pinned in **both** `uc_protocol` and `uc_log`, the way every cnc field is. The `uc_log::cnc` test is `artifact_hash_roundtrips_and_offset_pin_and_slots_are_independent`.

### The wire

`snap_report_kind_and_body_are_pinned` in `uc_protocol::v2::datagram` pins kind `26`, `SNAP_REPORT_BODY_LEN = 24`, the byte layout and the round trip. It also covers five refusals: short, long, non-zero reserved, row ≥ 8, and position 0. `snap_report_kind_is_pairwise` in `uc_crypto` pins that the report kind is pairwise.

The kind is also in the `uc_protocol_datagram` fuzz target's arm list. That target offers every body reader every input. See [fuzzing](fuzzing.md).

### The reporting edge

Six `uc_node::node` tests drive a real `Consensus` against a real UDP socket and drain the wire. Four of them state their whole claim in their names. A follower sends one report per declared row on its local edge. A row with no published artifact hash is skipped. With no leader to address, the report is dropped and counted. The report fires on the set-complete edge, not once per pass.

- `snapshot_report_goes_out_per_declared_row_on_a_followers_local_edge`
- `snapshot_report_skips_a_row_with_no_published_artifact_hash`
- `snapshot_report_with_no_leader_to_address_is_dropped_and_counted`
- `snapshot_report_fires_on_the_set_complete_edge_not_once_per_pass`

The other two add detail:

- `snapshot_report_on_the_leader_reaches_the_collector_not_the_wire`. The leader feeds itself in process. No datagram goes out.
- `snapshot_report_is_never_sent_on_the_fetch_edge`. A set pulled with `uc2ctl snapshot fetch` is not reported. It is not this node's own observation.

### The collector and its release rule

The collector holds reports until it can append a record. Its release rule is covered by three tests:

- `snapshot_reports_append_once_every_voter_has_reported`. The record appends once every voter has reported. The test includes the learner arm, where two voters plus a learner is not every voter.
- `a_snapshot_report_from_a_non_member_is_dropped`
- `both_leader_exits_clear_the_pending_snapshot_reports`

#### Liveness under a lagging voter

The collector keeps one collection per `(row, instant)`, each on its own clock. This follows the snapshot catalog spec, erratum R37. Five tests cover it:

- `a_voter_one_instant_behind_never_starves_agreement`. It is RED under the old one-instant-per-row rule, with zero appends.
- `each_pending_instant_times_out_on_its_own_clock`
- `a_newer_instant_never_discards_the_pending_reports_for_an_older_one`
- `an_appended_instant_drops_the_older_pending_ones_as_superseded`
- `pending_instants_are_bounded_per_row_and_the_oldest_is_evicted`

#### The re-offer to a new leader

When leadership changes, nodes re-offer their held sets to the new leader. Three tests cover the re-offer:

- `a_follower_re_offers_its_newest_set_to_a_new_leader_once`
- `a_new_leader_re_offers_its_own_newest_set_to_itself`
- `a_follower_re_offers_every_held_commanded_set_to_a_new_leader` pins ruling R38-2. A follower re-offers every held set that the catalog lists as not complete.

#### The timeout clock, ruling R38-1

A pending instant's timeout runs from the instant's log-time stamp. So a leader change does not restart it. Three tests cover the clock, the last of them the fallback:

- `a_fresh_leader_times_an_old_instant_out_on_the_logs_clock`
- `an_instant_times_out_when_the_log_clock_reaches_it_plus_the_timeout`
- `an_unlisted_instant_falls_back_to_its_first_sighting`

#### The re-offer's filter and cache, rulings R40 and R39

The re-offer filters the sets it offers and keeps a bounded report cache. Three tests state their claim in their names:

- `the_re_offer_skips_complete_reported_and_unheld_sets`
- `a_new_leader_re_offers_every_cached_held_set_to_itself`
- `the_report_cache_is_bounded_and_keeps_the_fuller_entry`

Three more pin a ruling each:

- `the_probe_seeds_the_report_cache_from_a_set_held_on_disk` pins ruling R40's seed from disk. It pins the row payload's hash to the slot's `artifact_hash`, and row 255's hash to the cluster agent's published word.
- `a_seed_arriving_after_the_leadership_edge_is_offered_to_that_leader` pins ruling R40. A seed that arrives after the leadership edge is offered to that leader.
- `a_report_above_the_leaders_log_extent_is_dropped` pins ruling R39's door. A report above the leader's log extent is dropped.

#### A node reports only sets it built, ruling R42

Under ruling R42, a set this node did not build is marked foreign and is never reported. Six tests state their claim in their names:

- `a_fetched_set_is_never_seeded_or_re_offered_but_a_built_one_is`
- `a_session_installed_set_is_recorded_as_foreign`
- `a_foreign_marker_excludes_a_set_after_a_restart`
- `pruning_a_set_removes_its_foreign_marker`
- `marking_a_set_foreign_evicts_its_cached_report`
- `after_a_restart_the_re_offer_skips_a_marked_set`

Two tests are in the receiver:

- `a_session_does_not_complete_until_its_cluster_artifact_lands_and_routes_it_first`. A session does not complete until its cluster artifact lands, and the session routes that artifact first.
- `an_aborted_session_leaves_its_row_copies_already_marked`. The marker lands before any of a session's artifacts.

One test is in backup. `the_snapshot_copy_carries_a_sets_foreign_marker` pins that the snapshot copy carries a set's foreign marker.

#### The seeder's per-probe budget and retry

The seeder hashes held sets within a budget on each probe. Two tests cover the budget and the retry:

- `a_backlog_of_held_sets_drains_over_successive_probes`
- `a_partially_hashed_set_is_retried_on_the_next_probe`

### The readiness gate

The readiness gate is B2's deferred boot window. It is pinned clause by clause, because each clause alone is vacuous. There is one test per clause:

- `the_declared_set_waits_for_a_known_leader`
- `the_declared_set_waits_for_a_commit_the_cluster_actually_took`. A restarted single voter is its own leader at commit 0, before it has ranked or gossiped anything. That is exactly the node whose pin sits above its recovered artifact.
- `the_declared_set_waits_for_the_cluster_fsm_to_reach_commit`

The gate's service-side consequence is two tests in `uc_service/tests/pinned_attach.rs`:

- `a_restarted_node_publishes_the_pin_before_the_declared_set`
- `a_pin_above_the_recovered_artifact_is_published_before_the_declared_set`. This is the case B2 could not close.

The bounded `NodeBooting` wait has four tests of its own in `uc_client::engine`, and two more in `uc_service::attach`. The four in `uc_client::engine` are:

- `names_present_with_declared_zero_is_refused_as_booting`
- `a_node_that_never_joins_is_refused_after_the_bounded_wait`
- `a_page_that_stops_opening_during_the_wait_reports_the_real_error`. The wait must not swallow a genuine re-open failure.
- `attach_waits_out_a_joining_node_and_then_succeeds`

### The end-to-end suite

The end-to-end suite is `uc_node/tests/snapshot_reports.rs`. It runs three real nodes over loopback UDP, three real snapshot-capable services, and one commanded coordinated instant. There is no sim and there are no fakes.

| test | what it proves |
|---|---|
| `three_voters_agree` | every node commits the same record. `verdict().agreed` holds. `uc2_snapshot_hash_mismatch{row="0"}` renders `0` on every node's own `/metrics`. |
| `one_divergent_node_is_named` | node 2 runs a state machine whose `freeze()` appends its node id to the image. It has the same FSM identity but different bytes. Three different readers of the SAME committed record must agree. The verdict names node 2 and only node 2 as the minority. The gauge reads `1` on **every** node, not on the leader's alone. The `uc2-cluster` agent's `snapshot_hash_diverged` record names the row and the node. |
| `a_learner_reports_but_does_not_count_toward_quorum` | two voters and a learner all genuinely report. The record carries both voters. A wall-clock bound well under `SNAP_REPORT_TIMEOUT_NS` pins that the voters' reports released it, not the 5 s fallback. That assertion is what tells the two mechanisms apart at all. |

`one_divergent_node_is_named` has a cheap red twin. A red twin is a deliberately broken variant that must FAIL, proving the test can detect the bug. Here the twin gives node 2 a plain `SumSm`, and it fails all three readers.

### What B3 does not verify

**The release rule's *choice* rests on an observation, not a test and not a fleet measurement.** The choice is every voter rather than a quorum. The observation comes from the three-node fixture above. That fixture runs three busy-spin nodes in ONE process on a dev box. In it, the same replica completes its set last on every instant, for the life of the process. This is recorded in the spec's plan-B3 errata.

No test pins that ordering, and none should. Which replica straggles is a property of a machine, not of the code. So per CLAUDE.md's benchmarking discipline, the underlying timing is smoke. No lag figure is claimed here. Only the persistence of the ordering is claimed, and that is all the argument needs.

**The cluster FSM's own artifact is not covered.** That artifact is `service_id = 255`. It is not reported at all.

## #33: the row running version

The row running version arrived with wire `0.10.0` and #33. Its end-to-end suite is `uc_node/tests/row_version.rs`. The suite runs three real nodes in one process. It uses two builds of one KV state machine, `1.0.0` and `2.0.0`, which differ only in an `append` command.

### The #33 repro

`a_mixed_version_row_never_loses_an_acknowledged_write` is the #33 repro. The leader runs 2.0 and the followers run 1.0. The test makes a 2.0-only write, then forces a leader change. It was written first. It **failed on the lost value itself** before any fix existed: 10 was read back where 15 was acknowledged. It now passes because the 1.0 followers are refused by name.

### The rest of the suite

Three tests state their claim in their names:

- `the_leader_records_its_attached_version_as_genesis`
- `attach_refuses_a_binary_off_the_running_line_by_name`
- `a_matching_restart_after_genesis_does_not_stop_again`

Two more add detail:

| test | what it proves |
|---|---|
| `clients_wait_until_every_declared_row_has_a_version` | the log must not grow while a declared row has no version. A submit merely timing out is not enough. |
| `a_committed_pin_stops_every_old_service_at_exactly_the_record` | `applied` equals the pin frame's start on every node. It runs a recorded register workload across the switch and checks it with `uc_lincheck`'s WGL checker, which returns `Linearizable`. WGL is the Wing-Gong-Lowe linearizability search. It also requires at least one write committed after the pin to be acknowledged, which only a 2.0 service can do. |

### Unit tests where the decisions live

The pure decisions have unit tests where they live:

- the pin and genesis rules, in `uc_node/src/cluster_fsm.rs`
- the version-gate verdict and its bounded wait, in `uc_service/src/version_gate.rs`
- `same_line`, in `uc_protocol::identity`
- the row-view seqlock and the `cluster_applied` word, in `uc_log::cnc`
- the image v1/v2 → v3 migration, against captured fixtures

The `uc_protocol_cluster_frame` fuzz target decodes kind 6.

### What #33 does not verify

The consensus kernel, `uc_sim` and the Lean model are untouched. The running version is cluster-FSM state applied at commit, like pins. "Patch changes nothing replicated" is the application's promise. Only `uc2-diffreplay upgrade` samples it.
