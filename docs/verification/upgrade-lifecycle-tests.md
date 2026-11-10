# The FSM upgrade lifecycle: what the tests prove

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

## The short version

Upgrading a state machine in UC is not "stop the old binary, start the new one". It is a pinned procedure. The operator picks a snapshot instant, a position P in the log where every node holds the same snapshot, and commits a record that says "from P on, this row runs version X". From then on, every instance of that row installs the snapshot at P before applying anything, and the old binary is refused by name. The [application upgrade how-to](../how-to/upgrade-an-application.md) describes the procedure. This page is about the evidence that it works.

The [simulation](simulation.md) cannot reach any of this, because it has no cluster-FSM frames. So the evidence is unit tests and integration tests, some in-crate and some driving real nodes, real services, real UDP sockets and real child processes. They establish five things, each with its own section below:

1. **The pin is recorded, replicated and visible** on every node.
2. **A service honours the pin when it attaches**, installing the origin snapshot even when it would rather carry on from where it was.
3. **An outside observer can verify an upgrade** using nothing but the application's own binaries.
4. **Nodes agree on what the origin snapshot contains**, so a pin points at bytes every node shares.
5. **A row runs one version line at a time**, which is the rule the whole feature exists to enforce.

Three of those sections end with what their tests do *not* verify. Those limits are as much a part of the record as the tests.

## Vocabulary

- A **row** is one slot for a state machine in the cluster. Each service process attaches to one row. The cluster can have up to eight.
- The **cluster FSM** is the cluster's own replicated state machine. It holds membership, settings, and since `2.13.0` the upgrade pins and snapshot reports. Its data is applied at commit on every node, like any state machine's.
- A **snapshot instant** is a position P in the log where every row freezes and writes a snapshot. A node that holds every row's snapshot at P holds a complete **set**. Each row's snapshot file is an **artifact**.
- A **pin** is a cluster-FSM record saying that a row must run a given version from a given instant on. That instant is the pin's **origin**. The target version is `--to` on the `uc2ctl upgrade pin` command.
- The **purge floor** is the position below which a node may discard its log, because a complete set covers it.
- The **counterfactual** is what the new binary would compute if there were no pin: replaying the whole log from genesis instead of installing the artifact at the origin. The two can differ, and the difference is exactly what a pin is for. The upgrade-lifecycle spec defines it in §2.3.
- The **cnc page** is the shared-memory control page every process on a host reads. Per-row facts such as the pinned version live in words on that page.

## 1. The pin is recorded and replicated

The schedule table and the settings record are cluster-FSM data. Since `2.13.0`, so are the upgrade pin and the snapshot report. Because the simulator has no cluster-FSM frames, this data's correctness is unit-tested in the crates that own it.

### The replicated half

Five tests in `cluster_fsm.rs` cover the record itself. They pin that pin validation reads replicated state only, so every node reaches the same verdict. They pin that the pin history is bounded per row, and that the newest report is held per row while a stale one is refused. They pin that pins and reports survive a round trip through the cluster image, that an image written before this feature installs with no pins rather than failing, and that the published cluster view carries both.

- `pin_refusals_are_replicated_state_only`
- `pin_history_is_bounded_per_row`
- `a_report_is_held_newest_per_row_and_a_stale_one_is_refused`
- `pins_and_reports_ride_the_image_and_an_old_image_installs_empty`
- `the_view_publishes_pins_and_reports`

### The door, and the retention exemption

Four tests in `node.rs` cover the admin door that accepts a pin request, and the one place a pin changes retention. They pin that a bad request is refused by name. They pin that an accepted pin is appended as a cluster frame and that the per-row cnc words follow only at commit. They pin that only one pin is in flight at a time, keyed on the cluster view's position. And they pin that snapshot retention keeps every pinned origin, since a pinned origin is a place a service will need to install from.

- `upgrade_pin_door_refusals_by_name`
- `an_upgrade_pin_is_appended_as_a_cluster_frame_and_the_words_follow_at_commit`
- `upgrade_pin_is_single_in_flight_on_the_view_position`
- `retention_keeps_every_pinned_origin`

### The floor hold

A pinned origin also holds the purge floor. If the log below the origin were purged before the new version had installed from it and caught up, the row could be left with no way forward. A fix round found a gap in that hold, and two more `node.rs` tests came from it.

`the_floor_is_held_at_a_pinned_origin_until_the_pinned_version_attaches` pins when the hold clears: only once the row is attached at the `--to` version **and** has replayed past the candidate floor. Being attached is not enough on its own.

`a_detached_row_under_a_pin_still_holds_the_floor` pins that a row with no service running at all still holds. "Not consumed" cannot distinguish an upgrade that is in flight from one that never started, so the hold must assume the former.

## 2. A service honours the pin when it attaches

Everything above proves the pin is recorded, replicated and observable. It says nothing about what a service process actually *does* with a pin when it attaches. `uc_service/tests/pinned_attach.rs` covers that with 11 tests. Each runs a real `Node` and a real `Service<S>` over an on-disk instance directory, with no sim.

### The install, and its control

`the_pinned_version_installs_the_origin_unconditionally_and_recomputes_the_tail` is the core claim: a service attaching under a pin installs the artifact at the origin, whatever state it had, and replays the log from there.

`a_durable_sm_above_the_origin_is_rewound_to_it` is the case that makes "unconditionally" matter. A durable state machine that had already applied past the origin is rewound to the origin rather than carrying on.

`the_same_swap_without_a_pin_computes_the_counterfactual` is the control. The same binary swap with no pin computes the §2.3 counterfactual instead. The pair shows the pin, not the swap, is what changes the outcome.

### The refusals

A service that cannot honour the pin must refuse to attach, by name. Four `ServiceError` refusals plus the artifact envelope's own version check are each pinned by a test:

| test | refusal |
|---|---|
| `a_stale_binary_is_refused_by_name_after_the_pin` | `PinnedVersionMismatch`, the old binary after the pin |
| `a_contended_pin_read_is_refused_not_ignored` | `PinUnreadable`, a contended read of the pin words is an error, not a missing pin |
| `a_pinned_row_started_without_snapshots_is_refused` | `PinRequiresSnapshots` |
| `a_pinned_origin_with_no_artifact_is_refused` | `PinnedArtifactMissing` |
| `a_pinned_artifact_built_by_the_wrong_version_is_refused` | `EnvelopeError::VersionMismatch`, surfaced as `MistaggedSnapshot` |

### Snapshots that land around the pin

`a_pinned_attach_prefers_the_origin_over_a_later_artifact` pins the gap guard: if a newer artifact exists above the origin, the service still installs the origin.

`a_pinned_attach_survives_a_cadence_instant_after_the_pin` pins that a snapshot instant from the automatic cadence, landing after the pin but before the old service stops, does not disturb the install.

`a_pinned_attach_converges_on_a_purging_cluster` pins that the install works on a cluster that is actively purging its log. It stands in for a two-row cluster where one row is busy enough to purge while the pinned row stays quiet. The test itself is a single-node, single-row fixture. It drives `PURGE_EXTRA_INSTANTS = 400` coordinated instants past the pin's origin on an otherwise idle row, and only then attaches the pinned binary. That is what actually exercises the claim "the origin's artifact must still be retrievable after the floor has moved past it many times". A real two-row harness would add cross-row scheduling noise without changing what the assertion proves.

### The end-to-end companion

`a_real_pin_makes_the_default_purge_off_swap_install_the_origin` in `uc_diffreplay/tests/reconstruction.rs` runs the same claim end to end. It sends a real admin `upgrade pin` request over the wire, through a running node, answered by the same code path an operator's `uc2ctl` drives. It confirms that the pinned install and the §2.3 counterfactual disagree, `Some(4)` against `Some(8)`, on the default purge-off configuration and not only the purging one above.

## 3. Checking an upgrade from outside

Every test so far links the service SDK into the test process. `uc2-diffreplay pin-verify` is different, and is spec §6.2 part 2. It runs the application's real binaries as child processes against a real node, and judges the upgrade from what UC's own surfaces say. That makes it usable on an application UC's authors never saw.

`uc_diffreplay/tests/pin_verify.rs` holds five end-to-end cases of that mode on the `register-replay` fixture, a small register state machine packaged as an application binary. The actors are named throughout: **OLD** is the binary from before the swap, **NEW** is the binary swapped in, **P** is the origin, **X** is the position OLD was stopped at, above P, and **Q** is the end of the replayed span. Each case runs a node and one service process per era. The cases that get that far also place a real admin `upgrade pin`, then run the application binary three more times, once as `project` and twice as `replay`. The same-version case is refused before any pin is placed. The stale-NEW case stops at the swap arm before NEW attaches.

### What a passing run proves

- The stale binary is **refused by name** after the pin. It exits non-zero, and its stderr carries the `ServiceError::PinnedVersionMismatch` error's own text. `the_refusal_marker_is_the_sdks_own_text` pins that marker to the error's Display output.
- NEW's live state equals what replaying the span from the pinned **artifact** computes, and differs from the genesis counterfactual. `an_in_memory_register_passes_and_demonstrates_the_counterfactual` covers the in-memory shape.
- The same holds for a state machine that persisted past the origin and must be rewound. `a_durable_register_is_rewound_to_the_origin_and_passes` covers it, over `uc_lincheck::register::Durable<S>`.

Those two shapes, empty and durable, are the ones §6.2 asks for.

### What makes a rewind observable

That OLD was stopped above P is only the precondition for a rewind. The rewind itself is made observable by a marker: pin-verify requires NEW to have printed the SDK's own `pinned install of snap-P` line on its stderr. This is `SwapArm::install_logged`, the same class of evidence as the refusal marker, and `the_install_marker_is_the_sdks_own_text` pins it to the SDK's text.

The durable case needs one more thing. It runs NEW as `register-replay --double-cas`, which is `DoublingCasRegisterSm` at `VERSION = 0.3.0`, a version change that alters a history-preserving command. The reason is that a durable service's wrong path is not the genesis replay. Its wrong path is to carry on from X with the state it had persisted. With a last-write-wins register and a change that only touches `Write`, the artifact path and the carry-on-from-X path compute the same value, so state alone could not tell them apart.

### The teeth

Three cases must *not* pass, which is how the mode is shown to have teeth:

- `a_new_binary_that_is_not_the_pinned_version_is_a_fail`: a NEW binary that is not on the pinned version's line is a FAIL, not a pass. The line is major.minor, and since #33 ruling R17 a patch build of `--to` counts as on the line.
- `a_pure_write_corpus_is_inconclusive_not_a_pass`: a span whose commands cannot tell the two paths apart is INCONCLUSIVE, not a pass.
- `a_same_version_run_is_refused_before_the_pin`: a run whose OLD already is the pinned version is refused before any pin is placed, because its refusal arm could not have refused anything.

### The evidence from one run

One run recorded this in its own report. The positions were P=12864 < X=16160 < Q=16256. The artifact path and the live state both projected `value=Some(249)`, while the genesis path projected `value=Some(398)`.

The durable case ran on the same span with `--double-cas` and parted all three paths. Live and artifact both projected `value=Some(400)`. Genesis projected `value=Some(398)`. And the carry-on-from-X value, what a skipped install would have left, was `value=Some(249)`, measured on the `--double` build this case used before. It equals neither of the others.

### What pin-verify does not verify

- **It is one node**, a single voter in a scratch instance directory. Nothing here is evidence about a pin propagating across a cluster, about the "confirm the pin on every node" step, or about a node that has not applied the pin yet.
- **It cannot tell whether an arbitrary application's state machine is durable or in-memory.** The sequence exercises both shapes, since OLD is stopped at X above P before the pin, but the check is the same either way. Only the fixture pins each shape, by construction.
- **`TIMER` frames in a corpus are skipped and counted, never re-submitted.** A node mints those, so a timer-driven divergence is outside what a span can demonstrate.
- **Re-submission is not row-aware.** `live::message_frames` re-submits every `MESSAGE` frame in the span regardless of which row the recorded cluster routed it to, because it inherits `drive::walk_block`'s dispatch. On a corpus exported from a multi-FSM cluster, the mode feeds other rows' commands into `--row`. Both paths see the same bytes, so the comparison itself is unaffected, but the span being replayed is not the row's own history.
- **`uc2ctl upgrade show` is deliberately not consulted.** Its record lands one instant behind the artifact it reads, so this suite says nothing about that surface. The metrics and `upgrade show` readings are covered by the tests in sections 1 and 4.

## 4. Nodes agree on what the origin contains

A pin points at a snapshot instant, so it matters that every node's snapshot at that instant holds the same bytes. Since `2.13.0`, every node reports the hash of each artifact it builds, the leader collects the reports, and a `SnapshotReport` record carrying every node's observation is committed. A pure function, `verdict`, reads that record and says whether the nodes agreed and which node, if any, is in the minority. The snapshot catalog lists each instant with its verdict. The pieces from plan B1 recorded and replicated the report. Plan B3 is what *produces* it, and each of its seams has its own proof.

### The hash

The hash is `uc_service::snapshots::artifact_hash_of`. `artifact_hash_is_sha256_of_the_payload_truncated` pins it against a hand-computed SHA-256. The same test drives a real `SnapshotStore::publish` through the streaming `HashingWriter` in two `write_all` calls and asserts the published hash equals the digest of the concatenation. So "the hash covers the payload as streamed, with the envelope excluded" is asserted directly, not inferred.

The hash is published in a cnc word, `+504 artifact_hash` on the row's slot line. Its offset is pinned in both `uc_protocol` and `uc_log`, as every cnc field is, and `uc_log::cnc`'s `artifact_hash_roundtrips_and_offset_pin_and_slots_are_independent` covers the round trip.

### The wire

A report travels node to node as datagram kind `26`. `snap_report_kind_and_body_are_pinned` in `uc_protocol::v2::datagram` pins the kind, `SNAP_REPORT_BODY_LEN = 24`, the byte layout, the round trip, and five refusals: a short body, a long body, non-zero reserved bytes, a row of 8 or more, and position 0. `snap_report_kind_is_pairwise` in `uc_crypto` pins that the kind is sealed with the pairwise key, not the group key. The kind is also in the `uc_protocol_datagram` fuzz target's arm list, which offers every body reader every input. See [fuzzing](fuzzing.md).

### When a node reports

Six `uc_node::node` tests drive a real `Consensus` against a real UDP socket and read back what goes out. Four of them say their whole claim in their names: a follower sends one report per declared row on its local edge, a row with no published artifact hash is skipped, a report with no leader to address is dropped and counted, and the report fires once when the set completes rather than once per pass.

- `snapshot_report_goes_out_per_declared_row_on_a_followers_local_edge`
- `snapshot_report_skips_a_row_with_no_published_artifact_hash`
- `snapshot_report_with_no_leader_to_address_is_dropped_and_counted`
- `snapshot_report_fires_on_the_set_complete_edge_not_once_per_pass`

The other two add a detail each. `snapshot_report_on_the_leader_reaches_the_collector_not_the_wire`: the leader feeds its own report to its collector in process, and no datagram goes out. `snapshot_report_is_never_sent_on_the_fetch_edge`: a set pulled from a learner with `uc2ctl snapshot fetch` is not reported, because it is not this node's own observation.

### The leader's collector

The leader holds incoming reports for an instant until it can append the record. Three tests cover the release rule. `snapshot_reports_append_once_every_voter_has_reported` pins that the record appends once every voter has reported, including the learner arm: two voters plus a learner is not every voter. `a_snapshot_report_from_a_non_member_is_dropped` and `both_leader_exits_clear_the_pending_snapshot_reports` say their claims in their names.

**A lagging voter must not starve agreement.** The collector keeps one collection per `(row, instant)`, each on its own clock, per the snapshot catalog spec's erratum R37. `a_voter_one_instant_behind_never_starves_agreement` pins that, and was RED under the old one-collection-per-row rule, with zero records appended. Four more cover the mechanics: `each_pending_instant_times_out_on_its_own_clock`, `a_newer_instant_never_discards_the_pending_reports_for_an_older_one`, `an_appended_instant_drops_the_older_pending_ones_as_superseded`, and `pending_instants_are_bounded_per_row_and_the_oldest_is_evicted`.

**The timeout runs on the log's clock.** A pending instant's timeout counts from the instant's log-time stamp, so a leader change does not restart it. That is ruling R38-1, pinned by `a_fresh_leader_times_an_old_instant_out_on_the_logs_clock` and `an_instant_times_out_when_the_log_clock_reaches_it_plus_the_timeout`. `an_unlisted_instant_falls_back_to_its_first_sighting` covers the fallback for an instant the catalog does not list.

### Re-offers after a leader change

A report sent to a leader that then died would be lost, so nodes re-offer their held sets to a new leader. `a_follower_re_offers_its_newest_set_to_a_new_leader_once` and `a_new_leader_re_offers_its_own_newest_set_to_itself` cover the basic case. `a_follower_re_offers_every_held_commanded_set_to_a_new_leader` pins ruling R38-2: every held set the catalog lists as not yet complete is re-offered, not only the newest.

The re-offer filters what it sends and keeps a bounded cache of reports. `the_re_offer_skips_complete_reported_and_unheld_sets`, `a_new_leader_re_offers_every_cached_held_set_to_itself` and `the_report_cache_is_bounded_and_keeps_the_fuller_entry` say their claims in their names. Three more each pin a ruling:

- `the_probe_seeds_the_report_cache_from_a_set_held_on_disk` pins ruling R40's seeding from disk on the agent's periodic probe. It pins a row payload's hash to its slot's `artifact_hash` word, and row 255's, the cluster FSM's own artifact, to the cluster agent's published word.
- `a_seed_arriving_after_the_leadership_edge_is_offered_to_that_leader` pins ruling R40's other half.
- `a_report_above_the_leaders_log_extent_is_dropped` pins ruling R39's door: a report for a position the leader's log has not reached is dropped.

The seeder works within a per-probe budget. `a_backlog_of_held_sets_drains_over_successive_probes` and `a_partially_hashed_set_is_retried_on_the_next_probe` cover the budget and the retry.

### A node reports only sets it built

A set that arrived from a peer, by snapshot session or by fetch, is someone else's observation. Ruling R42 says such a set is marked foreign and never reported. Six tests say their claims in their names:

- `a_fetched_set_is_never_seeded_or_re_offered_but_a_built_one_is`
- `a_session_installed_set_is_recorded_as_foreign`
- `a_foreign_marker_excludes_a_set_after_a_restart`
- `pruning_a_set_removes_its_foreign_marker`
- `marking_a_set_foreign_evicts_its_cached_report`
- `after_a_restart_the_re_offer_skips_a_marked_set`

Two sit in the receiver, where a snapshot session lands: `a_session_does_not_complete_until_its_cluster_artifact_lands_and_routes_it_first`, and `an_aborted_session_leaves_its_row_copies_already_marked`, which pins that the foreign marker lands before any of a session's artifacts do. One sits in backup: `the_snapshot_copy_carries_a_sets_foreign_marker`.

### The boot gate

A node that has just started must not let a service attach before the pins are known, or a stale binary could attach into the window. So the node publishes its declared row set, the cnc word that lets services attach, only once three things are true: it knows the leader, it has learned a commit the cluster actually took, and its cluster FSM has caught up to that commit. Each clause is pinned on its own, because any one alone is vacuous:

- `the_declared_set_waits_for_a_known_leader`
- `the_declared_set_waits_for_a_commit_the_cluster_actually_took`. A restarted single voter is its own leader at commit 0, before it has ranked or gossiped anything, and that is exactly the node whose pin sits above its recovered artifact.
- `the_declared_set_waits_for_the_cluster_fsm_to_reach_commit`

The service-side consequence is two tests in `uc_service/tests/pinned_attach.rs`: `a_restarted_node_publishes_the_pin_before_the_declared_set`, and `a_pin_above_the_recovered_artifact_is_published_before_the_declared_set`, which is the case section 2's tests could not close on their own.

While the gate holds, an attaching client or service sees `NodeBooting` and waits a bounded time. That wait has four tests in `uc_client::engine` and two more in `uc_service::attach`. The four are `names_present_with_declared_zero_is_refused_as_booting`, `a_node_that_never_joins_is_refused_after_the_bounded_wait`, `a_page_that_stops_opening_during_the_wait_reports_the_real_error`, which pins that the wait must not swallow a genuine re-open failure, and `attach_waits_out_a_joining_node_and_then_succeeds`.

### The end-to-end suite

`uc_node/tests/snapshot_reports.rs` runs three real nodes over loopback UDP, three real snapshot-capable services, and one commanded coordinated instant. There is no sim and nothing is faked.

- `three_voters_agree`: every node commits the same record, `verdict().agreed` holds, and `uc2_snapshot_hash_mismatch{row="0"}` reads `0` on every node's own `/metrics`.
- `one_divergent_node_is_named`: node 2 runs a state machine whose `freeze()` appends its node id to the image, so it has the same FSM identity but different bytes. Three different readers of the same committed record must agree. The verdict names node 2, and only node 2, as the minority. The gauge reads `1` on **every** node, not only the leader's. The `uc2-cluster` agent's `snapshot_hash_diverged` log record names the row and the node. This test has a cheap red twin, a deliberately broken variant that must fail: give node 2 a plain `SumSm` and all three readers fail.
- `a_learner_reports_but_does_not_count_toward_quorum`: two voters and a learner all genuinely report. The record carries both voters. A wall-clock bound well under `SNAP_REPORT_TIMEOUT_NS` pins that the voters' reports released the record, not the 5 s fallback, which is the only assertion that tells the two mechanisms apart.

### What the reports do not verify

**The release rule's choice rests on an observation, not a test and not a fleet measurement.** The rule waits for every voter rather than a quorum. The reason is an observation from the three-node fixture above, which runs three busy-spin nodes in one process on a dev box: the same replica completes its set last on every instant, for the life of the process. The spec's plan-B3 errata record it. No test pins that ordering, and none should. Which replica straggles is a property of a machine, not of the code. Per CLAUDE.md's benchmarking discipline the timing underneath is smoke, no lag figure is claimed, and only the persistence of the ordering is claimed, which is all the argument needs.

**The cluster FSM's own artifact is not reported.** That is row 255, `service_id = 255`, and nothing here covers it.

## 5. A row runs one version line at a time

Before `2.14.0` nothing stopped two versions of a state machine from running the same row on different nodes. Issue #33 showed what that costs: an acknowledged write could be lost. The fix, shipped with wire `0.10.0`, records a running version per row in the cluster FSM, set at genesis or by a pin, and refuses by name any binary off that version's line.

The end-to-end suite is `uc_node/tests/row_version.rs`. It runs three real nodes in one process with two builds of one KV state machine, `1.0.0` and `2.0.0`, which differ only in an `append` command.

### The reproduction

`a_mixed_version_row_never_loses_an_acknowledged_write` is the #33 reproduction. The leader runs 2.0 and the followers run 1.0. The test makes a 2.0-only write, then forces a leader change. It was written first, before any fix existed, and it **failed on the lost value itself**: 10 was read back where 15 had been acknowledged. It now passes because the 1.0 followers are refused by name.

### The rest of the suite

Three tests say their claim in their names: `the_leader_records_its_attached_version_as_genesis`, `attach_refuses_a_binary_off_the_running_line_by_name`, and `a_matching_restart_after_genesis_does_not_stop_again`.

`clients_wait_until_every_declared_row_has_a_version` pins that the log must not grow while a declared row has no version yet. A submit that merely times out is not good enough.

`a_committed_pin_stops_every_old_service_at_exactly_the_record` pins that `applied` equals the pin frame's start on every node. It runs a recorded register workload across the switch and checks the history with `uc_lincheck`'s WGL checker, the Wing-Gong-Lowe linearizability search, which returns `Linearizable`. It also requires at least one write committed after the pin to be acknowledged, which only a 2.0 service can do.

### Unit tests where the decisions live

The pure decisions are unit-tested where they are made: the pin and genesis rules in `uc_node/src/cluster_fsm.rs`, the version-gate verdict and its bounded wait in `uc_service/src/version_gate.rs`, `same_line` in `uc_protocol::identity`, the row-view seqlock and the `cluster_applied` word in `uc_log::cnc`, and the cluster image's v1/v2 → v3 migration against captured fixtures. The `uc_protocol_cluster_frame` fuzz target decodes the new frame, kind 6.

### What the running version does not verify

The consensus kernel, `uc_sim` and the Lean model are untouched. The running version is cluster-FSM state applied at commit, like a pin. And "a patch release changes nothing replicated" is the application's promise, which only `uc2-diffreplay upgrade` samples.
