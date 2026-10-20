# Versioning and the semver promise

`ultima_cluster` ships as **one version number**. The git tag, the
`[workspace.package] version` in the root `Cargo.toml`, every published
crate, the release tarballs and the container image all carry the same
string — `2.6.0` at the time of writing. There is no per-crate version
line to reconcile, and a crate at `2.6.0` is only ever meant to be used
with the other eleven at `2.6.0`.

That lockstep is a deliberate trade. It means a breaking change anywhere in
the promised surface below moves *everything* to `3.0.0`, including crates
that did not change. In exchange, "which versions go together" is never a
question anyone has to answer, and `cargo package` catches a straggler pin
before a release does (CI's `publish-check` job runs it on every push).

## The promised surface

These are the items covered by semver. A breaking change to any of them is a
major bump; additions are minor; everything else is a patch.

| Item | Where it lives |
|---|---|
| `RawStateMachine` | `uc_service::traits::RawStateMachine` |
| `StateMachine` | `uc_service::traits::StateMachine` |
| `SnapshotStateMachine` | `uc_service::traits::SnapshotStateMachine` |
| `OutputHandler` (and `RawOutputHandler`) | `uc_service::traits` |
| `Sessioned<S>` and `SessionConfig` | `uc_service::session` |
| `NodeConfig`, and the `node.toml` it mirrors | `uc_node::node::NodeConfig`, `uc_node::config_file` |
| Starting a node: `Node::start`, `Node::start_with` | `uc_node::node::Node` |
| `gateway.toml` | `uc_gateway::config` (`EdgeConfig`, `Member`) |
| The three client tiers | `uc_client::{Engine, PipelinedClient, Client}` |
| The `uc_remote` protocol, **v1** | `uc_remote::frame::PROTOCOL_VERSION` |
| `RemoteClient`, the Rust implementation of it | `uc_remote::client::RemoteClient` |
| `uc2ctl` verbs and exit codes | the `uc2ctl` binary |

Promising an item promises **the types it unavoidably exposes to a caller**
along with it — a signature you cannot name is not usable API. Concretely,
and covered by this policy on the same terms as the rows above:

- `uc_service::{ServiceBuilder, Service, ServiceConfig, ServiceError,
  SnapshotError, OutputError}` — `ServiceBuilder::new(cfg, sm).start()` is
  how a state machine is attached at all, and it returns
  `Result<Service<S>, ServiceError>`. (the now-retired `SnapshotPolicy` was here until
  2.11.0, which deleted it: the snapshot cadence became a replicated cluster
  setting, `[settings] snapshot.interval_bytes`, so a service no longer
  chooses one. Removing a promised type is a MAJOR change under this policy —
  it ships in 2.11.0 under the same maintainer-decided carve-out as the FSM
  identity break below, and for the same reason: nothing outside this
  repository depends on it yet.)
- `uc_client::{PipelinedConfig, Ticket, ClientError, WaitStrategy}` — the
  configuration, the handle and the error type the `PipelinedClient`/`Client`
  tiers hand back.
- `uc_node::{StartOpts, SubmitError}` — `Node::start_with` takes the first
  and the node's submit path returns the second.
- `uc_remote::{RemoteConfig, RemoteResponse, RemoteError}` —
  `RemoteClient::connect` takes the first and every request resolves into one
  of the other two.

`Engine`'s own send/poll halves and their outcome types
(`uc_client::{EngineConfig, SendHalf, PollHalf, Completion, Outcome,
Consistency, SubmitError}`) are in the same position and are covered too.

- **M14b adds per-FSM routing and fan-in to the `uc_client` surface**:
  `FanInTicket`; `Outcome::{Responses, BadService}`;
  `SubmitError::ServiceNotDeclared`; `ClientError::ServiceNotDeclared`;
  `SendHalf::{declared, try_submit_to, try_submit_all, try_query_on}`;
  `PipelinedClient::{declared, submit_to, submit_all, query_snapshot_on,
  query_linearizable_on}`; `Client::{declared, submit_to, submit_all,
  query_snapshot_on, query_linearizable_on}` — all additive (minor).
  `uc_client` now depends on `bytes` (`Outcome::Responses` and
  `FanInTicket` expose `bytes::Bytes`, the same type the SM contract uses).
  One behavioural change is worth flagging even though it is not a
  breaking change under this policy: `Outcome` and `SubmitError` gained
  variants, so an exhaustive match on either downstream breaks at compile
  time — a documented minor-version hazard of the three-tier promise.
- **M14c adds the per-FSM observability surface**, all additive:
  `uc_service_attached`, `uc_service_lag_bytes`,
  `uc_service_lag_waits_total`, `uc_services_declared` and
  `uc2_fsm_lag_bytes` as new metric families; a `service="<id>"` sample per
  declared id on `uc_service_applied_bytes`, `uc_service_epoch`,
  `uc_service_snapshot_pos_bytes` and `uc_service_heartbeat_age_seconds`;
  the `Uc2ServiceAbsent` and `Uc2ServicePinnedAtLagBound` rules; the
  `service_attached`/`service_detached` `[log]` records; and a `services:`
  section in `uc2ctl status`'s output. Nothing was renamed or removed. Two
  consequences worth flagging even though neither is breaking under this
  policy: a query that assumed one sample per `uc_service_*` family now
  sees several, so `sum(...)` double counts unless it says `{service=""}`
  (the shipped rules and dashboard were updated); and a scraper of
  `uc2ctl status` stdout sees new lines between `log:` and `members:`.
  The metric series contract is not itself in the promised-surface table —
  `uc_node::obs` is listed as not promised — but it is treated as an
  operator interface in practice: families are added, not renamed.
- **Log time and timers (2.11.0) add to `uc_service` additively**, on
  top of FSM identity's already-breaking trait change: `ApplyCtx` gains
  `time_ns` and `term` (it is `#[non_exhaustive]` for exactly this) plus
  `schedule`/`cancel`/`timers`; `TimerReq` and `TimerEvent` are new types; a
  **provided** `on_timer(&mut self, ctx, ev)` and a provided
  `pending_timers()` land on both `RawStateMachine` and `StateMachine`, so
  every existing implementor compiles unchanged; `uc_service::Timed<S>` is a
  new wrapper alongside `Sessioned<S>`. Nothing was renamed or removed. New
  metric families (`uc2_timers_{pending,fired_total,late_total,rearmed_total}`,
  `uc2_log_time_ns`, `uc2_log_time_lag_seconds`), one new alert rule
  (`Uc2LogTimeFrozen`), one new `[log]` record (`timer_late`; the drafted
  `timers_rearmed` was retired before shipping) and two new `uc2ctl status` fields
  (`log_time_ns=`,
  per-row `timers_pending=`) follow the same "added, not renamed" convention.
- **Jumbo-frame discovery (2.12 pending) adds to the two operator-facing
  line formats, additively.** `audit.jsonl` gains a **13th key**, `source`
  (`"operator"` on every admin request, `"discovery"` on the `settings_apply`
  a node writes itself when the committed rung rises), appended LAST so every
  byte before it is unchanged. Backup tier 3b (`2.14.0`) appends a **14th**,
  `fetch_source` (`"tier"` or `"peer"` on a snapshot-fetch record, `null`
  elsewhere), on the same rule. The audit line's key ORDER is a contract
  (`uc_node::audit`, which shares one formatter with `uc_node::obs::log`), and
  the rule that makes a 13th or 14th key a minor change rather than a breaking one is:
  **keys are appended, never reordered, renamed or removed**, and a reader
  looks fields up by name — `uc2ctl audit` and the test readers do. `uc_node`
  is not in the promised-surface table, so this is not semver-governed API, but
  the audit file is an operator interface in practice and is held to that rule,
  exactly as the metric series contract is ("families are added, not
  renamed"). New metric families (`uc2_datagram_mtu_bytes`,
  `uc2_payload_ceiling_bytes`, `uc2_probe_min_mtu_bytes`,
  `uc2_probe_sent_total`, `uc2_probe_acked_total`, `uc2_send_emsgsize_total`,
  `uc2_commands_over_standard_total`, `uc2_jumbo_gate_pending`), two alert
  rules and the jumbo `[log]` records follow the same convention. The
  `node.toml` side is not additive — `max_payload` is **refused by name** and
  `force_jumbo_frames` is the optional new key — and rides the flag day
  described below.
- **The monotonic log clock (2.12 pending) adds one metric family and one
  `[log]` record, both additive**: the `uc2_log_clock_smear_ns` gauge
  (per-node, not leader-gated — ns of a backward wall-clock step still being
  retired, `0` when none) and the `log_clock_step` record (`direction`,
  `step_ns`, `smear_ns`), emitted once per detected step. No Rust surface,
  wire frame or cnc field changed — `pass_clock()` moved from
  `SystemTime::now()` to the private `uc_node::log_clock` module, an
  internal detail nothing outside `uc_node` depends on. Nothing was renamed
  or removed.

The normative descriptions live where the surface does:
[the state-machine contract](state-machine-contract.md),
[configuration](configuration.md), [`gateway.toml`](gateway-config.md),
[the remote protocol](remote-protocol.md), [`uc2ctl`](uc2ctl.md), and the
[rustdoc](https://peterknego.github.io/ultima_cluster/) for the library
types. This page says only *what is promised*, not what each thing does.

Two of those rows are not Rust API and deserve saying out loud:

- **`node.toml` and `gateway.toml` are API.** Removing a key, renaming one,
  or changing what an existing value means is a breaking change even though
  no Rust signature moved. Making a previously-optional section *required*
  is breaking too — which is exactly what `v2.6.0` did to `[crypto]` and
  `[admin]`, and why the upgrade note exists
  ([Upgrade a cluster](../how-to/upgrade-a-cluster.md)).
- **`uc2ctl`'s exit codes are API.** Scripts branch on them. The binary
  exits `0` on success and non-zero on failure: `1` for any runtime
  failure (the single `process::exit(1)` in it, reached by every runtime
  failure path), `2` for a command-line usage error (clap — an unknown
  flag, a missing `--instance-dir`, a bad subcommand). The `0` accepted /
  `1` refused / `2` retry triple is a **printed value, not an exit code** —
  it is the response status the node returns and `uc2ctl` prints; statuses
  `1` and `2` both exit `1`. See
  [`uc2ctl`: Response statuses](uc2ctl.md#response-statuses).

## What is not promised

Everything not in the table above. Concretely, and per crate, these public
modules are public because the workspace's own crates, tests and gate
harnesses need them across crate boundaries — **not** because they are an
API for downstream code. They may change in any release:

| Crate | Not promised |
|---|---|
| `uc_protocol` | all of it — `ring` (the lock-free ring buffers — not the `ring` crypto crate that `deny.toml` bans), `v2`, `magic`, `error_codes`, `version`, `identity` (FSM identity, 2.11.0 — `FsmName`, `FsmIdentity`, `fnv1a_64`, packed version). It is the wire spec, governed by the level rule below, not by semver. `2.11.0` also **relays `FrameHeader`** (`session_id`/`correlation_id: u64` become `client_id`/`seq: u32` plus `time_ns: u64`) and adds `FRAME_TYPE_TIMER`, `TimerBody`, `MSG_V2_SCHED` and `SchedRecord` — a breaking Rust change on an unpromised item, and a flag day on the wire. |
| `uc_log` | `agent`, `archive`, `buffer`, `cnc`, `counters`, `reader`, `region`, `state`, `writer` |
| `uc_consensus` | `commit`, `config`, `election`, `reconcile` |
| `uc_net` | `fault`, `flow`, `rebuild`, `receiver`, `sender`. **`2.8.0` changed these signatures, in a minor release** — see the note below. |
| `uc_crypto` | `admin`, `group`, `handshake`, `identity`, `replay`, `rotation`, `schedule`, `seal`, `transport` |
| `uc_node` | `audit`, `backup`, `ipc`, `obs`, `preflight`, `recovery`, and everything in `node` except `NodeConfig`/`Node::start*` |
| `uc_service` | `snapshots` |
| `uc_gateway` | `config_file`; `Edge`/`EdgeStats` beyond what `gateway.toml` implies |
| `uc_remote` | `conn`, `frame` as *Rust items* — the wire format they encode is promised (the remote protocol version, v2 since read-your-writes), the Rust names are not |
| `uc_journal` | `bench_support` (already `#[doc(hidden)]`), and the crate generally: it is the node's storage primitive, published so the workspace can be, not offered as a general-purpose journal |

`uc_net`'s snapshot-session seam is the worked example of what "not
promised" means in practice: **`2.8.0` (M14c) changed those public signatures
in a minor release**, deliberately, because one log now feeds N FSMs and a
session ships one artifact per declared id rather than one per session.

| item | ≤ `2.7.0` | `2.8.0` | `2.11.0` (FSM identity) |
|---|---|---|---|
| `sender::SnapshotSource` | `Arc<dyn Fn() -> Option<(u64, PathBuf, u64, Vec<u8>)>>` | `Arc<dyn Fn() -> Option<SnapshotSet>>` | unchanged |
| `sender::SnapshotSet` / `sender::SnapArtifact` | did not exist | the set (`services_declared`, `config`, one `SnapArtifact` per declared id) | unchanged (identity/version ride on `SnapBeginBody`, not this seam) |
| `receiver::Receiver::set_snapshot_intake` | `(snap_dir, incoming)` | `(snap_root, own_declared, incoming)` | `(snap_root, [u64; 8], Arc<dyn Fn() -> [u32; 8]>, incoming)` — the node's own per-row identity hashes plus a closure the receiver calls for the node's own per-row versions (read fresh off the cnc slot band; `uc_net` has no cnc dependency of its own) |

Nothing downstream broke, because nothing downstream is supposed to be
calling them — `uc_node` is the only caller, and it ships in lockstep. The
wire change that rode along (`SNAP_BEGIN`) is a **flag day**, governed by the
rule below, not by this table: wire `0.6.0`, then `0.7.0` for FSM identity.

Also outside the promise:

- **`#[doc(hidden)]` items**, wherever they appear.
- **Non-default features**, which exist for measurement and testing:
  `uc_service/apply-profile` (rdtsc probes on the apply thread),
  `uc_gateway/test-util` (`Edge::fault_for_tests`),
  `uc_protocol/uc-bench-probes`, `uc_journal/bench-support`,
  `uc_node/mutation-testing` and `uc_consensus/mutation-testing`.
- **`uc_sim`, `uc_lincheck`, `examples/counter`, `examples/kv`,
  `examples/uc_adjudicate`, `testing/uc_crashtest`** — six. These are
  `publish = false`: the proof and teaching
  apparatus, not the product. Nothing in them is API, and they are not on
  crates.io. (`uc_diffreplay` is **not** in this list — it is the fourteenth
  published crate, because an application's own CI runs its driver.)

## The wire and the cnc page: levels and flag days, not semver

Two version numbers are deliberately *outside* this policy, because semver's
"a minor bump is safe" contract is the wrong promise for them:

- **The node-to-node wire protocol** (`uc_protocol::version::CURRENT`,
  currently `0.12.0` (the wire level, `2.15.0`); `0.11.0` (the snapshot
  catalog, the snapshot lifecycle and the backup tier — `0.10.0`, #33's row
  running version, was never released on its own and folded into it)
  shipped in `2.14.0`, `0.9.0` in `2.13.0` — see
  [wire protocol](wire-protocol.md)).
- **The `cnc.dat` page layout** (`CNC_V2_VERSION`, currently cnc `3.5` (the
  wire level's three words, `2.15.0`); cnc `3.4` shipped in `2.14.0`, `3.3`
  in `2.13.0` — see [the cnc control page](cnc-page.md)).

Neither follows the crate's semver, and since `2.15.0` neither is simply "a
flag day" either. **The level rule:**

- **A wire minor is a level.** `0.12.0` is level 12, and every binary runs
  inside a range `WIRE_MIN..WIRE_MAX` one level wide (`WIRE_MIN = WIRE_MAX -
  1`; `0.12.0` alone is `12..12`, since nothing below it is level-aware). A
  cluster commits one level and raises it only once every member reports it
  can follow, so a release whose wire minor is the cluster's level or one
  above it **rolls**, one node at a time under load. One level back is the
  whole promise: two levels need the middle release.
- **A wire major is a flag day**: every node stopped and restarted on the new
  version together. So is everything the wire level section below lists as
  staying a flag day by choice (the header lengths, crypto on or off, cnc
  growth beyond the reserved bands).
- **Stop-all is not an escape from the level.** From `0.13.0` on, a cluster
  two levels behind must step through the middle release even with every
  node stopped, because the new binary has dropped the older level's
  encoders; it stops with `WireLevelBelowBinary` and names the release.
- **A cnc minor is a per-host restart.** An older attacher refuses a newer
  page, so a host's node and every process attached to its instance
  directory (services, shmem clients, the gateway) restart together. It is
  per host, so it fits inside a roll; a cnc **major** stays a cluster flag
  day.

`2.15.0` (wire `0.12.0`, cnc `3.5`) is itself the last additive flag day:
a `0.11.0` node carries no level, so it is refused by name rather than
rolled. Every bump before it was a flag day in which mixed-version operation
was unsupported and was not made safe by the version numbers agreeing on a
major. `v2.5.0`'s content-attested durable reports are the worked example —
a `0.4.0` peer's report reads as unattested and is not counted, so a mixed
cluster stalls commits rather than making unsound ones. Both procedures, the
rolling one and the stop-all one, are
[Upgrade a cluster](../how-to/upgrade-a-cluster.md); they apply whether or
not the crate version's major digit moved.

**`2.15.0` ships the most recent flag day on both lines** (wire `0.12.0`,
cnc `3.5`); `2.14.0` shipped the one before (wire `0.11.0`, cnc `3.4`;
`0.10.0` folded into it), and `2.13.0` the one before that.
`2.11.0` carried **two**
wire-and-page features at once: wire `0.6.0` → `0.7.0` and cnc `3.0` → `3.1`,
bundled as one combined flag day per the rule that stood until `2.15.0`, that
a cnc layout change is a flag day regardless of the digit.

- **FSM identity** — `SNAP_BEGIN`'s bitmask becomes a per-row identity-hash
  array plus a per-row version array (a 0.6.0 sender's shorter body is dropped
  by length, the same as every prior bump); the once-reserved cnc slot line 7
  becomes node-written at boot (name + hash).
- **Log time and timers** — the 32-byte log frame header is **relaid**:
  `session_id: u64` and `correlation_id: u64`, of which the client filled only
  32 bits each, become `client_id: u32` + `seq: u32`, freeing 8 bytes for a
  leader-written `time_ns: u64` stamp. A new frame type (`TIMER = 5`) rides
  along, plus two cnc words (`log_time_ns` at page 1 offset 4048, and per-row
  `timers_pending` at slot line 7 `+488`) and one new per-row IPC ring
  (`svc_sched.<row>.ring`). The relayout is the sharper half of this flag day:
  a `0.6.0` peer's frames still *parse* on a `0.7.0` node and mean something
  different, so mixing versions is not merely unsupported, it is unsound.
  Upgrade every node together.

### The `2.12.0` flag day

Jumbo-frame MTU discovery is a further, separate flag day: wire `0.7.0` →
`0.8.0` and cnc `3.1` → `3.2`. Two new **pairwise** datagram kinds, `PROBE`
(24) and `PROBE_ACK` (25) ([wire protocol](wire-protocol.md#probe--probe_ack-bodies-wire-080-2120)),
and one new live cnc word, `payload_ceiling` at offset 3984
([cnc page](cnc-page.md#counters-and-status)) — no existing wire layout
changes, unlike `2.11.0`'s relaid log frame header. The replicated Settings
record gains a new field, `datagram_mtu: u32` (`0` = the baseline rung); a
`0.7.0`/v1 Settings record is still **accepted on read** (it decodes with
`datagram_mtu` defaulting to `0`), so no instance directory needs a wipe for
this flag day alone. `node.toml`'s `max_payload` key is **refused by name**
as of this flag day, pointing at discovery. As with every prior bump, mixed
versions are unsupported: stop every node before starting any node on the
new build.

### The `2.13.0` flag day

The FSM upgrade lifecycle is a further, separate flag day: wire `0.8.0` →
`0.9.0` and cnc `3.2` → `3.3`. It is three surfaces, not two, and the third
one costs a file deletion on every node.

**Wire `0.9.0`.** Two new `CLUSTER` kinds, `4` `UpgradePin` and `5`
`SnapshotReport` ([wire protocol](wire-protocol.md#cluster-body-wire-070)),
and one new **pairwise** datagram kind, `SNAP_REPORT` (`26`), which carries a
node's `(row, position, artifact hash)` to the leader. No existing wire layout
changes, so a `0.8.0` peer's frames of these kinds still *parse* (the `CLUSTER`
prefix is unchanged) but decode as an unknown kind and are dropped, and that
node's cluster FSM silently diverges from one that applied them: stop every
node before starting any node, exactly as for a layout change.

**cnc `3.3`.** Four new node-written words on the row's **service status
line** — `upgrade_origin` (`+16`), `pinned_version` (`+24`), the seqlock commit
word `pin_seq` (`+32`) and `pinned_from` (`+40`) — plus one
**service**-written word on slot line 7, `artifact_hash` (`+504`)
([cnc page](cnc-page.md#counters-and-status)). The first four are published
together under `pin_seq`: a reader takes them through
`ServiceStatusLine::pin()`, which returns a set of values that were stored
together, or reports the read as contended. `ClusterFsm::VERSION` itself stays
`1` — it is the cluster image's own on-disk version (`2`, since this flag day)
that gates artifact compatibility, not the FSM identity version a user's own
state machine declares, and a version-1 image is still **read** (with empty
pin and report histories), so a `2.12.0` node's recovered cluster artifact is
fine.

**`ULTSNAP2`, and the one-time wipe.** Every **row** artifact now carries a
24-byte envelope, `magic ‖ P: u64 ‖ version: u32 ‖ 4 reserved`, naming the
`S::VERSION` that built it — which is what the pinned install's cross-check
reads. `ULTSNAP1` (16 bytes, no version field) is refused **by name**, never
decoded as version 0, so **`snapshots/<row>/` is cleared once per node** during
the upgrade window. `snapshots/cluster/` is untouched. The precondition is
stated in [Upgrade a
cluster](../how-to/upgrade-a-cluster.md#wire--cnc-change-in-2130-upgrade-pins-and-snapshot-reports-090-cnc-33):
the row must be able to rebuild what was deleted, which means a durable state
machine or a journal that still holds genesis. A cluster running purge with an
in-memory state machine can do neither.

### The row running version flag day (`0.10.0`, cnc `3.4`)

Issue [#33](https://github.com/PeterKnego/ultima_cluster/issues/33), not yet
released: wire `0.9.0` → `0.10.0` and cnc `3.3` → `3.4`, one more flag day on
the same terms as the rest.

**Wire `0.10.0`.** One new `CLUSTER` kind, `6` `RowGenesis` (8 bytes: `row ‖
reserved [3] ‖ version`), which the leader appends on its own to record a
row's first running version ([wire protocol](wire-protocol.md#cluster-body-wire-070)).
No existing layout changes. A `0.9.0` peer refuses kind 6 as undecodable and
advances past it, so its cluster FSM never learns any row's running version
and silently diverges: stop every node before starting any node.

**cnc `3.4`.** Three node-written words, no layout move: `running_version`
(`+48`) and `running_record_pos` (`+56`) on the row's service status line,
published under the existing `pin_seq` seqlock with the pin words (line 0 is
now full), and `cluster_applied` at page-1 offset `4056`
([cnc page](cnc-page.md#counters-and-status)). Every new word reads `0` as
absent.

**Cluster image 2 → 3, no wipe.** The cluster artifact gains a trailing
per-row running-version blob. Versions 1 and 2 are still read: a row with
pin history takes its newest pin's `to` as its running version (record
position = the image's applied position), and a row without pins has none
until the leader records one. No `snapshots/` directory needs clearing for
this flag day. `ClusterFsm::VERSION` itself stays `1`, as before.

**What changes for an application.** No trait changes, but three behaviour
changes an operator will see:

- a service whose `VERSION` is off its row's running line (major.minor) is
  **refused at attach** with the new `ServiceError::VersionMismatch`;
- an attached service **fail-stops** at a record that moves its row to
  another line (`version_superseded`), with `applied` at exactly the record's
  start — so `Service::is_alive()` can now turn `false` at a pin;
- a leader admits **no client writes** until every declared row has a running
  version, which it records from its own attached services.

And one relaxation: the snapshot envelope's version check (both the unpinned
install's and the pinned install's cross-check against `from`) compares
**lines**, not exact versions, so patch builds of one line install each
other's artifacts. The rules are [Upgrade an application § The version
rules](../how-to/upgrade-an-application.md#the-version-rules).

**API note.** `ServiceError` gains a variant, `VersionMismatch { name, row,
running, mine }`. `ServiceError` is a promised type and is not
`#[non_exhaustive]`, so an exhaustive `match` on it downstream stops compiling
— the same documented minor-version hazard `Outcome` and `SubmitError`
carried in M14b. Adding a variant is additive under this policy.
`uc_protocol::identity::same_line` is new, but `uc_protocol` is not promised.

### The snapshot catalog flag day (`0.11.0`)

Shipped in `2.14.0`, and a further flag day on the same terms:
wire `0.10.0` → `0.11.0`, cnc **unchanged** at `3.4` — the catalog is not on
the shmem page, only on the cluster artifact and `/metrics`.

**Wire `0.11.0`.** `STATUS`'s body grows from 16 B to 144 B, carrying a
per-node `Holdings` advertisement (journal span, catalogued sets held,
per-row applied, byte counts). No wire-version word rides node↔node
datagrams: a `0.11.0` leader drops a `0.10.0` follower's 16 B body
(counted, `uc2_status_refused_total`), so replication to that follower
stalls, while a `0.10.0` leader accepts a 144 B body and ignores the tail
— unsound both ways. `SNAP_REPORT`/`SnapshotReport` (`CLUSTER` kind 5) admits row `255`
— the cluster artifact's own hash, closing the gap recorded on the backlog
since plan B3. `Settings` grows to encoding version 3 (35 B), adding
`retain_sets: u16`; a version-1 or version-2 record is still accepted on
read, with `retain_sets` reading `1` (the newest-only retention it ran). No existing layout changes
anywhere, so a `0.10.0` peer's frames of these kinds still *parse* but
decode as undecodable/unknown and are dropped — its cluster FSM silently
diverges rather than refusing outright. Stop every node before starting any
node.

**Cluster image 3 → 4, no wipe.** The artifact gains a trailing
length-prefixed catalog blob; a v1–v3 image is still **read**, with the
blob empty (the `Empty` state, [Upgrade a
cluster](../how-to/upgrade-a-cluster.md#wire-change-after-0100-the-snapshot-catalog-0110)).
`snapshots/<row>/` and `snapshots/cluster/` are both untouched.

**What changes for an operator.** The purge floor and every install source
move from "the newest complete set on this node's own disk" to "the
newest set the cluster **agrees** on, that this node holds" — a diverged
set (rows that disagree) is visible and fetchable but never the floor and
never installed. `retain_sets` (default `1`, the prior newest-only
behaviour) is the first replicated retention policy; `0` and anything above
`56` are refused with `47`. No API change: this flag day is entirely wire,
cnc-page-adjacent (the artifact, not the live page) and operator-surface.

### The wire level (0.12.0, cnc 3.5) — the last additive flag day

`2.15.0`, issue [#31](https://github.com/PeterKnego/ultima_cluster/issues/31):
wire `0.11.0` → `0.12.0` and cnc `3.4` → `3.5`. It is a flag day on the old
terms — stop every node before starting any — and it is the release that
ends them for additive wire changes (the level rule above). The argument is
[the wire level explainer](../notes/uc2-wire-level-explained.md).

**Wire `0.12.0`.** Datagram header byte 13, the always-zero `flags` byte,
becomes `wire`: every datagram carries its sender's `WIRE_MAX`, and a
receiver refuses any byte outside `[WIRE_MIN, WIRE_MAX + 1]` before reading
anything else — `0` counted as `prelevel` (a `0.11.0` peer), anything else as
`out_of_range` ([wire protocol](wire-protocol.md#datagram-header)). The
replicated `Settings` record grows to version 7 (49 B) with `wire_level` and
`wire_raise`; versions 1–6 still decode, as level 12 with auto raise.
`NEW_TERM` gains an 8-byte body written as zeros (a 64 B frame). This is the
**first flag day a mixed cluster is refused rather than misparsed**: a
`0.11.0` node's datagrams carry `0` and are refused by name and counted
(`uc2_wire_datagrams_refused_total{reason="prelevel"}`).

**cnc `3.5`.** Three words, no layout move: `wire_level` at page-1 offset
`3992` (the `uc2-cluster` agent), `wire_binary` at `4000` (the node, at boot,
`(WIRE_MIN << 8) | WIRE_MAX`) and `wire_max` at peer slot `+8` (the consensus
agent, leader-authoritative) ([cnc page](cnc-page.md)).

**No wipe.** The cluster image version is unchanged; no `snapshots/`
directory is cleared.

**Constants.** `uc_protocol::version` gains `WIRE_LEVEL_FIRST = 12`,
`WIRE_MAX` (`CURRENT`'s minor), `WIRE_MIN` (by const arithmetic, never
hand-edited) and `WireRange`, and `MIN_COMPATIBLE` becomes `0.WIRE_MIN.0`
rather than documentary. `uc_protocol` is not a promised crate, so none of
this is API.

**What stays a flag day, by choice.** The wire major and the cnc major; the
16-byte datagram header's length and the `DATA` self-locating contract; the
32-byte frame header's length; crypto on or off per cluster; cnc page growth
beyond the reserved bands.

**Release discipline from here.** A release that adds no gated wire feature
keeps `CURRENT` and rolls with no step. A release that adds one bumps the
wire minor, writes exactly one `if level >= N` gate, deletes the `N - 1`
gate as always-true, adds the new level's golden corpus beside the old
one (`uc_protocol/tests/golden_level12.rs` is the first; an existing
level's file is never edited, since it pins what that level emits), keeps
every older
**decoder** and drops the `N - 1` **encoders**. A consensus safety fix ships
as a gated level by default; the release notes may declare it a flag day
instead when running pre-fix until the raise is unacceptable. The checklist
line is in [Cut a release](../how-to/cut-a-release.md).

**API notes.** None for an application: no trait, config struct or SDK type
changed. Operator surface only — `uc2ctl wire show` / `wire raise` (admin op
12, refusal `63 wire_holdout`), the `wire_raise` settings-file key, the
`wire:` line in `uc2ctl status`, five metric families and the
`Uc2WireLevelBelowMembers` alert.

### `2.13.0` API notes

Three public-API changes ride this minor. None of them is a semver *break* by
the rules above, and all three are named here rather than left to be
discovered at a downstream `cargo build`.

- **`boot_wait` is a new public field on three config structs** —
  `uc_service::ServiceConfig`, `uc_client::EngineConfig` and
  `uc_client::PipelinedConfig` ([configuration §
  `boot_wait`](configuration.md#attaching-a-service-or-a-client-boot_wait)).
  Adding a public field to a struct with public fields is **additive for every
  caller that constructs it from `Default` or a builder, and breaking for one
  that writes an exhaustive struct literal**. This policy's position: a struct
  whose fields are public and whose `Default` is the documented way to build it
  may gain a field in a minor; a caller that spells every field out is asking
  for that coupling, and the fix is `..Default::default()`. The alternative —
  `#[non_exhaustive]` on the config structs — would break the plain-struct
  construction the SDK's own examples use, which is a worse trade.
- **`ApplyCtx::ids` takes `&mut self`** (`uc_service/src/traits.rs`), because
  the driver counts a frame's `ids()` calls as a comparison surface. `apply`
  receives `&mut ApplyCtx`, so every in-tree caller and every ordinary state
  machine compiles unchanged; a caller holding a shared `&ApplyCtx` is the one
  shape that does not.
- **`SnapshotStateMachine::project` is a provided method**, not a required one,
  so no existing implementation has to change. The default returns
  `SnapshotError::Codec("project() not implemented by this state machine (diff
  replay spec §5.8)")`, so a row that never overrides it simply has no state
  surface for diff replay to compare — implement it if you want one.

## The one-way door: one tier per type

`uc_service` has two state-machine tiers, and a type implements **exactly
one** of them:

```rust
pub trait RawStateMachine: Send + 'static { /* bytes in, bytes out */ }
pub trait StateMachine:    Send + 'static { /* typed apply/query   */ }

impl<S: StateMachine> RawStateMachine for S { /* bincode standard */ }
```

That blanket impl is the reason a typed state machine works everywhere a raw
one does, with no wrapper type. It is also a door that only opens once: a
*second* blanket impl of `RawStateMachine` for any other trait would overlap
with this one and fail to compile, and removing this one would break every
typed state machine in existence. **`RawStateMachine` will therefore never
gain another blanket impl, in any version.** A third tier, if one is ever
wanted, has to be an explicit wrapper type the user names.

The practical consequence for your code: implement `StateMachine` **or**
`RawStateMachine`, never both on the same type. See
[the state-machine contract](state-machine-contract.md).

## MSRV

`rust-version = "1.89"` in `[workspace.package]` is the floor, and it is a
real one: `std::fs::File::try_lock_exclusive`/`unlock`
(`uc_node/src/backup.rs`) stabilised in 1.89.0. `rust-toolchain.toml` pins
a newer stable (1.96.0) for this repository's own builds and for releases,
and that pin moves freely — it is not the floor. CI's `msrv` job proves the
floor separately by running `cargo clippy --workspace --all-targets --locked
-- -D warnings` on a real 1.89.0 toolchain.

**Raising the floor is a minor bump, not a major one.** It is announced in
`RELEASES.md` when it happens. Lowering it is not promised at all.

## Breaking means 3.0.0

There is no `2.x` deprecation lane and no compatibility shim layer. If a
promised item has to change incompatibly, the version becomes `3.0.0`, the
change is described in `RELEASES.md` with what to do about it, and — for
anything touching the wire, the cnc page or the on-disk layout — in
[Upgrade a cluster](../how-to/upgrade-a-cluster.md).

### The one carve-out: the 2.9.0 crate rename

`2.9.0` renamed every crate to a uniform `uc_` prefix (`uc2_node` →
`uc_node`, `ultima-journal` → `uc_journal`, the `uc2ctl` package →
`uc_ctl`), which moves the path of every promised item — `RawStateMachine`
is now `uc_service::traits::RawStateMachine`. Under the rule above that is a
major bump. It shipped as a minor one, for one reason that cannot recur:

**Nothing had ever been published to crates.io.** The ordered publish in
[Cut a release](../how-to/cut-a-release.md) §6 had not been run once, so no
resolver anywhere could have picked up the old names, and no lockfile could
break. What the rename could break is a source tree that types
`uc2_node::` or builds `-p uc2_node` by hand — a `sed` away, and named in
`RELEASES.md`. The wire protocol, the `cnc.dat` page, the on-disk layout and
every binary name (`uc2-node`, `uc2ctl`, `uc2-gateway`) were untouched, so
no cluster and no operator command changed at all.

This is an exception granted once, on a fact that stopped being true the
moment `2.9.0` was published. **Any later rename of a promised path is a
`3.0.0`**, whatever the reason.

### FSM identity: a breaking trait and config change riding as a minor

FSM identity (spec
`docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md` §10) is a
**second, narrower carve-out**, on the project having no external users yet
to break — not the "nothing published" fact 2.9.0 relied on (crates.io
publishing started at `2.9.0`; every crate is live, so this one *could*
break a resolver, in principle, if anyone depended on the changed items).
The break, under the rule above, is real:

- `RawStateMachine`/`StateMachine` gain a **required** `const NAME:
  &'static str`; `apply`'s signature changes from `(&mut self, position:
  u64, cmd)` to `(&mut self, ctx: &mut ApplyCtx, cmd)` — every implementor
  in a downstream tree must add both.
- `uc_service::ServiceConfig` **loses** its `.service_id()` setter — a
  service no longer states its row, it is found by name.
- `uc_node`'s `--service-id` CLI flags are **gone**, replaced by `--fsm
  <name>` on the harness binaries and `--services <names>` on the node.
- `[services]` goes from optional (absent ⇒ `ids = [0]`) to **required**,
  and its `ids` key is refused outright, pointing at `names`.

Two later changes in the same release ride **this same carve-out**, because
they are the same kind of break in the same flag day:

- the retired `uc_service::SnapshotPolicy` and `ServiceConfig::snapshot_policy` are
  **removed** — removing a promised type is a MAJOR change under the rule
  above. The snapshot cadence became cluster data (the replicated
  `snapshot.interval_bytes`), so a service no longer chooses one:
  `start_with_snapshots()` is the whole opt-in, and the trigger is the log.
- The `node.toml` keys **move**: a top-level `admission_bytes` and a
  `[services] fsm_lag` are startup refusals by name, pointing at `[settings]`
  and `uc2ctl settings apply`. A config-schema break is not covered by the
  API rule above at all — `node.toml` is an operator interface, versioned by
  the flag day (`docs/how-to/upgrade-a-cluster.md`) rather than by semver —
  but it is recorded here so the whole break is in one place.

The maintainer's decision (spec §10, applied 2026-09-02): ship this as the
**next minor**, `2.11.0`, rather than `3.0.0` — the same reasoning as every
other freely-made API change on this project while it has no external
users (there is no deprecation lane to skip, and no lockfile anywhere
depends on the old shapes). This is **not** a standing exception: it is one
maintainer decision for one release, recorded here so it is not mistaken
for a change to the rule above. The next breaking change is `3.0.0` unless
the maintainer says otherwise again, at the time.

### Snapshots become mandatory (#67)

Mandatory snapshots (spec `docs/superpowers/specs/2026-09-28-uc2-mandatory-snapshots-design.md`,
D1/D2) rides the **same no-external-users carve-out** as FSM identity above,
for the next release past `2.13.0`. `ServiceBuilder::start()` now requires
`S: SnapshotStateMachine`, and `start_with_snapshots()` — the previous
opt-in, listed above under `2.11.0`'s FSM identity break — is **removed
outright**, with no deprecation shim. Under the rule at the top of this
section, removing a public method is a breaking change; it ships as a minor
for the same one-maintainer-decision reason as every prior carve-out on this
line. The new `WholeStateSnapshot` helper trait (`uc_service::snapshots`) is
additive — see [the state-machine contract § Snapshots](state-machine-contract.md#snapshots-required-the-instant-the-envelope-and-the-exclusive-frontier).

### Read-your-writes reads (`2.14.0`)

Spec `docs/superpowers/specs/2026-10-08-uc2-read-your-writes-design.md`,
explainer `docs/notes/uc2-read-your-writes-explained.md`. Two items, neither
touches the node-to-node wire:

- **`FLAG_V2_MIN_POSITION`, folded into cnc `3.4`.** A new query flag whose
  record carries an 8-byte `min_position` prefix, with no layout change. It is
  folded into cnc 3.4 (`2.14.0`) rather than bumping it (released `2.13.0`
  shipped 3.3). Additive API (a minor): new `query_read_your_writes*`,
  `query_at_least_on`, `read_token`, `observe` and `ReadToken`. Because a 3.4
  client attaches to a 3.3 page (`version_compatible` accepts an older
  minor), the client refuses a read-your-writes read with a non-zero token on
  such a page by name (`ReadYourWritesUnsupported`) instead of sending a
  record an old node would misread. Dev builds of 3.4 from before this
  feature would accept the flag and misread it; none was released.
- **Remote protocol v2.** `PROTOCOL_VERSION` 1 to 2 and the query flag
  `FLAG_MIN_POSITION = 0x20`. A v1 client and a v2 edge refuse each other at
  `HELLO` with `HELLO_REFUSED_VERSION`: upgrade gateways and remote clients
  together. This retires the old statement that the remote protocol stays v1.

**API note.** `Consistency` (in `uc_client` and in `uc_remote`) gains the
variant `ReadYourWrites`, `SubmitError` gains `ReadYourWritesUnsupported`, and
`ClientError` gains `ReadYourWritesUnsupported`; `EngineStats` and
`RemoteStats` gain a public field `stale_answers`. None of these types is
`#[non_exhaustive]`, so an exhaustive `match` on the enums, or a struct
literal of the stats, stops compiling downstream: the same documented
minor-version hazard `Outcome` and `SubmitError` carried in M14b. Adding a
variant or field is additive under this policy.

### A remote client's starting seq (#34, `2.14.0`)

`uc_remote::RemoteConfig` gains a public field `first_seq` (default `1`), and
`RemoteSendHalf`/`RemoteClient` gain `next_seq()`, plus the constant
`MAX_FIRST_SEQ`. No wire change: the seq was always a client-chosen `u64`.
The field is the `boot_wait` case above: additive for a caller that builds
`RemoteConfig` from `Default`, breaking for an exhaustive struct literal.

## Related

- [Cut a release](../how-to/cut-a-release.md) — the lockstep bump, the tag,
  and the manual crates.io publish order.
- [Upgrade a cluster](../how-to/upgrade-a-cluster.md) — what taking a new
  version costs an operator.
- `RELEASES.md` — what each version introduced, feature by feature.
