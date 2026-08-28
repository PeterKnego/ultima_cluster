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
| `RawStateMachine` | `uc2_service::traits::RawStateMachine` |
| `StateMachine` | `uc2_service::traits::StateMachine` |
| `SnapshotStateMachine` | `uc2_service::traits::SnapshotStateMachine` |
| `OutputHandler` (and `RawOutputHandler`) | `uc2_service::traits` |
| `Sessioned<S>` and `SessionConfig` | `uc2_service::session` |
| `NodeConfig`, and the `node.toml` it mirrors | `uc2_node::node::NodeConfig`, `uc2_node::config_file` |
| Starting a node: `Node::start`, `Node::start_with` | `uc2_node::node::Node` |
| `gateway.toml` | `uc2_gateway::config` (`EdgeConfig`, `Member`) |
| The three client tiers | `uc2_client::{Engine, PipelinedClient, Client}` |
| The `uc2_remote` protocol, **v1** | `uc2_remote::frame::PROTOCOL_VERSION` |
| `RemoteClient`, the Rust implementation of it | `uc2_remote::client::RemoteClient` |
| `uc2ctl` verbs and exit codes | the `uc2ctl` binary |

Promising an item promises **the types it unavoidably exposes to a caller**
along with it — a signature you cannot name is not usable API. Concretely,
and covered by this policy on the same terms as the rows above:

- `uc2_service::{ServiceBuilder, Service, ServiceConfig, ServiceError,
  SnapshotPolicy, SnapshotError, OutputError}` — `ServiceBuilder::new(cfg,
  sm).start()` is how a state machine is attached at all, and it returns
  `Result<Service<S>, ServiceError>`.
- `uc2_client::{PipelinedConfig, Ticket, ClientError, WaitStrategy}` — the
  configuration, the handle and the error type the `PipelinedClient`/`Client`
  tiers hand back.
- `uc2_node::{StartOpts, SubmitError}` — `Node::start_with` takes the first
  and the node's submit path returns the second.
- `uc2_remote::{RemoteConfig, RemoteResponse, RemoteError}` —
  `RemoteClient::connect` takes the first and every request resolves into one
  of the other two.

`Engine`'s own send/poll halves and their outcome types
(`uc2_client::{EngineConfig, SendHalf, PollHalf, Completion, Outcome,
Consistency, SubmitError}`) are in the same position and are covered too.

- **M14b adds per-FSM routing and fan-in to the `uc2_client` surface**:
  `FanInTicket`; `Outcome::{Responses, BadService}`;
  `SubmitError::ServiceNotDeclared`; `ClientError::ServiceNotDeclared`;
  `SendHalf::{declared, try_submit_to, try_submit_all, try_query_on}`;
  `PipelinedClient::{declared, submit_to, submit_all, query_snapshot_on,
  query_linearizable_on}`; `Client::{declared, submit_to, submit_all,
  query_snapshot_on, query_linearizable_on}` — all additive (minor).
  `uc2_client` now depends on `bytes` (`Outcome::Responses` and
  `FanInTicket` expose `bytes::Bytes`, the same type the SM contract uses).
  One behavioural change is worth flagging even though it is not a
  breaking change under this policy: `Outcome` and `SubmitError` gained
  variants, so an exhaustive match on either downstream breaks at compile
  time — a documented minor-version hazard of the three-tier promise.
- **M14c adds the per-FSM observability surface**, all additive:
  `uc2_service_attached`, `uc2_service_lag_bytes`,
  `uc2_service_lag_waits_total`, `uc2_services_declared` and
  `uc2_fsm_lag_bytes` as new metric families; a `service="<id>"` sample per
  declared id on `uc2_service_applied_bytes`, `uc2_service_epoch`,
  `uc2_service_snapshot_pos_bytes` and `uc2_service_heartbeat_age_seconds`;
  the `Uc2ServiceAbsent` and `Uc2ServicePinnedAtLagBound` rules; the
  `service_attached`/`service_detached` `[log]` records; and a `services:`
  section in `uc2ctl status`'s output. Nothing was renamed or removed. Two
  consequences worth flagging even though neither is breaking under this
  policy: a query that assumed one sample per `uc2_service_*` family now
  sees several, so `sum(...)` double counts unless it says `{service=""}`
  (the shipped rules and dashboard were updated); and a scraper of
  `uc2ctl status` stdout sees new lines between `log:` and `members:`.
  The metric series contract is not itself in the promised-surface table —
  `uc2_node::obs` is listed as not promised — but it is treated as an
  operator interface in practice: families are added, not renamed.

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
| `uc_protocol` | all of it — `ring` (the lock-free ring buffers — not the `ring` crypto crate that `deny.toml` bans), `v2`, `magic`, `error_codes`, `version`. It is the wire spec, governed by the flag-day rule below, not by semver. |
| `uc2_log` | `agent`, `archive`, `buffer`, `cnc`, `counters`, `reader`, `region`, `state`, `writer` |
| `uc2_consensus` | `commit`, `config`, `election`, `reconcile` |
| `uc2_net` | `fault`, `flow`, `rebuild`, `receiver`, `sender` |
| `uc2_crypto` | `admin`, `group`, `handshake`, `identity`, `replay`, `rotation`, `schedule`, `seal`, `transport` |
| `uc2_node` | `audit`, `backup`, `ipc`, `obs`, `preflight`, `recovery`, and everything in `node` except `NodeConfig`/`Node::start*` |
| `uc2_service` | `snapshots`, `ultima_db` |
| `uc2_gateway` | `config_file`; `Edge`/`EdgeStats` beyond what `gateway.toml` implies |
| `uc2_remote` | `conn`, `frame` as *Rust items* — the wire format they encode is promised (protocol v1), the Rust names are not |
| `ultima_journal` | `bench_support` (already `#[doc(hidden)]`), and the crate generally: it is the node's storage primitive, published so the workspace can be, not offered as a general-purpose journal |

Also outside the promise:

- **`#[doc(hidden)]` items**, wherever they appear.
- **Non-default features**, which exist for measurement and testing:
  `uc2_service/apply-profile` (rdtsc probes on the apply thread),
  `uc2_gateway/test-util` (`Edge::fault_for_tests`),
  `uc_protocol/uc-bench-probes`, `ultima_journal/bench-support`,
  `uc2_node/mutation-testing` and `uc2_consensus/mutation-testing`.
  `uc2_service/ultima_db` is a real adapter but its API tracks
  `ultima-db`'s, not this promise.
- **`uc2_sim`, `uc-lincheck`, `examples/counter`, `examples/uc2-crashtest`.**
  These are `publish = false`: the proof and teaching apparatus, not the
  product. Nothing in them is API, and they are not on crates.io.

## The wire and the cnc page are flag-day, not semver

Two version numbers are deliberately *outside* this policy, because semver's
"a minor bump is safe" contract is the wrong promise for them:

- **The node-to-node wire protocol** (`uc_protocol::version::CURRENT`,
  currently `0.6.0` — see [wire protocol](wire-protocol.md)).
- **The `cnc.dat` page layout** (`CNC_V2_VERSION` — see
  [the cnc control page](cnc-page.md)).

A change to either is a **flag day**: every node in a cluster is stopped and
restarted on the new version together. Mixed-version operation is not
supported and is not made safe by the version numbers agreeing on a major.
`v2.5.0`'s content-attested durable reports are the worked example — a
`0.4.0` peer's report reads as unattested and is not counted, so a mixed
cluster stalls commits rather than making unsound ones. The procedure is
[Upgrade a cluster](../how-to/upgrade-a-cluster.md); it applies whether or
not the crate version's major digit moved.

## The one-way door: one tier per type

`uc2_service` has two state-machine tiers, and a type implements **exactly
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
(`uc2_node/src/backup.rs`) stabilised in 1.89.0. `rust-toolchain.toml` pins
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

## Related

- [Cut a release](../how-to/cut-a-release.md) — the lockstep bump, the tag,
  and the manual crates.io publish order.
- [Upgrade a cluster](../how-to/upgrade-a-cluster.md) — what taking a new
  version costs an operator.
- `RELEASES.md` — what each version introduced, feature by feature.
