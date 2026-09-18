# Build an application on UltimaCluster

This tutorial walks the whole life of an application built on UC — from the
first design decision to a running three-node cluster you upgrade in place —
using one worked example: a replicated key-value store. By the end you will
have built a service, proven it linearizable, packaged it, deployed it, watched
it survive a leader kill, and upgraded it to a new version without losing a
write.

The worked example ships in the tree as [`examples/kv`](../../examples/kv), so
you can read every line referenced here, run its tests, and stand it up
yourself. Where a step has a full procedure of its own, this tutorial does the
step once and links to the how-to that covers every option; the goal here is
the *whole path*, not the depth of any one stop.

> **Before you start.** Read
> [What state machine replication is](../notes/state-machine-replication-explained.md)
> if the model is new to you, and skim the
> [SDLC standard for applications](../reference/application-sdlc.md) — this
> tutorial is that standard, walked end to end. You need the 2.12.0 release
> ([Quickstart §1](../QUICKSTART.md)) and a Rust toolchain (MSRV 1.89) to build
> the example.

## 1. Design: what the log makes you decide

An application on UC is a **state machine**: a deterministic function from a
command and the current state to a new state and a response. UC replicates the
*commands* — the ordered log — and every replica applies them independently, so
the design work is deciding what your commands are and proving that applying
them is deterministic.

For the KV store the commands are four operations — `put`, `get`, `delete`, and
`compare-and-set` — over `Bytes` keys and values. The state is an in-memory
map. Two design rules the log forces on you, both from the
[state machine contract](../reference/state-machine-contract.md):

- **`apply` is sync, deterministic, and does no I/O.** No clock, no RNG, no
  network — every replica must compute the same result from the same command at
  the same log position. If you need "now", it arrives on the apply context
  (`ctx.time_ns`, the leader's stamp carried on the frame); if you need an id,
  derive it from the position with `IdGen`. Reaching for `SystemTime::now()`
  inside `apply` is the classic divergence bug.
- **The command is the unit of ordering.** One command is one log frame and
  must fit one datagram (the [payload ceiling](../reference/limits.md)); design
  operations whose response is bounded. This is why the KV store has no `range`
  or `prefix` scan — an unbounded response cannot ride one frame.

See [`examples/kv/docs/DESIGN.md`](../../examples/kv/docs/DESIGN.md) for the KV
store's design note, and
[`examples/kv/WIRE-FORMAT.md`](../../examples/kv/WIRE-FORMAT.md) for the command
and image encodings it settled on.

## 2. Build: the two-tier SDK

A service implements one of two traits from `uc_service`
([write-a-service-binary](../how-to/write-a-service-binary.md) is the full
guide):

- `RawStateMachine` — bytes in, bytes out, the core contract; or
- the typed `StateMachine` — a sync `apply`/`query` over your own command and
  response types, which gets `RawStateMachine` for free via a blanket impl.

The KV store implements the typed tier, plus two capabilities: it is a
`SnapshotStateMachine` (so a node that has fallen below the purge floor can
install a snapshot instead of replaying from genesis), and it wraps itself in
`Sessioned` (so a retried write over a remote hop applies exactly once). Read
[`examples/kv/src/lib.rs`](../../examples/kv/src/lib.rs) alongside the
[state machine contract](../reference/state-machine-contract.md): the state
machine is the `apply`/`query` core, the snapshot is `build_snapshot` /
`install_snapshot` over the artifact's payload bytes (UC owns the 16-byte
`ULTSNAP1 ‖ P` envelope; the payload is entirely yours), and the service binary
`kv-service` is the thin `main` that attaches the state machine to a node.

Build the example:

```bash
cargo build --release -p kv_store   # produces kv-service, kv, kv-load
```

## 3. Test: prove it before you trust it

SMR applications fail in ways ordinary applications don't — silent divergence
after a leader election, replay corruption, a snapshot that doesn't round-trip.
The example carries the tests that catch each, and they are the tests *your*
application wants too:

- **State-machine invariants and an old-image fixture** (`tests/sm_invariants.rs`):
  the four operations, the two-shape v2 model, and a real v1 image
  (`tests/fixtures/v1-golden.kvimage`) installed into the v2 binary yielding the
  v1 golden digest — the compatibility guarantee, checked without any cluster.
- **A real three-node cluster** (`tests/cluster.rs`, behind `--features
  cluster-tests`): the operations end to end, a follower `SIGKILL` with a write
  during the outage and a read-back, a leader kill, a 40k-key load with a
  snapshot and purge and a below-floor restart, and the flag-day upgrade.

```bash
cargo test -p kv_store                       # the fast invariant + list tests
cargo test -p kv_store --features cluster-tests -- --nocapture   # the real cluster
```

For the platform's own linearizability proof of *your* state machine, the
`uc_lincheck` WGL checker and the Elle list-append tier are the tools; the KV
store's gate doc records both passing
(`docs/benchmarks/uc2-dogfood-kv-gate-2026-09-15.md`).

## 4. Package and deploy

An application ships as a **service binary** you install beside `uc2-node` on
every host. Package it the way `packaging/` packages the node: a binary in
`/usr/local/bin`, a per-host systemd unit that names your instance directory
and app id, bound to the node's lifecycle. Then stand up a cluster —
[run a cluster](../how-to/run-a-cluster.md) is the full procedure; the short of
it is one `node.toml` per host declaring the members and the
`[services] names` your FSM answers to, `uc2-node` started under systemd on
each, and one `kv-service` process per declared name.

Make it reachable to remote clients with a [gateway](../how-to/run-a-gateway.md)
on each node, and talk to it with the `kv` CLI:

```bash
kv --gateways host0:9200,host1:9200,host2:9200 put greeting "hello"
kv --gateways host0:9200,host1:9200,host2:9200 get --linearizable greeting
```

A value written through one gateway reads back through any other — that is
replication working.

## 5. Operate

A deployed cluster is a thing you watch and reshape:

- **Monitor it.** [Stand up Prometheus and Grafana](../how-to/monitor-a-cluster.md),
  load the shipped alert rules, and import the dashboard. Read that page's
  alert table with the operator report's caveat in mind
  ([operator report](../notes/uc2-dogfood-kv-operator-report.md)): several
  degraded-but-quorate states are quiet on a lightly-loaded cluster, so a
  liveness check (is every voter still being heard?) belongs beside the shipped
  rules.
- **Reshape membership under load.**
  [Add a learner, promote it, remove a voter](../how-to/change-cluster-membership.md) —
  one change at a time, while the cluster keeps serving.
- **Survive a lost node.** [Back up an instance directory, verify it, and
  restore it](../how-to/back-up-a-cluster.md) onto a fresh node that rejoins
  and catches up from the log.

Each of these was exercised, blind, in the operator dogfood; the
[operator report](../notes/uc2-dogfood-kv-operator-report.md) is the honest
account of where the docs helped and where they didn't.

## 6. Upgrade

Eventually your application changes. The KV store's v2 adds list-valued keys
(`append`, `list`) and moves `KV_VERSION` to `2.0.0`. At 2.12.0 an application
upgrade is a **flag day** — you stop every service, install the new binary, and
start every service — because a mixed-version cluster can commit a command the
old replicas cannot apply, and a failover to an old leader would then lose an
acknowledged write. That hazard, and the exact procedure, are the subject of
the next how-to:

> **[Upgrade an application](../how-to/upgrade-an-application.md)** — the
> flag-day procedure as it actually is at 2.12.0, what to back up first (the v2
> service rewrites its rollback artifact in place, so an off-node copy is the
> only safe rollback), and why the rolling upgrade is not here yet.

Run it against the example and you close the loop: a value acknowledged under
v1 reads back under v2, and every replica agrees on the store's contents. That
is the whole life of an application on UC — designed against the log, built on
the two-tier SDK, proven with the capstones, deployed on bare hosts, operated
through faults and reconfiguration, and upgraded in place.

## Where to go next

- The reference for each stop: [state machine contract](../reference/state-machine-contract.md),
  [configuration](../reference/configuration.md), [limits](../reference/limits.md),
  [instance directory](../reference/instance-directory.md),
  [semver policy](../reference/semver-policy.md).
- The two dogfood experience reports —
  [builder](../notes/uc2-dogfood-kv-builder-report.md) and
  [operator](../notes/uc2-dogfood-kv-operator-report.md) — for the honest
  account of building and running this very example from the docs alone.
