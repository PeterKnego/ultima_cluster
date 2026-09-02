# UC v2 — FSM identity: name the state machine, keep the row

**Date:** 2026-09-02 (amended the same day: cut from "placement-independent
rows" to **named rows** after the §2.1 comparison; per-FSM version added
in §7; **`ApplyCtx` introduced in §3.3** — carried back from the
timestamps/scheduler design running in parallel, so the apply signature is
rewritten once, not twice)
**Status:** approved design (brainstorm 2026-09-01/02; five sections
approved in turn; the cut and the version decision approved 2026-09-02;
Aeron comparison read from source). Next: the implementation plan.
**Baseline:** `main` f0e1709 (`v2.10.0`; wire 0.6.0; cnc 3.0; remote
protocol v1; M14 multi-service as shipped — `[services] ids`, slot-numbered
FSMs, `docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md`).
**Backlog line:** `docs/BACKLOG.md` item 2.

## 1. Goal and locked decisions

M14 identifies an FSM by its **slot number** (`service_id: u8`, `0..8`):
the service declares it in `ServiceConfig`, the node declares the set in
`[services] ids`, and nothing anywhere states *what logic a number holds*.
Two nodes agree on the *set of numbers* (checked on the snapshot path) and
never on the logic behind each. Same logic at different numbers stalls
every joiner (declared-set mismatch, unexplained); **different logic at
the same number diverges silently** — no refusal, no alert, two replicas
of "FSM 1" applying different code to the same log.

This spec gives an FSM an identity that is a property of its business
logic — declared in code, the same wherever the binary attaches — and
binds it to the row: the node's config names each row, a service finds its
row by name, and a cluster whose rows carry different names refuses to
exchange snapshots **by name**. The row itself stays what it is today: a
cluster-wide index, required to match on every node. It is the per-FSM
analog of `app_id`.

| decision | choice | why (§) |
|---|---|---|
| where identity lives | **in code**: a required `const NAME: &'static str` on the state-machine trait; `ServiceConfig` loses `.service_id()` | §2, §3.3 |
| node config | `[services] names = ["kv", "orders"]` — **index = row**; the section becomes required | §4.1 |
| the row | **unchanged in meaning**: a cluster-wide index; a service no longer states it (found by name in the cnc page); rows **must match** across nodes and the check now says which name differs | §2.1, §5 |
| cluster-wide check | `SNAP_BEGIN` carries the identity hashes **in row order** (wire 0.7.0), replacing the bitmask; compared positionally | §5 |
| per-FSM version | `const VERSION: u32` (packed semver) beside `NAME`; attach-written to the cnc slot, carried per row on `SNAP_BEGIN`, equality-checked and exported; compatibility semantics stay with backlog item 3 | §7 |
| what is keyed by name | refusals, `uc2ctl status`, metric labels, log records. **Disk and rings stay keyed by row** | §4.4, §4.5 |
| client | `fsm("orders") -> u8` convenience; the row-taking API stays | §6 |
| apply signature | `apply(&mut self, ctx: &mut ApplyCtx, cmd, out)` — `ApplyCtx { position }` now; the parallel time/scheduler design adds its fields later without touching the signature again | §3.3 |
| first consumer | `uc_service::ids::IdGen` via `ctx.ids()` — deterministic, stateless, placement-independent IDs from `(position, identity, ordinal-within-apply)`; unreachable from `query` by construction | §3.4 |
| flag day | one combined: cnc 3.0 → 3.1 and wire 0.6.0 → 0.7.0 | §4.2, §5 |
| deliberately not done | placement-independent rows (hash-routed artifacts, name-keyed disk, client handles, a named default) | §2.1, §11 |

### What does not change

Consensus, elections, the commit pipeline, the frame layout, crypto
framing, the ingress/egress rings, the M13 client `Engine` internals, the
remote protocol (v1; the gateway keeps targeting row 0), the snapshot
artifact's bytes and its routing (by row, as today), every on-disk name,
and the lag policy. The Lean model, conformance vectors and loom models are
untouched: no consensus-relevant state, ring or frame moved.

## 2. Why this shape, and what Aeron does

**Identity in code, not config.** The maintainer's framing (2026-09-01):
"FSM represents business logic and should generate same deterministic IDs
across nodes. FSM on node1 in slot1 should create same ID as same FSM on
node2 slotX." A name in deployment config can be mis-set per host; a name
on the type ships with the logic and cannot be. `ServiceConfig` therefore
loses its `.service_id()` setter rather than gaining a `.name()` beside it.

**Build on `service_id`, don't replace it.** Also the maintainer's words
("service_name or service_id is same thing; if we already have service_id
we can build on it"). The first draft of this spec went further — rows
free to differ across nodes, artifacts routed by hash, disk and client
keyed by name — and §2.1 is the comparison that cut it back. IDs are
placement-independent in both designs because `IdGen` never sees the row;
the freedom to *order* rows differently per node was a derived goal, not a
requirement, and UC already demands identical lag policy, crypto mode and
declared set on every node.

### 2.1 Three designs compared — and the cut

| aspect | UC today (M14) | Aeron Cluster | **named rows (this spec)** | placement-independent rows (first draft, cut) |
|---|---|---|---|---|
| identity declared | operator: `service_id` u8 in `ServiceConfig` | operator: `serviceId` int per container | **code: `const NAME`** | code: `const NAME` |
| node config | `ids = [0, 1]` | `serviceCount` | **`names = [...]`, index = row** | `names = [...]`, order free |
| service states its row | yes | yes | **no — found by name** | no |
| rows agree across nodes | by convention, unchecked | by convention, unchecked | **required, checked by name** | not required |
| cross-node check | bitmask on `SNAP_BEGIN` | none | **hashes in row order** | hash set, order ignored, + default |
| when the check fires | snapshot sessions | never | snapshot sessions (+ alerting on exported hashes) | snapshot sessions |
| wrong logic at same row | silent divergence | silent divergence | **refused by name** | refused by name |
| artifact routing | by row | by id | **by row, unchanged** | by hash to the local row |
| on-disk names | `<id>` | `<id>` | **`<id>`, unchanged** | `<name>` everywhere |
| client API | `submit_to(u8)` | session-based | **+ `fsm("name") -> u8`** | handles, `submit_to(&handle)` |
| remote default | row 0 | none | **row 0, its name checked** | named `default`, page-1 field |
| per-FSM version | none | one cluster-wide `appVersion` in the log | **`const VERSION`, equality-checked** (§7) | reserved bytes only |
| deterministic IDs | none | none (inputs only) | **`IdGen` from `NAME`** | `IdGen` from `NAME` |
| flag day | — | — | cnc + wire, one release | cnc + wire, one release |
| size | shipped | shipped | **small milestone** | M14c-sized |

- **UC today.** Zero cost; but the failure the maintainer named (same
  logic, different numbers) stalls joiners without saying why, and the
  worse one (different logic, same number) is silent.
- **Aeron.** Simplest possible; checks nothing; its `serviceName` is a
  label. UC already exceeds it with the bitmask. Not a target to copy.
- **Named rows.** Everything actually asked for: identity ships with the
  code, a mis-attached service is refused by name at the door, a mismatched
  cluster is refused by name, IDs are placement-independent. Disk, artifact
  routing, client engine and gateway untouched; the wire change is a
  bitmask → ordered hash array swap. Cost: operators list names in the
  same order on every node — no new discipline given the fields UC already
  requires to match.
- **Placement-independent rows.** Buys order freedom, names on disk, and
  restores-by-name; costs hash-routed artifacts, renamed rings and
  directories, a handle API, a default-row field and a much larger test
  surface — for a freedom nobody requested. Names on disk and client
  handles can follow later **without a flag day** (neither touches the
  wire); that is the door left open in §11.

**One caveat that applies to every column with a check:** `SNAP_BEGIN`
fires only on snapshot sessions (learner join, below-floor node), so the
cross-node guard is *late* in every design here, exactly as the declared-
set check is today (M14 spec: "checked where it can be"). The **early**
guards are local — attach refusal against the node's own config — and
operational: the hashes and versions are exported per row so a cross-node
alert compares them in steady state (§4.5).

**What Aeron Cluster does** (read 2026-09-02 from
`~/ultima/aeron/aeron-cluster/src/main/java/io/aeron/cluster/` and the Go
port `~/ultima/aeron-go/cluster/`):

- `serviceId` is a dense index in `[0, serviceCount)`; each container is
  configured with its id, the consensus module with the count, and the only
  check is a range check against the count (`ConsensusModuleAgent`,
  "serviceId=… invalid for serviceCount=…"). Snapshots are recorded and
  replicated per id (`RecordingLog.Entry.serviceId`), the mark file is
  `cluster-mark-service-<id>.dat`, and the per-service ack queues sized by
  count are the all-services barrier (UC's lag floor analog). **Nothing
  cross-checks id against logic between nodes**; the count never travels.
  M14 is a faithful copy; named rows go one step past it.
- `serviceName` (`aeron.cluster.service.name`, default
  `"clustered-service"`) is **cosmetic**: it feeds `Agent.roleName()` and
  is written into the mark-file header for tooling. Never validated, never
  on the wire, never in the log. That is why this spec does not stop at a
  label.
- There is **no default responder**: any service may respond on any client
  session. UC's row-0 default is UC-specific (the remote path reaches one
  FSM).
- **Deterministic IDs:** Aeron ships no generator. Its replicated inputs
  are the cluster session id (assigned by the consensus module, carried in
  `SessionOpenEvent`, `nextSessionId` snapshotted), the leader-stamped
  timestamp on every log message, the log position and the term id;
  services derive from those. Same principle as §3.4 — randomness is
  *input*, never generated in apply — with leader-stamped time as the one
  input UC's frame does not carry.
- **Version:** one cluster-wide `appVersion` (packed semantic version),
  configured on the consensus module *and* every service container
  (`ctx.appVersion()`); the leader stamps it into the log in every
  `NewLeadershipTermEvent` and into every snapshot marker; every module and
  service validates `ctx.appVersion()` against the log/snapshot value on
  each new-term event and on snapshot load through a pluggable
  `VersionValidator` (default: major-equality), fail-stop on mismatch. §7
  takes the *static* half of that (the per-binary `ctx.appVersion`) now
  and leaves the log-stamped half to backlog item 3.

## 3. The identity (`uc_protocol`) and the SDK surface (`uc_service`)

### 3.1 Name rules

A new leaf module, `uc_protocol::identity`, `core`-only like `version` and
`magic`. A name is **1..=32 bytes** of lowercase ASCII letters, digits, `_`
and `-`, and starts with a letter. The bound is what lets the name sit
verbatim in one cnc slot line; the alphabet keeps it a valid metric label
value and (should §11's door ever open) a path component on every platform.

### 3.2 Hash

`fsm_hash(name) -> u64`: **FNV-1a 64** over the name's bytes, a `const fn`.
A **frozen constant** in the same class as the wire constants: a
golden-vector test pins it, and a change is a flag day (a replica on a
different SDK build must mint the same IDs and match the same `SNAP_BEGIN`
array). The hash goes on the wire, into the cnc status line and into
`IdGen`; the name goes into the cnc name line, labels and refusals.

`FsmIdentity { name: &'static str, hash: u64, version: u32 }`, constructed
by `FsmIdentity::parse(name, version)` — a `const fn` that **panics on an
invalid name**, so a bad name is a compile-time error at the first use of
the provided const below, not a runtime refusal.

### 3.3 Trait change

```rust
/// Everything the framework knows about the committed frame being applied.
/// Built by the apply loop (and by journal replay / snapshot tail-replay)
/// once per frame; a state machine never constructs one on the live path.
/// `#[non_exhaustive]`: the timestamps/scheduler design adds `time_ns`,
/// `term` and schedule/cancel here without changing `apply`'s signature.
#[non_exhaustive]
pub struct ApplyCtx {
    /// The frame's absolute byte position (the idempotency key).
    pub position: u64,
    identity: FsmIdentity,
}
impl ApplyCtx {
    pub fn new(position: u64, identity: FsmIdentity) -> Self { Self { position, identity } }
    /// For a state machine's own unit tests: `ApplyCtx::for_sm::<MySm>(pos)`
    /// (built on `new`, filling in `MySm::IDENTITY` for you).
    pub fn for_sm<S: RawStateMachine>(position: u64) -> Self { Self::new(position, S::IDENTITY) }
    /// The deterministic ID generator for THIS apply call (§3.4).
    pub fn ids(&self) -> IdGen { IdGen::new(self.position, self.identity) }
}

pub trait RawStateMachine: Send + 'static {
    /// The FSM's identity — the same wherever this type attaches.
    const NAME: &'static str;
    /// Packed semantic version of this FSM's logic (§7). 0 = unversioned.
    const VERSION: u32 = 0;
    /// Provided; evaluated (and validated) at first use.
    const IDENTITY: FsmIdentity = FsmIdentity::parse(Self::NAME, Self::VERSION);
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>);
    fn query(&self, q: &[u8], out: &mut Vec<u8>);
    fn last_applied(&self) -> Option<u64>;
}
```

**Why a context, and why now.** The parallel timestamps/scheduler design
(leader-stamped `time_ns`, term, schedule/cancel — its own spec and wire
release, nothing here depends on it) needs the same signature change one
release later. This plan already rewrites every `apply` impl, so the
context lands here and that work adds fields to a `#[non_exhaustive]`
struct instead of breaking the trait a second time. Two benefits inside
this spec: §3.4's "build the generator from the position given to you" is
*enforced* — there is no other position to pass — and "never call it from
`query`" gets a type-level backstop, since `query` receives no context.
The context carries the identity **by value** (`FsmIdentity` is `Copy`,
three words) so `ctx.ids()` needs no turbofish; the apply loop builds it
from `S::IDENTITY`. The reference is **`&mut`**, not `&` (time-planning
review, 2026-09-02): the scheduler's FSM-facing calls, `ctx.schedule(..)`
and `ctx.cancel(..)`, push requests the apply loop drains after the call;
a shared reference would force interior mutability into the context on the
hot apply path — exactly the body cost the M14a apply-hop lesson warns
about — where a mutable reference lets them land in a plain `Vec`. Nothing
in this spec needs the mutability, but changing it later would re-touch
every call site this work already edits. `Sessioned` still passes the
context straight through: a mutable reference reborrows. `ApplyCtx` is
therefore not `Copy`.

The typed `StateMachine` mirrors it: `apply(&mut self, ctx: &mut ApplyCtx,
cmd: Self::Command) -> Self::Response`, `NAME` required, `VERSION`
provided; the blanket impl forwards both consts and passes `ctx` through.
`Sessioned<S>` forwards `S::NAME` and `S::VERSION` and passes `ctx` to the
inner apply unchanged (it reads `ctx.position` where it read `position`).
No `dyn` use of either trait exists in the tree (checked 2026-09-02), so
associated consts are safe. Every state machine in the tree (≈ 24 impls:
`uc_lincheck`'s `RegisterSm`/`ListAppendSm`, the counter example, the gate
examples, the test SMs) gains one `NAME` line and swaps `position: u64`
for `ctx: &mut ApplyCtx` (reading `ctx.position`), mechanically.

### 3.4 `IdGen` — deterministic IDs

`uc_service::ids::IdGen`: a value type, no heap, no state beyond
`(position, identity, ordinal: u32)`.

```rust
fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Cmd) -> Resp {
    let mut ids = ctx.ids();     // one generator per apply call, by construction
    let order_id = ids.next();   // u128
    let line_id  = ids.next();   // u128, no visible relation to order_id
    ...
}
```

**Input (128 bits):** `position: u64 ‖ ordinal: u32 ‖ fold32(identity.hash)`
where `fold32(h) = (h >> 32) as u32 ^ h as u32`. The version is **not** an
input: an upgrade must not change the IDs a replay mints.
**Permutation:** a **three-round Feistel** over the two 64-bit halves with a
standard 64-bit finalizer (murmur3 `fmix64`) as the round function. A
Feistel network is a bijection for *any* round function, so uniqueness
within one FSM is by injectivity, not by birthday odds; three rounds
suffice for consecutive ordinals to share no visible structure. The
permutation is **frozen** like the hash, with golden vectors and an
inverse-round-trip test.

**Why per-apply scoping is the whole correctness story.** The ordinal is
*per apply call*, never per process lifetime. A lifetime counter diverges
three ways: a snapshot-installed replica has called it 0 times where a
journal-replayed one has called it N; a read that calls it advances only
the replica that served the read; a restarted service recounts from its
last applied position. Per-apply reset makes the series a pure function of
committed input — nothing to snapshot, replay-safe by construction, and
identical on a node that installed a snapshot and one that replayed the
journal. The row is not an input either, which is what makes the IDs
placement-independent regardless of §2.1's cut.

**Documented rules (SDK docs + the explainer):**

- Build the generator from `ctx` inside apply. Both halves of the old
  rule are now structural: the only position available is the frame's
  (via `ctx`), and `query` receives no context, so it cannot mint. A
  state machine that stashes a `ctx.ids()` generator in `self` and uses
  it later reintroduces the lifetime-counter divergence above; the docs
  say so, and `IdGen` is `!Send` so the obvious stash into shared state
  fails to compile.
- Under `Sessioned`, a replayed command returns the cached response, so the
  IDs a client sees are stable across retries. Without it, a retry is a new
  position and a new ID — the ordinary at-least-once duplicate, not
  something the generator can fix.
- Two FSMs on one log mint disjoint series because their folds differ; a
  32-bit fold collision between two names in one deployment is the user's
  to rule out with a one-line test (the fold is public).
- Renaming an FSM changes the series it mints from then on and never
  collides with what it minted before (the input differs).
- IDs are unique within one cluster (one `app_id`), not across clusters.
- Wall-clock-sortable IDs (Snowflake, ULID) are *not* provided: the FSM has
  no clock, and a leader-stamped time would be a frame-header change. The
  position already gives strict, global, time-correlated order; Snowflake
  proper belongs at the client edge, where the client is a worker with a
  clock, and the FSM stores what arrives.

**One type, one row.** Identity is per type, so a harness that runs one
state machine type at several rows (apply_bench, the two-FSM lincheck
capstones, m12_gate's fleet rows) wraps it in `uc_service::Tagged<const
ROW: u8, S>`, a zero-cost forwarding newtype whose `NAME` is `fsm{ROW}`.
`ServicesConfig::tagged(n)` declares `fsm0..fsm{n-1}`. A production
deployment never needs it — two rows running the same logic on one log
compute the same state twice.

## 4. Node (`uc_node`): config, boot, cnc, attach, observability

### 4.1 Config

```toml
[services]
names   = ["kv", "orders"]   # index = row; 1..=8, valid names, no duplicates
fsm_lag = "16MiB"            # unchanged
```

`ServicesConfig` becomes `{ names: [FsmIdentity-without-version; ≤8] (in
list order = row order), fsm_lag }`. Refusals, each naming the field in the
M9 style: empty list, invalid name (with the rule quoted), duplicate, more
than 8. **`ids` is refused by field name** with a pointer to `names` — no
deployments exist, so no shim. The shared CLI form becomes
`--services kv,orders`.

**`[services]` becomes required** in `node.toml` — a file without it
refuses to start by name, the same explicit-choice rule `[crypto]` and
`[admin]` have had since 2.6.0. The alternative (absent ⇒ one FSM with a
magic name) would force every single-FSM state machine to carry that name
to attach anywhere — identity by config through the back door.
Programmatic configs (`NodeConfig::services`, the test harnesses) name
their FSMs explicitly; `ServicesConfig::default()` goes away and only
`none_for_tests()` remains.

Rows are **contiguous from 0** (the row is the list index), so
`services_declared` loses its holes; row 0 remains the default responder
the remote path reaches, and its *name* is now part of what the cluster-
wide check compares. The consensus agent's floor computation over declared
rows is unchanged.

`ServicesConfig::single(name)` is the one-FSM programmatic form; `from_names(&[..], lag)` the general one; `from_cli` requires `--services` (absent is a refusal, as the section is).

### 4.2 Boot and the cnc page (cnc 3.0 → 3.1)

The node already zeroes the slot band at every start. Additions:

| where | field | width | writer |
|---|---|---|---|
| slot `+448..+480` (line 7) | `name`, NUL-padded | 32 B | **node, in `CncPage::init`** (before the header CRC) |
| slot `+480..+488` (line 7) | `identity_hash` u64 | 8 B | node, in `CncPage::init` |
| slot `+488..+512` | reserved (zero) | 24 B | — |
| slot `+8` (status line 0) | `version` u32 (§7), stored as a u64 word | 8 B | service, at attach (same line and writer as `status`) |

Line 7 was "reserved, written by nobody"; it becomes node-written at boot,
which keeps the one-writer-per-line rule (the M14 spec §3.4 named line 7
as the place for per-slot additions). No page-1 change. Offsets are pinned
in **both** `uc_protocol` and `uc_log` with the usual offset-assertion
tests. `CNC_V2_VERSION` becomes `3.1` — by `docs/reference/semver-policy.md`
a cnc layout change is a **flag day** regardless of the digit; §10 bundles
it with the wire change.

### 4.3 Attach (`uc_service`)

`ServiceConfig` drops `service_id`. Attach validates the page
(magic/crc/version/`app_id`) and captures `instance_id` as today, then
**scans the eight name lines for `S::NAME`**:

- not found → `ServiceError::UnknownFsm { name, declared: Vec<String> }`,
  a named refusal listing what the node declares;
- found → the row; the rest of attach runs unchanged with the row where
  `service_id` was. The internal `service_id: u8` field **stays as the
  row** (the "build on it" decision) and is `pub(crate)`.

Attach writes `identity_hash` and `version` into the slot's status line.
The per-FSM lock, rings, snapshot directory and progress file are opened by
row exactly as today.

### 4.4 On disk — unchanged

`snapshots/<id>/`, `service.<id>.lock`, `egress_service.<id>.broadcast`,
`svc_query.<id>.ring`, `state/output_progress.<id>.state` all stay keyed by
row. Backup/verify/restore are untouched. Keying them by name is §11's
open door: it touches no wire and can be done without a flag day if a
reason appears.

### 4.5 Observability

- Metric labels gain the name: `service="orders",row="1"` (the row label
  keeps existing dashboards' grouping keys usable). Two new gauges per
  row, `uc2_service_identity_hash` and `uc2_service_version`, exported
  from the slot band so a cross-node alert rule can compare them in steady
  state — the **early** guard for the late `SNAP_BEGIN` check (§2.1).
  The alert-rule file gets that rule; the dashboard is swept.
- `uc2ctl status` prints `row=1 name=orders version=1.2.0 hash=…
  attached=… applied=…` per row.
- `obs_event!` records that carry a service id carry the name as well.

## 5. Wire 0.7.0 — `SNAP_BEGIN` and the cluster-wide check

**The only wire carrier of FSM identity is the snapshot-session opener**
(M14 spec: commands are broadcast; durable reports are aggregates;
`uc_net/src/sender.rs` — one artifact per declared FSM, plus the declared
mask the receiver compares). So the wire change is confined to one body.

`SnapBeginBody` 0.7.0 (fixed length; `SNAP_BEGIN_FIXED_LEN` grows from 34):

| field | 0.6.0 | 0.7.0 |
|---|---|---|
| `session: u32`, `layout: u8`, `service_id: u8` (the artifact's row), `snapshot_pos: u64`, `config_len` + config | as today | as today; `layout = 2` |
| `services_declared: u64` (bitmask) | | **`identity: [u64; 8]`** — hash per row, in row order, `0` = row undeclared (the mask is derived) |
| — | | **`version: [u32; 8]`** — per row, from the slot band; `0` = no service attached / unversioned (§7) |

Hashes rather than names: the receiver already knows its own names and only
needs to match, and a fixed body keeps the decoder bounds-checked the way
the 0.6.0 one is. `uc_protocol::version::CURRENT` → `0.7.0`. A 0.6.0
sender's body is shorter than the new fixed length, so the receiver drops
it by the same length check that drops 0.5.0 today: **a mixed cluster
stalls a joiner rather than installing a wrong artifact** — the same
failure mode as the last two wire changes, and the standing flag-day rule.

**The receiver's check, positional.** For each row `r`: `identity[r]` must
equal the receiver's own hash for row `r` (both zero = both undeclared).
On mismatch the session is refused **by name**: "row 1: ours=orders,
theirs=kv" (a sender hash the receiver knows anywhere in its own list is
printed as that name; an unknown one as its hash). This subsumes today's
declared-set check (a set difference is a positional difference) and
`uc2_snapshot_refused_declared_set_total` keeps its meaning. Versions are
compared per row only when **both sides are non-zero** (§7); a mismatch is
refused by name with both versions and counted in a new
`uc2_snapshot_refused_version_total`. Artifacts route by row, as today.
`SNAP_DONE` echoes the same `SnapBeginBody` (`uc_net/src/receiver.rs`,
`snap_send_done`), so it carries the 0.7.0 layout with no separate change.
The receiver learns its own per-row versions through a closure the node
installs with `set_snapshot_intake` (the services write them into the cnc
page at attach; `uc_net` has no cnc dependency).

The `SNAP_BEGIN` fuzz target moves to the new layout;
`docs/reference/wire-protocol.md` and `docs/how-to/upgrade-a-cluster.md`
get the 0.7.0 entry.

## 6. Client (`uc_client`) and gateway (`uc_gateway`)

**Local client.** The engine already opens the cnc page at attach; it now
reads the eight name lines once. Two additions, nothing removed:

```rust
let orders: u8 = client.fsm("orders")?;     // ClientError::UnknownFsm { name, declared } otherwise
let names: &[FsmName] = client.declared_names();
let r: R = client.submit_to(orders, &cmd)?;  // unchanged API
```

The `query.ring` payload stays row-prefixed; `MSG_V2_BAD_SERVICE` carries a
row and the client adds the name to the error where it can. The `PollHalf`
fan-in buffer and the all-atomic `Slot` are untouched.

**Gateway.** Unchanged: it targets row 0, whose name is now checked
cluster-wide by §5, so two gateways cannot answer remote clients from two
different FSMs without the cluster first refusing to exchange snapshots and
alerting on the exported hashes. Name-addressed remote routing remains the
remote-v2 question backlog item 1 owns.

**Harnesses.** Every gate/bench binary that takes `--services 0,1` takes
names through the shared parser (§4.1); `m14_fleet_gate.py`, `hop_bench`,
`apply_bench` and the two-FSM capstones change their arguments and nothing
else.

## 7. Per-FSM version — the static half of Aeron's mechanism

**Decision (2026-09-02):** each state machine declares
`const VERSION: u32` beside `NAME`, provided with default `0` =
unversioned, packed `major:8 ‖ minor:8 ‖ patch:16` — UC's own
`ProtocolVersion` layout (Aeron's `SemanticVersion` is 8/8/8; both put the
major in the high bits, so the packed integer compares in semver order) —
so a future validator can do major-equality or a floor without a
re-encoding. The maintainer asked for a per-FSM designator (UC's FSMs are
separate binaries, deployed separately), so this is **per FSM**, not
per-application as Aeron's is.

**What it does now:**

- written by the service into its slot's status line at attach (§4.2), so
  `uc2ctl status` and `/metrics` answer "which version of `orders` is
  attached on each node" (§4.5) — the operational question this exists for;
- carried per row on `SNAP_BEGIN` (§5) and **equality-checked** where both
  sides report a non-zero value; a mismatch refuses the session by name
  with both versions. `0` on either side means *unknown*, not *mismatch*:
  a joiner's services may not have attached yet when it receives its first
  snapshot session, and an unversioned FSM must keep working.

**What it deliberately does not do:** define compatibility. Equality is
the only relation; there is no validator, no floor, and the version is not
stamped into the log. Aeron's mechanism has two halves — the static
`ctx.appVersion` on every module and container, and the leader-stamped
copy in every `NewLeadershipTermEvent` and snapshot marker, validated
against each other with a pluggable `VersionValidator` (default
major-equality, fail-stop). This section ships the static half; the
log-stamped half, the validator, and the rolling-upgrade semantics that
follow from them are **backlog item 3**, which now has a concrete
comparator and a field to build on rather than bytes reserved for one.
The carrier for that half is a term-boundary log event, not `SNAP_BEGIN`.

**Determinism note:** `VERSION` is not an `IdGen` input (§3.4); an upgrade
must not change what a replay mints. Whether two versions apply a given
command identically remains the user's determinism obligation — the
version makes a mixed deployment *visible* and, on the snapshot path,
*refused*; it does not make one safe.

## 8. Failure modes

| situation | outcome |
|---|---|
| service binary's `NAME` not in the node's list | attach refused by name, listing the declared names; the node runs on (the row stays unattached, as today for a never-attached id) |
| invalid `NAME` in code | compile-time error at first use of `IDENTITY` |
| two nodes list different names, or the same names in a different order | every snapshot session between them refused by name, per row ("row 1: ours=orders, theirs=kv"); joiner stalls; counter + alert. Steady state: the exported hashes differ → the cross-node alert rule fires |
| two nodes attach different `VERSION`s of the same FSM | snapshot sessions between them refused by name with both versions; steady state: `uc2_service_version` differs → alert. Live divergence is **not** prevented (§7) |
| a 0.6.0 node in a 0.7.0 cluster (or vice versa) | its `SNAP_BEGIN` is dropped by length; joiner stalls; flag day |
| a cnc 3.0 service SDK against a 3.1 node | refused by cnc version at attach (existing gate) |
| `node.toml` without `[services]` | named startup refusal (as `[crypto]`/`[admin]`) |
| `IdGen` used in `query` | not detectable by the SDK; documented as forbidden (§3.4) |
| renaming an FSM | a per-FSM flag day for config on every node (disk is by row, so no directory moves); same trade as `app_id` |

## 9. Test plan and acceptance

**Unit tier** (each test written first and watched red; the plan records
which fix each one was reverted against):

- `uc_protocol`: name-rule table (accept/reject with the reason), FNV
  golden vectors, the slot name line / hash / version words pinned by
  offset assertions in both `uc_protocol` and `uc_log`, `SnapBeginBody`
  0.7.0 round trip + a 0.6.0-length body dropped, packed-semver
  encode/decode, the fuzz target on the new layout.
- `uc_service`: `IdGen` golden vectors; inverse round trip proving the
  permutation is a bijection (sampled + an exhaustive small-domain case);
  disjoint series for two identities; `VERSION` not an input; `Sessioned`
  forwards `NAME`/`VERSION` and passes `ctx` through unchanged (its own
  `max_pos_seen` still advances from `ctx.position`); the apply loop,
  journal replay and snapshot tail-replay each build `ApplyCtx` with the
  frame's position (one assertion per path); the unknown-name attach
  refusal; attach writes
  hash + version; the raw-contract and session suites updated for `NAME`.
- `uc_node`: every `[services]` refusal by field name (`ids` and the
  missing section included); boot writes names before publish; positional
  identity mismatch refused by name with the "row r: ours/theirs" message;
  version mismatch refused only when both non-zero; metric labels and the
  two new gauges; `uc2ctl status` output.
- `uc_client`: `fsm()` resolution, `UnknownFsm`, `declared_names()`.

**Capstones.** The two-FSM `lin_v2` scenarios, the hard-crash two-FSM
scenario (`uc_crashtest`) and Elle's `quiet_two_fsm` pass all run with
named FSMs, including a learner join / below-floor snapshot install.
Adjudicated by the untouched WGL checker. One negative scenario in the
`uc_node` services suite: a joiner whose names are in the other order is
refused by name and stalls, and the refusal counter and exported hashes say
so. Loom, Lean and conformance: unchanged, re-run as regression only.

**Fleet gate** (`docs/benchmarks/uc2-fsm-identity-gate-<date>.md`, bars
pre-committed before the run, honest-failure protocol): the M14 gate's
rows with names substituted (steady-window, `m14_fleet_gate.py`). Consensus,
the log frame, the rings and the client `Engine` are untouched, but the
apply loop is not: it now builds a 48-byte `ApplyCtx` per frame
(`uc_service/src/apply.rs`). The expected delta is null only if that
construction inlines away — M14a found that code added to a hot loop's body
can cost even on paths that never execute it — so the run must include an
`apply_bench` A/B (pre-identity vs. this branch) using the harness's
measured build-to-build resolution (`scripts/hop1_ab.sh`'s same-source
rebuild control) before treating "null" as established, not a hoped-for
number asserted from this section's prose alone.

## 10. Release and docs

- **Version:** the trait gains a required const and `ServiceConfig` loses a
  setter — a **breaking API change** under `docs/reference/semver-policy.md`,
  on top of the cnc + wire flag day. **Open decision for the maintainer:**
  ship as a major, or ride the no-external-users allowance as the next
  minor. The spec does not choose.
- The standing writeup rule: a section atop `RELEASES.md`; the
  `docs/releases.md` entry; QUICKSTART / how-to / reference sweep;
  `docs/ops/uc2-runbook.md` (cnc decode: the name line, hash, version);
  `docs/reference/{wire-protocol,cnc-page,semver-policy}.md`;
  `docs/how-to/upgrade-a-cluster.md` (0.7.0 + cnc 3.1); the M14 spec gets
  an as-built erratum pointing here; `docs/BACKLOG.md` item 2 → this spec,
  item 3 → §7.
- One plain-language explainer,
  `docs/notes/uc2-fsm-identity-and-deterministic-ids-explained.md`,
  covering: why identity is in code, why the row stays, what the version
  does and does not promise, why `IdGen` is per-apply, the retry and
  cross-FSM rules, what Aeron does, and the §2.1 comparison.

## 11. Out of scope and doors left open (recorded so they are not re-derived)

- **Placement-independent rows** — cut by §2.1. Its pieces that touch no
  wire (name-keyed disk and rings, client handles) can be added later
  without a flag day; the piece that does (hash-routed artifacts, a named
  default) needs a new argument.
- Name-addressed **remote** routing — remote protocol v2, backlog item 1.
- **Version compatibility semantics** (validator, floor, log-stamped
  version, activation) — backlog item 3, building on §7's field.
- **Code-hash verification** of an FSM. A name and a version say two
  replicas *intend* to be the same FSM at the same logic; they do not
  verify it. That is the user's determinism obligation, stated in the
  explainer.
- Leader-stamped timestamps and a deterministic scheduler are being
  designed separately and will follow as their own wire release; nothing
  here depends on them. They add fields to `ApplyCtx` (§3.3), not a new
  signature. (A Snowflake-style ID would take its time input from there.)
- Stage-2 multi-log (M14 spec §10) — unaffected; a name is per log.

## 12. Implementation order (for the plan)

1. `uc_protocol`: `identity` module (rules, FNV, packed semver,
   `FsmIdentity`), cnc 3.1 offsets + `uc_log` pins, `SnapBeginBody` 0.7.0 +
   fuzz target.
2. `uc_service`: `ApplyCtx` + trait `NAME`/`VERSION`/`IDENTITY`, `IdGen`
   (frozen + golden) via `ctx.ids()`, the three ctx-building call sites
   (apply loop, replay, tail-replay), attach-by-name writing hash +
   version, `Sessioned` forwarding, all in-tree SM impls.
3. `uc_node`: `ServicesConfig` by name (section required), boot writes,
   positional snapshot-path check + version check, metrics + alert rule,
   `uc2ctl`.
4. `uc_client`: `fsm()` / `declared_names()`.
5. Harnesses + capstones with names; the negative order scenario; Elle;
   crashtest.
6. Docs + release writeup; fleet gate.
