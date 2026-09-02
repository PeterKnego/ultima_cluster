# UC v2 — FSM identity: name the state machine, not the slot

**Date:** 2026-09-02
**Status:** approved design (brainstorm 2026-09-01/02, five sections
approved in turn; Aeron comparison read from source before this was written).
Next: the implementation plan.
**Baseline:** `main` bd4587e (`v2.10.0`; wire 0.6.0; cnc 3.0; remote
protocol v1; M14 multi-service as shipped — `[services] ids`, slot-numbered
FSMs, `docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md`).
**Backlog line:** `docs/BACKLOG.md` item 2.

## 1. Goal and locked decisions

M14 identifies an FSM by its **slot number** (`service_id: u8`, `0..8`):
the service declares it in `ServiceConfig`, the node declares the set in
`[services] ids`, and every per-FSM path, label, ring, artifact and wire
field is keyed by it. Two nodes agree on the *set of numbers* (checked on
the snapshot path) and never on *what logic each number holds*. The slot is
placement; nothing states identity. An FSM whose replicas sit at different
slot numbers on different nodes stalls every joiner today (declared-set
mismatch) and, if the numbers happen to overlap, installs one FSM's
snapshot into another's directory.

This spec gives an FSM an identity that is a property of its business
logic — declared in code, the same wherever the binary attaches — and
demotes the slot to a node-internal row index that appears on no public
surface. It is the per-FSM analog of `app_id`.

| decision | choice | why (§) |
|---|---|---|
| where identity lives | **in code**: a required `const NAME: &'static str` on the state-machine trait | §2, §3.3 |
| what the node config declares | **names** (`[services] names = [...]`) plus an optional `default` | §4.1 |
| the slot | **internal**: the row = the name's index in the node's list, as written; assigned at boot, never declared by a service, never compared across nodes | §4.2, §9 |
| what is keyed by name | every per-FSM path on disk, every metric label, every wire artifact, the client API | §4.4, §4.5, §5, §6 |
| cluster-wide agreement | the **set of identity hashes + the default**, on `SNAP_BEGIN` (wire 0.7.0), replacing the bitmask | §5 |
| first consumer | `uc_service::ids::IdGen` — deterministic, stateless, placement-independent IDs from `(position, identity, ordinal-within-apply)` | §3.4 |
| FSM version | **not designed here**; bytes reserved; Aeron's shape recorded for backlog item 3 | §7 |
| flag day | one combined: cnc 3.0 → 3.1 and wire 0.6.0 → 0.7.0 | §4.2, §5 |

### What does not change

Consensus, elections, the commit pipeline, the frame layout, crypto
framing, the ingress/egress rings, the M13 client `Engine` internals (the
fan-in buffer and slot bookkeeping stay row-indexed), the remote protocol
(v1, no FSM selector — the gateway keeps targeting one default FSM), the
snapshot artifact's bytes (still entirely the service's own), and the lag
policy. The Lean model, conformance vectors and loom models are untouched:
no consensus-relevant state, ring or frame moved.

## 2. Why this shape, and what Aeron does

**Identity in code, not config.** The maintainer's framing (2026-09-01):
"FSM represents business logic and should generate same deterministic IDs
across nodes. FSM on node1 in slot1 should create same ID as same FSM on
node2 slotX." A name in deployment config can be mis-set per host; a name
on the type ships with the logic and cannot be. `ServiceConfig` therefore
loses its `.service_id()` setter rather than gaining a `.name()` beside it
(the "both, config may override" variant was rejected as the weakest
guarantee for the most surface).

**The slot is routing, not identity.** Nothing in correctness depends on
the row number agreeing across nodes once every cross-node exchange is
keyed by name: commands are broadcast (every FSM applies every frame; the
row is in no log frame and on no wire except `SNAP_BEGIN`); queries are
node-local (a client resolves against the node it is attached to and the
read barrier checks that node's slot); the one real cross-node hazard —
"FSM 0 answers remote clients" — becomes a *named* default compared
cluster-wide. So the row can be assigned per node and forgotten.

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
  M14 is a faithful copy of this, and this spec goes past it.
- `serviceName` (`aeron.cluster.service.name`, default
  `"clustered-service"`) exists and is **cosmetic**: it feeds
  `Agent.roleName()` and is written into the mark-file header for tooling.
  It is never validated, never on the wire, never in the log. That is the
  "name as a label only" half-measure in production form; it is why this
  spec does not stop at a label.
- There is **no default responder**: any service may respond on any client
  session. UC's default is UC-specific (the remote path reaches one FSM).
- **Deterministic IDs:** Aeron ships no generator. Its replicated inputs
  are the cluster session id (assigned by the consensus module, carried in
  `SessionOpenEvent`, `nextSessionId` snapshotted), the leader-stamped
  timestamp on every log message, the log position and the term id;
  services derive from those. Same principle as §3.4 — randomness is
  *input*, never generated in apply — with leader-stamped time as the one
  input UC's frame does not carry.
- **Version:** see §7.

## 3. The identity (`uc_protocol`) and the SDK surface (`uc_service`)

### 3.1 Name rules

A new leaf module, `uc_protocol::identity`, `core`-only like `version` and
`magic`. A name is **1..=32 bytes** of lowercase ASCII letters, digits, `_`
and `-`, and starts with a letter. The bound is what lets the name sit
verbatim in one cnc slot line and in a directory name with no escaping;
the alphabet is what keeps it a valid path component and metric label
value on every platform.

### 3.2 Hash

`fsm_hash(name) -> u64`: **FNV-1a 64** over the name's bytes, a `const fn`.
It is a **frozen constant** in the same class as the wire constants: a
golden-vector test pins it, and a change is a flag day (a replica on a
different SDK build must mint the same IDs and match the same
`SNAP_BEGIN` set). The hash is what goes on the wire and into `IdGen`; the
name is what goes into the cnc page, on disk, into labels and into
refusals.

`FsmIdentity { name: &'static str, hash: u64 }`, constructed by
`FsmIdentity::parse(name)` — a `const fn` that **panics on an invalid
name**, so a bad name is a compile-time error at the first use of the
provided const below (const evaluation happens where the const is used),
not a runtime refusal.

### 3.3 Trait change

```rust
pub trait RawStateMachine: Send + 'static {
    /// The FSM's identity — the same wherever this type attaches.
    const NAME: &'static str;
    /// Provided; evaluated (and validated) at first use.
    const IDENTITY: FsmIdentity = FsmIdentity::parse(Self::NAME);
    /// Provided: the deterministic ID generator for one apply call.
    fn ids(position: u64) -> IdGen { IdGen::new(position, Self::IDENTITY) }
    fn apply(&mut self, position: u64, cmd: &[u8], out: &mut Vec<u8>);
    fn query(&self, q: &[u8], out: &mut Vec<u8>);
    fn last_applied(&self) -> Option<u64>;
}
```

The typed `StateMachine` gets the same required `NAME` and provided
`ids()`; the blanket impl forwards `NAME`. `Sessioned<S>` forwards
`S::NAME`. No `dyn` use of either trait exists in the tree (checked
2026-09-02), so the associated const is safe. Every state machine in the
tree (≈ 24 impls: `uc_lincheck`'s `RegisterSm`/`ListAppendSm`, the counter
example, the gate examples, the test SMs) gains one line, mechanically.

### 3.4 `IdGen` — deterministic IDs

`uc_service::ids::IdGen`: a value type, no heap, no state beyond
`(position, identity, ordinal: u32)`.

```rust
// in apply — built once per call from the position handed to you
let mut ids = Self::ids(position);
let order_id = ids.next();   // u128
let line_id  = ids.next();   // u128, no visible relation to order_id
```

**Input (128 bits):** `position: u64 ‖ ordinal: u32 ‖ fold32(identity.hash)`
where `fold32(h) = (h >> 32) as u32 ^ h as u32`.
**Permutation:** a **three-round Feistel** over the two 64-bit halves with a
standard 64-bit finalizer (murmur3 `fmix64`) as the round function. A
Feistel network is a bijection for *any* round function, so uniqueness
within one FSM is by injectivity, not by birthday odds; three rounds
suffice for consecutive ordinals to share no visible structure. The
permutation is **frozen** like the hash, with golden vectors and an
inverse-round-trip test.

**Why this scoping is the whole correctness story.** The ordinal is *per
apply call*, never per process lifetime. A lifetime counter diverges three
ways: a snapshot-installed replica has called it 0 times where a
journal-replayed one has called it N; a read that calls it advances only
the replica that served the read; a restarted service recounts from its
last applied position. Per-apply reset makes the series a pure function of
committed input — nothing to snapshot, replay-safe by construction, and
the same on a node that installed a snapshot and one that replayed the
journal.

**Documented rules (SDK docs + the explainer):**

- Build the generator once per apply from the position given to you. Never
  call it from `query` — a read has no position that means the same thing
  on every replica.
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

## 4. Node (`uc_node`): config, boot, cnc, disk, attach, observability

### 4.1 Config

```toml
[services]
names   = ["kv", "orders"]   # 1..=8, each a valid name, no duplicates
default = "kv"               # optional; must be listed; falls back to names[0]
fsm_lag = "16MiB"            # unchanged
```

`ServicesConfig` becomes `{ names: [FsmIdentity; ≤8] (in list order),
default_row: u8, fsm_lag }`. Refusals, each naming the field in the M9
style: empty list, invalid name (with the rule quoted), duplicate, more
than 8, `default` not in the list. **`ids` is refused by field name** with
a pointer to `names` — there are no deployments to migrate, so no
compatibility shim. The shared CLI form becomes `--services kv,orders`
(`--default kv`). **`[services]` becomes required** in `node.toml` — a file without it
refuses to start by name, the same explicit-choice rule `[crypto]` and
`[admin]` have had since 2.6.0. The alternative (absent ⇒ one FSM with a
magic name) would force every single-FSM state machine to carry that name
to attach anywhere, which is identity by config through the back door.
Programmatic configs (`NodeConfig::services`, the test harnesses) name
their FSMs explicitly; `ServicesConfig::default()` goes away and only
`none_for_tests()` remains.

One simplification falls out: rows are **contiguous from 0** (the row is
the list index), so `services_declared` loses its holes. The consensus
agent's floor computation over declared rows is unchanged.

### 4.2 Boot and the cnc page (cnc 3.0 → 3.1)

The node already zeroes the slot band at every start. It now also writes,
before the page is published:

| where | field | width | writer |
|---|---|---|---|
| slot `+448..+480` (reserved line 7) | `name` — the row's name, NUL-padded | 32 B | **node, boot-once** |
| slot `+480..+512` | reserved (zero) — see §7 | 32 B | — |
| slot `+8` (status line 0) | `identity_hash` u64 | 8 B | service, at attach (same line, same writer as `status`) |
| page 1 `4048` (the 4032 line) | `default_row` u64 | 8 B | node, boot-once (same line, same writer as `services_declared`/`fsm_lag_bytes` at 4032/4040) |

Line 7 was "reserved, written by nobody"; it becomes node-written at boot,
which keeps the one-writer-per-line rule (the M14 spec §3.4 named line 7
as the place for per-slot additions). The 4032 line is the last free line
of page 1 and already belongs to the node at boot; its third word is
free. Offsets are pinned in **both** `uc_protocol` and `uc_log` with the
usual offset-assertion tests. `CNC_V2_VERSION` becomes `3.1` — by
`docs/reference/semver-policy.md` a cnc layout change is a **flag day**
regardless of the digit; §10 bundles it with the wire change.

### 4.3 Attach (`uc_service`)

`ServiceConfig` drops `service_id`. Attach validates the page
(magic/crc/version/`app_id`) and captures `instance_id` as today, then
**scans the eight name lines for `S::NAME`**:

- not found → `ServiceError::UnknownFsm { name, declared: Vec<String> }`,
  a named refusal listing what the node declares;
- found → the row; the rest of attach runs unchanged with the row where
  `service_id` was. The internal `service_id: u8` field **stays as the
  row** (the "build on it" decision); it is `pub(crate)`.

Attach writes `identity_hash` into the slot's status line so a later reader
(`uc2ctl`, a test) can cross-check name and hash without re-hashing. The
per-FSM lock (`service.<name>.lock`) is taken by name (§4.4).

### 4.4 On disk — everything per-FSM is keyed by name

| today (`<id>` = slot) | after |
|---|---|
| `snapshots/<id>/` | `snapshots/<name>/` |
| `service.<id>.lock` | `service.<name>.lock` |
| `egress_service.<id>.broadcast` | `egress_service.<name>.broadcast` |
| `svc_query.<id>.ring` | `svc_query.<name>.ring` |
| `state/output_progress.<id>.state` | `state/output_progress.<name>.state` |

`SnapshotStore::open(instance_dir, &FsmIdentity)`. The artifact's bytes and
`snap-<pos>.ultsnap` naming are unchanged. The backup manifest lists
snapshots per name; `verify`'s per-FSM coverage form and `restore`'s
directory filter work on names. A restore from *another node's* backup then
lands each FSM's artifacts under its own name regardless of the row it held
there — that is where placement independence reaches the disk. The
runbook's instance-dir layout section is rewritten.

### 4.5 Observability

- Metric labels: `service="orders"` (the name), so a cross-node dashboard
  groups by FSM rather than by row. `uc2_services_declared` stays a mask of
  rows (alerting on "sets differ" needs only the count; names are in the
  labels). `uc2_service_default_row` is new. The alert-rule file and
  dashboard are swept for `service="0"`-style literals.
- `uc2ctl status` prints `name=orders row=1 hash=… attached=… applied=…`
  per row, and `default=kv`.
- `obs_event!` records that carry a service id carry the name.

## 5. Wire 0.7.0 — `SNAP_BEGIN` and the cluster-wide check

**The only wire carrier of FSM identity is the snapshot-session opener**
(M14 spec: commands are broadcast; durable reports are aggregates;
`uc_net/src/sender.rs` — one artifact per declared FSM, plus the declared
mask the receiver compares). So the wire change is confined to one body.

`SnapBeginBody` 0.7.0 (fixed length; `SNAP_BEGIN_FIXED_LEN` grows from 34):

| field | 0.6.0 | 0.7.0 |
|---|---|---|
| `session: u32`, `layout: u8`, `snapshot_pos: u64`, `config_len` + config | as today | as today; `layout = 2` |
| `service_id: u8` (the artifact's row) | | **`identity_hash: u64`** (the artifact's FSM) |
| `services_declared: u64` (bitmask) | | **`declared: [u64; 8]`** identity hashes in the sender's row order, `0` = unused |
| — | | **`default_hash: u64`** |
| — | | **reserved: 32 B** (zero) — see §7 |

Hashes rather than names: the receiver already knows its own names and only
needs to match, and a fixed body keeps the decoder bounds-checked the way
the 0.6.0 one is. `uc_protocol::version::CURRENT` → `0.7.0`. A 0.6.0
sender's body is shorter than the new fixed length, so the receiver drops
it by the same length check that drops 0.5.0 today: **a mixed cluster
stalls a joiner rather than installing a wrong artifact** — the same
failure mode as the last two wire changes, and the standing flag-day rule.

**The receiver's check.** Set equality over the non-zero hashes (row order
is *ignored* — that is the placement-independence contract on the wire),
plus `default_hash` equality. On mismatch it refuses by name: it prints its
own names, and for each sender hash the matching local name where one
exists and the hash otherwise. `uc2_snapshot_refused_declared_set_total`
keeps its meaning; `uc2_snapshot_refused_default_total` is new. On match,
**each artifact routes by hash to the receiver's own row** and lands under
`snapshots/<name>/`; the sender's row never reaches the receiver's disk.

The `SNAP_BEGIN` fuzz target moves to the new layout;
`docs/reference/wire-protocol.md` and `docs/how-to/upgrade-a-cluster.md`
get the 0.7.0 entry.

## 6. Client (`uc_client`) and gateway (`uc_gateway`)

**Local client.** The engine already opens the cnc page at attach; it now
reads the eight name lines into a small table, so resolution is a one-time
lookup and the hot path never touches a string.

```rust
let orders = client.fsm("orders")?;            // FsmHandle { name, row }, bound to this attach
let r: R   = client.submit_to(&orders, &cmd)?;
let v: V   = client.query_snapshot_on(&orders, &q)?;
let all    = client.submit_all(&cmd)?;         // Vec<(FsmHandle, R)>, ascending by name
let names  = client.declared();                // &[FsmName]
```

- Unknown name → `ClientError::UnknownFsm { name, declared }`.
- The `query.ring` payload stays **row**-prefixed (`row:u8 ++ query`) — it
  is same-host IPC, so the row is the correct key there; `MSG_V2_BAD_SERVICE`
  carries a row and the client maps it back to a name in the error.
- Handles are per attach: a node restart changes `instance_id` and attach
  already refuses stale state, so a handle cannot outlive the config it was
  resolved against.
- The `PollHalf` fan-in buffer and the all-atomic `Slot` stay row-indexed,
  untouched.

**Gateway.** The edge relays remote traffic over the local engine and today
targets FSM 0 by number. It now targets **`default_row`** read from page 1
(§4.2). The remote protocol stays v1. Because `default` is compared
cluster-wide on the snapshot path (§5), two gateways cannot answer remote
clients from two different FSMs — the one real cross-node hazard is closed
without touching the remote wire. Name-addressed remote routing remains
the remote-v2 question backlog item 1 owns.

**Harnesses.** Every gate/bench binary that takes `--services 0,1` takes
names through the shared parser (§4.1); `m14_fleet_gate.py`, `hop_bench`,
`apply_bench` and the two-FSM capstones change their arguments and nothing
else.

## 7. FSM versioning — reserved, not designed

The maintainer asked (2026-09-02) whether 0.7.0 should carry a per-FSM
version, since rolling upgrades (backlog item 3) will need one. **No field
is added; bytes are reserved** (32 B per slot in cnc line 7, 32 B in
`SnapBeginBody`), and the reasoning is recorded here so it is not
re-derived:

- A static per-node version compared on `SNAP_BEGIN` can only *refuse*, and
  refuses late: snapshot sessions are rare, so two replicas on different
  logic would have diverged on live commands long before the check fires.
  It is a guard, not an upgrade mechanism, and pre-commits the wrong shape.
- **Aeron's shape** (read from source, §2): one **cluster-wide `appVersion`**
  (an integer in semantic-version layout, per *application*, not per
  service), configured on the consensus module and every service container;
  the **leader stamps it into the log** in every `NewLeadershipTermEvent`
  and into every snapshot marker; every module and service **validates on
  each new-term event and on snapshot load** through a pluggable
  `VersionValidator` (default: major-equality) and is fail-stop on
  incompatibility. The version is thus a replicated fact that changes at
  leadership boundaries, and compatibility semantics are the user's promise
  through the validator. That is a proven middle shape between "static
  field" and "deliberate activation record", and it argues the real carrier
  in UC is a **term-boundary log event**, not `SNAP_BEGIN`.
- Determinism's actual requirement — every replica applies a given command
  with the same logic — is met today with no UC support by shipping both
  logics and flipping on a replicated command. What item 3 can add is a
  first-class activation/version record so the switch position is visible
  to operators and the snapshot path, in whichever of the two shapes it
  chooses.
- Snapshot *format* versions are the service's business inside its own
  bytes; UC never decodes an artifact.

## 8. Failure modes

| situation | outcome |
|---|---|
| service binary's `NAME` not in the node's list | attach refused by name, listing the declared names; the node runs on (the row stays unattached, as today for a never-attached id) |
| invalid `NAME` in code | compile-time error at first use of `IDENTITY` |
| two nodes list different name sets, or a different `default` | every snapshot session between them refused by name; joiner stalls; counter + alert (as the declared-set mismatch today) |
| two nodes list the same names in different order | **nothing** — rows differ, every cross-node exchange is by hash, disk is by name (the §9 capstone) |
| a 0.6.0 node in a 0.7.0 cluster (or vice versa) | its `SNAP_BEGIN` is dropped by length; joiner stalls; flag day |
| a cnc 3.0 service SDK against a 3.1 node | refused by cnc version at attach (existing gate) |
| `node.toml` without `[services]` | named startup refusal (as `[crypto]`/`[admin]`) |
| restore of another node's backup with different row order | artifacts land under their names; rows are reassigned from this node's config at boot |
| `IdGen` used in `query` | not detectable by the SDK; documented as forbidden (§3.4) |
| renaming an FSM | a per-FSM flag day: config on every node + on-disk directories (documented in the runbook; same trade as `app_id`) |

## 9. Test plan and acceptance

**Unit tier** (each test written first and watched red; the plan records
which fix each one was reverted against):

- `uc_protocol`: name-rule table (accept/reject with the reason), FNV
  golden vectors, the slot name line and page-1 `default_row` pinned by
  offset assertions in both `uc_protocol` and `uc_log`, `SnapBeginBody`
  0.7.0 round trip + a 0.6.0-length body dropped, the fuzz target on the
  new layout.
- `uc_service`: `IdGen` golden vectors; inverse round trip proving the
  permutation is a bijection (sampled + an exhaustive small-domain case);
  disjoint series for two identities; `Sessioned` forwards `NAME`; the
  unknown-name attach refusal; the store keyed by name; the raw-contract
  and session suites updated for `NAME`.
- `uc_node`: every `[services]` refusal by field name (`ids` included);
  boot writes names and `default_row` before publish; declared-set and
  default mismatch refused by name on the snapshot path, with the
  by-name/by-hash message; per-name backup manifest, `verify` coverage and
  `restore` filter; metric labels by name; `uc2ctl status` output.
- `uc_client`: `fsm()` resolution, `UnknownFsm`, `submit_all` ordering by
  name, `BAD_SERVICE` mapped to a name.
- `uc_gateway`: the edge targets `default_row` (a node with `default` not at
  row 0 answers remote clients from the named FSM).

**Capstones — the placement-independence proof.** The two-FSM `lin_v2`
scenarios, the hard-crash two-FSM scenario (`uc_crashtest`) and Elle's
`quiet_two_fsm` pass all run with names and with **row order deliberately
differing across nodes** (`["kv","orders"]` vs `["orders","kv"]`),
including a learner join / below-floor snapshot install across that
difference. Adjudicated by the untouched WGL checker. Loom, Lean and
conformance: unchanged, re-run as regression only.

**Fleet gate** (`docs/benchmarks/uc2-fsm-identity-gate-<date>.md`, bars
pre-committed before the run, honest-failure protocol): the M14 gate's
rows with names substituted (steady-window, `m14_fleet_gate.py`), plus one
row for snapshot transfer under mismatched row order. The hot path is
row-indexed and untouched, so the expected delta is null; the bar is a
bound against the harness's measured build-to-build resolution
(`scripts/hop1_ab.sh`'s same-source rebuild control), not a hoped-for
number.

## 10. Release and docs

- **Version:** the trait gains a required const and `ServiceConfig` /
  the client lose a setter — a **breaking API change** under
  `docs/reference/semver-policy.md`, on top of the cnc + wire flag day.
  **Open decision for the maintainer:** ship as a major, or ride the
  no-external-users allowance as the next minor. The spec does not choose.
- The standing writeup rule: a section atop `RELEASES.md`; the
  `docs/releases.md` entry; QUICKSTART / how-to / reference sweep;
  `docs/ops/uc2-runbook.md` instance-dir layout; `docs/reference/
  {wire-protocol,cnc-page,semver-policy}.md`; `docs/how-to/upgrade-a-cluster.md`
  (0.7.0 + cnc 3.1); the M14 spec gets an as-built erratum pointing here;
  `docs/BACKLOG.md` item 2 → this spec, item 3 → §7.
- One plain-language explainer, `docs/notes/uc2-fsm-identity-and-deterministic-ids-explained.md`,
  covering both halves (why identity is in code, why the slot is not
  identity, why `IdGen` is per-apply, the retry and cross-FSM rules, what
  Aeron does).

## 11. Out of scope (recorded so it is not re-derived)

- Name-addressed **remote** routing — remote protocol v2, backlog item 1.
- **FSM versioning** — backlog item 3, with §7's reserved bytes and Aeron's
  shape.
- **Code-hash verification** of an FSM. A name says two replicas *intend*
  to be the same FSM; it does not verify they run the same code. That is
  the user's determinism obligation, stated in the explainer.
- Leader-stamped timestamps in the frame (the input a Snowflake-style ID
  would need) — a frame-header flag day with no requester.
- Stage-2 multi-log (M14 spec §10) — unaffected; a name is per log.

## 12. Implementation order (for the plan)

1. `uc_protocol`: `identity` module (rules, FNV, `FsmIdentity`), cnc 3.1
   offsets + `uc_log` pins, `SnapBeginBody` 0.7.0 + fuzz target.
2. `uc_service`: trait `NAME`/`IDENTITY`/`ids()`, `IdGen` (frozen + golden),
   attach-by-name, store/lock/rings/progress by name, `Sessioned`
   forwarding, all in-tree SM impls.
3. `uc_node`: `ServicesConfig` by name + `default`, boot writes, snapshot
   path check + routing by hash, backup/verify/restore, metrics, `uc2ctl`.
4. `uc_client` + `uc_gateway`: `fsm()`/handles, `default_row`.
5. Harnesses + capstones with mismatched row order; Elle; crashtest.
6. Docs + release writeup; fleet gate.
