# Configuration

The knobs a node and a service are constructed with, plus the environment
switches the workspace reads.

Field-level API documentation for these types is generated:
[`NodeConfig`](https://peterknego.github.io/ultima_cluster/uc_node/struct.NodeConfig.html)
and the `uc_service` config types. This page states the surface, its defaults,
and its limits.

## The config file

`uc2-node --config <path>` loads a TOML document that mirrors `NodeConfig`
one-for-one. `packaging/node.example.toml` is the annotated reference copy, and
a test asserts it stays valid.

Two properties are worth stating because they change how mistakes surface:

- **Unknown keys are refused.** The document is parsed with
  `deny_unknown_fields`, so a typo is a startup error naming the offending key,
  never a silently-ignored setting.
- **The file is validated before anything starts.** Every rule in
  [startup refusals](#startup-refusals) runs against the loaded config before
  the first agent is spawned.

Field names match the `NodeConfig` fields below. Four differ in shape:

| TOML | Maps to |
|---|---|
| `[[members]]` / `[[learners]]` tables of `id` + `addr` | `Vec<(NodeId, SocketAddr)>` |
| `[purge]` with `below_snapshot_slack_bytes` — absent means disabled | `PurgePolicy` |
| `[crypto]` with `enabled` (required), `key_path`, `allowlist_path`, optional `rotation_interval_ns` / `rotation_bytes` | `CryptoConfig` |
| `[services]` with `names` — **required**, no default (FSM identity, 2.11.0) | `ServicesConfig` |
| `[settings]` with `admission_bytes`, `fsm_lag` (a string), `snapshot_interval_bytes`, `snapshot_target` — optional; the **genesis seed** for the cluster's replicated settings record (the cluster FSM, 2.11.0) | `Settings` (`NodeConfig::settings_genesis`) |

Two keys exist only in the file and have no `NodeConfig` field:

**`seed`** — optional. Defaults to a distinct per-id value. Identical seeds
across nodes make every member time out at the same instant and split the vote.

**`allow_volatile_fs`** — optional, default `false`. Test and development only;
see [startup refusals](#startup-refusals).

**`[admin]`** has no `NodeConfig` field at all — see
[Admin authentication](#admin-authentication) below for why it lives on
`StartOpts` instead.

### `[log]` and `[metrics]`

Reserved in M9 (accepted-but-inert, so a config written ahead of the release
that defined them would not refuse to start); their schema is defined since
M10, and both are now validated exactly like every other section — unknown
keys inside them are a startup refusal too.

**`[log]`** — structured JSON-lines records on stderr. Optional; absent means
the default level.

| Key | Default | Meaning |
|---|---|---|
| `level` | `"info"` | one of `"error"`, `"warn"`, `"info"` (each level includes the ones before it: `error` < `warn` < `info`) |

**`[metrics]`** — the `/metrics`, `/healthz`, `/readyz` HTTP endpoint.
Optional; **absent means the endpoint never opens**, not a disabled state
with a listener.

| Key | Default | Meaning |
|---|---|---|
| `bind` | `127.0.0.1:9600` | the socket address the endpoint listens on |

A bare `[metrics]` section with no `bind` key still opens the endpoint, on
the default address. The endpoint is unauthenticated and read-only; see
[Monitor a cluster](../how-to/monitor-a-cluster.md#security-note) for the
bind/firewall guidance before exposing it beyond loopback.

For the full series contract, the alert rules, the dashboard, and the
structured-event vocabulary, see
[Monitor a cluster](../how-to/monitor-a-cluster.md).

### `[services]`

M14a: which state-machine processes (FSMs) this node hosts, and how far apart
they may drift. **Required since FSM identity (2.11.0, spec §4.1): a
`node.toml` without `[services]` refuses to start by name** — the same
explicit-choice rule `[crypto]` and `[admin]` have had since 2.6.0. There is
no default set: absent used to mean `ids = [0]`; now a node names every FSM
it hosts or does not start. The set is static and must be identical on every
node, in the **same order** — it is not a live-reconfiguration surface the
way `members` is, and (since FSM identity) the list's *order* is now part of
what a mismatched cluster is refused on. See
[Write one config file per host](../how-to/run-a-cluster.md#write-one-config-file-per-host)
for the operational picture and the M14a snapshot-transfer limitation.
Background: [how multi-service works](../notes/uc2-m14-multi-service-explained.md)
and [the FSM identity explainer](../notes/uc2-fsm-identity-and-deterministic-ids-explained.md).

| Key | Default | Meaning |
|---|---|---|
| `names` | none — **required** | The declared FSM names, in row order (list index = row). Each `1..=32` bytes of lowercase ASCII letters, digits, `_`, `-`, starting with a letter; no duplicates; at most 8; **the `uc_` prefix is reserved** for UC's own internal state machines and is refused by name. A service attaches by scanning for its own `S::NAME` — it no longer states its row. |

`ids` is **refused by field name**, pointing at `names` — there is no shim
(no deployments existed at the time of the change): `services.ids was
replaced by services.names (FSM identity): list the FSM names in row order,
e.g. names = ["kv", "orders"]`.

`fsm_lag` **moved out of this section** with the cluster FSM (2.11.0):
it is a cluster-wide policy, so it lives in the replicated settings record and
is seeded by [`[settings]`](#settings) below. A `fsm_lag` under `[services]`
is refused by name, pointing at `uc2ctl settings apply`.

The `uc_` prefix is reserved because UC's own cluster FSM declares
`const NAME = "uc_cluster"`; see
[the cluster FSM explainer](../notes/uc2-cluster-fsm-explained.md).

### `[settings]`

The cluster FSM (2.11.0, spec §6): the four **cluster-wide** policies
that used to live per host. This section is a **genesis seed only** — it is
read when the instance directory is fresh and there is no settings record
yet, and ignored from the first `CLUSTER` frame onward, exactly as
`[[members]]` has been since M7. The live values come from the log; change
them with [`uc2ctl settings apply`](uc2ctl.md#settings-apply) and read them
back with `uc2ctl settings show`. An absent section seeds
`Settings::genesis_default()` — every numeric key `0`, `snapshot_target =
"all"`.

Every key's `0` means **"derive at use"**, not "zero", and every replicated
value is **clamped against this host's own geometry at the point of use**
rather than refused in `apply` — the state machine that applies a settings
record cannot see the host it lands on.

| Key | Default | Meaning |
|---|---|---|
| `admission_bytes` | `0` → derive (256 KiB, `NodeConfig::admission_bytes_default`) | The ingress admission budget: the `append - commit` backpressure gate at the leader's door. Clamped to this host's `buffer_bytes / 2` at use. Published on the cnc page at offset 3712 and as `uc2_admission_bytes`. |
| `fsm_lag` | `0` → derive (`buffer_bytes / 4`) | How far `applied` may drift between any two declared FSMs before the admission door closes. A string: `"<n>[KiB\|MiB\|GiB]"` (e.g. `"16MiB"`, no spaces, no fractions, binary units only) or `"lockstep"` (no FSM starts frame k+1 until every FSM finished frame k). A byte bound is **clamped** at use into the range from one max-size frame up to this host's `buffer_bytes / 2`, rather than refused — but a byte bound **below 1376 B** (one max-size frame on the widest path the transport allows) is refused outright by `uc2ctl settings apply` with `47 settings_bounds`. A lag shorter than one frame is not a tighter policy: the report ceiling is `min_applied + fsm_lag`, so it can pin commit strictly inside the next frame **permanently**, and the only way to change a replicated setting is a command that has to commit. Write `"lockstep"` if you want the tightest possible pacing. Lockstep costs an N-way cross-core handshake per frame — ~1.6 µs at N=2 on the dev box, i.e. ~600 k frames/s per FSM against ~22 M bounded (`docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md`) — and while a sibling is stalled or dead every other FSM burns ≈ a core yielding on it. Those are **dev-box numbers, measured with the FSMs alone on the box**; on a contended host the cost is far higher — the 2026-08-29 fleet run measured lockstep at **60×** its bounded twin on a `c6id.2xlarge` leader host also running the node and the client (`docs/benchmarks/uc2-m14-gate-2026-08-29.md`, row e). **Lockstep needs a free CPU per declared FSM on top of the node's own agents: the cost is a gradient, not a cliff at one point — 3 busy threads on 2 CPUs is ~4× down (624 k → ~150 k), the two hyperthreads of one core ~7× down (~87 k), and 3 busy threads on 1 CPU ~880× down (709 frames/s per FSM at N=2) — while bounded mode on that worst rung is unaffected at 7.4 M frames/s** (full ladder in the record) — an operating-envelope fact, not a defect: lengthening the barrier's yield ladder ×4/×16 and making it unbounded were both measured at exactly 1.00× (`docs/benchmarks/uc2-m14c2-lockstep-oversubscription-2026-08-30.md`). Size the host, or pin the FSM threads, accordingly. |
| `snapshot_interval_bytes` | `0` → on demand only | How much log the **leader** lets accrue before it appends another `SNAPSHOT` frame. `0` means **no cadence**: instants are operator-commanded only ([`uc2ctl snapshot`](uc2ctl.md#snapshot)), which is the default and matches purge being off by default. The clock measures from the last instant this leader *commanded*, and it is re-based to the append frontier at every leader open so election churn cannot become a snapshot storm — both make the cadence err late rather than early. A leader flapping faster than the interval therefore never snapshots on its own. |
| `snapshot_target` | `"all"` | Who freezes for a **cadence-issued** instant: `"all"` (every node's rows) or `"learners"` (only a learner's — the standby form, so no voter pays the freeze and a voter picks the set up with [`uc2ctl snapshot fetch`](uc2ctl.md#snapshot-fetch)). Any other value is refused by name. `uc2ctl snapshot --standby` overrides it per command. |

Both `0` sentinels keep their meanings under that refusal: `fsm_lag = 0`
("derive this node's boot value") and `"lockstep"` are not byte bounds, so
neither is refused.

`fsm_lag = 0` in the **wire record** is "derive at use", so lockstep has its
own sentinel there (`FSM_LAG_LOCKSTEP = u64::MAX`) rather than reusing the cnc
page's `0`. You never write that number: `"lockstep"` in the TOML is what maps
onto it.

## Startup refusals

`uc2-node` refuses to start, naming the field, rather than failing later in a
way that looks like something else. Each rule exists because of the failure it
replaces:

| Refusal | What it prevented |
|---|---|
| `bind` must equal this node's own `members` entry | A leader elects, but followers never advance `durable` or `commit` — datagrams arrive from a source address matching no member. |
| `instance_dir` must not be on a RAM-backed filesystem | Every `fsync` is a silent no-op; the cluster appears to work and loses committed data on power loss. |
| `max_payload` is refused by name | **Retired since `2.12.0`** (jumbo-frame MTU discovery): the payload ceiling is discovered per cluster, not pinned in `node.toml` — the two checks this key used to satisfy (fit one datagram, carry a full schedule table) are gone with it, the second now a compile-time assert against the baseline ceiling. The refusal points at [`force_jumbo_frames`](#policies) for operators who need to *require* a jumbo path. |
| `buffer_bytes` must be a power of two | Ring geometry. |
| this node's `id` must appear in `members` or `learners` | A node not in its own cluster. |
| `members` and `learners` must be disjoint, ids unique | Ambiguous role and peer-band aliasing. |
| at most 8 members total | The control page's per-peer band holds 8 slots; enforced on the wire too. |
| `election_timeout_min_ns` < `election_timeout_max_ns` | An empty randomisation window. |
| `log.level` must be `"error"`, `"warn"`, or `"info"` | A silently-ignored typo picking the wrong verbosity. |
| unknown keys inside `[log]`/`[metrics]` are refused, by name, like every other section | M9 accepted anything inside these two sections unvalidated; M10 defines their schema, so a typo there is now caught the same way as everywhere else. |
| `[crypto]` section must be present | M12b (spec §3.3): `enabled` is an **explicit choice**, not absent-means-off like `[purge]` — an absent section is `ConfigError::CryptoChoiceRequired`, so a `node.toml` cannot silently run cleartext by omission. `enabled = false` must not also carry `key_path`/`allowlist_path`; `enabled = true` requires both. |
| `[admin]` section must be present | M12b (spec §3.3, §5.1): `auth` is likewise an explicit choice — an absent section is `ConfigError::AdminChoiceRequired`. `auth = "hmac"` requires at least one uniquely-named entry in `keys`; `auth = "none"` requires `keys` to be empty; `request_ttl_ms` (default 30000) must be `>= 1000` under either mode. |
| `[services]` section must be present | FSM identity (2.11.0, spec §4.1): there is no default FSM set — a `node.toml` must name every row, the same explicit-choice posture as `[crypto]`/`[admin]`. |
| `services.ids` is refused, pointing at `names` | FSM identity: `ids` was the pre-identity field; there is no shim — rewrite as `names = ["<fsm>", ...]` in row order. |
| `services.names` must not be empty | FSM identity: an explicitly-empty list would leave no FSM declared; there is no default to fall back to. |
| `services.names` entries must be valid FSM names | FSM identity: `1..=32` bytes of lowercase ASCII letters, digits, `_`, `-`, starting with a letter — the same rule the state-machine trait's `const NAME` is checked against at compile time. |
| `services.names` must not contain a duplicate name | FSM identity: a repeated name would double-attach one row, or leave a service unable to tell which row it found. |
| `services.names` entries must number at most 8 | FSM identity (was `services.ids` entries must be `< 8`): the cnc page's per-service band holds 8 slots. |
| `services.names` entries must not start with `uc_` | The cluster FSM (2.11.0, spec §4.1): `uc_` is reserved for UC's own internal state machines (`uc_cluster`), so a user FSM cannot collide with one. |
| `services.fsm_lag` is refused, pointing at `[settings]` | The cluster FSM (2.11.0, spec §6): the lag policy is cluster-wide, so it moved into the replicated settings record — `put fsm_lag under [settings] to seed genesis, and change it with uc2ctl settings apply`. |
| top-level `admission_bytes` is refused, pointing at `[settings]` | The same change: the admission window is cluster-wide, and its effective value used to change silently on failover. |
| `settings.fsm_lag` must parse | M14a's rule, now on the seed: an unparsable string (wrong suffix, spaces, a fraction) is refused by name rather than silently falling back to the derived bound. |
| `settings.snapshot_target` must be `"all"` or `"learners"` | The cluster FSM (2.11.0): an unknown target would silently pick one. |

The RAM-backed-filesystem refusal has two override channels, and **neither is
silent** — the override suppresses the refusal, never the notice, and the
warning is printed on every boot:

- `allow_volatile_fs = true` in the config file, the reviewable channel that
  shows up in a config diff;
- `UC2_ALLOW_VOLATILE_FS=1`, for suites that build a `NodeConfig` directly and
  never parse a file.

## `NodeConfig`

Passed to `Node::start`. The config file above is a mirror of this type.

### Identity and membership

**`id: NodeId`**
This node's id.

**`members: Vec<(NodeId, SocketAddr)>`**
Every voting member including this node, if it is a voter. Learners are not
listed here.
Seed only: authoritative for a fresh instance directory that has no durable
config record. After the first boot, the durable config record and the
cluster FSM's `FRAME_TYPE_CLUSTER` (kind = `Membership`) commands own
membership — the retired `FRAME_TYPE_CONFIG` frame type is gone — and this
field is ignored. A restart with an edited `members` list has no effect.

**`learners: Vec<(NodeId, SocketAddr)>`**
Learner peers. Default empty. A learner is replicated to but never counted: no
vote, no quorum slot, no flow-control window, no read-quorum ack. A node whose
own id appears here boots in learner mode with candidacy disabled. Learner ids
must be disjoint from `members`.
Seed only, on the same terms as `members`.

**`bind: SocketAddr`**
The replication socket bind address.

**`instance_dir: PathBuf`**
See [Instance directory](instance-directory.md). Reused across restarts.

**`app_id: String`**
Application identity, stamped into the cnc page. Attaching services and clients
must present the same value.

### Sizing

**`buffer_bytes: usize`**
Log ring buffer capacity. Must be a power of two. This much disk is reserved
at startup (see [Instance directory](instance-directory.md#on-disk-footprint));
a node that cannot reserve it refuses to start.

**`max_payload`** — **retired, `2.12.0` pending.** Refused by name at
startup. The payload ceiling this key used to pin is now DISCOVERED per
cluster: every node probes every peer up the rung ladder `RUNGS = [1408,
8832, 8960]`, and the leader commits `min` over every member's probed path
through the replicated Settings record, monotonically (never lowers). Every
node applies the committed rung at commit — the sender's budget, the
appender's door, and the live cnc `payload_ceiling` word every client and the
gateway edge reads per submit ([cnc page](cnc-page.md#counters-and-status)).
A fresh cluster starts at the `MTU_DEFAULT = 1408` B baseline rung (1344 B
crypto-off / 1312 B crypto-on) and rises toward `MTU_BOUND = 8960` B (8896 B /
8864 B) only once every path has proven it, never before. See
[Run a cluster on jumbo frames](../how-to/jumbo-frames.md) for the operator's
path, and [`force_jumbo_frames`](#policies) below for the one knob this
feature does add.

**`admission_bytes_default: u64`**
The **fallback** ingress admission budget in bytes — the `append - commit`
backpressure gate — used while the replicated `Settings::admission_bytes`
still reads `0` ("derive at use"). Renamed from `admission_bytes` by the
cluster FSM (2.11.0), which moved the live value cluster-wide. The
effective value is published on the cnc page at offset 3712 (since wire
protocol 0.3.0) and re-published whenever the committed setting moves.

**`settings_genesis: Settings`**
The cluster FSM (2.11.0): the [`[settings]`](#settings) seed, installed
as the cluster FSM's genesis image on a fresh instance directory and ignored
thereafter.

**`journal_segment_bytes: u64`**
Journal segment size. The archive rolls a new segment at this boundary.

### Elections

**`election_timeout_min_ns: u64`**, **`election_timeout_max_ns: u64`**
Bounds of the randomised election timeout.

**`seed: u64`**
Seed for the randomised timeout.

### Policies

**`purge: PurgePolicy`**
Journal purge policy. Default `PurgePolicy::Disabled`. The enabled form is
`PurgePolicy::BelowSnapshot { slack_bytes }`.
Snapshots shorten a service restart only together with purge: reconstruction
installs an artifact only when the journal no longer covers the start
position (`uc_service/src/replay.rs`); with purge off it replays the whole
journal. There is no per-service snapshot policy since 2.11.0 —
the retired `SnapshotPolicy` is gone and a snapshot is taken at a **coordinated instant**,
commanded with [`uc2ctl snapshot`](uc2ctl.md#snapshot) or by the replicated
`snapshot_interval_bytes` cadence below.
To turn it on, see [Keep the journal from growing without bound](../how-to/bound-journal-growth.md).

**`crypto: CryptoConfig`**
Node-to-node wire crypto. `NodeConfig`'s own default is `CryptoConfig::Disabled`
(library callers who build a `NodeConfig` directly, e.g. tests and harnesses,
still get this), but the TOML loader has no default of its own — `[crypto]`
is a required section (see [startup refusals](#startup-refusals) above) — and
the daemon prints nothing extra either way, unlike `[admin]`'s `auth = "none"`
boot warning. The enabled form carries the private key path and the allowlist
path.
To turn it on, see [Encrypt traffic between nodes](../how-to/encrypt-node-traffic.md).

**`force_jumbo_frames: bool`** — `2.12.0`, default `false`.
Turns path-MTU discovery into a **startup gate**. With it set, the node runs
its agents, replicates and votes as usual, but holds `can_serve` false and
answers `/readyz` with 503 — in **any** role, not just leader — until every
configured peer has proven the `JUMBO_MIN_RUNG = 8832` B datagram rung. It then
logs `jumbo_gate_passed` and serves. If 30 s (`JUMBO_GATE_WINDOW`, a constant)
elapses first the node **fail-stops** (exit 1) with one of two named refusals
— on a cluster that has not yet committed a jumbo rung; once it has, the join
gate below takes precedence and never fail-stops on silence:
`jumbo_path_too_narrow`, when a peer answered below the rung, or
`jumbo_peer_silent`, when a peer never answered at all — a liveness fact, and
worded as one. Both name the first offending member id and list every one.

It exists for an application whose commands do not fit the standard ceiling:
without it such a cluster starts, serves, and refuses those commands one at a
time at submit. Default `false` is the safe posture — a cluster on a 1500 B
network behaves exactly as it always did. Env override
`UC2_FORCE_JUMBO_FRAMES` (one of `1`/`true`/`0`/`false`; deploy-varying on
purpose, since the same image runs on a jumbo fabric and on a dev box). **On a
one-node cluster the gate passes immediately**, because there is no peer whose
path could be narrow; it is a multi-node guarantee. See
[Run a cluster on jumbo frames](../how-to/jumbo-frames.md#5-require-a-jumbo-path-at-startup).

Independent of this key, and not configurable: once a cluster has **committed**
a jumbo rung, a node whose path to some member *answers* below it fail-stops at
startup with `path_below_committed_mtu` rather than joining — the log already
holds frames it cannot receive. A member that answers nothing never refuses
anything; the node keeps replicating and voting, and serves once a quorum of
voters (one voter, for a learner) has proven the rung to it — so a single dead
member does not hold a restarted survivor, while a hold that outlasts
discovery is logged every 30 s (`jumbo_join_gate_holding`) and alerted after
5 min (`Uc2JumboGateHeld`).

**`faults: FaultConfig`**
Fault-injection configuration, used by the simulation and test harnesses.

## Environment overrides

Every key below overrides the config file, and **the environment wins**
(<https://12factor.net/config>). An unset variable leaves the file's value
alone; a set one replaces it whether or not the file states it. Overrides are
applied to the parsed document *before* validation, so a bad value is a
startup refusal in exactly the same shape a bad file value is — except the
message names the **variable**, because the file is fine and pointing you at
the TOML key would send you to edit the wrong thing.

They exist so one immutable image can run every node of a cluster: see
[`packaging/compose.yml`](../../packaging/compose.yml), which renders **one**
`node.toml` and **one** `gateway.toml` and varies the rest per container.

| Variable | Overrides | Format |
|---|---|---|
| `UC2_NODE_ID` | `id` | a node id, e.g. `2` |
| `UC2_BIND` | `bind` | `host:port` |
| `UC2_INSTANCE_DIR` | `instance_dir` | a path |
| `UC2_APP_ID` | `app_id` | a string |
| `UC2_MEMBERS` | `[[members]]` | `id@host:port` pairs, comma-separated: `0@10.0.0.1:9100,1@10.0.0.2:9100`. **Replaces** the table, never merges — a membership list must agree cluster-wide, so a half-overridden one is never what anyone means. |
| `UC2_LOG_LEVEL` | `[log] level` | `error` \| `warn` \| `info` |
| `UC2_METRICS_BIND` | `[metrics] bind` | `host:port`. Setting it **creates** the section, so a file with no `[metrics]` still gets an endpoint. |
| `UC2_FORCE_JUMBO_FRAMES` | `force_jumbo_frames` | `1` \| `true` \| `0` \| `false` — nothing else (`2.12.0`). Deploy-varying: the same image runs where the gate is wanted and where it is not. Anything else is refused by name rather than read as `false`. |
| `UC2_GATEWAY_INSTANCE_DIR` | gateway `[local] instance_dir` | a path |
| `UC2_GATEWAY_APP_ID` | gateway `[local] app_id` | a string |
| `UC2_GATEWAY_LISTEN` | gateway `[local] listen` | `host:port` |
| `UC2_GATEWAY_MEMBERS` | gateway `[[members]]` | `node_id@host:port` pairs, comma-separated. Replaces, same reasoning as above. |

**No override carries key material, deliberately.** `crypto.key_path` and the
`[admin]` key paths stay file-based: an environment variable is visible in
`/proc/<pid>/environ`, in `docker inspect`, and to every child process, while
a key *file* is mode-checked (the loaders refuse a group- or world-readable
one). A unit test (`no_env_override_carries_key_material`) fails if anyone
adds one.

**Only deploy-varying keys are overridable.** Tuning values — `buffer_bytes`,
`election_timeout_*`, `journal_segment_bytes` — are part of the build's
behaviour and stay in the file, which is what the twelve-factor page itself
recommends for "config that does not vary between deploys". The cluster-wide
policies under [`[settings]`](#settings) are not overridable for a stronger
reason: they are not per-host at all, and a per-host environment variable that
appeared to change one would be a lie. Change them with
[`uc2ctl settings apply`](uc2ctl.md#settings-apply).

Each override that fires emits a `config_env_override` record naming the
variable and its value, so a value that did not come from the file you are
reading is never silent. These records are emitted *before* `[log] level` is
applied, so they appear even at `warn` — the same never-silenced posture as
the volatile-filesystem and `admin auth = "none"` boot warnings.

## Environment switches

| Variable | Read by | Effect |
|---|---|---|
| `UC2_CLIENT_TIMEOUT_MS` | `uc_client` | Client request timeout, in milliseconds. |
| `UC2_CRYPTO` | test and gate harnesses | `1` boots harness clusters with crypto enabled. Not read by `Node::start`; production nodes are configured through `NodeConfig::crypto`. |
| `UC2_MUTATION` | `uc_node`, `mutation-testing` feature only | Selects an injected consensus bug. Compiled out of the default build. |
| `CARGO_TARGET_TMPDIR` | test harnesses | Root for test instance directories. |
| `UC2_ALLOW_VOLATILE_FS` | `uc_node::preflight` | Any value permits an `instance_dir` on a RAM-backed filesystem. Test and development only, and never silent — the node warns on every boot. |

Harness-only variables that select workload shape — `ELLE_DIR`,
`ELLE_TARGET_OPS`, `ELLE_WORKERS`, `ELLE_MIN_FAULTS`, `ELLE_HOLD_MS`,
`ELLE_BUDGET_SECS`, `ELLE_READ_FRAC`, `ELLE_KEYS`, `ELLE_SEED`,
`ELLE_JAVA_XMX`, `ELLE_VOTE_ORDER_TRIES`, `ELLE_STRICT_MODEL` — are documented
where they are used, in `scripts/elle_check.sh` and `scripts/elle_mutation.sh`.

`ELLE_DIR` and `ELLE_MUT_DIR` must not point at `tmpfs`.

## Crypto material

With `CryptoConfig` enabled, a node reads two files:

| File | Contents |
|---|---|
| private key | 32-byte X25519 private key, mode `0600` |
| allowlist | one `<node-id> <base64-x25519-public-key>` entry per line |

The allowlist is re-read at runtime, rate-limited to once per second, so a
joining member's key can be added without a restart.

## Admin authentication

M12b (`v2.6.0`): who may change cluster membership through `uc2ctl`. Full
walkthrough: [Change cluster membership](../how-to/change-cluster-membership.md).
Wire layout and reason-code table: `docs/superpowers/specs/2026-08-22-uc2-m12-adoptable-design.md`
§5's "As built" amendment.

`[admin]` is required, like `[crypto]` — an absent section refuses to start.
It has no `NodeConfig` field: `uc2-node`'s `main` (`uc_node/src/bin/uc2-node.rs`)
is the one place that turns `[admin]` into a live `AdminPolicy` and hands it
to `Node::start_with(cfg, StartOpts { socket: None, admin })`. `StartOpts`
carries `admin: AdminPolicy` and (separately) an optional pre-bound `socket`
— both are live process resources, not values a `Clone`-able config struct
should carry around. Library callers (`Node::start`, `Node::start_with_socket`)
get `StartOpts::default()`, which is `AdminPolicy::Filesystem` — the pre-M12b
posture, byte-for-byte, so in-process tests and harnesses that never touch
`[admin]` are unaffected.

| Key | Default | Meaning |
|---|---|---|
| `auth` | — (required) | `"hmac"` — every mutating admin request must carry a valid signature; or `"none"` — instance-directory file permissions are the only boundary, the pre-`v2.6.0` posture. |
| `keys` | `[]` | `[{ name, key_path }]`, one entry per admin key this node accepts. Required (≥ 1, unique `name`s) under `auth = "hmac"`; must be empty under `auth = "none"`. |
| `request_ttl_ms` | `30000` | How long a signed request's `expiry_ns` window may extend from the moment `uc2ctl` signs it. Must be `>= 1000` under either mode. This is a **node-side ceiling on the client**: a request's expiry must be `<= now + 2 × ttl` (the doubling absorbs ordinary clock skew), so a `uc2ctl --admin-ttl-secs` wider than that is refused `auth_expired` (22) on arrival, not honoured. |

**Key file rule** (shared with `[crypto]`'s key material,
`uc_crypto::admin::check_key_file_perms`): exactly 32 bytes, mode `0600` —
any group or world permission bit is a startup refusal (`uc2-node`) or a
command refusal (`uc2ctl`) naming the path. Generate one with:

```bash
uc2ctl gen-admin-key /etc/uc2/admin/alice.key
```

which writes 32 random bytes at mode `0600` from the moment the file is
created (no world-readable window) and refuses to overwrite an existing file.

### Request signature

For a non-Rust tool that wants to sign admin requests itself rather than
shelling out to `uc2ctl`: the tag is `HMAC-SHA256(key, canonical_bytes)`,
where `canonical_bytes` is, **every integer little-endian**:

```
u16 LE len(app_id) ‖ app_id bytes ‖ instance_id u128 LE ‖ seq u64 LE
‖ nonce u64 LE ‖ op u32 LE ‖ id u32 LE ‖ ip u32 LE ‖ port u16 LE
‖ expiry_ns u64 LE
```

`app_id` is length-prefixed (not null-terminated or fixed-width) because it
is operator-chosen and variable-length; every other field is fixed-width.
`key_name_hash` — the field that names which key signed the request — is
**not** part of the signed bytes; it is the standard 64-bit FNV-1a hash of
the key's name, computed separately. Source of truth:
`uc_crypto::admin::AdminMessage::canonical_bytes` (fields), `sign`/`verify`
(the HMAC), `fnv1a64` (the name hash) — pinned against a fixed test vector
in `uc_crypto/src/admin.rs`.

**`app_id` is a wrong-cluster guard, not a credential.** `uc2ctl` (and every
IPC attach) checks it against the running node's `app_id` so a request aimed
at the wrong cluster reads as "wrong cluster" rather than a confusing
mid-protocol error — it proves nothing about who is asking, and it is not a
substitute for `[admin]`.

**`auth = "none"` prints a boot-time warning on every start** (never
silenced, same convention as the volatile-filesystem override):

```
uc2-node: WARNING: [admin] auth = "none" — anyone who can write the instance directory can change cluster membership
```

**Residual: the kind-16 peer plane is trusted to `[crypto]`.** A follower
that authenticates an admin request locally forwards it to the leader as a
`ConfigProposal` (wire kind 16) over the node-to-node UDP socket, not the
admin band — the leader cannot re-verify the operator's HMAC signature
against that datagram (the canonical message is bound to the *requesting*
node's cnc page), so what it records is which peer vouched for the change
(`peer:<id>`). The leader drops a kind-16 datagram whose source address
resolves to no current member (`on_config_proposal`'s membership guard,
`uc_node::node`) before any work runs, but with `[crypto].enabled = false`
a network-path adversary who can spoof a member's UDP source address can
still inject a proposal onto that plane. **`[admin] auth = "hmac"` only
authenticates cluster-wide when paired with `[crypto].enabled = true`.**

## Cluster limits

| Limit | Value | Origin |
|---|---|---|
| Total members | 8 | the cnc peer-slot band |
| Membership changes in flight | 1 | single-server change rule |
| Nodes per instance directory | 1 | `instance.lock` |
