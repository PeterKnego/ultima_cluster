# Configuration

The knobs a node and a service are constructed with, plus the environment
switches the workspace reads.

Field-level API documentation for these types is generated:
[`NodeConfig`](https://peterknego.github.io/ultima_cluster/uc2_node/struct.NodeConfig.html)
and the `uc2_service` config types. This page states the surface, its defaults,
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

Field names match the `NodeConfig` fields below. Three differ in shape:

| TOML | Maps to |
|---|---|
| `[[members]]` / `[[learners]]` tables of `id` + `addr` | `Vec<(NodeId, SocketAddr)>` |
| `[purge]` with `below_snapshot_slack_bytes` — absent means disabled | `PurgePolicy` |
| `[crypto]` with `key_path`, `allowlist_path`, optional `rotation_interval_ns` / `rotation_bytes` — absent means cleartext | `CryptoConfig` |

Two keys exist only in the file and have no `NodeConfig` field:

**`seed`** — optional. Defaults to a distinct per-id value. Identical seeds
across nodes make every member time out at the same instant and split the vote.

**`allow_volatile_fs`** — optional, default `false`. Test and development only;
see [startup refusals](#startup-refusals).

### Reserved sections

Unknown keys are a startup refusal (`deny_unknown_fields`), with exactly two
exceptions. `[log]` and `[metrics]` are **reserved for M10** and are accepted
today so a config written for a later release does not refuse to start on this
one. They have **no effect here**, and the node prints a `NOTE` naming any that
are present on every boot — a section that does nothing must never look like a
section that works.

Their contents are not validated: this release does not define their schema, so
any keys inside them are accepted as-is. Every other unknown table is still
refused by name.

## Startup refusals

`uc2-node` refuses to start, naming the field, rather than failing later in a
way that looks like something else. Each rule exists because of the failure it
replaces:

| Refusal | What it prevented |
|---|---|
| `bind` must equal this node's own `members` entry | A leader elects, but followers never advance `durable` or `commit` — datagrams arrive from a source address matching no member. |
| `instance_dir` must not be on a RAM-backed filesystem | Every `fsync` is a silent no-op; the cluster appears to work and loses committed data on power loss. |
| `max_payload` must fit one datagram | A max-size frame plus headers and any crypto tag must fit the MTU; the node does not fragment. Oversized values panic inside the sender at construction. |
| `buffer_bytes` must be a power of two | Ring geometry. |
| `max_payload` must be well under `buffer_bytes` | A payload approaching the ring size cannot be buffered for retransmit. |
| this node's `id` must appear in `members` or `learners` | A node not in its own cluster. |
| `members` and `learners` must be disjoint, ids unique | Ambiguous role and peer-band aliasing. |
| at most 8 members total | The control page's per-peer band holds 8 slots; enforced on the wire too. |
| `election_timeout_min_ns` < `election_timeout_max_ns` | An empty randomisation window. |

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
`FRAME_TYPE_CONFIG` stream own membership, and this field is ignored. A restart
with an edited `members` list has no effect.

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
Log ring buffer capacity. Must be a power of two.

**`max_payload: usize`**
Maximum payload size.

**`admission_bytes: u64`**
Ingress admission budget in bytes — the `append - commit` backpressure gate.
Published on the cnc page at offset 3712 since wire protocol 0.3.0.

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
To turn it on, see [Keep the journal from growing without bound](../how-to/bound-journal-growth.md).

**`crypto: CryptoConfig`**
Node-to-node wire crypto. Default `CryptoConfig::Disabled`. The enabled form
carries the private key path and the allowlist path.
To turn it on, see [Encrypt traffic between nodes](../how-to/encrypt-node-traffic.md).

**`faults: FaultConfig`**
Fault-injection configuration, used by the simulation and test harnesses.

## Environment switches

| Variable | Read by | Effect |
|---|---|---|
| `UC2_CLIENT_TIMEOUT_MS` | `uc2_client` | Client request timeout, in milliseconds. |
| `UC2_CRYPTO` | test and gate harnesses | `1` boots harness clusters with crypto enabled. Not read by `Node::start`; production nodes are configured through `NodeConfig::crypto`. |
| `UC2_MUTATION` | `uc2_node`, `mutation-testing` feature only | Selects an injected consensus bug. Compiled out of the default build. |
| `CARGO_TARGET_TMPDIR` | test harnesses | Root for test instance directories. |
| `UC2_ALLOW_VOLATILE_FS` | `uc2_node::preflight` | Any value permits an `instance_dir` on a RAM-backed filesystem. Test and development only, and never silent — the node warns on every boot. |

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

## Cluster limits

| Limit | Value | Origin |
|---|---|---|
| Total members | 8 | the cnc peer-slot band |
| Membership changes in flight | 1 | single-server change rule |
| Nodes per instance directory | 1 | `instance.lock` |
