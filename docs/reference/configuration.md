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

Field names match the `NodeConfig` fields below. Four differ in shape:

| TOML | Maps to |
|---|---|
| `[[members]]` / `[[learners]]` tables of `id` + `addr` | `Vec<(NodeId, SocketAddr)>` |
| `[purge]` with `below_snapshot_slack_bytes` — absent means disabled | `PurgePolicy` |
| `[crypto]` with `enabled` (required), `key_path`, `allowlist_path`, optional `rotation_interval_ns` / `rotation_bytes` | `CryptoConfig` |
| `[services]` with `ids`, `fsm_lag` (a string) — absent means `ids = [0]` | `ServicesConfig` |

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
they may drift. Optional; **absent means `ids = [0]`** with the default lag
bound. The set is static and must be identical on every node — it is not a
live-reconfiguration surface the way `members` is. See
[Write one config file per host](../how-to/run-a-cluster.md#write-one-config-file-per-host)
for the operational picture and the M14a snapshot-transfer limitation.
Background: [how multi-service works](../notes/uc2-m14-multi-service-explained.md).

| Key | Default | Meaning |
|---|---|---|
| `ids` | `[0]` | The declared service ids, each `0..8`. Must include `0` (the default responder and the only FSM the remote path reaches). |
| `fsm_lag` | `buffer_bytes / 4` | How far `applied` may drift between any two declared FSMs before the admission door closes. A string: `"<n>[KiB|MiB|GiB]"` (e.g. `"16MiB"`, no spaces, no fractions, binary units only) or `"lockstep"` (no FSM starts frame k+1 until every FSM finished frame k). Lockstep costs an N-way cross-core handshake per frame — ~1.6 µs at N=2 on the dev box, i.e. ~600 k frames/s per FSM against ~22 M bounded (`docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md`) — and while a sibling is stalled or dead every other FSM burns ≈ a core yielding on it. |

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
| `log.level` must be `"error"`, `"warn"`, or `"info"` | A silently-ignored typo picking the wrong verbosity. |
| unknown keys inside `[log]`/`[metrics]` are refused, by name, like every other section | M9 accepted anything inside these two sections unvalidated; M10 defines their schema, so a typo there is now caught the same way as everywhere else. |
| `[crypto]` section must be present | M12b (spec §3.3): `enabled` is an **explicit choice**, not absent-means-off like `[purge]` — an absent section is `ConfigError::CryptoChoiceRequired`, so a `node.toml` cannot silently run cleartext by omission. `enabled = false` must not also carry `key_path`/`allowlist_path`; `enabled = true` requires both. |
| `[admin]` section must be present | M12b (spec §3.3, §5.1): `auth` is likewise an explicit choice — an absent section is `ConfigError::AdminChoiceRequired`. `auth = "hmac"` requires at least one uniquely-named entry in `keys`; `auth = "none"` requires `keys` to be empty; `request_ttl_ms` (default 30000) must be `>= 1000` under either mode. |
| `services.ids` must not be empty | M14a: an explicitly-empty list would leave no FSM (not even 0) declared; omit `[services]` entirely for the default `[0]` instead. |
| `services.ids` must not contain a duplicate id | M14a: a repeated id would double-count (or alias) one FSM's slot. |
| `services.ids` entries must be `< 8` | M14a: the cnc page's per-service band holds 8 slots. |
| `services.ids` must include `0` | M14a: FSM 0 is the default responder and the only FSM the remote path reaches; a set without it can never answer a remote client. |
| `services.fsm_lag` must parse | M14a: an unparsable string (wrong suffix, spaces, a fraction) is refused by name rather than silently falling back to the default bound. |
| `services.fsm_lag` must be `> 0` and `< buffer_bytes / 2` | M14a: `0` is the page's lockstep sentinel (write `"lockstep"` instead), and a bound at or above half the buffer cannot provably keep every FSM on the ring (the other half is the appender's overrun margin plus the leader's admission window). |

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
Log ring buffer capacity. Must be a power of two. This much disk is reserved
at startup (see [Instance directory](instance-directory.md#on-disk-footprint));
a node that cannot reserve it refuses to start.

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
Node-to-node wire crypto. `NodeConfig`'s own default is `CryptoConfig::Disabled`
(library callers who build a `NodeConfig` directly, e.g. tests and harnesses,
still get this), but the TOML loader has no default of its own — `[crypto]`
is a required section (see [startup refusals](#startup-refusals) above) — and
the daemon prints nothing extra either way, unlike `[admin]`'s `auth = "none"`
boot warning. The enabled form carries the private key path and the allowlist
path.
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

## Admin authentication

M12b (`v2.6.0`): who may change cluster membership through `uc2ctl`. Full
walkthrough: [Change cluster membership](../how-to/change-cluster-membership.md).
Wire layout and reason-code table: `docs/superpowers/specs/2026-08-22-uc2-m12-adoptable-design.md`
§5's "As built" amendment.

`[admin]` is required, like `[crypto]` — an absent section refuses to start.
It has no `NodeConfig` field: `uc2-node`'s `main` (`uc2_node/src/bin/uc2-node.rs`)
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
`uc2_crypto::admin::check_key_file_perms`): exactly 32 bytes, mode `0600` —
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
`uc2_crypto::admin::AdminMessage::canonical_bytes` (fields), `sign`/`verify`
(the HMAC), `fnv1a64` (the name hash) — pinned against a fixed test vector
in `uc2_crypto/src/admin.rs`.

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
`uc2_node::node`) before any work runs, but with `[crypto].enabled = false`
a network-path adversary who can spoof a member's UDP source address can
still inject a proposal onto that plane. **`[admin] auth = "hmac"` only
authenticates cluster-wide when paired with `[crypto].enabled = true`.**

## Cluster limits

| Limit | Value | Origin |
|---|---|---|
| Total members | 8 | the cnc peer-slot band |
| Membership changes in flight | 1 | single-server change rule |
| Nodes per instance directory | 1 | `instance.lock` |
