# Configuration

The knobs a node and a service are constructed with, plus the environment
switches the workspace reads.

Field-level API documentation for these types is generated:
[`NodeConfig`](https://peterknego.github.io/ultima_cluster/uc2_node/struct.NodeConfig.html)
and the `uc2_service` config types. This page states the surface, its defaults,
and its limits.

## `NodeConfig`

Passed to `Node::start`.

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
