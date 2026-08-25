# `gateway.toml` reference

Every key `uc2-gateway --config gateway.toml` reads, its default, and what
makes it a startup refusal. Unknown keys are refused (`deny_unknown_fields`
on every section, top level included) — a typo can never be silently
ignored, the same posture `uc2_node`'s `node.toml` loader takes.

Loading is two steps: `config_file::load_from_path` deserialises the TOML
(defaulting anything absent) into an `EdgeConfig`, then
`EdgeConfig::validate` runs the semantic checks. Both stages produce a named
error, never a panic or a silent default.

## `[local]` — required

| Key | Type | Meaning | Refusal |
|---|---|---|---|
| `instance_dir` | path | the co-located node's instance directory — the same `instance_dir` that node's `node.toml` points at | empty → `MissingInstanceDir` |
| `app_id` | string | must match the node's `app_id` exactly, and the value clients send in `HELLO` | empty → `MissingAppId` |
| `listen` | socket addr | where remote clients connect, e.g. `"0.0.0.0:9200"`. Port `0` binds an ephemeral port (read it back from `Edge::local_addr`) | must parse as a `SocketAddr`; a parse failure is a TOML deserialisation error, caught before validation runs |

## `[[members]]` — required, at least one

The node-id → gateway-address map, keyed by cluster node id — how
`REDIRECT` and `LEADER_CHANGED` tell a client where to reconnect. The cnc
page carries node ids and roles but no addresses, so this table is stated
out of band (the same shape as Aeron's `ingressEndpoints` string). **Keep it
byte-identical across every host's `gateway.toml`** — a client that lands on
an edge with a different map gets sent somewhere another edge doesn't
recognize.

| Key | Type | Meaning | Refusal |
|---|---|---|---|
| `node_id` | u32 | a cluster member's node id | listed twice → `DuplicateMember(id)` |
| `gateway` | string | that node's gateway `host:port`, as a client would dial it | empty → `EmptyGateway(id)` |

An empty `[[members]]` table (no entries at all) → `NoMembers`.

## `[limits]` — optional; every field inside is independently optional

Absent section = every field takes its default; a present section with only
some fields set takes the defaults for the rest.

| Key | Type | Default | Meaning | Refusal |
|---|---|---|---|---|
| `max_inflight` | u32 | `4096` | the local `Engine`'s inflight window, shared across every connection on this edge | `0` → `ZeroMaxInflight` |
| `per_conn_inflight` | u32 | `256` | credits granted to each connection at `HELLO_OK` — **an equal share of the edge's budget**, capped at this value; shrinks under backpressure and relaxes back up to the current share | `0` → `ZeroPerConnInflight`; greater than `max_inflight` → `PerConnExceedsMax { per_conn, max }`; greater than the **grant budget** (`max_inflight` less its 1/8 headroom) → `PerConnExceedsBudget { per_conn, budget, max_inflight }` |
| `request_timeout_ms` | u64 | `10000` | the `Engine`'s per-request deadline; a request that blows it completes `TimedOut` and the client is told `UNKNOWN`. **Also a client's exposure window when this host's node has died and the supervisor has not stopped the gateway yet** — consider `2000` for a gateway, see [the how-to](../how-to/run-a-gateway.md#when-the-node-underneath-dies) | `0` → `ZeroRequestTimeout` |
| `status_interval_ms` | u64 | `200` | a connection with no write for this long gets a standalone `STATUS` — also the edge→client liveness tick, so it must stay well under a client's `dead_after` | `0` → `ZeroStatusInterval` |
| `max_connections` | u32 | `1024` | hard ceiling on simultaneously-open client connections; each costs a reader thread and a socket. Over it, the acceptor answers `HELLO_REFUSED{BUSY}` (reason `4`) without spawning a reader, counted as `EdgeStats::refused_busy` — a conforming client treats `BUSY` like `FAULTED` and tries the next member | `0` → `ZeroMaxConnections` |

### The grant budget (2.7.0)

The edge holds **one** `Engine` inflight window (`max_inflight`) and divides
it across its connections instead of promising each one the same constant.
Two derived numbers:

- **budget** = `max_inflight` − `max_inflight / 8`. The 1/8 headroom is not a
  tuning dial and is not configurable: it is the slack that absorbs frames
  already on the wire when a grant shrinks.
- **grant** = `clamp(budget / live_connections, 1, per_conn_inflight)`, where
  `live_connections` counts the handshaken connections on this edge.

At the defaults that is a budget of `3584` and a grant of `256` (the cap)
for the first fourteen connections, `255` at fifteen, and so on down. A
connection is told its grant in `HELLO_OK`, and every later change reaches
it as an absolute `credits` value: a **reduction** is pushed as a standalone
`STATUS` before it can send into the smaller window, an **increase** rides
the next `RESPONSE` or the idle `STATUS` tick. `uc2_gateway::budget_for` and
`uc2_gateway::grant_for` are public if you would rather compute than read.

**What this replaces.** In `2.6.0` every connection was granted
`per_conn_inflight` in full and nothing counted the sum, so N connections
could promise N × 256 against a 4096-slot window and the only arbiter was
the `Engine` refusing submits — the reactive halve/relax ladder, per
connection, uncoordinated. That gap is closed; see
[the correction note](../notes/uc2-m12a-edge-flow-control-gap.md).

**The one case the budget does not cover:** past `live > budget` every grant
floors at `1` and the sum exceeds the budget again. `validate` **warns**
(it does not refuse — a floor of 1 still works, it is just miserable) when
`max_connections > budget`; the binary prints the warning at startup. Size
`max_connections` at or under the budget, or raise `max_inflight`.

## `[session]` — optional; its one field is independently optional

| Key | Type | Default | Meaning |
|---|---|---|---|
| `envelope` | bool | `true` | prepend the 16-byte LE `client_id ++ seq` header to `SUBMIT` payloads and lift the `Sessioned` tag off the response into `RESPONSE` flags, so a re-sent write is answered `replayed` instead of applied twice. `false` = raw pass-through (Aeron parity; dedup becomes the application's problem) |

`envelope` maps to `EdgeConfig::session_envelope`.

## What `EdgeConfig::validate` does **not** check

`listen`'s address is already a parsed `SocketAddr` by validation time — a
bad string is a TOML parse error, not a `validate` refusal. Whether the
instance directory actually exists, or whether `listen` can actually be
bound, is discovered later by `Edge::start` (`EdgeError::Attach` /
`EdgeError::Bind`) — those are runtime failures of a config that was
otherwise well-formed, not `ConfigError` values.

## Full example

See `packaging/gateway.example.toml` for a complete, commented file — every
optional key shown at its default, ready to copy as
`/etc/uc2/gateway.toml`.

## Exit codes (`uc2-gateway` binary)

| Code | Meaning |
|---:|---|
| `2` | the config file could not be read/parsed, or `EdgeConfig::validate` refused it by name. `packaging/systemd/uc2-gateway.service` sets `RestartPreventExitStatus=2`: a bad config is an operator problem a restart loop cannot fix. |
| `1` | `Edge::start` failed (the node's instance directory doesn't exist yet, its listener couldn't bind, …), **or** the edge later latched `faulted` (its node's shmem instance restarted underneath it) and the main loop exits to let systemd bring up a fresh gateway against the new node instance. |
| `0` | clean stop on `SIGTERM`/`SIGINT`. |
