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
| `per_conn_inflight` | u32 | `256` | initial credits granted to each connection in `HELLO_OK`; shrinks under backpressure, relaxes back up to this ceiling | `0` → `ZeroPerConnInflight`; greater than `max_inflight` → `PerConnExceedsMax { per_conn, max }` (one connection could exhaust the whole engine window) |
| `request_timeout_ms` | u64 | `10000` | the `Engine`'s per-request deadline; a request that blows it completes `TimedOut` and the client is told `UNKNOWN` | `0` → `ZeroRequestTimeout` |
| `status_interval_ms` | u64 | `200` | a connection with no write for this long gets a standalone `STATUS` — also the edge→client liveness tick, so it must stay well under a client's `dead_after` | `0` → `ZeroStatusInterval` |

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
