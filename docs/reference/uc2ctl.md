# `uc2ctl`

The administrative CLI. It performs live cluster reconfiguration and reports
cluster state.

`uc2ctl` communicates with a running node through the admin band of that node's
`cnc2.dat` control page. It does not open a network socket and it does not read
the replicated log.

To perform a membership change, see
[Change cluster membership](../how-to/change-cluster-membership.md).

## Admin-band commands vs. offline commands

Every command in [Sub-commands](#sub-commands) below (`add-learner` through
`status`) is an **admin-band** command: it needs a running node, reaches it
purely through the cnc page's admin request/response slots, and both
`--instance-dir` and `--app-id` are checked against that live node.

The six commands under [Offline commands](#offline-commands) are different in
kind, not just in name: `backup`, `verify-backup`, and `restore` are
filesystem-only and never touch a running node's cnc admin band at all (the
node may be running throughout a `backup`, since it never talks to it);
`force-single-member` takes the instance directory's exclusive `flock`
directly and **refuses if a node is running**; `gen-admin-key` and `audit`
(M12b, `v2.6.0`) are filesystem-only in the same sense as `backup` — `audit`
even works against an instance directory that has never had a node in it
yet, and `gen-admin-key` doesn't take `--instance-dir` at all, only a
destination path. None of the six accept `--app-id` in the same sense as the
admin-band commands — most don't take it at all (there is nothing on disk to
check it against), and `force-single-member` takes it purely as a typed
confirmation guard, not as anything validated against a live node.

## Synopsis

```
uc2ctl <COMMAND> --instance-dir <DIR> --app-id <ID> [command options]
```

## Common arguments

Every admin-band sub-command takes both.

**`--instance-dir <DIR>`**
The node's on-disk instance directory — the same path passed to `Node::start`.
`uc2ctl` opens `<DIR>/cnc2.dat`.

**`--app-id <ID>`**
Application identity. Must match the running node's `app_id`; the page open
fails otherwise. This is a wrong-cluster guard, not a credential — it is
checked, but it proves nothing about who is asking.

Every **mutating** admin-band command (`add-learner`, `promote`, `demote`,
`remove-learner`, `remove-voter`, and `schedule apply` since 2.11 pending)
additionally takes (M12b, `v2.6.0`):

**`--admin-key <PATH>`**
Sign this request with a named admin HMAC key — a 32-byte, mode-`0600` key
file (see [`gen-admin-key`](#gen-admin-key)). Required whenever the node's
`[admin]` policy is `hmac`; omit to send an unsigned request (accepted
unconditionally under the legacy `Filesystem`/`auth = "none"` policy, same as
before M12b). See [Configuration](configuration.md#admin-authentication).

**`--admin-key-name <NAME>`**
The key's name as loaded into the node's `[admin].keys` list. Defaults to
`--admin-key`'s file stem, so naming the file after the key (e.g.
`ops-alice.key`) needs no separate flag.

**`--admin-ttl-secs <N>`** (default `30`)
How long the signature is valid for, counted from the moment `uc2ctl` signs
it. The node refuses a request outside its own acceptance window with reason
`auth_expired` (22).

**There is a ceiling, set by the node, not by this flag.** The node accepts
an `expiry_ns` only in `(now, now + 2 × request_ttl_ms)` — its own
`[admin].request_ttl_ms`, doubled to leave room for ordinary clock skew.
Ask for more than that and every request is refused `auth_expired` (22)
immediately, which looks exactly like clock skew: with the default
`request_ttl_ms = 30000`, anything above `--admin-ttl-secs 60` is refused
outright. Raise `request_ttl_ms` on the nodes if you genuinely need a longer
window.

## Sub-commands

Sub-commands are: `add-learner`, `promote`, `demote`, `remove-learner`,
`remove-voter`, `schedule apply`, `schedule show`, `status`.

### `add-learner`

Adds a fresh learner. Wire op `1`.

- `--id <U32>` — the new member's node id.
- `--addr <IP:PORT>` — the new member's replication-socket bind address.

Returns the accepted/refused/retry outcome described under
[Response statuses](#response-statuses).

### `promote`

Promotes a caught-up learner to voter. Wire op `2`.

- `--id <U32>`

### `demote`

Demotes a voter to learner. Wire op `3`.

- `--id <U32>`

### `remove-learner`

Permanently removes a learner. The id is tombstoned. Wire op `4`.

- `--id <U32>`

### `remove-voter`

Permanently removes a voter. The id is tombstoned. Wire op `5`.

- `--id <U32>`

### `schedule apply`

Replicated schedule table (2.11 pending). Parse a TOML file of recurrences,
stage it, sign its digest, and apply it. Wire op `6`.

```
uc2ctl schedule apply <FILE.toml> --instance-dir <DIR> --app-id <ID> [--admin-key <PATH>]
```

The TOML is a list of `[[schedule]]` tables. Exactly one of `every` / `at` /
`once` per entry; `every` requires `anchor` and `anchor` requires `every`:

```toml
[[schedule]]
fsm    = "orders"                    # an FSM name from the cluster's [services] names
id     = 1                           # the timer id this FSM's on_timer will see
every  = "1h"                        # ns/us/ms/s/m/h/d suffixes
anchor = "2026-01-01T00:00:00Z"      # RFC 3339, UTC

[[schedule]]
fsm = "orders"
id  = 2
at  = "14:00"                        # HH:MM or HH:MM:SS, daily, UTC

[[schedule]]
fsm  = "kv"
id   = 100
once = "2026-12-24T18:00:00Z"        # fires once, then parks in the table
```

`every` takes an `ns`/`us`/`ms`/`s`/`m`/`h`/`d` suffix and must be > 0; `at` takes
`HH:MM` or `HH:MM:SS`; `anchor` and `once` take RFC 3339 with a `Z`, and a
calendrically invalid date (`2026-02-30`) is refused rather than normalised.

**Applying replaces the whole table.** There is no per-entry add or delete
verb: to remove an entry, apply a file without it; to empty the table, apply a
file with no `[[schedule]]` entries at all. Timer ids share the FSM's id space
with the ids its `apply` schedules programmatically, so reserve a range.

`uc2ctl` resolves every `fsm = "…"` against the node's own cnc name lines
**before** it stages anything, so a typo'd or stale name is refused locally
with the entry index — the request is never written. It then writes the
encoded bytes to `<instance_dir>/schedules.pending` (mode `0600`, fsync,
rename) and sends the admin request carrying that file's digest — the first
ten bytes of its SHA-256 — in the signed `id`/`ip`/`port` fields. Under
`[admin] auth = "hmac"` the table's *contents* are therefore authenticated,
even though the table itself is far too large for the 64-byte admin line.

Three consequences worth knowing:

- **Leader-only.** The staged file is node-local, so a follower cannot forward
  the request and cannot read the leader's file. It answers `retry` (status
  `2`) with the leader hint; re-run against the node the hint names.
- **Single in flight.** The leader also answers `retry` while the previous
  table frame is still above the commit position — the wait is one commit
  round trip. `uc2ctl` does **not** poll through a `retry`: it prints the
  staged file's path and exits non-zero, so re-run the same command.
- **A refused or timed-out apply leaves the staged file in place**, so a retry
  needs nothing re-staged. The node deletes `schedules.pending` only after a
  successful append — which is also what stops a re-presented request from
  appending the same table twice (it then refuses `schedule_missing`).

On success the printed `version` word is the **frame-end position of the new
table**, not a config version — one meaning per op. Every outcome, accepted or
refused, is recorded in `audit.jsonl` as `schedule_apply`; in that record the
`id` and `addr` fields render the digest, not an address.

### `schedule show`

Print the newest **adopted** schedule table from this node's durable state
(`<instance_dir>/state/schedules.state`) — not the staged file, which a
successful apply consumes. Read-only: it writes no admin request.

```
uc2ctl schedule show --instance-dir <DIR> --app-id <ID>
```

```
position=8192 time_ns=1788000000000000000
fsm=orders id=1 rule=every 1h anchor 2026-01-01T00:00:00Z
fsm=orders id=2 rule=at 14:00:00
```

Each entry's `identity_hash` is resolved back to a name through the same cnc
name lines `apply` used to resolve forward; a hash with no matching declared
row prints as `0x…`. A node that has adopted nothing prints
`no schedule table adopted`.

### `status`

Prints the node's current config version and pending state, per-member
peer-slot observability, the per-declared-FSM service table (M14), and the
leader/serving flags. Read-only: it writes no admin request.

- `--admission-bytes <U64>` — override for the staleness warning's admission
  window. Since wire protocol 0.3.0 the node publishes its configured value on
  the cnc page; this flag is needed only against pre-0.3.0 nodes, whose page
  reads `0`.

Output fields:

| Field | Meaning |
|---|---|
| `config` | the adopted config version, whether a change is pending, and — since the schedule table (2.11 pending) — `schedule_position=<n>`, the frame-end position of the adopted schedule table read from `state/schedules.state` (`0` = none adopted). It is the same number `uc2_schedule_table_position` exports and must be identical on every node |
| `leader` | `NODE_FLAG_LEADER` is set |
| `can_serve` | `NODE_FLAG_CAN_SERVE` is set |
| `term` | current term |
| `leader_hint` | the id this node believes leads; `unknown` when the raw value is `u64::MAX` |
| `log: commit / durable / append` | the three log counters, in bytes |
| `members` | one line per occupied peer slot: `id`, `role`, `reported_durable`, and a staleness marker when `commit - reported_durable` exceeds the admission window |
| `services` | the declared id list (cnc 4032's bitmask), the lag policy, and — since log time and timers (2.11 pending) — `log_time_ns=<n>`, the log's clock read from cnc `4048`, in **raw nanoseconds since the Unix epoch**. It is not formatted as RFC 3339: the binary carries no date formatter, and the raw value is what the `uc2_log_time_ns` metric and the cnc word both hold. `0` means no leader has stamped anything this page generation — `fsm_lag=lockstep` or `fsm_lag=<N> bytes` (cnc 4040). A node started for a harness (`ServicesConfig::none_for_tests`) prints `declared=[] fsm_lag=n/a` and no rows: with nothing declared there is no lag policy to report, even though cnc 4040 still holds a resolved bound (since **2.8.1**; earlier releases printed that bound, or `lockstep` when it happened to read 0) |
| per-FSM rows | one line per **declared** row, attached or not, in this order: `row=`, `name=` (the row's declared FSM name, node-written at boot, cnc 3.1), `version=` (the attached service's packed version, or the literal `unversioned` if the packed value is 0 — unattached or an FSM that never set `const VERSION`), `hash=0x...` (the row's identity hash, cnc 3.1), `attached=` (the slot's ATTACHED bit), `epoch=` (incarnations since this node booted), `incarnation=` (the status word's counter), `applied=`, `lag=` (`commit − applied`), `snapshot_pos=`, `heartbeat_age=` (`never` if that FSM has not stamped since boot), `timers_pending=` (that row's pending scheduled timers, cnc slot line 7 `+488`) — `name=`/`version=`/`hash=` are new since FSM identity and `timers_pending=` since log time and timers, both 2.11 pending; earlier releases printed only `attached=... epoch=... incarnation=...` |

## Offline commands

`backup`, `verify-backup`, `restore` (M11 Task 2), `force-single-member`
(M11 Task 4), and `gen-admin-key`/`audit` (M12b, `v2.6.0`). See
[the offline-vs-admin-band distinction](#admin-band-commands-vs-offline-commands)
above. Task walkthroughs: [Back up a cluster](../how-to/back-up-a-cluster.md),
[Recover from quorum loss](../how-to/recover-from-quorum-loss.md),
[Change cluster membership](../how-to/change-cluster-membership.md#if-the-cluster-requires-signed-admin-requests).

### `backup`

Ordered-copy an instance directory's durable `journal/` → `state/` →
`snapshots/<id>/` (one directory per declared FSM id since M14) into a fresh
backup artifact, then runs `verify-backup` on the copy and writes a
`MANIFEST`. Filesystem-only; the node may be running throughout.

- `--instance-dir <DIR>` — the node's on-disk instance directory.
- `--out <DIR>` — destination for the new artifact. Refused if it already
  exists and is non-empty (`ArtifactExists`).

Prints the resulting report as `key=value` lines: `journal_first_base`,
`journal_last_pos`, one `newest_snapshot.<id>` (or `none`) line per FSM id
present, the aggregate `newest_snapshot` (the min over ids present, or
`none`), `snapshot_floor`, `healed_torn_tail`, `files`. Nonzero exit on error.

### `verify-backup`

Read-only verification of a backup artifact — recovers its positions, checks
the coverage invariant **per FSM id** present in `snapshots/<id>/` (a journal
whose `first_base > 0` must be covered by that id's own retained snapshot,
else `Hole { service: <id>, .. }`), and cross-checks a `MANIFEST` if present
(`ManifestMismatch` on tamper/bitrot). May heal the artifact's own torn
active-segment tail (a shrink-only truncate, reported as
`healed_torn_tail=true`, which is the routine case under segment
preallocation, not by itself a sign of trouble); everything else is
read-only. Refuses (`LooksLikeLiveInstanceDir`) if the target's
`instance.lock` is currently held by a running node — the truncate heal
against a live writer's active segment is a narrow, real acked-write-loss
race, not something a typo'd path should be able to trigger. A stopped
node's own instance directory, lock file leftover and all, verifies fine in
place.

```
uc2ctl verify-backup <ARTIFACT>
```

Same `key=value` report as `backup`. Nonzero exit on error, including `Hole`
and `ManifestMismatch`.

### `restore`

Verify a backup artifact, then copy its three durable directories into a
fresh instance directory.

```
uc2ctl restore <ARTIFACT> --instance-dir <DIR>
```

Runs `verify-backup` on `<ARTIFACT>` first (so `<ARTIFACT>` also gets the
`LooksLikeLiveInstanceDir` refusal above — a live node's own instance dir is
never a valid artifact to restore from). Refuses (`TargetNotEmpty`) if
`--instance-dir`'s `journal/`, `state/`, or `snapshots/` already contain
anything — restore never merges or overwrites. Volatile leftovers
(`cnc2.dat`, `log.buf`, rings, `instance.lock`) are fine. `--instance-dir`
itself is also refused with `LooksLikeLiveInstanceDir` if a node currently
holds its `instance.lock` — belt-and-suspenders for the narrow window where a
just-booted node's durable subdirectories are still empty (so
`TargetNotEmpty` alone would not catch it). Same `key=value` report as
`backup` (the artifact's positions, not a property re-derived from the new
instance directory).

### `force-single-member`

Quorum-loss recovery: forces the given `--instance-dir` to a single-voter
cluster naming `--node-id` the sole member. OFFLINE like the three commands
above, but the refusal mechanism differs — it takes the instance's exclusive
`flock` directly, so **a running node refuses it**, rather than there being
nothing on disk to conflict with.

```
uc2ctl force-single-member --instance-dir <DIR> --app-id <A> --node-id <N> --confirm-cluster <A>
```

- `--instance-dir <DIR>` — the surviving node's own instance directory. Must
  not have a node currently running in it.
- `--app-id <ID>` — the cluster's application identity, printed in the
  data-loss statement.
- `--node-id <U32>` — the surviving node's own id. Must be a voter or learner,
  and not tombstoned, in the recovered config.
- `--confirm-cluster <ID>` — must equal `--app-id` exactly, or the command
  refuses before touching anything. A typed second confirmation, not a
  network-facing identity check.

Refuses without writing anything if: a node is running; `--confirm-cluster`
doesn't match `--app-id`; no durable config record exists yet (a never-booted
instance dir); the "doubly-ahead" crash window applies (two config adoptions
durably persisted in the same crash, before any archive catch-up — nothing
genuine left to revert to); `--node-id` is tombstoned; or `--node-id` is not a
member of the recovered config.

On success, prints the exact data-loss statement first, then the resulting
report (`old_version`, `new_version`, `durable`, `dropped_peers`). Every peer
but the survivor is **dropped from the config, not tombstoned** — they
wipe-and-rejoin later as fresh learners; see
[Recover from quorum loss](../how-to/recover-from-quorum-loss.md). A one-way
door: there is no "undo" verb.

### `gen-admin-key`

M12b (`v2.6.0`): generate a fresh named admin HMAC key file — 32 random
bytes, mode `0600` from the moment the file is created (no world-readable
window), refuses to overwrite an existing file. OFFLINE — writes only the
named file, no cnc admin-band interaction.

```
uc2ctl gen-admin-key <PATH>
```

Prints the `[admin]` snippet to paste into a node's config, naming the key
after `<PATH>`'s file stem:

```
wrote /etc/uc2/admin/alice.key
paste into the node's config file:
[admin]
keys = [{ name = "alice", key_path = "/etc/uc2/admin/alice.key" }]
```

### `audit`

M12b (`v2.6.0`): print a node's admin audit log
(`<instance_dir>/audit.jsonl`). OFFLINE — reads the file directly, no cnc
admin-band interaction; works whether or not a node is currently running.

```
uc2ctl audit --instance-dir <DIR> [--tail <N>] [--json]
```

- `--tail <N>` — print only the last `N` records (default: the whole file).
- `--json` — print each record's raw JSON line instead of the summarized,
  human-readable form (`<ts>  <actor>  <origin>  <op_name>  <id>  <addr>
  <outcome>(<reason>)  cfg=<version>`).

A line the summarizer cannot make sense of — a torn write from a crash
mid-record, or a hand-edited file — prints as-is prefixed `? ` rather than
being dropped or panicking; the whole point of this file is to never lose a
record silently. See
[Change cluster membership](../how-to/change-cluster-membership.md#read-the-audit-log)
and [Instance directory](instance-directory.md) for the file's durability
class (`O_APPEND`, `fsync` per record, no rotation).

## Response statuses

Mutating commands write an admin request and poll for the response line.
The status below is a **printed value**, not the process exit code.
`uc2ctl` exits `0` on success and non-zero on failure: `1` for any runtime
failure (the single `process::exit(1)`), `2` for a command-line usage error
(clap) — separately from the table's own `0`/`1`/`2` (see
[`semver-policy.md`](semver-policy.md)).

| Status | Printed as | Process outcome |
|---|---|---|
| `0` | `accepted: config version now <N>` | exit 0 |
| `1` | `refused: <reason>` | exit 1 (runtime failure) |
| `2` | `retry: leader unknown or the append ring was momentarily full` | exit 1 (runtime failure) |

`schedule apply` uses the same three statuses with its own wording, because
its `version` word is a schedule position rather than a config version and
because its `retry` has a second cause:

| Status | Printed as | Process outcome |
|---|---|---|
| `0` | `applied: position=<N>` | exit 0 |
| `1` | `refused: <reason> (schedule position <N>) — staged file kept at <PATH>` | exit 1 |
| `2` | `retry: leader unknown or a previous table is still uncommitted (schedule position <N>) — staged file kept at <PATH>, try again` | exit 1 |

## Refusal reasons

The `reason` field of a status-`1` response. Codes 1–10 and 12 are the
discriminants of `uc_consensus::config::ProposeError`; 11 is the node's own
defensive catch-all; 20–24 (M12b, `v2.6.0`) are admin-authentication
refusals (`uc_node::REASON_AUTH_*` / `REASON_AUDIT_FAILED`) — produced only
under `[admin] auth = "hmac"`, and disjoint from the `ProposeError` band so a
caller can tell "the cluster refused this change" from "the cluster refused
to believe this was you" without consulting the policy. 40–43 (2.11 pending)
are `schedule apply`'s own refusals (`uc_node::REASON_SCHEDULE_*`), in their
own band for the same reason.

| Code | Reason |
|---|---|
| 1 | `NotLeader` |
| 2 | `NotServing` — a change is still settling |
| 3 | `ChangePending` — one membership change in flight at a time |
| 4 | `Tombstoned` — the id was permanently removed and cannot rejoin |
| 5 | `AlreadyPresent` |
| 6 | `NotFound` |
| 7 | `WrongRole` — promoting a voter, or demoting a learner |
| 8 | `ZeroVoters` — the change would leave the cluster with no voters |
| 9 | `TooManyMembers` — the 8-member cap |
| 10 | `NotCaughtUp` — the learner is too far behind commit to promote safely |
| 11 | malformed or unknown op — the node did not recognise the request |
| 12 | `SelfDemote` — a leader cannot demote itself |
| 20 | `auth_missing` — the node requires a signed request: pass `--admin-key` |
| 21 | `auth_bad_tag` — wrong key, a stale auth line, or a tampered request |
| 22 | `auth_expired` — the signature's window is past, or stretched implausibly far into the future; check clock skew |
| 23 | `auth_unknown_key` — this key name is not in the node's `[admin].keys` |
| 24 | `audit_failed` — the node could not record the request (a full or failing disk) and refused rather than act unaccountably; **not** "nothing happened" — check `uc2ctl status` |
| 40 | `schedule_digest` — the staged file's digest is not the one the request signed: a different file was staged than was signed, or it changed in between. Re-run `schedule apply` |
| 41 | `schedule_missing` — no staged file on this node. Either `schedule apply` was run against a different instance directory, or a successful apply already consumed it |
| 42 | `schedule_decode` — the staged file is not a decodable schedule table (or is longer than a full 32-entry one) |
| 43 | `schedule_unknown_fsm` — an entry names an FSM that is not one of this node's declared rows. The **whole** table is refused, never partially adopted: a typo'd name would otherwise leave an operator believing a timer is armed that no row will ever fire |

Code `0` is not a `ProposeError`. It is the CLI's own malformed-op sentinel.

## Timeouts and limits

| Limit | Value |
|---|---|
| Response poll timeout | 10 s |
| Response poll interval | 20 ms |
| Total cluster members | 8 (voters plus learners) |
| Membership changes in flight | 1 |

On timeout, `uc2ctl` reports that a newer admin request may have superseded
this one. `uc2ctl status` shows the authoritative config version.

## Concurrency

One admin client per instance directory at a time. This includes `uc2ctl`,
`m7_gate`, and any direct caller of `write_admin_req`. Concurrent invocations
against the same instance directory may produce a malformed request.
