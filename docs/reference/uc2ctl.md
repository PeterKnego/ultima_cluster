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
`status`, except the read-only `cluster` and `catalog`, which dial the node's
`obs.sock` or read the newest cluster artifact) is an **admin-band** command: it needs a running node, reaches it
purely through the cnc page's admin request/response slots, and both
`--instance-dir` and `--app-id` are checked against that live node.

The commands under [Offline commands](#offline-commands) are different in
kind, not just in name: `backup`, `verify-backup`, and `restore` are
filesystem-only and never touch a running node's cnc admin band at all (the
node may be running throughout a `backup`, since it never talks to it);
`force-single-member` takes the instance directory's exclusive `flock`
directly and **refuses if a node is running**; `gen-admin-key` and `audit`
(M12b, `v2.6.0`) are filesystem-only in the same sense as `backup` — `audit`
even works against an instance directory that has never had a node in it
yet, and `gen-admin-key` doesn't take `--instance-dir` at all, only a
destination path. The backup tier's `tier show`, `tier verify`, `tier
restore`, `tier fork` and `tier export-corpus` (`0.11.0`, since `2.14.0`) read a
tier's files directly and never a node's admin band; `tier restore` and `tier
fork` refuse an instance directory a running node holds. None of these accept `--app-id` in the same sense as the
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
`remove-learner`, `remove-voter`, and — since 2.11.0 — `schedule apply`,
`settings apply`, `snapshot`, `snapshot fetch`, and — since the FSM upgrade
lifecycle — `upgrade pin`)
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
`remove-voter`, `schedule apply`, `schedule show`, `settings apply`,
`settings show`, `snapshot`, `snapshot fetch`, `snapshot show`,
`upgrade pin`, `upgrade show`, `tier forget`, `status`, plus the read-only
`cluster` and `catalog` (which read the node's observability socket, not the
admin band).

Every mutating one is a **cluster-FSM command** since 2.11.0: the
reconfiguration ops append `CLUSTER kind = 1` (Membership), `schedule apply`
appends `CLUSTER kind = 2`, `settings apply` appends `CLUSTER kind = 3`, and
— since the FSM upgrade lifecycle — `upgrade pin` appends `CLUSTER kind = 4`
(`UpgradePin`), and `tier forget` appends `CLUSTER kind = 7`
(`BackupWatermark`, a `Forget`). The operator-facing surface is unchanged; what changed is
that one internal state machine now applies all of these at commit
([the cluster FSM explainer](../notes/uc2-cluster-fsm-explained.md)).
**Single in flight, across all of them**: the leader answers `retry` while
any previous `CLUSTER` command is still above the committed cluster view —
so a `settings apply` will retry behind an in-flight membership change, an
`upgrade pin` behind an in-flight `settings apply`, and vice versa.

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

Replicated schedule table (2.11.0). Parse a TOML file of recurrences,
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
- **Single in flight, across every cluster command.** The leader also answers
  `retry` while the previous `CLUSTER` command — a table, a settings record or
  a membership change — is still above the committed cluster view; the wait is
  one commit round trip. `uc2ctl` does **not** poll through a `retry`: it
  prints the staged file's path and exits non-zero, so re-run the same
  command.
- **A refused or timed-out apply leaves the staged file in place**, so a retry
  needs nothing re-staged. The node deletes `schedules.pending` only after a
  successful append — which is also what stops a re-presented request from
  appending the same table twice (it then refuses `schedule_missing`).

On success the printed `version` word is the **frame-end position of the new
table**, not a config version — one meaning per op. Every outcome, accepted or
refused, is recorded in `audit.jsonl` as `schedule_apply`; in that record the
`id` and `addr` fields render the digest, not an address.

### `schedule show`

Print the **committed** schedule table — not the staged file, which a
successful apply consumes. Read-only: it writes no admin request. It reads the
running node's live [cluster view](cluster-view.md) over
`<instance_dir>/obs.sock`; when no node runs it reads the newest cluster
artifact (`<instance_dir>/snapshots/cluster/snap-<pos>.ultcluster`) and says
`offline: newest artifact at <P>` on **stderr**, leaving stdout unchanged. With
neither a node nor an artifact it prints `no cluster artifact yet (the table is
committed on the log; an artifact appears once every declared row has
snapshotted)`, rather than implying an empty table was adopted. The text is
rendered from the view's `cluster.schedule`, so it is the same live or offline.

```
uc2ctl schedule show --instance-dir <DIR> --app-id <ID>
```

```
position=8192
fsm=orders id=1 rule=every 1h anchor 2026-01-01T00:00:00Z
fsm=orders id=2 rule=at 14:00:00
```

Each entry's `identity_hash` is resolved back to a name through the same cnc
name lines `apply` used to resolve forward; a hash with no matching declared
row prints as `0x…`.

### `settings apply`

The cluster's **replicated settings record** (2.11.0, cluster-FSM spec
§6). Wire op `7`. `schedule apply`'s shape verbatim: parse a TOML file, encode
it, stage it at `<instance_dir>/settings.pending` (mode `0600`, fsync, rename),
and sign the first ten bytes of its SHA-256 into the request's `id`/`ip`/`port`
fields.

```
uc2ctl settings apply <FILE.toml> --instance-dir <DIR> --app-id <ID> [--admin-key <PATH>]
```

Every key is optional; an absent key keeps the record's "derive at use"
default rather than inventing a value:

```toml
admission_bytes         = 262144       # 0 / absent = derive (256 KiB)
fsm_lag                 = "16MiB"      # or "lockstep"; absent = derive (buffer_bytes / 4)
snapshot_interval_bytes = 0            # 0 = on demand only
snapshot_target         = "all"        # "all" or "learners"
retain_sets             = 3            # 0.11.0, the snapshot catalog; absent = 1 (today's newest-only)
auto_fetch              = true         # bool; absent = true (every node fetches the newest agreed set)
backup_retain_sets      = 0            # u16; absent/0 = a backup tier keeps every set
backup_retain_age       = "30d"        # "<n>(s|m|h|d)"; absent/0 = no age rule
purge_requires_tiers    = 0            # 0..=4 (3b); absent/0 = off; K = purge waits for K tiers' through
wire_raise              = "auto"       # "auto" or "manual" (2.15.0); absent = keep the committed mode
```

An unknown key is refused locally by name, before anything is staged. The same
three consequences `schedule apply` carries apply here: **leader-only** (the
staged file is node-local, so a follower answers `retry` with the leader hint),
**single in flight** across every `CLUSTER` command, and **a refused or
timed-out apply leaves the staged file in place** so a retry needs nothing
re-staged.

These are cluster-wide values, applied at one log position on every node.
`[settings]` in `node.toml` seeds them at genesis only; the old per-host
spellings (top-level `admission_bytes`, `[services] fsm_lag`) are startup
refusals pointing here. See
[Configuration § `[settings]`](configuration.md#settings).

On success the printed `version` word is the **frame-end position of the new
settings record**. Every outcome is recorded in `audit.jsonl` as
`settings_apply`, with the digest rendered in the `id`/`addr` fields.

### `settings show`

Print the **committed** settings record. Read-only: it writes no admin request,
and it reads the live [cluster view](cluster-view.md) when a node runs and the
newest cluster artifact (with `offline: newest artifact at <P>` on stderr)
when it does not, exactly as [`schedule show`](#schedule-show) does.

```
uc2ctl settings show --instance-dir <DIR> --app-id <ID>
```

```
position=8192 admission_bytes=262144 fsm_lag=16MiB snapshot_interval_bytes=0 snapshot_target=all retain_sets=1 auto_fetch=true backup_retain_sets=0 backup_retain_age_secs=0 purge_requires_tiers=0 wire_level=12 wire_raise=auto datagram_mtu=0 (baseline)
```

`fsm_lag` renders as `default` (the record's `0`), `lockstep`, a whole-MiB
count, or a raw byte count when it is neither. `auto_fetch` (bool, absent = `true`) is whether every node fetches the newest agreed set in the background. `retain_sets` (`0.11.0`, the
snapshot catalog, since `2.14.0`) is how many agreed snapshot sets the cluster
keeps, with the journal from the oldest of them, so each is a start point;
`1` is the default and matches today's newest-only retention.
`backup_retain_sets` and `backup_retain_age_secs` (backup tier, `0.11.0`,
since `2.14.0`) bound how long a backup tier keeps its sets (`0` = no rule); the
file spells the age as `backup_retain_age = "30d"`.
`purge_requires_tiers` (backup tier 3b, since `2.14.0`) is K, the number of
backup tiers whose confirmed `through` every node's purge waits for (`0` =
off, the default; `1..=4`); `settings show` prints it before `datagram_mtu`.
See [Back up to a tier § Backup-gated purge](../how-to/back-up-to-a-tier.md#backup-gated-purge).
`wire_level` and `wire_raise` (since `2.15.0`) are the committed cluster wire
level and who raises it: `auto` (the leader, once every member reports the
next level) or `manual` ([`uc2ctl wire raise`](#wire-raise)); `keep` appears
only for a record that leaves the committed mode alone. `wire_raise` is a
file key (absent = keep); `wire_level` is **leader-owned** like
`datagram_mtu` — naming it in a `settings apply` file is refused as an
unknown key. `uc2ctl settings show` prints both after `purge_requires_tiers`.
`datagram_mtu` (2.12 pending)
is the committed datagram rung: `0 (baseline)` is the `MTU_DEFAULT` every
cluster starts from, and a rung the leader's path-MTU discovery committed
prints as `(discovered)`. It is **leader-owned** — naming it in a `settings
apply` file is refused as an unknown key, and the cluster FSM keeps the
committed rung monotone, so an ordinary apply can never lower it. With no
node running and no cluster artifact it prints `no cluster artifact yet`.

### `snapshot`

Command a **coordinated snapshot instant** (2.11.0, coordinated-snapshot
spec §5). Wire op `8`, leader-only. The leader appends a `SNAPSHOT` frame; its
frame-end position **P** is the instant, and every declared row plus the
cluster FSM freezes there, having applied everything below it. When they have
all published `snap-<P>`, the node holds the **complete set at P** and its
purge floor moves to P.

```
uc2ctl snapshot --instance-dir <DIR> --app-id <ID> [--standby] [--admin-key <PATH>]
```

- `--standby` — only **learners** freeze. A voter's rows yield the frame like
  any other node-only frame, so no voter pays the freeze; the set comes back
  to a voter later with [`snapshot fetch`](#snapshot-fetch). Refused
  `49 snapshot_no_learner` when the committed membership holds no learner.

This op stages nothing and signs no digest — the whole command is the
`--standby` bit, which rides the request's `id` field — so the `ip`/`port`
fields are unused. On success the printed `version` word is **P**:

```
instant=73792
```

Two refusals are specific to it. `48 snapshot_unsupported` names a declared
row that lacks the snapshot capability bit — since #67 every Rust SDK
service gets it automatically (`ServiceBuilder::start()` requires `S:
SnapshotStateMachine`), so this is the backstop for a row attached outside
the SDK: it would ignore the frame and the set could never complete; the
audit record's `detail` field names the row. `49 snapshot_no_learner` is `--standby` with no learner. A **follower**
answers `retry` (status `2`), exactly as `schedule apply` does — check
`uc2ctl status`'s `leader_hint` for where to re-run it — and so does a leader
still opening its term. A leader whose previous instant's set has not
completed answers `retry` to the *cadence*, but never to this verb: an
operator's command always supersedes.

Audited as `snapshot`, with `id` carrying `--standby` and `config_version`
carrying P (or the newest complete set's position, on a refusal).

### `snapshot fetch`

Pull a learner's complete set onto **this** node, store-only (2.11.0,
spec §5.7 item 5). Wire op `9`. **Node-local**: it is never forwarded, so it
always acts on the node `--instance-dir` names, whoever leads. This is the
return path for a standby instant — a voter that did not freeze still needs
the set for its own purge floor and to serve joiners.

```
uc2ctl snapshot fetch --from <LEARNER-ID> [--position <P>] --instance-dir <DIR> --app-id <ID> [--admin-key <PATH>]
```

- `--from <U32>` — the learner id to pull from.
- `--position <U64>` — the set to pull. Omitted: the learner's newest complete
  set. The value is packed into the request's two free address words (`ip` =
  the low 32 bits, `port` = the next 16), so it is a **48-bit** field:
  `uc2ctl` refuses a `--position` at or above `1 << 48` by name, locally,
  before anything reaches the admin band, rather than letting it truncate to a
  lower and wrong position.

"Store-only" is the point: the artifacts are written and the set is marked
complete at P, and **nothing is installed** — no state machine is touched, no
`applied` moves backwards, and the floor is adopted through the ordinary
completeness path. A voter above P holding a set at P is not a joiner.

Status `0` means the pull is **underway**, not that it arrived:

```
accepted: fetch of position 73792 from node 1 is underway — not yet a promise it arrived; …
```

Since backup tier 3b the node tries its **backup tier first** (`[backup]` or
`[tier_source]`, when the set is within a registered entry's `through`), checks
each artifact against the cluster's agreed hash, and falls back to `--from`
when the tier lacks the set or a check fails (`tier_fetch_declined`). The
artifacts land the same way either way. The audit record's `fetch_source`
says which served it (`"tier"` or `"peer"`).

Poll `uc2_snapshot_fetched_position` (or `snapshot show`) for the landing.
`50 snapshot_above_durable` refuses a position above this node's own durable
frontier — an operator typo, or a learner transiently ahead of this voter; it
becomes legitimate again once this node's log catches up. `retry` means a
fetch is already in flight here; one is pending until it lands or its 60 s
intake deadline passes. Audited as `snapshot_fetch`.

### `snapshot show`

Print the newest **complete set**'s position and each artifact's presence —
the diagnostic for a stalled instant. Read-only, and **offline** in the same
sense as `schedule show`/`settings show`: it writes no admin request.

```
uc2ctl snapshot show --instance-dir <DIR> --app-id <ID>
```

```
row=0 name=orders newest=73792
row=1 name=kv newest=73792
cluster newest=73792
set=73792
```

It opens the cnc page only to learn which rows are declared and their names,
then reads directory listings under `<instance_dir>/snapshots/` and parses
**file names** only (`snap-<pos>.ultsnap`, `snap-<pos>.ultcluster`). It never
opens an artifact: the 24-byte `ULTSNAP2` envelope inside each one (a
pre-2.13.0 `ULTSNAP1` artifact is refused by name) is
[`verify-backup`](#verify-backup)'s business, not this command's.

`set=<P>` is the newest position present in **every** declared row's directory
*and* in `snapshots/cluster/` — the **intersection** of what is on disk, not
each side's own newest. That matters: a row that has already frozen the *next*
instant lists a newer file than the last complete set, and taking each side's
maximum would make an established set appear to vanish. `set=none` means no
such position exists, including "nothing has ever snapshotted". A row whose
`newest=` sits below the others is the row a `Uc2SnapshotStalled` alert is
pointing at.

This is the **node-local, on-disk** reading — a file listing, nothing more.
Since `0.11.0` (the snapshot catalog, `2.14.0`) this is NOT the same thing
as the purge floor: the cluster's own view is the replicated catalog, whose
sets are tagged AGREED or not (every row's reported hash matches) and whose
newest agreed position is the floor every node is bounded by. `set=<P>` can
list a position no quorum agrees on, and the catalog can agree on a position
this node does not hold. The live read of the catalog — rather than this directory
listing — is [`uc2ctl catalog`](#catalog); the five `uc2_catalog_*` gauges on
`/metrics` (`uc2_catalog_sets`, `uc2_catalog_agreed_position`,
`uc2_catalog_empty`, `uc2_catalog_stalled`, `uc2_catalog_diverged`) remain the
alerting surface.

### `upgrade pin`

Pin a declared row's **next version at a coordinated instant** (FSM upgrade
lifecycle spec §2.5, §3 S4 step 2). Wire op `10`. `settings apply`'s pipeline
verbatim: encode a 20-byte `UpgradePin` record, stage it at
`<instance_dir>/upgrade.pending` (mode `0600`, fsync, rename), and sign the
first ten bytes of its SHA-256 into the request's `id`/`ip`/`port` fields.

```
uc2ctl upgrade pin --row <R> --to <MAJOR.MINOR.PATCH> --origin <P> [--from <MAJOR.MINOR.PATCH>] [--patch] --instance-dir <DIR> --app-id <ID> [--admin-key <PATH>]
```

- `--row <U8>` — the declared row to pin.
- `--to <MAJOR.MINOR.PATCH>` — the version the row is pinned to.
- `--origin <U64>` — the coordinated instant's position (`uc2ctl snapshot`'s
  printed `instant=`) whose complete set the row installs, unconditionally, at
  its next attach. Must be `> 0`; `0` is refused locally, before anything is
  staged.
- `--from <MAJOR.MINOR.PATCH>` — the version the row is pinned *from*. It
  must be on the row's **running line** — the same major.minor as the row's
  running version; patch is ignored — or the pin is refused `53`. Optional:
  omitted, `uc2ctl` reads the row's running version off **this** node's page
  (the word `status`'s `running=` prints), falling back to the row's attached
  version word (`version=`) while no running version has been recorded, and
  refuses locally when neither is set (pass `--from` explicitly). A running
  version of `0` is a real version (an FSM that never set `const VERSION`) and
  is used as `--from 0.0.0`. `--to 0.0.0` is refused locally: packed `0` is
  what the pin words read as "no pin".
- `--patch` — allow a **same-line** pin: `--from` and `--to` share
  major.minor and differ only in patch. Without it such a pin is refused
  locally, before anything is staged, naming both versions: a same-line pin
  sets the running version but does **not** refuse the old build (patch
  builds of one line mix by design), which is right for a real patch release
  and wrong for anything else. The usual cause is a bare-integer
  `const VERSION` (`1`, `2`, `3` …), which packs as `0.0.x` — every such build
  is one line, so "pin 1 → 2" would refuse nothing. Give the state machine a
  real major.minor with `pack_version(major, minor, patch)` and pin across
  lines instead.

This is step 2 of the spec's S4 sequence — an upgrade is always
`uc2ctl snapshot` → `uc2ctl upgrade pin` → stop every instance of the row,
swap the binary, start. Step 1's complete set at P must already exist on
every node; step 2 records the intent to install it; only step 3 (stop/swap/
start) touches a running service, and only then does a v_new attach read the
pin and install `snap-<P>` unconditionally (until an agreed instant after
the swap completes the pin; then a restart starts from that set) — see
`docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md` §3 S4
for the full sequence and its documented limits (a bounded window of
already-emitted `on_committed` side effects between the pin and the stop is a
recorded limit, not a defect).

The same three consequences every `CLUSTER` command carries apply here:
**leader-only** (the staged file is node-local, so a follower answers `retry`
with the leader hint), **single in flight** across every `CLUSTER` command
(a pin retries behind an in-flight membership/schedule/settings change, and
vice versa), and **a refused or timed-out pin leaves the staged file in
place**, so a retry needs nothing re-staged.

A pin that changes the row's line (major.minor) **stops every attached
service of the row at the pin record**: each applies everything before the
record, publishes `applied` = the record's start position, logs
`version_superseded` and fail-stops (#33). A pin on the same line (a patch
pin) sets the running version and stops nothing. Either way the pin sets the
row's running version to `to`, which `status` shows as `running=`/`running_pos=`.

On success the printed `position` word is the frame-end position of the new
pin record:

```
pinned: row=0 from=1.2.3 to=1.3.0 origin=73792 position=81920
```

Every outcome is recorded in `audit.jsonl` as `upgrade_pin`, with the digest
rendered in the `id`/`addr` fields.

### `upgrade show`

Print the **committed** pin history and hash verdicts, per row. Read-only: it
writes no admin request. It reads the live [cluster view](cluster-view.md)
when a node runs and the newest cluster artifact (`offline: newest artifact at
<P>` on stderr) when it does not, as
[`schedule show`](#schedule-show)/[`settings show`](#settings-show) do; with
neither it says `no cluster artifact yet` rather than printing a value
nothing committed.

```
uc2ctl upgrade show --instance-dir <DIR> --app-id <ID>
```

```
position=81920
  row=0 pinned=1.3.0 from=1.2.3 origin=73792 history=[1.1.0->1.2.3@40960]
  row=0 running=1.3.0 set_at=78016 by=pin
  row=0 hash_verdict=agreed position=73792 nodes=3 hash=0x1122334455667788
  row=1 running=0.4.0 set_at=4160 by=genesis
```

`pinned=`/`from=`/`origin=` are the row's **newest** pin; `history=[...]`
(when present) lists every earlier pin, oldest first, as
`from->to@origin`. The `running=` line (#33) is the row's committed **running
version** as of the artifact, with `set_at=` the frame-end position of the
record that set it and `by=` which record that was: `pin` when the row's
newest pin names that version as its `to`, `genesis` otherwise (the leader
recorded its own attached version). `by=` is inferred from the history
printed above it rather than stored. A row with no running version prints no
`running=` line. **Migration:** a cluster artifact written before `0.10.0`
(image version 1 or 2) carries no running versions; on read, each row with
pins takes its newest pin's `to` (so it prints `by=pin`), with `set_at=` the
image's own applied position rather than the pin record's, and a row without
pins has none until the leader records one. `hash_verdict` reads the newest `SnapshotReport` this
node's leader collected for that row at that origin's position — computed by
`uc_protocol::v2::upgrade::verdict`, a pure function, never stored:
`agreed` (every reporting node's hash matches), `DIVERGED` (a majority hash
exists but at least one node disagrees — `minority=` names the dissenting
node ids), or `NO_MAJORITY` (no hash holds more than half the reporters —
never expected outside a badly split cluster, and worth treating as an
incident if seen). A row with a pin but no report yet prints only its pin
line.

**The verdict line is populated on a live cluster** as of the same release
that shipped the record. Every node hashes each row artifact as its builder
streams it, and on its own set-complete edge reports `(row, P, hash)` to the
leader over `SNAP_REPORT` (datagram kind 26); the leader appends one
`SnapshotReport` per `(row, P)` once **every voter in the current
membership** has reported it, or five seconds after the first report,
whichever comes first. So:

- `nodes=` on a healthy cluster equals the **voter count**, and often that
  plus any learners whose reports arrived in time — learners report and their
  hashes ride the record, but they never pace the release;
- `nodes=` **short of the voter count** means the five-second fallback fired:
  some voter did not report that instant. The record is still sound evidence
  about the nodes it names — it just names fewer of them. Check
  `uc2_snapshot_reports_timed_out_total` on the leader and
  `uc2_snapshot_reports_unsent_total` on the quiet node;
- a row that has snapshotted but shows **no verdict line at all** has had no
  report committed yet: either the instant is younger than this node's
  artifact (the lag above — the usual reason), or no node reported (see
  [Monitor a cluster](../how-to/monitor-a-cluster.md)).

**The verdict you read here is always at least one instant old, by
construction.** This command reads the newest cluster **artifact**, and that
artifact was frozen *as of* instant `P`; the `SnapshotReport` for `P` is
appended at a position strictly **above** `P`, because the leader cannot
release it until every voter has reported — which happens after the freeze.
An artifact therefore can never carry its own instant's verdict: `P`'s
verdict first appears in the artifact written at the *next* instant. Run
right after commanding one instant, this prints no verdict line for that row,
or the previous instant's. That is the same artifact-backed lag
[`schedule show`](#schedule-show) and [`settings show`](#settings-show) carry,
not a defect, and it is why `position=` on the verdict line is normally an
EARLIER instant than the one you just took. To see a given instant's verdict
durably, take a second instant (or wait for the `snapshot_interval_bytes`
cadence) and read again. **For a prompt reading, use `/metrics` instead**:
`uc2_snapshot_hash_mismatch{service,row}` is recomputed at scrape time from
the node's committed cluster **view**, so it reflects an instant as soon as
its record commits — see
[Upgrade an application § Verify](../how-to/upgrade-an-application.md).

The cluster FSM's own artifact (`service_id = 255`) is **not** reported —
`SnapshotReport.row` covers declared rows `0..8` only.

### `tier forget`

```
uc2ctl tier forget --tier-id <ID> --instance-dir <DIR> --app-id <ID> [--admin-key <FILE>]
```

Drops one backup tier's committed watermark from the cluster state — admin
op **11**, audited `tier_forget`. `--tier-id` is the tier's 64-bit id as its
`TIER` label carries it, `0x…` hex or decimal. The id does not fit one admin
request field, so it travels split: its low 32 bits in `id`, its high 32 in
`ip`, `port = 0` (the audit record's `detail` names the whole id as `tier
<16 hex digits>`).

Leader-only and single-in-flight like every `CLUSTER` command: a follower,
or a leader with a previous `CLUSTER` command still above commit, answers
`retry` — re-run it, against the node the leader hint names. On success it
prints `forgotten: tier 0x<16 hex digits> position=<P>`, where `P` is the `Forget`
record's frame-END position. An id no tier is registered under is refused
**62 `tier_unknown`**.

Forgetting touches **only the cluster's record**: the tier's files are
untouched, and a writer still pointed at the tier re-registers it on its next
report. To retire a tier for good, remove its `[backup]` section from every
node first, then forget it.

### `wire show`

```
uc2ctl wire show --instance-dir <DIR> --app-id <ID>
```

Prints the committed cluster wire level and what this node could run, read
off the cnc page (wire-level spec §5.2). Read-only: it writes no admin
request.

```
wire: level=12 binary=12..13 raise=auto
  member id=2 role=voter wire_max=13
  member id=3 role=learner wire_max=no report
verdict: holdout id=3 (no report this term)
```

`level` is the committed level (cnc `wire_level`), `binary` this binary's
supported range (cnc `wire_binary`), and `raise` the committed raise mode from
the [cluster view](cluster-view.md) — `auto`, `manual`, or `unknown (no
cluster view yet)` when the node runs no view and has no cluster artifact. The
member table is **leader-authoritative**: only the live leader prints one
`member` line per other member (its id, role, and the wire max it reported
this term, or `no report`) and a `verdict:` — `raisable to N`, `at level
(nothing above L)`, `holdout id=I (no report this term)` or `holdout id=I
reported M`, computed by the same predicate the leader's auto raise and the
`wire raise` door use, with this node's own max taken from its binary. Any
other node, or a node that is not running, prints a one-line `members:` note
pointing at the leader instead. The verdict names ONE holdout, checking this
node first and then the peer slots in order; when several members hold the
level back, the node's own refusal detail (the `wire raise` audit record) may
name a different one — both are real holdouts.

### `wire raise`

```
uc2ctl wire raise --instance-dir <DIR> --app-id <ID> [--admin-key <FILE>]
```

Asks the leader to raise the committed wire level to the highest level every
member reports (the minimum of the members' wire maxima) — admin op **12**, audited `wire_raise`. `id`/`ip`/`port` are
zero. It works in either raise mode (in `auto` it only does sooner what the
leader would do anyway) and appends a `Settings` record carrying the new
level.

Leader-only and single-in-flight like every `CLUSTER` command: a follower, or
a leader with a previous `CLUSTER` command still above commit, answers `retry`
— re-run it against the node the leader hint names. On success it prints

```
ok (cluster position P) — run `uc2ctl wire show` for the committed level
```

A cluster already at every member's level is also a success and appends
nothing, which the reply cannot tell apart from a raise — hence the pointer at `wire
show`. A member in the committed membership with no report this term, or
reporting below the next level, is refused **63 `wire_holdout`**; `wire show`
on the leader names it, and so does the audit record's `detail`
(`level=<L> holdout=id:<n> reported <m>` or `holdout=id:<n> no report`).

### `cluster`

Print the whole [cluster view](cluster-view.md): membership, settings, the
schedule table, per-row upgrade state, the catalog, backup tiers, the purge
gate, this node's services and holdings, and the `attention` list. Read-only,
and it needs no admin key: it never touches the admin band.

```
uc2ctl cluster --instance-dir <DIR> --app-id <ID> [--json]
```

```
source=live node=1 role=leader joined=true view_position=4000 commit=4096
catalog retain_sets=1 empty=false floor=1024
  set=1024 kind=full state=complete agreed=true stalled=false size=15 held_by=[1, 2]
    kv verdict=agreed hash=0x00000000000000aa
    cluster verdict=agreed hash=0x00000000000000bb
purge_gate requires_tiers=0 position=off tiers_reporting=0 released=false
self journal_first=0 durable=0 free_bytes=unknown journal_bytes=0 snapshots_bytes=0 sets_held=[1024]
node id=1 addr=127.0.0.1:7001 role=voter age_ms=0 stale=false durable=0
node id=2 addr=127.0.0.1:7002 role=voter age_ms=100 stale=false durable=0
node id=3 addr=127.0.0.1:7003 role=voter age_ms=5000 stale=true durable=0
membership config_version=3
  id=1 addr=127.0.0.1:7001 role=voter
  id=2 addr=127.0.0.1:7002 role=voter
  id=3 addr=127.0.0.1:7003 role=voter
settings position=0 admission_bytes=0 fsm_lag=default snapshot_interval_bytes=0 snapshot_target=all retain_sets=1 auto_fetch=true backup_retain_sets=0 backup_retain_age_secs=0 purge_requires_tiers=0 wire_level=12 wire_raise=auto datagram_mtu=0 (baseline)
schedule entries=0 position=0
upgrade row=kv running=unversioned pins=0
service kv attached=true version=unversioned lag_bytes=0 pinned=false
attention {"code":"node_stale","node":3}
```

The text uses the same words the JSON does (this sample is the leader's
[example document](cluster-view.md#example)); the `settings` line shows the
same settings fields [`settings show`](#settings-show) prints, but its
`position=` is the settings record's own position, while `settings show`
prints the view's position (`view_position`). A `null` in the document prints
as `unknown` or `-`. `--json` prints the node's `/cluster` body **verbatim**,
with only its `schema` checked; a node serving a different schema is refused
by name (`the node serves cluster view schema … ; this uc2ctl reads
uc2.cluster-view/1`).

`uc2ctl` dials `<instance_dir>/obs.sock` only while `instance.lock` is held. A
stopped node is read **offline** from the newest cluster artifact: the header
reads `source=artifact (offline: newest artifact at <P>)`, `--json` has
`"source": "artifact"` with the live-only fields `null`, and the offline
limits are in [the reference page](cluster-view.md#offline). A held lock with
no bound socket — a node that is still starting, or an embedded `Node` not run
by `uc2-node` — exits non-zero with `node running but its obs socket is not
bound (starting, or not run by uc2-node)`; it is not reported as offline. With
neither node nor artifact it prints `no cluster artifact yet (node not running;
nothing to read offline)` (on stderr under `--json`, so stdout stays JSON-only).
A path that is not an instance directory is refused by name and exits
non-zero — `no such instance directory` when it does not exist, `not an
instance directory` when it exists but holds neither `instance.lock` nor
`snapshots/cluster/` — so a mistyped `--instance-dir` never reads as a fresh
node. The same check guards `settings show`, `schedule show` and `upgrade
show`.

### `catalog`

The storage part of the same document: the catalog sets and floor, the backup
tiers, the purge gate, this node's holdings, the leader's per-member `nodes`,
and the storage `attention` codes (`catalog_empty`, `set_stalled`,
`set_diverged`, `purge_gate_released`, `tier_behind`, `node_stale`).

```
uc2ctl catalog --instance-dir <DIR> --app-id <ID> [--json]
```

The text is the first part of `uc2ctl cluster`'s output (header, catalog,
sets with per-row verdicts, tiers, purge gate, self, nodes, attention).
`--json` prints that storage sub-object under the **same `schema`**, cut from
the node's response as JSON so a field a newer node adds still passes through.
Online, offline and error behavior are those of [`cluster`](#cluster). Offline,
the newest set is marked `(cut at this artifact; its completion is not visible
offline)`, `empty` is `false` only when an earlier set completed and otherwise
prints `empty=unknown (offline)` (JSON `null`), and `catalog_empty` is never
raised.

### `status`

Prints the node's current config version and pending state, per-member
peer-slot observability, the per-declared-FSM service table (M14), and the
leader/serving flags. Read-only: it writes no admin request.

It works on a stopped node too — the page outlives the process, and reading a
stopped node's `log:` line is a step of the flag-day upgrade — so it exits `0`
either way. The first line, `node:`, says which case you are reading; a node
that is not running is never reported as a leader or as serving.

- `--admission-bytes <U64>` — override for the staleness warning's admission
  window. Since wire protocol 0.3.0 the node publishes its configured value on
  the cnc page; this flag is needed only against pre-0.3.0 nodes, whose page
  reads `0`.

Output fields:

| Field | Meaning |
|---|---|
| `config` | the adopted config version, whether a change is pending, and — since the schedule table (2.11.0) — `schedule_position=<n>`, the frame-end position of the committed schedule table, read from this node's newest **cluster artifact** (`snapshots/cluster/`) since the cluster FSM. `none` = no artifact yet, or an artifact holding no table; `?` = the artifact could not be read — a read error, not "no table", and it degrades to `?` rather than aborting so one unreadable file never takes the role/log/service lines down with it. It is the same number `uc2_schedule_table_position` exports and must be identical on every node once caught up |
| `ceiling` | jumbo frames (`2.12.0`): the live command payload ceiling in bytes, from the cnc word at offset 3984, plus the datagram rung it came from and whether that rung is `baseline` (1408) or `discovered`. The rung is read from this node's newest **cluster artifact**; with the default snapshot cadence (`0`) there may be none yet, so a ceiling the baseline rung cannot produce prints as `rung >1408, discovered — inferred from the ceiling`, saying plainly that the rung was inferred rather than read. A trailing `— capped by this node's own max_payload, not by the rung` means the binding half is this host's buffer bound, not the cluster's rung. `uc2_commands_over_standard_total` is deliberately **not** here: it is an in-process counter, not a cnc word, and lives in `/metrics` alone |
| `wire` | wire level (`2.15.0`): `level=<L> binary=<MIN>..<MAX>` — the committed cluster wire level (cnc `wire_level`, written by the cluster agent at apply) and this binary's supported range (cnc `wire_binary`). Printed after `ceiling`. The per-member reports and the raise verdict are [`wire show`](#wire-show)'s |
| `node` | printed first: whether a node is behind this page right now. `running=` is a non-blocking probe of `instance.lock` — `true` if a process holds it, `false` if the file is free or absent (the OS drops the flock on any exit, `SIGKILL` included), `?` if the probe itself failed (the reason follows in parentheses). `heartbeat_age=` is the age of the node's own heartbeat (cnc 896), `never` if unstamped. A node that is not running gets a trailing `-- NOT RUNNING`; a failed probe gets `-- UNVERIFIED`; one that holds its lock but has not stamped for 3 s (the `/readyz` bar) gets `-- STALE` (as does one whose age cannot be computed because this host's clock reads before the epoch, printed `heartbeat_age=?`). In every one of those cases the role below is reported as none: `status` believes the page's role only when a held lock and a fresh heartbeat are both proven. The node stamps its heartbeat from the log clock (monotonic plus a sampled wall offset) and `status` compares it with the wall clock, exactly as `/readyz` does — so a wall-clock step can skew the age for a moment (a forward step ≥ 3 s can mark a live node `STALE` for up to a second); the lock probe is unaffected. Since #35; before it, a killed node's frozen page read as whatever role it last held |
| `leader` | `NODE_FLAG_LEADER` is set **and** the `node` line says the node is live; always `false` for a node that is not running or is stale, whatever its page says |
| `can_serve` | `NODE_FLAG_CAN_SERVE` is set, under the same liveness condition as `leader` |
| `page_flags` | only when the node is not live: the role flags its page still holds (`leader+can_serve`, `leader`, `can_serve` or `none`), for forensics. Deliberately not spelled `leader=`/`can_serve=`, so a script that greps for the live role cannot match the stale one |
| `term` | current term |
| `leader_hint` | the id this node believes leads; `unknown` when the raw value is `u64::MAX` |
| `log: commit / durable / append` | the three log counters, in bytes |
| `members` | one line per occupied peer slot: `id`, `role`, `reported_durable`, and a staleness marker when `commit - reported_durable` exceeds the admission window |
| `services` | the declared id list (cnc 4032's bitmask), the lag policy, and — since log time and timers (2.11.0) — `log_time_ns=<n>`, the log's clock read from cnc `4048`, in **raw nanoseconds since the Unix epoch**. It is not formatted as RFC 3339: the binary carries no date formatter, and the raw value is what the `uc2_log_time_ns` metric and the cnc word both hold. `0` means no leader has stamped anything this page generation — `fsm_lag=lockstep` or `fsm_lag=<N> bytes` (cnc 4040). A node started for a harness (`ServicesConfig::none_for_tests`) prints `declared=[] fsm_lag=n/a` and no rows: with nothing declared there is no lag policy to report, even though cnc 4040 still holds a resolved bound (since **2.8.1**; earlier releases printed that bound, or `lockstep` when it happened to read 0). **Since `2.13.0` a configured node prints the same `declared=[]` and no rows while it is still joining its cluster** — the node publishes its declared set from the consensus pass, not at `Node::start`, so an empty list on a node that should have rows means "not joined yet", not "misconfigured". Give it a moment and re-run; if it persists, the node is not hearing a leader or its cluster FSM is not reaching commit |
| per-FSM rows | one line per **declared** row, attached or not, in this order: `row=`, `name=` (the row's declared FSM name, node-written at boot, cnc 3.1), `version=` (the attached service's packed version, or the literal `unversioned` if the packed value is 0 — unattached or an FSM that never set `const VERSION`), `hash=0x...` (the row's identity hash, cnc 3.1), `attached=` (the slot's ATTACHED bit), `epoch=` (incarnations since this node booted), `incarnation=` (the status word's counter), `applied=`, `lag=` (`commit − applied`), `snapshot_pos=`, `heartbeat_age=` (`never` if that FSM has not stamped since boot), `timers_pending=` (that row's pending scheduled timers, cnc slot line 7 `+488`), `upgrade_origin=` (the row's committed `UpgradePin` origin — the coordinated instant this row installs unconditionally at its next attach until the pin completes, `0` = no pin), `pinned=` (the version that pin names, `unversioned` when `upgrade_origin=0`), `pinned_from=` (the version the pin's origin artifact was BUILT by — the pinned install's cross-check version, `unversioned` when `upgrade_origin=0`), `artifact_hash=0x...` (the hash of the artifact this row's builder published at `snapshot_pos` — SHA-256 of the artifact PAYLOAD, envelope excluded, truncated to its first 8 bytes, from cnc slot line 7 `+504`; `0x0000000000000000` = this row has published no artifact on this node since boot). Because it is written by the builder immediately BEFORE `snapshot_pos`, `snapshot_pos=` and `artifact_hash=` on the same line always describe the same artifact. Comparing the pair across nodes is the manual form of the live check — for the cluster's own verdict use [`upgrade show`](#upgrade-show), which is computed from a committed record rather than from what each node happens to print. `name=`/`version=`/`hash=` are new since FSM identity, `timers_pending=` since log time and timers (both 2.11.0), and `upgrade_origin=`/`pinned=`/`pinned_from=`/`artifact_hash=` since the FSM upgrade lifecycle (cnc 3.3, `uc2ctl upgrade pin`/`show`); earlier releases printed only `attached=... epoch=... incarnation=...`. Last on the line, since cnc 3.4 (#33): `running=` — the row's committed **running version**, the version every service of this row must match in major.minor (a genesis record or a pin sets it; `none` = no record yet — while any declared row reads `none` on the leader, the leader admits no client writes; `unversioned` = a recorded version of `0`, which is a real version; `?` = the words could not be read consistently, never fabricated) — and `running_pos=` — the frame-end position of the record that set it (`0` with `running=none`; `?` with `running=?`). Unlike `version=`, which is whatever the attached binary says, `running=` is cluster state: identical on every node once caught up. A service attached at a `version=` off the `running=` line is either about to stop at that record or has stopped there (`applied=` at the record's start) |

## Offline commands

`backup`, `verify-backup`, `restore` (M11 Task 2), `force-single-member`
(M11 Task 4), `gen-admin-key`/`audit` (M12b, `v2.6.0`), and the backup tier's
`tier show`/`tier verify`/`tier restore`/`tier fork`/`tier export-corpus`
(`0.11.0`, since `2.14.0`). See
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
else `Hole { service: <id>, .. }`), checks every retained snapshot's 24-byte
`ULTSNAP2` envelope (a **legacy** `ULTSNAP1` artifact — 2.11.0/2.12.0, no
version field — is named and refused, same as one with no envelope at all,
`corrupt snapshot artifact`), and cross-checks a `MANIFEST` if present
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
wipe-and-rejoin later as fresh learners. The command rewrites only the
node's own config record; at the survivor's first leadership it appends the
single-voter membership as a cluster command, so the replicated cluster
state follows once that commits. See
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

A `snapshot_fetch` record with `actor` and `source` `auto` is the node's own
background fetch (snapshot lifecycle), not an operator's. Its 14th key,
`fetch_source` (backup tier 3b), is `"tier"` or `"peer"` on a fetch record
(the node's own id in the member field for an auto-fetch the tier served) and
`null` on every other record.

A line the summarizer cannot make sense of — a torn write from a crash
mid-record, or a hand-edited file — prints as-is prefixed `? ` rather than
being dropped or panicking; the whole point of this file is to never lose a
record silently. See
[Change cluster membership](../how-to/change-cluster-membership.md#read-the-audit-log)
and [Instance directory](instance-directory.md) for the file's durability
class (`O_APPEND`, `fsync` per record, no rotation).

### `tier show`

```
uc2ctl tier show --target <TIER> [--instance-dir <DIR>]
```

Reads a backup tier (the `[backup] target` path) and prints its `TIER`
identity (`tier_id`, `app_id`, `created_ns`), every complete set (position,
kind, time, row count, and how far its chain reaches), every set directory
with no `SET` label (`INCOMPLETE` — an interrupted copy, never a restore
start), every set whose `SET` label does not parse (`CORRUPT`, never a
restore start), every journal chunk `[from, to)`, the watermark
(`oldest_set`, `newest_set`, `through`), one `fence: <Q>` line per restore
fence (see [`tier restore`](#tier-restore)), and one `restorable: (a, b]` line per range a
restore can be built in — a restore point `Q` is a frame end with
`a < Q <= b`, never a set position itself. With `--instance-dir`, it also prints the committed backup
watermarks that node's newest cluster artifact holds, marking the one that
is this tier (`no cluster artifact yet` until that node's first instant — this
list is read from the artifact only, not the live view), the
setting `purge_requires_tiers=<K>`, and a `(counts toward K=<k>)` marker on
the K entries with the highest `through` (when K is above the number of
entries the gate is `0` and nothing counts: purge waits).
Filesystem-only; the node may be running.

### `tier verify`

```
uc2ctl tier verify --target <TIER> [--deep]
```

Checks every chunk (header against name, block CRCs, a whole-frame walk to
its end), byte equality wherever two chunks overlap, and every `SET`'s
objects present at their stated raw sizes; a set directory without a `SET`
label is reported `tier_set_incomplete`, and a `SET` label that does not
parse (or names another position) `tier_set_label_corrupt`. `--deep` also re-hashes every
artifact against its `SET` hash (a row's hash covers the payload after its
24-byte envelope, whose position must equal the set's). Prints
`tier verify: <sets> sets, <chunks> chunks, <n> findings`, then one
`finding: …` line per finding, each naming the object; **exits 1 when there
is any finding**, 0 when there is none. A restore fence is printed as an
`info: tier_fenced: …` line before the findings and is not a finding.
Read-only.

### `tier restore`

```
uc2ctl tier restore --target <TIER> [--at <Q>] --instance-dir <DIR> --confirm-cluster <APP_ID>
uc2ctl tier restore --target <TIER> [--at <Q>] --instance-dir <DIR> --new-node --node-id <N>
```

Builds an instance directory a node boots from unchanged, as of `Q`
(`--at`, default the tier's furthest reach — the `through` of `tier show`).
The start set `S` is the newest complete set below `Q` whose chain reaches
`Q`. The builder copies `S`'s artifacts (each checked against its `SET` size
and hash), writes the chain's bytes `[S, Q)` into a fresh journal, and
writes `state/` itself: snapshot floor `S`, a term map (the set's recorded lineage, then the terms
derived from the restored frame headers), the membership the cluster artifact at `S` carries
as the config record (position `S`), **no vote**, and the output-progress
marker at `S`. It leaves a `RESTORED` file (`tier_id`, `start_set`, `q`,
`mode`, `created_ns`) for the audit trail; nothing reads it. Everything is
assembled under `<DIR>/.restore-tmp/`, checked by the same verification as
[`verify-backup`](#verify-backup), and only then renamed into place.

Exactly one of:

- `--confirm-cluster <APP_ID>` — **whole-cluster restore**: run it on every
  node, with the same `--target` and the same `--at`, then start every node.
  It must equal the tier's `app_id` or nothing is built. It prints
  `restoring as of <Q>; history after <Q> is discarded on every node restored
  this way` before building. Every node starts from identical history with
  no vote, so the first election is ordinary. This is the quorum-loss
  domain: a restore of fewer than all nodes beside a live majority is not
  what this verb is for.
- `--new-node --node-id <N>` — one replacement node seeded from the tier,
  to join a **live** cluster. `N` must not be in the membership the node
  would boot with (the cluster artifact at `S` folded with every `Membership`
  frame in `[S, Q)`; `restore_id_reused`). The restored term map is the set's
  recorded lineage (the cluster's terms below `S`) followed by the terms of
  the restored frames, so a live leader's map aligns with it and the node's
  log is kept. Start it, then run [`add-learner N`](#add-learner) and, if it
  should vote, [`promote N`](#promote). A set with no `lineage` is refused
  (`set_lineage_missing`; `set_lineage_malformed` when it is inconsistent).
  Every restore mode writes the lineage into `term_map.state`.

**A restore below the reach fences the tier.** When `Q` lies below the
tier's furthest reach, the restored cluster will write a history past `Q`
that the tier's `(Q, reach]` does not hold. After `<DIR>` is placed, the
builder writes the write-once object `fence/<Q>` (`q`, `reach`,
`created_ns`, `app_id`, `restored_into`) to the tier and prints `fenced:
fence/<Q> …`; a fence it cannot write removes the placed directory again and
refuses. Every writer refuses a fenced target (`backup_target_fenced`,
`uc2_backup_errors_total{reason="fenced"}`) and stops, so point every
writer's `[backup] target` at a new, empty directory before starting the
restored nodes. Reads (`tier show`/`verify`/`restore`/`fork`/`export-corpus`)
ignore fences; restoring the other nodes at the same `Q` finds the fence
already there. A restore at the reach writes none; `tier fork` and `tier
export-corpus` never do.

The builder holds `<DIR>`'s exclusive `instance.lock` for the whole build,
as `force-single-member` does, so a node started there mid-build refuses.
While the assembled directories are renamed into place a `RESTORING` marker
sits in `<DIR>`; a crash in that window leaves it.

Refusals, each by name, each leaving `<DIR>` exactly as it was (and removing
it again if the command created it):

- a running node holds `<DIR>`'s `instance.lock`;
- `<DIR>` carries a `RESTORING` marker — an earlier restore was interrupted
  while placing; remove its `journal/`, `state/`, `snapshots/` and
  `RESTORING`, then restore again;
- `TargetNotEmpty` — `<DIR>`'s `journal/`, `state/` or `snapshots/` is not
  empty (volatile leftovers such as `cnc2.dat` and an unheld
  `instance.lock` are fine);
- `restore_point_unreachable: restorable (a, b] …` — `Q` is outside every
  restorable range, at a set position (an empty journal span), or not a frame
  end;
- `restore set <S> incomplete: <object> missing`, or `tier_set_mismatch` —
  an artifact vanished during the build or does not match its `SET`;
- a chain gap, a corrupt chunk or an overlap mismatch, as `tier verify`
  names them;
- any finding of the final verification of the assembled directory.

### `tier fork`

```
uc2ctl tier fork --target <TIER> [--at <Q>] --instance-dir <DIR> --app-id <NEW> --node-id <N>
```

Builds a **separate cluster** as of `Q` (`--at`, default the tier's furthest
reach) in `<DIR>`: the same build as `tier restore`, with two differences.
The output-progress marker is `Q`, not `S` — a fork replays **no** side
effects: every output at or below `Q` already happened in the real world, so
the fork's output handler sees only commands committed after it boots. Then
the [`force-single-member`](#force-single-member) plan is applied to the
result: `N` becomes the only voter at its recorded address, config version
one past the restored one, every other member dropped (not tombstoned). The
`RESTORED` file records `mode=fork:<N>`.

It prints `forking as of <Q> into a separate cluster "<NEW>" with node <N> as
its single voter; no output at or below <Q> is replayed` before building,
then `forked: … config_version=<V> dropped=[…]` and the next step.

Start node `N` on `<DIR>` with `app_id = "<NEW>"` in its `node.toml`. At its
first leadership the node appends the single-voter membership as a cluster
command, so the replicated cluster state (`uc2ctl status`, the cluster
artifact) agrees with the forced config once that commits; until it has,
membership changes answer `retry`. Grow the fork with
[`add-learner`](#add-learner) and [`promote`](#promote).

The fork inherits the source tier's watermark entry in its cluster state.
It does not hold a purge (the hold is local to a writer), but it **alerts**:
nothing advances it on the fork, so `uc2_backup_lag_bytes` grows with every
commit and `Uc2BackupBehind` fires past 256 MiB. Run `uc2ctl tier forget
--tier-id <id>` on the fork once its first leader is up.

Refusals, each by name, each building nothing — the `tier restore` list,
plus:

- `fork_same_app_id` — `--app-id` is the tier's own `app_id`. A fork is a
  separate cluster and must not be confusable with its source;
- `fork_node_unknown` — `N` is not a voter or learner (or is a tombstone) of
  the membership as of `Q`.

### `tier export-corpus`

```
uc2ctl tier export-corpus --target <TIER> --row <R> [--from <S>] --to <Q> --out <DIR>
```

Builds a [diff-replay](../how-to/diff-replay.md) **corpus** from the tier:
the `tier restore` build of `[S, Q)` into `<DIR>` (a whole-cluster recovery
build — `<DIR>` is never booted), plus a `CORPUS` manifest in the
`uc2-corpus-v1` format `uc2-diffreplay` reads, written by `uc_diffreplay`'s
own writer: `app_id` (the tier's), `row`, `origin = S`, `end = Q`, and
`version` — the row's version as the start set's `SET` records it. `S` is
`--from` when given (a complete set below `Q` whose chain reaches `Q`),
else the newest such set. Prints `exported: <DIR> row=… origin=… end=…
version=0x… tier=0x…`. Then, for example:

```
uc2-diffreplay determinism --corpus <DIR> --bin <your-service-binary> --report det.json
```

Refusals: the `tier restore` list (`restore_point_unreachable` names an `S`
that is not a complete set or whose chain does not reach `Q`), plus
`corpus_row_unknown` — the start set holds no artifact for `--row`.

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

`settings apply` does the same, with the cluster FSM's own view position as its
`version` word:

| Status | Printed as | Process outcome |
|---|---|---|
| `0` | `applied: version=<N>` (the new record's frame-end position) | exit 0 |
| `1` | `refused: <reason> (cluster position <N>) — staged file kept at <PATH>` | exit 1 |
| `2` | `retry: leader unknown or a previous cluster command is still uncommitted (cluster position <N>) — staged file kept at <PATH>, try again` | exit 1 |

`upgrade pin` does the same, with `pinned: row=<R> from=<V> to=<V> origin=<P>
position=<N>` in place of `applied:`:

| Status | Printed as | Process outcome |
|---|---|---|
| `0` | `pinned: row=<R> from=<V> to=<V> origin=<P> position=<N>` | exit 0 |
| `1` | `refused: <reason> (cluster position <N>) — staged file kept at <PATH>` | exit 1 |
| `2` | `retry: leader unknown or a previous cluster command is still uncommitted (cluster position <N>) — staged file kept at <PATH>, try again` | exit 1 |

`snapshot` and `snapshot fetch` stage no file, so their status lines say
nothing about one. `snapshot`'s `version` word is the instant's position;
`snapshot fetch`'s is the position it bound:

| Status | Printed as | Process outcome |
|---|---|---|
| `0` | `instant=<P>` / `accepted: fetch of position <P> from node <id> is underway …` | exit 0 |
| `1` | `refused: <reason> (snapshot position <N>)` / `refused: <reason> (position <N>)` | exit 1 |
| `2` | `retry: this node is not the leader, or its leader open has not completed, or the log buffer was momentarily full …` / `retry: a fetch is already in flight here …` | exit 1 |

## Refusal reasons

The `reason` field of a status-`1` response. Codes 1–10 and 12 are the
discriminants of `uc_consensus::config::ProposeError`; 11 is the node's own
defensive catch-all; 20–24 (M12b, `v2.6.0`) are admin-authentication
refusals (`uc_node::REASON_AUTH_*` / `REASON_AUDIT_FAILED`) — produced only
under `[admin] auth = "hmac"`, and disjoint from the `ProposeError` band so a
caller can tell "the cluster refused this change" from "the cluster refused
to believe this was you" without consulting the policy. 40–43 (2.11.0)
are `schedule apply`'s own refusals (`uc_node::REASON_SCHEDULE_*`) and 44–47
(2.11.0) are `settings apply`'s (`uc_node::REASON_SETTINGS_*`), each in
its own band for the same reason. 48–50 (2.11.0) are the coordinated-snapshot
ops' own (`uc_node::REASON_SNAPSHOT_*`), split across `snapshot` (48, 49) and
`snapshot fetch` (50). 51 is `schedule apply`'s own, added later. 52–59 (FSM
upgrade lifecycle) are `upgrade pin`'s own (`uc_node::REASON_PIN_*` and
`report_stale`), in their own band for the same reason, 60 (#33) is the
cluster FSM's refusal of a `RowGenesis` record, 61 (the snapshot catalog) is
`upgrade pin`'s door check on an unagreed origin, and 62 (the backup tier) is
`tier forget`'s refusal of an id no tier is registered under.

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
| 42 | `schedule_decode` — the staged file is not a decodable schedule table (or declares more than `MAX_SCHEDULE_ENTRIES` entries). This is about the FILE. A table that decodes perfectly but does not fit this node's `max_payload` is **51**, not 42 |
| 43 | `schedule_unknown_fsm` — an entry names an FSM that is not one of this node's declared rows. The **whole** table is refused, never partially adopted: a typo'd name would otherwise leave an operator believing a timer is armed that no row will ever fire |
| 44 | `settings_digest` — the staged settings file's digest is not the one the request signed: a different file was staged than was signed, or it changed in between. Re-run `settings apply` |
| 45 | `settings_missing` — no staged settings file on this node. Either `settings apply` was run against a different instance directory, or a successful apply already consumed it |
| 46 | `settings_decode` — the staged file is not a decodable settings record (wrong length, unknown encoding version, or an unknown `snapshot_target` byte) |
| 47 | `settings_bounds` — a field is out of range; the node's refusal detail and the audit record name which field. Eight fields are checked: `datagram_mtu` other than `0` or a ladder rung; `admission_bytes`, `snapshot_interval_bytes` or `backup_retain_age_secs` equal to `u64::MAX` (the reserved sentinel); an `fsm_lag` byte bound **below 1376 B** — one max-size frame; since `0.11.0` (the snapshot catalog), `retain_sets` above `48` (`uc_protocol::v2::catalog::MAX_RETAIN_SETS`), or equal to `0` from an operator's own `settings apply`; `purge_requires_tiers` above `4` (`MAX_BACKUP_TIERS`; backup tier 3b, `0` is off); and `wire_raise` other than `0` (keep), `1` (auto) or `2` (manual) (`2.15.0`). A REPLAYED older (version 1 or 2) record carries no `retain_sets` and reads it as `1`; it is never refused here. A sub-frame lag pins the report ceiling (`min_applied + fsm_lag`) inside the next frame and stops commit cluster-wide **permanently**, since changing a replicated setting needs a command that commits; `"lockstep"` is how you ask for the tightest pacing. `fsm_lag = "0"` (derive) and `"lockstep"` are sentinels, not bounds, and are never refused here |
| 48 | `snapshot_unsupported` — a declared row lacks the snapshot capability bit, so it would ignore the `SNAPSHOT` frame and the set at P could never complete. Since #67 every Rust SDK service sets this bit unconditionally (`ServiceBuilder::start()` requires `S: SnapshotStateMachine`), so this is now the backstop for a service attached without the current Rust SDK (a non-Rust attacher, or a binary built before #67). The audit record's `detail` field names the row; it is refused by name rather than left with a floor that never moves |
| 49 | `snapshot_no_learner` — `uc2ctl snapshot --standby` with no learner in the committed membership. Only a learner freezes for a standby instant, so with none there nothing anywhere would build the set |
| 50 | `snapshot_above_durable` — `uc2ctl snapshot fetch --position P` names a P above this node's own durable frontier. A voter must not adopt a floor above what it has made durable. Usually an operator typo, or a learner transiently ahead of this voter; legitimate again once this node's log catches up |
| 51 | `schedule_too_large` — the table decoded fine and names only declared rows, but the `CLUSTER` frame carrying it is larger than this node's `max_payload`, so the leader could not append it. A full 32-entry table needs **1072 bytes** of frame body (8 B of `CLUSTER` prefix + 8 B of table header + 32 × 33 B). Raise `max_payload` in `node.toml` and restart, or apply fewer entries. Since 2.11.0 the daemon refuses at STARTUP, by name, if `max_payload` cannot carry a full table, so on a node that booted this reason is reachable only where the two can still disagree — a node started before that check existed, or a hand-edited config. Before 2.11.0 this case was reported as **42**, which sent operators to inspect a file that was perfectly valid |
| 52 | `row_undeclared` — `uc2ctl upgrade pin --row R` names a row this node does not declare in `[services] names`: check the row number against `node.toml`. Named `pin_row_undeclared` through `2.13.0` (same number, same door check); since #33 the CLI prints the bare name, so this table is where the explanation lives |
| 53 | `pin_from_mismatch` — `--from` is not on the row's running line. When the row has a running version (`status`'s `running=`), `--from` must have the same major.minor (patch is ignored — `same_line(from, running)`); this is checked at the door and again by the cluster FSM at apply. With no running version recorded yet, `--from` must equal the version the service is attached at, checked at the door. A stale `--from` usually means another pin already landed since it was read |
| 54 | `pin_no_set` — no **complete** snapshot set at `--origin` on this node. Run `uc2ctl snapshot`, wait for `uc2_snapshot_set_position` to reach it (or `uc2ctl snapshot show`'s `set=`), and pin THAT position |
| 55 | `pin_not_monotone` — `--origin` is not above the row's current pin. A pin only ever moves a row's origin forward |
| 56 | `pin_digest` — the staged pin file's digest is not the one the request signed: a different file was staged than was signed, or it changed in between. Re-run `upgrade pin` |
| 57 | `pin_missing` — no staged pin file on this node. Either `upgrade pin` was run against a different instance directory, or a successful apply already consumed it |
| 58 | `pin_decode` — the staged file is not a decodable 20-byte `UpgradePin` record |
| 59 | `report_stale` — a `SnapshotReport` below the row's held report position. Never produced by `uc2ctl`; recorded here because it shares op 10's refusal band |
| 60 | `version_already_set` — a `RowGenesis` record (`CLUSTER` kind 6) for a row that already has a running version: genesis records a row's first version once and never changes one (#33). Never returned to `uc2ctl` — genesis is the leader's own append, with no admin request behind it — and listed here because it shares the band. A refused genesis changes nothing: like every refused `CLUSTER` record it advances the cluster FSM's `applied` and the row keeps the version it already had |
| 61 | `pin_origin_not_agreed` — `--origin` is a complete set on this node but the snapshot catalog has not AGREED it (every reporting node's hash must match; reports take up to ~5 s after the instant). Wait for that set to agree (`uc2_catalog_agreed_position` reaches it), or take a new instant (`uc2ctl snapshot`) and pin that; a set that diverged never agrees. (54 accepts only this node's newest complete set, so an older agreed origin is not a way out.) On a cluster whose catalog is still empty (no complete set yet), the pin is allowed unchecked and the node logs `upgrade_pin_agreement_unchecked` |
| 62 | `tier_unknown` — `uc2ctl tier forget --tier-id T` names a tier the committed cluster state does not hold: no backup writer has reported it, or it was already forgotten. Check the id against the tier's `TIER` label. Refused by the cluster FSM's own acceptance check (`BackupWatermark` `Forget`, `CLUSTER` kind 7), so nothing is appended |
| 63 | `wire_holdout` — `uc2ctl wire raise` found nothing to raise: a member in the committed membership has not reported its wire max this term, or reports below the next level. Run [`wire show`](#wire-show) on the leader, which names the holdout; the audit record's `detail` names it too. Door-only, so nothing is appended. A cluster already at every member's level is NOT refused: `wire raise` answers ok and appends nothing |

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
