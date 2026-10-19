# How to upgrade a cluster

Run a scripted, whole-cluster upgrade with a measured downtime number:
stop every node, verify they agree on what they hold, swap the binary, start
everyone back up, and confirm one leader is serving again before declaring
success.

Since `2.15.0` a cluster can also be upgraded **one node at a time under
load**, when the new release's wire minor is within one level of the
cluster's: see [Rolling upgrades since 2.15.0](#rolling-upgrades-since-2150).
The stop-all procedure below is still the one for `2.15.0` itself, for a wire
major bump, and for any release whose notes call it a flag day.

## Why this was a flag day, until 2.15.0

**Until `2.15.0`, every node stopped, upgraded, and restarted together.
There was no rolling or partial mode.** That was not a conservative default —
it followed directly from wire protocol 0.5.0's content-attested durable
reports (see ["Shipped in v2.3.0 — wire protocol 0.5.0"](../releases.md) in
the release notes): a follower on an older wire version reports its durable
position without the term it attributes to those bytes, the leader cannot
count that report as attested, and a mixed-version cluster **stalls
commits** rather than making an unsound one. Later flag days were worse: a
`0.8.0` through `0.10.0` peer meets the next release's new `CLUSTER` record,
cannot decode it, drops it, and its cluster FSM **diverges in silence**. A cluster
part-upgraded across one of those versions is a cluster not making progress,
or one quietly disagreeing with itself — so finish such an upgrade, or fully
undo it, and never leave it half-applied on purpose.

`2.15.0` (wire `0.12.0`) gives the cluster a committed **wire level**: every
node encodes at the level the cluster has committed, every datagram carries
its sender's maximum level in header byte 13, and the leader raises the level
only once every member can follow. A node one level ahead therefore runs
beside the old ones and sends nothing they cannot parse. The argument is
[The committed cluster wire level, explained](../notes/uc2-wire-level-explained.md).
`2.15.0` itself is a flag day — the last one for an additive wire change —
because a `0.11.0` node has no level to speak; see the `2.15.0` wire + cnc
section near the end of this page.

## Rolling upgrades since 2.15.0

1. Check the pair is permitted: the new release's wire minor is the cluster's
   level or one above it (`uc2ctl wire show` → `level=`; the release notes
   name the minor). Two above: install the middle release first — this is
   true for a stop-all upgrade too.
2. Decide whether you want a rollback window. If yes, before the first
   restart: add `wire_raise = "manual"` to the cluster's **full** settings
   file — the one last applied, or the record `uc2ctl settings show` prints
   — and `uc2ctl settings apply` it. Never apply a one-line file: `settings
   apply` replaces the whole record.
3. One host at a time: stop the node and every process attached to its
   instance directory (services, clients, the gateway — a cnc minor bump means
   they restart together, the standing per-host rule); install; start the
   node; wait for `can_serve` (`uc2ctl status`, or `/readyz`); start the
   services; move on. Under load, commit stalls only while a quorum re-forms.
4. `uc2ctl wire show` on the leader after the last host: `verdict: raisable`.
   Auto mode raises within a second; manual: `uc2ctl wire raise` — the
   point of no return. Until then you may roll any host back.
5. `uc2_wire_level` is the new level on every node. Done.

What each step leans on:

- **Step 1, the level rule.** A binary runs only while the committed level is
  inside its range, `WIRE_MIN..WIRE_MAX`, and `WIRE_MIN` is one below
  `WIRE_MAX` (`0.12.0`'s range is `12..12`, since nothing below it is
  level-aware). A binary too old for the cluster stops with
  `WireLevelAboveBinary`; one too new stops with `WireLevelBelowBinary` once
  it has joined, and the message names the release to install first. Neither
  needs a wipe: nothing in the instance directory is wrong. A release that
  adds no wire feature keeps its wire minor and is always permitted.
- **Step 2, the window.** `wire_raise = "manual"` keeps the level where it
  is until you run `uc2ctl wire raise`; until then every node can go back to
  the old binary, because nothing has been written that it cannot read. Set
  it **before the first restart**: in auto mode (the default) the leader can
  raise within a check interval of the last old node leaving, and a switch to
  manual made after that can lose the race. The key is flat
  (`wire_raise = "manual"`, not a table) and goes into the cluster's full
  settings file, because
  [`settings apply`](../reference/uc2ctl.md#settings-apply) replaces the
  record: every absent key reverts to its **default**, not to the committed
  value, so a one-line file would reset snapshot cadence, retention,
  `fsm_lag` and `purge_requires_tiers`. Only `wire_raise` keeps the committed
  value when absent (and `wire_level`, which is leader-owned and never a file
  key) — which is why the cluster's existing file, applied again without the
  key, does not touch the mode. The mode persists after the raise: apply the
  full file with `wire_raise = "auto"` if you want the next roll to raise on
  its own.
- **Step 3, the host.** A release that bumps the cnc minor (see its notes)
  makes an older attacher refuse the newer page, so a host's node, services,
  shmem clients and gateway restart together — the same per-host rule as
  [2.7.0's ring change](#ring-format-change-in-270-restart-a-hosts-processes-together).
  It is per host, so it fits inside a roll. Start the node before its
  services: attach waits on `NodeBooting` until the node has joined.
- **Step 4, the verdict.** `wire show` prints the verdict only on the live
  leader, since the per-member table is the leader's. `raisable to N` is the
  go-ahead; `holdout id=I (no report this term)` or `holdout id=I reported M`
  names a member still on the old binary or not yet heard from — every
  member counts, voters and learners alike, so a dead learner left in the
  membership holds the level until `uc2ctl remove` takes it out. `uc2ctl wire
  raise` refuses with `63 wire_holdout` in that state, and on a cluster
  already at every member's level it answers `ok (cluster position P)` and
  appends nothing, so it is safe to run twice. Like every `CLUSTER` command
  it is leader-only: a follower answers `retry`. In auto mode the leader
  checks every 100 ms and raises as soon as the last report arrives, so by
  the time you run `wire show` it will usually already print `level=` the new
  level and `verdict: at level` — that is the success case, not a stall.
- **Step 5, done.** `uc2ctl status` prints `wire: level=L binary=MIN..MAX` on
  every node, and `Uc2WireLevelBelowMembers` (30 m) has nothing left to fire
  on.

Rolling back **before** the raise is the same procedure in reverse, one host
at a time. **After** the raise there is no rollback onto the old binary — it
would stop with `WireLevelAboveBinary` — short of restoring a pre-upgrade
backup on every node.

The sections from here to the per-release notes are the **stop-all**
procedure, `scripts/uc2_flag_day.sh`; a rolling upgrade needs none of them.

## Prerequisite: traffic is actually stopped

`scripts/uc2_flag_day.sh` cannot see who is submitting to the cluster — it can
only read the cluster's own counters. It therefore **requires
`--yes-traffic-stopped`** and refuses immediately without it, touching
nothing. Stop your clients (or point them at a maintenance page) before you
run this, and pass the flag only once they actually are stopped — the flag is
an assertion the script trusts, not something it verifies.

## Run it

Real fleet, one systemd unit per host:

```bash
scripts/uc2_flag_day.sh \
  --hosts "user@h1,user@h2,user@h3" --ssh-key ~/.ssh/id_ed25519 \
  --unit uc2-node --uc2ctl /usr/local/bin/uc2ctl \
  --instance-dir /srv/uc2/nX --app-id myapp \
  --upgrade-cmd 'sudo install -m755 /tmp/uc2-node.new /usr/local/bin/uc2-node' \
  --yes-traffic-stopped
```

`--instance-dir` is a template: the literal substring `nX` is replaced with
`n0`, `n1`, `n2`, ... in host order, so one flag covers every host as long as
your instance directories follow that naming convention. `--unit` names the
same systemd unit on every host — one node per host is assumed.

Pre-stage the new binary on every host (`/tmp/uc2-node.new` above) before you
run this; `--upgrade-cmd` is whatever shell command installs it locally on
each host, and the transfer itself is out of scope for the script and for its
downtime measurement.

**`--local` mode** drives plain `uc2-node --config` processes with `SIGTERM`
instead of `systemctl`, same verification logic throughout — for dev boxes
and the gate harness, not production:

```bash
scripts/uc2_flag_day.sh \
  --hosts "local:/path/n0.toml,/path/n1.toml,/path/n2.toml" \
  --uc2ctl target/release/uc2ctl --uc2-node-bin target/release/uc2-node \
  --app-id myapp --upgrade-cmd 'true' --yes-traffic-stopped
```

In `--local` mode each entry is a node's TOML config file — its
`instance_dir` is read out of the file itself, so `--instance-dir` is neither
needed nor used. `--upgrade-cmd` runs **once**, not once per node (there is
one `uc2-node` binary path on the box); a real local upgrade swaps the binary
at `--uc2-node-bin` before this step, and the hook itself can be a no-op
(`true`).

## What it does, step by step

Every step prints a timestamp.

0. **Preflight.** Every host reachable, every node's `uc2ctl status` parses,
   and every node's config version agrees. Refuses, touching nothing, if any
   of that is false.
1. **Traffic-stopped confirmation** — the prerequisite above.
2. **Stop every node in parallel** (`systemctl stop`, or `SIGTERM` in
   `--local` mode). This drains the archive first (M9's clean-stop path), so
   the restart replays rather than reconstructs.
3. **Verify every stopped node's durable position agrees**, read from each
   node's leftover `cnc2.dat` via `uc2ctl status` (the file outlives the
   process; a stopped node's page is still valid to read). A mismatch means
   the cluster did not fully drain before stopping, or a node was already
   behind — upgrading now risks losing acked writes on the lagging node, so
   this aborts rather than proceeding.
4. **Run `--upgrade-cmd` on every host** (in parallel over ssh in fleet mode;
   once, locally, in `--local` mode).
5. **Start every node.**
6. **Wait, bounded 60 s,** for every node to report the same config version
   and exactly one node reporting `leader=true can_serve=true`.
7. **Print the measured downtime** and exit 0.

Steps 5 and 6 are in that order for a reason that became load-bearing in
`2.13.0`: a node does not open its attach door until it has joined its
cluster, so a service started against a node whose peers are not up yet sits
in its bounded `NodeBooting` wait (default 10 s) and then fails. **Start all
the nodes, wait for a leader, then start the services** — which is what step
6 buys you. Service units ordered `After=` their node unit satisfy this only
because the 10 s wait covers a normal election; nothing in systemd knows
about the cluster.

## The abort path is load-bearing

From step 2 (stopping nodes) through step 4 (`--upgrade-cmd`), **any
unexpected failure restarts every node on whatever binary is currently in
place** rather than leaving the cluster down — this is the un-upgrade path,
and it is not optional or best-effort. If `--upgrade-cmd` succeeded on some
hosts and failed on others before the abort fires, the restart can bring the
cluster up on a **mixed** version — which the 0.5.0 posture above self-stalls
commits on rather than doing anything unsound, but it is not automatically
healthy. The script says so explicitly in its abort log; run `uc2ctl status`
on every node before trusting the cluster again, and re-upgrade only once
every node agrees.

**Step 6 (waiting for convergence) is not covered by this path.** By step 6
every node has already been stopped, upgraded, and told to start — there is
nothing left to "un-upgrade" in the way steps 2-4 mean it, so a step 6
timeout does not trigger a restart of its own; it just gives up waiting and
exits `1`, leaving nodes exactly as step 5 left them. See the exit-code table
below.

## Exit codes

| Code | Meaning |
|---|---|
| `0` | upgraded; one leader serving with matching config version; `DOWNTIME: <secs>s` printed |
| `1` | three different situations, all exit `1`, **not all "nothing was touched"**: **(a)** refused before touching anything (preflight, missing `--yes-traffic-stopped`, bad arguments) — no node was stopped, upgraded, or restarted; **(b)** `abort_restart` fired on a failure at steps 2-4 (a stop that didn't confirm, disagreeing durable positions, or a failed `--upgrade-cmd`) — the un-upgrade path restarts every node and confirms every one back up before this exit; **or (c)** a **step 6 convergence timeout** — by step 6 every node has already been stopped, had `--upgrade-cmd` run, and been told to start, so this is not "nothing touched" either, but it is also NOT the un-upgrade path: a timeout does not call `abort_restart` or attempt a restart of its own, it just gives up waiting. Nodes are left exactly as step 5 left them (typically already on the NEW binary, possibly including one that never confirmed starting) — run `uc2ctl status` on every node before assuming anything |
| `3` | aborted after stopping nodes, and the abort path's restart (plus one retry) still left at least one node down — manual operator action required: start it by hand, then check `uc2ctl status` everywhere before touching the cluster again |

`3` is deliberately distinct from `1` so a monitoring wrapper can tell
"un-upgrade succeeded, cluster is fine" from "un-upgrade itself is in
trouble" apart without parsing the log.

## Reading the downtime number

`DOWNTIME` is `<last node's stop timestamp> → <first moment step 6 confirmed
convergence>`, in the script's own wall-clock. The stop side is exact; the
convergence side is measured only at a step-6 poll boundary — the poll
interval is `POLL_SECS`, `${UC2_FLAGDAY_POLL_SECS:-1}` (default 1s) — so the
printed number can read up to one `POLL_SECS` higher than the true
convergence instant. Tighten `POLL_SECS` if you need a finer bound; the
default trades a slightly padded number for fewer `uc2ctl status` calls
during the wait.

A number from a dev box or a small local run is a smoke test, not a bar — see
[Benchmarks](../BENCHMARKS.md) for how this project's gates separate the two.
The fleet-measured downtime bar for this script is a separate, pre-committed,
user-approved step; this page states no number.

## Stdout is now empty (2.10.0)

`uc2-node` and `uc2-gateway` write **nothing** to stdout as of 2.10.0. Every
record they emit — startup, role changes, drain, stop, the gateway's 10 s
stats — is a JSON line on **stderr**.

Before upgrading, check whether anything you run parses daemon stdout:

```sh
grep -rn "uc2-node\|uc2-gateway" /etc/systemd/system /opt/*/bin 2>/dev/null | grep -i "stdout\|| *grep\|awk\|tee"
```

Then fix each one:

- **A supervisor or log shipper reading stdout** — point it at stderr, or at
  the merged stream. Under systemd nothing changes: journald captures both.
- **Anything matching the old prose lines** (`uc2-node: node 0 listening on
  …`, `uc2-node: node 0 is now LEADER (term 1)`, `uc2-gateway: conns=… `) —
  match the JSON records instead: `"event":"node_listening"`,
  `"event":"became_leader"`, `"event":"gateway_stats"`. The full catalogue is
  in [Monitor a cluster](monitor-a-cluster.md#structured-records).
- **Nothing at all** — if you only read journald or the systemd units as
  shipped, this needs no action.

The pre-start refusal lines (`uc2-node: refusing to start: …`, the
volatile-filesystem `WARNING`) stay human prose on stderr, deliberately: they
are emitted before `[log] level` is read, and their machine-readable half is
the exit code — **2** for a refused config, **1** for a runtime failure.

## The `ultima_db` feature is gone (2.10.0)

If you build a service against `uc_service` with `features = ["ultima_db"]`,
that feature no longer exists and the build will fail by name. It provided a
`StoreStateMachine` adapter over the `ultima-db` crate. Supply your own
`StateMachine` instead — UC ships no store and prescribes no snapshot
encoding. `uc_lincheck`'s `RegisterSm` and `ListAppendSm` are worked examples
of the `StateMachine` + `SnapshotStateMachine` pair.

## Config choices added in v2.6.0: `[crypto].enabled` and `[admin]`

`v2.6.0` (M12b) made two sections of `node.toml` **explicit choices**
(spec §3.3): `[crypto]` gained a required `enabled` key, and `[admin]`
became a new required section. Absent means neither "off" nor "unchanged" —
it is a named startup refusal:

```
uc2-node: [crypto] section is required: set enabled = false for cleartext (the default posture) or enabled = true with key_path/allowlist_path
uc2-node: [admin] section is required: auth = "hmac" with keys = [...] or auth = "none" (filesystem access is the boundary)
```

**A `node.toml` written for M9–M11 refuses to start on `v2.6.0`+ until both
are added.** This is not a wire flag day — it changes nothing about the wire
protocol, the cnc page, or what any *other* node sees — so it does not by
itself need the whole cluster stopped together the way
[wire crypto](encrypt-node-traffic.md) or a
[binary upgrade past 0.5.0](#why-this-was-a-flag-day-until-2150)
does. It is a **per-host config edit**, done once before that host's binary
swap. In practice, run it during the same maintenance window as
`scripts/uc2_flag_day.sh` anyway — you are already touching every
`node.toml` to install the new binary, and there is no reason to make two
separate passes over the fleet.

Paste this into every `node.toml` to keep today's posture unchanged (the
same posture every pre-`v2.6.0` `node.toml` had implicitly):

```toml
[crypto]
enabled = false

[admin]
auth = "none"
```

If you want the new HMAC admin authentication instead of the `auth = "none"`
boot warning, see
[Change cluster membership: if the cluster requires signed admin requests](change-cluster-membership.md#if-the-cluster-requires-signed-admin-requests) —
generate a key with `uc2ctl gen-admin-key` first, since the config needs the
key's path before the node will start with `auth = "hmac"`.

`packaging/node.example.toml` ships both sections uncommented, annotated
with the posture each choice implies — diff your fleet's config against it
to confirm nothing was missed.

## Ring format change in 2.7.0: restart a host's processes together

2.7.0 changes the format of the two client-facing shared-memory rings
(`ingress.ring`, `query.ring`) — per-record commit, new file magic
`ULTRNG2`. This is **not** a wire flag day: nothing about node-to-node
traffic, the cnc page layout or the journal changes, and a 2.7.0 node
replicates with a 2.7.0 peer exactly as before.

What it does change is the *same-host* contract. A pre-2.7.0 service,
gateway or client that attaches to a 2.7.0 node's instance directory is
refused with a ring magic mismatch (and the reverse), because the two
binaries disagree about what a slot's first word means. So on each host,
stop and restart **all** of them together:

```bash
sudo systemctl stop uc2-gateway uc2-service uc2-node
# swap binaries
sudo systemctl start uc2-node uc2-service uc2-gateway
```

`scripts/uc2_flag_day.sh` already stops and starts whole hosts, so a flag
day run covers this by construction — the rule matters for anything that
attaches *outside* those units, most often a long-lived embedded client.
The rings are volatile (recreated at boot), so there is nothing to migrate
and no rollback step beyond restarting the old binaries together.

## Control-page change in 2.8.0: restart a host's processes together

M14 grows `cnc2.dat` from 4 KiB to 8 KiB and bumps its version to 3.0. Every
same-host party — the node, each service, every client, `uc2ctl`, the gateway
— refuses a page whose major version differs, by name (`VersionMismatch`),
so a 2.7 service cannot attach to a 2.8 node or vice-versa. This cnc-page
change is itself **same-host**, not cluster-wide: it needs no coordination
across hosts by construction. Whether `2.8.0` *also* carries a node↔node wire
flag day is a separate question, decided by that release's own notes — do not
assume a per-host upgrade is safe until they say so. (For the record: M14c is
the milestone that bumps the wire to `0.6.0` for `SNAP_BEGIN`, and that one
**is** a whole-cluster restart, on the same terms as every prior wire flag
day.) On each host:

1. stop the clients, the gateway, then the services, then the node;
2. swap binaries;
3. start the node (it re-creates the page and unlinks the old singular ring
   names), then the services with their `--service-id`, then the clients.

The instance directory's journal, state and snapshots are reused as-is. If
`[services]` is absent, the node declares FSM 0 only and behaves exactly as
before, except that a service must now attach as id 0 (the default).

## Wire change in 2.8.0: `SNAP_BEGIN` carries every FSM's snapshot (0.6.0)

M14c moves the node-to-node wire from `0.5.0` to `0.6.0`. One datagram
changes: `SNAP_BEGIN`, which opens a snapshot session. Its body grew from 26
to 34 fixed bytes and now names *which* FSM's artifact is being shipped
(`service_id`), the sender's declared FSM set (`services_declared`), and a
layout discriminator — because a session now carries **one artifact per
declared FSM**, not one artifact. `DATA`, `NAK`, `APPEND_POSITION`,
`TERM_MAP`, the 16-byte header and every admin datagram are byte-identical to
`0.5.0`.

This is separate from the same-host cnc 3.0 restart above: nodes, services
and clients on a host restart together because the page grew to 8 KiB
([cnc page](../reference/cnc-page.md)); the wire flag day below governs
node-to-node traffic across the whole cluster instead.

**This is a whole-cluster flag day, on the same terms as every prior one.**
A mixed `0.5.0`/`0.6.0` cluster replicates and elects normally — which is
precisely why it is dangerous: the damage is confined to snapshot sessions,
so it surfaces later, when a learner joins or a node falls below the purge
floor, not at upgrade time. A `0.5.0` receiver handed a `0.6.0` `SNAP_BEGIN`
misreads its config length and drops or mis-adopts the carried membership; a
`0.6.0` receiver refuses the session by name. Nothing in the header enforces
this (`version::CURRENT` is documentary and has no caller on any receive
path) — the rule is operational: **stop every node, swap, start every node.**
`scripts/uc2_flag_day.sh` does exactly that and needs no new flags.

Two named, counted refusals on the receiving node tell you a cluster is
mixed or mis-declared instead of leaving a joiner silently stuck. Each is
counted (`Node::snapshot_session_refusals`) *and* named in a
`snapshot_session_refused` log record the first time it happens:

| refusal | meaning | fix |
|---|---|---|
| `peer wire 0.5.0` | the `SNAP_BEGIN` is too short for `0.6.0` (a `0.5.0` body) or its layout byte is not one we speak | finish the flag day: some node is still on `0.5.0` |
| `declared-set mismatch` | the sender's declared FSM set differs from this node's (`[services] ids` at the time of this 2.8.0 upgrade; `[services] names`, in the same order, since FSM identity 2.11 — see the section below) | make `[services]`'s declared set identical, in the same order, on every node, then restart the odd one out |

Both drop the session; the joining node keeps NAKing, so the cluster is
stalled-but-safe until the mismatch is fixed — never half-installed.

The sending side has its own, quieter counterpart: a leader that cannot
assemble a complete set declines to open the session at all and says why once
(`snapshot_session_declined`, `reason = "floor 0" | "missing artifact" | "set
does not cover declared"`). `floor 0` is ordinary — nothing has snapshotted
yet, so the joiner is served by journal replay. The other two mean a declared
FSM's newest artifact is missing on the leader; the joiner re-NAKs until it
appears.

The snapshot **directory layout is unchanged from 2.8.0's own layout**:
artifacts already live in `snapshots/<service-id>/` (M14a). No migration, no
rollback step beyond restarting the old binaries together.

## Nothing to do for 2.8.1

`2.8.1` is a **proof-only** release (M14c2): it adds no feature, no config key
and no default change, and it moves neither the node↔node wire (`0.6.0`) nor
the control page (cnc `3.0`). Coming from `2.8.0`, the upgrade is the plain
binary swap this script already performs — none of the version-specific
sections above apply, and there is no migration or extra rollback step. Run the
same flag day anyway: it is the procedure this system supports, and it gives
you the same measured downtime number.

## Wire + cnc change in 2.11.0: FSM identity, log time and the cluster FSM (`0.7.0`, cnc `3.1`)

Five features share this flag day, because all five were implemented before
the release was cut:

- **FSM identity** gives each state machine a name declared in code and binds
  it to the row (spec
  `docs/superpowers/specs/2026-09-02-uc2-fsm-identity-design.md`;
  plain-language explainer:
  [`docs/notes/uc2-fsm-identity-and-deterministic-ids-explained.md`](../notes/uc2-fsm-identity-and-deterministic-ids-explained.md)).
- **Log time and timers** puts a leader-written timestamp in every log frame
  header and adds a `TIMER` frame type (spec
  `docs/superpowers/specs/2026-09-02-uc2-time-and-timers-design.md`;
  explainer:
  [`docs/notes/uc2-log-time-and-timers-explained.md`](../notes/uc2-log-time-and-timers-explained.md)).
- **The replicated schedule table**, built on that (same spec, §5; explainer
  section:
  [The schedule table](../notes/uc2-log-time-and-timers-explained.md#the-schedule-table)).
  It needs **no per-host edit**: the table lives on the log, and a fresh
  cluster simply has none until an operator applies one.
- **The cluster FSM** puts membership, that schedule table and a new
  replicated settings record into one internal state machine, applied at
  commit and snapshotted into its own artifact (spec
  `docs/superpowers/specs/2026-09-05-uc2-cluster-fsm-and-coordinated-snapshot-design.md`;
  explainer:
  [`docs/notes/uc2-cluster-fsm-explained.md`](../notes/uc2-cluster-fsm-explained.md)).
  **This one does need a per-host edit** — two moved `node.toml` keys — see
  below.
- **Coordinated snapshot instants** make a snapshot something the whole
  cluster takes at one log position on command, adding frame type `7`
  (`SNAPSHOT`) with a standby flag and datagram kinds `22`/`23` (same spec,
  §5; explainer section:
  [Instants](../notes/uc2-cluster-fsm-explained.md#instants-one-position-one-set)).
  It needs no `node.toml` edit, but it **does** need `snapshots/` emptied on
  a dev box — see below — and it removes an SDK type.

It is **one combined flag day**, on both lines at once — the same-host cnc
page (`3.0` → `3.1`) and the node-to-node wire (`0.6.0` → `0.7.0`) — because
all five changes ship in the same release.

**The `[services] ids` → `names` edit, required on every host.** `[services]`
is no longer optional (absent used to mean `ids = [0]`; it now refuses to
start, the same posture `[crypto]`/`[admin]` have had since 2.6.0), and its
`ids` key is refused outright rather than silently accepted:

```
services.ids was replaced by services.names (FSM identity): list the FSM
names in row order, e.g. names = ["kv", "orders"]
```

Before restarting any host, rewrite every `node.toml`'s `[services]` from
`ids = [0, 1]` to `names = ["<row-0-name>", "<row-1-name>"]` — **the same
names, in the same order, on every node** (row = list index, still
contiguous from 0). Pick each row's name to match what the attaching
service's `S::NAME` actually is; a service that does not find its name in
the node's list is refused `UnknownFsm`, not silently parked. `--service-id
<id>` is gone from every harness binary; the service side now takes
`--fsm <name>` (or, for a production service using `ServiceConfig`
directly, nothing at all — it attaches by its own `S::NAME`).

**The `uc_` name prefix is now reserved**, for UC's own internal state
machines (the cluster FSM declares `uc_cluster`). A `[services] names` entry
starting with it is a named startup refusal; rename the row before the flag
day if you have one.

**Existing snapshot artifacts are refused.** Every artifact now begins with a
16-byte `ULTSNAP1` envelope naming the position it was built at, and a file
written by an earlier build has none. On a real deployment this cannot bite —
nothing released ever wrote one — but a development instance directory carried
across the flag day must have its `snapshots/` emptied, or the first install
path to touch an old file refuses it by name. The node rebuilds the set at the
next instant.

**`SnapshotPolicy` is retired from the SDK.** A service that configured
`ServiceConfig::snapshot_policy(SnapshotPolicy { interval_bytes })` (retired)
no longer compiles: delete the call. `start_with_snapshots()` was the whole
opt-in as of this release, and the trigger moved to the log — command an
instant with `uc2ctl snapshot`, or set the replicated `snapshot_interval_bytes`
with `uc2ctl settings apply`. (#67 later renamed `start_with_snapshots()` to
`start()` and made snapshot support mandatory — see
[State-machine contract § Snapshots](../reference/state-machine-contract.md#snapshots-required-the-instant-the-envelope-and-the-exclusive-frontier).)
Check your `install_snapshot` while you are there: the position it is handed
is an **exclusive** frontier and it must return that position, not report it
from `last_applied()`
([State-machine contract § Snapshots](../reference/state-machine-contract.md#snapshots-required-the-instant-the-envelope-and-the-exclusive-frontier)).

**The `max_payload` edit, required only on hosts that PIN it.** If a
`node.toml` sets `max_payload` to a value below what a full schedule table
needs, the node is **refused by name** at startup:

```
max_payload (512) cannot carry a full schedule table: MAX_SCHEDULE_ENTRIES
(32) encodes to a 1072-byte CLUSTER frame body. `uc2ctl schedule apply` would
refuse any table over 15 entries on this node — raise max_payload to at least
1072.
```

**Delete the line.** The default is derived from the path budget (1312 B at
the built-in MTU) and satisfies the check on its own; pinning a number only
takes you back out of step with it the next time the MTU or a frame layout
moves. If you must pin, use at least 1072 — and note the crypto ceiling: a
value above 1312 refuses to start once `[crypto].enabled = true`, because a
max-size frame plus the GCM tag no longer fits one datagram.

The example config no longer suggests a value. This is the one 2.11.0 edit
that only bites hosts that opted into pinning: a `node.toml` that never
mentioned `max_payload` needs no change.

**If you are going straight to `2.12.0`, ignore the pinning advice above**:
the key is retired there and refused by name on *every* host that still
carries it, whatever its value. Delete the line — see [the `2.12.0`
section](#wire--cnc-change-in-2120-jumbo-frames-080-cnc-32) below.

**The `admission_bytes` and `fsm_lag` edit, required on every host that sets
either.** Both are cluster-wide policies now, so both are **refused by name**
where they used to live:

```
admission_bytes is a cluster-wide setting since the cluster FSM (2.11.0):
put it under [settings] to seed genesis, and change it with
`uc2ctl settings apply`
```

```
services.fsm_lag is a cluster-wide setting since the cluster FSM (2.11.0):
put `fsm_lag` under [settings] to seed genesis, and change it with
`uc2ctl settings apply`
```

Move both into a new `[settings]` section, with the **same values on every
host** — the section is a **genesis seed**, read only when the instance
directory is fresh, and from the first cluster command onward the file's copy
is ignored:

```toml
[settings]
admission_bytes = 262144
fsm_lag = "16MiB"
```

On an **existing** cluster the seed is never read at all, so the values that
take effect are whatever the cluster FSM's genesis defaults were — "derive at
use" for both. If your fleet ran a non-default `admission_bytes` or `fsm_lag`,
apply it explicitly after the upgrade rather than relying on the file:

```sh
uc2ctl settings apply settings.toml --instance-dir /srv/uc2/n0 --app-id myapp \
  --admin-key /etc/uc2/admin/alice.key
uc2ctl settings show --instance-dir /srv/uc2/n0 --app-id myapp
```

See [Configuration § `[settings]`](../reference/configuration.md#settings) and
[`uc2ctl` § `settings apply`](../reference/uc2ctl.md#settings-apply).

**cnc 3.1**: the once-reserved slot line 7 becomes node-written at boot
(the row's name, NUL-padded, plus its FNV-1a 64 hash); the status line's
second word carries the attached service's packed version, written at
attach. Log time adds two more previously-unused words to the same page
version: `log_time_ns` at page 1 offset `4048` (archive-agent-written, never
lowered) and per-row `timers_pending` at slot line 7 `+488`
(consensus-agent-written, republished every pass). Same-host restart only,
exactly like the 3.0 bump.

**One new file per declared FSM in the instance directory**:
`svc_sched.<row>.ring` (SPSC, service → node, 1 MiB), created by the node at
boot. The per-row reservation goes from 5 MiB to **6 MiB**, so the boot
reservation is ~79 MiB at the defaults with one FSM and ~121 MiB with eight
([Instance directory § Limits](../reference/instance-directory.md#limits)).
Check free space on each host before the flag day; a host that cannot reserve
it gets a named startup refusal, not a mid-run failure. Since the cluster FSM
that ring is written and drained only while a node **leads**.

**New instance-directory paths, created on their own**: `snapshots/cluster/`
(the cluster FSM's `snap-<pos>.ultcluster` artifacts) and a transient
`settings.pending` beside `schedules.pending`. There is **no**
`state/schedules.state` — the schedule table is cluster data and lives in the
artifact. `uc2ctl backup`/`verify-backup`/`restore` carry `snapshots/cluster/`
like any other artifact family, so a restored node comes back with the
cluster's membership, schedule table and settings rather than rebuilding them
from the restored journal.

**Wire 0.7.0, part one (FSM identity)**: `SNAP_BEGIN`'s `services_declared`
bitmask becomes a per-row identity-hash array (`identity: [u64; 8]`), and a
per-row packed version array (`version: [u32; 8]`) is added — see [the wire
protocol
reference](../reference/wire-protocol.md#snap_begin-body-wire-070-layout-v4)
for the exact layout. A 0.6.0 sender's shorter body is dropped by the same
length check that drops a 0.5.0 body today: **a mixed cluster stalls a
joiner rather than installing a wrong or half-checked artifact.**

**Wire 0.7.0, part two (log time and timers)**: the 32-byte **log frame
header is relaid**. `session_id: u64` and `correlation_id: u64`, of which the
client only ever filled 32 bits each, become `client_id: u32` + `seq: u32`,
freeing 8 bytes for `time_ns: u64` — the leader's stamp on the frame. A new
frame type, `TIMER = 5`, carries a 24-byte body (`identity_hash ‖ timer_id ‖
deadline_ns`). See [the wire protocol reference](../reference/wire-protocol.md#log-frames).

**Wire 0.7.0, part three (the cluster FSM)**: frame type `4` becomes
`CLUSTER`, a kind-dispatched command (`1` membership, `2` the schedule table,
`3` the settings record) where it used to be `CONFIG`; `SNAP_BEGIN` loses its
trailing `config` tail and becomes fixed-length at 120 B (layout **V4**),
because the snapshot session now carries the cluster FSM's own artifact under
the reserved `service_id = 255`. Three numbers are **retired before shipping**
and reserved so they are never reassigned: frame type `6`
(`SCHEDULE_TABLE`), datagram kind `21` (`SNAP_TABLE`) and `SNAP_BEGIN`
layout `2`. A body carrying a retired discriminator is refused **by name**.

**This half of the flag day is sharper than every previous one, and deserves
saying plainly.** Every prior wire bump was caught by a length check: an old
peer's body was too short, so it was dropped and the cluster stalled. A relaid
header is the same length. A `0.6.0` peer's frames *parse* on a `0.7.0` node
and mean something different (its `correlation_id` reads as a timestamp; its
`session_id`'s low half reads as a sequence). **Stop every node before
starting any node on the new binaries.** The header is still 32 bytes and the
command payload ceiling is unchanged (1344 B crypto-off / 1312 B crypto-on),
so nothing about sizing or configuration moves.

Two new named, counted refusals, on top of the existing `peer wire ≤ 0.6.0`
one:

| refusal | meaning | fix |
|---|---|---|
| `identity mismatch` | the sender's per-row identity hashes disagree with this node's at some row `r`, positionally — same names in a different order counts as a mismatch, not just a different set | make `[services] names` identical, **in the same order**, on every node; the log line names the row and both sides' FSM names ("row 1: ours=orders, theirs=kv") |
| `version mismatch` | both sides report a non-zero `VERSION` for the same row and they disagree | attach the same build of that FSM's service everywhere; the log line names the row, both FSM names and both packed versions. `0` on either side means *unknown* (a joiner whose service hasn't attached yet), not a mismatch — this refusal only fires when both sides are non-zero |

Both are counted (`Node::snapshot_session_refusals()`, now `(u64, u64,
u64)` — legacy-peer, identity, version) and drop the session; the joiner
keeps NAKing, stalled-but-safe, never half-installed. Steady state: the
row's exported `uc2_service_identity_hash`/`uc2_service_version` gauges
differ across nodes even before any snapshot session runs, and the
`Uc2ServiceIdentityDrift`/`Uc2ServiceVersionDrift` alerts fire — see
[Monitor a cluster](monitor-a-cluster.md).

**The flag-day procedure is the same shape as every prior one**: rewrite
every host's `node.toml` first (`ids` → `names`, in the same order), then
on each host, stop clients → gateway → services → node, swap binaries,
start node → services (now attaching by name, no `--service-id`) → clients.
`scripts/uc2_flag_day.sh` covers the binary-swap half unchanged; the config
edit is a manual step before it, same as any other `node.toml` change.

On-disk layout is **unchanged** for durable data: snapshots still live in
`snapshots/<row>/`, keyed by row, not name — no migration. The journal's
recorded frames carry the new header from the moment a `2.11.0` leader
appends, and older recorded frames are replayed with whatever header they
were written with; nothing rewrites the archive.

**After the flag day**, three things are new to watch
([Monitor a cluster](monitor-a-cluster.md)): `uc2_log_time_ns` should be
advancing on every node, `uc2_log_time_lag_seconds` should sit near zero on
the leader (the `Uc2LogTimeFrozen` rule fires above 5 s for 30 s), and
`uc2ctl status` prints `log_time_ns=` plus a `timers_pending=` field on each
per-FSM row.

## Wire + cnc change in 2.12.0: jumbo frames (`0.8.0`, cnc `3.2`)

Two features share this flag day. **Jumbo-frame MTU discovery** moves the
wire and the control page; the **monotonic log clock** rides along with no
flag-day surface of its own (no wire field, no cnc word, no config key — a
backward wall-clock step is now smeared at 500 ppm instead of freezing the
log's clock, which narrows `Uc2LogTimeFrozen`'s meaning to "nothing is being
appended" and adds the `uc2_log_clock_smear_ns` gauge and the `log_clock_step`
record; see [Monitor a
cluster](monitor-a-cluster.md#the-log-clock-and-the-timer-families-2110)).

**Wire 0.7.0 → 0.8.0: two new datagram kinds, no layout change.** `PROBE`
(24) and `PROBE_ACK` (25), both pairwise, carry the path-MTU ladder. Every
other datagram is byte-identical to `0.7.0`. A `0.7.0` peer counts the two
kinds as unknown and drops them, so a mixed cluster simply never raises its
ceiling — safe, but still unsupported: **stop every node before starting any
node**, as with every other flag day. The `Settings` record inside a `CLUSTER`
frame grows one `u32` (`datagram_mtu`), taking `SETTINGS_LEN` from 29 to 33.

**cnc 3.1 → 3.2: one new word, same-host restart.** `payload_ceiling` at
offset 3984 (`u64`, written by the consensus agent, read per submit by every
client and by the gateway edge) is the cluster's live command ceiling. A 3.1
attacher refuses by version, exactly as the 3.0 → 3.1 bump described, so each
host's clients, services and gateway restart with its node.

**No wipe, and this is the first flag day where that needed saying.** The
cluster FSM shipped in `2.11.0`, so a cluster artifact and committed `CLUSTER`
frames now survive an upgrade. A **version 1** `Settings` record — the 29 B
`2.11.0` shape — is still accepted on read and maps to `datagram_mtu = 0`,
which means "the baseline rung". Nothing in `snapshots/cluster/` needs
clearing and no instance directory needs rebuilding.

**The `max_payload` edit, required on every host that still sets it.** The key
is retired and refused by name:

```
max_payload is no longer configurable (2.12.0): the command payload ceiling is
discovered from the path MTU between nodes and committed cluster-wide. Delete
the line. To REQUIRE jumbo frames, set force_jumbo_frames = true instead.
```

Delete it from every `node.toml` before the flag day — whatever its value, and
whether or not it was at the default. There is no replacement key: the ceiling
is discovered per cluster and replicated, starting at the 1408 B baseline rung
(1344 B crypto-off / 1312 B crypto-on) and rising only once every path between
members has proven a larger one. The optional new key is
`force_jumbo_frames` (default `false`), which turns that discovery into a
startup gate — see [Run a cluster on jumbo frames](jumbo-frames.md).

**The behaviour change to check for before the flag day: do-not-fragment.**
The replication socket now sets DF (`IP_MTU_DISCOVER` / `IPV6_MTU_DISCOVER` +
`IPV6_DONTFRAG`), so a datagram the route cannot carry fails locally with
`EMSGSIZE` instead of being fragmented. **A path below 1436 B (IPv4) or
1456 B (IPv6) that works today by fragmenting UC's 1408 B baseline datagrams
will fail by name after the upgrade** — `uc2_send_emsgsize_total` rises and
`Uc2PathBelowMtu` fires. That is deliberate (one lost fragment loses the whole
datagram, and the loss surfaces as an unexplained NAK storm), but it is worth
proving the baseline first, from each node to each peer:

```sh
# -s 1408 puts 1436 B on the wire (1408 + 8 ICMP + 20 IPv4) — the same wire
# size as UC's 1408 B baseline datagram. -M do sets do-not-fragment.
ping -M do -s 1408 -c 3 <peer>
```

**A node host must run Linux (or Android).** The three DF socket options exist
in `libc` for those targets only, and running without DF would make discovery
over-report a path, so `uc2-node` refuses to start anywhere else, by name
(`do-not-fragment is not available on this OS`). This bites development boxes,
not fleets: a macOS or BSD machine can no longer run a node, though the client
and service crates are unaffected.

**After the flag day**, nothing is required of the operator — but one decision
is made for you, irreversibly: on a fabric that carries jumbo frames, the
cluster commits a jumbo rung as soon as two members have probed each other, the
rung is monotone with no opt-out, and from then on **no member whose path is
narrower can ever join** (`path_below_committed_mtu`, by design) — so a
cross-region learner over 1500 B peering becomes impossible for the life of the
cluster, with a new cluster as the only remedy. If you will need a narrower
member later, keep one narrow path in the cluster from the start; that is the
only opt-out, and [Run a cluster on jumbo
frames](jumbo-frames.md#before-you-start-this-is-a-one-way-door) explains it.

Each node probes
its peers on boot and on every membership change; the leader commits the
minimum once every member has answered, and every node applies it at commit.
Verify with `uc2ctl status`'s `ceiling:` line and the
`uc2_datagram_mtu_bytes` / `uc2_probe_min_mtu_bytes` gauges. A cluster on an
ordinary 1500 B network stays at the baseline and behaves exactly as it did
before, which is the intended outcome rather than a failure. A **one-node**
cluster also stays at the baseline: an empty member set is no evidence, so
discovery begins when the first peer joins.

## Wire + cnc change in 2.13.0: upgrade pins and snapshot reports (`0.9.0`, cnc `3.3`)

The FSM upgrade lifecycle's platform half. Two new cluster records —
`UpgradePin` ("at position `origin`, row `r` went from version `from` to
version `to`") and `SnapshotReport` (the per-node artifact hashes for one
`(row, position)` pair, with a deterministic verdict) — plus the operator
path for pins (`uc2ctl upgrade pin` / `upgrade show`, admin op 10) and the
cnc words a service reads at attach. See [The cluster
FSM](../notes/uc2-cluster-fsm-explained.md) § Pins and reports.

**Wire 0.8.0 → 0.9.0: two new `CLUSTER` kinds, and this one is NOT
benign.** `FRAME_TYPE_CLUSTER = 4` gains `kind 4 = UpgradePin` (20 B) and
`kind 5 = SnapshotReport` (16–112 B). Unlike `2.12.0`'s `PROBE`/`PROBE_ACK`
— pairwise datagrams a `0.7.0` peer could simply drop — these are **frames
on the replicated log**, and a `0.8.0` node applies an unknown kind as
"undecodable": it pushes a refusal byte, advances `applied`, and moves on.
It does not stall and it does not refuse. So a `0.8.0` node in a `0.9.0`
cluster keeps committing while its cluster FSM **silently diverges** from
every upgraded peer's — it holds no pins and no reports, its cluster
artifact differs, and a joiner it serves inherits that hole. There is no
counter that catches this. **Stop every node before starting any node**, as
with every other flag day; here the consequence of not doing so is silent
rather than loud.

**cnc 3.2 → 3.3: four new words on the service status line.**
`upgrade_origin` at slot offset 16, `pinned_version` at slot offset 24,
`pin_seq` (the seqlock commit word) at slot offset 32, and `pinned_from` at
slot offset 40 (`u64` each, low 32 bits of `pinned_version`/`pinned_from` =
the packed version) — **node**-written by the `uc2-cluster` agent and
republished on every view publish; `0` = no pin. `pinned_from` is the version
the artifact AT `upgrade_origin` was built by, which the service needs for
its own install cross-check (below). The triple is published under `pin_seq`
(bumped odd, `pinned_version` stored, `pinned_from` stored, `upgrade_origin`
stored, bumped even), so a reader gets a triple that was stored together or
no pin at all — never an old origin beside a new version. A reader that
cannot read the triple consistently (`PinRead::Contended`, after 64 collided
attempts) fails CLOSED rather than treating it as unpinned. This makes line 0
the second line with two writers — the service still owns `status`/`version`
at attach, the node owns the pin words. A 3.2 attacher refuses by version,
exactly as the 3.1 → 3.2 bump described, so each host's clients, services and
gateway restart with its node.

**Cluster image 1 → 2, and no `snapshots/cluster/` wipe is required by this
change.** The artifact under `snapshots/cluster/` grows two
length-prefixed blobs (the pin list and the report list) after the settings
record. A **version 1** image — the `2.11.0`/`2.12.0` shape — is still
accepted on read and maps to an empty pin list and an empty report list, the
same `Settings` v1/v2 precedent `2.12.0` set. A restarting node reads its own
pre-upgrade artifact, and a first `uc2ctl upgrade pin` rewrites it at v2.

**The per-row snapshot artifacts need a wipe: `ULTSNAP1` is refused by
name.** Plan B2 replaces the 16-byte `ULTSNAP1 ‖ P` envelope (`2.11.0`) with
a 24-byte `ULTSNAP2 ‖ P ‖ version` envelope on the row artifacts under
`snapshots/<row>/` — the version field is what lets an unpinned install
cross-check against `S::VERSION` and a pinned install cross-check against the
pin's `from`, and a header with no version stamp cannot support either
check. So, decided (not the open question an earlier draft of this section
left it): a `ULTSNAP1` artifact is **refused by name**
(`EnvelopeError::Legacy`), the same posture as a pre-2.11 artifact with no
envelope at all. **Clear `snapshots/<row>/` on every node once, as part of
this flag day** — `ULTSNAP1` artifacts are refused by name; `snapshots/cluster/`
is **untouched** by this change (cluster image 1 → 2, above, is
forward-compatible on read, no wipe required there). Until the next
coordinated instant rebuilds the row's artifacts in the new layout, every
node's rows reconstruct from the **journal** — the wipe removes artifacts,
not the floor: a node's purge floor is seeded at boot from `state/`, which
this step does not touch.

**That is the precondition, and it is not a detail: the wipe is safe only if
the row's state machine is durable, or the journal still reaches genesis.**
Deleting `snapshots/<row>/` while the floor (and the journal purge behind it)
sits above what the state machine holds leaves the gap guard with no covering
artifact, and every node fail-stops the row with `SnapshotRequired`. Concretely:

- **purge off (the default, `PurgePolicy::Disabled`)** — the journal still
  reaches genesis. Wipe freely; rows replay from 0 until the next instant.
- **purge on with a DURABLE state machine** — the SM's own cursor covers the
  floor, so nothing needs the deleted artifacts. Wipe.
- **purge on with an in-memory state machine** — **you cannot wipe.** There is
  no path from the floor to a live row without an artifact, and `ULTSNAP1` is
  refused by name. Such a deployment must wait for the envelope migration tool
  (rewrite `ULTSNAP1 ‖ P` as `ULTSNAP2 ‖ P ‖ <operator-supplied version>`
  in place), which is **not shipped** — it is a backlog item. Do not upgrade
  to 2.13.0 before it exists.

**Take a fresh instant after the wipe, before any `uc2ctl upgrade pin`.** Run
`uc2ctl snapshot` as soon as the fleet is up on 2.13.0. Until one completes,
the node's newest complete-set position is **seeded from the durable snapshot
floor in `state/`** — which the wipe does not touch, so it still names a
pre-wipe instant — and `pin_no_set` (reason 54) checks a pin's origin against
exactly that word
— so a stale value would let a pin be ACCEPTED at an origin whose artifact
the wipe just deleted, and the attach would then refuse with
`PinnedArtifactMissing` at the worst possible moment.

**The S4 sequence, and what each refusal means.** An upgrade of a row whose
`apply` semantics changed is always `uc2ctl snapshot` (the complete set at P)
→ `uc2ctl upgrade pin --row r --to <ver> --origin P` (records the intent) →
**confirm the pin is visible on every node** → stop every instance of row r,
swap the binary, start (only now does a v_new attach read the pin and install
`snap-<P>` unconditionally) → **`uc2ctl snapshot` again, as soon as the
swapped row has caught up**.

The two added steps are not optional:

- **Confirm the pin everywhere BEFORE stopping any instance.** `uc2ctl
  status` prints `upgrade_origin=`/`pinned=`/`pinned_from=` per row, and
  `uc2_upgrade_pin_origin{service="…",row="r"}` carries the same origin on
  each node's `/metrics`. A pin is a replicated command: a node that has not applied it
  yet reads `PinRead::NoPin`, and a v_new service attaching there takes the
  **unpinned** path — no refusal, no log line, just the §2.3 counterfactual
  on that one node. Check every node, not the leader.

  **The readiness gate backs this step up; it does not replace it.** Since
  `2.13.0` a node does not publish its declared set — the word every attach
  door reads — until it knows a leader, has learned a commit position, and
  its cluster FSM has consumed the log up to that commit. So a service
  **cannot** attach to a node that has not applied the committed cluster
  state, and the specific race this step was written against — a node
  restarting with a pin committed *above* the artifact it recovered from, and
  a co-restarting service attaching in the gap and reading "no pin" — is
  closed. What the gate cannot do is tell you the pin was ever *proposed*, or
  that it committed rather than being refused at the door. That is still your
  check, on every node, before you stop anything.

  Three operational consequences of the gate, all new in `2.13.0`:

  - **Attaching now waits.** `ServiceBuilder::start*`, `Client::connect` and
    `Engine::attach` retry `NodeBooting` internally for `boot_wait`
    (**default 10 s**; `Duration::ZERO` restores the old fail-immediately
    behaviour). `NodeBooting` means "this node has not joined its cluster
    yet", not "its page is missing". A node's own durable position can trail
    commit for a while — the cluster FSM applies at `min(commit, durable)` —
    and while that is transient (a long archive walk at boot, an artifact to
    install, a node catching up), a larger `boot_wait` is the right answer
    rather than treating the refusal as a fault. If it does **not** clear,
    no `boot_wait` is large enough and every attach on that node stays
    refused for the life of the incarnation: a follower's commit cannot
    outrun its own durable, so a persistent inversion is a **leader** whose
    durability path is failing, and the fix is there. The node names the
    clause holding it once, as a `services_declared_withheld` warning —
    `clause="walk_behind"` is this case.
  - **Boot-time observability reads empty.** Until the node joins,
    `/metrics` renders `uc_services_declared 0` with no per-FSM rows and
    `uc2ctl status` prints `declared=[]` with no FSM lines. One
    `services_declared_published` record marks the transition. During a
    rolling flag-day restart, expect a window where a fleet-wide
    declared-set query briefly sees two values.
  - **If you bring nodes up inside one process** (a harness, an embedded
    deployment), **start every node before attaching any service** — a
    service attached after node 0 and before node 1 spends its whole
    `boot_wait` waiting for a leader that cannot be elected yet.
- **Take an instant AFTER the swap — it completes the pin.** Once every
  instance of the row runs `to` and has caught up, run `uc2ctl snapshot`.
  When that set AGREES (`uc2_snapshot_hash_mismatch` reads `0`; the catalog
  lists it agreed) the pin is **complete**: the catalog lists an agreed set
  above the pin record, on the `to` line. (For a cross-line pin only a `to`
  build can freeze above the record — the old one stopped there. For a
  `--patch` pin an old same-line build's set counts too, by the line
  contract.) From then on the row is back to normal — a restarting service
  starts from that set (the node's start set) instead of re-installing the
  origin, and the origin's set is released to ordinary retention, with the
  floor and the journal purge free to move past it. Each node releases once
  it **holds** that set (its own freeze, or an auto-fetch) and any `to`
  instance attached there has caught up to it.
  **Until the pin completes, every node keeps the origin's set and holds its
  journal at the origin**, because any instance not yet upgraded still has
  to install the origin and replay from it; `snapshot_floor_held_for_pin`
  names that hold. With `snapshot_interval_bytes = 0` (the default) nothing
  takes that instant for you, and the journal grows until you do.

See
[`uc2ctl` § `upgrade pin`](../reference/uc2ctl.md#upgrade-pin) for the
command's own door refusals (`52`–`59`, checked before anything is proposed
or replicated). At attach, four more refusals guard the pinned install
itself — see [State machine contract §
Snapshots](../reference/state-machine-contract.md#snapshots-required-the-instant-the-envelope-and-the-exclusive-frontier)
for the full account:

- **`PinnedVersionMismatch`** — a stale binary (not the pin's `to`) tries to
  attach after the pin. Expected and intended: swap in the pinned version.
- **`PinUnreadable`** — the pin words could not be read consistently through
  the seqlock (the `uc2-cluster` agent is mid-publish). Transient; retry the
  attach.
- **`PinRequiresSnapshots`** — the row attached without the install
  capability. A pinned row must be able to install `snap-<origin>`; since #67
  every `ServiceBuilder::start()` carries this capability, so a Rust SDK
  service cannot reach this refusal any more — it stays as `attach`'s
  backstop for a service attached without the current Rust SDK (a non-Rust
  attacher, or a binary built before #67).
- **`PinnedArtifactMissing`** — the pin names an origin whose artifact is not
  on this node (pruned, or never fetched). Run `uc2ctl snapshot fetch`, or
  re-pin at a retained instant.

**A floor already above the origin at pin time is a different failure: a
fail-stop tail replay, not a clean attach refusal.** `pin_no_set` (reason 54)
only checks against THIS node's newest complete set at the moment the pin is
proposed; it says nothing about what a node's own purge floor does between
then and the swap. If a node's purge floor has already advanced past the
pinned origin by the time a v_new binary attaches, `install_snapshot` cannot
land at `origin` at all — the journal below the floor is gone — and the
attach path fail-stops rather than refusing cleanly by name. In practice the
node holds its floor at any pinned origin it has not yet released
(`snapshot_floor_held_for_pin`, below) precisely to keep this from happening
on the node that did the pinning. It stays a real risk for any node whose own
floor can be above the origin when the pin lands, and that is **not only the
node that was offline or behind**: reason 54 compares the origin against the
newest complete set of the node the pin was proposed on (the leader), so a
node that is AHEAD of it — one whose own later instant completed and whose
floor and journal purge have already moved past the origin — is refused
nothing at the door and fails at attach time just the same. Check the floor
on every node, not just the laggards.

**A pinned-but-abandoned upgrade holds the journal indefinitely.** A node
holds its snapshot/purge floor at a row's pinned origin until the pin is
**complete** — the catalog lists an agreed set above the pin record, on the
`to` line: for a cross-line pin, an instant taken after enough instances run
`to` to agree on it; for a `--patch` pin an old same-line build's set counts
too — **and** the pin is released on that node: the node holds that
completion set (its own freeze, or an auto-fetch) and no `to` instance
attached there (a patch build of `to` counts) is still replaying below it. `snapshot_floor_held_for_pin` (an `Info` obs
event, fields `node`, `position` the held floor, `candidate` the floor the
node would otherwise publish) names the hold whenever it is in effect. An
operator who pins an origin and then never swaps the binary — an abandoned
upgrade — holds the journal at that origin for as long as the row stays
pinned but incomplete; there is no bound or alert on how long that can run
(a bound/alert is plan D's, not shipped). Clear it by finishing the upgrade
(attach `to` on every node, then take an instant that agrees) or by pinning
forward (a newer pin supersedes it, and holds at its own origin until it
completes), not by waiting.

**After the flag day**, nothing is required of the operator beyond the wipe
above. A cluster that never runs `uc2ctl upgrade pin` holds an empty pin list
and behaves exactly as `2.12.0` did (modulo the wipe); the four cnc words
stay `0`, `uc2ctl status` prints `upgrade_origin=0 pinned=unversioned
pinned_from=unversioned`, and the new gauges read zero. The pins only start
to matter when you upgrade an FSM's `VERSION` — see [The cluster
FSM](../notes/uc2-cluster-fsm-explained.md) § Pins and reports for what a
pin then does at the service's next attach.

## Upgrading from 2.13.0 to 2.14.0

`2.14.0` is **one flag day** that carries several changes: node↔node wire
`0.9.0` → `0.11.0` (`0.10.0` was never released, so skip straight past it),
cnc `3.3` → `3.4`, and the client↔gateway remote protocol v1 → v2. The two
sections after this one explain the node↔node and cnc changes, and why each
is unsound to mix: [the row running version
(`0.10.0`)](#wire--cnc-change-after-2130-the-row-running-version-0100-cnc-34)
and [the snapshot catalog (`0.11.0`)](#wire-change-after-0100-the-snapshot-catalog-0110),
which also covers the snapshot lifecycle and the backup tier. This section is
the order to do it in. The full list of what changed is [`docs/releases.md` §
The flag day, in one
place](../releases.md#the-flag-day-in-one-place).

Unlike `2.13.0`, **nothing on disk is cleared**. The row artifacts keep the
`ULTSNAP2` envelope `2.13.0` wrote, so `snapshots/<row>/` stays as it is. The
cluster artifact moves from image version 2 to 6 and the replicated `Settings`
record from version 2 to 6. Every older version of both is still read: blobs
an older image does not have read as empty, and fields an older record does
not have read as today's defaults. No `node.toml` key is added as required
or refused. `[backup]` and `[tier_source]` are new and optional.

1. **Rebuild every service against the `2.14.0` SDK, keeping its `VERSION`
   (#67).** `ServiceBuilder::start_with_snapshots()` is gone, and `start()`
   now requires `S: SnapshotStateMachine` at compile time. A service that
   called `start_with_snapshots()` changes the call to `start()`. A service
   that called `start()` without snapshot support no longer compiles. Give its
   state machine one of the two implementations in [State-machine contract §
   Snapshots](../reference/state-machine-contract.md#snapshots-required-the-instant-the-envelope-and-the-exclusive-frontier):
   `WholeStateSnapshot`, where you write only an encode and a decode, or
   `SnapshotStateMachine` directly. Two rules for this window:
   - **Do not change `VERSION`.** Snapshot support is not an `apply` change.
     A row with no pin gets its running version from the leader's own service
     the first time the upgraded cluster sees it attached (step 7). Upgrade
     the application's version as a separate [pinned
     upgrade](upgrade-an-application.md) afterwards.
   - **Do not change the payload format of a row that already snapshots.** A
     row whose `2.13.0` service had its own `SnapshotStateMachine` keeps that
     implementation. Switching it to `WholeStateSnapshot` changes the bytes
     inside the envelope (the helper writes its own `cursor_present ‖ cursor`
     prefix), and the artifacts on disk, including the one step 4 takes, were
     written in the old format.

   Rebuild clients and gateways too. `uc_client` and `uc_remote` gain enum
   variants and public struct fields, which break an exhaustive `match` or a
   struct literal; see [the semver
   policy](../reference/semver-policy.md#snapshots-become-mandatory-67) and
   the read-your-writes note after it. A remote client written in another
   language must speak [remote protocol v2](../reference/remote-protocol.md)
   before the window.
2. **Check that no application's `VERSION` is a bare integer.** From `0.10.0`
   a bare `1`, `2`, `3` … packs as `0.0.x`, so all such builds are on one
   line. Fix that before the window, in the binaries step 1 builds. See [the
   `0.10.0` section](#wire--cnc-change-after-2130-the-row-running-version-0100-cnc-34)
   and [Upgrade an application § The version
   rules](upgrade-an-application.md#the-version-rules). Also finish any
   `uc2ctl upgrade pin` that is in flight: every instance of a pinned row runs
   the pin's `to` before the window, because that is the row's running
   version afterwards.
3. **Stop all traffic.** That includes the remote clients, because a v1
   client cannot talk to a v2 gateway (step 8).
4. **Take one full coordinated instant, still on `2.13.0`, and line the
   artifacts up.** Run `uc2ctl snapshot` (not `--standby`) once, on the
   leader. On **every** node, wait until `uc2ctl snapshot show` prints the same
   `set=<P>`, with `cluster newest=<P>`. The `uc2_snapshot_set_position`
   gauge shows the same thing. The point is that every node restarts from a
   cluster artifact at the same P. Two changes in this flag day make a node
   that restarts from an *older* artifact replay old frames under new rules:
   - Refusal `53` now compares lines, not exact versions. A node replaying
     an old pin frame could accept a pin that `2.13.0` refused ([`0.10.0`,
     Before you stop the
     nodes](#wire--cnc-change-after-2130-the-row-running-version-0100-cnc-34)).
   - The catalog lists every replayed `SNAPSHOT` frame as a commanded
     instant. The other nodes never see those entries, so their catalogs
     differ, and the cluster row reads diverged until the next set completes
     ([`0.11.0`](#wire-change-after-0100-the-snapshot-catalog-0110)).

   A `--standby` instant freezes only the learners, so the voters' cluster
   artifacts would not move, and it does not do the job here.

   If the instant is refused with **`48 snapshot_unsupported`**, the named
   row's `2.13.0` service was started without `start_with_snapshots()`, and
   no instant can complete while that service is attached. You have two
   options:
   - Restart that row's service on `2.13.0` with `start_with_snapshots()`
     and a `SnapshotStateMachine`, then take the instant.
   - Go on without the instant, but only if **no** node holds any
     `snapshots/cluster/snap-*.ultcluster`. Every node then rebuilds its
     cluster FSM from the same place, its journal.

   **Never clear `snapshots/cluster/` on some nodes and not others.** A node
   that installs a `2.13.0` (v2) image and a node that walks the same span
   from genesis can disagree about a pinned origin ([`0.11.0`, A pinned
   row](#wire-change-after-0100-the-snapshot-catalog-0110)).
5. **Stop every node before starting any node.** Use
   [`scripts/uc2_flag_day.sh`](#run-it) or do it by hand. On each host, stop
   clients → gateway → services → node. Nothing refuses a mixed cluster by
   name. A `0.11.0` leader drops a `0.9.0` follower's 16-byte `STATUS`, so
   replication to that follower stalls. A `0.9.0` node applies the new
   `CLUSTER` kinds (6 `RowGenesis`, 7 `BackupWatermark`) as undecodable, and
   its cluster FSM diverges in silence.
6. **Swap every binary on every host: node, `uc2ctl`, gateway and the
   services from step 1.** `snapshots/<row>/` and `snapshots/cluster/` stay
   as they are. A `3.3` service or client refuses a `3.4` page by version. A
   `3.4` client attaches to a `3.3` page but refuses a read-your-writes read
   on it (`ReadYourWritesUnsupported`). So each host's clients, services and
   gateway restart with its node, as at every page bump.
7. **Start every node, wait for one leader, then start every declared row's
   service on every node.** Client writes wait until every declared row has a
   running version. A row with a `2.13.0` pin carries the pin's `to` across.
   For a row with no pin, the leader records the version of **its own**
   attached service for that row (`row_genesis` in the audit log). Until then
   the leader drains nothing from the ingress ring and logs
   `version_gate_waiting` every 5 s, naming the row. A follower whose service
   is on another line is refused at attach with `VersionMismatch`.
8. **Start the gateways, then the remote clients, all on `2.14.0`.** Remote
   protocol v2 adds the read-your-writes query flag `FLAG_MIN_POSITION`
   (`0x20`). A v1 edge would read its 8-byte token as query bytes, so v1 and
   v2 refuse each other at `HELLO` with `HELLO_REFUSED_VERSION`, which is
   terminal: the client does not try the other members. Any v1 client still
   running after the window is refused on every connect until it is rebuilt.
   A remote client that saves its `client_id` across restarts can now resume
   its session with `RemoteConfig::first_seq` (#34). See [the read-your-writes
   explainer](../notes/uc2-read-your-writes-explained.md#version-requirements).
9. **Commit one `uc2ctl settings apply`, on the leader.** That way every node
   takes its replicated settings from the log, as a v6 record, rather than
   from whatever image it started on. `[settings]` in `node.toml` seeds
   genesis only, and an upgraded cluster's genesis is long past. The file
   replaces the whole record, and a key you leave out takes its **default**,
   not the value the cluster runs now. So write out every value you run today
   (`uc2ctl settings show` prints them). The new keys (`retain_sets`,
   `auto_fetch`, `backup_retain_sets`, `backup_retain_age`,
   `purge_requires_tiers`) may be left out. Each one then behaves as
   `2.13.0` did, apart from `auto_fetch`, which is on by default.
10. **Take one more instant, then check.** Run `uc2ctl snapshot` again. The
    catalog starts **empty** (no `2.13.0` image has one), and until a set
    completes every reader uses the pre-catalog behaviour. Then, on every
    node:
    - `uc2ctl status`. The first line reads `node: running=true`, exactly
      one node is `leader=true can_serve=true`, and every row's line ends
      `running=<ver> running_pos=<pos>`, never `running=none`, with
      `version=` on the same major.minor.
    - `uc2ctl catalog`. The new set is listed **agreed**, and the
      `catalog_empty` attention code is gone. The gauges agree:
      `uc2_catalog_empty` is `0` and `uc2_catalog_agreed_position` is the
      same on every node.
    - `uc2_status_refused_total` stays at `0`. A count there is a peer
      sending the old 16-byte `STATUS`, which means a node still on an old
      binary.
    - `uc2ctl settings show` / `schedule show` / `upgrade show` now read the
      live [cluster view](../reference/cluster-view.md), so they answer
      before the first artifact. `uc2ctl status`'s `schedule_position=` and
      ceiling rung still come from the newest cluster artifact.

    That instant also completes a pin carried over from `2.13.0`: the pin
    is complete once an agreed set built on its `to` line exists above the pin
    record. Until then every node holds its journal at the pin's origin.

**What else is new on the node.** Every node now serves the cluster view as
`GET /cluster` on an always-on `<instance_dir>/obs.sock` (mode `0600`). When
`[metrics]` is configured, it also serves it on the metrics listener, with no
authentication, so check who can reach that port. Each node also runs a
`uc2-holdings` thread. A node with `[backup]` runs a `uc2-backup` writer, and
one with `[tier_source]` runs a `uc2-tier-fetch` thread. None of these needs
configuring for the upgrade. See [Back up to a tier](back-up-to-a-tier.md)
for when you want them.

## Wire + cnc change after 2.13.0: the row running version (`0.10.0`, cnc `3.4`)

Issue [#33](https://github.com/PeterKnego/ultima_cluster/issues/33), shipped
in `2.14.0` inside the one flag day [above](#upgrading-from-2130-to-2140).
Every declared row gets a committed **running
version**, and the platform enforces that a row is never applied by two
versions of its state machine at once. The reasoning is [The row running
version, explained](../notes/uc2-row-running-version-explained.md); the rules
an application is built and upgraded by are [Upgrade an application § The
version rules](upgrade-an-application.md#the-version-rules).

**Wire 0.9.0 → 0.10.0: one new `CLUSTER` kind, and like `0.9.0`'s it is NOT
benign.** `kind 6 = RowGenesis` (8 B: `row ‖ reserved ‖ version`) is a frame
on the replicated log. A `0.9.0` node applies it as undecodable and moves on,
so its cluster FSM never learns any row's running version and diverges in
silence. **Stop every node before starting any node.**

**cnc 3.3 → 3.4: three new node-written words.** `running_version` (slot
`+48`; bit 32 = present, low 32 bits = the packed version) and
`running_record_pos` (slot `+56`) on the service status line, under the same
`pin_seq` seqlock as the pin words; and `cluster_applied` at page-1 offset
`4056`. The other `2.14.0` changes fold into the same `3.4` rather than
bumping it again: `write_reserve` (page `264`, #78), the snapshot lifecycle's
start-set pair (slot `+264` position, `+272` version), and the
read-your-writes query flag `FLAG_V2_MIN_POSITION`. See [the cnc control
page](../reference/cnc-page.md#service-slots). As with every page bump, each
host's clients, services and gateway restart with its node.

**No wipe.** The cluster artifact gains a per-row running-version blob
(image version 3; `2.14.0` writes version 6, see
[`0.11.0`](#wire-change-after-0100-the-snapshot-catalog-0110)), and the
version 1 and 2 images `2.11.0`–`2.13.0` wrote are still read: a row
with pin history takes its newest pin's `to` as its running version, and a
row without pins gets one the first time the leader sees its own service for
that row attached. The row artifacts' envelope (`ULTSNAP2`) is unchanged, so
neither `snapshots/<row>/` nor `snapshots/cluster/` needs clearing.

**Before you stop the nodes.**

- **Check that no application's `VERSION` is a bare integer.** A
  `const VERSION` of `1`, `2`, `3` … packs as `0.0.x`, and from `0.10.0` "the
  same version" means the same major.minor with patch ignored — so every
  bare-integer build is on one line, the `0.0` line. Its rows treat all such
  builds as the same version: they mix freely, and a pin from one to another
  refuses nothing (`uc2ctl upgrade pin` refuses a same-line pin unless you pass
  `--patch`, and a service logs `version_is_patch_only` at attach). If one is,
  give that state machine a real major.minor with `pack_version(major, minor,
  patch)` in the same window — see [Upgrade an application § The version
  rules](upgrade-an-application.md#the-version-rules).
- **Take a coordinated snapshot instant right before you stop them.** Run
  `uc2ctl snapshot` once traffic has stopped and wait until
  `uc2_snapshot_set_position` reaches it on every node, so every node's newest
  cluster artifact sits at the same position P. The pin rule changed in this
  release — refusal `53` used to require `--from` to equal the row's current
  pin's `to` exactly, and now requires only the same line — so a node that
  boots from an older cluster artifact and replays an old pin frame under the
  new rule could accept a pin that `2.13.0` refused. With every node starting
  from P, there is nothing older left to replay.

**What to expect when the cluster comes back up.**

- **Client writes wait until every declared row has a running version.** The
  leader records a row's first version (a `RowGenesis` record, audited as
  `row_genesis` with `actor="node"`, `source="genesis"`) from **its own**
  attached service for that row, and until every declared row has one it
  drains nothing from the ingress ring: clients see back-pressure, not an
  error. The node logs `version_gate_waiting` every 5 s naming the lowest row
  still waiting and `leader_service_attached`. A declared row whose service
  never attaches **on the leader** keeps the cluster closed to writes, so
  start every declared row's service on every node, as the flag-day order
  already says. A row that already had a pin before the upgrade carries its
  version across and needs no genesis.
- **Start the same line everywhere.** Genesis records whatever line the
  leader's service is on. A follower whose service is on another line
  (major.minor) is refused at attach with `VersionMismatch`, or — if it
  attached before the record committed — stops at the record with
  `version_superseded`. Both name the row and both versions.
- **An unversioned FSM is version `0`, and `0` is a real version.** A row
  whose leader ran a service without `const VERSION` records `0`, and a
  later binary with a real `VERSION` is refused until you move the row with
  `uc2ctl upgrade pin --from 0.0.0`.
- **`uc2ctl status`** prints `running=` and `running_pos=` last on each row's
  line; `/metrics` adds `uc2_row_running_version{service,row}` and the
  `Uc2RowVersionMismatch` alert, and `Uc2ServiceVersionDrift` now compares
  lines, not exact versions ([Monitor a
  cluster](monitor-a-cluster.md#the-per-fsm-families-m14)).
- **Harnesses that start a node and submit without a service now wait
  forever.** Anything that declares a row must attach a service for it on the
  leader before driving load — in-tree, the M10 alert-fire harness was
  re-shaped this way.

## Wire change after 0.10.0: the snapshot catalog (0.11.0)

The snapshot catalog, shipped in `2.14.0` inside the one flag day
[above](#upgrading-from-2130-to-2140). It is project 1 of a
four-project storage-service direction; the reasoning is
[the design spec](../superpowers/specs/2026-10-01-uc2-snapshot-catalog-design.md).
The cluster now keeps a replicated list of the snapshot **sets** it has
coordinated — position, kind, state, and per row the building version, the
hash and the agreement verdict — instead of forgetting a set the moment a
newer one lands, and the purge floor follows the newest set the cluster
**agrees** on rather than the newest set that merely exists on disk.

**Wire `0.10.0` → `0.11.0`.** `STATUS`'s body grows from 16 B to 144 B
(`uc_protocol::v2::datagram::STATUS_BODY_LEN`): every node now advertises
what it holds — its journal span, which catalogued sets it has complete on
disk, applied positions per row, and free/journal/snapshot byte counts
(`Holdings`). There is no wire-version word on node↔node datagrams, so
nothing refuses a `0.10.0` peer by name: a `0.11.0` leader **drops** a
`0.10.0` follower's 16-byte `STATUS` (counted as
`uc2_status_refused_total` and named on stderr as `status_refused`), so
that follower's flow-control window never opens and replication to it
stalls; a `0.10.0` leader accepts a `0.11.0` follower's 144-byte body and
ignores the tail. Mixing is unsound in both directions, and this procedure
forbids it. `SNAP_REPORT`/`SnapshotReport` (`CLUSTER` kind 5)
admits row `255`: the cluster artifact's own hash now joins the per-row
completeness report, closing the "the cluster FSM's own artifact has no
determinism check" gap. The replicated `Settings` record gains
`retain_sets: u16` (record version 3). A version-1 (`2.11.0`) or version-2
(`2.12.0`/`2.13.0`) record is still accepted on read and reads `retain_sets`
as `1`, the newest-only retention. No existing
layout changes anywhere, which is exactly why mixing is unsound rather than
merely unsupported: a `0.10.0` peer applies the row-255 report and the wider
`Settings` record as undecodable and silently diverges — its catalog never
completes a set. **Stop every node before starting any node.**

The same flag day also carries the **snapshot lifecycle**: the `SNAP_REPORT`
datagram's body grows from 24 B to 32 B (the artifact's size), the
`SnapshotReport` record's entries become `(node, hash, size)`, the
replicated `Settings` record gains `auto_fetch` (v4, default on), and catalog
row entries carry sizes (image v5). A cluster image from before v5 still
loads — its sets read with size 0 ("unknown"), which nothing refuses — and a
v1–v3 settings record reads `auto_fetch = true`. A dev cluster built from
`main` between the catalog merge and this change must also stop every node: a
24-byte report is refused by length.

It also carries the **backup tier** ([Back up to a
tier](back-up-to-a-tier.md)): `CLUSTER` kind 7 `BackupWatermark`, the
pairwise datagram kinds 27 `BACKUP_REPORT`, 28 `TIER_INSTALL` (200 B) and 29
`TIER_INSTALL_STATUS` (24 B), the `Settings` fields `backup_retain_sets` /
`backup_retain_age` (v5) and `purge_requires_tiers` (v6, 47 B), and a backup
watermark blob in the cluster image (v6). The `Settings` record `2.14.0`
writes is therefore version 6, and the cluster image version 6. Every older
record and image is still read: an absent backup field means keep every
backup and no purge gate, and an absent watermark blob reads as no tiers.
A `0.10.0` peer applies kind 7 as undecodable and drops the three pairwise
kinds, so this changes nothing about the rule: stop every node before
starting any. Nothing on disk is cleared.

**A pinned row in a pre-#33 (v1/v2) cluster image.** Such an image has no
running-version blob, so installing it restores a pin's record position as
the image's `applied` position (at or above the true one). Pin completion
(an agreed set above the pin record) is still judged identically on every
node: sets formed after the flag day lie above that position on every node.
The only residual is a cluster where one node installs a v1/v2 image while
another walks the same span from genesis — the two could retire the pinned
origin differently. Clear `snapshots/cluster/` on every node or on none.

**Before stopping: take one full instant and line the artifacts up.** Run
`uc2ctl snapshot` (not `--standby`) right before the window, wait for it to
complete, and confirm every node's newest `snapshots/cluster/snap-*.ultcluster`
is at the **same** position P. A node whose newest cluster artifact is older
than another's replays the `SNAPSHOT` frames between the two on restart and
lists them as commanded instants the others never see; those entries are
dropped the moment the next set completes, but until then the nodes' catalogs
— and so their cluster artifacts — differ, and the cluster row reads diverged.

**After the flag day: commit one `uc2ctl settings apply`.** Every node must
take its replicated settings — `retain_sets` included — from the log, not from
whatever image or `node.toml` it started on. Apply the cluster's intended
settings file once, on the leader, as soon as the cluster serves (re-applying
the values it already runs is fine). Do not rely on `[settings] retain_sets`
in `node.toml` for an upgraded cluster: `[settings]` seeds **genesis** only,
and an upgraded cluster's genesis is long past.

**Sets this node did not build carry a marker from now on — older ones do
not.** A set that arrives through a snapshot session (`uc2ctl snapshot
fetch`, or a below-floor install) is marked
`<instance_dir>/snapshots/cluster/snap-<pos>.foreign`, and the node never
reports a marked set's hashes as its own. Instance directories from before
this change carry no markers, so a set this node fetched or was installed
with before the upgrade is treated as its own; to exclude one, create that
empty file for its position by hand while the node is stopped.

**cnc `3.4`, no further bump.** The catalog is not on the shmem page; it
rides inside the cluster artifact, `/metrics` and the cluster view. The
lifecycle's start-set words (slot `+264`/`+272`) fold into the `3.4` that
[`0.10.0`](#wire--cnc-change-after-2130-the-row-running-version-0100-cnc-34)
opened, with no node-local process restart beyond the ordinary flag-day
one.

**Nothing on disk is cleared.** The catalog is a trailing length-prefixed
blob in the cluster image (version 4; `2.14.0` writes version 6). A v1–v3
artifact, which includes every one `2.13.0` wrote, is still **read**, with the
newer blobs empty — the same "accept every older version with its missing
trailing blobs read as empty" rule every prior image bump used. `snapshots/<row>/` and `snapshots/cluster/` are both
untouched; there is no wipe for this flag day.

**The `Empty` window.** A v1–v3 artifact (every `2.13.0` one) — or a fresh
genesis — starts with an empty catalog: no set has ever completed. Every reader takes a named
fallback in that state: the purge floor and every install source read as
today's pre-catalog behaviour (the node's own newest complete set on disk),
and the pruner keeps deleting on that same old rule rather than refusing to
delete anything. The gauge `uc2_catalog_empty` is `1` for as long as this
holds and `0` once the first instant completes — watch it after
the upgrade to know when the cluster has actually started keeping the new
replicated record, as opposed to merely running the new binary.

**`retain_sets` defaults to `1`.** Absent from both `node.toml`'s
`[settings]` (genesis) and a `uc2ctl settings apply` TOML file, it means
`1` — today's newest-only retention, unchanged from before this flag day.
Set it with [`uc2ctl settings apply`](../reference/uc2ctl.md#settings-apply)
to keep more than one agreed set; `0` and anything above `48` are refused at
the door with `47 settings_bounds`. See
[Configuration § `[settings]`](../reference/configuration.md#settings) and
[How to keep the journal from growing without bound § Choose a slack and
turn purging on](bound-journal-growth.md#choose-a-slack-and-turn-purging-on).

## Wire + cnc change in 2.15.0: the wire level (`0.12.0`, cnc `3.5`)

The committed cluster wire level, shipped in `2.15.0`
([#31](https://github.com/PeterKnego/ultima_cluster/issues/31)). It is what
makes [rolling upgrades](#rolling-upgrades-since-2150) possible from here on,
and getting onto it is **one more stop-all flag day** — the last one for an
additive wire change. The reasoning is
[the explainer](../notes/uc2-wire-level-explained.md); the design is
[the spec](../superpowers/specs/2026-10-19-uc2-wire-level-design.md) (read its
errata block first).

**Wire `0.11.0` → `0.12.0`.** Three changes, none of them a length change on
the replication path:

- **Datagram header byte 13 is `wire`.** It was a `flags` byte, always `0`
  and read by nobody; every datagram now carries its sender's `WIRE_MAX`
  there (`12` for every `0.12.0` binary), inside the 16 bytes crypto
  authenticates as AAD. A receiver checks it before reading anything else and
  accepts `[WIRE_MIN, WIRE_MAX + 1]`, which for `0.12.0` is `12..=13`.
- **`Settings` version 7** (`CLUSTER` kind 3, 49 B) appends `wire_level u8
  @47` and `wire_raise u8 @48` (`0` keep, `1` auto, `2` manual). An encoder
  writes version 7; versions 1–6 still decode, and decode as level 12 with
  auto raise, so a `2.14.0` cluster artifact and a fresh genesis read the
  same value. The cluster image version is unchanged (6): its settings blob
  is sized by the record's own version.
- **`NEW_TERM` gains an 8-byte body**, `wire_level u32 ‖ reserved u32`,
  written as zeros and read by nobody — reserved for a future per-term frame
  layout. The frame is 64 B instead of 32 B.

**The first flag day a mixed cluster is REFUSED rather than misparsed.** A
`0.11.0` node writes `0` in byte 13, so a `0.12.0` node refuses every one of
its datagrams by name and counts it:
`uc2_wire_datagrams_refused_total{reason="prelevel"}`, plus a `wire_refused`
line on stderr, throttled per source. A `0.11.0` node therefore cannot
replicate with, report to or vote for a `0.12.0` one; nothing diverges in
silence. It is still a flag day, so **stop every node before starting any
node**. On a crypto cluster the counter is bumped before authentication, so
unauthenticated traffic can drive `prelevel` too; the stderr line is written
only for a configured member.

**cnc `3.4` → `3.5`, additive.** Three words: `wire_level` at page-1 offset
`3992` (the `uc2-cluster` agent, at every `Settings` apply and install),
`wire_binary` at `4000` (the node, once at boot, `(WIRE_MIN << 8) |
WIRE_MAX`) and `wire_max` at peer slot `+8` (the consensus agent, on change;
leader-authoritative, cleared when a new leader takes office). A `3.4`
attacher refuses a `3.5` page, so a host's services, clients and gateway
restart with its node — which the flag day does anyway.

**Nothing on disk is cleared.** No `snapshots/` wipe and no artifact envelope
change: every older `Settings` record and cluster image is read as before.

**Precondition: a journal that reaches back before `2.11.0` needs a cluster
artifact first.** Before the cluster FSM, frame type 4 was `CONFIG`, and a
`2.15.0` node walking such a journal from position 0 can read one of those
frames as a `CLUSTER` record no level allows and fail-stop with
`ClusterRecordUndecodable`. If any node's journal still starts before
`2.11.0` (purge off, never snapshotted since), run `uc2ctl snapshot` on the
cluster BEFORE the flag day, so every node boots from a cluster artifact
instead of walking from 0.

**After the flag day** every node runs at level 12 with `wire_raise = auto`,
and `uc2ctl status` prints `wire: level=12 binary=12..12`. There is nothing
to raise until a release ships level 13. If you want your first roll to keep
a rollback window, add `wire_raise = "manual"` to the cluster's full
settings file and `uc2ctl settings apply` it any time before that roll
starts (the apply replaces the whole record, so never a one-line file). `[settings]` in `node.toml` has no
`wire_raise` key; genesis is always auto.

**Three new fail-stops, all in the `uc2-cluster` agent** (the daemon exits 1
with the name on stderr): `WireLevelAboveBinary` (the committed level is
above this binary's max — upgrade the node), `WireLevelBelowBinary` (below
its min, judged only once the node has joined — install the release the
message names first) and `ClusterRecordUndecodable` (a committed `CLUSTER`
record no correct peer could have written at the committed level — an
unknown kind, or a `Settings` version above the level's; the journal is
corrupt or a peer broke the level. A journal that reaches back before
`2.11.0` needs a cluster artifact first — see the precondition above —
otherwise restore from backup). A known kind with a malformed payload is
still the cluster FSM's ordinary refusal 42, as before `2.15.0`. With every node on the same
`2.15.0` binary none of them can fire on a healthy cluster.

## Where to go next

- [Configuration: Admin authentication](../reference/configuration.md#admin-authentication)
  — the full `[admin]` key table and the `[crypto]` pairing this section's
  config choices depend on.
- [Run a cluster on real hosts](run-a-cluster.md#what-a-planned-restart-costs)
  — what an ordinary single-node planned restart costs, for context against a
  whole-cluster flag day.
- [Encrypt traffic between nodes](encrypt-node-traffic.md) — another
  flag-day operation in this system, same "every node together" shape.
- [`uc2ctl`](../reference/uc2ctl.md) — the `status` fields this script parses
  to drive its verification steps.
- [The committed cluster wire level, explained](../notes/uc2-wire-level-explained.md)
  — why a release within one wire level rolls, and what the raise waits for;
  [`uc2ctl wire show` / `wire raise`](../reference/uc2ctl.md#wire-show) are
  the two verbs a roll uses.
