# How to upgrade a cluster

There are two procedures. The normal one, since `2.15.0`, is a **rolling
upgrade, one node at a time under load**, for any release whose wire minor is
the cluster's committed level or one above it:
[Rolling upgrades since 2.15.0](#rolling-upgrades-since-2150).

The other is a scripted, whole-cluster **stop-all** upgrade with a measured
downtime number: stop every node, verify they agree on what they hold, swap
the binary, start everyone back up, and confirm one leader is serving again
before declaring success:
[Stop-all upgrades: the flag day](#stop-all-upgrades-the-flag-day). It is the
procedure for a wire major, for any release whose notes call it a flag day,
and for getting onto `2.15.0` itself.

## Which procedure

Read the committed level with `uc2ctl wire show` (`level=`), and the new
release's wire minor from its release notes.

| the new release's wire minor is… | procedure |
|---|---|
| the committed level, or one above it | [rolling](#rolling-upgrades-since-2150) |
| two or more above it | install the release in between first — true for a stop-all upgrade too, because a binary refuses to run outside its own range |
| a new wire **major**, or the notes call the release a flag day | [stop-all](#stop-all-upgrades-the-flag-day) |
| `0.12.0`, and the cluster runs `2.14.0` | [stop-all, the `2.15.0` window](#getting-onto-2150-the-last-stop-all-upgrade-for-an-additive-wire-change) |

A cluster on a release **before `2.14.0`** steps release by release, each
step a stop-all upgrade with its own edits and checks:
[Upgrade from a release before 2.15.0](upgrade-from-an-older-release.md).

Why a wire minor rolls and a major does not: every node encodes at the level
the cluster has committed, every datagram carries its sender's maximum level
in header byte 13, and the leader raises the level only once every member can
follow — so a node one level ahead runs beside the old ones and sends nothing
they cannot parse. The argument is
[The committed cluster wire level, explained](../notes/uc2-wire-level-explained.md).

## The per-host rule: restart a host's processes together

Whichever procedure you run, a host is upgraded as a unit. The node, its
services, every shmem client and the gateway share the instance directory's
control page and rings, and a release that bumps the cnc minor (its notes say
so) makes an older attacher refuse the newer page by name. So on each host:

1. stop the clients, the gateway, then the services, then the node;
2. swap binaries;
3. start the node, wait for `can_serve` (`uc2ctl status`, or `/readyz`),
   then start the services, then the clients and the gateway.

Start the node before its services: a service's attach waits in
`NodeBooting` until the node has joined its cluster — leader known, a commit
position learned, the cluster FSM caught up to it — for `boot_wait` (default
10 s), then fails. Service units ordered `After=` their node unit satisfy
this only because that wait covers a normal election; nothing in systemd
knows about the cluster. If you bring nodes up inside one process (a
harness, an embedded deployment), start every node before attaching any
service.

The rule is per host, so it fits inside a roll. The rings are volatile and
recreated at boot; the journal, `state/` and `snapshots/` are reused as they
are unless a release's notes say otherwise.

## Rolling upgrades since 2.15.0

0. Take an off-node backup of every node before the first restart
   ([Back up to a tier](back-up-to-a-tier.md), or
   [`uc2ctl backup`](back-up-a-cluster.md)): after the raise it is the only
   way back.
1. Check the pair is permitted: the new release's wire minor is the cluster's
   level or one above it (`uc2ctl wire show` → `level=`; the release notes
   name the minor). Two above: install the middle release first.
2. Decide whether you want a rollback window. If yes, before the first
   restart: add `wire_raise = "manual"` to the cluster's **full** settings
   file — the one last applied, or the record `uc2ctl settings show` prints
   — and `uc2ctl settings apply` it. Never apply a one-line file: `settings
   apply` replaces the whole record.
3. One host at a time, [as a unit](#the-per-host-rule-restart-a-hosts-processes-together):
   stop the node and every process attached to its instance directory;
   install; start the node; wait for `can_serve`; start the services; move
   on. Restart followers and learners first and the leader last, so the roll
   costs one election. Under load, commit stalls only while a quorum
   re-forms.
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
- **Step 3, the host.** The per-host rule above. A release that bumps the
  cnc minor makes an older attacher refuse the newer page, which is why the
  host's services, clients and gateway restart with its node; a release that
  does not still restarts them, because the node they attach to goes away.
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
- **Membership during a roll.** There is no special case: a member added
  mid-roll blocks the raise until it reports a level above the committed one,
  and a member removed stops counting
  ([Change cluster membership](change-cluster-membership.md#upgrading-a-mixed-version-cluster)).
  A lone node two levels ahead of the cluster never joins: it waits in
  `NodeBooting`, printing throttled `wire_refused` lines that name its range —
  install the middle release on it instead.

Rolling back **before** the raise is the same procedure in reverse, one host
at a time. **After** the raise there is no rollback onto the old binary — it
would stop with `WireLevelAboveBinary` — short of restoring a pre-upgrade
backup on every node.

## Stop-all upgrades: the flag day

`scripts/uc2_flag_day.sh` stops every node, runs your upgrade command on
every host, starts every node and prints the downtime. A rolling upgrade
needs none of this section.

### Prerequisite: traffic is actually stopped

The script cannot see who is submitting to the cluster — it can only read
the cluster's own counters. It therefore **requires `--yes-traffic-stopped`**
and refuses immediately without it, touching nothing. Stop your clients (or
point them at a maintenance page) before you run this, and pass the flag
only once they actually are stopped — the flag is an assertion the script
trusts, not something it verifies. That includes remote clients: a release
that bumps the remote protocol refuses the old client at `HELLO`, so
gateways and remote clients upgrade in the same window.

### Run it

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
downtime measurement. The script swaps the node; the services, `uc2ctl`, the
gateway and the clients on each host are yours to swap in the same window,
[per host](#the-per-host-rule-restart-a-hosts-processes-together).

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

### What it does, step by step

Every step prints a timestamp.

0. **Preflight.** Every host reachable, every node's `uc2ctl status` parses,
   and every node's config version agrees. Refuses, touching nothing, if any
   of that is false.
1. **Traffic-stopped confirmation** — the prerequisite above.
2. **Stop every node in parallel** (`systemctl stop`, or `SIGTERM` in
   `--local` mode). This drains the archive first, so the restart replays
   rather than reconstructs.
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

Steps 5 and 6 come before any service starts for the reason the per-host
rule gives: a node does not open its attach door until it has joined its
cluster, so a service started against a node whose peers are not up yet
spends its `boot_wait` waiting for a leader that cannot be elected yet.
**Start all the nodes, wait for a leader, then start the services** — which
is what step 6 buys you.

### The abort path is load-bearing

From step 2 (stopping nodes) through step 4 (`--upgrade-cmd`), **any
unexpected failure restarts every node on whatever binary is currently in
place** rather than leaving the cluster down — this is the un-upgrade path,
and it is not optional or best-effort. If `--upgrade-cmd` succeeded on some
hosts and failed on others before the abort fires, the restart can bring the
cluster up on a **mixed** version. Within one wire level that is the mixed
cluster a roll runs anyway; across a flag-day boundary the old side is
refused by name (`uc2_wire_datagrams_refused_total`, since `2.15.0`) or
stalls (before it) — never anything unsound, but not automatically healthy
either. The script says so explicitly in its abort log; run `uc2ctl status`
on every node before trusting the cluster again, and re-upgrade only once
every node agrees.

**Step 6 (waiting for convergence) is not covered by this path.** By step 6
every node has already been stopped, upgraded, and told to start — there is
nothing left to "un-upgrade" in the way steps 2-4 mean it, so a step 6
timeout does not trigger a restart of its own; it just gives up waiting and
exits `1`, leaving nodes exactly as step 5 left them. See the exit-code table
below.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | upgraded; one leader serving with matching config version; `DOWNTIME: <secs>s` printed |
| `1` | three different situations, all exit `1`, **not all "nothing was touched"**: **(a)** refused before touching anything (preflight, missing `--yes-traffic-stopped`, bad arguments) — no node was stopped, upgraded, or restarted; **(b)** `abort_restart` fired on a failure at steps 2-4 (a stop that didn't confirm, disagreeing durable positions, or a failed `--upgrade-cmd`) — the un-upgrade path restarts every node and confirms every one back up before this exit; **or (c)** a **step 6 convergence timeout** — by step 6 every node has already been stopped, had `--upgrade-cmd` run, and been told to start, so this is not "nothing touched" either, but it is also NOT the un-upgrade path: a timeout does not call `abort_restart` or attempt a restart of its own, it just gives up waiting. Nodes are left exactly as step 5 left them (typically already on the NEW binary, possibly including one that never confirmed starting) — run `uc2ctl status` on every node before assuming anything |
| `3` | aborted after stopping nodes, and the abort path's restart (plus one retry) still left at least one node down — manual operator action required: start it by hand, then check `uc2ctl status` everywhere before touching the cluster again |

`3` is deliberately distinct from `1` so a monitoring wrapper can tell
"un-upgrade succeeded, cluster is fine" from "un-upgrade itself is in
trouble" apart without parsing the log.

### Reading the downtime number

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

## Getting onto 2.15.0: the last stop-all upgrade for an additive wire change

`2.15.0` ships the committed cluster wire level
([#31](https://github.com/PeterKnego/ultima_cluster/issues/31)), wire
`0.11.0` → `0.12.0` and cnc `3.4` → `3.5`. It is what makes the rolling
procedure possible from here on, and a `2.14.0` node has no level to speak,
so getting onto it is **one more stop-all window** — the last one for an
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
No `node.toml` key is added or refused.

**Precondition: a journal that reaches back before `2.11.0` needs a cluster
artifact first.** Before the cluster FSM, frame type 4 was `CONFIG`, and a
`2.15.0` node walking such a journal from position 0 can read one of those
frames as a `CLUSTER` record no level allows and fail-stop with
`ClusterRecordUndecodable`. If any node's journal still starts before
`2.11.0` (purge off, never snapshotted since), run `uc2ctl snapshot` on the
cluster BEFORE the flag day, so every node boots from a cluster artifact
instead of walking from 0.

**The window**, in order: stop traffic; run
[`scripts/uc2_flag_day.sh`](#run-it) or stop every node by hand; on each
host swap the node, `uc2ctl`, the services, the gateway and the clients
[together](#the-per-host-rule-restart-a-hosts-processes-together); start
every node, wait for a leader, then the services, then the gateways and
clients.

**After the flag day** every node runs at level 12 with `wire_raise = auto`,
and `uc2ctl status` prints `wire: level=12 binary=12..12`. There is nothing
to raise until a release ships level 13. If you want your first roll to keep
a rollback window, add `wire_raise = "manual"` to the cluster's full
settings file and `uc2ctl settings apply` it any time before that roll
starts (the apply replaces the whole record, so never a one-line file).
`[settings]` in `node.toml` has no `wire_raise` key; genesis is always auto.

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
still the cluster FSM's ordinary refusal 42, as before `2.15.0`. With every
node on the same `2.15.0` binary none of them can fire on a healthy cluster.

## Where to go next

- [Upgrade from a release before 2.15.0](upgrade-from-an-older-release.md)
  — the per-release edits, wipes and checks for every stop-all step from
  `2.6.0` to `2.14.0`.
- [Upgrade an application](upgrade-an-application.md) — upgrading a state
  machine's `VERSION` is a separate, pinned procedure, not part of a cluster
  upgrade.
- [The committed cluster wire level, explained](../notes/uc2-wire-level-explained.md)
  — why a release within one wire level rolls, and what the raise waits for;
  [`uc2ctl wire show` / `wire raise`](../reference/uc2ctl.md#wire-show) are
  the two verbs a roll uses.
- [The semver policy](../reference/semver-policy.md) — which version numbers
  are API and which are a wire or cnc flag day.
- [Run a cluster on real hosts](run-a-cluster.md#what-a-planned-restart-costs)
  — what one node's planned restart costs: each step of a roll.
- [Encrypt traffic between nodes](encrypt-node-traffic.md) — a flag-day
  operation, unlike a rolling upgrade: every node together.
- [`uc2ctl`](../reference/uc2ctl.md) — the `status` fields the script parses
  to drive its verification steps.
