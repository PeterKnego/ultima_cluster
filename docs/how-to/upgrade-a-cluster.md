# How to upgrade a cluster

Run a scripted, whole-cluster upgrade with a measured downtime number:
stop every node, verify they agree on what they hold, swap the binary, start
everyone back up, and confirm one leader is serving again before declaring
success.

## Why this is a flag day, not a rolling restart

**Every node stops, upgrades, and restarts together. There is no rolling or
partial mode.** This is not a conservative default — it follows directly from
wire protocol 0.5.0's content-attested durable reports (see
["Shipped in v2.3.0 — wire protocol 0.5.0"](../releases.md) in the release
notes): a follower on an older wire version reports its durable position
without the term it attributes to those bytes, the leader cannot count that
report as attested, and a mixed-version cluster **stalls commits** rather
than making an unsound one. Safe, but it means a cluster part-upgraded is a
cluster not making progress, not a cluster running a mildly-degraded old
version — so finish the upgrade, or fully undo it, and never leave it
half-applied on purpose.

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
[binary upgrade past 0.5.0](#why-this-is-a-flag-day-not-a-rolling-restart)
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

## Wire + cnc change in 2.11 (pending): FSM identity **and** log time (`0.7.0`, cnc `3.1`)

Two features share this flag day, because both were implemented before the
release was cut:

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

It is **one combined flag day**, on both lines at once — the same-host cnc
page (`3.0` → `3.1`) and the node-to-node wire (`0.6.0` → `0.7.0`) — because
both changes ship in the same release.

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
it gets a named startup refusal, not a mid-run failure.

**Wire 0.7.0, part one (FSM identity)**: `SNAP_BEGIN`'s `services_declared`
bitmask becomes a per-row identity-hash array (`identity: [u64; 8]`), and a
per-row packed version array (`version: [u32; 8]`) is added — see [the wire
protocol
reference](../reference/wire-protocol.md#snap_begin-body-wire-070-fsm-identity)
for the exact layout. A 0.6.0 sender's shorter body is dropped by the same
length check that drops a 0.5.0 body today: **a mixed cluster stalls a
joiner rather than installing a wrong or half-checked artifact.**

**Wire 0.7.0, part two (log time and timers)**: the 32-byte **log frame
header is relaid**. `session_id: u64` and `correlation_id: u64`, of which the
client only ever filled 32 bits each, become `client_id: u32` + `seq: u32`,
freeing 8 bytes for `time_ns: u64` — the leader's stamp on the frame. A new
frame type, `TIMER = 5`, carries a 24-byte body (`identity_hash ‖ timer_id ‖
deadline_ns`). See [the wire protocol reference](../reference/wire-protocol.md#log-frames).

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

## Where to go next

- [Configuration: Admin authentication](../reference/configuration.md#admin-authentication)
  — the full `[admin]` key table and the `[crypto]` pairing this section's
  config choices depend on.
- [Run a cluster on real hosts](run-a-cluster.md#what-a-planned-restart-costs)
  — what an ordinary single-node planned restart costs, for context against a
  whole-cluster flag day.
- [Encrypt traffic between nodes](encrypt-node-traffic.md) — the other
  flag-day operation in this system, same "every node together" shape.
- [`uc2ctl`](../reference/uc2ctl.md) — the `status` fields this script parses
  to drive its verification steps.
