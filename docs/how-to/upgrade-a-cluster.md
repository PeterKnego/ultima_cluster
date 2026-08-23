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
