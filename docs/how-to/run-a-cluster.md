# How to run a cluster on real hosts

Covers the move from a single-box cluster to nodes on separate machines, where
addresses and process supervision start to matter.

If you have not run one at all yet, work through
[the quickstart](../QUICKSTART.md) first — it gets three nodes up on one box
and is a better place to meet the moving parts.

## Give every node a durable instance directory

Each node owns one directory, and nothing else may write to it. Put it on a
real filesystem.

An instance directory on `tmpfs` makes every `fsync` a silent no-op: the
cluster will appear to work and will lose committed data on power loss. If you
are running in a container, check what the mount actually is rather than what
the image implies.

For which files must survive a power cut and which are rebuilt on boot, see
[Instance directory](../reference/instance-directory.md).

## Bind the exact address you advertise

Set each node's `NodeConfig.bind` to the same value as that node's own entry in
`members`. Not a wildcard, not `0.0.0.0` — the identical concrete address.

On a multi-homed host, pick the interface address the peers actually route to
and use that same value in both places.

This is worth getting right first because the failure it produces looks like
something else entirely:

> The cluster elects a leader, but followers never advance `durable` or
> `commit`. The leader's per-peer `reported_durable` slots stay at 0. The
> receiver's `append_pos_unknown_source` counter climbs.

Datagrams arrive from a source address that matches no entry in the member map,
so the receiver cannot attribute them to a peer and the consensus agent
discards the reports. Binding the advertised address fixes it.

`uc2-node` now refuses to start on this rather than mis-binding, naming both
the `bind` value and the `members` entry it disagrees with. The failure above is
what you would have seen before that check existed — and is still what you get
if you build a `NodeConfig` directly instead of loading a config file.

## Supervise the processes

`uc2-node` handles `SIGTERM`: it drains the archive to a bounded deadline, then
stops the agents cleanly, so the restarted node rejoins from its journal instead
of paying reconstruction. Install the packaged unit:

```bash
sudo install -m 0644 packaging/systemd/uc2-node.service /etc/systemd/system/
sudo install -D -m 0644 packaging/node.example.toml /etc/uc2/node.toml
sudo systemctl daemon-reload && sudo systemctl enable --now uc2-node
```

Edit `/etc/uc2/node.toml` first — every field is commented, and the two that
matter most are `bind` (see above) and `instance_dir`.

`TimeoutStopSec` is deliberately generous (10 s) rather than short. Earlier
guidance here recommended `TimeoutStopSec=1` *because* the binaries did not
notice `SIGTERM` and a long timeout only delayed the kill. Now the timeout is
the drain's budget: cutting it short throws away the work that makes the next
start cheap. It must exceed `--drain-timeout-secs` (default 5).

The service half is your own binary; supervise it with
`packaging/systemd/uc2-service@.service`, which `BindsTo` the node so the pair
stop together and in the right order. See
[Write a service binary](write-a-service-binary.md) for the signal handling it
must implement.

If you are starting nodes over SSH, do not background with `ssh host 'cmd &'` —
the busy-spin threads hold the pipe open and the SSH session hangs. Use
`systemd-run`, or `setsid` with redirected stdio.

## Confirm the cluster is actually serving

```bash
uc2ctl status --instance-dir /srv/uc2/n0 --app-id myapp
```

One node should report `leader=true` and `can_serve=true`, and every member
should appear in the member list with a `reported_durable` that advances under
load. A member whose `reported_durable` sits at 0 has not been heard from —
start with the address check above.

If something is wrong beyond that, see
[Diagnose a node](diagnose-a-node.md).

## Where to go next

- Adding or removing members later: [Change cluster membership](change-cluster-membership.md)
- Bounding disk growth: [Keep the journal from growing without bound](bound-journal-growth.md)
- Encrypting node traffic: [Encrypt traffic between nodes](encrypt-node-traffic.md)
