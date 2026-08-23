# How to run a cluster on real hosts

Covers the move from a single-box cluster to nodes on separate machines, where
addresses, the network path, client placement, and process supervision start to
matter.

If you have not run one at all yet, work through
[the quickstart](../QUICKSTART.md) first — it gets three nodes up on one box
and is a better place to meet the moving parts.

## Install the binaries on each host

Three things go onto every host:

- `uc2-node` — the node daemon.
- `uc2ctl` — the admin CLI, for status and membership changes.
- Your own service binary — the half that runs your state machine. See
  [Write a service binary](write-a-service-binary.md).

### From a release tarball (no toolchain needed)

Take the tarball for the host's architecture from
[the releases page](https://github.com/PeterKnego/ultima_cluster/releases),
**verify it**, then install:

```bash
VER=2.6.0
TARGET=x86_64-unknown-linux-gnu        # or aarch64-unknown-linux-gnu
BASE=https://github.com/PeterKnego/ultima_cluster/releases/download/v$VER

curl -fLO $BASE/uc2-$VER-$TARGET.tar.gz
curl -fLO $BASE/uc2-$VER-$TARGET.tar.gz.sigstore.json

cosign verify-blob \
  --bundle uc2-$VER-$TARGET.tar.gz.sigstore.json \
  --certificate-identity-regexp \
    'https://github.com/PeterKnego/ultima_cluster/.github/workflows/release.yml@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  uc2-$VER-$TARGET.tar.gz

tar xzf uc2-$VER-$TARGET.tar.gz
sudo install -m 0755 uc2-$VER-$TARGET/bin/* /usr/local/bin/
```

`/usr/local/bin` is the path the packaged systemd units expect. The tarball
also carries `packaging/` — `node.example.toml`, the systemd units, the
Prometheus rules and the Grafana dashboard referenced later in this guide.
The identity pin on `cosign verify-blob` is not optional dressing; see
[the quickstart's download section](../QUICKSTART.md#1-download-it-and-verify-it).

### Or from the container image

```bash
ghcr.io/peterknego/uc2:2.6.0
```

Multi-architecture (amd64 + arm64), built from the same tarballs, signed by
digest — `cosign verify` with the same two `--certificate-*` flags.
`packaging/compose.yml` in the tarball is a worked single-host example.

### Or from source

```bash
cargo build --release --locked -p uc2_node -p uc2ctl --bins
```

Requires Rust 1.89 or newer (the workspace's `rust-version` floor;
`rust-toolchain.toml` pins CI, releases and this repo's own dev builds to a
newer stable, but any toolchain at or above 1.89 builds it). The binaries land
in `target/release/`; copy them to `/usr/local/bin` on each host. Build once
per architecture, not once per host.

## Write one config file per host

Every host gets the same `[[members]]` block — the full voting membership,
identical everywhere — and differs only in `id`, `bind`, and (if the paths
differ) `instance_dir`. For node 1 of a three-node cluster:

```toml
id = 1
bind = "10.0.0.11:9100"
instance_dir = "/srv/uc2/n1"
app_id = "myapp"

[[members]]
id = 0
addr = "10.0.0.10:9100"

[[members]]
id = 1
addr = "10.0.0.11:9100"

[[members]]
id = 2
addr = "10.0.0.12:9100"

[crypto]
enabled = false

[admin]
auth = "none"
```

`[crypto]` and `[admin]` are required sections since `v2.6.0` — an absent
one is a named startup refusal, not "off" by omission. The values above are
the cleartext / filesystem-boundary posture every release before `v2.6.0`
had implicitly; see
[Encrypt traffic between nodes](encrypt-node-traffic.md) and
[Change cluster membership](change-cluster-membership.md#if-the-cluster-requires-signed-admin-requests)
to turn either on, and
[Configuration: Admin authentication](../reference/configuration.md#admin-authentication)
for the full `[admin]` key table.

[`packaging/node.example.toml`](../../packaging/node.example.toml) is the
annotated reference copy — every optional field with its default shown — and a
test pins it against the loader, so it cannot drift. Note it ships
`[admin] auth = "hmac"` with an `ops-alice` key, so a verbatim copy does not
start until that key file exists — run
`uc2ctl gen-admin-key /etc/uc2/admin/alice.key` first, or set
`auth = "none"` (with `keys = []`) for the pre-`v2.6.0` filesystem-boundary
posture. The full surface is in
[Configuration](../reference/configuration.md).

Two properties of the file save you from whole classes of quiet failure: a
typo'd key is a startup refusal naming the key, and every semantic rule is
checked before the first agent spawns — the node refuses with the offending
field named rather than failing later in a way that looks like something else.

One property surprises people later: **`members` is a seed, not a setting.** It
is authoritative only for a fresh instance directory. After the first boot the
durable config record owns membership, and a restart with an edited `members`
list has no effect. To change membership on a running cluster, use `uc2ctl` —
see [Change cluster membership](change-cluster-membership.md).

## Open the network path between the nodes

Node-to-node replication is UDP on the `bind` port. Every node's port must be
reachable from every other node, in both directions. Nothing else crosses the
network: services, clients, and `uc2ctl` all attach over shared memory on the
node's own host, so there are no client ports to open, and enabling
[wire crypto](encrypt-node-traffic.md) later uses the same socket.

The wire assumes the path carries **1408-byte UDP payloads without
fragmentation** — comfortable headroom on any standard 1500-MTU path, but worth
checking on overlays, VPNs, and tunnels that shrink the effective MTU. Related:
a max-size frame plus its headers (and the crypto tag, when encryption is on)
must fit that budget, because the node does not fragment frames. Raising
`max_payload` past it is a startup refusal that states the exact byte need.

## Give every node a durable instance directory

Each node owns one directory, and nothing else may write to it. Put it on a
real filesystem.

An instance directory on `tmpfs` makes every `fsync` a silent no-op: the
cluster will appear to work and will lose committed data on power loss. If you
are running in a container, check what the mount actually is rather than what
the image implies. `uc2-node` refuses to start on a RAM-backed filesystem, and
the test-only override never silences the warning.

The directory also needs free space **before** the node starts, not as it
fills: the node reserves its memory-mapped files on disk at boot — the log
buffer (`buffer_bytes`, 64 MiB by default) plus about 14 MiB of IPC rings, so
roughly 78 MiB at the defaults, and the journal grows on top of that. A node
that cannot reserve it refuses to start and says so:

```
uc2-node: failed to start node 0: io: No space left on device (os error 28)
```

That refusal is deliberate. These files are mapped into memory, and a write to
a page with no disk block behind it is a `SIGBUS`, which kills the process
outright rather than letting it fail-stop cleanly — so the space is claimed up
front, where the failure is a startup error you can act on.

For which files must survive a power cut and which are rebuilt on boot, see
[Instance directory](../reference/instance-directory.md).

## Bind the exact address you advertise

Set each node's `bind` to the same value as that node's own entry in
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

Exit codes distinguish the two ways a start can fail: **exit 2** is a config
refusal, which the packaged unit does not retry (the same file is refused
identically every time, and retrying only delays you seeing why); **exit 1** is
a runtime failure — a port still held, say — which is worth retrying and is.

The service half is your own binary; supervise it with
`packaging/systemd/uc2-service@.service`, which `BindsTo` the node so the pair
stop together and in the right order. See
[Write a service binary](write-a-service-binary.md) for the signal handling it
must implement.

If you are starting nodes over SSH, do not background with `ssh host 'cmd &'` —
the busy-spin threads hold the pipe open and the SSH session hangs. Use
`systemd-run`, or `setsid` with redirected stdio.

## Put a client next to every node

There is no network client. `uc2_client` attaches to a node over shared
memory, so anything that submits commands or reads state must run **on a host
that runs a node** — typically your gateway process (REST, gRPC, whatever faces
your callers), holding one client attached to its local node.

The shape that works — and the one the M9 fleet gate runs — is **one client
per node, alive at all times**:

- The leader's client serves writes and linearizable reads.
- A follower's client completes those with `NotLeader { hint }`, where `hint`
  is the leader's node id (`None` while an election is unresolved). Your
  gateway answers its own callers with a redirect to the leader's host, at
  whatever protocol level it speaks.
- Snapshot reads are served by every replica from its own copy of the state —
  no redirect, no quorum round-trip, may lag the leader slightly.

Two mistakes to avoid, both of which the gate's first fleet run paid for:

- **Do not tear down and respawn clients when leadership moves.** Keep one
  attached everywhere and let the new leader's client simply start succeeding.
  Respawning puts a process start plus a wait-for-serving inside your outage
  window: on the gate fleet, that harness mistake read as a 62 % throughput
  dip where the cluster's real dip was 8.5 %.
- **Back off in the `NotLeader` arm.** A follower-side loop that hot-retries
  connect/submit/`NotLeader` with no sleep burns a core on every non-leader
  host. A couple of milliseconds is enough.

`PipelinedClient::leader_hint()` exposes the same hint between requests, if
your gateway wants to route proactively rather than on rejection.

## What a planned restart costs

A `systemctl restart uc2-node` (or stop, upgrade binary, start) is cheap by
design, and the [M9 gate](../benchmarks/uc2-m9-gate-2026-08-19.md) measured it
on a real fleet under sustained load:

- **Stop is sub-second.** `SIGTERM` to process exit measured 0.042–0.098 s
  with the archive draining under load, against a < 1 s bar.
- **The restart replays, it does not reconstruct.** A drained node holds every
  acked byte in its journal and rejoins by replaying a short tail; the gate
  verifies no snapshot install occurs across the cycle.
- **Restarting the leader costs one election.** This release ships no
  leadership transfer, so a leader stop leaves the cluster leaderless for one
  randomized election timeout (150–300 ms by default). On the gate fleet the
  commit rate dipped 8.5 % over the 5 s around a leader restart and was
  confirmed back at baseline within 10.5 s of the `SIGTERM` — an upper bound
  dominated by the measurement's own SSH round-trips, not the cluster.

For a rolling restart (a binary upgrade, say), restart followers first and the
leader last, so the cluster pays that election exactly once.

## Confirm the cluster is actually serving

On any node's host — `uc2ctl` reads the local control page, so run it there,
not from your workstation:

```bash
uc2ctl status --instance-dir /srv/uc2/n0 --app-id myapp
```

One node should report `leader=true` and `can_serve=true`, and every member
should appear in the member list with a `reported_durable` that advances under
load. A member whose `reported_durable` sits at 0 has not been heard from —
start with the address check above.

If something is wrong beyond that, see
[Diagnose a node](diagnose-a-node.md).

If the node's `[metrics]` section is on, the same question has an HTTP
answer that a load balancer or an orchestrator can poll directly, without
`uc2ctl` or host access:

```bash
curl -s http://127.0.0.1:9600/readyz
```

`200` means this node is fit to route traffic to (role-aware: a leader also
needs `can_serve`, a follower just needs to be healthy); `503` names the
reason in the body. `/healthz` answers the narrower "should this process be
restarted?" question instead. See
[Monitor a cluster](monitor-a-cluster.md#the-probe-endpoints) for the full
probe semantics, and for wiring up Prometheus scraping and alerting instead
of polling by hand.

## Where to go next

- Adding or removing members later: [Change cluster membership](change-cluster-membership.md)
- Bounding disk growth: [Keep the journal from growing without bound](bound-journal-growth.md)
- Encrypting node traffic: [Encrypt traffic between nodes](encrypt-node-traffic.md)
- Watching it over time: [Monitor a cluster](monitor-a-cluster.md)
