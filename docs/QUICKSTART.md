# Quickstart

From a download to a real three-node cluster, in two commands and about a
minute. No Rust toolchain, no broker, no ZooKeeper, no container runtime.

Every command and every output from §2 onward is real — copied from an actual
run, not written from memory.

You need Linux (x86-64 or aarch64), `bash`, coreutils, and a directory on a
real disk. That is the whole list for §2 onward — **running** the cluster
needs nothing else. Step 1 also uses `curl` to fetch the release and
[`cosign`](https://docs.sigstore.dev/cosign/installation/) to verify it; both
are for getting the artifact, not for using it.

---

## 1. Download it, and verify it

Releases live at
[github.com/PeterKnego/ultima_cluster/releases](https://github.com/PeterKnego/ultima_cluster/releases).
Pick your architecture and take the tarball, its checksum, `SHA256SUMS`, and
the signature bundle:

```bash
VER=2.10.0
TARGET=x86_64-unknown-linux-gnu        # or aarch64-unknown-linux-gnu
BASE=https://github.com/PeterKnego/ultima_cluster/releases/download/v$VER

curl -fLO $BASE/uc2-$VER-$TARGET.tar.gz
curl -fLO $BASE/uc2-$VER-$TARGET.tar.gz.sha256
curl -fLO $BASE/uc2-$VER-$TARGET.tar.gz.sigstore.json
curl -fLO $BASE/SHA256SUMS
```

The cheap check, which catches a truncated download but not a hostile one:

```bash
sha256sum -c uc2-$VER-$TARGET.tar.gz.sha256
```

The real check. Every release file is signed **keylessly** by the GitHub
Actions workflow that built it — there is no long-lived private key anywhere,
and what you are verifying is *which workflow, in which repository, on what
kind of ref* produced the file:

```bash
cosign verify-blob \
  --bundle uc2-$VER-$TARGET.tar.gz.sigstore.json \
  --certificate-identity-regexp \
    'https://github.com/PeterKnego/ultima_cluster/.github/workflows/release.yml@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  uc2-$VER-$TARGET.tar.gz
```

The two `--certificate-*` flags are the whole point. Without them a signature
proves only that *somebody* held a sigstore certificate, which is not a claim
worth checking. `cosign verify-blob` without an identity pin is not
verification.

`SHA256SUMS` is the equivalent path if you are fetching several files: verify
it once (it is signed the same way, `SHA256SUMS.sigstore.json`) and then
`sha256sum -c SHA256SUMS --ignore-missing`. It lists **the `.tar.gz` files** —
both architectures' tarballs and the SBOM archive — and nothing else; the
`.sha256` sidecars and the `.sigstore.json` bundles are not in it, and do not
need to be (a sidecar restates a hash you now have signed, and a bundle
carries its own signature).

> **Release candidates.** Tags with a suffix — `v2.6.0-rc.1` — are real,
> fully signed releases marked **prerelease** on GitHub, and they never take
> over the "Latest release" pointer. They exist so the publishing path itself
> gets exercised before the version everyone downloads. Use the unsuffixed
> tag unless you are deliberately testing one.

Then unpack:

```bash
tar xzf uc2-$VER-$TARGET.tar.gz
cd uc2-$VER-$TARGET
```

You now have `bin/` (five binaries), `packaging/` (example configs, the
quickstart script, systemd units, Prometheus rules, a Grafana dashboard, a
`Dockerfile` and a `compose.yml`), `LICENSE`, and `README-release.md`.

## 2. Run the quickstart

```bash
packaging/quickstart-local.sh
```

```text
ultima_cluster quickstart
   binaries: /home/you/uc2-2.10.0-x86_64-unknown-linux-gnu/bin
   root:     /home/you/uc2-quickstart

1. writing configuration
   3 node.toml + 3 gateway.toml under /home/you/uc2-quickstart
2. starting three nodes
   started node0 (pid 2213586)
   started node1 (pid 2213587)
   started node2 (pid 2213588)
   waiting for a serving leader (up to 30s)
   node 0 is the serving leader
3. attaching a counter-service to each node
   started service0 (pid 2213627)
   started service1 (pid 2213628)
   started service2 (pid 2213629)
4. starting a gateway in front of each node
   started gateway0 (pid 2213630)
   started gateway1 (pid 2213631)
   started gateway2 (pid 2213632)
   waiting for the gateways to accept (up to 30s)
   listening on 127.0.0.1:9200,127.0.0.1:9201,127.0.0.1:9202
5. driving the cluster from outside, through the gateways
   reset            -> value=0 position=32 replayed=false
   add 5            -> value=5 position=96 replayed=false
   add 5            -> value=10 position=160 replayed=false
   get              -> value=10

PASS
```

Exit `0` is `PASS`, `1` is a named failure with the last twenty lines of every
log, `3` is a precondition it refused to run past (a missing binary, a port
already in use, a root on a RAM-backed filesystem).

Useful flags:

| Flag | Effect |
|---|---|
| `--bin-dir DIR` | where the five binaries are. Defaults to the `bin` sibling of the script — i.e. the tarball layout — or `$UC2_BIN_DIR`. |
| `--root DIR` | where cluster state goes. Default `$HOME/uc2-quickstart`. Every run starts a **fresh** cluster: `n0`/`n1`/`n2` under it are deleted first, but only if the script created that root (it leaves a marker file), so pointing it at a real instance directory is refused, not obeyed. A root under `/tmp` or `/dev/shm` is refused outright — those are usually RAM-backed, every `fsync` there is a silent no-op, and a node will not start on one. |
| `--secs N` | hold the cluster up N more seconds after the demo. Default 0. |
| `--keep` | leave it running and print the PIDs. Without this, everything the script started is killed on exit — including on failure and on `Ctrl-C`. |
| `--full` | accepted and ignored; three gateways is the only mode. |

Everything it wrote is under `--root`: `n0/`, `n1/`, `n2/` (the instance
directories), `gw0.toml`–`gw2.toml`, `admin.key`, and `logs/` with one file
per process it started. Budget about 250 MiB — each node **reserves** its log
buffer and IPC rings up front (~78 MiB), so a full disk is a named startup
refusal rather than a `SIGBUS` later.

To poke at it yourself:

```bash
packaging/quickstart-local.sh --keep
bin/counter-remote --gateways 127.0.0.1:9200,127.0.0.1:9201,127.0.0.1:9202 \
  --app-id quickstart add 7
```

```text
value=17 position=224 replayed=false
```

## 3. What just happened

Ten processes, in four roles — nine daemons and the client that drove them:

- **Three `uc2-node` daemons** — consensus, replication, durability. They
  elected a leader among themselves (node 0 here; which one wins is genuinely
  arbitrary, since each has a differently-seeded randomized election timeout).
- **Three `counter-service` processes**, one attached to each node over shared
  memory. This is where *your* state machine runs, in its own process. Every
  replica runs its own copy and applies the same commands in the same order.
- **Three `uc2-gateway` processes**, one per node — a TCP front door for
  clients that cannot attach to shared memory. One per node is what makes
  redirection work: a client that dials a follower's gateway is told where the
  leader is.
- **One `counter-remote`**, the client, driving the whole thing from outside
  over plain TCP.

Ask a node what it thinks:

```bash
bin/uc2ctl status --instance-dir ~/uc2-quickstart/n0 --app-id quickstart
```

```text
config: version=0 pending=false
role: leader=true can_serve=true term=1 leader_hint=0
log: commit=224 durable=224 append=224
members:
  id=1 role=voter reported_durable=224
  id=2 role=voter reported_durable=224
```

`commit`, `durable` and `append` are **byte positions**, not entry indices —
the absolute offset of a place in the log stream. There is no "entry 3" in
this system; there is "the frame at byte 160", which is why the writes above
report `position=96`, `position=160`. That is what lets replication be a
byte-stream fan-out. See
[ARCHITECTURE.md](/docs/ARCHITECTURE.md#positions-not-indices).

The two writes were acknowledged only after a **majority of the three nodes
had `fsync`'d them**. That is what "committed" means here. The read went
through the cluster's read barrier, from a gateway that may not have been the
leader's.

### The `node.toml` it wrote

This is a real node config — the same shape you would install at
`/etc/uc2/node.toml`. Node 0's, annotated:

```toml
id = 0                                       # this node's id, unique in the cluster
bind = "127.0.0.1:9100"                      # the UDP socket peers reach it on
instance_dir = "/home/you/uc2-quickstart/n0" # log buffer, journal, cnc page, audit log
app_id = "quickstart"                        # cluster identity; a mismatch is a refusal

[[members]]                                  # the FULL voting membership, byte-identical
id = 0                                       # on every host — including this node's own
addr = "127.0.0.1:9100"                      # entry, whose addr must equal `bind` exactly

[[members]]
id = 1
addr = "127.0.0.1:9101"

[[members]]
id = 2
addr = "127.0.0.1:9102"

[crypto]
enabled = false      # cleartext node-to-node traffic

[admin]
auth = "hmac"        # membership changes must be signed
keys = [{ name = "admin", key_path = "/home/you/uc2-quickstart/admin.key" }]
```

Three rules are worth carrying away from that file:

- **`bind` and this node's own `[[members]]` entry are the identical
  address.** Not a coincidence — `uc2-node` refuses to start if they
  disagree. It is the single most common misconfiguration.
- **`[crypto]` and `[admin]` are required sections.** Since `v2.6.0` an
  absent one is a startup refusal that names it, never a silent default. A
  `node.toml` written for `v2.5.0` will not start until both choices are
  written down — see
  [Upgrade a cluster](/docs/how-to/upgrade-a-cluster.md).
- **`enabled = false` is a *choice*, not "off".** It means cleartext between
  nodes, which is fine on loopback and not fine across a network you do not
  own. `auth = "hmac"` here means the quickstart generated a 32-byte key
  (`uc2ctl gen-admin-key`) and membership changes must be signed with it; the
  alternative, `auth = "none"`, makes filesystem permissions the only
  boundary.

### The `gateway.toml` it wrote

```toml
[local]
instance_dir = "/home/you/uc2-quickstart/n0"  # the node this edge relays into
app_id = "quickstart"                         # must match the node's exactly
listen = "127.0.0.1:9200"                     # where remote clients connect

[[members]]          # node id -> gateway address. This table must be
node_id = 0          # byte-identical on every host: it is the ONLY place
gateway = "127.0.0.1:9200"   # gateway addresses exist, and it is what
                             # answers REDIRECT and LEADER_CHANGED.
[[members]]
node_id = 1
gateway = "127.0.0.1:9201"

[[members]]
node_id = 2
gateway = "127.0.0.1:9202"

[session]
envelope = false     # raw pass-through: see below
```

`[session] envelope = false` is the honest setting for this demo.
`counter-service` runs a plain `CounterSm`, not a
`uc_service::Sessioned<CounterSm>`, so there is nothing on the far end to
strip a session envelope. A production service wraps its state machine in
`Sessioned` and turns the envelope on — that is what makes a re-sent write
answer `replayed=true` ("already applied; not applied twice") instead of
applying a second time. See
[the state-machine contract](/docs/reference/state-machine-contract.md).

## 4. The state machine you just ran

All of it, from [`examples/counter/src/lib.rs`](/examples/counter/src/lib.rs):

```rust
impl StateMachine for CounterSm {
    type Command = Command;
    type Response = Applied;
    type Query = Query;
    type QueryResponse = QueryResponse;

    fn apply(&mut self, position: u64, cmd: Command) -> Applied {
        match cmd {
            Command::Add(n) => self.value = self.value.wrapping_add(n),
            Command::Reset => self.value = 0,
        }
        self.last_applied = Some(position);
        Applied { value: self.value, position }
    }

    fn query(&self, q: Query) -> QueryResponse {
        match q {
            Query::Value => QueryResponse { value: self.value },
        }
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}
```

That is the entire contract: `apply` runs on every replica for every committed
command in log order, `query` answers reads from local state, and
`last_applied` tells the framework where to resume after a restart.

Notice the `wrapping_add`. `apply` must be deterministic — same state plus same
command, same result on every node forever — and plain `+` would panic on
overflow in debug while wrapping in release, so two replicas built differently
would diverge. See
[the apply contract](/docs/ARCHITECTURE.md#the-apply-path-and-the-sdk).

`StateMachine` is the *typed* tier: the framework does one bincode decode per
command and one encode per response for you. The other tier,
`RawStateMachine`, is bytes-in/bytes-out with no codec at all — you pick your
own framing, and the framework passes the payload straight through. A type
implements exactly one of the two;
[the state-machine contract](/docs/reference/state-machine-contract.md) has
both signatures, the byte-identity promise between them, and when the raw tier
is worth it.

## 5. A real three-node cluster

The quickstart is a demo: three nodes sharing one kernel and one disk are a
majority of *processes*, not a majority of *failure domains*. Moving it onto
real hosts is one node per machine, one config file per host, and a
supervisor:

- **[Run a cluster on real hosts](/docs/how-to/run-a-cluster.md)** — installing
  the binaries, the per-host `node.toml`, the address rule, durable instance
  directories, systemd units, and restart cost.
- **[Run a gateway](/docs/how-to/run-a-gateway.md)** — one edge per node host,
  the `[[members]]` map that must agree everywhere, and what a client sees on
  `REDIRECT` / `LEADER_CHANGED` / `RETRY`.
- **Containers.** The same demo, in containers, from the image the release
  publishes:

  ```bash
  UC2_IMAGE=ghcr.io/peterknego/uc2:2.10.0 \
    docker compose -f packaging/compose.yml up -d
  ```

  The image is signed too — `cosign verify` with the same two `--certificate-*`
  flags as above.
- **[Monitor a cluster](/docs/how-to/monitor-a-cluster.md)** — `[metrics]`,
  the `/healthz` and `/readyz` probes, and the alert rules and dashboard
  shipped under `packaging/`.

### Before running this for real

The counter example is deliberately minimal. A production deployment differs in
ways the runbook covers in full, but at minimum:

- **Instance directories must be on real durable storage**, not tmpfs. The
  archive agent's `fdatasync` is the entire durability story; on a RAM-backed
  filesystem it is a no-op and you have none.
- **Size the log buffer for your throughput.** `buffer_bytes` defaults to
  64 MiB in `node.toml` (the examples use 4 MiB, which is a toy). The appender
  may never overwrite bytes the archive has not recorded, so an undersized ring
  turns into ingress backpressure — and the whole of it is *reserved* on disk at
  startup, so sizing it up is a disk decision too. See
  [Configuration](/docs/reference/configuration.md).
- **Implement `SnapshotStateMachine`** if you want the log purged. Without it
  the journal grows forever.
- **Enable wire crypto** if node-to-node traffic crosses anything you do not
  trust. It is off by default, and `[admin] auth = "hmac"` only authenticates
  cluster-wide when paired with it — see
  [Encrypt traffic between nodes](/docs/how-to/encrypt-node-traffic.md).

## 6. From source

If you have a Rust toolchain (1.89 or newer; the repo pins a newer stable in
`rust-toolchain.toml` for its own builds), everything above builds from the
tree — and the smallest possible version of it is one process:

```bash
cargo run -p counter --bin counter-single
```

```text
instance dir: /tmp/.tmpmVhdVb
{"ts_ns":1787499054879647864,"level":"info","event":"became_leader","node":0,"term":1,"base":0}
{"ts_ns":1787499054882003565,"level":"info","event":"serving_changed","node":0,"term":1,"can_serve":true}
node is leader and serving

Add(  1) -> value   1  @ log position 32
Add(  2) -> value   3  @ log position 96
Add(  3) -> value   6  @ log position 160
Add( 10) -> value  16  @ log position 224
Add( -6) -> value  10  @ log position 288

linearizable read -> 10
snapshot read     -> 10

Reset      -> value   0  @ log position 352

Everything above went through consensus and was fsync'd before it was acked.
```

That is a real single-node cluster — same consensus code, same log, same
durability. It elects itself, appends the `NewTerm` frame that Raft §5.4.2
requires, commits as soon as its own `fsync` lands, and serves. Node, service
and client are all in this one process, which is a configuration choice rather
than a special mode: they coordinate through counters in shared memory whether
they share a process or not.

To build the shipped binaries and run the same quickstart from a source tree:

```bash
cargo build --release --locked \
  -p uc_node --bin uc2-node \
  -p uc_ctl --bin uc2ctl \
  -p uc_gateway --bin uc2-gateway \
  -p counter --bin counter-service --bin counter-remote

packaging/quickstart-local.sh --bin-dir target/release
```

(`target/release` unless you have set `CARGO_TARGET_DIR`.) This is what
nightly CI's `quickstart` job runs; the release workflow runs the same script
against the *tarball*, in a bare `ubuntu:24.04` container with no toolchain in
it, and refuses to publish anything if it does not print `PASS`.

## 7. Where to go next

- **[ARCHITECTURE.md](/docs/ARCHITECTURE.md)** — how it works, and why it is
  shaped this way: positions instead of indices, the four agents, the data and
  control planes, the apply path.
- **[VERIFICATION.md](/docs/VERIFICATION.md)** — what is proved, what is
  checked, what is only bug-hunted, and what is not verified at all.
- **[Write a service binary](/docs/how-to/write-a-service-binary.md)** — your
  state machine, in its own process, attached to a node.
- **[The state-machine contract](/docs/reference/state-machine-contract.md)** —
  both tiers, `SnapshotStateMachine`, `OutputHandler`, and `Sessioned<S>`.
- **[Client SDKs](/docs/reference/remote-protocol.md)** — the framed TCP
  protocol a remote client speaks, and `RemoteEngine`/`RemoteClient`, the Rust
  implementation of it. On-host clients use `uc_client`'s three tiers
  instead: `Engine`, `PipelinedClient` + `Ticket`, and the blocking `Client`
  shim. M14: `submit_to(id, …)`, `submit_all(…)` (every FSM's answer, in id
  order) and `query_*_on(id, …)` pick which state machine answers; the plain
  calls mean FSM 0.
- **[The operations runbook](/docs/ops/uc2-runbook.md)** — instance directory
  layout, backups, membership changes, quorum-loss recovery, upgrades.
- **[Versioning and the semver promise](/docs/reference/semver-policy.md)** —
  what is API, what is not, and what a flag day means.
- **[`examples/counter/`](/examples/counter)** — everything you just ran.
