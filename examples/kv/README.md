# kv_store — a replicated key-value store on ultima_cluster 2.12.0

**v2** (`KV_VERSION = 2.0.0`) adds two list operations to v1's four value
operations; every v1 operation, wire byte and behaviour is unchanged, and a
v2 node reads a v1 snapshot image. See § What changed in v2.

Operations on opaque byte keys and values, replicated by UC's
state-machine-replication log, exactly-once on retry, durable across node
restarts, with a journal you can bound with one command. A key is one of two
**shapes**:

- a **value** — **Put** / **Get** / **CAS** (v1);
- a **list** — **Append** / **List** (v2): the ordered sequence of everything
  appended to the key, oldest first.

- `kv-service` — the service half: runs the `kv` state machine attached to
  a `uc2-node` (one per node).
- `kv` — the command-line client, over TCP through any `uc2-gateway`, from
  anywhere.
- `kv-load` — a bulk loader used by the tests and the numbers below. It is
  the throughput tool, so it drives `uc_remote`'s lock-free `RemoteEngine`
  halves directly (a window of `try_submit`s drained by `poll`); the `kv` CLI
  uses the blocking `RemoteClient` because one request per invocation is
  exactly that client's job. If you copy one of these for your own client,
  copy the one that matches your shape — see
  `docs/how-to/run-a-gateway.md` § "Which client do I want?".
- `WIRE-FORMAT.md` — every byte, for a client in another language.
- `docs/DESIGN.md` — the design note the platform's SDLC standard asks for.

## Build

Rust 1.89+ (built with 1.96). The UC crates are now in-tree **workspace path
dependencies** — `uc_service`, `uc_remote` and `uc_diffreplay`, each pinned
`version = "2.13.0"` in lockstep with the workspace (`Cargo.toml`). The
clean-room original built against crates.io at `=2.12.0`; merging it in-tree
replaced that with the path deps.

```bash
cargo build --release
cargo test                      # state-machine invariants (unit + proptest), ~1 s
```

## Run a three-node cluster on this host

You need the 2.12.0 release binaries (`uc2-node`, `uc2ctl`, `uc2-gateway`);
the script defaults to `../release/uc2-2.12.0-x86_64-unknown-linux-gnu/bin`
(override with `UC2_BIN_DIR`). Cluster state goes to `$KV_ROOT`
(default `~/uc2-kv`; **not** `/tmp` — a node refuses a RAM-backed filesystem).

```bash
scripts/kvcluster.sh up --fresh
```

```text
kv cluster: root=/home/you/uc2-kv
1. nodes
   started node0 (pid …)  …
   node 0 is the serving leader
2. kv-services
3. gateways
   gateways: 127.0.0.1:9400,127.0.0.1:9401,127.0.0.1:9402
```

That is three `uc2-node`s (UDP 9300–9302), a `kv-service` attached to each
over shared memory, and a `uc2-gateway` in front of each (TCP 9400–9402),
each node with `/metrics` on 9500–9502. `scripts/kvcluster.sh` with no
arguments lists the other verbs (`down`, `status`, `leader`,
`stop|start|kill node|service|gateway N`, `wipe N`, `snapshot`,
`snapshot-show N`, `settings FILE`, `ctl N …`, `metrics N`).

The configs it writes are ordinary `node.toml`/`gateway.toml` files under
`$KV_ROOT` — copy them for a real deployment and change the addresses. The
parts that matter for this store:

```toml
[services]
names = ["kv"]             # the state machine's const NAME; kv-service attaches by it

[purge]
below_snapshot_slack_bytes = 1048576   # with a snapshot set, prune the journal below it

[session]                  # gateway.toml
envelope = true            # kv-service runs Sessioned<KvSm>: this is what makes retries safe
```

## Use `kv`

Give `kv` **every** gateway; it dials in order, follows redirects to the
leader, and re-sends across a failover on its own.

```bash
G=127.0.0.1:9400,127.0.0.1:9401,127.0.0.1:9402
kv --gateways $G put hello world
#  ok version=128 position=128 replayed=false
kv --gateways $G get hello --linearizable
#  version=128 value=world
kv --gateways $G cas hello world2 --version 128
#  ok version=192 position=192 replayed=false
kv --gateways $G cas hello world3 --version 128
#  version_mismatch current=192 position=256 replayed=false      (exit 3)
kv --gateways $G delete hello
#  ok deleted_version=192 position=320 replayed=false
kv --gateways $G get hello
#  not_found                                                     (exit 3)
kv --gateways $G cas fresh created --version 0                    # 0 = "must be absent"
kv --gateways $G digest
#  count=1 digest=0x… last_applied=384 via=127.0.0.1:9400
```

- A **version** is the log position of the write that last set the key.
  `get` returns it; `cas --version V` applies only if the key's version is
  still `V` (`0` = the key must not exist). Every write's reply carries the
  new version.
- `get` without `--linearizable` is a snapshot read served by whichever
  replica's gateway answered: fast, and it may lag the leader. With it, the
  read goes through the cluster's read barrier and reflects every write
  acknowledged before the call started.
- `digest` is a snapshot read of the replying replica's entry count, state
  hash and applied position — ask each gateway (`--gateways 127.0.0.1:9401`)
  to compare replicas. `via=` names the gateway that answered.
- `--hex` reads KEY/VALUE arguments as hex and prints values as hex; without
  it they are UTF-8 (values print lossily).

v2 list operations:

```bash
kv --gateways $G append events login          # ok version=… len=1 position=… replayed=false
kv --gateways $G append events logout         # ok version=… len=2 …
kv --gateways $G list events --linearizable
#  version=… len=2
#  [0] login
#  [1] logout
kv --gateways $G get events                   # wrong_shape (the key is a list; use `list`)   (exit 3)
```

A key is a value or a list, never both, and the shapes are strict: `get`/
`put`/`cas` on a list, or `append`/`list` on a value, print `wrong_shape`
(exit 3) and change nothing. `delete` removes either shape; the key may then
be recreated as the other. A list's version is the position of its last
append. Lists cap at 4096 elements / 64 KiB (`list_full`, exit 3), each
element ≤ 1024 B like a value.
- Exit codes: `0` success; `1` the request failed (no gateway, timed out,
  outcome unknowable); `2` bad arguments (including oversize key/value);
  `3` a negative answer (`not_found`, `version_mismatch`, `bad_request`).

### Limits

`MAX_KEY = 256 B` (non-empty), `MAX_VALUE = 1024 B` (may be empty). Derived
from the platform's standard command ceiling — 1312 B at the baseline
datagram rung with wire crypto on, the one size that holds on every UC
cluster — minus the 16 B session envelope, against a worst-case CAS frame of
`12 + key + value` bytes (`WIRE-FORMAT.md` § 4 has the arithmetic). `kv`
refuses oversize arguments (exit 2); the state machine refuses an oversize
frame from any client with `bad_request` and applies nothing.

Memory: the store is in memory. 300,000 keys × 512 B values measured
~250 MB RSS per `kv-service`; the worst case at the limits is ≈ 400 MB for
300k keys.

### Retries, and the `--client-id` footgun

Inside one `kv` process, retries are automatic and safe: the service runs
UC's `Sessioned` wrapper and the gateway's session envelope is on, so a
re-sent write is answered `replayed=true` and never applied twice.

Across processes there is no safe automatic retry, because UC's client
restarts its sequence counter at 1 in every process. `--client-id N` exists
for one purpose: **re-sending the single command whose answer you lost**:

```bash
kv --gateways $G --client-id 4242 put order-17 paid    # (connection dropped, no output)
kv --gateways $G --client-id 4242 put order-17 paid    # ok version=… replayed=true — applied once
```

Do **not** reuse a `--client-id` for a *different* command: it would be
answered with the cached reply of the previous one and silently not applied
(that is the platform's session semantics; see `LEDGER.md` L9).

## Operating

**Bound the journal.** Purge is configured (`[purge]`) and `kv-service`
starts with snapshot support, so one coordinated instant prunes every node:

```bash
scripts/kvcluster.sh snapshot            # uc2ctl snapshot on the leader → instant=<P>
scripts/kvcluster.sh snapshot-show 0     # row=0 name=kv newest=P / cluster newest=P / set=P
scripts/kvcluster.sh metrics 0 | grep -E 'uc_node_snapshot_floor_bytes|uc2_archive_first_base_bytes'
```

Measured: 178 MB of journal (300k puts) → 8 MB on all three nodes within
seconds; the freeze itself took 0.3 ms. For an automatic cadence, apply a
settings file (`snapshot_interval_bytes = 268435456`, say) with
`scripts/kvcluster.sh settings file.toml`, or seed it at genesis with
`KV_SNAPSHOT_INTERVAL`.

**What to expect during a leader change.** Writes pause for one election
(150–300 ms) and then continue; `kv` follows the redirect. A linearizable
read on the old leader fails its barrier and the client fails over. Snapshot
reads keep being served everywhere.

**What to expect during a partition.** The minority side cannot commit:
writes and linearizable reads there time out (`kv` exit 1 after
`--timeout-secs`); snapshot reads still answer from local state.

**Supervise gateways with the node.** A gateway over a *dead* node keeps
accepting writes into a ring nobody drains and answers `UNKNOWN` after
`request_timeout_ms` (2 s in the script's config); `uc2ctl status` on that
node still says `leader=true` from the frozen control page. The packaged
`uc2-gateway.service` has `BindsTo=uc2-node.service` for exactly this;
`scripts/kvcluster.sh` emulates it (stopping a node stops its gateway).

**Restarts.** Stop the service before the node; start the node before the
service. A service restarted below the purge floor installs the newest
snapshot set and tail-replays; a node wiped and restarted receives the set
from the leader (`snapshot_installed` in its log). Compare `kv digest`
across gateways afterwards.

> **Note added 2026-09-22 (2.13.0).** The paragraph below, and the recipe in
> § "Upgrading a running cluster v1 → v2", are the clean-room record of what
> the 2.12.0 documentation said, and are left as written. **Do not follow
> them on 2.13.0.** An application upgrade is now a **pinned, per-row**
> procedure: you name an origin with `uc2ctl upgrade pin`, every instance of
> the row installs that one artifact at attach, and a binary that does not
> match the pin is refused by name. An *unpinned* v2 attach is no longer the
> benign no-op this recipe assumes — it either refuses by name when an
> install is needed, or computes the counterfactual of the SDLC standard's
> § 2.3 against whatever the journal still retains. Follow
> [Upgrade an application](../../docs/how-to/upgrade-an-application.md) and
> [the SDLC standard's S1–S9](../../docs/reference/application-sdlc.md#the-upgrade-lifecycle-per-row)
> instead.

**Upgrading the store** is a flag day at 2.12.0 (the platform has no rolling
application upgrade yet): stop traffic, stop every `kv-service`, install the
new binary, start them. The wire and snapshot formats carry version bytes;
see `docs/DESIGN.md` § 7.

## What changed in v2

- **Two operations** (`append`, `list`) and the two-shape model above. The
  four v1 operations are byte-for-byte and behaviour-for-behaviour unchanged.
- **`KV_VERSION` moved `1.0.0` → `2.0.0`** (a major move: a v1 binary can
  neither apply `Append` nor read a v2 image). `uc2ctl status` shows
  `version=2.0.0` on each row; `/metrics` exports `uc2_service_version`. The
  platform equality-checks this on the snapshot path and alerts
  `Uc2ServiceVersionDrift` on a mismatch — it does **not** stop a mixed
  cluster from committing (see below).
- **Snapshot image version 2** carries a shape byte per entry. A v2 binary
  still reads a v1 image (values only); a v1 binary refuses a v2 image by
  name. `WIRE-FORMAT.md` § 5 has both layouts and the digest-continuity rule.

### Upgrading a running cluster v1 → v2 (a flag day)

> **Superseded by 2.13.0** — see the note in § Operating. Kept as written,
> because it is the clean-room record of what the 2.12.0 docs said; the
> procedure it teaches is refused by the platform now. Use
> [Upgrade an application](../../docs/how-to/upgrade-an-application.md)
> (S1–S9).

The platform has **no rolling application upgrade** at 2.12.0
(`docs/reference/application-sdlc.md` § 5), so upgrading the store is a flag
day at the *service* layer — the nodes and gateways stay up:

```bash
for n in 0 1 2; do scripts/kvcluster.sh stop  service $n; done   # SIGTERM each kv-service
# install the v2 binary in KV_BIN_DIR
for n in 0 1 2; do scripts/kvcluster.sh start service $n; done   # v2 attaches, replays, resumes
scripts/kvcluster.sh ctl 0 status | grep row=0                   # version=2.0.0
```

Acknowledged writes are never lost: they are quorum-durable in the log, and a
restarted v2 service reconstructs from the snapshot floor plus the journal
tail. **Do not run the cluster mixed** (some services v1, some v2): a v2
leader will *commit* an `Append` the v1 followers cannot apply, and if that
leader then fails, the v1 successor has no record of the acknowledged write
until every service is v2 and replays the log. The store cannot prevent this;
the platform only makes it *visible* (`Uc2ServiceVersionDrift`). Stop every
service, then start every service. Rolling back past a v2 snapshot is refused
(a v1 service fail-stops on a v2 image), so roll back only to a set taken
before the upgrade. `LEDGER.md` L17–L22 is the full second-pass record.

## Tests

```bash
cargo test                                   # state-machine invariants, golden replay, snapshot codec, Sessioned dedup
cargo test --features cluster-tests -- --nocapture   # the real three-node cluster, ~20 s
```

`cargo test` (unit) covers the state machine including the two shapes, image
v1/v2 round-trips, and the **old-image test** — a real v1 image
(`tests/fixtures/v1-golden.kvimage`, written by the v1 binary) installed into
the v2 binary yields the v1 golden digest.

`cargo test --features cluster-tests` runs two real clusters (own ports,
under `target/tmp`):

- `three_node_cluster_end_to_end`: the four v1 ops **and** the two v2 ops;
  the shape rules; a retried write and a retried append (`replayed=true`);
  SIGKILL of a follower + a write during the outage + read back; a leader
  kill with a write and read after; a 40k-key load, snapshot, purge, service
  restart below the floor, wipe-and-rejoin; `kv-service` SIGTERM → exit 0.
- `upgrade_v1_to_v2_flag_day` (needs the v1 binaries — see below): a v1
  cluster with v1 data and a v1 snapshot; a partial upgrade shown to be
  unsafe (a mixed cluster commits an append a v1 successor then can't see);
  the flag day healing it from the log; v1 data/ops intact and v2 ops
  available; a v2 service over a below-floor v1 artifact; and a v1 service
  refusing a v2 image (rollback).

**The v1 binaries** the upgrade test needs are the *previous* KV version
(`KV_VERSION = 1.0.0`, value keys only, no `append`/`list`). Point
`KV_V1_BIN_DIR` at a build of them, or drop `kv`, `kv-service` and `kv-load`
into `.run/v1-bin/` (gitignored). Build them from the git tag that precedes
the one carrying this example — check out that tag into a worktree and
`cargo build --release -p kv_store`, then copy the three binaries across.
When they are absent the test **skips** with a clear message rather than
failing, so the default `cargo test --features cluster-tests` run is green
without them. The v1-on-disk image path is also covered without any v1 binary
by the `tests/fixtures/v1-golden.kvimage` fixture in the fast unit tests
(`old_image` in `sm_invariants.rs`).
