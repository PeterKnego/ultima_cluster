# Command cheatsheet

This page lists the commands that you use with `ultima_cluster` (UC). Part 1 is
for developers. Part 2 is for operators. Each row tells you what a command does
and why you use it. The link on each command goes to its full reference.

## How to read the tables

- `<DIR>` is the instance directory of a node. `<ID>` is the application
  identity (`app_id`) of the cluster.
- In the `uc2ctl` rows, `…` stands for `--instance-dir <DIR> --app-id <ID>`.
- If the `[admin]` policy of the node is `auth = "hmac"`, add
  `--admin-key <PATH>` to each `uc2ctl` command that changes the cluster.
  [`uc2ctl` § Common arguments](uc2ctl.md#common-arguments) tells you more.

## Part 1: Developer

### 1.1 Build an application on UC

| Command | What it does | Why you use it |
|---|---|---|
| `cargo run -p counter --bin counter-single` | Starts one node and one counter service in one process. | See UC work before you set up a cluster. [Quickstart § From source](../QUICKSTART.md#6-from-source) |
| `packaging/quickstart-local.sh --bin-dir target/release` | Starts three nodes, three services and three gateways on this host, then prints `PASS`. | Make sure that a local build runs end to end. Add `--keep` to leave the cluster up. [Quickstart](../QUICKSTART.md) |
| `examples/kv/scripts/kvcluster.sh up --fresh` | Starts a three-node cluster of the key-value store example on this host. | Use a real application with snapshots and sessions as a model for your own. [`examples/kv`](../../examples/kv/README.md) |
| `kv --gateways <HOSTS> put <KEY> <VALUE>` | Sends one command to the key-value store through a gateway. | Try commands, linearizable reads and retries by hand. [`examples/kv`](../../examples/kv/README.md) |
| `cargo test` (in your service crate) | Runs the unit tests of your FSM. | Test `apply` as a plain function. It needs no cluster. [Build an application](../tutorials/build-an-application.md) |
| `uc2-diffreplay corpus export … --row <R> --from <P> --out <CORPUS>` | Copies a snapshot at position P and the log after it from a node into a corpus directory. | Get real input for a replay. Use `--around <POS>` to cut a corpus around a bug. [Diff replay](../how-to/diff-replay.md) |
| `uc2-diffreplay upgrade --corpus <C> --old <BIN> --new <BIN> --declare intent.toml --report <FILE>` | Replays the corpus through two builds and compares all outputs with your declared intent. | Prove that a new version changes only what you declared. Exit 0 means that you declared each difference. [Diff replay § Run](../how-to/diff-replay.md#4-run) |
| `uc2-diffreplay determinism --corpus <C> --bin <BIN> --report <FILE>` | Replays the corpus through the same build two times. | Find nondeterminism in your FSM. The result must be empty. [Diff replay](../how-to/diff-replay.md) |
| `uc2-diffreplay reconstruction --corpus <C> --bin <BIN> --report <FILE>` | Compares the state from the snapshot with the state from a replay from the start of the log. | Find a state that depends on how a node recovered. [Diff replay](../how-to/diff-replay.md) |
| `uc2-diffreplay pin-verify --corpus <C> --old <BIN> --new <BIN> --app-id <ID> --fsm <NAME> --to <VER> --report <FILE>` | Does a pinned upgrade on a live single-node cluster with your real binaries. | Rehearse an upgrade before you do it in production. [Diff replay § Verify the pin live](../how-to/diff-replay.md#5-verify-the-pin-live-reconstruction-mode-part-2) |

### 1.2 Work on UC itself

The [contributor command cheatsheet](contributor-cheatsheet.md) lists the
commands that you use to build, test and prove UC itself.

## Part 2: Operator

### 2.1 Run the processes

| Command | What it does | Why you use it |
|---|---|---|
| `uc2-node --config /etc/uc2/node.toml` | Starts a node. Each configuration error stops the start with an error that names the key. | Run one cluster member. [Run a cluster](../how-to/run-a-cluster.md) |
| `uc2-gateway --config /etc/uc2/gateway.toml` | Starts the TCP gateway for remote clients on the host of a node. | Let clients on other hosts reach the cluster. [Run a gateway](../how-to/run-a-gateway.md) |
| `sudo systemctl enable --now uc2-node` | Starts the node as a systemd service. Use `uc2-gateway` for the gateway. | Let systemd restart a node after a failure. |
| `sudo systemctl stop uc2-node` | Sends `SIGTERM`. The node drains and stops. | A planned stop. The restart then replays only the tail of the journal. |
| `uc2-node --version` | Prints the version. `uc2ctl` and `uc2-gateway` also take `--version`. | Make sure that each host runs the same release. |

### 2.2 Inspect a cluster

| Command | What it does | Why you use it |
|---|---|---|
| [`uc2ctl status …`](uc2ctl.md#status) | Prints the role, term, positions, members and FSMs of one node. | The first command when you examine a node. |
| `curl -s http://<METRICS_BIND>/readyz` | If the node can serve, it returns 200. If not, it returns 503. `/healthz` tells you whether the node is alive. | Use these for load balancers and health checks. [Monitor a cluster](../how-to/monitor-a-cluster.md) |
| `curl -s http://<METRICS_BIND>/metrics` | Returns all metrics in Prometheus text format. | Examine one metric by hand. Prometheus uses the same endpoint. |
| [`uc2ctl schedule show …`](uc2ctl.md#schedule-show) | Prints the committed schedule table. | Make sure that a `schedule apply` took effect. |
| [`uc2ctl settings show …`](uc2ctl.md#settings-show) | Prints the committed replicated settings. | Make sure that a `settings apply` took effect. |
| [`uc2ctl snapshot show …`](uc2ctl.md#snapshot-show) | Prints the newest snapshot position of each row and of the cluster artifact, and the position of the newest complete set. | Find the position P for a pin, a fetch or a backup. |
| [`uc2ctl upgrade show …`](uc2ctl.md#upgrade-show) | Prints the upgrade pins in the committed cluster artifact. | Examine the pin history of each row. The live pin is in `uc2ctl status`. |
| [`uc2ctl audit --instance-dir <DIR> --tail 20`](uc2ctl.md#audit) | Prints the last 20 records of the admin audit log. Add `--json` for raw records. | Find who changed the cluster, and when. |

### 2.3 Change the membership

The cluster accepts one membership change at a time.

| Command | What it does | Why you use it |
|---|---|---|
| [`uc2ctl add-learner … --id <N> --addr <IP:PORT>`](uc2ctl.md#add-learner) | Adds a node that gets the log but does not vote. | The first step to add a node. A learner catches up before it votes. |
| [`uc2ctl promote … --id <N>`](uc2ctl.md#promote) | Changes a learner into a voter. | Make a caught-up learner count toward the quorum. |
| [`uc2ctl demote … --id <N>`](uc2ctl.md#demote) | Changes a voter into a learner. | The first step to remove a voter. |
| [`uc2ctl remove-learner … --id <N>`](uc2ctl.md#remove-learner) | Removes a learner from the cluster. | Take a node out of the cluster. A removed id cannot join again. |
| [`uc2ctl remove-voter … --id <N>`](uc2ctl.md#remove-voter) | Removes a voter from the cluster. | Take a voter out in one step, for example the leader. |

[Change cluster membership](../how-to/change-cluster-membership.md) has the
procedures.

### 2.4 Change replicated configuration

| Command | What it does | Why you use it |
|---|---|---|
| [`uc2ctl schedule apply <FILE.toml> …`](uc2ctl.md#schedule-apply) | Replaces the schedule table of recurrent ticks for all nodes. | Run periodic or daily work in an FSM. [Run work on a schedule](../how-to/run-work-on-a-schedule.md) |
| [`uc2ctl settings apply <FILE.toml> …`](uc2ctl.md#settings-apply) | Changes the four replicated settings for all nodes. | Change `fsm_lag`, `admission_bytes` or the snapshot cadence without a restart. [Configuration § `[settings]`](configuration.md#settings) |

### 2.5 Take snapshots

| Command | What it does | Why you use it |
|---|---|---|
| [`uc2ctl snapshot …`](uc2ctl.md#snapshot) | Puts a snapshot instant on the log. Each node makes a snapshot of each row at one position. | Bound the growth of the journal, make an upgrade origin, or make a backup point. |
| [`uc2ctl snapshot … --standby`](uc2ctl.md#snapshot) | Only the learners make the snapshot. | Take a snapshot and never stop commit. |
| [`uc2ctl snapshot fetch … --from <LEARNER>`](uc2ctl.md#snapshot-fetch) | Copies a complete snapshot set from a learner to this node. | Give a voter a set after a standby instant. |

[Keep the journal from growing without bound](../how-to/bound-journal-growth.md)
tells you how to use snapshots with purge.

### 2.6 Upgrade

| Command | What it does | Why you use it |
|---|---|---|
| `scripts/uc2_flag_day.sh --hosts … --upgrade-cmd '…' --yes-traffic-stopped` | Stops all nodes, runs your upgrade command on each host, starts all nodes and prints the downtime. If a step fails, it starts all nodes again. | Upgrade UC across a change of wire or cnc version. [Upgrade a cluster](../how-to/upgrade-a-cluster.md) |
| [`uc2ctl upgrade pin … --row <R> --to <VER> --origin <P>`](uc2ctl.md#upgrade-pin) | Records on the log that row R changes to version VER from the snapshot at P. | Make each instance of the new FSM version start from the same state. [Upgrade an application](../how-to/upgrade-an-application.md) |

### 2.7 Back up and recover

| Command | What it does | Why you use it |
|---|---|---|
| [`uc2ctl backup --instance-dir <DIR> --out <DIR>`](uc2ctl.md#backup) | Copies a node's journal, state and snapshots to a new directory. The node can run. | Keep a copy off the node before an upgrade or for disaster recovery. [Back up a cluster](../how-to/back-up-a-cluster.md) |
| [`uc2ctl verify-backup <ARTIFACT>`](uc2ctl.md#verify-backup) | Makes sure that a backup is complete and consistent. | Examine a backup before you need it. |
| [`uc2ctl restore <ARTIFACT> --instance-dir <DIR>`](uc2ctl.md#restore) | Writes a backup into an empty instance directory. | Rebuild a node on a new host. |
| [`uc2ctl force-single-member --instance-dir <DIR> --app-id <ID> --node-id <N> --confirm-cluster <ID>`](uc2ctl.md#force-single-member) | Makes one stopped node the only member of its cluster. | Recover after the cluster loses a majority of its voters for ever. The cluster can lose committed writes. [Recover from quorum loss](../how-to/recover-from-quorum-loss.md) |

### 2.8 Secure the admin commands

| Command | What it does | Why you use it |
|---|---|---|
| [`uc2ctl gen-admin-key <PATH>`](uc2ctl.md#gen-admin-key) | Writes a new 32-byte admin key file with mode `0600`. | Sign admin commands when `[admin] auth = "hmac"`. [Configuration § Admin authentication](configuration.md#admin-authentication) |
