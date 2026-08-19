# Quickstart

From nothing to a running replicated cluster. Every command and every output
below is real — copied from an actual run, not written from memory.

You need a Rust toolchain (the repo pins one in `rust-toolchain.toml`) and Linux
or macOS. Nothing else — no broker, no ZooKeeper, no container.

---

## 1. The whole thing in one command

```bash
cargo run -p counter --bin counter-single
```

```text
instance dir: /tmp/.tmpkLWLAJ
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
durability. It elects itself, appends the NewTerm frame that Raft §5.4.2
requires, commits as soon as its own fsync lands, and serves.

Two things worth noticing:

- **The positions are byte offsets, not indices.** They step by 64 because that
  is how large each frame is. There is no "entry 3" in this system; there is
  "the frame at byte 160." See [ARCHITECTURE.md](/docs/ARCHITECTURE.md#positions-not-indices).
- **The node, the service, and the client are all in this one process.** That is
  a configuration choice, not a special mode — they coordinate through counters
  in shared memory whether they share a process or not.

## 2. The state machine you just ran

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

## 3. A real three-node cluster

Now the interesting version. We will use five terminals: three nodes, three
services, and one client.

**Set up:**

```bash
export BASE=~/.cache/counter-demo
rm -rf $BASE && mkdir -p $BASE/{0,1,2}
```

**Write a config file per node.** This is how a real node is configured — the
same file shape you would install at `/etc/uc2/node.toml`:

```bash
for i in 0 1 2; do
  cat > $BASE/n$i.toml <<EOF
id = $i
bind = "127.0.0.1:1900$((i+1))"
instance_dir = "$BASE/$i"
app_id = "counter"

[[members]]
id = 0
addr = "127.0.0.1:19001"

[[members]]
id = 1
addr = "127.0.0.1:19002"

[[members]]
id = 2
addr = "127.0.0.1:19003"
EOF
done
```

Note `bind` and this node's own `members` entry are the identical address. That
is a rule, not a coincidence — `uc2-node` refuses to start if they disagree.

**Start the three nodes**, one per terminal:

```bash
cargo run -p uc2_node --bin uc2-node -- --config $BASE/n0.toml
cargo run -p uc2_node --bin uc2-node -- --config $BASE/n1.toml
cargo run -p uc2_node --bin uc2-node -- --config $BASE/n2.toml
```

Within a second or so, one of them announces itself:

```text
uc2-node: node 0 listening on 127.0.0.1:19001
uc2-node: node 0 is now follower (term 0)
uc2-node: node 0 is now LEADER (term 1)
```

The other two stay followers. Which node wins is genuinely arbitrary — each has
a differently-seeded randomized election timeout, precisely so they do not all
stand for election at the same instant and split the vote forever.

**Attach a service to each node.** Every replica runs its own copy of the state
machine:

```bash
cargo run -p counter --bin counter-service -- --instance-dir $BASE/0
cargo run -p counter --bin counter-service -- --instance-dir $BASE/1
cargo run -p counter --bin counter-service -- --instance-dir $BASE/2
```

```text
service attached at /home/you/.cache/counter-demo/0
```

**Write to the leader** — substitute whichever node won:

```bash
cargo run -p counter --bin counter-client -- --instance-dir $BASE/0 --add 7 --count 3
```

```text
Add(7) -> value 7 @ position 32
Add(7) -> value 14 @ position 96
Add(7) -> value 21 @ position 160
linearizable read -> 21
```

Each of those returned only after a **majority of nodes had fsync'd** the
command. That is what "committed" means here.

**Now read from a follower**, and watch replication prove itself:

```bash
cargo run -p counter --bin counter-client -- --instance-dir $BASE/1 --read-only --snapshot
```

```text
snapshot read -> 21
```

Node 1 was never written to directly. It has the value because it applied the
same commands, in the same order, from its own copy of the log.

**Beyond one-shot CLI calls.** `counter-client` above uses `uc2_client::Client`
— one blocking call per command, the simplest possible shape. A gateway
process (REST, gRPC) juggling many outstanding requests wants
`uc2_client::PipelinedClient` instead: `submit`/`query_*` hand a typed
command/query to a driver thread and return immediately with a `Ticket`,
which resolves later via a blocking `wait()` or as a `std::future::Future`
you can `.await`:

```rust
use uc2_client::{PipelinedClient, PipelinedConfig};

let client = PipelinedClient::connect(&instance_dir, "my-app", PipelinedConfig::default())?;

// Fire off several requests without waiting on each one individually...
let tickets: Vec<_> = (0..8).map(|n| client.submit::<Command, Applied>(&Command::Add(n))).collect::<Result<_, _>>()?;

// ...then collect the results as they resolve.
for ticket in tickets {
    let applied: Applied = ticket.wait()?;
    println!("-> {applied:?}");
}
```

`Client` is now a thin shim over exactly this machinery (submit, then
immediately `wait()`) — same correctness guarantees, different call shape.
For a gateway chasing maximum single-process throughput (the shape the M5 gate
measures), attach the lower-level `uc2_client::Engine` directly instead —
see its module docs for the `try_submit`/`poll` API.

## 4. Things worth trying

**Point a write at a follower.** It refuses, and tells you who the leader is:

```bash
cargo run -p counter --bin counter-client -- --instance-dir $BASE/1 --read-only
```

```text
Error: linearizable reads are leader-only and this node is a follower (node 0
is the leader). Either point --instance-dir at the leader, or pass --snapshot
to read this replica's own copy of the state.
```

Linearizable reads go through a quorum read barrier, which only a leader can
run — that is what makes them immune to returning stale data from a node that
was deposed a microsecond ago. Snapshot reads skip the barrier, are cheaper, and
may lag slightly.

**Kill the leader.** `Ctrl-C` the leader's terminal and watch a survivor promote
itself within a few hundred milliseconds:

```text
node 1 is now LEADER (term 2)
```

Then write to the new leader and read back — the counter still holds 21. No
acknowledged write was lost. Measured failover on real hardware is p50 202 ms;
see the [M4 gate record](/docs/benchmarks/uc2-m4-gate-2026-07-11.md).

**Restart a node.** Its state survives in `$BASE/<id>` — it rejoins as a
follower, catches up from the log, and its service replays from where it left
off.

## 5. Where to go next

- **[Run a cluster on real hosts](/docs/how-to/run-a-cluster.md)** — the same
  cluster you just ran, moved onto separate machines: per-host configs, the
  network path, where clients live, systemd supervision.
- **[ARCHITECTURE.md](/docs/ARCHITECTURE.md)** — how it works, and why it is
  shaped this way.
- **[VERIFICATION.md](/docs/VERIFICATION.md)** — what is proved, what is checked,
  and what is not.
- **[The operations runbook](/docs/ops/uc2-runbook.md)** — instance directory
  layout, durability requirements, purge, live reconfiguration, wire crypto.
- **[`examples/counter/`](/examples/counter)** — everything you just ran.

### Before running this for real

The counter example is deliberately minimal. A production deployment differs in
ways the runbook covers in full, but at minimum:

- **Instance directories must be on real durable storage**, not tmpfs. The
  archive agent's `fdatasync` is the entire durability story; on a RAM-backed
  filesystem it is a no-op and you have none.
- **Size the log buffer for your throughput.** The example's 4 MiB ring is a toy;
  the default is ~512 MiB. The appender may never overwrite bytes the archive has
  not recorded, so an undersized ring turns into ingress backpressure.
- **Implement `SnapshotStateMachine`** if you want the log purged. Without it the
  journal grows forever.
- **Enable wire crypto** if node-to-node traffic crosses anything you do not
  trust. It is off by default.
