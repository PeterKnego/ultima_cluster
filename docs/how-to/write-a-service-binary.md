# Write a service binary

The node half of `ultima_cluster` ships as a binary you configure. The service
half does not, and cannot: it runs *your* state machine. What ships instead is
this template — the lifecycle every service binary needs, with the two parts
that are easy to leave out and painful to discover missing.

The working copy is
[`examples/counter/src/bin/counter-service.rs`](../../examples/counter/src/bin/counter-service.rs).
Read this page for why it is shaped that way, then copy that file.

## What a service binary is responsible for

A service process attaches to a node's shared memory, polls the committed log
in place, and applies each command to your state machine. It owns the business
logic and nothing else — no consensus, no transport, no durability.

Three obligations follow from that, and only the first is obvious:

1. Implement `StateMachine` — `apply` is sync, deterministic, and does no I/O.
2. Stop on `SIGTERM`, explicitly.
3. Exit when your apply agent dies, rather than staying up looking healthy.

## The template

```rust
fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // The node creates the control page on startup. Tolerate being launched
    // first — under a supervisor you do not control the order.
    let cnc = args.instance_dir.join("cnc2.dat");
    let deadline = Instant::now() + Duration::from_secs(args.wait_secs);
    while !cnc.exists() {
        anyhow::ensure!(Instant::now() < deadline, "no node at {}", args.instance_dir.display());
        std::thread::sleep(Duration::from_millis(20));
    }

    let cfg = ServiceConfig::new(args.instance_dir.clone(), args.app_id);
    let service = ServiceBuilder::new(cfg, MyStateMachine::default()).start()?;

    let stop = Arc::new(AtomicBool::new(false));
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        signal_hook::flag::register(sig, Arc::clone(&stop))?;
    }

    while !stop.load(Ordering::Relaxed) {
        if !service.is_alive() {
            eprintln!("apply agent died; exiting for restart");
            return Err(anyhow::anyhow!("apply agent fail-stopped"));
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    service.stop();
    Ok(())
}
```

Use `signal-hook`, not an async runtime. The service is a polling loop over
shared memory; adding tokio to catch one signal buys nothing.

## Why the signal handler is not optional

Without it, `SIGTERM`'s default disposition kills the process outright and
`Service::stop()` never runs. Everything the service was doing stops mid-step:
the apply agent's thread dies wherever it happened to be, and the node's
control page keeps its attachment recorded until the OS tears the mapping down.

You can see the difference in the exit status. A service killed by the default
disposition reports `unix_wait_status(15)`; one that handled the signal exits
`0`. `examples/counter/tests/lifecycle.rs` asserts exactly that, and it is the
cheapest possible check that your binary got this right — copy it.

Under systemd this is the difference between a clean `systemctl stop` and one
that reports a failed unit every time.

## Why `is_alive` matters more than it looks

The apply agent is a single-writer polling thread, and it is **fail-stop**: when
it hits a contract it cannot honour — an instance mismatch, a log rewind under
its feet — its work closure panics and the thread exits. Deliberately. The
alternative is applying commands against state that no longer corresponds to the
log, which is a correctness violation rather than an outage.

But a panicked apply thread does not stop the *process*. Your `main` is still
alive, still holding its attachment, still looking healthy to whatever is
supervising it. The service simply stops applying anything. Polling `is_alive`
and exiting non-zero is what turns that silent stall into a restart.

A restart is cheap and correct here: the service reattaches, replays from the
journal or installs a snapshot, and catches up. Staying up is what costs you.

## Stopping the two halves together

The node and the service are separate processes sharing an instance directory.
Stop the service first, then the node — the service is the reader, and stopping
the writer out from under it is the log-rewind case above.

Both halves take `SIGTERM`, so a supervisor that stops units in dependency order
gets this right for free. See
[Run a cluster on real hosts](run-a-cluster.md) for the unit files.

## Related

- [Run a cluster on real hosts](run-a-cluster.md) — where these binaries go and
  how they are supervised.
- [Diagnose a node that is not serving](diagnose-a-node.md) — when the service
  is up but nothing is being applied.
- [Architecture](../ARCHITECTURE.md) — why apply is sync and deterministic.
