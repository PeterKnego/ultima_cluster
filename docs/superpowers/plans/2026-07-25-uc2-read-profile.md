# Linearizable-Read Profile Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `uc2_node/examples/read_profile.rs` — a read-throughput measurement harness that decides, by measurement, whether UC's ReadIndex barrier costs read *capacity*.

**Architecture:** An m5_gate-shaped example binary with `node` / `service` / `client` / `all` / `ladder` roles. The measurement is an A/B: the identical pipelined read workload run with and without `FLAG_V2_LINEARIZABLE`, since a snapshot read is the same path minus the barrier. Agent occupancy is read from `/proc/<pid>/task/*/status` (`voluntary_ctxt_switches`, a yield-rate proxy) because the agents' `IdleStrategy::Yield` saturates CPU% by construction. No production code is modified.

**Tech Stack:** Rust 2024 workspace; `clap` (CLI), `hdrhistogram` (latency), `bincode` (payloads), `anyhow`. All dev-dependencies of `uc2_node` already.

**Spec:** `docs/superpowers/specs/2026-07-25-uc2-read-profile-design.md`

## Global Constraints

- **Production code is untouched.** Only `uc2_node/examples/read_profile.rs` (new) and `uc2_node/Cargo.toml` (one `[[example]]` stanza). No edits under any `src/`.
- **Never write test/scratch artifacts to `/tmp`** — it is RAM-backed tmpfs with no swap on this box; an OOM SIGKILLs the largest process. Instance dirs go under `target/`. Every role that takes a directory asserts `!starts_with("/tmp")`.
- **SPDX header on every new file:** `// SPDX-License-Identifier: Apache-2.0` then `// Copyright 2026 Peter Knego`.
- **`cargo clippy --workspace --all-targets -- -D warnings` must pass with zero warnings.**
- **This is an instrument, not a gate.** No PASS/FAIL bar, no nonzero exit on a slow number. The only nonzero exits are for *harness* failures (unresolved reads, non-monotonic reads).
- **Local runs verify WIRING, not performance (spec §3.1).** This box carries a concurrent Veil model-check session (`lean` at ~384% CPU / 7.2 GB RSS, load ~4.2, ~7 GB available). A local run's job is to prove reads resolve, the monotonic guard holds, and teardown is clean — **no local number goes in the report and no local run evaluates the decision rule for the record.** Keep local runs short (a few seconds, few rungs) and use the reduced smoke buffer; the box has no swap and an OOM SIGKILLs the largest process, which could take the neighbouring session down.
- **The AWS fleet run is the measurement** (Task 7). It costs money and requires explicit user approval before `terraform apply`.
- **Decision rule is pre-committed** (spec §2): Rung A is justified iff the linearizable plateau is ≤70% of the snapshot plateau AND `uc2-consensus` or `uc2-receiver` is top-occupancy. Borderline 65–75% ⇒ not justified without a fleet run. The harness prints this evaluation; it never adjusts the threshold.

---

### Task 1: Agent-occupancy sampler

The one piece of pure, fallible logic in the harness: parsing `/proc` thread stats. Written first, TDD, against a synthetic `/proc` tree so it needs no live cluster.

**Files:**
- Create: `uc2_node/examples/read_profile.rs`
- Modify: `uc2_node/Cargo.toml` (add `[[example]]` stanza so the example's unit tests run)
- Test: inline `#[cfg(test)] mod tests` in `uc2_node/examples/read_profile.rs`

**Interfaces:**
- Consumes: nothing (first task).
- Produces:
  - `fn sample_yields(task_dir: &Path) -> std::io::Result<Vec<(String, u64)>>` — one `(thread_name, voluntary_ctxt_switches)` per thread directory under `task_dir`. Threads whose `comm` or `status` is unreadable are skipped, not fatal (threads exit mid-scan).
  - `struct Occupancy { pub name: String, pub yields_per_sec: f64 }`
  - `fn occupancy_delta(before: &[(String, u64)], after: &[(String, u64)], secs: f64) -> Vec<Occupancy>` — joined by name, sorted **ascending** by `yields_per_sec` (fewest yields first = busiest first).

- [ ] **Step 1: Add the Cargo.toml stanza**

Append to `uc2_node/Cargo.toml`:

```toml
# read_profile (leader-lease profile harness) carries unit tests for its
# /proc occupancy parser; `test = true` makes `cargo test` run them.
[[example]]
name = "read_profile"
test = true
```

- [ ] **Step 2: Write the failing tests**

Create `uc2_node/examples/read_profile.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Linearizable-read profile harness. See
//! `docs/superpowers/specs/2026-07-25-uc2-read-profile-design.md`.

use std::path::Path;

fn main() {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Build a synthetic `/proc/<pid>/task` tree: one dir per thread, each
    /// holding a `comm` and a `status` file in the kernel's format.
    fn fake_task_dir(threads: &[(&str, u64)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        for (i, (name, yields)) in threads.iter().enumerate() {
            let t = dir.path().join(format!("{}", 1000 + i));
            fs::create_dir(&t).unwrap();
            fs::write(t.join("comm"), format!("{name}\n")).unwrap();
            fs::write(
                t.join("status"),
                format!(
                    "Name:\t{name}\nThreads:\t1\nvoluntary_ctxt_switches:\t{yields}\n\
                     nonvoluntary_ctxt_switches:\t7\n"
                ),
            )
            .unwrap();
        }
        dir
    }

    #[test]
    fn samples_name_and_yield_count_per_thread() {
        let dir = fake_task_dir(&[("uc2-consensus", 100), ("uc2-sender", 250)]);
        let mut got = sample_yields(dir.path()).expect("sample");
        got.sort();
        assert_eq!(
            got,
            vec![
                ("uc2-consensus".to_string(), 100),
                ("uc2-sender".to_string(), 250)
            ]
        );
    }

    #[test]
    fn skips_threads_missing_files_rather_than_failing() {
        let dir = fake_task_dir(&[("uc2-consensus", 100)]);
        // A thread that exited between readdir and read: dir exists, files don't.
        fs::create_dir(dir.path().join("2000")).unwrap();
        let got = sample_yields(dir.path()).expect("sample");
        assert_eq!(got, vec![("uc2-consensus".to_string(), 100)]);
    }

    #[test]
    fn delta_ranks_busiest_first_and_normalizes_by_time() {
        let before = vec![("uc2-consensus".into(), 100u64), ("uc2-sender".into(), 100)];
        // Over 2 s: consensus yielded 20 times (busy), sender 2000 (idle).
        let after = vec![("uc2-consensus".into(), 120u64), ("uc2-sender".into(), 2100)];
        let got = occupancy_delta(&before, &after, 2.0);
        assert_eq!(got[0].name, "uc2-consensus", "busiest (fewest yields) ranks first");
        assert_eq!(got[0].yields_per_sec, 10.0);
        assert_eq!(got[1].name, "uc2-sender");
        assert_eq!(got[1].yields_per_sec, 1000.0);
    }

    #[test]
    fn delta_ignores_threads_absent_from_either_sample() {
        let before = vec![("uc2-consensus".into(), 100u64), ("gone".into(), 5)];
        let after = vec![("uc2-consensus".into(), 120u64), ("new".into(), 5)];
        let got = occupancy_delta(&before, &after, 1.0);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name, "uc2-consensus");
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p uc2_node --example read_profile`
Expected: FAIL — `cannot find function 'sample_yields' in this scope` (and `occupancy_delta`, `Occupancy`).

- [ ] **Step 4: Implement the sampler**

Insert above `fn main()`:

```rust
/// One agent thread's yield rate over a measurement window.
///
/// **Why yields and not CPU time:** the node's agents idle on
/// `IdleStrategy::Yield` (`uc2_log/src/agent.rs:28` → `std::thread::yield_now()`),
/// so an IDLE agent still burns a core in a yield loop and CPU% is saturated by
/// construction. Each empty duty cycle costs one `sched_yield`, which the kernel
/// counts in `voluntary_ctxt_switches` — so a LOW yield rate means a BUSY agent.
/// This is an ordinal signal (it ranks agents); it is not a duty-cycle percentage.
#[derive(Debug, Clone, PartialEq)]
struct Occupancy {
    name: String,
    yields_per_sec: f64,
}

/// Read `(thread_name, voluntary_ctxt_switches)` for every thread under a
/// `/proc/<pid>/task` directory. Threads that vanish mid-scan (exited between
/// readdir and read) are skipped rather than failing the sample.
fn sample_yields(task_dir: &Path) -> std::io::Result<Vec<(String, u64)>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(task_dir)? {
        let path = entry?.path();
        let Ok(comm) = std::fs::read_to_string(path.join("comm")) else { continue };
        let Ok(status) = std::fs::read_to_string(path.join("status")) else { continue };
        let yields = status
            .lines()
            .find_map(|l| l.strip_prefix("voluntary_ctxt_switches:"))
            .and_then(|v| v.trim().parse::<u64>().ok());
        let Some(yields) = yields else { continue };
        out.push((comm.trim().to_string(), yields));
    }
    Ok(out)
}

/// Join two samples by thread name and rank by yield rate ASCENDING — fewest
/// yields first, i.e. busiest agent first. Threads missing from either sample
/// are dropped (they did not exist for the whole window, so their rate is not
/// comparable).
fn occupancy_delta(
    before: &[(String, u64)],
    after: &[(String, u64)],
    secs: f64,
) -> Vec<Occupancy> {
    let mut out: Vec<Occupancy> = after
        .iter()
        .filter_map(|(name, late)| {
            let (_, early) = before.iter().find(|(n, _)| n == name)?;
            Some(Occupancy {
                name: name.clone(),
                yields_per_sec: late.saturating_sub(*early) as f64 / secs,
            })
        })
        .collect();
    out.sort_by(|a, b| a.yields_per_sec.total_cmp(&b.yields_per_sec));
    out
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p uc2_node --example read_profile`
Expected: PASS, 4 tests.

- [ ] **Step 6: Commit**

```bash
git add uc2_node/examples/read_profile.rs uc2_node/Cargo.toml
git commit -m "test(read-profile): /proc agent-occupancy sampler

Yield-rate proxy (voluntary_ctxt_switches), not CPU time: the agents idle
on IdleStrategy::Yield so CPU% is saturated by construction and carries no
signal. Ordinal only — it ranks agents, which is all the decision rule needs."
```

---

### Task 2: Roles, state machine, and the `all` smoke cluster

Everything needed to stand a cluster up and elect a leader, with no measurement yet. Verified by running it.

**Files:**
- Modify: `uc2_node/examples/read_profile.rs`

**Interfaces:**
- Consumes: nothing from Task 1 (the sampler is used in Task 5).
- Produces:
  - `struct ProfileSm` implementing `uc2_service::StateMachine` with `Command = Vec<u8>`, `Response = u64`, `Query = ()`, `QueryResponse = u64`.
  - `fn node_config(id, members, bind, instance_dir, app_id, admission_bytes) -> NodeConfig`
  - `fn env_cap(var: &str, requested: u64) -> u64`
  - `fn await_single_leader(nodes: &[Node], secs: u64) -> usize`
  - `fn boot_cluster(root: &Path, app_id: &str) -> (Vec<Node>, Vec<Service>, Vec<PathBuf>, usize)` — boots 3 nodes + 3 services, waits for exactly one leader, returns them plus the leader's index. (`Service` = the value returned by `ServiceBuilder::start()`.)
  - CLI enum `Role { Node, Service, Client, All, Ladder }` (`Client`/`Ladder` bodies land in Tasks 3/5).
  - Constants `CNC_FILE`, `QUERY_RING`, `EGRESS_SERVICE`, `EGRESS_NODE`, `INGRESS_RING`, `ALL_APP_ID`.

- [ ] **Step 1: Write the imports, CLI, and constants**

Replace the `use std::path::Path;` line and `fn main() {}` in `uc2_node/examples/read_profile.rs` with:

```rust
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};

use uc2_consensus::election::NodeId;
use uc2_net::fault::FaultConfig;
use uc2_node::{Node, NodeConfig};
use uc2_service::{ServiceBuilder, ServiceConfig, StateMachine};

/// Well-known file names under the instance dir — the shared contract with
/// `uc2_node::InstanceDir` (`uc2_node/src/ipc.rs`). Hardcoded here rather than
/// via `InstanceDir`, which requires the exclusive flock only the owning node
/// may take: this harness is an ATTACHING party, exactly like `uc2_client`.
const CNC_FILE: &str = "cnc2.dat";
const QUERY_RING: &str = "query.ring";
const EGRESS_SERVICE: &str = "egress_service.broadcast";
const EGRESS_NODE: &str = "egress_node.broadcast";

const ALL_APP_ID: &str = "uc2-read-profile-smoke";

const NODE_BUFFER_BYTES: usize = 256 << 20;
/// In-process smoke/ladder buffer. Deliberately far smaller than the fleet's
/// 256 MiB: `all`/`ladder` boot THREE nodes in one process, and this box is
/// shared with a concurrent model-checking session (no swap — an OOM SIGKILLs
/// the largest process). Local runs prove wiring, not throughput, so the hot
/// window does not need to be large.
const SMOKE_BUFFER_BYTES: usize = 32 << 20;
const NODE_MAX_PAYLOAD: usize = 512;
const ELECTION_TIMEOUT_MIN_NS: u64 = 150_000_000;
const ELECTION_TIMEOUT_MAX_NS: u64 = 300_000_000;

#[derive(Parser)]
#[command(
    name = "read_profile",
    about = "UC v2 linearizable-read profile: does the ReadIndex barrier cost read capacity?"
)]
struct Cli {
    #[command(subcommand)]
    role: Role,
}

#[derive(Subcommand)]
enum Role {
    /// Cluster-member node (fleet: one process per host).
    Node(NodeArgs),
    /// State-machine service, attached to a running node.
    Service(ServiceArgs),
    /// The measuring read client (bypasses uc2_client — see `run_read_measurement`).
    Client(ClientArgs),
    /// Local smoke: 3 nodes + 3 services + 1 read client, in-process. NOT a fleet number.
    All(AllArgs),
    /// Local smoke: sweep the concurrency ladder across both arms and both mixes.
    Ladder(LadderArgs),
}

#[derive(clap::Args)]
struct NodeArgs {
    #[arg(long)]
    id: NodeId,
    #[arg(long)]
    bind: SocketAddr,
    #[arg(long)]
    instance_dir: PathBuf,
    /// Comma-separated `id@addr` member list (every member INCLUDING self).
    #[arg(long)]
    members: String,
    #[arg(long, default_value = "uc2-read-profile")]
    app_id: String,
    #[arg(long, default_value_t = 256)]
    admission_kib: u64,
}

#[derive(clap::Args)]
struct ServiceArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "uc2-read-profile")]
    app_id: String,
}

/// Which arm of the A/B to run. The ONLY difference is `FLAG_V2_LINEARIZABLE`
/// on the query record — `node.rs:1956` is the fork.
#[derive(Copy, Clone, Debug, PartialEq, Eq, clap::ValueEnum)]
enum Mode {
    /// Linearizable read: READ_PROBE quorum barrier + frontier wait.
    Lin,
    /// Snapshot read: forwarded straight to the service (`node.rs:1958`).
    Snap,
}

#[derive(clap::Args)]
struct ClientArgs {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "uc2-read-profile")]
    app_id: String,
    #[arg(long, default_value_t = 10)]
    secs: u64,
    /// Concurrent in-flight reads — the ladder axis.
    #[arg(long, default_value_t = 64)]
    readers: u64,
    #[arg(long, value_enum, default_value_t = Mode::Lin)]
    mode: Mode,
    /// Background writes/sec (0 = the read-only arm).
    #[arg(long, default_value_t = 0)]
    write_rate: u64,
    /// PID of the node process, for /proc agent-occupancy sampling. Omit to
    /// skip occupancy (the client cannot see another process's threads).
    #[arg(long)]
    node_pid: Option<u32>,
}

#[derive(clap::Args)]
struct AllArgs {
    #[arg(long, default_value_t = 10)]
    secs: u64,
    #[arg(long, default_value_t = 64)]
    readers: u64,
    #[arg(long, value_enum, default_value_t = Mode::Lin)]
    mode: Mode,
    #[arg(long, default_value_t = 0)]
    write_rate: u64,
    #[arg(long)]
    root: Option<PathBuf>,
}

#[derive(clap::Args)]
struct LadderArgs {
    /// Seconds per rung (each rung is one arm at one concurrency).
    #[arg(long, default_value_t = 6)]
    secs: u64,
    /// Concurrency rungs to sweep.
    #[arg(long, value_delimiter = ',', default_values_t = [1u64, 4, 16, 64, 256, 1024])]
    readers: Vec<u64>,
    /// Background writes/sec for the mixed arm.
    #[arg(long, default_value_t = 20_000)]
    write_rate: u64,
    #[arg(long)]
    root: Option<PathBuf>,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.role {
        Role::Node(a) => run_node(a),
        Role::Service(a) => run_service(a),
        Role::Client(a) => run_client_role(a),
        Role::All(a) => run_all(a),
        Role::Ladder(a) => run_ladder(a),
    }
}
```

- [ ] **Step 2: Write the state machine and the node/service roles**

Append (still above the `#[cfg(test)]` module):

```rust
/// The harness state machine: a counter. `apply` is the cheapest possible
/// deterministic mutation and `query` returns the count, so the measurement
/// isolates the read pipeline rather than any user business logic. The count
/// is monotonically non-decreasing, which is what makes the Task 4 monotonic
/// read guard possible.
#[derive(Default)]
struct ProfileSm {
    count: u64,
    last_applied: Option<u64>,
}

impl StateMachine for ProfileSm {
    type Command = Vec<u8>;
    type Response = u64;
    type Query = ();
    type QueryResponse = u64;

    fn apply(&mut self, position: u64, _cmd: Vec<u8>) -> u64 {
        self.count += 1;
        self.last_applied = Some(position);
        self.count
    }

    fn query(&self, _q: ()) -> u64 {
        self.count
    }

    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

/// Sandbox safety cap (m1–m5 pattern): clip `requested` to the env var when set
/// and nonzero; unset/zero is a no-op (the fleet's mode).
fn env_cap(var: &str, requested: u64) -> u64 {
    match std::env::var(var).ok().and_then(|s| s.parse::<u64>().ok()) {
        Some(cap) if cap > 0 => requested.min(cap),
        _ => requested,
    }
}

/// A distinct, index-derived election seed so each node's randomized timeout
/// differs — a clean boot then elects exactly one leader (m4/m5 precedent).
fn seed_for(id: NodeId) -> u64 {
    0xA1B2_C3D4_5566_7788 ^ (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn parse_members(s: &str) -> Vec<(NodeId, SocketAddr)> {
    s.split(',')
        .map(|part| {
            let (id, addr) = part
                .split_once('@')
                .unwrap_or_else(|| panic!("bad --members entry {part:?}, expected id@addr"));
            let id: NodeId = id.parse().unwrap_or_else(|e| panic!("bad member id {id:?}: {e}"));
            let addr: SocketAddr =
                addr.parse().unwrap_or_else(|e| panic!("bad member addr {addr:?}: {e}"));
            (id, addr)
        })
        .collect()
}

fn node_config(
    id: NodeId,
    members: Vec<(NodeId, SocketAddr)>,
    bind: SocketAddr,
    instance_dir: PathBuf,
    app_id: String,
    admission_bytes: u64,
    buffer_bytes: usize,
) -> NodeConfig {
    NodeConfig {
        id,
        members,
        bind,
        instance_dir,
        app_id,
        buffer_bytes,
        max_payload: NODE_MAX_PAYLOAD,
        admission_bytes,
        election_timeout_min_ns: ELECTION_TIMEOUT_MIN_NS,
        election_timeout_max_ns: ELECTION_TIMEOUT_MAX_NS,
        seed: seed_for(id),
        faults: FaultConfig::default(),
        purge: uc2_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
    }
}

fn run_node(a: NodeArgs) -> anyhow::Result<()> {
    assert!(
        !a.instance_dir.starts_with("/tmp"),
        "node instance_dir must be on a real filesystem (never /tmp — RAM tmpfs, no swap)"
    );
    let id = a.id;
    let members = parse_members(&a.members);
    let cfg = node_config(
        a.id,
        members,
        a.bind,
        a.instance_dir,
        a.app_id,
        a.admission_kib * 1024,
        NODE_BUFFER_BYTES,
    );
    let _node = Node::start(cfg)?;
    println!("read_profile node {id} up (pid {}); parking", std::process::id());
    loop {
        std::thread::park();
    }
}

fn run_service(a: ServiceArgs) -> anyhow::Result<()> {
    let cnc = a.instance_dir.join(CNC_FILE);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !cnc.exists() {
        anyhow::ensure!(Instant::now() < deadline, "timed out waiting for cnc2.dat at {cnc:?}");
        std::thread::sleep(Duration::from_millis(20));
    }
    let cfg = ServiceConfig::new(a.instance_dir, a.app_id);
    let _svc = ServiceBuilder::new(cfg, ProfileSm::default()).start()?;
    println!("read_profile service up; parking");
    loop {
        std::thread::park();
    }
}
```

Note the node role prints its **pid** — the fleet orchestrator passes it to the client as `--node-pid` for occupancy sampling.

- [ ] **Step 3: Write the cluster boot helper and a temporary `all` body**

```rust
/// Wait for EXACTLY one serving leader; assert no split-brain throughout
/// (m4/m5/lincheck_v2 precedent). Returns the leader's index.
fn await_single_leader(nodes: &[Node], secs: u64) -> usize {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let serving: Vec<usize> =
            (0..nodes.len()).filter(|&i| nodes[i].can_serve() && nodes[i].is_leader()).collect();
        assert!(serving.len() <= 1, "split-brain in smoke cluster: nodes {serving:?} all serve");
        if serving.len() == 1 {
            return serving[0];
        }
        assert!(Instant::now() < deadline, "no leader elected within {secs}s");
        std::thread::yield_now();
    }
}

/// Boot a 3-node in-process cluster with a service per node, elect a leader.
/// Returns `(nodes, services, instance_dirs, leader_index)`.
#[allow(clippy::type_complexity)]
fn boot_cluster(
    root: &Path,
    app_id: &str,
) -> anyhow::Result<(Vec<Node>, Vec<uc2_service::Service<ProfileSm>>, Vec<PathBuf>, usize)> {
    assert!(
        !root.starts_with("/tmp"),
        "root must be on a real filesystem (never /tmp — RAM tmpfs, no swap); got {root:?}"
    );
    let _ = std::fs::remove_dir_all(root);
    std::fs::create_dir_all(root)?;

    const N: usize = 3;
    let socks: Vec<UdpSocket> =
        (0..N).map(|_| UdpSocket::bind("127.0.0.1:0").expect("bind")).collect();
    let members: Vec<(NodeId, SocketAddr)> =
        socks.iter().enumerate().map(|(i, s)| (i as NodeId, s.local_addr().unwrap())).collect();

    let mut nodes = Vec::with_capacity(N);
    let mut services = Vec::with_capacity(N);
    let mut dirs = Vec::with_capacity(N);
    for (i, sock) in socks.into_iter().enumerate() {
        let addr = members[i].1;
        let instance_dir = root.join(format!("n{i}"));
        let cfg = node_config(
            i as NodeId,
            members.clone(),
            addr,
            instance_dir.clone(),
            app_id.into(),
            256 * 1024,
            SMOKE_BUFFER_BYTES,
        );
        let node = Node::start_with_socket(cfg, sock).expect("node start");
        let svc =
            ServiceBuilder::new(ServiceConfig::new(&instance_dir, app_id), ProfileSm::default())
                .start()
                .expect("service start");
        nodes.push(node);
        services.push(svc);
        dirs.push(instance_dir);
    }
    let leader = await_single_leader(&nodes, 30);
    Ok((nodes, services, dirs, leader))
}

/// Node-first-then-service teardown, per the v1/lincheck_v2 precedent: a node's
/// shutdown must not wait on a service that tore down first.
fn stop_cluster(nodes: Vec<Node>, services: Vec<uc2_service::Service<ProfileSm>>) {
    for node in nodes {
        node.stop();
    }
    for svc in services {
        svc.stop();
    }
}

fn run_all(a: AllArgs) -> anyhow::Result<()> {
    let root = a.root.unwrap_or_else(|| PathBuf::from("target/read_profile_smoke"));
    let (nodes, services, dirs, leader) = boot_cluster(&root, ALL_APP_ID)?;
    println!("leader elected: n{leader} at {:?}", dirs[leader]);
    let _ = (a.secs, a.readers, a.mode, a.write_rate); // measurement lands in Task 3
    stop_cluster(nodes, services);
    Ok(())
}

fn run_client_role(_a: ClientArgs) -> anyhow::Result<()> {
    anyhow::bail!("client role lands in Task 3")
}

fn run_ladder(_a: LadderArgs) -> anyhow::Result<()> {
    anyhow::bail!("ladder role lands in Task 5")
}
```

(`ServiceBuilder::start()` returns `Result<Service<S>, ServiceError>` — `uc2_service/src/lib.rs:89,250` — so `uc2_service::Service<ProfileSm>` is the correct type in both signatures.)

- [ ] **Step 4: Verify it builds and boots a leader**

Run: `cargo build -p uc2_node --example read_profile`
Expected: builds clean.

Run: `cargo run -p uc2_node --release --example read_profile -- all --secs 1`
Expected: prints `leader elected: n<i> at "target/read_profile_smoke/n<i>"` and exits 0 within ~30 s.

- [ ] **Step 5: Verify the Task 1 tests still pass**

Run: `cargo test -p uc2_node --example read_profile`
Expected: PASS, 4 tests.

- [ ] **Step 6: Commit**

```bash
git add uc2_node/examples/read_profile.rs
git commit -m "feat(read-profile): roles, ProfileSm, and the in-process smoke cluster

node/service/all roles over the real SDK stack, m5_gate-shaped. The node role
prints its pid so the fleet orchestrator can pass --node-pid for occupancy
sampling. No measurement yet."
```

---

### Task 3: The pipelined read client (the A/B core)

The measurement itself. Reads are issued straight into `query.ring` with the harness's own `local_seq`, because `uc2_client::query_linearizable` blocks per call (`uc2_client/src/client.rs:154-184`) and read concurrency is the ladder axis.

**Files:**
- Modify: `uc2_node/examples/read_profile.rs`

**Interfaces:**
- Consumes: `ProfileSm`, `Mode`, `env_cap`, `boot_cluster`, `stop_cluster`, the file-name constants (Task 2).
- Produces:
  - `struct ReadStats { pub reads: u64, pub retried: u64, pub not_leader: u64, pub duplicates: u64, pub overwritten: u64, pub inflight_at_end: u64, pub elapsed: Duration, pub reads_per_sec: f64, pub p50_ms: f64, pub p99_ms: f64, pub max_read_value: u64 }`
  - `fn run_read_measurement(instance_dir: &Path, app_id: &str, secs: u64, readers: u64, mode: Mode, write_rate: u64, node_task_dir: Option<PathBuf>) -> (ReadStats, Vec<Occupancy>)`

- [ ] **Step 1: Add the imports this task needs**

Add to the import block:

```rust
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use hdrhistogram::Histogram;
use uc2_log::cnc::CncPage;
use uc_protocol::ring::{BroadcastConsumer, BroadcastRing, MpscRing, RingError};
use uc_protocol::v2::cnc::NODE_FLAG_CAN_SERVE;
use uc_protocol::v2::ipc::{
    FLAG_V2_LINEARIZABLE, MSG_V2_NOT_LEADER, MSG_V2_QUERY, MSG_V2_RESPONSE, MSG_V2_RETRY,
    client_from_extra, extra_client,
};
```

(`MSG_V2_SUBMIT` and the `INGRESS_RING` constant are deliberately NOT added here — nothing writes to the ingress ring until Task 4's writer, and adding them now would build with unused-import / dead-code warnings.)

And these constants:

```rust
/// Slot-array size for the correlation tables — vastly larger than any
/// realistic in-flight window, so a slot is never reused while its previous
/// occupant is outstanding.
const SLOTS: usize = 1 << 20;
const SLOT_MASK: usize = SLOTS - 1;
const HIST_MAX_NS: u64 = 60_000_000_000;
const DRAIN_GRACE: Duration = Duration::from_secs(5);
const LEADER_WAIT: Duration = Duration::from_secs(30);
```

- [ ] **Step 2: Write the matcher**

```rust
struct ReadStats {
    reads: u64,
    retried: u64,
    not_leader: u64,
    duplicates: u64,
    overwritten: u64,
    inflight_at_end: u64,
    elapsed: Duration,
    reads_per_sec: f64,
    p50_ms: f64,
    p99_ms: f64,
    /// Highest counter value any read returned — fed to the Task 4 monotonic guard.
    max_read_value: u64,
}

struct MatcherCtx {
    send_ns: Arc<Box<[AtomicU64]>>,
    owner: Arc<Box<[AtomicU64]>>,
    resolved: Arc<AtomicU64>,
    reads: Arc<AtomicU64>,
    not_leader: Arc<AtomicU64>,
    retried: Arc<AtomicU64>,
    duplicates: Arc<AtomicU64>,
    overwritten: Arc<AtomicU64>,
    last_response_ns: Arc<AtomicU64>,
    max_read_value: Arc<AtomicU64>,
    hist: Arc<Mutex<Histogram<u64>>>,
    client_id: u32,
    t0: Instant,
}

/// Decode a query answer's payload: `0u64 LE placeholder ++ bincode(u64)`
/// (`uc2_service/src/egress.rs:62-66`). Returns None for a write response,
/// whose payload is the applied position + the write's own response.
fn decode_query_answer(payload: &[u8]) -> Option<u64> {
    let rest = payload.get(8..)?;
    bincode::serde::decode_from_slice::<u64, _>(rest, bincode::config::standard())
        .ok()
        .map(|(v, _)| v)
}

/// One duty cycle of the matcher: drain one record and resolve it if it is
/// addressed to this client.
///
/// Duplicate tolerance is the m5_gate contract verbatim: `owner[idx]` holds
/// `local_seq + 1` while outstanding and is cleared by whichever delivery wins
/// the CAS, so a service replay that re-publishes a historical response is
/// counted as a duplicate rather than double-timed.
fn poll_egress(ring: &mut BroadcastConsumer, ctx: &MatcherCtx, buf: &mut Vec<u8>) -> bool {
    match ring.try_read(buf) {
        Ok(Some(rec)) => {
            let (cid, local_seq) = client_from_extra(rec.header_extra);
            if cid != ctx.client_id {
                return true; // addressed to another client
            }
            let idx = (local_seq as usize) & SLOT_MASK;
            let expected = local_seq as u64 + 1;
            let claimed = ctx.owner[idx]
                .compare_exchange(expected, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok();
            match rec.msg_type {
                MSG_V2_RESPONSE if claimed => {
                    let now = ctx.t0.elapsed().as_nanos() as u64;
                    let send = ctx.send_ns[idx].load(Ordering::Acquire);
                    let lat = now.saturating_sub(send).min(HIST_MAX_NS);
                    let _ = ctx.hist.lock().unwrap().record(lat);
                    if let Some(v) = decode_query_answer(buf) {
                        ctx.max_read_value.fetch_max(v, Ordering::Relaxed);
                    }
                    ctx.reads.fetch_add(1, Ordering::Relaxed);
                    ctx.resolved.fetch_add(1, Ordering::Relaxed);
                    ctx.last_response_ns.fetch_max(now, Ordering::Relaxed);
                }
                MSG_V2_RESPONSE => {
                    ctx.duplicates.fetch_add(1, Ordering::Relaxed);
                }
                MSG_V2_NOT_LEADER if claimed => {
                    ctx.not_leader.fetch_add(1, Ordering::Relaxed);
                    ctx.resolved.fetch_add(1, Ordering::Relaxed);
                }
                MSG_V2_RETRY if claimed => {
                    ctx.retried.fetch_add(1, Ordering::Relaxed);
                    ctx.resolved.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
            true
        }
        Ok(None) => false,
        Err(RingError::Overwritten) => {
            ctx.overwritten.fetch_add(1, Ordering::Relaxed);
            true
        }
        Err(_) => true,
    }
}
```

**Counting note:** `reads` counts only `MSG_V2_RESPONSE`. In the mixed arm the write responses land on the same egress rings, but they are addressed to the *writer's* `client_id` (Task 4 gives the writer its own), so they are filtered out at the `cid != ctx.client_id` check and never inflate the read rate.

- [ ] **Step 3: Write the measurement core**

```rust
/// The measuring read client. Issues `--readers` concurrent reads, pipelined
/// through `query.ring` with this harness's own `local_seq`, and correlates
/// answers off both egress broadcasts.
///
/// **The A/B:** `mode` sets exactly one bit — `FLAG_V2_LINEARIZABLE`. With it
/// set the read takes the nonce + READ_PROBE + AwaitQuorum path; clear, the node
/// forwards it straight to the service (`node.rs:1956-1958`). Everything else —
/// admission, the per-cycle drain cap, the service, the egress path — is
/// identical, so the delta between arms IS the barrier's end-to-end cost.
fn run_read_measurement(
    instance_dir: &Path,
    app_id: &str,
    secs: u64,
    readers: u64,
    mode: Mode,
    write_rate: u64,
    node_task_dir: Option<PathBuf>,
) -> (ReadStats, Vec<Occupancy>) {
    let cnc = CncPage::open_file(&instance_dir.join(CNC_FILE), app_id)
        .unwrap_or_else(|e| panic!("cnc attach {instance_dir:?}: {e}"));
    let client_id = cnc.status().next_client_id.fetch_add(1) as u32;
    await_serving(&cnc, LEADER_WAIT);

    let (query_producer, _query_consumer) = MpscRing::open(&instance_dir.join(QUERY_RING))
        .unwrap_or_else(|e| panic!("open query.ring: {e}"))
        .into_split();
    let mut egress_service = BroadcastRing::open(&instance_dir.join(EGRESS_SERVICE))
        .unwrap_or_else(|e| panic!("open egress_service.broadcast: {e}"))
        .subscribe();
    let mut egress_node = BroadcastRing::open(&instance_dir.join(EGRESS_NODE))
        .unwrap_or_else(|e| panic!("open egress_node.broadcast: {e}"))
        .subscribe();

    // `ProfileSm::Query = ()`, so the query payload is bincode's encoding of
    // the unit type — encoded once and reused, keeping the send loop allocation-free.
    let query_bytes = bincode::serde::encode_to_vec((), bincode::config::standard())
        .expect("encode unit query");
    let flags = match mode {
        Mode::Lin => FLAG_V2_LINEARIZABLE,
        Mode::Snap => 0,
    };

    let send_ns: Arc<Box<[AtomicU64]>> =
        Arc::new((0..SLOTS).map(|_| AtomicU64::new(0)).collect::<Vec<_>>().into_boxed_slice());
    let owner: Arc<Box<[AtomicU64]>> =
        Arc::new((0..SLOTS).map(|_| AtomicU64::new(0)).collect::<Vec<_>>().into_boxed_slice());
    let sent = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let ctx = MatcherCtx {
        send_ns: Arc::clone(&send_ns),
        owner: Arc::clone(&owner),
        resolved: Arc::new(AtomicU64::new(0)),
        reads: Arc::new(AtomicU64::new(0)),
        not_leader: Arc::new(AtomicU64::new(0)),
        retried: Arc::new(AtomicU64::new(0)),
        duplicates: Arc::new(AtomicU64::new(0)),
        overwritten: Arc::new(AtomicU64::new(0)),
        last_response_ns: Arc::new(AtomicU64::new(0)),
        max_read_value: Arc::new(AtomicU64::new(0)),
        hist: Arc::new(Mutex::new(
            Histogram::new_with_bounds(1, HIST_MAX_NS, 3).expect("histogram"),
        )),
        client_id,
        t0: Instant::now(),
    };
    let t0 = ctx.t0;
    let resolved = Arc::clone(&ctx.resolved);
    let reads = Arc::clone(&ctx.reads);
    let not_leader = Arc::clone(&ctx.not_leader);
    let retried = Arc::clone(&ctx.retried);
    let duplicates = Arc::clone(&ctx.duplicates);
    let overwritten = Arc::clone(&ctx.overwritten);
    let last_response_ns = Arc::clone(&ctx.last_response_ns);
    let max_read_value = Arc::clone(&ctx.max_read_value);
    let hist = Arc::clone(&ctx.hist);

    let matcher = {
        let stop = Arc::clone(&stop);
        thread::Builder::new()
            .name("rp-matcher".into())
            .spawn(move || {
                let ctx = ctx;
                let mut buf = Vec::new();
                loop {
                    let mut did = false;
                    did |= poll_egress(&mut egress_service, &ctx, &mut buf);
                    did |= poll_egress(&mut egress_node, &ctx, &mut buf);
                    if !did {
                        if stop.load(Ordering::Relaxed) {
                            break;
                        }
                        thread::sleep(Duration::from_micros(20));
                    }
                }
            })
            .expect("spawn matcher thread")
    };

    let writer = spawn_writer(instance_dir, app_id, write_rate, Arc::clone(&stop));

    // Sample agent occupancy across the measurement window only (after warm-up
    // attach, before drain) so boot-time yields do not pollute the rate.
    let occ_before = node_task_dir.as_ref().and_then(|d| sample_yields(d).ok());
    let occ_t0 = Instant::now();

    // Send loop: keep `readers` reads in flight. `RingError::Full` means
    // yield+retry, exactly like the real uc2_client.
    let mut local_seq: u32 = 0;
    let deadline = t0 + Duration::from_secs(secs);
    'send: while Instant::now() < deadline {
        // Pause while the attached node is not a serving leader: without this a
        // leadership flip degenerates into a NOT_LEADER feedback flood that
        // measures nothing (the m5_gate lesson).
        if cnc.status().flags.load_acquire() & NODE_FLAG_CAN_SERVE == 0 {
            thread::sleep(Duration::from_millis(1));
            continue;
        }
        while sent.load(Ordering::Relaxed).wrapping_sub(resolved.load(Ordering::Relaxed))
            >= readers
        {
            if Instant::now() >= deadline {
                break 'send;
            }
            thread::yield_now();
        }
        let idx = (local_seq as usize) & SLOT_MASK;
        send_ns[idx].store(t0.elapsed().as_nanos() as u64, Ordering::Release);
        owner[idx].store(local_seq as u64 + 1, Ordering::Release);
        let extra = extra_client(client_id, local_seq);
        loop {
            match query_producer.try_write(MSG_V2_QUERY, flags, extra, &query_bytes) {
                Ok(()) => break,
                Err(RingError::Full) => thread::yield_now(),
                Err(e) => panic!("query.ring write error: {e}"),
            }
        }
        sent.fetch_add(1, Ordering::Relaxed);
        local_seq = local_seq.wrapping_add(1);
    }
    let send_window_end_ns = t0.elapsed().as_nanos() as u64;
    let occ_secs = occ_t0.elapsed().as_secs_f64();
    let occ_after = node_task_dir.as_ref().and_then(|d| sample_yields(d).ok());

    let drain_deadline = Instant::now() + DRAIN_GRACE;
    while resolved.load(Ordering::Relaxed) < sent.load(Ordering::Relaxed)
        && Instant::now() < drain_deadline
    {
        thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Relaxed);
    matcher.join().expect("matcher thread panicked");
    if let Some(w) = writer {
        w.join().expect("writer thread panicked");
    }

    let sends = sent.load(Ordering::Relaxed);
    let resolved_n = resolved.load(Ordering::Relaxed);
    // Drain-inclusive clock floored at the send window's end: a run whose
    // responses stop arriving mid-window must not excise its dead tail from the
    // denominator (the m5_gate lesson).
    let elapsed =
        Duration::from_nanos(last_response_ns.load(Ordering::Relaxed).max(send_window_end_ns));
    let n_reads = reads.load(Ordering::Relaxed);
    let reads_per_sec =
        if elapsed.as_secs_f64() > 0.0 { n_reads as f64 / elapsed.as_secs_f64() } else { 0.0 };
    let (p50_ms, p99_ms) = {
        let h = hist.lock().unwrap();
        let ms = |ns: u64| ns as f64 / 1e6;
        (ms(h.value_at_quantile(0.50)), ms(h.value_at_quantile(0.99)))
    };

    let occupancy = match (occ_before, occ_after) {
        (Some(b), Some(a)) => occupancy_delta(&b, &a, occ_secs.max(1e-9)),
        _ => Vec::new(),
    };

    (
        ReadStats {
            reads: n_reads,
            retried: retried.load(Ordering::Relaxed),
            not_leader: not_leader.load(Ordering::Relaxed),
            duplicates: duplicates.load(Ordering::Relaxed),
            overwritten: overwritten.load(Ordering::Relaxed),
            inflight_at_end: sends.saturating_sub(resolved_n),
            elapsed,
            reads_per_sec,
            p50_ms,
            p99_ms,
            max_read_value: max_read_value.load(Ordering::Relaxed),
        },
        occupancy,
    )
}

fn await_serving(cnc: &CncPage, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if cnc.status().flags.load_acquire() & NODE_FLAG_CAN_SERVE != 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "no serving leader at this instance_dir within {timeout:?}"
        );
        thread::sleep(Duration::from_millis(20));
    }
}
```

- [ ] **Step 4: Add a no-op writer stub so this task compiles standalone**

The real writer lands in Task 4; this keeps Task 3 independently runnable.

```rust
/// Background write load. Task 3 ships the no-op (`write_rate` is ignored);
/// Task 4 replaces the body with the real paced writer.
fn spawn_writer(
    _instance_dir: &Path,
    _app_id: &str,
    _write_rate: u64,
    _stop: Arc<AtomicBool>,
) -> Option<thread::JoinHandle<()>> {
    None
}
```

- [ ] **Step 5: Wire the `client` and `all` roles to the measurement**

```rust
fn print_read_report(mode: Mode, readers: u64, write_rate: u64, s: &ReadStats, occ: &[Occupancy]) {
    let arm = match mode {
        Mode::Lin => "linearizable (probe barrier)",
        Mode::Snap => "snapshot (no barrier)",
    };
    println!();
    println!("========== uc2 read profile: {arm} ==========");
    println!("readers (in-flight)   : {readers}");
    println!("background writes/s   : {write_rate}");
    println!("reads resolved        : {}", s.reads);
    println!("retries               : {}", s.retried);
    println!("not_leader redirects  : {}", s.not_leader);
    println!("dup answers dropped   : {}", s.duplicates);
    println!("broadcast overwritten : {}", s.overwritten);
    println!("in-flight at end      : {}", s.inflight_at_end);
    println!("elapsed (drain-incl.) : {:.3} s", s.elapsed.as_secs_f64());
    println!("reads/s               : {:.0}", s.reads_per_sec);
    println!("p50                   : {:.3} ms", s.p50_ms);
    println!("p99                   : {:.3} ms", s.p99_ms);
    if occ.is_empty() {
        println!("agent occupancy       : (not sampled — pass --node-pid)");
    } else {
        println!("agent occupancy (busiest first, fewer yields = busier):");
        for o in occ {
            println!("    {:<22} {:>12.0} yields/s", o.name, o.yields_per_sec);
        }
    }
    println!("=================================================================");
}

fn run_client_role(a: ClientArgs) -> anyhow::Result<()> {
    let secs = env_cap("UC2_RP_MAX_SECS", a.secs);
    let readers = env_cap("UC2_RP_MAX_READERS", a.readers);
    let task_dir = a.node_pid.map(|p| PathBuf::from(format!("/proc/{p}/task")));
    let (stats, occ) = run_read_measurement(
        &a.instance_dir,
        &a.app_id,
        secs,
        readers,
        a.mode,
        a.write_rate,
        task_dir,
    );
    print_read_report(a.mode, readers, a.write_rate, &stats, &occ);
    anyhow::ensure!(
        stats.inflight_at_end == 0,
        "{} reads never resolved — the run did not complete; its numbers describe nothing",
        stats.inflight_at_end
    );
    Ok(())
}
```

And replace `run_all`'s placeholder line with:

```rust
    let secs = env_cap("UC2_RP_MAX_SECS", a.secs);
    let readers = env_cap("UC2_RP_MAX_READERS", a.readers);
    println!("*** LOCAL SMOKE — NOT a fleet number *** (3 nodes + 3 services + client, one box)");
    let (stats, occ) = run_read_measurement(
        &dirs[leader],
        ALL_APP_ID,
        secs,
        readers,
        a.mode,
        a.write_rate,
        Some(PathBuf::from("/proc/self/task")),
    );
    print_read_report(a.mode, readers, a.write_rate, &stats, &occ);
    stop_cluster(nodes, services);
    anyhow::ensure!(stats.inflight_at_end == 0, "{} reads never resolved", stats.inflight_at_end);
    return Ok(());
```

(Delete the now-dead `let _ = (a.secs, ...)` line and the old `stop_cluster` call.)

- [ ] **Step 6: Run both arms and verify they measure**

Run: `cargo run -p uc2_node --release --example read_profile -- all --secs 5 --readers 64 --mode snap`
Expected: nonzero `reads/s`, `in-flight at end : 0`, an occupancy list naming `uc2-consensus` / `uc2-apply` / etc., exit 0.

Run: `cargo run -p uc2_node --release --example read_profile -- all --secs 5 --readers 64 --mode lin`
Expected: same shape, exit 0. The two `reads/s` numbers are the first A/B data point.

- [ ] **Step 7: Commit**

```bash
git add uc2_node/examples/read_profile.rs
git commit -m "feat(read-profile): pipelined read client and the lin/snap A/B

Reads go straight into query.ring with the harness's own local_seq —
uc2_client::query_linearizable blocks per call and read concurrency is the
ladder axis. Mode sets exactly one bit (FLAG_V2_LINEARIZABLE), so the delta
between arms is the barrier's end-to-end cost and nothing else."
```

---

### Task 4: Background write load and the monotonic-read guard

The mixed arm, plus the correctness tooth that stops a mis-wired harness from reporting a flattering number.

**Files:**
- Modify: `uc2_node/examples/read_profile.rs`

**Interfaces:**
- Consumes: `spawn_writer` (stub from Task 3), `ReadStats`, `run_read_measurement`.
- Produces: real `spawn_writer` with the same signature; `ReadStats.max_read_value` becomes meaningful for the guard.

- [ ] **Step 1: Add the ingress import and constant, then replace the writer stub**

Add to the import block and the constant block respectively (deferred from Tasks 2–3 so those tasks build warning-free):

```rust
use uc_protocol::v2::ipc::MSG_V2_SUBMIT;
```

```rust
const INGRESS_RING: &str = "ingress.ring";
```

Then replace the stub with the paced writer:

```rust
/// Background write load for the mixed arm: a paced submitter on its OWN
/// `client_id`, so its responses are filtered out of the read matcher at the
/// `cid != ctx.client_id` check and cannot inflate the read rate.
///
/// Returns `None` when `write_rate == 0` (the read-only arm), which is what
/// makes that arm the clean isolation: with no writes in flight,
/// `service_applied >= commit_at` already holds when a read is admitted, so the
/// frontier wait is free and the A/B delta is the barrier alone.
fn spawn_writer(
    instance_dir: &Path,
    app_id: &str,
    write_rate: u64,
    stop: Arc<AtomicBool>,
) -> Option<thread::JoinHandle<()>> {
    if write_rate == 0 {
        return None;
    }
    let dir = instance_dir.to_path_buf();
    let app_id = app_id.to_string();
    Some(
        thread::Builder::new()
            .name("rp-writer".into())
            .spawn(move || {
                let cnc = CncPage::open_file(&dir.join(CNC_FILE), &app_id)
                    .unwrap_or_else(|e| panic!("writer cnc attach: {e}"));
                let client_id = cnc.status().next_client_id.fetch_add(1) as u32;
                let (ingress, _c) = MpscRing::open(&dir.join(INGRESS_RING))
                    .unwrap_or_else(|e| panic!("writer open ingress.ring: {e}"))
                    .into_split();
                let payload = bincode::serde::encode_to_vec(
                    &vec![0xABu8; 64],
                    bincode::config::standard(),
                )
                .expect("encode write payload");
                let period = Duration::from_nanos(1_000_000_000 / write_rate.max(1));
                let mut local_seq: u32 = 0;
                let mut next = Instant::now();
                while !stop.load(Ordering::Relaxed) {
                    let now = Instant::now();
                    if now < next {
                        thread::sleep((next - now).min(Duration::from_millis(1)));
                        continue;
                    }
                    next += period;
                    if cnc.status().flags.load_acquire() & NODE_FLAG_CAN_SERVE == 0 {
                        continue;
                    }
                    let extra = extra_client(client_id, local_seq);
                    match ingress.try_write(MSG_V2_SUBMIT, 0, extra, &payload) {
                        Ok(()) | Err(RingError::Full) => {}
                        Err(e) => panic!("writer ingress.ring error: {e}"),
                    }
                    local_seq = local_seq.wrapping_add(1);
                }
            })
            .expect("spawn writer thread"),
    )
}
```

The writer never waits for its own responses — it is load, not a measurement — so it needs no correlation table.

- [ ] **Step 2: Add the monotonic-read guard to the matcher**

In `MatcherCtx` add:

```rust
    /// Highest value returned by any read SO FAR, used to detect a REGRESSION.
    /// `ProfileSm::query` returns a monotonically non-decreasing counter, so a
    /// linearizable read that returns less than a previously-returned value is a
    /// stale answer — a harness or read-path defect either way. Snapshot reads
    /// are NOT guarded: a snapshot read is served from local applied state with
    /// no barrier, so it may legitimately regress on a follower.
    guard_monotonic: bool,
    regression: Arc<AtomicU64>,
```

In `poll_egress`'s `MSG_V2_RESPONSE if claimed` arm, replace the `max_read_value` line with:

```rust
                    if let Some(v) = decode_query_answer(buf) {
                        let prev = ctx.max_read_value.fetch_max(v, Ordering::Relaxed);
                        if ctx.guard_monotonic && v < prev {
                            ctx.regression.fetch_max(prev - v, Ordering::Relaxed);
                        }
                    }
```

In `run_read_measurement`, set `guard_monotonic: mode == Mode::Lin` and a fresh `regression: Arc::new(AtomicU64::new(0))`, clone it out alongside the other counters, and add `regression: u64` to `ReadStats`, populated from it.

- [ ] **Step 3: Fail loudly on a regression**

In `run_client_role` and `run_all`, before the existing `inflight_at_end` check:

```rust
    anyhow::ensure!(
        stats.regression == 0,
        "LINEARIZABLE READ REGRESSED by {} — a read returned a value lower than one \
         already returned. Either the harness is mis-wired or the read path is serving \
         stale state; the throughput numbers above are meaningless either way.",
        stats.regression
    );
```

Add `println!("read regression      : {}", s.regression);` to `print_read_report`.

- [ ] **Step 4: Verify the mixed arm runs clean**

Run: `cargo run -p uc2_node --release --example read_profile -- all --secs 5 --readers 64 --mode lin --write-rate 20000`
Expected: nonzero reads/s, `read regression : 0`, `in-flight at end : 0`, exit 0. `max_read_value` should be well above zero (writes are landing).

Run: `cargo run -p uc2_node --release --example read_profile -- all --secs 5 --readers 64 --mode snap --write-rate 20000`
Expected: same, exit 0.

- [ ] **Step 5: Verify the guard actually has teeth**

Temporarily change `guard_monotonic: mode == Mode::Lin` to `guard_monotonic: true` and re-run the **snapshot** mixed arm above. Snapshot reads on a leader may still be monotonic, so this may or may not trip — if it does not, instead confirm the guard's wiring by temporarily inverting the comparison to `v > prev` and re-running the linearizable arm, which MUST then fail with the regression message.

Revert the temporary change before continuing. Expected end state: the guard fires when it should and the file is back to `mode == Mode::Lin`.

- [ ] **Step 6: Commit**

```bash
git add uc2_node/examples/read_profile.rs
git commit -m "feat(read-profile): background write load and monotonic-read guard

The writer runs on its own client_id so its responses cannot inflate the read
rate. The guard applies to the linearizable arm only — a snapshot read is
served from local applied state and may legitimately regress. A mis-wired
harness now fails loudly instead of reporting a flattering number."
```

---

### Task 5: The ladder and the decision-rule evaluation

Sweeps concurrency across both arms and both mixes, then evaluates the pre-committed rule.

**Files:**
- Modify: `uc2_node/examples/read_profile.rs`

**Interfaces:**
- Consumes: `run_read_measurement`, `ReadStats`, `Occupancy`, `boot_cluster`, `stop_cluster`.
- Produces:
  - `struct Rung { pub readers: u64, pub mode: Mode, pub write_rate: u64, pub reads_per_sec: f64, pub p50_ms: f64, pub p99_ms: f64, pub top_agent: Option<String> }`
  - `fn evaluate_decision_rule(rungs: &[Rung], write_rate: u64) -> String` — the verdict block, one per mix.

- [ ] **Step 1: Write the failing test for the decision rule**

Add to the `#[cfg(test)] mod tests` block:

```rust
    fn rung(readers: u64, mode: Mode, rps: f64, top: &str) -> Rung {
        Rung {
            readers,
            mode,
            write_rate: 0,
            reads_per_sec: rps,
            p50_ms: 0.1,
            p99_ms: 0.2,
            top_agent: Some(top.to_string()),
        }
    }

    #[test]
    fn rule_justifies_rung_a_when_gap_and_consensus_is_top() {
        let rungs = vec![
            rung(64, Mode::Snap, 1_000_000.0, "uc2-apply"),
            rung(64, Mode::Lin, 500_000.0, "uc2-consensus"), // 50% of snapshot
        ];
        let out = evaluate_decision_rule(&rungs, 0);
        // "NOT JUSTIFIED" also contains "JUSTIFIED", so assert both directions.
        assert!(out.contains("Rung A JUSTIFIED"), "got: {out}");
        assert!(!out.contains("NOT JUSTIFIED"), "got: {out}");
    }

    #[test]
    fn rule_declines_when_ratio_is_above_the_band() {
        let rungs = vec![
            rung(64, Mode::Snap, 1_000_000.0, "uc2-apply"),
            rung(64, Mode::Lin, 900_000.0, "uc2-consensus"), // 90%
        ];
        let out = evaluate_decision_rule(&rungs, 0);
        assert!(out.contains("NOT JUSTIFIED"), "got: {out}");
    }

    #[test]
    fn rule_declines_in_the_borderline_band_even_with_the_right_top_agent() {
        let rungs = vec![
            rung(64, Mode::Snap, 1_000_000.0, "uc2-apply"),
            rung(64, Mode::Lin, 700_000.0, "uc2-consensus"), // 70% — inside 65..=75
        ];
        let out = evaluate_decision_rule(&rungs, 0);
        assert!(out.contains("BORDERLINE"), "got: {out}");
        assert!(
            !out.contains("Rung A JUSTIFIED"),
            "borderline must not read as justified: {out}"
        );
    }

    #[test]
    fn rule_declines_when_a_non_probe_agent_is_top() {
        let rungs = vec![
            rung(64, Mode::Snap, 1_000_000.0, "uc2-apply"),
            rung(64, Mode::Lin, 400_000.0, "uc2-apply"), // big gap, wrong bottleneck
        ];
        let out = evaluate_decision_rule(&rungs, 0);
        assert!(out.contains("NOT JUSTIFIED"), "got: {out}");
        assert!(out.contains("uc2-apply"), "verdict should name the actual top agent: {out}");
    }

    #[test]
    fn rule_flags_equal_plateaus_as_the_drain_cap_suspect() {
        let rungs = vec![
            rung(64, Mode::Snap, 500_000.0, "uc2-consensus"),
            rung(64, Mode::Lin, 499_000.0, "uc2-consensus"), // within 1%
        ];
        let out = evaluate_decision_rule(&rungs, 0);
        assert!(out.contains("QUERY_DRAIN_PER_CYCLE"), "got: {out}");
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p uc2_node --example read_profile`
Expected: FAIL — `cannot find function 'evaluate_decision_rule'`, `cannot find struct 'Rung'`.

- [ ] **Step 3: Implement `Rung` and the rule**

```rust
/// One measured point: an arm at one concurrency under one write mix.
#[derive(Debug, Clone)]
struct Rung {
    readers: u64,
    mode: Mode,
    write_rate: u64,
    reads_per_sec: f64,
    p50_ms: f64,
    p99_ms: f64,
    top_agent: Option<String>,
}

/// The decision rule from the spec (§2), evaluated verbatim and never tuned:
///
///   Build Rung A iff (a) the linearizable plateau is <=70% of the snapshot
///   plateau AND (b) uc2-consensus or uc2-receiver is the top-occupancy agent.
///   Borderline 65-75% => NOT justified without a fleet run.
///
/// Plateau = the best rate that arm reached across the ladder (the ladder's
/// point is to climb until the rate stops improving, so the max IS the plateau).
fn evaluate_decision_rule(rungs: &[Rung], write_rate: u64) -> String {
    let plateau = |mode: Mode| -> Option<&Rung> {
        rungs
            .iter()
            .filter(|r| r.mode == mode && r.write_rate == write_rate)
            .max_by(|a, b| a.reads_per_sec.total_cmp(&b.reads_per_sec))
    };
    let (Some(lin), Some(snap)) = (plateau(Mode::Lin), plateau(Mode::Snap)) else {
        return "VERDICT: INCONCLUSIVE — both arms must have at least one rung.".into();
    };
    if snap.reads_per_sec <= 0.0 {
        return "VERDICT: INCONCLUSIVE — the snapshot arm measured zero reads/s.".into();
    }
    let ratio = lin.reads_per_sec / snap.reads_per_sec * 100.0;
    let top = lin.top_agent.clone().unwrap_or_else(|| "(not sampled)".into());
    let probe_agent_is_top = top == "uc2-consensus" || top == "uc2-receiver";

    let mut out = String::new();
    out.push_str(&format!(
        "  linearizable plateau : {:>12.0} reads/s (at {} readers)\n",
        lin.reads_per_sec, lin.readers
    ));
    out.push_str(&format!(
        "  snapshot plateau     : {:>12.0} reads/s (at {} readers)\n",
        snap.reads_per_sec, snap.readers
    ));
    out.push_str(&format!("  ratio (lin/snap)     : {ratio:.1}%  [rule: <=70% and not 65-75%]\n"));
    out.push_str(&format!("  top-occupancy agent  : {top}  [rule: uc2-consensus or uc2-receiver]\n"));

    // Both arms cross the same per-cycle query drain, so equal plateaus point at
    // the drain cap rather than at the barrier (spec §6.2).
    if (ratio - 100.0).abs() < 2.0 {
        out.push_str(
            "  NOTE: the two arms plateau within 2% of each other — suspect \
             QUERY_DRAIN_PER_CYCLE (node.rs:186) as the ceiling, not the probe.\n",
        );
    }

    let verdict = if (65.0..=75.0).contains(&ratio) {
        format!(
            "VERDICT: BORDERLINE ({ratio:.1}% is inside the 65-75% band) — \
             NOT justified on this data; resolve with a fleet run or decline."
        )
    } else if ratio <= 70.0 && probe_agent_is_top {
        "VERDICT: Rung A JUSTIFIED — both clauses met.".to_string()
    } else if ratio > 70.0 {
        format!(
            "VERDICT: Rung A NOT JUSTIFIED — clause (a) unmet: the barrier costs \
             only {:.1}% of read capacity.",
            100.0 - ratio
        )
    } else {
        format!(
            "VERDICT: Rung A NOT JUSTIFIED — clause (b) unmet: the top-occupancy agent \
             is {top}, not a probe-touching agent. Removing probe traffic would not \
             move this ceiling; profile {top} instead."
        )
    };
    out.push_str(&verdict);
    out
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p uc2_node --example read_profile`
Expected: PASS, 9 tests (4 from Task 1 + 5 here).

- [ ] **Step 5: Implement the ladder driver**

```rust
fn run_ladder(a: LadderArgs) -> anyhow::Result<()> {
    let root = a.root.unwrap_or_else(|| PathBuf::from("target/read_profile_ladder"));
    let secs = env_cap("UC2_RP_MAX_SECS", a.secs);
    println!("*** LOCAL SMOKE — NOT a fleet number *** (3 nodes + 3 services on one box)");

    let mut rungs: Vec<Rung> = Vec::new();
    for &write_rate in &[0u64, a.write_rate] {
        for mode in [Mode::Snap, Mode::Lin] {
            for &readers in &a.readers {
                let readers = env_cap("UC2_RP_MAX_READERS", readers);
                // A fresh cluster per rung: a rung must not inherit the previous
                // rung's warm caches, log-buffer fill, or leader.
                let (nodes, services, dirs, leader) = boot_cluster(&root, ALL_APP_ID)?;
                let (stats, occ) = run_read_measurement(
                    &dirs[leader],
                    ALL_APP_ID,
                    secs,
                    readers,
                    mode,
                    write_rate,
                    Some(PathBuf::from("/proc/self/task")),
                );
                stop_cluster(nodes, services);
                anyhow::ensure!(
                    stats.regression == 0,
                    "linearizable read regressed by {} at readers={readers}",
                    stats.regression
                );
                if stats.inflight_at_end != 0 {
                    println!(
                        "  WARNING: {} reads unresolved at readers={readers} \
                         (rung recorded, treat with suspicion)",
                        stats.inflight_at_end
                    );
                }
                // In `all`/`ladder` mode /proc/self/task holds the harness's own
                // threads too; only the named agent threads are of interest.
                let top_agent = occ
                    .iter()
                    .find(|o| o.name.starts_with("uc2-"))
                    .map(|o| o.name.clone());
                println!(
                    "  rung: mode={:<5} readers={readers:<5} writes/s={write_rate:<7} \
                     reads/s={:>10.0}  p50={:.3}ms  top={}",
                    match mode { Mode::Lin => "lin", Mode::Snap => "snap" },
                    stats.reads_per_sec,
                    stats.p50_ms,
                    top_agent.clone().unwrap_or_else(|| "-".into())
                );
                rungs.push(Rung {
                    readers,
                    mode,
                    write_rate,
                    reads_per_sec: stats.reads_per_sec,
                    p50_ms: stats.p50_ms,
                    p99_ms: stats.p99_ms,
                    top_agent,
                });
            }
        }
        if write_rate == a.write_rate && a.write_rate == 0 {
            break; // read-only and mixed are the same sweep; do not run it twice
        }
    }

    println!();
    println!("================== decision rule (spec §2) ==================");
    for &write_rate in &[0u64, a.write_rate] {
        let mix = if write_rate == 0 { "read-only arm" } else { "mixed arm" };
        println!("-- {mix} (writes/s = {write_rate}) --");
        println!("{}", evaluate_decision_rule(&rungs, write_rate));
        if write_rate == a.write_rate && a.write_rate == 0 {
            break;
        }
    }
    println!("============================================================");
    println!(
        "REMINDER: local smoke. The read-only arm is the clean isolation; the mixed arm's \
         delta includes the frontier wait, since a snapshot read skips that too."
    );
    Ok(())
}
```

- [ ] **Step 6: Run the ladder end to end**

Run: `cargo run -p uc2_node --release --example read_profile -- ladder --secs 4 --readers 1,16,128 --write-rate 20000`
Expected: 12 rung lines (2 mixes × 2 arms × 3 concurrencies), then two verdict blocks, exit 0. Every rung shows `reads/s > 0`.

- [ ] **Step 7: Commit**

```bash
git add uc2_node/examples/read_profile.rs
git commit -m "feat(read-profile): concurrency ladder and pre-committed decision rule

The rule from spec §2 is evaluated in code, including the 65-75% borderline
band that resolves to NOT JUSTIFIED and the equal-plateau case that points at
QUERY_DRAIN_PER_CYCLE rather than the probe. Unit-tested against all five
outcome shapes so the verdict cannot be quietly re-tuned after seeing data."
```

---

### Task 6: Clippy, docs, and the smoke result record

**Files:**
- Modify: `uc2_node/examples/read_profile.rs` (module doc)
- Create: `docs/benchmarks/uc2-read-profile-2026-07-25.md`

**Interfaces:**
- Consumes: everything above.
- Produces: the report doc (spec §7).

- [ ] **Step 1: Write the module doc**

Replace the placeholder module doc at the top of `read_profile.rs` with a full one covering: the question being answered, the A/B design and why a snapshot read is the right control, the yield-rate occupancy proxy and why not CPU time, the role split and CLI examples for all five roles, the env caps, the pre-committed decision rule, and a LOUD note that local runs are smoke. Follow `m5_gate.rs`'s module-doc shape.

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --workspace --all-targets -- -D warnings`
Expected: zero warnings. Fix anything reported.

- [ ] **Step 3: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: passes — no existing test's behavior changed (production code is untouched).

- [ ] **Step 4: Verify end-to-end wiring at minimal scale**

**This is a wiring check, not a measurement** (Global Constraints): the box carries a concurrent model-check session, so the numbers printed here are noise and must not be recorded anywhere.

Run: `cargo run -p uc2_node --release --example read_profile -- ladder --secs 3 --readers 1,16 --write-rate 5000`
Expected: 8 rung lines (2 mixes × 2 arms × 2 concurrencies), both verdict blocks print without panicking, every rung shows `reads/s > 0` and no regression, exit 0.

Before running, check the box is not already saturated: `uptime && free -g`. If available memory is under ~3 GB, wait — do not risk an OOM that could kill the neighbouring session.

- [ ] **Step 5: Commit**

```bash
git add uc2_node/examples/read_profile.rs
git commit -m "docs(read-profile): module doc; harness verified end to end

Wiring verified locally (reads resolve, monotonic guard holds, clean
teardown). No numbers recorded: this box carries a concurrent model-check
session, so the real ladder runs on the AWS fleet."
```

---

### Task 7: The AWS fleet run — the actual measurement

Everything before this task produces an instrument. This task produces the data. **It spends real money and must not `terraform apply` without explicit user approval** (Step 3 is a hard stop).

**Files:**
- Modify: `bench-infra/scripts/m6_fleet_gate.py` (add a read-profile mode)
- Create: `docs/benchmarks/uc2-read-profile-<run-date>.md`

**Interfaces:**
- Consumes: the `read_profile` binary's `node` / `service` / `client` roles and their CLI flags (Tasks 2–3), including `--node-pid` for occupancy.
- Produces: a `--read-profile` mode on the orchestrator, and the report doc.

- [ ] **Step 1: Read the orchestrator before touching it**

Read `bench-infra/scripts/m6_fleet_gate.py` — specifically the `Host` class (`probe`, `ctl`, start/stop), the `--local` vs `--fleet` connectivity split, the durable-fs guard, and how an existing scenario is structured. **Follow those patterns rather than inventing parallel plumbing**; this plan deliberately does not pre-write the Python, because the host abstractions are the orchestrator's own and must be used as they are.

Fleet operational facts that already bit previous runs (from the M6/M7 gate history): `rsync` ships the local tree (no git push); ansible builds as root (`sudo cargo`, `CARGO_HOME=/opt/bench/.cargo`); `/opt/bench` is the NVMe mount and instance dirs must live there; remote daemons need `systemd-run` (a bare `ssh &` hangs); ssh needs `-i <key>` with `SSH_AUTH_SOCK` unset.

- [ ] **Step 2: Add the `--read-profile` mode**

Requirements for the new mode (3 hosts, one role per host):

- Start a `read_profile node` + `read_profile service` per host; wait for exactly one leader; abort if two hosts ever report serving.
- Sweep the ladder by invoking `read_profile client` **on the leader's host** (shmem is same-host only), once per rung: `--readers` from the sweep list × `--mode {lin,snap}` × `--write-rate {0, W}`, passing `--node-pid` (the node role prints its pid at startup — capture it) so occupancy is sampled from the *node* process, not the client.
- Collect each rung's `reads/s`, `p50`, `p99`, `in-flight at end`, `read regression`, and the occupancy ranking; emit them as machine-readable JSON alongside the human log.
- **Exit code:** 0 even for an unfavourable verdict — this is an instrument, not a gate. Exit 1 **only** on harness failure: any rung with `read regression != 0`, or unresolved reads on more than one rung.
- Reuse the existing per-rung teardown discipline: a fresh cluster per rung, so no rung inherits the previous one's warm state or leader.

- [ ] **Step 3: Validate with `--local`, then STOP for approval**

Run the orchestrator in `--local` mode (real separate processes on loopback) with a short sweep to validate the orchestration itself — not to collect numbers.

Expected: all rungs run, JSON emitted, exit 0.

**Then stop.** Report to the user: the sweep plan, the host class and count, the estimated run time, and ask for explicit approval before `terraform apply`. Do not proceed on inferred consent.

- [ ] **Step 4: Provision, run, collect**

Only after approval:

```bash
cd bench-infra && make up-uc          # terraform apply + ansible provision
# then the orchestrator in --fleet mode with the full sweep
```

Capture the full log and the JSON to the worktree.

- [ ] **Step 5: Destroy the fleet and VERIFY it is gone**

```bash
cd bench-infra && make destroy
terraform -chdir=terraform state list   # MUST be empty
```

The real terraform state is `bench-infra/terraform/` (the repo-root terraform directory is an empty decoy). A run left up bills indefinitely — verify the state list is empty rather than trusting `destroy`'s output.

- [ ] **Step 6: Write the report doc**

Create `docs/benchmarks/uc2-read-profile-<run-date>.md` with: the ladder table for both arms and both mixes; the per-agent occupancy ranking at each plateau; the decision rule evaluated clause by clause with the verdict; and a section addressing each spec §6 threat with what the run actually showed — specifically whether both arms plateaued together (`QUERY_DRAIN_PER_CYCLE` suspect), whether the client sustained the target concurrency, and the reminder that the mixed arm's delta includes the frontier wait. Record the fleet host class and the exact sweep.

Record the verdict the rule returned, including if it declines Rung A. **A "measured, declined" outcome is a successful run of this plan, not a failure.**

- [ ] **Step 7: Commit**

```bash
git add bench-infra/scripts/m6_fleet_gate.py docs/benchmarks/uc2-read-profile-*.md
git commit -m "bench(read-profile): fleet read-profile mode and the measured verdict

Three-host AWS run of the concurrency ladder across both arms and both write
mixes. The decision rule from the spec is evaluated on fleet numbers; the
verdict recorded is what the rule returned."
```

---

## Post-Plan: what happens with the verdict

The harness produces a disposition, not a change to the read path:

- **Rung A JUSTIFIED** → a fleet run to confirm on real hardware, then a Rung-A implementation plan.
- **NOT JUSTIFIED / BORDERLINE** → record it in the leader-lease brief as a measured decline and leave the read path alone.
- **`QUERY_DRAIN_PER_CYCLE` implicated** → a different and much cheaper piece of work than either rung; worth its own brief.

Rung B stays sequenced behind the Veil V2 coherence-window result regardless of this outcome (leader-lease brief §5).
