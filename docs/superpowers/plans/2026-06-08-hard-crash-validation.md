# Hard-crash validation — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use `- [ ]`.

**Goal:** Prove service-state reconstruction + the ReadIndex/seqlock read barrier survive a true `kill -9` of the service mid-apply, on a single-node cluster, checked by the WGL linearizability checker.

**Architecture:** Extract the WGL checker + register SM into a `uc-lincheck` lib; add node-only and service-only reference binaries (`examples/uc-crashtest`) sharing an instance_dir; a feature-gated integration test spawns them as child processes, drives load via `uc_client`, `kill -9`s the service mid-load, restarts it, and runs the checker on the recorded history.

**Tech stack:** Rust, openraft, shmem IPC, `uc_client`, `std::process::Command` + `Child::kill` (SIGKILL).

Spec: `docs/superpowers/specs/2026-06-08-hard-crash-validation-design.md`.

Branch: `feat/hard-crash-validation` (already created).

---

## Phase 1 — Extract `uc-lincheck` lib + refactor the capstone

**Files:**
- Create: `uc-lincheck/Cargo.toml`, `uc-lincheck/src/lib.rs`, `uc-lincheck/src/{model,history,checker,register}.rs`
- Modify: `Cargo.toml` (workspace members), `uc_node/Cargo.toml` (dev-dep), `uc_node/tests/lincheck/mod.rs`, `uc_node/tests/lin_register.rs`, `uc_node/tests/lincheck/cluster.rs`
- Delete: `uc_node/tests/lincheck/{model,history,checker,register_sm}.rs`

- [ ] **Step 1: Create the crate skeleton**

`uc-lincheck/Cargo.toml`:
```toml
[package]
name = "uc-lincheck"
edition.workspace = true
version.workspace = true
license.workspace = true
authors.workspace = true
publish = false

[lib]
path = "src/lib.rs"

[dependencies]
uc_service = { path = "../uc_service" }
serde      = { workspace = true }
bincode    = { workspace = true }
```

`uc-lincheck/src/lib.rs`:
```rust
//! Reusable WGL linearizability harness: the CAS-register sequential model, an
//! operation-history recorder, the linearizability checker, and the in-memory
//! `RegisterSm` (a `uc_service::StateMachine`). Used by the in-process lincheck
//! capstone (`uc_node` tests) and the multi-process hard-crash test
//! (`examples/uc-crashtest`).
pub mod checker;
pub mod history;
pub mod model;
pub mod register;
```

- [ ] **Step 2: Move the four modules into the crate, fixing paths**

`git mv` (or copy+delete) the existing files:
- `uc_node/tests/lincheck/model.rs` → `uc-lincheck/src/model.rs` (no path edits needed; it's self-contained).
- `uc_node/tests/lincheck/history.rs` → `uc-lincheck/src/history.rs`. Change `pub use crate::lincheck::model::{Op, RegResp};` → `pub use crate::model::{Op, RegResp};`.
- `uc_node/tests/lincheck/checker.rs` → `uc-lincheck/src/checker.rs`. Change any `crate::lincheck::{model,history}` paths → `crate::{model,history}` (grep for `crate::lincheck`).
- `uc_node/tests/lincheck/register_sm.rs` → `uc-lincheck/src/register.rs` (no path edits; it imports `uc_service` + `serde` + `bincode` only).

The existing `#[cfg(test)] mod tests` in each become this crate's unit tests.

- [ ] **Step 3: Add to the workspace + uc_node dev-deps**

In root `Cargo.toml`, add `"uc-lincheck"` to `members`.
In `uc_node/Cargo.toml` `[dev-dependencies]`, add `uc-lincheck = { path = "../uc-lincheck" }`.

- [ ] **Step 4: Repoint the capstone at the lib**

`uc_node/tests/lincheck/mod.rs` → just:
```rust
//! In-process 3-node lincheck cluster harness (capstone-only). The checker,
//! history, model, and register SM now live in the `uc-lincheck` crate.
pub mod cluster;
```
In `uc_node/tests/lincheck/cluster.rs`: replace `use crate::lincheck::register_sm::{Cmd, CmdResp, RegisterSm};` with `use uc_lincheck::register::{Cmd, CmdResp, RegisterSm};`.
In `uc_node/tests/lin_register.rs`: replace `use lincheck::register_sm::{Cmd, CmdResp};` and any `lincheck::{history,checker,model}::…` references with `uc_lincheck::{register::{Cmd,CmdResp}, history::{History, Outcome}, checker::{check_register, Verdict}, model::{Op, RegResp}}` as used. (grep `lincheck::` in lin_register.rs and map each: `register_sm`→`uc_lincheck::register`, `history`→`uc_lincheck::history`, `checker`→`uc_lincheck::checker`, `model`→`uc_lincheck::model`; `cluster` stays `lincheck::cluster`.)

- [ ] **Step 5: Build + verify**

Run: `cargo clippy --workspace --all-targets -- -D warnings` → clean.
Run: `cargo test -p uc-lincheck` → the moved unit tests pass.
Run: `cargo test -p uc_node --test lin_register -- --test-threads=1` → smoke + checker unit tests pass; then the capstone across seeds:
```bash
for s in 4359 1 88888 7 42; do LIN_SEED=$s cargo test -p uc_node --test lin_register linearizable_under_failover -- --exact 2>&1 | grep "test result"; done
```
Expected: all `ok`.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor(lincheck): extract WGL checker + register SM into uc-lincheck crate

Moves model/history/checker/register_sm out of uc_node/tests/lincheck (test
modules) into a reusable uc-lincheck lib so the multi-process hard-crash test can
share the exact checker. Capstone repointed at the lib (cluster.rs stays in-process);
verified green across seeds."
```

---

## Phase 2 — Multi-process foundation (node + service bins + smoke test)

**Files:**
- Create: `examples/uc-crashtest/Cargo.toml`, `src/bin/uc-crashtest-node.rs`, `src/bin/uc-crashtest-service.rs`, `tests/smoke.rs`
- Modify: root `Cargo.toml` (members)

- [ ] **Step 1: Crate + workspace member**

`examples/uc-crashtest/Cargo.toml`:
```toml
[package]
name = "uc-crashtest"
edition.workspace = true
version.workspace = true
license.workspace = true
authors.workspace = true
publish = false

[[bin]]
name = "uc-crashtest-node"
path = "src/bin/uc-crashtest-node.rs"

[[bin]]
name = "uc-crashtest-service"
path = "src/bin/uc-crashtest-service.rs"

[dependencies]
uc_node    = { path = "../../uc_node" }
uc_service = { path = "../../uc_service" }
uc-lincheck = { path = "../../uc-lincheck" }
ultima-journal = { workspace = true }
tokio      = { workspace = true }
clap       = { version = "4", features = ["derive"] }
anyhow     = { workspace = true }
tracing-subscriber = { workspace = true }

[dev-dependencies]
uc_client  = { path = "../../uc_client" }
uc-lincheck = { path = "../../uc-lincheck" }
tokio      = { workspace = true }
tempfile   = { workspace = true }
rand       = { workspace = true }
bincode    = { workspace = true }
serde      = { workspace = true }
```
Add `"examples/uc-crashtest"` to root `Cargo.toml` members.

- [ ] **Step 2: node-only binary**

`src/bin/uc-crashtest-node.rs`:
```rust
//! Node-only reference binary for the multi-process hard-crash test. Creates the
//! instance_dir/cnc.dat, runs raft (single-node), waits for the service handshake,
//! serves clients. Parks until killed (the test SIGKILLs it).
use std::path::PathBuf;
use clap::Parser;
use uc_lincheck::register::RegisterSm;
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, RaftTuning,
    ServiceRingConfig, TlsConfig,
};

#[derive(Parser)]
struct Args {
    #[arg(long)] instance_dir: PathBuf,
    #[arg(long)] data_dir: PathBuf,
    #[arg(long, default_value = "uc-crashtest")] app_id: String,
    #[arg(long, default_value = "127.0.0.1:0")] raft_addr: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.data_dir)?;
    let cfg = NodeConfig {
        node_id: 1,
        data_dir: args.data_dir,
        raft_listen_addr: args.raft_addr.parse()?,
        app_id: args.app_id,
        bootstrap: BootstrapConfig::SingleNode,
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),
        ipc_mode: IpcMode::Shmem { instance_dir: args.instance_dir },
        client_rings: ClientRingConfig::default(),
        service_rings: ServiceRingConfig::default(),
        log_durability: ultima_journal::Durability::Eventual,
    };
    // start() creates cnc.dat then blocks on the service handshake; keep the
    // handle alive and park (the test kills this process).
    let _node = NodeBuilder::new(cfg, RegisterSm::default()).start().await?;
    std::future::pending::<()>().await;
    Ok(())
}
```

- [ ] **Step 3: service-only binary**

`src/bin/uc-crashtest-service.rs`:
```rust
//! Service-only reference binary. Waits for the node's cnc.dat, attaches, runs the
//! in-memory RegisterSm. Parks until killed (the test SIGKILLs it mid-apply).
use std::path::PathBuf;
use std::time::Duration;
use clap::Parser;
use uc_lincheck::register::RegisterSm;
use uc_service::runtime::ServiceConfig;
use uc_service::ServiceBuilder;

#[derive(Parser)]
struct Args {
    #[arg(long)] instance_dir: PathBuf,
    #[arg(long)] data_dir: PathBuf,
    #[arg(long, default_value = "uc-crashtest")] app_id: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.data_dir)?;
    // Wait for the node to create cnc.dat before attaching.
    let cnc = args.instance_dir.join("cnc.dat");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !cnc.exists() {
        anyhow::ensure!(std::time::Instant::now() < deadline, "timed out waiting for cnc.dat");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let cfg = ServiceConfig {
        instance_dir: args.instance_dir,
        app_id: args.app_id,
        data_dir: args.data_dir,
        ..ServiceConfig::default()
    };
    let _svc = ServiceBuilder::new(cfg, RegisterSm::default()).run().await?;
    std::future::pending::<()>().await;
    Ok(())
}
```

- [ ] **Step 4: smoke test harness**

`tests/smoke.rs` (gated behind feature `hard-crash-tests` so default `cargo test` stays hermetic — see Step 5):
```rust
#![cfg(feature = "hard-crash-tests")]
//! Multi-process smoke: spawn node + service as real processes, write+read via a
//! real uc_client, shut down cleanly.
use std::path::Path;
use std::process::{Child, Command};
use std::time::{Duration, Instant};
use uc_client::Client;

fn spawn(bin: &str, args: &[(&str, &str)]) -> Child {
    let mut c = Command::new(env!(concat!("CARGO_BIN_EXE_", "uc-crashtest-node"))); // placeholder; see note
    let _ = (bin, &mut c, args);
    unreachable!()
}
```
NOTE for the implementer: `env!("CARGO_BIN_EXE_uc-crashtest-node")` / `..._uc-crashtest-service` give the built bin paths (only valid inside this crate's tests). Write a small `spawn_node(instance, data)` and `spawn_service(instance, data)` returning `Child`, each `Command::new(env!("CARGO_BIN_EXE_…"))` with `--instance-dir`/`--data-dir` args and stdout/stderr inherited. Poll `instance/cnc.dat` for readiness before spawning the service. Then:
```rust
// pseudocode body of the smoke test:
let tmp = tempfile::tempdir().unwrap();
let inst = tmp.path().join("inst");
let node = spawn_node(&inst, &tmp.path().join("nodedata"));
wait_for_path(&inst.join("cnc.dat"), Duration::from_secs(10));
let mut svc = spawn_service(&inst, &tmp.path().join("svcdata"));
let client = Client::connect(&inst, "uc-crashtest").await.unwrap();
// wait for leader
for _ in 0..100 { if client.current_leader().is_some() { break } tokio::time::sleep(Duration::from_millis(50)).await; }
let r: uc_lincheck::register::CmdResp =
    client.submit(&uc_lincheck::register::Cmd::Write(7)).await.unwrap();
assert!(matches!(r, uc_lincheck::register::CmdResp::WriteAck));
let v: Option<u64> = client.query_linearizable(&()).await.unwrap();
assert_eq!(v, Some(7));
let _ = client.shutdown().await;
let _ = svc.kill(); let _ = node.kill();
```
Use `Cmd`/`CmdResp` from `uc_lincheck::register` (matching the SM's `Command`/`Response`). Reads are `Query = ()`, `QueryResponse = Option<u64>`.

- [ ] **Step 5: feature gate**

In `examples/uc-crashtest/Cargo.toml` add:
```toml
[features]
hard-crash-tests = []
```
The tests `#![cfg(feature = "hard-crash-tests")]` so `cargo test` is a no-op without the feature; run explicitly with `cargo test -p uc-crashtest --features hard-crash-tests`.

- [ ] **Step 6: Build + run smoke**

`cargo build -p uc-crashtest` (bins compile).
`cargo test -p uc-crashtest --features hard-crash-tests --test smoke -- --nocapture` → passes (write 7, read 7).
`cargo clippy --workspace --all-targets -- -D warnings` → clean (note: clippy builds the bins).

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "test(crashtest): multi-process node/service bins + smoke (feature hard-crash-tests)"
```

---

## Phase 3 — Hard-crash test

**Files:** Create `examples/uc-crashtest/tests/hard_crash.rs`.

- [ ] **Step 1: Write the hard-crash test**

`tests/hard_crash.rs` (`#![cfg(feature = "hard-crash-tests")]`). Reuse the spawn helpers (factor them into a `mod common;` shared file, or duplicate). Structure:
```text
seed from LIN_SEED env (default 1).
tmp dirs; spawn node; wait cnc.dat; spawn service (Arc<Mutex<Child>> so the fault
  loop can replace it); connect client; wait for leader.
History = uc_lincheck::history::History::default().
Spawn N=3 worker tasks: until stop, throttle ~5ms, pick a seeded op:
  - Write(v): inv=h.invoke(); match client.submit(Cmd::Write(v)):
       Ok(_) => record Ok(RegResp::Ack); Err(timeout/stalled/overwritten) => record Indeterminate;
       Err(other) => record Indeterminate (service down window).
  - Read: inv=h.invoke(); match client.query_linearizable::<(),Option<u64>>(&()):
       Ok(v) => record Ok(RegResp::Value(v)); Err(transient) => Indeterminate.
  - Cas{old,new}: like Write; Ok(CmdResp::CasResult(b)) => Ok(RegResp::CasOk(b)).
  (Mirror uc_node/tests/lin_register.rs::worker exactly for op selection + outcome
   classification; last_seen AtomicU64 so CAS picks recently-seen values.)
Fault loop: a few times (e.g. 5), sleep ~700ms, then HARD-CRASH the service:
   { let mut g = svc.lock(); let _ = g.kill(); let _ = g.wait(); }   // SIGKILL + reap
   respawn: *g = spawn_service(&inst, &svcdata);  wait cnc.dat still present.
   (The client stays connected to the NODE; submits during the down window time out
    → Indeterminate, which the checker tolerates.)
After target ops (e.g. 400 Ok) or the fault loop ends: stop workers, join.
let entries = history.into_entries();
assert!(matches!(uc_lincheck::checker::check_register(&entries),
                 uc_lincheck::checker::Verdict::Linearizable),
        "hard-crash history not linearizable (seed {seed})");
client.shutdown(); kill node + service.
```
Key correctness point: a `kill -9` landing mid-`submit` makes that op **Indeterminate** (may or may not have committed) — record it as such, never as Ok. The checker already allows an Indeterminate op to have happened or not.

- [ ] **Step 2: Run across seeds**

```bash
for s in 1 7 42 88888 4359; do
  LIN_SEED=$s cargo test -p uc-crashtest --features hard-crash-tests --test hard_crash -- --exact --nocapture 2>&1 | grep -E "Linearizable|VIOLATION|test result|not linearizable"
done
```
Expected: every seed `Linearizable` / `test result: ok`.
If a VIOLATION appears: it's a REAL reconstruction-under-hard-crash bug — debug with `superpowers:systematic-debugging` (the recorded history pinpoints the stale op). Do NOT weaken the test.

- [ ] **Step 3: clippy + commit**

`cargo clippy --workspace --all-targets -- -D warnings` clean.
```bash
git add -A
git commit -m "test(crashtest): hard-crash (kill -9 mid-apply) linearizability test

Single-node: SIGKILLs the service process during sustained uc_client load, restarts
it, and asserts the recorded op history is WGL-linearizable across seeds — proving
node-driven reconstruction + the ReadIndex/seqlock barrier survive a TRUE crash
(not the graceful shutdowns the in-process capstone uses)."
```

---

## Final

- [ ] Update CLAUDE.md: document the real multi-process / hard-crash test path
  (`cargo test -p uc-crashtest --features hard-crash-tests`) replacing the
  aspirational `--features multi-process-tests` line; note `uc-lincheck` as the
  shared checker crate.
- [ ] Update `docs/tasks/task14_service_state_reconstruction.md` Tests section +
  drop the "hard `kill -9` not exercised" known-limitation bullet (now covered).
- [ ] Final review (subagent) over the new crate + the capstone refactor.
- [ ] `superpowers:finishing-a-development-branch` → merge to main locally.

## Self-review notes (spec coverage)
- spec "multi-process foundation" → Phase 2. "hard-crash fault" → Phase 3.
- spec "extract uc-lincheck + refactor capstone" → Phase 1.
- spec "single-node" → node bin is `BootstrapConfig::SingleNode`; one service.
- spec "WGL checker, client records history, Indeterminate on kill" → Phase 3 Step 1.
- spec "gate behind feature; condition-polling; bounded faults" → Phase 2 Step 5, Phase 3 Step 1.
