# M14c2 — Multi-Service Proof Pass (2.8.1): Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the multi-service proof gap disclosed by 2.8.0 — the two-FSM linearizability, partition, hard-crash and Elle capstones — settle the lockstep 60× finding by experiment (and fix it if it is a defect), pin the fleet rig, close the M14c deferrals, and ship it all as `2.8.1`.

**Architecture:** One harness extension (`uc_node/tests/lincheck_v2/mod.rs` learns a second FSM per node, a `Slow<SM>` wrapper and a `submit_all_cmd` that records into two histories) unlocks three tiers; the crashtest gets the same via a shared `ServicesConfig::from_cli` and two new process-level tests; every two-FSM capstone asserts per-FSM linearizability with the untouched `uc_lincheck` checker **plus** the replication-equivalence oracle (every `submit_all`'s responses byte-equal), and each new oracle is shown to bite on an injected divergence. The lockstep question is answered by a reproducible dev-box experiment with a pre-committed decision rule before any code changes. No wire/cnc change; 2.8.1 is API-compatible with 2.8.0.

**Tech Stack:** Rust 1.96 workspace; `uc_node` test harness (`lincheck_v2`), `uc_lincheck` (WGL checker, `RegisterSm`, `EdnRecorder`), `uc_client::Client`, `uc_service::{StateMachine, SnapshotStateMachine}`; `examples/uc_crashtest` (real processes, `hard-crash-tests` feature); `scripts/elle_check.sh` + vendored `elle-cli`; `uc_node/examples/apply_bench` + `taskset`/`stress-ng`; Python 3 fleet drivers under `bench-infra/scripts/`.

**Spec:** `docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md` **§16** (binding; §12's capstone paragraph is the origin, §16.3 restates the oracles). Read §16 in full before Task 1.

## Global Constraints

- **Every existing test stays byte-for-byte unchanged in behaviour.** `ClusterCfg::default()` keeps `services: FsmSet::Single`; `LinClusterV2<SM>` keeps its one-generic form via a defaulted second parameter; `make_config` still passes `ServicesConfig::default()` for `Single`. The whole workspace suite must stay green after every task.
- **The checker is not touched.** `uc_lincheck::checker::check_register` and `History` are consumed as-is; two-FSM capstones build one `History` per FSM.
- **The replication-equivalence oracle, verbatim (spec §16.3):** for every `submit_all`, the two responses are byte-equal (compared as the decoded `CmdResp`/`LaResp` values, which is the same thing for these deterministic SMs). A single unequal pair = FAIL. Reads are per-FSM history evidence only (spec erratum in Task 0).
- **Slow-FSM oracle (ruling 2026-08-30):** bounded mode — `applied_0 − applied_1 ≤ fsm_lag` at every 50 ms sample **and** over the second half of the run FSM 0's applied-bytes rate is within 10 % of FSM 1's; lockstep — `applied_0 − applied_1 ≤ one frame` at every sample (one frame = `max_payload` + the 32-byte header, ≤ 288 bytes here).
- **Every new oracle is demonstrated to bite**: one `#[should_panic]`/`Err`-asserting test per oracle with an injected divergence (`Corrupt<SM>` wrapper), kept in the tree.
- **Lockstep decision rule, verbatim (spec §16.4):** product defect iff it reproduces only under oversubscription *and* one of (a) ladder ×4/×16, (b) yield-not-sleep while a sibling is live, (c) futex wait on the sibling's applied word restores ≥ 50 % of the unconstrained rate under the same oversubscription without regressing unconstrained N=1/N=2 bounded by more than a same-source-rebuild control; otherwise an operating-envelope fact stated with the number.
- **Fleet spend is user-gated** (Tasks 9 and, if reached, the row-e re-measure). Local numbers are smoke, never a gate (CLAUDE.md).
- **Never write scratch to `/tmp`**; Elle histories go under `$HOME/.cache/uc2-elle*`; `apply_bench --root` under `$HOME/.cache`.
- `cargo fmt` stays deferred; match surrounding style by hand. `cargo clippy --workspace --all-targets -- -D warnings` clean after every task.
- Commit subjects: `type(scope): imperative summary` as in `git log --oneline -30`.
- 2.8.1 is proof-only unless Task 8's decision says defect; then the smallest fix from Task 8's candidates ships with it. The tag is `git tag -a` (cut-a-release §4 as corrected 2026-08-30). crates.io publish only on the maintainer's explicit go.

---

## File structure

| file | responsibility | task |
|---|---|---|
| `docs/superpowers/specs/…-multi-service-design.md` §16.3 | erratum: equivalence is on `submit_all` responses only | 0 |
| `uc_node/src/services.rs` | `ServicesConfig::from_cli(ids, fsm_lag)` + tests | 1 |
| `uc_gateway/examples/m12_gate.rs`, `examples/uc_crashtest/src/bin/uc_crashtest-node.rs` | use `from_cli`; node bin gains `--services`/`--fsm-lag` | 1 |
| `uc_node/tests/lincheck_v2/mod.rs` | `FsmSet`, `Slow<SM, MS>`, `Corrupt<SM>`, second service per slot, `submit_all_cmd`, `service_applied`, respawn of both | 2 |
| `uc_node/tests/lin_v2.rs` | `two_fsm_bounded`, `two_fsm_lockstep`, `two_fsm_oracle_bites`, `two_fsm_slow`, `two_fsm_slow_lockstep` | 3, 4 |
| `uc_node/tests/lin_partition_v2.rs` | `minority_partition_and_heal_two_fsm` | 5 |
| `examples/uc_crashtest/tests/common/mod.rs`, `tests/hard_crash.rs` | `spawn_service_id`, `spawn_node_with_services`, `submit_all_cmd`, two tests | 6 |
| `uc_node/tests/elle_v2.rs`, `scripts/elle_check.sh` | `elle_quiet_two_fsm`, pass registered | 7 |
| `scripts/lockstep_oversub.sh`, `docs/benchmarks/uc2-m14c2-lockstep-oversubscription-2026-08-30.md`, maybe `uc_service/src/apply.rs` | the experiment, its record, the conditional fix | 8 |
| `bench-infra/scripts/m12_fleet_gate.py`, `m14_fleet_gate.py`, `m14_ab_27_vs_28.py` | `CPUAffinity` per unit, `--pin` | 9 |
| `uc_net/src/{sender,receiver}.rs` + tests, `uc_node/src/obs/metrics.rs` | deferrals (net) | 10a |
| `uc_service/src/apply.rs`, `uc_node/src/node.rs`, `uc_node/tests/learner.rs`, `uc_ctl/src/main.rs`, docs | deferrals (service/node/ctl/docs) | 10b |
| `uc_node/tests/lin_v2.rs` (or `purge_safety.rs`) | `snapshot_install_needs_purge` pin | 11 |
| `Cargo.toml` + pins, `RELEASES.md`, `docs/releases.md`, `docs/VERIFICATION.md`, gate doc, `README.md` | 2.8.1 | 12 |

---

### Task 0: Spec erratum — equivalence is on responses, not reads

**Files:**
- Modify: `docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md` (§16.3 table, Elle row; §16.3 preamble)

- [ ] **Step 1: Edit the Elle row and preamble**

In the §16.3 table, the `elle_v2::elle_quiet_two_fsm` row's last cell `one Elle history per FSM, each under both models; equivalence on every read` → `one Elle history per FSM, each under both models; equivalence on every submit_all's two responses (reads at different instants may legitimately differ)`. In the preamble sentence "the **replication-equivalence oracle** — for every `submit_all`, the two responses are byte-equal", append: "Reads are evidence for their own FSM's history only; two linearizable reads of two FSMs are two operations at two instants and are never compared."

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md
git commit -m "spec(m14): §16.3 erratum — the equivalence oracle covers submit_all responses only; reads are per-FSM evidence"
```

---

### Task 1: `ServicesConfig::from_cli` — one CLI parser for every bin

**Files:**
- Modify: `uc_node/src/services.rs` (after `from_ids`), `uc_gateway/examples/m12_gate.rs` (`services_from_flags`), `examples/uc_crashtest/src/bin/uc_crashtest-node.rs` (args + `NodeConfig.services`)
- Test: `uc_node/src/services.rs` `#[cfg(test)]` module (exists — add to it)

**Interfaces:**
- Consumes: `ServicesConfig::from_ids(&[u8], Option<FsmLag>) -> Result<Self, String>`, `parse_fsm_lag(&str) -> Result<FsmLag, String>` (`services.rs:172`).
- Produces: `pub fn from_cli(ids: Option<&str>, fsm_lag: Option<&str>) -> Result<ServicesConfig, String>` on `ServicesConfig`; error strings begin with `--services` or `--fsm-lag`. Crashtest node CLI: `--services 0,1` and `--fsm-lag lockstep|<bytes>`.

- [ ] **Step 1: Write the failing tests** (in `services.rs`'s test module)

```rust
    #[test]
    fn from_cli_absent_is_default_and_both_flags_parse() {
        assert_eq!(ServicesConfig::from_cli(None, None).unwrap().declared(), 0b1);
        let s = ServicesConfig::from_cli(Some("0, 1"), Some("65536")).unwrap();
        assert_eq!(s.declared(), 0b11);
        assert_eq!(s.resolve_lag(1 << 20), FsmLag::Bounded(65536));
        let s = ServicesConfig::from_cli(Some("0,1"), Some("lockstep")).unwrap();
        assert_eq!(s.resolve_lag(1 << 20), FsmLag::Lockstep);
        // lag without ids applies to the default set
        let s = ServicesConfig::from_cli(None, Some("lockstep")).unwrap();
        assert_eq!(s.declared(), 0b1);
        assert_eq!(s.resolve_lag(1 << 20), FsmLag::Lockstep);
    }

    #[test]
    fn from_cli_refuses_by_flag_name() {
        assert!(ServicesConfig::from_cli(Some("1"), None).unwrap_err().starts_with("--services"));
        assert!(ServicesConfig::from_cli(Some("0,x"), None).unwrap_err().starts_with("--services"));
        assert!(ServicesConfig::from_cli(Some("0"), Some("bogus")).unwrap_err().starts_with("--fsm-lag"));
    }
```

- [ ] **Step 2: Run to verify they fail** — `cargo test -p uc_node --lib services::tests::from_cli 2>&1 | tail -3` → `no function or associated item named from_cli`.

- [ ] **Step 3: Implement** (in `impl ServicesConfig`, after `from_ids`)

```rust
    /// The CLI form every gate/harness binary shares: `--services 0,1`
    /// (absent ⇒ the default set `{0}`) and `--fsm-lag lockstep|<bytes>`
    /// (absent ⇒ the default bound), refused by flag name the way
    /// `node.toml`'s loader refuses by field name.
    pub fn from_cli(ids: Option<&str>, fsm_lag: Option<&str>) -> Result<Self, String> {
        let lag = match fsm_lag {
            None => None,
            Some(raw) => Some(parse_fsm_lag(raw.trim()).map_err(|d| format!("--fsm-lag {raw:?}: {d}"))?),
        };
        match ids {
            None if lag.is_none() => Ok(Self::default()),
            None => Self::from_ids(&[0], lag).map_err(|d| format!("--services (default 0): {d}")),
            Some(list) => {
                let ids = list
                    .split(',')
                    .map(|s| s.trim().parse::<u8>().map_err(|e| format!("--services {list:?}: {s:?} is not an id ({e})")))
                    .collect::<Result<Vec<u8>, String>>()?;
                Self::from_ids(&ids, lag).map_err(|d| format!("--services {list:?}: {d}"))
            }
        }
    }
```

Then in `m12_gate.rs` replace `services_from_flags`'s body with `ServicesConfig::from_cli(services, fsm_lag).map_err(anyhow::Error::msg)` (keep the function so its three tests still compile; they assert on the same prefixes). In `uc_crashtest-node.rs` add to `Args`:

```rust
    /// M14c2: declared FSM ids (`0,1`). Absent = `{0}`.
    #[arg(long)]
    services: Option<String>,
    /// M14c2: `lockstep` or a byte bound.
    #[arg(long)]
    fsm_lag: Option<String>,
```

and in the `NodeConfig` literal: `services: uc_node::ServicesConfig::from_cli(args.services.as_deref(), args.fsm_lag.as_deref()).unwrap_or_else(|e| { eprintln!("{e}"); std::process::exit(2) }),`.

- [ ] **Step 4: Verify** — `cargo test -p uc_node --lib services:: 2>&1 | tail -3` (all pass); `cargo test -p uc_gateway --example m12_gate 2>&1 | tail -2` (7 pass); `cargo build -p uc_crashtest 2>&1 | tail -1`; `cargo run -p uc_crashtest --bin uc_crashtest-node -- --help | grep -c 'services\|fsm-lag'` → 2; clippy clean.

- [ ] **Step 5: Commit** — `git commit -am "feat(node): ServicesConfig::from_cli — the shared --services/--fsm-lag parser; m12_gate and uc_crashtest-node use it (M14c2 T1)"`

---

### Task 2: The harness learns a second FSM

**Files:**
- Modify: `uc_node/tests/lincheck_v2/mod.rs` (`ClusterCfg`, `NodeSlot`, `LinClusterV2`, `make_config`, `spawn_service`, `start_cfg`, `kill_and_restart_leader`, `supervise_services`, `crash_and_restart_leader_service`, `crash_and_restart_random_follower_service`, the spare-node spawn if it spawns a service, `stop`; new `submit_all_cmd`, `service_applied`, `Slow`, `Corrupt`)
- Test: a smoke test appended to `uc_node/tests/lin_v2.rs`

**Interfaces:**
- Consumes: `uc_client::Client::submit_all<C, R>(&self, &C) -> Result<Vec<(u8, R)>, ClientError>` (`uc_client/src/client.rs:140`); `uc_log::cnc::CncPage::open_file(&Path, app_id)` + `.service_slot(i).applied.load_acquire()`; `ServiceConfig::service_id(u8)`; `ServicesConfig::from_ids(&[0, 1], Some(lag))`.
- Produces:
  ```rust
  #[derive(Clone, Copy)] pub enum FsmSet { Single, Two { lag: uc_node::FsmLag } }
  pub struct ClusterCfg { …, pub services: FsmSet }              // default Single
  pub struct Slow<SM, const MICROS: u64>(pub SM);                 // StateMachine + SnapshotStateMachine + Default
  pub struct Corrupt<SM>(pub SM);                                 // apply flips every CasResult; for oracle-bites tests
  pub struct LinClusterV2<SM = RegisterSm, SM1 = SM>              // SM1 used only when services == Two
  impl LinClusterV2<SM, SM1> { pub fn service_applied(&self, node: usize, id: u8) -> u64 }
  pub fn submit_all_cmd<C, R>(conn: &mut WorkerConn, cmd: &C, deadline: Instant) -> SubmitOutcome<Vec<(u8, R)>>
  ```
- Design note: `NodeSlot<SM, SM1>` gains `service1: Option<uc_service::Service<SM1>>`, `None` under `Single`. Every place that takes/crashes/respawns `service` does the same for `service1` (node-before-service teardown order kept: node, service, service1).

- [ ] **Step 1: Write the failing smoke test** (append to `lin_v2.rs`)

```rust
/// M14c2 T2 smoke: two FSMs boot, one `submit_all` answers from both with
/// equal responses, and the cnc slots show both attached and applied.
#[test]
fn two_fsm_smoke() {
    let _g = serialize();
    let dir = tempdir();
    let ccfg = ClusterCfg { services: lincheck_v2::FsmSet::Two { lag: uc_node::FsmLag::Bounded(64 * 1024) }, ..ClusterCfg::default() };
    let cluster: LinClusterV2 = LinClusterV2::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    let leader = cluster.await_single_serving(30);
    let client = cluster.client(leader);
    let resps: Vec<(u8, CmdResp)> = client.submit_all(&Cmd::Write(7)).expect("submit_all");
    assert_eq!(resps.len(), 2, "{resps:?}");
    assert_eq!(resps[0].1, resps[1].1, "replication-equivalence: {resps:?}");
    let r2: Vec<(u8, CmdResp)> = client.submit_all(&Cmd::Cas { old: 7, new: 8 }).expect("submit_all cas");
    assert!(r2.iter().all(|(_, r)| *r == CmdResp::CasResult(true)), "{r2:?}");
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && (cluster.service_applied(leader, 0) == 0 || cluster.service_applied(leader, 1) == 0) {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(cluster.service_applied(leader, 0) > 0 && cluster.service_applied(leader, 1) > 0);
    cluster.stop();
}
```

- [ ] **Step 2: Run to verify it fails** — `cargo test -p uc_node --test lin_v2 two_fsm_smoke 2>&1 | grep -E 'error\[' | head -3` → `no variant or associated item named Two` / `no field services`.

- [ ] **Step 3: Implement the harness extension**

`ClusterCfg`:
```rust
/// M14c2: which FSM set every node declares. `Single` is byte-for-byte the
/// pre-M14c2 harness (one implicit FSM 0). `Two` declares ids {0, 1} with the
/// given lag policy and starts a second service (`SM1`) per node.
#[derive(Clone, Copy)]
pub enum FsmSet {
    Single,
    Two { lag: uc_node::FsmLag },
}
// in ClusterCfg: pub services: FsmSet,   // Default: FsmSet::Single
```

`make_config` gets `services: uc_node::ServicesConfig` from the caller:
```rust
fn services_config(ccfg: ClusterCfg) -> uc_node::ServicesConfig {
    match ccfg.services {
        FsmSet::Single => uc_node::ServicesConfig::default(),
        FsmSet::Two { lag } => uc_node::ServicesConfig::from_ids(&[0, 1], Some(lag)).expect("ids 0,1"),
    }
}
```
and `services: services_config(ccfg),` in the `NodeConfig` literal.

`spawn_service` gains an id:
```rust
fn spawn_service<SM: SnapshotStateMachine + Default>(dir: &Path, snapshot_interval_bytes: u64, id: u8) -> uc_service::Service<SM> {
    let cfg = ServiceConfig::new(dir, APP).service_id(id);
    if snapshot_interval_bytes == 0 {
        ServiceBuilder::new(cfg, SM::default()).start().expect("service start")
    } else {
        ServiceBuilder::new(cfg.snapshot_policy(SnapshotPolicy { interval_bytes: snapshot_interval_bytes }), SM::default())
            .start_with_snapshots().expect("snapshot service start")
    }
}
/// The second FSM under `FsmSet::Two`, else `None`.
fn spawn_service1<SM1: SnapshotStateMachine + Default>(dir: &Path, ccfg: ClusterCfg) -> Option<uc_service::Service<SM1>> {
    matches!(ccfg.services, FsmSet::Two { .. }).then(|| spawn_service::<SM1>(dir, ccfg.snapshot_interval_bytes, 1))
}
```
Every existing `spawn_service(&dir, self.ccfg.snapshot_interval_bytes)` call becomes `spawn_service(&dir, self.ccfg.snapshot_interval_bytes, 0)` followed by `spawn_service1::<SM1>(&dir, self.ccfg)` into `service1`. `NodeSlot<SM, SM1>` and `LinClusterV2<SM = RegisterSm, SM1 = SM>` with bounds `SM1: SnapshotStateMachine + Default` on every impl. Crash paths: after `service.take().crash()` add `if let Some(s1) = self.nodes[i].service1.take() { s1.crash(); }`; `supervise_services` also checks `service1.is_alive()`; `stop` drops `service1` after `service`. The spare-node path (`random_config_op`) spawns the spare's service via the same helper — pass `0` and add `service1` there too (it must be a full two-FSM node under `Two`, else the leader refuses the join's declared-set check).

`Slow` and `Corrupt` (put beside `RegisterSm`'s use, in the harness module):
```rust
/// FSM 1's stand-in for the slow-FSM oracle: `apply` sleeps `MICROS` then
/// delegates; output identical to `SM`'s, so the equivalence oracle holds.
#[derive(Default)]
pub struct Slow<SM, const MICROS: u64>(pub SM);

impl<SM: uc_service::StateMachine, const MICROS: u64> uc_service::StateMachine for Slow<SM, MICROS> {
    type Command = SM::Command;
    type Response = SM::Response;
    type Query = SM::Query;
    type QueryResponse = SM::QueryResponse;
    fn apply(&mut self, position: u64, cmd: SM::Command) -> SM::Response {
        std::thread::sleep(Duration::from_micros(MICROS));
        uc_service::StateMachine::apply(&mut self.0, position, cmd)
    }
    fn query(&self, q: SM::Query) -> SM::QueryResponse { uc_service::StateMachine::query(&self.0, q) }
    fn last_applied(&self) -> Option<u64> { uc_service::StateMachine::last_applied(&self.0) }
}
impl<SM: uc_service::SnapshotStateMachine, const MICROS: u64> uc_service::SnapshotStateMachine for Slow<SM, MICROS> {
    type SnapshotHandle = SM::SnapshotHandle;
    fn freeze(&self) -> Result<(SM::SnapshotHandle, u64), uc_service::SnapshotError> { self.0.freeze() }
    fn stream_snapshot(h: SM::SnapshotHandle, dst: &mut dyn std::io::Write) -> Result<(), uc_service::SnapshotError> { SM::stream_snapshot(h, dst) }
    fn install_snapshot(&mut self, position: u64, src: &mut dyn std::io::Read) -> Result<u64, uc_service::SnapshotError> { self.0.install_snapshot(position, src) }
}

/// An FSM that answers every CAS with the OPPOSITE result — exists only to
/// prove the replication-equivalence oracle bites (`two_fsm_oracle_bites`).
#[derive(Default)]
pub struct Corrupt<SM>(pub SM);
impl uc_service::StateMachine for Corrupt<RegisterSm> {
    type Command = Cmd; type Response = CmdResp; type Query = (); type QueryResponse = Option<u64>;
    fn apply(&mut self, position: u64, cmd: Cmd) -> CmdResp {
        match uc_service::StateMachine::apply(&mut self.0, position, cmd) {
            CmdResp::CasResult(b) => CmdResp::CasResult(!b),
            other => other,
        }
    }
    fn query(&self, q: ()) -> Option<u64> { uc_service::StateMachine::query(&self.0, q) }
    fn last_applied(&self) -> Option<u64> { uc_service::StateMachine::last_applied(&self.0) }
}
impl uc_service::SnapshotStateMachine for Corrupt<RegisterSm> { /* forward all three exactly as Slow does */ }
```
(The `StateMachine` vs blanket-`RawStateMachine` ambiguity means every delegating call is written UFCS, as above — the M14d lesson.)

`submit_all_cmd` is `submit_cmd` with `client.submit_all::<C, R>(cmd)` in place of `client.submit::<C, R>(cmd)` and `SubmitOutcome<Vec<(u8, R)>>` as the return type; the error classification arms are identical (copy them — the `ServiceNotDeclared` comment becomes "this harness only ever fans in over declared ids").

`service_applied`:
```rust
    /// M14c2: FSM `id`'s `applied` byte position as published on `node`'s cnc page.
    pub fn service_applied(&self, node: usize, id: u8) -> u64 {
        let cnc = uc_log::cnc::CncPage::open_file(&self.nodes[node].instance_dir.join("cnc2.dat"), APP).expect("open cnc");
        cnc.service_slot(id as usize).applied.load_acquire()
    }
```

- [ ] **Step 4: Verify** — `cargo test -p uc_node --test lin_v2 two_fsm_smoke 2>&1 | tail -3` → ok; then the untouched suites: `cargo test -p uc_node --test lin_v2 --test lin_partition_v2 --test elle_v2 2>&1 | grep -E '^test result'` (elle's are `#[ignore]`d — 0 run is fine); `cargo test --workspace 2>&1 | grep -E '^test result' | grep -v ' 0 failed'` → no lines; clippy clean.

- [ ] **Step 5: Commit** — `git commit -am "test(lincheck_v2): FsmSet::Two — a second service per node, Slow/Corrupt wrappers, submit_all_cmd into two histories, service_applied; two_fsm_smoke (M14c2 T2)"`

---

### Task 3: `lin_v2::two_fsm_bounded` / `two_fsm_lockstep` + the oracle-bites test

**Files:**
- Modify: `uc_node/tests/lincheck_v2/mod.rs` (new `worker2`, `spawn_workers2`), `uc_node/tests/lin_v2.rs` (three tests)

**Interfaces:**
- Produces: `pub fn spawn_workers2(dirs, h0: &Arc<History>, h1: &Arc<History>, equiv_failures: &Arc<AtomicU64>, stop, last_seen, seed, throttle, n_workers) -> Vec<JoinHandle<()>>` — like `spawn_workers` but every write/CAS goes through `submit_all_cmd`, is recorded into both histories, and any unequal response pair increments `equiv_failures` (and is recorded as `Indeterminate` in both, so the checker is not fed a lie). Reads: FSM 0 via `read_leader` into `h0`; FSM 1 via `client.query_linearizable_on(1, &())` (a `read_leader_on(conn, id, …)` twin of `read_leader`) into `h1`.

- [ ] **Step 1: Write the failing tests**

```rust
fn run_two_fsm(label: &str, lag: uc_node::FsmLag, seed: u64) {
    const TARGET_OPS: usize = 600;
    const N_WORKERS: u32 = 3;
    const THROTTLE: Duration = Duration::from_millis(20);
    const FAULT_PERIOD: Duration = Duration::from_millis(1200);
    let budget = Duration::from_secs(std::env::var("UC2_LIN_BUDGET_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(115));
    let ccfg = ClusterCfg {
        purge: PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        journal_segment_bytes: 16 * 1024,
        snapshot_interval_bytes: 32 * 1024,
        services: lincheck_v2::FsmSet::Two { lag },
        ..ClusterCfg::default()
    };
    let _g = serialize();
    let dir = tempdir();
    let mut cluster: LinClusterV2 = LinClusterV2::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    cluster.await_single_serving(30);
    let dirs = Arc::new(cluster.dirs());
    let (h0, h1) = (Arc::new(History::default()), Arc::new(History::default()));
    let equiv_failures = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));
    let handles = lincheck_v2::spawn_workers2(&dirs, &h0, &h1, &equiv_failures, &stop, &last_seen, seed, THROTTLE, N_WORKERS);
    let mut frng = StdRng::seed_from_u64(seed ^ 0xFA17);
    let start = Instant::now();
    while History::ok_count(&h0.snapshot()) < TARGET_OPS || cluster.max_archive_first_base() == 0 {
        std::thread::sleep(FAULT_PERIOD);
        cluster.supervise_services();
        match frng.random_range(0..3u8) {
            0 => cluster.kill_and_restart_leader(),
            1 => cluster.crash_and_restart_leader_service(),
            _ => cluster.crash_and_restart_random_follower_service(&mut frng),
        }
        assert!(start.elapsed() < budget, "[{label}] budget exhausted: ok={} floor={}", History::ok_count(&h0.snapshot()), cluster.max_archive_first_base());
    }
    stop.store(true, Ordering::Relaxed);
    join_workers(handles);
    cluster.stop();
    assert_eq!(equiv_failures.load(Ordering::Relaxed), 0, "[{label}] replication-equivalence violated");
    for (id, h) in [(0u8, h0), (1u8, h1)] {
        let entries = Arc::try_unwrap(h).map(History::into_entries).unwrap_or_else(|a| a.snapshot());
        match check_register(&entries) {
            Verdict::Linearizable => {}
            v => panic!("[{label}] FSM {id}: {v:?} (seed={seed})"),
        }
    }
}

#[test] fn two_fsm_bounded()  { run_two_fsm("two_fsm_bounded",  uc_node::FsmLag::Bounded(64 * 1024), std::env::var("LIN_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(0x14c2)); }
#[test] fn two_fsm_lockstep() { run_two_fsm("two_fsm_lockstep", uc_node::FsmLag::Lockstep,            std::env::var("LIN_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(0x14c3)); }

/// The oracle must bite: FSM 1 = `Corrupt<RegisterSm>` flips every CAS, so the
/// first CAS `submit_all` disagrees and `equiv_failures` is non-zero.
#[test]
#[should_panic(expected = "replication-equivalence violated")]
fn two_fsm_oracle_bites() {
    let _g = serialize();
    let dir = tempdir();
    let ccfg = ClusterCfg { services: lincheck_v2::FsmSet::Two { lag: uc_node::FsmLag::Bounded(64 * 1024) }, ..ClusterCfg::default() };
    let cluster: LinClusterV2<uc_lincheck::register::RegisterSm, lincheck_v2::Corrupt<uc_lincheck::register::RegisterSm>> =
        LinClusterV2::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    let leader = cluster.await_single_serving(30);
    let client = cluster.client(leader);
    let _: Vec<(u8, CmdResp)> = client.submit_all(&Cmd::Write(1)).unwrap();
    let r: Vec<(u8, CmdResp)> = client.submit_all(&Cmd::Cas { old: 1, new: 2 }).unwrap();
    cluster.stop();
    assert_eq!(r[0].1, r[1].1, "replication-equivalence violated: {r:?}");
}
```

- [ ] **Step 2: Run to verify they fail** — `cargo test -p uc_node --test lin_v2 two_fsm 2>&1 | grep -E 'error\[' | head` → `spawn_workers2` not found.

- [ ] **Step 3: Implement `worker2`/`spawn_workers2`/`read_leader_on`** in the harness — `worker2` is `worker` with: writes/CAS via `submit_all_cmd::<_, CmdResp>`; on `SubmitOutcome::Ok(v)`: `let (a, b) = (&v[0].1, &v[1].1); if a != b { equiv_failures.fetch_add(1); record Indeterminate in both } else { record Ok(RegResp::…) in both }` (CAS `last_seen` update from `a`); reads alternate FSM 0 (`read_leader` → `h0`) and FSM 1 (`read_leader_on(conn, 1, …)` → `h1`). `read_leader_on` = `read_leader` with `client.query_linearizable_on::<Q, QR>(id, q)`.

- [ ] **Step 4: Verify** — `cargo test -p uc_node --test lin_v2 two_fsm 2>&1 | grep -E '^test |test result'` → `two_fsm_smoke`, `two_fsm_bounded`, `two_fsm_lockstep` ok; `two_fsm_oracle_bites` ok (should_panic satisfied). Run each seeded twice (`LIN_SEED=1`, `=2`). Workspace suite green; clippy clean.

- [ ] **Step 5: Commit** — `git commit -am "test(lin_v2): two_fsm_bounded/two_fsm_lockstep — per-FSM WGL + replication-equivalence under failover and purge churn; the oracle is shown to bite (M14c2 T3)"`

---

### Task 4: The slow-FSM oracle — `two_fsm_slow` and `two_fsm_slow_lockstep`

**Files:**
- Modify: `uc_node/tests/lin_v2.rs`

**Interfaces:**
- Consumes: `LinClusterV2<RegisterSm, Slow<RegisterSm, 200>>` (200 µs per apply: on this box `RegisterSm` applies in well under 20 µs, so FSM 1 alone is ≥ 10× slower than FSM 0 unthrottled — comfortably "the limiter"); `service_applied(node, id)`; `cluster.leader()`.
- Produces: `fn sample_lag(cluster, stop) -> JoinHandle<Vec<(Instant, u64, u64)>>` sampling `(applied_0, applied_1)` on the current leader every 50 ms.

- [ ] **Step 1: Write the failing test**

```rust
fn run_two_fsm_slow(label: &str, lag: uc_node::FsmLag, seed: u64) {
    const SECS: u64 = 20;
    const N_WORKERS: u32 = 4;
    let _g = serialize();
    let dir = tempdir();
    let ccfg = ClusterCfg { services: lincheck_v2::FsmSet::Two { lag }, ..ClusterCfg::default() };
    let cluster: LinClusterV2<uc_lincheck::register::RegisterSm, lincheck_v2::Slow<uc_lincheck::register::RegisterSm, 200>> =
        LinClusterV2::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    let leader = cluster.await_single_serving(30);
    let dirs = Arc::new(cluster.dirs());
    let (h0, h1) = (Arc::new(History::default()), Arc::new(History::default()));
    let equiv = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));
    let handles = lincheck_v2::spawn_workers2(&dirs, &h0, &h1, &equiv, &stop, &last_seen, seed, Duration::ZERO, N_WORKERS);
    // sampler: (t, applied_0, applied_1) on the leader every 50 ms; no faults in this run
    let samples = {
        let stop = Arc::clone(&stop);
        let dir0 = dirs[leader].clone();
        std::thread::spawn(move || {
            let cnc = uc_log::cnc::CncPage::open_file(&dir0.join("cnc2.dat"), "uc2-lincheck-v2").expect("cnc");
            let mut v = Vec::new();
            while !stop.load(Ordering::Relaxed) {
                v.push((Instant::now(), cnc.service_slot(0).applied.load_acquire(), cnc.service_slot(1).applied.load_acquire()));
                std::thread::sleep(Duration::from_millis(50));
            }
            v
        })
    };
    std::thread::sleep(Duration::from_secs(SECS));
    stop.store(true, Ordering::Relaxed);
    join_workers(handles);
    let samples = samples.join().unwrap();
    cluster.stop();
    assert_eq!(equiv.load(Ordering::Relaxed), 0, "[{label}] replication-equivalence violated");
    // (i) the bound at every sample
    let bound = match lag { uc_node::FsmLag::Bounded(b) => b, uc_node::FsmLag::Lockstep => 288 };
    for (t, a0, a1) in &samples {
        assert!(a0.saturating_sub(*a1) <= bound, "[{label}] lag {} > bound {bound} at {:?}", a0.saturating_sub(*a1), t);
    }
    // (ii) convergence over the second half (ruling 2026-08-30)
    let half = samples.len() / 2;
    let (t0, a0_0, a1_0) = samples[half];
    let (t1, a0_1, a1_1) = *samples.last().unwrap();
    let dt = (t1 - t0).as_secs_f64();
    let (r0, r1) = ((a0_1 - a0_0) as f64 / dt, (a1_1 - a1_0) as f64 / dt);
    assert!(r1 > 0.0, "[{label}] FSM 1 made no progress in the second half");
    let ratio = r0 / r1;
    assert!((0.9..=1.1).contains(&ratio), "[{label}] FSM 0 rate {r0:.0} B/s vs FSM 1 {r1:.0} B/s: ratio {ratio:.3} outside [0.9, 1.1]");
    eprintln!("[{label}] samples={} rate0={r0:.0} rate1={r1:.0} ratio={ratio:.3}", samples.len());
}

#[test] fn two_fsm_slow()          { run_two_fsm_slow("two_fsm_slow",          uc_node::FsmLag::Bounded(64 * 1024), 0x51); }
#[test] fn two_fsm_slow_lockstep() { run_two_fsm_slow("two_fsm_slow_lockstep", uc_node::FsmLag::Lockstep,           0x52); }
```
(`APP` in the harness is the string the sampler passes to `open_file`; use the harness's `APP` constant — make it `pub` — rather than the literal.)

- [ ] **Step 2: Run to verify it fails** — compile error on `pub APP` visibility or the sampler; fix by exporting `APP`. Then the first real run: if (ii) fails with a ratio far above 1.1, that is a **finding** about bounded mode, not a test bug — record it, do not loosen the bar; consult the controller.

- [ ] **Step 3: Verify** — both tests green twice; `two_fsm_slow`'s printed ratio recorded in the commit message. Workspace suite green; clippy clean.

- [ ] **Step 4: Commit** — `git commit -am "test(lin_v2): the slow-FSM oracle — lag bound at every sample and rate convergence within 10% in the second half, bounded and lockstep (M14c2 T4)"`

---

### Task 5: `lin_partition_v2::minority_partition_and_heal_two_fsm`

**Files:**
- Modify: `uc_node/tests/lin_partition_v2.rs` (`Run` gains an optional second history; one new test)

- [ ] **Step 1: Write the failing test** — a copy of `minority_partition_and_heal` whose `run_minority(seed, ccfg)` is called with `ClusterCfg { services: lincheck_v2::FsmSet::Two { lag: uc_node::FsmLag::Bounded(64 * 1024) }, ..ClusterCfg::default() }` and a `two_fsm: true` flag that makes `Run` use `spawn_workers2` (two histories + `equiv` counter) and check both histories with `check_or_transient(…, "minority-two-fsm/0")` and `/1`, asserting `equiv == 0` before the checks.

- [ ] **Step 2: Run to fail** (`run_minority` has no such parameter) → implement by threading `two_fsm` through `Run::start_cfg` (a `Run` field `h1: Option<Arc<History>>`, `equiv: Arc<AtomicU64>`), keeping every existing scenario's path identical when `two_fsm == false`.

- [ ] **Step 3: Verify** — `cargo test -p uc_node --test lin_partition_v2 two_fsm 2>&1 | tail -3` ok; the four existing scenarios still ok; clippy clean.

- [ ] **Step 4: Commit** — `git commit -am "test(lin_partition_v2): minority partition + heal with two FSMs — per-FSM WGL + equivalence before and after heal (M14c2 T5)"`

---

### Task 6: Hard-crash with two FSMs

**Files:**
- Modify: `examples/uc_crashtest/tests/common/mod.rs` (`spawn_service_id`, `spawn_node_with_services`), `examples/uc_crashtest/tests/hard_crash.rs` (`submit_all_cmd`, `worker2`, two tests)

**Interfaces:**
- Consumes: the node bin's `--services`/`--fsm-lag` (Task 1); the service bin's `--service-id`; `Reap`; `wait_for_ready`; `warmup_write`; `assert_linearizable(entries, dump_prefix, tag)`.
- Produces:
  ```rust
  pub fn spawn_service_id(instance_dir: &Path, id: u8) -> Reap            // --service-id id
  pub fn spawn_node_with_services(instance_dir: &Path, services: &str, fsm_lag: &str) -> Reap
  ```

- [ ] **Step 1: Write the failing tests** (in `hard_crash.rs`, after `linearizable_under_service_sigkill`)

```rust
/// M14c2: SIGKILL FSM 1 mid-load (five times), respawn it; both FSMs'
/// histories stay linearizable and every `submit_all` pair stayed equal.
#[test]
fn two_fsm_service_sigkill() {
    shorten_client_timeout();
    let seed: u64 = std::env::var("LIN_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    let tmp = tempdir();
    let inst = tmp.path().join("inst");
    std::fs::create_dir_all(&inst).unwrap();
    let _node = spawn_node_with_services(&inst, "0,1", "65536");
    wait_for_ready(&inst, Duration::from_secs(10));
    let _svc0 = spawn_service_id(&inst, 0);
    let svc1 = Arc::new(Mutex::new(Some(spawn_service_id(&inst, 1))));
    let dir = Arc::new(inst.clone());
    let (h0, h1) = (Arc::new(History::default()), Arc::new(History::default()));
    let equiv = Arc::new(AtomicU64::new(0));
    let last_seen = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    warmup_write(&inst, &h0, &last_seen);
    let handles = spawn_workers2(&dir, &h0, &h1, &equiv, &last_seen, &stop, seed, Duration::from_millis(7), 3);
    for _ in 0..5 {
        std::thread::sleep(Duration::from_millis(700));
        let mut g = svc1.lock().unwrap();
        g.take();                                   // SIGKILL + reap FSM 1
        *g = Some(spawn_service_id(&inst, 1));
    }
    std::thread::sleep(Duration::from_secs(1));
    stop.store(true, Ordering::Relaxed);
    join_workers(handles);
    assert_eq!(equiv.load(Ordering::Relaxed), 0, "replication-equivalence violated");
    assert_linearizable(&Arc::try_unwrap(h0).map(History::into_entries).unwrap_or_else(|a| a.snapshot()), "two_fsm_service_sigkill_fsm0", "fsm0");
    assert_linearizable(&Arc::try_unwrap(h1).map(History::into_entries).unwrap_or_else(|a| a.snapshot()), "two_fsm_service_sigkill_fsm1", "fsm1");
}

/// M14c2: SIGKILL the NODE with both FSMs attached; respawn node then both
/// services (three times). Both histories linearizable across every restart.
#[test]
fn two_fsm_node_sigkill() {
    // Same skeleton as `node_sigkill_recovery_once` but the node is spawned with
    // `spawn_node_with_services(&inst, "0,1", "65536")`, both services via
    // `spawn_service_id`, workers via `spawn_workers2`, the restart loop kills
    // node → service0 → service1 (in that order), waits for a FRESH instance id
    // (`wait_for_fresh_instance`), respawns node then service0 then service1,
    // and the end checks both histories + `equiv == 0`.
    …
}
```

- [ ] **Step 2: Run to fail** — `cargo test -p uc_crashtest --features hard-crash-tests --test hard_crash two_fsm 2>&1 | grep 'error\[' | head -3` → missing helpers.

- [ ] **Step 3: Implement** — `common/mod.rs`:
```rust
pub fn spawn_service_id(instance_dir: &Path, id: u8) -> Reap {
    let mut cmd = Command::new(SERVICE_BIN);
    cmd.arg("--instance-dir").arg(instance_dir).arg("--app-id").arg(APP_ID).arg("--service-id").arg(id.to_string());
    let child = cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit()).spawn().unwrap_or_else(|e| panic!("spawn {SERVICE_BIN}: {e}"));
    Reap(child)
}
pub fn spawn_node_with_services(instance_dir: &Path, services: &str, fsm_lag: &str) -> Reap {
    let mut cmd = Command::new(NODE_BIN);
    cmd.arg("--instance-dir").arg(instance_dir).arg("--app-id").arg(APP_ID).arg("--services").arg(services).arg("--fsm-lag").arg(fsm_lag);
    let child = cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit()).spawn().unwrap_or_else(|e| panic!("spawn {NODE_BIN}: {e}"));
    Reap(child)
}
```
`hard_crash.rs`: `submit_all_cmd(conn, cmd, deadline) -> SubmitOutcome2` (its own `SubmitOutcome` carries `CmdResp`; add a variant type `SubmitOutcome2 { Ok(Vec<(u8, CmdResp)>), Indeterminate, Fatal(ClientError) }` with the same classification as `submit_cmd`), `worker2` and `spawn_workers2` mirroring Task 3's harness versions but over this file's `Conn`. Write the full `two_fsm_node_sigkill` body per the comment (it is `node_sigkill_recovery_once`'s loop with the two-service spawn/kill order).

- [ ] **Step 4: Verify** — `cargo test -p uc_crashtest --features hard-crash-tests --test hard_crash 2>&1 | grep -E '^test |test result'` → all (old + 2 new) ok, twice; clippy `-p uc_crashtest --features hard-crash-tests` clean.

- [ ] **Step 5: Commit** — `git commit -am "test(crashtest): two-FSM SIGKILL scenarios — FSM 1 killed mid-load, and the node with both attached; per-FSM linearizability + equivalence across every restart (M14c2 T6)"`

---

### Task 7: Elle clean tier with two FSMs

**Files:**
- Modify: `uc_node/tests/elle_v2.rs` (`elle_worker2`, `run_pass2`, `elle_quiet_two_fsm`), `scripts/elle_check.sh` (default passes)

- [ ] **Step 1: Add the test** — `elle_quiet_two_fsm` = `elle_quiet` with `ClusterCfg { services: FsmSet::Two { lag: Bounded(64 KiB) }, .. }`, workers `elle_worker2` (appends via `submit_all_cmd::<_, LaResp>` recorded into **two** `EdnRecorder`s — one per FSM — with an unequal pair recorded as `:info` in both and counted in `equiv`; reads alternate FSM 0 (`read_leader`) and FSM 1 (`read_leader_on(…, 1, …)`), each into its own recorder), and `run_pass2` writing `history.edn` under `<elle_dir>/quiet_two_fsm/fsm0/` and `/fsm1/` plus the `seed`/`crypto` sidecars, asserting `equiv == 0`.

- [ ] **Step 2: Register the pass** — `scripts/elle_check.sh`: default `PASSES=(quiet failover partition purge reconfig quiet_two_fsm)`; the history loop handles a pass whose dir holds `fsm0/history.edn` and `fsm1/history.edn` by adjudicating each (both models) — implement as: `for hist in "$ELLE_DIR/$pass"/history.edn "$ELLE_DIR/$pass"/fsm*/history.edn; do [ -f "$hist" ] || continue; …; done` around the existing verdict block, preserving the crypto-sidecar check per pass.

- [ ] **Step 3: Verify** — `ELLE_DIR=$HOME/.cache/uc2-elle-m14c2 ELLE_TARGET_OPS=8000 scripts/elle_check.sh quiet_two_fsm` → both histories `true` under both models; `scripts/elle_check.sh quiet` still passes (unchanged path). Record the elle-cli verdict lines in the commit message.

- [ ] **Step 4: Commit** — `git commit -am "test(elle): the clean tier with two FSMs — one history per FSM, both models; elle_check.sh adjudicates per-FSM histories (M14c2 T7)"`

---

### Task 8: The lockstep experiment — protocol, record, decision (and the conditional fix)

**Files:**
- Create: `scripts/lockstep_oversub.sh`, `docs/benchmarks/uc2-m14c2-lockstep-oversubscription-2026-08-30.md`
- Possibly modify: `uc_service/src/apply.rs` (only if the decision rule says defect)

**Interfaces:**
- Consumes: `uc_node/examples/apply_bench` (`--root --fsms --mode --secs --warmup-secs`; prints `fsm={id} applied_frames/s=… lag_waits=…` and `hop: min applied_frames/s=…`), `taskset`, `stress-ng` (both present on the box), the constants `LAG_WAIT_SPINS=256`, `LAG_WAIT_YIELDS=2048` (`apply.rs:527-528`), `APPLY_IDLE = Sleep(50 µs)` (`uc_service/src/lib.rs:81`).

- [ ] **Step 1: The script**

```bash
#!/usr/bin/env bash
# lockstep under CPU oversubscription: reproduce M14 gate row e on the dev box.
# Usage: scripts/lockstep_oversub.sh [--cores 0-1] [--spinners N] [--secs 8]
# Runs apply_bench --fsms 2 --mode lockstep (a) unconstrained, (b) pinned to
# --cores with N stress-ng cpu spinners on the same cores. Prints both
# `hop: min applied_frames/s` lines. Smoke only — never a bar.
set -euo pipefail
CORES="0-1"; SPINNERS=2; SECS=8; ROOT="$HOME/.cache/uc2-apply-bench"
while [ $# -gt 0 ]; do case "$1" in --cores) CORES="$2"; shift 2;; --spinners) SPINNERS="$2"; shift 2;; --secs) SECS="$2"; shift 2;; *) echo "unknown $1" >&2; exit 2;; esac; done
cargo build --release -p uc_node --example apply_bench >/dev/null
BIN="${CARGO_TARGET_DIR:-$HOME/.cache/cargo-target}/release/examples/apply_bench"
run() { "$BIN" --root "$ROOT" --fsms 2 --mode lockstep --secs "$SECS" --warmup-secs 1 2>&1 | grep -E 'hop: min|lag_waits'; }
echo "== unconstrained"; run
echo "== pinned to $CORES with $SPINNERS spinner(s)"
taskset -c "$CORES" stress-ng --cpu "$SPINNERS" --timeout $((SECS + 3)) --quiet & SP=$!
sleep 0.5; taskset -c "$CORES" "$BIN" --root "$ROOT" --fsms 2 --mode lockstep --secs "$SECS" --warmup-secs 1 2>&1 | grep -E 'hop: min|lag_waits'
wait $SP || true
```

- [ ] **Step 2: Run the ladder** — unconstrained; `--cores 0-1 --spinners 0` (3 busy threads on 2 cores: driver + 2 apply); `--spinners 1`; `--spinners 2`; `--cores 0-3 --spinners 4`. Record every `hop: min applied_frames/s` and `lag_waits` in the bench doc's table. Target reproduction: a rung at ≤ 30 k frames/s. If none reproduces, widen (`--cores 0`, more spinners) before concluding "does not reproduce".

- [ ] **Step 3: Bisect** (only once reproduced), one variant at a time, each measured at the reproducing rung and unconstrained, with a same-source rebuild control first (build `main` twice, run both — that spread is the resolution):
  - (a) `LAG_WAIT_YIELDS = 8192` then `32768`;
  - (b) never sleep on a live sibling: in `lockstep_wait`, replace the fixed ladder with `loop { … yield …; if sibling heartbeat older than 1 s → return None }` (the heartbeat is `service_slot(sib).heartbeat_ns`; `unix_ns()` exists in the file) — i.e. the ladder ends only when the sibling looks dead;
  - (c) futex: after `LAG_WAIT_SPINS`, `futex_wait` on the sibling's `applied` word (`uc_protocol::ring::futex` has the wait/wake primitives; the sibling would need a `futex_wake` after each `applied` store — a second change in the publish path; measure the cost at N=1 too).
  Apply the decision rule verbatim (Global Constraints). Write the doc: table, the rule, the verdict, and — if defect — the chosen patch and its N=1/N=2 unconstrained numbers vs the control.

- [ ] **Step 4: If defect** — land the smallest passing variant in `apply.rs` with its doc comment updated (the M14a paragraph at `apply.rs:621-633` gets a second paragraph naming this experiment), `cargo test -p uc_service` and the full suite green, `two_fsm_slow_lockstep` (Task 4) still green. **If envelope** — add the sentence with the number to `docs/reference/configuration.md`'s `[services]` lockstep paragraph and `docs/reference/limits.md`.

- [ ] **Step 5: Commit** — `git commit -am "bench(m14c2): lockstep under oversubscription — <reproduced at N k/s | not reproduced>; decision: <defect: fix … | envelope>; script + record (M14c2 T8)"`

---

### Task 9: Pin the fleet rig (user-gated run)

**Files:**
- Modify: `bench-infra/scripts/m12_fleet_gate.py` (`unit_start_cmd`/`start_unit` gain `cpus=None`), `bench-infra/scripts/m14_fleet_gate.py` (`--pin`), `bench-infra/scripts/m14_ab_27_vs_28.py` (`--pin`), `docs/benchmarks/uc2-m14c2-fleet-pinning-<date>.md`

- [ ] **Step 1: Driver change** — `unit_start_cmd(host, unit, args, nofile=False, cpus=None)`: when `cpus`, add `-p CPUAffinity={cpus} ` after the `LimitNOFILE` flag. `start_unit` and `start_units_batch` forward it. Pin map for an 8-vCPU `c6id.2xlarge` (siblings are `(i, i+4)` on this instance family — verify with `lscpu -e` on a host and record it): node `0-3`? No — the four agents must not share physical cores with each other: node → `0,1,2,3` is four *siblings-of-each-other* pairs' first halves; use node `0,1,4,5` (two physical cores, both threads each), service0 `2`, service1 `6`, client `3,7`. Encode as constants with the `lscpu` evidence in a comment; `--pin` applies it, default off.

- [ ] **Step 2: Validation run (needs the user's go; ~$3)** — `python3 bench-infra/scripts/m14_ab_27_vs_28.py --fleet --reps 4 --pin --hosts …` on ONE binary (`--tree27` pointed at `main` too, so A = B = main; the driver's A/B machinery gives 8 arms), then the same `--reps 4` unpinned. Record both spreads. Adopt (`--pin` default on in `m14_fleet_gate.py`, and the gate doc's "Reading the rules" says so) iff the pinned spread < 5 % (spec §16.5).

- [ ] **Step 3: Commit** — `git commit -am "bench(fleet): -p CPUAffinity per unit (--pin) — pinned spread <X>% vs unpinned <Y>% on 8 arms; <adopted as default | not adopted> (M14c2 T9)"`

---

### Task 10a: M14c deferrals — `uc_net`

**Files:** `uc_net/src/sender.rs` (`try_open_snap_session` at `:1016`), `uc_net/src/receiver.rs` (`snap_chunk` `:1770`, `snap_upkeep` `:1981`, intake), `uc_node/src/obs/metrics.rs`, `uc_net/tests/snapshot_session.rs`

- [ ] **Step 1: Sender** — unit tests for `try_open_snap_session`'s three refusal paths (no artifact for a declared id, a `.part` in the store, an unreadable file), each asserting the named log/counter; skip serving a repair NAK whose range lies inside an artifact whose `SNAP_BEGIN` has not been sent (test: NAK before BEGIN → no `SNAP_CHUNK` emitted); a `snap_open_failed` counter bumped on the `File::open` TOCTOU path, exported as `uc2_snapshot_open_failed_total`.
- [ ] **Step 2: Receiver** — an intake timeout: an intake with no chunk for 60 s (constant `SNAP_INTAKE_TIMEOUT_NS`) is abandoned (`.part` unlinked, counted `uc2_snapshot_intake_abandoned_total`); an undecodable BEGIN from the live peer counted once per session (`uc2_snapshot_begin_undecodable_total`); `snap_chunk`'s `seek`/`write_all` failure counted in the existing `snap_intake_io_failures` and logged once; `snap_upkeep` re-drives at most once per `SNAP_REDRIVE_INTERVAL_NS = 250 ms` (a `last_publish_try_ns` field). Tests in `uc_net/tests/snapshot_session.rs` for the timeout and the cadence (virtual `now` is already injectable there).
- [ ] **Step 3: Verify** — `cargo test -p uc_net -p uc_node 2>&1 | grep -E '^test result' | grep -v ' 0 failed'` → none; the new counters appear in `uc_node/src/obs/metrics.rs`'s `CONTRACT_SERIES` and `every_contract_series_is_present` passes; clippy clean.
- [ ] **Step 4: Commit** — `git commit -am "fix(net): M14c deferrals — snapshot session refusal tests, NAK-before-BEGIN skip, snap_open_failed, intake timeout, undecodable-BEGIN count, snap_chunk write-failure count, re-drive cadence (M14c2 T10a)"`

---

### Task 10b: M14c deferrals — service, node, ctl, tests, docs

**Files:** `uc_service/src/apply.rs` (`lag_waits` at `:329-333`), `uc_node/src/node.rs` (`note_service_transitions` `:2889`), `uc_node/tests/learner.rs` (`fresh_learner_joins_a_purged_two_fsm_leader_and_both_fsms_converge` `:641`), `uc_ctl/src/main.rs` (`:535`), `docs/how-to/diagnose-a-node.md`, `docs/reference/semver-policy.md`, `packaging/prometheus/uc2-alerts.yml`

- [ ] **Step 1** — `lag_waits` (ruling K): count a bounded-mode episode when `plan()` returns `Wait` for a cap that sits mid-frame too (today only the `Lockstep`/`Bounded` `Wait` edge inside `lockstep_wait` counts); unit test in `apply.rs`'s test module: bounded, cursor at cap mid-frame → `lag_waits` increments once per episode.
- [ ] **Step 2** — `note_service_transitions`: take the two `service_mins` words as parameters instead of re-loading them; the learner test asserts the *session* delivered both artifacts (`snap_sessions == 1` on the voter) and drops the dead disjunct; add the decline-latch test named in the M14c record; `uc2ctl status` prints `fsm_lag=n/a` when `declared == 0`.
- [ ] **Step 3** — docs: wrap the over-long line in `diagnose-a-node.md`; `semver-policy.md` notes the changed `uc_net` signatures; alerts: a one-frame tolerance on `Uc2ServicePinnedAtLagBound` for non-dividing frame sizes + the scenario's `Disclosure` payload note; the harness-page heartbeat assertion made non-vacuous (header-only page must fail it).
- [ ] **Step 4: Verify** — `cargo test --workspace` green; `scripts/m10_alert_fire.sh` still 16/16; clippy clean.
- [ ] **Step 5: Commit** — `git commit -am "fix(service,node,ctl): M14c deferrals — lag_waits counts bounded stalls, transitions take the loaded words, learner session assertion + decline latch, status fsm_lag=n/a, docs/alerts nits (M14c2 T10b)"`

---

### Task 11: Pin the fact — a snapshot shortens a restart only with purge

**Files:**
- Modify: `uc_node/tests/lin_v2.rs` (or a new `uc_node/tests/snapshot_restart.rs` including the harness module the same way)

- [ ] **Step 1: The counting SM** (in the harness module, beside `Slow`)
```rust
pub static INSTALLS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
/// Counts `install_snapshot` calls — the observable for "did reconstruction
/// install the newest artifact or replay the whole journal".
#[derive(Default)]
pub struct InstallCounting(pub RegisterSm);
// StateMachine: forward all three (UFCS). SnapshotStateMachine: forward freeze/stream;
// install_snapshot: INSTALLS.fetch_add(1, Relaxed); then forward.
```
- [ ] **Step 2: The test**
```rust
fn restart_installs(purge: bool) -> u32 {
    let _g = serialize();
    let dir = tempdir();
    let ccfg = ClusterCfg {
        purge: if purge { PurgePolicy::BelowSnapshot { slack_bytes: 0 } } else { PurgePolicy::Disabled },
        journal_segment_bytes: 16 * 1024, snapshot_interval_bytes: 32 * 1024, ..ClusterCfg::default()
    };
    let mut cluster: LinClusterV2<lincheck_v2::InstallCounting> = LinClusterV2::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    let leader = cluster.await_single_serving(30);
    let client = cluster.client(leader);
    for i in 0..3000u64 { let _: CmdResp = client.submit(&Cmd::Write(i)).unwrap(); }   // ≥ several segments
    if purge { let d = Instant::now() + Duration::from_secs(30); while cluster.max_archive_first_base() == 0 && Instant::now() < d { std::thread::sleep(Duration::from_millis(50)); } assert!(cluster.max_archive_first_base() > 0, "purge never advanced"); }
    lincheck_v2::INSTALLS.store(0, Ordering::Relaxed);
    cluster.crash_and_restart_leader_service();
    let _: Option<u64> = client.query_linearizable(&()).unwrap();   // the fresh service is caught up
    cluster.stop();
    lincheck_v2::INSTALLS.load(Ordering::Relaxed)
}
#[test] fn snapshot_restart_installs_only_with_purge() {
    assert_eq!(restart_installs(false), 0, "purge off: reconstruction must replay, never install (replay.rs gap guard)");
    assert_eq!(restart_installs(true), 1, "purge on: the newest artifact is installed exactly once");
}
```
- [ ] **Step 3: Verify** — `cargo test -p uc_node --test lin_v2 snapshot_restart 2>&1 | tail -3` ok; then flip the expectation to prove it bites (`(false) == 1` must fail) and flip back.
- [ ] **Step 4: Commit** — `git commit -am "test(node): a SnapshotPolicy shortens a service restart only together with purge — pinned (replay.rs gap guard; M14d run-1 lesson) (M14c2 T11)"`

---

### Task 12: Release 2.8.1

**Files:** `Cargo.toml` + every intra-workspace `version = "2.8.0"` pin, `packaging/{Dockerfile,compose.yml}`, `docs/how-to/run-a-cluster.md`, `docs/QUICKSTART.md`, `README.md` (download lines + the release table row), `SECURITY.md` (`2.8.x` stays), `docs/VERIFICATION.md` (header; §11 M14 bullet; summary rows for WGL/Elle/crashtest name the two-FSM variants), `RELEASES.md` (2.8.1 section), `docs/releases.md`, `docs/benchmarks/uc2-m14-gate-2026-08-29.md` (coverage statement: pointer to 2.8.1), `.github/workflows/nightly.yml` (no change needed if the new tests are picked up by the existing `cargo test` invocations and `elle_check.sh`'s default passes — verify with the matrix, else add them)

- [ ] **Step 1** — version `2.8.0 → 2.8.1` everywhere `cut-a-release.md` §1 lists (same `grep`/`sed` sweep as M14d T8, historical mentions kept); `cargo metadata … uc_node … .version` → `2.8.1`; build + clippy + `cargo package -p uc_protocol --allow-dirty --no-verify`.
- [ ] **Step 2** — `docs/VERIFICATION.md` §11: replace the "NOT yet covered" bullet with the coverage record: the seven capstones by name and file, the equivalence oracle, the slow-FSM oracle with its criterion, the crashtest scenarios, the Elle two-FSM tier, the lockstep verdict (defect fixed / envelope) with the bench doc, the pinning result; the summary table rows say "…under leader kills, crashes, partitions, purge — single- and two-FSM".
- [ ] **Step 3** — `RELEASES.md` 2.8.1 section on top: one bullet per proof tier added (each linking the test file or bench doc), a **Fixed** bullet (Task 8's fix if any; Task 10a/b's fixes), no **Performance** bullet unless row e was re-measured; the paragraph that 2.8.0's pre-release flag stays and 2.8.1 is Latest; `docs/releases.md` mirror; the M14 gate doc's coverage statement gains "closed by 2.8.1: see VERIFICATION §11".
- [ ] **Step 4** — nightly: `gh workflow run nightly.yml --ref main` after merge; row-g style evidence (ci + nightly ids) into `docs/releases.md`'s 2.8.1 entry.
- [ ] **Step 5** — tag: `git tag -a v2.8.1 -m "ultima_cluster 2.8.1 — multi-service proof pass"`; push; watch `release.yml`; §5 verification as a stranger (checksums, `cosign verify-blob` ×3, `cosign verify` on the image, the quickstart); `gh release edit v2.8.1 --latest`.
- [ ] **Step 6** — on the maintainer's explicit go: `cut-a-release.md` §6, the ordered crates.io publish, one crate at a time, waiting for each to index.
- [ ] **Step 7: Commit the writeup before the tag** — `git commit -am "docs(release): v2.8.1 — the multi-service proof pass: seven two-FSM capstones, lockstep <verdict>, fleet rig pinned, M14c deferrals closed; VERIFICATION §11 rewritten"`

---

## Self-review

**Spec coverage (§16):** 16.1 cut → the task list; 16.2 harness → T1 (`from_cli`), T2 (`FsmSet`, `Slow`, `submit_all_cmd`, `service_applied`, respawns) ✓; 16.3 capstones → T3 (bounded/lockstep + oracle-bites), T4 (slow ×2), T5 (partition), T6 (two crashtest scenarios), T7 (Elle) ✓ — the "shown to fail on an injected divergence" clause → T3's `two_fsm_oracle_bites`, T4's convergence assert (a real finding if it fails), T11's flip; 16.4 → T8 with the rule verbatim; 16.5 → T9; 16.6 → T10a/b + T11; 16.7 → T12; 16.8 acceptance → T12 steps 2–5 + every task's verify step.

**Placeholder scan:** T6's `two_fsm_node_sigkill` body is given as a precise construction recipe over a named existing function (`node_sigkill_recovery_once`) rather than pasted code — acceptable because that function is in the same file the implementer edits; every other code step carries code. T8/T9/T12 commit messages carry `<…>` slots that are filled from measured results, not placeholders for design.

**Type consistency:** `FsmSet::Two { lag: FsmLag }` (T2) used identically in T3/T4/T5/T11; `LinClusterV2<SM, SM1>` with `Slow<RegisterSm, 200>` (T4) and `Corrupt<RegisterSm>` (T3) and `InstallCounting` (T11, single-generic form); `spawn_workers2(dirs, h0, h1, equiv, stop, last_seen, seed, throttle, n)` — T3's harness signature vs T6's crashtest-local twin `spawn_workers2(dir, h0, h1, equiv, last_seen, stop, …)` deliberately mirrors that file's existing `spawn_workers(dir, history, last_seen, stop, …)` argument order; `service_applied(node, id)` (T2) used in T2's smoke and T4's sampler (which opens the page directly for the 50 ms loop — same slot, same `load_acquire`); `spawn_service_id`/`spawn_node_with_services` (T6) only in T6; `ServicesConfig::from_cli(Option<&str>, Option<&str>)` (T1) used by both bins.
