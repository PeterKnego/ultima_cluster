# Linearizability Test Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A CI-runnable, seeded linearizability regression test that drives a concurrent CAS-register workload through repeated leader-kill/restart and service-crash failovers and proves the recorded history linearizable with an in-repo Wing-Gong-Lowe checker.

**Architecture:** Three pure units (`model`, `history`, `checker`) with no cluster deps, plus a `register_sm` (the replicated SM), a `cluster` 3-node shmem fault harness (built on the `m2`/`m3` patterns), and the `lin_register` integration test. The checker is generic over a `Model` trait; the cluster keeps quorum (one fault at a time).

**Tech Stack:** Rust, `uc_node`/`uc_service`/`uc_client` (test-support feature), `tempfile`, `rand` (seeded), the existing shmem `ClusterFixture`-style spawn. Test-only — no production-crate changes.

**Scope:** Spec at `docs/superpowers/specs/2026-06-05-linearizability-harness-design.md`. CAS register only; partition/quorum-loss/DST deferred.

---

## File Structure

All under `uc_node/tests/` (its own test binary; runs alone, no cross-binary port contention):

- `tests/lincheck/mod.rs` — `pub mod model; pub mod history; pub mod checker; pub mod register_sm; pub mod cluster;`
- `tests/lincheck/model.rs` — `Model` trait + `RegisterModel` (pure). **Phase 1.**
- `tests/lincheck/history.rs` — `Op`, `RegResp`, `Outcome`, `Entry`, `History` recorder (pure types + a `Mutex`-backed recorder). **Phase 1.**
- `tests/lincheck/checker.rs` — `Verdict`, `check::<M: Model>(...)` WGL search + unit tests. **Phase 1.**
- `tests/lincheck/register_sm.rs` — `RegisterSm: uc_service::StateMachine`. **Phase 2.**
- `tests/lincheck/cluster.rs` — `LinCluster` 3-node shmem harness + faults. **Phase 2.**
- `tests/lin_register.rs` — `mod lincheck;` + the seeded workload + fault scheduler + the test. **Phase 3.**

The pure trio (`model`/`history`/`checker`) imports nothing from the cluster, so it unit-tests in milliseconds independent of any node.

---

## Phase 1 — Pure model, history, and checker (TDD in isolation)

### Task 1.1: `Model` trait + `RegisterModel`

**Files:**
- Create: `uc_node/tests/lincheck/mod.rs`
- Create: `uc_node/tests/lincheck/model.rs`

- [ ] **Step 1: Create the module file**

Create `uc_node/tests/lincheck/mod.rs`:

```rust
//! Test-only linearizability harness: pure model/history/checker + a 3-node
//! shmem fault cluster. See
//! docs/superpowers/specs/2026-06-05-linearizability-harness-design.md.
pub mod model;
pub mod history;
pub mod checker;
```

(`register_sm` and `cluster` are added to this list in Phase 2.)

- [ ] **Step 2: Write `model.rs` with `RegisterModel` + a unit test**

Create `uc_node/tests/lincheck/model.rs`:

```rust
//! Sequential specification of the object under test. Pure — no cluster deps.

use std::hash::Hash;

/// A deterministic sequential spec: given a state and an operation, return the
/// next state and the response a correct single-threaded implementation produces.
pub trait Model {
    type State: Clone + Eq + Hash;
    type Op: Clone;
    type Resp: Clone + Eq + std::fmt::Debug;
    fn init() -> Self::State;
    fn step(state: &Self::State, op: &Self::Op) -> (Self::State, Self::Resp);
}

/// Abstract op against the CAS register (shared with `history`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Write(u64),
    Read,
    Cas { old: u64, new: u64 },
}

/// Abstract response (shared with `history`). `Value` carries the read result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegResp {
    Ack,
    Value(Option<u64>),
    CasOk(bool),
}

/// Single CAS register. State is the current value (None = never written).
pub struct RegisterModel;

impl Model for RegisterModel {
    type State = Option<u64>;
    type Op = Op;
    type Resp = RegResp;
    fn init() -> Option<u64> {
        None
    }
    fn step(state: &Option<u64>, op: &Op) -> (Option<u64>, RegResp) {
        match op {
            Op::Write(v) => (Some(*v), RegResp::Ack),
            Op::Read => (*state, RegResp::Value(*state)),
            Op::Cas { old, new } => {
                if *state == Some(*old) {
                    (Some(*new), RegResp::CasOk(true))
                } else {
                    (*state, RegResp::CasOk(false))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_step_semantics() {
        let s0 = RegisterModel::init();
        assert_eq!(s0, None);
        let (s1, r) = RegisterModel::step(&s0, &Op::Write(5));
        assert_eq!((s1, r), (Some(5), RegResp::Ack));
        let (_s, r) = RegisterModel::step(&s1, &Op::Read);
        assert_eq!(r, RegResp::Value(Some(5)));
        let (s2, r) = RegisterModel::step(&s1, &Op::Cas { old: 5, new: 9 });
        assert_eq!((s2, r), (Some(9), RegResp::CasOk(true)));
        let (s3, r) = RegisterModel::step(&s1, &Op::Cas { old: 7, new: 9 });
        assert_eq!((s3, r), (Some(5), RegResp::CasOk(false)));
    }
}
```

- [ ] **Step 3: Run the model unit test**

Run: `cargo test -p uc_node --test lin_register model::`
Expected: FAIL TO COMPILE — `tests/lin_register.rs` doesn't exist yet. Instead create a minimal stub so the module compiles:

Create `uc_node/tests/lin_register.rs`:
```rust
#[path = "lincheck/mod.rs"]
mod lincheck;
```
Then run: `cargo test -p uc_node --test lin_register model::register_step_semantics`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add uc_node/tests/lincheck/mod.rs uc_node/tests/lincheck/model.rs uc_node/tests/lin_register.rs
git commit -m "test(lincheck): Model trait + RegisterModel sequential spec"
```

---

### Task 1.2: History types + recorder

**Files:**
- Create: `uc_node/tests/lincheck/history.rs`
- Modify: `uc_node/tests/lincheck/mod.rs`

- [ ] **Step 1: Add `history` to the module list**

In `uc_node/tests/lincheck/mod.rs`, add after `pub mod model;`:
```rust
pub mod history;
```

- [ ] **Step 2: Write `history.rs` + a unit test**

Create `uc_node/tests/lincheck/history.rs`:

```rust
//! Operation history recording. Pure types + a thread-safe recorder. The
//! real-time order is captured by a global monotonic sequence stamped at
//! invoke and at return: op A precedes B iff `A.ret < B.invoke`.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

pub use crate::lincheck::model::{Op, RegResp};

/// Observed outcome of one operation.
#[derive(Clone, Debug)]
pub enum Outcome {
    /// Committed and the response was observed; must linearize with this response.
    Ok(RegResp),
    /// May or may not have committed; response not observed.
    Indeterminate,
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub client: u32,
    pub op: Op,
    pub invoke: u64,
    pub ret: u64,
    pub outcome: Outcome,
}

/// Records entries from concurrent workers and stamps the global sequence.
pub struct History {
    seq: AtomicU64,
    entries: Mutex<Vec<Entry>>,
}

impl Default for History {
    fn default() -> Self {
        Self {
            seq: AtomicU64::new(0),
            entries: Mutex::new(Vec::new()),
        }
    }
}

impl History {
    /// Stamp an invoke; call right before firing the op.
    pub fn invoke(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }
    /// Stamp a return and record the completed entry.
    pub fn record(&self, client: u32, op: Op, invoke: u64, outcome: Outcome) {
        let ret = self.seq.fetch_add(1, Ordering::Relaxed);
        self.entries.lock().unwrap().push(Entry {
            client,
            op,
            invoke,
            ret,
            outcome,
        });
    }
    /// Consume the recorded entries.
    pub fn into_entries(self) -> Vec<Entry> {
        self.entries.into_inner().unwrap()
    }
    /// Count of Ok outcomes (for the liveness gate).
    pub fn ok_count(entries: &[Entry]) -> usize {
        entries
            .iter()
            .filter(|e| matches!(e.outcome, Outcome::Ok(_)))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invoke_return_ordering() {
        let h = History::default();
        let i1 = h.invoke();
        h.record(0, Op::Write(1), i1, Outcome::Ok(RegResp::Ack));
        let i2 = h.invoke();
        h.record(0, Op::Read, i2, Outcome::Ok(RegResp::Value(Some(1))));
        let es = h.into_entries();
        assert_eq!(es.len(), 2);
        // First op returned (ret) before second op was invoked.
        assert!(es[0].ret < es[1].invoke);
    }
}
```

- [ ] **Step 3: Run the history unit test**

Run: `cargo test -p uc_node --test lin_register history::invoke_return_ordering`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add uc_node/tests/lincheck/history.rs uc_node/tests/lincheck/mod.rs
git commit -m "test(lincheck): history types + recorder with global real-time seq"
```

---

### Task 1.3: The WGL linearizability checker (the core)

**Files:**
- Create: `uc_node/tests/lincheck/checker.rs`
- Modify: `uc_node/tests/lincheck/mod.rs`

- [ ] **Step 1: Add `checker` to the module list**

In `uc_node/tests/lincheck/mod.rs`, add:
```rust
pub mod checker;
```

- [ ] **Step 2: Write `checker.rs` (Wing-Gong-Lowe search) + the known-good/known-bad unit tests FIRST**

Create `uc_node/tests/lincheck/checker.rs`:

```rust
//! Generic Wing-Gong-Lowe linearizability checker over a `Model`. Pure.
//!
//! Search: repeatedly linearize a real-time-eligible "frontier" op (one whose
//! `invoke` is <= the minimum `ret` of the remaining ops), apply it to the
//! model, and require the model's response to equal the observed response for
//! `Ok` ops. Backtrack on dead-ends. Memoize visited (remaining-set, state).
//!
//! Indeterminate ops: `ret = u64::MAX` (eligible any time at/after invoke),
//! response unconstrained, and OPTIONAL (the search may drop them — they may
//! never have committed). Indeterminate READS carry no information and are
//! dropped before the search; only indeterminate mutations remain.
//!
//! A visited-state budget returns `Inconclusive` rather than a false `Ok`.

use std::collections::HashSet;
use std::hash::Hash;

use crate::lincheck::history::{Entry, Outcome};
use crate::lincheck::model::{Model, Op, RegResp};

#[derive(Debug, PartialEq)]
pub enum Verdict {
    Linearizable,
    Violation,
    Inconclusive,
}

/// Internal normalized op: (op, observed-response-or-None, invoke, ret).
struct NOp {
    op: Op,
    observed: Option<RegResp>, // None = indeterminate (response unconstrained)
    invoke: u64,
    ret: u64,
}

/// Default visited-state budget; exceeding it yields `Inconclusive`.
pub const DEFAULT_BUDGET: u64 = 5_000_000;

/// Check a register history for linearizability against `RegisterModel`.
pub fn check_register(entries: &[Entry]) -> Verdict {
    check_register_with_budget(entries, DEFAULT_BUDGET)
}

pub fn check_register_with_budget(entries: &[Entry], budget: u64) -> Verdict {
    // Normalize: drop indeterminate reads (no information); map outcomes.
    let mut ops: Vec<NOp> = Vec::new();
    for e in entries {
        match (&e.op, &e.outcome) {
            (Op::Read, Outcome::Indeterminate) => continue, // drop
            (_, Outcome::Indeterminate) => ops.push(NOp {
                op: e.op.clone(),
                observed: None,
                invoke: e.invoke,
                ret: u64::MAX,
            }),
            (_, Outcome::Ok(r)) => ops.push(NOp {
                op: e.op.clone(),
                observed: Some(r.clone()),
                invoke: e.invoke,
                ret: e.ret,
            }),
        }
    }
    let n = ops.len();
    let mut remaining: Vec<bool> = vec![true; n];
    let mut visited: HashSet<(Vec<bool>, Option<u64>)> = HashSet::new();
    let mut budget_left = budget;
    let res = search::<RegisterModel>(&ops, &mut remaining, RegisterModel::init(), &mut visited, &mut budget_left);
    match res {
        SearchResult::Ok => Verdict::Linearizable,
        SearchResult::NoLinearization => Verdict::Violation,
        SearchResult::BudgetExceeded => Verdict::Inconclusive,
    }
}

enum SearchResult {
    Ok,
    NoLinearization,
    BudgetExceeded,
}

fn search<M: Model<State = Option<u64>, Op = Op, Resp = RegResp>>(
    ops: &[NOp],
    remaining: &mut Vec<bool>,
    state: Option<u64>,
    visited: &mut HashSet<(Vec<bool>, Option<u64>)>,
    budget: &mut u64,
) -> SearchResult {
    if *budget == 0 {
        return SearchResult::BudgetExceeded;
    }
    *budget -= 1;

    // Done iff no required (Ok) ops remain; leftover indeterminate ops are dropped.
    let any_required = (0..ops.len()).any(|i| remaining[i] && ops[i].observed.is_some());
    if !any_required {
        return SearchResult::Ok;
    }

    // Memo: skip states (remaining-set, model-state) we've explored before.
    let key = (remaining.clone(), state);
    if !visited.insert(key) {
        return SearchResult::NoLinearization;
    }

    // Real-time frontier: candidates are remaining ops whose invoke <= min ret.
    let min_ret = (0..ops.len())
        .filter(|&i| remaining[i])
        .map(|i| ops[i].ret)
        .min()
        .unwrap_or(u64::MAX);

    let mut hit_budget = false;
    for i in 0..ops.len() {
        if !remaining[i] || ops[i].invoke > min_ret {
            continue;
        }
        // Option 1: linearize op i.
        let (state2, resp) = M::step(&state, &ops[i].op);
        let resp_ok = match &ops[i].observed {
            Some(obs) => &resp == obs,
            None => true, // indeterminate: unconstrained
        };
        if resp_ok {
            remaining[i] = false;
            match search::<M>(ops, remaining, state2, visited, budget) {
                SearchResult::Ok => {
                    remaining[i] = true;
                    return SearchResult::Ok;
                }
                SearchResult::BudgetExceeded => hit_budget = true,
                SearchResult::NoLinearization => {}
            }
            remaining[i] = true;
        }
        // Option 2: indeterminate op may be dropped (never committed).
        if ops[i].observed.is_none() {
            remaining[i] = false;
            match search::<M>(ops, remaining, state, visited, budget) {
                SearchResult::Ok => {
                    remaining[i] = true;
                    return SearchResult::Ok;
                }
                SearchResult::BudgetExceeded => hit_budget = true,
                SearchResult::NoLinearization => {}
            }
            remaining[i] = true;
        }
        if *budget == 0 {
            return SearchResult::BudgetExceeded;
        }
    }
    if hit_budget {
        SearchResult::BudgetExceeded
    } else {
        SearchResult::NoLinearization
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lincheck::history::{Entry, Outcome};
    use crate::lincheck::model::{Op, RegResp};

    fn e(client: u32, op: Op, invoke: u64, ret: u64, outcome: Outcome) -> Entry {
        Entry { client, op, invoke, ret, outcome }
    }

    #[test]
    fn sequential_history_is_linearizable() {
        // write(1) ; read->1 ; cas(1,2)->true ; read->2  (non-overlapping)
        let h = vec![
            e(0, Op::Write(1), 0, 1, Outcome::Ok(RegResp::Ack)),
            e(0, Op::Read, 2, 3, Outcome::Ok(RegResp::Value(Some(1)))),
            e(0, Op::Cas { old: 1, new: 2 }, 4, 5, Outcome::Ok(RegResp::CasOk(true))),
            e(0, Op::Read, 6, 7, Outcome::Ok(RegResp::Value(Some(2)))),
        ];
        assert_eq!(check_register(&h), Verdict::Linearizable);
    }

    #[test]
    fn stale_read_after_write_is_violation() {
        // write(1) fully precedes read, but read observed the old value (None).
        let h = vec![
            e(0, Op::Write(1), 0, 1, Outcome::Ok(RegResp::Ack)),
            e(1, Op::Read, 2, 3, Outcome::Ok(RegResp::Value(None))),
        ];
        assert_eq!(check_register(&h), Verdict::Violation);
    }

    #[test]
    fn double_applied_cas_is_violation() {
        // write(1); two concurrent cas(1,2)->true BOTH succeed — impossible.
        let h = vec![
            e(0, Op::Write(1), 0, 1, Outcome::Ok(RegResp::Ack)),
            e(1, Op::Cas { old: 1, new: 2 }, 2, 5, Outcome::Ok(RegResp::CasOk(true))),
            e(2, Op::Cas { old: 1, new: 2 }, 2, 5, Outcome::Ok(RegResp::CasOk(true))),
        ];
        assert_eq!(check_register(&h), Verdict::Violation);
    }

    #[test]
    fn concurrent_overlap_is_linearizable() {
        // write(1) and read overlap; read may observe None OR 1 — both ok.
        let h = vec![
            e(0, Op::Write(1), 0, 5, Outcome::Ok(RegResp::Ack)),
            e(1, Op::Read, 1, 4, Outcome::Ok(RegResp::Value(None))),
        ];
        assert_eq!(check_register(&h), Verdict::Linearizable);
    }

    #[test]
    fn indeterminate_write_may_be_present_or_absent() {
        // An indeterminate write(9) overlaps a later read that saw 1.
        // The checker may DROP the indeterminate write so the read is consistent.
        let h = vec![
            e(0, Op::Write(1), 0, 1, Outcome::Ok(RegResp::Ack)),
            e(1, Op::Write(9), 2, u64::MAX, Outcome::Indeterminate),
            e(0, Op::Read, 3, 4, Outcome::Ok(RegResp::Value(Some(1)))),
        ];
        assert_eq!(check_register(&h), Verdict::Linearizable);
    }

    #[test]
    fn indeterminate_write_that_must_have_happened() {
        // read observed 9, only an indeterminate write(9) could have set it.
        // The checker must be willing to PLACE the indeterminate write.
        let h = vec![
            e(1, Op::Write(9), 0, u64::MAX, Outcome::Indeterminate),
            e(0, Op::Read, 1, 2, Outcome::Ok(RegResp::Value(Some(9)))),
        ];
        assert_eq!(check_register(&h), Verdict::Linearizable);
    }
}
```

- [ ] **Step 3: Run the checker unit tests**

Run: `cargo test -p uc_node --test lin_register checker::`
Expected: PASS — all six: `sequential_history_is_linearizable`, `stale_read_after_write_is_violation`, `double_applied_cas_is_violation`, `concurrent_overlap_is_linearizable`, `indeterminate_write_may_be_present_or_absent`, `indeterminate_write_that_must_have_happened`.

- [ ] **Step 4: Clippy the pure trio**

Run: `cargo clippy -p uc_node --tests -- -D warnings`
Expected: zero warnings (note: this compiles the whole `lin_register` test target; the cluster module doesn't exist yet, so it compiles only `model`/`history`/`checker` + the stub).

- [ ] **Step 5: Commit**

```bash
git add uc_node/tests/lincheck/checker.rs uc_node/tests/lincheck/mod.rs
git commit -m "test(lincheck): WGL linearizability checker + known-good/bad unit tests"
```

---

## Phase 2 — Register SM + 3-node shmem fault harness

### Task 2.1: `RegisterSm` (the replicated state machine)

**Files:**
- Create: `uc_node/tests/lincheck/register_sm.rs`
- Modify: `uc_node/tests/lincheck/mod.rs`

- [ ] **Step 1: Add `register_sm` to the module list**

In `uc_node/tests/lincheck/mod.rs`, add:
```rust
pub mod register_sm;
```

- [ ] **Step 2: Write `register_sm.rs`**

Create `uc_node/tests/lincheck/register_sm.rs`:

```rust
//! The replicated CAS-register state machine the cluster runs. Mirrors the
//! `Counter` test SM shape in m2/m3. `Read` is a Query; `Write`/`Cas` are
//! Commands.

use std::io::{Read as IoRead, Write as IoWrite};

use serde::{Deserialize, Serialize};
use uc_service::{SnapshotError, StateMachine};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Cmd {
    Write(u64),
    Cas { old: u64, new: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CmdResp {
    WriteAck,
    CasResult(bool),
}

#[derive(Default)]
pub struct RegisterSm {
    value: Option<u64>,
    last_applied: Option<u64>,
}

impl StateMachine for RegisterSm {
    type Command = Cmd;
    type Response = CmdResp;
    type Query = (); // Read
    type QueryResponse = Option<u64>;

    fn apply(&mut self, log_index: u64, cmd: Cmd) -> CmdResp {
        self.last_applied = Some(log_index);
        match cmd {
            Cmd::Write(v) => {
                self.value = Some(v);
                CmdResp::WriteAck
            }
            Cmd::Cas { old, new } => {
                if self.value == Some(old) {
                    self.value = Some(new);
                    CmdResp::CasResult(true)
                } else {
                    CmdResp::CasResult(false)
                }
            }
        }
    }
    fn query(&self, _q: ()) -> Option<u64> {
        self.value
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
    fn build_snapshot(&self, dst: &mut dyn IoWrite) -> Result<u64, SnapshotError> {
        let bytes = bincode::serde::encode_to_vec(
            (self.value, self.last_applied),
            bincode::config::standard(),
        )
        .map_err(|e| SnapshotError::Codec(e.to_string()))?;
        dst.write_all(&bytes)?;
        Ok(self.last_applied.unwrap_or(0))
    }
    fn install_snapshot(&mut self, src: &mut dyn IoRead) -> Result<u64, SnapshotError> {
        let mut buf = Vec::new();
        src.read_to_end(&mut buf)?;
        let ((v, la), _) = bincode::serde::decode_from_slice::<(Option<u64>, Option<u64>), _>(
            &buf,
            bincode::config::standard(),
        )
        .map_err(|e| SnapshotError::Codec(e.to_string()))?;
        self.value = v;
        self.last_applied = la;
        Ok(la.unwrap_or(0))
    }
}
```

- [ ] **Step 3: Build the test target**

Run: `cargo build -p uc_node --tests 2>&1 | tail -3` (the `lin_register` target must compile with the new module).
Expected: clean. (No new test logic yet; this only checks the SM compiles against the `StateMachine` trait.)

- [ ] **Step 4: Commit**

```bash
git add uc_node/tests/lincheck/register_sm.rs uc_node/tests/lincheck/mod.rs
git commit -m "test(lincheck): RegisterSm replicated CAS-register state machine"
```

---

### Task 2.2: `LinCluster` — 3-node shmem spawn + route-to-leader (smoke)

**Files:**
- Create: `uc_node/tests/lincheck/cluster.rs`
- Modify: `uc_node/tests/lincheck/mod.rs`
- Modify: `uc_node/tests/lin_register.rs`

- [ ] **Step 1: Add `cluster` to the module list**

In `uc_node/tests/lincheck/mod.rs`, add:
```rust
pub mod cluster;
```

- [ ] **Step 2: Write `cluster.rs` — spawn + route-to-leader**

Create `uc_node/tests/lincheck/cluster.rs`. This mirrors `m2_multi_node::spawn_3_node_cluster` (quoted in the plan's research) but in `IpcMode::Shmem`, with a service + client per node. Each node owns persistent `data_dir`/`instance_dir` `TempDir`s kept alive for the cluster's lifetime (so a killed node can restart against its files).

```rust
//! 3-node shmem cluster with leader-kill/restart + service-crash faults,
//! keeping a quorum at all times. Built on the m2/m3 spawn patterns.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use uc_client::Client;
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, NodeHandle, NodeId,
    PeerSeed, RaftTuning, ServiceRingConfig, TlsConfig,
};
use uc_service::runtime::ServiceConfig;
use uc_service::{Service, ServiceBuilder};

use crate::lincheck::register_sm::{Cmd, CmdResp, RegisterSm};

/// Serialize cluster bring-up across tests in this binary (mirrors m2).
static CLUSTER_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const APP_ID: &str = "lincheck";

struct Node {
    id: NodeId,
    addr: SocketAddr,
    instance_dir: Arc<TempDir>,
    data_dir: Arc<TempDir>,
    svc_data_dir: Arc<TempDir>,
    peers: Vec<PeerSeed>,
    handle: Option<NodeHandle<RegisterSm>>,
    service: Option<Service>,
    client: Option<Arc<Client>>, // Arc so submit/read can clone-out and release the lock before .await
}

pub struct LinCluster {
    nodes: tokio::sync::Mutex<Vec<Node>>, // all methods are &self; faults + workers share Arc<LinCluster>
    _serial: tokio::sync::MutexGuard<'static, ()>,
}
```

**Concurrency model (important — applies to every method below).** Workers and the
fault scheduler all hold `Arc<LinCluster>` and call `&self` methods. The node vec
is behind a `tokio::sync::Mutex`. **Never hold that lock across a network/await
that can block:** `submit_cmd`/`read` lock only to clone the leader's
`Arc<Client>`, then **drop the guard before** `client.submit(...).await`. Fault
methods take the relevant handles out under a brief lock, **drop the guard**,
`await` the shutdown/restart, then re-lock to install the new handles. This keeps
worker concurrency real and lets a fault run while workers are mid-flight.

fn pick_addrs(n: usize) -> Vec<SocketAddr> {
    (0..n)
        .map(|_| {
            let s = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let a = s.local_addr().unwrap();
            drop(s);
            a
        })
        .collect()
}

fn node_config(id: NodeId, instance: &TempDir, data: &TempDir, addr: SocketAddr, peers: Vec<PeerSeed>) -> NodeConfig {
    NodeConfig {
        node_id: id,
        data_dir: data.path().to_owned(),
        raft_listen_addr: addr,
        app_id: APP_ID.into(),
        bootstrap: BootstrapConfig::Peers { peers },
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),
        ipc_mode: IpcMode::Shmem { instance_dir: instance.path().to_owned() },
        client_rings: ClientRingConfig::default(),
        service_rings: ServiceRingConfig::default(),
        log_durability: ultima_journal::Durability::Eventual,
    }
}

async fn wait_for_cnc(dir: &std::path::Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !dir.join("cnc.dat").exists() {
        assert!(Instant::now() < deadline, "cnc.dat never appeared in {dir:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

impl LinCluster {
    /// Bring up a 3-node shmem cluster + one service + one client per node.
    pub async fn start_3() -> Self {
        let serial = CLUSTER_SERIAL.lock().await;
        let addrs = pick_addrs(3);
        let peers: Vec<PeerSeed> = (1..=3u64)
            .zip(addrs.iter())
            .map(|(id, a)| PeerSeed { node_id: id, raft_addr: *a })
            .collect();

        // Create dirs + spawn nodes concurrently (bootstrapper waits for peers).
        let mut nodes: Vec<Node> = Vec::new();
        let mut node_tasks = Vec::new();
        for (i, addr) in addrs.iter().enumerate() {
            let id = (i as u64) + 1;
            let instance = Arc::new(TempDir::new().unwrap());
            let data = Arc::new(TempDir::new().unwrap());
            let svc_data = Arc::new(TempDir::new().unwrap());
            let cfg = node_config(id, &instance, &data, *addr, peers.clone());
            let task = tokio::spawn(async move {
                NodeBuilder::new(cfg, RegisterSm::default()).start().await
            });
            node_tasks.push((id, *addr, instance, data, svc_data, task));
        }
        for (id, addr, instance, data, svc_data, task) in node_tasks {
            let handle = tokio::time::timeout(Duration::from_secs(30), task)
                .await
                .unwrap_or_else(|_| panic!("node {id} start timed out"))
                .expect("node task panic")
                .unwrap_or_else(|e| panic!("node {id} start: {e:?}"));
            nodes.push(Node {
                id,
                addr,
                instance_dir: instance,
                data_dir: data,
                svc_data_dir: svc_data,
                peers: peers.clone(),
                handle: Some(handle),
                service: None,
                client: None,
            });
        }

        // Spawn a service + connect a client per node.
        for n in &mut nodes {
            wait_for_cnc(n.instance_dir.path(), Duration::from_secs(10)).await;
            n.service = Some(spawn_service(n.instance_dir.path(), n.svc_data_dir.path()).await);
            n.client = Some(Arc::new(
                Client::connect(n.instance_dir.path(), APP_ID)
                    .await
                    .expect("client connect"),
            ));
        }

        let cluster = LinCluster { nodes: tokio::sync::Mutex::new(nodes), _serial: serial };
        cluster.wait_for_stable_leader(Duration::from_secs(15)).await;
        cluster
    }

    /// node_id of the current leader, agreed by a majority of live nodes.
    /// Locks briefly; `current_leader()` is a fast shmem read, so holding the
    /// lock across it (not across submit/read) is fine.
    pub async fn leader_id(&self) -> Option<NodeId> {
        let nodes = self.nodes.lock().await;
        for n in nodes.iter() {
            if let Some(h) = &n.handle
                && let Some(l) = h.current_leader().await
            {
                return Some(l);
            }
        }
        None
    }

    /// Clone out the `Arc<Client>` for `id` (caller drops the guard before await).
    async fn client_for(&self, id: NodeId) -> Option<Arc<Client>> {
        let nodes = self.nodes.lock().await;
        nodes.iter().find(|n| n.id == id).and_then(|n| n.client.clone())
    }

    pub async fn wait_for_stable_leader(&self, timeout: Duration) -> NodeId {
        let deadline = Instant::now() + timeout;
        loop {
            assert!(Instant::now() < deadline, "no stable leader within {timeout:?}");
            // All live nodes must agree on the same leader id.
            let mut seen: Option<NodeId> = None;
            let mut agree = true;
            let mut count = 0;
            {
                let nodes = self.nodes.lock().await;
                for n in nodes.iter() {
                    if let Some(h) = &n.handle {
                        count += 1;
                        match h.current_leader().await {
                            Some(l) => match seen {
                                None => seen = Some(l),
                                Some(s) if s == l => {}
                                Some(_) => agree = false,
                            },
                            None => agree = false,
                        }
                    }
                }
            }
            if agree && count >= 2 && let Some(l) = seen {
                return l;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Submit a command to the current leader, retrying on did-not-execute
    /// errors. Returns Ok(resp) | "indeterminate" | propagates fatal.
    pub async fn submit_cmd(&self, cmd: &Cmd) -> SubmitOutcome {
        use uc_client::ClientError as CE;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if Instant::now() > deadline {
                return SubmitOutcome::Indeterminate; // gave up routing; treat as in-limbo
            }
            let Some(lid) = self.leader_id().await else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            // Clone the Arc<Client> and DROP the lock before the network await.
            let Some(client) = self.client_for(lid).await else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            match client.submit::<Cmd, CmdResp>(cmd).await {
                Ok(r) => return SubmitOutcome::Ok(r),
                // did-not-execute → retry against the (new) leader
                Err(CE::NotLeader { .. }) | Err(CE::BackpressureFull) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                // indeterminate → may have committed; do not retry
                Err(CE::Timeout(_)) | Err(CE::ResponseOverwritten)
                | Err(CE::NodeStalled) | Err(CE::ServiceStalled) => {
                    return SubmitOutcome::Indeterminate;
                }
                Err(other) => return SubmitOutcome::Fatal(format!("{other:?}")),
            }
        }
    }

    /// Linearizable read against the current leader.
    pub async fn read(&self) -> ReadOutcome {
        use uc_client::ClientError as CE;
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if Instant::now() > deadline {
                return ReadOutcome::Indeterminate;
            }
            let Some(lid) = self.leader_id().await else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            let Some(client) = self.client_for(lid).await else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            match client.query_linearizable::<(), Option<u64>>(&()).await {
                Ok(v) => return ReadOutcome::Ok(v),
                Err(CE::NotLeader { .. }) | Err(CE::BackpressureFull) => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(CE::Timeout(_)) | Err(CE::ResponseOverwritten)
                | Err(CE::NodeStalled) | Err(CE::ServiceStalled) => {
                    return ReadOutcome::Indeterminate;
                }
                Err(other) => return ReadOutcome::Fatal(format!("{other:?}")),
            }
        }
    }

    pub async fn shutdown(self) {
        // Take everything out under the lock, then await teardown unlocked.
        let mut drained = std::mem::take(&mut *self.nodes.lock().await);
        for n in &mut drained {
            if let Some(c) = n.client.take() {
                // Last Arc owner here; unwrap to call the by-value shutdown.
                if let Ok(c) = Arc::try_unwrap(c) {
                    let _ = c.shutdown().await;
                }
            }
            if let Some(s) = n.service.take() {
                let _ = s.shutdown().await;
            }
            if let Some(h) = n.handle.take() {
                let _ = h.shutdown().await;
            }
        }
    }
}

async fn spawn_service(instance_dir: &std::path::Path, data_dir: &std::path::Path) -> Service {
    let cfg = ServiceConfig {
        instance_dir: instance_dir.to_owned(),
        app_id: APP_ID.into(),
        data_dir: data_dir.to_owned(),
        ..ServiceConfig::default()
    };
    ServiceBuilder::new(cfg, RegisterSm::default())
        .run()
        .await
        .expect("service start")
}

#[derive(Debug)]
pub enum SubmitOutcome {
    Ok(CmdResp),
    Indeterminate,
    Fatal(String),
}

#[derive(Debug)]
pub enum ReadOutcome {
    Ok(Option<u64>),
    Indeterminate,
    Fatal(String),
}
```

- [ ] **Step 3: Write a no-fault smoke test in `lin_register.rs`**

Replace `uc_node/tests/lin_register.rs` with:

```rust
#[path = "lincheck/mod.rs"]
mod lincheck;

use lincheck::cluster::{LinCluster, ReadOutcome, SubmitOutcome};
use lincheck::register_sm::Cmd;

#[tokio::test(flavor = "current_thread")]
async fn smoke_3node_submit_read() {
    let cluster = LinCluster::start_3().await;
    // A few sequential writes + reads through the leader, no faults.
    for v in 1..=5u64 {
        match cluster.submit_cmd(&Cmd::Write(v)).await {
            SubmitOutcome::Ok(_) => {}
            o => panic!("write {v} not Ok: {o:?}"),
        }
        match cluster.read().await {
            ReadOutcome::Ok(Some(got)) => assert_eq!(got, v, "read after write {v}"),
            o => panic!("read after write {v}: {o:?}"),
        }
    }
    cluster.shutdown().await;
}
```

- [ ] **Step 4: Run the smoke test**

Run: `cargo test -p uc_node --test lin_register smoke_3node_submit_read -- --nocapture`
Expected: PASS — the 3-node shmem cluster comes up, elects a leader, and write/read round-trips work. If `Client::connect` or `submit` mismatches, fix per the real signatures (`Client::submit::<Cmd, CmdResp>`, `query_linearizable::<(), Option<u64>>`).

- [ ] **Step 5: Commit**

```bash
git add uc_node/tests/lincheck/cluster.rs uc_node/tests/lincheck/mod.rs uc_node/tests/lin_register.rs
git commit -m "test(lincheck): LinCluster 3-node shmem spawn + route-to-leader + smoke"
```

---

### Task 2.3: Faults — leader kill/restart + service crash/restart

**Files:**
- Modify: `uc_node/tests/lincheck/cluster.rs`

- [ ] **Step 1: Add the fault methods to `LinCluster`**

Add to `impl LinCluster` in `uc_node/tests/lincheck/cluster.rs`:

```rust
    /// Kill the current leader's node + service (graceful), then restart the
    /// node (rejoin via persisted data_dir) + a fresh service, and reconnect
    /// its client (restart → new instance_id invalidates the old client).
    /// `&self`: takes handles out under a brief lock, awaits teardown/restart
    /// UNLOCKED, then re-locks to install — so workers aren't blocked on the lock
    /// across the multi-second failover.
    pub async fn kill_and_restart_leader(&self) {
        let Some(lid) = self.leader_id().await else { return };
        let (idx, id, addr, instance, data, svc_data, peers, client, service, handle) = {
            let mut nodes = self.nodes.lock().await;
            let Some(i) = nodes.iter().position(|n| n.id == lid) else { return };
            let n = &mut nodes[i];
            (
                i, n.id, n.addr,
                n.instance_dir.clone(), n.data_dir.clone(), n.svc_data_dir.clone(),
                n.peers.clone(),
                n.client.take(), n.service.take(), n.handle.take(),
            )
        };
        // Teardown unlocked.
        if let Some(c) = client {
            if let Ok(c) = Arc::try_unwrap(c) { let _ = c.shutdown().await; }
        }
        if let Some(s) = service { let _ = s.shutdown().await; }
        if let Some(h) = handle { let _ = h.shutdown().await; }
        // Survivors re-elect (quorum 2/3 holds).
        self.wait_for_stable_leader(Duration::from_secs(15)).await;
        // Restart the killed node against its persisted dirs.
        let cfg = node_config(id, &instance, &data, addr, peers);
        let new_handle = NodeBuilder::new(cfg, RegisterSm::default())
            .start()
            .await
            .unwrap_or_else(|e| panic!("node {id} restart: {e:?}"));
        wait_for_cnc(instance.path(), Duration::from_secs(10)).await;
        let new_service = spawn_service(instance.path(), svc_data.path()).await;
        let new_client = Arc::new(
            Client::connect(instance.path(), APP_ID)
                .await
                .expect("client reconnect after restart"),
        );
        {
            let mut nodes = self.nodes.lock().await;
            let n = &mut nodes[idx];
            n.handle = Some(new_handle);
            n.service = Some(new_service);
            n.client = Some(new_client);
        }
        self.wait_for_stable_leader(Duration::from_secs(15)).await;
    }

    /// Crash the current leader's SERVICE only (node stays up); the service
    /// watcher transfers leadership. Then restart a fresh service on the same
    /// instance_dir so that node is fully functional again.
    pub async fn crash_and_restart_leader_service(&self) {
        let Some(lid) = self.leader_id().await else { return };
        let (idx, instance, svc_data, service) = {
            let mut nodes = self.nodes.lock().await;
            let Some(i) = nodes.iter().position(|n| n.id == lid) else { return };
            let n = &mut nodes[i];
            (i, n.instance_dir.clone(), n.svc_data_dir.clone(), n.service.take())
        };
        if let Some(s) = service { let _ = s.shutdown().await; }
        // Leadership transfers away from the stalled node (m3 path).
        self.wait_for_stable_leader(Duration::from_secs(15)).await;
        // Restart the service so the node can serve again.
        let new_service = spawn_service(instance.path(), svc_data.path()).await;
        {
            let mut nodes = self.nodes.lock().await;
            nodes[idx].service = Some(new_service);
        }
    }
```

- [ ] **Step 2: VERIFICATION — write a fault round-trip test**

Add to `uc_node/tests/lin_register.rs`:

```rust
#[tokio::test(flavor = "current_thread")]
async fn fault_roundtrip_keeps_serving() {
    let cluster = LinCluster::start_3().await; // methods are &self; shutdown consumes self
    // Establish state.
    assert!(matches!(cluster.submit_cmd(&Cmd::Write(1)).await, SubmitOutcome::Ok(_)));
    // Kill+restart the leader; cluster must keep serving.
    cluster.kill_and_restart_leader().await;
    match cluster.submit_cmd(&Cmd::Write(2)).await {
        SubmitOutcome::Ok(_) | SubmitOutcome::Indeterminate => {}
        o => panic!("post leader-restart submit: {o:?}"),
    }
    // Crash+restart the leader's service; cluster must keep serving.
    cluster.crash_and_restart_leader_service().await;
    match cluster.submit_cmd(&Cmd::Write(3)).await {
        SubmitOutcome::Ok(_) | SubmitOutcome::Indeterminate => {}
        o => panic!("post service-crash submit: {o:?}"),
    }
    // Reads still work and reflect a committed value.
    match cluster.read().await {
        ReadOutcome::Ok(Some(_)) | ReadOutcome::Indeterminate => {}
        o => panic!("post-fault read: {o:?}"),
    }
    cluster.shutdown().await;
}
```

- [ ] **Step 3: Run the fault round-trip — this validates the two spec verification points**

Run: `cargo test -p uc_node --test lin_register fault_roundtrip_keeps_serving -- --nocapture --test-threads=1`
Expected: PASS — confirms (a) a killed voter restarts/rejoins and the cluster commits while it's down without a membership change, and (b) the client reconnect after restart works.

If it FAILS because the cluster can't make progress while the killed voter is down (openraft needs it removed), apply the fallback: in `kill_and_restart_leader`, after tearing down, call `new_leader.remove_node(killed_id)` on the new leader, and on restart `new_leader.add_learner(killed_id, addr)` + promote — mirroring `m4_client_leader_failover`. Re-run until green.

If the client reconnect errors with something other than expected after restart, capture the exact `ClientError` and adjust (reconnect is already unconditional here, so this should just work).

- [ ] **Step 4: Commit**

```bash
git add uc_node/tests/lincheck/cluster.rs uc_node/tests/lin_register.rs
git commit -m "test(lincheck): leader kill/restart + service crash/restart faults (quorum-preserving)"
```

---

## Phase 3 — Seeded workload + the linearizability test

### Task 3.1: Seeded concurrent workload + history recording

**Files:**
- Modify: `uc_node/tests/lin_register.rs`

- [ ] **Step 1: Add the workload driver to `lin_register.rs`**

Add to `uc_node/tests/lin_register.rs` (imports at top: `use std::sync::Arc;`, `use rand::{Rng, SeedableRng}; use rand::rngs::StdRng;`, and the lincheck `history`/`model` types):

```rust
use std::sync::Arc;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use lincheck::history::{History, Outcome};
use lincheck::model::{Op, RegResp};

/// One worker: until `stop`, pick a seeded op, submit/read via the leader,
/// classify the outcome, and record it.
async fn worker(
    id: u32,
    cluster: Arc<LinCluster>,
    history: Arc<History>,
    stop: Arc<std::sync::atomic::AtomicBool>,
    mut rng: StdRng,
    last_seen: Arc<std::sync::atomic::AtomicU64>,
) {
    use std::sync::atomic::Ordering;
    while !stop.load(Ordering::Relaxed) {
        let choice = rng.random_range(0..3u8);
        match choice {
            0 => {
                let v = rng.random_range(1..1000u64);
                let inv = history.invoke();
                let outcome = match cluster.submit_cmd(&Cmd::Write(v)).await {
                    SubmitOutcome::Ok(_) => Outcome::Ok(RegResp::Ack),
                    SubmitOutcome::Indeterminate => Outcome::Indeterminate,
                    SubmitOutcome::Fatal(e) => panic!("fatal submit: {e}"),
                };
                history.record(id, Op::Write(v), inv, outcome);
            }
            1 => {
                let inv = history.invoke();
                let outcome = match cluster.read().await {
                    ReadOutcome::Ok(v) => {
                        if let Some(x) = v { last_seen.store(x, Ordering::Relaxed); }
                        Outcome::Ok(RegResp::Value(v))
                    }
                    ReadOutcome::Indeterminate => Outcome::Indeterminate,
                    ReadOutcome::Fatal(e) => panic!("fatal read: {e}"),
                };
                history.record(id, Op::Read, inv, outcome);
            }
            _ => {
                // CAS using a recently-seen value as `old` (so some succeed),
                // sometimes a random old (so some fail).
                let old = if rng.random_bool(0.7) {
                    last_seen.load(Ordering::Relaxed)
                } else {
                    rng.random_range(1..1000u64)
                };
                let new = rng.random_range(1..1000u64);
                let inv = history.invoke();
                let outcome = match cluster.submit_cmd(&Cmd::Cas { old, new }).await {
                    SubmitOutcome::Ok(lincheck::register_sm::CmdResp::CasResult(b)) => {
                        if b { last_seen.store(new, Ordering::Relaxed); }
                        Outcome::Ok(RegResp::CasOk(b))
                    }
                    SubmitOutcome::Ok(_) => panic!("cas returned non-cas response"),
                    SubmitOutcome::Indeterminate => Outcome::Indeterminate,
                    SubmitOutcome::Fatal(e) => panic!("fatal cas: {e}"),
                };
                history.record(id, Op::Cas { old, new }, inv, outcome);
            }
        }
    }
}
```

- [ ] **Step 2: Build to check the workload compiles**

Run: `cargo build -p uc_node --tests 2>&1 | tail -3`
Expected: clean. Add `rand` to `uc_node`'s `[dev-dependencies]` if not already present (`rand = { workspace = true }`); the workspace already pins `rand = "0.9"`.

- [ ] **Step 3: Commit**

```bash
git add uc_node/tests/lin_register.rs uc_node/Cargo.toml
git commit -m "test(lincheck): seeded concurrent CAS-register workload + history recording"
```

---

### Task 3.2: The linearizability test (workers + fault scheduler + checker)

**Files:**
- Modify: `uc_node/tests/lin_register.rs`

- [ ] **Step 1: Add the full test**

Add to `uc_node/tests/lin_register.rs`:

```rust
use lincheck::checker::{check_register, Verdict};

#[tokio::test(flavor = "current_thread")]
async fn linearizable_under_failover() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    const DEFAULT_SEED: u64 = 0x1107;
    let seed: u64 = std::env::var("LIN_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(DEFAULT_SEED);
    let target_ops: usize = 1200;
    let n_workers: u32 = 8;

    let cluster = Arc::new(LinCluster::start_3().await);
    let history = Arc::new(History::default());
    let stop = Arc::new(AtomicBool::new(false));
    let last_seen = Arc::new(AtomicU64::new(0));

    // Workers.
    let mut handles = Vec::new();
    for w in 0..n_workers {
        let rng = StdRng::seed_from_u64(seed ^ (w as u64).wrapping_mul(0x9E3779B97F4A7C15));
        handles.push(tokio::spawn(worker(
            w,
            cluster.clone(),
            history.clone(),
            stop.clone(),
            rng,
            last_seen.clone(),
        )));
    }

    // Fault scheduler: inject one fault at a time (the methods are &self and lock
    // internally — Task 2.2), waiting for recovery between faults, until enough
    // ops have completed. Workers keep running against the shared Arc<LinCluster>.
    let mut fault_rng = StdRng::seed_from_u64(seed ^ 0xFA17);
    while History::ok_count(&history.snapshot()) < target_ops {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        if fault_rng.random_bool(0.5) {
            cluster.kill_and_restart_leader().await;
        } else {
            cluster.crash_and_restart_leader_service().await;
        }
    }

    stop.store(true, Ordering::Relaxed);
    for h in handles { let _ = h.await; }

    let cluster = Arc::try_unwrap(cluster).ok().expect("sole owner at shutdown");
    cluster.shutdown().await;

    let entries = Arc::try_unwrap(history).ok().expect("sole history owner").into_entries();

    // Liveness gate: most ops must have completed Ok, else the run is meaningless.
    let ok = History::ok_count(&entries);
    assert!(
        ok * 100 >= entries.len() * 80,
        "liveness: only {ok}/{} ops completed Ok (<80%) — cluster failed to progress",
        entries.len()
    );

    match check_register(&entries) {
        Verdict::Linearizable => {}
        Verdict::Violation => {
            dump_history(&entries, seed);
            panic!("LINEARIZABILITY VIOLATION (seed={seed}); history dumped");
        }
        Verdict::Inconclusive => {
            panic!("checker Inconclusive (seed={seed}); lower target_ops/workers");
        }
    }
}
```

- [ ] **Step 2: Add `History::snapshot()` and the `dump_history` helper**

(The `&self` + internal `tokio::sync::Mutex<Vec<Node>>` model is already in place from Task 2.2 — no cluster refactor needed here.)

Add a `snapshot` method to `History` in `uc_node/tests/lincheck/history.rs` (used by the progress check, which must read the in-progress history without consuming it):

```rust
impl History {
    /// Clone the entries recorded so far (for the progress / liveness check).
    pub fn snapshot(&self) -> Vec<Entry> {
        self.entries.lock().unwrap().clone()
    }
}
```

(`Entry`, `Op`, `RegResp`, and `Outcome` already derive `Clone` from Tasks 1.1/1.2, so `.clone()` compiles as-is.)

Add the `dump_history` helper to `uc_node/tests/lin_register.rs`:

```rust
fn dump_history(entries: &[lincheck::history::Entry], seed: u64) {
    let path = format!("/tmp/lincheck_history_{seed}.txt");
    let mut s = String::new();
    for e in entries {
        s.push_str(&format!("{e:?}\n"));
    }
    let _ = std::fs::write(&path, s);
    eprintln!("history ({} entries) dumped to {path}", entries.len());
}
```

- [ ] **Step 3: Run the linearizability test**

Run: `cargo test -p uc_node --test lin_register linearizable_under_failover -- --nocapture --test-threads=1`
Expected: PASS — `Linearizable`, ≥80% Ok. If `Inconclusive`, lower `target_ops`/`n_workers`. If `Violation`, that's either a real bug (investigate via the dumped history) or a harness/classification bug (re-examine the outcome mapping). Run 3× to check stability.

- [ ] **Step 4: Clippy + commit**

Run: `cargo clippy -p uc_node --tests -- -D warnings`
Expected: zero warnings.

```bash
git add uc_node/tests/lin_register.rs uc_node/tests/lincheck/cluster.rs uc_node/tests/lincheck/history.rs
git commit -m "test(lincheck): linearizable_under_failover — seeded workload + fault scheduler + checker + liveness gate"
```

---

### Task 3.3: Consolidate task doc + delete artifacts

**Files:**
- Create: `docs/tasks/task12_linearizability_harness.md`
- Delete: the spec + this plan (per CLAUDE.md feature workflow)

- [ ] **Step 1: Write `docs/tasks/task12_linearizability_harness.md`**

Record: the goal (CAS-register linearizability under failover), the module layout (`tests/lincheck/`), the three-way outcome classification, the WGL checker + indeterminate handling (drop indeterminate reads; optional unconstrained indeterminate mutations; budget→Inconclusive), the fault model (leader kill/restart + service crash/restart, quorum-preserving, one at a time), how to run (`LIN_SEED` env, `--test-threads=1`), the liveness gate, the determinism caveat (seeded but not bit-reproducible; failures reproduce via the dumped history), and the deferred items (partition/quorum-loss/DST/multi-key).

- [ ] **Step 2: Delete the ephemeral artifacts**

```bash
git rm docs/superpowers/specs/2026-06-05-linearizability-harness-design.md \
       docs/superpowers/plans/2026-06-05-linearizability-harness.md
```

- [ ] **Step 3: Commit**

```bash
git add docs/tasks/task12_linearizability_harness.md
git commit -m "docs(task12): consolidate linearizability harness"
```

---

## Final verification

- [ ] Pure checker/model/history tests green: `cargo test -p uc_node --test lin_register checker:: model:: history::` → all pass (the always-on cheap guards).
- [ ] Smoke + fault round-trip green: `cargo test -p uc_node --test lin_register smoke_3node_submit_read fault_roundtrip_keeps_serving -- --test-threads=1`.
- [ ] The capstone green and stable: `cargo test -p uc_node --test lin_register linearizable_under_failover -- --test-threads=1` (run 3×); `Linearizable`, ≥80% Ok, never `Inconclusive`.
- [ ] `cargo clippy -p uc_node --tests -- -D warnings` → zero warnings.
- [ ] No production-crate files changed (`git diff --name-only <base>..HEAD` shows only `uc_node/tests/**`, `uc_node/Cargo.toml` dev-deps, and `docs/**`).
- [ ] Full suite still green: `cargo test --workspace -- --test-threads=1`.
```
