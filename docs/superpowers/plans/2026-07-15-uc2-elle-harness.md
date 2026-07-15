# UC v2 Elle Consistency Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** An in-process elle consistency tier for UC v2: five list-append passes (quiet/failover/partition/purge/reconfig) over `LinClusterV2` adjudicated by the vendored elle-cli under `serializable` + strict real-time models, plus mutation testing proving three injected consensus bugs are caught.

**Architecture:** A new `ListAppendSm` + pure EDN recorder live in `uc-lincheck`; the driver is an `#[ignore]`d test target `uc2_node/tests/elle_v2.rs` sharing the existing `lincheck_v2` harness (genericized over the SM); shell scripts port ultima_db's checker/mutation machinery (`../ultima_db/scripts/elle_check.sh`, `elle_mutation.sh`, `tools/elle-cli/`); mutation knobs are `#[cfg(feature = "mutation-testing")]` fields in `uc2_consensus`/`uc2_node` driven by a `UC2_MUTATION` env read once in `uc2_node`.

**Tech Stack:** Rust (workspace conventions), elle-cli 0.1.9 standalone jar (Java/Temurin 21 + `jq` at check time only), bash scripts, GitHub Actions.

**Design spec:** `docs/superpowers/specs/2026-07-15-uc2-elle-harness-design.md` — read it first. Two approved deviations from the spec:
1. A fifth pass, **`partition`** (leader-isolation + heal), added because `skip-read-barrier` needs a deposed-but-alive leader to produce a stale read; it doubles as a clean pass.
2. The spec's §4.4 table maps `NotLeader`/`BackpressureFull`/`Retry` to `:fail` + a fresh invocation. The driver instead reuses the proven WGL routing (`submit_cmd` retries those errors *inside* one invocation, so elle sees one `:invoke` → eventual `:ok`/`:info`). Semantically safe: it only widens the op's concurrency window, which adds legal linearization orders — it can never manufacture a false anomaly. The guaranteed-not-committed judgment itself is unchanged (it's `submit_cmd`'s documented contract).

## Global Constraints

- `cargo clippy --workspace --all-targets -- -D warnings` must stay clean after every task.
- `cargo test` (default suite) must stay green after every task; the four lincheck capstones (`lin_v2`, `lin_partition_v2`) are the regression oracle for any `lincheck_v2/mod.rs` change.
- The WGL checker/history/model in `uc-lincheck` (`checker.rs`, `history.rs`, `model.rs`) are **untouched**.
- No new dependencies in `uc-lincheck` (it has `uc2_service` + `serde` + `bincode` only); the EDN module is std-only.
- `mutation-testing` features are **off in every default build**; feature-on + `UC2_MUTATION` unset must be byte-for-byte normal behavior.
- elle-cli's exit code is untrusted — always parse the stdout verdict; `unknown` is always a hard FAIL.
- Instance-dir tempdirs go under `CARGO_TARGET_TMPDIR` (ext4), never `/tmp` (tmpfs quota); EDN histories go under `ELLE_DIR` (default `/tmp/uc2-elle` — small text files, tmpfs is fine).
- Elle history semantics: `:fail` = definitely did not commit; `:info` = maybe committed, and the worker's process id is retired (never used again). Append values are globally unique.
- Superpowers artifacts under `docs/superpowers/` are retained, never deleted.

---

### Task 1: `ListAppendSm` in `uc-lincheck`

**Files:**
- Create: `uc-lincheck/src/list_append.rs`
- Modify: `uc-lincheck/src/lib.rs` (add `pub mod list_append;`)

**Interfaces:**
- Consumes: `uc2_service::{StateMachine, SnapshotStateMachine, SnapshotError}` (existing traits; see `uc-lincheck/src/register.rs` for the shape to mirror).
- Produces: `uc_lincheck::list_append::{LaCmd, LaResp, LaRead, ListAppendSm}` with `ListAppendSm: StateMachine<Command = LaCmd, Response = LaResp, Query = LaRead, QueryResponse = Vec<u64>> + SnapshotStateMachine + Default`. Task 5's driver and Task 4's generic harness rely on exactly these bounds.

- [ ] **Step 1: Write the failing tests**

Create `uc-lincheck/src/list_append.rs` with the tests first (module body only — the types come in Step 3):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The replicated list-append state machine for the elle consistency harness
//! (design spec 2026-07-15). Mirrors `RegisterSm`'s posture exactly: plain
//! in-memory, persists NOTHING — the proof object for service-state
//! reconstruction under node-kill / service-crash / purge churn. `Append` is a
//! Command; `Read` is a linearizable Query. Elle's list-append inference
//! requires each value be appended at most once per key — the driver draws
//! values from one global `AtomicU64`, so uniqueness holds across retries.

#[cfg(all(test, feature = "v2"))]
mod v2_tests {
    use super::{LaCmd, LaRead, LaResp, ListAppendSm};
    use uc2_service::StateMachine;

    #[test]
    fn apply_query_roundtrip_via_v2_trait() {
        let mut sm = ListAppendSm::default();
        assert_eq!(sm.last_applied(), None);
        assert_eq!(sm.query(LaRead { key: 7 }), Vec::<u64>::new());
        assert_eq!(sm.apply(128, LaCmd::Append { key: 7, val: 10 }), LaResp::AppendAck);
        assert_eq!(sm.apply(256, LaCmd::Append { key: 7, val: 20 }), LaResp::AppendAck);
        assert_eq!(sm.apply(384, LaCmd::Append { key: 3, val: 30 }), LaResp::AppendAck);
        // Per-key append order is the apply order; other keys are untouched.
        assert_eq!(sm.query(LaRead { key: 7 }), vec![10, 20]);
        assert_eq!(sm.query(LaRead { key: 3 }), vec![30]);
        assert_eq!(sm.query(LaRead { key: 99 }), Vec::<u64>::new());
        assert_eq!(sm.last_applied(), Some(384));
    }

    #[test]
    fn snapshot_roundtrip_via_v2_capability() {
        use uc2_service::SnapshotStateMachine;

        let mut sm = ListAppendSm::default();
        sm.apply(4096, LaCmd::Append { key: 1, val: 42 });
        let (handle, s) = sm.freeze().unwrap();
        assert_eq!(s, 4096);
        let mut bytes = Vec::new();
        ListAppendSm::stream_snapshot(handle, &mut bytes).unwrap();

        let mut restored = ListAppendSm::default();
        assert_eq!(restored.install_snapshot(4096, &mut bytes.as_slice()).unwrap(), 4096);
        assert_eq!(restored.query(LaRead { key: 1 }), vec![42]);
        assert_eq!(restored.last_applied(), Some(4096));

        // A mis-tagged install (wrong artifact position) is refused.
        assert!(restored.install_snapshot(99, &mut bytes.as_slice()).is_err());
    }
}
```

Add `pub mod list_append;` to `uc-lincheck/src/lib.rs` (below `pub mod register;`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p uc-lincheck 2>&1 | tail -5`
Expected: compile error — `LaCmd`/`ListAppendSm` not defined.

- [ ] **Step 3: Write the implementation**

Add above the test module in `list_append.rs` (mirror `register.rs`'s structure — same feature gate, same snapshot idiom):

```rust
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LaCmd {
    Append { key: u32, val: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LaResp {
    AppendAck,
}

/// The linearizable read of one key's list.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LaRead {
    pub key: u32,
}

#[derive(Default)]
pub struct ListAppendSm {
    lists: BTreeMap<u32, Vec<u64>>,
    last_applied: Option<u64>,
}

#[cfg(feature = "v2")]
impl uc2_service::StateMachine for ListAppendSm {
    type Command = LaCmd;
    type Response = LaResp;
    type Query = LaRead;
    type QueryResponse = Vec<u64>;

    fn apply(&mut self, position: u64, cmd: LaCmd) -> LaResp {
        let LaCmd::Append { key, val } = cmd;
        self.lists.entry(key).or_default().push(val);
        self.last_applied = Some(position);
        LaResp::AppendAck
    }
    fn query(&self, q: LaRead) -> Vec<u64> {
        self.lists.get(&q.key).cloned().unwrap_or_default()
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

// The M6 snapshot capability: lets the purge pass drive the REAL purge path.
// `SnapshotHandle = Vec<u8>` (bincode of `(lists, last_applied)`); install
// asserts the payload's recorded position matches the artifact tag — same
// belt-and-suspenders as `RegisterSm`.
#[cfg(feature = "v2")]
impl uc2_service::SnapshotStateMachine for ListAppendSm {
    type SnapshotHandle = Vec<u8>;

    fn freeze(&self) -> Result<(Vec<u8>, u64), uc2_service::SnapshotError> {
        let buf = bincode::serde::encode_to_vec(
            (&self.lists, self.last_applied),
            bincode::config::standard(),
        )
        .map_err(|e| uc2_service::SnapshotError::Codec(e.to_string()))?;
        Ok((buf, self.last_applied.unwrap_or(0)))
    }

    fn stream_snapshot(
        handle: Vec<u8>,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc2_service::SnapshotError> {
        std::io::Write::write_all(dst, &handle)?;
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc2_service::SnapshotError> {
        let mut buf = Vec::new();
        std::io::Read::read_to_end(src, &mut buf)?;
        let ((lists, la), _) = bincode::serde::decode_from_slice::<
            (BTreeMap<u32, Vec<u64>>, Option<u64>),
            _,
        >(&buf, bincode::config::standard())
        .map_err(|e| uc2_service::SnapshotError::Codec(e.to_string()))?;
        if la.unwrap_or(0) != position {
            return Err(uc2_service::SnapshotError::Codec(format!(
                "snapshot payload position {} != requested {position}",
                la.unwrap_or(0)
            )));
        }
        self.lists = lists;
        self.last_applied = Some(position);
        Ok(position)
    }
}
```

If `RegisterSm`'s `freeze` uses a different bincode call shape than shown, copy `register.rs`'s exact idiom — the two files must stay parallel.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p uc-lincheck 2>&1 | tail -5`
Expected: PASS (including the two new `list_append::v2_tests`).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p uc-lincheck --all-targets -- -D warnings
git add uc-lincheck/src/list_append.rs uc-lincheck/src/lib.rs
git commit -m "feat(uc-lincheck): ListAppendSm for the elle harness (elle T1)"
```

---

### Task 2: EDN history recorder in `uc-lincheck`

**Files:**
- Create: `uc-lincheck/src/edn.rs`
- Modify: `uc-lincheck/src/lib.rs` (add `pub mod edn;`)

**Interfaces:**
- Consumes: std only.
- Produces (Task 5 relies on these exact signatures):
  - `pub enum EdnOp { Append { key: u32, val: u64 }, Read { key: u32, result: Option<Vec<u64>> } }`
  - `pub enum EdnType { Invoke, Ok, Fail, Info }`
  - `pub fn edn_line(index: u64, typ: EdnType, process: u64, time_ns: u64, op: &EdnOp) -> String`
  - `pub struct EdnRecorder` with `new(initial_processes: u64) -> Self`, `retire(&self) -> u64`, `record(&self, typ: EdnType, process: u64, op: &EdnOp)`, `ok_count(&self) -> u64`, `completed_count(&self) -> u64`, `write_to(&self, path: &std::path::Path) -> std::io::Result<()>`.

- [ ] **Step 1: Write the failing tests**

Create `uc-lincheck/src/edn.rs` with tests first. The line format is byte-for-byte the ultima_db `elle-history` encoding (fixture-compatible with the vendored elle-cli):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn edn_line_append_invoke() {
        let op = EdnOp::Append { key: 7, val: 42 };
        assert_eq!(
            edn_line(0, EdnType::Invoke, 3, 1200, &op),
            "{:index 0, :type :invoke, :f :txn, :process 3, :time 1200, :value [[:append 7 42]]}"
        );
    }

    #[test]
    fn edn_line_read_nil_and_lists() {
        assert_eq!(
            edn_line(1, EdnType::Invoke, 0, 5, &EdnOp::Read { key: 1, result: None }),
            "{:index 1, :type :invoke, :f :txn, :process 0, :time 5, :value [[:r 1 nil]]}"
        );
        assert_eq!(
            edn_line(9, EdnType::Ok, 0, 5, &EdnOp::Read { key: 1, result: Some(vec![12, 42]) }),
            "{:index 9, :type :ok, :f :txn, :process 0, :time 5, :value [[:r 1 [12 42]]]}"
        );
        assert_eq!(
            edn_line(2, EdnType::Ok, 0, 5, &EdnOp::Read { key: 1, result: Some(vec![]) }),
            "{:index 2, :type :ok, :f :txn, :process 0, :time 5, :value [[:r 1 []]]}"
        );
    }

    #[test]
    fn recorder_indexes_counts_and_retires() {
        let r = EdnRecorder::new(2); // processes 0 and 1 pre-allocated
        let op = EdnOp::Append { key: 1, val: 1 };
        r.record(EdnType::Invoke, 0, &op);
        r.record(EdnType::Ok, 0, &op);
        r.record(EdnType::Invoke, 1, &op);
        r.record(EdnType::Info, 1, &op);
        // Fresh ids start above the pre-allocated range and are never reused.
        assert_eq!(r.retire(), 2);
        assert_eq!(r.retire(), 3);
        assert_eq!(r.ok_count(), 1);
        assert_eq!(r.completed_count(), 2); // Ok + Info (invokes don't count)

        let dir = std::env::temp_dir().join(format!("edn-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("history.edn");
        r.write_to(&path).unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 4);
        // Global :index stamps are the emission order.
        assert!(lines[0].starts_with("{:index 0, :type :invoke"));
        assert!(lines[3].starts_with("{:index 3, :type :info"));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
```

Add `pub mod edn;` to `uc-lincheck/src/lib.rs`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p uc-lincheck 2>&1 | tail -5`
Expected: compile error — `EdnOp` not defined.

- [ ] **Step 3: Write the implementation**

Above the tests in `edn.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Elle EDN history recording (design spec 2026-07-15). Pure std, hand-formatted
//! EDN — one map per line, the exact encoding ultima_db's `elle-history` driver
//! emits and the vendored elle-cli consumes. Singleton txns only (one op per
//! `:value`). Jepsen process semantics: after an `:info` (maybe-committed)
//! outcome a process id is RETIRED — `retire()` hands out a fresh one and the
//! old id must never record again (the driver asserts this).

use std::fmt::Write as _;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Clone, Debug)]
pub enum EdnOp {
    Append { key: u32, val: u64 },
    Read { key: u32, result: Option<Vec<u64>> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdnType {
    Invoke,
    Ok,
    Fail,
    Info,
}

/// Format one elle event line. `:f :txn` and a single-op `:value` vector —
/// the singleton-txn list-append encoding.
pub fn edn_line(index: u64, typ: EdnType, process: u64, time_ns: u64, op: &EdnOp) -> String {
    let typ = match typ {
        EdnType::Invoke => ":invoke",
        EdnType::Ok => ":ok",
        EdnType::Fail => ":fail",
        EdnType::Info => ":info",
    };
    let mut v = String::new();
    match op {
        EdnOp::Append { key, val } => write!(v, "[:append {key} {val}]").unwrap(),
        EdnOp::Read { key, result: None } => write!(v, "[:r {key} nil]").unwrap(),
        EdnOp::Read { key, result: Some(list) } => {
            write!(v, "[:r {key} [").unwrap();
            for (j, x) in list.iter().enumerate() {
                if j > 0 {
                    v.push(' ');
                }
                write!(v, "{x}").unwrap();
            }
            v.push_str("]]");
        }
    }
    format!("{{:index {index}, :type {typ}, :f :txn, :process {process}, :time {time_ns}, :value [{v}]}}")
}

/// Thread-safe event recorder: a global monotonic `:index`, `:time` in ns since
/// construction, fresh-forever process ids, and Ok/completed counters for the
/// driver's liveness gate.
pub struct EdnRecorder {
    start: Instant,
    index: AtomicU64,
    next_process: AtomicU64,
    ok: AtomicU64,
    completed: AtomicU64,
    lines: Mutex<Vec<String>>,
}

impl EdnRecorder {
    pub fn new(initial_processes: u64) -> Self {
        Self {
            start: Instant::now(),
            index: AtomicU64::new(0),
            next_process: AtomicU64::new(initial_processes),
            ok: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            lines: Mutex::new(Vec::new()),
        }
    }

    /// A fresh, never-before-used process id (call after an `:info`).
    pub fn retire(&self) -> u64 {
        self.next_process.fetch_add(1, Ordering::Relaxed)
    }

    /// Stamp index + time and append the formatted line.
    pub fn record(&self, typ: EdnType, process: u64, op: &EdnOp) {
        match typ {
            EdnType::Invoke => {}
            EdnType::Ok => {
                self.ok.fetch_add(1, Ordering::Relaxed);
                self.completed.fetch_add(1, Ordering::Relaxed);
            }
            EdnType::Fail | EdnType::Info => {
                self.completed.fetch_add(1, Ordering::Relaxed);
            }
        }
        let time_ns = self.start.elapsed().as_nanos() as u64;
        let mut lines = self.lines.lock().unwrap();
        // Index is taken under the lock so line order == index order in the file.
        let index = self.index.fetch_add(1, Ordering::Relaxed);
        lines.push(edn_line(index, typ, process, time_ns, op));
    }

    pub fn ok_count(&self) -> u64 {
        self.ok.load(Ordering::Relaxed)
    }
    /// Ok + Fail + Info (i.e. ops that finished one way or another).
    pub fn completed_count(&self) -> u64 {
        self.completed.load(Ordering::Relaxed)
    }

    pub fn write_to(&self, path: &std::path::Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let lines = self.lines.lock().unwrap();
        std::fs::write(path, lines.join("\n") + "\n")
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p uc-lincheck 2>&1 | tail -5`
Expected: PASS (three new `edn::tests`).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p uc-lincheck --all-targets -- -D warnings
git add uc-lincheck/src/edn.rs uc-lincheck/src/lib.rs
git commit -m "feat(uc-lincheck): elle EDN history recorder (elle T2)"
```

---

### Task 3: Vendor elle-cli + fixtures; pin the strict model name

**Files:**
- Create: `tools/elle-cli/elle-cli-0.1.9-standalone.jar` (binary, copied)
- Create: `tools/elle-cli/LICENSE` (copied)
- Create: `tools/elle-cli/README.md`
- Create: `tools/elle-cli/fixtures/known_bad.edn` (copied)
- Create: `tools/elle-cli/fixtures/realtime_violation.edn` (new)

**Interfaces:**
- Produces: the jar path and fixture paths Tasks 6/11's scripts hard-code, and the **strict model name** (recorded in the README and used as the scripts' default).

- [ ] **Step 1: Ensure java is available**

```bash
command -v java || sudo apt-get install -y default-jre-headless
java -version
```
Expected: any Java ≥ 11.

- [ ] **Step 2: Copy the vendored checker from ultima_db**

```bash
mkdir -p tools/elle-cli/fixtures
cp ../ultima_db/tools/elle-cli/elle-cli-0.1.9-standalone.jar tools/elle-cli/
cp ../ultima_db/tools/elle-cli/LICENSE tools/elle-cli/
cp ../ultima_db/tools/elle-cli/fixtures/known_bad.edn tools/elle-cli/fixtures/
sha256sum tools/elle-cli/elle-cli-0.1.9-standalone.jar
```
Expected sha256: `c9ba9b9fd32640e73d632cb5f15069c162ba6528a67f27a878767187c59f539a` (must match — it's the pin in ultima_db's README).

- [ ] **Step 3: Write the real-time-violation fixture**

Create `tools/elle-cli/fixtures/realtime_violation.edn` — two singleton txns where the read *begins after* the append's `:ok` yet observes the pre-append state. Legal under plain `serializable` (the read may serialize first), illegal under any strict/real-time model:

```
{:index 0, :type :invoke, :f :txn, :process 0, :time 0, :value [[:append 1 10]]}
{:index 1, :type :ok, :f :txn, :process 0, :time 5, :value [[:append 1 10]]}
{:index 2, :type :invoke, :f :txn, :process 1, :time 10, :value [[:r 1 nil]]}
{:index 3, :type :ok, :f :txn, :process 1, :time 15, :value [[:r 1 []]]}
```

- [ ] **Step 4: Probe the jar for the strict model name**

```bash
J="java -jar tools/elle-cli/elle-cli-0.1.9-standalone.jar --model list-append"
$J --consistency-models strong-serializable tools/elle-cli/fixtures/realtime_violation.edn || true
$J --consistency-models strict-serializable tools/elle-cli/fixtures/realtime_violation.edn || true
$J --consistency-models serializable tools/elle-cli/fixtures/realtime_violation.edn || true
```

Decision rule (record the winner as `STRICT_MODEL`):
- The strict candidate must print a `false` verdict on this fixture, AND `serializable` must print `true` on it.
- Try `strong-serializable` first, then `strict-serializable`. If **neither** name yields a `false` verdict here, the vendored 0.1.9 jar predates strict-model support: download the newest elle-cli release from https://github.com/ligurio/elle-cli/releases, replace the jar (update filename, sha256, and version in the README + both scripts in Tasks 6/11), and re-probe. Do not proceed until one strict name rejects this fixture while `serializable` accepts it.

Also confirm the ported baseline: `$J --consistency-models serializable tools/elle-cli/fixtures/known_bad.edn` prints `false`.

- [ ] **Step 5: Write `tools/elle-cli/README.md`**

Adapt ultima_db's (`../ultima_db/tools/elle-cli/README.md`): upstream URL, version, sha256, EPL-2.0 note, Java requirement, and UC-specific usage — used by `scripts/elle_check.sh` / `scripts/elle_mutation.sh`, both fixtures described (known_bad = dependency-cycle self-test; realtime_violation = strict-model self-test), and a line recording the probed `STRICT_MODEL` name and the probe result from Step 4.

- [ ] **Step 6: Commit**

```bash
git add tools/elle-cli
git commit -m "chore(tools): vendor elle-cli + fixtures for the elle harness (elle T3)"
```

---

### Task 4: Genericize the `lincheck_v2` harness over the SM

**Files:**
- Modify: `uc2_node/tests/lincheck_v2/mod.rs`

**Interfaces:**
- Consumes: `uc_lincheck::list_append::ListAppendSm` bounds from Task 1 (only via the trait bound — this file never names `ListAppendSm`).
- Produces (Task 5 relies on):
  - `pub struct LinClusterV2<SM: SnapshotStateMachine + Default = RegisterSm>` — all existing pub methods unchanged in name/behavior, now on `impl<SM: SnapshotStateMachine + Default> LinClusterV2<SM>`.
  - `pub struct WorkerConn` with `pub fn new(dirs: Arc<Vec<PathBuf>>, start: usize) -> Self` and `pub fn drop_client(&mut self)`.
  - `pub enum SubmitOutcome<R> { Ok(R), Indeterminate, Fatal(String) }` and `pub enum ReadOutcome<QR> { Ok(QR), Indeterminate, Fatal(String) }`.
  - `pub fn submit_cmd<C: Serialize, R: DeserializeOwned>(conn: &mut WorkerConn, cmd: &C, deadline: Instant) -> SubmitOutcome<R>`.
  - `pub fn read_leader<Q: Serialize, QR: DeserializeOwned>(conn: &mut WorkerConn, q: &Q, deadline: Instant) -> ReadOutcome<QR>`.

Behavior must be byte-for-byte identical for the existing capstones — this task is type-level only.

- [ ] **Step 1: Genericize the service spawn and slot/cluster structs**

In `uc2_node/tests/lincheck_v2/mod.rs`:

1. Extend the `uc2_service` import with `SnapshotStateMachine` (it already imports `ServiceBuilder, ServiceConfig, SnapshotPolicy`).
2. `spawn_service` (line ~175) becomes generic — replace `RegisterSm` with the type parameter:

```rust
fn spawn_service<SM: SnapshotStateMachine + Default>(
    dir: &Path,
    snapshot_interval_bytes: u64,
) -> uc2_service::Service<SM> {
    if snapshot_interval_bytes == 0 {
        ServiceBuilder::new(ServiceConfig::new(dir, APP), SM::default())
            .start()
            .expect("service start")
    } else {
        let cfg = ServiceConfig::new(dir, APP)
            .snapshot_policy(SnapshotPolicy { interval_bytes: snapshot_interval_bytes });
        ServiceBuilder::new(cfg, SM::default())
            .start_with_snapshots()
            .expect("snapshot service start")
    }
}
```

3. `NodeSlot` (line ~196) → `pub struct NodeSlot<SM: SnapshotStateMachine + Default>` with `service: Option<uc2_service::Service<SM>>`; its `impl` block becomes `impl<SM: SnapshotStateMachine + Default> NodeSlot<SM>`.
4. `LinClusterV2` (line ~235) → `pub struct LinClusterV2<SM: SnapshotStateMachine + Default = RegisterSm>` with `nodes: Vec<NodeSlot<SM>>` and `spare: Option<NodeSlot<SM>>`.
5. The main `impl LinClusterV2` block → `impl<SM: SnapshotStateMachine + Default> LinClusterV2<SM>`; every internal `spawn_service(...)` call now infers `SM`. Where `Self`/`LinClusterV2` appears in return types (`start`, `start_cfg`) it stays `Self`.
6. **Move `read_from` (line ~593) into its own non-generic impl block** — it is register-typed (`query_linearizable::<(), Option<u64>>`, `RegResp`):

```rust
// Register-typed probe used by the WGL partition scenarios only.
impl LinClusterV2<RegisterSm> {
    pub fn read_from(&self, node: usize) -> Outcome { /* body unchanged */ }
}
```

- [ ] **Step 2: Genericize the worker routing helpers**

Still in `mod.rs`:

1. `WorkerConn` (line ~872): add `pub` to the struct and to `new`, `drop_client` (leave the other methods' visibility as-is unless the compiler demands more).
2. `SubmitOutcome` (line ~838) / `ReadOutcome` (line ~849) become generic — payload variants change from the concrete `CmdResp` / `Option<u64>` to the type parameter:

```rust
pub enum SubmitOutcome<R> {
    Ok(R),
    Indeterminate,
    Fatal(String),
}
pub enum ReadOutcome<QR> {
    Ok(QR),
    Indeterminate,
    Fatal(String),
}
```

3. `submit_cmd` (line ~927) and `read_leader` (line ~975) take the payload types as generics; bodies unchanged except the client-call turbofish:

```rust
pub fn submit_cmd<C: serde::Serialize, R: serde::de::DeserializeOwned>(
    conn: &mut WorkerConn,
    cmd: &C,
    deadline: Instant,
) -> SubmitOutcome<R> {
    // ... identical body; the one changed line:
    //     match client.submit::<C, R>(cmd) {
}

pub fn read_leader<Q: serde::Serialize, QR: serde::de::DeserializeOwned>(
    conn: &mut WorkerConn,
    q: &Q,
    deadline: Instant,
) -> ReadOutcome<QR> {
    // ... identical body; the one changed line:
    //     match client.query_linearizable::<Q, QR>(q) {
}
```

4. Fix the WGL `worker()` call sites (line ~1036/1045/1065) with explicit types where inference fails:
   - `submit_cmd::<_, CmdResp>(&mut conn, &Cmd::Write(v), deadline)`
   - `read_leader::<(), Option<u64>>(&mut conn, &(), deadline)`
   - `submit_cmd::<_, CmdResp>(&mut conn, &Cmd::Cas { old, new }, deadline)`

- [ ] **Step 3: Compile everything that uses the harness**

Run: `cargo test -p uc2_node --no-run 2>&1 | tail -5`
Expected: clean compile of `lin_v2`, `lin_partition_v2`, and every other test target. Chase any remaining `RegisterSm`-hardcoding the compiler flags (e.g. `lin_partition_v2.rs` naming `LinClusterV2` bare — the default type param keeps it compiling unchanged).

- [ ] **Step 4: Regression — smoke + one full capstone**

```bash
cargo test -p uc2_node --release --test lin_v2 smoke_3node_write_then_read -- --nocapture
cargo test -p uc2_node --release --test lin_v2 linearizable_under_failover_v2 -- --nocapture
```
Expected: both PASS (`Linearizable`, liveness ≥ 80%). This is the proof the refactor changed nothing behaviorally.

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add uc2_node/tests/lincheck_v2/mod.rs
git commit -m "refactor(uc2_node): genericize lincheck_v2 harness over the SM (elle T4)"
```

---

### Task 5: elle driver — env knobs, worker, `run_pass`, quiet pass

**Files:**
- Create: `uc2_node/tests/elle_v2.rs`

**Interfaces:**
- Consumes: Task 1 (`ListAppendSm` types), Task 2 (`EdnRecorder`), Task 4 (generic harness + routing helpers).
- Produces: `#[ignore]`d test `elle_quiet`; the `run_pass` helper + `elle_worker` that Tasks 7–8 add passes onto; env contract `ELLE_DIR`/`ELLE_SEED`/`ELLE_WORKERS`/`ELLE_KEYS`/`ELLE_TARGET_OPS`/`ELLE_BUDGET_SECS` that Tasks 6/11's scripts rely on.

- [ ] **Step 1: Write the driver**

Create `uc2_node/tests/elle_v2.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Elle consistency-harness driver (design spec 2026-07-15). Each `#[ignore]`d
//! test is one PASS: it boots a LinClusterV2 running `ListAppendSm`, drives
//! UNTHROTTLED seeded workers through the singleton-txn list-append workload,
//! runs the pass's nemesis arms, and writes `$ELLE_DIR/<pass>/history.edn`
//! for `scripts/elle_check.sh` to adjudicate (serializable + strict model,
//! anomaly set must be empty). Never in the default `cargo test`:
//!
//! ```bash
//! cargo test -p uc2_node --release --test elle_v2 -- --ignored --exact elle_quiet
//! ```
//!
//! Elle semantics (vs the WGL harness): `:fail` = guaranteed-not-committed
//! only; maybe-committed appends are `:info` and RETIRE the worker's process
//! id (a Jepsen process may not act after an indeterminate outcome). Failed
//! reads are `:fail` (no side effect). Append values come from one global
//! AtomicU64 — unique across all workers and retries.

#[path = "lincheck_v2/mod.rs"]
mod lincheck_v2;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use lincheck_v2::{
    ClusterCfg, LinClusterV2, ReadOutcome, SubmitOutcome, WorkerConn, read_leader, serialize,
    submit_cmd,
};
use uc2_net::fault::FaultConfig;
use uc_lincheck::edn::{EdnOp, EdnRecorder, EdnType};
use uc_lincheck::list_append::{LaCmd, LaRead, LaResp, ListAppendSm};

// ------------------------------------------------------------------ env knobs

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn elle_dir() -> PathBuf {
    PathBuf::from(std::env::var("ELLE_DIR").unwrap_or_else(|_| "/tmp/uc2-elle".into()))
}

/// Instance dirs on ext4 (journal segments blow the tmpfs /tmp quota).
fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-elle-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

// ------------------------------------------------------------------- worker

/// One unthrottled worker: 50/50 append/read over `keys` hot keys.
fn elle_worker(
    id: u32,
    dirs: Arc<Vec<PathBuf>>,
    rec: Arc<EdnRecorder>,
    stop: Arc<AtomicBool>,
    mut rng: StdRng,
    keys: u32,
    values: Arc<AtomicU64>,
) {
    let mut conn = WorkerConn::new(dirs, id as usize);
    // Initial process ids 0..n_workers are pre-allocated by EdnRecorder::new.
    let mut process = id as u64;
    while !stop.load(Ordering::Relaxed) {
        let deadline = Instant::now() + Duration::from_secs(15);
        let key = rng.random_range(0..keys);
        if rng.random_bool(0.5) {
            let val = values.fetch_add(1, Ordering::Relaxed);
            let op = EdnOp::Append { key, val };
            rec.record(EdnType::Invoke, process, &op);
            match submit_cmd::<_, LaResp>(&mut conn, &LaCmd::Append { key, val }, deadline) {
                SubmitOutcome::Ok(LaResp::AppendAck) => rec.record(EdnType::Ok, process, &op),
                // Maybe-committed: :info, then this process id never acts again.
                SubmitOutcome::Indeterminate => {
                    rec.record(EdnType::Info, process, &op);
                    process = rec.retire();
                }
                SubmitOutcome::Fatal(e) => panic!("fatal submit: {e}"),
            }
        } else {
            let op = EdnOp::Read { key, result: None };
            rec.record(EdnType::Invoke, process, &op);
            match read_leader::<LaRead, Vec<u64>>(&mut conn, &LaRead { key }, deadline) {
                ReadOutcome::Ok(list) => {
                    rec.record(EdnType::Ok, process, &EdnOp::Read { key, result: Some(list) });
                }
                // Reads have no side effect: a lost read definitely didn't
                // happen — :fail, and the process may continue.
                ReadOutcome::Indeterminate => rec.record(EdnType::Fail, process, &op),
                ReadOutcome::Fatal(e) => panic!("fatal read: {e}"),
            }
        }
    }
    conn.drop_client();
}

// ------------------------------------------------------------------ run_pass

/// Drive one pass: boot, spawn workers, tick the nemesis every `fault_period`
/// until the op target AND the pass's non-vacuity hold (or the budget runs
/// out), then write `$ELLE_DIR/<name>/history.edn` (+ a `seed` sidecar) and
/// assert the liveness/non-vacuity gates.
#[allow(clippy::too_many_arguments)]
fn run_pass<F, V>(
    name: &str,
    ccfg: ClusterCfg,
    default_target_ops: u64,
    min_ok_pct: u64,
    fault_period: Duration,
    mut nemesis_tick: F,
    non_vacuous: V,
    vacuity_label: &str,
) where
    F: FnMut(&mut LinClusterV2<ListAppendSm>, &mut StdRng, u32),
    V: Fn(&LinClusterV2<ListAppendSm>, u32) -> bool,
{
    let seed = env_u64("ELLE_SEED", 0x1107);
    let n_workers = env_u64("ELLE_WORKERS", 4) as u32;
    let keys = env_u64("ELLE_KEYS", 8) as u32;
    let target = env_u64("ELLE_TARGET_OPS", default_target_ops);
    let budget = Duration::from_secs(env_u64("ELLE_BUDGET_SECS", 120));

    let _g = serialize();
    let dir = tempdir();
    let mut cluster =
        LinClusterV2::<ListAppendSm>::start_cfg(dir.path(), 3, FaultConfig::default(), ccfg);
    cluster.await_single_serving(30);

    let dirs = Arc::new(cluster.dirs());
    let rec = Arc::new(EdnRecorder::new(n_workers as u64));
    let stop = Arc::new(AtomicBool::new(false));
    let values = Arc::new(AtomicU64::new(1));

    let handles: Vec<_> = (0..n_workers)
        .map(|w| {
            let rng = StdRng::seed_from_u64(seed ^ (w as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15));
            let (dirs, rec, stop, values) =
                (Arc::clone(&dirs), Arc::clone(&rec), Arc::clone(&stop), Arc::clone(&values));
            std::thread::spawn(move || elle_worker(w, dirs, rec, stop, rng, keys, values))
        })
        .collect();

    let mut frng = StdRng::seed_from_u64(seed ^ 0xFA17);
    let mut faults = 0u32;
    let start = Instant::now();
    while rec.ok_count() < target || !non_vacuous(&cluster, faults) {
        std::thread::sleep(fault_period);
        nemesis_tick(&mut cluster, &mut frng, faults);
        faults += 1;
        if start.elapsed() > budget {
            break;
        }
    }
    let elapsed = start.elapsed();
    let vacuity_ok = non_vacuous(&cluster, faults);

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        if let Err(e) = h.join() {
            std::panic::resume_unwind(e);
        }
    }
    cluster.stop();

    let out = elle_dir().join(name);
    rec.write_to(&out.join("history.edn")).expect("write history");
    std::fs::write(out.join("seed"), format!("{seed}\n")).expect("write seed");

    let (ok, completed) = (rec.ok_count(), rec.completed_count());
    eprintln!(
        "[elle {name}] seed={seed} faults={faults} completed={completed} ok={ok} \
         elapsed={:.1}s -> {}",
        elapsed.as_secs_f64(),
        out.join("history.edn").display()
    );
    assert!(vacuity_ok, "vacuous {name} pass: {vacuity_label} (faults={faults})");
    assert!(
        ok * 100 >= completed * min_ok_pct,
        "liveness: only {ok}/{completed} ops Ok (<{min_ok_pct}%) in the {name} pass"
    );
}

// ------------------------------------------------------------------- passes

/// Quiet pass: no faults — the baseline history and the biggest cycle-search
/// load for elle-cli (largest event count).
#[test]
#[ignore]
fn elle_quiet() {
    run_pass(
        "quiet",
        ClusterCfg::default(),
        50_000,
        90,
        Duration::from_millis(100),
        |_cluster, _rng, _faults| {},
        |_cluster, _faults| true,
        "unreachable",
    );
}
```

- [ ] **Step 2: Compile**

Run: `cargo test -p uc2_node --release --test elle_v2 --no-run 2>&1 | tail -3`
Expected: clean compile. (Nothing runs in default `cargo test` — the test is ignored.)

- [ ] **Step 3: Run the quiet pass**

Run: `cargo test -p uc2_node --release --test elle_v2 -- --ignored --exact elle_quiet --nocapture`
Expected: PASS in well under the budget; the `[elle quiet]` line reports ≥ 50k ok ops; `/tmp/uc2-elle/quiet/history.edn` exists with ~2× that many lines.

- [ ] **Step 4: Hand-check the history with elle-cli**

```bash
java -jar tools/elle-cli/elle-cli-0.1.9-standalone.jar --model list-append \
    --consistency-models serializable /tmp/uc2-elle/quiet/history.edn
java -jar tools/elle-cli/elle-cli-0.1.9-standalone.jar --model list-append \
    --consistency-models <STRICT_MODEL from Task 3> /tmp/uc2-elle/quiet/history.edn
```
Expected: `... true` for both. If `unknown`: halve `ELLE_TARGET_OPS`, rerun, and lower the in-file default — record the working size. If `false`: **stop — that is a real finding**; do not tune it away, dump the verdict and investigate (see the runbook flow, Task 12).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add uc2_node/tests/elle_v2.rs
git commit -m "feat(uc2_node): elle driver + quiet pass (elle T5)"
```

---

### Task 6: `scripts/elle_check.sh` — fixtures self-test, driver auto-run, classification

**Files:**
- Create: `scripts/elle_check.sh` (mode 755)

**Interfaces:**
- Consumes: Task 3 jar/fixtures + `STRICT_MODEL`, Task 5 driver test names + env contract.
- Produces: the one-command entry point (`scripts/elle_check.sh [pass ...]`), also invoked by Task 11's mutation script (its `classify` function) and Task 12's CI.

- [ ] **Step 1: Write the script**

Port `../ultima_db/scripts/elle_check.sh` (same `verdict`/`classify`/hard-fail-on-no-JSON machinery). Full script:

```bash
#!/usr/bin/env bash
# UC v2 elle consistency check (design spec 2026-07-15). Generates missing pass
# histories via the elle_v2 driver, then asserts each is valid with an EMPTY
# anomaly set under BOTH serializable and the strict (real-time) model.
# Usage: scripts/elle_check.sh [pass ...]     (default: all five passes)
set -euo pipefail

JAVA="${JAVA:-java}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$ROOT/tools/elle-cli/elle-cli-0.1.9-standalone.jar"
FIX_CYCLE="$ROOT/tools/elle-cli/fixtures/known_bad.edn"
FIX_RT="$ROOT/tools/elle-cli/fixtures/realtime_violation.edn"
ELLE_DIR="${ELLE_DIR:-/tmp/uc2-elle}"
# Pinned by the Task-3 probe (see tools/elle-cli/README.md).
STRICT_MODEL="${ELLE_STRICT_MODEL:-strong-serializable}"
# Set by elle_mutation.sh so mutated driver runs build with the fault feature.
CARGO_FEATURES="${ELLE_CARGO_FEATURES:-}"

PASSES=("$@")
[ ${#PASSES[@]} -eq 0 ] && PASSES=(quiet failover partition purge reconfig)

command -v "$JAVA" >/dev/null 2>&1 || { echo "error: java not found (set JAVA=)" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "error: jq not found" >&2; exit 1; }
[ -f "$JAR" ] || { echo "error: missing $JAR" >&2; exit 1; }
[ -f "$FIX_CYCLE" ] && [ -f "$FIX_RT" ] || { echo "error: missing fixtures" >&2; exit 1; }

# verdict <model> <history>: echoes true|false|unknown (exit code untrusted).
verdict() {
    local out v
    out="$("$JAVA" -jar "$JAR" --model list-append --consistency-models "$1" "$2")" || true
    v="$(printf '%s\n' "$out" | awk 'END { print $NF }')"
    case "$v" in
        true|false|unknown) printf '%s\n' "$v" ;;
        *) echo "error: no verdict from elle-cli on $2 (output: '$out')" >&2; exit 1 ;;
    esac
}

# classify <model> <history>: echoes "<valid?>|<sorted,joined anomaly-types>".
classify() {
    local out
    out="$("$JAVA" -jar "$JAR" --model list-append --consistency-models "$1" --verbose "$2")" || true
    printf '%s' "$out" \
        | jq -r '((.["valid?"])|tostring) + "|" + (((.["anomaly-types"]) // []) | sort | join(","))' 2>/dev/null \
        || { echo "error: elle-cli produced no JSON report for $2" >&2; exit 1; }
}

require() { # <expected> <actual> <label>
    if [ "$2" != "$1" ]; then
        echo "FAIL: $3 (got: '$2', expected '$1')" >&2
        case "$2" in unknown*) echo "hint: shrink the history (ELLE_TARGET_OPS) — unknown never passes" >&2 ;; esac
        exit 1
    fi
    echo "OK: $3"
}

echo "== fixture self-tests (checker teeth before any real verdict) =="
require false "$(verdict serializable "$FIX_CYCLE")" "cycle fixture rejected under serializable"
require true  "$(verdict serializable "$FIX_RT")"    "realtime fixture accepted under plain serializable"
require false "$(verdict "$STRICT_MODEL" "$FIX_RT")" "realtime fixture rejected under $STRICT_MODEL"

for pass in "${PASSES[@]}"; do
    hist="$ELLE_DIR/$pass/history.edn"
    if [ ! -f "$hist" ]; then
        echo "== generating $pass history (elle_v2 driver) =="
        # shellcheck disable=SC2086
        (cd "$ROOT" && ELLE_DIR="$ELLE_DIR" cargo test -p uc2_node --release $CARGO_FEATURES \
            --test elle_v2 -- --ignored --exact "elle_$pass" --nocapture)
    fi
    echo "== $pass: $(wc -l < "$hist") events =="
    require "true|" "$(classify serializable "$hist")"    "$pass clean under serializable"
    require "true|" "$(classify "$STRICT_MODEL" "$hist")" "$pass clean under $STRICT_MODEL"
done

echo "elle consistency check passed (${PASSES[*]})"
```

`chmod 755 scripts/elle_check.sh`. If Task 3 pinned `strict-serializable` (or a newer jar), update `STRICT_MODEL`'s default and the `JAR` filename here.

- [ ] **Step 2: End-to-end quiet run**

```bash
rm -rf /tmp/uc2-elle/quiet
scripts/elle_check.sh quiet
```
Expected: three fixture OKs, driver generates the history, then `OK: quiet clean under serializable` + `OK: quiet clean under <strict>` and `elle consistency check passed (quiet)`.

- [ ] **Step 3: Commit**

```bash
git add scripts/elle_check.sh
git commit -m "feat(scripts): elle_check.sh — fixtures self-test + pass adjudication (elle T6)"
```

---

### Task 7: Faulted passes — `failover` and `partition`

**Files:**
- Modify: `uc2_node/tests/elle_v2.rs` (append two tests)

**Interfaces:**
- Consumes: Task 5's `run_pass`; `LinClusterV2` nemesis methods `kill_and_restart_leader`, `crash_and_restart_leader_service`, `partition_leader`, `partition_minority`, `heal`, `await_reconverged` (all existing, all pub).
- Produces: `#[ignore]`d tests `elle_failover`, `elle_partition` (the latter is also Task 11's `skip-read-barrier` catch vehicle).

- [ ] **Step 1: Add the two passes**

Append to `elle_v2.rs`:

```rust
/// Failover pass: the lin_v2 failover capstone's fault mix — leader node
/// kill+restart vs leader service crash+restart, 50/50, one quorum-preserving
/// fault at a time. Also the catch vehicle for the `commit-quorum-minus-one`
/// and `skip-vote-order-check` mutations (elle_mutation.sh).
#[test]
#[ignore]
fn elle_failover() {
    run_pass(
        "failover",
        ClusterCfg::default(),
        20_000,
        70,
        Duration::from_secs(1),
        |cluster, rng, _faults| {
            if rng.random_bool(0.5) {
                cluster.kill_and_restart_leader();
            } else {
                cluster.crash_and_restart_leader_service();
            }
        },
        |_cluster, faults| faults >= 3,
        "fewer than 3 faults landed",
    );
}

/// Partition pass (spec deviation, approved): isolate-then-heal cycles — 2/3
/// leader isolation (a deposed-but-alive leader is the stale-read window the
/// `skip-read-barrier` mutation needs), 1/3 minority isolation. Clean runs
/// must stay anomaly-free under the strict model: the barrier is exactly what
/// makes a partitioned leader refuse stale answers.
#[test]
#[ignore]
fn elle_partition() {
    run_pass(
        "partition",
        ClusterCfg::default(),
        20_000,
        60,
        Duration::from_millis(1200),
        |cluster, rng, _faults| {
            if rng.random_bool(2.0 / 3.0) {
                cluster.partition_leader();
            } else {
                cluster.partition_minority();
            }
            std::thread::sleep(Duration::from_millis(800));
            cluster.heal();
            cluster.await_reconverged(20);
        },
        |_cluster, faults| faults >= 3,
        "fewer than 3 partition cycles landed",
    );
}
```

If `partition_leader`/`partition_minority` return a value the compiler warns about, bind with `let _ =`.

- [ ] **Step 2: Run both passes through the checker**

```bash
rm -rf /tmp/uc2-elle/failover /tmp/uc2-elle/partition
scripts/elle_check.sh failover partition
```
Expected: both generated and `OK: ... clean` under both models. If a pass trips the liveness gate, raise `ELLE_BUDGET_SECS`/lower the in-file `min_ok_pct` is NOT the first move — first check the fault pacing against the capstones' tuning notes in `lin_v2.rs`. If elle says `false`: real finding, stop and investigate.

- [ ] **Step 3: Clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add uc2_node/tests/elle_v2.rs
git commit -m "feat(uc2_node): elle failover + partition passes (elle T7)"
```

---

### Task 8: Faulted passes — `purge` and `reconfig`

**Files:**
- Modify: `uc2_node/tests/elle_v2.rs` (append two tests)

**Interfaces:**
- Consumes: `run_pass`; `ClusterCfg { purge, journal_segment_bytes, snapshot_interval_bytes, spare_node }`; nemesis methods `crash_and_restart_random_follower_service(rng)`, `random_config_op(rng)`, `spare_is_voting()`, `max_archive_first_base()`, field `config_ops_accepted`.
- Produces: `#[ignore]`d tests `elle_purge`, `elle_reconfig` — completing the five-pass clean tier.

- [ ] **Step 1: Add the two passes**

Append to `elle_v2.rs` (fault mixes copied from the corresponding `lin_v2.rs` capstones — purge posture and reconfig arm gates verbatim):

```rust
/// Purge pass: the M6 purge-churn capstone's posture — aggressive
/// snapshot-backed purge (16 KiB segments, 32 KiB snapshot cadence, zero
/// slack) with the follower-service-crash arm forcing below-floor
/// snapshot-install reconstruction. Non-vacuity: the archive floor advanced.
#[test]
#[ignore]
fn elle_purge() {
    let ccfg = ClusterCfg {
        purge: uc2_node::PurgePolicy::BelowSnapshot { slack_bytes: 0 },
        journal_segment_bytes: 16 * 1024,
        snapshot_interval_bytes: 32 * 1024,
        spare_node: false,
    };
    run_pass(
        "purge",
        ccfg,
        20_000,
        70,
        Duration::from_millis(1200),
        |cluster, rng, _faults| match rng.random_range(0..3u8) {
            0 => cluster.kill_and_restart_leader(),
            1 => cluster.crash_and_restart_leader_service(),
            _ => cluster.crash_and_restart_random_follower_service(rng),
        },
        |cluster, _faults| cluster.max_archive_first_base() > 0,
        "purge never advanced the archive floor",
    );
}

/// Reconfig pass: the M7 reconfig-churn capstone's four arms — leader kill
/// (gated off while the spare is a voter), follower service crash, a short
/// minority partition (same gate), and one step of the spare's
/// add/promote/demote/remove cycle. Non-vacuity: >= 3 accepted config ops.
#[test]
#[ignore]
fn elle_reconfig() {
    let ccfg = ClusterCfg { spare_node: true, ..ClusterCfg::default() };
    run_pass(
        "reconfig",
        ccfg,
        20_000,
        70,
        Duration::from_millis(1200),
        |cluster, rng, _faults| match rng.random_range(0..4u8) {
            0 if !cluster.spare_is_voting() => cluster.kill_and_restart_leader(),
            0 => {}
            1 => cluster.crash_and_restart_random_follower_service(rng),
            2 => {
                if !cluster.spare_is_voting() {
                    cluster.partition_minority();
                    std::thread::sleep(Duration::from_millis(800));
                    cluster.heal();
                    cluster.await_reconverged(20);
                }
            }
            _ => {
                cluster.random_config_op(rng);
            }
        },
        |cluster, _faults| cluster.config_ops_accepted >= 3,
        "reconfig churn never actually reconfigured (config_ops_accepted < 3)",
    );
}
```

Note the `lin_v2.rs` reconfig capstone's hard-won tuning notes (arm gates while `spare_is_voting`, 1200 ms period) are preserved verbatim — do not "simplify" them.

- [ ] **Step 2: Run both passes through the checker, then the full five**

```bash
rm -rf /tmp/uc2-elle/purge /tmp/uc2-elle/reconfig
scripts/elle_check.sh purge reconfig
rm -rf /tmp/uc2-elle && scripts/elle_check.sh          # full clean tier, one command
```
Expected: `elle consistency check passed (quiet failover partition purge reconfig)`. Reconfig may need more than 120 s on a loaded box — `ELLE_BUDGET_SECS=240` is fine; if the non-vacuity floor isn't met within budget, check `random_config_op`'s pending-gate notes in `lincheck_v2/mod.rs` before touching pacing.

- [ ] **Step 3: Clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add uc2_node/tests/elle_v2.rs
git commit -m "feat(uc2_node): elle purge + reconfig passes — clean tier complete (elle T8)"
```

---

### Task 9: Mutation knobs in `uc2_consensus`

**Files:**
- Modify: `uc2_consensus/Cargo.toml` (add feature)
- Modify: `uc2_consensus/src/commit.rs`
- Modify: `uc2_consensus/src/election.rs`

**Interfaces:**
- Consumes: nothing new.
- Produces (Task 10 wiring relies on): feature `mutation-testing`; `CommitTracker::force_quorum_minus_one(&mut self)`; `ElectionSm::set_mutate_quorum_minus_one(&mut self, on: bool)`; `ElectionSm::set_mutate_skip_vote_order(&mut self, on: bool)` — all `#[cfg(feature = "mutation-testing")]`. `uc2_consensus` stays env-free (pure-sync posture): knobs are plain fields, set by the caller.

- [ ] **Step 1: Add the feature**

In `uc2_consensus/Cargo.toml`:

```toml
[features]
# Fault injection for the elle mutation-testing tier (scripts/elle_mutation.sh).
# NEVER on by default; knob fields are plain bools set by uc2_node's wiring —
# this crate stays env-free (pure-sync, no I/O).
mutation-testing = []
```

- [ ] **Step 2: Write the failing knob tests**

In `uc2_consensus/src/commit.rs` tests module, add:

```rust
/// Mutation tooth (elle harness): forcing quorum-1 makes a 3-node tracker
/// commit on the leader's own durable alone — the injected bug the failover
/// elle pass must catch as a lost update.
#[cfg(feature = "mutation-testing")]
#[test]
fn forced_quorum_minus_one_commits_without_any_report() {
    let mut t = CommitTracker::new(2, 3);
    t.force_quorum_minus_one();
    // No follower reports: {own=1000, 0, 0} -> rank 1 -> own -> commits.
    assert_eq!(t.advance(1000), Some(1000));
}
```

In `uc2_consensus/src/election.rs` tests module, locate the existing test that asserts a vote is REFUSED for a candidate with a lexicographically smaller `(last_term, last_durable)` (search the tests for `log_ok` / a `RequestVote` with a lower `last_durable` asserting no grant — the constructors at lines ~1503/1967 show the arrange shape: `ElectionSm::new(cfg(1), None, &[(1, 0), (2, 4096)], 6000, 0)` is a node with durable 6000). Mirror its arrange exactly and add:

```rust
/// Mutation tooth (elle harness): with the vote-order check skipped, a
/// candidate with a SHORTER log is granted — the injected bug the failover
/// elle pass must catch as divergence/lost updates after an election.
#[cfg(feature = "mutation-testing")]
#[test]
fn skip_vote_order_grants_stale_candidate() {
    // Node 1 has durable 6000 in term 2 (the arrange shape used throughout
    // this test module, e.g. `ElectionSm::new(cfg(1), None, &[(1, 0),
    // (2, 4096)], 6000, 0)` — reuse the module's actual `cfg`/driver
    // helpers). A RequestVote from a candidate with last_durable 0 is
    // normally REFUSED by log_ok; with the knob on it must be GRANTED.
    let mut s = ElectionSm::new(cfg(1), None, &[(1, 0), (2, 4096)], 6000, 0);
    s.set_mutate_skip_vote_order(true);
    let mut out = Vec::new();
    s.handle(
        Event::RequestVote { from: 2, new_term: 3, last_term: 2, last_durable: 0 },
        &mut out,
    );
    assert!(
        out.iter().any(|a| matches!(a, Action::PersistAndSendVote { .. })),
        "stale candidate must be granted with the vote-order check skipped, got: {out:?}"
    );
}
```

(Adjust the event-driving call to the module's actual API — the tests around line 1503 show the real `handle`/driver-helper name and the `out` collection type. First confirm the control: the same test **without** `set_mutate_skip_vote_order(true)` must produce no `PersistAndSendVote` — if the module already has that refusal test, leave it as the feature-off control; if not, add the knob-off assertion as a second test.)

- [ ] **Step 3: Run to verify they fail**

Run: `cargo test -p uc2_consensus --features mutation-testing 2>&1 | tail -5`
Expected: compile error — `force_quorum_minus_one` / `set_mutate_skip_vote_order` not defined.

- [ ] **Step 4: Implement the knobs**

`commit.rs` — one method, mutating the stored quorum (no hot-path branch):

```rust
impl CommitTracker {
    /// Mutation tooth (elle harness): degrade the rank to quorum-1. Applied at
    /// construction time by ElectionSm's knob so it survives config-boundary
    /// tracker rebuilds.
    #[cfg(feature = "mutation-testing")]
    pub fn force_quorum_minus_one(&mut self) {
        self.quorum = (self.quorum - 1).max(1);
    }
}
```

`election.rs` — two cfg-gated fields on `ElectionSm` (the compiler will flag every constructor; initialize both `false` in each):

```rust
#[cfg(feature = "mutation-testing")]
mutate_quorum_minus_one: bool,
#[cfg(feature = "mutation-testing")]
mutate_skip_vote_order: bool,
```

Setters (in the main `impl ElectionSm`):

```rust
/// Mutation tooth: commit at quorum-1. Also degrades the CURRENT tracker so
/// wiring order (construct -> set) works.
#[cfg(feature = "mutation-testing")]
pub fn set_mutate_quorum_minus_one(&mut self, on: bool) {
    self.mutate_quorum_minus_one = on;
    if on {
        self.tracker.force_quorum_minus_one();
    }
}
/// Mutation tooth: grant votes ignoring the (last_term, last_durable) order.
#[cfg(feature = "mutation-testing")]
pub fn set_mutate_skip_vote_order(&mut self, on: bool) {
    self.mutate_skip_vote_order = on;
}
```

(If the tracker field's name differs from `tracker`, use the actual field — line ~1317 shows `self.tracker = CommitTracker::new(...)`.)

Apply at BOTH `CommitTracker::new` sites in `election.rs` (lines ~387 and ~1317 — construction and the M7 rebuild-at-boundary), immediately after construction:

```rust
#[cfg(feature = "mutation-testing")]
if self.mutate_quorum_minus_one {
    tracker.force_quorum_minus_one();
}
```

(At the line-387 site, which runs inside `new` before `self` exists, apply it from the local knob value being initialized — if that site initializes the field to `false` unconditionally, the setter's degrade-current-tracker behavior covers it; only the ~1317 rebuild site strictly needs the re-apply.)

`log_ok` (line ~1160) gets the skip:

```rust
fn log_ok(&self, last_term: u32, last_durable: u64) -> bool {
    #[cfg(feature = "mutation-testing")]
    if self.mutate_skip_vote_order {
        return true;
    }
    let our_term = self.term_map.last().map(|(t, _)| *t).unwrap_or(0);
    (last_term, last_durable) >= (our_term, self.durable)
}
```

- [ ] **Step 5: Run tests both ways**

```bash
cargo test -p uc2_consensus --features mutation-testing 2>&1 | tail -3
cargo test -p uc2_consensus 2>&1 | tail -3
```
Expected: PASS both. The feature-off run proves the default build is untouched (knob code compiles out); the feature-on run proves inertness of unset knobs (all pre-existing tests still pass) plus the two new teeth.

- [ ] **Step 6: Clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p uc2_consensus --all-targets --features mutation-testing -- -D warnings
git add uc2_consensus
git commit -m "feat(uc2_consensus): mutation-testing knobs — commit quorum-1 + vote-order skip (elle T9)"
```

---

### Task 10: `uc2_node` mutation module, wiring, and the read-barrier skip

**Files:**
- Create: `uc2_node/src/mutation.rs`
- Modify: `uc2_node/Cargo.toml` (forwarding feature)
- Modify: `uc2_node/src/lib.rs` (cfg-gated `mod mutation;`)
- Modify: `uc2_node/src/node.rs` (wiring at the two `ElectionSm::new` sites + the PendingRead phase skip)

**Interfaces:**
- Consumes: Task 9's setters.
- Produces: env contract `UC2_MUTATION` ∈ {`commit-quorum-minus-one`, `skip-vote-order-check`, `skip-read-barrier`} (unset/empty = none; unknown = panic), read once via `OnceLock`. Task 11's script sets it.

- [ ] **Step 1: Feature forwarding**

`uc2_node/Cargo.toml`:

```toml
[features]
# Fault injection for the elle mutation-testing tier. Off in every normal
# build; even when compiled in, inert unless UC2_MUTATION is set.
mutation-testing = ["uc2_consensus/mutation-testing"]
```

- [ ] **Step 2: Write the module with failing parse tests**

Create `uc2_node/src/mutation.rs` (port of ultima_db's `src/mutation.rs` pattern):

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Elle mutation-testing fault injection (design spec 2026-07-15,
//! scripts/elle_mutation.sh). Compiled ONLY under `--features
//! mutation-testing`; even then inert unless `UC2_MUTATION` names a mutation.
//! The env var is read exactly once (OnceLock); an unknown value panics so a
//! typo'd mutation run can never silently test nothing.

use std::sync::OnceLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mutation {
    /// CommitTracker ranks at quorum-1: commit without a real quorum.
    CommitQuorumMinusOne,
    /// ElectionSm grants votes ignoring the (last_term, last_durable) order.
    SkipVoteOrderCheck,
    /// Linearizable reads skip the READ_PROBE quorum barrier (stale reads —
    /// a pure real-time anomaly, caught only by the strict elle model).
    SkipReadBarrier,
}

fn parse(v: Option<&str>) -> Option<Mutation> {
    match v {
        None | Some("") => None,
        Some("commit-quorum-minus-one") => Some(Mutation::CommitQuorumMinusOne),
        Some("skip-vote-order-check") => Some(Mutation::SkipVoteOrderCheck),
        Some("skip-read-barrier") => Some(Mutation::SkipReadBarrier),
        Some(other) => panic!("unknown UC2_MUTATION value: {other:?}"),
    }
}

/// The active mutation, if any. Env read once, process-wide.
pub(crate) fn active() -> Option<Mutation> {
    static ACTIVE: OnceLock<Option<Mutation>> = OnceLock::new();
    *ACTIVE.get_or_init(|| parse(std::env::var("UC2_MUTATION").ok().as_deref()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_maps_known_values() {
        assert_eq!(parse(None), None);
        assert_eq!(parse(Some("")), None);
        assert_eq!(parse(Some("commit-quorum-minus-one")), Some(Mutation::CommitQuorumMinusOne));
        assert_eq!(parse(Some("skip-vote-order-check")), Some(Mutation::SkipVoteOrderCheck));
        assert_eq!(parse(Some("skip-read-barrier")), Some(Mutation::SkipReadBarrier));
    }

    #[test]
    #[should_panic(expected = "unknown UC2_MUTATION")]
    fn parse_panics_on_unknown() {
        parse(Some("skip-everything"));
    }
}
```

In `uc2_node/src/lib.rs`, next to the other module declarations:

```rust
#[cfg(feature = "mutation-testing")]
pub(crate) mod mutation;
```

Run: `cargo test -p uc2_node --features mutation-testing --lib mutation 2>&1 | tail -3`
Expected: the two parse tests PASS (they're self-contained; "failing first" here is the compile step).

- [ ] **Step 3: Wire the consensus knobs**

In `uc2_node/src/node.rs`, immediately after **each** of the two `ElectionSm::new(...)` call sites (lines ~477 and ~3219 — boot and whatever the second construction context is; read the surrounding code first), insert (adjusting the binding name to the site's, e.g. `sm`):

```rust
#[cfg(feature = "mutation-testing")]
match crate::mutation::active() {
    Some(crate::mutation::Mutation::CommitQuorumMinusOne) => {
        sm.set_mutate_quorum_minus_one(true)
    }
    Some(crate::mutation::Mutation::SkipVoteOrderCheck) => sm.set_mutate_skip_vote_order(true),
    _ => {}
}
```

- [ ] **Step 4: Wire the read-barrier skip**

In `uc2_node/src/node.rs`, find the production `PendingRead` construction (search `phase: ReadPhase::AwaitQuorum` — take the non-`#[cfg(test)]` site, in the linearizable-read admission path near the "ReadIndex barrier" comment at line ~1263). Immediately after the `PendingRead` value is built (before it is pushed to `pending_reads`), insert:

```rust
// Mutation tooth: skip the READ_PROBE quorum barrier entirely — the read is
// served from local applied state without confirming leadership. A deposed
// leader then answers stale reads (the elle partition pass catches this
// under the strict model).
#[cfg(feature = "mutation-testing")]
if matches!(crate::mutation::active(), Some(crate::mutation::Mutation::SkipReadBarrier)) {
    read.phase = ReadPhase::AwaitApplied;
}
```

(Adjust the binding name `read` to the site's. If the site builds the struct inline in a `push`, bind it to a `let mut` first.)

- [ ] **Step 5: Verify inertness two ways**

```bash
cargo test -p uc2_node 2>&1 | tail -3                                  # feature off: untouched
cargo test -p uc2_node --features mutation-testing 2>&1 | tail -3      # feature on, env unset: inert
```
Expected: both PASS with the same test outcomes (the second run adds only the two `mutation::tests`). `UC2_MUTATION` must not be set in the environment for the second run.

- [ ] **Step 6: Clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p uc2_node --all-targets --features mutation-testing -- -D warnings
git add uc2_node
git commit -m "feat(uc2_node): UC2_MUTATION module + consensus/read-barrier wiring (elle T10)"
```

---

### Task 11: `scripts/elle_mutation.sh` — control + three catches

**Files:**
- Create: `scripts/elle_mutation.sh` (mode 755)

**Interfaces:**
- Consumes: Tasks 6 (check script's classify pattern), 10 (env contract), 7 (failover/partition passes).
- Produces: the teeth proof — `scripts/elle_mutation.sh` exits 0 iff the control is clean AND all three mutations flip the verdict.

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# UC v2 elle mutation testing (design spec 2026-07-15 §4.7): prove the elle
# harness catches injected consensus bugs. Control first (feature compiled in,
# UC2_MUTATION unset -> clean checks must pass), then each mutation under its
# catching pass, asserting the previously-clean verdict FLIPS to invalid.
# If a mutation is not caught: RAISE contention/fault rate, never weaken the
# assertion.
set -euo pipefail

JAVA="${JAVA:-java}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$ROOT/tools/elle-cli/elle-cli-0.1.9-standalone.jar"
MUT_DIR="${ELLE_MUT_DIR:-/tmp/uc2-elle-mut}"
STRICT_MODEL="${ELLE_STRICT_MODEL:-strong-serializable}"
# Mutation runs are sized down: the anomaly only needs to occur once.
OPS="${ELLE_MUTATION_OPS:-10000}"
BUDGET="${ELLE_MUTATION_BUDGET_SECS:-180}"

command -v "$JAVA" >/dev/null 2>&1 || { echo "error: java not found" >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { echo "error: jq not found" >&2; exit 1; }

# gen <subdir> <pass> [UC2_MUTATION value]: run the driver (feature build).
gen() {
    local sub="$1" pass="$2" mut="${3:-}"
    rm -rf "$MUT_DIR/$sub"
    (cd "$ROOT" && UC2_MUTATION="$mut" ELLE_DIR="$MUT_DIR/$sub" \
        ELLE_TARGET_OPS="$OPS" ELLE_BUDGET_SECS="$BUDGET" \
        cargo test -p uc2_node --release --features mutation-testing \
        --test elle_v2 -- --ignored --exact "elle_$pass" --nocapture)
    echo "$MUT_DIR/$sub/$pass/history.edn"
}

# strict_valid <history>: echoes the strict-model "valid?" (true|false).
strict_valid() {
    local out
    out="$("$JAVA" -jar "$JAR" --model list-append --consistency-models "$STRICT_MODEL" --verbose "$1")" || true
    printf '%s' "$out" | jq -r '.["valid?"]|tostring' 2>/dev/null \
        || { echo "error: no JSON from elle-cli on $1" >&2; exit 1; }
}

echo "== control: feature-on build, UC2_MUTATION unset — must be clean (inertness) =="
h="$(gen control failover "")"
v="$(strict_valid "$h")"
[ "$v" = "true" ] || { echo "FAIL: feature-on control not clean (valid?=$v) — feature not inert" >&2; exit 1; }
echo "OK: control clean"

check_caught() { # <mutation> <pass>
    echo "== mutation: $1 (pass: $2) — clean verdict must flip =="
    local h v
    h="$(gen "$1" "$2" "$1")"
    v="$(strict_valid "$h")"
    if [ "$v" != "false" ]; then
        echo "FAIL: $1 NOT caught (strict valid?=$v) — no teeth; raise contention" \
             "(ELLE_MUTATION_OPS, fault rate), never weaken this assertion" >&2
        exit 1
    fi
    echo "OK: $1 CAUGHT"
}

check_caught commit-quorum-minus-one failover
check_caught skip-vote-order-check   failover
check_caught skip-read-barrier       partition

echo "elle mutation testing passed: control clean, 3/3 mutations caught"
```

`chmod 755 scripts/elle_mutation.sh`. Match the `STRICT_MODEL`/`JAR` values to Task 6's.

- [ ] **Step 2: Run it**

Run: `scripts/elle_mutation.sh`
Expected: `OK: control clean`, then `OK: ... CAUGHT` three times.

Debugging expectations if a catch misses (in likelihood order):
- `commit-quorum-minus-one`: the lost-update window is the acked-but-not-follower-durable tail at kill time — raise fault rate (add a temporary faster `fault_period` via a dedicated env if needed) or ops.
- `skip-vote-order-check`: needs the *staler* follower to win an election — more kill cycles (raise `ELLE_MUTATION_BUDGET_SECS` so more faults land).
- `skip-read-barrier`: needs a client reading from the isolated leader during the ~800 ms window — raise the isolation hold or read ratio. A **liveness-gate failure** on the mutated partition run is acceptable to relax via `min_ok_pct` env only if it blocks history generation; the flip assertion itself is untouchable.

If a driver-level knob (fault period / read ratio / isolation hold) needs to be env-tunable to land a catch, add the env var to `elle_v2.rs` with the current value as default and note it in the script.

- [ ] **Step 3: Re-run the clean tier (no cross-contamination)**

```bash
rm -rf /tmp/uc2-elle && scripts/elle_check.sh
```
Expected: still fully green (clean builds don't carry the feature; this guards against accidental default-feature leaks).

- [ ] **Step 4: Commit**

```bash
git add scripts/elle_mutation.sh
git commit -m "feat(scripts): elle_mutation.sh — control + 3 consensus-bug catches (elle T11)"
```

---

### Task 12: CI, CLAUDE.md, runbook

**Files:**
- Modify: `.github/workflows/nightly.yml` (add `elle` job)
- Create: `.github/workflows/elle-weekly.yml`
- Modify: `CLAUDE.md` (build/test command block)
- Modify: `docs/ops/uc2-runbook.md` (new section)

**Interfaces:**
- Consumes: Tasks 6 + 11 scripts.
- Produces: the nightly clean tier + weekly mutation tier; operator docs.

- [ ] **Step 1: Nightly `elle` job**

In `.github/workflows/nightly.yml`, add a job alongside the existing ones — copy the existing jobs' checkout/toolchain/cache steps verbatim, then:

```yaml
  elle:
    runs-on: ubuntu-latest
    steps:
      # ... (checkout + rust toolchain + cache steps copied from the capstones job)
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: '21'
      - name: Elle clean passes (5 passes, reduced sizing for hosted runners)
        run: ELLE_TARGET_OPS=8000 ELLE_BUDGET_SECS=300 scripts/elle_check.sh
```

(`jq` ships on ubuntu-latest; the jar is vendored — nothing downloads. Sizing: 8k ops keeps elle-cli's cycle search well clear of `unknown` on a 4-vCPU runner; the budget mirrors the capstones' `UC2_LIN_BUDGET_SECS=240` widening precedent.)

- [ ] **Step 2: Weekly mutation workflow**

Create `.github/workflows/elle-weekly.yml` (same header/permissions style as `nightly.yml`):

```yaml
name: elle-weekly
on:
  schedule:
    - cron: '17 4 * * 0' # Sundays 04:17 UTC (offset from nightly's 03:17)
  workflow_dispatch:

jobs:
  elle-mutation:
    runs-on: ubuntu-latest
    steps:
      # ... (checkout + rust toolchain + cache steps copied from nightly.yml)
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: '21'
      - name: Elle mutation suite (control + 3 catches)
        run: ELLE_MUTATION_OPS=6000 ELLE_MUTATION_BUDGET_SECS=300 scripts/elle_mutation.sh
```

- [ ] **Step 3: CLAUDE.md commands**

In `CLAUDE.md`'s Build & Test Commands block, add after the m7_gate/uc2ctl lines:

```bash
scripts/elle_check.sh                            # elle consistency tier: 5 list-append passes, needs java+jq
scripts/elle_mutation.sh                         # elle mutation testing: 3 injected consensus bugs must be caught
```

- [ ] **Step 4: Runbook section**

Append to `docs/ops/uc2-runbook.md`:

```markdown
## Reading an elle failure

`scripts/elle_check.sh` FAILing on a pass means elle found a dependency cycle
or an aborted/stale read that linearizability forbids — a real consistency
bug, not flake. The history is the reproducible artifact
(`$ELLE_DIR/<pass>/history.edn`, seed in the `seed` sidecar). Re-run elle-cli
by hand for per-anomaly explanations + SVG cycle plots:

    java -jar tools/elle-cli/elle-cli-0.1.9-standalone.jar --model list-append \
        --consistency-models strong-serializable --directory out/ \
        /tmp/uc2-elle/failover/history.edn

- `unknown` verdicts are cycle-search timeouts: shrink `ELLE_TARGET_OPS` —
  never accept an unknown as a pass.
- A `serializable`-clean but strict-model-dirty history = a real-time (stale
  read) violation: suspect the READ_PROBE barrier / leader-change path.
- Mutation runs (`scripts/elle_mutation.sh`) invert the assertion: a mutation
  that is NOT caught means the harness lost its teeth — raise contention
  (`ELLE_MUTATION_OPS`, fault rate); never weaken the flip assertion.
```

(Use the strict model name pinned in Task 3 if it differs.)

- [ ] **Step 5: Validate + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
# YAML sanity: python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/nightly.yml')); yaml.safe_load(open('.github/workflows/elle-weekly.yml'))"
git add .github/workflows/nightly.yml .github/workflows/elle-weekly.yml CLAUDE.md docs/ops/uc2-runbook.md
git commit -m "ci+docs: nightly elle tier, weekly mutation suite, runbook + CLAUDE.md (elle T12)"
```

---

## Completion

After Task 12: run the full local proof stack once —

```bash
cargo test 2>&1 | tail -5                       # default suite untouched
rm -rf /tmp/uc2-elle && scripts/elle_check.sh   # clean tier green
scripts/elle_mutation.sh                        # teeth proven
cargo clippy --workspace --all-targets -- -D warnings
```

Then write the record doc `docs/benchmarks/uc2-elle-gate-2026-07-XX.md` (v2 convention): pass sizes, event counts, verdicts, mutation catch table, elle-cli version + strict model name, and any sizing gotchas found — mirror the structure of `docs/benchmarks/uc2-m7-gate-2026-07-13.md`. CI proof (nightly `elle` job green) lands on its own schedule; a `workflow_dispatch` run of `elle-weekly` is the immediate way to validate both workflows after push.
