# UC v2 Elle Consistency Harness (in-process) — Design

**Date:** 2026-07-15
**Status:** approved (brainstorm 2026-07-15)
**Prior art:** `../ultima_db` task45/task47 (`docs/tasks/task45_elle_consistency_harness.md`,
`task47_elle_anomaly_and_mutation.md`, `docs/consistency-verification-elle-2026-07-07.md`)
— the list-append driver, vendored elle-cli, anomaly classification, and
mutation-testing patterns are ported from there, adapted from a transactional
embedded store to a linearizable SMR cluster.

## 1. Context and goal

UC v2's correctness stack proves linearizability three ways (deterministic sim,
WGL lincheck capstones, SIGKILL crashtest), but the WGL checker is exponential
in the concurrency window: the capstones throttle workers at 20–150 ms/op and
cap histories at ~1.5k entries (`lin_v2.rs` reconfig capstone carries a
`MAX_OPS = 1500` guard because ~4k entries blew the checker's stack). The
histories the fleet gates generate at real throughput are unadjudicated.

[Elle](https://github.com/jepsen-io/elle) — the checker behind the Jepsen
analyses — infers dependency cycles from **list-append** histories in
near-linear time, so it can adjudicate histories of 10^5+ events from
**unthrottled** workers under the full fault mix. This harness adds elle as a
fourth proof tier: in-process (this design), fleet/Jepsen-style later.

Scope decision (user, 2026-07-15): **in-process first**, over the existing
`LinClusterV2` harness (real UDP on loopback, real instance dirs, real
node/service agents — the fidelity gap vs multi-process is process isolation
only, already covered by the crashtest). **Mutation testing is in v1.**

## 2. Claims checked

Each pass produces one list-append history (singleton txns: one `append` or
one `read` per txn) and must be:

- **`serializable`: valid, anomaly set = ∅**
- **`strong-serializable`: valid, anomaly set = ∅** — the model that checks
  real-time order, i.e. the actual linearizability claim. A stale read that
  plain serializability would legally reorder into the past is caught only
  here.

No anomaly whitelist exists (unlike ultima_db's SI ⊆ `{G2-item}`): UC claims
linearizability, so **any** anomaly on any pass is a hard FAIL. An `unknown`
verdict (elle cycle-search timeout/OOM) is also a hard FAIL — the response is
to shrink the history sizing, never to accept it.

Implementation note: verify the vendored elle-cli accepts
`--consistency-models strong-serializable` (elle names the model
`:strong-serializable`). If the 0.1.9 jar predates the flag, vendor the newer
elle-cli release that has it instead — the assertion stands regardless of jar
version.

## 3. Non-goals (v1)

- Fleet/Jepsen-style runs over real hosts + nemesis (`bench-infra`) — the
  separate, later step already earmarked in ultima_db's elle doc.
- Multi-process driver in `examples/uc_crashtest` — named follow-up: point the
  same EDN writer at the crashtest harness.
- Multi-op transactions / predicate workloads — UC's SDK surface is single
  commands + queries; singleton-txn list-append is the faithful encoding.
- PR gating — cluster timing is nondeterministic and the checker needs Java;
  this runs in the nightly proofs tier.
- Replacing WGL — the capstones stay untouched; elle is additive scale, not a
  substitute oracle (WGL adjudicates CAS semantics elle's list-append cannot).

## 4. Components

### 4.1 `ListAppendSm` (`uc_lincheck/src/list_append.rs`)

Beside `RegisterSm`, same posture (plain in-memory, persists nothing — it is a
reconstruction proof object):

```rust
pub enum LaCmd  { Append { key: u32, val: u64 } }      // Command
pub enum LaResp { AppendAck }                           // Response
pub struct LaRead { pub key: u32 }                      // Query
// QueryResponse = Vec<u64>  (the list at `key`; empty if never appended)
```

- State: `BTreeMap<u32, Vec<u64>>` + `last_applied: Option<u64>`.
- Implements `uc_service::StateMachine` (behind the existing `v2` feature)
  and `SnapshotStateMachine` (bincode of `(map, last_applied)`, install
  rejects a mis-tagged position — mirrors `RegisterSm`) so the purge pass
  drives the real snapshot/purge path.
- Unit tests mirror `register.rs`: apply/query roundtrip via the v2 trait,
  snapshot roundtrip + mis-tag refusal.

### 4.2 EDN history writer (`uc_lincheck/src/edn.rs`)

Pure module, zero new deps, hand-formatted EDN (straight port of ultima_db's
`elle-history` encoding): one map per line,
`{:index N, :type :invoke|:ok|:fail|:info, :f :txn, :process P, :time NS,
:value [[:append K V]]}` / `[[:r K nil-or-[V ...]]]`.

- A thread-safe recorder in the spirit of `uc_lincheck::History`: global
  `AtomicU64` index; `:time` from a monotonic clock (nanos since run start).
  An `:invoke` line is emitted at invoke (not retroactively at completion) so
  concurrency is captured even for ops that never return.
- **Process-id allocation** lives here: `retire(process) -> new_process` hands
  out fresh Jepsen process ids (see §4.4).
- Unit tests: EDN escaping/format golden tests, invoke/complete index
  ordering, uniqueness of append values across retries, process retirement.

### 4.3 Driver (`uc_node/tests/elle_v2.rs`)

An integration-test target (not an example bin: it shares
`tests/lincheck_v2/mod.rs` via `#[path]`, exactly like the four capstones).
All tests `#[ignore]`d — never in default `cargo test`; invoked explicitly by
the scripts:

```bash
cargo test -p uc_node --release --test elle_v2 -- --ignored elle_quiet
```

One `#[ignore]`d test per pass (`elle_quiet`, `elle_failover`, `elle_purge`,
`elle_reconfig`); each starts its own `LinClusterV2` with the pass's
`ClusterCfg`, spawns **unthrottled** seeded workers (`StdRng` per worker,
seeded `ELLE_SEED ^ worker_index`; op mix ~50/50 append/read over
`ELLE_KEYS` hot keys; append values from one global `AtomicU64`), runs the
pass's nemesis arms on the scheduler thread, and writes
`$ELLE_DIR/<pass>/history.edn` on completion. Env knobs (idiom precedent:
`LIN_SEED`, `UC2_LIN_BUDGET_SECS`):

| Env | Default | Meaning |
|---|---|---|
| `ELLE_DIR` | `/tmp/uc2-elle` | history output root (never under `target/`) |
| `ELLE_SEED` | `0x1107` | op-generation + nemesis seed |
| `ELLE_WORKERS` | 4 | client worker threads |
| `ELLE_KEYS` | 8 | hot keyspace size |
| `ELLE_TARGET_OPS` | 50_000 | stop after this many completed ops (quiet pass; faulted passes default 20_000) |
| `ELLE_BUDGET_SECS` | 120 | hard wall-clock guard per pass |

The instance-dir tempdir uses `CARGO_TARGET_TMPDIR` (ext4), same as `lin_v2`
(journal segments would blow the tmpfs `/tmp` quota). Liveness gate per pass:
≥ 80 % `:ok`, plus each pass's non-vacuity condition (§4.5) — a pass that
fails liveness/vacuity exits nonzero **before** any elle verdict is trusted.

### 4.4 Error → EDN mapping (correctness-critical)

Elle's `:fail` means **definitely did not commit**; getting this wrong turns
the checker into a false-alarm (or false-pass) machine. Mapping from
`uc_client::ClientError`, informed by the existing `lincheck_v2` worker's
outcome routing:

| Outcome | EDN | Rationale |
|---|---|---|
| `Ok(resp)` | `:ok` | committed, response observed |
| `NotLeader`, `BackpressureFull`, `Retry` | `:fail` (appends) | refused at the door, pre-append — guaranteed no commit. The worker then re-routes/retries as a **fresh invocation** (fresh value), never re-issues the same op |
| `Timeout`, `InstanceRestart`, `ResponseOverwritten`, `Decode`, `ShutDown` (mid-flight) | `:info` | in-limbo: may have committed (`ResponseOverwritten`/`Decode` = committed but response lost) |
| any error on a **read** | `:fail` | reads have no side effect; a failed read definitely didn't happen |

After an `:info`, the worker **retires its process id** and continues under a
fresh one (a Jepsen process may not issue ops after an indeterminate outcome).
This differs from the WGL harness's `Outcome::Indeterminate` handling and gets
its own unit tests in `edn.rs` plus a driver-level assertion (no process id
ever appears after its `:info`).

### 4.5 Passes

All four nemesis arms already exist on `LinClusterV2` — the nemesis layer is
reuse. Fault pacing follows the capstones (~1–1.2 s period, quorum-preserving,
one fault at a time, recovery-gated).

| Pass | ClusterCfg | Nemesis arms | Non-vacuity |
|---|---|---|---|
| `quiet` | default, 3 nodes | none | — (baseline + largest history: the cycle-search load test) |
| `failover` | default, 3 nodes | leader kill+restart; leader service crash+restart (50/50) | ≥ 3 faults landed |
| `purge` | `BelowSnapshot{slack:0}`, 16 KiB segments, 32 KiB snapshot cadence | leader kill; leader service crash; random follower service crash (1/3 each) | archive floor advanced (`max_archive_first_base() > 0`) |
| `reconfig` | `spare_node: true` | leader kill (gated on `!spare_is_voting()`); follower service crash; partition-minority + heal (gated); `random_config_op` (1/4 each — the `lin_v2` reconfig arm table verbatim) | `config_ops_accepted >= 3` |

### 4.6 Checker machinery (ported from ultima_db)

- **`tools/elle-cli/`** — vendored elle-cli standalone jar (EPL-2.0, sha256
  pinned in its README; copy ultima_db's 0.1.9 unless the
  `strong-serializable` flag requires a newer release, §2). No Clojure
  toolchain; runtime deps are `java` (Temurin 21) + `jq`, checked up front.
- **Fixtures** (`tools/elle-cli/fixtures/`), self-tested before any real
  verdict is trusted:
  1. a cycle fixture (e.g. lost-update) that must be **rejected** under
     `serializable` — proves the checker catches dependency cycles;
  2. a **real-time-only** fixture: two singleton txns where op B begins after
     op A's `:ok` yet reads state excluding A's append — legal under plain
     `serializable`, must be **rejected** under `strong-serializable`. This
     self-tests exactly the model distinction the linearizability claim rides
     on (lesson from ultima_db: their first hand-written fixture was legally
     serializable; fixtures must be validated, not assumed).
- **`scripts/elle_check.sh`** — port of ultima_db's: runs the fixture
  self-tests, then for each pass history runs elle-cli `--verbose` under both
  models, parses the JSON via `jq` into `"<valid?>|<sorted anomaly-types>"`,
  and asserts exactly `true|`. Parse failure = hard FAIL (never "no anomalies
  found"); `unknown` = hard FAIL. Also invokes the driver (all four passes)
  when histories are absent, so `scripts/elle_check.sh` is the one-command
  entry point.

### 4.7 Mutation testing (the teeth — in v1)

A checker that never fails is worthless; three injected consensus bugs prove
each surface (write/commit, election, read) has a proven tooth.

**Injection mechanism.** A `mutation-testing` cargo feature, **off in every
normal build** (default `cargo build`/`test`/sim/gates never compile it).
`UC2_MUTATION` is read exactly once via `OnceLock` in **`uc_node`** (a
`mutation` module mirroring ultima_db's `src/mutation.rs`: known values map to
the enum, unset/empty = `None`, unknown value = `panic!`). `uc_consensus`
stays env-free to preserve its pure-sync/no-I/O posture: the consensus-side
sites are `#[cfg(feature = "mutation-testing")]` knob fields on the affected
components (default `false`), set by `uc_node` at wiring time from
`mutation::active()`. The feature forwards `uc_node/mutation-testing →
uc_consensus/mutation-testing`.

**Safety invariant: feature-on + env-unset = byte-for-byte normal behavior**,
verified two ways: the full test suite passes with the feature compiled in and
env unset, and the mutation driver's control run (below) requires the clean
elle checks to pass on a feature-on build before attempting any mutation.

**Injection sites** (one statement each):

| `UC2_MUTATION` value | Site | Bug injected | Caught by |
|---|---|---|---|
| `commit-quorum-minus-one` | `uc_consensus::CommitTracker` | commit advances at the (quorum−1)-th highest durable position | `failover` pass: an acked append durable on the leader alone dies with the kill → lost update / aborted read |
| `skip-vote-order-check` | `uc_consensus::ElectionSm` | vote granted ignoring the lexicographic `(last_term, last_durable)` comparison | `failover` pass: a stale leader elected over a longer log → divergence / lost updates |
| `skip-read-barrier` | `uc_node` linearizable-read path | `query_linearizable` skips the READ_PROBE/ACK quorum barrier | **`strong-serializable` only**: stale reads are pure real-time anomalies, invisible to plain serializability — this tooth proves both the read path and the strict model |

**`scripts/elle_mutation.sh`** — port of ultima_db's: builds the driver once
with `--features mutation-testing`, then (1) **control**: env unset, failover
pass, clean checks must pass ("feature not inert" hard-fail otherwise);
(2)–(4) each mutation under its catching pass, asserting the previously-clean
verdict **flips to invalid** under the listed model. The assertion is
inverted (verdict flips), not anomaly-name-matched — robust to which anomaly
type elle assigns. If a mutation is not reliably caught, **raise contention /
fault rate, never weaken the assertion** (ultima_db rule). Expected knob:
`skip-read-barrier` may need a follower-read or read-heavy mix to fire
reliably; the driver exposes the op mix via env for this.

**Named follow-up tooth:** `skip-tombstone-check` in the M7 config-change
predicate (the absence-vs-tombstone bug class), caught under the `reconfig`
pass. Deferred only because it needs the reconfig nemesis interplay tuned
first.

### 4.8 CI

Extends the existing nightly-proofs split (precedent: ultima_db's
`consistency.yml`):

- **Nightly**: the four clean passes + `elle_check.sh` (Temurin 21
  provisioned per-job; jar vendored so nothing downloads; `jq` on the
  runner). Reduced sizing via env if runner speed demands it — sized to stay
  clear of `unknown`.
- **Weekly + `workflow_dispatch`**: the mutation suite (feature-enabled
  rebuild + 4 generations make it too heavy nightly).
- **Never on the PR fast tier** (v1).

## 5. Testing the harness itself

- `edn.rs`: golden-format tests, process-retirement semantics, index/time
  monotonicity, append-value uniqueness.
- `list_append.rs`: v2-trait roundtrip, snapshot roundtrip + mis-tag refusal.
- Fixture self-tests run at the top of every `elle_check.sh` invocation
  (checker distrust is permanent, not a one-time validation).
- Mutation control run (inertness) + three catch assertions.
- `uc_node::mutation` parse unit tests (known values, panic-on-unknown),
  compiled only under the feature.
- Determinism: op generation and nemesis scheduling are seeded; cluster
  interleaving is not (same posture as the capstones — the *history* is the
  reproducible artifact, dumped on failure).

## 6. Reading a failure

Runbook section (`docs/ops/uc2-runbook.md`): a FAIL means elle found a cycle
or aborted/stale read the model forbids — re-run elle-cli by hand with
`--directory out/` for per-anomaly explanations + SVG cycle plots:

```bash
java -jar tools/elle-cli/elle-cli-*-standalone.jar --model list-append \
    --consistency-models strong-serializable --directory out/ \
    /tmp/uc2-elle/failover/history.edn
```

Histories persist under `$ELLE_DIR/<pass>/` with the seed in a sidecar file.

## 7. Docs & conventions

- CLAUDE.md build-commands block gains the two script entry points.
- Record doc `docs/benchmarks/uc2-elle-gate-<date>.md` when the harness lands
  and all passes are green (v2 convention: the gate doc is the permanent
  record).
- This spec + plan stay under `docs/superpowers/` (retained artifacts).

## 8. Follow-ups (out of v1, named)

1. `skip-tombstone-check` mutation (reconfig tooth).
2. Multi-process pass: EDN writer over the `uc_crashtest` harness (`kill -9`
   process isolation).
3. Fleet/Jepsen-style run over `bench-infra` hosts with a real nemesis
   (iptables partitions, `uc2ctl` reconfig under load) — the "full Jepsen for
   ultima_cluster" step from ultima_db's elle doc.
4. Snapshot-read pass (`query_snapshot`) asserting a weaker model — needs its
   own model decision; deliberately out of scope.
