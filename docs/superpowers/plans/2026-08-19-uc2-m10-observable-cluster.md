# UC v2.3+ M10 — observable cluster (metrics, structured logs, probes, alerts) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** make a running cluster observable without touching its hot path — an in-daemon `/metrics` endpoint over the cnc page the daemon already holds, transition-triggered structured log records, role-aware liveness/readiness probes that never route traffic to an elected-but-not-serving leader, and shipped Prometheus alert rules + a Grafana dashboard, each rule proven to fire.

**Architecture:** one new module tree, `uc2_node/src/obs/`, and nothing else structural. `obs::log` is a zero-dependency JSON-lines emitter with a global level filter; transition records are emitted at the consensus `exec` sites (all of which already live in `uc2_node`) and derived edge-triggered from counters in the daemon's existing 100 ms poll loop for events owned by other crates (NAK storms, seal failures) — no other crate is touched for logging. `obs::metrics` renders Prometheus text from an `ObsSources` bundle of `Arc` clones the `Node` already holds (cnc page, sender/receiver stats, truncation/wipe counters, agent-health flags). `obs::http` is a hand-rolled `std::net` GET-only responder on its own thread serving `/metrics`, `/healthz`, `/readyz` — no tokio, no HTTP crate (none exists in the tree, and the M9 daemon is fully synchronous). The M9-reserved `[log]`/`[metrics]` config sections gain their schema, replacing the presence-only `ReservedSections` plumbing.

**Tech Stack:** Rust workspace (edition 2024), `uc2_node` only (obs modules, config schema, daemon wiring, gate), `uc2_log` (one small `AgentRunner` addition: a shared finished-flag), `serde`+`toml` (config), `std::net::TcpListener` (exporter), hand-formatted JSON (no `serde_json` — it is not in the workspace and the records are flat scalars). Alert-rule verification uses `promtool` (external, like elle needs `java`+`jq`).

**Spec:** `docs/superpowers/specs/2026-08-19-uc2-production-readiness-design.md` §5 (M10), §1 (locked decisions: in-daemon endpoint over cnc attach, no sidecar), §3 (non-goals).

## Global Constraints

Copied from the spec and CLAUDE.md house rules. Every task's requirements implicitly include this section.

- **No consensus, wire-protocol, or cnc-layout changes.** M10 reads the page; it adds no field and moves no offset. If a task appears to need a new cnc field, stop — derive the series from what is there or from an existing accessor (spec §3).
- **The four polling agents must not take an allocation per record.** Transition-triggered logging only: elections, truncations, snapshot installs, config transitions, NAK storms, seal failures, fail-stops. One structured record per transition, never one per operation. A counter increment is not a log site.
- **The scrape must not perturb the hot path.** The exporter thread reads atomics with `load_acquire` and never takes a lock the agents hold (there is none to take — every cnc field is a padded atomic with one writer). The gate proves this with a fleet A/B rather than assuming it (spec §5).
- **No tokio in `uc2_node`.** The exporter is a plain blocking thread over `std::net`. No async runtime, no HTTP dependency.
- **`deny_unknown_fields` on every config struct** — including the new `[log]` and `[metrics]` sections. M10 defines their schema; from now on a typo inside them is a startup refusal naming the key (M9 explicitly documented their contents as unvalidated-until-M10).
- **Readiness keys on `can_serve`, not the leader flag.** `flags == 0x01` (elected, NewTerm not yet quorum-committed) must read NOT ready (spec §5).
- **Existing daemon output stays character-compatible.** The quickstart pins `uc2-node: node 0 is now LEADER (term 1)` — those lines stay. Structured records are additional lines on stderr.
- **`clippy --workspace --all-targets -- -D warnings` stays clean** (CI enforces it).
- **Journals/instance dirs never on `/tmp` for load runs** (RAM tmpfs, no swap). Unit tests may use `tempfile::tempdir()`; journal-bearing integration tests use `tempdir_in(env!("CARGO_TARGET_TMPDIR"))` (the `failover.rs` pattern).
- **Stage only your own files** — never `git add -A`. Branch: `uc2/m10-observable-cluster`. Stage `Cargo.lock` explicitly and name it when touched.
- **Honest gates:** the gate binary prints the bar and `exit(1)` on FAIL. "Task 10 complete" ≠ "M10 gate passed" — fleet rows are a separate, user-approved step (M1–M9 precedent). Perf numbers are never gated on the dev box (it measured an 18-point spread against a 10-point bar in M9; local runs are smoke).

## File Structure

| File | Responsibility |
|---|---|
| `uc2_node/src/obs/mod.rs` (new) | Module root; re-exports `log`, `metrics`, `http`, `ObsSources`. |
| `uc2_node/src/obs/log.rs` (new) | Level filter, sink, JSON-line formatting, `emit` + `obs_event!` macro. Zero deps. |
| `uc2_node/src/obs/metrics.rs` (new) | `render_prometheus(&ObsSources) -> String`. Pure function, no I/O. |
| `uc2_node/src/obs/http.rs` (new) | `ObsServer::serve(sources, bind)` — the blocking GET responder thread; `/metrics`, `/healthz`, `/readyz`. |
| `uc2_node/src/node.rs` (modify) | `ObsSources` struct + `Node::observability()`; transition-record emission at the `exec` sites; a `last_flags` edge in `publish_status`. Nothing else. |
| `uc2_log/src/agent.rs` (modify) | `AgentRunner` gains a shared `finished: Arc<AtomicBool>` set by a drop-guard in the worker thread (panic included) + `finished_flag()` accessor. |
| `uc2_node/src/config_file.rs` (modify) | Typed `[log]`/`[metrics]` sections replacing the raw `toml::Table` capture. |
| `uc2_node/src/preflight.rs` (modify) | `ObsOptions` on `StartupOptions`; `ReservedSections` retired. |
| `uc2_node/src/bin/uc2-node.rs` (modify) | Set log level, start/stop `ObsServer`, agent fail-stop detection, counter-derived NAK-storm/seal-failure/snapshot-published records in the poll loop. |
| `uc2_node/tests/obs_log.rs` (new) | Integration: election + truncation records from a real in-process cluster. |
| `uc2_node/tests/obs_http.rs` (new) | Integration: endpoint routing, probe semantics (incl. the 0x01 case via `CncPage::heap`), coverage of the series contract. |
| `uc2_node/tests/lifecycle.rs` (modify) | RESERVED-notice test replaced by acts-on-`[log]`/`[metrics]` tests. |
| `packaging/node.example.toml` (modify) | Real, annotated `[log]`/`[metrics]` sections replace the RESERVED block. |
| `packaging/prometheus/uc2-alerts.yml` (new) | The shipped alert rules. |
| `packaging/grafana/uc2-dashboard.json` (new) | The shipped dashboard. |
| `uc2_node/examples/m10_alerts.rs` (new) | Scenario driver: breaks real clusters, scrapes real `/metrics`, writes `promtool test rules` inputs. |
| `scripts/m10_alert_fire.sh` (new) | Orchestrates: run the driver, run `promtool`, one PASS/FAIL per rule. |
| `uc2_node/examples/m10_gate.rs` (new) | The pre-committed gate: coverage row + probe-regime row + local perturbation smoke. |
| `docs/benchmarks/uc2-m10-gate-2026-08-19.md` (new) | Gate doc — decide rule pre-committed before any run. |
| `docs/reference/configuration.md`, `docs/how-to/monitor-a-cluster.md` (new), `docs/how-to/README.md`, `docs/ops/uc2-runbook.md`, `docs/how-to/diagnose-a-node.md` (modify) | Documentation cutover. |

## As-built anchor map (read these before your task)

| Seam | Where |
|---|---|
| Cnc attach, read-only, cross-process proven | `uc2_log/src/cnc.rs:357` `CncPage::open_file(path, expected_app_id) -> Result<Arc<CncPage>, CncError>`; validates magic/CRC/version/app_id. `uc2ctl/src/main.rs:222-225` uses exactly this while the node writes — the concurrent-reader pattern. |
| Cnc getters | `cnc.rs:385` `counters() -> &LogCounters` (append/durable/sent/commit); `:397` `service() -> &ServiceProgress` (service_applied/service_epoch/output_completed); `:403` `status() -> &NodeStatusV2` (term/flags/leader_hint/node_heartbeat_ns/service_heartbeat_ns/output_progress); `:409` `snapshots() -> &SnapshotSlots` (service_snapshot_pos/node_snapshot_floor/incoming_snapshot_pos); `:417` `archive_first_base()`; `:424` `peer_slot(i)` (8 slots; `id_and_role` = id<<8\|role, 0 = unoccupied; `reported_durable`, `advertised_limit`, `naks_plus_replay` — the last is dormant since M6, do not export it, use sender/receiver stats instead); `:433` `config_version()`; `:480` `config_pending()`; `:450` `admission_bytes()`; `:466` `seal_failures()`; `:378` `CncPage::heap(meta)` (synthetic page for tests). All fields are `PaddedAtomicU64` read with `load_acquire()`. |
| Node flags | `uc_protocol/src/v2/cnc.rs:147-148` `NODE_FLAG_LEADER = 1`, `NODE_FLAG_CAN_SERVE = 2`. `flags == 1` IS the elected-not-serving state (no separate constant). Written by `Node::publish_status`, `uc2_node/src/node.rs:2706-2731`. |
| Node observability accessors (existing) | `node.rs:1211` `is_leader`, `:1215` `can_serve`, `:1219` `current_term`, `:1223` `counters`, `:1230` `service_applied`, `:1238` `archive_first_base`, `:1286` `crypto_handshake_failures`, `:1292` `reports_unattested`, `:1303` `crypto_stats() -> &FollowerStats`, `:1311` `config_version`, `:1364` `truncations`, `:1370` `wipes`, `:1379` `replay_datagrams`, `:1388` `reports_implausible`. |
| Stats structs | `uc2_net/src/sender.rs:171-222` `SenderStats` (all `AtomicU64`): `datagrams, bytes, naks_served, heartbeats, flow_stalls, overruns, replay_datagrams, naks_dropped, naks_rejected, snap_sessions, snap_chunks, snap_chunk_naks, seal_failures`. `uc2_net/src/receiver.rs:365-466` `FollowerStats`: `datagrams, bytes, dropped_stale_term, dropped_dup, dropped_overrun, dropped_malformed, dropped_gated, net_drops[], naks_sent, statuses_sent, append_positions_sent, truncation_resyncs, term_change_discards, counter_ahead_resyncs, dropped_straddle, dropped_auth_failed, dropped_replay, dropped_unknown_epoch, peer_appears_cleartext, dropped_unknown_peer, dropped_handshake, seal_failures`. Node holds them as `Arc`: `node.rs:373` `route_drops`, `:377` `sender_stats`. **The spec's `append_pos_unknown_source` does not exist by that name — it is `FollowerStats::dropped_unknown_peer`.** |
| Node Arc counters | `node.rs:355` `truncations: Arc<AtomicU64>`, `:359` `wipes: Arc<AtomicU64>` — cloneable into `ObsSources`. The crypto counters (`reports_unattested` etc.) are likewise `Arc<AtomicU64>` fields; verify at the field definitions when building the bundle. |
| Transition sites (all in `uc2_node/src/node.rs`, `fn exec` at `:3587`) | `Action::BecomeLeader` arm `:3621-3671` (`term`, `base` in scope); `Action::BecomeFollower` arm `:3672-3696` (`term`, `leader: Option<NodeId>`); `Action::Truncate` arm `:3731-3801` (`epoch`, `to`); `Action::CountWipe` `:3810-3815`; `Action::ConfigAdopted` arm `:3816-3877` (`position`, `config.version`, `prev_position`); `Action::HaltRemoved` `:3878-3884` and `Action::StepDownRemoved` `:3885-3895` (already `eprintln!` — convert); incoming snapshot adoption `fn maybe_adopt_incoming_snapshot` `:2527-2598` (`pos` in scope; currently fully silent). |
| Agent lifecycle | `uc2_log/src/agent.rs:47` `AgentRunner::spawn(name, idle, work)`; `:69` `is_finished()` (true mid-run == the closure panicked); `:75` `stop()` joins and **re-raises** a panic; `Drop` (`:84-90`) swallows it. `Node.agents: Vec<AgentRunner>` (`node.rs:408`) is private — the daemon cannot currently see a mid-run agent death; M10 closes this. Agent names at spawn sites: `"uc2-archive"` `:896`, sender `:1085`, receiver `:1087`, `"uc2-consensus"` `:1181` (read the sites for the exact sender/receiver names). |
| Config file seam | `uc2_node/src/config_file.rs:98,:101` `log: Option<toml::Table>`, `metrics: Option<toml::Table>` (presence-only today); `:141` `pub fn load_from_path(path) -> Result<(NodeConfig, StartupOptions), ConfigError>`; `:186` builds `ReservedSections`. `uc2_node/src/preflight.rs:150-163` `StartupOptions`, `:182-208` `ReservedSections` (to be retired). |
| Daemon loop | `uc2_node/src/bin/uc2-node.rs:90-102` — 100 ms poll, `was_leader` edge-detection, then `stop_draining`. The obs server starts before this loop and stops after it. |
| Reserved-notice tests to replace | `uc2_node/tests/lifecycle.rs:313` `daemon_starts_and_announces_the_m10_reserved_sections` (asserts `RESERVED`/`NO effect`/`[log]`/`[metrics]` substrings). Daemon-spawn pattern: `env!("CARGO_BIN_EXE_uc2-node")`, `daemon_config` helper `:153`, SIGTERM via `libc::kill`, assert on piped stderr. |
| In-process cluster pattern | `uc2_node/tests/failover.rs` — `spawn_cluster_ring(n, buffer_bytes)`, `NodeH`, `await_single_leader`, `await_serving_among`, `submit_n`, `partition(a, b)` (`:405`). Test files are separate crates: copy the minimal helpers you need, do not import across test files. |
| Example config pin | `the_packaged_example_config_is_valid` (in `uc2_node`'s tests) asserts `packaging/node.example.toml` loads and passes preflight — keep it green when editing the example. |
| Heartbeat clocks | Both heartbeats are unix nanoseconds: node side `publish_status` (`node.rs:2730`), service side `uc2_service/src/apply.rs:184,:271`. Age = `SystemTime::now()` unix ns minus the field. |

## The series contract

The single source of truth for `/metrics`. Task 5 implements it, Task 6 serves it, Task 8's rules reference only names from it, Task 10's coverage row asserts every row of it. All positions are byte positions (this system has no indices). `type` is the Prometheus TYPE line. Counters are cumulative since node start.

| Series | Type | Source |
|---|---|---|
| `uc2_build_info{version}` | gauge (const 1) | `env!("CARGO_PKG_VERSION")` |
| `uc2_node_id` | gauge | `ObsSources::node_id` |
| `uc2_is_leader` | gauge 0/1 | `status().flags & NODE_FLAG_LEADER` |
| `uc2_can_serve` | gauge 0/1 | `status().flags & NODE_FLAG_CAN_SERVE` |
| `uc2_term` | gauge | `status().term` |
| `uc2_leader_hint` | gauge | `status().leader_hint`; **omit the series when it reads `u64::MAX`** (unknown) |
| `uc2_config_version` | gauge | `config_version()` |
| `uc2_config_pending` | gauge 0/1 | `config_pending()` |
| `uc2_crypto_enabled` / `uc2_purge_enabled` | gauge 0/1 | `ObsSources` bools (from `NodeConfig`) |
| `uc2_admission_bytes` / `uc2_journal_segment_bytes` | gauge | cnc `admission_bytes()` / `ObsSources` (from config) |
| `uc2_agent_alive{agent="consensus"\|"sender"\|"receiver"\|"archive"}` | gauge 0/1 | finished-flags, inverted |
| `uc2_append_bytes`, `uc2_durable_bytes`, `uc2_sent_bytes`, `uc2_commit_bytes` | gauge | `counters()` |
| `uc2_service_applied_bytes`, `uc2_service_epoch`, `uc2_output_completed_bytes` | gauge | `service()` |
| `uc2_output_progress_bytes` | gauge | `status().output_progress` |
| `uc2_service_snapshot_pos_bytes`, `uc2_node_snapshot_floor_bytes`, `uc2_incoming_snapshot_pos_bytes` | gauge | `snapshots()` |
| `uc2_archive_first_base_bytes` | gauge | `archive_first_base()` |
| `uc2_commit_lag_bytes` | gauge | `append.saturating_sub(commit)` |
| `uc2_apply_lag_bytes` | gauge | `commit.saturating_sub(service_applied)` |
| `uc2_admission_saturation` | gauge | `commit_lag as f64 / admission_bytes as f64` (0 when admission is 0) |
| `uc2_node_heartbeat_age_seconds`, `uc2_service_heartbeat_age_seconds` | gauge | `(now_unix_ns - hb) / 1e9`; a never-written heartbeat (0) yields a huge age, which is the correct alert-side signal |
| `uc2_peer_reported_durable_bytes{peer,role}` | gauge | occupied `peer_slot(i)`; `role` ∈ `voter`\|`learner` |
| `uc2_peer_replication_lag_bytes{peer,role}` | gauge | `commit.saturating_sub(reported_durable)` |
| `uc2_peer_advertised_limit_bytes{peer,role}` | gauge | slot `advertised_limit` |
| `uc2_truncations_total`, `uc2_wipes_total` | counter | Node Arc counters |
| `uc2_reports_unattested_total`, `uc2_reports_implausible_total` | counter | Node Arc counters (wire-0.5.0 attestation) |
| `uc2_crypto_handshake_failures_total` | counter | Node Arc counter |
| `uc2_sender_seal_failures_total`, `uc2_receiver_seal_failures_total` | counter | `SenderStats.seal_failures`, `FollowerStats.seal_failures` |
| `uc2_unknown_source_datagrams_total` | counter | `FollowerStats.dropped_unknown_peer` (the spec's `append_pos_unknown_source`) |
| `uc2_cleartext_peer_datagrams_total` | counter | `FollowerStats.peer_appears_cleartext` |
| `uc2_naks_sent_total` / `uc2_naks_served_total` / `uc2_naks_dropped_total` / `uc2_naks_rejected_total` | counter | receiver / sender / sender / sender |
| `uc2_replay_datagrams_total`, `uc2_flow_stalls_total`, `uc2_overruns_total`, `uc2_heartbeats_sent_total` | counter | `SenderStats` |
| `uc2_sender_datagrams_total`, `uc2_sender_bytes_total`, `uc2_receiver_datagrams_total`, `uc2_receiver_bytes_total` | counter | `SenderStats` / `FollowerStats` |
| `uc2_snapshot_sessions_total`, `uc2_snapshot_chunks_total`, `uc2_snapshot_chunk_naks_total` | counter | `SenderStats.snap_*` |
| `uc2_receiver_dropped_total{reason}` | counter | `FollowerStats.dropped_*`; `reason` ∈ `stale_term, dup, overrun, malformed, gated, straddle, auth_failed, replay, unknown_epoch, unknown_peer, handshake` |
| `uc2_truncation_resyncs_total`, `uc2_term_change_discards_total`, `uc2_counter_ahead_resyncs_total` | counter | `FollowerStats` |
| `uc2_net_event_drops_total` | counter | sum of `FollowerStats.net_drops[]` |

Spec coverage check against §5's list: commit lag ✓, apply lag ✓, per-peer replication lag ✓, admission-window saturation ✓, heartbeat staleness both processes ✓, rates for `reports_unattested` ✓ / `append_pos_unknown_source` (→ `uc2_unknown_source_datagrams_total`) ✓ / `naks_plus_replay` (→ the four NAK counters + `uc2_replay_datagrams_total`; the cnc per-peer `naks_plus_replay` slot has been dormant since M6 and is not exported) ✓ / `seal_failures` ✓.

---

### Task 0: Branch

- [ ] **Step 1:** `git checkout -b uc2/m10-observable-cluster` (from current `main`). No worktree — this session works in place.

### Task 1: Structured log core (`obs::log`)

**Files:**
- Create: `uc2_node/src/obs/mod.rs`, `uc2_node/src/obs/log.rs`
- Modify: `uc2_node/src/lib.rs` (add `pub mod obs;`)

**Interfaces:**
- Consumes: nothing (std only).
- Produces (later tasks rely on these exact names):
  - `uc2_node::obs::log::LogLevel` — `enum LogLevel { Error, Warn, Info }`, `impl FromStr` accepting `"error" | "warn" | "info"` (case-sensitive, lowercase), `Default = Info`.
  - `uc2_node::obs::log::{set_level, level}` — global filter (`AtomicU8`).
  - `uc2_node::obs::log::{Field, FieldValue, emit}` — `pub fn emit(level: LogLevel, event: &'static str, fields: &[Field<'_>])`.
  - `uc2_node::obs_event!` macro (exported at crate root via `#[macro_export]`): `obs_event!(Info, "became_leader", node = id as u64, term = term as u64)` — values are `u64`, `i64`, `bool`, or `&str`, dispatched by a `From` impl on `FieldValue`.
  - `uc2_node::obs::log::capture_for_tests() -> Arc<Mutex<Vec<u8>>>` — swaps the sink to an in-memory buffer and returns it; `stderr_for_tests()` swaps back.

**Record shape** (one line, `\n`-terminated, valid JSON):

```json
{"ts_ns":1755600000000000000,"level":"info","event":"became_leader","node":0,"term":3,"base":4096}
```

`ts_ns` is unix nanoseconds from `SystemTime::now()`. Key order: `ts_ns`, `level`, `event`, then fields in call order. String values JSON-escaped (`"` `\\` and control chars < 0x20 as `\u00XX`).

- [ ] **Step 1: Write the failing tests** (`#[cfg(test)]` in `log.rs`):

```rust
#[test]
fn a_record_is_one_valid_json_line_with_fields_in_order() {
    let _g = TEST_LOCK.lock().unwrap();
    let buf = capture_for_tests();
    emit(LogLevel::Info, "became_leader", &[
        Field { key: "node", value: FieldValue::U64(0) },
        Field { key: "term", value: FieldValue::U64(3) },
    ]);
    let s = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(s.ends_with('\n'));
    assert!(s.contains(r#""level":"info","event":"became_leader","node":0,"term":3}"#), "{s}");
    assert!(s.starts_with(r#"{"ts_ns":"#));
    stderr_for_tests();
}

#[test]
fn the_level_filter_suppresses_below_threshold() {
    let _g = TEST_LOCK.lock().unwrap();
    let buf = capture_for_tests();
    set_level(LogLevel::Error);
    emit(LogLevel::Info, "noise", &[]);
    assert!(buf.lock().unwrap().is_empty());
    emit(LogLevel::Error, "kept", &[]);
    assert!(!buf.lock().unwrap().is_empty());
    set_level(LogLevel::Info);
    stderr_for_tests();
}

#[test]
fn string_values_are_escaped() {
    let _g = TEST_LOCK.lock().unwrap();
    let buf = capture_for_tests();
    emit(LogLevel::Warn, "e", &[Field { key: "msg", value: FieldValue::Str("a\"b\\c\nd") }]);
    let s = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(s.contains(r#""msg":"a\"b\\c
d""#), "{s}");
    stderr_for_tests();
}

#[test]
fn the_macro_expands_to_emit() {
    let _g = TEST_LOCK.lock().unwrap();
    let buf = capture_for_tests();
    crate::obs_event!(Info, "config_adopted", node = 1u64, version = 7u64, pending = false);
    let s = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(s.contains(r#""event":"config_adopted","node":1,"version":7,"pending":false"#), "{s}");
    stderr_for_tests();
}
```

The tests share the process-global sink; guard each with a file-local `static TEST_LOCK: Mutex<()>`.

- [ ] **Step 2: Run to verify failure** — `cargo test -p uc2_node --lib obs::log` — expected: compile error (module absent).

- [ ] **Step 3: Implement.** Core shape:

```rust
use std::io::Write;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub enum LogLevel { Error = 0, Warn = 1, #[default] Info = 2 }

static LEVEL: AtomicU8 = AtomicU8::new(LogLevel::Info as u8);
pub fn set_level(l: LogLevel) { LEVEL.store(l as u8, Ordering::Relaxed); }
pub fn level() -> LogLevel { /* match on the u8 */ }

pub enum FieldValue<'a> { U64(u64), I64(i64), Bool(bool), Str(&'a str) }
pub struct Field<'a> { pub key: &'static str, pub value: FieldValue<'a> }

enum Sink { Stderr, Capture(Arc<Mutex<Vec<u8>>>) }
static SINK: OnceLock<Mutex<Sink>> = OnceLock::new();
fn sink() -> &'static Mutex<Sink> { SINK.get_or_init(|| Mutex::new(Sink::Stderr)) }

pub fn emit(level: LogLevel, event: &'static str, fields: &[Field<'_>]) {
    if level > self::level() { return; }
    let mut line = String::with_capacity(128);
    // {"ts_ns":..,"level":"..","event":"..",  then fields, then }\n
    // push_json_str() escapes " \\ and <0x20 as \u00XX
    ...
    match &mut *sink().lock().unwrap() {
        Sink::Stderr => { let _ = std::io::stderr().lock().write_all(line.as_bytes()); }
        Sink::Capture(buf) => buf.lock().unwrap().extend_from_slice(line.as_bytes()),
    }
}
```

`FromStr` for `LogLevel` returns `Err(String)` naming the offending value (`"log.level must be one of error|warn|info, got \"{s}\""`) — Task 2's config error uses it verbatim. The macro:

```rust
#[macro_export]
macro_rules! obs_event {
    ($lvl:ident, $event:expr $(, $key:ident = $val:expr)* $(,)?) => {
        $crate::obs::log::emit(
            $crate::obs::log::LogLevel::$lvl,
            $event,
            &[$($crate::obs::log::Field {
                key: stringify!($key),
                value: $crate::obs::log::FieldValue::from($val),
            }),*],
        )
    };
}
```

with `From<u64> / From<i64> / From<bool> / From<&'a str>` impls on `FieldValue<'a>`.

- [ ] **Step 4: Run tests** — `cargo test -p uc2_node --lib obs::log` — expected: 4 PASS.
- [ ] **Step 5: Clippy** — `cargo clippy -p uc2_node --all-targets -- -D warnings`.
- [ ] **Step 6: Commit** — `git add uc2_node/src/obs uc2_node/src/lib.rs && git commit -m "feat(obs): structured JSON-lines log core — level filter, sink, obs_event! macro"`.

### Task 2: `[log]` / `[metrics]` config schema

**Files:**
- Modify: `uc2_node/src/config_file.rs`, `uc2_node/src/preflight.rs`, `uc2_node/src/bin/uc2-node.rs` (drop the RESERVED notice only — the rest of the daemon changes come in Task 7), `uc2_node/tests/lifecycle.rs`, `packaging/node.example.toml`

**Interfaces:**
- Consumes: `obs::log::LogLevel` (Task 1).
- Produces:
  - `preflight::ObsOptions { pub log_level: LogLevel, pub metrics_bind: Option<SocketAddr> }` (`Default`: `Info`, `None`).
  - `StartupOptions` becomes `{ pub allow_volatile_fs: bool, pub obs: ObsOptions }` — **`ReservedSections` is deleted** (M10 is the release it was reserved for).
  - Config file schema: `[log]` with optional `level` (string, `error|warn|info`, default `info`); `[metrics]` with optional `bind` (socket address, default `127.0.0.1:9600` when the section is present). **Section absent = feature off** (no endpoint; default level) — same absent-means-disabled convention as `[purge]`/`[crypto]`.

- [ ] **Step 1: Write the failing tests** (extend the `#[cfg(test)]` module in `config_file.rs`, following its existing fixture style):

```rust
#[test]
fn log_and_metrics_sections_parse_into_obs_options() {
    let (_cfg, opts) = load_str(&format!("{MINIMAL}\n[log]\nlevel = \"warn\"\n[metrics]\nbind = \"127.0.0.1:9601\"\n")).unwrap();
    assert_eq!(opts.obs.log_level, LogLevel::Warn);
    assert_eq!(opts.obs.metrics_bind, Some("127.0.0.1:9601".parse().unwrap()));
}

#[test]
fn a_bare_metrics_section_gets_the_default_bind() {
    let (_cfg, opts) = load_str(&format!("{MINIMAL}\n[metrics]\n")).unwrap();
    assert_eq!(opts.obs.metrics_bind, Some("127.0.0.1:9600".parse().unwrap()));
}

#[test]
fn absent_sections_mean_off_and_info() {
    let (_cfg, opts) = load_str(MINIMAL).unwrap();
    assert_eq!(opts.obs.log_level, LogLevel::Info);
    assert_eq!(opts.obs.metrics_bind, None);
}

#[test]
fn a_bad_log_level_is_a_refusal_naming_the_field() {
    let e = load_str(&format!("{MINIMAL}\n[log]\nlevel = \"verbose\"\n")).unwrap_err();
    let msg = e.to_string();
    assert!(msg.contains("log.level") && msg.contains("verbose"), "{msg}");
}

#[test]
fn a_typo_inside_log_or_metrics_is_now_refused() {
    // M9 accepted arbitrary keys here (schema undefined); M10 defines it, so deny_unknown_fields applies.
    let e = load_str(&format!("{MINIMAL}\n[metrics]\nport = 9600\n")).unwrap_err();
    assert!(e.to_string().contains("port"), "{e}");
}
```

(`load_str`/`MINIMAL` — reuse the module's existing helpers; if the helper is named differently, follow the file's own fixture names.)

- [ ] **Step 2: Run to verify failure** — `cargo test -p uc2_node --lib config_file` — expected: compile errors (`opts.obs` absent).

- [ ] **Step 3: Implement.** In `config_file.rs`:

```rust
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct LogSectionFile { #[serde(default)] level: Option<String> }

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct MetricsSectionFile { #[serde(default)] bind: Option<std::net::SocketAddr> }
```

Replace `log: Option<toml::Table>` / `metrics: Option<toml::Table>` on `NodeConfigFile` with the typed options. In the build step, map to `ObsOptions`:

```rust
let log_level = match f.log.and_then(|l| l.level) {
    None => LogLevel::default(),
    Some(s) => s.parse::<LogLevel>().map_err(|e| ConfigError::Invalid { field: "log.level", detail: e })?,
};
let metrics_bind = f.metrics.map(|m| m.bind.unwrap_or_else(|| "127.0.0.1:9600".parse().unwrap()));
```

(Use the file's existing error-variant shape; the requirement is that the message names `log.level` and echoes the bad value.) In `preflight.rs`: add `ObsOptions`, delete `ReservedSections` and its `impl`, update `StartupOptions`. In `uc2-node.rs`: delete the `opts.reserved.any()` NOTE block (lines 45-51) — the sections now act, and Task 7 wires them. In `lifecycle.rs`: replace `daemon_starts_and_announces_the_m10_reserved_sections` with a test that a daemon started with `[metrics]` no longer prints `RESERVED` (full endpoint behaviour is Task 7's test). In `packaging/node.example.toml`: replace the RESERVED block with real annotated sections:

```toml
# Structured logging. ABSENT section means level "info".
#
# [log]
# level = "info"          # error | warn | info

# The observability endpoint: /metrics (Prometheus), /healthz, /readyz.
# ABSENT means no endpoint. Bind to a loopback or private address — the
# endpoint is read-only but unauthenticated.
#
# [metrics]
# bind = "127.0.0.1:9600"
```

- [ ] **Step 4: Run** — `cargo test -p uc2_node` (the whole crate: config tests, lifecycle, the example-config pin) — expected: PASS.
- [ ] **Step 5: Clippy** — workspace clean.
- [ ] **Step 6: Commit** — `feat(config): [log]/[metrics] gain their M10 schema; ReservedSections retired`.

### Task 3: Transition records at the consensus sites

**Files:**
- Modify: `uc2_node/src/node.rs`
- Create: `uc2_node/tests/obs_log.rs`

**Interfaces:**
- Consumes: `obs_event!` (Task 1).
- Produces: the record vocabulary (event names are load-bearing — Task 8's how-to documents them, and the gate greps them):
  - `became_leader` (Info): `node`, `term`, `base`
  - `became_follower` (Info): `node`, `term`, `leader` (u64; omit the field when unknown)
  - `serving_changed` (Info): `node`, `term`, `can_serve` (bool) — edge-detected in `publish_status`
  - `log_truncated` (Warn): `node`, `epoch`, `to`
  - `log_wiped` (Warn): `node`
  - `snapshot_installed` (Info): `node`, `pos`
  - `config_adopted` (Info): `node`, `position`, `version`, `prev_position`
  - `halt_removed` (Error) / `stepdown_removed` (Warn): `node`, `term`

**Emission sites** (anchor map has the line ranges): the seven `exec`-side sites plus one `publish_status` edge. `publish_status` gains a `last_flags: u64` field on the agent struct (initialised to the boot value) and emits `serving_changed` only when `flags & NODE_FLAG_CAN_SERVE` differs from the previous cycle — one branch and no allocation on the untaken path. The existing `eprintln!` at `HaltRemoved`/`StepDownRemoved` are replaced by the structured record (keep the human sentence as a `msg` field if it carries content the fields don't).

- [ ] **Step 1: Write the failing integration test** (`uc2_node/tests/obs_log.rs`). Copy the minimal ring helpers from `failover.rs` (`spawn_cluster_ring`-equivalent, `await_single_leader`, `partition`) — test files are separate crates, so they cannot be imported. Two tests, sharing a file-local `TEST_LOCK` (the capture sink is process-global):

```rust
#[test]
fn an_election_emits_became_leader_and_followers_note_it() {
    let _g = TEST_LOCK.lock().unwrap();
    let buf = uc2_node::obs::log::capture_for_tests();
    let mut cluster = spawn_ring(3);
    let leader = await_single_leader(&cluster, 10);
    await_serving(&cluster, leader, 10);
    let text = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
    assert!(text.lines().any(|l| l.contains(r#""event":"became_leader""#)), "{text}");
    assert!(text.lines().any(|l| l.contains(r#""event":"serving_changed""#) && l.contains(r#""can_serve":true"#)), "{text}");
    uc2_node::obs::log::stderr_for_tests();
}

#[test]
fn a_healed_deposed_leader_emits_log_truncated() {
    // The M4-era choreography: partition the leader with a minority, commit on the
    // majority side under a new term, heal — the old leader must truncate its
    // uncommitted tail, and M10 must say so. Follow failover.rs's partition test
    // shape for the exact wait conditions.
    ...
    assert!(text.lines().any(|l| l.contains(r#""event":"log_truncated""#)), "{text}");
}
```

Every line in the capture must parse as JSON — add a small `fn assert_json_lines(text: &str)` (hand-rolled brace/quote sanity or exact-prefix checks; no serde_json) applied to the whole buffer in both tests.

- [ ] **Step 2: Run to verify failure** — `cargo test -p uc2_node --test obs_log` — expected: FAIL (no records).
- [ ] **Step 3: Implement the emissions** at the eight sites, e.g. in the `BecomeLeader` arm:

```rust
crate::obs_event!(Info, "became_leader", node = self.id as u64, term = term as u64, base = base);
```

- [ ] **Step 4: Run** — `cargo test -p uc2_node --test obs_log` PASS, then the neighbouring suites that exercise these paths: `cargo test -p uc2_node --test failover --test lifecycle`. The known-flaky reconfig suite is not in this task's gate; do not chase pre-existing intermittents (see `docs/superpowers/plans/2026-08-16-nightly-flake-hunt-brief.md` for which those are).
- [ ] **Step 5: Clippy; commit** — `feat(obs): transition records at the consensus sites — elections, truncation, snapshot, config, removal`.

### Task 4: Agent-health flags and the `ObsSources` bundle

**Files:**
- Modify: `uc2_log/src/agent.rs`, `uc2_node/src/node.rs`

**Interfaces:**
- Consumes: existing `Node` Arc fields (anchor map).
- Produces:
  - `AgentRunner::finished_flag(&self) -> Arc<AtomicBool>` — false while the worker loop runs; set true when the closure returns **or panics** (drop-guard in the worker thread). `is_finished()` keeps its meaning.
  - `uc2_node::obs::ObsSources` (defined in `obs/mod.rs`, constructed by `node.rs`):

```rust
pub struct ObsSources {
    pub node_id: u32,
    pub cnc: Arc<CncPage>,
    pub sender: Arc<uc2_net::sender::SenderStats>,
    pub receiver: Arc<uc2_net::receiver::FollowerStats>,
    pub truncations: Arc<AtomicU64>,
    pub wipes: Arc<AtomicU64>,
    pub reports_unattested: Arc<AtomicU64>,
    pub reports_implausible: Arc<AtomicU64>,
    pub crypto_handshake_failures: Arc<AtomicU64>,
    pub crypto_enabled: bool,
    pub purge_enabled: bool,
    pub admission_bytes: u64,
    pub journal_segment_bytes: u64,
    pub agents: Vec<(&'static str, Arc<AtomicBool>)>, // (name, finished_flag)
}
```

  - `Node::observability(&self) -> ObsSources` — clones the Arcs; agent names as spawned (`consensus`, `sender`, `receiver`, `archive` — strip any `uc2-` prefix to match the metric label values in the series contract).

- [ ] **Step 1: Failing test in `uc2_log/src/agent.rs`'s test module:**

```rust
#[test]
fn the_finished_flag_survives_a_panicking_agent() {
    let r = AgentRunner::spawn("panics", IdleStrategy::Sleep(Duration::from_millis(1)), || {
        panic!("deliberate");
    }).unwrap();
    let flag = r.finished_flag();
    let deadline = Instant::now() + Duration::from_secs(2);
    while !flag.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "flag never set");
        thread::sleep(Duration::from_millis(5));
    }
    drop(r); // Drop swallows the panic — that behaviour is unchanged
}
```

(Adjust the `spawn` signature/idle variant to the file's actual API — the anchor map has it at `agent.rs:47`.)

- [ ] **Step 2: Run to verify failure** — `cargo test -p uc2_log finished_flag` — compile error.
- [ ] **Step 3: Implement:** add the field, set via a drop-guard inside the spawned thread (fires on both return and unwind); `finished_flag()` clones the Arc. Then `ObsSources` + `Node::observability()` — a straight clone-and-collect over existing fields; the only judgement call is verifying each crypto counter field is `Arc<AtomicU64>` at its definition before cloning (the getters at `node.rs:1286-1394` point at them).
- [ ] **Step 4: A second failing-then-passing test** in `uc2_node/tests/obs_http.rs`-to-be is deferred to Task 6; here add a smoke assertion to `uc2_node/tests/lifecycle.rs`'s in-process section: start a `single_node`, call `node.observability()`, assert `agents.len() == 4` and every flag reads false, stop, assert all true.
- [ ] **Step 5: Run** — `cargo test -p uc2_log && cargo test -p uc2_node --test lifecycle`; clippy.
- [ ] **Step 6: Commit** — `feat(obs): agent finished-flags (panic-proof) + Node::observability() source bundle`.

### Task 5: The Prometheus encoder (`obs::metrics`)

**Files:**
- Create: `uc2_node/src/obs/metrics.rs`

**Interfaces:**
- Consumes: `ObsSources` (Task 4).
- Produces: `pub fn render_prometheus(s: &ObsSources) -> String` — the series contract, exactly. Each series preceded by `# HELP` and `# TYPE` lines. Also `pub fn now_unix_ns() -> u64` (shared with Task 6's probes).

- [ ] **Step 1: Failing unit tests** (in-module, using `CncPage::heap` + a hand-built `ObsSources` with fresh `Arc`s — no cluster needed):

```rust
fn synthetic_sources() -> ObsSources { /* CncPage::heap(&meta), zeroed Arcs, 4 live agent flags */ }

#[test]
fn every_contract_series_is_present() {
    let s = synthetic_sources();
    let text = render_prometheus(&s);
    for name in CONTRACT_SERIES { // the const list below — Task 10's gate reuses it
        assert!(text.contains(&format!("\n{name}")) || text.starts_with(name),
                "missing series {name}");
    }
}

#[test]
fn derived_lags_saturate_and_saturation_divides() {
    let s = synthetic_sources();
    s.cnc.counters().append.store_release(1_000_000);
    s.cnc.counters().commit.store_release(400_000);
    s.cnc.service().service_applied.store_release(150_000);
    s.cnc.store_admission_bytes(262_144);
    let text = render_prometheus(&s);
    assert!(text.contains("uc2_commit_lag_bytes 600000"), "{text}");
    assert!(text.contains("uc2_apply_lag_bytes 250000"), "{text}");
    assert!(text.contains("uc2_admission_saturation 2.288818359375"), "{text}");
}

#[test]
fn peer_slots_export_only_occupied_with_labels() {
    let s = synthetic_sources();
    s.cnc.peer_slot(0).id_and_role.store_release(pack_id_and_role(2, CNC_PEER_ROLE_VOTER));
    s.cnc.peer_slot(0).reported_durable.store_release(1234);
    let text = render_prometheus(&s);
    assert!(text.contains(r#"uc2_peer_reported_durable_bytes{peer="2",role="voter"} 1234"#), "{text}");
    assert!(!text.contains(r#"peer="0""#), "unoccupied slots must not appear: {text}");
}

#[test]
fn leader_hint_is_omitted_when_unknown() {
    let s = synthetic_sources();
    s.cnc.status().leader_hint.store_release(u64::MAX);
    assert!(!render_prometheus(&s).contains("uc2_leader_hint"));
    s.cnc.status().leader_hint.store_release(1);
    assert!(render_prometheus(&s).contains("uc2_leader_hint 1"));
}

#[test]
fn a_dead_agent_reads_zero() {
    let s = synthetic_sources();
    s.agents[1].1.store(true, Ordering::Release);
    let text = render_prometheus(&s);
    assert!(text.contains(r#"uc2_agent_alive{agent="sender"} 0"#), "{text}");
    assert!(text.contains(r#"uc2_agent_alive{agent="consensus"} 1"#), "{text}");
}
```

`CONTRACT_SERIES: &[&str]` is a `pub const` in `metrics.rs` listing every metric family name from the series contract table — it is the machine-readable form of the contract, and Task 10's coverage row iterates it against a live scrape.

- [ ] **Step 2: Run to verify failure**; **Step 3: implement** (a `push_gauge(&mut String, name, help, value)` / `push_counter` / `push_labeled` helper trio keeps it flat; u64 values formatted as integers, f64 via `{}`); **Step 4: tests PASS**; **Step 5: clippy**.
- [ ] **Step 6: Commit** — `feat(obs): Prometheus text encoder over ObsSources — the M10 series contract`.

### Task 6: The endpoint and the probes (`obs::http`)

**Files:**
- Create: `uc2_node/src/obs/http.rs`, `uc2_node/tests/obs_http.rs`

**Interfaces:**
- Consumes: `ObsSources`, `render_prometheus`, `now_unix_ns`.
- Produces:

```rust
pub struct ObsServer { /* handle, stop flag, local_addr */ }
impl ObsServer {
    pub fn serve(sources: ObsSources, bind: SocketAddr) -> io::Result<ObsServer>, // binds NOW (refusal at startup, not first scrape)
    pub fn local_addr(&self) -> SocketAddr,
    pub fn stop(self),
}
```

**Routes and semantics** (the probe rules the spec gates on):

| Route | 200 when | 503 body names the cause |
|---|---|---|
| `/metrics` | always (render + return) | — |
| `/healthz` (liveness) | all four agent flags alive AND node heartbeat age < 3 s | `agent <name> fail-stopped` / `node heartbeat stale` |
| `/readyz` (readiness) | role-aware: leader → `can_serve` AND service heartbeat age < 3 s; follower/learner → healthy AND service heartbeat age < 3 s | `elected, NewTerm not yet quorum-committed` for `flags == 0x01`; `service heartbeat stale`; `agent fail-stopped` |

Body on 200 is `ok role=<leader|follower> can_serve=<bool>\n`. **`flags == NODE_FLAG_LEADER` alone (the `0x01` state) is NOT ready** — that is the exact naive-probe mistake the spec names. Server mechanics: one thread, `TcpListener` with a 100 ms accept timeout loop (so `stop()` is prompt), per-connection: read until `\r\n\r\n` (4 KiB cap, 1 s read timeout), parse only `GET <path>`, respond `HTTP/1.1 <code>`, `Content-Type: text/plain; version=0.0.4` for `/metrics` (`text/plain` otherwise), `Content-Length`, `Connection: close`. Anything not `GET /metrics|/healthz|/readyz` → 404. No keep-alive, no TLS, no auth — the how-to (Task 8) says to bind loopback/private and why.

- [ ] **Step 1: Failing integration tests** (`obs_http.rs`; plain `TcpStream` client helper `fn get(addr, path) -> (u16, String)`):

```rust
#[test]
fn metrics_healthz_readyz_serve_and_404_otherwise() {
    let (srv, sources) = synthetic_server(); // CncPage::heap-backed, port 0
    sources.cnc.status().node_heartbeat_ns.store_release(now_unix_ns());
    sources.cnc.status().service_heartbeat_ns.store_release(now_unix_ns());
    assert_eq!(get(srv.local_addr(), "/metrics").0, 200);
    assert!(get(srv.local_addr(), "/metrics").1.contains("uc2_commit_bytes"));
    assert_eq!(get(srv.local_addr(), "/healthz").0, 200);
    assert_eq!(get(srv.local_addr(), "/readyz").0, 200);
    assert_eq!(get(srv.local_addr(), "/nope").0, 404);
    srv.stop();
}

#[test]
fn an_elected_but_not_serving_leader_is_not_ready() {
    let (srv, sources) = synthetic_server();
    sources.cnc.status().node_heartbeat_ns.store_release(now_unix_ns());
    sources.cnc.status().service_heartbeat_ns.store_release(now_unix_ns());
    sources.cnc.status().flags.store_release(NODE_FLAG_LEADER); // 0x01, no CAN_SERVE
    let (code, body) = get(srv.local_addr(), "/readyz");
    assert_eq!(code, 503);
    assert!(body.contains("NewTerm"), "{body}");
    assert_eq!(get(srv.local_addr(), "/healthz").0, 200, "liveness must NOT flap on 0x01");
    srv.stop();
}

#[test]
fn a_dead_agent_fails_liveness_by_name() { /* flip agents[3].1; healthz -> 503 containing "archive" */ }

#[test]
fn a_stale_service_heartbeat_fails_readiness_but_not_liveness() { /* service hb old, node hb fresh */ }

#[test]
fn a_real_single_node_cluster_serves_and_becomes_ready() {
    // single_node() from lifecycle.rs pattern + node.observability() + serve();
    // poll /readyz until 200; then /metrics must show uc2_is_leader 1 and uc2_can_serve 1.
}
```

- [ ] **Step 2: verify failure; Step 3: implement; Step 4: PASS; Step 5: clippy.**
- [ ] **Step 6: Commit** — `feat(obs): /metrics, /healthz, /readyz — role-aware probes that refuse the 0x01 leader`.

### Task 7: Daemon wiring and counter-derived records

**Files:**
- Modify: `uc2_node/src/bin/uc2-node.rs`, `uc2_node/tests/lifecycle.rs`

**Behaviour added to the daemon:**
1. After preflight: `obs::log::set_level(opts.obs.log_level)`.
2. After `Node::start`: if `opts.obs.metrics_bind` is `Some(addr)`, `ObsServer::serve(node.observability(), addr)` — a bind failure is a **startup error, exit 1** (runtime failure, retried by systemd — not exit 2, the config was valid). Print one banner line: `uc2-node: observability endpoint on http://<addr>/metrics`.
3. In the 100 ms poll loop, every 10th tick (~1 s), a derived-events pass over cheap counter reads, each edge-triggered and rate-limited to one record per 10 s per event:
   - `nak_storm` (Warn): `naks_dropped` delta > 0 since last pass — fields `node`, `naks_dropped`, `naks_served`.
   - `seal_failures` (Warn): sender+receiver `seal_failures` delta > 0 — fields `node`, `count`, `is_leader` (the diagnose doc: a leader's climb is benign, a follower's is not — the field lets the reader apply that).
   - `snapshot_published` (Info): cnc `service_snapshot_pos` advanced — fields `node`, `pos`. (The service builds snapshots in its own process; the daemon observes the page — no `uc2_service` change.)
   - `agent_failstopped` (Error): any finished-flag true → emit with `agent` name, print one human line, **skip the drain and `return ExitCode::FAILURE`** so systemd's `Restart=on-failure` takes over. (Today a mid-run agent panic leaves a zombie daemon that looks healthy; `stop_draining` would re-raise the panic at exit. Fail fast instead — the restarted node replays its journal.)
4. On SIGTERM: `srv.stop()` before `stop_draining` (scrapes must not race teardown), then the existing drain path unchanged.

- [ ] **Step 1: Failing integration tests** in `lifecycle.rs`, following the existing daemon-spawn pattern:

```rust
#[test]
fn the_daemon_serves_metrics_when_configured_and_stops_cleanly() {
    // daemon_config with extra = "[metrics]\nbind = \"127.0.0.1:0\"\n" won't work — port 0
    // isn't knowable from outside. Use a picked free port: bind a TcpListener to :0,
    // note the port, drop it, and write that port into the config (the lifecycle-test
    // race window is acceptable in-suite).
    // assert: banner line on stdout names the addr; GET /metrics returns uc2_commit_bytes;
    // GET /healthz 200; SIGTERM -> exit 0 within the drain budget; the port closes.
}

#[test]
fn the_daemon_without_a_metrics_section_opens_no_port() { /* no banner, connect refused */ }
```

- [ ] **Step 2: verify failure; Step 3: implement; Step 4: PASS** (`cargo test -p uc2_node --test lifecycle`); **Step 5: clippy.**
- [ ] **Step 6: Commit** — `feat(daemon): wire obs — endpoint lifecycle, log level, derived NAK/seal/snapshot records, fail-fast on agent death`.

### Task 8: Alert rules, dashboard, documentation

**Files:**
- Create: `packaging/prometheus/uc2-alerts.yml`, `packaging/grafana/uc2-dashboard.json`, `docs/how-to/monitor-a-cluster.md`
- Modify: `docs/reference/configuration.md` (the `[log]`/`[metrics]` schema replaces the reserved-sections paragraph; new refusal rows), `docs/how-to/README.md`, `docs/ops/uc2-runbook.md` (index lines), `docs/how-to/diagnose-a-node.md` (cross-link: "these interpretations ship as alert rules"), `docs/how-to/run-a-cluster.md` ("Confirm the cluster is actually serving" gains the probe/metrics option)

**The rules file** — every rule's `expr` uses only series-contract names; `for:` values follow the diagnose doc's semantics. Ship exactly these (group `uc2`, `interval: 15s`):

```yaml
groups:
- name: uc2
  interval: 15s
  rules:
  - alert: Uc2AgentDead
    expr: uc2_agent_alive == 0
    for: 0m
    labels: { severity: critical }
    annotations: { summary: "agent {{ $labels.agent }} fail-stopped on {{ $labels.instance }}" }
  - alert: Uc2NoLeader
    expr: max(uc2_is_leader) == 0
    for: 30s
    labels: { severity: critical }
    annotations: { summary: "no node reports leadership — election cannot complete" }
  - alert: Uc2LeaderNotServing
    expr: uc2_is_leader == 1 and uc2_can_serve == 0
    for: 30s
    labels: { severity: critical }
    annotations: { summary: "{{ $labels.instance }} elected but its NewTerm frame is not quorum-committed (flags 0x01)" }
  - alert: Uc2ServiceWedged
    expr: uc2_service_heartbeat_age_seconds > 5 and uc2_node_heartbeat_age_seconds < 3
    for: 1m
    labels: { severity: critical }
    annotations: { summary: "apply loop wedged on {{ $labels.instance }}: node alive, service heartbeat stale" }
  - alert: Uc2ReplicationStalled
    expr: delta(uc2_commit_bytes[1m]) == 0 and delta(uc2_append_bytes[1m]) > 0
    for: 1m
    labels: { severity: critical }
    annotations: { summary: "{{ $labels.instance }} appends but cannot commit — no quorum acknowledging" }
  - alert: Uc2PeerNeverHeard
    expr: uc2_peer_reported_durable_bytes == 0
    for: 2m
    labels: { severity: warning }
    annotations: { summary: "peer {{ $labels.peer }} has never reported — check the bind-address rule first" }
  - alert: Uc2PeerLagging
    expr: uc2_peer_replication_lag_bytes > on(instance) group_left uc2_admission_bytes
    for: 5m
    labels: { severity: warning }
    annotations: { summary: "peer {{ $labels.peer }} lags by more than the admission window" }
  - alert: Uc2AdmissionSaturated
    expr: uc2_admission_saturation > 0.9
    for: 1m
    labels: { severity: warning }
    annotations: { summary: "ingress admission window ≥90% consumed on {{ $labels.instance }} — commit is not keeping up with append" }
  - alert: Uc2PurgeStalled
    expr: uc2_purge_enabled == 1 and (uc2_node_snapshot_floor_bytes - uc2_archive_first_base_bytes) > 2 * uc2_journal_segment_bytes
    for: 10m
    labels: { severity: warning }
    annotations: { summary: "purge enabled but the journal head lags the snapshot floor by >2 segments on {{ $labels.instance }}" }
  - alert: Uc2RepeatedWipes
    expr: delta(uc2_wipes_total[10m]) > 1
    for: 0m
    labels: { severity: warning }
    annotations: { summary: "{{ $labels.instance }} wiped-and-rejoined more than once in 10m" }
  - alert: Uc2UnattestedReports
    expr: delta(uc2_reports_unattested_total[5m]) > 0
    for: 0m
    labels: { severity: critical }
    annotations: { summary: "unattested durable reports on {{ $labels.instance }} — a pre-0.5.0 peer is in the cluster; commits will stall (flag-day violation)" }
  - alert: Uc2CleartextPeer
    expr: delta(uc2_cleartext_peer_datagrams_total[5m]) > 0
    for: 0m
    labels: { severity: critical }
    annotations: { summary: "cleartext datagrams from a peer while crypto is on — a node missed the flag day" }
  - alert: Uc2FollowerSealFailures
    expr: delta(uc2_receiver_seal_failures_total[5m]) > 0 and uc2_is_leader == 0
    for: 0m
    labels: { severity: warning }
    annotations: { summary: "authentication failures opening peer traffic on follower {{ $labels.instance }} (a leader's seal_failures climb is benign — this one is not)" }
```

**The dashboard** — one Grafana JSON (schemaVersion ≥ 39, datasource `${DS_PROMETHEUS}` variable), six panels, each a straight PromQL over contract series: (1) timeseries "Commit / apply lag" (`uc2_commit_lag_bytes`, `uc2_apply_lag_bytes`); (2) timeseries "Throughput" (`delta(uc2_commit_bytes[1m])/60`); (3) timeseries "Per-peer replication lag" (`uc2_peer_replication_lag_bytes`); (4) stat row "Cluster" (`max(uc2_term)`, `max(uc2_is_leader)`, `min(uc2_agent_alive)`, `max(uc2_config_version)`); (5) timeseries "Heartbeat ages" (both age series); (6) timeseries "Repair & drops" (`delta(uc2_naks_sent_total[1m])`, `delta(uc2_replay_datagrams_total[1m])`, `delta(uc2_receiver_dropped_total[1m])`). Write the JSON by hand, `title` "UC cluster", `uid` `uc2-cluster`; keep it minimal but importable (this is a shipped artifact, so `packaging/README`-level notes go in the how-to instead).

**The how-to** (`monitor-a-cluster.md`, follow the house how-to voice): enabling `[metrics]`; a Prometheus `scrape_configs` snippet (one target per node, port 9600); installing the rules file; importing the dashboard; the probe table (`/healthz` restart-worthy vs `/readyz` route-worthy, and why readiness keys on `can_serve` — link the 0x01 explanation in diagnose-a-node); the structured-record vocabulary from Task 3 (a table: event → fields → what it means); the security note (unauthenticated read-only endpoint — bind loopback/private, firewall it; it exposes positions and peer addresses, not payloads).

- [ ] **Step 1:** Write `uc2-alerts.yml` exactly as above; write the dashboard JSON; verify both mechanically: `promtool check rules packaging/prometheus/uc2-alerts.yml` (promtool required on PATH — same class of external dependency as elle's `java`; note it in the how-to) and `python3 -m json.tool packaging/grafana/uc2-dashboard.json > /dev/null`.
- [ ] **Step 2:** Write the how-to and the four doc edits. Keep `configuration.md`'s "reserved sections" paragraph as a one-line historical note ("reserved in M9, defined since M10").
- [ ] **Step 3:** Run the doc-adjacent tests: `cargo test -p uc2_node the_packaged_example_config_is_valid`.
- [ ] **Step 4: Commit** — `feat(packaging),docs: alert rules + dashboard + monitor-a-cluster how-to; [log]/[metrics] documented`.

### Task 9: Fire every alert rule against a deliberately broken cluster

**Files:**
- Create: `uc2_node/examples/m10_alerts.rs`, `scripts/m10_alert_fire.sh`

**Method.** For each rule, a scenario constructs the failure in a real in-process cluster (fault-injection partitions, killed processes/threads, `Node::crash()`), an in-process `ObsServer` per node is scraped over real HTTP once per second into per-scenario sample sets, and the samples become `promtool test rules` input series (`values: "v1 v2 v3 ..."` at `interval: 1s` — promtool accepts explicit space-separated values), with an `alert_rule_test` asserting the rule fires. The rules file under test is the shipped `packaging/prometheus/uc2-alerts.yml`, unmodified — the artifact is what is proven.

Scenario table (pre-commit this in the gate doc, Task 10):

| Rule | Scenario |
|---|---|
| Uc2AgentDead | 1-node cluster; kill the archive agent by poisoning its journal dir permissions post-start, or (simpler and honest) a synthetic `ObsSources` with one flag flipped — **synthetic, disclosed** |
| Uc2NoLeader | 3 nodes; `crash()` the leader and one follower; the survivor cannot form a quorum |
| Uc2LeaderNotServing | **synthetic cnc page, disclosed**: flags `0x01` served through the real exporter. The real state is a sub-RTT race window (vote quorum without append quorum needs a fault the seeded layer cannot cut selectively); constructing it live is a consensus-harness project, not an alert test |
| Uc2ServiceWedged | node + service in-process; stop the service's apply agent, keep the node |
| Uc2ReplicationStalled | 3 nodes under client load; partition the leader from both followers (leader keeps appending into admission, commits stop) |
| Uc2PeerNeverHeard | 3-node config where one member is never started |
| Uc2PeerLagging | 3 nodes under load; partition one follower, keep writing past the admission window on the majority |
| Uc2AdmissionSaturated | same scenario, leader-side series |
| Uc2PurgeStalled | **synthetic, disclosed**: purge-enabled sources with floor − first_base > 2 segments (driving a real purge stall requires breaking the archive mid-run) |
| Uc2RepeatedWipes | wipe counter bumped by the real NoCommonPrefix path if a compact scenario exists in the snapshot suites; otherwise synthetic, disclosed |
| Uc2UnattestedReports / Uc2CleartextPeer / Uc2FollowerSealFailures | synthetic counters bumped through the real encoder — the triggering conditions are a 0.4.0 peer / a mixed-crypto cluster / a corrupting adversary, none of which the in-process world can host honestly |

The split is stated, not hidden: **every rule fires from real scraped series through the real exporter; the broken *state* is genuine where the failure is constructible in-process and synthesized where it is not, and the gate doc records which is which.** (Precedent: the elle gate recorded the same class of structural-can't-expose findings rather than pretending coverage.)

- [ ] **Step 1:** Write `m10_alerts.rs`: `--scenario <name> --out <dir>` runs one scenario and writes `<name>.series` (metric name + label set + sampled values); `--all` runs the lot. Scenarios reuse the obs_http/obs_log test helpers' shape (copy, don't import). Output dir defaults under `$HOME/.cache/uc2-m10-alerts` — **never `/tmp`** (tmpfs).
- [ ] **Step 2:** Write `scripts/m10_alert_fire.sh`: check `promtool` on PATH (exit 2 with an install hint if absent — mirroring `elle_check.sh`'s java probe), run the example, generate `test.yml` from the series files + an `alert_rule_test` block per rule, run `promtool test rules`, print one `PASS rule=<name>` / `FAIL rule=<name>` line per rule, exit 1 on any FAIL.
- [ ] **Step 3:** Run it: `scripts/m10_alert_fire.sh`. Expected: every rule PASS. A rule that cannot be made to fire is a defect in the rule (wrong expr, wrong series name) — fix the rule, not the test.
- [ ] **Step 4: Commit** — `test(m10): every shipped alert rule fired once — real scrapes, promtool-adjudicated, synthetic states disclosed`.

### Task 10: The M10 gate

**Files:**
- Create: `docs/benchmarks/uc2-m10-gate-2026-08-19.md` (FIRST, its own commit, before the harness), `uc2_node/examples/m10_gate.rs`

**The pre-committed bar** (write into the gate doc verbatim, then implement):

| # | Measure | Bar | Adjudicated |
|---|---|---|---|
| 1 | `/metrics` coverage | every family in `CONTRACT_SERIES` present in a live scrape of a serving 3-node cluster, plus ≥1 occupied per-peer series on the leader | local |
| 2 | Probe regime across a leader kill | under load, kill the leader; a 100 Hz poller on every node's `/readyz` observes **zero** samples where a 200 coincides with that node's flags reading `0x01` (poller reads flags via a second read-only cnc attach in the same sample); some node serves 200 within 5 s | local + repeated on the fleet |
| 3 | Scrape perturbation | M5-gate A/B on the same fleet, scrape-on (1 s interval, all nodes) vs scrape-off, back-to-back: throughput ratio ≥ 0.95 (the same-fleet A/B method that resolved the M5 client-extraction question; cross-fleet comparisons are noise) | **fleet only** |
| 4 | Alert rules | `scripts/m10_alert_fire.sh` exits 0 — every shipped rule fired once, scenario table as pre-committed (synthetic states disclosed by name) | local |

Honest-failure protocol verbatim from M9: bar and result in separate commits, bar first; FAIL is recorded and diagnosed before re-run; local runs of row 3 are smoke and never adjudicate (the dev box measured an 18-point spread against a 10-point bar in M9 — same hardware, same reason); `v2.4.0` tags only when the fleet rows pass, and tagging is the maintainer's step.

- [ ] **Step 1:** Write the gate doc: the bar table above, why each row is the right bar (row 1: the contract is the product; row 2: the spec's named naive-probe failure; row 3: "reads cannot perturb the hot path" is a claim about cache traffic under load, only falsifiable on real cross-host hardware; row 4: a rule that has never fired is documentation, not alerting), the scenario table from Task 9, the honest-failure protocol. Commit it alone: `docs(bench): pre-commit the M10 observability gate decide rule`.
- [ ] **Step 2:** Write `m10_gate.rs` with subcommands `coverage`, `probes`, `perturb-smoke`, `all` (clap, the `m9_gate.rs` role pattern): `coverage` boots a 3-node in-process cluster with obs servers, scrapes, iterates `CONTRACT_SERIES`; `probes` runs the row-2 choreography with `Node::crash()` on the leader and the dual-read sampler; `perturb-smoke` runs a short local load with and without a 1 s scraper and prints both numbers **ungated, labelled SMOKE**. Print the bar, print each row's PASS/FAIL, `exit(1)` on any gated FAIL.
- [ ] **Step 3:** Run `cargo run -p uc2_node --release --example m10_gate -- all` locally. Rows 1, 2, 4 must PASS; row 3 prints smoke numbers.
- [ ] **Step 4:** Full local proof stack: `cargo test -p uc2_node && cargo test && cargo clippy --workspace --all-targets -- -D warnings`.
- [ ] **Step 5: Commit** — `feat(bench): m10_gate — coverage + probe-regime rows, perturbation smoke`.
- [ ] **Step 6:** **STOP.** The fleet rows (2-repeat and 3) are a separate, user-approved step: a `bench-infra/scripts/m10_fleet_gate.py` in the m9 orchestrator's mould, an M5 A/B, real Prometheus scraping all nodes. Do not book hardware, do not write the orchestrator into this branch without the user's go — M7/M9 precedent, and the fleet costs money.

---

## Out of scope (named so they are not re-litigated mid-execution)

- **The fleet orchestrator and the fleet run** — user-approved follow-up (Task 10 Step 6).
- **`tracing` adoption.** Declared in the workspace root but consumed nowhere; M10's records are flat scalars and the hand-rolled emitter is ~100 lines with zero deps. Revisit only if a milestone needs spans or an ecosystem subscriber.
- **Auth/TLS on the endpoint** — M10 ships read-only + bind-guidance; the admin-plane authorisation question is spec §2's open finding and not this milestone.
- **A `uc2_service`-side endpoint.** The service's observable state (applied, epoch, heartbeat, snapshot pos) is already on the cnc page the node exports.
- **Exporting the dormant per-peer `naks_plus_replay` cnc slot** — dormant since M6; the sender/receiver aggregates carry the signal.

## Self-review (executed at plan-writing time)

- **Spec §5 coverage:** exporter over the held cnc page (Tasks 4-7, no sidecar) ✓; every derived series named in the spec (series-contract check paragraph) ✓; transition-triggered logging, each listed event mapped to a site or a derived edge (Tasks 3, 7) ✓; readiness on `can_serve` with the 0x01 case tested twice (Task 6 unit, Task 10 row 2) ✓; alert rules + dashboard shipped as files (Task 8) and every rule fired (Task 9) ✓; gate rows mirror the spec's acceptance sentence, M5 re-run as fleet row 3 ✓.
- **Placeholder scan:** the `...` in Task 1 Step 3 and Task 3 Step 1 are elision of code whose shape is fully specified adjacent to them (format string layout; failover.rs choreography named by pattern); no TBDs.
- **Type consistency:** `ObsSources` field names match between Tasks 4/5/6; `CONTRACT_SERIES` defined Task 5, consumed Task 10; `ObsOptions` defined Task 2, consumed Task 7; `finished_flag` defined Task 4, consumed Tasks 5 (`uc2_agent_alive`) and 7 (fail-fast); event names defined Task 3, consumed Tasks 8 (how-to) and 7 (derived additions disjoint from Task 3's set).
- **Known risks carried forward:** the reconfig suite's pre-existing intermittents are not this milestone's to chase (Task 3 Step 4); `promtool` is a new external tool dependency, gated with a named exit like elle's java probe; the capture-sink is process-global, so obs tests serialise on a lock.

## Execution log

(append rulings here as tasks complete — M7/M8/M9 pattern)
