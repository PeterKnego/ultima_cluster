# Hard-crash validation — design

## Goal

Prove service-state reconstruction + the ReadIndex/seqlock read barrier survive a
**true hard crash** of the service process (`kill -9` / abort mid-apply), not the
graceful shutdowns the in-process lincheck capstone uses. Single-node topology;
correctness checked with the existing WGL linearizability checker.

## Why this needs new infrastructure

Today both the lincheck capstone and `counter_loop_service` run `uc_node` +
`uc_service` **in one OS process** (two tokio tasks over a shared instance_dir).
Nothing can `kill -9` the service while the node survives. The shmem architecture
is *designed* for the split; we just need reference binaries that each run one
half, plus a harness that spawns them as child processes and hard-kills the
service. (This also realizes the `multi-process-tests` path CLAUDE.md references
but that doesn't exist yet.)

## Topology (v1)

- 1 **node** process: `NodeBuilder::start` (single-node bootstrap) — creates the
  instance_dir/`cnc.dat`, runs raft, waits for the service handshake, serves
  clients. Runs until killed.
- 1 **service** process: waits for `cnc.dat`, `ServiceBuilder::run` — attaches and
  runs the SM. Runs until killed; restartable on the same instance_dir.
- Test driver: in-process `uc_client` submitting Writes/CAS + linearizable Reads.

Multi-node (3 node processes + QUIC across procs) is explicitly out of scope for
v1 (much larger orchestration; revisit later).

## SM + correctness

- SM: the existing in-memory CAS-register (`RegisterSm`: `Write`/`Cas`/`Read`),
  non-persisting — so surviving a hard crash proves node-driven reconstruction.
- Checker: the existing WGL linearizability checker. The `uc_client` driver records
  its own op history (invoke ts, return ts, outcome incl. Indeterminate for
  timeouts) and runs the checker on it. `kill -9` mid-write ⇒ the in-flight op is
  Indeterminate (may or may not have committed) — the checker already models that.

## Checker reuse: extract `uc-lincheck` lib

The checker (`checker.rs` 269, `history.rs` 95, `model.rs` 73) and the register
SM/types (`register_sm.rs` 120) live in `uc_node/tests/lincheck/` as test modules —
not reachable from another crate. Extract them into a new workspace lib crate
**`uc-lincheck`** (depends on `uc_service` for the `StateMachine` trait):

- `uc-lincheck`: `history`, `model`, `checker`, `register` (Cmd/CmdResp/RegisterSm).
- Refactor `uc_node/tests/lincheck/` to `use uc_lincheck::…` (delete the 4 copied
  modules; keep `cluster.rs` — it's the in-process harness, capstone-only). Re-run
  the capstone (10 seeds) to confirm no regression. One source of truth for the
  checker.

## Crash mechanism

The test spawns the service via `std::process::Command` (path from
`env!("CARGO_BIN_EXE_uc-crashtest-service")`) and crashes it with
`Child::kill()` (SIGKILL on Unix — a true uncatchable hard crash, no graceful
shutdown / no in-flight-apply completion). It crashes *during* sustained load so a
kill lands mid-apply, then respawns the service on the same instance_dir.

## Components

New crate `examples/uc-crashtest/`:
- `[[bin]] uc-crashtest-node` — node-only (CLI: instance_dir, data_dir, app_id).
- `[[bin]] uc-crashtest-service` — service-only (CLI: instance_dir, data_dir).
- `tests/smoke.rs` — spawn node + service, write 1/2/3 + read via `uc_client`,
  assert, clean shutdown. (Establishes the harness.)
- `tests/hard_crash.rs` — spawn node + service; N seeded workers drive Write/Cas/
  Read via `uc_client`, recording a `uc_lincheck::History`; a fault loop `kill -9`s
  the service mid-load and respawns it (a few times); assert the recorded history
  is `Verdict::Linearizable` across seeds.

`uc-lincheck/` lib (new workspace member).

Touched: `Cargo.toml` (workspace members), `uc_node/tests/lincheck/*`, CLAUDE.md
(document the real multi-process/hard-crash test path).

## Phasing

1. **Extract `uc-lincheck`** + refactor the capstone to use it; capstone green (10 seeds).
2. **Multi-process foundation**: node + service bins + `smoke.rs` (spawn, write/read, shutdown).
3. **Hard-crash test**: `hard_crash.rs` (kill -9 mid-load + restart + WGL checker), seeds green.

## Risks / open points

- **Test hygiene:** these tests spawn real binaries and bind real (localhost) ports;
  mark them `#[ignore]` or gate behind a `hard-crash-tests` feature so the default
  `cargo test` stays fast/hermetic; run explicitly in CI.
- **Flake risk:** hard crashes + restarts are timing-heavy; use condition-polling
  (not fixed sleeps) for readiness, generous client deadlines (Indeterminate is
  checker-tolerated), and a bounded fault count.
- **kill -9 mid-apply on the node side:** the node's `apply()` parks on the service
  rings; a hard service crash leaves no epoch bump until restart. The
  service-watcher + reconstruct-on-reattach + seqlock should cover it — this test
  is precisely what proves that (or finds the gap).
- **Capstone refactor risk:** mechanical `mod`→`use` change, verified by re-running
  the 10-seed capstone.
