# `testing/` — internal test harnesses

Workspace crates that exist to **test UC**, not to teach it. They are
`publish = false`, they are not part of the published API, and nothing here is
meant to be read as a worked example.

| crate | what it is |
|---|---|
| [`uc_crashtest`](uc_crashtest) | Multi-process crash-recovery harness: reference node / service / gateway binaries over a shared instance dir, plus the hard-crash, survival, ENOSPC and remote-linearizability suites. The real `kill -9` path for reconstruction validation. |

## Why this is not `examples/`

`examples/` is teaching material a user is invited to read and copy —
[`counter`](../examples/counter) (the hello-world) and
[`kv`](../examples/kv) (the worked `SnapshotStateMachine` + `Sessioned`
application). A harness that spawns processes and SIGKILLs them is neither,
and filing it there invited readers to treat test apparatus as a pattern to
follow. The dogfood charter already had to carve it out by name
(`docs/superpowers/specs/2026-09-13-uc2-dogfood-kv-charter.md`, decision 7:
"not `uc_crashtest`") when deciding what counts as published documentation.

## Running them

Every suite is behind a feature, so a plain `cargo test` never spawns
processes:

```bash
cargo test -p uc_crashtest --features hard-crash-tests   # SIGKILL mid-load, assert linearizable
cargo test -p uc_crashtest --features survival-tests -- --test-threads=1
cargo test -p uc_crashtest --features enospc-tests -- --test-threads=1   # needs scripts/enospc_fixture.sh
```

The package names are unchanged by living here, so every `-p uc_crashtest`
invocation — in `.github/workflows/nightly.yml`, in `uc_node/examples/m11_gate.rs`,
in `bench-infra/scripts/` — works exactly as before.

The wider proof surface these fit into is
[`docs/VERIFICATION.md`](../docs/VERIFICATION.md): the deterministic sim
(`uc_sim`), the WGL lincheck capstones (`uc_lincheck`), Elle, the Lean proofs,
loom, and the fuzz tier.
