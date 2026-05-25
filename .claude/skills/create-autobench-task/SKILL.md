---
name: create-autobench-task
description: Use when the user wants to add a new optimization task to the uc_autobench framework — phrases like "create a new auto-optimization task", "scaffold uc_autobench task for X", "add an autobench task for the ultima_db btree". Project-local. Scaffolds task.toml + Rust impl + microbench/e2e binaries + torture test stub, then prints a closing checklist. Do not use for one-off changes; this is specifically for new optimization targets that should run through the uc_autobench loop.
---

# create-autobench-task — scaffold a new uc_autobench optimization task

## What this skill does

Creates the seven files needed to register a new task with the `uc_autobench` framework:

1. `uc_autobench/tasks/<id>/task.toml`
2. `uc_autobench/src/tasks/<id>.rs`
3. `uc_autobench/src/bin/<id>-microbench.rs` (runnable stub printing zeros)
4. `uc_autobench/src/bin/<id>-e2e.rs` (optional; only if user wants Goodhart gate)
5. `uc_autobench/tests/<id>_torture.rs` (stub with one passing test + a TODO checklist)
6. Edit `uc_autobench/src/tasks/mod.rs` to add `pub mod <id>;`
7. Edit `uc_autobench/src/bin/auto-bench.rs` to register the task in the match arm
8. Edit `uc_autobench/Cargo.toml` to register the new `[[bin]]`s

Then prints the closing checklist (see end of this skill).

## When to use

- User says "I want to add an auto-optimization task for X" (where X is anything in ultima_cluster or ultima_db).
- User asks how to add a new uc_autobench task.

## When NOT to use

- For a one-off optimization the user can do by hand.
- For tuning an existing task — that's editing `tasks/<id>/task.toml`, not scaffolding.
- For framework changes themselves — those go through normal brainstorming.

## Workflow

### Step 1 — gather inputs (one question at a time)

Ask the user, one question per message, with multiple-choice options where possible:

1. **Task id and short description** — e.g. `udb-btree`, "Optimize ultima_db B-tree node layout". Used as `task.id` and the slug for filenames.
2. **Target module to optimize** — repo-relative path (e.g. `ultima_db/src/btree/`). The skill reads this dir to suggest sensible defaults for `mutable_paths`.
3. **Contract mode** — `rust_api` / `rust_api_plus_wire` / `behavior_only`. Explain each briefly (see `uc_autobench/CLAUDE.md` "The contract" section). Recommend based on whether the module has a wire format.
4. **Mutable + frozen paths** — propose defaults based on the module structure, ask user to confirm or override.
5. **Primary microbench metric** — what scalar to optimize (e.g. `lookup_p99_ns`, `bytes_per_sec`). And direction (minimize/maximize).
6. **E2E gate?** — Y/N. If Y: what binary + metric + regress_pct. **Strongly recommend Y** — without it, Goodhart bites. If N, explicitly warn and add a note in `extra_prompt_context()`.
7. **Budget** — `max_iterations` (default 200), `wall_clock_hours` (default 12), `plateau_window` (default 30). Offer defaults; user can override.

### Step 2 — scaffold the files

Generate each file from inputs. Use `Read` to inspect the target module (#2) before generating mutable_paths defaults.

The `task.toml` mirrors `uc_autobench/tasks/shmem/task.toml` — copy that as the schema reference. `microbench.metrics` is the source of truth for the metric keys; everything below must agree with it.

The `src/tasks/<id>.rs` impl provides a struct implementing the `OptimizationTask` trait (see `uc_autobench/src/task.rs`). The trait methods you must implement:

- `fn id(&self) -> &str`
- `fn spec(&self) -> &TaskSpec`
- `fn read_state(&self, root: &Path) -> anyhow::Result<HashMap<PathBuf, String>>` — read the `mutable_paths` files so the LLM sees current state.
- `fn parse_microbench(&self, stdout: &str) -> anyhow::Result<BenchResult>` — parse the one JSON line into a `BenchResult` (usually just `BenchResult::from_json_line(stdout)`).
- `fn parse_e2e(&self, stdout: &str) -> anyhow::Result<BenchResult>` — has a default that reuses `parse_microbench`; only override if the e2e binary emits a different shape.
- `fn extra_prompt_context(&self) -> &str` — task-specific invariants the LLM must respect (and, if no e2e gate, a warning about known cheap-shots).

Use `ShmemTask` in `src/tasks/shmem.rs` as the reference impl.

The microbench/e2e binaries should be **runnable stubs** — they print one flat JSON line of `{string: f64}` with **zeros** for all metrics, so the harness wires through end-to-end before the real bench logic is written. The metric keys MUST match `task.toml`'s `microbench.metrics` exactly (a typo becomes a silent NaN). The JSON line goes to **stdout**; everything else (timing, progress) goes to stderr.

The torture test stub should contain exactly one passing test (`fn smoke_compiles()`) and a `// TODO:` checklist enumerating the conformance tests the user must implement. Reference `uc_autobench/tests/ring_torture.rs` as the model — note that file is FROZEN (the loop never proposes changes to it) and validates the target via its PUBLIC API only, with a CRC trailer per payload to catch torn reads. **Do not auto-generate the real conformance tests** — they require human understanding of the target's invariants. Produce only the stub + checklist.

### Step 3 — register the task

Edit `uc_autobench/src/tasks/mod.rs` to add `pub mod <id>;`.

Edit `uc_autobench/src/bin/auto-bench.rs`: add a match arm for the new task id alongside the existing `"shmem"` arm (which builds `Box::new(ShmemTask::load()?)` and falls through to `other => anyhow::bail!("unknown task: {other}")`).

Edit `uc_autobench/Cargo.toml`: add `[[bin]]` entries (`name` + `path`) for the new `<id>-microbench` and (if gated) `<id>-e2e` binaries, matching the existing `shmem-microbench` / `shmem-e2e` entries.

### Step 4 — sanity check

Run: `cargo build -p uc_autobench`
If it fails, fix and re-try before printing the checklist.

### Step 5 — print the closing checklist

Print this verbatim (substituting `<id>`):

> Task `<id>` scaffolded. Before running:
> 1. Implement the conformance suite in `tests/<id>_torture.rs` — **this is the correctness floor; do not skip.**
> 2. Implement the microbench in `bin/<id>-microbench.rs` — make sure JSON keys match `task.toml`'s `metrics`.
> 3. (If gated) implement the e2e binary.
> 4. Run `cargo test -p uc_autobench --test <id>_torture` and `cargo run -p uc_autobench --bin <id>-microbench --release` once by hand to verify wiring.
> 5. Then: `cargo run -p uc_autobench --bin auto-bench --release -- --task <id>`

## Notes

- Do not generate the conformance tests automatically — they require understanding the module's invariants. The skill produces a stub with a `// TODO:` checklist.
- Do not generate the real microbench logic — same reason. Stubs are runnable so the harness wires through, but they print zeros.
- The skill MUST run `cargo build -p uc_autobench` after generation. A scaffolded task that doesn't compile defeats the purpose.