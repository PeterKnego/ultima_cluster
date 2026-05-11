---
name: review-branch
description: Use when reviewing local ultima_cluster changes that are not yet on the remote — committed-but-unpushed commits and/or uncommitted working-tree edits. Run this before `git push`, before opening a PR, or whenever you want a sanity pass on a branch in progress. Tailored to ultima_cluster conventions (uc_protocol no_std posture, unsafe shmem ring code, openraft trait impls, atomic memory ordering, raft log_index pinning, M3 MPSC/Broadcast wrap-race caveat). Project-local; for generic PRs use pr-review-toolkit:review-pr instead.
---

# review-branch — local pre-push review for ultima_cluster

## Overview

Reviews everything on the local branch that the remote hasn't seen: committed commits ahead of `origin/main`, plus uncommitted working-tree changes. Mechanical checks (`cargo build`, `clippy`, `test`) run first to surface cheap failures; then specialized review subagents run **in parallel** with `ultima_cluster`-specific context; findings are aggregated by severity.

## When to use

- Before `git push` on a branch with new work.
- Before opening a PR.
- After finishing a task in a multi-task plan, to sanity-check before moving on.
- When picking up a stale branch you haven't looked at recently.

**Do not use:**
- For an existing GitHub PR — use `pr-review-toolkit:review-pr` (it has full PR context: description, CI, prior review threads).
- For reviewing `origin/main` itself — that's already shipped.

## Inputs (slash-args)

| Flag | Effect |
|---|---|
| _(none)_ | Review committed-but-unpushed commits **and** uncommitted working tree |
| `--committed-only` | Skip working-tree (use when you've staged a checkpoint you don't want reviewed yet) |
| `--worktree-only` | Skip committed commits (use to review just the last batch of edits) |
| `--base <ref>` | Override the comparison base (default `origin/main`) |
| `--skip-mechanical` | Skip cargo build/clippy/test (use if you already ran them) |

## Workflow (the actual procedure to execute)

### Step 1 — Compute the review range

```bash
# 1a. Make sure origin/main is up to date enough to compare against.
#     If the repo has no remote, fall back to the local `main` ref.
git fetch origin main 2>/dev/null || true

# 1b. Resolve the base ref (default origin/main, override via --base).
BASE="${BASE:-origin/main}"
git rev-parse --verify "$BASE" >/dev/null 2>&1 || BASE=main

# 1c. Enumerate unpushed commits + per-file change summary.
git log --oneline "$BASE..HEAD"
git diff --stat "$BASE...HEAD"
git diff --stat                # working-tree only
git status -s                  # untracked + unstaged
```

If there is **nothing** in `BASE..HEAD` and the working tree is clean: tell the user there's nothing to review and exit.

### Step 2 — Cheap mechanical checks (skip if `--skip-mechanical`)

Run all four in parallel via Bash with `run_in_background: true`, then wait and collect:

```bash
cargo build --workspace 2>&1
cargo clippy --workspace --all-targets -- -D warnings 2>&1
cargo test --workspace --no-fail-fast 2>&1
cargo fmt --check 2>&1
```

Any failure here is a **Blocker** finding — surface it before any LLM review. The point is fast feedback; don't burn reviewer subagent time on bugs the compiler will catch.

### Step 3 — Triage which review subagents to dispatch

Inspect the diff to decide:

| Condition | Dispatch |
|---|---|
| Always (any code change) | `pr-review-toolkit:code-reviewer` |
| Always (any code change) | `pr-review-toolkit:silent-failure-hunter` |
| New `pub struct` / `pub enum` / `pub trait` added | `pr-review-toolkit:type-design-analyzer` |
| Files in `*/tests/` changed OR new `#[test]` / `#[tokio::test]` blocks | `pr-review-toolkit:pr-test-analyzer` |
| Doc-comment churn (>20 added/removed doc-comment lines) | `pr-review-toolkit:comment-analyzer` |

Detection commands (run after Step 1):

```bash
# New pub types added:
git diff "$BASE...HEAD" -- '*.rs' | grep -E '^\+(pub )?(struct|enum|trait) ' | head

# Test changes:
git diff --name-only "$BASE...HEAD" | grep -E '(tests/|_test\.rs$|/test_)' | head
git diff "$BASE...HEAD" -- '*.rs' | grep -E '^\+\s*#\[(tokio::)?test\]' | head

# Doc churn (count added doc-comment lines):
git diff "$BASE...HEAD" -- '*.rs' | grep -cE '^\+\s*///' || true
```

### Step 4 — Dispatch all chosen subagents in one parallel batch

Send all chosen subagents in a **single message with multiple Agent tool calls** (this is mandatory — sequential dispatch wastes wall time).

Each subagent's prompt MUST include:
1. **What changed** — the output of `git diff --stat "$BASE...HEAD"` plus working-tree stat.
2. **How to get the diff** — instruct them to run `git diff "$BASE...HEAD"` for committed changes and `git diff` for working tree. **Don't** paste the diff into the prompt; let them read it themselves so they can navigate to surrounding context.
3. **The branch base** — so they know which commits to consider in-scope.
4. **The ultima_cluster review checklist below** (paste the entire "Checklist for subagents" section into each subagent's prompt verbatim).
5. **Output format** — `Severity: <Blocker|Important|Nit> · <file>:<line> — <one-line finding>` followed by a paragraph of rationale. Keep each finding to ≤ 5 sentences.

### Step 5 — Aggregate findings

Collect all subagent reports + mechanical-check failures into one structured output:

```
## Branch review — <BASE>..HEAD (+ working tree)

### Mechanical
- cargo build:  ✓ / ✗ (<excerpt of failure>)
- cargo clippy: ✓ / ✗
- cargo test:   ✓ / ✗ (N passed / M failed)
- cargo fmt:    ✓ / ✗

### Blockers (must fix before push)
1. <file>:<line> — <finding>
   <rationale>
   _Reported by: code-reviewer_

### Important (should fix; defensible to defer)
…

### Nits (style/wording)
…

### What looks good
- <1-3 bullets — explicit affirmation of solid bits>
```

**Deduplicate** identical findings reported by multiple subagents; keep the most specific phrasing and credit all reporters.

## Checklist for subagents (paste verbatim into each subagent prompt)

> You are reviewing local changes in the `ultima_cluster` repo. Read `/Users/peter/Projects/ultima/ultima_cluster/CLAUDE.md` first; that is the canonical conventions doc. The design spec is at `docs/superpowers/specs/2026-05-10-ultima-cluster-design.md`. Per-task records are in `docs/tasks/`.
>
> **Hard rules (any violation = Blocker):**
>
> 1. `uc_protocol` is `no_std`-friendly **only for** `version.rs`, `magic.rs`, `error_codes.rs`. New code in those three files must not import outside `core`. Other modules (`ring`, `cnc`, `frames`, `liveness`, `handshake`) may use `std`.
> 2. `StateMachine::apply` is **sync, deterministic, no I/O**. No `async`, no clock, no `SystemTime::now()`, no `rand`, no network. Trait signature enforces this; if `apply` reaches I/O via a helper, that's a Blocker.
> 3. `output_handler` is **async, leader-only, retryable**. Returns `Result<(), OutputError>`.
> 4. Reads are typed `Query`/`QueryResponse`, **not closures** across the IPC boundary. Closures are OK only in `IpcMode::Embedded`.
> 5. `AppCommand = bytes::Bytes` end-to-end. No `Vec<u8>` copies in the apply pipeline.
> 6. Per-record framing: **length-last with `Release`** on the producer; consumer reads `producer_position` with `Acquire`, treats `length == 0` as "not yet committed."
> 7. Inter-node QUIC zero-copy: `quinn::SendStream::write_chunks(&[Bytes])`. No internal copies into a single buffer.
> 8. `StoreStateMachine` pins `ultima_db` version to `log_index`: `store.begin_write(Some(log_index))` on every apply.
> 9. `build_snapshot` and `install_snapshot` return `u64` (the represented `log_index`), not unit.
> 10. One node per instance directory — `instance.lock` exclusive flock. Service + clients try shared lock as liveness probe.
> 11. `app_id` + `instance_id` + `protocol_version` checked at every IPC entry point.
>
> **Memory-ordering / unsafe checklist for `uc_protocol::ring`:**
>
> - Every `unsafe` block has a `// SAFETY:` comment that names the invariants the caller upholds.
> - Atomic loads of `producer_position` from the consumer side: `Acquire`. Producer Release-stores `producer_position`.
> - Length field is the commit point: written **last** with a Release fence + non-atomic store. Reading length: byte-copy (atomic-by-alignment is not guaranteed for `*mut u8 + offset`).
> - Tail-wrap padding: msg_type = `0xffff`, length = bytes-to-end-of-slot-region.
> - Ring header tests must use mmap-backed buffers (page-aligned). `Vec<u8>` gives byte alignment and will UB when cast to `*const RingHeader` — flag any test using `vec![0u8; ...]` for ring/cnc validation.
> - MPSC and Broadcast have a **known post-wrap torn-record race** documented in module headers; do NOT use them for high-traffic rings until M4. In M3 they're acceptable only for cnc control rings (tiny traffic, no wrap expected).
>
> **Cargo / workspace conventions:**
>
> - All new deps go through `[workspace.dependencies]` in the root `Cargo.toml`. Per-crate `Cargo.toml` references via `{ workspace = true }`.
> - Tests use `tempfile` (`tempfile = { workspace = true }` in `[dev-dependencies]`), not hand-rolled tempdirs.
> - bincode 2 with `bincode::config::standard()` + `bincode::serde::{encode_to_vec, decode_from_slice}`.
> - `cargo clippy --workspace -- -D warnings` must pass clean. `cargo fmt` must pass.
>
> **Docs convention:**
>
> - In-progress work goes under `docs/superpowers/` (plans, specs). Final, canonical records go under `docs/tasks/taskNN_feature_name.md`. Superpowers artifacts must NOT be committed alongside the final task doc — they are ephemeral. A PR that adds a `taskNN_*.md` file should also delete the corresponding `docs/superpowers/plans/*.md` and `docs/superpowers/specs/*.md` for that feature.
>
> **Test coverage expectations:**
>
> - Every new public type or function in `uc_protocol` has a unit test in the same module.
> - Every new feature in `uc_node` has an integration test in `uc_node/tests/`.
> - Tests that touch shmem layouts must use real mmap'd files (see above).
>
> **Things that are NOT defects (don't flag):**
>
> - `unsafe impl Send for FooInner {}` — these are intentional; `FooInner` owns the mmap and the synchronization is documented above.
> - Empty stub modules for tasks not yet implemented (e.g., `cnc.rs` with just a doc comment).
> - The MPSC/Broadcast wrap race — already documented as a known M4 follow-up.

## Output format Claude returns to the user

A single message with the structure shown in Step 5. Sort findings by severity (Blocker → Important → Nit), then by file path. Use `file:line` so IDE clicks navigate. Cite which subagent reported each finding so the user can drill into raw output if needed.

If everything passes mechanically and no Blocker/Important findings emerge: keep the report short — one summary line plus the green-check mechanical table plus "What looks good" affirmations.

## Variations / edge cases

- **Repo has no `origin/main`** (fresh repo, or working solo). Step 1c falls back to comparing against local `main`. If HEAD == main, there is nothing to review on a branch sense — but working-tree edits are still reviewed.
- **HEAD is `main` and ahead of `origin/main`** (current ultima_cluster state). This is normal — `$BASE..HEAD` is exactly the unpushed commits.
- **Detached HEAD**. Use `git symbolic-ref --short HEAD` to detect; if detached, ask the user what they want as the base.
- **Massive diff (> ~2000 lines changed)**. Don't paste the diff into subagent prompts; have them read it via `git diff` themselves and chunk if needed. Mention the size in the report so the user can adjust expectations.

## Quick reference

```bash
# The 30-second version of this skill if you just want to run it yourself:
BASE=origin/main
git log --oneline $BASE..HEAD
git diff --stat $BASE...HEAD
cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
```

…then dispatch the four `pr-review-toolkit:*` subagents in parallel with the checklist above.
