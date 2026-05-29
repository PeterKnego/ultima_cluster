# uc_autobench

Autoresearch loop: you (Claude Code) propose code changes to optimize a target,
the harness measures, and you advance the branch on wins / revert on losses.

## Setup (run once at the start of a new run)

1. Confirm task: read `uc_autobench/tasks/<TASK>/program.md` for task-specific
   constraints (mutable paths, metric, baselines, special instructions).
2. Confirm working tree is clean (`git status`). If not, stop and ask.
3. Propose a run tag based on today's date (e.g. `may29`). The branch
   `autoresearch/<TASK>-<tag>` must not already exist.
4. Create the branch: `git checkout -b autoresearch/<TASK>-<tag>` from `main`.
5. If `uc_autobench/tasks/<TASK>/results.tsv` does not exist, create it with
   the header row described in the task overlay.
6. Confirm setup with the human, then begin the loop.

## The loop

LOOP FOREVER:

1. Read state: `cat uc_autobench/tasks/<TASK>/results.tsv`. Find current best.
2. Form a hypothesis. Write a one-line description.
3. Edit ONLY files in `mutable_paths` (see task overlay). Never touch
   `frozen_paths`. Never edit the microbench, e2e, torture, or fixture code.
4. Run: `cargo run -p uc_autobench --bin run-iter --release -- \
     --task <TASK> --json \
     --baseline-spsc-p99-ns <best> --baseline-e2e-p99-ns <e2e_best> \
     > /tmp/run-iter.json 2>&1`
5. Parse: `jq '.status, .metrics, .gate, .stderr_tail' /tmp/run-iter.json`.
6. Decide:
   - status=="pass" AND primary metric improved AND gate passed → KEEP:
     append TSV row, `git add -A`, `git commit -m "<TASK>: <description>"`.
   - status=="pass" but no improvement OR gate failed → DISCARD:
     append TSV row with status=discard, revert mutable paths with
     `git checkout -- <mutable_paths>`, then `git add` the TSV and commit
     `discard: <description>`.
   - status starts with "*_failed" or "timeout" → CRASH:
     same as DISCARD but status=crash and description includes the failure
     stage. If the failure looks like a trivial fix (typo, missing import),
     attempt up to 2 quick fixes before giving up.
7. GOTO 1.

## Rules

- NEVER stop on your own. The human will interrupt. If you're stuck, think
  harder: re-read the in-scope files, look for combinations of prior
  near-misses, try more radical structural changes.
- NEVER touch frozen files. NEVER add a dependency to `Cargo.toml`. NEVER
  modify `run-iter`, microbench, e2e, or torture binaries.
- Simplicity wins: a small gain that adds ugly complexity is not worth it.
  A wash that deletes code is always a keep.
- Use `git checkout --` for reverts, not `git reset --hard` (which would
  drop the TSV row you just added).
