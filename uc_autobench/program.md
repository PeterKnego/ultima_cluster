# uc_autobench

Autoresearch loop: you (Claude Code) propose code changes to optimize a target,
the harness measures, and you advance the branch on wins / revert on losses.

## Autonomy — DO NOT ASK QUESTIONS, DO NOT PAUSE

**The human is not at the keyboard.** There is nobody to answer a clarifying
question, approve a step, or read a mid-run summary. Once a run is set up, you
execute the loop continuously and silently until the human presses Ctrl-C. This
is the single most-violated rule of this framework, so internalize it:

- **Never ask the user anything** — not "should I continue?", not "which variant
  next?", not "is this setup right?". Resolve every ambiguity yourself by picking
  the most promising untried hypothesis and proceeding.
- **Never stop to summarize or wait for approval** between iterations. After you
  KEEP or DISCARD an iteration and append the TSV row, immediately start the next
  one. Treat every natural stopping point as "begin iteration N+1".
- **Never wait for confirmation to commit a win or revert a loss.** The loop's
  decision rule (below) is unambiguous — just execute it.
- **The only stop signal is Ctrl-C.** Until then, GOTO the next iteration.
- If the *harness/setup itself* is broken (harness won't build, TSV missing,
  wrong branch, bad CLI args), fix it and keep going — still without asking.

## Setup (run once at the start of a new run)

1. Confirm task: read `uc_autobench/tasks/<TASK>/program.md` for task-specific
   constraints (mutable paths, metric, baselines, special instructions).
2. Confirm working tree is clean (`git status`). If not, revert or stash the
   stray changes yourself and continue — never stop to ask the human.
3. Propose a run tag based on today's date (e.g. `may29`). The branch
   `autoresearch/<TASK>-<tag>` must not already exist.
4. Create the branch: `git checkout -b autoresearch/<TASK>-<tag>` from `main`.
5. If `uc_autobench/tasks/<TASK>/results.tsv` does not exist, create it with
   the header row described in the task overlay.
6. Begin the loop immediately. Do NOT wait for confirmation — the human is not
   present and only intervenes via Ctrl-C (see "Autonomy" above).

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

- NEVER stop on your own, and NEVER ask the user a question (see "Autonomy"
  above — the human is absent and only intervenes via Ctrl-C). The human will
  interrupt when satisfied. If you're stuck, think harder: re-read the in-scope
  files, look for combinations of prior near-misses, try more radical structural
  changes.
- NEVER touch frozen files. NEVER add a dependency to `Cargo.toml`. NEVER
  modify `run-iter`, microbench, e2e, or torture binaries.
- Simplicity wins: a small gain that adds ugly complexity is not worth it.
  A wash that deletes code is always a keep.
- Use `git checkout --` for reverts, not `git reset --hard` (which would
  drop the TSV row you just added).
