# The FSM upgrade lifecycle — release docs, the SDLC standard, the how-to and the skill (plan D) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `2.13.0` release documentable and cuttable: the lifecycle the spec designed (three axes, the change taxonomy, the common origin, version-as-input, the nine conventions, the per-row S1–S9 stages) lands in the SDLC standard and the application-upgrade how-to; the judgement steps of the diff-replay loop become a project skill; the release writeup (`RELEASES.md`, `docs/releases.md`, the reference sweep, `CLAUDE.md`, the crates.io order with a fourteenth crate, the semver notes) is written before the tag; and every carry ledgered by plans B1–C is closed.

**Architecture:** Docs and release scaffolding only — no wire, cnc, or product-code change. Plans A, B1, B2, B3 and C (PRs #50–#56) put everything on `main`; this plan writes it up and closes the loop the spec's §11 table left open (items 1, 2, 9). The one code-adjacent surface is release packaging: `uc_diffreplay` is a publishable crate the ordered publish list, CI's `publish-check` batch, and the crate-count prose do not yet know about, and the version bump moves every lockstep pin.

**Tech Stack:** Markdown; the repo's link checker (`scripts/check_doc_links.py`); `cargo package`/`cargo publish --dry-run` (CI's `publish-check`); the `.claude/skills/<name>/SKILL.md` project-skill format (`.claude/skills/review-branch/SKILL.md` is the model).

**Spec:** `docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md` — §2.1 (axes), §2.2 (common origin), §2.4 (taxonomy), §2.5 (version as input), §3 (S1–S9), §5 (conventions 5.1–5.9), §8 (skill support: 8.1 where it helps, 8.2 where it does not), §11 items 1, 2, 9; plus each plan's "Errata … as built" block (B1 under §2.5, B2 under §3 S4, B3 under §6.5.2, C under §6.2) — the errata are the as-built truth wherever they differ from the body. The release procedure is `docs/how-to/cut-a-release.md` §1 ("Before the tag: the writeup is part of the release").

## Global Constraints

- **The tag is not cut here.** Plan D ends at the writeup and the version bump; `cut-a-release.md` §2–§7 (workflow dry run, tag, verify, crates.io, after) are the maintainer's, and `RELEASES.md`/`docs/releases.md`'s new headings carry a `<tag date>` placeholder flagged the way `v2.8.1`'s were (`<!-- tag date: fill at tag time -->`), so `grep -rn "tag date\|PENDING:" RELEASES.md docs/releases.md` finds every pre-tag scaffold.
- **Every doc statement is true against `main` at `5fe8a91`**, and the load-bearing ones cite the code (file:line in the task report). No dev-box number is presented as a fact; the plan-C e2e values (249/398/400) are labelled as one run's evidence where quoted.
- **Retained plans and specs are not rewritten.** The spec gains at most one erratum item (plan C's block, item 10 — the `§6.3` any-language clause) and nothing else; plan files are untouched.
- **`RELEASES.md` shape** (CLAUDE.md § Release documentation): one bullet per feature, each linking to a doc that exists; an optional fixed-bugs bullet; an optional performance bullet. The `docs/releases.md` entry is the engineering record with a release-evidence table whose post-tag rows read `pending` until the tagger fills them.
- **The crate count is fourteen from this release**: `uc_diffreplay` (`publish` unset ⇒ publishable, versioned in lockstep, added in plan A) joins the thirteen. Every place that says "13"/"thirteen" is updated, and the publish order places it after every crate it depends on (`uc_service`, `uc_protocol`, `uc_journal`, and — default features on — `uc_node`, `uc_net`, `uc_log`, `uc_client`).
- **Version bump = one lockstep move**: root `Cargo.toml` `[workspace.package] version`, every intra-workspace `version = "2.12.0"` pin, and the literal strings outside the manifest that `cut-a-release.md` §1 names (`packaging/compose.yml`, `packaging/Dockerfile` comments, `docs/QUICKSTART.md`, `docs/how-to/run-a-cluster.md`) — found by `grep -rn "2\.12\.0" packaging/ docs/ Cargo.toml */Cargo.toml examples/*/Cargo.toml testing/*/Cargo.toml`, each hit judged (a historical mention of the 2.12.0 release stays; a "current version" string moves).
- `python3 scripts/check_doc_links.py` → 0 errors; `cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets -- -D warnings`; the MSRV gate `CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings` (a lesson from PR #56: 1.89's clippy fires lints 1.96 does not); `./scripts/check_publish_metadata.sh`; the batched `cargo package --no-verify -p …` over all fourteen crates; `cargo test --workspace` after the bump. No `git stash`. Scratch under `$HOME/scratch/`. Use a private `CARGO_TARGET_DIR`.

### Errata against the spec text (decided while planning; Task 4 records item 10 in the spec)

1. **Items 1 and 2 land in ONE document.** §11 item 1 folds §2.1/§2.2/§2.4/§2.5/§3 into `application-sdlc.md`; item 2 says the §5 conventions are "written to the SBE header and pointing at deliverable 2". There is no SBE header yet (deliverable 2 is its own unwritten spec), so the conventions become the standard's "Schema and protocol versioning" section, each convention stated as the rule an application follows today, with the SBE-header enforcement (5.1, 5.7) marked as deliverable 2's and pointed at, not promised.
2. **The how-to is per row, and the pin is a one-way door.** §3 S6 says the flag day "becomes" per-row; as built (plan B2), a pinned row refuses its old binary by name forever after the pin and there is no unpin verb (`docs/BACKLOG.md`, "pin consumed"), so the how-to's rollback section says so: after the pin, rollback is the pre-upgrade off-node backup restored on every node, not the old binary restarted.
3. **The skill is a project-local `.claude/skills/` skill**, not a `docs/agents/` pointer: it runs a procedure (draft, classify, attribute, judge) over a diff and a report, which is what `review-branch` already is in this repo; `CLAUDE.md` § Agent skills gains a line naming it.
4. **The fourteenth crate is a release fact plan D records, not a plan-D decision** — plan A left `uc_diffreplay` publishable on purpose ("versioned in lockstep at 2.12.0 like the other 13 — a decision recorded here; the release-order note in `cut-a-release.md` §6 is updated in plan D").
5. **`ApplyCtx::ids` takes `&mut self`** (`uc_service/src/traits.rs:155`, since plan A counted calls for the driver's `ids_calls` surface). `apply` receives `&mut ApplyCtx`, so every in-tree caller compiles unchanged, but a caller holding `&ApplyCtx` would not: it is recorded in `semver-policy.md` as an additive-in-practice change riding the minor, beside the FSM-identity carve-out — not hidden.

---

## File structure

| file | responsibility |
|---|---|
| `docs/reference/application-sdlc.md` | the standard: gains the three axes, the change taxonomy, the common-origin requirement, version-as-input, the nine conventions, and the per-row S1–S9 upgrade lifecycle (Task 1) |
| `docs/how-to/upgrade-an-application.md` | rewritten around S4 — instant, pin, confirm on every node, stop, swap, verify, roll back, close axis H (Task 2) |
| `.claude/skills/diff-replay-judge/SKILL.md` | the §8 skill: declaration drafting, diff classification, residue attribution, state-diff judgement, hazard spotting (Task 3) |
| `RELEASES.md`, `docs/releases.md` | the `v2.13.0` writeup and engineering record (Task 4) |
| `docs/reference/semver-policy.md`, `docs/how-to/cut-a-release.md`, `.github/workflows/ci.yml`, `.github/workflows/release.yml`, `deny.toml`, `scripts/check_publish_metadata.sh`, `uc_diffreplay/Cargo.toml` | the fourteenth crate, the publish order, the semver notes (Task 4) |
| `CLAUDE.md` | the project-status block, the standing facts, the crate list and count (Task 4) |
| the spec (plan C errata item 10), `docs/BACKLOG.md` (close the two plan-D carries), `docs/reference/README.md`, `docs/how-to/README.md`, `README.md` | pointers and carries (Tasks 1, 2, 4) |
| root `Cargo.toml` + every workspace-member `Cargo.toml`, `packaging/compose.yml`, `packaging/Dockerfile`, `docs/QUICKSTART.md`, `docs/how-to/run-a-cluster.md` | the `2.13.0` bump (Task 5) |

---

### Task 1: The SDLC standard learns the lifecycle (spec §11 items 1 and 2)

**Files:**
- Modify: `docs/reference/application-sdlc.md` (124 lines today: §1 Design, §2 Implementation with 2.1 determinism / 2.2 schema and protocol versioning / 2.3 review gates, §3 Testing, §4 Verification, §5 Rollout, §6 Observability, summary checklist)
- Modify: `docs/reference/README.md:34` and `docs/how-to/README.md:79` (their one-line descriptions of the standard), `docs/tutorials/build-an-application.md` (its "upgrade" step points at the standard's lifecycle section)
- Test: `python3 scripts/check_doc_links.py`; a reviewer truth check against the spec's errata blocks

**Interfaces:**
- Consumes: spec §2.1 (the axes table), §2.2, §2.4 (the taxonomy table, incl. the `IdGen` trap and the decode trap paragraphs), §2.5 (+ plan B1 errata), §3 S1–S9 (+ plan B2 errata under S4), §5.1–5.9, §6.5.1 (the bug-fix corpus), and the shipped surfaces: `uc2ctl upgrade pin/show` (`docs/reference/uc2ctl.md` § `upgrade pin`), `uc2-diffreplay` (`docs/how-to/diff-replay.md`), the attach refusals (`docs/reference/state-machine-contract.md` § Attaching).
- Produces: section anchors Task 2 and Task 3 link to — `#the-three-compatibility-axes`, `#the-change-taxonomy`, `#the-common-origin`, `#the-version-is-an-input`, `#schema-and-protocol-conventions`, `#the-upgrade-lifecycle-per-row` with sub-anchors `#s1-classify-the-change` … `#s9-close-axis-h` (GitHub slug rule: lower-case, spaces → `-`, punctuation dropped).

- [ ] **Step 1: Read the sources once, then restructure the standard**

Keep the existing six sections and the checklist; the standard stays one page an application team reads top to bottom. Insert, in this order:

1. Under **§1 Design Phase**, a new subsection **"The three compatibility axes"** — the §2.1 table verbatim (P / H / F, question, window, closed by) and its three paragraphs, rewritten in the standard's imperative voice ("Name which axis a change touches before you design it"). Axis H's "closed by" points at `#s9-close-axis-h`.
2. Under §1, **"The change taxonomy"** — the §2.4 table (thirteen rows, five columns) as the classification every version bump starts with, followed by the two expansions (the `IdGen` trap; the decode trap with its Appendix A numbers: 12 probes, 5 silent misparses, 4 of 5 caught by a length check — [#49]). Under the table, the codec note: the rows describe the bincode tier as shipped; under deliverable 2 (typed-over-SBE) the first five change character — link the spec §5.7.
3. Under §1, **"The common origin"** — §2.2's requirement in the box quote ("Every instance of the new version must begin from the same log position, reconstructed from the same artifact"), why (the artifact is the only faithful carrier of pre-upgrade history), and what UC provides: the coordinated instant (`FRAME_TYPE_SNAPSHOT`, 2.11.0) as the origin and the `UpgradePin` (2.13.0) as the mechanism that makes every new-version service start from it.
4. Under §1, **"The version is an input, and it is not in the log"** — §2.5 in two paragraphs plus the B1 errata facts: the `UpgradePin` record (`CLUSTER kind 4`, `row ‖ from ‖ to ‖ origin`), per-row history (≤ 4 pins), the cnc words (`upgrade_origin`, `pinned_version`, `pinned_from`, `pin_seq`), refusals 52–59 by name, `uc2ctl upgrade pin/show`.
5. Replace **§2.2 "Schema and protocol versioning"** with **"Schema and protocol conventions"** — the nine §5 rules, each one heading + the rule in two to five sentences + what enforces it today: 5.1 the version tag (today: `const VERSION`, equality-checked at snapshot install and at the pinned attach; the per-command SBE tag is deliverable 2, not shipped — link the spec §5.1/§5.7); 5.2 tolerant readers are wrong for replication; 5.3 package the FSM as a library crate; 5.4 queries are the probe suite; 5.5 expand/migrate/contract — two releases; 5.6 timer ids are a permanent namespace; 5.7 codec choice (the three responses, in the spec's order; the decision that 2.13.0 ships with bincode + [#49]'s length check); 5.8 provide a state projection (`SnapshotStateMachine::project`, the `project` CLI form — `examples/kv` is the worked example); 5.9 keep a regression corpus (`examples/kv/tests/corpora/README.md`, `--around <pos>`).
6. Under **§5 Rollout Phase**, a new subsection **"The upgrade lifecycle, per row"** — S1–S9 as numbered sub-headings, each: what it is, the command or artifact, what refuses you if you skip it. S1 Classify (the taxonomy → obligations); S2 Declare (`pack_version`, `const VERSION`; what `0` means); S3 Shims (the three, from the spec: the image reader for the old snapshot, the old-command arm, the migration step — and that they are not interchangeable); S4 Pin (instant → `uc2ctl upgrade pin --row R --to VER --origin P` → the cluster agent writes the words → unconditional install at attach → refusal backstop; B2's errata as facts: the fourth word, tri-state `pin()` fails closed, install happens in `attach` and needs `start_with_snapshots`, two cross-checks, `ULTSNAP2`, the floor hold); S5 Diff replay (`upgrade` mode with a declaration; `pin-verify` as the pre-flag-day rehearsal — link the how-to § "Verify the pin live"); S6 Roll out (link `upgrade-an-application.md`); S7 Confirm (`/metrics` `uc2_snapshot_hash_mismatch` live; `upgrade show` one instant behind); S8 The point of no return (rollback is the pre-upgrade off-node backup; the pin is a one-way door); S9 Close axis H (when the old arm may be deleted: a pinned origin above the last occurrence of the old format, and the row's oldest retained artifact is at or above it).
7. **The summary checklist** gains one line per new obligation (classify; declare; the three shims; pin before stopping; rehearse with `pin-verify`; confirm on `/metrics`; keep the off-node backup; close axis H on the next version).

Voice: the standard's existing sentences are short imperatives with a "why" clause; match them. Where the spec says "decided (§10 Qn)", the standard says the rule, not the decision history. Every command shown must exist (`uc2ctl upgrade pin --row <R> --to <MAJOR.MINOR.PATCH> --origin <P> [--from …]` is the reference's synopsis at `docs/reference/uc2ctl.md:419`).

- [ ] **Step 2: Update the two README descriptions and the tutorial's pointer**

`docs/reference/README.md:34` and `docs/how-to/README.md:79`: the standard now covers "the lifecycle an application follows — including the upgrade stages S1–S9 and the conventions a version change must respect". `docs/tutorials/build-an-application.md`'s upgrade step links `application-sdlc.md#the-upgrade-lifecycle-per-row` beside its link to the how-to.

- [ ] **Step 3: Check links, self-review against the errata**

Run: `python3 scripts/check_doc_links.py` — Expected: `0 error(s)`.
Self-review: every S4 sentence agrees with the plan B2 errata block (the fourth word, fail-closed `Contended`, install in `attach`, `PinRequiresSnapshots`, two cross-checks, `ULTSNAP2` 24 B, the floor hold); every 2.5 sentence with plan B1's (refusals 52–59, staged `upgrade.pending`, the seqlock word).

- [ ] **Step 4: Commit**

```bash
git add docs/reference/application-sdlc.md docs/reference/README.md docs/how-to/README.md docs/tutorials/build-an-application.md
git commit -m "docs: the SDLC standard learns the upgrade lifecycle — axes, taxonomy, common origin, version-as-input, the nine conventions, S1–S9 per row (plan D T1)"
```

---

### Task 2: The application-upgrade how-to, rewritten around the pin (per row)

**Files:**
- Modify: `docs/how-to/upgrade-an-application.md` (whole page; keep its preamble's `pin-verify` rehearsal paragraph, its § 4 "Verify" body — the `/metrics`-live vs `upgrade show`-one-instant-behind text is true and reviewed — and its "Why a flag day" closing section, updated)
- Modify: `docs/BACKLOG.md` — the "Plan D carries: `docs/how-to/upgrade-an-application.md` …" entry moves to the file's Shipped/closed convention (read how the file marks a closed item; do not delete history)
- Test: link checker; a reviewer truth check against `uc_ctl/src/main.rs:1035` (the `status` line's fields), `docs/reference/uc2ctl.md` § `upgrade pin` / `upgrade show`, `uc_service/src/config.rs:171-215` (the four attach refusals' text), `docs/reference/instance-directory.md` § the pinned origin's set is exempt.

**Interfaces:**
- Consumes: Task 1's anchors; the shipped commands: `uc2ctl snapshot`, `uc2ctl upgrade pin --row R --to VER --origin P [--from VER]`, `uc2ctl status` (prints, per row, `… upgrade_origin=<P> pinned=<VER> pinned_from=<VER> artifact_hash=0x…` — `uc_ctl/src/main.rs:1035-1036`), `uc2ctl upgrade show`, `uc2ctl backup`/`restore`, `uc2-diffreplay pin-verify`.
- Produces: the page other docs link as the S6 procedure.

- [ ] **Step 1: Rewrite the page to this outline**

```
# Upgrade an application
  (what this is; distinct from upgrading the cluster; since 2.13.0 an upgrade
   is a PINNED, per-row flag day: you name the origin before you stop anything,
   the platform refuses the old binary after it, and the new one installs the
   origin unconditionally)
## Before you start
  - S1–S3 done (link the standard): classified, VERSION declared, the shims
    written (the new binary reads the old image)
  - a maintenance window (seconds per row)
  - admin access; the off-node backup destination
  - the rehearsal paragraph (keep verbatim from today's page)
## 1. Back up every node first — the rollback point does not survive the upgrade
  (keep; add: the backup must predate the pin — after the pin the old binary
   cannot rejoin, so this copy is the ONLY way back)
## 2. Take the origin: one coordinated instant, P
  uc2ctl snapshot …  → note P from its output / `uc2ctl status`'s snapshot_pos
  (the complete set at P on every node; wait for `uc2_snapshot_instant_position`
   or `status` to show P everywhere)
## 3. Pin the row to the new version at P
  uc2ctl upgrade pin --row 0 --to 2.0.0 --origin P --instance-dir … --app-id … --admin-key …
  (leader-only; refusals by name: 52 undeclared row, 53 from mismatch, 54 no
   complete set at P, 55 not monotone, 56/57/58 the staged file; what each means
   and what to do)
## 4. Confirm the pin on EVERY node before you stop anything
  uc2ctl status … | grep 'row=0'   →  upgrade_origin=P pinned=2.0.0 pinned_from=1.0.0
  (this is the cnc words, live — NOT `upgrade show`, which reads the artifact
   and lands one instant behind; a node that has not applied the pin yet would
   let its old service reattach)
## 5. Stop every service of the row — all of them, before starting any
  (keep today's text; the mixed-window hazard [#33] is unchanged)
## 6. Install the new binary and start every service
  (what happens at attach, in order: the pin is read; a binary whose VERSION is
   not the pinned one exits with `… is pinned to version … cannot rejoin after
   uc2ctl upgrade pin`; a row started with plain start() is refused
   `PinRequiresSnapshots`; otherwise snap-P is installed UNCONDITIONALLY — a
   durable state machine above P is rewound to it — and the tail from P is
   replayed under the new version; the node's log line `pinned install of
   snap-P` is the record)
## 7. Verify
  (keep today's § 4 body: versions, a pre-upgrade read, the /metrics live
   gauge, `upgrade show` one instant behind with the second-instant recipe,
   `nodes=`, DIVERGED → `uc2-diffreplay determinism`)
## Rolling back
  - before step 3: stop the new services, start the old — nothing pinned
  - after step 3: the pin is a one-way door; the old binary is refused by name
    on every node; roll back by restoring step 1's backup on every node (the
    backup predates the pin) and starting the old binary — the writes since
    the backup are lost, which is why step 1 is not optional. There is no
    unpin verb (docs/BACKLOG.md "pin consumed").
## Afterwards: close axis H
  (S9: once every retained artifact is at or above a pinned origin that
   covers the last old-format command, the next version may delete the old
   arm; until then it stays)
## Why a flag day, and not a rolling swap
  (keep; update "at 2.12.0" → the pin makes the ORIGIN safe, not the mixed
   window; log-stamped application versions are still backlog; [#33])
```

The refusal texts are quoted from `uc_service/src/config.rs` (`PinnedVersionMismatch`, `PinRequiresSnapshots`, `PinnedArtifactMissing`, `PinUnreadable`) and the reason numbers from `docs/reference/uc2ctl.md` § `upgrade pin` — copy them, do not paraphrase numbers.

- [ ] **Step 2: Close the BACKLOG carry, check links**

`docs/BACKLOG.md`: the `upgrade-an-application.md` plan-D carry is closed in the file's own convention (a "Shipped" move or a struck line with the date and this plan's name — match what the file does for closed items).
Run: `python3 scripts/check_doc_links.py` — Expected: `0 error(s)`.

- [ ] **Step 3: Commit**

```bash
git add docs/how-to/upgrade-an-application.md docs/BACKLOG.md
git commit -m "docs: upgrade an application — the pinned, per-row procedure: instant, pin, confirm on every node, stop, swap, verify, roll back, close axis H (plan D T2)"
```

---

### Task 3: The diff-replay judgement skill (spec §11 item 9, §8)

**Files:**
- Create: `.claude/skills/diff-replay-judge/SKILL.md`
- Modify: `CLAUDE.md` § "Agent skills" (one new sub-heading naming the skill and when to invoke it; the block's other three entries are the model)
- Test: the skill's own dry run (Step 2) against the plan-A corpus in `examples/kv/tests/corpora/`; `python3 scripts/check_doc_links.py`

**Interfaces:**
- Consumes: `uc_diffreplay/README.md` § "The declaration (`intent.toml`)" (the format: `tag_offset`, `[tags]`, `[timers]`, `[touched] arms/migration`, `[[expect]] surface/arm/note`), § "The trace an app binary writes"; the report (`docs/how-to/diff-replay.md` § 4 and the spec §6.4: verdicts pass / undeclared / unexplained / absent, the profile's surfaces `response`, `sched`, `projection_origin`, `projection_end`, `ids_calls`, `output`); the taxonomy (Task 1's anchor `#the-change-taxonomy`); `uc_service/src/ids.rs` (the `IdGen` ordinal rule).
- Produces: a skill named `diff-replay-judge`.

- [ ] **Step 1: Write the skill**

`.claude/skills/diff-replay-judge/SKILL.md`, frontmatter in the `review-branch` shape, body exactly this structure (fill each section with the procedure, not a summary of it):

```markdown
---
name: diff-replay-judge
description: Use before and after a `uc2-diffreplay upgrade` run on an ultima_cluster state machine — to DRAFT the intent declaration from the code diff, CLASSIFY the change against the taxonomy, ATTRIBUTE the report's unexplained residue to a hunk, JUDGE whether the state diff at the origin is the migration the code made, and SPOT the determinism hazards a lint cannot (a changed `ids()` call count, `HashMap` iteration, floats, a mid-enum insert, a field reorder). The harness is code; this skill decides what to run and explains what broke. Not for anything an assertion already answers (does the snapshot load, do two digests match, does every row report the new version) — see spec §8.2.
---

# diff-replay-judge

## When to use
(the five moments from spec §8.1, each one line: before the run — draft; at S1 — classify;
 after a run with `unexplained` entries — attribute; after a run with a `projection_origin`
 diff — judge; on any diff touching apply/ids/collections — hazards)

## Inputs
| what | where |
|---|---|
| the code diff | `git diff <old>..<new> -- <fsm crate>` (the FSM is a library crate, convention 5.3) |
| the corpus | `uc2-diffreplay corpus export …` output dir (manifest: row, origin, end, version) |
| the report | `--report R.json` of an `upgrade` run (profile + verdicts) — optional for draft/classify |
| the two binaries | `--old`/`--new` with `replay`/`project` forms |

## Procedure

### 1. Draft the declaration (before the run)
(read the diff; list every command arm it touches and how — response, state, timers,
 output; map arms to the encoding's first bytes (`[tags]`, `tag_offset` = 16 for a
 `Sessioned` app) and timer ids to arm names (`[timers]`); write `[touched] arms = […]`,
 `migration = true` iff the image format or the state shape changed; one `[[expect]]`
 per intended difference with `surface` and a one-line `note`. Hand the file to the
 developer: they edit and own it. Output: `intent.toml`.)

### 2. Classify the change (S1)
(for each hunk, the taxonomy row it matches — name the row verbatim — and the
 obligations that row incurs: axis-P/H risk, downgrade safety, whether S4's pin is
 what makes it safe, whether S9 will owe the old arm. Output: a short obligations
 list the S2–S9 stages consume. If a hunk matches "inserted mid-enum" or "fields
 reordered", stop and say so first: those are the measured silent-misparse shapes.)

### 3. Attribute the unexplained residue (after the run)
(for each report entry with verdict `unexplained`: the position, the surface, the
 arm (from the tag), both versions' values; read both versions' code path for that
 arm and name the hunk that explains the difference — or say "cannot attribute" with
 what you looked at. Never mark an entry explained without a hunk.)

### 4. Judge the state diff at the origin
(`projection_origin` differs ⇒ the two versions read the same artifact into different
 state — the migration delta. Question: is every line of that delta the migration the
 code made (S3's image reader), and nothing else? A line the migration does not
 account for is a defect in the reader, not in the harness. Output: "the migration
 is exactly …" or the first line it cannot account for.)

### 5. Spot the invisible hazards
(walk the diff for: a changed number of `ctx.ids()` calls on an existing path (the
 ordinal rule in `uc_service/src/ids.rs` — every later id in that call moves; the
 report's `ids_calls` surface shows it); iteration over a `HashMap`/`HashSet` feeding
 apply output or the image (`RandomState` differs per process — the `determinism`
 mode catches it, this step names the line); a float in replicated state; `SystemTime`/
 `Instant`/RNG inside apply. Output: file:line per hazard, or "none found" with the
 files read.)

## What this skill does not do
(spec §8.2 verbatim in spirit: it does not validate that a snapshot loads, compare
 digests, or check versions — those are `uc2-diffreplay` and `uc2ctl` assertions and
 dressing them as judgement makes them slower and less trustworthy.)

## Worked example
(`examples/kv`: the v1→v2 change and the corpus under `examples/kv/tests/corpora/`;
 the declaration that ships there; one `unexplained` residue and its attribution;
 the `projection_origin` migration delta and its judgement.)
```

The worked example must be real: read `examples/kv/tests/corpora/README.md` and the shipped `intent.toml` there, run `uc2-diffreplay upgrade` on it once (the README says how), and write what the report actually shows.

- [ ] **Step 2: Dry-run the skill's procedure once, by hand, and record it**

Follow §1–§5 of the skill against the kv corpus as if invoked; the result is the "Worked example" section. If a step cannot be followed as written, fix the skill, not the example.

- [ ] **Step 3: Register it in `CLAUDE.md`**

Under `## Agent skills`, add `### Diff-replay judgement` — two sentences: the five judgement steps of the diff-replay loop (draft, classify, attribute, judge, hazards) are `.claude/skills/diff-replay-judge/SKILL.md`; the harness itself is `uc2-diffreplay` (`docs/how-to/diff-replay.md`).

- [ ] **Step 4: Commit**

```bash
git add .claude/skills/diff-replay-judge/SKILL.md CLAUDE.md
git commit -m "skills: diff-replay-judge — draft the declaration, classify the change, attribute the residue, judge the state diff, spot the invisible hazards (plan D T3, spec §8)"
```

---

### Task 4: The `v2.13.0` writeup, the fourteenth crate, the semver notes, CLAUDE.md

**Files:**
- Modify: `RELEASES.md` (top), `docs/releases.md` (top), `docs/reference/semver-policy.md` (§ "The `2.13.0` flag day"; § "The promised surface" or the carve-out section for the two API notes), `docs/how-to/cut-a-release.md` (§6 list + "Thirteen" + the `publish = false` sentence), `.github/workflows/ci.yml` (`publish-check`: the batched `-p` list and its "13" comments), `.github/workflows/release.yml:29` (comment), `deny.toml` (the bans comment naming 13, if present), `scripts/check_publish_metadata.sh` (its crate list, if it has one), `uc_diffreplay/Cargo.toml` (crates.io metadata — `keywords`/`categories` inherit the workspace's; verify `./scripts/check_publish_metadata.sh` passes with it included), `CLAUDE.md` (status block; standing facts; "Workspace crates" list; the "13 publishable crates" bullet), the spec (plan C errata item **10**), `docs/BACKLOG.md` (close the `uc2_cluster_fsm_position` plan-D carry), `README.md` (its "Scope and limits" pointer only if it names a version).
- Test: `python3 scripts/check_doc_links.py`; `./scripts/check_publish_metadata.sh`; `cargo package --no-verify` over the fourteen; `grep -rn "13 publishable\|thirteen\|13 crates\|Thirteen" …` → only historical mentions remain.

**Interfaces:**
- Consumes: the four archived ledgers' "what shipped" lines (`~/uc2-sdd-archive/2026-09-20-uc2-upgrade-pin-and-snapshot-report/progress.md`, `…/2026-09-21-uc2-pinned-install-at-attach/`, `…/2026-09-21-uc2-live-snapshot-reports/`, `…/2026-09-21-uc2-pin-verify/`), the five spec errata blocks, the merged PRs #50–#56 (`gh pr view N --json title,body`), the two hotfixes (#53 counter `NodeBooting` + nightly fixtures + docs link; #55 dead anchor), `docs/releases.md`'s `v2.12.0` entry as the template (its intro, the per-feature table, the "Release evidence" table).
- Produces: the headings `## v2.13.0 — <tag date> — the FSM upgrade lifecycle` in both files with `<!-- tag date: fill at tag time -->` beneath; `docs/releases.md`'s evidence table with `pending` post-tag rows.

- [ ] **Step 1: `RELEASES.md`**

Replace the current `## Unreleased` heading with `## v2.13.0 — <tag date> — the FSM upgrade lifecycle` + the tag-date comment. Lead paragraph: one flag day, wire `0.8.0` → `0.9.0` and cnc `3.2` → `3.3`, plus a `ULTSNAP2` artifact-envelope change that needs one wipe of `snapshots/<row>/` per node (unsafe on purge-on with an in-memory state machine — say it, link `upgrade-a-cluster.md` § 2.13.0). Then the bullets, each linking docs that exist:
- **Upgrade pins** (B1): the `UpgradePin` record, per-row history, `uc2ctl upgrade pin/show`, refusals 52–59, the cnc words, gauges `uc2_upgrade_pin_origin/version`. → the standard § version-as-input, `uc2ctl.md`, the cluster-FSM explainer § Pins.
- **The pinned install at attach** (B2): unconditional install of the origin, the four attach refusals, `ULTSNAP2`, the same-version gap guard, the purge-floor hold at an unconsumed pin. → `state-machine-contract.md` § Snapshots / Attaching, `instance-directory.md`.
- **Live snapshot-hash reports** (B3): `artifact_hash` on the page, `SNAP_REPORT` (26), every-voter-or-timeout `SnapshotReport`, the verdict at commit, `uc2_snapshot_hash_mismatch`, `snapshot_hash_diverged`, `Uc2SnapshotHashDiverged`. → `monitor-a-cluster.md`, the explainer.
- **The readiness gate and `boot_wait`** (B3 T5): a service or client attaches only once its node has joined its cluster; `NodeBooting` semantics; `boot_wait` on three config structs; start every node before attaching any service; `services_declared_published/withheld`. → `configuration.md`, `limits.md`.
- **Diff replay** (A): `uc_diffreplay` — the crate, the driver, `upgrade`/`determinism`/`reconstruction` modes, `project()`, the regression corpus, `--around`. → `docs/how-to/diff-replay.md`, the README.
- **`pin-verify`** (C): reconstruction mode part 2 on a real node; the durable fixture; PASS requires the observed install. → the how-to § 5.
- **The SDLC standard and the per-row upgrade how-to** (this plan) and **the `diff-replay-judge` skill**.
- **Also in this release**: the dogfood bullets that are on `main` untagged today (`examples/kv`, the tutorial, the experience reports, the reference fixes) — fold the current Unreleased text under a sub-bullet "The application lifecycle, documented end to end", trimmed.
- **Fixed**: the apply overrun→replay livelock (pre-existing; now takes the gap guard's path); the `uc_node` log-sink capture flake; the `counter-service` `NodeBooting` exit (#53); nightly's missing fixture builds (#53); the docs landing-page link and a dead anchor (#53, #55); the `CncPage::meta` panic is `2.12.0`'s — do not repeat it.
- **Changed readings** (the plan-D carries): `uc2_cluster_fsm_position` now reads the cluster agent's walk cursor (`consumed`), the quantity its help text always described — a dashboard keeps working and reads a different number; `uc2_timers_rearmed_total` was `2.11.0`'s retirement — do not repeat.
- **API notes** (link semver-policy): `ServiceConfig`/`EngineConfig`/`PipelinedConfig` gain `boot_wait` (public fields — exhaustive struct literals in downstream code break); `ApplyCtx::ids` takes `&mut self`; `SnapshotStateMachine::project` is a provided method.
- **Performance**: none measured — no fleet gate ran for this release; say so in one line rather than omit the bullet silently.

- [ ] **Step 2: `docs/releases.md`**

A `## v2.13.0 — <tag date> — the FSM upgrade lifecycle` entry above `v2.12.0`, in the `v2.12.0` entry's shape: the intro paragraph (baseline `v2.12.0`; the plans and PRs in merge order A #50 → B1 #51 → B2 #52 → #53 → B3 #54 → #55 → C #56 → D); the per-feature table (spec / plans / explainer / how-to / gate doc — "none; no fleet gate" / flag-day surface); one `###` section per plan summarising what shipped and the rulings that changed the design (from each errata block, by number); a `### Fixed on the way` section; and the `### Release evidence` table with the `ci.yml`/`docs.yml`/`release.yml`/tag/crates.io rows reading `pending — filled at tag time` and the local rows filled from this plan's Task 6 run (fmt, clippy ×5 incl. MSRV, `cargo test --workspace`, `pin_verify`, the link checker, `publish-check` batch). End with `<!-- PENDING: tag-time evidence rows above -->` so the tagger's grep finds it.

- [ ] **Step 3: `semver-policy.md`**

§ "The `2.13.0` flag day" is stale (it predates B3 and C): rewrite it to the as-built surface — wire `0.9.0`: `CLUSTER` kinds 4 and 5 AND the pairwise `SNAP_REPORT` 26; cnc `3.3`: status-line words `+16 upgrade_origin`, `+24 pinned_version`, `+32 pin_seq`, `+40 pinned_from` and line-7 `+504 artifact_hash`; `ULTSNAP2` (24 B) with the one-time wipe; the cluster image v2 (v1 read). Add, beside the FSM-identity carve-out, a short **"`2.13.0` API notes"** subsection: the three `boot_wait` public fields (additive, but breaks exhaustive literals — the policy's stance on public-field additions, stated); `ApplyCtx::ids(&mut self)` (erratum 5 above); `SnapshotStateMachine::project` provided.

- [ ] **Step 4: The fourteenth crate**

`cut-a-release.md` §6: "Fourteen crates"; insert `cargo publish -p uc_diffreplay` after `cargo publish -p uc_node` (it depends on `uc_service`, `uc_protocol`, `uc_journal`, and with default features `uc_node`, `uc_net`, `uc_log`, `uc_client` — all earlier); one sentence on why; the `publish = false` sentence gains `kv_store` and keeps `uc_lincheck` (whose optional dependency on `uc_diffreplay` is dev-only in effect). `.github/workflows/ci.yml` `publish-check`: `-p uc_diffreplay` in the batched `cargo package` (after `-p uc_node`), comments "13" → "14"; `release.yml:29` "thirteen" → "fourteen"; `deny.toml`'s bans comment if it counts; `scripts/check_publish_metadata.sh` if it enumerates. Run `./scripts/check_publish_metadata.sh` and `CARGO_TARGET_DIR=… cargo package --no-verify -p uc_journal -p uc_protocol -p uc_obs -p uc_crypto -p uc_log -p uc_consensus -p uc_net -p uc_client -p uc_service -p uc_node -p uc_diffreplay -p uc_remote -p uc_gateway -p uc_ctl` — both must pass (a missing `description`/`license`/`repository` on `uc_diffreplay` fails here, not at release time).

- [ ] **Step 5: `CLAUDE.md`**

The Project status block: a new **"`2.13.0` — on `main`, unreleased"** paragraph ABOVE the `2.12.0` one (one flag day: wire `0.9.0`, cnc `3.3`, `ULTSNAP2`; the seven plans A–D by name with their PRs; the standing consequences a new task must know: stop every node before starting any; start every node before attaching any service; the one-time `snapshots/<row>/` wipe; the pin is a one-way door; `uc2_cluster_fsm_position` repointed; 14 crates and the publish order; MSRV clippy before every push). "Current version" stays `2.12.0` (tagged) until the tag — say "the newest tag is `2.12.0`; `main` carries `2.13.0` unreleased". Standing facts: the wire/cnc bullet ("The wire protocol SHIPPED is 0.8.0") becomes "`0.9.0` on `main`, `0.8.0` the newest tag"; the "13 publishable crates" bullet → 14 with `uc_diffreplay`; the "Workspace crates" list gains a `uc_diffreplay` entry (one paragraph in the list's voice); § Build & Test gets the MSRV clippy line and `cargo test -p uc_diffreplay --test pin_verify -- --test-threads=1`; "Next up" is rewritten: (1) cut `2.13.0` (`cut-a-release.md` §2–§7), (2) the standing bar question, (3)–(5) carried from today's list.

- [ ] **Step 6: Spec erratum 10 and the backlog**

In the spec's `#### Errata (plan C, as built)` block, append **10.** — `pin-verify`'s PASS requires the SDK's `pinned install of snap-P` stderr line, which only `uc_service`'s Rust attach path prints, so §6.3's "Language: any" row holds for `replay`/`project`/`determinism`/`upgrade` but a non-Rust service half, or one behind a wrapper that swallows stderr, always FAILs `pin-verify`; and update that block's count sentence ("Nine places" → "Ten"). `docs/BACKLOG.md`: close the `uc2_cluster_fsm_position` plan-D carry the way Task 2 closed the other.

- [ ] **Step 7: Link check, greps, commit**

Run: `python3 scripts/check_doc_links.py` (0 errors); `grep -rn "tag date\|PENDING:" RELEASES.md docs/releases.md` (exactly the scaffolds you placed); `grep -rn "13 publishable\|thirteen\|13 crates\|Thirteen" .github/ deny.toml scripts/ docs/how-to/cut-a-release.md CLAUDE.md` (only mentions inside historical release entries remain — list each survivor in the report with why it stays).

```bash
git add RELEASES.md docs/releases.md docs/reference/semver-policy.md docs/how-to/cut-a-release.md .github/workflows/ci.yml .github/workflows/release.yml deny.toml scripts/check_publish_metadata.sh uc_diffreplay/Cargo.toml CLAUDE.md docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md docs/BACKLOG.md README.md
git commit -m "docs: the v2.13.0 writeup — RELEASES, the engineering record, semver notes, the fourteenth crate in the publish order and publish-check, CLAUDE.md status (plan D T4)"
```

(Only add the files you changed; `git add` of an untouched file is harmless but the list above is the expected set.)

---

### Task 5: The `2.13.0` version bump

**Files:**
- Modify: root `Cargo.toml` `[workspace.package] version = "2.13.0"`; every workspace member's `Cargo.toml` intra-workspace pin `version = "2.12.0"` → `"2.13.0"` (`grep -rln 'version = "2.12.0"' --include=Cargo.toml .`); `Cargo.lock` (regenerated by any cargo command); `packaging/compose.yml` (`${UC2_IMAGE:-ghcr.io/peterknego/uc2:2.12.0}` → `2.13.0`), `packaging/Dockerfile` comments, `docs/QUICKSTART.md` and `docs/how-to/run-a-cluster.md` worked-example strings — every `grep -rn "2\.12\.0" packaging/ docs/QUICKSTART.md docs/how-to/run-a-cluster.md` hit that names the CURRENT version (a hit inside a historical release paragraph stays).
- Test: `cargo package --no-verify` over the fourteen (Task 4's command); `cargo test --workspace`; `cargo run -p uc_ctl -- --version` and `cargo run -p uc_node --bin uc2-node -- --version` print `2.13.0`.

**Interfaces:** none downstream; this is the release commit's mechanical half (`cut-a-release.md` §1's fourth and fifth checkboxes).

- [ ] **Step 1: Bump, regenerate the lock, verify the version strings**

```bash
grep -rln 'version = "2.12.0"' --include=Cargo.toml . | sort      # the pins to move (expect the root + every member that pins a sibling)
# edit each; then
CARGO_TARGET_DIR=$HOME/.cache/cargo-target-b3 cargo metadata --format-version 1 --no-deps | python3 -c "import json,sys; m=json.load(sys.stdin); print(sorted({p['version'] for p in m['packages']}))"
```
Expected: `['2.13.0']` only.

- [ ] **Step 2: The literal strings outside the manifest**

`grep -rn "2\.12\.0" packaging/ docs/QUICKSTART.md docs/how-to/run-a-cluster.md` — update each current-version hit; leave historical ones; list both kinds in the report.

- [ ] **Step 3: Prove it packages and runs**

Run: the fourteen-crate `cargo package --no-verify` (Task 4 Step 4's command) — Expected: fourteen `Packaged` lines, no error. `CARGO_TARGET_DIR=… cargo run -p uc_ctl -- --version` → `uc2ctl 2.13.0`; `cargo run -p uc_node --bin uc2-node -- --version` → `2.13.0`; `cargo run -p uc_gateway --bin uc2-gateway -- --version` → `2.13.0`.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock */Cargo.toml examples/*/Cargo.toml testing/*/Cargo.toml packaging/ docs/QUICKSTART.md docs/how-to/run-a-cluster.md
git commit -m "release: bump the workspace to 2.13.0 (lockstep pins, packaging and quickstart strings) (plan D T5)"
```

---

### Task 6: Proof stack and the release-evidence rows

**Files:**
- Modify: `docs/releases.md` (fill the LOCAL rows of the `v2.13.0` evidence table with this task's tails; post-tag rows stay `pending`)
- Test: everything below, tails pasted verbatim into the report.

- [ ] **Step 1: The stack, sequential, private target dirs**

`cargo fmt --all -- --check`; `cargo clippy --workspace --all-targets -- -D warnings`; the four feature-gated clippy runs (`-p uc_crashtest --features hard-crash-tests`, `-p uc_lincheck --features replay-bin`, `-p uc_service --features apply-profile`, `-p uc_gateway --features test-util`); `cargo clippy -p uc_diffreplay --no-default-features --lib --bins -- -D warnings`; **`CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings`**; `./scripts/check_publish_metadata.sh`; the fourteen-crate `cargo package --no-verify`; `cargo build -p uc_lincheck --features replay-bin --bin register-replay`; `cargo build -p uc_diffreplay`; `cargo test --workspace`; `cargo test -p uc_diffreplay --test pin_verify -- --test-threads=1`; `cargo test -p uc_node --test lin_v2`; `cargo test -p uc_crashtest --features hard-crash-tests`; `(cd fuzz && CARGO_TARGET_DIR=$HOME/.cache/cargo-target-b3-fuzz cargo +nightly fuzz build)`; `python3 scripts/check_doc_links.py`. A non-green tail is reported verbatim with a reading, never retried into green.

- [ ] **Step 2: Fill the local evidence rows, commit**

```bash
git add docs/releases.md
git commit -m "docs: v2.13.0 release evidence — the local rows (plan D T6)"
```

---

## Self-review

**Spec coverage.** §11 item 1 (axes, taxonomy, common origin, version-as-input, per-row stages → the standard; the how-to per row) → Tasks 1 and 2. Item 2 (§5 conventions, pointing at deliverable 2) → Task 1 Step 1.5 with erratum 1. Item 9 (skill: declaration drafting, attribution, state-diff judgement) → Task 3, all five §8.1 steps, with §8.2's boundary. The release documentation rule (CLAUDE.md, `cut-a-release.md` §1) → Tasks 4–6. The carries: `upgrade-an-application.md` rewrite (T2), `uc2_cluster_fsm_position` note (T4), `boot_wait`/`ids` semver lines (T4 Step 3), §6.3 clause (T4 Step 6), publish order + crate count (T4 Step 4), `CLAUDE.md` (T4 Step 5), MSRV clippy in the stack (T6). Items 10–11 (phase 2) are out of scope, as the spec says.

**Placeholder scan.** The skill's section bodies are described in parentheses inside the code block — those parentheses are instructions to the implementer for prose it must write in full (no parenthetical survives into the file); the worked example must be run, not invented. `<tag date>` and `pending` are deliberate scaffolds the release procedure retires, named in Global Constraints.

**Type consistency.** Anchors produced by Task 1 are the ones Tasks 2 and 3 link (`#the-change-taxonomy`, `#the-upgrade-lifecycle-per-row`, `#s9-close-axis-h`); the publish order string in Task 4 Step 4 is the one Task 5 Step 3 and Task 6 run; the `status` field names in Task 2 (`upgrade_origin=`, `pinned=`, `pinned_from=`) are `uc_ctl/src/main.rs:1035`'s.
