---
name: diff-replay-judge
description: Use before and after a `uc2-diffreplay upgrade` run on an ultima_cluster state machine — to DRAFT the intent declaration from the code diff, CLASSIFY the change against the taxonomy, ATTRIBUTE the report's unexplained residue to a hunk, JUDGE whether the state diff at the origin is the migration the code made, and SPOT the determinism hazards a lint cannot (a changed `ids()` call count, `HashMap` iteration, floats, a mid-enum insert, a field reorder). The harness is code; this skill decides what to run and explains what broke. Not for anything an assertion already answers (does the snapshot load, do two digests match, does every row report the new version) — see spec §8.2.
---

# diff-replay-judge

## When to use

- **Before the run — draft.** A version bump is being prepared and no `intent.toml` exists yet: read the code diff and draft the declaration the `upgrade` run will be judged against (step 1).
- **At S1 — classify.** The change is written but nothing downstream has been decided: classify every hunk against the taxonomy and emit the obligations S2–S9 consume (step 2).
- **After a run with `Unexplained` findings — attribute.** The mechanical pass could not name a cause; read both versions' path for that command and name the hunk, or say it cannot (step 3).
- **After a run with a `projection_origin` diff — judge.** The two builds read the same artifact into different state: decide whether that delta is exactly the migration the image reader makes (step 4).
- **On any diff touching `apply`, `on_timer`, `ctx.ids()` or a collection — hazards.** Walk for the determinism hazards that are diff properties rather than single-version defects (step 5).

## Inputs

| what | where |
|---|---|
| the code diff | `git diff <old>..<new> -- <fsm crate>` (the FSM is a library crate, convention 5.3) |
| the corpus | `uc2-diffreplay corpus export …` output dir (manifest: row, origin, end, version) |
| the report | `--report R.json` of an `upgrade` run (profile + verdicts) — optional for draft/classify |
| the two binaries | `--old`/`--new` with `replay`/`project` forms |

Two spellings that are easy to get wrong and silently cost a match. The
declaration and the report name six surfaces — `response`, `sched`,
`projection_origin`, `projection_end`, `ids`, `output`
(`uc_diffreplay::diff::Surface::name`); the per-entry *trace* field behind
the `ids` surface is called `ids_calls`
(`uc_service/src/traits.rs`, `uc_diffreplay::trace::Entry::ids_calls`). Write
`surface = "ids"` in an `[[expect]]`; read `ids_calls` in a trace. And
`[tags]` keys are hex of the command payload's leading bytes *after*
`tag_offset` — 16 for a `Sessioned<S>` app, 0 for a bare one.

## Procedure

### 1. Draft the declaration (before the run)

**In:** the code diff, the corpus, the app's command encoding.
**Out:** `intent.toml` beside the corpus, plus the arm→tag table it was built
from. Hand both to the developer: they edit and own the file. This step
drafts what the change *does*; only the developer can say what it was
*supposed* to do.

1. Read the diff and list every command arm it touches, and on which surface
   each one moves: response bytes, state, timers scheduled or cancelled, the
   number of ids minted, `on_committed` output. One line per arm. An arm the
   diff does not reach belongs on no list — leaving it out is what makes a
   divergence there a finding.
2. Map arm names to the encoding's leading bytes. Do not read the tags off
   the encoder by eye; read them off the corpus, which is what the harness
   will match against:

   ```bash
   <bin> replay --corpus <CORPUS> --out $HOME/scratch/trace.json
   python3 -c 'import json,sys; t=json.load(open(sys.argv[1])); [print(e["pos"], e["kind"], bytes(e["tag"]).hex()) for e in t["entries"]]' \
       $HOME/scratch/trace.json
   ```

   The printed tag is the first 32 payload bytes. Drop `tag_offset` bytes
   from the front (16 for a `Sessioned` app — `client_id ‖ seq`), and the
   next bytes are the app's own discriminant: those are the `[tags]` keys.
   Longest hex prefix wins, so `"01"` and `"0102"` can coexist; keys are
   lowercased at parse.
3. Map timer ids to arm names in `[timers]`, the id as a decimal string
   (`"9" = "reaper"`). A TIMER frame carries no application payload, so
   `[tags]` can never reach it and a timer divergence without this entry is
   permanently `Unexplained`.
4. Write `[touched] arms = […]` — exactly the arms the change is allowed to
   move. Set `migration = true` if and only if the image format or the state
   shape changed: it is the single switch that lets a non-empty
   `projection_origin` attribute at all, and with it false a migration delta
   reads `Unexplained` by construction.
5. Write one `[[expect]]` per intended difference: `surface`, an optional
   `arm`, and a one-line `note` in the developer's words ("put acks now carry
   ttl"). Two rules the parser enforces and one it does not:
   - an `[[expect]]` on `projection_origin` or `projection_end` takes **no**
     `arm` and is refused by name if it carries one (a projection is one
     comparison over the whole state);
   - unknown keys are refused, so a typo cannot quietly read as "not
     declared";
   - *not* enforced: one `[[expect]]` is satisfied by every divergence on its
     `(surface, arm)` pair, so a second `[[expect]]` on the same pair reads
     `Absent` unless two divergences actually arrive. Write one line per
     pair, not one per position.

   Omitting `arm` is not just brevity: an `arm`-less `[[expect]]` is a
   **wildcard** over every arm on that surface — the one-line way to declare
   a change that moves several arms the same way. A specific entry always
   beats a wildcard for the arm it names, regardless of declaration order,
   so a wildcard declared first never steals a specific entry's match; the
   wildcard then covers the rest.
6. Check the file parses before handing it over. `upgrade` is the only mode
   that reads a declaration, so run it with the same binary on both sides:

   ```bash
   uc2-diffreplay upgrade --corpus <CORPUS> --old <BIN> --new <BIN> \
       --declare intent.toml --report $HOME/scratch/parse.json
   ```

   A malformed declaration is refused by name and the run never starts. Read
   the verdicts with care: one binary means an empty profile, so every
   `[[expect]]` in the file will read `Absent`. That run checks the **file**,
   not the change.

### 2. Classify the change (S1)

**In:** the diff; the taxonomy at `docs/reference/application-sdlc.md`
§ "The change taxonomy" (spec §2.4).
**Out:** an obligations list, one line per hunk, that S2–S9 consume, plus the
version digit S2 must bump.

1. **First, before any other classification**, scan for the two measured
   silent-misparse shapes: a command variant **inserted mid-`enum`** and two
   **fields reordered**. If either is present, stop and say so at the top of
   the output. Appendix A probed 12 realistic schema changes and 5 decode
   successfully into a *wrong value*; four of those five leave
   `bytes_read < cmd.len()` and would become the intended fail-stop under a
   length check on the typed tier's decode — that check is filed as #49 and
   is **not shipped** — and the field reorder is byte-identical, so it
   survives even that. Say which tier the app is on: these are hazards of the
   typed (bincode) tier; a raw-tier app whose encoding carries an explicit
   discriminant and refuses trailing bytes gets a named refusal instead, and
   the row still applies to whatever *its* decoder tolerates.
2. For each hunk, name the taxonomy row it matches **verbatim** ("New command
   variant (appended)", "Snapshot image format", "Changed `apply` semantics
   of an existing command", …). A hunk matching no row is worth saying so
   explicitly — the table is the shipped bincode tier and does not cover
   everything.
3. For each named row, carry its four columns into obligations: the axis-P
   risk (can an un-upgraded replica still apply?), the axis-H risk (can this
   binary still apply history?), downgrade safety, and which of S3's three
   shims the change owes — forward decode (must already be in v_old; cannot
   be written retroactively), backward apply (bounded by S9, not permanent),
   snapshot dual-read (new binary, old image). Add, per row: whether S4's
   pinned origin is what makes it safe (it is, for every "changed `apply`
   semantics" row), and whether S9 will owe the deletion of an old arm once a
   pinned origin sits above that shape's last occurrence.
4. Derive the digit S2 must bump: **major** for any row whose axis-P risk is
   severe (the rule is axis-P only; "worst" is an axis-**H** cell and does
   not enter it), **minor** for additive-but-inert (and the `upgrade` run
   is what confirms the inertness), **patch** for no replicated behaviour
   change at all. `NAME` is never bumped — it is the identity hash and the
   `fold32` input to `IdGen`, so changing it is a different FSM with a
   different id stream, not a version change.

### 3. Attribute the unexplained residue (after the run)

**In:** the `--report R.json` of an `upgrade` run; both versions' sources.
**Out:** one line per `Unexplained` finding naming a hunk, or `cannot
attribute` with exactly what was read.

1. Read the findings and the profile:

   ```bash
   jq -r '.findings[] | "\(.verdict) \(.surface) arm=\(.arm) pos=\(.pos) \(.note)"' R.json
   jq -r '.notes[]' R.json
   python3 -c 'import json,sys; r=json.load(open(sys.argv[1])); [print(d["pos"], d["surface"], bytes(d["tag"]).hex(), "a="+bytes(d["a"]).hex(), "b="+bytes(d["b"]).hex()) for d in r["profile"]["entries"]]' R.json
   ```

   `a` is the old build, `b` the new one; `notes` are the caveats the mode
   itself attaches (for example that `reconstruction` compares no origin
   projection), and an empty diff on a surface a note disclaims is not
   evidence.
2. An `Unexplained` finding has one of two notes, and they mean different
   things.
   - **`no touched arm explains this`** — the mechanical pass had a
     divergence with a position, a surface and a tag, and could not reach an
     arm in `[touched] arms`. Two sub-cases, and the first is not a code
     finding: if the tag maps to **no** arm at all, the declaration's `[tags]`
     / `[timers]` / `tag_offset` is wrong — fix the table and re-run. If it
     maps to an arm that is **not in the touched set**, the pass is telling
     you a command the change never claimed to touch has moved: a shared
     helper, a state field that arm reads, or a real bug. That is the entry
     this step exists for.
   - **`position dispatched by one build only (only_in_a|only_in_b)`** — not
     a value difference at all. Read the line's own columns with that in
     mind: it is stamped `response` and `arm=-` whatever diverged, because
     the finding is minted with a fixed surface and no arm, so do not chase
     a response bug on the strength of the word `response`. The two builds
     disagree about *which frames
     the FSM saw*. Look at the wrapper stacks first (`Sessioned`, `Timed` —
     a session envelope answering `replayed` on one side dispatches nothing
     on that side), then at anything that can refuse a frame before dispatch,
     then at the two traces' `origin`/`end`.
3. For each entry, decode `a` and `b` with the app's own decoder — the
   harness treats both as opaque bytes — and state the difference in the
   app's own terms ("status `OK` with version 96 vs status `WRONG_SHAPE`").
4. Read **both** versions' code path for that arm and name the hunk:

   ```bash
   git diff <old>..<new> -- <fsm crate>/src/<the file the arm lives in>
   git log -L :<fn name>:<path> <old>..<new>
   ```

   The output line is `pos=<P> surface=<S> arm=<A> a=<…> b=<…> →
   <file>:<line> (<commit>)`.
5. Never mark an entry explained without a hunk. "Consistent with the
   symptom" is not "the cause" — if the path reads the same in both builds,
   write `cannot attribute` and list the files and functions read, which is
   itself a finding and belongs in the report to the developer.
6. Finish with the `Undeclared` findings (`observed and attributed, but not
   declared`): the mechanical pass named an arm, so each one is either a
   forgotten line in the declaration or a bug in that arm. Telling those
   apart is the same reading as step 4 — say which, per entry. An intended
   difference goes back into `intent.toml` as an `[[expect]]` and **the run
   is repeated**: the verdict comes from the tool's exit code, never from
   this skill's prose.

### 4. Judge the state diff at the origin

**In:** `profile.projection_origin.{removed,added}` in the report; the new
build's image reader (`install_snapshot` and its dual-read branch).
**Out:** the sentence "the migration is exactly …", or the first delta line
the reader cannot account for.

1. Know what this diff is. Both builds installed the **same** artifact at the
   origin P and rendered it through their own `project()`, with no command
   applied. Any difference is therefore the pure migration delta — the S3
   dual-read shim alone, with every behavioural change excluded by
   construction. This is the one place it can be read on its own.
2. Print it and pair the lines by key:

   ```bash
   jq -r '.profile.projection_origin.removed[] | "- " + .' R.json
   jq -r '.profile.projection_origin.added[]   | "+ " + .' R.json
   ```

   A projection is canonical (sorted, one record per line), so the diff is a
   multiset difference and order carries no information.
3. Write the delta as **one sentence** ("every entry gained `shape=value`;
   nothing else moved"). If it cannot be said in one sentence, it is more
   than a migration and the extra part is what to chase.
4. Check that sentence against the reader, line by line, in the code: every
   field the delta **adds** must come from a default the reader supplies on
   the old-image branch; every field it **drops** must be one the reader
   deliberately discards. A delta line the reader does not account for is a
   defect in the **reader**, not in the harness — name the line and the
   branch that should have produced it.
5. Judge `projection_end` only after the origin is settled. It carries the
   migration delta *plus* the behavioural delta over the span, and it
   attributes to the touched set as a whole; untangling the two is exactly
   what step 4's separation buys.
6. Two shapes that look like a missing migration and are not. An empty
   `projection_origin` is never judged at all, so it produces no finding on
   its own; what surfaces is an `[[expect]] surface = "projection_origin"`
   that no diff satisfies, as `Absent`. (`migration = true` by itself is not
   a claim the harness checks — it only decides how a non-empty origin diff
   attributes, and a non-empty one with no matching `[[expect]]` reads
   `Undeclared`.) Either way, check the artifact really is the old image, via each
   trace's `artifact_version` (the version stamped in the `ULTSNAP2`
   envelope; `null` when the run started from genesis) against its `version`
   (the build's own `VERSION`). And a `reconstruction` report compares no
   origin projection at all and says so in `notes` — never judge a migration
   from one.

### 5. Spot the invisible hazards

**In:** the diff and both crates' sources.
**Out:** `file:line` per hazard, each labelled with which shape it is — or
"none found" followed by the list of files actually read. "None found" over
an unread file is worth nothing.

Walk the diff for five shapes. The first is the one no single-version test
and no lint can see.

1. **A changed id-minting shape on an existing path** — two different
   changes with two different consequences, and only one of them is visible
   to the harness. `uc_service/src/ids.rs` computes `permute(position,
   (ordinal << 32) | fold32(identity))`, where the ordinal is the counter
   **inside one generator**: `ApplyCtx::ids()` hands back a fresh `IdGen`
   with `ordinal: 0` every time, and the only other inputs are the frame's
   position and the FSM's identity.
   - **A changed number of `ctx.ids()` calls.** A second generator in the
     same apply call starts from ordinal 0 over the same position and
     identity, so it mints the **identical series** as the first: the
     consequence is *duplicated* ids, not shifted ones (`ids.rs`'s
     `same_inputs_same_series` pins it). This is the one the harness sees —
     `ApplyCtx::ids()` increments `ids_calls`, and `uc_diffreplay` compares
     that count per position as the `ids` surface. Say "duplicated series",
     never "every later id moved", when explaining such a finding.
   - **A changed number of `next()` calls on one generator.** *This* is what
     shifts every later id in that call, because each `next()` consumes an
     ordinal. The harness **cannot** see it: `diff.rs` compares `ids_calls`
     and nothing else, so an extra or removed `next()` reaches the report
     only indirectly, as a `response`, `sched` or projection difference —
     which is exactly why it belongs on this list.

   ```bash
   git grep -n 'ctx\.ids()\|\.ids()\|\.next()' <old> -- <fsm crate>
   git grep -n 'ctx\.ids()\|\.ids()\|\.next()' <new> -- <fsm crate>
   ```

   Compare both counts **per arm**, not per file, and keep them apart: how
   many generators the arm takes, and how many ids it mints from each. A
   `.next()` hit that is not on an `IdGen` is noise — read the receiver.
   This step is what tells you either change is coming before the run, and
   what names the arm when only the second one's side effects arrive.
2. **Iteration over a `HashMap`/`HashSet` feeding apply output, the image or
   the projection.** `RandomState` is seeded per process, so the order
   differs between two runs of the *same* binary. The `determinism` mode
   catches it; this step names the **line**, and does not replace the mode.

   ```bash
   git grep -n 'HashMap\|HashSet' -- <fsm crate>
   ```

   The fix is a `BTreeMap`/`BTreeSet`, or sorting before emitting.
3. **A float in replicated state or in an apply computation** — any
   `f32`/`f64` that reaches state, a response, or the image.
4. **Ambient nondeterminism inside `apply`/`on_timer`**: `SystemTime::now`,
   `Instant::now`, an RNG, a thread id, a pointer address. `ctx.time_ns` is
   the replicated substitute for the clock and `ctx.ids()` for the RNG; both
   are deterministic, and reaching past them is a divergence bug.
5. **The Appendix A shapes**, if the app is on the typed (bincode) tier: a
   variant inserted mid-`enum`, two fields reordered. These are step 2's
   first check; repeat the `file:line` here so the hazard list is complete on
   its own.

## What this skill does not do

It never re-does an assertion. It does not check that a snapshot loads, does
not compare two image digests, and does not check that every instance of a
row reports the new version — `uc2-diffreplay`'s own exit code, `uc2ctl
status` and `uc2ctl upgrade show` answer those mechanically and exactly, and
dressing a mechanical check as judgement makes it slower and less
trustworthy (spec §8.2).

It also does not pronounce the verdict. Everything it produces — a drafted
declaration, an obligations list, an attribution, a hazard list — goes back
through the harness, and the run's exit code is the answer. An intended
difference discovered in step 3 is not "explained" until it is an
`[[expect]]` in the declaration and the `upgrade` run passes with it. And it
does not supply the *intent*: what the change was supposed to do is the
developer's understanding of their own change, which is why step 1 hands the
file over rather than committing it.

## Worked example

`examples/kv` (`KvSm`, raw tier, its own wire format, run live as
`Sessioned<KvSm>`) and the regression corpus
`examples/kv/tests/corpora/put-then-delete/`: two puts (`a=1`, `b=2`) below
the origin, one `delete a` above it, generated from a real in-process node.
Origin 192; the artifact at 192 holds both keys.

**What the tree supports, and what it does not.** `examples/kv` ships **one**
runnable build — the v2 shape (lists, `KV_VERSION = 2.0.0`, image version 2,
reading images 1 and 2). A v1 binary that could answer the `replay`/`project`
contract does not exist in the tree, and `upgrade` takes two binary paths with
no per-side knobs, so **no two-build `upgrade` run is possible here**. Steps 1
and 3 below were run against real reports; steps 2, 4 and 5 were exercised on
the v1→v2 **code** alone, and each says so.

### The runs (verbatim)

```console
$ T=/home/claude/.cache/cargo-target-b3/debug
$ cargo build -p uc_diffreplay -p kv_store          # exit 0

$ $T/uc2-diffreplay determinism \
    --corpus examples/kv/tests/corpora/put-then-delete \
    --bin $T/kv-service --report $HOME/scratch/t3/det.json
diff replay — determinism — corpus examples/kv/tests/corpora/put-then-delete
  divergences: 0 entries, 0 only in a, 0 only in b, origin projection 0−/0+, end projection 0−/0+
  0 pass, 0 undeclared, 0 unexplained, 0 absent → PASS
EXIT=0

$ $T/uc2-diffreplay reconstruction \
    --corpus examples/kv/tests/corpora/put-then-delete \
    --bin $T/kv-service --report $HOME/scratch/t3/rec.json
diff replay — reconstruction — corpus examples/kv/tests/corpora/put-then-delete
  note: projection_origin: not applicable in reconstruction mode — the genesis run installs no artifact, so there is no origin state to compare
  divergences: 0 entries, 0 only in a, 0 only in b, origin projection 0−/0+, end projection 0−/0+
  0 pass, 0 undeclared, 0 unexplained, 0 absent → PASS
reconstruction: end projections AGREE (no semantic change below P)
EXIT=0

$ $T/uc2-diffreplay upgrade \
    --corpus examples/kv/tests/corpora/put-then-delete \
    --old $T/kv-service --new $T/kv-service \
    --declare examples/kv/tests/corpora/put-then-delete/intent.toml \
    --report $HOME/scratch/t3/up.json
diff replay — upgrade — corpus examples/kv/tests/corpora/put-then-delete
  divergences: 0 entries, 0 only in a, 0 only in b, origin projection 0−/0+, end projection 0−/0+
  0 pass, 0 undeclared, 0 unexplained, 0 absent → PASS
EXIT=0
```

The `reconstruction` line is the honest shape of this corpus: the span is one
`delete`, whose result does not depend on the state at P, so the artifact path
and the genesis counterfactual end in the same place and the run shows nothing
about the counterfactual. A span that could tell them apart ends in
state-dependent commands (a CAS chain).

### Step 1 — draft, on a real report

The tags were read off the corpus, not off the encoder:

```console
$ $T/kv-service replay --corpus examples/kv/tests/corpora/put-then-delete \
    --out $HOME/scratch/t3/trace.json
$ python3 -c 'import json,sys; t=json.load(open(sys.argv[1])); [print(e["pos"], e["kind"], bytes(e["tag"]).hex()) for e in t["entries"]]' \
    $HOME/scratch/t3/trace.json
192 Message 010000000000000002000000000000000102010061
```

Sixteen bytes of `Sessioned` envelope (`client_id = 1`, `seq = 2`, LE), then
`01` `02` — `FORMAT_VERSION ‖ OP_DELETE` — then the key (`0100` length, `61`
= `a`). That is exactly the shipped declaration's `tag_offset = 16` and
`[tags] "0102" = "delete"`, confirmed against the bytes the harness will
match. The same trace carries `version = 33554432` and
`artifact_version = 33554432` (both `0x02000000` = 2.0.0): this artifact was
written by the build replaying it. In a genuine v1→v2 run the two would
differ, which is the step-4 check.

The file-parses check found two real refusals. An `[[expect]]` with an `arm`
on a projection surface is refused before the run starts:

```console
$ $T/uc2-diffreplay upgrade --corpus examples/kv/tests/corpora/put-then-delete \
    --old $T/kv-service --new $T/kv-service \
    --declare $HOME/scratch/t3/intent-bad.toml --report $HOME/scratch/t3/up-bad.json
Error: declaration: [[expect]] surface = "projection_origin" takes no arm — projections are attributed to the touched set as a whole
EXIT=1
```

and a declaration whose expectation the run never observes reads `Absent` —
here, the shipped file plus one `[[expect]] surface = "response", arm =
"delete"`, against two identical binaries:

```console
$ $T/uc2-diffreplay upgrade --corpus examples/kv/tests/corpora/put-then-delete \
    --old $T/kv-service --new $T/kv-service \
    --declare $HOME/scratch/t3/intent-absent.toml --report $HOME/scratch/t3/up-absent.json
diff replay — upgrade — corpus examples/kv/tests/corpora/put-then-delete
  divergences: 0 entries, 0 only in a, 0 only in b, origin projection 0−/0+, end projection 0−/0+
  Absent      response           arm=delete     pos=-        declared but not observed: delete acks now carry the freed byte count
  0 pass, 0 undeclared, 0 unexplained, 1 absent → FAIL
EXIT=1
```

That is the column order to read in every text report — verdict, surface,
arm, position, note — and the reason step 1 says the parse check tests the
file rather than the change.

### Step 2 — classify, on the code alone

The v1→v2 change of `examples/kv` (v1: every key a value; v2: a key is a
value **or** a list), classified against the taxonomy:

| hunk | taxonomy row | obligations |
|---|---|---|
| `OP_APPEND = 4`, `QOP_LIST = 3` added | New command variant (appended) | axis-P **severe** (a v1 replica cannot apply an `append`); axis-H permanent until S9; not downgrade-safe; owes a forward-decode shim that v1 never had — so v1 refuses it by name (`BAD_UNKNOWN_OP`) instead of misparsing |
| image version 1 → 2, a shape byte per entry | Snapshot image format | new binary must read the old image — the S3 dual-read shim, `IMAGE_VERSION_V1`/`V2` in `install_snapshot`; **not downgrade-safe**: a v1 binary fail-stops on a v2 image |
| `Put`/`Cas` answer `ST_WRONG_SHAPE` when the key holds a list | Changed `apply` semantics of an existing command | axis-P severe in principle; safe **only** under S4's pinned origin. Note the arm is unreachable over a v1-written image, because only `append` can create a list |
| `Bytes` → `enum Shape { Value, List }` | In-memory state shape | not replicated directly; reaches the snapshot image only — which is the row above |

No hunk matches "inserted mid-enum" or "fields reordered": `KvSm` is a
**raw**-tier state machine whose wire carries an explicit `FORMAT_VERSION ‖ op`
pair and whose `decode_command` refuses trailing bytes (`BAD_TRAILING`) and
unknown ops (`BAD_UNKNOWN_OP`), so the measured misparse shapes — which are
the typed (bincode) tier's, and whose interim length check is filed as #49 and
not shipped — do not reach it. Digit: the severe axis-P rows make this a
**major** bump, which is what `KV_VERSION = 2.0.0` is.

### Step 3 — attribute, on a real report

The only non-`Pass` finding this tree can produce is the `Absent` above, and
step 3's residue is `Unexplained`, which needs two builds. What the step was
exercised on is therefore the finding vocabulary itself, from
`uc_diffreplay/src/confirm.rs`: the two `Unexplained` notes are `no touched
arm explains this` and `position dispatched by one build only (only_in_a)` /
`(only_in_b)`; `Undeclared` reads `observed and attributed, but not
declared`; `Absent` reads `declared but not observed: <the note>` — confirmed
verbatim in the run above. Worth noting for a real v1→v2 run on this corpus:
the shipped declaration has `[touched] arms = []`, so **every** divergence
would arrive `Unexplained` by construction. Drafting the touched set (step 1)
is not paperwork — it is what makes attribution mean anything.

### Step 4 — judge the migration, on the code alone

No two-build run exists here, so there is no `projection_origin` diff to read;
what a v1→v2 run would print is derivable from the reader and the projection,
and is recorded here as the sentence step 4 demands rather than as a report
line. `install_snapshot` takes the v1 branch (`image_version ==
IMAGE_VERSION_V1`) and supplies `shape_byte = SHAPE_VALUE` for every entry,
reading no shape byte from the stream; `project()` renders each entry as
`key=… version=… shape=value bytes=…`. So the sentence is: **the migration is
exactly "every entry gains `shape=value`; no key, version, digest or cursor
moves"** — and since v1's own projection would print the same `key`/`version`
fields, the delta a real run should show on `projection_origin` is *empty* on
those fields, with `shape=` the only text v1 could not have produced. Any
other line — a changed `digest=`, a moved `cursor=`, a key gone — would be a
defect in the reader, not in the harness, and is what the step would name.

For reference, the corpus's own projections (from the trace above, both
sides of a same-build run, hence identical):

```
count=2                                 count=1
cursor=96                               cursor=192
digest=0xe8da1d782e66c2de               digest=0x1505b09d0eb484a3
key=61 version=32 shape=value bytes=31  key=62 version=96 shape=value bytes=32
key=62 version=96 shape=value bytes=32  session client=1 seq=Some(2)
session client=1 seq=Some(1)
         (at the origin, 192)                      (at the end)
```

### Step 5 — hazards, on the code alone

Read: `examples/kv/src/lib.rs`, `examples/kv/src/wire.rs`,
`examples/kv/src/bin/kv-service.rs`.

- id minting: **no call of either kind on either side** — `git grep -n
  'ctx\.ids()\|\.ids()\|\.next()' HEAD -- examples/kv/src` exits 1 (no
  match), so `KvSm::apply` takes no generator and mints no id: neither the
  duplicated-series shape nor the shifted-series one can arise, and
  `ids_calls` is `0` in the trace above.
- `HashMap`/`HashSet`: **none in code**. `git grep -n 'HashMap\|HashSet' --
  examples/kv` returns three lines, none of them code: two state the rule
  (`examples/kv/src/lib.rs:16`, `examples/kv/docs/DESIGN.md:15`) and one is
  prose about an OrdMap-vs-HashMap benchmark
  (`examples/kv/docs/DESIGN.md:53`). The state is `Arc<BTreeMap<Bytes,
  Entry>>` and lists are `Vec<Bytes>`, so `project()` is canonical for free.
- floats: none.
- ambient nondeterminism in `apply`: none — no clock, no RNG, no I/O.
- Appendix A shapes: none reachable; see step 2.

Result: **none found**, over those three files.
