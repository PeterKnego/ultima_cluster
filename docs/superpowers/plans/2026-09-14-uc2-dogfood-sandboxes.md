# Dogfood clean-room sandboxes — how they are built and how a run is started

**Ticket:** "Assemble the clean-room sandboxes and the read audit" (map #16).
**Built:** 2026-09-14. Vocabulary: root `CONTEXT.md` § Dogfooding.

## What exists

Two sandbox directories, identical in the material they hold and different
only in persona:

| | builder | operator |
|---|---|---|
| directory | `~/ultima/kv_store/` | `~/ultima/kv-ops/` |
| persona rules | `CLAUDE.md` (builder) | `CLAUDE.md` (operator) |
| task input, handed over later | `BRIEF.md` (ticket "Write the builder's brief") | `CARD.md` + `HOSTS.md` per scenario (ticket "Design the operator scenario cards") |
| `docs-snapshot/` | the published-docs tier at `main` commit `1b47f4e`, 92 files | same |
| `release/` | the signed 2.12.0 x86_64 tarball, `sha256sum -c` OK and `cosign verify-blob` Verified OK in place, unpacked | same |
| `rustdoc/` | `cargo doc --no-deps` of the 12 published library crates from crates.io (`uc_ctl` is binary-only and has none) | same |
| `LEDGER.md` | empty template | same |
| `.claude/settings.json` | Read-tool deny rules + WebFetch/WebSearch denied | same |

**The published-docs tier, as copied** (by `git archive` at the recorded
commit, so uncommitted files never leak): `README.md`, `RELEASES.md`,
`SECURITY.md`, `LICENSE*`, `docs/QUICKSTART.md`, `docs/ARCHITECTURE.md`,
`docs/how-to/`, `docs/reference/`, `docs/ops/`, `docs/notes/`,
`docs/security/`, `docs/images/`, `packaging/`, `examples/counter/`.
`docs/ARCHITECTURE.md` and `docs/images/` are two judgement calls beyond the
glossary's list — README points a stranger at the first, and the docs
embed the second — and the glossary should be read as including them.
Excluded, and checked absent: `docs/superpowers`, `docs/benchmarks`,
`docs/releases.md`, `docs/VERIFICATION.md`, `docs/BENCHMARKS.md`,
`docs/BACKLOG.md`, `docs/agents`, `docs/tasks`, `CLAUDE.md`, every crate,
`examples/uc_crashtest`, `fuzz/`, `proofs/`, `bench-infra/`, `scripts/`.

Links from the snapshot into the excluded tier are dead **on purpose**; a
persona hitting one logs it, and the ledger then says which internal pages
the public docs lean on.

**Where the rustdoc came from.** A throwaway crate under
`~/scratch/rustdoc-2.12.0/` depending on the 13 crates at `=2.12.0`, built
with `CARGO_HOME=~/scratch/cargo-home-docbuild`, so the crates' source
that cargo downloaded to build the docs sits under `~/scratch/`, never
under either sandbox. Only `target/doc/` was copied in.

## The three walls, and what each actually stops

1. **`CLAUDE.md`** states the rule and the reason. It stops an agent that
   reads it and means to comply — which is the normal case.
2. **`.claude/settings.json` deny rules** stop the **Read tool** from
   opening the UC repos, the other sandbox, `~/scratch`,
   `~/.cargo/registry` and `~/.cargo/git`, and this machine's Claude
   project transcripts, plugins and skills; `WebFetch`/`WebSearch` are
   denied outright. They do not stop `Bash` — `cat`, `sed`, `python` can
   read anything — and a rule that tried to enumerate every Bash reader
   would be theatre.
3. **`scripts/dogfood_audit.py`** (in this repo, never in a sandbox) reads
   the session's transcripts afterwards — every `Read`/`Glob`/`Grep`/
   `Edit`/`Write` path and every path-like token in every `Bash` command,
   including subagents' transcripts — and classifies each path INSIDE,
   FORBIDDEN, BENIGN (toolchain/OS) or OUTSIDE. One FORBIDDEN read voids
   the run; OUTSIDE reads are listed for the maintainer to judge. This is
   the wall for Bash.

The crate source that cargo downloads into `~/.cargo/registry/src` when the
builder compiles its app is the one thing on disk the persona must not
read that no wall can remove: walls 2 and 3 both name it.

## Starting a persona run (the maintainer's step)

```sh
# builder: after BRIEF.md has been placed in the directory
cd ~/ultima/kv_store && claude
# operator: after CARD.md and HOSTS.md have been placed
cd ~/ultima/kv-ops && claude
```

Start it from a **fresh terminal**, not from inside a session that has
`ultima_cluster` open. Claude Code keys project memory and transcripts on
the working directory, so each sandbox gets its own empty memory and its
own transcript directory (`~/.claude/projects/-home-claude-ultima-kv_store/`
and `…-kv-ops/`). There is no user-level `~/.claude/CLAUDE.md` on this
machine, and the user-level `settings.json` carries no UC knowledge
(checked 2026-09-14). The five user-level skills (`pinchtab`, `quint-*`,
`ste`) are visible to the persona and are unrelated to UC.

Model choice is the maintainer's; the persona's `CLAUDE.md` does not
depend on it.

## Auditing a run (the maintainer's step, always, before reading the ledger)

```sh
scripts/dogfood_audit.py --sandbox ~/ultima/kv_store      # exit 0 CLEAN, 1 VOID/JUDGE, 2 no transcripts
scripts/dogfood_audit.py --sandbox ~/ultima/kv-ops --json    # machine-readable
```

The verdict goes into the run ticket's resolution comment verbatim.

## Verification of the audit (2026-09-14)

Two throwaway sandboxes (`~/ultima/kv-audit-leak`, `~/ultima/kv-audit-clean`,
each a copy of the builder `CLAUDE.md` + deny rules + one doc file) were
driven with `claude -p --model haiku --allowedTools Bash Read`:

- the **leak** session was told to Read `ultima_cluster/CLAUDE.md`, Read a
  sandbox file, `head` a repo file through Bash, and `ls` the cargo
  registry through Bash;
- the **clean** session was told to Read a sandbox file and run
  `ls docs-snapshot && cat /etc/os-release`.

Results are recorded in the ticket's resolution comment (what the deny
rule blocked, what the audit caught, both verdicts). Both throwaway
directories and their transcript directories were deleted afterwards.
