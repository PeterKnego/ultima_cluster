# Diátaxis documentation plan — ultima_cluster

Durable record for the `diataxis-docs` skill. Lives at the repo root,
deliberately outside `docs/`, so no site generator renders it.

**Run 1 — 2026-08-06. Complete.** Applied annotations and the parking lot have
been trimmed; what remains is what a future run needs.

## Output format and tooling

- **Markdown under `docs/`.** No generator config exists (no mkdocs, sphinx,
  docusaurus, mdbook).
- **Autodoc: rustdoc.** `.github/workflows/docs.yml` runs
  `cargo doc --workspace --no-deps --lib` and publishes to GitHub Pages on every
  push to `main`. **Never write hand-maintained API reference** — improve
  docstrings instead. This is the standing constraint on the reference quadrant.

## Audiences and goals (user-supplied, do not re-ask)

**Audiences: all four, unranked** — operators running a cluster, Rust
developers embedding the SDK, contributors to the internals, and evaluators
reading rather than running. Every quadrant has a reader; none may be dropped
for want of one.

**Approved how-to scope: all four goal clusters** — operate, secure, diagnose,
reproduce.

**Restructure appetite: moderate.** Split the runbook; every other existing
document keeps its path. **Naming: keep existing names**, add only what is
missing. `docs/notes/` is *not* renamed to `explanation/`.

## Approved reference scope

Non-API only, for the autodoc reason above: the CLI, the on-disk layout, the
cnc page, configuration and environment switches, the wire protocol, and the
read path.

## The documentation set

### Tutorial — `docs/QUICKSTART.md`

| Need | Source |
|---|---|
| Learn the system by getting a real cluster running | `examples/counter`, executed |

### How-to — `docs/how-to/`

| Guide | Need | Source |
|---|---|---|
| `run-a-cluster.md` | get nodes onto real machines | runbook §2, §4 |
| `change-cluster-membership.md` | grow, shrink, replace hardware, retire a leader | runbook §6; `uc2ctl`; `reconfig.rs` |
| `encrypt-node-traffic.md` | encrypt node traffic | runbook §11; `crypto_cluster.rs` |
| `bound-journal-growth.md` | stop the disk filling | runbook §5; `purge_safety.rs` |
| `diagnose-a-node.md` | find why clients are failing | runbook §3 |
| `investigate-a-failed-run.md` | act on a red correctness run | runbook §9, §10 |
| `reproduce-a-result.md` | check a published claim on your own hardware | runbook §8; `BENCHMARKS.md`; `VERIFICATION.md` |

### Reference — `docs/reference/`

| Page | Need | Source |
|---|---|---|
| `uc2ctl.md` | look up a sub-command or a refusal code | `uc2ctl/src/main.rs` |
| `instance-directory.md` | know what each file is and what must be durable | runbook §1 |
| `cnc-page.md` | decode the control page | `uc_protocol::v2::cnc` |
| `configuration.md` | look up a knob, default, or limit | `NodeConfig`; env switches |
| `wire-protocol.md` | read the wire | `uc_protocol::v2` |
| `read-path.md` | know how a read is certified | runbook §7 |

### Explanation — `docs/ARCHITECTURE.md`, `docs/notes/*`

Pre-existing and kept. No new documents: the quadrant was already served, and
adding pages for their own sake is the empty-scaffold failure.

## Not created

None. All four quadrants had genuine material.

## Tutorial verification status

| Step | Status |
|---|---|
| §1 `cargo run -p counter --bin counter-single` | **VERIFIED 2026-08-06** by execution in a disposable worktree at `badd703`; output matches, modulo the per-run temp path. |
| §3–5 three-node cluster | **UNVERIFIED.** Seven processes across five terminals, not run on a shared machine. Discharge by running it on a quiet box and diffing captured output. |

## Deviations from the approved plan, with reasons

**Three how-to guides renamed.** The plan named two for tool operations, which
the title test rejects — a guide answers to a human project, not to taking the
machinery through its motions.

| Approved | Written | Why |
|---|---|---|
| `enable-purge.md` | `bound-journal-growth.md` | nobody's goal is "enable purge"; it is to stop the disk filling |
| `enable-wire-crypto.md` | `encrypt-node-traffic.md` | `CryptoConfig::Enabled` is the how, not the why |
| `reproduce-a-gate.md` | `reproduce-a-result.md` | "gate" is this project's internal word |

**One sentence added to `ARCHITECTURE.md`** (§"four single-writer polling
agents"), explaining why cnc counters sit on 64-byte strides. It was trimmed out
of `cnc-page.md` by the reference sheet's no-explanation rule, and the guards
require misplaced content to be moved rather than deleted. The other two items
trimmed from reference were already covered in `ARCHITECTURE.md` (the 8-member
cap; vote durability).

## Known gaps for a future run

1. **`QUICKSTART.md` opening form and person.** The tutorial sheet wants "In
   this tutorial we will …" and first-person plural throughout. The document
   opens differently and mostly uses "you". Both are whole-document rewrites of
   a page the approved plan kept in place, and neither is a quadrant blur —
   deferred deliberately, not overlooked.
2. **`CLAUDE.md` describes the runbook** as holding instance-dir layout, cnc
   decode, purge enablement and reconfiguration. The path is still valid, but
   the file is now a landing page and that description is stale. Left alone
   because `CLAUDE.md` is the maintainer's instruction file, not documentation.
3. **`VERIFICATION.md` §"Reproducing everything"** is how-to content inside an
   explanation document. Cross-linked from `reproduce-a-result.md` rather than
   moved, because moving it would gut a document whose value is being one
   continuous argument. Revisit if it grows.
