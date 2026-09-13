# ultima_cluster

The vocabulary of UC, a State Machine Replication application server, as
used across its docs, code and planning. The consensus and transport terms
are defined where the design spec defines them; this glossary holds the
terms that are easy to say loosely and expensive to mean loosely.

## Language

### Dogfooding

**Published docs**:
What a stranger who found UC on crates.io or the release page is pointed
to: the user-facing tier (README, QUICKSTART, RELEASES,
SECURITY, `docs/how-to`, `docs/reference`, `docs/ops`, `docs/notes`,
`docs/security`), everything under `packaging/`, the rustdoc of the
published crates, and the `examples/counter` source. Not the engineering
record (`docs/superpowers`, `docs/benchmarks`, `docs/releases.md`,
`docs/VERIFICATION.md`, `docs/BACKLOG.md`, `docs/agents`, `CLAUDE.md`), not
crate source, not test apparatus such as `examples/uc_crashtest`. For
a clean-room run it is a snapshot: the docs from `main` at the commit the
sandbox was assembled, the binaries and crates from the release under test.
_Avoid_: the docs, everything in docs/, the repo

**Builder**:
The persona that writes an application on UC from the published docs alone,
never from source, for the purpose of judging the developer experience.
_Avoid_: developer (ambiguous with UC's own maintainers), author

**Operator**:
The persona that provisions, runs, monitors, diagnoses and upgrades a UC
cluster from the published docs and the release tarball alone, for the
purpose of judging the operator experience.
_Avoid_: admin, SRE, user

**Clean-room**:
The discipline under which a builder or operator works: a sandbox holding
only the published docs, the release, and rustdoc, with the transcript
audited for reads outside it. A read outside the sandbox voids the finding.
_Avoid_: blind, black-box, from scratch

**Friction ledger**:
The running record a builder or operator keeps of every gap, wart and
surprise met, each with the assumption they proceeded on. Nothing in it
stops the work; a wrong assumption that later fails a check is a blocking
defect.
_Avoid_: issues list, bug list, notes, DEFECTS.md (a filename, not the concept)

**Experience report**:
The per-persona deliverable distilled from a friction ledger: what the
docs and product did and did not let the persona do, ranked, with each
item resolved into a doc fix, an API fix, or an accepted limit.
_Avoid_: retrospective, postmortem, review

**Docs sufficiency**:
The bar the dogfood is judged against: the persona reaches the goal with
zero reads outside the clean-room, and every friction-ledger item is
resolved. Correctness capstones must pass; performance is reported, never
barred.
_Avoid_: usability, DX, quality
