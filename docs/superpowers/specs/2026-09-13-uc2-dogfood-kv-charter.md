# Dogfood KV store — charter (wayfinder charting session, 2026-09-13)

**Status:** CHARTED. The effort is run as a wayfinder map on this repo's
issue tracker (label `wayfinder:map`); this page is the record of the
charting session's decisions, which the map's Notes point at rather than
restate. Vocabulary: root `CONTEXT.md` § Dogfooding. Predecessor: the
parked brief `2026-09-01-uc2-dogfood-kv-and-go-client-brief.md`, whose
eight decisions stand except where a row below overrides one.

## Destination

A merged `examples/kv` service in two versions, two experience reports
(builder, operator), and a documented application lifecycle — a
`docs/tutorials/build-an-application.md` walking design → build → test →
package → deploy → operate → upgrade with the KV store as the worked
example, plus a how-to for upgrading an application — with every
friction-ledger item landed on `main` as a doc fix, an API fix, an accepted
limit, or a product ticket.

## Decisions taken (maintainer, 2026-09-13)

| # | Question | Decision |
|---|---|---|
| 1 | Destination artifact | The KV store is kept and merged, not thrown away; the reports and the lifecycle doc are the point, the fixes their proof. |
| 2 | Who builds | A **clean-room builder**: published docs, rustdoc and the 2.12.0 release only, never source. The maintainer reviews the ledger, does not write the code. |
| 3 | Who operates | A **clean-room operator**, same discipline, separate ledger and report. |
| 4 | UC under test | **Frozen at the 2.12.0 release** (tarball + crates.io). Fixes land on `main` for 2.13 and reach the personas only through a deliberate upgrade step. Docs snapshot: `main` at the commit the sandbox is assembled. |
| 5 | Bar | **Docs sufficiency**, extended to the operator track: zero reads outside the clean-room, every ledger item resolved; correctness capstones must pass; performance reported, never barred. Honest-failure protocol. |
| 6 | Hosts | Terraform supplies **bare hosts**; everything after the first ssh is the operator's, from the docs and the tarball. |
| 7 | "Published docs" | The user-facing tier: README, QUICKSTART, RELEASES, SECURITY, `docs/how-to`, `docs/reference`, `docs/ops`, `docs/notes`, `docs/security`, `packaging/`, rustdoc of the 13 crates, `examples/counter` source. Not the engineering record, not crate source, not `examples/uc_crashtest`, not `~/.cargo/registry/src`. |
| 8 | Who drives the clean-room session | A **separate Claude Code session the maintainer starts in a separate directory** (`~/ultima/kv_store`, own `CLAUDE.md`, own memory, no path to this repo). Its transcript under `~/.claude/projects/` is what gets audited. Same for the operator. |
| 9 | Requirements or design | **Requirements only**: sessioned KV; Put, Get, Delete, CAS, Append; opaque values; no TTL/range; must fit the cluster's command ceiling; must survive purge. The builder designs the encoding and derives caps from the docs. The brief's wire format is withdrawn (it was shaped for a Go reader). |
| 10 | v1 → v2 | **v1 = Put/Get/Delete/CAS; v2 adds Append and the list kind**, so apply semantics and the snapshot payload change and `VERSION` must move. |
| 11 | Adjudication | **Split**: the builder writes the tests the docs tell it to; the maintainer side runs the repo's capstones (per-key WGL through `uc_remote` under leader kills, Elle list-append through the remote path, snapshot/purge churn) against the builder's binary as a black box. `uc_lincheck` stays unpublished for this run. |
| 12 | Client | A **`kv` CLI over `uc_remote`** required; shmem optional. |
| 13 | Fleet | **3 × `c6i.2xlarge` voters + 1 × `c6i.large` observer** (client, Prometheus, Grafana), on-demand, one AZ, us-east-1, destroyed after each operator session. Every `terraform apply` is maintainer-approved. |
| 14 | Operator scenarios | Provision + deploy 3 nodes from the tarball; deploy the KV service and prove a write through the gateway; stand up monitoring and confirm the alert rules load; diagnose injected faults; membership change (add learner, promote, remove voter); backup / verify / restore; **flag-day application upgrade v1 → v2**. Platform upgrade to 2.13.0 and rolling application upgrade: fog. |
| 15 | Faults | **Maintainer side injects, blind to the operator**: node SIGKILL, full disk, partition, killed service, stopped gateway; one per exercise. |
| 16 | "SDLC" deliverable | The term is **application lifecycle**. Deliverable: `docs/tutorials/build-an-application.md` (the empty Diátaxis tutorial slot) + a how-to for upgrading an application; bars and results in `docs/benchmarks/uc2-dogfood-kv-gate-<date>.md`; the two reports under `docs/notes/`. |
| 17 | Out of scope | Go client and conformance harness; a second service; multi-FSM deployment; range/prefix/TTL; any performance bar; remote protocol v2 (FSM selector + ceiling advertisement) — exposed, not fixed. |
| 18 | The SDLC standard vs the product | `docs/reference/application-sdlc.md` (maintainer-authored, 2026-09-13) is published as the standard applications are held to. Its §5 (canary, mixed-version, rolling order) describes a **target the product does not yet meet**: `VERSION` is equality-only on the snapshot path, an app upgrade is a flag day today. §5 carries a status note; the rolling scenario waits for the release that ships it (`docs/BACKLOG.md` item 3). §2–3's tooling (determinism lint, golden replay, cross-replica state hash) is likewise absent and becomes product work as the ledgers name it. |

## Routine calls made without asking (object on the map if wrong)

- The KV store is built outside the tree against the crates.io 2.12.0
  crates and moved in with path deps afterwards, as the brief planned.
- The builder is not time-boxed: it stops when it declares done or hits a
  block with no assumption to proceed on, and each such block is a decision
  for the maintainer.
- UC fixes from the ledgers land on `main` as found; the freeze keeps them
  invisible to the personas.
- A run ticket (a builder or operator session) may span more than one
  agent session; its claim persists until it resolves.

## Facts gathered while charting (do not re-derive)

From a survey of the tree at `6f77139`:

- `docs/tutorials/` is **absent**; `docs/QUICKSTART.md` is the entry point
  and routes to `write-a-service-binary.md` → `state-machine-contract.md` →
  `remote-protocol.md`. No page owns the end-to-end application lifecycle.
- `examples/counter` is **typed tier, no `SnapshotStateMachine`, no
  `Sessioned`**, so the shipped example covers neither purge nor
  exactly-once. The only in-tree example of sessions + snapshots + timers
  together is `examples/uc_crashtest`'s service binary, which is test
  apparatus and out of the published tier.
- A service's `const VERSION` is written to the cnc slot and compared for
  equality on the snapshot path (`uc_net/src/receiver.rs`, refusal
  `snap_refused_version_mismatch`); nothing on the live commit path checks
  it. `docs/how-to/upgrade-a-cluster.md` covers UC's own binary only; **an
  application-upgrade procedure is absent**, and the refusal table's remedy
  ("attach the same build everywhere") is the implicit flag day.
  `packaging/systemd/uc2-service@.service` `BindsTo=` the node unit, so a
  service restart is coupled to the node.
- The tarball does not ship the user's service binary
  (`run-a-cluster.md`: "you build and ship it yourself"). The only
  release-binary deploy path is `bench-infra/scripts/fleet_quickstart.sh`
  (installs the tarball + the `packaging/` systemd units onto bare hosts;
  its default `VER` is stale at 2.10.0). `bench-infra`'s ansible builds
  from a synced source tree and installs no release binaries and no
  systemd units.
- Monitoring: 26 alert rules and one 15-panel dashboard ship in
  `packaging/`; nothing documents standing up Prometheus/Grafana.
  `docs/how-to/diagnose-a-node.md` is the only troubleshooting page.
- `uc_lincheck` is `publish = false`; the builder cannot self-adjudicate
  linearizability.
- `remote-protocol.md` v1 advertises no payload ceiling, so a `uc_remote`
  client cannot learn the cluster's discovered rung.
