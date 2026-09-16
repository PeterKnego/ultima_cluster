# uc2 dogfood KV gate — PRE-COMMITMENT, no run yet

**Date:** 2026-09-15 (bars committed). **Runs: NONE YET.** This is the gate
for wayfinder map #16, "Dogfood KV store: a two-track experience
assessment of building on and operating UC" — charter
[`docs/superpowers/specs/2026-09-13-uc2-dogfood-kv-charter.md`](../superpowers/specs/2026-09-13-uc2-dogfood-kv-charter.md),
vocabulary root `CONTEXT.md` § Dogfooding. The bars below were grilled
with the maintainer on ticket #19 ("Pre-commit the dogfood gate document
and its bars"), 2026-09-14/15; the ticket's resolution comment is the
decision record, this page is the pre-commitment.

> **Decide rule committed before any run.** This document's bar table is
> committed, with every result cell **UNRUN**, before any builder session,
> operator session or adjudication run against it — the honest-failure
> protocol carried forward from M7/M9/M10/M11/M12/M13/M14/M14c2 and every
> 2.11.0/2.12.0 gate. Nothing in a bar may be edited to match a result: a
> run that misses a bar is recorded as a **FAIL** and the bar is **kept,
> unmoved**. This document itself is a PRE-COMMITMENT, not a record — its
> own commit message says so — and must not be read as "gated" until the
> result cells are filled.

## What this gate measures

Not UC's speed. **Whether the published docs suffice** for a stranger to
build an application on UC and for a stranger to operate a cluster running
it, judged by a builder and an operator who work **clean-room** (published
docs + the `2.12.0` release + rustdoc, never source, transcripts audited),
plus whether what they built is **correct** by the repo's own checkers,
run maintainer-side against the builder's binary as a black box.

UC is **frozen at `2.12.0`** for both personas. Fixes found here land on
`main` for `2.13` and never reach a persona mid-run.

## Conventions that bind every row

These were decided on ticket #19 and apply wherever a row is silent.

1. **Audit verdicts.** `scripts/dogfood_audit.py` returns one of CLEAN,
   JUDGE, BREACH, VOID. **CLEAN passes.** **JUDGE passes** if the
   maintainer rules every OUTSIDE read benign, and the ruling is recorded
   in the result cell path by path. **BREACH passes** on sufficiency (a
   forbidden read was attempted and denied; the persona saw nothing) and
   the attempt is recorded in the cell as a discipline breach. **VOID**
   (a forbidden read succeeded) makes the row **VOID**, not FAIL: a
   voided run is not a measurement, and it is re-run from a fresh
   sandbox. This is the `CONTEXT.md` *Clean-room* rule as corrected
   2026-09-15. A VOID on a builder run voids that run's B1 row only;
   B2/B3/B4 cells already measured against its binary stay as
   measurements of that binary, are marked "from a voided run", and are
   re-taken against the re-run's binary before they count.
2. **A maintainer intervention is a docs-sufficiency FAIL.** The charter
   lets a persona stop at a block with no assumption to proceed on and
   hand the question to the maintainer. Each such answered block **fails
   the B1 (or B1') row for that run**, and the cell records the question
   and the answer verbatim. The run **continues** after the answer, so
   the rows downstream (B2–B4) are still measured; "the docs fell short
   here" and "the thing built is wrong" are separate findings.
3. **A friction-ledger assumption that later fails a capstone is a
   blocking doc defect.** The adjudication — ticket #24 for v1, and for
   v2 the same pass with Elle added, which ticket #25's resolution names —
   traces every B2 failure to either a builder assumption (→ fails B1 for that run too) or a product defect
   (→ fails B2 only, becomes a repo issue). The two rows may disagree;
   that is the point of separating them. Because clause (iv) of a B1 row
   depends on this tracing, a B1 cell is finalised only after its run's
   adjudication, not at the end of the run.
4. **Correctness rows have four outcomes only:** PASS / FAIL / NOT RUN /
   VOID. There is **no "inconclusive"** on a correctness row; that word is
   reserved for rate rows, and this gate bars no rate. A multi-seed row
   rolls up as: any seed FAIL → FAIL; else any seed NOT RUN → NOT RUN
   (re-run the seeds that did not measure); else PASS.
5. **Rep counts are fixed here, not on the day:** 5 seeds for each WGL
   clause, 5 Elle passes, matching the hard-crash test and
   `scripts/elle_check.sh` respectively.
6. **This file is never renamed.** The runs span weeks and several fleet
   sessions, unlike a single-day fleet gate, so the name keeps the
   pre-commitment date and **every result cell carries its own run
   date**. Rows may be **appended** later with their own pre-commit date
   (the platform-upgrade scenario, if `2.13.0` lands mid-experiment, is
   the expected case); an existing row's bar is never edited.
7. **Where things run.** B2 runs on the maintainer's dev box as a
   multi-process black-box test (the `remote_lin` shape: real node,
   gateway and service binaries spawned, the service being the builder's).
   B3 runs on the **operator's fleet, after the operator's session on it
   has ended** and the ledger is handed back — never blind during a
   scenario — with the register-SM arm deployed onto the same hosts for a
   paired number. Every `terraform apply` is maintainer-approved; the
   fleet is destroyed and leak-checked after each session.
8. **Operator card text is pasted into the row at issue time.** Ticket
   #21 writes the cards; each card's success criterion is copied into its
   B1' sub-row **before** that card is issued, so the bar the operator is
   held to is the text they received. Copying a card in is an append under
   rule 6, not an edit.

## The bar

Pre-committed 2026-09-15. Cells read **UNRUN** until filled.

### B1 — docs sufficiency, builder (one row per run)

| row | run | bar | result |
|---|---|---|---|
| B1-v1 | Builder builds KV v1 (Put/Get/Delete/CAS, sessioned, opaque values, must survive purge, must fit the ceiling) from `~/ultima/kv_store` | (i) the builder declares done; (ii) `scripts/dogfood_audit.py --sandbox ~/ultima/kv_store` passes under convention 1; (iii) **zero maintainer interventions** (convention 2); (iv) no ledger assumption is traced to a B2-v1 failure (convention 3). Ledger item count **reported, not barred** | **PASS** (2026-09-16). (i) builder declared done (#23); (ii) audit exits VOID on ONE hit — `df -T /home/claude/scratch`, a filesystem-metadata stat that reads no file content — JUDGED benign (no forbidden content seen), run stands; the OUTSIDE reads are all the builder's own cluster state (`~/uc2-kv`), its own build artifacts (`~/.cache/cargo-target`) and OS/toolchain; the script over-classifies pure-stat commands (known tooling limitation); (iii) zero interventions (#23); (iv) the one B2-v1 failure (B2-v1.iii) traces to product defect #32, NOT a builder assumption. 16 ledger items reported (#23) |
| B1-v2 | Builder extends to KV v2 (Append, the list kind, a `VERSION` bump) | same four clauses against the v2 run's transcript and B2-v2 | **PASS** (2026-09-16). (i) builder declared done (`~/ultima/kv_store/DONE-v2.md`); (ii) audit is the same single benign hit as B1-v1 (`df -T /home/claude/scratch`, metadata; the v2 session shares the transcript and added **no** new forbidden read), judged benign; (iii) zero interventions (the ledger logs gaps L17–L22 as assumptions, none handed back); (iv) the one B2-v2 failure (B2-v2.iii) traces to product defect #32, NOT a builder assumption. **6 new ledger items (L17–L22)** reported — versioning, snapshot-format change, the flag-day upgrade, and L22 (a mixed-version commit can make an acknowledged write invisible). Adjudicator's note, not a B1 failure: the builder's own `WIRE-FORMAT.md` §3.1 omits the APPEND command row (op 4) |

### B1' — docs sufficiency, operator (one sub-row per scenario card)

Each sub-row's bar is: the card's success criterion (pasted in under
convention 8) met **from the published docs and the tarball alone**, the
audit of `~/ultima/kv-ops` passing under convention 1, zero maintainer
interventions under convention 2. An ops step that cannot be completed
from the docs is a blocking defect and fails the sub-row. **Any sub-row
FAIL fails B1'.** The seven scenarios are the charter's (decision 14);
their titles here are placeholders for the card titles #21 issues.

| row | scenario (charter 14) | card text (pasted at issue) | result |
|---|---|---|---|
| B1'-1 | provision + deploy 3 nodes from the tarball | **Card 1 — Bring up the cluster.** Goal: from `release/`, stand up a three-node cluster across the hosts in `HOSTS.md`. Success: the three nodes form one cluster and one reports as the serving leader | UNRUN |
| B1'-2 | deploy the KV service; prove a write through the gateway | **Card 2 — Prove it serves.** Goal: deploy the `kv` service from `app/` to every node and make the store reachable to a remote client. Success: a value written through one gateway reads back unchanged through a *different* gateway | UNRUN |
| B1'-3 | stand up monitoring; confirm the alert rules load | **Card 3 — See the cluster.** Goal: stand up Prometheus and Grafana on the observer host and point them at the cluster. Success: every shipped alert rule loads and evaluates healthy in Prometheus, and the shipped dashboard renders the cluster's live metrics | UNRUN |
| B1'-4 | diagnose injected faults (one per exercise; maintainer-injected, blind) | _(not yet issued)_ | UNRUN |
| B1'-5 | membership change: add learner, promote, remove voter | _(not yet issued)_ | UNRUN |
| B1'-6 | backup / verify / restore | _(not yet issued)_ | UNRUN |
| B1'-7 | flag-day application upgrade v1 → v2 | _(not yet issued)_ | UNRUN |

### B2 — correctness (maintainer-side, black box)

Run by the adjudication harness (ticket #20) against the builder's service
binary through `uc_remote`, with the builder's wire format plugged in as an
encoding adapter. Outcomes per convention 4; reps per convention 5. The
harness is `examples/uc_adjudicate` (`uc2-adjudicate wgl`/`elle`/`diverge`/
`diff-snapshots`/`known-keys`/`rate`); its README states the adapter contract
and the two findings its development runs produced. These result cells stay
UNRUN until the #24 adjudication fills them; harness-development smoke runs are
not gate runs.

| row | clause | bar | result |
|---|---|---|---|
| B2-v1.i | per-key WGL linearizability through `uc_remote` under leader kills | **Linearizable** on every key, all 5 seeds | **PASS** (2026-09-16). 5/5 seeds, every key Linearizable, 27 leader kills total; `uc2-adjudicate wgl --adapter kv-v1` on the 2.12.0 tarball binaries against the builder's `kv-service` (app HEAD `9e82e84`) |
| B2-v1.ii | acked-write loss | **0** acknowledged writes lost, all 5 seeds | **PASS** (2026-09-16). 0 acknowledged writes lost across all 10 runs (5 wgl + 5 churn) |
| B2-v1.iii | snapshot + purge churn | **Linearizable**, all 5 seeds, **and** ≥ 1 snapshot install observed on a restarted or joining service per seed — a seed with no install is **NOT RUN**, not PASS | **FAIL** (2026-09-16), traced to **product defect #32** (not a builder assumption). 5/5 seeds: per-key **Linearizable** and ≥1 snapshot install observed on every seed (6–9 service installs), but a UC node **fail-stops** (`IngressRingCorrupt`) under the coordinated instant + ingress load, so the run is not a clean PASS. The KV is correct; the platform is not. Honest FAIL, bar unmoved; re-runnable at the baseline rung on a fleet to disambiguate the jumbo correlation (#32) |
| B2-v2.i | per-key WGL under leader kills, v2 binary | as B2-v1.i | **PASS** (2026-09-16). 5/5 seeds, every key Linearizable under leader kills (27 kills); v2 binary via the `kv-v2` adapter |
| B2-v2.ii | acked-write loss, v2 binary | as B2-v1.ii | **PASS** (2026-09-16). 0 acknowledged writes lost, all 5 seeds |
| B2-v2.iii | snapshot + purge churn, v2 binary | as B2-v1.iii | **FAIL** (2026-09-16), traced to **product defect #32** (same as B2-v1.iii, not the builder). 5/5 seeds per-key Linearizable with ≥1 install observed, but a UC node fail-stops (`IngressRingCorrupt`) under the coordinated instant + ingress load. Honest FAIL, bar unmoved |
| B2-v2.iv | Elle list-append over `Append`, driven through the remote path | **clean under both `serializable` and `strong-serializable`**, all 5 passes (what `scripts/elle_check.sh` checks) | **PASS** (2026-09-16). Elle list-append through `uc_remote`, 5/5 passes **clean under both `serializable` and `strong-serializable`** (the vendored elle-cli via `scripts/dogfood_elle.sh`); ~3000 `:ok` ops per history, 6 leader kills each — non-vacuous |

### B3 — performance (reported, no bar)

| row | measure | bar | result |
|---|---|---|---|
| B3-v1 | KV v1 throughput and p50/p99 latency through the builder's `kv` CLI over `uc_remote` on the operator's fleet after the provisioning session (#26), **paired** with a register-SM arm through the harness on the same hosts, same day | **reported, no bar.** The cell states whether the driver had a warmup / measure window (the `m12_gate` steady-window rule), and cites the M13 hop-bench number as context only — a different fleet shape, not a comparison | UNRUN |
| B3-v2 | the same, v2 binary, on the fleet after the upgrade session (#28) | as B3-v1 | UNRUN |

Either cell may read NOT RUN if no fleet existed at the right moment; a
dev-box rate is smoke and is never written here.

### B4 — application upgrade v1 → v2 (bar)

| row | clause | bar | result |
|---|---|---|---|
| B4.i | state agreement after the operator declares the flag-day upgrade done | the harness's divergence check finds **every voter and any learner in agreement** on the full KV state | UNRUN |
| B4.ii | acknowledged writes across the upgrade | **every** write acknowledged before the upgrade (a maintainer-side known key set written through the harness beforehand) is **readable with its acknowledged value after** | UNRUN |

Reported, not barred, in the B4 cells: whether the operator found an
upgrade procedure at all (that is B1'-7's business — the charter's survey
expects a documented absence), and whether a rolling swap was attempted.
**If a rolling attempt diverged the cluster, B4.i FAILs and B1'-7 records a
blocking doc defect**, because the docs did not prevent it.

### B5 — ledger resolution (appended 2026-09-15, under convention 6)

Charter decision 5 puts "every ledger item resolved" inside docs
sufficiency. It cannot be a clause of a run row — resolution happens after
the run, in the triage tickets — so it is its own row, adjudicated when
the deliverables ticket (#30) closes. An item is **resolved** when it has
landed on `main` as a doc fix, an API fix, an accepted limit recorded in
the experience report, or a product ticket (the charter's Destination).

| row | measure | bar | result |
|---|---|---|---|
| B5-builder | every item on the builder's ledger (v1 and v2 runs) | **100 %** resolved; the cell lists the count per outcome kind | UNRUN |
| B5-operator | every item on the operator's ledger (all three sessions) | **100 %** resolved; the cell lists the count per outcome kind | UNRUN |

## Results

Every cell above reads UNRUN. Fill cells in place with the run date, the
verdict, and a pointer to the evidence (the transcript audit's report, the
harness's output, the ledger delta), never by editing the bar column.

## When this gate is run

- B1-v1 and B2-v1 during and after ticket #23 / #24; B1-v2 and B2-v2
  after #25 and its adjudication.
- B1'-1 … B1'-3 and B3-v1 in the first operator session (#26); B1'-4 …
  B1'-6 in the second (#27); B1'-7, B4 and B3-v2 in the third (#28).
- B5 when the deliverables ticket (#30) closes.
- Anything appended under convention 6 says when, in its own row.
