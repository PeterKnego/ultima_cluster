# uc2 elle gate — transactional-safety checking + mutation testing

**Status: PASS (local, green).** In-process elle consistency harness plus a
mutation-testing tier that proves the harness has teeth. Branch
`uc2/elle-harness`; design spec
`docs/superpowers/specs/2026-07-15-uc2-elle-harness-design.md`, plan
`docs/superpowers/plans/2026-07-15-uc2-elle-harness.md`. This is not a milestone
gate (M1–M7 are the milestones); it is a standing correctness capability, run by
`scripts/elle_check.sh` (nightly CI `elle` job) and `scripts/elle_mutation.sh`
(weekly `elle-weekly.yml`).

## What it adds

The existing WGL lincheck capstones (`lin_v2`, `lin_partition_v2`) check
**linearizability of a single register**. Elle checks **transactional safety of a
list-append workload** by cycle detection over the recorded history, under two
consistency models — `serializable` and `strong-serializable` (the strict,
real-time/linearizable model). It catches a class of anomaly the register
capstone cannot phrase: real-time (stale-read) violations that plain
serializability would legally reorder into the past.

- Workload: singleton list-append transactions (`ListAppendSm` in `uc_lincheck`),
  append values globally unique from one `AtomicU64`.
- Checker: vendored `elle-cli-0.1.9-standalone.jar` (EPL-2.0, sha256
  `c9ba9b9fd32640e73d632cb5f15069c162ba6528a67f27a878767187c59f539a`), pinned in
  `tools/elle-cli/`. Exit codes are untrusted; the scripts parse the stdout/JSON
  verdict and treat `unknown` (cycle-search timeout) as a hard FAIL.
- **Strict model = `strong-serializable`** (the 0.1.9 jar supports it — this
  retired the plan's main open risk at T3).
- History recorder: `uc_lincheck::edn` (Jepsen EDN; `:info`+process-retire for
  maybe-committed appends, `:fail` for failed reads).
- Harness: `LinClusterV2` — real node/service agents, real reliable-UDP on
  loopback, real instance dirs — genericized over the SM.

## Clean tier — `scripts/elle_check.sh` (PASS)

Five passes, each `valid` with an **empty anomaly set under BOTH models**. Every
invocation first self-tests the checker against two fixtures (a known write-skew
cycle rejected under `serializable`; a real-time violation accepted under plain
serializable but rejected under `strong-serializable`) — the checker's own teeth,
verified before any real verdict.

| Pass | events | serializable | strong-serializable |
| --- | --- | --- | --- |
| quiet | 100,836 | clean | clean |
| failover | 45,702 | clean | clean |
| partition | 51,770 | clean | clean |
| purge | 54,574 | clean | clean |
| reconfig | 96,714 | clean | clean |

Sizing notes: `reconfig` runs **1 worker** (its history is driven by
time-to-config-non-vacuity × throughput; at 4 workers the ~195k-event history
stalls elle-cli's strict cycle search into `unknown`). CI caps `ELLE_TARGET_OPS`
at 8000 to keep the 4-vCPU runner clear of `unknown`.

## Mutation tier — `scripts/elle_mutation.sh` (PASS: control clean + 3/3 teeth)

Three injected consensus bugs behind a `mutation-testing` cargo feature (OFF in
every default build, inert when the feature is on but `UC2_MUTATION` is unset —
verified by the CONTROL run). `UC2_MUTATION` is read once via a `OnceLock` in
`uc_node`; `uc_consensus` stays env-free (feature-gated boolean setters only).

### The finding that shaped the design

The design spec mapped `commit-quorum-minus-one` and `skip-vote-order-check` to
the **failover** pass and `skip-read-barrier` to **partition**. Empirically
(≈20 fault-injected runs) that mapping **cannot** expose two of the three teeth,
because UC's own safety layers absorb them:

- `kill_and_restart_leader` restarts the **same node on its disk**, so a
  quorum-1-committed or stale-truncatable tail never actually dies — the
  failover pass exposes neither commit-quorum nor vote-order.
- UC has **no check-quorum step-down** (`election.rs::on_tick`) and layered read
  guards, so skipping *only* the READ_PROBE barrier still yields no stale read
  under gross leader-isolation (verified: read list-lengths stay monotone).

This is a real robustness result about UC, not a harness defect. Per two
user-approved decisions, the teeth use a **dedicated adversary** and **oracles
matched to how UC actually catches each bug**:

| Mutation | Site | Adversary pass | Oracle | Result |
| --- | --- | --- | --- | --- |
| `commit-quorum-minus-one` | `CommitTracker` | `elle_mut_commit_quorum` (leader-isolation split-brain) | elle verdict **INVALID** (serializable AND strict) | `incompatible-order`, `strong-PL-1-cycle-exists` |
| `skip-read-barrier` | `uc_node` read path | `elle_mut_read_barrier` (directed 2-process probe) | elle **INVALID under the STRICT model ONLY** (valid under plain serializable) | `G-single-item-realtime` |
| `skip-vote-order-check` | `ElectionSm::log_ok` | `elle_mut_vote_order` (minority-isolate → term climb → heal) | driver run **HARD-FAILS** (exit ≠ 0) | *originally* an `uc2-archive` truncation-below-commit panic (`node.rs:716`); **re-based 2026-08-02 onto an explicit `CommittedTruncationWitness`** — see below |

`skip-read-barrier` is the tooth that **proves the strict model earns its keep**:
its anomaly is invisible to plain serializability and only the real-time model
catches it. Landing it reliably required a **directed 2-process probe** — a
rerouting client commits an append on the live majority, then a client pinned
directly to the isolated old leader (`cluster.client(l)`, no reroute) reads the
same key; append-completes-before-read-starts, so a missing value is a genuine
real-time anomaly. The mutation was strengthened (user-approved) to bypass the
leadership `can_serve` gate **on the read path only** (writes stay gated), so an
isolated leader genuinely serves stale reads. Natural-worker attempts all failed
(a pinned worker reroutes away on its first `NotLeader`'d submit; the fragile
2-node majority never outran the most-advanced isolated node).

`skip-vote-order-check` manifests as a **hard failure**, which is a timing race
(whether a stale candidate wins at all). The script **retries up to
`ELLE_VOTE_ORDER_TRIES`** (3 local / 5 CI); caught iff any attempt hard-fails,
and a clean control passes every attempt.

### 2026-08-02 — the tooth went silent, and why (oracle re-based)

`elle-weekly` run 30736463470 failed: control clean, teeth 1–2 caught, **tooth 3
missed 5/5**. Reproduced locally 3/3 on `main`.

**Root cause: the oracle was borrowed from a different bug, and that bug got
fixed.** As written, this tooth asserted nothing of its own — it was scored on
whatever made the driver exit non-zero, and what did was the `uc2-archive`
fail-stop above. That fail-stop was **issue #6, a genuine UC defect**: a stale
winner opening its term below the archive's cursor corrupted the record walk.
`2fd845e` (07-30) fixed it by routing the leader-open collapse through the
archive agent. The injected bug still destroys committed data afterwards — it
simply no longer crashes anything. The two preceding weeklies caught it via that
panic (07-19 try 2, `node.rs:734`) and via a harness-side panic (07-26 try 3);
neither was the safety property. **A mutation-testing oracle that depends on a
defect elsewhere expires the day that defect is fixed.**

The replacement is `lincheck_v2::CommittedTruncationWitness`, a background
sampler over every node's cnc page, convicting on the property directly: **the
committed frontier must never vanish from the cluster** — `C` = the furthest
position any node ever called committed; a violation is every node's `durable`
sitting below `C` for 3 consecutive samples, i.e. nobody holds it any more.

Two calibration findings, both worth keeping:

* A **first draft convicted per node** (`durable` steps back below *that node's*
  own commit view) and needed an arbitrary byte margin to stay clean. Measurement
  killed it: unmutated control runs regularly show one node cut **17–20 KB**
  below its own commit view (107 such events in one 90 s run) while the other two
  keep the frontier — a diverged tail being cut, not data lost. The cluster-wide
  formulation needs no margin and has no such band.
* Under the mutation, **all three** nodes drop below `C` together, by
  25,888 / 62,496 / 421,152 B across three runs.

Verification (4-vCPU dev box, `main` + this change):

| Arm | Params | Result |
| --- | --- | --- |
| mutation ON | CI (`MIN_FAULTS=12 HOLD_MS=3000 WORKERS=4`) | **3/3 convicted, each on try 1** (vs the old ≈1-in-2-to-3) |
| control | CI params | clean |
| control | append-heavy variant (`READ_FRAC=0.05 KEYS=64`), the workload that maximises single-node dips | **4/4 clean** |

Local caveat: several runs on this box died `signal: 9` (OOM) — the in-RAM EDN
history is ~500 MB and the box had ~2 GB free. Unrelated to the witness, but note
that **the script counts any non-zero exit as a catch**, so an OOM kill would be
scored as a catch. The witness line in the log is what actually distinguishes
them, which is why the failure hint now greps for it.

Full run: control clean (all three passes), then commit-quorum
`serializable=false strict=false`, read-barrier `serializable=true strict=false`,
vote-order caught on retry. Exit 0.

## Feature-off inertness (verified)

- feature-OFF + feature-ON `cargo clippy -p uc_node --all-targets -- -D warnings`: clean.
- feature-OFF `uc_node` lib unit tests: 24/24 (the read-path mutation is
  `#[cfg]`-shadowed → default build byte-identical).
- workspace `clippy --all-targets` + `cargo test --workspace --no-run`: clean.
- clean-tier `elle_check.sh` green from a clean build → no feature cross-contamination.

## Sizing gotchas (for the next operator)

- `unknown` = elle-cli cycle-search timeout; never a pass. Shrink
  `ELLE_TARGET_OPS` or raise the checker heap (`ELLE_JAVA_XMX`).
- `reconfig` needs 1 worker (above).
- `vote-order` is a timing race → retried; raise `ELLE_MIN_FAULTS` / `ELLE_HOLD_MS`
  / worker count if it stops biting, never weaken the catch. And when a tooth
  stops biting, ask WHY before raising the dose — see the 2026-08-02 entry above:
  the dose was fine, the oracle had quietly expired.
- **Never write histories to `/tmp`** — it is RAM-backed tmpfs with no swap on
  the dev box; large histories OOM-kill the run. Both scripts default `ELLE_DIR` /
  `ELLE_MUT_DIR` to `$HOME/.cache` (disk). Codified in `CLAUDE.md`.

## CI

- Nightly `elle` job (`.github/workflows/nightly.yml`): clean tier, 5 passes.
- Weekly `elle-weekly.yml` (Sun 04:17 UTC + `workflow_dispatch`): mutation tier.
- Both use `actions/setup-java@v4` (temurin 21); the jar is vendored, `jq` ships
  on `ubuntu-latest` — nothing downloads.
