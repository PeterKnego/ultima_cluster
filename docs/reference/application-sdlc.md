# SDLC Standard for Applications Built on UltimaCluster

**Scope:** This document defines the software development lifecycle for any application logic that runs inside the UltimaCluster state machine replication (SMR) layer, or otherwise interacts directly with the replicated log. It is not a general engineering process document — it exists because SMR applications fail in ways ordinary applications don't: silent state divergence, replay corruption, and non-reproducible bugs that only appear after a leader election or restart.

---

## 1. Design Phase

Before any code is written, every new state machine feature or app must produce a short design note covering:

- **Determinism boundary.** Which parts of the feature execute inside the replicated state machine ("hot path") vs. outside it (client-side, read-only services, async side effects)? Draw this boundary explicitly.
- **State shape.** What new fields/collections are added to replicated state? What is their serialization format and version?
- **Failure semantics.** What happens to this feature during leader election, node crash-restart, network partition, or a mixed-version cluster during rolling upgrade?
- **Invariants.** What must always be true about this state (e.g., "position size is monotonically adjusted only via matched trades," "applied index never regresses")? These become property-based test targets later.

Design notes are reviewed by at least one person with cluster-layer experience before implementation starts — this is the cheapest point to catch a determinism violation, long before it becomes a 2am incident.

---

## 2. Implementation Phase

### 2.1 Determinism rules (non-negotiable)

Code inside the replicated path must never:
- Read wall-clock time directly (`SystemTime::now()`, `Instant::now()`) — time must come from the replicated log entry.
- Generate randomness without a log-supplied seed.
- Rely on iteration order of non-ordered collections (`HashMap`/`HashSet`) — use `BTreeMap`/`BTreeSet` or an explicitly ordered structure.
- Perform I/O, spawn threads, or call external services.
- Use floating-point operations whose results can vary across CPU architectures, unless verified bit-identical across your deployment targets (relevant given cross-arch dev/prod use, e.g. Graviton in production).

**Enforcement:** a custom clippy lint (or at minimum a documented code-review checklist) flags these patterns. Ideally, the "deterministic core" lives in its own crate/module with a restricted dependency graph so non-deterministic crates can't even be imported.

### 2.2 Schema and protocol versioning

- Every SBE (or other wire) schema is versioned. Fields are only ever appended, never redefined or removed in place.
- Snapshot formats are versioned, with an explicit migration path: old snapshot + new binary must produce correct state.
- Breaking changes require a documented upgrade playbook (see Section 5).

### 2.3 Code review gates

In addition to normal review, cluster-path changes require sign-off confirming:
- No determinism rule violations (2.1)
- Any new state field has a versioning plan
- Invariants from the design note are covered by tests (Section 3)

---

## 3. Testing Phase

Testing for SMR applications is structured as a pyramid, with the top layers being mandatory CI gates, not optional extras.

| Layer | Purpose | Gate |
|---|---|---|
| Unit tests | Standard logic correctness | Every PR |
| Property-based tests (`proptest`) | Verify invariants from the design note hold under randomized inputs | Every PR touching state machine logic |
| Golden replay tests | Record real log sequences; replay on a fresh instance; assert identical resulting state hash | Every PR touching replicated state |
| Cross-node divergence checks | Periodic state hash comparison across replicas | Staging, continuous |
| Fault injection / chaos tests | Network partition, crash-restart, disk full, clock skew | Required before merging cluster-layer changes |
| Consistency checking (Jepsen/Elle-style) | Catch linearizability violations that unit/integration tests miss | Scheduled runs + before major releases |
| Fuzzing (wire format decode paths) | Malformed/adversarial input handling | Continuous (`cargo-fuzz`), tracked as a backlog burn-down |

UC ships an Elle-based checker and a linearizability checker for its own core (`docs/VERIFICATION.md`); whether and how an application can reuse them is part of what the dogfood effort settles.

---

## 4. Verification Phase (for high-risk core logic)

Not every feature needs formal proof, but the highest-risk components do:
- Log durability and commit semantics
- Snapshot correctness and restore logic
- Any code that determines what gets applied vs. rejected

For these, the standard is: property-based tests as the everyday gate, with formal verification (Lean 4 / Aeneas) reserved for the small, stable core where a bug would cause silent, unrecoverable state corruption rather than a crash.

---

## 5. Rollout Phase

> **Status at 2.12.0.** This section describes the target, not the current
> release. A rolling upgrade of an application is not yet supported: a state
> machine's `VERSION` is compared for equality on the snapshot path only, two
> versions on the live commit path are not detected, and an application
> upgrade today is a flag day. Rolling upgrades are planned for a coming
> release (`docs/BACKLOG.md` item 3, "Rolling upgrades and leadership
> transfer"). The flag-day procedure that this status leaves you with is
> [Upgrade an application](../how-to/upgrade-an-application.md) — the how-to the
> playbook below points to until the rolling path exists.

- **Canary first.** New state machine versions deploy to a non-voting learner node before promotion to voter status.
- **Mixed-version tolerance.** Every change must specify whether the cluster can run mixed versions during rollout, and for how long.
- **Rollback plan.** Documented and tested — not just "redeploy old binary," but what happens to state written by the new version if rolled back.
- **Upgrade playbook** (required for any snapshot/schema change):
  1. Rolling upgrade order (learners → followers → leader)
  2. Expected behavior of mixed-version cluster during the window
  3. Verification steps post-upgrade (state hash comparison across nodes)
  4. Rollback trigger conditions and procedure

---

## 6. Observability Requirements

Before a feature is considered production-ready, it must expose:
- Apply latency (per state machine operation)
- Log replication lag
- Snapshot duration and size
- Leader election frequency
- Durability level in effect (quorum vs. per-node fsync)
- Feature-specific invariant violations, if detectable at runtime (defensive assertions that log rather than crash, where safe)

Every app also documents a short runbook: what does this feature do, and what should an operator expect to see, during a leader change or network partition?

---

## Summary checklist (per feature)

- [ ] Design note: determinism boundary, state shape, failure semantics, invariants
- [ ] Determinism rules followed (no time/randomness/hash-order/I-O in hot path)
- [ ] Schema/snapshot versioning plan
- [ ] Unit + property-based tests for stated invariants
- [ ] Golden replay test added
- [ ] Fault injection test passes
- [ ] Upgrade/rollback playbook written (if state format changes)
- [ ] Observability metrics wired
- [ ] Runbook documented
