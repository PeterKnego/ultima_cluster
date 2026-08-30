# M8 wire crypto — formal-methods follow-ups

**Date:** 2026-07-29. Recorded at M8 merge (`main` @ the wire-crypto merge commit).
Two forward items for the correctness infrastructure. Neither blocks M8; both
came out of the question "does formal verification / consistency checking need
adaptation now that node↔node auth+enc exists?" during the M8 finish.

**Short version.** Elle needed adaptation and got it in M8 (T15: a `UC2_CRYPTO=1`
tier over a crypto-enabled cluster, a posture sidecar, and a standing
`elle-crypto` nightly job — crypto sits *below* the consistency boundary elle
observes, so running it green with crypto on is the complete and correct check).
Lean and Veil do **not** need adaptation to stay valid, because both model the
consensus plane and abstract the transport, and a sealed datagram once opened is
byte-identical to a cleartext one. The two items below are the residue: one
documentation note, one genuinely valuable new bug-hunt.

---

## 1. Lean — record the trust-boundary assumption (documentation, not a proof)

**What.** Add a sentence to the Lean gate doc
(`docs/benchmarks/uc2-lean-gate-2026-07-16.md`) and/or the assumptions preamble
of the proof corpus stating that the proofs assume **authentic delivery** — a
delivered datagram carries what its sender put in it — and that M8 wire crypto
is what makes that assumption hold on an *untrusted* network (previously it held
by the "trusted private network" posture).

**Why it matters (the substance, so the note is written correctly).** The
`Uc2Model` proofs are over byte positions and consensus transitions; they are
**not** a Byzantine-content model. They implicitly assume that a datagram which
arrives carries authentic content. On a trusted network that was true by
deployment posture. Wire crypto — specifically the **header-as-AAD** decision
(the T6 review finding) — is the mechanism that makes the same assumption hold on
an untrusted network: AES-256-GCM guarantees a datagram either opens to exactly
what was sealed or fails closed, and the 16-byte header (position / term / kind /
key_epoch) is authenticated associated data, so an on-path attacker cannot
rewrite a routing field on an otherwise-valid datagram. There is no "opens to
different bytes" or "routing field tampered" failure mode. So crypto does not
require a new proof — it **extends the validity domain of the existing proofs
from trusted to untrusted networks**. Had the header *not* been authenticated
(the hole T6 caught, which would have reintroduced the Finding-#6b acked-write-
loss class through the transport), that assumption would have been *false* on an
untrusted wire, and the proofs' conclusions would not transfer. Worth stating
explicitly so a future reader knows exactly which real-world property the proofs
now rest on.

**Scope.** A few sentences. No proof, no model, no conformance-rig change (the
rig is over `uc_consensus`, which crypto never touches).

---

## 2. Veil — a bug-hunt of the key-epoch / rotation / activation plane

**What.** A Veil (`proofs-veil/`) explicit-state model of the group-key plane:
`mint → deliver → ack → activate → rotate`, driven by datagram loss / reorder,
node restart, and leader change. A new hunt in the shape of the M7 reconfig
spike (`docs/benchmarks/uc2-veil-spike-2026-07-24.md`,
`proofs-veil/README.md`) — scratch-only, never the record, guardrails intact.

**Why it matters — this is the highest-value formal follow-up M8 leaves behind.**
M8 shipped **three real bugs in already-shipped code** that per-task unit review
and the deterministic sim both missed, and that surfaced only when the crypto
plane, elections, and multi-process timing composed:

- **Group-key single-delivery gap (T7 code, found at T12):** `GroupPlane::mint`
  emitted each `HS_KEY` exactly once with no retransmit, so a single lost
  datagram left a replica unable to open *any* group traffic until the next
  rotation (up to an hour), and it could not self-heal (a NAK'd retransmit is
  itself group-sealed under the missing epoch).
- **Cold-start livelock (T7/T12 code, found at T17):** `mint` restamped its
  activation clock on every mint, and a node mints on every `BecomeLeader` while
  elections retry far faster than the 2 s activation grace — so a cluster
  cold-starting with one member down never activated an epoch and never formed.
- **Minter-local epoch collision (found at T12, deferred):** epoch numbers start
  at 1 in every process and `KeySchedule` keys on the number alone, so after a
  leader change a new leader's epoch can collide with the outgoing leader's and
  followers overwrite the old key under the same number (transient DATA loss,
  NAK-repaired — a nice-to-have, not a safety break).

All three are bugs in a **small protocol state machine** over
`(epoch, acked-set, activation-time, leader-change, loss, restart)` — precisely
Veil's sweet spot, the same explicit-state shape that rediscovered the textbook
disjoint-quorum data-loss counterexample in the M7 hunt. A model of this plane
would very plausibly have produced the redelivery gap and the cold-start
livelock as bounded counterexamples *before* they shipped. That the sim did not
find them — it preserves crypto state across a simulated restart and does not
model the mint-per-election / activation-grace interaction — is exactly the
argument for modeling the plane where the sim is blind.

**Scope.** A Veil spike session or two, following the existing guardrails
(Lean 4.28 `veil-2.0-preview` in a separate checkout; countermodels →
directed `uc_sim` regressions + Rust fixes; never on `proofs/`'s build path).
The rotation bugs are already fixed in shipped code, so the immediate value is
(a) a regression oracle and (b) hunting the *next* rotation-plane bug of the same
class before it ships.

---

## Not on this list (and why)

- **Elle** — done in M8 (above); nothing further. Crypto failure modes are
  liveness (stall / churn, fail-closed), not consistency anomalies, so no new
  elle "tooth" is warranted; adversarial consistency (header tampering) is
  T14's domain, and T14 proved a forged/tampered datagram is refused.
- **Nonce uniqueness** — the one catastrophic property (no `(key, nonce)` repeat;
  a repeat leaks the GCM auth subkey). Guarded today by hand-argument, the M8
  final-review's independent path enumeration, and the runtime single-call guards
  on the transport halves. Not a natural fit for Lean (not consensus) or elle
  (not an observable outcome). If defense-in-depth on the single unrecoverable
  property is ever wanted, it's a targeted invariant over the counter +
  per-sender-per-boot key-derivation scheme — a distinct, small effort, listed
  here only so the option is on record.
