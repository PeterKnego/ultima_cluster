# Veil: bug-hunting only, never the record

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

Veil is a bounded model checker. It explores every state of a small model up to a fixed depth and reports any state that breaks a rule. UC uses it to hunt for bugs and to check designs. UC does not use it as evidence that anything is correct. This page says why, and what the guardrails are.

**Location:** [`proofs-veil/`](/proofs-veil), kept as an archive.
**Record:** [`docs/benchmarks/uc2-veil-commit-plane-checkpoint-2026-07-26.md`](/docs/benchmarks/uc2-veil-commit-plane-checkpoint-2026-07-26.md).

## What Veil is used for

[Veil](https://github.com/verse-lab/veil) was presented at CAV 2025. Here it runs as a bounded explicit-state model checker over the election plane and the reconfiguration plane. Its useful output is a countermodel trace: a concrete sequence of steps that reaches a bad state. Such a trace becomes a directed `uc_sim` regression and a Rust fix. A second, lesser output is candidate invariant text.

## The three guardrails

1. **Veil is never the record.** Permanent proofs live in `proofs/`. Those are Lean v4.32.0, with the standard axiom trio and no SMT solver in the trusted base. A Veil model has no conformance rig against the real Rust. It is scratchpad-only. Any bug it finds is reconfirmed in Rust on its own before a fix is made.
2. **Toolchain isolation.** The models target Veil's `veil-2.0-preview` on Lean v4.28.0. That is incompatible with the v4.32.0 that `proofs/` uses. The models are never on any `proofs/` build path, never in a CI gate, and never behind a "proved" claim. They do not build in this repo. There is deliberately no CI job.
3. **No migration of anything proved.** Election safety, log matching, and the leader-completeness support stack are done in `proofs/`. Veil touches none of them.

## Why say this out loud

A bounded model check over 4.2 million states is genuinely useful. It is also not a proof. A project that blurs the two has made its strongest claims unfalsifiable. Keeping the distinction sharp is the point of this page.
