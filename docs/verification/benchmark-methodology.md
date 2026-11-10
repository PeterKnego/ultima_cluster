# Benchmark methodology

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

Performance claims fail the same way verification claims do: someone picks the pass bar after seeing the number. This page records the rules UC uses to stop that, and the harnesses that sit under the gates.

## The bar is committed before the run

Every milestone gate in [`docs/benchmarks/`](/docs/benchmarks) commits its pass/fail rule to the repository before the run, in its own commit. The M8 gate put it this way:

> **Status: RULE PRE-COMMITTED, NOTHING MEASURED YET.** This file is committed
> *before* either arm runs. Everything below the "Pre-committed decide rule"
> heading is a promise made in ignorance of the data; the result sections are
> empty placeholders and are filled in afterwards, whatever they say.
>
> A bar chosen after seeing the number is not a bar, it is a description.

If an arm misses the bar, the document records an honest FAIL and the real number. The bar does not move. Git history is the audit trail. The decide rule and the result are separate commits, in that order.

## The hop-isolation harnesses

Below the gates sit harnesses that measure one hop of the pipeline at a time. They produce ratios and ladders, never gated numbers. A figure from a developer box is smoke, not evidence.

- `uc_gateway/examples/hop_bench` measures the client, edge and node hops, with stand-ins at each boundary. Its worked example is `docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md`.
- `uc_node/examples/apply_bench` measures the state machine's apply loop alone, driven by a fake node. Its worked example is `docs/benchmarks/uc2-m14a-apply-hop-2026-08-27.md`. That example found and fixed lockstep's 50 µs sleep throttle on the day M14a merged.
- `scripts/hop1_ab.sh` A/Bs exact binaries of hop 1, the client, back to back. It includes a same-source rebuild control. Its worked example is `docs/benchmarks/uc2-m14c-client-hop-2026-08-28.md`. That is the run that showed the M14b "−4.2 %" reading did not survive a fresh-build control.

## Alert rules are proven to fire

A rule that parses is not a rule that works. `scripts/m10_alert_fire.sh` builds or breaks a real cluster per rule, using `uc_node/examples/m10_alerts.rs`. It scrapes each node's real `/metrics` HTTP endpoint once a second. It then time-dilates the captured samples onto a synthetic timeline sized to that rule's `for:` clause, and lets `promtool test rules` decide. The output is one `PASS` or `FAIL` line per shipped rule. The dilation policy is disclosed at runtime, and every scenario is labelled `real` or `synthetic`.

A rule that ships without an adjudication entry fails the run before any cluster starts. All 16 rules are covered. The two M14c per-FSM rules, `Uc2ServiceAbsent` and `Uc2ServicePinnedAtLagBound`, are backed by `real` two-FSM scenarios: a declared FSM that never attaches, and one whose apply loop is slow enough that the node's own report ceiling pins `commit − applied` exactly at the lag bound.
