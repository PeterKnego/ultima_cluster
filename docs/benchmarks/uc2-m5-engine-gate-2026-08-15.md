# uc2 M5 gate RE-RUN — the public Engine as the measured path (pipelined-client acceptance)

**Date:** 2026-08-15
**Status:** PRE-COMMITTED — this header and the decide rule below are written
and committed BEFORE the fleet run; results land in the "Fleet result"
section at the bottom afterward. (Project discipline: the rule may not be
touched after data exists.)

## Why a re-run

The pipelined-client arc (spec
`docs/superpowers/specs/2026-08-13-uc2-pipelined-client-design.md`, merged
main `c3011e2`) rewrote `m5_gate`'s client role onto the PUBLIC
`uc2_client::Engine` (`SendHalf`/`PollHalf`) — the raw ring pump the original
gate used is gone. The plan's acceptance criterion: **the measured path is
now the shipped path, and the M5 fleet gate must reproduce its numbers
through the public API.** Sandbox smoke showed parity (~407k median vs 435k
baseline on the core-starved box) after the poll-thread idle ruling
(20 µs sleep, old-matcher parity); this run is the real proof.

One measurement-semantics change vs the original harness, deliberate and
review-mandated: the client now also counts engine-swept losses
(`Outcome::TimedOut`/`InstanceRestart` → `lost`) and the PASS bar gained
`lost == 0` — the engine's 30 s deadline sweep would otherwise convert a
lost response into a resolved slot and erase the old harness's
in-flight-at-end evidence. The bar is strictly TIGHTER than the original.

## Pre-committed decide rule

Setup must match the original gate (`uc2-m5-gate-2026-07-12.md`) exactly:
3 × c6id.2xlarge, us-east-1, single AZ, cluster placement group, private-IP
binding, one `node` + one `service` per host under `systemd-run`, instance
dirs on NVMe (`/opt/bench/m5/inst`, fresh per point), client on the leader's
host, 64 B payloads, 15 s per point, the same 7-point sweep
(admission {64,128,256} KiB × inflight {4096,1024} + 64/512).

1. **GATE (spec §9 bar):** PASS iff at least one sweep point clears the
   client binary's own bar — `responses/s ≥ 400_000 && p50 ≤ 1.0 ms &&
   in-flight at end == 0 && lost == 0` — with `RESULT: PASS` printed by the
   binary itself (never inferred).
2. **ENGINE PARITY (the extraction question):** at the historical best point
   (**256 KiB admission, inflight 1024**; original: 1,639,187 resp/s @ p50
   0.600 ms), the re-run's responses/s must be **≥ 90% of 1,639,187
   (= 1,475,268)** with p50 ≤ 1.0 ms. The 10% envelope is the project's
   standing dip rule (M6/M7 precedent). Verdict vocabulary:
   - both (1) and (2) hold → **PASS, parity — extraction accepted**;
   - (1) holds, (2) misses → **PASS with regression** — recorded honestly,
     investigated off-fleet; no on-fleet tuning beyond the pre-committed
     sweep;
   - (1) misses → **FAIL (honest)**, investigate.
3. **Invalidation rules (unchanged from the original doc):** mid-run
   leadership loss (`not_leader > 0`) or a stalled service
   (`in-flight at end` large / `lost > 0` / `broadcast overwritten > 0`)
   invalidates the POINT — diagnose and re-run that point (once); it never
   counts as a sweep result.

Source tree: local main @ `c3011e2` (pushed), rsync-shipped by ansible,
built `--release` on-host.

## Fleet result

*(to be filled by the run — verbatim best-point console + full sweep table +
the parity computation against the rule above)*
