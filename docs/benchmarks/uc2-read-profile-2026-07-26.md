# UC v2 — Linearizable-read profile: fleet measurement

**Date:** 2026-07-26
**Spec:** `docs/superpowers/specs/2026-07-25-uc2-read-profile-design.md`
**Plan:** `docs/superpowers/plans/2026-07-25-uc2-read-profile.md` (Task 7)
**Harness:** `uc_node/examples/read_profile.rs` (merged to main @ `bb93cec`)
**Orchestrator:** `bench-infra/scripts/m6_fleet_gate.py --fleet --read-profile`

**Fleet:** 3 × `c6id.2xlarge` (8 vCPU, local NVMe), `us-east-1`, single placement
group, one `node` + one `service` per host, client on the leader's host.
**Sweep:** `--rp-readers 1,4,16,64,256,1024 --rp-secs 20 --rp-write-rate 20000`
— 24 rungs (6 concurrencies × {lin, snap} × {read-only, mixed}), **fresh 3-node
cluster booted and torn down per rung**.
**Result:** 24/24 rungs valid, 0 client failures. Fleet destroyed; terraform
state verified empty.

---

## 1. Verdict

> **Rung A JUSTIFIED — both clauses met.** Independently on both write mixes.

| Arm | lin plateau | snap plateau | ratio | clause (a) ≤70% | clause (b) |
| --- | ---: | ---: | ---: | --- | --- |
| Read-only (`write_rate=0`) | 244,052 reads/s @1024 | 585,414 reads/s @256 | **41.7%** | met (outside 65–75 band) | met |
| Mixed (`write_rate=20000`) | 206,520 reads/s @1024 | 982,997 reads/s @256 | **21.0%** | met | met |

**The ReadIndex barrier costs roughly 58% of read capacity** on the read-only
arm — the arm where the delta is attributable to the barrier alone (§4). This is
a capacity gap that survives to the top of the ladder, not a latency artifact
that pipelining hides.

Per spec §2 this licenses **Rung A** (batch-probe coalescing, clock-free, no
safety-model change). It says nothing about Rung B, which remains sequenced
behind the Veil V2 coherence-window result per the leader-lease brief §5.

## 2. The ladder

Both arms, both mixes. `degraded` = `(retried + not_leader) / (reads + retried +
not_leader)`; `depth` = client in-flight reads sampled every 10 ms during the
measurement window (mean/min) against the `--readers` target.

### Read-only arm (`write_rate=0`)

| readers | mode | reads/s | p50 (ms) | degraded | depth mean/min | inflight_end |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | lin | 6,985 | 0.163 | 0.0% | 1.0 / 0 | 0 |
| 1 | snap | 8,955 | 0.082 | 0.0% | 1.0 / 0 | 0 |
| 4 | lin | 24,577 | 0.164 | 0.0% | 4.0 / 0 | 0 |
| 4 | snap | 35,632 | 0.083 | 0.0% | 4.0 / 0 | 0 |
| 16 | lin | 83,862 | 0.167 | 0.0% | 15.9 / 11 | 0 |
| 16 | snap | 140,514 | 0.085 | 0.0% | 15.9 / 6 | 0 |
| 64 | lin | 128,145 | 0.501 | 0.0% | 63.8 / 35 | 0 |
| 64 | snap | 523,103 | 0.096 | 0.0% | 63.1 / 38 | 0 |
| 256 | lin | 145,302 | 1.763 | 0.0% | 255.9 / 235 | 0 |
| 256 | snap | **585,414** | 0.461 | 0.0% | 254.8 / 226 | 0 |
| 1024 | lin | **244,052** | 4.293 | 0.0% | 1023.9 / 1008 | 0 |
| 1024 | snap | 523,074 | 1.949 | 0.0% | 1023.2 / 1008 | 0 |

### Mixed arm (`write_rate=20000`)

| readers | mode | reads/s | p50 (ms) | degraded | depth mean/min | inflight_end |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | lin | 5,637 | 0.168 | 0.0% | 1.0 / 0 | 0 |
| 1 | snap | 8,529 | 0.089 | 0.0% | 1.0 / 0 | 0 |
| 4 | lin | 21,412 | 0.170 | 0.0% | 4.0 / 0 | 0 |
| 4 | snap | 34,598 | 0.089 | 0.0% | 4.0 / 1 | 0 |
| 16 | lin | 75,789 | 0.200 | 0.0% | 15.9 / 5 | 0 |
| 16 | snap | 136,053 | 0.092 | 0.0% | 15.8 / 0 | 0 |
| 64 | lin | 161,061 | 0.388 | 0.0% | 63.7 / 9 | 0 |
| 64 | snap | 555,777 | 0.098 | 0.0% | 63.0 / 3 | 0 |
| 256 | lin | 162,568 | 1.577 | 0.0% | 255.9 / 237 | 0 |
| 256 | snap | **982,997** | 0.266 | 0.0% | 252.9 / 178 | 0 |
| 1024 | lin | **206,520** | 4.817 | 0.0% | 1023.3 / 957 | 0 |
| 1024 | snap | 977,889 | 1.044 | 0.0% | 1021.3 / 938 | 0 |

Totals: linearizable arm 4,882,009 reads (read-only) / 4,131,564 (mixed);
snapshot arm 11,708,472 / 19,660,078. **Zero** retries, redirects, regressions,
or unresolved reads across all 24 rungs.

## 3. Decision rule, clause by clause

Evaluated by `read_profile decide --rungs`, i.e. by the same
`evaluate_decision_rule` the unit tests pin — the orchestrator never
re-implements it, because a rule re-implemented outside its tests is no longer a
pre-commitment.

- **Clause (a) — lin plateau ≤ 70% of snap plateau.** 41.7% (read-only), 21.0%
  (mixed). Both far outside the 65–75% borderline band that would have forced
  "not justified without a further run". **Met.**
- **Clause (b) — the gap is present in the READ-ONLY arm, with ≥90% sustained
  concurrency and no degraded arm.** Read-only ratio 41.7% independently
  satisfies (a); sustained depth 1023.9/1024 (99.99%) and 254.8/256 (99.5%);
  0.0% degraded on every rung. **Met.**

Ratios at *matched* concurrency, since the two arms' maxima land at different
readers (§5.1): 24.8% at 256 readers, 46.7% at 1024. The verdict does not turn
on which convention is used.

## 4. Why the read-only arm is the load-bearing one

A snapshot read skips **both** the probe barrier and the service-frontier wait
(`node.rs:1958` forwards it immediately). With no writes in flight,
`service_applied >= commit_at` already holds when a linearizable read is
admitted, so the frontier wait is free and the lin-vs-snap delta is the barrier
alone. In the mixed arm the delta is barrier **+** frontier wait, which is why
its 21.0% is *not* quoted as the barrier's cost even though it is the more
dramatic number.

## 5. Threats to validity (spec §6), against what the run showed

1. **Snapshot skips more than the barrier** — handled by quoting the read-only
   arm (§4). Addressed.
2. **`QUERY_DRAIN_PER_CYCLE = 64` as the real ceiling** — **discharged by data.**
   The two arms plateau at very different values (244k vs 585k read-only; 207k vs
   983k mixed). Had the per-cycle query drain been the binding constraint, both
   arms would have converged on the same number, since both cross the same drain.
3. **The load generator as the bottleneck** — **discharged.** Sustained in-flight
   depth held at 99.5–99.99% of target on every plateau rung, so the plateau
   describes the node, not the single-threaded client. This was the false-negative
   path (both arms hitting the client's ceiling → ratio ≈ 100% → "not justified").
4. **Shared-core smoke** — not applicable: this is a 3-host fleet, one role per
   host. No local number appears in this document.
5. **The yield-rate occupancy proxy is dead** — confirmed again on real hardware.
   `uc2-sender`, `uc2-receiver` and `uc2-consensus` reported 0.05–0.55 yields/s
   (i.e. noise) while `uc2-archive` and `uc2-apply` reported ~5k and ~9k. Had the
   original clause (b) shipped, it would have ranked the three genuinely-busy
   agents as "busiest" on rounding noise. It is diagnostic output only and does
   not feed the verdict (spec §2.1, §4.3).
6. **Snapshot reads dropped on a full `svc_query` ring** — did not occur.
   `inflight_at_end == 0` on all 24 rungs, so no rung stalled its send governor
   and no ratio exceeded 100%.

## 6. Caveats

- **Neither arm cleanly plateaued.** The linearizable arm was still climbing at
  the top of the ladder (145,302 → 244,052 reads/s from 256 → 1024 readers), and
  the snapshot arm peaked at 256 then fell back at 1024. "Plateau" is therefore
  each arm's **maximum over the swept range**, not a demonstrated asymptote. The
  true linearizable ceiling may lie beyond 1024 readers. This does not threaten
  the verdict — every matched-concurrency ratio from 64 readers upward is ≤47% —
  but a follow-up sweep extending past 1024 would sharpen the headline number.
- **Latency is not free at the top.** The linearizable arm reaches its maximum
  throughput at p50 4.29 ms / p99 4.55 ms (read-only). Rung A amortizes probe
  *coordination*; it should not be expected to fix that latency, which is
  queueing at 1024 outstanding reads.
- **One 20 s sample per rung, single run.** No repetition, so no run-to-run
  variance is characterised.

## 7. Disposition

Proceed to a **Rung A** (batch-probe coalescing) implementation plan. It is the
clock-free rung: one probe round certifies every pending read whose `commit_at`
is at or below the confirmed position, with **no change to the safety model** —
the certification rule is the existing barrier rule applied set-wise.

Re-run this harness after Rung A lands; the same binary and the same
pre-committed rule measure the improvement.

**Rung B remains out of scope** — it introduces a bounded-clock-drift assumption
into read safety and stays sequenced behind the Veil V2 coherence-window result
(leader-lease brief §5).

## 8. Cost and hygiene

Fleet up for approximately one hour across one failed and one successful sweep
(3 × `c6id.2xlarge`). **Destroyed: "Destroy complete! Resources: 11 destroyed",
`terraform -chdir=terraform state list` verified empty, inventory removed.** The
`aws` CLI is not installed on the driver box, so the empty terraform state and
the explicit destroy count are the teardown evidence rather than an AWS-side
query.

Artifacts (per-rung client logs + `rungs.jsonl`) were retained with the run and
are reproducible from the sweep command in the header.
