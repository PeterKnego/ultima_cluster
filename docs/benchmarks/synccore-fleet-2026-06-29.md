# SyncCore vs RaftCore — fleet A/B (real fsync + QUIC), 2026-06-29

First real-I/O fleet measurement of UC on SyncCore vs RaftCore. **Inconclusive at the
resolution achieved** (single rep per arm; the denoising reps failed when the fleet went
unreachable) — but it does surface a stability concern worth fixing before the next run.

## Setup

- 3× AWS `c6id.2xlarge` (8 vCPU + local NVMe), us-east-1, placement group. UC-only fleet
  (`make up-uc`), `durability: consistent` (real `fdatasync` per commit), rate ladder
  [100…20000] msg/s, payload 64B, inflight 128, 10s measure/rung.
- A/B by build feature: RaftCore (default) vs SyncCore (`-e uc_sync_core=true` →
  `--features sync-core`). Verified the SyncCore arm's `uc-node-launch` binary actually had
  SyncCore compiled in (203 `SyncCore` symbols + `sync_core.rs`/`sync_durability.rs` paths).
- Infra wiring added this round: rsync the `../openraft` path-dep to the hosts, and a
  `uc_sync_core` build toggle (committed to bench-infra).

## What we got (single rep per arm)

Aggregate fitness:
- RaftCore: throughput 9,401 msg; knee 5,000 msg/s; p99@knee 123 ms.
- SyncCore: throughput 10,258 msg (+9%); knee 10,000 msg/s; p99@knee 559 ms.

Per-rate p50 / p99 (ms), and SyncCore Δ vs RaftCore:

| rate | RaftCore p50/p99 | SyncCore p50/p99 | p50 Δ | p99 Δ |
|---|---|---|---|---|
| 100 | 8.14 / 8.68 | 7.35 / 8.07 | −10% | −7% |
| 500 | 6.52 / 14.35 | 9.71 / 22.87 | +49% | +59% |
| 1000 | 9.18 / 50.46 | 10.09 / 29.95 | +10% | −41% |
| 2000 | 8.54 / 67.76 | 26.18 / 68.49 | +206% | +1% |
| 5000 | 10.31 / 123.27 | 23.05 / 100.53 | +124% | −18% |
| 10000 | 171.18 / 551.55 | 224.00 / 559.42 | +31% | +1% |
| 20000 | 7629 / 16375 | 4408 / 9261 | −42% | −43% |

**These per-rate deltas swing from +206% to −43% — that is run-to-run noise, not signal.**
A single rep cannot resolve an effect this size against fleet variance. The only directional
hints: SyncCore's knee shifts up (5k→10k) and the overload tail (20k) is markedly better,
but neither is trustworthy at n=1.

## The denoising attempt failed — and why it matters

To cut the noise I launched 2 more reps per arm (medians of 3). **All four rep sweeps
failed: the fleet went unreachable mid-batch** — every node timed out with ansible's
"Timeout waiting for privilege escalation prompt" (sudo couldn't even be scheduled), and a
direct SSH to node0 hung (2-min timeout). The boxes became unresponsive.

Timing: provision, the RaftCore sweep, and the *first* SyncCore sweep all succeeded; the
hang began on the **repeated** cluster relaunches in the SyncCore rep batch. Cause
**unconfirmed**, but the leading candidate is SyncCore-specific:

- The SyncCore consensus loop runs on a dedicated `std::thread` spawned in `Raft::new`, and
  that thread is **detached** (its result returns via a oneshot; the `JoinHandle` is
  dropped). If node teardown doesn't reliably fire `rx_shutdown`, that thread keeps
  **busy-spinning forever**. `make iterate` relaunches the 3-node cluster each rep, so a
  per-restart leaked busy-spin thread would accumulate and saturate all 8 vCPU within a few
  iterations — exactly the "box stops responding to sudo/SSH" signature, and exactly on the
  *repeat*-launch path that first exercised it.

This is a real, fixable risk to investigate before the next fleet run (confirm the SyncCore
consensus thread + durability consumer are joined on node drop; don't detach). An AWS/
instance coincidence can't be ruled out, but the pattern points at thread cleanup.

## Conclusion

- **The fleet number is inconclusive at n=1.** The e2e effect of the 3d redesign is within
  single-run noise. This is consistent with the floor decomposition: 3d does **not** touch
  the dominant replication-choreography bucket (~37%) or the 3-proc IPC/linger — replication
  is still delegated to RaftCore's async tasks under sync-core — so its measured win (the
  `save_committed` overlap) is expected to be small e2e and easily masked, like every prior
  µs-scale optimization in this project.
- **The controlled latency-injected microbench remains the reliable evidence** that the 3d
  redesign is valuable in its target regime (SyncCore beats RaftCore once per-commit
  durability >~25 µs). The fleet neither confirms nor refutes that at the resolution reached.

## Next (in order)

1. **Fix/confirm SyncCore node teardown** — ensure the consensus `std::thread` (and the
   durability consumer) stop on `Raft` drop; reproduce locally by launching+dropping a
   sync-core node in a loop and watching thread count / CPU. This is a prerequisite for any
   repeated-run fleet bench (and matters for production lifecycle).
2. **Denoised fleet run** — 5+ reps per arm, medians, ideally interleaved; only after (1).
3. The real e2e win likely needs **3c** (replication off RaftCore) so the dominant bucket is
   actually addressed — 3d alone is expected to be masked e2e regardless of denoising.

Cost guard: fleet fully destroyed (11 resources, terraform state empty). Raw single-run CSVs
in the session scratchpad (`armA_raftcore.csv`, `armB_synccore.csv`).

---

## Diagnostic re-run (instrumented, same day) — hang RULED OUT as SyncCore

Re-ran a small fleet with a 1 Hz on-node sampler (load, `uc-node-launch` proc + thread
count, memory) across 4 back-to-back SyncCore iterates, to test the saturation / thread-leak
/ OOM hypotheses.

**Findings (3× c6id.2xlarge, 8 vCPU):**
- **No saturation.** Peak load across the entire run was **~2.5–2.85 on 8 cores** — never
  close to saturated. (A busy-spin death-spiral would show load ≥ 8.)
- **No leak / no accumulation.** Peak **4 threads and 1 process** per `uc-node-launch`, ever.
  Between iterates `ucprocs` returned to 0 (clean teardown); the one flaky iterate that left a
  straggler was cleaned by the next iterate's start-`pkill`.
- **No OOM.**
- **The "hang" reproduced — at low load — as a transient ansible failure.** One of the four
  iterates failed with the *exact* original signature: `Timeout (62s) waiting for privilege
  escalation prompt` → all nodes `UNREACHABLE`, while load was only ~2.7. It **self-recovered**:
  the very next iterate ran cleanly. An earlier diag attempt's `make up-uc` *provision* also
  flaked once and then succeeded on retry.

**Conclusion:** the fleet "hang" is **intermittent ansible sudo/SSH privilege-escalation
flakiness** (strikes at low load, self-recovers), **not** a SyncCore resource problem.
SyncCore runs on the fleet at the same low load as RaftCore, with a bounded ~4 threads/node.
Both the teardown-leak hypothesis (test-harness cycle) and the saturation hypothesis are now
ruled out.

**Actionables (bench-infra, not SyncCore):**
- Harden the flaky ansible tasks against the sudo-escalation timeout — e.g. enable SSH
  pipelining / longer `ControlPersist`, raise the become timeout + add retries on the
  election-wait / launch tasks, and check whether remote `sudo` is doing slow DNS/PAM lookups
  (a classic cause of low-load 60s sudo stalls on cloud hosts).
- With that fixed, a denoised multi-rep SyncCore-vs-RaftCore A/B becomes reliable. The
  single-rep numbers above stand as a first (noisy) data point; SyncCore is **not** pathological
  on the fleet.
