# Design — Inter-node latency Phase B (cross-host Rust datapaths + the deferred V2 commit A/B)

**Date:** 2026-06-19.
**Status:** Spec. Cross-host, real-NIC, **measurement-and-decide** phase. Follows Phase A
(`2026-06-19-inter-node-latency-gap-analysis-design.md`, shipped in task17). Gate-A judged Phase B
warranted: the local ladder could not size the bare-metal busy-spin / kernel-bypass win (loopback has
no NIC interrupt to avoid), and the V2 pipelined-commit benefit is RTT-bound so it didn't show locally.
**Canonical record:** results land as **Part C of `docs/tasks/task17_inter_node_latency.md`**.

---

## 1. What Phase B answers

Two questions, both requiring a **real NIC + real RTT** (neither shows on loopback):
1. Can a cheaper-than-rewrite datapath get UC's inter-node latency materially toward Aeron's
   cross-host floor (task16 §6.6: Aeron 47µs p50 / 66µs p99 vs UC-UDP 98µs / UC-QUIC 91µs)?
   Candidates, cheapest-first: **`SO_BUSY_POLL` kernel UDP**, then (gated) **AF_XDP via `xsk-rs`**.
2. What is the **cross-host pipelined-commit** benefit of the Phase A V2 `stream_append` change
   (depth=1 vs depth=8) — the decisive number the loopback A/B could not produce (it bottlenecked on
   CPU/serialization, not RTT; depth=8 showed only ~14% p50 at moderate load, washed out at saturation).

## 2. Scope decisions (settled in brainstorming — do not relitigate)

- **Measure-and-decide.** Phase B builds the datapaths as cross-host **echo bench prototypes** (like the
  Phase A ladder rungs, but real-NIC), measures, and produces a Gate-B recommendation. **No production
  `ClusterTransport` is built in Phase B** — productionizing a winner is a separate follow-on.
- **`SO_BUSY_POLL` first, AF_XDP gated.** Measure the cheap busy-poll win first; build the heavy AF_XDP
  datapath only if busy-poll doesn't close enough of the gap (judgment gate, §Gate-B1).
- **Fold in the V2 commit A/B** on the same fleet (it needs the cross-host cluster anyway).
- **Judgment-based gates** (no hard µs threshold).
- **Deliverable = Part C of `docs/tasks/task17_inter_node_latency.md`.**

## 3. Fleet

One fleet covers everything: **3× `c7i.4xlarge`**, single-AZ + **cluster placement group**, AWS ENA
NICs (support AF_XDP incl. zero-copy; 16 vCPU ≥ the ≥4 isolated busy-spin cores Aeron needs).
`os_tune` core isolation applied so every datapath (busy-poll / AF_XDP / Aeron / UC) compares on equal
pinning. 3 nodes because the V2 commit A/B (§B-V2) needs a real Raft cluster; the per-link datapath RTT
(§B1/§B2) uses only node0↔node1. ~$2.14/hr on-demand — stages ordered so the cheap measurements run
first and the fleet is destroyed as soon as the run completes. Provision via the existing task16
bench-infra (`make -C bench-infra up-fanout FANOUT_INSTANCE_TYPE=c7i.4xlarge`, `cloud=aws`).

## 4. Stages

### B0 — Enabler (cheap, first)

Fix the two task16 harness rough edges that block clean cloud runs (documented in task16 §6.6 +
task17 follow-ups):
- `build_aeron` role should `chown /opt/bench/aeron-deploy` to the run user (else the JVM can't write
  results — the AWS run hit this).
- `download_results` runs on node0 and cannot scp to the control box (results stay on the node;
  currently read from the streamed orchestrator log) — make it fetch the tarball to the control box,
  or document the manual fetch.
Then provision the fleet (§3) and confirm `os_tune` isolation is active.

### B1 — `SO_BUSY_POLL` kernel UDP (the cheap win, measured first)

Add a **busy-poll UDP echo rung** to `internode-rpc-bench` (a new transport string, e.g.
`busypoll-udp`, alongside the Phase A `bare-udp`/`busyspin-udp` rungs in `bench_support.rs`). A normal
kernel `UdpSocket` with `SO_BUSY_POLL` + `SO_PREFER_BUSY_POLL`, driven by a **dedicated blocking-recv
thread** (tokio's epoll reactor doesn't busy-poll natively), with the `net.core.busy_poll` /
`net.core.busy_read` sysctls set on the hosts via `os_tune`. Busy-polls the NAPI ring on recv,
removing interrupt→softirq→wakeup latency while keeping full kernel UDP semantics. Burns a core.
Measure cross-host single-inflight RTT (64 B; 1024 B secondary) vs UC-UDP / UC-QUIC / Aeron.

### B-V2 — Cross-host pipelined-commit A/B (fold-in, same fleet)

Run `commit-path-load` against the real 3-node cluster, open-loop with high inflight (build a
replication backlog), `UC_PIPELINE_DEPTH=1` (sequential) vs `8` (pipelined). Record commit
p50/p99/p99.9 + achieved throughput at several rates, including a **lagging-follower catch-up** scenario
(a follower restarted / behind, where pipelining should help most). This is the decisive measurement of
the Phase A V2 `stream_append` work on real RTT.

### Gate B1 (judgment)

Did `SO_BUSY_POLL` close a **meaningful fraction** of the ~44µs UC-vs-Aeron gap (toward the 47µs floor),
enough that AF_XDP's weeks-of-work (raw L2 framing, UMEM, privileges) isn't justified? If yes →
recommend busy-poll, **skip B2**. If no → proceed to B2.

### B2 — AF_XDP via `xsk-rs` (gated, the big effort)

**Echo harness only** (a `afxdp-udp` rung; static MAC/IP, no ARP, minimal eth/IP/UDP framing +
checksums for the fixed 2-node link). Validation ordering:
1. **Copy / generic mode first** — works on ~any NIC; validate frames round-trip and framing is correct
   before chasing performance.
2. **`XDP_ZEROCOPY`** — only after confirming ENA driver support; NIC DMAs directly into UMEM, no copy.
Measure cross-host RTT at each stage vs UC / Aeron. Requires `CAP_NET_ADMIN`/root + an XDP program
loaded on the bench NIC (provisioned via `os_tune`/ansible).

### Gate B (final)

Per-datapath **adopt / no-adopt** recommendation, with cross-host p50/p99 justifying any added
complexity (AF_XDP privileges/framing; busy_poll core burn). Productionizing a winner as a
`ClusterTransport` (the task16 seam supports it) is a **separate follow-on**, explicitly out of scope
here.

## 5. Measurement methodology (shared with Phase A)

- Harness: `internode-rpc-bench` `--mode ping` single-inflight RTT for the datapath rungs;
  `commit-path-load` for the V2 commit A/B; `aeron-echo-baseline.sh` for the Aeron reference.
- p50/p99/p99.9, equal sample counts, fixed 64 B (1024 B secondary). All datapaths measured on the
  SAME fleet with the SAME `os_tune` core isolation — no tuned-vs-untuned apples-to-oranges (the trap
  task16 §6.6 fell into).
- The datapath rungs are **bench-only** (no production transport changes in Phase B).

## 6. Deliverable & success criteria

- **Part C of `docs/tasks/task17_inter_node_latency.md`:** the cross-host datapath RTT table
  (UC-UDP/QUIC, `SO_BUSY_POLL`, AF_XDP-copy/zerocopy if built, Aeron), the V2 depth-1-vs-8 commit A/B
  numbers, the Gate-B1 and Gate-B decisions, and an adopt/no-adopt recommendation per datapath.
- **Success:** `SO_BUSY_POLL` rung built + measured cross-host vs UC/Aeron; V2 commit A/B measured
  cross-host (depth 1 vs 8, incl. a catch-up scenario); Gate-B1 decision recorded; AF_XDP built +
  measured **iff** Gate-B1 warranted; fleet destroyed; recommendation written.

## 7. Out of scope / non-goals

- Building any production `ClusterTransport` (busy-poll, AF_XDP) — separate follow-on if one wins.
- AF_XDP beyond a fixed 2-node static-config echo harness.
- Changing the default transport (QUIC stays default).
- AF_XDP if Gate-B1 says busy-poll already closes the gap enough.

## 8. Risks

- **Cost / fleet lifetime:** ~$2.14/hr; nothing auto-reaps (`ttl_hours` is advisory) — destroy
  immediately after the run.
- **AF_XDP complexity & privileges:** raw L2 framing, UMEM lifecycle, `CAP_NET_ADMIN`, an XDP program
  to load; **zero-copy is ENA-driver-dependent** — verify support before relying on it; fall back to
  copy mode for the correctness/first number.
- **`SO_BUSY_POLL`:** burns a core; tokio integration needs a dedicated recv thread; benefit is
  NIC/driver-dependent (confirm ENA honors busy-poll).
- **Measurement validity:** must compare on equal core-isolation, or it repeats task16 §6.6's
  tuned-vs-untuned mistake.
- **Harness rough edges (B0):** if not fixed first, the Aeron reference + result collection will
  misbehave as they did on the task16 AWS run.
