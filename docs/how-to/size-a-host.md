# How to size a host for a node

What hardware a `uc2-node` actually needs, from measured sweeps rather than
folklore. Every number here has a benchmark doc behind it; where your
hardware differs, re-measure with
[Reproduce a published result](reproduce-a-result.md) instead of scaling our
numbers by clock speed.

## The short version

- **CPU generation dominates everything else on this page.** The same
  cluster, same binaries, same 16-vCPU shape ran ~4× lower p50 latency on
  2025-generation cores (Graviton/Neoverse-V3) than the same-day numbers on
  a 2020-generation Xeon fleet would suggest, and +41 % throughput unpinned
  ([arch sweep](../benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md)).
  Old published numbers are floors from the hardware they name, not
  properties of the software.
- **Give the node 4 physical cores on SMT x86; 2–3 suffice on modern
  no-SMT ARM.** The node is four busy-polling agents; on Intel (8c×2SMT)
  throughput plateaus at 4 whole cores
  ([core-count sweep](../benchmarks/uc2-node-core-count-sweep-2026-08-31.md)),
  on Graviton at 2–3 — one Neoverse-V3 core ran the whole node at 87 % of
  its six-core rate.
- **Budget cores for the other processes too.** The service's apply agent
  and your client (or the gateway's threads, on a remote-serving node) each
  want a core of their own under load. The historical "collapse" cases were
  CPU oversubscription, not any component's steady state
  ([convoy explainer](../notes/uc2-m13-mpsc-publish-convoy-explained.md)).
  Starved polling threads degrade non-linearly — do not run a loaded node on
  fewer cores than the processes you co-locate on it.
- **Do not pin by default.** Pinning trades throughput for variance: it
  tightened p50 spread 31–37× in our runs but cost 9–20 % of mean
  throughput, and on a 16-core no-SMT host the unpinned scheduler beat every
  pinned width outright
  ([fleet pinning](../benchmarks/uc2-m14c2-fleet-pinning-2026-08-30.md),
  [arch sweep](../benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md)).
  Reach for `--pin` when tail consistency matters more than mean rate.
- **arm64 is a first-class citizen** as of 2026-08-31: the full correctness
  stack — workspace tests, both linearizability capstones, the SIGKILL
  crash suite — passed on real weakly-ordered hardware (Graviton), and the
  published binaries include aarch64. Verdict and caveats in the
  [arch sweep](../benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md).

## Memory and disk

- RAM: modest. Every published fleet number ran on 32 GiB hosts; the
  instance directory reserves ~78 MiB up front (fallocated, so a full disk
  is a named startup refusal, not a mid-run SIGBUS).
- Disk: the journal is the growth axis — size it with
  [Keep the journal from growing without bound](bound-journal-growth.md).
  All published numbers ran journals on local instance-store NVMe; earlier
  A/Bs found fsync latency does not gate end-to-end throughput, but we have
  not published EBS-only rate numbers — measure before assuming parity.
- `ENOSPC` is fail-stop by design; see
  [Diagnose a node](diagnose-a-node.md#is-the-disk-about-to-fill).

## Network

Nodes speak UDP with a one-command-per-datagram ceiling sized for a 1500 B
path (payload ≤ 1344 B cleartext / 1312 B encrypted — see
[Limits](../reference/limits.md)). At every rate we have published, the
bottleneck was CPU structure, never the NIC. Keep voters in one low-jitter
domain (same placement group / rack) — election timeouts and the commit
quorum both feel path jitter directly.

## What these numbers are not

All sweeps above are the direct shared-memory path at one operating point
(64 B payloads, inflight 4096, three voters). A gateway-fronted node, larger
payloads, or a WAN change the picture — re-measure on your own shape. The
honest-comparison rules are in
[Reproduce a published result](reproduce-a-result.md).
