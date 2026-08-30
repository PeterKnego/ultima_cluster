# ultima_cluster

A clustered application server, built on strong [core principles](CORE_PRINCIPLES.md), 
allowing you to write applications that are correct, resilient and high performance.
Built on the state-of-the-art State Machine Replication architecture, with a full non-blocking design that can perform 1.5M queries per second with response time of p99 <1ms.

## What it is

Ultima_cluster is a State Machine Replication application server. You write a deterministic state machine; it runs your state machine on every node in a cluster, applying the same commands in the same order, and survives node failure without losing acknowledged writes. A replicated command log is what drives every change in the user-supplied state machine. Read more in [Architecture](/docs/ARCHITECTURE.md).

## Why?

SMR is used when you need ultimate performance, correctness and resiliency at the same time. All state is held and manipulated in memory (only occasional snapshots are persisted) and correctness is guaranteed via guaranteed order of commands across all state machines in the cluster.

### High-perf: 1.5 M responses/s at p99 0.905 ms

End to end through the SDK — client submit, consensus, apply, response — with
**every operation quorum-fsync'd before it is acked** and reads linearizable.
p50 0.653 ms, p90 0.757 ms. Measured on 3 × `c6id.2xlarge`, 64 B payloads,
through the **pipelined client** (`uc2_client`'s public `Engine`) that ships
today.

### Leader failover p50 202 ms, zero committed loss in 10 of 10 kills

p90 279 ms, worst 394 ms. Measured on a **4-vCPU sandbox over loopback, not the
fleet** — failover here is timeout-dominated and real NVMe fsync is faster than
the sandbox's ext4, so this is a conservative upper bound; the fleet
detection-timing confirmation is still outstanding.
→ [M4 gate record](/docs/benchmarks/uc2-m4-gate-2026-07-11.md)

## Try it

Download the signed `v2.8.1` tarball and run its quickstart — no toolchain
needed:

```bash
tar xzf uc2-2.8.1-x86_64-unknown-linux-gnu.tar.gz
uc2-2.8.1-x86_64-unknown-linux-gnu/packaging/quickstart-local.sh
```

Three nodes, three services and three gateways come up on this host, a real
election happens, writes are committed by a majority and a linearizable read
comes back through a gateway. It prints `PASS` and cleans up after itself.
**[`docs/QUICKSTART.md`](/docs/QUICKSTART.md)** has the download-and-verify
step, the annotated configs, and the path onto real hosts.

## Correctness

Three tiers, kept clearly apart because the words mean different things:

- **Proved** (Lean 4, sorry-free): the consensus safety kernels, `election_safety`
  and `log_matching`; 100,000+ vectors of real Rust output replayed through
  the model bit for bit. `leader_completeness` remains **open**.
- **Checked:** nine cluster invariants under seeded fault fuzz, WGL
  linearizability under leader kills / partitions / purge, Elle transactional
  safety with a mutation tier, multi-process `SIGKILL` recovery, `loom`.
- **Bug-hunted only:** bounded model checking — never the proof of record.

The proof effort found and fixed **four real, shipped safety bugs** the fuzz
and crash tiers had missed. Full picture, including what is *not* verified:
**[`docs/VERIFICATION.md`](/docs/VERIFICATION.md)**.

## Documentation

- **[Core Principles](/CORE_PRINCIPLES.md)** — correctness, resiliency, high
  performance, in plain language.
- **[Architecture](/docs/ARCHITECTURE.md)** — how it works, and the crate map.
- **[Quickstart](/docs/QUICKSTART.md)** — zero to a running three-node cluster.
- **[Verification](/docs/VERIFICATION.md)** · **[Benchmarks](/docs/BENCHMARKS.md)**
  — what is proved, what is measured, on what.
- **[How-to guides](/docs/how-to)** · **[Reference](/docs/reference)** ·
  **[Operations runbook](/docs/ops/uc2-runbook.md)** · **[API docs](https://peterknego.github.io/ultima_cluster/)**.
- **[Limits](/docs/reference/limits.md)** — every hard limit, standing
  constraint and accepted residual, each linked to the doc that owns it.
- **[RELEASES.md](/RELEASES.md)** — every feature, release by release, each
  linked to its detailed doc.

What shipped since the core, and where to read about it:

| Release | Feature | Doc |
|---|---|---|
| v2.1.0 (M7) | Change membership live, one member at a time, under load | [Change cluster membership](/docs/how-to/change-cluster-membership.md) |
| v2.3.0 (M8) | Opt-in encrypted and authenticated node-to-node traffic | [Encrypt node traffic](/docs/how-to/encrypt-node-traffic.md) |
| v2.3.0 (M9) | The `uc2-node` daemon: one TOML file per host, named startup refusals, systemd | [Run a cluster](/docs/how-to/run-a-cluster.md) |
| v2.4.0 (M10) | `/metrics`, `/healthz`, `/readyz`, alert rules and a dashboard | [Monitor a cluster](/docs/how-to/monitor-a-cluster.md) |
| v2.5.0 (M11) | Offline backup / verify / restore; recovery after losing a majority; full-disk fail-stop | [Back up](/docs/how-to/back-up-a-cluster.md) · [Recover from quorum loss](/docs/how-to/recover-from-quorum-loss.md) |
| v2.6.0 (M12) | Gateway + remote client for processes off the node host; signed admin requests with an audit log; signed release artifacts; security package | [Run a gateway](/docs/how-to/run-a-gateway.md) · [Admin authentication](/docs/notes/uc2-admin-authentication.md) · [Security](/docs/security) |
| v2.7.0 (M13) | Remote path at the cluster's own speed: a rebuilt remote client, a convoy-free ingress ring, a global gateway credit budget | [Remote protocol](/docs/reference/remote-protocol.md) · [M13 gate](/docs/benchmarks/uc2-m13-gate-2026-08-24.md) |
| v2.8.0 (M14) | Several state machines per cluster, fed by one log; submit to any, or fan a query across all | [How it works](/docs/notes/uc2-m14-multi-service-explained.md) · [M14 gate](/docs/benchmarks/uc2-m14-gate-2026-08-29.md) |
| v2.8.1 (M14c2) | The multi-service proof pass: linearizability, partition, hard-crash and Elle capstones run with two state machines | [What is verified § 11](/docs/VERIFICATION.md#11-what-is-not-verified) · [Lockstep envelope](/docs/benchmarks/uc2-m14c2-lockstep-oversubscription-2026-08-30.md) |

Building from source: [Quickstart](/docs/QUICKSTART.md) (from-source section);
developer build/test/lint commands are in [`CLAUDE.md`](/CLAUDE.md).

## Security

Out of the box, nodes trust their network: traffic between them is neither
encrypted nor authenticated, so run them on a private network you control.
Turn on wire crypto and every node proves its identity to every other with a
key you issued, and traffic between them cannot be read, altered or replayed
by anyone on the network path. What it does **not** protect against: someone
who has taken over a node host, or a legitimate cluster member gone rogue —
they hold the keys.

Administrative commands are signed, and every one is written to an
append-only audit log before it is answered. Every place the system reads
bytes it did not write is listed, guarded, and fuzzed.

[Threat model](/docs/security/threat-model.md) ·
[attack surface](/docs/security/attack-surface.md) ·
[self-assessment](/docs/security/self-assessment.md) ·
[reporting a vulnerability](/SECURITY.md).

## Scope 

What is shipped, release by release — each feature with a link to its
detailed doc — lives in **[RELEASES.md](/RELEASES.md)**. What it will not
do, and what it does only under conditions — member and payload ceilings,
flag-day upgrades, the crypto and admin residuals, what is not verified —
is collected in **[Limits](/docs/reference/limits.md)**.

## License

Apache-2.0
