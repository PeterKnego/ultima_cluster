# uc_adjudicate — the dogfood adjudication harness

Maintainer-side test apparatus for wayfinder map #16 ("Dogfood KV store").
It decides whether a service binary built by a clean-room **builder** is
correct — by *this repo's own checkers*, never by the builder's tests —
treating the binary as a black box behind `uc2-gateway` and the `uc_remote`
protocol. It is repo test apparatus, invisible to the personas (charter
decision 11), and every gate row of
`docs/benchmarks/uc2-dogfood-kv-gate-2026-09-15.md` is a subcommand of the
`uc2-adjudicate` binary.

This is **not** a persona sandbox. It links the in-tree crates (`uc_lincheck`,
`uc_remote`, `uc_log`, `uc_protocol`), reads source, and runs on the
maintainer's box or fleet — everything the clean-room forbids the builder.

## What is fixed and what is plugged in

Fixed: the cluster rig (release `uc2-node`/`uc2-gateway`/`uc2ctl` from a
tarball's `bin/`), the workload generators, the checkers and the oracles.
Plugged in: the **service binary's path** and an **encoding adapter**
(`src/adapter.rs`), one per service wire format. The adapters this build
knows are `kv-v1` (the builder's v1 KV) and `register` (the reference
`uc_crashtest-service`, for the paired rate arm and a with-teeth self-check).
The v2 adapter is added after the v2 builder run, from the v2 wire page.

## The rows

| subcommand | gate row | what it does |
|---|---|---|
| `wgl` | B2.i / B2.ii | per-key WGL linearizability through `uc_remote` under leader kills; the acked-write-loss oracle; live digest agreement across replicas |
| `wgl --churn` | B2.iii | the above plus coordinated instants, below-floor service restarts and one wipe-and-rejoin, with **≥ 1 snapshot install observed and counted** (a seed with none is NOT RUN, per the gate) |
| `elle` | B2-v2.iv | an Elle list-append history through the remote path (`history.edn`), adjudicated by `scripts/dogfood_elle.sh` under both `serializable` and `strong-serializable`; NOT RUN against a v1 adapter |
| `known-keys write` / `verify` | B4.ii | write a known set before the upgrade, read every acknowledged value back after |
| `diverge` | B4.i | live digest agreement across every gateway's replica |
| `diff-snapshots` | B4.i (offline) | key-level difference of two row-0 artifacts at one instant |
| `rate` | B3 (paired arm) | steady-window Put rate + p50/p99 through one driver (the `m12_gate` warmup/measure window), the register-SM pair for B3 |

Every subcommand prints one JSON result line on stdout (the evidence pointer a
gate cell cites) and a human summary on stderr, and exits **0 PASS / 1 FAIL /
3 NOT RUN / 2 usage-or-setup-error** — the gate's four correctness outcomes
(convention 4), with no "inconclusive" on a correctness row.

## What the encoding adapter needs from the builder's wire-format page

The `Adapter` trait (`src/adapter.rs`) is the contract between the harness and
a service's published wire format. To write one (as `src/kv_v1.rs` was written
from the builder's `WIRE-FORMAT.md`), the page must state, and only these:

- the **command** layout of Put, Delete and CAS, and the **query** layout of
  Get and the digest — byte for byte, including the op discriminant;
- the **reply** status byte and the bytes after it **per (request, status)**;
- what a **version** is (here a log position; `0` = absent for CAS) and that
  each write's reply carries the new one — the harness's CAS material;
- the key and value **bounds**, so the generator never sends an over-cap frame;
- a **digest** query returning `(count, state-hash, last_applied)` — the
  divergence check; the hash must be order-independent so two replicas with
  the same entries agree;
- (v2) the **Append** command and **list read**, for the Elle row;
- the **snapshot image** layout, for the offline `diff-snapshots` check.

The page does **not** need to describe the remote protocol, the session
envelope, or the `Sessioned` tag — the gateway owns all three and the harness
speaks `uc_remote` for them.

## Two findings this harness produced on its first runs

Recorded here because they are exactly what a black-box adjudication is for.

1. **The builder's snapshot-image page omits the framework's session
   prefix.** `WIRE-FORMAT.md` § 5 states the on-disk artifact is
   `ULTSNAP2 ‖ P ‖ version ‖ image`. It is not, for the deployed
   configuration: the KV runs
   `Sessioned<KvSm>` (required for exactly-once, `[session] envelope = true`),
   and `uc_service::Sessioned::stream_snapshot` writes `u64 blob_len ‖
   session-table blob` BEFORE the inner image. The real artifact is
   `ULTSNAP2 ‖ P ‖ version ‖ session_blob_len:u64 ‖ session_blob ‖ kv_image`,
   with the session blob's length at **offset 24** (the envelope is 24 bytes
   since `2.13.0`; it was the 16-byte `ULTSNAP1 ‖ P` before).
   `diverge::read_artifact`
   strips the prefix by its length; the page is a doc defect for the operator
   ledger (`§ 5` is right only for a non-`Sessioned` service).

2. **A coordinated snapshot instant under concurrent ingress load fail-stops a
   node with `IngressRingCorrupt`.** On loopback (where MTU discovery engages
   the 8896 B jumbo rung), a churn run — periodic `uc2ctl snapshot` instants
   while a filler writes at a bounded window — drove nodes to
   `consensus fatal (fail-stop): IngressRingCorrupt ring=ingress`. It
   reproduces **without any leader kills** and only when both the instants and
   the ingress load are present (churn with no load filler never fail-stops).
   The reported "commit word length" decodes to a KV command header
   (`0x030101` = `format=1 op=1 key_len=3`), i.e. the ingress MPSC consumer
   desynced from record boundaries and read command bytes as a length prefix.
   The harness's own input is well-formed (WGL is Linearizable, acked-loss 0),
   so the desync is inside UC's ingress path, not the workload. **This is a
   candidate product defect for the map's triage (ticket #24), not a harness
   bug** — the harness correctly detects the node exit and reports FAIL. It is
   why a clean B2.iii PASS could not be shown on this dev box; the churn
   machinery is otherwise proven (instants commanded, 9 installs observed and
   counted, wipe-and-rejoin executed, per-key Linearizable throughout).

## Running it

```bash
cargo build --release -p uc_adjudicate
UC2_BIN_DIR=/path/to/uc2-2.13.0-…/bin \
  target/release/uc2-adjudicate wgl \
    --adapter kv-v1 --service-bin /path/to/kv-service --seed 1 --secs 20
# churn (B2.iii); needs a tarball bin dir and the builder's kv-service:
… wgl --adapter kv-v1 --service-bin … --churn
# self-check against the reference register SM (no third-party binary needed
# beyond the in-tree one):
cargo build --release -p uc_crashtest --bin uc_crashtest-service
… wgl --adapter register --service-bin target/release/uc_crashtest-service
```

Scratch (instance dirs, logs, `history.edn`) goes under
`$HOME/scratch/uc2-adjudicate/` by default — real disk, never `/tmp`
(tmpfs OOM; see `CLAUDE.md` § Local scratch). `--root` overrides it.
