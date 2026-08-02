# UC v2 — Workload-envelope map (payload-size ladder): measurement brief

**Date:** 2026-08-02
**Status:** Draft brief, written as a session handoff — **not yet reviewed, no
measurement approved or run.** The §5 predictions and §6 jumbo rule become a
pre-commitment when this file is committed unchanged ahead of the first fleet
run; adjust in review before that run, never after seeing data.
**Motivation:** Every end-to-end UC number exists at exactly one payload size:
64 B. The bytes-bound regime has only ever been measured at the replication
layer (M2: 235–323 MB/s durable per follower), never through the full SDK
pipeline. This map finds where the write path crosses from ops-bound to
bytes-bound, what resource binds each regime, and what the *sustained* —
not burst — bytes-bound plateau is.
**Relationship to other work:** consumes reference lines from the sibling
`hi-perf-cmp` repo's streaming-throughput spec
(`hi-perf-cmp/docs/superpowers/specs/2026-08-02-network-payload-streaming-design.md`,
drafted the same day); independent of the net-decomp brief
(`2026-08-02-uc2-net-decomp-brief.md`) — that one attributes the 64 B budget,
this one maps the envelope. Neither blocks the other.

---

## 1. The constraint that shapes everything

**The replication plane requires a frame to fit in one datagram.**
`Sender::new` hard-asserts `HEADER_LEN + max_payload` (plus crypto overhead
when on) fits `cfg.mtu` — the assert message itself names the escape hatch:
*"raise mtu — the jumbo-frame knob"* (`uc2_net/src/sender.rs:419-427`). There
is no frame fragmentation across datagrams. Consequently the reachable
envelope has three regimes:

1. **64 B → 1360 B** — measurable today. 1360 = `MTU_DEFAULT` 1408 − 16
   (datagram header) − 32 (frame header). `m5_gate` already carries a
   `--payload` knob; only its harness constant `NODE_MAX_PAYLOAD = 512`
   (`m5_gate.rs:279`) caps the sweep, and that constant exists purely to
   satisfy the sender assert at default MTU.
2. **→ ~8.8 KiB** — reachable with the existing jumbo knob (AWS VPCs support
   MTU 9001 intra-VPC; UC `mtu ≈ 8900` after IP/UDP headroom). Still
   measurement-only: no `src/` change.
3. **Beyond** — a *feature* (multi-datagram fragmentation/reassembly), not a
   sweep. Out of scope (§9), pre-scoped there as the follow-on this map's
   numbers would justify or kill.

The IPC rings cap messages at 64 KiB (`uc2_node/src/node.rs:4005`), so the
shmem plane is not the binder anywhere in regimes 1–2; the log buffer's
capacity ≥ 4× max-claim assert (`uc2_log/src/buffer.rs:104-107`) is trivially
satisfied at 256 MiB harness buffers.

And the arithmetic says the interesting physics is early: at M5's 1.64 M
ops/s, 1 KiB payloads would mean ~1.6 GB/s × 2 followers out of the leader —
several times the `c6id.2xlarge` NIC's ~3.1 Gbps *baseline* (12.5 Gbps is
burst). The ops→bytes crossover almost certainly sits inside regime 1.

**Explicit non-goal: this is an instrument, not a gate.** No bar, no
pass/fail. Outputs are the envelope curve, a binder attribution per regime,
and one narrow pre-committed disposition (§6, jumbo as default fleet config).

## 2. Grid

Two MTU arms × payload ladder, write path only, crypto OFF (an optional
crypto-ON rung may be recorded as a labelled diagnostic, never feeding §5/§6):

- **Standard arm (MTU 1408):** payloads {64, 256, 512, 1024, 1360}.
- **Jumbo arm (MTU 9001 / UC mtu ~8900):** payloads {64, 1024, 2048, 4096,
  8192}. Jumbo deliberately re-runs *small* payloads: a bigger datagram packs
  more 64 B frames per syscall, so jumbo may move the ops-bound end too, and
  §6 needs the 64 B comparison anyway.

Rungs are 60 s. **Plus one soak rung per arm** (≥ 600 s, the arm's largest
payload, max rate) to walk through the EC2 burst-credit window — the soak's
final-quarter throughput is the sustained-plateau claim.

Reads stay out entirely: the read path has its own open lead (the ~540 k
shared ceiling) and mixing the two instruments would blur both.

## 3. Harness shape

New instrument example `uc2_node/examples/envelope_map.rs`, following the
`read_profile`/net-decomp precedent (gate-role split `node`/`service`/
`client`/`all`/`decide-free report`; per-rung JSON lines; local runs verify
wiring and produce no numbers) rather than growing `m5_gate` — `m5_gate` is a
gate with a pre-committed bar and should not double as an instrument.

Harness-level requirements (no `src/` change anywhere):

- **`NODE_MAX_PAYLOAD` and `--mtu` move in lockstep per rung** — the sender
  assert ties them; the harness sets both from the rung definition.
- **The client's in-flight window is sized in *bytes*, not ops.** An op-count
  window (`m5_gate`'s `--inflight 4096`) at 8 KiB offers a completely
  different load than at 64 B; the envelope's independent variable must be
  offered bytes with a depth cap, and the admission window
  (`--admission-kib`, cnc `admission_bytes`) scales with it per rung.
- **Responses stay small** (position-keyed acks, the `m5_gate` matcher
  pattern), so the egress broadcast is not part of the swept variable — state
  this in the report rather than leaving it implicit.
- Per-rung health captured in-band: inflight-at-end (must be 0 for a valid
  rung), NAK counts, admission-closed time, archive/fsync stats, first-half
  vs second-half throughput split.
- The orchestrator (`bench-infra`) snapshots ENA allowance counters
  (`ethtool -S`: `bw_out_allowance_exceeded`, `bw_in_allowance_exceeded`,
  `pps_allowance_exceeded`) around every rung; a nonzero bandwidth delta
  labels the rung **throttled** — its number is a burst observation, and only
  soak-sustained numbers may be quoted as the plateau.

## 4. Fleet

**Same class as every UC number: 3 × `c6id.2xlarge`, cluster placement group,
journals on instance-store NVMe.** The alternative considered — an extra
stronger-NIC arm (e.g. `c6in`) to see the envelope without the NIC allowance
in the way — is **dropped**: the isolated ceiling question is exactly what the
hi-perf-cmp streaming spec measures on the same class, cheaper, on a 2-node
fleet. Same-class-only keeps every rung comparable with M5.

Fleet cost and approval follow the standing rule (separately user-approved
`terraform apply`); the soak rungs make this run longer than a gate run, and
the provisioning session can be shared with the other outstanding fleet arms
(M8 A/B, M4, net-decomp) where scheduling allows.

## 5. Pre-registered predictions

The instrument-grade substitute for a gate bar: falsifiable statements
committed before the run. A wrong prediction is a finding, not a failure.

- **P1 (crossover):** the ops→bytes crossover on the standard arm lies
  between 256 B and 1 KiB.
- **P2 (plateau):** the bytes-bound sustained plateau ≈
  `min( isolated-socket sustained goodput ÷ 2 followers, NVMe write
  bandwidth, archive record+fsync throughput )`, where the first term comes
  from hi-perf-cmp's `goodput_sustained` on the same instance class **if that
  run has landed**, else the ~3.1 Gbps baseline spec figure (the report says
  which anchor it used).
- **P3 (jumbo):** jumbo improves the bytes-bound end and does not regress
  64 B (fewer syscalls per byte at every size).
- **P4 (burst):** second-half throughput < first-half on bytes-bound standard
  rungs (credit decay); the soak's final quarter is materially below the
  60 s rung numbers at the same size.
- **P5 (tie-back):** the 64 B standard rung reproduces M5's plateau within
  noise on the same hardware class — if it does not, the run does not speak
  for the M5 record and no other rung is read.

## 6. One pre-committed disposition: jumbo as default fleet config

> **Jumbo MTU (9001) becomes the recommended fleet configuration in the
> runbook iff both hold:** the jumbo arm's soak-sustained bytes-bound plateau
> exceeds the standard arm's by **≥ 15 %**, **and** the jumbo 64 B rung is
> within **−3 %** of the standard 64 B rung (throughput and p99). Otherwise
> jumbo remains a documented knob.

Borderline (10–20 % on the first clause) is resolved only by a re-run or
treated as not justified — never by local smoke.

## 7. Threats to validity

1. **Burst credits** — the headline threat; defended by ENA deltas, the
   half-split, and the soak rungs (§3). No 60 s bytes-bound number is a
   plateau claim.
2. **Path-MTU blackhole on the jumbo arm** — a do-not-fragment probe runs
   before the arm; failure aborts the arm loudly instead of surfacing as
   mystery NAK storms.
3. **The client as the ceiling** — per the read-profile threat-3 precedent:
   sampled in-flight depth (bytes and ops) per rung, mean and minimum against
   target; a rung whose client did not sustain its offered load describes the
   harness. At 8 KiB × high rate the single-threaded client memcpy is a real
   suspect; report client-process CPU alongside.
4. **NAK/repair contamination** — UC repairs loss, so a rung driven into
   overrun/NAK territory measures the repair path; NAK counts label such
   rungs, and their numbers are reported as "with repair active", never
   pooled with clean rungs.
5. **Admission/window artifacts** — an admission window that closes
   frequently at large payloads throttles offered load invisibly;
   admission-closed time is reported per rung and the window scaled per §3.
6. **Archive/fsync shape shifts with payload** — larger frames fill ≤1 MiB
   archive blocks faster (fewer frames per fdatasync); archive stats per rung
   make this visible so a storage binder is attributed with evidence, not
   inferred.
7. **Shared-box smoke is not evidence** — the standing §3.1 discipline of the
   read-profile spec applies verbatim; no local number reaches the report.
8. **Predictions are drafts until ratified** (§Status). Changing one after
   data exists voids the pre-registration and the record must say so.

## 8. Output

A report under `docs/benchmarks/uc2-envelope-map-<date>.md`, gate-doc-shaped:

- the full curve, both arms: ops/s, payload MB/s, wire MB/s per follower,
  p50/p99 per rung, with per-rung health and ENA deltas;
- the crossover point and a binder attribution per regime (NIC allowance /
  NVMe / archive / admission / client), each backed by the §3 health data;
- §5 evaluated prediction by prediction; §6 evaluated clause by clause with a
  verdict line;
- every threat in §7 addressed with what the run showed;
- an index row in `docs/BENCHMARKS.md`.

## 9. Out of scope

- **Frame fragmentation (> ~8.8 KiB commands).** Pre-scoped for the record:
  it would touch the sender's one-frame-one-datagram invariant, NAK
  granularity, and the log-buffer claim path — a transport design project
  that only the map's numbers *plus actual demand* for large commands should
  open. Nothing else here prepares it.
- Reads, and the read-only ceiling investigation (separate lead).
- Crypto-ON beyond one labelled diagnostic rung.
- The net-decomp brief's K/L technology questions and the hi-perf-cmp
  technology cells.
- WAN / cross-region anything.

## 10. Handoff state (for the next session)

Sequencing agreed in the originating conversation:

1. **hi-perf-cmp first (recommended, not blocking):** run its `network-rtt`
   payload ladder + `network-throughput` streaming cells on the 2-node fleet
   (same instance class) so P2 anchors to a measured isolated ceiling. Its
   spec is drafted at the path in the header — note it was left
   **uncommitted** in that checkout (mid-work on another branch); commit it
   there on its own branch first.
2. This map on the 3-host fleet, same class as M5.
3. The fragmentation feature only if the map plus demand justify it.

Open decisions for the implementing session: ratify ladder sizes, rung/soak
durations, the §6 thresholds, and the §5 prediction set; confirm harness
placement (`envelope_map.rs` default); decide fleet-session sharing with the
other outstanding arms.

Next step per house workflow: review this brief, then
`superpowers:writing-plans` for the harness.

## Appendix — code anchors

| Concern | Location |
| --- | --- |
| One-frame-one-datagram assert ("raise mtu") | `uc2_net/src/sender.rs:419-427` |
| Harness payload cap (512, MTU-driven) | `uc2_node/examples/m5_gate.rs:275-279` |
| Existing `--payload` / `--inflight` knobs | `uc2_node/examples/m5_gate.rs:10` |
| `MTU_DEFAULT = 1408` | `uc_protocol/src/v2/datagram.rs:18` |
| Log-buffer max-claim capacity assert | `uc2_log/src/buffer.rs:104-107` |
| Harness buffer size (256 MiB) | `uc2_node/examples/m5_gate.rs:274` |
| IPC rings: 64 KiB max message | `uc2_node/src/node.rs:4005` |
| Admission window (`admission_bytes`) | `uc2_node/src/node.rs:183`, cnc @3712 |
| Archive: ≤1 MiB blocks, fdatasync per block | `uc2_log/src/archive.rs` (module header) |
| M2 bytes-regime numbers (replication layer only) | `docs/benchmarks/uc2-m2-gate-2026-07-10.md` |
| Instrument-example precedent | `uc2_node/examples/read_profile.rs`; net-decomp brief |
| ENA counters / fleet orchestration | `bench-infra/scripts/m6_fleet_gate.py` |
