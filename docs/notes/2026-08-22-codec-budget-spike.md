# Codec budget spike — how much does serde+bincode cost on the apply thread?

*Spike, 2026-08-22. Decision input for the M12 design (bytes-in/bytes-out
state-machine contract vs the typed serde trait). Dev-box numbers: ratios are
the result, absolutes are not a reported figure (see the dev-box rule in
CLAUDE.md / the M5 gate doc for where real numbers come from).*

## Question

Every command crosses the apply boundary as bytes (`payload: &[u8]` off the
log-buffer frame, `uc2_service/src/apply.rs`), and every response leaves it as
bytes, but the `StateMachine` trait is typed: the framework bincode-decodes
`S::Command` before `apply` and bincode-encodes `S::Response` after it. How
much of the apply thread's budget is that, as the payload grows from 64 B to
the ~1.3 KB a single datagram admits (`MTU_DEFAULT = 1408`,
`uc_protocol/src/v2/datagram.rs:18`, not operator-configurable)? And is the
*format* (bincode vs SBE) the cost, or the typing?

## Part 1 — isolated codec ladder (hi-perf-cmp)

`hi-perf-cmp` branch `serialization-uc-kv-ladder`, note
`docs/notes/2026-08-22-uc-kv-codec-ladder.md`. A UC-shaped single-command
frame (header + `op/flags/ttl/key/value`), value 64 B → 8 KiB, five Rust
cells: hand-laid raw frame, SBE (`sbe_gen` schema, zero-copy view), bincode as
UC calls it (`Vec<u8>` fields, `encode_to_vec`), bincode with `bytes::Bytes`
fields (byte-identical wire — asserted), bincode encoding into a reused slice.
Batch means, ns per command:

| value B | raw enc / dec-owned | SBE enc / dec-owned | bincode `Vec<u8>` enc / dec | bincode `Bytes` enc / dec |
|---:|---:|---:|---:|---:|
| 64   | 6 / 28   | 6 / 27   | 151 / 101   | 121 / 44  |
| 256  | 7 / 34   | 8 / 34   | 295 / 223   | 131 / 65  |
| 1024 | 18 / 44  | 20 / 45  | 653 / 691   | 118 / 84  |
| 4096 | 56 / 127 | 58 / 128 | 1950 / 2609 | 191 / 175 |
| 8192 | 105 / 249| 105 / 248| 3701 / 5170 | 248 / 309 |

Zero-copy *view* decode (raw or SBE): 16 ns @64 B … 143 ns @8 KiB, 0 alloc.

Findings: **SBE costs nothing over a hand-laid frame (0.98–1.14×) and saves
nothing** — its case would be schema evolution, not speed. **bincode as UC
calls it is 25–42× raw on encode and up to 21× on decode, and none of that is
the format**: serde types `Vec<u8>` as a *sequence of u8* so bincode walks the
payload element-wise, and `encode_to_vec` allocates per frame. `bytes::Bytes`
(or `serde_bytes`) fields give the identical wire at 1.2–1.9× raw. Above
~1 KiB the only cost left is the payload *copy* (view vs owned).

## Part 2 — budget share inside UC (`m5_gate all`, local smoke)

Feature `uc2_service/apply-profile` (this commit; zero-cost when off): rdtsc
probes around decode / `apply` / response publish on the apply thread,
summary to stderr. `m5_gate all --secs 6 --payload N` on the 4-vCPU dev box
(3 nodes + 3 services + client in one process — oversubscribed, so absolute
throughput and latency are meaningless here; per-frame costs and shares are
the reading). `CountSm` is the gate's FSM: `Command = Vec<u8>`, `apply`
ignores the bytes and counts, `Response = u64`.

| payload | arm | decode ns/frame | publish ns/frame (encode) | codec share of apply-thread cycles | responses/s (dev box, smoke only) |
|---:|---|---:|---:|---:|---:|
| 64 B   | `Vec<u8>` (today) | 113  | 42 (4)  | 56 % | 324 k |
| 64 B   | `Bytes`           | 13   | 40 (4)  | 17 % | 256 k |
| 256 B  | `Vec<u8>`         | 433  | 103 (4) | 71 % | 147 k |
| 256 B  | `Bytes`           | 38   | 86 (4)  | 21 % | 150 k |
| 512 B  | `Vec<u8>`         | 843  | 110 (4) | 78 % | 83 k  |
| 512 B  | `Bytes`           | 39   | 91 (4)  | 15 % | 71 k  |
| 1024 B | `Vec<u8>`         | 1601 | 131 (4) | 85 % | 40 k  |
| 1024 B | `Bytes`           | 54   | 121 (4) | 16 % | 40 k  |

("codec share" = decode + response-encode cycles over all cycles spent inside
`apply_cycle`, summed over the three services; the response encode of a `u64`
is 4 ns, the rest of "publish" is the egress ring write.)

Findings:

1. **With today's `Vec<u8>` typing the apply thread is decode-bound**: 56–85 %
   of its cycles, ~1.5 ns per payload byte, matching Part 1's element-wise
   decode. A one-type change (`bytes::Bytes`, byte-identical wire) removes
   ~90 % of it (decode 13–54 ns) and the share drops to 15–21 %, most of which
   is now the payload copy + `Vec` allocation of an owned `Bytes`.
2. **End-to-end throughput did not move on this box**, either arm: the ladder
   sits on a ~20–40 MB/s bytes-per-second ceiling that is the replication /
   fsync pipeline under 4-vCPU oversubscription, not the apply thread. So the
   codec is a real cost that is currently *hidden* behind a slower stage
   locally; the fleet decides whether it is binding there.
3. **Where it becomes binding**: a single apply thread's ceiling under today's
   decode is ~1/181 ns ≈ 5.5 M/s at 64 B (not binding at fleet rates of
   ~1 M/s) but ~1/981 ns ≈ 1.0 M/s at 512 B and ~0.6 M/s at 1 KiB — i.e. at the
   M5 fleet rate the apply thread saturates somewhere between 512 B and 1 KiB
   commands. With `Bytes` the ceiling is ≥ 5 M/s at every size the datagram
   admits; with a zero-copy (borrowed slice) contract it is `apply` itself.
4. `m5_gate`'s `CountSm` — the gate that produced every published M5/M8/M10
   number — has been paying a full decode to ignore the bytes. The published
   rates are therefore *conservative* with respect to this cost.

## Decision (pre-committed rule: seam + zero-copy path if codec ≥ 10 % of the
apply budget at any payload ≤ 1 KiB — it is 56–85 %, or 15–21 % after the
cheap fix)

- **M12a lowers the state-machine contract to bytes-in/bytes-out**
  (`apply(&mut self, position, cmd: &[u8], out: &mut Vec<u8>)`, the `out`
  buffer reused by the framework; same for `query`), with today's typed serde
  `StateMachine` kept as a blanket adapter on top — zero change for existing
  state machines, zero decode and zero allocation for one that wants none,
  SBE flyweights usable directly without UC depending on SBE. This is also
  what makes the gateway's "opaque command bytes" truly opaque (polyglot) and
  lets the session envelope be a fixed 16-byte prefix peeled at the raw layer.
- The typed adapter keeps bincode — the format is not the cost — but the docs
  tell users to type blobs as `bytes::Bytes`/`serde_bytes`, and the response
  path stops allocating per publish (reuse the buffer).
- No SBE in the framework. Users who want SBE write a raw state machine.
- The `apply-profile` feature stays (off by default) as the tool that measures
  this; the fleet gate for M12a re-runs the M5 ladder with it on to state the
  real share.

## Caveats

Dev box; in-process `all` mode; 3 services share one set of global counters
(the shares are aggregates); `rdtsc` measures wall cycles including
preemption, so per-frame ns are upper bounds under oversubscription; the
`Vec<u8>` arm and the `Bytes` arm were separate runs minutes apart. None of
this is a gate number.

## Post-fix smoke (Task 4)

M12a Task 4 added `m5_gate`'s raw `CountSm` twin (`RawCountSm`, `--raw-sm`) —
the typed/raw A/B this note called for. `cargo run -p uc2_node --release
--example m5_gate --features uc2_service/apply-profile -- all --secs 6
--payload 509`, with and without `--raw-sm` (`--payload 509` rather than the
task's nominal 512: `NODE_MAX_PAYLOAD` in `m5_gate.rs` is a hard 512 B door
enforced at `try_submit` and a 509 B raw command bincode-encodes to exactly
512 B (`Vec<u8>`'s 3-byte length-varint overhead at this size) — `avg_payload`
below reports the post-encode 512 B, matching this note's Part 2 table
convention). Same dev box, same "3 services share one global counter set,
report repeats 3x" caveat as Part 2 — **smoke (dev box, 4 vCPU), not a gate
number**:

```
typed (CountSm, StateMachine + bincode):
apply-profile[final] frames=1369266 avg_payload=512B per-frame: sm_apply=731ns publish=120ns batch_arm=864ns | sm_apply/batch_arm=84.7% sm_apply/apply_cycle_total=75.8% batch_arm/apply_cycle_total=89.5% apply_cycle_calls=160274

raw (RawCountSm, --raw-sm):
apply-profile[final] frames=1449796 avg_payload=512B per-frame: sm_apply=12ns publish=82ns batch_arm=107ns | sm_apply/batch_arm=11.3% sm_apply/apply_cycle_total=5.8% batch_arm/apply_cycle_total=51.1% apply_cycle_calls=173273
```

731 ns → 12 ns (61x) confirms the Part 2 prediction directionally (this note
expected "a few ns" raw vs. "≈800 ns" typed at 512 B — 12 ns and 731 ns land
in the same ranges) on a busier box than the original spike (this run carries
the M12a raw/typed dispatch machinery and a slightly different payload
convention, so the absolute ns are not directly comparable to Part 2's table
row-for-row). `sm_apply/apply_cycle_total` drops 75.8% → 5.8%, the same shape
as Part 2's `Bytes`-arm finding: removing the typed decode/encode does not
move end-to-end `responses/s` on this box (73,847 → 79,874, both far below
the 400k gate bar and both `RESULT: FAIL (honest)`, expected per the
dev-box-is-not-a-bench rule) — the codec cost is real and now near-zero for
the raw tier, but this box's bottleneck lives elsewhere, same conclusion as
Part 2. The gate doc (Task 12) cites this section for the M12a spec §4.6
item 5 codec-share smoke number, ahead of the fleet run that states the
real one.
