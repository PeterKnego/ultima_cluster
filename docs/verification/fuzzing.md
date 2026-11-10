# Fuzzing and Miri: decoders on untrusted bytes

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

This tier feeds random bytes into every decoder that reads data ultima_cluster did not write itself. It is bug-hunting evidence, not proof. A green run means no crash was found in a fixed time budget. A second tool, Miri, runs the same decoders and looks for undefined behaviour instead of crashes.

- **Location:** [`fuzz/`](/fuzz)
- **Runner:** [`scripts/fuzz_smoke.sh`](/scripts/fuzz_smoke.sh)
- **CI:** `nightly.yml`'s `fuzz-groups` and `fuzz` jobs

## Terms used on this page

- **Fuzzing** feeds a program a stream of generated inputs and watches for crashes.
- **libFuzzer** is the LLVM fuzzing engine that `cargo fuzz` drives. See the [libFuzzer docs](https://llvm.org/docs/LibFuzzer.html).
- **Coverage-guided** means libFuzzer keeps the inputs that reach new code and mutates those further.
- **Structure-unaware** means the target receives raw bytes, with no grammar telling the fuzzer what a valid message looks like.
- **Seed corpus** is the starting set of inputs the fuzzer mutates from.
- **Totality** means a decoder gives an answer for every possible input: a value or an error, never a crash.
- **A property target** checks a stated rule on every input, such as "decode then encode gives back the same value". It does this on top of the plain no-panic check.
- **Miri** is a Rust interpreter that runs tests step by step and reports undefined behaviour the compiled program would hide.

## What is being defended

Every place ultima_cluster parses bytes it did not write has a structure-unaware libFuzzer target behind it. The property under test is **totality**. For any input, the decoder returns a value or an error. It does not panic. It does not index out of bounds. It does not allocate a size an attacker chose.

A panic is not a memory-safety failure in Rust. On a node, though, a panic is a fail-stop. The datagram path runs on the receiver agent. `apply` runs on the service's apply thread. A panic in either place takes the process down. Availability is the thing being defended here.

## The twenty-seven targets

| Target | Seam, and why its input is untrusted |
|---|---|
| `uc_protocol_datagram` | `uc_protocol::v2::datagram`, which is the 16-byte header and every body reader. It is the **first code an unauthenticated UDP packet reaches**. With `[crypto].enabled = false` it is reached before any authentication at all. Three `2.11.0` seeds cover the `SNAP_BEGIN` shapes that release's flag day produced. `10-snap-begin-legacy-tail` is a body with a trailing carry, which the fixed-length reader must ignore rather than refuse. `14-snap-begin-v4` is the shipped layout. `15-snap-begin-cluster` has `service_id = 255`, the cluster artifact. The `SNAP_TABLE` body reader those seeds replaced is retired with kind 21. Since `2.13.0` the arm list also carries `read_snap_report_body`, which is kind 26 from plan B3. The target offers **every** reader the body of **every** input. So a 24-byte-exact decoder with reserved-byte, row-range and non-zero-position refusals is driven by bodies of every other length and shape, not only its own. |
| `uc_protocol_log_frame` | `uc_protocol::v2::frame::read_header`, driven behind the real caller's `len >= HEADER_LEN` guard. It is deliberately caller-guarded. The target pins the guard's contract rather than pretending the guard is absent. |
| `uc_protocol_timer_frame` | New in `2.11.0`. The TIMER body the apply loop decodes from a committed frame. It is guarded by length and total on any slice. |
| `uc_protocol_sched_record` | New in `2.11.0`. The 17-byte service→node schedule record the consensus agent decodes. It comes from a shared-memory ring any local process can write. |
| `uc_protocol_schedule_table` | New in `2.11.0`. The replicated schedule table. It covers the `SCHEDULE_TABLE` frame body every node decodes off the log. It also covers the bytes an operator stages in `schedules.pending` for the leader to read back. It is also a **property** target. The recurrence arithmetic `next_after`, `latest_at_or_before` and `arm` runs on the consensus agent, so it must be total for any rule and any clock. |
| `uc_protocol_cluster_frame` | From cluster-FSM plan 1. It drives `uc_protocol::v2::frame::read_cluster_prefix` and the kind-dispatched payload decoders: `config::decode_config`, `schedule::decode_schedule_table`, `settings::decode_settings`, `upgrade::decode_upgrade_pin` and `upgrade::decode_snapshot_report`. Together they decode the `CLUSTER` body every node reads off the log. The body is defined in spec §4.3. |
| `uc_protocol_settings` | From cluster-FSM plan 1. It drives `uc_protocol::v2::settings`, the codec of the replicated settings record from spec §6. It must be total on any slice. A decoded value must round-trip through re-encoding. |
| `uc_protocol_cluster_image` | From cluster-FSM plan 3, spec §4.8. It drives `uc_protocol::v2::cluster_image`, the cluster IMAGE codec. The codec moved out of `uc_node::cluster_fsm` so it can be fuzzed without `uc_node`. It must be total, CRC-checked and exactly framed. A decoded image round-trips through re-encoding. `uc_node_cluster_artifact` below still exercises the same decoder through the real `ClusterFsm::install_snapshot` entry point. |
| `uc_protocol_cnc` | `uc_protocol::v2::cnc`, the 8 KiB control page every attaching process maps and parses. Since M14a it includes the page-2 service-slot band and the 4032 pair. It is a file on disk that any local process with write access can corrupt. |
| `ring_mpsc_record` | The MPSC slot decision `classify_commit_word` and the record decoder `decode_record_slice` in `uc_protocol::ring::common`. This is what the node's consensus agent meets in a shared-memory ring any local process can write. |
| `uc_remote_frame` | `uc_remote::frame`, which is the gateway edge's 24-byte TCP frame header and every typed body decoder. Its input comes from any client that can open a socket to the gateway. |
| `uc_crypto_open` | `uc_crypto::seal::{open_in_place, open_detached}`. This is the AEAD envelope's framing arithmetic. It runs on attacker-chosen bytes *before* the tag has been verified. |
| `uc_crypto_handshake` | `uc_crypto::handshake::Peers::on_message`, the pre-auth Noise `IK` surface. With crypto enabled, this is the first thing in the process to see bytes from anyone who can reach the UDP port. |
| `uc_crypto_group_key` | `uc_crypto::group::GroupPlane::on_key_message`. Two distinct message shapes share datagram kind 20. So this is a decoder that must tell hostile inputs apart. |
| `uc_crypto_admin` | `uc_crypto::admin`, a **property** target over the M12b signed-tag layout. It checks canonical-length agreement and the sign/verify round-trip. It checks that tag bit-flips are rejected. It checks that a foreign key is rejected. |
| `uc_journal_record` | The segment header and record decoder of `uc_journal`. This is what crash recovery meets in a torn or corrupt segment after a power loss or a full disk. |
| `uc_journal_stable_value` | `uc_journal::stable_value`, the durable slots for the vote, the term map and the snapshot floor. Corruption here is a consensus-safety input, not merely a data-loss one. |
| `uc_service_session` | `Sessioned<S>`, the exactly-once envelope. It runs under a fuzz-derived, deliberately tiny `SessionConfig`. That makes client eviction, byte eviction and window trim all reachable. The target also covers the snapshot install path. |
| `uc_node_toml` | `uc_node::config_file::parse_str`, the `node.toml` parser. It sits behind every M9, M11 and M12b named startup refusal. |
| `uc_gateway_toml` | `uc_gateway::config_file::parse_str`, the gateway's whole named-refusal path. That includes the gateway's own `EdgeConfig::validate`. |
| `uc_node_http` | `uc_node::obs::http::route_raw`, the **unauthenticated** request parser for `/metrics`, `/healthz` and `/readyz`. |
| `uc_service_snapshot_envelope` | From coordinated snapshots. It drives `uc_service::snapshots::decode_snapshot_envelope`. That decodes the 24-byte `ULTSNAP2` header every artifact file has begun with since `2.13.0`. A pre-`2.13.0` `ULTSNAP1` header is refused by name as `EnvelopeError::Legacy`. It is not decoded as version 0. This is a pure decoder, total on any slice. It is the one check between a renamed, mis-copied or wrong-version artifact and a bad install. Such an install would silently leave a span of frames unapplied or cross a version boundary uncaught. |
| `uc_node_cluster_artifact` | `ClusterFsm::install_snapshot`. It covers the cluster IMAGE a below-floor joiner installs **by fiat** off a snapshot session. It also covers the image a restarting node reads off disk. CRC32 is a checksum, not a MAC. So every length-prefixed read behind it is attacker-chosen. |
| `uc_protocol_probe` | From jumbo spec §4.2. It drives the path-MTU probe codecs in `uc_protocol::v2::datagram`: `read_probe_rung`, `read_probe_ack_body` and `write_probe_ack_body`. Its scope is `Scope::Pairwise`. It is reached by any peer that can put a UDP datagram on the wire. It runs ahead of the commit rule that consumes the result. |
| `uc_protocol_status_body` | `uc_protocol::v2::datagram::{read_status_body, write_status_body}`, the fixed-length status body. It is a PROPERTY target. Anything the reader accepts must re-encode and read back equal. |
| `uc_node_tier_object` | From backup-tier spec §4. It drives `uc_node::tier::object::ObjectReader` over an arbitrary object stream. The stream has a 48-byte header and 1 MiB blocks with length and CRC. Both object codecs, `None` and `Zstd`, are covered, and the read is bounded to 64 MiB. It also drives `layout::SetLabel::parse` and `TierInfo::parse` on lossy UTF-8. The tier is a directory on a mount an operator points at. So a restore, `verify` or fork meets whatever bytes that mount holds. |
| `uc_protocol_compat` | From wire-level spec §6.1. It drives the level-aware Settings decode `decode_settings_at` and the datagram `wire` byte under a committed level the fuzzer chooses. It checks that nothing above the level is accepted. It is a PROPERTY target alongside the no-panic one. |

## Method

### A deterministic seed corpus

Seeds are generated from fixed literals by the real encoders. The command is `cargo +nightly run --bin seed-corpus`. So the committed corpus is deterministic, and a corpus change is reviewable in a diff.

### The nightly run

Nightly CI runs every target for **600 seconds** on the committed seed corpus, across six matrix legs. A crash fails the leg and uploads the crashing input as an artifact. The `fuzz-groups` job asserts that the legs together cover exactly the set of declared targets. So a new target cannot be silently left unfuzzed.

### What a green run means

There are two honest limits on a green run.

1. This is a **regression gate**, not a bug hunt. Green means "no new crash from this corpus in that budget". Real hunting is a long local `cargo fuzz run`.
2. A fuzz job can be green while fuzzing almost nothing. So the runner asserts a floor on libFuzzer's reported execution count: `--min-runs 10000` against 600 s. That assertion exists because it happened. See the harness finding below.

## What it found

### Five caller-guarded datagram readers panicked on short slices

`read_datagram_header`, `read_request_vote_body`, `read_vote_body`, `read_nak_body` and `read_status_body` sliced fixed offsets out of their input. They relied entirely on every caller's length pre-guard. Every caller did in fact guard. So this was never reachable through the receiver.

But a totality property that holds only by the discipline of five call sites is waiting to be broken by the sixth. All five now return `Option` and are total. The pre-guards were kept, so behaviour on the real path is unchanged byte for byte.

### `Sessioned::apply` broke the contract it was itself a caller of

The `Sessioned::apply` bug was user-reachable, and a fail-stop. `RawStateMachine::apply` documents `out` as *cleared by the caller*. `Sessioned` pushed its one-byte FRESH tag into `out` first. Then it recovered the response as `out[1..]`.

An inner state machine that starts with `out.clear()` truncated the tag away. The contract invites exactly that call. The slice then panicked **on the apply thread**, which killed the service on its first command.

The session target's seed generator found this before the target had been fuzzed once. The fix gives the inner machine a genuinely cleared buffer. The regression test asserts the response **bytes**. The fuzz target's inner machine deliberately keeps its `out.clear()`, so the fix stays guarded.

### `Sessioned::install_snapshot` pre-allocated up to 1 GiB from an unvalidated length

`Sessioned::install_snapshot` read an 8-byte length and bounds-checked it against a 1 GiB ceiling. Then it ran `vec![0u8; len]` before reading a single blob byte. It used the sanity bound as an instruction rather than a ceiling. So a truncated or corrupt snapshot artifact cost a 1 GiB zeroing and an RSS spike per attempt, on the apply thread.

It is now bounded with `take(len)` plus a named truncation error. This was found not as a crash but as a **throughput collapse**. The target managed ten executions in ninety seconds, where every other target did millions. After the fix, 20 000 executions went from 91.8 s to 0.34 s.

### A harness finding that nearly invalidated the tier

This finding is about the harness, not the product. Four of the fourteen targets were executing roughly *sixteen inputs per sixty-second run*. Each still printed a perfectly clean line.

The cause was symbol lookup. libFuzzer symbolizes each newly discovered function to print a `NEW_FUNC` line. `cargo fuzz` builds with its own `--config profile.release.debug="line-tables-only"`. The root workspace's release profile does not reach the excluded `fuzz/` workspace at all. So each sanitizer binary carried about 27 MB of debug info. `llvm-symbolizer` needed about ninety seconds to index one for a single address. That is longer than the whole budget.

`-print_funcs=0` fixed it. For 400 runs, the time went from 90,180 ms to 57 ms. The lesson is not the flag. It is that **a fuzz tier can be green and vacuous**. That is why the run-count floor is now asserted in the script and in CI.

## What fuzzing does *not* cover

- **`FramedConn::read_frame`'s accumulate loop.** The targets drive the frame *decoder*. The socket-side loop that assembles partial reads into a frame is covered by the gateway's own tests, not by libFuzzer.
- **`CncPage` over a real mmap.** The cnc target parses a byte slice. The real page is a shared, concurrently written memory map. Its layout is pinned by offset-assertion tests. Its concurrency is not a fuzzing question.
- **The receiver's stateful dispatch.** Each target is one decoder on one input. Sequences of datagrams drive the receiver's state machine across terms, epochs and sessions. Those sequences are the job of the [simulation](simulation.md) and the [capstones](linearizability.md).
- **`bincode` itself.** It is an external crate reached through these seams. It is a dependency-posture question for `deny.toml` and the SBOM, not a target here.

## Miri: undefined-behaviour detection over the pure decoders

### What runs under Miri

The pure decoders the fuzz targets drive also run under [Miri](https://github.com/rust-lang/miri) in nightly CI, in the `miri` job. Three groups of tests are selected.

| crate | what is covered | tests |
|---|---|---|
| `uc_protocol` | the `v2::` wire, cnc and ipc layer, and `version` packing | 43 |
| `uc_journal` | the segment and `stable_value` record decoders | 19 |
| `uc_remote` | the `outgoing`, `completion` and `slots` SPSC structures | 29 |

The `uc_remote` tests were added with the 2.7.0 client. Those structures are Vec-backed, so Miri models them fully. Miri caught a real Stacked-Borrows aliasing bug there during that client's development.

libFuzzer finds inputs that panic. Miri finds undefined behaviour on inputs that do not. Every selected test passes with Miri's **isolation left on**. Nothing needed excluding, and isolation is never disabled.

### The IPC ring buffers are not Miri-checked

**The IPC ring buffers are not Miri-checked, and cannot be.** They are file-backed shared memory. Miri stops on them at three separate points.

With isolation on, it stops at:

```text
unsupported operation: `open` not available when isolation is enabled
```

With `-Zmiri-disable-isolation`, it gets one step further and stops at:

```text
unsupported operation: unsupported flags for `fallocate` in `mode` argument: 16
```

Mode 16 is `FALLOC_FL_ZERO_RANGE`, the M11 block-reservation fix. Past that point, Miri would stop regardless, on:

```text
unsupported operation: Miri does not support file-backed memory mappings
```

The stated fallback is a Vec-backed ring variant that would exist purely so Miri could run it. That variant **has not been built**, because it would check a different object than the one that ships.

### What covers the rings instead

[Loom](loom.md) runs a small concurrent program under every possible interleaving. Its coverage of the rings is partial. There are four loom models:

- one of the **log buffer's** frame-visibility protocol
- one of the **MPSC ring's** per-record commit protocol, from M13a
- one of the **Broadcast ring's** seqlock read barrier
- one of the **follower write-reserve** seqlock, from #78

None of the four models `uc_protocol::ring` as a whole. So the rings' layout is frozen by offset-pin tests. Their interleavings and undefined behaviour are covered by **nothing**, with two exceptions:

- MPSC has the commit-protocol loom model, plus the `ring_mpsc_record` fuzz target since 2.7.0.
- Broadcast has the seqlock model since 2026-08-31.

**SPSC, the futex layer and the mapping itself remain uncovered.** This is stated again in [What is not verified](not-verified.md).
