# Fuzzing and Miri: decoders on untrusted bytes

Part of [Verification](../VERIFICATION.md), the map of what is proved, checked, and only bug-hunted.

## The short version

Every place UC parses bytes it did not write itself has a fuzz target behind it. A fuzzer throws millions of generated inputs at a decoder and watches for a crash. UC has twenty-seven such targets, one per decoder that meets bytes from the network, from another process, from the log, from disk, or from an operator. The property under test is simple: for any input whatsoever, the decoder returns a value or an error. It never panics, never indexes out of bounds, and never allocates a size an attacker chose.

This is bug-hunting evidence, not proof. A green nightly run means no crash was found from the committed seed inputs within a 600-second budget per target. The tier has found three real bugs and one flaw in itself, all told below. A second tool, Miri, runs the same pure decoders and looks for undefined behaviour instead of crashes.

- **Location:** [`fuzz/`](/fuzz)
- **Runner:** [`scripts/fuzz_smoke.sh`](/scripts/fuzz_smoke.sh)
- **CI:** `nightly.yml`'s `fuzz-groups` and `fuzz` jobs

## Why a panic matters here

In Rust, a panic is not a memory-safety failure. On a UC node it is still a fail-stop. The datagram path runs on the receiver agent, and `apply` runs on the service's apply thread. A panic on either takes the whole process down. So an input that merely panics a decoder is a way to knock a node over, and availability is what this tier defends.

## How the fuzzer works

The fuzzer is [libFuzzer](https://llvm.org/docs/LibFuzzer.html), the LLVM engine that `cargo fuzz` drives. It is **coverage-guided**: it keeps any input that reaches new code and mutates that input further, so it digs into a decoder's branches instead of wandering at random. The targets are **structure-unaware**: each receives raw bytes, with no grammar telling the fuzzer what a valid message looks like. That is the point. A decoder that only survives well-formed input is not total.

Some targets are also **property** targets. On top of "never panic" they check a stated rule on every input, usually "anything the decoder accepts must re-encode and decode back to the same value".

### The seed corpus is generated, not hand-written

Each target starts from a **seed corpus**, a set of valid inputs the fuzzer mutates from. UC's seeds are produced by the real encoders from fixed literals, with `cargo +nightly run --bin seed-corpus`. So the committed corpus is deterministic, and a change to it shows up as a reviewable diff.

### What the nightly run does

Nightly CI runs every target for **600 seconds** on the committed seed corpus, spread across six matrix legs. A crash fails the leg and uploads the crashing input as an artifact. A separate job, `fuzz-groups`, asserts that the six legs together cover exactly the declared set of targets, so a new target cannot be left out by accident.

### What a green run means, and does not

1. **It is a regression gate, not a bug hunt.** Green means "no new crash from this corpus in that budget". Real hunting is a long local `cargo fuzz run` on one target.
2. **A fuzz job can be green while fuzzing almost nothing.** So the runner asserts a floor on libFuzzer's reported execution count, `--min-runs 10000` against 600 s. That assertion exists because it once happened. See [the harness finding](#the-tier-was-once-green-and-vacuous) below.

## The twenty-seven targets, by where the bytes come from

### From the network, from any peer

These decoders meet bytes from anyone who can send a UDP datagram to a node's port. Some run before any authentication at all.

| target | what it decodes | why the bytes are untrusted |
|---|---|---|
| `uc_protocol_datagram` | `uc_protocol::v2::datagram`: the 16-byte header and every body reader. | The **first code an unauthenticated UDP packet reaches**. With `[crypto].enabled = false` it runs before any authentication whatsoever. |
| `uc_protocol_probe` | The path-MTU probe codecs in `uc_protocol::v2::datagram`: `read_probe_rung`, `read_probe_ack_body` and `write_probe_ack_body`. From the jumbo spec §4.2. | Its scope is `Scope::Pairwise`: any peer that can put a datagram on the wire reaches it, and it runs ahead of the commit rule that consumes the result. |
| `uc_protocol_status_body` | `uc_protocol::v2::datagram::{read_status_body, write_status_body}`, the fixed-length status body. A property target. | Any peer can send one. Anything the reader accepts must re-encode and read back equal. |
| `uc_protocol_compat` | The level-aware Settings decode, `decode_settings_at`, and the datagram `wire` byte, under a committed wire level the fuzzer chooses. From the wire-level spec §6.1. A property target alongside the no-panic one. | A peer at a different wire level sends these. Nothing above the committed level may be accepted. |
| `uc_crypto_open` | `uc_crypto::seal::{open_in_place, open_detached}`: the framing arithmetic of the AEAD envelope, the authenticated encryption wrapper around each datagram. | It runs on attacker-chosen bytes *before* the authentication tag has been verified. |
| `uc_crypto_handshake` | `uc_crypto::handshake::Peers::on_message`, the Noise `IK` handshake surface. | With crypto enabled this is the first thing in the process to see bytes from anyone who can reach the UDP port, before the peer is authenticated. |
| `uc_crypto_group_key` | `uc_crypto::group::GroupPlane::on_key_message`. | Two distinct message shapes share datagram kind 20, so this decoder must tell hostile inputs apart. |

Three details about `uc_protocol_datagram`. First, it offers **every** body reader the body of **every** input. So a decoder that expects exactly 24 bytes, with refusals for reserved bytes, an out-of-range row and a zero position, is driven by bodies of every other length and shape, not only its own. Since `2.13.0` the arm list includes `read_snap_report_body`, kind 26 from plan B3, which is exactly such a decoder. Second, three seeds from `2.11.0` cover the `SNAP_BEGIN` shapes that release's flag day produced: `10-snap-begin-legacy-tail`, a body with a trailing carry that the fixed-length reader must ignore rather than refuse, `14-snap-begin-v4`, the shipped layout, and `15-snap-begin-cluster`, which carries `service_id = 255`, the cluster artifact. Third, the `SNAP_TABLE` body reader those seeds replaced is retired along with kind 21.

### From another process on the same host

A node, its services and its clients share memory through files under the instance directory. Any local process with write access to those files can put arbitrary bytes in them.

| target | what it decodes | why the bytes are untrusted |
|---|---|---|
| `uc_protocol_cnc` | `uc_protocol::v2::cnc`, the 8 KiB control page every attaching process maps and parses. Since M14a that includes the page-2 service-slot band and the 4032 pair. | A file on disk that any local process with write access can corrupt. |
| `ring_mpsc_record` | The MPSC slot decision, `classify_commit_word`, and the record decoder, `decode_record_slice`, in `uc_protocol::ring::common`. | This is what the node's consensus agent meets in a shared-memory ring any local process can write. |
| `uc_protocol_sched_record` | The 17-byte service-to-node schedule record the consensus agent decodes. New in `2.11.0`. | It arrives through a shared-memory ring any local process can write. |

### From the replicated log

These bytes were appended by a leader and are applied by every node. A decoder here runs on every replica, so a crash would take down the whole cluster at once rather than one node.

| target | what it decodes | why the bytes are untrusted |
|---|---|---|
| `uc_protocol_log_frame` | `uc_protocol::v2::frame::read_header`, driven behind the real caller's `len >= HEADER_LEN` guard. | The frame header is read off the shared log buffer. The target is deliberately caller-guarded: it pins the guard's contract rather than pretending the guard is absent. |
| `uc_protocol_timer_frame` | The TIMER body the apply loop decodes from a committed frame. New in `2.11.0`. | Guarded by length, and must be total on any slice. |
| `uc_protocol_cluster_frame` | `uc_protocol::v2::frame::read_cluster_prefix` and the payload decoders it dispatches to by kind: `config::decode_config`, `schedule::decode_schedule_table`, `settings::decode_settings`, `upgrade::decode_upgrade_pin` and `upgrade::decode_snapshot_report`. From cluster-FSM plan 1. | Together they decode the `CLUSTER` body, defined in spec §4.3, that every node reads off the log. |
| `uc_protocol_settings` | `uc_protocol::v2::settings`, the codec of the replicated settings record from spec §6. From cluster-FSM plan 1. A property target. | Total on any slice, and a decoded value must round-trip through re-encoding. |
| `uc_protocol_schedule_table` | The replicated schedule table, new in `2.11.0`: the `SCHEDULE_TABLE` frame body every node decodes off the log, and the bytes an operator stages in `schedules.pending` for the leader to read back. Also a property target. | The recurrence arithmetic, `next_after`, `latest_at_or_before` and `arm`, runs on the consensus agent, so it must be total for any rule and any clock. |
| `uc_service_session` | `Sessioned<S>`, the exactly-once envelope on client commands, plus its snapshot install path. | Runs under a fuzz-derived, deliberately tiny `SessionConfig`, so that client eviction, byte eviction and window trim are all reachable. |

### From disk

What a node reads back after a crash, a power loss or a full disk, and what an operator's backup mount holds.

| target | what it decodes | why the bytes are untrusted |
|---|---|---|
| `uc_journal_record` | The segment header and record decoder of `uc_journal`. | This is what crash recovery meets in a torn or corrupt segment after a power loss or a full disk. |
| `uc_journal_stable_value` | `uc_journal::stable_value`, the durable slots for the vote, the term map and the snapshot floor. | Corruption here is a consensus-safety input, not merely a data-loss one. |
| `uc_service_snapshot_envelope` | `uc_service::snapshots::decode_snapshot_envelope`, the 24-byte `ULTSNAP2` header every artifact file has begun with since `2.13.0`. A pure decoder, total on any slice. From coordinated snapshots. | It is the one check between a renamed, mis-copied or wrong-version artifact and a bad install, which would silently leave a span of frames unapplied or cross a version boundary uncaught. A pre-`2.13.0` `ULTSNAP1` header is refused by name as `EnvelopeError::Legacy`, not decoded as version 0. |
| `uc_node_cluster_artifact` | `ClusterFsm::install_snapshot`: the cluster image a below-floor joiner installs **by fiat** off a snapshot session, and the one a restarting node reads off disk. | CRC32 is a checksum, not a MAC, so every length-prefixed read behind it is attacker-chosen. |
| `uc_protocol_cluster_image` | `uc_protocol::v2::cluster_image`, the cluster image codec. From cluster-FSM plan 3, spec §4.8. A property target. | The codec moved out of `uc_node::cluster_fsm` so it can be fuzzed without `uc_node`. It must be total, CRC-checked and exactly framed, and a decoded image round-trips through re-encoding. `uc_node_cluster_artifact` above still exercises the same decoder through the real `ClusterFsm::install_snapshot` entry point. |
| `uc_node_tier_object` | `uc_node::tier::object::ObjectReader` over an arbitrary object stream: a 48-byte header, 1 MiB blocks with length and CRC, both object codecs, `None` and `Zstd`, with the read bounded to 64 MiB. Also `layout::SetLabel::parse` and `TierInfo::parse` on lossy UTF-8. From the backup-tier spec §4. | The tier is a directory on a mount an operator points at. A restore, `verify` or fork meets whatever bytes that mount holds. |

### From clients and operators

Bytes from a remote client over TCP, from anyone who can reach the metrics port, and from the configuration files an operator writes.

| target | what it decodes | why the bytes are untrusted |
|---|---|---|
| `uc_remote_frame` | `uc_remote::frame`: the gateway edge's 24-byte TCP frame header and every typed body decoder. | Any client that can open a socket to the gateway sends these. |
| `uc_node_http` | `uc_node::obs::http::route_raw`, the request parser for `/metrics`, `/healthz` and `/readyz`. | That endpoint is **unauthenticated**. |
| `uc_node_toml` | `uc_node::config_file::parse_str`, the `node.toml` parser. | It sits behind every M9, M11 and M12b named startup refusal, so a bad file must produce a named error and not a panic. |
| `uc_gateway_toml` | `uc_gateway::config_file::parse_str`, the gateway's whole named-refusal path, including its own `EdgeConfig::validate`. | The same reason as `node.toml`. |
| `uc_crypto_admin` | `uc_crypto::admin`, the M12b signed-tag layout on admin requests. A property target. | It checks canonical-length agreement and the sign/verify round-trip, and that a bit-flipped tag and a foreign key are both rejected. |

## What it found

### Five datagram readers panicked on short input

`read_datagram_header`, `read_request_vote_body`, `read_vote_body`, `read_nak_body` and `read_status_body` each sliced fixed offsets out of their input and relied on every caller to check the length first. Every caller did, so this was never reachable through the receiver. But a totality property that holds only by the discipline of five call sites is waiting to be broken by the sixth. All five now return `Option` and are total. The pre-guards were kept, so behaviour on the real path is unchanged byte for byte.

### `Sessioned::apply` broke a contract it relied on

This one was user-reachable and a fail-stop. `RawStateMachine::apply` documents its output buffer `out` as *cleared by the caller*. `Sessioned`, the exactly-once wrapper, pushed its one-byte FRESH tag into `out` first and then recovered the inner response as `out[1..]`. An inner state machine that began with `out.clear()`, which the contract invites, truncated the tag away, and the slice panicked **on the apply thread**. That killed the service on its first command.

The session target's seed generator found this before the target had been fuzzed even once. The fix gives the inner machine a genuinely cleared buffer. The regression test asserts the response **bytes**, and the fuzz target's inner machine deliberately keeps its `out.clear()` so the fix stays guarded.

### `Sessioned::install_snapshot` allocated a gigabyte on request

`Sessioned::install_snapshot` read an 8-byte length, checked it against a 1 GiB ceiling, and then ran `vec![0u8; len]` before reading a single byte of the blob. It treated the sanity bound as an instruction rather than a ceiling. A truncated or corrupt snapshot artifact therefore cost a 1 GiB zeroing and a memory spike on every attempt, on the apply thread. It is now bounded with `take(len)` plus a named truncation error.

This was found not as a crash but as a **throughput collapse**. The target managed ten executions in ninety seconds while every other target did millions. After the fix, 20 000 executions went from 91.8 s to 0.34 s.

### The tier was once green and vacuous

This finding is about the harness, not the product, and it nearly made the tier meaningless. Four of the fourteen targets that existed at the time were executing roughly *sixteen inputs per sixty-second run*, and each still printed a perfectly clean line.

The cause was symbol lookup. libFuzzer symbolizes each newly discovered function to print a `NEW_FUNC` line. `cargo fuzz` builds with its own `--config profile.release.debug="line-tables-only"`, and the root workspace's release profile does not reach the excluded `fuzz/` workspace at all. So each sanitizer binary carried about 27 MB of debug info, and `llvm-symbolizer` needed about ninety seconds to index one for a single address. That is longer than the whole budget.

`-print_funcs=0` fixed it: for 400 runs, 90,180 ms became 57 ms. The lesson is not the flag. It is that **a fuzz tier can be green and vacuous**, which is why the run-count floor is now asserted in the script and in CI.

## What fuzzing does *not* cover

- **`FramedConn::read_frame`'s accumulate loop.** The targets drive the frame *decoder*. The socket-side loop that assembles partial reads into a frame is covered by the gateway's own tests, not by libFuzzer.
- **`CncPage` over a real mmap.** The cnc target parses a byte slice. The real page is a shared, concurrently written memory map. Its layout is pinned by offset-assertion tests, and its concurrency is not a fuzzing question.
- **The receiver's stateful dispatch.** Each target is one decoder on one input. Sequences of datagrams that drive the receiver's state machine across terms, epochs and sessions are the job of the [simulation](simulation.md) and the [capstones](linearizability.md).
- **`bincode` itself.** It is an external crate reached through these seams, and a dependency-posture question for `deny.toml` and the SBOM rather than a target here.

## Miri: undefined behaviour in the pure decoders

### What Miri is, and what runs under it

[Miri](https://github.com/rust-lang/miri) is an interpreter for Rust. It runs a test step by step and reports undefined behaviour that a compiled program would silently hide, such as reading uninitialised memory or breaking Rust's aliasing rules. libFuzzer finds inputs that panic. Miri finds undefined behaviour on inputs that do not.

The pure decoders the fuzz targets drive also run under Miri in nightly CI, in the `miri` job:

| crate | what is covered | tests |
|---|---|---|
| `uc_protocol` | the `v2::` wire, cnc and ipc layer, and `version` packing | 43 |
| `uc_journal` | the segment and `stable_value` record decoders | 19 |
| `uc_remote` | the `outgoing`, `completion` and `slots` SPSC structures | 29 |

The `uc_remote` tests came with the 2.7.0 client. Those structures are Vec-backed, so Miri models them fully, and it caught a real Stacked-Borrows aliasing bug there during that client's development. Every selected test passes with Miri's **isolation left on**. Nothing needed excluding, and isolation is never disabled.

### The IPC ring buffers cannot run under Miri

**The IPC ring buffers are not Miri-checked, and cannot be.** They are file-backed shared memory, and Miri stops on them at three separate points. With isolation on, it stops at:

```text
unsupported operation: `open` not available when isolation is enabled
```

With `-Zmiri-disable-isolation`, it gets one step further and stops at:

```text
unsupported operation: unsupported flags for `fallocate` in `mode` argument: 16
```

Mode 16 is `FALLOC_FL_ZERO_RANGE`, the M11 block-reservation fix. Past that it would stop regardless, on:

```text
unsupported operation: Miri does not support file-backed memory mappings
```

The stated fallback is a Vec-backed ring variant that would exist purely so Miri could run it. That variant **has not been built**, because it would check a different object than the one that ships.

### What covers the rings instead

[Loom](loom.md) runs a small concurrent program under every possible interleaving. Its coverage of the rings is partial. There are four loom models: one of the **log buffer's** frame-visibility protocol, one of the **MPSC ring's** per-record commit protocol from M13a, one of the **Broadcast ring's** seqlock read barrier, and one of the **follower write-reserve** seqlock from #78. None of the four models `uc_protocol::ring` as a whole.

So the rings' layout is frozen by offset-pin tests, and their interleavings and undefined behaviour are covered by **nothing**, with two exceptions. MPSC has the commit-protocol loom model plus the `ring_mpsc_record` fuzz target, since 2.7.0. Broadcast has the seqlock model, since 2026-08-31. **SPSC, the futex layer and the mapping itself remain uncovered.** This is stated again in [What is not verified](not-verified.md).
