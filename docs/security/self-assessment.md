# Security self-assessment

What the maintainers looked at, what they found, and what they know they have
not looked at. Written to be useful to an external reviewer: the findings are
here so nobody has to rediscover them, and the "focus here" section says where
we think the remaining risk is.

Companion documents: [threat model](threat-model.md) (assets, actors,
boundaries, what is out of model) and [attack surface](attack-surface.md)
(every parser, its guards, its fuzz target).

---

## 1. Scope and method

**Who:** the maintainers. **When:** 2026-08-23 / 2026-08-24, as milestone M12d
of the `v2.6.0` release. **Revised 2026-08-29 for `2.8.0` (M14, multi-service):**
the M12d dating below is kept as history; the additions are F7, item 8 in §4,
and the M14 line in §5.

**What was looked at:** every place the system parses bytes it did not write —
the pre-auth UDP datagram path, the crypto plane (Noise `IK` handshake, AEAD
envelope, group key), the gateway's TCP framing, the M12b admin credential and
audit path, the on-disk journal and `StableValue` records, the cnc control
page, the session envelope, the TOML config loaders and the unauthenticated
observability endpoint. The [attack surface](attack-surface.md) inventory is
the enumeration this assessment worked from.

**Method**, in three layers:

1. **Code reading** of each seam above, against the threat model's actor list.
2. **Coverage-guided fuzzing** — fifteen `cargo-fuzz` targets, one per parser
   family, with a deterministic committed seed corpus, run nightly for 600 s
   each with an asserted execution floor. A single 20-second local sweep of the
   fourteen targets that existed during M12d executes ~118 M inputs; cumulative
   local execution during M12d was several hundred million. The fifteenth,
   `ring_mpsc_record`, arrived with the 2.7.0 MPSC ring rewrite.
   [VERIFICATION §7](/docs/VERIFICATION.md#7-fuzzing--decoders-total-on-untrusted-bytes)
   is the record; [`fuzz/README.md`](/fuzz/README.md) is the operating manual.
3. **The pre-existing correctness tiers**, which are older than this
   assessment and cover the *behaviour* rather than the parsers: machine-checked
   Lean proofs of the consensus kernels, a deterministic simulation with safety
   invariants and seeded fuzz, WGL linearizability capstones under failover /
   purge churn / partition, Elle transactional checking with a mutation tier,
   multi-process SIGKILL crash tests, loom, and Miri over the pure decoders.
   All of it, including what each tier does *not* cover, is in
   [VERIFICATION.md](/docs/VERIFICATION.md).

**What was not done:** no external penetration test, no independent code
audit, no cryptographic review of the Noise/AEAD composition beyond reading it
against the M8 design, no formal analysis of the handshake state machine. The
external review is a separate, user-scheduled step — gate row 10 of
[the M12 gate](/docs/benchmarks/uc2-m12-gate-2026-08-22.md).

## 2. Findings

Six, numbered. Severity is this project's own judgement, stated with the
reachability that justifies it — three of the six were **not** reachable by an
adversary, and saying so is more useful than inflating them.

### F1 — five caller-guarded datagram readers panicked on short slices

**Severity:** low (defence in depth) · **Status:** fixed, `112b81f` (M12d
Task 1) · **Found by:** the first fuzz target, on its first real run.

`read_datagram_header`, `read_request_vote_body`, `read_vote_body`,
`read_nak_body` and `read_status_body` sliced fixed offsets out of their input
and relied entirely on every caller's length pre-guard. Every caller did in
fact guard, so **this was never reachable through the receiver** — but a
totality property that holds only by the discipline of five call sites is a
property waiting to be broken by the sixth, on the one code path an
unauthenticated UDP packet reaches first. All five now return `Option` and are
total on `&[u8]`; the caller pre-guards were kept, so behaviour on the real
path is byte-for-byte unchanged.

### F2 — `Sessioned::apply` violated the contract it was itself a caller of

**Severity:** high (user-reachable fail-stop) · **Status:** fixed, `7c908b1`
(M12d Task 2) · **Found by:** the session target's seed generator, before the
target had been fuzzed once.

`RawStateMachine::apply` documents `out` as *cleared by the caller*.
`Sessioned` pushed its one-byte FRESH/REPLAYED tag into `out` first and then
recovered the response as `out[1..]`. An inner state machine that begins with
`out.clear()` — which the contract invites — truncated the tag away, and the
slice panicked **on the apply thread**, killing the service on its first
command. No adversary needed: a contract-abiding state machine was enough.
Fixed by handing the inner machine a genuinely cleared buffer; the regression
test asserts the response **bytes**, and the fuzz target's inner machine keeps
its `out.clear()` so the fix stays guarded.

### F3 — `Sessioned::install_snapshot` pre-allocated up to 1 GiB from an unvalidated length

**Severity:** medium (DoS on a local/replicated surface) · **Status:** fixed,
`7c908b1` (M12d Task 2) · **Found by:** fuzzing, as a throughput collapse
rather than a crash.

It read an 8-byte length, bounds-checked it against a 1 GiB ceiling, and then
`vec![0u8; len]` **before reading a single blob byte** — using a sanity bound
as an instruction rather than as a ceiling. A truncated or corrupt snapshot
artifact therefore cost a 1 GiB zeroing and an RSS spike per attempt, on the
apply thread. Now bounded with `take(len)` plus a named truncation error.
The signature was ten executions in ninety seconds where every other target did
millions; 20 000 executions went 91.8 s → 0.34 s after the fix (~157–270×).

### F4 — admin verification read its identity binding from the writable cnc page

**Severity:** high (authentication bypass) · **Status:** fixed before merge,
M12b `50473d5` · **Found by:** code review, pre-merge.

The signed admin tag binds `instance_id` and `app_id`. The first implementation
re-read them **from the cnc page** per request. The page is a writable file
whose header is only magic-checked, so an actor with instance-directory write
access but no admin key could restore a captured `instance_id` after a node
restart and replay a captured, still-unexpired request. The binding is now
taken from the node's own boot-time state
(`Consensus::admin_instance_id`/`admin_app_id`) and never re-read from the
page. Regression test:
`uc2_node/tests/admin_auth.rs::a_capture_replayed_after_a_restart_is_refused`
(forges the cnc header back to the captured `instance_id`, asserts
`auth_bad_tag`/reason 21 and the config version unmoved).

### F5 — `RemoteClient` reconnect budget pinning under load

**Severity:** low (liveness, not confidentiality or integrity) · **Status:**
fixed, `fc27536` (M12c) · **Found by:** driving the quickstart from outside the
workspace.

`request_timeout` was not enforced while the client's reader was reconnecting,
so a request could outlive its stated budget across a dial storm. The client now
sweeps for expired requests between every dial attempt and every hop; the
resulting bound (`request_timeout + 2 × connect_timeout + SWEEP_INTERVAL`) is
documented in [the remote protocol reference](/docs/reference/remote-protocol.md).

### F6 — harness finding: the fuzz tier was green and nearly vacuous

**Severity:** n/a (test infrastructure) · **Status:** fixed, `736c1f3` (M12d
Task 3) · **Recorded because it nearly invalidated the tier.**

Four of the fourteen targets were executing roughly **sixteen inputs per
sixty-second run** while printing a perfectly clean line. libFuzzer symbolizes
each newly discovered function to print `NEW_FUNC`, and `llvm-symbolizer`
needed about ninety seconds to index one 27 MB sanitizer binary for a single
address — longer than the whole budget. `-print_funcs=0` fixed it (400 runs:
90 180 ms → 57 ms). The lesson is not the flag; it is that a fuzz tier can be
green and vacuous, which is why `scripts/fuzz_smoke.sh` now asserts a floor on
libFuzzer's reported execution count and CI passes `--min-runs 10000` against a
600 s budget. **Green now means fuzzed.**

### F7 — an unservable `SNAP_NAK` pinned the snapshot-session slot

**Severity:** low (liveness of a joiner; no integrity effect) · **Status:**
fixed, `a405e71` (M14c) · **Found by:** the M14c review of the per-FSM intake.

With one artifact per declared FSM in a session (wire 0.6.0), a `SNAP_NAK`
for a range the sender could not serve left the session slot occupied, so a
joiner could wedge a sender's slot until the 30 s cycle. The sender now
refuses a set that misses a declared id up front, an unservable NAK releases
the slot, and intake I/O failures are retried and counted
(`uc2_snapshot_intake_io_failures_total`). Reachability: any peer, or anyone
spoofing one with crypto off — the same reach as every SNAP kind.

## 3. Known weaknesses, not fixed

Each of these is a decision with a reason, not a backlog item we forgot. The
[threat model §5](threat-model.md#5-out-of-model) is the full list; these are
the ones a reviewer should weigh.

| Weakness | Why it is accepted |
|---|---|
| **Cleartext by default.** `[crypto].enabled = false` means no source authentication on the node↔node plane at all. | It is an explicit, named choice in `node.toml` (a node with no `[crypto]` section refuses to start), and the alternative — crypto on by default — is a flag day that cannot be rolled back per-node. The mitigation is that the decoders behind it are total and fuzzed. |
| **A malicious cluster member can forge fan-out traffic as any node.** | The group key is symmetric; this is inherent to seal-once-send-to-many. The alternative is N seals per fan-out, which the M8 design rejected on cost. Verbatim residual 3 in [threat model §6](threat-model.md#6-residuals-stated-elsewhere-and-where). |
| **No client authentication or TLS on the remote link.** | An explicit M12 non-goal ("no TLS on the remote client link in this release"). Reachability is authorization; the guidance is to keep the port private or front it with a proxy. |
| **`[admin] auth = "hmac"` is only cluster-wide when paired with `[crypto].enabled = true`.** | A follower forwards an authenticated request to the leader over the node-to-node socket as wire kind 16, which the leader cannot re-verify. Stated in four places, including the README. Fixing it properly means an admin credential the *leader* can check, i.e. a wire change — deliberately not smuggled into M12b. |
| **The IPC rings' `unsafe` code is checked for layout and nothing else.** | `uc_protocol/src/ring/{spsc,mpsc,broadcast,common,futex}.rs` has **no interleaving coverage and no UB coverage**. Miri does not support file-backed memory mappings (three distinct blockers, each reproduced — VERIFICATION §7), and a `Vec`-backed variant would check a different object than the one that ships. The tree's only loom model (`uc2_log/tests/loom_frame.rs`) covers the **log buffer's** frame-visibility protocol — a hand-written model of that handshake — not the rings. Offset-pin tests freeze the layout. This is the least-covered `unsafe` code in the system and it is stated as such, here and in VERIFICATION §11. |
| **`leader_completeness` is not proved**, and the Lean model collapses the durable counter's two readers into one. | Open, and stated: [VERIFICATION §11](/docs/VERIFICATION.md#11-what-is-not-verified). A real acked-write-loss bug once lived in exactly that model gap and was found from the Rust side. |
| **A malformed query frame fail-stops a typed state machine, pre-commit, from an unauthenticated client.** | `uc2_service/src/traits.rs`'s blanket `RawStateMachine` impl decodes a query with `.expect("corrupt query frame (fail-stop)")`, and `uc2_service/src/apply.rs`'s query branch calls it **while holding the state machine's `Mutex`** — so a single malformed `QUERY` body arriving through a gateway (`SendHalf::try_query`) panics the apply thread and poisons the lock, with no quorum, no leadership and no commit involved. It is a one-frame remote kill switch for any deployment that pairs the typed tier with untrusted clients. **Documented rather than fixed in M12d on purpose:** turning a decode error into a refusal changes the tier's error semantics (today, undecodable bytes are treated as corruption, and corruption is a fail-stop by design — the same `.expect` guards the post-commit apply path, where fail-stop *is* the right answer because every replica sees the same committed bytes). Choosing the new contract is a design decision, parked as a follow-up. **The workaround today is the raw tier**, which hands you the bytes and lets you reject them; the other mitigation is not exposing a gateway to untrusted clients. |
| **`bincode` is unmaintained (RUSTSEC-2025-0141).** | A maintenance-status advisory with no patched version ("No safe upgrade is available!"). It is the wire codec for the cnc page, log records and the remote protocol, and the typed tier's byte-identity promise is defined against it, so replacing it is a wire-format migration. Ignored on purpose in `deny.toml`, with the reasoning, the date, and an instruction to re-check when a maintained successor appears. |
| **The typed state-machine tier decodes with no configured byte limit.** | `bincode::config::standard()` is `NoLimit`. The bounds that exist are the ≤ 1344-byte payload cap, serde's 1 MiB pre-allocation cap, and fail-stop on a decode error. The documented stance is "committed bytes are trusted"; the answer for untrusted input is the raw tier. Stated rather than papered over — see [attack surface §1](attack-surface.md#1-the-inventory). |

## 4. What an external review should focus on

Ranked by where we think the residual risk actually is:

1. **The pre-auth UDP path with crypto OFF.** This is the default posture, and
   the decoders are the only thing in front of the node's state. Fuzzing is a
   regression gate, not a proof of totality. Worth a fresh pair of eyes on
   `uc2_net::receiver`'s dispatch — in particular the consensus plane (kinds
   5–11 plus the admin-forward kinds 16 and 17), which is forwarded **raw** to
   the consensus agent with no term filter, because a higher-term
   `RequestVote` must reach the state machine.
2. **The `snow` handshake state machine under malformed messages.** Turning
   crypto on *adds* a pre-auth parser (kinds 18/19) that anyone who can reach
   the UDP port can drive. Our target fuzzes `Peers::on_message` with the
   claimed sender id and `now_ns` taken from the input; nobody has reviewed the
   state machine's behaviour under interleaved, out-of-order or replayed
   handshake messages from multiple claimed identities.
3. **The typed tier's pre-commit query decode.** We have documented it (§3) and
   deliberately not changed it. A reviewer's judgement on whether fail-stop is
   defensible for an *uncommitted, unauthenticated* input — and on whether
   anything else in the SDK shares that shape — is worth more than ours here.
4. **The admin canonical-bytes layout.** `AdminMessage::canonical_bytes` is
   what the HMAC covers, and every property of M12b's authentication reduces to
   "does this byte string uniquely determine the request". It is pinned against
   a fixed test vector; it has not been reviewed for ambiguity (field framing,
   length prefixes, the `fnv1a64` name hash) by anyone outside the project.
   The ruled decision to omit a `(seq, nonce)` replay ring rests on that layout
   plus the boot-time `instance_id` binding (F4) — if the layout is ambiguous,
   that reasoning weakens.
5. **The `uc_protocol::ring` buffers' `unsafe` mmap code.** Five files —
   `spsc`, `mpsc`, `broadcast`, `common`, `futex` — whose layout is frozen by
   offset-pin tests. As of 2.7.0 the MPSC ring is the one exception to "covered
   by nothing": its claim-then-commit protocol has a loom model
   (`uc_protocol/tests/loom_mpsc.rs`, P1–P5 plus mutation runs), its slot
   decoder has a fuzz target (`ring_mpsc_record`), and its producer-preemption
   case is a unit test. That model runs over a `Vec` of loom atomics — the
   **mmap itself is outside both loom and Miri**, so mapping-level UB is still
   uncovered, and the model omits the tail-padding path, the futex park/wake
   and the crc32. **SPSC's interleavings and the broadcast seqlock remain
   unmodelled**; for the seqlock that is deliberate (a faithful loom model of
   it correctly fails under loom's full C++ semantics — see
   `uc2_log/tests/loom_frame.rs`'s header), which is itself worth an outside
   opinion. Those are the places we would look first.
6. **The gateway's credit / backpressure ladder under a malicious client.** The
   caps (`MAX_FRAME_LEN`, `max_connections`, credits, write timeout) are
   designed against an over-eager client, not a hostile one. Behaviour under a
   client that opens the maximum connections, sends `HELLO` and then stalls, or
   that never reads its socket while pipelining, deserves adversarial attention.
7. **`Sessioned` eviction under adversarial `client_id`s.** `client_id` is
   client-chosen. A client that churns fresh ids evicts other clients' dedup
   state (LRU, by `(last_seen_pos, client_id)`, on either the client-count or
   byte budget). We believe the consequence is confined to *liveness of
   exactly-once* for the evicted clients — the tier degrades to at-least-once —
   and not to a correctness violation. That belief deserves checking.
8. **The multi-artifact snapshot intake state machine** (`uc2_net/src/receiver.rs`,
   M14c): adopt-on-complete across N artifacts, `.part` files, an abandoned
   intake's unlink, the declared-set and layout refusals, and the interaction
   with a concurrent second session from another peer. It is unit-tested and
   exercised by one two-FSM learner test; nobody outside the project has read
   it against interleaved or malformed sessions.

## 5. Verification inventory

Not repeated here: [`docs/VERIFICATION.md`](/docs/VERIFICATION.md) is the
record, and its §11 ("What is *not* verified") is the part worth reading first.
The short form:

| Tier | What it covers |
|---|---|
| Lean 4 proofs | election safety, log matching over the consensus kernels; `leader_completeness` open |
| Deterministic simulation (`uc2_sim`) | safety invariants + seeded fuzz over the whole protocol, virtual time |
| WGL linearizability capstones | failover, purge/snapshot churn, partition/quorum loss |
| Elle | transactional safety, plus a mutation tier that proves the harness can fail |
| Multi-process SIGKILL | real processes, real reconstruction |
| loom | two hand-written models: the **log buffer's** frame-visibility protocol (`uc2_log/tests/loom_frame.rs`) and the **MPSC ring's** claim-then-commit protocol (`uc_protocol/tests/loom_mpsc.rs`, since 2.7.0). Both model the protocol over loom atomics, **not** the mmap; SPSC and the broadcast seqlock are still unmodelled |
| **Fuzzing (§7)** | **fifteen decoder targets, nightly, with an execution floor** |
| **Miri (§7)** | **the pure decoders — 62 tests, isolation on, no exclusions; the mmap rings are out of reach** |
| **M14 multi-service** | unit + in-process integration + sim inv10 + fuzz seeds; **no two-FSM lincheck/partition/crash/Elle yet** (M14c2, `2.8.1`) — [VERIFICATION §11](/docs/VERIFICATION.md#11-what-is-not-verified) |

## 6. Dependency posture

- **`cargo-deny`** runs on every push (`ci.yml`'s `deny` job), twice: once on
  the default-feature graph and once on `--all-features`, each checking
  `advisories licenses bans sources`. `bans` enforces "exactly one AES-GCM
  implementation in the graph".
- **One documented ignore:** RUSTSEC-2025-0141 (`bincode 2.0.1` unmaintained) —
  §3 above and `deny.toml` carry the reasoning and the re-check instruction.
- **SBOM per release:** a CycloneDX document per workspace member, shipped as
  `uc2-<ver>.cdx.tar.gz` (`cargo cyclonedx` has no workspace-wide single-document
  mode, and the release does not pretend otherwise).
- **MSRV 1.89**, enforced by an `msrv` CI job running clippy on a real 1.89.0
  toolchain — not merely `check`.

## 7. Release integrity

Artifacts are signed with **cosign keyless** (Sigstore, GitHub OIDC) in
`release.yml`: `cosign sign-blob --bundle` over every tarball, the SBOM archive
and `SHA256SUMS`; `cosign sign --recursive` over the image digest, so a client
pulling a per-platform manifest still finds a signature. Verification is
identity-pinned — the identity regexp is
`https://github.com/PeterKnego/ultima_cluster/.github/workflows/release.yml@refs/tags/v.*`
with issuer `https://token.actions.githubusercontent.com` — and written out in
[`docs/how-to/cut-a-release.md`](/docs/how-to/cut-a-release.md) §5 and
`packaging/README-release.md`. Nothing is published unless the `release-smoke`
job passes first.

**Honest caveat:** signing has never executed, because keyless signing needs a
GitHub OIDC identity and the `release`/`image` jobs run only on a real tag. The
`v2.6.0-rc.1` tag exists to close that gate before the real one is spent — gate
rows 5 and 6.

---

**Status: package prepared 2026-08-24; revised for 2.8.0 on 2026-08-29; external review pending (gate row 10).**
