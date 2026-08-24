# ultima_cluster releases

## v2.6.0 — M12 adoptable cluster — *prepared, not yet tagged*

**Four sub-milestones, one tag.** M12a (gateway kit) merged at `185783e`,
M12b (admin authn + audit) at `9897219`, M12c (packaging + publishing) at
`e571c27`, M12d (security posture) on `uc2/m12d-security-posture`. They tag
together as `v2.6.0`; the tag, the `v2.6.0-rc.1` rehearsal that exercises the
release workflow for the first time, the two fleet rows, and the external
review are all separate, user-owned steps. Gate record:
`docs/benchmarks/uc2-m12-gate-2026-08-22.md`. Spec:
`docs/superpowers/specs/2026-08-22-uc2-m12-adoptable-design.md`, whose four
"As built" amendments are the authoritative correction of the sketch against
what shipped.

**Nothing in this release touches consensus, the node-to-node wire protocol,
or the cnc page layout.** `uc_protocol::version::CURRENT` stays `0.5.0`. The
one cnc change is M12b's 64-byte admin-auth line at `CNC_OFF_ADMIN_AUTH =
3904`, inside the existing reserved band — no version bump, no flag day.

### M12a — the gateway kit (`185783e`)

- **The two-tier state-machine contract.** `RawStateMachine` (`apply(&mut
  self, position, cmd: &[u8], out: &mut Vec<u8>)`) is the core trait;
  `ServiceBuilder` takes `S: RawStateMachine`; the typed `StateMachine` is a
  blanket impl onto it. The decision record is
  `docs/notes/2026-08-22-codec-budget-spike.md`: an isolated codec ladder
  measured serde+bincode's `Vec<u8>` handling at 25–42× a hand-laid frame on
  encode and up to 21× on decode — *not* the format's fault, but serde typing
  a byte vector as a sequence of `u8` and walking it element-wise — and a
  **dev-box** `m5_gate` `apply-profile` run put the typed `CountSm` at
  `sm_apply = 731 ns` per frame (75.8 % of the apply cycle) against the raw
  `RawCountSm`'s 12 ns (5.8 %). Those are shares from a box that is not a
  bench; the fleet number is gate row 3 and has not been run.
  Byte-identity with `v2.5.0` is asserted, not assumed
  (`uc2_service/tests/raw_contract.rs`). A cheaper intermediate exists and is
  documented: typing a blob field as `bytes::Bytes`/`serde_bytes` gives the
  identical wire at 1.2–1.9× raw.
- **`Sessioned<S>`** — a 16-byte `client_id ++ seq` envelope, a one-byte
  FRESH/REPLAYED/EXPIRED tag, an LRU dedup table that rides snapshots, and a
  replicated `SessionConfig` that `install_snapshot` refuses to silently
  retune.
- **`uc2_remote` protocol v1 and `RemoteClient`** — framed TCP, per-connection
  credits, `REDIRECT`/`LEADER_CHANGED`/`RETRY`/`UNKNOWN`, pipelined submit and
  query, ordered re-send after failover.
- **`uc2_gateway::Edge` + the `uc2-gateway` binary + `gateway.toml`** — a
  per-node TCP front door over `uc2_client::Engine`, static `node_id → address`
  member map (Aeron's `ingressEndpoints` shape), leader watch off the cnc page.

**Rulings and mechanisms worth remembering.**

- **Redirect, not forward.** A `SUBMIT` that arrives at an edge whose node is
  not serving gets a `REDIRECT` to the leader's gateway address — the edge
  never relays it onward. Queries are answered locally regardless of role.
  This keeps the edge stateless and keeps the leader from having to trust
  another node's framing.
- **The per-connection not-serving latch.** A connection told once that this
  node cannot take writes is told the same thing for every later `SUBMIT` on
  that connection, even if the node starts serving immediately after. The
  invariant it buys: *the set of `SUBMIT`s a connection gets accepted is
  always a prefix of what it sent*. Without it, `Sessioned`'s
  FRESH/REPLAYED/EXPIRED classification breaks on a gap the dedup table cannot
  classify.
- **Probe-before-flush.** A freshly (re)connected client writes exactly one
  request and waits for proof the far end will serve it before releasing the
  rest of its window — and acts on `HELLO_OK`'s named leader first, so a
  pipelined window is flushed *at* the leader instead of being redirected
  frame by frame. `docs/notes/uc2-gateway-shapes-and-flow-control.md` is the
  writeup.
- **The faulted-exit contract.** `InstanceRestart` latches the edge `faulted`
  permanently; the daemon polls `is_faulted` and exits 1, so `Restart=on-failure`
  brings up a fresh edge against the new node instance.
- **Head-of-line blocking is documented, not tuned away.** One driver thread
  per edge serializes outbound writes, so a stalled client's write (up to the
  1 s `WRITE_TIMEOUT`) can delay other clients on that edge. Any fleet cost
  number for gate row 2 includes it.
- **What the capstone does and does not cover.** The remote
  lincheck capstone is `submit` → `wait` at concurrency 4 (an op must be a
  single interval to be a linearizability history entry at all). Pipelining is
  exercised by `failover.rs` and the `m12_gate` harness, not by the capstone.

### M12b — admin authentication and audit (`9897219`)

- **Signed admin requests.** `HMAC-SHA256` over
  `len(app_id) ‖ app_id ‖ instance_id ‖ seq ‖ nonce ‖ op ‖ id ‖ ip ‖ port ‖
  expiry_ns`, every integer little-endian, under a named 32-byte key
  (`uc2_crypto::admin`). New reason codes 20 `auth_missing`, 21 `auth_bad_tag`,
  22 `auth_expired`, 23 `auth_unknown_key`, 24 `audit_failed`. `uc2ctl` gains
  `--admin-key`/`--admin-key-name`/`--admin-ttl-secs` on every mutating verb,
  plus `gen-admin-key` and offline `audit`.
- **`audit.jsonl`** — append-only, one `write_all` + `sync_data` per record,
  written *before* the answer is published at every answer site. A failed
  record refuses the request (24) rather than answering it unrecorded.
- **Explicit-choice config.** `[crypto]` and `[admin]` are required sections;
  absence is a named startup refusal (`ConfigError::CryptoChoiceRequired` /
  `AdminChoiceRequired`).

**Rulings.**

- **No `(seq, nonce)` replay ring** — a deliberate deviation from spec §5.2's
  sketch. The tag covers `seq`; the consensus agent only acts on
  `seq > last_admin_seq`, so a capture cannot be re-presented at its original
  `seq` and re-presenting it higher invalidates the tag. A restart resets
  `last_admin_seq` but re-randomizes `instance_id`, which the tag also covers.
  `expiry_ns` bounds the one remaining case (a live, correctly-sequenced
  request delayed in flight). A ring would refuse nothing these two checks do
  not already refuse.
- **`AdminPolicy` is not a `NodeConfig` field.** It lives on `StartOpts`
  beside the optional pre-bound socket — both live process resources, not
  values a `Clone`-able TOML mirror should carry. Library callers get
  `AdminPolicy::Filesystem`, the pre-M12b posture byte-for-byte; only the
  `uc2-node` daemon builds a policy from `[admin]`.
- **The dedup-re-send carve-out.** A byte-identical re-send of an
  already-answered proposal is served from the leader's cache and *counted*
  (`config_proposal_dedup_resend`), not re-recorded — it repeats an answer the
  file already holds. Without this, one captured kind-16 datagram re-sent in a
  loop drove one `fsync` per packet on the consensus thread.

**The review finding that mattered (C1, fixed pre-merge in `50473d5`).**
`verify_admin` originally read `instance_id`/`app_id` from `self.cnc.meta()`
*per request*. The cnc page is a file in the instance directory whose header
is only magic-checked, so an actor with directory write access and **no admin
key** could capture a signed `(auth, req)` pair, await or induce a restart
(resetting `last_admin_seq` to 0), `pwrite` the captured `instance_id` back
into `CNC_OFF_INSTANCE_LO/HI`, re-present the captured lines, and have the
change applied a second time — which also falsified the restart half of the
no-replay-ring argument above. The binding now comes from
`Consensus::admin_instance_id`/`admin_app_id`, set once in `Node::start_with`.
Pinned by `uc2_node/tests/admin_auth.rs::a_capture_replayed_after_a_restart_is_refused`,
with anti-vacuity confirmed: reverting the binding makes that replay verify and
reach `propose_config`.

**The residual, stated in four places rather than one.** A follower forwards an
authenticated request to the leader as a `ConfigProposal` (wire kind 16) over
the node-to-node UDP socket. The leader cannot re-verify the operator's HMAC
there — the canonical message is bound to the *requesting* node's identity — so
it records which peer vouched (`peer:<id>`). `on_config_proposal`'s membership
guard drops a datagram whose source address resolves to no current member, but
an address filter is not authentication: with `[crypto].enabled = false`, a
spoofing network-path adversary can inject a proposal. **`[admin] auth = "hmac"`
authenticates cluster-wide only paired with `[crypto].enabled = true`**, and a
flood of *fresh* nonces from a member still costs one `fsync` each.

### M12c — packaging and publishing (`e571c27`)

- **Version identity.** Lockstep `2.6.0` across the workspace, publish metadata
  and path-dep version pins for all 12 publishable crates, `rust-version =
  "1.89"` (probed to `File::try_lock_exclusive`, not guessed) with the pinned
  stable at 1.96.0, and `docs/reference/semver-policy.md`.
- **Supply chain.** `deny.toml` plus two `cargo-deny` passes (default graph and
  `--all-features`, so `uc2_service`'s non-default `ultima_db` adapter is
  actually resolved), a CycloneDX SBOM, and CI `deny` / `publish-check` / `msrv`
  jobs. Dropping `snow`'s `std` feature removed `ring` and made the spec's
  "exactly one AES-GCM implementation in the graph" rule true. Dead workspace
  deps (`quinn`, `rustls`, `tokio`, `futures`) removed.
- **`release.yml`** — native x86_64 and aarch64 builds, tarballs +
  `SHA256SUMS` + SBOM, keyless cosign signatures (`--recursive` on the image,
  so a client pulling by platform digest still finds one), a distroless ghcr
  image, and a `release-smoke` publish gate that unpacks the tarball in a bare
  `ubuntu:24.04` with no toolchain and runs `packaging/quickstart-local.sh`
  out of it, then brings up `packaging/compose.yml` as three nodes + three
  services + three gateways and drives `counter-remote` to `value=10`.
- **Docs.** Artifacts-first `docs/QUICKSTART.md`, `docs/how-to/cut-a-release.md`,
  `packaging/README-release.md`.

**Rulings and honest caveats.**

- **Leaves-only `cargo publish --dry-run`.** `publish-check` runs
  `cargo package --no-verify` over **all 12** crates in one invocation (which
  is what forces every path dep to carry a resolvable `version =`), but the
  per-crate `--dry-run` covers only the **4 dependency-free leaves**
  (`ultima-journal`, `uc_protocol`, `uc2_remote`, `uc2_consensus`). A non-leaf
  crate's dry run cannot pass before the first publish — its path deps must
  resolve against the real registry — and this is not only a bootstrap gap:
  `uc2_node`'s dev-dependency on `uc2_service` is a genuine dev-only cycle no
  publish order avoids. Row 7 therefore claims *packaging* for 12 and
  *publishing* for 4; the full sequence is first exercised by the manual
  ordered publish in `cut-a-release.md` §6.
- **`cargo fmt --check` deferred, per the spec's own condition.** Spec §1 made
  the one-shot reformat conditional on no long-lived branch being open. Two
  worktrees are open (`fix/remaining-flakes`, `worktree-uc2-multi-service`) and
  the reformat measures **2 731 hunks**, every one of which would become a
  conflict in both. The re-run condition is written verbatim in gate row 13.
  `clippy -D warnings` — the gate that catches defects rather than whitespace —
  is enforced on both the pinned stable and the MSRV floor.
- **What CI cannot prove locally, said as such.** Docker, buildx and ghcr do
  not exist on the dev box, so the bare-container run, the image build and the
  compose stack are CI-only; keyless signing needs a GitHub OIDC identity the
  box does not have (a local `cosign sign-blob` would either fail or, worse,
  sign under some *other* identity). Both are first exercised by the
  `v2.6.0-rc.1` tag. And one gap CI does not close at all: `release-smoke`
  runs the **x86_64** tarball only — the aarch64 binaries are built and
  packaged but never executed anywhere, so that half is unclaimed until
  somebody runs the arm tarball on arm hardware.
- **One accepted advisory, written into `deny.toml` with its reasoning:**
  RUSTSEC-2025-0141, `bincode 2.0.1` *unmaintained* — a maintenance-status
  advisory with no patched version to move to. bincode is the wire codec for
  the cnc page, log records and the remote protocol, and the typed tier's
  byte-identity promise is defined against it, so replacing it is a wire-format
  migration, not a hygiene fix.

**Fixed on the way:** `uc2_remote`'s `request_timeout` was not enforced while
reconnecting — the sweep now runs between every dial attempt, the per-attempt
connect-shortening (which pinned the dial budget under load) is gone, and the
`HELLO` read is capped at the attempt deadline so the documented
`2 × connect_timeout` bound is literal (`ae0f245`, `fc27536`, `b4b3b0c`). The
architecture doc's log-buffer default was also corrected from a stale
"~512 MiB" to `buffer_bytes`' real 64 MiB.

### M12d — security posture (this branch)

- **A `cargo-fuzz` crate outside the workspace** (`exclude = ["fuzz"]` plus its
  own empty `[workspace]`, because `libfuzzer-sys` needs nightly and the
  workspace pins stable at an MSRV floor), **14 targets** across the datagram,
  log-frame, cnc, remote-frame, crypto (open/handshake/group-key/admin),
  journal (record/stable-value), session-envelope, node/gateway TOML and
  observability-HTTP decoders, a committed seed corpus, `scripts/fuzz_smoke.sh`,
  and two nightly jobs — `fuzz-groups` (asserts the four matrix legs' union is
  *exactly* the manifest's target set, and emits the matrix, so the checked
  list and the matrix are one object) and `fuzz` (600 s per target,
  `--min-runs 10000`, crash artifacts uploaded).
- **Five `uc_protocol` datagram readers made total** — they return `Option`
  instead of relying on caller guards.
- **The security package**: `docs/security/threat-model.md`,
  `attack-surface.md` (19 parser rows), `self-assessment.md`, plus
  `SECURITY.md` and a README **Security posture** / **Scope and limits**
  section. `docs/VERIFICATION.md` gains §7 Fuzzing.

**What the fuzzing found** (numbering matches the self-assessment):

1. **F1** — five caller-guarded readers panicked on short slices. Never
   reachable through the receiver, but the totality of the first code an
   unauthenticated UDP packet reaches held only by the discipline of five call
   sites. Pre-guards kept, hot path byte-for-byte unchanged (`112b81f`).
2. **F2** — `Sessioned::apply` violated the `out`-is-cleared contract it was
   itself a caller of: a contract-abiding inner state machine starting with
   `out.clear()` truncated the session tag away and the slice panicked **on the
   apply thread**, killing the service on its first command. User-reachable
   (`7c908b1`).
3. **F3** — `Sessioned::install_snapshot` pre-allocated up to 1 GiB from an
   unvalidated 8-byte length, using a sanity bound as an instruction. Bounded
   with `take()`; 20 000 executions went 91.8 s → 0.34 s (`7c908b1`).
4. **F6 (the harness finding).** Four of fourteen targets were executing ~16 inputs
   per 60 s run while printing a clean line — `llvm-symbolizer` needed ~90 s to
   index a 27 MB sanitizer binary for one address. `-print_funcs=0` fixed it
   (400 runs: 90 180 ms → 57 ms). **A fuzz tier can be green and vacuous**,
   which is why `--min-runs` exists and why CI asserts it (`736c1f3`).

**Rulings.**

- **Corpus is deterministic seeds only.** Every seed is generated by the real
  encoders in `fuzz/src/seeds.rs` — no captured traffic, no accumulated
  coverage corpus in the tree — so the corpus is reproducible from source and
  reviewable as code.
- **Miri is blocked on the rings, and each blocker was reproduced, not
  assumed.** Miri runs the *pure* decoders (`uc_protocol`'s `v2::` wire/cnc/ipc
  layer and `version` packing, 43 tests; `ultima_journal`'s segment and
  `stable_value` decoders, 19 tests) — 62 tests, all passing **with isolation
  left on**. The IPC rings cannot be checked: isolation on gives
  ``unsupported operation: `open` not available``; isolation off gives
  ``unsupported flags for `fallocate` … 16`` (`FALLOC_FL_ZERO_RANGE`, the M11
  block-reservation fix); past both, ``Miri does not support file-backed memory
  mappings``. The spec's fallback — a `Vec`-backed ring variant — was
  **deliberately not built**: it would check a different object than the one
  that ships. The gap is restated in `docs/VERIFICATION.md` §11.
- **Two seams exposed for fuzzing, with their posture stated.**
  `uc2_node::config_file::parse_str` and `uc2_gateway::config_file::parse_str`
  are ordinary public API (the loaders' pure inner half).
  `uc2_node::obs::http::route_raw` and `ObsSources::for_tests` are
  `#[cfg(any(test, fuzzing))]` and absent from a shipped build, with
  `check-cfg = ['cfg(fuzzing)']` declared so `clippy -D warnings` stays clean
  without promoting the seam to a Cargo feature (which would have made it API).
  `ultima_journal::fuzz_seams` is `pub` (a separate compilation unit cannot see
  `pub(crate)`) but `#[doc(hidden)]`.
- **`--min-runs 10000` is a stall floor, not a coverage bar.** It catches a
  symbolizer-class stall; it does not catch a target that has merely become
  100× slower. A tighter per-target bar needs per-target numbers from a runner,
  which do not exist yet.

**Two things documented rather than fixed.** (i) A malformed **query** frame
fail-stops a *typed* state machine pre-commit, from an unauthenticated client:
the blanket `RawStateMachine` impl decodes with `.expect("corrupt query frame
(fail-stop)")` and `apply.rs`'s query branch calls it while holding the SM
mutex, so one bad `QUERY` body panics the apply thread and poisons the lock —
no quorum, no leadership, no commit involved. The same `.expect` guards the
post-commit apply path, where fail-stop *is* right, so changing its error
semantics is a design decision; parked as a follow-up, with the raw tier as
the workaround. (ii) The `uc_protocol::ring` buffers have **no interleaving or
UB coverage at all** — an earlier draft's claim that loom covered them was
wrong and was corrected everywhere; the tree's one loom model
(`uc2_log/tests/loom_frame.rs`) checks the *log buffer's* frame-visibility
protocol, and nothing checks the MPSC claim-then-commit sequence or the
broadcast seqlock.

### Gate status at writeup time

| Row | Status |
|---|---|
| 1 remote lincheck capstone | green 3× locally under `hard-crash-tests`; **CI adjudication pending the next nightly** |
| 2 gateway throughput vs direct `Engine` (bar ≥ 0.8×) | **fleet-only, no fleet run yet** — the gate doc's local smoke ratios are labelled not-the-number |
| 3 codec share on the apply thread | **fleet-only, no fleet run yet** |
| 4 admin authn + audit + replay | **PASS**, per-PR CI |
| 5 quickstart from artifacts, no toolchain | **BUILT, partially proven** — container half is CI-only until the first `-rc` tag; aarch64 unclaimed |
| 6 artifacts and image verifiable | **BUILT, unproven** until the first `-rc` tag |
| 7 crates publishable | **PASS** for packaging (12) and publishing (4 leaves), with the stated dry-run caveat |
| 8 decoder fuzz job green | **BUILT, first nightly run pending** |
| 9 security package present | **PASS** — it claims the package exists and is honest, not that the system is secure |
| 10 external review | **pending**, user-scheduled |
| 11 MSRV floor real and enforced | **PASS** |
| 12 supply chain (advisories/licenses/bans) | **PASS**, one documented ignore |
| 13 `cargo fmt --check` gate | **DEFERRED** — the spec's own condition is not met (2 731 hunks, two open worktrees) |

### Upgrade

- **Per-host config edit, before the binary swap:** add `[crypto]` (with
  `enabled`) and `[admin]` (with `auth`) to every `node.toml`. Absence is a
  named startup refusal. `packaging/node.example.toml` ships both sections
  uncommented and annotated. Full remedy, including the paste that keeps
  today's posture unchanged: `docs/how-to/upgrade-a-cluster.md`.
- **No wire flag day.** `uc_protocol::version::CURRENT` is unchanged at
  `0.5.0`; the cnc page layout is unchanged (M12b's admin line sits in the
  existing reserved band at 3904). The binary swap is still run the way every
  upgrade in this system is run — everyone stopped together, per the how-to —
  but nothing in `2.6.0` *adds* a wire reason for it.
- **The `v2.5.0` instance-directory reservation is unchanged**: ~78 MiB at the
  defaults (`buffer_bytes` + ~14 MiB of rings), reserved at startup, refused
  loudly if unavailable.

## v2.5.0 — 2026-08-21 — M11 survivable cluster

**A cluster survives losing a host, losing quorum, filling its disk, and
being upgraded — and each of those is asserted by a test that actually
destroys something, not described in a runbook.** The milestone's own review
loop and its final gate row turned up four pre-existing journal-layer defects
and two IPC-layer ones; all six are fixed here.

- **Offline `uc2ctl backup` / `verify-backup` / `restore`.** The artifact is
  an ordered copy: state before journal before the log buffer, so a backup
  taken while the node is running under load and racing its own purge can
  still be proven complete. `verify-backup` asserts the purge-straddle
  coverage invariant rather than trusting the copy — a deliberately
  wrong-ordered artifact is reported as a `Hole`, which the gate's
  anti-vacuity test pins. All three verbs refuse a live instance directory.
  The acceptance case is a CI crashtest, not a procedure: a follower is
  backed up under load, its host is destroyed (`rm -rf`), a new host is
  restored from the artifact alone, and it rejoins and converges.
- **`uc2ctl force-single-member` for quorum loss.** An offline, explicitly
  non-persisting recovery wrapper: it states the data-loss window before
  writing anything, and refuses the doubly-ahead crash window outright.
  Dropped peers rejoin as fresh ids with fresh instance directories — the
  runbook's fresh-id rule, enforced rather than documented.
- **Full-disk fail-stop, asserted end to end**, plus a `free_disk_bytes` cnc
  field (reserved band 3840) and the `Uc2DiskLow` alert for the warning
  before the wall. This row is where the milestone earned its keep — see
  "What the ENOSPC row found" below.
- **`scripts/uc2_flag_day.sh`**: stop-all → verify every stopped node agrees
  on `durable` → run the operator's upgrade hook on every host → start-all →
  wait for one serving leader, with a measured downtime number and a
  load-bearing abort path (any failure on the way back up restarts every node
  on whatever binary is in place, so the cluster is never left down). Exit
  codes 0/1/3.

### What the ENOSPC row found

The gate's true-`ENOSPC` row (3b) could not run on the dev box for lack of
passwordless sudo and was carried as `SKIPPED-PENDING`. Its first real CI run
failed — and the pending status turned out to have been concealing a test
that could never have passed, followed by two genuine product defects:

1. **The test could not induce the fault.** Its load is a single serial CAS
   writer, measured at ~15.2 KB/s of instance-dir growth; the fixture left
   8 MiB of headroom, which needs ~550 s to exhaust against a 60 s bound. The
   test now squeezes the remaining space itself after warm-up
   (`squeeze_free_space`, leaving 256 KiB — ~17 s at the measured rate), with
   a 1 GiB interlock so an operator-supplied `UC2_ENOSPC_DIR` pointing at a
   real volume aborts instead of filling it.
2. **A full disk killed processes with `SIGBUS` instead of fail-stopping.**
   `uc_protocol::ring::create_shared_backing_file` zeroed via
   `FALLOC_FL_PUNCH_HOLE`, which keeps the mapped files sparse by design
   (measured: `log.buf` 1 MiB apparent / 80 KiB allocated). A sparse mapping
   has pages with no block behind them, so the first write to such a page
   allocates at **page-fault time**; on a full filesystem that fails, and the
   kernel raises `SIGBUS` — not an `io::Error`, so it cannot be returned,
   matched, or handled. It kills whichever process touched the page — node,
   service, *or* client, since all three map these files — and the documented
   fail-stop chain (journal halt → `ArchiveError` → `agent_failstopped` →
   exit 1) never runs. Observed directly: `code=None signal=Some(7)
   core=true` with no `agent_failstopped` in stderr, and separately the test's
   own client process taking the `SIGBUS` instead.
   **Fixed** with `fallocate(FALLOC_FL_ZERO_RANGE)`, which zeroes *and*
   reserves the blocks as unwritten extents — no zeroes are written, so
   startup stays fast — moving the failure to `fallocate`'s return value,
   where the daemon already refuses to start with a named error. Aeron
   reaches the same answer from the same constraint: sparseness is a knob
   there (`aeron.term.buffer.sparse.file`, "save space at the expense of
   latency") and storage checks are on by default
   (`FileStoreLogFactory.checkStorage`, *"insufficient usable storage for new
   log of length="*). The `fallocate` form is stronger — Aeron's
   `getUsableSpace()` check is look-then-leap and races; a reservation either
   succeeds or reports `ENOSPC` atomically.
   **Upgrade note:** these files are no longer sparse. A default instance
   directory reserves ~78 MiB at startup (64 MiB log buffer + ~14 MiB rings),
   and a node that cannot reserve it refuses to start.
3. **Even a correct fail-stop did not say why.** `ultima_journal`'s segment
   preallocator replaced the underlying `io::Error` with
   `Error::other("segment preallocation failed")`, so a full disk halted the
   node without ever naming `ENOSPC`. The failing error's kind and errno are
   now captured and rebuilt for each waiter. This was latent for *every*
   preallocation errno, not just this one.

With all three fixed, row 3b passes as written — named `StorageFull` /
`os error 28`, daemon exit 1, survivors committing throughout, and node 0
rejoining and converging once space is returned — locally and in CI's
`survival` job.

### Journal-layer fixes from the review loop

- **A crash-torn tail refused boot.** `Journal::open` now heals a torn tail on
  the active segment instead of refusing, and zeros the healed span through
  physical EOF so the residue cannot wedge the next truncate.
- **A masked acked-durability hole at segment rolls**: a rolled-off segment is
  now fsynced before its successor exists, making the acked-durability
  guarantee real at the boundary.
- **A latent writer panic**: the dirty flag survives truncation, so an emptied
  segment list no longer panics.

### Gate

`docs/benchmarks/uc2-m11-gate-2026-08-20.md`. Six rows, bar pre-committed at
plan commit `7ff6b4b` before implementation and never edited. Rows 1, 2, 3a,
3b, 4 local/CI; row 5 fleet-only, measured at **14.007 s and 14.709 s**
against a 60 s bar on a 4-host `c6id.xlarge` fleet in us-east-1, with equal
durable positions across every stopped node, no committed-high-water loss,
and 314 KB/s of new writes committed after the upgrade. Driver:
`bench-infra/scripts/m11_fleet_gate.py` — a new one, because every earlier
fleet gate launches nodes as transient `systemd-run` units, which cannot
serve `uc2_flag_day.sh`'s `systemctl start` after its `systemctl stop`; the
M11 fleet installs the shipped `packaging/systemd` unit instead. Three rows
were recorded FAIL on the way and diagnosed before re-running, including one
worth remembering: GNU `install` truncates its destination in place rather
than unlinking it, so an inode-equality witness reports "never replaced" for
a successful install.

Two limits of the fleet row, stated rather than implied: it ran on 4 hosts,
not 5 (the account's 32-vCPU cap, plus three instances that booted with no
networking), and the upgrade installed a byte-identical binary, since there
is one tree — so it measures downtime, not cross-version interoperation.

## v2.4.0 — 2026-08-20 — M10 observable cluster

**A running cluster can now be watched, probed, and alerted on without
touching the source.** Metrics, structured logs, health probes, and shipped
alert rules — the whole layer reads state the hot path already publishes, and
the fleet gate measured its cost at ~1.7% under a 1s all-nodes scrape.

- **An in-daemon observability endpoint** (`[metrics]` config section, off
  when absent): `GET /metrics` (Prometheus text, 62 metric families —
  commit/apply/replication lag, admission saturation, heartbeat ages, per-peer
  lag on the leader, and every repair/drop/crypto counter), `/healthz`
  (liveness: the four agents alive + node heartbeat fresh), `/readyz`
  (role-aware readiness). Hand-rolled over `std::net`; zero new dependencies;
  the exporter reads the same atomics the agents publish — no lock the hot
  path can contend on.
- **Readiness keys on `can_serve`, never the leader flag.** The elected-but-
  not-serving `0x01` window is exactly what a naive `leader == true` probe
  gets wrong; the fleet gate killed leaders three times and never observed a
  ready response from a node in that state.
- **Transition-triggered structured logging** (`[log]` section): one JSON
  line per election, truncation, snapshot install, config adoption, removal,
  NAK storm, seal-failure burst, snapshot publication — never one per
  operation. The daemon now also **fails fast when an agent fail-stops**
  (exit 1 for systemd to restart) instead of lingering as a healthy-looking
  zombie.
- **Shipped ops artifacts**: `packaging/prometheus/uc2-alerts.yml` (13 rules,
  every one proven to fire against a deliberately broken cluster via
  promtool; the per-peer rules are leader-scoped — the peer band is
  leader-authoritative and followers export zeros), a Grafana dashboard, and
  `docs/how-to/monitor-a-cluster.md`.
- **Fleet gate** (`docs/benchmarks/uc2-m10-gate-2026-08-20.md`): a 10-minute
  healthy soak under a real Prometheus fired zero alerts with full series
  coverage from every node; the scrape-perturbation A/B held at median 0.9830
  against a pre-committed >= 0.95 bar; wire-0.5.0 hygiene held
  (`reports_unattested` 0 everywhere). Runs 1-2 were honest failures —
  harness defects, recorded in the gate doc, including one operational
  finding worth knowing: the journal holds an fd per segment, so keep the
  packaged unit's `LimitNOFILE` and enable purge for long-lived clusters.

No wire, cnc-page, or consensus changes. `[log]`/`[metrics]`, reserved in
v2.3.0, now have their schema — unknown keys inside them refuse at boot like
everywhere else.


## v2.3.0 — 2026-08-19 — M9 deployable node

**UC is now deployable by someone who is not the author.** Before this tag the
only way to start a node was an example binary configured in Rust source; the
docs described a daemon the build did not produce. M9 ships it.

- **A real `uc2-node` daemon.** Starts from a TOML config file
  (`packaging/node.example.toml` is the shipped reference;
  `docs/reference/configuration.md` documents every field). The file is a
  one-to-one mirror of `NodeConfig` with `deny_unknown_fields` — a typo is a
  startup refusal naming the key, not a silently-ignored setting. `[log]` and
  `[metrics]` are reserved for M10: parsed, announced as inert on every boot,
  never silently swallowed. `seed` defaults to a distinct per-id derivation so
  operators cannot livelock a cluster through identical election timers.
- **Named startup refusals.** Every rule that used to fail later and look like
  something else now refuses at boot with the offending field named: `bind`
  must equal this node's own members entry (the mismatch that elects a leader
  whose followers never commit); `max_payload` must fit one datagram against
  the MTU (the assert that used to panic inside the sender); `buffer_bytes`
  power-of-two; membership disjointness/uniqueness/8-cap; election window
  ordering; and an instance_dir on a RAM-backed filesystem is refused **by
  name** — every fsync there is a silent no-op. The tmpfs override
  (`allow_volatile_fs` / `UC2_ALLOW_VOLATILE_FS`) is never silent: the node
  warns on every boot it is active.
- **Clean lifecycle.** `SIGTERM` → bounded archive drain → exit 0, so a planned
  restart rejoins from the journal instead of paying reconstruction. Packaged
  systemd units: `TimeoutStopSec=10` (room for the drain),
  `RestartPreventExitStatus=2` (a config refusal is not retried into a restart
  loop), and a `BindsTo=` service unit so the service's lifecycle follows its
  node's.
- **Service-binary template.** `docs/how-to/write-a-service-binary.md` plus the
  `counter` example's SIGTERM handling and `is_alive` supervision — the shape a
  user's crate instantiates. Docs are cut over from example binaries to the
  packaged daemon.
- **Fleet-gated** (`docs/benchmarks/uc2-m9-gate-2026-08-19.md` is the record,
  including run 1's honest FAIL and its diagnosis — the harness's load model,
  not the cluster): leader stop under load **0.042 s, exit 0**; restart rejoins
  with **no snapshot install** (snapshot builds proven at ~25 MB alongside);
  commit rate recovered by **10.5 s observable** against a pre-committed 15 s
  bar (the observable figure is plumbing-dominated — an upper bound). Cluster
  switchover after a leader stop is **≈0.4 s** (derived from the ungated
  8.5 % × 5 s dip window).
- **Deployment model, stated plainly.** `uc2_client` is a same-host SDK: the
  intended shape is one app client per node — the leader's serves requests, a
  follower's answers its callers with a redirect to the leader
  (`NotLeader` carries a leader hint). Place `instance_dir` on a real disk;
  the node now refuses tmpfs by name.

**Rollup.** v2.3.0 is the first tag since v2.1.0 and therefore ships everything
below it: wire protocol **0.3.0** (post-M7 hardening, including the three
consensus safety fixes found by the Lean effort), **0.4.0** (M8 wire crypto —
opt-in and **off by default**; its cross-host fleet A/B remains a separate open
step, which is why no v2.2.0 was cut), and **0.5.0** (content-attested durable
reports, a consensus safety fix; **flag day** — upgrade all nodes together, a
mixed cluster stalls commits rather than committing unsoundly). Also since
v2.1.0: the pipelined client SDK (the public `Engine`/`PipelinedClient` tiers)
and the Rung A linearizable-read batch-probe rounds (~953k lin reads/s @ p50
1.08 ms mixed on the read-profile fleet).

## Shipped in v2.3.0 — wire protocol 0.5.0 — content-attested durable reports

**Consensus safety fix. Flag day for node↔node traffic: run one version.**

A follower's `AppendPosition` report used to carry a POSITION only — "I hold
this many bytes" — with nothing saying WHICH bytes. A leader ranking those
reports was therefore taking a position quorum, not a content quorum, and a
replica holding a deposed leader's copy of the same byte range counted toward
committing the current leader's history. Under rapid leader churn that
certified commits no live quorum backed; a later leader then truncated a
follower BELOW its own commit counter, and the service applied — and could
serve — bytes from a dead timeline.

- **Wire protocol → 0.5.0** (`version::CURRENT`). `DGRAM_KIND_APPEND_POSITION`
  gains an 8-byte body (`AppendPositionBody`) carrying `durable_term`: the term
  the sender attributes to the byte below its reported position. The 16-byte
  header is UNCHANGED, and the `cnc.dat` page is untouched
  (`CNC_V2_VERSION` unmoved), so service/client binaries are unaffected.
- **Leader-side check.** A report whose `durable_term` disagrees with the
  leader's own term map is declined (counted in `reports_unattested`). Equal
  terms at the same position imply identical prefixes (Log Matching), so this
  is the `(index, term)` pair Raft carries — it upgrades the ranking to a
  content quorum.
- **Mixed-version behaviour.** A 0.4.0 peer's header-only report decodes as
  *unattested* and is not counted. A mixed cluster therefore STALLS commits
  rather than making unsound ones — safe, but it means upgrading all nodes.
- Companion fixes in the same arc: the tracker's per-follower slot takes the
  latest report instead of a high-water mark (a follower's durable regresses
  when it truncates); term observations are delivered losslessly; the SM's
  durable is clamped to its term-observation frontier; and the follower's
  commit advance and its reports are both bounded by a validated frontier.

Measured on the directed rig (`uc2_node/tests/stale_read_hunt.rs`, 300 s of
500 ms-cadence leader kills): log rewinds beneath the applied frontier went
from 11 per run to **0**, with zero acked-write loss throughout.

## Shipped in v2.3.0 — wire protocol 0.4.0 — M8 wire crypto

**Opt-in, off by default.** Authenticated + encrypted node↔node UDP transport.
A cluster runs either all-encrypted or all-cleartext — **flag day, no mixed
mode**. Nothing changes for a deployment that does not set `CryptoConfig`.
Design: `docs/superpowers/specs/2026-07-28-uc2-wire-crypto-design.md`. Gate:
`docs/benchmarks/uc2-m8-gate-2026-07-29.md`. Operator setup: runbook §11.

- **Identity + handshake.** Each node holds an X25519 static keypair; peers are
  authorized by an allowlist (`node id → static public key`, SSH
  `authorized_keys`-style, re-read at runtime so M7 node-adds need no restart).
  Noise `IK` (`Noise_IK_25519_AESGCM_SHA256`, via `snow`) establishes per-peer
  pairwise keys; the allowlist is enforced explicitly on the responder side.
- **Two key scopes, split by datagram kind.** Pairwise keys seal the unicast /
  low-rate kinds; a **cluster group key** seals the byte-identical fan-out
  (`DATA`/`HEARTBEAT`/`COMMIT_POSITION`/`READ_PROBE`) so the leader seals once
  and sends N times. The group key is minted by the leader, delivered per peer
  over the pairwise channel, and **rotates** on becoming leader, on a timer /
  byte budget, and on a committed `Remove*`.
- **Wire envelope.** The 16-byte datagram header stays cleartext and is
  authenticated as AES-256-GCM **associated data** (so `position`/`term`/`kind`/
  `key_epoch` cannot be rewritten undetected); an 8-byte per-sender counter and
  a 16-byte tag follow the payload — **24 bytes overhead**. The nonce is
  `0 ‖ counter` under a key derived **per sender per boot**
  (`HKDF(group_key, sender_id ‖ boot_salt)`), which makes counter reuse after a
  restart impossible by construction. RFC-6479 sliding-window anti-replay per
  `(sender, epoch)`.
- **Wire protocol → 0.4.0** (`version::CURRENT`). The `cnc.dat` page layout and
  its live `CNC_V2_VERSION` compatibility gate are **unchanged** — M8 changes
  the UDP datagram format, not the shmem page, so a 0.4.0 node's service/client
  IPC still accepts the older peers it did before. A new cnc observability
  field (`seal_failures`) is added in the reserved band.
- **Threat model.** A network-path adversary (read / inject / replay / reorder /
  corrupt, no node private key). **Out of model, documented residuals:** a
  compromised host; a malicious cluster member (the group key is symmetric, so
  any holder can forge fan-out traffic as any node); a removed node retains
  decryption of captured traffic until the next rotation; cleartext headers
  leak positions/terms/kinds to a passive observer.
- **Boot refusal.** An `Enabled` node whose key files are missing or unreadable
  refuses to start (it must not silently fall back to cleartext).
- **Correctness.** The full local proof stack and all four capstones
  (`lin_v2`, `lin_partition_v2`, the multi-process SIGKILL crashtest, and the
  elle tier under both models) pass with crypto ON, with the anti-vacuity of
  "crypto was actually on" proven by mutation (T15). Deterministic sim coverage
  of the handshake under loss/partition and key rotation (T13); an adversarial
  tier proving a replayed VOTE is refused, a revoked/impostor peer cannot
  establish, a cleartext downgrade is refused, and a corruption+replay storm
  neither panics nor diverges (T14).
- **Throughput (local same-box A/B, gate doc):** encrypted median **94.1%** of
  the cleartext control — a **5.9% regression, PASS** against the pre-committed
  ≤10% bar — on a deliberately worst-case contention box (3 in-process nodes,
  4 cores). Hardware AES-NI dispatch verified (8.2× vs a forced-software build).
  The definitive absolute number is the cross-host fleet A/B, owner-approved
  separately.
- **Known benign observability wart:** on an encrypted leader, the in-window
  `seal_failures` counter climbs continuously — the receiver reports its
  position to `cfg.leader`, which on the leader is *itself*, and there is no
  self-session, so each self-addressed report fails to seal. Pre-existing v2
  self-send made visible by the counter; harmless (the leader's position
  reaches commit ranking in memory). A follow-up will suppress the
  self-addressed report.
- **Deferred / follow-up:** the lock-free `sealing_epoch` fast path (not needed
  — arm A passed); suppressing the leader self-send; a release-mode OOB-read in
  `uc2_log`'s `read_frame_validated` (`debug_assert!`-only bounds guard,
  pre-existing v2 code from `72f649b`, out of M8 scope, surfaced during T14).

*The 0.3.0 items below shipped in the same tag (v2.3.0); 0.5.0 supersedes the
version number.*

## Shipped in v2.3.0 — wire protocol 0.3.0
Post-M7 follow-up hardening (no new externally-visible features). Wire protocol
bumped **0.2.0 → 0.3.0**, additive only:
- cnc-page `admission_bytes` field pinned at offset 3712.
- admin reply reason codes **11** (malformed/unknown op) and **12**
  (self-demote refused).

A 0.3.0 node accepts a 0.2.0 peer (same major, peer minor not newer — see
`cnc::version_compatible`, the live gate; `version::CURRENT`/`MIN_COMPATIBLE`
are documentation-only and enforce nothing).

Safety fixes in this line:
- **Commit advance was not clamped to the current term's NewTerm base — a
  Raft §5.4.2 / Figure-8 acked-write-loss window** (Finding #6b, lean
  leader-completeness effort; affects all prior v2 releases): the leader's
  commit ranking (`rank_leader`) advanced/stored/gossiped off the
  positions-only `CommitTracker` unconditionally — `new_term_pos` (the NewTerm
  no-op frame appended at every election) gated only linearizable reads,
  ingress admission, and M7 proposals (`serving`), never the commit store. At
  any failover inheriting an uncommitted tail, followers reconcile clean and
  their 20 ms AppendPosition floor reports the election base BEFORE the
  NewTerm frame is quorum-durable, so the leader could commit (and ack, apply,
  fire outputs for) an OLD-TERM-ONLY range; a divergent higher-lastTerm rival
  could then still win the next term with a commit-quorum member's grant
  (their data-stamped `last_term` had not yet reached the new term) and
  truncate the committed bytes cluster-wide. The loss continuation needs a
  rival's vote datagrams to beat the in-flight NewTerm byte to a voter — a
  real race under loss/NAK repair — but the unsafe commit itself fires in the
  normal post-reconcile path; never observed outside the directed
  reproductions (no production deployment exists — pre-release fix). Fixed:
  `rank_leader` now advances/stores/gossips ONLY once the ranked position
  covers `new_term_pos` (Raft §5.4.2: never commit a prior-term range by
  counting replicas; cost: commit stalls at most one NewTerm replication round
  per election, which the read path already paid via `serving`). Found by the
  Lean commit-certification model (46-step kernel-checked Figure-8
  countermodel), reproduced RED-first and pinned by the sim
  (`old_term_range_must_not_commit_before_new_term_quorum`, inv2 at the
  violating advance) plus a `uc2_consensus` unit pin
  (`commit_clamped_to_new_term_base_never_certifies_old_term_only_range`).
  Remedy: upgrade; no back-port is planned.
- **Intake-gate reopen was keyed to `current_term`, not the data-plane term
  handle — a candidate cross-stream accept / acked-write-loss window**
  (Finding #9, lean LC-closure effort; affects all prior v2 releases): the
  receiver filters inbound DATA on the node-level `term_handle`
  (`receiver.rs:635` `dropped_stale_term`), but both intake-gate REOPEN sites
  keyed off `current_term` — the clean-reconcile arm (`node.rs` feed,
  `t >= sm.current_term()`) and the truncation-ack arm (`on_truncated`). A
  CANDIDATE's handle LAGS its `StartElection`-bumped `current_term`
  (`Action::StartElection` stores no handle, `node.rs:2440-2450`), so a
  candidate that adopted term T (handle T, gate closed), campaigned to T+1,
  then cleanly reconciled a term-T+1 leader's map REOPENED intake for its
  stale handle-T stream — and then accepted a term-T `serveTail`/NAK-repair
  byte its own term map never attributed (a cross-stream write), which its
  role-blind AppendPosition report (`receiver.rs:1049-1078`, retargeted to the
  new leader) could then feed into a commit over content that leader does not
  hold (§5.4.2 / Figure-8 acked-write-loss family, same class as #6b).
  Requires a candidate with a lagged handle + a clean higher-term reconcile +
  a co-term leader ranking the report; never observed outside the directed
  reproduction (no production deployment exists — pre-release fix). Fixed:
  BOTH reopen arms now fire only when `current_term == adopted_term` (== the
  `term_handle` the receiver filters at); a candidate's data intake stays
  CLOSED until it resolves (win / step-down / higher-term adoption re-keys the
  handle), costing nothing in steady state (followers always satisfy the
  equality). Found by the Lean LC-closure model (`n=5`, 56-step kernel-checked
  countermodel `finding_candidate_gate_reopen_fca_violation`, later deleted
  with the fix), reproduced RED-first and pinned by the sim
  (`finding9_lagged_handle_candidate_reopen_needs_handle_keyed`: the
  `handle_keyed:false` counterfactual reopens a lagged-handle candidate's gate,
  the shipped `handle_keyed:true` keeps it closed + converges). Remedy:
  upgrade; no back-port is planned.
- **Boot-open intake gate could certify a phantom commit** (Finding #5, lean
  leader-completeness effort; affects all prior v2 releases): a voter that
  granted a term-T vote (persisted), held a divergent tail, and crashed before
  reconciling rebooted with the receiver intake gate OPEN — its 20 ms
  AppendPosition floor report (raw divergent durable, stamped term T) could
  reach the T-leader before the 100 ms idle term-map re-ship and be counted
  toward quorum commit over content the reporter does not hold (worst case:
  committed-acked write loss after a leader crash). Requires the 4-way
  conjunction divergent-tail voter + persisted vote above the data-stamped map
  + crash before reconcile + report-beats-gossip; never observed outside the
  directed reproduction. Fixed: the gate (and the reconcile latch) now boots
  CLOSED iff the recovered vote term exceeds the data-stamped term map's last
  term, reopening via the existing reconcile paths (cost: one extra reconcile
  round after such a reboot). Found by the Lean commit-certification model
  (machine-checked countermodel), reproduced and pinned by the sim's inv7
  phantom oracle (`rebooted_unreconciled_voter_must_not_certify_phantom_commit`,
  RED pre-fix → GREEN post-fix). Remedy: upgrade; no back-port is planned.

Loose-end hardening in this line:
- **Leader-as-learner wedge closed** (T1): a leader that adopts its own demote
  from the log now relinquishes leadership to a non-voting learner-follower once
  the demote commits (a commit-triggered step-down mirroring self-removal),
  instead of leading-as-a-learner until an operator intervened. Safety was never
  affected; this removes the silent liveness wedge.
- **Config observations delivered losslessly** (T5): a dropped config-frame
  observation could silently run stale membership until a restart; delivery is
  now lossless.

## v2.1.0 — 2026-07-14
M7 live single-server reconfiguration (promote/demote/add/remove under load,
no restarts, `uc2ctl` admin path, tombstone-based fresh-forever ids, leader
self-removal). 5-host fleet gate passed: worst transition dip 4.7% (<10%),
self-removal gap 3.22 s (<10 s), zero loss/divergence, snapshots+purge paired.
Wire protocol 0.2.0 (FRAME_TYPE_CONFIG=4, admin datagram kinds 16/17).

## v2.0.0 — known issues
- **MPSC ingress ring free-space underflow under producer contention**
  (clients→node ingress only): a stale `claim_pos` snapshot overtaken by the
  consumer could underflow the free-space computation — debug builds panic,
  release builds see spurious backpressure. **Not data corruption** (the CAS
  re-validates before any write). Fixed in v2.1.0 (8c1ae01, regression test
  98900fd). Remedy: upgrade to v2.1.0; no v2.0.1 is planned.
