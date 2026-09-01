# Backlog — candidate directions after M14

*Written 2026-09-01 against `v2.10.0`. Status: a ranked list of options, not
a plan. Nothing here is scheduled; the maintainer picks. Every item cites the
document that first recorded it, so the reasoning can be re-checked rather
than re-derived. When an item is taken up it gets a spec under
`docs/superpowers/specs/` and a gate doc under `docs/benchmarks/`, and its
line here is updated to point at them. When an item is dropped, say why
here rather than deleting it — the "Deprioritized" section of
`docs/superpowers/specs/2026-08-01-uc2-formal-roadmap.md` is the model.*

## Where this list comes from

M8–M14 turned the v2 engine into a deployable product (`RELEASES.md`). What
the record does not contain is a user: the only in-tree service is the
`examples/counter` crate, `docs/reference/remote-protocol.md` still describes
itself as "the page a non-Rust port implements from", and no port exists.
Every remaining gap the docs record is either an accepted residual
(`docs/reference/limits.md`, `docs/security/self-assessment.md`) or a
deferral waiting for a reason to matter. The ranking below follows from
that: the first direction is the one that supplies the reason.

## Ranked directions

### 1. Dogfood — a real service plus a second-language client

Build one non-trivial reference service in-tree (a sessioned key-value
store or an order book), and a remote client in a second language (Go or
Python) written from `docs/reference/remote-protocol.md` alone, as that page
invites. Run both through the gate discipline.

- **Why first:** it exercises the whole M8–M14 surface the way a user would,
  and it settles two questions the docs cannot settle on their own:
  - whether the **command payload ceiling** (≤ 1344 B crypto-off / ≤ 1312 B
    crypto-on, one command per datagram — `docs/security/attack-surface.md`
    §3, `CLAUDE.md` standing facts) is a real adoption blocker. Moving it is
    a wire flag day (fragmented commands, jumbo frames, or an OS-bypass
    fabric), so it needs a workload to justify it.
  - whether the **remote protocol needs a v2**: `SUBMIT`/`QUERY` carry no
    FSM selector, so a remote client reaches only FSM 0
    (`docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md` §11 "Out
    of scope"; `docs/releases.md` 2.8.0 entry).
- **Cost:** moderate. **Output:** a backlog grounded in use, plus the two
  decisions above.

### 2. Rolling upgrades and leadership transfer

The two operations items `docs/superpowers/specs/2026-08-19-uc2-production-readiness-design.md`
deferred by name:

- **Version negotiation / upgrade window.** Today every node↔node wire or
  `cnc.dat` change is a cluster-wide flag day
  (`docs/how-to/upgrade-a-cluster.md`, `docs/reference/semver-policy.md`).
  The spec's reason for deferring: a negotiated floor becomes
  consensus-relevant state, which is real design work, not a script.
- **Leadership transfer.** A planned leader stop costs one election
  timeout. Needs a new protocol message (a Raft `TimeoutNow` analog); the
  spec calls it "a consensus change wearing an operations hat; gets its own
  spec or none at all."
- **Crypto-on-by-default** was parked "revisit at M12, not before" in the
  same spec and never revisited; it belongs in this milestone.
- **Why:** the flag-day rule is the limit an operator hits first. This is
  the gap between "deployable" and "operable at scale".
- **Cost:** high — both items touch consensus and are a wire flag day
  themselves.

### 3. Geo — async cross-region learner with a stale-read mode

`docs/notes/uc2-m7-vs-aeron-cluster-standby-2026-07-24.md` found that a
UC learner is already most of an Aeron Cluster Standby, and sketched a
phased shape: (prereq) wire crypto → Phase A stale-read query mode off a
learner → Phase B learner-as-relay → Phase C DR failover as a *separately
scoped* consistency weakening.

- **Status of the prerequisite:** met — wire crypto shipped in M8
  (`v2.3.0`).
- **Why:** the largest capability gap against the stated comparator; Phase A
  is mostly additive and low-risk per the note.
- **Cost:** moderate for Phase A; Phase C is a product decision before it
  is code.

### 4. Verification debt

`docs/VERIFICATION.md` §11 and `docs/superpowers/specs/2026-08-01-uc2-formal-roadmap.md`
record what is not proved:

- **`leader_completeness`** — the roadmap's HIGHEST-priority task (F-UC-1,
  ≈ 7–12 S2-equivalents), not started. The joint-induction blueprint is in
  the phase-2 memo; the sole named open theorem in the corpus.
- **The Lean model collapses the durable counter's two readers into one**
  (issue #7). A real acked-write-loss bug lived in exactly that gap and was
  found from the Rust side. Proofs composed over that lemma are weaker than
  they look until the split lands.
- **SPSC and the futex layer have no loom model**; MPSC and Broadcast do,
  and the Broadcast model found a real weak-memory defect the day it was
  written (2026-08-31). The mmap itself is outside loom and Miri; a
  Vec-backed variant for Miri "has not been built, and that trade-off is
  recorded rather than resolved".
- **aarch64 tests in CI.** Binaries are built, tests never run; the full
  stack has passed on Graviton exactly once
  (`docs/benchmarks/uc2-arch-sweep-c8id-vs-c9gd-2026-08-31.md`). A one-time
  pass is a data point, not a regression gate.
- **Term-map follow-ons** from
  `docs/notes/uc2-term-map-window-loss-explained.md`: commit-floor anchoring
  of the wire window, election credential floors for wiped nodes, a
  persisted commit watermark (the truncation-below-commit defence forgets
  state across reboot).
- **Why:** every proof gate so far found exactly one real bug; this is the
  direction most likely to find the next one.

### 5. Performance, round three

All framed by the docs as characterisation, not defects:

- **One remote connection on Graviton is 0.498× direct against a 0.5×
  bar** — `docs/benchmarks/uc2-m13-remote-on-arm-2026-09-01.md` ("FAIL by
  0.2 %", on c6id-era bars; the c6id gate itself remains PASS).
- **M14 gate row e has never been re-measured**, and the pinned rig's
  residual 14.3 % spread is undiagnosed
  (`docs/benchmarks/uc2-m14c2-fleet-pinning-2026-08-30.md`,
  `docs/VERIFICATION.md` §11).
- Recorded follow-ons: sharded per-client ingress and demand-weighted
  credits (`docs/superpowers/specs/2026-08-24-uc2-m13-remote-path-design.md`
  §8 "Follow-ons (not M13)"), service-side raw passthrough and per-slot response-buffer
  reuse (`docs/superpowers/specs/2026-08-13-uc2-pipelined-client-design.md`
  §10 "Out of scope / deferred"), the Rung B time-based leader lease
  (`docs/superpowers/specs/2026-07-24-uc2-leader-lease-design.md`,
  discharged for the LAN goal; only for WAN reads).
- **Why not first:** no user is asking for more than the current ceiling;
  every number here is a fleet characterisation with its caveats disclosed.

### 6. External review

`docs/security/self-assessment.md` §4 "What an external review should focus on" ranks seven
areas for outside eyes, led by the pre-auth UDP dispatch with crypto OFF and
the `snow` handshake state machine under interleaved malformed messages.
Cheap relative to the surface it covers; pairs with direction 1, since a
reviewer wants a workload to attack.

## Small items, worth doing regardless of direction

- **Release-mode bounds guard in `read_frame_validated`**
  (`uc_log/src/buffer.rs`): the check ahead of the `unsafe` slice read is
  `debug_assert!` only. Deferred in the M8 release notes as pre-existing
  code (`docs/releases.md`, v2.3.0 "Deferred / follow-up"). The code's own
  safety analysis says it is reachable only via a corrupted commit word or a
  mid-frame position, so this is hygiene, not a live defect — and a one-line
  check.
- **`nightly.yml` has never run on the `v2.10.0` tag commit**
  (`docs/releases.md`, release-evidence table).
- **`uc2-gateway --version`** — fixed on `main` after the tag, lands in the
  next release (`docs/releases.md`).
- **Leader self-send `seal_failures` wart** — the encrypted leader's
  self-addressed position report fails to seal and counts; harmless,
  suppression deferred since M8 (`docs/releases.md`, v2.3.0).
- **Minter-local epoch collision** after leader change — transient DATA
  loss, NAK-repaired, "a nice-to-have, not a safety break"
  (`docs/notes/uc2-m8-formal-methods-followups.md`).

## Accepted residuals — listed so they are not re-proposed

These are decisions with a reason, not forgotten work. Do not reopen one
without a new argument:

- The four wire-crypto residuals (cleartext headers; a removed node keeps
  decryption until the next rotation; any group-key holder can forge fan-out
  traffic; no compromised-host story) —
  `docs/superpowers/specs/2026-07-28-uc2-wire-crypto-design.md` §7,
  `docs/reference/limits.md`.
- Admin HMAC is cluster-wide only with `[crypto].enabled = true` (the
  kind-16 forward plane) — closing it is a wire change
  (`docs/notes/uc2-admin-authentication.md`).
- The typed tier's pre-commit query decode fail-stops on a malformed frame —
  documented, not changed, in M12d (`docs/security/self-assessment.md` §3).
- `bincode` unmaintained (RUSTSEC-2025-0141) — no patched version exists;
  one documented `deny.toml` ignore.
- Twelve-factor #6 (stateless processes) is opposed by design; #5's release
  ledger and #8's "simple" horizontal scale are partial by the nature of a
  consensus system (`docs/notes/uc2-twelve-factor-assessment.md`).
- `--pin` stays opt-in: pinned spread 14.3 % against a < 5 % bar, and it
  costs 9.4 % of mean throughput
  (`docs/benchmarks/uc2-m14c2-fleet-pinning-2026-08-30.md`).
- Lockstep-mode collapse under CPU oversubscription is an operating-envelope
  fact, not a defect
  (`docs/benchmarks/uc2-m14c2-lockstep-oversubscription-2026-08-30.md`,
  `docs/reference/limits.md`).
