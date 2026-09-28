# UC2 mandatory snapshots — design (#67)

**Status:** design approved in conversation on 2026-09-28. This document is the written spec for review. **Issue:** [#67].

## 1. Problem

Today snapshots are optional. `ServiceBuilder::start()` attaches a service
with no snapshot support. `start_with_snapshots()` is the opt-in and needs
`S: SnapshotStateMachine` (`uc_service/src/lib.rs:212`, `:318`).

After [#33] that choice is a dead end:

- Every change to a row's version is a pin.
- A pin needs a snapshot-capable row (refusal 48, and the attach refusal
  `PinRequiresSnapshots`).

So a service started with `start()` can never change version. More broadly,
a long-running cluster without snapshots cannot purge its journal and cannot
rebuild a fresh or restarted FSM in reasonable time. This is the #33 spec's
§3.1 arithmetic: at 10 % of the measured single-FSM ingest,
`docs/benchmarks/uc2-m14-gate-2026-08-29.md:42`, the log grows by ~1.5 TB
per day, and the measured replay rate, same doc `:415-422`, needs ~7.5 days
per month of history.

Snapshots are therefore not an option in practice. The SDK should say so.

## 2. Scope

**In:**

- One start method, which requires snapshot capability at compile time.
- A codec-neutral helper that makes snapshot support cheap for any FSM whose
  whole state can be encoded to bytes.
- Migrating every in-tree caller.
- Docs.

**Out:**

- **The typed-tier codec.** The typed tier stays serde + bincode. Its
  replacement by SBE is deliverable 2 of the lifecycle spec
  (`2026-09-19-uc2-fsm-upgrade-lifecycle-design.md` §11) and has its own
  spec. This design must not tie snapshot encoding to either codec.
- **The `uc_starter` template.** It lives in a separate repo, and its
  `docs/how-to/remove-sessions-or-snapshots.md` must drop the "remove
  snapshots" path. That is a follow-up in that repo; this spec only records it.
- **No node, wire or cnc change.** The node's runtime refusals stay as a
  backstop for a process that attaches without the Rust SDK: 48
  `snapshot_unsupported`, and `PinRequiresSnapshots` at attach.

## 3. Decisions

| # | decision | why |
|---|---|---|
| D1 | Snapshots are enforced at **compile time**: one `ServiceBuilder::start()` requiring `S: SnapshotStateMachine`. `start_with_snapshots()` is removed. | A service without snapshots should not build. Runtime refusal would surface in production. |
| D2 | **No deprecation period.** | There are no external users, so the API is free to change. |
| D3 | A **codec-neutral** helper trait. The app owns the encoding and the SDK owns the snapshot mechanics. | Serde/bincode is slated for replacement (deliverable 2). SBE suits commands, not arbitrary state. The error-prone part of a snapshot implementation is the frontier and cursor logic, not the encoding. |
| D4 | The helper is **opt-in** through a trait impl, never automatic. | Its `freeze` copies the whole state under the apply lock. That cost must be a visible choice. |
| D5 | The SDK owns and **checks the cursor** (`last_applied`) across a helper-based snapshot. | If an app forgets to encode its cursor, the idempotency guard would re-apply frames already in the snapshot, a silent double-apply. The SDK makes that a named error. |

## 4. The API

### 4.1 One start method

```rust
impl<S, O> ServiceBuilder<S, O> {
    pub fn start(self) -> Result<Service<S>, ServiceError>
    where
        S: SnapshotStateMachine;
}
```

The body is today's `start_with_snapshots()` body: the install capability,
the snapshot builder thread, and the `CNC_SVC_STATUS_SNAPSHOT_CAPABLE` bit.
The old snapshot-less body is deleted.

Doc comments and the builder docs no longer describe "start without
snapshots".

### 4.2 The helper: `WholeStateSnapshot`

```rust
/// Snapshot support for an FSM whose whole state can be written to bytes.
/// The app owns the encoding (any codec); the SDK owns everything else:
/// the handle, streaming, the cursor, and the exclusive-frontier check.
///
/// Cost: `encode_state` runs on the apply thread with the SM lock held
/// (it is `freeze`), so a snapshot pauses apply for as long as encoding
/// takes: proportional to state size. For large state, implement
/// `SnapshotStateMachine` directly (e.g. a persistent map pinned in O(1)),
/// or take snapshots as standby instants on a learner so voters never pause.
pub trait WholeStateSnapshot: RawStateMachine {
    /// Encode the whole state. Must be deterministic: the same state
    /// yields the same bytes (replicas' artifact hashes are compared).
    fn encode_state(&self) -> Result<Vec<u8>, SnapshotError>;

    /// Replace the whole state from bytes `encode_state` produced,
    /// INCLUDING the state's own record of `last_applied`.
    fn decode_state(&mut self, bytes: &[u8]) -> Result<(), SnapshotError>;
}

impl<S: WholeStateSnapshot> SnapshotStateMachine for S {
    type SnapshotHandle = Vec<u8>;
    // freeze / stream_snapshot / install_snapshot: §4.3
}
```

The blanket impl is sound under Rust coherence. `WholeStateSnapshot` is
local to `uc_service`, and a type that implements `SnapshotStateMachine` by
hand simply does not implement `WholeStateSnapshot`. The implementation
plan's first task proves this with a compile test in a separate crate: one
type uses the helper and one implements the trait by hand.

### 4.3 What the blanket implementation does

The SDK frames the app's bytes in a small **helper frame**:

```
cursor_present u8 ‖ cursor u64 LE ‖ app bytes
```

This sits inside the existing `ULTSNAP2` envelope. The envelope stays the
node's business and is unchanged.

**`freeze()`**

1. Let `c = self.last_applied()`.
2. Build `bytes = frame(c, self.encode_state()?)`.
3. Return `(bytes, c.unwrap_or(0))`. The trait contract says
   `position == last_applied().unwrap_or(0)` at pin time.

**`stream_snapshot(handle, dst)`** writes the handle.

**`install_snapshot(position, src)`**

1. Read all bytes and split the frame. A frame that is short or malformed
   gives `SnapshotError::Codec("whole-state frame")`.
2. If `recorded > position`, return `Codec("mis-tagged: cursor above tag")`.
   The tag is an **exclusive** frontier, so the recorded cursor may be at or
   below it, never above. This is the rule every hand-written
   implementation repeats today (`uc_lincheck/src/register.rs:108-116`).
3. Call `self.decode_state(app_bytes)?`.
4. **Check D5:** `self.last_applied()` must equal `recorded`. If not, return
   `Codec("decode_state did not restore last_applied (encode it with the state)")`.
5. Return `Ok(position)`.

`project()` keeps its existing default from the trait
(`uc_service/src/traits.rs:~514`). An app that wants a diff-replay
projection overrides it on the helper type exactly as it does today.

### 4.4 Placement

The trait lives in `uc_service/src/snapshots.rs`, beside the envelope code,
and is re-exported from the crate root. It gets its own module docs: what it
costs, when to write the full trait instead, and the D5 check.

## 5. Migration

| caller | change |
|---|---|
| **The 105 `.start()` calls** (tests, gate harnesses, `apply_bench`, the crashtest service, `examples/counter`) | Each FSM gains snapshot support. Where several test files share a trivial FSM shape, it goes in a shared test-support helper rather than being copied. |
| **The 73 `.start_with_snapshots()` calls** | Rename to `.start()`. |
| **Typed test FSMs** | Implement `WholeStateSnapshot` by encoding their state with bincode, which they already depend on. The helper does not care which codec. |
| **Raw-tier test FSMs** (e.g. `SumSm`-style) | `WholeStateSnapshot` too. Their state is a few integers. |
| **`examples/counter`** | Implements `WholeStateSnapshot`. It is the teaching example, so it shows the easy path, with a comment pointing at the full trait for large state. |
| **`uc_node/examples/apply_bench`** (`RawCount`) | Gains a `WholeStateSnapshot` impl. The apply hot loop does not change: snapshots run only at an instant, and `apply_bench` commands none. So no A/B is owed. The plan must confirm the harness commands no instant. |
| **Hand-written impls already in tree** (`examples/kv`, `uc_lincheck`, the crashtest service) | Unchanged except for the rename. They may move to the helper later; that is not required. |

## 6. Docs

- `docs/reference/state-machine-contract.md`:
  - snapshot support is required;
  - the two ways to provide it (the helper or the full trait);
  - the helper's cost and the D5 check.
- Tutorials and how-tos that call snapshots optional or use `start()` without
  snapshots. The plan must find every such statement:
  `grep -rn "start()\|optional\|without snapshots" docs README.md QUICKSTART.md`.
  - `docs/how-to/upgrade-an-application.md`: remove the "rows without
    snapshots cannot change version" caveat, since that row no longer exists.
  - `docs/notes/uc2-row-running-version-explained.md` §"why only a pin":
    point to this spec.
- `docs/reference/semver-policy.md`: record the SDK API change.
- The #33 spec: an errata pointer ("#67 made snapshots mandatory; the
  genesis-pin rejection stands").
- `RELEASES.md` / `docs/releases.md` at the next release cut: an SDK break,
  one-line migration (`start_with_snapshots` → `start`; add
  `WholeStateSnapshot` or the full trait).

## 7. Proof

1. **Compile-fail test** (`trybuild` or a doc-test with `compile_fail`): an
   FSM without snapshot support cannot call `ServiceBuilder::start()`.
2. **Coherence test:** in one test crate, a helper-based FSM and a
   hand-written `SnapshotStateMachine` both compile and attach.
3. **Helper unit tests:**
   - freeze then install round-trips state and cursor;
   - a recorded cursor above the tag is refused;
   - a `decode_state` that does not restore `last_applied` is refused with
     the D5 message (a deliberately broken test FSM);
   - a truncated frame is refused;
   - `encode_state` determinism: two freezes of equal state give equal bytes.
4. **End to end:** `examples/counter` takes a coordinated snapshot, a fresh
   node joins below the purge floor and installs it, and the value
   converges. Use the existing learner/purge test shape.
5. **Regression:** the whole workspace, the lin capstones, hard-crash,
   `pin_verify`, and clippy on both toolchains plus fmt. Run the gate-driver
   smokes that #33 Task 14 fixed (m5/m6/m7/m10/m12 direct) to confirm they
   still start.

## 8. Risks

- **Churn:** about 100 call sites. The work is mechanical, but the plan
  should batch it by crate, with each batch compiling.
- **Helper cost:** an app that picks the helper for multi-GB state gets long
  apply pauses at every instant. This is documented at the trait and in the
  contract, with the two ways out.
- **Encoding determinism is the app's job.** `HashMap` iteration order is
  the classic trap. It is documented, and the existing live snapshot-hash
  reports (`SnapshotReport`) catch it in production.

[#33]: https://github.com/PeterKnego/ultima_cluster/issues/33
[#67]: https://github.com/PeterKnego/ultima_cluster/issues/67
