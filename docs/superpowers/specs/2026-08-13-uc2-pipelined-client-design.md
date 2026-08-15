# UC2 pipelined client — design spec

Date: 2026-08-13
Status: approved design, pre-implementation
Scope: `uc2_client` only. No wire-protocol change, no node/service change, no
version bump.

## 1. Motivation

`uc2_client` today exposes exactly one submit path: a blocking, one-command-
per-call `Client::submit` whose internals (a `Mutex<HashMap>` registration
table plus a per-request `sync_channel`) cannot approach the throughput the
server is built and benchmarked for. The only code that *can* — the m5_gate
client pump (`uc2_node/examples/m5_gate.rs`, client role) — lives in an example
binary, is measurement-shaped (histograms, pass bars, panic-on-error, payload
discarded), and re-derives a pile of hard-won session-correctness knowledge
(exactly-once correlation, duplicate-response tolerance, the serving gate)
that no application author should have to rediscover.

The product's pitch is the throughput number and the gates prove that number.
If the only public API cannot reach it, the published 1.64 M responses/s is
achieved by code no user can call. Therefore:

1. Build a public high-performance **engine** layer ("B") that hides every
   session-correctness obligation while exposing every scheduling decision.
2. Build a convenience **ticket** layer ("A") on top, serving both sync
   (REST, thread-per-request) and async (gRPC/tonic) gateways from one type.
3. Rewrite the gate harness clients on the public engine, so the measured
   path IS the shipped path. Reproducing the current M5 numbers through the
   public API is the acceptance criterion.

Target consumer: an RPC gateway process (REST or gRPC) co-hosted with a node,
translating external requests into UC commands/queries at high rate.

## 2. Goals and non-goals

Goals:
- Pipelined submits AND queries (linearizable + snapshot) through one
  correlation engine, at m5_gate-pump performance.
- One public convenience type serving sync (`wait()`) and async (`.await`)
  callers with zero new dependencies (no tokio; `std::future` only).
- The existing blocking `Client` API preserved verbatim as a shim over the
  new machinery; the old matcher implementation deleted.
- Correctness obligations (dedup, wrap safety, slot liveness, restart/
  overwrite classification) live inside the engine, invisible to callers.
- Wait-strategy discipline informed by `ultima_rings`' measured findings,
  ported (~60 lines) rather than depended on — `uc2_client` keeps its small
  advertised dep set.

Non-goals:
- No wire/protocol change of any kind (`(client_id: u32, local_seq: u32)`
  header_extra layout untouched; no new msg types; cnc page untouched).
- No auto-retry / redirect policy — the gateway owns retry, idempotency and
  deadline budgets; we surface classification + leader hint.
- No cross-host client transport. Clients remain same-host shmem attachers;
  cross-host routing stays the gateway's job.
- No batch-submit API (`submit_batch`) — pipelining covers the need without
  coupling unrelated requests' latency.
- The engine is public but its promotion story stops there: no stability
  promise beyond the workspace's normal pre-1.0 semver discipline.

## 3. Architecture: two layers, thread at the top

```
gateway threads               PipelinedClient (A)            Engine (B)
sync: t.wait()  ─┐   submit ──▶ serde encode ──▶ SendHalf::try_submit ──▶ ingress.ring
async: t.await ──┤                                                        query.ring
                 └◀─ per-slot park/waker ◀── driver thread ── PollHalf::poll ◀── egress broadcasts
```

**B is passive; A owns the thread.** The engine is a pure-sync, waitless
object in the house style of `uc2_consensus`/`uc2_crypto`: it never sleeps,
spins, parks or spawns. Its `poll()` is one bounded duty cycle. The caller —
the gates on a dedicated busy-spin core, or A's driver thread — owns cadence
entirely. This inverts today's structure (the thread currently lives at the
bottom, inside `Client`'s matcher); the inversion is what makes the
zero-handoff completion path possible: the polling thread and the consuming
code are the same thread, no intermediate queue.

Following `uc2_crypto`'s `SharedTransport` precedent, the engine splits:

- **`SendHalf`** — `Clone + Send` (not `Sync`; clone per submitting thread,
  the `MpscProducer`-supported pattern). Carries per-clone ring producers and
  the shared slot table. All submit-side calls are `&self`, nonblocking.
- **`PollHalf`** — single-owner (`Send`, one instance). Owns the broadcast
  subscriptions, the read buffer, deadline sweep, restart detection.

One engine = one `client_id` = one attach. A process wanting more parallelism
than one engine provides opens more engines (each a fresh `client_id`).

## 4. The engine (B) — public API

Bytes-level, io_uring-shaped.

```rust
pub struct EngineConfig {
    pub max_inflight: u32,          // inflight window; slot table = next pow2 ≥ this
    pub request_timeout: Duration,  // per-request deadline, enforced by the engine
    // + Default
}

let (send, mut poll) = Engine::attach(instance_dir, app_id, cfg)?;

// submit side — &self, nonblocking, never sleeps:
send.try_submit(user_data: u64, cmd_bytes: &[u8]) -> Result<(), SubmitError>;
send.try_query(user_data: u64, query_bytes: &[u8], Consistency) -> Result<(), SubmitError>;

pub enum Consistency { Linearizable, Snapshot }

pub enum SubmitError {
    Backpressure,        // inflight window full, or ingress/query ring full
    NotServing,          // node lost CAN_SERVE — do not free-run into a dead leader
    PayloadTooLarge,     // > node max payload: fail LOUD here (node would drop silently)
    Ring(RingError),     // attach-shaped I/O failures
}

// completion side — one bounded duty cycle, zero-alloc, borrow-only:
poll.poll(|c: Completion<'_>| { ... }) -> usize;   // completions emitted this cycle

pub struct Completion<'a> {
    pub user_data: u64,
    pub position: Option<u64>,  // the applied position (Some for Response —
                                // the SMR idempotency key, stripped from the
                                // egress payload prefix by the engine)
    pub outcome: Outcome<'a>,
}
pub enum Outcome<'a> {
    Response(&'a [u8]),        // borrowed from the engine's read buffer,
                               // valid only during the callback
    NotLeader { hint: Option<u32> },
    Retry,                     // transient node/service refusal (MSG_V2_RETRY)
    TimedOut,                  // request_timeout elapsed; engine reclaimed the slot
    InstanceRestart,           // node restarted mid-flight; will never resolve
}
```

### The central contract

**Every accepted `try_submit`/`try_query` produces exactly one completion for
its `user_data`, in bounded time.** All the hidden machinery exists to enforce
this: nothing accepted may leak, double-complete, or hang forever.

`user_data` is caller-owned and opaque (a pointer, index, or request id); the
engine echoes it back and never interprets it. Uniqueness is NOT required by
the engine (correlation is internal); reusing a live `user_data` merely makes
the two completions indistinguishable to the caller.

### The byte contract

The engine itself is format-free: it never inspects submitted or returned
payload bytes, and the node/log treat `AppCommand` as opaque `Bytes`
end-to-end. The only format constraint is the one imposed by the OTHER
endpoint — the target service's apply boundary — and it belongs to
`uc2_service`, not to the engine:

- Today's typed `StateMachine` trait makes the service framework run the
  codec (the ONE decode at the apply boundary, `uc2_service/src/apply.rs`),
  and the framework's chosen codec is bincode (standard config). So against
  today's SDK, submitted bytes must decode as `bincode(Command)`, query
  bytes as `bincode(Query)` — a property of the service SDK the engine's
  caller targets, which a future service-side raw-passthrough capability
  (§10) would remove entirely.
- `Outcome::Response` bytes are the egress payload with its prefix stripped:
  the layout is `position: u64 LE ++ response bytes`
  (`uc2_service/src/egress.rs`; today's SDK makes the body
  `bincode(Response)`), and the engine strips the prefix, exposing it as
  `Completion.position`.
- Encode-once reuse (the m5_gate trick) falls out for any service whose
  `Command` carries opaque bytes (`Vec<u8>`/`bytes::Bytes`): bincode of a
  byte payload is a length prefix plus the bytes, encoded once and resent
  verbatim.

In A, this contract is invisible: `PipelinedClient` bincode-encodes typed
commands/queries at the API entry and decodes responses on the waiter's
thread, exactly as `Client` does today (`client.rs`/`matcher.rs`).

### What the engine hides (the session-correctness inventory)

Inherited from the m5_gate pump and the current `Client`, now with one home:

1. **Attach discipline** — cnc open with `app_id` + protocol-version
   validation, `client_id` allocation via `next_client_id.fetch_add`,
   subscribe both egress broadcasts BEFORE any submit can be issued (no
   answer publishable before subscription ⇒ none missable).
2. **Exactly-once correlation** — fixed slot array (next pow2 ≥
   `max_inflight`), CAS-claimed owner words. A restarted service legitimately
   RE-publishes historical responses under their original `(client_id,
   local_seq)` (at-least-once contract); only the CAS winner resolves, later
   deliveries are counted and dropped.
3. **Wrap-safe sequencing** — internal sequence is u64; the wire carries
   `seq as u32`. Slot owner words hold the full u64 (generation tag), so a
   stale wire-level collision would need a 2^32 outstanding gap — impossible
   with a bounded window. The m5 pump documents wrap as unreachable in gate
   runs; a months-running gateway gets it guaranteed instead.
4. **Flow control** — inflight accounting (`accepted − completed`) enforced
   at `try_submit`; ring-full surfaces as `Backpressure`, never a hidden
   spin.
5. **The serving gate** — submits refused (`NotServing`) while the attached
   node lacks `CAN_SERVE`. Lesson: free-running into a leaderless node
   degenerates into a NOT_LEADER feedback flood that starves the very
   election trying to fix it (observed on the core-starved sandbox). The
   engine surfaces the pause as an error; it does not secretly sleep.
6. **Slot liveness** — `poll` expires deadlines (`TimedOut`) and detects
   instance restart via the torn-tolerant cnc header re-read
   (`InstanceRestart` for ALL in-flight; a torn/zeroed header during
   in-place recreate reads as restart, per the M5 final-review fix). A
   broadcast `RingError::Overwritten` (this reader fell behind; the skipped
   records are unrecoverable) is COUNTED in stats, and affected requests are
   left to resolve or hit `TimedOut`: the engine cannot know which responses
   were inside the lost window, and eagerly failing all in-flight would
   falsely error requests whose responses arrive after recovery. The
   deadline is the honest backstop — a capability the m5 pump (drain-grace
   only) did not have. Deadline and restart checks are amortized (coarse
   timestamp, checked every N duty cycles) so the steady-state cycle stays
   two `try_read`s + slot CAS.
7. **Fail-loud payload bound** — `PayloadTooLarge` at the door; the node's
   appender silently drops oversized records with no response ever published
   (the m5_gate "everything times out" trap).

### Poll-side notes

- `poll` drains BOTH broadcasts (`egress_service`, `egress_node`) with
  bounded work per call, reusing one read buffer (`try_read` overwrites it) —
  zero steady-state allocation.
- Records addressed to other clients are skipped (every client sees every
  record on the shared broadcasts; there is no ring-level targeting).
- A corrupt/bad-CRC record is dropped and the cycle continues (the standing
  defensive posture).
- `Outcome::Response` payload is a borrow; callers keep bytes by copying (A
  copies into the ticket).

## 5. The ticket layer (A) — public API

```rust
pub struct PipelinedConfig {
    pub driver_wait: WaitStrategy,   // ported closed enum; default Park
    pub max_inflight: u32,
    pub request_timeout: Duration,
}

let client = PipelinedClient::connect(instance_dir, app_id, cfg)?;   // Arc-share freely

// sync (REST worker):
let t: Ticket<Resp> = client.submit(&cmd)?;
let resp = t.wait()?;                       // or t.wait_timeout(d)

// async (gRPC handler, any runtime):
let resp = client.submit(&cmd)?.await?;

// reads:
client.query_linearizable(&q)? / client.query_snapshot(&q)?  -> Ticket<QResp>
```

- **`submit` serde-encodes (bincode, as today) and hands to the engine.** On
  `Backpressure` it parks briefly and retries for up to the same 1 s grace
  the current client uses, then errors; `try_submit` fails fast. On
  `NotServing` it likewise waits out the grace (elections take 150–300 ms)
  before erroring. Once accepted it returns a `Ticket` immediately.
- **`Ticket<R>` is both a blocking handle and a `Future`.** One slot-backed
  state cell: `wait()` parks the caller thread (park/unpark per slot);
  `poll()` (the `Future` impl) stores the task `Waker`. Response bytes are
  copied into the ticket by the driver; serde decode runs on the WAITER's
  thread, keeping the driver cycle minimal. `std::future` only — no runtime
  dependency, sync and async callers share everything.
- **Failures resolve, never hang**: `ClientError::{NotLeader{hint}, Retry,
  Timeout, InstanceRestart, Lost, Backpressure, ...}` (existing variants
  reused where they exist). No auto-retry (non-goal, §2).
- **Dropping a `Ticket` abandons interest**; the engine still completes the
  slot and the driver discards the orphan. No leak, no double-complete.
- **One driver thread per `PipelinedClient`** (`AgentRunner`, as today's
  matcher): runs `PollHalf::poll` under the configured `WaitStrategy` and
  resolves tickets. The `PipelinedClient` is `&self` throughout and intended
  to be shared as one `Arc` across all gateway threads.

### Wait strategy (ported from `ultima_rings`)

The `WaitStrategy` closed enum (`BusySpin | BackoffYield | Backoff | Park`)
and the ~60-line `Idle` ladder are PORTED with attribution (source:
`ultima_rings/src/wait.rs`, incl. the 64 µs `park_timeout` floor finding) —
NOT a dependency, preserving `uc2_client`'s small advertised dep set.

Placement of waits, informed by `ultima_rings`' measurements (`BusySpin`
collapses under oversubscription; on a busy machine `Park` is fastest and
keeps 86%+ of external throughput — and a gateway machine is busy by
definition):

- **Engine: waitless** (§3). `BusySpin` remains legitimate only for a
  dedicated-core caller driving `PollHalf` directly — i.e. the gates.
- **Driver thread: `driver_wait`, default `Park`** — after a short
  spin/yield ladder, parks on the SERVICE broadcast's existing cross-process
  futex parker (`uc_protocol::ring::broadcast` wake-all on write, with the
  futex `expected`-value guard); producer `ParkMode` mismatch degrades to
  poll-latency, not unsoundness. A thread can only park on ONE ring's futex,
  so the park is timed (bounded rung, 64 µs floor respected): `egress_node`
  records — rare, NOT_LEADER/RETRY only — are picked up within one rung
  rather than futex-woken. Idle-cost expectation per `ultima_rings`: ~2% of
  a core parked vs ~10% on a timed-backoff ladder alone.
- **`Ticket::wait()`: always park/unpark** (in-process, woken by the driver;
  ~10 µs wake cost is noise against a ~1 ms commit round trip). No spin
  option: a REST worker spinning through a consensus round trip while
  holding a core is exactly the oversubscription failure the measurements
  document.

## 6. Failure semantics (consolidated)

| Event | Engine surfaces | Ticket resolves to |
|---|---|---|
| window/ring full | `SubmitError::Backpressure` | (submit-side error after grace) |
| node not serving leader | `SubmitError::NotServing` | (submit-side error after grace) |
| oversized payload | `SubmitError::PayloadTooLarge` | (submit-side error, immediate) |
| `MSG_V2_NOT_LEADER` | `Outcome::NotLeader{hint}` | `Err(NotLeader{hint})` |
| `MSG_V2_RETRY` | `Outcome::Retry` | `Err(Retry)` |
| deadline elapsed | `Outcome::TimedOut` | `Err(Timeout)` |
| node restart mid-flight | `Outcome::InstanceRestart` for all in-flight | `Err(InstanceRestart)` |
| broadcast overwritten | counted (stat); affected requests resolve or hit `TimedOut` | `Err(Timeout)` for any actually lost |
| duplicate response | dropped + counted (stat) | — (first delivery already resolved) |
| stale kind-mismatch | dropped + counted (stat, as today) | — |

Engine drop: dropping both halves abandons the session; in-flight requests
are simply never completed (the caller destroyed the completion path — there
is no one left to notify). A's `Drop` fails outstanding tickets with
`ShutDown` first (today's `drain_with_shutdown` contract), then stops the
driver.

Diagnostics: the engine keeps the pump's counters (duplicates, overwrites,
kind-mismatch drops, not-leader, retries) behind a `stats()` accessor —
they were load-bearing for diagnosing every historical incident.

## 7. Compatibility

- `Client`'s public API is UNCHANGED (`connect`, `submit`, `query_snapshot`,
  `query_linearizable`, `leader_hint`, `kind_mismatch_drops`, `shutdown`,
  `UC2_CLIENT_TIMEOUT_MS`). It becomes a shim: `submit` =
  `pipelined.submit()?.wait()`. `matcher.rs` and the `Mutex<HashMap>` +
  per-request `sync_channel` machinery are DELETED — one code path.
- Wire and shmem contracts untouched: same ring files, same msg types, same
  `header_extra` codec, same cnc layout. A new client talks to an old node.

## 8. Module layout

```
uc2_client/src/
  engine.rs      // B: SendHalf/PollHalf, slot table, session correctness (§4)
  pipelined.rs   // A: PipelinedClient + driver thread
  ticket.rs      // Ticket<R>: park/unpark + Waker state cell
  wait.rs        // ported WaitStrategy + Idle ladder (attributed)
  client.rs      // compat shim (shrinks)
  error.rs       // extended ClientError
  lib.rs         // exports: Client, PipelinedClient, Ticket, Engine halves, WaitStrategy
  (matcher.rs deleted)
```

## 9. Testing

The correctness knowledge moving out of m5_gate arrives as tests, not
comments. All scratch dirs via `CARGO_TARGET_TMPDIR` (never `/tmp`, per the
standing tmpfs rule).

Engine unit/integration (in-process node+service, lin_v2-harness style):
- duplicate-response injection → exactly one resolution, duplicate counted;
- seq wrap → start the internal sequence near `u32::MAX`, run a window
  across the wire wrap, all completions correct;
- restart mid-flight → recreate the cnc under the engine (incl. the torn-
  header window) → `InstanceRestart` for all in-flight, none hang;
- overwrite → force a slow poller past the broadcast horizon → overwrite
  counted in stats, every affected request still resolves (response or
  `TimedOut`), none hang;
- timeout expiry, `PayloadTooLarge` at the door, `NotServing` while
  `CAN_SERVE` is clear, backpressure at `max_inflight`;
- exactly-one-completion accounting under a randomized soak (accepted ==
  completions, per `user_data`).

Ticket layer:
- sync `wait()` and async `.await` round trips against a real in-process
  cluster (async driven by a hand-rolled `block_on` — no runtime dep, not
  even dev-dep);
- drop-ticket accounting (orphan completion discarded, no leak);
- compat: the existing `Client` test surface passes unchanged on the shim.

Acceptance (the release criterion):
- m5_gate's client role rewritten on the public `Engine` (histogram/pass-bar
  logic stays in the example); the M5 gate re-run must reproduce current
  numbers (≥400 k resp/s bar; expectation: parity with the ~1.64 M measured,
  no regression attributable to the extraction);
- m6/m7 gate client sides follow mechanically on the same engine.

## 10. Out of scope / deferred

- Publishing `uc2_client` docs positioning the engine as the "advanced" tier
  and tickets as the default (rustdoc lands with the code; QUICKSTART touch-
  up can ride the same branch).
- Per-slot response-buffer reuse in A (amortizing the one copy/alloc per
  response) — measure first; only if the gate re-run shows it matters.
- Any future `ultima_rings` hard dependency (revisit if/when that crate
  becomes the stack's shared substrate).
- **Service-side raw passthrough** (`uc2_service` change, separate arc): a
  way for an SM to declare "my `Command`/`Response` are bytes — hand me the
  committed payload directly, skip the framework codec." Removes the
  bincode wrapper for apps bringing their own format (proto, flatbuffers):
  today they must define `Command = Vec<u8>` and pay bincode's varint
  prefix plus an alloc-and-copy per apply (`decode_from_slice::<Vec<u8>>`),
  which sits oddly against the "refcounted, no intermediate copies"
  `AppCommand` philosophy. With passthrough, the engine's byte contract
  collapses to nothing — the format becomes purely an app-level agreement
  between gateway and state machine.
- Cross-host redirect helpers (leader-hint-driven re-attach) — gateway
  policy, possibly a later cookbook example.
