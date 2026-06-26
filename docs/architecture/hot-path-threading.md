# Hot-path threading & handoff

How the steady-state commit pipeline is actually executed: the threading model,
where openraft is invoked, and how work is handed across the three process
boundaries. This is a *reference* doc — it describes the implementation as it
stands, with `file:line` anchors. For the latency *consequences* of this shape
see `docs/benchmarks/floor-decomposition-2026-06-25.md`.

## What counts as the hot path

The **per-request steady-state commit pipeline**: one client write (or one
linearizable read), from submission to acknowledgment, while the cluster is
healthy and the leader is stable. Everything that runs *once per operation*.

Explicitly **out of scope** (cold paths): `Raft::new` / `initialize` /
`add_learner`, membership changes, leader election, snapshot install, and crash
recovery / service reconstruction.

There are two hot paths:

- **Write commit path** — `submit → openraft client_write → replicate+commit → apply → response`.
- **Linearizable read path** — `query → ensure_linearizable (ReadIndex) → service catch-up → seqlock-validated query → response`.

## The three concurrency domains

| Domain | Process | Execution model | Why |
|---|---|---|---|
| openraft + node IPC | `uc_node` | **single-threaded** tokio `current_thread` runtime | apply ordering is serial anyway; a `multi_thread` runtime intermittently times out the shmem handshake (`uc_node/src/test_support.rs:16`) |
| QUIC inter-node | `uc_node` (same runtime) | async tokio tasks, task-per-stream | per-RPC-class concurrency without head-of-line blocking |
| Service apply | `uc_service` (separate process) | **dedicated `std::thread`, sync, no tokio** | `apply` is sync, deterministic, possibly CPU-bound — must not pin a tokio worker (`uc_service/src/runtime/apply_loop.rs:3`) |

**Key fact: openraft is not multithreaded here.** Every openraft call —
`client_write`, `ensure_linearizable`, `RaftStateMachine::apply`,
`append_entries` — runs as a future on one `current_thread` runtime. openraft's
own `RaftCore` is one more tokio task on that same runtime. So *inside* the node
there is no thread-to-thread handoff in steady state — only *task*-to-*task*
handoff via `.await`, cooperatively scheduled on a single OS thread.

The expensive handoffs are at the two **process boundaries** (node↔service) and
the **host boundary** (node↔node). Each is a lock-free `/dev/shm` ring + a
conditional cross-process futex.

## Topology

```
┌─────────────────────────┐   ┌──────────────────────────────────────────────────┐   ┌──────────────────────────┐
│   CLIENT process(es)    │   │                  uc_node process                  │   │   uc_service process     │
│                         │   │      single-threaded tokio current_thread RT      │   │                          │
│  input handling         │   │   (openraft RaftCore + ALL dispatchers = tasks    │   │  apply = a real OS thread│
│                         │   │    cooperatively scheduled on ONE OS thread)      │   │  others = tokio tasks    │
└─────────────────────────┘   └──────────────────────────────────────────────────┘   └──────────────────────────┘
            │                                       │                                              │
   ─────────┼──────────────── /dev/shm ────────────┼─────────────── /dev/shm ─────────────────────┼─────────
            │                                       │                                              │
            │  ┌─────────────────┐                  │                  ┌────────────────────┐      │
            ├─▶│ submit.ring MPSC│─────────────────▶│ client_dispatcher│                    │      │
            │  └─────────────────┘ (futex)          │  (tokio task)    │                    │      │
            │                                       │     │ .await      │                    │      │
            │                                       │     ▼             │  ┌──────────────┐  │      │
            │                                       │  openraft         │─▶│ apply.ring   │──┼──────┤  (futex)
            │                                       │  client_write     │  │   SPSC       │  │      ▼
            │                                       │     │             │  └──────────────┘  │   apply OS thread
            │                                       │     ▼ commit      │                    │   read_or_park()
            │                                       │  RaftStateMachine │  ┌──────────────┐  │   sm.blocking_write()
            │                                       │  ::apply (RaftCore│◀─│apply_resp.ring│◀─┼── guard.apply() SYNC
            │                                       │   task)           │  │   SPSC       │  │      │
            │  ┌─────────────────┐                  │     │             │  └──────────────┘  │      │
            │◀─│response.broadcast│◀─────────────────│     ▼ broadcast   │                    │      │
            │  └─────────────────┘ (futex)          │                   │  ┌──────────────┐  │      │
            │                                       │  output_dispatcher│─▶│ output.ring  │──┼──────┤
            │                                       │  (tokio task)     │  │   SPSC       │  │      ▼
            │                                       │                   │  └──────────────┘  │   output loop
            │                                       │                   │  ┌──────────────┐  │   (tokio task)
            │                                       │                   │◀─│output_resp   │◀─┼── on_committed().await
            │                                       │                   │  └──────────────┘  │   (async, leader-only)
            │  ┌─────────────────┐                  │                   │  ┌──────────────┐  │
            ├─▶│ query.ring MPSC │─────────────────▶│ query_dispatcher  │─▶│ query.ring   │──┼──────▶ query loop
            │  └─────────────────┘                  │ ensure_lineariz...│  │   SPSC       │  │      (tokio task)
            │                                       │ ().await          │  └──────────────┘  │      sm.query() SYNC
            │                                       │                   │                    │
            │                              ▲        └───────────────────┘                    │
            │                              │ QUIC (quinn): 1 conn/peer, 1 bidi stream/RPC,    │
            │                              │ RaftNetworkV2 pipelined depth=8, task-per-stream │
            │                              ▼                                                  │
            │                    ┌──────────────────────┐                                    │
            │                    │  uc_node on PEER host │  raft.append_entries().await       │
            │                    └──────────────────────┘                                    │
```

Execution-context legend:

```
●  OS thread, runs blocking code          ○  tokio task (cooperative, on a shared runtime)
═  process / host boundary                 →  lock-free shmem ring (futex wakeup)

uc_node:    everything is ○ on ONE OS thread  (openraft RaftCore, all dispatchers)
            + a few ● "ring-park-*" parker threads that ONLY translate futex→Notify
uc_service: apply = ●  (sync, std::thread)    |  query / output / liveness = ○ (tokio tasks)
```

## Where openraft is invoked (the four edges)

1. **node → openraft (write):** `raft.client_write(cmd).await`
   — `uc_node/src/ipc/client_dispatcher.rs:111`. The dispatcher `tokio::spawn`s
   a task per submit (rather than awaiting inline) so multiple writes are in
   flight; bounded by submit-ring capacity and the client in-flight window.
2. **node → openraft (read):** `raft.ensure_linearizable().await` (ReadIndex
   barrier) — `uc_node/src/ipc/client_dispatcher.rs:211`, then `query_link.submit()`.
3. **openraft → us (storage):** `RaftStateMachine::apply` —
   `uc_node/src/raft/state_machine_shmem.rs:513`. Called on openraft's RaftCore
   task per committed entry; we do **not** apply in-process — we publish to the
   service ring and await the response.
4. **openraft → us (network):** `RaftNetwork` impl —
   `uc_node/src/network/quic/instance.rs:170`, lifted to `RaftNetworkV2` by the
   pipelining wrapper `uc_node/src/network/pipelined.rs:126` (depth via
   `UC_PIPELINE_DEPTH`, default 8 — `uc_node/src/network/mod.rs:34`). Receiver
   side dispatches `raft.append_entries().await` from a task-per-stream
   (`uc_node/src/network/quic/server.rs`).

openraft itself is created at `uc_node/src/runtime/builder.rs:417` (`Raft::new`);
it spawns its `RaftCore` task on the caller's `current_thread` runtime.

## Write hot path as a sequence (one client write)

```
CLIENT          submit.ring     client_dispatcher    RaftCore(openraft)    apply.ring      apply OS thread
 proc            (MPSC)          ○ tokio task         ○ tokio task          (SPSC)          ● std::thread (svc proc)
  │                 │                  │                    │                  │                  │
  │ write SubmitFrame                  │                    │                  │                  │
  ├────────────────▶│                  │                    │                  │                  │
  │   CAS claim + publish(Release)     │                    │                  │                  │
  │   futex_wake ──▶ wakes parker ─────┤                    │                  │                  │
  │                 │   try_read()     │                    │                  │                  │
  │                 │   (Acquire)      │                    │                  │                  │
  │                 │                  │ spawn task:        │                  │                  │
  │                 │                  │ raft.client_write(cmd).await          │                  │
  │                 │                  ├───────────────────▶│                  │                  │
  │                 │                  │              replicate via QUIC ──┐    │                  │
  │                 │                  │              journal append+fsync │    │                  │
  │                 │                  │              (group commit)       │    │                  │
  │                 │                  │              COMMIT ◀─────────────┘    │                  │
  │                 │                  │                    │ apply():         │                  │
  │                 │                  │                    │ publish_apply ──▶│                  │
  │                 │                  │                    │  (Release)       │ futex_wake ──────┤
  │                 │                  │                    │                  │   read_or_park():│
  │                 │                  │                    │                  │   spin 64 → arm →│
  │                 │                  │                    │                  │   recheck → park │
  │                 │                  │                    │                  │   blocking_write()
  │                 │                  │                    │                  │   guard.apply()  │
  │                 │                  │                    │                  │   SYNC, no I/O   │
  │                 │                  │                    │ await_apply_resp │◀── publish resp ─┤
  │                 │                  │                    │◀── apply_resp.ring (futex)           │
  │                 │                  │ ◀── client_write returns                                 │
  │ ◀── response.broadcast (futex_wake all clients)                                               │
  │                 │                  │                    │ output.ring ──▶ output loop ○ (async, leader-only)
```

The one real cross-process, cross-threading-model handoff in the write path is
**openraft's async RaftCore task ⇄ the service's sync apply OS thread**, mediated
entirely by two SPSC rings (`apply.ring` + `apply_resp.ring`) plus futex.

Relevant anchors on the apply edge:
- publish + await: `uc_node/src/raft/state_machine_shmem.rs:583` (`publish_apply`), `:604` (`await_apply_resp`); fast path batches up to `apply_pipeline_depth` consecutive entries.
- service consume + apply: `uc_service/src/runtime/apply_loop.rs:93` (`read_or_park`), `:119` (`blocking_write`), `:120` (`guard.apply`, sync).

## The ring handoff, zoomed in (every arrow above is this)

```
   PRODUCER (e.g. RaftCore task)                 CONSUMER (e.g. apply OS thread)
   ─────────────────────────────                 ──────────────────────────────────
   write record bytes into slot                  spin SPIN_TRIES (64) × spin_loop()
   write length prefix LAST                          │  (torn-write guard: len==0 ⇒ retry)
   publish_position.store(pos, Release) ──┐          ▼  still empty?
                                          │       waiters.fetch_add(1) = arm()
   if waiters != 0 (Acquire load):        │       try_read() AGAIN   ← lost-wakeup guard
       futex_wake(&publish_position) ─────┼──┐    still empty?
   (skip syscall entirely if no waiters)  │  │       │
                                          │  └────▶ futex_wait(&publish_position, seq, PARK_CEIL)
                                          │          (NO FUTEX_PRIVATE_FLAG: word is in
                                          │           /dev/shm, shared across processes)
   publish_position.load(Acquire) ◀───────┘       wake → disarm() → try_read() (Acquire)
   ── single release/acquire edge publishes all slot bytes ──
```

The whole machine reduces to: **one Release/Acquire edge per message for
correctness, one conditional cross-process futex syscall per message for wakeup,
spin-then-park to avoid the syscall under load.** The same primitive backs all
seven rings.

### Ring memory ordering (per type)

- **SPSC** (service↔node) — `uc_protocol/src/ring/spsc.rs`. Producer commits with
  `publish_position.store(Release)` (`:198`); its own position is `Relaxed`
  (sole writer); `consumer_position` read `Acquire` only when the cached value
  says full. Consumer `publish_position.load(Acquire)` synchronizes with the
  producer's Release. Torn-write guard: length prefix written last, `len==0` ⇒
  retry (`:282`).
- **MPSC** (clients→node) — `uc_protocol/src/ring/mpsc.rs`. Producers
  `compare_exchange_weak(AcqRel)` to claim a slot range (`:164`), then **publish
  in claim order** — spin until `publish_position == my_start` (`:196`), then
  `store(Release)` (`:199`). This in-order publish is the fix for the old M3
  post-wrap torn-read race; consumers only read slots strictly below
  `publish_position`.
- **Broadcast** (node→clients) — `uc_protocol/src/ring/broadcast.rs`. Single
  producer `store(Release)` (`:122`) then wakes all (`futex_wake(i32::MAX)`).
  Consumers hold an in-memory `head`, reset on lapping (no rewind into
  overwritten records).

### Wakeup mechanics

- `uc_protocol/src/ring/futex.rs` — `SYS_futex` with **no `FUTEX_PRIVATE_FLAG`**;
  the futex word is the low 32 bits of `publish_position`, which lives in the
  `/dev/shm` ring file mmap'd by both processes.
- Producer skips the `FUTEX_WAKE` syscall when `waiters == 0` (an `AcqRel`
  counter that consumers `arm()`/`disarm()` — `uc_protocol/src/ring/common.rs:160`).
- Spin-then-park policy in `read_or_park` (`uc_protocol/src/ring/spsc.rs:221`):
  spin `SPIN_TRIES` (64), then **arm → recheck → park** (the recheck closes the
  lost-wakeup race where a record lands between the last check and the
  `FUTEX_WAIT`), with a `PARK_CEIL` timeout backstop. Busy mode
  (`spin_budget == u32::MAX`) polls forever and never parks (the O1 busy-spin
  consumer variant).

### Bridging futex → async (the parker threads)

Async tokio consumers in `uc_node` cannot call blocking `FUTEX_WAIT` (it would
stall the whole `current_thread` runtime). So each such ring gets a dedicated OS
thread `ring-park-<name>` (`uc_node/src/ipc/ring_bridge.rs`) that parks on the
futex and fires a `tokio::sync::Notify`; the async loop does
`try_read()`-then-`bridge.notified().await`. These parker threads are the *only*
extra OS threads in `uc_node` and do no application work — they exist solely to
translate a blocking futex wakeup into an async one.

## Service-side threading (uc_service)

| Loop | Context | State call | Anchor |
|---|---|---|---|
| **apply** | `std::thread` (sync, no tokio) | `sm.apply()` sync | `uc_service/src/runtime/apply_loop.rs:74` |
| query | tokio task | `sm.query()` sync | `uc_service/src/runtime/query_loop.rs:46` |
| output | tokio task (leader-only) | `handler.on_committed().await` async | `uc_service/src/runtime/output_loop.rs:61` |
| liveness | tokio task | atomic heartbeat tick | `uc_service/src/runtime/liveness.rs:53` |

The user SM is wrapped in a `tokio::sync::RwLock` (not `Mutex`) so the output
loop can hold a `Send` read guard across its `on_committed().await` while apply
takes the exclusive write lock via `blocking_write()` from the plain OS thread
(`uc_service/src/runtime/apply_loop.rs:9`). Shutdown stops the output loop first
to release the read lock before apply's `blocking_write()` can finish draining
(`uc_service/src/runtime/service.rs:276`).

## cnc.dat — not in the data handoff

`uc_protocol/src/cnc.rs`. The control file carries metadata and status, **not**
the data-ring head/tail atomics (those live in each ring file). Hot-path-relevant
fields: `NodeStatus.heartbeat_seq` (service watches leader liveness),
`ServiceStatus.service_epoch` (bumped on service reattach; node detects process
reincarnation), and two small MPSC control rings (`control_to_service` /
`control_to_node`) for out-of-band messages.

## Why this shape (the performance consequence)

The latency floor is **software/structural, not physical**
(`docs/benchmarks/floor-decomposition-2026-06-25.md`): ~73% software/structural
(openraft async commit→apply + 3-proc IPC + replication pipeline) vs ~27%
physical (fsync + wire RTT). The rings and futexes are already ~tens of ns, so
per-edge micro-optimization is null — the cost is the *number* of async hops and
process-boundary crossings the architecture forces per commit
(submit → apply → apply_resp → broadcast, plus QUIC replication choreography),
which is fixed by the 3-process + openraft-async design. The leader-side
`append_entries` RPC probe further shows ~74% of the replication bucket is
openraft async choreography rather than UC's RPC code, so no cheap UC-layer win
remains — only openraft-core changes or a co-location rewrite.
