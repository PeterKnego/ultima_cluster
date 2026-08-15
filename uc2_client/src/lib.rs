// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The client SDK: submit commands and run reads against a cluster, over
//! shared memory — no async runtime, no `openraft`, no `quinn`. Three tiers,
//! all attaching to the SAME per-instance shmem IPC (the cnc v2 page, the
//! client-facing MPSC ingress/query rings, the node/service broadcast egress
//! rings), from lowest-level/highest-throughput to most convenient:
//!
//! - **[`Engine`]** ([`Engine::attach`] -> [`SendHalf`] + [`PollHalf`]): the
//!   public, io_uring-shaped submit/poll engine — bytes in, bytes out,
//!   nonblocking, `user_data`-correlated. Hides every session-correctness
//!   obligation (exactly-once resolution, wrap-safe sequencing, the serving
//!   gate, slot liveness, restart/overwrite classification — see
//!   `engine.rs`'s module docs for the full inventory) while exposing every
//!   scheduling decision: this is what a max-throughput RPC gateway or the
//!   `m5_gate` measurement harness (`uc2_node/examples/m5_gate.rs`) runs on
//!   directly — the measured path IS the shipped path.
//! - **[`PipelinedClient`]** ([`PipelinedClient::connect`]): a convenience
//!   layer over one `Engine`, at the same throughput. `submit`/`try_submit`/
//!   `query_linearizable`/`query_snapshot` hand a typed command/query to a
//!   hand-spawned driver thread and return a [`Ticket`] immediately — a
//!   caller holds a WINDOW of outstanding tickets instead of blocking one
//!   round trip per call, resolved via blocking `wait()` or polled as a
//!   `std::future::Future`. Serves both sync (REST, thread-per-request) and
//!   async (gRPC/tonic) gateways from one type, with zero new deps (no
//!   tokio).
//! - **[`Client`]**: the original one-command-per-call blocking API, now a
//!   thin shim over `PipelinedClient` (submit-then-immediately-`wait()`) —
//!   preserved verbatim for existing callers. [`Client::connect`] attaches
//!   like the others; writes are leader-only, [`Client::query_linearizable`]
//!   goes through the node's quorum read barrier, and
//!   [`Client::query_snapshot`] is answered from the local replica. See
//!   `docs/QUICKSTART.md` for a worked example.
//!
//! ## The byte contract
//!
//! The engine itself is format-free — it never inspects submitted or
//! returned payload bytes. The format constraint comes from the OTHER
//! endpoint, the target service's apply boundary, and belongs to
//! `uc2_service`: against today's typed `StateMachine` SDK, submitted bytes
//! must decode as `bincode(Command)` and query bytes as `bincode(Query)`;
//! `Completion`/ticket response bytes are `bincode(Response)` with the wire's
//! `position: u64 LE` prefix already stripped. See
//! `docs/superpowers/specs/2026-08-13-uc2-pipelined-client-design.md` §4 for
//! the full contract (and the deferred raw-passthrough follow-up).
//!
//! Spec: `docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md`
//! §7 (original SDK); `docs/superpowers/specs/2026-08-13-uc2-pipelined-client-design.md`
//! (engine + ticket layers, this module's current shape).

mod client;
mod engine;
mod error;
mod pipelined;
mod slots;
mod ticket;
mod wait;

pub use client::Client;
pub use engine::{
    Completion, Consistency, Engine, EngineConfig, EngineStats, Outcome, PollHalf, SendHalf,
    SubmitError,
};
pub use error::ClientError;
pub use pipelined::{PipelinedClient, PipelinedConfig};
pub use ticket::Ticket;
pub use wait::WaitStrategy;
