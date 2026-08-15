// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The client SDK: submit commands and run reads against a cluster.
//!
//! Synchronous and blocking, over shared memory — no async runtime, no
//! `openraft`, no `quinn`. A client attaches to one node's instance directory;
//! writes are leader-only, [`Client::query_linearizable`] goes through the
//! node's quorum read barrier, and [`Client::query_snapshot`] is answered from
//! the local replica. See `docs/QUICKSTART.md` for a worked example.
//!
//! [`Client::connect`] attaches to a running node's shared-memory IPC (the
//! cnc v2 page, the client-facing MPSC ingress/query rings, and the
//! node/service broadcast egress rings) under an instance directory, and
//! drives `submit`/`query_snapshot`/`query_linearizable` as synchronous,
//! blocking calls correlated by a `(client_id, local_seq)` pair carried in
//! every frame's `header_extra` — the small dep set (no `openraft`, no
//! `quinn`, no async runtime) mirrors the v1 `uc_client` design intent, just
//! over the v2 wire shapes and sync std primitives instead of tokio.
//!
//! Spec: `docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md`
//! §7; plan `docs/superpowers/plans/2026-07-11-uc2-m5-sdk.md` Task 10.

mod client;
mod engine;
mod error;
mod matcher;
mod slots;
mod wait;

pub use client::Client;
pub use engine::{Consistency, Engine, EngineConfig, EngineStats, PollHalf, SendHalf, SubmitError};
pub use error::ClientError;
pub use wait::WaitStrategy;
