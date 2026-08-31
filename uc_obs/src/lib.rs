// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! UC's observability core: the structured log record format, and nothing
//! else.
//!
//! # Why this is its own crate
//!
//! Every UC daemon emits the same JSON-lines record format, but they do not
//! share a parent: `uc2-gateway` must not depend on `uc_node` (that would
//! pull consensus, transport and crypto into a front-door process), and
//! `uc_net` — which today *counts* snapshot-session refusals and lets
//! `uc_node` sample the counters once per duty cycle, purely because it has
//! no logging dependency — should be able to emit at the point the event
//! happens instead.
//!
//! So the format lives below all of them. It is deliberately **not** a
//! general "common" or "util" crate: the name is a promise about what may be
//! added, and a crate rename is a major version under
//! `docs/reference/semver-policy.md` (the one carve-out was spent on the
//! 2.9.0 `uc_*` rename). Shared code that is not observability belongs in a
//! crate named for what it is.
//!
//! # What is here
//!
//! [`log`] — [`log::emit`], the [`obs_event!`] macro over it, the
//! [`log::LogLevel`] filter, and [`log::format_line_at`], the single
//! formatter that both the log stream and `uc_node`'s admin audit file
//! render through so their JSON escaping and key order cannot drift.

pub mod log;
