// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Diff replay (spec `2026-09-19-uc2-fsm-upgrade-lifecycle-design.md` §4, §6):
//! replay the same input — a snapshot plus a log span — on different FSMs,
//! then compare everything they do **on the captured surfaces**: per-position
//! responses, per-position `svc_sched` records, and the state projection at
//! the origin and at the end.
//!
//! One of spec §4.2's surfaces is NOT captured, and an empty diff says
//! nothing about it: probe-query answers (the projection is the state view
//! instead). `on_committed` emissions and the ids an FSM mints
//! (`ApplyCtx::ids_calls`) ARE captured (plan B2 T6) — `drive_with`'s
//! optional `RawOutputHandler` records `Surface::Output`, and every entry
//! carries `Surface::Ids`. The crate README's "What this does not compare"
//! is the full statement.
//!
//! The pieces, in the order the loop runs them:
//! - [`corpus`] — the input: a backup artifact plus a `CORPUS` manifest.
//! - [`drive`] — the in-process replay driver an app's binary embeds.
//! - [`trace`] — what one run captured, on each captured surface.
//! - [`diff`] — two traces → a divergence profile.
//! - [`attribute`] — profile × declaration → each divergence named to an arm, or unexplained.
//! - [`confirm`] — attributed profile × declaration → verdicts.
//! - [`report`] — the attributed diff report, JSON and text.
//!
//! Behind the `pin-verify` feature, [`live`] adds the black-box rig the
//! `pin-verify` mode drives: a real node, the app's own binary as a child
//! process, a real `uc2ctl upgrade pin`, and a corpus's commands re-submitted
//! through the raw client engine.

pub mod attribute;
pub mod confirm;
pub mod corpus;
pub mod diff;
pub mod drive;
#[cfg(feature = "pin-verify")]
pub mod live;
pub mod pinverify;
pub mod report;
pub mod trace;
