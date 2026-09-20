// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Diff replay (spec `2026-09-19-uc2-fsm-upgrade-lifecycle-design.md` §4, §6):
//! replay the same input — a snapshot plus a log span — on different FSMs,
//! then compare the differences in their snapshots, outputs and logs.
//!
//! The pieces, in the order the loop runs them:
//! - [`corpus`] — the input: a backup artifact plus a `CORPUS` manifest.
//! - [`drive`] — the in-process replay driver an app's binary embeds.
//! - [`trace`] — what one run captured, on every surface.
//! - [`diff`] — two traces → a divergence profile.
//! - [`attribute`] — profile × declaration → each divergence named to an arm, or unexplained.
//! - [`confirm`] — attributed profile × declaration → verdicts.
//! - [`report`] — the attributed diff report, JSON and text.

pub mod attribute;
pub mod confirm;
pub mod corpus;
pub mod diff;
pub mod drive;
pub mod report;
pub mod trace;
