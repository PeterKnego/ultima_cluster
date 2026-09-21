// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The `live` rig on its own: a node, the register fixture's serve form, an
//! attach observed through the cnc page, a clean SIGTERM stop, a refused
//! attach observed as a non-zero exit with the error on stderr, and a
//! recorded span re-submitted onto a fresh node through the raw engine.
mod common;
use std::time::Duration;

use uc_diffreplay::corpus::Corpus;
use uc_diffreplay::live::{
    AttachOutcome, artifact_path, command_instant, message_frames, replay_span, spawn_app,
    start_node,
};
use uc_log::cnc::CncPage;

const T: Duration = Duration::from_secs(60);

#[test]
fn the_serve_form_attaches_and_stops_cleanly() {
    let inst = common::tempdir();
    let dir = inst.path();
    let node = start_node(dir, "live1", common::register_name(), T).unwrap();
    let cnc = CncPage::open_file(&dir.join("cnc2.dat"), "live1").unwrap();
    let before = uc_diffreplay::live::incarnation(&cnc, 0);
    let mut app = spawn_app(
        &common::register_replay_bin(),
        &["serve".to_string()],
        dir,
        "live1",
        &dir.join("old.stderr"),
    )
    .unwrap();
    assert!(
        matches!(
            app.wait_attached(&cnc, 0, before, T),
            AttachOutcome::Attached
        ),
        "{}",
        app.stderr()
    );
    // An instant completes for the row: the artifact appears.
    let p = command_instant(&node, T).unwrap();
    assert!(
        uc_diffreplay::live::wait_for(|| artifact_path(dir, 0, p).is_file(), T),
        "row 0 never published snap-{p}"
    );
    let st = app.stop(T).unwrap();
    assert!(st.success(), "clean stop must exit 0: {st}");
    node.stop();
}

#[test]
fn a_refused_attach_is_a_nonzero_exit_with_the_error_on_stderr() {
    let inst = common::tempdir();
    let dir = inst.path();
    let node = start_node(dir, "live2", common::register_name(), T).unwrap();
    let cnc = CncPage::open_file(&dir.join("cnc2.dat"), "live2").unwrap();
    let before = uc_diffreplay::live::incarnation(&cnc, 0);
    // Wrong app id: the page refuses the attach by name.
    let mut app = spawn_app(
        &common::register_replay_bin(),
        &["serve".to_string()],
        dir,
        "not-live2",
        &dir.join("bad.stderr"),
    )
    .unwrap();
    match app.wait_attached(&cnc, 0, before, T) {
        AttachOutcome::Exited { code, stderr } => {
            assert_ne!(code, Some(0));
            assert!(stderr.contains("Error:"), "stderr: {stderr}");
        }
        AttachOutcome::Attached => panic!("a wrong app id must not attach"),
        AttachOutcome::TimedOut { stderr } => {
            panic!("a wrong app id must be refused, not time out: {stderr}")
        }
    }
    node.stop();
}

/// The other half of the rig: [`message_frames`] indexes a corpus's recorded
/// commands, and [`replay_span`] puts them back on the wire — onto a node
/// that has never seen them, through the app's own binary.
#[test]
fn a_recorded_span_replays_onto_a_fresh_node() {
    let src = common::tempdir();
    let out = common::tempdir();
    let (p, _) = common::build_register_history(src.path(), "spanrec", 5, 5);
    let corpus = Corpus::export(src.path(), "spanrec", 0, p, u64::MAX, 0, out.path()).unwrap();
    let (frames, timers) = message_frames(&corpus).unwrap();
    assert_eq!(
        timers, 0,
        "RegisterSm schedules nothing, so the span has no TIMER frames"
    );
    // The five writes recorded after the instant at P.
    assert_eq!(
        frames.len(),
        5,
        "the span [{p}, end) should carry the post-instant writes"
    );

    let inst = common::tempdir();
    let dir = inst.path();
    let node = start_node(dir, "span", common::register_name(), T).unwrap();
    let cnc = CncPage::open_file(&dir.join("cnc2.dat"), "span").unwrap();
    let before = uc_diffreplay::live::incarnation(&cnc, 0);
    let mut app = spawn_app(
        &common::register_replay_bin(),
        &["serve".to_string()],
        dir,
        "span",
        &dir.join("span.stderr"),
    )
    .unwrap();
    assert!(
        matches!(
            app.wait_attached(&cnc, 0, before, T),
            AttachOutcome::Attached
        ),
        "{}",
        app.stderr()
    );

    let r = replay_span(&frames, dir, "span", 0, 0..frames.len(), T).unwrap();
    assert_eq!(r.submitted, frames.len() as u64);
    assert!(
        r.last_position > 0,
        "a completed command names its position"
    );
    // The row's published `applied` cursor reaches the last response's
    // frame START — and passes it, since `applied` is the cursor AFTER the
    // batch. That cursor, not `last_position`, is the frontier a pin-verify
    // sequence hands the next step.
    assert!(
        matches!(
            app.wait_applied(&cnc, 0, r.last_position, T),
            AttachOutcome::Attached
        ),
        "row 0 never reached {}: {}",
        r.last_position,
        app.stderr()
    );

    // A span that runs past the corpus is refused by name, not indexed out of
    // bounds.
    let e = replay_span(&frames, dir, "span", 0, 0..frames.len() + 1, T).unwrap_err();
    assert!(e.to_string().contains("runs past the corpus"), "{e}");

    let st = app.stop(T).unwrap();
    assert!(st.success(), "clean stop must exit 0: {st}");
    node.stop();
}
