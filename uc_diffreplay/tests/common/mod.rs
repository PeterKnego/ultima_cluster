// `common` is compiled into EVERY test binary in this directory, so an item
// only one of them uses is unused in the others — for functions
// (`dead_code`) and, since the rig moved into the crate, for the re-exports
// below (`unused_imports`) alike.
#![allow(dead_code, unused_imports)]
use std::path::{Path, PathBuf};
use std::time::Duration;

use uc_client::Client;
use uc_lincheck::register::{Cmd, CmdResp, RegisterSm};
use uc_log::cnc::{AdminResp, CncPage};
use uc_node::Node;
use uc_service::{ServiceBuilder, ServiceConfig, StateMachine};

// The rig itself lives in the crate under test now (`uc_diffreplay::live`,
// behind the `pin-verify` feature), so `pin-verify` and these tests drive
// exactly the same node config, the same admin flow and the same waits.
// What stays here is only the test-local shape: the panicking wrappers and
// the fixtures.
pub use uc_diffreplay::live::{artifact_path, node_config, wait_for};

pub fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("diffreplay-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .unwrap()
}

pub fn start_single_node(dir: &Path, app_id: &str, fsm: &str) -> Node {
    uc_diffreplay::live::start_node(dir, app_id, fsm, Duration::from_secs(60)).unwrap()
}

pub fn wait_until(f: impl FnMut() -> bool) {
    assert!(wait_for(f, Duration::from_secs(10)), "condition never held");
}

/// The `register-replay` fixture binary, beside this test binary in the
/// cargo target directory. It sits behind `uc_lincheck`'s `replay-bin`
/// required-feature, so `cargo test` does NOT build it — assert rather than
/// skip (a silently skipped e2e test is not a test) and name the command
/// that fixes it. CI builds it before the test job.
pub fn register_replay_bin() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_BIN_EXE_uc2-diffreplay"));
    p.set_file_name("register-replay");
    assert!(
        p.exists(),
        "build it first: cargo build -p uc_lincheck --features replay-bin --bin register-replay ({})",
        p.display()
    );
    p
}

/// `uc2ctl snapshot` in process: command an instant, return its position P.
pub fn command_instant(node: &Node) -> u64 {
    uc_diffreplay::live::command_instant(node, Duration::from_secs(10)).unwrap()
}

pub fn register_name() -> &'static str {
    <RegisterSm as StateMachine>::NAME
}

/// `uc2ctl upgrade pin --row <row> --from <from> --to <to> --origin <origin>`
/// in process (FSM upgrade lifecycle spec §2.5, plan B1), asserting it was
/// accepted — the rig's own [`uc_diffreplay::live::pin_row`] with this
/// suite's timeout and a panic instead of an `Err`.
pub fn pin_row(dir: &Path, cnc: &CncPage, row: u8, from: u32, to: u32, origin: u64) -> AdminResp {
    uc_diffreplay::live::pin_row(dir, cnc, row, from, to, origin, Duration::from_secs(30)).unwrap()
}

/// Drive a single node with RegisterSm: `before`, a coordinated instant at
/// P, `after`. Returns P — the position an exported corpus anchors on, and
/// the artifact it installs from.
///
/// Both halves matter to a caller that exports `[P, …)`: only `after` lands
/// inside that span, so a corpus whose replay must see a command needs it
/// in `after`. `before` is what the artifact at P holds.
pub fn build_register_history_with(dir: &Path, app_id: &str, before: &[Cmd], after: &[Cmd]) -> u64 {
    // `start_single_node` already waits out `can_serve` (`live::start_node`),
    // so the client below cannot race the election.
    let node = start_single_node(dir, app_id, register_name());
    let cfg = ServiceConfig::new(dir.to_path_buf(), app_id.to_string());
    let svc = ServiceBuilder::new(cfg, RegisterSm::default())
        .start_with_snapshots()
        .unwrap();
    let client = Client::connect(dir, app_id).unwrap();
    for c in before {
        let _: CmdResp = client.submit(c).unwrap();
    }
    let p = command_instant(&node);
    // The instant completes when the row's artifact appears.
    let art = artifact_path(dir, 0, p);
    wait_until(|| art.is_file());
    for c in after {
        let _: CmdResp = client.submit(c).unwrap();
    }
    client.shutdown();
    svc.stop();
    node.stop();
    p
}

/// The all-writes special case: N writes, an instant at P, M more writes.
/// Returns `(P, u64::MAX)` — `Node` has no "applied frontier" accessor in
/// this plan's scope, so the end is left to the caller (the driver stops at
/// the journal's last frame instead of naming Q precisely).
pub fn build_register_history(dir: &std::path::Path, app_id: &str, n: u64, m: u64) -> (u64, u64) {
    let before: Vec<Cmd> = (0..n).map(Cmd::Write).collect();
    let after: Vec<Cmd> = (n..n + m).map(Cmd::Write).collect();
    (
        build_register_history_with(dir, app_id, &before, &after),
        u64::MAX,
    )
}
