// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M9: the service half's signal discipline.
//!
//! Lives in the `counter` package, not `uc_node`, because cargo sets
//! `CARGO_BIN_EXE_<name>` only for the package that DEFINES the binary — a test
//! in `uc_node` can never see `counter-service`. Ruling 1 in the M9 plan. The
//! node half's own signal handling is covered by `uc_node/tests/lifecycle.rs`.

use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// Distinct from every port used by `uc_node/tests/lifecycle.rs` — these
/// suites can run concurrently.
const PORT: u16 = 19711;

/// Kills its child on drop, INCLUDING while a panic unwinds.
///
/// Without this a failing assertion leaks a `counter-node`, which is a
/// busy-spin process: one leaked run burned 39 minutes of CPU and hung the
/// suite, because the orphan inherits the harness's stdout pipe and cargo
/// blocks reading it until EOF. Stdio is nulled for the same reason.
struct Reaped(Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn(cmd: &mut Command) -> Reaped {
    Reaped(
        cmd.stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn"),
    )
}

#[test]
fn service_template_stops_cleanly_on_sigterm() {
    let dir = tempfile::Builder::new()
        .prefix("counter-lifecycle-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let inst = dir.path().join("n1");
    std::fs::create_dir_all(&inst).unwrap();

    let _node = spawn(
        Command::new(env!("CARGO_BIN_EXE_counter-node"))
            .arg("--instance-dir")
            .arg(&inst)
            .arg("--id")
            .arg("1")
            .arg("--bind")
            .arg(format!("127.0.0.1:{PORT}")),
    );
    std::thread::sleep(Duration::from_millis(1000));

    let mut svc = spawn(
        Command::new(env!("CARGO_BIN_EXE_counter-service"))
            .arg("--instance-dir")
            .arg(&inst),
    );
    std::thread::sleep(Duration::from_millis(1000));

    unsafe { libc::kill(svc.0.id() as i32, libc::SIGTERM) };
    let svc_status = svc.0.wait().expect("wait service");
    assert!(
        svc_status.success(),
        "service must handle SIGTERM and exit 0, got {svc_status:?} — a service killed by \
         the default disposition never calls Service::stop"
    );
}
