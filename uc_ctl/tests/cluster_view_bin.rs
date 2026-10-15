// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Cluster view (storage-service project 4, spec §6.3) — end-to-end bin
//! tests for `uc2ctl cluster` / `uc2ctl catalog`: the live read over
//! `<instance_dir>/obs.sock` (an `ObsServer` this test binds for its
//! in-process node, planning ruling PR11), the named refusal when the lock is
//! held but no socket answers (PR10), and the offline document built from the
//! newest cluster artifact once the node stops.

mod common;

use std::path::Path;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::{
    await_leader, await_row0_capable, parse_instant, run_ctl, start_capable_service, start_node,
};
use uc_node::Node;

const APP: &str = "ctlview";

fn scratch(prefix: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

fn serve(node: &Node, dir: &Path) -> uc_node::obs::http::ObsServer {
    uc_node::obs::http::ObsServer::serve_on(node.observability(), None, Some(dir))
        .expect("obs socket")
}

#[test]
fn cluster_json_reads_the_live_view_over_the_socket() {
    let root = scratch("uc2ctl-view-live-");
    let (node, dir) = start_node(root.path(), "n0", APP);
    await_leader(&node, 20);
    let srv = serve(&node, &dir);
    let d = dir.to_str().unwrap();
    let r = run_ctl(&["cluster", "--json", "--instance-dir", d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    let v: serde_json::Value = serde_json::from_str(&r.stdout).expect("stdout is JSON");
    assert_eq!(v["schema"], "uc2.cluster-view/1");
    assert_eq!(v["source"], "live");
    assert_eq!(v["role"], "leader");
    // Verbatim (spec §6.3, ruling P16): the node's pretty-printed body, line
    // for line — a second read of the same idle node has the same shape.
    let (code, body) = uc_node::obs::socket::get(&dir, "/cluster").unwrap();
    assert_eq!(code, 200);
    assert!(body.starts_with("{\n  \"schema\""), "{body}");
    assert!(r.stdout.starts_with("{\n  \"schema\""), "{}", r.stdout);
    assert_eq!(r.stdout.lines().count(), body.lines().count());
    srv.stop();
    node.stop();
}

#[test]
fn catalog_text_on_a_live_node_prints_the_source_and_attention() {
    let root = scratch("uc2ctl-view-catalog-");
    let (node, dir) = start_node(root.path(), "n0", APP);
    await_leader(&node, 20);
    let srv = serve(&node, &dir);
    let d = dir.to_str().unwrap();
    let r = run_ctl(&["catalog", "--instance-dir", d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    assert!(r.stdout.contains("source=live"), "{}", r.stdout);
    assert!(r.stdout.contains("role=leader"), "{}", r.stdout);
    assert!(r.stdout.contains("catalog_empty"), "{}", r.stdout);
    assert!(r.stdout.contains(" empty=true "), "{}", r.stdout);
    let r = run_ctl(&["catalog", "--json", "--instance-dir", d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    let v: serde_json::Value = serde_json::from_str(&r.stdout).expect("stdout is JSON");
    assert_eq!(v["schema"], "uc2.cluster-view/1");
    assert_eq!(v["source"], "live");
    assert!(v["holdings"].is_object(), "{v}");
    assert!(v["catalog"]["empty"].as_bool().unwrap(), "{v}");
    assert!(
        v.get("cluster").is_none(),
        "catalog is the filtered sub-object"
    );
    srv.stop();
    node.stop();
}

#[test]
fn a_held_lock_without_a_socket_is_reported_by_name() {
    let root = scratch("uc2ctl-view-nosock-");
    let (node, dir) = start_node(root.path(), "n0", APP);
    await_leader(&node, 20);
    let d = dir.to_str().unwrap();
    for verb in ["cluster", "catalog"] {
        let r = run_ctl(&[verb, "--instance-dir", d, "--app-id", APP]);
        assert_ne!(r.status, 0, "{r:?}");
        assert!(
            r.stderr.contains(
                "node running but its obs socket is not bound (starting, or not run by uc2-node)"
            ),
            "{}",
            r.stderr
        );
    }
    node.stop();
}

#[test]
fn a_stopped_node_reads_offline_and_a_leftover_socket_is_ignored() {
    let root = scratch("uc2ctl-view-offline-");
    let (node, dir) = start_node(root.path(), "n0", APP);
    await_leader(&node, 20);
    let svc = start_capable_service(&dir, APP);
    await_row0_capable(&dir, APP, 20);
    let d = dir.to_str().unwrap().to_string();
    let r = run_ctl(&["snapshot", "--instance-dir", &d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    let p = parse_instant(&r.stdout);
    let deadline = Instant::now() + Duration::from_secs(30);
    while node.snapshot_set_position() != p {
        assert!(Instant::now() < deadline, "set {p} never completed");
        std::thread::sleep(Duration::from_millis(20));
    }
    svc.stop();
    node.stop();
    drop(uc_node::obs::socket::bind(&dir).unwrap()); // a dead obs.sock
    let r = run_ctl(&["catalog", "--json", "--instance-dir", &d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    let v: serde_json::Value = serde_json::from_str(&r.stdout).unwrap();
    assert_eq!(v["source"], "artifact");
    assert!(v["holdings"].is_null());
    // P32: the artifact cannot show its own set's completion, so with no
    // earlier complete set emptiness is unknown (null, not skipped) and
    // offline never raises catalog_empty.
    assert!(
        v["catalog"]
            .as_object()
            .unwrap()
            .get("empty")
            .is_some_and(|e| e.is_null()),
        "{v}"
    );
    assert!(
        v["attention"]
            .as_array()
            .unwrap()
            .iter()
            .all(|a| a["code"] != "catalog_empty"),
        "{v}"
    );
    assert_eq!(
        v["catalog"]["sets"][0]["rows"][0]["name"], "sum",
        "names come from the stale cnc2.dat"
    );
    let r = run_ctl(&["catalog", "--instance-dir", &d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    assert!(
        r.stdout
            .contains(&format!("offline: newest artifact at {p}")),
        "{}",
        r.stdout
    );
    // The artifact at P was cut AT the instant, so it records its own set
    // as still commanded; the words are the JSON words (ruling P18).
    assert!(
        r.stdout
            .contains(&format!("set={p} kind=full state=commanded")),
        "{}",
        r.stdout
    );
    assert!(
        r.stdout
            .contains("(cut at this artifact; its completion is not visible offline)"),
        "{}",
        r.stdout
    );
    assert!(
        r.stdout.contains(" empty=unknown (offline) "),
        "{}",
        r.stdout
    );
    let r = run_ctl(&["cluster", "--json", "--instance-dir", &d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    let v: serde_json::Value = serde_json::from_str(&r.stdout).unwrap();
    assert_eq!(v["source"], "artifact");
    assert!(v["self"].is_null() && v["nodes"].is_null(), "{v}");
}

/// P32: once an EARLIER set completed, the newest artifact proves the
/// catalog is not empty, so offline `empty` reads `false`.
#[test]
fn a_stopped_node_after_two_instants_reads_empty_false_offline() {
    let root = scratch("uc2ctl-view-offline2-");
    let (node, dir) = start_node(root.path(), "n0", APP);
    await_leader(&node, 20);
    let svc = start_capable_service(&dir, APP);
    await_row0_capable(&dir, APP, 20);
    let d = dir.to_str().unwrap().to_string();
    let mut p = 0;
    for i in 0..2 {
        let r = run_ctl(&["snapshot", "--instance-dir", &d, "--app-id", APP]);
        assert_eq!(r.status, 0, "{r:?}");
        p = parse_instant(&r.stdout);
        let deadline = Instant::now() + Duration::from_secs(30);
        while node.snapshot_set_position() != p {
            assert!(Instant::now() < deadline, "set {p} never completed");
            std::thread::sleep(Duration::from_millis(20));
        }
        // The node holding its set is not the catalog recording it: wait
        // until the first set's completion has COMMITTED, so the second
        // instant's artifact (cut after it) records it as complete.
        if i == 0 {
            let view = node.observability().cluster_view;
            while !view.catalog_has_complete.load(Ordering::Acquire) {
                assert!(
                    Instant::now() < deadline,
                    "set {p} never completed in the catalog"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
    svc.stop();
    node.stop();
    let r = run_ctl(&["catalog", "--json", "--instance-dir", &d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    let v: serde_json::Value = serde_json::from_str(&r.stdout).unwrap();
    assert_eq!(v["source"], "artifact");
    assert_eq!(v["view_position"], p, "{v}");
    assert_eq!(v["catalog"]["empty"], false, "{v}");
    let r = run_ctl(&["catalog", "--instance-dir", &d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    assert!(r.stdout.contains(" empty=false "), "{}", r.stdout);
}

#[test]
fn a_stopped_node_with_no_artifact_says_so() {
    let root = scratch("uc2ctl-view-none-");
    let (node, dir) = start_node(root.path(), "n0", APP);
    await_leader(&node, 20);
    node.stop();
    let d = dir.to_str().unwrap();
    let r = run_ctl(&["cluster", "--instance-dir", d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    assert!(r.stdout.contains("no cluster artifact yet"), "{}", r.stdout);
    // Under --json stdout stays JSON-only: the message goes to stderr.
    let r = run_ctl(&["cluster", "--json", "--instance-dir", d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    assert!(r.stdout.is_empty(), "{}", r.stdout);
    assert!(r.stderr.contains("no cluster artifact yet"), "{}", r.stderr);
}

/// A mistyped `--instance-dir` must not read as a fresh node ("no cluster
/// artifact yet", exit 0): a missing path and an existing directory no node
/// ever ran in are both refused by name, by every command that reads the view.
#[test]
fn a_path_that_is_not_an_instance_dir_is_refused_by_name() {
    let root = scratch("uc2ctl-view-notdir-");
    let missing = root.path().join("no-such-dir");
    let empty = root.path().join("empty");
    std::fs::create_dir(&empty).unwrap();
    for (dir, want) in [
        (&missing, "no such instance directory"),
        (&empty, "not an instance directory"),
    ] {
        let d = dir.to_str().unwrap();
        for args in [
            &["cluster"][..],
            &["catalog", "--json"][..],
            &["settings", "show"][..],
            &["schedule", "show"][..],
            &["upgrade", "show"][..],
        ] {
            let mut argv = args.to_vec();
            argv.extend(["--instance-dir", d, "--app-id", APP]);
            let r = run_ctl(&argv);
            assert_ne!(r.status, 0, "{argv:?} must fail: {r:?}");
            assert!(r.stderr.contains(want), "{argv:?}: {}", r.stderr);
            assert!(
                !r.stdout.contains("no cluster artifact yet"),
                "{argv:?}: {}",
                r.stdout
            );
        }
    }
}

#[test]
fn settings_show_reads_the_live_genesis_before_any_instant() {
    let root = scratch("uc2ctl-view-settings-");
    let (node, dir) = start_node(root.path(), "n0", APP);
    await_leader(&node, 20);
    let srv = serve(&node, &dir);
    let d = dir.to_str().unwrap();
    let r = run_ctl(&["settings", "show", "--instance-dir", d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    assert!(
        !r.stdout.contains("no cluster artifact yet"),
        "{}",
        r.stdout
    );
    assert!(r.stdout.contains("retain_sets=1"), "{}", r.stdout);
    assert!(r.stdout.contains("snapshot_target=all"), "{}", r.stdout);
    assert!(!r.stderr.contains("offline:"), "live prints no marker");
    // The other two read the live view as well, without erroring.
    for cmd in ["schedule", "upgrade"] {
        let r = run_ctl(&[cmd, "show", "--instance-dir", d, "--app-id", APP]);
        assert_eq!(r.status, 0, "{cmd}: {r:?}");
        assert!(!r.stderr.contains("offline:"), "{cmd}: {}", r.stderr);
    }
    srv.stop();
    node.stop();
}

/// P13/P21: a stopped node reads the artifact; stderr carries the marker,
/// stdout is what the renderer prints for that artifact.
#[test]
fn show_commands_on_a_stopped_node_mark_stderr_and_keep_stdout() {
    let root = scratch("uc2ctl-view-showoff-");
    let (node, dir) = start_node(root.path(), "n0", APP);
    await_leader(&node, 20);
    let svc = start_capable_service(&dir, APP);
    await_row0_capable(&dir, APP, 20);
    let d = dir.to_str().unwrap().to_string();
    let r = run_ctl(&["snapshot", "--instance-dir", &d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    let p = parse_instant(&r.stdout);
    let deadline = Instant::now() + Duration::from_secs(30);
    while node.snapshot_set_position() != p {
        assert!(Instant::now() < deadline, "set {p} never completed");
        std::thread::sleep(Duration::from_millis(20));
    }
    svc.stop();
    node.stop();
    let (pos, settings) = uc_node::cluster_agent::read_committed_settings(dir.as_path())
        .unwrap()
        .expect("artifact");
    let r = run_ctl(&["settings", "show", "--instance-dir", &d, "--app-id", APP]);
    assert_eq!(r.status, 0, "{r:?}");
    // The renderer is private to the binary: pin the line's head and the
    // settings it must carry from the old artifact reader.
    assert_eq!(r.stdout.lines().count(), 1, "{}", r.stdout);
    assert!(
        r.stdout.starts_with(&format!(
            "position={pos} admission_bytes={} ",
            settings.admission_bytes
        )),
        "{}",
        r.stdout
    );
    assert!(
        r.stdout
            .contains(&format!("retain_sets={}", settings.retain_sets)),
        "{}",
        r.stdout
    );
    assert_eq!(r.stderr.matches("offline: newest artifact at").count(), 1);
    assert!(
        r.stderr
            .contains(&format!("offline: newest artifact at {pos}")),
        "{}",
        r.stderr
    );
    for cmd in ["schedule", "upgrade"] {
        let r = run_ctl(&[cmd, "show", "--instance-dir", &d, "--app-id", APP]);
        assert_eq!(r.status, 0, "{cmd}: {r:?}");
        assert!(
            r.stderr.contains("offline: newest artifact at"),
            "{}",
            r.stderr
        );
        assert!(!r.stdout.contains("offline:"), "{}", r.stdout);
    }
}
