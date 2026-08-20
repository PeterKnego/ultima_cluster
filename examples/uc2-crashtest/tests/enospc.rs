// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M11 Task 6: `ENOSPC` asserted end-to-end.
//!
//! The as-built anchor (`docs/superpowers/plans/2026-08-20-uc2-m11-survivable-cluster.md`,
//! "ENOSPC chain" row) is already real, wired product code, not something
//! this task adds: the journal writer halts on a write/fsync error
//! (`ultima_journal/src/journal/writer.rs:405-445`) -> the archive agent's
//! `.expect("archive fail-stop")` panics (`uc2_node/src/node.rs:1023`) -> the
//! agent's finished-flag flips -> the `uc2-node` daemon's outer loop notices
//! it, logs `agent_failstopped`, and exits `ExitCode::FAILURE`
//! (`uc2_node/src/bin/uc2-node.rs:134-145`) -> systemd's
//! `Restart=on-failure` would take it from there in production. This file
//! asserts that whole chain fires for real, end to end, from a genuinely
//! full filesystem, and that the cluster and the operator both come out the
//! other side intact: the surviving two nodes keep committing throughout,
//! and node 0 rejoins and converges once space is returned.
//!
//! # Why this spawns the REAL `uc2-node` daemon, not `uc2-crashtest-node`
//!
//! This crate's own reference bin (`uc2-crashtest-node.rs`) has no
//! fail-stop/exit-1 loop at all — it just sleep-polls forever regardless of
//! agent health (see its own doc comment), because the hard-crash suite it
//! serves kills processes from the OUTSIDE. The `agent_failstopped` ->
//! exit-1 chain this task must prove lives ONLY in the production daemon
//! binary (`uc2_node/src/bin/uc2-node.rs`), driven by a TOML config file
//! (`--config`), not `uc2-crashtest-node`'s CLI flags. Node 0 in both tests
//! below is therefore the real `uc2-node` daemon; nodes 1/2 (the
//! survivors, never under test) stay on the lighter-weight
//! `uc2-crashtest-node`/`uc2-crashtest-service` reference bins, exactly as
//! `survival.rs` uses them — the two bin flavors interoperate freely (same
//! wire protocol, same `NodeConfig` fields underneath either CLI surface).
//!
//! `CARGO_BIN_EXE_uc2-node` is NOT available here (verified empirically):
//! that env var is scoped to a package's OWN `[[bin]]` targets, and
//! `uc2-node` belongs to the `uc2_node` package, a dependency of this crate,
//! not this crate itself. [`uc2_node_daemon_bin`] below builds it on demand
//! via `cargo build -p uc2_node --bin uc2-node --message-format=json` and
//! parses the emitted artifact path — the same technique the `escargot`
//! crate automates, done by hand here so this crate needs no new dev-dep.
//!
//! # Two tests, one choreography, two different triggers
//!
//! - [`write_denied_drives_the_same_failstop_chain`]: **runs locally, no
//!   sudo needed.** `chmod 0555` on node 0's `journal/` directory mid-load
//!   drives `EACCES` on the journal writer's next segment-file create. This
//!   is the executable validation of the whole choreography above
//!   (daemon-binary discovery, TOML config shape, the load/reconnect
//!   machinery, the restart-and-converge tail) — the controller ruling for
//!   this task is that the box has no passwordless sudo, so this is the
//!   only one of the two tests actually exercised before landing.
//! - [`enospc_fails_stops_asserted_and_the_cluster_survives`]: the real
//!   bar — genuine `ENOSPC` from a small loopback ext4 filesystem
//!   (`scripts/enospc_fixture.sh`). Gated on BOTH the `enospc-tests`
//!   feature AND the `UC2_ENOSPC_DIR` env var (elle-style: unset means
//!   "skip cleanly", not "fail") because this box cannot mount loopback
//!   filesystems without sudo either — CI (`nightly.yml`'s `survival` job,
//!   which DOES have sudo) is where this one actually runs.
//!
//! The product's fail-stop path is errno-agnostic — the journal writer
//! halts on ANY write/fsync `io::Error`, `ENOSPC` and `EACCES` alike — so a
//! chain proven under `EACCES` is genuine evidence for the `ENOSPC` chain
//! too, not a simulation of it. What only the `ENOSPC` test can add is the
//! `ENOSPC` errno itself in the panic's io-error text; both tests assert
//! the shared `agent_failstopped`/`archive fail-stop`/exit-1 markers.
#![cfg(feature = "enospc-tests")]

use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use uc2_client::Client;
use uc2_log::cnc::CncPage;
use uc_lincheck::register::{Cmd, CmdResp};
use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER};

mod common;
use common::*;

/// A tempdir on the ext4 target volume, never `/tmp` (see CLAUDE.md and
/// `survival.rs`'s identical helper) — used for everything except the
/// env-gated `UC2_ENOSPC_DIR`, which is operator/CI-provided.
fn tempdir() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-enospc-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

// ------------------------------------------- trimmed copies from survival.rs
//
// Per the task brief: "copy what you need into tests/enospc.rs, don't
// import across test files" — each integration test file is its own crate
// as far as rustc is concerned, so there is no cheap shared-module option
// other than `common/` (already used) without restructuring the harness.

fn free_addr() -> SocketAddr {
    let s = UdpSocket::bind("127.0.0.1:0").expect("bind probe");
    let addr = s.local_addr().unwrap();
    drop(s);
    addr
}

fn members_arg(members: &[(u32, SocketAddr)]) -> String {
    members.iter().map(|(id, a)| format!("{id}@{a}")).collect::<Vec<_>>().join(",")
}

fn spawn_node_multi(instance_dir: &Path, id: u32, bind: SocketAddr, members: &str) -> Reap {
    let child = Command::new(NODE_BIN)
        .arg("--instance-dir")
        .arg(instance_dir)
        .arg("--app-id")
        .arg(APP_ID)
        .arg("--id")
        .arg(id.to_string())
        .arg("--bind")
        .arg(bind.to_string())
        .arg("--members")
        .arg(members)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn node {id}: {e}"));
    Reap(child)
}

fn open_cnc(dir: &Path) -> Option<Arc<CncPage>> {
    CncPage::open_file(&dir.join("cnc2.dat"), APP_ID).ok()
}

/// Single-shot (non-blocking) leader scan across `dirs`: `Some(idx)` if
/// exactly one currently reports itself the sole serving leader, `None` if
/// none currently do (caller retries). A REAL split-brain (more than one
/// simultaneously) is still a hard invariant violation, not a retry case.
fn find_leader_once(dirs: &[PathBuf]) -> Option<usize> {
    let want = NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE;
    let serving: Vec<usize> = (0..dirs.len())
        .filter(|&i| open_cnc(&dirs[i]).is_some_and(|c| c.status().flags.load_acquire() & want == want))
        .collect();
    assert!(serving.len() <= 1, "split-brain: dirs {serving:?} all serve simultaneously");
    serving.first().copied()
}

// ------------------------------------------------- the uc2-node daemon bin

/// Resolve the real production `uc2-node` daemon binary. See the module doc
/// for why `CARGO_BIN_EXE_uc2-node` doesn't work here. Cached process-wide:
/// every test in this binary that needs it triggers at most one
/// build-and-parse.
fn uc2_node_daemon_bin() -> &'static Path {
    static BIN: OnceLock<PathBuf> = OnceLock::new();
    BIN.get_or_init(|| {
        let out = Command::new(env!("CARGO"))
            .args(["build", "-p", "uc2_node", "--bin", "uc2-node", "--message-format=json"])
            .output()
            .expect("spawn `cargo build -p uc2_node --bin uc2-node`");
        assert!(
            out.status.success(),
            "cargo build -p uc2_node --bin uc2-node failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        // Hand-rolled JSON scan (no serde_json dep in this crate — same
        // rationale as the crypto b64 encoder in common/mod.rs): each
        // compiler-artifact line for the requested bin carries an
        // `"executable":"<path>"` field. Linux paths here never contain a
        // `"` or `\`, so a literal substring slice needs no unescaping.
        for line in stdout.lines() {
            if line.contains("\"name\":\"uc2-node\"")
                && let Some(rest) = line.split("\"executable\":\"").nth(1)
                && let Some(end) = rest.find('"')
            {
                return PathBuf::from(&rest[..end]);
            }
        }
        panic!(
            "cargo build --message-format=json for uc2-node produced no executable artifact:\n{stdout}"
        );
    })
}

/// Minimal TOML the daemon's `config_file::load_from_path` accepts — see
/// `uc2_node/src/config_file.rs`'s `NodeConfigFile`. `buffer_bytes` (must be
/// a power of two) and `journal_segment_bytes` are both kept small so
/// node 0's footprint fits comfortably inside a tiny filesystem and rolls a
/// new journal segment within seconds under light load (the CAUTION in this
/// task's brief: a segment ROLL, not a same-segment write, is what turns a
/// full/permission-denied filesystem into an error the writer observes —
/// writes into an already-open fd succeed under POSIX regardless).
fn write_node_toml(
    cfg_dir: &Path,
    id: u32,
    bind: SocketAddr,
    instance_dir: &Path,
    members: &[(u32, SocketAddr)],
) -> PathBuf {
    let mut members_toml = String::new();
    for (mid, maddr) in members {
        members_toml.push_str(&format!("[[members]]\nid = {mid}\naddr = \"{maddr}\"\n\n"));
    }
    let body = format!(
        "id = {id}\n\
         bind = \"{bind}\"\n\
         instance_dir = \"{}\"\n\
         app_id = \"{APP_ID}\"\n\
         buffer_bytes = {}\n\
         journal_segment_bytes = {}\n\
         \n\
         {members_toml}",
        instance_dir.display(),
        1u64 << 20,  // 1 MiB — power of two (preflight requires it), small.
        32 * 1024,   // 32 KiB — well inside the brief's 16-64 KiB guidance.
    );
    let path = cfg_dir.join(format!("n{id}.toml"));
    std::fs::write(&path, body).expect("write node.toml");
    path
}

/// Spawn the real daemon with `--config <config_path>`, stderr piped (not
/// inherited — the caller needs to inspect it for the fail-stop markers).
fn spawn_daemon(config_path: &Path) -> Child {
    Command::new(uc2_node_daemon_bin())
        .arg("--config")
        .arg(config_path)
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {} --config {}: {e}", uc2_node_daemon_bin().display(), config_path.display()))
}

/// Drain `child`'s piped stderr on a background thread — required so
/// `Child::wait`/`try_wait` can't deadlock behind a full pipe — mirroring
/// each line to this process's own stderr (visible under `--nocapture`)
/// while accumulating it into the returned buffer for the fail-stop marker
/// assertions. The thread exits on its own once the pipe closes (child
/// exit).
fn capture_stderr(child: &mut Child, tag: &'static str) -> Arc<Mutex<String>> {
    let stderr = child.stderr.take().expect("child spawned with Stdio::piped() stderr");
    let buf = Arc::new(Mutex::new(String::new()));
    let buf2 = Arc::clone(&buf);
    std::thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for line in reader.lines().map_while(Result::ok) {
            eprintln!("[{tag}] {line}");
            let mut b = buf2.lock().unwrap();
            b.push_str(&line);
            b.push('\n');
        }
    });
    buf
}

/// Poll `child.try_wait()` up to `timeout`. `None` on timeout (still
/// running).
fn poll_exit(child: &mut Child, timeout: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ------------------------------------------------------- the choreography
//
// Shared by both tests below (same shape, different trigger — see the
// module doc): boot 3 nodes, drive load until node 0 fail-stops (asserting
// the chain), prove the survivors keep committing, return space/permission,
// restart node 0 and require it to converge. Each test supplies its own
// trigger (a side thread doing the `chmod`, or simply the fixture's
// pre-filled ballast) and its own "space returned" step inline, around
// these shared helpers.

/// True for the load loop's per-op error handling: "the currently-connected
/// node just isn't ready yet, keep this same client and retry." Reused
/// (not imported) from `common::is_transient`'s exact semantics — that
/// helper covers the SAME-client-retry case; here it doubles as the
/// SAME-client-retry test, with anything else read as "this node might be
/// dead, reconnect elsewhere."
fn same_client_retry(e: &uc2_client::ClientError) -> bool {
    is_transient(e)
}

/// Drives a serial `Cas` chain against whichever `dirs[idx]` is CURRENTLY
/// the sole serving leader, reconnecting whenever the connected node stops
/// answering usefully (covers a leadership hand-off, including node 0
/// itself dying if it happened to be leader at the moment of its own
/// fail-stop). Runs until `node0` exits (checked every iteration) or
/// `deadline`. Returns the final acked count.
///
/// `dirs` is the set of instance dirs eligible to serve the load — during
/// phase 1 this is ALL THREE nodes (node 0 legitimately might be leader
/// right up until it dies: a write commits to every voter's journal
/// regardless of who leads, so node 0's own journal grows whether it
/// leads or follows; excluding it here would risk deadlocking on the ~1/3
/// chance it wins the initial election and no OTHER node ever becomes
/// leader to drive its journal growth). During phase 2 the caller passes
/// only the two survivor dirs.
fn drive_load_until(
    dirs: &[PathBuf],
    node0: Option<&mut Child>,
    acked: &mut u64,
    min_acks: Option<u64>,
    deadline: Instant,
) -> bool {
    let mut cur: Option<Client> = None;
    let mut node0 = node0;
    loop {
        if let Some(child) = node0.as_deref_mut()
            && let Ok(Some(_status)) = child.try_wait()
        {
            return true; // node0 exited -- caller inspects the ExitStatus itself.
        }
        if let Some(want) = min_acks
            && *acked >= want
        {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        if cur.is_none() {
            match find_leader_once(dirs) {
                Some(idx) => match Client::connect(&dirs[idx], APP_ID) {
                    Ok(c) => {
                        // Resync `acked` to the newly-connected node's ACTUAL
                        // value before resuming the CAS chain. Required for
                        // soundness, not just belt-and-braces: a `Cas` whose
                        // response never reached the client (a hard error --
                        // e.g. the previously-connected node died mid-request)
                        // may have already committed. Blindly retrying the
                        // SAME (old, new) pair against a new connection would
                        // then legitimately (and misleadingly) come back
                        // `CasResult(false)`, since the register already
                        // holds `new`. A single serial writer makes this
                        // read-then-resume race-free: nothing else submits
                        // between this read and the next `submit` below.
                        match c.query_linearizable::<(), Option<u64>>(&()) {
                            Ok(v) => {
                                *acked = v.unwrap_or(0);
                                cur = Some(c);
                            }
                            Err(_) => std::thread::sleep(Duration::from_millis(20)),
                        }
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(20)),
                },
                None => std::thread::sleep(Duration::from_millis(20)),
            }
            continue;
        }
        let client = cur.as_ref().unwrap();
        match client.submit::<Cmd, CmdResp>(&Cmd::Cas { old: *acked, new: *acked + 1 }) {
            Ok(CmdResp::CasResult(true)) => *acked += 1,
            Ok(CmdResp::CasResult(false)) => panic!(
                "CAS(old={}, new={}) unexpectedly failed under a single serial writer",
                *acked,
                *acked + 1
            ),
            Ok(other) => panic!("unexpected response to Cas: {other:?}"),
            Err(e) if same_client_retry(&e) => std::thread::sleep(Duration::from_millis(20)),
            Err(e) => {
                eprintln!("[load] dropping client after {e} -- reconnecting to a (possibly new) leader");
                cur = None;
            }
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Warm the register to `Some(0)` on whichever of `dirs` is leader (bounded,
/// reused by both tests before starting the timed load).
fn warm_up(dirs: &[PathBuf], secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        if let Some(idx) = find_leader_once(dirs)
            && let Ok(c) = Client::connect(&dirs[idx], APP_ID)
        {
            let d2 = Instant::now() + Duration::from_secs(secs);
            submit_until_ok(&c, &Cmd::Write(0), d2);
            c.shutdown();
            return;
        }
        assert!(Instant::now() < deadline, "no leader elected among {dirs:?} within {secs}s");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Common 3-node bring-up: node 0 is the REAL `uc2-node` daemon (config
/// written under `tmp`, instance dir `dir0`); nodes 1/2 are the lighter
/// `uc2-crashtest-node`/`uc2-crashtest-service` reference bins under `tmp`.
/// See [`boot_cluster`].
struct Cluster {
    dirs: Vec<PathBuf>,
    node0_child: Child,
    node0_stderr: Arc<Mutex<String>>,
    node0_old_instance_id: u128,
    node0_cfg_path: PathBuf,
    // Index 0 stays `None` (node 0 is tracked separately above); 1/2 hold
    // the survivors, auto-killed+reaped on drop. Never read after
    // construction -- held purely for `Reap`'s `Drop` side effect, same
    // convention `Reap` itself documents.
    #[allow(dead_code)]
    survivor_node_procs: Vec<Option<Reap>>,
    #[allow(dead_code)]
    svc_procs: Vec<Option<Reap>>,
}

fn boot_cluster(tmp: &Path, dir0: PathBuf) -> Cluster {
    const N: usize = 3;
    let addrs: Vec<SocketAddr> = (0..N).map(|_| free_addr()).collect();
    let members: Vec<(u32, SocketAddr)> = (0..N as u32).map(|i| (i, addrs[i as usize])).collect();
    let members_str = members_arg(&members);

    let dirs: Vec<PathBuf> = vec![dir0.clone(), tmp.join("n1"), tmp.join("n2")];
    std::fs::create_dir_all(&dirs[1]).unwrap();
    std::fs::create_dir_all(&dirs[2]).unwrap();

    let node0_cfg_path = write_node_toml(tmp, 0, addrs[0], &dir0, &members);
    let mut node0_child = spawn_daemon(&node0_cfg_path);
    let node0_stderr = capture_stderr(&mut node0_child, "node0");
    wait_for_ready(&dirs[0], Duration::from_secs(15));
    let node0_old_instance_id = {
        let c = connect_with_retry(&dirs[0], Duration::from_secs(10));
        let id = c.instance_id();
        c.shutdown();
        id
    };

    let mut survivor_node_procs: Vec<Option<Reap>> = vec![None, None, None];
    survivor_node_procs[1] = Some(spawn_node_multi(&dirs[1], 1, addrs[1], &members_str));
    wait_for_ready(&dirs[1], Duration::from_secs(10));
    survivor_node_procs[2] = Some(spawn_node_multi(&dirs[2], 2, addrs[2], &members_str));
    wait_for_ready(&dirs[2], Duration::from_secs(10));

    let svc_procs: Vec<Option<Reap>> = dirs.iter().map(|d| Some(spawn_service(d))).collect();

    Cluster {
        dirs,
        node0_child,
        node0_stderr,
        node0_old_instance_id,
        node0_cfg_path,
        survivor_node_procs,
        svc_procs,
    }
}

/// Phase 1 + assertion: drive load (across all 3 dirs -- see
/// `drive_load_until`'s doc) until node 0 exits on its own, bounded by
/// `bound_secs`, then assert exit code 1 and the fail-stop markers.
fn run_until_failstop_and_assert(cluster: &mut Cluster, acked: &mut u64, bound_secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(bound_secs);
    let exited = drive_load_until(&cluster.dirs, Some(&mut cluster.node0_child), acked, None, deadline);
    assert!(
        exited,
        "node 0's daemon never exited within {bound_secs}s -- the fault was never induced, or the \
         fail-stop chain did not fire"
    );
    let status =
        poll_exit(&mut cluster.node0_child, Duration::from_secs(5)).expect("node0 already exited");
    let stderr_text = cluster.node0_stderr.lock().unwrap().clone();
    assert_eq!(
        status.code(),
        Some(1),
        "node 0 daemon exit code (expected 1, the agent_failstopped path); stderr:\n{stderr_text}"
    );
    assert!(
        stderr_text.contains("agent_failstopped"),
        "stderr missing 'agent_failstopped':\n{stderr_text}"
    );
    assert!(
        stderr_text.contains("archive fail-stop"),
        "stderr missing 'archive fail-stop':\n{stderr_text}"
    );
}

/// Phase 2 + assertion: the two survivors keep committing without node 0.
fn assert_survivors_keep_committing(cluster: &Cluster, acked: &mut u64, min_acks: u64, bound_secs: u64) {
    let survivor_dirs = vec![cluster.dirs[1].clone(), cluster.dirs[2].clone()];
    let before = *acked;
    let deadline = Instant::now() + Duration::from_secs(bound_secs);
    let ok = drive_load_until(&survivor_dirs, None, acked, Some(before + min_acks), deadline);
    assert!(
        ok && *acked >= before + min_acks,
        "the two survivors did not keep committing without node 0 within {bound_secs}s \
         (acked before={before}, after={acked})"
    );
}

/// Phase 3: restart node 0 on the SAME config/instance dir, wait for a
/// fresh instance_id, require its durable position to converge with the
/// survivors' current commit, and prove the whole node (not just the raw
/// journal byte count) is healthy: respawn its service (the ORIGINAL
/// service process fail-stops itself on the instance_id change by its own
/// v2.0 contract -- "supervisor respawns the service", and this harness is
/// that supervisor) and require a fresh linearizable read to reach the
/// survivors' value.
fn restart_node0_and_assert_converges(cluster: &mut Cluster) {
    let mut node0_child = spawn_daemon(&cluster.node0_cfg_path);
    let _stderr = capture_stderr(&mut node0_child, "node0-restarted");
    wait_for_fresh_instance(&cluster.dirs[0], cluster.node0_old_instance_id, Duration::from_secs(15));
    cluster.node0_child = node0_child; // now guarded by the struct's own teardown at scope-end.
    cluster.svc_procs[0] = Some(spawn_service(&cluster.dirs[0]));

    let leader_idx = loop {
        if let Some(idx) = find_leader_once(&[cluster.dirs[1].clone(), cluster.dirs[2].clone()]) {
            break idx + 1; // offset: this 2-elem scan skips index 0.
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let leader_client = connect_with_retry(&cluster.dirs[leader_idx], Duration::from_secs(10));
    let target_commit = open_cnc(&cluster.dirs[leader_idx])
        .expect("survivor leader cnc")
        .counters()
        .commit
        .load_acquire();
    let target_value: Option<u64> =
        leader_client.query_linearizable(&()).expect("survivor leader linearizable read");
    leader_client.shutdown();

    let converge_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(c) = open_cnc(&cluster.dirs[0])
            && c.counters().durable.load_acquire() >= target_commit
        {
            break;
        }
        assert!(
            Instant::now() < converge_deadline,
            "node 0's durable position never converged (>= {target_commit}) after restarting on \
             space/permission return"
        );
        std::thread::sleep(Duration::from_millis(50));
    }

    // Not just the byte-level journal: the restarted node's own SERVICE
    // must also catch up and be able to answer.
    let restored_client = connect_with_retry(&cluster.dirs[0], Duration::from_secs(10));
    loop {
        let v: Option<u64> = restored_client.query_snapshot(&()).expect("node0 snapshot read");
        if v.unwrap_or(0) >= target_value.unwrap_or(0) {
            break;
        }
        assert!(
            Instant::now() < converge_deadline,
            "node 0's service never caught up to the survivors' value ({target_value:?}, last saw {v:?})"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    restored_client.shutdown();
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = self.node0_child.kill();
        let _ = self.node0_child.wait();
        // survivor_node_procs / svc_procs drop themselves (Reap).
    }
}

// ---------------------------------------------------------- the two tests

/// Sudo-free sibling of the `ENOSPC` test below, and the one that actually
/// RUNS on this box: `chmod 0555` on node 0's `journal/` directory, mid-load,
/// drives `EACCES` on the writer's next segment-file create -- the SAME
/// io-error -> journal-halt -> `ArchiveError` -> `archive.do_work(...)
/// .expect("archive fail-stop")` panic -> `agent_failstopped` -> daemon
/// exit-1 chain the `ENOSPC` test exercises, just with a different errno at
/// the root (the product's fail-stop is errno-agnostic: ANY journal
/// write/fsync `io::Error` halts it, not specifically `ENOSPC` -- see the
/// module doc). This is the executable proof that the choreography itself
/// (daemon-binary discovery, TOML config, load/reconnect machinery, the
/// restart-and-converge tail) actually works, since the `ENOSPC` test below
/// cannot be run on this host (no passwordless sudo) and must be taken on
/// faith until CI (which has sudo) runs it.
#[test]
fn write_denied_drives_the_same_failstop_chain() {
    shorten_client_timeout();
    let tmp = tempdir();
    let dir0 = tmp.path().join("n0");
    std::fs::create_dir_all(&dir0).unwrap();

    let mut cluster = boot_cluster(tmp.path(), dir0.clone());
    warm_up(&cluster.dirs, 15);

    let mut acked: u64 = 0;
    let chmod_applied = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // A tiny watcher thread: as soon as a handful of CAS ops have landed
    // (proving the cluster is genuinely live and node 0 is receiving
    // replicated writes into its own journal), flip its journal/ dir to
    // read+execute-only. Done from a side thread rather than inline in the
    // load loop so the trigger fires on wall-clock/ack progress
    // independently of which node the load loop happens to be talking to
    // at that instant.
    {
        let journal_dir = cluster.dirs[0].join("journal");
        let flag = Arc::clone(&chmod_applied);
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(400));
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&journal_dir, std::fs::Permissions::from_mode(0o555));
            eprintln!("[test] chmod 0555 applied to {}", journal_dir.display());
            flag.store(true, std::sync::atomic::Ordering::Release);
        });
    }

    run_until_failstop_and_assert(&mut cluster, &mut acked, 60);
    assert!(
        chmod_applied.load(std::sync::atomic::Ordering::Acquire),
        "node 0 exited before the chmod trigger even ran -- this would be a false pass"
    );

    assert_survivors_keep_committing(&cluster, &mut acked, 10, 20);

    // "Space returned" for this trigger = permission returned.
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(cluster.dirs[0].join("journal"), std::fs::Permissions::from_mode(0o755))
        .expect("chmod 0755 back");

    restart_node0_and_assert_converges(&mut cluster);
}

/// The real bar: genuine `ENOSPC` from a small loopback ext4 filesystem
/// created by `scripts/enospc_fixture.sh`. Gated on `UC2_ENOSPC_DIR`
/// (elle-style: unset = skip, not fail) because this box cannot mount
/// loopback filesystems without sudo -- see `write_denied_drives_the_same_
/// failstop_chain` above for the executable validation of everything this
/// test shares with it, and the module doc for why this one could not be
/// run locally while implementing this task.
///
/// Contract with the fixture/CI step (`.github/workflows/nightly.yml`'s
/// `survival` job): `UC2_ENOSPC_DIR` names a directory that is ALREADY a
/// mounted loopback ext4 filesystem, pre-filled by the fixture's ballast
/// file down to a small headroom margin. This test's own "space returned"
/// step is `rm $UC2_ENOSPC_DIR/uc2-enospc-ballast` -- the fixture's
/// `destroy` (unmount + remove the image) is a SEPARATE, later CI step, run
/// unconditionally (`if: always()`), not part of this test.
#[test]
fn enospc_fails_stops_asserted_and_the_cluster_survives() {
    let Ok(dir0_str) = std::env::var("UC2_ENOSPC_DIR") else {
        eprintln!(
            "skipped: UC2_ENOSPC_DIR is not set -- this test needs a pre-mounted, pre-ballasted \
             loopback ext4 filesystem (scripts/enospc_fixture.sh); run under nightly.yml's \
             `survival` job, or manually with sudo (see the fixture script's usage)."
        );
        return;
    };
    let dir0 = PathBuf::from(dir0_str);
    let ballast = dir0.join("uc2-enospc-ballast");
    assert!(
        ballast.exists(),
        "UC2_ENOSPC_DIR={} has no ballast file at {} -- was it created via \
         `scripts/enospc_fixture.sh create <dir> <size-mb>`?",
        dir0.display(),
        ballast.display()
    );

    shorten_client_timeout();
    let tmp = tempdir();

    let mut cluster = boot_cluster(tmp.path(), dir0);
    warm_up(&cluster.dirs, 15);

    let mut acked: u64 = 0;
    // No explicit trigger needed: the fixture's ballast already leaves only
    // a small headroom margin, so ordinary load exhausts it within seconds
    // as node 0's journal rolls new (small) segments.
    run_until_failstop_and_assert(&mut cluster, &mut acked, 60);

    let stderr_text = cluster.node0_stderr.lock().unwrap().clone();
    // ENOSPC == errno 28; Rust's io::Error Display embeds "(os error 28)"
    // regardless of the OS's (possibly localized) strerror text, so this is
    // the portable assertion -- the brief's "the io error text".
    assert!(
        stderr_text.contains("os error 28"),
        "stderr does not name ENOSPC (os error 28):\n{stderr_text}"
    );

    assert_survivors_keep_committing(&cluster, &mut acked, 10, 20);

    // Space returned: delete the ballast on the SAME filesystem/instance
    // dir -- no unmount/remount (the fixture's `destroy` is a separate,
    // later step; see this test's own doc comment).
    std::fs::remove_file(&ballast).expect("rm the ballast file (space returned)");

    restart_node0_and_assert_converges(&mut cluster);
}
