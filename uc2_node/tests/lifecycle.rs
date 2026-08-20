// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M9: daemon lifecycle — drain-on-stop and restart cost.
//!
//! Construction follows `failover.rs`: a pre-bound socket, an instance dir
//! under `CARGO_TARGET_TMPDIR` (ext4, never the RAM-backed `/tmp` — see
//! CLAUDE.md "Local box"), and a sole voter that elects itself.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use uc2_log::archive::{Archive, ArchiveConfig};
use uc2_node::{DrainOutcome, Node, NodeConfig};

const PAYLOAD: usize = 96;
const RING: usize = 1 << 22;
/// Enough load that the archive is genuinely behind when the stop is issued.
/// See the drain test's comment on why a small load cannot discriminate.
const LOAD: u64 = 5_000;

fn config_for(addr: SocketAddr, instance_dir: PathBuf) -> NodeConfig {
    NodeConfig {
        id: 0,
        members: vec![(0, addr)],
        learners: Vec::new(),
        bind: addr,
        instance_dir,
        app_id: "lifecycle".into(),
        buffer_bytes: RING,
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: 0xA1B2_C3D4_5566_7788,
        faults: Default::default(),
        purge: uc2_node::PurgePolicy::Disabled,
        journal_segment_bytes: uc2_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto: uc2_node::CryptoConfig::Disabled,
    }
}

/// A sole-voter node on a freshly bound loopback socket, already serving.
fn single_node(instance_dir: &Path) -> (Node, SocketAddr) {
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind");
    let addr = sock.local_addr().unwrap();
    let node = Node::start_with_socket(config_for(addr, instance_dir.to_path_buf()), sock)
        .expect("start");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !node.can_serve() {
        assert!(Instant::now() < deadline, "sole voter never became leader");
        std::thread::yield_now();
    }
    (node, addr)
}

/// Submit `n` distinct payloads, retrying a full ingress (failover.rs pattern).
fn append_some_load(node: &Node, n: u64) {
    for i in 0..n {
        let mut p = vec![0u8; PAYLOAD];
        p[..8].copy_from_slice(&i.to_le_bytes());
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match node.submit(p.clone()) {
                Ok(()) => break,
                Err(uc2_node::SubmitError::Full) => {
                    assert!(Instant::now() < deadline, "ingress stayed full");
                    std::thread::yield_now();
                }
                Err(e) => panic!("submit to serving sole voter: {e:?}"),
            }
        }
    }
}

/// The drain's whole purpose: after it reports `Drained`, the JOURNAL itself
/// holds every appended byte.
///
/// Asserted by reopening the journal, not by restarting the node and reading
/// its counters. A restart cannot discriminate: `log.buf` is file-backed, so a
/// restarted archive picks the un-recorded tail straight back out of the buffer
/// and catches `durable` up within milliseconds whether or not the drain ever
/// ran. `Archive::recovered_position` is what the drain actually moves.
///
/// HONEST LIMIT — this pins the CONTRACT, not a behaviour difference. Mutation
/// (replacing the drain with a bare `stop()`) does NOT turn this test red, at
/// any load tried up to 5000 payloads. `Node::stop` tears the agents down in
/// order and the archive is LAST (`agents: [consensus, sender, receiver,
/// archive]`), so it keeps polling while the other three threads join and
/// clears a backlog of this size on its own. The drain's value is that the
/// catch-up becomes a bounded guarantee that REPORTS what it achieved, instead
/// of an accident of teardown order — which is what Task 8's restart-cost gate
/// measures under real load.
#[test]
fn stop_draining_leaves_durable_caught_up_with_append() {
    let dir = tempfile::Builder::new()
        .prefix("uc2-lifecycle-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let instance_dir = dir.path().join("n0");

    let (node, _addr) = single_node(&instance_dir);
    append_some_load(&node, LOAD);

    let append_before = node.counters().append.load_acquire();
    assert!(append_before > 0, "test must actually append something");

    match node.stop_draining(Duration::from_secs(10)) {
        DrainOutcome::Drained => {}
        other => panic!("expected Drained, got {other:?}"),
    }

    // The node is stopped, so its archive is dropped and the journal is quiet.
    let arch = Archive::open(ArchiveConfig::new(instance_dir.join("journal")))
        .expect("reopen journal");
    let recorded = arch.recovered_position();
    assert!(
        recorded >= append_before,
        "stop_draining reported Drained while the journal ends at {recorded}, short of the \
         {append_before} bytes appended — the drain did not actually drain"
    );
}

/// A drain that cannot finish must still stop the node. A shutdown that hangs
/// is worse than one that costs a replay.
///
/// This is a non-hang guard, not a proof that the deadline is honoured: when
/// the archive happens to be already caught up the call returns `Drained`
/// immediately and the bound is met trivially. It does catch an unbounded
/// wait, which is the failure that matters.
#[test]
fn stop_draining_honours_its_deadline() {
    let dir = tempfile::Builder::new()
        .prefix("uc2-lifecycle-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let (node, _addr) = single_node(&dir.path().join("n0"));
    append_some_load(&node, 512);

    let t0 = Instant::now();
    let outcome = node.stop_draining(Duration::from_nanos(1));
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_secs(30),
        "a 1 ns deadline must not become an unbounded wait (took {elapsed:?}, got {outcome:?})"
    );
}

/// M10 (Task 4) smoke: `Node::observability()` sees a live 4-agent bundle
/// while the node runs, and every agent's flag flips true once the node is
/// stopped (agents are told to stop and their threads join). The Arcs
/// returned by `observability()` are cloned BEFORE `stop()` runs (which
/// consumes the node), so polling them afterwards is genuinely observing the
/// worker threads exit, not a stale snapshot.
#[test]
fn observability_reports_four_agents_alive_then_all_finished_after_stop() {
    let dir = tempfile::Builder::new()
        .prefix("uc2-lifecycle-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir");
    let (node, _addr) = single_node(&dir.path().join("n0"));

    let obs = node.observability();
    assert_eq!(obs.agents.len(), 4, "expected exactly 4 agents: {:?}", {
        obs.agents.iter().map(|(n, _)| *n).collect::<Vec<_>>()
    });
    for (name, flag) in &obs.agents {
        assert!(
            !flag.load(std::sync::atomic::Ordering::Acquire),
            "agent {name} already reports finished on a live node"
        );
    }
    let names: Vec<&'static str> = obs.agents.iter().map(|(n, _)| *n).collect();
    assert_eq!(
        names,
        vec!["consensus", "sender", "receiver", "archive"],
        "agents must be reported in this fixed order regardless of spawn order"
    );

    node.stop();

    let deadline = Instant::now() + Duration::from_secs(10);
    for (name, flag) in &obs.agents {
        while !flag.load(std::sync::atomic::Ordering::Acquire) {
            assert!(Instant::now() < deadline, "agent {name} never reported finished after stop");
            std::thread::yield_now();
        }
    }
}

// ------------------------------------------------------- the uc2-node daemon

/// Write a single-voter config and return its path plus the instance dir.
fn daemon_config(dir: &Path, port: u16, bind: &str, extra: &str) -> (PathBuf, PathBuf) {
    let inst = dir.join("n1");
    std::fs::create_dir_all(&inst).unwrap();
    let cfg = dir.join("node.toml");
    std::fs::write(
        &cfg,
        format!(
            r#"{extra}
id = 1
bind = "{bind}:{port}"
instance_dir = "{}"
app_id = "lifecycle"

[[members]]
id = 1
addr = "127.0.0.1:{port}"
"#,
            inst.display()
        ),
    )
    .unwrap();
    (cfg, inst)
}

fn scratch() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("uc2-daemon-")
        .tempdir_in(env!("CARGO_TARGET_TMPDIR"))
        .expect("tempdir")
}

#[test]
fn daemon_starts_from_a_config_file_and_stops_cleanly_on_sigterm() {
    use std::process::Command;

    let dir = scratch();
    let (cfg, _inst) = daemon_config(dir.path(), 19701, "127.0.0.1", "");

    let mut child = Command::new(env!("CARGO_BIN_EXE_uc2-node"))
        .arg("--config")
        .arg(&cfg)
        .spawn()
        .unwrap();

    // Give it time to elect itself in a one-node cluster.
    std::thread::sleep(Duration::from_millis(1500));

    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };

    let start = Instant::now();
    let status = child.wait().unwrap();
    let elapsed = start.elapsed();

    assert!(status.success(), "clean shutdown must exit 0, got {status:?}");
    assert!(elapsed < Duration::from_secs(1), "SIGTERM to exit took {elapsed:?}, bar is < 1s");
}

#[test]
fn daemon_refuses_a_config_with_a_bind_mismatch() {
    use std::process::Command;

    let dir = scratch();
    let (cfg, _inst) = daemon_config(dir.path(), 19702, "0.0.0.0", "");

    let out = Command::new(env!("CARGO_BIN_EXE_uc2-node"))
        .arg("--config")
        .arg(&cfg)
        .output()
        .unwrap();
    assert!(!out.status.success(), "must refuse to start");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("bind"), "refusal must name the field, got: {err}");
    // Exit 2 specifically means "this config is bad and will be bad next time".
    // packaging/systemd/uc2-node.service keys RestartPreventExitStatus on it, so
    // the code is a contract with the shipped unit, not an implementation
    // detail: drifting it to 1 turns a loud refusal back into a restart loop.
    assert_eq!(out.status.code(), Some(2), "a config refusal must exit 2, got {:?}", out.status);
}

/// A RAM-backed instance_dir is refused by default, and the override starts the
/// node while WARNING every boot. Without this the override channel is only
/// unit-tested; this is the assertion that the daemon actually prints it, which
/// is the half `preflight` deliberately does not own.
#[test]
fn daemon_refuses_a_volatile_instance_dir_then_warns_when_overridden() {
    use std::process::Command;

    // /proc/mounts, not the code under test — see preflight's volatile_dir.
    let Some(vol) = std::fs::read_to_string("/proc/mounts").ok().and_then(|m| {
        m.lines().find_map(|l| {
            let mut f = l.split_whitespace();
            match (f.next(), f.next(), f.next()) {
                (Some(_), Some(mp), Some("tmpfs")) if mp == "/dev/shm" || mp == "/tmp" => {
                    Some(PathBuf::from(mp))
                }
                _ => None,
            }
        })
    }) else {
        eprintln!("no RAM-backed fs available; skipping");
        return;
    };

    let inst = vol.join(format!("uc2-volatile-probe-{}", std::process::id()));
    std::fs::create_dir_all(&inst).unwrap();
    let dir = scratch();
    let write_cfg = |extra: &str| {
        let cfg = dir.path().join(format!("node{}.toml", extra.len()));
        std::fs::write(
            &cfg,
            format!(
                "{extra}\nid = 1\nbind = \"127.0.0.1:19703\"\ninstance_dir = \"{}\"\n\
                 app_id = \"lifecycle\"\n\n[[members]]\nid = 1\naddr = \"127.0.0.1:19703\"\n",
                inst.display()
            ),
        )
        .unwrap();
        cfg
    };

    let out = Command::new(env!("CARGO_BIN_EXE_uc2-node"))
        .arg("--config")
        .arg(write_cfg(""))
        .output()
        .unwrap();
    assert!(!out.status.success(), "a RAM-backed instance_dir must be refused by default");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("RAM-backed"), "refusal must say why, got: {err}");

    // Overridden: it starts, so SIGTERM it — but it must have warned first.
    let child = Command::new(env!("CARGO_BIN_EXE_uc2-node"))
        .arg("--config")
        .arg(write_cfg("allow_volatile_fs = true"))
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(800));
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    let out = child.wait_with_output().unwrap();
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "the override must let the node start and stop cleanly");
    assert!(
        err.contains("WARNING") && err.contains("OVERRIDDEN"),
        "the override must never be silent, got stderr: {err}"
    );

    let _ = std::fs::remove_dir_all(&inst);
}

/// `[log]`/`[metrics]` now have an M10 schema: the daemon must START with
/// them present, and it must no longer print the old M9 "RESERVED ... NO
/// effect" notice — the sections act now (Task 7 wires the endpoint itself;
/// this only pins that the M9-era placeholder notice is gone).
///
/// Note the sections are appended AFTER `[[members]]`, not prepended: a table
/// header at the top of the document would capture `id`/`bind`/`app_id` into
/// it. That is also why this test does not reuse `daemon_config`'s `extra`,
/// which prepends.
#[test]
fn a_daemon_with_metrics_configured_no_longer_prints_the_reserved_notice() {
    use std::process::Command;

    let dir = scratch();
    let inst = dir.path().join("n1");
    std::fs::create_dir_all(&inst).unwrap();
    let cfg = dir.path().join("node.toml");
    std::fs::write(
        &cfg,
        format!(
            r#"id = 1
bind = "127.0.0.1:19704"
instance_dir = "{}"
app_id = "lifecycle"

[[members]]
id = 1
addr = "127.0.0.1:19704"

[log]
level = "info"

[metrics]
bind = "127.0.0.1:19605"
"#,
            inst.display()
        ),
    )
    .unwrap();

    // No assertion between spawn and kill — a leaked busy-spin node burns a
    // core and blocks cargo on the inherited pipe (see the M9 plan, Ruling 11).
    let child = Command::new(env!("CARGO_BIN_EXE_uc2-node"))
        .arg("--config")
        .arg(&cfg)
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(800));
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    let out = child.wait_with_output().unwrap();
    let err = String::from_utf8_lossy(&out.stderr);

    assert!(
        out.status.success(),
        "a config carrying [log]/[metrics] must start and stop cleanly, stderr: {err}"
    );
    assert!(
        !err.contains("RESERVED"),
        "the M9 placeholder notice must be gone now that the sections have a schema, got stderr: {err}"
    );
}

// --------------------------------------------------------- M10 Task 7: obs wiring

/// Bind `127.0.0.1:0`, note the port, and drop the listener — a "probably
/// free" port for a config file that must name a concrete address (an OS
/// process, unlike an in-process `TcpListener::bind(":0")`, cannot report
/// the port it landed on back into a file another process reads at spawn
/// time). The race window between drop and the daemon's own bind is
/// accepted in-suite, matching the brief.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").expect("bind ephemeral").local_addr().unwrap().port()
}

/// A minimal blocking HTTP/1.1 GET: write the request line, half-close the
/// write side, read the response to EOF (the server sends `Connection:
/// close`, so EOF marks the end), and split off the status code.
fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect to obs endpoint");
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
        .expect("write request");
    stream.shutdown(std::net::Shutdown::Write).ok();
    let mut body = String::new();
    stream.read_to_string(&mut body).expect("read response");
    let status = body
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0);
    (status, body)
}

/// Write a config with a `[metrics]` section AFTER `[[members]]` (a table
/// header any earlier would swallow `id`/`bind`/`app_id` into it — see the
/// comment on the sibling test above).
fn daemon_config_with_metrics(dir: &Path, port: u16, metrics_port: u16) -> (PathBuf, PathBuf) {
    let inst = dir.join("n1");
    std::fs::create_dir_all(&inst).unwrap();
    let cfg = dir.join("node.toml");
    std::fs::write(
        &cfg,
        format!(
            r#"id = 1
bind = "127.0.0.1:{port}"
instance_dir = "{}"
app_id = "lifecycle"

[[members]]
id = 1
addr = "127.0.0.1:{port}"

[metrics]
bind = "127.0.0.1:{metrics_port}"
"#,
            inst.display()
        ),
    )
    .unwrap();
    (cfg, inst)
}

/// Spawn the daemon with stdout piped, collecting lines into a shared `Vec`
/// on a background reader thread so the test can poll for a banner while the
/// process keeps running (the pipe would otherwise fill and stall the child
/// once its buffer is full).
fn spawn_daemon_capturing_stdout(cfg: &Path) -> (std::process::Child, Arc<Mutex<Vec<String>>>) {
    use std::process::Command;

    let mut child = Command::new(env!("CARGO_BIN_EXE_uc2-node"))
        .arg("--config")
        .arg(cfg)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let lines = Arc::new(Mutex::new(Vec::new()));
    let lines_writer = Arc::clone(&lines);
    std::thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines().map_while(Result::ok) {
            lines_writer.lock().unwrap().push(line);
        }
    });
    (child, lines)
}

fn wait_for_line(lines: &Mutex<Vec<String>>, needle: &str, deadline: Instant) -> Option<String> {
    loop {
        if let Some(found) = lines.lock().unwrap().iter().find(|l| l.contains(needle)).cloned() {
            return Some(found);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::yield_now();
    }
}

#[test]
fn the_daemon_serves_metrics_when_configured_and_stops_cleanly() {
    let dir = scratch();
    let metrics_port = free_port();
    let (cfg, _inst) = daemon_config_with_metrics(dir.path(), 19710, metrics_port);
    let metrics_addr: SocketAddr = format!("127.0.0.1:{metrics_port}").parse().unwrap();

    let (mut child, lines) = spawn_daemon_capturing_stdout(&cfg);

    let deadline = Instant::now() + Duration::from_secs(10);
    let banner = wait_for_line(&lines, "observability endpoint on http://", deadline)
        .expect("banner line naming the observability endpoint never appeared");
    assert!(
        banner.contains(&format!("http://127.0.0.1:{metrics_port}/metrics")),
        "banner must name the configured addr, got: {banner}"
    );

    let (status, body) = http_get(metrics_addr, "/metrics");
    assert_eq!(status, 200, "GET /metrics status, body: {body}");
    assert!(body.contains("uc2_commit_bytes"), "metrics body missing uc2_commit_bytes: {body}");

    let (status, _) = http_get(metrics_addr, "/healthz");
    assert_eq!(status, 200, "GET /healthz status");

    let start = Instant::now();
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    let status = child.wait().unwrap();
    let elapsed = start.elapsed();
    assert!(status.success(), "clean shutdown must exit 0, got {status:?}");
    assert!(elapsed < Duration::from_secs(6), "SIGTERM to exit took {elapsed:?}, over the drain budget");

    assert!(
        TcpStream::connect(metrics_addr).is_err(),
        "the observability port must be closed after the daemon stops"
    );
}

/// M11 (Task 5): the daemon's ~1s derived-events pass `statvfs`s the
/// instance dir and publishes `free_disk_bytes` — a metrics-enabled daemon's
/// scrape must contain `uc2_free_disk_bytes` with a plausible (>0) value.
/// Only the daemon loop writes this field (none of the four polling agents
/// gain a syscall for it), so this has to be a real spawned-process test,
/// not an in-process `ObsSources` fixture.
#[test]
fn the_daemon_publishes_free_disk_bytes() {
    let dir = scratch();
    let metrics_port = free_port();
    let (cfg, _inst) = daemon_config_with_metrics(dir.path(), 19712, metrics_port);
    let metrics_addr: SocketAddr = format!("127.0.0.1:{metrics_port}").parse().unwrap();

    let (mut child, lines) = spawn_daemon_capturing_stdout(&cfg);
    let deadline = Instant::now() + Duration::from_secs(10);
    wait_for_line(&lines, "observability endpoint on http://", deadline)
        .expect("banner line naming the observability endpoint never appeared");

    // The derived-events pass runs every 10th 100ms tick (~1s) — wait past at
    // least one so free_disk_bytes has actually been stored, not just left
    // at the boot-time zero sentinel.
    let deadline = Instant::now() + Duration::from_secs(10);
    let value = loop {
        let (status, body) = http_get(metrics_addr, "/metrics");
        assert_eq!(status, 200, "GET /metrics status, body: {body}");
        if let Some(v) = body
            .lines()
            .find_map(|l| l.strip_prefix("uc2_free_disk_bytes "))
            .and_then(|v| v.trim().parse::<u64>().ok())
        {
            break v;
        }
        assert!(Instant::now() < deadline, "uc2_free_disk_bytes never appeared in a scrape");
        std::thread::sleep(Duration::from_millis(100));
    };
    assert!(value > 0, "uc2_free_disk_bytes must be a plausible >0 reading, got {value}");

    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    let status = child.wait().unwrap();
    assert!(status.success(), "clean shutdown must exit 0, got {status:?}");
}

#[test]
fn the_daemon_without_a_metrics_section_opens_no_port() {
    let dir = scratch();
    let (cfg, _inst) = daemon_config(dir.path(), 19711, "127.0.0.1", "");

    let (mut child, lines) = spawn_daemon_capturing_stdout(&cfg);
    std::thread::sleep(Duration::from_millis(1500));

    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    let status = child.wait().unwrap();
    assert!(status.success(), "clean shutdown must exit 0, got {status:?}");

    let captured = lines.lock().unwrap();
    assert!(
        !captured.iter().any(|l| l.contains("observability endpoint")),
        "no [metrics] section must mean no observability banner, got: {captured:?}"
    );
}
