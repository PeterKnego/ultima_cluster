// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M11 gate: **survivable cluster** (see
//! `docs/superpowers/specs/2026-08-19-uc2-production-readiness-design.md` §6;
//! plan `docs/superpowers/plans/2026-08-20-uc2-m11-survivable-cluster.md`).
//!
//! The pre-committed bar lives in `docs/benchmarks/uc2-m11-gate-2026-08-20.md`
//! (restated in [`BAR`] below). Six rows: 1 backup/restore CI, 2 quorum-loss
//! e2e (adjudicated from the SAME three suite runs as row 1 — the controller
//! ruling's split), 3a ENOSPC sudo-free `EACCES` smoke, 3b genuine-`ENOSPC`
//! fixture (`SKIPPED-PENDING` unless `UC2_ENOSPC_DIR` is set), 4 disk-low
//! observability, 5 flag-day downtime (FLEET ONLY — printed here as an
//! ungated local SMOKE via `flagday-smoke`).
//!
//! Subcommands: `survival` (row 1, its own 3x survival-suite run + the
//! straddle anti-vacuity test), `quorum` (row 2, its own 3x run), `enospc`
//! (rows 3a/3b), `alerts` (row 4), `flagday-smoke` (ungated local smoke of
//! row 5's script), `all` (every gated row in one process, sharing ONE 3x
//! survival-suite run between rows 1 and 2 to hold the runtime budget — see
//! the module's own doc for why re-running it twice would not fit — bar
//! printed first, per-row verdicts printed as they land, `RESULT:
//! PASS|FAIL`, exit 0/1).
//!
//! Real subprocesses throughout: `cargo test`/`cargo build` shell-outs for
//! rows 1-3, the real `uc2-node`/`uc2ctl` daemon/CLI binaries (built on
//! demand — see `examples/uc_crashtest/tests/enospc.rs`'s doc for why
//! `CARGO_BIN_EXE_*` doesn't reach an `examples/` binary here either, since
//! this file lives in the SAME package as `uc2-node` but is still an
//! `examples/` target) for row 4's live scrape and row 5's smoke, and
//! `scripts/m10_alert_fire.sh`/`scripts/uc2_flag_day.sh` shelled out to
//! directly. Every tempdir/scratch path lives on real disk under this
//! binary's own target dir (never `/tmp` — RAM tmpfs, no swap on this box).
//! Row 4's completeness-cross-check anti-vacuity probe runs against a
//! **scratch copy** of `scripts/m10_alert_fire.sh` — see
//! [`check_yaml_builders_anti_vacuity`]'s doc comment — the tracked script is
//! never opened for writing by this binary, at any point, so a signal to
//! this process can never leave it mutated.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};

use uc_log::cnc::CncPage;
use uc_protocol::v2::cnc::{NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER};

/// The pre-committed bar, restated from
/// `docs/benchmarks/uc2-m11-gate-2026-08-20.md`.
const BAR: &[(&str, &str)] = &[
    (
        "1",
        "backup/restore CI: Task 3 crashtest green 3/3 consecutive \
         (a_follower_backed_up_under_load_restores_onto_a_new_host_and_converges), plus the \
         straddle anti-vacuity test green — LOCAL, GATED",
    ),
    (
        "2",
        "quorum-loss e2e: a_survivor_forced_single_after_quorum_loss_recovers_and_repairs green \
         3/3, adjudicated from the SAME three suite runs as row 1 — LOCAL, GATED",
    ),
    (
        "3a",
        "ENOSPC sudo-free EACCES smoke: write_denied_drives_the_same_failstop_chain green — \
         LOCAL, GATED, unconditional",
    ),
    (
        "3b",
        "ENOSPC genuine fixture: enospc_fails_stops_asserted_and_the_cluster_survives green — \
         CI (sudo survival job) or a user-assisted local fixture run; SKIPPED-PENDING (not a \
         pass) when UC2_ENOSPC_DIR is unset; `all` FAILS a pending 3b unless \
         --allow-pending-enospc is passed",
    ),
    (
        "4",
        "disk-low observability: uc2_free_disk_bytes present in a live daemon's real /metrics \
         scrape; scripts/m10_alert_fire.sh 14/14 incl. Uc2DiskLow, exit 0; the YAML<->builders \
         completeness cross-check trips (named FAIL) when a builder is deliberately removed, then \
         the script is restored and re-verified green — LOCAL, GATED",
    ),
    (
        "5",
        "flag-day downtime: stop -> verify-equal-durable -> upgrade -> start -> verify-serving on \
         a real fleet, downtime <= 60s, zero acked loss — FLEET ONLY; local run is SMOKE, ungated",
    ),
];

fn print_bar() {
    println!("== uc2 M11 gate bar (pre-committed docs/benchmarks/uc2-m11-gate-2026-08-20.md) ==");
    for (n, desc) in BAR {
        println!("  [{n}] {desc}");
    }
}

// ------------------------------------------------------------------ verdicts

enum RowStatus {
    Pass,
    Fail,
    SkippedPending,
}

struct Verdict {
    row: &'static str,
    status: RowStatus,
    detail: String,
}

impl Verdict {
    fn new_pf(row: &'static str, pass: bool, detail: String) -> Verdict {
        Verdict { row, status: if pass { RowStatus::Pass } else { RowStatus::Fail }, detail }
    }

    fn label(&self) -> &'static str {
        match self.status {
            RowStatus::Pass => "PASS",
            RowStatus::Fail => "FAIL",
            RowStatus::SkippedPending => "SKIPPED-PENDING",
        }
    }

    /// Whether this row's status is acceptable for `all`/a standalone
    /// subcommand's own exit code. `SkippedPending` is acceptable ONLY with
    /// `--allow-pending-enospc` — see the controller ruling in the gate doc.
    fn gate_ok(&self, allow_pending: bool) -> bool {
        match self.status {
            RowStatus::Pass => true,
            RowStatus::Fail => false,
            RowStatus::SkippedPending => allow_pending,
        }
    }
}

fn print_verdict(v: &Verdict) {
    println!("  [{}] {} — {}", v.label(), v.row, v.detail);
}

fn report_rows(vs: &[Verdict]) {
    println!();
    for v in vs {
        print_verdict(v);
    }
    finish(vs.iter().all(|v| v.gate_ok(false)));
}

fn finish(ok: bool) {
    if ok {
        println!("RESULT: PASS");
    } else {
        println!("RESULT: FAIL (honest)");
        std::process::exit(1);
    }
}

// ------------------------------------------------------------------ CLI

#[derive(Parser)]
#[command(name = "m11_gate", about = "UC v2 M11 gate: survivable cluster")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Row 1: backup/restore CI, 3x suite run + the straddle anti-vacuity test.
    Survival,
    /// Row 2: quorum-loss e2e, its own 3x suite run.
    Quorum,
    /// Rows 3a/3b: ENOSPC sudo-free smoke + the genuine fixture (env-gated).
    Enospc(PendingEnospcArgs),
    /// Row 4: disk-low observability.
    Alerts,
    /// Row 5, LOCAL SMOKE ONLY, ungated: scripts/uc2_flag_day.sh --local.
    #[command(name = "flagday-smoke")]
    FlagdaySmoke,
    /// Every gated row: bar first, per-row verdicts as they land, RESULT + exit 0/1.
    All(PendingEnospcArgs),
}

#[derive(clap::Args)]
struct PendingEnospcArgs {
    /// Treat a pending row 3b (UC2_ENOSPC_DIR unset -> SKIPPED-PENDING) as
    /// acceptable for this run's own PASS/FAIL. Per the controller ruling in
    /// the gate doc, the RECORDED gate run may use this flag only alongside
    /// CI/user-assisted fixture evidence noted in that document.
    #[arg(long)]
    allow_pending_enospc: bool,
}

fn main() {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Survival => {
            print_bar();
            let runs = run_survival_suite_x3();
            let (straddle_pass, straddle_detail) = run_backup_straddle_test();
            let v = row1_verdict(&runs, straddle_pass, &straddle_detail);
            report_rows(&[v]);
        }
        Cmd::Quorum => {
            print_bar();
            let runs = run_survival_suite_x3();
            let v = row2_verdict(&runs);
            report_rows(&[v]);
        }
        Cmd::Enospc(a) => {
            print_bar();
            let v3a = run_enospc_3a();
            let v3b = run_enospc_3b();
            report_rows_with_flag(&[v3a, v3b], a.allow_pending_enospc);
        }
        Cmd::Alerts => {
            print_bar();
            let root = default_root();
            let v = run_alerts_row(&root);
            report_rows(&[v]);
        }
        Cmd::FlagdaySmoke => {
            let root = default_root();
            run_flagday_smoke(&root);
        }
        Cmd::All(a) => {
            print_bar();
            println!();
            let root = default_root();

            let runs = run_survival_suite_x3();
            let (straddle_pass, straddle_detail) = run_backup_straddle_test();
            let v1 = row1_verdict(&runs, straddle_pass, &straddle_detail);
            print_verdict(&v1);
            let v2 = row2_verdict(&runs);
            print_verdict(&v2);

            let v3a = run_enospc_3a();
            print_verdict(&v3a);
            let v3b = run_enospc_3b();
            print_verdict(&v3b);

            let v4 = run_alerts_row(&root);
            print_verdict(&v4);

            run_flagday_smoke(&root); // ungated — prints its own SMOKE line, never gates.

            println!("\n== M11 gate results ==");
            for v in [&v1, &v2, &v3a, &v3b, &v4] {
                print_verdict(v);
            }
            println!(
                "  [INFO] 5 flag-day downtime — fleet-only bar; printed above as LOCAL SMOKE, ungated"
            );

            let all_ok = v1.gate_ok(false)
                && v2.gate_ok(false)
                && v3a.gate_ok(false)
                && v3b.gate_ok(a.allow_pending_enospc)
                && v4.gate_ok(false);
            if all_ok {
                println!(
                    "RESULT: PASS (local/CI rows); row 5 is fleet-only, separate, user-approved"
                );
            } else {
                println!("RESULT: FAIL (honest)");
            }
            std::process::exit(if all_ok { 0 } else { 1 });
        }
    }
}

fn report_rows_with_flag(vs: &[Verdict], allow_pending: bool) {
    println!();
    for v in vs {
        print_verdict(v);
    }
    finish(vs.iter().all(|v| v.gate_ok(allow_pending)));
}

// =========================================================== rows 1 + 2

const BACKUP_TEST: &str = "a_follower_backed_up_under_load_restores_onto_a_new_host_and_converges";
const QUORUM_TEST: &str = "a_survivor_forced_single_after_quorum_loss_recovers_and_repairs";
const STRADDLE_TEST: &str = "a_wrong_order_copy_across_a_purge_is_detected_as_a_hole";
const EACCES_TEST: &str = "write_denied_drives_the_same_failstop_chain";
const ENOSPC_TEST: &str = "enospc_fails_stops_asserted_and_the_cluster_survives";

struct SuiteRun {
    exit_ok: bool,
    backup_pass: Option<bool>,
    quorum_pass: Option<bool>,
    tail: String,
}

/// Parse `cargo test`'s own per-test result line (`test <name> ... ok` or
/// `test <name> ... FAILED`) out of combined stdout+stderr. `None` if the
/// named test never appears at all (e.g. a compile failure upstream of any
/// test running).
fn parse_test_line(text: &str, name: &str) -> Option<bool> {
    for line in text.lines() {
        let l = line.trim();
        if let Some(rest) = l.strip_prefix("test ")
            && let Some(idx) = rest.find(" ... ")
        {
            let tname = &rest[..idx];
            if tname == name {
                let status = rest[idx + 5..].trim();
                return Some(status == "ok");
            }
        }
    }
    None
}

fn tail_str(s: &str, n: usize) -> String {
    if s.len() <= n {
        return s.to_string();
    }
    let mut start = s.len() - n;
    while !s.is_char_boundary(start) {
        start += 1;
    }
    format!("...(truncated)...\n{}", &s[start..])
}

fn cargo_test(args: &[&str]) -> (bool, String) {
    let out = Command::new(env!("CARGO"))
        .args(args)
        .current_dir(repo_root())
        .output()
        .unwrap_or_else(|e| panic!("spawn cargo {}: {e}", args.join(" ")));
    let combined =
        format!("{}\n{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    (out.status.success(), combined)
}

fn run_survival_suite_once(run_n: usize) -> SuiteRun {
    println!(
        "  [survival suite] run {run_n}/3: cargo test -p uc_crashtest --features \
         survival-tests --test survival -- --test-threads=1 ..."
    );
    let (exit_ok, combined) = cargo_test(&[
        "test",
        "-p",
        "uc_crashtest",
        "--features",
        "survival-tests",
        "--test",
        "survival",
        "--",
        "--test-threads=1",
    ]);
    let backup_pass = parse_test_line(&combined, BACKUP_TEST);
    let quorum_pass = parse_test_line(&combined, QUORUM_TEST);
    println!(
        "  [survival suite] run {run_n}/3: exit_ok={exit_ok} backup={backup_pass:?} \
         quorum={quorum_pass:?}"
    );
    if backup_pass != Some(true) || quorum_pass != Some(true) {
        println!("{}", tail_str(&combined, 6000));
    }
    SuiteRun { exit_ok, backup_pass, quorum_pass, tail: tail_str(&combined, 6000) }
}

fn run_survival_suite_x3() -> Vec<SuiteRun> {
    (1..=3).map(run_survival_suite_once).collect()
}

fn run_backup_straddle_test() -> (bool, String) {
    println!("  [row 1] straddle anti-vacuity: cargo test -p uc_node --test backup a_wrong_order ...");
    let (exit_ok, combined) = cargo_test(&["test", "-p", "uc_node", "--test", "backup", "a_wrong_order"]);
    let test_pass = parse_test_line(&combined, STRADDLE_TEST);
    let pass = exit_ok && test_pass == Some(true);
    if !pass {
        println!("{}", tail_str(&combined, 4000));
    }
    (pass, format!("exit_ok={exit_ok} test_result={test_pass:?}"))
}

fn row1_verdict(runs: &[SuiteRun], straddle_pass: bool, straddle_detail: &str) -> Verdict {
    let backups: Vec<Option<bool>> = runs.iter().map(|r| r.backup_pass).collect();
    let all_backup_pass = backups.iter().all(|b| *b == Some(true));
    let pass = all_backup_pass && straddle_pass;
    let detail = format!(
        "backup-under-load->restore->rejoin 3/3: {backups:?}; straddle anti-vacuity: \
         pass={straddle_pass} ({straddle_detail})"
    );
    if !pass {
        for (i, r) in runs.iter().enumerate() {
            if r.backup_pass != Some(true) {
                println!("  [row 1] run {}/3 exit_ok={} tail:\n{}", i + 1, r.exit_ok, r.tail);
            }
        }
    }
    Verdict::new_pf("1 backup/restore CI", pass, detail)
}

fn row2_verdict(runs: &[SuiteRun]) -> Verdict {
    let quorums: Vec<Option<bool>> = runs.iter().map(|r| r.quorum_pass).collect();
    let pass = quorums.iter().all(|q| *q == Some(true));
    let detail = format!("quorum-loss e2e 3/3 (same suite runs as row 1): {quorums:?}");
    if !pass {
        for (i, r) in runs.iter().enumerate() {
            if r.quorum_pass != Some(true) {
                println!("  [row 2] run {}/3 exit_ok={} tail:\n{}", i + 1, r.exit_ok, r.tail);
            }
        }
    }
    Verdict::new_pf("2 quorum-loss e2e", pass, detail)
}

// =========================================================== rows 3a / 3b

fn run_enospc_3a() -> Verdict {
    println!("== row 3a: ENOSPC sudo-free EACCES smoke ==");
    let (exit_ok, combined) =
        cargo_test(&["test", "-p", "uc_crashtest", "--features", "enospc-tests", "write_denied"]);
    let test_pass = parse_test_line(&combined, EACCES_TEST);
    let pass = exit_ok && test_pass == Some(true);
    if !pass {
        println!("{}", tail_str(&combined, 4000));
    }
    Verdict::new_pf(
        "3a ENOSPC sudo-free EACCES smoke",
        pass,
        format!("exit_ok={exit_ok} test_result={test_pass:?}"),
    )
}

fn run_enospc_3b() -> Verdict {
    println!("== row 3b: ENOSPC genuine fixture ==");
    match std::env::var("UC2_ENOSPC_DIR") {
        Err(_) => {
            let msg = "UC2_ENOSPC_DIR is not set — row 3b needs a pre-mounted, pre-ballasted \
                loopback ext4 filesystem. Create one (needs sudo): \
                `scripts/enospc_fixture.sh create <dir> <size-mb>`, then re-run this gate with \
                UC2_ENOSPC_DIR=<dir> set; destroy it after with \
                `scripts/enospc_fixture.sh destroy <dir>`. In CI this runs unconditionally under \
                nightly.yml's sudo-enabled `survival` job."
                .to_string();
            println!("  SKIPPED-PENDING: {msg}");
            Verdict { row: "3b ENOSPC genuine fixture", status: RowStatus::SkippedPending, detail: msg }
        }
        Ok(dir) => {
            let (exit_ok, combined) = cargo_test(&[
                "test",
                "-p",
                "uc_crashtest",
                "--features",
                "enospc-tests",
                "--test",
                "enospc",
                "--",
                ENOSPC_TEST,
            ]);
            let test_pass = parse_test_line(&combined, ENOSPC_TEST);
            let pass = exit_ok && test_pass == Some(true);
            if !pass {
                println!("{}", tail_str(&combined, 4000));
            }
            Verdict::new_pf(
                "3b ENOSPC genuine fixture",
                pass,
                format!("UC2_ENOSPC_DIR={dir} exit_ok={exit_ok} test_result={test_pass:?}"),
            )
        }
    }
}

// =========================================================== row 4: alerts

/// Boundary-matched family presence — same rationale as `uc_node/src/obs/metrics.rs`'s
/// `every_contract_series_is_present` test (a bare `contains` would false-positive on
/// a name that's a prefix of another).
fn series_present(text: &str, name: &str) -> bool {
    text.contains(&format!("\n{name} ")) || text.contains(&format!("\n{name}{{"))
}

fn try_scrape(addr: SocketAddr) -> Option<String> {
    let mut stream = TcpStream::connect(addr).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(3))).ok()?;
    write!(stream, "GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").ok()?;
    stream.flush().ok()?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).ok()?;
    let text = String::from_utf8_lossy(&raw).to_string();
    let mut halves = text.splitn(2, "\r\n\r\n");
    let _head = halves.next().unwrap_or("");
    Some(halves.next().unwrap_or("").to_string())
}

/// Drain a piped child stream on a background thread so a full pipe can never
/// deadlock the child (same concern `examples/uc_crashtest/tests/enospc.rs`'s
/// `capture_stderr` names) — this row doesn't need the daemon's own log text.
fn drain_in_background<R: Read + Send + 'static>(mut r: R) {
    std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        while let Ok(n) = r.read(&mut buf) {
            if n == 0 {
                break;
            }
        }
    });
}

fn write_alerts_node_toml(cfg_path: &Path, bind: SocketAddr, instance_dir: &Path, metrics_bind: SocketAddr) {
    let body = format!(
        "id = 0\n\
         bind = \"{bind}\"\n\
         instance_dir = \"{}\"\n\
         app_id = \"m11-gate-alerts\"\n\
         buffer_bytes = {}\n\
         journal_segment_bytes = {}\n\
         \n\
         [[members]]\n\
         id = 0\n\
         addr = \"{bind}\"\n\
         \n\
         [metrics]\n\
         bind = \"{metrics_bind}\"\n",
        instance_dir.display(),
        1u64 << 20,
        1u64 << 20,
    );
    std::fs::write(cfg_path, body).expect("write alerts-row node.toml");
}

/// Live-daemon half of row 4: boot a real `uc2-node` daemon with `[metrics]`
/// on, scrape its real `/metrics` over real HTTP, and confirm
/// `uc2_free_disk_bytes` actually renders (the daemon's own outer loop is
/// the only writer — see `uc_node/src/bin/uc2-node.rs`'s `free_disk_bytes`
/// doc — so this is genuine evidence the wiring works end to end, not a
/// synthetic `ObsSources` field).
fn check_live_free_disk_bytes_scrape(root: &Path) -> (bool, String) {
    let scratch = root.join("alerts-daemon");
    let _ = std::fs::remove_dir_all(&scratch);
    let instance_dir = scratch.join("n0");
    std::fs::create_dir_all(&instance_dir).expect("create alerts-row instance dir");

    let bind = free_udp_addr();
    let metrics_bind = free_tcp_addr();
    let cfg_path = scratch.join("n0.toml");
    write_alerts_node_toml(&cfg_path, bind, &instance_dir, metrics_bind);

    let bin = uc_node_debug_bin();
    let mut child = Command::new(&bin)
        .arg("--config")
        .arg(&cfg_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn uc2-node daemon for the alerts row: {e}"));
    if let Some(so) = child.stdout.take() {
        drain_in_background(so);
    }
    if let Some(se) = child.stderr.take() {
        drain_in_background(se);
    }

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut found = false;
    let mut sample_line = String::new();
    while Instant::now() < deadline {
        if let Some(body) = try_scrape(metrics_bind)
            && series_present(&body, "uc2_free_disk_bytes")
        {
            found = true;
            sample_line = body
                .lines()
                .find(|l| l.starts_with("uc2_free_disk_bytes"))
                .unwrap_or("")
                .to_string();
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    let _ = child.kill();
    let _ = child.wait();
    (found, format!("uc2_free_disk_bytes present={found} ({sample_line})"))
}

/// Run the real, unmutated `scripts/m10_alert_fire.sh`; count `PASS rule=`
/// lines and check its own exit code (row 4's bar: 14/14 including
/// `Uc2DiskLow`, exit 0).
fn run_alert_fire_script() -> (bool, usize, String) {
    let script = repo_root().join("scripts/m10_alert_fire.sh");
    let out = Command::new("bash")
        .arg(&script)
        .current_dir(repo_root())
        .output()
        .unwrap_or_else(|e| panic!("spawn scripts/m10_alert_fire.sh: {e}"));
    let combined =
        format!("{}\n{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    let pass_count = combined.lines().filter(|l| l.starts_with("PASS rule=")).count();
    (out.status.success(), pass_count, combined)
}

/// Anti-vacuity: run the completeness cross-check on a **scratch copy** of
/// `scripts/m10_alert_fire.sh` with one `RULE_BUILDERS` entry removed — the
/// tracked file at `scripts/m10_alert_fire.sh` is never opened for writing
/// anywhere in this function, at any point, so there is no window (however
/// short) in which a `SIGTERM`/`SIGKILL` to this gate process could leave it
/// mutated. This replaces an earlier design (fix round 1) that mutated the
/// real file in place and relied on a `Drop` guard to restore it — verified
/// broken: `Drop` does not run on an unhandled signal (the default
/// disposition kills the process immediately), and the mutated-file window
/// was the ENTIRE ~80s `m10_alert_fire.sh` invocation below, not a couple of
/// seconds — a `SIGTERM` landing anywhere in that window left the tracked
/// script corrupted in the working tree. See the gate doc's dated amendment
/// for the full incident note and the bar-wording correction that goes with
/// it.
///
/// The scratch copy is made genuinely equivalent to the real script by
/// hardcoding its `ROOT=` line to this process's own `repo_root()` (an
/// absolute path) instead of the original `dirname($0)/..` derivation —
/// verified empirically (see the fix-round commit) that this alone is
/// sufficient: `$ROOT/packaging/prometheus/uc2-alerts.yml` and the `cargo
/// run --manifest-path $ROOT/Cargo.toml` invocation both resolve to the REAL
/// repo exactly as they would from an in-place run, no symlinks needed
/// (`ROOT` literally IS the real repo root — the copy's own location on disk
/// is otherwise irrelevant, since nothing else in the script derives a path
/// from `$0`).
fn check_yaml_builders_anti_vacuity(root: &Path) -> (bool, String) {
    let script_path = repo_root().join("scripts/m10_alert_fire.sh");
    let original = std::fs::read_to_string(&script_path).expect("read m10_alert_fire.sh");

    // Tripwire (kept from the prior design, per the review's note that it is
    // a real partial mitigation): if the tracked file were ever left in a
    // corrupted state by some OTHER means (e.g. a stale mutation from an
    // older build of this gate, or a hand edit), this fails loudly here
    // instead of silently building a scratch copy from the wrong baseline.
    let builder_needle = "    \"Uc2DiskLow\": build_Uc2DiskLow,\n";
    assert!(
        original.contains(builder_needle),
        "m10_alert_fire.sh no longer contains the expected RULE_BUILDERS entry for Uc2DiskLow \
         (or the tracked file is unexpectedly not pristine -- check `git status`/`git diff \
         scripts/m10_alert_fire.sh` before continuing) -- this anti-vacuity probe needs the \
         real script's current shape to build a faithful scratch copy"
    );
    let root_needle = "ROOT=\"$(cd \"$(dirname \"$0\")/..\" && pwd)\"\n";
    assert!(
        original.contains(root_needle),
        "m10_alert_fire.sh's ROOT= line has changed shape -- this scratch-copy probe needs updating"
    );

    let repo = repo_root();
    let repo_str = repo.display().to_string();
    assert!(!repo_str.contains('"'), "repo root path must not contain a double quote");
    let mutated = original
        .replacen(root_needle, &format!("ROOT=\"{repo_str}\"\n"), 1)
        .replacen(builder_needle, "", 1);
    assert_ne!(mutated, original, "the mutation must actually change the scratch copy's text");

    let scratch = root.join("alerts-anti-vacuity");
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("create anti-vacuity scratch dir");
    let scratch_script = scratch.join("m10_alert_fire_mutated.sh");
    std::fs::write(&scratch_script, &mutated).expect("write the mutated scratch copy");

    // `scripts/m10_alert_fire.sh` is never touched from here on — only the
    // scratch copy is executed.
    let out = Command::new("bash")
        .arg(&scratch_script)
        .output()
        .unwrap_or_else(|e| panic!("spawn the mutated scratch copy of m10_alert_fire.sh: {e}"));
    let combined =
        format!("{}\n{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    let tripped = !out.status.success()
        && combined.contains("Uc2DiskLow")
        && combined.contains("have no RULE_BUILDERS entry");

    let _ = std::fs::remove_dir_all(&scratch);

    // The tracked file was never opened for writing above — re-confirmed
    // cheaply here too, so a future regression in this function trips loudly
    // rather than silently.
    let still_pristine = std::fs::read_to_string(&script_path).expect("re-read m10_alert_fire.sh") == original;
    assert!(
        still_pristine,
        "scripts/m10_alert_fire.sh changed during the anti-vacuity probe — this function must \
         never write to it (see its own doc comment)"
    );

    (tripped, tail_str(&combined, 2000))
}

fn run_alerts_row(root: &Path) -> Verdict {
    println!("== row 4: disk-low observability ==");
    let (disk_found, disk_detail) = check_live_free_disk_bytes_scrape(root);
    println!("  live daemon scrape: {disk_detail}");

    let (script_ok, pass_count, script_out) = run_alert_fire_script();
    println!("  scripts/m10_alert_fire.sh: exit_ok={script_ok} PASS-count={pass_count}/14");
    if !(script_ok && pass_count == 14) {
        println!("{}", tail_str(&script_out, 4000));
    }

    let (tripped, trip_out) = check_yaml_builders_anti_vacuity(root);
    println!(
        "  YAML<->builders anti-vacuity trip: tripped={tripped} (run against a scratch copy — \
         scripts/m10_alert_fire.sh itself was never opened for writing)"
    );
    if !tripped {
        println!("{}", tail_str(&trip_out, 4000));
    }

    let pass = disk_found && script_ok && pass_count == 14 && tripped;
    let detail = format!(
        "live scrape uc2_free_disk_bytes present={disk_found}; m10_alert_fire.sh exit_ok={script_ok} \
         PASS={pass_count}/14; anti-vacuity trip (scratch copy, tracked script untouched)={tripped}"
    );
    Verdict::new_pf("4 disk-low observability", pass, detail)
}

// =========================================================== row 5: flag-day smoke

fn write_flagday_node_toml(
    cfg_path: &Path,
    id: u32,
    bind: SocketAddr,
    instance_dir: &Path,
    members: &[(u32, SocketAddr)],
    app: &str,
) {
    let mut members_toml = String::new();
    for (mid, maddr) in members {
        members_toml.push_str(&format!("[[members]]\nid = {mid}\naddr = \"{maddr}\"\n\n"));
    }
    let body = format!(
        "id = {id}\n\
         bind = \"{bind}\"\n\
         instance_dir = \"{}\"\n\
         app_id = \"{app}\"\n\
         buffer_bytes = {}\n\
         journal_segment_bytes = {}\n\
         \n\
         {members_toml}",
        instance_dir.display(),
        1u64 << 20,
        1u64 << 20,
    );
    std::fs::write(cfg_path, body).expect("write flagday-smoke node.toml");
}

/// Panic-safe teardown: on drop, `pkill` every `uc2-node --config <cfg>`
/// pattern this run could have started (the ORIGINAL processes we spawned,
/// AND any the script itself started later under different, un-tracked
/// PIDs via its own `nohup` — see `scripts/uc2_flag_day.sh`'s `--local`
/// mode) — TERM first, then a bounded KILL sweep, regardless of how
/// [`run_flagday_smoke`] returns, including via a panic. Like every `Drop`
/// guard, this does NOT run on `SIGKILL`/an unhandled `SIGTERM` to this gate
/// process — row 5's local smoke is ungated, so a killed run can leak
/// orphaned local `uc2-node` processes on this dev box; a PID-file-based
/// recovery sweep (independent of this process's own lifetime) is a
/// candidate for a future task, not implemented here.
struct KillGuard(Vec<PathBuf>);
impl Drop for KillGuard {
    fn drop(&mut self) {
        for cfg in &self.0 {
            let pat = format!("uc2-node --config {}", cfg.display());
            let _ = Command::new("pkill").args(["-TERM", "-f", "--", &pat]).status();
        }
        std::thread::sleep(Duration::from_millis(500));
        for cfg in &self.0 {
            let pat = format!("uc2-node --config {}", cfg.display());
            let _ = Command::new("pkill").args(["-KILL", "-f", "--", &pat]).status();
        }
    }
}

fn run_flagday_smoke(root: &Path) {
    println!("\n== row 5 (FLEET-ONLY BAR): flag-day downtime — LOCAL SMOKE, ungated ==");
    let scratch = root.join("flagday-smoke");
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).expect("create flagday-smoke scratch root");

    let node_bin = uc_node_release_bin();
    let ctl_bin = uc2ctl_release_bin();

    const N: usize = 3;
    let addrs: Vec<SocketAddr> = (0..N).map(|_| free_udp_addr()).collect();
    let members: Vec<(u32, SocketAddr)> = (0..N as u32).map(|i| (i, addrs[i as usize])).collect();
    let app = "m11-flagday-smoke";

    let mut cfg_paths: Vec<PathBuf> = Vec::with_capacity(N);
    let mut instance_dirs: Vec<PathBuf> = Vec::with_capacity(N);
    for i in 0..N as u32 {
        let idir = scratch.join(format!("n{i}"));
        std::fs::create_dir_all(&idir).expect("create flagday-smoke instance dir");
        let cfg = scratch.join(format!("n{i}.toml"));
        write_flagday_node_toml(&cfg, i, addrs[i as usize], &idir, &members, app);
        let cfg_abs = std::fs::canonicalize(&cfg).expect("canonicalize flagday-smoke config path");
        cfg_paths.push(cfg_abs);
        instance_dirs.push(idir);
    }

    let _kill_guard = KillGuard(cfg_paths.clone());

    // Spawn all three now, matching the exact `<bin> --config <abs-cfg>`
    // argv shape scripts/uc2_flag_day.sh's --local pgrep/pkill pattern
    // expects — see local_cfg_abs/start_node/step 2 in that script.
    let mut children: Vec<Child> = Vec::with_capacity(N);
    for cfg in &cfg_paths {
        let child = Command::new(&node_bin)
            .arg("--config")
            .arg(cfg)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn uc2-node --config {}: {e}", cfg.display()));
        children.push(child);
    }

    let want = NODE_FLAG_LEADER | NODE_FLAG_CAN_SERVE;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut leader_idx = None;
    while Instant::now() < deadline {
        let serving: Vec<usize> = (0..N)
            .filter(|&i| {
                open_cnc(&instance_dirs[i], app).is_some_and(|c| c.status().flags.load_acquire() & want == want)
            })
            .collect();
        if serving.len() == 1 {
            leader_idx = Some(serving[0]);
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let Some(leader_idx) = leader_idx else {
        println!(
            "  WARNING: no single leader elected within 30s — flagday-smoke SKIPPED (harness \
             setup only, ungated; see the gate doc — row 5's real bar is fleet-only)"
        );
        for mut c in children {
            let _ = c.kill();
            let _ = c.wait();
        }
        return; // KillGuard still sweeps on drop.
    };
    println!("  local 3-node cluster up, leader=n{leader_idx}");

    let hosts_arg =
        format!("local:{},{},{}", cfg_paths[0].display(), cfg_paths[1].display(), cfg_paths[2].display());
    let script = repo_root().join("scripts/uc2_flag_day.sh");
    let out = Command::new("bash")
        .arg(&script)
        .args([
            "--hosts",
            &hosts_arg,
            "--uc2ctl",
            &ctl_bin.display().to_string(),
            "--uc2-node-bin",
            &node_bin.display().to_string(),
            "--app-id",
            app,
            "--upgrade-cmd",
            "true",
            "--yes-traffic-stopped",
        ])
        .current_dir(repo_root())
        .output()
        .unwrap_or_else(|e| panic!("spawn scripts/uc2_flag_day.sh --local: {e}"));
    let combined =
        format!("{}\n{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
    let downtime_line = combined.lines().find(|l| l.starts_with("DOWNTIME:")).unwrap_or("no DOWNTIME line found");
    println!(
        "  SMOKE (ungated — see gate doc; row 5's real bar is fleet-only): uc2_flag_day.sh \
         exit={:?}, {downtime_line}",
        out.status.code()
    );
    if out.status.code() != Some(0) {
        println!("{}", tail_str(&combined, 4000));
    }

    // The originally-spawned Children may already be gone (the script
    // stopped and possibly restarted them under different, untracked PIDs)
    // — best-effort only; KillGuard (by command-line pattern, not PID) is
    // the authoritative teardown, on drop below.
    for mut c in children {
        let _ = c.kill();
        let _ = c.wait();
    }
}

// =========================================================== shared plumbing

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().expect("uc_node has a parent dir").to_path_buf()
}

/// `target/<profile>/examples/m11_gate` -> `target/<profile>/m11_gate_scratch`
/// — real disk (this workspace's shared target dir), never `/tmp`. Same
/// pattern as `m9_gate.rs`/`m10_gate.rs`.
fn default_root() -> PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().and_then(|p| p.parent()).map(|p| p.join("m11_gate_scratch")))
        .unwrap_or_else(|| PathBuf::from("target/m11_gate_scratch"))
}

fn free_udp_addr() -> SocketAddr {
    let s = UdpSocket::bind("127.0.0.1:0").expect("bind UDP probe");
    let addr = s.local_addr().unwrap();
    drop(s);
    addr
}

fn free_tcp_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind TCP probe");
    let addr = l.local_addr().unwrap();
    drop(l);
    addr
}

fn open_cnc(dir: &Path, app: &str) -> Option<Arc<CncPage>> {
    CncPage::open_file(&dir.join("cnc2.dat"), app).ok()
}

/// Build (once, cached) a package's own `[[bin]]` target on demand and
/// return its executable path — the hand-rolled `cargo build
/// --message-format=json` scan `examples/uc_crashtest/tests/enospc.rs` uses
/// for the identical reason: `CARGO_BIN_EXE_<name>` is not set for an
/// `examples/` target, only for integration tests/benchmarks.
fn build_bin(cache: &'static OnceLock<PathBuf>, pkg: &str, bin: &str, release: bool) -> PathBuf {
    cache
        .get_or_init(|| {
            let mut args: Vec<String> =
                vec!["build".into(), "-p".into(), pkg.into(), "--bin".into(), bin.into()];
            if release {
                args.push("--release".into());
            }
            args.push("--message-format=json".into());
            let out = Command::new(env!("CARGO"))
                .args(&args)
                .current_dir(repo_root())
                .output()
                .unwrap_or_else(|e| panic!("spawn cargo build -p {pkg} --bin {bin}: {e}"));
            assert!(
                out.status.success(),
                "cargo build -p {pkg} --bin {bin} failed:\n{}",
                String::from_utf8_lossy(&out.stderr)
            );
            let stdout = String::from_utf8_lossy(&out.stdout);
            for line in stdout.lines() {
                if line.contains(&format!("\"name\":\"{bin}\""))
                    && let Some(rest) = line.split("\"executable\":\"").nth(1)
                    && let Some(end) = rest.find('"')
                {
                    return PathBuf::from(&rest[..end]);
                }
            }
            panic!(
                "cargo build --message-format=json for {bin} produced no executable artifact:\n{stdout}"
            );
        })
        .clone()
}

fn uc_node_debug_bin() -> PathBuf {
    static CACHE: OnceLock<PathBuf> = OnceLock::new();
    build_bin(&CACHE, "uc_node", "uc2-node", false)
}

fn uc_node_release_bin() -> PathBuf {
    static CACHE: OnceLock<PathBuf> = OnceLock::new();
    build_bin(&CACHE, "uc_node", "uc2-node", true)
}

fn uc2ctl_release_bin() -> PathBuf {
    static CACHE: OnceLock<PathBuf> = OnceLock::new();
    build_bin(&CACHE, "uc_ctl", "uc2ctl", true)
}
