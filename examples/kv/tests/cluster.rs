//! The three-node cluster, driven for real: the release binaries, three
//! `kv-service`s, three gateways, and `kv`/`kv-load` from outside.
//!
//! Opt-in (`--features cluster-tests`): it takes a minute or two and needs
//! the 2.12.0 release binaries at `../release/…/bin` (or `UC2_BIN_DIR`).
//! Ports are offset by 10 (9310…, 9410…, 9510…) so a manual cluster on the
//! defaults can stay up.
//!
//! What it demonstrates, in order — the brief's three guarantees plus what
//! a replicated store needs:
//!  1. the four operations through a gateway, with versions and exit codes;
//!  2. a retried write applies once (`replayed=true`, value unchanged);
//!  3. an acknowledged write survives SIGKILL of a node (and its service and
//!     gateway) and is read back from that node after restart;
//!  4. writes continue across a leader kill, and a linearizable read that
//!     starts after an ack sees it;
//!  5. the journal is bounded: load, snapshot, purge shrinks every journal,
//!     a service restarted below the floor and a wiped node both converge
//!     to the same digest;
//!  6. `kv-service` exits 0 on SIGTERM (docs/how-to/write-a-service-binary.md).
#![cfg(feature = "cluster-tests")]

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

struct Cluster {
    script: PathBuf,
    root: PathBuf,
    gateways: String,
    offset: u32,
    /// Which `kv-service` the script starts: this build's, or an override
    /// (the preserved v1 binaries, for the upgrade test).
    bin_dir: PathBuf,
}

impl Cluster {
    fn new() -> Self {
        Self::at(10, "kvcluster")
    }
    fn at(offset: u32, name: &str) -> Self {
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/kvcluster.sh");
        let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
        let gw = 9400 + offset;
        Cluster {
            script,
            root,
            gateways: format!("127.0.0.1:{gw},127.0.0.1:{},127.0.0.1:{}", gw + 1, gw + 2),
            offset,
            bin_dir: Self::bin_dir(),
        }
    }
    fn bin_dir() -> PathBuf {
        Path::new(env!("CARGO_BIN_EXE_kv"))
            .parent()
            .unwrap()
            .to_path_buf()
    }
    fn sh(&self, args: &[&str]) -> Output {
        Command::new(&self.script)
            .args(args)
            .env("KV_ROOT", &self.root)
            .env("KV_BIN_DIR", &self.bin_dir)
            .env("KV_PORT_OFFSET", self.offset.to_string())
            .stdin(Stdio::null())
            .output()
            .expect("run kvcluster.sh")
    }
    fn sh_ok(&self, args: &[&str]) -> String {
        let o = self.sh(args);
        assert!(
            o.status.success(),
            "kvcluster.sh {args:?} failed:\n{}\n{}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        );
        String::from_utf8_lossy(&o.stdout).into_owned()
    }
    fn kv_at(&self, gateways: &str, args: &[&str]) -> (i32, String) {
        let o = Command::new(env!("CARGO_BIN_EXE_kv"))
            .arg("--gateways")
            .arg(gateways)
            .args(args)
            .output()
            .expect("run kv");
        let out = format!(
            "{}{}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        );
        (o.status.code().unwrap_or(-1), out.trim().to_string())
    }
    fn kv(&self, args: &[&str]) -> (i32, String) {
        self.kv_at(&self.gateways, args)
    }
    fn gateway(&self, n: u32) -> String {
        format!("127.0.0.1:{}", 9400 + self.offset + n)
    }
    fn status(&self, n: u32) -> String {
        self.sh_ok(&["ctl", &n.to_string(), "status"])
    }
    fn row_version(&self, n: u32) -> String {
        let st = self.status(n);
        st.lines()
            .find(|l| l.trim_start().starts_with("row=0"))
            .and_then(|l| l.split("version=").nth(1))
            .and_then(|v| v.split(' ').next())
            .unwrap_or("?")
            .to_string()
    }
    fn digest_via(&self, n: u32) -> String {
        let (code, out) = self.kv_at(&self.gateway(n), &["digest"]);
        assert_eq!(code, 0, "digest via gateway {n}: {out}");
        // "count=N digest=0x… last_applied=P via=…" — compare count+digest only.
        out.split_whitespace().take(2).collect::<Vec<_>>().join(" ")
    }
    fn leader(&self) -> u32 {
        self.sh_ok(&["wait-leader", "30"])
            .trim()
            .parse()
            .expect("leader id")
    }
    fn journal_segments(&self, n: u32) -> usize {
        std::fs::read_dir(self.root.join(format!("n{n}/journal")))
            .map(|d| {
                d.filter_map(|e| e.ok())
                    .filter(|e| e.file_name().to_string_lossy().starts_with("seg-0"))
                    .count()
            })
            .unwrap_or(0)
    }
    fn metric(&self, n: u32, name: &str) -> Option<u64> {
        let body = self.sh_ok(&["metrics", &n.to_string()]);
        body.lines().find_map(|l| {
            l.strip_prefix(&format!("{name} "))
                .and_then(|v| v.trim().parse::<f64>().ok())
                .map(|f| f as u64)
        })
    }
    /// A row-0-labeled gauge sample, e.g. `uc2_service_version{service="kv",row="0"} 33554432`.
    fn metric_row0(&self, n: u32, name: &str) -> Option<String> {
        let body = self.sh_ok(&["metrics", &n.to_string()]);
        body.lines()
            .find(|l| {
                l.starts_with(name) && l.contains("row=\"0\"") && l[name.len()..].starts_with('{')
            })
            .and_then(|l| l.rsplit(' ').next())
            .map(|v| v.trim().to_string())
    }
    /// The leader's digest, once it is answering stably. NOTE: a RemoteClient
    /// hops to the leader on connect (L21), so this reads the leader whatever
    /// gateway it dials — it proves the committed value is readable, not that
    /// a given node's replica holds it. The wipe/rejoin `snapshot_installed`
    /// check is what proves a local reconstruction.
    fn wait_leader_digest(&self, timeout: Duration) -> String {
        let deadline = Instant::now() + timeout;
        loop {
            let a = self.digest_via(0);
            std::thread::sleep(Duration::from_millis(300));
            let b = self.digest_via(0);
            if a == b {
                return a;
            }
            assert!(Instant::now() < deadline, "leader digest never settled");
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = self.sh(&["down"]);
    }
}

/// The kv image's version word inside an on-disk artifact: after UC's 16-byte
/// `ULTSNAP1 ‖ P` envelope and `Sessioned`'s `u64 len ‖ dedup table` prefix
/// (WIRE-FORMAT.md § 5).
fn kv_image_version(artifact: &Path) -> u32 {
    let b = std::fs::read(artifact).unwrap();
    assert_eq!(&b[..8], b"ULTSNAP1");
    let table_len = u64::from_le_bytes(b[16..24].try_into().unwrap()) as usize;
    let at = 24 + table_len;
    u32::from_le_bytes(b[at..at + 4].try_into().unwrap())
}

fn dump_logs(root: &Path) -> String {
    let mut s = String::new();
    if let Ok(entries) = std::fs::read_dir(root.join("logs")) {
        let mut paths: Vec<PathBuf> = entries.flatten().map(|e| e.path()).collect();
        paths.sort();
        for p in paths {
            let body = std::fs::read_to_string(&p).unwrap_or_default();
            let tail: Vec<&str> = body.lines().rev().take(15).collect();
            s.push_str(&format!("\n----- {} -----\n", p.display()));
            for l in tail.into_iter().rev() {
                s.push_str(l);
                s.push('\n');
            }
        }
    }
    s
}

#[test]
fn three_node_cluster_end_to_end() {
    let c = Cluster::new();
    let up = c.sh(&["up", "--fresh"]);
    assert!(
        up.status.success(),
        "up failed:\n{}\n{}\n{}",
        String::from_utf8_lossy(&up.stdout),
        String::from_utf8_lossy(&up.stderr),
        dump_logs(&c.root)
    );

    // 1. The four operations, versions, exit codes.
    let (code, out) = c.kv(&["put", "k", "v1"]);
    assert_eq!(code, 0, "{out}");
    let v1: u64 = out
        .split("version=")
        .nth(1)
        .unwrap()
        .split(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        v1 >= 32,
        "a user frame never sits at position 0 (L11): {out}"
    );
    assert!(out.contains("replayed=false"));
    let (code, out) = c.kv(&["get", "k", "--linearizable"]);
    assert_eq!(
        (code, out.as_str()),
        (0, format!("version={v1} value=v1").as_str())
    );
    let (code, out) = c.kv(&["cas", "k", "v2", "--version", "1"]);
    assert_eq!(code, 3, "{out}");
    assert!(
        out.starts_with(&format!("version_mismatch current={v1}")),
        "{out}"
    );
    let (code, out) = c.kv(&["cas", "k", "v2", "--version", &v1.to_string()]);
    assert_eq!(code, 0, "{out}");
    let v2: u64 = out
        .split("version=")
        .nth(1)
        .unwrap()
        .split(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert!(v2 > v1);
    let (code, out) = c.kv(&["get", "k"]);
    assert_eq!(
        (code, out.as_str()),
        (0, format!("version={v2} value=v2").as_str())
    );
    let (code, out) = c.kv(&["delete", "k"]);
    assert_eq!(code, 0, "{out}");
    assert!(
        out.starts_with(&format!("ok deleted_version={v2} position=")),
        "delete reports the removed version: {out}"
    );
    let del_pos: u64 = out
        .split("position=")
        .nth(1)
        .unwrap()
        .split(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert!(
        del_pos > v2,
        "the delete frame sits after the write it removes"
    );
    let (code, out) = c.kv(&["get", "k", "--linearizable"]);
    assert_eq!((code, out.as_str()), (3, "not_found"));
    let (code, _) = c.kv(&["delete", "k"]);
    assert_eq!(code, 3);
    let (code, out) = c.kv(&["cas", "k", "created", "--version", "0"]);
    assert_eq!(code, 0, "CAS with 0 creates an absent key: {out}");
    // v2: Append / List, and the shape rules.
    let (code, out) = c.kv(&["append", "log", "one"]);
    assert_eq!(code, 0, "{out}");
    assert!(
        out.starts_with("ok version=") && out.contains(" len=1 "),
        "{out}"
    );
    let (code, out) = c.kv(&["append", "log", "two"]);
    assert_eq!(code, 0, "{out}");
    assert!(out.contains(" len=2 "), "{out}");
    let vl: u64 = out
        .split("version=")
        .nth(1)
        .unwrap()
        .split(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let (code, out) = c.kv(&["list", "log", "--linearizable"]);
    assert_eq!(code, 0, "{out}");
    assert_eq!(out, format!("version={vl} len=2\n[0] one\n[1] two"));
    let (code, out) = c.kv(&["get", "log"]);
    assert_eq!(code, 3, "{out}");
    assert!(out.starts_with("wrong_shape"), "{out}");
    let (code, out) = c.kv(&["append", "k", "x"]);
    assert_eq!((code, out.starts_with("wrong_shape")), (3, true), "{out}");
    let (code, out) = c.kv(&["list", "k"]);
    assert_eq!((code, out.starts_with("wrong_shape")), (3, true), "{out}");
    let (code, out) = c.kv(&["put", "log", "x"]);
    assert_eq!((code, out.starts_with("wrong_shape")), (3, true), "{out}");
    let (code, out) = c.kv(&["delete", "log"]);
    assert_eq!(code, 0, "{out}");
    assert!(
        out.starts_with(&format!("ok deleted_version={vl}")),
        "{out}"
    );
    let (code, out) = c.kv(&["list", "log"]);
    assert_eq!((code, out.as_str()), (3, "not_found"));
    // The retry guarantee covers Append too.
    let (_, first) = c.kv(&["--client-id", "9", "append", "log2", "a"]);
    let (_, again) = c.kv(&["--client-id", "9", "append", "log2", "a"]);
    assert!(
        first.contains("len=1 ") && first.contains("replayed=false"),
        "{first}"
    );
    assert!(
        again.contains("len=1 ") && again.contains("replayed=true"),
        "{again}"
    );
    let (_, out) = c.kv(&["list", "log2"]);
    assert!(out.ends_with("len=1\n[0] a"), "appended once: {out}");
    assert_eq!(c.row_version(0), "2.0.0");

    // Oversize is refused client-side (exit 2) and never reaches the cluster.
    let big = "x".repeat(1025);
    let (code, out) = c.kv(&["put", "k", &big]);
    assert_eq!(code, 2, "{out}");
    let max = "y".repeat(1024);
    let (code, _) = c.kv(&["put", "max", &max]);
    assert_eq!(code, 0);
    let (code, out) = c.kv(&["get", "max"]);
    assert_eq!(code, 0);
    assert!(out.ends_with(&max));

    // 2. A retried write applies once.
    let (code, out) = c.kv(&["--client-id", "7", "put", "retry", "first"]);
    assert_eq!(code, 0, "{out}");
    assert!(out.contains("replayed=false"), "{out}");
    let vr: u64 = out
        .split("version=")
        .nth(1)
        .unwrap()
        .split(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let (code, out) = c.kv(&["--client-id", "7", "put", "retry", "SECOND"]);
    assert_eq!(code, 0, "{out}");
    assert!(
        out.contains("replayed=true"),
        "the re-send must be answered from the dedup cache: {out}"
    );
    assert!(
        out.contains(&format!("version={vr}")),
        "the cached reply is the original: {out}"
    );
    let (_, out) = c.kv(&["get", "retry", "--linearizable"]);
    assert_eq!(
        out,
        format!("version={vr} value=first"),
        "applied once, not twice"
    );

    // 3. Durability across SIGKILL of a node, its service and its gateway.
    let (code, out) = c.kv(&["put", "durable", "before"]);
    assert_eq!(code, 0, "{out}");
    let victim = (c.leader() + 1) % 3; // a follower
    c.sh_ok(&["kill", "service", &victim.to_string()]);
    c.sh_ok(&["kill", "node", &victim.to_string()]); // kills its gateway too (BindsTo emulation)
    let (code, out) = c.kv(&["put", "during", "outage"]);
    assert_eq!(code, 0, "two of three still commit: {out}");
    c.sh_ok(&["start", "node", &victim.to_string()]);
    std::thread::sleep(Duration::from_millis(500));
    c.sh_ok(&["start", "service", &victim.to_string()]);
    c.sh_ok(&["start", "gateway", &victim.to_string()]);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (code, out) = c.kv_at(&c.gateway(victim), &["get", "during"]);
        if code == 0 && out.ends_with("value=outage") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "restarted node never caught up: {out}"
        );
        std::thread::sleep(Duration::from_millis(300));
    }
    let (_, out) = c.kv_at(&c.gateway(victim), &["get", "durable"]);
    assert!(out.ends_with("value=before"), "{out}");

    // 4. Leader kill: writes continue; a read after an ack sees the write.
    let old_leader = c.leader();
    c.sh_ok(&["kill", "node", &old_leader.to_string()]);
    let t = Instant::now();
    let (code, out) = c.kv(&["put", "after-leader-kill", "yes"]);
    assert_eq!(code, 0, "write after leader kill: {out}");
    let took = t.elapsed();
    let (code, out) = c.kv(&["get", "after-leader-kill", "--linearizable"]);
    assert_eq!((code, out.ends_with("value=yes")), (0, true), "{out}");
    let new_leader = c.leader();
    assert_ne!(new_leader, old_leader);
    eprintln!("leader {old_leader} killed; write succeeded in {took:?}; new leader {new_leader}");
    c.sh_ok(&["stop", "service", &old_leader.to_string()]);
    c.sh_ok(&["start", "node", &old_leader.to_string()]);
    std::thread::sleep(Duration::from_millis(500));
    c.sh_ok(&["start", "service", &old_leader.to_string()]);
    c.sh_ok(&["start", "gateway", &old_leader.to_string()]);
    c.wait_leader_digest(Duration::from_secs(30));

    // 5. Bound the journal: load, snapshot, purge, converge below the floor.
    let load = Command::new(env!("CARGO_BIN_EXE_kv-load"))
        .args([
            "--gateways",
            &c.gateways,
            "--keys",
            "40000",
            "--value-bytes",
            "512",
            "--verify",
            "50",
        ])
        .output()
        .expect("kv-load");
    assert!(
        load.status.success(),
        "kv-load failed:\n{}\n{}",
        String::from_utf8_lossy(&load.stdout),
        String::from_utf8_lossy(&load.stderr)
    );
    eprintln!("{}", String::from_utf8_lossy(&load.stdout).trim());
    let before = c.wait_leader_digest(Duration::from_secs(30));
    assert!(before.starts_with("count=40"), "{before}");
    let segs_before: Vec<usize> = (0..3).map(|n| c.journal_segments(n)).collect();
    assert!(
        segs_before.iter().all(|&s| s >= 3),
        "need several 4 MiB segments to purge: {segs_before:?}"
    );
    let out = c.sh_ok(&["snapshot"]);
    assert!(out.starts_with("instant="), "{out}");
    let p: u64 = out.trim().trim_start_matches("instant=").parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let floors: Vec<Option<u64>> = (0..3)
            .map(|n| c.metric(n, "uc_node_snapshot_floor_bytes"))
            .collect();
        let bases: Vec<Option<u64>> = (0..3)
            .map(|n| c.metric(n, "uc2_archive_first_base_bytes"))
            .collect();
        let segs: Vec<usize> = (0..3).map(|n| c.journal_segments(n)).collect();
        if floors.iter().all(|f| *f == Some(p))
            && bases.iter().all(|b| b.unwrap_or(0) > 0)
            && segs.iter().zip(&segs_before).all(|(a, b)| a < b)
        {
            eprintln!(
                "snapshot at {p}: floors {floors:?}, first_base {bases:?}, segments {segs_before:?} -> {segs:?}"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "purge did not happen: floors {floors:?} first_base {bases:?} segments {segs_before:?} -> {segs:?}\n{}",
            dump_logs(&c.root)
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    for n in 0..3 {
        let show = c.sh_ok(&["snapshot-show", &n.to_string()]);
        assert!(show.contains(&format!("set={p}")), "node {n}: {show}");
    }
    // A service restarted below the floor installs the artifact and agrees.
    c.sh_ok(&["kill", "service", "0"]);
    c.sh_ok(&["start", "service", "0"]);
    // A wiped node rejoins by snapshot session and agrees.
    c.sh_ok(&["wipe", "2"]);
    c.sh_ok(&["start", "node", "2"]);
    std::thread::sleep(Duration::from_secs(1));
    c.sh_ok(&["start", "service", "2"]);
    c.sh_ok(&["start", "gateway", "2"]);
    let after = c.wait_leader_digest(Duration::from_secs(60));
    assert_eq!(
        after, before,
        "state must survive purge + install + rejoin unchanged"
    );
    let n2 = std::fs::read_to_string(c.root.join("logs/node2.log")).unwrap_or_default();
    assert!(
        n2.contains("\"event\":\"snapshot_installed\""),
        "node 2 should have installed the set:\n{}",
        dump_logs(&c.root)
    );

    // 6. The service half's signal discipline: SIGTERM -> exit 0.
    c.sh_ok(&["stop", "service", "1"]);
    let mut svc = Command::new(env!("CARGO_BIN_EXE_kv-service"))
        .arg("--instance-dir")
        .arg(c.root.join("n1"))
        .arg("--app-id")
        .arg("kv")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn kv-service");
    std::thread::sleep(Duration::from_secs(2));
    unsafe { libc::kill(svc.id() as i32, libc::SIGTERM) };
    let st = svc.wait().expect("wait kv-service");
    assert!(
        st.success(),
        "kv-service must handle SIGTERM and exit 0, got {st:?}"
    );
}

/// The store's own upgrade, v1 -> v2, end to end (BRIEF-v2 § Done means and
/// § The lifecycle question). Needs the previous KV version's binaries: point
/// `KV_V1_BIN_DIR` at them, or build them into `.run/v1-bin` (README § Tests).
/// Skipped, not failed, when they are absent.
///
/// Shows, in order: a v1 cluster with v1 data and a v1 snapshot; that a
/// service-only partial upgrade lets a mixed-version cluster COMMIT an Append
/// the followers cannot apply, so a failover to a v1 successor makes an
/// acknowledged write invisible (L22); that the full flag day heals it from
/// the durable log; that v1 data and ops still work; that a v2 service
/// installs the v1 on-disk artifact below the floor; and that a v1 service
/// REFUSES a v2 image (rollback is one-way past a v2 snapshot, L18).
#[test]
fn upgrade_v1_to_v2_flag_day() {
    let v1 = std::env::var("KV_V1_BIN_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| Path::new(env!("CARGO_MANIFEST_DIR")).join(".run/v1-bin"));
    if !v1.join("kv-service").exists() {
        eprintln!(
            "SKIP upgrade_v1_to_v2_flag_day: no v1 binaries at {}. \
             Set KV_V1_BIN_DIR, or build the previous KV version into .run/v1-bin (README § Tests).",
            v1.display()
        );
        return;
    }
    let v2 = Cluster::bin_dir();
    let v1_kv = v1.join("kv");

    // A v1 cluster: v1 kv-service everywhere.
    let mut c = Cluster::at(20, "kvupgrade");
    c.bin_dir = v1.clone();
    let up = c.sh(&["up", "--fresh"]);
    assert!(
        up.status.success(),
        "up failed:\n{}\n{}",
        String::from_utf8_lossy(&up.stdout),
        dump_logs(&c.root)
    );
    let kv1 = |args: &[&str]| -> (i32, String) {
        let o = Command::new(&v1_kv)
            .arg("--gateways")
            .arg(&c.gateways)
            .args(args)
            .output()
            .unwrap();
        (
            o.status.code().unwrap_or(-1),
            format!(
                "{}{}",
                String::from_utf8_lossy(&o.stdout),
                String::from_utf8_lossy(&o.stderr)
            )
            .trim()
            .to_string(),
        )
    };
    for n in 0..3 {
        assert_eq!(c.row_version(n), "1.0.0", "node {n}");
    }
    // v1 data, plus enough log to exceed the 16 MiB ring, then a v1 snapshot
    // BEFORE any v2 exists — this is the "old image" the v2 binary must read.
    let (code, out) = kv1(&["put", "v1-key", "v1-value"]);
    assert_eq!(code, 0, "{out}");
    let v1_version: u64 = out
        .split("version=")
        .nth(1)
        .unwrap()
        .split(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let load = Command::new(v1.join("kv-load"))
        .args([
            "--gateways",
            &c.gateways,
            "--keys",
            "40000",
            "--value-bytes",
            "512",
            "--verify",
            "20",
            "--prefix",
            "v1",
        ])
        .output()
        .unwrap();
    assert!(
        load.status.success(),
        "{}",
        String::from_utf8_lossy(&load.stderr)
    );
    let out = c.sh_ok(&["snapshot"]);
    let p1: u64 = out
        .trim()
        .trim_start_matches("instant=")
        .parse()
        .expect("instant");
    let deadline = Instant::now() + Duration::from_secs(30);
    while (0..3).any(|n| c.metric(n, "uc_node_snapshot_floor_bytes") != Some(p1)) {
        assert!(Instant::now() < deadline, "v1 snapshot never completed");
        std::thread::sleep(Duration::from_millis(300));
    }
    assert_eq!(
        kv_image_version(&c.root.join(format!("n0/snapshots/0/snap-{p1}.ultsnap"))),
        1,
        "the artifact on disk is a v1 image"
    );
    // The v1 client does not know `append` (clap usage error, exit 2).
    assert_eq!(kv1(&["append", "x", "y"]).0, 2);

    // ---- The hazard: a service-only partial upgrade. v2 on the leader only.
    let lead = c.leader();
    c.sh_ok(&["stop", "service", &lead.to_string()]);
    c.bin_dir = v2.clone();
    c.sh_ok(&["start", "service", &lead.to_string()]);
    let deadline = Instant::now() + Duration::from_secs(15);
    while c.row_version(lead) != "2.0.0" {
        assert!(
            Instant::now() < deadline,
            "v2 service never attached on the leader"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    let follower = (lead + 1) % 3;
    assert_eq!(c.row_version(follower), "1.0.0");
    // The platform's own drift signal, node-local (no leader hop): the two
    // nodes export different packed versions. This is Uc2ServiceVersionDrift.
    let vlead = c.metric_row0(lead, "uc2_service_version");
    let vfoll = c.metric_row0(follower, "uc2_service_version");
    assert_ne!(
        vlead, vfoll,
        "uc2_service_version must differ across a mixed cluster: lead={vlead:?} follower={vfoll:?}"
    );
    eprintln!("mixed cluster: node {lead} version {vlead:?}, node {follower} version {vfoll:?}");
    // The v2 leader accepts and commits an Append the v1 followers cannot apply.
    let (code, out) = c.kv_at(&c.gateway(lead), &["append", "mixed", "diverge"]);
    assert_eq!(code, 0, "the v2 leader commits the append: {out}");
    assert!(out.contains(" len=1 "), "{out}");
    // Kill the v2 leader; a v1 node is elected and has NO record of the append
    // (its service stored the frame but could not apply op 4).
    c.sh_ok(&["kill", "service", &lead.to_string()]);
    c.sh_ok(&["kill", "node", &lead.to_string()]);
    let v1_leader = c.leader();
    assert_eq!(c.row_version(v1_leader), "1.0.0");
    let (code, out) = c.kv(&["get", "mixed", "--linearizable"]);
    assert_eq!(
        (code, out.as_str()),
        (3, "not_found"),
        "the acknowledged append is invisible under the v1 successor (L22): {out}"
    );
    let (code, out) = c.kv(&["list", "mixed"]);
    assert_eq!(code, 3, "and list is unknown to v1: {out}");
    eprintln!(
        "mixed-version hazard shown: an acked append is not_found after failover to a v1 leader"
    );

    // ---- The flag day: bring the ex-leader's node back, then upgrade every
    // service to v2. The append is durably in the log, so replay re-applies it.
    c.sh_ok(&["start", "node", &lead.to_string()]);
    std::thread::sleep(Duration::from_millis(500));
    for n in 0..3 {
        c.sh_ok(&["stop", "service", &n.to_string()]);
    }
    for n in 0..3 {
        c.sh_ok(&["start", "service", &n.to_string()]);
    }
    c.sh_ok(&["start", "gateway", &lead.to_string()]); // it died with the node (BindsTo)
    let deadline = Instant::now() + Duration::from_secs(30);
    while (0..3).any(|n| c.row_version(n) != "2.0.0") {
        assert!(
            Instant::now() < deadline,
            "not every service reached v2:\n{}",
            dump_logs(&c.root)
        );
        std::thread::sleep(Duration::from_millis(300));
    }
    // Healed from the log: the acknowledged append is back.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (code, out) = c.kv(&["list", "mixed", "--linearizable"]);
        if code == 0 && out.ends_with("len=1\n[0] diverge") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the append never healed: ({code}) {out}"
        );
        std::thread::sleep(Duration::from_millis(300));
    }
    eprintln!("flag day complete: the append reappeared everywhere from the durable log");
    // v1 data intact, v1 ops unchanged, v2 ops available.
    let (code, out) = c.kv(&["get", "v1-key", "--linearizable"]);
    assert_eq!(
        (code, out.as_str()),
        (0, format!("version={v1_version} value=v1-value").as_str())
    );
    assert_eq!(c.kv(&["get", "load:v1:123"]).0, 0);
    assert_eq!(c.kv(&["append", "after", "upgrade"]).0, 0);
    let healed = c.wait_leader_digest(Duration::from_secs(10));

    // ---- A v1 image installed by a v2 service, below the purge floor.
    // (The floor is still the v1 set p1; nothing newer was snapshotted.)
    let install_node = (lead + 1) % 3;
    c.sh_ok(&["kill", "service", &install_node.to_string()]);
    c.sh_ok(&["start", "service", &install_node.to_string()]);
    let deadline = Instant::now() + Duration::from_secs(30);
    while c.row_version(install_node) != "2.0.0" {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(200));
    }
    let n = c.root.join(format!("logs/node{install_node}.log"));
    // A restart replays from the floor; if the log still holds the tail it may
    // replay rather than install. Either way the state must match; assert the
    // value survived, and note which path the log shows.
    let (code, out) = c.kv(&["list", "mixed", "--linearizable"]);
    assert_eq!(
        (code, out.ends_with("len=1\n[0] diverge")),
        (0, true),
        "{out}"
    );
    let log = std::fs::read_to_string(&n).unwrap_or_default();
    eprintln!(
        "node {install_node} restarted below floor {p1}: {}",
        if log.contains("snapshot_installed") {
            "installed the v1 artifact"
        } else {
            "replayed the journal"
        }
    );

    // ---- Going backwards is refused past a v2 snapshot. Take a v2 snapshot,
    // wait for the v1 set to be retired, then try a v1 service on it.
    let out = c.sh_ok(&["snapshot"]);
    let p2: u64 = out.trim().trim_start_matches("instant=").parse().unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while (0..3).any(|n| c.metric(n, "uc_node_snapshot_floor_bytes") != Some(p2)) {
        assert!(Instant::now() < deadline, "v2 snapshot never completed");
        std::thread::sleep(Duration::from_millis(300));
    }
    assert_eq!(
        kv_image_version(&c.root.join(format!("n2/snapshots/0/snap-{p2}.ultsnap"))),
        2,
        "a v2 image"
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while c
        .root
        .join(format!("n2/snapshots/0/snap-{p1}.ultsnap"))
        .exists()
    {
        assert!(Instant::now() < deadline, "v1 artifact never retired");
        std::thread::sleep(Duration::from_millis(300));
    }
    c.sh_ok(&["stop", "service", "2"]);
    let mut old = Command::new(v1.join("kv-service"))
        .arg("--instance-dir")
        .arg(c.root.join("n2"))
        .arg("--app-id")
        .arg("kv")
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let st = loop {
        if let Some(st) = old.try_wait().unwrap() {
            break st;
        }
        assert!(
            Instant::now() < deadline,
            "a v1 service must refuse a v2 image, not run on it"
        );
        std::thread::sleep(Duration::from_millis(200));
    };
    let mut err = String::new();
    std::io::Read::read_to_string(old.stderr.as_mut().unwrap(), &mut err).unwrap();
    assert!(
        !st.success(),
        "v1 service on a v2 artifact must fail: {st:?}"
    );
    assert!(
        err.contains("unknown kv image version 2"),
        "v1 must refuse by name: {err}"
    );
    eprintln!(
        "rollback refused: v1 kv-service exited {st:?}: {}",
        err.trim()
    );
    // v2 comes back fine and the state is unchanged.
    c.sh_ok(&["start", "service", "2"]);
    let fin = c.wait_leader_digest(Duration::from_secs(60));
    assert_eq!(
        fin, healed,
        "nothing was written since; the state is stable"
    );
}
