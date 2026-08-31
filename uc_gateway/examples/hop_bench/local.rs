// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Dev-box smoke for the hop matrix: spawns this same binary's roles as
//! subprocesses (a sink, optionally the real edge, then one load role) and
//! prints one line per composition. Relative numbers only — everything shares
//! one small box, so a sink, an edge and a driver all contend for the same
//! cores at every point; the fleet driver (`bench-infra/scripts/m13_hop_bench.py`)
//! is where numbers that go in a doc come from.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

#[derive(clap::Args)]
pub struct Args {
    /// Scratch root for the dummy node's instance dir. NOT /tmp (tmpfs) —
    /// defaults under `$HOME/.cache`.
    #[arg(long)]
    pub root: Option<PathBuf>,
    #[arg(long, default_value_t = 3)]
    pub secs: u64,
    #[arg(long, default_value_t = 64)]
    pub payload: usize,
    #[arg(long, default_value_t = 1024)]
    pub inflight: u64,
    /// Comma-separated connection counts for the TCP ladders.
    #[arg(long, default_value = "1,4")]
    pub conns: String,
    #[arg(long, default_value_t = 47101)]
    pub edge_port: u16,
    #[arg(long, default_value_t = 47102)]
    pub dummy_edge_port: u16,
}

struct Sink {
    child: Child,
}

impl Drop for Sink {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Spawn a parking role and wait for its `READY` line (its stdout is echoed
/// with a prefix from a helper thread for the rest of its life).
fn spawn_sink(exe: &PathBuf, label: &'static str, args: &[String]) -> anyhow::Result<Sink> {
    let mut child = Command::new(exe)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| anyhow::anyhow!("spawn {label}: {e}"))?;
    let stdout = child.stdout.take().expect("piped stdout");
    let (tx, rx) = mpsc::channel::<()>();
    std::thread::spawn(move || {
        let mut ready_sent = false;
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if line.trim() == "READY" && !ready_sent {
                ready_sent = true;
                let _ = tx.send(());
            } else if line.starts_with("dummy-") {
                // per-second sink stats: keep the smoke output readable
            } else {
                println!("  [{label}] {line}");
            }
        }
    });
    match rx.recv_timeout(Duration::from_secs(20)) {
        Ok(()) => Ok(Sink { child }),
        Err(_) => {
            let _ = child.kill();
            anyhow::bail!("{label} never printed READY")
        }
    }
}

fn run_load(exe: &PathBuf, label: &str, args: &[String]) -> anyhow::Result<String> {
    let t = Instant::now();
    let out = Command::new(exe).args(args).output()?;
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    let result = stdout
        .lines()
        .find(|l| l.starts_with("RESULT "))
        .map(|s| s.to_string());
    for l in stdout
        .lines()
        .filter(|l| l.starts_with("   ") || l.starts_with("== "))
    {
        println!("  [{label}] {l}");
    }
    if !out.status.success() {
        println!(
            "  [{label}] EXIT {:?} after {:.1}s\n{stderr}",
            out.status.code(),
            t.elapsed().as_secs_f64()
        );
    }
    result.ok_or_else(|| anyhow::anyhow!("{label}: no RESULT line\n{stdout}\n{stderr}"))
}

fn field(result: &str, key: &str) -> String {
    // RESULT lines are flat JSON; pull `"key":value` without a JSON dep.
    let pat = format!("\"{key}\":");
    result
        .find(&pat)
        .map(|i| {
            let rest = &result[i + pat.len()..];
            let end = rest.find([',', '}']).unwrap_or(rest.len());
            rest[..end].trim_matches('"').to_string()
        })
        .unwrap_or_else(|| "-".into())
}

fn s(v: impl ToString) -> String {
    v.to_string()
}

pub fn run(a: Args) -> anyhow::Result<()> {
    let exe = std::env::current_exe()?;
    let root = a.root.clone().unwrap_or_else(|| {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
        PathBuf::from(home).join(".cache").join("uc2-hop-bench")
    });
    let inst = root.join("dummy-node");
    std::fs::create_dir_all(&inst)?;
    let conns: Vec<usize> = a
        .conns
        .split(',')
        .filter_map(|x| x.trim().parse().ok())
        .collect();
    let secs = s(a.secs);
    let payload = s(a.payload);
    let inflight = s(a.inflight);
    let mut rows: Vec<(String, String)> = Vec::new();

    println!(
        "hop_bench local smoke: root {inst:?}, {} s/point, payload {}, inflight {}",
        a.secs, a.payload, a.inflight
    );

    // A: engine-load → dummy-node
    {
        let sink = spawn_sink(
            &exe,
            "dummy-node",
            &[s("dummy-node"), s("--instance-dir"), s(inst.display())],
        )?;
        for engines in [1usize, 2] {
            let label = format!("A hop1 engine→dummy-node engines={engines}");
            let r = run_load(
                &exe,
                &label,
                &[
                    s("engine-load"),
                    s("--instance-dir"),
                    s(inst.display()),
                    s("--secs"),
                    secs.clone(),
                    s("--payload"),
                    payload.clone(),
                    s("--inflight"),
                    inflight.clone(),
                    s("--engines"),
                    s(engines),
                ],
            )?;
            rows.push((label, r));
        }
        drop(sink);
    }

    // B/C: blaster / remote-load → dummy-edge
    {
        let de = format!("127.0.0.1:{}", a.dummy_edge_port);
        let sink = spawn_sink(
            &exe,
            "dummy-edge",
            &[
                s("dummy-edge"),
                s("--listen"),
                de.clone(),
                s("--credits"),
                inflight.clone(),
            ],
        )?;
        for &n in &conns {
            let label = format!("B hop3-floor blaster→dummy-edge conns={n}");
            let r = run_load(
                &exe,
                &label,
                &[
                    s("blaster"),
                    s("--gateway"),
                    de.clone(),
                    s("--secs"),
                    secs.clone(),
                    s("--payload"),
                    payload.clone(),
                    s("--inflight"),
                    inflight.clone(),
                    s("--conns"),
                    s(n),
                ],
            )?;
            rows.push((label, r));
        }
        for &n in &conns {
            let label = format!("C hop3 remote→dummy-edge conns={n}");
            let r = run_load(
                &exe,
                &label,
                &[
                    s("remote-load"),
                    s("--gateways"),
                    de.clone(),
                    s("--secs"),
                    secs.clone(),
                    s("--payload"),
                    payload.clone(),
                    s("--inflight"),
                    inflight.clone(),
                    s("--conns"),
                    s(n),
                ],
            )?;
            rows.push((label, r));
        }
        drop(sink);
    }

    // D/E: blaster / remote-load → edge → dummy-node
    {
        let sink = spawn_sink(
            &exe,
            "dummy-node",
            &[s("dummy-node"), s("--instance-dir"), s(inst.display())],
        )?;
        let listen = format!("127.0.0.1:{}", a.edge_port);
        let edge = spawn_sink(
            &exe,
            "edge",
            &[
                s("edge"),
                s("--instance-dir"),
                s(inst.display()),
                s("--listen"),
                listen.clone(),
                s("--max-inflight"),
                s(65536),
                s("--per-conn-inflight"),
                s(4096),
            ],
        )?;
        for &n in &conns {
            let label = format!("D hop2 blaster→edge→dummy-node conns={n}");
            let r = run_load(
                &exe,
                &label,
                &[
                    s("blaster"),
                    s("--gateway"),
                    listen.clone(),
                    s("--secs"),
                    secs.clone(),
                    s("--payload"),
                    payload.clone(),
                    s("--inflight"),
                    inflight.clone(),
                    s("--conns"),
                    s(n),
                ],
            )?;
            rows.push((label, r));
        }
        for &n in &conns {
            let label = format!("E hop2+3 remote→edge→dummy-node conns={n}");
            let r = run_load(
                &exe,
                &label,
                &[
                    s("remote-load"),
                    s("--gateways"),
                    listen.clone(),
                    s("--secs"),
                    secs.clone(),
                    s("--payload"),
                    payload.clone(),
                    s("--inflight"),
                    inflight.clone(),
                    s("--conns"),
                    s(n),
                ],
            )?;
            rows.push((label, r));
        }
        drop(edge);
        drop(sink);
    }

    println!();
    println!(
        "{:<52} {:>10} {:>8} {:>8} {:>6} {:>6}",
        "point (dev box — relative only)", "resp/s", "p50ms", "p99ms", "lost", "retry"
    );
    for (label, r) in &rows {
        println!(
            "{:<52} {:>10} {:>8} {:>8} {:>6} {:>6}",
            label,
            field(r, "responses_per_sec"),
            field(r, "p50_ms"),
            field(r, "p99_ms"),
            field(r, "lost"),
            field(r, "retried")
        );
    }
    let bad: Vec<&String> = rows
        .iter()
        .filter(|(_, r)| field(r, "lost") != "0")
        .map(|(l, _)| l)
        .collect();
    anyhow::ensure!(bad.is_empty(), "points with lost responses: {bad:?}");
    Ok(())
}
