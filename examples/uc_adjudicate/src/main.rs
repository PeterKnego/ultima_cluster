//! `uc2-adjudicate` — the dogfood adjudication harness CLI. One subcommand
//! per gate row; every subcommand prints one JSON result line on stdout
//! (the evidence pointer a gate cell cites) and a human summary on
//! stderr, and exits 0 PASS / 1 FAIL / 3 NOT RUN / 2 usage or setup error.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use uc_adjudicate::adapter::{self, Adapter};
use uc_adjudicate::report::{Json, Outcome, str_array};
use uc_adjudicate::rig::{Rig, RigCfg};
use uc_adjudicate::{diverge, elle, known_keys, rate, wgl};

#[derive(Parser)]
#[command(
    name = "uc2-adjudicate",
    version,
    about = "Black-box adjudication of a service binary by the repo's checkers"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Clone)]
struct AdapterArgs {
    /// The encoding adapter: which service's wire format to speak.
    #[arg(long, value_parser = clap::builder::PossibleValuesParser::new(adapter::ADAPTER_NAMES))]
    adapter: String,
    /// Application identity every party presents.
    #[arg(long, default_value = "adjudicate")]
    app_id: String,
}

#[derive(Args, Clone)]
struct RigArgs {
    /// A release tarball's `bin/` (uc2-node, uc2-gateway, uc2ctl).
    /// Falls back to `$UC2_BIN_DIR`.
    #[arg(long)]
    uc_bin_dir: Option<PathBuf>,
    /// The service binary under test.
    #[arg(long)]
    service_bin: PathBuf,
    /// Root directory for the run (instance dirs, logs). Must be real disk.
    /// Default: `$HOME/scratch/uc2-adjudicate/<adapter>-<seed>-<unix secs>`.
    #[arg(long)]
    root: Option<PathBuf>,
    /// Voters.
    #[arg(long, default_value_t = 3)]
    nodes: usize,
}

#[derive(Args, Clone)]
struct GatewayArgs {
    /// Every gateway's address, comma-separated.
    #[arg(long, value_delimiter = ',', required = true)]
    gateways: Vec<String>,
}

#[derive(Subcommand)]
enum Cmd {
    /// B2.i/ii (and iii with --churn): per-key WGL under leader kills.
    Wgl {
        #[command(flatten)]
        adapter: AdapterArgs,
        #[command(flatten)]
        rig: RigArgs,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 20)]
        secs: u64,
        #[arg(long, default_value_t = 4)]
        workers: u32,
        #[arg(long, default_value_t = 4)]
        keys: u32,
        #[arg(long, default_value_t = 40)]
        throttle_ms: u64,
        #[arg(long, default_value_t = 3000)]
        kill_period_ms: u64,
        /// Snapshot + purge churn (row iii): instants, below-floor service
        /// restarts, one wipe-and-rejoin, installs observed and counted.
        #[arg(long)]
        churn: bool,
        #[arg(long, default_value_t = 1500)]
        churn_period_ms: u64,
        #[arg(long, default_value_t = uc_lincheck::checker::DEFAULT_BUDGET)]
        budget: u64,
    },
    /// B2-v2.iv: Elle list-append history through the remote path.
    Elle {
        #[command(flatten)]
        adapter: AdapterArgs,
        #[command(flatten)]
        rig: RigArgs,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        #[arg(long, default_value_t = 20)]
        secs: u64,
        #[arg(long, default_value_t = 4)]
        workers: u32,
        #[arg(long, default_value_t = 4)]
        keys: u32,
        #[arg(long, default_value_t = 20)]
        throttle_ms: u64,
        /// Leader kills on this period; 0 = none (the "quiet" pass).
        #[arg(long, default_value_t = 3000)]
        kill_period_ms: u64,
        /// Where `history.edn` goes (never /tmp).
        #[arg(long)]
        out: PathBuf,
    },
    /// B4.ii: the known-key set.
    KnownKeys {
        #[command(subcommand)]
        cmd: KnownKeysCmd,
    },
    /// B4.i: live digest agreement across every gateway's replica.
    Diverge {
        #[command(flatten)]
        adapter: AdapterArgs,
        #[command(flatten)]
        gw: GatewayArgs,
        #[arg(long, default_value_t = 60)]
        timeout_secs: u64,
    },
    /// B4.i offline: key-level difference of two row-0 artifacts.
    DiffSnapshots {
        #[command(flatten)]
        adapter: AdapterArgs,
        a: PathBuf,
        b: PathBuf,
    },
    /// B3 paired arm: steady-window Put rate and latency through one driver.
    Rate {
        #[command(flatten)]
        adapter: AdapterArgs,
        #[command(flatten)]
        gw: GatewayArgs,
        #[arg(long, default_value_t = 4)]
        workers: u32,
        #[arg(long, default_value_t = 64)]
        window: u32,
        #[arg(long, default_value_t = 2)]
        warmup_secs: u64,
        #[arg(long, default_value_t = 8)]
        measure_secs: u64,
        #[arg(long, default_value_t = 1024)]
        keys: u32,
        #[arg(long, default_value_t = 1)]
        seed: u64,
    },
}

#[derive(Subcommand)]
enum KnownKeysCmd {
    /// Write the set through the gateways and record every acknowledged
    /// (key, value, version) to --set.
    Write {
        #[command(flatten)]
        adapter: AdapterArgs,
        #[command(flatten)]
        gw: GatewayArgs,
        #[arg(long)]
        set: PathBuf,
        #[arg(long, default_value_t = 1000)]
        count: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
    },
    /// Read every key in --set back linearizably and compare.
    Verify {
        #[command(flatten)]
        adapter: AdapterArgs,
        #[command(flatten)]
        gw: GatewayArgs,
        #[arg(long)]
        set: PathBuf,
    },
}

fn adapter(a: &AdapterArgs) -> Result<Arc<dyn Adapter>> {
    adapter::by_name(&a.adapter)
        .map(Arc::from)
        .with_context(|| format!("unknown adapter {}", a.adapter))
}

fn default_root(name: &str, seed: u64) -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    PathBuf::from(home)
        .join("scratch")
        .join("uc2-adjudicate")
        .join(format!("{name}-{seed}-{now}"))
}

fn start_rig(
    r: &RigArgs,
    a: &AdapterArgs,
    ad: Arc<dyn Adapter>,
    seed: u64,
    churn: bool,
) -> Result<Rig> {
    let root = r
        .root
        .clone()
        .unwrap_or_else(|| default_root(ad.name(), seed));
    let uc_bin_dir = match &r.uc_bin_dir {
        Some(p) => p.clone(),
        None => PathBuf::from(
            std::env::var("UC2_BIN_DIR").context("--uc-bin-dir or $UC2_BIN_DIR is required")?,
        ),
    };
    eprintln!(
        "[adjudicate] rig root {} (uc bin {})",
        root.display(),
        uc_bin_dir.display()
    );
    Rig::start(RigCfg {
        uc_bin_dir,
        service_bin: r.service_bin.clone(),
        adapter: ad,
        root,
        n: r.nodes,
        app_id: a.app_id.clone(),
        purge: churn,
        buffer_bytes: None,
        journal_segment_bytes: churn.then_some(1 << 20),
        gateway_request_timeout_ms: 2000,
    })
}

fn finish(outcome: Outcome, line: String) -> ! {
    println!("{line}");
    eprintln!("[adjudicate] {}", outcome.label());
    std::process::exit(outcome.exit_code())
}

fn main() {
    match run() {
        Ok(()) => {}
        Err(e) => {
            eprintln!("[adjudicate] error: {e:#}");
            std::process::exit(2);
        }
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Wgl {
            adapter: a,
            rig: r,
            seed,
            secs,
            workers,
            keys,
            throttle_ms,
            kill_period_ms,
            churn,
            churn_period_ms,
            budget,
        } => {
            let ad = adapter(&a)?;
            let rig = start_rig(&r, &a, ad.clone(), seed, churn)?;
            let root = rig.cfg.root.clone();
            let cfg = wgl::WglCfg {
                seed,
                secs,
                workers,
                keys,
                throttle: Duration::from_millis(throttle_ms),
                kill_period: Duration::from_millis(kill_period_ms),
                churn,
                churn_period: Duration::from_millis(churn_period_ms),
                budget,
                request_timeout: Duration::from_secs(15),
            };
            let rep = wgl::run(rig, ad.clone(), &cfg)?;
            let keys_json: Vec<String> = rep
                .per_key
                .iter()
                .map(|k| {
                    Json::new()
                        .str("key", &k.key)
                        .str("verdict", format!("{:?}", k.verdict))
                        .num("spent", k.spent)
                        .num("entries", k.entries)
                        .finish()
                })
                .collect();
            let digests = match &rep.digests {
                None => "null".to_string(),
                Some(Err(e)) => Json::new().str("error", e).finish(),
                Some(Ok(rows)) => {
                    let v: Vec<String> = rows
                        .iter()
                        .map(|(g, d)| {
                            Json::new()
                                .str("gateway", g)
                                .num("count", d.count)
                                .num("digest", d.digest)
                                .num("last_applied", d.last_applied)
                                .finish()
                        })
                        .collect();
                    format!("[{}]", v.join(","))
                }
            };
            let c = &rep.counters;
            let line = Json::new()
                .str("row", if churn { "B2.iii" } else { "B2.i+ii" })
                .str("outcome", rep.outcome.label())
                .str("adapter", ad.name())
                .str("uc", &rep.uc_version)
                .num("seed", seed)
                .num("secs", secs)
                .num("workers", workers)
                .num("ok_ops", rep.ok_ops)
                .num("indeterminate_ops", rep.indeterminate_ops)
                .num("unsent_ops", rep.unsent_ops)
                .num("decode_errors", rep.decode_errors)
                .num("reconnects", rep.reconnects)
                .num("kills", c.kills)
                .num("node_restarts", c.node_restarts)
                .num("restart_timeouts", c.restart_timeouts)
                .num("gw_respawns", c.gw_respawns)
                .num("svc_exits", c.svc_exits)
                .num("node_exits", c.node_exits)
                .num("instants", c.instants)
                .num("instant_failures", c.instant_failures)
                .num("svc_restarts", c.svc_restarts)
                .num("wipes", c.wipes)
                .num("node_installs", rep.node_installs)
                .num("service_installs_inferred", rep.service_installs)
                .raw("per_key", &format!("[{}]", keys_json.join(",")))
                .raw("acked_loss", &str_array(&rep.acked_loss))
                .raw("final_read_failures", &str_array(&rep.final_read_failures))
                .raw("decode_samples", &str_array(&rep.decode_samples))
                .raw("digests", &digests)
                .raw("notes", &str_array(&rep.notes))
                .str("root", root.display().to_string())
                .finish();
            for k in &rep.per_key {
                eprintln!(
                    "[adjudicate] {}: {:?} ({} entries, {} budget)",
                    k.key, k.verdict, k.entries, k.spent
                );
            }
            for n in &rep.notes {
                eprintln!("[adjudicate] note: {n}");
            }
            for l in &rep.acked_loss {
                eprintln!("[adjudicate] ACKED-WRITE LOSS: {l}");
            }
            finish(rep.outcome, line)
        }
        Cmd::Elle {
            adapter: a,
            rig: r,
            seed,
            secs,
            workers,
            keys,
            throttle_ms,
            kill_period_ms,
            out,
        } => {
            let ad = adapter(&a)?;
            let rig = if ad.caps().append {
                Some(start_rig(&r, &a, ad.clone(), seed, false)?)
            } else {
                None
            };
            let cfg = elle::ElleCfg {
                seed,
                secs,
                workers,
                keys,
                throttle: Duration::from_millis(throttle_ms),
                kill_period: (kill_period_ms > 0).then(|| Duration::from_millis(kill_period_ms)),
                out,
                request_timeout: Duration::from_secs(15),
            };
            let rep = elle::run(rig, ad.clone(), &cfg)?;
            let line = Json::new()
                .str("row", "B2-v2.iv")
                .str("outcome", rep.outcome.label())
                .str("adapter", ad.name())
                .num("seed", seed)
                .num("ok", rep.ok)
                .num("completed", rep.completed)
                .num("kills", rep.counters.kills)
                .str(
                    "history",
                    rep.history
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_default(),
                )
                .raw("notes", &str_array(&rep.notes))
                .finish();
            for n in &rep.notes {
                eprintln!("[adjudicate] note: {n}");
            }
            if let Some(h) = &rep.history {
                eprintln!(
                    "[adjudicate] history written: {} — adjudicate with scripts/dogfood_elle.sh {}",
                    h.display(),
                    h.display()
                );
            }
            finish(rep.outcome, line)
        }
        Cmd::KnownKeys { cmd } => match cmd {
            KnownKeysCmd::Write {
                adapter: a,
                gw,
                set,
                count,
                seed,
            } => {
                let ad = adapter(&a)?;
                let rep =
                    known_keys::write(ad.as_ref(), &gw.gateways, &a.app_id, &set, count, seed)?;
                let outcome = if rep.acknowledged > 0 {
                    Outcome::Pass
                } else {
                    Outcome::NotRun
                };
                let line = Json::new()
                    .str("row", "B4.ii-write")
                    .str("outcome", outcome.label())
                    .num("acknowledged", rep.acknowledged)
                    .num("unacknowledged", rep.unacknowledged)
                    .str("set", set.display().to_string())
                    .finish();
                finish(outcome, line)
            }
            KnownKeysCmd::Verify {
                adapter: a,
                gw,
                set,
            } => {
                let ad = adapter(&a)?;
                let rep = known_keys::verify(ad.as_ref(), &gw.gateways, &a.app_id, &set)?;
                let outcome = if rep.checked == 0 {
                    // An empty (or all-blank) set file verifies zero keys; a
                    // PASS there would be vacuous. NOT RUN so the caller knows
                    // the write step must run first.
                    Outcome::NotRun
                } else if !rep.unreadable.is_empty() {
                    Outcome::NotRun
                } else if rep.missing.is_empty() && rep.wrong_value.is_empty() {
                    Outcome::Pass
                } else {
                    Outcome::Fail
                };
                let line = Json::new()
                    .str("row", "B4.ii")
                    .str("outcome", outcome.label())
                    .num("checked", rep.checked)
                    .num("missing", rep.missing.len())
                    .num("wrong_value", rep.wrong_value.len())
                    .num("version_changed", rep.version_changed.len())
                    .num("unreadable", rep.unreadable.len())
                    .raw("missing_keys", &str_array(&rep.missing))
                    .raw("wrong_values", &str_array(&rep.wrong_value))
                    .raw("version_changes", &str_array(&rep.version_changed))
                    .raw("unreadable_keys", &str_array(&rep.unreadable))
                    .finish();
                finish(outcome, line)
            }
        },
        Cmd::Diverge {
            adapter: a,
            gw,
            timeout_secs,
        } => {
            let ad = adapter(&a)?;
            if gw.gateways.len() < 2 {
                // One replica agrees with itself vacuously; divergence needs
                // at least two to compare.
                let line = Json::new()
                    .str("row", "B4.i")
                    .str("outcome", "NOT RUN")
                    .str("error", "need >= 2 gateways to compare replicas")
                    .finish();
                finish(Outcome::NotRun, line)
            }
            let rows = diverge::live_digests(
                ad.as_ref(),
                &gw.gateways,
                &a.app_id,
                Duration::from_secs(timeout_secs),
            );
            let (outcome, line) = match rows {
                Ok(rows) => {
                    let agree = diverge::agree(&rows);
                    let v: Vec<String> = rows
                        .iter()
                        .map(|r| {
                            Json::new()
                                .str("gateway", &r.gateway)
                                .num("count", r.digest.count)
                                .num("digest", r.digest.digest)
                                .num("last_applied", r.digest.last_applied)
                                .finish()
                        })
                        .collect();
                    let o = if agree { Outcome::Pass } else { Outcome::Fail };
                    (
                        o,
                        Json::new()
                            .str("row", "B4.i")
                            .str("outcome", o.label())
                            .raw("replicas", &format!("[{}]", v.join(",")))
                            .finish(),
                    )
                }
                Err(e) => (
                    Outcome::NotRun,
                    Json::new()
                        .str("row", "B4.i")
                        .str("outcome", "NOT RUN")
                        .str("error", format!("{e:#}"))
                        .finish(),
                ),
            };
            finish(outcome, line)
        }
        Cmd::DiffSnapshots {
            adapter: a,
            a: pa,
            b: pb,
        } => {
            let ad = adapter(&a)?;
            let (p1, i1) = diverge::read_artifact(ad.as_ref(), &pa)?;
            let (p2, i2) = diverge::read_artifact(ad.as_ref(), &pb)?;
            if p1 != p2 {
                bail!(
                    "artifacts are at different instants: {p1} vs {p2} — compare a set at ONE position"
                );
            }
            let d = diverge::diff(&i1, &i2);
            let outcome = if d.is_empty() {
                Outcome::Pass
            } else {
                Outcome::Fail
            };
            for l in &d {
                eprintln!("[adjudicate] {l}");
            }
            let line = Json::new()
                .str("row", "B4.i-offline")
                .str("outcome", outcome.label())
                .num("instant", p1)
                .num("entries_a", i1.len())
                .num("entries_b", i2.len())
                .num("differences", d.len())
                .finish();
            finish(outcome, line)
        }
        Cmd::Rate {
            adapter: a,
            gw,
            workers,
            window,
            warmup_secs,
            measure_secs,
            keys,
            seed,
        } => {
            let ad = adapter(&a)?;
            let cfg = rate::RateCfg {
                workers,
                window,
                warmup: Duration::from_secs(warmup_secs),
                measure: Duration::from_secs(measure_secs),
                keys,
                seed,
            };
            let rep = rate::run(ad.clone(), gw.gateways.clone(), a.app_id.clone(), &cfg)?;
            let outcome = if rep.measured_ops > 0 {
                Outcome::Pass
            } else {
                Outcome::NotRun
            };
            let line = Json::new()
                .str("row", "B3-arm")
                .str("outcome", outcome.label())
                .str("adapter", ad.name())
                .num("workers", workers)
                .num("window", window)
                .num("warmup_secs", warmup_secs)
                .num("measure_secs", measure_secs)
                .num("measured_ops", rep.measured_ops)
                .num("ops_per_s", format!("{:.0}", rep.ops_per_s))
                .num("p50_us", rep.p50_us)
                .num("p99_us", rep.p99_us)
                .num("max_us", rep.max_us)
                .num("errors", rep.errors)
                .num("warmup_ops", rep.warmup_ops)
                .str(
                    "driver",
                    "blocking RemoteClient, window of tickets; steady window per m12_gate",
                )
                .finish();
            eprintln!(
                "[adjudicate] {} ops/s, p50 {} us, p99 {} us over {} s (errors {})",
                rep.ops_per_s as u64, rep.p50_us, rep.p99_us, measure_secs, rep.errors
            );
            finish(outcome, line)
        }
    }
}
