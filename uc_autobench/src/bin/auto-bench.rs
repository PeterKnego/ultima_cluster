//! uc_autobench CLI. Usage:
//!   auto-bench --task shmem
//!   auto-bench --task shmem --resume <run-id>

use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    /// Task id (matches a tasks/<id>/task.toml).
    #[arg(long)]
    task: String,
    /// Resume a previous run by run-id (must exist under runs_dir/<task>/<run-id>).
    #[arg(long)]
    resume: Option<String>,
    /// Where run artifacts are written (default: auto-bench-runs/).
    #[arg(long, default_value = "auto-bench-runs")]
    runs_dir: PathBuf,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("uc_autobench=info")),
        )
        .init();
    let args = Args::parse();
    match args.task.as_str() {
        "shmem" => {
            // Filled in by Task 18.
            anyhow::bail!("shmem task not yet registered (filled in by Task 18)");
        }
        other => anyhow::bail!("unknown task: {other}"),
    }
}
