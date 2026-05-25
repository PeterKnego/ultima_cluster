//! uc_autobench CLI. Usage:
//!   auto-bench --task shmem
//!   auto-bench --task shmem --resume <run-id>

use clap::Parser;
use std::path::PathBuf;
use uc_autobench::llm::AnthropicClient;
use uc_autobench::orchestrator::{OrchestratorConfig, run_loop};
use uc_autobench::tasks::shmem::ShmemTask;

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
    if args.resume.is_some() {
        anyhow::bail!("--resume not yet implemented (planned for v1.1)");
    }
    let repo_root = std::env::current_dir()?;
    let git_head = std::process::Command::new("git")
        .arg("rev-parse")
        .arg("HEAD")
        .output()?
        .stdout;
    let git_head = String::from_utf8(git_head)?.trim().to_string();
    let run_id = jiff::Timestamp::now().to_string().replace([':', '.'], "-");

    match args.task.as_str() {
        "shmem" => {
            let task = Box::new(ShmemTask::load()?);
            let client = Box::new(AnthropicClient::from_env()?);
            let cfg = OrchestratorConfig {
                repo_root,
                runs_dir: args.runs_dir,
                run_id,
                git_head,
            };
            let outcome = run_loop(task, client, cfg)?;
            println!(
                "Done. iterations={} best={:?}",
                outcome.iterations_run, outcome.best_variant_id
            );
            Ok(())
        }
        other => anyhow::bail!("unknown task: {other}"),
    }
}
