//! Service-only reference binary. Waits for the node's cnc.dat, attaches, runs the
//! in-memory RegisterSm. Parks until killed (the test SIGKILLs it mid-apply).
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use uc_lincheck::register::RegisterSm;
use uc_service::ServiceBuilder;
use uc_service::runtime::ServiceConfig;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long)]
    data_dir: PathBuf,
    #[arg(long, default_value = "uc-crashtest")]
    app_id: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    std::fs::create_dir_all(&args.data_dir)?;
    let cnc = args.instance_dir.join("cnc.dat");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !cnc.exists() {
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "timed out waiting for cnc.dat"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let cfg = ServiceConfig {
        instance_dir: args.instance_dir,
        app_id: args.app_id,
        data_dir: args.data_dir,
        ..ServiceConfig::default()
    };
    let _svc = ServiceBuilder::new(cfg, RegisterSm::default()).run().await?;
    std::future::pending::<()>().await;
    Ok(())
}
