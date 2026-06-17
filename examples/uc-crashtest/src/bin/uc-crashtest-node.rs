//! Node-only reference binary for the multi-process hard-crash test. Creates the
//! instance_dir/cnc.dat, runs raft (single-node), waits for the service handshake,
//! then serves clients. Parks until killed (the test SIGKILLs it).
use std::path::PathBuf;

use clap::Parser;
use uc_lincheck::register::RegisterSm;
use uc_node::{
    BootstrapConfig, ClientRingConfig, IpcMode, NodeBuilder, NodeConfig, RaftTuning,
    ServiceRingConfig, TlsConfig,
};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long)]
    data_dir: PathBuf,
    #[arg(long, default_value = "uc-crashtest")]
    app_id: String,
    #[arg(long, default_value = "127.0.0.1:0")]
    raft_addr: String,
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
    let cfg = NodeConfig {
        node_id: 1,
        data_dir: args.data_dir,
        raft_listen_addr: args.raft_addr.parse()?,
        app_id: args.app_id,
        bootstrap: BootstrapConfig::SingleNode,
        raft: RaftTuning::default(),
        tls: TlsConfig::default(),
        transport: uc_node::Transport::Quic,
        ipc_mode: IpcMode::Shmem {
            instance_dir: args.instance_dir,
        },
        client_rings: ClientRingConfig::default(),
        service_rings: ServiceRingConfig::default(),
        log_durability: ultima_journal::Durability::Eventual,
    };
    let _node = NodeBuilder::new(cfg, RegisterSm::default()).start().await?;
    std::future::pending::<()>().await;
    Ok(())
}
