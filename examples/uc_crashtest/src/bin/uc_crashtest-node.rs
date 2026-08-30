// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Node-only reference binary for the v2 multi-process hard-crash test
//! (M5 Task 14, spec §8 L3). Creates the instance_dir's `cnc2.dat` +
//! shared-memory IPC rings, runs the v2 sync node (a single-node cluster by
//! default), then parks until killed (the test SIGKILLs it).
//!
//! Unlike the v1 `uc-crashtest-node` binary, this has no `#[tokio::main]` —
//! the v2 stack is entirely synchronous (polling agents on OS threads):
//! `Node::start` spawns its agent threads and returns immediately, so `main`
//! just has to stay alive for them.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use uc_consensus::election::NodeId;
use uc_net::fault::FaultConfig;
use uc_node::{CryptoConfig, Node, NodeConfig};

#[derive(Parser)]
struct Args {
    #[arg(long)]
    instance_dir: PathBuf,
    #[arg(long, default_value = "uc_crashtest")]
    app_id: String,
    #[arg(long, default_value = "127.0.0.1:0")]
    bind: SocketAddr,
    #[arg(long, default_value_t = 0)]
    id: NodeId,
    /// Comma-separated `id@addr` member list (every member INCLUDING self).
    /// Defaults to a single-node cluster of just `--id` at `--bind`.
    #[arg(long)]
    members: Option<String>,
    /// M8 Task 15 (UC2_CRYPTO=1 path): this node's X25519 private key file.
    /// Must be paired with `--crypto-allowlist`; when either is absent the
    /// node boots with `CryptoConfig::Disabled` (the pre-M8 default).
    #[arg(long)]
    crypto_key: Option<PathBuf>,
    /// M8 Task 15: the shared allowlist naming every trusted peer's public
    /// key. See `--crypto-key`.
    #[arg(long)]
    crypto_allowlist: Option<PathBuf>,
    /// M14c2: declared FSM ids (`0,1`). Absent = `{0}`.
    #[arg(long)]
    services: Option<String>,
    /// M14c2: `lockstep` or a byte bound.
    #[arg(long)]
    fsm_lag: Option<String>,
}

fn parse_members(s: &str) -> Vec<(NodeId, SocketAddr)> {
    s.split(',')
        .map(|part| {
            let (id, addr) = part
                .split_once('@')
                .unwrap_or_else(|| panic!("bad --members entry {part:?}, expected id@addr"));
            let id: NodeId = id.parse().unwrap_or_else(|e| panic!("bad member id {id:?}: {e}"));
            let addr: SocketAddr =
                addr.parse().unwrap_or_else(|e| panic!("bad member addr {addr:?}: {e}"));
            (id, addr)
        })
        .collect()
}

/// A distinct, id-derived election seed (M7 Task 10: the multi-node
/// crashtest cluster needs every node's randomized election timeout to
/// differ, or all processes racing the identical xorshift stream livelock
/// on simultaneous elections). `id == 0` reproduces the historical fixed
/// `seed: 1` byte-for-byte (`1 ^ (0).wrapping_mul(..) == 1`), so the
/// existing single-node tests (default `--id 0`) are unaffected.
fn seed_for(id: uc_consensus::election::NodeId) -> u64 {
    1 ^ (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let members = match &args.members {
        Some(s) => parse_members(s),
        None => vec![(args.id, args.bind)],
    };

    // M8 Task 15: crypto ON iff BOTH a key and an allowlist were given.
    let crypto = match (&args.crypto_key, &args.crypto_allowlist) {
        (Some(key_path), Some(allowlist_path)) => CryptoConfig::Enabled {
            key_path: key_path.clone(),
            allowlist_path: allowlist_path.clone(),
            rotation: uc_crypto::rotation::RotationPolicy::default(),
        },
        _ => CryptoConfig::Disabled,
    };
    let crypto_enabled = matches!(crypto, CryptoConfig::Enabled { .. });

    let instance_dir = args.instance_dir.clone();
    let cfg = NodeConfig {
        id: args.id,
        members,
        bind: args.bind,
        instance_dir: args.instance_dir,
        app_id: args.app_id,
        buffer_bytes: 1 << 22, // 4 MiB
        max_payload: 256,
        admission_bytes: 256 * 1024,
        election_timeout_min_ns: 150_000_000,
        election_timeout_max_ns: 300_000_000,
        seed: seed_for(args.id),
        faults: FaultConfig::default(),
        purge: uc_node::PurgePolicy::Disabled,
        learners: Vec::new(),
        journal_segment_bytes: uc_node::DEFAULT_JOURNAL_SEGMENT_BYTES,
        crypto,
        services: uc_node::ServicesConfig::from_cli(args.services.as_deref(), args.fsm_lag.as_deref())
            .unwrap_or_else(|e| {
                eprintln!("{e}");
                std::process::exit(2)
            }),
    };

    let node = Node::start(cfg)?;
    // Anti-vacuity signal (M8 Task 15): a real crypto epoch is a LEADER-ONLY
    // mint (`Node::crypto_epoch`'s doc), so this only fires once this process
    // actually wins an election under crypto — proof wire crypto genuinely
    // engaged, not merely that `--crypto-key`/`--crypto-allowlist` were
    // parsed. The test harness (a SEPARATE process, no `Node` handle) polls
    // for this sentinel file on disk rather than piping/parsing stdout.
    // Harmless overhead when crypto is disabled (`crypto_epoch()` always
    // `None` then, so the loop body below never writes).
    let mut last_epoch: Option<u16> = None;
    loop {
        if crypto_enabled
            && let Some(e) = node.crypto_epoch()
            && last_epoch != Some(e)
        {
            let _ = std::fs::write(instance_dir.join("crypto_epoch_active"), e.to_string());
            last_epoch = Some(e);
        }
        // Not a park(): this process is torn down by a real SIGKILL from the
        // test harness, but while alive it polls its own crypto epoch.
        std::thread::sleep(Duration::from_millis(100));
    }
}
