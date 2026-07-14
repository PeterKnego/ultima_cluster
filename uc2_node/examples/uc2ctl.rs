// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc2ctl`: the M7 admin CLI — the front door for live cluster reconfiguration.
//!
//! Talks to a running node PURELY through its cnc page's admin request/
//! response slots (`uc2_log::cnc::CncPage::{read,write}_admin_{req,resp}`,
//! spec §7 T1): write a fresh request line (fields, then `seq = old_seq + 1`
//! last, with `Release` — the accessor enforces the ordering), poll the
//! response line for the echoed `seq`, print the outcome. No IPC ring, no
//! shmem client SDK — this is the same cross-process control-plane channel a
//! node's own admin-slot poll (`do_work` step 11, `uc2_node::node`) drains,
//! reached here directly by an external operator process.
//!
//! House style matches `m6_gate`'s `probe` role: `CncPage::open_file` +
//! `app_id` check, one JSON-ish/plain-text report line, exit code carries the
//! verdict (`0` = accepted, `1` = refused/timeout/attach error).
//!
//! ```text
//! cargo run -p uc2_node --example uc2ctl -- add-learner --instance-dir D --app-id A --id 4 --addr 127.0.0.1:5004
//! cargo run -p uc2_node --example uc2ctl -- promote        --instance-dir D --app-id A --id 4
//! cargo run -p uc2_node --example uc2ctl -- demote         --instance-dir D --app-id A --id 4
//! cargo run -p uc2_node --example uc2ctl -- remove-learner --instance-dir D --app-id A --id 4
//! cargo run -p uc2_node --example uc2ctl -- remove-voter   --instance-dir D --app-id A --id 4
//! cargo run -p uc2_node --example uc2ctl -- status         --instance-dir D --app-id A
//! ```

use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};

use uc2_log::cnc::{AdminReq, CncPage};
use uc_protocol::v2::cnc::{
    CNC_MAX_PEER_SLOTS, CNC_PEER_ROLE_LEARNER, CNC_PEER_ROLE_VOTER, NODE_FLAG_CAN_SERVE,
    NODE_FLAG_LEADER,
};

/// How long a mutating command polls the response line before giving up.
const POLL_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(20);

#[derive(Parser)]
#[command(name = "uc2ctl", about = "UC v2 M7 admin CLI: live cluster reconfiguration")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Args)]
struct CommonArgs {
    /// The node's on-disk instance directory (same one passed to `Node::start`).
    #[arg(long)]
    instance_dir: PathBuf,
    /// Application identity — must match the running node's `app_id`.
    #[arg(long)]
    app_id: String,
}

#[derive(clap::Args)]
struct AddLearnerArgs {
    #[command(flatten)]
    common: CommonArgs,
    /// The new member's node id.
    #[arg(long)]
    id: u32,
    /// The new member's `ip:port` (its replication-socket bind address).
    #[arg(long)]
    addr: String,
}

#[derive(clap::Args)]
struct IdArgs {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long)]
    id: u32,
}

#[derive(clap::Args)]
struct StatusArgs {
    #[command(flatten)]
    common: CommonArgs,
    /// The cluster's configured admission window in bytes (`NodeConfig::admission_bytes`)
    /// — used only for the staleness warning below (`uc2ctl` cannot read a live
    /// node's in-process config; pass the same value the cluster was started
    /// with). Default matches every reference gate's `256 * 1024`.
    #[arg(long, default_value_t = 256 * 1024)]
    admission_bytes: u64,
}

#[derive(Subcommand)]
enum Cmd {
    /// Add a fresh learner (wire op 1).
    AddLearner(AddLearnerArgs),
    /// Promote a caught-up learner to voter (wire op 2).
    Promote(IdArgs),
    /// Demote a voter to learner (wire op 3).
    Demote(IdArgs),
    /// Permanently remove a learner (tombstoned; wire op 4).
    RemoveLearner(IdArgs),
    /// Permanently remove a voter (tombstoned; wire op 5).
    RemoveVoter(IdArgs),
    /// Print the cluster's current config version/pending state, per-member
    /// peer-slot observability, and leader/serving flags.
    Status(StatusArgs),
}

fn main() {
    let cli = Cli::parse();
    let r = match cli.cmd {
        Cmd::AddLearner(a) => run_mutate(&a.common, 1, a.id, parse_addr(&a.addr)),
        Cmd::Promote(a) => run_mutate(&a.common, 2, a.id, (0, 0)),
        Cmd::Demote(a) => run_mutate(&a.common, 3, a.id, (0, 0)),
        Cmd::RemoveLearner(a) => run_mutate(&a.common, 4, a.id, (0, 0)),
        Cmd::RemoveVoter(a) => run_mutate(&a.common, 5, a.id, (0, 0)),
        Cmd::Status(a) => run_status(&a),
    };
    if let Err(e) = r {
        eprintln!("uc2ctl error: {e}");
        std::process::exit(1);
    }
}

/// Parse `ip:port` into the wire's `(ip: u32, port: u16)` shape (`Ipv4Addr` ->
/// its big-endian `u32` -- the SAME byte order `NodeConfig`'s `SocketAddr` ->
/// wire conversions use elsewhere in this workspace).
fn parse_addr(s: &str) -> (u32, u16) {
    let addr = std::net::SocketAddrV4::from_str(s)
        .unwrap_or_else(|e| panic!("bad --addr {s:?} (want ip:port): {e}"));
    (u32::from(*addr.ip()), addr.port())
}

/// Reason strings for the wire `reason` code (`uc2_consensus::config::ProposeError`'s
/// discriminants — see that module for the authoritative table). `0` is not a
/// real `ProposeError`; it is this CLI's own "malformed op" sentinel (the node
/// never emits an op uc2ctl doesn't itself send).
fn reason_str(reason: u32) -> &'static str {
    match reason {
        1 => "NotLeader",
        2 => "NotServing (single-server-change precondition: a change is still settling)",
        3 => "ChangePending (one membership change in flight at a time)",
        4 => "Tombstoned (this id was permanently removed before; it cannot rejoin)",
        5 => "AlreadyPresent",
        6 => "NotFound",
        7 => "WrongRole (promote a voter / demote a learner)",
        8 => "ZeroVoters (would leave the cluster with no voters)",
        9 => "TooManyMembers (8-member cap)",
        10 => "NotCaughtUp (learner is too far behind commit to promote safely)",
        11 => "malformed/unknown op (node didn't recognize the request — CLI/node version mismatch?)",
        12 => "SelfDemote (a leader cannot demote itself; RemoveVoter it and rejoin a fresh id as learner)",
        _ => "unknown/malformed",
    }
}

/// Shared mutating-command flow: attach, write a fresh admin request (`seq =
/// old_seq + 1`, a random nonce), poll the response line, print + exit.
/// CONTRACT: one admin client (this CLI, m7_gate, or any direct write_admin_req caller) per instance dir at a time; concurrent invocations may produce a nonsense request.
fn run_mutate(common: &CommonArgs, op: u32, id: u32, (ip, port): (u32, u16)) -> anyhow::Result<()> {
    let cnc = open(common)?;

    // The admin band is a single seqlock slot: the current occupant's `seq`
    // (0 if none has ever been written on this cnc-page generation) plus one
    // is our fresh request's seq. `read_admin_req(0)` returns the latest
    // request whenever ANY has been written (seq > 0), which is exactly the
    // value we need — we are not trying to observe a NEW request, just read
    // the slot's current seq.
    let old_seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0);
    let seq = old_seq + 1;
    let nonce = rand::random::<u64>();
    cnc.write_admin_req(&AdminReq { seq, nonce, op, id, ip, port });

    let deadline = Instant::now() + POLL_TIMEOUT;
    loop {
        if let Some(resp) = cnc.read_admin_resp(seq) {
            match resp.status {
                0 => {
                    println!("accepted: config version now {}", resp.version);
                    return Ok(());
                }
                1 => {
                    println!("refused: {} (config version {})", reason_str(resp.reason), resp.version);
                    anyhow::bail!("refused: {}", reason_str(resp.reason));
                }
                2 => {
                    println!(
                        "retry: leader unknown or the append ring was momentarily full \
                         (config version {}) — try again",
                        resp.version
                    );
                    anyhow::bail!("retry: try again");
                }
                other => anyhow::bail!("unrecognized response status {other}"),
            }
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timeout waiting {POLL_TIMEOUT:?} for a response to seq {seq} — a newer admin \
                 request may have superseded this one (only one forward is in flight at a time); \
                 `uc2ctl status` shows the authoritative config version"
            );
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

fn open(common: &CommonArgs) -> anyhow::Result<std::sync::Arc<CncPage>> {
    CncPage::open_file(&common.instance_dir.join("cnc2.dat"), &common.app_id)
        .map_err(|e| anyhow::anyhow!("open cnc: {e:?}"))
}

fn run_status(a: &StatusArgs) -> anyhow::Result<()> {
    let cnc = open(&a.common)?;
    let flags = cnc.status().flags.load_acquire();
    let leader = (flags & NODE_FLAG_LEADER) != 0;
    let can_serve = (flags & NODE_FLAG_CAN_SERVE) != 0;
    let term = cnc.status().term.load_acquire();
    let leader_hint = cnc.status().leader_hint.load_acquire();
    let commit = cnc.counters().commit.load_acquire();
    let durable = cnc.counters().durable.load_acquire();
    let append = cnc.counters().append.load_acquire();

    println!(
        "config: version={} pending={}",
        cnc.config_version(),
        cnc.config_pending() != 0
    );
    println!(
        "role: leader={leader} can_serve={can_serve} term={term} leader_hint={}",
        if leader_hint == u64::MAX { "unknown".to_string() } else { leader_hint.to_string() }
    );
    println!("log: commit={commit} durable={durable} append={append}");
    println!("members:");
    for i in 0..CNC_MAX_PEER_SLOTS {
        let slot = cnc.peer_slot(i);
        let raw = slot.id_and_role.load_acquire();
        if raw == 0 {
            continue; // dormant slot
        }
        let id = (raw >> 8) as u32;
        let role_bits = (raw & 0xff) as u8;
        let role = match role_bits {
            CNC_PEER_ROLE_VOTER => "voter",
            CNC_PEER_ROLE_LEARNER => "learner",
            _ => "unknown",
        };
        let reported_durable = slot.reported_durable.load_acquire();
        // Staleness warning (informational, never blocking): a member whose
        // last reported durable trails commit by more than one admission
        // window may be effectively dark — removing a live voter in that
        // state can stall the cluster (it can no longer ack the removal's own
        // commit). A dormant/never-reported peer reads 0 here, which is
        // caught by the same comparison whenever commit > admission_bytes.
        let behind = commit.saturating_sub(reported_durable);
        let stale = behind > a.admission_bytes;
        let warn = if stale {
            format!(
                " -- STALE: {behind} bytes behind commit (> admission window {}); \
                 removing a live voter while node {id} is dark leaves you stalled",
                a.admission_bytes
            )
        } else {
            String::new()
        };
        println!("  id={id} role={role} reported_durable={reported_durable}{warn}");
    }
    Ok(())
}
