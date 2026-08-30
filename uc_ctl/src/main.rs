// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc2ctl`: the M7 admin CLI — the front door for live cluster reconfiguration.
//!
//! Talks to a running node PURELY through its cnc page's admin request/
//! response slots (`uc_log::cnc::CncPage::{read,write}_admin_{req,resp}`,
//! spec §7 T1): write a fresh request line (fields, then `seq = old_seq + 1`
//! last, with `Release` — the accessor enforces the ordering), poll the
//! response line for the echoed `seq`, print the outcome. No IPC ring, no
//! shmem client SDK — this is the same cross-process control-plane channel a
//! node's own admin-slot poll (`do_work` step 11, `uc_node::node`) drains,
//! reached here directly by an external operator process.
//!
//! House style matches `m6_gate`'s `probe` role: `CncPage::open_file` +
//! `app_id` check, one JSON-ish/plain-text report line, exit code carries the
//! verdict (`0` = accepted, `1` = refused/timeout/attach error).
//!
//! Runs on the SAME HOST as the node it administers — it reaches the node
//! through shared memory, not the network, so there is no remote mode. The
//! fleet orchestrator ssh's to each host and invokes it locally.
//!
//! ```text
//! uc2ctl add-learner    --instance-dir D --app-id A --id 4 --addr 127.0.0.1:5004
//! uc2ctl promote        --instance-dir D --app-id A --id 4
//! uc2ctl demote         --instance-dir D --app-id A --id 4
//! uc2ctl remove-learner --instance-dir D --app-id A --id 4
//! uc2ctl remove-voter   --instance-dir D --app-id A --id 4
//! uc2ctl status         --instance-dir D --app-id A
//! ```
//!
//! M11 Task 2 adds three OFFLINE verbs (`uc_node::backup`) that do NOT touch
//! a running node's cnc admin band at all — filesystem-only, so there is no
//! `--app-id` (nothing to check it against on disk) and `backup`/`restore`
//! take only `--instance-dir` (no `--app-id` companion, unlike every verb
//! above):
//!
//! ```text
//! uc2ctl backup        --instance-dir D --out ARTIFACT
//! uc2ctl verify-backup  ARTIFACT
//! uc2ctl restore        ARTIFACT --instance-dir D
//! ```
//!
//! M11 Task 4 adds ONE more OFFLINE verb, quorum-loss recovery. Like
//! `backup`/`restore` it never touches a running node's cnc admin band (it
//! takes the instance dir's exclusive flock instead — a running node is
//! refused, not raced) — but unlike them it needs `--app-id` back, purely as
//! a confirmation guard (`--confirm-cluster` must echo it exactly) before an
//! operation that discards data, not as anything checked against a live
//! node:
//!
//! ```text
//! uc2ctl force-single-member --instance-dir D --app-id A --node-id N --confirm-cluster A
//! ```
//!
//! From a source checkout, without installing:
//!
//! ```text
//! cargo run -p uc_ctl -- status --instance-dir D --app-id A
//! ```
//!
//! **Semver:** see `docs/reference/semver-policy.md`. Promised surface: this
//! binary's verbs and its exit codes. It exits `0` on success and non-zero
//! on failure: `1` for any runtime failure (the single `process::exit(1)`
//! in it), `2` for a command-line usage error (clap). The `0` accepted /
//! `1` refused / `2` retry triple is a printed value, not an exit code — it
//! is the response status the node returns and this binary PRINTS —
//! statuses 1 and 2 both exit 1. See `docs/reference/uc2ctl.md` ("Response
//! statuses").
//! Nothing in this crate is a Rust API; it publishes a binary, not a
//! library.

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};

use uc_crypto::admin::{AdminKey, AdminMessage, generate_key_file, sign};
use uc_log::cnc::{AdminAuth, AdminReq, CncPage, unpack_service_status};
use uc_protocol::v2::cnc::{
    CNC_MAX_PEER_SLOTS, CNC_MAX_SERVICES, CNC_PEER_ROLE_LEARNER, CNC_PEER_ROLE_VOTER,
    NODE_FLAG_CAN_SERVE, NODE_FLAG_LEADER,
};

/// How long a mutating command polls the response line before giving up.
const POLL_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(20);

#[derive(Parser)]
#[command(
    name = "uc2ctl",
    version,
    about = "ultima_cluster admin CLI: live cluster reconfiguration"
)]
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
    /// M12b: sign this request with a named admin HMAC key (a 32-byte,
    /// mode-0600 key file — see `uc2ctl gen-admin-key`). Required whenever
    /// the node's `[admin]` policy is `hmac`; omit to send an unsigned
    /// request (the legacy `Filesystem` policy accepts it unconditionally,
    /// same as before M12b).
    #[arg(long)]
    admin_key: Option<PathBuf>,
    /// The key's name as loaded into the node's `[admin].keys` list.
    /// Defaults to `--admin-key`'s file stem (so naming the file after the
    /// key, e.g. `ops-alice.key`, needs no separate flag).
    #[arg(long)]
    admin_key_name: Option<String>,
    /// How long the signature is valid for, counted from the moment
    /// `uc2ctl` signs it. The node refuses a request outside its own
    /// acceptance window (reason 22, `auth_expired`).
    #[arg(long, default_value_t = 30)]
    admin_ttl_secs: u64,
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
    /// Override for the staleness warning's admission window. Since 0.3.0
    /// the node publishes its configured value on the cnc page and this
    /// flag is only needed against pre-0.3.0 nodes (whose page reads 0 —
    /// then this default applies).
    #[arg(long)]
    admission_bytes: Option<u64>,
}

// ---------------------------------------------------------------- M11 Task 2: offline backup

/// `backup`/`restore` deliberately do NOT reuse `CommonArgs`: both are
/// OFFLINE (filesystem-only — see `uc_node::backup`'s module doc), so there
/// is no running node's cnc page to check an `app_id` against, and dragging
/// an unused `--app-id` flag onto these two would be actively misleading
/// (implying a check that never happens). `verify-backup` needs neither flag
/// at all — just the artifact path.
#[derive(clap::Args)]
struct BackupArgs {
    /// The node's on-disk instance directory (same one passed to
    /// `Node::start`). The node MAY be running throughout.
    #[arg(long)]
    instance_dir: PathBuf,
    /// Destination directory for the new backup artifact. Created if absent;
    /// refused if it already exists and is non-empty.
    #[arg(long)]
    out: PathBuf,
}

#[derive(clap::Args)]
struct VerifyBackupArgs {
    /// Path to a backup artifact (as produced by `uc2ctl backup`).
    artifact: PathBuf,
}

#[derive(clap::Args)]
struct RestoreArgs {
    /// Path to a backup artifact (as produced by `uc2ctl backup`).
    artifact: PathBuf,
    /// The instance directory to restore into. Must not already have a
    /// non-empty `journal/`, `state/`, or `snapshots/` — a fresh directory,
    /// or a decommissioned instance dir with those cleared. Volatile
    /// leftovers (`cnc2.dat`, `log.buf`, rings, `instance.lock`) are fine.
    #[arg(long)]
    instance_dir: PathBuf,
}

// ---------------------------------------------------------------- M11 Task 4: quorum-loss

/// OFFLINE (M11 Task 4), like `backup`/`restore` — no cnc admin-band
/// interaction; the instance's exclusive flock stands in for the running-
/// node check instead. `--confirm-cluster` is a confirmation guard, checked
/// ONLY here in the CLI (the `uc_node::recovery::force_single_member`
/// library function has no notion of it): it must equal `--app-id` exactly,
/// or the command refuses before touching anything.
#[derive(clap::Args)]
struct ForceSingleMemberArgs {
    /// The surviving node's own on-disk instance directory. Must NOT have a
    /// node currently running in it.
    #[arg(long)]
    instance_dir: PathBuf,
    /// The cluster's application identity — printed in the data-loss
    /// statement, and echoed by `--confirm-cluster` as the "yes, I mean it"
    /// gate.
    #[arg(long)]
    app_id: String,
    /// The surviving node's own id (must be a voter or learner, and not
    /// tombstoned, in the recovered config).
    #[arg(long)]
    node_id: u32,
    /// Must equal `--app-id` exactly, or the command refuses. Forces an
    /// operator to type the cluster's identity a second time before an
    /// operation that discards data.
    #[arg(long)]
    confirm_cluster: String,
}

// ---------------------------------------------------------------- M12b Task 6: admin auth

#[derive(clap::Args)]
struct GenAdminKeyArgs {
    /// Destination path for the fresh 32-byte key file. Written mode 0600
    /// from creation (no world-readable window); refuses to overwrite an
    /// existing file.
    path: PathBuf,
}

#[derive(clap::Args)]
struct AuditArgs {
    /// The node's on-disk instance directory. OFFLINE: read directly off
    /// disk, no cnc admin-band attach — works whether or not a node is
    /// currently running in it.
    #[arg(long)]
    instance_dir: PathBuf,
    /// Print only the last N records (default: the whole file).
    #[arg(long)]
    tail: Option<usize>,
    /// Print each record's raw JSON line instead of the summarized,
    /// human-readable form.
    #[arg(long)]
    json: bool,
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
    /// OFFLINE (M11 Task 2): ordered-copy an instance directory's durable
    /// journal/state/snapshots into a fresh backup artifact. Filesystem-only
    /// — no cnc admin-band interaction; the node may be running throughout.
    Backup(BackupArgs),
    /// OFFLINE (M11 Task 2): read-only verification of a backup artifact —
    /// recovers its positions, checks the coverage invariant, cross-checks
    /// its `MANIFEST` if present. May heal the ARTIFACT's own torn
    /// active-segment tail (a shrink-only truncate); everything else is
    /// read-only. Filesystem-only, no cnc admin-band interaction.
    VerifyBackup(VerifyBackupArgs),
    /// OFFLINE (M11 Task 2): verify a backup artifact, then copy its three
    /// durable directories into a fresh instance directory. Refuses a target
    /// whose `journal/`, `state/`, or `snapshots/` is already non-empty.
    /// Filesystem-only, no cnc admin-band interaction — the target
    /// `instance_dir` should not have a node running in it.
    Restore(RestoreArgs),
    /// OFFLINE (M11 Task 4): quorum-loss recovery. Forces the given
    /// `--instance-dir` to a single-voter cluster naming `--node-id` the sole
    /// member — the recovered config's other voters/learners are DROPPED
    /// (never tombstoned; they wipe-and-rejoin later as fresh learners).
    /// Prints the data-loss statement, then the resulting `ForceReport`, then
    /// writes. Refuses if a node is running, if `--confirm-cluster` doesn't
    /// echo `--app-id`, if `--node-id` is tombstoned, or if it isn't a member
    /// of the recovered config. A one-way door — there is no "undo" verb.
    ForceSingleMember(ForceSingleMemberArgs),
    /// M12b: generate a fresh named admin HMAC key file (`[admin].keys`
    /// material) — 32 random bytes, mode 0600, refuses to overwrite.
    GenAdminKey(GenAdminKeyArgs),
    /// M12b: print a node's admin audit log
    /// (`<instance_dir>/audit.jsonl`). OFFLINE — reads the file directly,
    /// no cnc admin-band interaction.
    Audit(AuditArgs),
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
        Cmd::Backup(a) => run_backup(&a),
        Cmd::VerifyBackup(a) => run_verify_backup(&a),
        Cmd::Restore(a) => run_restore(&a),
        Cmd::ForceSingleMember(a) => run_force_single_member(&a),
        Cmd::GenAdminKey(a) => run_gen_admin_key(&a),
        Cmd::Audit(a) => run_audit(&a),
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

/// Reason strings for the wire `reason` code (`uc_consensus::config::ProposeError`'s
/// discriminants — see that module for the authoritative table). `0` is not a
/// real `ProposeError`; it is this CLI's own "malformed op" sentinel (the node
/// never emits an op uc2ctl doesn't itself send). 20-24 are M12b's admin-auth
/// reasons (`uc_node::REASON_AUTH_*` / `REASON_AUDIT_FAILED`) — distinct from
/// the `ProposeError` band and only ever seen with `status == 1` (refused).
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
        20 => "auth_missing (the node requires a signed request: pass --admin-key)",
        21 => "auth_bad_tag (wrong key, a stale auth line, or a tampered request)",
        22 => "auth_expired (expired, clock skew between uc2ctl and the node, or --admin-ttl-secs wider than the node's 2×request_ttl_ms window)",
        23 => "auth_unknown_key (this key name is not in the node's [admin].keys)",
        24 => "audit_failed (the node could not record the request — check its audit.jsonl and disk)",
        _ => "unknown/malformed",
    }
}

/// `SystemTime::now()` as Unix nanoseconds — `uc_node::obs::metrics::now_unix_ns`
/// exists but this CLI has no reason to depend on that module for one clock
/// read, so it computes the identical thing directly.
fn unix_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

/// The default admin key name when `--admin-key-name` is not given: the key
/// file's stem (`"ops-alice.key"` -> `"ops-alice"`).
fn key_name_from_path(path: &Path) -> String {
    path.file_stem().map(|s| s.to_string_lossy().to_string()).unwrap_or_else(|| path.display().to_string())
}

/// Loads `--admin-key` (if given) into an `AdminKey`, under `--admin-key-name`
/// or the file's stem. A load failure (bad permissions, wrong length, missing
/// file) is turned into an anyhow error naming the path and pointing at
/// `gen-admin-key`, and NOTHING is written to the cnc admin band on this path
/// — `run_mutate` calls this before touching the page.
fn load_admin_key(common: &CommonArgs) -> anyhow::Result<Option<AdminKey>> {
    let Some(path) = &common.admin_key else {
        return Ok(None);
    };
    let name = common.admin_key_name.clone().unwrap_or_else(|| key_name_from_path(path));
    let key = AdminKey::load(&name, path).map_err(|e| {
        anyhow::anyhow!(
            "loading admin key {path:?}: {e} (a 0600 32-byte key file; generate with \
             `uc2ctl gen-admin-key`)"
        )
    })?;
    Ok(Some(key))
}

/// Shared mutating-command flow: attach, write the admin auth line (M12b —
/// signed under `--admin-key`, or cleared for an unsigned request), write a
/// fresh admin request (`seq = old_seq + 1`, a random nonce), poll the
/// response line, print + exit.
/// CONTRACT: one admin client (this CLI, m7_gate, or any direct write_admin_req caller) per instance dir at a time; concurrent invocations may produce a nonsense request.
fn run_mutate(common: &CommonArgs, op: u32, id: u32, (ip, port): (u32, u16)) -> anyhow::Result<()> {
    let cnc = open(common)?;
    // Load the key (if any) BEFORE writing anything to the admin band — a
    // bad key file must fail cleanly without ever touching the page.
    let key = load_admin_key(common)?;

    // The admin band is a single seqlock slot: the current occupant's `seq`
    // (0 if none has ever been written on this cnc-page generation) plus one
    // is our fresh request's seq. `read_admin_req(0)` returns the latest
    // request whenever ANY has been written (seq > 0), which is exactly the
    // value we need — we are not trying to observe a NEW request, just read
    // the slot's current seq.
    let old_seq = cnc.read_admin_req(0).map(|r| r.seq).unwrap_or(0);
    let seq = old_seq + 1;
    let nonce = rand::random::<u64>();

    // M12b: the auth line MUST be written before the request line — the
    // node's consensus agent treats `write_admin_req`'s `seq` store as the
    // release that publishes both (see `uc_log::cnc::AdminAuth`'s doc).
    match &key {
        Some(key) => {
            let expiry_ns = unix_ns().saturating_add(common.admin_ttl_secs.saturating_mul(1_000_000_000));
            let meta = cnc.meta();
            let msg = AdminMessage {
                app_id: &meta.app_id,
                instance_id: meta.instance_id,
                seq,
                nonce,
                op,
                id,
                ip,
                port,
                expiry_ns,
            };
            let tag = sign(key, &msg);
            cnc.write_admin_auth(&AdminAuth { tag, expiry_ns, key_name_hash: key.name_hash });
        }
        None => cnc.write_admin_auth(&AdminAuth::ZERO),
    }
    cnc.write_admin_req(&AdminReq { seq, nonce, op, id, ip, port });

    let result = poll_admin_response(&cnc, seq);
    // Clear the auth line on EVERY exit path — accepted, refused, retry, or
    // timeout. This is tidiness, not a security control: a stale line CANNOT
    // authenticate anything, because the tag covers `seq` and the next
    // request carries a higher one. Left in place it would merely turn what
    // should be reason 20 (auth_missing) into reason 21 (auth_bad_tag) for a
    // following unsigned request — a confusing diagnostic, not an auth
    // bypass. Clearing it keeps the reason code honest (defence in depth).
    cnc.write_admin_auth(&AdminAuth::ZERO);
    result
}

/// Poll the admin response line for `seq`, print the outcome, and return the
/// verdict as a `Result` — `Ok` only for `status == 0` (accepted).
fn poll_admin_response(cnc: &CncPage, seq: u64) -> anyhow::Result<()> {
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
    let admission_bytes = a.admission_bytes.unwrap_or_else(|| {
        match cnc.admission_bytes() {
            0 => 256 * 1024, // pre-0.3.0 node: fall back to the old default
            v => v,
        }
    });

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
    // M14c (spec §9): the per-service table, straight off page 2 of the page
    // this command already opened. One row per DECLARED id (the bitmask at
    // cnc 4032), including ids nothing has attached to — a declared-but-
    // absent FSM holds min(applied) still, which closes the admission door
    // and caps this node's durable report, so it is exactly the row that
    // explains a stalled cluster. A harness page (declared == 0) prints the
    // header and no rows.
    let declared = cnc.services_declared();
    let fsm_lag = cnc.fsm_lag_bytes();
    let ids: Vec<u8> =
        (0..CNC_MAX_SERVICES as u8).filter(|i| declared & (1u64 << i) != 0).collect();
    // M14c2 T10b: with NOTHING declared there is no lag policy to report. The
    // page still carries a resolved `fsm_lag_bytes` (the node writes it whether
    // or not it declares FSMs), so printing it would name a bound this node
    // paces nothing against — and a page that happened to hold 0 would read as
    // `lockstep`, a policy it does not have. Say `n/a`.
    let lag_desc = if declared == 0 {
        "n/a".to_string()
    } else if fsm_lag == 0 {
        "lockstep".to_string()
    } else {
        format!("{fsm_lag} bytes")
    };
    println!("services: declared={ids:?} fsm_lag={lag_desc}");
    let now_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    for id in ids {
        let s = cnc.service_slot(id as usize);
        let (_, attached, incarnation) = unpack_service_status(s.status.load_acquire());
        let applied = s.applied.load_acquire();
        let hb = s.heartbeat_ns.load_acquire();
        // A never-stamped heartbeat is `never`, not a 55-year age: the
        // slot is zeroed at node boot and only a service ever writes it.
        let age = if hb == 0 {
            "never".to_string()
        } else {
            format!("{:.3}s", now_ns.saturating_sub(hb) as f64 / 1e9)
        };
        println!(
            "  id={id} attached={attached} epoch={} incarnation={incarnation} \
             applied={applied} lag={} snapshot_pos={} heartbeat_age={age}",
            s.epoch.load_acquire(),
            commit.saturating_sub(applied),
            s.snapshot_pos.load_acquire(),
        );
    }
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
        let stale = behind > admission_bytes;
        let warn = if stale {
            format!(
                " -- STALE: {behind} bytes behind commit (> admission window {}); \
                 removing a live voter while node {id} is dark leaves you stalled",
                admission_bytes
            )
        } else {
            String::new()
        };
        println!("  id={id} role={role} reported_durable={reported_durable}{warn}");
    }
    Ok(())
}

// ---------------------------------------------------------------- M11 Task 2: offline backup

/// `BackupReport` printed as `key=value` lines, one per field, same
/// convention as `run_status`'s output — no serde_json in this workspace.
fn print_backup_report(r: &uc_node::backup::BackupReport) {
    println!("journal_first_base={}", r.journal_first_base);
    println!("journal_last_pos={}", r.journal_last_pos);
    for id in 0..CNC_MAX_SERVICES {
        if let Some(pos) = r.newest_snapshots[id] {
            println!("newest_snapshot.{id}={pos}");
        }
    }
    println!(
        "newest_snapshot={}",
        r.newest_snapshot().map(|p| p.to_string()).unwrap_or_else(|| "none".to_string())
    );
    println!("snapshot_floor={}", r.snapshot_floor);
    println!("healed_torn_tail={}", r.healed_torn_tail);
    println!("files={}", r.files);
}

fn run_backup(a: &BackupArgs) -> anyhow::Result<()> {
    let report = uc_node::backup::backup_instance(&a.instance_dir, &a.out)
        .map_err(|e| anyhow::anyhow!("backup: {e}"))?;
    print_backup_report(&report);
    Ok(())
}

fn run_verify_backup(a: &VerifyBackupArgs) -> anyhow::Result<()> {
    let report = uc_node::backup::verify_artifact(&a.artifact)
        .map_err(|e| anyhow::anyhow!("verify-backup: {e}"))?;
    print_backup_report(&report);
    Ok(())
}

fn run_restore(a: &RestoreArgs) -> anyhow::Result<()> {
    let report = uc_node::backup::restore_artifact(&a.artifact, &a.instance_dir)
        .map_err(|e| anyhow::anyhow!("restore: {e}"))?;
    print_backup_report(&report);
    Ok(())
}

// ---------------------------------------------------------------- M11 Task 4: quorum-loss

/// Fix round 1, Important 5: the surviving node's own on-disk instance dir
/// is identified purely by the operator-supplied `--instance-dir`; a wrong
/// `--node-id`/`--app-id` typed against the RIGHT directory (or vice versa)
/// would otherwise be forced silently. Cross-check them against a leftover
/// `cnc2.dat`'s own `meta()` — WHEN ONE IS PRESENT. `cnc2.dat` is volatile
/// (boot recreates it unconditionally, per `docs/reference/instance-directory.md`'s
/// durable/volatile split), so its absence is expected on a truly fresh
/// dir, or one whose cnc file was already cleaned up — not an error, just
/// nothing to cross-check (printed, not silent). Its PRESENCE, though — the
/// ordinary case for a survivor that was just SIGKILLed or cleanly stopped —
/// is a strong, free signal: `CncPage::open_file` already validates
/// magic/crc/version and the `app_id` match (returning the actual `app_id`
/// on mismatch via `CncError::AppIdMismatch`, exactly what `run_status`
/// relies on for every other command), so this reuses that same call rather
/// than reimplementing header decoding.
fn check_leftover_cnc_matches(a: &ForceSingleMemberArgs) -> anyhow::Result<()> {
    let cnc_path = a.instance_dir.join("cnc2.dat");
    if !cnc_path.is_file() {
        println!(
            "note: no leftover cnc2.dat in {:?} (volatile — recreated by boot); skipping the \
             --node-id/--app-id cross-check",
            a.instance_dir
        );
        return Ok(());
    }
    match CncPage::open_file(&cnc_path, &a.app_id) {
        Ok(cnc) => {
            let leftover_id = cnc.meta().node_id;
            if leftover_id != a.node_id {
                anyhow::bail!(
                    "refusing: leftover cnc2.dat in {:?} belongs to node_id {leftover_id}, not \
                     --node-id {} — wrong instance dir?",
                    a.instance_dir,
                    a.node_id
                );
            }
            Ok(())
        }
        Err(uc_log::cnc::CncError::AppIdMismatch { expected, actual }) => {
            anyhow::bail!(
                "refusing: leftover cnc2.dat in {:?} belongs to app_id {actual:?}, not --app-id \
                 {expected:?} — wrong instance dir?",
                a.instance_dir
            )
        }
        Err(e) => {
            println!(
                "note: leftover cnc2.dat in {:?} could not be read ({e}) — skipping the \
                 --node-id/--app-id cross-check",
                a.instance_dir
            );
            Ok(())
        }
    }
}

/// Fix round 1, Important 6: ONE `uc_node::recovery::plan_force_single_member`
/// call acquires the instance flock, reads the recovered config, and builds
/// the (not-yet-written) forced record — all under that single lock. The
/// data-loss statement is printed from the resulting `PlannedForce` (which
/// keeps holding the lock), then `PlannedForce::commit` performs the one
/// write, still under the SAME lock. There is no separate `recovered_config`
/// call and no second lock/open/recover cycle in between — unlike the
/// original version of this function, this is no longer "read, release,
/// re-acquire, write."
fn run_force_single_member(a: &ForceSingleMemberArgs) -> anyhow::Result<()> {
    if a.confirm_cluster != a.app_id {
        anyhow::bail!(
            "refusing: --confirm-cluster {:?} does not match --app-id {:?} — this is a \
             one-way, data-losing operation; type the cluster's app-id again to confirm",
            a.confirm_cluster,
            a.app_id
        );
    }

    check_leftover_cnc_matches(a)?;

    let planned = uc_node::recovery::plan_force_single_member(&a.instance_dir, a.node_id)
        .map_err(|e| anyhow::anyhow!("force-single-member: {e}"))?;
    println!("{}", planned.data_loss_statement());

    let report = planned.commit().map_err(|e| anyhow::anyhow!("force-single-member: {e}"))?;
    println!("old_version={}", report.old_version);
    println!("new_version={}", report.new_version);
    println!("durable={}", report.durable);
    println!("dropped_peers={:?}", report.dropped_peers);
    Ok(())
}

// ---------------------------------------------------------------- M12b Task 6: admin auth

fn run_gen_admin_key(a: &GenAdminKeyArgs) -> anyhow::Result<()> {
    generate_key_file(&a.path).map_err(|e| anyhow::anyhow!("gen-admin-key: {e}"))?;
    let abs = std::fs::canonicalize(&a.path).unwrap_or_else(|_| a.path.clone());
    let name = key_name_from_path(&a.path);
    println!("wrote {}", abs.display());
    println!("paste into the node's config file:");
    println!("[admin]");
    println!("keys = [{{ name = \"{name}\", key_path = \"{}\" }}]", abs.display());
    Ok(())
}

/// One field's value out of a flat, single-line JSON object, as decoded
/// text (`"ops-alice"` -> `ops-alice`, `20` -> `20`, `null` -> `null`).
/// `None` on anything that doesn't look like `"key":value` — this is a
/// deliberately small hand parser, not a JSON parser: it works because the
/// audit line's shape is pinned (`uc_node::obs::log::format_line_at`, one
/// flat object, fixed key order — see `uc_node/src/audit.rs`'s module
/// doc), not because this function understands JSON in general.
///
/// **Escape-aware, deliberately conservative** (fix round 1, Important 1):
/// a string value is decoded through [`decode_json_string`], which returns
/// `None` on any escape it does not recognize or an unterminated string —
/// so an admin key named `ops"alice` (which `format_line_at` renders as
/// `ops\"alice` per its own `push_json_escaped`) is either decoded back to
/// `ops"alice` correctly or, on anything this parser cannot make sense of,
/// yields `None` for the WHOLE line (via `format_audit_line`'s `?`), never
/// a silently truncated value. Never panics: every step is a checked
/// lookup, so a garbled line simply yields `None`.
fn json_field(line: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\":");
    let start = line.find(&needle)? + needle.len();
    let rest = line.get(start..)?;
    if let Some(stripped) = rest.strip_prefix('"') {
        decode_json_string(stripped)
    } else {
        let end = rest.find([',', '}'])?;
        Some(rest[..end].to_string())
    }
}

/// Decode a JSON string body — the characters immediately AFTER the opening
/// `"`, up to and including its closing `"` — into the field's text value.
/// Handles exactly the escapes `push_json_escaped`
/// (`uc_node/src/obs/log.rs`) can emit (`\"`, `\\`, `\u00XX` for control
/// bytes) plus the standard `\n`/`\r`/`\t` shorthands for generality; any
/// other escape, or running out of input before a closing `"`, returns
/// `None` rather than guess.
fn decode_json_string(s: &str) -> Option<String> {
    let mut out = String::new();
    let mut chars = s.chars();
    loop {
        match chars.next()? {
            '"' => return Some(out),
            '\\' => match chars.next()? {
                '"' => out.push('"'),
                '\\' => out.push('\\'),
                '/' => out.push('/'),
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                't' => out.push('\t'),
                'u' => {
                    let hex: String = (0..4).map(|_| chars.next()).collect::<Option<String>>()?;
                    let code_point = u32::from_str_radix(&hex, 16).ok()?;
                    out.push(char::from_u32(code_point)?);
                }
                _ => return None,
            },
            c => out.push(c),
        }
    }
}

/// Render one audit line in the summarized, human-readable form. `None` if
/// any field this format needs is missing — the caller falls back to
/// printing the raw line with a `?` marker rather than a partial/misleading
/// summary.
fn format_audit_line(line: &str) -> Option<String> {
    let ts = json_field(line, "ts_ns")?;
    let actor = json_field(line, "actor")?;
    let origin = json_field(line, "origin")?;
    let op_name = json_field(line, "op_name")?;
    let id = json_field(line, "id")?;
    // `addr` is either a quoted "ip:port" string or the literal JSON `null`
    // (unquoted) — `json_field`'s unquoted branch already returns "null" as
    // text for that case, which is exactly what should print.
    let addr = json_field(line, "addr")?;
    let outcome = json_field(line, "outcome")?;
    let reason = json_field(line, "reason")?;
    let cfg = json_field(line, "config_version")?;
    Some(format!("{ts}  {actor}  {origin}  {op_name}  {id}  {addr}  {outcome}({reason})  cfg={cfg}"))
}

fn run_audit(a: &AuditArgs) -> anyhow::Result<()> {
    let path = a.instance_dir.join("audit.jsonl");
    let text = match std::fs::read_to_string(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            println!("no audit log at {}", path.display());
            return Ok(());
        }
        Err(e) => return Err(anyhow::anyhow!("reading {}: {e}", path.display())),
    };

    let mut lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    if let Some(n) = a.tail {
        let skip = lines.len().saturating_sub(n);
        lines = lines[skip..].to_vec();
    }

    for line in lines {
        if a.json {
            println!("{line}");
            continue;
        }
        match format_audit_line(line) {
            Some(s) => println!("{s}"),
            // A malformed line is printed as-is with a marker rather than
            // dropped or panicked on — the audit log's whole point is never
            // to lose a record silently, including one this parser can't
            // fully make sense of.
            None => println!("? {line}"),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Fix round 1, Important 1: json_field must be escape-aware, not stop
    // at the first raw `"` — a value containing an escaped quote or
    // backslash must decode correctly, and anything this small parser
    // cannot make sense of must come back `None` (so the caller falls back
    // to printing the raw line with a `?` marker) rather than a silently
    // truncated value.

    #[test]
    fn json_field_reads_a_plain_string_value() {
        let line = r#"{"ts_ns":1,"actor":"ops-alice","outcome":"accepted"}"#;
        assert_eq!(json_field(line, "actor").as_deref(), Some("ops-alice"));
        assert_eq!(json_field(line, "outcome").as_deref(), Some("accepted"));
    }

    #[test]
    fn json_field_decodes_escaped_quote_and_backslash() {
        // What `push_json_escaped` (uc_node/src/obs/log.rs) emits for an
        // admin key literally named `ops"alice\bob`.
        let line = r#"{"actor":"ops\"alice\\bob","outcome":"accepted"}"#;
        assert_eq!(json_field(line, "actor").as_deref(), Some(r#"ops"alice\bob"#));
    }

    #[test]
    fn json_field_decodes_a_unicode_escape() {
        // \u0041 is "A" — exercises the `\u00XX` branch `push_json_escaped`
        // uses for control bytes, at a codepoint that's easy to eyeball.
        // `"\\u0041"` in this ordinary Rust string literal decodes to the
        // literal six characters `\u0041`, so `json_field` receives a real
        // backslash-escape to decode, not an already-resolved "A".
        let line = "{\"actor\":\"\\u0041\"}";
        assert_eq!(json_field(line, "actor").as_deref(), Some("A"));
    }

    #[test]
    fn json_field_returns_none_on_an_unterminated_string() {
        let line = r#"{"actor":"ops-alice"#; // no closing quote
        assert_eq!(json_field(line, "actor"), None);
    }

    #[test]
    fn json_field_returns_none_on_an_unterminated_escape() {
        let line = r#"{"actor":"ops-alice\"#; // trailing backslash, nothing after it
        assert_eq!(json_field(line, "actor"), None);
    }

    #[test]
    fn json_field_returns_none_on_an_unrecognized_escape() {
        let line = r#"{"actor":"ops\qalice"}"#; // \q is not an escape this parser knows
        assert_eq!(json_field(line, "actor"), None);
    }

    #[test]
    fn json_field_reads_a_numeric_field() {
        let line = r#"{"ts_ns":1755600000000000000,"reason":20,"config_version":7}"#;
        assert_eq!(json_field(line, "ts_ns").as_deref(), Some("1755600000000000000"));
        assert_eq!(json_field(line, "reason").as_deref(), Some("20"));
        assert_eq!(json_field(line, "config_version").as_deref(), Some("7"));
    }

    #[test]
    fn json_field_reads_the_literal_null() {
        let line = r#"{"addr":null,"id":4}"#;
        assert_eq!(json_field(line, "addr").as_deref(), Some("null"));
    }

    #[test]
    fn format_audit_line_falls_back_to_none_when_a_field_cannot_be_decoded() {
        // `\q` is not an escape this parser knows — a single bad escape in
        // ONE field (here `actor`) must fail the WHOLE line via
        // `format_audit_line`'s `?` chain, so the caller prints `? <line>`
        // instead of a record with every other field intact but `actor`
        // silently wrong.
        let line = r#"{"ts_ns":1,"actor":"ops\qalice","origin":"local","op_name":"add_learner","id":4,"addr":null,"outcome":"accepted","reason":0,"config_version":1}"#;
        assert_eq!(format_audit_line(line), None);
    }
}
