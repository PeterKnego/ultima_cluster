// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc2ctl`'s shared plumbing, split into a library so it has two callers:
//! the `uc2ctl` binary itself (`main.rs`) and — since cluster-FSM plan 1
//! task 8 — an in-tree test that wants to drive a staged-file admin op
//! (`schedule apply`/`show`, `settings apply`/`show`) as an ordinary Rust
//! call instead of shelling out to the built binary
//! (`uc_node/tests/admin_auth.rs`'s settings-apply capstone is the first
//! such caller; `uc_ctl` is its dev-dependency, unversioned like
//! `uc_lincheck`/`uc_sim` are `uc_node`'s — the same asymmetric-cycle idiom
//! `uc_node`'s own `Cargo.toml` already uses for `uc_service`, just with the
//! two crates swapped: `uc_ctl` depends on `uc_node` normally, `uc_node`
//! depends on `uc_ctl` dev-only and unversioned so `cargo package` strips
//! the edge and the cycle exists only in test builds).
//!
//! **This does not change `uc2ctl`'s promised surface.** See `main.rs`'s
//! module doc and `docs/reference/semver-policy.md`: the binary's verbs and
//! exit codes are still the only thing promised to an operator. What lives
//! here is internal wiring an in-tree test needs to reach without a
//! subprocess, not a published API contract — nothing stops it moving or
//! changing shape without notice.
//!
//! [`CommonArgs`] is the `--instance-dir`/`--app-id`/`--admin-key*` flag set
//! every mutating verb shares; [`open`] attaches to a node's cnc page;
//! [`signed_admin_request`] is the write-auth-line/write-request/poll-
//! response round trip `main.rs`'s module doc describes in full;
//! [`reason_str`] renders a wire `reason` code. [`schedule`] and [`settings`]
//! are the two staged-file admin ops (time-and-timers plan 2 / cluster-FSM
//! plan 1 §6).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use uc_crypto::admin::{AdminKey, AdminMessage, sign};
use uc_log::cnc::{AdminAuth, AdminReq, CncPage};

pub mod schedule;
pub mod settings;

/// How long a mutating command polls the response line before giving up.
pub const POLL_TIMEOUT: Duration = Duration::from_secs(10);
pub const POLL_INTERVAL: Duration = Duration::from_millis(20);

#[derive(clap::Args)]
pub struct CommonArgs {
    /// The node's on-disk instance directory (same one passed to `Node::start`).
    #[arg(long)]
    pub instance_dir: PathBuf,
    /// Application identity — must match the running node's `app_id`.
    #[arg(long)]
    pub app_id: String,
    /// M12b: sign this request with a named admin HMAC key (a 32-byte,
    /// mode-0600 key file — see `uc2ctl gen-admin-key`). Required whenever
    /// the node's `[admin]` policy is `hmac`; omit to send an unsigned
    /// request (the legacy `Filesystem` policy accepts it unconditionally,
    /// same as before M12b).
    #[arg(long)]
    pub admin_key: Option<PathBuf>,
    /// The key's name as loaded into the node's `[admin].keys` list.
    /// Defaults to `--admin-key`'s file stem (so naming the file after the
    /// key, e.g. `ops-alice.key`, needs no separate flag).
    #[arg(long)]
    pub admin_key_name: Option<String>,
    /// How long the signature is valid for, counted from the moment
    /// `uc2ctl` signs it. The node refuses a request outside its own
    /// acceptance window (reason 22, `auth_expired`).
    #[arg(long, default_value_t = 30)]
    pub admin_ttl_secs: u64,
}

/// Reason strings for the wire `reason` code (`uc_consensus::config::ProposeError`'s
/// discriminants — see that module for the authoritative table). `0` is not a
/// real `ProposeError`; it is this CLI's own "malformed op" sentinel (the node
/// never emits an op uc2ctl doesn't itself send). 20-24 are M12b's admin-auth
/// reasons (`uc_node::REASON_AUTH_*` / `REASON_AUDIT_FAILED`) — distinct from
/// the `ProposeError` band and only ever seen with `status == 1` (refused).
pub fn reason_str(reason: u32) -> &'static str {
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
        11 => {
            "malformed/unknown op (node didn't recognize the request — CLI/node version mismatch?)"
        }
        12 => {
            "SelfDemote (a leader cannot demote itself; RemoveVoter it and rejoin a fresh id as learner)"
        }
        20 => "auth_missing (the node requires a signed request: pass --admin-key)",
        21 => "auth_bad_tag (wrong key, a stale auth line, or a tampered request)",
        22 => {
            "auth_expired (expired, clock skew between uc2ctl and the node, or --admin-ttl-secs wider than the node's 2×request_ttl_ms window)"
        }
        23 => "auth_unknown_key (this key name is not in the node's [admin].keys)",
        24 => {
            "audit_failed (the node could not record the request — check its audit.jsonl and disk)"
        }
        // Time-and-timers plan 2 (spec §5): `ADMIN_OP_SCHEDULE_APPLY` (wire op
        // 6) refusal reasons — `uc_node::REASON_SCHEDULE_*` (`uc_node::node`).
        40 => {
            "schedule_digest (the staged file changed between staging and applying, or a different file was staged than was signed — re-run `schedule apply`)"
        }
        41 => {
            "schedule_missing (no staged file on this node — was `schedule apply` run against this same instance dir, or already consumed?)"
        }
        42 => "schedule_decode (the staged file is not a decodable schedule table)",
        43 => {
            "schedule_unknown_fsm (an entry names an fsm that is not one of this cluster's declared rows)"
        }
        // Cluster-FSM plan 1 (spec §6): `ADMIN_OP_SETTINGS_APPLY` (wire op
        // 7) refusal reasons — `uc_node::REASON_SETTINGS_*` (`uc_node::node`),
        // the exact twin of the 40-43 band above.
        44 => {
            "settings_digest (the staged file changed between staging and applying, or a different file was staged than was signed — re-run `settings apply`)"
        }
        45 => {
            "settings_missing (no staged file on this node — was `settings apply` run against this same instance dir, or already consumed?)"
        }
        46 => "settings_decode (the staged file is not a decodable settings record)",
        47 => {
            "settings_bounds (a field is out of range — see the node's refusal detail / audit record for which one)"
        }
        _ => "unknown/malformed",
    }
}

/// `SystemTime::now()` as Unix nanoseconds — `uc_node::obs::metrics::now_unix_ns`
/// exists but this crate has no reason to depend on that module for one clock
/// read, so it computes the identical thing directly.
fn unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// The default admin key name when `--admin-key-name` is not given: the key
/// file's stem (`"ops-alice.key"` -> `"ops-alice"`).
pub fn key_name_from_path(path: &Path) -> String {
    path.file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| path.display().to_string())
}

/// Loads `--admin-key` (if given) into an `AdminKey`, under `--admin-key-name`
/// or the file's stem. A load failure (bad permissions, wrong length, missing
/// file) is turned into an anyhow error naming the path and pointing at
/// `gen-admin-key`, and NOTHING is written to the cnc admin band on this path
/// — `signed_admin_request` calls this before touching the page.
fn load_admin_key(common: &CommonArgs) -> anyhow::Result<Option<AdminKey>> {
    let Some(path) = &common.admin_key else {
        return Ok(None);
    };
    let name = common
        .admin_key_name
        .clone()
        .unwrap_or_else(|| key_name_from_path(path));
    let key = AdminKey::load(&name, path).map_err(|e| {
        anyhow::anyhow!(
            "loading admin key {path:?}: {e} (a 0600 32-byte key file; generate with \
             `uc2ctl gen-admin-key`)"
        )
    })?;
    Ok(Some(key))
}

/// Shared admin-band flow: attach, write the admin auth line (M12b — signed
/// under `--admin-key`, or cleared for an unsigned request), write a fresh
/// admin request (`seq = old_seq + 1`, a random nonce), poll the response
/// line, and return it RAW (`status`/`reason`/`version` uninterpreted) —
/// every op means something different by `version` (a config version for
/// ops 1-5, a schedule-table position for op 6, a cluster position for op
/// 7), so interpreting and printing the triple is the caller's job
/// (`main.rs`'s `run_mutate`, `schedule::apply`, `settings::apply`).
/// `Err` only on a bad key file, an attach failure, or a poll timeout.
/// `state_desc` names what `version` means for THIS op ("config version",
/// "schedule position", "cluster position", …) — folded only into the
/// timeout message's `uc2ctl status` pointer.
/// CONTRACT: one admin client (this CLI, m7_gate, or any direct write_admin_req caller) per instance dir at a time; concurrent invocations may produce a nonsense request.
pub fn signed_admin_request(
    common: &CommonArgs,
    op: u32,
    id: u32,
    ip: u32,
    port: u16,
    state_desc: &str,
) -> anyhow::Result<uc_log::cnc::AdminResp> {
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
            let expiry_ns =
                unix_ns().saturating_add(common.admin_ttl_secs.saturating_mul(1_000_000_000));
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
            cnc.write_admin_auth(&AdminAuth {
                tag,
                expiry_ns,
                key_name_hash: key.name_hash,
            });
        }
        None => cnc.write_admin_auth(&AdminAuth::ZERO),
    }
    cnc.write_admin_req(&AdminReq {
        seq,
        nonce,
        op,
        id,
        ip,
        port,
    });

    let result = poll_admin_response_raw(&cnc, seq, state_desc);
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

/// Poll the admin response line for `seq` until it appears or the deadline
/// passes. Returns the response AS-IS for any status (0/1/2/other) —
/// interpreting `status` is the caller's job. `state_desc` (see
/// `signed_admin_request`) names what the timeout message's
/// `uc2ctl status` pointer refers to.
fn poll_admin_response_raw(
    cnc: &CncPage,
    seq: u64,
    state_desc: &str,
) -> anyhow::Result<uc_log::cnc::AdminResp> {
    let deadline = Instant::now() + POLL_TIMEOUT;
    loop {
        if let Some(resp) = cnc.read_admin_resp(seq) {
            return Ok(resp);
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timeout waiting {POLL_TIMEOUT:?} for a response to seq {seq} — a newer admin \
                 request may have superseded this one (only one forward is in flight at a time); \
                 `uc2ctl status` shows the authoritative {state_desc}"
            );
        }
        std::thread::sleep(POLL_INTERVAL);
    }
}

/// Attach to a running node's cnc page by instance dir + app id.
pub fn open(common: &CommonArgs) -> anyhow::Result<Arc<CncPage>> {
    CncPage::open_file(&common.instance_dir.join("cnc2.dat"), &common.app_id)
        .map_err(|e| anyhow::anyhow!("open cnc: {e:?}"))
}
