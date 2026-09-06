// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc2ctl settings apply <file.toml>` / `uc2ctl settings show` (cluster-FSM
//! spec §6, §8) — [`crate::schedule`]'s exact twin over the replicated
//! settings record: three cluster-wide policies (the FSM lag policy, the
//! admission-window byte bound, and the snapshot cadence/target) that used to
//! live per host in `node.toml`.
//!
//! `apply` parses the operator's TOML into a
//! [`uc_protocol::v2::settings::Settings`] (an absent key keeps
//! [`Settings::genesis_default`]'s "derive at use" zero / `Target::All` — this
//! module never invents a value the operator did not write); encodes it and
//! stages it as `<instance_dir>/settings.pending` (temp name, fsync, rename —
//! so the node never reads a half-written file); then sends
//! `ADMIN_OP_SETTINGS_APPLY` through the same signed-request channel every
//! other `uc2ctl` mutating verb uses, carrying the staged file's
//! [`uc_node::staged_digest`] in the request's `(id, ip, port)` fields — the
//! node recomputes the identical digest over the file IT reads back, so the
//! file an operator signed is the file the cluster adopts.
//!
//! `show` reads back the COMMITTED settings from this instance directory's
//! newest cluster artifact (`uc_node::cluster_agent::read_committed_settings`,
//! NOT the staged file — that one is consumed by a successful apply). There
//! is no live, in-process reading in this plan (spec §13 phase 2 is what
//! would add one) — a node whose declared rows have not all snapshotted yet
//! has no cluster artifact, so `show` says so honestly rather than printing
//! a value nothing committed.
//!
//! A refused or timed-out `apply` deliberately leaves the staged file in
//! place: the node only deletes `settings.pending` on a successful append
//! (`Node::apply_settings`), so a retry needs nothing restaged.

use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use serde::Deserialize;

use uc_node::services::{FsmLag, parse_fsm_lag};
use uc_protocol::v2::cnc::ADMIN_OP_SETTINGS_APPLY;
use uc_protocol::v2::settings::{FSM_LAG_LOCKSTEP, Settings, Target, encode_settings};

use crate::CommonArgs;

// ---------------------------------------------------------------- TOML shape

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SettingsFile {
    admission_bytes: Option<u64>,
    fsm_lag: Option<String>,
    snapshot_interval_bytes: Option<u64>,
    snapshot_target: Option<String>,
}

/// TOML text -> a validated [`Settings`]. Every key is optional; an absent
/// key keeps [`Settings::genesis_default`]'s meaning ("derive at use" for
/// `fsm_lag`/`admission_bytes`/`snapshot_interval_bytes`, `Target::All` for
/// `snapshot_target`) — this function never invents a non-zero/non-default
/// value the operator did not write.
///
/// `fsm_lag` reuses [`uc_node::services::parse_fsm_lag`] — `"lockstep"` or a
/// byte count with an optional `KiB`/`MiB`/`GiB` suffix, the exact vocabulary
/// `[services] fsm_lag` in `node.toml` already accepts — mapped onto the
/// wire record's own sentinel (`0` means "derive at use" in THIS record,
/// which is why lockstep needs its own sentinel, [`FSM_LAG_LOCKSTEP`],
/// rather than reusing the cnc page's `0`).
pub fn parse_settings(toml_text: &str) -> Result<Settings, String> {
    let file: SettingsFile = toml::from_str(toml_text).map_err(|e| e.to_string())?;

    let fsm_lag_bytes = match &file.fsm_lag {
        None => 0,
        Some(s) => match parse_fsm_lag(s) {
            Ok(FsmLag::Lockstep) => FSM_LAG_LOCKSTEP,
            Ok(FsmLag::Bounded(b)) => b,
            Err(e) => return Err(format!("fsm_lag: {e}")),
        },
    };
    let snapshot_target = match file.snapshot_target.as_deref() {
        None => Target::All,
        Some("all") => Target::All,
        Some("learners") => Target::Learners,
        Some(other) => {
            return Err(format!(
                "snapshot_target: unknown value {other:?} (want \"all\" or \"learners\")"
            ));
        }
    };

    Ok(Settings {
        admission_bytes: file.admission_bytes.unwrap_or(0),
        fsm_lag_bytes,
        snapshot_interval_bytes: file.snapshot_interval_bytes.unwrap_or(0),
        snapshot_target,
    })
}

// ---------------------------------------------------------------- apply

/// `uc2ctl settings apply <file>`: parse, stage, sign, send. See the module
/// doc for the flow.
pub fn apply(common: &CommonArgs, file: &Path) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(file)
        .map_err(|e| anyhow::anyhow!("reading {}: {e}", file.display()))?;
    let settings = parse_settings(&text).map_err(|e| anyhow::anyhow!("{e}"))?;

    let mut bytes = Vec::new();
    encode_settings(&settings, &mut bytes);

    let pending_path = common.instance_dir.join(uc_node::SETTINGS_PENDING_FILE);
    let tmp_path = common
        .instance_dir
        .join(format!("{}.tmp", uc_node::SETTINGS_PENDING_FILE));
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp_path)
            .map_err(|e| anyhow::anyhow!("staging {}: {e}", tmp_path.display()))?;
        f.write_all(&bytes)
            .map_err(|e| anyhow::anyhow!("staging {}: {e}", tmp_path.display()))?;
        f.sync_all()
            .map_err(|e| anyhow::anyhow!("fsync {}: {e}", tmp_path.display()))?;
    }
    std::fs::rename(&tmp_path, &pending_path)
        .map_err(|e| anyhow::anyhow!("staging {}: {e}", pending_path.display()))?;

    let (id, ip, port) = uc_node::staged_digest(&bytes);
    let resp = crate::signed_admin_request(
        common,
        ADMIN_OP_SETTINGS_APPLY,
        id,
        ip,
        port,
        "cluster position",
    )
    .map_err(|e| anyhow::anyhow!("{e} (staged file kept at {})", pending_path.display()))?;

    match resp.status {
        0 => {
            println!("applied: version={}", resp.version);
            Ok(())
        }
        1 => {
            println!(
                "refused: {} (cluster position {}) — staged file kept at {}",
                crate::reason_str(resp.reason),
                resp.version,
                pending_path.display()
            );
            anyhow::bail!("refused: {}", crate::reason_str(resp.reason));
        }
        2 => {
            println!(
                "retry: leader unknown or a previous cluster command is still uncommitted \
                 (cluster position {}) — staged file kept at {}, try again",
                resp.version,
                pending_path.display()
            );
            anyhow::bail!("retry: try again");
        }
        other => anyhow::bail!("unrecognized response status {other}"),
    }
}

// ---------------------------------------------------------------- show

/// `uc2ctl settings show`: the COMMITTED settings, read out of this instance
/// directory's newest cluster artifact
/// (`uc_node::cluster_agent::read_committed_settings`). `None` (no artifact
/// yet) prints the honest line rather than a value nothing committed.
pub fn show(common: &CommonArgs) -> anyhow::Result<()> {
    let Some((position, settings)) =
        uc_node::cluster_agent::read_committed_settings(&common.instance_dir)
            .map_err(|e| anyhow::anyhow!("reading the cluster artifact: {e}"))?
    else {
        println!("no cluster artifact yet");
        return Ok(());
    };

    // Fully spelled units only for the common cases an operator actually
    // sets ("lockstep", "default", a whole MiB count) — an arbitrary byte
    // count (accepted by `parse_fsm_lag` via bare digits or KiB/GiB) falls
    // back to raw bytes rather than lying about the unit.
    let fsm_lag = match settings.fsm_lag_bytes {
        0 => "default".to_string(),
        FSM_LAG_LOCKSTEP => "lockstep".to_string(),
        b if b.is_multiple_of(1 << 20) => format!("{}MiB", b >> 20),
        b => format!("{b}B"),
    };
    let target = match settings.snapshot_target {
        Target::All => "all",
        Target::Learners => "learners",
    };
    println!(
        "position={position} admission_bytes={} fsm_lag={fsm_lag} \
         snapshot_interval_bytes={} snapshot_target={target}",
        settings.admission_bytes, settings.snapshot_interval_bytes
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_settings_accepts_the_four_keys_and_refuses_unknowns() {
        let s = parse_settings(
            "admission_bytes = 4096\nfsm_lag = \"lockstep\"\nsnapshot_interval_bytes = 10\nsnapshot_target = \"learners\"\n",
        )
        .unwrap();
        assert_eq!(
            s,
            Settings {
                admission_bytes: 4096,
                fsm_lag_bytes: FSM_LAG_LOCKSTEP,
                snapshot_interval_bytes: 10,
                snapshot_target: Target::Learners,
            }
        );
        assert_eq!(parse_settings("").unwrap(), Settings::genesis_default());
        assert!(parse_settings("bogus = 1").unwrap_err().contains("bogus"));
        assert!(
            parse_settings("snapshot_target = \"voters\"")
                .unwrap_err()
                .contains("snapshot_target")
        );
    }

    #[test]
    fn fsm_lag_accepts_byte_counts_and_units() {
        let s = parse_settings("fsm_lag = \"16MiB\"\n").unwrap();
        assert_eq!(s.fsm_lag_bytes, 16 << 20);
        let s = parse_settings("fsm_lag = \"65536\"\n").unwrap();
        assert_eq!(s.fsm_lag_bytes, 65536);
    }

    #[test]
    fn snapshot_target_all_is_accepted_explicitly() {
        let s = parse_settings("snapshot_target = \"all\"\n").unwrap();
        assert_eq!(s.snapshot_target, Target::All);
    }

    #[test]
    fn bad_fsm_lag_is_refused_by_name() {
        let e = parse_settings("fsm_lag = \"bogus\"\n").unwrap_err();
        assert!(e.contains("fsm_lag"), "{e}");
    }

    #[test]
    fn admission_bytes_and_snapshot_interval_default_to_zero() {
        let s = parse_settings("").unwrap();
        assert_eq!(s.admission_bytes, 0);
        assert_eq!(s.snapshot_interval_bytes, 0);
        assert_eq!(s.fsm_lag_bytes, 0);
    }
}
