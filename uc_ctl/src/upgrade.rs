// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego
//! `uc2ctl upgrade pin` / `upgrade show` (FSM upgrade lifecycle spec §2.5,
//! §3 S4 step 2): stage the 20-byte `UpgradePin` record at
//! `<instance_dir>/upgrade.pending`, sign its digest into the admin line,
//! and submit `ADMIN_OP_UPGRADE_PIN` — `settings apply`'s pipeline
//! verbatim. `show` reads the newest cluster artifact.

use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::Path;

use uc_protocol::identity::{VersionDisplay, pack_version};
use uc_protocol::v2::cnc::ADMIN_OP_UPGRADE_PIN;
use uc_protocol::v2::upgrade::{UpgradePin, encode_upgrade_pin, verdict};

use crate::CommonArgs;

/// `MAJOR.MINOR.PATCH` → the packed version `S::VERSION` carries.
pub fn parse_semver(s: &str) -> Result<u32, String> {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 3 {
        return Err(format!("version {s:?}: expected MAJOR.MINOR.PATCH"));
    }
    let major: u8 = parts[0]
        .parse()
        .map_err(|_| format!("version {s:?}: major must be 0..=255"))?;
    let minor: u8 = parts[1]
        .parse()
        .map_err(|_| format!("version {s:?}: minor must be 0..=255"))?;
    let patch: u16 = parts[2]
        .parse()
        .map_err(|_| format!("version {s:?}: patch must be 0..=65535"))?;
    Ok(pack_version(major, minor, patch))
}

fn stage(instance_dir: &Path, bytes: &[u8]) -> anyhow::Result<std::path::PathBuf> {
    let pending = instance_dir.join(uc_node::UPGRADE_PENDING_FILE);
    let tmp = instance_dir.join(format!("{}.tmp", uc_node::UPGRADE_PENDING_FILE));
    {
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)
            .map_err(|e| anyhow::anyhow!("staging {}: {e}", tmp.display()))?;
        f.write_all(bytes)
            .map_err(|e| anyhow::anyhow!("staging {}: {e}", tmp.display()))?;
        f.sync_all()
            .map_err(|e| anyhow::anyhow!("fsync {}: {e}", tmp.display()))?;
    }
    std::fs::rename(&tmp, &pending)
        .map_err(|e| anyhow::anyhow!("staging {}: {e}", pending.display()))?;
    Ok(pending)
}

pub fn pin(
    common: &CommonArgs,
    row: u8,
    from: Option<&str>,
    to: &str,
    origin: u64,
) -> anyhow::Result<()> {
    if row as usize >= uc_protocol::v2::cnc::CNC_MAX_SERVICES {
        anyhow::bail!("--row must be 0..=7");
    }
    if origin == 0 {
        anyhow::bail!(
            "--origin must be a coordinated instant's position (> 0); run `uc2ctl snapshot` first"
        );
    }
    let to = parse_semver(to).map_err(|e| anyhow::anyhow!("--to: {e}"))?;
    let from = match from {
        Some(s) => parse_semver(s).map_err(|e| anyhow::anyhow!("--from: {e}"))?,
        None => {
            // The row's ATTACHED version word — what the node's own door
            // check (53) compares against when the row has no pin yet.
            let cnc = crate::open(common)?;
            let v = cnc.service_slot(row as usize).status.version();
            if v == 0 {
                anyhow::bail!("row {row} has no attached version word on this node; pass --from");
            }
            v
        }
    };
    let mut bytes = Vec::new();
    encode_upgrade_pin(
        &UpgradePin {
            row,
            from,
            to,
            origin,
        },
        &mut bytes,
    );
    let pending = stage(&common.instance_dir, &bytes)?;
    let (id, ip, port) = uc_node::staged_digest(&bytes);
    let resp = crate::signed_admin_request(
        common,
        ADMIN_OP_UPGRADE_PIN,
        id,
        ip,
        port,
        "cluster position",
    )
    .map_err(|e| anyhow::anyhow!("{e} (staged file kept at {})", pending.display()))?;
    match resp.status {
        0 => {
            println!(
                "pinned: row={row} from={} to={} origin={origin} position={}",
                VersionDisplay(from),
                VersionDisplay(to),
                resp.version
            );
            Ok(())
        }
        1 => {
            println!(
                "refused: {} (cluster position {}) — staged file kept at {}",
                crate::reason_str(resp.reason),
                resp.version,
                pending.display()
            );
            anyhow::bail!("refused: {}", crate::reason_str(resp.reason));
        }
        2 => {
            println!(
                "retry: leader unknown or a previous cluster command is still uncommitted \
                 (cluster position {}) — staged file kept at {}, try again",
                resp.version,
                pending.display()
            );
            anyhow::bail!("retry: try again");
        }
        other => anyhow::bail!("unrecognized response status {other}"),
    }
}

pub fn show(common: &CommonArgs) -> anyhow::Result<()> {
    let Some((position, pins, reports)) =
        uc_node::cluster_agent::read_committed_upgrade(&common.instance_dir)?
    else {
        println!("no cluster artifact yet");
        return Ok(());
    };
    println!("position={position}");
    for row in 0..uc_protocol::v2::cnc::CNC_MAX_SERVICES as u8 {
        let history: Vec<&UpgradePin> = pins.iter().filter(|p| p.row == row).collect();
        if let Some(newest) = history.last() {
            print!(
                "  row={row} pinned={} from={} origin={}",
                VersionDisplay(newest.to),
                VersionDisplay(newest.from),
                newest.origin
            );
            if history.len() > 1 {
                let older: Vec<String> = history[..history.len() - 1]
                    .iter()
                    .map(|p| {
                        format!(
                            "{}->{}@{}",
                            VersionDisplay(p.from),
                            VersionDisplay(p.to),
                            p.origin
                        )
                    })
                    .collect();
                print!(" history=[{}]", older.join(","));
            }
            println!();
        }
        if let Some(r) = reports.iter().find(|r| r.row == row) {
            let v = verdict(r);
            match (v.agreed, v.majority_hash) {
                (true, Some(h)) => println!(
                    "  row={row} hash_verdict=agreed position={} nodes={} hash=0x{h:016x}",
                    r.position,
                    r.hashes.len()
                ),
                (false, Some(h)) => println!(
                    "  row={row} hash_verdict=DIVERGED position={} nodes={} majority=0x{h:016x} minority={:?}",
                    r.position,
                    r.hashes.len(),
                    v.minority
                ),
                (false, None) => println!(
                    "  row={row} hash_verdict=NO_MAJORITY position={} nodes={}",
                    r.position,
                    r.hashes.len()
                ),
                (true, None) => unreachable!("agreed implies a majority"),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_semver_packs_and_refuses() {
        assert_eq!(
            parse_semver("1.2.3"),
            Ok(uc_protocol::identity::pack_version(1, 2, 3))
        );
        assert_eq!(parse_semver("0.0.0"), Ok(0));
        assert!(parse_semver("1.2").is_err());
        assert!(parse_semver("1.2.3.4").is_err());
        assert!(parse_semver("256.0.0").is_err());
        assert!(parse_semver("1.0.65536").is_err());
        assert!(parse_semver("a.b.c").is_err());
    }
}
