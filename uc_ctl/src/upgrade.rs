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

use uc_log::cnc::{CncPage, RowRead};
use uc_protocol::identity::{VersionDisplay, pack_version, same_line, unpack_version};
use uc_protocol::v2::cnc::ADMIN_OP_UPGRADE_PIN;
use uc_protocol::v2::upgrade::{UpgradePin, encode_upgrade_pin, verdict};

use crate::CommonArgs;

/// `--from`'s default (spec §8): the row's RUNNING version off the local
/// page's row view when a genesis or pin record has set one, falling back
/// to the ATTACHED version word otherwise — the only signal before a row's
/// first record lands, and this crate's pre-#33-task-10 behaviour.
/// `RowRead::Contended` (the single-writer cluster agent mid-store;
/// effectively unreachable) falls back the same way: a reader that must
/// decide never fabricates a running version out of a contended read.
fn default_from(cnc: &CncPage, row: u8) -> Option<u32> {
    let slot = cnc.service_slot(row as usize);
    if let RowRead::View {
        running: Some(v), ..
    } = slot.status.row_view()
    {
        return Some(v);
    }
    let attached = slot.status.version();
    (attached != 0).then_some(attached)
}

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
    patch: bool,
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
    // Packed `0` is the "unversioned" sentinel — what an unversioned row's
    // cnc word reads and what `status` prints as `unversioned`. Pinning TO
    // it would make the pin indistinguishable from "no pin" at the cnc
    // words. Refused here, beside `--origin 0`, rather than at the node.
    if to == 0 {
        anyhow::bail!(
            "--to must be a real version: 0.0.0 packs to 0, the \"unversioned\" sentinel"
        );
    }
    let from = match from {
        Some(s) => parse_semver(s).map_err(|e| anyhow::anyhow!("--from: {e}"))?,
        None => {
            // #33 spec §8: prefer the row's RUNNING version (the door check
            // (53) now compares `from` against it whenever one is
            // recorded); fall back to the ATTACHED version word when the
            // row has no running version yet.
            let cnc = crate::open(common)?;
            default_from(&cnc, row).ok_or_else(|| {
                anyhow::anyhow!(
                    "row {row} has no running version and no attached version word on this \
                     node; pass --from"
                )
            })?
        }
    };
    // #33 final review I1(a): a same-line pin (equal major.minor, patch
    // ignored — D3) sets the running version but stops nothing, so it does
    // NOT refuse the old build. That is right for a real patch release and
    // wrong for everything else — most sharply for a bare-integer
    // `const VERSION` (1, 2, 3 …), which packs as 0.0.x, so every such build
    // is one line and a "1 -> 2" pin would refuse nothing. Refused here,
    // before staging, unless `--patch` says the operator means it.
    if same_line(from, to) && !patch {
        anyhow::bail!(
            "--from {} and --to {} are on the same line (major.minor {}): a same-line pin \
             does NOT refuse the old build — patch builds of one line mix by design. If this \
             is a patch release, pass --patch. If the versions are bare integers (1, 2, 3 …), \
             they pack as 0.0.x and share one line: give the state machine a real major.minor \
             with `pack_version(major, minor, patch)` and pin across lines",
            VersionDisplay(from),
            VersionDisplay(to),
            {
                let (ma, mi, _) = unpack_version(to);
                format!("{ma}.{mi}")
            }
        );
    }
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
    let Some((position, pins, reports, running)) =
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
        // #33 spec §8: the row's running version and which record set it —
        // `pin` when the row's newest pin's `to` is the running version,
        // `genesis` otherwise (the running version was recorded before any
        // pin ever touched this row, or by a row that has never been
        // pinned at all).
        if let Some(entry) = running[row as usize] {
            let by = if history
                .last()
                .is_some_and(|newest: &&UpgradePin| newest.to == entry.version)
            {
                "pin"
            } else {
                "genesis"
            };
            println!(
                "  row={row} running={} set_at={} by={by}",
                VersionDisplay(entry.version),
                entry.record_pos
            );
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
                // `agreed` with no majority needs an EMPTY hash vector
                // (`windows(2)` is vacuously true, and nothing is a
                // majority of nothing). `decode_snapshot_report` refuses
                // `count == 0`, and this report came off the artifact
                // through it — see `verdict`'s non-empty precondition.
                (true, None) => unreachable!("agreed implies a majority"),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_log::cnc::CncMeta;

    fn test_cnc(dir: &std::path::Path) -> std::sync::Arc<CncPage> {
        CncPage::create_file(
            &dir.join("cnc2.dat"),
            &CncMeta {
                node_id: 1,
                instance_id: 0x1122_3344_5566_7788,
                app_id: "test".into(),
                buffer_bytes: 1 << 20,
                max_payload: 256,
                services: [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES],
            },
        )
        .unwrap()
    }

    /// #33 spec §8: `--from`'s default prefers the row's RUNNING version
    /// over the attached version word when a row view has been recorded —
    /// even when the two disagree, which only happens transiently around a
    /// pin (the attached word is the OLD binary's; running is the row's new
    /// truth the moment the pin commits).
    #[test]
    fn default_from_prefers_running_over_attached_version() {
        // A unit test in `src/`, not `tests/` — `CARGO_TARGET_TMPDIR` is
        // only set for integration-test binaries, so this follows
        // `uc_log::cnc`'s own unit-test precedent (a single 8 KiB page, not
        // a heavy artifact) rather than CLAUDE.md's scratch-dir rule for
        // multi-GB test output.
        let dir = tempfile::tempdir().unwrap();
        let cnc = test_cnc(dir.path());
        let slot = cnc.service_slot(0);
        slot.status.store_version(pack_version(1, 0, 0));
        slot.status
            .store_row_view(None, Some(pack_version(2, 1, 0)), 640);
        assert_eq!(default_from(&cnc, 0), Some(pack_version(2, 1, 0)));
    }

    /// With no row view recorded yet, `--from` falls back to the attached
    /// version word — the pre-#33-task-10 behaviour, unchanged.
    #[test]
    fn default_from_falls_back_to_the_attached_version_word() {
        let dir = tempfile::tempdir().unwrap();
        let cnc = test_cnc(dir.path());
        cnc.service_slot(1)
            .status
            .store_version(pack_version(3, 4, 5));
        assert_eq!(default_from(&cnc, 1), Some(pack_version(3, 4, 5)));
    }

    /// Neither a row view nor an attached version word: `None`, so `pin`
    /// bails with "pass --from" rather than staging a bogus record.
    #[test]
    fn default_from_is_none_with_nothing_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let cnc = test_cnc(dir.path());
        assert_eq!(default_from(&cnc, 2), None);
    }

    #[test]
    fn parse_semver_packs_and_refuses() {
        assert_eq!(
            parse_semver("1.2.3"),
            Ok(uc_protocol::identity::pack_version(1, 2, 3))
        );
        // `parse_semver` itself accepts 0.0.0 — it is a well-formed
        // triple; `pin` is what refuses the packed sentinel.
        assert_eq!(parse_semver("0.0.0"), Ok(0));
        assert!(parse_semver("1.2").is_err());
        assert!(parse_semver("1.2.3.4").is_err());
        assert!(parse_semver("256.0.0").is_err());
        assert!(parse_semver("1.0.65536").is_err());
        assert!(parse_semver("a.b.c").is_err());
    }

    fn args() -> CommonArgs {
        CommonArgs {
            instance_dir: std::path::PathBuf::from("/nonexistent/uc2-pin-test"),
            app_id: "test".into(),
            admin_key: None,
            admin_key_name: None,
            admin_ttl_secs: 30,
        }
    }

    /// `--to 0.0.0` packs to the `unversioned` sentinel, so `pin` refuses it
    /// locally, before staging anything — like `--origin 0` and `--row 8`.
    /// Driven through the argument checks only: all three precede every file
    /// and socket touch, so the bogus instance dir above is never reached.
    #[test]
    fn pin_refuses_to_0_0_0_and_origin_0_before_touching_anything() {
        let e = pin(&args(), 0, Some("1.0.0"), "0.0.0", 4096, false).unwrap_err();
        assert!(
            e.to_string().contains("unversioned"),
            "expected the sentinel refusal, got {e}"
        );
        let e = pin(&args(), 0, Some("1.0.0"), "2.0.0", 0, false).unwrap_err();
        assert!(
            e.to_string().contains("--origin"),
            "expected the origin refusal, got {e}"
        );
        let e = pin(&args(), 8, Some("1.0.0"), "2.0.0", 4096, false).unwrap_err();
        assert!(
            e.to_string().contains("--row"),
            "expected the row refusal, got {e}"
        );
    }

    /// #33 final review I1(a): a same-line pin (equal major.minor, patch
    /// ignored) does NOT refuse the old build — patch builds of one line mix
    /// by design — so it is refused locally unless `--patch` says the
    /// operator means exactly that. A bare-integer `VERSION` packs as
    /// `0.0.x`, so `0.0.1 -> 0.0.2` (the 2.13.0-style "pin 1 -> 2") is the
    /// case this catches. Refused before staging: the bogus instance dir is
    /// never reached.
    #[test]
    fn pin_refuses_a_same_line_pin_without_patch() {
        for (from, to) in [("1.2.0", "1.2.5"), ("0.0.1", "0.0.2")] {
            let e = pin(&args(), 0, Some(from), to, 4096, false).unwrap_err();
            let msg = e.to_string();
            assert!(
                msg.contains("--patch"),
                "expected the same-line refusal, got {msg}"
            );
            assert!(
                msg.contains(from) && msg.contains(to),
                "must name both versions: {msg}"
            );
            assert!(
                msg.contains("pack_version"),
                "must point at pack_version: {msg}"
            );
        }
    }

    /// With `--patch`, the same-line pin proceeds past the local check: the
    /// next failure is staging into the nonexistent instance dir, which
    /// proves the line check let it through.
    #[test]
    fn pin_with_patch_proceeds_past_the_line_check() {
        let e = pin(&args(), 0, Some("1.2.0"), "1.2.5", 4096, true).unwrap_err();
        let msg = e.to_string();
        assert!(
            !msg.contains("--patch"),
            "the line check must not fire: {msg}"
        );
        assert!(
            msg.contains("staging"),
            "expected to reach staging, got {msg}"
        );
        // A cross-line pin never needs the flag.
        let e = pin(&args(), 0, Some("1.2.0"), "1.3.0", 4096, false).unwrap_err();
        assert!(e.to_string().contains("staging"), "got {e}");
    }
}
