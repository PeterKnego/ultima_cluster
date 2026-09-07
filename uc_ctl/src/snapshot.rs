// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc2ctl snapshot [--standby]` / `snapshot fetch --from <id> [--position P]`
//! / `snapshot show` (coordinated-snapshot spec §8) — admin ops 8/9.
//!
//! `take` (the default `snapshot` action) commands an instant on the leader:
//! op 8, `id = standby as u32` — this op has no digest to sign (unlike
//! `schedule apply`/`settings apply`), so the request's `ip`/`port` fields
//! are unused, and `version` on the response is the instant's position `P`.
//! Printed as `instant=<P>` on `0`, the reason name on `1`, a retry line on
//! `2` — `uc_ctl::settings::apply`'s shape, minus the staged file (there is
//! nothing to stage: the whole command is the `standby` bit).
//!
//! `fetch` asks THIS voter to pull a learner's complete set store-only: op
//! 9, `id = learner_id`, and the target position packed into the request's
//! two free address fields **exactly** the way the node's own
//! `fetch_position` (`uc_node::node`) unpacks them — pinned equal by this
//! module's own test: the low 32 bits in `ip`, the next 16 in `port` (`ip =
//! (p & 0xFFFF_FFFF) as u32`, `port = (p >> 32) as u16`). That is **48
//! bits**: a `--position` at or above `1 << 48` is refused HERE, by name,
//! before the request ever reaches the admin band — past that bound the
//! value would silently truncate to a lower, wrong position rather than
//! fail loudly. `--position` omitted sends `0`, the wire's own "the
//! learner's newest complete set" sentinel.
//!
//! `show` is OFFLINE, like `schedule show`/`settings show` — but unlike
//! them it does not read the cluster artifact's CONTENT: it opens the cnc
//! page only to learn the declared row set + names (the same page `status`
//! reads), then reads directory listings under `<instance_dir>/snapshots/`
//! — one subdirectory per declared row, plus `snapshots/cluster/` — and
//! parses only each entry's FILE NAME (`snap-<pos>.ultsnap` /
//! `snap-<pos>.ultcluster`). It never opens a file: the 16-byte
//! `ULTSNAP1`-tagged envelope inside each one (`uc_service::snapshots`) is
//! not this command's business. `set=<P>` is the newest position present in
//! **every** declared row's directory **and** `snapshots/cluster/` — the
//! intersection of what is on disk, not each side's own newest — so a row
//! that has already frozen a NEXT instant (and so also lists a newer file
//! than the last complete set) does not make an established set disappear
//! from this reading. `set=none` when no such position exists, including
//! "nothing has ever snapshotted".

use std::collections::HashSet;
use std::path::Path;

use uc_protocol::v2::cnc::{ADMIN_OP_SNAPSHOT, ADMIN_OP_SNAPSHOT_FETCH, CNC_MAX_SERVICES};

use crate::CommonArgs;

/// The fetch position's wire encoding is 48 bits (32 in `ip`, 16 in `port`) —
/// one past this is unrepresentable.
const MAX_FETCH_POSITION: u64 = 1 << 48;

// ---------------------------------------------------------------- take

/// `uc2ctl snapshot [--standby]`: command a coordinated-snapshot instant on
/// the leader (admin op 8).
pub fn take(common: &CommonArgs, standby: bool) -> anyhow::Result<()> {
    let resp = crate::signed_admin_request(
        common,
        ADMIN_OP_SNAPSHOT,
        standby as u32,
        0,
        0,
        "snapshot position",
    )?;
    match resp.status {
        0 => {
            println!("instant={}", resp.version);
            Ok(())
        }
        1 => {
            println!(
                "refused: {} (snapshot position {})",
                crate::reason_str(resp.reason),
                resp.version
            );
            anyhow::bail!("refused: {}", crate::reason_str(resp.reason));
        }
        2 => {
            println!(
                "retry: leader unknown, or an instant is already in flight (snapshot position \
                 {}) — try again",
                resp.version
            );
            anyhow::bail!("retry: try again");
        }
        other => anyhow::bail!("unrecognized response status {other}"),
    }
}

// ---------------------------------------------------------------- fetch

/// `position` -> `(ip, port)`, the SAME 48-bit packing
/// `uc_node::node::fetch_position` unpacks (pinned equal by this module's
/// test). `Err` names the bound rather than truncating silently.
fn encode_fetch_position(position: u64) -> Result<(u32, u16), String> {
    if position >= MAX_FETCH_POSITION {
        return Err(format!(
            "--position {position} is unrepresentable: a fetch position is limited to 48 bits \
             (< {MAX_FETCH_POSITION}) by the admin request's wire encoding (spec §8)"
        ));
    }
    Ok(((position & 0xFFFF_FFFF) as u32, (position >> 32) as u16))
}

/// `uc2ctl snapshot fetch --from <id> [--position P]`: point this voter at
/// learner `from` and pull the set at `position` (default: the learner's
/// newest complete set) store-only (admin op 9). Node-local — never
/// forwarded, so this always runs against the node named by `--instance-dir`
/// regardless of who leads.
pub fn fetch(common: &CommonArgs, from: u32, position: Option<u64>) -> anyhow::Result<()> {
    let position = position.unwrap_or(0);
    let (ip, port) =
        encode_fetch_position(position).map_err(|e| anyhow::anyhow!("snapshot fetch: {e}"))?;
    let resp =
        crate::signed_admin_request(common, ADMIN_OP_SNAPSHOT_FETCH, from, ip, port, "position")?;
    match resp.status {
        0 => {
            println!(
                "accepted: fetch of position {} from node {from} is underway — not yet a \
                 promise it arrived; poll `snapshot show` or `status` for \
                 `uc2_snapshot_fetched_position` to move",
                resp.version
            );
            Ok(())
        }
        1 => {
            println!(
                "refused: {} (position {})",
                crate::reason_str(resp.reason),
                resp.version
            );
            anyhow::bail!("refused: {}", crate::reason_str(resp.reason));
        }
        2 => {
            println!(
                "retry: a fetch is already in flight here, or the receiver's route was \
                 momentarily full (position {}) — try again",
                resp.version
            );
            anyhow::bail!("retry: try again");
        }
        other => anyhow::bail!("unrecognized response status {other}"),
    }
}

// ---------------------------------------------------------------- show

/// Every `snap-<pos><suffix>` position under `dir`, by exact name only (a
/// `.tmp` builder file, a part file, or anything else that doesn't strip
/// clean is invisible here — the same convention
/// `uc_node::backup::scan_snapshots` uses). A missing directory yields the
/// empty set, not an error: a row that has never snapshotted has none.
fn positions_in(dir: &Path, suffix: &str) -> HashSet<u64> {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return HashSet::new();
    };
    rd.flatten()
        .filter_map(|e| {
            e.file_name()
                .to_str()?
                .strip_prefix("snap-")?
                .strip_suffix(suffix)?
                .parse::<u64>()
                .ok()
        })
        .collect()
}

fn newest_str(positions: &HashSet<u64>) -> String {
    positions
        .iter()
        .max()
        .map(|p| p.to_string())
        .unwrap_or_else(|| "none".to_string())
}

/// `uc2ctl snapshot show`: the newest COMPLETE set's position and each
/// artifact's presence — the diagnostic for a stalled instant (spec §8). See
/// the module doc for what "complete" means here.
pub fn show(common: &CommonArgs) -> anyhow::Result<()> {
    let cnc = crate::open(common)?;
    let declared = cnc.services_declared();
    let root = common.instance_dir.join("snapshots");

    let mut shared: Option<HashSet<u64>> = None;
    for id in 0..CNC_MAX_SERVICES as u8 {
        if declared & (1u64 << id) == 0 {
            continue;
        }
        let name = cnc
            .service_slot(id as usize)
            .identity
            .name()
            .map(|n| n.as_str().to_string())
            .unwrap_or_default();
        let positions = positions_in(&root.join(id.to_string()), ".ultsnap");
        println!("row={id} name={name} newest={}", newest_str(&positions));
        shared = Some(match shared {
            None => positions,
            Some(acc) => acc.intersection(&positions).copied().collect(),
        });
    }

    let cluster_positions = positions_in(&root.join("cluster"), ".ultcluster");
    println!("cluster newest={}", newest_str(&cluster_positions));
    shared = Some(match shared {
        None => cluster_positions,
        Some(acc) => acc.intersection(&cluster_positions).copied().collect(),
    });

    let set = shared.unwrap_or_default();
    println!("set={}", newest_str(&set));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pinned equal to `uc_node::node::fetch_position`'s unpacking
    /// (`(ip as u64) | ((port as u64) << 32)`) — the CLI and the node must
    /// never disagree about which bits mean what.
    #[test]
    fn encode_fetch_position_round_trips_through_the_nodes_unpacking() {
        for p in [
            0u64,
            1,
            0xFFFF_FFFF,
            1 << 32,
            1 << 47,
            MAX_FETCH_POSITION - 1,
        ] {
            let (ip, port) = encode_fetch_position(p).unwrap();
            let decoded = (ip as u64) | ((port as u64) << 32);
            assert_eq!(decoded, p, "round trip for {p}");
        }
    }

    #[test]
    fn encode_fetch_position_refuses_above_48_bits_by_name() {
        let e = encode_fetch_position(MAX_FETCH_POSITION).unwrap_err();
        assert!(e.contains("--position"), "{e}");
        assert!(e.contains("48 bits"), "{e}");
        assert!(encode_fetch_position(u64::MAX).is_err());
    }

    #[test]
    fn positions_in_reads_exact_names_only() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snap-4096.ultsnap"), b"x").unwrap();
        std::fs::write(dir.path().join("snap-8192.ultsnap"), b"x").unwrap();
        std::fs::write(dir.path().join("snap-8192.ultsnap.tmp"), b"building").unwrap();
        std::fs::write(dir.path().join("notes.txt"), b"an operator's").unwrap();

        let positions = positions_in(dir.path(), ".ultsnap");
        assert_eq!(positions, HashSet::from([4096, 8192]));
        assert_eq!(newest_str(&positions), "8192");
    }

    #[test]
    fn positions_in_is_empty_for_a_missing_directory() {
        let positions = positions_in(Path::new("/nonexistent/does/not/exist"), ".ultsnap");
        assert!(positions.is_empty());
        assert_eq!(newest_str(&positions), "none");
    }
}
