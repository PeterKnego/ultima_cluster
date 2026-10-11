// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Backup-tier spec §6.3: `uc2ctl tier …`.
//!
//! `forget` sends `ADMIN_OP_TIER_FORGET` (wire op 11). The `u64` tier id does
//! not fit one admin-request field, so it rides split: its low 32 bits in
//! `id`, its high 32 in `ip`, `port = 0` — the node rebuilds
//! `(ip << 32) | id`. Leader-only: a follower answers retry, and the
//! operator re-runs it against the node the leader hint names.
//!
//! `show`, `verify`, `restore`, `fork` and `export-corpus` are OFFLINE
//! (spec §8): they read the target's files directly, never a node's admin
//! band.

use std::path::Path;

use uc_node::tier::TierError;
use uc_node::tier::chain::{list_tier, render_show, verify as verify_tier};
use uc_node::tier::materialize::{
    Mode, export_corpus as export_corpus_dir, fork as fork_dir, materialize, restorable_ranges,
    restore_point,
};
use uc_node::tier::target::{FsTarget, read_tier};
use uc_protocol::v2::cnc::ADMIN_OP_TIER_FORGET;

use crate::CommonArgs;

/// Forget `tier_id`: drop its committed watermark. Prints `forgotten: …` and
/// returns `Ok` on status 0; prints and errors on a refusal (`62
/// tier_unknown`) or a retry.
pub fn forget(common: &CommonArgs, tier_id: u64) -> anyhow::Result<()> {
    let lo = tier_id as u32;
    let hi = (tier_id >> 32) as u32;
    let resp =
        crate::signed_admin_request(common, ADMIN_OP_TIER_FORGET, lo, hi, 0, "cluster position")?;
    match resp.status {
        0 => {
            println!("forgotten: tier {tier_id:016x} position={}", resp.version);
            Ok(())
        }
        1 => {
            println!(
                "refused: {} (cluster position {})",
                crate::reason_str(resp.reason),
                resp.version
            );
            anyhow::bail!("refused: {}", crate::reason_str(resp.reason));
        }
        2 => {
            println!(
                "retry: leader unknown or a previous cluster command is still uncommitted \
                 (cluster position {}) — try again",
                resp.version
            );
            anyhow::bail!("retry: try again");
        }
        other => anyhow::bail!("unrecognized response status {other}"),
    }
}

fn tier_err(verb: &str, e: TierError) -> anyhow::Error {
    anyhow::anyhow!("tier {verb}: {e}")
}

fn open_target(
    verb: &str,
    target: &Path,
) -> anyhow::Result<(FsTarget, uc_node::tier::layout::TierInfo)> {
    if !target.is_dir() {
        anyhow::bail!(
            "tier {verb}: target {} is not a directory",
            target.display()
        );
    }
    let t = FsTarget::new(target);
    let info = read_tier(&t)
        .map_err(|e| tier_err(verb, e))?
        .ok_or_else(|| anyhow::anyhow!("tier {verb}: no TIER file at {}", target.display()))?;
    Ok((t, info))
}

/// `tier show`: the tier's sets, chunks and watermark; with an instance
/// directory, that node's committed watermarks (its newest cluster artifact).
pub fn show(target: &Path, instance_dir: Option<&Path>) -> anyhow::Result<()> {
    let (t, info) = open_target("show", target)?;
    let l = list_tier(&t).map_err(|e| tier_err("show", e))?;
    print!("{}", render_show(&info, &l));
    let ranges = restorable_ranges(&l);
    if ranges.is_empty() {
        println!("restorable: none (no complete set with journal after it)");
    }
    for (a, b) in ranges {
        println!("restorable: ({a}, {b}] (a frame end Q with {a} < Q <= {b})");
    }
    if let Some(d) = instance_dir {
        match uc_node::cluster_agent::read_committed_backups(d)
            .map_err(|e| anyhow::anyhow!("tier show: {}: {e}", d.display()))?
        {
            None => println!("committed (instance dir): no cluster artifact yet"),
            Some((pos, entries)) => {
                println!("committed (instance dir, cluster artifact at {pos}):");
                if entries.is_empty() {
                    println!("  no registered tier");
                }
                for e in entries {
                    let mark = if e.tier_id == info.tier_id {
                        " (this tier)"
                    } else {
                        ""
                    };
                    println!(
                        "  tier {:#018x}{mark} oldest_set={} newest_set={} through={} reporter={}",
                        e.tier_id, e.oldest_set, e.newest_set, e.through, e.reporter
                    );
                }
            }
        }
    }
    Ok(())
}

/// `tier verify [--deep]`: prints every finding; an error (exit 1) when
/// there is any.
pub fn verify(target: &Path, deep: bool) -> anyhow::Result<()> {
    let (t, _) = open_target("verify", target)?;
    let rep = verify_tier(&t, deep).map_err(|e| tier_err("verify", e))?;
    println!(
        "tier verify{}: {} sets, {} chunks, {} findings",
        if deep { " --deep" } else { "" },
        rep.sets,
        rep.chunks,
        rep.findings.len()
    );
    for i in &rep.info {
        println!("info: {i}");
    }
    for f in &rep.findings {
        println!("finding: {f}");
    }
    if rep.findings.is_empty() {
        Ok(())
    } else {
        anyhow::bail!("tier verify: {} finding(s)", rep.findings.len())
    }
}

/// How `tier restore` was asked to build.
pub enum RestoreMode {
    /// Whole-cluster restore; the string is `--confirm-cluster`.
    Cluster(String),
    /// One replacement node under this new id.
    NewNode(u32),
}

/// The data-loss line `tier restore` prints before it builds anything.
pub fn data_loss_line(q: u64) -> String {
    format!("restoring as of {q}; history after {q} is discarded on every node restored this way")
}

/// Ruling R12a: why `tier restore --new-node` is refused in this release.
pub fn new_node_unsupported(node_id: u32) -> String {
    format!(
        "tier restore: refusing: new_node_unsupported: a tier-seeded replacement node (id \
         {node_id}) is not supported yet — its restored term map starts at the set and cannot \
         align with a live leader's lineage; add a replacement node with a fresh, empty \
         instance dir and the normal join (a snapshot session from its peers)"
    )
}

/// `tier restore`: refuse a `--confirm-cluster` that is not the tier's
/// `app_id`, print the data-loss line, then build `instance_dir`.
pub fn restore(
    target: &Path,
    at: Option<u64>,
    instance_dir: &Path,
    mode: RestoreMode,
) -> anyhow::Result<()> {
    if let RestoreMode::NewNode(node_id) = mode {
        // Ruling R12a: deferred to project 3b — refused before anything is
        // read or built.
        anyhow::bail!("{}", new_node_unsupported(node_id));
    }
    let (t, info) = open_target("restore", target)?;
    let mode = match mode {
        RestoreMode::Cluster(confirm) => {
            if confirm != info.app_id {
                anyhow::bail!(
                    "tier restore: refusing: --confirm-cluster {confirm:?} does not match the \
                     tier's app_id {:?} — a whole-cluster restore discards history; type the \
                     cluster's app_id to confirm",
                    info.app_id
                );
            }
            Mode::Recovery
        }
        RestoreMode::NewNode(_) => unreachable!("refused above (R12a)"),
    };
    let l = list_tier(&t).map_err(|e| tier_err("restore", e))?;
    let (_, q) = restore_point(&l, at).map_err(|e| tier_err("restore", e))?;
    println!("{}", data_loss_line(q));
    let m = materialize(&t, Some(q), instance_dir, mode, now_ns())
        .map_err(|e| tier_err("restore", e))?;
    println!(
        "restored: {} start_set={} q={} tier={:#018x} mode={:?}",
        instance_dir.display(),
        m.start_set,
        m.q,
        info.tier_id,
        m.mode
    );
    if m.fenced {
        println!(
            "fenced: {} — Q lies below the tier's reach, so this tier's history past {} is \
             abandoned; every writer refuses this target from now on (backup_target_fenced). \
             Restore every other node at the same --at, then point every node's [backup] target \
             at a NEW, empty directory before starting them (this tier stays readable for \
             further restores)",
            uc_node::tier::layout::fence_object(m.q),
            m.q
        );
    }
    Ok(())
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// What `tier fork` prints before it builds anything.
pub fn fork_line(app_id: &str, node_id: u32, q: u64) -> String {
    format!(
        "forking as of {q} into a separate cluster {app_id:?} with node {node_id} as its single \
         voter; no output at or below {q} is replayed"
    )
}

/// `tier fork`: refuse `fork_same_app_id`, print the fork line, build
/// `instance_dir` and force `node_id` to the single voter.
pub fn fork(
    target: &Path,
    at: Option<u64>,
    instance_dir: &Path,
    app_id: &str,
    node_id: u32,
) -> anyhow::Result<()> {
    let (t, info) = open_target("fork", target)?;
    if app_id == info.app_id {
        anyhow::bail!(
            "tier fork: refusing: fork_same_app_id: --app-id {app_id:?} is the tier's own app_id \
             — a fork is a separate cluster and runs under a new app_id"
        );
    }
    let l = list_tier(&t).map_err(|e| tier_err("fork", e))?;
    let (_, q) = restore_point(&l, at).map_err(|e| tier_err("fork", e))?;
    println!("{}", fork_line(app_id, node_id, q));
    let f = fork_dir(&t, Some(q), instance_dir, app_id, node_id, now_ns())
        .map_err(|e| tier_err("fork", e))?;
    println!(
        "forked: {} start_set={} q={} tier={:#018x} app_id={app_id} node_id={node_id} \
         config_version={} dropped={:?}",
        instance_dir.display(),
        f.materialized.start_set,
        f.materialized.q,
        info.tier_id,
        f.force.new_version,
        f.force.dropped_peers
    );
    println!(
        "next: start node {node_id} on this directory under app_id {app_id:?} (its first \
         leadership commits the single-voter membership into the cluster state); grow it with \
         `uc2ctl add-learner` then `uc2ctl promote`"
    );
    Ok(())
}

/// `tier export-corpus`: the restore of `[S, Q)` into `out`, then the
/// diff-replay `CORPUS` manifest, written by `uc_diffreplay`'s own writer.
pub fn export_corpus(
    target: &Path,
    row: u8,
    from: Option<u64>,
    to: u64,
    out: &Path,
) -> anyhow::Result<()> {
    let (t, info) = open_target("export-corpus", target)?;
    let c = export_corpus_dir(&t, row, from, to, out, now_ns())
        .map_err(|e| tier_err("export-corpus", e))?;
    let m = uc_diffreplay::corpus::CorpusManifest {
        app_id: c.app_id,
        row: c.row,
        origin: c.origin,
        end: c.end,
        version: c.version,
    };
    m.write(out)
        .map_err(|e| anyhow::anyhow!("tier export-corpus: {e:#}"))?;
    println!(
        "exported: {} row={} origin={} end={} version={:#x} tier={:#018x}",
        out.display(),
        m.row,
        m.origin,
        m.end,
        m.version,
        info.tier_id
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> tempfile::TempDir {
        let root = std::env::var("CARGO_TARGET_TMPDIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_ctl_tests")
            });
        std::fs::create_dir_all(&root).expect("scratch root");
        tempfile::Builder::new()
            .prefix("uc2-tier-")
            .tempdir_in(&root)
            .expect("tempdir")
    }

    /// A `--confirm-cluster` that is not the tier's `app_id` is refused
    /// before anything is built; so is a target with no `TIER`.
    #[test]
    fn restore_refuses_a_wrong_confirm_cluster_and_builds_nothing() {
        let d = tmp();
        let tier = d.path().join("tier");
        std::fs::create_dir_all(&tier).unwrap();
        let out = d.path().join("out");
        let e = restore(&tier, None, &out, RestoreMode::Cluster("app".into())).unwrap_err();
        assert!(e.to_string().contains("no TIER"), "{e}");
        // Ruling R12a: `--new-node` is refused by name BEFORE the target is
        // opened — here it has no TIER, which the open would have named.
        let e = restore(&tier, None, &out, RestoreMode::NewNode(9)).unwrap_err();
        assert!(e.to_string().contains("new_node_unsupported"), "{e}");
        assert!(!e.to_string().contains("no TIER"), "{e}");
        assert!(!out.exists());
        uc_node::tier::target::open_or_create_tier(&FsTarget::new(&tier), "app", 1).unwrap();
        let e = restore(&tier, None, &out, RestoreMode::Cluster("other".into())).unwrap_err();
        assert!(e.to_string().contains("--confirm-cluster"), "{e}");
        // The right app_id on a tier with no set: unreachable, by name.
        let e = restore(&tier, None, &out, RestoreMode::Cluster("app".into())).unwrap_err();
        assert!(e.to_string().contains("restore_point_unreachable"), "{e}");
        assert!(!out.exists());
        assert_eq!(
            data_loss_line(4096),
            "restoring as of 4096; history after 4096 is discarded on every node restored this way"
        );
    }
}
