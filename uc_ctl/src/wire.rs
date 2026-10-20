// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `uc2ctl wire show` / `uc2ctl wire raise` (wire-level spec §4.3, §5.2).
//!
//! `show` reads the cnc page: the committed level (`wire_level`, written by
//! the cluster agent at apply), this binary's range (`wire_binary`), and — on
//! the leader only, since the peer band is leader-authoritative — each
//! member's reported wire max, with the verdict of the same pure predicate
//! the leader and the admin door use ([`uc_node::wire_raise_target`]). The
//! raise MODE is committed cluster data, so it comes from the cluster view.
//!
//! `raise` sends `ADMIN_OP_WIRE_RAISE` (wire op 12) with `id`/`ip`/`port`
//! zero. An already-at-level cluster answers the same `(0, 0, position)` a
//! real raise does (ruling 9), so the success line names neither — it points
//! at `wire show` for the committed level.

use std::collections::HashMap;
use std::sync::atomic::Ordering;

use uc_node::WireHoldout;
use uc_protocol::WireRange;
use uc_protocol::v2::cnc::{
    ADMIN_OP_WIRE_RAISE, CNC_MAX_PEER_SLOTS, CNC_PEER_ROLE_LEARNER, NODE_FLAG_LEADER,
};
use uc_protocol::v2::settings::{WIRE_RAISE_AUTO, WIRE_RAISE_MANUAL};

use crate::CommonArgs;

/// `uc2ctl wire raise`: ask the leader to raise the committed wire level.
pub fn raise(common: &CommonArgs) -> anyhow::Result<()> {
    let resp =
        crate::signed_admin_request(common, ADMIN_OP_WIRE_RAISE, 0, 0, 0, "cluster position")?;
    match resp.status {
        0 => {
            println!(
                "ok (cluster position {}) — run `uc2ctl wire show` for the committed level",
                resp.version
            );
            Ok(())
        }
        1 => {
            println!(
                "refused: {} (cluster position {}) — `uc2ctl wire show` on the leader names the holdout",
                crate::reason_str(resp.reason),
                resp.version
            );
            anyhow::bail!("refused: {}", crate::reason_str(resp.reason))
        }
        2 => {
            println!(
                "retry: not the leader, or a previous cluster command is still uncommitted \
                 (cluster position {}) — re-run against the leader",
                resp.version
            );
            anyhow::bail!("retry: try again")
        }
        other => anyhow::bail!("unrecognized response status {other}"),
    }
}

/// The raise mode's word, or the honest "no view" when there is none.
fn mode_word(mode: Option<u8>) -> &'static str {
    match mode {
        Some(WIRE_RAISE_AUTO) => "auto",
        Some(WIRE_RAISE_MANUAL) => "manual",
        Some(_) => "keep",
        None => "unknown (no cluster view yet)",
    }
}

/// The verdict line for a [`uc_node::wire_raise_target`] answer.
fn verdict_line(level: u8, verdict: Result<u8, WireHoldout>) -> String {
    match verdict {
        Ok(target) => format!("verdict: raisable to {target}"),
        Err(WireHoldout::AtLevel) => format!("verdict: at level (nothing above {level})"),
        Err(WireHoldout::NoReport(id)) => {
            format!("verdict: holdout id={id} (no report this term)")
        }
        Err(WireHoldout::Below { id, reported }) => {
            format!("verdict: holdout id={id} reported {reported}")
        }
    }
}

/// The cnc `wire_binary` word as `status` and `wire show` print it:
/// `min..max`, or `pre-level page` for a zero word — a cnc 3.4 page, or a
/// page the node has not yet written at boot (review M4).
pub fn binary_word(word: u64) -> String {
    if word == 0 {
        return "pre-level page".to_string();
    }
    let b = WireRange::unpack(word);
    format!("{}..{}", b.min, b.max)
}

/// `uc2ctl wire show`: the committed level, this binary's range, the raise
/// mode, and (on the leader) the per-member table and the raise verdict.
pub fn show(common: &CommonArgs) -> anyhow::Result<()> {
    let cnc = crate::open(common)?;
    let level = cnc.wire_level() as u8;
    let word = cnc.wire_binary();
    let binary = WireRange::unpack(word);
    let mode = crate::view::show_doc(common)
        .ok()
        .flatten()
        .map(|d| d.cluster.settings.wire_raise);
    println!(
        "wire: level={level} binary={} raise={}",
        binary_word(word),
        mode_word(mode)
    );
    // #35: a dead node's page still says whatever it last wrote, so the
    // leader flag counts only on a live node — the `status` rule.
    let live = crate::node_liveness(
        uc_node::ipc::probe_instance_lock(&common.instance_dir),
        cnc.status().node_heartbeat_ns.load_acquire(),
        crate::wall_now_ns(),
    )
    .is_live();
    let leader = live && cnc.status().flags.load_acquire() & NODE_FLAG_LEADER != 0;
    if !leader {
        println!(
            "members: (this node is not the live leader; the table is leader-authoritative — run on the leader)"
        );
        return Ok(());
    }
    // The peer band excludes this node, which reads its own max from the
    // binary range — as the leader's predicate does.
    // (`NodeId` is `u32`; `uc_ctl` takes no `uc_consensus` dependency for it.)
    let self_id: u32 = cnc
        .try_meta()
        .ok_or_else(|| anyhow::anyhow!("the cnc page is being rewritten — try again"))?
        .node_id;
    let mut members = vec![self_id];
    let mut reported = HashMap::new();
    for i in 0..CNC_MAX_PEER_SLOTS {
        let head = &cnc.peer_slot(i).head;
        let id_and_role = head.id_and_role.load(Ordering::Acquire);
        if id_and_role == 0 {
            continue;
        }
        let id = (id_and_role >> 8) as u32;
        let role = (id_and_role & 0xFF) as u8;
        let max = head.wire_max.load(Ordering::Acquire) as u8;
        println!(
            "  member id={id} role={} wire_max={}",
            if role == CNC_PEER_ROLE_LEARNER {
                "learner"
            } else {
                "voter"
            },
            if max == 0 {
                "no report".to_string()
            } else {
                max.to_string()
            }
        );
        members.push(id);
        if max != 0 {
            reported.insert(id, max);
        }
    }
    let verdict = uc_node::wire_raise_target(binary.max, level, &members, self_id, &reported);
    println!("{}", verdict_line(level, verdict));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mode_and_verdict_words() {
        assert_eq!(mode_word(Some(WIRE_RAISE_AUTO)), "auto");
        assert_eq!(mode_word(Some(WIRE_RAISE_MANUAL)), "manual");
        assert_eq!(mode_word(Some(0)), "keep");
        assert_eq!(mode_word(None), "unknown (no cluster view yet)");
        assert_eq!(verdict_line(12, Ok(13)), "verdict: raisable to 13");
        assert_eq!(
            verdict_line(12, Err(WireHoldout::AtLevel)),
            "verdict: at level (nothing above 12)"
        );
        assert_eq!(
            verdict_line(12, Err(WireHoldout::NoReport(2))),
            "verdict: holdout id=2 (no report this term)"
        );
        assert_eq!(
            verdict_line(
                12,
                Err(WireHoldout::Below {
                    id: 3,
                    reported: 12
                })
            ),
            "verdict: holdout id=3 reported 12"
        );
    }
}
