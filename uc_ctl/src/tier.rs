// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Backup-tier spec §6.3: `uc2ctl tier …`.
//!
//! `forget` sends `ADMIN_OP_TIER_FORGET` (wire op 11). The `u64` tier id does
//! not fit one admin-request field, so it rides split: its low 32 bits in
//! `id`, its high 32 in `ip`, `port = 0` — the node rebuilds
//! `(ip << 32) | id`. Leader-only: a follower answers retry, and the
//! operator re-runs it against the node the leader hint names.

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
