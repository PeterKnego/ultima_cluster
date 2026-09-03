// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Regenerates the committed seed corpora under `fuzz/corpus/<target>/`.
//!
//! Deterministic and idempotent: run it, `git status` stays clean unless a
//! seed definition in `seeds.rs` actually changed.
//!
//! Usage: `cd fuzz && cargo +nightly run --bin seed-corpus`

use std::path::Path;
use uc_fuzz::seeds;

fn write_target(root: &Path, target: &str, entries: Vec<seeds::Seed>) -> std::io::Result<()> {
    let dir = root.join("corpus").join(target);
    std::fs::create_dir_all(&dir)?;
    for s in &entries {
        let path = dir.join(s.name);
        if s.regen == seeds::Regen::IfAbsent && path.exists() {
            // Captured from an RNG-drawing path: keep the committed bytes.
            continue;
        }
        // Overwrite unconditionally, but only touch the file when the bytes
        // differ, so re-running does not churn mtimes.
        let same = std::fs::read(&path).map(|old| old == s.bytes).unwrap_or(false);
        if !same {
            std::fs::write(&path, &s.bytes)?;
        }
    }
    // Prune anything the generator did not produce. This enforces the
    // documented policy — "the committed corpus is exactly the generator's
    // output" — and, more practically, removes seeds left behind when one is
    // RENAMED (the writer alone would keep the old file forever) as well as
    // libFuzzer's own hash-named discoveries from a local run. Delete a seed
    // definition and the file goes with it.
    let wanted: std::collections::HashSet<&str> = entries.iter().map(|s| s.name).collect();
    let mut pruned = 0usize;
    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !wanted.contains(name) {
            std::fs::remove_file(entry.path())?;
            pruned += 1;
        }
    }
    if pruned > 0 {
        println!("{target}: {} seeds in {} ({pruned} pruned)", entries.len(), dir.display());
    } else {
        println!("{target}: {} seeds in {}", entries.len(), dir.display());
    }
    Ok(())
}

fn main() -> std::io::Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    write_target(root, "uc_protocol_datagram", seeds::uc_protocol_datagram())?;
    write_target(root, "uc_remote_frame", seeds::uc_remote_frame())?;
    write_target(root, "uc_crypto_open", seeds::uc_crypto_open())?;
    write_target(root, "uc_crypto_handshake", seeds::uc_crypto_handshake())?;
    write_target(root, "uc_crypto_group_key", seeds::uc_crypto_group_key())?;
    write_target(root, "uc_crypto_admin", seeds::uc_crypto_admin())?;
    write_target(root, "uc_journal_record", seeds::uc_journal_record())?;
    write_target(root, "uc_journal_stable_value", seeds::uc_journal_stable_value())?;
    write_target(root, "uc_protocol_cnc", seeds::uc_protocol_cnc())?;
    write_target(root, "ring_mpsc_record", seeds::ring_mpsc_record())?;
    write_target(root, "uc_protocol_log_frame", seeds::uc_protocol_log_frame())?;
    write_target(root, "uc_protocol_timer_frame", seeds::uc_protocol_timer_frame())?;
    write_target(root, "uc_protocol_sched_record", seeds::uc_protocol_sched_record())?;
    write_target(root, "uc_protocol_schedule_table", seeds::uc_protocol_schedule_table())?;
    write_target(root, "uc_service_session", seeds::uc_service_session())?;
    write_target(root, "uc_node_toml", seeds::uc_node_toml())?;
    write_target(root, "uc_gateway_toml", seeds::uc_gateway_toml())?;
    write_target(root, "uc_node_http", seeds::uc_node_http())?;
    Ok(())
}
