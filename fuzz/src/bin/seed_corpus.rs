// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Regenerates the committed seed corpora under `fuzz/corpus/<target>/`.
//!
//! Deterministic and idempotent: run it, `git status` stays clean unless a
//! seed definition in `seeds.rs` actually changed.
//!
//! Usage: `cd fuzz && cargo +nightly run --bin seed-corpus`

use std::path::Path;
use uc2_fuzz::seeds;

fn write_target(root: &Path, target: &str, entries: Vec<seeds::Seed>) -> std::io::Result<()> {
    let dir = root.join("corpus").join(target);
    std::fs::create_dir_all(&dir)?;
    for s in &entries {
        let path = dir.join(s.name);
        // Overwrite unconditionally, but only touch the file when the bytes
        // differ, so re-running does not churn mtimes.
        let same = std::fs::read(&path).map(|old| old == s.bytes).unwrap_or(false);
        if !same {
            std::fs::write(&path, &s.bytes)?;
        }
    }
    println!("{target}: {} seeds in {}", entries.len(), dir.display());
    Ok(())
}

fn main() -> std::io::Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    write_target(root, "uc_protocol_datagram", seeds::uc_protocol_datagram())?;
    Ok(())
}
