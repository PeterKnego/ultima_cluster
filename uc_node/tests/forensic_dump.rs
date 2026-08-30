//! Offline forensic dump of a preserved instance-dir set (stale-read hunt,
//! 2026-08-16). Not a CI test — run against evidence kept by
//! `stale_read_hunt.rs`:
//!
//! ```bash
//! UC2_FORENSIC_DIR=/path/to/kept-root cargo test -p uc_node --release \
//!     --test forensic_dump -- --ignored --nocapture
//! ```

use uc_journal::{Journal, JournalConfig};

#[test]
#[ignore = "offline forensic tool, needs UC2_FORENSIC_DIR"]
fn forensic_dump() {
    let root = std::path::PathBuf::from(
        std::env::var("UC2_FORENSIC_DIR").expect("set UC2_FORENSIC_DIR to the kept evidence root"),
    );
    for n in 0..8 {
        let dir = root.join(format!("n{n}"));
        if !dir.exists() {
            continue;
        }
        println!("===== n{n} =====");
        match uc_log::state::NodeState::open(&dir.join("state")) {
            Ok(st) => {
                println!("  vote      : {:?}", st.vote());
                println!("  term_map  : {:?}", st.term_map());
                println!("  snap_floor: {}", st.snapshot_floor());
                if let Some(c) = st.config_record() {
                    println!("  config    : v{} @pos {}", c.config.version, c.position);
                }
            }
            Err(e) => println!("  state open failed: {e:?}"),
        }
        match Journal::open(JournalConfig::new(dir.join("journal"))) {
            Ok(j) => {
                let (first, last) = (j.first_seq(), j.last_seq());
                println!("  journal   : first_seq={first:?} last_seq={last:?}");
                if let (Some(f), Some(l)) = (first, last) {
                    // Block meta = base byte position; end = base + payload len.
                    for seq in [f, l.saturating_sub(2), l.saturating_sub(1), l] {
                        if seq < f || seq > l {
                            continue;
                        }
                        if let Ok(Some((meta, payload))) = j.read(seq) {
                            println!(
                                "    block seq={seq} base={meta} len={} end={}",
                                payload.len(),
                                meta + payload.len() as u64
                            );
                        }
                    }
                }
            }
            Err(e) => println!("  journal open failed: {e:?}"),
        }
        // Ghost-tail walk: raw frames in log.buf past the journal end. The
        // buffer is a raw ring (offset = position & (len-1), no file header);
        // for positions below the 4 MiB capacity the offset IS the position.
        // Frame header: length u32 | type u8 | flags u8 | pad u16 |
        // leadership_term_id u32 (uc_protocol::v2::frame layout, 32 B total).
        if let Ok(buf) = std::fs::read(dir.join("log.buf")) {
            let start: u64 = std::env::var("UC2_FORENSIC_WALK_FROM")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(603104);
            let mask = (buf.len() as u64) - 1;
            let mut pos = start;
            println!("  ghost walk from {start}:");
            for _ in 0..2000 {
                let off = (pos & mask) as usize;
                if off + 32 > buf.len() {
                    break;
                }
                let h = uc_protocol::v2::frame::read_header(&buf[off..off + 32]);
                if h.length == 0 || h.length as usize > 1 << 20 {
                    println!("    pos={pos} length={} -> end of valid frames", h.length);
                    break;
                }
                let aligned = uc_protocol::v2::frame::align_frame_len(h.length as usize) as u64;
                // Only print term transitions + first/last few to bound output.
                println!(
                    "    pos={pos} len={} type={} term={}",
                    h.length, h.frame_type, h.leadership_term_id
                );
                pos += aligned;
                if pos > start + 60_000 {
                    println!("    ... (walk capped at +60KB)");
                    break;
                }
            }
            println!("    walk ended at pos={pos}");
        }
    }
}
