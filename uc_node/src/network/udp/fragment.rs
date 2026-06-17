//! Split a logical message (an encoded RPC `Frame`) into DATA-segment-sized
//! chunks, tagging BEGIN/END for reassembly.
use bytes::Bytes;

use super::wire::{FLAG_BEGIN, FLAG_END, SEG_HEADER_LEN};

/// Returns `(seq, flags, chunk)` for each fragment. `first_seq` is the seq of
/// the BEGIN fragment; subsequent fragments increment by 1.
pub fn fragment(payload: &Bytes, first_seq: u64, mtu: usize) -> Vec<(u64, u8, Bytes)> {
    let max_chunk = mtu.saturating_sub(SEG_HEADER_LEN + 4).max(1);
    if payload.len() <= max_chunk {
        return vec![(first_seq, FLAG_BEGIN | FLAG_END, payload.clone())];
    }
    let mut out = Vec::new();
    let mut off = 0usize;
    let mut seq = first_seq;
    while off < payload.len() {
        let end = (off + max_chunk).min(payload.len());
        let mut flags = 0u8;
        if off == 0 {
            flags |= FLAG_BEGIN;
        }
        if end == payload.len() {
            flags |= FLAG_END;
        }
        out.push((seq, flags, payload.slice(off..end)));
        off = end;
        seq += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::udp::wire::{FLAG_BEGIN, FLAG_END};
    use bytes::Bytes;

    #[test]
    fn single_fragment_when_small() {
        let p = Bytes::from_static(b"short");
        let frags = fragment(&p, 10, 1408);
        assert_eq!(frags.len(), 1);
        assert_eq!(frags[0].0, 10);
        assert_eq!(frags[0].1, FLAG_BEGIN | FLAG_END);
        assert_eq!(&frags[0].2[..], b"short");
    }

    #[test]
    fn splits_large_payload_with_begin_end() {
        let p = Bytes::from(vec![0u8; 5000]);
        let frags = fragment(&p, 0, 1408);
        assert!(frags.len() >= 4);
        assert_eq!(frags[0].1 & FLAG_BEGIN, FLAG_BEGIN);
        assert_eq!(frags[0].1 & FLAG_END, 0);
        let last = frags.last().unwrap();
        assert_eq!(last.1 & FLAG_END, FLAG_END);
        // seqs are contiguous from first_seq
        for (i, f) in frags.iter().enumerate() {
            assert_eq!(f.0, i as u64);
        }
        // reassembled bytes equal the input
        let total: usize = frags.iter().map(|f| f.2.len()).sum();
        assert_eq!(total, 5000);
    }
}
