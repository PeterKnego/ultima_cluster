//! AES-256-GCM seal/open over the M8 wire envelope (spec §4):
//!
//! ```text
//! [ 16B header (cleartext, AAD; carries key_epoch at offset 14) ]
//! [  8B nonce counter (u64 LE)                                   ]
//! [  ciphertext (the former payload)                             ]
//! [ 16B AES-256-GCM tag                                          ]
//! ```
//!
//! The header stays readable on the wire (the receiver demuxes by `kind` and
//! locates by `position` before it can even look up a key), but it is
//! authenticated as associated data: tampering with any header byte —
//! `position`, `leadership_term_id`, `kind`, `flags`, or `key_epoch` — fails
//! the tag exactly as tampering with the ciphertext would.
//!
//! # The nonce
//!
//! The 96-bit GCM nonce is `0u32 ‖ counter` (big-endian). The all-zero
//! 4-byte prefix is safe ONLY because the key passed in here is already
//! per-sender-per-boot (see `schedule::derive_send_key`): two different
//! senders, or the same sender across a restart, never share a key, so the
//! `(key, nonce)` pair this module builds is never repeated. Do not widen
//! the prefix or mix anything else into it — that would just be reinventing
//! `derive_send_key`'s job worse and in two places.
//!
//! # Untrusted input
//!
//! `open_in_place` is fed raw bytes off the network by anyone who can reach
//! the UDP port. Every length check here precedes every index — see
//! `a_truncated_datagram_is_rejected_not_indexed_out_of_bounds` below, which
//! sweeps every length from 0 up through one byte short of the minimum valid
//! sealed datagram.

use crate::CryptoError;
use aes_gcm::aead::{AeadInPlace, generic_array::GenericArray};
use aes_gcm::{Aes256Gcm, KeyInit};
use uc_protocol::v2::crypto::{COUNTER_LEN, CRYPTO_OVERHEAD, TAG_LEN, read_counter, write_counter};
use uc_protocol::v2::datagram::DATAGRAM_HEADER_LEN;

/// Builds the 96-bit GCM nonce `0u32 ‖ counter` (big-endian). Not secret —
/// the counter is the cleartext field this module itself writes into the
/// datagram — so this returns by value with no wrapping.
fn nonce_bytes(counter: u64) -> [u8; 12] {
    let mut n = [0u8; 12];
    n[4..].copy_from_slice(&counter.to_be_bytes());
    n
}

/// Seals `buf` in place: splices the 8-byte little-endian `counter` in right
/// after the existing `DATAGRAM_HEADER_LEN`-byte cleartext header, then
/// AES-256-GCM-encrypts everything after that (the payload) under AAD =
/// the header, appending the 16-byte tag.
///
/// `buf` is the caller's already-staged outgoing datagram (header written,
/// payload appended) — this never allocates a second buffer; the splice and
/// the tag append both grow `buf` in place, reusing whatever spare capacity
/// the caller's reusable send buffer already carries.
///
/// `key` is never copied into a bare, unwrapped local: it is borrowed
/// straight into `Aes256Gcm::new`, which is the only place its bytes are
/// read. The cipher built from it is a function-local dropped at the end of
/// this call, never stored.
pub fn seal_in_place(buf: &mut Vec<u8>, key: &[u8; 32], counter: u64) -> Result<(), CryptoError> {
    if buf.len() < DATAGRAM_HEADER_LEN {
        return Err(CryptoError::TooShort);
    }

    let mut counter_bytes = [0u8; COUNTER_LEN];
    write_counter(&mut counter_bytes, counter);
    buf.splice(DATAGRAM_HEADER_LEN..DATAGRAM_HEADER_LEN, counter_bytes);

    let nonce_arr = nonce_bytes(counter);
    let nonce = GenericArray::from_slice(&nonce_arr);
    let cipher = Aes256Gcm::new(GenericArray::from_slice(key));

    let (header, rest) = buf.split_at_mut(DATAGRAM_HEADER_LEN);
    let (_counter, payload) = rest.split_at_mut(COUNTER_LEN);
    let tag = cipher
        .encrypt_in_place_detached(nonce, &*header, payload)
        // Unreachable in practice (aes-gcm only errors here past the ~64GiB
        // GCM plaintext limit, far beyond any UC datagram), but this is
        // still a fallible crypto call touching our own staged buffer, not
        // an `unwrap` — no panic path is acceptable on this crate's surface.
        .map_err(|_| CryptoError::AuthFailed)?;
    buf.extend_from_slice(&tag);
    Ok(())
}

/// Opens `buf` in place: length-checks first, reads the plaintext counter,
/// AES-256-GCM-decrypts under AAD = the header (rejecting on any header or
/// ciphertext tamper, or a wrong key, as [`CryptoError::AuthFailed`]), then
/// strips the counter and tag so `buf` is left holding exactly
/// `header ++ plaintext` — the same layout [`seal_in_place`] started from.
/// Returns the counter for the caller's replay-window check
/// (`replay::ReplayWindow::check_and_set`); this function does not itself
/// consult replay state.
///
/// **Invariant the caller must uphold:** `buf.len()` must be exactly the
/// length of the received datagram — not a fixed-size receive buffer with
/// trailing unwritten/stale bytes. This function has no way to distinguish
/// "the sender's payload legitimately ended here" from "the caller handed
/// over garbage past the real datagram boundary"; both the AAD scope and
/// the ciphertext region are derived purely from `buf.len()`. A caller
/// reading into a reusable, oversized buffer must truncate it to the
/// socket read's reported byte count before calling this.
///
/// `key` is borrowed straight into `Aes256Gcm::new`, same discipline as
/// `seal_in_place`.
pub fn open_in_place(buf: &mut Vec<u8>, key: &[u8; 32]) -> Result<u64, CryptoError> {
    if buf.len() < DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD {
        return Err(CryptoError::TooShort);
    }

    let counter = read_counter(&buf[DATAGRAM_HEADER_LEN..DATAGRAM_HEADER_LEN + COUNTER_LEN]);
    let nonce_arr = nonce_bytes(counter);
    let nonce = GenericArray::from_slice(&nonce_arr);
    let cipher = Aes256Gcm::new(GenericArray::from_slice(key));

    let (header, rest) = buf.split_at_mut(DATAGRAM_HEADER_LEN);
    let (_counter, ciphertext_and_tag) = rest.split_at_mut(COUNTER_LEN);
    // Safe: the length check above guarantees
    // ciphertext_and_tag.len() == buf.len() - DATAGRAM_HEADER_LEN - COUNTER_LEN
    // >= TAG_LEN.
    let split_at = ciphertext_and_tag.len() - TAG_LEN;
    let (ciphertext, tag_bytes) = ciphertext_and_tag.split_at_mut(split_at);
    let tag = GenericArray::from_slice(tag_bytes);

    cipher
        .decrypt_in_place_detached(nonce, &*header, ciphertext, tag)
        .map_err(|_| CryptoError::AuthFailed)?;

    // Strip tag then counter so the caller sees the original header ++
    // plaintext layout. Order matters only for which indices are valid at
    // each step; both are plain in-place Vec shrinks, no allocation.
    let new_len = buf.len() - TAG_LEN;
    buf.truncate(new_len);
    buf.drain(DATAGRAM_HEADER_LEN..DATAGRAM_HEADER_LEN + COUNTER_LEN);
    Ok(counter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::v2::datagram::{DATAGRAM_HEADER_LEN, DatagramHeader, write_datagram_header};

    fn dgram(payload: &[u8], epoch: u16) -> Vec<u8> {
        let mut v = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(&mut v, &DatagramHeader {
            position: 4096, leadership_term_id: 3, kind: 1, flags: 0, key_epoch: epoch,
        });
        v.extend_from_slice(payload);
        v
    }

    #[test]
    fn round_trips_and_leaves_the_header_in_the_clear() {
        let key = [9u8; 32];
        let mut d = dgram(b"frames go here", 1);
        let header_before = d[..DATAGRAM_HEADER_LEN].to_vec();
        seal_in_place(&mut d, &key, 42).unwrap();
        assert_eq!(&d[..DATAGRAM_HEADER_LEN], &header_before[..], "header stays readable");
        assert_ne!(&d[DATAGRAM_HEADER_LEN + 8..], b"frames go here", "payload is sealed");

        assert_eq!(open_in_place(&mut d, &key).unwrap(), 42);
        assert_eq!(&d[DATAGRAM_HEADER_LEN..], b"frames go here");
    }

    #[test]
    fn tampering_with_the_header_fails_the_tag_because_it_is_aad() {
        let key = [9u8; 32];
        let mut d = dgram(b"payload", 1);
        seal_in_place(&mut d, &key, 1).unwrap();
        d[0] ^= 0xFF; // rewrite `position`
        assert!(matches!(open_in_place(&mut d, &key), Err(CryptoError::AuthFailed)));
    }

    #[test]
    fn tampering_with_ciphertext_tag_or_counter_all_fail() {
        let key = [9u8; 32];
        for byte in [DATAGRAM_HEADER_LEN, DATAGRAM_HEADER_LEN + 9, 0] {
            let mut d = dgram(b"payload", 1);
            seal_in_place(&mut d, &key, 1).unwrap();
            let idx = if byte == 0 { d.len() - 1 } else { byte };
            d[idx] ^= 0x01;
            assert!(
                matches!(open_in_place(&mut d, &key), Err(CryptoError::AuthFailed)),
                "flipping byte {idx} must not open"
            );
        }
    }

    #[test]
    fn a_wrong_key_fails_rather_than_producing_garbage() {
        let mut d = dgram(b"payload", 1);
        seal_in_place(&mut d, &[1u8; 32], 1).unwrap();
        assert!(matches!(open_in_place(&mut d, &[2u8; 32]), Err(CryptoError::AuthFailed)));
    }

    #[test]
    fn a_truncated_datagram_is_rejected_not_indexed_out_of_bounds() {
        // The untrusted-input contract: never panic.
        for len in 0..DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD {
            let mut d = vec![0u8; len];
            assert!(matches!(open_in_place(&mut d, &[1u8; 32]), Err(CryptoError::TooShort)));
        }
    }

    #[test]
    fn header_only_datagrams_seal_to_an_empty_payload() {
        // HEARTBEAT and APPEND_POSITION carry no body.
        let key = [9u8; 32];
        let mut d = dgram(b"", 1);
        seal_in_place(&mut d, &key, 7).unwrap();
        assert_eq!(d.len(), DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD);
        assert_eq!(open_in_place(&mut d, &key).unwrap(), 7);
        assert_eq!(d.len(), DATAGRAM_HEADER_LEN);
    }

    // -- Task 5 review remediation: the six tests above all pass against
    // two independently-verified wrong implementations (a no-encryption
    // mutant that authenticates everything but never encrypts the payload,
    // and an AAD-stops-at-byte-14 mutant that leaves `key_epoch`
    // unauthenticated). The tests below close those gaps plus two adjacent
    // ones the review flagged in the same pass.

    #[test]
    fn every_header_byte_is_authenticated() {
        // The only header-tamper coverage above flips d[0] (`position`).
        // An AAD that silently stopped at byte 14 would leave `key_epoch`
        // (offset 14..16) rewritable — a downgrade primitive against the
        // key-rotation path (M8 T8) — while still passing every test above.
        // Sweep every header byte individually.
        let key = [9u8; 32];
        for i in 0..DATAGRAM_HEADER_LEN {
            let mut d = dgram(b"payload", 1);
            seal_in_place(&mut d, &key, 1).unwrap();
            d[i] ^= 0xFF;
            assert!(
                matches!(open_in_place(&mut d, &key), Err(CryptoError::AuthFailed)),
                "header byte {i} is not authenticated"
            );
        }
    }

    #[test]
    fn payload_ciphertext_region_is_actually_sealed() {
        // The "payload is sealed" assertion in
        // round_trips_and_leaves_the_header_in_the_clear compares a 30-byte
        // slice (8B counter + 5B... here 15B ciphertext + 16B tag) against
        // the 14-byte plaintext array — Rust's slice equality is false on
        // length alone, so that assertion is vacuously true even against a
        // mutant that never encrypts anything. Compare only the ciphertext
        // region (same length as the plaintext) against the plaintext.
        let key = [9u8; 32];
        let mut d = dgram(b"frames go here", 1);
        seal_in_place(&mut d, &key, 42).unwrap();
        let ct_start = DATAGRAM_HEADER_LEN + COUNTER_LEN;
        let ct_end = d.len() - TAG_LEN;
        assert_ne!(&d[ct_start..ct_end], &b"frames go here"[..]);
    }

    #[test]
    fn counter_is_written_little_endian_on_the_wire() {
        // Pins the wire byte order, not just internal write/read
        // round-trip agreement — a peer implementation could agree with
        // itself on the wrong order and still fail to interoperate.
        let key = [9u8; 32];
        let mut d = dgram(b"x", 1);
        seal_in_place(&mut d, &key, 42).unwrap();
        assert_eq!(
            &d[DATAGRAM_HEADER_LEN..DATAGRAM_HEADER_LEN + COUNTER_LEN],
            &42u64.to_le_bytes()[..]
        );
    }

    #[test]
    fn nonce_does_not_truncate_the_counter() {
        // A nonce that dropped the counter's high bits would repeat a
        // (key, nonce) pair periodically (e.g. every 256 datagrams if only
        // the low byte survived) — catastrophic under GCM: a repeated
        // nonce under one key leaks the authentication subkey and breaks
        // every message ever sealed under that key, not just the one
        // repeat. Two counters that agree only in their low byte must
        // still seal to different ciphertext.
        //
        // Compares only the ciphertext-and-tag region, NOT the whole
        // datagram: the wire counter field itself always differs between
        // the two seals (that's just `write_counter` doing its job), so
        // comparing the full buffers would be exactly the same vacuous
        // mistake the review flagged in the mandated "payload is sealed"
        // check — always unequal regardless of whether the nonce actually
        // collided.
        let key = [9u8; 32];
        let mut a = dgram(b"same plaintext", 1);
        let mut b = a.clone();
        seal_in_place(&mut a, &key, 42).unwrap();
        seal_in_place(&mut b, &key, 42 + (1u64 << 40)).unwrap();
        let ct_and_tag_start = DATAGRAM_HEADER_LEN + COUNTER_LEN;
        assert_ne!(
            &a[ct_and_tag_start..],
            &b[ct_and_tag_start..],
            "counters differing above the low byte must not collide in the nonce"
        );
    }

    #[test]
    fn known_answer_test_pins_the_whole_sealed_envelope() {
        // Freezes the entire envelope — header AAD scope, nonce
        // construction, counter placement, ciphertext, tag — against
        // silent future drift. This is the multi-language wire format
        // (uc_protocol::v2::crypto); a second-language peer has nothing
        // but this kind of byte-exact vector to implement against.
        let key = [0x42u8; 32];
        let mut d = vec![0u8; DATAGRAM_HEADER_LEN];
        write_datagram_header(&mut d, &DatagramHeader {
            position: 4096,
            leadership_term_id: 3,
            kind: 1,
            flags: 0,
            key_epoch: 7,
        });
        d.extend_from_slice(b"hello");
        seal_in_place(&mut d, &key, 42).unwrap();
        assert_eq!(
            d,
            vec![
                // header: position=4096 LE, term=3 LE, kind=1, flags=0, key_epoch=7 LE
                0, 16, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 1, 0, 7, 0,
                // counter=42 LE
                42, 0, 0, 0, 0, 0, 0, 0,
                // ciphertext (5 bytes) ++ tag (16 bytes)
                0x1f, 0x6e, 0xff, 0x14, 0xe1, 0x85, 0x66, 0xea, 0x05, 0x95, 0xf8, 0x56, 0x38,
                0x7d, 0xd1, 0x6c, 0x3b, 0xe5, 0xa0, 0xbd, 0x58,
            ],
            "sealed envelope byte-for-byte KAT (key=[0x42;32], counter=42, \
             header {{position:4096, term:3, kind:1, flags:0, key_epoch:7}}, \
             payload=b\"hello\")"
        );
    }

    #[test]
    fn boundary_length_with_garbage_bytes_fails_closed() {
        // The truncation sweep in
        // a_truncated_datagram_is_rejected_not_indexed_out_of_bounds covers
        // 0..DATAGRAM_HEADER_LEN+CRYPTO_OVERHEAD but never that upper bound
        // itself — the exact minimal shape of a well-formed empty-payload
        // sealed frame (ciphertext_and_tag.len() == TAG_LEN, split_at ==
        // 0). A "nothing to decrypt, skip the tag check" special-case for
        // an empty ciphertext region would wrongly accept this: random
        // bytes, not a valid seal, must still fail closed rather than
        // either panic or silently open.
        let key = [9u8; 32];
        let mut d = vec![0x5Au8; DATAGRAM_HEADER_LEN + CRYPTO_OVERHEAD];
        assert!(matches!(open_in_place(&mut d, &key), Err(CryptoError::AuthFailed)));
    }
}
