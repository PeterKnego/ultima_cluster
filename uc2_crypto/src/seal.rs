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
}
