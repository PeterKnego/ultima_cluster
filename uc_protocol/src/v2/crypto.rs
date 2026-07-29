// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M8 wire-crypto layouts (spec: docs/superpowers/specs/2026-07-28-uc2-wire-crypto-design.md §4).
//! Core-only: offsets, constants, and pure codecs. No crypto code lives here —
//! that is `uc2_crypto`'s job. A sealed datagram is:
//!
//! ```text
//! [ 16B header (cleartext, used as AAD; carries key_epoch) ]
//! [  8B nonce counter (u64 LE, per-sender monotonic)       ]
//! [  ciphertext                                            ]
//! [ 16B AES-256-GCM tag                                    ]
//! ```
//!
//! The 96-bit GCM nonce a peer must reconstruct from the wire counter is
//! `0u32 ‖ counter`, with `counter` placed **big-endian** in the low 8
//! bytes — note this is the opposite byte order from the counter's own
//! on-wire encoding above (`u64 LE`). The wire field and the nonce built
//! from it are two different things: only the LE field is transmitted; the
//! big-endian nonce is an ephemeral value both sides derive identically and
//! never send. Getting this order backwards in a second-language
//! implementation does not error — it produces a self-consistent peer that
//! silently fails to interoperate (every datagram it seals or opens
//! disagrees with a peer using the canonical order), so this sentence is
//! the only thing pinning it: this crate is core-only and carries no crypto
//! code to enforce it by type. See `uc2_crypto::seal` for the actual
//! AEAD implementation and its known-answer test.

/// Per-sender monotonic counter; the low 64 bits of the 96-bit GCM nonce.
pub const COUNTER_LEN: usize = 8;
/// AES-256-GCM authentication tag.
pub const TAG_LEN: usize = 16;
/// Bytes a seal adds on top of `DATAGRAM_HEADER_LEN`.
pub const CRYPTO_OVERHEAD: usize = COUNTER_LEN + TAG_LEN;

/// Noise IK message 1 (initiator -> responder). Body is opaque to this layer.
pub const DGRAM_KIND_HS_INIT: u8 = 18;
/// Noise IK message 2 (responder -> initiator).
pub const DGRAM_KIND_HS_RESP: u8 = 19;
/// Group-key delivery/ack over an established pairwise channel.
pub const DGRAM_KIND_HS_KEY: u8 = 20;

/// `buf` must be at least [`COUNTER_LEN`] bytes.
pub fn write_counter(buf: &mut [u8], counter: u64) {
    buf[..COUNTER_LEN].copy_from_slice(&counter.to_le_bytes());
}

/// `buf` must be at least [`COUNTER_LEN`] bytes.
pub fn read_counter(buf: &[u8]) -> u64 {
    u64::from_le_bytes(buf[..COUNTER_LEN].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overhead_is_counter_plus_tag() {
        assert_eq!(COUNTER_LEN, 8);
        assert_eq!(TAG_LEN, 16);
        assert_eq!(CRYPTO_OVERHEAD, COUNTER_LEN + TAG_LEN);
        assert_eq!(CRYPTO_OVERHEAD, 24);
    }

    #[test]
    fn kinds_do_not_collide_with_m7_admin() {
        // 16/17 are CONFIG_PROPOSAL/CONFIG_REPLY (M7). Crypto starts at 18.
        assert_eq!(DGRAM_KIND_HS_INIT, 18);
        assert_eq!(DGRAM_KIND_HS_RESP, 19);
        assert_eq!(DGRAM_KIND_HS_KEY, 20);
    }

    #[test]
    fn counter_round_trips_little_endian() {
        let mut buf = [0u8; COUNTER_LEN];
        write_counter(&mut buf, 0x0102_0304_0506_0708);
        assert_eq!(buf[0], 0x08, "little-endian, as every other v2 field");
        assert_eq!(read_counter(&buf), 0x0102_0304_0506_0708);
    }
}
