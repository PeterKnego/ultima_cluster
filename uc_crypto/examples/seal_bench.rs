// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! **AES-NI dispatch check** for the M8 wire-crypto gate — and, on a new host,
//! the operator's way to confirm the same thing before trusting a throughput
//! number (runbook §11).
//!
//! ```text
//! cargo run -p uc_crypto --release --example seal_bench
//! ```
//!
//! ## Why this exists
//!
//! The M8 seal path was measured at ~1.2 µs per 1368-byte datagram ≈ 1.1 GB/s.
//! The `aes` crate picks its backend at RUNTIME via CPUID (`cpufeatures`), so
//! a build that looks identical can be running either hardware AES-NI or the
//! constant-time bitsliced software fallback — and those differ by roughly an
//! order of magnitude. The T5 review flagged it directly: *sanity-check AES-NI
//! dispatch is actually engaging BEFORE the throughput gate, because that is
//! the number the ≤10% bar rides on.* A gate measured on the software fallback
//! would be measuring a system nobody ships.
//!
//! ## How to read the result
//!
//! This binary times [`seal_with`]/[`open_with`] over a realistic MTU-sized
//! datagram, using the same cipher-reuse pattern the real fan-out path uses
//! (one `Aes256Gcm` built per epoch, N seals through it — see
//! `transport.rs`'s per-epoch cache).
//!
//! The check is a comparison, not an absolute threshold, because absolute
//! numbers vary by host. Run it twice:
//!
//! ```text
//! cargo run -p uc_crypto --release --example seal_bench                    # as shipped
//! RUSTFLAGS="--cfg aes_force_soft --cfg polyval_force_soft" \
//!   cargo run -p uc_crypto --release --example seal_bench                  # forced software
//! ```
//!
//! If the first is several times faster than the second, hardware dispatch is
//! engaging. If the two are within noise of each other, they are running the
//! same (software) code and any throughput number taken on that host describes
//! the wrong system.
//!
//! `--cfg aes_force_soft` / `--cfg polyval_force_soft` are the `aes` and
//! `polyval` crates' own documented escape hatches for exactly this; nothing
//! in UC reads them.

use std::hint::black_box;
use std::time::Instant;

use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::{Aes256Gcm, KeyInit};
use uc_crypto::seal::{open_with, seal_with};
use uc_protocol::v2::datagram::DATAGRAM_HEADER_LEN;

/// A realistic sealed datagram: MTU 1408 minus the 16-byte cleartext header,
/// the 8-byte counter and the 16-byte tag leaves 1368 bytes of payload — the
/// exact size the T5 review's ~1.2 µs figure was taken at.
const PAYLOAD_LEN: usize = 1368;
const ITERS: usize = 200_000;

fn main() {
    let key = [0x5Au8; 32];
    let cipher = Aes256Gcm::new(GenericArray::from_slice(&key));

    // Staged exactly as the sender agent stages one: header then payload.
    let template: Vec<u8> = {
        let mut v = vec![0u8; DATAGRAM_HEADER_LEN];
        v.extend(std::iter::repeat_n(0xABu8, PAYLOAD_LEN));
        v
    };

    // Warm up: first-call CPUID dispatch, page faults, branch predictors.
    let mut buf = template.clone();
    for i in 0..1000u64 {
        buf.clear();
        buf.extend_from_slice(&template);
        seal_with(&mut buf, &cipher, i + 1).expect("seal");
        open_with(&mut buf, &cipher).expect("open");
    }

    let mut sealed_bytes = 0usize;
    let t0 = Instant::now();
    for i in 0..ITERS as u64 {
        buf.clear();
        buf.extend_from_slice(&template);
        seal_with(&mut buf, &cipher, i + 1).expect("seal");
        sealed_bytes += buf.len();
        black_box(&buf);
    }
    let seal_elapsed = t0.elapsed();

    // Open is measured separately (re-sealing each iteration so every open
    // sees a fresh valid tag), because the receive path is one-sided: a
    // follower opens 1× for every N the leader seals.
    let t1 = Instant::now();
    let mut opened = 0u64;
    for i in 0..ITERS as u64 {
        buf.clear();
        buf.extend_from_slice(&template);
        seal_with(&mut buf, &cipher, i + 1).expect("seal");
        opened += open_with(&mut buf, &cipher).expect("open");
        black_box(&buf);
    }
    let round_trip_elapsed = t1.elapsed();
    assert!(opened > 0, "counters must come back");

    let seal_ns = seal_elapsed.as_nanos() as f64 / ITERS as f64;
    let rt_ns = round_trip_elapsed.as_nanos() as f64 / ITERS as f64;
    let open_ns = rt_ns - seal_ns;
    let gbs = sealed_bytes as f64 / seal_elapsed.as_secs_f64() / 1e9;

    println!("================= uc_crypto seal bench (AES-NI check) =================");
    println!(
        "payload               : {PAYLOAD_LEN} B  (sealed datagram {} B)",
        buf.len()
    );
    println!("iterations            : {ITERS}");
    println!("seal                  : {seal_ns:.1} ns/datagram   ({gbs:.2} GB/s)");
    println!("seal+open round trip  : {rt_ns:.1} ns/datagram");
    println!("open (by difference)  : {open_ns:.1} ns/datagram");
    println!("------------------------------------------------------------------------");
    println!(
        "Compare against a forced-software build to confirm hardware dispatch:\n  \
         RUSTFLAGS=\"--cfg aes_force_soft --cfg polyval_force_soft\" \\\n    \
         cargo run -p uc_crypto --release --example seal_bench\n\
         Several-fold slower there = AES-NI is engaging here. Within noise = it is not."
    );
    println!("========================================================================");
}
