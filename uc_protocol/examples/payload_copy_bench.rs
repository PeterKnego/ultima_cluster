//! Microbench: payload **memcpy** vs **refcount-clone** (the `bytes::Bytes` /
//! `Arc` handoff model) across the hot-path payload sizes, for the Aeron-vs-UC
//! threading investigation (§5, candidates C-1/C-2 in §2).
//!
//! `Bytes::clone` is an `Arc` refcount bump — size-independent. This bench uses
//! `Arc<[u8]>::clone` (identical mechanism, no `bytes` dep needed) vs a real
//! `copy_from_slice` memcpy, so we can price what a removable copy costs vs the
//! refcount handoff it would become.
//!
//! Sizes 64 / 256 / 4096 B come from the §2 census: hot-path `payload_buf`
//! preallocates 4096 B and typical KV payloads are tens–hundreds of bytes.
//!
//! Run: `cargo run -p uc_protocol --release --example payload_copy_bench`

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

const SIZES: &[usize] = &[64, 256, 4096];
const ITERS: u32 = 5_000_000;

fn bench_memcpy(src: &[u8]) -> f64 {
    let mut dst = vec![0u8; src.len()];
    // warm
    dst.copy_from_slice(src);
    let start = Instant::now();
    for _ in 0..ITERS {
        dst.copy_from_slice(black_box(src));
        black_box(&dst);
    }
    start.elapsed().as_nanos() as f64 / ITERS as f64
}

fn bench_refcount_clone(src: &Arc<[u8]>) -> f64 {
    let start = Instant::now();
    for _ in 0..ITERS {
        let c = Arc::clone(black_box(src));
        black_box(&c);
        // drop(c) here decrements; we measure clone+drop = the steady-state
        // cost of passing a refcounted payload through one hop.
    }
    start.elapsed().as_nanos() as f64 / ITERS as f64
}

fn main() {
    println!("== payload copy microbench (release) ==");
    println!("{:<10} {:>14} {:>18}", "size(B)", "memcpy ns", "Arc-clone ns");
    for &n in SIZES {
        let src: Vec<u8> = (0..n).map(|i| i as u8).collect();
        let arc: Arc<[u8]> = Arc::from(src.clone().into_boxed_slice());
        let cp = bench_memcpy(&src);
        let rc = bench_refcount_clone(&arc);
        println!("{n:<10} {cp:>14.2} {rc:>18.2}");
    }
    println!(
        "\nArc-clone is size-independent (refcount bump); memcpy grows with size.\n\
         A removable copy (C-1/C-2) saves ~memcpy(size); below ~100ns it is low-priority."
    );
}
