// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Deterministic IDs (spec §3.4): one generator per apply call, built from
//! the frame's position and the FSM's identity; the same series on every
//! replica, whether it replayed the journal or installed a snapshot.

use std::marker::PhantomData;

use uc_protocol::identity::FsmIdentity;

// FROZEN round constants — a change is a flag day (spec §3.4).
const K0: u64 = 0x9E37_79B9_7F4A_7C15;
const K1: u64 = 0xD1B5_4A32_D192_ED03;
const K2: u64 = 0x8CB9_2BA7_2F3D_8DD7;

/// murmur3's 64-bit finalizer.
#[inline]
fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

/// Three-round Feistel over the two 64-bit halves. A Feistel network is a
/// bijection for ANY round function, so distinct inputs give distinct IDs by
/// construction. FROZEN.
#[inline]
pub(crate) fn permute(mut a: u64, mut b: u64) -> u128 {
    a ^= fmix64(b ^ K0);
    b ^= fmix64(a ^ K1);
    a ^= fmix64(b ^ K2);
    ((a as u128) << 64) | b as u128
}

#[cfg(test)]
pub(crate) fn unpermute(x: u128) -> (u64, u64) {
    let (mut a, mut b) = ((x >> 64) as u64, x as u64);
    a ^= fmix64(b ^ K2);
    b ^= fmix64(a ^ K1);
    a ^= fmix64(b ^ K0);
    (a, b)
}

/// The ID generator for ONE apply call. Obtain it from
/// [`ApplyCtx::ids`](crate::ApplyCtx::ids); never keep one across calls — a
/// stashed generator reintroduces the lifetime-counter divergence spec §3.4
/// describes, and the type is `!Send` so the obvious stash into a
/// `Send` state machine fails to compile.
pub struct IdGen {
    position: u64,
    fold: u32,
    ordinal: u32,
    _not_send: PhantomData<*const ()>,
}

impl IdGen {
    pub fn new(position: u64, identity: FsmIdentity) -> IdGen {
        IdGen {
            position,
            fold: identity.fold32(),
            ordinal: 0,
            _not_send: PhantomData,
        }
    }

    /// The next ID in this apply call's series. Input: `position ‖ ordinal ‖
    /// fold32(identity)`; output: the frozen permutation of it.
    ///
    /// Named `next`, not `Iterator::next`, on purpose (spec §3.4's public
    /// surface): `IdGen` is a minting call, not a sequence to iterate.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> u128 {
        let o = self.ordinal;
        self.ordinal = o
            .checked_add(1)
            .expect("IdGen: more than 2^32 IDs in one apply call");
        permute(self.position, ((o as u64) << 32) | self.fold as u64)
    }

    /// How many IDs this generator has minted.
    pub fn ordinal(&self) -> u32 {
        self.ordinal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::identity::FsmIdentity;

    const ORDERS: FsmIdentity = FsmIdentity::parse("orders", 0);
    const KV: FsmIdentity = FsmIdentity::parse("kv", 0);

    /// FROZEN golden vectors. Fill the `EXPECTED` values from the first run
    /// (`cargo test -p uc_service ids::tests::golden -- --nocapture` prints
    /// them), then NEVER change them: a replica on a different build must mint
    /// the same IDs.
    #[test]
    fn golden() {
        let mut g = IdGen::new(0, ORDERS);
        let a = g.next();
        let b = g.next();
        let mut h = IdGen::new(u64::MAX, KV);
        let c = h.next();
        eprintln!("golden: {a:#034x} {b:#034x} {c:#034x}");
        const EXPECTED: [u128; 3] = [
            0x63fd241ded9a1e75107b396d73cc9983,
            0x649c4e1b515159e04ac38a72c4abe7d8,
            0x6b3639883b961e80d2dcac069a1f4dcb,
        ]; // pinned from the first run (`eprintln!` above); FROZEN forever after.
        assert_eq!([a, b, c], EXPECTED);
    }

    #[test]
    fn permutation_is_a_bijection() {
        for &(a, b) in &[
            (0u64, 0u64),
            (1, 0),
            (0, 1),
            (u64::MAX, u64::MAX),
            (0xdead_beef, 0xcafe_babe),
        ] {
            assert_eq!(unpermute(permute(a, b)), (a, b));
        }
        // Exhaustive on a small sub-domain: 2^12 inputs, no collision.
        let mut seen = std::collections::HashSet::new();
        for a in 0..64u64 {
            for b in 0..64u64 {
                assert!(seen.insert(permute(a, b)));
            }
        }
    }

    #[test]
    fn consecutive_ordinals_and_positions_share_no_visible_structure() {
        let mut g = IdGen::new(1000, ORDERS);
        let x = g.next();
        let y = g.next();
        assert_ne!(x >> 64, y >> 64, "high halves differ");
        assert_ne!(x as u64, y as u64, "low halves differ");
        let z = IdGen::new(1001, ORDERS).next();
        assert_ne!(x, z);
        assert_eq!(g.ordinal(), 2);
    }

    #[test]
    fn two_identities_mint_disjoint_series_and_version_is_not_an_input() {
        let a = IdGen::new(5, ORDERS).next();
        let b = IdGen::new(5, KV).next();
        assert_ne!(a, b);
        const ORDERS_V2: FsmIdentity = FsmIdentity::parse("orders", 0x0200_0000);
        assert_eq!(
            IdGen::new(5, ORDERS_V2).next(),
            a,
            "an upgrade must not change what a replay mints"
        );
    }

    #[test]
    fn same_inputs_same_series() {
        let mut g1 = IdGen::new(42, ORDERS);
        let mut g2 = IdGen::new(42, ORDERS);
        for _ in 0..5 {
            assert_eq!(g1.next(), g2.next());
        }
    }
}
