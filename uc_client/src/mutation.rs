// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Capstone tooth (read-your-writes spec, planning erratum 3): with the
//! `mutation-testing` feature AND `UC2_CLIENT_MUTATION=skip-min-position-guard`,
//! the engine hands an answer below its token up instead of turning it into
//! Retry. Without the feature this is a constant `false`.

#[cfg(feature = "mutation-testing")]
pub(crate) fn guard_disabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("UC2_CLIENT_MUTATION").as_deref() == Ok("skip-min-position-guard")
    })
}

#[cfg(not(feature = "mutation-testing"))]
#[inline(always)]
pub(crate) fn guard_disabled() -> bool {
    false
}
