// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Wall-clock "now" ([`now_unix_ns`]) and RFC 3339 rendering for machine-read
//! output (the cluster view). UTC only,
//! always nine fraction digits, so every rendered time has one fixed width
//! and sorts lexically. Dependency-free on purpose (this crate is the leaf
//! every daemon shares).

const NS_PER_SEC: u64 = 1_000_000_000;

/// Unix nanoseconds now, from the wall clock; `0` if the clock reads
/// before the epoch. The one "now" every daemon's `/healthz`/`/readyz` and
/// `/metrics` compare heartbeats against (#173: shared by `uc_node` and
/// `uc2-gateway`).
pub fn now_unix_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// `ns` nanoseconds since the Unix epoch as `YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ`.
pub fn rfc3339_ns(ns: u64) -> String {
    let secs = ns / NS_PER_SEC;
    let frac = ns % NS_PER_SEC;
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{frac:09}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

/// Howard Hinnant's `civil_from_days`: proleptic Gregorian, day 0 = 1970-01-01.
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::rfc3339_ns;

    #[test]
    fn renders_the_epoch_a_leap_day_and_the_largest_u64() {
        assert_eq!(rfc3339_ns(0), "1970-01-01T00:00:00.000000000Z");
        // 2024-02-29T12:34:56.000000789Z
        assert_eq!(
            rfc3339_ns(1_709_210_096_000_000_789),
            "2024-02-29T12:34:56.000000789Z"
        );
        // u64::MAX ns is 2554-07-21T23:34:33.709551615Z.
        assert_eq!(rfc3339_ns(u64::MAX), "2554-07-21T23:34:33.709551615Z");
    }
}
