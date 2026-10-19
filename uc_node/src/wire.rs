// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The binary's wire-level range and the two refusals a committed level can
//! trigger (wire-level spec §2.3). The check is one function so the cluster
//! agent calls it at boot (the recovered artifact), at every accepted
//! Settings apply, and at every artifact install.

use std::fmt;
use uc_protocol::WireRange;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireFault {
    AboveBinary { level: u8, range: WireRange },
    BelowBinary { level: u8, range: WireRange },
}

impl fmt::Display for WireFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            WireFault::AboveBinary { level, range } => write!(
                f,
                "WireLevelAboveBinary level={level} binary={}..{} — this cluster committed wire level \
                 {level} and this binary speaks at most {}: upgrade this node to a release whose wire \
                 minor is at least {level}",
                range.min, range.max, range.max
            ),
            WireFault::BelowBinary { level, range } => write!(
                f,
                "WireLevelBelowBinary level={level} binary={}..{} — this binary rolls from wire level {} \
                 and the cluster is at {level}: install the release at wire level {} first (one level at a \
                 time, stop-all included)",
                range.min,
                range.max,
                range.min,
                level + 1
            ),
        }
    }
}

pub fn check_level(level: u8, range: WireRange) -> Result<(), WireFault> {
    if level > range.max {
        Err(WireFault::AboveBinary { level, range })
    } else if level < range.min {
        Err(WireFault::BelowBinary { level, range })
    } else {
        Ok(())
    }
}

#[cfg_attr(not(feature = "wire-level-override"), allow(dead_code))]
pub(crate) fn parse_range(s: &str) -> Option<WireRange> {
    let (a, b) = s.trim().split_once(',')?;
    let (min, max) = (a.trim().parse::<u8>().ok()?, b.trim().parse::<u8>().ok()?);
    (min <= max && min != 0).then_some(WireRange { min, max })
}

/// `UC2_WIRE_RANGE=min,max`, honoured only under `wire-level-override`.
pub fn range_from_env() -> Option<WireRange> {
    #[cfg(feature = "wire-level-override")]
    {
        std::env::var("UC2_WIRE_RANGE")
            .ok()
            .and_then(|s| parse_range(&s))
    }
    #[cfg(not(feature = "wire-level-override"))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_level_names_the_side() {
        let r = WireRange { min: 12, max: 13 };
        assert_eq!(check_level(12, r), Ok(()));
        assert_eq!(check_level(13, r), Ok(()));
        assert_eq!(
            check_level(14, r),
            Err(WireFault::AboveBinary {
                level: 14,
                range: r
            })
        );
        assert_eq!(
            check_level(11, r),
            Err(WireFault::BelowBinary {
                level: 11,
                range: r
            })
        );
        assert!(
            WireFault::AboveBinary {
                level: 14,
                range: r
            }
            .to_string()
            .contains("WireLevelAboveBinary")
        );
        assert!(
            WireFault::BelowBinary {
                level: 11,
                range: r
            }
            .to_string()
            .contains("install the release at wire level 12")
        );
    }

    #[test]
    fn env_override_parses_min_comma_max_or_is_none() {
        assert_eq!(parse_range("12,13"), Some(WireRange { min: 12, max: 13 }));
        assert_eq!(parse_range("13,12"), None); // min above max
        assert_eq!(parse_range("x"), None);
    }
}
