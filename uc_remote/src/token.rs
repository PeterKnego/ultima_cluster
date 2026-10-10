// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The read-your-writes token.

/// The same token as `uc_client`'s `ReadToken`, carried across processes as
/// text; convert between the two with `as_u64`/`from_u64`.
///
/// A position in the log: a read carrying it is answered only from state
/// applied at least that far. `NONE` (0) means "no constraint".
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadToken(u64);

impl ReadToken {
    pub const NONE: ReadToken = ReadToken(0);

    pub const fn from_u64(v: u64) -> ReadToken {
        ReadToken(v)
    }

    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for ReadToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl std::str::FromStr for ReadToken {
    type Err = std::num::ParseIntError;
    fn from_str(s: &str) -> Result<ReadToken, Self::Err> {
        u64::from_str_radix(s, 16).map(ReadToken)
    }
}

#[cfg(test)]
mod tests {
    use super::ReadToken;
    use uc_protocol::v2::ipc::ReadToken as Owner;

    #[test]
    fn text_format_matches_uc_protocols_token() {
        for v in [0u64, 1, u64::MAX, 0xdead_beef] {
            let (a, b) = (ReadToken::from_u64(v), Owner::from_u64(v));
            assert_eq!(a.to_string(), b.to_string());
            assert_eq!(a.to_string().parse::<ReadToken>().unwrap(), a);
            assert_eq!(b.to_string().parse::<ReadToken>().unwrap().as_u64(), v);
            assert_eq!(a.to_string().parse::<Owner>().unwrap().as_u64(), v);
        }
        assert!("zz".parse::<ReadToken>().is_err());
    }
}
