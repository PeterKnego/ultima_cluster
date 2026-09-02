// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! FSM identity (spec `2026-09-02-uc2-fsm-identity-design.md` §3): the name a
//! state machine declares in code, its FROZEN hash, and the packed per-FSM
//! version. `core`-only — the node, the service SDK, the client, the cnc page
//! and the wire all use exactly these rules and this hash.

use core::fmt;

/// Bytes, not chars: a name is ASCII, so the two agree.
pub const FSM_NAME_MAX_LEN: usize = 32;

/// A validated FSM name: 1..=32 bytes of `[a-z0-9_-]`, first byte a letter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsmName {
    bytes: [u8; FSM_NAME_MAX_LEN],
    len: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameError {
    Empty,
    TooLong(usize),
    BadFirstByte(u8),
    BadByte(u8),
}

impl fmt::Display for NameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NameError::Empty => write!(f, "FSM name is empty"),
            NameError::TooLong(n) => write!(f, "FSM name is {n} bytes, max {FSM_NAME_MAX_LEN}"),
            NameError::BadFirstByte(b) => {
                write!(f, "FSM name must start with a-z, got {:?}", *b as char)
            }
            NameError::BadByte(b) => write!(
                f,
                "FSM name may contain only a-z 0-9 _ -, got {:?}",
                *b as char
            ),
        }
    }
}

/// FNV-1a 64 over `bytes`. FROZEN: this is what goes on the wire and into
/// `IdGen`; changing it is a flag day.
pub const fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut i = 0;
    while i < bytes.len() {
        h ^= bytes[i] as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
        i += 1;
    }
    h
}

impl FsmName {
    pub const fn parse(s: &str) -> Result<FsmName, NameError> {
        let b = s.as_bytes();
        if b.is_empty() {
            return Err(NameError::Empty);
        }
        if b.len() > FSM_NAME_MAX_LEN {
            return Err(NameError::TooLong(b.len()));
        }
        if !b[0].is_ascii_lowercase() {
            return Err(NameError::BadFirstByte(b[0]));
        }
        let mut out = [0u8; FSM_NAME_MAX_LEN];
        let mut i = 0;
        while i < b.len() {
            let c = b[i];
            if !(c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' || c == b'-') {
                return Err(NameError::BadByte(c));
            }
            out[i] = c;
            i += 1;
        }
        Ok(FsmName {
            bytes: out,
            len: b.len() as u8,
        })
    }

    /// For `const` contexts (the trait's provided `IDENTITY`): a bad name is a
    /// compile-time error at the first use.
    pub const fn parse_or_panic(s: &str) -> FsmName {
        match Self::parse(s) {
            Ok(n) => n,
            Err(_) => {
                panic!("invalid FSM NAME: 1..=32 bytes of [a-z0-9_-], starting with a letter")
            }
        }
    }

    pub fn as_str(&self) -> &str {
        // ASCII by construction, so this cannot fail.
        core::str::from_utf8(&self.bytes[..self.len as usize]).unwrap_or("")
    }

    /// Hash of exactly the name's bytes (no padding).
    pub const fn hash(&self) -> u64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        let mut i = 0;
        while i < self.len as usize {
            h ^= self.bytes[i] as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
            i += 1;
        }
        h
    }

    /// The 32-byte NUL-padded form the cnc slot line carries.
    pub const fn padded(&self) -> [u8; FSM_NAME_MAX_LEN] {
        self.bytes
    }

    /// Inverse of [`padded`](Self::padded). All-zero (an undeclared row) or a
    /// line that fails the rules is `None` — a shared-memory page never panics
    /// an attacher.
    pub fn from_padded(b: &[u8; FSM_NAME_MAX_LEN]) -> Option<FsmName> {
        let len = b.iter().position(|&c| c == 0).unwrap_or(FSM_NAME_MAX_LEN);
        if len == 0 {
            return None;
        }
        let s = core::str::from_utf8(&b[..len]).ok()?;
        FsmName::parse(s).ok()
    }
}

/// `major:8 ‖ minor:8 ‖ patch:16` — the same packing as `ProtocolVersion`
/// (Aeron's `SemanticVersion` is 8/8/8; both order as integers). FROZEN.
pub const fn pack_version(major: u8, minor: u8, patch: u16) -> u32 {
    ((major as u32) << 24) | ((minor as u32) << 16) | patch as u32
}

pub const fn unpack_version(v: u32) -> (u8, u8, u16) {
    ((v >> 24) as u8, (v >> 16) as u8, v as u16)
}

/// `Display` for a packed version: `"1.2.3"`, or `"unversioned"` for `0`.
pub struct VersionDisplay(pub u32);

impl fmt::Display for VersionDisplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 == 0 {
            return f.write_str("unversioned");
        }
        let (a, b, c) = unpack_version(self.0);
        write!(f, "{a}.{b}.{c}")
    }
}

/// What a state machine type IS: its name (identity) and its logic version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FsmIdentity {
    pub name: FsmName,
    pub version: u32,
}

impl FsmIdentity {
    pub const fn parse(name: &str, version: u32) -> FsmIdentity {
        FsmIdentity {
            name: FsmName::parse_or_panic(name),
            version,
        }
    }
    pub const fn hash(&self) -> u64 {
        self.name.hash()
    }
    /// The 32-bit fold `IdGen` mixes in (spec §3.4).
    pub const fn fold32(&self) -> u32 {
        let h = self.hash();
        (h >> 32) as u32 ^ h as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_rules_table() {
        for ok in [
            "a",
            "kv",
            "orders",
            "order-book_v2",
            "a234567890123456789012345678901",
        ] {
            assert!(FsmName::parse(ok).is_ok(), "{ok}");
        }
        assert_eq!(FsmName::parse(""), Err(NameError::Empty));
        assert_eq!(
            FsmName::parse("a23456789012345678901234567890123"),
            Err(NameError::TooLong(33))
        );
        assert_eq!(FsmName::parse("1abc"), Err(NameError::BadFirstByte(b'1')));
        assert_eq!(FsmName::parse("_abc"), Err(NameError::BadFirstByte(b'_')));
        assert_eq!(FsmName::parse("Orders"), Err(NameError::BadFirstByte(b'O')));
        assert_eq!(FsmName::parse("ord ers"), Err(NameError::BadByte(b' ')));
        assert_eq!(FsmName::parse("ordérs"), Err(NameError::BadByte(0xC3)));
        assert_eq!(FsmName::parse("kv").unwrap().as_str(), "kv");
    }

    /// FROZEN: FNV-1a 64 published vectors. Never change these.
    #[test]
    fn fnv1a_64_golden_vectors() {
        assert_eq!(fnv1a_64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a_64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a_64(b"foobar"), 0x8594_4171_f739_67e8);
        // The name hash is the hash of exactly the name's bytes, no padding.
        assert_eq!(FsmName::parse("a").unwrap().hash(), fnv1a_64(b"a"));
    }

    #[test]
    fn padded_roundtrip_and_zero_line_is_none() {
        let n = FsmName::parse("orders").unwrap();
        let p = n.padded();
        assert_eq!(&p[..6], b"orders");
        assert!(p[6..].iter().all(|&b| b == 0));
        assert_eq!(FsmName::from_padded(&p), Some(n));
        assert_eq!(FsmName::from_padded(&[0u8; 32]), None);
        let mut bad = p;
        bad[3] = b' ';
        assert_eq!(
            FsmName::from_padded(&bad),
            None,
            "a corrupt line is not a name"
        );
    }

    #[test]
    fn identity_is_const_and_fold32_is_stable() {
        const ID: FsmIdentity = FsmIdentity::parse("orders", pack_version(1, 2, 3));
        assert_eq!(ID.name.as_str(), "orders");
        assert_eq!(ID.version, 0x0102_0003);
        let h = ID.hash();
        assert_eq!(ID.fold32(), (h >> 32) as u32 ^ h as u32);
        assert_eq!(unpack_version(ID.version), (1, 2, 3));
        assert_eq!(VersionDisplay(ID.version).to_string(), "1.2.3");
        assert_eq!(VersionDisplay(0).to_string(), "unversioned");
    }
}
