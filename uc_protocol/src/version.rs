/// Encoded as packed u32: (major:u8 << 24) | (minor:u8 << 16) | (patch:u16).
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ProtocolVersion(pub u32);

impl ProtocolVersion {
    pub const fn new(major: u8, minor: u8, patch: u16) -> Self {
        Self(((major as u32) << 24) | ((minor as u32) << 16) | (patch as u32))
    }
    pub const fn major(self) -> u8 {
        (self.0 >> 24) as u8
    }
    pub const fn minor(self) -> u8 {
        ((self.0 >> 16) & 0xFF) as u8
    }
    pub const fn patch(self) -> u16 {
        (self.0 & 0xFFFF) as u16
    }

    /// Compatible if same major and `other.minor <= self.minor`.
    pub const fn compatible_with(self, other: ProtocolVersion) -> bool {
        self.major() == other.major() && other.minor() <= self.minor()
    }
}

// 0.4.0: M8 wire-crypto layouts — key_epoch header field (datagram.rs),
// crypto envelope constants + kinds 18-20 (v2::crypto). No cnc-page change:
// M8 alters the UDP datagram format, not the shmem cnc page, so
// `CNC_V2_VERSION` is deliberately left untouched — bumping it would refuse
// old service/client binaries at local IPC attach over a change that cannot
// affect them.
//
// NB (post-M7 loose-end): these two constants and `ProtocolVersion::
// compatible_with` are NOT on any live enforcement path — grep the workspace
// and `CURRENT` has no caller outside this file, `MIN_COMPATIBLE` is only
// re-exported (never read), and `compatible_with` is referenced only by this
// module's own tests and a doc-comment. The version actually gated at every
// IPC / peer handshake is the cnc-page field, checked by
// `uc_protocol::v2::cnc::version_compatible(local, peer)` over the packed
// `CNC_V2_VERSION` u32 (that module is `core`-only, so it re-spells the
// same-major / peer-minor-not-newer rule directly rather than depend on this
// type). The two version lines are INDEPENDENT, not lockstep: `CNC_V2_VERSION`
// gates the cnc shmem page format at local IPC attach and has its own history
// (stuck at major=2/minor=0 since M5 while `CURRENT` moved 0.1.0 through
// 0.4.0); `CURRENT` documents the semver of the wire *datagram* protocol but
// does not itself enforce anything.
pub const CURRENT: ProtocolVersion = ProtocolVersion::new(0, 4, 0);
pub const MIN_COMPATIBLE: ProtocolVersion = ProtocolVersion::new(0, 1, 0);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_round_trip() {
        let v = ProtocolVersion::new(1, 2, 3);
        assert_eq!(v.major(), 1);
        assert_eq!(v.minor(), 2);
        assert_eq!(v.patch(), 3);
    }

    #[test]
    fn compat_same_major_lower_minor_ok() {
        let a = ProtocolVersion::new(1, 5, 0);
        let b = ProtocolVersion::new(1, 3, 0);
        assert!(a.compatible_with(b));
    }

    #[test]
    fn compat_higher_minor_in_other_rejected() {
        let a = ProtocolVersion::new(1, 3, 0);
        let b = ProtocolVersion::new(1, 5, 0);
        assert!(!a.compatible_with(b));
    }

    #[test]
    fn compat_different_major_rejected() {
        let a = ProtocolVersion::new(1, 0, 0);
        let b = ProtocolVersion::new(2, 0, 0);
        assert!(!a.compatible_with(b));
    }
}
