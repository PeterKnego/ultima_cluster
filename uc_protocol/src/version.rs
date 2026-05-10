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

pub const CURRENT: ProtocolVersion = ProtocolVersion::new(0, 1, 0);
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
