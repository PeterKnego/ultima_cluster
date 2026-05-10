/// Stable u16 error codes for cross-crate / cross-process error transport.
/// Code values MUST NOT be reused; deprecate by name only.
#[repr(u16)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum ErrorCode {
    Unknown = 0,
    AppIdMismatch = 1,
    ProtocolMismatch = 2,
    InstanceIdChanged = 3,
    NotLeader = 10,
    Stalled = 11,
    ApplyFailed = 20,
    QueryFailed = 21,
    SnapshotFailed = 30,
    OutputRetryable = 40,
    OutputPermanent = 41,
    BadFrame = 50,
    Timeout = 60,
}

impl ErrorCode {
    pub const fn as_u16(self) -> u16 {
        self as u16
    }

    pub const fn from_u16(v: u16) -> Self {
        match v {
            1 => Self::AppIdMismatch,
            2 => Self::ProtocolMismatch,
            3 => Self::InstanceIdChanged,
            10 => Self::NotLeader,
            11 => Self::Stalled,
            20 => Self::ApplyFailed,
            21 => Self::QueryFailed,
            30 => Self::SnapshotFailed,
            40 => Self::OutputRetryable,
            41 => Self::OutputPermanent,
            50 => Self::BadFrame,
            60 => Self::Timeout,
            _ => Self::Unknown,
        }
    }
}
