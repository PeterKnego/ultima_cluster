//! Shared-memory ring buffer primitives.
//!
//! Three flavors:
//!   * [`spsc`] — single-producer single-consumer. Used for service↔node
//!     (apply/query/output rings). Tight, lock-free, no CAS on the hot path.
//!   * [`mpsc`] — many-producers single-consumer. Used for clients→node
//!     submit ring (M4) and cnc control rings.
//!   * [`broadcast`] — single-producer many-consumers, no backpressure. Used
//!     for node→clients response ring (M4).
//!
//! All ring files share the same on-disk header (`RingHeader`) and per-record
//! framing (`FrameHeader` + payload + crc32 trailer) defined in [`common`].

pub mod broadcast;
pub mod common;
mod futex;
pub mod mpsc;
pub mod spsc;

pub use broadcast::{BroadcastConsumer, BroadcastProducer, BroadcastRing};
pub use common::{
    BUSY_SPIN_CHUNK, COMMIT_CLAIMED, COMMIT_LAP_MASK, COMMIT_LAP_SHIFT, COMMIT_LEN_MASK,
    FrameHeader, MPSC_MAX_RECORD_BYTES, PARK_CEIL, ParkMode, RecordHeader, RingError, RingHeader,
    RingWaitHandle, SPIN_TRIES, SlotState, classify_commit_word, create_shared_backing_file,
    decode_record_slice, encode_commit_word, lap_of,
};
pub use mpsc::{MpscConsumer, MpscProducer, MpscRing};
pub use spsc::{SpscConsumer, SpscProducer, SpscRing};
