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
pub mod mpsc;
pub mod spsc;

pub use broadcast::{BroadcastConsumer, BroadcastProducer, BroadcastRing};
pub use common::{FrameHeader, RecordHeader, RingError, RingHeader};
pub use mpsc::{MpscConsumer, MpscProducer, MpscRing};
pub use spsc::{SpscConsumer, SpscProducer, SpscRing};
