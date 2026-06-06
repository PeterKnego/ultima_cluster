//! Service-side handshake helpers.
//!
//! After [`super::attach::attach`] succeeds, the service publishes a
//! `ServiceReady { last_applied }` frame on the cnc `control_to_node` MPSC
//! ring and transitions [`ServiceStatus::state`] from `Handshaking` to
//! `Ready`. The MPSC producer that targets the cnc control ring is
//! constructed in M3 Task 11 (it needs a sub-mmap-region attach API on
//! `MpscRing`); this module only provides the pure helpers and the
//! state-transition setter.

use std::sync::atomic::Ordering;

use uc_protocol::cnc::ServiceStatus;
use uc_protocol::handshake::{MSG_TYPE_SERVICE_READY, encode_extra_service_ready};
use uc_protocol::ring::RingError;
use uc_protocol::ring::mpsc::MpscProducer;

/// Publish `ServiceReady { last_applied }` to the cnc `control_to_node`
/// ring. Empty payload — all the information rides in `header_extra`.
pub fn send_service_ready(
    control_to_node: &MpscProducer,
    last_applied: u64,
) -> Result<(), RingError> {
    control_to_node.try_write(
        MSG_TYPE_SERVICE_READY,
        0,
        encode_extra_service_ready(last_applied),
        &[],
    )
}

/// Set `ServiceStatus::state`. Caller passes one of the
/// `cnc::service_state::*` constants. Release ordering so a node side
/// reading the state with Acquire observes any prior heartbeat / last_applied
/// writes by this service.
pub fn set_service_state(status: &ServiceStatus, state: u32) {
    status.state.store(state, Ordering::Release);
}

/// Publish the service's recovered `last_applied` (channel A). Call BEFORE
/// `set_service_state(.., READY)`; the state-flag Release carries it to the node.
/// A restarted service MUST overwrite it (0 for a fresh in-memory SM).
pub fn publish_service_last_applied(status: &ServiceStatus, last_applied: u64) {
    status.last_applied.store(last_applied, Ordering::Release);
}

/// Bump `service_epoch` so the node detects this incarnation as a reattach.
/// Call BEFORE `set_service_state(.., READY)`. Single live service writes it.
pub fn bump_service_epoch(status: &ServiceStatus) {
    let prev = status.service_epoch.load(Ordering::Relaxed);
    status.service_epoch.store(prev + 1, Ordering::Release);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, AtomicU64};

    use uc_protocol::cnc::service_state;
    use uc_protocol::handshake::decode_extra_service_ready;
    use uc_protocol::ring::common::{FRAME_HEADER_LEN, FRAME_TRAILER_LEN, RecordHeader};
    use uc_protocol::ring::mpsc::MpscRing;

    #[test]
    fn send_service_ready_round_trip() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let ring = MpscRing::create(
            tmp.path(),
            4096,
            (FRAME_HEADER_LEN + FRAME_TRAILER_LEN) as u32,
        )
        .expect("create");
        let (producer, mut consumer) = ring.into_split();

        send_service_ready(&producer, 42).expect("send");

        let mut buf = Vec::new();
        let RecordHeader {
            msg_type,
            flags: _,
            header_extra,
        } = consumer
            .try_read(&mut buf)
            .expect("read")
            .expect("non-empty");

        assert_eq!(msg_type, MSG_TYPE_SERVICE_READY);
        assert_eq!(decode_extra_service_ready(header_extra), 42);
        assert!(buf.is_empty(), "ServiceReady payload should be empty");
    }

    #[test]
    fn set_state_round_trip() {
        // Construct a standalone ServiceStatus (no mmap) to exercise the
        // state-setter atomic-store path. The exact byte layout doesn't
        // matter here — we only touch the `state` field.
        let status = ServiceStatus {
            state: AtomicU32::new(service_state::HANDSHAKING),
            _pad_1: 0,
            last_applied: AtomicU64::new(0),
            last_output_ack: AtomicU64::new(0),
            heartbeat_seq: AtomicU64::new(0),
            heartbeat_at_ns: AtomicU64::new(0),
            service_pid: AtomicU32::new(0),
            _pad_2a: 0,
            service_epoch: AtomicU64::new(0),
            _pad_2: [0; 8],
        };
        set_service_state(&status, service_state::READY);
        assert_eq!(status.state.load(Ordering::Acquire), service_state::READY);
    }

    #[test]
    fn publish_service_last_applied_stores_value() {
        let status = ServiceStatus {
            state: AtomicU32::new(service_state::HANDSHAKING),
            _pad_1: 0,
            last_applied: AtomicU64::new(0),
            last_output_ack: AtomicU64::new(0),
            heartbeat_seq: AtomicU64::new(0),
            heartbeat_at_ns: AtomicU64::new(0),
            service_pid: AtomicU32::new(0),
            _pad_2a: 0,
            service_epoch: AtomicU64::new(0),
            _pad_2: [0; 8],
        };
        publish_service_last_applied(&status, 99);
        assert_eq!(status.last_applied.load(Ordering::Acquire), 99);
    }

    #[test]
    fn bump_service_epoch_increments() {
        let status = ServiceStatus {
            state: AtomicU32::new(service_state::HANDSHAKING),
            _pad_1: 0,
            last_applied: AtomicU64::new(0),
            last_output_ack: AtomicU64::new(0),
            heartbeat_seq: AtomicU64::new(0),
            heartbeat_at_ns: AtomicU64::new(0),
            service_pid: AtomicU32::new(0),
            _pad_2a: 0,
            service_epoch: AtomicU64::new(0),
            _pad_2: [0; 8],
        };
        bump_service_epoch(&status);
        assert_eq!(status.service_epoch.load(Ordering::Acquire), 1);
        bump_service_epoch(&status);
        assert_eq!(status.service_epoch.load(Ordering::Acquire), 2);
    }
}
