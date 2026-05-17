//! Node-side ownership of the four service-side SPSC ring files
//! (`apply.ring`, `apply_resp.ring`, `query.ring`, `query_resp.ring`)
//! under `<instance_dir>/service/`.
//!
//! Ring sizing (chosen to keep one IPC hop's worth of buffering on each):
//! * apply / apply_resp — 64 MiB cap, 16 MiB max frame. Apply payloads
//!   are user commands and their bincode-encoded responses; sized so
//!   the leader can keep going if the service apply loop briefly stalls.
//! * query / query_resp — 16 MiB cap, 4 MiB max frame. Queries are
//!   typically smaller and the round-trip latency budget is tighter, so
//!   the queue depth is smaller.
//!
//! Ownership: this module produces onto `apply.ring` + `query.ring` and
//! consumes from `apply_resp.ring` + `query_resp.ring`. The service side
//! takes the opposite halves via [`uc_service::runtime::attach::attach`].

use std::path::Path;

use uc_protocol::ring::spsc::{SpscConsumer, SpscProducer, SpscRing};

use super::instance::IpcError;

/// 64 MiB cap, 16 MiB max single frame.
pub const APPLY_RING_CAP: u64 = 64 * 1024 * 1024;
pub const APPLY_RING_MAX_MSG: u32 = 16 * 1024 * 1024;
/// 16 MiB cap, 4 MiB max single frame.
pub const QUERY_RING_CAP: u64 = 16 * 1024 * 1024;
pub const QUERY_RING_MAX_MSG: u32 = 4 * 1024 * 1024;

/// Node-side halves of the four service rings. The matching halves
/// (consumer/producer pairs) live on the service side.
pub struct ServiceLink {
    pub apply_producer: SpscProducer,
    pub apply_resp_consumer: SpscConsumer,
    pub query_producer: SpscProducer,
    pub query_resp_consumer: SpscConsumer,
}

impl ServiceLink {
    /// Create all four ring files under `<instance_dir>/service/` and take
    /// the node-side halves. The service-side process picks up its halves
    /// via `uc_service::runtime::attach::attach`.
    ///
    /// Caller must ensure `<instance_dir>/service/` exists — typically
    /// created by [`super::instance::Instance::create`] just before this.
    pub fn create(instance_dir: &Path) -> Result<Self, IpcError> {
        let service_dir = instance_dir.join("service");

        let apply = SpscRing::create(
            &service_dir.join("apply.ring"),
            APPLY_RING_CAP,
            APPLY_RING_MAX_MSG,
        )?;
        let apply_resp = SpscRing::create(
            &service_dir.join("apply_resp.ring"),
            APPLY_RING_CAP,
            APPLY_RING_MAX_MSG,
        )?;
        let query = SpscRing::create(
            &service_dir.join("query.ring"),
            QUERY_RING_CAP,
            QUERY_RING_MAX_MSG,
        )?;
        let query_resp = SpscRing::create(
            &service_dir.join("query_resp.ring"),
            QUERY_RING_CAP,
            QUERY_RING_MAX_MSG,
        )?;

        let (apply_producer, _) = apply.into_split();
        let (_, apply_resp_consumer) = apply_resp.into_split();
        let (query_producer, _) = query.into_split();
        let (_, query_resp_consumer) = query_resp.into_split();

        Ok(ServiceLink {
            apply_producer,
            apply_resp_consumer,
            query_producer,
            query_resp_consumer,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::frames::apply::{
        MSG_TYPE_APPLY, MSG_TYPE_APPLY_RESP, decode_extra_apply, encode_extra_apply,
        encode_flags_apply,
    };
    use uc_protocol::ring::spsc::SpscRing;

    /// `ServiceLink::create` is normally driven by `Instance::create`'s
    /// service-dir prep; the test wires that up directly.
    fn fresh_instance_dir() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("service")).unwrap();
        tmp
    }

    #[test]
    fn create_writes_four_ring_files() {
        let tmp = fresh_instance_dir();
        let _link = ServiceLink::create(tmp.path()).expect("create");

        let service_dir = tmp.path().join("service");
        for name in [
            "apply.ring",
            "apply_resp.ring",
            "query.ring",
            "query_resp.ring",
        ] {
            let p = service_dir.join(name);
            assert!(p.is_file(), "{} not created", p.display());
            let meta = std::fs::metadata(&p).unwrap();
            assert!(meta.len() > 0, "{} is empty", p.display());
        }
    }

    /// End-to-end across the SPSC boundary: node-side `ServiceLink`
    /// produces onto `apply.ring`; a separate `SpscRing::open` on the
    /// same file gets the consumer half (simulating the service side).
    #[test]
    fn apply_producer_round_trips_to_service_side_consumer() {
        let tmp = fresh_instance_dir();
        let mut link = ServiceLink::create(tmp.path()).expect("create");

        let service_side =
            SpscRing::open(&tmp.path().join("service").join("apply.ring")).expect("service open");
        let (_, mut consumer) = service_side.into_split();

        link.apply_producer
            .try_write(MSG_TYPE_APPLY, encode_flags_apply(0), encode_extra_apply(99), b"hello")
            .expect("write");

        let mut buf = Vec::new();
        let rec = consumer
            .try_read(&mut buf)
            .expect("read")
            .expect("non-empty");
        assert_eq!(rec.msg_type, MSG_TYPE_APPLY);
        assert_eq!(decode_extra_apply(rec.header_extra), 99);
        assert_eq!(&buf[..], b"hello");
    }

    /// Mirror of the above for the response direction: service side writes
    /// onto `apply_resp.ring`; the node-side `apply_resp_consumer` in
    /// `ServiceLink` reads it.
    #[test]
    fn apply_resp_consumer_round_trips_from_service_side_producer() {
        let tmp = fresh_instance_dir();
        let mut link = ServiceLink::create(tmp.path()).expect("create");

        let service_side = SpscRing::open(&tmp.path().join("service").join("apply_resp.ring"))
            .expect("service open");
        let (mut producer, _) = service_side.into_split();

        producer
            .try_write(MSG_TYPE_APPLY_RESP, encode_flags_apply(0), encode_extra_apply(42), b"ok")
            .expect("write");

        let mut buf = Vec::new();
        let rec = link
            .apply_resp_consumer
            .try_read(&mut buf)
            .expect("read")
            .expect("non-empty");
        assert_eq!(rec.msg_type, MSG_TYPE_APPLY_RESP);
        assert_eq!(decode_extra_apply(rec.header_extra), 42);
        assert_eq!(&buf[..], b"ok");
    }
}
