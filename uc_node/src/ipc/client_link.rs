//! Node-side ownership of the three client-facing ring files
//! (`submit.ring`, `query.ring`, `response.broadcast`) and the
//! `sessions.dir/` directory under `<instance_dir>/clients/`.
//!
//! Ring sizing (chosen to ride wrap traffic without producer stalls under
//! M4-scale workloads, but small enough that the wrap-fix is exercised
//! routinely in tests):
//!   * submit / query — 16 MiB cap, 4 MiB max single frame (MPSC).
//!   * response — 16 MiB cap, 4 MiB max single frame (Broadcast).
//!
//! Ownership: this module consumes from `submit.ring` and `query.ring`
//! (node side reads what clients write), and produces onto
//! `response.broadcast`. Clients take the matching halves via
//! `uc_client::Client::connect`.

use std::path::Path;

use uc_protocol::ring::broadcast::{BroadcastProducer, BroadcastRing};
use uc_protocol::ring::mpsc::{MpscConsumer, MpscRing};

use super::instance::IpcError;

pub const CLIENT_RING_CAP: u64 = 16 * 1024 * 1024;
pub const CLIENT_RING_MAX_MSG: u32 = 4 * 1024 * 1024;

pub struct ClientLink {
    pub submit_consumer: MpscConsumer,
    pub query_consumer: MpscConsumer,
    pub response_producer: BroadcastProducer,
}

impl ClientLink {
    /// Create the three ring files and `sessions.dir/` using the default
    /// production sizing ([`CLIENT_RING_CAP`] / [`CLIENT_RING_MAX_MSG`]).
    ///
    /// Caller must ensure `<instance_dir>/clients/` already exists
    /// (`Instance::create` handles that).
    pub fn create(instance_dir: &Path) -> Result<Self, IpcError> {
        Self::create_with_cap(instance_dir, CLIENT_RING_CAP, CLIENT_RING_MAX_MSG)
    }

    /// Create the three ring files and `sessions.dir/` with caller-specified
    /// ring sizing. Used by [`NodeBuilder`] when `NodeConfig::client_rings`
    /// overrides the defaults (e.g. small rings for wrap-path tests).
    pub fn create_with_cap(
        instance_dir: &Path,
        cap_bytes: u64,
        max_msg: u32,
    ) -> Result<Self, IpcError> {
        let clients_dir = instance_dir.join("clients");
        std::fs::create_dir_all(clients_dir.join("sessions.dir"))?;

        let submit = MpscRing::create(&clients_dir.join("submit.ring"), cap_bytes, max_msg)?;
        let query = MpscRing::create(&clients_dir.join("query.ring"), cap_bytes, max_msg)?;
        let response =
            BroadcastRing::create(&clients_dir.join("response.broadcast"), cap_bytes, max_msg)?;

        let (_, submit_consumer) = submit.into_split();
        let (_, query_consumer) = query.into_split();
        let response_producer = response.producer();

        Ok(ClientLink {
            submit_consumer,
            query_consumer,
            response_producer,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_instance_dir() -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("clients")).unwrap();
        tmp
    }

    #[test]
    fn create_writes_three_ring_files_and_sessions_dir() {
        let tmp = fresh_instance_dir();
        let _link = ClientLink::create(tmp.path()).expect("create");
        let clients_dir = tmp.path().join("clients");
        for name in ["submit.ring", "query.ring", "response.broadcast"] {
            let p = clients_dir.join(name);
            assert!(p.is_file(), "{} not created", p.display());
            assert!(std::fs::metadata(&p).unwrap().len() > 0);
        }
        assert!(clients_dir.join("sessions.dir").is_dir());
    }

    #[test]
    fn client_side_can_open_each_ring() {
        let tmp = fresh_instance_dir();
        let _link = ClientLink::create(tmp.path()).expect("create");
        let clients_dir = tmp.path().join("clients");
        let _ = MpscRing::open(&clients_dir.join("submit.ring")).expect("open submit");
        let _ = MpscRing::open(&clients_dir.join("query.ring")).expect("open query");
        let _ = BroadcastRing::open(&clients_dir.join("response.broadcast")).expect("open resp");
    }
}
