//! UDP transport: implements `ClusterTransport`. Unlike QUIC (separate client
//! and server endpoints), the UDP transport uses ONE shared `UdpMux` (one
//! socket per process) for both outbound RPCs and inbound dispatch. The mux is
//! bound on whichever of `build_factory` / `spawn_server` is called first and
//! handed out to both.

use std::sync::Arc;

use openraft::Raft;
use openraft::storage::RaftStateMachine;
use parking_lot::Mutex;

use super::UdpTuning;
use super::factory::UdpRaftNetworkFactory;
use super::mux::UdpMux;
use super::server::{UdpServerHandle, spawn_udp_server};
use crate::network::NetworkError;
use crate::network::transport::{ClusterTransport, TransportCtx};
use crate::raft::TypeConfig;

/// Selects the reliable-UDP transport. Holds the single shared `UdpMux`.
pub struct UdpTransport {
    tuning: UdpTuning,
    shared: Mutex<Option<Arc<UdpMux>>>,
}

impl UdpTransport {
    pub fn new(tuning: UdpTuning) -> Self {
        Self {
            tuning,
            shared: Mutex::new(None),
        }
    }

    /// Return the shared mux, binding it on first use. The parking_lot mutex is
    /// never held across the `.await` on `bind`: take-clone / unlock, bind if
    /// absent, then re-lock and keep whoever stored first (benign-race-safe,
    /// though the single-threaded builder path can't actually race).
    async fn mux_or_bind(&self, ctx: &TransportCtx) -> Result<Arc<UdpMux>, NetworkError> {
        if let Some(m) = self.shared.lock().clone() {
            return Ok(m);
        }
        let mux = UdpMux::bind(ctx.listen_addr, self.tuning.clone()).await?;
        let mut guard = self.shared.lock();
        match guard.as_ref() {
            Some(existing) => Ok(existing.clone()),
            None => {
                *guard = Some(mux.clone());
                Ok(mux)
            }
        }
    }
}

impl ClusterTransport for UdpTransport {
    type Factory = UdpRaftNetworkFactory;
    type Server = UdpServerHandle;

    async fn build_factory(&self, ctx: &TransportCtx) -> Result<Self::Factory, NetworkError> {
        let mux = self.mux_or_bind(ctx).await?;
        #[allow(unused_mut)]
        let mut f = UdpRaftNetworkFactory::new(mux, ctx.app_id.clone());
        #[cfg(feature = "fault-injection")]
        f.set_fault_injection(ctx.node_id, ctx.fault_table.clone());
        Ok(f)
    }

    async fn spawn_server<SM>(
        &self,
        ctx: &TransportCtx,
        raft: Raft<TypeConfig, SM>,
    ) -> Result<Self::Server, NetworkError>
    where
        SM: RaftStateMachine<TypeConfig>,
    {
        let mux = self.mux_or_bind(ctx).await?;
        spawn_udp_server(mux, raft)
    }
}
