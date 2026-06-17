use openraft::Raft;
use openraft::storage::RaftStateMachine;

use super::super::NetworkError;
use super::super::transport::{ClusterTransport, TransportCtx};
use super::{QuicRaftNetworkFactory, ServerHandle, spawn_server, tls};
use crate::raft::TypeConfig;

/// Marker selecting the QUIC transport.
pub struct QuicTransport;

impl ClusterTransport for QuicTransport {
    type Factory = QuicRaftNetworkFactory;
    type Server = ServerHandle;

    fn build_factory(&self, ctx: &TransportCtx) -> Result<Self::Factory, NetworkError> {
        let client_tls_cfg = tls::build_client_config()?;
        let endpoint = quinn::Endpoint::client("0.0.0.0:0".parse().unwrap())
            .map_err(|e| NetworkError::Connect(format!("client endpoint: {e}")))?;
        #[allow(unused_mut)]
        let mut f = QuicRaftNetworkFactory::new(endpoint, client_tls_cfg, ctx.app_id.clone());
        #[cfg(feature = "fault-injection")]
        f.set_fault_injection(ctx.node_id, ctx.fault_table.clone());
        Ok(f)
    }

    fn spawn_server<SM>(
        &self,
        ctx: &TransportCtx,
        raft: Raft<TypeConfig, SM>,
    ) -> Result<Self::Server, NetworkError>
    where
        SM: RaftStateMachine<TypeConfig>,
    {
        // Confirmed: tls::load_or_init / build_server_config / build_client_config
        // all already return Result<_, NetworkError>, so no error mapping needed.
        let (cert_der, key_der) = tls::load_or_init(&ctx.data_dir, &ctx.app_id)?;
        let server_tls_cfg = tls::build_server_config(cert_der, key_der)?;
        spawn_server(ctx.listen_addr, server_tls_cfg, raft)
    }
}
