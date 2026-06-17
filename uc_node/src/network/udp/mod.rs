//! Aeron-style reliable-unicast UDP inter-node transport.
//!
//! One shared `tokio::net::UdpSocket` per process, demultiplexed to per-peer
//! sessions by `session_id`. Each session is a reliable, ordered, fragmenting
//! message channel: flat u64 per-segment seq, range-NAK retransmit from an
//! in-memory window, receiver-window flow control, MTU fragmentation. The
//! existing `frame.rs` Frame/MessageType is the RPC payload above this layer.

pub mod server_stub;
pub use server_stub::UdpServerHandle;
pub mod fragment;
pub mod reassembly;
pub mod wire;

#[derive(Debug, Clone)]
pub struct UdpTuning {
    /// Max UDP datagram size (header + payload). Aeron default 1408.
    pub mtu: usize,
    /// Receiver flow-control window in bytes. Aeron LAN default 128 KiB.
    pub flow_window_bytes: u64,
    /// Idle sender heartbeat cadence (ms).
    pub heartbeat_ms: u64,
    /// Periodic Status Message cadence (ms).
    pub sm_interval_ms: u64,
    /// Suppress duplicate retransmits for the same range for this long (ms).
    pub nak_linger_ms: u64,
    /// Tear down a session after this long with no inbound traffic (ms).
    pub session_timeout_ms: u64,
}

impl Default for UdpTuning {
    fn default() -> Self {
        Self {
            mtu: 1408,
            flow_window_bytes: 128 * 1024,
            heartbeat_ms: 100,
            sm_interval_ms: 200,
            nak_linger_ms: 10,
            session_timeout_ms: 5000,
        }
    }
}
