//! Temporary stub; replaced by udp/server.rs in Phase C (Task 12).
use std::net::SocketAddr;
pub struct UdpServerHandle;
impl UdpServerHandle {
    pub async fn shutdown(self) {}
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        Ok("0.0.0.0:0".parse().unwrap())
    }
}
