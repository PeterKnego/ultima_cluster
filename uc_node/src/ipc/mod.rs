//! Inter-process communication layer between `uc_node` and `uc_service`.
//!
//! `uc_node` owns the instance directory: it takes the exclusive
//! `instance.lock` flock (one node per instance), creates `cnc.dat`, and
//! creates the four service-side SPSC ring files under `service/`.
//! `uc_service` and clients attach as shared-lock holders for liveness
//! probing.
//!
//! ```text
//! <instance_dir>/instance.lock              # node holds EX flock
//! <instance_dir>/cnc.dat                    # node writes once at start
//! <instance_dir>/service/apply.ring         # node→service (we produce)
//! <instance_dir>/service/apply_resp.ring    # service→node (we consume)
//! <instance_dir>/service/query.ring         # node→service (we produce)
//! <instance_dir>/service/query_resp.ring    # service→node (we consume)
//! ```
//!
//! The cnc-resident control rings (`control_to_service` / `control_to_node`)
//! live inside `cnc.dat` itself; node-side construction of MPSC producers
//! over those sub-regions arrives with the sub-mmap attach API.

pub mod client_dispatcher;
pub mod client_link;
pub mod handshake;
pub mod instance;
pub mod liveness;
pub mod metrics_publisher;
pub mod query_link;
pub(crate) mod ring_bridge;
pub mod service_link;
pub mod service_watcher;
pub mod session_gc;

pub use handshake::HandshakeError;
pub use instance::{Instance, IpcError};
