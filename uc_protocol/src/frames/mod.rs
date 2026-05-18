//! Per-RPC frame types — typed encoders/decoders for the `header_extra`
//! payload of records that flow over the SPSC service↔node rings.
//!
//! Layout convention: each frame type owns a stable `msg_type` (u16) and
//! defines a layout for `header_extra: [u8; 8]`. The variable-size payload
//! is the bincode-encoded `Command` / `Response` / `Query` / `QueryResponse`
//! supplied by the user (see `uc_service::StateMachine`). Payload encoding
//! is intentionally not part of `uc_protocol` — different SDKs may pick
//! different codecs.

pub mod apply;
pub mod client;
pub mod output;
pub mod query;
pub mod snapshot;
