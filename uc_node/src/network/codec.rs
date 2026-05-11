//! RPC body encode/decode for the message types defined in `frame.rs`.
//!
//! Most bodies are bincode-encoded openraft request/response types. The
//! exception is `AppendEntriesReq` body, which COULD use a custom encoding
//! to enable scatter-gather zero-copy of entry payloads via
//! `quinn::SendStream::write_chunks(&[Bytes])` — that optimization is
//! deferred. M2 uses bincode for all six bodies; correctness first.

use bytes::Bytes;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use super::NetworkError;
use crate::raft::TypeConfig;

/// Bincode encode.
pub fn encode<T: serde::Serialize>(value: &T) -> Result<Bytes, NetworkError> {
    let bytes = bincode::serde::encode_to_vec(value, bincode::config::standard())
        .map_err(|e| NetworkError::Decode(format!("encode: {e}")))?;
    Ok(Bytes::from(bytes))
}

/// Bincode decode.
pub fn decode<T: serde::de::DeserializeOwned>(bytes: &Bytes) -> Result<T, NetworkError> {
    let (val, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|e| NetworkError::Decode(format!("decode: {e}")))?;
    Ok(val)
}

// Convenience wrappers per RPC kind.

pub fn encode_vote_req(req: &VoteRequest<u64>) -> Result<Bytes, NetworkError> {
    encode(req)
}
pub fn decode_vote_req(b: &Bytes) -> Result<VoteRequest<u64>, NetworkError> {
    decode(b)
}

pub fn encode_vote_resp(resp: &VoteResponse<u64>) -> Result<Bytes, NetworkError> {
    encode(resp)
}
pub fn decode_vote_resp(b: &Bytes) -> Result<VoteResponse<u64>, NetworkError> {
    decode(b)
}

pub fn encode_append_entries_req(
    req: &AppendEntriesRequest<TypeConfig>,
) -> Result<Bytes, NetworkError> {
    encode(req)
}
pub fn decode_append_entries_req(
    b: &Bytes,
) -> Result<AppendEntriesRequest<TypeConfig>, NetworkError> {
    decode(b)
}

pub fn encode_append_entries_resp(
    resp: &AppendEntriesResponse<u64>,
) -> Result<Bytes, NetworkError> {
    encode(resp)
}
pub fn decode_append_entries_resp(b: &Bytes) -> Result<AppendEntriesResponse<u64>, NetworkError> {
    decode(b)
}

pub fn encode_install_snapshot_req(
    req: &InstallSnapshotRequest<TypeConfig>,
) -> Result<Bytes, NetworkError> {
    encode(req)
}
pub fn decode_install_snapshot_req(
    b: &Bytes,
) -> Result<InstallSnapshotRequest<TypeConfig>, NetworkError> {
    decode(b)
}

pub fn encode_install_snapshot_resp(
    resp: &InstallSnapshotResponse<u64>,
) -> Result<Bytes, NetworkError> {
    encode(resp)
}
pub fn decode_install_snapshot_resp(
    b: &Bytes,
) -> Result<InstallSnapshotResponse<u64>, NetworkError> {
    decode(b)
}
