//! The encoding adapter: the one part of the harness that knows the service
//! under test's wire format. The workload speaks [`KvOp`], a versionless
//! deletable register per key; the adapter turns that into the service's
//! command/query bytes and its replies back into [`KvResp`].
//!
//! ## Why the model is versionless while the KV compares versions
//!
//! The KV's CAS takes an `expected_version` (a log position), not an
//! expected value. The harness still checks it against a value-CAS model,
//! soundly, because every value the harness writes to a key is UNIQUE
//! (`worker << 40 | counter`): a version names exactly one write, and so
//! does a value, so "the key's current version is V" and "the key's current
//! value is the value V's write carried" are the same predicate. A worker
//! learns the (version, value) pairs from its own acknowledged writes and
//! reads, and issues `CAS(expected_version = V, new)` while the model sees
//! `Cas { old: value_of(V), new }`. Nothing about this weakens the check: a
//! CAS that succeeds against a stale version is a CAS that succeeds against
//! the wrong value.

use std::collections::BTreeMap;

use uc_lincheck::model::Model;

/// One operation on one key, as the model sees it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum KvOp {
    Put(u64),
    Get,
    Delete,
    /// `old = None` means "the key must be absent" (the KV's version 0).
    Cas {
        old: Option<u64>,
        new: u64,
    },
}

/// The model's response to a [`KvOp`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum KvResp {
    Ack,
    Value(Option<u64>),
    /// `true` iff the key existed.
    Deleted(bool),
    CasOk(bool),
}

/// Sequential spec of one key: a deletable register with value-CAS.
pub struct KvModel;

impl Model for KvModel {
    type State = Option<u64>;
    type Op = KvOp;
    type Resp = KvResp;
    fn init() -> Option<u64> {
        None
    }
    fn is_read(op: &KvOp) -> bool {
        matches!(op, KvOp::Get)
    }
    fn step(state: &Option<u64>, op: &KvOp) -> (Option<u64>, KvResp) {
        match op {
            KvOp::Put(v) => (Some(*v), KvResp::Ack),
            KvOp::Get => (*state, KvResp::Value(*state)),
            KvOp::Delete => (None, KvResp::Deleted(state.is_some())),
            KvOp::Cas { old, new } => {
                if *state == *old {
                    (Some(*new), KvResp::CasOk(true))
                } else {
                    (*state, KvResp::CasOk(false))
                }
            }
        }
    }
}

/// A decoded reply: the model's view plus the version the reply names, if
/// any (a write's new version, a read's current version). The version is
/// the acked-write oracle's ordering key and the worker's CAS material.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decoded {
    pub resp: KvResp,
    pub version: Option<u64>,
}

/// What the adapter's service can do. The generators consult this so a
/// register arm (one key, no delete) and a KV arm (many keys, delete,
/// CAS-absent, digest, snapshots) run through the same code.
#[derive(Clone, Copy, Debug)]
pub struct Caps {
    /// Per-key: `false` means one register, keys ignored.
    pub keys: bool,
    pub delete: bool,
    /// `Cas { old: None }` (must-be-absent) is expressible.
    pub cas_absent: bool,
    /// A whole-state digest query exists (the divergence check).
    pub digest: bool,
    /// The service attaches with snapshots (churn rows are runnable).
    pub snapshots: bool,
    /// Append + list read exist (the Elle row is runnable).
    pub append: bool,
}

/// A replica's whole-state summary, for the divergence check.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Digest {
    pub count: u64,
    pub digest: u64,
    pub last_applied: u64,
}

/// A parsed snapshot image: key → (version, value).
pub type Image = BTreeMap<Vec<u8>, (u64, Vec<u8>)>;

pub trait Adapter: Send + Sync {
    /// The adapter's own name, for reports.
    fn name(&self) -> &'static str;
    /// The FSM's `const NAME` — what `[services] names` must declare.
    fn fsm_name(&self) -> &'static str;
    /// Whether the service runs `Sessioned` — what `[session] envelope`
    /// must be.
    fn sessioned(&self) -> bool;
    fn caps(&self) -> Caps;
    /// Extra arguments for spawning the service binary, after the harness's
    /// own `--instance-dir <dir> --app-id <id>`.
    fn service_args(&self) -> Vec<String>;
    /// Encode one op. `Get` encodes a QUERY payload, everything else a
    /// SUBMIT payload. `cas_version` is the wire's expected version for a
    /// `Cas` (0 = must be absent); ignored for the other ops.
    fn encode(&self, key: &[u8], op: &KvOp, cas_version: u64) -> Vec<u8>;
    /// Decode the reply bytes of `op`.
    fn decode(&self, op: &KvOp, bytes: &[u8]) -> Result<Decoded, String>;
    /// A Put of arbitrary value bytes (the churn filler's log volume);
    /// `None` if the service's values are not opaque bytes.
    fn encode_put_bytes(&self, key: &[u8], value: &[u8]) -> Option<Vec<u8>>;
    fn encode_digest(&self) -> Option<Vec<u8>>;
    fn decode_digest(&self, bytes: &[u8]) -> Result<Digest, String>;
    fn encode_append(&self, key: &[u8], val: u64) -> Option<Vec<u8>>;
    fn encode_list_read(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn decode_list(&self, bytes: &[u8]) -> Result<Vec<u64>, String>;
    /// Parse the service's snapshot artifact PAYLOAD (after UC's 24-byte
    /// `ULTSNAP2 ‖ P ‖ version ‖ reserved` envelope).
    fn parse_snapshot_image(&self, image: &[u8]) -> Result<Image, String>;
}

/// The adapters this build knows, by `--adapter` name.
pub fn by_name(name: &str) -> Option<Box<dyn Adapter>> {
    match name {
        "kv-v1" => Some(Box::new(crate::kv_v1::KvV1)),
        "kv-v2" => Some(Box::new(crate::kv_v2::KvV2)),
        "register" => Some(Box::new(crate::register::Register)),
        _ => None,
    }
}

pub const ADAPTER_NAMES: &[&str] = &["kv-v1", "kv-v2", "register"];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_semantics() {
        let s = KvModel::init();
        let (s, r) = KvModel::step(&s, &KvOp::Cas { old: None, new: 7 });
        assert_eq!((s, r), (Some(7), KvResp::CasOk(true)));
        let (s, r) = KvModel::step(&s, &KvOp::Cas { old: None, new: 8 });
        assert_eq!((s, r), (Some(7), KvResp::CasOk(false)));
        let (s, r) = KvModel::step(&s, &KvOp::Delete);
        assert_eq!((s, r), (None, KvResp::Deleted(true)));
        let (s, r) = KvModel::step(&s, &KvOp::Delete);
        assert_eq!((s, r), (None, KvResp::Deleted(false)));
        let (_, r) = KvModel::step(&s, &KvOp::Get);
        assert_eq!(r, KvResp::Value(None));
    }
}
