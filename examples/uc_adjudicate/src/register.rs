//! The register adapter: `testing/uc_crashtest`'s `uc_crashtest-service`
//! running `Sessioned<RegisterSm>` (`--sessioned`), the reference SM every
//! repo capstone already adjudicates. Its purpose here is the B3 paired
//! rate arm and a with-teeth self-check of the harness against a known-good
//! service; it is one register, keys are ignored, no delete, no digest, no
//! snapshots (the crashtest binary's `--snapshots` does not compose with
//! `--sessioned`).
//!
//! Wire: bincode (standard config) of `uc_lincheck::register::{Cmd, CmdResp}`
//! for commands, `()` → `Option<u64>` for the read — exactly what
//! `testing/uc_crashtest/tests/remote_lin.rs` sends.

use uc_lincheck::register::{Cmd, CmdResp};

use super::adapter::{Adapter, Caps, Decoded, Digest, Image, KvOp, KvResp};

pub struct Register;

fn enc<T: serde::Serialize>(t: &T) -> Vec<u8> {
    bincode::serde::encode_to_vec(t, bincode::config::standard()).expect("bincode encode")
}

fn dec<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T, String> {
    bincode::serde::decode_from_slice(b, bincode::config::standard())
        .map(|(t, _)| t)
        .map_err(|e| format!("bincode: {e}"))
}

impl Adapter for Register {
    fn name(&self) -> &'static str {
        "register"
    }
    fn fsm_name(&self) -> &'static str {
        "register"
    }
    fn sessioned(&self) -> bool {
        true
    }
    fn caps(&self) -> Caps {
        Caps {
            keys: false,
            delete: false,
            cas_absent: false,
            digest: false,
            snapshots: false,
            append: false,
        }
    }
    fn service_args(&self) -> Vec<String> {
        vec!["--sessioned".into()]
    }
    fn encode(&self, _key: &[u8], op: &KvOp, _cas_version: u64) -> Vec<u8> {
        match op {
            KvOp::Put(v) => enc(&Cmd::Write(*v)),
            KvOp::Cas {
                old: Some(old),
                new,
            } => enc(&Cmd::Cas {
                old: *old,
                new: *new,
            }),
            KvOp::Get => enc(&()),
            KvOp::Delete | KvOp::Cas { old: None, .. } => {
                unreachable!("the generator honours Caps: no delete / cas-absent on the register")
            }
        }
    }
    fn decode(&self, op: &KvOp, b: &[u8]) -> Result<Decoded, String> {
        match op {
            KvOp::Get => Ok(Decoded {
                resp: KvResp::Value(dec::<Option<u64>>(b)?),
                version: None,
            }),
            _ => match dec::<CmdResp>(b)? {
                CmdResp::WriteAck => Ok(Decoded {
                    resp: KvResp::Ack,
                    version: None,
                }),
                CmdResp::CasResult(ok) => Ok(Decoded {
                    resp: KvResp::CasOk(ok),
                    version: None,
                }),
            },
        }
    }
    fn encode_put_bytes(&self, _key: &[u8], _value: &[u8]) -> Option<Vec<u8>> {
        None
    }
    fn encode_digest(&self) -> Option<Vec<u8>> {
        None
    }
    fn decode_digest(&self, _b: &[u8]) -> Result<Digest, String> {
        Err("the register has no digest".into())
    }
    fn encode_append(&self, _key: &[u8], _val: u64) -> Option<Vec<u8>> {
        None
    }
    fn encode_list_read(&self, _key: &[u8]) -> Option<Vec<u8>> {
        None
    }
    fn decode_list(&self, _b: &[u8]) -> Result<Vec<u64>, String> {
        Err("the register has no list kind".into())
    }
    fn parse_snapshot_image(&self, _b: &[u8]) -> Result<Image, String> {
        Err("the register adapter does not parse artifacts".into())
    }
}
