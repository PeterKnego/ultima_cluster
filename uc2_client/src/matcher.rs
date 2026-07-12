// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The client's matcher thread: polls both broadcast consumers
//! (`egress_service.broadcast`, `egress_node.broadcast`), decodes the
//! `(client_id, local_seq)` pair pinned into every client-facing frame's
//! `header_extra` (Task 8's egress layout, `uc_protocol::v2::ipc`), and routes
//! each record addressed to THIS client to the `mpsc::SyncSender` registered
//! for that `local_seq` — dropping frames addressed to other clients (every
//! client sees every broadcast record; there is no ring-level per-client
//! targeting) and frames for a `local_seq` nobody is waiting on any more (a
//! late answer after the caller already timed out).
//!
//! A `RingError::Overwritten` on EITHER consumer means the client's broadcast
//! reader fell behind the producer and unread records (of unknown content —
//! could have been this client's answer) were overwritten: every in-flight
//! registration fails with [`RawResp::Overwritten`] (v1 semantics — lapped
//! records are unrecoverable, not just this one request's).

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use uc2_log::agent::{AgentRunner, IdleStrategy};
use uc_protocol::ring::{BroadcastConsumer, RingError};
use uc_protocol::v2::ipc::{MSG_V2_NOT_LEADER, MSG_V2_RESPONSE, MSG_V2_RETRY, client_from_extra};

/// Idle sleep between empty poll cycles (brief mechanics: "50 µs sleep idle").
const MATCHER_IDLE: Duration = Duration::from_micros(50);

/// The per-`local_seq` registration table, shared between `Client` (insert on
/// submit/query, remove on write-failure/timeout) and the matcher thread
/// (remove-and-answer on a matching record, or drain-all on shutdown/overwrite).
pub(crate) type Registrations = Arc<Mutex<HashMap<u32, mpsc::SyncSender<RawResp>>>>;

/// What the matcher hands back to a blocked `submit`/`query_*` call. Carries
/// just enough to build the typed `Result<R, ClientError>` the public API
/// returns (see [`decode_response`]).
pub(crate) enum RawResp {
    /// A `MSG_V2_RESPONSE` payload, verbatim: `position: u64 LE ++
    /// bincode(response)` (the pinned Task 8 egress layout).
    Response(Vec<u8>),
    /// A `MSG_V2_NOT_LEADER` answer; the decoded `leader_hint` (`None` =
    /// unknown, wire value `u64::MAX`).
    NotLeader(Option<u32>),
    /// A `MSG_V2_RETRY` answer: transient, no side effect possible yet.
    Retry,
    /// This client's broadcast consumer was lapped; the real answer (if any)
    /// is unrecoverable.
    Overwritten,
    /// The client shut down while this request was still registered.
    ShutDown,
}

/// Decode a [`RawResp`] into the typed result `submit`/`query_*` return.
/// Shared by both call sites — the wire shape of a submit response and a
/// query answer is identical (`MSG_V2_RESPONSE`, `position ++ bincode(resp)`);
/// only the ring/msg_type used to SEND differs.
pub(crate) fn decode_response<R: serde::de::DeserializeOwned>(
    raw: RawResp,
) -> Result<R, crate::error::ClientError> {
    use crate::error::ClientError;
    match raw {
        RawResp::Response(payload) => {
            if payload.len() < 8 {
                return Err(ClientError::Decode(format!(
                    "response payload too short: {} bytes (need >= 8 for the position prefix)",
                    payload.len()
                )));
            }
            let (resp, _): (R, usize) =
                bincode::serde::decode_from_slice(&payload[8..], bincode::config::standard())
                    .map_err(|e| ClientError::Decode(e.to_string()))?;
            Ok(resp)
        }
        RawResp::NotLeader(hint) => Err(ClientError::NotLeader { hint }),
        RawResp::Retry => Err(ClientError::Retry),
        RawResp::Overwritten => Err(ClientError::ResponseOverwritten),
        RawResp::ShutDown => Err(ClientError::ShutDown),
    }
}

/// Spawn the matcher agent thread. Takes ownership of both broadcast
/// consumers (subscribed by the caller BEFORE spawning, so no record
/// published from that point on is missed) and a handle to the shared
/// registration table.
pub(crate) fn spawn_matcher(
    client_id: u32,
    mut egress_service: BroadcastConsumer,
    mut egress_node: BroadcastConsumer,
    registrations: Registrations,
) -> io::Result<AgentRunner> {
    AgentRunner::spawn("uc2-client-matcher", IdleStrategy::Sleep(MATCHER_IDLE), move || {
        let mut did = false;
        did |= poll_ring(&mut egress_service, client_id, &registrations);
        did |= poll_ring(&mut egress_node, client_id, &registrations);
        did
    })
}

/// One duty-cycle poll of a single broadcast consumer. Returns whether it did
/// any work (a record was read, or an overwrite was handled) — the caller
/// bounds this per ring per cycle to one `try_read`, matching `AgentRunner`'s
/// "bounded work per call" contract.
pub(crate) fn poll_ring(
    ring: &mut BroadcastConsumer,
    client_id: u32,
    registrations: &Registrations,
) -> bool {
    let mut buf = Vec::new();
    match ring.try_read(&mut buf) {
        Ok(Some(rec)) => {
            let (cid, local_seq) = client_from_extra(rec.header_extra);
            if cid != client_id {
                return true; // addressed to another client; every client sees every record
            }
            let raw = match rec.msg_type {
                MSG_V2_RESPONSE => RawResp::Response(buf),
                MSG_V2_NOT_LEADER => {
                    let hint = u64::from_le_bytes(
                        buf.get(..8)
                            .and_then(|s| s.try_into().ok())
                            .unwrap_or([0xff; 8]), // malformed: treat as unknown, never panic
                    );
                    RawResp::NotLeader(if hint == u64::MAX { None } else { Some(hint as u32) })
                }
                MSG_V2_RETRY => RawResp::Retry,
                _ => return true, // not a client-facing msg_type; ignore
            };
            if let Some(tx) = registrations.lock().unwrap().remove(&local_seq) {
                let _ = tx.send(raw); // best-effort: the caller may have already timed out
            }
            true
        }
        Ok(None) => false,
        Err(RingError::Overwritten) => {
            fail_all(registrations, || RawResp::Overwritten);
            true
        }
        // A corrupt/bad-crc record on the shared broadcast is not this
        // client's fault to recover from and not actionable here; drop it
        // and keep the agent alive (mirrors the node's own defensive
        // posture in `drain_ingress_ring`).
        Err(_) => true,
    }
}

/// Drain every in-flight registration, sending a freshly-constructed value
/// (via `mk`) to each. Used both for the `Overwritten` fail-all and (via
/// [`drain_with_shutdown`]) for `Client::shutdown`. Takes a constructor rather
/// than a value because `RawResp` isn't `Clone` (it can carry a response
/// payload) — the two callers here only ever construct data-less variants.
fn fail_all(registrations: &Registrations, mk: impl Fn() -> RawResp) {
    let mut map = registrations.lock().unwrap();
    let entries: Vec<_> = map.drain().collect();
    drop(map);
    for (_, tx) in entries {
        let _ = tx.send(mk());
    }
}

/// `Client::shutdown`'s second step: after the matcher thread is stopped and
/// joined (so nothing else touches the table concurrently), fail every
/// still-registered request with [`RawResp::ShutDown`].
pub(crate) fn drain_with_shutdown(registrations: &Registrations) {
    fail_all(registrations, || RawResp::ShutDown);
}

#[cfg(test)]
mod tests {
    use super::*;
    use uc_protocol::ring::BroadcastRing;
    use uc_protocol::v2::ipc::extra_client;

    fn reg() -> Registrations {
        Arc::new(Mutex::new(HashMap::new()))
    }

    #[test]
    fn response_addressed_to_this_client_is_routed_and_removed() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let ring = BroadcastRing::create(tmp.path(), 4096, 256).unwrap();
        let mut producer = ring.producer();
        let mut consumer = ring.subscribe();

        let registrations = reg();
        let (tx, rx) = mpsc::sync_channel(1);
        registrations.lock().unwrap().insert(7, tx);

        let mut payload = 100u64.to_le_bytes().to_vec(); // position
        payload.extend(bincode::serde::encode_to_vec(42u64, bincode::config::standard()).unwrap());
        producer.write(MSG_V2_RESPONSE, 0, extra_client(1, 7), &payload).unwrap();

        assert!(poll_ring(&mut consumer, 1, &registrations));
        let raw = rx.try_recv().expect("routed");
        let decoded: u64 = decode_response(raw).unwrap();
        assert_eq!(decoded, 42);
        assert!(registrations.lock().unwrap().is_empty(), "registration consumed");
    }

    #[test]
    fn response_for_other_client_is_skipped_not_delivered() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let ring = BroadcastRing::create(tmp.path(), 4096, 256).unwrap();
        let mut producer = ring.producer();
        let mut consumer = ring.subscribe();

        let registrations = reg();
        let (tx, rx) = mpsc::sync_channel::<RawResp>(1);
        registrations.lock().unwrap().insert(0, tx);

        // Same local_seq (0) but a DIFFERENT client_id (99, not 1).
        let mut payload = 8u64.to_le_bytes().to_vec();
        payload.extend(bincode::serde::encode_to_vec(1u64, bincode::config::standard()).unwrap());
        producer.write(MSG_V2_RESPONSE, 0, extra_client(99, 0), &payload).unwrap();

        assert!(poll_ring(&mut consumer, 1, &registrations), "still counts as progress");
        assert!(rx.try_recv().is_err(), "must not be delivered to the wrong client");
        assert!(registrations.lock().unwrap().contains_key(&0), "registration untouched");
    }

    #[test]
    fn not_leader_decodes_the_hint_with_max_as_unknown() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let ring = BroadcastRing::create(tmp.path(), 4096, 256).unwrap();
        let mut producer = ring.producer();
        let mut consumer = ring.subscribe();

        let registrations = reg();
        let (tx, rx) = mpsc::sync_channel(1);
        registrations.lock().unwrap().insert(3, tx);
        producer
            .write(MSG_V2_NOT_LEADER, 0, extra_client(5, 3), &u64::MAX.to_le_bytes())
            .unwrap();
        assert!(poll_ring(&mut consumer, 5, &registrations));
        let err = decode_response::<()>(rx.try_recv().unwrap()).unwrap_err();
        assert!(matches!(err, crate::error::ClientError::NotLeader { hint: None }));

        // A concrete hint (leader 2).
        let (tx2, rx2) = mpsc::sync_channel(1);
        registrations.lock().unwrap().insert(4, tx2);
        producer.write(MSG_V2_NOT_LEADER, 0, extra_client(5, 4), &2u64.to_le_bytes()).unwrap();
        assert!(poll_ring(&mut consumer, 5, &registrations));
        let err = decode_response::<()>(rx2.try_recv().unwrap()).unwrap_err();
        assert!(matches!(err, crate::error::ClientError::NotLeader { hint: Some(2) }));
    }

    #[test]
    fn retry_decodes_to_the_retry_error() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let ring = BroadcastRing::create(tmp.path(), 4096, 256).unwrap();
        let mut producer = ring.producer();
        let mut consumer = ring.subscribe();

        let registrations = reg();
        let (tx, rx) = mpsc::sync_channel(1);
        registrations.lock().unwrap().insert(9, tx);
        producer.write(MSG_V2_RETRY, 0, extra_client(1, 9), &[]).unwrap();
        assert!(poll_ring(&mut consumer, 1, &registrations));
        let err = decode_response::<()>(rx.try_recv().unwrap()).unwrap_err();
        assert!(matches!(err, crate::error::ClientError::Retry));
    }

    #[test]
    fn overwritten_fails_every_in_flight_registration() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        // Tiny ring: a handful of records laps it many times over.
        let ring = BroadcastRing::create(tmp.path(), 256, 128).unwrap();
        let mut producer = ring.producer();
        let mut consumer = ring.subscribe();

        let registrations = reg();
        let (tx_a, rx_a) = mpsc::sync_channel(1);
        let (tx_b, rx_b) = mpsc::sync_channel(1);
        registrations.lock().unwrap().insert(1, tx_a);
        registrations.lock().unwrap().insert(2, tx_b);

        for _ in 0..20 {
            producer.write(MSG_V2_RESPONSE, 0, [0; 8], &[0u8; 32]).unwrap();
        }

        assert!(poll_ring(&mut consumer, 1, &registrations));
        assert!(matches!(rx_a.try_recv(), Ok(RawResp::Overwritten)));
        assert!(matches!(rx_b.try_recv(), Ok(RawResp::Overwritten)));
        assert!(registrations.lock().unwrap().is_empty());
    }

    #[test]
    fn drain_with_shutdown_sends_shutdown_to_every_pending() {
        let registrations = reg();
        let (tx1, rx1) = mpsc::sync_channel(1);
        let (tx2, rx2) = mpsc::sync_channel(1);
        registrations.lock().unwrap().insert(1, tx1);
        registrations.lock().unwrap().insert(2, tx2);

        drain_with_shutdown(&registrations);

        assert!(matches!(rx1.try_recv(), Ok(RawResp::ShutDown)));
        assert!(matches!(rx2.try_recv(), Ok(RawResp::ShutDown)));
        assert!(registrations.lock().unwrap().is_empty());
    }

    #[test]
    fn decode_response_rejects_undersized_payload() {
        let err = decode_response::<u64>(RawResp::Response(vec![1, 2, 3])).unwrap_err();
        assert!(matches!(err, crate::error::ClientError::Decode(_)));
    }

    #[test]
    fn decode_response_surfaces_bincode_decode_failures() {
        // Valid 8-byte position prefix; the remaining single byte 0xFF is a
        // bincode varint length-extension marker with no bytes following it,
        // so decoding a String (length-prefixed) reliably fails with
        // UnexpectedEnd — unlike a fixed-width integer, which has no invalid
        // bit pattern and would spuriously "succeed" on any garbage.
        let mut payload = 0u64.to_le_bytes().to_vec();
        payload.push(0xFF);
        let err = decode_response::<String>(RawResp::Response(payload)).unwrap_err();
        assert!(matches!(err, crate::error::ClientError::Decode(_)));
    }
}
