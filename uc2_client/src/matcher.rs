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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use uc2_log::agent::{AgentRunner, IdleStrategy};
use uc_protocol::ring::{BroadcastConsumer, RingError};
use uc_protocol::v2::ipc::{
    FLAG_V2_IS_QUERY, MSG_V2_NOT_LEADER, MSG_V2_RESPONSE, MSG_V2_RETRY, client_from_extra,
};

/// Idle sleep between empty poll cycles (brief mechanics: "50 µs sleep idle").
const MATCHER_IDLE: Duration = Duration::from_micros(50);

/// Which response kind a pending request expects — the discriminator the
/// matcher checks against a delivered `MSG_V2_RESPONSE`'s `FLAG_V2_IS_QUERY`
/// bit (T14 defense-in-depth). A submit registration must only be satisfied by
/// a submit response, a query registration only by a query answer.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum RegKind {
    /// Registered by `submit` — expects a `MSG_V2_RESPONSE` with the query bit
    /// CLEAR.
    Submit,
    /// Registered by `query_*` — expects a `MSG_V2_RESPONSE` with
    /// `FLAG_V2_IS_QUERY` SET.
    Query,
}

/// One in-flight request's slot in the registration table: the kind it expects
/// (for the matcher's kind check) plus the channel to hand the answer back on.
pub(crate) struct Pending {
    pub(crate) kind: RegKind,
    pub(crate) tx: mpsc::SyncSender<RawResp>,
}

/// The per-`local_seq` registration table, shared between `Client` (insert on
/// submit/query, remove on write-failure/timeout) and the matcher thread
/// (remove-and-answer on a matching record, or drain-all on shutdown/overwrite).
pub(crate) type Registrations = Arc<Mutex<HashMap<u32, Pending>>>;

/// Count of `MSG_V2_RESPONSE` records dropped by the matcher because the
/// delivered kind (submit vs query, per `FLAG_V2_IS_QUERY`) did not match the
/// waiting registration's expected kind — a stale cross-generation catch-up
/// response that collided on `(client_id, local_seq)`. Exposed via
/// [`crate::Client::kind_mismatch_drops`] (a stat, never silent).
pub(crate) type KindMismatchDrops = Arc<AtomicU64>;

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
    kind_mismatch_drops: KindMismatchDrops,
) -> io::Result<AgentRunner> {
    AgentRunner::spawn("uc2-client-matcher", IdleStrategy::Sleep(MATCHER_IDLE), move || {
        let mut did = false;
        did |= poll_ring(&mut egress_service, client_id, &registrations, &kind_mismatch_drops);
        did |= poll_ring(&mut egress_node, client_id, &registrations, &kind_mismatch_drops);
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
    kind_mismatch_drops: &AtomicU64,
) -> bool {
    let mut buf = Vec::new();
    match ring.try_read(&mut buf) {
        Ok(Some(rec)) => {
            let (cid, local_seq) = client_from_extra(rec.header_extra);
            if cid != client_id {
                return true; // addressed to another client; every client sees every record
            }
            match rec.msg_type {
                MSG_V2_RESPONSE => {
                    // Kind check (T14 MAJOR, defense in depth). A submit response
                    // (query bit clear) may only satisfy a Submit registration; a
                    // query answer (FLAG_V2_IS_QUERY set) only a Query one. A
                    // kind-MISMATCHED delivery is a stale cross-generation
                    // catch-up response that collided on (client_id, local_seq):
                    // DROP it and count it, leaving the registration in place to
                    // receive its correct answer. This kills the observed
                    // submit→query type-confusion class (bincode: WriteAck=None,
                    // CasResult decodes as Some(_)) even under a residual id
                    // collision. NOT_LEADER/RETRY are kind-agnostic (pre-side-
                    // effect signals) and route below regardless.
                    let delivered = if rec.flags & FLAG_V2_IS_QUERY != 0 {
                        RegKind::Query
                    } else {
                        RegKind::Submit
                    };
                    let mut map = registrations.lock().unwrap();
                    match map.get(&local_seq) {
                        Some(p) if p.kind == delivered => {
                            let tx = map.remove(&local_seq).unwrap().tx;
                            drop(map);
                            let _ = tx.send(RawResp::Response(buf));
                        }
                        Some(_) => {
                            // Kind mismatch: leave the registration, drop+count.
                            drop(map);
                            kind_mismatch_drops.fetch_add(1, Ordering::Relaxed);
                        }
                        None => {} // nobody waiting on this local_seq (already timed out)
                    }
                }
                MSG_V2_NOT_LEADER => {
                    let hint = u64::from_le_bytes(
                        buf.get(..8)
                            .and_then(|s| s.try_into().ok())
                            .unwrap_or([0xff; 8]), // malformed: treat as unknown, never panic
                    );
                    let raw =
                        RawResp::NotLeader(if hint == u64::MAX { None } else { Some(hint as u32) });
                    // best-effort: the caller may have already timed out
                    if let Some(p) = registrations.lock().unwrap().remove(&local_seq) {
                        let _ = p.tx.send(raw);
                    }
                }
                MSG_V2_RETRY => {
                    if let Some(p) = registrations.lock().unwrap().remove(&local_seq) {
                        let _ = p.tx.send(RawResp::Retry);
                    }
                }
                _ => return true, // not a client-facing msg_type; ignore
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
    for (_, p) in entries {
        let _ = p.tx.send(mk());
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

    fn drops() -> KindMismatchDrops {
        Arc::new(AtomicU64::new(0))
    }

    /// Insert a pending registration of the given kind, returning the rx half.
    fn register(regs: &Registrations, local_seq: u32, kind: RegKind) -> mpsc::Receiver<RawResp> {
        let (tx, rx) = mpsc::sync_channel(1);
        regs.lock().unwrap().insert(local_seq, Pending { kind, tx });
        rx
    }

    #[test]
    fn response_addressed_to_this_client_is_routed_and_removed() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let ring = BroadcastRing::create(tmp.path(), 4096, 256).unwrap();
        let mut producer = ring.producer();
        let mut consumer = ring.subscribe();

        let registrations = reg();
        let rx = register(&registrations, 7, RegKind::Submit);

        let mut payload = 100u64.to_le_bytes().to_vec(); // position
        payload.extend(bincode::serde::encode_to_vec(42u64, bincode::config::standard()).unwrap());
        producer.write(MSG_V2_RESPONSE, 0, extra_client(1, 7), &payload).unwrap();

        assert!(poll_ring(&mut consumer, 1, &registrations, &drops()));
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
        let rx = register(&registrations, 0, RegKind::Submit);

        // Same local_seq (0) but a DIFFERENT client_id (99, not 1).
        let mut payload = 8u64.to_le_bytes().to_vec();
        payload.extend(bincode::serde::encode_to_vec(1u64, bincode::config::standard()).unwrap());
        producer.write(MSG_V2_RESPONSE, 0, extra_client(99, 0), &payload).unwrap();

        assert!(poll_ring(&mut consumer, 1, &registrations, &drops()), "still counts as progress");
        assert!(rx.try_recv().is_err(), "must not be delivered to the wrong client");
        assert!(registrations.lock().unwrap().contains_key(&0), "registration untouched");
    }

    #[test]
    fn query_answer_to_a_submit_registration_is_dropped_counted_and_correct_still_delivered() {
        // T14 MAJOR: a stale cross-generation catch-up that collided on
        // (client_id, local_seq) arrives as a query-flagged RESPONSE while a
        // SUBMIT is registered on that local_seq. It must be DROPPED + counted,
        // the registration left intact, and the client's real submit response
        // (query bit clear) delivered when it arrives.
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let ring = BroadcastRing::create(tmp.path(), 4096, 256).unwrap();
        let mut producer = ring.producer();
        let mut consumer = ring.subscribe();

        let registrations = reg();
        let counter = drops();
        let rx = register(&registrations, 5, RegKind::Submit);

        // Wrong kind: FLAG_V2_IS_QUERY set, addressed to (1, 5). Payload decodes
        // (position ++ bincode) but the kind guard rejects it before any deliver.
        let mut wrong = 0u64.to_le_bytes().to_vec();
        wrong.extend(bincode::serde::encode_to_vec(1u64, bincode::config::standard()).unwrap());
        producer.write(MSG_V2_RESPONSE, FLAG_V2_IS_QUERY, extra_client(1, 5), &wrong).unwrap();
        assert!(poll_ring(&mut consumer, 1, &registrations, &counter));
        assert!(rx.try_recv().is_err(), "kind-mismatched delivery must be dropped");
        assert_eq!(counter.load(Ordering::Relaxed), 1, "the drop is counted, not silent");
        assert!(registrations.lock().unwrap().contains_key(&5), "registration stays for the real answer");

        // The correct submit response (query bit clear) now arrives and routes.
        let mut right = 200u64.to_le_bytes().to_vec();
        right.extend(bincode::serde::encode_to_vec(9u64, bincode::config::standard()).unwrap());
        producer.write(MSG_V2_RESPONSE, 0, extra_client(1, 5), &right).unwrap();
        assert!(poll_ring(&mut consumer, 1, &registrations, &counter));
        let decoded: u64 = decode_response(rx.try_recv().expect("real answer routed")).unwrap();
        assert_eq!(decoded, 9);
        assert_eq!(counter.load(Ordering::Relaxed), 1, "no further drops");
        assert!(registrations.lock().unwrap().is_empty(), "registration consumed by the real answer");
    }

    #[test]
    fn submit_response_to_a_query_registration_is_dropped_and_counted() {
        // The mirror case: a submit-flagged RESPONSE against a QUERY
        // registration (this is the exact captured T14 violation shape — a
        // stale WriteAck/CasResult decoded as a query's Option<u64>).
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let ring = BroadcastRing::create(tmp.path(), 4096, 256).unwrap();
        let mut producer = ring.producer();
        let mut consumer = ring.subscribe();

        let registrations = reg();
        let counter = drops();
        let rx = register(&registrations, 3, RegKind::Query);

        let mut submit_resp = 0u64.to_le_bytes().to_vec();
        submit_resp.extend(bincode::serde::encode_to_vec(1u64, bincode::config::standard()).unwrap());
        producer.write(MSG_V2_RESPONSE, 0, extra_client(1, 3), &submit_resp).unwrap();
        assert!(poll_ring(&mut consumer, 1, &registrations, &counter));
        assert!(rx.try_recv().is_err(), "submit response must not satisfy a query registration");
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        assert!(registrations.lock().unwrap().contains_key(&3));
    }

    #[test]
    fn not_leader_decodes_the_hint_with_max_as_unknown() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let ring = BroadcastRing::create(tmp.path(), 4096, 256).unwrap();
        let mut producer = ring.producer();
        let mut consumer = ring.subscribe();

        let registrations = reg();
        // NOT_LEADER is kind-agnostic (a pre-side-effect signal): route it even
        // to a Query registration.
        let rx = register(&registrations, 3, RegKind::Query);
        producer
            .write(MSG_V2_NOT_LEADER, 0, extra_client(5, 3), &u64::MAX.to_le_bytes())
            .unwrap();
        assert!(poll_ring(&mut consumer, 5, &registrations, &drops()));
        let err = decode_response::<()>(rx.try_recv().unwrap()).unwrap_err();
        assert!(matches!(err, crate::error::ClientError::NotLeader { hint: None }));

        // A concrete hint (leader 2).
        let rx2 = register(&registrations, 4, RegKind::Submit);
        producer.write(MSG_V2_NOT_LEADER, 0, extra_client(5, 4), &2u64.to_le_bytes()).unwrap();
        assert!(poll_ring(&mut consumer, 5, &registrations, &drops()));
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
        let rx = register(&registrations, 9, RegKind::Submit);
        producer.write(MSG_V2_RETRY, 0, extra_client(1, 9), &[]).unwrap();
        assert!(poll_ring(&mut consumer, 1, &registrations, &drops()));
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
        let rx_a = register(&registrations, 1, RegKind::Submit);
        let rx_b = register(&registrations, 2, RegKind::Query);

        for _ in 0..20 {
            producer.write(MSG_V2_RESPONSE, 0, [0; 8], &[0u8; 32]).unwrap();
        }

        assert!(poll_ring(&mut consumer, 1, &registrations, &drops()));
        assert!(matches!(rx_a.try_recv(), Ok(RawResp::Overwritten)));
        assert!(matches!(rx_b.try_recv(), Ok(RawResp::Overwritten)));
        assert!(registrations.lock().unwrap().is_empty());
    }

    #[test]
    fn drain_with_shutdown_sends_shutdown_to_every_pending() {
        let registrations = reg();
        let rx1 = register(&registrations, 1, RegKind::Submit);
        let rx2 = register(&registrations, 2, RegKind::Query);

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
