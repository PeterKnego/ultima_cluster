# M14b — Per-FSM query routing and client fan-in Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a client pick which FSM answers — `submit_to(id)`, `submit_all()` (fan-in over every declared FSM), `query_*_on(id)` — by routing queries per id on the node and matching responses per ring on the client, and prove in the deterministic sim that the M14a report ceiling stalls commit exactly when a quorum's FSMs are capped.

**Architecture:** The `MSG_V2_QUERY` payload gains a leading `service_id: u8`; the node strips it, answers `MSG_V2_BAD_SERVICE` on `egress_node.broadcast` for an id with no ring, forwards snapshot reads to `svc_query.<id>.ring`, and carries the id through the existing per-slot read barrier (which M14a already keyed on `slot[service_id]`). The client `Engine` opens every declared FSM's `egress_service.<id>.broadcast`, tags each slot with an `expected` ring bitmask and a `received` bitmask, accepts a `MSG_V2_RESPONSE` only from an expected ring, and completes when `received == expected` — buffering the partial responses of a fan-in on the `PollHalf` and handing them to the callback as one ordered `Outcome::Responses`. The blocking tiers add `submit_to`/`submit_all`/`query_*_on`, a `FanInTicket<R>` for the fan-in, and a local `ServiceNotDeclared` refusal. The sim gains one report choke point, a per-node apply ceiling, invariant inv10 (a clamped report never exceeds its unclamped value or its ceiling, and never decreases between reset events) and the "commit stalls iff a quorum is capped" scenario.

**Tech Stack:** Rust 2024 (workspace edition), stable 1.96.0 pinned / MSRV 1.89, `bytes::Bytes` for every fan-in payload (the workspace dep `uc2_service`/`uc2_remote` already use; `uc2_client` gains it), `uc2_sim` driving the real `ElectionSm`, `cargo-fuzz` on nightly for the `ring_mpsc_record` seed extension.

**Spec:** `docs/superpowers/specs/2026-08-21-uc2-multi-service-design.md` — this plan implements §5.4, §6.1–§6.4, and §12's "unit: client slot-table mask completion" + "`uc2_sim`" rows. **Landed before this plan:** M14a (`main` 6111257) — cnc 3.0, `[services]`, per-id rings, the slot band, the lag barrier, the door term and the report ceiling. **Not in this plan:** §7.3 (N artifacts per snapshot session, wire 0.6.0), §9 (labelled metrics/alerts/`uc2ctl status`), the §12 capstones/elle/crashtest (M14c), the fleet gate and release writeup (M14d).

## Deviations from the spec, for the reviewer

1. **Fan-in payloads are `bytes::Bytes` — the spec's type, not `Vec<u8>`.** The 2026-08-22 codec spike measured `Vec<u8>` payloads making the apply thread decode-bound (56–85 %) where `Bytes` on the same wire sat at 15–21 %, and `AppCommand = Bytes` is the SM contract end to end; the client's fan-in follows the same standard. `uc2_client` gains the workspace `bytes` dependency (already used by `uc2_service` and the published `uc2_remote`). The spec's `Vec<Option<Bytes>>` becomes a `PollHalf`-owned per-slot `FanIn { seq, position, parts: Vec<(u8, Bytes)> }`, handed to the callback as `Outcome::Responses(&[(u8, Bytes)])` (ordered by id) and carried to a `FanInTicket` as `Resolved::Many { parts: Vec<(u8, Bytes)> }` — one `Bytes::copy_from_slice` per piece at the ring-read boundary (the reused read buffer forces that one copy), refcounted from there through the ticket to the caller with no further copies.
2. **`submit_all` returns a `FanInTicket<R>`, not `Ticket<Vec<(u8, R)>>`.** `TicketCore` resolves to one `(position, bytes)` blob that `Ticket::wait` bincode-decodes as `R`; a fan-in carries N per-id blobs that each decode as `R`, which is not the same as one blob decoding as `Vec<(u8, R)>`. `TicketCore` gains a `Resolved::{One, Many}` payload and a second ticket type decodes `Many`. Same blocking/`Future` shape, honest type.
3. **The driver still parks on FSM 0's ring handle.** `RingWaitHandle::park` is a single futex; there is no wait-on-N primitive, and `wait_handle()` has five callers (the pipelined driver, the gateway twice, `hop_bench`). A response that lands only on ring k ≠ 0 wakes nobody and resolves at the driver's existing 1 ms park ceiling (`spawn_driver`'s `park(seq, 1 ms)`). Accepted for M14b: ≤ 1 ms added latency on non-default FSMs under `WaitStrategy::Park`; `BusySpin`/`Backoff` unaffected; the gateway only ever uses FSM 0. Documented on `wait_handle`.
4. **`MSG_V2_BAD_SERVICE` is gated on "does this node have a ring for the id", not on the declared bitmask.** A `ServicesConfig::none_for_tests()` node (declared `0`) rings FSM 0 and every unit-test harness in `uc2_node` is one; `is_declared(0)` is false there while `svc_query[0]` exists. For a real node the two predicates are identical (`ring_ids() == declared`). The gate is `svc_query[id].is_some()` — exactly what `forward_svc_query` already checks.
5. **An empty `MSG_V2_QUERY` payload (no id byte) is dropped**, matching the service's own precedent for a `svc_query` record shorter than its prefix (`apply.rs::drain_queries`: "a query has no recovery contract; the client times out/retries"). The SDK always writes the prefix, so only a raw ring writer can reach this.
6. **Client payload cap counts the wire payload** — for a query, `query.len() + 1` is compared against `max_payload`, so the cap describes what is written, not what the caller passed.
7. **inv10 is scoped to the sim's report choke point, not stated against `sm.validated_up_to()`.** The sim's `RawM3` and `Mechanism`-leak modes deliberately report raw durables above any validated frontier, and several red pins assert *those* traces trip a *specific* invariant (`phantom`, `inv5`). inv10 therefore states what the ceiling mechanism guarantees: clamped ≤ unclamped, clamped ≤ ceiling when set, and clamped non-decreasing between reset events (truncate, crash/restart, role change, a lowered ceiling). `inject_report` (a forged-wire model) is exempt.

## Global Constraints

- MSRV **1.89**; `cargo clippy --workspace --all-targets -- -D warnings` clean after **every** task; `x.is_multiple_of(n)` rather than `x % n == 0`.
- **Never write scratch or test artifacts to `/tmp`** (RAM-backed, no swap). Tests use `tempdir_in(env!("CARGO_TARGET_TMPDIR"))`.
- **Private `CARGO_TARGET_DIR`** for the proof-stack run (`~/.cache/cargo-target` is shared across worktrees); the M14a-era warm dir `/home/claude/cargo-target-uc2-m14a` is fine.
- **Record formats (spec §6.3):** `ingress.ring` `MSG_V2_SUBMIT` unchanged; `query.ring` `MSG_V2_QUERY` payload = `service_id: u8 ++ query bytes`; `svc_query.<id>.ring` `MSG_V2_SVC_QUERY` payload unchanged (`expected_epoch: u64 LE ++ query`); `egress_service.<id>.broadcast` unchanged; `egress_node.broadcast` gains `MSG_V2_BAD_SERVICE = 7` (payload `service_id: u8`). The ring framing (`ULTRNG2`), the log frame, the datagram header, `version::CURRENT` (0.5.0) and `CNC_V2_VERSION` (3.0) are untouched.
- **The read barrier's quorum round (`read_round.rs`) is untouched** — one round certifies reads for any FSM (spec §5.4).
- **`uc2_service`, `uc2_consensus`, `uc2_net`, `uc2_crypto` are not modified.** The remote protocol stays v1: `uc2_remote` untouched; `uc2_gateway` only gains the new `Outcome`/`SubmitError` match arms (spec §6.4: remote clients get FSM 0).
- **Harness rule:** a page whose `services_declared` reads `0` is a harness node — every attacher treats it as `{0}` (M14a's rule, kept by the client).
- Public API additions land in `docs/reference/semver-policy.md` (the promised surface tables) in Task 8.
- Commit after every task with a conventional message. One task, one commit (a fix round may add one).

## File Structure

| File | Create/Modify | Responsibility |
|---|---|---|
| `uc_protocol/src/v2/ipc.rs` | Modify | `MSG_V2_BAD_SERVICE = 7`; `split_query_payload`/`write_query_payload` (the `service_id ++ query` codec, core-friendly); routing-table doc; pins. |
| `fuzz/fuzz_targets/ring_mpsc_record.rs`, `fuzz/src/seeds.rs`, `fuzz/corpus/ring_mpsc_record/*`, `fuzz/README.md` | Modify | The decoded record's payload goes through `split_query_payload` when `msg_type == MSG_V2_QUERY`; two query seeds. |
| `uc2_node/src/node.rs` | Modify | `send_bad_service`; `has_service_ring`; `drain_query_ring` parses/strips the id, gates, routes; `forward_svc_query`'s "M14b answers first" comment realised; unit tests. |
| `uc2_node/examples/read_profile.rs` | Modify | Its raw `MSG_V2_QUERY` writes carry the `0u8` prefix. |
| `uc2_client/Cargo.toml` | Modify | `bytes = { workspace = true }`. |
| `uc2_client/src/slots.rs` | Modify | `Slot.{expected, received}`, `claim(.., expected)`, `resolve(.., ring)` with `Partial`/`WrongRing`, `slot_index`, `slot_count`; tests. |
| `uc2_client/src/engine.rs` | Modify | N egress rings, `Shared.declared`, the query prefix scratch, `try_submit_to`/`try_submit_all`/`try_query_on`, per-ring `handle_record` + `FanIn` buffer, `Outcome::{Responses, BadService}`, `SubmitError::ServiceNotDeclared`, stats `wrong_ring`/`bad_service`. |
| `uc2_client/src/ticket.rs` | Modify | `Resolved::{One, Many}`; `FanInTicket<R>`; `fan_in_ticket_pair`. |
| `uc2_client/src/pipelined.rs` | Modify | `submit_to`, `submit_all`, `query_snapshot_on`, `query_linearizable_on`, `declared()`; `dispatch` generalised over the ticket pair; driver maps the two new outcomes. |
| `uc2_client/src/client.rs`, `error.rs`, `lib.rs` | Modify | Shim mirrors; `ClientError::ServiceNotDeclared`; re-export `FanInTicket`. |
| `uc2_client/tests/engine_synthetic.rs`, `uc2_client/tests/pipelined.rs` | Modify | Mask/fan-in/bad-service tests on a synthetic dir; two-FSM tests against a real node. |
| `uc2_gateway/src/edge.rs`, `uc2_gateway/examples/hop_bench/engine_load.rs`, `uc2_gateway/examples/m12_gate.rs`, `uc2_node/examples/m5_gate.rs` | Modify | The exhaustive `Outcome`/`SubmitError` matches gain the new arms. |
| `uc2_node/tests/services.rs` | Modify | End-to-end: `submit_to`/`submit_all`/`query_*_on`, the local refusal, `BAD_SERVICE` on a raw record, the default `submit` ignoring the other FSM's ring. |
| `examples/counter/src/bin/counter-client.rs` | Modify | `--service-id` / `--all` so the feature is demonstrable. |
| `uc2_sim/src/world.rs`, `uc2_sim/src/invariants.rs`, `uc2_sim/tests/scenarios.rs` | Modify | `send_report` choke point; `Node.apply_ceiling` + `World::set_apply_ceiling`; inv10; the capped-quorum scenarios; a third fuzz loop. |
| `docs/reference/{instance-directory,read-path,semver-policy}.md`, `docs/VERIFICATION.md`, `docs/QUICKSTART.md`, `README.md` | Modify | Task 8. |

---

### Task 1: `MSG_V2_BAD_SERVICE` and the `service_id ++ query` codec (+ fuzz)

**Files:**
- Modify `uc_protocol/src/v2/ipc.rs` (module doc lines 9–27; constants 36–51; tests 79–116)
- Modify `fuzz/fuzz_targets/ring_mpsc_record.rs`, `fuzz/src/seeds.rs` (`ring_mpsc_record()` at ~630), `fuzz/README.md` (the `ring_mpsc_record` row, ~line 164); regenerate `fuzz/corpus/ring_mpsc_record/`

**Interfaces:**
```rust
// uc_protocol::v2::ipc
pub const MSG_V2_BAD_SERVICE: u16 = 7;
/// `query.ring` payload → `(service_id, query bytes)`; `None` when empty.
pub fn split_query_payload(payload: &[u8]) -> Option<(u8, &[u8])>;
/// Build a `query.ring` payload into `out` (cleared first): `service_id ++ query`.
pub fn write_query_payload(service_id: u8, query: &[u8], out: &mut Vec<u8>);
```
(`write_query_payload` needs `alloc`'s `Vec`; `ipc.rs` already lives in a crate with `std` — `uc_protocol`'s `core`-only rule covers `version`/`magic`/`error_codes`, and `ipc.rs` already uses `try_into().unwrap()` on slices; `Vec` is fine here as `extra_client` returns an array and the ring code uses `Vec` freely.)

- [ ] **Step 1: Write the failing tests**

In `ipc.rs`'s test module, extend `msg_type_and_flag_codes_are_stable` with `assert_eq!(MSG_V2_BAD_SERVICE, 7);` and add:

```rust
    #[test]
    fn query_payload_codec_round_trips_and_pins_the_prefix() {
        let mut out = Vec::new();
        write_query_payload(3, b"read", &mut out);
        assert_eq!(out, [3, b'r', b'e', b'a', b'd']);
        assert_eq!(split_query_payload(&out), Some((3, &b"read"[..])));
        // Reused buffer is cleared first.
        write_query_payload(0, b"", &mut out);
        assert_eq!(out, [0]);
        assert_eq!(split_query_payload(&out), Some((0, &b""[..])));
        // An empty payload has no id byte.
        assert_eq!(split_query_payload(&[]), None);
        // Any byte is a valid id at this layer (range/declared checks are the node's).
        assert_eq!(split_query_payload(&[255, 1]), Some((255, &[1][..])));
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc_protocol --lib ipc`
Expected: compile error — `MSG_V2_BAD_SERVICE`, `write_query_payload`, `split_query_payload` not found.

- [ ] **Step 3: Implement**

After `MSG_V2_RETRY`:

```rust
/// `egress_node.broadcast` (M14b): the query named a service id this node has
/// no ring for — undeclared, out of range (`>= CNC_MAX_SERVICES`), or a harness
/// node's non-zero id. Payload = `service_id: u8` (the offending id). Kind-
/// agnostic like [`MSG_V2_RETRY`]: no side effect happened, the slot resolves.
pub const MSG_V2_BAD_SERVICE: u16 = 7;
```

After `client_from_extra`:

```rust
/// M14b: the `query.ring` payload is `service_id: u8 ++ query bytes`. Split it;
/// `None` for an empty payload (no id byte — a malformed record the node drops).
#[inline]
pub fn split_query_payload(payload: &[u8]) -> Option<(u8, &[u8])> {
    payload.split_first().map(|(id, rest)| (*id, rest))
}

/// M14b: build a `query.ring` payload into `out` (cleared first).
#[inline]
pub fn write_query_payload(service_id: u8, query: &[u8], out: &mut Vec<u8>) {
    out.clear();
    out.reserve(1 + query.len());
    out.push(service_id);
    out.extend_from_slice(query);
}
```

Module doc: the `query.ring` bullet becomes "payload is `service_id: u8` (M14b — which FSM answers) followed by the query bytes; `flags` bit 0 selects linearizable vs. snapshot routing"; the `egress_node.broadcast` bullet lists `[`MSG_V2_NOT_LEADER`]` (payload `leader_hint`), and `[`MSG_V2_BAD_SERVICE`]` (payload `service_id: u8`, M14b). Update `MSG_V2_QUERY`'s own doc line to `payload = service_id: u8 ++ query bytes`.

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p uc_protocol --lib ipc` — expected PASS (4 tests in the module).

- [ ] **Step 5: The fuzz target**

`fuzz/fuzz_targets/ring_mpsc_record.rs`: import `uc_protocol::v2::ipc::{MSG_V2_QUERY, split_query_payload}` and, in the `SlotState::Committed` arm, after `decode_record_slice(&body[..take], &mut buf)`, add:

```rust
            // 3. M14b: a decoded query record's payload goes through the
            // node's id split — total on any bytes (`None` for empty).
            if let Ok((hdr, _)) = decode_record_slice(&body[..take], &mut buf)
                && hdr.msg_type == MSG_V2_QUERY
            {
                let _ = split_query_payload(&buf);
            }
```

(`decode_record_slice(slice, &mut buf) -> Result<(RecordHeader, usize), RingError>` fills `buf` with the decoded payload — the M13a plan's pinned shape; bind the header from the tuple and call `split_query_payload(&buf)` when `hdr.msg_type == MSG_V2_QUERY`.)

`fuzz/src/seeds.rs::ring_mpsc_record()`: add two seeds after `01-committed-record`:

```rust
    // M14b: a committed QUERY record with the service-id prefix, and one with
    // an EMPTY payload (no id byte) — the node's split must be total on both.
    let q = record(2, 0, [9; 8], &[1u8, b'q', b'r', b'y']);
    seeds.push(Seed::fixed("10-query-with-id", input(encode_commit_word(3, q.len() as u32, false), 3, &q)));
    let q0 = record(2, 0, [9; 8], &[]);
    seeds.push(Seed::fixed("11-query-empty", input(encode_commit_word(3, q0.len() as u32, false), 3, &q0)));
```

`fuzz/README.md`: the `ring_mpsc_record` row's text gains "…and, for a query record, the M14b `service_id ++ query` split (`split_query_payload`)".

```bash
(cd fuzz && cargo +nightly run --bin seed-corpus)
git status --short fuzz/corpus/ring_mpsc_record/     # exactly two new files, 10-query-with-id and 11-query-empty
scripts/fuzz_smoke.sh 30 ring_mpsc_record
git clean -fdq fuzz/corpus                             # the smoke's hash-named working corpus is NOT repo content (fuzz/README.md:132-136)
git checkout -- fuzz/Cargo.lock 2>/dev/null || true
```

Expected: the smoke prints its clean line for `ring_mpsc_record`; after the clean, `git status --short` shows only the two generator seeds under `fuzz/corpus/ring_mpsc_record/` plus the three source edits.

- [ ] **Step 6: Clippy + commit**

```bash
cargo clippy -p uc_protocol --all-targets -- -D warnings
git add uc_protocol/src/v2/ipc.rs fuzz/fuzz_targets/ring_mpsc_record.rs fuzz/src/seeds.rs fuzz/README.md fuzz/corpus/ring_mpsc_record
git commit -m "feat(protocol): MSG_V2_BAD_SERVICE + the service_id ++ query payload codec; fuzz the split"
```

---

### Task 2: The node routes queries by id and answers `BAD_SERVICE`

After this task the node *requires* the prefix; the client (Task 4) does not yet write it — so between Tasks 2 and 4 every SDK query is misparsed (its first payload byte read as the id). The `uc2_node` suites that issue queries through the client (`query_barrier.rs`, `services.rs`, `lin_v2`'s reads, `backup.rs`'s restore read) are therefore expected red until Task 4 lands; Task 2's own verification is the unit tests plus the suites that do not query. **Run Tasks 2–4 back to back.**

**Files:**
- Modify `uc2_node/src/node.rs` (imports 40–43; `send_retry` ~3378; `forward_svc_query` ~3388–3411; `drain_query_ring` ~3540–3634; the test module's `harness` rings ~5886–5899 are used as-is)
- Modify `uc2_node/examples/read_profile.rs:1032` (and its imports at 187)

**Interfaces:**
- `Consensus::send_bad_service(&mut self, client_id: u32, local_seq: u32, service_id: u8)`; `Consensus::has_service_ring(&self, id: u8) -> bool`.
- `PendingRead.service_id` is now the record's id.

- [ ] **Step 1: Write the failing unit tests** (in `node.rs`'s test module, next to `a_query_ring_hole_is_counted_on_its_own_cnc_line`)

```rust
    /// M14b: the query payload's first byte names the FSM. A query naming an
    /// id this node has no ring for is answered MSG_V2_BAD_SERVICE on the
    /// node broadcast, keyed by the client pair, and never parked.
    #[test]
    fn a_query_for_an_id_without_a_ring_is_answered_bad_service() {
        use uc_protocol::v2::ipc::{MSG_V2_BAD_SERVICE, MSG_V2_QUERY, write_query_payload};
        let mut h = harness();
        drive_to_serving_leader(&mut h);
        let mut node_egress =
            BroadcastRing::open(&h._dir.path().join("egress_node.broadcast")).unwrap().subscribe();
        let (mut producer, _c) =
            MpscRing::open(&h._dir.path().join("query.ring")).unwrap().into_split();
        let mut payload = Vec::new();
        write_query_payload(5, b"q", &mut payload); // 5: no ring on the harness node
        producer.try_write(MSG_V2_QUERY, 0, extra_client(9, 1), &payload).unwrap();
        write_query_payload(200, b"q", &mut payload); // out of range: same answer
        producer.try_write(MSG_V2_QUERY, FLAG_V2_LINEARIZABLE, extra_client(9, 2), &payload).unwrap();
        assert!(h.cons.drain_query_ring());
        assert!(h.cons.pending_reads.is_empty(), "a bad id is never parked");
        let mut buf = Vec::new();
        let mut got = Vec::new();
        while let Ok(Some(rec)) = node_egress.try_read(&mut buf) {
            got.push((rec.msg_type, client_from_extra(rec.header_extra), buf.clone()));
        }
        assert_eq!(got, vec![
            (MSG_V2_BAD_SERVICE, (9, 1), vec![5]),
            (MSG_V2_BAD_SERVICE, (9, 2), vec![200]),
        ]);
    }

    /// M14b: a snapshot read is forwarded to the NAMED id's ring (the harness
    /// gets a second ring for id 1 for this test), payload unchanged
    /// (`expected_epoch 0 ++ query`), and a linearizable read carries the id
    /// into its PendingRead.
    #[test]
    fn queries_route_to_the_named_ids_ring_and_pending_reads_carry_the_id() {
        use uc_protocol::v2::ipc::{MSG_V2_QUERY, MSG_V2_SVC_QUERY, write_query_payload};
        let mut h = harness();
        drive_to_serving_leader(&mut h);
        let (svc1_producer, mut svc1_consumer) =
            SpscRing::create(&h._dir.path().join("svc_query.1.ring"), 4096, 1024).unwrap().into_split();
        h.cons.svc_query[1] = Some(svc1_producer);
        let (mut producer, _c) =
            MpscRing::open(&h._dir.path().join("query.ring")).unwrap().into_split();
        let mut payload = Vec::new();
        write_query_payload(1, b"snap", &mut payload);
        producer.try_write(MSG_V2_QUERY, 0, extra_client(9, 3), &payload).unwrap();
        write_query_payload(1, b"lin", &mut payload);
        producer.try_write(MSG_V2_QUERY, FLAG_V2_LINEARIZABLE, extra_client(9, 4), &payload).unwrap();
        assert!(h.cons.drain_query_ring());
        let mut buf = Vec::new();
        let rec = svc1_consumer.try_read(&mut buf).unwrap().expect("forwarded to svc_query.1");
        assert_eq!(rec.msg_type, MSG_V2_SVC_QUERY);
        assert_eq!(client_from_extra(rec.header_extra), (9, 3));
        assert_eq!(&buf[..8], &0u64.to_le_bytes(), "snapshot reads skip the epoch check");
        assert_eq!(&buf[8..], b"snap", "the id byte is stripped before forwarding");
        assert!(svc1_consumer.try_read(&mut buf).unwrap().is_none());
        assert_eq!(h.cons.pending_reads.len(), 1);
        assert_eq!(h.cons.pending_reads[0].service_id, 1);
        assert_eq!(h.cons.pending_reads[0].query, b"lin");
        assert_eq!(h.cons.pending_reads[0].phase, ReadPhase::AwaitQuorum);
    }

    /// M14b: an EMPTY query payload (no id byte) is dropped — neither parked
    /// nor answered (plan deviation 5; the service drops short records the
    /// same way).
    #[test]
    fn an_empty_query_payload_is_dropped() {
        use uc_protocol::v2::ipc::MSG_V2_QUERY;
        let mut h = harness();
        drive_to_serving_leader(&mut h);
        let mut node_egress =
            BroadcastRing::open(&h._dir.path().join("egress_node.broadcast")).unwrap().subscribe();
        let (mut producer, _c) =
            MpscRing::open(&h._dir.path().join("query.ring")).unwrap().into_split();
        producer.try_write(MSG_V2_QUERY, FLAG_V2_LINEARIZABLE, extra_client(9, 5), &[]).unwrap();
        assert!(h.cons.drain_query_ring());
        assert!(h.cons.pending_reads.is_empty());
        let mut buf = Vec::new();
        assert!(node_egress.try_read(&mut buf).unwrap().is_none(), "no answer for a malformed record");
    }
```

(`BroadcastRing`, `SpscRing`, `MpscRing`, `extra_client`, `client_from_extra`, `FLAG_V2_LINEARIZABLE`, `ReadPhase` are already in scope in the test module — check the module's `use` lines and add what is missing.)

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc2_node --lib a_query_for_an_id_without_a_ring` — expected: compile error (`MSG_V2_BAD_SERVICE` import exists after Task 1, but the assertion on `got` fails: today the record is forwarded to ring 0 with the whole payload) — at minimum the `got == vec![…]` assertion fails with an empty `got`.

- [ ] **Step 3: Implement**

Imports: add `MSG_V2_BAD_SERVICE` and `split_query_payload` to the `uc_protocol::v2::ipc::{…}` import.

Next to `send_retry`:

```rust
    /// M14b: answer a query naming an id this node has no ring for with
    /// `MSG_V2_BAD_SERVICE` on the node egress, payload = the offending id.
    /// Pre-answer and side-effect-free (nothing was forwarded), so the client
    /// may re-issue with a different id.
    fn send_bad_service(&mut self, client_id: u32, local_seq: u32, service_id: u8) {
        let extra = extra_client(client_id, local_seq);
        let _ = self.egress_node.write(MSG_V2_BAD_SERVICE, 0, extra, &[service_id]);
    }

    /// M14b: whether `svc_query.<id>.ring` exists on this node — the declared
    /// set for a real node, `{0}` for a harness node (plan deviation 4).
    fn has_service_ring(&self, id: u8) -> bool {
        self.svc_query.get(id as usize).is_some_and(|p| p.is_some())
    }
```

`drain_query_ring`, the `Ok(Some(rec))` arm — replace everything from `let (client_id, local_seq) = …` up to and including the snapshot `continue` with:

```rust
                    let (client_id, local_seq) = client_from_extra(rec.header_extra);
                    // M14b: `service_id: u8 ++ query`. No id byte = malformed,
                    // dropped (the service drops short svc_query records the
                    // same way; the SDK always writes the prefix).
                    let Some((service_id, _)) = split_query_payload(&buf) else {
                        continue;
                    };
                    if !self.has_service_ring(service_id) {
                        self.send_bad_service(client_id, local_seq, service_id);
                        continue;
                    }
                    buf.remove(0); // strip the id; `buf` is now the query bytes
                    if rec.flags & FLAG_V2_LINEARIZABLE == 0 {
                        // Snapshot: forward immediately to the named id, epoch check skipped (0).
                        self.forward_svc_query(service_id, client_id, local_seq, 0, &buf);
                        continue;
                    }
```

and in the `PendingRead { … }` literal replace `service_id: 0,` (with its M14a comment) by `service_id,` with the comment "M14b: from the record; the barrier certifies on this FSM's slot". In `forward_svc_query`, the `else { return false; // not a ring id — M14b answers … }` comment becomes `// unreachable for queries admitted by drain_query_ring (has_service_ring); kept as the safe default`.

`uc2_node/examples/read_profile.rs:1032`: build the payload with `write_query_payload(0, &query_bytes, &mut scratch)` (a `Vec<u8>` scratch declared once outside the loop) and write `&scratch`; import `write_query_payload`.

- [ ] **Step 4: Run**

```bash
cargo test -p uc2_node --lib                       # incl. the 3 new tests and the 11 barrier tests
cargo build -p uc2_node --examples
cargo test -p uc2_node --test smoke --test learner --test failover   # suites that never query
```

Expected: PASS. (`query_barrier`, `services`, `lin_v2`, `backup` are expected red until Task 4 — do not "fix" them here.)

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p uc2_node --all-targets -- -D warnings
git add uc2_node/src/node.rs uc2_node/examples/read_profile.rs
git commit -m "feat(node): route MSG_V2_QUERY by its service_id prefix; MSG_V2_BAD_SERVICE for an id without a ring"
```

---

### Task 3: Slot masks — `expected`/`received`, `Partial`, `WrongRing`

Pure `uc2_client::slots` change with its own unit tests; nothing calls the new arguments until Task 4 (the two `engine.rs` call sites are updated to pass `expected = 0b1`/`ring = None` so the crate keeps compiling).

**Files:**
- Modify `uc2_client/src/slots.rs` (module doc 4–18; `Slot` 44–49; `Resolve` 38–42; `claim` 84–113; `resolve` 133–158; tests 219–479)
- Modify `uc2_client/src/engine.rs` (`send`'s `claim` call ~314; `handle_record`'s three `resolve` calls; the two unit tests' `claim` calls at ~635/659)

**Interfaces:**
```rust
pub(crate) enum Resolve {
    /// Terminal: the slot is freed. `fan_in` ⇔ `expected` had more than one bit.
    Won { user_data: u64, fan_in: bool },
    /// This ring's piece was recorded; other expected rings are still pending. Slot stays live.
    Partial,
    KindMismatch,
    /// A response from a ring the request did not expect — dropped, slot untouched.
    WrongRing,
    Miss,
}
impl SlotTable {
    pub(crate) fn claim(&self, user_data: u64, kind: ReqKind, deadline_ns: u64, expected: u8) -> Result<u64, ClaimError>; // expected != 0
    pub(crate) fn resolve(&self, wire_seq: u32, expect_kind: Option<ReqKind>, ring: Option<u8>) -> Resolve;
    pub(crate) fn slot_index(&self, wire_seq: u32) -> usize;
    pub(crate) fn slot_count(&self) -> usize;   // no longer cfg(test)
}
```
`ring: None` = a terminal, kind-agnostic answer (`NOT_LEADER`/`RETRY`/`BAD_SERVICE`) — completes the slot whatever `received` holds. `ring: Some(r)` = a `MSG_V2_RESPONSE` from ring `r` (`r < 8`).

- [ ] **Step 1: Write the failing tests** (replace the single-`claim` calls in the existing tests with a trailing `, 0b1` and add these)

```rust
    #[test]
    fn a_response_from_an_unexpected_ring_is_dropped_and_the_slot_survives() {
        let t = SlotTable::new(8, 0);
        let seq = t.claim(7, ReqKind::Submit, u64::MAX, 0b01).unwrap(); // expects ring 0 only
        assert_eq!(t.resolve(seq as u32, Some(ReqKind::Submit), Some(1)), Resolve::WrongRing);
        assert_eq!(t.inflight(), 1);
        assert_eq!(t.resolve(seq as u32, Some(ReqKind::Submit), Some(0)), Resolve::Won { user_data: 7, fan_in: false });
        assert_eq!(t.inflight(), 0);
    }

    #[test]
    fn fan_in_completes_only_when_every_expected_ring_answered_in_any_order() {
        let t = SlotTable::new(8, 0);
        let seq = t.claim(8, ReqKind::Submit, u64::MAX, 0b101).unwrap(); // rings 0 and 2
        assert_eq!(t.resolve(seq as u32, Some(ReqKind::Submit), Some(2)), Resolve::Partial);
        assert_eq!(t.inflight(), 1, "a partial keeps the slot live");
        // A duplicate from the same ring is a Miss, not a second piece.
        assert_eq!(t.resolve(seq as u32, Some(ReqKind::Submit), Some(2)), Resolve::Miss);
        assert_eq!(t.resolve(seq as u32, Some(ReqKind::Submit), Some(0)), Resolve::Won { user_data: 8, fan_in: true });
        assert_eq!(t.inflight(), 0);
        assert_eq!(t.resolve(seq as u32, Some(ReqKind::Submit), Some(0)), Resolve::Miss, "freed");
    }

    #[test]
    fn a_terminal_answer_completes_a_partial_fan_in() {
        let t = SlotTable::new(8, 0);
        let seq = t.claim(9, ReqKind::Query, u64::MAX, 0b11).unwrap();
        assert_eq!(t.resolve(seq as u32, Some(ReqKind::Query), Some(1)), Resolve::Partial);
        // NOT_LEADER / RETRY / BAD_SERVICE are ring-less and kind-agnostic: they end the whole request.
        assert_eq!(t.resolve(seq as u32, None, None), Resolve::Won { user_data: 9, fan_in: true });
        assert_eq!(t.inflight(), 0);
    }

    #[test]
    fn received_is_reset_when_a_slot_index_is_reused() {
        let t = SlotTable::new(1, 0); // 64 slots
        let seq = t.claim(1, ReqKind::Submit, u64::MAX, 0b11).unwrap();
        assert_eq!(t.resolve(seq as u32, Some(ReqKind::Submit), Some(0)), Resolve::Partial);
        // Drain (shutdown) frees it with ring 1 still pending.
        let mut aborted = Vec::new();
        t.drain_abort(|ud| aborted.push(ud));
        assert_eq!(aborted, vec![1]);
        // Wrap the table so the same index is claimed again with a fresh mask.
        t.set_next_seq_for_tests(seq + t.slot_count() as u64);
        let seq2 = t.claim(2, ReqKind::Submit, u64::MAX, 0b11).unwrap();
        assert_eq!(t.slot_index(seq2 as u32), t.slot_index(seq as u32));
        // The old ring-0 piece must not count for the new request.
        assert_eq!(t.resolve(seq2 as u32, Some(ReqKind::Submit), Some(1)), Resolve::Partial);
        assert_eq!(t.resolve(seq2 as u32, Some(ReqKind::Submit), Some(0)), Resolve::Won { user_data: 2, fan_in: true });
    }

    #[test]
    #[should_panic(expected = "expected mask must name at least one ring")]
    fn claim_refuses_an_empty_expected_mask() {
        let t = SlotTable::new(8, 0);
        let _ = t.claim(1, ReqKind::Submit, u64::MAX, 0);
    }
```

Also update the existing `kind_mismatch_leaves_the_slot_for_the_real_answer` to pass `Some(0)` as `ring` on both `resolve` calls (a `MSG_V2_RESPONSE` always comes from a ring), `second_resolve_is_a_miss_exactly_once` likewise, and the `concurrent_exactly_once_stress` resolver to `resolve(seq as u32, None, Some(0))` — its claims use `0b1`.

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc2_client --lib slots` — compile errors (arity, missing variants).

- [ ] **Step 3: Implement**

`Slot` gains `expected: AtomicU8` and `received: AtomicU8` (initialised 0 in `new`). Module doc: add invariant 7 — "`expected`/`received` are written in claim phase 2 (under the RESERVED word) and thereafter only by `resolve` on the poll thread; a slot completes on the single owner CAS only when the last expected bit lands or a ring-less terminal answer arrives."

`claim`: signature gains `expected: u8`; first line `assert!(expected != 0, "expected mask must name at least one ring");`; phase 2 adds `slot.expected.store(expected, Ordering::Relaxed); slot.received.store(0, Ordering::Relaxed);`.

`resolve`:

```rust
    pub(crate) fn resolve(&self, wire_seq: u32, expect_kind: Option<ReqKind>, ring: Option<u8>) -> Resolve {
        let slot = &self.slots[(wire_seq as usize) & self.mask];
        let owner = slot.owner.load(Ordering::Acquire);
        if owner == FREE || owner == RESERVED {
            return Resolve::Miss;
        }
        let seq = owner - 1;
        if seq as u32 != wire_seq {
            return Resolve::Miss; // stale generation
        }
        if let Some(expect) = expect_kind
            && slot.kind.load(Ordering::Relaxed) != expect as u8
        {
            return Resolve::KindMismatch; // leave the slot for the real answer
        }
        let expected = slot.expected.load(Ordering::Relaxed);
        if let Some(r) = ring {
            debug_assert!(r < 8);
            let bit = 1u8 << r;
            if expected & bit == 0 {
                return Resolve::WrongRing; // not ours to answer; slot untouched
            }
            // `received` is only ever mutated on the poll thread (this
            // function) after the claim-time reset, so a plain fetch_or is
            // exact: a repeated bit is a duplicate delivery on that ring.
            let prev = slot.received.fetch_or(bit, Ordering::AcqRel);
            if prev & bit != 0 {
                return Resolve::Miss;
            }
            if (prev | bit) != expected {
                return Resolve::Partial; // more rings still to answer
            }
        }
        let user_data = slot.user_data.load(Ordering::Relaxed);
        if slot
            .owner
            .compare_exchange(owner, FREE, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Resolve::Miss; // lost the race to sweep/another delivery
        }
        self.inflight.fetch_sub(1, Ordering::AcqRel);
        Resolve::Won { user_data, fan_in: expected.count_ones() > 1 }
    }

    /// The table index a wire seq maps to (the fan-in buffer is keyed by it).
    pub(crate) fn slot_index(&self, wire_seq: u32) -> usize {
        (wire_seq as usize) & self.mask
    }

    pub(crate) fn slot_count(&self) -> usize {
        self.slots.len()
    }
```

(`slot_count` loses its `#[cfg(test)]`; `set_next_seq_for_tests` keeps it.)

`engine.rs` (keep it compiling, semantics unchanged for now): `claim(user_data, kind, deadline_ns, 0b1)`; in `handle_record`, `resolve(wire_seq, Some(delivered), Some(0))` for `MSG_V2_RESPONSE` and `resolve(wire_seq, None, None)` for the two terminal arms; the `MSG_V2_RESPONSE` match gains `Resolve::Partial | Resolve::WrongRing => 0` (Task 4 gives them their real handling). The two engine unit tests pass `0b1`.

- [ ] **Step 4: Run**

Run: `cargo test -p uc2_client` — expected PASS (`slots` 10 → 15 tests; every synthetic test unchanged in behaviour).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy -p uc2_client --all-targets -- -D warnings
git add uc2_client/src/slots.rs uc2_client/src/engine.rs
git commit -m "feat(client): slot expected/received ring masks — Partial, WrongRing, fan-in completion on the last piece"
```

---

### Task 4: The `Engine` — N egress rings, the query prefix, per-ring matching and the fan-in

This is the task that makes the SDK's queries parse again on the node (Task 2). Everything the blocking tiers need lands here on `SendHalf`/`PollHalf`; `PipelinedClient`/`Client` follow in Task 5.

**Files:**
- Modify `uc2_client/src/engine.rs` (consts 51–60; imports 33–49; `SubmitError` 111–130; `StatCells`/`EngineStats` 132–179; `Shared` 181–195; `SendHalf`/`PollHalf`/`Outcome` 197–246; `attach` 248–288; `send`/`try_submit`/`try_query` 290–342; `Clone for SendHalf` 427–435; `poll`/`wait_handle` 437–468; `drain_ring` 470–494; `handle_record` 496–570; `maintenance` 572–602)
- Modify `uc2_client/Cargo.toml` (add `bytes = { workspace = true }` under `[dependencies]`)
- Modify `uc2_client/tests/engine_synthetic.rs` (fixtures 17–61 and 169–196; new tests)

**Interfaces:**
```rust
// uc2_client (engine.rs), all pub unless noted
pub enum Outcome<'a> { Response(&'a [u8]), Responses(&'a [(u8, Bytes)]), NotLeader { hint: Option<u32> }, Retry, BadService { id: u8 }, TimedOut, InstanceRestart { attached: u128, current: u128 } }
pub enum SubmitError { …existing…, ServiceNotDeclared { id: u8, declared: u64 } }
pub struct EngineStats { …existing…, pub wrong_ring: u64, pub bad_service: u64 }
impl SendHalf {
    pub fn declared(&self) -> u64;                                             // the page's mask, 0 folded to 0b1
    pub fn try_submit(&self, user_data: u64, cmd: &[u8]) -> Result<(), SubmitError>;            // = try_submit_to(.., 0, ..)
    pub fn try_submit_to(&self, user_data: u64, id: u8, cmd: &[u8]) -> Result<(), SubmitError>;
    pub fn try_submit_all(&self, user_data: u64, cmd: &[u8]) -> Result<(), SubmitError>;
    pub fn try_query(&self, user_data: u64, q: &[u8], c: Consistency) -> Result<(), SubmitError>;   // = try_query_on(.., 0, ..)
    pub fn try_query_on(&self, user_data: u64, id: u8, q: &[u8], c: Consistency) -> Result<(), SubmitError>;
}
pub(crate) fn egress_service_ring(id: u8) -> String;   // "egress_service.<id>.broadcast"
```
`Completion.position` is `Some` for both `Response` and `Responses` (one frame ⇒ one position; the first piece's).

- [ ] **Step 1: Write the failing tests** (`uc2_client/tests/engine_synthetic.rs`)

Add fixtures next to `make_instance`:

```rust
/// A synthetic dir whose page declares FSMs {0, 1} and has both egress rings.
fn make_instance_two_fsms(dir: &Path, app_id: &str) {
    let page = CncPage::create_file(&dir.join("cnc2.dat"), &meta(app_id)).unwrap();
    page.store_services_declared(0b11);
    MpscRing::create(&dir.join("ingress.ring"), MIB, 128).unwrap();
    MpscRing::create(&dir.join("query.ring"), MIB, 256).unwrap();
    BroadcastRing::create(&dir.join("egress_service.0.broadcast"), MIB, 128).unwrap();
    BroadcastRing::create(&dir.join("egress_service.1.broadcast"), MIB, 128).unwrap();
    BroadcastRing::create(&dir.join("egress_node.broadcast"), MIB, 128).unwrap();
}

/// Egress producer for FSM `id`'s ring.
fn egress_for(dir: &Path, id: u8) -> uc_protocol::ring::BroadcastProducer {
    BroadcastRing::open(&dir.join(format!("egress_service.{id}.broadcast"))).unwrap().producer()
}

/// `MSG_V2_RESPONSE` payload: `position ++ body`.
fn response(position: u64, body: &[u8]) -> Vec<u8> {
    let mut p = position.to_le_bytes().to_vec();
    p.extend_from_slice(body);
    p
}
```

Extend `drain`'s match with:

```rust
            uc2_client::Outcome::Responses(parts) => format!(
                "responses:{:?}",
                parts.iter().map(|(id, b)| (*id, String::from_utf8_lossy(b).into_owned())).collect::<Vec<_>>()
            ),
            uc2_client::Outcome::BadService { id } => format!("badservice:{id}"),
```

Add the tests (imports: `MSG_V2_BAD_SERVICE, MSG_V2_QUERY, FLAG_V2_LINEARIZABLE` from `uc_protocol::v2::ipc`, `SubmitError`, `Consistency` from `uc2_client`):

```rust
#[test]
fn attach_opens_every_declared_ring_and_default_submit_ignores_the_other_fsm() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance_two_fsms(dir.path(), "two");
    let (send, mut poll) = Engine::attach(dir.path(), "two", cfg()).unwrap();
    assert_eq!(send.declared(), 0b11);
    let cid = send.client_id();
    send.try_submit(1, b"x").unwrap();                       // expects FSM 0 only
    // FSM 1 answers first (it is faster today): not ours, dropped and counted.
    egress_for(dir.path(), 1).write(MSG_V2_RESPONSE, 0, extra_client(cid, 0), &response(96, b"one")).unwrap();
    assert!(drain(&mut poll).is_empty());
    assert_eq!(poll.stats().wrong_ring, 1);
    egress_for(dir.path(), 0).write(MSG_V2_RESPONSE, 0, extra_client(cid, 0), &response(96, b"zero")).unwrap();
    assert_eq!(drain(&mut poll), vec![(1, Some(96), "resp:4".to_string())]);
    assert_eq!(poll.stats().responses, 1);
}

#[test]
fn submit_all_fans_in_every_declared_ring_in_id_order_whatever_the_arrival_order() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance_two_fsms(dir.path(), "all");
    let (send, mut poll) = Engine::attach(dir.path(), "all", cfg()).unwrap();
    let cid = send.client_id();
    send.try_submit_all(2, b"x").unwrap();
    egress_for(dir.path(), 1).write(MSG_V2_RESPONSE, 0, extra_client(cid, 0), &response(4096, b"b")).unwrap();
    assert!(drain(&mut poll).is_empty(), "one of two pieces: not complete");
    assert_eq!(send.inflight(), 1);
    egress_for(dir.path(), 0).write(MSG_V2_RESPONSE, 0, extra_client(cid, 0), &response(4096, b"a")).unwrap();
    assert_eq!(
        drain(&mut poll),
        vec![(2, Some(4096), "responses:[(0, \"a\"), (1, \"b\")]".to_string())],
        "ordered by id, not by arrival"
    );
    assert_eq!(send.inflight(), 0);
    // A late duplicate from ring 0 is a Miss (the slot is free), not a second completion.
    egress_for(dir.path(), 0).write(MSG_V2_RESPONSE, 0, extra_client(cid, 0), &response(4096, b"a")).unwrap();
    assert!(drain(&mut poll).is_empty());
    assert_eq!(poll.stats().duplicates, 1);
}

#[test]
fn submit_to_expects_only_the_named_ring() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance_two_fsms(dir.path(), "to");
    let (send, mut poll) = Engine::attach(dir.path(), "to", cfg()).unwrap();
    let cid = send.client_id();
    send.try_submit_to(3, 1, b"x").unwrap();
    egress_for(dir.path(), 0).write(MSG_V2_RESPONSE, 0, extra_client(cid, 0), &response(96, b"zero")).unwrap();
    assert!(drain(&mut poll).is_empty());
    assert_eq!(poll.stats().wrong_ring, 1);
    egress_for(dir.path(), 1).write(MSG_V2_RESPONSE, 0, extra_client(cid, 0), &response(96, b"one")).unwrap();
    assert_eq!(drain(&mut poll), vec![(3, Some(96), "resp:3".to_string())]);
}

#[test]
fn an_undeclared_or_out_of_range_id_is_refused_at_the_door() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance_two_fsms(dir.path(), "undecl");
    let (send, _poll) = Engine::attach(dir.path(), "undecl", cfg()).unwrap();
    assert!(matches!(send.try_submit_to(4, 2, b"x"), Err(SubmitError::ServiceNotDeclared { id: 2, declared: 0b11 })));
    assert!(matches!(send.try_query_on(4, 9, b"q", Consistency::Snapshot), Err(SubmitError::ServiceNotDeclared { id: 9, declared: 0b11 })));
    assert_eq!(send.inflight(), 0, "a door refusal never claims a slot");
    // A harness page (declared 0) folds to {0}: id 1 is refused, id 0 accepted.
    let dir0 = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir0.path(), "harness", MIB, MIB);
    let (send0, _p0) = Engine::attach(dir0.path(), "harness", cfg()).unwrap();
    assert_eq!(send0.declared(), 0b1);
    assert!(matches!(send0.try_submit_to(5, 1, b"x"), Err(SubmitError::ServiceNotDeclared { id: 1, declared: 0b1 })));
    send0.try_submit_to(5, 0, b"x").unwrap();
}

#[test]
fn try_query_on_writes_the_service_id_prefix_and_counts_it_toward_the_cap() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance_two_fsms(dir.path(), "prefix");
    let (send, _poll) = Engine::attach(dir.path(), "prefix", EngineConfig { max_payload: Some(4), ..cfg() }).unwrap();
    let (_qp, mut qc) = MpscRing::open(&dir.path().join("query.ring")).unwrap().into_split();
    send.try_query_on(6, 1, b"zz", Consistency::Linearizable).unwrap();
    let mut buf = Vec::new();
    let rec = qc.try_read(&mut buf).unwrap().expect("one query record");
    assert_eq!(rec.msg_type, MSG_V2_QUERY);
    assert_eq!(rec.flags, FLAG_V2_LINEARIZABLE);
    assert_eq!(buf, [1, b'z', b'z'], "service_id ++ query");
    // The cap counts the wire payload: 4 query bytes + 1 prefix = 5 > 4.
    assert!(matches!(send.try_query_on(7, 0, b"zzzz", Consistency::Snapshot), Err(SubmitError::PayloadTooLarge { len: 5, max: 4 })));
    // A submit has no prefix: 4 bytes fit exactly.
    send.try_submit(8, b"zzzz").unwrap();
}

#[test]
fn bad_service_on_the_node_ring_resolves_kind_agnostic_with_the_id() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance_two_fsms(dir.path(), "bad");
    let (send, mut poll) = Engine::attach(dir.path(), "bad", cfg()).unwrap();
    let cid = send.client_id();
    send.try_query_on(9, 1, b"q", Consistency::Snapshot).unwrap();
    egress_node(dir.path()).write(MSG_V2_BAD_SERVICE, 0, extra_client(cid, 0), &[1]).unwrap();
    assert_eq!(drain(&mut poll), vec![(9, None, "badservice:1".to_string())]);
    assert_eq!(poll.stats().bad_service, 1);
    assert_eq!(send.inflight(), 0);
}

#[test]
fn a_terminal_answer_ends_a_partial_fan_in_and_late_pieces_are_duplicates() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance_two_fsms(dir.path(), "term");
    let (send, mut poll) = Engine::attach(dir.path(), "term", cfg()).unwrap();
    let cid = send.client_id();
    send.try_submit_all(10, b"x").unwrap();
    egress_for(dir.path(), 1).write(MSG_V2_RESPONSE, 0, extra_client(cid, 0), &response(96, b"b")).unwrap();
    assert!(drain(&mut poll).is_empty());
    egress_node(dir.path()).write(MSG_V2_RETRY, 0, extra_client(cid, 0), &[]).unwrap();
    assert_eq!(drain(&mut poll), vec![(10, None, "retry".to_string())]);
    egress_for(dir.path(), 0).write(MSG_V2_RESPONSE, 0, extra_client(cid, 0), &response(96, b"a")).unwrap();
    assert!(drain(&mut poll).is_empty());
    assert_eq!(poll.stats().duplicates, 1);
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc2_client --test engine_synthetic` — compile errors (`declared`, `try_submit_to`, `Outcome::Responses`, …).

- [ ] **Step 3: Implement**

Consts: replace `EGRESS_SERVICE` with

```rust
/// M14b: FSM `id`'s response ring. The engine opens one per declared id.
pub(crate) fn egress_service_ring(id: u8) -> String {
    format!("egress_service.{id}.broadcast")
}
```

Imports: add `bytes::Bytes`, `std::cell::RefCell`, `uc_protocol::v2::cnc::CNC_MAX_SERVICES`, `MSG_V2_BAD_SERVICE`, `write_query_payload` (ipc).

`SubmitError` gains:

```rust
    /// M14b: `id` is not in the attached node's declared set (or `>= 8`).
    /// Refused at the door — no slot claimed, nothing written.
    #[error("service id {id} is not declared on this node (declared set 0b{declared:b})")]
    ServiceNotDeclared { id: u8, declared: u64 },
```

`StatCells`/`EngineStats`/`snapshot`: add `wrong_ring` ("a RESPONSE from a ring the request did not expect — a sibling FSM's answer to a request that named another; dropped") and `bad_service`.

`Shared` gains `declared: u64`. `SendHalf` gains `scratch: RefCell<Vec<u8>>` (cloned as a fresh `RefCell::new(Vec::new())`). `PollHalf` becomes:

```rust
pub struct PollHalf {
    shared: Arc<Shared>,
    /// One consumer per declared FSM, ascending id (FSM 0 first).
    egress_services: Vec<(u8, BroadcastConsumer)>,
    egress_node: BroadcastConsumer,
    buf: Vec<u8>,
    /// M14b fan-in buffer, one entry per slot index: the pieces of a
    /// `try_submit_all` that have arrived so far. Keyed by the slot's wire
    /// seq so a reused index never mixes generations; a swept slot's stale
    /// pieces stay until the index is reused (bounded: ≤ 8 pieces per slot).
    fanin: Vec<FanIn>,
    cycle: u64,
}

#[derive(Default)]
struct FanIn {
    seq: u32,
    position: u64,
    /// `Bytes`, not `Vec<u8>`: refcounted from here to the caller (deviation 1).
    parts: Vec<(u8, Bytes)>,
}
```

`Outcome` gains, after `Response`:

```rust
    /// M14b: a completed `try_submit_all` — one `(service_id, response)` per
    /// declared FSM, ascending by id, all for the same log position. `Bytes`
    /// so the pieces travel refcounted to the caller (deviation 1).
    Responses(&'a [(u8, Bytes)]),
```

and after `Retry`:

```rust
    /// M14b: the node has no ring for the requested id (`MSG_V2_BAD_SERVICE`).
    /// Pre-side-effect, like `Retry`.
    BadService { id: u8 },
```

`attach`: after `let instance_id = …`:

```rust
        // M14b: which FSMs exist here. A page reading 0 is a harness node
        // (nothing declared, FSM 0 ringed) — the same fold every attacher uses.
        let declared = match cnc.services_declared() {
            0 => 0b1,
            d => d,
        };
```

replace the `egress_service` open with

```rust
        let mut egress_services = Vec::new();
        for id in 0..CNC_MAX_SERVICES as u8 {
            if declared & (1u64 << id) != 0 {
                let ring = BroadcastRing::open(&instance_dir.join(egress_service_ring(id)))?.subscribe();
                egress_services.push((id, ring));
            }
        }
```

`Shared { …, declared }`; `SendHalf { shared, ingress, query, scratch: RefCell::new(Vec::new()) }`; `PollHalf { shared, egress_services, egress_node, buf: Vec::new(), fanin: (0..table_len).map(|_| FanIn::default()).collect(), cycle: 0 }` where `table_len = shared.table.slot_count()` (read before the `Arc` move, or via `shared.table.slot_count()` after — `Shared` is in an `Arc`, so read it after constructing `shared`).

`send` gains `expected: u8` and `prefix: Option<u8>`:

```rust
    fn send(
        &self,
        ring: &MpscProducer,
        msg_type: u16,
        flags: u16,
        kind: ReqKind,
        user_data: u64,
        bytes: &[u8],
        expected: u8,
        prefix: Option<u8>,
    ) -> Result<(), SubmitError> {
        let s = &self.shared;
        if s.dead.load(Ordering::Acquire) { … unchanged … }
        if s.serving_gate && … { … unchanged … }
        // The cap describes the WIRE payload (deviation 6): a query carries
        // its one-byte service id.
        let wire_len = bytes.len() + usize::from(prefix.is_some());
        if let Some(max) = s.max_payload
            && wire_len > max
        {
            return Err(SubmitError::PayloadTooLarge { len: wire_len, max });
        }
        let deadline_ns = s.t0.elapsed().as_nanos() as u64 + s.timeout_ns;
        let seq = s
            .table
            .claim(user_data, kind, deadline_ns, expected)
            .map_err(|_| SubmitError::Backpressure)?;
        let extra = extra_client(s.client_id, seq as u32);
        let write_result = match prefix {
            None => ring.try_write(msg_type, flags, extra, bytes),
            Some(id) => {
                // One `try_write` takes one slice: assemble `id ++ bytes` in
                // this half's scratch (SendHalf is !Sync; the RefCell is never
                // contended).
                let mut scratch = self.scratch.borrow_mut();
                write_query_payload(id, bytes, &mut scratch);
                ring.try_write(msg_type, flags, extra, &scratch)
            }
        };
        finish_write(&s.table, &s.stats, seq, write_result)
    }

    /// The bit for a declared id, or the door refusal.
    fn expect_one(&self, id: u8) -> Result<u8, SubmitError> {
        let declared = self.shared.declared;
        if (id as usize) < CNC_MAX_SERVICES && declared & (1u64 << id) != 0 {
            Ok(1u8 << id)
        } else {
            Err(SubmitError::ServiceNotDeclared { id, declared })
        }
    }

    /// The declared set this engine attached to (bit i ⇔ FSM i), `0b1` on a harness node.
    pub fn declared(&self) -> u64 {
        self.shared.declared
    }

    /// Submit a command; FSM 0 answers. See the module's central contract.
    pub fn try_submit(&self, user_data: u64, cmd_bytes: &[u8]) -> Result<(), SubmitError> {
        self.try_submit_to(user_data, 0, cmd_bytes)
    }

    /// M14b: submit a command; FSM `id` answers (every FSM applies it).
    pub fn try_submit_to(&self, user_data: u64, id: u8, cmd_bytes: &[u8]) -> Result<(), SubmitError> {
        let expected = self.expect_one(id)?;
        self.send(&self.ingress, MSG_V2_SUBMIT, 0, ReqKind::Submit, user_data, cmd_bytes, expected, None)
    }

    /// M14b: submit a command and collect EVERY declared FSM's answer
    /// (`Outcome::Responses`, ascending by id, one completion).
    pub fn try_submit_all(&self, user_data: u64, cmd_bytes: &[u8]) -> Result<(), SubmitError> {
        let expected = self.shared.declared as u8; // ids < 8 ⇒ the mask fits
        self.send(&self.ingress, MSG_V2_SUBMIT, 0, ReqKind::Submit, user_data, cmd_bytes, expected, None)
    }

    /// Issue a read against FSM 0; nonblocking.
    pub fn try_query(&self, user_data: u64, query_bytes: &[u8], c: Consistency) -> Result<(), SubmitError> {
        self.try_query_on(user_data, 0, query_bytes, c)
    }

    /// M14b: issue a read against FSM `id`. The wire payload is `id ++ query`.
    pub fn try_query_on(&self, user_data: u64, id: u8, query_bytes: &[u8], c: Consistency) -> Result<(), SubmitError> {
        let expected = self.expect_one(id)?;
        let flags = match c {
            Consistency::Linearizable => FLAG_V2_LINEARIZABLE,
            Consistency::Snapshot => 0,
        };
        self.send(&self.query, MSG_V2_QUERY, flags, ReqKind::Query, user_data, query_bytes, expected, Some(id))
    }
```

`poll`:

```rust
    pub fn poll(&mut self, mut cb: impl FnMut(Completion<'_>)) -> usize {
        self.cycle += 1;
        let maint = self.cycle.is_multiple_of(64);
        let PollHalf { shared, egress_services, egress_node, buf, fanin, .. } = self;
        let mut emitted = 0usize;
        if maint {
            emitted += maintenance(shared, &mut cb);
        }
        for (id, ring) in egress_services.iter_mut() {
            emitted += drain_ring(ring, Some(*id), shared, buf, fanin, &mut cb);
        }
        emitted += drain_ring(egress_node, None, shared, buf, fanin, &mut cb);
        emitted
    }

    /// Wait handle for FSM 0's egress broadcast — the default responder's
    /// ring. Deviation 3: a `RingWaitHandle` is one futex, so a parked driver
    /// is woken by FSM 0's publishes only; a completion that lands solely on
    /// another FSM's ring resolves at the park timeout (≤ 1 ms in the
    /// pipelined driver). The gateway only ever uses FSM 0.
    pub fn wait_handle(&self) -> uc_protocol::ring::RingWaitHandle {
        self.egress_services[0].1.wait_handle()
    }
```

`drain_ring(ring, ring_id: Option<u8>, shared, buf, fanin: &mut [FanIn], cb)` passes `ring_id` and `fanin` into `handle_record(shared, fanin, ring_id, &rec, buf, cb)`.

`handle_record`:

```rust
fn handle_record(
    shared: &Shared,
    fanin: &mut [FanIn],
    ring_id: Option<u8>,
    rec: &RecordHeader,
    buf: &[u8],
    cb: &mut impl FnMut(Completion<'_>),
) -> usize {
    let (cid, wire_seq) = client_from_extra(rec.header_extra);
    if cid != shared.client_id {
        return 0; // every client sees every broadcast record
    }
    match rec.msg_type {
        MSG_V2_RESPONSE => {
            let Some(ring) = ring_id else {
                return 0; // a RESPONSE never travels on the node ring
            };
            if buf.len() < 8 {
                shared.stats.corrupt.fetch_add(1, Ordering::Relaxed);
                return 0;
            }
            let delivered = if rec.flags & FLAG_V2_IS_QUERY != 0 { ReqKind::Query } else { ReqKind::Submit };
            let position = u64::from_le_bytes(buf[..8].try_into().unwrap());
            match shared.table.resolve(wire_seq, Some(delivered), Some(ring)) {
                Resolve::Won { user_data, fan_in: false } => {
                    shared.stats.responses.fetch_add(1, Ordering::Relaxed);
                    cb(Completion { user_data, position: Some(position), outcome: Outcome::Response(&buf[8..]) });
                    1
                }
                Resolve::Won { user_data, fan_in: true } => {
                    let f = &mut fanin[shared.table.slot_index(wire_seq)];
                    f.push_piece(wire_seq, position, ring, &buf[8..]);
                    f.parts.sort_by_key(|p| p.0);
                    shared.stats.responses.fetch_add(1, Ordering::Relaxed);
                    cb(Completion { user_data, position: Some(f.position), outcome: Outcome::Responses(&f.parts) });
                    f.parts.clear();
                    1
                }
                Resolve::Partial => {
                    fanin[shared.table.slot_index(wire_seq)].push_piece(wire_seq, position, ring, &buf[8..]);
                    0
                }
                Resolve::WrongRing => {
                    shared.stats.wrong_ring.fetch_add(1, Ordering::Relaxed);
                    0
                }
                Resolve::KindMismatch => { shared.stats.kind_mismatch.fetch_add(1, Ordering::Relaxed); 0 }
                Resolve::Miss => { shared.stats.duplicates.fetch_add(1, Ordering::Relaxed); 0 }
            }
        }
        MSG_V2_NOT_LEADER => { … unchanged, with resolve(wire_seq, None, None) … }
        MSG_V2_RETRY => { … unchanged, with resolve(wire_seq, None, None) … }
        MSG_V2_BAD_SERVICE => {
            let id = buf.first().copied().unwrap_or(u8::MAX);
            match shared.table.resolve(wire_seq, None, None) {
                Resolve::Won { user_data, .. } => {
                    shared.stats.bad_service.fetch_add(1, Ordering::Relaxed);
                    cb(Completion { user_data, position: None, outcome: Outcome::BadService { id } });
                    1
                }
                _ => 0,
            }
        }
        _ => 0,
    }
}

impl FanIn {
    /// Record one piece; a different wire seq means the slot index was
    /// reused — start over for the new generation.
    fn push_piece(&mut self, seq: u32, position: u64, ring: u8, body: &[u8]) {
        if self.seq != seq || self.parts.is_empty() {
            self.seq = seq;
            self.position = position;
            self.parts.clear();
        }
        self.parts.push((ring, Bytes::copy_from_slice(body)));
    }
}
```

(The `Won { fan_in: true }` arm matches `Resolve::Won { user_data, fan_in: true }` and the `NOT_LEADER`/`RETRY` arms bind `Resolve::Won { user_data, .. }`.)

- [ ] **Step 4: Run**

```bash
cargo test -p uc2_client
cargo test -p uc2_node --test query_barrier --test services   # green again: the SDK writes the prefix
```

Expected: PASS — `engine_synthetic` 14 → 21 tests; `services.rs` and `query_barrier.rs` back to green. `uc2_gateway` does not compile yet (exhaustive matches) — Task 5.

- [ ] **Step 5: Clippy on the crate + commit**

```bash
cargo clippy -p uc2_client --all-targets -- -D warnings
git add uc2_client/src/engine.rs uc2_client/tests/engine_synthetic.rs
git commit -m "feat(client): Engine opens every declared FSM ring; try_submit_to/try_submit_all/try_query_on; per-ring matching, fan-in, BadService"
```

---

### Task 5: The blocking tiers, the fan-in ticket, and every exhaustive match

**Files:**
- Modify `uc2_client/src/ticket.rs` (`State`/`TicketCore` 10–48; the three decode sites 73–164; `ticket_pair` 156–164; tests)
- Modify `uc2_client/src/pipelined.rs` (`PipelinedClient` 113–148; the four methods 150–187; `dispatch` 236–299; `spawn_driver`'s `resolve` closure 398–414; unit tests 439–561)
- Modify `uc2_client/src/client.rs` (49–127), `uc2_client/src/error.rs` (`ClientError`), `uc2_client/src/lib.rs` (re-exports 55–71)
- Modify `uc2_gateway/src/edge.rs` (`submit_or_query`'s `SubmitError` match ~1351–1400; `complete`'s `Outcome` match 1636–1677), `uc2_gateway/examples/hop_bench/engine_load.rs:171-182`, `uc2_gateway/examples/m12_gate.rs:686-712`, `uc2_node/examples/m5_gate.rs:614-636`

**Interfaces:**
```rust
// uc2_client
pub struct FanInTicket<R> { … }                      // wait()/wait_timeout()/Future → Result<Vec<(u8, R)>, ClientError>
impl PipelinedClient {
    pub fn declared(&self) -> u64;
    pub fn submit_to<C: Serialize, R: DeserializeOwned>(&self, id: u8, cmd: &C) -> Result<Ticket<R>, ClientError>;
    pub fn submit_all<C: Serialize, R: DeserializeOwned>(&self, cmd: &C) -> Result<FanInTicket<R>, ClientError>;
    pub fn query_snapshot_on<Q: Serialize, QR: DeserializeOwned>(&self, id: u8, q: &Q) -> Result<Ticket<QR>, ClientError>;
    pub fn query_linearizable_on<Q: Serialize, QR: DeserializeOwned>(&self, id: u8, q: &Q) -> Result<Ticket<QR>, ClientError>;
}
impl Client {   // blocking shim: same names, returning R / Vec<(u8, R)> / QR
    pub fn submit_to<C, R>(&self, id: u8, cmd: &C) -> Result<R, ClientError>;
    pub fn submit_all<C, R>(&self, cmd: &C) -> Result<Vec<(u8, R)>, ClientError>;
    pub fn query_snapshot_on<Q, QR>(&self, id: u8, q: &Q) -> Result<QR, ClientError>;
    pub fn query_linearizable_on<Q, QR>(&self, id: u8, q: &Q) -> Result<QR, ClientError>;
    pub fn declared(&self) -> u64;
}
pub enum ClientError { …, ServiceNotDeclared { id: u8, declared: u64 } }   // both the door refusal and MSG_V2_BAD_SERVICE
// ticket.rs (crate-private)
pub(crate) enum Resolved { One { position: u64, bytes: Bytes }, Many { position: u64, parts: Vec<(u8, Bytes)> } }
impl TicketCore { pub(crate) fn resolve(&self, r: Result<Resolved, ClientError>); }
pub(crate) fn fan_in_ticket_pair<R>() -> (FanInTicket<R>, Arc<TicketCore>);
```

- [ ] **Step 1: Write the failing tests**

`ticket.rs` tests (next to `resolve_then_wait_decodes`):

```rust
    #[test]
    fn fan_in_ticket_decodes_every_piece_in_order() {
        let (t, core) = fan_in_ticket_pair::<u64>();
        let enc = |v: u64| Bytes::from(bincode::serde::encode_to_vec(v, bincode::config::standard()).unwrap());
        core.resolve(Ok(Resolved::Many { position: 96, parts: vec![(0, enc(7)), (3, enc(9))] }));
        assert_eq!(t.wait().unwrap(), vec![(0, 7u64), (3, 9u64)]);
    }

    #[test]
    fn a_single_response_on_a_fan_in_ticket_and_vice_versa_are_decode_errors() {
        let (t, core) = fan_in_ticket_pair::<u64>();
        core.resolve(Ok(Resolved::One { position: 96, bytes: Bytes::from_static(&[7, 0, 0, 0, 0, 0, 0, 0]) }));
        assert!(matches!(t.wait(), Err(ClientError::Decode(_))));
        let (t1, core1) = ticket_pair::<u64>();
        core1.resolve(Ok(Resolved::Many { position: 96, parts: vec![] }));
        assert!(matches!(t1.wait(), Err(ClientError::Decode(_))));
    }

    #[test]
    fn fan_in_ticket_error_resolution_surfaces_the_error() {
        let (t, core) = fan_in_ticket_pair::<u64>();
        core.resolve(Err(ClientError::ServiceNotDeclared { id: 4, declared: 0b11 }));
        assert!(matches!(t.wait(), Err(ClientError::ServiceNotDeclared { id: 4, declared: 0b11 })));
    }
```

(Existing ticket tests call `core.resolve(Ok((pos, bytes)))` — update each to `Ok(Resolved::One { position: pos, bytes: Bytes::from(bytes) })`; the `resolved_bytes` helper likewise. `use bytes::Bytes;` at the top of `ticket.rs`.)

`pipelined.rs` unit tests (its `make_instance` fixture creates a harness page; add a two-FSM variant like Task 4's — `make_instance_two_fsms(dir, app)` storing `services_declared(0b11)` and creating `egress_service.1.broadcast`):

```rust
    #[test]
    fn submit_all_against_a_two_fsm_page_fans_in_and_submit_to_an_undeclared_id_is_refused() {
        use uc_protocol::ring::BroadcastRing;
        use uc_protocol::v2::ipc::{MSG_V2_RESPONSE, extra_client};
        let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
        make_instance_two_fsms(dir.path(), "fan");
        let client = PipelinedClient::connect(dir.path(), "fan", PipelinedConfig { serving_gate: false, ..PipelinedConfig::default() }).unwrap();
        assert_eq!(client.declared(), 0b11);
        assert!(matches!(client.submit_to::<u64, u64>(2, &1), Err(ClientError::ServiceNotDeclared { id: 2, declared: 0b11 })));
        let ticket = client.submit_all::<u64, u64>(&1).unwrap();
        let cid = client.client_id();
        let enc = |v: u64| {
            let mut p = 4096u64.to_le_bytes().to_vec();
            p.extend(bincode::serde::encode_to_vec(v, bincode::config::standard()).unwrap());
            p
        };
        // seq 0 is this client's first request; FSM 1 answers before FSM 0.
        BroadcastRing::open(&dir.path().join("egress_service.1.broadcast")).unwrap().producer()
            .write(MSG_V2_RESPONSE, 0, extra_client(cid, 0), &enc(11)).unwrap();
        BroadcastRing::open(&dir.path().join("egress_service.0.broadcast")).unwrap().producer()
            .write(MSG_V2_RESPONSE, 0, extra_client(cid, 0), &enc(10)).unwrap();
        assert_eq!(ticket.wait().unwrap(), vec![(0, 10u64), (1, 11u64)]);
        client.shutdown();
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc2_client --lib ticket` / `--lib pipelined` — compile errors.

- [ ] **Step 3: Implement**

`ticket.rs`:

```rust
/// What a completion carried: one response, or a fan-in's per-FSM pieces.
/// `Bytes` throughout (deviation 1): a `Ticket`/`FanInTicket` decodes from
/// the refcounted piece, and a caller who wants the raw bytes gets them
/// without a copy.
pub(crate) enum Resolved {
    One { position: u64, bytes: Bytes },
    Many { position: u64, parts: Vec<(u8, Bytes)> },
}
```

`State.done: Option<Result<Resolved, ClientError>>`; `TicketCore::resolve(&self, r: Result<Resolved, ClientError>)`. Factor the three decode sites into one helper and add its fan-in twin:

```rust
fn decode_one<R: DeserializeOwned>(r: Result<Resolved, ClientError>) -> Result<R, ClientError> {
    match r? {
        Resolved::One { bytes, .. } => bincode::serde::decode_from_slice::<R, _>(&bytes, bincode::config::standard())
            .map(|(v, _)| v)
            .map_err(|e| ClientError::Decode(e.to_string())),
        Resolved::Many { .. } => Err(ClientError::Decode("fan-in completion on a single-response ticket".into())),
    }
}

fn decode_many<R: DeserializeOwned>(r: Result<Resolved, ClientError>) -> Result<Vec<(u8, R)>, ClientError> {
    match r? {
        Resolved::Many { parts, .. } => parts
            .into_iter()
            .map(|(id, bytes)| {
                bincode::serde::decode_from_slice::<R, _>(&bytes, bincode::config::standard())
                    .map(|(v, _)| (id, v))
                    .map_err(|e| ClientError::Decode(format!("fsm {id}: {e}")))
            })
            .collect(),
        Resolved::One { .. } => Err(ClientError::Decode("single response on a fan-in ticket".into())),
    }
}
```

`Ticket<R>::wait`/`wait_timeout`/`poll` call `decode_one(result)`. `FanInTicket<R>` is a copy of `Ticket<R>` (same fields, same `wait`/`wait_timeout`/`Future` bodies) calling `decode_many`; `fan_in_ticket_pair` mirrors `ticket_pair`. Re-export `FanInTicket` from `lib.rs`.

`error.rs`:

```rust
    /// M14b: `id` is not a declared FSM on the attached node — refused at the
    /// door (`SubmitError::ServiceNotDeclared`) or answered by the node
    /// (`MSG_V2_BAD_SERVICE`, when the node has no ring for it). No side effect.
    #[error("service id {id} is not declared on this node (declared set 0b{declared:b})")]
    ServiceNotDeclared { id: u8, declared: u64 },
```

`pipelined.rs`: `PipelinedClient` gains `declared: u64` (from `send.declared()` at connect) and `pub fn declared(&self) -> u64`. `dispatch` becomes generic over the ticket pair:

```rust
    fn dispatch_with<T>(
        &self,
        bytes: &[u8],
        retry: bool,
        pair: fn() -> (T, Arc<TicketCore>),
        submit_fn: impl Fn(&SendHalf, u64, &[u8]) -> Result<(), SubmitError>,
    ) -> Result<T, ClientError> {
        let (ticket, core) = pair();
        … the existing body, with one new arm …
                Err(SubmitError::ServiceNotDeclared { id, declared }) => {
                    reclaim(user_data);
                    return Err(ClientError::ServiceNotDeclared { id, declared });
                }
    }
    fn dispatch<R: DeserializeOwned>(&self, bytes: &[u8], retry: bool, submit_fn: impl Fn(&SendHalf, u64, &[u8]) -> Result<(), SubmitError>) -> Result<Ticket<R>, ClientError> {
        self.dispatch_with(bytes, retry, ticket_pair::<R>, submit_fn)
    }
```

New methods:

```rust
    /// M14b: submit; FSM `id` answers. Retries like `submit`.
    pub fn submit_to<C: Serialize, R: DeserializeOwned>(&self, id: u8, cmd: &C) -> Result<Ticket<R>, ClientError> {
        let bytes = encode(cmd)?;
        self.dispatch(&bytes, true, move |send, ud, b| send.try_submit_to(ud, id, b))
    }
    /// M14b: submit; EVERY declared FSM answers, collected in id order.
    pub fn submit_all<C: Serialize, R: DeserializeOwned>(&self, cmd: &C) -> Result<FanInTicket<R>, ClientError> {
        let bytes = encode(cmd)?;
        self.dispatch_with(&bytes, true, fan_in_ticket_pair::<R>, |send, ud, b| send.try_submit_all(ud, b))
    }
    pub fn query_linearizable_on<Q: Serialize, QR: DeserializeOwned>(&self, id: u8, q: &Q) -> Result<Ticket<QR>, ClientError> {
        let bytes = encode(q)?;
        self.dispatch(&bytes, true, move |send, ud, b| send.try_query_on(ud, id, b, Consistency::Linearizable))
    }
    pub fn query_snapshot_on<Q: Serialize, QR: DeserializeOwned>(&self, id: u8, q: &Q) -> Result<Ticket<QR>, ClientError> {
        let bytes = encode(q)?;
        self.dispatch(&bytes, true, move |send, ud, b| send.try_query_on(ud, id, b, Consistency::Snapshot))
    }
```

`spawn_driver` gains a `declared: u64` parameter (passed from `connect`) and its `resolve` closure maps:

```rust
                Outcome::Response(bytes) => Ok(Resolved::One { position: c.position.unwrap_or(0), bytes: Bytes::copy_from_slice(bytes) }),
                Outcome::Responses(parts) => Ok(Resolved::Many { position: c.position.unwrap_or(0), parts: parts.to_vec() }), // Bytes clone = a refcount bump
                Outcome::BadService { id } => Err(ClientError::ServiceNotDeclared { id, declared }),
                … the other four unchanged …
```

`client.rs` shim: four one-liners (`self.inner.submit_to(id, cmd)?.wait()`, `self.inner.submit_all(cmd)?.wait()`, `query_snapshot_on`, `query_linearizable_on`) and `declared()`.

Exhaustive-match sites: `edge.rs` `complete()` gains

```rust
        Outcome::Responses(_) | Outcome::BadService { .. } => {
            // Unreachable on the edge: it only ever issues FSM-0 requests
            // (protocol v1 has no service selector, spec §6.4). Answer as a
            // transient so a client that somehow sees it retries.
            write_retry_into(buf, &conn, seq, RETRY_SERVICE_UNAVAILABLE, RETRY_BACKOFF_US);
        }
```

(match the exact call shape the `Outcome::Retry` arm uses in that function) and `submit_or_query`'s `SubmitError` match gains `Err(SubmitError::ServiceNotDeclared { .. }) => { /* unreachable: the edge never names an id */ … same handling as PayloadTooLarge's arm … }`. `engine_load.rs`, `m12_gate.rs`, `m5_gate.rs`: fold `Outcome::Responses(_) | Outcome::BadService { .. }` into their `lost` arm (a bench never issues them).

- [ ] **Step 4: Run**

```bash
cargo build --workspace --all-targets
cargo test -p uc2_client
cargo test -p uc2_gateway
```

Expected: PASS (gateway suites unchanged in behaviour).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add -A
git commit -m "feat(client): submit_to/submit_all/query_*_on on PipelinedClient and Client; FanInTicket; ServiceNotDeclared; consumers' new Outcome arms"
```

---

### Task 6: End to end — two FSMs, four calls, one bad id (+ the counter example)

**Files:**
- Modify `uc2_node/tests/services.rs` (helpers exist: `APP`, `serialize`, `tempdir`, `config`, `wait_until`, `open_cnc`, `ids`, `Cmd`, `CountSm`, `start_service`)
- Modify `examples/counter/src/bin/counter-client.rs`

- [ ] **Step 1: Write the failing tests** (append to `services.rs`)

```rust
#[test]
fn submit_to_submit_all_and_query_on_route_by_id_end_to_end() {
    use uc2_client::{Client, ClientError, PipelinedClient, PipelinedConfig};
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 1], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc0 = start_service(dir.path(), 0);
    let svc1 = start_service(dir.path(), 1);
    let client = PipelinedClient::connect(dir.path(), APP, PipelinedConfig::default()).unwrap();
    assert_eq!(client.declared(), 0b11);
    let t1: u64 = client.submit_to::<Cmd, u64>(1, &Cmd::Add(5)).unwrap().wait().unwrap();
    assert_eq!(t1, 5, "FSM 1 answered its own total");
    let all = client.submit_all::<Cmd, u64>(&Cmd::Add(1)).unwrap().wait().unwrap();
    assert_eq!(all, vec![(0, 6), (1, 6)], "same log, same SM ⇒ identical totals, ordered by id");
    assert_eq!(client.query_snapshot_on::<(), u64>(1, &()).unwrap().wait().unwrap(), 6);
    assert_eq!(client.query_linearizable_on::<(), u64>(1, &()).unwrap().wait().unwrap(), 6);
    assert_eq!(client.query_linearizable_on::<(), u64>(0, &()).unwrap().wait().unwrap(), 6);
    assert!(matches!(client.submit_to::<Cmd, u64>(2, &Cmd::Add(1)), Err(ClientError::ServiceNotDeclared { id: 2, declared: 0b11 })));
    assert!(matches!(client.query_snapshot_on::<(), u64>(7, &()), Err(ClientError::ServiceNotDeclared { id: 7, declared: 0b11 })));
    // The default `submit` still means FSM 0 — and FSM 1's answer to it is
    // dropped as a wrong-ring record, counted.
    let d: u64 = client.submit::<Cmd, u64>(&Cmd::Add(1)).unwrap().wait().unwrap();
    assert_eq!(d, 7);
    wait_until("FSM 1's answer to the default submit was dropped", || client.stats().wrong_ring >= 1);
    client.shutdown();
    // The blocking shim mirrors all four.
    let c = Client::connect(dir.path(), APP).unwrap();
    assert_eq!(c.declared(), 0b11);
    assert_eq!(c.submit_to::<Cmd, u64>(1, &Cmd::Add(1)).unwrap(), 8);
    assert_eq!(c.submit_all::<Cmd, u64>(&Cmd::Add(1)).unwrap(), vec![(0, 9), (1, 9)]);
    assert_eq!(c.query_snapshot_on::<(), u64>(1, &()).unwrap(), 9);
    assert_eq!(c.query_linearizable_on::<(), u64>(1, &()).unwrap(), 9);
    c.shutdown();
    svc0.stop();
    svc1.stop();
    node.stop();
}

/// A raw query record naming an id the node has no ring for is answered
/// MSG_V2_BAD_SERVICE on the node broadcast (the SDK refuses such ids
/// locally, so this drives the ring directly).
#[test]
fn a_raw_query_for_an_id_without_a_ring_gets_bad_service_from_the_node() {
    use uc_protocol::ring::{BroadcastRing, MpscRing};
    use uc_protocol::v2::ipc::{MSG_V2_BAD_SERVICE, MSG_V2_QUERY, client_from_extra, extra_client, write_query_payload};
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), ids(&[0, 1], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let mut node_egress = BroadcastRing::open(&dir.path().join("egress_node.broadcast")).unwrap().subscribe();
    let (mut producer, _c) = MpscRing::open(&dir.path().join("query.ring")).unwrap().into_split();
    let mut payload = Vec::new();
    write_query_payload(5, b"q", &mut payload);
    producer.try_write(MSG_V2_QUERY, 0, extra_client(0x77, 1), &payload).unwrap();
    let mut buf = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(Instant::now() < deadline, "no BAD_SERVICE within 10 s");
        match node_egress.try_read(&mut buf) {
            Ok(Some(rec)) if client_from_extra(rec.header_extra) == (0x77, 1) => {
                assert_eq!(rec.msg_type, MSG_V2_BAD_SERVICE);
                assert_eq!(buf, [5]);
                break;
            }
            Ok(_) => std::thread::sleep(Duration::from_millis(1)),
            Err(e) => panic!("egress_node read: {e}"),
        }
    }
    node.stop();
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc2_node --test services submit_to_submit_all` — before Task 5 it would not compile; on the Task 5 tree it should pass — so this test's RED is "does not compile on `main` before this plan" (record that); the raw-`BAD_SERVICE` test fails on the pre-Task-2 tree with a 10 s timeout. Run both on the current tree: expected PASS. If either fails here, that is a real integration defect — fix it in this task (the earlier tasks' unit tests passed; the seam is what this task proves).

- [ ] **Step 3: The example**

`examples/counter/src/bin/counter-client.rs`: add `#[arg(long)] service_id: Option<u8>` ("Ask FSM <id> instead of FSM 0") and `#[arg(long)] all: bool` ("Submit to every declared FSM and print each answer"); route: `all` ⇒ `client.submit_all::<_, _>(&cmd)` printing one line per `(id, resp)`; `service_id: Some(id)` ⇒ `submit_to(id, …)` / `query_*_on(id, …)`; else today's calls. Keep the existing output format for the default path byte-identical (the counter `tests/quickstart_local.rs` greps it — read that test before editing).

- [ ] **Step 4: Run**

```bash
cargo test -p uc2_node --test services
cargo test -p counter
cargo test -p uc2_node --test query_barrier --test lin_v2 --test backup
```

Expected: PASS (`services.rs` 11 → 13 tests).

- [ ] **Step 5: Clippy + commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings
git add -A
git commit -m "test(m14b): two-FSM end-to-end routing (submit_to/submit_all/query_on, local refusal, BAD_SERVICE); counter-client --service-id/--all"
```

---

### Task 7: The sim — one report choke point, a per-node apply ceiling, inv10, the capped-quorum scenarios

Deviation 7 in full. Today `world.rs` builds `Msg::Report` inline at four sites (`on_archive`'s `reportable` match, the Mechanism floor re-send, the C-1 leak arm, `reopen_gate`); a fifth, `inject_report`, forges a wire value and stays untouched. This task funnels the four through `send_report`, which clamps to the node's `apply_ceiling` and runs inv10.

**Files:**
- Modify `uc2_sim/src/world.rs` (`Node` 524–605 + its literal in `World::new` ~772–799; the four send sites 1358, 1383, 1404, 1616–1636; `reopen_gate`'s two callers ~1546 and ~1946; the reset points: `Action::Truncate` ~2196, `on_truncated_feedback`'s cut ~1532, `do_crash` ~2331, `on_restart` ~1437, `Action::BecomeFollower` ~2123, `Action::BecomeLeader` ~2069, `HaltRemoved` ~2313; new hooks in the scripting block ~2352+)
- Modify `uc2_sim/src/invariants.rs` (module doc 1–61; struct 136–168; `new`/`on_restart` 170–193; new methods; unit tests)
- Modify `uc2_sim/tests/scenarios.rs` (`fuzz_default_seeds` 473–507; two new scenarios)
- Modify `docs/VERIFICATION.md` (§2 at 172–210: "nine" → "ten", the table; §11: drop "the sim scenario … is M14b")

**Interfaces:**
```rust
// uc2_sim::world::World
pub fn set_apply_ceiling(&mut self, node: usize, ceiling: Option<u64>);   // scripting hook; a change resets inv10's monotonicity baseline
pub fn last_report(&self, node: usize) -> u64;                             // what the node last sent (0 after a reset)
// crate-private
fn send_report(&mut self, node: usize, leader: usize, unclamped: u64, now: u64, step: u64) -> Result<(), InvariantViolation>;
// uc2_sim::invariants::InvariantChecker
pub fn on_report(&mut self, node: usize, clamped: u64, unclamped: u64, ceiling: Option<u64>, step: u64) -> Result<(), InvariantViolation>;
pub fn on_report_reset(&mut self, node: usize);
pub fn last_report(&self, node: usize) -> u64;
```

- [ ] **Step 1: Write the failing unit tests** (`invariants.rs` test module)

```rust
    #[test]
    fn inv10_catches_a_report_above_its_unclamped_value() {
        let mut c = checker(vec![(1, 0)], 0);
        let err = c.on_report(1, 200, 100, None, 7).unwrap_err();
        assert!(err.invariant.contains("inv10"), "{err}");
        assert!(err.detail.contains("above its unclamped"), "{err}");
    }

    #[test]
    fn inv10_catches_a_report_above_its_apply_ceiling() {
        let mut c = checker(vec![(1, 0)], 0);
        let err = c.on_report(1, 150, 200, Some(100), 7).unwrap_err();
        assert!(err.invariant.contains("inv10"), "{err}");
        assert!(err.detail.contains("above its apply ceiling 100"), "{err}");
    }

    #[test]
    fn inv10_catches_a_decrease_without_a_reset_and_tolerates_one_after() {
        let mut c = checker(vec![(1, 0)], 0);
        c.on_report(2, 96, 96, None, 1).unwrap();
        c.on_report(2, 96, 192, Some(96), 2).unwrap();   // equal (a floor re-send) is fine
        c.on_report(2, 192, 192, None, 3).unwrap();
        let err = c.on_report(2, 96, 96, None, 4).unwrap_err();
        assert!(err.invariant.contains("inv10"), "{err}");
        assert!(err.detail.contains("decreased"), "{err}");
        c.on_report_reset(2);                              // truncation / restart / role change
        c.on_report(2, 96, 96, None, 5).unwrap();
        assert_eq!(c.last_report(2), 96);
        c.on_restart(2);                                   // restart resets it too
        assert_eq!(c.last_report(2), 0);
    }
```

And the two scenarios in `scenarios.rs` (after `minority_partition_cannot_commit_and_heals`):

```rust
/// M14b (spec §12): a per-node apply ceiling models "the slowest FSM's
/// applied position + fsm_lag" — what M14a's report ceiling clamps a real
/// node's AppendPosition to. With BOTH followers capped, the leader alone is
/// not a quorum, so commit freezes at the cap while the leader's durable runs
/// past it; releasing ONE follower restores a quorum and commit resumes.
/// Every existing invariant runs throughout (`run*` returning `Ok`).
#[test]
fn capped_quorum_stalls_commit_and_releasing_one_follower_resumes_it() {
    const FRAME: u64 = 96;
    let mut w = World::new(base_cfg(11));
    w.run_until_leader().expect("invariants");
    let leader = w.current_leader().unwrap();
    let followers = w.majority_excluding(leader);
    let cap = w.max_commit() + 2 * FRAME;
    for &f in &followers {
        w.set_apply_ceiling(f, Some(cap));
    }
    w.append_and_replicate(40 * FRAME);
    w.run_steps(8_000).expect("invariants");
    assert!(w.leader_durable() > cap + 10 * FRAME, "vacuity: the leader must hold durable bytes well past the cap (durable {})", w.leader_durable());
    assert!(w.max_commit() <= cap, "commit {} ran past the capped quorum's ceiling {cap}", w.max_commit());
    for &f in &followers {
        assert!(w.last_report(f) <= cap, "follower {f} reported {} > cap {cap}", w.last_report(f));
    }
    w.set_apply_ceiling(followers[0], None);
    assert!(
        w.run_until(|w| w.max_commit() > cap).expect("invariants"),
        "commit must resume once a quorum is uncapped (timed out)"
    );
    assert!(w.last_report(followers[1]) <= cap, "the still-capped follower stays capped");
}

/// M14b: a capped MINORITY never stalls the cluster — the leader plus the
/// uncapped follower are a quorum (spec §5.3: a lagging minority falls to
/// journal replay, the cluster does not wait for it).
#[test]
fn a_capped_minority_does_not_stall_commit() {
    const FRAME: u64 = 96;
    let mut w = World::new(base_cfg(12));
    w.run_until_leader().expect("invariants");
    let leader = w.current_leader().unwrap();
    let f = w.majority_excluding(leader)[0];
    let cap = w.max_commit() + FRAME;
    w.set_apply_ceiling(f, Some(cap));
    w.append_and_replicate(20 * FRAME);
    assert!(
        w.run_until(|w| w.max_commit() > cap + 10 * FRAME).expect("invariants"),
        "one capped follower must not stall a 3-node cluster (timed out)"
    );
    assert!(w.last_report(f) <= cap, "the capped follower never reports past its ceiling");
}
```

Add a third loop to `fuzz_default_seeds`:

```rust
    // M14b: the same seeds with one node's report capped from the first
    // leader on — a capped MINORITY under drops/dups/crashes. Every invariant
    // (inv10 included) must hold; liveness is not asserted here (the capped
    // node may be the leader, which never reports).
    for seed in 0..50u64 {
        let mut w = World::new(SimConfig {
            n_nodes: 3,
            seed,
            max_steps: 20_000,
            drop_per_million: 20_000,
            dup_per_million: 5_000,
            crash_per_million: 500,
            ..SimConfig::default()
        });
        if let Err(v) = w.run_until_leader() {
            panic!("seed {seed} (capped): {v}");
        }
        let capped = (seed % 3) as usize;
        w.set_apply_ceiling(capped, Some(w.max_commit() + 96));
        if let Err(v) = w.run() {
            panic!("seed {seed} (capped node {capped}): {v}");
        }
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p uc2_sim` — compile errors (`on_report`, `set_apply_ceiling`, `last_report`).

- [ ] **Step 3: Implement — the checker**

`InvariantChecker` gains `last_report: Vec<u64>` (init `vec![0; n]` in `new`; `on_restart` also sets `self.last_report[node] = 0`). Module doc: add "inv10 — report ceiling (M14b): a node's outgoing durable report never exceeds the value it would have reported unclamped, never exceeds its apply ceiling when one is set, and never decreases between reset events (truncation, crash/restart, a role change, a changed ceiling). `inject_report` — a forged wire value by construction — bypasses it."

```rust
    /// inv10 (M14b): the report ceiling. `clamped` is what the node sends,
    /// `unclamped` what it would have sent without a ceiling, `ceiling` the
    /// cap in force (`None` = uncapped).
    pub fn on_report(
        &mut self,
        node: usize,
        clamped: u64,
        unclamped: u64,
        ceiling: Option<u64>,
        step: u64,
    ) -> Result<(), InvariantViolation> {
        if clamped > unclamped {
            return Err(self.viol(
                "report ceiling (inv10)",
                step,
                format!("node {node} reported {clamped} above its unclamped value {unclamped}"),
            ));
        }
        if let Some(c) = ceiling
            && clamped > c
        {
            return Err(self.viol(
                "report ceiling (inv10)",
                step,
                format!("node {node} reported {clamped} above its apply ceiling {c}"),
            ));
        }
        let last = self.last_report[node];
        if clamped < last {
            return Err(self.viol(
                "report ceiling (inv10)",
                step,
                format!(
                    "node {node} report decreased {last} -> {clamped} with no truncation, restart, \
                     role change or ceiling change in between"
                ),
            ));
        }
        self.last_report[node] = clamped;
        Ok(())
    }

    /// A legitimate discontinuity in a node's reports: truncation (durable
    /// went down), crash/restart, a role change (`matched` resets), or a
    /// changed apply ceiling. Restarts also come through `on_restart`.
    pub fn on_report_reset(&mut self, node: usize) {
        self.last_report[node] = 0;
    }

    pub fn last_report(&self, node: usize) -> u64 {
        self.last_report[node]
    }
```

- [ ] **Step 4: Implement — the world**

`Node` gains `/// M14b: the slowest local FSM's reach — outgoing reports are clamped to it. apply_ceiling: Option<u64>` (`None` in the `World::new` literal).

The choke point, placed next to `reopen_gate`:

```rust
    /// M14b: the ONE place a node's durable report leaves it. Clamps to the
    /// node's apply ceiling (spec §5.3's `min(validated, min_applied + lag)`
    /// with the ceiling standing in for `min_applied + lag`) and runs inv10.
    /// `inject_report` deliberately bypasses this — it forges a wire value.
    fn send_report(
        &mut self,
        node: usize,
        leader: usize,
        unclamped: u64,
        now: u64,
        step: u64,
    ) -> Result<(), InvariantViolation> {
        let ceiling = self.nodes[node].apply_ceiling;
        let durable = ceiling.map_or(unclamped, |c| unclamped.min(c));
        self.checker.on_report(node, durable, unclamped, ceiling, step)?;
        let (id, term) = (self.nodes[node].id, self.nodes[node].sm.current_term());
        let durable_term = self.nodes[node].sm.term_at(durable);
        self.send(node, leader, Msg::Report { from: id, term, durable, durable_term }, now);
        Ok(())
    }
```

Rewrite the four sites to `self.send_report(node, leader, <the durable they computed>, now, step)?;` — in `on_archive`'s `reportable` arm the value is the `reportable` `Some(durable)`; the floor and leak arms use `self.nodes[node].durable`; `reopen_gate(&mut self, node, now)` becomes `reopen_gate(&mut self, node: usize, now: u64, step: u64) -> Result<(), InvariantViolation>` and its two callers propagate with `?` (both already return `Result` and have `step` in scope — `on_truncated_feedback` and the `Msg::TermMap` arm of `deliver`).

Reset points — add `self.checker.on_report_reset(node);` at: the `Action::Truncate` arm right after `self.checker.on_truncate(…)?`; `on_truncated_feedback` right after the deferred `nd.durable = t; nd.append = t;` cut; `do_crash`; `Action::BecomeFollower` beside `matched = 0`; `Action::BecomeLeader` beside the append collapse; the `HaltRemoved` arm beside its `matched = 0`. (`on_restart` is covered by `checker.on_restart`.)

Scripting hooks, in the block with `partition_node`:

```rust
    /// M14b: cap node's outgoing durable reports at `ceiling` (`None` lifts it).
    /// Models the slowest local FSM's applied position plus `fsm_lag`. A
    /// change is a legitimate report discontinuity for inv10.
    pub fn set_apply_ceiling(&mut self, node: usize, ceiling: Option<u64>) {
        self.nodes[node].apply_ceiling = ceiling;
        self.checker.on_report_reset(node);
    }

    /// M14b: the durable position node last reported (0 after a reset).
    pub fn last_report(&self, node: usize) -> u64 {
        self.checker.last_report(node)
    }
```

- [ ] **Step 5: Run**

```bash
cargo test -p uc2_sim
cargo test -p uc2_sim --release --features sim-heavy fuzz_heavy   # the 1000-seed tiers, ~minutes
```

Expected: PASS — every existing scenario and pin unchanged (the red pins still name `phantom`/`inv5`, not inv10), 3 new unit tests, 2 new scenarios, the third fuzz loop. **If inv10 fires on an existing seed with a "decreased" detail, that is a legitimate decrease path this plan's reset list missed — find the state change that lowered the Gated/raw report (a `matched` or `durable` write), add `on_report_reset` at that exact site with a comment naming the event, and record it in the report. Never widen inv10 to allow decreases in general.**

- [ ] **Step 6: Discrimination check**

Temporarily make `send_report` ignore the ceiling (`let durable = unclamped;`); run `cargo test -p uc2_sim capped_quorum` — expected FAIL on `commit … ran past the capped quorum's ceiling`; revert. Record the observed commit value.

- [ ] **Step 7: `VERIFICATION.md`**

§2: "nine whole-cluster safety invariants" → "ten"; add the row `| inv10 | Report ceiling — a clamped report never exceeds its unclamped value or its apply ceiling, and never decreases except across a truncation, restart, role change or ceiling change (M14b) |`; add one sentence after the directed-scenarios paragraph: "`capped_quorum_stalls_commit_and_releasing_one_follower_resumes_it` models M14a's report ceiling as a per-node apply cap and pins that commit stalls iff a commit quorum is capped." §11: delete the sentence ending "…is M14b" and replace with "M14b's sim scenario covers the ceiling's liveness property; the real node's ceiling is exercised by `uc2_node/tests/services.rs` on a 3-node in-process cluster."

- [ ] **Step 8: Clippy + commit**

```bash
cargo clippy -p uc2_sim --all-targets -- -D warnings
git add uc2_sim docs/VERIFICATION.md
git commit -m "feat(sim): report choke point + per-node apply ceiling; inv10 (report ceiling); capped-quorum stall/release scenarios (discrimination: commit reached <N> uncapped vs cap <C>)"
```

---

### Task 8: Documentation

**Files:**
- Modify `docs/reference/instance-directory.md` (rows 19 and 22 of the Files table)
- Modify `docs/reference/read-path.md` (the sentence at 22–24; a `MSG_V2_BAD_SERVICE` row in "Diagnostic signatures")
- Modify `docs/reference/semver-policy.md` (the promised-surface bullets naming `uc2_client` items, ~46–62)
- Modify `docs/QUICKSTART.md` (the "Client SDKs" bullet, ~453–457), `README.md` (the `uc2_client` row, line 178)
- Check `docs/reference/cnc-page.md` "Counters and status" for the 4032/4040 rows (M14a's Task 10 added them; if absent, add them as that plan specified)

- [ ] **Step 1: `instance-directory.md`**

Row 19 (`query.ring`): "Query submissions, both linearizable and snapshot reads. Payload is `service_id: u8` — which FSM answers (M14) — followed by the query bytes; same record framing as `ingress.ring`." Row 22 (`egress_node.broadcast`): "Node-originated answers to clients: `MSG_V2_NOT_LEADER` (with the leader hint), `MSG_V2_RETRY`, and `MSG_V2_BAD_SERVICE` (the query named an id this node has no ring for). Submit and query *responses* come from the FSMs' own rings." Row 21 (`egress_service.<id>.broadcast`): append "A client opens every declared id's ring and accepts a response only from the FSM(s) its request named."

- [ ] **Step 2: `read-path.md`**

Lines 22–24: "…gated on **the named FSM's** service catching up to the read position (the query's `service_id` selects the slot; M14), on a follower header-term check, on a capture-recheck, and on a service-epoch backstop…". After the round paragraph (16–20): "One probe round certifies every parked read regardless of which FSM it names — the round certifies a commit position, which is service-agnostic." Diagnostic-signatures table: `| `MSG_V2_BAD_SERVICE` answered on `egress_node` | the client named an FSM id this node has no ring for (undeclared, ≥ 8, or a non-zero id on a harness node); the SDK refuses such ids locally, so this means a raw ring writer or a client attached to a differently-declared node |`.

- [ ] **Step 3: `semver-policy.md`**

Add to the `uc2_client` promised-surface bullets: `FanInTicket`; `Outcome::{Responses, BadService}`; `SubmitError::ServiceNotDeclared`; `ClientError::ServiceNotDeclared`; `SendHalf::{declared, try_submit_to, try_submit_all, try_query_on}`; `PipelinedClient::{declared, submit_to, submit_all, query_snapshot_on, query_linearizable_on}`; `Client::{declared, submit_to, submit_all, query_snapshot_on, query_linearizable_on}` — all additive (minor); `uc2_client` now depends on `bytes` (`Outcome::Responses` and `FanInTicket` expose `bytes::Bytes`, the same type the SM contract uses). Note the one behavioural change: `Outcome` and `SubmitError` gained variants, so exhaustive matches downstream break at compile time (a documented minor-version hazard of the three-tier promise — state it).

- [ ] **Step 4: `QUICKSTART.md` + `README.md`**

QUICKSTART's Client-SDKs bullet: add "M14: `submit_to(id, …)`, `submit_all(…)` (every FSM's answer, in id order) and `query_*_on(id, …)` pick which state machine answers; the plain calls mean FSM 0." README's `uc2_client` row: "Submit, linearizable/snapshot queries — to FSM 0 by default, to any declared FSM by id, or fanned in across all of them (M14)."

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs(m14b): per-FSM query routing, BAD_SERVICE, the fan-in client API, semver rows"
```

---

### Task 9: The local proof stack (smoke, not a gate)

Runs things and records what it saw; no code. Use the warm private dir. Every command in the foreground with a `timeout`.

- [ ] **Step 1**
```bash
export CARGO_TARGET_DIR=/home/claude/cargo-target-uc2-m14a
cargo clippy --workspace --all-targets -- -D warnings
timeout 2400 cargo test --workspace --no-fail-fast 2>&1 | tail -40
```
Expected: clippy clean; every binary `0 failed`.

- [ ] **Step 2**
```bash
timeout 900 cargo test -p uc2_node --test lin_v2 2>&1 | tail -12
timeout 900 cargo test -p uc2_node --test lin_partition_v2 2>&1 | tail -12
timeout 900 cargo test -p uc2-crashtest --features hard-crash-tests 2>&1 | tail -12
timeout 1800 cargo test -p uc2_sim --release --features sim-heavy 2>&1 | tail -8
```
Expected: every capstone `Linearizable`; the heavy sim tier green.

- [ ] **Step 3**
```bash
scripts/fuzz_smoke.sh 60 --min-runs 10000 ring_mpsc_record
git clean -fdq fuzz/corpus; git checkout -- fuzz/Cargo.lock 2>/dev/null || true
```
Expected: PASS with ≥ 10 000 runs; tree clean afterwards.

- [ ] **Step 4**
```bash
UC2_M5_MAX_SECS=6 cargo run -p uc2_node --release --example m5_gate -- all --secs 6 --root /home/claude/m14b-smoke; rm -rf /home/claude/m14b-smoke
```
Record responses/s and p50 **as smoke** (the M14a same-box figure was 158–184 k resp/s; a same-box spread of ±18 % is documented noise — never compare to a bar).

- [ ] **Step 5**
```bash
git commit --allow-empty -m "test(m14b): local proof stack — workspace suite, lin capstones, hard-crash, sim-heavy, fuzz smoke, m5 smoke (numbers are smoke, not a gate)"
```

---

## Self-review

**Spec coverage**

| Spec clause | Where |
|---|---|
| §5.4 `query.ring` payload `service_id ++ query`; `drain_query_ring` forwards to `svc_query.<id>.ring`; undeclared id → `MSG_V2_BAD_SERVICE` on `egress_node` keyed by the client pair | Task 1 (codec + constant), Task 2 (node) — gate = ring presence (deviation 4) |
| §5.4 `advance_pending_reads` carries `service_id`, checks `slot[id]` | M14a already did the bracket; Task 2 populates the id from the record (test asserts it) |
| §5.4 the quorum round unchanged | Global constraint; Task 2 touches neither `read_round.rs` nor the round logic |
| §5.4 `can_serve`/`readyz` unchanged | untouched |
| §6.1 `Engine::attach` reads `services_declared`, opens every declared ring; `poll` round-robins N + `egress_node`; `client_id` node-wide | Task 4 |
| §6.2 `Slot.{expected, received}`; fan-in buffer on the `PollHalf`; the per-call table (`try_submit` = {0}, `try_submit_to` = {id}, `try_submit_all` = declared, queries likewise); drop outside `expected`; completion at `received == expected`; undeclared id fails locally | Tasks 3, 4, 5 (`Vec<u8>` not `Bytes` — deviation 1; `FanInTicket` — deviation 2) |
| §6.3 record formats | Task 1 (`MSG_V2_QUERY`, `MSG_V2_BAD_SERVICE`), unchanged elsewhere |
| §6.4 remote path = FSM 0; edge unchanged beyond attaching | Task 5 (only the exhaustive-match arms; no selector) |
| §12 unit: client slot-table mask completion (drop-outside-mask, fan-in ordering) | Task 3 (slots) + Task 4 (engine_synthetic: id order vs arrival order) |
| §12 `uc2_sim`: new invariant (report ≤ validated, monotone except truncation) + apply-ceiling scenario (stalls iff a quorum is capped) | Task 7 (deviation 7 scopes the invariant to the ceiling path; both scenarios + a fuzz loop) |
| §12 fuzz: `ring_mpsc_record`'s new decode step | Task 1 |
| §8 "Client attached before all FSMs": requests to an unattached FSM wait (client timeout) | unchanged behaviour; the id's ring exists, the slot's epoch is 0 ⇒ the read parks until the deadline, submits wait for the response — no plan change needed |

Not covered here, by design: §7.3, §9, the §12 capstones/elle/crashtest, the fleet gate, the release writeup (M14c/M14d).

**Placeholder scan**: grepped for `TBD`, `TODO`, `similar to`, `add error handling`, `fill in`. None. Two values are left to the run on purpose: `<N>`/`<C>` in Task 7's commit message (the discrimination observation) and the m5 smoke numbers in Task 9.

**Type consistency**: `expected: u8` flows `SendHalf::expect_one → send → SlotTable::claim`; `ring: Option<u8>` flows `drain_ring(ring_id) → handle_record → SlotTable::resolve`; `Resolve::Won { user_data: u64, fan_in: bool }` is matched with both fields at every site; `FanIn.parts: Vec<(u8, Bytes)>` matches `Outcome::Responses(&[(u8, Bytes)])`, `Resolved::Many { parts: Vec<(u8, Bytes)> }`, and `FanInTicket<R>::wait() -> Vec<(u8, R)>`; `ServiceNotDeclared { id: u8, declared: u64 }` has the same field names and types on `SubmitError` and `ClientError`; `write_query_payload(u8, &[u8], &mut Vec<u8>)` / `split_query_payload(&[u8]) -> Option<(u8, &[u8])>` are called with those shapes in Tasks 2, 4 and the tests; `World::set_apply_ceiling(usize, Option<u64>)` / `last_report(usize) -> u64` / `InvariantChecker::on_report(usize, u64, u64, Option<u64>, u64)` agree between Task 7's code and tests.

**Three facts worth re-checking during execution:**
1. `SlotTable::resolve` mutates `received` with `fetch_or` *after* the owner/generation check and before the completing CAS; the argument that this is race-free rests on every owner→FREE transition except `release` happening on the poll thread (`maintenance`/`drain_abort` run inside `poll`). If a second poll thread is ever introduced, `received` needs the same generation discipline as `owner`.
2. `PollHalf::poll` destructures `self` to borrow `egress_services`, `buf` and `fanin` mutably at once; do not reintroduce a `self.` access inside the loop.
3. inv10's reset list is exactly the set of state changes that lower a node's unclamped report today; a future sim change that lowers `matched` or `durable` elsewhere must add a reset at that site, or inv10 will fire with a "decreased" detail on an honest trace.

## Execution record (2026-08-28, subagent-driven; the SDD ledger, condensed)

Branch `worktree-uc2-multi-service`, merge base `3a7f9a5`, final HEAD `efc5339`. Task commits: T1 `5feae7c`, T2 `d900791`, T3 `5d74ba2` + `e11aaae` (1 fix round, doc-only), T4 `40a6d54` (Ruling B folded in), T5 `0f87c1f`, T6 `4823bd0` + `52b5102` (1 fix round), T7 `cbbe7bd`, T8 `eef2b91`, T9 `ad670cf` (empty) + `b9674bb` (crashtest fix); final whole-branch review (0 Critical, 2 Important) → fix wave `efc5339`. Evidence on `efc5339`: `cargo test --workspace --no-fail-fast` 1 378 passed / 1 failed — the one failure is `failover::contested_first_election_first_block_truncation_recovers` ("no clean base-0 construction in 24 tries", a ~50/50 construction race starved by the parallel full run; 7/7 on an isolated re-run — Ruling I below); clippy `--workspace --all-targets -D warnings` clean. On `ad670cf`/`b9674bb` (T9): workspace suite 0 failed, `lin_v2` 7/7 + `lin_partition_v2` 7/7, `uc2-crashtest --features hard-crash-tests` 4/4 (after `b9674bb`), sim-heavy 38 passed, fuzz smoke `ring_mpsc_record` 40.8 M runs clean, m5 smoke 165 498 resp/s p50 23.9 ms (smoke, dev box; M14a same-box 158–184 k). T7's discrimination: with the ceiling ignored, commit reached 18 912 against cap 288.

### Rulings made during execution (each with what it costs if wrong)

- **A (pre-flight, T3/T4/T5):** fan-in is a claim-time flag — `SlotTable::claim(.., expected, fan_in)` stores it and `Resolve::Won { fan_in }` returns it — not `expected.count_ones() > 1` as this plan wrote. On a node declaring only FSM 0 (the default; id 0 is mandatory) `try_submit_all` has `expected = 0b1`, so the plan's derivation would have completed it as `Outcome::Response` and `FanInTicket::wait` would have returned `Decode`. Cost if wrong: one bool per slot, a 5-arg `claim`.
- **B (pre-flight, T4):** the four downstream exhaustive-match sites this plan put in T5 moved into T4 so `cargo clippy --workspace` is clean at every commit (Global Constraints bind over task text). Cost: commit attribution.
- **C (T6):** the plan's `wait_until(.., || stats().wrong_ring >= 1)` after the default `submit` was already true from the first `submit_to(1, ..)` (every FSM answers every frame; the `expected` mask is client-local), and the literal delta `wrong_ring > before` deterministically times out — `poll` drains ring 0 first, FSM 0's answer frees the slot, and FSM 1's late piece is `Resolve::Miss` (`duplicates`), not `WrongRing` (the ring check sits after the owner check). The test asserts the delta of `wrong_ring + duplicates`. Consequence for the docs: a sibling's answer to a single-FSM request is "dropped and counted", not "counted as `wrong_ring`". Cost: none.
- **D (T9):** the crashtest fix's scoped re-review was folded into the final whole-branch review. Cost if wrong: a mis-grouped harness arm.
- **E (final review Important 1):** the fan-in buffer resets on a generation's FIRST piece (`resolve` reports `first = received-before == 0`), and the three ring-less terminal arms clear the parts — this plan's seq-keyed `push_piece` could deliver a stale sibling piece after a u32 wire-seq wrap. Cost: one bool through `Resolve`.
- **F (parked):** the attach-level refusal for a page whose declared set has no id < 8 reuses `ClientError::ServiceNotDeclared { id: 0, declared: <raw page word> }` (mirrors `uc2_service`'s page gate; avoids a public-API variant). Cost if wrong: an attach refusal indistinguishable from a door refusal by pattern match — revisit when `ClientError` next changes.
- **G (parked):** no engine-level test for a fan-in abandoned by the deadline sweep (the case only `first` protects; the RETRY-aborted test is also covered by the terminal-arm clear); `slots.rs` pins `first`'s semantics. M14c with the Partial-then-sweep slots test.
- **H (parked):** `scratch_base()` in the engine unit tests assumes the `<target>/<profile>/deps` layout. Harmless here.
- **I (finish):** the one full-suite failure is an environment-sensitive construction flake (see above), not a regression — the test is election/first-block truncation, untouched by M14b (`uc2_consensus`/`uc2_net`/the replication path are not in the diff), green in T9's full run. Cost if wrong: a masked election regression.

### Plan defects found by execution

- Deviation 7's `Vec<u8>` note is stale (the header says `Bytes`); Ruling A above; T4/T5's compile break between commits (Ruling B); T6's `wrong_ring` assertion (Ruling C); T4's seq-keyed fan-in buffer (Ruling E); the claim that `examples/counter/tests/quickstart_local.rs` greps `counter-client` (it drives `counter-remote` — no test guards `counter-client`'s default output; verified byte-identical by diff); T5's `env!("CARGO_TARGET_TMPDIR")` in a lib unit test (cargo defines it for integration tests/benches only); `cargo build --workspace --all-targets` does not compile feature-gated tests — `uc2-crashtest --features hard-crash-tests` broke on the new `ClientError` variant and only T9 caught it.

### Deferred to M14c (triaged by the final review as CAN WAIT)

- Hot path: `resolve`'s `received.fetch_or` runs on every single-ring `RESPONSE` too; guard with `expected != bit` and A/B the exact binaries before adopting (M14a's lesson).
- `Partial` pieces increment no stat (`responses` counts completions); `send()` has 9 params under `allow(too_many_arguments)`; no back-to-back `try_query_on` scratch-reuse test; `decode_many` on an empty `Many` yields `Ok(vec![])` (unreachable — `declared` is never 0); `FanInTicket`'s `wait_timeout`/`Future`/drop-then-resolve untested (one-line wrappers); `#[allow(dead_code)]` on `Resolved::*::position`; `pipelined.rs` lib test on `tempfile::tempdir()` (= `/tmp`, follows two pre-existing fixtures).
- `edge.rs`'s unreachable `ServiceNotDeclared` arm answers `RETRY_PAYLOAD_TOO_LARGE`; `forward_svc_query`'s dead branch could `debug_assert`; `counter-client --all` + `--service-id` asymmetry (`conflicts_with`); the T6 test comment overstates the `duplicates` determinism.
- Sim: `on_report_reset` zeroes the baseline (could lower to the new floor); `set_apply_ceiling` resets on a raise; the scenario's `last_report(f) <= cap` reads a resettable baseline (inv10 is the real guard); the heavy tiers carry no capped arm; a mid-frame cap is accepted; VERIFICATION §11 repeats "3-node in-process cluster".
- Docs: `instance-directory.md`'s `egress_service.<id>.broadcast` row still says owner "node → service" (pre-existing); three M14b-visible behaviours live in rustdoc only — a query at exactly `max_payload` fails `PayloadTooLarge { len: n+1 }` (deviation 6), a completion landing only on ring k ≠ 0 resolves at the ≤ 1 ms park ceiling under `Park` (deviation 3), and `submit_all` with one unattached FSM times out every fan-in — carry into the M14d release writeup; the semver bullet omits `submit_all`'s two return shapes.
