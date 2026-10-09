# Read-your-writes reads — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a third read mode, `ReadYourWrites`, that any node (leader, follower, learner) answers once its service has applied up to a client-carried position token, giving read-your-writes and monotonic reads at near snapshot-read cost.

**Architecture:** The client keeps a token (the highest exclusive applied position it has seen; a write's frame-start position counts as `+1`). A query with a non-zero token carries it in an 8-byte prefix flagged `FLAG_V2_MIN_POSITION`. The node refuses a token above its own `durable` position at once, forwards a caught-up read immediately with the service's real epoch, and otherwise parks it in a per-row min-heap until `applied ≥ token` or a 1 s deadline. Query answers carry the service's applied frontier, which the client both checks (guard) and folds into its token. The remote protocol carries the same prefix; the gateway relays each remote query's own token.

**Tech Stack:** Rust (MSRV 1.89, pinned 1.96), the UC workspace: `uc_protocol`, `uc_log`, `uc_service`, `uc_node`, `uc_client`, `uc_remote`, `uc_gateway`, `uc_lincheck`.

**Spec:** `docs/superpowers/specs/2026-10-08-uc2-read-your-writes-design.md` — read its **Errata (planning, 2026-10-09)** block first; this plan implements the spec as amended there.

## Global Constraints

- Work in the worktree `~/ultima/uc-session-reads` (branch `design/session-reads`). Never in the main checkout.
- Use a private target dir for every build and test: `export CARGO_TARGET_DIR=$HOME/.cache/cargo-target-session-reads`.
- Scratch files go under `$HOME/scratch/`, never `/tmp`.
- `FLAG_V2_MIN_POSITION: u16 = 2` on `query.ring`; payload `service_id: u8 ‖ min_position: u64 LE ‖ query`.
- Remote: `FLAG_MIN_POSITION: u8 = 0x20`; query payload `min_position: u64 LE ‖ query`; `PROTOCOL_VERSION` 1 → 2.
- cnc page version 3.4 → 3.5: `CNC_V2_VERSION = (3 << 24) | (5 << 16)`. No cnc layout change.
- The node↔node wire does not change. Do not touch `uc_protocol::v2::datagram`, frame headers or wire version constants.
- Token rule: write response position `p` → token `p + 1`; query answer position → token as is; `fetch_max` only.
- A token of 0 sends a plain snapshot record, byte-identical to today.
- `READ_BARRIER_TIMEOUT_NS` (1 s) is the parked-read deadline. `MAX_PARKED_MIN_POSITION_READS = 4096`.
- Commit messages: plain, no `Co-Authored-By` or any attribution trailer.
- `cargo fmt --all` before every commit.

## Review Focus

1. **A 3.5 client on a 3.4 node** — a reasonable person expects a clear refusal, not a misread query. Pinned in Task 5 (`ryw_on_an_old_page_is_refused_by_name`).
2. **A token of 0 on a fresh client** — expected to behave exactly like a snapshot read, on any node version. Pinned in Task 5 (`a_zero_token_sends_a_plain_snapshot_record`).
3. **A forged token (`u64::MAX`) flood** — expected to cost nothing and park nothing. Pinned in Task 4 (`a_token_above_durable_is_refused_and_never_parked`) and Task 7 (`a_forged_token_is_refused_at_once`).
4. **Snapshot reads through a default `Client` on a follower** — today refused `NotServing`; after this change expected to work. Pinned in Task 7 (`a_default_client_on_a_follower_can_snapshot_read`).
5. **A parked read whose service ring is momentarily full** — expected to stay parked and go next pass, never be lost or answered twice. Pinned in Task 3 (`a_repark_keeps_the_read_and_its_deadline`).

---

## File map

| File | Change | Responsibility |
|---|---|---|
| `uc_protocol/src/v2/ipc.rs` | modify | new flag, payload codec, `ReadToken` |
| `uc_protocol/src/v2/cnc.rs` | modify | cnc 3.5 + `CNC_MIN_POSITION_MINOR` |
| `uc_log/src/cnc.rs` | modify | `CncPage::header_version` |
| `fuzz/fuzz_targets/ring_mpsc_record.rs` | modify | fuzz the new split |
| `uc_service/src/egress.rs`, `uc_service/src/apply.rs` | modify | answers carry the applied frontier |
| `uc_node/src/min_position.rs` | **create** | parked-read structure + stats (pure, unit-tested) |
| `uc_node/src/node.rs` | modify | admission, advance step, wiring |
| `uc_node/src/mutation.rs`, `uc_node/Cargo.toml` | modify | `skip-min-position-wait` tooth |
| `uc_node/src/obs/{mod,metrics}.rs`, `uc_node/tests/obs_http.rs` | modify | two metric families |
| `uc_client/src/{slots,engine,pipelined,client,error,lib}.rs`, `uc_client/src/mutation.rs` (create), `uc_client/Cargo.toml` | modify | mode, token, guard, gate, API |
| `uc_node/tests/read_your_writes.rs` | **create** | in-process integration tests |
| `uc_remote/src/{frame,slots,link,engine,client,lib}.rs` | modify | remote mode, token, guard |
| `uc_gateway/src/edge.rs`, `uc_gateway/examples/hop_bench/dummy_edge.rs`, `uc_gateway/tests/read_your_writes.rs` (create) | modify | relay the token |
| `uc_lincheck/src/session.rs` (create), `uc_lincheck/src/lib.rs` | modify | session-guarantee checker |
| `uc_node/tests/read_your_writes_capstone.rs` (create), `scripts/ryw_mutation.sh` (create) | create | capstone + teeth |
| docs (Task 12) | modify/create | semver policy, CLAUDE.md, explainer |

---

### Task 1: Protocol — flag, payload codec, `ReadToken`, cnc 3.5, page version accessor

**Files:**
- Modify: `uc_protocol/src/v2/ipc.rs:67-107` (flags and query payload helpers), its test module at `:157`
- Modify: `uc_protocol/src/v2/cnc.rs:60-72` (version doc + constant), pinned asserts at `:921` and `:984`
- Modify: `uc_log/src/cnc.rs` (add `header_version` beside `try_meta`, `:1262`)
- Modify: `fuzz/fuzz_targets/ring_mpsc_record.rs:9,42`

**Interfaces:**
- Produces: `uc_protocol::v2::ipc::{FLAG_V2_MIN_POSITION, write_min_position_query_payload, split_min_position_query_payload, ReadToken}`; `uc_protocol::v2::cnc::CNC_MIN_POSITION_MINOR: u32 = 5`; `uc_log::cnc::CncPage::header_version(&self) -> Option<u32>`.

- [ ] **Step 1: Write the failing tests** — append to the `#[cfg(test)] mod tests` in `uc_protocol/src/v2/ipc.rs`:

```rust
    #[test]
    fn min_position_query_payload_round_trips_and_pins_the_layout() {
        let mut out = Vec::new();
        write_min_position_query_payload(3, 0x0102_0304_0506_0708, b"read", &mut out);
        assert_eq!(out[0], 3);
        assert_eq!(&out[1..9], &0x0102_0304_0506_0708u64.to_le_bytes());
        assert_eq!(&out[9..], b"read");
        assert_eq!(
            split_min_position_query_payload(&out),
            Some((3, 0x0102_0304_0506_0708, &b"read"[..]))
        );
        write_min_position_query_payload(0, 1, b"", &mut out);
        assert_eq!(split_min_position_query_payload(&out), Some((0, 1, &b""[..])));
    }

    #[test]
    fn a_min_position_payload_shorter_than_nine_bytes_does_not_split() {
        for n in 0..9 {
            assert_eq!(split_min_position_query_payload(&vec![7u8; n]), None, "len {n}");
        }
    }

    #[test]
    fn the_min_position_flag_is_the_next_free_query_bit() {
        assert_eq!(FLAG_V2_LINEARIZABLE, 1);
        assert_eq!(FLAG_V2_MIN_POSITION, 2);
        assert_eq!(FLAG_V2_LINEARIZABLE & FLAG_V2_MIN_POSITION, 0);
    }

    #[test]
    fn read_tokens_order_and_round_trip_through_text() {
        let t = ReadToken::from_u64(0xdead_beef);
        assert_eq!(t.as_u64(), 0xdead_beef);
        assert_eq!(t.to_string(), "00000000deadbeef");
        assert_eq!("00000000deadbeef".parse::<ReadToken>().unwrap(), t);
        assert_eq!("deadbeef".parse::<ReadToken>().unwrap(), t);
        assert!("not-hex".parse::<ReadToken>().is_err());
        assert!(ReadToken::from_u64(1) < ReadToken::from_u64(2));
        assert_eq!(ReadToken::NONE.as_u64(), 0);
        assert_eq!(ReadToken::default(), ReadToken::NONE);
    }
```

And in `uc_protocol/src/v2/cnc.rs`'s test module add:

```rust
    #[test]
    fn cnc_3_5_is_the_min_position_page() {
        assert_eq!(CNC_V2_VERSION, (3 << 24) | (5 << 16));
        assert_eq!(CNC_MIN_POSITION_MINOR, 5);
        assert_eq!((CNC_V2_VERSION >> 16) & 0xFF, CNC_MIN_POSITION_MINOR);
    }
```

And in `uc_log/src/cnc.rs`'s test module (next to `open_file_rejects_incompatible_version`):

```rust
    #[test]
    fn header_version_reads_the_page_version() {
        let page = CncPage::heap(&test_meta());
        assert_eq!(page.header_version(), Some(CNC_V2_VERSION));
    }
```

- [ ] **Step 2: Run them to see them fail**

Run: `cargo test -p uc_protocol --lib v2:: && cargo test -p uc_log --lib cnc::tests::header_version`
Expected: compile errors — `write_min_position_query_payload`, `ReadToken`, `CNC_MIN_POSITION_MINOR`, `header_version` not found.

- [ ] **Step 3: Implement** — in `uc_protocol/src/v2/ipc.rs`, after `FLAG_V2_LINEARIZABLE`:

```rust
/// `query.ring` `flags` bit 1 (read-your-writes, spec 2026-10-08 §4.1): the
/// payload is `service_id: u8 ++ min_position: u64 LE ++ query bytes`, and the
/// node answers only once the row's applied frontier is `>= min_position`.
/// Never combined with [`FLAG_V2_LINEARIZABLE`]; a record carrying both is
/// malformed and dropped.
pub const FLAG_V2_MIN_POSITION: u16 = 2;
```

After `write_query_payload`:

```rust
/// Read-your-writes: build a [`FLAG_V2_MIN_POSITION`] `query.ring` payload
/// into `out` (cleared first).
#[inline]
pub fn write_min_position_query_payload(
    service_id: u8,
    min_position: u64,
    query: &[u8],
    out: &mut Vec<u8>,
) {
    out.clear();
    out.reserve(9 + query.len());
    out.push(service_id);
    out.extend_from_slice(&min_position.to_le_bytes());
    out.extend_from_slice(query);
}

/// Read-your-writes: split a [`FLAG_V2_MIN_POSITION`] payload into
/// `(service_id, min_position, query)`. `None` below 9 bytes — a malformed
/// record the node drops.
#[inline]
pub fn split_min_position_query_payload(payload: &[u8]) -> Option<(u8, u64, &[u8])> {
    if payload.len() < 9 {
        return None;
    }
    let min = u64::from_le_bytes(payload[1..9].try_into().ok()?);
    Some((payload[0], min, &payload[9..]))
}

/// A read-your-writes token (spec 2026-10-08 §3): the lowest applied frontier
/// a read may be answered from. Opaque to applications; printable as 16 hex
/// digits so it can ride in a cookie or a header. `NONE` (0) means "no
/// constraint" — a snapshot read.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReadToken(u64);

impl ReadToken {
    pub const NONE: ReadToken = ReadToken(0);
    pub const fn from_u64(v: u64) -> ReadToken {
        ReadToken(v)
    }
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl core::fmt::Display for ReadToken {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl core::str::FromStr for ReadToken {
    type Err = core::num::ParseIntError;
    fn from_str(s: &str) -> Result<ReadToken, Self::Err> {
        u64::from_str_radix(s, 16).map(ReadToken)
    }
}
```

In `uc_protocol/src/v2/cnc.rs`, extend the version doc above `CNC_V2_VERSION` and bump it:

```rust
/// 3.5 (read-your-writes, spec 2026-10-08): no layout change. `query.ring`
/// records may carry `FLAG_V2_MIN_POSITION`, and query answers carry the
/// row's applied frontier instead of 0. A 3.4 attacher is refused by the
/// minor check; a 3.5 attacher on a 3.4 page is NOT (`version_compatible`
/// accepts an older peer minor), so the client checks
/// [`CNC_MIN_POSITION_MINOR`] itself before sending the flag.
pub const CNC_V2_VERSION: u32 = (3 << 24) | (5 << 16);

/// The first cnc minor whose node parses `FLAG_V2_MIN_POSITION`.
pub const CNC_MIN_POSITION_MINOR: u32 = 5;
```

Update the two pinned asserts that read `assert_eq!(CNC_V2_VERSION, (3 << 24) | (4 << 16));` (at `:921` and inside `cnc_3_4_row_version_words_sit_in_the_free_status_line_words` at `:984`) to `(3 << 24) | (5 << 16)`, adding `// cnc 3.5: read-your-writes, no layout change.` to the comment block above the first.

In `uc_log/src/cnc.rs`, beside `try_meta`:

```rust
    /// The page header's version word (`major << 24 | minor << 16`), or
    /// `None` for a page torn mid-rewrite (the same posture as
    /// [`Self::try_meta`]). Read-your-writes (spec 2026-10-08, planning
    /// erratum 1) needs it: `open_file` accepts an OLDER page minor, so an
    /// attacher checks the minor itself before using a 3.5 feature.
    pub fn header_version(&self) -> Option<u32> {
        cnc::read_cnc_header(self.page()).map(|h| h.version)
    }
```

In `fuzz/fuzz_targets/ring_mpsc_record.rs`, import `split_min_position_query_payload` beside `split_query_payload` and add `let _ = split_min_position_query_payload(&buf);` after line 42's call.

- [ ] **Step 4: Run the tests to see them pass**

Run: `cargo test -p uc_protocol --lib && cargo test -p uc_log --lib cnc::`
Expected: PASS (all, including the two updated pinned asserts).

- [ ] **Step 5: Check the fuzz crate still builds** (it is outside the workspace and needs nightly)

Run: `(cd fuzz && cargo +nightly build --bin ring_mpsc_record)`
Expected: builds. If nightly is not installed, record "fuzz build not verified" in the task report; do not skip silently.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add uc_protocol uc_log fuzz/fuzz_targets/ring_mpsc_record.rs
git commit -m "protocol: FLAG_V2_MIN_POSITION payload codec, ReadToken, cnc 3.5, CncPage::header_version"
```

---

### Task 2: Service — query answers carry the applied frontier

**Files:**
- Modify: `uc_service/src/egress.rs:63-80` (`publish_query_answer`)
- Modify: `uc_service/src/apply.rs:1182-1225` (`drain_queries`)
- Test: `uc_node/tests/services.rs` (append)

**Interfaces:**
- Consumes: nothing new.
- Produces: every `MSG_V2_RESPONSE` with `FLAG_V2_IS_QUERY` carries the row's exclusive applied frontier in its 8-byte position prefix.

- [ ] **Step 1: Write the failing test** — append to `uc_node/tests/services.rs`:

```rust
/// Read-your-writes (spec 2026-10-08 §4.3): a query answer's position prefix
/// is the service's applied frontier when it answered, not 0.
#[test]
fn a_query_answer_carries_the_rows_applied_frontier() {
    use uc_protocol::ring::{BroadcastRing, MpscRing};
    use uc_protocol::v2::ipc::{
        FLAG_V2_IS_QUERY, MSG_V2_QUERY, MSG_V2_RESPONSE, client_from_extra, extra_client,
        write_query_payload,
    };
    let _g = serialize();
    let dir = tempdir();
    let node = Node::start(config(dir.path(), names(&["count"], None))).unwrap();
    wait_until("serving", || node.can_serve());
    let svc = start_service::<CountSm>(dir.path());
    let client = Client::connect(dir.path(), APP).unwrap();
    for _ in 0..5 {
        let _: u64 = client.submit(&Cmd::Add(1)).unwrap();
    }
    let cnc = CncPage::open_file(&dir.path().join("cnc2.dat"), APP).unwrap();
    let applied = cnc.service_slot(0).applied.load_acquire();
    assert!(applied > 0);

    let mut egress = BroadcastRing::open(&dir.path().join("egress_service.0.broadcast"))
        .unwrap()
        .subscribe();
    let (producer, _c) = MpscRing::open(&dir.path().join("query.ring"))
        .unwrap()
        .into_split();
    let q = bincode::serde::encode_to_vec((), bincode::config::standard()).unwrap();
    let mut payload = Vec::new();
    write_query_payload(0, &q, &mut payload);
    producer
        .try_write(MSG_V2_QUERY, 0, extra_client(0x78, 1), &payload)
        .unwrap();
    let mut buf = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(Instant::now() < deadline, "no answer within 10 s");
        match egress.try_read(&mut buf) {
            Ok(Some(rec)) if client_from_extra(rec.header_extra) == (0x78, 1) => {
                assert_eq!(rec.msg_type, MSG_V2_RESPONSE);
                assert_ne!(rec.flags & FLAG_V2_IS_QUERY, 0);
                let pos = u64::from_le_bytes(buf[..8].try_into().unwrap());
                assert_eq!(pos, applied, "no writes since: the answer reports exactly `applied`");
                break;
            }
            Ok(_) => std::thread::sleep(Duration::from_millis(1)),
            Err(e) => panic!("egress read: {e}"),
        }
    }
    client.shutdown();
    svc.stop();
    node.stop();
}
```

- [ ] **Step 2: Run it to see it fail**

Run: `cargo test -p uc_node --test services a_query_answer_carries_the_rows_applied_frontier`
Expected: FAIL — `left: 0, right: <applied>`.

- [ ] **Step 3: Implement** — in `uc_service/src/egress.rs` change `publish_query_answer`:

```rust
    /// Publish a QUERY answer: `MSG_V2_RESPONSE` with `FLAG_V2_IS_QUERY`,
    /// echoing the `svc_query` record's `header_extra`. The position prefix is
    /// the row's applied frontier when the query ran (an EXCLUSIVE end —
    /// read-your-writes spec 2026-10-08 §4.3), so a client can raise its token
    /// from it and check it against the token it sent.
    pub(crate) fn publish_query_answer(&mut self, header_extra: [u8; 8], applied: u64, resp: &[u8]) {
        self.scratch.clear();
        self.scratch.extend_from_slice(&applied.to_le_bytes());
        self.scratch.extend_from_slice(resp);
        let _ = self.producer.write(
            MSG_V2_RESPONSE,
            FLAG_V2_IS_QUERY,
            header_extra,
            &self.scratch,
        );
    }
```

In `uc_service/src/apply.rs` `drain_queries`, read the cursor once before the loop (`let applied = st.follower.cursor;` right after `let my_epoch = st.my_epoch;`) — queries run after the cycle's applies, on the apply thread, so the cursor cannot move during the drain — and pass it:

```rust
                st.egress
                    .publish_query_answer(rec.header_extra, applied, &st.resp_buf);
```

- [ ] **Step 4: Run it to see it pass, and the service suite**

Run: `cargo test -p uc_node --test services && cargo test -p uc_service`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add uc_service/src/egress.rs uc_service/src/apply.rs uc_node/tests/services.rs
git commit -m "service: query answers carry the row's applied frontier"
```

---

### Task 3: Node — the parked-read structure (pure module)

**Files:**
- Create: `uc_node/src/min_position.rs`
- Modify: `uc_node/src/lib.rs` (add `pub mod min_position;`)

**Interfaces:**
- Produces:
  - `pub const MAX_PARKED_MIN_POSITION_READS: usize = 4096;`
  - `pub struct MinPositionReadStats { pub refused_ahead: AtomicU64, pub refused_cap: AtomicU64, pub parked: AtomicU64 }` (`Default`)
  - `pub(crate) struct ParkedRead { pub client_id: u32, pub local_seq: u32, pub service_id: u8, pub query: Vec<u8>, pub token: u64, pub deadline_ns: u64 }`
  - `pub(crate) struct ParkedReads` with `new(cap: usize)`, `len()`, `is_empty()`, `park(ParkedRead) -> Result<(), ParkedRead>`, `peek_token(&mut self, row: u8) -> Option<u64>`, `pop(&mut self, row: u8) -> Option<ParkedRead>`, `expire(&mut self, now_ns: u64, f: impl FnMut(ParkedRead))`.

- [ ] **Step 1: Write the module with its failing tests** — create `uc_node/src/min_position.rs` containing ONLY the test module and the type signatures below with `todo!()` bodies, then the tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn read(row: u8, token: u64, deadline_ns: u64, seq: u32) -> ParkedRead {
        ParkedRead {
            client_id: 9,
            local_seq: seq,
            service_id: row,
            query: vec![seq as u8],
            token,
            deadline_ns,
        }
    }

    #[test]
    fn the_lowest_token_surfaces_first_whatever_the_park_order() {
        let mut p = ParkedReads::new(16);
        p.park(read(0, 900, 10, 1)).unwrap();
        p.park(read(0, 100, 10, 2)).unwrap();
        p.park(read(0, 500, 10, 3)).unwrap();
        assert_eq!(p.peek_token(0), Some(100));
        assert_eq!(p.pop(0).unwrap().local_seq, 2);
        assert_eq!(p.pop(0).unwrap().local_seq, 3);
        assert_eq!(p.pop(0).unwrap().local_seq, 1);
        assert_eq!(p.pop(0), None);
        assert!(p.is_empty());
    }

    #[test]
    fn rows_are_independent() {
        let mut p = ParkedReads::new(16);
        p.park(read(1, 50, 10, 1)).unwrap();
        assert_eq!(p.peek_token(0), None);
        assert_eq!(p.peek_token(1), Some(50));
    }

    #[test]
    fn the_cap_refuses_and_returns_the_read() {
        let mut p = ParkedReads::new(2);
        p.park(read(0, 1, 10, 1)).unwrap();
        p.park(read(0, 2, 10, 2)).unwrap();
        let back = p.park(read(0, 3, 10, 3)).unwrap_err();
        assert_eq!(back.local_seq, 3);
        assert_eq!(p.len(), 2);
    }

    #[test]
    fn expiry_takes_exactly_the_reads_past_their_deadline() {
        let mut p = ParkedReads::new(16);
        p.park(read(0, 1, 10, 1)).unwrap();
        p.park(read(0, 2, 20, 2)).unwrap();
        p.park(read(1, 3, 30, 3)).unwrap();
        let mut got = Vec::new();
        p.expire(20, |r| got.push(r.local_seq));
        assert_eq!(got, vec![1, 2]);
        assert_eq!(p.len(), 1);
        assert_eq!(p.peek_token(0), None, "expired reads leave the heap too");
        assert_eq!(p.peek_token(1), Some(3));
    }

    #[test]
    fn a_released_read_is_never_expired() {
        let mut p = ParkedReads::new(16);
        p.park(read(0, 1, 10, 1)).unwrap();
        assert_eq!(p.pop(0).unwrap().local_seq, 1);
        let mut got = Vec::new();
        p.expire(u64::MAX, |r| got.push(r.local_seq));
        assert!(got.is_empty());
    }

    #[test]
    fn a_reused_slab_slot_is_not_confused_with_its_old_occupant() {
        let mut p = ParkedReads::new(16);
        p.park(read(0, 1, 10, 1)).unwrap();
        p.pop(0).unwrap(); // frees slot 0, its expiry entry is now stale
        p.park(read(0, 2, 99, 2)).unwrap(); // reuses slot 0
        let mut got = Vec::new();
        p.expire(10, |r| got.push(r.local_seq));
        assert!(got.is_empty(), "the stale entry must not expire the new read");
        assert_eq!(p.len(), 1);
    }

    #[test]
    fn a_repark_keeps_the_read_and_its_deadline() {
        let mut p = ParkedReads::new(16);
        p.park(read(0, 5, 10, 1)).unwrap();
        let r = p.pop(0).unwrap();
        p.park(r).unwrap(); // the node does this when svc_query is full
        assert_eq!(p.peek_token(0), Some(5));
        let mut got = Vec::new();
        p.expire(10, |r| got.push((r.local_seq, r.deadline_ns)));
        assert_eq!(got, vec![(1, 10)]);
    }

    #[test]
    fn churn_never_grows_the_bookkeeping_without_bound() {
        let mut p = ParkedReads::new(8);
        for i in 0..100_000u32 {
            p.park(read(0, i as u64, u64::MAX, i)).unwrap();
            p.pop(0).unwrap();
        }
        assert!(p.bookkeeping_len() <= 4 * 8 + 8, "{}", p.bookkeeping_len());
    }
}
```

- [ ] **Step 2: Run to see the tests fail**

Run: `cargo test -p uc_node --lib min_position`
Expected: FAIL (panics at `todo!()`).

- [ ] **Step 3: Implement** — the full module body above the test module:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Read-your-writes (spec 2026-10-08 §5.1): min-position reads parked on the
//! consensus agent until the row's applied frontier reaches their token.
//!
//! Per-pass cost must not grow with the number of parked reads, because a
//! client chooses the token and so chooses how long a read waits. Each row has
//! a min-heap keyed by token (the lowest surfaces first, so a high token never
//! holds back a lower one), and one FIFO orders every read by deadline (all
//! reads get the same timeout, so admission order is deadline order). Entries
//! are tagged with a generation so a released read's leftover heap/FIFO entry
//! is recognised as stale, and both are compacted when stale entries pile up.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::sync::atomic::AtomicU64;

use uc_protocol::v2::cnc::CNC_MAX_SERVICES;

/// Hard cap on parked min-position reads per node (spec §5.1 step 5).
pub const MAX_PARKED_MIN_POSITION_READS: usize = 4096;

/// Counters shared with `/metrics` (`uc2_read_min_position_refused_total`
/// by `reason`, and the `uc2_read_min_position_parked` gauge).
#[derive(Debug, Default)]
pub struct MinPositionReadStats {
    /// Tokens above this node's `durable` position, refused at once.
    pub refused_ahead: AtomicU64,
    /// Reads refused because the parked set was full.
    pub refused_cap: AtomicU64,
    /// Reads parked right now (gauge).
    pub parked: AtomicU64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParkedRead {
    pub client_id: u32,
    pub local_seq: u32,
    pub service_id: u8,
    pub query: Vec<u8>,
    pub token: u64,
    pub deadline_ns: u64,
}

pub(crate) struct ParkedReads {
    /// `(generation, read)`; `None` = free.
    slab: Vec<Option<(u64, ParkedRead)>>,
    free: Vec<usize>,
    /// Per row: `Reverse((token, generation, slab id))`.
    heaps: Vec<BinaryHeap<Reverse<(u64, u64, usize)>>>,
    /// `(deadline_ns, generation, slab id)` in admission order.
    expiry: VecDeque<(u64, u64, usize)>,
    next_gen: u64,
    len: usize,
    cap: usize,
}

impl ParkedReads {
    pub(crate) fn new(cap: usize) -> ParkedReads {
        ParkedReads {
            slab: Vec::new(),
            free: Vec::new(),
            heaps: (0..CNC_MAX_SERVICES).map(|_| BinaryHeap::new()).collect(),
            expiry: VecDeque::new(),
            next_gen: 1,
            len: 0,
            cap,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn live(&self, generation: u64, id: usize) -> bool {
        matches!(self.slab.get(id), Some(Some((g, _))) if *g == generation)
    }

    fn take(&mut self, id: usize) -> ParkedRead {
        let (_, r) = self.slab[id].take().expect("live slab entry");
        self.free.push(id);
        self.len -= 1;
        r
    }

    /// Park `read`, or hand it back when the set is full.
    pub(crate) fn park(&mut self, read: ParkedRead) -> Result<(), ParkedRead> {
        if self.len >= self.cap {
            return Err(read);
        }
        let generation = self.next_gen;
        self.next_gen += 1;
        let row = read.service_id as usize;
        let (token, deadline) = (read.token, read.deadline_ns);
        let id = match self.free.pop() {
            Some(id) => {
                self.slab[id] = Some((generation, read));
                id
            }
            None => {
                self.slab.push(Some((generation, read)));
                self.slab.len() - 1
            }
        };
        self.heaps[row].push(Reverse((token, generation, id)));
        self.expiry.push_back((deadline, generation, id));
        self.len += 1;
        self.compact();
        Ok(())
    }

    /// The lowest parked token on `row` (stale heap entries are dropped).
    pub(crate) fn peek_token(&mut self, row: u8) -> Option<u64> {
        loop {
            let Reverse((token, g, id)) = *self.heaps[row as usize].peek()?;
            if self.live(g, id) {
                return Some(token);
            }
            self.heaps[row as usize].pop();
        }
    }

    /// Remove and return the lowest-token read on `row`.
    pub(crate) fn pop(&mut self, row: u8) -> Option<ParkedRead> {
        while let Some(Reverse((_, g, id))) = self.heaps[row as usize].pop() {
            if self.live(g, id) {
                return Some(self.take(id));
            }
        }
        None
    }

    /// Remove every read whose deadline is `<= now_ns`, oldest first.
    pub(crate) fn expire(&mut self, now_ns: u64, mut f: impl FnMut(ParkedRead)) {
        while let Some(&(deadline, g, id)) = self.expiry.front() {
            if !self.live(g, id) {
                self.expiry.pop_front();
                continue;
            }
            if deadline > now_ns {
                break;
            }
            self.expiry.pop_front();
            f(self.take(id));
        }
    }

    /// Drop stale entries once they outnumber live ones 4:1, so churn (park
    /// then release, millions of times) cannot grow memory. Amortised O(1).
    fn compact(&mut self) {
        let bound = 4 * self.cap.max(1);
        if self.expiry.len() > bound {
            let slab = &self.slab;
            self.expiry
                .retain(|&(_, g, id)| matches!(slab.get(id), Some(Some((lg, _))) if *lg == g));
        }
        for h in 0..self.heaps.len() {
            if self.heaps[h].len() > bound {
                let slab = &self.slab;
                self.heaps[h]
                    .retain(|Reverse((_, g, id))| matches!(slab.get(*id), Some(Some((lg, _))) if lg == g));
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn bookkeeping_len(&self) -> usize {
        self.expiry.len() + self.heaps.iter().map(|h| h.len()).sum::<usize>()
    }
}
```

Note on `a_repark_keeps_the_read_and_its_deadline`: a re-parked read goes to the BACK of the FIFO with its original (earlier) deadline, so `expire` may reach it late by up to the deadline of the reads ahead of it (≤ 1 s). That is the accepted cost of a re-park, which happens only when `svc_query` is momentarily full; the test pins that the read and its deadline survive.

Add `pub mod min_position;` to `uc_node/src/lib.rs` beside `pub mod mutation;`.

- [ ] **Step 4: Run to see the tests pass**

Run: `cargo test -p uc_node --lib min_position`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add uc_node/src/min_position.rs uc_node/src/lib.rs
git commit -m "node: per-row parked-read heaps for min-position reads"
```

---

### Task 4: Node — admission, advance step, mutation tooth, metrics

**Files:**
- Modify: `uc_node/src/node.rs` — imports (`:60-62`), `Consensus` fields (near `pending_reads`, `:3437`), both `Consensus` constructions (`:2372`, `:13735`), `Node` field + `Node::start` wiring (beside `schedule_refused`, `:2242`, `:2406`, `:2559`, `:3630`), `observability()` (`:3051`), `drain_query_ring` (`:9332`), the pass (`:4424`), new methods next to `advance_pending_reads`.
- Modify: `uc_node/src/mutation.rs` (new variant + parse arm + test)
- Modify: `uc_node/src/obs/mod.rs` (`ObsSources` field + `:216` literal), `uc_node/src/obs/metrics.rs` (`CONTRACT_SERIES`, render, literals at `:1766`, `:2449`, `:2556`, `:2656`), `uc_node/tests/obs_http.rs:90`
- Test: `uc_node/src/node.rs` test module (beside `queries_route_to_the_named_ids_ring_and_pending_reads_carry_the_id`, `:23714`)

**Interfaces:**
- Consumes: Task 1's `FLAG_V2_MIN_POSITION`, `split_min_position_query_payload`; Task 3's `ParkedReads`, `ParkedRead`, `MinPositionReadStats`, `MAX_PARKED_MIN_POSITION_READS`.
- Produces: `ObsSources::min_position: Arc<MinPositionReadStats>`; metric families `uc2_read_min_position_refused_total{reason="ahead"|"cap"}` and `uc2_read_min_position_parked`; `Mutation::SkipMinPositionWait` (`UC2_MUTATION=skip-min-position-wait`).

- [ ] **Step 1: Write the failing unit tests** — in the node test module:

```rust
    /// Read-your-writes (spec 2026-10-08 §5.1). A harness with a row-1 ring
    /// whose consumer the test holds, and a min-position record writer.
    fn ryw_setup(
        h: &mut Harness,
    ) -> (uc_protocol::ring::MpscProducer, SpscConsumer, uc_protocol::ring::BroadcastConsumer) {
        let (svc1_producer, svc1_consumer) =
            SpscRing::create(&h._dir.path().join("svc_query.1.ring"), 4096, 1024)
                .unwrap()
                .into_split();
        h.cons.svc_query[1] = Some(svc1_producer);
        let (producer, _c) = MpscRing::open(&h._dir.path().join("query.ring"))
            .unwrap()
            .into_split();
        let node_egress = BroadcastRing::open(&h._dir.path().join("egress_node.broadcast"))
            .unwrap()
            .subscribe();
        (producer, svc1_consumer, node_egress)
    }

    fn send_ryw(p: &uc_protocol::ring::MpscProducer, seq: u32, token: u64) {
        use uc_protocol::v2::ipc::{FLAG_V2_MIN_POSITION, MSG_V2_QUERY, write_min_position_query_payload};
        let mut payload = Vec::new();
        write_min_position_query_payload(1, token, b"q", &mut payload);
        p.try_write(MSG_V2_QUERY, FLAG_V2_MIN_POSITION, extra_client(9, seq), &payload)
            .unwrap();
    }

    fn node_answers(e: &mut uc_protocol::ring::BroadcastConsumer) -> Vec<(u16, (u32, u32))> {
        let mut buf = Vec::new();
        let mut out = Vec::new();
        while let Ok(Some(rec)) = e.try_read(&mut buf) {
            out.push((rec.msg_type, client_from_extra(rec.header_extra)));
        }
        out
    }

    #[test]
    fn a_token_above_durable_is_refused_and_never_parked() {
        let mut h = harness(); // NOT driven to leader: no leadership gate applies
        let (p, mut svc1, mut egress) = ryw_setup(&mut h);
        h.cons.cnc.counters().durable.store_release(1000);
        send_ryw(&p, 1, 1001);
        send_ryw(&p, 2, u64::MAX);
        assert!(h.cons.drain_query_ring());
        assert!(h.cons.parked_reads.is_empty());
        assert_eq!(
            node_answers(&mut egress),
            vec![(MSG_V2_RETRY, (9, 1)), (MSG_V2_RETRY, (9, 2))]
        );
        assert!(svc1.try_read(&mut Vec::new()).unwrap().is_none());
        assert_eq!(h.cons.min_position_stats.refused_ahead.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn a_caught_up_read_forwards_at_once_with_the_real_epoch() {
        let mut h = harness();
        let (p, mut svc1, _egress) = ryw_setup(&mut h);
        h.cons.cnc.counters().durable.store_release(1000);
        h.cons.cnc.service_slot(1).applied.store_release(800);
        h.cons.cnc.service_slot(1).epoch.store_release(3);
        send_ryw(&p, 1, 800);
        assert!(h.cons.drain_query_ring());
        let mut buf = Vec::new();
        let rec = svc1.try_read(&mut buf).unwrap().expect("forwarded");
        assert_eq!(rec.msg_type, MSG_V2_SVC_QUERY);
        assert_eq!(&buf[..8], &3u64.to_le_bytes(), "the real epoch, not the 0 sentinel");
        assert_eq!(&buf[8..], b"q", "id and token stripped");
        assert!(h.cons.parked_reads.is_empty());
    }

    #[test]
    fn a_lagging_read_parks_then_forwards_when_applied_reaches_the_token() {
        let mut h = harness();
        let (p, mut svc1, _egress) = ryw_setup(&mut h);
        h.cons.cnc.counters().durable.store_release(1000);
        h.cons.cnc.service_slot(1).epoch.store_release(1);
        h.cons.cnc.service_slot(1).applied.store_release(500);
        send_ryw(&p, 1, 900); // parked: 500 < 900 <= 1000
        send_ryw(&p, 2, 600); // parked, LOWER token, admitted later
        assert!(h.cons.drain_query_ring());
        assert_eq!(h.cons.parked_reads.len(), 2);
        assert!(!h.cons.advance_min_position_reads());
        h.cons.cnc.service_slot(1).applied.store_release(700);
        assert!(h.cons.advance_min_position_reads());
        let mut buf = Vec::new();
        let rec = svc1.try_read(&mut buf).unwrap().expect("the lower token released first");
        assert_eq!(client_from_extra(rec.header_extra), (9, 2));
        assert!(svc1.try_read(&mut buf).unwrap().is_none());
        h.cons.cnc.service_slot(1).applied.store_release(900);
        assert!(h.cons.advance_min_position_reads());
        let rec = svc1.try_read(&mut buf).unwrap().expect("then the higher one");
        assert_eq!(client_from_extra(rec.header_extra), (9, 1));
        assert!(h.cons.parked_reads.is_empty());
    }

    #[test]
    fn a_parked_read_past_its_deadline_is_retried() {
        // `now_ns()` is the real monotonic clock (no test override), so park
        // directly with a deadline already in the past — the same approach
        // the linearizable deadline tests take with `mk_read(.., deadline)`.
        let mut h = harness();
        let (_p, _svc1, mut egress) = ryw_setup(&mut h);
        h.cons.cnc.service_slot(1).epoch.store_release(1);
        h.cons
            .parked_reads
            .park(crate::min_position::ParkedRead {
                client_id: 9,
                local_seq: 1,
                service_id: 1,
                query: b"q".to_vec(),
                token: 900,
                deadline_ns: 0,
            })
            .unwrap();
        assert!(h.cons.advance_min_position_reads());
        assert_eq!(node_answers(&mut egress), vec![(MSG_V2_RETRY, (9, 1))]);
        assert!(h.cons.parked_reads.is_empty());
    }

    #[test]
    fn the_cap_refuses_with_retry() {
        let mut h = harness();
        let (p, _svc1, mut egress) = ryw_setup(&mut h);
        h.cons.parked_reads = crate::min_position::ParkedReads::new(1);
        h.cons.cnc.counters().durable.store_release(1000);
        h.cons.cnc.service_slot(1).epoch.store_release(1);
        send_ryw(&p, 1, 900);
        send_ryw(&p, 2, 900);
        assert!(h.cons.drain_query_ring());
        assert_eq!(h.cons.parked_reads.len(), 1);
        assert_eq!(node_answers(&mut egress), vec![(MSG_V2_RETRY, (9, 2))]);
        assert_eq!(h.cons.min_position_stats.refused_cap.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn min_position_and_linearizable_together_is_dropped() {
        use uc_protocol::v2::ipc::{FLAG_V2_MIN_POSITION, MSG_V2_QUERY, write_min_position_query_payload};
        let mut h = harness();
        let (p, mut svc1, mut egress) = ryw_setup(&mut h);
        let mut payload = Vec::new();
        write_min_position_query_payload(1, 0, b"q", &mut payload);
        p.try_write(
            MSG_V2_QUERY,
            FLAG_V2_MIN_POSITION | FLAG_V2_LINEARIZABLE,
            extra_client(9, 1),
            &payload,
        )
        .unwrap();
        p.try_write(MSG_V2_QUERY, FLAG_V2_MIN_POSITION, extra_client(9, 2), &[1, 2, 3])
            .unwrap(); // shorter than 9 bytes
        assert!(h.cons.drain_query_ring());
        assert!(node_answers(&mut egress).is_empty());
        assert!(svc1.try_read(&mut Vec::new()).unwrap().is_none());
        assert!(h.cons.parked_reads.is_empty() && h.cons.pending_reads.is_empty());
    }
```

These tests use `MSG_V2_RETRY`, `MSG_V2_SVC_QUERY`, `SpscConsumer` and `Ordering`; import them inside each test (or at the top of the test module) the way the neighbouring query tests do (`use uc_protocol::v2::ipc::{…}` inside the test fn).

- [ ] **Step 2: Run to see them fail**

Run: `cargo test -p uc_node --lib ryw_ -- --nocapture; cargo test -p uc_node --lib a_token_above_durable`
Expected: compile errors (`parked_reads`, `min_position_stats`, `advance_min_position_reads` not found).

- [ ] **Step 3: Implement**

`uc_node/src/mutation.rs` — add the variant, the parse arm `Some("skip-min-position-wait") => Some(Mutation::SkipMinPositionWait),`, and a parse test mirroring the existing ones:

```rust
    /// Read-your-writes capstone tooth T1/T2 (spec planning erratum 3): forward
    /// a min-position read at admission, as a snapshot read, ignoring its token.
    SkipMinPositionWait,
```

`Consensus` fields (beside `pending_reads`):

```rust
    /// Read-your-writes (spec 2026-10-08 §5.1): min-position reads waiting
    /// for their row's applied frontier. Separate from `pending_reads` so
    /// linearizable reads and Rung A are untouched.
    parked_reads: crate::min_position::ParkedReads,
    min_position_stats: Arc<crate::min_position::MinPositionReadStats>,
```

Both constructions: `parked_reads: crate::min_position::ParkedReads::new(crate::min_position::MAX_PARKED_MIN_POSITION_READS),` and `min_position_stats: Arc::clone(&min_position_stats),` (in `Node::start`, create `let min_position_stats = Arc::new(crate::min_position::MinPositionReadStats::default());` beside `schedule_refused`, keep a clone on `Node` as `min_position_stats`); the harness construction uses `Arc::new(Default::default())`.

In `drain_query_ring`, right after `let (client_id, local_seq) = client_from_extra(rec.header_extra);`:

```rust
                    if rec.flags & FLAG_V2_MIN_POSITION != 0 {
                        self.admit_min_position_read(client_id, local_seq, rec.flags, &buf);
                        continue;
                    }
```

New methods beside `advance_pending_reads`:

```rust
    /// Read-your-writes admission (spec 2026-10-08 §5.1, steps 1-6).
    fn admit_min_position_read(&mut self, client_id: u32, local_seq: u32, flags: u16, buf: &[u8]) {
        if flags & FLAG_V2_LINEARIZABLE != 0 {
            return; // malformed: never both (§4.1); dropped like an id-less record
        }
        let Some((service_id, token, query)) = split_min_position_query_payload(buf) else {
            return; // shorter than 9 bytes: malformed, dropped
        };
        if !self.has_service_ring(service_id) {
            self.send_bad_service(client_id, local_seq, service_id);
            return;
        }
        #[cfg(feature = "mutation-testing")]
        if matches!(
            crate::mutation::active(),
            Some(crate::mutation::Mutation::SkipMinPositionWait)
        ) {
            self.forward_svc_query(service_id, client_id, local_seq, 0, query);
            return;
        }
        // Step 3, the durable bound: this node does not hold the bytes the
        // token names, so waiting could be forever (a forged token) or is
        // someone else's job (a lagging node). Never parked.
        if token > self.cnc.counters().durable.load_acquire() {
            self.min_position_stats
                .refused_ahead
                .fetch_add(1, Ordering::Relaxed);
            self.send_retry(client_id, local_seq);
            return;
        }
        // Step 4, the fast path.
        if let Some(e) = self.min_position_ready(service_id, token)
            && self.forward_svc_query(service_id, client_id, local_seq, e, query)
        {
            return;
        }
        // Steps 5-6: park, or refuse at the cap.
        let read = crate::min_position::ParkedRead {
            client_id,
            local_seq,
            service_id,
            query: query.to_vec(),
            token,
            deadline_ns: self.now_ns() + READ_BARRIER_TIMEOUT_NS,
        };
        if let Err(read) = self.parked_reads.park(read) {
            self.min_position_stats
                .refused_cap
                .fetch_add(1, Ordering::Relaxed);
            self.send_retry(read.client_id, read.local_seq);
        }
        self.min_position_stats
            .parked
            .store(self.parked_reads.len() as u64, Ordering::Relaxed);
    }

    /// B's capture-recheck bracket for a min-position read: `Some(epoch)` iff
    /// an attached incarnation (`epoch >= 1`) has applied at least `token`
    /// and was still the same incarnation after the check.
    fn min_position_ready(&self, service_id: u8, token: u64) -> Option<u64> {
        let slot = self.cnc.service_slot(service_id as usize);
        let e = slot.epoch.load_acquire();
        let applied = slot.applied.load_acquire();
        (e >= 1 && applied >= token && slot.epoch.load_acquire() == e).then_some(e)
    }

    /// Release parked reads whose row has caught up (lowest token first) and
    /// RETRY those past their deadline. A pass that releases and expires
    /// nothing costs one heap peek per row with parked reads.
    fn advance_min_position_reads(&mut self) -> bool {
        if self.parked_reads.is_empty() {
            return false;
        }
        let mut did = false;
        for row in 0..CNC_MAX_SERVICES as u8 {
            while let Some(token) = self.parked_reads.peek_token(row) {
                let Some(e) = self.min_position_ready(row, token) else {
                    break;
                };
                let r = self.parked_reads.pop(row).expect("peeked");
                if !self.forward_svc_query(r.service_id, r.client_id, r.local_seq, e, &r.query) {
                    // svc_query momentarily full: keep it (a slot was just
                    // freed, so this cannot hit the cap) and try next pass.
                    let _ = self.parked_reads.park(r);
                    break;
                }
                did = true;
            }
        }
        let now = self.now_ns();
        let mut expired = Vec::new();
        self.parked_reads.expire(now, |r| expired.push(r));
        for r in expired {
            self.send_retry(r.client_id, r.local_seq);
            did = true;
        }
        self.min_position_stats
            .parked
            .store(self.parked_reads.len() as u64, Ordering::Relaxed);
        did
    }
```

In the consensus pass, after `did |= self.advance_pending_reads();` (`:4424`):

```rust
        // 3e. Read-your-writes: release caught-up min-position reads, RETRY
        // expired ones (spec 2026-10-08 §5.1).
        did |= self.advance_min_position_reads();
```

Imports: add `FLAG_V2_MIN_POSITION` and `split_min_position_query_payload` to the `uc_protocol::v2::ipc` import list at `:60-62`; `CNC_MAX_SERVICES` if not already imported.

Metrics: add `pub min_position: Arc<crate::min_position::MinPositionReadStats>,` to `ObsSources` (doc: "Read-your-writes: refused min-position reads by reason, and the parked gauge — the SAME allocation the consensus agent bumps."); set it in `observability()` with `Arc::clone(&self.min_position_stats)` and in every `ObsSources { … }` literal (`obs/mod.rs:216`, `obs/metrics.rs:1766/2449/2556/2656`, `tests/obs_http.rs:90`) with `Arc::new(Default::default())`. In `CONTRACT_SERIES` add `"uc2_read_min_position_refused_total", "uc2_read_min_position_parked",` after `"uc2_schedule_apply_refused_total"`. In `render_prometheus`, after the `uc2_schedule_apply_refused_total` counter:

```rust
    push_labeled(
        out,
        "uc2_read_min_position_refused_total",
        "Read-your-writes reads this node answered RETRY at admission without parking: reason=\"ahead\" when the token named bytes this node does not hold (a lagging node, or a forged or stale token), reason=\"cap\" when the parked set was full. Deadline RETRYs of parked reads are not counted here.",
        "counter",
        &[
            ("reason=\"ahead\"".to_string(), s.min_position.refused_ahead.load(Ordering::Relaxed)),
            ("reason=\"cap\"".to_string(), s.min_position.refused_cap.load(Ordering::Relaxed)),
        ],
    );
    push_gauge(
        out,
        "uc2_read_min_position_parked",
        "Read-your-writes reads parked on this node right now, waiting for their row's applied frontier to reach their token.",
        s.min_position.parked.load(Ordering::Relaxed),
    );
```

(`push_labeled` already takes the label body without braces — check one existing call before writing.)

- [ ] **Step 4: Run to see them pass, plus every existing read test**

Run: `cargo test -p uc_node --lib && cargo test -p uc_node --test query_barrier && cargo test -p uc_node --test obs_http`
Expected: PASS. `query_barrier` passing unchanged is the "linearizable reads and Rung A untouched" check.

- [ ] **Step 5: Run the mutation build once** (the tooth must compile)

Run: `cargo build -p uc_node --features mutation-testing`
Expected: builds.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add uc_node
git commit -m "node: admit, park and release min-position reads; durable bound, cap, metrics"
```

---

### Task 5: Client engine — mode, token, guard, version gate, serving gate, explicit API

**Files:**
- Modify: `uc_client/src/slots.rs` (`Slot`, `new`, `claim` → `claim_inner`, new `claim_with_min_position`, new `min_position`)
- Modify: `uc_client/src/engine.rs` (`Consistency`, `SubmitError`, `StatCells`/`EngineStats`, `Shared`, `attach`, `send`, `try_query_on`, new methods, `handle_record`)
- Create: `uc_client/src/mutation.rs`; modify `uc_client/src/lib.rs`, `uc_client/Cargo.toml`
- Modify: `uc_node/Cargo.toml` (`mutation-testing` turns on `uc_client/mutation-testing`)
- Test: `uc_client/tests/engine_synthetic.rs` (append), `uc_client/src/slots.rs` tests

**Interfaces:**
- Consumes: Task 1's `FLAG_V2_MIN_POSITION`, `write_min_position_query_payload`, `ReadToken`, `CNC_MIN_POSITION_MINOR`, `CncPage::header_version`.
- Produces: `Consistency::ReadYourWrites`; `SubmitError::ReadYourWritesUnsupported`; `EngineStats::stale_answers: u64`; `SendHalf::read_token(&self) -> ReadToken`, `SendHalf::observe(&self, ReadToken)`, `SendHalf::try_query_at_least(&self, user_data: u64, id: u8, query_bytes: &[u8], token: ReadToken) -> Result<(), SubmitError>`; `pub use uc_protocol::v2::ipc::ReadToken` from `uc_client`.

- [ ] **Step 1: Write the failing tests** — in `uc_client/tests/engine_synthetic.rs` (reuse its `make_instance`, `cfg()`, `producer`, `response`, `drain` helpers; read their bodies at `:1440-1490` first):

```rust
use uc_client::ReadToken;
use uc_protocol::v2::ipc::{FLAG_V2_MIN_POSITION, split_min_position_query_payload};

fn read_query_record(dir: &Path) -> (u16, Vec<u8>) {
    let (_p, mut c) = MpscRing::open(&dir.join("query.ring")).unwrap().into_split();
    let mut buf = Vec::new();
    let rec = c.try_read(&mut buf).unwrap().expect("a query record");
    (rec.flags, buf)
}

#[test]
fn a_zero_token_sends_a_plain_snapshot_record() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "ryw0", 1 << 20, 1 << 20);
    let (s, _p) = Engine::attach(dir.path(), "ryw0", cfg()).unwrap();
    s.try_query(1, b"q", Consistency::ReadYourWrites).unwrap();
    let (flags, payload) = read_query_record(dir.path());
    assert_eq!(flags, 0);
    assert_eq!(payload, [0, b'q']);
}

#[test]
fn write_and_answer_positions_raise_the_token_by_the_right_rule() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "ryw1", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(dir.path(), "ryw1", cfg()).unwrap();
    let mut eg = producer(dir.path(), "egress_service.0.broadcast");
    s.try_submit(1, b"w").unwrap();
    eg.write(MSG_V2_RESPONSE, 0, extra_client(s.client_id(), 0), &response(4096, b"ok")).unwrap();
    drain(&mut p);
    assert_eq!(s.read_token(), ReadToken::from_u64(4097), "a write's frame START + 1");
    s.try_query(2, b"q", Consistency::Snapshot).unwrap();
    eg.write(MSG_V2_RESPONSE, FLAG_V2_IS_QUERY, extra_client(s.client_id(), 1), &response(8000, b"v")).unwrap();
    drain(&mut p);
    assert_eq!(s.read_token(), ReadToken::from_u64(8000), "an answer's frontier, as is");
    s.observe(ReadToken::from_u64(10));
    assert_eq!(s.read_token(), ReadToken::from_u64(8000), "observe never lowers");
}

#[test]
fn a_ryw_query_carries_the_token_and_an_answer_below_it_is_a_retry() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "ryw2", 1 << 20, 1 << 20);
    let (s, mut p) = Engine::attach(dir.path(), "ryw2", cfg()).unwrap();
    s.observe(ReadToken::from_u64(5000));
    s.try_query(7, b"q", Consistency::ReadYourWrites).unwrap();
    let (flags, payload) = read_query_record(dir.path());
    assert_eq!(flags, FLAG_V2_MIN_POSITION);
    assert_eq!(split_min_position_query_payload(&payload), Some((0, 5000, &b"q"[..])));
    let mut eg = producer(dir.path(), "egress_service.0.broadcast");
    eg.write(MSG_V2_RESPONSE, FLAG_V2_IS_QUERY, extra_client(s.client_id(), 0), &response(4999, b"stale")).unwrap();
    let got = drain(&mut p);
    assert_eq!(got, vec![(7, None, "retry".to_string())], "the guard: never hand stale data up");
    assert_eq!(p.stats().stale_answers, 1);
    assert_eq!(s.read_token(), ReadToken::from_u64(5000), "a rejected answer does not move the token");
}

#[test]
fn try_query_at_least_uses_its_own_token_not_the_automatic_one() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "ryw3", 1 << 20, 1 << 20);
    let (s, _p) = Engine::attach(dir.path(), "ryw3", cfg()).unwrap();
    s.observe(ReadToken::from_u64(9000));
    s.try_query_at_least(1, 0, b"q", ReadToken::from_u64(42)).unwrap();
    let (_, payload) = read_query_record(dir.path());
    assert_eq!(split_min_position_query_payload(&payload), Some((0, 42, &b"q"[..])));
}

#[test]
fn ryw_on_an_old_page_is_refused_by_name() {
    use uc_protocol::v2::cnc::{CNC_OFF_HEADER_CRC, CNC_OFF_VERSION};
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "ryw4", 1 << 20, 1 << 20);
    // Rewrite the page as cnc 3.4 (crc recomputed): a 3.5 attacher accepts it.
    let path = dir.path().join("cnc2.dat");
    let mut raw = std::fs::read(&path).unwrap();
    let v34: u32 = (3 << 24) | (4 << 16);
    raw[CNC_OFF_VERSION..CNC_OFF_VERSION + 4].copy_from_slice(&v34.to_le_bytes());
    let crc = crc32fast::hash(&raw[..CNC_OFF_HEADER_CRC]);
    raw[CNC_OFF_HEADER_CRC..CNC_OFF_HEADER_CRC + 4].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, &raw).unwrap();
    let (s, _p) = Engine::attach(dir.path(), "ryw4", cfg()).unwrap();
    s.try_query(1, b"q", Consistency::ReadYourWrites).expect("token 0: a plain snapshot read");
    s.observe(ReadToken::from_u64(1));
    assert_eq!(
        s.try_query(2, b"q", Consistency::ReadYourWrites),
        Err(SubmitError::ReadYourWritesUnsupported)
    );
}

#[test]
fn the_serving_gate_lets_any_node_reads_through() {
    let dir = tempfile::tempdir_in(env!("CARGO_TARGET_TMPDIR")).unwrap();
    make_instance(dir.path(), "ryw5", 1 << 20, 1 << 20);
    let gated = EngineConfig { serving_gate: true, ..cfg() };
    let (s, _p) = Engine::attach(dir.path(), "ryw5", gated).unwrap(); // CAN_SERVE clear
    assert_eq!(s.try_submit(1, b"w"), Err(SubmitError::NotServing));
    assert_eq!(s.try_query(2, b"q", Consistency::Linearizable), Err(SubmitError::NotServing));
    s.try_query(3, b"q", Consistency::Snapshot).expect("snapshot reads pass the gate");
    s.try_query(4, b"q", Consistency::ReadYourWrites).expect("ryw reads pass the gate");
}
```

(`crc32fast` is a dev-dependency of `uc_log`; add it to `uc_client`'s `[dev-dependencies]` if the test does not compile without it. `SubmitError` must derive `PartialEq` — it already does if `assert_eq!` on it compiles elsewhere in this file; otherwise use `matches!`.)

In `uc_client/src/slots.rs` tests add:

```rust
    #[test]
    fn min_position_is_read_back_for_the_live_generation_only() {
        let t = SlotTable::new(4, 0);
        let seq = t.claim_with_min_position(1, ReqKind::Query, u64::MAX, 1, false, 777).unwrap();
        assert_eq!(t.min_position(seq as u32), 777);
        assert_eq!(t.min_position(seq as u32 + 1), 0, "another generation reads 0");
        assert!(matches!(t.resolve(seq as u32, Some(ReqKind::Query), Some(0)), Resolve::Won { .. }));
        assert_eq!(t.min_position(seq as u32), 0, "a freed slot reads 0");
        let seq2 = t.claim(1, ReqKind::Query, u64::MAX, 1, false).unwrap();
        assert_eq!(t.min_position(seq2 as u32), 0, "plain claim stores 0");
    }
```

- [ ] **Step 2: Run to see them fail**

Run: `cargo test -p uc_client`
Expected: compile errors (`ReadYourWrites`, `read_token`, `claim_with_min_position`, … not found).

- [ ] **Step 3: Implement**

`uc_client/src/slots.rs`: add `min_position: AtomicU64, // read-your-writes: the token this query was sent with (0 = none)` to `Slot`, initialise it in `new`, rename `claim`'s body to `fn claim_inner(…, min_position: u64)` storing `slot.min_position.store(min_position, Ordering::Relaxed);` in phase 2, and add:

```rust
    pub(crate) fn claim(&self, user_data: u64, kind: ReqKind, deadline_ns: u64, expected: u8, fan_in: bool) -> Result<u64, ClaimError> {
        self.claim_inner(user_data, kind, deadline_ns, expected, fan_in, 0)
    }

    /// Read-your-writes: claim a query slot that remembers the token it was
    /// sent with, so the answer can be checked against it (spec §5.3 guard).
    pub(crate) fn claim_with_min_position(&self, user_data: u64, kind: ReqKind, deadline_ns: u64, expected: u8, fan_in: bool, min_position: u64) -> Result<u64, ClaimError> {
        self.claim_inner(user_data, kind, deadline_ns, expected, fan_in, min_position)
    }

    /// The token the live generation at `wire_seq` was sent with; 0 for a
    /// free, reserved or other-generation slot. Read BEFORE `resolve` frees
    /// it; a generation that changes in between makes `resolve` return `Miss`,
    /// so a stale value here is never acted on (invariant 4).
    pub(crate) fn min_position(&self, wire_seq: u32) -> u64 {
        let slot = &self.slots[(wire_seq as usize) & self.mask];
        let owner = slot.owner.load(Ordering::Acquire);
        if owner == FREE || owner == RESERVED || (owner - 1) as u32 != wire_seq {
            return 0;
        }
        slot.min_position.load(Ordering::Relaxed)
    }
```

`uc_client/Cargo.toml`: add `[features]\n# Capstone tooth (spec 2026-10-08 planning erratum 3); inert unless UC2_CLIENT_MUTATION is set.\nmutation-testing = []`. `uc_node/Cargo.toml`: change `mutation-testing = ["uc_consensus/mutation-testing"]` to `mutation-testing = ["uc_consensus/mutation-testing", "uc_client/mutation-testing"]`.

`uc_client/src/mutation.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Capstone tooth (read-your-writes spec, planning erratum 3): with the
//! `mutation-testing` feature AND `UC2_CLIENT_MUTATION=skip-min-position-guard`,
//! the engine hands an answer below its token up instead of turning it into
//! Retry. Without the feature this is a constant `false`.

#[cfg(feature = "mutation-testing")]
pub(crate) fn guard_disabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("UC2_CLIENT_MUTATION").as_deref() == Ok("skip-min-position-guard")
    })
}

#[cfg(not(feature = "mutation-testing"))]
#[inline(always)]
pub(crate) fn guard_disabled() -> bool {
    false
}
```

`uc_client/src/lib.rs`: `mod mutation;` and `pub use uc_protocol::v2::ipc::ReadToken;`.

`uc_client/src/engine.rs`:

- `Consistency`: add
  ```rust
      /// Read-your-writes (spec 2026-10-08): answered by ANY node once its
      /// service has applied at least this client's token — every write this
      /// client had acknowledged, and every state a previous read returned.
      /// Not linearizable: another client's recent write may be missing.
      ReadYourWrites,
  ```
- `SubmitError`: add
  ```rust
      /// A read-your-writes read with a non-zero token on a node older than
      /// cnc 3.5, which cannot parse the token (spec planning erratum 1).
      #[error("this node predates read-your-writes reads (cnc page older than 3.5)")]
      ReadYourWritesUnsupported,
  ```
- `StatCells` gets `stale_answers: AtomicU64`; `EngineStats` gets `/// Read-your-writes answers below the token sent, turned into Retry by the client guard.\npub stale_answers: u64,`; `snapshot()` copies it.
- `Shared` gets:
  ```rust
      /// Read-your-writes (spec §3): the highest exclusive applied frontier
      /// this client has seen. Raised with `fetch_max` only.
      token: AtomicU64,
      /// cnc >= 3.5: the node parses `FLAG_V2_MIN_POSITION`. Read once at attach.
      min_position_supported: bool,
  ```
  In `attach`, after `meta`: `let page_version = cnc.header_version().ok_or(uc_log::cnc::CncError::BadHeader)?;` and in the `Shared` literal `token: AtomicU64::new(0), min_position_supported: (page_version >> 16) & 0xFF >= uc_protocol::v2::cnc::CNC_MIN_POSITION_MINOR,`.
- `send`: add a trailing parameter `min_position: u64`. Gate only writes and linearizable reads:
  ```rust
          let gated = kind == ReqKind::Submit || flags & FLAG_V2_LINEARIZABLE != 0;
          if gated && s.serving_gate && s.cnc.status().flags.load_acquire() & NODE_FLAG_CAN_SERVE == 0 {
              return Err(SubmitError::NotServing);
          }
          let wire_len = bytes.len() + usize::from(prefix.is_some()) + if min_position > 0 { 8 } else { 0 };
  ```
  claim with `s.table.claim_with_min_position(user_data, kind, deadline_ns, expected, fan_in, min_position)`; in the `Some(id)` write arm:
  ```rust
              Some(id) => {
                  let mut scratch = self.scratch.borrow_mut();
                  if min_position > 0 {
                      write_min_position_query_payload(id, min_position, bytes, &mut scratch);
                      ring.try_write(msg_type, flags | FLAG_V2_MIN_POSITION, extra, &scratch)
                  } else {
                      write_query_payload(id, bytes, &mut scratch);
                      ring.try_write(msg_type, flags, extra, &scratch)
                  }
              }
  ```
  Every existing `self.send(...)` call passes `0` as the new last argument.
- Replace `try_query_on`'s body, and add the explicit API:
  ```rust
      pub fn try_query_on(&self, user_data: u64, id: u8, query_bytes: &[u8], c: Consistency) -> Result<(), SubmitError> {
          let (flags, min) = match c {
              Consistency::Linearizable => (FLAG_V2_LINEARIZABLE, 0),
              Consistency::Snapshot => (0, 0),
              Consistency::ReadYourWrites => (0, self.shared.token.load(Ordering::Acquire)),
          };
          self.query_with_min(user_data, id, query_bytes, flags, min)
      }

      /// Read-your-writes with an EXPLICIT token, independent of this
      /// client's automatic one (spec §5.3). The gateway relays each remote
      /// client's own token through this.
      pub fn try_query_at_least(&self, user_data: u64, id: u8, query_bytes: &[u8], token: ReadToken) -> Result<(), SubmitError> {
          self.query_with_min(user_data, id, query_bytes, 0, token.as_u64())
      }

      fn query_with_min(&self, user_data: u64, id: u8, query_bytes: &[u8], flags: u16, min: u64) -> Result<(), SubmitError> {
          let expected = self.expect_one(id)?;
          if min > 0 && !self.shared.min_position_supported {
              return Err(SubmitError::ReadYourWritesUnsupported);
          }
          self.send(&self.query, MSG_V2_QUERY, flags, ReqKind::Query, user_data, query_bytes, expected, false, Some(id), min)
      }

      /// This client's read-your-writes token: hand it to another process
      /// (cookie, header) and pass it to that client's `observe`.
      pub fn read_token(&self) -> ReadToken {
          ReadToken::from_u64(self.shared.token.load(Ordering::Acquire))
      }

      /// Merge a token carried in from elsewhere. Never lowers the token.
      pub fn observe(&self, token: ReadToken) {
          self.shared.token.fetch_max(token.as_u64(), Ordering::AcqRel);
      }
  ```
- `handle_record`, `MSG_V2_RESPONSE` arm: read the sent token before resolving, then guard and raise on the single-ring win, and raise on the fan-in win:
  ```rust
              let min_sent = if delivered == ReqKind::Query {
                  shared.table.min_position(wire_seq)
              } else {
                  0
              };
              // … existing `match shared.table.resolve(...)` …
                  Resolve::Won { user_data, fan_in: false, .. } => {
                      if delivered == ReqKind::Query
                          && position < min_sent
                          && !crate::mutation::guard_disabled()
                      {
                          shared.stats.stale_answers.fetch_add(1, Ordering::Relaxed);
                          shared.stats.retry.fetch_add(1, Ordering::Relaxed);
                          cb(Completion { user_data, position: None, outcome: Outcome::Retry });
                          return 1;
                      }
                      let seen = if delivered == ReqKind::Query { position } else { position.saturating_add(1) };
                      shared.token.fetch_max(seen, Ordering::AcqRel);
                      // … existing completion …
                  }
                  Resolve::Won { user_data, fan_in: true, first } => {
                      // … existing push/sort …
                      shared.token.fetch_max(f.position.saturating_add(1), Ordering::AcqRel);
                      // … existing completion …
                  }
  ```
- Fix every exhaustive `match` on `SubmitError` the compiler reports: `uc_client/src/pipelined.rs` (map to a new `ClientError::ReadYourWritesUnsupported`, reclaiming `user_data` like its siblings — add that variant to `uc_client/src/error.rs` with the same message) and `uc_gateway/src/edge.rs` (`Err(SubmitError::ReadYourWritesUnsupported)` → unreachable for the gateway, which runs beside a same-version node; answer it like `ServiceNotDeclared`).

- [ ] **Step 4: Run to see them pass, then the feature-gated crates**

Run: `cargo test -p uc_client && cargo build -p uc_gateway && cargo clippy -p uc_crashtest --features hard-crash-tests --all-targets -- -D warnings`
Expected: PASS / builds / clean. The last command catches exhaustive `ClientError` matches behind `hard-crash-tests`; fix any it reports with an arm mirroring `ServiceNotDeclared`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add uc_client uc_node/Cargo.toml uc_gateway/src/edge.rs testing
git commit -m "client: ReadYourWrites mode, automatic + explicit tokens, answer guard, page-version gate, any-node reads pass the serving gate"
```

---

### Task 6: Client SDK — blocking and pipelined API

**Files:**
- Modify: `uc_client/src/pipelined.rs` (after `query_snapshot_on`, `:276`), `uc_client/src/client.rs` (after `query_snapshot_on`, `:164`)

**Interfaces:**
- Consumes: Task 5's `Consistency::ReadYourWrites`, `SendHalf::{read_token, observe, try_query_at_least}`.
- Produces: `PipelinedClient::{query_read_your_writes, query_read_your_writes_on, query_at_least_on, read_token, observe}` and `Client::{query_read_your_writes, query_read_your_writes_on, query_at_least_on, read_token, observe, stats}`.

- [ ] **Step 1: Write the failing test** — in `uc_client/tests/engine_synthetic.rs` there is no pipelined harness; use a compile-and-behaviour test in `uc_node/tests/read_your_writes.rs` (Task 7 creates it). For this task, add a doc-test on `Client::query_read_your_writes`:

```rust
    /// Read-your-writes read (spec 2026-10-08): answered by any node once it
    /// has applied every write this client had acknowledged.
    ///
    /// ```no_run
    /// # fn demo(c: &uc_client::Client) -> Result<(), uc_client::ClientError> {
    /// let _: u64 = c.submit(&1u64)?;
    /// let token = c.read_token(); // carry this to another process if needed
    /// let v: u64 = c.query_read_your_writes(&())?;
    /// # let _ = (token, v); Ok(()) }
    /// ```
```

- [ ] **Step 2: Run to see it fail**

Run: `cargo test -p uc_client --doc`
Expected: FAIL (`query_read_your_writes`, `read_token` not found).

- [ ] **Step 3: Implement** — `PipelinedClient`:

```rust
    /// Read-your-writes read against FSM 0 (spec 2026-10-08): any node answers
    /// once it has applied this client's token.
    pub fn query_read_your_writes<Q: Serialize, QR: DeserializeOwned>(&self, q: &Q) -> Result<Ticket<QR>, ClientError> {
        self.query_read_your_writes_on(0, q)
    }

    pub fn query_read_your_writes_on<Q: Serialize, QR: DeserializeOwned>(&self, id: u8, q: &Q) -> Result<Ticket<QR>, ClientError> {
        let bytes = encode(q)?;
        self.dispatch(&bytes, true, move |send, ud, b| {
            send.try_query_on(ud, id, b, Consistency::ReadYourWrites)
        })
    }

    /// Read with an explicit token, ignoring this client's automatic one.
    pub fn query_at_least_on<Q: Serialize, QR: DeserializeOwned>(&self, id: u8, q: &Q, token: ReadToken) -> Result<Ticket<QR>, ClientError> {
        let bytes = encode(q)?;
        self.dispatch(&bytes, true, move |send, ud, b| send.try_query_at_least(ud, id, b, token))
    }

    pub fn read_token(&self) -> ReadToken {
        self.send.lock().unwrap().read_token()
    }

    pub fn observe(&self, token: ReadToken) {
        self.send.lock().unwrap().observe(token)
    }
```

`Client` (blocking): `query_read_your_writes`, `query_read_your_writes_on`, `query_at_least_on` (each `self.inner.<same>(…)?.wait()`), `read_token`, `observe`, and `pub fn stats(&self) -> EngineStats { self.inner.stats() }`. Import `ReadToken`/`EngineStats` as needed.

- [ ] **Step 4: Run to see it pass**

Run: `cargo test -p uc_client`
Expected: PASS (including the doc-test, which is `no_run` and only compiles).

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add uc_client
git commit -m "client: read-your-writes on PipelinedClient and Client"
```

---

### Task 7: In-process integration tests

**Files:**
- Create: `uc_node/tests/read_your_writes.rs`

**Interfaces:**
- Consumes: Tasks 1–6.
- Produces: nothing new.

- [ ] **Step 1: Write the tests** — copy the harness from `uc_node/tests/query_barrier.rs:1-189` (`CountSm` + `WholeStateSnapshot`, `make_config`, `Cluster`, `spawn_cluster`, `await_single_leader`, `cut`) into the new file, renaming the prefix to `"uc2-ryw-"` and `APP` to `"ryw"`. Then add a helper and the tests:

```rust
fn start_services(c: &Cluster) -> Vec<uc_service::Service<CountSm>> {
    c.dirs
        .iter()
        .map(|d| {
            ServiceBuilder::new(ServiceConfig::new(d, APP), CountSm::default())
                .start()
                .unwrap()
        })
        .collect()
}

/// RETRY is the documented answer of a node that is briefly behind; a caller
/// retries. Bounded so a real failure still fails.
fn ryw_read(client: &Client, within: Duration) -> Result<u64, ClientError> {
    let deadline = Instant::now() + within;
    loop {
        match client.query_read_your_writes::<(), u64>(&()) {
            Err(ClientError::Retry) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(2))
            }
            other => return other,
        }
    }
}

#[test]
fn a_follower_read_sees_the_writers_acknowledged_writes() {
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 30);
    let svcs = start_services(&c);
    let follower = (leader + 1) % 3;
    let writer = Client::connect(&c.dirs[leader], APP).unwrap();
    let reader = Client::connect(&c.dirs[follower], APP).unwrap();
    for i in 1..=50u64 {
        let total: u64 = writer.submit(&Cmd::Add(1)).unwrap();
        assert_eq!(total, i);
        reader.observe(writer.read_token());
        let seen = ryw_read(&reader, Duration::from_secs(5)).unwrap();
        assert!(seen >= total, "follower read {seen} after write acknowledged at {total}");
    }
    writer.shutdown();
    reader.shutdown();
    for s in svcs {
        s.stop();
    }
    for n in c.nodes.drain(..) {
        n.stop();
    }
}

#[test]
fn a_default_client_on_a_follower_can_snapshot_read() {
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 30);
    let svcs = start_services(&c);
    let follower = (leader + 1) % 3;
    let reader = Client::connect(&c.dirs[follower], APP).unwrap();
    let _: u64 = reader
        .query_snapshot(&())
        .expect("snapshot reads are any-node reads: the serving gate lets them through");
    reader.shutdown();
    for s in svcs {
        s.stop();
    }
    for n in c.nodes.drain(..) {
        n.stop();
    }
}

#[test]
fn a_follower_without_its_service_parks_then_retries_at_the_deadline() {
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 30);
    let mut svcs = start_services(&c);
    let follower = (leader + 1) % 3;
    let writer = Client::connect(&c.dirs[leader], APP).unwrap();
    let reader = Client::connect(&c.dirs[follower], APP).unwrap();
    let _: u64 = writer.submit(&Cmd::Add(1)).unwrap();
    // Stop the follower's service: its node keeps receiving (durable moves),
    // its applied frontier does not.
    svcs.remove(follower).stop();
    let _: u64 = writer.submit(&Cmd::Add(1)).unwrap();
    let token = writer.read_token();
    // Wait until the follower holds the bytes, so the read parks rather than
    // being refused as "ahead".
    let cnc = uc_log::cnc::CncPage::open_file(&c.dirs[follower].join("cnc2.dat"), APP).unwrap();
    let t0 = Instant::now();
    while cnc.counters().durable.load_acquire() < token.as_u64() {
        assert!(t0.elapsed() < Duration::from_secs(5), "follower never received the write");
        std::thread::sleep(Duration::from_millis(1));
    }
    reader.observe(token);
    let started = Instant::now();
    let res = reader.query_read_your_writes::<(), u64>(&());
    let waited = started.elapsed();
    assert!(matches!(res, Err(ClientError::Retry)), "got {res:?}");
    assert!(waited >= Duration::from_millis(800), "answered after {waited:?}: it did not park");
    assert!(waited < Duration::from_secs(5));
    // Restart the service: the same token is now answered.
    let svc = ServiceBuilder::new(ServiceConfig::new(&c.dirs[follower], APP), CountSm::default())
        .start()
        .unwrap();
    let seen = ryw_read(&reader, Duration::from_secs(10)).unwrap();
    assert!(seen >= 2);
    writer.shutdown();
    reader.shutdown();
    svc.stop();
    for s in svcs {
        s.stop();
    }
    for n in c.nodes.drain(..) {
        n.stop();
    }
}

#[test]
fn a_forged_token_is_refused_at_once() {
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 30);
    let svcs = start_services(&c);
    let follower = (leader + 1) % 3;
    let reader = Client::connect(&c.dirs[follower], APP).unwrap();
    reader.observe(uc_client::ReadToken::from_u64(u64::MAX));
    let started = Instant::now();
    let res = reader.query_read_your_writes::<(), u64>(&());
    assert!(matches!(res, Err(ClientError::Retry)), "got {res:?}");
    assert!(started.elapsed() < Duration::from_millis(500), "a forged token must not park");
    let stats = c.nodes[follower].observability().min_position;
    assert!(stats.refused_ahead.load(std::sync::atomic::Ordering::Relaxed) >= 1);
    assert_eq!(stats.parked.load(std::sync::atomic::Ordering::Relaxed), 0);
    reader.shutdown();
    for s in svcs {
        s.stop();
    }
    for n in c.nodes.drain(..) {
        n.stop();
    }
}
```

The cross-row case (spec §7 integration 4) is covered by the token rule (one log, every row walks every position) and the node unit tests' row-1 ring; add it here only if a two-FSM in-process cluster helper already exists (`uc_node/tests/services.rs` `names(&["count", "fsm1"], None)` is single-node, where every row is trivially caught up, so it would not test anything).

- [ ] **Step 2: Run them**

Run: `cargo test -p uc_node --test read_your_writes -- --test-threads=1`
Expected: PASS (4 tests). If `a_follower_without_its_service…` sees `waited < 800 ms`, the read was not parked: check that the token is `≤ durable` at send time (the wait loop) before suspecting the node.

- [ ] **Step 3: Watch the key test fail with the wait disabled** — prove `a_follower_read_sees…` can fail:

Run: `UC2_MUTATION=skip-min-position-wait UC2_CLIENT_MUTATION=skip-min-position-guard cargo test -p uc_node --features mutation-testing --test read_your_writes a_follower_read_sees -- --test-threads=1`
Expected: FAIL at least once in 5 runs (a follower answers below the acknowledged total). Record the observed failure count in the task report. If it never fails in 5 runs, raise the write count to 500 and retry; report the outcome either way.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add uc_node/tests/read_your_writes.rs
git commit -m "test: read-your-writes on followers, parking, forged tokens, snapshot reads past the gate"
```

---

### Task 8: Remote protocol and client

**Files:**
- Modify: `uc_remote/src/frame.rs` (`:19` version, `:30-38` flags, helpers, frame tests)
- Modify: `uc_remote/src/slots.rs` (`Slot`, `new`, `claim` → `claim_inner` + `claim_with_min_position`, new `kind_and_min_position`)
- Modify: `uc_remote/src/link.rs` (`Link` field `read_token`, `Link::start` literal `:311`, stats cell `stale_answers`, the `FrameType::Response` arm `:1507`)
- Modify: `uc_remote/src/engine.rs` (`Consistency`, `RemoteStats`, `try_query`, `send`, new API)
- Modify: `uc_remote/src/client.rs` (`query_at_least`, `read_token`, `observe`), `uc_remote/src/lib.rs` (`pub use uc_protocol::v2::ipc::ReadToken;`)
- Test: `uc_remote/tests/engine_fake_edge.rs` (append)

**Interfaces:**
- Consumes: Task 1's `ReadToken`.
- Produces: `uc_remote::frame::{FLAG_MIN_POSITION, write_min_position_query, split_min_position_query}`, `PROTOCOL_VERSION = 2`, `uc_remote::Consistency::ReadYourWrites`, `RemoteSendHalf::{read_token, observe, try_query_at_least}`, `RemoteClient::{query_at_least, read_token, observe}`, `RemoteStats::stale_answers`.

- [ ] **Step 1: Write the failing tests**

Extend the fake edge in `uc_remote/tests/common/fake_edge.rs` so it records query frames and can answer one query stale:
- `Behaviour` gets `pub stale_query_once: bool` (default `false`).
- `Observed` gets `pub queries: Mutex<Vec<(u8, Vec<u8>)>>` (flags, payload as received).
- `Action::Respond` gets `stale: bool`.
- In the reader, where a request becomes `Action::Respond` (the final `else` arm, `:432-439`): before building the action, `if h.ty == FrameType::Query { o.queries.lock().unwrap().push((h.flags, payload.to_vec())); }`; keep a per-connection `let mut stale_used = false;` beside `used_once`, and set `stale: h.ty == FrameType::Query && b.stale_query_once && !std::mem::replace(&mut stale_used, true)` — written so it only flips when the condition is a query.
- In `respond`, the `Action::Respond` arm destructures `stale` and writes `position: if stale { 0 } else { seq * 64 }`.

Then append to `uc_remote/tests/engine_fake_edge.rs`:

```rust
fn complete_one(poll: &mut uc_remote::RemotePollHalf) -> (u64, Option<u64>) {
    let deadline = Instant::now() + WAIT;
    let mut got = None;
    while got.is_none() && Instant::now() < deadline {
        poll.poll(|c| {
            if let RemoteOutcome::Response { .. } = c.outcome {
                got = Some((c.user_data, c.position));
            }
        });
    }
    got.expect("a response within WAIT")
}

#[test]
fn a_zero_token_ryw_query_goes_out_as_a_plain_query() {
    let edge = FakeEdge::spawn(Behaviour { credits: 4, ..Default::default() });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    send.try_query(1, Consistency::ReadYourWrites, b"q").unwrap();
    complete_one(&mut poll);
    let q = edge.observed.queries.lock().unwrap().clone();
    assert_eq!(q, vec![(0u8, b"q".to_vec())]);
    send.shutdown();
}

#[test]
fn a_ryw_query_carries_the_token_and_a_stale_answer_is_resent_not_completed() {
    use uc_remote::frame::FLAG_MIN_POSITION;
    let edge = FakeEdge::spawn(Behaviour {
        credits: 4,
        stale_query_once: true,
        ..Default::default()
    });
    let (send, mut poll) = RemoteEngine::connect(cfg(vec![edge.addr.clone()])).unwrap();
    send.try_submit(1, b"w").unwrap(); // seq 1, answered at position 64
    assert_eq!(complete_one(&mut poll), (1, Some(64)));
    assert_eq!(send.read_token().as_u64(), 65, "a write's position + 1");
    send.try_query(2, Consistency::ReadYourWrites, b"q").unwrap(); // seq 2
    let (ud, pos) = complete_one(&mut poll);
    assert_eq!((ud, pos), (2, Some(128)), "completed by the re-send, not the stale answer");
    let q = edge.observed.queries.lock().unwrap().clone();
    let mut want = 65u64.to_le_bytes().to_vec();
    want.extend_from_slice(b"q");
    assert_eq!(q.len(), 2, "sent, answered stale, re-sent in place");
    assert!(q.iter().all(|(f, p)| *f == FLAG_MIN_POSITION && *p == want), "{q:?}");
    assert_eq!(send.stats().stale_answers, 1);
    assert_eq!(send.read_token().as_u64(), 128);
    send.shutdown();
}
```

And in `uc_remote/src/frame.rs`'s tests:

```rust
    #[test]
    fn protocol_v2_and_the_min_position_flag_are_pinned() {
        assert_eq!(PROTOCOL_VERSION, 2);
        assert_eq!(FLAG_MIN_POSITION, 0x20);
        for f in [FLAG_LINEARIZABLE, FLAG_IS_QUERY, FLAG_REPLAYED, FLAG_EXPIRED, FLAG_ENVELOPED] {
            assert_eq!(f & FLAG_MIN_POSITION, 0);
        }
        let mut out = Vec::new();
        write_min_position_query(77, b"q", &mut out);
        assert_eq!(split_min_position_query(&out), Some((77, &b"q"[..])));
        assert_eq!(split_min_position_query(&out[..7]), None);
    }
```

- [ ] **Step 2: Run to see them fail**

Run: `cargo test -p uc_remote`
Expected: compile errors.

- [ ] **Step 3: Implement**

`frame.rs`:

```rust
/// Remote protocol 2 (read-your-writes, spec 2026-10-08 §4.4): QUERY frames
/// may carry [`FLAG_MIN_POSITION`]. A v1 edge would read the prefix as query
/// bytes, so v1 and v2 refuse each other at HELLO.
pub const PROTOCOL_VERSION: u16 = 2;

/// QUERY flag: the payload is `min_position: u64 LE ++ query`, and the edge
/// answers only from state applied at least that far.
pub const FLAG_MIN_POSITION: u8 = 0x20;

pub fn write_min_position_query(min_position: u64, query: &[u8], out: &mut Vec<u8>) {
    out.clear();
    out.reserve(8 + query.len());
    out.extend_from_slice(&min_position.to_le_bytes());
    out.extend_from_slice(query);
}

pub fn split_min_position_query(payload: &[u8]) -> Option<(u64, &[u8])> {
    if payload.len() < 8 {
        return None;
    }
    Some((u64::from_le_bytes(payload[..8].try_into().ok()?), &payload[8..]))
}
```

Update any existing frame test that pins `PROTOCOL_VERSION == 1`.

`slots.rs`: `min_position: AtomicU64` on `Slot` (init 0), `claim` delegates to `claim_inner(…, 0)`, plus `claim_with_min_position(…, min_position)`, plus:

```rust
    /// `(kind, token sent)` for the live generation at `seq`; `None` when the
    /// slot holds another generation or nothing. Read BEFORE `resolve`.
    pub(crate) fn kind_and_min_position(&self, seq: u64) -> Option<(ReqKind, u64)> {
        let s = self.slot(seq);
        if s.owner.load(Ordering::Acquire) != seq + 1 {
            return None;
        }
        let kind = if s.kind.load(Ordering::Relaxed) == ReqKind::Query as u8 {
            ReqKind::Query
        } else {
            ReqKind::Submit
        };
        Some((kind, s.min_position.load(Ordering::Relaxed)))
    }
```

`link.rs`: `pub(crate) read_token: AtomicU64,` on `Link` (init `AtomicU64::new(0)` in `Link::start`), a `stale_answers` cell beside `retries` in the link's stats, and in the `FrameType::Response` arm, before `self.link.slots.resolve(h.seq)`:

```rust
                let (kind, min_sent) = self
                    .link
                    .slots
                    .kind_and_min_position(h.seq)
                    .unwrap_or((crate::slots::ReqKind::Submit, 0));
                if kind == crate::slots::ReqKind::Query && !expired && meta.position < min_sent {
                    // Read-your-writes guard (spec §5.5): an answer from state
                    // older than the token we sent. Do not resolve; re-send in
                    // place after a short backoff, as for a transient RETRY.
                    self.link.stats.stale_answers.fetch_add(1, Ordering::Relaxed);
                    self.link.queue_retransmit(h.seq, STALE_ANSWER_BACKOFF);
                    credit_update(&self.link, self.generation, meta.credits, meta.acked_seq);
                    return Act::Continue;
                }
```

with `const STALE_ANSWER_BACKOFF: Duration = Duration::from_millis(1);` near the other backoff constants; and inside the `Resolve::Won` branch, before `complete`:

```rust
                    if !expired {
                        let seen = match kind {
                            crate::slots::ReqKind::Query => meta.position,
                            crate::slots::ReqKind::Submit => meta.position.saturating_add(1),
                        };
                        self.link.read_token.fetch_max(seen, Ordering::AcqRel);
                    }
```

`engine.rs`: `Consistency::ReadYourWrites` (same doc as Task 5); `RemoteStats::stale_answers` (copied from the cell); `send` gains `min_position: u64` and claims with `slots.claim_with_min_position(…)`; every existing caller passes 0. `try_query`:

```rust
    pub fn try_query(&self, user_data: u64, consistency: Consistency, q: &[u8]) -> Result<(), SubmitError> {
        match consistency {
            Consistency::Linearizable => self.send(FrameType::Query, FLAG_LINEARIZABLE, ReqKind::Query, user_data, q, 0),
            Consistency::Snapshot => self.send(FrameType::Query, 0, ReqKind::Query, user_data, q, 0),
            Consistency::ReadYourWrites => {
                let t = self.link.read_token.load(Ordering::Acquire);
                self.try_query_at_least(user_data, q, ReadToken::from_u64(t))
            }
        }
    }

    pub fn try_query_at_least(&self, user_data: u64, q: &[u8], token: ReadToken) -> Result<(), SubmitError> {
        if token.as_u64() == 0 {
            return self.send(FrameType::Query, 0, ReqKind::Query, user_data, q, 0);
        }
        let mut buf = Vec::new();
        crate::frame::write_min_position_query(token.as_u64(), q, &mut buf);
        self.send(FrameType::Query, FLAG_MIN_POSITION, ReqKind::Query, user_data, &buf, token.as_u64())
    }

    pub fn read_token(&self) -> ReadToken {
        ReadToken::from_u64(self.link.read_token.load(Ordering::Acquire))
    }

    pub fn observe(&self, token: ReadToken) {
        self.link.read_token.fetch_max(token.as_u64(), Ordering::AcqRel);
    }
```

`client.rs`: `query_at_least(&self, q, token) -> Result<Ticket, RemoteError>` via the same `enqueue` loop (add an enqueue variant taking a closure, or a `Query::AtLeast(ReadToken)` arm to its `Option<Consistency>`), and `read_token()` / `observe()` delegating to the locked send half.

Also update `uc_gateway/examples/hop_bench/dummy_edge.rs` if it pins version 1 (it imports `HELLO_REFUSED_VERSION`; it compares against `PROTOCOL_VERSION`, so it follows automatically — verify by building it).

- [ ] **Step 4: Run to see them pass**

Run: `cargo test -p uc_remote && cargo build -p uc_gateway --examples`
Expected: PASS / builds.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add uc_remote uc_gateway/examples
git commit -m "remote: protocol v2, ReadYourWrites queries with a min-position prefix, answer guard re-sends in place"
```

---

### Task 9: Gateway relays the remote token

**Files:**
- Modify: `uc_gateway/src/edge.rs` (`dispatch`, `:1344-1460`)
- Create: `uc_gateway/tests/read_your_writes.rs`

**Interfaces:**
- Consumes: Task 5's `SendHalf::try_query_at_least`; Task 8's `FLAG_MIN_POSITION`, `split_min_position_query`, `RemoteClient::{read_token, observe}`, `Consistency::ReadYourWrites`.
- Produces: nothing new.

- [ ] **Step 1: Write the failing test** — `uc_gateway/tests/read_your_writes.rs`, built on `common::start_cluster` (`uc_gateway/tests/common/mod.rs:188`) and the `roundtrip.rs` shapes (`enc`, `dec`, `read_query`, `remote_config`):

```rust
//! Read-your-writes through two edges: write via the leader's gateway, read
//! via a follower's gateway carrying the writer's token (spec 2026-10-08 §5.4).

use std::time::Duration;

use uc_gateway::{Edge, EdgeConfig, Member};
use uc_lincheck::register::{Cmd, CmdResp};
use uc_remote::{Consistency, RemoteClient, RemoteConfig};

mod common;

fn enc(c: &Cmd) -> Vec<u8> {
    bincode::serde::encode_to_vec(c, bincode::config::standard()).unwrap()
}
fn dec(b: &[u8]) -> CmdResp {
    bincode::serde::decode_from_slice(b, bincode::config::standard()).unwrap().0
}
fn read_query() -> Vec<u8> {
    bincode::serde::encode_to_vec((), bincode::config::standard()).unwrap()
}
fn edge_on(dir: &std::path::Path) -> Edge {
    Edge::start(EdgeConfig {
        instance_dir: dir.to_path_buf(),
        app_id: common::APP.into(),
        listen: "127.0.0.1:0".parse().unwrap(),
        members: vec![Member { node_id: 0, gateway: "127.0.0.1:0".into() }],
        ..EdgeConfig::defaults()
    })
    .unwrap()
}
fn client_to(edge: &Edge) -> RemoteClient {
    RemoteClient::connect(RemoteConfig {
        app_id: common::APP.into(),
        members: vec![edge.local_addr().to_string()],
        request_timeout: Duration::from_secs(20),
        ..Default::default()
    })
    .unwrap()
}

#[test]
fn a_follower_gateway_read_sees_a_write_made_through_the_leaders_gateway() {
    let root = common::tempdir();
    let mut slots = common::start_cluster(root.path(), 3);
    let leader = common::await_single_leader(&slots, 30);
    let follower = (leader + 1) % 3;
    let le = edge_on(&slots[leader].instance_dir);
    let fe = edge_on(&slots[follower].instance_dir);
    let writer = client_to(&le);
    let reader = client_to(&fe);
    for v in 1..=20u64 {
        let r = writer.submit(&enc(&Cmd::Write(v))).unwrap().wait().unwrap();
        assert_eq!(dec(&r.bytes), CmdResp::WriteAck);
        reader.observe(writer.read_token());
        let r = reader
            .query(&read_query(), Consistency::ReadYourWrites)
            .unwrap()
            .wait()
            .unwrap();
        let got: Option<u64> =
            bincode::serde::decode_from_slice(&r.bytes, bincode::config::standard()).unwrap().0;
        assert_eq!(got, Some(v), "the follower gateway's read missed write {v}");
        assert!(r.position >= writer.read_token().as_u64(), "answer below the token");
    }
    writer.shutdown();
    reader.shutdown();
    le.stop();
    fe.stop();
    for s in &mut slots {
        s.stop();
    }
}
```

(`Write(v)` then `Read` on `RegisterSm` returns `Some(v)`; with one writer the latest write is the expected value. If `common::start_cluster`'s services are `Sessioned<RegisterSm>`, keep the edges' default `session_envelope = true` as above.)

Also add the version-refusal test to the same file:

```rust
#[test]
fn a_protocol_v1_client_is_refused_by_name() {
    use uc_remote::frame::{FrameType, HELLO_REFUSED_VERSION, Header, Hello, HelloRefused};
    let root = common::tempdir();
    let mut slots = common::start_cluster(root.path(), 3);
    let leader = common::await_single_leader(&slots, 30);
    let edge = edge_on(&slots[leader].instance_dir);
    let mut c = common::dial_raw(edge.local_addr());
    let mut out = Vec::new();
    Hello { app_id: common::APP }.encode(&mut out);
    c.write_frame(
        Header { ty: FrameType::Hello, flags: 0, version: 1, client_id: 7, seq: 0 },
        &out,
    )
    .unwrap();
    let (_, p) = common::read_until_frame(&mut c, FrameType::HelloRefused, Duration::from_secs(5))
        .expect("a v1 HELLO is refused");
    assert_eq!(HelloRefused::decode(&p).unwrap().reason, HELLO_REFUSED_VERSION);
    edge.stop();
    for s in &mut slots {
        s.stop();
    }
}
```


- [ ] **Step 2: Run to see it fail**

Run: `cargo test -p uc_gateway --test read_your_writes`
Expected: FAIL — the edge does not strip the prefix, so the service gets 8 extra bytes and the read fails to decode, or the answer misses the write.

- [ ] **Step 3: Implement** — in `dispatch`, at the top (after the credit gate), split the prefix for queries and use the stripped payload for every later size and envelope computation:

```rust
    // Read-your-writes (remote protocol 2): strip and keep the min-position
    // prefix. A malformed prefix is a protocol violation by a client that
    // spoke v2 at HELLO; drop the connection rather than guess.
    let (min_token, payload) = if is_query && h.flags & FLAG_MIN_POSITION != 0 {
        match split_min_position_query(payload) {
            Some((t, rest)) => (Some(uc_client::ReadToken::from_u64(t)), rest),
            None => return false,
        }
    } else {
        (None, payload)
    };
```

and replace the query arm of the engine call:

```rust
        let res = if is_query {
            match min_token {
                Some(token) => send.try_query_at_least(user_data, 0, body, token),
                None => {
                    let c = if h.flags & FLAG_LINEARIZABLE != 0 {
                        Consistency::Linearizable
                    } else {
                        Consistency::Snapshot
                    };
                    send.try_query(user_data, body, c)
                }
            }
        } else {
            send.try_submit(user_data, body)
        };
```

Include the 8-byte prefix in the wire-size check: `let wire_len = payload.len() + if envelope { SESSION_HEADER_LEN } else { 0 } + if min_token.is_some() { 9 } else { 0 };` (8 token + the 1-byte service id the engine adds — check how the existing check treats the id byte and match it). Import `FLAG_MIN_POSITION` and `split_min_position_query`.

- [ ] **Step 4: Run to see it pass, and the gateway suite**

Run: `cargo test -p uc_gateway`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add uc_gateway
git commit -m "gateway: relay each remote query's own read-your-writes token"
```

---

### Task 10: Session checker and the capstone with mutation teeth

**Files:**
- Create: `uc_lincheck/src/session.rs`; modify `uc_lincheck/src/lib.rs` (`pub mod session;`)
- Create: `uc_node/tests/read_your_writes_capstone.rs`
- Create: `scripts/ryw_mutation.sh`

**Interfaces:**
- Consumes: Tasks 1–7.
- Produces: `uc_lincheck::session::SessionChecker` with `new()`, `record_write_ack(&mut self, session: u64, value: u64)`, `record_read(&mut self, session: u64, value: u64) -> Result<(), String>`, `violations(&self) -> &[String]`.

- [ ] **Step 1: Write the checker with failing tests** — `uc_lincheck/src/session.rs`:

```rust
// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Session-guarantee checker for a MONOTONE state (a counter that only grows):
//! per session, every read must be at least the session's last acknowledged
//! write (read-your-writes) and at least its previous read (monotonic reads).
//! Linearizability is the wrong test for a deliberately weaker guarantee.

use std::collections::HashMap;

#[derive(Default)]
pub struct SessionChecker {
    floor: HashMap<u64, u64>,
    violations: Vec<String>,
}

impl SessionChecker {
    pub fn new() -> SessionChecker {
        SessionChecker::default()
    }

    /// A write this session had acknowledged left the state at `value`.
    pub fn record_write_ack(&mut self, session: u64, value: u64) {
        let f = self.floor.entry(session).or_insert(0);
        *f = (*f).max(value);
    }

    /// A read this session got returned `value`.
    pub fn record_read(&mut self, session: u64, value: u64) -> Result<(), String> {
        let f = self.floor.entry(session).or_insert(0);
        if value < *f {
            let v = format!("session {session}: read {value} below its floor {}", *f);
            self.violations.push(v.clone());
            return Err(v);
        }
        *f = value;
        Ok(())
    }

    pub fn violations(&self) -> &[String] {
        &self.violations
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_read_below_the_sessions_own_write_is_a_violation() {
        let mut c = SessionChecker::new();
        c.record_write_ack(1, 10);
        assert!(c.record_read(1, 9).is_err());
        assert!(c.record_read(1, 10).is_ok());
        assert_eq!(c.violations().len(), 1);
    }

    #[test]
    fn reads_must_not_go_backwards() {
        let mut c = SessionChecker::new();
        assert!(c.record_read(1, 7).is_ok());
        assert!(c.record_read(1, 6).is_err());
    }

    #[test]
    fn sessions_are_independent() {
        let mut c = SessionChecker::new();
        c.record_write_ack(1, 100);
        assert!(c.record_read(2, 5).is_ok(), "another session's write is not owed");
    }
}
```

Run: `cargo test -p uc_lincheck session` → PASS once the module is wired (`pub mod session;` in `lib.rs`; check whether `lib.rs` gates modules behind features and place it in the always-on set).

- [ ] **Step 2: Write the capstone** — `uc_node/tests/read_your_writes_capstone.rs`, reusing Task 7's harness (copy it; test files do not share modules here unless a `tests/common` exists — check and reuse it if so) plus `cut` and an `unblock` mirror:

```rust
fn heal(nodes: &[Node], a: usize, b: usize, members: &[(u32, SocketAddr)]) {
    for h in nodes[a].partition_handles() {
        h.unblock(members[b].1);
    }
    for h in nodes[b].partition_handles() {
        h.unblock(members[a].1);
    }
}

/// UC2_RYW_TOOTH: unset → the checker must be clean; "T1" (node skips the
/// wait, client guard off) → it must find a violation; "T2" (node skips the
/// wait, guard on) → it must be clean AND the guard must have fired.
#[test]
fn ryw_capstone_under_leader_churn() {
    let tooth = std::env::var("UC2_RYW_TOOTH").ok();
    let mut c = spawn_cluster(3);
    let _ = await_single_leader(&c.nodes, 30);
    let svcs = start_services(&c);
    let checker = std::sync::Arc::new(std::sync::Mutex::new(uc_lincheck::session::SessionChecker::new()));
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stale = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let reads = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

    let workers: Vec<_> = (0..4u64)
        .map(|w| {
            let dirs = c.dirs.clone();
            let (checker, stop, stale, reads) =
                (checker.clone(), stop.clone(), stale.clone(), reads.clone());
            std::thread::spawn(move || {
                let readers: Vec<Client> = dirs.iter().map(|d| Client::connect(d, APP).unwrap()).collect();
                let mut writer: Option<(usize, Client)> = None;
                let mut i = 0usize;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    // Find (or re-find after a failover) a client on the leader.
                    if writer.is_none() {
                        for (k, d) in dirs.iter().enumerate() {
                            if let Ok(cl) = Client::connect(d, APP) {
                                if cl.query_linearizable::<(), u64>(&()).is_ok() {
                                    writer = Some((k, cl));
                                    break;
                                }
                            }
                        }
                        if writer.is_none() {
                            std::thread::sleep(Duration::from_millis(20));
                            continue;
                        }
                    }
                    let (_, wc) = writer.as_ref().unwrap();
                    let total: u64 = match wc.submit(&Cmd::Add(1)) {
                        Ok(t) => t,
                        Err(_) => {
                            writer = None;
                            continue;
                        }
                    };
                    checker.lock().unwrap().record_write_ack(w, total);
                    let token = wc.read_token();
                    let r = &readers[i % readers.len()];
                    i += 1;
                    r.observe(token);
                    let before = r.stats().stale_answers;
                    match r.query_read_your_writes::<(), u64>(&()) {
                        Ok(v) => {
                            reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let _ = checker.lock().unwrap().record_read(w, v);
                        }
                        Err(_) => {} // RETRY / timeout: no answer, nothing to check
                    }
                    stale.fetch_add(r.stats().stale_answers - before, std::sync::atomic::Ordering::Relaxed);
                }
                for r in readers {
                    r.shutdown();
                }
                if let Some((_, wc)) = writer {
                    wc.shutdown();
                }
            })
        })
        .collect();

    // Churn: isolate the current leader for 600 ms every 1.5 s, for 15 s.
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(15) {
        std::thread::sleep(Duration::from_millis(1500));
        if let Some(l) = (0..3).find(|&i| c.nodes[i].can_serve()) {
            for f in (0..3).filter(|&i| i != l) {
                cut(&c.nodes, l, f, &c.members);
            }
            std::thread::sleep(Duration::from_millis(600));
            for f in (0..3).filter(|&i| i != l) {
                heal(&c.nodes, l, f, &c.members);
            }
        }
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for w in workers {
        w.join().unwrap();
    }
    let violations = checker.lock().unwrap().violations().to_vec();
    let stale = stale.load(std::sync::atomic::Ordering::Relaxed);
    let reads = reads.load(std::sync::atomic::Ordering::Relaxed);
    println!("ryw capstone: reads={reads} violations={} stale_answers={stale}", violations.len());
    assert!(reads > 100, "too few answered reads ({reads}) to judge anything");
    match tooth.as_deref() {
        None => assert!(violations.is_empty(), "RYW VIOLATION: {violations:?}"),
        Some("T1") => assert!(!violations.is_empty(), "tooth T1 not caught: the capstone has no teeth"),
        Some("T2") => {
            assert!(violations.is_empty(), "guard on, yet: {violations:?}");
            assert!(stale > 0, "tooth T2: the guard never fired");
        }
        Some(other) => panic!("unknown UC2_RYW_TOOTH {other:?}"),
    }
    for s in svcs {
        s.stop();
    }
    for n in c.nodes.drain(..) {
        n.stop();
    }
}
```

- [ ] **Step 3: Run the clean capstone**

Run: `cargo test -p uc_node --test read_your_writes_capstone -- --nocapture`
Expected: PASS, printing `violations=0`.

- [ ] **Step 4: Write and run the teeth script** — `scripts/ryw_mutation.sh`:

```bash
#!/usr/bin/env bash
# Read-your-writes capstone teeth (spec 2026-10-08, planning erratum 3).
# T1: node skips the wait, client guard off → the capstone must FIND a violation.
# T2: node skips the wait, client guard on  → clean, and the guard must fire.
set -euo pipefail
cd "$(dirname "$0")/.."
run() {
    local tooth="$1"; shift
    echo "== tooth $tooth =="
    env "$@" UC2_RYW_TOOTH="$tooth" \
        cargo test -p uc_node --features mutation-testing \
        --test read_your_writes_capstone -- --nocapture
}
run T1 UC2_MUTATION=skip-min-position-wait UC2_CLIENT_MUTATION=skip-min-position-guard
run T2 UC2_MUTATION=skip-min-position-wait
echo "both teeth caught"
```

Run: `chmod +x scripts/ryw_mutation.sh && scripts/ryw_mutation.sh`
Expected: `both teeth caught`. If T1 is not caught, raise the worker count or duration and record it; a capstone that cannot catch T1 is not done.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add uc_lincheck uc_node/tests/read_your_writes_capstone.rs scripts/ryw_mutation.sh
git commit -m "test: session-guarantee checker and read-your-writes capstone with two mutation teeth"
```

---

### Task 11: Adversarial load test

**Files:**
- Modify: `uc_node/tests/read_your_writes.rs` (append)

**Interfaces:**
- Consumes: Tasks 4–7.

- [ ] **Step 1: Write the test** — append to `uc_node/tests/read_your_writes.rs`:

```rust
/// Smoke, not a gate (dev box): a flood of forged (`u64::MAX`) and
/// at-`durable` tokens against a follower must not stall commit and must
/// never park more than the cap (spec 2026-10-08 §6.4).
#[test]
#[ignore = "smoke: run explicitly; a busy dev box can starve the writer for reasons unrelated to reads"]
fn token_floods_leave_commit_progress_and_the_parked_cap_intact() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use uc_protocol::ring::MpscRing;
    use uc_protocol::v2::ipc::{
        FLAG_V2_MIN_POSITION, MSG_V2_QUERY, extra_client, write_min_position_query_payload,
    };
    let mut c = spawn_cluster(3);
    let leader = await_single_leader(&c.nodes, 30);
    let svcs = start_services(&c);
    let follower = (leader + 1) % 3;
    let writer = Client::connect(&c.dirs[leader], APP).unwrap();
    let commits_in = |secs: u64| {
        let t0 = Instant::now();
        let mut n = 0u64;
        while t0.elapsed() < Duration::from_secs(secs) {
            let _: u64 = writer.submit(&Cmd::Add(1)).unwrap();
            n += 1;
        }
        n
    };
    let quiet = commits_in(3);

    let stop = Arc::new(AtomicBool::new(false));
    let stats = c.nodes[follower].observability().min_position;
    let peak = Arc::new(AtomicU64::new(0));
    let sampler = {
        let (stop, stats, peak) = (stop.clone(), stats.clone(), peak.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                peak.fetch_max(stats.parked.load(Ordering::Relaxed), Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(10));
            }
        })
    };
    let flooders: Vec<_> = (0..8u32)
        .map(|k| {
            let dir = c.dirs[follower].clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let (p, _c) = MpscRing::open(&dir.join("query.ring")).unwrap().into_split();
                let cnc = uc_log::cnc::CncPage::open_file(&dir.join("cnc2.dat"), APP).unwrap();
                let q = bincode::serde::encode_to_vec((), bincode::config::standard()).unwrap();
                let mut payload = Vec::new();
                let (mut seq, mut forged) = (0u32, 0u64);
                while !stop.load(Ordering::Relaxed) {
                    let token = if seq % 2 == 0 {
                        u64::MAX
                    } else {
                        cnc.counters().durable.load_acquire()
                    };
                    write_min_position_query_payload(0, token, &q, &mut payload);
                    let extra = extra_client(0x7000_0000 + k, seq);
                    if p.try_write(MSG_V2_QUERY, FLAG_V2_MIN_POSITION, extra, &payload).is_ok() {
                        if token == u64::MAX {
                            forged += 1;
                        }
                        seq = seq.wrapping_add(1);
                    }
                }
                forged
            })
        })
        .collect();
    let loaded = commits_in(3);
    stop.store(true, Ordering::Relaxed);
    let forged: u64 = flooders.into_iter().map(|h| h.join().unwrap()).sum();
    sampler.join().unwrap();
    std::thread::sleep(Duration::from_millis(500)); // let the node drain the ring
    println!("commits: quiet={quiet} under-flood={loaded}; forged sent={forged}; peak parked={}", peak.load(Ordering::Relaxed));
    assert!(loaded * 2 >= quiet, "commit rate fell below half under the flood");
    assert!(peak.load(Ordering::Relaxed) <= uc_node::min_position::MAX_PARKED_MIN_POSITION_READS as u64);
    assert!(stats.refused_ahead.load(Ordering::Relaxed) >= forged, "every forged token refused, none parked");
    writer.shutdown();
    for s in svcs {
        s.stop();
    }
    for n in c.nodes.drain(..) {
        n.stop();
    }
}
```

- [ ] **Step 2: Run it**

Run: `cargo test -p uc_node --test read_your_writes -- --ignored --nocapture`
Expected: PASS; report the two commit rates it prints.

- [ ] **Step 3: Commit**

```bash
cargo fmt --all
git add uc_node/tests/read_your_writes.rs
git commit -m "test: forged and at-durable token floods leave commit progress and the parked cap intact (smoke)"
```

---

### Task 12: Docs and the full proof stack

**Files:**
- Create: `docs/notes/uc2-read-your-writes-explained.md`
- Modify: `docs/reference/semver-policy.md`, `CLAUDE.md` (Standing facts: cnc 3.5, remote protocol v2; workspace crate notes for `uc_client`/`uc_remote`), `docs/superpowers/specs/2026-10-08-uc2-read-your-writes-design.md` (add an `#### As built` block listing any deviation found while executing)

- [ ] **Step 1: Write the explainer** — plain language, for an application developer: the three read modes (`Linearizable`, `Snapshot`, `ReadYourWrites`) with what each guarantees and where it runs; how the token works (automatic inside one client; `read_token()`/`observe()` across processes, with a cookie example using the hex form); what RETRY means for a read-your-writes read (this node is behind; retry or use another node); the 3.5/v2 version requirements; the forged-token and rebuilt-cluster notes from spec §6.4. Link the spec, `docs/notes/smr-read-options-compared.md` (on branch `bench/read-spread`; say so if it is not on `main` yet) and `docs/benchmarks/uc2-read-spread-2026-10-07.md`.

- [ ] **Step 2: Update the reference docs** — `semver-policy.md`: one entry each for `FLAG_V2_MIN_POSITION` + cnc 3.5 (additive minor; a 3.5 client refuses read-your-writes on a 3.4 node by name), and remote protocol v2 (v1 and v2 refuse each other at HELLO). `CLAUDE.md`: in Standing facts, a bullet stating cnc is 3.5 and the remote protocol is v2 as of this feature (unreleased until the next cut), replacing the sentence "The client↔gateway remote protocol is separate and stays v1".

- [ ] **Step 3: Run the full proof stack** (private target dir throughout)

```bash
export CARGO_TARGET_DIR=$HOME/.cache/cargo-target-session-reads
cargo fmt --all -- --check
cargo build --workspace
cargo build -p uc_lincheck --features replay-bin --bin register-replay && cargo build -p uc_diffreplay
cargo test
cargo test -p uc_node --test lin_v2
cargo test -p uc_node --test lin_partition_v2
cargo test -p uc_node --test read_your_writes -- --test-threads=1
cargo test -p uc_node --test read_your_writes_capstone
scripts/ryw_mutation.sh
cargo test -p uc_crashtest --features hard-crash-tests
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p uc_crashtest --features hard-crash-tests --all-targets -- -D warnings
cargo clippy -p uc_node --features mutation-testing --all-targets -- -D warnings
CARGO_TARGET_DIR=$HOME/.cache/cargo-target-msrv cargo +1.89.0 clippy --workspace --all-targets --locked -- -D warnings
```

Expected: all green. Record each command's result in the task report verbatim (pass/fail counts); a red step is reported as red, not retried until green.

- [ ] **Step 4: Commit**

```bash
git add docs CLAUDE.md
git commit -m "docs: read-your-writes explainer, semver policy, standing facts, spec as-built"
```
