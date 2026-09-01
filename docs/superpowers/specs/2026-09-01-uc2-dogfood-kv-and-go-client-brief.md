# Dogfood: KV example service + Go client — decisions so far (PARKED)

**Date:** 2026-09-01
**Status:** PARKED after the first brainstorming pass. The maintainer
paused this to add features to UC first. Everything below is a *decision
already taken* or a *design section already presented*; nothing is built.
Pick up from "Where the design stopped". Backlog entry:
`docs/BACKLOG.md` direction 1.

## Decisions taken (answered by the maintainer, 2026-09-01)

| Question | Decision |
|---|---|
| Reference service | **Sessioned KV store** (over order book / ledger). |
| Second-language client | **Go**, in a **separate repo** (not `clients/go/` in-tree). |
| Primary bar for the whole direction | **Docs sufficiency**: a client written from `docs/reference/remote-protocol.md` alone, and a service written from the service-side docs alone, pass conformance + linearizability. Every doc gap is a logged defect. Performance and correctness-under-faults are tested but do not decide FAIL. |
| Multi-service | **Single FSM first.** The remote FSM-selector gap (`SUBMIT`/`QUERY` carry no selector) stays recorded, not solved. |
| Approach | **A — clean-room, docs-first**: raw-tier KV with a hand-specified byte format; an in-tree **conformance harness** (fake-edge binary + scenario files over real TCP) so any port can be adjudicated; Go client built clean-room. Rejected: B (typed tier + a Go bincode port — UC would be promising a wire format it does not own) and C (Go client tested only against a live cluster — promises not adjudicable). |
| Split | **Two separate efforts, KV first.** The Go client's live capstone needs a service whose wire format is not bincode's, so the KV must exist first, and its byte format is designed with a second-language reader in mind. The conformance harness belongs to the Go-client effort but lives in this repo. |
| KV built clean-room too | **Yes.** An agent given only the published docs (`write-a-service-binary`, `state-machine-contract`, `instance-directory`, `configuration`, `bound-journal-growth`) and the released binaries, never `uc_service` source. Makes the KV effort a docs-sufficiency test of the service-side docs. |
| KV operation set | **Put / Get / Delete / CAS + Append.** Register ops give the WGL checker; list-append on a key gives the Elle tier. Rejected: range/TTL/prefix (unbounded responses collide with the payload ceiling; TTL needs a clock apply cannot have). |

## Design sections presented (batch 1; maintainer had not yet confirmed)

### 1. Scope and placement

`examples/kv` beside `counter`, `publish = false`. One library (codec, state
machine, thin client wrapper) + `kv-service` and `kv-client` (shmem or
gateway) binaries. Built outside the repo against the crates.io `2.10.0`
crates during the clean-room phase, moved in with path deps afterwards.
Single FSM, sessions on, `SnapshotStateMachine` implemented so purge can be
enabled.

### 2. Wire format (the artifact a Go client reads)

Fixed little-endian, explicit length prefixes, versioned; documented on its
own reference page.

```
command  = u8 ver(=1) ++ u8 op ++ u16 klen ++ key ++ body
  PUT     body = u16 vlen ++ value
  DELETE  body = (empty)
  CAS     body = u64 expected_version ++ u16 vlen ++ value     (0 = must not exist)
  APPEND  body = u16 elen ++ element
query    = u8 ver ++ u8 op(GET) ++ u16 klen ++ key

write response = u8 status ++ u64 version
GET response   = u8 status ++ u64 version ++ u8 kind ++ payload
  kind BYTES: u16 vlen ++ value
  kind LIST:  u16 count ++ (u16 elen ++ element)*
status: 0 OK, 1 NOT_FOUND, 2 VERSION_MISMATCH, 3 WRONG_KIND, 4 MALFORMED, 5 TOO_LARGE
```

- A key's **version = log position of its last write** (free, stable,
  unique); CAS compares versions, not values (the etcd shape; keeps CAS
  small).
- The session envelope's 16 B header / 1 B tag are outside this format; the
  gateway lifts the tag into frame flags so a remote client never sees it.
- **Caps are derived from the payload ceiling, not chosen:** key ≤ 256 B,
  value ≤ 1024 B, a list's total element bytes ≤ 1024 B. Largest command
  (CAS) = 6 + 8 + 256 + 1024 = 1294 B, + 16 B envelope = 1310 B ≤ 1312 B
  crypto-on ceiling (2 B to spare). The reference page shows the arithmetic.

### 3. State machine

`BTreeMap<Vec<u8>, Entry>` with `Entry { version, kind, bytes | list }`
(never a `HashMap`). Put overwrites any kind; Append on a BYTES key is
`WRONG_KIND`. A command that fails to decode gets `MALFORMED` and never
panics the apply thread — apply is total. Snapshot format = one version byte
+ entry count + entries using the GET field encoding (one encoding
documented, not two). `freeze` clones the map under the lock — O(n), not the
trait's O(1), stated honestly on the page with the immutable-map alternative
named. Values are opaque bytes; no TTL.

## Where the design stopped

Batch 2 was not presented. It was going to cover:

- **Clients:** a Rust `kv` client wrapper over `uc_client::Engine` (local)
  and `uc_remote::RemoteClient` (remote); the CLI.
- **Clean-room protocol:** builder = a subagent with a directory holding
  only the allowed doc pages, the release tarball binaries and `cargo doc`
  output of the published crates; forbidden = the repo tree and
  `~/.cargo/registry/src` reads; enforcement = instruction + transcript
  audit of paths read; every unanswerable question goes to a `DEFECTS.md`
  ledger with the assumption the builder proceeded on (never stop). After
  the build: review the ledger, fix the docs in-tree, re-verify against the
  builder's assumptions.
- **Verification:** codec round-trip + malformed-never-panics tests;
  per-key WGL capstone through `RemoteClient` under leader kills
  (linearizability is compositional, so per-key checks are sound — model
  `examples/uc_crashtest/tests/remote_lin.rs`, parameterised on the service
  binary and encoding); Elle list-append driven **through the remote path**
  (new history writer; today `uc_node/tests/elle_v2.rs` drives
  `ListAppendSm` in-process over shmem); snapshot + purge churn (check
  whether `LinClusterV2<KvSm>` works for a raw-tier SM); hard-crash reuse
  if cheap.
- **Pre-committed bars:** B1 docs sufficiency — the clean-room build
  reaches green on all capstones with **zero** source reads (audited);
  ledger count reported, not barred; a wrong guess that fails a capstone is
  a *blocking* doc defect. B2 correctness — WGL Linearizable, Elle clean
  under `strong-serializable`, 0 acked-write loss. B3 performance —
  **reported, no bar** (KV through the remote client vs the register
  `remote_lin` rate; fleet row optional).
- **Docs deliverables:** the KV wire-format reference page, a how-to or
  tutorial, the `RELEASES.md` entry.

## Facts gathered that the pickup should not re-derive

- `docs/reference/remote-protocol.md` already says it is "the page a
  non-Rust port implements from" and has a "Failover promises (what a
  conforming client implements)" section — the port target exists; the
  adjudicator (a process-boundary conformance harness) does not.
  `uc_remote/tests/client_fake_edge.rs` is the in-process Rust ancestor.
- The typed tier's wire format is `bincode::config::standard()` over the
  service's serde types (`docs/reference/state-machine-contract.md`); the
  raw tier is "you own the wire format". `examples/counter` is typed, so the
  Go client cannot use it as its capstone service.
- `Sessioned<S>` wraps either tier; envelope = 16 B (`client_id`, `seq`)
  LE; response tag 1 B; `SessionConfig` is replicated state (flag day).
- `LinClusterV2<SM>` (used by `elle_v2.rs`, `lin_v2.rs`) is generic over
  the state machine — the in-process capstones may come nearly free.
