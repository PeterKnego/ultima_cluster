// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Freeze → stream → install roundtrip for the `ultima_db` `StoreStateMachine`
//! adapter, proving the position-as-version lockstep across a wholesale snapshot
//! install at a sparse byte position. Gated on the `ultima_db` feature.

#![cfg(feature = "ultima_db")]

use serde::{Deserialize, Serialize};
use ultima_db::{Persistence, Store, StoreConfig};
use uc2_service::SnapshotStateMachine;
use uc2_service::StateMachine;
use uc2_service::ultima_db::StoreStateMachine;

// A minimal KV command/query vocabulary over a single `ultima_db` table. Rows
// carry the string key; `get` scans for it (the store auto-assigns row IDs).

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
enum Cmd {
    Put(String, u64),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
enum Query {
    Get(String),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct KvRow {
    key: String,
    val: u64,
}

const TABLE: &str = "kv";

type Sm = StoreStateMachine<Cmd, (), Query, Option<u64>>;

fn kv_store_sm(dir: std::path::PathBuf) -> Sm {
    // Smr (checkpoint-only) persistence: the SMR log provides durability, and
    // `install_snapshot`'s post-install `checkpoint()` needs a real dir.
    let cfg = StoreConfig::builder().persistence(Persistence::smr(dir)).build();
    let store = Store::new(cfg).expect("open store");
    store.register_table::<KvRow>(TABLE).expect("register table");
    StoreStateMachine::builder(store)
        .apply_fn(|tx, cmd| {
            let Cmd::Put(k, v) = cmd;
            let mut t = tx.open_table::<KvRow>(TABLE).unwrap();
            t.insert(KvRow { key: k, val: v }).unwrap();
        })
        .query_fn(|tx, q| {
            let Query::Get(k) = q;
            let t = tx.open_table::<KvRow>(TABLE).unwrap();
            t.iter().find(|(_, r)| r.key == k).map(|(_, r)| r.val)
        })
        .build()
        .expect("build sm")
}

#[test]
fn store_sm_freeze_stream_install_roundtrip_at_position() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = kv_store_sm(dir.path().join("a"));
    // positions are sparse byte positions — apply at 96, 192, 4096
    for (pos, k, v) in [(96u64, "a", 1u64), (192, "b", 2), (4096, "c", 3)] {
        sm.apply(pos, Cmd::Put(k.into(), v));
    }
    assert_eq!(sm.last_applied(), Some(4096));

    let (handle, s) = sm.freeze().unwrap();
    assert_eq!(s, 4096);
    let mut buf = Vec::new();
    Sm::stream_snapshot(handle, &mut buf).unwrap();

    // Fresh, independent store: install the snapshot at the tagged position.
    let mut fresh = kv_store_sm(dir.path().join("b"));
    assert_eq!(fresh.last_applied(), None, "fresh store starts empty");
    let installed = fresh.install_snapshot(4096, &mut buf.as_slice()).unwrap();
    assert_eq!(installed, 4096);
    assert_eq!(fresh.last_applied(), Some(4096), "position-as-version lockstep");
    assert_eq!(fresh.query(Query::Get("c".into())), Some(3));
    assert_eq!(fresh.query(Query::Get("a".into())), Some(1));
    assert_eq!(fresh.query(Query::Get("missing".into())), None);

    // A subsequent apply at a still-higher position lands cleanly (the version
    // counter was advanced past the installed position).
    fresh.apply(8192, Cmd::Put("d".into(), 4));
    assert_eq!(fresh.last_applied(), Some(8192));
    assert_eq!(fresh.query(Query::Get("d".into())), Some(4));
}
