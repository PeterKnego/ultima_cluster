//! Shared types for the `counter_loop` benchmark.
//!
//! - `Cmd` / `Resp` / `Query` / `QueryResp`: the bincode-serialisable
//!   command/query shapes consumed by the state machine.
//! - `Counter`: the ultima_db record (one row per named counter).
//! - `build_state_machine`: factory for the `StoreStateMachine` wired with
//!   a unique-`by_name` index, used by both the in-process node (degenerate
//!   snapshot-only role under shmem mode) and the service.
//! - `proto`: the generated tonic bindings.

use serde::{Deserialize, Serialize};
use ultima_db::{IndexKind, Store};
use uc_service::ultima_db::StoreStateMachine;

pub mod proto {
    tonic::include_proto!("counter_loop");
}

pub const TABLE_NAME: &str = "counters";
pub const INDEX_NAME: &str = "by_name";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Cmd {
    Inc { name: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Resp {
    pub name: String,
    pub value: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Query {
    Get(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryResp {
    pub value: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Counter {
    pub name: String,
    pub value: u64,
}

pub type CounterSm = StoreStateMachine<Cmd, Resp, Query, QueryResp>;

/// Build a `StoreStateMachine` backed by a fresh in-memory `ultima_db::Store`.
///
/// `define_index` is called inside `apply_fn` so the unique-name index exists
/// from the first write onward; ultima_db treats repeat definitions of the
/// same (name, kind) as a no-op.
pub fn build_state_machine() -> CounterSm {
    StoreStateMachine::builder(Store::default())
        .apply_fn(|tx, cmd| {
            let Cmd::Inc { name } = cmd;
            let mut table = tx
                .open_table::<Counter>(TABLE_NAME)
                .expect("open_table apply");
            table
                .define_index(INDEX_NAME, IndexKind::Unique, |c: &Counter| c.name.clone())
                .expect("define_index by_name");

            let new_value = match table
                .get_unique::<String>(INDEX_NAME, &name)
                .expect("get_unique by_name")
            {
                Some((id, existing)) => {
                    let v = existing.value + 1;
                    table
                        .update(
                            id,
                            Counter {
                                name: name.clone(),
                                value: v,
                            },
                        )
                        .expect("update counter row");
                    v
                }
                None => {
                    table
                        .insert(Counter {
                            name: name.clone(),
                            value: 1,
                        })
                        .expect("insert counter row");
                    1
                }
            };

            Resp {
                name,
                value: new_value,
            }
        })
        .query_fn(|tx, q| {
            let Query::Get(name) = q;
            let table = tx
                .open_table::<Counter>(TABLE_NAME)
                .expect("open_table query");
            let value = table
                .get_unique::<String>(INDEX_NAME, &name)
                .expect("get_unique query")
                .map(|(_id, c)| c.value);
            QueryResp { value }
        })
        .build()
        .expect("StoreStateMachine build")
}
