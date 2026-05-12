//! Fluent builder for [`StoreStateMachine`].

use serde::{Serialize, de::DeserializeOwned};
use ultima_db::{ReadTx, Store, WriteTx};

use super::store_state_machine::{ApplyFn, QueryFn, StoreStateMachine};

pub struct StoreStateMachineBuilder<C, R, Q, QR>
where
    C: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    Q: Serialize + DeserializeOwned + Send + Sync + 'static,
    QR: Serialize + DeserializeOwned + Send + 'static,
{
    store: Store,
    apply_fn: Option<ApplyFn<C, R>>,
    query_fn: Option<QueryFn<Q, QR>>,
}

impl<C, R, Q, QR> StoreStateMachine<C, R, Q, QR>
where
    C: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    Q: Serialize + DeserializeOwned + Send + Sync + 'static,
    QR: Serialize + DeserializeOwned + Send + 'static,
{
    /// Start a fluent builder. The store must already be opened by the
    /// caller; the adapter does not take ownership of `StoreConfig`.
    pub fn builder(store: Store) -> StoreStateMachineBuilder<C, R, Q, QR> {
        StoreStateMachineBuilder {
            store,
            apply_fn: None,
            query_fn: None,
        }
    }
}

impl<C, R, Q, QR> StoreStateMachineBuilder<C, R, Q, QR>
where
    C: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + DeserializeOwned + Send + 'static,
    Q: Serialize + DeserializeOwned + Send + Sync + 'static,
    QR: Serialize + DeserializeOwned + Send + 'static,
{
    /// Closure invoked from `StateMachine::apply`. Sees the open
    /// `WriteTx` pinned to the raft `log_index`; commits automatically
    /// after the closure returns.
    pub fn apply_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(&mut WriteTx, C) -> R + Send + Sync + 'static,
    {
        self.apply_fn = Some(Box::new(f));
        self
    }

    /// Closure invoked from `StateMachine::query`. Sees a `ReadTx` over the
    /// latest committed version.
    pub fn query_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(&ReadTx, Q) -> QR + Send + Sync + 'static,
    {
        self.query_fn = Some(Box::new(f));
        self
    }

    pub fn build(self) -> Result<StoreStateMachine<C, R, Q, QR>, BuildError> {
        Ok(StoreStateMachine {
            store: self.store,
            apply_fn: self.apply_fn.ok_or(BuildError::MissingApplyFn)?,
            query_fn: self.query_fn.ok_or(BuildError::MissingQueryFn)?,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("missing apply_fn")]
    MissingApplyFn,
    #[error("missing query_fn")]
    MissingQueryFn,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StateMachine;

    #[test]
    fn missing_apply_fn_rejected() {
        let store = Store::default();
        let r = StoreStateMachine::<u8, u8, u8, u8>::builder(store)
            .query_fn(|_tx, q| q)
            .build();
        assert!(matches!(r, Err(BuildError::MissingApplyFn)));
    }

    #[test]
    fn missing_query_fn_rejected() {
        let store = Store::default();
        let r = StoreStateMachine::<u8, u8, u8, u8>::builder(store)
            .apply_fn(|_tx, c| c)
            .build();
        assert!(matches!(r, Err(BuildError::MissingQueryFn)));
    }

    #[test]
    fn apply_pins_log_index_into_latest_version() {
        // The closure body doesn't touch the WriteTx — we only verify that
        // begin_write(Some(log_index)) + commit succeeds and that
        // last_applied() reports the log_index that was just committed.
        let mut sm = StoreStateMachine::<u32, u32, u32, u32>::builder(Store::default())
            .apply_fn(|_tx, cmd| cmd.wrapping_add(1))
            .query_fn(|_tx, q| q.wrapping_mul(2))
            .build()
            .expect("build");

        // Fresh store: no writes yet.
        assert_eq!(sm.last_applied(), None);

        let resp = sm.apply(7, 41);
        assert_eq!(resp, 42);
        assert_eq!(sm.last_applied(), Some(7));

        // Reads see the latest committed version (no panic).
        assert_eq!(sm.query(5), 10);

        let resp = sm.apply(8, 100);
        assert_eq!(resp, 101);
        assert_eq!(sm.last_applied(), Some(8));
    }
}
