// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The replicated list-append state machine for the elle consistency harness
//! (design spec 2026-07-15). Mirrors `RegisterSm`'s posture exactly: plain
//! in-memory, persists NOTHING — the proof object for service-state
//! reconstruction under node-kill / service-crash / purge churn. `Append` is a
//! Command; `Read` is a linearizable Query. Elle's list-append inference
//! requires each value be appended at most once per key — the driver draws
//! values from one global `AtomicU64`, so uniqueness holds across retries.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LaCmd {
    Append { key: u32, val: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LaResp {
    AppendAck,
}

/// The linearizable read of one key's list.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LaRead {
    pub key: u32,
}

#[derive(Default)]
pub struct ListAppendSm {
    lists: BTreeMap<u32, Vec<u64>>,
    last_applied: Option<u64>,
}

#[cfg(feature = "v2")]
impl uc_service::StateMachine for ListAppendSm {
    const NAME: &'static str = "list-append";

    type Command = LaCmd;
    type Response = LaResp;
    type Query = LaRead;
    type QueryResponse = Vec<u64>;

    fn apply(&mut self, ctx: &mut uc_service::ApplyCtx, cmd: LaCmd) -> LaResp {
        let LaCmd::Append { key, val } = cmd;
        self.lists.entry(key).or_default().push(val);
        self.last_applied = Some(ctx.position);
        LaResp::AppendAck
    }
    fn query(&self, q: LaRead) -> Vec<u64> {
        self.lists.get(&q.key).cloned().unwrap_or_default()
    }
    fn last_applied(&self) -> Option<u64> {
        self.last_applied
    }
}

// The M6 snapshot capability: lets the purge pass drive the REAL purge path.
// `SnapshotHandle = Vec<u8>` (bincode of `(lists, last_applied)`); install
// asserts the payload's recorded position matches the artifact tag — same
// belt-and-suspenders as `RegisterSm`.
#[cfg(feature = "v2")]
impl uc_service::SnapshotStateMachine for ListAppendSm {
    type SnapshotHandle = Vec<u8>;

    fn freeze(&self) -> Result<(Vec<u8>, u64), uc_service::SnapshotError> {
        let buf = bincode::serde::encode_to_vec(
            (&self.lists, self.last_applied),
            bincode::config::standard(),
        )
        .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))?;
        Ok((buf, self.last_applied.unwrap_or(0)))
    }

    fn stream_snapshot(
        handle: Vec<u8>,
        dst: &mut dyn std::io::Write,
    ) -> Result<(), uc_service::SnapshotError> {
        std::io::Write::write_all(dst, &handle)?;
        Ok(())
    }

    fn install_snapshot(
        &mut self,
        position: u64,
        src: &mut dyn std::io::Read,
    ) -> Result<u64, uc_service::SnapshotError> {
        let mut buf = Vec::new();
        std::io::Read::read_to_end(src, &mut buf)?;
        let ((lists, la), _) = bincode::serde::decode_from_slice::<
            (BTreeMap<u32, Vec<u64>>, Option<u64>),
            _,
        >(&buf, bincode::config::standard())
        .map_err(|e| uc_service::SnapshotError::Codec(e.to_string()))?;
        if la.unwrap_or(0) != position {
            return Err(uc_service::SnapshotError::Codec(format!(
                "snapshot payload position {} != requested {position}",
                la.unwrap_or(0)
            )));
        }
        self.lists = lists;
        self.last_applied = Some(position);
        Ok(position)
    }
}

#[cfg(all(test, feature = "v2"))]
mod v2_tests {
    use super::{LaCmd, LaRead, LaResp, ListAppendSm};
    use uc_service::{ApplyCtx, StateMachine};

    // `<ListAppendSm as RawStateMachine>::IDENTITY` is spelled via UFCS at
    // each call site rather than importing `RawStateMachine` — that trait's
    // `apply` would shadow `StateMachine::apply`, making every
    // `sm.apply(...)` call below ambiguous.

    #[test]
    fn apply_query_roundtrip_via_v2_trait() {
        let mut sm = ListAppendSm::default();
        assert_eq!(sm.last_applied(), None);
        assert_eq!(sm.query(LaRead { key: 7 }), Vec::<u64>::new());
        assert_eq!(
            sm.apply(
                &mut ApplyCtx::new(128, <ListAppendSm as uc_service::RawStateMachine>::IDENTITY),
                LaCmd::Append { key: 7, val: 10 }
            ),
            LaResp::AppendAck
        );
        assert_eq!(
            sm.apply(
                &mut ApplyCtx::new(256, <ListAppendSm as uc_service::RawStateMachine>::IDENTITY),
                LaCmd::Append { key: 7, val: 20 }
            ),
            LaResp::AppendAck
        );
        assert_eq!(
            sm.apply(
                &mut ApplyCtx::new(384, <ListAppendSm as uc_service::RawStateMachine>::IDENTITY),
                LaCmd::Append { key: 3, val: 30 }
            ),
            LaResp::AppendAck
        );
        // Per-key append order is the apply order; other keys are untouched.
        assert_eq!(sm.query(LaRead { key: 7 }), vec![10, 20]);
        assert_eq!(sm.query(LaRead { key: 3 }), vec![30]);
        assert_eq!(sm.query(LaRead { key: 99 }), Vec::<u64>::new());
        assert_eq!(sm.last_applied(), Some(384));
    }

    #[test]
    fn snapshot_roundtrip_via_v2_capability() {
        use uc_service::SnapshotStateMachine;

        let mut sm = ListAppendSm::default();
        sm.apply(
            &mut ApplyCtx::new(
                4096,
                <ListAppendSm as uc_service::RawStateMachine>::IDENTITY,
            ),
            LaCmd::Append { key: 1, val: 42 },
        );
        let (handle, s) = sm.freeze().unwrap();
        assert_eq!(s, 4096);
        let mut bytes = Vec::new();
        ListAppendSm::stream_snapshot(handle, &mut bytes).unwrap();

        let mut restored = ListAppendSm::default();
        assert_eq!(
            restored
                .install_snapshot(4096, &mut bytes.as_slice())
                .unwrap(),
            4096
        );
        assert_eq!(restored.query(LaRead { key: 1 }), vec![42]);
        assert_eq!(restored.last_applied(), Some(4096));

        // A mis-tagged install (wrong artifact position) is refused.
        assert!(
            restored
                .install_snapshot(99, &mut bytes.as_slice())
                .is_err()
        );
    }
}
