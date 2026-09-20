// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! What one replay captured, on every surface of spec §4.2 that the driver
//! can see directly: per-position responses and schedule records, and the
//! state projection at the origin and at the end.

use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Trace {
    pub row: u8,
    pub version: u32,
    pub origin: u64,
    pub end: u64,
    pub projection_at_origin: Option<String>,
    pub projection_at_end: Option<String>,
    pub entries: Vec<Entry>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub pos: u64,
    pub kind: EntryKind,
    /// First 32 bytes of the command payload. Wide enough to reach the app's
    /// discriminant past a framework envelope (`Sessioned<S>`'s 16-byte
    /// `client_id ‖ seq`); the declaration's `tag_offset` says how much of
    /// this prefix to skip before matching an arm.
    pub tag: Vec<u8>,
    pub response: Vec<u8>,
    pub sched: Vec<Sched>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum EntryKind {
    Message,
    Timer {
        id: u64,
        deadline_ns: u64,
        table: bool,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Sched {
    pub op: String,
    pub id: u64,
    pub deadline_ns: u64,
}

impl Trace {
    pub fn write_json(&self, w: impl Write) -> anyhow::Result<()> {
        Ok(serde_json::to_writer_pretty(w, self)?)
    }
    pub fn read_json(r: impl Read) -> anyhow::Result<Trace> {
        Ok(serde_json::from_reader(r)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn json_roundtrip_preserves_every_field() {
        let t = Trace {
            row: 0,
            version: 1,
            origin: 32,
            end: 96,
            projection_at_origin: Some("value=None\n".into()),
            projection_at_end: Some("value=Some(7)\n".into()),
            entries: vec![
                Entry {
                    pos: 32,
                    kind: EntryKind::Message,
                    tag: vec![0, 7],
                    response: vec![0],
                    sched: vec![],
                },
                Entry {
                    pos: 64,
                    kind: EntryKind::Timer {
                        id: 9,
                        deadline_ns: 5,
                        table: false,
                    },
                    tag: vec![],
                    response: vec![],
                    sched: vec![Sched {
                        op: "schedule".into(),
                        id: 9,
                        deadline_ns: 50,
                    }],
                },
            ],
        };
        let mut buf = Vec::new();
        t.write_json(&mut buf).unwrap();
        assert_eq!(Trace::read_json(&buf[..]).unwrap(), t);
    }
}
