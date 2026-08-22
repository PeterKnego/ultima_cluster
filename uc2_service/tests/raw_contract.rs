//! The raw contract and its blanket typed adapter produce the same bytes the
//! v2.5.0 framework produced (position prefix ++ bincode(resp)) — clients
//! built against 2.5.0 keep decoding responses unchanged.
use uc2_service::{RawStateMachine, StateMachine};

#[derive(serde::Serialize, serde::Deserialize)]
enum Cmd { Add(i64) }
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
struct Resp { value: i64, position: u64 }
#[derive(serde::Serialize, serde::Deserialize)]
enum Q { Value }

#[derive(Default)]
struct Counter { v: i64, last: Option<u64> }
impl StateMachine for Counter {
    type Command = Cmd; type Response = Resp; type Query = Q; type QueryResponse = i64;
    fn apply(&mut self, position: u64, cmd: Cmd) -> Resp {
        match cmd { Cmd::Add(n) => self.v += n }
        self.last = Some(position);
        Resp { value: self.v, position }
    }
    fn query(&self, _q: Q) -> i64 { self.v }
    fn last_applied(&self) -> Option<u64> { self.last }
}

#[test]
fn typed_sm_is_a_raw_sm_with_byte_identical_wire() {
    let mut sm = Counter::default();
    let cmd_bytes = bincode::serde::encode_to_vec(Cmd::Add(5), bincode::config::standard()).unwrap();
    let mut out = Vec::new();
    RawStateMachine::apply(&mut sm, 4096, &cmd_bytes, &mut out);
    // exactly what v2.5.0's egress encoded after the 8-byte position prefix
    let expected = bincode::serde::encode_to_vec(&Resp { value: 5, position: 4096 }, bincode::config::standard()).unwrap();
    assert_eq!(out, expected);
    assert_eq!(RawStateMachine::last_applied(&sm), Some(4096));

    let q = bincode::serde::encode_to_vec(&Q::Value, bincode::config::standard()).unwrap();
    out.clear();
    RawStateMachine::query(&sm, &q, &mut out);
    assert_eq!(out, bincode::serde::encode_to_vec(5i64, bincode::config::standard()).unwrap());
}

struct Echo { last: Option<u64> }
impl RawStateMachine for Echo {
    fn apply(&mut self, position: u64, cmd: &[u8], out: &mut Vec<u8>) { self.last = Some(position); out.extend_from_slice(cmd); }
    fn query(&self, q: &[u8], out: &mut Vec<u8>) { out.extend_from_slice(q); }
    fn last_applied(&self) -> Option<u64> { self.last }
}

#[test]
fn raw_sm_sees_the_bytes_untouched() {
    let mut sm = Echo { last: None };
    let mut out = Vec::new();
    RawStateMachine::apply(&mut sm, 7, b"\x00\x01raw", &mut out);
    assert_eq!(out, b"\x00\x01raw");
}
