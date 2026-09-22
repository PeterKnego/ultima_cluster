//! The typed tier's decode must consume the WHOLE payload (#49).
//!
//! `bincode::serde::decode_from_slice` does not require consuming its
//! buffer: a payload whose encoded prefix parses as the target type decodes
//! successfully and the trailing bytes are dropped in silence. Four of the
//! five schema-skew shapes measured in the issue leave `bytes_read < len`, so
//! a length check turns them into the fail-stop the blanket impl already
//! intends. Every slice that reaches the typed decoder is exact (the log
//! reader slices to the header's `length`, `Sessioned` strips its 16-byte
//! envelope, `Timed` forwards untouched, the query agent strips its 8-byte
//! epoch prefix), so the check cannot fire on a correct frame — the
//! `Sessioned` pair below pins that.
use uc_service::{
    ApplyCtx, OutputError, OutputHandler, RawOutputHandler, RawStateMachine, SessionConfig,
    Sessioned, StateMachine, Timed, TypedOutput,
};

fn enc<T: serde::Serialize>(v: &T) -> Vec<u8> {
    bincode::serde::encode_to_vec(v, bincode::config::standard()).unwrap()
}

#[derive(serde::Serialize, serde::Deserialize)]
enum Cmd {
    Add(i64),
}
#[derive(serde::Serialize, serde::Deserialize)]
enum Q {
    Value,
}

#[derive(Default)]
struct Counter {
    v: i64,
    last: Option<u64>,
}
impl StateMachine for Counter {
    const NAME: &'static str = "counter";

    type Command = Cmd;
    type Response = i64;
    type Query = Q;
    type QueryResponse = i64;
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Cmd) -> i64 {
        match cmd {
            Cmd::Add(n) => self.v += n,
        }
        self.last = Some(ctx.position);
        self.v
    }
    fn query(&self, _q: Q) -> i64 {
        self.v
    }
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
}

#[test]
#[should_panic(expected = "trailing bytes")]
fn apply_fail_stops_on_trailing_bytes() {
    let mut sm = Counter::default();
    let mut bytes = enc(&Cmd::Add(5));
    bytes.push(0xEE);
    let mut out = Vec::new();
    RawStateMachine::apply(
        &mut sm,
        &mut ApplyCtx::new(4096, Counter::IDENTITY),
        &bytes,
        &mut out,
    );
}

#[test]
#[should_panic(expected = "trailing bytes")]
fn query_fail_stops_on_trailing_bytes() {
    let sm = Counter::default();
    let mut bytes = enc(&Q::Value);
    bytes.push(0xEE);
    let mut out = Vec::new();
    RawStateMachine::query(&sm, &bytes, &mut out);
}

struct Sink;
impl OutputHandler<Counter> for Sink {
    async fn on_committed(
        &self,
        _position: u64,
        _cmd: &Cmd,
        _state: &Counter,
    ) -> Result<(), OutputError> {
        Ok(())
    }
}

#[test]
#[should_panic(expected = "trailing bytes")]
fn on_committed_fail_stops_on_trailing_bytes() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let handler = TypedOutput(Sink);
    let sm = Counter::default();
    let mut bytes = enc(&Cmd::Add(5));
    bytes.push(0xEE);
    let _ = rt.block_on(RawOutputHandler::on_committed(&handler, 4096, &bytes, &sm));
}

fn session_frame(body: &[u8]) -> Vec<u8> {
    let mut f = Vec::new();
    f.extend_from_slice(&1u64.to_le_bytes()); // client_id
    f.extend_from_slice(&1u64.to_le_bytes()); // seq
    f.extend_from_slice(body);
    f
}

/// The wrapper hands the exact remainder down: a correct envelope-wrapped
/// frame applies, so the check has no false positive through `Sessioned`.
#[test]
fn sessioned_exact_body_applies_through_the_wrapper() {
    let mut s = Sessioned::new(Counter::default(), SessionConfig::default());
    let mut out = Vec::new();
    s.apply(
        &mut ApplyCtx::new(64, <Sessioned<Counter> as RawStateMachine>::IDENTITY),
        &session_frame(&enc(&Cmd::Add(5))),
        &mut out,
    );
    assert_eq!(s.inner().v, 5);
    // TAG_FRESH ++ bincode(5i64)
    assert_eq!(&out[1..], enc(&5i64).as_slice());
}

#[test]
#[should_panic(expected = "trailing bytes")]
fn sessioned_overlong_body_fail_stops() {
    let mut s = Sessioned::new(Counter::default(), SessionConfig::default());
    let mut body = enc(&Cmd::Add(5));
    body.push(0xEE);
    let mut out = Vec::new();
    s.apply(
        &mut ApplyCtx::new(64, <Sessioned<Counter> as RawStateMachine>::IDENTITY),
        &session_frame(&body),
        &mut out,
    );
}

/// The issue's worst measured shape: a variant inserted mid-enum. The OLD
/// binary's `Put(11, 22)` encodes as `[1, 11, 22]`; the NEW enum's index 1 is
/// the inserted `Get(u32)`, which parses `[1, 11]` and leaves one byte — so
/// without the check the new build applies a `Get(11)` nobody sent.
#[derive(serde::Serialize, serde::Deserialize)]
enum OldKv {
    Delete(u32),
    Put(u32, u32),
}
#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
enum NewKv {
    Delete(u32),
    Get(u32),
    Put(u32, u32),
}
#[derive(Default)]
struct Recorder {
    applied: Vec<NewKv>,
    last: Option<u64>,
}
impl StateMachine for Recorder {
    const NAME: &'static str = "recorder";

    type Command = NewKv;
    type Response = ();
    type Query = ();
    type QueryResponse = ();
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: NewKv) {
        self.applied.push(cmd);
        self.last = Some(ctx.position);
    }
    fn query(&self, _q: ()) {}
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
}

#[test]
#[should_panic(expected = "trailing bytes")]
fn inserted_variant_fail_stops_instead_of_applying_a_command_nobody_sent() {
    let old = enc(&OldKv::Put(11, 22));
    assert_eq!(old, [1, 11, 22]);
    let mut sm = Recorder::default();
    let mut out = Vec::new();
    RawStateMachine::apply(
        &mut sm,
        &mut ApplyCtx::new(32, Recorder::IDENTITY),
        &old,
        &mut out,
    );
    // Reached only while the defect is present: the misparse the issue measured.
    assert_eq!(sm.applied, vec![NewKv::Get(11)]);
}

/// The `Timed` leg of the trace: the wrapper forwards the slice untouched, so
/// an exact payload applies through it and an over-long one fail-stops.
#[test]
fn timed_exact_body_applies_through_the_wrapper() {
    let mut t = Timed::new(Counter::default());
    let mut out = Vec::new();
    t.apply(
        &mut ApplyCtx::new(64, <Timed<Counter> as RawStateMachine>::IDENTITY),
        &enc(&Cmd::Add(5)),
        &mut out,
    );
    assert_eq!(out, enc(&5i64));
}

#[test]
#[should_panic(expected = "trailing bytes")]
fn timed_overlong_body_fail_stops() {
    let mut t = Timed::new(Counter::default());
    let mut body = enc(&Cmd::Add(5));
    body.push(0xEE);
    let mut out = Vec::new();
    t.apply(
        &mut ApplyCtx::new(64, <Timed<Counter> as RawStateMachine>::IDENTITY),
        &body,
        &mut out,
    );
}

/// The `read == 0` shape: a command type that consumes nothing. Any
/// non-empty payload is trailing bytes for it — the shape that felled the two
/// `type Command = ()` gate harnesses, which used to submit zero-filled
/// blocks as ballast.
#[derive(Default)]
struct Unit {
    n: u64,
    last: Option<u64>,
}
impl StateMachine for Unit {
    const NAME: &'static str = "unit";

    type Command = ();
    type Response = ();
    type Query = ();
    type QueryResponse = ();
    fn apply(&mut self, ctx: &mut ApplyCtx, _cmd: ()) {
        self.n += 1;
        self.last = Some(ctx.position);
    }
    fn query(&self, _q: ()) {}
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
}

#[test]
fn unit_command_applies_an_empty_payload() {
    let mut sm = Unit::default();
    let mut out = Vec::new();
    RawStateMachine::apply(
        &mut sm,
        &mut ApplyCtx::new(32, Unit::IDENTITY),
        &[],
        &mut out,
    );
    assert_eq!(sm.n, 1);
}

#[test]
#[should_panic(expected = "decoded 0 of 1")]
fn unit_command_fail_stops_on_any_byte_at_all() {
    let mut sm = Unit::default();
    let mut out = Vec::new();
    RawStateMachine::apply(
        &mut sm,
        &mut ApplyCtx::new(32, Unit::IDENTITY),
        &[0],
        &mut out,
    );
}
