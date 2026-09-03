//! The raw contract and its blanket typed adapter produce the same bytes the
//! v2.5.0 framework produced (position prefix ++ bincode(resp)) — clients
//! built against 2.5.0 keep decoding responses unchanged.
use uc_service::{ApplyCtx, RawStateMachine, StateMachine};

#[derive(serde::Serialize, serde::Deserialize)]
enum Cmd {
    Add(i64),
}
#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
struct Resp {
    value: i64,
    position: u64,
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
    type Response = Resp;
    type Query = Q;
    type QueryResponse = i64;
    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: Cmd) -> Resp {
        match cmd {
            Cmd::Add(n) => self.v += n,
        }
        let position = ctx.position;
        self.last = Some(position);
        Resp {
            value: self.v,
            position,
        }
    }
    fn query(&self, _q: Q) -> i64 {
        self.v
    }
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
}

#[test]
fn typed_sm_is_a_raw_sm_with_byte_identical_wire() {
    let mut sm = Counter::default();
    let cmd_bytes =
        bincode::serde::encode_to_vec(Cmd::Add(5), bincode::config::standard()).unwrap();
    let mut out = Vec::new();
    RawStateMachine::apply(
        &mut sm,
        &mut ApplyCtx::new(4096, Counter::IDENTITY),
        &cmd_bytes,
        &mut out,
    );
    // exactly what v2.5.0's egress encoded after the 8-byte position prefix
    let expected = bincode::serde::encode_to_vec(
        &Resp {
            value: 5,
            position: 4096,
        },
        bincode::config::standard(),
    )
    .unwrap();
    assert_eq!(out, expected);
    assert_eq!(RawStateMachine::last_applied(&sm), Some(4096));

    let q = bincode::serde::encode_to_vec(&Q::Value, bincode::config::standard()).unwrap();
    out.clear();
    RawStateMachine::query(&sm, &q, &mut out);
    assert_eq!(
        out,
        bincode::serde::encode_to_vec(5i64, bincode::config::standard()).unwrap()
    );
}

struct Echo {
    last: Option<u64>,
}
impl RawStateMachine for Echo {
    const NAME: &'static str = "echo";

    fn apply(&mut self, ctx: &mut ApplyCtx, cmd: &[u8], out: &mut Vec<u8>) {
        self.last = Some(ctx.position);
        out.extend_from_slice(cmd);
    }
    fn query(&self, q: &[u8], out: &mut Vec<u8>) {
        out.extend_from_slice(q);
    }
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
}

#[test]
fn raw_sm_sees_the_bytes_untouched() {
    let mut sm = Echo { last: None };
    let mut out = Vec::new();
    RawStateMachine::apply(
        &mut sm,
        &mut ApplyCtx::new(7, Echo::IDENTITY),
        b"\x00\x01raw",
        &mut out,
    );
    assert_eq!(out, b"\x00\x01raw");
}

#[test]
fn ctx_carries_time_and_term_and_collects_requests_in_order() {
    use uc_service::TimerReq;

    let mut ctx = ApplyCtx::for_sm::<Echo>(64).with_time(1_234).with_term(9);
    assert_eq!((ctx.position, ctx.time_ns, ctx.term), (64, 1_234, 9));
    ctx.schedule(7, 5_000);
    ctx.cancel(3);
    ctx.schedule(7, 6_000);
    assert_eq!(
        ctx.timers(),
        &[
            TimerReq::Schedule {
                id: 7,
                at_ns: 5_000
            },
            TimerReq::Cancel { id: 3 },
            TimerReq::Schedule {
                id: 7,
                at_ns: 6_000
            },
        ]
    );
    let ev = uc_service::TimerEvent {
        id: 7,
        deadline_ns: 1_000,
        table: false,
    };
    assert!(ev.late(&ctx), "stamp 1_234 > deadline 1_000");
    let on_time = uc_service::TimerEvent {
        id: 7,
        deadline_ns: 1_234,
        table: false,
    };
    assert!(!on_time.late(&ctx));
}

struct TimerRecorder {
    seen: Vec<(u64, u64, u64, u32)>, // (position, id, deadline, term)
    last: Option<u64>,
}
impl RawStateMachine for TimerRecorder {
    const NAME: &'static str = "timer-recorder";
    fn apply(&mut self, ctx: &mut ApplyCtx, _cmd: &[u8], _out: &mut Vec<u8>) {
        self.last = Some(ctx.position);
    }
    fn query(&self, _q: &[u8], _out: &mut Vec<u8>) {}
    fn last_applied(&self) -> Option<u64> {
        self.last
    }
    fn on_timer(&mut self, ctx: &mut ApplyCtx, ev: uc_service::TimerEvent) {
        self.seen
            .push((ctx.position, ev.id, ev.deadline_ns, ctx.term));
        self.last = Some(ctx.position);
    }
}

#[test]
fn on_timer_defaults_to_a_noop_and_wrappers_forward_it() {
    use uc_service::{SessionConfig, Sessioned, TimerEvent};

    // default: Echo does not override on_timer; calling it is a no-op that compiles
    let mut echo = Echo { last: None };
    let mut ctx = ApplyCtx::for_sm::<Echo>(96).with_time(5);
    RawStateMachine::on_timer(
        &mut echo,
        &mut ctx,
        TimerEvent {
            id: 1,
            deadline_ns: 5,
            table: false,
        },
    );
    // Sessioned forwards and advances its own last_applied
    let mut s = Sessioned::new(
        TimerRecorder {
            seen: vec![],
            last: None,
        },
        SessionConfig::default(),
    );
    let mut ctx = ApplyCtx::new(128, <Sessioned<TimerRecorder> as RawStateMachine>::IDENTITY)
        .with_time(7)
        .with_term(2);
    s.on_timer(
        &mut ctx,
        TimerEvent {
            id: 42,
            deadline_ns: 7,
            table: false,
        },
    );
    assert_eq!(s.last_applied(), Some(128));
    assert_eq!(s.inner().seen, vec![(128, 42, 7, 2)]);
}

#[test]
fn ctx_ids_is_the_only_generator_and_sessioned_forwards_the_context() {
    use uc_service::{ApplyCtx, RawStateMachine, SessionConfig, Sessioned};
    struct Minter {
        seen: Vec<u128>,
        last: Option<u64>,
    }
    impl RawStateMachine for Minter {
        const NAME: &'static str = "minter";
        fn apply(&mut self, ctx: &mut ApplyCtx, _cmd: &[u8], out: &mut Vec<u8>) {
            let mut ids = ctx.ids();
            self.seen.push(ids.next());
            self.last = Some(ctx.position);
            out.extend_from_slice(&ctx.position.to_le_bytes());
        }
        fn query(&self, _q: &[u8], _out: &mut Vec<u8>) {}
        fn last_applied(&self) -> Option<u64> {
            self.last
        }
    }
    let direct = {
        let mut m = Minter {
            seen: vec![],
            last: None,
        };
        let mut out = Vec::new();
        m.apply(&mut ApplyCtx::new(64, Minter::IDENTITY), &[], &mut out);
        m.seen[0]
    };
    let mut s = Sessioned::new(
        Minter {
            seen: vec![],
            last: None,
        },
        SessionConfig::default(),
    );
    let mut cmd = Vec::new();
    cmd.extend_from_slice(&1u64.to_le_bytes()); // client_id
    cmd.extend_from_slice(&1u64.to_le_bytes()); // seq
    let mut out = Vec::new();
    s.apply(
        &mut ApplyCtx::new(64, <Sessioned<Minter> as RawStateMachine>::IDENTITY),
        &cmd,
        &mut out,
    );
    assert_eq!(<Sessioned<Minter> as RawStateMachine>::NAME, "minter");
    assert_eq!(
        s.inner().seen[0],
        direct,
        "same position, same identity → same ID through the wrapper"
    );
    assert_eq!(s.last_applied(), Some(64));
}
