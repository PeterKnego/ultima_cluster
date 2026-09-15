//! Sequential specification of the object under test. Pure — no cluster deps.

use std::hash::Hash;

/// A deterministic sequential spec: given a state and an operation, return the
/// next state and the response a correct single-threaded implementation produces.
pub trait Model {
    type State: Clone + Eq + Hash;
    type Op: Clone;
    type Resp: Clone + Eq + std::fmt::Debug;
    fn init() -> Self::State;
    fn step(state: &Self::State, op: &Self::Op) -> (Self::State, Self::Resp);
    /// `true` for an op that never changes the state. The checker drops an
    /// INDETERMINATE read before searching (it carries no information),
    /// while an indeterminate mutation stays in as an optional op — so the
    /// model, not the checker, has to say which is which.
    fn is_read(op: &Self::Op) -> bool;
}

/// Abstract op against the CAS register (shared with `history`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Write(u64),
    Read,
    Cas { old: u64, new: u64 },
}

/// Abstract response (shared with `history`). `Value` carries the read result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegResp {
    Ack,
    Value(Option<u64>),
    CasOk(bool),
}

/// Single CAS register. State is the current value (None = never written).
pub struct RegisterModel;

impl Model for RegisterModel {
    type State = Option<u64>;
    type Op = Op;
    type Resp = RegResp;
    fn init() -> Option<u64> {
        None
    }
    fn is_read(op: &Op) -> bool {
        matches!(op, Op::Read)
    }
    fn step(state: &Option<u64>, op: &Op) -> (Option<u64>, RegResp) {
        match op {
            Op::Write(v) => (Some(*v), RegResp::Ack),
            Op::Read => (*state, RegResp::Value(*state)),
            Op::Cas { old, new } => {
                if *state == Some(*old) {
                    (Some(*new), RegResp::CasOk(true))
                } else {
                    (*state, RegResp::CasOk(false))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_step_semantics() {
        let s0 = RegisterModel::init();
        assert_eq!(s0, None);
        let (s1, r) = RegisterModel::step(&s0, &Op::Write(5));
        assert_eq!((s1, r), (Some(5), RegResp::Ack));
        let (_s, r) = RegisterModel::step(&s1, &Op::Read);
        assert_eq!(r, RegResp::Value(Some(5)));
        let (s2, r) = RegisterModel::step(&s1, &Op::Cas { old: 5, new: 9 });
        assert_eq!((s2, r), (Some(9), RegResp::CasOk(true)));
        let (s3, r) = RegisterModel::step(&s1, &Op::Cas { old: 7, new: 9 });
        assert_eq!((s3, r), (Some(5), RegResp::CasOk(false)));
    }
}
