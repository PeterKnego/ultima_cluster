//! Generic Wing-Gong-Lowe linearizability checker over a `Model`. Pure.
//! `check_model` is the generic entry; `check_register*` are it at
//! `RegisterModel`.
//!
//! Search: repeatedly linearize a real-time-eligible "frontier" op (one whose
//! `invoke` is <= the minimum `ret` of the remaining ops), apply it to the
//! model, and require the model's response to equal the observed response for
//! `Ok` ops. Backtrack on dead-ends. Memoize visited (remaining-set, state).
//!
//! Indeterminate ops: `ret = u64::MAX` (eligible any time at/after invoke),
//! response unconstrained, and OPTIONAL (the search may drop them — they may
//! never have committed). Indeterminate READS carry no information and are
//! dropped before the search; only indeterminate mutations remain.
//!
//! A visited-state budget returns `Inconclusive` rather than a false `Ok`.

use std::collections::HashSet;

use crate::history::{Entry, GenEntry, GenOutcome};
use crate::model::{Model, RegisterModel};

#[derive(Debug, PartialEq)]
pub enum Verdict {
    Linearizable,
    Violation,
    Inconclusive,
}

/// Internal normalized op: (op, observed-response-or-None, invoke, ret).
struct NOp<O, R> {
    op: O,
    observed: Option<R>, // None = indeterminate (response unconstrained)
    invoke: u64,
    ret: u64,
}

/// Default visited-state budget; exceeding it yields `Inconclusive`.
pub const DEFAULT_BUDGET: u64 = 5_000_000;

/// Check a register history for linearizability against `RegisterModel`.
pub fn check_register(entries: &[Entry]) -> Verdict {
    check_register_with_budget(entries, DEFAULT_BUDGET)
}

pub fn check_register_with_budget(entries: &[Entry], budget: u64) -> Verdict {
    check_register_reporting(entries, budget).0
}

/// [`check_register_with_budget`], plus **how much of the budget the search
/// actually spent**.
///
/// The point is calibration, not curiosity. `Inconclusive` says only "the
/// search did not finish"; it does not say whether the budget was marginal (a
/// slightly harder history would have been fine at 2x) or hopeless (the
/// history is exponential and no realistic budget helps). Without the number
/// there is no way to choose between raising the budget and reducing what the
/// workload generates, so a capstone that goes Inconclusive gets fixed by
/// guessing. Callers print this beside the verdict.
pub fn check_register_reporting(entries: &[Entry], budget: u64) -> (Verdict, u64) {
    check_model::<RegisterModel>(entries, budget)
}

/// The generic entry point: check a history against ANY [`Model`], with
/// the same search, the same indeterminate-op rules and the same budget
/// semantics as [`check_register_reporting`] (which is this function at
/// `RegisterModel`). Added for the dogfood adjudication harness, whose
/// per-key model has a Delete the register lacks; the register capstones
/// are unchanged by it.
pub fn check_model<M: Model>(entries: &[GenEntry<M::Op, M::Resp>], budget: u64) -> (Verdict, u64) {
    // Normalize: drop indeterminate reads (no information); map outcomes.
    let mut ops: Vec<NOp<M::Op, M::Resp>> = Vec::new();
    for e in entries {
        match &e.outcome {
            GenOutcome::Indeterminate if M::is_read(&e.op) => continue, // drop
            GenOutcome::Indeterminate => ops.push(NOp {
                op: e.op.clone(),
                observed: None,
                invoke: e.invoke,
                ret: u64::MAX,
            }),
            GenOutcome::Ok(r) => ops.push(NOp {
                op: e.op.clone(),
                observed: Some(r.clone()),
                invoke: e.invoke,
                ret: e.ret,
            }),
        }
    }
    let n = ops.len();
    let mut remaining: Vec<bool> = vec![true; n];
    let mut visited: HashSet<(Vec<bool>, M::State)> = HashSet::new();
    let mut budget_left = budget;
    let res = search::<M>(
        &ops,
        &mut remaining,
        M::init(),
        &mut visited,
        &mut budget_left,
    );
    let spent = budget - budget_left;
    let verdict = match res {
        SearchResult::Ok => Verdict::Linearizable,
        SearchResult::NoLinearization => Verdict::Violation,
        SearchResult::BudgetExceeded => Verdict::Inconclusive,
    };
    (verdict, spent)
}

enum SearchResult {
    Ok,
    NoLinearization,
    BudgetExceeded,
}

fn search<M: Model>(
    ops: &[NOp<M::Op, M::Resp>],
    remaining: &mut Vec<bool>,
    state: M::State,
    visited: &mut HashSet<(Vec<bool>, M::State)>,
    budget: &mut u64,
) -> SearchResult {
    if *budget == 0 {
        return SearchResult::BudgetExceeded;
    }
    *budget -= 1;

    // Done iff no required (Ok) ops remain; leftover indeterminate ops are dropped.
    let any_required = (0..ops.len()).any(|i| remaining[i] && ops[i].observed.is_some());
    if !any_required {
        return SearchResult::Ok;
    }

    // Memo: skip (remaining-set, model-state) we've already PROVEN unlinearizable.
    // We only cache a key after a *complete* exploration (below) — never a
    // budget-truncated one — so a memo hit always means a real dead end.
    let key = (remaining.clone(), state.clone());
    if visited.contains(&key) {
        return SearchResult::NoLinearization;
    }

    // Real-time frontier: candidates are remaining ops whose invoke <= min ret.
    let min_ret = (0..ops.len())
        .filter(|&i| remaining[i])
        .map(|i| ops[i].ret)
        .min()
        .unwrap_or(u64::MAX);

    let mut hit_budget = false;
    for i in 0..ops.len() {
        if !remaining[i] || ops[i].invoke > min_ret {
            continue;
        }
        // Option 1: linearize op i.
        let (state2, resp) = M::step(&state, &ops[i].op);
        let resp_ok = match &ops[i].observed {
            Some(obs) => &resp == obs,
            None => true, // indeterminate: unconstrained
        };
        if resp_ok {
            remaining[i] = false;
            match search::<M>(ops, remaining, state2, visited, budget) {
                SearchResult::Ok => {
                    remaining[i] = true;
                    return SearchResult::Ok;
                }
                SearchResult::BudgetExceeded => hit_budget = true,
                SearchResult::NoLinearization => {}
            }
            remaining[i] = true;
        }
        // Option 2: indeterminate op may be dropped (never committed).
        if ops[i].observed.is_none() {
            remaining[i] = false;
            match search::<M>(ops, remaining, state.clone(), visited, budget) {
                SearchResult::Ok => {
                    remaining[i] = true;
                    return SearchResult::Ok;
                }
                SearchResult::BudgetExceeded => hit_budget = true,
                SearchResult::NoLinearization => {}
            }
            remaining[i] = true;
        }
        if *budget == 0 {
            return SearchResult::BudgetExceeded;
        }
    }
    if hit_budget {
        // Don't cache budget-truncated states — they weren't fully explored.
        SearchResult::BudgetExceeded
    } else {
        // Fully explored with no linearization: safe to memoize as a dead end.
        visited.insert(key);
        SearchResult::NoLinearization
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::{Entry, Outcome};
    use crate::model::{Op, RegResp};

    fn e(client: u32, op: Op, invoke: u64, ret: u64, outcome: Outcome) -> Entry {
        Entry {
            client,
            op,
            invoke,
            ret,
            outcome,
        }
    }

    #[test]
    fn sequential_history_is_linearizable() {
        // write(1) ; read->1 ; cas(1,2)->true ; read->2  (non-overlapping)
        let h = vec![
            e(0, Op::Write(1), 0, 1, Outcome::Ok(RegResp::Ack)),
            e(0, Op::Read, 2, 3, Outcome::Ok(RegResp::Value(Some(1)))),
            e(
                0,
                Op::Cas { old: 1, new: 2 },
                4,
                5,
                Outcome::Ok(RegResp::CasOk(true)),
            ),
            e(0, Op::Read, 6, 7, Outcome::Ok(RegResp::Value(Some(2)))),
        ];
        assert_eq!(check_register(&h), Verdict::Linearizable);
    }

    #[test]
    fn stale_read_after_write_is_violation() {
        // write(1) fully precedes read, but read observed the old value (None).
        let h = vec![
            e(0, Op::Write(1), 0, 1, Outcome::Ok(RegResp::Ack)),
            e(1, Op::Read, 2, 3, Outcome::Ok(RegResp::Value(None))),
        ];
        assert_eq!(check_register(&h), Verdict::Violation);
    }

    #[test]
    fn double_applied_cas_is_violation() {
        // write(1); two concurrent cas(1,2)->true BOTH succeed — impossible.
        let h = vec![
            e(0, Op::Write(1), 0, 1, Outcome::Ok(RegResp::Ack)),
            e(
                1,
                Op::Cas { old: 1, new: 2 },
                2,
                5,
                Outcome::Ok(RegResp::CasOk(true)),
            ),
            e(
                2,
                Op::Cas { old: 1, new: 2 },
                2,
                5,
                Outcome::Ok(RegResp::CasOk(true)),
            ),
        ];
        assert_eq!(check_register(&h), Verdict::Violation);
    }

    #[test]
    fn concurrent_overlap_is_linearizable() {
        // write(1) and read overlap; read may observe None OR 1 — both ok.
        let h = vec![
            e(0, Op::Write(1), 0, 5, Outcome::Ok(RegResp::Ack)),
            e(1, Op::Read, 1, 4, Outcome::Ok(RegResp::Value(None))),
        ];
        assert_eq!(check_register(&h), Verdict::Linearizable);
    }

    #[test]
    fn indeterminate_write_may_be_present_or_absent() {
        // An indeterminate write(9) overlaps a later read that saw 1.
        // The checker may DROP the indeterminate write so the read is consistent.
        let h = vec![
            e(0, Op::Write(1), 0, 1, Outcome::Ok(RegResp::Ack)),
            e(1, Op::Write(9), 2, u64::MAX, Outcome::Indeterminate),
            e(0, Op::Read, 3, 4, Outcome::Ok(RegResp::Value(Some(1)))),
        ];
        assert_eq!(check_register(&h), Verdict::Linearizable);
    }

    /// A second model, to prove `check_model` is generic and not the
    /// register in disguise: a register with a Delete. The dogfood KV's
    /// per-key model has this shape.
    struct DelModel;
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum DOp {
        Put(u64),
        Get,
        Del,
    }
    #[derive(Clone, Debug, PartialEq, Eq)]
    enum DResp {
        Ack,
        Val(Option<u64>),
        Deleted(bool),
    }
    impl Model for DelModel {
        type State = Option<u64>;
        type Op = DOp;
        type Resp = DResp;
        fn init() -> Option<u64> {
            None
        }
        fn is_read(op: &DOp) -> bool {
            matches!(op, DOp::Get)
        }
        fn step(state: &Option<u64>, op: &DOp) -> (Option<u64>, DResp) {
            match op {
                DOp::Put(v) => (Some(*v), DResp::Ack),
                DOp::Get => (*state, DResp::Val(*state)),
                DOp::Del => (None, DResp::Deleted(state.is_some())),
            }
        }
    }
    fn d(
        client: u32,
        op: DOp,
        invoke: u64,
        ret: u64,
        outcome: GenOutcome<DResp>,
    ) -> GenEntry<DOp, DResp> {
        GenEntry {
            client,
            op,
            invoke,
            ret,
            outcome,
        }
    }

    #[test]
    fn generic_model_delete_then_stale_read_is_violation() {
        // put(1); del -> true; get -> Some(1)  (non-overlapping): impossible.
        let h = vec![
            d(0, DOp::Put(1), 0, 1, GenOutcome::Ok(DResp::Ack)),
            d(0, DOp::Del, 2, 3, GenOutcome::Ok(DResp::Deleted(true))),
            d(1, DOp::Get, 4, 5, GenOutcome::Ok(DResp::Val(Some(1)))),
        ];
        assert_eq!(
            check_model::<DelModel>(&h, DEFAULT_BUDGET).0,
            Verdict::Violation
        );
        // The same with the read overlapping the delete is fine.
        let h2 = vec![
            d(0, DOp::Put(1), 0, 1, GenOutcome::Ok(DResp::Ack)),
            d(0, DOp::Del, 2, 6, GenOutcome::Ok(DResp::Deleted(true))),
            d(1, DOp::Get, 3, 5, GenOutcome::Ok(DResp::Val(Some(1)))),
        ];
        assert_eq!(
            check_model::<DelModel>(&h2, DEFAULT_BUDGET).0,
            Verdict::Linearizable
        );
    }

    #[test]
    fn generic_model_drops_indeterminate_reads_but_keeps_indeterminate_deletes() {
        // An indeterminate delete may explain a later `None`.
        let h = vec![
            d(0, DOp::Put(1), 0, 1, GenOutcome::Ok(DResp::Ack)),
            d(1, DOp::Get, 2, u64::MAX, GenOutcome::Indeterminate),
            d(2, DOp::Del, 3, u64::MAX, GenOutcome::Indeterminate),
            d(0, DOp::Get, 4, 5, GenOutcome::Ok(DResp::Val(None))),
        ];
        assert_eq!(
            check_model::<DelModel>(&h, DEFAULT_BUDGET).0,
            Verdict::Linearizable
        );
    }

    #[test]
    fn indeterminate_write_that_must_have_happened() {
        // read observed 9, only an indeterminate write(9) could have set it.
        // The checker must be willing to PLACE the indeterminate write.
        let h = vec![
            e(1, Op::Write(9), 0, u64::MAX, Outcome::Indeterminate),
            e(0, Op::Read, 1, 2, Outcome::Ok(RegResp::Value(Some(9)))),
        ];
        assert_eq!(check_register(&h), Verdict::Linearizable);
    }
}
