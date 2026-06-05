//! Generic Wing-Gong-Lowe linearizability checker over a `Model`. Pure.
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

use crate::lincheck::history::{Entry, Outcome};
use crate::lincheck::model::{Model, Op, RegResp, RegisterModel};

#[derive(Debug, PartialEq)]
pub enum Verdict {
    Linearizable,
    Violation,
    Inconclusive,
}

/// Internal normalized op: (op, observed-response-or-None, invoke, ret).
struct NOp {
    op: Op,
    observed: Option<RegResp>, // None = indeterminate (response unconstrained)
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
    // Normalize: drop indeterminate reads (no information); map outcomes.
    let mut ops: Vec<NOp> = Vec::new();
    for e in entries {
        match (&e.op, &e.outcome) {
            (Op::Read, Outcome::Indeterminate) => continue, // drop
            (_, Outcome::Indeterminate) => ops.push(NOp {
                op: e.op.clone(),
                observed: None,
                invoke: e.invoke,
                ret: u64::MAX,
            }),
            (_, Outcome::Ok(r)) => ops.push(NOp {
                op: e.op.clone(),
                observed: Some(r.clone()),
                invoke: e.invoke,
                ret: e.ret,
            }),
        }
    }
    let n = ops.len();
    let mut remaining: Vec<bool> = vec![true; n];
    let mut visited: HashSet<(Vec<bool>, Option<u64>)> = HashSet::new();
    let mut budget_left = budget;
    let res = search::<RegisterModel>(&ops, &mut remaining, RegisterModel::init(), &mut visited, &mut budget_left);
    match res {
        SearchResult::Ok => Verdict::Linearizable,
        SearchResult::NoLinearization => Verdict::Violation,
        SearchResult::BudgetExceeded => Verdict::Inconclusive,
    }
}

enum SearchResult {
    Ok,
    NoLinearization,
    BudgetExceeded,
}

fn search<M: Model<State = Option<u64>, Op = Op, Resp = RegResp>>(
    ops: &[NOp],
    remaining: &mut Vec<bool>,
    state: Option<u64>,
    visited: &mut HashSet<(Vec<bool>, Option<u64>)>,
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

    // Memo: skip states (remaining-set, model-state) we've explored before.
    let key = (remaining.clone(), state);
    if !visited.insert(key) {
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
            match search::<M>(ops, remaining, state, visited, budget) {
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
        SearchResult::BudgetExceeded
    } else {
        SearchResult::NoLinearization
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lincheck::history::{Entry, Outcome};
    use crate::lincheck::model::{Op, RegResp};

    fn e(client: u32, op: Op, invoke: u64, ret: u64, outcome: Outcome) -> Entry {
        Entry { client, op, invoke, ret, outcome }
    }

    #[test]
    fn sequential_history_is_linearizable() {
        // write(1) ; read->1 ; cas(1,2)->true ; read->2  (non-overlapping)
        let h = vec![
            e(0, Op::Write(1), 0, 1, Outcome::Ok(RegResp::Ack)),
            e(0, Op::Read, 2, 3, Outcome::Ok(RegResp::Value(Some(1)))),
            e(0, Op::Cas { old: 1, new: 2 }, 4, 5, Outcome::Ok(RegResp::CasOk(true))),
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
            e(1, Op::Cas { old: 1, new: 2 }, 2, 5, Outcome::Ok(RegResp::CasOk(true))),
            e(2, Op::Cas { old: 1, new: 2 }, 2, 5, Outcome::Ok(RegResp::CasOk(true))),
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
