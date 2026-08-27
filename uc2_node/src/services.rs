// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M14a: the declared service set and the FSM lag policy (`[services]` in
//! `node.toml`, `NodeConfig::services` programmatically). See the design spec
//! §3.3 and §5.1–§5.2.

use uc_protocol::v2::cnc::CNC_MAX_SERVICES;

/// The FSM pacing policy (spec §1, "FSM pacing"). There is deliberately no
/// unbounded variant: an FSM slower than the log's sustained rate can never
/// catch up from journal replay, so "unbounded" is a silent death spiral.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsmLag {
    /// No FSM starts frame k+1 until every FSM finished frame k.
    Lockstep,
    /// `applied_a - applied_b <= bytes` for any two declared FSMs.
    Bounded(u64),
}

/// The declared service set + lag policy. Static per node; must match
/// cluster-wide (checked on the snapshot path in M14c, exported for alerting
/// in M14c). Absent `[services]` ⇒ `{0}` with the default bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServicesConfig {
    /// Bit `i` set ⇔ id `i` declared. `0` only via `none_for_tests`.
    declared: u64,
    /// `None` ⇒ `Bounded(buffer_bytes / 4)`, resolved once `buffer_bytes` is known.
    fsm_lag: Option<FsmLag>,
}

impl Default for ServicesConfig {
    fn default() -> Self {
        Self { declared: 0b1, fsm_lag: None }
    }
}

impl ServicesConfig {
    /// Build from an explicit id list. Refusals (each names the field, M9
    /// style): empty list, duplicate id, id ≥ 8, id 0 missing (FSM 0 is the
    /// default responder and the only FSM the remote path reaches).
    pub fn from_ids(ids: &[u8], fsm_lag: Option<FsmLag>) -> Result<Self, String> {
        if ids.is_empty() {
            return Err("services.ids must not be empty (omit the [services] section for the default [0])".into());
        }
        let mut declared = 0u64;
        for &id in ids {
            if id as usize >= CNC_MAX_SERVICES {
                return Err(format!("services.ids: service id {id} is out of range (0..{CNC_MAX_SERVICES})"));
            }
            if declared & (1 << id) != 0 {
                return Err(format!("services.ids: duplicate service id {id}"));
            }
            declared |= 1 << id;
        }
        if declared & 1 == 0 {
            return Err("services.ids: service id 0 must be declared (it is the default responder)".into());
        }
        Ok(Self { declared, fsm_lag })
    }

    /// HARNESS ONLY: a node with no FSMs declared. The aggregates are not
    /// published, the admission door's FSM term and the report ceiling are
    /// inert, and page 1's service band behaves as it did on cnc 2.0 (a test
    /// may poke it). Unreachable from `node.toml` (`from_ids` refuses an
    /// empty list); exists so node-only tests are not silently stalled by a
    /// service that was never going to attach.
    #[doc(hidden)]
    pub fn none_for_tests() -> Self {
        Self { declared: 0, fsm_lag: None }
    }

    pub fn declared(&self) -> u64 {
        self.declared
    }

    pub fn is_declared(&self, id: u8) -> bool {
        (id as usize) < CNC_MAX_SERVICES && self.declared & (1 << id) != 0
    }

    /// Declared ids, ascending.
    pub fn ids(&self) -> impl Iterator<Item = u8> + '_ {
        (0..CNC_MAX_SERVICES as u8).filter(move |&i| self.is_declared(i))
    }

    /// The ids the node creates rings/dirs for: the declared set, or `{0}`
    /// for a `none_for_tests` node (clients still need FSM 0's rings to
    /// attach).
    pub fn ring_ids(&self) -> impl Iterator<Item = u8> + '_ {
        let mask = if self.declared == 0 { 1 } else { self.declared };
        (0..CNC_MAX_SERVICES as u8).filter(move |&i| mask & (1 << i) != 0)
    }

    pub fn resolve_lag(&self, buffer_bytes: u64) -> FsmLag {
        self.fsm_lag.unwrap_or(FsmLag::Bounded(buffer_bytes / 4))
    }

    /// The cnc 4040 encoding: the byte bound, or `0` for lockstep.
    pub fn page_lag_value(&self, buffer_bytes: u64) -> u64 {
        match self.resolve_lag(buffer_bytes) {
            FsmLag::Lockstep => 0,
            FsmLag::Bounded(b) => b,
        }
    }

    /// The bound must provably keep every FSM on the ring: below half the
    /// buffer (the other half is the appender's overrun margin plus the
    /// leader's admission window). `0` is refused because it is the page's
    /// lockstep sentinel — a config that means lockstep must say so.
    pub fn validate(&self, buffer_bytes: u64) -> Result<(), String> {
        match self.resolve_lag(buffer_bytes) {
            FsmLag::Lockstep => Ok(()),
            FsmLag::Bounded(0) => {
                Err("services.fsm_lag = 0 is not a bound; write \"lockstep\" for lockstep".into())
            }
            FsmLag::Bounded(b) if b >= buffer_bytes / 2 => Err(format!(
                "services.fsm_lag must be below buffer_bytes / 2 ({} < {}); got {b}",
                b,
                buffer_bytes / 2
            )),
            FsmLag::Bounded(_) => Ok(()),
        }
    }
}

/// `"lockstep"`, or a byte count as `<digits>` with an optional `KiB`/`MiB`/
/// `GiB` suffix (no spaces, no fractions, binary units only — the same
/// vocabulary the spec uses). Errors name the field.
pub fn parse_fsm_lag(s: &str) -> Result<FsmLag, String> {
    if s == "lockstep" {
        return Ok(FsmLag::Lockstep);
    }
    let (digits, shift) = if let Some(d) = s.strip_suffix("GiB") {
        (d, 30)
    } else if let Some(d) = s.strip_suffix("MiB") {
        (d, 20)
    } else if let Some(d) = s.strip_suffix("KiB") {
        (d, 10)
    } else {
        (s, 0)
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(format!(
            "services.fsm_lag must be \"lockstep\" or <digits>[KiB|MiB|GiB], got {s:?}"
        ));
    }
    let n: u64 = digits
        .parse()
        .map_err(|_| format!("services.fsm_lag: {digits:?} does not fit in u64"))?;
    n.checked_shl(shift)
        .filter(|v| shift == 0 || *v >> shift == n)
        .map(FsmLag::Bounded)
        .ok_or_else(|| format!("services.fsm_lag: {s:?} overflows u64"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_fsm_zero_with_unset_lag_resolving_to_a_quarter_buffer() {
        let s = ServicesConfig::default();
        assert_eq!(s.declared(), 0b1);
        assert!(s.is_declared(0));
        assert!(!s.is_declared(1));
        assert_eq!(s.ids().collect::<Vec<_>>(), vec![0]);
        assert_eq!(s.resolve_lag(4 << 20), FsmLag::Bounded(1 << 20));
        assert_eq!(s.page_lag_value(4 << 20), 1 << 20);
        s.validate(4 << 20).unwrap();
    }

    #[test]
    fn from_ids_builds_the_bitmask_in_any_order() {
        let s = ServicesConfig::from_ids(&[2, 0, 5], None).unwrap();
        assert_eq!(s.declared(), 0b10_0101);
        assert_eq!(s.ids().collect::<Vec<_>>(), vec![0, 2, 5]);
        assert_eq!(s.ring_ids().collect::<Vec<_>>(), vec![0, 2, 5]);
    }

    #[test]
    fn from_ids_refusals_are_named() {
        let e = ServicesConfig::from_ids(&[], None).unwrap_err();
        assert!(e.contains("services.ids must not be empty"), "{e}");
        let e = ServicesConfig::from_ids(&[0, 1, 1], None).unwrap_err();
        assert!(e.contains("duplicate service id 1"), "{e}");
        let e = ServicesConfig::from_ids(&[0, 8], None).unwrap_err();
        assert!(e.contains("service id 8 is out of range (0..8)"), "{e}");
        let e = ServicesConfig::from_ids(&[1, 2], None).unwrap_err();
        assert!(e.contains("service id 0 must be declared"), "{e}");
    }

    #[test]
    fn lag_validation_refuses_half_the_ring_and_zero() {
        let buf = 4u64 << 20;
        ServicesConfig::from_ids(&[0], Some(FsmLag::Bounded((buf / 2) - 1))).unwrap().validate(buf).unwrap();
        let e = ServicesConfig::from_ids(&[0], Some(FsmLag::Bounded(buf / 2))).unwrap().validate(buf).unwrap_err();
        assert!(e.contains("services.fsm_lag must be below buffer_bytes / 2"), "{e}");
        let e = ServicesConfig::from_ids(&[0], Some(FsmLag::Bounded(0))).unwrap().validate(buf).unwrap_err();
        assert!(e.contains("services.fsm_lag = 0 is not a bound; write \"lockstep\""), "{e}");
        ServicesConfig::from_ids(&[0], Some(FsmLag::Lockstep)).unwrap().validate(buf).unwrap();
        assert_eq!(ServicesConfig::from_ids(&[0], Some(FsmLag::Lockstep)).unwrap().page_lag_value(buf), 0);
    }

    #[test]
    fn none_for_tests_declares_nothing_but_still_rings_fsm_zero() {
        let s = ServicesConfig::none_for_tests();
        assert_eq!(s.declared(), 0);
        assert_eq!(s.ids().count(), 0);
        assert_eq!(s.ring_ids().collect::<Vec<_>>(), vec![0]);
        s.validate(4 << 20).unwrap();
    }

    #[test]
    fn parse_fsm_lag_table() {
        assert_eq!(parse_fsm_lag("lockstep"), Ok(FsmLag::Lockstep));
        assert_eq!(parse_fsm_lag("65536"), Ok(FsmLag::Bounded(65536)));
        assert_eq!(parse_fsm_lag("64KiB"), Ok(FsmLag::Bounded(64 << 10)));
        assert_eq!(parse_fsm_lag("16MiB"), Ok(FsmLag::Bounded(16 << 20)));
        assert_eq!(parse_fsm_lag("1GiB"), Ok(FsmLag::Bounded(1 << 30)));
        for bad in ["", "16 MiB", "16mb", "MiB", "1.5MiB", "-1", "99999999999GiB", "Lockstep"] {
            let e = parse_fsm_lag(bad).unwrap_err();
            assert!(e.contains("services.fsm_lag"), "{bad:?}: {e}");
        }
    }
}
