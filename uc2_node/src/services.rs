// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M14a: the declared service set and the FSM lag policy (`[services]` in
//! `node.toml`, `NodeConfig::services` programmatically). See the design spec
//! §3.3 and §5.1–§5.2.

use uc2_log::cnc::CncPage;
use uc_protocol::v2::cnc::CNC_MAX_SERVICES;
use uc_protocol::v2::frame::{HEADER_LEN, align_frame_len};

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

    /// [`ring_ids`](Self::ring_ids) as a bitmask — what the snapshot session
    /// puts on the wire and compares (M14c, spec §14.3). Identical to
    /// [`declared`](Self::declared) for any node built by `from_ids`; `{0}` for
    /// a `none_for_tests` harness node, matching M14a's standing rule that a
    /// page whose `services_declared` reads 0 is treated as `{0}`.
    pub fn ring_mask(&self) -> u64 {
        if self.declared == 0 { 1 } else { self.declared }
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

/// The page-1 aggregates the node publishes each cycle (spec §3.2): the
/// slowest FSM's numbers. Every reader that used to read "the service" now
/// reads "the slowest service" — the purge floor, the output marker, the
/// readiness heartbeat, `uc2ctl status`, the unlabelled `/metrics` families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServiceMins {
    pub applied: u64,
    pub snapshot_pos: u64,
    pub output_completed: u64,
    pub heartbeat_ns: u64,
}

/// N acquire loads, no stores. `None` for a `none_for_tests` node.
pub fn service_mins(cnc: &CncPage, services: &ServicesConfig) -> Option<ServiceMins> {
    let mut m = ServiceMins {
        applied: u64::MAX,
        snapshot_pos: u64::MAX,
        output_completed: u64::MAX,
        heartbeat_ns: u64::MAX,
    };
    let mut any = false;
    for id in services.ids() {
        let s = cnc.service_slot(id as usize);
        m.applied = m.applied.min(s.applied.load_acquire());
        m.snapshot_pos = m.snapshot_pos.min(s.snapshot_pos.load_acquire());
        m.output_completed = m.output_completed.min(s.output_completed.load_acquire());
        m.heartbeat_ns = m.heartbeat_ns.min(s.heartbeat_ns.load_acquire());
        any = true;
    }
    any.then_some(m)
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

/// The door/ceiling term (spec §5.2): the byte bound, or one max-size frame
/// under lockstep ("at most one frame past the FSMs"). `None` ⇔ nothing
/// declared ⇔ no FSM term at all.
pub fn fsm_lag_eff(services: &ServicesConfig, buffer_bytes: u64, max_payload: usize) -> Option<u64> {
    if services.declared() == 0 {
        return None;
    }
    Some(match services.resolve_lag(buffer_bytes) {
        FsmLag::Lockstep => align_frame_len(HEADER_LEN + max_payload) as u64,
        FsmLag::Bounded(b) => b,
    })
}

/// M14c (spec §9): how stale a declared FSM's heartbeat may get before the
/// node calls it gone and emits `service_detached`. Deliberately the SAME
/// 3 s bar `obs::http`'s `HEARTBEAT_STALE_NS` applies to `/readyz` — a
/// service readiness already refuses to count is a service the transition
/// log should have named — and pinned equal to it by a unit test in
/// `obs::http`. They are separate constants because they answer different
/// questions (serve traffic? / say so in the log); if you move one, decide
/// about the other rather than discovering the drift later.
pub const SERVICE_STALE_NS: u64 = 3_000_000_000;

/// Q (spec §5.3): what this node attests toward the leader's commit ranking
/// — never more than it has validated, never more than `fsm_lag` past its
/// slowest FSM. Reporting less than you hold is always safe in Raft.
pub fn report_ceiling(validated_up_to: u64, min_applied: u64, fsm_lag_eff: Option<u64>) -> u64 {
    match fsm_lag_eff {
        None => validated_up_to,
        Some(lag) => validated_up_to.min(min_applied.saturating_add(lag)),
    }
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
    fn service_mins_is_the_min_over_declared_ids_and_ignores_undeclared_slots() {
        let page = uc2_log::cnc::CncPage::heap(&uc2_log::cnc::CncMeta {
            node_id: 1, instance_id: 7, app_id: "t".into(), buffer_bytes: 1 << 20, max_payload: 256,
        });
        let s = ServicesConfig::from_ids(&[0, 2], None).unwrap();
        page.service_slot(0).applied.store_release(500);
        page.service_slot(0).snapshot_pos.store_release(400);
        page.service_slot(0).output_completed.store_release(300);
        page.service_slot(0).heartbeat_ns.store_release(1_000);
        page.service_slot(2).applied.store_release(200);
        page.service_slot(2).snapshot_pos.store_release(900);
        page.service_slot(2).output_completed.store_release(50);
        page.service_slot(2).heartbeat_ns.store_release(2_000);
        page.service_slot(1).applied.store_release(1); // undeclared: must not count
        let m = service_mins(&page, &s).unwrap();
        assert_eq!(m, ServiceMins { applied: 200, snapshot_pos: 400, output_completed: 50, heartbeat_ns: 1_000 });
        // A declared-but-dormant id (slot 2 zeroed) drags every min to 0 — spec §5.1, intentional.
        page.service_slot(2).applied.store_release(0);
        page.service_slot(2).snapshot_pos.store_release(0);
        assert_eq!(service_mins(&page, &s).unwrap().applied, 0);
        assert_eq!(service_mins(&page, &s).unwrap().snapshot_pos, 0);
        assert!(service_mins(&page, &ServicesConfig::none_for_tests()).is_none());
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

    #[test]
    fn fsm_lag_eff_table() {
        let b = 4u64 << 20;
        assert_eq!(fsm_lag_eff(&ServicesConfig::none_for_tests(), b, 256), None);
        assert_eq!(fsm_lag_eff(&ServicesConfig::default(), b, 256), Some(1 << 20));
        assert_eq!(fsm_lag_eff(&ServicesConfig::from_ids(&[0], Some(FsmLag::Bounded(4096))).unwrap(), b, 256), Some(4096));
        // Lockstep: one max-size frame — header 32 + 256 payload, 32-aligned = 288.
        assert_eq!(fsm_lag_eff(&ServicesConfig::from_ids(&[0], Some(FsmLag::Lockstep)).unwrap(), b, 256), Some(288));
        assert_eq!(fsm_lag_eff(&ServicesConfig::from_ids(&[0], Some(FsmLag::Lockstep)).unwrap(), b, 1), Some(64));
    }

    #[test]
    fn report_ceiling_never_exceeds_validated_and_is_inert_without_fsms() {
        assert_eq!(report_ceiling(10_000, 2_000, Some(4_096)), 6_096);
        assert_eq!(report_ceiling(10_000, 9_000, Some(4_096)), 10_000);
        assert_eq!(report_ceiling(10_000, 0, Some(4_096)), 4_096, "absent FSMs cap the report at the bound");
        assert_eq!(report_ceiling(10_000, u64::MAX, Some(4_096)), 10_000, "saturating add");
        assert_eq!(report_ceiling(10_000, 0, None), 10_000);
    }
}
