// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! M14a: the declared service set and the FSM lag policy (`[services]` in
//! `node.toml`, `NodeConfig::services` programmatically). See the design spec
//! §3.3 and §5.1–§5.2. FSM identity (spec §4.1-4.2): rows are named, in
//! `[services] names` order — there is no default set, a node names its FSMs
//! or refuses to start.

use uc_log::cnc::CncPage;
use uc_protocol::identity::FsmName;
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

/// The declared FSM set (row → name, in `[services] names` order) + lag
/// policy. Static per node; must match cluster-wide (checked by name on the
/// snapshot path, exported for alerting). There is no default: a node names
/// its FSMs or refuses to start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServicesConfig {
    /// Row `i`'s name, or `None` if row `i` is undeclared. Declared rows are
    /// always a contiguous prefix `0..count` (assignment order = list order).
    names: [Option<FsmName>; CNC_MAX_SERVICES],
    count: u8,
    /// `None` ⇒ `Bounded(buffer_bytes / 4)`, resolved once `buffer_bytes` is known.
    fsm_lag: Option<FsmLag>,
}

impl ServicesConfig {
    /// Build from an explicit name list, in row order. Refusals (each names
    /// the field, M9 style): empty list, more than `CNC_MAX_SERVICES`, an
    /// invalid name (`FsmName::parse`'s own message), a duplicate name.
    pub fn from_names(names: &[&str], fsm_lag: Option<FsmLag>) -> Result<Self, String> {
        if names.is_empty() {
            return Err("services.names must not be empty: list the FSM names in row order".into());
        }
        if names.len() > CNC_MAX_SERVICES {
            return Err(format!(
                "services.names: at most {CNC_MAX_SERVICES} FSMs per log, got {}",
                names.len()
            ));
        }
        let mut out = [None; CNC_MAX_SERVICES];
        for (i, raw) in names.iter().enumerate() {
            let n = FsmName::parse(raw).map_err(|e| format!("services.names: {raw:?}: {e}"))?;
            if out[..i].contains(&Some(n)) {
                return Err(format!("services.names: duplicate FSM name {raw:?}"));
            }
            out[i] = Some(n);
        }
        Ok(Self {
            names: out,
            count: names.len() as u8,
            fsm_lag,
        })
    }

    /// One FSM at row 0. Programmatic use (tests, harnesses); panics on an
    /// invalid name, which is a bug at the call site, not a config error.
    pub fn single(name: &str) -> Self {
        Self::from_names(&[name], None).expect("a valid FSM name")
    }

    /// `fsm0..fsm{n-1}`: the rows `uc_service::Tagged<ROW, S>` attaches to
    /// (Task 5's multi-FSM harness rows).
    pub fn tagged(n: u8) -> Self {
        let names: Vec<String> = (0..n).map(|i| format!("fsm{i}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        Self::from_names(&refs, None).expect("fsm0..fsm7 are valid names")
    }

    /// The CLI form every gate/harness binary shares: `--services kv,orders`
    /// (a comma-separated list of names in row order, REQUIRED — there is no
    /// default set) and `--fsm-lag lockstep|<bytes>` (absent ⇒ the default
    /// bound), refused by flag name the way `node.toml`'s loader refuses by
    /// field name.
    pub fn from_cli(names: Option<&str>, fsm_lag: Option<&str>) -> Result<Self, String> {
        let lag = match fsm_lag {
            None => None,
            Some(raw) => {
                Some(parse_fsm_lag(raw.trim()).map_err(|d| format!("--fsm-lag {raw:?}: {d}"))?)
            }
        };
        let Some(list) = names else {
            return Err(
                "--services is required: a comma-separated list of FSM names in row order, e.g. --services kv,orders"
                    .into(),
            );
        };
        let parts: Vec<&str> = list.split(',').map(str::trim).collect();
        Self::from_names(&parts, lag).map_err(|d| format!("--services {list:?}: {d}"))
    }

    /// HARNESS ONLY: a node with no FSMs declared. The aggregates are not
    /// published, the admission door's FSM term and the report ceiling are
    /// inert, and page 1's service band behaves as it did on cnc 2.0 (a test
    /// may poke it). Unreachable from `node.toml` (`from_names` refuses an
    /// empty list); exists so node-only tests are not silently stalled by a
    /// service that was never going to attach.
    #[doc(hidden)]
    pub fn none_for_tests() -> Self {
        Self {
            names: [None; CNC_MAX_SERVICES],
            count: 0,
            fsm_lag: None,
        }
    }

    pub fn count(&self) -> u8 {
        self.count
    }

    /// Harness helper: the same names, another lag.
    pub fn with_lag(mut self, lag: Option<FsmLag>) -> Self {
        self.fsm_lag = lag;
        self
    }

    /// Bit `i` set ⇔ row `i` declared. Rows are always a contiguous prefix,
    /// so this is `(1 << count) - 1`.
    pub fn declared(&self) -> u64 {
        (1u64 << self.count) - 1
    }

    pub fn name_of(&self, row: u8) -> Option<FsmName> {
        self.names.get(row as usize).copied().flatten()
    }

    pub fn row_of(&self, name: &str) -> Option<u8> {
        let n = FsmName::parse(name).ok()?;
        self.names
            .iter()
            .position(|x| *x == Some(n))
            .map(|i| i as u8)
    }

    pub fn service_names(&self) -> [Option<FsmName>; CNC_MAX_SERVICES] {
        self.names
    }

    /// Row `i`'s identity hash, or `0` for an undeclared row (spec §4.2).
    pub fn identity_hashes(&self) -> [u64; CNC_MAX_SERVICES] {
        let mut h = [0u64; CNC_MAX_SERVICES];
        for (i, n) in self.names.iter().enumerate() {
            if let Some(n) = n {
                h[i] = n.hash();
            }
        }
        h
    }

    pub fn is_declared(&self, id: u8) -> bool {
        (id as usize) < CNC_MAX_SERVICES && self.declared() & (1 << id) != 0
    }

    /// Declared ids, ascending.
    pub fn ids(&self) -> impl Iterator<Item = u8> + '_ {
        (0..CNC_MAX_SERVICES as u8).filter(move |&i| self.is_declared(i))
    }

    /// The ids the node creates rings/dirs for: the declared set, or `{0}`
    /// for a `none_for_tests` node (clients still need FSM 0's rings to
    /// attach).
    pub fn ring_ids(&self) -> impl Iterator<Item = u8> + '_ {
        let mask = if self.declared() == 0 {
            1
        } else {
            self.declared()
        };
        (0..CNC_MAX_SERVICES as u8).filter(move |&i| mask & (1 << i) != 0)
    }

    /// [`ring_ids`](Self::ring_ids) as a bitmask — what the snapshot session
    /// puts on the wire and compares (M14c, spec §14.3). Identical to
    /// [`declared`](Self::declared) for any node built by `from_names`; `{0}`
    /// for a `none_for_tests` harness node, matching M14a's standing rule that
    /// a page whose `services_declared` reads 0 is treated as `{0}`.
    pub fn ring_mask(&self) -> u64 {
        if self.declared() == 0 {
            1
        } else {
            self.declared()
        }
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

/// One declared FSM's slot words that the attach/detach EDGES key on, read in
/// the same pass as the mins above.
///
/// M14c2 T10b: the edge check used to re-load `heartbeat_ns` a second time in
/// the same duty cycle (`note_service_transitions` ran its own loop over the
/// same slots right after [`service_mins`]). Reading each slot once and handing
/// the words on pins that both readers adjudicate the SAME sample rather than
/// two reads taken a few instructions apart — that is the point of this type;
/// no performance claim is made or measured. Indexed BY SERVICE ID — declared
/// sets are sparse, so undeclared entries stay at the `Default`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ServiceLiveness {
    pub epoch: u64,
    /// Raw slot status word — unpack with [`uc_log::cnc::unpack_service_status`].
    pub status: u64,
    pub heartbeat_ns: u64,
}

/// N acquire loads, no stores. `None` for a `none_for_tests` node.
///
/// Retained for tests; production reads
/// [`service_mins_and_liveness`] (M14c2 T10b), which is the same pass plus the
/// per-id words the attach/detach edges need.
pub fn service_mins(cnc: &CncPage, services: &ServicesConfig) -> Option<ServiceMins> {
    let mut live = [ServiceLiveness::default(); CNC_MAX_SERVICES];
    service_mins_and_liveness(cnc, services, &mut live)
}

/// [`service_mins`] plus the per-id edge words, in ONE pass over the declared
/// slots (M14c2 T10b — see [`ServiceLiveness`]). `live` is filled in BY SERVICE
/// ID for each declared id; entries for undeclared ids are left untouched.
pub fn service_mins_and_liveness(
    cnc: &CncPage,
    services: &ServicesConfig,
    live: &mut [ServiceLiveness; CNC_MAX_SERVICES],
) -> Option<ServiceMins> {
    let mut m = ServiceMins {
        applied: u64::MAX,
        snapshot_pos: u64::MAX,
        output_completed: u64::MAX,
        heartbeat_ns: u64::MAX,
    };
    let mut any = false;
    for id in services.ids() {
        let s = cnc.service_slot(id as usize);
        let heartbeat_ns = s.heartbeat_ns.load_acquire();
        m.applied = m.applied.min(s.applied.load_acquire());
        m.snapshot_pos = m.snapshot_pos.min(s.snapshot_pos.load_acquire());
        m.output_completed = m.output_completed.min(s.output_completed.load_acquire());
        m.heartbeat_ns = m.heartbeat_ns.min(heartbeat_ns);
        live[id as usize] = ServiceLiveness {
            epoch: s.epoch.load_acquire(),
            status: s.status.load_acquire(),
            heartbeat_ns,
        };
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
pub fn fsm_lag_eff(
    services: &ServicesConfig,
    buffer_bytes: u64,
    max_payload: usize,
) -> Option<u64> {
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
    fn from_names_assigns_rows_in_list_order_and_single_is_one_row() {
        let c = ServicesConfig::from_names(&["orders", "kv"], None).unwrap();
        assert_eq!(c.count(), 2);
        assert_eq!(c.declared(), 0b11);
        assert_eq!(c.row_of("orders"), Some(0));
        assert_eq!(c.row_of("kv"), Some(1));
        assert_eq!(c.row_of("nope"), None);
        assert_eq!(c.name_of(1).unwrap().as_str(), "kv");
        assert_eq!(c.name_of(2), None);
        assert_eq!(
            c.identity_hashes()[0],
            FsmName::parse("orders").unwrap().hash()
        );
        assert_eq!(c.identity_hashes()[2], 0);
        assert_eq!(c.ids().collect::<Vec<_>>(), vec![0, 1]);
        let s = ServicesConfig::single("count");
        assert_eq!(
            (s.count(), s.declared(), s.resolve_lag(1 << 24)),
            (1, 0b1, FsmLag::Bounded(1 << 22))
        );
        assert_eq!(ServicesConfig::tagged(3).row_of("fsm2"), Some(2));
    }

    #[test]
    fn from_names_refusals_are_named() {
        let e = |n: &[&str]| ServicesConfig::from_names(n, None).unwrap_err();
        assert!(
            e(&[]).contains("services.names must not be empty"),
            "{}",
            e(&[])
        );
        assert!(e(&["a", "a"]).contains("duplicate FSM name \"a\""));
        assert!(e(&["1abc"]).contains("services.names: \"1abc\": FSM name must start with a-z"));
        let nine: Vec<String> = (0..9).map(|i| format!("f{i}")).collect();
        let nine: Vec<&str> = nine.iter().map(String::as_str).collect();
        assert!(e(&nine).contains("at most 8 FSMs"));
    }

    #[test]
    fn from_cli_requires_services_and_parses_both_flags() {
        let e = ServicesConfig::from_cli(None, None).unwrap_err();
        assert!(e.starts_with("--services is required"), "{e}");
        let c = ServicesConfig::from_cli(Some("kv, orders"), Some("lockstep")).unwrap();
        assert_eq!(c.row_of("orders"), Some(1));
        assert_eq!(c.resolve_lag(1 << 24), FsmLag::Lockstep);
        assert!(
            ServicesConfig::from_cli(Some("Kv"), None)
                .unwrap_err()
                .starts_with("--services")
        );
        assert!(
            ServicesConfig::from_cli(Some("kv"), Some("16 MiB"))
                .unwrap_err()
                .starts_with("--fsm-lag")
        );
    }

    #[test]
    fn lag_validation_refuses_half_the_ring_and_zero() {
        let buf = 4u64 << 20;
        ServicesConfig::from_names(&["a"], Some(FsmLag::Bounded((buf / 2) - 1)))
            .unwrap()
            .validate(buf)
            .unwrap();
        let e = ServicesConfig::from_names(&["a"], Some(FsmLag::Bounded(buf / 2)))
            .unwrap()
            .validate(buf)
            .unwrap_err();
        assert!(
            e.contains("services.fsm_lag must be below buffer_bytes / 2"),
            "{e}"
        );
        let e = ServicesConfig::from_names(&["a"], Some(FsmLag::Bounded(0)))
            .unwrap()
            .validate(buf)
            .unwrap_err();
        assert!(
            e.contains("services.fsm_lag = 0 is not a bound; write \"lockstep\""),
            "{e}"
        );
        ServicesConfig::from_names(&["a"], Some(FsmLag::Lockstep))
            .unwrap()
            .validate(buf)
            .unwrap();
        assert_eq!(
            ServicesConfig::from_names(&["a"], Some(FsmLag::Lockstep))
                .unwrap()
                .page_lag_value(buf),
            0
        );
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
        let page = uc_log::cnc::CncPage::heap(&uc_log::cnc::CncMeta {
            node_id: 1,
            instance_id: 7,
            app_id: "t".into(),
            buffer_bytes: 1 << 20,
            max_payload: 256,
            services: [None; uc_protocol::v2::cnc::CNC_MAX_SERVICES],
        });
        let s = ServicesConfig::from_names(&["a", "b"], None).unwrap();
        page.service_slot(0).applied.store_release(500);
        page.service_slot(0).snapshot_pos.store_release(400);
        page.service_slot(0).output_completed.store_release(300);
        page.service_slot(0).heartbeat_ns.store_release(1_000);
        page.service_slot(1).applied.store_release(200);
        page.service_slot(1).snapshot_pos.store_release(900);
        page.service_slot(1).output_completed.store_release(50);
        page.service_slot(1).heartbeat_ns.store_release(2_000);
        page.service_slot(5).applied.store_release(1); // undeclared: must not count
        let m = service_mins(&page, &s).unwrap();
        assert_eq!(
            m,
            ServiceMins {
                applied: 200,
                snapshot_pos: 400,
                output_completed: 50,
                heartbeat_ns: 1_000
            }
        );
        // A declared-but-dormant id (slot 1 zeroed) drags every min to 0 — spec §5.1, intentional.
        page.service_slot(1).applied.store_release(0);
        page.service_slot(1).snapshot_pos.store_release(0);
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
        for bad in [
            "",
            "16 MiB",
            "16mb",
            "MiB",
            "1.5MiB",
            "-1",
            "99999999999GiB",
            "Lockstep",
        ] {
            let e = parse_fsm_lag(bad).unwrap_err();
            assert!(e.contains("services.fsm_lag"), "{bad:?}: {e}");
        }
    }

    #[test]
    fn fsm_lag_eff_table() {
        let b = 4u64 << 20;
        assert_eq!(fsm_lag_eff(&ServicesConfig::none_for_tests(), b, 256), None);
        assert_eq!(
            fsm_lag_eff(&ServicesConfig::single("a"), b, 256),
            Some(1 << 20)
        );
        assert_eq!(
            fsm_lag_eff(
                &ServicesConfig::from_names(&["a"], Some(FsmLag::Bounded(4096))).unwrap(),
                b,
                256
            ),
            Some(4096)
        );
        // Lockstep: one max-size frame — header 32 + 256 payload, 32-aligned = 288.
        assert_eq!(
            fsm_lag_eff(
                &ServicesConfig::from_names(&["a"], Some(FsmLag::Lockstep)).unwrap(),
                b,
                256
            ),
            Some(288)
        );
        assert_eq!(
            fsm_lag_eff(
                &ServicesConfig::from_names(&["a"], Some(FsmLag::Lockstep)).unwrap(),
                b,
                1
            ),
            Some(64)
        );
    }

    #[test]
    fn report_ceiling_never_exceeds_validated_and_is_inert_without_fsms() {
        assert_eq!(report_ceiling(10_000, 2_000, Some(4_096)), 6_096);
        assert_eq!(report_ceiling(10_000, 9_000, Some(4_096)), 10_000);
        assert_eq!(
            report_ceiling(10_000, 0, Some(4_096)),
            4_096,
            "absent FSMs cap the report at the bound"
        );
        assert_eq!(
            report_ceiling(10_000, u64::MAX, Some(4_096)),
            10_000,
            "saturating add"
        );
        assert_eq!(report_ceiling(10_000, 0, None), 10_000);
    }
}
