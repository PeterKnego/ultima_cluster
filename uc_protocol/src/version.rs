/// Encoded as packed u32: (major:u8 << 24) | (minor:u8 << 16) | (patch:u16).
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ProtocolVersion(pub u32);

impl ProtocolVersion {
    pub const fn new(major: u8, minor: u8, patch: u16) -> Self {
        Self(((major as u32) << 24) | ((minor as u32) << 16) | (patch as u32))
    }
    pub const fn major(self) -> u8 {
        (self.0 >> 24) as u8
    }
    pub const fn minor(self) -> u8 {
        ((self.0 >> 16) & 0xFF) as u8
    }
    pub const fn patch(self) -> u16 {
        (self.0 & 0xFFFF) as u16
    }

    /// Compatible if same major and `other.minor <= self.minor`.
    pub const fn compatible_with(self, other: ProtocolVersion) -> bool {
        self.major() == other.major() && other.minor() <= self.minor()
    }
}

// 0.4.0: M8 wire-crypto layouts — key_epoch header field (datagram.rs),
// crypto envelope constants + kinds 18-20 (v2::crypto). No cnc-page change:
// M8 alters the UDP datagram format, not the shmem cnc page, so
// `CNC_V2_VERSION` is deliberately left untouched — bumping it would refuse
// old service/client binaries at local IPC attach over a change that cannot
// affect them.
//
// NB (updated 0.12.0, wire-level spec §3.2): the datagram receiver enforces
// `WIRE_MIN` since 0.12.0 — `uc_net::receiver`'s `wire_admit` refuses (and
// counts) any datagram whose header byte @13 is outside
// `[WIRE_MIN, WIRE_MAX + 1]`, before anything else in it is read. `WIRE_MAX`
// and `WIRE_MIN` derive from `CURRENT` below, but `CURRENT` itself — like
// `ProtocolVersion::compatible_with` — is still not compared anywhere; only
// the derived byte is. The version gated at every local IPC attach is the
// separate cnc-page field, checked by
// `uc_protocol::v2::cnc::version_compatible(local, peer)` over the packed
// `CNC_V2_VERSION` u32 (that module is `core`-only, so it re-spells the
// same-major / peer-minor-not-newer rule directly rather than depend on this
// type). The two version lines are INDEPENDENT, not lockstep:
// `CNC_V2_VERSION` gates the cnc shmem page format at local IPC attach and
// moved 2.0 → 3.0 in M14a (page grew to 8 KiB) without touching this
// constant.
// 0.5.0: content-attested durable reports — `DGRAM_KIND_APPEND_POSITION`
// gains an 8-byte body carrying the term the sender attributes to the byte
// below its reported position (`AppendPositionBody`, datagram.rs). The 16-byte
// header is unchanged; a 0.4.0 peer's header-only report decodes as
// "unattested" and is simply not counted toward quorum, so a mixed-version
// cluster degrades to stalled commits rather than to unsound ones. As with
// 0.4.0 the cnc page is untouched (`CNC_V2_VERSION` unchanged): this alters
// the UDP datagram format, not the shmem layout.
// 0.6.0 (M14c): `DGRAM_KIND_SNAP_BEGIN`'s body carries the per-FSM fields a
// multi-service snapshot session needs — `layout`, `service_id` and
// `services_declared` — growing its fixed part from 26 to 34 bytes
// (`SNAP_BEGIN_FIXED_LEN`). Every other datagram, the 16-byte header included,
// is byte-identical to 0.5.0, so a mixed 0.5.0/0.6.0 cluster replicates and
// elects normally; only a snapshot session between mixed versions goes wrong,
// and only the 0.6.0 side can detect it (the `layout` byte). **This constant
// is documentary and is not itself checked on any receive path** — the flag
// day rests on the standing operational rule (upgrade all nodes together,
// `docs/how-to/upgrade-a-cluster.md`), not on a version gate.
// 0.7.0 (FSM identity, spec §5): `DGRAM_KIND_SNAP_BEGIN`'s body drops the
// single `services_declared` bitmask and instead carries two per-row arrays
// — `identity` (8 × u64 FSM name hashes) and `version` (8 × u32 packed
// service versions), growing its fixed part from 34 to 122 bytes
// (`SNAP_BEGIN_FIXED_LEN`). A receiver now compares declared FSMs
// **positionally, by name** rather than by row-count alone, and refuses a
// 0.6.0-or-earlier body by its `layout` discriminator
// (`SNAP_BEGIN_LAYOUT_V2_RETIRED`). As with 0.6.0 this alters only the snapshot-
// session body; the flag day rests on the standing operational rule, not a
// version gate.
// 0.8.0 (jumbo frames, spec §4.2): two new pairwise datagram kinds,
// `DGRAM_KIND_PROBE = 24` (a path-MTU probe padded to the rung it tests) and
// `DGRAM_KIND_PROBE_ACK = 25` (the rung acknowledged plus the responder's own
// verified minimum). No existing layout changes. A 0.7.0 peer counts both as
// unknown kinds and drops them, so a mixed cluster never raises its ceiling —
// safe, and unsupported anyway (flag-day rule). Ships in `2.12.0`.
// 0.9.0 — `CLUSTER` kinds 4 (UpgradePin) and 5 (SnapshotReport); cnc 3.3. A
// 0.8.0 peer applies either as undecodable and silently diverges, hence the
// bump.
// 0.10.0 (#33): `CLUSTER` kind 6 `RowGenesis`; a 0.9.0 peer refuses it as
// undecodable and diverges silently — flag day.
// 0.11.0 (snapshot catalog): STATUS body 16 B → 144 B (layout word 2 in the
// old reserved slot), SNAP_REPORT/report row 255 = the cluster artifact,
// Settings v3 `retain_sets`, cluster image v4. No layout change on the
// replication path, and no wire-version word on node↔node datagrams — so
// nothing refuses a 0.10.0 peer by name, and mixing is unsound both ways: a
// 0.11.0 leader DROPS a 0.10.0 follower's 16 B STATUS (counted,
// `statuses_refused` / `status_refused`), so that follower's flow-control
// window never opens and replication to it stalls; a 0.10.0 leader accepts a
// 144 B body and ignores the tail; and a 0.10.0 peer drops the row-255 report
// as undecodable, so its catalog never completes a set, in silence.
// + backup tier 3a: `CLUSTER` kind 7 `BackupWatermark`, pairwise datagram 27
// `BACKUP_REPORT`, Settings v5, cluster image v6 — a 0.10.0 peer drops kind 27
// and refuses kind 7 as undecodable; same flag day.
// + backup tier 3b: pairwise 28 `TIER_INSTALL` / 29 `TIER_INSTALL_STATUS`,
// Settings v6, `SET` lineage — a 0.10.0 peer drops both kinds; same flag day.
// 0.12.0 (wire level, spec docs/superpowers/specs/2026-10-19-uc2-wire-level-design.md):
// the datagram header byte @13 carries the sender's WIRE_MAX (0 = a pre-level
// peer, refused by name); Settings v7 adds `wire_level`/`wire_raise`; NEW_TERM
// gains an 8-byte reserved body; cnc 3.5. A 0.11.0 peer's datagrams are
// REFUSED and counted (`prelevel`) — the first flag day a mixed cluster
// notices. From here a wire MINOR bump is a level (rolling), a MAJOR bump a
// flag day.
pub const CURRENT: ProtocolVersion = ProtocolVersion::new(0, 12, 0);

/// The level at which levels began. A pre-level Settings record (v1–v6)
/// decodes as this level, and `0.12.0` is the one release whose `WIRE_MIN`
/// equals its `WIRE_MAX`.
pub const WIRE_LEVEL_FIRST: u8 = 12;
/// The highest wire level this binary speaks: `CURRENT`'s minor.
pub const WIRE_MAX: u8 = CURRENT.minor();
/// The lowest committed level this binary may run at — one level back
/// (spec §1.3 item 4), by const arithmetic, never hand-edited.
pub const WIRE_MIN: u8 = if WIRE_MAX == WIRE_LEVEL_FIRST {
    WIRE_LEVEL_FIRST
} else {
    WIRE_MAX - 1
};
/// Enforced since 0.12.0: the receiver refuses a datagram whose wire byte
/// is below `WIRE_MIN` (spec §3.2).
pub const MIN_COMPATIBLE: ProtocolVersion = ProtocolVersion::new(0, WIRE_MIN, 0);

/// A binary's wire-level range (spec §2.1). `BINARY` is this build's; a
/// test-only override (`uc_node`'s `wire-level-override` feature) may
/// substitute another so one tree can play both sides of a roll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireRange {
    pub min: u8,
    pub max: u8,
}

impl WireRange {
    pub const BINARY: WireRange = WireRange {
        min: WIRE_MIN,
        max: WIRE_MAX,
    };

    /// Spec §2.3: a node runs only while the committed level is inside.
    pub const fn contains(self, level: u8) -> bool {
        self.min <= level && level <= self.max
    }

    /// Spec §3.2: a peer's header byte is accepted in `[min, max + 1]` — one
    /// above is a peer rolling ahead; `0` is a pre-level peer.
    pub const fn accepts_peer(self, byte: u8) -> bool {
        byte != 0 && self.min <= byte && byte <= self.max.saturating_add(1)
    }

    /// The cnc `wire_binary` word: `(min << 8) | max`.
    pub const fn pack(self) -> u64 {
        ((self.min as u64) << 8) | self.max as u64
    }

    pub const fn unpack(word: u64) -> WireRange {
        WireRange {
            min: ((word >> 8) & 0xFF) as u8,
            max: (word & 0xFF) as u8,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_round_trip() {
        let v = ProtocolVersion::new(1, 2, 3);
        assert_eq!(v.major(), 1);
        assert_eq!(v.minor(), 2);
        assert_eq!(v.patch(), 3);
    }

    #[test]
    fn compat_same_major_lower_minor_ok() {
        let a = ProtocolVersion::new(1, 5, 0);
        let b = ProtocolVersion::new(1, 3, 0);
        assert!(a.compatible_with(b));
    }

    #[test]
    fn compat_higher_minor_in_other_rejected() {
        let a = ProtocolVersion::new(1, 3, 0);
        let b = ProtocolVersion::new(1, 5, 0);
        assert!(!a.compatible_with(b));
    }

    #[test]
    fn compat_different_major_rejected() {
        let a = ProtocolVersion::new(1, 0, 0);
        let b = ProtocolVersion::new(2, 0, 0);
        assert!(!a.compatible_with(b));
    }

    #[test]
    fn current_is_the_wire_level_flag_day() {
        assert_eq!(CURRENT, ProtocolVersion::new(0, 12, 0));
        assert_eq!(WIRE_LEVEL_FIRST, 12);
        assert_eq!(WIRE_MAX, 12);
        // 0.12.0 is the only release whose min equals its max: nothing
        // below it is level-aware.
        assert_eq!(WIRE_MIN, 12);
        assert_eq!(MIN_COMPATIBLE, ProtocolVersion::new(0, WIRE_MIN, 0));
    }

    #[test]
    fn wire_range_contains_and_accepts_one_above() {
        let r = WireRange { min: 12, max: 13 };
        assert!(r.contains(12) && r.contains(13));
        assert!(!r.contains(11) && !r.contains(14));
        // a peer may be ONE level above me (it is rolling ahead) …
        assert!(r.accepts_peer(12) && r.accepts_peer(13) && r.accepts_peer(14));
        // … never a pre-level peer (0), below my min, or two above.
        assert!(!r.accepts_peer(0) && !r.accepts_peer(11) && !r.accepts_peer(15));
        assert_eq!(
            WireRange::BINARY,
            WireRange {
                min: WIRE_MIN,
                max: WIRE_MAX
            }
        );
    }

    #[test]
    fn wire_range_packs_min_high_max_low() {
        let r = WireRange { min: 12, max: 13 };
        assert_eq!(r.pack(), (12u64 << 8) | 13);
        assert_eq!(WireRange::unpack(r.pack()), r);
        assert_eq!(WireRange::unpack(0), WireRange { min: 0, max: 0 });
    }
}
