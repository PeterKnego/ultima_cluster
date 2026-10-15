// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! The cluster view's serde types — the schema `uc2.cluster-view/1`.
//! Conventions (spec §5): hashes are `Hex64` strings, times RFC 3339
//! strings, any u64 that can exceed 2^53 a `DecU64` string, absent `null`.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A u64 hash as `"0x" + 16 lowercase hex digits`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Hex64(pub u64);

impl Serialize for Hex64 {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("0x{:016x}", self.0))
    }
}

impl<'de> Deserialize<'de> for Hex64 {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        let digits = s
            .strip_prefix("0x")
            .ok_or_else(|| serde::de::Error::custom("hash must start with 0x"))?;
        if digits.len() != 16 {
            return Err(serde::de::Error::custom("hash must have 16 hex digits"));
        }
        u64::from_str_radix(digits, 16)
            .map(Hex64)
            .map_err(serde::de::Error::custom)
    }
}

/// A u64 that can exceed 2^53, as a decimal string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecU64(pub u64);

impl Serialize for DecU64 {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.0.to_string())
    }
}

impl<'de> Deserialize<'de> for DecU64 {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map(DecU64).map_err(serde::de::Error::custom)
    }
}

/// A packed FSM version: the text an operator reads and the packed word a
/// tool compares (`uc_protocol::identity::pack_version`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionView {
    pub text: String,
    pub packed: u32,
}

impl VersionView {
    pub fn of(packed: u32) -> Self {
        VersionView {
            text: uc_protocol::identity::VersionDisplay(packed).to_string(),
            packed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    Live,
    Artifact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Leader,
    Follower,
    Learner,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaderRef {
    pub id: u32,
    /// The leader's replication address from the committed membership;
    /// `null` when it is not a member this node knows.
    pub addr: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterViewDoc {
    pub schema: String,
    pub source: Source,
    pub node_id: Option<u32>,
    pub role: Option<Role>,
    pub joined: Option<bool>,
    pub leader: Option<LeaderRef>,
    pub view_position: u64,
    pub commit: Option<u64>,
    pub read_at: String,
    pub cluster: ClusterPart,
    #[serde(rename = "self")]
    pub self_part: Option<SelfPart>,
    pub nodes: Option<Vec<NodeEntry>>,
    /// `"leader"` on a live non-leader (ask the leader for `nodes`), else null.
    pub nodes_from: Option<String>,
    pub attention: Vec<Attention>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterPart {
    pub membership: MembershipView,
    pub settings: SettingsView,
    pub schedule: ScheduleView,
    pub upgrades: Vec<UpgradeRowView>,
    pub catalog: CatalogView,
    pub tiers: Vec<TierView>,
    pub purge_gate: PurgeGateView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipView {
    pub config_version: u64,
    pub members: Vec<MemberView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberView {
    pub id: u32,
    pub addr: String,
    pub role: MemberRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberRole {
    Voter,
    Learner,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettingsView {
    pub position: u64,
    pub fsm_lag: FsmLagView,
    pub admission_bytes: DecU64,
    pub snapshot_interval_bytes: DecU64,
    pub snapshot_target: TargetView,
    /// Raw replicated word; `0` is the BASELINE rung (`datagram_mtu_rung`).
    pub datagram_mtu: u32,
    pub datagram_mtu_rung: String,
    pub retain_sets: u16,
    pub auto_fetch: bool,
    pub backup_retain_sets: u16,
    pub backup_retain_age_secs: DecU64,
    pub purge_requires_tiers: u8,
}

/// `Settings::fsm_lag_bytes`: `0` derived, `u64::MAX` lockstep, else bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum FsmLagView {
    Derived,
    Lockstep,
    Bytes { bytes: DecU64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetView {
    All,
    Learners,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleView {
    pub position: u64,
    pub entries: Vec<ScheduleEntryView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleEntryView {
    /// The row whose identity hash matches, `null` when no name matches.
    pub row_name: Option<String>,
    pub identity_hash: Hex64,
    pub timer_id: DecU64,
    pub rule: RuleView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RuleView {
    Every {
        period_ns: DecU64,
        anchor: String,
        anchor_ns: DecU64,
    },
    DailyAt {
        secs_of_day: u32,
    },
    Once {
        at: String,
        at_ns: DecU64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeRowView {
    pub row: u8,
    pub name: Option<String>,
    /// Every pin on this row, oldest first.
    pub pins: Vec<PinView>,
    pub running: Option<RunningView>,
    pub report: Option<ReportView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PinView {
    pub from: VersionView,
    pub to: VersionView,
    pub origin: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunningView {
    pub version: VersionView,
    pub record_pos: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportView {
    pub position: u64,
    pub verdict: HashVerdictView,
    pub majority_hash: Option<Hex64>,
    pub minority: Vec<u32>,
    pub nodes: Vec<ReportNodeView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportNodeView {
    pub node: u32,
    pub hash: Hex64,
    /// `null` when the report carries size `0` (unknown — a pre-lifecycle
    /// record), never a sentinel `0` (ruling P30).
    pub size: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HashVerdictView {
    Agreed,
    Diverged,
    NoMajority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogView {
    pub retain_sets: u16,
    /// Live: always `true`/`false` (`ClusterState::catalog_empty`). Offline:
    /// `false` when the artifact records a set before its own position as
    /// `Complete`, else `null` — never `true`, because the artifact at P is
    /// cut AT the instant and cannot record P's own completion (ruling P32).
    pub empty: Option<bool>,
    pub floor: Option<u64>,
    /// Oldest first.
    pub sets: Vec<SetView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetView {
    pub position: u64,
    pub kind: SetKindView,
    pub commanded_at: String,
    pub state: SetStateView,
    pub agreed: bool,
    pub stalled: bool,
    /// Names of the diverged rows (`"cluster"` for the cluster artifact,
    /// `"row<N>"` for a row with no known name).
    pub diverged_rows: Vec<String>,
    pub size: Option<u64>,
    /// Declared rows only (`ClusterState::declared_mask`).
    pub rows: Vec<RowEntryView>,
    pub cluster: RowEntryView,
    /// Leader only: node ids holding this set complete on disk.
    pub held_by: Option<Vec<u32>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetKindView {
    Full,
    Standby,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetStateView {
    Commanded,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowEntryView {
    /// The row id; `255` for the cluster artifact.
    pub row: u8,
    pub name: Option<String>,
    /// `null` while unreported.
    pub version: Option<VersionView>,
    pub hash: Option<Hex64>,
    pub verdict: VerdictView,
    pub size: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerdictView {
    Unreported,
    Agreed,
    Diverged,
    NoMajority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierView {
    /// `0x` + 16 lowercase hex digits, as `uc2ctl tier show` prints it and
    /// `tier forget --tier-id` parses it (ruling P29).
    pub id: String,
    pub oldest_set: u64,
    pub newest_set: u64,
    pub through: u64,
    pub reporter: u64,
    pub updated_position: u64,
    /// `commit - through` against this node's commit; `null` offline.
    pub lag_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PurgeGateView {
    pub requires_tiers: u8,
    /// `null` when `requires_tiers == 0`; `0` holds every purge (fewer
    /// than K tiers report — see `tiers_reporting`).
    pub position: Option<u64>,
    pub tiers_reporting: usize,
    /// This node's low-space release level; `null` offline.
    pub released: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SelfPart {
    pub holdings: HoldingsView,
    pub services: Vec<ServiceView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HoldingsView {
    pub journal_first: u64,
    pub durable: u64,
    pub commit: u64,
    pub applied: Vec<RowAppliedView>,
    /// `null` until the `uc2-holdings` probe's first successful `statvfs`
    /// (`auto_fetch::FREE_BYTES_UNKNOWN`), never a sentinel `0` (ruling P30).
    pub free_bytes: Option<u64>,
    /// An estimate; `0` is a real reading (an empty journal), not "unknown".
    pub journal_bytes: u64,
    /// `0` is a real reading (no `snapshots/` bytes), not "unknown".
    pub snapshots_bytes: u64,
    /// Set positions held complete on disk; `null` when the holdings were
    /// computed against a different catalog than the one rendered.
    pub sets_held: Option<Vec<u64>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowAppliedView {
    pub row: u8,
    pub name: Option<String>,
    pub applied: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceView {
    pub row: u8,
    pub name: Option<String>,
    pub attached: bool,
    pub version: Option<VersionView>,
    pub applied: u64,
    pub lag_bytes: u64,
    pub timers_pending: u64,
    pub pinned: bool,
    pub pinned_version: Option<VersionView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeEntry {
    pub id: u32,
    pub addr: String,
    pub role: MemberRole,
    /// `null` for a member the leader has never heard from.
    pub holdings: Option<HoldingsView>,
    /// `null` when never heard from (such an entry is `stale`).
    pub age_ms: Option<u64>,
    pub stale: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attention {
    pub code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lag_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node: Option<u32>,
}

impl Attention {
    pub fn code(code: &str) -> Self {
        Attention {
            code: code.to_string(),
            set: None,
            row: None,
            tier: None,
            lag_bytes: None,
            node: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_are_prefixed_hex_strings_and_round_trip() {
        let j = serde_json::to_string(&Hex64(0x3f9a)).unwrap();
        assert_eq!(j, "\"0x0000000000003f9a\"");
        assert_eq!(serde_json::from_str::<Hex64>(&j).unwrap(), Hex64(0x3f9a));
        assert!(serde_json::from_str::<Hex64>("\"3f9a\"").is_err());
        assert!(serde_json::from_str::<Hex64>("\"0x3f9a\"").is_err());
    }

    #[test]
    fn large_u64s_are_decimal_strings_and_round_trip() {
        let j = serde_json::to_string(&DecU64(u64::MAX)).unwrap();
        assert_eq!(j, "\"18446744073709551615\"");
        assert_eq!(
            serde_json::from_str::<DecU64>(&j).unwrap(),
            DecU64(u64::MAX)
        );
    }

    #[test]
    fn enums_are_lowercase_words_and_fsm_lag_is_tagged() {
        assert_eq!(
            serde_json::to_string(&VerdictView::NoMajority).unwrap(),
            "\"no_majority\""
        );
        assert_eq!(
            serde_json::to_string(&Role::Learner).unwrap(),
            "\"learner\""
        );
        assert_eq!(
            serde_json::to_string(&FsmLagView::Bytes { bytes: DecU64(7) }).unwrap(),
            r#"{"mode":"bytes","bytes":"7"}"#
        );
        assert_eq!(
            serde_json::to_string(&FsmLagView::Lockstep).unwrap(),
            r#"{"mode":"lockstep"}"#
        );
    }

    #[test]
    fn attention_omits_context_that_does_not_apply() {
        let mut a = Attention::code("set_diverged");
        a.set = Some(1024);
        a.row = Some("kv".into());
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            r#"{"code":"set_diverged","set":1024,"row":"kv"}"#
        );
    }

    #[test]
    fn a_version_carries_its_text_and_its_packed_word() {
        let v = serde_json::to_value(VersionView::of(0)).unwrap();
        assert_eq!(v["text"], "unversioned");
        assert_eq!(v["packed"], 0);
        // `self` as the key of `self_part` is pinned on whole documents by
        // Task 8's offline test and Task 5's golden example.
    }
}
