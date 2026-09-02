// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Startup refusals.
//!
//! Every rule here exists because the misconfiguration it catches currently
//! fails LATER and looks like something else. A node that refuses to start
//! with a message naming the offending field is strictly better than a
//! cluster that elects a leader and never commits.

use std::net::SocketAddr;

use uc_consensus::election::NodeId;
// The cnc PeerSlots band holds 8 entries; voters + learners share it. Reuse
// the wire crate's constant rather than a local literal — `uc_protocol`
// enforces this same cap at config-frame proposal AND at decode, and a
// second copy here could drift out of agreement with the wire.
use uc_protocol::v2::config::MAX_MEMBERS;
use uc_protocol::v2::crypto::CRYPTO_OVERHEAD;
use uc_protocol::v2::datagram::{DATAGRAM_HEADER_LEN, MTU_DEFAULT};
use uc_protocol::v2::frame::{FRAME_ALIGNMENT, HEADER_LEN, align_frame_len};

use crate::config_file::AdminSection;
use crate::obs::log::LogLevel;
use crate::{CryptoConfig, NodeConfig};

#[derive(Debug, thiserror::Error)]
pub enum PreflightError {
    #[error("buffer_bytes must be a power of two, got {0}")]
    BufferNotPowerOfTwo(usize),
    #[error("max_payload ({max_payload}) must be well below buffer_bytes ({buffer_bytes})")]
    PayloadTooLarge {
        max_payload: usize,
        buffer_bytes: usize,
    },
    #[error("this node's id ({0}) appears in neither members nor learners")]
    SelfNotAMember(NodeId),
    #[error("learners and members must be disjoint; id {0} appears in both")]
    LearnerIsAlsoMember(NodeId),
    #[error(
        "bind ({bind}) must be identical to this node's own members entry ({expected}) — \
         not a wildcard, not 0.0.0.0; a mismatch elects a leader whose followers never commit"
    )]
    BindMismatch { bind: String, expected: String },
    #[error("duplicate id {0} in members/learners")]
    DuplicateId(NodeId),
    #[error("cluster has {0} total members (voters + learners); the hard cap is 8")]
    TooManyMembers(usize),
    #[error("election_timeout_min_ns ({min}) must be < election_timeout_max_ns ({max})")]
    ElectionWindow { min: u64, max: u64 },
    #[error(
        "max_payload ({max_payload}) does not fit one datagram: a max-size frame needs \
         {need} bytes against an MTU of {mtu}. The node does not fragment frames — \
         uc_net::sender asserts this at construction, so an oversized value panics the \
         node instead of failing here."
    )]
    PayloadExceedsMtu {
        max_payload: usize,
        need: usize,
        mtu: usize,
    },
    #[error("instance_dir {path} does not exist or cannot be probed: {detail}")]
    InstanceDirUnprobeable { path: String, detail: String },
    #[error(
        "instance_dir {path} is on a RAM-backed filesystem ({fs}) — every fsync there is a \
         silent no-op, so the cluster will appear to work and lose committed data on power \
         loss. Put it on a real disk. For tests only, set allow_volatile_fs = true in the \
         config file (or UC2_ALLOW_VOLATILE_FS=1); the node will then start and warn on \
         every boot."
    )]
    VolatileFilesystem { path: String, fs: String },
}

/// Pure semantic checks over a built config. Filesystem checks are separate
/// (Task 3) so this stays unit-testable without touching disk.
pub fn check_semantics(cfg: &NodeConfig) -> Result<(), PreflightError> {
    if !cfg.buffer_bytes.is_power_of_two() {
        return Err(PreflightError::BufferNotPowerOfTwo(cfg.buffer_bytes));
    }
    // Stated as a division, NOT `max_payload * 4 > buffer_bytes`: the multiply
    // overflows for a large `max_payload`, which panics in debug and WRAPS in
    // release — silently admitting the very worst value the check exists to
    // refuse. `buffer_bytes` is a power of two by the check above, so the
    // division is exact.
    if cfg.max_payload > cfg.buffer_bytes / 4 {
        return Err(PreflightError::PayloadTooLarge {
            max_payload: cfg.max_payload,
            buffer_bytes: cfg.buffer_bytes,
        });
    }
    // A frame larger than one datagram panics `Sender::new` — a hard assert
    // deep in the transport, long after the config looked fine. Refuse it here
    // with the arithmetic spelled out. The node never overrides the sender's
    // `mtu`, so `MTU_DEFAULT` is the real budget.
    //
    // Computed with checked arithmetic rather than guarded by a `> MTU_DEFAULT`
    // sentinel. The sentinel was overflow-safe but reported `usize::MAX` as the
    // requirement, so a plausible `max_payload = 4096` refused with "needs
    // 18446744073709551615 bytes" instead of 4144 — on the one surface whose
    // whole job is refusing by name WITH THE NUMBERS SHOWN. Saturation is now
    // reserved for values too large to represent at all, which
    // `PayloadTooLarge` above has already refused.
    let overhead = if matches!(cfg.crypto, CryptoConfig::Enabled { .. }) {
        CRYPTO_OVERHEAD
    } else {
        0
    };
    let need = cfg
        .max_payload
        .checked_add(HEADER_LEN)
        // `align_frame_len` rounds UP by as much as FRAME_ALIGNMENT - 1 and
        // would wrap on its own; it takes no checked form, so bound its input.
        .filter(|total| *total <= usize::MAX - (FRAME_ALIGNMENT - 1))
        .map(align_frame_len)
        .and_then(|f| f.checked_add(DATAGRAM_HEADER_LEN))
        .and_then(|f| f.checked_add(overhead))
        .unwrap_or(usize::MAX);
    if need > MTU_DEFAULT {
        return Err(PreflightError::PayloadExceedsMtu {
            max_payload: cfg.max_payload,
            need,
            mtu: MTU_DEFAULT,
        });
    }
    if cfg.election_timeout_min_ns >= cfg.election_timeout_max_ns {
        return Err(PreflightError::ElectionWindow {
            min: cfg.election_timeout_min_ns,
            max: cfg.election_timeout_max_ns,
        });
    }

    let mut seen = std::collections::HashSet::new();
    for (id, _) in cfg.members.iter().chain(cfg.learners.iter()) {
        if !seen.insert(*id) {
            // A learner id colliding with a member id is the more specific
            // (and more confusing) case, so name it as such.
            if cfg.members.iter().any(|(m, _)| m == id) && cfg.learners.iter().any(|(l, _)| l == id)
            {
                return Err(PreflightError::LearnerIsAlsoMember(*id));
            }
            return Err(PreflightError::DuplicateId(*id));
        }
    }
    if seen.len() > MAX_MEMBERS {
        return Err(PreflightError::TooManyMembers(seen.len()));
    }

    let own = cfg
        .members
        .iter()
        .chain(cfg.learners.iter())
        .find(|(id, _)| *id == cfg.id)
        .ok_or(PreflightError::SelfNotAMember(cfg.id))?;
    if own.1 != cfg.bind {
        return Err(PreflightError::BindMismatch {
            bind: cfg.bind.to_string(),
            expected: own.1.to_string(),
        });
    }
    Ok(())
}

/// Startup policy that is NOT part of the node's runtime configuration.
///
/// M12b: no longer `Copy` — [`AdminSection`] carries a `Vec`/`String`
/// (`keys`), same reason [`crate::NodeConfig`] itself isn't `Copy`.
#[derive(Debug, Clone, Default)]
pub struct StartupOptions {
    /// Permit an instance directory on a RAM-backed filesystem.
    ///
    /// TEST AND DEVELOPMENT ONLY. Every `fsync` on such a filesystem is a
    /// silent no-op, so a cluster configured this way will appear to work and
    /// will lose committed data on power loss. Setting this never silences the
    /// startup warning — see [`FsVerdict::VolatileOverridden`].
    pub allow_volatile_fs: bool,
    /// M10's observability options — see [`ObsOptions`].
    pub obs: ObsOptions,
    /// M12b (spec §3.3, §5.1): `[admin]` as loaded from the config file —
    /// REQUIRED there ([`crate::config_file::ConfigError::AdminChoiceRequired`]
    /// on absence), so this field's own [`Default`] (`auth = none`, no keys)
    /// only ever shows up in test/harness code that builds a
    /// `StartupOptions` by hand rather than through
    /// [`crate::config_file::load_from_path`]. The daemon is what turns this
    /// into a real [`uc_crypto::admin::AdminPolicy`] — see
    /// `uc2-node.rs::main`.
    pub admin: AdminSection,
}

/// M10 observability options: `[log]`/`[metrics]` from the config file,
/// typed. Both sections are optional and, like `[purge]`/`[crypto]`, ABSENT
/// means the feature is off — no `[log]` keeps the default level (`info`);
/// no `[metrics]` means no endpoint. A later task (Task 7) is what actually
/// binds `metrics_bind` and applies `log_level`; this module only carries the
/// typed values out of the config file.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObsOptions {
    /// The structured-log level filter. Defaults to [`LogLevel::Info`] when
    /// `[log]` is absent or `level` is unstated.
    pub log_level: LogLevel,
    /// Where to bind the `/metrics`/`/healthz`/`/readyz` endpoint. `None`
    /// means `[metrics]` was absent — no endpoint. `Some` when the section
    /// was present, defaulting to `127.0.0.1:9600` if `bind` was unstated.
    pub metrics_bind: Option<SocketAddr>,
}

/// What the durability probe concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FsVerdict {
    /// The instance directory is on a filesystem whose `fsync` is real.
    Durable,
    /// It is NOT, and an override let the node start anyway. The caller MUST
    /// warn — the override suppresses the refusal, never the notice.
    VolatileOverridden { fs: String },
}

/// Filesystem magics that mean "this fsync is a lie".
#[cfg(target_os = "linux")]
const VOLATILE_MAGICS: &[i64] = &[
    0x0102_1994, // TMPFS_MAGIC
    0x8584_58f6, // RAMFS_MAGIC
];

/// Refuse an instance directory whose fsync is a no-op.
///
/// Mirrors `bench-infra/scripts/m6_fleet_gate.py:assert_durable_fs`, which has
/// refused this for gate runs since M6 — the node itself never did.
///
/// An override does not skip the probe: it downgrades the refusal to a
/// [`FsVerdict::VolatileOverridden`] the caller is obliged to warn about.
pub fn check_durable_fs(
    instance_dir: &std::path::Path,
    opts: &StartupOptions,
) -> Result<FsVerdict, PreflightError> {
    let overridden = opts.allow_volatile_fs || std::env::var_os("UC2_ALLOW_VOLATILE_FS").is_some();

    // On a first boot the instance dir does not exist yet, so fall back to its
    // parent — but EXACTLY one level. Walking further up would reach `/`, which
    // is on real disk on any normal host, and would launder every bad path into
    // a pass: the refusal below would become unreachable code.
    let probe = if instance_dir.exists() {
        instance_dir
    } else {
        match instance_dir.parent().filter(|p| p.exists()) {
            Some(p) => p,
            None => {
                return degrade(
                    overridden,
                    PreflightError::InstanceDirUnprobeable {
                        path: instance_dir.display().to_string(),
                        detail: "neither it nor its parent directory exists".to_string(),
                    },
                );
            }
        }
    };

    match fs_kind(probe) {
        Ok(FsKind::Durable) => Ok(FsVerdict::Durable),
        Ok(FsKind::Volatile(fs)) => degrade(
            overridden,
            PreflightError::VolatileFilesystem {
                path: instance_dir.display().to_string(),
                fs,
            },
        ),
        Err(e) => degrade(overridden, e),
    }
}

/// One place decides what an override does to a durability failure: it becomes
/// a verdict the caller must announce, never a silent success. Note this covers
/// an unprobeable path as well as a volatile one — the override means "start
/// despite a failed durability probe", whatever made the probe fail.
fn degrade(overridden: bool, err: PreflightError) -> Result<FsVerdict, PreflightError> {
    if overridden {
        Ok(FsVerdict::VolatileOverridden {
            fs: err.to_string(),
        })
    } else {
        Err(err)
    }
}

enum FsKind {
    Durable,
    Volatile(String),
}

#[cfg(target_os = "linux")]
fn fs_kind(path: &std::path::Path) -> Result<FsKind, PreflightError> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|e| {
        PreflightError::InstanceDirUnprobeable {
            path: path.display().to_string(),
            detail: e.to_string(),
        }
    })?;
    let mut buf: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is a valid NUL-terminated path; `buf` is a zeroed statfs we
    // own for the duration of the call.
    if unsafe { libc::statfs(c.as_ptr(), &mut buf) } != 0 {
        return Err(PreflightError::InstanceDirUnprobeable {
            path: path.display().to_string(),
            detail: std::io::Error::last_os_error().to_string(),
        });
    }
    // `statfs.f_type` is `__fsword_t`: already `i64` on 64-bit targets, but a
    // 32-bit int on 32-bit ones. The cast is a no-op on x86_64 — hence the
    // allow — and load-bearing anywhere else.
    #[allow(clippy::unnecessary_cast)]
    let magic = buf.f_type as i64;
    if VOLATILE_MAGICS.contains(&magic) {
        return Ok(FsKind::Volatile(format!("magic {magic:#x}")));
    }
    Ok(FsKind::Durable)
}

#[cfg(not(target_os = "linux"))]
fn fs_kind(path: &std::path::Path) -> Result<FsKind, PreflightError> {
    // macOS and friends: the fleet is Linux, and a dev box that cannot be
    // probed is not worth a false refusal. Existence is still required, so a
    // typo'd path is caught.
    if path.exists() {
        Ok(FsKind::Durable)
    } else {
        Err(PreflightError::InstanceDirUnprobeable {
            path: path.display().to_string(),
            detail: "path does not exist".into(),
        })
    }
}

/// Full preflight: semantics plus the durability probe.
pub fn check(cfg: &NodeConfig, opts: &StartupOptions) -> Result<FsVerdict, PreflightError> {
    check_semantics(cfg)?;
    check_durable_fs(&cfg.instance_dir, opts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PurgePolicy;
    use std::net::SocketAddr;

    fn base() -> NodeConfig {
        NodeConfig {
            id: 1,
            members: vec![
                (1, "10.0.0.1:9100".parse::<SocketAddr>().unwrap()),
                (2, "10.0.0.2:9100".parse::<SocketAddr>().unwrap()),
            ],
            learners: Vec::new(),
            bind: "10.0.0.1:9100".parse().unwrap(),
            instance_dir: std::path::PathBuf::from("/srv/uc2/n1"),
            app_id: "myapp".into(),
            buffer_bytes: 1 << 26,
            // 512 B — the same budget the examples use; a frame must fit one
            // datagram. See `a_payload_that_cannot_fit_a_datagram_is_refused`.
            max_payload: 512,
            admission_bytes: 256 * 1024,
            election_timeout_min_ns: 150_000_000,
            election_timeout_max_ns: 300_000_000,
            seed: 7,
            faults: Default::default(),
            purge: PurgePolicy::Disabled,
            journal_segment_bytes: crate::DEFAULT_JOURNAL_SEGMENT_BYTES,
            crypto: CryptoConfig::Disabled,
            services: crate::services::ServicesConfig::single("sm"),
        }
    }

    #[test]
    fn a_valid_config_passes() {
        assert!(check_semantics(&base()).is_ok());
    }

    #[test]
    fn buffer_bytes_must_be_power_of_two() {
        let mut c = base();
        c.buffer_bytes = (1 << 26) + 1;
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("buffer_bytes"), "got: {msg}");
    }

    #[test]
    fn max_payload_must_fit_the_buffer() {
        let mut c = base();
        c.max_payload = c.buffer_bytes;
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("max_payload"), "got: {msg}");
    }

    /// The check must REFUSE a payload so large that `max_payload * 4`
    /// overflows `usize` — a wrapping multiply would silently ADMIT the worst
    /// possible value. Debug builds would panic instead; both are wrong for a
    /// function whose entire job is refusing bad input.
    #[test]
    fn an_overflowing_max_payload_is_refused_not_wrapped() {
        let mut c = base();
        c.max_payload = usize::MAX / 2;
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("max_payload"), "got: {msg}");
    }

    /// The refusal must report the REAL byte requirement, not a sentinel.
    ///
    /// `max_payload = 4096` is an entirely plausible operator value: it is over
    /// the 1408 B MTU but well under `buffer_bytes / 4`, so `PayloadTooLarge`
    /// does not fire first and this is the message the operator actually sees.
    /// An overflow-avoidance sentinel made it read "needs 18446744073709551615
    /// bytes", which is wrong by fifteen orders of magnitude on the one surface
    /// whose entire job is refusing by name WITH THE NUMBERS SHOWN.
    #[test]
    fn the_mtu_refusal_reports_the_true_requirement_not_a_sentinel() {
        let mut c = base();
        c.max_payload = 4096;
        let err = check_semantics(&c).unwrap_err();
        let expected = align_frame_len(HEADER_LEN + 4096) + DATAGRAM_HEADER_LEN;
        match err {
            PreflightError::PayloadExceedsMtu {
                max_payload,
                need,
                mtu,
            } => {
                assert_eq!(max_payload, 4096);
                assert_eq!(mtu, MTU_DEFAULT);
                assert_eq!(need, expected, "must report what the frame actually needs");
                assert!(
                    need < 8192,
                    "a sentinel leaked into the operator's message: {need}"
                );
            }
            other => panic!("expected PayloadExceedsMtu, got {other:?}"),
        }
    }

    /// An oversized max_payload must be a NAMED refusal, not a panic inside
    /// `Sender::new`. This is the exact defect the shipped default carried:
    /// 1 MiB against a 1408 B MTU, which killed the daemon on its first boot.
    #[test]
    fn a_payload_that_cannot_fit_a_datagram_is_refused() {
        let mut c = base();
        c.max_payload = 1 << 20;
        c.buffer_bytes = 1 << 26; // large enough that the buffer rule is not what fires
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("max_payload"), "got: {msg}");
        assert!(
            msg.contains("datagram"),
            "must name the real constraint, got: {msg}"
        );
    }

    /// Enabling crypto shrinks the budget, so a payload that just fits in
    /// cleartext must be re-checked against the sealed size.
    #[test]
    fn the_mtu_budget_accounts_for_crypto_overhead() {
        let mut c = base();
        c.max_payload = MTU_DEFAULT - HEADER_LEN - DATAGRAM_HEADER_LEN;
        let cleartext = check_semantics(&c);
        c.crypto = CryptoConfig::Enabled {
            key_path: "/etc/uc2/node.key".into(),
            allowlist_path: "/etc/uc2/allowlist.toml".into(),
            rotation: Default::default(),
        };
        let sealed = check_semantics(&c);
        assert!(
            cleartext.is_err() || sealed.is_err(),
            "a payload at the raw MTU cannot fit once headers and any crypto tag are added"
        );
        if cleartext.is_ok() {
            assert!(
                sealed.is_err(),
                "crypto overhead must tighten the budget, not loosen it"
            );
        }
    }

    #[test]
    fn own_id_must_appear_in_members_or_learners() {
        let mut c = base();
        c.id = 99;
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("99"), "error must name the id, got: {msg}");
    }

    #[test]
    fn learner_ids_must_be_disjoint_from_members() {
        let mut c = base();
        c.learners = vec![(2, "10.0.0.9:9100".parse().unwrap())];
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("learners"), "got: {msg}");
    }

    #[test]
    fn bind_must_equal_this_nodes_own_member_entry() {
        // The failure this prevents: a leader elects, but followers never
        // advance durable/commit, because datagrams arrive from a source
        // address matching no member entry. See how-to/run-a-cluster.md.
        let mut c = base();
        c.bind = "0.0.0.0:9100".parse().unwrap();
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("bind"), "got: {msg}");
        assert!(
            msg.contains("10.0.0.1:9100"),
            "error must show the expected addr, got: {msg}"
        );
    }

    #[test]
    fn duplicate_member_ids_are_refused() {
        let mut c = base();
        c.members.push((1, "10.0.0.3:9100".parse().unwrap()));
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("duplicate"), "got: {msg}");
    }

    #[test]
    fn total_membership_is_capped_at_eight() {
        let mut c = base();
        c.members = (1..=9)
            .map(|i| (i, format!("10.0.0.{i}:9100").parse().unwrap()))
            .collect();
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains('8'), "got: {msg}");
    }

    /// The cap must track `uc_protocol`'s wire-enforced `MAX_MEMBERS`, not a
    /// local copy of the literal. If the PeerSlots band ever grows, a stale
    /// duplicate here would refuse configs the wire would happily accept.
    #[test]
    fn the_cap_is_the_protocols_cap() {
        assert_eq!(MAX_MEMBERS, uc_protocol::v2::config::MAX_MEMBERS);
    }

    // ---- Task 3: the durability probe ----

    /// `UC2_ALLOW_VOLATILE_FS` is process-global and cargo runs tests in
    /// parallel threads. Every test that reads or writes it takes this first.
    static FS_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Take the env lock, tolerating poisoning. A panicking test poisons the
    /// mutex, and `.lock().unwrap()` would then panic in every OTHER test that
    /// takes it — turning one genuine failure into a screenful of cascade and
    /// hiding which assertion actually broke.
    fn fs_lock() -> std::sync::MutexGuard<'static, ()> {
        FS_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn no_override() -> StartupOptions {
        StartupOptions {
            allow_volatile_fs: false,
            ..Default::default()
        }
    }

    /// A directory known to be RAM-backed, located via `/proc/mounts` — an
    /// oracle INDEPENDENT of the code under test.
    ///
    /// An earlier version asked `fs_kind` itself. That made every tmpfs test
    /// vacuous: corrupting `VOLATILE_MAGICS` simply made this return `None`,
    /// so the tests skipped instead of failing. Proven by mutation — with the
    /// wrong magic the suite must go RED, not quiet.
    ///
    /// Returns `None` only where no tmpfs is mounted at all, so an exotic
    /// environment skips rather than reporting a false defect.
    fn volatile_dir() -> Option<std::path::PathBuf> {
        let mounts = std::fs::read_to_string("/proc/mounts").ok()?;
        let mut any = None;
        for line in mounts.lines() {
            let mut f = line.split_whitespace();
            let (Some(_dev), Some(mp), Some(fstype)) = (f.next(), f.next(), f.next()) else {
                continue;
            };
            if fstype != "tmpfs" || !std::path::Path::new(mp).is_dir() {
                continue;
            }
            // Prefer the two conventionally writable ones.
            if mp == "/dev/shm" || mp == "/tmp" {
                return Some(std::path::PathBuf::from(mp));
            }
            any.get_or_insert_with(|| std::path::PathBuf::from(mp));
        }
        any
    }

    #[test]
    fn a_real_disk_directory_passes_the_fs_check() {
        let _g = fs_lock();
        // CARGO_MANIFEST_DIR, not CARGO_TARGET_TMPDIR: the latter is defined
        // only for integration tests and benches, never for a unit test in
        // src/. This crate's own source dir is on real disk.
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let v = check_durable_fs(dir, &no_override()).unwrap();
        assert_eq!(v, FsVerdict::Durable);
    }

    /// The rule this whole task exists for. Without this test nothing
    /// exercises `VOLATILE_MAGICS` at all — a wrong magic constant would let
    /// every other test pass while the node happily booted on tmpfs.
    #[test]
    fn a_tmpfs_instance_dir_is_refused() {
        let _g = fs_lock();
        let Some(vol) = volatile_dir() else {
            eprintln!("no RAM-backed fs available; skipping");
            return;
        };
        let msg = check_durable_fs(&vol.join("uc2-preflight-probe"), &no_override())
            .unwrap_err()
            .to_string();
        assert!(msg.contains("RAM-backed"), "got: {msg}");
        assert!(
            msg.contains("allow_volatile_fs"),
            "must name the escape hatch, got: {msg}"
        );
    }

    /// First boot: the leaf does not exist yet, so the probe must fall back to
    /// the parent — and must NOT keep walking up to `/`, which is on real disk
    /// and would launder any bad path into a pass.
    #[test]
    fn a_first_boot_instance_dir_is_probed_via_its_parent() {
        let _g = fs_lock();
        let leaf = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("no-such-dir-yet");
        assert!(!leaf.exists());
        assert_eq!(
            check_durable_fs(&leaf, &no_override()).unwrap(),
            FsVerdict::Durable
        );

        // And the same fallback on a volatile parent still refuses.
        if let Some(vol) = volatile_dir() {
            assert!(check_durable_fs(&vol.join("no-such-dir-yet"), &no_override()).is_err());
        }
    }

    #[test]
    fn a_missing_instance_dir_parent_is_refused() {
        let _g = fs_lock();
        let msg = check_durable_fs(
            std::path::Path::new("/nonexistent-xyzzy/n1"),
            &no_override(),
        )
        .unwrap_err()
        .to_string();
        assert!(msg.contains("nonexistent-xyzzy"), "got: {msg}");
    }

    #[test]
    fn the_config_override_bypasses_the_check_but_reports_it() {
        let _g = fs_lock();
        let Some(vol) = volatile_dir() else {
            eprintln!("no RAM-backed fs available; skipping");
            return;
        };
        let opts = StartupOptions {
            allow_volatile_fs: true,
            ..Default::default()
        };
        // The override must suppress the REFUSAL without suppressing the
        // NOTICE: a cluster on a RAM-backed fs must never look quiet.
        let v = check_durable_fs(&vol.join("uc2-preflight-probe"), &opts).unwrap();
        match v {
            FsVerdict::VolatileOverridden { ref fs } => {
                assert!(!fs.is_empty(), "the verdict must name what was overridden");
            }
            FsVerdict::Durable => panic!(
                "an overridden check must report VolatileOverridden, \
                 not Durable — otherwise the daemon prints nothing"
            ),
        }
    }

    #[test]
    fn the_env_override_also_bypasses_the_check_and_reports_it() {
        let _g = fs_lock();
        let Some(vol) = volatile_dir() else {
            eprintln!("no RAM-backed fs available; skipping");
            return;
        };
        unsafe { std::env::set_var("UC2_ALLOW_VOLATILE_FS", "1") };
        let v = check_durable_fs(&vol.join("uc2-preflight-probe"), &no_override());
        unsafe { std::env::remove_var("UC2_ALLOW_VOLATILE_FS") };
        assert!(
            matches!(v, Ok(FsVerdict::VolatileOverridden { .. })),
            "the env channel must behave exactly like the config channel"
        );
    }

    #[test]
    fn election_window_must_be_ordered() {
        let mut c = base();
        c.election_timeout_min_ns = c.election_timeout_max_ns + 1;
        let msg = check_semantics(&c).unwrap_err().to_string();
        assert!(msg.contains("election_timeout"), "got: {msg}");
    }
}
