// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Node identity keys (X25519, one static key pair per node) and the peer
//! allowlist that maps a [`NodeId`] to the public key it is authorized to
//! present in the Noise IK handshake (T6). Neither type touches the network;
//! both are pure file-load + in-memory lookup, so the handshake code (which
//! *does* see attacker-controlled bytes) can treat "is this id/key pair
//! authorized" as an infallible in-memory `Option` check.

use crate::{CryptoError, NodeId};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use zeroize::Zeroizing;

/// A node's long-lived X25519 identity: the private half zeroized on drop,
/// the public half re-derived at load rather than trusted from a second
/// on-disk copy (a stale/edited public-key file next to the private one
/// would otherwise be a silent way to advertise the wrong identity).
pub struct Identity {
    private: Zeroizing<[u8; 32]>,
    public: [u8; 32],
}

impl Identity {
    /// Loads a 32-byte raw X25519 private key from `key_path`.
    ///
    /// Refuses (rather than warns on) a group- or world-readable key file on
    /// Unix — a private key an operator can read off disk by accident is a
    /// key an attacker with any foothold on the box can read too, and a
    /// misconfiguration here should fail loudly at startup, not degrade
    /// silently into "the handshake mysteriously doesn't trust anyone."
    pub fn load(key_path: &Path) -> Result<Identity, CryptoError> {
        crate::admin::check_key_file_perms(key_path)?;

        // Read straight into an already-`Zeroizing`-wrapped buffer: a
        // `std::fs::read` into a plain `Vec<u8>`, or a `TryInto<[u8; 32]>`
        // landing in a bare `let raw = ..;` binding, would each leave an
        // un-scrubbed copy of the private key sitting in freed memory after
        // this function returns — arrays are `Copy`, so even a `Zeroizing::
        // new(raw)` right after such a binding zeroizes only the wrapper's
        // copy, not the original `raw` slot. Writing directly into the
        // wrapped buffer means no unwrapped copy of the key ever exists.
        let invalid = || CryptoError::KeyFileInvalid(key_path.display().to_string());
        let mut file = std::fs::File::open(key_path)?;
        let mut private = Zeroizing::new([0u8; 32]);
        file.read_exact(&mut *private).map_err(|_| invalid())?;
        // `read_exact` is happy to read the first 32 bytes of a longer file;
        // confirm nothing follows so an oversized key file is rejected
        // outright rather than silently truncated.
        let mut probe = [0u8; 1];
        if file.read(&mut probe)? != 0 {
            return Err(invalid());
        }

        let secret = x25519_dalek::StaticSecret::from(*private);
        let public = x25519_dalek::PublicKey::from(&secret);

        Ok(Identity {
            private,
            public: public.to_bytes(),
        })
    }

    /// The raw 32-byte private scalar. Callers feed this straight into the
    /// Noise IK builder (T6); it never leaves the process otherwise.
    pub fn private_bytes(&self) -> &[u8; 32] {
        &self.private
    }

    /// The raw 32-byte public key, safe to publish in an allowlist line.
    pub fn public_bytes(&self) -> [u8; 32] {
        self.public
    }
}

/// Rate limit on how often [`Allowlist::reload_if_stale`] is willing to touch
/// the filesystem, in nanoseconds. See the type-level doc for why this is
/// driven by an explicit `now_ns`, not a clock read.
const RELOAD_MIN_INTERVAL_NS: u64 = 1_000_000_000;

/// The set of peers a node's handshake responder (T6) will accept: a static
/// `NodeId -> X25519 public key` map loaded from an operator-managed file,
/// one `<node id> <base64 public key> [# comment]` line per peer.
///
/// Held live (not just loaded once) so an operator can add/remove a peer
/// without restarting the node — see [`Allowlist::reload_if_stale`].
pub struct Allowlist {
    path: PathBuf,
    entries: Vec<(NodeId, [u8; 32])>,
    mtime: SystemTime,
    /// `now_ns` at the last reload *attempt* (disk touched or not), so
    /// `reload_if_stale` can rate-limit without a clock read. 0 until the
    /// first call to `reload_if_stale` — `load` itself doesn't take a
    /// `now_ns`, so there is no attempt to anchor to yet.
    last_reload_attempt_ns: u64,
}

impl Allowlist {
    pub fn load(path: &Path) -> Result<Allowlist, CryptoError> {
        let contents = std::fs::read_to_string(path)?;
        let entries = parse_allowlist(&contents)?;
        let mtime = std::fs::metadata(path)?.modified()?;
        Ok(Allowlist {
            path: path.to_path_buf(),
            entries,
            mtime,
            last_reload_attempt_ns: 0,
        })
    }

    pub fn lookup(&self, id: NodeId) -> Option<[u8; 32]> {
        self.entries
            .iter()
            .find(|(entry_id, _)| *entry_id == id)
            .map(|(_, key)| *key)
    }

    /// Re-reads the allowlist file if (a) at least [`RELOAD_MIN_INTERVAL_NS`]
    /// has elapsed since the last reload attempt, evaluated against the
    /// caller-supplied `now_ns` (this crate never reads the clock itself),
    /// AND (b) the file's mtime has actually changed. Returns whether the
    /// in-memory contents changed as a result.
    ///
    /// The rate limit is checked FIRST and short-circuits before any syscall
    /// — a caller polling this every tick under load must not turn into a
    /// `stat(2)` storm. Only once that gate passes does this touch disk to
    /// check mtime (and, if it moved, re-read and re-parse the file).
    pub fn reload_if_stale(&mut self, now_ns: u64) -> Result<bool, CryptoError> {
        if now_ns.saturating_sub(self.last_reload_attempt_ns) < RELOAD_MIN_INTERVAL_NS {
            return Ok(false);
        }
        self.last_reload_attempt_ns = now_ns;

        let mtime = std::fs::metadata(&self.path)?.modified()?;
        if mtime == self.mtime {
            return Ok(false);
        }

        let contents = std::fs::read_to_string(&self.path)?;
        let entries = parse_allowlist(&contents)?;
        self.entries = entries;
        self.mtime = mtime;
        Ok(true)
    }
}

/// Parses allowlist file contents into `(id, public key)` pairs.
///
/// A malformed line is a hard error, not a skip: a silently-dropped bad line
/// is an authorization hole (the operator believes a peer is listed when it
/// isn't) rather than a merely cosmetic one, so this never degrades to
/// "best-effort" parsing. Blank lines and `#`-comment lines are the only
/// lines allowed to be absent of an entry.
fn parse_allowlist(contents: &str) -> Result<Vec<(NodeId, [u8; 32])>, CryptoError> {
    let mut entries: Vec<(NodeId, [u8; 32])> = Vec::new();

    for (idx, raw_line) in contents.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let malformed = || CryptoError::MalformedAllowlist { line: line_no };
        let mut fields = line.split_whitespace();
        let id_field = fields.next().ok_or_else(malformed)?;
        let key_field = fields.next().ok_or_else(malformed)?;
        // Any further whitespace-separated fields are an operator comment
        // (e.g. a hostname) and are intentionally ignored.

        let id: NodeId = id_field.parse().map_err(|_| malformed())?;
        let key_bytes = BASE64.decode(key_field).map_err(|_| malformed())?;
        let key: [u8; 32] = key_bytes.as_slice().try_into().map_err(|_| malformed())?;

        if entries.iter().any(|(existing_id, _)| *existing_id == id) {
            return Err(CryptoError::DuplicateAllowlistId(id));
        }
        entries.push((id, key));
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Scratch root on real ext4 (`target/`), never `/tmp` (RAM-backed tmpfs,
    /// no swap on the dev box — CLAUDE.md). `CARGO_TARGET_TMPDIR` is
    /// integration-test-only (`tests/*.rs` binaries); these are inline
    /// `#[cfg(test)]` unit tests in the lib target, which never get it set
    /// (confirmed: `env!("CARGO_TARGET_TMPDIR")` fails to compile here) — same
    /// gap already documented for examples in
    /// `uc_node/examples/read_profile.rs`'s `scratch_root`. Falls back to a
    /// package-relative `target/` directory instead.
    fn tmp() -> std::path::PathBuf {
        let d = std::env::var("CARGO_TARGET_TMPDIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../target/uc_crypto_tests")
            })
            .join("uc2-crypto-identity");
        std::fs::create_dir_all(&d).unwrap();
        assert!(
            !d.starts_with("/tmp"),
            "test scratch must not live on tmpfs: {d:?}"
        );
        d
    }

    // Fixed private-key bytes for the test fixtures. Deliberately arbitrary
    // (not all-zero, not a recognizable pattern) so a reader can see this is
    // a real X25519 scalar, not a stand-in placeholder. Public keys are
    // DERIVED below (`b64_key_a`/`b64_key_b`), never pasted as opaque
    // base64 blobs, so the relationship between the private fixture used by
    // the permission/derivation tests and the public fixture used by the
    // allowlist tests is visible in this file rather than asserted by fiat.
    const PRIV_KEY_A: [u8; 32] = [
        0x1a, 0x2b, 0x3c, 0x4d, 0x5e, 0x6f, 0x70, 0x81, 0x92, 0xa3, 0xb4, 0xc5, 0xd6, 0xe7, 0xf8,
        0x09, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff, 0x01,
    ];
    const PRIV_KEY_B: [u8; 32] = [
        0xf0, 0xe1, 0xd2, 0xc3, 0xb4, 0xa5, 0x96, 0x87, 0x78, 0x69, 0x5a, 0x4b, 0x3c, 0x2d, 0x1e,
        0x0f, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22, 0x11,
        0x00, 0x02,
    ];

    /// Derives the base64 X25519 public key for a fixed private-key fixture
    /// — the fixtures are generated here, not pasted as opaque constants.
    fn derive_b64_public(priv_key: [u8; 32]) -> String {
        let secret = x25519_dalek::StaticSecret::from(priv_key);
        let public = x25519_dalek::PublicKey::from(&secret);
        BASE64.encode(public.as_bytes())
    }

    fn b64_key_a() -> String {
        derive_b64_public(PRIV_KEY_A)
    }

    fn b64_key_b() -> String {
        derive_b64_public(PRIV_KEY_B)
    }

    fn write_private_key(path: &std::path::Path, key: [u8; 32]) {
        std::fs::write(path, key).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
    }

    #[test]
    fn allowlist_parses_id_and_base64_key() {
        let p = tmp().join("allow-ok");
        let mut f = std::fs::File::create(&p).unwrap();
        // node id, base64 X25519 public key, optional trailing comment
        writeln!(f, "1 {} node-one", b64_key_a()).unwrap();
        writeln!(f, "# a comment line is ignored").unwrap();
        writeln!(f).unwrap();
        writeln!(f, "2 {}", b64_key_b()).unwrap();
        drop(f);

        let a = Allowlist::load(&p).unwrap();
        assert!(a.lookup(1).is_some());
        assert!(a.lookup(2).is_some());
        assert_ne!(a.lookup(1), a.lookup(2));
        assert_eq!(a.lookup(3), None, "unlisted id is not authorized");
    }

    #[test]
    fn allowlist_rejects_a_malformed_line_rather_than_skipping_it() {
        // A silently-skipped bad line is an authorization hole: the operator
        // thinks a peer is listed when it is not.
        let p = tmp().join("allow-bad");
        std::fs::write(&p, format!("1 {}\nnot-a-line\n", b64_key_a())).unwrap();
        assert!(matches!(
            Allowlist::load(&p),
            Err(CryptoError::MalformedAllowlist { line: 2 })
        ));
    }

    #[test]
    fn allowlist_rejects_a_duplicate_id() {
        let p = tmp().join("allow-dup");
        std::fs::write(&p, format!("1 {}\n1 {}\n", b64_key_a(), b64_key_b())).unwrap();
        assert!(matches!(
            Allowlist::load(&p),
            Err(CryptoError::DuplicateAllowlistId(1))
        ));
    }

    #[test]
    fn identity_refuses_a_world_readable_private_key() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let p = tmp().join("key-loose");
            std::fs::write(&p, PRIV_KEY_A).unwrap();
            std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(matches!(
                Identity::load(&p),
                Err(CryptoError::KeyFilePermissions { .. })
            ));
        }
    }

    #[test]
    fn identity_public_key_is_derived_not_stored() {
        let p = tmp().join("key-ok");
        write_private_key(&p, PRIV_KEY_A);
        let id = Identity::load(&p).unwrap();
        assert_eq!(id.public_bytes().len(), 32);
        assert_ne!(id.public_bytes(), [0u8; 32]);
    }

    #[test]
    fn reload_if_stale_respects_the_rate_limit_before_touching_disk() {
        let p = tmp().join("allow-reload-rate-limit");
        std::fs::write(&p, format!("1 {}\n", b64_key_a())).unwrap();
        let mut a = Allowlist::load(&p).unwrap();
        assert!(a.lookup(2).is_none());

        // Real fs mtime must actually move across the edit below.
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(&p, format!("1 {}\n2 {}\n", b64_key_a(), b64_key_b())).unwrap();

        // <1s since the load-time anchor (0): rate-limited, must not reload
        // even though the file on disk already changed.
        assert!(!a.reload_if_stale(500_000_000).unwrap());
        assert!(
            a.lookup(2).is_none(),
            "a rate-limited call must not touch disk"
        );

        // >=1s since the anchor: the gate opens and the changed mtime causes
        // an actual reload.
        assert!(a.reload_if_stale(1_000_000_000).unwrap());
        assert!(a.lookup(2).is_some());
    }

    #[test]
    fn reload_if_stale_is_a_no_op_when_unchanged_even_after_the_rate_limit_elapses() {
        let p = tmp().join("allow-reload-noop");
        std::fs::write(&p, format!("1 {}\n", b64_key_a())).unwrap();
        let mut a = Allowlist::load(&p).unwrap();

        assert!(!a.reload_if_stale(2_000_000_000).unwrap());
        assert!(a.lookup(1).is_some());
    }
}
