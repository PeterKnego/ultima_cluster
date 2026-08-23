// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Admin-plane authentication (M12b spec §5.1–§5.2): named HMAC-SHA256 admin
//! keys, the canonical request message both sides sign over, and the shared
//! key-file hygiene helpers (`Identity::load` in `identity.rs` uses the same
//! `check_key_file_perms` rule).
//!
//! [`AdminPolicy`] is the seam `uc2_node` and `uc2ctl` both build on: the
//! default `Filesystem` policy carries forward today's "the OS file
//! permissions on the instance dir are the only admin auth" posture, and
//! `Hmac` opts a cluster into named, replay-bounded (`ttl`) request
//! signatures without either crate re-deriving the canonical byte layout or
//! the constant-time comparison independently.

use crate::CryptoError;
use hmac::{Hmac, Mac};
use rand::rngs::OsRng;
use rand::TryRngCore;
use sha2::Sha256;
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use zeroize::Zeroizing;

/// Length in bytes of an admin HMAC key.
pub const ADMIN_KEY_LEN: usize = 32;
/// Length in bytes of an admin HMAC-SHA256 tag.
pub const ADMIN_TAG_LEN: usize = 32;

/// The standard 64-bit FNV-1a hash, used to derive `AdminKey::name_hash` from
/// an operator-chosen key name so wire messages can name a key by an 8-byte
/// hash instead of an unbounded string.
pub fn fnv1a64(name: &str) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut hash = OFFSET_BASIS;
    for byte in name.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// A named admin HMAC key. The secret zeroizes on drop; `name_hash` is what
/// travels on the wire (see `AdminMessage`'s `id` field family — callers
/// match a received `key_name_hash` against `AdminPolicy::key_by_hash`).
pub struct AdminKey {
    pub name: String,
    pub name_hash: u64,
    secret: Zeroizing<[u8; ADMIN_KEY_LEN]>,
}

impl fmt::Debug for AdminKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdminKey")
            .field("name", &self.name)
            .field("name_hash", &self.name_hash)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl AdminKey {
    /// Builds an `AdminKey` from an already-loaded 32-byte secret.
    pub fn new(name: &str, secret: [u8; ADMIN_KEY_LEN]) -> Self {
        AdminKey {
            name: name.to_string(),
            name_hash: fnv1a64(name),
            secret: Zeroizing::new(secret),
        }
    }

    /// Loads a named admin key from `path`: checks file permissions (the
    /// shared 0o077 rule), then reads exactly [`ADMIN_KEY_LEN`] bytes —
    /// anything else (short or long) is refused rather than truncated or
    /// zero-padded.
    pub fn load(name: &str, path: &Path) -> Result<AdminKey, CryptoError> {
        check_key_file_perms(path)?;
        let bytes = std::fs::read(path)?;
        if bytes.len() != ADMIN_KEY_LEN {
            return Err(CryptoError::AdminKeyLength {
                path: path.display().to_string(),
                len: bytes.len(),
            });
        }
        let mut secret = [0u8; ADMIN_KEY_LEN];
        secret.copy_from_slice(&bytes);
        Ok(AdminKey::new(name, secret))
    }
}

/// How a node/`uc2ctl` client authenticates admin requests. `Filesystem`
/// (the `Default`) is today's posture: instance-dir file permissions are the
/// only gate, unchanged by M12b. `Hmac` opts into named, TTL-bounded request
/// signatures — `keys` is `Arc`-shared so hot-reload can swap the whole set
/// in one atomic pointer store without a lock on the request path.
#[derive(Clone, Default)]
pub enum AdminPolicy {
    #[default]
    Filesystem,
    Hmac {
        keys: Arc<Vec<AdminKey>>,
        ttl: Duration,
    },
}

impl fmt::Debug for AdminPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AdminPolicy::Filesystem => write!(f, "AdminPolicy::Filesystem"),
            AdminPolicy::Hmac { keys, ttl } => f
                .debug_struct("AdminPolicy::Hmac")
                .field("keys", &format!("<{} redacted>", keys.len()))
                .field("ttl", ttl)
                .finish(),
        }
    }
}

impl AdminPolicy {
    /// Looks up an admin key by its `name_hash`. Always `None` under
    /// `Filesystem`.
    pub fn key_by_hash(&self, h: u64) -> Option<&AdminKey> {
        match self {
            AdminPolicy::Filesystem => None,
            AdminPolicy::Hmac { keys, .. } => keys.iter().find(|k| k.name_hash == h),
        }
    }
}

/// The fields an admin request's HMAC tag covers, and the one canonical byte
/// layout both the signer (`uc2ctl`) and the verifier (`uc2_node`) build
/// independently from the same wire fields — see `canonical_bytes` for the
/// exact layout, pinned by test (b) in this module.
pub struct AdminMessage<'a> {
    pub app_id: &'a str,
    pub instance_id: u128,
    pub seq: u64,
    pub nonce: u64,
    pub op: u32,
    pub id: u32,
    pub ip: u32,
    pub port: u16,
    pub expiry_ns: u64,
}

impl AdminMessage<'_> {
    /// Serializes this message into the exact byte layout the HMAC tag
    /// covers:
    ///
    /// `u16 LE len(app_id) ++ app_id bytes ++ instance_id u128 LE ++ seq u64
    /// LE ++ nonce u64 LE ++ op u32 LE ++ id u32 LE ++ ip u32 LE ++ port u16
    /// LE ++ expiry_ns u64 LE`
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let app_id_bytes = self.app_id.as_bytes();
        let len: u16 = app_id_bytes
            .len()
            .try_into()
            .expect("app_id longer than u16::MAX");
        let mut out = Vec::with_capacity(2 + app_id_bytes.len() + 16 + 8 + 8 + 4 + 4 + 4 + 2 + 8);
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(app_id_bytes);
        out.extend_from_slice(&self.instance_id.to_le_bytes());
        out.extend_from_slice(&self.seq.to_le_bytes());
        out.extend_from_slice(&self.nonce.to_le_bytes());
        out.extend_from_slice(&self.op.to_le_bytes());
        out.extend_from_slice(&self.id.to_le_bytes());
        out.extend_from_slice(&self.ip.to_le_bytes());
        out.extend_from_slice(&self.port.to_le_bytes());
        out.extend_from_slice(&self.expiry_ns.to_le_bytes());
        out
    }
}

type HmacSha256 = Hmac<Sha256>;

/// Signs `m` under `key` with HMAC-SHA256 over `m.canonical_bytes()`.
pub fn sign(key: &AdminKey, m: &AdminMessage<'_>) -> [u8; ADMIN_TAG_LEN] {
    let mut mac =
        HmacSha256::new_from_slice(&*key.secret).expect("HMAC accepts any key length");
    mac.update(&m.canonical_bytes());
    mac.finalize().into_bytes().into()
}

/// Verifies `tag` against `m` under `key` in constant time
/// (`hmac::Mac::verify_slice`) — never compare tags with `==`.
pub fn verify(key: &AdminKey, m: &AdminMessage<'_>, tag: &[u8; ADMIN_TAG_LEN]) -> bool {
    let mut mac =
        HmacSha256::new_from_slice(&*key.secret).expect("HMAC accepts any key length");
    mac.update(&m.canonical_bytes());
    mac.verify_slice(tag).is_ok()
}

/// The shared key-file hygiene rule: refuse a group- or world-readable key
/// file on Unix. Moved out of `identity.rs::Identity::load` so both X25519
/// identity files and admin HMAC key files enforce the identical rule from
/// one place.
pub fn check_key_file_perms(path: &Path) -> Result<(), CryptoError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::metadata(path)?;
        let mode = meta.mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(CryptoError::KeyFilePermissions {
                path: path.display().to_string(),
                mode,
            });
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

/// Generates a fresh 32-byte random admin key file at `path`: `create_new`
/// (refuses to overwrite an existing file) + mode 0600 from the moment the
/// file is created, so no window exists where the key is on disk but
/// world-readable.
pub fn generate_key_file(path: &Path) -> Result<(), CryptoError> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;

    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true).mode(0o600);
    let mut file = opts.open(path).map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            CryptoError::AdminKeyExists {
                path: path.display().to_string(),
            }
        } else {
            CryptoError::Io(e)
        }
    })?;

    let mut secret = [0u8; ADMIN_KEY_LEN];
    OsRng
        .try_fill_bytes(&mut secret)
        .expect("the OS RNG is unavailable");
    file.write_all(&secret)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hmac::Mac;
    use std::io::Write;

    // `CARGO_TARGET_TMPDIR` is integration-test-only (`tests/*.rs` binaries);
    // these are inline `#[cfg(test)]` unit tests in the lib target, which
    // never get it set — same gap already documented in
    // `identity.rs`'s own `tmp()` helper. Falls back to a package-relative
    // `target/` directory on real disk, never `/tmp` (RAM-backed tmpfs, no
    // swap on the dev box — CLAUDE.md).
    fn scratch_root() -> std::path::PathBuf {
        let d = std::env::var("CARGO_TARGET_TMPDIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| {
                std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc2_crypto_tests")
            });
        std::fs::create_dir_all(&d).unwrap();
        assert!(
            !d.starts_with("/tmp"),
            "test scratch must not live on tmpfs: {d:?}"
        );
        d
    }

    fn tmp() -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix("uc2-crypto-admin-")
            .tempdir_in(scratch_root())
            .unwrap()
    }

    fn write_key_file(path: &Path, bytes: &[u8], mode: u32) {
        let mut f = std::fs::File::create(path).unwrap();
        f.write_all(bytes).unwrap();
        drop(f);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
        }
    }

    // (a) fnv1a64 pinned constants. Hand-computed once via a throwaway
    // Python script implementing the standard 64-bit FNV-1a (offset basis
    // 0xcbf29ce484222325, prime 0x100000001b3) and pinned here so any future
    // change to the implementation is caught by this test, not silently
    // accepted.
    #[test]
    fn fnv1a64_matches_pinned_constants() {
        assert_eq!(fnv1a64("ops-alice"), 0xb586c043776657b2);
        assert_eq!(fnv1a64(""), 0xcbf29ce484222325);
    }

    fn fixture_message() -> AdminMessage<'static> {
        AdminMessage {
            app_id: "myapp",
            instance_id: 1,
            seq: 7,
            nonce: 9,
            op: 1,
            id: 2,
            ip: 0x7f000001,
            port: 9100,
            expiry_ns: 1_000,
        }
    }

    // (b) fixed HMAC vector + canonical layout length assertion.
    #[test]
    fn sign_matches_hmac_over_canonical_bytes_and_layout_length() {
        let m = fixture_message();
        let bytes = m.canonical_bytes();
        assert_eq!(
            bytes.len(),
            2 + 5 + 16 + 8 + 8 + 4 + 4 + 4 + 2 + 8,
            "canonical layout length must match the documented field widths"
        );

        let key_bytes = [0x0b; 32];
        let key = AdminKey::new("vector-key", key_bytes);

        let mut mac = Hmac::<Sha256>::new_from_slice(&key_bytes).unwrap();
        mac.update(&bytes);
        let expected: [u8; 32] = mac.finalize().into_bytes().into();

        let tag = sign(&key, &m);
        assert_eq!(tag, expected);
        assert!(verify(&key, &m, &tag));
    }

    // (c) verify is false on any flipped byte of tag or message.
    #[test]
    fn verify_rejects_any_tampering() {
        let key = AdminKey::new("vector-key", [0x0b; 32]);
        let m = fixture_message();
        let tag = sign(&key, &m);

        let mut bad_tag = tag;
        bad_tag[0] ^= 0x01;
        assert!(!verify(&key, &m, &bad_tag));

        let mut tampered = fixture_message();
        tampered.seq += 1;
        assert!(!verify(&key, &tampered, &tag));

        let mut tampered_ip = fixture_message();
        tampered_ip.ip ^= 1;
        assert!(!verify(&key, &tampered_ip, &tag));
    }

    // (d) AdminKey::load permission + length checks.
    #[test]
    fn load_refuses_a_group_or_world_readable_key_file() {
        let dir = tmp();
        let p = dir.path().join("admin.key");
        write_key_file(&p, &[0x22; 32], 0o644);
        assert!(matches!(
            AdminKey::load("ops", &p),
            Err(CryptoError::KeyFilePermissions { .. })
        ));
    }

    #[test]
    fn load_refuses_a_wrong_length_key_file() {
        let dir = tmp();
        let p = dir.path().join("admin.key");
        write_key_file(&p, &[0x22; 31], 0o600);
        match AdminKey::load("ops", &p) {
            Err(CryptoError::AdminKeyLength { len, .. }) => assert_eq!(len, 31),
            other => panic!("expected AdminKeyLength, got {other:?}"),
        }
    }

    #[test]
    fn load_accepts_a_0600_32_byte_key_file() {
        let dir = tmp();
        let p = dir.path().join("admin.key");
        write_key_file(&p, &[0x22; 32], 0o600);
        let key = AdminKey::load("ops-alice", &p).unwrap();
        assert_eq!(key.name, "ops-alice");
        assert_eq!(key.name_hash, fnv1a64("ops-alice"));
    }

    // (e) generate_key_file: 0600/32 bytes, refuses to overwrite.
    #[test]
    fn generate_key_file_creates_0600_32_bytes_and_refuses_overwrite() {
        let dir = tmp();
        let p = dir.path().join("generated.key");
        generate_key_file(&p).unwrap();

        let meta = std::fs::metadata(&p).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            assert_eq!(meta.mode() & 0o777, 0o600);
        }
        assert_eq!(meta.len(), ADMIN_KEY_LEN as u64);

        assert!(matches!(
            generate_key_file(&p),
            Err(CryptoError::AdminKeyExists { .. })
        ));
    }

    #[test]
    fn generate_key_file_output_is_loadable() {
        let dir = tmp();
        let p = dir.path().join("generated2.key");
        generate_key_file(&p).unwrap();
        let key = AdminKey::load("ops-bob", &p).unwrap();
        assert_eq!(key.name_hash, fnv1a64("ops-bob"));
    }
}
