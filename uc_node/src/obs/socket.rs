// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! `<instance_dir>/obs.sock`: the obs router over a unix socket, so a
//! same-host reader (`uc2ctl`) needs no `[metrics]` listener. Trust is host
//! access plus the file's `0600` mode — the model `cnc2.dat` already uses.
//! `sun_path` holds 107 bytes; a longer instance dir is reached through
//! `/proc/self/fd/<dirfd>/obs.sock` (the node is Linux-only).

use std::fs::File;
use std::io::{self, Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub const OBS_SOCKET_FILE: &str = "obs.sock";
const SUN_PATH_MAX: usize = 107;

/// Run `f` on a path that names `<dir>/obs.sock` and fits `sun_path`.
fn with_short_path<T>(dir: &Path, f: impl FnOnce(&Path) -> io::Result<T>) -> io::Result<T> {
    let direct = dir.join(OBS_SOCKET_FILE);
    if direct.as_os_str().len() <= SUN_PATH_MAX {
        return f(&direct);
    }
    let handle = File::open(dir)?;
    let via: PathBuf = format!("/proc/self/fd/{}/{OBS_SOCKET_FILE}", handle.as_raw_fd()).into();
    let r = f(&via);
    drop(handle);
    r
}

/// Bind `<instance_dir>/obs.sock`, replacing a stale file (the caller holds
/// `instance.lock`, so no live node owns it), mode `0600`, non-blocking.
pub fn bind(instance_dir: &Path) -> io::Result<UnixListener> {
    let path = instance_dir.join(OBS_SOCKET_FILE);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e),
    }
    let l = with_short_path(instance_dir, |p| UnixListener::bind(p))?;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    l.set_nonblocking(true)?;
    Ok(l)
}

/// Connect to `<instance_dir>/obs.sock`. A file left by a killed node
/// refuses (`ConnectionRefused`).
pub fn connect(instance_dir: &Path) -> io::Result<UnixStream> {
    with_short_path(instance_dir, |p| UnixStream::connect(p))
}

/// One `GET` over the socket: `(status, body)`.
pub fn get(instance_dir: &Path, path: &str) -> io::Result<(u16, String)> {
    let mut s = connect(instance_dir)?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    s.set_write_timeout(Some(Duration::from_secs(5)))?;
    // One `write_all` of the whole request, not `write!` (which issues a
    // syscall per format piece): a peer that answers after its first read
    // and closes with request bytes still unread makes ours read ECONNRESET.
    let req = format!("GET {path} HTTP/1.1\r\nHost: obs\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes())?;
    let mut raw = String::new();
    s.read_to_string(&mut raw)?;
    let (head, body) = raw
        .split_once("\r\n\r\n")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no header terminator"))?;
    let code = head
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no status code"))?;
    Ok((code, body.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scratch dir on real disk. `CARGO_TARGET_TMPDIR` exists only for
    /// integration-test binaries, so these lib unit tests use the
    /// package-relative fallback `audit.rs`'s helper uses — never `/tmp`.
    fn dir(name: &str) -> std::path::PathBuf {
        let root =
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/uc_node_tests");
        let d = root.join(name);
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn echo_once(l: UnixListener) -> std::thread::JoinHandle<()> {
        std::thread::spawn(move || {
            use std::io::{Read, Write};
            // `bind` hands back a non-blocking listener (the obs loop polls
            // it); this one-shot echo wants to wait for its client.
            l.set_nonblocking(false).unwrap();
            let (mut s, _) = l.accept().unwrap();
            let mut buf = [0u8; 256];
            let _ = s.read(&mut buf).unwrap();
            s.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
                .unwrap();
        })
    }

    #[test]
    fn binds_0600_replaces_a_stale_file_and_round_trips() {
        use std::os::unix::fs::PermissionsExt;
        let d = dir("obs-sock-basic");
        std::fs::write(d.join(OBS_SOCKET_FILE), b"stale").unwrap();
        let l = bind(&d).unwrap();
        let mode = std::fs::metadata(d.join(OBS_SOCKET_FILE))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        let t = echo_once(l);
        assert_eq!(get(&d, "/cluster").unwrap(), (200, "ok".to_string()));
        t.join().unwrap();
    }

    #[test]
    fn a_path_longer_than_sun_path_binds_and_connects_through_proc_self_fd() {
        let long = "x".repeat(120);
        let d = dir(&format!("obs-sock-{long}"));
        assert!(d.join(OBS_SOCKET_FILE).as_os_str().len() > 107);
        let t = echo_once(bind(&d).unwrap());
        assert_eq!(get(&d, "/").unwrap().0, 200);
        t.join().unwrap();
    }

    #[test]
    fn a_leftover_socket_with_no_listener_refuses() {
        let d = dir("obs-sock-leftover");
        drop(bind(&d).unwrap());
        assert!(
            d.join(OBS_SOCKET_FILE).exists(),
            "dropping a listener leaves the file"
        );
        let e = connect(&d).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::ConnectionRefused);
    }
}
