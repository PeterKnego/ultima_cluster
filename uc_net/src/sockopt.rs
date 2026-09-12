// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Socket options the replication socket needs and `std` does not expose.
//!
//! Jumbo spec §4.3: with do-not-fragment set, a datagram larger than the
//! route MTU fails locally with `EMSGSIZE` and never leaves the host, and
//! one larger than a downstream hop is dropped there — so a probe that is
//! acked was carried WHOLE, and a data send that does not fit is a counted
//! failure rather than a silent fragment (one lost fragment loses the whole
//! datagram, which surfaces as a mystery NAK storm).
//!
//! **Linux and Android only.** The three options this needs
//! (`IP_MTU_DISCOVER`, `IPV6_MTU_DISCOVER`, `IPV6_DONTFRAG`) exist in the
//! `libc` crate for those targets alone, and DF is load-bearing rather than
//! decorative here: without it the kernel fragments and a probe is acked for
//! a size the path does not carry, so discovery over-reports. Every other OS
//! therefore gets an `ErrorKind::Unsupported` error, which `Node::start_with`
//! propagates — a node on such a host refuses to start, BY NAME, instead of
//! running with a silently unsound ladder.

use std::io;
use std::net::UdpSocket;
#[cfg(any(target_os = "linux", target_os = "android"))]
use std::os::fd::AsRawFd;

/// Set DF on `sock` for its address family: `IP_MTU_DISCOVER =
/// IP_PMTUDISC_DO` for IPv4, `IPV6_MTU_DISCOVER = IPV6_PMTUDISC_DO` plus
/// `IPV6_DONTFRAG = 1` for IPv6.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub fn set_dont_fragment(sock: &UdpSocket) -> io::Result<()> {
    let fd = sock.as_raw_fd();
    let v6 = sock.local_addr()?.is_ipv6();
    if v6 {
        setsockopt(
            fd,
            libc::IPPROTO_IPV6,
            libc::IPV6_MTU_DISCOVER,
            libc::IPV6_PMTUDISC_DO,
        )?;
        setsockopt(fd, libc::IPPROTO_IPV6, libc::IPV6_DONTFRAG, 1)?;
    } else {
        setsockopt(
            fd,
            libc::IPPROTO_IP,
            libc::IP_MTU_DISCOVER,
            libc::IP_PMTUDISC_DO,
        )?;
    }
    Ok(())
}

/// The non-Linux arm of [`set_dont_fragment`]: refuse, by name. See the
/// module doc — running without DF would over-report the path.
#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub fn set_dont_fragment(_sock: &UdpSocket) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "do-not-fragment is not available on this OS; path-MTU discovery would \
         over-report the path (jumbo spec §4.3)",
    ))
}

/// Read back `IP_MTU_DISCOVER` / `IPV6_MTU_DISCOVER` (tests, diagnostics).
#[cfg(any(target_os = "linux", target_os = "android"))]
pub fn mtu_discover(sock: &UdpSocket) -> io::Result<libc::c_int> {
    let fd = sock.as_raw_fd();
    let (level, name) = if sock.local_addr()?.is_ipv6() {
        (libc::IPPROTO_IPV6, libc::IPV6_MTU_DISCOVER)
    } else {
        (libc::IPPROTO_IP, libc::IP_MTU_DISCOVER)
    };
    let mut v: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    // SAFETY: `fd` is borrowed from `sock: &UdpSocket` and is open for the
    // whole call — the socket cannot be dropped while that reference lives, and
    // nothing here closes it. `v` and `len` are live locals, and `len` holds
    // exactly `size_of::<c_int>()`, which is the width `getsockopt` writes for
    // these two option names.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            level,
            name,
            &mut v as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(v)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn setsockopt(
    fd: i32,
    level: libc::c_int,
    name: libc::c_int,
    value: libc::c_int,
) -> io::Result<()> {
    // SAFETY: `fd` comes from the caller's `&UdpSocket` (the only caller is
    // `set_dont_fragment`, which holds that borrow across this call), so it is a
    // valid, open descriptor and is not closed here. `value` is a live local
    // that outlives the call, and the length passed is exactly its own type's
    // size.
    let rc = unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            &value as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
mod tests {
    use super::*;

    #[test]
    fn df_is_set_on_a_v4_socket() {
        let s = UdpSocket::bind("127.0.0.1:0").unwrap();
        set_dont_fragment(&s).unwrap();
        assert_eq!(mtu_discover(&s).unwrap(), libc::IP_PMTUDISC_DO);
    }

    #[test]
    fn df_is_set_on_a_v6_socket() {
        let Ok(s) = UdpSocket::bind("[::1]:0") else {
            return; // no IPv6 loopback on this box: nothing to assert
        };
        set_dont_fragment(&s).unwrap();
        assert_eq!(mtu_discover(&s).unwrap(), libc::IPV6_PMTUDISC_DO);
    }

    // DF's effect on an oversize send cannot be shown on loopback (MTU 65536:
    // a datagram over it is refused with EMSGSIZE with or without DF). The
    // behaviour is proven on the fleet's 1500 B arm (spec §10 row b); these
    // tests pin only that the option is set.
}

/// The other arm's one property: a refusal, not a silent success. NOTE: this
/// module compiles only on a non-Linux target, and the repo has no such
/// toolchain — it is written but never built here.
#[cfg(all(test, not(any(target_os = "linux", target_os = "android"))))]
mod tests {
    use super::*;

    #[test]
    fn df_is_refused_by_name_off_linux() {
        let s = UdpSocket::bind("127.0.0.1:0").unwrap();
        let e = set_dont_fragment(&s).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::Unsupported);
    }
}
