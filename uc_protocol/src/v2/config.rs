// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Peter Knego

//! Cluster-config wire codec (M7, spec 2026-07-13): the payload of a
//! `FRAME_TYPE_CONFIG` frame, the snapshot-session config carry, and the
//! durable record's canonical byte form. Core-only: addresses are
//! `(ipv4 u32, port u16)` — `SocketAddr` conversion happens in `uc2_node`.
//!
//! Layout (all LE):
//!   version u64 | prev_position u64 | n_voters u16 | n_learners u16 |
//!   n_tombstones u16 | voters[n]{id u32, ip u32, port u16} |
//!   learners[n]{..} | tombstones[n]{u32}

/// Hard cap: voters + learners (incl. transitional states) — the cnc
/// PeerSlots band has 8 slots. Enforced at proposal AND at decode.
pub const MAX_MEMBERS: usize = 8;

pub const CONFIG_FIXED_LEN: usize = 22;
pub const MEMBER_LEN: usize = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireMember {
    pub id: u32,
    pub ip: u32,   // IPv4, network-order value stored as a plain u32
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireConfig {
    pub version: u64,
    /// Frame-end position of the PREDECESSOR config entry (0 for genesis) —
    /// audit trail; the durable record keeps the authoritative prev.
    pub prev_position: u64,
    pub voters: Vec<WireMember>,
    pub learners: Vec<WireMember>,
    pub tombstones: Vec<u32>,
}

pub fn encode_config(c: &WireConfig, out: &mut Vec<u8>) {
    out.extend_from_slice(&c.version.to_le_bytes());
    out.extend_from_slice(&c.prev_position.to_le_bytes());
    out.extend_from_slice(&(c.voters.len() as u16).to_le_bytes());
    out.extend_from_slice(&(c.learners.len() as u16).to_le_bytes());
    out.extend_from_slice(&(c.tombstones.len() as u16).to_le_bytes());
    for m in c.voters.iter().chain(c.learners.iter()) {
        out.extend_from_slice(&m.id.to_le_bytes());
        out.extend_from_slice(&m.ip.to_le_bytes());
        out.extend_from_slice(&m.port.to_le_bytes());
    }
    for t in &c.tombstones {
        out.extend_from_slice(&t.to_le_bytes());
    }
}

pub fn decode_config(buf: &[u8]) -> Option<WireConfig> {
    if buf.len() < CONFIG_FIXED_LEN {
        return None;
    }
    let version = u64::from_le_bytes(buf[0..8].try_into().ok()?);
    let prev_position = u64::from_le_bytes(buf[8..16].try_into().ok()?);
    let nv = u16::from_le_bytes(buf[16..18].try_into().ok()?) as usize;
    let nl = u16::from_le_bytes(buf[18..20].try_into().ok()?) as usize;
    let nt = u16::from_le_bytes(buf[20..22].try_into().ok()?) as usize;
    if nv + nl > MAX_MEMBERS {
        return None; // structural invalid — refuse at the wire, too
    }
    let need = CONFIG_FIXED_LEN + (nv + nl) * MEMBER_LEN + nt * 4;
    if buf.len() != need {
        return None;
    }
    let mut off = CONFIG_FIXED_LEN;
    let member = |off: &mut usize| -> WireMember {
        let id = u32::from_le_bytes(buf[*off..*off + 4].try_into().unwrap());
        let ip = u32::from_le_bytes(buf[*off + 4..*off + 8].try_into().unwrap());
        let port = u16::from_le_bytes(buf[*off + 8..*off + 10].try_into().unwrap());
        *off += MEMBER_LEN;
        WireMember { id, ip, port }
    };
    let voters = (0..nv).map(|_| member(&mut off)).collect();
    let learners = (0..nl).map(|_| member(&mut off)).collect();
    let mut tombstones = Vec::with_capacity(nt);
    for _ in 0..nt {
        tombstones.push(u32::from_le_bytes(buf[off..off + 4].try_into().ok()?));
        off += 4;
    }
    Some(WireConfig { version, prev_position, voters, learners, tombstones })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> WireConfig {
        WireConfig {
            version: 3,
            prev_position: 4096,
            voters: vec![
                WireMember { id: 1, ip: u32::from_be_bytes([10, 0, 0, 1]), port: 19100 },
                WireMember { id: 2, ip: u32::from_be_bytes([10, 0, 0, 2]), port: 19100 },
                WireMember { id: 3, ip: u32::from_be_bytes([10, 0, 0, 3]), port: 19100 },
            ],
            learners: vec![WireMember { id: 5, ip: u32::from_be_bytes([10, 0, 0, 5]), port: 19100 }],
            tombstones: vec![4],
        }
    }

    #[test]
    fn config_roundtrips() {
        let c = sample();
        let mut buf = Vec::new();
        encode_config(&c, &mut buf);
        assert_eq!(decode_config(&buf), Some(c));
    }

    #[test]
    fn config_wire_layout_is_pinned_le() {
        // version=3, prev_position=4096, counts (voters=3, learners=1, tombs=1)
        let c = sample();
        let mut buf = Vec::new();
        encode_config(&c, &mut buf);
        assert_eq!(&buf[0..8], &3u64.to_le_bytes());          // version
        assert_eq!(&buf[8..16], &4096u64.to_le_bytes());      // prev_position
        assert_eq!(&buf[16..18], &3u16.to_le_bytes());        // n_voters
        assert_eq!(&buf[18..20], &1u16.to_le_bytes());        // n_learners
        assert_eq!(&buf[20..22], &1u16.to_le_bytes());        // n_tombstones
        // first voter entry: id u32 | ip u32 | port u16 = 10 bytes
        assert_eq!(&buf[22..26], &1u32.to_le_bytes());
        assert_eq!(&buf[26..30], &u32::from_be_bytes([10, 0, 0, 1]).to_le_bytes());
        assert_eq!(&buf[30..32], &19100u16.to_le_bytes());
        assert_eq!(buf.len(), 22 + 4 * 10 + 4); // header + 4 members + 1 tombstone u32
    }

    #[test]
    fn decode_rejects_truncated_and_oversized() {
        let mut buf = Vec::new();
        encode_config(&sample(), &mut buf);
        assert_eq!(decode_config(&buf[..buf.len() - 1]), None, "truncated");
        let mut big = sample();
        big.voters = (0..9)
            .map(|i| WireMember { id: i, ip: 0, port: 0 })
            .collect();
        let mut b2 = Vec::new();
        encode_config(&big, &mut b2);
        assert_eq!(decode_config(&b2), None, "over MAX_MEMBERS refused at decode too");
    }
}
