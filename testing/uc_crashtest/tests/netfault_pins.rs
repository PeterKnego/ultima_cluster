//! Pins `testing/netfault` — the eBPF network fault injector — to the wire it
//! parses. The injector is C and bash, outside the cargo workspace, so nothing
//! else would notice when a kind is added to `uc_protocol::v2::datagram` or
//! the header is relaid: a `--kind nak` rule would silently match nothing,
//! which is the worst failure a fault injector can have. This test reads the
//! two sources as text and asserts their numbers against the Rust constants,
//! so the PR that changes the wire must change the injector too.
//!
//! What is pinned: the header length and the two field offsets the program
//! reads (`kind` at 12, `key_epoch` at 14), the crypto overhead it subtracts
//! from a sealed payload, the DATA kind it tracks retransmissions for, and the
//! CLI's whole name↔number table, both ways: every live kind has a name, and
//! every name maps to the constant it claims.

use std::fs;
use std::path::PathBuf;

use uc_protocol::v2::crypto::{
    CRYPTO_OVERHEAD, DGRAM_KIND_HS_INIT, DGRAM_KIND_HS_KEY, DGRAM_KIND_HS_RESP,
};
use uc_protocol::v2::datagram::*;

fn netfault_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../netfault")
}

fn read(rel: &str) -> String {
    let path = netfault_dir().join(rel);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// The value of a `#define NAME value` line in the BPF source.
fn define(src: &str, name: &str) -> u64 {
    let prefix = format!("#define {name} ");
    let line = src
        .lines()
        .find(|l| l.starts_with(&prefix))
        .unwrap_or_else(|| panic!("no `{prefix}` in netfault.bpf.c"));
    line[prefix.len()..]
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap_or_else(|e| panic!("{name}: {e}"))
}

/// The words of a `NAME=( ... )` bash array, which may span lines.
fn bash_array(src: &str, name: &str) -> Vec<String> {
    let start = src
        .find(&format!("\n{name}=("))
        .unwrap_or_else(|| panic!("no `{name}=(` in bin/netfault"))
        + name.len()
        + 3;
    let end = start + src[start..].find(')').unwrap();
    src[start..end]
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

/// Every datagram kind a level-12 binary can put on the wire, with the name
/// the CLI must know it by. Retired numbers (21) are deliberately absent: a
/// rule on a kind nothing sends is a rule that matches nothing.
fn live_kinds() -> Vec<(&'static str, u8)> {
    vec![
        ("data", DGRAM_KIND_DATA),
        ("heartbeat", DGRAM_KIND_HEARTBEAT),
        ("nak", DGRAM_KIND_NAK),
        ("status", DGRAM_KIND_STATUS),
        ("append-position", DGRAM_KIND_APPEND_POSITION),
        ("commit-position", DGRAM_KIND_COMMIT_POSITION),
        ("request-vote", DGRAM_KIND_REQUEST_VOTE),
        ("vote", DGRAM_KIND_VOTE),
        ("term-map", DGRAM_KIND_TERM_MAP),
        ("read-probe", DGRAM_KIND_READ_PROBE),
        ("read-probe-ack", DGRAM_KIND_READ_PROBE_ACK),
        ("snap-begin", DGRAM_KIND_SNAP_BEGIN),
        ("snap-chunk", DGRAM_KIND_SNAP_CHUNK),
        ("snap-nak", DGRAM_KIND_SNAP_NAK),
        ("snap-done", DGRAM_KIND_SNAP_DONE),
        ("config-proposal", DGRAM_KIND_CONFIG_PROPOSAL),
        ("config-reply", DGRAM_KIND_CONFIG_REPLY),
        ("hs-init", DGRAM_KIND_HS_INIT),
        ("hs-resp", DGRAM_KIND_HS_RESP),
        ("hs-key", DGRAM_KIND_HS_KEY),
        ("snap-request", DGRAM_KIND_SNAP_REQUEST),
        ("snap-redirect", DGRAM_KIND_SNAP_REDIRECT),
        ("probe", DGRAM_KIND_PROBE),
        ("probe-ack", DGRAM_KIND_PROBE_ACK),
        ("snap-report", DGRAM_KIND_SNAP_REPORT),
        ("backup-report", DGRAM_KIND_BACKUP_REPORT),
        ("tier-install", DGRAM_KIND_TIER_INSTALL),
        ("tier-install-status", DGRAM_KIND_TIER_INSTALL_STATUS),
    ]
}

#[test]
fn the_bpf_program_reads_the_header_the_wire_defines() {
    let src = read("bpf/netfault.bpf.c");
    assert_eq!(define(&src, "UC_HEADER_LEN"), DATAGRAM_HEADER_LEN as u64);
    assert_eq!(define(&src, "UC_KIND_DATA"), u64::from(DGRAM_KIND_DATA));
    assert_eq!(define(&src, "UC_CRYPTO_OVERHEAD"), CRYPTO_OVERHEAD as u64);
    // The struct's own layout is asserted at compile time in C; what is pinned
    // here is that those asserts name the offsets the Rust encoder writes.
    let header = DatagramHeader {
        position: 0,
        leadership_term_id: 0,
        kind: 0xAB,
        wire: 0,
        key_epoch: 0xCDEF,
    };
    let mut buf = [0u8; DATAGRAM_HEADER_LEN];
    write_datagram_header(&mut buf, &header);
    let kind_at = buf.iter().position(|&b| b == 0xAB).unwrap();
    let epoch_at = buf
        .windows(2)
        .position(|w| w == 0xCDEF_u16.to_le_bytes())
        .unwrap();
    for (field, offset) in [("kind", kind_at), ("key_epoch", epoch_at)] {
        let assert = format!("__builtin_offsetof(struct uc_header, {field}) == {offset}");
        assert!(
            src.contains(&assert),
            "netfault.bpf.c must assert `{assert}`"
        );
    }
}

#[test]
fn the_cli_names_every_live_kind_and_only_those() {
    let cli = read("bin/netfault");
    let names = bash_array(&cli, "KIND_NAMES");
    let ids: Vec<u8> = bash_array(&cli, "KIND_IDS")
        .iter()
        .map(|s| s.parse().unwrap())
        .collect();
    assert_eq!(
        names.len(),
        ids.len(),
        "KIND_NAMES and KIND_IDS are parallel arrays"
    );
    let table: Vec<(&str, u8)> = names.iter().map(String::as_str).zip(ids).collect();
    assert_eq!(
        table,
        live_kinds(),
        "bin/netfault's kind table must match uc_protocol"
    );
}
