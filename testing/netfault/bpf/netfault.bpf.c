// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Fredrik Dahlberg (faulteron, the Aeron original); Copyright 2026 Peter Knego (the UC variant)
//
// Impairs IPv4 UDP packets by rule: a peer address, a port matched as source or destination, a UC datagram kind, and
// a direction (out of the interface, into it, or either); each may be any. A rule drops a percentage of what it matches, duplicates a percentage of the rest and delays it,
// counting each. One program per direction, sharing the rules: delay needs the fq qdisc on the way out, so it applies
// to egress only.
//
// The kind is read from UC's 16-byte datagram header (uc_protocol::v2::datagram, spec §5), which every node↔node
// datagram starts with and which stays cleartext under wire crypto (it is the AEAD's associated data). The layout is
// pinned by the _Static_asserts below and by testing/uc_crashtest/tests/netfault_pins.rs against the Rust constants:
// a wire change that moves a field must change both, in the same PR.

#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/in.h>
#include <linux/ip.h>
#include <linux/pkt_cls.h>
#include <linux/udp.h>

#include <bpf/bpf_endian.h>
#include <bpf/bpf_helpers.h>

#define ANY 0xffff
#define ANY_PORT 0
#define ANY_PEER 0
#define RETRANSMIT 0xfffe // a pseudo kind: DATA starting below what its flow already sent
#define DIR_ANY 0
#define DIR_OUT 1 // egress: leaving the interface
#define DIR_IN 2  // ingress: arriving at it
#define CLONE_MARK 0xfa017e40 // a duplicate this program made, passed through as it re-enters

// uc_protocol::v2::datagram — the numbers a rule is keyed on. Pinned against the Rust constants by netfault_pins.rs.
#define UC_HEADER_LEN 16
#define UC_KIND_DATA 1
// uc_protocol::v2::crypto: a sealed payload carries an 8-byte counter and a 16-byte tag after the header, and
// key_epoch 0 is reserved as the cleartext sentinel, so a non-zero epoch says the payload is sealed.
#define UC_CRYPTO_OVERHEAD 24

// The datagram header, little-endian on the wire. Only DATA has a self-locating position and a payload of log bytes;
// every other kind is demuxed by kind alone here.
struct uc_header
{
    __u64 position;
    __u32 leadership_term_id;
    __u8 kind;
    __u8 wire;
    __u16 key_epoch;
};
_Static_assert(sizeof(struct uc_header) == UC_HEADER_LEN, "uc_header is 16 bytes");
_Static_assert(__builtin_offsetof(struct uc_header, kind) == 12, "kind is at offset 12");
_Static_assert(__builtin_offsetof(struct uc_header, key_epoch) == 14, "key_epoch is at offset 14");

struct rule_key
{
    __u16 port;    // network byte order, or ANY_PORT
    __u16 kind;    // a UC datagram kind, RETRANSMIT or ANY
    __u32 peer;    // the far end's IPv4 address, network byte order, or ANY_PEER
    __u8 dir;      // DIR_OUT, DIR_IN or DIR_ANY
    __u8 pad[3];
};

struct rule
{
    __u32 loss;      // percent dropped
    __u32 duplicate; // percent of what is not dropped sent twice
    __u32 delay_us;  // egress only: held delay_us plus up to jitter_us
    __u32 jitter_us;
    __u64 dropped;
    __u64 duplicated;
    __u64 delayed;
};

// A flow is one sender's DATA to one destination: the stream of log bytes a leader replicates to one follower.
struct flow_key
{
    __u32 saddr;
    __u32 daddr;
    __u16 sport;
    __u16 dport;
};

struct flow
{
    __u32 term;   // the leadership term the frontier was seen in
    __u32 pad;
    __u64 end;    // the position after the furthest DATA sent in that term
};

struct
{
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 256);
    __type(key, struct rule_key);
    __type(value, struct rule);
} rules SEC(".maps");

struct
{
    __uint(type, BPF_MAP_TYPE_LRU_HASH);
    __uint(max_entries, 4096);
    __type(key, struct flow_key);
    __type(value, struct flow);
} flows SEC(".maps");

// DATA is self-locating: position is the absolute stream position of its first payload byte, and the payload is a run
// of complete frames, so the datagram ends at position plus its payload length. A retransmission (a NAK repair, or
// the tail re-sent on a heartbeat's loss detection) starts below the furthest end its flow has reached in the same
// term. A later term starts a new frontier: a new leader may truncate, so positions can legitimately fall.
static __always_inline int is_retransmit(struct iphdr* ip, struct udphdr* udp, struct uc_header* hdr)
{
    struct flow_key key = {.saddr = ip->saddr, .daddr = ip->daddr, .sport = udp->source, .dport = udp->dest};
    __u64 payload = bpf_ntohs(udp->len) - sizeof(struct udphdr) - UC_HEADER_LEN;
    if (hdr->key_epoch != 0 && payload >= UC_CRYPTO_OVERHEAD)
    {
        payload -= UC_CRYPTO_OVERHEAD;
    }
    struct flow next = {.term = hdr->leadership_term_id, .pad = 0, .end = hdr->position + payload};
    struct flow* seen = bpf_map_lookup_elem(&flows, &key);
    if (seen)
    {
        if (hdr->leadership_term_id < seen->term)
        {
            return 0; // a stale term's DATA is refused by the receiver; it is not a repair
        }
        if (hdr->leadership_term_id == seen->term)
        {
            if (hdr->position < seen->end)
            {
                return 1;
            }
            if (next.end <= seen->end)
            {
                return 0;
            }
        }
    }
    bpf_map_update_elem(&flows, &key, &next, BPF_ANY);
    return 0;
}

// What a packet is, for every rule lookup it takes.
struct selector
{
    __u16 kind;
    int retransmit;
};

static __always_inline struct rule* lookup(__u32 peer, __u16 port, __u8 dir, __u16 kind)
{
    struct rule_key key = {.port = port, .kind = kind, .peer = peer, .dir = dir, .pad = {0, 0, 0}};
    return bpf_map_lookup_elem(&rules, &key);
}

// The most specific selector first: retransmit, then the kind, then any; within each the packet's own direction
// before either.
static __always_inline struct rule* find(__u32 peer, __u16 port, __u8 dir, struct selector* what)
{
    struct rule* rule = 0;
    if (what->retransmit)
    {
        rule = lookup(peer, port, dir, RETRANSMIT);
        if (!rule)
        {
            rule = lookup(peer, port, DIR_ANY, RETRANSMIT);
        }
    }
    if (!rule && what->kind != ANY)
    {
        rule = lookup(peer, port, dir, what->kind);
        if (!rule)
        {
            rule = lookup(peer, port, DIR_ANY, what->kind);
        }
    }
    if (!rule)
    {
        rule = lookup(peer, port, dir, ANY);
    }
    return rule ? rule : lookup(peer, port, DIR_ANY, ANY);
}

// The most specific address first: the peer, then any; within each, destination port, source port, any.
static __always_inline struct rule* find_any(__u32 peer, __u16 dport, __u16 sport, __u8 dir, struct selector* what)
{
    struct rule* rule = find(peer, dport, dir, what);
    if (!rule)
    {
        rule = find(peer, sport, dir, what);
    }
    return rule ? rule : find(peer, ANY_PORT, dir, what);
}

static __always_inline struct rule* match(struct __sk_buff* skb, int egress)
{
    // A header in a paged fragment would otherwise read as ANY and slip past a kind rule.
    bpf_skb_pull_data(skb, sizeof(struct ethhdr) + sizeof(struct iphdr) + sizeof(struct udphdr) + UC_HEADER_LEN);

    void* data = (void*)(long)skb->data;
    void* data_end = (void*)(long)skb->data_end;

    struct ethhdr* eth = data;
    if ((void*)(eth + 1) > data_end || eth->h_proto != bpf_htons(ETH_P_IP))
    {
        return 0;
    }
    struct iphdr* ip = (void*)(eth + 1);
    if ((void*)(ip + 1) > data_end || ip->protocol != IPPROTO_UDP)
    {
        return 0;
    }
    struct udphdr* udp = (void*)ip + ip->ihl * 4;
    if ((void*)(udp + 1) > data_end)
    {
        return 0;
    }

    // A datagram too short for the header is not UC's: it matches only `any` rules.
    struct selector what = {.kind = ANY, .retransmit = 0};
    struct uc_header* hdr = (void*)(udp + 1);
    if ((void*)(hdr + 1) <= data_end)
    {
        what.kind = hdr->kind;
        if (hdr->kind == UC_KIND_DATA)
        {
            what.retransmit = is_retransmit(ip, udp, hdr);
        }
    }

    __u32 peer = egress ? ip->daddr : ip->saddr;
    __u8 dir = egress ? DIR_OUT : DIR_IN;
    struct rule* rule = find_any(peer, udp->dest, udp->source, dir, &what);
    return rule ? rule : find_any(ANY_PEER, udp->dest, udp->source, dir, &what);
}

static __always_inline int impair(struct __sk_buff* skb, int egress)
{
    if (skb->mark == CLONE_MARK)
    {
        skb->mark = 0;
        return TC_ACT_OK;
    }
    struct rule* rule = match(skb, egress);
    if (!rule)
    {
        return TC_ACT_OK;
    }
    if (bpf_get_prandom_u32() % 100 < rule->loss)
    {
        __sync_fetch_and_add(&rule->dropped, 1);
        return TC_ACT_SHOT;
    }
    if (bpf_get_prandom_u32() % 100 < rule->duplicate)
    {
        __u32 mark = skb->mark;
        skb->mark = CLONE_MARK;
        if (bpf_clone_redirect(skb, skb->ifindex, egress ? 0 : BPF_F_INGRESS) == 0)
        {
            __sync_fetch_and_add(&rule->duplicated, 1);
        }
        skb->mark = mark;
    }
    if (egress && (rule->delay_us || rule->jitter_us))
    {
        __u64 delay_ns = (rule->delay_us + bpf_get_prandom_u32() % (rule->jitter_us + 1)) * 1000ULL;
        bpf_skb_set_tstamp(skb, bpf_ktime_get_ns() + delay_ns, BPF_SKB_TSTAMP_DELIVERY_MONO);
        __sync_fetch_and_add(&rule->delayed, 1);
    }
    return TC_ACT_OK;
}

SEC("tc")
int netfault_in(struct __sk_buff* skb)
{
    return impair(skb, 0);
}

SEC("tc")
int netfault_out(struct __sk_buff* skb)
{
    return impair(skb, 1);
}

// Not GPL-compatible, so no GPL-only helper is open to this program; none of those it calls is one.
char LICENSE[] SEC("license") = "Apache-2.0";
