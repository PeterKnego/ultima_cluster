// SPDX-License-Identifier: Apache-2.0
// Impairs IPv4 UDP packets by rule: a peer address, a port matched as source or destination, an Aeron frame type, and
// for DATA an SBE schema and template; each may be any. A rule drops a percentage of what it matches, duplicates a
// percentage of the rest and delays it, counting each. One program per direction, sharing the rules: delay needs the
// fq qdisc on the way out, so it applies to egress only.

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
#define RETRANSMIT 0xfffe // a pseudo type: DATA starting below what its flow already sent
#define AERON_DATA 1
#define AERON_BEGIN_FLAG 0x80
#define CLONE_MARK 0xfa017e40 // a duplicate this program made, passed through as it re-enters

// Aeron's DATA header and the SBE message header after it; any other frame shares the first eight bytes.
struct aeron_frame
{
    __s32 frame_length;
    __u8 version;
    __u8 flags;
    __u16 type;
    __s32 term_offset;
    __s32 session_id;
    __s32 stream_id;
    __s32 term_id;
    __s64 reserved_value;
    __u16 block_length;
    __u16 template_id;
    __u16 schema_id;
    __u16 schema_version;
};

struct rule_key
{
    __u16 port;     // network byte order, or ANY_PORT
    __u16 type;     // Aeron frame type, RETRANSMIT or ANY
    __u16 template; // SBE template, for DATA, or ANY
    __u16 schema;   // SBE schema, for DATA, or ANY
    __u32 peer;     // the far end's IPv4 address, network byte order, or ANY_PEER
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

// A publication sends a copy to each destination, so a flow is a publication and where its copy goes.
struct flow_key
{
    __s32 session_id;
    __s32 stream_id;
    __u32 daddr;
    __u16 dport;
    __u16 pad;
};

struct flow
{
    __s32 term_id;
    __s32 end; // term offset after the furthest DATA sent in term_id
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

static __always_inline int is_retransmit(struct iphdr* ip, struct udphdr* udp, struct aeron_frame* frame)
{
    struct flow_key key = {
        .session_id = frame->session_id, .stream_id = frame->stream_id, .daddr = ip->daddr, .dport = udp->dest};
    // A datagram holds whole aligned frames from one stretch of the term, so it ends where its payload does.
    struct flow next = {.term_id = frame->term_id,
                        .end = frame->term_offset + bpf_ntohs(udp->len) - (int)sizeof(struct udphdr)};
    struct flow* seen = bpf_map_lookup_elem(&flows, &key);
    if (seen)
    {
        __s32 terms_ahead = next.term_id - seen->term_id;
        if (terms_ahead < 0 || (terms_ahead == 0 && frame->term_offset < seen->end))
        {
            return 1;
        }
        if (terms_ahead == 0 && next.end <= seen->end)
        {
            return 0;
        }
    }
    bpf_map_update_elem(&flows, &key, &next, BPF_ANY);
    return 0;
}

// What a packet is, for every rule lookup it takes.
struct selector
{
    __u16 type;
    __u16 schema;
    __u16 template;
    int retransmit;
};

static __always_inline struct rule* lookup(__u32 peer, __u16 port, __u16 type, __u16 schema, __u16 template)
{
    struct rule_key key = {.port = port, .type = type, .template = template, .schema = schema, .peer = peer};
    return bpf_map_lookup_elem(&rules, &key);
}

// The most specific selector first: retransmit, then type, schema and template, type and schema, type, any.
static __always_inline struct rule* find(__u32 peer, __u16 port, struct selector* what)
{
    struct rule* rule = 0;
    if (what->retransmit)
    {
        rule = lookup(peer, port, RETRANSMIT, ANY, ANY);
    }
    if (!rule && what->template != ANY)
    {
        rule = lookup(peer, port, what->type, what->schema, what->template);
    }
    if (!rule && what->schema != ANY)
    {
        rule = lookup(peer, port, what->type, what->schema, ANY);
    }
    if (!rule && what->type != ANY)
    {
        rule = lookup(peer, port, what->type, ANY, ANY);
    }
    return rule ? rule : lookup(peer, port, ANY, ANY, ANY);
}

// The most specific address first: the peer, then any; within each, destination port, source port, any.
static __always_inline struct rule* find_any(__u32 peer, __u16 dport, __u16 sport, struct selector* what)
{
    struct rule* rule = find(peer, dport, what);
    if (!rule)
    {
        rule = find(peer, sport, what);
    }
    return rule ? rule : find(peer, ANY_PORT, what);
}

static __always_inline struct rule* match(struct __sk_buff* skb, int egress)
{
    // A header in a paged fragment would otherwise read as ANY and slip past a typed rule.
    bpf_skb_pull_data(skb, sizeof(struct ethhdr) + sizeof(struct iphdr) + sizeof(struct udphdr) +
                               sizeof(struct aeron_frame));

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

    // The first frame stands for the datagram: Aeron batches only DATA and PAD frames into one.
    struct selector what = {.type = ANY, .schema = ANY, .template = ANY, .retransmit = 0};
    struct aeron_frame* frame = (void*)(udp + 1);
    if ((void*)&frame->term_offset <= data_end)
    {
        what.type = frame->type;
    }
    if (what.type == AERON_DATA && (void*)(frame + 1) <= data_end && frame->frame_length > 0)
    {
        what.retransmit = is_retransmit(ip, udp, frame);
        if (frame->flags & AERON_BEGIN_FLAG)
        {
            what.schema = frame->schema_id;
            what.template = frame->template_id;
        }
    }

    __u32 peer = egress ? ip->daddr : ip->saddr;
    struct rule* rule = find_any(peer, udp->dest, udp->source, &what);
    return rule ? rule : find_any(ANY_PEER, udp->dest, udp->source, &what);
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
int faulteron_in(struct __sk_buff* skb)
{
    return impair(skb, 0);
}

SEC("tc")
int faulteron_out(struct __sk_buff* skb)
{
    return impair(skb, 1);
}

// Not GPL-compatible, so no GPL-only helper is open to this program; none of those it calls is one.
char LICENSE[] SEC("license") = "Apache-2.0";
