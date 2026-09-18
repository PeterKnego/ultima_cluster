# What a remote client's *shape* costs — and what it does not (2026-09-18)

**Measurement, no bar.** This page answers one question raised by the dogfood
(map #16): the KV store measured **13,557 ops/s** through a real cluster
(gate row B3-v1), against a platform that commits ~1.4 M ops/s cluster-wide.
Was the state machine the bottleneck?

**It was not.** The 13.5 k was an artifact of how the client was *driven*. The
same KV store, same class of hardware, same three-node cross-host cluster,
driven with a window instead of one-request-at-a-time, does **142,657
writes/s** — 10.5× — with the state machine reporting `lag=0` throughout.

Issue [#43](https://github.com/PeterKnego/ultima_cluster/issues/43). Fleet:
4 × `c6id`-class `c6i.2xlarge`, us-east-1, one placement group, destroyed and
leak-checked after the run. Every point 10 s, payload 64 B, `lost=0`.

## 1. The client half alone (hop 3, no FSM, no node, no cluster)

Driver on `hosts[3]` → `dummy-edge` on `hosts[0]`: a TCP server that answers
every SUBMIT immediately out of nothing. Everything behind the client is
removed on purpose, so a difference between drivers is the *client's* cost.

| driver | shape | in flight | rate | p50 | p99 |
|---|---|---|---|---|---|
| `blaster` | raw protocol, no client library | 1024 | 6,162,562/s | 0.154 ms | 0.239 ms |
| `remote-load` | **`RemoteEngine` halves** | 1024 | 1,653,039/s | 0.729 ms | 0.898 ms |
| `remote-client-load` | **`RemoteClient`, window** | 1024 | **1,756,310/s** | 0.577 ms | 0.818 ms |
| `remote-client-load` | `RemoteClient`, per-call ×1 | 1 | **11,357/s** | 0.087 ms | 0.109 ms |
| `remote-client-load` | per-call ×4 | 4 | 40,598/s | 0.098 ms | 0.141 ms |
| `remote-client-load` | per-call ×16 | 16 | 125,271/s | 0.127 ms | 0.194 ms |
| `remote-client-load` | per-call ×64 | 64 | 187,326/s | 0.338 ms | 0.419 ms |

### The two findings

**(a) The blocking wrapper is not the cross-host cost.** At matched depth on
one connection, `RemoteClient` with a window (1.76 M/s) **matched and slightly
beat** the lock-free halves (1.65 M/s). The wrapper's per-request `Mutex`,
`Arc` allocation and condvar wakeup are a few microseconds; a real network
round trip is ~90 µs, and swamps them. On loopback, where there is no latency
to hide behind, the same comparison measured the halves **1.92× faster**
(dev-box smoke: 4.45 M/s vs 2.32 M/s at depth 256) — so the loopback number
*overstates* the wrapper's share for any real deployment.

**(b) The per-call shape is the whole story.** One request in flight yields
11,357/s at p50 0.087 ms — which is exactly Little's law, `1 ÷ 87 µs ≈
11,500`. Throughput is concurrency ÷ latency, and per-call pins concurrency at
one per thread. **The dogfood's 13,557 ops/s sits on this line**, not on any
property of the KV state machine.

Note the latency columns do not move the way intuition suggests: per-call has
the *best* p50 (0.087 ms) and the worst throughput. A windowed request queues
behind the ones the caller itself sent, which is real latency for that request
and the honest price of the throughput.

## 2. The real KV store, real cluster

Three voters (`uc2-node` + `kv-service` v2 + `uc2-gateway`) across
`hosts[0..2]`, driver on `hosts[3]`, through the gateways over `uc_remote` —
the whole stack, nothing dummied. `kv-load` on the engine halves:

| driver shape | connections | writes/s |
|---|---|---|
| dogfood B3-v1 (blocking client, 4 workers × 64) | 1 | 13,557 |
| `kv-load`, window 64 | 1 | 11,414 |
| `kv-load`, window 512 | 1 | 46,245 |
| **`kv-load`, window 512, four processes** | 4 | **142,657** |

The leader reported `applied=255,952,352 lag=0` throughout: the state machine
never fell behind the log. Throughput scaled ~3.1× from one connection to
four, so **142 k is not the FSM's ceiling either** — it is where the client
host (8 vCPU) and the per-connection credit grant (`max_credits_seen: 256`)
bind. The FSM's own apply hop runs in the millions of frames/s
(`uc2-m14a-apply-hop-2026-08-27.md`).

## 3. What this is not

- **Not a bar.** Rate bars are fleet-only *and* pre-committed; this page is a
  measurement, and nothing here gates anything.
- **The multi-connection rows are client-host-bound.** Arm C reads 5.00 M/s at
  two connections but 4.28 M/s at four — non-monotonic, because the 8-vCPU
  client host saturates. The clean comparison is the one-connection row; the
  ladders are shape, not ceiling.
- **`dummy-edge` is not a cluster.** §1 removes the FSM, the node and
  consensus deliberately. Its numbers must never be quoted as UC throughput.
- **§1 and §2 are not comparable to each other.** One is a client against an
  instant-answer sink; the other is the whole replicated stack.

## 4. What changed because of it

- The on-ramp now steers the choice: a "Which client do I want?" table in
  [Run a gateway](../how-to/run-a-gateway.md#which-client-do-i-want), a
  pointer from the [remote protocol reference](../reference/remote-protocol.md),
  and a `RemoteClient` rustdoc that opens with *the convenient client, not the
  fast one*.
- `examples/kv`'s `kv-load` drives the engine halves, so the code developers
  copy for throughput demonstrates the fast pattern; the `kv` CLI stays on
  `RemoteClient`, because one request per invocation is exactly its job.
- **#43's step 3 (rebuilding `RemoteClient` lock-free) is closed as not worth
  doing.** Finding (a) says the wrapper is not the cross-host cost. The
  remaining open question is whether to offer an *async* client, which
  pipelines naturally and would make the per-call trap unreachable — a
  product decision about the target developer, not a performance fix.

## Reproducing it

```bash
# §1, against the dummy sink (needs a 4-host fleet; see bench-infra)
python3 bench-infra/scripts/m13_hop_bench.py --fleet --arms 3 \
    --secs 10 --payload 64 --conn-inflight 1024 --inflight 4096 --conns 1,2,4

# the two client shapes, directly
hop_bench remote-load          --gateways HOST:9311 --inflight 1024 --conns 1
hop_bench remote-client-load   --gateways HOST:9311 --inflight 1024 --conns 1 --mode window
hop_bench remote-client-load   --gateways HOST:9311 --mode per-call --waiters 1

# §2, against a real KV cluster
kv-load --gateways G0:9200,G1:9200,G2:9200 --keys 400000 --value-bytes 64 --window 512
```
