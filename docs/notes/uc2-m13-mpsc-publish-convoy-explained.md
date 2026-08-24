# The MPSC ingress ring convoy, explained — why eight gateway connections collapsed a cluster that four could not

*Written 2026-08-24 from the M13 hop-isolation bench
(`docs/benchmarks/uc2-m13-hop-bench-2026-08-24.md`). Status: root cause
confirmed on the fleet and reproduced on a laptop with no gateway involved;
a scheduling mitigation is on branch `uc2/m13-hop-bench`; the structural
fix is M13 work.*

## The symptom, as M12 saw it

Four remote clients through one gateway edge aggregated to ~450 k
responses/s. Eight clients dropped to ~11 k/s with second-scale latencies,
the edge process burning seven of the host's eight cores while the client
host sat idle. M12 attributed it to the edge granting each connection a full
credit window with no global budget tied to the node's admission window —
a real gap in the flow-control design, and a plausible story: eight windows
of 1024 exceed the node's ~2–4k-frame admission window, four do not.

## Why that story could not be right

The hop bench replaced the pieces one at a time. With the real node swapped
for a dummy that pops every record and answers instantly — no admission
window, no consensus, nothing to refuse — eight connections still
collapsed. With the edge's own window at 65536 or at 4096: same collapse.
With the total number of outstanding requests cut to 2,048 (eight
connections at 256 each, under the admission window even on the real
cluster): same collapse. With a single connection holding 4,096 outstanding:
no collapse at all, 1.4 M/s. Whatever the trigger was, it counted
*connections*, not requests, and it did not care what was behind the edge.

The edge's own counters during a collapsed run settled it: **zero
backpressure events, zero retries**. The credit ladder — the mechanism M12
blamed — never ran.

## What the threads were doing

A per-thread sample of the edge process mid-collapse: the eight reader
threads at 82–97% CPU, *in user space* (not in any syscall), and the driver
thread — the only thread that completes requests — at 7%, parked. Readers
that are not reading, spinning on something that is not the kernel.

There is exactly one user-space spin on a reader's path: the shared-memory
MPSC ring that carries commands from the edge into the node. Its producer
does three things: claim a slot with a compare-and-swap on
`claim_position`, write the record into that slot, and then advance
`publish_position` past the slot — but only after the *previous* claimant
has advanced it past theirs:

```rust
while header.publish_position.load(Acquire) != claim_pos {
    std::hint::spin_loop();
}
header.publish_position.store(target_pos, Release);
```

Publication is serialized in claim order, and the wait is an unbounded spin.

## The convoy

Picture eight reader threads, a driver, and the node's own agent on an
8-vCPU host: ten threads that all want to run. Sooner or later the scheduler
takes a core away from a reader *between* its claim and its publish. Every
reader that claims after it now spins, waiting for a `publish_position` that
cannot move until the preempted thread runs again. But the spinners are
occupying the cores. The preempted thread waits for a scheduler quantum;
during that quantum every other producer arrives, claims, and joins the
spin. When the victim finally publishes, the next producer in line
publishes — and by then someone else has been preempted mid-window, because
the cores are still oversubscribed.

The loop feeds itself. Throughput falls to whatever the scheduler lets
through — a few thousand records a second — while every core reads 100%
busy. Downstream, the driver has nothing to complete, so no credits return,
so the clients stop sending, so the client host goes idle. That is the
whole M12 picture, and nothing in it involves a window.

It also explains the threshold. Sink + driver + six readers is eight
threads on eight cores: every producer always has a core, and nobody is
ever preempted inside the claim→publish window, so the spin is always
short. Add two readers and preemption there becomes routine.

## It is not a gateway bug

On a 4-vCPU laptop, with no gateway and no TCP — just N local `Engine`s,
each one producer thread, submitting into one dummy node:

| engines | resp/s |
|---:|---:|
| 2 | 1,531,127 |
| 4 | **5,589** |
| 8 | 1,229 |

The defect is in `uc_protocol::ring::mpsc`. The gateway was merely the
first thing in the tree to put eight producer threads on one ingress ring
on a host that had other work to do. Any deployment with several shmem
clients on a busy node host is exposed the same way.

## Two consequences for M12's guidance

- The operating-envelope rule ("total client inflight across all
  connections must stay under the node's window") does not prevent this.
  The fleet collapsed at eight connections with 2,048 outstanding.
- Bounding the edge's CPU with `CPUQuota=` — the other M12 recommendation —
  makes the convoy *worse*, because it starves the preempted producer
  harder. Containment of a churning edge is still sensible once the churn
  is impossible; it is not a remedy for this.

## The fixes

**Mitigation (on the branch, measured):** spin a bounded number of times,
then `yield_now()`. A waiter gives its core to whoever is runnable, which
includes the thread it is waiting for. Fast path unchanged. On the laptop
reproduction the 4-engine rung went from 5,589 to 160,604 resp/s — no
collapse, but still an order of magnitude under the single-engine number,
because the ring is still serializing publication across threads and the
scheduler is still in the loop.

**Structural fix (M13):** stop making producers wait for each other.
Each producer commits its own record by writing the length last (the
atomic-after-write prefix the rest of UC's framing already uses); the
single consumer walks records in claim order and stops at the first one
whose length is still zero. A preempted producer then costs the consumer
one thread's scheduling latency, once, instead of stalling every other
producer — the design Aeron's many-to-one ring buffer has always had.
`publish_position` becomes advisory or goes away, the wait/signal handle
follows, and the ring tests grow a case that preempts a producer inside the
window. The loom-on-rings item that has been open since the security
package named it is the right time to cash in here.

## The broader lesson from the bench

Every hop measured alone, the ranking was the opposite of what the
end-to-end numbers suggested: the shmem hop does 2.8 M/s, the raw TCP
floor 7.4 M/s, the shipped edge 1.4 M/s on one connection, the shipped
edge into the shipped cluster 1.14 M/s — and the shipped client 0.17 M/s
against a sink that answers instantly. The ~10× M12 charged to "TCP through
a gateway" was the client's own lock-and-futex structure, and the collapse
it charged to flow control was a spin in a ring. Isolation benches earn
their keep by removing explanations, not by confirming them.
