# Core Principles

`ultima_cluster` is built around three guiding principles, in this order.
When two of them pull in different directions, the one higher on the list
wins.

## 1. Correctness

Every machine must see exactly the same commands in exactly the same order, or
the copies silently drift apart. The part of the system that agrees on that
order is kept deliberately small and free of anything unpredictable — no
clocks, no network calls, no threads — so that it can be checked by a
simulator that replays millions of failure scenarios, and cross-checked
against a mathematical model that has been machine-verified. A command counts
as accepted only once a majority of machines have written it to disk.

## 2. Resiliency

The system keeps running when a machine fails, and never loses a write it has
already confirmed. It does this by running the same application on several
machines at once, all fed the same stream of commands in the same order. If
one machine dies, the others already hold everything it held; if one falls
behind, it is caught up in the background rather than holding the rest up.
None of this is taken on faith: the failure paths — machines killed mid-work,
networks split, a majority lost and restored — are exercised by an automated
test suite on every change.

## 3. High performance

Once the first two are satisfied, the system should be as fast as the hardware
allows. Seven rules govern the path a message takes through it, again in rank
order.

### 3.1 Batch from backlog, never from a timer

Traffic comes in bursts. Handling a burst as one unit is far cheaper than
handling each message alone — but the system never *waits* to see whether more
messages are coming. It takes whatever has piled up, sends it, and moves on. So
under heavy load it is efficient, and under light load a message is never held
back for company it may never get.

### 3.2 Nobody waits on anybody

Many senders feed the system at once. No sender's progress ever depends on
another sender getting its turn first — if it did, one sender that happened to
be paused by the operating system would stall everyone behind it. That exact
failure was measured, once, as the whole system grinding to a crawl; the rule
is written the way it is because of it.

### 3.3 Never sleep on a live partner

Waking a paused thread costs more than the network round trip it was waiting
for. So the working threads keep checking rather than parking themselves, and
they only ever go to sleep when the other side is genuinely gone. A thread
that dozes while its partner is still active does not merely slow itself: the
partner waits on it, its neighbours wait on the partner, and the whole group
ends up sleeping in turns.

### 3.4 Keep the common path plain

The path taken by an ordinary message stays short and predictable; anything
rare — a message that had to be split, a partner that fell behind — is handled
off to the side. This matters more than intuition suggests: code that sits in
the main loop slows it down even when it never runs, simply by being there.

### 3.5 One writer per value

Every shared number in the system — how far the log has grown, how far it has
been saved, how far it has been applied — is updated by exactly one component.
Nobody ever has to take turns, so there is no queue and no lock. Where the
programming language can enforce this it does; where it cannot, because the
writer is a separate program, the rule is written down and tested.

### 3.6 Share as little as possible

Even a value with a single writer costs something every time another part of
the system reads it, because the hardware has to keep everyone's view in step.
So the first question is whether a value needs to be shared at all, and the
shared ones are laid out so that no two live so close together that updating
one disturbs the other.

### 3.7 Copy only on purpose

Copying data is cheap but not free, and the bandwidth it uses is wanted
elsewhere. Data is handed along by reference wherever that is safe. The one
place it is copied is where the alternative would be unsafe — reading directly
from memory another program may overwrite — and that copy is kept because
never trading safety for speed is the point of principles 1 and 2.

---

For how these principles shape the system, see
[Architecture](docs/ARCHITECTURE.md); for how the first two are checked, see
[Verification](docs/VERIFICATION.md).
