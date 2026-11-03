# netfault scenarios: the first network faults against a real UC cluster

*Design, 2026-11-03. Status: approved to proceed by the maintainer the same
day ("go ahead"); the builder-check round that followed changed three things,
each marked "(builder check)" below. Builds on `testing/netfault` (PR #194,
merged 2026-11-03) and the survey of the crash harness and the fleet drivers
made the same day. No scenario code exists yet.*

## 1. Intent

`testing/netfault` can drop, duplicate and delay UDP on a real interface by
peer, port and UC datagram kind. It has been proved against hand-made
datagrams and pinned to the wire, and it has **never impaired a UC node**.
Every network fault UC has ever been tested under was injected from inside
the process (`uc_net::fault::FaultSocket`, `uc_sim`'s virtual world). This
work puts the shipped node binary on a real kernel under faults no in-process
layer can express — one message kind, on one link, in one direction — and
watches it with the same linearizability checker the capstones use.

It has two homes, built in this order:

1. **Nightly** — a feature-gated test suite in `uc_crashtest`, three real
   node processes on one Linux host, run by `nightly.yml`. The regression
   net: a red here is a defect or a flake, never a number.
2. **Fleet** — the same scenarios on `bench-infra`'s hosts under the
   throughput driver, recorded as a gate doc. **The first fleet run is a
   measurement with no bar** (the maintainer's decision, 2026-11-03): every
   row is `REPORTED`, and the numbers it produces are the proposal for the
   bars of the second run. Safety is the one exception, see §4.

## 2. Goals, non-goals, anti-goals

**Goals.**

- A node under one-kind, one-link, one-direction faults stays **safe**: every
  history the checker sees is linearizable.
- The fault is **proved to have happened**: a scenario whose rule counted
  nothing is a failed scenario, not a pass. (The whole value of the tool is
  specificity; a vacuous rule is the failure mode to fear most.)
- The suite is **cheap enough for nightly**: under two minutes of wall time
  on a GitHub runner for all scenarios, no fleet, no docker.
- The fleet gate **replaces the dead netem knob** (`group_vars/all.yml`
  defines `netem_*`; nothing applies it) with per-link rules the driver
  installs and removes itself.

**Non-goals.**

- Not a performance gate. The fleet run reports throughput under each fault
  as a measurement; it judges nothing this run.
- Not a container harness. The nightly suite runs processes on the host and
  attaches netfault to `lo`; docker is not involved.
- Not a new client. The workload is the harness's existing shmem client and
  `RegisterSm` operations.
- Not a cross-host linearizability checker. The history stays in one process
  (§6).

**Anti-goals** — a result that would make the work a failure even if green:

- A scenario that passes because the rule matched nothing (wrong port, wrong
  peer, wrong direction). Every scenario asserts its rule's counters.
- A pass that depends on the fault being gentle. Each scenario's fault is
  total (`block`) wherever the protocol gives a path to survive it; loss
  percentages are used only where a total block would simply halt the
  cluster by design.
- A nightly suite that is skipped silently when it cannot attach. It refuses
  by name.

## 3. Constraints

- **Root.** Attaching needs CAP_BPF and CAP_NET_ADMIN. The test process is
  not root; it runs the CLI through `sudo -n`. GitHub's runners have
  passwordless sudo; a dev box may not. The suite is behind a cargo feature
  (`netfault-tests`) so a plain `cargo test` never reaches it.
- **One attachment per host.** netfault's rules live in one map per
  interface. Scenarios must not run concurrently: `--test-threads=1` in the
  nightly command and a process-wide mutex in the suite, so a stray parallel
  run fails fast rather than sharing rules.
- **A rule is bidirectional unless told otherwise (builder check).** The
  peer is the destination going out and the source coming in, and the port
  matches as source or destination, so `--peer X <port>` matches the X link
  in **both** directions. Scenarios A, C and D would still work because
  their kinds are sent by one role only; Scenario B would not — blocking
  "F1's votes" would also block F2's votes arriving at F1, and no election
  could complete. So netfault gains a **direction selector** first (§4a):
  `--out` (egress only), `--in` (ingress only), or neither (both, as
  today). Every scenario rule in both homes names a direction.
- **Loopback sees every packet twice.** On `lo` a datagram traverses the
  egress hook and then the ingress hook, with the same flow map: the egress
  pass advances the DATA frontier, and the ingress pass then sees the same
  datagram start below it and classifies it `retransmit` (builder check).
  With `--out` on every nightly rule, the ingress pass consults no rule and
  the classification is harmless; the egress classification is unaffected
  because the ingress pass never advances the frontier. Loss percentages
  and counters are likewise rolled once. The fleet has no doubling: each
  host's interface sees its own traffic once per direction. **Every rule in
  the nightly suite is `--out` and names a peer.**
- **Delay is egress-only** (netfault's design: `fq` holds the stamped
  packet). "Delay what node B receives from A" is a rule on A's traffic
  toward B. On `lo`, A and B share the interface, so the rule is
  `--peer <B's address> <A's port>`.
- **The crashtest node has no `/metrics`.** Node state is read from the cnc
  page through `common::open_cnc` as the existing tests do; fault state is
  read from `netfault stats`.
- **The checker's clock is one in-process counter**
  (`uc_lincheck::history::History`). All operations of one history must be
  issued from one process. This fixes where the workload runs in both homes.
- **Fleet hosts** are Ubuntu 24.04 (noble AMI; the kernel is whatever the
  AMI ships, recorded by the collect role's manifest), interface `ens5`, private
  addresses `10.10.1.10 + index`, nodes bound to `private_ip:19100`
  (`bench-infra`). netfault's 5.18 floor is clear by a wide margin. The
  interface is never hard-coded: `m12_fleet_gate.detect_iface` finds it.

## 4a. First, the tool change: a direction selector (builder check)

`rule_key` gains a `dir` field: `0` any, `1` out (egress), `2` in
(ingress). The lookup tries the packet's own direction before `any`, so the
most-specific-first rule keeps its meaning, and the worst case rises from
18 to 36 hash probes for a packet no rule matches. The CLI takes `--out` or
`--in` (mutually exclusive; neither means both), `stats` prints `out`/`in`
after the peer, the self-test gains cases proving an `--out` rule leaves
the reverse direction untouched and an `--in` rule the forward one, the
README's selector list and the precedence sentence grow by one item, and
`netfault_pins.rs` is unaffected (it pins kinds and the header, not the
key). This ships before any scenario, in its own PR, with the self-test
green.

## 4. The scenarios

Four scenarios, the same four in both homes. Every rule below carries
`--out` (so "F1's port toward L" means what F1 *sends* to L and nothing
else) and names its peer. Each has a **fault** (what
netfault does and on which link), a **proof the fault fired** (which
counter must be non-zero), the **safety check** (always the same: the
history is linearizable, `Violation` fails, `Inconclusive` is accepted as the
capstones accept it), a **liveness check** (nightly only; the fleet
measures instead), and the **measurements** the fleet records.

Addresses: node 0, 1, 2. In the nightly suite they are `127.0.0.2`,
`127.0.0.3`, `127.0.0.4`, each on a free port of its own, started with
`--bind <ip>:<port>` and `--members` listing all three. On the fleet they
are the three voter hosts' private addresses on port 19100. The leader is
whichever node `await_single_leader_multi` (nightly) or `wait_leader`
(fleet) reports once the cluster is up; rules are installed **after** the
leader is known, and the scenario names nodes by role (leader L, followers
F1, F2), never by index.

### Scenario A — NAK loss on one link

- **Fault.** `block --out --kind nak --peer <L> <F1's port>`: every NAK F1
  sends toward L is dropped on the way out. The other link is untouched; so is
  everything else F1 sends. Then, to give F1 something to repair, `rule
  --out --kind data --loss 10 --peer <F1> <L's port>`: one DATA datagram in
  ten from L to F1 is lost. F1 now detects loss it can never repair by NAK.
- **Fired.** The NAK rule's `dropped` > 0 and the DATA rule's `dropped` > 0.
- **Safety.** Linearizable throughout, including after heal.
- **Liveness (nightly).** L and F2 form a quorum, so commit must keep
  advancing during the fault: `ok` operations during the fault window > 0.
  After `detach`, F1 must catch up: its durable position reaches L's commit
  position within 10 s (read from the cnc page; the catch-up goes through
  deep NAK or journal replay, whichever the gap selects, and the scenario
  does not care which).
- **Measured (fleet).** Committed-writes/s before, during and after; time
  from heal to F1's durable position reaching L's commit; `uc2_naks_*`
  counters on all three nodes, read through the probe.

### Scenario B — one member's votes silenced

- **Fault.** `block --out --kind vote --peer <F2> <F1's port>` and the same
  with `--peer <L>`: F1's VOTE datagrams never leave; its REQUEST_VOTE,
  STATUS and everything else do, and votes *arriving* at F1 are untouched
  (this is exactly the case the direction selector exists for). Then L's node and service are SIGKILLed, exactly as
  `leader_node_sigkill_recovery_multi_once` does.
- **Why this shape.** With three voters and L dead, a candidate needs one
  vote besides its own. F2 as candidate needs F1's vote, which cannot
  arrive; F1 as candidate needs F2's vote, which can. So the cluster must
  elect, and the member it elects is the one nobody can hear voting. That is
  a liveness claim the protocol makes and nothing has tested.
- **Fired.** The vote rule's `dropped` > 0 (at least F1's vote for F2's
  first term).
- **Safety.** Linearizable across the failover.
- **Liveness (nightly).** A single serving leader within 10 s of the kill
  (`await_single_leader_multi` with its never-two-leaders assertion), and the
  workload's `ok_after_recovery > ok_before_kill`. The scenario records which
  node won and expects F1; a win by F2 is recorded as a finding (it would
  mean a vote reached F2 by a path the rule did not cover), not a pass.
  L is restarted after the election and must rejoin as a follower.
- **Measured (fleet).** Election latency (kill to `can_serve` on the new
  leader); the elected node's role; see §6 for the fleet's client placement
  in this row.

### Scenario C — one direction slow and reordering

- **Fault.** `rule --out --delay 20 --jitter 30 --peer <L> <F1's port>`:
  everything F1 sends toward L (STATUS reports, NAKs, votes) is held 20 to 50 ms, and
  jitter past the gap between datagrams reorders them. L's traffic toward
  F1 is untouched.
- **Fired.** The rule's `delayed` > 0.
- **Safety.** Linearizable.
- **Liveness (nightly).** Commit keeps advancing (`ok` > 0 during the fault
  window) — F2's reports arrive on time, so the quorum is never held by the
  slow report. Linearizable reads keep completing: the `READ_PROBE` barrier
  needs a quorum of acks, and F2's is prompt.
- **Measured (fleet).** Committed-writes/s and linearizable-read latency
  p50/p99 during the fault versus baseline; `uc2_peer_replication_lag_bytes`
  for F1 over the window.

### Scenario D — repairs dropped

- **Fault.** `rule --out --kind data --loss 10 --peer <F1> <L's port>` and
  `block --out --kind retransmit --peer <F1> <L's port>`: one DATA datagram in ten from L
  to F1 is lost, and every DATA datagram L sends F1 that starts below the
  flow's frontier — every repair — is dropped. F1 NAKs, L repairs, the
  repair never arrives; F1 falls behind by exactly the lost datagrams until
  heal.
- **Fired.** The retransmit rule's `dropped` > 0. This is also the first
  live check that netfault's `retransmit` classification sees UC's repairs
  as repairs (the self-test used hand-made positions).
- **Safety.** Linearizable.
- **Liveness (nightly).** Commit keeps advancing (L and F2). After `detach`,
  F1's durable position reaches L's commit within 10 s.
- **Measured (fleet).** `uc2_naks_sent_total` on F1 and `uc2_naks_served_total`
  on L during the window (served but never delivered), the retransmit rule's
  `dropped` count, F1's lag at heal, and time to catch up.

**Common shape of every scenario** (nightly): start three nodes, wait ready,
start three services, wait for one leader, `attach` on `lo`, warm up, start
workers (3 workers, the 15 ms throttle and 15 s per-op deadline of the
existing tests), install the rules, run the fault window (5 s), remove the
rules (`detach`, which heals everything at once), run the heal window
(5 s), join workers, read `stats` before detaching, assert fired, assert
linearizable, assert liveness. `attach`/`detach` are paired by a guard that
detaches on drop, so a panicking scenario leaves `lo` clean. A scenario's
history is dumped to `CARGO_TARGET_TMPDIR` on failure as the capstones do.

## 5. Nightly home — how it is built

- **Where.** `testing/uc_crashtest/tests/netfault.rs`, behind a new empty
  feature `netfault-tests` on `uc_crashtest`, included from `tests/common`.
  New helpers go into `tests/common/mod.rs` beside the existing ones:
  `free_addr_on(ip)`, a `Netfault` guard (`attach`/`rule`/`block`/`stats`/
  `detach` over `sudo -n`, parsing `stats` lines into a map keyed by the
  line's selector prefix), and cnc readers for the positions the liveness
  checks need.
- **Finding the program.** `NETFAULT_OBJ` if set, else
  `<CARGO_MANIFEST_DIR>/../netfault/build/netfault.bpf.o`. Missing object,
  failing `sudo -n`, or an attach error **fail the suite by name** at its
  first scenario. The suite never skips.
- **Nightly job.** A new job in `nightly.yml`, not a step on `crashtest`:
  install clang and the libbpf headers, **plus the host-side runtime the
  CLI needs — `bpftool`, `jq`, `iproute2` (builder check:** `ci.yml`'s
  `netfault` job runs the CLI only inside the docker image, so the host
  path is unproved on a runner. Ubuntu packages `bpftool` through
  `linux-tools-<kernel>`, which may not exist for the runner's Azure
  kernel; the fallback is a pinned static binary from the `libbpf/bpftool`
  GitHub releases, verified by checksum. The builder proves whichever works
  on a draft PR before the suite depends on it). Then `make -C
  testing/netfault`, then `cargo test -p uc_crashtest --features
  hard-crash-tests,netfault-tests --test netfault -- --test-threads=1`.
  The `hard-crash-tests` feature is a dependency because the suite reuses
  that feature's helpers; the builder may instead lift the shared helpers
  out from under the feature if that is cleaner. A crypto-on leg
  (`UC2_CRYPTO=1`, as `crashtest-crypto` does) runs the same four
  scenarios, since the kind rules are claimed to work under crypto and
  Scenario D's `retransmit` frontier depends on the sealed-payload
  arithmetic.
- **Runtime.** Each scenario is about 15 s plus node boot; four scenarios
  twice (crypto off and on) is well inside the two-minute goal.

## 6. Fleet home — how it is built

- **Install.** A new ansible role `netfault`: apt `clang libbpf-dev
  linux-libc-dev bpftool jq iproute2`, then `make -C <checkout>/testing/
  netfault install` with `become`. It joins `provision.yml` after
  `build_uc`. The dead `netem_*` vars are removed from `group_vars` and the
  collect manifest in the same change.
- **Driver.** `bench-infra/scripts/netfault_gate.py`, built from the
  existing library drivers (`m6_fleet_gate`'s `SshHost`, `start_unit`,
  `kill_unit`, `wait_leader`; `m12_fleet_gate`'s `ssh`, `detect_iface`,
  `Verdict`), with the probe driven as `m6_gate`'s `probe` role through
  `start_unit` (there is no `probe` library function — builder check). It installs rules with `sudo NETFAULT_DEV=<iface>
  netfault …` on the host that owns the impaired direction, reads `stats`
  before `detach`, and records per-row JSON lines plus the usual
  `[PASS]/[FAIL]`/`RESULT:` log. Rows A, C and D run 30 s baseline, 30 s
  fault, 30 s heal under the load client, with `m12_gate`'s warmup and
  measure windows honoured (the standing lesson: no rate is comparable
  without them).
- **The nodes, the load and the checker (builder check).** `m6_gate`'s
  loadclient is one sequential thread against its own register codec, and
  a single sequential client's history is linearizable whenever its reads
  are monotone, so a verdict from it would be vacuous. The fleet rows
  therefore run the **crashtest binaries** (`uc_crashtest-node`,
  `uc_crashtest-service` serving `uc_lincheck::RegisterSm`) under transient
  systemd units exactly as `m6_fleet_gate` starts its roles, and the load is
  a new **history client** binary in `uc_crashtest` — the suite's concurrent
  workers (`spawn_workers_multi`'s shape: several workers, a throttle, a
  per-op deadline) over shmem to the local node, recording one
  `uc_lincheck` history and running `check_register` at the end of the row,
  with the worker count and throttle chosen so a 90 s row stays well under
  the checker's budget (`DEFAULT_BUDGET`; three workers at the 15 ms
  throttle give about 18 k entries). It runs on the leader's host. There is
  no separate throughput client this run: the committed rate is read from
  the probe's cnc `commit` word over the window, and the history client's
  own ok/s is reported beside it. That is a smaller load than the rate gates
  drive, and the gate doc says so; this run is a measurement, and the
  second run can add the throughput client as a second FSM once the
  numbers say what to look for. **Row B on
  the fleet cannot keep the client alive across the kill** — the client is on
  the leader's host and dies with it — so row B on the fleet records election
  latency and the elected node only, with no history, and says so in its
  row. (Nightly covers B's history: there, the client is the test process,
  which outlives every node.) Rules on the fleet also carry `--out`, so
  "F1 toward L" is installed on F1's host and nothing else is touched.
- **The gate doc.** `docs/benchmarks/uc2-netfault-gate-<date>.md` in the
  standard shape (what it measures, the decide rule committed before any
  run, rig, a `| row | arm | bar | result |` table, per-row sections,
  when it is run, related). Every result cell this run is `REPORTED` with
  the measurement; the bar column says `no bar — first run is a
  measurement (maintainer, 2026-11-03)`. **Exception:** the safety column
  of rows A, C and D is a hard PASS/FAIL on the WGL verdict, because a
  rejected history is a defect regardless of any number (this is the
  designer's call, flagged for the maintainer). The doc's closing section
  proposes bars for the second run from the numbers observed.
- **Fleet rules fire once.** Unlike `lo`, a host's interface sees each
  direction once, so rules may omit `--peer` where the port alone names the
  link; the driver still names peers for symmetry with the nightly suite.

## 7. What a red means

Nightly, in order of the line the maintainer wants to read first:

1. `Violation` from the checker — a consensus defect. Hard failure, history
   dumped.
2. A rule that counted nothing — the fault did not happen. Hard failure
   naming the rule; the first suspect is a port/peer/direction mistake in
   the scenario, the second a change in which node sends that kind.
3. A liveness check missed — commit did not advance during the fault, or
   F1 did not catch up within 10 s of heal, or no leader within 10 s. A
   real finding or a runner flake; the ledger in
   `docs/VERIFICATION.md`/memory decides which after a re-run, as with every
   nightly red.

Fleet: a `FAIL` can come only from the safety column; everything else is a
reported number and a proposed bar.

## 8. Left to the builder

- The exact cnc fields and helper names for "F1's durable position" and
  "L's commit position" (both exist on the page; `open_cnc` reaches them).
- Whether the shared process-spawning helpers move out from under
  `hard-crash-tests` or the new feature simply implies it.
- The fleet load client's extension: in `uc_node/examples/m6_gate.rs`'s
  loadclient role or a new role in the same binary.
- The shape of `netfault stats` parsing (a line's prefix up to ` loss=` is
  the selector, the rest `name=value` pairs; the self-test's `expect`
  already does this in awk).
- Fault-window lengths beyond the ones stated, if a scenario needs longer
  to produce a counted event (Scenario B's vote, in particular, may need
  the kill to come a little after the rule so the first term's vote is the
  one dropped).
- A first task that is a **spike**: two UDP sockets on `127.0.0.2` and
  `127.0.0.3`, one `block --peer` rule on `lo`, proof that exactly one
  direction is cut and that `fq` on `lo` delays. Everything in §3's loopback
  paragraph is reasoning, not yet observation; the spike turns it into
  observation before the suite is built on it.

## 9. The builder-check round

Three questions came back; none was the maintainer's to answer, and all
three are settled above as the designer's calls: the direction selector
(§4a, §3), the fleet history client in place of the sequential loadclient
(§6), and the host-side `bpftool` install with its fallback (§5). Two
further claims the check found the code contradicts were corrected in
place (the loopback `retransmit` double classification in §3; `probe` is a
unit role, not a library function, in §6).

## 10. Related

- `testing/netfault/README.md` — the tool; `docs/VERIFICATION.md` §2 "The
  kernel-level fault layer" — the current (instrument-only) record.
- `docs/BACKLOG.md` §5 — the direction this design takes up; its line is
  updated to point here when this ships.
- `testing/uc_crashtest/tests/hard_crash.rs` — the multi-process pattern
  reused; `tests/common/mod.rs` — the helpers.
- `bench-infra/scripts/m6_fleet_gate.py`, `m12_fleet_gate.py` — the fleet
  library drivers; `docs/benchmarks/uc2-jumbo-frame-discovery-gate-2026-09-13.md`
  — the gate-doc shape, including `REPORTED` rows.
- `docs/notes/uc2-term-map-window-loss-explained.md` — why NAK repair and
  deep NAK matter, the mechanism Scenarios A and D exercise.
