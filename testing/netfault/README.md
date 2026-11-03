# netfault — network fault injection for UC nodes, in eBPF

A pair of `tc` programs that drop, duplicate and delay UDP on one interface by peer address, port, direction and
**UC datagram kind**. A test can cut one link between two nodes and leave every other, cut a node off from its peers while its
clients stay attached, take away one half of loss repair (NAKs one way), make a link slow, reordering and
duplicating, or silence one node's votes and nothing else. Process kills and pauses cannot produce any of these,
and neither can the in-process `uc_net::fault::FaultSocket`, which never touches a kernel socket.

It is a fork of [faulteron](https://github.com/FredrikJDahlberg/faulteron), Fredrik Dahlberg's injector for Aeron,
with the Aeron frame parser replaced by UC's 16-byte datagram header (`uc_protocol::v2::datagram`). It lives in
this repository because what it parses is the wire: `testing/uc_crashtest/tests/netfault_pins.rs` pins the kind
table and the header offsets against the Rust constants, so a wire change that moves them fails `cargo test` until
this tool follows. See [`NOTICE`](NOTICE).

The header stays cleartext under wire crypto (it is the AEAD's associated data), so every kind rule works with
`[crypto] enabled = true`; a sealed DATA payload's frontier accounts for the 24-byte counter and tag.

Linux only, kernel 5.18 or later, IPv4 only. It runs as root on a Linux host, or in a privileged container of a
**rootful** docker daemon that shares the target container's network namespace. Under rootless docker nothing can
load BPF, however privileged the container. Traffic over shared memory (clients and services on the node's host)
never reaches an interface, so no rule can touch it; only the node↔node plane and the gateway's TCP are visible.

## Build

```bash
make                          # build/netfault.bpf.o, which bin/netfault finds from the checkout
sudo make install             # netfault and its program under /usr/local; PREFIX moves them
sudo NETFAULT_DEV=ens5 netfault attach
```

Building needs clang, `libbpf-dev` and `linux-libc-dev`; running needs root, `bpftool`, `iproute2` and `jq`.
`NETFAULT_DEV` names the interface, `eth0` by default, and `NETFAULT_OBJ` overrides where the program is found.
Or build the image, for containerised clusters:

```bash
docker build -t netfault:local testing/netfault
```

## Use

```bash
F() { docker run --rm --privileged --network "container:$1" netfault:local netfault "${@:2}"; }
F node-1 attach                                   # attach at tc ingress and egress of eth0, fq as root qdisc
F node-1 block 9001                               # drop all UDP to or from the replication port
F node-1 block --kind nak 9001                    # drop only NAKs: no loss repair toward or from node-1
F node-1 rule --kind data --loss 5 9001           # drop 5% of DATA
F node-1 rule --delay 20 --jitter 30 --duplicate 10 9001
F node-1 block --kind retransmit 9001             # drop only repairs: the repair-of-a-repair path
F node-1 block --out --kind vote 9001             # node-1's votes never leave; votes reaching it are untouched
F node-1 block --kind probe 9001                  # the cluster never raises its jumbo rung
F node-1 block --peer node-2                      # cut the node-1 ↔ node-2 link, on any port
F node-1 rule --kind snap-chunk --delay 50 9001   # a slow snapshot session
F node-1 stats                                    # <port> <kind> then settings and counters as name=value
F node-1 remove --kind nak 9001
F node-1 detach                                   # heals everything
```

On a host, replace `F node-1` with `sudo NETFAULT_DEV=<iface> netfault`.

A rule matches UDP to or from each port given, or any port if none is, narrowed by a selector:

- `--peer`: the far end of the packet, its destination going out and its source coming in, by IPv4 address or by a
  host name resolved in the target's network namespace. A cluster uses the same port on every node, so a peer is
  what singles one link out.
- `--kind`: a UC datagram kind by name — `data`, `heartbeat`, `nak`, `status`, `append-position`,
  `commit-position`, `request-vote`, `vote`, `term-map`, `read-probe`, `read-probe-ack`, `snap-begin`,
  `snap-chunk`, `snap-nak`, `snap-done`, `config-proposal`, `config-reply`, `hs-init`, `hs-resp`, `hs-key`,
  `snap-request`, `snap-redirect`, `probe`, `probe-ack`, `snap-report`, `backup-report`, `tier-install`,
  `tier-install-status` — or a number; `retransmit`; or `any` (the default), which also matches traffic that is
  not UC. `retransmit` is DATA whose position starts below the furthest its flow has already sent in the same
  leadership term; a flow is one sender's DATA to one destination address and port. A later term starts a new
  frontier, since a new leader may truncate, and a stale term's DATA is neither a repair nor a frontier.
- `--out` or `--in`: only packets leaving the interface, or only packets arriving at it. Neither means both, so a
  rule on a link acts on both directions of it unless told otherwise — `--peer node-2` cuts the link, `--out --peer
  node-2` cuts only what this node sends on it. (Roles make some kinds one-directional anyway: a leader sends DATA,
  a follower NAKs.)

A packet goes to the most specific rule that matches it: one naming its peer before one that does not, then its
destination port, its source port, any port, within each `retransmit`, then kind, then `any`, and within each of
those one naming the packet's direction before one that does not. A packet no rule matches costs up to 36 hash
lookups. A rule drops `--loss` percent of what it matches, sends `--duplicate` percent
of the rest twice, and holds it `--delay` plus up to `--jitter` milliseconds. Jitter longer than the gap between
packets reorders them.

- **Delay applies only to packets leaving the interface,** because the `fq` qdisc holds them until the departure
  time the program stamps. To delay what reaches a node, impair the sender.
- **A datagram is one unit.** A DATA datagram carries a run of frames; a rule acts on the whole datagram.
- **The attachment outlives each `docker run`.** A container restart gets a new network namespace and drops it.
- **A duplicate goes out at once; a delay holds only the original.**

## Tests

`test/netfault-rules.sh` needs a rootful docker alone and takes about ten seconds. It sends hand-made UC datagrams
and timestamped lines between two throwaway containers, with one rule of every kind installed, and checks which
rule each packet went to, what the rules counted, what the receiver got (delay, duplicates both ways, reordering),
that the CLI refuses bad input without changing any rule, and that `remove` and `detach` undo exactly what they
should. Its retransmission cases cover a term bump (not a repair) and a sealed flow (the frontier is the log bytes,
not the wire bytes), and its direction cases that an `--out` rule never acts on what arrives and an `--in` rule never
on what leaves. On a box whose default docker context is rootless, run it with `DOCKER_CONTEXT=default`.

`cargo test -p uc_crashtest --test netfault_pins` is the wire pin, and runs in the ordinary workspace test job.

The **scenarios** — `testing/uc_crashtest/tests/netfault.rs`, feature `netfault-tests` — put three real node
processes on `127.0.0.2/3/4`, attach this tool on `lo` (as root, or through `sudo -n`), and run four faults under
the WGL linearizability checker: NAK loss on one link, one member's votes silenced, one direction slow and
reordering, repairs dropped. Every rule is `--out` and names a `--peer`, because on `lo` a datagram passes both
hooks. `nightly.yml` runs them cleartext and with wire crypto on. Without host root, `scripts/run-suite-in-container.sh
[--crypto]` runs them inside a privileged container on a rootful docker, where the test process is root. The fleet
measurement is plan 2 of the design (`docs/superpowers/specs/2026-11-03-uc2-netfault-scenarios-design.md` §6).
