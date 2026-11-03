<img src="doc/branding/faulteron-logo.jpg" alt="faulteron, an Aeron eBPF fault injector" width="480">

[![License](https://img.shields.io/github/license/FredrikJDahlberg/faulteron)](LICENSE)

Network fault injection for Aeron deployments, in eBPF. A pair of tc programs drop, duplicate and delay UDP on one
interface by peer address, port, Aeron frame type, retransmission and SBE message. A test can cut one link between
two members and leave every other, cut a member off from its peers while its clients stay connected, take away
one half of Aeron's loss repair, make a link slow, reordering and duplicating, or silence one member's votes and
nothing else. Process kills and pauses cannot produce any of these.
[seqeron](https://github.com/FredrikJDahlberg/seqeron), an Aeron Cluster sequencer, drives it in its network-fault
tests.

Linux only, kernel 5.18 or later, IPv4 only. It runs in a privileged container that shares the target
container's network namespace, or as root on a Linux host; on macOS, use the Docker VM (Colima or Docker
Desktop). Traffic over `aeron:ipc` never reaches an interface, so no rule can touch it.

## Build

```bash
docker build -t faulteron:local .
```

On a Linux host, without docker, the Makefile builds the program (it needs clang, `libbpf-dev` and
`linux-libc-dev`) and installs it beside the CLI, which then needs root, `bpftool`, `iproute2` and `jq`:

```bash
make                          # build/faulteron.bpf.o, which bin/faulteron finds from the checkout
sudo make install             # faulteron and its program under /usr/local; PREFIX moves them
sudo FAULTERON_DEV=ens5 faulteron attach
```

`FAULTERON_DEV` names the interface, `eth0` by default, and `FAULTERON_OBJ` overrides where the program is found.

## Use

```bash
F() { docker run --rm --privileged --network "container:$1" faulteron:local faulteron "${@:2}"; }
F node-1 attach                                   # attach at tc ingress and egress of eth0, fq as root qdisc
F node-1 block 9303 9314                          # drop all UDP to or from these ports
F node-1 block --type nak 9324                    # drop only Aeron NAKs
F node-1 rule --type data --loss 5 9324           # drop 5% of Aeron DATA
F node-1 rule --delay 20 --jitter 30 --duplicate 10 9313
F node-1 block --type retransmit 9324             # drop only retransmitted DATA
F node-1 block --template request-vote 9303       # drop only RequestVote messages
F node-1 block --peer node-2                      # drop all UDP to and from node-2, on any port
F node-1 rule --schema 101 --delay 50 9301        # delay archive control messages
F node-1 stats                                    # <port> <type> then settings and counters as name=value
F node-1 remove --type nak 9324
F node-1 detach                                   # heals everything
```

A rule matches UDP to or from each port given, or any port if none is, narrowed by a selector:

- `--peer`: the far end of the packet, its destination going out and its source coming in, by IPv4 address or by a
  host name resolved in the target's network namespace. Multi-host Aeron Cluster deployments use the same ports
  on every member, so a peer is what singles one out.
- `--type`: an Aeron frame type (`data`, `nak`, `sm`, `err`, `setup`, `rttm`, `res` or a number), `retransmit`,
  or `any` (the default), which also matches traffic that is not Aeron. `retransmit` is DATA that starts below
  what its flow has already sent; a flow is a publication's session and stream plus the destination address and
  port, since a multi-destination publication sends each destination its own copy.
- `--schema`: the SBE schema id of a DATA frame's message, such as Aeron Cluster's 111 or Aeron Archive's 101.
  It implies `--type data`.
- `--template`: the SBE template id within `--schema`, which defaults to Aeron Cluster's. Cluster messages also
  go by name: `canvass-position`, `request-vote`, `vote`, `new-leadership-term`, `append-position`,
  `commit-position`, `catchup-position`, `stop-catchup`, `termination-position`, `termination-ack`,
  `backup-query`, `backup-response`, `heartbeat-request`, `heartbeat-response`, `standby-snapshot` (ids as of
  Aeron 1.53.2). It implies `--type data`.

Schema and template are read from a message's first fragment only; a later fragment matches as plain DATA. A packet goes
to the most specific rule that matches it: one naming its peer before one that does not, then its destination port, its
source port, any port, and within each `retransmit`, then type, schema and template, type and schema, type, `any`. A
packet no rule matches costs up to 30 hash lookups. A rule drops `--loss` percent of what it matches, sends
`--duplicate` percent of the rest twice, and holds it `--delay` plus up to `--jitter` milliseconds. Jitter longer than
the gap between packets reorders them.

- **Delay applies only to packets leaving the interface,** because the `fq` qdisc holds them until the departure
  time the program stamps. To delay what reaches a member, impair the sender.
- **Aeron batches DATA frames,** so a DATA rule acts on whole datagrams.
- **The attachment outlives each `docker run`.** A container restart gets a new network namespace and drops it.

## Tests

`test/faulteron-rules.sh` needs docker alone and takes about ten seconds. It sends hand-made Aeron frames and
timestamped lines between two throwaway containers, with one rule of every kind installed, and checks which rule
each packet went to, what the rules counted, what the receiver got (delay, duplicates both ways, reordering), that
the CLI refuses bad input without changing any rule, and that `remove` and `detach` undo exactly what they should.

seqeron's network-fault tests, which partition, slow and silence its three-node docker cluster with faulteron, are
in seqeron's repository, under `seqeron-service/src/test/scripts/faulteron`, since they assert its guarantees and
change with it. They build the injector image from a checkout of this repository.

## License

Copyright 2026 Fredrik Dahlberg

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

[NOTICE](NOTICE) records the copyright, and §4(d) of the License obliges anyone redistributing faulteron to carry it
forward.
