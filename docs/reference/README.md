# Reference

Descriptions of the surfaces `ultima_cluster` exposes: what each one is, what
values it takes, and what its limits are. Dry by design — for the *why*, see
[Architecture](../ARCHITECTURE.md); for tasks, see
[the how-to guides](../how-to/).

**The library API is generated, not written here.**
[The rustdoc](https://peterknego.github.io/ultima_cluster/) covers every crate
and is rebuilt on each push to `main`. These pages cover the surfaces rustdoc
does not: the CLI, the on-disk layout, the shared-memory page, configuration,
and the wire.

## Operating surfaces

What an operator touches directly.

- [`uc2ctl`](uc2ctl.md) — the admin CLI: every sub-command, its arguments, the
  three response statuses, and all twelve refusal reasons.
- [Instance directory](instance-directory.md) — every file a node owns, which
  process writes it, and which must survive a power cut.
- [Configuration](configuration.md) — `NodeConfig` field by field, the
  environment switches, crypto key material, and the cluster limits.

## Internal surfaces

Formats you need when decoding a live system or reading the wire.

- [The cnc control page](cnc-page.md) — the 4 KiB page's pinned layout: header,
  counters and their single writers, node flags, and the eight peer slots.
- [Wire protocol](wire-protocol.md) — all twenty datagram kinds with their
  sealing scope, the log frame types, alignment, and the version gates.
- [Linearizable read path](read-path.md) — how a read is certified, the
  constants that govern it, and what its failure signatures mean.
