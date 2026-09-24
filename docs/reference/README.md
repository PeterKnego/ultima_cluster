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

- [Command cheatsheet](cheatsheet.md) — every command an application
  developer or an operator uses, with what it does and why, one line each
  ([printable A4 PDF](../uc-cheatsheet.pdf)).
- [Contributor command cheatsheet](contributor-cheatsheet.md) — the build,
  lint, proof and benchmark commands for work on UC itself.
- [`uc2ctl`](uc2ctl.md) — the admin CLI: every sub-command, its arguments, the
  three response statuses, and all twelve refusal reasons.
- [Instance directory](instance-directory.md) — every file a node owns, which
  process writes it, and which must survive a power cut.
- [Configuration](configuration.md) — `NodeConfig` field by field, the
  environment switches, crypto key material, and the cluster limits.
- [Limits](limits.md) — every hard limit, standing constraint and accepted
  residual in one table set, each row pointing at the page that owns it.

## SDK surfaces

What a service binary is written against.

- [The state-machine contract](state-machine-contract.md) — the two tiers
  (`RawStateMachine`/`StateMachine`), their exact signatures, the
  byte-identity promise, and the `out`-buffer discipline.
- [SDLC standard for applications](application-sdlc.md) — the lifecycle an
  application on UC follows: the design note, the three compatibility axes and
  the change taxonomy, the determinism rules, the conventions a version change
  must respect, the test pyramid, the upgrade stages S1–S9 per row, and the
  observability a feature must expose before it is production-ready.

## Compatibility

- [Versioning and the semver promise](semver-policy.md) — the lockstep version
  number, exactly which items are covered by semver and which are not, why the
  wire protocol and the cnc page are flag-day instead, the MSRV rule, and the
  one-way door in the two-tier state-machine contract.

## Internal surfaces

Formats you need when decoding a live system or reading the wire.

- [The cnc control page](cnc-page.md) — the 4 KiB page's pinned layout: header,
  counters and their single writers, node flags, and the eight peer slots.
- [Wire protocol](wire-protocol.md) — all twenty datagram kinds with their
  sealing scope, the log frame types, alignment, and the version gates.
- [Linearizable read path](read-path.md) — how a read is certified, the
  constants that govern it, and what its failure signatures mean.

## Related

- [Security](../security/) — which of these surfaces is authenticated and which
  is not: the [threat model](../security/threat-model.md), the
  [attack surface](../security/attack-surface.md) (one row per parser, plus
  bind-address guidance), and the
  [self-assessment](../security/self-assessment.md).
