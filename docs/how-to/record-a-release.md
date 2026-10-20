# Record a release

[Twelve-factor #5](https://12factor.net/build-release-run) defines a
**release** as a build *plus* the config of the deploy it is going into,
carrying "a unique release ID" in "an append-only ledger" that "cannot be
mutated once it is created".

UC has no deploy system of its own — you run it under systemd, compose or a
scheduler — so the ledger is yours. What UC guarantees is that both halves of
the pair are **identifiable and independently verifiable**, so the ledger you
keep is checkable against a running process rather than being a note to self.

This page is the recipe. If you only want to know what changed between
versions, you want [`RELEASES.md`](../../RELEASES.md) instead.

## The two halves

**The build.** A release tarball ships with its own `sha256`, and the
container image has a digest:

```bash
sha256sum -c uc2-2.9.0-x86_64-unknown-linux-gnu.tar.gz.sha256
cosign verify-blob --signature ... uc2-2.9.0-x86_64-unknown-linux-gnu.tar.gz   # see cut-a-release.md
docker inspect --format '{{index .RepoDigests 0}}' ghcr.io/peterknego/uc2:2.9.0
```

A running node reports the version it was built from on its metrics endpoint:

```
uc2_build_info{version="2.9.0"} 1
```

**The config.** Every node digests its config file at startup and says so, in
the same log stream as everything else:

```json
{"ts_ns":1788160116836250555,"level":"info","event":"config_loaded",
 "path":"/etc/uc2/node.toml",
 "sha256":"a3781d905dcca976a2d34b31a19d948fef55fd1b8a8e4b9f77b596fca0b62721"}
```

That digest is plain SHA-256 over the file's bytes, so you can check it
against the copy in version control with no UC tooling at all:

```bash
sha256sum /etc/uc2/node.toml
git -C infra show HEAD:clusters/prod/node.toml | sha256sum
```

It is the digest of the file **as read, before environment overrides**. That
is deliberate: the file is the artifact under version control, and hashing a
post-override "effective config" would require a canonical serialisation that
is not stable across UC versions. The overrides are recorded separately —
each one that fires emits its own record:

```json
{"ts_ns":...,"level":"info","event":"config_env_override","var":"UC2_BIND","value":"10.0.0.2:9100"}
```

So the full identity of what a node is running is: **the build's digest, the
config file's digest, and the set of `config_env_override` records.**

## The recipe

For each cluster-wide upgrade, append one immutable row. A file in git is
enough — the properties that matter are append-only and never edited in
place, not the storage:

| field | where it comes from |
|---|---|
| `release_id` | yours; monotonic, never reused (`prod-2026-08-31-01`) |
| `at` | when the change was applied, UTC |
| `build` | tarball `sha256` or image digest — never a floating tag |
| `version` | the `uc2_build_info{version}` you expect to see afterwards |
| `config_sha256` | one per distinct config file in the deploy |
| `env` | the `UC2_*` overrides in effect, per role |
| `nodes` | which hosts it was applied to |

Then verify, rather than assume, that the cluster came up as the row claims:

```bash
# every node reports the version this release says it should
for h in n0 n1 n2; do
  curl -s "http://$h:9600/metrics" | grep uc2_build_info
done

# and the config digest each node actually loaded
journalctl -u uc2-node --since "10 min ago" -o cat \
  | grep '"event":"config_loaded"' | tail -1
```

A row whose `config_sha256` does not match what the node reports means the
host's file drifted from the one in version control — which is precisely the
failure a ledger exists to catch.

## What this does not buy you

**Rollback is still a cluster event, not a release switch.** Since `2.15.0`
a release within one wire level of the cluster's rolls, and rolls back, one
host at a time until the level is raised (manual mode,
`wire_raise = "manual"`, keeps that window open); after the raise, going back
means restoring a pre-upgrade backup on every node, as
[Upgrade a cluster](upgrade-a-cluster.md#rolling-upgrades-since-2150)
describes. A release that is a [flag day](../reference/semver-policy.md) goes
back the way it went forward: every node stopped, the previous binaries
started together. There is no per-node release pointer to flip, and a ledger does not
create one. The ledger tells you *what* to go back to; it does not make going
back cheaper.

Through `2.14.0` this constraint was absolute and chosen deliberately: a
rolling deploy that tolerated mixed versions would have had to accept a
`0.4.0` peer's unattested durable report, and that trade was refused on
safety grounds. The committed wire level is how mixed versions became safe:
every node encodes at the committed level and refuses an out-of-range peer by
name ([the wire level, explained](../notes/uc2-wire-level-explained.md)). See
[the twelve-factor assessment](../notes/uc2-twelve-factor-assessment.md) for
the full reasoning on factors 5 and 10.

## Related

- [Upgrade a cluster](upgrade-a-cluster.md) — the rolling procedure and the
  stop-all (flag-day) procedure.
- [Cut a release](cut-a-release.md) — the maintainer side: tagging, signing,
  the ordered crates.io publish.
- [Configuration § Environment overrides](../reference/configuration.md#environment-overrides)
  — what may vary per deploy, and what deliberately may not.
- [Monitor a cluster](monitor-a-cluster.md#structured-records) — the record
  catalogue, including `config_loaded` and `config_env_override`.
