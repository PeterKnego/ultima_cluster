# ultima_cluster — release tarball

You are holding `uc2-<version>-<target>.tar.gz`, unpacked. It contains
everything needed to run a real cluster: five static-ish binaries built
against glibc for `<target>`, example configuration, service units, and the
monitoring assets. There is no installer and no build step.

```
uc2-<version>-<target>/
  bin/
    uc2-node          the daemon: consensus, replication, durability
    uc2ctl            admin CLI: status, membership, backup/restore, keys
    uc2-gateway       TCP front door for clients that cannot attach to shmem
    counter-service   example state machine (the "service half" of a node)
    counter-remote    example remote client, driven through a gateway
  packaging/
    node.example.toml      every node setting, with its default and why
    gateway.example.toml   every gateway setting, same
    quickstart-local.sh    a whole three-node cluster on this host, now
    compose.yml            the same demo in containers
    Dockerfile             builds the container image from this tarball
    systemd/               uc2-node, uc2-gateway, uc2-service@ units
    prometheus/            alert rules (every one of them proven to fire)
    grafana/               dashboard
  LICENSE
  README-release.md        this file
```

## Verify it first

Every release ships `SHA256SUMS` and a `.sigstore.json` bundle per file,
signed keylessly by the GitHub Actions workflow that built them — there is no
long-lived private key to steal.

```sh
sha256sum -c SHA256SUMS --ignore-missing

cosign verify-blob \
  --bundle uc2-<version>-<target>.tar.gz.sigstore.json \
  --certificate-identity-regexp \
    'https://github.com/PeterKnego/ultima_cluster/.github/workflows/release.yml@refs/tags/v.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  uc2-<version>-<target>.tar.gz
```

The identity regexp is the load-bearing part: it says *which workflow in which
repository* produced the file. A signature that verifies against some other
identity is not this project's.

`uc2-<version>.cdx.tar.gz` alongside them is the CycloneDX SBOM — one document
per workspace crate, listing the full dependency graph the binaries were built
from.

## Run it

```sh
packaging/quickstart-local.sh
```

Three nodes, three services and three gateways start on this host, an election
happens, two writes are committed by a real majority, and a linearizable read
is driven back through a gateway from the outside. It prints `PASS` and cleans
up after itself; `--keep` leaves the cluster running so you can poke at it. It
needs nothing but `bash` and coreutils — no toolchain, no root, no containers
— and refuses to put cluster state on a RAM-backed filesystem, because every
`fsync` there is a silent lie.

For the container version of the same demo:

```sh
UC2_IMAGE=ghcr.io/peterknego/uc2:<version> docker compose -f packaging/compose.yml up -d
```

Both are single-host demos. A cluster whose three nodes share a kernel and a
disk has a majority of processes, not a majority of failure domains.

## Then read

- `QUICKSTART.md` in the repository — the guided version of the above.
- `docs/how-to/run-a-cluster.md` — one node per machine, for real. Start with
  the address rule; it causes the most confusing failure in the system.
- `docs/how-to/monitor-a-cluster.md` — wiring up `packaging/prometheus/` and
  `packaging/grafana/`.
- `docs/reference/configuration.md` — every setting, normatively.
- `RELEASES.md` — what changed in this version, and what it obliges you to do
  when upgrading. Since v2.6.0 a `node.toml` must state `[crypto]` and
  `[admin]` explicitly; an older file is a named startup refusal, not a
  silent default.

Apache-2.0. See `LICENSE`.
