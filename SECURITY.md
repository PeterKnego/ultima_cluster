<!-- SPDX-License-Identifier: Apache-2.0 -->
# Security policy

## Supported versions

The **latest minor release** is the only supported line. Today that is
**`2.7.x`**: fixes land on the newest patch of the newest minor, and there
are no backports to earlier minors. Versions move in lockstep across every
crate, the tag, the tarballs and the image — see
[the semver policy](docs/reference/semver-policy.md).

| Version | Supported |
|---|---|
| `2.7.x` | yes |
| `< 2.7` | no — upgrade |

## Reporting a vulnerability

**Please do not open a public issue.** Open a private security advisory on
GitHub instead:

> <https://github.com/PeterKnego/ultima_cluster/security/advisories/new>
> (repository → **Security** → **Advisories** → **Report a vulnerability**)

That channel is private to the maintainers until an advisory is published.
There is no separate security email address.

Useful things to include, if you have them: the version or commit, the
configuration that matters (`[crypto].enabled`, `[admin] auth`, whether a
gateway is exposed), what an attacker who can reach the affected surface could
do, and a reproducer — a fuzz artifact, a packet capture, or a test.

## What to expect

- **Acknowledgement within 7 days** of the report.
- An assessment after that: whether it is in the threat model, what the
  severity looks like, and an intended timeline.
- **Coordinated disclosure.** We will agree a date with you, publish a fixed
  release and the advisory together, and credit you unless you prefer
  otherwise.
- If the report turns out to be a documented residual rather than a defect, we
  will say so and point at where it is documented — and if it is *not*
  documented well enough, that is itself a fix we will make.

## Before you report: what is already known

Several properties look like vulnerabilities and are documented, deliberate
positions. Checking these first saves everyone a round trip:

- **Wire crypto is off by default.** Cleartext node-to-node traffic with no
  source authentication is the default posture, exactly as stated.
- **A malicious cluster member can forge fan-out traffic as any node** (the
  group key is symmetric), and a compromised host is a cluster member by
  definition. Both are explicitly out of model.
- **The remote client link is plain TCP with no client authentication and no
  TLS** in this release.
- **`/metrics`, `/healthz` and `/readyz` are unauthenticated**; the bind
  address is the control.
- **`app_id` is not a credential.**
- **`[admin] auth = "hmac"` authenticates cluster-wide only when paired with
  `[crypto].enabled = true`.**

All of it, with the reasoning: [`docs/security/`](docs/security/) —
[threat model](docs/security/threat-model.md),
[attack surface](docs/security/attack-surface.md),
[self-assessment](docs/security/self-assessment.md).

A report that one of the above is *worse than documented* — reachable in a way
the documents do not describe, or with a consequence they do not state — is
very much wanted.
