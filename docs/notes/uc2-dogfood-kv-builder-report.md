# Building an application on UC: a clean-room builder's report

*Written 2026-09-18 as the builder half of the dogfood KV experience
assessment (wayfinder map #16,
charter `docs/superpowers/specs/2026-09-13-uc2-dogfood-kv-charter.md`). A
"builder" is an agent given only the **published docs** — the 2.12.0 release
tarball, `docs/how-to`/`reference`/`notes`, rustdoc, and `examples/counter` —
in a sandbox that cannot read this repository, and asked to build a real
service on UC. Every place the published material fell short is a **ledger
item**; the point of the exercise is the ledger, not the code. The worked
example the builder produced is now `examples/kv`.*

## The question

Can someone build a correct, non-trivial replicated service on UC from the
published documentation alone, without reading the source? The KV store is the
test case: a four-operation key-value store (put / get / delete / compare-and-set)
that must be linearizable, survive leader loss, and reconstruct after a crash —
then a second version that adds list-valued keys (`append` / `list`) and a
`VERSION` bump, to exercise the application-upgrade story.

## What the answer turned out to be

**Yes, from the docs alone, in one sitting, with no help.** The v1 store was
built in a single 28-minute clean-room session with **zero maintainer
interventions**, and its gate rows passed: linearizable under leader kills with
zero acknowledged-write loss (`docs/benchmarks/uc2-dogfood-kv-gate-2026-09-15.md`,
rows B2-v1.i/ii), and Elle list-append clean for v2 (B2-v2.iv). The one
gate failure, B2.iii, is **not the builder's** — it is a platform fail-stop
(`IngressRingCorrupt` under a coordinated snapshot instant with concurrent
ingress load), filed as product issue
[#32](https://github.com/PeterKnego/ultima_cluster/issues/32); the KV store
itself stayed linearizable through it.

That is the headline: the `RawStateMachine` / `StateMachine` contract, the
`SnapshotStateMachine` capability, and the `Sessioned` exactly-once wrapper
were learnable and usable from the reference alone. The builder reached for
`bytes::Bytes` end-to-end, implemented a deterministic `apply`, and got
exactly-once-over-a-remote-hop working — the hard parts of SMR — without seeing
the code that enforces them.

## Where the docs fell short

The builder ledger ran to 22 items across the two versions. They triaged
([#29](https://github.com/PeterKnego/ultima_cluster/issues/29)) into two
mechanical doc fixes landed immediately (`5a6f694`), six product tickets, and
fourteen items folded into this report and the lifecycle docs. The ones worth
carrying forward:

- **A mixed-version live upgrade can acknowledge a write no quorum can apply
  (L20 + L22) — silent loss.** The builder discovered, writing the v2 upgrade
  test, that a service-only *rolling* swap lets a v2 leader commit an `Append`
  the v1 followers cannot apply; if that leader then fails, the v1 successor
  has no record of the acknowledged write until every service is v2 and
  replays the log. UC makes this *visible* (`Uc2ServiceVersionDrift`) but does
  not prevent it. This is the single most important finding of the builder
  track, filed P1 as
  [#33](https://github.com/PeterKnego/ultima_cluster/issues/33). It is the
  reason the application upgrade is a **flag day** at 2.12.0, and the reason
  the upgrade how-to leads with "stop every service, then start every service."

- **A persisted `client_id` cannot resume its `seq` (L9).** The `Sessioned`
  wrapper gives exactly-once over a remote hop, but `RemoteConfig` has no way
  to set a starting sequence number, so a client that persists its id across a
  restart cannot safely continue — it risks stale `REPLAYED` answers. Filed
  [#34](https://github.com/PeterKnego/ultima_cluster/issues/34).

- **A dead node reads as a serving leader (L13).** `uc2ctl status` on a node
  whose peers are gone still prints `leader=true can_serve=true`, and a gateway
  in front of it swallows writes. Filed
  [#35](https://github.com/PeterKnego/ultima_cluster/issues/35); the operator
  track hit the alerting side of the same coin (see the operator report).

- **The shipped snapshot example does not exist (L8).** The reference points a
  `SnapshotStateMachine` author at examples that ship no snapshot code —
  `examples/counter` is typed-tier, no snapshots, no sessions. The builder had
  to infer the artifact-envelope contract from prose. **Merging the KV store as
  `examples/kv` is the fix**: it is the first shipped
  `SnapshotStateMachine` + `Sessioned` worked example.

The remaining items are prose gaps (dependency versions unknowable behind
`workspace = true`, the position-0 apply contract unstated, the payload ceiling
the `Sessioned` envelope quietly shrinks) and are corrected in the reference
sweep this ticket carries.

## Accepted limits

Three ledger items were recorded as **accepted limits**, not fixed, because the
fix is out of this effort's scope:

- **A remote client cannot read a follower's replica (L21).** `RemoteClient`
  follows the leader hint and hops to the leader, so `digest` and every read go
  to the leader. Per-replica reads are a remote-protocol-v2 concern (an FSM
  selector, an advertised ceiling), which the charter rules out of scope. The
  KV store exposes the gap; it does not close it.
- **rustdoc `[source]` links reach crate source (L1)** — a boundary the
  clean-room treats as off-limits but a real user would not care about.
- **A wipe within the log buffer replays with no snapshot session (L16)** —
  correct behaviour, surprising in the moment, clarified in the reference.

## The verdict

UC is **buildable-on from the published docs alone** for a real linearizable
service, including the genuinely hard parts — deterministic apply, snapshots,
exactly-once. The friction was concentrated in two places: the **upgrade
story** (a rolling swap looks safe and is not) and the **absence of a shipped
snapshot example**. Both are now closed — the first by the flag-day how-to and
issue #33, the second by `examples/kv` itself. The gate's one red cell is a
platform defect the store survived, not a builder mistake. On the charter's bar
— docs sufficiency under the honest-failure protocol — the builder track
**passes**, with its ledger fully resolved (gate row B5-builder).
