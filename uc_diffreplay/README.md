# uc_diffreplay — diff replay for UC state machines

Replay the same input (a snapshot + a log span) on different FSMs, then
compare everything they do on the captured surfaces (see
[What this does not compare](#what-this-does-not-compare)).

    uc2-diffreplay corpus export --instance-dir D --app-id A --row 0 --around POS --out CORPUS
    uc2-diffreplay upgrade       --corpus CORPUS --old ./svc-v1 --new ./svc-v2 --declare intent.toml --report r.json
    uc2-diffreplay determinism   --corpus CORPUS --bin ./svc --report r.json
    uc2-diffreplay reconstruction --corpus CORPUS --bin ./svc --report r.json
    uc2-diffreplay pin-verify    --corpus CORPUS --old ./svc-v1 --new ./svc-v2 \
        --app-id A --fsm NAME --to 1.1.0 --report r.json

An app binary takes part by embedding the driver behind a `replay`
subcommand — see `examples/kv/src/bin/kv-service.rs`. `uc_lincheck/src/bin/
register-replay.rs` (built behind the `uc_lincheck` `replay-bin` feature) is
the harness's own end-to-end fixture, over `RegisterSm`.

## CLI contract for app binaries

`uc2-diffreplay` shells out to the app's own binary rather than linking
against it, so the contract is three subcommands. Two of them any binary
embedding [`uc_diffreplay::drive::run_replay_cli`] /
[`uc_diffreplay::drive::project_artifact`] must expose:

    <bin> replay --corpus DIR --out TRACE.json [--from-genesis]
    <bin> project --artifact FILE --position P

`replay` drives the SM over the corpus at `DIR` (installing its artifact at
the corpus's origin, unless `--from-genesis` replays from position 0 — the
§2.3 counterfactual) and writes the resulting [`uc_diffreplay::trace::Trace`]
to `--out` as JSON. `project` installs `--artifact` at `--position` and
prints the SM's canonical projection to stdout. Both exit non-zero on
failure; `uc2-diffreplay` treats a non-zero exit from `replay` as a hard
error (spawn/replay failure), not a divergence.

The third is the **serve** form, which `pin-verify` runs and the service
binaries in the tree already have — `examples/kv`, `examples/counter` and
`register-replay serve` all meet it (`uc_crashtest-service` does not register
a SIGTERM handler, so a clean stop of it dies by signal rather than exiting
0; it is a crash-test half, not a template):

    <bin> <serve argv…> --instance-dir D --app-id A

It attaches to the node at `D` and applies until it is stopped. Two things
about it are contract rather than convention: a clean stop (SIGTERM) exits
**0**, and a **failed attach exits non-zero with the error on stderr** —
which is how `pin-verify`'s refusal arm sees a pin's
`ServiceError::PinnedVersionMismatch`. `<serve argv…>` is whatever the app
calls its serve verb plus its own knobs — and it may be **empty**: for
`kv-service` and `counter-service` serving is what the binary does with no
subcommand at all.

**One knob list drives all three forms.** `pin-verify` takes the serve argv
(`--old-arg serve --new-arg serve --new-arg --double`), and
[`uc_diffreplay::pinverify::app_knobs`] strips the leading verb so the
`replay` and `project` forms can put their own verb in front of the same
knobs. An app's knobs therefore have to ride **after** its verb — a first
argument beginning with `-` is treated as a knob, not a verb, and kept.

`uc2-diffreplay pin-verify` is the live half of the `reconstruction` mode
(spec §6.2 part 2): on a throwaway single-voter node, with the app's real
binaries, it places a real `uc2ctl upgrade pin` and checks that the stale
binary is refused by name and that the new one's live state is the ARTIFACT
path's rather than the genesis counterfactual's.
`docs/how-to/diff-replay.md` § "Verify the pin live" is the command, the
verdict table and how to give a span something the counterfactual can
disagree about.

## The declaration (`intent.toml`)

`upgrade` judges a profile against a declaration: `[tags]` maps the hex of a
command's leading bytes to an arm name (longest prefix wins), `[touched]
arms` names the arms the change is allowed to move, and `[[expect]]` records
what it should do to each surface.

`tag_offset` (default 0) is how many leading tag bytes are **framework
envelope** rather than application bytes, dropped before the `[tags]`
prefixes are matched. A service running `uc_service::Sessioned<S>` puts a
16-byte `client_id ‖ seq` envelope ahead of the app's own frame, so its
declaration needs `tag_offset = 16` — without it every tag begins with a
client id and no arm ever matches. `examples/kv/tests/corpora/put-then-delete/
intent.toml` is the worked example.

`[timers]` is `[tags]` for TIMER frames: a timer frame carries no application
payload, so there is nothing to tag and its arm comes from the timer id
instead (`[timers] "9" = "reaper"` — the id as a decimal string, since TOML
keys are strings). Without it a timer divergence is permanently
`Unexplained`.

An `[[expect]]` on `projection_origin` or `projection_end` takes **no**
`arm`, and is refused by name if it carries one: a projection is one
comparison over the whole state, attributed to the change's touched set as a
whole rather than to any single arm.

Unknown keys are refused — a declaration is a statement of intent, and a
typo in one must not read as "not declared".

Spec: `docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md`.

## What this does not compare

Spec §4.2 lists the surfaces an FSM is observable through and argues the list
is complete. The **driver captures five of them**: response bytes per
position, `svc_sched` records per position, the state projection at the
origin and at the end, `ApplyCtx::ids_calls` per position, and — when
`drive_with` is given a `RawOutputHandler` — the `on_committed` result per
position. Those five are compared, and a divergence on any of them is a
finding.

This one is **not captured**, so an empty diff says nothing about it:

- **Probe-query answers.** The projection is the state view instead: thorough
  and O(state), where the queries would have been cheap and partial.

Two notes on the two surfaces that ARE captured but conditionally:

- **`on_committed` emissions** are only recorded when the caller passes a
  handler to `drive_with` (`drive` itself passes none, via `DriveOptions::
  default()`) — `Entry.output` is `None`, not a passing comparison, when no
  handler ran on either side.
- **Ids the FSM mints** are captured as a per-frame *count*
  (`ApplyCtx::ids_calls`), not as the ids themselves — a build that mints a
  different number of ids for the same frame shows up as a `Surface::Ids`
  divergence even if every id it DID mint matches.

The report names the mode's own further caveats in its `notes` (for example
`reconstruction` does not compare the origin projection at all).

## The trace an app binary writes

JSON, `uc_diffreplay::trace::Trace`: `row`, `version`, `artifact_version`,
`origin`, `end`, `projection_at_origin`, `projection_at_end`, and
`entries[]` of
`{ pos, kind: "Message" | { "Timer": { id, deadline_ns, table } }, tag, response, sched[], ids_calls, output }`.
`artifact_version` is the version stamped in the installed artifact's
`ULTSNAP2` envelope — `null` when the run started from genesis (nothing
installed). It may disagree with `version` (this run's own `S::VERSION`):
that disagreement is exactly the case the pinned-install cross-check
(Tasks 3/4) exists for. The driver itself passes `None` as the expected
version when it installs, by design — its whole point is to install an
artifact across a version boundary and compare what each build's `apply`
does with it, not to enforce sameness — so it reports the envelope's version
rather than asserting it.
`tag` is the first 32 bytes of the command payload — an app-defined
discriminant, opaque to the harness; `tag_offset` in the declaration says
where the app's own bytes start. `output` is `null` unless the driver ran
with a `RawOutputHandler`. A non-Rust app produces the same JSON and takes
part in every mode.

## Running the tests

The e2e, reconstruction and pin-verify tests shell out to prebuilt binaries
and hard-assert they exist, so build them first:

    cargo build -p uc_lincheck --features replay-bin --bin register-replay
    cargo build -p uc_diffreplay

(the second one is what `examples/kv`'s `regression_corpora` test needs). Then

    cargo test -p uc_diffreplay -p uc_service -p uc_lincheck -p kv_store

`tests/pin_verify.rs` is the heaviest of them: every case runs a real node
and a real service process per era, and the cases that complete the swap arm
run the app binary three more times (one `project`, two `replay`), so it needs
the `register-replay` fixture built above and runs best on its own —

    cargo test -p uc_diffreplay --test pin_verify -- --test-threads=1
