# uc_diffreplay — diff replay for UC state machines

Replay the same input (a snapshot + a log span) on different FSMs, then
compare the differences in their snapshots, outputs and logs.

    uc2-diffreplay corpus export --instance-dir D --app-id A --row 0 --around POS --out CORPUS
    uc2-diffreplay upgrade       --corpus CORPUS --old ./svc-v1 --new ./svc-v2 --declare intent.toml --report r.json
    uc2-diffreplay determinism   --corpus CORPUS --bin ./svc --report r.json
    uc2-diffreplay reconstruction --corpus CORPUS --bin ./svc --report r.json

An app binary takes part by embedding the driver behind a `replay`
subcommand — see `examples/kv/src/bin/kv-service.rs`. `uc_lincheck/src/bin/
register-replay.rs` (built behind the `uc_lincheck` `replay-bin` feature) is
the harness's own end-to-end fixture, over `RegisterSm`.

## CLI contract for app binaries

`uc2-diffreplay` shells out to the app's own binary rather than linking
against it, so the contract is two subcommands any binary embedding
[`uc_diffreplay::drive::run_replay_cli`] / [`uc_diffreplay::drive::project_artifact`]
must expose:

    <bin> replay --corpus DIR --out TRACE.json [--from-genesis]
    <bin> project --artifact FILE --position P

`replay` drives the SM over the corpus at `DIR` (installing its artifact at
the corpus's origin, unless `--from-genesis` replays from position 0 — the
§2.3 counterfactual) and writes the resulting [`uc_diffreplay::trace::Trace`]
to `--out` as JSON. `project` installs `--artifact` at `--position` and
prints the SM's canonical projection to stdout. Both exit non-zero on
failure; `uc2-diffreplay` treats a non-zero exit from `replay` as a hard
error (spawn/replay failure), not a divergence.

Spec: `docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md`.
