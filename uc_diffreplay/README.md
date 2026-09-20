# uc_diffreplay — diff replay for UC state machines

Replay the same input (a snapshot + a log span) on different FSMs, then
compare the differences in their snapshots, outputs and logs.

    uc2-diffreplay corpus export --instance-dir D --app-id A --row 0 --around POS --out CORPUS
    uc2-diffreplay upgrade       --corpus CORPUS --old ./svc-v1 --new ./svc-v2 --declare intent.toml --report r.json
    uc2-diffreplay determinism   --corpus CORPUS --bin ./svc --report r.json
    uc2-diffreplay reconstruction --corpus CORPUS --bin ./svc --report r.json

An app binary takes part by embedding the driver behind a `replay`
subcommand — see `examples/kv/src/bin/kv-service.rs`.

Spec: `docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md`.
