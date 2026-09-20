# Diff replay an FSM change

Replay the same input — a snapshot plus a log span — through the old and the
new build of your state machine, diff everything they did, and confirm the
differences are the ones you meant. The full model is the spec
(`docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md`, §4);
this page is the commands.

## 1. Make your service binary replayable

Add `replay` and `project` subcommands that call
`uc_diffreplay::drive::run_replay_cli` / `project_artifact` with the **same
wrapper stack** your live service uses (`Sessioned`, `Timed`, …) —
`examples/kv/src/bin/kv-service.rs` is the worked example. Implement
`SnapshotStateMachine::project()`: canonical text, sorted, one record per line.

## 2. Capture a corpus

    uc2ctl snapshot …                                  # a complete set at P
    <stop the node>
    uc2-diffreplay corpus export --instance-dir /srv/uc2/n0 --app-id kv --row 0 \
        --from P --out ./corpus                        # or --around <pos> for a bug

## 3. Declare what you intend

`intent.toml` — which arms the change touched, what should differ:

    tag_offset = 16   # Sessioned apps: skip the client_id ‖ seq envelope

    [tags]        # first bytes of your command encoding → arm name
    "0101" = "put"
    "0102" = "delete"
    [touched]
    arms = ["put"]
    migration = true                                    # the image format changed
    [[expect]]
    surface = "projection_origin"
    note = "every entry gains ttl=0"
    [[expect]]
    surface = "response"
    arm = "put"
    note = "put acks now carry ttl"

A bare (non-`Sessioned`) app leaves `tag_offset` out — it defaults to 0.

## 4. Run

    uc2-diffreplay upgrade --corpus ./corpus --old ./kv-service-1.0 --new ./kv-service-1.1 \
        --declare intent.toml --report report.json

Exit 0 = every difference is attributed and declared, every declaration was
observed. Anything else exits 1 and the report names it: `Undeclared`,
`Unexplained`, or `Absent`.

## Also

    uc2-diffreplay determinism    --corpus C --bin ./kv-service   # same build twice; must be empty
    uc2-diffreplay reconstruction --corpus C --bin ./kv-service   # artifact vs genesis origin (spec §2.3)

`examples/kv/tests/corpora/README.md` is the regression-corpus convention —
how a corpus becomes a `cargo test` that runs on every build.
