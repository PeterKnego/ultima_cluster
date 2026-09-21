# Diff replay an FSM change

Replay the same input — a snapshot plus a log span — through the old and the
new build of your state machine, diff everything they did on the captured
surfaces, and confirm the differences are the ones you meant. The full model
is the spec
(`docs/superpowers/specs/2026-09-19-uc2-fsm-upgrade-lifecycle-design.md`, §4);
this page is the commands.

**What "everything" means here.** Five surfaces are captured and compared:
the response bytes at each position, the `svc_sched` records (timers
scheduled and cancelled) at each position, the state projection at the origin
and at the end, how many times each frame called `ctx.ids()`
(`ApplyCtx::ids_calls`), and — when the driver is given a `RawOutputHandler`
(`drive::DriveOptions`) — the `on_committed` result at each position. One
more from the spec's §4.2 list is **not** captured, so an empty diff is not
evidence about it: probe-query answers (the projection is the state view
instead). `uc_diffreplay/README.md` § "What this does not compare" is the
standing statement.

## 1. Make your service binary replayable

Add `replay` and `project` subcommands that call
`uc_diffreplay::drive::run_replay_cli` / `project_artifact` with the **same
wrapper stack** your live service uses (`Sessioned`, `Timed`, …) —
`examples/kv/src/bin/kv-service.rs` is the worked example. Implement
`SnapshotStateMachine::project()`: canonical text, sorted, one record per line.

## 2. Capture a corpus

    uc2ctl snapshot …                                  # a complete set at P
    <stop the node>                                    # optional — see below
    uc2-diffreplay corpus export --instance-dir /srv/uc2/n0 --app-id kv --row 0 \
        --from P --out ./corpus                        # or --around <pos> for a bug

Exporting from a **running** node is safe: `uc_node::backup`'s ordered copy
is the correctness argument, not quiescence. Stopping the node first is still
the simplest way to get a quiescent span, because nothing appends while you
pick the end Q.

## 3. Declare what you intend

`intent.toml` — which arms the change touched, what should differ:

    tag_offset = 16   # Sessioned apps: skip the client_id ‖ seq envelope

    [tags]        # first bytes of your command encoding → arm name
    "0101" = "put"
    "0102" = "delete"
    [timers]      # timer id (decimal) → arm name; a TIMER frame has no payload to tag
    "9" = "reaper"
    [touched]
    arms = ["put"]
    migration = true                                    # the image format changed
    [[expect]]
    surface = "projection_origin"                       # projections take NO arm
    note = "every entry gains ttl=0"
    [[expect]]
    surface = "response"
    arm = "put"
    note = "put acks now carry ttl"

A bare (non-`Sessioned`) app leaves `tag_offset` out — it defaults to 0.
An `[[expect]]` on `projection_origin` or `projection_end` must not carry an
`arm` (it is refused by name): a projection is one comparison over the whole
state, attributed to the touched set as a whole. Unknown keys are refused
too, so a typo cannot quietly read as "not declared".

## 4. Run

    uc2-diffreplay upgrade --corpus ./corpus --old ./kv-service-1.0 --new ./kv-service-1.1 \
        --declare intent.toml --report report.json

Exit 0 = every difference is attributed and declared, every declaration was
observed. Anything else exits 1 and the report names it: `Undeclared`,
`Unexplained`, or `Absent`.

Each side's raw trace also carries `artifact_version` — the version stamped
in the installed artifact's `ULTSNAP2` envelope (`null` when the run started
from genesis). The driver installs with no expected-version check of its own
(that would defeat the point of comparing across a version boundary), so
this field is how you confirm, after the fact, which version actually built
the artifact each side started from. `uc_diffreplay/README.md` § "The trace
an app binary writes" has the full field list.

## Also

    uc2-diffreplay determinism    --corpus C --bin ./kv-service --report det.json   # same build twice; must be empty
    uc2-diffreplay reconstruction --corpus C --bin ./kv-service --report rec.json   # artifact vs genesis origin (spec §2.3)

`examples/kv/tests/corpora/README.md` is the regression-corpus convention —
how a corpus becomes a `cargo test` that runs on every build.
