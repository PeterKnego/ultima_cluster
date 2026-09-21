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

## 5. Verify the pin live (reconstruction mode part 2)

`reconstruction` above is part 1 of the spec's §6.2 two-part job: it
*demonstrates* that the artifact path and the genesis counterfactual can be
told apart. Part 2 is verifying that the running system **refuses the wrong
one** — that a committed `uc2ctl upgrade pin` turns the stale binary away by
name and steers the new one onto the artifact path. That needs a real node
and your real binaries, which is what `pin-verify` runs:

    uc2-diffreplay pin-verify --corpus ./corpus \
        --old ./kv-service-v1 --new ./kv-service-v2 \
        --app-id kv --fsm kv --to 1.1.0 --report pin.json

The harness's own fixture is the same shape with the register binary on both
sides, the "new build" selected by a knob:

    uc2-diffreplay pin-verify --corpus ./corpus \
        --old ./register-replay --old-arg serve \
        --new ./register-replay --new-arg serve --new-arg --double \
        --app-id pv --fsm register --to 2 --report pin.json

`--old-arg` / `--new-arg` carry the app's **serve** argv — its serve verb
followed by its own knobs, repeated once per token. The `replay` and
`project` forms reuse the knobs behind their own verb, so one `--new-arg`
list drives all three forms.

**The inputs that are not obvious.**

- **`--to` is an input, not something the harness reads off your binary.**
  Reading NEW's `VERSION` would mean attaching NEW, and attaching NEW before
  the pin is exactly the accident the pin prevents. Give the version the pin
  will name, in either spelling `uc2ctl upgrade pin --to` takes:
  `MAJOR.MINOR.PATCH`, or a raw packed `u32` (decimal or `0x…`) for a state
  machine whose `const VERSION` is a bare integer. Packed `0` is the
  "unversioned" sentinel and is refused. After the swap the harness compares
  `--to` with the version the row actually attached at, and a mismatch is a
  FAIL naming both.
- **`--fsm` is the row's FSM name**, as `node.toml`'s `[services] names`
  declares it — the scratch node is configured with that one row.
- **`--split` is how many of the corpus's `MESSAGE` frames are re-submitted
  before the harness takes its instant at P** (default: half). Everything
  after the split lands above P, which is the part the counterfactual can
  disagree about.

**The sequence**, all of it on a throwaway single-voter node in the scratch
directory. The node starts and OLD attaches; the corpus's recorded `MESSAGE`
frames are re-submitted verbatim through the raw client engine (`TIMER`
frames are skipped and counted — a node mints those); at `--split` the
harness commands a coordinated instant **P**; the rest of the frames follow;
OLD is stopped at the frontier **X > P**; a real `uc2ctl upgrade pin --from
<the version OLD was attached at> --to <--to> --origin P` is placed through
the admin band and waited for at the row's own cnc words; then the two arms
— OLD is started again and must be **refused** (a non-zero exit whose stderr
carries the SDK's `is pinned to version`), and NEW is started and must
attach at `--to` and catch up to X. A second instant **Q** turns NEW's live
state into an artifact anyone can project.

**The three projections and the verdict.** The harness then asks NEW for
three views of the same span: its **live** state at Q (`NEW project` on the
artifact at Q), the **artifact path** (`NEW replay` over a corpus exported
from `[P, Q)`), and the **genesis path** (`NEW replay --from-genesis` over
the same corpus — only claimed when the exported journal says it starts at
position 0).

| verdict | when | exit |
|---|---|---|
| **PASS** | the refusal arm held, `live == artifact`, and `artifact != genesis` | 0 |
| **INCONCLUSIVE** | the refusal arm held and `live == artifact`, but `artifact == genesis` | 0 |
| **FAIL** | anything else, including a comparison that never happened | 1 |

INCONCLUSIVE is an honest outcome, not a failure: everything held, but the
span could not tell the two paths apart, so the run shows nothing about the
counterfactual. That is what a **state-dependent tail** is for. A
last-write-wins state machine replayed over writes alone ends in the same
place either way; end the span with commands whose result depends on the
state at P (the fixture uses a CAS chain) and the two paths diverge. Note
also that `corpus export` at P keeps only frames at or above P: the commands
you want the demonstration to run must be **inside** the exported span, with
`--split` deciding which of them land before the instant.

A run whose OLD is already at `--to` is **refused up front**, before any
command is submitted and before any pin is placed: a pin naming the running
version cannot hold the refusal arm, because the "stale" binary is the
pinned one.

**Reading the report.** `pin.json` (and the same thing as text on stdout)
names each phase: `origin` is P, `frontier` is **X — the position OLD had
applied to when it was stopped**, which is what makes a *durable* state
machine attach above the origin and therefore what makes the pinned
install's rewind to P observable at all; `end` is Q. `refusal.matched` is
the arm that matters most — an exit alone could be any startup failure, so
the arm holds only when the stale binary also said why.

Scratch (the throwaway instance dir, both stderr files, the exported corpus
and the traces) lands in `<report>.pinverify/`. It is **kept on a FAIL** and
swept on a PASS or an INCONCLUSIVE — so a failing run's evidence sits beside
the report that names it. `--scratch DIR` puts it where you say and never
removes it.

## Also

    uc2-diffreplay determinism    --corpus C --bin ./kv-service --report det.json   # same build twice; must be empty
    uc2-diffreplay reconstruction --corpus C --bin ./kv-service --report rec.json   # artifact vs genesis origin (spec §2.3) — part 1

`examples/kv/tests/corpora/README.md` is the regression-corpus convention —
how a corpus becomes a `cargo test` that runs on every build.
