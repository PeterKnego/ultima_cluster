# UC2 cluster view: the live operator / AI read surface (storage-service project 4)

Status: design, approved in brainstorming 2026-10-14. Implements project 4 of
the [snapshot catalog design](2026-10-01-uc2-snapshot-catalog-design.md)
(§6.3 "the live-read requirement", §12), widened by the maintainer to the
whole committed cluster FSM view.

## 1. Problem

The cluster now replicates a lot of storage state — the snapshot catalog
(sets, per-row verdicts, sizes), backup-tier watermarks, `retain_sets`,
`purge_requires_tiers` — and the leader holds a live per-node table of what
every node has on disk. None of it can be read live:

- `uc2ctl settings show` / `schedule show` / `upgrade show` read the newest **cluster artifact**, which exists only after the first
  instant and lags by one instant — "no cluster artifact yet" is a recorded
  gap since `2.11.0`.
- `/metrics` has five `uc2_catalog_*` gauges: counts and positions, never
  *which* set, *which* row, *which* node.
- `uc_node::cluster_agent::read_committed_catalog` reads the catalog offline,
  for a `uc2ctl catalog` that was never built.

The catalog is what an operator (or an AI agent acting for one) asks about
*before* and *during* an instant, so the artifact alone is not enough.

## 2. Goal and scope

**Outcome.** One read answers: which sets exist and whether they are agreed,
who holds them, where the floor is, what holds purge back, how far each tier
has backed up — plus the rest of the committed cluster state (membership,
settings, schedule, pins) and each row's attached service. It works before
the first instant and during one.

**Readers.** Both an operator/agent on a node host (`uc2ctl`) and a remote
reader over HTTP. One JSON producer behind both doors.

**In:**
- `GET /cluster` on the existing obs router, served over a new always-on
  unix socket `<instance_dir>/obs.sock` and over the `[metrics]` TCP
  listener when configured.
- `uc2ctl cluster [--json]` and `uc2ctl catalog [--json]`; `settings show`,
  `schedule show`, `upgrade show` switch to the live view, with an offline
  artifact fallback.
- A versioned JSON schema, `uc2.cluster-view/1`, with a reference page.
- `serde_json` as a workspace dependency.

**Out:**
- **Writes.** Every mutation stays on the HMAC-signed `cnc.dat` admin band,
  host-local. An authenticated remote write door (for a future admin GUI) is
  its own backlog item (§9).
- Forwarding a follower's request to the leader.
- Per-node obs addresses in replicated membership (a wire change). A GUI or
  agent gets its node URL list from its own config, as a Prometheus scrape
  config does.
- TLS, authentication and CORS on the TCP door (unchanged from `/metrics`).
- History. Time series stay in Prometheus; this is current state only.

**No flag day.** No node↔node wire change, no `cnc.dat` change, no on-disk
format change. Ships in `2.14.0`.

## 3. What each node answers

Every node answers `GET /cluster` with four parts.

### 3.1 Header

| field | meaning |
|---|---|
| `schema` | `"uc2.cluster-view/1"` |
| `source` | `"live"` (from the node) or `"artifact"` (uc2ctl's offline fallback, §6.3) |
| `node_id` | this node's id |
| `role` | `"leader"` \| `"follower"` \| `"learner"`, from the cnc node-flags word |
| `joined` | `false` until the node has joined (the attach gate's condition: leader known ∧ commit learned ∧ cluster walk consumed to commit) |
| `leader` | `{ "id", "addr" }` as this node knows it, or `null`; `addr` is the replication address, not an obs URL |
| `view_position` | the position the `cluster` part is applied through (`ClusterView::consumed`) |
| `commit` | this node's commit counter |
| `read_at` | RFC 3339 time the response was built |

`commit - view_position` is how far the cluster agent trails; staleness is
data in the payload, not a guess the reader has to make.

### 3.2 `cluster` — the committed cluster state

Built from `ClusterView::to_state()` (plus the settings atomics), so every
node applied through the same `view_position` renders identical content:

- `membership` — members (id, address, voter/learner), config version.
- `settings` — every replicated setting, including `fsm_lag`,
  `admission_bytes`, `snapshot_interval_bytes`, `snapshot_target`,
  `retain_sets`, `purge_requires_tiers`, `datagram_mtu`; plus
  `settings_position`.
- `schedule` — the table (entries with id, row name, rule) and its position.
- `upgrades` — per row: the committed running version, active pin (origin,
  version, sequence), and the snapshot-report history the readiness gate
  reads.
- `catalog` — see §4.
- `tiers` — one entry per backup tier: `id` (16 hex digits, as `uc2ctl tier
  show` prints it), `oldest_set`, `newest_set`, `through`, `reporter`,
  `updated_position`, and the derived `lag_bytes` (`commit - through`).
- `purge_gate` — `requires_tiers` (K), `position` (gate(K) from
  `tier::gate_position`, or `null` when K = 0 or fewer than K tiers report),
  `released` (this node's low-space release level).

### 3.3 `self` — this node, on every node

- `holdings` — the node's own `Holdings` cache (the same struct it sends in
  `STATUS`): `journal_first`, `durable`, `commit`, `applied` per row,
  `free_bytes`, `journal_bytes`, `snapshots_bytes`, and `sets_held` rendered
  as a list of set positions (not a bitmask).
- `services` — one entry per declared row: `row`, `name`, `attached`
  (`bool`, from the slot's liveness), `version` (the attached service's
  packed version, or `null`), `applied`, `lag_bytes` (`commit - applied`),
  `timers_pending`, `pinned` (`bool`) and `pinned_version`.

### 3.4 `nodes` — every member, on the leader only

- Leader: one entry per committed member from `soft_table()` — `id`, `addr`,
  `role`, `holdings` (as §3.3), `age_ms` since the leader last heard its
  `STATUS`, and `stale: true` past the soft table's staleness bar
  (`soft_stale_ns`). The leader's own entry is included.
- Follower or learner: `nodes: null`, `nodes_from: "leader"`.
- A demoted node's in-memory soft table is never printed: `nodes` is filled
  only while the node-flags word says leader.

## 4. The catalog part

`cluster.catalog`:

- `retain_sets`, `empty` (no set has ever completed), `floor` (newest agreed
  position, `null` when none).
- `sets` — oldest first, one per `SetEntry`: `position`, `kind`
  (`"full"`/`"standby"`), `commanded_at` (RFC 3339), `state`
  (`"commanded"`/`"complete"`), `agreed`, `stalled`, `diverged_rows` (row
  names), `size` (`total_size()`, `null` when unknown), `rows` (one per
  declared row: `row`, `name`, `version`, `hash`, `verdict`, `size`) and
  `cluster` (the cluster artifact's entry, same shape).
- Leader only, per set: `held_by` — node ids whose `sets_held` includes it.
  On a follower the field is `null`.

Every derived field is computed by the same query the node itself acts on
(`CatalogQuery::newest_agreed`, `agreed_for`, `stalled`, `diverged`), never
re-derived in the view.

## 5. The JSON conventions

- **Numbers that survive every parser.** Hashes are hex strings
  (`"0x" + 16 digits`). Times are RFC 3339 UTC strings with nanoseconds,
  formatted without a new date dependency. Durations are integers with the
  unit in the name (`age_ms`, `stale_after_ms`). Positions and byte counts
  are JSON numbers — exact in every parser up to 2⁵³ (9 PiB of log); the
  reference page states the bound.
- **Words, not codes.** Enums are lowercase strings: `agreed`, `diverged`,
  `no_majority`, `unreported`, `commanded`, `complete`, `full`, `standby`,
  `leader`, `follower`, `learner`, `voter`. Rows carry both `name` and `row`.
- **`null` for absent**, never a sentinel `0`.
- **`attention`**, top level: a list of `{ "code", ...context }` for
  conditions the node already detects. No new diagnosis logic:

  | code | condition | same source as |
  |---|---|---|
  | `catalog_empty` | no set ever completed | `uc2_catalog_empty` |
  | `set_stalled` | a set in `stalled()` (context: `set`) | `Uc2SnapshotStalled` / `uc2_catalog_stalled` |
  | `set_diverged` | a `(set, row)` in `diverged()` (context: `set`, `row`) | `Uc2SnapshotSetDiverged` |
  | `purge_gate_released` | this node's gate release level is set | `Uc2BackupGateReleased` |
  | `tier_behind` | a tier's `commit - through` > 256 MiB (context: `tier`, `lag_bytes`) | `Uc2BackupBehind`'s threshold (the alert adds `for: 15m`) |
  | `node_stale` | leader only: a member's soft entry is stale (context: `node`) | the soft table's staleness bar |

  Each code is listed in the reference page with its meaning and the
  runbook section to read. A new code is an additive change (§5, stability).
- **Stability.** The serialized `#[derive(Serialize)]` structs in one
  `uc_node::view` module ARE the schema. Adding a field or an `attention`
  code keeps `/1`; renaming, removing or changing a field's meaning bumps to
  `/2`. A checked-in example response and a golden test fail on any
  accidental change.

## 6. Doors and lifecycle

### 6.1 The obs thread

- The `uc2-obs` accept loop polls **both** listeners non-blocking on its
  existing 100 ms cadence: the unix socket always, TCP when `[metrics]` is
  configured. It is started by the `uc2-node` daemon, as the TCP server is
  today; the daemon now always starts it.
- The existing `REQUEST_CAP` (4 KiB), per-read timeout and `CONN_DEADLINE`
  apply to both doors.
- The view is rendered on the obs thread from `Arc`s and the cluster view's
  lock, as `/metrics` is: no hot-path agent does any work for it.
- An embedding application (the `Node` library without the daemon) gets the
  render function, `uc_node::view::render(&ObsSources) -> String`, and
  serves it however it likes.

### 6.2 The socket file

- `<instance_dir>/obs.sock`, mode `0600`. At boot the daemon unlinks any
  stale file and binds — safe because it already holds `instance.lock`, so no
  other live node owns the directory. Unlinked on clean shutdown.
- **Long paths.** `sun_path` holds 107 bytes; an instance dir can be longer.
  The node is Linux-only, so when the direct path does not fit, both the
  daemon (bind) and `uc2ctl` (connect) go through
  `/proc/self/fd/<dirfd>/obs.sock` on an open handle to the instance dir. No
  refusal, no knob.
- **No knob to disable it.** Read-only, local, permission-guarded; adding a
  knob later is cheap, removing one is not.
- `uc2ctl backup`/`verify-backup`/`restore` copy only the journal, `state/`
  and `snapshots/` trees, so `obs.sock` is never copied; a test pins that.

### 6.3 `uc2ctl`

- `uc2ctl cluster [--json] --instance-dir D` — the whole view.
- `uc2ctl catalog [--json] --instance-dir D` — the storage part: catalog,
  tiers, purge gate, `self.holdings`, `nodes`, and storage `attention`
  codes. It filters the same `/cluster` response client-side.
- `settings show`, `schedule show`, `upgrade show` read the live view and
  keep their current text output.
- `--json` prints the node's response verbatim (the catalog's is the
  filtered sub-object under the same `schema`), so the CLI and the HTTP door
  never disagree.
- **Offline fallback.** When the instance lock shows no live node
  (`probe_instance_lock`, #35), `uc2ctl` builds the same view from the newest
  cluster artifact through the same `uc_node::view` builder: `source:
  "artifact"`, `view_position` = the artifact's position, `self`, `nodes`
  and `services` `null`, header `role`/`joined`/`leader` `null`, `attention`
  limited to what the artifact alone shows. Text output says `offline:
  newest artifact at P`. With neither a live node nor an artifact, the
  existing "no cluster artifact yet" message stands.
- A live lock with a socket that refuses (a node still booting, before the
  daemon binds) is reported by name — `node starting, obs socket not yet
  bound` — not as offline.

## 7. Security

A new route on an already unauthenticated listener (`docs/security/
attack-surface.md`, `threat-model.md` get the entry):

- **Exposed:** member addresses, settings, the schedule table, pins, artifact
  hashes, tier ids and positions, per-node disk figures, attached service
  versions. The same class `/metrics` already exposes, in more detail.
- **Never exposed:** the HMAC admin key, crypto keys or group key, any
  command payload. A test renders a view from a node configured with both
  and asserts no key byte string appears.
- **Local socket:** trust is host access + file mode, the model `cnc.dat` and
  `uc2ctl` already use.
- **Bounded output:** sets ≤ `retain_sets` + commanded, rows ≤ 8, members
  ≤ 8, tiers ≤ `MAX_BACKUP_TIERS` (4). The reference page states the
  worst-case size; a test renders it.
- **Residual:** anyone who can reach the TCP obs port can map the cluster's
  storage state. Guidance unchanged: keep `[metrics]` on the ops network.
- The `uc_node_http` fuzz target already drives the router; it gains a
  `/cluster` seed.

## 8. Testing

- **Schema:** golden example response; hashes as hex strings; times as
  RFC 3339; `null` never `0` for absent; a position near 2⁵³ serializes
  exactly.
- **Roles:** leader fills `nodes` and marks a silent member `stale`; follower
  answers `nodes: null`, `nodes_from: "leader"`, `held_by: null`; a demoted
  leader never prints its old soft table; a node not yet joined answers
  `joined: false` rather than refusing.
- **End to end (3 daemons, read through the socket):** before the first
  instant — genesis settings shown, `attention` has `catalog_empty`; after an
  instant — the set `agreed` with `held_by` naming its holders; a diverged
  set (reusing the catalog suite's scenario) — `set_diverged` naming the row;
  a registered tier — watermark, `lag_bytes` and `purge_gate` populated.
- **Socket:** an instance dir over 107 bytes works through `/proc/self/fd`;
  a SIGKILLed node's leftover `obs.sock` reads as "node not running"; a
  booting node reads as "node starting".
- **Offline:** `uc2ctl settings show` on a stopped node prints the artifact
  view with its marker; `uc2ctl catalog --json` offline has `source:
  "artifact"`.
- **Hygiene:** no key material in rendered JSON; backup skips `obs.sock`;
  worst-case output size; fuzz seed.

## 9. Docs and backlog

- New reference page `docs/reference/cluster-view.md`: every field, the
  number/time conventions, the size bound, each `attention` code linked to
  its runbook section, the `/1` stability rules, and the note that a GUI or
  agent supplies its own node URL list.
- `docs/reference/uc2ctl.md`: `cluster`, `catalog`, the three `show`
  commands and the offline fallback.
- `docs/ops/uc2-runbook.md`: "reading the catalog" replaces "today only the
  five gauges"; the obs socket in the instance-dir layout.
- `docs/security/attack-surface.md`, `threat-model.md`: §7.
- `docs/BACKLOG.md`: project 4 done; new item **"Admin writes over HTTP (for
  a GUI)"** with its open questions — authenticating a remote HMAC request,
  replay protection, TLS, CORS versus a GUI backend that polls the nodes.
- Release (`2.14.0`): one `RELEASES.md` bullet linking the reference page;
  the `docs/releases.md` entry notes the always-on obs thread and the new
  instance-dir file.

#### Errata (planning, 2026-10-14)

Planning rulings (the plan's PR1–PR13) and execution rulings that amend the
text above.

- **PR1** — the soft table reaches the obs thread through two new `ObsSources`
  fields, `soft_wire: Arc<Mutex<uc_net::SoftTableWire>>` and `soft_stale_ns:
  u64`; `soft_table_from_wire` and `addr_of` in `node.rs` become
  `pub(crate)`; every struct-literal `ObsSources { .. }` site is updated.
- **PR2** — `stalled` on a set means "commanded and superseded": `state ==
  Commanded` and a NEWER entry exists. That is the condition
  `Uc2SnapshotStalled` keys on; `CatalogQuery::stalled(timeout)` has no
  production caller and no timeout constant exists, so the view invents none.
  The newest commanded set is in progress, not stalled.
- **PR3** — `tier_behind` uses a new Rust constant `TIER_BEHIND_BYTES = 256
  MiB`, pinned to the alert file's `uc2_backup_lag_bytes > 268435456` by a
  test. Tier `lag_bytes` is `commit - through` against THIS node's commit, on
  every node (the metric stays leader-only; the header's `commit` says whose
  commit was used); `null` offline.
- **PR4** — schedule entries name their row by matching `identity_hash`
  against the row names (live: `cnc.service_names()`; offline: a readable
  stale `cnc2.dat`), `null` when no name matches.
- **PR5** — `view_position` is `ClusterView::consumed`, read separately from
  `to_state()` (whose `applied` is the view tag `position`). Offline: the
  artifact's position.
- **PR6** — `purge_gate.position` is `null` when K = 0; otherwise
  `tier::gate_position(k, tiers)` verbatim, which is `0` ("holds every
  purge") when fewer than K tiers report. A `tiers_reporting` count is added
  so `0` is self-explaining.
- **PR7** — `joined` mirrors `uc_service::attach::node_booting`: `joined =
  !(services_declared == 0 && any row name is Some)`.
- **PR8** — `uc2ctl cluster`/`catalog` take the existing `CommonArgs`, so
  `--app-id` is required as for every other subcommand; it names the
  `cnc2.dat` used for liveness and offline row names.
- **PR9** — the show commands render through the view: `settings show`,
  `schedule show` and `upgrade show` fetch the document (live, else offline),
  convert its sub-objects back to the domain types with
  `uc_node::view::convert`, and print through their EXISTING renderers, so
  the text is identical by construction. The model is therefore lossless for
  those three sub-objects (packed versions, exact ns as strings).
- **PR10** — when the live read is unavailable but the instance lock is held,
  `cluster`/`catalog` print `node running but its obs socket is not bound
  (starting, or not run by uc2-node)` (spec §6.3 said `node starting, obs
  socket not yet bound`) and exit non-zero; the three show commands fall back
  to the offline document silently.
- **PR11** — end-to-end content tests run in-process `Node`s (the
  `uc_node/tests/catalog.rs` harness) and call
  `view::render_live(&node.observability())`; the socket is tested against
  one real daemon (`lifecycle.rs`) and in `uc_ctl` tests that bind an
  `ObsServer` themselves. No 3-daemon harness is built. The registered-tier
  end-to-end test (spec §8) likewise runs in-process via
  `Node::offer_backup_claim_for_test`, not through daemons.
- **PR12** — RFC 3339 rendering is a new public `uc_obs::time::rfc3339_ns`
  (`uc_obs` is the dependency-free leaf both daemons share).
  `uc_obs::time::civil_from_days` is public too and shared with `uc_ctl`.
  `uc_ctl`'s private second-precision helper stays as is.
- **PR13** — offline row names come from a stale `cnc2.dat` when it opens;
  otherwise names are `null`. Offline membership comes from the artifact.
- **P7** — `settings.admission_bytes`, `snapshot_interval_bytes`,
  `backup_retain_age_secs` and `fsm_lag.bytes` are decimal strings
  (`DecU64`), because the admin door accepts values up to `u64::MAX - 1` and
  no JSON number may exceed 2^53.
- **P8** — the leader's `nodes` lists every committed member; one it has
  never heard from has `holdings: null`, `age_ms: null`, `stale: true`.
- **P13** — the three show commands print `offline: newest artifact at P` on
  STDERR when they used the offline document; stdout is unchanged.
- **P14** — `set_stalled` (commanded and superseded by a newer entry, PR2)
  differs from the `uc2_catalog_stalled` gauge, which counts every commanded
  entry including the in-progress newest one.
- **P15** — the embedder entry point is `uc_node::view::render_live(&ObsSources)`
  (§6.1 said `render`).
- **P16** — `cluster --json` prints the node's body verbatim (§6.3 holds);
  `catalog --json` filters the parsed JSON value, so unknown fields survive,
  and prints the storage sub-object under the same `schema`.
- **P19** — `self.services` is empty until the node has joined.
- **P28** — the offline document is cut from the newest artifact at P, which
  records its own set P as `commanded` (P's completion commits after P). So
  offline `empty`, `floor` and the newest set's `state` trail by one set; the
  offline document never raises `catalog_empty`; offline text marks that set
  "(cut at this artifact; its completion is not visible offline)".
- Release entries (`RELEASES.md`, `docs/releases.md`, §9's last bullet) are
  written at the `2.14.0` cut, not on this branch.

#### Errata (as built)

(added by the final review)
