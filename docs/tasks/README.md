# docs/tasks — v1-era historical record

**Everything in this directory describes the retired v1 stack** (the
`openraft`-based `uc_node`/`uc_service`/`uc_client` design), whose code was
deleted from the workspace on 2026-07-13 (`main` d268aab). These taskNN docs are
the *consolidated permanent record* of that era — each was written to stand on
its own — and are retained as history: the performance investigations and
negative results in particular (SyncCore Model-B, busy-spin runtimes, the
threading/copying and floor-decomposition analyses) explain *why* v2 is shaped
the way it is.

Do **not** read these as current architecture. The current stack is **UC v2**:

- Canonical spec: `../superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md`
- Per-milestone record: `../benchmarks/uc2-m{1..6}-gate-*.md` (+ the retained
  uc2 plans under `../superpowers/plans/`)
- Ops: `../ops/uc2-runbook.md`

The raw v1 design scaffolding (superpowers specs/plans) was removed from the
tree in the same cleanup; recover any of it from git history if ever needed
(`git log --diff-filter=D -- docs/superpowers`).
