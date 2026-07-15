# elle-cli (vendored)

Standalone CLI for Jepsen's Elle transactional-safety checker.

- Upstream: https://github.com/ligurio/elle-cli
- Version: 0.1.9 (release asset `elle-cli-bin-0.1.9.zip`)
- sha256(elle-cli-0.1.9-standalone.jar): `c9ba9b9fd32640e73d632cb5f15069c162ba6528a67f27a878767187c59f539a`
  (matches the pin recorded in `ultima_db`'s `tools/elle-cli/README.md` — this
  jar is a straight copy from the sibling `../ultima_db` checkout, not a
  re-download)
- License: Eclipse Public License 2.0 (see `LICENSE`, copied from upstream)
- Requires: Java 11+ (this box: Temurin 21)
- Used by: `scripts/elle_check.sh` and `scripts/elle_mutation.sh` (the UC
  elle consistency harness)

Invocation shape the pipeline relies on:

```
java -jar elle-cli-0.1.9-standalone.jar --model list-append \
    --consistency-models <model> <history.edn>
```

stdout: `<file>\t<true|false|unknown>`. **The process exit code is untrusted**
— elle-cli's exit status does not reliably track the verdict, so callers must
parse the stdout verdict line, never `$?`.

## Strict model name

UC's linearizable-read barrier (see `docs/superpowers/specs/2026-07-09-uc-v2-aeron-shaped-smr-design.md`)
claims real-time (strict) consistency, not just serializability, so the
harness needs a consistency-model name that actually rejects real-time-order
violations. Probed on this vendored 0.1.9 jar (2026-07-15):

```
$ java -jar elle-cli-0.1.9-standalone.jar --model list-append \
    --consistency-models strong-serializable fixtures/realtime_violation.edn
fixtures/realtime_violation.edn 	 false

$ java -jar elle-cli-0.1.9-standalone.jar --model list-append \
    --consistency-models strict-serializable fixtures/realtime_violation.edn
fixtures/realtime_violation.edn 	 false

$ java -jar elle-cli-0.1.9-standalone.jar --model list-append \
    --consistency-models serializable fixtures/realtime_violation.edn
fixtures/realtime_violation.edn 	 true
```

Both `strong-serializable` and `strict-serializable` reject the fixture
(`false`) while plain `serializable` accepts it (`true`), so this jar does
support strict-model checking. Per the decision rule (try `strong-serializable`
first), the pinned model is:

**`STRICT_MODEL = strong-serializable`**

This is the default consistency model the harness scripts (`scripts/elle_check.sh`,
`scripts/elle_mutation.sh`, Tasks 6/11) pass via `--consistency-models`.

## Fixtures

`fixtures/known_bad.edn` — a hand-written list-append write-skew history (two
transactions that each read a key the other appends to — a pure
rw-antidependency cycle). It is invalid under `serializable` but valid under
`snapshot-isolation`; the dependency-cycle self-test. Confirmed:

```
$ java -jar elle-cli-0.1.9-standalone.jar --model list-append \
    --consistency-models serializable fixtures/known_bad.edn
fixtures/known_bad.edn 	 false
```

`fixtures/realtime_violation.edn` — two singleton transactions: an `:append`
that completes (`:ok`) at `:time 5`, then an `:r` (read) that only *invokes*
at `:time 10` — strictly after the append's completion — yet observes the
pre-append state (`[]` instead of `[10]`). This is legal under plain
`serializable` (the read is allowed to serialize before the append in
process-free serialization order) but illegal under any strict/real-time
model, which must respect the observed real-time order between non-overlapping
transactions. The strict-model self-test: `serializable` must accept it
(`true`) and `STRICT_MODEL` must reject it (`false`) — see probe output above.

Both scripts require elle-cli to reject `known_bad.edn` under `serializable`
and reject `realtime_violation.edn` under `STRICT_MODEL` before trusting any
real verdict from the harness.
