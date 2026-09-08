#!/usr/bin/env python3
"""Leaf helpers for the time-and-timers fleet gate rows (a/b/c/e/g/h of
`docs/benchmarks/uc2-time-and-timers-gate-2026-09-03.md`).

This module is deliberately DEPENDENCY-FREE — stdlib only, no `m6`/`m12`/`m14`
import — so every function in it is a pure transformation of text or numbers
and `--selftest` can pin all of them with literal inputs and no fleet. The
fleet arms and the row verdicts that print `GATE-JSON` live in
`m14_fleet_gate.py` beside their M14 siblings; this file holds only what they
stand on:

  * a tolerant Prometheus text-exposition parser (`parse_prom`) plus lookup
    helpers, because every new row reads its evidence off `/metrics`;
  * histogram helpers (`hist_series`, `hist_quantile`, `hist_mean`) for the
    two NEW families the Rust side adds for row c — `uc2_timer_lateness_ns`
    (per row) and `uc2_consensus_pass_ns` (node-level);
  * `longest_stall`, row h's commit-gap measure over the client's per-second
    `TL` buckets;
  * `schedule_toml`, row e's 32-entry table;
  * the A/B arithmetic rows a/b/e are judged by (`ab_stats`, `ab_reading`),
    which mirrors `scripts/apply_ab.sh`'s rule so the two harnesses cannot
    drift: a run whose arms are noisier than the resolution they are being
    judged against resolves nothing and says so.

Run `python3 tt_fleet_gate.py --selftest` for this module alone;
`m14_fleet_gate.py --selftest` runs these cases too, as part of its own.
"""

import math
import statistics
import sys

# --------------------------------------------------------------- bars/knobs
# Pre-committed in the gate doc's bar table; never edited to fit a run.
BAR_TT_G_CONVERGE_SECS = 60.0   # row g: below-floor join with the shipper restarted
ROW_C_MIN_FIRES = 10000         # row c: "the distribution over >= 10 000 on-time fires"
ROW_C_BAR_MULTIPLE = 2.0        # row c: p99 <= 2 x the measured consensus-pass length

# Row h: the deliberately large FSM state (256 MiB) that makes the freeze long
# enough to see against a live commit stream.
ROW_H_STATE_BYTES = 268435456
# Row h's commit-stall measure: a 1 s bucket at or below this fraction of the
# arm's own baseline rate counts as stalled.
STALL_FRACTION = 0.10

# Row e: MAX_SCHEDULE_ENTRIES `every 100ms` rules on ONE declared row, ids from
# a reserved high band (the how-to's advice), anchored at a fixed instant so
# the table is byte-identical from run to run.
MAX_SCHEDULE_ENTRIES = 32
SCHEDULE_ID_BASE = 5000
SCHEDULE_EVERY = "100ms"
SCHEDULE_ANCHOR = "2026-01-01T00:00:00Z"
SCHEDULE_CONVERGE_SECS = 30.0   # every voter's uc2_schedule_table_position must agree by then

# Row h: how long a commanded instant has to complete cluster-wide.
INSTANT_WAIT_SECS = 120.0

# The default `/metrics` port every node the driver starts listens on.
METRICS_PORT_DEFAULT = 9310
SCRAPE_TIMEOUT_SECS = 5

# The `le` ladder both NEW histogram families are exported with — a mirror of
# `uc_node::timers::NS_BUCKETS` (100 ns .. 100 ms, then `+Inf`), whose floor
# dropped from 10 us to 100 ns on 2026-09-07 because an idle leader's pass is
# ~300 ns and row c's quantiles have to be able to see it.
#
# Nothing here READS this constant — `hist_series` takes the bounds off the
# scrape's own `le` labels, so a further change on the Rust side cannot break
# the parser. It is recorded so a report can say which ladder a quantile was
# read off: `hist_quantile` returns a bucket UPPER BOUND, so the ladder IS the
# resolution of the answer, and a p99 quoted against the old floor would have
# read `<= 10 us` for every pass on an idle rig.
LATENESS_BUCKETS = (
    100.0, 200.0, 500.0, 1_000.0, 2_000.0, 5_000.0,
    10_000.0, 20_000.0, 50_000.0, 100_000.0, 200_000.0, 500_000.0,
    1_000_000.0, 2_000_000.0, 5_000_000.0, 10_000_000.0, 20_000_000.0,
    50_000_000.0, 100_000_000.0, float("inf"),
)


# ------------------------------------------------- Prometheus text parsing
def _split_labels(body):
    """Split a label body (`a="1",b="x,y"`) on commas OUTSIDE quotes."""
    parts, buf, in_quotes, escaped = [], [], False, False
    for ch in body:
        if escaped:
            buf.append(ch)
            escaped = False
            continue
        if ch == "\\" and in_quotes:
            buf.append(ch)
            escaped = True
            continue
        if ch == '"':
            in_quotes = not in_quotes
            buf.append(ch)
            continue
        if ch == "," and not in_quotes:
            parts.append("".join(buf))
            buf = []
            continue
        buf.append(ch)
    parts.append("".join(buf))
    return [p.strip() for p in parts if p.strip()]


def _unquote(value):
    v = value.strip()
    if len(v) >= 2 and v[0] == '"' and v[-1] == '"':
        v = v[1:-1]
    return v.replace('\\"', '"').replace("\\n", "\n").replace("\\\\", "\\")


def parse_prom(text):
    """Prometheus text exposition -> `{(name, frozenset(labels.items())): float}`.

    Tolerant on purpose: `# HELP`/`# TYPE` lines, blank lines, an optional
    trailing timestamp and an unparsable value are all skipped rather than
    raised on, because this parser reads a LIVE endpoint mid-run and a gate
    row must not die on one malformed line it does not read. `NaN`/`+Inf`
    parse as the float they name.

    The key's label set is a `frozenset` of `(name, value)` string pairs, so
    two samples of the same family are distinct keys iff their labels differ,
    whatever order the exporter wrote them in.
    """
    out = {}
    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "{" in line:
            name, _, rest = line.partition("{")
            body, sep, tail = rest.rpartition("}")
            if not sep:
                continue
            labels = {}
            bad = False
            for part in _split_labels(body):
                k, eq, v = part.partition("=")
                if not eq:
                    bad = True
                    break
                labels[k.strip()] = _unquote(v)
            if bad:
                continue
            value_tok = tail.split()
        else:
            name, _, tail = line.partition(" ")
            labels = {}
            value_tok = tail.split()
        if not value_tok:
            continue
        try:
            value = float(value_tok[0])
        except ValueError:
            continue
        out[(name.strip(), frozenset(labels.items()))] = value
    return out


def prom_find(metrics, name, **labels):
    """Every `(labels_dict, value)` of family `name` whose labels are a
    SUPERSET of `labels` — so `prom_find(m, "uc2_timers_late_total")` returns
    one entry per row and `prom_find(m, ..., row="0")` narrows to one."""
    want = {k: str(v) for k, v in labels.items()}
    hits = []
    for (n, ls), v in metrics.items():
        if n != name:
            continue
        d = dict(ls)
        if all(d.get(k) == v2 for k, v2 in want.items()):
            hits.append((d, v))
    hits.sort(key=lambda kv: sorted(kv[0].items()))
    return hits


def prom_get(metrics, name, **labels):
    """The single matching sample's value, or `None` when there is not exactly
    one. Ambiguity reads as absence deliberately: a row that silently took
    the first of several series would be reporting an arbitrary one."""
    hits = prom_find(metrics, name, **labels)
    return hits[0][1] if len(hits) == 1 else None


# ------------------------------------------------------ histogram helpers
def hist_series(metrics, name, **labels):
    """`(buckets, sum_value, count_value)` for histogram family `name`.

    `buckets` is `[(le, cumulative_count)]` sorted ascending by `le`, read off
    `<name>_bucket`; `sum_value`/`count_value` come from `<name>_sum` /
    `<name>_count`. Any of the three may be `None`/empty when the family (or
    that label set) is absent from the scrape."""
    buckets = []
    for d, v in prom_find(metrics, name + "_bucket", **labels):
        le = d.get("le")
        if le is None:
            continue
        try:
            buckets.append((float(le), v))
        except ValueError:
            continue
    buckets.sort(key=lambda b: b[0])
    return buckets, prom_get(metrics, name + "_sum", **labels), \
        prom_get(metrics, name + "_count", **labels)


def hist_quantile(buckets, q):
    """The UPPER BOUND of the bucket that holds the q-th sample — deliberately
    conservative, and NOT an interpolation.

    `buckets` is cumulative, so the total is the largest cumulative count (the
    `+Inf` bucket in a well-formed histogram). The q-th sample is the
    `ceil(q * total)`-th, and the answer is the `le` of the first bucket whose
    cumulative count reaches it. Reporting the bound rather than a linear
    interpolation inside it means row c's p99 can only ever be quoted HIGHER
    than the truth, never lower — the right direction to be wrong in for a
    "p99 must be under a bar" row. A sample in the `+Inf` bucket therefore
    reads `inf`, which fails any finite bar, as it should.

    `None` when there are no buckets or no samples."""
    if not buckets:
        return None
    total = max(c for _, c in buckets)
    if total <= 0:
        return None
    rank = math.ceil(q * total)
    if rank < 1:
        rank = 1
    for le, cum in buckets:
        if cum >= rank:
            return le
    return buckets[-1][0]


def hist_mean(sum_value, count_value):
    """`_sum / _count`, or `None` when either is missing or the count is 0."""
    if sum_value is None or not count_value:
        return None
    return sum_value / count_value


# ------------------------------------------------------------ row h helper
def longest_stall(timeline, baseline_rps, frac=STALL_FRACTION, lo_ms=None, hi_ms=None):
    """The longest run of consecutive 1 s buckets at or below `frac x
    baseline_rps`, in seconds, over `[(unix_ms, responses)]`.

    This is row h's "longest gap in commit advance": the client's completion
    rate is the visible end of the commit stream, so a run of near-zero
    buckets IS the stall the freeze caused. Same 1 s bucket shape
    `recovery_time` reads, and the same deliberate coarseness — a gap shorter
    than one bucket is not resolvable here and reads as 0.0 s, which is
    exactly the answer row h's standby arm wants to be able to give.

    `lo_ms`/`hi_ms`, when given, bound the window (half-open) so the measure
    covers only the instant, not the whole arm. Returns seconds (float)."""
    window = [(ms, r) for ms, r in timeline
              if (lo_ms is None or ms >= lo_ms) and (hi_ms is None or ms < hi_ms)]
    window.sort(key=lambda b: b[0])
    if not window or baseline_rps <= 0:
        return 0.0
    threshold = frac * baseline_rps
    best = run = 0
    for _, r in window:
        if r <= threshold:
            run += 1
            best = max(best, run)
        else:
            run = 0
    return float(best)


# ------------------------------------------------------------ row e helper
def schedule_toml(count, fsm, base_id=SCHEDULE_ID_BASE, every=SCHEDULE_EVERY,
                  anchor=SCHEDULE_ANCHOR):
    """Row e's table file: `count` `[[schedule]]` entries, all on ONE declared
    row `fsm`, ids `base_id + i`, the same `every`/`anchor` on each.

    `fsm` is the row's DECLARED NAME, not a fixed string: an entry naming a row
    this cluster does not declare refuses the WHOLE table (refusal 43
    `schedule_unknown_fsm`), and the driver's `slow1` arm declares row 0 as
    `spin` rather than `count`. Callers pass `fsm_name(0, spin)`.

    Shape is `docs/how-to/run-work-on-a-schedule.md`'s verbatim."""
    if count < 1 or count > MAX_SCHEDULE_ENTRIES:
        raise ValueError(f"schedule table size {count} outside 1..{MAX_SCHEDULE_ENTRIES}")
    out = [
        "# time-and-timers gate row e: MAX_SCHEDULE_ENTRIES `every` rules on one row.",
        "# Generated by bench-infra/scripts/tt_fleet_gate.py — do not hand-edit.",
    ]
    for i in range(count):
        out += [
            "",
            "[[schedule]]",
            f'fsm    = "{fsm}"',
            f"id     = {base_id + i}",
            f'every  = "{every}"',
            f'anchor = "{anchor}"',
        ]
    return "\n".join(out) + "\n"


# --------------------------------------------------------- A/B arithmetic
def ab_stats(arms):
    """`arms` = `{label: {"base": [rate, ...], "head": [rate, ...]}}` (one rate
    per interleaved rep) -> per-label means, ranges, rep-to-rep spread, the
    standard error of each arm's MEAN, and the head-vs-base delta.

    `sem_pct` is `stdev / (mean * sqrt(n)) * 100`, the same quantity
    `scripts/apply_ab.sh` computes: unlike a min/max spread it shrinks as
    `1/sqrt(n)`, so `--ab-reps` is a real remedy for a run that cannot resolve
    its bar rather than a knob that cannot move the number. Both are reported;
    only `sem_pct` gates the verdict."""
    out = {}
    for label, sides in arms.items():
        row = {}
        for side in ("base", "head"):
            xs = [float(x) for x in sides.get(side, []) if x]
            if not xs:
                continue
            mean = statistics.mean(xs)
            sd = statistics.stdev(xs) if len(xs) > 1 else 0.0
            row[side] = {
                "n": len(xs),
                "mean": mean,
                "min": min(xs),
                "max": max(xs),
                "spread_pct": (100.0 * (max(xs) - min(xs)) / mean) if mean else 0.0,
                "sem_pct": (100.0 * sd / (mean * math.sqrt(len(xs)))) if mean and len(xs) > 1 else None,
            }
        if "base" in row and "head" in row and row["base"]["mean"]:
            row["delta_pct"] = 100.0 * (row["head"]["mean"] - row["base"]["mean"]) / row["base"]["mean"]
        # PAIRED statistic (maintainer ruling, 2026-09-08). The driver already
        # runs base and head interleaved WITHIN each rep, so rep i's two rates
        # are adjacent in time and see the same thermal state, the same noisy
        # neighbour, the same drift. Differencing the two arm MEANS throws that
        # away and leaves every bit of common-mode noise in the comparison,
        # which is why the 2026-09-07 run read arm sems of 13-21 % against a
        # 1.12 % bar and could not resolve it at any affordable rep count.
        # The per-rep delta cancels whatever both arms saw together; what
        # survives is the difference between the binaries.
        #
        # `paired_sem_pp` is in PERCENTAGE POINTS of the delta itself, not a
        # fraction of a rate, so it compares directly against the resolution.
        b, h = sides.get("base", []), sides.get("head", [])
        pairs = [(float(x), float(y)) for x, y in zip(b, h) if x and y]
        if len(pairs) >= 1:
            deltas = [100.0 * (y - x) / x for x, y in pairs]
            pmean = statistics.mean(deltas)
            psd = statistics.stdev(deltas) if len(deltas) > 1 else 0.0
            row["paired"] = {
                "n": len(deltas),
                "deltas_pct": [round(d, 4) for d in deltas],
                "delta_pct": pmean,
                "sem_pp": (psd / math.sqrt(len(deltas))) if len(deltas) > 1 else None,
                "spread_pp": (max(deltas) - min(deltas)) if len(deltas) > 1 else 0.0,
            }
        out[label] = row
    return out


def ab_reading(stats, resolution_pct):
    """`(reading, worst_delta_pct, worst_sem_pct)` over every arm in `stats`.

    The rule is `scripts/apply_ab.sh`'s, transplanted to fleet rates so the
    two harnesses cannot drift (gate doc, "Rows d and f: the verdict rule"):

        missing arm / no resolution / n < 2      -> inconclusive (named)
        worst PAIRED sem > resolution            -> inconclusive (noisy run)
        |worst PAIRED delta| <= resolution       -> within resolution
        otherwise                                -> outside resolution

    **Paired since 2026-09-08** (maintainer ruling). Base and head already run
    interleaved within each rep, so the per-rep delta cancels the noise both
    arms saw together — thermal state, a noisy neighbour, drift. Judging the
    difference of two arm MEANS discarded that and left the comparison at the
    mercy of rig variance: the 2026-09-07 run read arm sems of 13-21 % against
    a 1.12 % bar, which no affordable rep count could resolve (sem falls as
    1/sqrt(n), so it would have taken ~430 reps per arm).

    Only `within resolution` is a PASS. "Inconclusive" is a third answer, not
    a soft pass: a run whose own arms are noisier than the bar cannot resolve
    it, and recording that as either outcome would be dishonest. Run quality
    never widens the bar — these are NULL bars ("the stamp is free"), so
    anything added to the right-hand side only makes it easier to bless a
    real regression."""
    if not stats:
        return "inconclusive (no arms)", None, None
    deltas, sems = [], []
    for label, row in sorted(stats.items()):
        if "base" not in row or "head" not in row or "delta_pct" not in row:
            return f"inconclusive (missing arm: {label})", None, None
        for side in ("base", "head"):
            if row[side]["n"] < 2:
                return f"inconclusive (too few reps: {label}/{side})", None, None
        # PAIRED (2026-09-08 ruling): judge the per-rep paired delta and ITS
        # standard error, not the difference of two arm means and the arms'
        # own sems. The unpaired numbers stay in the GATE-JSON for continuity
        # and for comparison against runs made before this change.
        pr = row.get("paired")
        if pr is None or pr["n"] < 2:
            return f"inconclusive (too few paired reps: {label})", None, None
        deltas.append(pr["delta_pct"])
        if pr["sem_pp"] is not None:
            sems.append(pr["sem_pp"])
    worst_delta = max(deltas, key=abs)
    worst_sem = max(sems) if sems else None
    if resolution_pct is None or resolution_pct <= 0:
        return "inconclusive (no resolution recorded)", worst_delta, worst_sem
    if worst_sem is not None and worst_sem > resolution_pct:
        return "inconclusive (noisy run)", worst_delta, worst_sem
    if abs(worst_delta) <= resolution_pct:
        return "within resolution", worst_delta, worst_sem
    return "outside resolution", worst_delta, worst_sem


def late_offenders(late_counts):
    """`late_counts` = `[(arm, host, service, row, late)]` -> the nonzero ones.

    Rows b and e both assert `uc2_timers_late_total == 0` on EVERY node for
    EVERY row after the warm-up window; this is the clause, kept pure so the
    selftest can pin what "offending host/row/count" means."""
    return [tuple(t) for t in late_counts if int(t[4]) != 0]


# ---------------------------------------------------------------- selftest
SAMPLE_METRICS = """\
# HELP uc2_timers_late_total Fired timers whose stamp exceeded their deadline.
# TYPE uc2_timers_late_total counter
uc2_timers_late_total{service="count",row="0"} 0
uc2_timers_late_total{service="spin",row="1"} 3
# HELP uc2_commit_bytes Committed byte position.
# TYPE uc2_commit_bytes gauge
uc2_commit_bytes 123456789
# TYPE uc2_timer_lateness_ns histogram
uc2_timer_lateness_ns_bucket{service="count",row="0",le="10000"} 5
uc2_timer_lateness_ns_bucket{service="count",row="0",le="20000"} 90
uc2_timer_lateness_ns_bucket{service="count",row="0",le="50000"} 99
uc2_timer_lateness_ns_bucket{service="count",row="0",le="+Inf"} 100
uc2_timer_lateness_ns_sum{service="count",row="0"} 1500000
uc2_timer_lateness_ns_count{service="count",row="0"} 100
uc2_timer_lateness_ns_max{service="count",row="0"} 44000
# TYPE uc2_consensus_pass_ns histogram
uc2_consensus_pass_ns_bucket{le="10000"} 800
uc2_consensus_pass_ns_bucket{le="+Inf"} 1000
uc2_consensus_pass_ns_sum 9000000
uc2_consensus_pass_ns_count 1000
uc2_schedule_table_position 8192
uc2_weird{note="a}b,c"} 7
uc2_stamped 42 1757200000000
"""


def selftest():
    """Every pure function in this module, on literal inputs. Returns the
    number of failures; `m14_fleet_gate.py --selftest` folds this in."""
    fails = 0

    def expect(name, cond):
        nonlocal fails
        print(f"  [{'ok' if cond else 'FAIL'}] {name}")
        fails += 0 if cond else 1

    m = parse_prom(SAMPLE_METRICS)
    expect("parse_prom skips HELP/TYPE and keeps every sample", len(m) == 17)
    expect("parse_prom reads a labelled sample",
           m[("uc2_timers_late_total", frozenset({("service", "count"), ("row", "0")}))] == 0.0)
    expect("parse_prom reads a bare sample", m[("uc2_commit_bytes", frozenset())] == 123456789.0)
    expect("parse_prom ignores a trailing timestamp", m[("uc2_stamped", frozenset())] == 42.0)
    expect("parse_prom keeps '}' and ',' inside a quoted label value",
           m[("uc2_weird", frozenset({("note", "a}b,c")}))] == 7.0)
    expect("parse_prom on empty text is empty", parse_prom("") == {})
    expect("parse_prom skips a malformed value line", parse_prom("uc2_x notanumber\n") == {})
    expect("prom_find returns one entry per row",
           [v for _, v in prom_find(m, "uc2_timers_late_total")] == [0.0, 3.0])
    expect("prom_find narrows on a label", prom_find(m, "uc2_timers_late_total", row="1")[0][1] == 3.0)
    expect("prom_get on a unique bare sample", prom_get(m, "uc2_schedule_table_position") == 8192.0)
    expect("prom_get on an ambiguous family is None", prom_get(m, "uc2_timers_late_total") is None)
    expect("prom_get on an absent family is None", prom_get(m, "uc2_nope") is None)

    buckets, s, c = hist_series(m, "uc2_timer_lateness_ns", service="count", row="0")
    expect("hist_series reads 4 buckets, sum and count",
           len(buckets) == 4 and s == 1500000.0 and c == 100.0)
    expect("hist_series sorts by le with +Inf last",
           [b[0] for b in buckets] == [10000.0, 20000.0, 50000.0, float("inf")])
    expect("hist_quantile p99 is the bucket's UPPER bound (99th sample lands in le=50000)",
           hist_quantile(buckets, 0.99) == 50000.0)
    expect("hist_quantile p50 is le=20000", hist_quantile(buckets, 0.50) == 20000.0)
    expect("hist_quantile p999 lands in +Inf and reads inf",
           hist_quantile(buckets, 0.999) == float("inf"))
    expect("hist_quantile on no buckets is None", hist_quantile([], 0.99) is None)
    expect("hist_quantile on an all-zero histogram is None",
           hist_quantile([(10000.0, 0), (float("inf"), 0)], 0.99) is None)
    expect("hist_quantile is conservative at an exact boundary (5 samples <= 10000, q=0.05)",
           hist_quantile(buckets, 0.05) == 10000.0)
    expect("hist_mean = sum/count", hist_mean(s, c) == 15000.0)
    expect("hist_mean with a zero count is None", hist_mean(1.0, 0.0) is None)
    expect("hist_mean with no sum is None", hist_mean(None, 10.0) is None)
    pb, ps, pc = hist_series(m, "uc2_consensus_pass_ns")
    expect("node-level histogram needs no labels", hist_mean(ps, pc) == 9000.0)
    expect("node-level histogram p99 lands in +Inf", hist_quantile(pb, 0.99) == float("inf"))

    # longest_stall: 1 s buckets at 1000/s, a 3 s hole, then back.
    tl = [(ms, 1000) for ms in range(0, 5000, 1000)] + \
         [(ms, 0) for ms in range(5000, 8000, 1000)] + \
         [(ms, 1000) for ms in range(8000, 12000, 1000)]
    expect("longest_stall finds the 3 s hole", longest_stall(tl, 1000.0) == 3.0)
    expect("longest_stall with no hole is 0", longest_stall(
        [(ms, 1000) for ms in range(0, 5000, 1000)], 1000.0) == 0.0)
    expect("longest_stall counts a bucket AT the 10 % threshold as stalled",
           longest_stall([(0, 1000), (1000, 100), (2000, 1000)], 1000.0) == 1.0)
    expect("longest_stall leaves a bucket just above the threshold alone",
           longest_stall([(0, 1000), (1000, 101), (2000, 1000)], 1000.0) == 0.0)
    expect("longest_stall takes the LONGEST of two holes",
           longest_stall([(0, 1000), (1000, 0), (2000, 1000), (3000, 0), (4000, 0), (5000, 1000)],
                         1000.0) == 2.0)
    expect("longest_stall honours the window bounds",
           longest_stall(tl, 1000.0, lo_ms=8000) == 0.0)
    expect("longest_stall on an empty timeline is 0", longest_stall([], 1000.0) == 0.0)
    expect("longest_stall with no baseline is 0", longest_stall(tl, 0.0) == 0.0)

    t = schedule_toml(32, "count")
    expect("schedule_toml writes MAX_SCHEDULE_ENTRIES entries", t.count("[[schedule]]") == 32)
    expect("schedule_toml ids start at 5000", "id     = 5000" in t and "id     = 5031" in t)
    expect("schedule_toml carries the every/anchor rule verbatim",
           'every  = "100ms"' in t and 'anchor = "2026-01-01T00:00:00Z"' in t)
    expect("schedule_toml names the DECLARED row, not a fixed string",
           'fsm    = "spin"' in schedule_toml(1, "spin"))
    try:
        schedule_toml(33, "count")
        expect("schedule_toml refuses past MAX_SCHEDULE_ENTRIES", False)
    except ValueError:
        expect("schedule_toml refuses past MAX_SCHEDULE_ENTRIES", True)

    arms = {"n1": {"base": [1000.0, 1010.0, 990.0], "head": [1005.0, 995.0, 1000.0]}}
    st = ab_stats(arms)
    expect("ab_stats means", abs(st["n1"]["base"]["mean"] - 1000.0) < 1e-9)
    expect("ab_stats delta is head vs base", abs(st["n1"]["delta_pct"] - 0.0) < 1e-9)
    expect("ab_stats reports rep-to-rep spread", abs(st["n1"]["base"]["spread_pct"] - 2.0) < 1e-9)
    expect("ab_stats sem shrinks with n", st["n1"]["base"]["sem_pct"] < st["n1"]["base"]["spread_pct"])
    expect("ab_reading within a generous resolution", ab_reading(st, 5.0)[0] == "within resolution")
    expect("ab_reading noisy when sem exceeds the resolution",
           ab_reading(st, 0.01)[0] == "inconclusive (noisy run)")
    st_out = ab_stats({"n1": {"base": [1000.0, 1000.0, 1000.0], "head": [900.0, 900.0, 900.0]}})
    expect("ab_reading outside resolution on a real regression",
           ab_reading(st_out, 1.0)[0] == "outside resolution")
    expect("ab_reading reports the delta it judged",
           abs(ab_reading(st_out, 1.0)[1] + 10.0) < 1e-9)
    # THE PAIRED STATISTIC, and why it exists (2026-09-08 ruling). Common-mode
    # noise: every rep drifts wildly (100 -> 200 -> 150), but head is a steady
    # +1 % over base IN EACH REP. Judging the arm MEANS leaves all that drift
    # in the arms' own sems and the run cannot resolve any tight bar; the
    # per-rep paired delta cancels it and reads +1.000 % with zero spread.
    _drift = ab_stats({"n1": {"base": [100.0, 200.0, 150.0],
                              "head": [101.0, 202.0, 151.5]}})
    expect("paired delta cancels common-mode drift",
           abs(_drift["n1"]["paired"]["delta_pct"] - 1.0) < 1e-9)
    expect("paired sem is ~0 when every rep moves together",
           _drift["n1"]["paired"]["sem_pp"] < 1e-9)
    expect("the UNPAIRED arm sem on that same data is enormous by comparison",
           _drift["n1"]["base"]["sem_pct"] > 15.0)
    expect("paired judging resolves a bar the unpaired statistic could not",
           ab_reading(_drift, 1.5)[0] == "within resolution")
    # ...and it is not a way to bless a regression: a real, consistent -10 %
    # under the same drift still reads OUTSIDE.
    _drift_reg = ab_stats({"n1": {"base": [100.0, 200.0, 150.0],
                                  "head": [90.0, 180.0, 135.0]}})
    expect("a consistent regression under drift still reads outside",
           ab_reading(_drift_reg, 1.5)[0] == "outside resolution")
    expect("ab_reading with no resolution is inconclusive",
           ab_reading(st_out, None)[0] == "inconclusive (no resolution recorded)")
    expect("ab_reading with a single rep is inconclusive",
           ab_reading(ab_stats({"n1": {"base": [1.0], "head": [1.0]}}), 5.0)[0].startswith(
               "inconclusive (too few reps"))
    expect("ab_reading with a missing arm side is inconclusive",
           ab_reading(ab_stats({"n1": {"base": [1.0, 1.0]}}), 5.0)[0].startswith(
               "inconclusive (missing arm"))
    expect("ab_reading on no arms is inconclusive", ab_reading({}, 5.0)[0] == "inconclusive (no arms)")
    expect("ab_reading takes the WORST arm's delta",
           abs(ab_reading(ab_stats({
               "n1": {"base": [100.0, 100.0], "head": [100.0, 100.0]},
               "pair": {"base": [100.0, 100.0], "head": [80.0, 80.0]},
           }), 1.0)[1] + 20.0) < 1e-9)

    expect("late_offenders passes a clean sweep",
           late_offenders([("n1", "h0", "count", "0", 0), ("n1", "h1", "count", "0", 0)]) == [])
    expect("late_offenders names the offender",
           late_offenders([("n1", "h0", "count", "0", 0), ("pair", "h2", "spin", "1", 4)])
           == [("pair", "h2", "spin", "1", 4)])
    return fails


def main():
    if "--selftest" not in sys.argv[1:]:
        print("usage: tt_fleet_gate.py --selftest  (leaf helpers only; the fleet "
              "rows live in m14_fleet_gate.py)")
        return 2
    fails = selftest()
    print(f"tt selftest: {'PASS' if fails == 0 else f'FAIL ({fails})'}")
    return 0 if fails == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
