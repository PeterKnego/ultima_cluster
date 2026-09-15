#!/usr/bin/env python3
"""Clean-room read audit for a dogfood persona session (wayfinder map #16).

Given a sandbox root and the Claude Code project directory that holds the
session's transcripts, list every file path the session touched through a
tool call — Read, Glob, Grep, LS, Edit, Write, NotebookEdit, and every
path-like token in a Bash command — and classify each as INSIDE the
sandbox, FORBIDDEN (a path the clean-room rule names outright), or OUTSIDE
(anything else off the sandbox root, for the maintainer to judge).

Each tool call is matched with its result, so a read the permission walls
DENIED (the result is an error saying so) is told apart from one that
SUCCEEDED. Verdicts:
  VOID   — a FORBIDDEN read succeeded: the persona saw it; the run's findings
           are void.
  BREACH — FORBIDDEN reads were attempted but every one was denied: nothing
           leaked, the run stands, and the attempt is recorded in the report
           as a discipline breach.
  JUDGE  — no FORBIDDEN read, but OUTSIDE reads exist for the maintainer to
           judge (a `cat /etc/os-release` is harmless, `cat ~/.bash_history`
           is not), or TEXT: an off-sandbox or forbidden path the persona
           WROTE into a file through a here-document (a ledger entry naming
           `~/.cargo/registry/src` is harmless; a script that reads it and is
           run later is not — judge the file it went into).
  CLEAN  — nothing off the sandbox but toolchain/OS paths.
Exit status: 0 CLEAN, 1 anything else, 2 no transcripts.

The transcript format this parses: one JSON object per line; assistant
turns carry `message.content[]` blocks of `type: "tool_use"` with `name`
and `input`. Subagent transcripts sit under `<project>/<session>/subagents/`
and are scanned too. Bash commands are scanned textually — every token
that looks like a path — because Bash can read anything; the Read-tool deny
rules in the sandbox's `.claude/settings.json` are the hard wall for the
Read tool only, and this audit is the wall for the rest.

Usage:
  scripts/dogfood_audit.py --sandbox ~/ultima/kv_store \
      [--project ~/.claude/projects/-home-claude-ultima-kv_store] \
      [--session <uuid>] [--json]

The project directory defaults to Claude Code's encoding of the sandbox
path (slashes → dashes). Pass --session to audit one session only.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
from pathlib import Path

# Paths the clean-room rule names outright. Matching any of these is
# FORBIDDEN wherever it happens — including a `~/.cargo/registry/src` tree
# that cargo legitimately populated under the sandbox's own CARGO_HOME.
FORBIDDEN_PATTERNS = [
    r"/ultima/ultima_cluster(/|$)",
    r"/ultima/openraft(/|$)",
    r"/ultima/aeron-go(/|$)",
    r"/ultima/hi-perf-cmp(/|$)",
    r"/ultima/ultima_db(/|$)",
    r"/uc2-sdd-archive(/|$)",
    r"/\.cargo/registry/src(/|$)",
    r"/\.cargo/git(/|$)",
    r"/\.claude/projects(/|$)",
    r"/\.claude/plugins(/|$)",
    r"/\.claude/skills(/|$)",
    r"/scratch(/|$)",
]

# Off-root paths nobody needs to judge: the toolchain, the OS, cargo's own
# non-source state. Listed so the OUTSIDE report is signal, not noise.
BENIGN_PREFIXES = (
    "/usr/", "/etc/", "/proc/", "/dev/", "/sys/", "/bin/", "/sbin/",
    "/lib", "/opt/", "/var/log/", "/run/", "/tmp/",
    os.path.expanduser("~/.rustup/"),
    os.path.expanduser("~/.cargo/bin/"),
    os.path.expanduser("~/.cargo/registry/index/"),
    os.path.expanduser("~/.cargo/registry/cache/"),
    os.path.expanduser("~/.cargo/config"),
)

PATH_INPUT_KEYS = ("file_path", "path", "notebook_path", "directory")

# A token in a Bash command that looks like a filesystem path.
BASH_PATH_RE = re.compile(r"(?<![\w@:])(~?/[\w./@+~-]+|\.\.?/[\w./@+~-]*)")


def encode_project(sandbox: Path) -> str:
    """Claude Code's project-directory encoding: every character that is not
    a letter or digit becomes a dash (`/home/x/kv_store` -> `-home-x-kv-store`)."""
    return re.sub(r"[^A-Za-z0-9]", "-", str(sandbox))


def project_dir_for(sandbox: Path) -> Path:
    return Path.home() / ".claude" / "projects" / encode_project(sandbox)


# The harness gives each session a private scratchpad at
# `$TMPDIR/claude-<uid>/<encoded project>/<session>/scratchpad`; on a box whose
# TMPDIR points at real disk it can land under `~/scratch`, which is FORBIDDEN
# for a different reason (the rustdoc build's cargo source lives there).
# Files the session itself writes to its own scratchpad are not reads of
# anything, so that one path — anchored to a known temp root, THIS uid, and
# the session ids actually being audited — is exempt from the `/scratch`
# pattern only. It never exempts any other FORBIDDEN pattern, so a crafted
# scratchpad-shaped directory under a source repo stays FORBIDDEN.
SCRATCH_PATTERN = r"/scratch(/|$)"


def scratchpad_re(sandbox: Path, session_ids: list[str]) -> re.Pattern:
    roots = {"/tmp", os.path.expanduser("~/scratch/tmp"), tempfile.gettempdir()}
    roots |= {os.path.realpath(r) for r in list(roots)}
    root_alt = "|".join(re.escape(r.rstrip("/")) for r in sorted(roots))
    sid_alt = "|".join(re.escape(x) for x in session_ids) or "(?!)"
    return re.compile(rf"^(?:{root_alt})/claude-{os.getuid()}/{re.escape(encode_project(sandbox))}/(?:{sid_alt})/scratchpad(/|$)")


# A here-document fed to a WRITE (`cat > f <<EOF`, `cat <<EOF > f`,
# `tee f <<EOF`) is data on its way into a file, not a command; path-like
# tokens inside it (a ledger entry naming `~/.cargo/registry/src`, a README's
# `/home/you/...`) are text the persona wrote, not reads. Those tokens are NOT
# dropped: they are tokenised separately and reported as TEXT for the
# maintainer to judge (a script written into a file and run later is the case
# that must stay visible). A here-document fed to anything else (`bash <<EOF`,
# `python3 - <<EOF`), or whose header line also pipes or chains (`tee f <<EOF
# | bash`), is executed and stays in the command proper.
# Group 1 = the `-` of `<<-` (then leading tabs before the terminator are
# allowed, as in the shell); group 2 = quote; group 3 = delimiter; group 4 =
# the rest of the header line; group 5 = the body.
HEREDOC_RE = re.compile(r"<<(-?)\s*(['\"]?)(\w+)\2([^\n]*)\n(.*?)\n(?(1)\t*)\3(?=\n|$)", re.S)
# Form A: `cat > f <<EOF` / `cat >> f <<EOF` / `tee [-a] f <<EOF`, tail empty.
# Form B: `cat <<EOF > f` — bare `cat`, the redirect in the tail.
# Anything else on the header line (a pipe, `;`, `&&`) means the body may be
# executed, so it stays in the command proper.
WRITE_HEAD_A_RE = re.compile(r"(^|[;&|]\s*)(cat\s*>{1,2}\s*\S+|tee\s+(-a\s+)?\S+)\s*$")
WRITE_HEAD_B_RE = re.compile(r"(^|[;&|]\s*)cat\s*$")
TAIL_EMPTY_RE = re.compile(r"^\s*$")
TAIL_REDIRECT_RE = re.compile(r"^\s*>{1,2}\s*\S+\s*$")


def split_write_heredocs(cmd: str) -> tuple[str, list[str]]:
    """(command with write-heredoc bodies removed, [those bodies])."""
    bodies: list[str] = []

    def repl(m: re.Match) -> str:
        start = cmd.rfind("\n", 0, m.start()) + 1
        lead, tail = cmd[start:m.start()], m.group(4)
        form_a = WRITE_HEAD_A_RE.search(lead) and TAIL_EMPTY_RE.match(tail)
        form_b = WRITE_HEAD_B_RE.search(lead) and TAIL_REDIRECT_RE.match(tail)
        if form_a or form_b:
            bodies.append(m.group(5))
            return m.group(0)[: m.end(4) - m.start()] + "\n" + m.group(3)
        return m.group(0)

    return HEREDOC_RE.sub(repl, cmd), bodies


DENIAL_RE = re.compile(r"denied|not allowed|permission", re.I)


def scan(jsonl: Path):
    """(tool_uses, results): tool_uses = [(line, id, name, input)],
    results = {tool_use_id: "denied" | "error" | "ok"}."""
    uses, results = [], {}
    with jsonl.open(errors="replace") as fh:
        for n, line in enumerate(fh, 1):
            if '"tool_use"' not in line and '"tool_result"' not in line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            content = (obj.get("message") or {}).get("content")
            if not isinstance(content, list):
                continue
            for block in content:
                if not isinstance(block, dict):
                    continue
                if block.get("type") == "tool_use":
                    uses.append((n, block.get("id"), block.get("name") or "?", block.get("input") or {}))
                elif block.get("type") == "tool_result":
                    body = block.get("content")
                    text = body if isinstance(body, str) else json.dumps(body)
                    if block.get("is_error"):
                        results[block.get("tool_use_id")] = "denied" if DENIAL_RE.search(text or "") else "error"
                    else:
                        results[block.get("tool_use_id")] = "ok"
    return uses, results


def paths_from(name: str, inp: dict) -> list[tuple[str, str]]:
    """(path, how) pairs a tool call touches."""
    out: list[tuple[str, str]] = []
    for k in PATH_INPUT_KEYS:
        v = inp.get(k)
        if isinstance(v, str) and v:
            out.append((v, f"{name}.{k}"))
    if name == "Glob":
        pat = inp.get("pattern")
        if isinstance(pat, str) and pat.startswith(("/", "~", ".")):
            out.append((pat, "Glob.pattern"))
    if name == "Bash":
        cmd, bodies = split_write_heredocs(inp.get("command") or "")
        for m in BASH_PATH_RE.finditer(cmd):
            out.append((m.group(1), "Bash"))
        for body in bodies:
            for m in BASH_PATH_RE.finditer(body):
                out.append((m.group(1), "Bash-text"))
    if name == "Agent":
        # a subagent's own reads are in its own transcript; but a prompt that
        # names a forbidden path is itself a leak worth seeing
        prompt = inp.get("prompt") or ""
        for m in BASH_PATH_RE.finditer(prompt):
            out.append((m.group(1), "Agent.prompt"))
    return out


def classify(raw: str, sandbox: Path, cwd_hint: Path, scratch_re: re.Pattern) -> tuple[str, str]:
    p = os.path.expanduser(raw)
    if not p.startswith("/"):
        p = str((cwd_hint / p))
    p = os.path.normpath(p)
    for pat in FORBIDDEN_PATTERNS:
        if re.search(pat, p):
            if pat == SCRATCH_PATTERN and scratch_re.match(p):
                return "BENIGN", p
            return "FORBIDDEN", p
    try:
        Path(p).relative_to(sandbox)
        return "INSIDE", p
    except ValueError:
        pass
    if p.startswith(BENIGN_PREFIXES):
        return "BENIGN", p
    return "OUTSIDE", p


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sandbox", required=True, type=Path)
    ap.add_argument("--project", type=Path)
    ap.add_argument("--session", help="audit only this session id")
    ap.add_argument("--json", action="store_true", help="machine-readable report")
    a = ap.parse_args()

    sandbox = a.sandbox.expanduser().resolve()
    project = (a.project or project_dir_for(sandbox)).expanduser()
    if not project.is_dir():
        print(f"no transcripts: {project} does not exist", file=sys.stderr)
        return 2

    files = sorted(project.rglob("*.jsonl"))
    if a.session:
        files = [f for f in files if a.session in str(f)]
    if not files:
        print(f"no transcripts under {project}", file=sys.stderr)
        return 2

    session_ids = sorted({f.stem for f in files} | {q.name for f in files for q in f.parents if re.fullmatch(r"[0-9a-f-]{36}", q.name)})
    scratch_re = scratchpad_re(sandbox, session_ids)
    findings: dict[str, list[dict]] = {"FORBIDDEN": [], "OUTSIDE": [], "TEXT": [], "BENIGN": [], "INSIDE": []}
    counts = {"tool_uses": 0, "files": len(files)}
    for f in files:
        uses, results = scan(f)
        for line_no, tid, name, inp in uses:
            counts["tool_uses"] += 1
            outcome = results.get(tid, "no-result")
            for raw, how in paths_from(name, inp):
                cls, p = classify(raw, sandbox, sandbox, scratch_re)
                if how == "Bash-text" and cls in ("FORBIDDEN", "OUTSIDE"):
                    cls = "TEXT"  # written into a file, not read; the maintainer judges
                findings[cls].append({"path": p, "raw": raw, "how": how, "file": f.name,
                                      "line": line_no, "outcome": outcome})

    # dedupe by (path, how) for the human report
    def uniq(rows):
        seen, out = set(), []
        for r in rows:
            k = (r["path"], r["how"], r["outcome"])
            if k not in seen:
                seen.add(k); out.append(r)
        return out

    forbidden_seen = [r for r in findings["FORBIDDEN"] if r["outcome"] != "denied"]
    if forbidden_seen:
        verdict = "VOID"
    elif findings["FORBIDDEN"]:
        verdict = "BREACH"
    elif [r for r in findings["OUTSIDE"] + findings["TEXT"] if r["outcome"] != "denied"]:
        verdict = "JUDGE"
    else:
        verdict = "CLEAN"
    if a.json:
        print(json.dumps({"verdict": verdict, "sandbox": str(sandbox), "project": str(project),
                          "counts": counts, "forbidden": uniq(findings["FORBIDDEN"]),
                          "outside": uniq(findings["OUTSIDE"]), "text": uniq(findings["TEXT"]),
                          "inside_paths": len(uniq(findings["INSIDE"]))}, indent=2))
    else:
        print(f"sandbox : {sandbox}\nproject : {project}\ntranscripts: {counts['files']}  tool uses: {counts['tool_uses']}")
        print(f"inside  : {len(uniq(findings['INSIDE']))} distinct paths   benign off-root: {len(uniq(findings['BENIGN']))}")
        for cls in ("FORBIDDEN", "OUTSIDE", "TEXT"):
            rows = uniq(findings[cls])
            print(f"\n{cls}: {len(rows)}")
            for r in rows:
                print(f"  [{r['outcome']:9}] {r['path']}   via {r['how']}   ({r['file']}:{r['line']})")
        tail = {"VOID": "  — a forbidden read SUCCEEDED; the run's findings are void",
                "BREACH": "  — forbidden reads were attempted, all denied; run stands, breach recorded",
                "JUDGE": "  — judge the OUTSIDE reads and the TEXT the persona wrote", "CLEAN": ""}[verdict]
        print(f"\nVERDICT: {verdict}{tail}")
    return 0 if verdict == "CLEAN" else 1


if __name__ == "__main__":
    sys.exit(main())
