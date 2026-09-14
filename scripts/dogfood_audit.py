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
           is not).
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


def project_dir_for(sandbox: Path) -> Path:
    enc = str(sandbox).replace("/", "-")
    return Path.home() / ".claude" / "projects" / enc


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
        cmd = inp.get("command") or ""
        for m in BASH_PATH_RE.finditer(cmd):
            out.append((m.group(1), "Bash"))
    if name == "Agent":
        # a subagent's own reads are in its own transcript; but a prompt that
        # names a forbidden path is itself a leak worth seeing
        prompt = inp.get("prompt") or ""
        for m in BASH_PATH_RE.finditer(prompt):
            out.append((m.group(1), "Agent.prompt"))
    return out


def classify(raw: str, sandbox: Path, cwd_hint: Path) -> tuple[str, str]:
    p = os.path.expanduser(raw)
    if not p.startswith("/"):
        p = str((cwd_hint / p))
    p = os.path.normpath(p)
    for pat in FORBIDDEN_PATTERNS:
        if re.search(pat, p):
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

    findings: dict[str, list[dict]] = {"FORBIDDEN": [], "OUTSIDE": [], "BENIGN": [], "INSIDE": []}
    counts = {"tool_uses": 0, "files": len(files)}
    for f in files:
        uses, results = scan(f)
        for line_no, tid, name, inp in uses:
            counts["tool_uses"] += 1
            outcome = results.get(tid, "no-result")
            for raw, how in paths_from(name, inp):
                cls, p = classify(raw, sandbox, sandbox)
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
    elif [r for r in findings["OUTSIDE"] if r["outcome"] != "denied"]:
        verdict = "JUDGE"
    else:
        verdict = "CLEAN"
    if a.json:
        print(json.dumps({"verdict": verdict, "sandbox": str(sandbox), "project": str(project),
                          "counts": counts, "forbidden": uniq(findings["FORBIDDEN"]),
                          "outside": uniq(findings["OUTSIDE"]),
                          "inside_paths": len(uniq(findings["INSIDE"]))}, indent=2))
    else:
        print(f"sandbox : {sandbox}\nproject : {project}\ntranscripts: {counts['files']}  tool uses: {counts['tool_uses']}")
        print(f"inside  : {len(uniq(findings['INSIDE']))} distinct paths   benign off-root: {len(uniq(findings['BENIGN']))}")
        for cls in ("FORBIDDEN", "OUTSIDE"):
            rows = uniq(findings[cls])
            print(f"\n{cls}: {len(rows)}")
            for r in rows:
                print(f"  [{r['outcome']:9}] {r['path']}   via {r['how']}   ({r['file']}:{r['line']})")
        tail = {"VOID": "  — a forbidden read SUCCEEDED; the run's findings are void",
                "BREACH": "  — forbidden reads were attempted, all denied; run stands, breach recorded",
                "JUDGE": "  — judge the OUTSIDE reads", "CLEAN": ""}[verdict]
        print(f"\nVERDICT: {verdict}{tail}")
    return 0 if verdict == "CLEAN" else 1


if __name__ == "__main__":
    sys.exit(main())
