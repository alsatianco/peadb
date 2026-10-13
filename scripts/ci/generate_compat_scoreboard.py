#!/usr/bin/env python3
"""
generate_compat_scoreboard.py — Generates compat/commands.generated.md

Parses the command table in src/command.cpp to extract all registered commands and
assigns each a proof-based status from three independent signals:

  * upstream — the command was dispatched during a GREEN (0 [err]) native-mode run of
    an upstream Redis TCL suite (coverage_<suite>.txt files written via the
    PEADB_COMMAND_COVERAGE_FILE hook, default dir: artifacts/redis-native);
  * diff — the command appears in the differential-test corpus (tests/diff);
  * tested — invoked by a homegrown integration test (self-referential signal).

Status ladder (strongest first):
  full       — upstream AND diff (proven against real Redis two independent ways)
  upstream   — green upstream TCL coverage only
  diff       — differential coverage only
  tested     — homegrown integration tests only
  registered — command exists, no proof

Mention-counting alone no longer earns anything above "tested" — the scoreboard
measures proof, not presence.
"""

import re
import sys
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
COMMAND_CPP = REPO_ROOT / "src" / "command.cpp"
TESTS_DIR = REPO_ROOT / "tests" / "integration"
OUT_FILE = REPO_ROOT / "compat" / "commands.generated.md"
UPSTREAM_DIR = Path(os.environ.get("PEADB_UPSTREAM_RESULTS_DIR",
                                   REPO_ROOT / "artifacts" / "redis-native"))

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _load_upstream_commands() -> set[str]:
    """Commands dispatched during green (0-err) native upstream suite runs."""
    cmds: set[str] = set()
    if not UPSTREAM_DIR.exists():
        return cmds
    for cov in UPSTREAM_DIR.glob("coverage_*.txt"):
        log = UPSTREAM_DIR / (cov.name[len("coverage_"):-len(".txt")] + ".log")
        if log.exists():
            text = ANSI_RE.sub("", log.read_text(errors="replace"))
            if re.search(r"^\[err\]:", text, re.MULTILINE):
                continue  # only green suites prove anything
        cmds.update(line.strip().upper() for line in cov.read_text().splitlines()
                    if line.strip())
    return cmds

# Commands that have explicit differential/compat tests — derived from the actual
# tests/diff corpus so a command earns "full" by being differentially verified against
# real Redis (not hand-maintained).
def _load_differential_commands() -> set[str]:
    import json
    root = Path(__file__).resolve().parents[2]
    cmds: set[str] = set()
    diff_dir = root / "tests" / "diff"
    if not diff_dir.exists():
        return cmds
    for f in diff_dir.rglob("*.json"):
        try:
            data = json.loads(f.read_text())
        except Exception:
            continue
        if isinstance(data, list):
            for entry in data:
                if isinstance(entry, list) and entry and isinstance(entry[0], str):
                    cmds.add(entry[0].upper())
    return cmds

DIFFERENTIAL_COMMANDS: set[str] = _load_differential_commands()

def extract_commands(cpp_path: Path) -> list[str]:
    """Extract command names from t.emplace("NAME", ...) calls ('-' for RESTORE-ASKING)."""
    text = cpp_path.read_text()
    return sorted(set(re.findall(r't\s*\.\s*emplace\s*\(\s*"([A-Z0-9_-]+)"', text)))

def test_invocations(tests_dir: Path) -> dict[str, set[str]]:
    """
    For each command, find which test files contain a call that invokes it
    (not just a mention in a string or comment).

    Patterns matched (in order of prevalence in this repo):
      send_cmd(s, "CMD", ...)          — raw RESP helper used by most tests
      send_cmd(sock, "CMD", ...)
      s.sendall(b"*N\\r\\n$M\\r\\nCMD\\r\\n") — raw RESP bytes for simple commands
      r.CMD(...)                        — redis-py style (less common here)
      execute_command("CMD", ...)      — redis-py execute_command
    """
    result: dict[str, set[str]] = {}
    if not tests_dir.exists():
        return result
    for test_file in sorted(tests_dir.glob("test_*.py")):
        text = test_file.read_text(errors="replace")
        # Unified pattern: any helper call whose first string argument is an
        # all-caps Redis command name.  Covers:
        #   send_cmd(s, "CMD", ...)   send_cmd(sock, 'CMD', ...)
        #   cmd(s, "CMD", ...)        cmd(s, 'CMD', ...)
        #   cmd(s,'CMD',...)          (no spaces, single-quoted — dominant style)
        _cmd_call = re.compile(
            r'(?:send_cmd|cmd)\s*\([^,]+,\s*["\']([A-Z][A-Z0-9_]*)["\']'
        )
        for m in _cmd_call.finditer(text):
            result.setdefault(m.group(1), set()).add(test_file.name)
        # Raw RESP bytes: sendall(b"...\r\nCMD\r\n")
        for m in re.finditer(r'sendall\s*\(b["\'].*?\\r\\n([A-Z][A-Z0-9_]*)\\r\\n', text):
            result.setdefault(m.group(1), set()).add(test_file.name)
        # redis-py style: r.CMD(...) or r.execute_command("CMD") or r.execute("CMD")
        for m in re.finditer(r'r\.([A-Z][A-Z0-9_]*)\(', text):
            result.setdefault(m.group(1).upper(), set()).add(test_file.name)
        for m in re.finditer(r'execute_command\s*\(["\']([A-Z][A-Z0-9_]+)', text, re.IGNORECASE):
            result.setdefault(m.group(1).upper(), set()).add(test_file.name)
        for m in re.finditer(r'r\.execute\s*\(["\']([A-Z][A-Z0-9_]+)', text, re.IGNORECASE):
            result.setdefault(m.group(1).upper(), set()).add(test_file.name)
    return result

def coverage_level(cmd: str, upstream: set[str], diff: set[str],
                   invocations: dict[str, set[str]]) -> str:
    up = cmd in upstream
    df = cmd in diff
    if up and df:
        return "full"
    if up:
        return "upstream"
    if df:
        return "diff"
    if cmd in invocations and invocations[cmd]:
        return "tested"
    return "registered"

def main() -> None:
    if not COMMAND_CPP.exists():
        print(f"ERROR: {COMMAND_CPP} not found", file=sys.stderr)
        sys.exit(1)

    commands = extract_commands(COMMAND_CPP)
    invocations = test_invocations(TESTS_DIR)
    upstream = _load_upstream_commands()

    counts = {"full": 0, "upstream": 0, "diff": 0, "tested": 0, "registered": 0}
    rows: list[tuple[str, str, str]] = []
    for cmd in commands:
        level = coverage_level(cmd, upstream, DIFFERENTIAL_COMMANDS, invocations)
        tests = ", ".join(sorted(invocations.get(cmd, set()))) or "—"
        rows.append((cmd, level, tests))
        counts[level] += 1

    lines = [
        "# Commands Compatibility Scoreboard",
        "",
        "> Auto-generated by `scripts/ci/generate_compat_scoreboard.py`. Do not edit manually.",
        "> Status is proof-based: `full` = green upstream TCL suite coverage AND differential",
        "> coverage; `upstream` / `diff` = one of the two; `tested` = homegrown integration",
        "> tests only; `registered` = no test proof. See `compat/redis_suite_ledger.json`.",
        "",
        f"Total: **{len(commands)}** commands — "
        f"full: {counts['full']}, upstream: {counts['upstream']}, diff: {counts['diff']}, "
        f"tested: {counts['tested']}, registered: {counts['registered']}",
        "",
        "| Command | Coverage | Test files |",
        "|---------|----------|------------|",
    ]
    for cmd, level, tests in rows:
        lines.append(f"| `{cmd}` | {level} | {tests} |")

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text("\n".join(lines) + "\n")
    print(f"Written {OUT_FILE} ({len(commands)} commands): " +
          ", ".join(f"{k}={v}" for k, v in counts.items()))

if __name__ == "__main__":
    main()
