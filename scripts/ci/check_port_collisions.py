#!/usr/bin/env python3
"""check_port_collisions.py -- ratchet check for shared ports between integration tests.

Purpose
    Every ``tests/integration/test_*.py`` file starts its own ``peadb-server``
    subprocess (or two, for replication/cluster tests) and tears it down again
    in a ``finally`` block. ``scripts/ci/run_integration_tests.sh`` runs those
    files strictly sequentially, so two files reusing the same TCP port only
    breaks when one file's server has not actually released the port yet when
    the next file starts (a slow shutdown, or -- the case that motivated this
    check -- a straggler process the previous test failed to reap; see that
    supervisor's process-group cleanup). That failure mode is
    confusing: a ``ConnectionRefusedError`` or hang on an unrelated test, not
    a clear "port already in use" message.

    This script does not, and cannot, prove two tests will never collide --
    only ``run_integration_tests.sh``'s straggler reaping and each test's own
    cleanup do that. What this tracks is exposure: how many pairs of files are
    one straggler away from a flaky failure. A scan of the tree at the time
    this check was written found ~20 such pairs, all pre-existing. Renumbering
    a hundred files to remove them is out of proportion to the risk, so this
    is a **ratchet**, not a hard rule: collisions already in the checked-in
    baseline (``scripts/ci/port_collision_baseline.json``) are grandfathered
    forever, but a *new* collision -- a new test file picking a port some
    other file already uses -- fails CI. This mirrors ``check_suite_ratchet.py``'s
    shape exactly: a baseline that can only improve except via a deliberate,
    explicit override.

What it does
    1. Scans every ``tests/integration/test_*.py`` file for 4-digit integer
       literals in the 6000-6999 range -- the port convention this suite
       already uses exclusively (verified by scanning the tree). A port
       passed only through a variable (``'--port', str(port)``) is still
       found, because the same literal also appears as a plain assignment
       somewhere in the file (``port = 6488``, ``PORT = 6574``, ...); this
       script does not otherwise chase indirection.
    2. Groups files by shared port to build the current collision set: any
       port referenced by two or more files.
    3. Compares that against the checked-in baseline. A port collision is
       **new** if the port did not collide in the baseline at all, or if a
       file not previously recorded for that port has joined its colliding
       set (an existing collision's blast radius just grew).
    4. Shrinking or resolving a collision (a port drops to 0-1 files) always
       passes silently; run with ``--update`` to fold that improvement back
       into the baseline.

How to run
    From the repo root:

        python3 scripts/ci/check_port_collisions.py

    A reported new collision fails the check on purpose -- pick a different
    port instead where possible. If it is genuinely fine (e.g. the two tests
    can never run concurrently with each other for independent reasons),
    accept it explicitly and fold it into the baseline in the same commit:

        PORT_COLLISION_ALLOW=1 python3 scripts/ci/check_port_collisions.py --update

    ``--update`` alone (without the env var) only ever records *improvements*
    -- it refuses to write while there are unresolved new collisions, the
    same way ``check_suite_ratchet.py --update`` refuses to fold in an
    unacknowledged regression.

Prerequisites
    - Python 3.9+
    - ``tests/integration/`` and ``scripts/ci/port_collision_baseline.json``
      must exist.

Interpreting the output
    Success (no new collisions):
        port collision check OK: heuristic scan of literal 6xxx ports in
        tests/integration/test_*.py (21 known collisions, 0 new)
        (exit code 0)

    Failure (a new collision):
        port collision check FAILED:
          - port 6500 newly collides: test_p6_foo.py joins test_b1_security.py
        (exit code 1)

    Action: pick a port scripts/ci/check_port_collisions.py doesn't already
    report as taken, or deliberately accept it as described above.

Notes / limitations
    - This is a literal-number scan, not a real parse of subprocess argv, so
      a literal in the 6000-6999 range that is not actually a port (e.g. it
      happens to appear in a comment or an unrelated assertion) can produce a
      false "collision" -- the same kind of false positive
      ``check_command_test_coverage.py``'s word-boundary scan accepts.
      Treat a report as a strong signal to double-check, not infallible
      proof.
    - Only ports written as plain 4-digit literals are found. A port
      constructed at runtime (e.g. ``BASE_PORT + i`` in a loop) is only
      caught via its ``BASE_PORT`` literal, not every derived value.
    - A port outside 6000-6999 is invisible to this check. That is a known
      limitation, not a silent gap: every port in this suite follows that
      convention today (matching Redis's own default port, 6379), so a test
      author picking a different range is already off the beaten path.
    - Only ``tests/integration/test_*.py`` is scanned. Other suites (unit
      tests, the differential harness, the Redis TCL bridge) use their own
      port allocation and are out of scope here.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
TESTS_DIR = ROOT / "tests" / "integration"
DEFAULT_BASELINE = pathlib.Path(__file__).resolve().parent / "port_collision_baseline.json"

# The port convention this suite already uses exclusively (verified by scanning
# the tree): a bare 4-digit literal starting with 6, not embedded in a longer
# number (e.g. not the "6" in "16384").
PORT_RE = re.compile(r"(?<!\d)(6[0-9]{3})(?!\d)")

BASELINE_COMMENT = (
    "Baseline of known TCP-port collisions between tests/integration/test_*.py files, "
    "generated by scripts/ci/check_port_collisions.py --update. A collision here means "
    "two or more test files both reference the same 6000-6999 port literal, almost "
    "always because each starts its own peadb-server on that port. This is tolerated "
    "only because run_integration_tests.sh runs files strictly sequentially and each "
    "test tears down its own server before the next one starts (see that script's "
    "straggler-reaping comment for the one place that assumption can briefly break). "
    "Entries here are grandfathered and do not fail CI; check_port_collisions.py fails "
    "only when a *new* collision -- a port not listed here at all, or a new file "
    "joining an already-listed port's set -- shows up, so a developer adding a "
    "colliding port to a new test finds out from CI without a hundred existing files "
    "needing to be renumbered."
)


def scan_ports() -> dict[str, set[str]]:
    """Return {filename: {port_str, ...}} for every tests/integration/test_*.py file."""
    file_ports: dict[str, set[str]] = {}
    for path in sorted(TESTS_DIR.glob("test_*.py")):
        text = path.read_text(encoding="utf-8", errors="ignore")
        ports = set(PORT_RE.findall(text))
        if ports:
            file_ports[path.name] = ports
    return file_ports


def collisions_from(file_ports: dict[str, set[str]]) -> dict[str, list[str]]:
    """Return {port: sorted [file, ...]} for every port used by 2+ files."""
    port_files: dict[str, set[str]] = {}
    for fname, ports in file_ports.items():
        for port in ports:
            port_files.setdefault(port, set()).add(fname)
    return {port: sorted(files) for port, files in port_files.items() if len(files) > 1}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--baseline", type=pathlib.Path, default=DEFAULT_BASELINE)
    ap.add_argument("--update", action="store_true",
                    help="fold the current collision set into the baseline (only takes "
                         "effect when there is no unresolved new collision, unless "
                         "PORT_COLLISION_ALLOW=1 is also set)")
    args = ap.parse_args()

    if not TESTS_DIR.is_dir():
        print(f"port collision check: no such directory {TESTS_DIR}", file=sys.stderr)
        return 2

    current = collisions_from(scan_ports())

    baseline_doc: dict = {"comment": BASELINE_COMMENT, "collisions": {}}
    if args.baseline.exists():
        baseline_doc = json.loads(args.baseline.read_text())
    baseline = {port: set(files) for port, files in baseline_doc.get("collisions", {}).items()}

    escape_ok = os.environ.get("PORT_COLLISION_ALLOW") == "1"

    failures = []
    new_collision_ports = 0
    for port, files in sorted(current.items()):
        newcomers = sorted(set(files) - baseline.get(port, set()))
        if not newcomers:
            continue
        new_collision_ports += 1
        if escape_ok:
            continue
        still_known = sorted(set(files) - set(newcomers))
        if still_known:
            failures.append(f"port {port} newly collides: {', '.join(newcomers)} "
                            f"joins {', '.join(still_known)}")
        else:
            failures.append(f"port {port} newly collides: {' with '.join(newcomers)}")

    if args.update and not failures:
        baseline_doc["collisions"] = dict(sorted(current.items()))
        baseline_doc["comment"] = BASELINE_COMMENT
        args.baseline.write_text(json.dumps(baseline_doc, indent=2, sort_keys=True) + "\n")
        print(f"port collision check: baseline updated ({args.baseline})")

    if failures:
        print("port collision check FAILED:", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        print("", file=sys.stderr)
        print("Pick a port scripts/ci/check_port_collisions.py doesn't already report as "
              "taken, or see the module docstring for how to deliberately accept a new "
              "collision.", file=sys.stderr)
        return 1

    print(f"port collision check OK: heuristic scan of literal 6xxx ports in "
          f"tests/integration/test_*.py ({len(current)} known collisions, "
          f"{new_collision_ports} new)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
