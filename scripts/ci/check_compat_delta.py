#!/usr/bin/env python3
"""check_compat_delta.py — CI validation for ``compat/delta.md`` entries.

Purpose
    Every compatibility delta (i.e. a known behavioural difference between
    PeaDB and upstream Redis) must be tracked in ``compat/delta.md`` with a
    consistent set of metadata fields.  This script enforces that requirement
    so that incomplete entries don't slip past code review.

What it does
    1. Reads ``compat/delta.md`` from the repo root.
    2. Splits the file on numbered-list boundaries (``1. …``, ``2. …``, …) to
       identify individual delta sections.
    3. For each section, checks that **all** required metadata bullets are
       present (case-sensitive, prefix-matched):
         - ``- Status:``
         - ``- Owner:``
         - ``- Severity:``
         - ``- Target milestone:``
         - ``- Implemented:``
         - ``- Missing:``
    4. Reports any missing fields and exits accordingly.

How to run
    From the repo root:

        python3 scripts/ci/check_compat_delta.py

    Or directly (the script is executable):

        ./scripts/ci/check_compat_delta.py

    No arguments or environment variables are required.

Prerequisites
    - Python 3.9+
    - The file ``compat/delta.md`` must exist at the repo root.

Interpreting the output
    Success:
        compat/delta.md validation passed
        (exit code 0)

    Failure — missing fields:
        compat/delta.md validation failed:
         - 3. SUBSCRIBE partial support: missing field - Owner:
         - 3. SUBSCRIBE partial support: missing field - Severity:
        (exit code 1)

    Failure — empty file (no numbered sections found):
        No compatibility delta entries found
        (exit code 1)
"""
import pathlib
import re
import sys


def main() -> int:
    root = pathlib.Path(__file__).resolve().parents[2]
    path = root / "compat" / "delta.md"
    txt = path.read_text(encoding="utf-8")

    # Split into numbered sections ("1. ", "2. ", ...).
    parts = re.split(r"\n(?=\d+\.\s)", txt)
    sections = [p for p in parts if re.match(r"^\d+\.\s", p.strip())]
    if not sections:
        print("No compatibility delta entries found", file=sys.stderr)
        return 1

    required = ["- Status:", "- Owner:", "- Severity:", "- Target milestone:", "- Implemented:", "- Missing:"]
    errors = []
    for s in sections:
        header = s.strip().splitlines()[0]
        for field in required:
            if field not in s:
                errors.append(f"{header}: missing field {field}")

    if errors:
        print("compat/delta.md validation failed:", file=sys.stderr)
        for e in errors:
            print(f" - {e}", file=sys.stderr)
        return 1

    print("compat/delta.md validation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
