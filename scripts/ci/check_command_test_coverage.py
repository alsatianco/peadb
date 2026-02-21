#!/usr/bin/env python3
"""check_command_test_coverage.py -- CI gate for command/test linkage.

Purpose
    Prevents new commands from being added to ``src/command.cpp`` without at
    least one corresponding mention in the ``tests/`` tree.  This acts as a
    lightweight "did you write a test?" gate -- it does NOT verify behavioural
    correctness, only that each command name appears somewhere in test files.

What it does
    1. Parses ``src/command.cpp`` for command registrations of the form
       ``t.emplace("COMMAND_NAME", ...)``, extracting all uppercase command
       names (e.g. SET, HGETALL, XADD).
    2. Reads every file under ``tests/`` (binary-safe; decoding errors are
       suppressed) and concatenates their text.
    3. For each extracted command name, searches for a whole-word match
       (\bCOMMAND_NAME\b) in the concatenated test text.
    4. Reports any command names that have NO match at all.

How to run
    From the repo root:

        python3 scripts/ci/check_command_test_coverage.py

    No arguments or environment variables are required.

Prerequisites
    - Python 3.9+
    - ``src/command.cpp`` and the ``tests/`` directory must exist.

Interpreting the output
    Success (all commands referenced in tests):
        Command test coverage OK (142 commands referenced)
        (exit code 0)

    Failure (some commands are missing from tests):
        Missing test references for commands:
         - XAUTOCLAIM
         - OBJECT
        (exit code 1)

    Action: add at least one integration or unit test that exercises each
    reported command, then re-run.

Notes / limitations
    - A word-boundary match counts (even inside comments), so false-negatives
      are rare, but false-positives (a command name appearing in a comment
      without a real test) are possible.  Treat this as a canary, not as
      proof of thorough coverage.
    - Only uppercase [A-Z0-9_]+ names registered via t.emplace(...) are
      considered.
"""
from __future__ import annotations

import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]


def main() -> int:
    cmd_src = (ROOT / 'src/command.cpp').read_text(encoding='utf-8', errors='ignore')
    commands = sorted(set(re.findall(r't\.emplace\("([A-Z0-9_]+)"', cmd_src)))

    texts = []
    for p in ROOT.joinpath('tests').rglob('*'):
        if p.is_file():
            texts.append(p.read_text(encoding='utf-8', errors='ignore'))
    all_text = '\n'.join(texts)

    missing = []
    for c in commands:
        if re.search(r'\b' + re.escape(c) + r'\b', all_text) is None:
            missing.append(c)

    if missing:
        print('Missing test references for commands:')
        for m in missing:
            print(' -', m)
        return 1

    print(f'Command test coverage OK ({len(commands)} commands referenced)')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
