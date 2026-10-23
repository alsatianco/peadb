#!/usr/bin/env python3
"""Monotonic upstream-suite ratchet: green suites can never silently regress.

Compares the latest runtest results (``pass_rate.json`` from
``scripts/redis/parse_tcl_results.py``) against the checked-in ledger
``compat/redis_suite_ledger.json`` and fails when:

  1. any adopted suite has ``err`` > 0;
  2. a suite's ``ok`` count dropped below the ledger baseline;
  3. a suite's skipfile grew beyond the ledger baseline;

unless ``compat/delta.md`` is touched in the same change (regressions must be
consciously registered as deltas, never silent) — detected via uncommitted /
staged git changes, or forced with ``RATCHET_ALLOW_REGRESSION=1``.

Improvements (more ok, fewer skipfile lines, newly adopted suites) always pass;
run with ``--update`` to fold them back into the ledger. Timed-suite counts
use the parser's documented normalization; raw repetitions do not raise their
baselines. A counting-policy mismatch always fails, even with a delta update.

Usage:
    check_suite_ratchet.py [--results artifacts/redis-parity/pass_rate.json]
                           [--ledger compat/redis_suite_ledger.json] [--update]
"""
import argparse
import json
import os
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]


def delta_md_touched() -> bool:
    if os.environ.get("RATCHET_ALLOW_REGRESSION") == "1":
        return True
    try:
        out = subprocess.run(["git", "status", "--porcelain", "compat/delta.md"],
                             cwd=ROOT, capture_output=True, text=True, check=True).stdout
        return bool(out.strip())
    except Exception:
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--results", type=pathlib.Path,
                    default=ROOT / "artifacts/redis-parity/pass_rate.json")
    ap.add_argument("--ledger", type=pathlib.Path,
                    default=ROOT / "compat/redis_suite_ledger.json")
    ap.add_argument("--update", action="store_true",
                    help="write current counts back into the ledger (adoption/improvement)")
    args = ap.parse_args()

    if not args.results.exists():
        print(f"ratchet: no results at {args.results} — run the TCL stage first", file=sys.stderr)
        return 2
    results = json.loads(args.results.read_text())
    ledger = json.loads(args.ledger.read_text())

    failures = []
    escape_ok = delta_md_touched()

    for suite, entry in sorted(ledger["suites"].items()):
        if entry.get("mode") == "excluded":
            continue
        got = results["suites"].get(suite)
        if got is None:
            failures.append(f"{suite}: adopted in ledger but missing from results")
            continue
        if got.get("counting_policy") != entry.get("counting_policy"):
            failures.append(f"{suite}: counting policy differs from ledger "
                            f"({entry.get('counting_policy')} -> {got.get('counting_policy')})")
        if got["err"] > 0:
            failures.append(f"{suite}: {got['err']} failing test(s) — adopted suites must stay green")
        base = entry.get("last_counts") or {}
        if base.get("ok") is not None and got["ok"] < base["ok"] and not escape_ok:
            failures.append(f"{suite}: ok count regressed {base['ok']} -> {got['ok']} "
                            "(requires a compat/delta.md update in the same change)")
        base_skip = entry.get("skipfile_lines")
        if base_skip is not None and got["skipfile_lines"] > base_skip and not escape_ok:
            failures.append(f"{suite}: skipfile grew {base_skip} -> {got['skipfile_lines']} "
                            "(requires a compat/delta.md update in the same change)")

    if args.update and not failures:
        for suite, entry in ledger["suites"].items():
            got = results["suites"].get(suite)
            if entry.get("mode") != "excluded" and got is not None:
                entry["last_counts"] = {"ok": got["ok"], "err": got["err"], "ignored": got["ignored"]}
                entry["skipfile_lines"] = got["skipfile_lines"]
        args.ledger.write_text(json.dumps(ledger, indent=2, sort_keys=True) + "\n")
        print(f"ratchet: ledger updated ({args.ledger})")

    if failures:
        print("suite ratchet FAILED:", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        return 1
    print(f"suite ratchet OK — pass rate {results['pass_rate']:.1%}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
