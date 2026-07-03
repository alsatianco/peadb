#!/usr/bin/env python3
"""Parse upstream Redis runtest logs into per-suite pass/fail counts.

Reads every ``*.log`` in a log directory (as written by
``scripts/redis/run_redis_tests.sh``), strips ANSI colour codes, and counts the
``[ok]:`` / ``[err]:`` / ``[ignore]:`` / ``[skip]:`` test-result lines the Tcl
harness prints. Each log file maps back to its suite name
(``unit_type_string.log`` -> ``unit/type/string``).

Outputs (both optional, see flags):
  * a JSON report: per-suite counts + totals + the headline pass-rate
    ``sum(ok) / sum(ok + err + skipfile_lines)`` — skipfile lines count against
    the rate so skipping a test never *improves* the number;
  * a Markdown summary table.

Usage:
    parse_tcl_results.py LOG_DIR [--skipfiles-dir DIR]
                         [--out-json PATH] [--out-md PATH]

Exit code 0 unless no logs were found (2).
"""
import argparse
import json
import pathlib
import re
import sys

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
RESULT_RE = re.compile(r"^\[(ok|err|skip|ignore)\]:", re.MULTILINE)


def suite_from_log_name(name: str) -> str:
    return name[: -len(".log")].replace("_", "/")


def count_results(log_path: pathlib.Path) -> dict:
    text = ANSI_RE.sub("", log_path.read_text(errors="replace"))
    counts = {"ok": 0, "err": 0, "ignored": 0}
    for m in RESULT_RE.finditer(text):
        kind = m.group(1)
        if kind == "ok":
            counts["ok"] += 1
        elif kind == "err":
            counts["err"] += 1
        else:
            counts["ignored"] += 1
    counts["exceptions"] = text.count("exception")
    return counts


def skipfile_lines(skipdir: pathlib.Path, suite: str) -> int:
    f = skipdir / (suite.replace("/", "_") + ".skip")
    if not f.exists():
        return 0
    return sum(1 for line in f.read_text().splitlines()
               if line.strip() and not line.strip().startswith("#"))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("log_dir", type=pathlib.Path)
    ap.add_argument("--skipfiles-dir", type=pathlib.Path,
                    default=pathlib.Path(__file__).resolve().parent / "skipfiles")
    ap.add_argument("--out-json", type=pathlib.Path)
    ap.add_argument("--out-md", type=pathlib.Path)
    args = ap.parse_args()

    logs = sorted(p for p in args.log_dir.glob("*.log")
                  if p.name not in ("peadb-start.log",))
    if not logs:
        print(f"no runtest logs found in {args.log_dir}", file=sys.stderr)
        return 2

    suites = {}
    for log in logs:
        suite = suite_from_log_name(log.name)
        counts = count_results(log)
        counts["skipfile_lines"] = skipfile_lines(args.skipfiles_dir, suite)
        suites[suite] = counts

    tot_ok = sum(s["ok"] for s in suites.values())
    tot_err = sum(s["err"] for s in suites.values())
    tot_skipfile = sum(s["skipfile_lines"] for s in suites.values())
    denominator = tot_ok + tot_err + tot_skipfile
    pass_rate = (tot_ok / denominator) if denominator else 0.0

    report = {
        "suites": suites,
        "totals": {"ok": tot_ok, "err": tot_err, "skipfile_lines": tot_skipfile},
        "pass_rate": round(pass_rate, 4),
    }

    if args.out_json:
        args.out_json.parent.mkdir(parents=True, exist_ok=True)
        args.out_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")

    lines = ["| Suite | ok | err | ignored | skipfile |",
             "|-------|----|-----|---------|----------|"]
    for suite in sorted(suites):
        s = suites[suite]
        lines.append(f"| {suite} | {s['ok']} | {s['err']} | {s['ignored']} | {s['skipfile_lines']} |")
    lines.append(f"\n**Pass rate: {pass_rate:.1%}** (ok={tot_ok}, err={tot_err}, skipfile={tot_skipfile})")
    md = "\n".join(lines)
    if args.out_md:
        args.out_md.parent.mkdir(parents=True, exist_ok=True)
        args.out_md.write_text(md + "\n")
    print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
