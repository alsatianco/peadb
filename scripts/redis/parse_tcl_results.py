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
    parse_tcl_results.py LOG_DIR [--skipfiles-dir DIR] [--require-complete]
                         [--out-json PATH] [--out-md PATH]

Exit code 0 on success, 2 when no logs were found, or 1 when
--require-complete finds an unfinished, failing or exceptional suite. Without
that flag, partial/failing logs can still be parsed for diagnostics, except
PSYNC2: its timed-cycle accounting always requires complete, recognized output.
PSYNC2 credits one ten-result cycle plus its two final checks (ok=12); raw_ok
and cycles preserve the observed work without inflating parity for faster runs.
"""
import argparse
import json
import pathlib
import re
import sys

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
RESULT_RE = re.compile(r"^\[(ok|err|skip|ignore)\]:", re.MULTILINE)
PSYNC2_POLICY = "psync2-one-cycle-v1"


def psync2_counts(text: str, counts: dict) -> dict:
    """Credit one whole cycle plus the two final checks, retaining every raw pass.

    Redis 7.2.5's unmodified psync2.tcl runs as many ten-result cycles as fit
    into 40 seconds. Every cycle must still have its complete expected shape;
    an unknown/missing result requires reviewing this pinned counting policy.
    """
    def invalid():
        raise ValueError("incomplete or unexpected PSYNC2 results for " + PSYNC2_POLICY)

    if ("All tests passed without errors!" not in text or counts["err"]
            or re.search(r"^\[exception\]:", text, re.MULTILINE)):
        invalid()
    names = re.findall(r"^\[ok\]: (.*?)(?: \(\d+ ms\))?$", text, re.MULTILINE)
    if len(names) != counts["ok"]:
        invalid()
    finals = ["PSYNC2: Bring the master back again for next test",
              "PSYNC2: Partial resync after restart using RDB aux fields"]
    if names[-2:] != finals or (len(names) - 2) % 10 or len(names) < 12:
        invalid()
    cycles = (len(names) - 2) // 10
    for i in range(cycles):
        group = names[i * 10:(i + 1) * 10]
        if group[0] != f"PSYNC2: --- CYCLE {i + 1} ---":
            invalid()
        master = re.fullmatch(r"PSYNC2: \[NEW LAYOUT\] Set #([0-4]) as master", group[1])
        replicas = [re.fullmatch(r"PSYNC2: Set #([0-4]) to replicate from #([0-4])", name)
                    for name in group[2:6]]
        if not master or not all(replicas):
            invalid()
        if {r[1] for r in replicas} != set("01234") - {master[1]}:
            invalid()
        if (group[6] != "PSYNC2: cluster is consistent after failover"
                or group[7] != "PSYNC2: generate load while killing replication links"
                or not re.fullmatch(r"PSYNC2: cluster is consistent after load \(x = \d+\)",
                                    group[8])
                or group[9] != "PSYNC2: total sum of full synchronizations is exactly 4"):
            invalid()
    # Skipping any cycle phase must not be hidden by the normalized count.
    if re.search(r"^\[(skip|ignore)\]: PSYNC2:", text, re.MULTILINE):
        invalid()
    return dict(counts, raw_ok=counts["ok"], ok=12, cycles=cycles,
                counting_policy=PSYNC2_POLICY)


def suite_from_log_name(name: str) -> str:
    return name[: -len(".log")].replace("_", "/")


def count_results(log_path: pathlib.Path, require_complete: bool = False) -> dict:
    text = ANSI_RE.sub("", log_path.read_text(errors="replace"))
    if require_complete and (
        "All tests passed without errors!" not in text
        or re.search(r"^\[(err|exception)\]:", text, re.MULTILINE)
    ):
        raise ValueError(f"incomplete or failed runtest log: {log_path}")
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
    if suite_from_log_name(log_path.name) == "integration/psync2":
        counts = psync2_counts(text, counts)
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
    ap.add_argument("--require-complete", action="store_true",
                    help="refuse logs without a successful harness completion or with errors")
    args = ap.parse_args()

    logs = sorted(p for p in args.log_dir.glob("*.log")
                  if p.name not in ("peadb-start.log",))
    if not logs:
        print(f"no runtest logs found in {args.log_dir}", file=sys.stderr)
        return 2

    suites = {}
    for log in logs:
        suite = suite_from_log_name(log.name)
        try:
            counts = count_results(log, args.require_complete)
        except ValueError as exc:
            print(exc, file=sys.stderr)
            return 1
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
    notes = []
    for suite in sorted(suites):
        s = suites[suite]
        lines.append(f"| {suite} | {s['ok']} | {s['err']} | {s['ignored']} | {s['skipfile_lines']} |")
        if "counting_policy" in s:
            notes.append(f"{suite}: ok={s['ok']} under `{s['counting_policy']}`; "
                         f"raw_ok={s['raw_ok']} across {s['cycles']} complete cycles.")
    lines.append(f"\n**Pass rate: {pass_rate:.1%}** (ok={tot_ok}, err={tot_err}, skipfile={tot_skipfile})")
    lines.extend("\n" + note for note in notes)
    md = "\n".join(lines)
    if args.out_md:
        args.out_md.parent.mkdir(parents=True, exist_ok=True)
        args.out_md.write_text(md + "\n")
    print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
