#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import pathlib
import sys
from typing import Any

from harness.resp import RespError, roundtrip
from harness.servers import start_peadb, start_redis


def normalize(v: Any) -> Any:
    if isinstance(v, RespError):
        return {"error": v.message}
    if isinstance(v, list):
        return [normalize(x) for x in v]
    if isinstance(v, dict):
        return {str(k): normalize(val) for k, val in v.items()}
    return v


def compare_state(host_a: str, port_a: int, host_b: str, port_b: int) -> list[str]:
    mismatches: list[str] = []

    keys_a = sorted(roundtrip(host_a, port_a, ["KEYS", "*"]) or [])
    keys_b = sorted(roundtrip(host_b, port_b, ["KEYS", "*"]) or [])

    if normalize(keys_a) != normalize(keys_b):
        mismatches.append(f"keys mismatch: {normalize(keys_a)} != {normalize(keys_b)}")
        return mismatches

    for key in (keys_a or []):
        ta = normalize(roundtrip(host_a, port_a, ["TYPE", key]))
        tb = normalize(roundtrip(host_b, port_b, ["TYPE", key]))
        if ta != tb:
            mismatches.append(f"type mismatch for key {key!r}: {ta} != {tb}")

        ttla = normalize(roundtrip(host_a, port_a, ["PTTL", key]))
        ttlb = normalize(roundtrip(host_b, port_b, ["PTTL", key]))
        if isinstance(ttla, int) and isinstance(ttlb, int):
            # Allow slight drift if both are expiring keys.
            if ttla >= 0 and ttlb >= 0 and abs(ttla - ttlb) > 50:
                mismatches.append(f"PTTL drift for key {key!r}: {ttla} != {ttlb}")
            elif (ttla < 0) != (ttlb < 0):
                mismatches.append(f"PTTL class mismatch for key {key!r}: {ttla} != {ttlb}")
        elif ttla != ttlb:
            mismatches.append(f"PTTL mismatch for key {key!r}: {ttla} != {ttlb}")

    return mismatches


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", default="tests/diff/basic/ping_echo_quit.json")
    parser.add_argument("--redis-port", type=int, default=6392)
    parser.add_argument("--peadb-port", type=int, default=6393)
    args = parser.parse_args()

    root = pathlib.Path(__file__).resolve().parents[2]
    commands = json.loads((root / args.case).read_text(encoding="utf-8"))

    redis = start_redis(root, args.redis_port)
    peadb = start_peadb(root, args.peadb_port)

    mismatches: list[str] = []
    repro_lines: list[str] = []

    try:
      for cmd in commands:
        repro_lines.append(" ".join(cmd))
        r1 = normalize(roundtrip(redis.host, redis.port, cmd))
        r2 = normalize(roundtrip(peadb.host, peadb.port, cmd))
        # KEYS returns elements in arbitrary order; sort for comparison.
        if cmd and cmd[0].upper() == "KEYS":
            if isinstance(r1, list):
                r1 = sorted(r1, key=str)
            if isinstance(r2, list):
                r2 = sorted(r2, key=str)
        if r1 != r2:
            mismatches.append(f"reply mismatch for {cmd}: redis={r1} peadb={r2}")

      mismatches.extend(compare_state(redis.host, redis.port, peadb.host, peadb.port))
    finally:
      redis.stop()
      peadb.stop()

    if mismatches:
        out = root / "tests/diff/last_repro.txt"
        out.write_text("\n".join(repro_lines) + "\n", encoding="utf-8")
        print("FAIL: differential mismatches found", file=sys.stderr)
        for m in mismatches:
            print(f" - {m}", file=sys.stderr)
        print(f"Repro saved to {out}", file=sys.stderr)
        return 1

    print("Differential tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
