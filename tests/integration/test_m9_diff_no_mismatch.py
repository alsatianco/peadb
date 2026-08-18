#!/usr/bin/env python3
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from _redis_path import redis_server_path  # noqa: E402


def main():
    if redis_server_path() is None:
        print("SKIP: redis-server not found (install redis-server or build third_party/redis)")
        return 0
    cmd = [sys.executable, str(ROOT / "tests" / "diff" / "run_diff_tests.py"), "--case", "tests/diff/basic/ping_echo_quit.json"]
    subprocess.check_call(cmd, cwd=ROOT)
    print("M9 differential no-mismatch tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
