#!/usr/bin/env python3
import pathlib
import shutil
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]


def _have_redis_server() -> bool:
    local = ROOT / "third_party/redis/src/redis-server"
    if local.exists():
        return True
    return shutil.which("redis-server") is not None


def main():
    if not _have_redis_server():
        print("SKIP: redis-server not found (install redis-server or build third_party/redis)")
        return 0
    cmd = [sys.executable, str(ROOT / "tests" / "diff" / "run_diff_tests.py"), "--case", "tests/diff/basic/ping_echo_quit.json"]
    subprocess.check_call(cmd, cwd=ROOT)
    print("M9 differential no-mismatch tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
