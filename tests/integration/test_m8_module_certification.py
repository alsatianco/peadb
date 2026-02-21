#!/usr/bin/env python3
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]


def main():
    suite = [
        ROOT / "tests" / "integration" / "test_m8_module_load.py",
        ROOT / "tests" / "integration" / "test_m8_module_key_api.py",
        ROOT / "tests" / "integration" / "test_m8_module_command_api.py",
    ]
    for t in suite:
        subprocess.check_call([sys.executable, str(t)])
    print("M8 module certification smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
