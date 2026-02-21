#!/usr/bin/env python3
import os
import pathlib
import stat
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[2]


def main():
    script = ROOT / "scripts" / "qa" / "run_sentinel_validation.sh"
    st = os.stat(script)
    assert st.st_mode & stat.S_IXUSR
    out = subprocess.check_output([str(script), "--help"], text=True)
    assert "dry-run" in out.lower()
    assert "redis-cli" in out
    print("M9 sentinel script tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
