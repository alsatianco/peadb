#!/usr/bin/env python3
import os
import pathlib
import stat
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[2]


def main():
    script = ROOT / "scripts" / "qa" / "run_stability_checks.sh"
    st = os.stat(script)
    assert st.st_mode & stat.S_IXUSR
    out = subprocess.check_output([str(script), "--help"], text=True)
    assert "--seconds" in out
    assert "asan" in out.lower()
    assert "ubsan" in out.lower()
    print("M9 stability script tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
