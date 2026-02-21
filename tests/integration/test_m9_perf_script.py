#!/usr/bin/env python3
import os
import pathlib
import stat
import subprocess
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[2]


def main():
    script = ROOT / "scripts" / "qa" / "run_perf_baseline.sh"
    st = os.stat(script)
    assert st.st_mode & stat.S_IXUSR
    with tempfile.TemporaryDirectory(prefix="m9perf_") as td:
        out = pathlib.Path(td) / "perf.csv"
        subprocess.check_call([str(script), "--dry-run", "--output", str(out)])
        txt = out.read_text(encoding="utf-8")
        assert "metric,value" in txt
    print("M9 perf script tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
