#!/usr/bin/env python3
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]


def main():
    wf = ROOT / ".github" / "workflows" / "nightly.yml"
    txt = wf.read_text(encoding="utf-8")
    assert "cron:" in txt
    assert "tests/test_helper.tcl" in txt
    assert "--single unit/" not in txt
    assert "artifacts" in txt.lower()
    print("M9 nightly workflow tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
