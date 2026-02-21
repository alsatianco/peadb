#!/usr/bin/env python3
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]


def main() -> int:
    decisions = ROOT / "docs/decisions/d0-decisions.md"
    delta = ROOT / "compat/delta.md"
    redis = ROOT / "third_party/redis"

    assert decisions.exists(), "missing docs/decisions/d0-decisions.md"
    assert delta.exists(), "missing compat/delta.md"

    content = decisions.read_text(encoding="utf-8")
    assert "7.2.5" in content, "Redis pin not documented"

    if not redis.exists():
        print("SKIP: third_party/redis not present — git-SHA check skipped")
    else:
        head = subprocess.check_output(["git", "-C", str(redis), "rev-parse", "HEAD"], text=True).strip()
        assert re.fullmatch(r"[0-9a-f]{40}", head), "invalid redis HEAD"
        assert head == "f60370ce28b946c1146dcea77c9c399d39601aaa", f"unexpected redis HEAD: {head}"

    print("D0 artifacts verified")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AssertionError as e:
        print(f"FAIL: {e}", file=sys.stderr)
        raise SystemExit(1)
