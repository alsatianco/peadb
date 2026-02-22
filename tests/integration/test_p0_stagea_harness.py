#!/usr/bin/env python3
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]


def main():
    runner = (ROOT / "scripts" / "redis" / "run_redis_tests.sh").read_text(encoding="utf-8")
    for suite in ["unit/keyspace", "unit/type/string", "unit/expire", "unit/multi", "unit/scripting"]:
        assert suite in runner
    assert "repro_commands.txt" in runner
    assert "failed_suites.txt" in runner

    ci_path = ROOT / ".github" / "workflows" / "ci.yml"
    if ci_path.exists():
        ci = ci_path.read_text(encoding="utf-8")
        assert "scripts/ci/check_compat_delta.py" in ci
    else:
        print("P0 note: .github/workflows/ci.yml not present, skipping CI workflow assertion")

    delta = (ROOT / "compat" / "delta.md").read_text(encoding="utf-8")
    assert "- Owner:" in delta
    assert "- Severity:" in delta
    assert "- Target milestone:" in delta
    print("P0 stage-a harness tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
