#!/usr/bin/env python3
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]


def main():
    doc = ROOT / "docs" / "decisions" / "m8-modules-abi.md"
    txt = doc.read_text(encoding="utf-8")
    assert "Redis 7.2.5" in txt
    assert "RedisModule_OnLoad" in txt
    assert "RedisModule_OpenKey" in txt
    assert "RedisModule_StringSet" in txt
    assert "RedisModule_StringDMA" in txt
    assert "MODULE LOAD" in txt
    print("M8 ABI plan tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
