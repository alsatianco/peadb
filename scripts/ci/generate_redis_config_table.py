#!/usr/bin/env python3
"""Generate the table of Redis 7.2.5 configuration settings from the pinned source.

The config file and command-line loader (``src/config.cpp``) must accept exactly the settings Redis accepts, check their values the way Redis does, and refuse everything else with Redis's own errors. Rather than hand-copy 180+ names, aliases, types, numeric ranges and enum values, we read them from the ``static_configs[]`` table in the pinned ``src/config.c`` (plus its ``configEnum`` arrays).

Output: ``include/redis_config_table.generated.hpp``. Re-run after bumping the Redis pin and commit the regenerated header.

Usage:
    REDIS_DIR=~/.cache/peadb/redis-7.2.5 python3 scripts/ci/generate_redis_config_table.py
    (REDIS_DIR resolution is shared with the test suite -- see tests/integration/_redis_path.py.)
"""
from __future__ import annotations

import pathlib
import re
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent

_INTEGRATION_DIR = str(REPO_ROOT / "tests" / "integration")
if _INTEGRATION_DIR not in sys.path:
    sys.path.insert(0, _INTEGRATION_DIR)
from _redis_path import _resolve_redis_dir  # noqa: E402

NUMERIC_TYPES = {"Int", "UInt", "Long", "ULong", "LongLong", "ULongLong", "SizeT", "SSizeT", "TimeT", "OffT"}
# numericBoundaryCheck() (config.c) compares these types as unsigned; every other numeric type is compared as signed.
UNSIGNED_TYPES = {"UInt", "ULongLong", "SizeT"}
# Field position of is_valid_fn per create*Config macro (0-based, after name).
IS_VALID_INDEX = {"Bool": 5, "String": 6, "SDS": 6, "Enum": 6}
NUMERIC_IS_VALID_INDEX = 8
# Settings compiled only with LOG_REQ_RES, which release builds of Redis do not define.
SKIPPED_IFDEFS = {"LOG_REQ_RES"}


def split_args(text: str) -> list[str]:
    """Split a C argument list at top-level commas (ignores commas inside (), {} and strings)."""
    args, depth, cur, in_str = [], 0, [], False
    i = 0
    while i < len(text):
        c = text[i]
        if in_str:
            cur.append(c)
            if c == "\\":
                cur.append(text[i + 1])
                i += 1
            elif c == '"':
                in_str = False
        elif c == '"':
            in_str = True
            cur.append(c)
        elif c in "({":
            depth += 1
            cur.append(c)
        elif c in ")}":
            depth -= 1
            cur.append(c)
        elif c == "," and depth == 0:
            args.append("".join(cur).strip())
            cur = []
        else:
            cur.append(c)
        i += 1
    if "".join(cur).strip():
        args.append("".join(cur).strip())
    return args


def c_string(tok: str) -> str | None:
    tok = tok.strip()
    if tok == "NULL":
        return None
    m = re.fullmatch(r'"([^"\\]*)"', tok)
    if not m:
        raise ValueError(f"expected a string literal or NULL, got {tok!r}")
    return m.group(1)


def parse_enums(src: str) -> dict[str, list[str]]:
    enums = {}
    for m in re.finditer(r"configEnum (\w+)\[\] = \{(.*?)\{NULL, *0\}\s*\};", src, re.S):
        enums[m.group(1)] = re.findall(r'\{"([^"]+)",', m.group(2))
    return enums


def parse_table(src: str) -> list[tuple[str, list[str]]]:
    start = src.index("standardConfig static_configs[] = {")
    end = src.index("{NULL}", start)
    entries, skipping = [], 0
    for line in src[start:end].splitlines()[1:]:
        s = line.strip()
        if s.startswith("#ifdef") or s.startswith("#if "):
            if s.split()[1] in SKIPPED_IFDEFS or skipping:
                skipping += 1
            continue
        if s.startswith("#endif"):
            if skipping:
                skipping -= 1
            continue
        if skipping or not s.startswith("create"):
            continue
        m = re.match(r"create(\w+)Config\((.*)\),\s*(/\*.*)?$", s)
        if not m:
            raise ValueError(f"cannot parse config table line: {s}")
        entries.append((m.group(1), split_args(m.group(2))))
    return entries


def cxx_str(value: str | None) -> str:
    return "nullptr" if value is None else f'"{value}"'


def main() -> int:
    config_c = _resolve_redis_dir() / "src" / "config.c"
    src = config_c.read_text(encoding="utf-8")
    enums = parse_enums(src)
    rows, used_enums = [], []
    for kind, args in parse_table(src):
        name, alias, flags = c_string(args[0]), c_string(args[1]), args[2]
        multi_arg = "MULTI_ARG_CONFIG" in flags
        row = {"name": name, "alias": alias, "multi_arg": multi_arg, "lower": "0", "upper": "0",
               "num_flags": "0", "unsigned": False, "enum": None, "is_valid": None}
        if kind == "Bool":
            row["type"] = "Bool"
        elif kind in ("String", "SDS"):
            row["type"] = "String"
        elif kind == "Enum":
            row["type"] = "Enum"
            row["enum"] = args[3]
            if args[3] not in enums:
                raise ValueError(f"unknown enum {args[3]} for {name}")
            if args[3] not in used_enums:
                used_enums.append(args[3])
        elif kind in NUMERIC_TYPES:
            row["type"] = "Numeric"
            row["lower"], row["upper"] = args[3], args[4]
            num_flags = []
            for f, bit in (("MEMORY_CONFIG", "kNumMemory"), ("PERCENT_CONFIG", "kNumPercent"), ("OCTAL_CONFIG", "kNumOctal")):
                if f in args[7]:
                    num_flags.append(bit)
            row["num_flags"] = " | ".join(num_flags) if num_flags else "0"
            row["unsigned"] = kind in UNSIGNED_TYPES
            row["is_valid"] = args[NUMERIC_IS_VALID_INDEX]
        elif kind == "Special":
            row["type"] = "Special"
        else:
            raise ValueError(f"unknown config kind {kind} for {name}")
        if kind in IS_VALID_INDEX:
            row["is_valid"] = args[IS_VALID_INDEX[kind]]
        if row["is_valid"] == "NULL":
            row["is_valid"] = None
        rows.append(row)

    out = [
        "// AUTO-GENERATED by scripts/ci/generate_redis_config_table.py — DO NOT EDIT.",
        "// Source: upstream Redis 7.2.5 src/config.c (static_configs[] and its configEnum arrays):",
        "// every setting a config file or command line may name, with its alias, type, numeric",
        "// range and enum values. Settings built only with LOG_REQ_RES are left out, as in a",
        "// release build of Redis.",
        "#pragma once",
        "",
        "#include <climits>",
        "#include <cstddef>",
        "#include <sys/types.h>",
        "",
        "namespace peadb {",
        "",
        "// clang-format off",
        "",
        "enum class RedisConfigType { Bool, String, Enum, Numeric, Special };",
        "",
        "// Numeric parse flags, as in config.c (INTEGER_CONFIG is 0).",
        "inline constexpr int kNumMemory = 1;   // MEMORY_CONFIG: accepts 1k, 5mb, 2gb, ...",
        "inline constexpr int kNumPercent = 2;  // PERCENT_CONFIG: accepts N% (stored negative)",
        "inline constexpr int kNumOctal = 4;    // OCTAL_CONFIG: parsed in base 8",
        "",
        "struct RedisConfigSpec {",
        "  const char* name;",
        "  const char* alias;             // nullptr when the setting has no alias",
        "  RedisConfigType type;",
        "  bool multi_arg;                // MULTI_ARG_CONFIG: takes more than one argument",
        "  int numeric_flags;             // Numeric only: kNum* bits",
        "  bool numeric_unsigned;         // Numeric only: bounds are compared as unsigned",
        "  long long lower;               // Numeric only: inclusive bounds as Redis stores them",
        "  long long upper;",
        "  const char* const* enum_names; // Enum only: allowed values in Redis's order, nullptr-terminated",
        "  const char* is_valid;          // name of Redis's extra validator function, or nullptr",
        "};",
        "",
    ]
    for e in used_enums:
        names = ", ".join(f'"{n}"' for n in enums[e])
        out.append(f"inline constexpr const char* kRedisEnum_{e}[] = {{{names}, nullptr}};")
    out += [
        "",
        "inline constexpr RedisConfigSpec kRedisConfigSpecs[] = {",
    ]
    for r in rows:
        enum_ref = f"kRedisEnum_{r['enum']}" if r["enum"] else "nullptr"
        out.append(
            f"    {{{cxx_str(r['name'])}, {cxx_str(r['alias'])}, RedisConfigType::{r['type']}, "
            f"{'true' if r['multi_arg'] else 'false'}, {r['num_flags']}, "
            f"{'true' if r['unsigned'] else 'false'}, static_cast<long long>({r['lower']}), "
            f"static_cast<long long>({r['upper']}), {enum_ref}, {cxx_str(r['is_valid'])}}},"
        )
    out += [
        "};",
        "",
        "inline constexpr std::size_t kRedisConfigSpecCount = sizeof(kRedisConfigSpecs) / sizeof(kRedisConfigSpecs[0]);",
        "",
        "// clang-format on",
        "",
        "}  // namespace peadb",
        "",
    ]
    dest = REPO_ROOT / "include" / "redis_config_table.generated.hpp"
    dest.write_text("\n".join(out), encoding="utf-8")
    print(f"Wrote {dest} ({len(rows)} settings, {len(used_enums)} enums)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
