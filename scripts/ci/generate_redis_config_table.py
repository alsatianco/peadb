#!/usr/bin/env python3
"""Generate the table of Redis 7.2.5 configuration settings from the pinned source.

The config file and command-line loader (``src/config.cpp``) must accept exactly the settings Redis accepts, check their values the way Redis does, and refuse everything else with Redis's own errors; ``CONFIG GET`` and ``CONFIG SET`` (``src/command.cpp``) must know every setting, its flags and its default. Rather than hand-copy 180+ names, aliases, types, numeric ranges, enum values, flags and defaults, we read them from the ``static_configs[]`` table in the pinned ``src/config.c`` (plus its ``configEnum`` arrays and the ``#define``s in ``src/server.h``).

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
# Field position of the default value per create*Config macro (0-based, after name).
DEFAULT_INDEX = {"Bool": 4, "String": 5, "SDS": 5, "Enum": 5}
NUMERIC_DEFAULT_INDEX = 6
# The config flags CONFIG GET and CONFIG SET act on (server.h), and the bit each gets here.
FLAG_BITS = (("IMMUTABLE_CONFIG", "kCfgImmutable"), ("PROTECTED_CONFIG", "kCfgProtected"),
             ("DENY_LOADING_CONFIG", "kCfgDenyLoading"), ("HIDDEN_CONFIG", "kCfgHidden"),
             ("SENSITIVE_CONFIG", "kCfgSensitive"))
# Special settings keep their default outside static_configs[]: initServerConfig() (server.c) sets
# them, or their getter reports live state. These are the values CONFIG GET prints on a Redis
# 7.2.5 started with no config file. None means "no fixed default" (dir is the working directory).
SPECIAL_DEFAULTS = {
    "dir": None,
    "save": "3600 1 300 100 60 10000",
    "client-output-buffer-limit": "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60",
    "oom-score-adj-values": "0 200 800",
    "notify-keyspace-events": "",
    "bind": "* -::*",
    "replicaof": "",
    "latency-tracking-info-percentiles": "50 99 99.9",
}


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


def parse_enums(src: str) -> dict[str, list[tuple[str, str]]]:
    """Each configEnum array as (name, value token) pairs, in Redis's order."""
    enums = {}
    for m in re.finditer(r"configEnum (\w+)\[\] = \{(.*?)\{NULL, *0\}\s*\};", src, re.S):
        enums[m.group(1)] = [(n, v.strip()) for n, v in re.findall(r'\{"([^"]+)",\s*([^}]+)\}', m.group(2))]
    return enums


def parse_defines(text: str) -> dict[str, str]:
    """Object-like #define NAME value lines (comments dropped)."""
    defines = {}
    for m in re.finditer(r"^#define\s+(\w+)\s+(.+?)\s*(?:/\*.*)?$", text, re.M):
        defines[m.group(1)] = m.group(2).strip()
    return defines


def eval_numeric(expr: str, defines: dict[str, str]) -> int:
    """A numeric default such as 512ll*1024*1024, 100<<20 or CONFIG_DEFAULT_HZ."""
    e = expr.strip()
    for _ in range(5):
        e = re.sub(r"\b[A-Z_][A-Z0-9_]*\b", lambda m: f"({defines[m.group(0)]})" if m.group(0) in defines else m.group(0), e)
    e = re.sub(r"(?<=\d)(?:ULL|ull|LL|ll|UL|ul|L|l|U|u)\b", "", e)
    if not re.fullmatch(r"[\d\s()+\-*/<>]+", e):
        raise ValueError(f"cannot evaluate numeric default {expr!r} (became {e!r})")
    return int(eval(e))  # only digits and arithmetic operators remain


def default_text(kind: str, args: list[str], flags: str, enums: dict, defines: dict[str, str]) -> str:
    """The default as CONFIG GET prints it (boolConfigGet(), numericConfigGet(), ...)."""
    if kind == "Bool":
        return "yes" if eval_numeric(args[DEFAULT_INDEX[kind]], defines) else "no"
    if kind in ("String", "SDS"):
        tok = args[DEFAULT_INDEX[kind]].strip()
        tok = defines.get(tok, tok)
        return c_string(tok) or ""
    if kind == "Enum":
        tok = args[DEFAULT_INDEX[kind]].strip()
        for name, value in enums[args[3]]:
            if value == tok:
                return name
        raise ValueError(f"default {tok!r} of {c_string(args[0])} is not in enum {args[3]}")
    value = eval_numeric(args[NUMERIC_DEFAULT_INDEX], defines)
    if "PERCENT_CONFIG" in args[7] and value < 0:
        return f"{-value}%"
    if "OCTAL_CONFIG" in args[7]:
        return format(value, "o")
    return str(value)


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
    if value is None:
        return "nullptr"
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def main() -> int:
    config_c = _resolve_redis_dir() / "src" / "config.c"
    src = config_c.read_text(encoding="utf-8")
    enums = parse_enums(src)
    defines = parse_defines((_resolve_redis_dir() / "src" / "server.h").read_text(encoding="utf-8"))
    defines.update(parse_defines(src))
    rows, used_enums = [], []
    for kind, args in parse_table(src):
        name, alias, flags = c_string(args[0]), c_string(args[1]), args[2]
        multi_arg = "MULTI_ARG_CONFIG" in flags
        cfg_flags = [bit for f, bit in FLAG_BITS if f in flags]
        row = {"name": name, "alias": alias, "multi_arg": multi_arg, "lower": "0", "upper": "0",
               "num_flags": "0", "unsigned": False, "enum": None, "is_valid": None,
               "flags": " | ".join(cfg_flags) if cfg_flags else "0",
               "sds": kind == "SDS", "empty_to_null": False, "null_default": False}
        if kind == "Bool":
            row["type"] = "Bool"
        elif kind in ("String", "SDS"):
            row["type"] = "String"
            row["empty_to_null"] = args[3] == "EMPTY_STRING_IS_NULL"
            tok = defines.get(args[5].strip(), args[5].strip())
            row["null_default"] = tok == "NULL"
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
            if name not in SPECIAL_DEFAULTS:
                raise ValueError(f"no default known for the special setting {name}: add it to SPECIAL_DEFAULTS")
        else:
            raise ValueError(f"unknown config kind {kind} for {name}")
        if kind in IS_VALID_INDEX:
            row["is_valid"] = args[IS_VALID_INDEX[kind]]
        if row["is_valid"] == "NULL":
            row["is_valid"] = None
        row["default"] = SPECIAL_DEFAULTS[name] if kind == "Special" else default_text(kind, args, flags, enums, defines)
        rows.append(row)
    unused = set(SPECIAL_DEFAULTS) - {r["name"] for r in rows if r["type"] == "Special"}
    if unused:
        raise ValueError(f"SPECIAL_DEFAULTS names settings that are not special: {sorted(unused)}")

    out = [
        "// AUTO-GENERATED by scripts/ci/generate_redis_config_table.py — DO NOT EDIT.",
        "// Source: upstream Redis 7.2.5 src/config.c (static_configs[] and its configEnum arrays):",
        "// every setting a config file or command line may name, with its alias, type, numeric",
        "// range, enum values, flags and default. Settings built only with LOG_REQ_RES are left out,",
        "// as in a release build of Redis.",
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
        "// Config flags (server.h) that CONFIG GET and CONFIG SET act on.",
        "inline constexpr int kCfgImmutable = 1;    // IMMUTABLE_CONFIG: startup only",
        "inline constexpr int kCfgProtected = 2;    // PROTECTED_CONFIG: needs enable-protected-configs",
        "inline constexpr int kCfgDenyLoading = 4;  // DENY_LOADING_CONFIG: not while loading",
        "inline constexpr int kCfgHidden = 8;       // HIDDEN_CONFIG: CONFIG GET <pattern> skips it",
        "inline constexpr int kCfgSensitive = 16;   // SENSITIVE_CONFIG: value redacted in logs",
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
        "  int flags;                     // kCfg* bits",
        "  const char* default_value;     // as CONFIG GET prints it; nullptr when there is none (dir)",
        "  bool string_sds;              // String only: binary-safe SDS instead of a C string",
        "  bool string_empty_to_null;    // String only: empty value removes the directive",
        "  bool string_null_default;     // String only: NULL differs from an empty default",
        "};",
        "",
    ]
    for e in used_enums:
        names = ", ".join(f'"{n}"' for n, _ in enums[e])
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
            f"static_cast<long long>({r['upper']}), {enum_ref}, {cxx_str(r['is_valid'])}, "
            f"{r['flags']}, {cxx_str(r['default'])}, "
            f"{str(r['sds']).lower()}, {str(r['empty_to_null']).lower()}, {str(r['null_default']).lower()}}},"
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
