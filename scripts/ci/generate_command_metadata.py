#!/usr/bin/env python3
"""Generate C++ COMMAND metadata from the pinned upstream Redis command JSON.

Redis ships a machine-readable spec for every command and subcommand under
``src/commands/*.json``. Rather than hand-maintain COMMAND LIST / INFO metadata
(subcommand enumeration, ACL categories, the movablekeys flag) and the command
flags that decide behavior (WRITE, MAY_REPLICATE, ...), we generate them from
that spec at the 7.2.5 pin, exactly like ``generate_compat_scoreboard.py``.

ACL categories combine each command's explicit ``acl_categories`` with the
*implicit* categories Redis derives from command flags (``setImplicitACLCategories``
in ``server.c``) — see ``implicit_acl_categories()`` below.

Output: ``include/command_metadata.generated.hpp`` — a sorted table of
``{full_name, acl_cats, movablekeys, flags}`` consumed by the COMMAND handler,
CLIENT PAUSE and the read-only script check. ``flags`` holds Redis's ``CMD_*``
bits with the values ``src/server.h`` gives them. Re-run after bumping the Redis
pin and commit the regenerated header. Requires clang-format 18.1.8 (the CI pin);
set CLANG_FORMAT to its executable when it is not on PATH.

Usage:
    REDIS_DIR=~/.cache/peadb/redis-7.2.5 python3 scripts/ci/generate_command_metadata.py
    (REDIS_DIR resolution mirrors scripts/redis/run_redis_tests.sh exactly --
    see tests/integration/_redis_path.py, reused below.)
"""
from __future__ import annotations

import glob
import json
import os
import pathlib
import re
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent

# redis-server/source-tree discovery (REDIS_DIR, else PEADB_CACHE_DIR, else
# XDG_CACHE_HOME, else ~/.cache/peadb -- with a set-but-empty var falling
# through, matching bash's ":-") is shared with the test suite and
# scripts/redis/*.py -- see tests/integration/_redis_path.py for the
# resolution order and rationale. Do not re-implement that search here.
_INTEGRATION_DIR = str(REPO_ROOT / "tests" / "integration")
if _INTEGRATION_DIR not in sys.path:
    sys.path.insert(0, _INTEGRATION_DIR)
from _redis_path import _resolve_redis_dir  # noqa: E402

# ACL categories Redis's implicit-categories rule can add that this repo's
# fixed test data must exercise (guards against the rule silently regressing
# to a no-op -- see the assertion at the end of main()).
_REQUIRED_IMPLICIT_CATEGORIES = frozenset(
    {"read", "write", "fast", "slow", "blocking", "pubsub"}
)


def redis_commands_dir() -> pathlib.Path:
    return _resolve_redis_dir() / "src" / "commands"


def redis_command_flag_bits() -> dict[str, int]:
    """Bit position of every ``CMD_<NAME>`` command flag in the pinned
    ``src/server.h`` (``#define CMD_WRITE (1ULL<<0)``; ``CMD_ALLOW_BUSY`` has
    extra parentheses). The key-spec and channel flags (``CMD_KEY_*``,
    ``CMD_CHANNEL_*``) reuse the same bit numbers but never appear in a
    command's ``command_flags``, so main() only uses the names it finds there."""
    text = (_resolve_redis_dir() / "src" / "server.h").read_text()
    return {m.group(1): int(m.group(2))
            for m in re.finditer(r"^#define CMD_([A-Z_]+) \(+1ULL<<(\d+)\)+", text, re.M)}


def flag_constant(name: str) -> str:
    """``MAY_REPLICATE`` -> ``kRedisCmdMayReplicate``."""
    return "kRedisCmd" + "".join(part.capitalize() for part in name.split("_"))


def implicit_acl_categories(spec: dict, explicit_cats: set[str]) -> set[str]:
    """Mirror Redis's setImplicitACLCategories() (src/server.c:2958-2976),
    which derives extra ACL categories from a command's flags on top of any
    explicit ``acl_categories`` in its JSON spec:

        WRITE          -> write
        READONLY       -> read   (but NOT if the command is already @scripting)
        ADMIN          -> admin, dangerous
        PUBSUB         -> pubsub
        FAST           -> fast
        BLOCKING       -> blocking

    The remaining upstream rule -- "if it's not @fast it's @slow" -- is
    applied by the caller (main()) against the fully-combined category set,
    since it must see @fast whether that came from here or from an explicit
    acl_categories entry.

    ``explicit_cats`` must be the lowercased explicit acl_categories for the
    same command, so the scripting exclusion for @read can be evaluated.
    """
    flags = set(spec.get("command_flags") or [])
    cats: set[str] = set()
    if "WRITE" in flags:
        cats.add("write")
    # Exclude scripting commands from @read (upstream: "if (c->flags &
    # CMD_READONLY && !(c->acl_categories & ACL_CATEGORY_SCRIPTING))"). This
    # exclusion is not optional -- e.g. EVAL_RO is READONLY but @scripting,
    # and must not gain @read.
    if "READONLY" in flags and "scripting" not in explicit_cats:
        cats.add("read")
    if "ADMIN" in flags:
        cats.add("admin")
        cats.add("dangerous")
    if "PUBSUB" in flags:
        cats.add("pubsub")
    if "FAST" in flags:
        cats.add("fast")
    if "BLOCKING" in flags:
        cats.add("blocking")
    return cats


def is_movablekeys(spec: dict) -> bool:
    """Redis marks a command movablekeys when its key positions can't be a legacy
    first/last/step range — i.e. any key_spec uses a non-index begin_search or a
    non-range find_keys (keyword / keynum / unknown)."""
    for ks in spec.get("key_specs", []) or []:
        bs = ks.get("begin_search", {}) or {}
        fk = ks.get("find_keys", {}) or {}
        if "index" not in bs or "range" not in fk:
            return True
    return False


def main() -> int:
    cmd_dir = redis_commands_dir()
    files = sorted(glob.glob(str(cmd_dir / "*.json")))
    if not files:
        print(f"no command JSON found in {cmd_dir}; set REDIS_DIR", file=sys.stderr)
        return 2

    flag_bits = redis_command_flag_bits()
    entries: dict[str, tuple[str, bool, list[str]]] = {}
    all_cats_seen: set[str] = set()
    flags_seen: set[str] = set()
    for f in files:
        data = json.load(open(f))
        for name, spec in data.items():
            container = spec.get("container")
            full = (container.lower() + "|" + name.lower()) if container else name.lower()
            explicit_cats = {c.lower() for c in (spec.get("acl_categories") or [])}
            cats = explicit_cats | implicit_acl_categories(spec, explicit_cats)
            # "If it's not @fast is @slow in this binary world" (server.c) --
            # applied last, against the fully-combined category set, since
            # @fast can itself come from the implicit WRITE/READONLY-derived
            # rule above.
            if "fast" not in cats:
                cats.add("slow")
            all_cats_seen |= cats
            flags = sorted(set(spec.get("command_flags") or []))
            unknown = [f for f in flags if f not in flag_bits]
            if unknown:
                print(f"error: {full}: command flags {unknown} have no CMD_ define in server.h",
                      file=sys.stderr)
                return 2
            flags_seen |= set(flags)
            entries[full] = (" ".join(sorted(cats)), is_movablekeys(spec), flags)

    # Regression guard: the implicit-categories rule above must actually add
    # these six categories (Redis has no command that carries them as an
    # explicit acl_categories entry, so before this rule existed they never
    # appeared in the generated header at all -- see task 4a). If this trips,
    # the implicit-derivation logic silently broke; fix it rather than
    # skipping this check.
    missing_implicit = _REQUIRED_IMPLICIT_CATEGORIES - all_cats_seen
    if missing_implicit:
        print(
            "error: expected ACL categories missing from generated metadata "
            f"(implicit-category derivation regressed?): {sorted(missing_implicit)}",
            file=sys.stderr,
        )
        return 2

    out = ["// AUTO-GENERATED by scripts/ci/generate_command_metadata.py — DO NOT EDIT.",
           "// Source: upstream Redis 7.2.5 src/commands/*.json (COMMAND LIST/INFO metadata:",
           "// full command/subcommand names, ACL categories, movablekeys flag, command flags).",
           "#pragma once",
           "",
           "#include <cstdint>",
           "#include <vector>",
           "",
           "namespace peadb {",
           "",
           "// Redis's command flags, with the CMD_* bit values of the pinned src/server.h.",
           "enum : std::uint64_t {"]
    for name in sorted(flags_seen, key=lambda n: flag_bits[n]):
        out.append(f"  {flag_constant(name)} = 1ULL << {flag_bits[name]},")
    out += ["};",
           "",
           "struct CommandMetaEntry {",
           "  const char* full_name;   // lowercase; \"parent|sub\" for subcommands",
           "  const char* acl_cats;    // space-separated lowercase ACL categories (no @)",
           "  bool movablekeys;",
           "  std::uint64_t flags;     // the command's \"command_flags\" as kRedisCmd* bits",
           "};",
           "",
           "inline const std::vector<CommandMetaEntry>& command_metadata() {",
           "  static const std::vector<CommandMetaEntry> kMeta = {"]
    for full in sorted(entries):
        cats, mk, flags = entries[full]
        flag_expr = " | ".join(flag_constant(f) for f in flags) or "0"
        out.append(f'      {{"{full}", "{cats}", {"true" if mk else "false"}, {flag_expr}}},')
    out += ["  };",
            "  return kMeta;",
            "}",
            "",
            "}  // namespace peadb",
            ""]

    dest = REPO_ROOT / "include" / "command_metadata.generated.hpp"
    formatter = os.environ.get("CLANG_FORMAT", "clang-format")
    version = subprocess.check_output([formatter, "--version"], text=True).strip()
    if version != "clang-format version 18.1.8":
        raise RuntimeError(f"Expected clang-format version 18.1.8, got {version}")
    formatted = subprocess.check_output(
        [formatter, f"--assume-filename={dest}"], input="\n".join(out), text=True
    )
    dest.write_text(formatted)
    print(f"Wrote {dest} ({len(entries)} commands+subcommands)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
