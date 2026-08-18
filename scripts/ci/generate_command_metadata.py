#!/usr/bin/env python3
"""Generate C++ COMMAND metadata from the pinned upstream Redis command JSON.

Redis ships a machine-readable spec for every command and subcommand under
``src/commands/*.json``. Rather than hand-maintain COMMAND LIST / INFO metadata
(subcommand enumeration, ACL categories, the movablekeys flag), we generate it
from that spec at the 7.2.5 pin, exactly like ``generate_compat_scoreboard.py``.

ACL categories combine each command's explicit ``acl_categories`` with the
*implicit* categories Redis derives from command flags (``setImplicitACLCategories``
in ``server.c``) — see ``implicit_acl_categories()`` below.

Output: ``include/command_metadata.generated.hpp`` — a sorted table of
``{full_name, acl_cats, movablekeys}`` consumed by the COMMAND handler. Re-run
after bumping the Redis pin and commit the regenerated header.

Usage:
    REDIS_DIR=~/.cache/peadb/redis-7.2.5 python3 scripts/ci/generate_command_metadata.py
    (REDIS_DIR resolution mirrors scripts/redis/run_redis_tests.sh exactly --
    see tests/integration/_redis_path.py, reused below.)
"""
from __future__ import annotations

import glob
import json
import pathlib
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

    entries: dict[str, tuple[str, bool]] = {}
    all_cats_seen: set[str] = set()
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
            entries[full] = (" ".join(sorted(cats)), is_movablekeys(spec))

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
           "// full command/subcommand names, ACL categories, movablekeys flag).",
           "#pragma once",
           "",
           "#include <vector>",
           "",
           "namespace peadb {",
           "",
           "struct CommandMetaEntry {",
           "  const char* full_name;   // lowercase; \"parent|sub\" for subcommands",
           "  const char* acl_cats;    // space-separated lowercase ACL categories (no @)",
           "  bool movablekeys;",
           "};",
           "",
           "inline const std::vector<CommandMetaEntry>& command_metadata() {",
           "  static const std::vector<CommandMetaEntry> kMeta = {"]
    for full in sorted(entries):
        cats, mk = entries[full]
        out.append(f'      {{"{full}", "{cats}", {"true" if mk else "false"}}},')
    out += ["  };",
            "  return kMeta;",
            "}",
            "",
            "}  // namespace peadb",
            ""]

    dest = REPO_ROOT / "include" / "command_metadata.generated.hpp"
    dest.write_text("\n".join(out))
    print(f"Wrote {dest} ({len(entries)} commands+subcommands)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
