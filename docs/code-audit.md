# Code and language audit — 2026-10-22

Baseline: `97435e5`. The audit covered all 374 tracked files, Python entry
points and imports, CMake sources, C/C++ references, test discovery, workflows
and script/documentation references. Untracked personal notes and ignored
build products were excluded.

## Why GitHub shows so much Python

The GitHub languages API returned 1,280,668 Python bytes out of 3,116,987
counted source bytes: **41.1%** at audit time, close to the reported 40.8%.
The baseline's tracked Python file sizes exactly match that API count.
There were 195 Python files, totalling 32,221 physical lines.

[GitHub Linguist](https://github.com/github-linguist/linguist#git-repository)
measures file sizes; its percentage does not measure runtime dependencies,
execution time or the implementation language of the server. Vendored code,
including bundled Lua, is normally excluded from these statistics.

| Python at the baseline | Files | Bytes | Purpose / decision |
| --- | ---: | ---: | --- |
| `tests/integration/` | 173 | 1,112,695 | 170 discovered test entry points and three shared helpers; retain behavioral coverage |
| `scripts/ci/` | 8 | 61,439 | Test supervisor, metadata generators, coverage and compatibility gates; retain |
| `tests/acceptance/` | 1 | 26,165 | Migration acceptance, also exercised by an integration wrapper; retain |
| `tests/diff/` | 3 | 9,215 | Cross-engine differential runner and harness; retain |
| `scripts/redis/` | 5 | 24,855 | Tcl result parser and RDB import/export/sync tools; retain |
| Tracked Python in `AI-TEMP/` | 2 | 31,516 | Tests for the tracked development automation; retain with their script and validation notes |
| `scripts/debug/` | 2 | 9,162 | Superseded manual drivers; removed |
| `scripts/examples/` | 1 | 5,621 | Optional real redis-py client stress example; retain |

**86.9% of Python bytes are integration tests.** The server is compiled C/C++
and does not need Python to start or serve requests. Python is needed for
development, testing and the optional migration utilities. Most of it uses
the standard library; `redis` in `scripts/requirements.txt` belongs to the
optional redis-py example.

The automation files under `AI-TEMP/` are intentional, documented, previously
tested tooling, despite the directory name and ignore rule. An ignored path
can still contain tracked files. Removing just their Python tests would
detach them from the shell implementation they validate.

There is no identical whole-file duplication among tracked files. There is
also no evidence that the integration files are orphaned: the supervisor
discovers every `tests/integration/test_*.py`, and PR/nightly workflows invoke
that supervisor. A filename retaining an old milestone is not evidence of
unused coverage.

## Cleanup performed

- Removed the manual RESP3/Lua and replication debug drivers. Their scenarios
  already have assertions in `test_p2_scripting_debug_protocol.py` and
  `test_p2_script_replication_shapes.py`. The replication driver also assumed
  an empty `$0` snapshot, which the real server no longer sends.
- Removed 73 unused Python imports, two uncalled local helpers
  (`read_int_reply` and the maxmemory test's `wait_for`), an unused shutdown
  result binding, and a literal-false ternary branch. Server waiting and all
  behavioral assertions remain active.
- Removed `ServerContext`: its replication, persistence and stats members
  were never read or populated, and the two copied callbacks had no consumers.
  The real state and callbacks remain in the existing server/command paths.
- Removed `ShardLoop`, its dispatch/init/shutdown stubs, the unused duplicate
  CRC16/slot helpers and the ineffective `PEADB_NUM_SHARDS` build setting.
  No production or test caller used them. The actual cluster hash function
  and the sharding roadmap remain; the compatibility ledger now distinguishes
  implemented behavior from future work.
- Removed `lua_compat.h` and the Lua 5.2+ branches in `lua_engine.cpp`.
  CMake always builds bundled Lua 5.1.5, and the C modules already include
  the native Lua headers. These compatibility paths could not execute in
  the supported build.
- Removed eight uncalled C++ functions: `parse_eval_set_keys1_literal`,
  `encode_bool`, `record_cmd_call`, `encode_command_resp_size`, `dump_prefix`,
  `command_writes`, `lua_redis_pcall_wrapper`, and `fenv_newindex_forbidden`.
  Also removed the compiler-reported unused `g_script_prng_state` global.
  Updated documentation that named the removed helpers.

The cleanup retains all 170 integration entry points and all 16 C++ unit
tests. There are now 193 Python files. This is a small reduction in Python
bytes; the language percentage remains about 41% because unused C++ was
removed too.

## Remaining redundancy and priorities

1. **Share test transport and process fixtures.** At the baseline, comparing
   Python function ASTs (including names, ignoring locations, excluding
   `main` and constructors) found 77 duplicate groups, 546 copies beyond
   the first, and 106,582 source bytes in those extra copies. The largest
   groups are RESP encoders/readers, command wrappers, readiness and stop
   helpers. These are repeated plumbing, not duplicate test assertions.
   Start with byte-identical RESP2 helpers and validate each migration.
   Keep explicit adapters for binary replies, RESP3 attributes/pushes,
   replication keepalives, exact RDB lengths and authentication. Lifecycle
   changes need the existing supervisor tests on Linux as well as macOS.
2. **Decide the io_uring prototype's future.** `IoUringPoller` in
   `include/poller.hpp` has no instantiation; `src/server.cpp` always creates
   `Poller`. CMake's liburing detection compiles the prototype but does not
   select it. Either wire it with Linux runtime tests or remove the prototype
   and detection together. Architecture documentation now describes the
   current behavior accurately; this audit does not claim Linux validation.
3. **Keep static checks inexpensive.** Ruff found only the removed unused
   imports/binding for `F401,F811,F821,F841`; those checks now pass. Vulture
   helped locate the dead ternary, but missed some uncalled helpers whose
   names are used in other files. Name/reference scans require manual review:
   module exports loaded via `dlopen`/`dlsym`, callbacks, generated metadata,
   platform branches and public container APIs are not safe deletion targets
   merely because they have no ordinary call site. The remaining Vulture
   finding is the unused `frame` argument of the migration acceptance signal
   handler; its two-argument callback signature is required and retained.

Reducing test duplication is the useful next step. Rewriting valid tests in
C++ just to change the language bar would introduce churn and retain the
same maintenance burden. No Linguist overrides were added to exclude tests
or relabel Python as another language.

## Verification

These checks cover the delivered worktree, not just the baseline commit,
on macOS arm64 with AppleClang 21 and Python 3.13:

- Fresh Release configure/build in `AI-TEMP/code-audit-build`: exit 0;
  rebuilt after removing the compiler-reported unused PRNG global.
- CTest after that final rebuild: **16/16 passed**, exit 0
  (`AI-TEMP/code-audit-unit-final.log`).
- Full integration supervisor with the pinned Redis 7.2.5 tools on `PATH`:
  **170 passed, 0 failed**, exit 0 (`AI-TEMP/code-audit-integration-final.log`;
  per-file logs in `artifacts/integration/run-dsx6ifpx/`). Two existing
  artifact checks were skipped inside otherwise passing files: a Git SHA
  check for the absent `third_party/redis` checkout (the reference is in
  the cache) and an assertion for an absent old `ci.yml` workflow.
- `tests/diff/run_diff_tests.py` against pinned Redis 7.2.5: passed, exit 0.
- Command-reference coverage (236 commands), port-collision ratchet
  (21 known, zero new) and compatibility-delta validation: passed, exit 0.
- Ruff 0.17.1, `F401,F811,F821,F841`, on all remaining tracked Python:
  passed, exit 0. All 193 remaining Python files compile. AST comparison
  of the 74 modified Python files confirmed only the documented cleanup.
- clang-format 18.1.8 on changed C/C++ files and `git diff --check`:
  passed, exit 0.

The first integration attempt lacked `redis-cli` on `PATH`, reported failures
and was stopped through its owned supervisor (exit 143). It is not a pass;
its output remains in `AI-TEMP/code-audit-integration.log`. The corrected full
run above passed. No fresh upstream Tcl sweep, sanitizer run or Linux run
was performed for this cleanup.
