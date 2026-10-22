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
2. **io_uring prototype removed in the follow-up below.** The selected epoll
   and kqueue implementations remain; adding io_uring is future feature work.
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

## Clean-code follow-up — 2026-10-22

Baseline: `2e333b1`. Rechecked the current tracked C/C++, Python, shell,
build targets, workflows and embedded Lua. The earlier cleanup remains in
place. Untracked personal notes and concurrent changes to the tracked
`AI-TEMP/auto-improve-codex.sh` and its tests are outside this cleanup.

Additional confirmed removals:

- `IoUringPoller`, its headers and CMake's liburing discovery, compile flag,
  include path and link dependency. The sole server instantiation is
  `Poller`, and neither supported platform selected the prototype. Updated
  architecture/compatibility documentation and marked D0.3's backend policy
  as a future target.
- `DataStore::save_snapshot_file`, the uncalled `PEADB-SNAPSHOT-V1` writer.
  Startup still uses `load_snapshot_file` for legacy recovery; native saves
  and replication use `rdb_save`. The legacy reader, its dependencies and
  its regression tests remain.
- `rdb_save_to_string`, an uncalled internal wrapper, and the false branch
  it alone passed to `rdb_build_string`'s delay argument. The surviving
  caller always enabled the delay; file saves and full-sync snapshots still
  honor `rdb-key-save-delay`. Single-key DUMP serialization is independent.
- The uncalled `set_active_expire_enabled`, `expire_paused` and
  `lazy_expire_disabled` accessors, and the obsolete zset ziplist threshold
  getter/setter. DEBUG still controls active expiry directly; pause/lazy
  expiry internals and the live `encoding_limits()` configuration remain.
- `SessionState::repl_send_select`, which was never read or assigned;
  the sandbox's unused `_pairs` local; and the unused `ok`/`is_nil` unit-test
  helpers. Also removed the unused `<regex>` include from `datastore.cpp`;
  it uses no regex symbols. No behavioral assertion was removed.

These are internal server interfaces: CMake has no library/header install
or export registration, and native module discovery uses the separate
`RedisModule_*` ABI. Searches included source, tests, scripts, generated
metadata and documentation, followed by reviewing the actual persistence,
replication and configuration callers.

Retained boundaries:

- Dict's standard container surface (`cbegin`, `cend`, `max_load_factor` and
  conventional type aliases) has no direct calls for some members. They
  intentionally support generic container consumers. Removing that surface
  requires deciding whether compatibility with those consumers is part of
  the container contract; a name count alone does not resolve that question.
- Exported native-module entry points and registered Lua callbacks remain.
- The Lua development package and pkg-config still have a consumer in the
  standalone fuzz build; they are not unused setup dependencies.
- Shared test transport/lifecycle consolidation remains a separate live-code
  refactor. All integration entry points and behavioral assertions remain.

Follow-up verification:

- Full macOS integration supervisor: **170 passed, zero failed**, exit 0
  (`AI-TEMP/clean-code-integration.log`, per-file logs in
  `artifacts/integration/run-vkynq3oi/`). The absent `third_party/redis`
  checkout skipped the D0 Git SHA assertion; the cached reference was used.
- Fresh macOS arm64 Release configure/build in `build-clean-code`, followed
  by the final rebuild and CTest: **16/16 passed**, exit 0. Logs:
  `AI-TEMP/clean-code-{configure,build,build-final,unit}.log`. After the
  final redundant `<regex>` include was removed, another rebuild produced
  byte-identical datastore object/server SHA-256 hashes, so the full suite
  above exercised the delivered binary. CTest again passed **16/16**, exit 0
  (`AI-TEMP/clean-code-include-build.log`, `clean-code-unit-final.log`).
- Fresh Linux arm64 Release build from a copy of the edited sources in
  `peadb-ci-toolchain` (GCC 13.3): **16/16 unit tests passed** and **12/12
  focused integration files passed**, exit 0. Covered protocol/connection
  hygiene, delayed full-sync snapshots, stream database framing, startup
  load/BGSAVE status, encoding thresholds, paused expiry, Lua and restart
  history. Logs: `AI-TEMP/clean-code-linux.log` and the snapshot's
  `configure.log`, `build.log` and `artifacts/integration/run-eiysd4jt/`.
  This initial root run skipped unreadable-file/read-only-directory checks;
  rerunning those two files as UID 1000 executed them and passed **2/2**,
  exit 0 (`AI-TEMP/clean-code-linux-permissions.log`). The snapshot's
  cleanup source files were byte-compared with the final worktree.
  A second fresh Linux build after removing `<regex>` passed **16/16**
  unit tests and **3/3** affected integration files as UID 1000, exit 0
  (`AI-TEMP/clean-code-linux-final.log`).
- Both differential JSON cases passed with Redis 7.2.5, exit 0. All
  cross-engine runs used the cached 7.2.5 reference binaries; the Linux
  reference version was checked inside its container.
- Command coverage (236 commands), port-collision ratchet (21 known, zero
  new) and compatibility-delta gates passed, exit 0.
- All 32 first-party C/C++ CMake sources passed a syntax scan with unused
  warnings enabled: **zero failures and zero unused warnings**. The
  unreachable-code scan also passed with no findings. Logs:
  `AI-TEMP/clean-code-{unused-check,unreachable-check,unused-check-final}.log`.
  The initial
  standalone scan split compiler commands incorrectly and produced version
  macro quoting errors; the successful rerun used `shlex.split`. The normal
  Release builds passed independently. Existing missing-field-initializer
  warnings in the command table remain outside this removal task.
- Ruff (`F401,F811,F821,F841`) passed and all 193 tracked Python files
  parsed. Vulture still exits 3 for the required `frame` signal-handler
  parameter; it found no independently removable Python code. ShellCheck
  0.11.0's unused-variable check (`SC2034`) passed on all tracked shell scripts.
- clang-format 18.1.8 on changed production C/C++ files and whitespace
  validation passed. Removed-symbol searches found no remaining consumers.

No fresh upstream Tcl sweep or sanitizer run was performed for this follow-up.
