# Feedback on commit 2f42edad92afa2e8bf8a95db7df91e6c3e8d186d

## Verdict

Not ready as-is. The commit makes useful progress in several areas, but it still leaves multiple high-severity correctness and CI gaps from the Auggie, Opus, Codex, and GPT analyses. The most serious problems are data loss in AOF rewrite, stream loss in RDB saves, non-continuous replication after `REPLICAOF`, and broken `WAIT` reply ordering under pipelining.

## Blocking findings

1. Critical: `BGREWRITEAOF` loses writes after the rewrite finishes.

`BGREWRITEAOF` forks at `src/command.cpp:2014-2027`; the child calls `rewrite_aof_file()`, which renames the temp AOF into place and then calls `open_aof_fd()` at `src/command.cpp:724-729`. That reopen happens only in the child process. The parent keeps appending through its old `g_aof_fd`, which still points at the pre-rename inode. Any writes accepted after the child rename are not present in the live `appendonly.aof`.

I confirmed this with a probe: `SET before 1`, `BGREWRITEAOF`, wait for the child, `SET after 2`, restart with AOF enabled. `GET before` returned `1`; `GET after` returned nil.

Fix direction: the parent must reopen/swap the AOF fd after child completion, and background rewrite needs a parent-side diff/rewrite buffer or equivalent handoff so writes that occur during/after rewrite are preserved.

2. Critical: native RDB still drops stream keys on save/restart.

The RDB writer still explicitly skips streams at `src/rdb.cpp:722-726` with a warning and `continue`. This directly leaves the Auggie/GPT/Opus stream-persistence issue unresolved. The analyses asked for real stream RDB support or a loud failure; this commit still returns success from `SAVE`/`BGSAVE` while losing data.

I confirmed this with `XADD stream 1-0 f v`, `SAVE`, restart, `XLEN stream`; the post-restart length was `0`.

Fix direction: either implement stream RDB encoding, or make `SAVE`/`BGSAVE` fail when stream keys exist until the format is supported.

3. Critical: `REPLICAOF` performs only initial full sync, then closes the master connection.

`replica_do_fullsync()` performs `PSYNC ? -1`, reads the RDB payload, then closes the fd at `src/command.cpp:7032` and returns. There is no loop consuming the master's command stream, no reconnect/partial-resync path, and no ACK flow. This does not satisfy Opus C6/GPT 2.2's in-process replication-client requirement.

I confirmed this with two PeaDB servers: `SET before 1` before `REPLICAOF` appeared on the replica, but `SET after 2` on the master after sync never appeared on the replica.

Fix direction: after loading the RDB, keep the socket open, parse/apply the replication stream continuously, emit `REPLCONF ACK`, and use the backlog for reconnects.

4. High: nonblocking `WAIT` breaks pipelined reply ordering.

`WAIT` stores `session.wait_state` and returns an empty reply at `src/command.cpp:2030-2056`, but the server parser continues consuming subsequent commands from the same input buffer at `src/server.cpp:563-575`. A pipelined `WAIT 1 200` followed by `PING` returns `+PONG` immediately, before the deferred `WAIT` reply. That violates Redis protocol ordering for a blocking command.

Fix direction: when a command transitions the session into `blocked` or `wait_state`, stop parsing that client's remaining buffered input and leave it queued until the blocking reply is emitted.

5. High: the new GitHub integration job is likely broken in a clean checkout.

The PR workflow builds `build/peadb-server` and then runs `../scripts/ci/run_integration_tests.sh` from `build` (`.github/workflows/pr.yml:27-34`). The runner `cd`s to the repo root and invokes each Python test (`scripts/ci/run_integration_tests.sh:110-129`). Most tests launch `ROOT / "peadb-server"`; for example `tests/integration/test_m0_smoke.py:17-18`.

The repo-root `peadb-server` in this local checkout is an untracked symlink to `build/peadb-server`, not a tracked artifact. A clean GitHub checkout will not have it, so integration smoke will fail with `FileNotFoundError`.

Fix direction: standardize on `PEADB_BIN`, pass `build/peadb-server` from the runner, or set CMake's runtime output directory to the repo root intentionally.

6. Medium: compatibility gates are still not the executable truth model requested by the analyses.

`scripts/ci/check_command_test_coverage.py` is still explicitly mention-based (`scripts/ci/check_command_test_coverage.py:4-18`, `:72-75`), so it can pass from comments or golden text. The generated scoreboard detects only redis-py-style calls (`scripts/ci/generate_compat_scoreboard.py:44-53`), while this repo's tests mostly use helper calls like `cmd(s, "GET", ...)`; as committed, `compat/commands.generated.md:5` says all 207 commands are `stub`.

Fix direction: parse the actual test helpers, classify command execution accurately, fail CI when the generated scoreboard changes without being committed, and add differential/upstream-test status instead of only command-name presence.

7. Medium: the compatibility delta file is already stale.

`compat/delta.md:90-98` says `REPLICAOF` uses a Python helper, full-resync only, and lacks a bounded backlog. The code no longer shells out and does add a master-side backlog, but it still lacks continuous replica streaming. This undermines the intended single source of truth. The stream RDB delta at `compat/delta.md:60-68` is honest about data loss, but the implementation still allows successful saves that lose stream keys.

## Coverage against the four analyses

Addressed or partially addressed:

- Config precedence was fixed in `src/main.cpp` by loading `--config` first, then applying CLI overrides.
- BGSAVE child tracking/reaping and overlap rejection were added.
- RDB/AOF writes now use temp-file rename in the main save/rewrite paths.
- Write buffer compaction and per-read reply coalescing were improved.
- Some runtime observability, memory estimation, active expiry randomization, SCAN cursor work, zset ordered indexing, command inventory, and basic unit tests were added.
- CI workflow skeletons were added.

Not addressed or still incomplete:

- Stream RDB persistence remains data-losing.
- `BGREWRITEAOF` is now asynchronous, but unsafe because the parent fd is not switched and no rewrite diff is preserved.
- `appendfsync` exists as a hardcoded global but is not parsed from config or exposed through `CONFIG GET/SET`.
- `REPLICAOF` is not a continuous replication client.
- `PSYNC` partial resync is only partially present on the master side; replicas always request full sync and then disconnect.
- `WAIT` no longer sleeps the whole loop, but it violates pipelined reply ordering.
- `INFO replication` still reports `connected_slaves` from `g_connected_replicas`, which is not maintained for replica sessions.
- Compatibility scoreboards and command coverage gates are not reliable execution-based gates.
- Lua cjson/cmsgpack upstream failures are not proven fixed by Redis Tcl tests.
- Pub/Sub delivery/subscribed-mode tests, output-buffer classes, real memory accounting, `RespValue`, db-explicit APIs, `command.cpp` splitting, cluster-bus compatibility, and real ServerContext migration remain future work.

## Verification performed

- `cmake --build build -j4`: passed.
- `ctest --test-dir build --output-on-failure`: passed, 4/4 tests.
- `python3 scripts/ci/check_command_test_coverage.py`: passed, but the gate is mention-based.
- `python3 scripts/ci/check_compat_delta.py`: passed.
- `python3 scripts/ci/generate_compat_scoreboard.py`: regenerated the committed all-stub scoreboard unchanged.
- `clang-format --dry-run --Werror`: not run locally because `clang-format` is not installed in this environment.
- Custom socket probes confirmed the `WAIT`, stream RDB, `REPLICAOF`, and `BGREWRITEAOF` issues above.
