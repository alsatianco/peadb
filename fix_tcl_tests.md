# Plan: Fix remaining Redis Tcl test failures

## Symptom
running `scripts/run_all_tests.sh`, one test failed. Detail below.
```
==========================================
=== Upstream Redis Tcl tests
==========================================
=== Running unit/keyspace ===
=== Running unit/type/string ===
FAILED unit/type/string (log: /home/duck/git/peadb/artifacts/redis-stage-a/unit_type_string.log)
=== Running unit/expire ===
FAILED unit/expire (log: /home/duck/git/peadb/artifacts/redis-stage-a/unit_expire.log)
=== Running unit/multi ===
FAILED unit/multi (log: /home/duck/git/peadb/artifacts/redis-stage-a/unit_multi.log)
=== Running unit/scripting ===
FAILED unit/scripting (log: /home/duck/git/peadb/artifacts/redis-stage-a/unit_scripting.log)
Redis Stage-A suites failed: unit/type/string unit/expire unit/multi unit/scripting
Repro commands saved to /home/duck/git/peadb/artifacts/redis-stage-a/repro_commands.txt
--- FAIL: Upstream Redis Tcl tests ---
```

## Summary

18 test failures across 4 suites (16 `[err]` + 2 `[exception]`).  
Root-caused to **5 independent bugs**, fixable by **5 targeted patches**.

| Bug | Description | Files to change | Tests fixed |
|-----|-------------|----------------|-------------|
| A | SYNC phantom `{}` in replication stream | `src/command.cpp` | 15 |
| B | Lazy-expire DEL not replicated for SCAN / RANDOMKEY | `src/command.cpp` | 2 |
| C | cjson missing 4 functions | `src/lua_cjson_stub.c` | 1 (+unblocks remaining scripting tests) |
| D | XREAD/XREADGROUP BLOCK not rejected in Lua | `src/command.cpp` | 1 |
| E | Lua error messages don't match Redis format | `src/lua_engine.cpp` | 1 (2 sub-assertions) |

---

## Bug A — SYNC trailing `\r\n` phantom (Critical — 15 tests)

### Root cause

The SYNC handler (command.cpp line 1132) returns:

```cpp
return std::string("$0\r\n\r\n");
```

Redis's replication protocol sends `$<count>\r\n<data>` with **no** trailing
`\r\n` after the RDB payload.  The spurious `\r\n` stays in the socket buffer.

When the Tcl test helper `read_from_replication_stream` runs, it reads the
phantom `\r\n` as an empty RESP message.  Because `gets` in binary/lf mode
returns `\r` (one byte), `[string range "\r" 1 end]` yields `""`, and the
`for` loop condition `{$j < $count}` evaluates `{0 < ""}` which is false via
string comparison — so the function returns `{}` (empty list).

This shifts **every** subsequent replication-stream assertion by one position,
causing all pattern comparisons to fail.

### Fix

```cpp
// src/command.cpp  —  SYNC handler (line 1132)
- return std::string("$0\r\n\r\n");
+ return std::string("$0\r\n");
```

Also fix PSYNC (line 1184) which has the same issue:

```cpp
- reply += "$" + std::to_string(rdb_data.size()) + "\r\n" + rdb_data + "\r\n";
+ reply += "$" + std::to_string(rdb_data.size()) + "\r\n" + rdb_data;
```

### Tests fixed (15)

| Suite | Test name |
|-------|-----------|
| unit/type/string | GETDEL propagate as DEL command to replica |
| unit/type/string | GETEX without argument does not propagate to replica |
| unit/expire | All TTL in commands are propagated as absolute timestamp in replication stream |
| unit/expire | GETEX propagate as to replica as PERSIST, DEL, or nothing |
| unit/expire | Redis should not propagate the read command on lazy expire |
| unit/multi | MULTI / EXEC is not propagated (single write command) |
| unit/multi | MULTI / EXEC is propagated correctly (multiple commands) |
| unit/multi | MULTI / EXEC is propagated correctly (multiple commands with SELECT) |
| unit/multi | MULTI / EXEC is propagated correctly (empty transaction) |
| unit/multi | MULTI / EXEC is propagated correctly (read-only commands) |
| unit/multi | MULTI / EXEC is propagated correctly (write command, no effect) |
| unit/multi | MULTI / EXEC with REPLICAOF |
| unit/multi | MULTI and script timeout (**[exception]** — cascading from REPLICAOF test; the assertion failure prevents `r replicaof no one` from executing, leaving the server in slave mode) |

### Verification

After the fix, the first `read_from_replication_stream` call will correctly
block (non-blocking, retries 10× at 100 ms) until a real RESP command arrives.

---

## Bug B — Lazy-expire DEL replication for SCAN / RANDOMKEY (2 tests)

### Root cause

The SCAN handler calls `store().keys(pattern)`, which **silently erases**
expired keys during iteration without emitting any replication events.
Similarly, the RANDOMKEY handler calls `store().randomkey()`.

The function `append_lazy_expire_dels(session)` exists (command.cpp line 441)
and does exactly the right thing — it calls `store().collect_expired_keys()`
and emits `DEL` events for each — but it is **never called**.

Note: calling it *after* `keys()` / `randomkey()` won't work because those
methods already erased the expired keys.  It must be called **before**.

### Fix

**SCAN handler** (command.cpp ~line 2713):

```cpp
  // Before the existing store().keys() call, add:
  append_lazy_expire_dels(session);
  const auto ks = store().keys(pattern);
```

**RANDOMKEY handler** (command.cpp ~line 2687):

Change the unnamed `SessionState&` parameter to `SessionState& session` and add:

```cpp
  [](const std::vector<std::string>&, SessionState& session, bool&) {
    append_lazy_expire_dels(session);
    const auto k = store().randomkey();
    return k.has_value() ? encode_bulk(*k) : encode_null(RespVersion::Resp2);
  }
```

### Tests fixed (2)

Both also require Bug A fix.

| Suite | Test name |
|-------|-----------|
| unit/expire | SCAN: Lazy-expire should not be wrapped in MULTI/EXEC |
| unit/expire | RANDOMKEY: Lazy-expire should not be wrapped in MULTI/EXEC |

### Notes

- These tests are tagged `{needs:debug}` — they rely on `DEBUG SET-ACTIVE-EXPIRE 0`
  which PeaDB already supports (`g_active_expire` global).
- The GET handler already does lazy-expire DEL replication correctly
  (command.cpp line 2162), so the pattern is established in the codebase.

---

## Bug C — cjson missing functions (1 test + prevents exception cascade)

### Root cause

The `cjson_funcs[]` table in `lua_cjson_stub.c` only registers three
functions: `encode`, `decode`, `encode_empty_table_as_object`.

The "EVAL - JSON smoke test" calls four additional functions:

1. `cjson.encode_keep_buffer(false)` — controls buffer reuse
2. `cjson.encode_max_depth(1)` — limits encode nesting depth
3. `cjson.decode_max_depth(1)` — limits decode nesting depth
4. `cjson.encode_invalid_numbers(true)` — allows NaN/Inf encoding

The missing `encode_keep_buffer` causes a Lua exception that **aborts the
entire test client**, skipping all subsequent scripting tests.

### Fix

Add four functions to `lua_cjson_stub.c`:

**`encode_keep_buffer(bool)`** — No-op stub that returns the cjson module
(chainable API pattern):

```c
static int cjson_encode_keep_buffer(lua_State *L) {
    (void)luaL_checkany(L, 1);
    lua_getglobal(L, "cjson");
    return 1;
}
```

**`encode_max_depth(int)`** — Stores the depth limit in a module-level
variable.  The existing `json_encode_value` already has a depth parameter;
change its hard-coded `128` to use this variable:

```c
static int g_encode_max_depth = 128;

static int cjson_encode_max_depth(lua_State *L) {
    int d = (int)luaL_checkinteger(L, 1);
    g_encode_max_depth = d;
    lua_getglobal(L, "cjson");
    return 1;
}
```

Update `json_encode_value`:

```c
- if (depth > 128)
+ if (depth > g_encode_max_depth)
```

**`decode_max_depth(int)`** — Similarly stores a decode depth limit.  Add a
`depth` parameter to `json_decode_value` and check it:

```c
static int g_decode_max_depth = 1000;

static int cjson_decode_max_depth(lua_State *L) {
    int d = (int)luaL_checkinteger(L, 1);
    g_decode_max_depth = d;
    lua_getglobal(L, "cjson");
    return 1;
}
```

Thread `depth` through `json_decode_value` and check at entry:

```c
if (depth > g_decode_max_depth)
    return luaL_error(L, "cjson.decode: too many nested data structures");
```

**`encode_invalid_numbers(bool)`** — Stores a flag.  Update
`json_encode_value`'s `LUA_TNUMBER` case:

```c
static int g_encode_invalid_numbers = 0;

static int cjson_encode_invalid_numbers(lua_State *L) {
    g_encode_invalid_numbers = lua_toboolean(L, 1);
    lua_getglobal(L, "cjson");
    return 1;
}
```

Update the number encoding in `json_encode_value`:

```c
  if (isinf(n) || isnan(n)) {
-     luaL_error(L, "cjson.encode: cannot encode inf/nan");
+     if (!g_encode_invalid_numbers)
+         luaL_error(L, "cjson.encode: cannot encode inf/nan");
+     else if (isnan(n))
+         luaL_addstring(b, "null");
+     else
+         luaL_addstring(b, n > 0 ? "1e+9999" : "-1e+9999");
  }
```

Register all four in `cjson_funcs[]`:

```c
{"encode_keep_buffer",     cjson_encode_keep_buffer},
{"encode_max_depth",       cjson_encode_max_depth},
{"decode_max_depth",       cjson_decode_max_depth},
{"encode_invalid_numbers", cjson_encode_invalid_numbers},
```

### Tests fixed (1)

| Suite | Test name |
|-------|-----------|
| unit/scripting | EVAL - JSON smoke test |

The `[exception]` is eliminated, which unblocks the rest of the scripting
test suite (cmsgpack tests, etc.) that were being skipped due to the test
client abort.

---

## Bug D — XREAD / XREADGROUP BLOCK error in Lua context (1 test)

### Root cause

Both XREAD (command.cpp ~line 4117) and XREADGROUP (~line 4046) silently
parse and ignore the BLOCK option.  In Redis, using BLOCK from a Lua script
returns a specific error.

### Fix

In both handlers, when `BLOCK` option is detected, check `in_lua_script_context()`:

**XREAD** (~line 4117):

```cpp
  if (opt == "BLOCK") {
      if (i + 1 >= args.size()) return encode_error("syntax error");
      std::int64_t v = 0;
      if (!parse_i64(args[i + 1], v) || v < 0)
          return encode_error("value is not an integer or out of range");
+     if (in_lua_script_context())
+         return encode_error("xread command is not allowed with BLOCK option from scripts");
      i += 2;
      continue;
  }
```

**XREADGROUP** (~line 4046):

```cpp
  if (opt == "BLOCK") {
      if (i + 1 >= args.size()) return encode_error("syntax error");
      std::int64_t block = 0;
      if (!parse_i64(args[i + 1], block) || block < 0)
          return encode_error("value is not an integer or out of range");
+     if (in_lua_script_context())
+         return encode_error("xreadgroup command is not allowed with BLOCK option from scripts");
      i += 2;
      continue;
  }
```

### Tests fixed (1)

| Suite | Test name |
|-------|-----------|
| unit/scripting | EVAL - Scripts can't run XREAD and XREADGROUP with BLOCK option |

---

## Bug E — Lua error messages don't match Redis format (1 test)

### Root cause

When `redis.call()` dispatches a command that fails, PeaDB passes through the
raw error text from `handle_command`:

- `"ERR unknown command"` (PeaDB) vs `"Unknown Redis command called from script"` (Redis)
- `"ERR wrong number of arguments for 'get' command"` (PeaDB) vs `"Wrong number of args calling Redis command from script"` (Redis)

The tests pattern-match on `*Unknown Redis*` and `*number of args*`.

### Fix

In `lua_redis_call` (lua_engine.cpp ~line 603), when `val.type == RespValue::Error`,
intercept the error string before raising it:

```cpp
if (val.type == RespValue::Error) {
    std::string err = val.str;
    // Rewrite error messages to match Redis Lua-context format
    if (err.find("unknown command") != std::string::npos) {
        err = "@user_script:0: Unknown Redis command called from script";
    } else if (err.find("wrong number of arguments") != std::string::npos) {
        err = "@user_script:0: Wrong number of args calling Redis command from script";
    }
    lua_pushstring(L, err.c_str());
    return lua_error(L);
}
```

### Tests fixed (1 — 2 sub-assertions)

| Suite | Test name |
|-------|-----------|
| unit/scripting | EVAL - redis.call variant raises a Lua error on Redis cmd error (1) — sub-assertions for `*Unknown Redis*` and `*number of args*` |

The third sub-assertion (`*against a key*` for WRONGTYPE) already passes.

---

## Implementation order

1. **Bug A** — single-line change, highest impact (15 tests)
2. **Bug C** — ~60 lines of C, unblocks the scripting test client from aborting
3. **Bug B** — two small additions, completes the expire suite
4. **Bug D** — two 2-line additions
5. **Bug E** — 6-line change in lua_engine.cpp

Build & run after each fix to verify incrementally.

## Verification command

```bash
cd /home/duck/git/peadb && make -C build-asan -j$(nproc) && \
  bash scripts/run_all_tests.sh 2>&1 | tail -20
```

To test individual suites:

```bash
bash scripts/redis/run_redis_tests.sh unit/type/string
bash scripts/redis/run_redis_tests.sh unit/expire
bash scripts/redis/run_redis_tests.sh unit/multi
bash scripts/redis/run_redis_tests.sh unit/scripting
```

---

## Codex feedback

Overall, this is a strong and actionable root-cause analysis. The failure clustering, causal links, and patch ordering are well reasoned.

### What looks correct

- **Bug A diagnosis is highly credible**: the extra CRLF after an RDB bulk payload can absolutely desynchronize downstream RESP parsing and explain the broad replication assertion drift.
- **Bug B timing detail is important and correct**: lazy-expire DEL propagation must happen **before** calls that may erase expired keys (`keys()` / `randomkey()`), otherwise events are lost.
- **Bug C identifies a real contract gap**: Redis tests rely on several `cjson` API knobs even when behavior is mostly permissive; missing symbols can abort script-side progression.
- **Bug D expected Lua restriction is aligned with Redis behavior**: rejecting blocking stream reads from scripts is consistent.
- **Bug E message-shape normalization is pragmatic**: matching Redis Lua-context wording is often required by Tcl pattern assertions.

### Suggested guardrails before/while landing

1. **Bug A (SYNC/PSYNC framing):**
  - Confirm no other replication paths append trailing CRLF after binary bulk payloads.
  - Add a small regression test that validates exact byte framing for empty/non-empty RDB payloads.

2. **Bug C (cjson depth/flags):**
  - Clamp invalid depth values (`<= 0`) with Redis-compatible error text if possible.
  - Ensure new globals are process-safe for current server model (single-threaded is fine).
  - Verify recursion checks are applied consistently at all decode recursion entry points.

3. **Bug E (error rewrite):**
  - Keep rewrites narrow (only the two known patterns) to avoid masking unrelated command errors.
  - Preserve existing WRONGTYPE path unchanged, as noted.

### Minor implementation note

- For `encode_keep_buffer`, accepting and ignoring the argument is fine for test compatibility; returning the module table (chainable style) is the right choice.

### Confidence

- **High confidence** on Bugs A/B/D/E fixes and expected test impact.
- **Medium-high confidence** on Bug C due to slightly wider surface area (recursive decode plumbing + config semantics), but plan is solid.

---

## GPT feedback

This is a strong root-cause writeup: it clusters failures correctly, explains the Tcl harness behavior in a believable way, and proposes small, test-targeted patches.

### The only “double-check me” area: RDB framing in SYNC/PSYNC (Bug A)

Your conclusion (extra `\r\n` desyncing `read_from_replication_stream`) is very plausible, but it hinges on an implementation detail that’s easy to get subtly wrong:

- In Redis replication, the RDB transfer is *length-prefixed* (starts with `$<len>\r\n`) and the receiver reads exactly `<len>` bytes; Redis does not rely on scanning a RESP-style terminating `\r\n` after the payload.
- If PeaDB currently appends a trailing `\r\n` after the payload, that can indeed leave a spare empty line in the socket buffer for the Tcl helper to consume as a phantom message.

Practical guardrail before landing:

- Capture a real Redis `SYNC`/`PSYNC` exchange (tcpdump/wireshark or a minimal client) and confirm whether the payload is followed immediately by the next command bytes, with no extra CRLF beyond the `$<len>\r\n` header.
- After the patch, validate with a byte-level test (or even a small script) that: `+FULLRESYNC...\r\n$<len>\r\n` is followed by exactly `<len>` bytes, and then the next RESP array begins immediately.

### Bug B (lazy-expire replication): good call on ordering

Agree that `append_lazy_expire_dels(session)` must run *before* `keys()`/`randomkey()` if those helpers eagerly purge expired keys. One extra thing to watch:

- If `append_lazy_expire_dels()` can emit multiple `DEL`s, ensure it doesn’t accidentally wrap/queue them as part of a later MULTI/EXEC response path for SCAN/RANDOMKEY. (Your note suggests it already emits raw replication events, which is what the tests want.)

### Bug C (cjson knobs): keep behavior permissive and chainable

The missing symbols diagnosis is spot on. A few small compatibility notes to reduce surprises:

- Consider clamping invalid depth values (e.g., `<= 0`) to a sane minimum or returning a Redis-ish error; the upstream test likely uses positive values, but this avoids footguns.
- For `encode_max_depth`/`decode_max_depth`, make sure the depth check applies at all recursive entry points (arrays/objects), not only one path.
- For `encode_invalid_numbers(true)`: Redis’s cjson behavior is mostly about *not erroring*; your suggested `null`/`±1e+9999` output is probably fine for tests, but it’s worth ensuring your float formatting doesn’t produce `nan`/`inf` strings in any locale-dependent way.

### Bug D (XREAD/GROUP BLOCK in Lua): narrow and correct

Agree with rejecting `BLOCK` only in script context, while keeping the non-Lua parsing behavior unchanged. One detail:

- Match Redis’s error *string* as closely as possible (the Tcl suite tends to pattern match). Your proposed messages look right; if a test still fails, it’s likely wording/prefix rather than logic.

### Bug E (Lua error message rewrite): keep it intentionally minimal

The rewrite approach is pragmatic. Two suggestions to keep it robust:

- Keep matching strictly to the two patterns you listed (unknown command + wrong #args) so you don’t mask unrelated errors.
- Preserve any existing `@user_script:<line>:` style the engine may already be emitting elsewhere, so stack traces remain consistent.

### Suggested verification flow (fast)

If you want tighter confidence while iterating, run suites in this order after each patch:

1. `unit/type/string` (fastest signal for Bug A)
2. `unit/expire` (catches Bug B interactions)
3. `unit/multi` (replication stream alignment + REPLICAOF cleanup)
4. `unit/scripting` (cjson + Lua-context errors)
