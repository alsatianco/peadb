#pragma once

#include "command.hpp"

#include <string>
#include <vector>
#include <functional>

namespace peadb {

// Initialize the Lua 5.1 engine (call once at startup).
void lua_engine_init();

// Destroy the Lua engine (call at shutdown).
void lua_engine_shutdown();

// Evaluate a Lua script with the given keys and argv.
// Returns the RESP-encoded reply.
std::string lua_engine_eval(
    const std::string& script,
    const std::vector<std::string>& keys,
    const std::vector<std::string>& argv,
    SessionState& session);

// Set a callback used by redis.call / redis.pcall inside scripts
// to dispatch commands to the PeaDB command handler.
using CommandDispatchFn = std::function<std::string(const std::vector<std::string>&, SessionState&, bool&)>;
void lua_engine_set_dispatch(CommandDispatchFn fn);

// Set readonly mode for script execution (used by EVAL_RO / EVALSHA_RO).
void lua_engine_set_readonly(bool readonly);

// Returns whether the most recent script error was produced by an inner
// Redis command (redis.call / redis.pcall path), and clears the flag.
bool lua_engine_consume_last_error_from_redis();

// ─── FUNCTION / FCALL (Lua function libraries) ───────────────────────────────
// Function flags (match Redis script flags bitmask).
enum FunctionFlags {
  kFuncNoWrites       = 1 << 0,
  kFuncAllowOom       = 1 << 1,
  kFuncAllowStale     = 1 << 2,
  kFuncNoCluster      = 1 << 3,
  kFuncAllowCrossSlot = 1 << 4,
};

struct LuaRegisteredFunction {
  std::string name;
  std::string description;
  int flags = 0;
  int callback_ref = -1;  // luaL_ref into the Lua registry (LUA_NOREF when unset)
};

// Compile + run a library body (the code AFTER the shebang line) in the restricted
// "load" environment, collecting the functions it registers. On success returns true and
// fills `out`. On failure returns false and sets `err` to the exact Redis message
// (without the leading "ERR "), e.g. "Error compiling function: ...",
// "Error registering functions: ...", "No functions registered", "FUNCTION LOAD timeout".
bool lua_engine_load_library(const std::string& body,
                             std::vector<LuaRegisteredFunction>& out,
                             std::string& err);

// Invoke a registered function callback with keys/args. Returns the RESP reply (or a
// RESP error string). `no_writes` puts the call in read-only mode (redis.call blocks writes).
std::string lua_engine_fcall(int callback_ref,
                             const std::vector<std::string>& keys,
                             const std::vector<std::string>& argv,
                             SessionState& session,
                             bool no_writes);

// Release a callback ref (on library delete/flush/replace).
void lua_engine_unref_function(int callback_ref);

} // namespace peadb
