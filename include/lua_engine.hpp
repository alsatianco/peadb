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

} // namespace peadb
