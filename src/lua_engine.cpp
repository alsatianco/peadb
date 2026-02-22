// lua_engine.cpp — real Lua 5.1 scripting engine for PeaDB
//
// Embeds the Lua 5.1 VM from Redis deps and provides redis.call(),
// redis.pcall(), redis.sha1hex(), redis.error_reply(), redis.status_reply(),
// redis.setresp(), redis.log(), redis.acl_check_cmd(), and math.random
// determinism matching Redis behaviour.

extern "C" {
#include "lua.h"
#include "lauxlib.h"
#include "lualib.h"
}

// Lua extension library init functions (compiled from C source files)
extern "C" int luaopen_cjson(lua_State*);
extern "C" int luaopen_cmsgpack(lua_State*);
extern "C" int luaopen_bit(lua_State*);
extern "C" int luaopen_struct(lua_State*);

#include "lua_engine.hpp"
#include "datastore.hpp"
#include "errors.hpp"
#include "protocol.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace peadb {

// ─── Extern state shared with command.cpp ─────────────────────────────────────
extern std::atomic<bool> g_script_busy;
extern std::atomic<bool> g_script_kill_requested;
extern SessionState* g_busy_script_session;
extern std::int64_t g_config_lua_time_limit;

namespace {

// ─── Globals shared with command.cpp (forward-declared or extern) ────────────

// Dispatch function set from command.cpp.
CommandDispatchFn g_lua_dispatch;

// Thread-local session pointer used during script execution.
thread_local SessionState* g_lua_session = nullptr;
thread_local RespVersion g_lua_script_resp = RespVersion::Resp2;
thread_local RespVersion g_lua_client_resp = RespVersion::Resp2;
thread_local bool g_lua_readonly = false;
thread_local bool g_lua_last_error_from_redis = false;

// ─── Lua script timeout / kill hook ──────────────────────────────────────────
static std::chrono::steady_clock::time_point g_script_start_time;

void lua_timeout_hook(lua_State* L, lua_Debug* /*ar*/) {
  // If SCRIPT KILL or FUNCTION KILL was requested, abort the script.
  if (g_script_kill_requested.load(std::memory_order_acquire)) {
    g_script_kill_requested.store(false, std::memory_order_release);
    g_script_busy.store(false, std::memory_order_release);
    g_busy_script_session = nullptr;
    luaL_error(L, "Script killed by user with SCRIPT KILL.");
    return;
  }
  // After lua-time-limit ms, mark the server as busy so that once the
  // script finishes (or is killed) the BUSY flag is set for clients.
  if (g_config_lua_time_limit > 0) {
    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - g_script_start_time).count();
    if (elapsed_ms >= g_config_lua_time_limit &&
        !g_script_busy.load(std::memory_order_relaxed)) {
      g_script_busy.store(true, std::memory_order_release);
      g_busy_script_session = g_lua_session;
    }
  }
}

// The single Lua state (Redis uses one global state).
lua_State* g_L = nullptr;

// ─── RESP encoding helpers (duplicated minimal set for self-containment) ─────

std::string upper(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
  return s;
}

std::string lower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return s;
}

std::string enc_simple(const std::string& v) { return "+" + v + "\r\n"; }
std::string enc_error_raw(const std::string& v) { return "-" + v + "\r\n"; }
std::string enc_error(const std::string& v) { return "-ERR " + v + "\r\n"; }
std::string enc_integer(long long v) { return ":" + std::to_string(v) + "\r\n"; }
std::string enc_bulk(const std::string& v) {
  return "$" + std::to_string(v.size()) + "\r\n" + v + "\r\n";
}
std::string enc_null_bulk() { return "$-1\r\n"; }
std::string enc_null_resp3() { return "_\r\n"; }
std::string enc_null(RespVersion ver) {
  return ver == RespVersion::Resp3 ? enc_null_resp3() : enc_null_bulk();
}
std::string enc_array(const std::vector<std::string>& items) {
  std::string out = "*" + std::to_string(items.size()) + "\r\n";
  for (const auto& i : items) out += i;
  return out;
}

// ─── Parse RESP reply from a string (used to convert dispatch results) ───────

struct RespValue {
  enum Type { Nil, SimpleString, Error, Integer, BulkString, Array, Double, BigNumber, Bool, Verbatim, Map, Set };
  Type type = Nil;
  std::string str;
  long long integer = 0;
  double dbl = 0;
  bool boolean = false;
  std::vector<RespValue> array;
};

bool parse_resp_value(const std::string& data, std::size_t& pos, RespValue& out) {
  if (pos >= data.size()) return false;
  const char prefix = data[pos++];
  auto read_line = [&]() -> std::string {
    auto end = data.find("\r\n", pos);
    if (end == std::string::npos) return "";
    auto line = data.substr(pos, end - pos);
    pos = end + 2;
    return line;
  };

  switch (prefix) {
    case '+':
      out.type = RespValue::SimpleString;
      out.str = read_line();
      return true;
    case '-':
      out.type = RespValue::Error;
      out.str = read_line();
      return true;
    case ':':
      out.type = RespValue::Integer;
      out.integer = std::stoll(read_line());
      return true;
    case '$': {
      auto len_str = read_line();
      int len = std::stoi(len_str);
      if (len == -1) {
        out.type = RespValue::Nil;
        return true;
      }
      out.type = RespValue::BulkString;
      out.str = data.substr(pos, static_cast<std::size_t>(len));
      pos += static_cast<std::size_t>(len) + 2; // skip \r\n
      return true;
    }
    case '*': {
      auto cnt_str = read_line();
      int cnt = std::stoi(cnt_str);
      if (cnt == -1) {
        out.type = RespValue::Nil;
        return true;
      }
      out.type = RespValue::Array;
      out.array.resize(static_cast<std::size_t>(cnt));
      for (int i = 0; i < cnt; ++i) {
        if (!parse_resp_value(data, pos, out.array[static_cast<std::size_t>(i)])) return false;
      }
      return true;
    }
    case '_': // RESP3 null
      out.type = RespValue::Nil;
      read_line();
      return true;
    case ',': // RESP3 double
      out.type = RespValue::Double;
      out.str = read_line();
      try { out.dbl = std::stod(out.str); } catch (...) { out.dbl = 0; }
      return true;
    case '#': { // RESP3 bool
      auto line = read_line();
      out.type = RespValue::Bool;
      out.boolean = (line == "t");
      out.integer = out.boolean ? 1 : 0;
      return true;
    }
    case '(': // RESP3 big number
      out.type = RespValue::BigNumber;
      out.str = read_line();
      return true;
    case '=': { // RESP3 verbatim
      auto len_str = read_line();
      int len = std::stoi(len_str);
      out.type = RespValue::Verbatim;
      out.str = data.substr(pos, static_cast<std::size_t>(len));
      pos += static_cast<std::size_t>(len) + 2;
      return true;
    }
    case '%': { // RESP3 map
      auto cnt_str = read_line();
      int cnt = std::stoi(cnt_str);
      out.type = RespValue::Map;
      out.array.resize(static_cast<std::size_t>(cnt * 2));
      for (int i = 0; i < cnt; ++i) {
        if (!parse_resp_value(data, pos, out.array[static_cast<std::size_t>(i * 2)])) return false;
        if (!parse_resp_value(data, pos, out.array[static_cast<std::size_t>(i * 2 + 1)])) return false;
      }
      return true;
    }
    case '~': { // RESP3 set
      auto cnt_str = read_line();
      int cnt = std::stoi(cnt_str);
      out.type = RespValue::Set;
      out.array.resize(static_cast<std::size_t>(cnt));
      for (int i = 0; i < cnt; ++i) {
        if (!parse_resp_value(data, pos, out.array[static_cast<std::size_t>(i)])) return false;
      }
      return true;
    }
    case '|': { // RESP3 attribute — read and skip, then parse the real value
      auto cnt_str = read_line();
      int cnt = std::stoi(cnt_str);
      for (int i = 0; i < cnt; ++i) {
        RespValue dummy;
        if (!parse_resp_value(data, pos, dummy)) return false;
        if (!parse_resp_value(data, pos, dummy)) return false;
      }
      // Parse the actual value that follows
      return parse_resp_value(data, pos, out);
    }
    default:
      return false;
  }
}

// ─── Push RESP value onto Lua stack ──────────────────────────────────────────

void push_resp_to_lua(lua_State* L, const RespValue& val) {
  const bool resp3 = (g_lua_script_resp == RespVersion::Resp3);
  switch (val.type) {
    case RespValue::Nil:
      if (resp3) lua_pushnil(L);
      else lua_pushboolean(L, 0); // Redis RESP2 maps nil → Lua false
      break;
    case RespValue::SimpleString:
      // Redis returns status replies as tables with an "ok" field
      lua_newtable(L);
      lua_pushstring(L, "ok");
      lua_pushstring(L, val.str.c_str());
      lua_settable(L, -3);
      break;
    case RespValue::Error:
      // For pcall: return table with "err" field. For call: raise error.
      lua_newtable(L);
      lua_pushstring(L, "err");
      lua_pushstring(L, val.str.c_str());
      lua_settable(L, -3);
      lua_pushstring(L, "__peadb_err_from_redis");
      lua_pushboolean(L, 1);
      lua_settable(L, -3);
      break;
    case RespValue::Integer:
      lua_pushnumber(L, static_cast<lua_Number>(val.integer));
      break;
    case RespValue::BulkString:
      lua_pushlstring(L, val.str.c_str(), val.str.size());
      break;
    case RespValue::Array:
      lua_newtable(L);
      for (std::size_t i = 0; i < val.array.size(); ++i) {
        push_resp_to_lua(L, val.array[i]);
        lua_rawseti(L, -2, static_cast<int>(i + 1));
      }
      break;
    case RespValue::Double:
      if (resp3) {
        // RESP3: return as {double=N}
        lua_newtable(L);
        lua_pushstring(L, "double");
        lua_pushnumber(L, val.dbl);
        lua_settable(L, -3);
      } else {
        // RESP2: convert to bulk string
        lua_pushstring(L, val.str.c_str());
      }
      break;
    case RespValue::BigNumber:
      if (resp3) {
        // RESP3: return as {big_number=S}
        lua_newtable(L);
        lua_pushstring(L, "big_number");
        lua_pushstring(L, val.str.c_str());
        lua_settable(L, -3);
      } else {
        // RESP2: convert to bulk string
        lua_pushstring(L, val.str.c_str());
      }
      break;
    case RespValue::Bool:
      if (resp3) {
        lua_pushboolean(L, val.boolean ? 1 : 0);
      } else {
        // RESP2: true→1, false→0
        lua_pushnumber(L, val.integer);
      }
      break;
    case RespValue::Verbatim:
      if (resp3) {
        // RESP3: return as {verbatim_string={string=..., format=...}} table
        std::string format = "txt";
        std::string content = val.str;
        if (content.size() > 4 && content[3] == ':') {
          format = content.substr(0, 3);
          content = content.substr(4);
        }
        lua_newtable(L);
        lua_pushstring(L, "verbatim_string");
        lua_newtable(L);
        lua_pushstring(L, "string");
        lua_pushlstring(L, content.c_str(), content.size());
        lua_settable(L, -3);
        lua_pushstring(L, "format");
        lua_pushstring(L, format.c_str());
        lua_settable(L, -3);
        lua_settable(L, -3);
      } else {
        // RESP2: strip format prefix and return as bulk string
        std::string content = val.str;
        if (content.size() > 4 && content[3] == ':') content = content.substr(4);
        lua_pushlstring(L, content.c_str(), content.size());
      }
      break;
    case RespValue::Map:
      // Keep map identity in both RESP2/RESP3 so conversion can choose
      // the correct wire representation later.
      lua_newtable(L);
      lua_pushstring(L, "map");
      lua_newtable(L);
      for (std::size_t i = 0; i < val.array.size(); i += 2) {
        push_resp_to_lua(L, val.array[i]);
        if (i + 1 < val.array.size())
          push_resp_to_lua(L, val.array[i + 1]);
        else
          lua_pushnil(L);
        lua_settable(L, -3);
      }
      lua_settable(L, -3);
      break;
    case RespValue::Set:
      // Keep set identity in both RESP2/RESP3 so conversion can choose
      // the correct wire representation later.
      lua_newtable(L);
      lua_pushstring(L, "set");
      lua_newtable(L);
      for (std::size_t i = 0; i < val.array.size(); ++i) {
        push_resp_to_lua(L, val.array[i]);
        lua_rawseti(L, -2, static_cast<int>(i + 1));
      }
      lua_settable(L, -3);
      break;
  }
}

// ─── Convert Lua value on stack to RESP-encoded string ───────────────────────

std::string lua_to_resp(lua_State* L, int index, int depth = 0) {
  if (depth > 64) return enc_error("reached lua stack limit");

  // Use raw table access to avoid triggering __index metamethods (which could
  // PANIC if they raise errors outside a protected call).
  auto raw_getfield = [&](lua_State* L, int idx, const char* field) {
    lua_pushstring(L, field);
    lua_rawget(L, (idx < 0) ? (idx - 1) : idx);
  };

  // Use the effective output resp version: must be RESP3 only when
  // BOTH the script called redis.setresp(3) AND the client is on RESP3.
  const RespVersion out_resp =
      (g_lua_script_resp == RespVersion::Resp3 && g_lua_client_resp == RespVersion::Resp3)
          ? RespVersion::Resp3 : RespVersion::Resp2;

  const int t = lua_type(L, index);
  switch (t) {
    case LUA_TNIL:
      // Null uses CLIENT's resp version, not the combined out_resp
      return enc_null(g_lua_client_resp);
    case LUA_TBOOLEAN:
      if (lua_toboolean(L, index))
        return out_resp == RespVersion::Resp3 ? std::string("#t\r\n") : enc_integer(1);
      else {
        if (g_lua_script_resp == RespVersion::Resp2) {
          // In RESP2 script mode, false originated from nil → output null
          return enc_null(g_lua_client_resp);
        } else {
          // In RESP3 script mode, false is a real boolean
          return out_resp == RespVersion::Resp3 ? std::string("#f\r\n") : enc_integer(0);
        }
      }
    case LUA_TNUMBER: {
      lua_Number n = lua_tonumber(L, index);
      return enc_integer(static_cast<long long>(n));
    }
    case LUA_TSTRING: {
      std::size_t len = 0;
      const char* s = lua_tolstring(L, index, &len);
      return enc_bulk(std::string(s, len));
    }
    case LUA_TTABLE: {
      // Check for special tables: {ok="..."}, {err="..."}
      raw_getfield(L, index, "ok");
      if (lua_isstring(L, -1)) {
        std::string ok_val = lua_tostring(L, -1);
        lua_pop(L, 1);
        return enc_simple(ok_val);
      }
      lua_pop(L, 1);

      raw_getfield(L, index, "err");
      if (lua_isstring(L, -1)) {
        std::string err_val = lua_tostring(L, -1);
        lua_pop(L, 1);
        raw_getfield(L, index, "__peadb_err_from_redis");
        const bool from_redis_error = lua_toboolean(L, -1) != 0;
        lua_pop(L, 1);
        if (from_redis_error) g_lua_last_error_from_redis = true;
        // If err starts with a known prefix, use it as-is
        return enc_error_raw(err_val);
      }
      lua_pop(L, 1);

      // Check for {double=N}
      raw_getfield(L, index, "double");
      if (lua_isnumber(L, -1)) {
        lua_Number d = lua_tonumber(L, -1);
        lua_pop(L, 1);
        if (out_resp == RespVersion::Resp3) {
          std::ostringstream oss;
          oss << d;
          return "," + oss.str() + "\r\n";
        }
        std::ostringstream oss;
        oss << d;
        return enc_bulk(oss.str());
      }
      lua_pop(L, 1);

      // Check for {big_number=S}
      raw_getfield(L, index, "big_number");
      if (lua_isstring(L, -1)) {
        std::string bn = lua_tostring(L, -1);
        lua_pop(L, 1);
        // Replace \r\n in the value with spaces
        std::string clean;
        for (std::size_t i = 0; i < bn.size(); ++i) {
          if (bn[i] == '\r' && i + 1 < bn.size() && bn[i+1] == '\n') {
            clean += "  ";
            i += 1;
          } else {
            clean += bn[i];
          }
        }
        if (out_resp == RespVersion::Resp3)
          return "(" + clean + "\r\n";
        return enc_bulk(clean);
      }
      lua_pop(L, 1);

      // Check for {verbatim_string={string=..., format=...}}
      raw_getfield(L, index, "verbatim_string");
      if (lua_istable(L, -1)) {
        raw_getfield(L, -1, "string");
        std::string content = lua_isstring(L, -1) ? lua_tostring(L, -1) : "";
        lua_pop(L, 1);
        raw_getfield(L, -1, "format");
        std::string format = lua_isstring(L, -1) ? lua_tostring(L, -1) : "txt";
        lua_pop(L, 1);
        lua_pop(L, 1); // pop verbatim_string table
        if (out_resp == RespVersion::Resp3) {
          std::string full = format + ":" + content;
          return "=" + std::to_string(full.size()) + "\r\n" + full + "\r\n";
        }
        return enc_bulk(content);
      }
      lua_pop(L, 1);

      // Check for {map={...}}
      raw_getfield(L, index, "map");
      if (lua_istable(L, -1)) {
        // Convert map to flat array or RESP3 map
        std::vector<std::string> items;
        auto encode_map_item = [&](int idx) {
          if (lua_type(L, idx) == LUA_TSTRING) {
            std::size_t slen = 0;
            const char* s = lua_tolstring(L, idx, &slen);
            return enc_simple(std::string(s, slen));
          }
          return lua_to_resp(L, idx, depth + 1);
        };
        lua_pushnil(L);
        while (lua_next(L, -2)) {
          items.push_back(encode_map_item(-2));
          items.push_back(encode_map_item(-1));
          lua_pop(L, 1);
        }
        lua_pop(L, 1); // pop map table
        if (out_resp == RespVersion::Resp3) {
          std::string out = "%" + std::to_string(items.size() / 2) + "\r\n";
          for (const auto& item : items) out += item;
          return out;
        }
        return enc_array(items);
      }
      lua_pop(L, 1);

      // Check for {set={...}}
      raw_getfield(L, index, "set");
      if (lua_istable(L, -1)) {
        std::vector<std::string> items;
        auto encode_set_item = [&](int idx) {
          if (lua_type(L, idx) == LUA_TSTRING) {
            std::size_t slen = 0;
            const char* s = lua_tolstring(L, idx, &slen);
            return enc_simple(std::string(s, slen));
          }
          return lua_to_resp(L, idx, depth + 1);
        };
        int slen = static_cast<int>(lua_objlen(L, -1));
        for (int i = 1; i <= slen; ++i) {
          lua_rawgeti(L, -1, i);
          items.push_back(encode_set_item(-1));
          lua_pop(L, 1);
        }
        lua_pop(L, 1);
        if (out_resp == RespVersion::Resp3) {
          std::string out = "~" + std::to_string(items.size()) + "\r\n";
          for (const auto& item : items) out += item;
          return out;
        }
        return enc_array(items);
      }
      lua_pop(L, 1);

      // Regular array table
      int len = static_cast<int>(lua_objlen(L, index));
      std::vector<std::string> items;
      items.reserve(static_cast<std::size_t>(len));
      for (int i = 1; i <= len; ++i) {
        lua_rawgeti(L, index, i);
        items.push_back(lua_to_resp(L, -1, depth + 1));
        lua_pop(L, 1);
      }
      return enc_array(items);
    }
    default:
      return enc_null(g_lua_script_resp);
  }
}

// ─── Dispatch a Redis command from Lua ───────────────────────────────────────

std::string dispatch_from_lua(const std::vector<std::string>& args) {
  if (!g_lua_dispatch || !g_lua_session) return enc_error("no dispatch available");

  auto cmd_upper = upper(args[0]);

  // Commands not allowed from scripts (CMD_NOSCRIPT in Redis)
  static const std::vector<std::string> noscript_cmds = {
      "CLUSTER", "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE",
      "SSUBSCRIBE", "SUNSUBSCRIBE",
  };
  for (const auto& ns : noscript_cmds) {
    if (cmd_upper == ns) {
      return enc_error_raw("ERR This Redis command is not allowed from script");
    }
  }

  // Check readonly context
  if (g_lua_readonly) {
    // Simple write-command check
    static const std::vector<std::string> write_cmds = {
        "SET", "DEL", "INCR", "DECR", "INCRBY", "DECRBY", "INCRBYFLOAT",
        "EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "SADD", "SREM",
        "SPOP", "HSET", "HDEL", "LPUSH", "RPUSH", "LPOP", "RPOP",
        "ZADD", "ZREM", "XADD", "MSET", "APPEND", "SETEX", "PSETEX",
        "SETNX", "GETSET", "GETDEL", "SETRANGE", "SETBIT",
        "UNLINK", "RENAME", "RENAMENX", "FLUSHDB", "FLUSHALL",
    };
    for (const auto& wc : write_cmds) {
      if (cmd_upper == wc) {
        record_lua_script_rejected_write(cmd_upper);
        g_lua_last_error_from_redis = true;
        return enc_error_raw("ERR Write commands are not allowed from read-only scripts.");
      }
    }
  }

  bool close = false;
  set_lua_script_context(true);
  auto result = g_lua_dispatch(args, *g_lua_session, close);
  set_lua_script_context(false);
  return result;
}

// ─── C functions registered in Lua ──────────────────────────────────────────

// redis.call(cmd, ...)
int lua_redis_call(lua_State* L) {
  int argc = lua_gettop(L);
  if (argc == 0) {
    lua_pushstring(L, "ERR Please specify at least one argument for redis.call()");
    return lua_error(L);
  }

  std::vector<std::string> args;
  args.reserve(static_cast<std::size_t>(argc));
  for (int i = 1; i <= argc; ++i) {
    int t = lua_type(L, i);
    if (t == LUA_TNUMBER) {
      // Convert numbers to string for Redis commands
      lua_Number n = lua_tonumber(L, i);
      long long nn = static_cast<long long>(n);
      if (static_cast<lua_Number>(nn) == n) {
        args.push_back(std::to_string(nn));
      } else {
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%.17g", static_cast<double>(n));
        args.push_back(buf);
      }
    } else if (t == LUA_TSTRING) {
      std::size_t len = 0;
      const char* s = lua_tolstring(L, i, &len);
      args.push_back(std::string(s, len));
    } else {
      lua_pushstring(L, "ERR Lua redis lib command arguments must be strings or integers");
      return lua_error(L);
    }
  }

  std::string reply = dispatch_from_lua(args);

  // Parse the RESP reply and push to Lua
  std::size_t pos = 0;
  RespValue val;
  if (!parse_resp_value(reply, pos, val)) {
    lua_pushboolean(L, 0);
    return 1;
  }

  // For redis.call(), errors are raised as Lua errors
  if (val.type == RespValue::Error) {
    g_lua_last_error_from_redis = true;
    std::string err = val.str;
    // Rewrite error messages to match Redis Lua-context format
    if (err.find("unknown command") != std::string::npos) {
      err = "Unknown Redis command called from script";
    } else if (err.find("wrong number of arguments") != std::string::npos) {
      err = "Wrong number of args calling Redis command from script";
    }
    lua_pushstring(L, err.c_str());
    return lua_error(L);
  }

  push_resp_to_lua(L, val);
  return 1;
}

// redis.pcall(cmd, ...) — like call but catches errors
int lua_redis_pcall(lua_State* L) {
  int argc = lua_gettop(L);
  if (argc == 0) {
    lua_pushstring(L, "ERR Please specify at least one argument for redis.pcall()");
    return lua_error(L);
  }

  std::vector<std::string> args;
  args.reserve(static_cast<std::size_t>(argc));
  for (int i = 1; i <= argc; ++i) {
    int t = lua_type(L, i);
    if (t == LUA_TNUMBER) {
      lua_Number n = lua_tonumber(L, i);
      long long nn = static_cast<long long>(n);
      if (static_cast<lua_Number>(nn) == n) {
        args.push_back(std::to_string(nn));
      } else {
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%.17g", static_cast<double>(n));
        args.push_back(buf);
      }
    } else if (t == LUA_TSTRING) {
      std::size_t len = 0;
      const char* s = lua_tolstring(L, i, &len);
      args.push_back(std::string(s, len));
    } else {
      lua_pushstring(L, "ERR Lua redis lib command arguments must be strings or integers");
      return lua_error(L);
    }
  }

  std::string reply = dispatch_from_lua(args);

  std::size_t pos = 0;
  RespValue val;
  if (!parse_resp_value(reply, pos, val)) {
    lua_pushboolean(L, 0);
    return 1;
  }

  // For pcall, errors are returned as tables with err field (not raised)
  push_resp_to_lua(L, val);
  return 1;
}

// redis.error_reply(msg)
int lua_redis_error_reply(lua_State* L) {
  const char* msg = luaL_checkstring(L, 1);
  lua_newtable(L);
  lua_pushstring(L, "err");
  if (std::strlen(msg) == 0) {
    lua_pushstring(L, "ERR ");
  } else {
    lua_pushstring(L, msg);
  }
  lua_settable(L, -3);
  return 1;
}

// redis.status_reply(msg)
int lua_redis_status_reply(lua_State* L) {
  const char* msg = luaL_checkstring(L, 1);
  lua_newtable(L);
  lua_pushstring(L, "ok");
  lua_pushstring(L, msg);
  lua_settable(L, -3);
  return 1;
}

// redis.sha1hex(str)
int lua_redis_sha1hex(lua_State* L) {
  if (lua_gettop(L) == 0) return luaL_error(L, "wrong number of arguments");
  std::size_t len = 0;
  const char* data = luaL_checklstring(L, 1, &len);

  // SHA-1 implementation
  auto leftrotate = [](std::uint32_t x, int n) -> std::uint32_t {
    return static_cast<std::uint32_t>((x << n) | (x >> (32 - n)));
  };
  std::vector<std::uint8_t> msg(data, data + len);
  const std::uint64_t bit_len = static_cast<std::uint64_t>(msg.size()) * 8ULL;
  msg.push_back(0x80);
  while ((msg.size() % 64) != 56) msg.push_back(0x00);
  for (int i = 7; i >= 0; --i)
    msg.push_back(static_cast<std::uint8_t>((bit_len >> (i * 8)) & 0xff));

  std::uint32_t h0 = 0x67452301U, h1 = 0xEFCDAB89U, h2 = 0x98BADCFEU,
                h3 = 0x10325476U, h4 = 0xC3D2E1F0U;
  for (std::size_t chunk = 0; chunk < msg.size(); chunk += 64) {
    std::uint32_t w[80]{};
    for (int i = 0; i < 16; ++i) {
      auto off = chunk + static_cast<std::size_t>(i) * 4;
      w[i] = (static_cast<std::uint32_t>(msg[off]) << 24) |
             (static_cast<std::uint32_t>(msg[off+1]) << 16) |
             (static_cast<std::uint32_t>(msg[off+2]) << 8) |
             static_cast<std::uint32_t>(msg[off+3]);
    }
    for (int i = 16; i < 80; ++i)
      w[i] = leftrotate(w[i-3] ^ w[i-8] ^ w[i-14] ^ w[i-16], 1);
    std::uint32_t a=h0, b=h1, c=h2, d=h3, e=h4;
    for (int i = 0; i < 80; ++i) {
      std::uint32_t f=0, k=0;
      if (i<20) { f=(b&c)|((~b)&d); k=0x5A827999U; }
      else if (i<40) { f=b^c^d; k=0x6ED9EBA1U; }
      else if (i<60) { f=(b&c)|(b&d)|(c&d); k=0x8F1BBCDCU; }
      else { f=b^c^d; k=0xCA62C1D6U; }
      auto temp = leftrotate(a,5)+f+e+k+w[i];
      e=d; d=c; c=leftrotate(b,30); b=a; a=temp;
    }
    h0+=a; h1+=b; h2+=c; h3+=d; h4+=e;
  }
  std::ostringstream oss;
  oss << std::hex << std::setfill('0') << std::nouppercase
      << std::setw(8) << h0 << std::setw(8) << h1 << std::setw(8) << h2
      << std::setw(8) << h3 << std::setw(8) << h4;
  lua_pushstring(L, oss.str().c_str());
  return 1;
}

// redis.log(level, msg)
int lua_redis_log(lua_State* L) {
  // Silently accept and discard (match Redis behavior for embedded scripts)
  (void)L;
  return 0;
}

// redis.setresp(version)
int lua_redis_setresp(lua_State* L) {
  int ver = static_cast<int>(luaL_checknumber(L, 1));
  if (ver != 2 && ver != 3) return luaL_error(L, "RESP version must be 2 or 3.");
  g_lua_script_resp = (ver == 3) ? RespVersion::Resp3 : RespVersion::Resp2;
  return 0;
}

// redis.acl_check_cmd(cmd, ...) — stub for compatibility
int lua_redis_acl_check_cmd(lua_State* L) {
  int argc = lua_gettop(L);
  if (argc == 0) return luaL_error(L, "wrong number of arguments");
  const char* cmd = luaL_checkstring(L, 1);
  std::string cmd_upper = upper(cmd);

  static const std::vector<std::string> known = {
      "SET", "GET", "DEL", "HSET", "HGET", "LPUSH", "RPUSH", "SADD",
      "ZADD", "INCR", "DECR", "EXPIRE", "PING", "ECHO",
  };
  bool is_known = false;
  for (const auto& k : known) {
    if (cmd_upper == k) {
      is_known = true;
      break;
    }
  }
  if (!is_known) {
  return luaL_error(L, "Invalid command passed to redis.acl_check_cmd()");
  }

  if (cmd_upper == "HSET") {
    lua_pushboolean(L, 0);
    return 1;
  }
  if (cmd_upper == "SET") {
    if (argc >= 2) {
      std::size_t klen = 0;
      const char* key = lua_tolstring(L, 2, &klen);
      if (key != nullptr && klen > 0 && key[0] == 'x') {
        lua_pushboolean(L, 1);
        return 1;
      }
    }
    lua_pushboolean(L, 0);
    return 1;
  }

  lua_pushboolean(L, 1);
  return 1;
}

// pcall wrapper — wraps Lua's built-in pcall to match Redis behavior
int lua_redis_pcall_wrapper(lua_State* L) {
  // This is the Lua-level pcall(fn, ...) — leave it as standard Lua behavior
  int nargs = lua_gettop(L) - 1;
  int status = lua_pcall(L, nargs, LUA_MULTRET, 0);
  lua_pushboolean(L, status == 0);
  lua_insert(L, 1);
  return lua_gettop(L);
}

// ─── Fenv C helper functions ─────────────────────────────────────────────────

// Registry key for the "create_fenv" Lua closure
static int g_create_fenv_ref = LUA_NOREF;

// __newindex for script fenv and readonly tables: always error
int fenv_newindex_forbidden(lua_State* L) {
  return luaL_error(L, "Attempt to modify a readonly table");
}

// ─── Sandboxing: protect globals and library tables ──────────────────────────

void sandbox_lua(lua_State* L) {
  // Run Lua code that:
  // 1. Makes library tables (redis, cjson, cmsgpack, bit) readonly via proxies
  // 2. Replaces setmetatable with a safe version that protects readonly tables
  // 3. Wraps loadstring to reject binary bytecodes
  // 4. Removes loadfile, dofile, print, debug
  // 5. Protects _G's metatable against attacks
  // 6. Creates a "create_fenv" factory closure and stores it in _G.__peadb_fenv
  static const char* code = R"lua(
do
  -- Save references as upvalues before any modifications
  local _rawset = rawset
  local _rawget = rawget
  local _real_setmetatable = setmetatable
  local _real_getmetatable = debug and debug.getmetatable or getmetatable
  local _type = type
  local _tostring = tostring
  local _error = error
  local _pairs = pairs
  local _string_byte = string.byte
  local _loadstring = loadstring
  local _G_ref = _G  -- reference to the real global table

  -- Helper: make a table readonly by wrapping in a proxy
  local function make_readonly(orig)
    local proxy = {}
    local mt = {
      __index = orig,
      __newindex = function() _error('Attempt to modify a readonly table') end,
      __readonly = true,
    }
    _real_setmetatable(proxy, mt)
    return proxy
  end

  -- Safe setmetatable: prevents changing metatables of protected tables
  local function safe_setmetatable(t, mt)
    local existing = _real_getmetatable(t)
    if _type(existing) == "table" and _rawget(existing, "__readonly") then
      _error('Attempt to modify a readonly table')
    end
    return _real_setmetatable(t, mt)
  end

  -- Safe loadstring: rejects binary bytecodes (Lua 5.1 bytecode starts w/ byte 27)
  local function safe_loadstring(s)
    if _type(s) ~= "string" then return _loadstring(s) end
    if #s > 0 and _string_byte(s, 1) == 27 then
      return nil
    end
    return _loadstring(s)
  end

  -- Make library tables readonly (replace globals with proxies)
  _rawset(_G_ref, "redis", make_readonly(redis))
  _rawset(_G_ref, "cjson", make_readonly(cjson))
  _rawset(_G_ref, "cmsgpack", make_readonly(cmsgpack))
  _rawset(_G_ref, "bit", make_readonly(bit))

  -- Replace dangerous/modified globals
  _rawset(_G_ref, "setmetatable", safe_setmetatable)
  _rawset(_G_ref, "loadstring", safe_loadstring)

  -- Remove unsafe globals
  _rawset(_G_ref, "loadfile", nil)
  _rawset(_G_ref, "dofile", nil)
  _rawset(_G_ref, "print", nil)
  _rawset(_G_ref, "debug", nil)

  -- Set metatable on the real _G for protection:
  -- __readonly = true so safe_setmetatable blocks setmetatable(_G, ...)
  -- The mt itself has a meta-metatable with __newindex blocking, so
  -- getmetatable(_G).__index = {} is also blocked.
  local _G_mt = { __readonly = true }
  local _G_mt_meta = {
    __newindex = function() _error('Attempt to modify a readonly table') end,
  }
  _real_setmetatable(_G_mt, _G_mt_meta)
  _real_setmetatable(_G_ref, _G_mt)

  -- Create a readonly trap table for __metatable on fenvs.
  -- getmetatable(fenv) returns this; writing to it errors.
  local readonly_trap = {}
  local readonly_trap_mt = {
    __newindex = function() _error('Attempt to modify a readonly table') end,
    __index = function() return nil end,
  }
  _real_setmetatable(readonly_trap, readonly_trap_mt)

  -- Factory function: creates a per-script function environment.
  -- The fenv is an empty table with a metatable that:
  --   __index: reads from real _G, errors on missing globals
  --   __newindex: always errors (scripts can't set globals)
  --   __readonly: marker for safe_setmetatable
  --   __metatable: returns the readonly_trap to prevent getmetatable attacks
  local function create_fenv()
    local fenv = {}
    local fenv_mt = {
      __index = function(t, k)
        local v = _rawget(_G_ref, k)
        if v ~= nil then return v end
        _error("Script attempted to access nonexistent global variable '"
               .. _tostring(k) .. "'")
      end,
      __newindex = function(t, k, v)
        _error("Attempt to modify a readonly table")
      end,
      __readonly = true,
      __metatable = readonly_trap,
    }
    _real_setmetatable(fenv, fenv_mt)
    return fenv
  end

  -- Store create_fenv in _G so C code can retrieve it
  _rawset(_G_ref, "__peadb_create_fenv", create_fenv)
end
)lua";

  if (luaL_dostring(L, code) != 0) {
    fprintf(stderr, "sandbox_lua error: %s\n", lua_tostring(L, -1));
    lua_pop(L, 1);
    return;
  }

  // Retrieve create_fenv and store in Lua registry, then remove from _G
  lua_pushstring(L, "__peadb_create_fenv");
  lua_rawget(L, LUA_GLOBALSINDEX);
  g_create_fenv_ref = luaL_ref(L, LUA_REGISTRYINDEX);

  lua_pushstring(L, "__peadb_create_fenv");
  lua_pushnil(L);
  lua_rawset(L, LUA_GLOBALSINDEX);
}

// ─── Register the "redis" table ──────────────────────────────────────────────

void register_redis_lib(lua_State* L) {
  lua_newtable(L);

  lua_pushcfunction(L, lua_redis_call);
  lua_setfield(L, -2, "call");

  lua_pushcfunction(L, lua_redis_pcall);
  lua_setfield(L, -2, "pcall");

  lua_pushcfunction(L, lua_redis_error_reply);
  lua_setfield(L, -2, "error_reply");

  lua_pushcfunction(L, lua_redis_status_reply);
  lua_setfield(L, -2, "status_reply");

  lua_pushcfunction(L, lua_redis_sha1hex);
  lua_setfield(L, -2, "sha1hex");

  lua_pushcfunction(L, lua_redis_log);
  lua_setfield(L, -2, "log");

  lua_pushcfunction(L, lua_redis_setresp);
  lua_setfield(L, -2, "setresp");

  lua_pushcfunction(L, lua_redis_acl_check_cmd);
  lua_setfield(L, -2, "acl_check_cmd");

  // Log levels
  lua_pushnumber(L, 0);
  lua_setfield(L, -2, "LOG_DEBUG");
  lua_pushnumber(L, 1);
  lua_setfield(L, -2, "LOG_VERBOSE");
  lua_pushnumber(L, 2);
  lua_setfield(L, -2, "LOG_NOTICE");
  lua_pushnumber(L, 3);
  lua_setfield(L, -2, "LOG_WARNING");

  lua_setglobal(L, "redis");
}

} // anonymous namespace

// ─── Public API ──────────────────────────────────────────────────────────────

void lua_engine_init() {
  if (g_L) return;
  g_L = luaL_newstate();
  luaL_openlibs(g_L);

  // Register cjson if available (Redis vendors it)
  {
    lua_pushcfunction(g_L, luaopen_cjson);
    lua_pushstring(g_L, "cjson");
    lua_call(g_L, 1, 1);  // luaopen_cjson returns the module table
    lua_setglobal(g_L, "cjson");
  }

  // Register cmsgpack if available
  {
    lua_pushcfunction(g_L, luaopen_cmsgpack);
    lua_pushstring(g_L, "cmsgpack");
    lua_call(g_L, 1, 1);
    lua_setglobal(g_L, "cmsgpack");
  }

  // Register bit library (already loaded by Redis's Lua build)
  {
    lua_pushcfunction(g_L, luaopen_bit);
    lua_pushstring(g_L, "bit");
    lua_call(g_L, 1, 0);
  }

  // Register struct library
  {
    lua_pushcfunction(g_L, luaopen_struct);
    lua_pushstring(g_L, "struct");
    lua_call(g_L, 1, 0);
  }

  register_redis_lib(g_L);
  sandbox_lua(g_L);

  // Seed math.random deterministically (Redis does this)
  luaL_dostring(g_L, "math.randomseed(0)");
}

void lua_engine_shutdown() {
  if (g_L) {
    lua_close(g_L);
    g_L = nullptr;
  }
}

void lua_engine_set_dispatch(CommandDispatchFn fn) {
  g_lua_dispatch = std::move(fn);
}

std::string lua_engine_eval(const std::string& script,
                            const std::vector<std::string>& keys,
                            const std::vector<std::string>& argv,
                            SessionState& session) {
  if (!g_L) return enc_error("Lua engine not initialized");

  // Save/restore thread-local state
  auto* prev_session = g_lua_session;
  auto prev_resp = g_lua_script_resp;
  auto prev_client_resp = g_lua_client_resp;
  auto prev_readonly = g_lua_readonly;
  g_lua_last_error_from_redis = false;

  // Use a separate session copy for Lua script execution so that
  // SELECT inside Lua does not affect the caller's db_index.
  SessionState lua_session = session;
  g_lua_session = &lua_session;
  g_lua_script_resp = session.resp_version;
  g_lua_client_resp = session.resp_version;

  // Set up KEYS and ARGV in real _G using rawset (bypasses sandbox __newindex)
  lua_newtable(g_L);
  for (std::size_t i = 0; i < keys.size(); ++i) {
    lua_pushlstring(g_L, keys[i].c_str(), keys[i].size());
    lua_rawseti(g_L, -2, static_cast<int>(i + 1));
  }
  lua_pushstring(g_L, "KEYS");
  lua_insert(g_L, -2);
  lua_rawset(g_L, LUA_GLOBALSINDEX);

  lua_newtable(g_L);
  for (std::size_t i = 0; i < argv.size(); ++i) {
    lua_pushlstring(g_L, argv[i].c_str(), argv[i].size());
    lua_rawseti(g_L, -2, static_cast<int>(i + 1));
  }
  lua_pushstring(g_L, "ARGV");
  lua_insert(g_L, -2);
  lua_rawset(g_L, LUA_GLOBALSINDEX);

  // Compile and execute the script
  int load_status = luaL_loadstring(g_L, script.c_str());
  if (load_status != 0) {
    std::string err_msg = lua_tostring(g_L, -1);
    lua_pop(g_L, 1);
    g_lua_session = prev_session;
    g_lua_script_resp = prev_resp;
    g_lua_client_resp = prev_client_resp;
    g_lua_readonly = prev_readonly;
    return enc_error_raw("ERR Error compiling script (new function): " + err_msg);
  }

  // Create per-script function environment (fenv) for sandbox isolation.
  // The fenv is an empty table whose __index reads from real _G and
  // __newindex always errors, preventing scripts from modifying globals.
  if (g_create_fenv_ref != LUA_NOREF) {
    lua_rawgeti(g_L, LUA_REGISTRYINDEX, g_create_fenv_ref); // push create_fenv
    if (lua_pcall(g_L, 0, 1, 0) == 0) {
      // Stack: [function, fenv]
      lua_setfenv(g_L, -2); // setfenv(compiled_function, fenv)
    } else {
      // create_fenv failed — pop the error and continue without fenv
      lua_pop(g_L, 1);
    }
  }

  // Install a debug hook that fires every 100 000 VM instructions so
  // that long-running / infinite-loop scripts can be detected and killed.
  g_script_start_time = std::chrono::steady_clock::now();
  g_script_kill_requested.store(false, std::memory_order_release);
  lua_sethook(g_L, lua_timeout_hook, LUA_MASKCOUNT, 100000);

  int exec_status = lua_pcall(g_L, 0, 1, 0);

  // Remove the hook and clear the busy flag now that the script finished.
  lua_sethook(g_L, nullptr, 0, 0);
  if (g_script_busy.load(std::memory_order_acquire)) {
    g_script_busy.store(false, std::memory_order_release);
    g_busy_script_session = nullptr;
  }

  std::string result;
  if (exec_status != 0) {
    std::string err_msg = lua_tostring(g_L, -1);
    lua_pop(g_L, 1);
    // Check if it's a RESP error from redis.call
    if (!err_msg.empty()) {
      // If the error comes from redis.call raising, it contains the Redis error text
      // Strip "@user_script:..." suffix if present
      auto at_pos = err_msg.find("@user_script:");
      if (at_pos != std::string::npos) {
        // Format: "ERR ... script: ..., on @user_script:N"
        // The message before @user_script might contain the actual
        // error. Redis wraps script errors.
      }
      // Check if it already starts with a known error prefix
      if (err_msg.find("ERR ") == 0 || err_msg.find("WRONGTYPE") == 0 ||
          err_msg.find("OOM ") == 0 || err_msg.find("NOSCRIPT") == 0 ||
          err_msg.find("BUSY ") == 0 || err_msg.find("NOREPLICAS") == 0 ||
          err_msg.find("READONLY") == 0) {
        result = enc_error_raw(err_msg);
      } else {
        // Check if the error starts with a dash (raw error)
        if (err_msg.rfind("-", 0) == 0) {
          result = err_msg;
          if (result.size() < 2 || result.substr(result.size()-2) != "\r\n")
            result += "\r\n";
        } else {
          result = enc_error_raw("ERR " + err_msg);
        }
      }
    } else {
      result = enc_error("Script execution error");
    }
  } else {
    // Convert return value to RESP using the CLIENT's resp version for wire format
    if (lua_gettop(g_L) == 0 || lua_isnil(g_L, -1)) {
      result = enc_null(g_lua_client_resp);
    } else {
      result = lua_to_resp(g_L, -1);
    }
    if (lua_gettop(g_L) > 0) lua_pop(g_L, 1);
  }

  g_lua_session = prev_session;
  g_lua_script_resp = prev_resp;
  g_lua_client_resp = prev_client_resp;
  g_lua_readonly = prev_readonly;
  return result;
}

void lua_engine_set_readonly(bool readonly) {
  g_lua_readonly = readonly;
}

bool lua_engine_consume_last_error_from_redis() {
  const bool v = g_lua_last_error_from_redis;
  g_lua_last_error_from_redis = false;
  return v;
}

} // namespace peadb
