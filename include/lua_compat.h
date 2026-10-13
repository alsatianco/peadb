/*
 * lua_compat.h — Compatibility shims for Lua 5.1 through 5.5.
 *
 * PeaDB's Lua scripting was written against the Lua 5.1 API (matching Redis).
 * This header provides forward-compatibility so the code compiles with newer
 * Lua versions installed by system package managers (e.g. Homebrew Lua 5.4/5.5).
 */

#ifndef PEADB_LUA_COMPAT_H
#define PEADB_LUA_COMPAT_H

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

/* ── lua_objlen → lua_rawlen (renamed in Lua 5.2) ────────────────────────── */
#if LUA_VERSION_NUM >= 502
#ifndef lua_objlen
#define lua_objlen(L, i) lua_rawlen(L, i)
#endif
#endif

/* ── luaL_register (removed in Lua 5.2) ──────────────────────────────────── */
#if LUA_VERSION_NUM >= 502
#ifndef PEADB_COMPAT_LUAL_REGISTER
#define PEADB_COMPAT_LUAL_REGISTER
static inline void luaL_register_compat(lua_State* L, const char* name, const luaL_Reg* funcs) {
  /* In Lua 5.2+, create a new table, populate it, and set it as a global.
   * Like Lua 5.1's luaL_register, the table is left on the stack. */
  lua_newtable(L);
  luaL_setfuncs(L, funcs, 0);
  if (name) {
    lua_pushvalue(L, -1);   /* duplicate table on stack */
    lua_setglobal(L, name); /* pops one copy, sets as global */
  }
  /* one copy of the table remains on the stack */
}
#define luaL_register(L, name, funcs) luaL_register_compat(L, name, funcs)
#endif
#endif

/* ── LUA_GLOBALSINDEX (removed in Lua 5.2) ───────────────────────────────── */
#if LUA_VERSION_NUM >= 502
/*
 * In Lua 5.2+, the globals table moved into the registry.  The macros below
 * provide helpers that approximate the old behaviour.
 */
#ifndef PEADB_COMPAT_GLOBALSINDEX
#define PEADB_COMPAT_GLOBALSINDEX

/* Push the global table onto the stack.  Works on 5.2+ (5.1 has lua_pushvalue). */
static inline void peadb_pushglobaltable(lua_State* L) {
  (void)lua_rawgeti(L, LUA_REGISTRYINDEX, LUA_RIDX_GLOBALS);
}

/* rawget from globals:  push name, then call this. */
static inline void peadb_rawget_global(lua_State* L) {
  peadb_pushglobaltable(L);
  lua_pushvalue(L, -2); /* copy the key */
  lua_rawget(L, -2);    /* globals[key] */
  lua_remove(L, -2);    /* remove globals table */
  lua_remove(L, -2);    /* remove original key */
}

/* rawset into globals:  stack should have key, value at top. */
static inline void peadb_rawset_global(lua_State* L) {
  peadb_pushglobaltable(L);
  lua_pushvalue(L, -3); /* copy key */
  lua_pushvalue(L, -3); /* copy value */
  lua_rawset(L, -3);    /* globals[key] = value */
  lua_pop(L, 1);        /* pop globals table */
  lua_pop(L, 2);        /* pop original key + value */
}

#endif /* PEADB_COMPAT_GLOBALSINDEX */
#endif

/* ── lua_setfenv (removed in Lua 5.2) ────────────────────────────────────── */
#if LUA_VERSION_NUM >= 502
#ifndef lua_setfenv
/*
 * In Lua 5.2+, function environments were replaced by upvalue-based _ENV.
 * Setting the first upvalue to the table approximates the old behaviour
 * for compiled chunks.  Returns 0 on failure.
 */
static inline int lua_setfenv_compat(lua_State* L, int idx) {
  /* The table to use as the environment should be on top of the stack. */
  const char* name = lua_setupvalue(L, idx, 1);
  if (name == NULL) {
    lua_pop(L, 1); /* pop the table that wasn't consumed */
    return 0;
  }
  return 1;
}
#define lua_setfenv(L, idx) lua_setfenv_compat(L, idx)
#endif
#endif

#endif /* PEADB_LUA_COMPAT_H */
