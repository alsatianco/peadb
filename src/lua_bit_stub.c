/*
 * lua_bit_stub.c — LuaBitOp-compatible bit library for PeaDB Lua scripting.
 *
 * Provides the "bit" module with all operations used by Redis Lua scripts:
 * tobit, tohex, bnot, band, bor, bxor, lshift, rshift, arshift, rol, ror, bswap.
 */

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <stdint.h>

typedef int32_t SBits;
typedef uint32_t UBits;

static UBits barg(lua_State *L, int idx) {
    double n = luaL_checknumber(L, idx);
    return (UBits)(int64_t)n;
}

static int bit_tobit(lua_State *L) {
    lua_pushnumber(L, (lua_Number)(SBits)barg(L, 1));
    return 1;
}

static int bit_tohex(lua_State *L) {
    UBits b = barg(L, 1);
    int n = lua_isnoneornil(L, 2) ? 8 : (int)luaL_checknumber(L, 2);
    const char *hexdigits = "0123456789abcdef";
    char buf[8];
    int i;
    if (n < 0) { n = -n; hexdigits = "0123456789ABCDEF"; }
    if (n > 8) n = 8;
    for (i = n; --i >= 0; ) { buf[i] = hexdigits[b & 15]; b >>= 4; }
    lua_pushlstring(L, buf, (size_t)n);
    return 1;
}

static int bit_bnot(lua_State *L) {
    lua_pushnumber(L, (lua_Number)(SBits)(~barg(L, 1)));
    return 1;
}

static int bit_band(lua_State *L) {
    int i;
    UBits b = barg(L, 1);
    for (i = lua_gettop(L); i >= 2; i--)
        b &= barg(L, i);
    lua_pushnumber(L, (lua_Number)(SBits)b);
    return 1;
}

static int bit_bor(lua_State *L) {
    int i;
    UBits b = barg(L, 1);
    for (i = lua_gettop(L); i >= 2; i--)
        b |= barg(L, i);
    lua_pushnumber(L, (lua_Number)(SBits)b);
    return 1;
}

static int bit_bxor(lua_State *L) {
    int i;
    UBits b = barg(L, 1);
    for (i = lua_gettop(L); i >= 2; i--)
        b ^= barg(L, i);
    lua_pushnumber(L, (lua_Number)(SBits)b);
    return 1;
}

static int bit_lshift(lua_State *L) {
    UBits b = barg(L, 1);
    UBits n = barg(L, 2) & 31;
    lua_pushnumber(L, (lua_Number)(SBits)(b << n));
    return 1;
}

static int bit_rshift(lua_State *L) {
    UBits b = barg(L, 1);
    UBits n = barg(L, 2) & 31;
    lua_pushnumber(L, (lua_Number)(SBits)(b >> n));
    return 1;
}

static int bit_arshift(lua_State *L) {
    SBits b = (SBits)barg(L, 1);
    UBits n = barg(L, 2) & 31;
    lua_pushnumber(L, (lua_Number)(b >> n));
    return 1;
}

static int bit_rol(lua_State *L) {
    UBits b = barg(L, 1);
    UBits n = barg(L, 2) & 31;
    lua_pushnumber(L, (lua_Number)(SBits)((b << n) | (b >> (32 - n))));
    return 1;
}

static int bit_ror(lua_State *L) {
    UBits b = barg(L, 1);
    UBits n = barg(L, 2) & 31;
    lua_pushnumber(L, (lua_Number)(SBits)((b >> n) | (b << (32 - n))));
    return 1;
}

static int bit_bswap(lua_State *L) {
    UBits b = barg(L, 1);
    b = ((b >> 24) & 0xff) | ((b >> 8) & 0xff00) |
        ((b & 0xff00) << 8) | ((b & 0xff) << 24);
    lua_pushnumber(L, (lua_Number)(SBits)b);
    return 1;
}

static const luaL_Reg bit_funcs[] = {
    {"tobit",   bit_tobit},
    {"tohex",   bit_tohex},
    {"bnot",    bit_bnot},
    {"band",    bit_band},
    {"bor",     bit_bor},
    {"bxor",    bit_bxor},
    {"lshift",  bit_lshift},
    {"rshift",  bit_rshift},
    {"arshift", bit_arshift},
    {"rol",     bit_rol},
    {"ror",     bit_ror},
    {"bswap",   bit_bswap},
    {NULL, NULL}
};

int luaopen_bit(lua_State *L) {
    luaL_register(L, "bit", bit_funcs);
    return 1;
}
