/*
 * lua_cmsgpack_stub.c — Minimal cmsgpack module for PeaDB Lua scripting.
 *
 * Provides cmsgpack.pack() and cmsgpack.unpack() with basic MessagePack
 * support sufficient for Redis-compatible Lua scripting.
 */

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ── Encode helpers ────────────────────────────────────────────────────────── */

typedef struct {
    char *buf;
    size_t len;
    size_t cap;
} mp_buf;

static void mp_init(mp_buf *b) {
    b->cap = 256;
    b->len = 0;
    b->buf = (char *)malloc(b->cap);
}

static void mp_ensure(mp_buf *b, size_t extra) {
    while (b->len + extra > b->cap) {
        b->cap *= 2;
        b->buf = (char *)realloc(b->buf, b->cap);
    }
}

static void mp_write(mp_buf *b, const void *data, size_t n) {
    mp_ensure(b, n);
    memcpy(b->buf + b->len, data, n);
    b->len += n;
}

static void mp_write_byte(mp_buf *b, uint8_t v) {
    mp_ensure(b, 1);
    b->buf[b->len++] = (char)v;
}

static void mp_write_u16be(mp_buf *b, uint16_t v) {
    uint8_t d[2] = {(uint8_t)(v >> 8), (uint8_t)v};
    mp_write(b, d, 2);
}

static void mp_write_u32be(mp_buf *b, uint32_t v) {
    uint8_t d[4] = {(uint8_t)(v >> 24), (uint8_t)(v >> 16),
                    (uint8_t)(v >> 8),   (uint8_t)v};
    mp_write(b, d, 4);
}

static void mp_write_u64be(mp_buf *b, uint64_t v) {
    uint8_t d[8];
    for (int i = 7; i >= 0; i--) { d[i] = (uint8_t)(v & 0xff); v >>= 8; }
    mp_write(b, d, 8);
}

static void mp_pack_nil(mp_buf *b) { mp_write_byte(b, 0xc0); }
static void mp_pack_bool(mp_buf *b, int v) { mp_write_byte(b, v ? 0xc3 : 0xc2); }

static void mp_pack_integer(mp_buf *b, int64_t v) {
    if (v >= 0) {
        if (v <= 127) {
            mp_write_byte(b, (uint8_t)v);
        } else if (v <= 0xff) {
            mp_write_byte(b, 0xcc);
            mp_write_byte(b, (uint8_t)v);
        } else if (v <= 0xffff) {
            mp_write_byte(b, 0xcd);
            mp_write_u16be(b, (uint16_t)v);
        } else if (v <= 0xffffffff) {
            mp_write_byte(b, 0xce);
            mp_write_u32be(b, (uint32_t)v);
        } else {
            mp_write_byte(b, 0xcf);
            mp_write_u64be(b, (uint64_t)v);
        }
    } else {
        if (v >= -32) {
            mp_write_byte(b, (uint8_t)(v & 0xff));
        } else if (v >= -128) {
            mp_write_byte(b, 0xd0);
            mp_write_byte(b, (uint8_t)(int8_t)v);
        } else if (v >= -32768) {
            mp_write_byte(b, 0xd1);
            mp_write_u16be(b, (uint16_t)(int16_t)v);
        } else if (v >= (int64_t)-2147483648LL) {
            mp_write_byte(b, 0xd2);
            mp_write_u32be(b, (uint32_t)(int32_t)v);
        } else {
            mp_write_byte(b, 0xd3);
            mp_write_u64be(b, (uint64_t)v);
        }
    }
}

static void mp_pack_double(mp_buf *b, double v) {
    mp_write_byte(b, 0xcb);
    uint64_t u;
    memcpy(&u, &v, 8);
    mp_write_u64be(b, u);
}

static void mp_pack_string(mp_buf *b, const char *s, size_t len) {
    if (len <= 31) {
        mp_write_byte(b, (uint8_t)(0xa0 | len));
    } else if (len <= 0xffff) {
        mp_write_byte(b, 0xda);
        mp_write_u16be(b, (uint16_t)len);
    } else {
        mp_write_byte(b, 0xdb);
        mp_write_u32be(b, (uint32_t)len);
    }
    mp_write(b, s, len);
}

static void mp_pack_value(lua_State *L, mp_buf *b, int idx, int depth);

static int mp_table_is_array(lua_State *L, int idx) {
    int count = 0;
    lua_pushnil(L);
    while (lua_next(L, idx) != 0) {
        if (lua_type(L, -2) != LUA_TNUMBER) { lua_pop(L, 2); return 0; }
        double k = lua_tonumber(L, -2);
        if (k != (int)k || k < 1) { lua_pop(L, 2); return 0; }
        count++;
        lua_pop(L, 1);
    }
    return count == (int)lua_objlen(L, idx);
}

static void mp_pack_value(lua_State *L, mp_buf *b, int idx, int depth) {
    if (depth > 128) luaL_error(L, "cmsgpack.pack: nesting too deep");
    if (idx < 0) idx = lua_gettop(L) + idx + 1;

    switch (lua_type(L, idx)) {
        case LUA_TNIL:
            mp_pack_nil(b);
            break;
        case LUA_TBOOLEAN:
            mp_pack_bool(b, lua_toboolean(L, idx));
            break;
        case LUA_TNUMBER: {
            double n = lua_tonumber(L, idx);
            if (floor(n) == n && fabs(n) < 9.2e18) {
                mp_pack_integer(b, (int64_t)n);
            } else {
                mp_pack_double(b, n);
            }
            break;
        }
        case LUA_TSTRING: {
            size_t len;
            const char *s = lua_tolstring(L, idx, &len);
            mp_pack_string(b, s, len);
            break;
        }
        case LUA_TTABLE: {
            if (mp_table_is_array(L, idx)) {
                int n = (int)lua_objlen(L, idx);
                if (n <= 15) {
                    mp_write_byte(b, (uint8_t)(0x90 | n));
                } else if (n <= 0xffff) {
                    mp_write_byte(b, 0xdc);
                    mp_write_u16be(b, (uint16_t)n);
                } else {
                    mp_write_byte(b, 0xdd);
                    mp_write_u32be(b, (uint32_t)n);
                }
                for (int i = 1; i <= n; i++) {
                    lua_rawgeti(L, idx, i);
                    mp_pack_value(L, b, lua_gettop(L), depth + 1);
                    lua_pop(L, 1);
                }
            } else {
                /* Count entries */
                int count = 0;
                lua_pushnil(L);
                while (lua_next(L, idx) != 0) { count++; lua_pop(L, 1); }
                if (count <= 15) {
                    mp_write_byte(b, (uint8_t)(0x80 | count));
                } else if (count <= 0xffff) {
                    mp_write_byte(b, 0xde);
                    mp_write_u16be(b, (uint16_t)count);
                } else {
                    mp_write_byte(b, 0xdf);
                    mp_write_u32be(b, (uint32_t)count);
                }
                lua_pushnil(L);
                while (lua_next(L, idx) != 0) {
                    mp_pack_value(L, b, lua_gettop(L) - 1, depth + 1);
                    mp_pack_value(L, b, lua_gettop(L), depth + 1);
                    lua_pop(L, 1);
                }
            }
            break;
        }
        default:
            luaL_error(L, "cmsgpack.pack: unsupported type: %s",
                        lua_typename(L, lua_type(L, idx)));
    }
}

static int cmsgpack_pack(lua_State *L) {
    int nargs = lua_gettop(L);
    if (nargs == 0) return luaL_error(L, "cmsgpack.pack requires at least one argument");
    mp_buf b;
    mp_init(&b);
    for (int i = 1; i <= nargs; i++) {
        mp_pack_value(L, &b, i, 0);
    }
    lua_pushlstring(L, b.buf, b.len);
    free(b.buf);
    return 1;
}

/* ── Decode helpers ────────────────────────────────────────────────────────── */

static uint8_t  rd_u8(const uint8_t *d, int *p)  { uint8_t  v = d[*p]; *p += 1; return v; }
static uint16_t rd_u16(const uint8_t *d, int *p)  { uint16_t v = ((uint16_t)d[*p] << 8) | d[*p+1]; *p += 2; return v; }
static uint32_t rd_u32(const uint8_t *d, int *p)  { uint32_t v = ((uint32_t)d[*p] << 24) | ((uint32_t)d[*p+1] << 16) |
                                                                  ((uint32_t)d[*p+2] << 8) | d[*p+3]; *p += 4; return v; }
static uint64_t rd_u64(const uint8_t *d, int *p)  { uint64_t v = 0; for (int i = 0; i < 8; i++) { v = (v << 8) | d[*p+i]; } *p += 8; return v; }

static int mp_unpack_value(lua_State *L, const uint8_t *d, int len, int *pos, int depth) {
    if (depth > 128 || *pos >= len) return luaL_error(L, "cmsgpack.unpack: invalid data");

    uint8_t tag = rd_u8(d, pos);

    /* Positive fixint 0x00-0x7f */
    if (tag <= 0x7f) { lua_pushnumber(L, tag); return 1; }
    /* Fixmap 0x80-0x8f */
    if ((tag & 0xf0) == 0x80) {
        int n = tag & 0x0f;
        lua_newtable(L);
        for (int i = 0; i < n; i++) {
            mp_unpack_value(L, d, len, pos, depth + 1);
            mp_unpack_value(L, d, len, pos, depth + 1);
            lua_settable(L, -3);
        }
        return 1;
    }
    /* Fixarray 0x90-0x9f */
    if ((tag & 0xf0) == 0x90) {
        int n = tag & 0x0f;
        lua_newtable(L);
        for (int i = 1; i <= n; i++) {
            mp_unpack_value(L, d, len, pos, depth + 1);
            lua_rawseti(L, -2, i);
        }
        return 1;
    }
    /* Fixstr 0xa0-0xbf */
    if ((tag & 0xe0) == 0xa0) {
        int n = tag & 0x1f;
        if (*pos + n > len) return luaL_error(L, "cmsgpack.unpack: truncated");
        lua_pushlstring(L, (const char *)(d + *pos), n);
        *pos += n;
        return 1;
    }
    /* Negative fixint 0xe0-0xff */
    if (tag >= 0xe0) { lua_pushnumber(L, (int8_t)tag); return 1; }

    switch (tag) {
        case 0xc0: lua_pushboolean(L, 0); return 1; /* nil → false (Redis compat) */
        case 0xc2: lua_pushboolean(L, 0); return 1;
        case 0xc3: lua_pushboolean(L, 1); return 1;
        case 0xcc: lua_pushnumber(L, rd_u8(d, pos)); return 1;
        case 0xcd: lua_pushnumber(L, rd_u16(d, pos)); return 1;
        case 0xce: lua_pushnumber(L, rd_u32(d, pos)); return 1;
        case 0xcf: lua_pushnumber(L, (double)rd_u64(d, pos)); return 1;
        case 0xd0: { int8_t  v = (int8_t)rd_u8(d, pos);   lua_pushnumber(L, v); return 1; }
        case 0xd1: { int16_t v = (int16_t)rd_u16(d, pos);  lua_pushnumber(L, v); return 1; }
        case 0xd2: { int32_t v = (int32_t)rd_u32(d, pos);  lua_pushnumber(L, v); return 1; }
        case 0xd3: { int64_t v = (int64_t)rd_u64(d, pos);  lua_pushnumber(L, (double)v); return 1; }
        case 0xcb: { /* float64 */
            uint64_t u = rd_u64(d, pos);
            double v; memcpy(&v, &u, 8);
            lua_pushnumber(L, v);
            return 1;
        }
        case 0xca: { /* float32 */
            uint32_t u = rd_u32(d, pos);
            float v; memcpy(&v, &u, 4);
            lua_pushnumber(L, v);
            return 1;
        }
        case 0xd9: case 0xda: case 0xdb: { /* str 8/16/32 */
            size_t n = 0;
            if (tag == 0xd9) n = rd_u8(d, pos);
            else if (tag == 0xda) n = rd_u16(d, pos);
            else n = rd_u32(d, pos);
            if (*pos + (int)n > len) return luaL_error(L, "cmsgpack.unpack: truncated");
            lua_pushlstring(L, (const char *)(d + *pos), n);
            *pos += (int)n;
            return 1;
        }
        case 0xc4: case 0xc5: case 0xc6: { /* bin 8/16/32 */
            size_t n = 0;
            if (tag == 0xc4) n = rd_u8(d, pos);
            else if (tag == 0xc5) n = rd_u16(d, pos);
            else n = rd_u32(d, pos);
            if (*pos + (int)n > len) return luaL_error(L, "cmsgpack.unpack: truncated");
            lua_pushlstring(L, (const char *)(d + *pos), n);
            *pos += (int)n;
            return 1;
        }
        case 0xdc: case 0xdd: { /* array 16/32 */
            uint32_t n = (tag == 0xdc) ? rd_u16(d, pos) : rd_u32(d, pos);
            lua_newtable(L);
            for (uint32_t i = 1; i <= n; i++) {
                mp_unpack_value(L, d, len, pos, depth + 1);
                lua_rawseti(L, -2, (int)i);
            }
            return 1;
        }
        case 0xde: case 0xdf: { /* map 16/32 */
            uint32_t n = (tag == 0xde) ? rd_u16(d, pos) : rd_u32(d, pos);
            lua_newtable(L);
            for (uint32_t i = 0; i < n; i++) {
                mp_unpack_value(L, d, len, pos, depth + 1);
                mp_unpack_value(L, d, len, pos, depth + 1);
                lua_settable(L, -3);
            }
            return 1;
        }
        default:
            return luaL_error(L, "cmsgpack.unpack: unsupported tag 0x%02x", tag);
    }
}

static int cmsgpack_unpack(lua_State *L) {
    size_t len;
    const char *s = luaL_checklstring(L, 1, &len);
    int pos = 0;
    mp_unpack_value(L, (const uint8_t *)s, (int)len, &pos, 0);
    return 1;
}

static const luaL_Reg cmsgpack_funcs[] = {
    {"pack",   cmsgpack_pack},
    {"unpack", cmsgpack_unpack},
    {NULL, NULL}
};

int luaopen_cmsgpack(lua_State *L) {
    luaL_register(L, "cmsgpack", cmsgpack_funcs);
    return 1;
}
