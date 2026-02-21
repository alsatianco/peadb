/*
 * lua_struct_stub.c — Minimal struct library for PeaDB Lua scripting.
 *
 * Provides struct.pack, struct.unpack, struct.size compatible with the
 * Roberto Ierusalimschy "struct" library used by Redis.
 *
 * Supports format characters: b B h H i I l f d n > < = ! (space ignored).
 */

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <stdint.h>
#include <string.h>
#include <stdlib.h>

/* Default is native endian */
static int is_little_endian(void) {
    union { uint16_t u; uint8_t b[2]; } t;
    t.u = 1;
    return t.b[0] == 1;
}

typedef struct {
    int little; /* current endianness: 1 = little, 0 = big */
} FmtState;

static void fmt_init(FmtState *st) {
    st->little = is_little_endian();
}

static void write_bytes(char *dst, const void *src, size_t n, int little) {
    int native_le = is_little_endian();
    if (little == native_le) {
        memcpy(dst, src, n);
    } else {
        const uint8_t *s = (const uint8_t *)src;
        for (size_t i = 0; i < n; i++)
            dst[i] = (char)s[n - 1 - i];
    }
}

static void read_bytes(void *dst, const char *src, size_t n, int little) {
    int native_le = is_little_endian();
    if (little == native_le) {
        memcpy(dst, src, n);
    } else {
        uint8_t *d = (uint8_t *)dst;
        for (size_t i = 0; i < n; i++)
            d[i] = (uint8_t)src[n - 1 - i];
    }
}

static int struct_pack(lua_State *L) {
    size_t fmtlen;
    const char *fmt = luaL_checklstring(L, 1, &fmtlen);
    FmtState st;
    fmt_init(&st);

    luaL_Buffer b;
    luaL_buffinit(L, &b);

    int arg = 2;
    for (size_t fi = 0; fi < fmtlen; fi++) {
        char c = fmt[fi];
        switch (c) {
            case ' ': break;
            case '>': st.little = 0; break;
            case '<': st.little = 1; break;
            case '=': st.little = is_little_endian(); break;
            case '!': st.little = is_little_endian(); break;
            case 'b': case 'B': {
                uint8_t v = (uint8_t)(int)luaL_checknumber(L, arg++);
                luaL_addchar(&b, (char)v);
                break;
            }
            case 'h': case 'H': {
                uint16_t v = (uint16_t)(int)luaL_checknumber(L, arg++);
                char tmp[2];
                write_bytes(tmp, &v, 2, st.little);
                luaL_addlstring(&b, tmp, 2);
                break;
            }
            case 'i': case 'I': {
                uint32_t v = (uint32_t)luaL_checknumber(L, arg++);
                char tmp[4];
                write_bytes(tmp, &v, 4, st.little);
                luaL_addlstring(&b, tmp, 4);
                break;
            }
            case 'l': {
                int64_t v = (int64_t)luaL_checknumber(L, arg++);
                char tmp[8];
                write_bytes(tmp, &v, 8, st.little);
                luaL_addlstring(&b, tmp, 8);
                break;
            }
            case 'f': {
                float v = (float)luaL_checknumber(L, arg++);
                char tmp[4];
                write_bytes(tmp, &v, 4, st.little);
                luaL_addlstring(&b, tmp, 4);
                break;
            }
            case 'd': {
                double v = luaL_checknumber(L, arg++);
                char tmp[8];
                write_bytes(tmp, &v, 8, st.little);
                luaL_addlstring(&b, tmp, 8);
                break;
            }
            case 'n': {
                lua_Number v = luaL_checknumber(L, arg++);
                char tmp[sizeof(lua_Number)];
                write_bytes(tmp, &v, sizeof(lua_Number), st.little);
                luaL_addlstring(&b, tmp, sizeof(lua_Number));
                break;
            }
            case 's': {
                size_t slen;
                const char *s = luaL_checklstring(L, arg++, &slen);
                /* Prefix with uint32 length */
                uint32_t l = (uint32_t)slen;
                char tmp[4];
                write_bytes(tmp, &l, 4, st.little);
                luaL_addlstring(&b, tmp, 4);
                luaL_addlstring(&b, s, slen);
                break;
            }
            default:
                return luaL_error(L, "struct.pack: unsupported format character '%c'", c);
        }
    }
    luaL_pushresult(&b);
    return 1;
}

static int struct_unpack(lua_State *L) {
    size_t fmtlen, datalen;
    const char *fmt = luaL_checklstring(L, 1, &fmtlen);
    const char *data = luaL_checklstring(L, 2, &datalen);
    int pos = luaL_optinteger(L, 3, 1) - 1; /* Lua 1-based to 0-based */
    FmtState st;
    fmt_init(&st);

    int nret = 0;
    for (size_t fi = 0; fi < fmtlen; fi++) {
        char c = fmt[fi];
        switch (c) {
            case ' ': break;
            case '>': st.little = 0; break;
            case '<': st.little = 1; break;
            case '=': st.little = is_little_endian(); break;
            case '!': st.little = is_little_endian(); break;
            case 'b': {
                if (pos + 1 > (int)datalen) return luaL_error(L, "struct.unpack: data too short");
                int8_t v; read_bytes(&v, data + pos, 1, st.little);
                lua_pushnumber(L, v); pos += 1; nret++;
                break;
            }
            case 'B': {
                if (pos + 1 > (int)datalen) return luaL_error(L, "struct.unpack: data too short");
                uint8_t v; read_bytes(&v, data + pos, 1, st.little);
                lua_pushnumber(L, v); pos += 1; nret++;
                break;
            }
            case 'h': {
                if (pos + 2 > (int)datalen) return luaL_error(L, "struct.unpack: data too short");
                int16_t v; read_bytes(&v, data + pos, 2, st.little);
                lua_pushnumber(L, v); pos += 2; nret++;
                break;
            }
            case 'H': {
                if (pos + 2 > (int)datalen) return luaL_error(L, "struct.unpack: data too short");
                uint16_t v; read_bytes(&v, data + pos, 2, st.little);
                lua_pushnumber(L, v); pos += 2; nret++;
                break;
            }
            case 'i': {
                if (pos + 4 > (int)datalen) return luaL_error(L, "struct.unpack: data too short");
                int32_t v; read_bytes(&v, data + pos, 4, st.little);
                lua_pushnumber(L, v); pos += 4; nret++;
                break;
            }
            case 'I': {
                if (pos + 4 > (int)datalen) return luaL_error(L, "struct.unpack: data too short");
                uint32_t v; read_bytes(&v, data + pos, 4, st.little);
                lua_pushnumber(L, v); pos += 4; nret++;
                break;
            }
            case 'l': {
                if (pos + 8 > (int)datalen) return luaL_error(L, "struct.unpack: data too short");
                int64_t v; read_bytes(&v, data + pos, 8, st.little);
                lua_pushnumber(L, (lua_Number)v); pos += 8; nret++;
                break;
            }
            case 'f': {
                if (pos + 4 > (int)datalen) return luaL_error(L, "struct.unpack: data too short");
                float v; read_bytes(&v, data + pos, 4, st.little);
                lua_pushnumber(L, v); pos += 4; nret++;
                break;
            }
            case 'd': {
                if (pos + 8 > (int)datalen) return luaL_error(L, "struct.unpack: data too short");
                double v; read_bytes(&v, data + pos, 8, st.little);
                lua_pushnumber(L, v); pos += 8; nret++;
                break;
            }
            case 'n': {
                int sz = (int)sizeof(lua_Number);
                if (pos + sz > (int)datalen) return luaL_error(L, "struct.unpack: data too short");
                lua_Number v; read_bytes(&v, data + pos, sz, st.little);
                lua_pushnumber(L, v); pos += sz; nret++;
                break;
            }
            case 's': {
                if (pos + 4 > (int)datalen) return luaL_error(L, "struct.unpack: data too short");
                uint32_t slen; read_bytes(&slen, data + pos, 4, st.little);
                pos += 4;
                if (pos + (int)slen > (int)datalen) return luaL_error(L, "struct.unpack: data too short");
                lua_pushlstring(L, data + pos, slen);
                pos += (int)slen; nret++;
                break;
            }
            default:
                return luaL_error(L, "struct.unpack: unsupported format character '%c'", c);
        }
    }
    lua_pushnumber(L, pos + 1); /* next position (1-based) */
    return nret + 1;
}

static int struct_size(lua_State *L) {
    size_t fmtlen;
    const char *fmt = luaL_checklstring(L, 1, &fmtlen);
    int total = 0;
    for (size_t fi = 0; fi < fmtlen; fi++) {
        switch (fmt[fi]) {
            case ' ': case '>': case '<': case '=': case '!': break;
            case 'b': case 'B': total += 1; break;
            case 'h': case 'H': total += 2; break;
            case 'i': case 'I': total += 4; break;
            case 'l': total += 8; break;
            case 'f': total += 4; break;
            case 'd': total += 8; break;
            case 'n': total += (int)sizeof(lua_Number); break;
            case 's': return luaL_error(L, "struct.size: variable-size format 's'");
            default:
                return luaL_error(L, "struct.size: unsupported format character '%c'", fmt[fi]);
        }
    }
    lua_pushnumber(L, total);
    return 1;
}

static const luaL_Reg struct_funcs[] = {
    {"pack",   struct_pack},
    {"unpack", struct_unpack},
    {"size",   struct_size},
    {NULL, NULL}
};

int luaopen_struct(lua_State *L) {
    luaL_register(L, "struct", struct_funcs);
    return 1;
}
