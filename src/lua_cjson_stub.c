/*
 * lua_cjson_stub.c — Minimal cjson module for PeaDB Lua scripting.
 *
 * Provides cjson.encode() and cjson.decode() with basic JSON support,
 * sufficient for Redis-compatible Lua scripting.
 */

#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>

#include <ctype.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Forward declarations ──────────────────────────────────────────────────── */

static void json_encode_value(lua_State *L, luaL_Buffer *b, int idx, int depth);
static int  json_decode_value(lua_State *L, const char *s, int len, int *pos);

/* ── Encode ────────────────────────────────────────────────────────────────── */

static void json_encode_string(luaL_Buffer *b, const char *s, size_t len) {
    luaL_addchar(b, '"');
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)s[i];
        switch (c) {
            case '"':  luaL_addstring(b, "\\\""); break;
            case '\\': luaL_addstring(b, "\\\\"); break;
            case '\b': luaL_addstring(b, "\\b");  break;
            case '\f': luaL_addstring(b, "\\f");  break;
            case '\n': luaL_addstring(b, "\\n");  break;
            case '\r': luaL_addstring(b, "\\r");  break;
            case '\t': luaL_addstring(b, "\\t");  break;
            default:
                if (c < 0x20) {
                    char esc[8];
                    snprintf(esc, sizeof(esc), "\\u%04x", c);
                    luaL_addstring(b, esc);
                } else {
                    luaL_addchar(b, (char)c);
                }
        }
    }
    luaL_addchar(b, '"');
}

/* Check if a table is an array (consecutive integer keys 1..n) */
static int table_is_array(lua_State *L, int idx) {
    int count = 0;
    lua_pushnil(L);
    while (lua_next(L, idx) != 0) {
        if (lua_type(L, -2) != LUA_TNUMBER) {
            lua_pop(L, 2);
            return 0;
        }
        double k = lua_tonumber(L, -2);
        if (k != (int)k || k < 1) {
            lua_pop(L, 2);
            return 0;
        }
        count++;
        lua_pop(L, 1);
    }
    /* Verify keys are 1..count */
    return count == (int)lua_objlen(L, idx);
}

static void json_encode_value(lua_State *L, luaL_Buffer *b, int idx, int depth) {
    if (depth > 128)
        luaL_error(L, "cjson.encode: nesting too deep");

    /* Normalize index to absolute */
    if (idx < 0) idx = lua_gettop(L) + idx + 1;

    switch (lua_type(L, idx)) {
        case LUA_TNIL:
            luaL_addstring(b, "null");
            break;
        case LUA_TBOOLEAN:
            luaL_addstring(b, lua_toboolean(L, idx) ? "true" : "false");
            break;
        case LUA_TNUMBER: {
            double n = lua_tonumber(L, idx);
            char buf[64];
            if (isinf(n) || isnan(n)) {
                luaL_error(L, "cjson.encode: cannot encode inf/nan");
            }
            if (n == (long long)n && fabs(n) < 1e15) {
                snprintf(buf, sizeof(buf), "%lld", (long long)n);
            } else {
                snprintf(buf, sizeof(buf), "%.14g", n);
            }
            luaL_addstring(b, buf);
            break;
        }
        case LUA_TSTRING: {
            size_t len;
            const char *s = lua_tolstring(L, idx, &len);
            json_encode_string(b, s, len);
            break;
        }
        case LUA_TTABLE: {
            if (table_is_array(L, idx)) {
                int n = (int)lua_objlen(L, idx);
                luaL_addchar(b, '[');
                for (int i = 1; i <= n; i++) {
                    if (i > 1) luaL_addchar(b, ',');
                    lua_rawgeti(L, idx, i);
                    json_encode_value(L, b, lua_gettop(L), depth + 1);
                    lua_pop(L, 1);
                }
                luaL_addchar(b, ']');
            } else {
                luaL_addchar(b, '{');
                int first = 1;
                lua_pushnil(L);
                while (lua_next(L, idx) != 0) {
                    if (!first) luaL_addchar(b, ',');
                    first = 0;
                    /* Key */
                    if (lua_type(L, -2) == LUA_TSTRING) {
                        size_t klen;
                        const char *k = lua_tolstring(L, -2, &klen);
                        json_encode_string(b, k, klen);
                    } else if (lua_type(L, -2) == LUA_TNUMBER) {
                        char kbuf[64];
                        snprintf(kbuf, sizeof(kbuf), "%.14g", lua_tonumber(L, -2));
                        luaL_addchar(b, '"');
                        luaL_addstring(b, kbuf);
                        luaL_addchar(b, '"');
                    } else {
                        luaL_error(L, "cjson.encode: unsupported key type");
                    }
                    luaL_addchar(b, ':');
                    json_encode_value(L, b, lua_gettop(L), depth + 1);
                    lua_pop(L, 1);
                }
                luaL_addchar(b, '}');
            }
            break;
        }
        default:
            luaL_error(L, "cjson.encode: unsupported type: %s",
                        lua_typename(L, lua_type(L, idx)));
    }
}

static int cjson_encode(lua_State *L) {
    luaL_checkany(L, 1);
    luaL_Buffer b;
    luaL_buffinit(L, &b);
    json_encode_value(L, &b, 1, 0);
    luaL_pushresult(&b);
    return 1;
}

/* ── Decode ────────────────────────────────────────────────────────────────── */

static void skip_ws(const char *s, int len, int *pos) {
    while (*pos < len && (s[*pos] == ' ' || s[*pos] == '\t' ||
                          s[*pos] == '\n' || s[*pos] == '\r'))
        (*pos)++;
}

static int decode_string(lua_State *L, const char *s, int len, int *pos) {
    if (*pos >= len || s[*pos] != '"') return 0;
    (*pos)++; /* skip opening " */
    luaL_Buffer b;
    luaL_buffinit(L, &b);
    while (*pos < len && s[*pos] != '"') {
        if (s[*pos] == '\\') {
            (*pos)++;
            if (*pos >= len) return 0;
            switch (s[*pos]) {
                case '"':  luaL_addchar(&b, '"');  break;
                case '\\': luaL_addchar(&b, '\\'); break;
                case '/':  luaL_addchar(&b, '/');  break;
                case 'b':  luaL_addchar(&b, '\b'); break;
                case 'f':  luaL_addchar(&b, '\f'); break;
                case 'n':  luaL_addchar(&b, '\n'); break;
                case 'r':  luaL_addchar(&b, '\r'); break;
                case 't':  luaL_addchar(&b, '\t'); break;
                case 'u': {
                    /* Minimal \uXXXX handling — ASCII range only */
                    if (*pos + 4 >= len) return 0;
                    char hex[5] = {s[*pos+1], s[*pos+2], s[*pos+3], s[*pos+4], 0};
                    unsigned int cp = (unsigned int)strtoul(hex, NULL, 16);
                    if (cp < 0x80) {
                        luaL_addchar(&b, (char)cp);
                    } else if (cp < 0x800) {
                        luaL_addchar(&b, (char)(0xC0 | (cp >> 6)));
                        luaL_addchar(&b, (char)(0x80 | (cp & 0x3F)));
                    } else {
                        luaL_addchar(&b, (char)(0xE0 | (cp >> 12)));
                        luaL_addchar(&b, (char)(0x80 | ((cp >> 6) & 0x3F)));
                        luaL_addchar(&b, (char)(0x80 | (cp & 0x3F)));
                    }
                    *pos += 4;
                    break;
                }
                default: return 0;
            }
        } else {
            luaL_addchar(&b, s[*pos]);
        }
        (*pos)++;
    }
    if (*pos >= len) return 0;
    (*pos)++; /* skip closing " */
    luaL_pushresult(&b);
    return 1;
}

static int json_decode_value(lua_State *L, const char *s, int len, int *pos) {
    skip_ws(s, len, pos);
    if (*pos >= len) return 0;

    switch (s[*pos]) {
        case '"':
            return decode_string(L, s, len, pos);

        case '{': {
            (*pos)++; /* skip { */
            lua_newtable(L);
            skip_ws(s, len, pos);
            if (*pos < len && s[*pos] == '}') { (*pos)++; return 1; }
            while (1) {
                skip_ws(s, len, pos);
                /* key must be string */
                if (!decode_string(L, s, len, pos)) return 0;
                skip_ws(s, len, pos);
                if (*pos >= len || s[*pos] != ':') return 0;
                (*pos)++;
                if (!json_decode_value(L, s, len, pos)) { lua_pop(L, 1); return 0; }
                lua_settable(L, -3);
                skip_ws(s, len, pos);
                if (*pos >= len) return 0;
                if (s[*pos] == '}') { (*pos)++; return 1; }
                if (s[*pos] != ',') return 0;
                (*pos)++;
            }
        }

        case '[': {
            (*pos)++; /* skip [ */
            lua_newtable(L);
            skip_ws(s, len, pos);
            if (*pos < len && s[*pos] == ']') { (*pos)++; return 1; }
            int i = 1;
            while (1) {
                if (!json_decode_value(L, s, len, pos)) return 0;
                lua_rawseti(L, -2, i++);
                skip_ws(s, len, pos);
                if (*pos >= len) return 0;
                if (s[*pos] == ']') { (*pos)++; return 1; }
                if (s[*pos] != ',') return 0;
                (*pos)++;
            }
        }

        case 't': /* true */
            if (*pos + 3 < len && strncmp(s + *pos, "true", 4) == 0) {
                *pos += 4;
                lua_pushboolean(L, 1);
                return 1;
            }
            return 0;

        case 'f': /* false */
            if (*pos + 4 < len && strncmp(s + *pos, "false", 5) == 0) {
                *pos += 5;
                lua_pushboolean(L, 0);
                return 1;
            }
            return 0;

        case 'n': /* null */
            if (*pos + 3 < len && strncmp(s + *pos, "null", 4) == 0) {
                *pos += 4;
                lua_getglobal(L, "cjson");
                if (lua_istable(L, -1)) {
                    lua_getfield(L, -1, "null");
                    lua_remove(L, -2);
                } else {
                    lua_pop(L, 1);
                    lua_pushnil(L);
                }
                return 1;
            }
            return 0;

        default: { /* number */
            char *end = NULL;
            double n = strtod(s + *pos, &end);
            if (end == s + *pos) return 0;
            *pos = (int)(end - s);
            lua_pushnumber(L, n);
            return 1;
        }
    }
}

static int cjson_decode(lua_State *L) {
    size_t len;
    const char *s = luaL_checklstring(L, 1, &len);
    int pos = 0;
    if (!json_decode_value(L, s, (int)len, &pos))
        return luaL_error(L, "invalid JSON");
    return 1;
}

static int cjson_encode_empty_as_object(lua_State *L) {
    (void)L;
    return 0; /* no-op compatibility stub */
}

static const luaL_Reg cjson_funcs[] = {
    {"encode",       cjson_encode},
    {"decode",       cjson_decode},
    {"encode_empty_table_as_object", cjson_encode_empty_as_object},
    {NULL, NULL}
};

int luaopen_cjson(lua_State *L) {
    luaL_register(L, "cjson", cjson_funcs);

    /* cjson.null sentinel (light userdata) */
    lua_pushlightuserdata(L, NULL);
    lua_setfield(L, -2, "null");

    return 1;
}
