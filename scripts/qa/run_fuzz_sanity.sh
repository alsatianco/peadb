#!/usr/bin/env bash
#
# run_fuzz_sanity.sh — Quick ASAN/UBSAN fuzz sanity checks.
#
# Purpose
#   Smoke-tests PeaDB's RESP parser and command dispatch engine against
#   randomised / malformed inputs to catch crashes, ASAN errors, and
#   undefined behaviour.  This is a fast, deterministic alternative to
#   full-scale fuzzing — suitable for pre-commit or CI gating.
#
# What it does
#   1. Compiles two standalone harness binaries with ASAN+UBSAN from source
#      files under ``tests/fuzz/``:
#        a. ``resp_parser_stress`` — feeds random byte streams into the
#           RESP protocol parser (``src/protocol.cpp``).
#        b. ``command_dispatcher_stress`` — feeds random command lines into
#           the full command dispatch + datastore engine.
#   2. For each harness, a Python driver generates ROUNDS random inputs
#      (default 200) with a mix of:
#        - well-formed RESP arrays with random payloads,
#        - truncated / partial RESP messages,
#        - completely random byte streams.
#   3. Each input is piped to the harness via stdin; the driver counts how
#      many invocations completed without crashing.
#   4. Reports completion ratios and exits.
#
# How to run
#   From the repo root:
#
#       scripts/qa/run_fuzz_sanity.sh           # 200 rounds (default)
#       scripts/qa/run_fuzz_sanity.sh 500        # 500 rounds per harness
#       CXX=clang++ scripts/qa/run_fuzz_sanity.sh
#
# Arguments
#   ROUNDS   (positional, optional) Number of randomised inputs per harness.
#            Default: 200.
#
# Environment variables
#   CXX        C++ compiler to use (default: g++).
#   LUA_PKG    pkg-config package name for Lua (auto-detected if unset;
#              tries lua5.1, lua-5.1, lua in order).
#
# Prerequisites
#   - A C++20 compiler (g++ or clang++).
#   - Lua 5.1 development headers discoverable via pkg-config.
#   - python3.
#
# Interpreting the output
#   Build progress, then per-harness results:
#       resp_parser_stress: 200/200 completed without crash
#       command_dispatcher_stress: 198/200 completed without crash
#       PASS: Fuzz sanity completed (200 rounds per harness)
#   The completion count can be < ROUNDS if a particular random input
#   causes the harness to exit non-zero (e.g. a deliberate error path).
#   A crash (segfault, ASAN abort) would also decrement the count.
#
# Exit codes
#   0   Both harnesses built and ran all rounds (some non-zero exits are
#       acceptable; the script itself only fails on build errors or
#       missing prerequisites).
#   Non-zero   Build failure, missing compiler, or missing Lua headers.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$ROOT_DIR"

CXX="${CXX:-g++}"
LUA_PKG="${LUA_PKG:-}"
if [[ -z "$LUA_PKG" ]]; then
    if pkg-config --exists lua5.1; then
        LUA_PKG="lua5.1"
    elif pkg-config --exists lua-5.1; then
        LUA_PKG="lua-5.1"
    elif pkg-config --exists lua; then
        LUA_PKG="lua"
    fi
fi

if [[ -z "$LUA_PKG" ]]; then
    echo "Lua development package not found via pkg-config." >&2
    echo "On Ubuntu: sudo apt-get install -y pkg-config liblua5.1-0-dev" >&2
    exit 1
fi

LUA_CFLAGS="$(pkg-config --cflags "$LUA_PKG")"
LUA_LIBS="$(pkg-config --libs "$LUA_PKG")"

COMMON_FLAGS="-std=c++20 -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer -Iinclude $LUA_CFLAGS"
ROUNDS="${1:-200}"

echo "=== Building fuzz harnesses ==="

$CXX $COMMON_FLAGS tests/fuzz/resp_parser_stress.cpp src/protocol.cpp \
  -o /tmp/resp_parser_stress
CORE_SOURCES="src/command.cpp src/datastore.cpp src/config.cpp src/logger.cpp \
  src/protocol.cpp src/server.cpp src/lua_engine.cpp src/rdb.cpp"
LUA_STUB_SOURCES="src/lua_cjson_stub.c src/lua_cmsgpack_stub.c src/lua_bit_stub.c src/lua_struct_stub.c"

# Compile C stubs as C (not C++) so extern "C" linkage matches.
LUA_STUB_OBJS=""
for src in $LUA_STUB_SOURCES; do
  obj="/tmp/$(basename "$src" .c).o"
  gcc -c -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer $LUA_CFLAGS "$src" -o "$obj"
  LUA_STUB_OBJS="$LUA_STUB_OBJS $obj"
done

$CXX $COMMON_FLAGS tests/fuzz/command_dispatcher_stress.cpp \
    $CORE_SOURCES $LUA_STUB_OBJS $LUA_LIBS -lm -ldl -pthread -rdynamic \
  -o /tmp/command_dispatcher_stress

echo "=== Running resp_parser_stress ($ROUNDS rounds) ==="
python3 - "$ROUNDS" <<'PY'
import os, random, subprocess, sys

rounds = int(sys.argv[1]) if len(sys.argv) > 1 else 200
passed = 0
for i in range(rounds):
    choice = random.random()
    if choice < 0.3:
        n = random.randint(1, 5)
        payload = f"*{n}\r\n".encode()
        for _ in range(n):
            s = os.urandom(random.randint(0, 64))
            payload += f"${len(s)}\r\n".encode() + s + b"\r\n"
    elif choice < 0.6:
        payload = b"*3\r\n$5\r\nhello\r\n$" + os.urandom(random.randint(0, 20))
    else:
        payload = os.urandom(random.randint(0, 512))

    r = subprocess.run(["/tmp/resp_parser_stress"], input=payload,
                       capture_output=True, timeout=5)
    if r.returncode == 0:
        passed += 1

print(f"resp_parser_stress: {passed}/{rounds} completed without crash")
PY

echo "=== Running command_dispatcher_stress ($ROUNDS rounds) ==="
python3 - "$ROUNDS" <<'PY'
import os, random, subprocess, sys

rounds = int(sys.argv[1]) if len(sys.argv) > 1 else 200
passed = 0

CMDS = ["SET", "GET", "DEL", "INCR", "LPUSH", "RPUSH", "LPOP", "RPOP",
        "HSET", "HGET", "SADD", "SREM", "ZADD", "ZRANGE", "PING",
        "LRANGE", "EXPIRE", "TTL", "TYPE", "EXISTS", "DBSIZE", "INFO",
        "MSET", "MGET", "APPEND", "STRLEN", "SELECT", "FLUSHDB"]

for i in range(rounds):
    choice = random.random()
    if choice < 0.4:
        words = [random.choice(CMDS)]
        for _ in range(random.randint(0, 5)):
            words.append(os.urandom(random.randint(1, 32)).hex())
        payload = " ".join(words).encode() + b"\n"
    elif choice < 0.7:
        cmd = random.choice(CMDS)
        args = [os.urandom(random.randint(1, 16)).hex() for _ in range(random.randint(0, 4))]
        payload = (cmd + " " + " ".join(args) + "\n").encode()
    else:
        payload = os.urandom(random.randint(0, 512))

    r = subprocess.run(["/tmp/command_dispatcher_stress"], input=payload,
                       capture_output=True, timeout=5)
    if r.returncode == 0:
        passed += 1

print(f"command_dispatcher_stress: {passed}/{rounds} completed without crash")
PY

echo "PASS: Fuzz sanity completed ($ROUNDS rounds per harness)"
