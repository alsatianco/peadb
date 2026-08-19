cmake_minimum_required(VERSION 3.16)

# The repo-root VERSION file is the single source of truth for PeaDB's own version.
# Keep it a plain MAJOR.MINOR.PATCH: project() rejects a "v" prefix or a "-rc1" suffix
# outright, and an empty file fails configure. scripts/qa/run_fuzz_sanity.sh reads the
# same file so its hand-rolled build cannot drift.
file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/VERSION" PEADB_VERSION_RAW LIMIT_COUNT 1)
project(peadb VERSION ${PEADB_VERSION_RAW} LANGUAGES C CXX)

# Default to Release if no build type specified (Debug builds are much slower)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Historical: macOS system Lua (5.2+) miscompiled at -O3. PeaDB now bundles Lua 5.1
# (third_party/lua-5.1.5) on every platform, matching the interpreter Redis 7.2.5
# embeds (number formatting, table iteration order, cjson/cmsgpack semantics).
# Keep -O2 on macOS for now so benchmark baselines stay comparable.
if(APPLE AND CMAKE_BUILD_TYPE STREQUAL "Release")
  set(CMAKE_C_FLAGS_RELEASE   "-O2 -DNDEBUG" CACHE STRING "" FORCE)
  set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG" CACHE STRING "" FORCE)
endif()

set(PEADB_NUM_SHARDS 1 CACHE STRING "Number of event-loop shards (1 = single-threaded, legacy behavior)")

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND AND NOT APPLE)
  pkg_check_modules(LIBURING QUIET liburing>=2.4)
  if(LIBURING_FOUND)
    message(STATUS "io_uring found (${LIBURING_VERSION}) — enabling PEADB_HAVE_IO_URING")
  endif()
endif()

# Bundled Lua 5.1.5 — the exact interpreter Redis 7.2.5 embeds. Scripting parity
# (float tostring, table hash order, cjson/cmsgpack output) depends on it, so the
# system Lua is never used.
file(GLOB LUA_BUNDLED_SOURCES ${CMAKE_SOURCE_DIR}/third_party/lua-5.1.5/src/*.c)
add_library(lua_bundled STATIC ${LUA_BUNDLED_SOURCES})
target_include_directories(lua_bundled PUBLIC ${CMAKE_SOURCE_DIR}/third_party/lua-5.1.5/src)
if(APPLE)
  target_compile_definitions(lua_bundled PRIVATE LUA_USE_MACOSX)
elseif(UNIX)
  target_compile_definitions(lua_bundled PRIVATE LUA_USE_LINUX)
endif()
set_target_properties(lua_bundled PROPERTIES POSITION_INDEPENDENT_CODE ON)
# Vanilla Lua 5.1 contains well-known benign UB (float->int casts in ltable.c) that
# Redis also ships; keep sanitizers out of the vendored code so ASAN/UBSAN builds of
# PeaDB itself stay usable.
target_compile_options(lua_bundled PRIVATE -fno-sanitize=all)

add_library(peadb_core
    src/command.cpp
    src/datastore.cpp
    src/config.cpp
    src/logger.cpp
    src/protocol.cpp
    src/server.cpp
    src/sha256.cpp
    src/shard_loop.cpp
    src/lua_engine.cpp
    src/rdb.cpp
    src/lua_cjson_stub.c
    src/lua_cmsgpack_stub.c
    src/lua_bit_stub.c
    src/lua_struct_stub.c
)
target_include_directories(peadb_core PUBLIC include)
target_link_libraries(peadb_core PUBLIC lua_bundled m)
target_compile_options(peadb_core PRIVATE -Wall -Wextra -Wno-unused-parameter)
target_compile_definitions(peadb_core PUBLIC PEADB_NUM_SHARDS=${PEADB_NUM_SHARDS})
target_compile_definitions(peadb_core PUBLIC PEADB_VERSION="${PROJECT_VERSION}")
if(LIBURING_FOUND)
  target_compile_definitions(peadb_core PUBLIC PEADB_HAVE_IO_URING)
  target_include_directories(peadb_core PUBLIC ${LIBURING_INCLUDE_DIRS})
  target_link_libraries(peadb_core PUBLIC ${LIBURING_LIBRARIES})
endif()

add_executable(peadb-server src/main.cpp)
# Place the binary at the repo root so integration tests can find it via
# ROOT / "peadb-server" without relying on an untracked symlink. Overridable
# (e.g. by scripts/qa/run_stability_checks.sh, which builds a sanitiser
# variant into build-asan/ and must not clobber the repo-root Release
# binary) via -DPEADB_SERVER_OUTPUT_DIR=... ; a plain
# -DCMAKE_RUNTIME_OUTPUT_DIRECTORY=... does NOT work here because this
# set_target_properties call would win over it regardless.
set(PEADB_SERVER_OUTPUT_DIR "${CMAKE_SOURCE_DIR}" CACHE PATH
    "Directory the peadb-server binary is built into")
set_target_properties(peadb-server PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${PEADB_SERVER_OUTPUT_DIR}")
if(APPLE)
  target_link_libraries(peadb-server PRIVATE peadb_core)
else()
  target_link_libraries(peadb-server PRIVATE peadb_core dl)
  target_link_options(peadb-server PRIVATE -rdynamic)
endif()

include(CTest)
if(BUILD_TESTING)
  add_executable(config_parser_test tests/unit/config_parser_test.cpp)
  target_link_libraries(config_parser_test PRIVATE peadb_core)
  add_test(NAME config_parser_test COMMAND config_parser_test)

  add_executable(wrongtype_framework_test tests/unit/wrongtype_framework_test.cpp)
  target_link_libraries(wrongtype_framework_test PRIVATE peadb_core)
  add_test(NAME wrongtype_framework_test COMMAND wrongtype_framework_test)

  add_executable(command_contract_test tests/unit/command_contract_test.cpp)
  target_link_libraries(command_contract_test PRIVATE peadb_core)
  add_test(NAME command_contract_test COMMAND command_contract_test)

  add_executable(mutation_epoch_test tests/unit/mutation_epoch_test.cpp)
  target_link_libraries(mutation_epoch_test PRIVATE peadb_core)
  add_test(NAME mutation_epoch_test COMMAND mutation_epoch_test)

  add_executable(expire_options_test tests/unit/expire_options_test.cpp)
  target_link_libraries(expire_options_test PRIVATE peadb_core)
  add_test(NAME expire_options_test COMMAND expire_options_test)

  add_executable(zadd_flags_test tests/unit/zadd_flags_test.cpp)
  target_link_libraries(zadd_flags_test PRIVATE peadb_core)
  add_test(NAME zadd_flags_test COMMAND zadd_flags_test)

  add_executable(multi_exec_test tests/unit/multi_exec_test.cpp)
  target_link_libraries(multi_exec_test PRIVATE peadb_core)
  add_test(NAME multi_exec_test COMMAND multi_exec_test)

  add_executable(entry_size_test tests/unit/entry_size_test.cpp)
  target_link_libraries(entry_size_test PRIVATE peadb_core)
  add_test(NAME entry_size_test COMMAND entry_size_test)

  add_executable(command_metadata_acl_test tests/unit/command_metadata_acl_test.cpp)
  target_link_libraries(command_metadata_acl_test PRIVATE peadb_core)
  add_test(NAME command_metadata_acl_test COMMAND command_metadata_acl_test)

  add_executable(sha256_test tests/unit/sha256_test.cpp)
  target_link_libraries(sha256_test PRIVATE peadb_core)
  add_test(NAME sha256_test COMMAND sha256_test)
endif()
