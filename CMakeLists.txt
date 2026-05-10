cmake_minimum_required(VERSION 3.16)
project(peadb LANGUAGES C CXX)

# Default to Release if no build type specified (Debug builds are much slower)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# On macOS with non-bundled Lua (5.2+), -O3 triggers miscompilation in Lua C
# API interactions.  Cap at -O2 until PeaDB bundles its own Lua 5.1.
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

find_package(Lua 5.1)
if(NOT LUA_LIBRARIES OR NOT LUA_INCLUDE_DIR)
  message(FATAL_ERROR
    "Lua 5.1 development files not found (headers + library).\n"
    "\n"
    "Install Lua 5.1 dev package, e.g.:\n"
    "  Ubuntu/Debian: sudo apt-get install liblua5.1-0-dev\n"
    "\n"
    "Alternatively, set LUA_INCLUDE_DIR and LUA_LIBRARIES to point at your Lua 5.1 install."
  )
endif()

add_library(peadb_core
    src/command.cpp
    src/datastore.cpp
    src/config.cpp
    src/logger.cpp
    src/protocol.cpp
    src/server.cpp
    src/shard_loop.cpp
    src/lua_engine.cpp
    src/rdb.cpp
    src/lua_cjson_stub.c
    src/lua_cmsgpack_stub.c
    src/lua_bit_stub.c
    src/lua_struct_stub.c
)
target_include_directories(peadb_core PUBLIC include ${LUA_INCLUDE_DIR})
target_link_libraries(peadb_core PUBLIC ${LUA_LIBRARIES} m)
target_compile_options(peadb_core PRIVATE -Wall -Wextra -Wno-unused-parameter)
target_compile_definitions(peadb_core PUBLIC PEADB_NUM_SHARDS=${PEADB_NUM_SHARDS})
if(LIBURING_FOUND)
  target_compile_definitions(peadb_core PUBLIC PEADB_HAVE_IO_URING)
  target_include_directories(peadb_core PUBLIC ${LIBURING_INCLUDE_DIRS})
  target_link_libraries(peadb_core PUBLIC ${LIBURING_LIBRARIES})
endif()

add_executable(peadb-server src/main.cpp)
# Place the binary at the repo root so integration tests can find it via
# ROOT / "peadb-server" without relying on an untracked symlink.
set_target_properties(peadb-server PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}")
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
endif()
