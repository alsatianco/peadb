cmake_minimum_required(VERSION 3.16)
project(peadb LANGUAGES C CXX)

# Default to Release if no build type specified (Debug builds are much slower)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

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
    src/lua_engine.cpp
    src/rdb.cpp
    src/lua_cjson_stub.c
    src/lua_cmsgpack_stub.c
    src/lua_bit_stub.c
    src/lua_struct_stub.c
)
target_include_directories(peadb_core PUBLIC include ${LUA_INCLUDE_DIR})
target_link_libraries(peadb_core PUBLIC ${LUA_LIBRARIES} m)

add_executable(peadb-server src/main.cpp)
target_link_libraries(peadb-server PRIVATE peadb_core dl)
target_link_options(peadb-server PRIVATE -rdynamic)

include(CTest)
if(BUILD_TESTING)
  add_executable(config_parser_test tests/unit/config_parser_test.cpp)
  target_link_libraries(config_parser_test PRIVATE peadb_core)
  add_test(NAME config_parser_test COMMAND config_parser_test)

  add_executable(wrongtype_framework_test tests/unit/wrongtype_framework_test.cpp)
  target_link_libraries(wrongtype_framework_test PRIVATE peadb_core)
  add_test(NAME wrongtype_framework_test COMMAND wrongtype_framework_test)
endif()
