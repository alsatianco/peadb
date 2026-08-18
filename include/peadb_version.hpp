#pragma once

// PEADB_VERSION carries PeaDB's own version, which is distinct from the Redis 7.2.5
// compatibility target reported as redis_version. The build injects it from the repo-root
// VERSION file: CMakeLists.txt reads that file with file(STRINGS ...), and
// scripts/qa/run_fuzz_sanity.sh passes the same value when it compiles src/command.cpp
// outside CMake.
//
// There is deliberately no fallback definition. A build path that forgets to define
// PEADB_VERSION must fail here rather than silently report a stale version in INFO.
#ifndef PEADB_VERSION
#error "PEADB_VERSION is not defined - the build must pass -DPEADB_VERSION=\"<major.minor.patch>\" sourced from the repo-root VERSION file"
#endif
