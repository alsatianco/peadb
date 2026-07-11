#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>
#include <vector>

namespace peadb {

// Function-library persistence hooks. Libraries live at the server level (command.cpp), not in
// the DataStore, so RDB save/load calls back through these (RDB opcode 245 / FUNCTION2, matching
// Redis). `dump` returns every library's full source; `load` feeds one library source back and
// returns false (setting err) on a collision, which fails the whole RDB load — this drives the
// DEBUG RELOAD NOFLUSH-without-MERGE "already exists" semantics. Registered once at startup by
// functions_register_rdb_hooks().
using FunctionDumpFn = std::function<std::vector<std::string>()>;
using FunctionLoadFn = std::function<bool(const std::string& library_code, std::string& err)>;
void rdb_register_function_hooks(FunctionDumpFn dump, FunctionLoadFn load);

// Save all databases to a Redis-compatible RDB file.
bool rdb_save(const std::filesystem::path& path, std::string& err);

// Save all databases to an in-memory RDB blob (for PSYNC full-sync).
// Sets err on failure (e.g. unsupported key types); returns empty string.
std::string rdb_save_to_string(std::string& err);

// Load a Redis-compatible RDB file into the datastore.
// verify_crc: check the trailing CRC64 (on-disk startup loads). Pass false for the
// replication path, which may receive an RDB from a foreign master (different CRC variant).
bool rdb_load(const std::filesystem::path& path, std::string& err, bool verify_crc = true);

// Serialize a single key+value into Redis DUMP format (RDB payload + CRC64).
std::string rdb_dump_key(const std::string& key);

// Restore a key from Redis DUMP payload. Returns true on success.
bool rdb_restore_key(const std::string& key, const std::string& payload,
                     std::int64_t ttl_ms, bool replace, bool absttl,
                     std::string& err);

// DEBUG SET-SKIP-CHECKSUM-VALIDATION: when set, RESTORE skips the DUMP-payload CRC64
// check (the upstream Redis test suite uses this to feed synthetic payloads).
void rdb_set_skip_checksum_validation(bool skip);

} // namespace peadb
