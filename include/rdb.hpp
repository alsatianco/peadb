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

// Load a Redis-compatible RDB file into the datastore (startup, DEBUG RELOAD, a replica's full
// sync). The header and the trailing CRC64 are checked first (the CRC64 unless rdbchecksum is no
// or DEBUG SET-SKIP-CHECKSUM-VALIDATION is on); a file that fails them leaves the dataset as it
// is (DEBUG RELOAD flushes it before the call, as Redis does). `before_replace` runs once those
// checks pass, right before rdb_load() flushes the keyspace. A file cut short fails after that.
bool rdb_load(const std::filesystem::path& path, std::string& err,
              const std::function<void()>& before_replace = nullptr);

// Serialize a single key+value into Redis DUMP format (RDB payload + CRC64).
std::string rdb_dump_key(const std::string& key);

// FUNCTION DUMP: every library's source in Redis's payload format (FUNCTION2 opcodes + footer).
std::string rdb_dump_functions(const std::vector<std::string>& library_codes);

// FUNCTION RESTORE: check a FUNCTION DUMP payload (version, CRC64) and return the library
// sources it holds; on failure sets err to Redis's reply text.
bool rdb_parse_function_payload(const std::string& payload, std::vector<std::string>& library_codes,
                                std::string& err);

// Restore a key from Redis DUMP payload. Returns true on success.
bool rdb_restore_key(const std::string& key, const std::string& payload,
                     std::int64_t ttl_ms, bool replace, bool absttl,
                     std::string& err);

// DEBUG SET-SKIP-CHECKSUM-VALIDATION: when set, RESTORE skips the DUMP-payload CRC64
// check (the upstream Redis test suite uses this to feed synthetic payloads).
void rdb_set_skip_checksum_validation(bool skip);

// rdbchecksum (startup-only, like Redis's IMMUTABLE_CONFIG): when off, RDB files and full syncs
// are written with a zero checksum and loaded without checking it. DUMP payloads always carry one.
void rdb_set_checksum(bool enabled);
bool rdb_checksum();

// Redis's CRC-64 (crc64.c: Jones polynomial, reflected, initial value 0): the checksum at the end
// of RDB files and DUMP payloads. crc64(0, "123456789", 9) == 0xe9c6d914c4b8d9ca; chaining works:
// crc64(crc64(0, a), b) == crc64(0, a + b).
std::uint64_t crc64(std::uint64_t crc, const void* data, std::size_t len);

} // namespace peadb
