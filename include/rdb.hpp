#pragma once

#include "datastore.hpp"

#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <sys/types.h>

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

// Save all databases to a Redis-compatible RDB file, through temp-<pid>.rdb next to it (Redis's
// rdbSave()). On failure `err` is the line Redis logs for it.
struct RdbReplicationInfo;
bool rdb_save(const std::filesystem::path& path, std::string& err,
              const RdbReplicationInfo* repl = nullptr);

// The temp file rdb_save() writes in process `pid` before the rename: temp-<pid>.rdb in the
// dump's directory. rdb_remove_temp_file() deletes the one in the working directory (Redis's
// rdbRemoveTempFile(); `dbfilename` is always a plain file name there).
std::string rdb_temp_file_name(const std::filesystem::path& path, pid_t pid);
void rdb_remove_temp_file(pid_t pid);

// rdb-key-save-delay, a hidden Redis setting the upstream tests use to slow saves down:
// microseconds to wait after each key in an RDB file save or an AOF rewrite (negative: 1
// microsecond once in every -N keys). debug_delay() is Redis's debugDelay().
void rdb_set_key_save_delay(int usec);
int rdb_key_save_delay();
void debug_delay(int usec);

// The replication state a full sync's RDB carries (Redis's rdbSaveInfo): the database the
// replication stream has selected, and the replication id and offset the RDB was taken at.
struct RdbReplicationInfo {
  int stream_db = 0;
  std::string replid;
  std::int64_t offset = 0;
};

// Serialize all databases to an in-memory RDB blob, with the repl-stream-db,
// repl-id and repl-offset aux fields when `repl` is given.
// Sets err on failure (e.g. unsupported key types); returns empty string.
std::string rdb_save_to_string(std::string& err, const RdbReplicationInfo* repl = nullptr);

// Load a Redis-compatible RDB file into the datastore (startup, DEBUG RELOAD, a replica's full
// sync). The header and the trailing CRC64 are checked first (the CRC64 unless rdbchecksum is no
// or DEBUG SET-SKIP-CHECKSUM-VALIDATION is on); a file that fails them leaves the dataset as it
// is (DEBUG RELOAD flushes it before the call, as Redis does). When `before_replace` is
// given (full sync), a structural decoding pass also checks all database IDs and values before
// the callback publishes the snapshot and the loader flushes the keyspace. It discards one value
// at a time, then re-decodes the same bytes to apply. Function-library Lua compilation/collisions
// and apply-time allocation failures are not staged and can still fail after replacement.
// `repl_stream_db`, when given, receives the file's repl-stream-db aux field (-1 without one).
bool rdb_load(const std::filesystem::path& path, std::string& err,
              const std::function<void()>& before_replace = nullptr, int* repl_stream_db = nullptr);

// Serialize a single key+value into Redis DUMP format (RDB payload + CRC64).
std::string rdb_dump_key(const std::string& key);

// FUNCTION DUMP: every library's source in Redis's payload format (FUNCTION2 opcodes + footer).
std::string rdb_dump_functions(const std::vector<std::string>& library_codes);

// FUNCTION RESTORE: check a FUNCTION DUMP payload (version, CRC64) and return the library
// sources it holds; on failure sets err to Redis's reply text.
bool rdb_parse_function_payload(const std::string& payload, std::vector<std::string>& library_codes,
                                std::string& err);

// A value read from a DUMP payload by RESTORE, held apart from the keyspace until it is stored.
struct DumpValue {
  DataStore::StreamDump stream;
  std::uint8_t type = 0;                                    // the RDB type byte
  std::string str;                                          // string
  std::vector<std::string> items;                           // list items, set members
  std::vector<std::pair<std::string, std::string>> fields;  // hash
  std::vector<std::pair<std::string, double>> scored;       // zset members and scores
};

// RESTORE, Redis's verifyDumpPayload() and rdbLoadObject(): check the payload's footer and read
// its value without touching the keyspace. On failure sets err to Redis's reply text.
bool rdb_decode_dump_payload(const std::string& payload, DumpValue& out, std::string& err);

// Store a value rdb_decode_dump_payload() read under `key`, which must not exist, with its expire
// time if it has one.
void rdb_store_dump_value(const std::string& key, DumpValue&& value,
                          std::optional<std::int64_t> expire_at_ms);

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

}  // namespace peadb
