#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

namespace peadb {

// Save all databases to a Redis-compatible RDB file.
bool rdb_save(const std::filesystem::path& path, std::string& err);

// Save all databases to an in-memory RDB blob (for PSYNC full-sync).
std::string rdb_save_to_string();

// Load a Redis-compatible RDB file into the datastore.
bool rdb_load(const std::filesystem::path& path, std::string& err);

// Serialize a single key+value into Redis DUMP format (RDB payload + CRC64).
std::string rdb_dump_key(const std::string& key);

// Restore a key from Redis DUMP payload. Returns true on success.
bool rdb_restore_key(const std::string& key, const std::string& payload,
                     std::int64_t ttl_ms, bool replace, bool absttl,
                     std::string& err);

} // namespace peadb
