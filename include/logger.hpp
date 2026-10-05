#pragma once

#include <string>

namespace peadb {

enum class LogLevel {
  Nothing = -1,  // Redis's loglevel "nothing": no line is printed
  Error = 0,
  Warn = 1,
  Info = 2,
  Debug = 3,
};

void set_log_level(LogLevel level);
// Maps a Redis loglevel name onto PeaDB's levels: debug and verbose -> Debug, notice -> Info,
// warning -> Warn, nothing -> Nothing. Returns false for any other name.
bool parse_redis_log_level(const std::string& name, LogLevel& level);
void log(LogLevel level, const std::string& message);

} // namespace peadb
