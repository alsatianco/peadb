#pragma once

#include <string>

namespace peadb {

enum class LogLevel {
  Error = 0,
  Warn = 1,
  Info = 2,
  Debug = 3,
};

void set_log_level(LogLevel level);
LogLevel parse_log_level(const std::string& level);
void log(LogLevel level, const std::string& message);

} // namespace peadb
