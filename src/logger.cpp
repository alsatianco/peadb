#include "logger.hpp"

#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>

namespace peadb {
namespace {
std::mutex g_log_mutex;
LogLevel g_level = LogLevel::Info;

const char* to_string(LogLevel level) {
  switch (level) {
    case LogLevel::Error:
      return "ERROR";
    case LogLevel::Warn:
      return "WARN";
    case LogLevel::Info:
      return "INFO";
    case LogLevel::Debug:
      return "DEBUG";
  }
  return "UNKNOWN";
}
}  // namespace

void set_log_level(LogLevel level) { g_level = level; }

LogLevel parse_log_level(const std::string& level) {
  if (level == "error") return LogLevel::Error;
  if (level == "warn") return LogLevel::Warn;
  if (level == "info") return LogLevel::Info;
  if (level == "debug") return LogLevel::Debug;
  return LogLevel::Info;
}

void log(LogLevel level, const std::string& message) {
  if (static_cast<int>(level) > static_cast<int>(g_level)) {
    return;
  }

  const auto now = std::chrono::system_clock::now();
  const auto now_time = std::chrono::system_clock::to_time_t(now);
  std::tm tm_buf {};
  localtime_r(&now_time, &tm_buf);

  std::ostringstream ts;
  ts << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");

  std::lock_guard<std::mutex> lock(g_log_mutex);
  std::cerr << "[" << ts.str() << "] [" << to_string(level) << "] " << message << '\n';
}

}  // namespace peadb
