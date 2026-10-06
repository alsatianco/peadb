#include "logger.hpp"

#include <atomic>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>

#include <unistd.h>

namespace peadb {
namespace {
std::mutex g_log_mutex;
// Atomic because log_from_signal_handler() reads it inside a signal handler.
std::atomic<LogLevel> g_level{LogLevel::Info};

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
    case LogLevel::Nothing:
      break;
  }
  return "UNKNOWN";
}
}  // namespace

void set_log_level(LogLevel level) { g_level = level; }

bool parse_redis_log_level(const std::string& name, LogLevel& level) {
  // Redis logs accepted connections at "verbose", and PeaDB logs them at Debug, so verbose
  // shares Debug with debug.
  if (name == "debug" || name == "verbose") level = LogLevel::Debug;
  else if (name == "notice") level = LogLevel::Info;
  else if (name == "warning") level = LogLevel::Warn;
  else if (name == "nothing") level = LogLevel::Nothing;
  else return false;
  return true;
}

void log(LogLevel level, const std::string& message) {
  if (static_cast<int>(level) > static_cast<int>(g_level.load())) {
    return;
  }

  const auto now = std::chrono::system_clock::now();
  const auto now_time = std::chrono::system_clock::to_time_t(now);
  std::tm tm_buf {};
  localtime_r(&now_time, &tm_buf);

  std::ostringstream ts;
  ts << std::put_time(&tm_buf, "%Y-%m-%d %H:%M:%S");

  std::lock_guard<std::mutex> lock(g_log_mutex);
  // stdout + explicit flush, like Redis with an empty `logfile`: the upstream test
  // harness polls the captured stdout file for the startup log contract, so lines
  // must hit the fd immediately even when stdout is not a tty.
  std::cout << "[" << ts.str() << "] [" << to_string(level) << "] " << message << std::endl;
}

void log_from_signal_handler(LogLevel level, const char* message) {
  if (static_cast<int>(level) > static_cast<int>(g_level.load())) return;
  // Only async-signal-safe calls (time(2), write(2)), like Redis's serverLogFromHandler(): no
  // localtime, no stream, no lock. The line shows the Unix time instead of the local time.
  char line[512];
  std::size_t n = 0;
  auto put = [&](const char* s) {
    while (*s != '\0' && n < sizeof(line) - 1) line[n++] = *s++;
  };
  char digits[24];
  std::size_t nd = 0;
  for (auto t = static_cast<unsigned long long>(::time(nullptr)); nd == 0 || t != 0; t /= 10) {
    digits[nd++] = static_cast<char>('0' + t % 10);
  }
  put("[signal-handler ");
  while (nd > 0 && n < sizeof(line) - 1) line[n++] = digits[--nd];
  put("] [");
  put(to_string(level));
  put("] ");
  put(message);
  line[n++] = '\n';
  const ssize_t w = ::write(STDOUT_FILENO, line, n);
  (void)w;
}

}  // namespace peadb
