#include "logger.hpp"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
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
// Set in a forked child (log_in_forked_child()); the UTC offset of local time saved by the parent
// just before the fork (log_before_fork()).
bool g_forked_child = false;
long g_utc_offset_seconds = 0;

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

// The log line of log(), built without locks for a forked child: the local time is the Unix time
// plus the offset the parent saved, turned into a date with Howard Hinnant's civil_from_days().
void log_line_in_forked_child(LogLevel level, const std::string& message) {
  const long long t = static_cast<long long>(::time(nullptr)) + g_utc_offset_seconds;
  long long days = t / 86400;
  long long secs = t % 86400;
  if (secs < 0) {
    secs += 86400;
    --days;
  }
  days += 719468;
  const long long era = (days >= 0 ? days : days - 146096) / 146097;
  const long long doe = days - era * 146097;
  const long long yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  const long long doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
  const long long mp = (5 * doy + 2) / 153;
  const long long day = doy - (153 * mp + 2) / 5 + 1;
  const long long month = mp < 10 ? mp + 3 : mp - 9;
  const long long year = yoe + era * 400 + (month <= 2 ? 1 : 0);
  char stamp[64];
  std::snprintf(stamp, sizeof(stamp), "[%04lld-%02lld-%02lld %02lld:%02lld:%02lld] [%s] ", year,
                month, day, secs / 3600, secs / 60 % 60, secs % 60, to_string(level));
  std::string line = stamp;
  line += message;
  line += '\n';
  std::size_t off = 0;
  while (off < line.size()) {
    const ssize_t w = ::write(STDOUT_FILENO, line.data() + off, line.size() - off);
    if (w < 0 && errno == EINTR) continue;
    if (w <= 0) return;
    off += static_cast<std::size_t>(w);
  }
}
}  // namespace

void log_before_fork() {
  const std::time_t now = std::time(nullptr);
  std::tm tm_buf{};
  localtime_r(&now, &tm_buf);
  g_utc_offset_seconds = tm_buf.tm_gmtoff;
}

void log_in_forked_child() { g_forked_child = true; }

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
  if (g_forked_child) {
    log_line_in_forked_child(level, message);
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
