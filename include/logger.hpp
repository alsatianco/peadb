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
// For signal handlers: writes one line with async-signal-safe calls only.
void log_from_signal_handler(LogLevel level, const char* message);
// For a child made by fork(): another thread may have held the log lock or the time-zone lock at
// the fork, so the child must not take them. log_before_fork() (in the parent, just before fork)
// saves the local time offset; log_in_forked_child() (in the child, just after it) makes every
// later log() call format the time with that offset and write the line with write(2) alone, like
// Redis's nolocks_localtime().
void log_before_fork();
void log_in_forked_child();

}  // namespace peadb
