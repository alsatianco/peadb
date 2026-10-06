// The log line of a forked BGSAVE or AOF rewrite child (compat/delta.md item 34). The child must
// not take the log lock or the time-zone lock, so it formats the local time itself from the offset
// the parent saved before the fork; this checks that the line it writes is the line log() writes
// in the parent: same date and time as localtime(), same level tag, levels filtered the same way.

#include "logger.hpp"

#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <string>

#include <sys/wait.h>
#include <unistd.h>

namespace {
int failures = 0;
void check(bool ok, const std::string& what) {
  if (!ok) {
    std::cerr << "FAIL: " << what << "\n";
    ++failures;
  }
}

std::string local_stamp(std::time_t t) {
  std::tm tm_buf{};
  localtime_r(&t, &tm_buf);
  char out[32];
  std::strftime(out, sizeof(out), "%Y-%m-%d %H:%M:%S", &tm_buf);
  return out;
}

// Forks a child that logs `message` at `level` the way a BGSAVE child does, and returns what it
// wrote to stdout.
std::string child_output(peadb::LogLevel level, const std::string& message) {
  int fds[2];
  if (::pipe(fds) != 0) return "pipe failed";
  peadb::log_before_fork();
  const pid_t pid = ::fork();
  if (pid == 0) {
    ::dup2(fds[1], STDOUT_FILENO);
    ::close(fds[0]);
    ::close(fds[1]);
    peadb::log_in_forked_child();
    peadb::log(level, message);
    ::_exit(0);
  }
  ::close(fds[1]);
  std::string out;
  char buf[256];
  ssize_t n = 0;
  while ((n = ::read(fds[0], buf, sizeof(buf))) > 0) out.append(buf, static_cast<std::size_t>(n));
  ::close(fds[0]);
  int status = 0;
  ::waitpid(pid, &status, 0);
  return out;
}

void check_zone(const char* zone) {
  ::setenv("TZ", zone, 1);
  ::tzset();
  const std::time_t before = std::time(nullptr);
  const std::string line = child_output(peadb::LogLevel::Warn, "Background saving error");
  const std::time_t after = std::time(nullptr);
  bool matched = false;
  for (std::time_t t = before; t <= after; ++t) {
    if (line == "[" + local_stamp(t) + "] [WARN] Background saving error\n") matched = true;
  }
  check(matched, std::string("child log line in TZ=") + zone + ": got '" + line + "', expected " +
                     "[" + local_stamp(before) + "] [WARN] Background saving error");
}
}  // namespace

int main() {
  peadb::set_log_level(peadb::LogLevel::Info);
  // Whole-hour, half-hour and quarter-hour offsets on both sides of UTC.
  for (const char* zone : {"UTC0", "Asia/Ho_Chi_Minh", "Asia/Kathmandu", "America/St_Johns",
                           "Pacific/Chatham", "America/Los_Angeles"}) {
    check_zone(zone);
  }
  // The level filter applies in the child too.
  peadb::set_log_level(peadb::LogLevel::Warn);
  check(child_output(peadb::LogLevel::Info, "DB saved on disk").empty(),
        "an Info line is dropped when the level is Warn");
  check(child_output(peadb::LogLevel::Error, "x").find("] [ERROR] x\n") != std::string::npos,
        "an Error line is written when the level is Warn");
  if (failures != 0) {
    std::cerr << failures << " check(s) failed\n";
    return 1;
  }
  std::cout << "logger_fork_test: all checks passed\n";
  return 0;
}
