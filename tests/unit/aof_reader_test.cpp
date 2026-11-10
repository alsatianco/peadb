#include "command.hpp"
#include "zmalloc.hpp"
#include "datastore.hpp"

#include <stdexcept>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <unistd.h>

namespace {
int failures = 0;
void check(bool ok, const char* what) {
  if (!ok) {
    ++failures;
    std::cerr << "FAIL: " << what << '\n';
  }
}
std::string resp(std::initializer_list<std::string> args) {
  std::string out = "*" + std::to_string(args.size()) + "\r\n";
  for (const auto& arg : args) out += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
  return out;
}
void write(const std::filesystem::path& path, const std::string& bytes) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out << bytes;
}
std::string read(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}
void controls(const std::filesystem::path& dir) {
  using namespace peadb;
  const auto path = dir / "control.aof";
  std::string large(8 * 1024 * 1024, 'v');
  large.replace(65520, 12, "\r\n$*\0binary", 12);
  const auto bytes = resp({"SELECT", "3"}) + resp({"SET", "large", large}) + resp({"MULTI"}) +
                     resp({"INCR", "count"}) + resp({"EXEC"});
  write(path, bytes);
  std::string err;
  std::size_t previous = 0, inside = 0, calls = 0;
  std::uint64_t records = 0;
  AofLoadControl control;
  control.checkpoint = [&](const AofLoadProgress& p) {
    ++calls;
    check(p.bytes >= previous && p.bytes <= p.total && p.total == bytes.size(),
          "truthful monotonic consumed-byte progress");
    check(p.bytes - previous <= 65536, "bounded checkpoints within an argument");
    check(p.records >= records && p.records <= 5, "complete record progress");
    if (p.bytes > 100000 && p.bytes < large.size() - 100000) {
      ++inside;
      check(p.records == 1 && !store().get("large"), "partial argument is not executed");
    }
    records = p.records;
    previous = p.bytes;
    check(loading_mode() == LoadingMode::Blocking, "control has blocking admission");
    store().select_db(15);
    return true;
  };
  store().flushall();
  store().select_db(7);
  check(load_aof_file(path.string(), err, {}, &control), "streaming binary/multirecord replay");
  check(calls > 100 && inside > 100 && previous == bytes.size() && records == 5,
        "final byte/record progress");
  check(loading_mode() == LoadingMode::None && store().current_db() == 3,
        "callback DB selection cannot redirect replay");
  check(store().get("large") == large && store().get("count") == "1", "exact bytes/EXEC effects");

  // Cancel inside a large argument, not only at record boundaries, without tail repair.
  store().flushall();
  control.checkpoint = [&](const AofLoadProgress& p) {
    return p.bytes < 100000;
  };
  check(!load_aof_file(path.string(), err, {}, &control) && err == "AOF load cancelled",
        "mid-argument cancellation is distinct");
  check(!store().get("large") && !store().get("count") && read(path) == bytes,
        "cancelled partial command is not executed and source is never truncated");
  check(loading_mode() == LoadingMode::None, "cancellation restores loading state");

  const auto transaction = resp({"SET", "prefix", "kept"}) + resp({"MULTI"}) +
                           resp({"SET", "queued", "never"}) + resp({"EXEC"});
  write(path, transaction);
  control.checkpoint = [&](const AofLoadProgress& p) {
    return p.records < 3;
  };
  check(!load_aof_file(path.string(), err, {}, &control) && err == "AOF load cancelled",
        "cancel a queued transaction before EXEC");
  check(store().get("prefix") == "kept" && !store().get("queued") && read(path) == transaction,
        "cancellation retains prior commands, drops queued effects, preserves source");
  control.checkpoint = [&](const AofLoadProgress&) -> bool {
    store().select_db(12);
    throw std::runtime_error("observer failed");
  };
  store().select_db(9);
  bool threw = false;
  try {
    load_aof_file(path.string(), err, {}, &control);
  } catch (const std::runtime_error&) {
    threw = true;
  }
  check(threw && store().current_db() == 9 && loading_mode() == LoadingMode::None,
        "control exceptions restore replay/loading/DB context");

  // Read-ahead across an incomplete MULTI must truncate to the pre-MULTI offset.
  const auto prefix = resp({"SET", "prefix", "complete"});
  write(path, prefix + resp({"MULTI"}) + resp({"SET", "queued", "no"}) + "*1\r\n$4\r\nEX");
  check(load_aof_file(path.string(), err) && read(path) == prefix && !store().get("queued"),
        "torn transaction repair uses complete-command file offsets");
  write(path, "# annotation without newline");
  check(load_aof_file(path.string(), err) && read(path).empty(), "torn annotation is truncated");
  write(path, resp({"SET", "bad", "x"}));
  auto malformed = read(path);
  malformed.back() = 'x';
  write(path, malformed);
  check(!load_aof_file(path.string(), err) && err.find("Bad file format") == 0 &&
            read(path) == malformed,
        "bad bulk delimiter is fatal, never tail repair");
  write(path, "#" + std::string(1024, 'x') + "\n");
  check(!load_aof_file(path.string(), err) && err.find("Bad file format") == 0,
        "oversize line cannot grow input memory without bound");
  write(path, "*1\r\n$9223372036854775807\r\n");
  check(load_aof_file(path.string(), err) && read(path).empty(),
        "impossible declared bulk is a short read before allocation");
  write(path, "");
  previous = 1;
  control.checkpoint = [&](const AofLoadProgress& p) {
    previous = p.bytes;
    return true;
  };
  check(load_aof_file(path.string(), err, {}, &control) && previous == 0, "empty-file progress");
  check(!load_aof_file(dir.string(), err) && err.find("regular append log") != std::string::npos,
        "nonregular input is an I/O error, never short-read success");
  check(!load_aof_file((dir / "missing").string(), err), "open error fails");
}
}  // namespace

int main() {
  char scratch[] = "/tmp/peadb-aof-reader-XXXXXX";
  const char* dir = mkdtemp(scratch);
  if (!dir) return 2;
  const auto path = std::filesystem::path(dir) / "large.aof";
  {
    std::ofstream file(path, std::ios::binary);
    const std::string annotation = "#" + std::string(1000, 'x') + "\r\n";
    for (int i = 0; i < 40000; ++i) file << annotation;
  }
  const auto baseline = peadb::zmalloc_used_memory();
  std::size_t first_growth = 0;
  bool first = true;
  std::string err;
  const bool ok = peadb::load_aof_file(path.string(), err, [&] {
    if (first) {
      const auto now = peadb::zmalloc_used_memory();
      first_growth = now > baseline ? now - baseline : 0;
      first = false;
    }
  });
  controls(dir);
  std::filesystem::remove_all(dir);
  std::cout << "First-checkpoint heap growth: " << first_growth << '\n';
  if (!ok || first || first_growth > 256 * 1024) {
    std::cerr << "FAIL: AOF input must not retain the complete serialized file: " << err << '\n';
    return 1;
  }
  return failures ? 1 : 0;
}
