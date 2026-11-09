#include "command.hpp"
#include "functions.hpp"
#include "lua_engine.hpp"
#include "rdb.hpp"
#include "rdb_reader.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <unistd.h>

namespace {
int failures = 0;
void check(bool ok, const char* what) {
  if (!ok) {
    ++failures;
    std::cerr << "FAIL: " << what << '\n';
  }
}
using Clock = std::chrono::steady_clock;
using Phase = peadb::RdbLoadProgress::Phase;

void exercise(const std::filesystem::path& path) {
  using namespace peadb;
  auto& live = store();
  live.select_db(3);
  const std::string large(2 * 1024 * 1024, 'v');
  live.set("large", large, false, false, {}, false);
  RdbReplicationInfo original{3, std::string(40, 'b'), 127};
  std::string err;
  check(rdb_save(path, err, &original), "save fixture");
  const auto size = std::filesystem::file_size(path);
  live.flushall();
  live.select_db(7);
  live.set("sentinel", "live", false, false, {}, false);
  DataStore target;
  FunctionRegistry functions;
  RdbLoadStats stats;
  RdbReplicationInfo aux;
  RdbLoadControl control;
  RdbLoadTarget load{target, functions, stats};
  load.replication = &aux;
  load.control = &control;
  target.select_db(11);
  std::size_t previous = 0, callbacks = 0, inside_large = 0;
  Phase phase = Phase::Checksum;
  control.checkpoint = [&](const RdbLoadProgress& p) {
    ++callbacks;
    if (p.phase != phase) {
      check(phase == Phase::Checksum && p.phase == Phase::Apply, "ordered pass transition");
      phase = p.phase;
      previous = 0;
    }
    check(p.bytes >= previous && p.bytes <= p.total && p.total == size, "truthful byte progress");
    check(p.bytes - previous <= 65536, "bounded progress within a large serialized value");
    previous = p.bytes;
    if (p.phase == Phase::Apply && p.bytes > 100000 && p.bytes < size - 100000) {
      ++inside_large;
      check(p.keys == 0 && stats.loaded == 0, "partial value is not an applied key");
    }
    if (p.phase == Phase::Checksum) check(p.keys == 0, "checksum is not applied progress");
    // A later event pump may select a client's DB; decoding must resume in the target DB.
    target.select_db(15);
    return true;
  };
  check(rdb_load_into(path, load, err), "controlled load succeeds");
  check(callbacks > 60 && inside_large > 20 && previous == size, "large-value/final callbacks");
  check(stats.loaded == 1 && stats.expired == 0 && aux.valid() && aux.offset == 127,
        "complete counters and AUX only on success");
  check(target.current_db() == 11 && live.current_db() == 7, "target/live DB context restored");
  target.select_db(3);
  check(target.get("large") == large, "callback DB selection cannot redirect insertion");
  target.select_db(11);

  // Cancellation in checksum leaves both target and caller-owned replacement untouched.
  unsigned replacements = 0;
  control.checkpoint = [&](const RdbLoadProgress& p) {
    check(p.phase == Phase::Checksum, "cancel before application");
    return p.bytes < 65536;
  };
  check(!rdb_load(
            path, err, [&] { ++replacements; }, &aux, {}, &control) &&
            err == "RDB load cancelled" && replacements == 0 && !aux.valid(),
        "checksum cancellation is distinct and cannot publish replacement/AUX");
  check(live.get("sentinel") == "live", "checksum cancellation keeps live keys");

  // A full-sync preflight pass is separately identified and cancellable before replacement.
  unsigned preflight_callbacks = 0;
  control.checkpoint = [&](const RdbLoadProgress& p) {
    if (p.phase == Phase::Preflight) {
      ++preflight_callbacks;
      check(p.keys == 0, "preflight does not claim applied keys");
      return p.bytes < 100000;
    }
    return true;
  };
  check(!rdb_load(
            path, err, [&] { ++replacements; }, &aux, {}, &control) &&
            err == "RDB load cancelled" && replacements == 0 && preflight_callbacks > 1,
        "preflight cancellation preserves replacement boundary");

  control.checkpoint = [&](const RdbLoadProgress& p) {
    target.select_db(14);
    return p.phase != Phase::Apply || p.bytes < 100000;
  };
  check(!rdb_load_into(path, load, err) && err == "RDB load cancelled" &&
            target.current_db() == 11 && !aux.valid() && stats.loaded == 0,
        "mid-value cancellation restores context without publishing AUX or a partial key");
  target.select_db(3);
  check(!target.get("large"), "partly decoded value never inserted");
  target.select_db(11);
  control.checkpoint = [&](const RdbLoadProgress& p) -> bool {
    if (p.phase == Phase::Apply && p.bytes > 100000) {
      target.select_db(14);
      throw std::runtime_error("callback failed");
    }
    return true;
  };
  bool threw = false;
  try {
    rdb_load_into(path, load, err);
  } catch (const std::runtime_error&) {
    threw = true;
  }
  check(threw && target.current_db() == 11, "callback exception restores database and propagates");

  // A multi-second key delay must not withhold cancellation for its whole duration.
  control.key_delay = 2000000;
  std::optional<Clock::time_point> began;
  unsigned during_delay = 0;
  control.checkpoint = [&](const RdbLoadProgress& p) {
    if (p.phase != Phase::Apply || p.keys == 0) return true;
    if (!began) began = Clock::now();
    ++during_delay;
    return Clock::now() - *began < std::chrono::milliseconds(40);
  };
  check(!rdb_load_into(path, load, err) && err == "RDB load cancelled" && began &&
            Clock::now() - *began < std::chrono::milliseconds(500) && during_delay > 1 &&
            target.current_db() == 11 && !aux.valid(),
        "delay polling cancels promptly after a whole key");

  control = {};
  check(rdb_load_into(path, load, err) && err.empty() && stats.loaded == 1 && aux.valid(),
        "healthy retry after cancellation");
  // INT_MIN is a supported Redis config value; signed negation must not overflow.
  control.key_delay = std::numeric_limits<int>::min();
  check(rdb_load_into(path, load, err), "INT_MIN sampled delay avoids signed overflow");
  debug_delay(std::numeric_limits<int>::min());

  // Cancellation applies to repeated EINTR even when no bytes have been consumed.
  unsigned reads = 0, polls = 0;
  detail::RdbReader interrupted(
      [&](void*, std::size_t) -> ssize_t {
        ++reads;
        errno = EINTR;
        return -1;
      },
      100, false,
      [&](std::size_t bytes) {
        check(bytes == 0, "EINTR progress counts no bytes");
        if (++polls == 5) throw std::runtime_error("cancel interrupted source");
      });
  threw = false;
  uint8_t byte;
  try {
    interrupted.read_u8(byte);
  } catch (const std::runtime_error&) {
    threw = true;
  }
  check(threw && reads == 4 && polls == 5 && interrupted.pos() == 0,
        "EINTR retry polls before each read without phantom progress");

  // Corruption must not be reported as cancellation or replace a healthy target.
  std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
  file.seekp(100000);
  file.put('x');
  file.close();
  control = {};
  check(!rdb_load_into(path, load, err) && err.find("Wrong RDB checksum") != std::string::npos,
        "ordinary CRC failure stays distinct from cancellation");
  check(live.current_db() == 7 && live.get("sentinel") == "live",
        "private loading leaves live state");
}
}  // namespace

int main() {
  peadb::lua_engine_init();
  char scratch[] = "/tmp/peadb-load-control-XXXXXX";
  const char* dir = mkdtemp(scratch);
  if (!dir) return 1;
  exercise(std::filesystem::path(dir) / "dump.rdb");
  std::filesystem::remove_all(dir);
  peadb::lua_engine_shutdown();
  return failures ? 1 : 0;
}
