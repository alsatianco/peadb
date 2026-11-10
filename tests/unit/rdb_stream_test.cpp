#include "command.hpp"
#include "functions.hpp"
#include "lua_engine.hpp"
#include "rdb.hpp"
#include "zmalloc.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <unistd.h>

namespace {
int failures = 0;
void check(bool ok, const char* message) {
  if (!ok) {
    ++failures;
    std::cerr << "FAIL: " << message << '\n';
  }
}
const std::string marker(40, 'm');
const std::string suffix = "*1\r\n$4\r\nPING\r\n";
std::string snapshot(std::string body) {
  body = "REDIS0011" + body + char(255);
  const auto crc = peadb::crc64(0, body.data(), body.size());
  for (int i = 0; i < 8; ++i) body += char(crc >> (8 * i));
  return body;
}
std::string bulk(const std::string& value) {
  return std::string{char(0x80), char(value.size() >> 24), char(value.size() >> 16),
                     char(value.size() >> 8), char(value.size())} +
         value;
}
std::string aux() {
  std::string result;
  for (const auto& pair : {std::pair<std::string, std::string>{"repl-stream-db", "3"},
                           {"repl-id", std::string(40, 'a')},
                           {"repl-offset", "42"}})
    result += char(250) + bulk(pair.first) + bulk(pair.second);
  return result;
}
std::string key(const std::string& name, const std::string& value) {
  return char(0) + bulk(name) + bulk(value);
}
peadb::RdbStreamFrame frame(bool eof, std::size_t size) {
  return {eof ? peadb::RdbStreamFrame::Kind::Eof : peadb::RdbStreamFrame::Kind::Length, size,
          eof ? marker : ""};
}
void boundaries() {
  using namespace peadb;
  const std::string value = std::string("binary\0", 7) + marker + std::string(180000, 'v');
  const auto bytes = snapshot(aux() + key("key", value));
  auto& live = store();
  live.flushall();
  live.select_db(7);
  live.set("live", "untouched", false, false, {}, false);
  const auto global_stats = rdb_last_load_stats();
  for (bool eof : {false, true}) {
    const auto input = bytes + (eof ? marker : "") + suffix;
    for (std::size_t fragment : {std::size_t(1), std::size_t(997), std::size_t(65536)}) {
      DataStore staging;
      staging.select_db(11);
      FunctionRegistry functions;
      RdbLoadStats stats;
      RdbReplicationInfo aux;
      RdbLoadControl control;
      RdbLoadTarget target{staging, functions, stats};
      target.control = &control;
      target.replication = &aux;
      std::size_t offset = 0, largest = 0, previous = 0, checkpoints = 0;
      unsigned calls = 0;
      control.checkpoint = [&](const RdbLoadProgress& p) {
        check(p.phase == RdbLoadProgress::Phase::Apply && p.bytes >= previous &&
                  p.bytes <= bytes.size() && p.total == (eof ? 0 : bytes.size()),
              "single-pass consumed progress/unknown EOF total");
        check(p.bytes - previous <= 65536, "bounded progress within large value");
        previous = p.bytes;
        ++checkpoints;
        staging.select_db(15);
        return true;
      };
      RdbStreamResult result;
      std::string err;
      const bool loaded = rdb_load_stream_into(
          [&](void* p, std::size_t n) -> ssize_t {
            largest = std::max(largest, n);
            if (++calls % 17 == 1) {
              errno = EINTR;
              return -1;
            }
            n = std::min({n, fragment, input.size() - offset});
            std::memcpy(p, input.data() + offset, n);
            offset += n;
            return n;
          },
          frame(eof, bytes.size()), target, result, err);
      check(loaded && err.empty(), "fragmented direct input succeeds");
      check(aux.valid() && aux.stream_db == 3 && aux.offset == 42 &&
                aux.replid == std::string(40, 'a'),
            "AUX published after complete frame");
      check(staging.current_db() == 11, "callback and final target DB restored");
      staging.select_db(0);
      check(staging.get("key") == value && stats.loaded == 1 && stats.expired == 0,
            "binary value containing delimiter not truncated");
      check(result.rdb_bytes == bytes.size() && result.frame_bytes == bytes.size() + (eof ? 40 : 0),
            "exact RDB/frame bytes");
      check(result.read_ahead + input.substr(offset) == suffix, "exact command suffix returned");
      check(largest <= 65536 && result.read_ahead.size() <= 65536 && checkpoints > 3 &&
                previous == bytes.size(),
            "bounded buffer/progress");
      check(eof || offset == bytes.size(), "length source never reads command bytes");
      check(live.current_db() == 7 && live.get("live") == "untouched" &&
                rdb_last_load_stats().loaded == global_stats.loaded,
            "private stream does not touch live state/counters");
    }
  }
}
void failures_and_controls() {
  using namespace peadb;
  const std::string library =
      "#!lua name=stream\nredis.register_function('ok',function() return 42 end)";
  const auto bytes = snapshot(aux() + key("key", std::string(150000, 'v')));
  auto exercise = [&](std::string input, RdbStreamFrame framing, int mode,
                      const char* expected = nullptr) {
    DataStore staging;
    staging.select_db(12);
    FunctionRegistry functions;
    RdbLoadStats stats;
    RdbReplicationInfo aux{0, std::string(40, 'a'), 42};
    RdbLoadControl control;
    control.checkpoint = [&](const RdbLoadProgress& p) {
      if (mode == 4 && p.bytes >= 65536) return false;
      if (mode == 5 && p.bytes >= 65536) throw std::runtime_error("checkpoint exception");
      return true;
    };
    RdbLoadTarget target{staging, functions, stats};
    target.control = &control;
    target.replication = &aux;
    RdbStreamResult result{123, 456, "stale"};
    std::size_t offset = 0;
    std::string err;
    bool threw = false, loaded = false;
    try {
      loaded = rdb_load_stream_into(
          [&](void* p, std::size_t n) -> ssize_t {
            if (mode == 1 && offset >= 65536) {
              errno = EIO;
              return -1;
            }
            if (mode == 2) return n + 1;
            if (mode == 3) throw std::runtime_error("source exception");
            n = std::min({n, std::size_t(65536), input.size() - offset});
            std::memcpy(p, input.data() + offset, n);
            offset += n;
            return n;
          },
          framing, target, result, err);
    } catch (const std::runtime_error&) {
      threw = true;
    }
    check(!loaded && (threw == (mode == 3 || mode == 5)), "failed/throwing source refuses");
    if (expected) check(err.find(expected) != std::string::npos, "specific failure reported");
    check(staging.current_db() == 12 && !aux.valid() && result.rdb_bytes == 0 &&
              result.frame_bytes == 0 && result.read_ahead.empty(),
          "failure cannot publish outputs");
    staging.select_db(0);
    staging.set("past", "v", false, false, 1, false);
    check(!staging.get("past"), "expiry guard restored after failure");
    check(store().current_db() == 7 && store().get("live") == "untouched",
          "failure isolates live state");
  };
  for (bool eof : {false, true}) {
    const auto framing = frame(eof, bytes.size());
    const auto input = bytes + (eof ? marker : "");
    for (std::size_t cut : {std::size_t(0), std::size_t(8), std::size_t(65537), bytes.size() - 1})
      exercise(input.substr(0, cut), framing, 0);
    auto corrupt = input;
    corrupt[bytes.size() - 1] ^= 1;
    exercise(corrupt, framing, 0, "Wrong RDB checksum");
    for (int mode = 1; mode <= 5; ++mode)
      exercise(input, framing, mode, mode == 4 ? "RDB load cancelled" : nullptr);
    exercise("REDIS0012" + input.substr(9), framing, 0, "Can't handle RDB format version 12");
    const auto invalid_db = snapshot(std::string("\xfe\x10", 2));
    exercise(invalid_db + (eof ? marker : ""), frame(eof, invalid_db.size()), 0, "more than 16");
    const auto bad_function =
        snapshot(char(245) + bulk(library) + char(245) + bulk("#!lua name=bad\ninvalid Lua"));
    exercise(bad_function + (eof ? marker : ""), frame(eof, bad_function.size()), 0);
  }
  exercise(bytes + marker.substr(0, 39), frame(true, 0), 0, "trailer");
  exercise(bytes + std::string(40, 'x') + suffix, frame(true, 0), 0, "trailer");
  exercise(bytes + "x", frame(false, bytes.size() + 1), 0, "length");
  exercise(bytes, frame(false, bytes.size() - 1), 0);
  exercise(bytes, {RdbStreamFrame::Kind::Eof, 0, "short"}, 0, "frame");
  exercise(bytes, frame(false, 8), 0, "frame");

  // Delay checkpoints must remain cancellable even without more source input.
  const auto small = snapshot(key("key", "v"));
  DataStore staging;
  FunctionRegistry functions;
  RdbLoadStats stats;
  RdbLoadTarget target{staging, functions, stats};
  unsigned delayed = 0;
  RdbLoadControl control{[&](const RdbLoadProgress& p) { return p.keys == 0 || ++delayed < 3; },
                         2000000};
  target.control = &control;
  std::size_t offset = 0;
  RdbStreamResult result;
  std::string err;
  check(!rdb_load_stream_into(
            [&](void* p, std::size_t n) -> ssize_t {
              n = std::min(n, small.size() - offset);
              std::memcpy(p, small.data() + offset, n);
              offset += n;
              return n;
            },
            frame(false, small.size()), target, result, err) &&
            err == "RDB load cancelled" && delayed == 3,
        "key delay polls cancellation without another read");

  bool worker_ok = true;
  std::thread worker([&] {
    DataStore private_data;
    FunctionRegistry private_functions;
    RdbLoadStats private_stats{88, 99};
    RdbLoadTarget private_target{private_data, private_functions, private_stats};
    bool read = false;
    RdbStreamResult out;
    std::string error;
    worker_ok = rdb_load_stream_into(
        [&](void*, std::size_t) -> ssize_t {
          read = true;
          return 0;
        },
        frame(false, small.size()), private_target, out, error);
    check(!read && private_stats.loaded == 88 && private_stats.expired == 99,
          "worker refusal precedes source and target mutation");
  });
  worker.join();
  check(!worker_ok, "stream decode requires Lua owner");
}
void checksum_policy() {
  using namespace peadb;
  const auto valid = snapshot(key("key", "value"));
  for (bool eof : {false, true}) {
    // The footer remains mandatory even when its comparison is disabled. Version four has
    // no checksum footer; an otherwise identical complete legacy frame still loads.
    for (int mode = 0; mode < 6; ++mode) {
      std::string bytes = valid;
      if (mode == 0) std::fill(bytes.end() - 8, bytes.end(), char(0));
      if (mode == 1 || mode == 2) bytes.back() ^= 1;
      if (mode == 3 || mode == 4) bytes.pop_back();
      if (mode == 5) {
        bytes.replace(5, 4, "0004");
        bytes.resize(bytes.size() - 8);
      }
      const auto input = bytes + (eof ? marker : "") + suffix;
      DataStore staging;
      FunctionRegistry functions;
      RdbLoadStats stats;
      RdbLoadTarget target{staging, functions, stats};
      target.checksum = mode != 1 && mode != 3;
      target.skip_checksum_validation = mode == 2 || mode == 4;
      RdbStreamResult result;
      std::string err;
      std::size_t offset = 0;
      const bool loaded = rdb_load_stream_into(
          [&](void* p, std::size_t n) -> ssize_t {
            n = std::min(n, input.size() - offset);
            std::memcpy(p, input.data() + offset, n);
            offset += n;
            return n;
          },
          frame(eof, bytes.size()), target, result, err);
      const bool expected = mode != 3 && mode != 4;
      check(loaded == expected, "explicit checksum policy/mandatory footer/legacy version");
      if (loaded)
        check(staging.get("key") == "value" && result.read_ahead + input.substr(offset) == suffix,
              "checksum override keeps exact frame/value");
    }
  }
}

void bounded_memory() {
  using namespace peadb;
  char scratch[] = "/tmp/peadb-rdb-stream-XXXXXX";
  const char* dir = mkdtemp(scratch);
  check(dir != nullptr, "memory fixture directory");
  if (!dir) return;
  const auto path = std::filesystem::path(dir) / "dump.rdb";
  auto& live = store();
  live.flushall();
  live.select_db(5);
  {
    DataStore::LazyExpireDisabled guard(live);
    const std::string value(256 * 1024, 'v');
    for (int i = 0; i < 256; ++i)
      live.set("expired:" + std::to_string(i), value, false, false, 1, false);
  }
  functions_register_rdb_hooks();
  std::string err;
  check(rdb_save(path, err), "64 MiB stream fixture saves");
  const auto size = std::filesystem::file_size(path);
  check(size > 64 * 1024 * 1024, "stream fixture exceeds 64 MiB");
  live.flushall();
  live.select_db(7);
  live.set("live", "untouched", false, false, {}, false);
  for (bool eof : {false, true}) {
    DataStore staging;
    staging.select_db(13);
    FunctionRegistry functions;
    RdbLoadStats stats;
    RdbLoadTarget target{staging, functions, stats};
    std::size_t peak = 0;
    RdbLoadControl control{[&](const RdbLoadProgress&) {
      peak = std::max(peak, zmalloc_used_memory());
      return true;
    }};
    target.control = &control;
    unsigned expired = 0;
    target.expired = [&](int db, const std::string&) {
      peak = std::max(peak, zmalloc_used_memory());
      ++expired;
      check(db == 5, "stream expiry uses target DB");
    };
    std::ifstream input(path, std::ios::binary);
    const auto tail = (eof ? marker : "") + suffix;
    std::size_t offset = 0;
    RdbStreamResult result;
    const auto before = zmalloc_used_memory();
    const bool loaded = rdb_load_stream_into(
        [&](void* p, std::size_t n) -> ssize_t {
          input.read(static_cast<char*>(p), n);
          const auto got = static_cast<std::size_t>(input.gcount());
          const auto extra = std::min(n - got, tail.size() - offset);
          std::memcpy(static_cast<char*>(p) + got, tail.data() + offset, extra);
          offset += extra;
          return got + extra;
        },
        frame(eof, size), target, result, err);
    const auto extra = peak > before ? peak - before : 0;
    check(loaded && err.empty() && extra < 4 * 1024 * 1024,
          "stream input memory bounded by one value, not 64 MiB RDB");
    check(stats.loaded == 0 && stats.expired == 256 && expired == 256 && staging.current_db() == 13,
          "stream memory fixture complete counts/context");
    check(result.rdb_bytes == size && result.read_ahead + tail.substr(offset) == suffix,
          "large stream exact suffix/position");
    std::cout << "64 MiB stream eof=" << eof << ": extra live allocation " << extra << '\n';
  }
  std::filesystem::remove_all(dir);
}

}  // namespace
int main() {
  peadb::lua_engine_init();
  boundaries();
  failures_and_controls();
  checksum_policy();
  bounded_memory();
  peadb::lua_engine_shutdown();
  return failures ? 1 : 0;
}
