// Bounded snapshot output, incremental CRC and sink failure/state restoration contracts.
#include "rdb.hpp"
#include "zmalloc.hpp"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unistd.h>

namespace {
int failures = 0;
void check(bool ok, const char* what) {
  if (!ok) {
    ++failures;
    std::cerr << "FAIL: " << what << '\n';
  }
}
std::uint64_t footer(const std::string& s) {
  std::uint64_t result = 0;
  for (unsigned i = 0; i < 8; ++i) {
    result |= static_cast<std::uint64_t>(static_cast<unsigned char>(s[s.size() - 8 + i]))
              << (8 * i);
  }
  return result;
}
}  // namespace

int main() {
  auto& ds = peadb::store();
  ds.flushall();
  ds.select_db(5);
  const std::string value(256 * 1024, 'x');
  for (int i = 0; i < 256; ++i) {
    ds.set("bulk:" + std::to_string(i), value, false, false, std::nullopt, false);
  }
  ds.select_db(15);
  ds.set("integer", "-2147483648", false, false, 9007199254740993LL, false);
  std::string err;
  peadb::RdbReplicationInfo repl{5, std::string(40, 'a'), 123};

  // Sample before constructing diagnostics; the discard sink allocates nothing. 64 MiB of
  // serialized values must not imply 64 MiB of extra live allocation. Allow one value plus
  // allocator rounding and the 64 KiB I/O buffer, with generous portable headroom.
  std::size_t peak = 0, largest = 0, total = 0;
  const std::size_t before = peadb::zmalloc_used_memory();
  const bool saved = peadb::rdb_write(
      [&](const void*, std::size_t n) -> ssize_t {
        peak = std::max(peak, peadb::zmalloc_used_memory());
        largest = std::max(largest, n);
        total += n;
        return n;
      },
      err, &repl);
  const std::size_t extra = peak > before ? peak - before : 0;
  check(saved && err.empty(), "discard sink succeeds");
  check(total > 64 * 1024 * 1024 && largest <= 64 * 1024, "large output uses bounded sink calls");
  check(extra < 4 * 1024 * 1024, "snapshot memory is bounded by a value, not the full output");
  check(ds.current_db() == 15, "success restores selected database");
  std::cout << "64 MiB snapshot: extra live allocation " << extra << ", max write " << largest
            << '\n';

  // Short writes and EINTR must not omit, duplicate or checksum a byte twice.
  ds.select_db(5);
  ds.flushdb();
  ds.set(std::string("binary\0key", 10), std::string(150000, '\x80'), false, false, std::nullopt,
         false);
  ds.select_db(9);
  std::string data;
  int calls = 0;
  check(peadb::rdb_write(
            [&](const void* bytes, std::size_t n) -> ssize_t {
              if (++calls % 7 == 1) {
                errno = EINTR;
                return -1;
              }
              n = std::min<std::size_t>(n, 997);
              data.append(static_cast<const char*>(bytes), n);
              return n;
            },
            err, &repl),
        "short writes and EINTR succeed");
  check(data.starts_with("REDIS0011") && data[data.size() - 9] == '\xff', "RDB framing intact");
  check(footer(data) == peadb::crc64(0, data.data(), data.size() - 8),
        "incremental CRC covers exact body");
  char scratch[] = "/tmp/peadb-rdb-writer-XXXXXX";
  const char* dir = mkdtemp(scratch);
  if (!dir) return 1;
  const auto path = std::filesystem::path(dir) / "dump.rdb";
  {
    std::ofstream file(path, std::ios::binary);
    file.write(data.data(), data.size());
  }
  peadb::RdbReplicationInfo loaded;
  check(peadb::rdb_load(path, err, nullptr, &loaded) && loaded.valid() && loaded.offset == 123,
        "fragmented output reloads with history");
  ds.select_db(15);
  check(ds.get("integer") == "-2147483648" && ds.pexpiretime("integer") == 9007199254740993LL,
        "integer encoding and exact expiry preserved");
  ds.select_db(5);
  check(ds.get(std::string("binary\0key", 10)) == std::string(150000, '\x80'),
        "binary value preserved");

  for (int mode = 0; mode < 5; ++mode) {
    ds.select_db(9);
    int invoked = 0;
    check(!peadb::rdb_write(
              [&](const void*, std::size_t n) -> ssize_t {
                ++invoked;
                if (mode == 0) {
                  errno = ENOSPC;
                  return -1;
                }
                if (mode == 1) return 0;
                if (mode == 2) return n + 1;  // contract violation must fail, not overrun
                if (mode == 3) throw std::bad_alloc();
                throw std::runtime_error("sink failure");
              },
              err),
          "failed sink aborts");
    check(invoked == 1 && !err.empty(), "no writes after sink failure");
    check(ds.current_db() == 9, "failure restores selected database");
  }
  int attempts = 0;
  ds.select_db(9);
  check(!peadb::rdb_write(
            [&](const void*, std::size_t n) -> ssize_t {
              if (++attempts <= 2) return std::min<std::size_t>(n, 4096);
              errno = ENOSPC;
              return -1;
            },
            err),
        "error after positive partial writes fails");
  check(attempts == 3 && ds.current_db() == 9, "partial failure stops and restores database");

  // Put the integer marker at the end of the first buffer. An integer parser catch-all
  // must never swallow a sink exception and try to serialize the raw string instead.
  ds.flushall();
  ds.select_db(5);
  ds.set("x", "32767", false, false, std::nullopt, false);
  std::string small;
  check(peadb::rdb_write(
            [&](const void* p, std::size_t n) -> ssize_t {
              small.append(static_cast<const char*>(p), n);
              return n;
            },
            err),
        "integer layout probe succeeds");
  const std::size_t marker = small.size() - 8 - 1 - 3;
  ds.flushall();
  ds.select_db(5);
  // Changing a one-byte key to a >16383-byte key adds four length-header bytes.
  ds.set(std::string(65535 - marker + 1 - 4, 'k'), "32767", false, false, std::nullopt, false);
  ds.select_db(9);
  attempts = 0;
  check(!peadb::rdb_write(
            [&](const void* p, std::size_t n) -> ssize_t {
              ++attempts;
              check(n == 65536 && static_cast<const unsigned char*>(p)[n - 1] == 0xc1,
                    "failure really occurs at an integer marker boundary");
              errno = EIO;
              return -1;
            },
            err),
        "integer sink error propagates");
  check(attempts == 1 && ds.current_db() == 9, "integer failure is not swallowed or retried");

  check(!peadb::rdb_write({}, err) && !err.empty(), "missing sink refused");
  // A failed save must not leave lazy expiry disabled.
  ds.set("past", "v", false, false, 1, false);
  check(!ds.get("past"), "expiry policy restored after failure");
  data.clear();
  peadb::rdb_set_checksum(false);
  check(peadb::rdb_write(
            [&](const void* p, std::size_t n) -> ssize_t {
              data.append(static_cast<const char*>(p), n);
              return n;
            },
            err),
        "checksum-disabled output succeeds");
  check(footer(data) == 0, "checksum-disabled footer is zero");
  peadb::rdb_set_checksum(true);
  ds.flushall();
  std::filesystem::remove_all(dir);
  return failures ? 1 : 0;
}
