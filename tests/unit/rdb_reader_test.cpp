// Bounded snapshot input: file size must not add another serialized dataset to memory.
#include "rdb.hpp"
#include "rdb_reader.hpp"
#include "zmalloc.hpp"

#include <algorithm>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
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

void primitives() {
  using peadb::detail::RdbReader;
  // Every integer width/endian and length encoding, followed by strings large enough to
  // cross both a refill and a short-read boundary. Embedded NULs must remain ordinary bytes.
  std::string data("\x12\x34\x12\x78\x56\x34\x12\x12\x34\x56\x78", 11);
  data += std::string("\x08\x07\x06\x05\x04\x03\x02\x01", 8);
  data += std::string("\x01\x02\x03\x04\x05\x06\x07\x08", 8);
  data += std::string("\x3f\x7f\xff\x80\x12\x34\x56\x78", 8);
  data += std::string("\x81\x01\x02\x03\x04\x05\x06\x07\x08", 9);
  data += std::string("\xc0\x80\xc1\x00\x80\xc2\x00\x00\x00\x80", 10);
  data += std::string("\xc3\x04\x03\x02", 4) + "abc";  // literal-only LZF
  data += std::string("\x80\x00\x02\x49\xf0", 5);      // 150000-byte raw string
  std::string large(150000, '\x80');
  large[65535] = '\0';
  data += large;
  data += std::string("\xff\xfe\xfd\x03", 4) + "1.5";  // legacy doubles
  const std::string framed = data + "NEXT-FRAME";

  for (const std::size_t fragment : {std::size_t(1), std::size_t(997), std::size_t(65536)}) {
    std::size_t offset = 0, largest = 0;
    unsigned calls = 0;
    RdbReader r(
        [&](void* p, std::size_t n) -> ssize_t {
          largest = std::max(largest, n);
          if (++calls % 7 == 1) {
            errno = EINTR;
            return -1;
          }
          n = std::min({n, fragment, framed.size() - offset});
          std::memcpy(p, framed.data() + offset, n);
          offset += n;
          return n;
        },
        data.size(), true);
    uint8_t u8;
    uint16_t u16;
    uint32_t u32;
    uint64_t u64;
    bool enc;
    check(r.read_u8(u8) && u8 == 0x12, "u8");
    check(r.read_u16_le(u16) && u16 == 0x1234, "u16 LE");
    check(r.read_u32_le(u32) && u32 == 0x12345678, "u32 LE");
    check(r.read_u32_be(u32) && u32 == 0x12345678, "u32 BE");
    check(r.read_u64_le(u64) && u64 == 0x0102030405060708, "u64 LE");
    check(r.read_u64_be(u64) && u64 == 0x0102030405060708, "u64 BE");
    check(r.read_length(u64, enc) && !enc && u64 == 63, "six-bit length");
    check(r.read_length(u64, enc) && !enc && u64 == 16383, "14-bit length");
    check(r.read_length(u64, enc) && !enc && u64 == 0x12345678, "32-bit length");
    check(r.read_length(u64, enc) && !enc && u64 == 0x0102030405060708, "64-bit length");
    std::string value;
    check(r.read_string(value) && value == "-128", "int8 string");
    check(r.read_string(value) && value == "-32768", "int16 string");
    check(r.read_string(value) && value == "-2147483648", "int32 string");
    check(r.read_string(value) && value == "abc", "LZF string");
    check(r.read_string(value) && value == large, "fragmented binary large string");
    double score;
    check(r.read_double_old(score) && std::isinf(score) && score < 0, "negative infinity");
    check(r.read_double_old(score) && std::isinf(score) && score > 0, "positive infinity");
    check(r.read_double_old(score) && std::isnan(score), "NaN primitive");
    check(r.read_double_old(score) && score == 1.5, "legacy score");
    check(r.eof() && r.pos() == data.size() && r.remaining() == 0, "exact consumed length");
    check(offset == data.size() && largest <= 65536, "bounded read-ahead preserves next frame");
    check(r.checksum() == peadb::crc64(0, data.data(), data.size()), "short reads CRC once");
    check(!r.read_u8(u8), "read cannot cross declared boundary");
  }

  std::string borrowed = "abc";
  RdbReader memory(borrowed);
  borrowed[0] = 'z';
  std::string value;
  check(memory.read_bytes(value, 3) && value == "zbc", "memory form borrows without copying");

  for (int mode = 0; mode < 4; ++mode) {
    unsigned calls = 0;
    RdbReader r(
        [&](void*, std::size_t n) -> ssize_t {
          ++calls;
          if (mode == 0) return 0;
          if (mode == 1) {
            errno = EIO;
            return -1;
          }
          if (mode == 2) return n + 1;
          throw std::runtime_error("source failure");
        },
        100);
    uint8_t u8;
    if (mode == 3) {
      bool threw = false;
      try {
        r.read_u8(u8);
      } catch (const std::runtime_error&) {
        threw = true;
      }
      check(threw, "source exception propagates");
    } else {
      check(!r.read_u8(u8) && !r.read_u8(u8) && calls == 1, "source failure stays failed");
      check(r.error() == (mode == 0 ? 0 : EIO), "source errno preserved");
    }
  }

  // A partial primitive is consumed once, never retried from its beginning. The caller aborts
  // this decoder on failure; a truncated source cannot be mistaken for the declared EOF.
  unsigned calls = 0;
  RdbReader truncated(
      [&](void* p, std::size_t) -> ssize_t {
        if (++calls > 1) return 0;
        std::memcpy(p, "abc", 3);
        return 3;
      },
      8, true);
  uint64_t number;
  check(!truncated.read_u64_le(number) && truncated.pos() == 3 && !truncated.eof(),
        "truncated primitive aborts with exact position");
  check(truncated.checksum() == peadb::crc64(0, "abc", 3), "partial CRC covers consumed bytes");

  const std::string bad_length("\x81\xff\xff\xff\xff\xff\xff\xff\xff", 9);
  RdbReader oversized(bad_length);
  check(!oversized.read_string(value), "length beyond available input refused before allocation");
  const std::string bad_lzf("\xc3\x01\x03\xff", 4);
  RdbReader compressed(bad_lzf);
  check(!compressed.read_string(value), "invalid LZF refused");

  std::size_t skipped = 0;
  RdbReader skip(
      [&](void* p, std::size_t n) -> ssize_t {
        std::memset(p, 'x', n);
        skipped += n;
        return n;
      },
      200000, true);
  check(skip.skip(200000) && skip.eof() && skipped == 200000, "bounded skip consumes all input");
  check(skip.checksum() == peadb::crc64(0, std::string(200000, 'x').data(), 200000), "skip CRC");
}
}  // namespace

int main() {
  primitives();
  char scratch[] = "/tmp/peadb-rdb-reader-XXXXXX";
  const char* dir = mkdtemp(scratch);
  if (!dir) return 1;
  const auto path = std::filesystem::path(dir) / "dump.rdb";
  auto& ds = peadb::store();
  ds.flushall();
  ds.select_db(5);
  {
    const peadb::DataStore::LazyExpireDisabled no_expiry(ds);
    const std::string value(256 * 1024, 'x');
    for (int i = 0; i < 256; ++i)
      ds.set("expired:" + std::to_string(i), value, false, false, 1, false);
  }
  std::size_t peak = 0;
  peadb::rdb_register_function_hooks([](const auto& visit) { visit("memory sampling marker"); },
                                     [&](const std::string&, std::string&) {
                                       peak = std::max(peak, peadb::zmalloc_used_memory());
                                       return true;
                                     });
  std::string err;
  check(peadb::rdb_save(path, err), "large fixture saves");
  check(std::filesystem::file_size(path) > 64 * 1024 * 1024, "fixture exceeds 64 MiB");
  ds.flushall();
  ds.select_db(9);
  // Expired values are decoded and discarded one at a time. Sample inside callbacks, before
  // constructing diagnostics, so serialized-file allocation cannot hide behind the live data.
  const std::size_t before = peadb::zmalloc_used_memory();
  const bool loaded = peadb::rdb_load(path, err, nullptr, nullptr, [&](int db, const auto&) {
    peak = std::max(peak, peadb::zmalloc_used_memory());
    check(db == 5, "expiry callback uses snapshot database");
  });
  const std::size_t extra = peak > before ? peak - before : 0;
  check(loaded && err.empty(), "large file loads");
  check(extra < 4 * 1024 * 1024, "input memory is bounded by a value, not the full file");
  check(peadb::rdb_last_load_stats().expired == 256 && peadb::rdb_last_load_stats().loaded == 0,
        "expired values decoded and counted");
  check(ds.current_db() == 9, "load restores selected database");
  std::cout << "64 MiB input: extra live allocation " << extra << '\n';
  peadb::rdb_register_function_hooks(nullptr, nullptr);
  ds.flushall();

  auto write = [&](const std::string& data) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(data.data(), data.size());
  };
  const std::string empty = std::string("REDIS0011\xff") + std::string(8, '\0');
  unsigned replaced = 0;
  const auto replace = [&] {
    ++replaced;
  };
  ds.set("retained", "old", false, false, std::nullopt, false);
  for (std::size_t size = 0; size < empty.size(); ++size) {
    write(empty.substr(0, size));
    check(!peadb::rdb_load(path, err, replace), "short header/body/footer refused");
    check(replaced == 0 && ds.get("retained") == "old", "failed preflight preserves live data");
  }
  std::string bad_crc = empty;
  bad_crc.back() = 1;
  write(bad_crc);
  check(!peadb::rdb_load(path, err, replace) && replaced == 0 &&
            err.find("Wrong RDB checksum") != std::string::npos,
        "checksum checked before replacement");
  write(std::string("REDIS0011\xfe\x10\xff") + std::string(8, '\0'));
  check(!peadb::rdb_load(path, err, replace) && replaced == 0 && ds.get("retained") == "old",
        "out-of-range database preflight preserves data");
  peadb::rdb_set_checksum(false);
  write(empty.substr(0, empty.size() - 1));
  check(!peadb::rdb_load(path, err, replace) && replaced == 0,
        "checksum-disabled loader still requires entire footer");
  peadb::rdb_set_checksum(true);

  // Full-sync publication renames the spool file before applying. Retain the verified inode
  // across that callback even if the former pathname is reused for an unrelated file.
  write(empty);
  check(peadb::rdb_load(path, err,
                        [&] {
                          ++replaced;
                          std::filesystem::rename(path,
                                                  std::filesystem::path(dir) / "published.rdb");
                          write("unrelated pathname replacement");
                        }),
        "publication callback cannot change the opened input inode");
  check(replaced == 1 && !ds.get("retained"), "validated snapshot applied after rename");

  // An apply-time callback exception must release the fd and restore DB/expiry scope. This
  // remains an in-place load: partial state is not claimed to be a transactional target.
  write(std::string("REDIS0011\xfe\x05\xf5\x01x\xff") + std::string(8, '\0'));
  peadb::rdb_register_function_hooks(
      nullptr, [](const auto&, auto&) -> bool { throw std::runtime_error("function failure"); });
  bool threw = false;
  try {
    peadb::rdb_load(path, err);
  } catch (const std::runtime_error&) {
    threw = true;
  }
  check(threw && ds.current_db() == 9, "exception restores database selection");
  ds.set("past", "v", false, false, 1, false);
  check(!ds.get("past"), "exception restores lazy expiry");
  peadb::rdb_register_function_hooks(nullptr, nullptr);
  ds.flushall();
  std::filesystem::remove_all(dir);
  return failures ? 1 : 0;
}
