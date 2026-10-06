// Redis's CRC-64, the checksum at the end of every RDB file, full sync and DUMP payload. A wrong
// table here makes PeaDB and Redis refuse each other's data (compat/delta.md item 27), so the
// known answers below are the ones Redis's own self-test prints (crc64.c crc64Test).

#include "rdb.hpp"

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

namespace {
int failures = 0;
void check(bool ok, const std::string& what) {
  if (!ok) {
    std::cerr << "FAIL: " << what << "\n";
    ++failures;
  }
}

// Redis's _crc64(): the bit-by-bit pycrc form (polynomial as written, MSB-first shifts, bits of
// each byte taken LSB first, result bit-reflected). Independent of the table code under test.
std::uint64_t reference_crc64(std::uint64_t crc, const unsigned char* data, std::size_t len) {
  const std::uint64_t poly = 0xad93d23594c935a9ULL;
  // _crc64 works on the unreflected register; reflect the incoming value to continue a chain.
  auto reflect = [](std::uint64_t v) {
    std::uint64_t r = 0;
    for (int i = 0; i < 64; ++i) r |= ((v >> i) & 1ULL) << (63 - i);
    return r;
  };
  crc = reflect(crc);
  for (std::size_t off = 0; off < len; ++off) {
    const unsigned char c = data[off];
    for (unsigned i = 0x01; i & 0xff; i <<= 1) {
      bool bit = (crc & 0x8000000000000000ULL) != 0;
      if (c & i) bit = !bit;
      crc <<= 1;
      if (bit) crc ^= poly;
    }
  }
  return reflect(crc);
}

std::string hex(std::uint64_t v) {
  char buf[17];
  std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(v));
  return buf;
}
}  // namespace

int main() {
  const std::string check_string = "123456789";
  check(peadb::crc64(0, check_string.data(), check_string.size()) == 0xe9c6d914c4b8d9caULL,
        "crc64(\"123456789\") is Redis's check value e9c6d914c4b8d9ca, got " +
            hex(peadb::crc64(0, check_string.data(), check_string.size())));

  // Redis's second self-test vector: sizeof(li) in C counts the trailing NUL.
  const std::string li =
      "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed "
      "do eiusmod tempor incididunt ut labore et dolore magna "
      "aliqua. Ut enim ad minim veniam, quis nostrud exercitation "
      "ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis "
      "aute irure dolor in reprehenderit in voluptate velit esse "
      "cillum dolore eu fugiat nulla pariatur. Excepteur sint "
      "occaecat cupidatat non proident, sunt in culpa qui officia "
      "deserunt mollit anim id est laborum.";
  check(peadb::crc64(0, li.c_str(), li.size() + 1) == 0xc7794709e69683b3ULL,
        "lorem ipsum vector c7794709e69683b3, got " + hex(peadb::crc64(0, li.c_str(), li.size() + 1)));

  check(peadb::crc64(0, nullptr, 0) == 0, "empty input leaves 0");
  check(peadb::crc64(0x1234, "", 0) == 0x1234, "empty input leaves the running value");

  // The 8-bytes-per-step loop against the bit-by-bit reference, for every length up to 300 and
  // every start offset modulo 8 (unaligned loads).
  std::vector<unsigned char> buf(320);
  std::uint32_t x = 2463534242u;  // xorshift32, fixed seed
  for (auto& b : buf) {
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    b = static_cast<unsigned char>(x);
  }
  for (std::size_t off = 0; off < 8; ++off) {
    for (std::size_t len = 0; len <= 300; ++len) {
      const auto got = peadb::crc64(0, buf.data() + off, len);
      const auto want = reference_crc64(0, buf.data() + off, len);
      if (got != want) {
        check(false, "offset " + std::to_string(off) + " length " + std::to_string(len) + ": " +
                         hex(got) + " != " + hex(want));
      }
    }
  }

  // Chaining: crc64(crc64(0, a), b) == crc64(0, a + b), for every split point.
  const auto whole = peadb::crc64(0, buf.data(), 300);
  for (std::size_t split = 0; split <= 300; ++split) {
    const auto first = peadb::crc64(0, buf.data(), split);
    if (peadb::crc64(first, buf.data() + split, 300 - split) != whole) {
      check(false, "chained at split " + std::to_string(split));
    }
  }
  check(reference_crc64(0, reinterpret_cast<const unsigned char*>(check_string.data()), 9) ==
            0xe9c6d914c4b8d9caULL,
        "reference implementation agrees with Redis's check value");

  if (failures == 0) std::cout << "rdb_crc64_test: all checks passed\n";
  return failures == 0 ? 0 : 1;
}
