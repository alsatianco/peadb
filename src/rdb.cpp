// rdb.cpp — Redis-compatible RDB binary format reader/writer
//
// Implements the subset of the RDB format needed for PeaDB's data types:
//   - Strings (RDB_TYPE_STRING = 0)
//   - Lists (RDB_TYPE_LIST_QUICKLIST_2 = 18, RDB_TYPE_LIST = 1)
//   - Sets (RDB_TYPE_SET = 2, RDB_TYPE_SET_INTSET = 11)
//   - Hashes (RDB_TYPE_HASH = 4, RDB_TYPE_HASH_LISTPACK = 16)
//   - Sorted sets (RDB_TYPE_ZSET_2 = 5, RDB_TYPE_ZSET_LISTPACK = 17)
//   - Streams (Redis listpack types 15/19/21; writes type 21, reads old private type 200)
// Reads pre-7.0 zipmap/ziplist/quicklist-of-ziplists encodings (types 9/10/12/13/14).
//
// References: Redis src/rdb.h, src/rdb.c

#include "rdb.hpp"
#include "rdb_reader.hpp"
#include "datastore.hpp"
#include "logger.hpp"

#include <charconv>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <cstdio>
#include <cstring>
#include <exception>
#include <sstream>
#include <system_error>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>
#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace peadb {

namespace {
bool g_skip_checksum_validation = false;  // DEBUG SET-SKIP-CHECKSUM-VALIDATION
bool g_rdb_checksum = true;               // rdbchecksum (startup-only, as in Redis)
int g_rdb_key_save_delay = 0;             // rdb-key-save-delay (hidden Redis setting, for tests)
FunctionDumpFn g_function_dump_hook;      // visits every function library's source
FunctionLoadFn g_function_load_hook;      // loads one library's source (idempotent REPLACE)
RdbLoadStats g_rdb_load_stats;
}  // namespace

bool RdbReplicationInfo::valid() const {
  return stream_db >= 0 && static_cast<std::size_t>(stream_db) < store().database_count() &&
         offset >= 0 && offset < std::numeric_limits<std::int64_t>::max() && replid.size() == 40 &&
         std::all_of(replid.begin(), replid.end(), [](unsigned char c) {
           return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
         });
}

const RdbLoadStats& rdb_last_load_stats() {
  return g_rdb_load_stats;
}

void rdb_set_skip_checksum_validation(bool skip) {
  g_skip_checksum_validation = skip;
}
void rdb_set_checksum(bool enabled) {
  g_rdb_checksum = enabled;
}
bool rdb_checksum() {
  return g_rdb_checksum;
}
void rdb_set_key_save_delay(int usec) {
  g_rdb_key_save_delay = usec;
}
int rdb_key_save_delay() {
  return g_rdb_key_save_delay;
}

void debug_delay(int usec) {
  // Redis's debugDelay(): a negative value sleeps 1 microsecond once in every -usec calls.
  if (usec < 0) usec = (std::rand() % -usec) == 0 ? 1 : 0;
  if (usec <= 0) return;
  timespec left{usec / 1000000, static_cast<long>(usec % 1000000) * 1000};
  while (::nanosleep(&left, &left) != 0 && errno == EINTR) {
  }
}

std::string rdb_temp_file_name(const std::filesystem::path& path, pid_t pid) {
  return (path.parent_path() / ("temp-" + std::to_string(pid) + ".rdb")).string();
}

void rdb_register_function_hooks(FunctionDumpFn dump, FunctionLoadFn load) {
  g_function_dump_hook = std::move(dump);
  g_function_load_hook = std::move(load);
}

namespace {

// fsync the parent directory of `path` so an atomic rename is durable across
// power loss. File fsync alone only flushes contents, not the directory entry.
bool fsync_parent_dir(const std::filesystem::path& path) {
  std::filesystem::path parent = path.parent_path();
  if (parent.empty()) parent = ".";
  const int dfd = ::open(parent.c_str(), O_RDONLY | O_CLOEXEC);
  if (dfd < 0) return false;
  const int rc = ::fsync(dfd);
  ::close(dfd);
  return rc == 0;
}

// ── RDB constants ──────────────────────────────────────────────────────

constexpr uint8_t RDB_OPCODE_EXPIRETIME_MS = 252;
constexpr uint8_t RDB_OPCODE_EXPIRETIME = 253;
constexpr uint8_t RDB_OPCODE_SELECTDB = 254;
constexpr uint8_t RDB_OPCODE_EOF = 255;
constexpr uint8_t RDB_OPCODE_RESIZEDB = 251;
constexpr uint8_t RDB_OPCODE_AUX = 250;
constexpr uint8_t RDB_OPCODE_FUNCTION2 = 245;        // one string payload = one library's source
constexpr uint8_t RDB_OPCODE_FUNCTION_PRE_GA = 246;  // Redis 7.0 release-candidate format
constexpr uint8_t RDB_OPCODE_IDLE = 248;             // LRU idle seconds of the next key
constexpr uint8_t RDB_OPCODE_FREQ = 249;             // LFU counter of the next key

constexpr uint8_t RDB_TYPE_STRING = 0;
constexpr uint8_t RDB_TYPE_LIST = 1;
constexpr uint8_t RDB_TYPE_SET = 2;
constexpr uint8_t RDB_TYPE_ZSET = 3;
constexpr uint8_t RDB_TYPE_HASH = 4;
constexpr uint8_t RDB_TYPE_ZSET_2 = 5;
constexpr uint8_t RDB_TYPE_HASH_ZIPMAP = 9;
constexpr uint8_t RDB_TYPE_LIST_ZIPLIST = 10;
constexpr uint8_t RDB_TYPE_SET_INTSET = 11;
constexpr uint8_t RDB_TYPE_ZSET_ZIPLIST = 12;
constexpr uint8_t RDB_TYPE_HASH_ZIPLIST = 13;
constexpr uint8_t RDB_TYPE_LIST_QUICKLIST = 14;  // nodes are ziplists (Redis 3.2 to 6.2)
constexpr uint8_t RDB_TYPE_HASH_LISTPACK = 16;
constexpr uint8_t RDB_TYPE_ZSET_LISTPACK = 17;
constexpr uint8_t RDB_TYPE_LIST_QUICKLIST_2 = 18;
constexpr uint8_t RDB_TYPE_SET_LISTPACK = 20;
constexpr uint8_t RDB_TYPE_STREAM_LISTPACKS = 15;
constexpr uint8_t RDB_TYPE_STREAM_LISTPACKS_2 = 19;
constexpr uint8_t RDB_TYPE_STREAM_LISTPACKS_3 = 21;
// Legacy private stream snapshots remain readable for local recovery; all new writes use 21.
constexpr uint8_t RDB_TYPE_PEADB_STREAM = 200;

constexpr uint8_t RDB_ENC_INT8 = 0;
constexpr uint8_t RDB_ENC_INT16 = 1;
constexpr uint8_t RDB_ENC_INT32 = 2;
constexpr uint8_t RDB_ENC_LZF = 3;

constexpr int RDB_VERSION = 11;
// The newest RDB version Redis 7.2.5 reads (its rdb.h RDB_VERSION): a file or a DUMP payload with
// a newer one is refused, as Redis refuses it.
constexpr int RDB_VERSION_MAX_LOAD = 11;

// ── CRC64 ──────────────────────────────────────────────────────────────
// Redis's crc64.c: CRC-64 with the Jones polynomial 0xad93d23594c935a9, reflected in and out,
// initial value 0 and no final xor, so crc64(0, "123456789") == 0xe9c6d914c4b8d9ca. A reflected
// (right-shift) table needs the bit-reversed polynomial, 0x95ac9329ac4bc9b5; using the polynomial
// as written gave a different checksum, so neither engine could read the other's files (compat/
// delta.md item 27). Eight tables let the loop take 8 bytes per step, as Redis's crcspeed.c does.

constexpr uint64_t kCrc64ReflectedPoly = 0x95ac9329ac4bc9b5ULL;

struct Crc64Tables {
  uint64_t t[8][256];
  Crc64Tables() {
    for (int n = 0; n < 256; ++n) {
      uint64_t crc = static_cast<uint64_t>(n);
      for (int k = 0; k < 8; ++k) crc = (crc & 1) ? (crc >> 1) ^ kCrc64ReflectedPoly : crc >> 1;
      t[0][n] = crc;
    }
    // t[k][n] is the CRC of byte n followed by k zero bytes.
    for (int n = 0; n < 256; ++n) {
      for (int k = 1; k < 8; ++k) t[k][n] = t[0][t[k - 1][n] & 0xff] ^ (t[k - 1][n] >> 8);
    }
  }
};

const Crc64Tables& crc64_tables() {
  static const Crc64Tables tables;  // built once; C++ makes this initialization thread-safe
  return tables;
}
}  // namespace

uint64_t crc64(uint64_t crc, const void* data, size_t len) {
  const auto& t = crc64_tables().t;
  const auto* p = static_cast<const uint8_t*>(data);
  while (len >= 8) {
    uint64_t word;
    std::memcpy(&word, p, 8);
    if constexpr (std::endian::native == std::endian::big) word = __builtin_bswap64(word);
    crc ^= word;
    crc = t[7][crc & 0xff] ^ t[6][(crc >> 8) & 0xff] ^ t[5][(crc >> 16) & 0xff] ^
          t[4][(crc >> 24) & 0xff] ^ t[3][(crc >> 32) & 0xff] ^ t[2][(crc >> 40) & 0xff] ^
          t[1][(crc >> 48) & 0xff] ^ t[0][crc >> 56];
    p += 8;
    len -= 8;
  }
  while (len-- > 0) crc = t[0][(crc ^ *p++) & 0xff] ^ (crc >> 8);
  return crc;
}

namespace {

// ── Binary writer ──────────────────────────────────────────────────────

class RdbWriter {
 public:
  explicit RdbWriter(RdbWriteFn sink = {}, bool checksum = false)
      : sink_(std::move(sink)), checksum_enabled_(checksum) {
  }

  void write_u8(uint8_t v) {
    const char byte = static_cast<char>(v);
    write_raw(&byte, 1);
  }
  void write_u16_le(uint16_t v) {
    write_u8(v & 0xff);
    write_u8((v >> 8) & 0xff);
  }
  void write_u32_le(uint32_t v) {
    write_u8(v & 0xff);
    write_u8((v >> 8) & 0xff);
    write_u8((v >> 16) & 0xff);
    write_u8((v >> 24) & 0xff);
  }
  void write_u64_le(uint64_t v) {
    write_u32_le(static_cast<uint32_t>(v));
    write_u32_le(static_cast<uint32_t>(v >> 32));
  }
  void write_raw(const void* p, size_t n) {
    const auto* bytes = static_cast<const char*>(p);
    if (!sink_) {
      buf_.append(bytes, n);
      return;
    }
    while (n != 0) {
      const auto count = std::min(n, kBufferSize - buf_.size());
      buf_.append(bytes, count);
      bytes += count;
      n -= count;
      if (buf_.size() == kBufferSize) flush();
    }
  }
  void write_raw(const std::string& s) {
    write_raw(s.data(), s.size());
  }

  void flush() {
    if (!sink_) return;
    if (checksum_enabled_) checksum_ = crc64(checksum_, buf_.data(), buf_.size());
    std::size_t offset = 0;
    while (offset < buf_.size()) {
      const auto remaining = buf_.size() - offset;
      const auto written = sink_(buf_.data() + offset, remaining);
      if (written < 0 && errno == EINTR) continue;
      if (written <= 0 || static_cast<std::size_t>(written) > remaining) {
        throw std::system_error(written < 0 ? errno : EIO, std::generic_category());
      }
      offset += static_cast<std::size_t>(written);
    }
    buf_.clear();
  }

  uint64_t checksum() {
    flush();
    return checksum_;
  }

  // Redis "length" encoding
  void write_length(uint64_t len) {
    if (len < 64) {
      write_u8(static_cast<uint8_t>(len));  // 00xxxxxx
    } else if (len < 16384) {
      write_u8(static_cast<uint8_t>(0x40 | (len >> 8)));  // 01xxxxxx
      write_u8(static_cast<uint8_t>(len & 0xff));
    } else if (len <= 0xFFFFFFFF) {
      write_u8(0x80);  // 10000000
      uint32_t be = htonl(static_cast<uint32_t>(len));
      write_raw(&be, 4);
    } else {
      write_u8(0x81);  // 10000001
      uint64_t v = len;
      // network byte order (big-endian)
      for (int i = 7; i >= 0; --i) write_u8(static_cast<uint8_t>((v >> (i * 8)) & 0xff));
    }
  }

  // Redis string encoding: try integer encoding first, else raw
  void write_string(const std::string& s) {
    // Try integer encoding for small ints
    if (!s.empty() && s.size() <= 20) {
      std::int64_t val;
      const auto parsed = std::from_chars(s.data(), s.data() + s.size(), val);
      if (parsed.ec == std::errc{} && parsed.ptr == s.data() + s.size() &&
          std::to_string(val) == s) {
        if (val >= -128 && val <= 127) {
          write_u8(0xC0 | RDB_ENC_INT8);
          write_u8(static_cast<uint8_t>(static_cast<int8_t>(val)));
          return;
        }
        if (val >= -32768 && val <= 32767) {
          write_u8(0xC0 | RDB_ENC_INT16);
          write_u16_le(static_cast<uint16_t>(static_cast<int16_t>(val)));
          return;
        }
        if (val >= -2147483648LL && val <= 2147483647LL) {
          write_u8(0xC0 | RDB_ENC_INT32);
          write_u32_le(static_cast<uint32_t>(static_cast<int32_t>(val)));
          return;
        }
      }
    }
    // Raw string
    write_length(s.size());
    write_raw(s);
  }

  void write_double_string(double v) {
    // Redis ZSET_2 encodes doubles as binary IEEE 754 LE
    uint64_t bits;
    std::memcpy(&bits, &v, 8);
    write_u64_le(bits);
  }

  const std::string& data() const {
    return buf_;
  }
  void clear() {
    buf_.clear();
  }

 private:
  static constexpr std::size_t kBufferSize = 64 * 1024;
  RdbWriteFn sink_;
  bool checksum_enabled_ = false;
  uint64_t checksum_ = 0;
  std::string buf_;
};

// ── Binary reader ──────────────────────────────────────────────────────

using detail::RdbReader;

// ── Listpack encoder/decoder ───────────────────────────────────────────
// Redis 7 uses listpack for small hashes, zsets, etc.
// Listpack is: <total-bytes:u32> <num-entries:u16> [entry...] <end:0xFF>
// Each entry is: <encoding> <data> <backlen>
// For simplicity we encode all values as strings.

// Redis's LP_HDR_NUMELE_UNKNOWN: the entry count a listpack stores once it holds 65535 or more
// entries; the real count is then found by walking the entries.
constexpr uint16_t kListpackCountUnknown = 65535;
constexpr std::size_t kListpackHeaderAndEnd = 4 + 2 + 1;  // total bytes, entry count, 0xFF

// Bytes Redis's lpEncodeBacklen() uses for an entry whose encoding header + data is `l` bytes.
// The limits are Redis's (16383, not 16384, and so on): a different byte count would put the
// next entry at another offset than Redis reads it from.
std::size_t listpack_backlen_bytes(std::size_t l) {
  if (l <= 127) return 1;
  if (l < 16383) return 2;
  if (l < 2097151) return 3;
  if (l < 268435455) return 4;
  return 5;
}

// Encoding header bytes of a string entry of `n` bytes (lpEncodeString()).
std::size_t listpack_string_header_bytes(std::size_t n) {
  return n < 64 ? 1 : n < 4096 ? 2 : 5;
}

class ListpackWriter {
 public:
  // Bytes add_string(s) adds for a string of `n` bytes.
  static std::size_t string_entry_bytes(std::size_t n) {
    const std::size_t l = listpack_string_header_bytes(n) + n;
    return l + listpack_backlen_bytes(l);
  }

  // Size of the listpack finish() returns (Redis's lpBytes()).
  std::size_t bytes() const {
    return kListpackHeaderAndEnd + entries_.size();
  }

  void add_string(const std::string& s) {
    // Real Redis listpack string encodings (listpack.c LP_ENCODING_*_STR): the header MUST
    // be self-describing without colliding with the 7-bit-uint / int encodings, so the reader
    // can round-trip any length AND a genuine Redis RDB is loadable.
    const std::size_t n = s.size();
    std::size_t hdr;
    if (n <= 63) {
      entries_.push_back(static_cast<char>(0x80 | n));  // 10xxxxxx : 6-bit length
      hdr = 1;
    } else if (n <= 4095) {
      entries_.push_back(static_cast<char>(0xE0 | ((n >> 8) & 0x0f)));  // 1110xxxx xxxxxxxx
      entries_.push_back(static_cast<char>(n & 0xff));
      hdr = 2;
    } else {
      entries_.push_back(static_cast<char>(0xF0));  // 11110000 : 32-bit little-endian length
      entries_.push_back(static_cast<char>(n & 0xff));
      entries_.push_back(static_cast<char>((n >> 8) & 0xff));
      entries_.push_back(static_cast<char>((n >> 16) & 0xff));
      entries_.push_back(static_cast<char>((n >> 24) & 0xff));
      hdr = 5;
    }
    entries_.append(s);
    encode_backlen(static_cast<uint32_t>(hdr + n));
    ++count_;
  }

  void add_integer(int64_t v) {
    // Stream listpacks require actual integer entries (lpGetInteger), not numeric strings.
    std::size_t n;
    if (v >= 0 && v <= 127) {
      entries_.push_back(static_cast<char>(v));
      n = 1;
    } else if (v >= -4096 && v <= 4095) {
      const auto u = static_cast<uint64_t>(v) & 8191;
      entries_.push_back(static_cast<char>(0xC0 | (u >> 8)));
      entries_.push_back(static_cast<char>(u));
      n = 2;
    } else {
      entries_.push_back(static_cast<char>(0xF4));
      const auto u = static_cast<uint64_t>(v);
      for (int i = 0; i < 8; ++i) entries_.push_back(static_cast<char>(u >> (8 * i)));
      n = 9;
    }
    encode_backlen(n);
    ++count_;
  }

  // The caller keeps the listpack under 4 GB (the total-bytes field is 32 bits): the list writer
  // puts an element of 1 GB or more in a node of its own that is not a listpack, as Redis does.
  std::string finish() const {
    // total_bytes(4) + num_entries(2) + entries + 0xFF
    const auto total = static_cast<uint32_t>(bytes());
    const uint16_t count =
        count_ < kListpackCountUnknown ? static_cast<uint16_t>(count_) : kListpackCountUnknown;
    std::string out;
    out.reserve(total);
    // Little-endian u32 total bytes
    out.push_back(static_cast<char>(total & 0xff));
    out.push_back(static_cast<char>((total >> 8) & 0xff));
    out.push_back(static_cast<char>((total >> 16) & 0xff));
    out.push_back(static_cast<char>((total >> 24) & 0xff));
    // Little-endian u16 num entries
    out.push_back(static_cast<char>(count & 0xff));
    out.push_back(static_cast<char>((count >> 8) & 0xff));
    out += entries_;
    out.push_back(static_cast<char>(0xFF));
    return out;
  }

 private:
  // Redis lpEncodeBacklen: encodes the entry length (encoding header + data) so it can be read
  // backwards. The first byte (forward) carries the high bits with NO continuation flag; every
  // following byte sets 0x80. The byte COUNT is listpack_backlen_bytes(l).
  void encode_backlen(uint32_t l) {
    const std::size_t nbytes = listpack_backlen_bytes(l);
    if (nbytes == 1) {
      entries_.push_back(static_cast<char>(l));
    } else if (nbytes == 2) {
      entries_.push_back(static_cast<char>(l >> 7));
      entries_.push_back(static_cast<char>((l & 127) | 128));
    } else if (nbytes == 3) {
      entries_.push_back(static_cast<char>(l >> 14));
      entries_.push_back(static_cast<char>(((l >> 7) & 127) | 128));
      entries_.push_back(static_cast<char>((l & 127) | 128));
    } else if (nbytes == 4) {
      entries_.push_back(static_cast<char>(l >> 21));
      entries_.push_back(static_cast<char>(((l >> 14) & 127) | 128));
      entries_.push_back(static_cast<char>(((l >> 7) & 127) | 128));
      entries_.push_back(static_cast<char>((l & 127) | 128));
    } else {
      entries_.push_back(static_cast<char>(l >> 28));
      entries_.push_back(static_cast<char>(((l >> 21) & 127) | 128));
      entries_.push_back(static_cast<char>(((l >> 14) & 127) | 128));
      entries_.push_back(static_cast<char>(((l >> 7) & 127) | 128));
      entries_.push_back(static_cast<char>((l & 127) | 128));
    }
  }

  std::string entries_;
  std::size_t count_ = 0;
};

// Simple listpack reader
class ListpackReader {
 public:
  ListpackReader(const std::string& data) : data_(data), pos_(0) {
  }

  // Reads every entry, with the checks of Redis's lpValidateIntegrity() in its deep mode: the
  // total-bytes field is the blob's size, every entry and its backlen lie inside the blob and the
  // backlen matches the entry, and the entries end at the 0xFF in the last byte. False means a
  // corrupt listpack. The entry count is not checked, as in Redis with sanitize-dump-payload no
  // (the default): the entries up to the 0xFF are what is read. So a list saved by PeaDB before
  // compat/delta.md item 29 was fixed, whose count was cut to 16 bits, loads with all its items.
  bool parse(std::vector<std::string>& out, std::vector<bool>* integers = nullptr) {
    out.clear();
    if (integers) integers->clear();
    if (data_.size() < kListpackHeaderAndEnd) return false;
    if (read_u32_le_at(0) != data_.size()) return false;
    if (static_cast<uint8_t>(data_.back()) != 0xFF) return false;
    const uint16_t num_entries = read_u16_le_at(4);
    if (num_entries != kListpackCountUnknown) out.reserve(num_entries);
    pos_ = 6;
    while (pos_ < data_.size() - 1) {
      const auto enc = static_cast<uint8_t>(data_[pos_]);
      if (integers)
        integers->push_back(enc < 0x80 || (enc >= 0xC0 && enc < 0xE0) ||
                            (enc >= 0xF1 && enc <= 0xF4));
      std::string val;
      if (!read_entry(val)) return false;
      out.push_back(std::move(val));
    }
    return pos_ == data_.size() - 1;
  }

 private:
  uint32_t read_u32_le_at(size_t offset) {
    return static_cast<uint32_t>(static_cast<uint8_t>(data_[offset])) |
           (static_cast<uint32_t>(static_cast<uint8_t>(data_[offset + 1])) << 8) |
           (static_cast<uint32_t>(static_cast<uint8_t>(data_[offset + 2])) << 16) |
           (static_cast<uint32_t>(static_cast<uint8_t>(data_[offset + 3])) << 24);
  }

  uint16_t read_u16_le_at(size_t offset) {
    return static_cast<uint16_t>(static_cast<uint8_t>(data_[offset])) |
           (static_cast<uint16_t>(static_cast<uint8_t>(data_[offset + 1])) << 8);
  }

  bool read_entry(std::string& val) {
    if (pos_ >= data_.size()) return false;
    uint8_t enc = static_cast<uint8_t>(data_[pos_]);
    if (enc == 0xFF) return false;  // end marker

    // Decode one listpack element per the real Redis encodings (listpack.c). entry_len is the
    // encoding header + data (excluding the trailing backlen), which then determines how many
    // backlen bytes to skip to reach the next element.
    size_t entry_len;
    auto need = [&](size_t nbytes) {
      return pos_ + nbytes <= data_.size();
    };

    if ((enc & 0x80) == 0) {  // 0xxxxxxx : 7-bit unsigned int
      val = std::to_string(enc & 0x7f);
      entry_len = 1;
    } else if ((enc & 0xC0) == 0x80) {  // 10xxxxxx : 6-bit string
      uint32_t slen = enc & 0x3f;
      if (!need(1 + slen)) return false;
      val.assign(data_, pos_ + 1, slen);
      entry_len = 1 + slen;
    } else if ((enc & 0xE0) == 0xC0) {  // 110xxxxx xxxxxxxx : 13-bit signed int
      if (!need(2)) return false;
      int16_t v = static_cast<int16_t>(((static_cast<uint16_t>(enc & 0x1f)) << 8) |
                                       static_cast<uint8_t>(data_[pos_ + 1]));
      if (v & 0x1000) v |= static_cast<int16_t>(0xE000);  // sign-extend from 13 bits
      val = std::to_string(v);
      entry_len = 2;
    } else if ((enc & 0xF0) == 0xE0) {  // 1110xxxx xxxxxxxx : 12-bit string
      if (!need(2)) return false;
      uint32_t slen =
          ((static_cast<uint32_t>(enc & 0x0f)) << 8) | static_cast<uint8_t>(data_[pos_ + 1]);
      if (!need(2 + slen)) return false;
      val.assign(data_, pos_ + 2, slen);
      entry_len = 2 + slen;
    } else if (enc == 0xF0) {  // 11110000 : 32-bit LE length string
      if (!need(5)) return false;
      uint32_t slen = static_cast<uint8_t>(data_[pos_ + 1]) |
                      (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 2])) << 8) |
                      (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 3])) << 16) |
                      (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 4])) << 24);
      if (!need(static_cast<size_t>(5) + slen)) return false;
      val.assign(data_, pos_ + 5, slen);
      entry_len = 5 + slen;
    } else if (enc == 0xF1) {  // 16-bit signed int (LE)
      if (!need(3)) return false;
      int16_t v =
          static_cast<int16_t>(static_cast<uint8_t>(data_[pos_ + 1]) |
                               (static_cast<uint16_t>(static_cast<uint8_t>(data_[pos_ + 2])) << 8));
      val = std::to_string(v);
      entry_len = 3;
    } else if (enc == 0xF2) {  // 24-bit signed int (LE)
      if (!need(4)) return false;
      int32_t v = static_cast<uint8_t>(data_[pos_ + 1]) |
                  (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 2])) << 8) |
                  (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 3])) << 16);
      if (v & 0x800000) v |= static_cast<int32_t>(0xFF000000u);  // sign-extend from 24 bits
      val = std::to_string(v);
      entry_len = 4;
    } else if (enc == 0xF3) {  // 32-bit signed int (LE)
      if (!need(5)) return false;
      int32_t v = static_cast<int32_t>(
          static_cast<uint8_t>(data_[pos_ + 1]) |
          (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 2])) << 8) |
          (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 3])) << 16) |
          (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 4])) << 24));
      val = std::to_string(v);
      entry_len = 5;
    } else if (enc == 0xF4) {  // 64-bit signed int (LE)
      if (!need(9)) return false;
      uint64_t u = 0;
      for (int i = 0; i < 8; ++i)
        u |= static_cast<uint64_t>(static_cast<uint8_t>(data_[pos_ + 1 + i])) << (8 * i);
      val = std::to_string(static_cast<int64_t>(u));
      entry_len = 9;
    } else {
      return false;  // unknown encoding
    }

    // Like lpValidateNext(): the entry and its backlen end before the 0xFF terminator, and the
    // backlen, read backwards from its last byte, is the entry's length.
    const std::size_t nbacklen = listpack_backlen_bytes(entry_len);
    if (pos_ + entry_len + nbacklen > data_.size() - 1) return false;
    pos_ += entry_len + nbacklen;
    std::uint64_t backlen = 0;
    for (std::size_t i = 0; i < nbacklen; ++i) {
      const auto b = static_cast<uint8_t>(data_[pos_ - 1 - i]);
      backlen |= static_cast<std::uint64_t>(b & 127) << (7 * i);
      if ((b & 128) == 0) {
        if (i + 1 != nbacklen) return false;  // the first byte of the backlen came early
        break;
      }
      if (i + 1 == nbacklen) return false;  // no first byte (one without the 128 flag)
    }
    return backlen == entry_len;
  }

  std::string data_;
  size_t pos_;
};

}  // anonymous namespace

// ── RDB Save ───────────────────────────────────────────────────────────

namespace {
// Pre-7.0 containers are converted on load, like Redis's rdbLoadObject(). Lengths are
// checked against the actual blob before reading or allocating; headers never size reserves.
class LegacyReader {
 public:
  explicit LegacyReader(std::string_view blob) : blob_(blob) {
  }
  bool number(std::size_t bytes, uint64_t& n, bool big_endian = false) {
    if (bytes > remaining()) return false;
    n = 0;
    for (std::size_t i = 0; i < bytes; ++i) {
      const auto shift = 8 * (big_endian ? bytes - i - 1 : i);
      n |= static_cast<uint64_t>(static_cast<uint8_t>(blob_[pos_++])) << shift;
    }
    return true;
  }
  bool text(std::size_t bytes, std::string& value) {
    if (bytes > remaining()) return false;
    value.assign(blob_.data() + pos_, bytes);
    pos_ += bytes;
    return true;
  }
  bool skip(std::size_t bytes) {
    if (bytes > remaining()) return false;
    pos_ += bytes;
    return true;
  }
  std::size_t position() const {
    return pos_;
  }
  std::size_t remaining() const {
    return blob_.size() - pos_;
  }

 private:
  std::string_view blob_;
  std::size_t pos_ = 0;
};

bool read_ziplist(const std::string& blob, std::vector<std::string>& items) {
  if (blob.size() < 11 || static_cast<uint8_t>(blob.back()) != 255) return false;
  // Exclude ZIP_END from the entry reader, so no entry can consume the terminator.
  LegacyReader r(std::string_view(blob).substr(0, blob.size() - 1));
  uint64_t bytes, tail, count;
  if (!r.number(4, bytes) || bytes != blob.size() || !r.number(4, tail) || tail > blob.size() - 1 ||
      !r.number(2, count))
    return false;
  std::size_t previous = 0, last = 10;
  while (r.remaining()) {
    const auto start = r.position();
    uint64_t prev, encoding;
    if (!r.number(1, prev) || prev == 255) return false;
    if (prev == 254 && !r.number(4, prev)) return false;
    if (prev != previous || !r.number(1, encoding)) return false;
    std::string item;
    if (encoding < 0xc0) {
      uint64_t len = encoding & 0x3f;
      if (encoding >= 0x80) {
        if (!r.number(4, len, true)) return false;
      } else if (encoding >= 0x40) {
        uint64_t low;
        if (!r.number(1, low)) return false;
        len = (len << 8) | low;
      }
      if (!r.text(len, item)) return false;
    } else if (encoding >= 0xf1 && encoding <= 0xfd) {
      item = std::to_string((encoding & 15) - 1);
    } else {
      const std::size_t width = encoding == 0xfe   ? 1
                                : encoding == 0xc0 ? 2
                                : encoding == 0xf0 ? 3
                                : encoding == 0xd0 ? 4
                                : encoding == 0xe0 ? 8
                                                   : 0;
      uint64_t bits;
      if (width == 0 || !r.number(width, bits)) return false;
      if (width < 8 && (bits & (uint64_t{1} << (width * 8 - 1))))
        bits |= (~uint64_t{0}) << (width * 8);
      item = std::to_string(std::bit_cast<int64_t>(bits));
    }
    items.push_back(std::move(item));
    previous = r.position() - start;
    last = start;
  }
  // Redis ignores the tail for an empty ziplist and treats UINT16_MAX as unknown count.
  return (items.empty() || tail == last) && (count == 65535 || count == items.size());
}

bool read_zipmap(const std::string& blob,
                 std::vector<std::pair<std::string, std::string>>& fields) {
  if (blob.size() < 2 || static_cast<uint8_t>(blob.back()) != 255) return false;
  LegacyReader r(std::string_view(blob).substr(0, blob.size() - 1));
  uint64_t count;
  if (!r.number(1, count)) return false;
  auto text = [&](std::string& value, bool has_free) {
    uint64_t len, free = 0;
    if (!r.number(1, len) || len == 255) return false;
    if (len == 254 && !r.number(4, len)) return false;
    return (!has_free || r.number(1, free)) && r.text(len, value) && r.skip(free);
  };
  while (r.remaining()) {
    std::string field, value;
    if (!text(field, false) || !text(value, true)) return false;
    fields.emplace_back(std::move(field), std::move(value));
  }
  return !fields.empty() && (count == 254 || count == fields.size());
}

// Redis's quicklist node containers (quicklist.h).
constexpr uint64_t kQuicklistNodePlain = 1;   // one element, stored as it is
constexpr uint64_t kQuicklistNodePacked = 2;  // a listpack of elements

// Writes a list as Redis 7.2.5 holds it: a quicklist whose nodes follow quicklistPushTail()
// (quicklist.c) for `fill` = list-max-listpack-size. An element of 1 GB or more (packed_threshold)
// is a PLAIN node of its own; otherwise it joins the last node while that node's listpack, plus
// the element and 8 bytes of SIZE_ESTIMATE_OVERHEAD, stays within the limit: 4/8/16/32/64 KB for
// a fill of -1..-5 (lower values count as -5), or for a fill of 0 or more, 8 KB
// (SIZE_SAFETY_LIMIT) and `fill` elements (0 counts as 1). A node therefore holds at most 32,761
// elements (empty strings, 2 bytes each, in 64 KB), so the listpack's 16-bit entry count cannot
// overflow; one listpack for the whole list cut lists of 65,536 or more elements to N mod 65,536
// (compat/delta.md item 29).
void write_quicklist(RdbWriter& w, const std::vector<std::string>& items, std::int64_t fill) {
  constexpr std::size_t kPackedThreshold = std::size_t{1} << 30;
  constexpr std::size_t kSizeEstimateOverhead = 8;
  constexpr std::size_t kSizeSafetyLimit = 8192;
  static constexpr std::size_t kOptimizationLevel[] = {4096, 8192, 16384, 32768, 65536};
  auto exceeds_limit = [fill](std::size_t new_sz, std::size_t new_count) {
    if (fill < 0) {
      const std::uint64_t offset =
          std::min<std::uint64_t>(static_cast<std::uint64_t>(-(fill + 1)), 4);
      return new_sz > kOptimizationLevel[offset];
    }
    const auto count_limit = static_cast<std::uint64_t>(fill == 0 ? 1 : fill);
    return new_sz > kSizeSafetyLimit || new_count > count_limit;
  };

  // First pass: where each node ends. The node count goes before the nodes in the file.
  struct Node {
    std::size_t end;  // one past the node's last element
    bool plain;
  };
  std::vector<Node> nodes;
  std::size_t node_bytes = 0, node_count = 0;
  for (std::size_t i = 0; i < items.size(); ++i) {
    const std::size_t sz = items[i].size();
    if (sz >= kPackedThreshold) {
      nodes.push_back({i + 1, true});
      node_count = 0;
      continue;
    }
    if (node_count == 0 || exceeds_limit(node_bytes + sz + kSizeEstimateOverhead, node_count + 1)) {
      nodes.push_back({i, false});
      node_bytes = kListpackHeaderAndEnd;
      node_count = 0;
    }
    node_bytes += ListpackWriter::string_entry_bytes(sz);
    ++node_count;
    nodes.back().end = i + 1;
  }

  w.write_length(nodes.size());
  std::size_t begin = 0;
  for (const auto& node : nodes) {
    if (node.plain) {
      w.write_length(kQuicklistNodePlain);
      w.write_string(items[begin]);
    } else {
      ListpackWriter lp;
      for (std::size_t i = begin; i < node.end; ++i) lp.add_string(items[i]);
      w.write_length(kQuicklistNodePacked);
      w.write_string(lp.finish());
    }
    begin = node.end;
  }
}

// Redis stream IDs in radix-tree keys and PELs are two big-endian uint64s.
using RdbStreamID = std::pair<uint64_t, uint64_t>;
std::string stream_id_text(RdbStreamID id) {
  return std::to_string(id.first) + "-" + std::to_string(id.second);
}
RdbStreamID stream_id_parts(const std::string& text) {
  const auto dash = text.find('-');
  RdbStreamID id{};
  if (dash == std::string::npos) return id;
  std::from_chars(text.data(), text.data() + dash, id.first);
  std::from_chars(text.data() + dash + 1, text.data() + text.size(), id.second);
  return id;
}
std::string stream_raw_id(RdbStreamID id) {
  std::string raw;
  for (auto part : {id.first, id.second})
    for (int i = 7; i >= 0; --i) raw.push_back(static_cast<char>(part >> (i * 8)));
  return raw;
}
bool stream_decode_raw_id(const std::string& raw, RdbStreamID& id) {
  if (raw.size() != 16) return false;
  id = {};
  for (int i = 0; i < 8; ++i) {
    id.first = (id.first << 8) | static_cast<uint8_t>(raw[i]);
    id.second = (id.second << 8) | static_cast<uint8_t>(raw[8 + i]);
  }
  return true;
}
bool read_plain_length(RdbReader& r, uint64_t& n) {
  bool encoded = false;
  return r.read_length(n, encoded) && !encoded;
}
bool read_stream_id(RdbReader& r, RdbStreamID& id) {
  return read_plain_length(r, id.first) && read_plain_length(r, id.second);
}
void write_stream_id(RdbWriter& w, RdbStreamID id) {
  w.write_length(id.first);
  w.write_length(id.second);
}

// rdbSaveObject(OBJ_STREAM), type 21. Nodes are bounded independently of the in-memory
// representation; this preserves logical data, rather than Redis's allocation/trim boundaries.
void write_stream(RdbWriter& w, const DataStore::StreamDump& d) {
  struct Node {
    RdbStreamID id;
    std::string lp;
  };
  std::vector<Node> nodes;
  for (std::size_t begin = 0; begin < d.entries.size();) {
    const auto master = stream_id_parts(d.entries[begin].first);
    // Bound each node to 100 entries / roughly 4 KiB, allowing a single larger entry.
    std::size_t end = begin, bytes = 0;
    do {
      for (const auto& [f, v] : d.entries[end].second) bytes += f.size() + v.size() + 16;
      ++end;
    } while (end < d.entries.size() && end - begin < 100 && bytes < 4096);
    ListpackWriter lp;
    lp.add_integer(end - begin);
    lp.add_integer(0);  // no tombstones; deleted IDs remain in history and in the PEL
    const auto& master_fields = d.entries[begin].second;
    lp.add_integer(master_fields.size());
    for (const auto& [f, v] : master_fields) lp.add_string(f);
    lp.add_integer(0);  // master terminator
    for (std::size_t i = begin; i < end; ++i) {
      const auto id = stream_id_parts(d.entries[i].first);
      const auto& fields = d.entries[i].second;
      bool same = fields.size() == master_fields.size();
      for (std::size_t j = 0; same && j < fields.size(); ++j)
        same = fields[j].first == master_fields[j].first;
      lp.add_integer(same ? 2 : 0);  // STREAM_ITEM_FLAG_SAMEFIELDS
      lp.add_integer(std::bit_cast<int64_t>(id.first - master.first));
      lp.add_integer(std::bit_cast<int64_t>(id.second - master.second));
      if (!same) lp.add_integer(fields.size());
      for (const auto& [f, v] : fields) {
        if (!same) lp.add_string(f);
        lp.add_string(v);
      }
      lp.add_integer(3 + fields.size() + (same ? 0 : fields.size() + 1));
    }
    nodes.push_back({master, lp.finish()});
    begin = end;
  }
  w.write_length(nodes.size());
  for (const auto& node : nodes) {
    w.write_string(stream_raw_id(node.id));
    w.write_string(node.lp);
  }
  w.write_length(d.entries.size());
  write_stream_id(w, {d.last_ms, d.last_seq});
  write_stream_id(w, d.entries.empty() ? RdbStreamID{} : stream_id_parts(d.entries.front().first));
  write_stream_id(w, stream_id_parts(d.max_deleted_id));
  w.write_length(d.entries_added.value_or(d.entries.size()));
  // Redis orders groups, consumers and their pending IDs lexicographically / numerically.
  std::map<std::string, const DataStore::StreamGroupDump*> groups;
  for (const auto& g : d.groups) groups.emplace(g.name, &g);
  w.write_length(groups.size());
  for (const auto& [name, gp] : groups) {
    const auto& g = *gp;
    w.write_string(name);
    write_stream_id(w, stream_id_parts(g.last_delivered_id));
    w.write_length(static_cast<uint64_t>(g.entries_read));
    std::map<RdbStreamID, const DataStore::StreamPel*> pel;
    for (const auto& p : g.pending) pel.emplace(stream_id_parts(p.id), &p);
    w.write_length(pel.size());
    for (const auto& [id, p] : pel) {
      w.write_raw(stream_raw_id(id).data(), 16);
      w.write_u64_le(static_cast<uint64_t>(p->delivery_ms));
      w.write_length(static_cast<uint64_t>(p->delivery_count));
    }
    std::map<std::string, std::vector<RdbStreamID>> consumer_pels;
    for (const auto& [id, p] : pel) consumer_pels[p->consumer].push_back(id);
    w.write_length(g.consumers.size());
    for (const auto& [consumer, times] : g.consumers) {
      w.write_string(consumer);
      w.write_u64_le(static_cast<uint64_t>(times.seen_time));
      w.write_u64_le(static_cast<uint64_t>(times.active_time));
      const auto& ids = consumer_pels[consumer];
      w.write_length(ids.size());
      for (auto id : ids) w.write_raw(stream_raw_id(id).data(), 16);
    }
  }
}

// rdbLoadObject's stream formats 15/19/21, with structural validation before key replacement.
bool read_stream(RdbReader& r, uint8_t type, DataStore::StreamDump& d) {
  uint64_t count;
  if (!read_plain_length(r, count)) return false;
  using Fields = std::vector<std::pair<std::string, std::string>>;
  std::map<RdbStreamID, Fields> entries;
  std::unordered_set<std::string> nodes;
  while (count--) {
    std::string raw, blob;
    RdbStreamID master;
    if (!r.read_string(raw) || !stream_decode_raw_id(raw, master) || !nodes.insert(raw).second ||
        !r.read_string(blob))
      return false;
    std::vector<std::string> values;
    std::vector<bool> integers;
    if (!ListpackReader(blob).parse(values, &integers)) return false;
    std::size_t pos = 0;
    auto number = [&](int64_t& n) {
      if (pos >= values.size() || !integers[pos]) return false;
      const auto& v = values[pos++];
      const auto result = std::from_chars(v.data(), v.data() + v.size(), n);
      return result.ec == std::errc{} && result.ptr == v.data() + v.size();
    };
    int64_t live, deleted, nf, zero;
    if (!number(live) || live < 0 || !number(deleted) || deleted < 0 ||
        static_cast<uint64_t>(live) + static_cast<uint64_t>(deleted) > values.size() ||
        !number(nf) || nf <= 0 || static_cast<uint64_t>(nf) > values.size() - pos)
      return false;
    std::vector<std::string> master_fields;
    for (int64_t i = 0; i < nf; ++i) master_fields.push_back(values[pos++]);
    if (!number(zero) || zero != 0) return false;
    int64_t got_live = 0, got_deleted = 0;
    std::optional<RdbStreamID> previous;
    for (uint64_t i = 0, total = static_cast<uint64_t>(live) + deleted; i < total; ++i) {
      int64_t flags, ms, seq, fields = nf;
      if (!number(flags) || flags < 0 || flags > 3 || !number(ms) || !number(seq)) return false;
      const RdbStreamID id{master.first + static_cast<uint64_t>(ms),
                           master.second + static_cast<uint64_t>(seq)};
      if (id == RdbStreamID{} || (previous && id <= *previous)) return false;
      previous = id;
      const bool same = (flags & 2) != 0;
      if (!same && !number(fields)) return false;
      const auto parts = same ? 1u : 2u;
      if (fields <= 0 || static_cast<uint64_t>(fields) > (values.size() - pos) / parts)
        return false;
      Fields fv;
      for (int64_t j = 0; j < fields; ++j) {
        std::string field = same ? master_fields[j] : values[pos++];
        fv.emplace_back(std::move(field), values[pos++]);
      }
      int64_t pieces;
      if (!number(pieces) || pieces != 3 + fields + (same ? 0 : fields + 1)) return false;
      if (flags & 1)
        ++got_deleted;
      else {
        ++got_live;
        if (!entries.emplace(id, std::move(fv)).second) return false;
      }
    }
    if (pos != values.size() || got_live != live || got_deleted != deleted) return false;
  }
  for (auto& [id, fv] : entries) d.entries.emplace_back(stream_id_text(id), std::move(fv));
  uint64_t length, added;
  RdbStreamID last, first{}, max_deleted{};
  if (!read_plain_length(r, length) || length != d.entries.size() || !read_stream_id(r, last))
    return false;
  if (type >= RDB_TYPE_STREAM_LISTPACKS_2) {
    if (!read_stream_id(r, first) || !read_stream_id(r, max_deleted) ||
        !read_plain_length(r, added))
      return false;
  } else {
    added = length;
    if (!entries.empty()) first = entries.begin()->first;
  }
  // The datastore derives first-id from surviving entries; refuse inconsistent metadata
  // rather than silently changing it. All valid Redis streams obey these invariants.
  if (type >= RDB_TYPE_STREAM_LISTPACKS_2 &&
      (first != (entries.empty() ? RdbStreamID{} : entries.begin()->first) || added < length ||
       max_deleted > last))
    return false;
  if (!entries.empty() && entries.rbegin()->first > last) return false;
  d.last_ms = last.first;
  d.last_seq = last.second;
  d.entries_added = added;
  d.max_deleted_id = stream_id_text(max_deleted);
  if (!read_plain_length(r, count)) return false;
  std::unordered_set<std::string> group_names;
  while (count--) {
    DataStore::StreamGroupDump g;
    RdbStreamID cursor;
    if (!r.read_string(g.name) || !group_names.insert(g.name).second || !read_stream_id(r, cursor))
      return false;
    g.last_delivered_id = stream_id_text(cursor);
    if (type >= RDB_TYPE_STREAM_LISTPACKS_2) {
      uint64_t read;
      if (!read_plain_length(r, read)) return false;
      g.entries_read = std::bit_cast<int64_t>(read);
    } else {
      // streamEstimateDistanceFromFirstEverEntry(), after the legacy history reset above.
      if (length == 0)
        g.entries_read = 0;
      else if (cursor == last)
        g.entries_read = length;
      else if (cursor > last)
        g.entries_read = -1;
      else if (cursor < first)
        g.entries_read = 0;
      else if (cursor == first)
        g.entries_read = 1;
      else
        g.entries_read = -1;
    }
    uint64_t pel_size;
    if (!read_plain_length(r, pel_size)) return false;
    std::map<RdbStreamID, std::size_t> pel;
    while (pel_size--) {
      DataStore::StreamPel p;
      std::string raw;
      RdbStreamID id;
      uint64_t time, deliveries;
      if (!r.read_bytes(raw, 16) || !stream_decode_raw_id(raw, id) || !r.read_u64_le(time) ||
          !read_plain_length(r, deliveries) || !pel.emplace(id, g.pending.size()).second)
        return false;
      p.id = stream_id_text(id);
      p.delivery_ms = std::bit_cast<int64_t>(time);
      p.delivery_count = std::bit_cast<int64_t>(deliveries);
      g.pending.push_back(std::move(p));
    }
    uint64_t consumers;
    if (!read_plain_length(r, consumers)) return false;
    std::unordered_set<std::size_t> owned;
    while (consumers--) {
      std::string name;
      uint64_t seen, active;
      if (!r.read_string(name) || g.consumers.contains(name) || !r.read_u64_le(seen)) return false;
      active = seen;
      if (type >= RDB_TYPE_STREAM_LISTPACKS_3 && !r.read_u64_le(active)) return false;
      g.consumers.emplace(name, Entry::StreamConsumer{std::bit_cast<int64_t>(seen),
                                                      std::bit_cast<int64_t>(active)});
      uint64_t n;
      if (!read_plain_length(r, n)) return false;
      while (n--) {
        std::string raw;
        RdbStreamID id;
        if (!r.read_bytes(raw, 16) || !stream_decode_raw_id(raw, id)) return false;
        auto it = pel.find(id);
        if (it == pel.end() || !owned.insert(it->second).second) return false;
        g.pending[it->second].consumer = name;
      }
    }
    // A dangling or multiply-owned NACK cannot be represented safely by the datastore.
    if (owned.size() != g.pending.size()) return false;
    d.groups.push_back(std::move(g));
  }
  return true;
}

// Shared RDB object decoder. RESTORE holds the decoded value outside the keyspace, so even
// REPLACE cannot erase a key on a corrupt payload. RDB load may skip empty collection values.
bool read_dump_value(RdbReader& r, uint8_t type, DumpValue& out, bool allow_empty = false,
                     std::string* load_error = nullptr) {
  out = DumpValue{};
  out.type = type;
  auto corrupt = [&](const char* error) {
    if (load_error) *load_error = error;
    return false;
  };
  auto count = [&](uint64_t& len) {
    return read_plain_length(r, len) && (allow_empty || len > 0);
  };
  auto unique = [](const auto& items, auto name_of) {
    std::unordered_set<std::string_view> seen;
    for (const auto& item : items)
      if (!seen.insert(name_of(item)).second) return false;
    return true;
  };
  uint64_t len = 0;
  if (type == RDB_TYPE_STRING) {
    return r.read_string(out.str);
  } else if (type == RDB_TYPE_LIST || type == RDB_TYPE_SET) {
    if (!count(len)) return false;
    while (len--) {
      std::string item;
      if (!r.read_string(item)) return false;
      out.items.push_back(std::move(item));
    }
  } else if (type == RDB_TYPE_HASH_ZIPMAP) {
    std::string blob;
    if (!r.read_string(blob) || !read_zipmap(blob, out.fields))
      return corrupt("Zipmap integrity check failed.");
    out.type = RDB_TYPE_HASH;
  } else if (type == RDB_TYPE_LIST_ZIPLIST || type == RDB_TYPE_HASH_ZIPLIST ||
             type == RDB_TYPE_ZSET_ZIPLIST) {
    std::string blob;
    std::vector<std::string> items;
    const char* integrity = type == RDB_TYPE_LIST_ZIPLIST ? "List ziplist integrity check failed."
                            : type == RDB_TYPE_HASH_ZIPLIST
                                ? "Hash ziplist integrity check failed."
                                : "Zset ziplist integrity check failed.";
    if (!r.read_string(blob) || !read_ziplist(blob, items)) return corrupt(integrity);
    if (type == RDB_TYPE_LIST_ZIPLIST) {
      out.items = std::move(items);
      out.type = RDB_TYPE_LIST;
    } else {
      if (items.size() % 2 != 0) return corrupt(integrity);
      if (type == RDB_TYPE_HASH_ZIPLIST) {
        for (std::size_t i = 0; i < items.size(); i += 2)
          out.fields.emplace_back(std::move(items[i]), std::move(items[i + 1]));
        out.type = RDB_TYPE_HASH;
      } else {
        for (std::size_t i = 0; i < items.size(); i += 2) {
          const auto& text = items[i + 1];
          char* end = nullptr;
          const double score = std::strtod(text.c_str(), &end);
          if (text.empty() || end != text.c_str() + text.size() || std::isnan(score))
            return corrupt(integrity);
          out.scored.emplace_back(std::move(items[i]), score);
        }
        out.type = RDB_TYPE_ZSET_2;
      }
    }
  } else if (type == RDB_TYPE_LIST_QUICKLIST || type == RDB_TYPE_LIST_QUICKLIST_2) {
    if (!count(len)) return false;
    while (len--) {
      uint64_t container;
      std::string blob;
      container = kQuicklistNodePacked;
      if ((type == RDB_TYPE_LIST_QUICKLIST_2 && !read_plain_length(r, container)) ||
          !r.read_string(blob) || blob.empty())
        return false;
      if (container != kQuicklistNodePlain && container != kQuicklistNodePacked)
        return corrupt("Quicklist integrity check failed.");
      if (container == kQuicklistNodePlain)
        out.items.push_back(std::move(blob));
      else {
        std::vector<std::string> node;
        if (type == RDB_TYPE_LIST_QUICKLIST) {
          if (!read_ziplist(blob, node)) return corrupt("Ziplist integrity check failed.");
        } else if (!ListpackReader(blob).parse(node))
          return corrupt("Listpack integrity check failed.");
        for (auto& item : node) out.items.push_back(std::move(item));
      }
    }
    out.type = RDB_TYPE_LIST;
  } else if (type == RDB_TYPE_SET_INTSET) {
    std::string blob;
    if (!r.read_string(blob) || blob.size() < 8) return false;
    auto u32 = [&](std::size_t off) {
      uint32_t n = 0;
      for (int i = 0; i < 4; ++i)
        n |= static_cast<uint32_t>(static_cast<uint8_t>(blob[off + i])) << (8 * i);
      return n;
    };
    const uint32_t encoding = u32(0), n = u32(4);
    if ((encoding != 2 && encoding != 4 && encoding != 8) || n != (blob.size() - 8) / encoding ||
        (blob.size() - 8) % encoding != 0)
      return false;
    std::optional<int64_t> previous;
    for (uint32_t i = 0; i < n; ++i) {
      uint64_t bits = 0;
      for (uint32_t j = 0; j < encoding; ++j)
        bits |= static_cast<uint64_t>(
                    static_cast<uint8_t>(blob[8 + static_cast<std::size_t>(i) * encoding + j]))
                << (8 * j);
      const int64_t value = encoding == 2   ? static_cast<int16_t>(bits)
                            : encoding == 4 ? static_cast<int32_t>(bits)
                                            : std::bit_cast<int64_t>(bits);
      if (previous && value <= *previous) return false;
      previous = value;
      out.items.push_back(std::to_string(value));
    }
    out.type = RDB_TYPE_SET;
  } else if (type == RDB_TYPE_HASH_LISTPACK || type == RDB_TYPE_ZSET_LISTPACK ||
             type == RDB_TYPE_SET_LISTPACK) {
    std::string blob;
    std::vector<std::string> items;
    const char* integrity = type == RDB_TYPE_SET_LISTPACK ? "Set listpack integrity check failed."
                            : type == RDB_TYPE_HASH_LISTPACK
                                ? "Hash listpack integrity check failed."
                                : "Zset listpack integrity check failed.";
    if (!r.read_string(blob) || !ListpackReader(blob).parse(items)) return corrupt(integrity);
    if (type == RDB_TYPE_SET_LISTPACK) {
      out.items = std::move(items);
      out.type = RDB_TYPE_SET;
    } else {
      if (items.size() % 2 != 0) return corrupt(integrity);
      if (type == RDB_TYPE_HASH_LISTPACK) {
        for (std::size_t i = 0; i < items.size(); i += 2)
          out.fields.emplace_back(std::move(items[i]), std::move(items[i + 1]));
        out.type = RDB_TYPE_HASH;
      } else {
        for (std::size_t i = 0; i < items.size(); i += 2) {
          // strtod preserves subnormal scores (std::stod throws on ERANGE and lost them).
          const auto& text = items[i + 1];
          char* end = nullptr;
          const double score = std::strtod(text.c_str(), &end);
          if (text.empty() || end != text.c_str() + text.size() || std::isnan(score)) return false;
          out.scored.emplace_back(std::move(items[i]), score);
        }
        out.type = RDB_TYPE_ZSET_2;
      }
    }
  } else if (type == RDB_TYPE_HASH) {
    if (!count(len)) return false;
    while (len--) {
      std::string f, v;
      if (!r.read_string(f) || !r.read_string(v)) return false;
      out.fields.emplace_back(std::move(f), std::move(v));
    }
  } else if (type == RDB_TYPE_ZSET_2 || type == RDB_TYPE_ZSET) {
    if (!count(len)) return false;
    while (len--) {
      std::string member;
      double score;
      if (!r.read_string(member) ||
          !(type == RDB_TYPE_ZSET_2 ? r.read_double_binary(score) : r.read_double_old(score)) ||
          std::isnan(score))
        return false;
      out.scored.emplace_back(std::move(member), score);
    }
  } else if (type == RDB_TYPE_STREAM_LISTPACKS || type == RDB_TYPE_STREAM_LISTPACKS_2 ||
             type == RDB_TYPE_STREAM_LISTPACKS_3) {
    return read_stream(r, type, out.stream);
  } else
    return false;
  if (out.type == RDB_TYPE_LIST || out.type == RDB_TYPE_SET) {
    if (!allow_empty && out.items.empty()) return false;
    if (out.type == RDB_TYPE_SET &&
        !unique(out.items, [](const auto& s) -> std::string_view { return s; }))
      return false;
  } else if (out.type == RDB_TYPE_HASH) {
    if (!allow_empty && out.fields.empty()) return false;
    if (!unique(out.fields, [](const auto& fv) -> std::string_view { return fv.first; }))
      return false;
  } else {
    if (!allow_empty && out.scored.empty()) return false;
    if (!unique(out.scored, [](const auto& ms) -> std::string_view { return ms.first; }))
      return false;
  }
  return true;
}

// Wait rdb-key-save-delay after each key, as Redis's rdbSaveKeyValuePair() does.
// Used by file saves, including full-sync snapshots.
void rdb_serialize(RdbWriter& w, const RdbReplicationInfo* repl) {
  auto& ds = store();
  // Like Redis's rdbSaveRio(), every key is written, expired ones too (with their expire time):
  // saving deletes nothing, so no key leaves the dataset without its DEL (compat/delta.md item 37).
  // A master that loads the file leaves them out; a replica keeps them until its master's DEL.
  const DataStore::LazyExpireDisabled no_lazy_expire(ds);

  // Magic + version
  char header[10];
  std::snprintf(header, sizeof(header), "REDIS%04d", RDB_VERSION);
  w.write_raw(header, 9);

  // AUX fields
  auto write_aux = [&](const std::string& key, const std::string& val) {
    w.write_u8(RDB_OPCODE_AUX);
    w.write_string(key);
    w.write_string(val);
  };
  write_aux("redis-ver", "7.2.5");
  write_aux("redis-bits", "64");
  write_aux("ctime", std::to_string(std::time(nullptr)));
  write_aux("used-mem", "0");
  // An RDB with live/cached history carries the state it was taken at, as Redis's
  // rdbSaveInfoAuxFields() writes it: the replica selects repl-stream-db for its master's stream.
  if (repl != nullptr) {
    write_aux("repl-stream-db", std::to_string(repl->stream_db));
    write_aux("repl-id", repl->replid);
    write_aux("repl-offset", std::to_string(repl->offset));
  }

  // Function libraries (server-level, not per-db) — one FUNCTION2 opcode + string per library,
  // written after AUX and before the keyspace, matching Redis's RDB layout.
  if (g_function_dump_hook) {
    g_function_dump_hook([&](const std::string& code) {
      w.write_u8(RDB_OPCODE_FUNCTION2);
      w.write_string(code);
    });
  }

  // Traverse names in place: no snapshot-sized key vector. Getters cannot expire keys while
  // no_lazy_expire is active, and sinks must not re-enter the datastore.
  struct RestoreDatabase {
    DataStore& ds;
    int previous;
    ~RestoreDatabase() {
      ds.select_db(previous);
    }
  } restore_db{ds, ds.current_db()};

  for (std::size_t dbi = 0; dbi < ds.database_count(); ++dbi) {
    ds.select_db(dbi);
    std::size_t key_count = 0, expire_count = 0;
    ds.for_each_key([&](const std::string& key) {
      ++key_count;
      if (ds.pexpiretime(key) >= 0) ++expire_count;
    });
    if (key_count == 0) continue;

    w.write_u8(RDB_OPCODE_SELECTDB);
    w.write_length(static_cast<uint64_t>(dbi));

    // RESIZEDB
    w.write_u8(RDB_OPCODE_RESIZEDB);
    w.write_length(key_count);
    w.write_length(expire_count);

    ds.for_each_key([&](const std::string& key) {
      struct DelayAfterKey {
        int usec;
        ~DelayAfterKey() {
          if (usec != 0) debug_delay(usec);
        }
      } delay_after_key{g_rdb_key_save_delay};
      // Expire
      auto pttl_val = ds.pexpiretime(key);
      if (pttl_val >= 0) {
        w.write_u8(RDB_OPCODE_EXPIRETIME_MS);
        w.write_u64_le(static_cast<uint64_t>(pttl_val));
      }

      const auto type_str = ds.type_of(key);

      if (type_str == "string") {
        auto val = ds.get(key);
        if (!val.has_value()) return;
        w.write_u8(RDB_TYPE_STRING);
        w.write_string(key);
        w.write_string(*val);

      } else if (type_str == "list") {
        bool wrongtype = false;
        auto items = ds.lrange(key, 0, -1, wrongtype);
        if (wrongtype) return;
        w.write_u8(RDB_TYPE_LIST_QUICKLIST_2);
        w.write_string(key);
        write_quicklist(w, items, ds.encoding_limits().list_size);

      } else if (type_str == "set") {
        bool wrongtype = false;
        auto members = ds.smembers(key, wrongtype);
        if (wrongtype) return;
        w.write_u8(RDB_TYPE_SET);
        w.write_string(key);
        w.write_length(members.size());
        for (const auto& m : members) w.write_string(m);

      } else if (type_str == "hash") {
        bool wrongtype = false;
        auto fields = ds.hgetall(key, wrongtype);
        if (wrongtype) return;
        w.write_u8(RDB_TYPE_HASH);
        w.write_string(key);
        w.write_length(fields.size());
        for (const auto& [f, v] : fields) {
          w.write_string(f);
          w.write_string(v);
        }

      } else if (type_str == "zset") {
        bool wrongtype = false;
        auto items = ds.zrange(key, 0, -1, true, wrongtype);
        if (wrongtype || items.empty()) return;
        w.write_u8(RDB_TYPE_ZSET_2);
        w.write_string(key);
        w.write_length(items.size());
        for (const auto& [m, s] : items) {
          w.write_string(m);
          w.write_double_string(s);
        }

      } else if (type_str == "stream") {
        DataStore::StreamDump d;
        if (!ds.dump_stream(key, d)) return;
        w.write_u8(RDB_TYPE_STREAM_LISTPACKS_3);
        w.write_string(key);
        write_stream(w, d);
      }
    });
  }

  // EOF marker
  w.write_u8(RDB_OPCODE_EOF);

  // CRC64 of everything before it; with rdbchecksum no, Redis writes 0 ("no checksum") instead.
  const uint64_t checksum = g_rdb_checksum ? w.checksum() : 0;
  w.write_u64_le(checksum);

  w.flush();
}
}  // end anonymous namespace

bool rdb_write(const RdbWriteFn& sink, std::string& err, const RdbReplicationInfo* repl) {
  err.clear();
  if (!sink) {
    err = "Error saving DB: missing output sink";
    return false;
  }
  try {
    RdbWriter writer(sink, g_rdb_checksum);
    rdb_serialize(writer, repl);
    return true;
  } catch (const std::system_error& e) {
    err = std::string("Write error saving DB: ") + e.code().message();
  } catch (const std::exception& e) {
    err = std::string("Error saving DB: ") + e.what();
  }
  return false;
}

bool rdb_save(const std::filesystem::path& path, std::string& err, const RdbReplicationInfo* repl) {
  err.clear();
  auto cwd = [] {
    std::error_code ec;
    const auto dir = std::filesystem::current_path(ec);
    return ec ? std::string("unknown") : dir.string();
  };
  // Like Redis's rdbSave(): the data goes to temp-<pid>.rdb next to the dump and is renamed over
  // it. The pid keeps the BGSAVE child, a foreground SAVE and a shutdown's final save off each
  // other's file, and lets a shutdown delete the file of a BGSAVE child it stops. The file is
  // created before the keys are serialized, as Redis creates it before it writes the first key.
  const std::string tmp_path = rdb_temp_file_name(path, ::getpid());
  const int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    err = "Failed opening the temp RDB file " + tmp_path + " (in server root dir " + cwd() +
          ") for saving: " + std::strerror(errno);
    return false;
  }
  if (!rdb_write([fd](const void* data, std::size_t size) { return ::write(fd, data, size); }, err,
                 repl)) {
    if (err.starts_with("Write error saving DB:")) err.insert(21, " on disk");
    ::close(fd);
    ::unlink(tmp_path.c_str());
    return false;
  }

  auto write_error = [&] {
    err = std::string("Write error saving DB on disk: ") + std::strerror(errno);
    ::close(fd);
    ::unlink(tmp_path.c_str());
    return false;
  };
  // Fail fast on fsync error: a successful rename of an unfsynced temp file
  // can leave the new RDB with stale or torn contents after a power loss.
  if (::fsync(fd) != 0) return write_error();
  if (::close(fd) != 0) {
    err = std::string("Write error saving DB on disk: ") + std::strerror(errno);
    ::unlink(tmp_path.c_str());
    return false;
  }

  if (::rename(tmp_path.c_str(), path.c_str()) != 0) {
    err = "Error moving temp DB file " + tmp_path + " on the final destination " + path.string() +
          " (in server root dir " + cwd() + "): " + std::strerror(errno);
    ::unlink(tmp_path.c_str());
    return false;
  }
  // Crash-durability: fsync the parent directory so the new RDB directory
  // entry survives power loss. If the dir fsync fails, the rename is visible
  // to the running process but the new dirent may revert after a crash; this
  // violates the durability contract of SAVE/BGSAVE so we report it as a
  // failure. The file on disk is intact — operators can retry.
  if (!fsync_parent_dir(path)) {
    err = "RDB parent directory fsync failed (rename not crash-durable)";
    return false;
  }
  return true;
}

void rdb_remove_temp_file(pid_t pid) {
  ::unlink(rdb_temp_file_name(std::filesystem::path(), pid).c_str());
}

// ── RDB Load ───────────────────────────────────────────────────────────

bool rdb_load(const std::filesystem::path& path, std::string& err,
              const std::function<void()>& before_replace, RdbReplicationInfo* repl,
              const std::function<void(int, const std::string&)>& expired) {
  err.clear();
  if (repl) *repl = {};
  RdbReplicationInfo loaded_repl;

  // Keep the opened inode through validation, the replacement callback (which can rename it),
  // and application. A path re-open after publication could decode a different snapshot.
  struct InputFile {
    int fd;
    ~InputFile() {
      if (fd >= 0) ::close(fd);
    }
  } file{::open(path.c_str(), O_RDONLY)};
  if (file.fd < 0) {
    err = "cannot open RDB file";
    return false;
  }
  struct stat st {};
  if (::fstat(file.fd, &st) != 0 || st.st_size < 0 ||
      static_cast<std::uintmax_t>(st.st_size) > std::numeric_limits<std::size_t>::max()) {
    err = "cannot stat RDB file";
    return false;
  }
  const auto file_size = static_cast<std::size_t>(st.st_size);
  const detail::RdbReadFn source = [&](void* p, std::size_t n) {
    return ::read(file.fd, p, n);
  };
  RdbReader input(source, file_size, true);
  std::string header;
  if (!input.read_bytes(header, 9)) {
    err = "RDB file too short";
    return false;
  }

  // Check magic
  if (header.substr(0, 5) != "REDIS") {
    err = "invalid RDB magic";
    return false;
  }

  int rdb_ver = 0;
  try {
    rdb_ver = std::stoi(header.substr(5, 4));
  } catch (...) {
    err = "invalid RDB version";
    return false;
  }
  if (rdb_ver < 1 || rdb_ver > RDB_VERSION_MAX_LOAD) {
    err = "Can't handle RDB format version " + std::to_string(rdb_ver);
    return false;
  }

  // The trailing CRC64 of a version 5+ file, checked like Redis's rdbLoadRio() unless
  // rdbchecksum is no or DEBUG SET-SKIP-CHECKSUM-VALIDATION is on, for every load: startup,
  // DEBUG RELOAD and a replica's full sync. A stored 0 means the file was saved without a
  // checksum. Redis checks it after reading the keys; PeaDB checks it first, so a corrupt file
  // never replaces the dataset in memory.
  if (rdb_ver >= 5 && g_rdb_checksum && !g_skip_checksum_validation) {
    uint64_t stored = 0;
    if (input.remaining() < 8 || !input.skip(input.remaining() - 8)) {
      err = "Short read or OOM loading DB. Unrecoverable error, aborting now.";
      return false;
    }
    const uint64_t computed = input.checksum();
    if (!input.read_u64_le(stored)) {
      err = "Short read or OOM loading DB. Unrecoverable error, aborting now.";
      return false;
    }
    if (stored == 0) {
      log(LogLevel::Info, "RDB file was saved with checksum disabled: no check performed.");
    } else {
      if (computed != stored) {
        char msg[128];
        std::snprintf(
            msg, sizeof(msg), "Wrong RDB checksum expected: (%llx) but got (%llx). Aborting now.",
            static_cast<unsigned long long>(computed), static_cast<unsigned long long>(stored));
        err = msg;
        log(LogLevel::Error, "RDB CRC error");
        return false;
      }
    }
  }

  auto& ds = store();
  auto load = [&](bool apply) {
    if (::lseek(file.fd, 9, SEEK_SET) == -1) {
      err = "cannot seek RDB file";
      return false;
    }
    RdbReader r(source, file_size - 9);
    RdbReplicationInfo parsed_repl;
    if (apply) {
      g_rdb_load_stats = {};
      if (before_replace) before_replace();
      ds.flushall();
    }
    // No key expires while the file loads (Redis's keyIsExpired() while loading); a master leaves
    // out the keys that have expired already, a replica keeps them until its master's DEL (below).
    std::optional<DataStore::LazyExpireDisabled> no_lazy_expire;
    if (apply) no_lazy_expire.emplace(ds);
    const std::int64_t load_now = DataStore::now_ms();
    const int prev_db = ds.current_db();
    struct RestoreDatabase {
      DataStore& ds;
      int previous;
      bool apply;
      ~RestoreDatabase() {
        if (apply) ds.select_db(previous);
      }
    } restore_db{ds, prev_db, apply};
    if (apply) ds.select_db(0);

    std::optional<int64_t> pending_expire;
    // Like Redis, a file must end with the EOF opcode (and, from version 5, the 8-byte checksum
    // after it): a file cut at a key boundary is an error even when the checksum is not checked.
    bool saw_eof = false;

    while (!r.eof()) {
      uint8_t type;
      if (!r.read_u8(type)) break;

      if (type == RDB_OPCODE_EOF) {
        saw_eof = true;
        break;
      }

      // Written by a Redis with an LRU or LFU maxmemory-policy, before each key. Read and
      // dropped: a loaded key starts with fresh eviction data, and eviction is approximate in
      // PeaDB anyway (compat/delta.md item 12).
      if (type == RDB_OPCODE_IDLE) {
        uint64_t idle_seconds;
        bool enc;
        if (!r.read_length(idle_seconds, enc) || enc) {
          err = "malformed IDLE";
          return false;
        }
        continue;
      }
      if (type == RDB_OPCODE_FREQ) {
        uint8_t freq;
        if (!r.read_u8(freq)) {
          err = "malformed FREQ";
          return false;
        }
        continue;
      }

      if (type == RDB_OPCODE_AUX) {
        std::string aux_key, aux_val;
        if (!r.read_string(aux_key) || !r.read_string(aux_val)) {
          err = "malformed AUX field";
          return false;
        }
        // AUX names are case-insensitive in Redis. Parse complete, bounded values: malformed
        // optional metadata must never authorize PSYNC against an unrelated dataset.
        std::transform(aux_key.begin(), aux_key.end(), aux_key.begin(),
                       [](unsigned char c) { return c >= 'A' && c <= 'Z' ? c + ('a' - 'A') : c; });
        auto integer = [&]() -> std::optional<std::int64_t> {
          std::int64_t n;
          const auto [end, ec] =
              std::from_chars(aux_val.data(), aux_val.data() + aux_val.size(), n);
          if (ec != std::errc{} || end != aux_val.data() + aux_val.size()) return std::nullopt;
          return n;
        };
        if (aux_key == "repl-stream-db") {
          const auto db = integer();
          parsed_repl.stream_db =
              db && *db >= 0 && static_cast<std::uint64_t>(*db) < ds.database_count()
                  ? static_cast<int>(*db)
                  : -1;
        } else if (aux_key == "repl-id") {
          parsed_repl.replid = aux_val;
        } else if (aux_key == "repl-offset") {
          parsed_repl.offset = integer().value_or(-1);
        }
        continue;
      }

      if (type == RDB_OPCODE_FUNCTION2) {
        std::string code;
        if (!r.read_string(code)) {
          err = "malformed FUNCTION payload";
          return false;
        }
        if (apply && g_function_load_hook && !g_function_load_hook(code, err)) {
          return false;
        }
        continue;
      }

      if (type == RDB_OPCODE_SELECTDB) {
        uint64_t dbnum;
        bool enc;
        if (!r.read_length(dbnum, enc) || enc) {
          err = "malformed SELECTDB";
          return false;
        }
        if (dbnum >= ds.database_count()) {
          err = "FATAL: Data file was created with a Redis server configured to handle more than " +
                std::to_string(ds.database_count()) + " databases. Exiting";
          return false;
        }
        if (apply) ds.select_db(static_cast<int>(dbnum));
        continue;
      }

      if (type == RDB_OPCODE_RESIZEDB) {
        uint64_t db_size, exp_size;
        bool enc1, enc2;
        if (!r.read_length(db_size, enc1) || enc1 || !r.read_length(exp_size, enc2) || enc2) {
          err = "malformed RESIZEDB";
          return false;
        }
        continue;
      }

      if (type == RDB_OPCODE_EXPIRETIME_MS) {
        uint64_t ms;
        if (!r.read_u64_le(ms)) {
          err = "malformed EXPIRETIME_MS";
          return false;
        }
        pending_expire = static_cast<int64_t>(ms);
        continue;
      }

      if (type == RDB_OPCODE_EXPIRETIME) {
        uint32_t sec;
        if (!r.read_u32_le(sec)) {
          err = "malformed EXPIRETIME";
          return false;
        }
        pending_expire = static_cast<int64_t>(sec) * 1000;
        continue;
      }

      // It's a value type — read key
      std::string key;
      if (!r.read_string(key)) {
        err = "malformed key";
        return false;
      }

      std::optional<int64_t> expire_at = pending_expire;
      pending_expire.reset();

      if (type != RDB_TYPE_PEADB_STREAM) {
        DumpValue value;
        std::string load_error;
        if (!read_dump_value(r, type, value, true, &load_error)) {
          err = load_error.empty() ? "Bad data format (RDB type " + std::to_string(type) + ")"
                                   : load_error;
          return false;
        }
        if (apply) rdb_store_dump_value(key, std::move(value), expire_at);
      } else {
        DataStore::StreamDump d;
        uint64_t nentries = 0;
        bool enc = false;
        if (!r.read_length(nentries, enc) || enc) {
          err = "malformed stream";
          return false;
        }
        for (uint64_t i = 0; i < nentries; ++i) {
          std::string id;
          if (!r.read_string(id)) {
            err = "malformed stream entry id";
            return false;
          }
          uint64_t nf = 0;
          if (!r.read_length(nf, enc) || enc) {
            err = "malformed stream entry";
            return false;
          }
          std::vector<std::pair<std::string, std::string>> fields;
          for (uint64_t j = 0; j < nf; ++j) {
            std::string f, v;
            if (!r.read_string(f) || !r.read_string(v)) {
              err = "malformed stream field";
              return false;
            }
            fields.emplace_back(std::move(f), std::move(v));
          }
          d.entries.emplace_back(std::move(id), std::move(fields));
        }
        uint64_t lms = 0, lseq = 0;
        if (!r.read_length(lms, enc) || enc || !r.read_length(lseq, enc) || enc) {
          err = "malformed stream last-id";
          return false;
        }
        d.last_ms = lms;
        d.last_seq = lseq;
        uint64_t ngroups = 0;
        if (!r.read_length(ngroups, enc) || enc) {
          err = "malformed stream groups";
          return false;
        }
        for (uint64_t gi = 0; gi < ngroups; ++gi) {
          DataStore::StreamGroupDump gd;
          if (!r.read_string(gd.name) || !r.read_string(gd.last_delivered_id)) {
            err = "malformed stream group";
            return false;
          }
          uint64_t npel = 0;
          if (!r.read_length(npel, enc) || enc) {
            err = "malformed stream pel";
            return false;
          }
          for (uint64_t pi = 0; pi < npel; ++pi) {
            DataStore::StreamPel p;
            uint64_t dms = 0, dcnt = 0;
            if (!r.read_string(p.id) || !r.read_string(p.consumer) || !r.read_length(dms, enc) ||
                enc || !r.read_length(dcnt, enc) || enc) {
              err = "malformed stream pel entry";
              return false;
            }
            p.delivery_ms = static_cast<std::int64_t>(dms);
            p.delivery_count = static_cast<std::int64_t>(dcnt);
            gd.pending.push_back(std::move(p));
          }
          d.groups.push_back(std::move(gd));
        }
        if (apply) {
          ds.restore_stream(key, d);
          if (expire_at) ds.expire(key, *expire_at);
        }
      }
      // Redis's rdbLoadRio(): a master does not load a key that has already expired.
      if (apply) {
        if (expire_at && *expire_at < load_now && !ds.replica_expiry()) {
          ds.del(key);
          ++g_rdb_load_stats.expired;
          if (expired) expired(ds.current_db(), key);
        } else {
          ++g_rdb_load_stats.loaded;
        }
      }
    }

    uint64_t checksum_footer;
    if (!saw_eof || (rdb_ver >= 5 && !r.read_u64_le(checksum_footer))) {
      err = "Short read or OOM loading DB. Unrecoverable error, aborting now.";
      return false;
    }
    if (apply) loaded_repl = std::move(parsed_repl);
    return true;
  };
  // A full sync must refuse an out-of-range SELECTDB before its callback publishes the
  // snapshot or clears libraries/persistence/history. Decode one value at a time, discarding
  // it, then re-read the same immutable bytes to apply; no second whole dataset is retained.
  if (before_replace && !load(false)) return false;
  if (!load(true)) return false;
  if (repl) *repl = std::move(loaded_repl);
  return true;
}

// ── DUMP / RESTORE ─────────────────────────────────────────────────────

namespace {
// Redis's DUMP footer (createDumpPayload()): the RDB version as 2 little-endian bytes, then the
// CRC64 of everything before it as 8 little-endian bytes. DUMP always writes the checksum, even
// with rdbchecksum no, as Redis does.
std::string finish_dump_payload(std::string payload) {
  payload.push_back(static_cast<char>(RDB_VERSION & 0xff));
  payload.push_back(static_cast<char>((RDB_VERSION >> 8) & 0xff));
  const uint64_t checksum = crc64(0, payload.data(), payload.size());
  for (int i = 0; i < 8; ++i) payload.push_back(static_cast<char>((checksum >> (i * 8)) & 0xff));
  return payload;
}

// Redis's verifyDumpPayload(), for RESTORE and FUNCTION RESTORE: the 10-byte footer is there, its
// RDB version is one Redis 7.2.5 reads (checked even when DEBUG SET-SKIP-CHECKSUM-VALIDATION is
// on), and the CRC64 matches unless that DEBUG setting is on.
bool verify_dump_payload(const std::string& payload) {
  if (payload.size() < 10) return false;
  const auto* footer = reinterpret_cast<const unsigned char*>(payload.data() + payload.size() - 10);
  const int rdbver = footer[0] | (footer[1] << 8);
  if (rdbver > RDB_VERSION_MAX_LOAD) return false;
  if (g_skip_checksum_validation) return true;
  uint64_t stored = 0;
  for (int i = 0; i < 8; ++i) stored |= static_cast<uint64_t>(footer[2 + i]) << (8 * i);
  return crc64(0, payload.data(), payload.size() - 8) == stored;
}
}  // namespace

std::string rdb_dump_key(const std::string& key) {
  auto& ds = store();
  const auto type_str = ds.type_of(key);
  if (type_str == "none") return {};

  RdbWriter w;

  if (type_str == "string") {
    auto val = ds.get(key);
    if (!val.has_value()) return {};
    w.write_u8(RDB_TYPE_STRING);
    w.write_string(*val);

  } else if (type_str == "list") {
    bool wrongtype = false;
    auto items = ds.lrange(key, 0, -1, wrongtype);
    if (wrongtype) return {};
    w.write_u8(RDB_TYPE_LIST);
    w.write_length(items.size());
    for (const auto& item : items) w.write_string(item);

  } else if (type_str == "set") {
    bool wrongtype = false;
    auto members = ds.smembers(key, wrongtype);
    if (wrongtype) return {};
    w.write_u8(RDB_TYPE_SET);
    w.write_length(members.size());
    for (const auto& m : members) w.write_string(m);

  } else if (type_str == "hash") {
    bool wrongtype = false;
    auto fields = ds.hgetall(key, wrongtype);
    if (wrongtype) return {};
    w.write_u8(RDB_TYPE_HASH);
    w.write_length(fields.size());
    for (const auto& [f, v] : fields) {
      w.write_string(f);
      w.write_string(v);
    }

  } else if (type_str == "zset") {
    bool wrongtype = false;
    auto items = ds.zrange(key, 0, -1, true, wrongtype);
    if (wrongtype) return {};
    w.write_u8(RDB_TYPE_ZSET_2);
    w.write_length(items.size());
    for (const auto& [m, s] : items) {
      w.write_string(m);
      w.write_double_string(s);
    }
  } else if (type_str == "stream") {
    DataStore::StreamDump d;
    if (!ds.dump_stream(key, d)) return {};
    w.write_u8(RDB_TYPE_STREAM_LISTPACKS_3);
    write_stream(w, d);
  } else {
    return {};
  }

  // Redis DUMP format: <RDB payload> <rdb-version:LE u16> <crc64:LE u64>
  return finish_dump_payload(w.data());
}

std::string rdb_dump_functions(const std::vector<std::string>& library_codes) {
  // Redis's functionDumpCommand(): rdbSaveFunctions() (one FUNCTION2 opcode and source string
  // per library), then the DUMP footer.
  RdbWriter w;
  for (const auto& code : library_codes) {
    w.write_u8(RDB_OPCODE_FUNCTION2);
    w.write_string(code);
  }
  return finish_dump_payload(w.data());
}

bool rdb_parse_function_payload(const std::string& payload, std::vector<std::string>& library_codes,
                                std::string& err) {
  // Redis's functionRestoreCommand(), up to loading the libraries, with its error texts.
  library_codes.clear();
  if (!verify_dump_payload(payload)) {
    err = "DUMP payload version or checksum are wrong";
    return false;
  }
  RdbReader r(payload.data(), payload.size() - 10);
  while (!r.eof()) {
    uint8_t type = 0;
    if (!r.read_u8(type)) {
      err = "can not read data type";
      return false;
    }
    if (type == RDB_OPCODE_FUNCTION_PRE_GA) {
      err = "Pre-GA function format not supported";
      return false;
    }
    if (type != RDB_OPCODE_FUNCTION2) {
      err = "given type is not a function";
      return false;
    }
    std::string code;
    if (!r.read_string(code)) {
      err = "Failed loading library payload";
      return false;
    }
    library_codes.push_back(std::move(code));
  }
  return true;
}

bool rdb_decode_dump_payload(const std::string& payload, DumpValue& out, std::string& err) {
  err.clear();
  out = DumpValue{};
  // DUMP format: <type+data> <rdb-ver:u16 LE> <crc64:u64 LE>
  if (!verify_dump_payload(payload)) {
    err = "DUMP payload version or checksum are wrong";
    return false;
  }
  // Every other failure is Redis's one reply for a payload rdbLoadObject() cannot read; like it,
  // an empty collection, a set member, hash field or zset member twice, and a NaN score fail too.
  err = "Bad data format";
  RdbReader r(payload.data(), payload.size() - 10);
  uint8_t type;
  if (!r.read_u8(type) || !read_dump_value(r, type, out)) return false;
  err.clear();
  return true;
}

void rdb_store_dump_value(const std::string& key, DumpValue&& value,
                          std::optional<int64_t> expire_at_ms) {
  auto& ds = store();
  bool wrongtype = false;
  if (value.type == RDB_TYPE_STRING) {
    ds.set(key, value.str, false, false, expire_at_ms, false);
    return;
  }
  if (value.type == RDB_TYPE_LIST) {
    ds.rpush(key, value.items, wrongtype);
  } else if (value.type == RDB_TYPE_SET) {
    ds.sadd(key, value.items, wrongtype);
  } else if (value.type == RDB_TYPE_HASH) {
    ds.hset(key, value.fields, wrongtype);
  } else if (value.type == RDB_TYPE_STREAM_LISTPACKS || value.type == RDB_TYPE_STREAM_LISTPACKS_2 ||
             value.type == RDB_TYPE_STREAM_LISTPACKS_3) {
    ds.restore_stream(key, value.stream);
  } else {
    for (const auto& [member, score] : value.scored)
      ds.zadd_one(key, score, member, false, false, false, false, false);
  }
  if (expire_at_ms) ds.expire(key, *expire_at_ms);
}

}  // namespace peadb
