// rdb.cpp — Redis-compatible RDB binary format reader/writer
//
// Implements the subset of the RDB format needed for PeaDB's data types:
//   - Strings (RDB_TYPE_STRING = 0)
//   - Lists (RDB_TYPE_LIST_QUICKLIST_2 = 18, RDB_TYPE_LIST = 1)
//   - Sets (RDB_TYPE_SET = 2, RDB_TYPE_SET_INTSET = 11)
//   - Hashes (RDB_TYPE_HASH = 4, RDB_TYPE_HASH_LISTPACK = 16)
//   - Sorted sets (RDB_TYPE_ZSET_2 = 5, RDB_TYPE_ZSET_LISTPACK = 17)
//   - Streams (PeaDB's own type 200; Redis's stream types are not read, compat/delta.md item 5)
// The zipmap and ziplist encodings of Redis before 7.0 (types 9, 10, 12, 13, 14) are refused.
//
// References: Redis src/rdb.h, src/rdb.c

#include "rdb.hpp"
#include "datastore.hpp"
#include "logger.hpp"

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <cstdio>
#include <cstring>
#include <exception>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>

namespace peadb {

namespace {
bool g_skip_checksum_validation = false;  // DEBUG SET-SKIP-CHECKSUM-VALIDATION
bool g_rdb_checksum = true;               // rdbchecksum (startup-only, as in Redis)
int g_rdb_key_save_delay = 0;             // rdb-key-save-delay (hidden Redis setting, for tests)
FunctionDumpFn g_function_dump_hook;      // returns every function library's source
FunctionLoadFn g_function_load_hook;      // loads one library's source (idempotent REPLACE)
}  // namespace

void rdb_set_skip_checksum_validation(bool skip) { g_skip_checksum_validation = skip; }
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
constexpr uint8_t RDB_OPCODE_EXPIRETIME    = 253;
constexpr uint8_t RDB_OPCODE_SELECTDB      = 254;
constexpr uint8_t RDB_OPCODE_EOF           = 255;
constexpr uint8_t RDB_OPCODE_RESIZEDB      = 251;
constexpr uint8_t RDB_OPCODE_AUX           = 250;
constexpr uint8_t RDB_OPCODE_FUNCTION2     = 245;  // one string payload = one library's source
constexpr uint8_t RDB_OPCODE_FUNCTION_PRE_GA = 246;  // Redis 7.0 release-candidate format
constexpr uint8_t RDB_OPCODE_IDLE          = 248;  // LRU idle seconds of the next key
constexpr uint8_t RDB_OPCODE_FREQ          = 249;  // LFU counter of the next key

constexpr uint8_t RDB_TYPE_STRING   = 0;
constexpr uint8_t RDB_TYPE_LIST     = 1;
constexpr uint8_t RDB_TYPE_SET      = 2;
constexpr uint8_t RDB_TYPE_ZSET     = 3;
constexpr uint8_t RDB_TYPE_HASH     = 4;
constexpr uint8_t RDB_TYPE_ZSET_2   = 5;
constexpr uint8_t RDB_TYPE_HASH_ZIPMAP     = 9;
constexpr uint8_t RDB_TYPE_LIST_ZIPLIST    = 10;
constexpr uint8_t RDB_TYPE_SET_INTSET      = 11;
constexpr uint8_t RDB_TYPE_ZSET_ZIPLIST    = 12;
constexpr uint8_t RDB_TYPE_HASH_ZIPLIST    = 13;
constexpr uint8_t RDB_TYPE_LIST_QUICKLIST = 14;  // nodes are ziplists (Redis 3.2 to 6.2)
constexpr uint8_t RDB_TYPE_HASH_LISTPACK   = 16;
constexpr uint8_t RDB_TYPE_ZSET_LISTPACK   = 17;
constexpr uint8_t RDB_TYPE_LIST_QUICKLIST_2= 18;
constexpr uint8_t RDB_TYPE_SET_LISTPACK    = 20;
// PeaDB-specific stream encoding (NOT Redis byte-compatible — Redis can't load an RDB
// containing this type; cross-engine stream migration still uses the AOF/bridge path).
// Chosen outside Redis's type range so non-stream RDBs stay fully Redis-compatible.
constexpr uint8_t RDB_TYPE_PEADB_STREAM       = 200;

constexpr uint8_t RDB_ENC_INT8  = 0;
constexpr uint8_t RDB_ENC_INT16 = 1;
constexpr uint8_t RDB_ENC_INT32 = 2;
constexpr uint8_t RDB_ENC_LZF   = 3;

constexpr int RDB_VERSION = 10;
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
  void write_u8(uint8_t v) { buf_.push_back(static_cast<char>(v)); }
  void write_u16_le(uint16_t v) {
    write_u8(v & 0xff);
    write_u8((v >> 8) & 0xff);
  }
  void write_u32_le(uint32_t v) {
    write_u8(v & 0xff); write_u8((v >> 8) & 0xff);
    write_u8((v >> 16) & 0xff); write_u8((v >> 24) & 0xff);
  }
  void write_u64_le(uint64_t v) {
    write_u32_le(static_cast<uint32_t>(v));
    write_u32_le(static_cast<uint32_t>(v >> 32));
  }
  void write_raw(const void* p, size_t n) {
    buf_.append(reinterpret_cast<const char*>(p), n);
  }
  void write_raw(const std::string& s) { buf_ += s; }

  // Redis "length" encoding
  void write_length(uint64_t len) {
    if (len < 64) {
      write_u8(static_cast<uint8_t>(len)); // 00xxxxxx
    } else if (len < 16384) {
      write_u8(static_cast<uint8_t>(0x40 | (len >> 8))); // 01xxxxxx
      write_u8(static_cast<uint8_t>(len & 0xff));
    } else if (len <= 0xFFFFFFFF) {
      write_u8(0x80); // 10000000
      uint32_t be = htonl(static_cast<uint32_t>(len));
      write_raw(&be, 4);
    } else {
      write_u8(0x81); // 10000001
      uint64_t v = len;
      // network byte order (big-endian)
      for (int i = 7; i >= 0; --i) write_u8(static_cast<uint8_t>((v >> (i * 8)) & 0xff));
    }
  }

  // Redis string encoding: try integer encoding first, else raw
  void write_string(const std::string& s) {
    // Try integer encoding for small ints
    if (!s.empty() && s.size() <= 20) {
      try {
        std::int64_t val = std::stoll(s);
        // Verify it's exact (no leading zeros, etc.)
        if (std::to_string(val) == s) {
          if (val >= -128 && val <= 127) {
            write_u8(0xC0 | RDB_ENC_INT8); // 11 000000
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
      } catch (...) {}
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

  const std::string& data() const { return buf_; }
  void clear() { buf_.clear(); }

 private:
  std::string buf_;
};

// ── Binary reader ──────────────────────────────────────────────────────

class RdbReader {
 public:
  RdbReader(const std::string& data) : data_(data), pos_(0) {}
  RdbReader(const char* p, size_t n) : data_(p, n), pos_(0) {}

  bool ok() const { return pos_ <= data_.size(); }
  bool eof() const { return pos_ >= data_.size(); }
  size_t pos() const { return pos_; }
  size_t remaining() const { return data_.size() - pos_; }

  bool read_u8(uint8_t& v) {
    if (pos_ >= data_.size()) return false;
    v = static_cast<uint8_t>(data_[pos_++]);
    return true;
  }

  bool read_u16_le(uint16_t& v) {
    if (remaining() < 2) return false;
    v = static_cast<uint16_t>(static_cast<uint8_t>(data_[pos_])) |
        (static_cast<uint16_t>(static_cast<uint8_t>(data_[pos_ + 1])) << 8);
    pos_ += 2;
    return true;
  }

  bool read_u32_le(uint32_t& v) {
    if (remaining() < 4) return false;
    v = static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_])) |
        (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 1])) << 8) |
        (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 2])) << 16) |
        (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 3])) << 24);
    pos_ += 4;
    return true;
  }

  bool read_u32_be(uint32_t& v) {
    if (remaining() < 4) return false;
    v = (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_])) << 24) |
        (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 1])) << 16) |
        (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 2])) << 8) |
        static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 3]));
    pos_ += 4;
    return true;
  }

  bool read_u64_le(uint64_t& v) {
    if (remaining() < 8) return false;
    uint32_t lo, hi;
    read_u32_le(lo);
    read_u32_le(hi);
    v = static_cast<uint64_t>(lo) | (static_cast<uint64_t>(hi) << 32);
    return true;
  }

  bool read_u64_be(uint64_t& v) {
    if (remaining() < 8) return false;
    v = 0;
    for (int i = 7; i >= 0; --i) {
      v |= static_cast<uint64_t>(static_cast<uint8_t>(data_[pos_++])) << (i * 8);
    }
    return true;
  }

  bool read_bytes(std::string& out, size_t n) {
    if (remaining() < n) return false;
    out.assign(data_, pos_, n);
    pos_ += n;
    return true;
  }

  bool skip(size_t n) {
    if (remaining() < n) return false;
    pos_ += n;
    return true;
  }

  // Read Redis length encoding. Returns (length, is_encoded).
  bool read_length(uint64_t& len, bool& is_encoded) {
    is_encoded = false;
    uint8_t byte;
    if (!read_u8(byte)) return false;
    uint8_t type = (byte >> 6) & 0x03;
    switch (type) {
      case 0: // 00xxxxxx
        len = byte & 0x3f;
        return true;
      case 1: { // 01xxxxxx
        uint8_t next;
        if (!read_u8(next)) return false;
        len = ((static_cast<uint64_t>(byte) & 0x3f) << 8) | next;
        return true;
      }
      case 2: { // 10xxxxxx
        if ((byte & 0x3f) == 0) {
          // 10000000 — 4-byte big-endian
          uint32_t v;
          if (!read_u32_be(v)) return false;
          len = v;
          return true;
        } else if ((byte & 0x3f) == 1) {
          // 10000001 — 8-byte big-endian
          if (!read_u64_be(len)) return false;
          return true;
        }
        return false;
      }
      case 3: // 11xxxxxx — special encoding
        is_encoded = true;
        len = byte & 0x3f;
        return true;
    }
    return false;
  }

  // Read a Redis-encoded string
  bool read_string(std::string& out) {
    uint64_t len;
    bool is_encoded;
    if (!read_length(len, is_encoded)) return false;
    if (is_encoded) {
      switch (len) {
        case RDB_ENC_INT8: {
          uint8_t v;
          if (!read_u8(v)) return false;
          out = std::to_string(static_cast<int8_t>(v));
          return true;
        }
        case RDB_ENC_INT16: {
          uint16_t v;
          if (!read_u16_le(v)) return false;
          out = std::to_string(static_cast<int16_t>(v));
          return true;
        }
        case RDB_ENC_INT32: {
          uint32_t v;
          if (!read_u32_le(v)) return false;
          out = std::to_string(static_cast<int32_t>(v));
          return true;
        }
        case RDB_ENC_LZF: {
          // LZF compressed string
          uint64_t clen, ulen;
          bool enc2;
          if (!read_length(clen, enc2) || enc2) return false;
          if (!read_length(ulen, enc2) || enc2) return false;
          std::string compressed;
          if (!read_bytes(compressed, static_cast<size_t>(clen))) return false;
          // Simple LZF decompression
          out.resize(static_cast<size_t>(ulen));
          if (!lzf_decompress(compressed.data(), clen, &out[0], ulen)) return false;
          return true;
        }
        default:
          return false;
      }
    }
    return read_bytes(out, static_cast<size_t>(len));
  }

  bool read_double_binary(double& v) {
    uint64_t bits;
    if (!read_u64_le(bits)) return false;
    std::memcpy(&v, &bits, 8);
    return true;
  }

  // Old-style double: length-prefixed string or special bytes
  bool read_double_old(double& v) {
    uint8_t len;
    if (!read_u8(len)) return false;
    if (len == 255) { v = -std::numeric_limits<double>::infinity(); return true; }
    if (len == 254) { v = std::numeric_limits<double>::infinity(); return true; }
    if (len == 253) { v = std::numeric_limits<double>::quiet_NaN(); return true; }
    std::string s;
    if (!read_bytes(s, len)) return false;
    try { v = std::stod(s); } catch (...) { return false; }
    return true;
  }

 private:
  bool lzf_decompress(const char* in, size_t in_len, char* out, size_t out_len) {
    size_t ip = 0, op = 0;
    while (ip < in_len) {
      uint8_t ctrl = static_cast<uint8_t>(in[ip++]);
      if (ctrl < 32) {
        // Literal run
        size_t len = static_cast<size_t>(ctrl) + 1;
        if (ip + len > in_len || op + len > out_len) return false;
        std::memcpy(out + op, in + ip, len);
        ip += len;
        op += len;
      } else {
        // Back-reference
        size_t len = (ctrl >> 5) + 2;
        if (len == 9) { // 7 + 2
          if (ip >= in_len) return false;
          len += static_cast<uint8_t>(in[ip++]);
        }
        size_t ref = ((static_cast<size_t>(ctrl) & 0x1f) << 8);
        if (ip >= in_len) return false;
        ref += static_cast<uint8_t>(in[ip++]);
        ref += 1;
        if (op < ref || op + len > out_len) return false;
        // Copy byte-by-byte (may overlap)
        for (size_t i = 0; i < len; ++i) out[op + i] = out[op - ref + i];
        op += len;
      }
    }
    return op == out_len;
  }

  std::string data_;
  size_t pos_;
};

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
    // Encode as string for simplicity
    add_string(std::to_string(v));
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
  ListpackReader(const std::string& data) : data_(data), pos_(0) {}

  // Reads every entry, with the checks of Redis's lpValidateIntegrity() in its deep mode: the
  // total-bytes field is the blob's size, every entry and its backlen lie inside the blob and the
  // backlen matches the entry, and the entries end at the 0xFF in the last byte. False means a
  // corrupt listpack. The entry count is not checked, as in Redis with sanitize-dump-payload no
  // (the default): the entries up to the 0xFF are what is read. So a list saved by PeaDB before
  // compat/delta.md item 29 was fixed, whose count was cut to 16 bits, loads with all its items.
  bool parse(std::vector<std::string>& out) {
    out.clear();
    if (data_.size() < kListpackHeaderAndEnd) return false;
    if (read_u32_le_at(0) != data_.size()) return false;
    if (static_cast<uint8_t>(data_.back()) != 0xFF) return false;
    const uint16_t num_entries = read_u16_le_at(4);
    if (num_entries != kListpackCountUnknown) out.reserve(num_entries);
    pos_ = 6;
    while (pos_ < data_.size() - 1) {
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
    auto need = [&](size_t nbytes) { return pos_ + nbytes <= data_.size(); };

    if ((enc & 0x80) == 0) {                       // 0xxxxxxx : 7-bit unsigned int
      val = std::to_string(enc & 0x7f);
      entry_len = 1;
    } else if ((enc & 0xC0) == 0x80) {             // 10xxxxxx : 6-bit string
      uint32_t slen = enc & 0x3f;
      if (!need(1 + slen)) return false;
      val.assign(data_, pos_ + 1, slen);
      entry_len = 1 + slen;
    } else if ((enc & 0xE0) == 0xC0) {             // 110xxxxx xxxxxxxx : 13-bit signed int
      if (!need(2)) return false;
      int16_t v = static_cast<int16_t>(((static_cast<uint16_t>(enc & 0x1f)) << 8) |
                                       static_cast<uint8_t>(data_[pos_ + 1]));
      if (v & 0x1000) v |= static_cast<int16_t>(0xE000);  // sign-extend from 13 bits
      val = std::to_string(v);
      entry_len = 2;
    } else if ((enc & 0xF0) == 0xE0) {             // 1110xxxx xxxxxxxx : 12-bit string
      if (!need(2)) return false;
      uint32_t slen = ((static_cast<uint32_t>(enc & 0x0f)) << 8) |
                      static_cast<uint8_t>(data_[pos_ + 1]);
      if (!need(2 + slen)) return false;
      val.assign(data_, pos_ + 2, slen);
      entry_len = 2 + slen;
    } else if (enc == 0xF0) {                      // 11110000 : 32-bit LE length string
      if (!need(5)) return false;
      uint32_t slen = static_cast<uint8_t>(data_[pos_ + 1]) |
                      (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 2])) << 8) |
                      (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 3])) << 16) |
                      (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 4])) << 24);
      if (!need(static_cast<size_t>(5) + slen)) return false;
      val.assign(data_, pos_ + 5, slen);
      entry_len = 5 + slen;
    } else if (enc == 0xF1) {                      // 16-bit signed int (LE)
      if (!need(3)) return false;
      int16_t v = static_cast<int16_t>(static_cast<uint8_t>(data_[pos_ + 1]) |
                                       (static_cast<uint16_t>(static_cast<uint8_t>(data_[pos_ + 2])) << 8));
      val = std::to_string(v);
      entry_len = 3;
    } else if (enc == 0xF2) {                      // 24-bit signed int (LE)
      if (!need(4)) return false;
      int32_t v = static_cast<uint8_t>(data_[pos_ + 1]) |
                  (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 2])) << 8) |
                  (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 3])) << 16);
      if (v & 0x800000) v |= static_cast<int32_t>(0xFF000000u);  // sign-extend from 24 bits
      val = std::to_string(v);
      entry_len = 4;
    } else if (enc == 0xF3) {                      // 32-bit signed int (LE)
      if (!need(5)) return false;
      int32_t v = static_cast<int32_t>(static_cast<uint8_t>(data_[pos_ + 1]) |
                  (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 2])) << 8) |
                  (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 3])) << 16) |
                  (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 4])) << 24));
      val = std::to_string(v);
      entry_len = 5;
    } else if (enc == 0xF4) {                      // 64-bit signed int (LE)
      if (!need(9)) return false;
      uint64_t u = 0;
      for (int i = 0; i < 8; ++i) u |= static_cast<uint64_t>(static_cast<uint8_t>(data_[pos_ + 1 + i])) << (8 * i);
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

} // anonymous namespace

// ── RDB Save ───────────────────────────────────────────────────────────

namespace {
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

// `key_save_delay`: wait rdb-key-save-delay after each key, as Redis's rdbSaveKeyValuePair() does.
// Only the file saves wait: Redis builds a full sync in a child, but PeaDB builds it on the event
// loop (compat/delta.md item 46), where a wait would stop every client.
std::string rdb_build_string(std::string& err, bool key_save_delay) {
  err.clear();
  auto& ds = store();

  RdbWriter w;

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

  // Function libraries (server-level, not per-db) — one FUNCTION2 opcode + string per library,
  // written after AUX and before the keyspace, matching Redis's RDB layout.
  if (g_function_dump_hook) {
    for (const auto& code : g_function_dump_hook()) {
      w.write_u8(RDB_OPCODE_FUNCTION2);
      w.write_string(code);
    }
  }

  // Iterate databases
  // We need access to internals. Use a temporary approach:
  // Export via the existing data access methods
  const int prev_db = ds.current_db();

  for (int dbi = 0; dbi < 16; ++dbi) {
    ds.select_db(dbi);
    auto all_keys = ds.keys("*");
    if (all_keys.empty()) continue;

    w.write_u8(RDB_OPCODE_SELECTDB);
    w.write_length(static_cast<uint64_t>(dbi));

    // RESIZEDB
    w.write_u8(RDB_OPCODE_RESIZEDB);
    w.write_length(all_keys.size());
    // Count keys with expire
    size_t expire_count = 0;
    for (const auto& k : all_keys) {
      if (ds.pttl(k) >= 0) ++expire_count;
    }
    w.write_length(expire_count);

    for (const auto& key : all_keys) {
      struct DelayAfterKey {
        int usec;
        ~DelayAfterKey() {
          if (usec != 0) debug_delay(usec);
        }
      } delay_after_key{key_save_delay ? g_rdb_key_save_delay : 0};
      // Expire
      auto pttl_val = ds.pexpiretime(key);
      if (pttl_val >= 0) {
        w.write_u8(RDB_OPCODE_EXPIRETIME_MS);
        w.write_u64_le(static_cast<uint64_t>(pttl_val));
      }

      const auto type_str = ds.type_of(key);

      if (type_str == "string") {
        auto val = ds.get(key);
        if (!val.has_value()) continue;
        w.write_u8(RDB_TYPE_STRING);
        w.write_string(key);
        w.write_string(*val);

      } else if (type_str == "list") {
        bool wrongtype = false;
        auto items = ds.lrange(key, 0, -1, wrongtype);
        if (wrongtype) continue;
        w.write_u8(RDB_TYPE_LIST_QUICKLIST_2);
        w.write_string(key);
        write_quicklist(w, items, ds.encoding_limits().list_size);

      } else if (type_str == "set") {
        bool wrongtype = false;
        auto members = ds.smembers(key, wrongtype);
        if (wrongtype) continue;
        w.write_u8(RDB_TYPE_SET);
        w.write_string(key);
        w.write_length(members.size());
        for (const auto& m : members) w.write_string(m);

      } else if (type_str == "hash") {
        bool wrongtype = false;
        auto fields = ds.hgetall(key, wrongtype);
        if (wrongtype) continue;
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
        if (wrongtype || items.empty()) continue;
        w.write_u8(RDB_TYPE_ZSET_2);
        w.write_string(key);
        w.write_length(items.size());
        for (const auto& [m, s] : items) {
          w.write_string(m);
          w.write_double_string(s);
        }

      } else if (type_str == "stream") {
        DataStore::StreamDump d;
        if (!ds.dump_stream(key, d)) continue;
        w.write_u8(RDB_TYPE_PEADB_STREAM);
        w.write_string(key);
        w.write_length(d.entries.size());
        for (const auto& [id, fields] : d.entries) {
          w.write_string(id);
          w.write_length(fields.size());
          for (const auto& [f, v] : fields) { w.write_string(f); w.write_string(v); }
        }
        w.write_length(d.last_ms);
        w.write_length(d.last_seq);
        w.write_length(d.groups.size());
        for (const auto& gd : d.groups) {
          w.write_string(gd.name);
          w.write_string(gd.last_delivered_id);
          w.write_length(gd.pending.size());
          for (const auto& p : gd.pending) {
            w.write_string(p.id);
            w.write_string(p.consumer);
            w.write_length(static_cast<uint64_t>(p.delivery_ms));
            w.write_length(static_cast<uint64_t>(p.delivery_count));
          }
        }
      }
    }
  }
  ds.select_db(prev_db);

  // EOF marker
  w.write_u8(RDB_OPCODE_EOF);

  // CRC64 of everything before it; with rdbchecksum no, Redis writes 0 ("no checksum") instead.
  const uint64_t checksum = g_rdb_checksum ? crc64(0, w.data().data(), w.data().size()) : 0;
  w.write_u64_le(checksum);

  return w.data();
}
} // end anonymous namespace

bool rdb_save(const std::filesystem::path& path, std::string& err) {
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
  std::string data;
  try {
    data = rdb_build_string(err, /*key_save_delay=*/true);
  } catch (const std::exception& e) {  // e.g. out of memory: the whole file is built in memory
    err = std::string("Error saving DB: ") + e.what();
  }
  if (!err.empty()) {
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
  std::size_t written = 0;
  while (written < data.size()) {
    const ssize_t rc = ::write(fd, data.data() + written, data.size() - written);
    if (rc < 0 && errno == EINTR) continue;
    if (rc <= 0) {
      if (rc == 0) errno = EIO;
      return write_error();
    }
    written += static_cast<std::size_t>(rc);
  }
  // Fail fast on fsync error: a successful rename of an unfsynced temp file
  // can leave the new RDB with stale or torn contents after a power loss.
  if (::fsync(fd) != 0) return write_error();
  ::close(fd);

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

std::string rdb_save_to_string(std::string& err) {
  return rdb_build_string(err, /*key_save_delay=*/false);
}

// ── RDB Load ───────────────────────────────────────────────────────────

bool rdb_load(const std::filesystem::path& path, std::string& err,
              const std::function<void()>& before_replace) {
  err.clear();

  std::ifstream in(path, std::ios::binary);
  if (!in.is_open()) {
    err = "cannot open RDB file";
    return false;
  }
  std::string filedata((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
  in.close();

  if (filedata.size() < 9) {
    err = "RDB file too short";
    return false;
  }

  // Check magic
  if (filedata.substr(0, 5) != "REDIS") {
    err = "invalid RDB magic";
    return false;
  }

  int rdb_ver = 0;
  try { rdb_ver = std::stoi(filedata.substr(5, 4)); } catch (...) {
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
    const auto* tail = reinterpret_cast<const unsigned char*>(filedata.data() + filedata.size() - 8);
    for (int i = 0; i < 8; ++i) stored |= static_cast<uint64_t>(tail[i]) << (8 * i);
    if (stored == 0) {
      log(LogLevel::Info, "RDB file was saved with checksum disabled: no check performed.");
    } else {
      const uint64_t computed = crc64(0, filedata.data(), filedata.size() - 8);
      if (computed != stored) {
        char msg[128];
        std::snprintf(
            msg, sizeof(msg), "Wrong RDB checksum expected: (%llx) but got (%llx). Aborting now.",
            static_cast<unsigned long long>(computed), static_cast<unsigned long long>(stored));
        err = msg;
        return false;
      }
    }
  }

  RdbReader r(filedata.data() + 9, filedata.size() - 9);
  auto& ds = store();
  if (before_replace) before_replace();
  ds.flushall();
  const int prev_db = ds.current_db();
  ds.select_db(0);

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
        ds.select_db(prev_db);
        return false;
      }
      continue;
    }
    if (type == RDB_OPCODE_FREQ) {
      uint8_t freq;
      if (!r.read_u8(freq)) {
        err = "malformed FREQ";
        ds.select_db(prev_db);
        return false;
      }
      continue;
    }

    if (type == RDB_OPCODE_AUX) {
      std::string aux_key, aux_val;
      if (!r.read_string(aux_key) || !r.read_string(aux_val)) {
        err = "malformed AUX field";
        ds.select_db(prev_db);
        return false;
      }
      continue;
    }

    if (type == RDB_OPCODE_FUNCTION2) {
      std::string code;
      if (!r.read_string(code)) {
        err = "malformed FUNCTION payload";
        ds.select_db(prev_db);
        return false;
      }
      if (g_function_load_hook && !g_function_load_hook(code, err)) {
        ds.select_db(prev_db);
        return false;
      }
      continue;
    }

    if (type == RDB_OPCODE_SELECTDB) {
      uint64_t dbnum;
      bool enc;
      if (!r.read_length(dbnum, enc) || enc) {
        err = "malformed SELECTDB";
        ds.select_db(prev_db);
        return false;
      }
      ds.select_db(static_cast<int>(dbnum));
      continue;
    }

    if (type == RDB_OPCODE_RESIZEDB) {
      uint64_t db_size, exp_size;
      bool enc1, enc2;
      if (!r.read_length(db_size, enc1) || enc1 ||
          !r.read_length(exp_size, enc2) || enc2) {
        err = "malformed RESIZEDB";
        ds.select_db(prev_db);
        return false;
      }
      continue;
    }

    if (type == RDB_OPCODE_EXPIRETIME_MS) {
      uint64_t ms;
      if (!r.read_u64_le(ms)) {
        err = "malformed EXPIRETIME_MS";
        ds.select_db(prev_db);
        return false;
      }
      pending_expire = static_cast<int64_t>(ms);
      continue;
    }

    if (type == RDB_OPCODE_EXPIRETIME) {
      uint32_t sec;
      if (!r.read_u32_le(sec)) {
        err = "malformed EXPIRETIME";
        ds.select_db(prev_db);
        return false;
      }
      pending_expire = static_cast<int64_t>(sec) * 1000;
      continue;
    }

    // It's a value type — read key
    std::string key;
    if (!r.read_string(key)) {
      err = "malformed key";
      ds.select_db(prev_db);
      return false;
    }

    std::optional<int64_t> expire_at = pending_expire;
    pending_expire.reset();

    if (type == RDB_TYPE_STRING) {
      std::string val;
      if (!r.read_string(val)) {
        err = "malformed string value";
        ds.select_db(prev_db);
        return false;
      }
      ds.set(key, val, false, false, expire_at, false);

    } else if (type == RDB_TYPE_LIST) {
      // Encoded list (linked list of strings)
      uint64_t len;
      bool enc;
      if (!r.read_length(len, enc) || enc) { err = "malformed list"; ds.select_db(prev_db); return false; }
      std::vector<std::string> items;
      for (uint64_t i = 0; i < len; ++i) {
        std::string val;
        if (!r.read_string(val)) { err = "malformed list item"; ds.select_db(prev_db); return false; }
        items.push_back(std::move(val));
      }
      bool wt = false;
      ds.rpush(key, items, wt);
      if (expire_at) ds.expire(key, *expire_at);

    } else if (type == RDB_TYPE_LIST_QUICKLIST_2) {
      // Redis's rdbLoadObject(): the node count, then per node its container and a string. A
      // PLAIN node is one element as it is, a PACKED node a listpack of elements; any other
      // container, an empty string or a listpack that fails the checks is a corrupt file.
      // Empty listpacks are skipped, and a list with no element is not loaded, as in Redis.
      uint64_t node_count;
      bool enc;
      if (!r.read_length(node_count, enc) || enc) { err = "malformed quicklist"; ds.select_db(prev_db); return false; }
      std::vector<std::string> all_items;
      for (uint64_t n = 0; n < node_count; ++n) {
        uint64_t container;
        bool enc2;
        if (!r.read_length(container, enc2) || enc2) {
          err = "malformed quicklist2 container";
          ds.select_db(prev_db);
          return false;
        }
        if (container != kQuicklistNodePlain && container != kQuicklistNodePacked) {
          err = "Quicklist integrity check failed.";
          ds.select_db(prev_db);
          return false;
        }
        std::string node_data;
        if (!r.read_string(node_data) || node_data.empty()) {
          err = "malformed quicklist node";
          ds.select_db(prev_db);
          return false;
        }
        if (container == kQuicklistNodePlain) {
          all_items.push_back(std::move(node_data));
          continue;
        }
        ListpackReader lpr(node_data);
        std::vector<std::string> entries;
        if (!lpr.parse(entries)) {
          err = "Listpack integrity check failed.";
          ds.select_db(prev_db);
          return false;
        }
        for (auto& e : entries) all_items.push_back(std::move(e));
      }
      if (!all_items.empty()) {
        bool wt = false;
        ds.rpush(key, all_items, wt);
      }
      if (expire_at) ds.expire(key, *expire_at);

    } else if (type == RDB_TYPE_HASH_ZIPMAP || type == RDB_TYPE_LIST_ZIPLIST ||
               type == RDB_TYPE_ZSET_ZIPLIST || type == RDB_TYPE_HASH_ZIPLIST ||
               type == RDB_TYPE_LIST_QUICKLIST) {
      // Encodings that only Redis before 7.0 writes: a zipmap, a ziplist, or a quicklist whose
      // nodes are ziplists. PeaDB has no zipmap or ziplist reader yet (compat/delta.md item 53),
      // so the load stops instead of reading them as listpacks, which gave wrong data.
      err =
          "unsupported RDB type " + std::to_string(type) +
          " (an encoding written by Redis before 7.0: load the file in Redis 7 and save it again)";
      ds.select_db(prev_db);
      return false;

    } else if (type == RDB_TYPE_SET) {
      uint64_t len;
      bool enc;
      if (!r.read_length(len, enc) || enc) { err = "malformed set"; ds.select_db(prev_db); return false; }
      std::vector<std::string> members;
      for (uint64_t i = 0; i < len; ++i) {
        std::string m;
        if (!r.read_string(m)) { err = "malformed set member"; ds.select_db(prev_db); return false; }
        members.push_back(std::move(m));
      }
      bool wt = false;
      ds.sadd(key, members, wt);
      if (expire_at) ds.expire(key, *expire_at);

    } else if (type == RDB_TYPE_SET_INTSET) {
      std::string blob;
      if (!r.read_string(blob)) { err = "malformed intset"; ds.select_db(prev_db); return false; }
      // Intset format: encoding(u32) length(u32) entries...
      if (blob.size() >= 8) {
        uint32_t encoding = static_cast<uint32_t>(static_cast<uint8_t>(blob[0])) |
                           (static_cast<uint32_t>(static_cast<uint8_t>(blob[1])) << 8) |
                           (static_cast<uint32_t>(static_cast<uint8_t>(blob[2])) << 16) |
                           (static_cast<uint32_t>(static_cast<uint8_t>(blob[3])) << 24);
        uint32_t num = static_cast<uint32_t>(static_cast<uint8_t>(blob[4])) |
                      (static_cast<uint32_t>(static_cast<uint8_t>(blob[5])) << 8) |
                      (static_cast<uint32_t>(static_cast<uint8_t>(blob[6])) << 16) |
                      (static_cast<uint32_t>(static_cast<uint8_t>(blob[7])) << 24);
        std::vector<std::string> members;
        size_t entry_size = (encoding == 2) ? 2 : (encoding == 4) ? 4 : 8;
        for (uint32_t i = 0; i < num; ++i) {
          size_t off = 8 + i * entry_size;
          if (off + entry_size > blob.size()) break;
          int64_t val = 0;
          if (entry_size == 2) {
            val = static_cast<int16_t>(static_cast<uint16_t>(static_cast<uint8_t>(blob[off])) |
                  (static_cast<uint16_t>(static_cast<uint8_t>(blob[off + 1])) << 8));
          } else if (entry_size == 4) {
            val = static_cast<int32_t>(static_cast<uint32_t>(static_cast<uint8_t>(blob[off])) |
                  (static_cast<uint32_t>(static_cast<uint8_t>(blob[off + 1])) << 8) |
                  (static_cast<uint32_t>(static_cast<uint8_t>(blob[off + 2])) << 16) |
                  (static_cast<uint32_t>(static_cast<uint8_t>(blob[off + 3])) << 24));
          } else {
            uint64_t uv = 0;
            for (int b = 0; b < 8; ++b) uv |= static_cast<uint64_t>(static_cast<uint8_t>(blob[off + b])) << (b * 8);
            val = static_cast<int64_t>(uv);
          }
          members.push_back(std::to_string(val));
        }
        bool wt = false;
        ds.sadd(key, members, wt);
      }
      if (expire_at) ds.expire(key, *expire_at);

    } else if (type == RDB_TYPE_SET_LISTPACK) {
      std::string blob;
      if (!r.read_string(blob)) { err = "malformed set listpack"; ds.select_db(prev_db); return false; }
      ListpackReader lpr(blob);
      std::vector<std::string> entries;
      if (!lpr.parse(entries)) {
        err = "Set listpack integrity check failed.";
        ds.select_db(prev_db);
        return false;
      }
      if (!entries.empty()) { bool wt = false; ds.sadd(key, entries, wt); }
      if (expire_at) ds.expire(key, *expire_at);

    } else if (type == RDB_TYPE_HASH) {
      uint64_t len;
      bool enc;
      if (!r.read_length(len, enc) || enc) { err = "malformed hash"; ds.select_db(prev_db); return false; }
      std::vector<std::pair<std::string, std::string>> fields;
      for (uint64_t i = 0; i < len; ++i) {
        std::string f, v;
        if (!r.read_string(f) || !r.read_string(v)) { err = "malformed hash field"; ds.select_db(prev_db); return false; }
        fields.push_back({std::move(f), std::move(v)});
      }
      bool wt = false;
      ds.hset(key, fields, wt);
      if (expire_at) ds.expire(key, *expire_at);

    } else if (type == RDB_TYPE_HASH_LISTPACK) {
      std::string blob;
      if (!r.read_string(blob)) { err = "malformed hash listpack"; ds.select_db(prev_db); return false; }
      ListpackReader lpr(blob);
      std::vector<std::string> entries;
      if (!lpr.parse(entries) || entries.size() % 2 != 0) {
        err = "Hash listpack integrity check failed.";
        ds.select_db(prev_db);
        return false;
      }
      std::vector<std::pair<std::string, std::string>> fields;
      for (size_t i = 0; i + 1 < entries.size(); i += 2) {
        fields.push_back({entries[i], entries[i + 1]});
      }
      if (!fields.empty()) { bool wt = false; ds.hset(key, fields, wt); }
      if (expire_at) ds.expire(key, *expire_at);

    } else if (type == RDB_TYPE_ZSET || type == RDB_TYPE_ZSET_2) {
      uint64_t len;
      bool enc;
      if (!r.read_length(len, enc) || enc) { err = "malformed zset"; ds.select_db(prev_db); return false; }
      for (uint64_t i = 0; i < len; ++i) {
        std::string member;
        if (!r.read_string(member)) { err = "malformed zset member"; ds.select_db(prev_db); return false; }
        double score;
        if (type == RDB_TYPE_ZSET_2) {
          if (!r.read_double_binary(score)) { err = "malformed zset score"; ds.select_db(prev_db); return false; }
        } else {
          if (!r.read_double_old(score)) { err = "malformed zset score"; ds.select_db(prev_db); return false; }
        }
        ds.zadd_one(key, score, member, false, false, false, false, false);
      }
      if (expire_at) ds.expire(key, *expire_at);

    } else if (type == RDB_TYPE_ZSET_LISTPACK) {
      std::string blob;
      if (!r.read_string(blob)) { err = "malformed zset listpack"; ds.select_db(prev_db); return false; }
      ListpackReader lpr(blob);
      std::vector<std::string> entries;
      if (!lpr.parse(entries) || entries.size() % 2 != 0) {
        err = "Zset listpack integrity check failed.";
        ds.select_db(prev_db);
        return false;
      }
      for (size_t i = 0; i + 1 < entries.size(); i += 2) {
        double score = 0;
        try { score = std::stod(entries[i + 1]); } catch (...) {}
        ds.zadd_one(key, score, entries[i], false, false, false, false, false);
      }
      if (expire_at) ds.expire(key, *expire_at);

    } else if (type == RDB_TYPE_PEADB_STREAM) {
      DataStore::StreamDump d;
      uint64_t nentries = 0; bool enc = false;
      if (!r.read_length(nentries, enc) || enc) { err = "malformed stream"; ds.select_db(prev_db); return false; }
      for (uint64_t i = 0; i < nentries; ++i) {
        std::string id; if (!r.read_string(id)) { err = "malformed stream entry id"; ds.select_db(prev_db); return false; }
        uint64_t nf = 0; if (!r.read_length(nf, enc) || enc) { err = "malformed stream entry"; ds.select_db(prev_db); return false; }
        std::vector<std::pair<std::string, std::string>> fields;
        for (uint64_t j = 0; j < nf; ++j) {
          std::string f, v;
          if (!r.read_string(f) || !r.read_string(v)) { err = "malformed stream field"; ds.select_db(prev_db); return false; }
          fields.emplace_back(std::move(f), std::move(v));
        }
        d.entries.emplace_back(std::move(id), std::move(fields));
      }
      uint64_t lms = 0, lseq = 0;
      if (!r.read_length(lms, enc) || enc || !r.read_length(lseq, enc) || enc) { err = "malformed stream last-id"; ds.select_db(prev_db); return false; }
      d.last_ms = lms; d.last_seq = lseq;
      uint64_t ngroups = 0;
      if (!r.read_length(ngroups, enc) || enc) { err = "malformed stream groups"; ds.select_db(prev_db); return false; }
      for (uint64_t gi = 0; gi < ngroups; ++gi) {
        DataStore::StreamGroupDump gd;
        if (!r.read_string(gd.name) || !r.read_string(gd.last_delivered_id)) { err = "malformed stream group"; ds.select_db(prev_db); return false; }
        uint64_t npel = 0;
        if (!r.read_length(npel, enc) || enc) { err = "malformed stream pel"; ds.select_db(prev_db); return false; }
        for (uint64_t pi = 0; pi < npel; ++pi) {
          DataStore::StreamPel p;
          uint64_t dms = 0, dcnt = 0;
          if (!r.read_string(p.id) || !r.read_string(p.consumer) ||
              !r.read_length(dms, enc) || enc || !r.read_length(dcnt, enc) || enc) {
            err = "malformed stream pel entry"; ds.select_db(prev_db); return false;
          }
          p.delivery_ms = static_cast<std::int64_t>(dms);
          p.delivery_count = static_cast<std::int64_t>(dcnt);
          gd.pending.push_back(std::move(p));
        }
        d.groups.push_back(std::move(gd));
      }
      ds.restore_stream(key, d);
      if (expire_at) ds.expire(key, *expire_at);

    } else {
      // Unknown type — skip by reading and discarding a string
      // This may not work for all types but is best-effort
      err = "unsupported RDB type " + std::to_string(type);
      ds.select_db(prev_db);
      return false;
    }
  }

  ds.select_db(prev_db);
  if (!saw_eof || (rdb_ver >= 5 && r.remaining() < 8)) {
    err = "Short read or OOM loading DB. Unrecoverable error, aborting now.";
    return false;
  }
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

bool rdb_restore_key(const std::string& key, const std::string& payload,
                     int64_t ttl_ms, bool replace, bool absttl,
                     std::string& err) {
  err.clear();
  auto& ds = store();

  // Key-existence check comes FIRST (Redis restoreCommand): RESTORE onto an existing key
  // without REPLACE is BUSYKEY regardless of whether the payload is even well-formed.
  if (!replace && ds.type_of(key) != "none") {
    err = "Target key name already exists.";
    return false;
  }

  // DUMP format: <type+data> <rdb-ver:u16 LE> <crc64:u64 LE>
  if (!verify_dump_payload(payload)) {
    err = "DUMP payload version or checksum are wrong";
    return false;
  }

  // Strip the 2-byte version + 8-byte CRC
  size_t data_len = payload.size() - 10;
  RdbReader r(payload.data(), data_len);

  // Delete existing key if replacing
  if (replace) ds.del(key);

  std::optional<int64_t> expire_at;
  if (ttl_ms != 0) {
    if (absttl) {
      expire_at = ttl_ms;
    } else {
      expire_at = DataStore::now_ms() + ttl_ms;
    }
  }
  // An already-expired absolute TTL means the key must NOT be stored (Redis restoreCommand):
  // any existing key was already removed above by REPLACE; return OK without recreating it.
  if (expire_at && *expire_at <= DataStore::now_ms()) return true;

  uint8_t type;
  if (!r.read_u8(type)) {
    err = "Bad data format";
    return false;
  }

  if (type == RDB_TYPE_STRING) {
    std::string val;
    if (!r.read_string(val)) { err = "Bad data format"; return false; }
    ds.set(key, val, false, false, expire_at, false);

  } else if (type == RDB_TYPE_LIST) {
    uint64_t len;
    bool enc;
    if (!r.read_length(len, enc) || enc) { err = "Bad data format"; return false; }
    std::vector<std::string> items;
    for (uint64_t i = 0; i < len; ++i) {
      std::string val;
      if (!r.read_string(val)) { err = "Bad data format"; return false; }
      items.push_back(std::move(val));
    }
    bool wt = false;
    ds.rpush(key, items, wt);
    if (expire_at) ds.expire(key, *expire_at);

  } else if (type == RDB_TYPE_SET) {
    uint64_t len;
    bool enc;
    if (!r.read_length(len, enc) || enc) { err = "Bad data format"; return false; }
    std::vector<std::string> members;
    for (uint64_t i = 0; i < len; ++i) {
      std::string m;
      if (!r.read_string(m)) { err = "Bad data format"; return false; }
      members.push_back(std::move(m));
    }
    bool wt = false;
    ds.sadd(key, members, wt);
    if (expire_at) ds.expire(key, *expire_at);

  } else if (type == RDB_TYPE_HASH) {
    uint64_t len;
    bool enc;
    if (!r.read_length(len, enc) || enc) { err = "Bad data format"; return false; }
    std::vector<std::pair<std::string, std::string>> fields;
    for (uint64_t i = 0; i < len; ++i) {
      std::string f, v;
      if (!r.read_string(f) || !r.read_string(v)) { err = "Bad data format"; return false; }
      fields.push_back({std::move(f), std::move(v)});
    }
    bool wt = false;
    ds.hset(key, fields, wt);
    if (expire_at) ds.expire(key, *expire_at);

  } else if (type == RDB_TYPE_ZSET_2 || type == RDB_TYPE_ZSET) {
    uint64_t len;
    bool enc;
    if (!r.read_length(len, enc) || enc) { err = "Bad data format"; return false; }
    for (uint64_t i = 0; i < len; ++i) {
      std::string member;
      if (!r.read_string(member)) { err = "Bad data format"; return false; }
      double score;
      if (type == RDB_TYPE_ZSET_2) {
        if (!r.read_double_binary(score)) { err = "Bad data format"; return false; }
      } else {
        if (!r.read_double_old(score)) { err = "Bad data format"; return false; }
      }
      ds.zadd_one(key, score, member, false, false, false, false, false);
    }
    if (expire_at) ds.expire(key, *expire_at);

  } else {
    err = "Bad data format";
    return false;
  }

  return true;
}

} // namespace peadb
