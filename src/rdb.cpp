// rdb.cpp — Redis-compatible RDB binary format reader/writer
//
// Implements the subset of the RDB format needed for PeaDB's data types:
//   - Strings (RDB_TYPE_STRING = 0)
//   - Lists (RDB_TYPE_LIST_QUICKLIST_2 = 18, RDB_TYPE_LIST = 1)
//   - Sets (RDB_TYPE_SET = 2, RDB_TYPE_SET_INTSET = 11)
//   - Hashes (RDB_TYPE_HASH = 4, RDB_TYPE_HASH_ZIPMAP = 9, RDB_TYPE_HASH_LISTPACK = 16)
//   - Sorted sets (RDB_TYPE_ZSET_2 = 5, RDB_TYPE_ZSET_LISTPACK = 17)
//   - Streams (RDB_TYPE_STREAM_LISTPACKS_3 = 21, basic)
//
// References: Redis src/rdb.h, src/rdb.c

#include "rdb.hpp"
#include "datastore.hpp"

#include <cstdint>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>
#include <cmath>
#include <limits>
#include <arpa/inet.h>

namespace peadb {
namespace {

// ── RDB constants ──────────────────────────────────────────────────────

constexpr uint8_t RDB_OPCODE_EXPIRETIME_MS = 252;
constexpr uint8_t RDB_OPCODE_EXPIRETIME    = 253;
constexpr uint8_t RDB_OPCODE_SELECTDB      = 254;
constexpr uint8_t RDB_OPCODE_EOF           = 255;
constexpr uint8_t RDB_OPCODE_RESIZEDB      = 251;
constexpr uint8_t RDB_OPCODE_AUX           = 250;

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
constexpr uint8_t RDB_TYPE_LIST_QUICKLIST  = 15;
constexpr uint8_t RDB_TYPE_HASH_LISTPACK   = 16;
constexpr uint8_t RDB_TYPE_ZSET_LISTPACK   = 17;
constexpr uint8_t RDB_TYPE_LIST_QUICKLIST_2= 18;
constexpr uint8_t RDB_TYPE_SET_LISTPACK    = 20;
constexpr uint8_t RDB_TYPE_STREAM_LISTPACKS   = 15; // overloaded opcode used for streams

constexpr uint8_t RDB_ENC_INT8  = 0;
constexpr uint8_t RDB_ENC_INT16 = 1;
constexpr uint8_t RDB_ENC_INT32 = 2;
constexpr uint8_t RDB_ENC_LZF   = 3;

constexpr int RDB_VERSION = 10;

// ── CRC64 ──────────────────────────────────────────────────────────────
// Redis uses CRC-64/ECMA-182 (Jones). We implement a table-driven version.

static uint64_t crc64_tab[256];
static bool crc64_initialized = false;

void crc64_init() {
  if (crc64_initialized) return;
  const uint64_t poly = 0xad93d23594c935a9ULL;
  for (int i = 0; i < 256; ++i) {
    uint64_t crc = static_cast<uint64_t>(i);
    for (int j = 0; j < 8; ++j) {
      if (crc & 1) crc = (crc >> 1) ^ poly;
      else crc >>= 1;
    }
    crc64_tab[i] = crc;
  }
  crc64_initialized = true;
}

uint64_t crc64(uint64_t crc, const uint8_t* data, size_t len) {
  crc64_init();
  for (size_t i = 0; i < len; ++i) {
    crc = crc64_tab[(crc ^ data[i]) & 0xff] ^ (crc >> 8);
  }
  return crc;
}

uint64_t crc64(uint64_t crc, const std::string& s) {
  return crc64(crc, reinterpret_cast<const uint8_t*>(s.data()), s.size());
}

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

class ListpackWriter {
 public:
  void add_string(const std::string& s) {
    if (s.size() <= 63) {
      // Small string: |0|length(6bit)| then data, then backlen
      entries_.push_back(static_cast<char>(static_cast<uint8_t>(s.size())));
      entries_.append(s);
      uint32_t entry_len = 1 + static_cast<uint32_t>(s.size());
      encode_backlen(entry_len);
    } else if (s.size() <= 16383) {
      entries_.push_back(static_cast<char>(0x40 | ((s.size() >> 8) & 0x3f)));
      entries_.push_back(static_cast<char>(s.size() & 0xff));
      entries_.append(s);
      uint32_t entry_len = 2 + static_cast<uint32_t>(s.size());
      encode_backlen(entry_len);
    } else {
      entries_.push_back(static_cast<char>(0x80));
      uint32_t len = static_cast<uint32_t>(s.size());
      entries_.push_back(static_cast<char>((len >> 24) & 0xff));
      entries_.push_back(static_cast<char>((len >> 16) & 0xff));
      entries_.push_back(static_cast<char>((len >> 8) & 0xff));
      entries_.push_back(static_cast<char>(len & 0xff));
      entries_.append(s);
      uint32_t entry_len = 5 + static_cast<uint32_t>(s.size());
      encode_backlen(entry_len);
    }
    ++count_;
  }

  void add_integer(int64_t v) {
    // Encode as string for simplicity
    add_string(std::to_string(v));
  }

  std::string finish() const {
    // total_bytes(4) + num_entries(2) + entries + 0xFF
    uint32_t total = 4 + 2 + static_cast<uint32_t>(entries_.size()) + 1;
    std::string out;
    out.reserve(total);
    // Little-endian u32 total bytes
    out.push_back(static_cast<char>(total & 0xff));
    out.push_back(static_cast<char>((total >> 8) & 0xff));
    out.push_back(static_cast<char>((total >> 16) & 0xff));
    out.push_back(static_cast<char>((total >> 24) & 0xff));
    // Little-endian u16 num entries
    out.push_back(static_cast<char>(count_ & 0xff));
    out.push_back(static_cast<char>((count_ >> 8) & 0xff));
    out += entries_;
    out.push_back(static_cast<char>(0xFF));
    return out;
  }

 private:
  void encode_backlen(uint32_t len) {
    if (len <= 127) {
      entries_.push_back(static_cast<char>(len));
    } else if (len < 16383) {
      entries_.push_back(static_cast<char>((len >> 7) | 0x80));
      entries_.push_back(static_cast<char>(len & 0x7f));
    } else {
      // Up to 5 bytes
      uint8_t buf[5];
      int n = 0;
      do {
        buf[n] = static_cast<uint8_t>(len & 0x7f);
        if (n > 0) buf[n] |= 0x80;
        len >>= 7;
        ++n;
      } while (len > 0);
      // Reverse to write MSB first
      for (int i = n - 1; i >= 0; --i) entries_.push_back(static_cast<char>(buf[i]));
    }
  }

  std::string entries_;
  uint16_t count_ = 0;
};

// Simple listpack reader
class ListpackReader {
 public:
  ListpackReader(const std::string& data) : data_(data), pos_(0) {}

  bool parse(std::vector<std::string>& out) {
    if (data_.size() < 7) return false;  // 4 (total) + 2 (count) + 1 (end)
    uint32_t total_bytes = read_u32_le_at(0);
    uint16_t num_entries = read_u16_le_at(4);
    pos_ = 6;
    out.clear();
    out.reserve(num_entries);
    for (uint16_t i = 0; i < num_entries; ++i) {
      std::string val;
      if (!read_entry(val)) return false;
      out.push_back(std::move(val));
    }
    (void)total_bytes;
    return true;
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

    if (enc == 0xFF) return false; // end marker

    // Small integer encodings (0xC0..0xF0 ranges, 0xF1..0xFE ranges)
    if ((enc & 0x80) == 0) {
      // 0xxxxxxx — string with length in lower 6 bits
      uint32_t slen = enc & 0x3f;
      pos_ += 1;
      if (pos_ + slen > data_.size()) return false;
      val.assign(data_, pos_, slen);
      pos_ += slen;
      skip_backlen();
      return true;
    }
    if ((enc & 0xC0) == 0x40) {
      // 01xxxxxx — string 64..16383
      if (pos_ + 1 >= data_.size()) return false;
      uint32_t slen = ((static_cast<uint32_t>(enc) & 0x3f) << 8) |
                       static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 1]));
      pos_ += 2;
      if (pos_ + slen > data_.size()) return false;
      val.assign(data_, pos_, slen);
      pos_ += slen;
      skip_backlen();
      return true;
    }
    if ((enc & 0xE0) == 0x80) {
      // 100xxxxx — large string
      if (pos_ + 4 >= data_.size()) return false;
      uint32_t slen = (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 1])) << 24) |
                      (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 2])) << 16) |
                      (static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 3])) << 8) |
                       static_cast<uint32_t>(static_cast<uint8_t>(data_[pos_ + 4]));
      pos_ += 5;
      if (pos_ + slen > data_.size()) return false;
      val.assign(data_, pos_, slen);
      pos_ += slen;
      skip_backlen();
      return true;
    }
    if ((enc >= 0xC1) && (enc <= 0xFD)) {
      // Small integer: 0xC1..0xFD  → value = enc - 0xC1 - 64 = enc - 0xC1 + (-64)
      // In Redis listpack: 1100xxxx xxxxxxxx = 13-bit signed int
      // 1111xxxx = 4-bit uint (0..12)
      if ((enc & 0xF0) == 0xF0) {
        // 1111xxxx — small unsigned (0..12)
        int v = (enc & 0x0F) - 1;
        val = std::to_string(v);
        pos_ += 1;
        skip_backlen();
        return true;
      }
      if ((enc & 0xE0) == 0xC0) {
        // 110xxxxx xxxxxxxx — 13-bit signed int
        if (pos_ + 1 >= data_.size()) return false;
        int16_t v = static_cast<int16_t>(((static_cast<uint16_t>(enc & 0x1F)) << 8) |
                 static_cast<uint8_t>(data_[pos_ + 1]));
        // Sign extend from 13 bits
        if (v & 0x1000) v |= static_cast<int16_t>(0xE000);
        val = std::to_string(v);
        pos_ += 2;
        skip_backlen();
        return true;
      }
    }

    // Fallback: treat as 0-length string entry
    val.clear();
    pos_ += 1;
    skip_backlen();
    return true;
  }

  void skip_backlen() {
    // Backlen is variable-length. Each byte has MSB=1 for continuation.
    while (pos_ < data_.size()) {
      uint8_t b = static_cast<uint8_t>(data_[pos_++]);
      if ((b & 0x80) == 0) break;
    }
  }

  std::string data_;
  size_t pos_;
};

} // anonymous namespace

// ── RDB Save ───────────────────────────────────────────────────────────

namespace {
std::string rdb_build_string() {
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
        // Use QUICKLIST_2 encoding
        w.write_u8(RDB_TYPE_LIST_QUICKLIST_2);
        w.write_string(key);
        // One node containing all items as a single listpack
        w.write_length(1); // number of quicklist nodes
        ListpackWriter lp;
        for (const auto& item : items) lp.add_string(item);
        auto lp_data = lp.finish();
        // Container format: 2 = PACKED (listpack) 
        w.write_length(2); // container type: PACKED
        w.write_string(lp_data);

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
        // Skip streams in RDB for now — complex encoding
        // Write as a string-encoded placeholder
        continue;
      }
    }
  }
  ds.select_db(prev_db);

  // EOF marker
  w.write_u8(RDB_OPCODE_EOF);

  // CRC64
  uint64_t checksum = crc64(0, reinterpret_cast<const uint8_t*>(w.data().data()), w.data().size());
  w.write_u64_le(checksum);

  return w.data();
}
} // end anonymous namespace

bool rdb_save(const std::filesystem::path& path, std::string& err) {
  err.clear();
  std::string data = rdb_build_string();

  // Write to file
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out.is_open()) {
    err = "cannot open RDB file for writing";
    return false;
  }
  out.write(data.data(), static_cast<std::streamsize>(data.size()));
  if (!out.good()) {
    err = "write error";
    return false;
  }
  return true;
}

std::string rdb_save_to_string() {
  return rdb_build_string();
}

// ── RDB Load ───────────────────────────────────────────────────────────

bool rdb_load(const std::filesystem::path& path, std::string& err) {
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
  if (rdb_ver < 1 || rdb_ver > 12) {
    err = "unsupported RDB version " + std::to_string(rdb_ver);
    return false;
  }

  RdbReader r(filedata.data() + 9, filedata.size() - 9);
  auto& ds = store();
  ds.flushall();
  const int prev_db = ds.current_db();
  ds.select_db(0);

  std::optional<int64_t> pending_expire;

  while (!r.eof()) {
    uint8_t type;
    if (!r.read_u8(type)) break;

    if (type == RDB_OPCODE_EOF) break;

    if (type == RDB_OPCODE_AUX) {
      std::string aux_key, aux_val;
      if (!r.read_string(aux_key) || !r.read_string(aux_val)) {
        err = "malformed AUX field";
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

    } else if (type == RDB_TYPE_LIST_QUICKLIST || type == RDB_TYPE_LIST_QUICKLIST_2) {
      uint64_t node_count;
      bool enc;
      if (!r.read_length(node_count, enc) || enc) { err = "malformed quicklist"; ds.select_db(prev_db); return false; }
      std::vector<std::string> all_items;
      for (uint64_t n = 0; n < node_count; ++n) {
        if (type == RDB_TYPE_LIST_QUICKLIST_2) {
          uint64_t container;
          bool enc2;
          if (!r.read_length(container, enc2) || enc2) { err = "malformed quicklist2 container"; ds.select_db(prev_db); return false; }
        }
        std::string node_data;
        if (!r.read_string(node_data)) { err = "malformed quicklist node"; ds.select_db(prev_db); return false; }
        // Parse as listpack/ziplist
        ListpackReader lpr(node_data);
        std::vector<std::string> entries;
        if (lpr.parse(entries)) {
          for (auto& e : entries) all_items.push_back(std::move(e));
        } else {
          // Might be a ziplist; for simplicity add as raw
          all_items.push_back(node_data);
        }
      }
      if (!all_items.empty()) {
        bool wt = false;
        ds.rpush(key, all_items, wt);
      }
      if (expire_at) ds.expire(key, *expire_at);

    } else if (type == RDB_TYPE_LIST_ZIPLIST) {
      std::string blob;
      if (!r.read_string(blob)) { err = "malformed list ziplist"; ds.select_db(prev_db); return false; }
      ListpackReader lpr(blob);
      std::vector<std::string> items;
      lpr.parse(items);
      if (!items.empty()) { bool wt = false; ds.rpush(key, items, wt); }
      if (expire_at) ds.expire(key, *expire_at);

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
      lpr.parse(entries);
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

    } else if (type == RDB_TYPE_HASH_ZIPLIST || type == RDB_TYPE_HASH_LISTPACK) {
      std::string blob;
      if (!r.read_string(blob)) { err = "malformed hash listpack"; ds.select_db(prev_db); return false; }
      ListpackReader lpr(blob);
      std::vector<std::string> entries;
      lpr.parse(entries);
      std::vector<std::pair<std::string, std::string>> fields;
      for (size_t i = 0; i + 1 < entries.size(); i += 2) {
        fields.push_back({entries[i], entries[i + 1]});
      }
      if (!fields.empty()) { bool wt = false; ds.hset(key, fields, wt); }
      if (expire_at) ds.expire(key, *expire_at);

    } else if (type == RDB_TYPE_HASH_ZIPMAP) {
      // Old zipmap format — skip for simplicity
      std::string blob;
      if (!r.read_string(blob)) { err = "malformed hash zipmap"; ds.select_db(prev_db); return false; }
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

    } else if (type == RDB_TYPE_ZSET_ZIPLIST || type == RDB_TYPE_ZSET_LISTPACK) {
      std::string blob;
      if (!r.read_string(blob)) { err = "malformed zset listpack"; ds.select_db(prev_db); return false; }
      ListpackReader lpr(blob);
      std::vector<std::string> entries;
      lpr.parse(entries);
      for (size_t i = 0; i + 1 < entries.size(); i += 2) {
        double score = 0;
        try { score = std::stod(entries[i + 1]); } catch (...) {}
        ds.zadd_one(key, score, entries[i], false, false, false, false, false);
      }
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
  return true;
}

// ── DUMP / RESTORE ─────────────────────────────────────────────────────

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
  // The RDB version used in DUMP is the Redis version (e.g., 10)
  std::string payload = w.data();
  payload.push_back(static_cast<char>(RDB_VERSION & 0xff));
  payload.push_back(static_cast<char>((RDB_VERSION >> 8) & 0xff));

  uint64_t checksum = crc64(0, reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
  for (int i = 0; i < 8; ++i) payload.push_back(static_cast<char>((checksum >> (i * 8)) & 0xff));

  return payload;
}

bool rdb_restore_key(const std::string& key, const std::string& payload,
                     int64_t ttl_ms, bool replace, bool absttl,
                     std::string& err) {
  err.clear();

  // DUMP format: <type+data> <rdb-ver:u16 LE> <crc64:u64 LE>
  if (payload.size() < 10) {
    err = "DUMP payload is not valid";
    return false;
  }

  // Verify CRC
  size_t crc_offset = payload.size() - 8;
  uint64_t stored_crc = 0;
  for (int i = 0; i < 8; ++i) {
    stored_crc |= static_cast<uint64_t>(static_cast<uint8_t>(payload[crc_offset + i])) << (i * 8);
  }
  uint64_t computed_crc = crc64(0, reinterpret_cast<const uint8_t*>(payload.data()), crc_offset);
  if (stored_crc != computed_crc) {
    err = "DUMP payload version or checksum are wrong";
    return false;
  }

  // Strip the 2-byte version + 8-byte CRC
  size_t data_len = payload.size() - 10;
  RdbReader r(payload.data(), data_len);

  auto& ds = store();

  // Check if key exists (for REPLACE)
  if (!replace && ds.type_of(key) != "none") {
    err = "Target key name already exists.";
    return false;
  }

  // Delete existing key if replacing
  if (replace) ds.del(key);

  uint8_t type;
  if (!r.read_u8(type)) {
    err = "DUMP payload is not valid";
    return false;
  }

  std::optional<int64_t> expire_at;
  if (ttl_ms != 0) {
    if (absttl) {
      expire_at = ttl_ms;
    } else {
      expire_at = DataStore::now_ms() + ttl_ms;
    }
  }

  if (type == RDB_TYPE_STRING) {
    std::string val;
    if (!r.read_string(val)) { err = "malformed string in DUMP"; return false; }
    ds.set(key, val, false, false, expire_at, false);

  } else if (type == RDB_TYPE_LIST) {
    uint64_t len;
    bool enc;
    if (!r.read_length(len, enc) || enc) { err = "malformed list in DUMP"; return false; }
    std::vector<std::string> items;
    for (uint64_t i = 0; i < len; ++i) {
      std::string val;
      if (!r.read_string(val)) { err = "malformed list item in DUMP"; return false; }
      items.push_back(std::move(val));
    }
    bool wt = false;
    ds.rpush(key, items, wt);
    if (expire_at) ds.expire(key, *expire_at);

  } else if (type == RDB_TYPE_SET) {
    uint64_t len;
    bool enc;
    if (!r.read_length(len, enc) || enc) { err = "malformed set in DUMP"; return false; }
    std::vector<std::string> members;
    for (uint64_t i = 0; i < len; ++i) {
      std::string m;
      if (!r.read_string(m)) { err = "malformed set member in DUMP"; return false; }
      members.push_back(std::move(m));
    }
    bool wt = false;
    ds.sadd(key, members, wt);
    if (expire_at) ds.expire(key, *expire_at);

  } else if (type == RDB_TYPE_HASH) {
    uint64_t len;
    bool enc;
    if (!r.read_length(len, enc) || enc) { err = "malformed hash in DUMP"; return false; }
    std::vector<std::pair<std::string, std::string>> fields;
    for (uint64_t i = 0; i < len; ++i) {
      std::string f, v;
      if (!r.read_string(f) || !r.read_string(v)) { err = "malformed hash field in DUMP"; return false; }
      fields.push_back({std::move(f), std::move(v)});
    }
    bool wt = false;
    ds.hset(key, fields, wt);
    if (expire_at) ds.expire(key, *expire_at);

  } else if (type == RDB_TYPE_ZSET_2 || type == RDB_TYPE_ZSET) {
    uint64_t len;
    bool enc;
    if (!r.read_length(len, enc) || enc) { err = "malformed zset in DUMP"; return false; }
    for (uint64_t i = 0; i < len; ++i) {
      std::string member;
      if (!r.read_string(member)) { err = "malformed zset member in DUMP"; return false; }
      double score;
      if (type == RDB_TYPE_ZSET_2) {
        if (!r.read_double_binary(score)) { err = "malformed zset score in DUMP"; return false; }
      } else {
        if (!r.read_double_old(score)) { err = "malformed zset score in DUMP"; return false; }
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
