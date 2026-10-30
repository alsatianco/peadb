#pragma once

// Implementation detail shared by the snapshot and DUMP decoders. The memory form borrows
// its input; the source form owns at most 64 KiB of read-ahead. Neither owns the source/fd.
#include "rdb.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string_view>

namespace peadb::detail {

// Blocking read(2) contract: positive short reads and -1/EINTR retry, zero means EOF,
// other errors fail. A callback must never return more than requested. No event servicing or
// datastore mutation is permitted inside it yet. Streaming transport/admission is separate.
using RdbReadFn = std::function<ssize_t(void*, std::size_t)>;

class RdbReader {
 public:
  explicit RdbReader(std::string_view data) : data_(data), limit_(data.size()) {
  }
  RdbReader(const char* p, std::size_t n) : RdbReader(std::string_view(p, n)) {
  }
  RdbReader(RdbReadFn source, std::size_t length, bool checksum = false)
      : source_(std::move(source)), limit_(length), streamed_(true), track_checksum_(checksum) {
  }
  RdbReader(const RdbReader&) = delete;
  RdbReader& operator=(const RdbReader&) = delete;

  bool eof() const {
    return pos_ == limit_;
  }
  std::size_t pos() const {
    return pos_;
  }
  std::size_t remaining() const {
    return limit_ - pos_;
  }
  std::uint64_t checksum() const {
    return checksum_;
  }
  int error() const {
    return error_;
  }

  bool read_u8(uint8_t& v) {
    return read_raw(&v, 1);
  }
  bool read_u16_le(uint16_t& v) {
    return read_integer(v, 2, false);
  }
  bool read_u32_le(uint32_t& v) {
    return read_integer(v, 4, false);
  }
  bool read_u32_be(uint32_t& v) {
    return read_integer(v, 4, true);
  }
  bool read_u64_le(uint64_t& v) {
    return read_integer(v, 8, false);
  }
  bool read_u64_be(uint64_t& v) {
    return read_integer(v, 8, true);
  }

  bool read_bytes(std::string& out, std::size_t n) {
    if (n > remaining() || n > out.max_size()) return false;
    if (!streamed_) {
      out.assign(data_.data() + pos_, n);
      consume(data_.data() + pos_, n);
      return true;
    }
    out.clear();
    // Grow with actual input: a truncated source declaring a huge value must not cause an
    // enormous zero-filled allocation before its first read. Each byte is read/copied once.
    while (n) {
      const std::size_t take = std::min(n, buffer_.size());
      if (!fill()) return false;
      const std::size_t available = std::min(take, end_ - begin_);
      out.append(buffer_.data() + begin_, available);
      consume(buffer_.data() + begin_, available);
      begin_ += available;
      n -= available;
    }
    return true;
  }

  bool skip(std::size_t n) {
    if (n > remaining()) return false;
    while (n) {
      if (streamed_) {
        if (!fill()) return false;
        const std::size_t take = std::min(n, end_ - begin_);
        consume(buffer_.data() + begin_, take);
        begin_ += take;
        n -= take;
      } else {
        consume(data_.data() + pos_, n);
        n = 0;
      }
    }
    return true;
  }

  // Read Redis length encoding. Returns (length, is_encoded).
  bool read_length(uint64_t& len, bool& is_encoded) {
    is_encoded = false;
    uint8_t byte;
    if (!read_u8(byte)) return false;
    uint8_t type = (byte >> 6) & 0x03;
    switch (type) {
      case 0:  // 00xxxxxx
        len = byte & 0x3f;
        return true;
      case 1: {  // 01xxxxxx
        uint8_t next;
        if (!read_u8(next)) return false;
        len = ((static_cast<uint64_t>(byte) & 0x3f) << 8) | next;
        return true;
      }
      case 2: {  // 10xxxxxx
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
      case 3:  // 11xxxxxx — special encoding
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
        case 0: {
          uint8_t v;
          if (!read_u8(v)) return false;
          out = std::to_string(static_cast<int8_t>(v));
          return true;
        }
        case 1: {
          uint16_t v;
          if (!read_u16_le(v)) return false;
          out = std::to_string(static_cast<int16_t>(v));
          return true;
        }
        case 2: {
          uint32_t v;
          if (!read_u32_le(v)) return false;
          out = std::to_string(static_cast<int32_t>(v));
          return true;
        }
        case 3: {
          // LZF compressed string
          uint64_t clen, ulen;
          bool enc2;
          if (!read_length(clen, enc2) || enc2) return false;
          if (!read_length(ulen, enc2) || enc2) return false;
          // LZF's longest back-reference makes 264 bytes out of 3, so a declared length over 88
          // times the compressed one cannot be right; refuse it before allocating, which also
          // stops a few bytes from claiming gigabytes (Redis refuses such data too, after a lazy
          // allocation PeaDB's zero-filled one cannot copy).
          if (ulen / 88 > clen || clen > std::numeric_limits<std::size_t>::max() ||
              ulen > std::numeric_limits<std::size_t>::max())
            return false;
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
    if (len > std::numeric_limits<std::size_t>::max()) return false;
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
    if (len == 255) {
      v = -std::numeric_limits<double>::infinity();
      return true;
    }
    if (len == 254) {
      v = std::numeric_limits<double>::infinity();
      return true;
    }
    if (len == 253) {
      v = std::numeric_limits<double>::quiet_NaN();
      return true;
    }
    std::string s;
    if (!read_bytes(s, len)) return false;
    char* end = nullptr;
    v = std::strtod(s.c_str(), &end);
    return !s.empty() && end == s.c_str() + s.size();
  }

 private:
  bool fill() {
    if (begin_ != end_) return true;
    if (failed_ || remaining() == 0) return false;
    if (!source_) {
      failed_ = true;
      error_ = EINVAL;
      return false;
    }
    const std::size_t request = std::min(buffer_.size(), remaining());
    ssize_t got;
    do {
      got = source_(buffer_.data(), request);
    } while (got == -1 && errno == EINTR);
    if (got <= 0 || static_cast<std::size_t>(got) > request) {
      error_ = got < 0 ? errno : (got == 0 ? 0 : EIO);
      failed_ = true;
      return false;
    }
    begin_ = 0;
    end_ = static_cast<std::size_t>(got);
    return true;
  }

  void consume(const char* p, std::size_t n) {
    if (track_checksum_) checksum_ = crc64(checksum_, p, n);
    pos_ += n;
  }

  bool read_raw(void* out, std::size_t n) {
    if (n > remaining()) return false;
    char* p = static_cast<char*>(out);
    while (n) {
      const char* input;
      std::size_t take = n;
      if (streamed_) {
        if (!fill()) return false;
        input = buffer_.data() + begin_;
        take = std::min(take, end_ - begin_);
        begin_ += take;
      } else {
        input = data_.data() + pos_;
      }
      std::memcpy(p, input, take);
      consume(input, take);
      p += take;
      n -= take;
    }
    return true;
  }

  template <typename T>
  bool read_integer(T& out, std::size_t n, bool big_endian) {
    unsigned char bytes[8];
    if (!read_raw(bytes, n)) return false;
    out = 0;
    for (std::size_t i = 0; i < n; ++i)
      out |= static_cast<T>(bytes[i]) << (8 * (big_endian ? n - i - 1 : i));
    return true;
  }

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
        if (len == 9) {  // 7 + 2
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

  std::string_view data_;
  RdbReadFn source_;
  std::array<char, 64 * 1024> buffer_;
  std::size_t limit_ = 0, pos_ = 0, begin_ = 0, end_ = 0;
  std::uint64_t checksum_ = 0;
  int error_ = 0;
  bool streamed_ = false, failed_ = false, track_checksum_ = false;
};

}  // namespace peadb::detail
