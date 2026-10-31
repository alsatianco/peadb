#pragma once

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>

namespace peadb {

// Wire framing only: the file loader separately validates the RDB EOF opcode/CRC.
// The caller supplies at most 64 KiB per feed and owns socket timeout/cancellation.
class ReplicationSnapshot {
 public:
  static constexpr std::size_t kChunk = 65536;
  static constexpr std::size_t kMarker = 40;

  bool start(std::string_view header) {
    if (started_ || header.empty() || header.front() != '$') return false;
    if (header.starts_with("$EOF:")) {
      if (header.size() != 5 + kMarker) return false;
      marker_ = header.substr(5);
      pending_.reserve(kChunk + kMarker - 1);
      size_ = -1;  // unknown until the trailer is found
    } else {
      const auto digits = header.substr(1);
      const auto result = std::from_chars(digits.data(), digits.data() + digits.size(), size_);
      if (result.ec != std::errc{} || result.ptr != digits.data() + digits.size() || size_ < 0)
        return false;
      done_ = size_ == 0;
    }
    started_ = true;
    return true;
  }

  // sink must consume the full span synchronously; false aborts the attempt.
  // read_ahead receives only bytes after the wire boundary, never snapshot bytes.
  template <typename Sink>
  bool feed(std::string_view bytes, Sink&& sink, std::string& read_ahead) {
    if (!started_ || failed_ || done_ || bytes.size() > kChunk) return false;
    std::size_t count;
    if (marker_.empty()) {
      count = static_cast<std::size_t>(std::min<std::int64_t>(size_ - received_, bytes.size()));
      if (!emit(bytes.substr(0, count), sink)) return false;
      done_ = received_ == size_;
      if (done_) read_ahead.assign(bytes.substr(count));
    } else {
      pending_.append(bytes);
      const auto pos = pending_.find(marker_);
      if (pos != std::string::npos) {
        if (!emit(std::string_view(pending_).substr(0, pos), sink)) return false;
        read_ahead.assign(pending_, pos + kMarker, std::string::npos);
        pending_.clear();
        size_ = received_;
        done_ = true;
      } else {
        // Only this suffix can still be part of a delimiter split across reads.
        count = pending_.size() > kMarker - 1 ? pending_.size() - (kMarker - 1) : 0;
        if (!emit(std::string_view(pending_).substr(0, count), sink)) return false;
        pending_.erase(0, count);
      }
    }
    return true;
  }

  bool done() const {
    return done_;
  }
  std::int64_t size() const {
    return size_;
  }
  std::int64_t received() const {
    return received_;
  }
  std::size_t pending() const {
    return pending_.size();
  }

 private:
  template <typename Sink>
  bool emit(std::string_view bytes, Sink& sink) {
    if (bytes.size() >
            static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max() - received_) ||
        (!bytes.empty() && !sink(bytes))) {
      failed_ = true;
      return false;
    }
    received_ += static_cast<std::int64_t>(bytes.size());
    return true;
  }

  bool started_ = false, done_ = false, failed_ = false;
  std::int64_t size_ = 0, received_ = 0;
  std::string marker_, pending_;
};

}  // namespace peadb
