#include "datastore.hpp"

#include <chrono>
#include <cmath>
#include <cstdio>
#include <algorithm>
#include <cctype>
#include <sstream>
#include <regex>
#include <fstream>
#include <iomanip>
#include <random>
#include <limits>
#include <fnmatch.h>

namespace peadb {

namespace {
// Round-trip-safe double formatting for AOF rewrite (std::to_string gives only ~6
// significant digits, silently corrupting non-round scores on every rewrite).
std::string fmt_double(double d) {
  char buf[64];
  std::snprintf(buf, sizeof(buf), "%.17g", d);
  return buf;
}
}  // namespace

DataStore::DataStore() : current_db_(0), dbs_(16) {}

bool DataStore::select_db(int db_index) {
  if (db_index < 0 || db_index >= static_cast<int>(dbs_.size())) return false;
  current_db_ = db_index;
  return true;
}

int DataStore::current_db() const { return current_db_; }

std::int64_t DataStore::now_ms() {
  if (time_frozen_) return frozen_time_ms_;
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

void DataStore::freeze_time() {
  using namespace std::chrono;
  frozen_time_ms_ = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
  time_frozen_ = true;
}

void DataStore::unfreeze_time() {
  time_frozen_ = false;
}

bool DataStore::expire_if_needed(DB& db, const std::string& key) {
  const auto it = db.find(key);
  if (it == db.end()) return false;
  if (!it->second.expire_at_ms.has_value()) return false;
  if (*it->second.expire_at_ms > now_ms()) return false;
  db.erase(it);
  record_expired();
  return true;
}

std::optional<std::string> DataStore::get(const std::string& key) {
  auto& db = dbs_[current_db_];
  auto it = db.find(key);
  if (it == db.end()) { record_miss(); return std::nullopt; }
  if (it->second.expire_at_ms.has_value() && *it->second.expire_at_ms <= now_ms()) {
    db.erase(it);
    record_expired();
    record_miss();
    return std::nullopt;
  }
  record_hit();
  return it->second.str();
}

bool DataStore::set(const std::string& key, const std::string& value, bool nx, bool xx,
                    std::optional<std::int64_t> expire_at_ms, bool keep_ttl) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  const bool exists_now = it != db.end();
  if (nx && exists_now) return false;
  if (xx && !exists_now) return false;

  auto& e = exists_now ? it->second : db[key];
  // str() materializes the String payload, destroying any prior collection (replaces the
  // old per-field clear + type reset).
  e.str() = value;
  e.string_force_raw = false;
  e.encoding_big = false;  // a fresh value starts over on the encoding ladder
  e.encoding_no_intset = false;
  if (!exists_now || !keep_ttl) {
    e.expire_at_ms = expire_at_ms;
  }
  return true;
}

std::optional<std::string> DataStore::getdel(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  const auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  const auto out = it->second.str();
  db.erase(it);
  return out;
}

std::optional<std::string> DataStore::getex(const std::string& key, std::optional<std::int64_t> expire_at_ms,
                                            bool persist_ttl) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  if (persist_ttl) {
    it->second.expire_at_ms.reset();
  } else if (expire_at_ms.has_value()) {
    it->second.expire_at_ms = expire_at_ms;
  }
  return it->second.str();
}

bool DataStore::del(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  return db.erase(key) > 0;
}

std::size_t DataStore::exists(const std::vector<std::string>& keys) {
  std::size_t n = 0;
  auto& db = dbs_[current_db_];
  for (const auto& key : keys) {
    expire_if_needed(db, key);
    if (db.find(key) != db.end()) ++n;
  }
  return n;
}

std::string DataStore::type_of(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return "none";
  if (it->second.type() == ValueType::String) return "string";
  if (it->second.type() == ValueType::Hash) return "hash";
  if (it->second.type() == ValueType::List) return "list";
  if (it->second.type() == ValueType::Set) return "set";
  if (it->second.type() == ValueType::ZSet) return "zset";
  if (it->second.type() == ValueType::Stream) return "stream";
  return "none";
}

bool DataStore::is_wrongtype_for_string(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  return it->second.type() != ValueType::String;
}

bool DataStore::is_wrongtype_for_string_noexpire(const std::string& key) {
  auto& db = dbs_[current_db_];
  auto it = db.find(key);
  if (it == db.end()) return false;
  return it->second.type() != ValueType::String;
}

bool DataStore::is_wrongtype_for_hash(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  return it->second.type() != ValueType::Hash;
}

bool DataStore::is_wrongtype_for_list(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  return it->second.type() != ValueType::List;
}

bool DataStore::is_wrongtype_for_set(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  return it->second.type() != ValueType::Set;
}

bool DataStore::is_wrongtype_for_zset(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  return it->second.type() != ValueType::ZSet;
}

bool DataStore::is_wrongtype_for_stream(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  return it->second.type() != ValueType::Stream;
}

std::int64_t DataStore::pttl(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  const auto it = db.find(key);
  if (it == db.end()) return -2;
  if (!it->second.expire_at_ms.has_value()) return -1;
  return *it->second.expire_at_ms - now_ms();
}

std::int64_t DataStore::ttl(const std::string& key) {
  const auto p = pttl(key);
  if (p < 0) return p;
  return static_cast<std::int64_t>((p + 999) / 1000);
}

std::int64_t DataStore::pexpiretime(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  const auto it = db.find(key);
  if (it == db.end()) return -2;
  if (!it->second.expire_at_ms.has_value()) return -1;
  return *it->second.expire_at_ms;
}

std::int64_t DataStore::expiretime(const std::string& key) {
  const auto p = pexpiretime(key);
  if (p < 0) return p;
  return p / 1000;
}

bool DataStore::expire(const std::string& key, std::int64_t expire_at_ms) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  it->second.expire_at_ms = expire_at_ms;
  return true;
}

bool DataStore::persist(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end() || !it->second.expire_at_ms.has_value()) return false;
  it->second.expire_at_ms.reset();
  return true;
}

std::vector<std::string> DataStore::keys(const std::string& pattern) {
  std::vector<std::string> out;
  auto& db = dbs_[current_db_];
  for (auto it = db.begin(); it != db.end();) {
    if (it->second.expire_at_ms.has_value() && *it->second.expire_at_ms <= now_ms()) {
      it = db.erase(it);
      continue;
    }
    if (fnmatch(pattern.c_str(), it->first.c_str(), 0) == 0) {
      out.push_back(it->first);
    }
    ++it;
  }
  return out;
}

std::optional<std::string> DataStore::randomkey() {
  auto& db = dbs_[current_db_];
  if (db.empty()) return std::nullopt;
  static thread_local std::mt19937 rng{std::random_device{}()};
  const auto now = now_ms();
  const std::size_t num_buckets = db.bucket_count();
  // Pick a random starting bucket and walk forward — O(1/load_factor) amortised
  const std::size_t start = rng() % num_buckets;
  for (std::size_t i = 0; i < num_buckets; ++i) {
    const std::size_t bucket = (start + i) % num_buckets;
    for (auto it = db.begin(bucket); it != db.end(bucket); ++it) {
      if (it->second.expire_at_ms.has_value() && *it->second.expire_at_ms <= now) continue;
      return it->first;
    }
  }
  return std::nullopt;
}

bool DataStore::rename(const std::string& src, const std::string& dst, bool nx_only) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, src);
  expire_if_needed(db, dst);
  auto src_it = db.find(src);
  if (src_it == db.end()) return false;
  if (nx_only && db.find(dst) != db.end()) return false;
  db[dst] = src_it->second;
  db.erase(src_it);
  return true;
}

bool DataStore::swapdb(int a, int b) {
  if (a < 0 || b < 0 || a >= static_cast<int>(dbs_.size()) || b >= static_cast<int>(dbs_.size())) return false;
  if (a == b) return true;
  std::swap(dbs_[a], dbs_[b]);
  return true;
}

std::int64_t DataStore::move_key(const std::string& key, int dst_db) {
  if (dst_db < 0 || dst_db >= static_cast<int>(dbs_.size())) return 0;
  if (dst_db == current_db_) return 0;

  auto& src = dbs_[current_db_];
  expire_if_needed(src, key);
  auto sit = src.find(key);
  if (sit == src.end()) return 0;

  auto& dst = dbs_[dst_db];
  expire_if_needed(dst, key);
  if (dst.find(key) != dst.end()) return 0;

  dst[key] = sit->second;
  src.erase(sit);
  return 1;
}

std::int64_t DataStore::copy_key(const std::string& src, const std::string& dst, int db, bool replace) {
  if (db < 0 || db >= static_cast<int>(dbs_.size())) return 0;
  auto& src_db = dbs_[current_db_];
  expire_if_needed(src_db, src);
  auto src_it = src_db.find(src);
  if (src_it == src_db.end()) return 0;

  auto& dst_db = dbs_[db];
  expire_if_needed(dst_db, dst);
  if (!replace && dst_db.find(dst) != dst_db.end()) return 0;

  dst_db[dst] = src_it->second;
  return 1;
}

std::int64_t DataStore::incrby(const std::string& key, std::int64_t by, bool& ok, std::string& err) {
  ok = false;
  err.clear();

  auto val = get(key);
  long long base = 0;
  if (val.has_value()) {
    // Redis string2ll strictness: no whitespace anywhere, no '+' prefix, no embedded
    // NUL, whole string must parse, range-checked.
    const std::string& s = *val;
    char* end = nullptr;
    errno = 0;
    if (s.empty() ||
        (!std::isdigit(static_cast<unsigned char>(s.front())) && s.front() != '-') ||
        s.find('\0') != std::string::npos ||
        (base = std::strtoll(s.c_str(), &end, 10), end != s.c_str() + s.size()) ||
        errno == ERANGE) {
      err = "value is not an integer or out of range";
      return 0;
    }
  }

  if ((by > 0 && base > std::numeric_limits<long long>::max() - by) ||
      (by < 0 && base < std::numeric_limits<long long>::min() - by)) {
    err = "increment or decrement would overflow";
    return 0;
  }
  const long long out = base + by;
  // In-place value update preserves the key's TTL (Redis INCR/DECR semantics),
  // unlike a plain SET which clears it.
  set(key, std::to_string(out), false, false, std::nullopt, /*keep_ttl=*/true);
  ok = true;
  return out;
}

std::string DataStore::incrbyfloat(const std::string& key, long double by, bool& ok, std::string& err) {
  ok = false;
  err.clear();

  auto val = get(key);
  long double base = 0.0;
  if (val.has_value()) {
    // Redis string2ld strictness: whole-string parse, no leading whitespace, no
    // embedded NUL, NaN rejected ("value is not a valid float").
    const std::string& s = *val;
    char* end = nullptr;
    if (s.empty() || std::isspace(static_cast<unsigned char>(s.front())) ||
        s.find('\0') != std::string::npos ||
        (base = std::strtold(s.c_str(), &end), end != s.c_str() + s.size()) ||
        std::isnan(static_cast<double>(base))) {
      err = "value is not a valid float";
      return "";
    }
  }

  long double out = base + by;
  if (std::isnan(static_cast<double>(out)) || std::isinf(static_cast<double>(out))) {
    err = "increment would produce NaN or Infinity";
    return "";
  }
  // Redis ld2string: fixed %.17Lf notation with trailing zeros trimmed — never
  // scientific notation (INCRBYFLOAT 0.0000005 must yield "0.0000005", not "5e-07").
  char buf[5120];
  std::snprintf(buf, sizeof(buf), "%.17Lf", out);
  std::string s(buf);
  // Remove trailing zeros after decimal point
  if (s.find('.') != std::string::npos) {
    auto pos = s.find_last_not_of('0');
    if (pos != std::string::npos && s[pos] == '.') {
      s = s.substr(0, pos); // Remove the trailing dot too
    } else if (pos != std::string::npos) {
      s = s.substr(0, pos + 1);
    }
  }

  set(key, s, false, false, std::nullopt, false);
  ok = true;
  return s;
}

std::size_t DataStore::append(const std::string& key, const std::string& value) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    Entry e;
    e.str();
    e.str() = value;
    e.string_force_raw = true;
    db[key] = std::move(e);
    return value.size();
  }
  if (it->second.type() != ValueType::String) return 0;
  it->second.str() += value;
  it->second.string_force_raw = true;
  return it->second.str().size();
}

std::size_t DataStore::strlen(const std::string& key) {
  auto v = get(key);
  return v.has_value() ? v->size() : 0;
}

int DataStore::setbit(const std::string& key, std::uint64_t bit_offset, int bit, bool& ok, std::string& err) {
  ok = false;
  err.clear();
  if (bit_offset > 0xffffffffULL) {
    err = "bit offset is not an integer or out of range";
    return 0;
  }
  if (bit != 0 && bit != 1) {
    err = "bit is not an integer or out of range";
    return 0;
  }

  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    Entry e;
    e.str();
    db[key] = std::move(e);
    it = db.find(key);
  } else if (it->second.type() != ValueType::String) {
    err = "WRONGTYPE";
    return 0;
  }

  const std::size_t byte_index = static_cast<std::size_t>(bit_offset / 8ULL);
  const unsigned bit_index = static_cast<unsigned>(bit_offset % 8ULL);
  auto& s = it->second.str();
  if (s.size() <= byte_index) s.resize(byte_index + 1, '\0');

  unsigned char c = static_cast<unsigned char>(s[byte_index]);
  const unsigned char mask = static_cast<unsigned char>(1u << (7u - bit_index));
  const int prev = (c & mask) ? 1 : 0;
  if (bit == 1) c = static_cast<unsigned char>(c | mask);
  else c = static_cast<unsigned char>(c & static_cast<unsigned char>(~mask));
  s[byte_index] = static_cast<char>(c);
  it->second.string_force_raw = true;
  ok = true;
  return prev;
}

int DataStore::getbit(const std::string& key, std::uint64_t bit_offset, bool& ok, std::string& err) {
  ok = false;
  err.clear();
  if (bit_offset > 0xffffffffULL) {
    err = "bit offset is not an integer or out of range";
    return 0;
  }

  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    ok = true;
    return 0;
  }
  if (it->second.type() != ValueType::String) {
    err = "WRONGTYPE";
    return 0;
  }

  const std::size_t byte_index = static_cast<std::size_t>(bit_offset / 8ULL);
  const unsigned bit_index = static_cast<unsigned>(bit_offset % 8ULL);
  const auto& s = it->second.str();
  if (byte_index >= s.size()) {
    ok = true;
    return 0;
  }

  const unsigned char c = static_cast<unsigned char>(s[byte_index]);
  const unsigned char mask = static_cast<unsigned char>(1u << (7u - bit_index));
  ok = true;
  return (c & mask) ? 1 : 0;
}

std::size_t DataStore::setrange(const std::string& key, std::size_t offset, const std::string& value, bool& ok, std::string& err) {
  ok = false;
  err.clear();
  constexpr std::size_t kMaxStringLen = 512ULL * 1024ULL * 1024ULL;

  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end() && value.empty()) {
    ok = true;
    return 0;
  }
  if (it != db.end() && it->second.type() != ValueType::String) {
    err = "WRONGTYPE";
    return 0;
  }

  std::size_t base_len = 0;
  if (it != db.end()) base_len = it->second.str().size();
  if (value.empty()) {
    ok = true;
    return base_len;
  }
  if (offset > kMaxStringLen || value.size() > kMaxStringLen - offset) {
    err = "string exceeds maximum allowed size (proto-max-bulk-len)";
    return 0;
  }
  const std::size_t end_len = offset + value.size();
  if (end_len > kMaxStringLen) {
    err = "string exceeds maximum allowed size (proto-max-bulk-len)";
    return 0;
  }

  if (it == db.end()) {
    Entry e;
    e.str();
    db[key] = std::move(e);
    it = db.find(key);
  }
  auto& s = it->second.str();
  if (s.size() < offset) s.resize(offset, '\0');
  if (s.size() < end_len) s.resize(end_len, '\0');
  s.replace(offset, value.size(), value);
  it->second.string_force_raw = true;
  ok = true;
  return s.size();
}

std::string DataStore::getrange(const std::string& key, std::int64_t start, std::int64_t stop, bool& ok, std::string& err) {
  ok = false;
  err.clear();

  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    ok = true;
    return "";
  }
  if (it->second.type() != ValueType::String) {
    err = "WRONGTYPE";
    return "";
  }

  const auto& s = it->second.str();
  const std::int64_t len = static_cast<std::int64_t>(s.size());
  if (len == 0) {
    ok = true;
    return "";
  }
  if (start < 0) start = len + start;
  if (stop < 0) stop = len + stop;
  if (start < 0) start = 0;
  if (stop < 0) {
    ok = true;
    return "";
  }
  if (start >= len || start > stop) {
    ok = true;
    return "";
  }
  if (stop >= len) stop = len - 1;
  const auto from = static_cast<std::size_t>(start);
  const auto n = static_cast<std::size_t>(stop - start + 1);
  ok = true;
  return s.substr(from, n);
}

std::int64_t DataStore::hset(const std::string& key, const std::vector<std::pair<std::string, std::string>>& fields, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);

  auto it = db.find(key);
  if (it == db.end()) {
    Entry e;
    e.hash();
    db[key] = std::move(e);
    it = db.find(key);
  } else if (it->second.type() != ValueType::Hash) {
    wrongtype = true;
    return 0;
  }

  std::int64_t added = 0;
  for (const auto& [f, v] : fields) {
    if (it->second.hash().find(f) == it->second.hash().end()) ++added;
    it->second.hash()[f] = v;
  }
  return added;
}

std::optional<std::string> DataStore::hget(const std::string& key, const std::string& field, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  if (it->second.type() != ValueType::Hash) {
    wrongtype = true;
    return std::nullopt;
  }
  auto fit = it->second.hash().find(field);
  if (fit == it->second.hash().end()) return std::nullopt;
  return fit->second;
}

std::int64_t DataStore::hdel(const std::string& key, const std::vector<std::string>& fields, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type() != ValueType::Hash) {
    wrongtype = true;
    return 0;
  }
  std::int64_t removed = 0;
  for (const auto& f : fields) removed += it->second.hash().erase(f) ? 1 : 0;
  if (it->second.hash().empty()) db.erase(it);
  return removed;
}

std::int64_t DataStore::hlen(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type() != ValueType::Hash) {
    wrongtype = true;
    return 0;
  }
  return static_cast<std::int64_t>(it->second.hash().size());
}

bool DataStore::hexists(const std::string& key, const std::string& field, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  if (it->second.type() != ValueType::Hash) {
    wrongtype = true;
    return false;
  }
  return it->second.hash().find(field) != it->second.hash().end();
}

std::vector<std::pair<std::string, std::string>> DataStore::hgetall(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  std::vector<std::pair<std::string, std::string>> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return out;
  if (it->second.type() != ValueType::Hash) {
    wrongtype = true;
    return out;
  }
  out.reserve(it->second.hash().size());
  for (const auto& kv : it->second.hash()) out.push_back(kv);
  return out;
}

std::pair<std::uint64_t, std::vector<std::pair<std::string, std::string>>> DataStore::hscan(
    const std::string& key, std::uint64_t cursor, std::size_t count, bool& wrongtype) {
  wrongtype = false;
  std::vector<std::pair<std::string, std::string>> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return {0, out};
  if (it->second.type() != ValueType::Hash) {
    wrongtype = true;
    return {0, out};
  }

  std::size_t idx = 0;
  std::size_t emitted = 0;
  for (const auto& kv : it->second.hash()) {
    if (idx++ < cursor) continue;
    out.push_back(kv);
    if (++emitted >= count) break;
  }

  const std::uint64_t next = (cursor + emitted) >= it->second.hash().size() ? 0 : (cursor + emitted);
  return {next, out};
}

std::int64_t DataStore::lpush(const std::string& key, const std::vector<std::string>& values, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    Entry e;
    e.list();
    db[key] = std::move(e);
    it = db.find(key);
  } else if (it->second.type() != ValueType::List) {
    wrongtype = true;
    return 0;
  }
  for (const auto& v : values) it->second.list().push_front(v);
  return static_cast<std::int64_t>(it->second.list().size());
}

std::int64_t DataStore::rpush(const std::string& key, const std::vector<std::string>& values, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    Entry e;
    e.list();
    db[key] = std::move(e);
    it = db.find(key);
  } else if (it->second.type() != ValueType::List) {
    wrongtype = true;
    return 0;
  }
  for (const auto& v : values) it->second.list().push_back(v);
  return static_cast<std::int64_t>(it->second.list().size());
}

std::optional<std::string> DataStore::lpop(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  if (it->second.type() != ValueType::List) {
    wrongtype = true;
    return std::nullopt;
  }
  if (it->second.list().empty()) return std::nullopt;
  auto v = it->second.list().front();
  it->second.list().pop_front();
  if (it->second.list().empty()) db.erase(it);
  return v;
}

std::optional<std::string> DataStore::rpop(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  if (it->second.type() != ValueType::List) {
    wrongtype = true;
    return std::nullopt;
  }
  if (it->second.list().empty()) return std::nullopt;
  auto v = it->second.list().back();
  it->second.list().pop_back();
  if (it->second.list().empty()) db.erase(it);
  return v;
}

std::int64_t DataStore::llen(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type() != ValueType::List) {
    wrongtype = true;
    return 0;
  }
  return static_cast<std::int64_t>(it->second.list().size());
}

std::vector<std::string> DataStore::lrange(const std::string& key, std::int64_t start, std::int64_t stop, bool& wrongtype) {
  wrongtype = false;
  std::vector<std::string> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return out;
  if (it->second.type() != ValueType::List) {
    wrongtype = true;
    return out;
  }

  const auto n = static_cast<std::int64_t>(it->second.list().size());
  if (n == 0) return out;

  if (start < 0) start = n + start;
  if (stop < 0) stop = n + stop;
  if (start < 0) start = 0;
  if (stop >= n) stop = n - 1;
  if (start > stop || start >= n) return out;

  for (std::int64_t i = start; i <= stop; ++i) out.push_back(it->second.list()[static_cast<std::size_t>(i)]);
  return out;
}

std::int64_t DataStore::sadd(const std::string& key, const std::vector<std::string>& members, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    Entry e;
    e.set();
    db[key] = std::move(e);
    it = db.find(key);
  } else if (it->second.type() != ValueType::Set) {
    wrongtype = true;
    return 0;
  }
  std::int64_t added = 0;
  auto& e = it->second;
  for (const auto& m : members) {
    added += e.set().insert(m).second ? 1 : 0;
    // Intset disqualification is one-way in Redis: a non-integer member (or growing
    // past set-max-intset-entries) converts the set away from intset permanently,
    // even if the offending member is later removed. Detected at write time because
    // OBJECT ENCODING may not be consulted between the add and the removal.
    if (!e.encoding_no_intset) {
      bool is_int = !m.empty();
      std::size_t i = (m[0] == '-' ? 1 : 0);
      if (i >= m.size() || (m[i] == '0' && m.size() - i > 1)) is_int = false;
      for (std::size_t j = i; is_int && j < m.size(); ++j) {
        if (!std::isdigit(static_cast<unsigned char>(m[j]))) is_int = false;
      }
      if (is_int && m.size() > 19) is_int = false;  // beyond int64 digits
      if (!is_int || e.set().size() > static_cast<std::size_t>(std::max<std::int64_t>(0, encoding_limits_.set_intset))) {
        e.encoding_no_intset = true;
      }
    }
  }
  return added;
}

std::int64_t DataStore::srem(const std::string& key, const std::vector<std::string>& members, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type() != ValueType::Set) {
    wrongtype = true;
    return 0;
  }
  std::int64_t removed = 0;
  for (const auto& m : members) removed += it->second.set().erase(m) ? 1 : 0;
  if (it->second.set().empty()) db.erase(it);
  return removed;
}

bool DataStore::sismember(const std::string& key, const std::string& member, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  if (it->second.type() != ValueType::Set) {
    wrongtype = true;
    return false;
  }
  return it->second.set().find(member) != it->second.set().end();
}

std::vector<std::string> DataStore::smembers(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  std::vector<std::string> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return out;
  if (it->second.type() != ValueType::Set) {
    wrongtype = true;
    return out;
  }
  out.reserve(it->second.set().size());
  for (const auto& m : it->second.set()) out.push_back(m);
  return out;
}

std::int64_t DataStore::scard(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type() != ValueType::Set) {
    wrongtype = true;
    return 0;
  }
  return static_cast<std::int64_t>(it->second.set().size());
}

std::optional<std::string> DataStore::spop(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  if (it->second.type() != ValueType::Set) {
    wrongtype = true;
    return std::nullopt;
  }
  if (it->second.set().empty()) return std::nullopt;
  const auto pick = it->second.set().begin();
  const std::string value = *pick;
  it->second.set().erase(pick);
  if (it->second.set().empty()) db.erase(it);
  return value;
}

std::pair<std::uint64_t, std::vector<std::string>> DataStore::sscan(
    const std::string& key, std::uint64_t cursor, std::size_t count, bool& wrongtype) {
  wrongtype = false;
  std::vector<std::string> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return {0, out};
  if (it->second.type() != ValueType::Set) {
    wrongtype = true;
    return {0, out};
  }

  std::size_t idx = 0, emitted = 0;
  for (const auto& m : it->second.set()) {
    if (idx++ < cursor) continue;
    out.push_back(m);
    if (++emitted >= count) break;
  }
  const auto next = (cursor + emitted) >= it->second.set().size() ? 0 : (cursor + emitted);
  return {next, out};
}

DataStore::ZAddResult DataStore::zadd_one(const std::string& key, double score, const std::string& member,
                                          bool nx, bool xx, bool gt, bool lt, bool incr) {
  ZAddResult r;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    Entry e;
    e.zset_payload();
    db[key] = std::move(e);
    it = db.find(key);
  } else if (it->second.type() != ValueType::ZSet) {
    r.wrongtype = true;
    return r;
  }

  auto mit = it->second.zset().find(member);
  const bool exists = mit != it->second.zset().end();
  if (nx && exists) return r;
  if (xx && !exists) return r;

  double new_score = score;
  if (exists && incr) new_score = mit->second + score;
  if (exists && gt && new_score <= mit->second) return r;
  if (exists && lt && new_score >= mit->second) return r;

  auto& entry = it->second;
  if (!exists) {
    entry.zset()[member] = new_score;
    entry.zord()[{new_score, member}] = true;
    r.added = true;
    r.changed = true;
    r.score = new_score;
    return r;
  }

  if (mit->second != new_score) {
    entry.zord().erase({mit->second, member}); // remove old position
    mit->second = new_score;
    entry.zord()[{new_score, member}] = true;  // insert at new position
    r.changed = true;
  }
  r.score = mit->second;
  return r;
}

std::vector<std::pair<std::string, double>> DataStore::zrange(
    const std::string& key, std::int64_t start, std::int64_t stop, bool withscores, bool& wrongtype) {
  wrongtype = false;
  std::vector<std::pair<std::string, double>> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return out;
  if (it->second.type() != ValueType::ZSet) {
    wrongtype = true;
    return out;
  }

  // Use the ordered index for O(log N) range iteration instead of O(N log N) sort.
  auto& ordered = it->second.zord();
  const auto n = static_cast<std::int64_t>(ordered.size());
  if (n == 0) return out;
  if (start < 0) start = n + start;
  if (stop < 0) stop = n + stop;
  if (start < 0) start = 0;
  if (stop >= n) stop = n - 1;
  if (start > stop || start >= n) return out;

  auto oit = ordered.begin();
  std::advance(oit, start);
  for (std::int64_t i = start; i <= stop; ++i, ++oit)
    out.push_back({oit->first.second, oit->first.first}); // {member, score}
  (void)withscores;
  return out;
}

std::pair<std::uint64_t, std::vector<std::pair<std::string, double>>> DataStore::zscan(
    const std::string& key, std::uint64_t cursor, std::size_t count, bool& wrongtype) {
  wrongtype = false;
  std::vector<std::pair<std::string, double>> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return {0, out};
  if (it->second.type() != ValueType::ZSet) {
    wrongtype = true;
    return {0, out};
  }

  std::size_t idx = 0, emitted = 0;
  for (const auto& kv : it->second.zset()) {
    if (idx++ < cursor) continue;
    out.push_back(kv);
    if (++emitted >= count) break;
  }
  const auto next = (cursor + emitted) >= it->second.zset().size() ? 0 : (cursor + emitted);
  return {next, out};
}

std::vector<std::pair<std::string, double>> DataStore::zpop(
    const std::string& key, std::size_t count, bool max, bool& wrongtype) {
  wrongtype = false;
  std::vector<std::pair<std::string, double>> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return out;
  if (it->second.type() != ValueType::ZSet) {
    wrongtype = true;
    return out;
  }

  auto& entry = it->second;
  auto& ordered = entry.zord();
  const std::size_t take = std::min(count, ordered.size());
  out.reserve(take);
  for (std::size_t i = 0; i < take; ++i) {
    auto oit = max ? std::prev(ordered.end()) : ordered.begin();
    const std::string& member = oit->first.second;
    const double score = oit->first.first;
    out.push_back({member, score});
    entry.zset().erase(member);
    ordered.erase(oit);
  }
  if (entry.zset().empty()) db.erase(it);
  return out;
}

std::optional<std::string> DataStore::xadd(
    const std::string& key, const std::string& id,
    const std::vector<std::pair<std::string, std::string>>& fields,
    bool& wrongtype, std::string& err) {
  wrongtype = false;
  err.clear();
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    Entry e;
    e.stream();
    db[key] = std::move(e);
    it = db.find(key);
  } else if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return std::nullopt;
  }

  std::uint64_t ms = 0, seq = 0;
  if (id == "*") {
    const auto now = static_cast<std::uint64_t>(now_ms());
    if (now > it->second.stream_last_ms()) {
      ms = now;
      seq = 0;
    } else {
      ms = it->second.stream_last_ms();
      seq = it->second.stream_last_seq() + 1;
    }
  } else {
    const auto pos = id.find('-');
    try {
      if (pos == std::string::npos) {
        ms = static_cast<std::uint64_t>(std::stoull(id));
        seq = 0;
      } else {
        ms = static_cast<std::uint64_t>(std::stoull(id.substr(0, pos)));
        seq = static_cast<std::uint64_t>(std::stoull(id.substr(pos + 1)));
      }
    } catch (...) {
      err = "Invalid stream ID specified as stream command argument";
      return std::nullopt;
    }
    if (ms < it->second.stream_last_ms() || (ms == it->second.stream_last_ms() && seq <= it->second.stream_last_seq())) {
      err = "The ID specified in XADD is equal or smaller than the target stream top item";
      return std::nullopt;
    }
  }

  it->second.stream_last_ms() = ms;
  it->second.stream_last_seq() = seq;
  const std::string out_id = std::to_string(ms) + "-" + std::to_string(seq);
  it->second.stream_entries().push_back({out_id, fields});
  return out_id;
}

std::int64_t DataStore::xlen(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return 0;
  }
  return static_cast<std::int64_t>(it->second.stream_entries().size());
}

std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> DataStore::xrange(
    const std::string& key, const std::string& start, const std::string& stop, bool rev, bool& wrongtype) {
  wrongtype = false;
  std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return out;
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return out;
  }

  auto in_range = [&](const std::string& id) {
    bool ge_start = (start == "-" || id >= start);
    bool le_stop = (stop == "+" || id <= stop);
    return ge_start && le_stop;
  };

  if (!rev) {
    for (const auto& e : it->second.stream_entries()) if (in_range(e.first)) out.push_back(e);
  } else {
    for (auto rit = it->second.stream_entries().rbegin(); rit != it->second.stream_entries().rend(); ++rit) {
      if (in_range(rit->first)) out.push_back(*rit);
    }
  }
  return out;
}

namespace {
bool parse_stream_id_pair(const std::string& id, std::uint64_t& ms, std::uint64_t& seq) {
  const auto pos = id.find('-');
  if (pos == std::string::npos) return false;
  try {
    ms = static_cast<std::uint64_t>(std::stoull(id.substr(0, pos)));
    seq = static_cast<std::uint64_t>(std::stoull(id.substr(pos + 1)));
    return true;
  } catch (...) {
    return false;
  }
}

bool stream_id_gt(const std::string& a, const std::string& b) {
  std::uint64_t am = 0, as = 0, bm = 0, bs = 0;
  if (!parse_stream_id_pair(a, am, as) || !parse_stream_id_pair(b, bm, bs)) return a > b;
  if (am != bm) return am > bm;
  return as > bs;
}
} // namespace

bool DataStore::xgroup_create(const std::string& key, const std::string& group, const std::string& start_id,
                              bool mkstream, bool& wrongtype, std::string& err) {
  wrongtype = false;
  err.clear();
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    if (!mkstream) {
      err = "The XGROUP subcommand requires the key to exist";
      return false;
    }
    Entry e;
    e.stream();
    db[key] = std::move(e);
    it = db.find(key);
  }
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return false;
  }
  if (it->second.stream_groups().find(group) != it->second.stream_groups().end()) {
    err = "BUSYGROUP Consumer Group name already exists";
    return false;
  }
  Entry::StreamGroup g;
  g.last_delivered_id = start_id;
  it->second.stream_groups()[group] = std::move(g);
  return true;
}

bool DataStore::xgroup_setid(const std::string& key, const std::string& group, const std::string& id,
                             bool& wrongtype, std::string& err) {
  wrongtype = false;
  err.clear();
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    err = "NOGROUP No such key or consumer group";
    return false;
  }
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return false;
  }
  auto git = it->second.stream_groups().find(group);
  if (git == it->second.stream_groups().end()) {
    err = "NOGROUP No such key or consumer group";
    return false;
  }

  std::string next_id = id;
  if (id == "$") {
    next_id = it->second.stream_entries().empty() ? "0-0" : it->second.stream_entries().back().first;
  } else {
    std::uint64_t ms = 0, seq = 0;
    if (!parse_stream_id_pair(id, ms, seq)) {
      err = "Invalid stream ID specified as stream command argument";
      return false;
    }
  }
  git->second.last_delivered_id = next_id;
  return true;
}

std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>>>> DataStore::xreadgroup(
    const std::string& group, const std::string& consumer, const std::vector<std::pair<std::string, std::string>>& streams,
    bool& wrongtype, std::string& err) {
  wrongtype = false;
  err.clear();
  std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>>>> out;
  auto& db = dbs_[current_db_];

  for (const auto& [key, idreq] : streams) {
    expire_if_needed(db, key);
    auto it = db.find(key);
    if (it == db.end()) continue;
    if (it->second.type() != ValueType::Stream) {
      wrongtype = true;
      return {};
    }
    auto git = it->second.stream_groups().find(group);
    if (git == it->second.stream_groups().end()) {
      err = "NOGROUP No such key or consumer group";
      return {};
    }

    std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> rows;
    if (idreq == ">") {
      for (const auto& e : it->second.stream_entries()) {
        if (!stream_id_gt(e.first, git->second.last_delivered_id)) continue;
        rows.push_back(e);
        git->second.pending_to_consumer[e.first] = consumer;
        git->second.pending_per_consumer[consumer] += 1;
        git->second.pending_delivery_ms[e.first] = now_ms();
        git->second.pending_delivery_count[e.first] = 1;
        git->second.last_delivered_id = e.first;
        break;
      }
    }
    if (!rows.empty()) out.push_back({key, rows});
  }
  return out;
}

std::int64_t DataStore::xdel(const std::string& key, const std::vector<std::string>& ids, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return 0;
  }

  std::int64_t removed = 0;
  for (const auto& id : ids) {
    auto vit = std::find_if(it->second.stream_entries().begin(), it->second.stream_entries().end(),
                            [&](const auto& e) { return e.first == id; });
    if (vit == it->second.stream_entries().end()) continue;
    it->second.stream_entries().erase(vit);
    ++removed;

    for (auto& [_, grp] : it->second.stream_groups()) {
      auto pit = grp.pending_to_consumer.find(id);
      if (pit == grp.pending_to_consumer.end()) continue;
      auto cit = grp.pending_per_consumer.find(pit->second);
      if (cit != grp.pending_per_consumer.end()) {
        if (cit->second > 1) --cit->second;
        else grp.pending_per_consumer.erase(cit);
      }
      grp.pending_to_consumer.erase(pit);
      grp.pending_delivery_ms.erase(id);
      grp.pending_delivery_count.erase(id);
    }
  }
  return removed;
}

std::int64_t DataStore::xack(const std::string& key, const std::string& group, const std::vector<std::string>& ids,
                             bool& wrongtype, std::string& err) {
  wrongtype = false;
  err.clear();
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return 0;
  }
  auto git = it->second.stream_groups().find(group);
  if (git == it->second.stream_groups().end()) {
    err = "NOGROUP No such key or consumer group";
    return 0;
  }
  std::int64_t acked = 0;
  for (const auto& id : ids) {
    auto pit = git->second.pending_to_consumer.find(id);
    if (pit == git->second.pending_to_consumer.end()) continue;
    const auto consumer = pit->second;
    git->second.pending_to_consumer.erase(pit);
    git->second.pending_delivery_ms.erase(id);
    git->second.pending_delivery_count.erase(id);
    auto cit = git->second.pending_per_consumer.find(consumer);
    if (cit != git->second.pending_per_consumer.end() && cit->second > 0) {
      cit->second -= 1;
      if (cit->second == 0) git->second.pending_per_consumer.erase(cit);
    }
    ++acked;
  }
  return acked;
}

std::vector<std::string> DataStore::xpending_summary(const std::string& key, const std::string& group,
                                                     bool& wrongtype, std::string& err) {
  wrongtype = false;
  err.clear();
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return {"0", "", "", ""};
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return {};
  }
  auto git = it->second.stream_groups().find(group);
  if (git == it->second.stream_groups().end()) {
    err = "NOGROUP No such key or consumer group";
    return {};
  }
  if (git->second.pending_to_consumer.empty()) return {"0", "", "", ""};
  std::string min_id, max_id;
  bool first = true;
  for (const auto& kv : git->second.pending_to_consumer) {
    if (first) {
      min_id = max_id = kv.first;
      first = false;
      continue;
    }
    if (stream_id_gt(min_id, kv.first)) min_id = kv.first;
    if (stream_id_gt(kv.first, max_id)) max_id = kv.first;
  }
  return {std::to_string(git->second.pending_to_consumer.size()), min_id, max_id, std::to_string(git->second.pending_per_consumer.size())};
}

bool DataStore::dump_stream(const std::string& key, StreamDump& out) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end() || it->second.type() != ValueType::Stream) return false;
  const auto& e = it->second;
  out.entries = e.stream_entries();
  out.last_ms = e.stream_last_ms();
  out.last_seq = e.stream_last_seq();
  out.groups.clear();
  for (const auto& [gname, g] : e.stream_groups()) {
    StreamGroupDump gd;
    gd.name = gname;
    gd.last_delivered_id = g.last_delivered_id;
    for (const auto& [id, consumer] : g.pending_to_consumer) {
      StreamPel p;
      p.id = id;
      p.consumer = consumer;
      auto dm = g.pending_delivery_ms.find(id);
      p.delivery_ms = dm != g.pending_delivery_ms.end() ? dm->second : 0;
      auto dc = g.pending_delivery_count.find(id);
      p.delivery_count = dc != g.pending_delivery_count.end() ? dc->second : 1;
      gd.pending.push_back(std::move(p));
    }
    out.groups.push_back(std::move(gd));
  }
  return true;
}

void DataStore::restore_stream(const std::string& key, const StreamDump& d) {
  auto& db = dbs_[current_db_];
  auto& e = db[key];
  e = Entry{};
  e.stream();
  e.stream_entries() = d.entries;
  e.stream_last_ms() = d.last_ms;
  e.stream_last_seq() = d.last_seq;
  for (const auto& gd : d.groups) {
    Entry::StreamGroup g;
    g.last_delivered_id = gd.last_delivered_id;
    for (const auto& p : gd.pending) {
      g.pending_to_consumer[p.id] = p.consumer;
      g.pending_per_consumer[p.consumer] += 1;
      g.pending_delivery_ms[p.id] = p.delivery_ms;
      g.pending_delivery_count[p.id] = p.delivery_count;
    }
    e.stream_groups()[gd.name] = std::move(g);
  }
}

bool DataStore::xsetid(const std::string& key, const std::string& id, bool& wrongtype, std::string& err) {
  wrongtype = false; err.clear();
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) { err = "ERR The XSETID command requires the key to exist."; return false; }
  if (it->second.type() != ValueType::Stream) { wrongtype = true; return false; }
  std::uint64_t ms = 0, seq = 0;
  if (!parse_stream_id_pair(id, ms, seq)) {
    err = "ERR Invalid stream ID specified as stream command argument";
    return false;
  }
  if (!it->second.stream_entries().empty() && stream_id_gt(it->second.stream_entries().back().first, id)) {
    err = "ERR The ID specified in XSETID is smaller than the target stream top item";
    return false;
  }
  it->second.stream_last_ms() = ms;
  it->second.stream_last_seq() = seq;
  return true;
}

std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> DataStore::xclaim(
    const std::string& key, const std::string& group, const std::string& consumer,
    std::int64_t min_idle_ms, const std::vector<std::string>& ids, bool justid, bool force,
    bool& wrongtype, std::string& err) {
  wrongtype = false; err.clear();
  std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) { err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'"; return out; }
  if (it->second.type() != ValueType::Stream) { wrongtype = true; return out; }
  auto git = it->second.stream_groups().find(group);
  if (git == it->second.stream_groups().end()) { err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'"; return out; }
  auto& g = git->second;
  const std::int64_t now = now_ms();
  auto& entries = it->second.stream_entries();
  for (const auto& id : ids) {
    auto pit = g.pending_to_consumer.find(id);
    const bool in_pel = pit != g.pending_to_consumer.end();
    if (!in_pel && !force) continue;
    auto eit = std::find_if(entries.begin(), entries.end(), [&](const auto& e) { return e.first == id; });
    if (eit == entries.end()) {  // entry was deleted from the stream — drop the dangling PEL entry
      if (in_pel) {
        auto cit = g.pending_per_consumer.find(pit->second);
        if (cit != g.pending_per_consumer.end() && cit->second > 0 && --cit->second == 0) g.pending_per_consumer.erase(cit);
        g.pending_to_consumer.erase(pit);
        g.pending_delivery_ms.erase(id);
        g.pending_delivery_count.erase(id);
      }
      continue;
    }
    if (in_pel) {
      const auto dm = g.pending_delivery_ms.find(id);
      const std::int64_t idle = now - (dm != g.pending_delivery_ms.end() ? dm->second : 0);
      if (idle < min_idle_ms) continue;
      if (pit->second != consumer) {
        auto cit = g.pending_per_consumer.find(pit->second);
        if (cit != g.pending_per_consumer.end() && cit->second > 0 && --cit->second == 0) g.pending_per_consumer.erase(cit);
        pit->second = consumer;
        g.pending_per_consumer[consumer] += 1;
      }
    } else {
      g.pending_to_consumer[id] = consumer;
      g.pending_per_consumer[consumer] += 1;
      g.pending_delivery_count[id] = 0;
    }
    g.pending_delivery_ms[id] = now;
    if (!justid) g.pending_delivery_count[id] += 1;
    out.push_back(*eit);
  }
  return out;
}

DataStore::AutoClaimResult DataStore::xautoclaim(
    const std::string& key, const std::string& group, const std::string& consumer,
    std::int64_t min_idle_ms, const std::string& start, std::int64_t count, bool justid,
    bool& wrongtype, std::string& err) {
  wrongtype = false; err.clear();
  AutoClaimResult res;
  res.next_cursor = "0-0";
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) { err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'"; return res; }
  if (it->second.type() != ValueType::Stream) { wrongtype = true; return res; }
  auto git = it->second.stream_groups().find(group);
  if (git == it->second.stream_groups().end()) { err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'"; return res; }
  auto& g = git->second;
  const std::int64_t now = now_ms();
  if (count <= 0) count = 100;
  auto& entries = it->second.stream_entries();
  std::int64_t claimed = 0;
  for (const auto& e : entries) {
    if (stream_id_gt(start, e.first)) continue;          // skip ids before the cursor
    auto pit = g.pending_to_consumer.find(e.first);
    if (pit == g.pending_to_consumer.end()) continue;    // not pending
    const auto dm = g.pending_delivery_ms.find(e.first);
    const std::int64_t idle = now - (dm != g.pending_delivery_ms.end() ? dm->second : 0);
    if (idle < min_idle_ms) continue;
    if (claimed >= count) { res.next_cursor = e.first; break; }  // more remain → cursor here
    if (pit->second != consumer) {
      auto cit = g.pending_per_consumer.find(pit->second);
      if (cit != g.pending_per_consumer.end() && cit->second > 0 && --cit->second == 0) g.pending_per_consumer.erase(cit);
      pit->second = consumer;
      g.pending_per_consumer[consumer] += 1;
    }
    g.pending_delivery_ms[e.first] = now;
    if (!justid) g.pending_delivery_count[e.first] += 1;
    res.claimed.push_back(e);
    ++claimed;
  }
  return res;
}

bool DataStore::save_snapshot_file(const std::filesystem::path& path, std::string& err) {
  err.clear();
  std::ofstream out(path);
  if (!out.is_open()) {
    err = "cannot open snapshot for write";
    return false;
  }
  out << "PEADB-SNAPSHOT-V1\n";
  for (std::size_t dbi = 0; dbi < dbs_.size(); ++dbi) {
    for (const auto& [key, e] : dbs_[dbi]) {
      const auto ttl = e.expire_at_ms.has_value() ? std::to_string(*e.expire_at_ms) : std::string("-1");
      switch (e.type()) {
        case ValueType::String:
          out << "S " << dbi << " " << ttl << " " << std::quoted(key) << " " << std::quoted(e.str()) << "\n";
          break;
        case ValueType::Hash:
          out << "H " << dbi << " " << ttl << " " << std::quoted(key) << " " << e.hash().size();
          for (const auto& [f, v] : e.hash()) out << " " << std::quoted(f) << " " << std::quoted(v);
          out << "\n";
          break;
        case ValueType::List:
          out << "L " << dbi << " " << ttl << " " << std::quoted(key) << " " << e.list().size();
          for (const auto& v : e.list()) out << " " << std::quoted(v);
          out << "\n";
          break;
        case ValueType::Set:
          out << "T " << dbi << " " << ttl << " " << std::quoted(key) << " " << e.set().size();
          for (const auto& m : e.set()) out << " " << std::quoted(m);
          out << "\n";
          break;
        case ValueType::ZSet:
          out << "Z " << dbi << " " << ttl << " " << std::quoted(key) << " " << e.zset().size();
          for (const auto& [m, s] : e.zset()) out << " " << std::quoted(m) << " " << s;
          out << "\n";
          break;
        case ValueType::Stream:
          out << "X " << dbi << " " << ttl << " " << std::quoted(key) << " " << e.stream_entries().size() << " "
              << e.stream_last_ms() << " " << e.stream_last_seq();
          for (const auto& [id, fields] : e.stream_entries()) {
            out << " " << std::quoted(id) << " " << fields.size();
            for (const auto& [f, v] : fields) out << " " << std::quoted(f) << " " << std::quoted(v);
          }
          out << "\n";
          break;
      }
    }
  }
  return true;
}

bool DataStore::load_snapshot_file(const std::filesystem::path& path, std::string& err) {
  err.clear();
  std::ifstream in(path);
  if (!in.is_open()) {
    err = "cannot open snapshot for read";
    return false;
  }
  std::string header;
  if (!std::getline(in, header) || header != "PEADB-SNAPSHOT-V1") {
    err = "invalid snapshot header";
    return false;
  }

  for (auto& db : dbs_) db.clear();
  const int prev_db = current_db_;
  current_db_ = 0;

  std::string line;
  while (std::getline(in, line)) {
    if (line.empty()) continue;
    std::istringstream iss(line);
    std::string typ, ttl_s, key;
    std::size_t dbi = 0;
    if (!(iss >> typ >> dbi >> ttl_s >> std::quoted(key)) || dbi >= dbs_.size()) {
      err = "malformed snapshot line";
      current_db_ = prev_db;
      return false;
    }
    current_db_ = static_cast<int>(dbi);
    std::optional<std::int64_t> ttl;
    if (ttl_s != "-1") ttl = std::stoll(ttl_s);

    if (typ == "S") {
      std::string value;
      if (!(iss >> std::quoted(value))) { err = "malformed string"; current_db_ = prev_db; return false; }
      set(key, value, false, false, ttl, false);
    } else if (typ == "H") {
      std::size_t n = 0;
      if (!(iss >> n)) { err = "malformed hash"; current_db_ = prev_db; return false; }
      std::vector<std::pair<std::string, std::string>> fields;
      for (std::size_t i = 0; i < n; ++i) {
        std::string f, v;
        if (!(iss >> std::quoted(f) >> std::quoted(v))) { err = "malformed hash field"; current_db_ = prev_db; return false; }
        fields.push_back({f, v});
      }
      bool wrong = false;
      (void)hset(key, fields, wrong);
      if (ttl.has_value()) expire(key, *ttl);
    } else if (typ == "L") {
      std::size_t n = 0;
      if (!(iss >> n)) { err = "malformed list"; current_db_ = prev_db; return false; }
      std::vector<std::string> values;
      for (std::size_t i = 0; i < n; ++i) {
        std::string v;
        if (!(iss >> std::quoted(v))) { err = "malformed list value"; current_db_ = prev_db; return false; }
        values.push_back(v);
      }
      bool wrong = false;
      (void)rpush(key, values, wrong);
      if (ttl.has_value()) expire(key, *ttl);
    } else if (typ == "T") {
      std::size_t n = 0;
      if (!(iss >> n)) { err = "malformed set"; current_db_ = prev_db; return false; }
      std::vector<std::string> members;
      for (std::size_t i = 0; i < n; ++i) {
        std::string m;
        if (!(iss >> std::quoted(m))) { err = "malformed set member"; current_db_ = prev_db; return false; }
        members.push_back(m);
      }
      bool wrong = false;
      (void)sadd(key, members, wrong);
      if (ttl.has_value()) expire(key, *ttl);
    } else if (typ == "Z") {
      std::size_t n = 0;
      if (!(iss >> n)) { err = "malformed zset"; current_db_ = prev_db; return false; }
      for (std::size_t i = 0; i < n; ++i) {
        std::string m;
        double sc = 0;
        if (!(iss >> std::quoted(m) >> sc)) { err = "malformed zset member"; current_db_ = prev_db; return false; }
        (void)zadd_one(key, sc, m, false, false, false, false, false);
      }
      if (ttl.has_value()) expire(key, *ttl);
    } else if (typ == "X") {
      std::size_t n = 0;
      std::uint64_t lm = 0, ls = 0;
      if (!(iss >> n >> lm >> ls)) { err = "malformed stream"; current_db_ = prev_db; return false; }
      for (std::size_t i = 0; i < n; ++i) {
        std::string id;
        std::size_t fn = 0;
        if (!(iss >> std::quoted(id) >> fn)) { err = "malformed stream item"; current_db_ = prev_db; return false; }
        std::vector<std::pair<std::string, std::string>> fields;
        for (std::size_t j = 0; j < fn; ++j) {
          std::string f, v;
          if (!(iss >> std::quoted(f) >> std::quoted(v))) { err = "malformed stream field"; current_db_ = prev_db; return false; }
          fields.push_back({f, v});
        }
        bool wrong = false;
        std::string e;
        (void)xadd(key, id, fields, wrong, e);
      }
      if (ttl.has_value()) expire(key, *ttl);
    }
  }

  current_db_ = prev_db;
  return true;
}

std::uint64_t DataStore::mutation_epoch() const { return mutation_epoch_; }
void DataStore::increment_mutation_epoch() { ++mutation_epoch_; }

std::int64_t DataStore::entry_cost(const std::string& k, const Entry& e) const {
  // Per-key fixed overhead: the map node's key string + the (now small) Entry.
  std::int64_t total = static_cast<std::int64_t>(sizeof(k) + k.size() + sizeof(e));
  // Only the active payload is allocated now (tagged union), so account per type —
  // and add the heap container's own sizeof, which no longer rides inside Entry.
  switch (e.type()) {
    case ValueType::String:
      total += static_cast<std::int64_t>(e.str().size());
      break;
    case ValueType::Hash:
      total += static_cast<std::int64_t>(sizeof(Entry::HashT));
      for (const auto& [f, v] : e.hash()) total += static_cast<std::int64_t>(f.size() + v.size());
      break;
    case ValueType::List:
      total += static_cast<std::int64_t>(sizeof(Entry::ListT));
      for (const auto& v : e.list()) total += static_cast<std::int64_t>(v.size());
      break;
    case ValueType::Set:
      total += static_cast<std::int64_t>(sizeof(Entry::SetT));
      for (const auto& m : e.set()) total += static_cast<std::int64_t>(m.size());
      break;
    case ValueType::ZSet:
      total += static_cast<std::int64_t>(sizeof(Entry::ZSetT));
      for (const auto& [m, _] : e.zset()) total += static_cast<std::int64_t>(m.size() + sizeof(double));
      break;
    case ValueType::Stream:
      // Count entries + consumer-group PEL so stream-heavy datasets are not invisible
      // to used_memory / maxmemory.
      total += static_cast<std::int64_t>(sizeof(Entry::StreamT));
      for (const auto& [id, fields] : e.stream_entries()) {
        total += static_cast<std::int64_t>(id.size());
        for (const auto& [f, v] : fields) total += static_cast<std::int64_t>(f.size() + v.size());
      }
      for (const auto& [gname, grp] : e.stream_groups()) {
        total += static_cast<std::int64_t>(gname.size() + grp.last_delivered_id.size());
        for (const auto& [id, c] : grp.pending_to_consumer) total += static_cast<std::int64_t>(id.size() + c.size());
      }
      break;
  }
  return total;
}

std::int64_t DataStore::estimate_used_memory() const {
  std::int64_t total = sizeof(*this);
  for (const auto& db : dbs_)
    for (const auto& [k, e] : db) total += entry_cost(k, e);
  return total;
}

std::int64_t DataStore::used_memory() const {
  const std::int64_t now = now_ms();
  constexpr std::int64_t kRecomputeIntervalMs = 100;
  if (used_mem_cache_ < 0 || now - used_mem_cache_ms_ >= kRecomputeIntervalMs) {
    used_mem_cache_ = estimate_used_memory();
    used_mem_cache_ms_ = now;
  }
  return used_mem_cache_;
}

// ── Eviction (maxmemory-policy) ──

void DataStore::touch_lru(Entry& e, bool lfu) {
  if (lfu) {
    // Approximate LFU: the low 8 bits of `lru` are a logarithmic access counter (Redis
    // LFU_INIT_VAL=5). Increment probabilistically so the counter saturates slowly — the
    // higher the count, the lower the increment probability. (No decay-over-time yet; a
    // documented approximation. The high bits are unused in LFU mode.)
    std::uint32_t counter = e.lru & 0xFF;
    if (counter < 255) {
      static thread_local std::mt19937 rng{std::random_device{}()};
      const double r = static_cast<double>(rng()) / static_cast<double>(rng.max());
      const double base = counter > 5 ? counter - 5 : 0;
      const double p = 1.0 / (base * /*lfu-log-factor*/ 10.0 + 1.0);
      if (r < p) ++counter;
    }
    e.lru = counter;
  } else {
    // LRU: store a coarse millisecond access clock (truncated to 32 bits — wraps ~49
    // days, which only risks treating a 49-day-idle key as recent; acceptable for a cache).
    e.lru = static_cast<std::uint32_t>(now_ms() & 0xFFFFFFFF);
  }
}

void DataStore::touch_key_for_eviction(const std::string& key) {
  auto& db = dbs_[current_db_];
  auto it = db.find(key);
  if (it != db.end()) touch_lru(it->second, eviction_lfu_);
}

std::optional<std::int64_t> DataStore::object_idletime_seconds(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  const auto now = static_cast<std::uint32_t>(now_ms() & 0xFFFFFFFF);
  const std::uint32_t last = it->second.lru;
  return static_cast<std::int64_t>(now >= last ? now - last : 0) / 1000;
}

std::optional<std::int64_t> DataStore::object_freq(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  return static_cast<std::int64_t>(it->second.lru & 0xFF);
}

std::optional<DataStore::EvictVictim> DataStore::evict_one(MaxmemoryPolicy policy, int samples) {
  if (policy == MaxmemoryPolicy::NoEviction) return std::nullopt;
  const bool volatile_only = policy == MaxmemoryPolicy::VolatileLru ||
                             policy == MaxmemoryPolicy::VolatileLfu ||
                             policy == MaxmemoryPolicy::VolatileRandom ||
                             policy == MaxmemoryPolicy::VolatileTtl;
  const bool lfu = policy == MaxmemoryPolicy::AllkeysLfu || policy == MaxmemoryPolicy::VolatileLfu;
  const bool lru = policy == MaxmemoryPolicy::AllkeysLru || policy == MaxmemoryPolicy::VolatileLru;
  const bool ttl = policy == MaxmemoryPolicy::VolatileTtl;
  if (samples < 1) samples = 1;

  static thread_local std::mt19937 rng{std::random_device{}()};
  const std::int64_t now = now_ms();

  // Sample up to `samples` eligible keys across all DBs and keep the best victim.
  // Eligible = has a TTL for volatile-*, any key otherwise. We sample by walking from a
  // random (db,bucket) start until we've gathered `samples` candidates or exhausted the
  // keyspace — the same bucket-walk RANDOMKEY uses, generalized across databases.
  struct Cand { int db; std::string key; std::uint32_t lru; std::int64_t expire; };
  std::optional<Cand> best;
  auto better = [&](const Cand& c) -> bool {
    if (!best) return true;
    if (ttl)  return c.expire < best->expire;          // soonest to expire
    if (lfu)  return (c.lru & 0xFF) < (best->lru & 0xFF); // lowest frequency
    if (lru)  return c.lru < best->lru;                // oldest access clock
    return (rng() & 1) != 0;                            // random: keep flipping
  };

  int collected = 0;
  const std::size_t ndb = dbs_.size();
  const std::size_t db_start = rng() % (ndb ? ndb : 1);
  for (std::size_t di = 0; di < ndb && collected < samples; ++di) {
    auto& db = dbs_[(db_start + di) % ndb];
    if (db.empty()) continue;
    const std::size_t nb = db.bucket_count();
    if (nb == 0) continue;
    const std::size_t bstart = rng() % nb;
    for (std::size_t bi = 0; bi < nb && collected < samples; ++bi) {
      const std::size_t bucket = (bstart + bi) % nb;
      for (auto it = db.begin(bucket); it != db.end(bucket) && collected < samples; ++it) {
        const Entry& e = it->second;
        const bool has_ttl = e.expire_at_ms.has_value();
        if (has_ttl && *e.expire_at_ms <= now) continue;  // already logically expired
        if (volatile_only && !has_ttl) continue;
        Cand c{static_cast<int>((db_start + di) % ndb), it->first, e.lru,
               has_ttl ? *e.expire_at_ms : std::numeric_limits<std::int64_t>::max()};
        if (better(c)) best = c;
        ++collected;
      }
    }
  }
  if (!best) return std::nullopt;  // nothing evictable (e.g. volatile-* with no TTL keys)

  auto& vdb = dbs_[static_cast<std::size_t>(best->db)];
  auto vit = vdb.find(best->key);
  if (vit == vdb.end()) return std::nullopt;
  const std::int64_t freed = entry_cost(best->key, vit->second);
  vdb.erase(vit);
  record_evicted();
  // Keep the cached used_memory live so the caller's eviction loop sees memory fall
  // immediately rather than over-evicting against the ~100ms-stale cache.
  if (used_mem_cache_ >= 0) {
    used_mem_cache_ -= freed;
    if (used_mem_cache_ < 0) used_mem_cache_ = 0;
  }
  return EvictVictim{best->db, best->key, freed};
}

std::tuple<std::size_t, std::size_t, std::int64_t> DataStore::keyspace_stats() const {
  const auto& db = dbs_[current_db_];
  const auto now = now_ms();
  std::size_t total = 0, with_expiry = 0;
  std::int64_t ttl_sum = 0;
  for (const auto& [k, e] : db) {
    ++total;
    if (e.expire_at_ms.has_value()) {
      ++with_expiry;
      const auto ttl = *e.expire_at_ms - now;
      if (ttl > 0) ttl_sum += ttl;
    }
  }
  const std::int64_t avg_ttl = with_expiry > 0 ? ttl_sum / static_cast<std::int64_t>(with_expiry) : 0;
  return {total, with_expiry, avg_ttl};
}

std::vector<std::vector<std::string>> DataStore::export_aof_commands() const {
  std::vector<std::vector<std::string>> out;
  for (std::size_t dbi = 0; dbi < dbs_.size(); ++dbi) {
    if (dbs_[dbi].empty()) continue;
    out.push_back({"SELECT", std::to_string(dbi)});
    for (const auto& [key, e] : dbs_[dbi]) {
      switch (e.type()) {
        case ValueType::String:
          out.push_back({"SET", key, e.str()});
          break;
        case ValueType::Hash: {
          std::vector<std::string> cmd{"HSET", key};
          for (const auto& [f, v] : e.hash()) { cmd.push_back(f); cmd.push_back(v); }
          out.push_back(std::move(cmd));
          break;
        }
        case ValueType::List: {
          std::vector<std::string> cmd{"RPUSH", key};
          for (const auto& v : e.list()) cmd.push_back(v);
          out.push_back(std::move(cmd));
          break;
        }
        case ValueType::Set: {
          std::vector<std::string> cmd{"SADD", key};
          for (const auto& m : e.set()) cmd.push_back(m);
          out.push_back(std::move(cmd));
          break;
        }
        case ValueType::ZSet: {
          std::vector<std::string> cmd{"ZADD", key};
          for (const auto& [m, s] : e.zset()) { cmd.push_back(fmt_double(s)); cmd.push_back(m); }
          out.push_back(std::move(cmd));
          break;
        }
        case ValueType::Stream: {
          for (const auto& [id, fields] : e.stream_entries()) {
            std::vector<std::string> cmd{"XADD", key, id};
            for (const auto& [f, v] : fields) { cmd.push_back(f); cmd.push_back(v); }
            out.push_back(std::move(cmd));
          }
          break;
        }
      }
      if (e.expire_at_ms.has_value()) {
        out.push_back({"PEXPIREAT", key, std::to_string(*e.expire_at_ms)});
      }
    }
  }
  return out;
}

std::optional<std::string> DataStore::object_encoding(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  auto& e = it->second;  // non-const: computing a big encoding sets the sticky flag
  const auto& lim = encoding_limits_;
  // Redis string2ll-style integer check for the "int" / intset encodings: digits with
  // optional '-', no leading zeros (except "0" itself), must fit in int64.
  auto is_int = [](const std::string& s) -> bool {
    if (s.empty()) return false;
    std::size_t i = (s[0] == '-' ? 1 : 0);
    if (i >= s.size()) return false;
    if (s[i] == '0' && s.size() - i > 1) return false;  // leading zero
    for (std::size_t j = i; j < s.size(); ++j) {
      if (!std::isdigit(static_cast<unsigned char>(s[j]))) return false;
    }
    try {
      std::size_t consumed = 0;
      (void)std::stoll(s, &consumed);
      return consumed == s.size();
    } catch (...) {
      return false;
    }
  };
  // Entry-count thresholds: <= 0 disables the small encoding entirely (Redis semantics
  // for `config set hash-max-listpack-entries 0`); negative list size-classes ~ 128.
  auto within = [](std::size_t n, std::int64_t limit) {
    return limit > 0 && n <= static_cast<std::size_t>(limit);
  };
  auto sticky = [&e](bool small, const char* small_enc, const char* big_enc) {
    if (!small) e.encoding_big = true;
    return std::optional<std::string>(e.encoding_big ? big_enc : small_enc);
  };
  if (e.type() == ValueType::String) {
    if (e.string_force_raw) return "raw";
    if (is_int(e.str())) return "int";
    return e.str().size() <= 44 ? "embstr" : "raw";  // OBJ_ENCODING_EMBSTR_SIZE_LIMIT
  }
  if (e.type() == ValueType::List) {
    // Redis 7.2 lists convert listpack<->quicklist in BOTH directions, so this is
    // recomputed each call (no sticky flag). Byte estimates approximate the listpack
    // layout — exact parity needs the real backend (compat/delta.md item 13).
    bool small;
    if (lim.list_size < 0) {
      // Negative size class: total serialized bytes fit the node budget
      // (-1=4KB, -2=8KB, ... -5=64KB); values below -5 are clamped to -5 (Redis).
      int cls = static_cast<int>(-lim.list_size);
      if (cls > 5) cls = 5;
      const std::size_t budget = std::size_t{4096} << (cls - 1);
      std::size_t bytes = 11;  // listpack header + terminator
      small = true;
      for (const auto& v : e.list()) {
        // A listpack element is ~1-5 bytes for a small integer, else len + a small
        // header + 1-byte backlen; approximate with size + 2 (min 3 for ints).
        bytes += (v.size() <= 2 ? 3 : v.size() + 2);
        if (bytes > budget) { small = false; break; }
      }
    } else {
      // 0 is treated as 1 (a single element is still listpack).
      const std::int64_t entry_limit = lim.list_size == 0 ? 1 : lim.list_size;
      small = within(e.list().size(), entry_limit);
      if (small) for (const auto& v : e.list()) if (v.size() > 64) { small = false; break; }
    }
    return small ? "listpack" : "quicklist";
  }
  if (e.type() == ValueType::Set) {
    bool all_int = true;
    for (const auto& v : e.set()) if (!is_int(v)) { all_int = false; break; }
    if (!e.encoding_big && !e.encoding_no_intset && all_int &&
        within(e.set().size(), lim.set_intset)) return "intset";
    bool small = within(e.set().size(), lim.set_entries);
    if (small) for (const auto& v : e.set()) if (v.size() > static_cast<std::size_t>(lim.set_value)) { small = false; break; }
    return sticky(small, "listpack", "hashtable");
  }
  if (e.type() == ValueType::Hash) {
    bool small = within(e.hash().size(), lim.hash_entries);
    if (small) for (const auto& [f, v] : e.hash()) {
      if (f.size() > static_cast<std::size_t>(lim.hash_value) ||
          v.size() > static_cast<std::size_t>(lim.hash_value)) { small = false; break; }
    }
    return sticky(small, "listpack", "hashtable");
  }
  if (e.type() == ValueType::ZSet) {
    bool small = within(e.zset().size(), lim.zset_entries);
    if (small) for (const auto& [m, _] : e.zset()) {
      if (m.size() > static_cast<std::size_t>(lim.zset_value)) { small = false; break; }
    }
    return sticky(small, "listpack", "skiplist");
  }
  if (e.type() == ValueType::Stream) return "stream";
  return "raw";
}

namespace {
// Canonical, order-independent textual form of an Entry, shared by DEBUG
// DIGEST-VALUE and the whole-dataset DEBUG DIGEST.
std::string entry_digest_payload(const Entry& e) {
  std::ostringstream oss;
  oss << static_cast<int>(e.type()) << "|";
  if (e.expire_at_ms.has_value()) oss << "ttl=" << *e.expire_at_ms << "|";
  else oss << "ttl=-1|";
  if (e.type() == ValueType::String) {
    oss << e.str();
  } else if (e.type() == ValueType::Hash) {
    std::vector<std::pair<std::string, std::string>> items(e.hash().begin(), e.hash().end());
    std::sort(items.begin(), items.end());
    for (const auto& [f, v] : items) oss << f << "=" << v << ";";
  } else if (e.type() == ValueType::List) {
    for (const auto& v : e.list()) oss << v << ";";
  } else if (e.type() == ValueType::Set) {
    std::vector<std::string> items(e.set().begin(), e.set().end());
    std::sort(items.begin(), items.end());
    for (const auto& v : items) oss << v << ";";
  } else if (e.type() == ValueType::ZSet) {
    std::vector<std::pair<std::string, double>> items(e.zset().begin(), e.zset().end());
    std::sort(items.begin(), items.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    for (const auto& [m, s] : items) oss << m << "=" << s << ";";
  } else if (e.type() == ValueType::Stream) {
    for (const auto& [id, fields] : e.stream_entries()) {
      oss << id << "{";
      for (const auto& [f, v] : fields) oss << f << "=" << v << ";";
      oss << "}";
    }
  }
  return oss.str();
}
}  // namespace

std::optional<std::string> DataStore::debug_digest_value(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  const auto h = std::hash<std::string>{}(entry_digest_payload(it->second));
  std::ostringstream out;
  out << std::hex << h;
  return out.str();
}

bool DataStore::key_exists_raw(const std::string& key) const {
  return dbs_[current_db_].count(key) > 0;
}

std::string DataStore::debug_digest() {
  // Order-independent whole-dataset digest: XOR of per-key hashes over a canonical
  // payload (db index + key + typed value + ttl). Only ever compared PeaDB-vs-PeaDB
  // (pre/post DEBUG RELOAD, master vs replica), so no Redis byte parity is needed —
  // but it must be stable across processes of the same build, which std::hash is.
  // 40 hex chars like Redis; all-zeros for an empty dataset.
  std::uint64_t acc = 0;
  const auto now = now_ms();
  for (std::size_t dbi = 0; dbi < dbs_.size(); ++dbi) {
    for (const auto& [key, e] : dbs_[dbi]) {
      if (e.expire_at_ms.has_value() && *e.expire_at_ms <= now) continue;  // lazily expired
      const auto payload = std::to_string(dbi) + "|" + key + "|" + entry_digest_payload(e);
      acc ^= std::hash<std::string>{}(payload);
    }
  }
  char buf[17];
  std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(acc));
  return std::string(24, '0') + buf;
}

void DataStore::set_zset_max_ziplist_entries(std::int64_t v) {
  encoding_limits_.zset_entries = v;
}

std::int64_t DataStore::zset_max_ziplist_entries() const {
  return encoding_limits_.zset_entries;
}


std::vector<std::pair<int, std::string>> DataStore::active_expire_cycle(std::size_t budget_per_db) {
  std::vector<std::pair<int, std::string>> expired;
  const auto now = now_ms();
  static thread_local std::mt19937 rng{std::random_device{}()};

  for (std::size_t dbi = 0; dbi < dbs_.size(); ++dbi) {
    auto& db = dbs_[dbi];
    if (db.empty()) continue;
    const std::size_t num_buckets = db.bucket_count();
    // Start from a random bucket to avoid always skipping tail entries
    const std::size_t start_bucket = rng() % num_buckets;
    std::size_t scanned = 0;
    std::size_t bucket = start_bucket;

    do {
      // Collect expired keys in this bucket, then erase by key (local_iterator can't be passed to erase)
      std::vector<std::string> to_del;
      for (auto it = db.begin(bucket); it != db.end(bucket) && scanned < budget_per_db; ++it, ++scanned) {
        if (it->second.expire_at_ms.has_value() && *it->second.expire_at_ms <= now) {
          to_del.push_back(it->first);
        }
      }
      for (const auto& k : to_del) {
        db.erase(k);
        record_expired();
        expired.emplace_back(static_cast<int>(dbi), k);
      }
      bucket = (bucket + 1) % num_buckets;
    } while (scanned < budget_per_db && bucket != start_bucket);
  }
  return expired;
}

void DataStore::flushall() {
  for (auto& db : dbs_) db.clear();
}

void DataStore::flushdb() {
  dbs_[current_db_].clear();
}

// ── Lazyfree (background destruction of detached data) ──

DataStore::~DataStore() {
  {
    std::lock_guard<std::mutex> lk(lazyfree_mu_);
    lazyfree_stop_ = true;
  }
  lazyfree_cv_.notify_all();
  if (lazyfree_thread_.joinable()) lazyfree_thread_.join();
}

void DataStore::ensure_lazyfree_thread() {
  if (lazyfree_started_) return;
  lazyfree_started_ = true;
  lazyfree_thread_ = std::thread([this] {
    for (;;) {
      std::deque<DB> batch;
      {
        std::unique_lock<std::mutex> lk(lazyfree_mu_);
        lazyfree_cv_.wait(lk, [this] { return lazyfree_stop_ || !lazyfree_queue_.empty(); });
        if (lazyfree_queue_.empty() && lazyfree_stop_) return;
        batch.swap(lazyfree_queue_);
      }
      // Destruct the detached maps (and every Entry/payload they own) here, off the
      // event-loop thread. These maps are exclusively owned by `batch` — moved out of the
      // live keyspace before handoff — so there is no race with command processing.
      batch.clear();
    }
  });
}

void DataStore::lazyfree_enqueue(DB&& db) {
  if (db.empty()) return;
  ensure_lazyfree_thread();
  {
    std::lock_guard<std::mutex> lk(lazyfree_mu_);
    lazyfree_queue_.push_back(std::move(db));
  }
  lazyfree_cv_.notify_one();
}

bool DataStore::entry_is_big(const Entry& e) {
  // Threshold mirrors Redis's lazyfree heuristic (~64 elements): below it, inline free is
  // cheaper than the queue/thread handoff.
  constexpr std::size_t kBig = 64;
  switch (e.type()) {
    case ValueType::String: return false;
    case ValueType::Hash:   return e.hash().size() > kBig;
    case ValueType::List:   return e.list().size() > kBig;
    case ValueType::Set:    return e.set().size() > kBig;
    case ValueType::ZSet:   return e.zset().size() > kBig;
    case ValueType::Stream: return e.stream_entries().size() > kBig;
  }
  return false;
}

bool DataStore::unlink(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  if (entry_is_big(it->second)) {
    DB tmp;
    tmp.insert(db.extract(it));   // O(1) node move out of the live map
    lazyfree_enqueue(std::move(tmp));
  } else {
    db.erase(it);
  }
  return true;
}

void DataStore::flushdb_async() {
  DB tmp;
  std::swap(tmp, dbs_[current_db_]);   // O(1): live db becomes empty immediately
  lazyfree_enqueue(std::move(tmp));
}

void DataStore::flushall_async() {
  for (auto& db : dbs_) {
    DB tmp;
    std::swap(tmp, db);
    lazyfree_enqueue(std::move(tmp));
  }
}

std::size_t DataStore::dbsize() {
  return dbs_[current_db_].size();
}

namespace {
// Advance a cursor for SCAN iteration over `num_buckets` buckets.
// Uses reverse-binary-increment when num_buckets is a power of 2 (gives better
// resize resilience), otherwise falls back to linear increment.
// RBI with non-power-of-2 sizes produces out-of-range bucket indices (UB).
std::uint64_t scan_next_cursor(std::uint64_t cursor, std::uint64_t num_buckets) {
  if (num_buckets == 0) return 0;
  const bool is_pow2 = (num_buckets & (num_buckets - 1)) == 0;
  if (!is_pow2) return (cursor + 1) % num_buckets;
  const std::uint64_t mask = num_buckets - 1;
  auto rev64 = [](std::uint64_t v) -> std::uint64_t {
    v = ((v >> 1) & 0x5555555555555555ULL) | ((v & 0x5555555555555555ULL) << 1);
    v = ((v >> 2) & 0x3333333333333333ULL) | ((v & 0x3333333333333333ULL) << 2);
    v = ((v >> 4) & 0x0F0F0F0F0F0F0F0FULL) | ((v & 0x0F0F0F0F0F0F0F0FULL) << 4);
    v = ((v >> 8) & 0x00FF00FF00FF00FFULL) | ((v & 0x00FF00FF00FF00FFULL) << 8);
    v = ((v >> 16) & 0x0000FFFF0000FFFFULL) | ((v & 0x0000FFFF0000FFFFULL) << 16);
    v = (v >> 32) | (v << 32);
    return v;
  };
  cursor |= ~mask;
  cursor = rev64(cursor);
  cursor++;
  return rev64(cursor) & mask;
}
} // namespace

std::pair<std::uint64_t, std::vector<std::string>> DataStore::scan(
    std::uint64_t cursor, std::size_t count, const std::string& match, const std::string& type) {
  auto& db = dbs_[current_db_];
  std::vector<std::string> out;
  const auto now = now_ms();
  const std::uint64_t num_buckets = db.bucket_count();

  if (num_buckets == 0 || db.empty()) return {0, out};

  const bool is_pow2 = (num_buckets & (num_buckets - 1)) == 0;

  auto emit_bucket = [&](std::uint64_t b) {
    for (auto it = db.begin(b); it != db.end(b); ++it) {
      if (it->second.expire_at_ms.has_value() && *it->second.expire_at_ms <= now) continue;
      if (match != "*" && fnmatch(match.c_str(), it->first.c_str(), 0) != 0) continue;
      if (!type.empty()) {
        std::string entry_type;
        switch (it->second.type()) {
          case ValueType::String: entry_type = "string"; break;
          case ValueType::Hash: entry_type = "hash"; break;
          case ValueType::List: entry_type = "list"; break;
          case ValueType::Set: entry_type = "set"; break;
          case ValueType::ZSet: entry_type = "zset"; break;
          case ValueType::Stream: entry_type = "stream"; break;
        }
        if (entry_type != type) continue;
      }
      out.push_back(it->first);
    }
  };

  if (!is_pow2) {
    // For non-power-of-2 bucket counts (libstdc++ prime-sized tables): advance
    // monotonically from cursor without wrapping. This guarantees termination
    // because the cursor increases from 0 to num_buckets-1 and we return 0 when
    // we fall off the end. Wrapping would allow the cursor to revisit buckets it
    // has already seen, producing cycles that never return 0.
    std::uint64_t b = cursor % num_buckets;
    while (b < num_buckets) {
      emit_bucket(b);
      ++b;
      if (out.size() >= count) break;
    }
    return {(b >= num_buckets) ? 0 : b, out};
  }

  // Power-of-2 path: reverse-binary-increment for resize resilience.
  std::uint64_t bucket = cursor % num_buckets;
  std::size_t buckets_visited = 0;

  while (buckets_visited < num_buckets) {
    emit_bucket(bucket);
    ++buckets_visited;
    bucket = scan_next_cursor(bucket, num_buckets);
    if (bucket == cursor % num_buckets) { bucket = 0; break; } // full cycle
    if (out.size() >= count) break;
  }

  const std::uint64_t next = (buckets_visited >= num_buckets) ? 0 : bucket;
  return {next, out};
}

std::vector<std::string> DataStore::collect_expired_keys() {
  std::vector<std::string> out;
  auto& db = dbs_[current_db_];
  const auto now = now_ms();
  for (auto it = db.begin(); it != db.end();) {
    if (it->second.expire_at_ms.has_value() && *it->second.expire_at_ms <= now) {
      out.push_back(it->first);
      it = db.erase(it);
    } else {
      ++it;
    }
  }
  return out;
}

bool DataStore::lazy_expire_key(const std::string& key) {
  auto& db = dbs_[current_db_];
  auto it = db.find(key);
  if (it == db.end()) return false;
  if (!it->second.expire_at_ms.has_value()) return false;
  if (*it->second.expire_at_ms > now_ms()) return false;
  db.erase(it);
  return true;
}

DataStore& store() {
  static DataStore s;
  return s;
}

} // namespace peadb
