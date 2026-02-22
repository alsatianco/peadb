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
#include <fnmatch.h>

namespace peadb {

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
  return true;
}

std::optional<std::string> DataStore::get(const std::string& key) {
  auto& db = dbs_[current_db_];
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  if (it->second.expire_at_ms.has_value() && *it->second.expire_at_ms <= now_ms()) {
    db.erase(it);
    return std::nullopt;
  }
  return it->second.value;
}

bool DataStore::set(const std::string& key, const std::string& value, bool nx, bool xx,
                    std::optional<std::int64_t> expire_at_ms, bool keep_ttl) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  const bool exists_now = it != db.end();
  if (nx && exists_now) return false;
  if (xx && !exists_now) return false;

  if (exists_now) {
    auto& e = it->second;
    // Reuse existing entry, clear non-string fields only if type changed
    if (e.type != ValueType::String) {
      e.hash_value.clear();
      e.list_value.clear();
      e.set_value.clear();
      e.zset_value.clear();
      e.stream_entries.clear();
      e.stream_groups.clear();
    }
    e.type = ValueType::String;
    e.value = value;
    e.string_force_raw = false;
    if (!keep_ttl) {
      e.expire_at_ms = expire_at_ms;
    }
  } else {
    auto& e = db[key];
    e.type = ValueType::String;
    e.value = value;
    e.string_force_raw = false;
    e.expire_at_ms = expire_at_ms;
  }
  return true;
}

std::optional<std::string> DataStore::getdel(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  const auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  const auto out = it->second.value;
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
  return it->second.value;
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
  if (it->second.type == ValueType::String) return "string";
  if (it->second.type == ValueType::Hash) return "hash";
  if (it->second.type == ValueType::List) return "list";
  if (it->second.type == ValueType::Set) return "set";
  if (it->second.type == ValueType::ZSet) return "zset";
  if (it->second.type == ValueType::Stream) return "stream";
  return "none";
}

bool DataStore::is_wrongtype_for_string(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  return it->second.type != ValueType::String;
}

bool DataStore::is_wrongtype_for_string_noexpire(const std::string& key) {
  auto& db = dbs_[current_db_];
  auto it = db.find(key);
  if (it == db.end()) return false;
  return it->second.type != ValueType::String;
}

bool DataStore::is_wrongtype_for_hash(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  return it->second.type != ValueType::Hash;
}

bool DataStore::is_wrongtype_for_list(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  return it->second.type != ValueType::List;
}

bool DataStore::is_wrongtype_for_set(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  return it->second.type != ValueType::Set;
}

bool DataStore::is_wrongtype_for_zset(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  return it->second.type != ValueType::ZSet;
}

bool DataStore::is_wrongtype_for_stream(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  return it->second.type != ValueType::Stream;
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
  std::vector<std::string> ks;
  ks.reserve(db.size());
  for (auto it = db.begin(); it != db.end();) {
    if (it->second.expire_at_ms.has_value() && *it->second.expire_at_ms <= now_ms()) {
      ++it;
      continue;
    }
    ks.push_back(it->first);
    ++it;
  }
  if (ks.empty()) return std::nullopt;
  static thread_local std::mt19937 rng{
      static_cast<std::mt19937::result_type>(std::chrono::high_resolution_clock::now().time_since_epoch().count())};
  std::uniform_int_distribution<std::size_t> dist(0, ks.size() - 1);
  return ks[dist(rng)];
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
    try {
      base = std::stoll(*val);
    } catch (...) {
      err = "value is not an integer or out of range";
      return 0;
    }
  }

  const long long out = base + by;
  set(key, std::to_string(out), false, false, std::nullopt, false);
  ok = true;
  return out;
}

std::string DataStore::incrbyfloat(const std::string& key, long double by, bool& ok, std::string& err) {
  ok = false;
  err.clear();

  auto val = get(key);
  long double base = 0.0;
  if (val.has_value()) {
    try {
      base = std::stold(*val);
    } catch (...) {
      err = "value is not a valid float";
      return "";
    }
  }

  long double out = base + by;
  char buf[256];
  std::snprintf(buf, sizeof(buf), "%.17Lg", out);
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
    e.type = ValueType::String;
    e.value = value;
    e.string_force_raw = true;
    db[key] = std::move(e);
    return value.size();
  }
  if (it->second.type != ValueType::String) return 0;
  it->second.value += value;
  it->second.string_force_raw = true;
  return it->second.value.size();
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
    e.type = ValueType::String;
    db[key] = std::move(e);
    it = db.find(key);
  } else if (it->second.type != ValueType::String) {
    err = "WRONGTYPE";
    return 0;
  }

  const std::size_t byte_index = static_cast<std::size_t>(bit_offset / 8ULL);
  const unsigned bit_index = static_cast<unsigned>(bit_offset % 8ULL);
  auto& s = it->second.value;
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
  if (it->second.type != ValueType::String) {
    err = "WRONGTYPE";
    return 0;
  }

  const std::size_t byte_index = static_cast<std::size_t>(bit_offset / 8ULL);
  const unsigned bit_index = static_cast<unsigned>(bit_offset % 8ULL);
  const auto& s = it->second.value;
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
  if (it != db.end() && it->second.type != ValueType::String) {
    err = "WRONGTYPE";
    return 0;
  }

  std::size_t base_len = 0;
  if (it != db.end()) base_len = it->second.value.size();
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
    e.type = ValueType::String;
    db[key] = std::move(e);
    it = db.find(key);
  }
  auto& s = it->second.value;
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
  if (it->second.type != ValueType::String) {
    err = "WRONGTYPE";
    return "";
  }

  const auto& s = it->second.value;
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
    e.type = ValueType::Hash;
    db[key] = std::move(e);
    it = db.find(key);
  } else if (it->second.type != ValueType::Hash) {
    wrongtype = true;
    return 0;
  }

  std::int64_t added = 0;
  for (const auto& [f, v] : fields) {
    if (it->second.hash_value.find(f) == it->second.hash_value.end()) ++added;
    it->second.hash_value[f] = v;
  }
  return added;
}

std::optional<std::string> DataStore::hget(const std::string& key, const std::string& field, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  if (it->second.type != ValueType::Hash) {
    wrongtype = true;
    return std::nullopt;
  }
  auto fit = it->second.hash_value.find(field);
  if (fit == it->second.hash_value.end()) return std::nullopt;
  return fit->second;
}

std::int64_t DataStore::hdel(const std::string& key, const std::vector<std::string>& fields, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type != ValueType::Hash) {
    wrongtype = true;
    return 0;
  }
  std::int64_t removed = 0;
  for (const auto& f : fields) removed += it->second.hash_value.erase(f) ? 1 : 0;
  if (it->second.hash_value.empty()) db.erase(it);
  return removed;
}

std::int64_t DataStore::hlen(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type != ValueType::Hash) {
    wrongtype = true;
    return 0;
  }
  return static_cast<std::int64_t>(it->second.hash_value.size());
}

bool DataStore::hexists(const std::string& key, const std::string& field, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  if (it->second.type != ValueType::Hash) {
    wrongtype = true;
    return false;
  }
  return it->second.hash_value.find(field) != it->second.hash_value.end();
}

std::vector<std::pair<std::string, std::string>> DataStore::hgetall(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  std::vector<std::pair<std::string, std::string>> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return out;
  if (it->second.type != ValueType::Hash) {
    wrongtype = true;
    return out;
  }
  out.reserve(it->second.hash_value.size());
  for (const auto& kv : it->second.hash_value) out.push_back(kv);
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
  if (it->second.type != ValueType::Hash) {
    wrongtype = true;
    return {0, out};
  }

  std::size_t idx = 0;
  std::size_t emitted = 0;
  for (const auto& kv : it->second.hash_value) {
    if (idx++ < cursor) continue;
    out.push_back(kv);
    if (++emitted >= count) break;
  }

  const std::uint64_t next = (cursor + emitted) >= it->second.hash_value.size() ? 0 : (cursor + emitted);
  return {next, out};
}

std::int64_t DataStore::lpush(const std::string& key, const std::vector<std::string>& values, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    Entry e;
    e.type = ValueType::List;
    db[key] = std::move(e);
    it = db.find(key);
  } else if (it->second.type != ValueType::List) {
    wrongtype = true;
    return 0;
  }
  for (const auto& v : values) it->second.list_value.push_front(v);
  return static_cast<std::int64_t>(it->second.list_value.size());
}

std::int64_t DataStore::rpush(const std::string& key, const std::vector<std::string>& values, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    Entry e;
    e.type = ValueType::List;
    db[key] = std::move(e);
    it = db.find(key);
  } else if (it->second.type != ValueType::List) {
    wrongtype = true;
    return 0;
  }
  for (const auto& v : values) it->second.list_value.push_back(v);
  return static_cast<std::int64_t>(it->second.list_value.size());
}

std::optional<std::string> DataStore::lpop(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  if (it->second.type != ValueType::List) {
    wrongtype = true;
    return std::nullopt;
  }
  if (it->second.list_value.empty()) return std::nullopt;
  auto v = it->second.list_value.front();
  it->second.list_value.pop_front();
  if (it->second.list_value.empty()) db.erase(it);
  return v;
}

std::optional<std::string> DataStore::rpop(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  if (it->second.type != ValueType::List) {
    wrongtype = true;
    return std::nullopt;
  }
  if (it->second.list_value.empty()) return std::nullopt;
  auto v = it->second.list_value.back();
  it->second.list_value.pop_back();
  if (it->second.list_value.empty()) db.erase(it);
  return v;
}

std::int64_t DataStore::llen(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type != ValueType::List) {
    wrongtype = true;
    return 0;
  }
  return static_cast<std::int64_t>(it->second.list_value.size());
}

std::vector<std::string> DataStore::lrange(const std::string& key, std::int64_t start, std::int64_t stop, bool& wrongtype) {
  wrongtype = false;
  std::vector<std::string> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return out;
  if (it->second.type != ValueType::List) {
    wrongtype = true;
    return out;
  }

  const auto n = static_cast<std::int64_t>(it->second.list_value.size());
  if (n == 0) return out;

  if (start < 0) start = n + start;
  if (stop < 0) stop = n + stop;
  if (start < 0) start = 0;
  if (stop >= n) stop = n - 1;
  if (start > stop || start >= n) return out;

  for (std::int64_t i = start; i <= stop; ++i) out.push_back(it->second.list_value[static_cast<std::size_t>(i)]);
  return out;
}

std::int64_t DataStore::sadd(const std::string& key, const std::vector<std::string>& members, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    Entry e;
    e.type = ValueType::Set;
    db[key] = std::move(e);
    it = db.find(key);
  } else if (it->second.type != ValueType::Set) {
    wrongtype = true;
    return 0;
  }
  std::int64_t added = 0;
  for (const auto& m : members) added += it->second.set_value.insert(m).second ? 1 : 0;
  return added;
}

std::int64_t DataStore::srem(const std::string& key, const std::vector<std::string>& members, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type != ValueType::Set) {
    wrongtype = true;
    return 0;
  }
  std::int64_t removed = 0;
  for (const auto& m : members) removed += it->second.set_value.erase(m) ? 1 : 0;
  if (it->second.set_value.empty()) db.erase(it);
  return removed;
}

bool DataStore::sismember(const std::string& key, const std::string& member, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  if (it->second.type != ValueType::Set) {
    wrongtype = true;
    return false;
  }
  return it->second.set_value.find(member) != it->second.set_value.end();
}

std::vector<std::string> DataStore::smembers(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  std::vector<std::string> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return out;
  if (it->second.type != ValueType::Set) {
    wrongtype = true;
    return out;
  }
  out.reserve(it->second.set_value.size());
  for (const auto& m : it->second.set_value) out.push_back(m);
  return out;
}

std::int64_t DataStore::scard(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type != ValueType::Set) {
    wrongtype = true;
    return 0;
  }
  return static_cast<std::int64_t>(it->second.set_value.size());
}

std::optional<std::string> DataStore::spop(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  if (it->second.type != ValueType::Set) {
    wrongtype = true;
    return std::nullopt;
  }
  if (it->second.set_value.empty()) return std::nullopt;
  const auto pick = it->second.set_value.begin();
  const std::string value = *pick;
  it->second.set_value.erase(pick);
  if (it->second.set_value.empty()) db.erase(it);
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
  if (it->second.type != ValueType::Set) {
    wrongtype = true;
    return {0, out};
  }

  std::size_t idx = 0, emitted = 0;
  for (const auto& m : it->second.set_value) {
    if (idx++ < cursor) continue;
    out.push_back(m);
    if (++emitted >= count) break;
  }
  const auto next = (cursor + emitted) >= it->second.set_value.size() ? 0 : (cursor + emitted);
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
    e.type = ValueType::ZSet;
    db[key] = std::move(e);
    it = db.find(key);
  } else if (it->second.type != ValueType::ZSet) {
    r.wrongtype = true;
    return r;
  }

  auto mit = it->second.zset_value.find(member);
  const bool exists = mit != it->second.zset_value.end();
  if (nx && exists) return r;
  if (xx && !exists) return r;

  double new_score = score;
  if (exists && incr) new_score = mit->second + score;
  if (exists && gt && new_score <= mit->second) return r;
  if (exists && lt && new_score >= mit->second) return r;

  if (!exists) {
    it->second.zset_value[member] = new_score;
    r.added = true;
    r.changed = true;
    r.score = new_score;
    return r;
  }

  if (mit->second != new_score) {
    mit->second = new_score;
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
  if (it->second.type != ValueType::ZSet) {
    wrongtype = true;
    return out;
  }

  std::vector<std::pair<std::string, double>> items(it->second.zset_value.begin(), it->second.zset_value.end());
  std::sort(items.begin(), items.end(), [](const auto& a, const auto& b) {
    if (a.second == b.second) return a.first < b.first;
    return a.second < b.second;
  });

  const auto n = static_cast<std::int64_t>(items.size());
  if (n == 0) return out;
  if (start < 0) start = n + start;
  if (stop < 0) stop = n + stop;
  if (start < 0) start = 0;
  if (stop >= n) stop = n - 1;
  if (start > stop || start >= n) return out;

  for (std::int64_t i = start; i <= stop; ++i) out.push_back(items[static_cast<std::size_t>(i)]);
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
  if (it->second.type != ValueType::ZSet) {
    wrongtype = true;
    return {0, out};
  }

  std::size_t idx = 0, emitted = 0;
  for (const auto& kv : it->second.zset_value) {
    if (idx++ < cursor) continue;
    out.push_back(kv);
    if (++emitted >= count) break;
  }
  const auto next = (cursor + emitted) >= it->second.zset_value.size() ? 0 : (cursor + emitted);
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
  if (it->second.type != ValueType::ZSet) {
    wrongtype = true;
    return out;
  }

  std::vector<std::pair<std::string, double>> items(it->second.zset_value.begin(), it->second.zset_value.end());
  std::sort(items.begin(), items.end(), [](const auto& a, const auto& b) {
    if (a.second == b.second) return a.first < b.first;
    return a.second < b.second;
  });
  if (max) std::reverse(items.begin(), items.end());

  const std::size_t take = std::min(count, items.size());
  out.reserve(take);
  for (std::size_t i = 0; i < take; ++i) {
    out.push_back(items[i]);
    it->second.zset_value.erase(items[i].first);
  }
  if (it->second.zset_value.empty()) db.erase(it);
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
    e.type = ValueType::Stream;
    db[key] = std::move(e);
    it = db.find(key);
  } else if (it->second.type != ValueType::Stream) {
    wrongtype = true;
    return std::nullopt;
  }

  std::uint64_t ms = 0, seq = 0;
  if (id == "*") {
    const auto now = static_cast<std::uint64_t>(now_ms());
    if (now > it->second.stream_last_ms) {
      ms = now;
      seq = 0;
    } else {
      ms = it->second.stream_last_ms;
      seq = it->second.stream_last_seq + 1;
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
    if (ms < it->second.stream_last_ms || (ms == it->second.stream_last_ms && seq <= it->second.stream_last_seq)) {
      err = "The ID specified in XADD is equal or smaller than the target stream top item";
      return std::nullopt;
    }
  }

  it->second.stream_last_ms = ms;
  it->second.stream_last_seq = seq;
  const std::string out_id = std::to_string(ms) + "-" + std::to_string(seq);
  it->second.stream_entries.push_back({out_id, fields});
  return out_id;
}

std::int64_t DataStore::xlen(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type != ValueType::Stream) {
    wrongtype = true;
    return 0;
  }
  return static_cast<std::int64_t>(it->second.stream_entries.size());
}

std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> DataStore::xrange(
    const std::string& key, const std::string& start, const std::string& stop, bool rev, bool& wrongtype) {
  wrongtype = false;
  std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return out;
  if (it->second.type != ValueType::Stream) {
    wrongtype = true;
    return out;
  }

  auto in_range = [&](const std::string& id) {
    bool ge_start = (start == "-" || id >= start);
    bool le_stop = (stop == "+" || id <= stop);
    return ge_start && le_stop;
  };

  if (!rev) {
    for (const auto& e : it->second.stream_entries) if (in_range(e.first)) out.push_back(e);
  } else {
    for (auto rit = it->second.stream_entries.rbegin(); rit != it->second.stream_entries.rend(); ++rit) {
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
    e.type = ValueType::Stream;
    db[key] = std::move(e);
    it = db.find(key);
  }
  if (it->second.type != ValueType::Stream) {
    wrongtype = true;
    return false;
  }
  if (it->second.stream_groups.find(group) != it->second.stream_groups.end()) {
    err = "BUSYGROUP Consumer Group name already exists";
    return false;
  }
  Entry::StreamGroup g;
  g.last_delivered_id = start_id;
  it->second.stream_groups[group] = std::move(g);
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
  if (it->second.type != ValueType::Stream) {
    wrongtype = true;
    return false;
  }
  auto git = it->second.stream_groups.find(group);
  if (git == it->second.stream_groups.end()) {
    err = "NOGROUP No such key or consumer group";
    return false;
  }

  std::string next_id = id;
  if (id == "$") {
    next_id = it->second.stream_entries.empty() ? "0-0" : it->second.stream_entries.back().first;
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
    if (it->second.type != ValueType::Stream) {
      wrongtype = true;
      return {};
    }
    auto git = it->second.stream_groups.find(group);
    if (git == it->second.stream_groups.end()) {
      err = "NOGROUP No such key or consumer group";
      return {};
    }

    std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> rows;
    if (idreq == ">") {
      for (const auto& e : it->second.stream_entries) {
        if (!stream_id_gt(e.first, git->second.last_delivered_id)) continue;
        rows.push_back(e);
        git->second.pending_to_consumer[e.first] = consumer;
        git->second.pending_per_consumer[consumer] += 1;
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
  if (it->second.type != ValueType::Stream) {
    wrongtype = true;
    return 0;
  }

  std::int64_t removed = 0;
  for (const auto& id : ids) {
    auto vit = std::find_if(it->second.stream_entries.begin(), it->second.stream_entries.end(),
                            [&](const auto& e) { return e.first == id; });
    if (vit == it->second.stream_entries.end()) continue;
    it->second.stream_entries.erase(vit);
    ++removed;

    for (auto& [_, grp] : it->second.stream_groups) {
      auto pit = grp.pending_to_consumer.find(id);
      if (pit == grp.pending_to_consumer.end()) continue;
      auto cit = grp.pending_per_consumer.find(pit->second);
      if (cit != grp.pending_per_consumer.end()) {
        if (cit->second > 1) --cit->second;
        else grp.pending_per_consumer.erase(cit);
      }
      grp.pending_to_consumer.erase(pit);
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
  if (it->second.type != ValueType::Stream) {
    wrongtype = true;
    return 0;
  }
  auto git = it->second.stream_groups.find(group);
  if (git == it->second.stream_groups.end()) {
    err = "NOGROUP No such key or consumer group";
    return 0;
  }
  std::int64_t acked = 0;
  for (const auto& id : ids) {
    auto pit = git->second.pending_to_consumer.find(id);
    if (pit == git->second.pending_to_consumer.end()) continue;
    const auto consumer = pit->second;
    git->second.pending_to_consumer.erase(pit);
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
  if (it->second.type != ValueType::Stream) {
    wrongtype = true;
    return {};
  }
  auto git = it->second.stream_groups.find(group);
  if (git == it->second.stream_groups.end()) {
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
      switch (e.type) {
        case ValueType::String:
          out << "S " << dbi << " " << ttl << " " << std::quoted(key) << " " << std::quoted(e.value) << "\n";
          break;
        case ValueType::Hash:
          out << "H " << dbi << " " << ttl << " " << std::quoted(key) << " " << e.hash_value.size();
          for (const auto& [f, v] : e.hash_value) out << " " << std::quoted(f) << " " << std::quoted(v);
          out << "\n";
          break;
        case ValueType::List:
          out << "L " << dbi << " " << ttl << " " << std::quoted(key) << " " << e.list_value.size();
          for (const auto& v : e.list_value) out << " " << std::quoted(v);
          out << "\n";
          break;
        case ValueType::Set:
          out << "T " << dbi << " " << ttl << " " << std::quoted(key) << " " << e.set_value.size();
          for (const auto& m : e.set_value) out << " " << std::quoted(m);
          out << "\n";
          break;
        case ValueType::ZSet:
          out << "Z " << dbi << " " << ttl << " " << std::quoted(key) << " " << e.zset_value.size();
          for (const auto& [m, s] : e.zset_value) out << " " << std::quoted(m) << " " << s;
          out << "\n";
          break;
        case ValueType::Stream:
          out << "X " << dbi << " " << ttl << " " << std::quoted(key) << " " << e.stream_entries.size() << " "
              << e.stream_last_ms << " " << e.stream_last_seq;
          for (const auto& [id, fields] : e.stream_entries) {
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

std::vector<std::vector<std::string>> DataStore::export_aof_commands() const {
  std::vector<std::vector<std::string>> out;
  for (std::size_t dbi = 0; dbi < dbs_.size(); ++dbi) {
    if (dbs_[dbi].empty()) continue;
    out.push_back({"SELECT", std::to_string(dbi)});
    for (const auto& [key, e] : dbs_[dbi]) {
      switch (e.type) {
        case ValueType::String:
          out.push_back({"SET", key, e.value});
          break;
        case ValueType::Hash: {
          std::vector<std::string> cmd{"HSET", key};
          for (const auto& [f, v] : e.hash_value) { cmd.push_back(f); cmd.push_back(v); }
          out.push_back(std::move(cmd));
          break;
        }
        case ValueType::List: {
          std::vector<std::string> cmd{"RPUSH", key};
          for (const auto& v : e.list_value) cmd.push_back(v);
          out.push_back(std::move(cmd));
          break;
        }
        case ValueType::Set: {
          std::vector<std::string> cmd{"SADD", key};
          for (const auto& m : e.set_value) cmd.push_back(m);
          out.push_back(std::move(cmd));
          break;
        }
        case ValueType::ZSet: {
          std::vector<std::string> cmd{"ZADD", key};
          for (const auto& [m, s] : e.zset_value) { cmd.push_back(std::to_string(s)); cmd.push_back(m); }
          out.push_back(std::move(cmd));
          break;
        }
        case ValueType::Stream: {
          for (const auto& [id, fields] : e.stream_entries) {
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
  const auto& e = it->second;
  auto is_int = [](const std::string& s) -> bool {
    if (s.empty()) return false;
    std::size_t i = (s[0] == '-' ? 1 : 0);
    if (i >= s.size()) return false;
    for (; i < s.size(); ++i) if (!std::isdigit(static_cast<unsigned char>(s[i]))) return false;
    try {
      std::size_t consumed = 0;
      (void)std::stoll(s, &consumed);
      return consumed == s.size();
    } catch (...) {
      return false;
    }
  };
  if (e.type == ValueType::String) {
    if (e.string_force_raw) return "raw";
    if (is_int(e.value)) return "int";
    return e.value.size() <= 39 ? "embstr" : "raw";
  }
  if (e.type == ValueType::List) {
    bool small = e.list_value.size() <= 128;
    for (const auto& v : e.list_value) if (v.size() > 64) { small = false; break; }
    return small ? "listpack" : "quicklist";
  }
  if (e.type == ValueType::Set) {
    bool all_int = true;
    for (const auto& v : e.set_value) if (!is_int(v)) { all_int = false; break; }
    if (all_int && e.set_value.size() <= 512) return "intset";
    bool small = e.set_value.size() <= 128;
    for (const auto& v : e.set_value) if (v.size() > 64) { small = false; break; }
    return small ? "listpack" : "hashtable";
  }
  if (e.type == ValueType::Hash) {
    bool small = e.hash_value.size() <= 128;
    for (const auto& [f, v] : e.hash_value) if (f.size() > 64 || v.size() > 64) { small = false; break; }
    return small ? "listpack" : "hashtable";
  }
  if (e.type == ValueType::ZSet) {
    bool small = zset_max_ziplist_entries_ >= 0 &&
                 e.zset_value.size() <= static_cast<std::size_t>(zset_max_ziplist_entries_);
    for (const auto& [m, _] : e.zset_value) if (m.size() > 64) { small = false; break; }
    return small ? "listpack" : "skiplist";
  }
  if (e.type == ValueType::Stream) return "stream";
  return "raw";
}

std::optional<std::string> DataStore::debug_digest_value(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  const auto& e = it->second;
  std::ostringstream oss;
  oss << static_cast<int>(e.type) << "|";
  if (e.expire_at_ms.has_value()) oss << "ttl=" << *e.expire_at_ms << "|";
  else oss << "ttl=-1|";
  if (e.type == ValueType::String) {
    oss << e.value;
  } else if (e.type == ValueType::Hash) {
    std::vector<std::pair<std::string, std::string>> items(e.hash_value.begin(), e.hash_value.end());
    std::sort(items.begin(), items.end());
    for (const auto& [f, v] : items) oss << f << "=" << v << ";";
  } else if (e.type == ValueType::List) {
    for (const auto& v : e.list_value) oss << v << ";";
  } else if (e.type == ValueType::Set) {
    std::vector<std::string> items(e.set_value.begin(), e.set_value.end());
    std::sort(items.begin(), items.end());
    for (const auto& v : items) oss << v << ";";
  } else if (e.type == ValueType::ZSet) {
    std::vector<std::pair<std::string, double>> items(e.zset_value.begin(), e.zset_value.end());
    std::sort(items.begin(), items.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    for (const auto& [m, s] : items) oss << m << "=" << s << ";";
  } else if (e.type == ValueType::Stream) {
    for (const auto& [id, fields] : e.stream_entries) {
      oss << id << "{";
      for (const auto& [f, v] : fields) oss << f << "=" << v << ";";
      oss << "}";
    }
  }
  const auto payload = oss.str();
  const auto h = std::hash<std::string>{}(payload);
  std::ostringstream out;
  out << std::hex << h;
  return out.str();
}

void DataStore::set_zset_max_ziplist_entries(std::int64_t v) {
  zset_max_ziplist_entries_ = v;
}

std::int64_t DataStore::zset_max_ziplist_entries() const {
  return zset_max_ziplist_entries_;
}


void DataStore::active_expire_cycle(std::size_t budget_per_db) {
  const auto now = now_ms();
  for (auto& db : dbs_) {
    std::size_t scanned = 0;
    for (auto it = db.begin(); it != db.end() && scanned < budget_per_db;) {
      ++scanned;
      if (it->second.expire_at_ms.has_value() && *it->second.expire_at_ms <= now) {
        it = db.erase(it);
      } else {
        ++it;
      }
    }
  }
}

void DataStore::flushall() {
  for (auto& db : dbs_) db.clear();
}

void DataStore::flushdb() {
  dbs_[current_db_].clear();
}

std::size_t DataStore::dbsize() {
  return dbs_[current_db_].size();
}

std::pair<std::uint64_t, std::vector<std::string>> DataStore::scan(
    std::uint64_t cursor, std::size_t count, const std::string& match, const std::string& type) {
  auto& db = dbs_[current_db_];
  std::vector<std::string> out;
  std::size_t idx = 0, emitted = 0;
  const auto now = now_ms();
  for (auto it = db.begin(); it != db.end(); ++it) {
    // Skip expired keys
    if (it->second.expire_at_ms.has_value() && *it->second.expire_at_ms <= now) continue;
    if (idx++ < cursor) continue;
    // Match filter
    if (match != "*" && fnmatch(match.c_str(), it->first.c_str(), 0) != 0) continue;
    // Type filter
    if (!type.empty()) {
      std::string entry_type;
      switch (it->second.type) {
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
    if (++emitted >= count) break;
  }
  const auto next = (idx >= db.size()) ? 0ULL : static_cast<std::uint64_t>(idx);
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
