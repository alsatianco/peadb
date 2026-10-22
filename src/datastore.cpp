#include "datastore.hpp"

#include <chrono>
#include <cmath>
#include <cstdio>
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <random>
#include <limits>
#include <set>
#include <fnmatch.h>

namespace peadb {

namespace {
// The loop of Redis's scanGenericCommand() over one Dict: Dict::scan() calls from `cursor` until
// `count` elements have been looked at, count * 10 more buckets have been visited (so a sparse
// table cannot make one call walk it all), or the scan is over. `visit` gets every element of every
// visited bucket, before any MATCH filter. Returns the next cursor (0 at the end).
template <class D, class Fn>
std::uint64_t scan_dict(const D& d, std::uint64_t cursor, std::int64_t count, Fn&& visit) {
  constexpr std::int64_t kMax = std::numeric_limits<std::int64_t>::max();
  // Redis's `long maxiterations = count*10`, without its signed overflow for a huge COUNT.
  std::int64_t maxiterations = count > kMax / 10 ? kMax : count * 10;
  std::int64_t sampled = 0;
  do {
    cursor = d.scan(cursor, [&](const auto& element) {
      ++sampled;
      visit(element);
    });
  } while (cursor != 0 && maxiterations-- != 0 && sampled < count);
  return cursor;
}

// TYPE's name for a value type.
const char* type_name(ValueType t) {
  switch (t) {
    case ValueType::String:
      return "string";
    case ValueType::Hash:
      return "hash";
    case ValueType::List:
      return "list";
    case ValueType::Set:
      return "set";
    case ValueType::ZSet:
      return "zset";
    case ValueType::Stream:
      return "stream";
  }
  return "none";
}

// MATCH for the scans (stringmatchlen() in Redis; PeaDB uses fnmatch(), compat/delta.md item 76).
bool scan_match(const std::optional<std::string>& pattern, const std::string& s) {
  return !pattern || fnmatch(pattern->c_str(), s.c_str(), 0) == 0;
}

// ── Stream IDs are (ms, seq) pairs compared NUMERICALLY (never as strings — "10-0"
//    sorts before "9-0" lexically). These helpers are the single source of truth. ──
struct StreamID {
  std::uint64_t ms = 0;
  std::uint64_t seq = 0;
};
inline int stream_id_cmp(const StreamID& a, const StreamID& b) {
  if (a.ms != b.ms) return a.ms < b.ms ? -1 : 1;
  if (a.seq != b.seq) return a.seq < b.seq ? -1 : 1;
  return 0;
}
inline std::string stream_id_str(const StreamID& id) {
  return std::to_string(id.ms) + "-" + std::to_string(id.seq);
}
// Parse a bare "ms" or "ms-seq". Returns false on any garbage or overflow.
inline bool stream_id_parse_strict(const std::string& s, StreamID& out) {
  if (s.empty()) return false;
  const auto dash = s.find('-');
  auto to_u64 = [](const std::string& t, std::uint64_t& v) -> bool {
    if (t.empty()) return false;
    for (char c : t)
      if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    errno = 0;
    char* end = nullptr;
    v = std::strtoull(t.c_str(), &end, 10);
    return errno != ERANGE && end == t.c_str() + t.size();
  };
  if (dash == std::string::npos) {
    out.seq = 0;
    return to_u64(s, out.ms);
  }
  return to_u64(s.substr(0, dash), out.ms) && to_u64(s.substr(dash + 1), out.seq);
}
// Parse an XRANGE/XREVRANGE bound into an INCLUSIVE StreamID: "-"/"+", optional "("
// exclusive prefix, bare "ms" (seq defaults 0 for a start bound, UINT64_MAX for a stop
// bound), or "ms-seq". An exclusive "(id" is converted to the adjacent inclusive id
// (start: id+1, stop: id-1); overflow past +∞ / underflow below 0-0 is an error, as is
// "(-"/"(+". Returns false on any invalid bound so the caller can reply -ERR.
inline bool stream_id_parse_bound(const std::string& in, bool is_start, StreamID& out) {
  bool exclusive = false;
  std::string s = in;
  if (!s.empty() && s[0] == '(') {
    exclusive = true;
    s = s.substr(1);
  }
  if (!exclusive) {
    if (s == "-") {
      out = {0, 0};
      return true;
    }
    if (s == "+") {
      out = {UINT64_MAX, UINT64_MAX};
      return true;
    }
  } else if (s == "-" || s == "+" || s.empty()) {
    return false;  // exclusive of a special/empty bound is invalid
  }
  StreamID id;
  if (s.find('-') == std::string::npos) {
    std::uint64_t ms = 0;
    if (s.empty()) return false;
    for (char c : s)
      if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    errno = 0;
    char* end = nullptr;
    ms = std::strtoull(s.c_str(), &end, 10);
    if (errno == ERANGE || end != s.c_str() + s.size()) return false;
    id = {ms, is_start ? 0u : UINT64_MAX};
  } else if (!stream_id_parse_strict(s, id)) {
    return false;
  }
  if (exclusive) {
    if (is_start) {  // adjacent-greater
      if (id.seq == UINT64_MAX) {
        if (id.ms == UINT64_MAX) return false;  // past +∞
        id = {id.ms + 1, 0};
      } else {
        id.seq++;
      }
    } else {  // adjacent-smaller
      if (id.seq == 0) {
        if (id.ms == 0) return false;  // below 0-0
        id = {id.ms - 1, UINT64_MAX};
      } else {
        id.seq--;
      }
    }
  }
  out = id;
  return true;
}

// Round-trip-safe double formatting for AOF rewrite (std::to_string gives only ~6
// significant digits, silently corrupting non-round scores on every rewrite).
std::string fmt_double(double d) {
  char buf[64];
  std::snprintf(buf, sizeof(buf), "%.17g", d);
  return buf;
}

constexpr std::int64_t kInvalidEntriesRead = -1;  // Redis SCG_INVALID_ENTRIES_READ

// Does the range [start, +inf] of the stream include the max-deleted tombstone?
// (Mirrors Redis streamRangeHasTombstones — used to decide if entries-read stays valid.)
bool stream_range_has_tombstones(const Entry::StreamT& st, const StreamID& start) {
  StreamID maxdel{0, 0};
  stream_id_parse_strict(st.max_deleted_id.empty() ? "0-0" : st.max_deleted_id, maxdel);
  if (st.entries.empty() || (maxdel.ms == 0 && maxdel.seq == 0)) return false;
  StreamID first{0, 0};
  stream_id_parse_strict(st.entries.front().first, first);
  if (stream_id_cmp(first, maxdel) > 0) return false;  // tombstone is before the first entry
  return stream_id_cmp(start, maxdel) <= 0;            // start <= maxdel <= +inf
}

// Estimate a group's logical read-counter for a given id, or kInvalidEntriesRead when it
// cannot be determined (Redis streamEstimateDistanceFromFirstEverEntry).
std::int64_t stream_estimate_distance(const Entry::StreamT& st, const StreamID& id) {
  const std::int64_t added = static_cast<std::int64_t>(st.entries_added);
  if (added == 0) return 0;
  const std::int64_t length = static_cast<std::int64_t>(st.entries.size());
  const StreamID last{st.last_ms, st.last_seq};
  if (length == 0 && stream_id_cmp(id, last) < 1) return added;
  const int cmp_last = stream_id_cmp(id, last);
  if (cmp_last == 0) return added;
  if (cmp_last > 0) return kInvalidEntriesRead;  // a future id's counter is unknown
  StreamID first{0, 0};
  if (!st.entries.empty()) stream_id_parse_strict(st.entries.front().first, first);
  StreamID maxdel{0, 0};
  stream_id_parse_strict(st.max_deleted_id.empty() ? "0-0" : st.max_deleted_id, maxdel);
  const bool maxdel_zero = (maxdel.ms == 0 && maxdel.seq == 0);
  const int cmp_id_first = stream_id_cmp(id, first);
  if (maxdel_zero || stream_id_cmp(maxdel, first) < 0) {  // no fragmentation ahead
    if (cmp_id_first < 0) return added - length;
    if (cmp_id_first == 0) return added - length + 1;
  }
  return kInvalidEntriesRead;  // fragmented or arbitrary id → can't predict
}

// A consumer group's lag (entries not yet delivered), or nullopt when fragmentation makes
// it indeterminable (Redis streamReplyWithCGLag → null).
std::optional<std::int64_t> stream_cg_lag(const Entry::StreamT& st, const Entry::StreamGroup& g) {
  if (st.entries_added == 0) return 0;  // never-used stream
  StreamID glast{0, 0};
  stream_id_parse_strict(g.last_delivered_id, glast);
  if (g.entries_read != kInvalidEntriesRead && !stream_range_has_tombstones(st, glast))
    return static_cast<std::int64_t>(st.entries_added) - g.entries_read;
  const std::int64_t est = stream_estimate_distance(st, glast);
  if (est != kInvalidEntriesRead) return static_cast<std::int64_t>(st.entries_added) - est;
  return std::nullopt;
}
}  // namespace

DataStore::DataStore() : current_db_(0), dbs_(16), hidden_expired_(16), replica_hidden_(16) {
}

bool DataStore::select_db(int db_index) {
  if (db_index < 0 || db_index >= static_cast<int>(dbs_.size())) return false;
  current_db_ = db_index;
  return true;
}

int DataStore::current_db() const {
  return current_db_;
}

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

void DataStore::remove_expired(DB& db, DB::iterator it) {
  const std::size_t dbi = db_index_of(db);
  if (replica_expiry_) {
    replica_hidden_[dbi].insert(db.extract(it));
    has_replica_hidden_ = true;
    return;
  }
  if (expire_paused_) {
    hidden_expired_[dbi].insert(db.extract(it));
    return;
  }
  record_expired();
  if (!lazy_expire_hook_) {
    db.erase(it);
    return;
  }
  const std::string key = it->first;
  db.erase(it);
  lazy_expire_hook_(static_cast<int>(dbi), key);
}

void DataStore::expire_met_keys(DB& db, const std::vector<std::string>& keys) {
  if (replica_expiry_) return;  // a replica deletes nothing; the walk already skipped them
  for (const auto& key : keys) expire_if_needed(db, key);
}

void DataStore::set_replica_expiry(bool replica) {
  if (replica_expiry_ && !replica) restore_hidden_expired();
  replica_expiry_ = replica;
}

void DataStore::restore_replica_hidden() {
  has_replica_hidden_ = false;
  for (std::size_t i = 0; i < dbs_.size(); ++i) {
    auto& hidden = replica_hidden_[i];
    while (!hidden.empty()) {
      auto node = hidden.extract(hidden.begin());
      if (dbs_[i].find(node.key()) == dbs_[i].end()) dbs_[i].insert(std::move(node));
    }
  }
}

std::optional<ValueType> DataStore::live_type(const std::string& key) const {
  const auto& db = dbs_[current_db_];
  const auto it = db.find(key);
  if (it == db.end() || logically_expired(it->second)) return std::nullopt;
  return it->second.type();
}

void DataStore::set_expire_paused(bool paused) {
  if (paused == expire_paused_) return;
  expire_paused_ = paused;
  if (!paused) restore_paused_hidden();
}

void DataStore::restore_paused_hidden() {
  for (std::size_t i = 0; i < dbs_.size(); ++i) {
    auto& hidden = hidden_expired_[i];
    while (!hidden.empty()) {
      auto node = hidden.extract(hidden.begin());
      // A write during the pause (only possible inside the transaction that sent CLIENT PAUSE)
      // may have made a new key of the same name; it wins, as it would have over the expired one.
      if (dbs_[i].find(node.key()) == dbs_[i].end()) dbs_[i].insert(std::move(node));
    }
  }
}

bool DataStore::expire_if_needed(DB& db, const std::string& key) {
  const auto it = db.find(key);
  if (it == db.end() || !logically_expired(it->second)) return false;
  remove_expired(db, it);
  return true;
}

std::optional<std::string> DataStore::get(const std::string& key) {
  auto& db = dbs_[current_db_];
  auto it = db.find(key);
  if (it == db.end()) {
    record_miss();
    return std::nullopt;
  }
  if (logically_expired(it->second)) {
    remove_expired(db, it);
    record_miss();
    return std::nullopt;
  }
  record_hit();
  // A key of another type reads as no string (MGET, SORT's BY and GET patterns), as in Redis; the
  // non-const str() would turn it into an empty string and lose its data.
  if (it->second.type() != ValueType::String) return std::nullopt;
  return it->second.str();
}

std::optional<std::string> DataStore::get_string(const std::string& key, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  auto it = db.find(key);
  if (it != db.end() && logically_expired(it->second)) {
    remove_expired(db, it);
    it = db.end();
  }
  if (it == db.end()) {
    record_miss();
    return std::nullopt;
  }
  record_hit();
  if (it->second.type() != ValueType::String) {
    wrongtype = true;
    return std::nullopt;
  }
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

bool DataStore::del(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  return db.erase(key) > 0;
}

bool DataStore::del_raw(const std::string& key) {
  return dbs_[current_db_].erase(key) > 0;
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
  return type_name(it->second.type());
}

bool DataStore::is_wrongtype_for_string(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
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
  const auto& db = dbs_[current_db_];
  const auto now = now_ms();
  for (const auto& [key, e] : db) {
    if (logically_expired(e, now)) continue;
    if (fnmatch(pattern.c_str(), key.c_str(), 0) == 0) out.push_back(key);
  }
  return out;
}

void DataStore::for_each_key(const std::function<void(const std::string&)>& fn) {
  for (const auto& [key, e] : dbs_[current_db_]) fn(key);
}

std::optional<std::string> DataStore::randomkey() {
  auto& db = dbs_[current_db_];
  if (db.empty()) return std::nullopt;
  static thread_local std::mt19937 rng{std::random_device{}()};
  const auto now = now_ms();
  const std::size_t num_buckets = db.bucket_count();
  // Pick a random starting bucket and walk forward to the first bucket with a live key —
  // O(1/load_factor) amortised — then, like Redis's dictGetRandomKey(), a random live key of that
  // bucket's chain (not always its head, which would never return the other keys of the chain). The
  // expired keys met are deleted after the walk, as dbRandomKey() deletes each expired key it picks
  // (a replica keeps them, and only remembers the first one).
  const std::size_t start = rng() % num_buckets;
  std::vector<std::string> expired;
  std::optional<std::string> found;
  std::vector<const std::string*> live;
  for (std::size_t i = 0; i < num_buckets && !found; ++i) {
    const std::size_t bucket = (start + i) % num_buckets;
    for (auto it = db.begin(bucket); it != db.end(bucket); ++it) {
      if (!logically_expired(it->second, now)) {
        live.push_back(&it->first);
      } else if (!replica_expiry_ || expired.empty()) {
        expired.push_back(it->first);
      }
    }
    if (!live.empty()) found = *live[rng() % live.size()];
  }
  // Every key of a replica's database has expired: Redis's dbRandomKey() then returns one of them.
  if (!found && replica_expiry_ && !expired.empty()) return expired.front();
  expire_met_keys(db, expired);
  return found;
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
  if (a < 0 || b < 0 || a >= static_cast<int>(dbs_.size()) || b >= static_cast<int>(dbs_.size()))
    return false;
  if (a == b) return true;
  std::swap(dbs_[a], dbs_[b]);
  std::swap(hidden_expired_[a], hidden_expired_[b]);
  std::swap(replica_hidden_[a], replica_hidden_[b]);
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

std::int64_t DataStore::copy_key(const std::string& src, const std::string& dst, int db,
                                 bool replace) {
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

std::int64_t DataStore::incrby(const std::string& key, std::int64_t by, bool& ok,
                               std::string& err) {
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
    if (s.empty() || (!std::isdigit(static_cast<unsigned char>(s.front())) && s.front() != '-') ||
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

std::string DataStore::incrbyfloat(const std::string& key, long double by, bool& ok,
                                   std::string& err) {
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
      s = s.substr(0, pos);  // Remove the trailing dot too
    } else if (pos != std::string::npos) {
      s = s.substr(0, pos + 1);
    }
  }

  // Redis's LD_STR_HUMAN also normalizes values that round to negative zero.
  if (s == "-0") s = "0";

  // Keep the TTL, like Redis's dbReplaceValue(); it is propagated as SET ... KEEPTTL.
  set(key, s, false, false, std::nullopt, /*keep_ttl=*/true);
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

int DataStore::setbit(const std::string& key, std::uint64_t bit_offset, int bit, bool& ok,
                      std::string& err) {
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
  if (bit == 1)
    c = static_cast<unsigned char>(c | mask);
  else
    c = static_cast<unsigned char>(c & static_cast<unsigned char>(~mask));
  s[byte_index] = static_cast<char>(c);
  it->second.string_force_raw = true;
  ok = true;
  return prev;
}

int DataStore::getbit(const std::string& key, std::uint64_t bit_offset, bool& ok,
                      std::string& err) {
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

std::size_t DataStore::setrange(const std::string& key, std::size_t offset,
                                const std::string& value, bool& ok, std::string& err) {
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

std::string DataStore::getrange(const std::string& key, std::int64_t start, std::int64_t stop,
                                bool& ok, std::string& err) {
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

std::int64_t DataStore::hset(const std::string& key,
                             const std::vector<std::pair<std::string, std::string>>& fields,
                             bool& wrongtype) {
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

std::optional<std::string> DataStore::hget(const std::string& key, const std::string& field,
                                           bool& wrongtype) {
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

std::int64_t DataStore::hdel(const std::string& key, const std::vector<std::string>& fields,
                             bool& wrongtype) {
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
  auto& h = it->second.hash();
  for (const auto& f : fields) {
    if (h.erase(f) == 0) continue;
    ++removed;
    h.shrink_if_sparse();  // like Redis's hashTypeDelete(): a table under 10% full shrinks at once
  }
  if (h.empty()) db.erase(it);
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

std::vector<std::pair<std::string, std::string>> DataStore::hgetall(const std::string& key,
                                                                    bool& wrongtype) {
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
    const std::string& key, std::uint64_t cursor, std::int64_t count,
    const std::optional<std::string>& pattern, bool& wrongtype) {
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
  const auto& h = it->second.hash();
  if (scans_whole(it->second)) {
    for (const auto& [f, v] : h)
      if (scan_match(pattern, f)) out.emplace_back(f, v);
    return {0, out};
  }
  const std::uint64_t next = scan_dict(h, cursor, count, [&](const auto& fv) {
    if (scan_match(pattern, fv.first)) out.emplace_back(fv.first, fv.second);
  });
  return {next, out};
}

std::int64_t DataStore::lpush(const std::string& key, const std::vector<std::string>& values,
                              bool& wrongtype) {
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

std::int64_t DataStore::rpush(const std::string& key, const std::vector<std::string>& values,
                              bool& wrongtype) {
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

std::vector<std::string> DataStore::lrange(const std::string& key, std::int64_t start,
                                           std::int64_t stop, bool& wrongtype) {
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

  for (std::int64_t i = start; i <= stop; ++i)
    out.push_back(it->second.list()[static_cast<std::size_t>(i)]);
  return out;
}

DataStore::LsetResult DataStore::lset(const std::string& key, std::int64_t index,
                                      const std::string& value) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return LsetResult::NoKey;
  if (it->second.type() != ValueType::List) return LsetResult::WrongType;
  auto& l = it->second.list();
  const auto n = static_cast<std::int64_t>(l.size());
  if (index < 0) index += n;
  if (index < 0 || index >= n) return LsetResult::OutOfRange;
  l[static_cast<std::size_t>(index)] = value;
  return LsetResult::Ok;
}

std::int64_t DataStore::lrem(const std::string& key, std::int64_t count, const std::string& value,
                             bool& wrongtype, bool& key_removed) {
  wrongtype = false;
  key_removed = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type() != ValueType::List) {
    wrongtype = true;
    return 0;
  }
  auto& l = it->second.list();
  // One compacting pass (a deque erase in the middle is O(N) per call). The limit is unsigned
  // so that count == INT64_MIN does not overflow; like Redis it then removes every match.
  std::uint64_t limit = std::numeric_limits<std::uint64_t>::max();  // count 0: every match
  if (count > 0) limit = static_cast<std::uint64_t>(count);
  if (count < 0) limit = 0 - static_cast<std::uint64_t>(count);
  std::uint64_t removed = 0;
  auto compact = [&](auto first, auto last) {
    auto out = first;
    for (auto in = first; in != last; ++in) {
      if (removed < limit && *in == value) {
        ++removed;
        continue;
      }
      if (out != in) *out = std::move(*in);
      ++out;
    }
    return out;
  };
  if (count >= 0) {
    l.erase(compact(l.begin(), l.end()), l.end());
  } else {
    l.erase(l.begin(), compact(l.rbegin(), l.rend()).base());
  }
  if (l.empty()) {
    db.erase(it);
    key_removed = true;
  }
  return static_cast<std::int64_t>(removed);
}

std::int64_t DataStore::linsert(const std::string& key, bool before, const std::string& pivot,
                                const std::string& value, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type() != ValueType::List) {
    wrongtype = true;
    return 0;
  }
  auto& l = it->second.list();
  auto pos = std::find(l.begin(), l.end(), pivot);
  if (pos == l.end()) return -1;
  l.insert(before ? pos : std::next(pos), value);
  return static_cast<std::int64_t>(l.size());
}

bool DataStore::ltrim(const std::string& key, std::int64_t start, std::int64_t stop,
                      bool& wrongtype, bool& key_removed) {
  wrongtype = false;
  key_removed = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  if (it->second.type() != ValueType::List) {
    wrongtype = true;
    return false;
  }
  auto& l = it->second.list();
  // Index rules of Redis's ltrimCommand().
  const auto n = static_cast<std::int64_t>(l.size());
  if (start < 0) start += n;
  if (stop < 0) stop += n;
  if (start < 0) start = 0;
  if (start > stop || start >= n) {
    l.clear();
  } else {
    if (stop >= n) stop = n - 1;
    l.erase(l.begin() + static_cast<std::ptrdiff_t>(stop + 1), l.end());
    l.erase(l.begin(), l.begin() + static_cast<std::ptrdiff_t>(start));
  }
  if (l.empty()) {
    db.erase(it);
    key_removed = true;
  }
  return true;
}

std::int64_t DataStore::sadd(const std::string& key, const std::vector<std::string>& members,
                             bool& wrongtype) {
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
      if (!is_int || e.set().size() > static_cast<std::size_t>(
                                          std::max<std::int64_t>(0, encoding_limits_.set_intset))) {
        e.encoding_no_intset = true;
      }
    }
  }
  return added;
}

std::int64_t DataStore::srem(const std::string& key, const std::vector<std::string>& members,
                             bool& wrongtype) {
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
  auto& set = it->second.set();
  for (const auto& m : members) {
    if (set.erase(m) == 0) continue;
    ++removed;
    set.shrink_if_sparse();  // like Redis's setTypeRemoveAux()
  }
  if (set.empty()) db.erase(it);
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
  it->second.set().shrink_if_sparse();
  if (it->second.set().empty()) db.erase(it);
  return value;
}

std::pair<std::uint64_t, std::vector<std::string>> DataStore::sscan(
    const std::string& key, std::uint64_t cursor, std::int64_t count,
    const std::optional<std::string>& pattern, bool& wrongtype) {
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
  const auto& set = it->second.set();
  if (scans_whole(it->second)) {
    for (const auto& m : set)
      if (scan_match(pattern, m)) out.push_back(m);
    // An intset keeps its members in ascending order, so Redis returns them that way.
    if (std::string_view(encoding_of(it->second)) == "intset") {
      std::sort(out.begin(), out.end(), [](const std::string& a, const std::string& b) {
        return std::strtoll(a.c_str(), nullptr, 10) < std::strtoll(b.c_str(), nullptr, 10);
      });
    }
    return {0, out};
  }
  const std::uint64_t next = scan_dict(set, cursor, count, [&](const std::string& m) {
    if (scan_match(pattern, m)) out.push_back(m);
  });
  return {next, out};
}

DataStore::ZAddResult DataStore::zadd_one(const std::string& key, double score,
                                          const std::string& member, bool nx, bool xx, bool gt,
                                          bool lt, bool incr) {
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
    entry.zord().erase({mit->second, member});  // remove old position
    mit->second = new_score;
    entry.zord()[{new_score, member}] = true;  // insert at new position
    r.changed = true;
  }
  r.score = mit->second;
  return r;
}

std::int64_t DataStore::zrem(const std::string& key, const std::vector<std::string>& members,
                             bool& wrongtype, bool& key_removed) {
  wrongtype = false;
  key_removed = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type() != ValueType::ZSet) {
    wrongtype = true;
    return 0;
  }
  auto& entry = it->second;
  std::int64_t removed = 0;
  for (const auto& m : members) {
    auto mit = entry.zset().find(m);
    if (mit == entry.zset().end()) continue;
    entry.zord().erase({mit->second, m});
    entry.zset().erase(mit);
    entry.zset().shrink_if_sparse();  // like Redis's zsetDel() and zremrangeGenericCommand()
    ++removed;
  }
  if (entry.zset().empty()) {
    db.erase(it);
    key_removed = true;
  }
  return removed;
}

std::vector<std::pair<std::string, double>> DataStore::zrange(const std::string& key,
                                                              std::int64_t start, std::int64_t stop,
                                                              bool withscores, bool& wrongtype) {
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
    out.push_back({oit->first.second, oit->first.first});  // {member, score}
  (void)withscores;
  return out;
}

std::pair<std::uint64_t, std::vector<std::pair<std::string, double>>> DataStore::zscan(
    const std::string& key, std::uint64_t cursor, std::int64_t count,
    const std::optional<std::string>& pattern, bool& wrongtype) {
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
  const auto& z = it->second.zset();
  if (scans_whole(it->second)) {
    // A listpack keeps the members in score order, so Redis returns them that way.
    for (const auto& [score_member, _] : it->second.zord())
      if (scan_match(pattern, score_member.second))
        out.emplace_back(score_member.second, score_member.first);
    return {0, out};
  }
  const std::uint64_t next = scan_dict(z, cursor, count, [&](const auto& ms) {
    if (scan_match(pattern, ms.first)) out.emplace_back(ms.first, ms.second);
  });
  return {next, out};
}

std::vector<std::pair<std::string, double>> DataStore::zpop(const std::string& key,
                                                            std::size_t count, bool max,
                                                            bool& wrongtype) {
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
    entry.zset().shrink_if_sparse();
    ordered.erase(oit);
  }
  if (entry.zset().empty()) db.erase(it);
  return out;
}

std::optional<std::string> DataStore::xadd(
    const std::string& key, const std::string& id,
    const std::vector<std::pair<std::string, std::string>>& fields, bool& wrongtype,
    std::string& err) {
  wrongtype = false;
  err.clear();
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  const bool key_exists = it != db.end();
  if (key_exists && it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return std::nullopt;
  }

  // Compute the ID against the current last-id BEFORE creating anything, so an invalid
  // ID never leaves an empty stream behind (Redis: XADD with a bad ID doesn't create).
  const StreamID last = key_exists
                            ? StreamID{it->second.stream_last_ms(), it->second.stream_last_seq()}
                            : StreamID{0, 0};
  // last_id persists after XDEL / on an emptied stream, so new IDs must still exceed it:
  // gate the monotonicity checks on "has this stream ever assigned an id" (last != 0-0),
  // not on the current entry count.
  const bool have_last = key_exists && (last.ms != 0 || last.seq != 0);
  // Redis rejects any XADD once the stream's last id is the maximal possible value,
  // regardless of the requested id form (checked before append).
  if (have_last && last.ms == UINT64_MAX && last.seq == UINT64_MAX) {
    err = "The stream has exhausted the last possible ID, unable to add more items";
    return std::nullopt;
  }
  StreamID nid;
  if (id == "*") {
    // Full auto: max(now, last_ms). Bump seq on the same ms; if seq is exhausted the
    // stream is full (last.ms already UINT64_MAX) — error rather than wrap.
    const auto now = static_cast<std::uint64_t>(now_ms());
    if (have_last && now <= last.ms) {
      // Same/older ms than last: advance the seq, rolling to (ms+1, 0) on seq overflow.
      // The (MAX,MAX) case is already rejected by the top-level exhausted guard above.
      if (last.seq == UINT64_MAX) {
        nid = {last.ms + 1, 0};
      } else {
        nid = {last.ms, last.seq + 1};
      }
    } else {
      nid = {now, 0};
    }
  } else {
    // "ms-*" (explicit ms, auto seq) or "ms" / "ms-seq".
    const auto star = id.rfind("-*");
    if (star != std::string::npos && star == id.size() - 2) {
      std::uint64_t ms = 0;
      const std::string mspart = id.substr(0, star);
      if (mspart.empty()) {
        err = "Invalid stream ID specified as stream command argument";
        return std::nullopt;
      }
      for (char c : mspart)
        if (!std::isdigit(static_cast<unsigned char>(c))) {
          err = "Invalid stream ID specified as stream command argument";
          return std::nullopt;
        }
      errno = 0;
      char* e2 = nullptr;
      ms = std::strtoull(mspart.c_str(), &e2, 10);
      if (errno == ERANGE || e2 != mspart.c_str() + mspart.size()) {
        err = "Invalid stream ID specified as stream command argument";
        return std::nullopt;
      }
      if (have_last && ms < last.ms) {
        err = "The ID specified in XADD is equal or smaller than the target stream top item";
        return std::nullopt;
      }
      if (have_last && ms == last.ms) {
        if (last.seq == UINT64_MAX) {
          // ms-* with the same ms and seq already at max: cannot advance the seq, and
          // Redis does NOT roll over to ms+1 for the partial form — it's "equal or smaller".
          err = "The ID specified in XADD is equal or smaller than the target stream top item";
          return std::nullopt;
        }
        nid = {ms, last.seq + 1};
      } else {
        // New ms (or empty stream): seq starts at 0, except 0-* on an empty stream is
        // 0-1 since 0-0 is reserved.
        nid = {ms, ms == 0 ? 1u : 0u};
      }
    } else {
      if (!stream_id_parse_strict(id, nid)) {
        err = "Invalid stream ID specified as stream command argument";
        return std::nullopt;
      }
      // 0-0 is never a valid explicit XADD id.
      if (nid.ms == 0 && nid.seq == 0) {
        err = "The ID specified in XADD must be greater than 0-0";
        return std::nullopt;
      }
      if (have_last && stream_id_cmp(nid, last) <= 0) {
        err = "The ID specified in XADD is equal or smaller than the target stream top item";
        return std::nullopt;
      }
    }
  }

  // ID is valid — now create the stream if the key was absent (deferred so a bad ID
  // never leaves an empty stream behind).
  if (!key_exists) {
    Entry e;
    e.stream();
    db[key] = std::move(e);
    it = db.find(key);
  }
  it->second.stream_last_ms() = nid.ms;
  it->second.stream_last_seq() = nid.seq;
  it->second.stream().entries_added++;
  const std::string out_id = stream_id_str(nid);
  it->second.stream_entries().push_back({out_id, fields});
  return out_id;
}

bool DataStore::stream_info(const std::string& key, StreamInfo& out, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return false;
  }
  const auto& st = it->second.stream();
  out.length = static_cast<std::int64_t>(st.entries.size());
  out.last_id = stream_id_str({st.last_ms, st.last_seq});
  out.max_deleted_id = st.max_deleted_id;
  out.entries_added = st.entries_added;
  out.groups = st.groups.size();
  if (!st.entries.empty()) {
    out.first = st.entries.front();
    out.last = st.entries.back();
  }
  return true;
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

std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>>
DataStore::xrange(const std::string& key, const std::string& start, const std::string& stop,
                  bool rev, bool& wrongtype, bool* invalid_id) {
  wrongtype = false;
  if (invalid_id) *invalid_id = false;
  std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return out;
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return out;
  }

  // For XREVRANGE the caller passes bounds as (start=max, stop=min); parse each in its
  // own role so a bare "ms" defaults seq correctly (0 for the low bound, MAX for high).
  const std::string& lo_in = rev ? stop : start;
  const std::string& hi_in = rev ? start : stop;
  StreamID lo, hi;
  if (!stream_id_parse_bound(lo_in, /*is_start=*/true, lo) ||
      !stream_id_parse_bound(hi_in, /*is_start=*/false, hi)) {
    if (invalid_id) *invalid_id = true;
    return out;
  }
  auto in_range = [&](const std::string& id_str) {
    StreamID id;
    if (!stream_id_parse_strict(id_str, id)) return false;
    return stream_id_cmp(id, lo) >= 0 && stream_id_cmp(id, hi) <= 0;  // bounds already inclusive
  };

  if (!rev) {
    for (const auto& e : it->second.stream_entries())
      if (in_range(e.first)) out.push_back(e);
  } else {
    for (auto rit = it->second.stream_entries().rbegin(); rit != it->second.stream_entries().rend();
         ++rit) {
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
}  // namespace

bool DataStore::xgroup_create(const std::string& key, const std::string& group,
                              const std::string& start_id, bool mkstream, bool& wrongtype,
                              std::string& err, const std::int64_t* entries_read) {
  wrongtype = false;
  err.clear();
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    if (!mkstream) {
      err =
          "The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to "
          "use the MKSTREAM option to create an empty stream automatically.";
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
  auto& st = it->second.stream();
  const StreamID last{st.last_ms, st.last_seq};
  // "$" means "start after the current last entry"; otherwise validate the explicit id.
  StreamID startk;
  if (start_id == "$") {
    startk = last;
  } else if (!stream_id_parse_strict(start_id, startk)) {
    err = "Invalid stream ID specified as stream command argument";
    return false;
  }
  Entry::StreamGroup g;
  g.last_delivered_id = stream_id_str(startk);
  // Redis does NOT auto-derive entries-read on CREATE: it uses the explicit ENTRIESREAD or
  // leaves the counter invalid (-1 → reported as nil), later recovered lazily on read.
  g.entries_read = entries_read ? *entries_read : kInvalidEntriesRead;
  it->second.stream_groups()[group] = std::move(g);
  return true;
}

bool DataStore::xgroup_setid(const std::string& key, const std::string& group,
                             const std::string& id, bool& wrongtype, std::string& err,
                             const std::int64_t* entries_read) {
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
  auto& st = it->second.stream();
  // SETID accepts "$" (current last-id, persists past XDEL), "-"/"+" (min/max), or an id.
  StreamID nid;
  if (id == "$") {
    nid = {st.last_ms, st.last_seq};
  } else if (id == "-") {
    nid = {0, 0};
  } else if (id == "+") {
    nid = {UINT64_MAX, UINT64_MAX};
  } else if (!stream_id_parse_strict(id, nid)) {
    err = "Invalid stream ID specified as stream command argument";
    return false;
  }
  git->second.last_delivered_id = stream_id_str(nid);
  if (entries_read) git->second.entries_read = *entries_read;
  return true;
}

std::vector<std::pair<
    std::string,
    std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>>>>
DataStore::xreadgroup(const std::string& group, const std::string& consumer,
                      const std::vector<std::pair<std::string, std::string>>& streams,
                      std::size_t count, bool noack, bool& wrongtype, std::string& err,
                      bool* consumer_created, std::vector<StreamClaimEffect>* effects,
                      std::vector<std::string>* created_keys,
                      std::vector<StreamGroupEffect>* group_effects) {
  wrongtype = false;
  err.clear();
  if (consumer_created) *consumer_created = false;
  std::vector<std::pair<
      std::string,
      std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>>>>
      out;
  auto& db = dbs_[current_db_];

  // Redis validates every stream/group/ID before serving any of them.
  for (const auto& [key, idreq] : streams) {
    expire_if_needed(db, key);
    auto it = db.find(key);
    if (it != db.end() && it->second.type() != ValueType::Stream) {
      wrongtype = true;
      return {};
    }
    if (it == db.end() ||
        it->second.stream_groups().find(group) == it->second.stream_groups().end()) {
      err = "NOGROUP No such key '" + key + "' or consumer group '" + group +
            "' in XREADGROUP with GROUP option";
      return {};
    }
    StreamID id;
    if (idreq == "$") {
      err =
          "The $ ID is meaningless in the context of XREADGROUP: you want to read the history of "
          "this consumer by specifying a proper ID, or use the > ID to get new messages. The $ ID "
          "would just return an empty result set.";
      return {};
    }
    if (idreq != ">" && !stream_id_parse_strict(idreq, id)) {
      err = "Invalid stream ID specified as stream command argument";
      return {};
    }
  }
  // No validated key can expire between validation and the last stream's mutation.
  const LazyExpireDisabled no_lazy_expire(*this);
  const std::int64_t now = now_ms();

  for (const auto& [key, idreq] : streams) {
    expire_if_needed(db, key);
    auto it = db.find(key);
    if (it == db.end()) {
      // Redis reports NOGROUP for a missing key too (the group can't exist).
      err = "NOGROUP No such key '" + key + "' or consumer group '" + group +
            "' in XREADGROUP with GROUP option";
      return {};
    }
    if (it->second.type() != ValueType::Stream) {
      wrongtype = true;
      return {};
    }
    auto git = it->second.stream_groups().find(group);
    if (git == it->second.stream_groups().end()) {
      err = "NOGROUP No such key '" + key + "' or consumer group '" + group +
            "' in XREADGROUP with GROUP option";
      return {};
    }
    auto& g = git->second;
    // The consumer is created (and its seen-time bumped) whenever an XREADGROUP is processed
    // for the group — even a "> " read that finds no data and is about to block — matching
    // Redis. active-time is only bumped when data is actually delivered.
    {
      const bool is_new = g.consumers.find(consumer) == g.consumers.end();
      g.consumers[consumer].seen_time = now;
      if (consumer_created && is_new) *consumer_created = true;
      if (created_keys && is_new) created_keys->push_back(key);
    }
    auto mark_active = [&]() {
      g.consumers[consumer].active_time = now;
    };

    std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> rows;
    if (idreq == ">") {
      // Deliver new entries (id > group last-delivered-id), up to count.
      for (const auto& e : it->second.stream_entries()) {
        if (!stream_id_gt(e.first, g.last_delivered_id)) continue;
        rows.push_back(e);
        if (!noack) {
          auto old = g.pending_to_consumer.find(e.first);
          if (old != g.pending_to_consumer.end()) {
            auto cit = g.pending_per_consumer.find(old->second);
            if (cit != g.pending_per_consumer.end() && cit->second > 0 && --cit->second == 0)
              g.pending_per_consumer.erase(cit);
          }
          g.pending_to_consumer[e.first] = consumer;
          g.pending_per_consumer[consumer] += 1;
          g.pending_delivery_ms[e.first] = now;
          g.pending_delivery_count[e.first] = 1;
        }
        // Advance the read counter: increment when it's valid and no tombstone lies ahead,
        // otherwise recover it via the estimate (Redis streamReplyWithRange group logic).
        StreamID eid;
        stream_id_parse_strict(e.first, eid);
        auto& stt = it->second.stream();
        if (g.entries_read != -1 && !stream_range_has_tombstones(stt, eid)) {
          ++g.entries_read;
        } else if (stt.entries_added != 0) {
          g.entries_read = stream_estimate_distance(stt, eid);
        }
        g.last_delivered_id = e.first;
        if (!noack && effects)
          effects->push_back({key, group, consumer, e.first, g.last_delivered_id,
                              g.pending_delivery_ms[e.first], g.pending_delivery_count[e.first]});
        if (count && rows.size() >= count) break;
      }
      if (!rows.empty()) {
        mark_active();
        if (noack && group_effects)
          group_effects->push_back({key, group, g.last_delivered_id, g.entries_read});
        out.push_back({key, std::move(rows)});
      }
    } else {
      // History read: re-emit this consumer's own pending entries with id STRICTLY GREATER
      // than the given id (Redis increments the start id, treating it as already-delivered).
      StreamID start{0, 0};
      stream_id_parse_strict(idreq, start);  // "0" -> 0-0; bare ms gets seq 0
      std::vector<std::string> pel_ids;
      for (const auto& [pid, owner] : g.pending_to_consumer) {
        if (owner != consumer) continue;
        StreamID pidk;
        if (stream_id_parse_strict(pid, pidk) && stream_id_cmp(pidk, start) > 0)
          pel_ids.push_back(pid);
      }
      std::sort(pel_ids.begin(), pel_ids.end(), [](const std::string& a, const std::string& b) {
        StreamID ia, ib;
        stream_id_parse_strict(a, ia);
        stream_id_parse_strict(b, ib);
        return stream_id_cmp(ia, ib) < 0;
      });
      const auto& entries = it->second.stream_entries();
      for (const auto& pid : pel_ids) {
        auto eit = std::find_if(entries.begin(), entries.end(),
                                [&](const auto& e) { return e.first == pid; });
        if (eit != entries.end()) {
          rows.push_back(*eit);
          g.pending_delivery_ms[pid] = now;
          g.pending_delivery_count[pid] = static_cast<std::int64_t>(
              static_cast<std::uint64_t>(g.pending_delivery_count[pid]) + 1);
        } else {
          // Pending id whose stream entry was removed: emit id + empty fields (null sentinel).
          rows.push_back({pid, {}});
        }
        if (count && rows.size() >= count) break;
      }
      if (!rows.empty()) mark_active();
      // A specific-id read always reports the key, even with an empty result set.
      out.push_back({key, std::move(rows)});
    }
  }
  return out;
}

bool DataStore::stream_group_ids(const std::string& key, const std::string& group,
                                 std::string& last_id, std::int64_t& entries_read) {
  auto& db = dbs_[current_db_];
  auto it = db.find(key);
  if (it == db.end() || it->second.type() != ValueType::Stream) return false;
  auto git = it->second.stream_groups().find(group);
  if (git == it->second.stream_groups().end()) return false;
  last_id = git->second.last_delivered_id;
  entries_read = git->second.entries_read;
  return true;
}

std::int64_t DataStore::xdel(const std::string& key, const std::vector<std::string>& ids,
                             bool& wrongtype, bool is_trim) {
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
    // Normalize the requested id ("666" == "666-0") and compare numerically so a bare
    // ms matches the stored "ms-seq" entry.
    StreamID want;
    if (!stream_id_parse_strict(id, want)) continue;
    auto vit = std::find_if(
        it->second.stream_entries().begin(), it->second.stream_entries().end(), [&](const auto& e) {
          StreamID eid;
          return stream_id_parse_strict(e.first, eid) && stream_id_cmp(eid, want) == 0;
        });
    if (vit == it->second.stream_entries().end()) continue;
    const std::string canon_id = vit->first;
    // Track the highest deleted id (XINFO max-deleted-entry-id) — only for explicit XDEL;
    // trimming (MAXLEN/MINID) removes entries without advancing this tombstone marker.
    if (!is_trim && stream_id_gt(canon_id, it->second.stream().max_deleted_id)) {
      it->second.stream().max_deleted_id = canon_id;
    }
    it->second.stream_entries().erase(vit);
    ++removed;
    // NOTE: consumer-group PEL entries are intentionally NOT removed here. In Redis, XDEL
    // (and trimming) leave a dangling NACK — a deleted-but-still-pending message stays in
    // XPENDING and is re-read by XREADGROUP history as "<id> nil" until it is XACK'd
    // (bug #5570). Only XACK removes PEL entries.
  }
  return removed;
}

std::int64_t DataStore::xack(const std::string& key, const std::string& group,
                             const std::vector<std::string>& ids, bool& wrongtype,
                             std::string& err) {
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
    // Canonicalize "667" -> "667-0" so a bare-ms ack matches the stored PEL key.
    StreamID want;
    if (!stream_id_parse_strict(id, want)) continue;
    const std::string canon = stream_id_str(want);
    auto pit = git->second.pending_to_consumer.find(canon);
    if (pit == git->second.pending_to_consumer.end()) continue;
    const auto consumer = pit->second;
    git->second.pending_to_consumer.erase(pit);
    git->second.pending_delivery_ms.erase(canon);
    git->second.pending_delivery_count.erase(canon);
    auto cit = git->second.pending_per_consumer.find(consumer);
    if (cit != git->second.pending_per_consumer.end() && cit->second > 0) {
      cit->second -= 1;
      if (cit->second == 0) git->second.pending_per_consumer.erase(cit);
    }
    ++acked;
  }
  return acked;
}

std::vector<std::string> DataStore::xpending_summary(
    const std::string& key, const std::string& group,
    std::vector<std::pair<std::string, std::int64_t>>& consumers, bool& wrongtype,
    std::string& err) {
  wrongtype = false;
  err.clear();
  consumers.clear();
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'";
    return {};
  }
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return {};
  }
  auto git = it->second.stream_groups().find(group);
  if (git == it->second.stream_groups().end()) {
    err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'";
    return {};
  }
  const auto& g = git->second;
  if (g.pending_to_consumer.empty()) return {"0", "", ""};
  std::string min_id, max_id;
  bool first = true;
  std::map<std::string, std::int64_t> per_consumer;  // ordered for stable output
  for (const auto& kv : g.pending_to_consumer) {
    if (first) {
      min_id = max_id = kv.first;
      first = false;
    } else {
      if (stream_id_gt(min_id, kv.first)) min_id = kv.first;
      if (stream_id_gt(kv.first, max_id)) max_id = kv.first;
    }
    per_consumer[kv.second] += 1;
  }
  for (const auto& [c, n] : per_consumer) consumers.push_back({c, n});
  return {std::to_string(g.pending_to_consumer.size()), min_id, max_id};
}

std::vector<DataStore::PendingItem> DataStore::xpending_extended(
    const std::string& key, const std::string& group, const std::string& start,
    const std::string& end, std::int64_t count, const std::string* consumer,
    std::int64_t min_idle_ms, bool& wrongtype, std::string& err) {
  wrongtype = false;
  err.clear();
  std::vector<PendingItem> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'";
    return out;
  }
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return out;
  }
  auto git = it->second.stream_groups().find(group);
  if (git == it->second.stream_groups().end()) {
    err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'";
    return out;
  }
  const auto& g = git->second;
  StreamID lo, hi;
  if (!stream_id_parse_bound(start, /*is_start=*/true, lo)) {
    err = "Invalid stream ID specified as stream command argument";
    return out;
  }
  if (!stream_id_parse_bound(end, /*is_start=*/false, hi)) {
    err = "Invalid stream ID specified as stream command argument";
    return out;
  }
  const auto now = now_ms();
  for (const auto& [pid, owner] : g.pending_to_consumer) {
    if (consumer && owner != *consumer) continue;
    StreamID pk;
    if (!stream_id_parse_strict(pid, pk)) continue;
    if (stream_id_cmp(pk, lo) < 0 || stream_id_cmp(pk, hi) > 0) continue;
    auto dm = g.pending_delivery_ms.find(pid);
    const std::int64_t dms = dm != g.pending_delivery_ms.end() ? dm->second : now;
    const std::int64_t idle = now - dms;
    if (idle < min_idle_ms) continue;
    auto dc = g.pending_delivery_count.find(pid);
    out.push_back({pid, owner, idle, dc != g.pending_delivery_count.end() ? dc->second : 1});
  }
  std::sort(out.begin(), out.end(), [](const PendingItem& a, const PendingItem& b) {
    StreamID ia, ib;
    stream_id_parse_strict(a.id, ia);
    stream_id_parse_strict(b.id, ib);
    return stream_id_cmp(ia, ib) < 0;
  });
  if (count >= 0 && static_cast<std::int64_t>(out.size()) > count)
    out.resize(static_cast<std::size_t>(count));
  return out;
}

int DataStore::xgroup_destroy(const std::string& key, const std::string& group, bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return 0;
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return 0;
  }
  return it->second.stream_groups().erase(group) ? 1 : 0;
}

int DataStore::xgroup_createconsumer(const std::string& key, const std::string& group,
                                     const std::string& consumer, bool& wrongtype,
                                     std::string& err) {
  wrongtype = false;
  err.clear();
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'";
    return 0;
  }
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return 0;
  }
  auto git = it->second.stream_groups().find(group);
  if (git == it->second.stream_groups().end()) {
    err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'";
    return 0;
  }
  if (git->second.consumers.count(consumer)) return 0;  // already exists
  auto& c = git->second.consumers[consumer];
  c.seen_time = now_ms();
  c.active_time = -1;
  return 1;
}

std::int64_t DataStore::xgroup_delconsumer(const std::string& key, const std::string& group,
                                           const std::string& consumer, bool& wrongtype,
                                           std::string& err, bool* existed) {
  wrongtype = false;
  err.clear();
  if (existed) *existed = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'";
    return 0;
  }
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return 0;
  }
  auto git = it->second.stream_groups().find(group);
  if (git == it->second.stream_groups().end()) {
    err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'";
    return 0;
  }
  auto& g = git->second;
  if (existed) *existed = g.consumers.count(consumer) > 0;
  std::int64_t removed = 0;
  for (auto pit = g.pending_to_consumer.begin(); pit != g.pending_to_consumer.end();) {
    if (pit->second == consumer) {
      g.pending_delivery_ms.erase(pit->first);
      g.pending_delivery_count.erase(pit->first);
      pit = g.pending_to_consumer.erase(pit);
      ++removed;
    } else {
      ++pit;
    }
  }
  g.pending_per_consumer.erase(consumer);
  g.consumers.erase(consumer);
  return removed;  // number of pending entries owned by the deleted consumer
}

std::vector<DataStore::GroupInfo> DataStore::xinfo_groups(const std::string& key, bool& wrongtype,
                                                          std::string& err) {
  wrongtype = false;
  err.clear();
  std::vector<GroupInfo> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    err = "no such key";
    return out;
  }
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return out;
  }
  const auto& st = it->second.stream();
  std::vector<std::string> gnames;
  for (const auto& [name, _] : st.groups) gnames.push_back(name);
  std::sort(gnames.begin(), gnames.end());  // Redis iterates groups name-ordered (rax)
  for (const auto& name : gnames) {
    const auto& g = st.groups.at(name);
    GroupInfo gi;
    gi.name = name;
    gi.last_delivered_id = g.last_delivered_id;
    gi.entries_read = g.entries_read;
    const auto lag = stream_cg_lag(st, g);
    gi.lag_valid = lag.has_value();
    gi.lag = lag.value_or(0);
    gi.pending = g.pending_to_consumer.size();
    // Consumer count spans both explicitly-created consumers and any that only own PEL entries.
    std::set<std::string> names;
    for (const auto& [n, _] : g.consumers) names.insert(n);
    for (const auto& [_, owner] : g.pending_to_consumer) names.insert(owner);
    gi.consumers = names.size();
    out.push_back(std::move(gi));
  }
  return out;
}

std::vector<DataStore::ConsumerInfo> DataStore::xinfo_consumers(const std::string& key,
                                                                const std::string& group,
                                                                bool& wrongtype, std::string& err) {
  wrongtype = false;
  err.clear();
  std::vector<ConsumerInfo> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    err = "no such key";
    return out;
  }
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return out;
  }
  auto git = it->second.stream_groups().find(group);
  if (git == it->second.stream_groups().end()) {
    err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'";
    return out;
  }
  const auto& g = git->second;
  // Union of registered consumers and PEL owners, name-ordered (Redis iterates a sorted rax).
  std::map<std::string, Entry::StreamConsumer> all(g.consumers.begin(), g.consumers.end());
  std::map<std::string, std::size_t> pend;
  for (const auto& [_, owner] : g.pending_to_consumer) {
    pend[owner]++;
    all.emplace(owner, Entry::StreamConsumer{});
  }
  const auto now = now_ms();
  for (const auto& [name, c] : all) {
    ConsumerInfo ci;
    ci.name = name;
    ci.pending = pend.count(name) ? pend[name] : 0;
    ci.idle = now - c.seen_time;
    ci.inactive = c.active_time < 0 ? -1 : now - c.active_time;
    out.push_back(std::move(ci));
  }
  return out;
}

bool DataStore::xinfo_stream_full(const std::string& key, std::size_t count, StreamFullInfo& out,
                                  bool& wrongtype) {
  wrongtype = false;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return false;
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return false;
  }
  const auto& st = it->second.stream();
  out.length = static_cast<std::int64_t>(st.entries.size());
  out.last_id = stream_id_str({st.last_ms, st.last_seq});
  out.max_deleted_id = st.max_deleted_id.empty() ? "0-0" : st.max_deleted_id;
  out.recorded_first_id = st.entries.empty() ? "0-0" : st.entries.front().first;
  out.entries_added = static_cast<std::int64_t>(st.entries_added);
  out.entries.clear();
  const std::size_t lim = count == 0 ? st.entries.size() : count;
  for (std::size_t i = 0; i < st.entries.size() && i < lim; ++i)
    out.entries.push_back(st.entries[i]);
  out.groups.clear();
  std::vector<std::string> gnames;
  for (const auto& [gname, _] : st.groups) gnames.push_back(gname);
  std::sort(gnames.begin(), gnames.end());  // name-ordered groups (Redis rax order)
  for (const auto& gname : gnames) {
    const auto& g = st.groups.at(gname);
    StreamFullGroup fg;
    fg.name = gname;
    fg.last_delivered_id = g.last_delivered_id;
    fg.entries_read = g.entries_read;
    const auto lag = stream_cg_lag(st, g);
    fg.lag_valid = lag.has_value();
    fg.lag = lag.value_or(0);
    fg.pel_count = static_cast<std::int64_t>(g.pending_to_consumer.size());
    // Group-level PEL, id-ordered.
    std::vector<std::string> ids;
    for (const auto& [pid, _] : g.pending_to_consumer) ids.push_back(pid);
    std::sort(ids.begin(), ids.end(), [](const std::string& a, const std::string& b) {
      StreamID ia, ib;
      stream_id_parse_strict(a, ia);
      stream_id_parse_strict(b, ib);
      return stream_id_cmp(ia, ib) < 0;
    });
    auto dms = [&](const std::string& id) {
      auto i = g.pending_delivery_ms.find(id);
      return i != g.pending_delivery_ms.end() ? i->second : 0;
    };
    auto dcnt = [&](const std::string& id) {
      auto i = g.pending_delivery_count.find(id);
      return i != g.pending_delivery_count.end() ? i->second : 1;
    };
    for (const auto& id : ids) {
      auto owner = g.pending_to_consumer.at(id);
      fg.pending.push_back({id, owner, dms(id), dcnt(id)});
    }
    // Consumers (name-ordered), each with its own id-ordered PEL.
    std::map<std::string, Entry::StreamConsumer> all(g.consumers.begin(), g.consumers.end());
    for (const auto& [_, owner] : g.pending_to_consumer)
      all.emplace(owner, Entry::StreamConsumer{});
    const auto now = now_ms();
    for (const auto& [cname, c] : all) {
      StreamFullConsumer fc;
      fc.name = cname;
      fc.seen_time = c.seen_time ? c.seen_time : now;
      fc.active_time = c.active_time;
      std::vector<std::string> cids;
      for (const auto& [pid, own] : g.pending_to_consumer)
        if (own == cname) cids.push_back(pid);
      std::sort(cids.begin(), cids.end(), [](const std::string& a, const std::string& b) {
        StreamID ia, ib;
        stream_id_parse_strict(a, ia);
        stream_id_parse_strict(b, ib);
        return stream_id_cmp(ia, ib) < 0;
      });
      for (const auto& id : cids) fc.pending.push_back({id, dms(id), dcnt(id)});
      fg.consumers.push_back(std::move(fc));
    }
    out.groups.push_back(std::move(fg));
  }
  return true;
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
  out.entries_added = e.stream().entries_added;
  out.max_deleted_id = e.stream().max_deleted_id;
  out.groups.clear();
  for (const auto& [gname, g] : e.stream_groups()) {
    StreamGroupDump gd;
    gd.name = gname;
    gd.last_delivered_id = g.last_delivered_id;
    gd.entries_read = g.entries_read;
    gd.consumers = g.consumers;
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
  // Old private snapshots have no historical counter; retain their minimum valid value.
  e.stream().entries_added = d.entries_added.value_or(d.entries.size());
  e.stream().max_deleted_id = d.max_deleted_id;
  e.stream_last_ms() = d.last_ms;
  e.stream_last_seq() = d.last_seq;
  for (const auto& gd : d.groups) {
    Entry::StreamGroup g;
    g.last_delivered_id = gd.last_delivered_id;
    g.entries_read = gd.entries_read;
    g.consumers = gd.consumers;
    for (const auto& p : gd.pending) {
      g.consumers.try_emplace(p.consumer);
      g.pending_to_consumer[p.id] = p.consumer;
      g.pending_per_consumer[p.consumer] += 1;
      g.pending_delivery_ms[p.id] = p.delivery_ms;
      g.pending_delivery_count[p.id] = p.delivery_count;
    }
    e.stream_groups()[gd.name] = std::move(g);
  }
}

bool DataStore::xsetid(const std::string& key, const std::string& id, bool& wrongtype,
                       std::string& err, const std::string* entries_added,
                       const std::string* max_deleted_id) {
  wrongtype = false;
  err.clear();
  StreamID nid;
  if (!stream_id_parse_strict(id, nid)) {
    err = "ERR Invalid stream ID specified as stream command argument";
    return false;
  }
  StreamID maxdel{0, 0};
  if (max_deleted_id) {
    if (!stream_id_parse_strict(*max_deleted_id, maxdel)) {
      err = "ERR Invalid stream ID specified as stream command argument";
      return false;
    }
    // A supplied max-deleted tombstone may not exceed the new last-id (checked before
    // the key lookup, matching Redis xsetidCommand's option-parse order).
    if (stream_id_cmp(nid, maxdel) < 0) {
      err = "ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id";
      return false;
    }
  }
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    err = "ERR no such key";
    return false;
  }
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return false;
  }
  auto& st = it->second.stream();
  // The new last-id cannot regress below the stream's recorded max-deleted tombstone.
  StreamID cur_maxdel{0, 0};
  stream_id_parse_strict(st.max_deleted_id.empty() ? "0-0" : st.max_deleted_id, cur_maxdel);
  if (stream_id_cmp(nid, cur_maxdel) < 0) {
    err = "ERR The ID specified in XSETID is smaller than current max_deleted_entry_id";
    return false;
  }
  if (!st.entries.empty()) {
    // The new last-id cannot be below the current top (last valid) entry.
    StreamID top;
    stream_id_parse_strict(st.entries.back().first, top);
    if (stream_id_cmp(nid, top) < 0) {
      err = "ERR The ID specified in XSETID is smaller than the target stream top item";
      return false;
    }
    // A supplied entries-added counter cannot be below the current stream length.
    if (entries_added) {
      errno = 0;
      const auto ea = std::strtoull(entries_added->c_str(), nullptr, 10);
      if (st.entries.size() > ea) {
        err = "ERR The entries_added specified in XSETID is smaller than the target stream length";
        return false;
      }
    }
  }
  st.last_ms = nid.ms;
  st.last_seq = nid.seq;
  if (entries_added) {
    errno = 0;
    st.entries_added = std::strtoull(entries_added->c_str(), nullptr, 10);
  }
  if (max_deleted_id) st.max_deleted_id = stream_id_str(maxdel);
  return true;
}

std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>>
DataStore::xclaim(const std::string& key, const std::string& group, const std::string& consumer,
                  std::int64_t min_idle_ms, const std::vector<std::string>& ids_in, bool justid,
                  bool force, bool& wrongtype, std::string& err, const std::string* last_id,
                  bool* consumer_created, const StreamClaimOptions* options,
                  std::vector<StreamClaimEffect>* effects) {
  wrongtype = false;
  err.clear();
  if (consumer_created) *consumer_created = false;
  std::vector<std::pair<std::string, std::vector<std::pair<std::string, std::string>>>> out;
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'";
    return out;
  }
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return out;
  }
  auto git = it->second.stream_groups().find(group);
  if (git == it->second.stream_groups().end()) {
    err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'";
    return out;
  }
  auto& g = git->second;
  // Validate before LASTID, consumer creation or any PEL mutation.
  std::vector<std::string> ids;
  ids.reserve(ids_in.size());
  for (const auto& raw : ids_in) {
    StreamID id;
    if (!stream_id_parse_strict(raw, id)) {
      err = "ERR Invalid stream ID specified as stream command argument";
      return out;
    }
    ids.push_back(stream_id_str(id));
  }
  StreamID lid;
  if (last_id && !stream_id_parse_strict(*last_id, lid)) {
    err = "ERR Invalid stream ID specified as stream command argument";
    return out;
  }
  const std::int64_t now = now_ms();
  std::int64_t delivery = now;
  if (options) {
    if (options->time && *options->time >= 0 && *options->time <= now) delivery = *options->time;
    // Clamp bogus past/future values without overflowing the signed subtraction.
    if (options->idle && *options->idle >= 0 && *options->idle <= now)
      delivery = now - *options->idle;
  }
  // XCLAIM creates the target consumer as a side effect (needed so a replayed XCLAIM restores
  // the consumer even when the read that produced it was propagated as bare XCLAIMs).
  if (consumer_created && g.consumers.find(consumer) == g.consumers.end()) *consumer_created = true;
  g.consumers[consumer].seen_time = now;
  if (last_id) {
    StreamID cur{0, 0};
    stream_id_parse_strict(g.last_delivered_id, cur);
    if (stream_id_cmp(lid, cur) > 0) g.last_delivered_id = stream_id_str(lid);
  }
  auto& entries = it->second.stream_entries();
  bool got_data = false;
  for (const auto& id : ids) {
    auto pit = g.pending_to_consumer.find(id);
    const bool in_pel = pit != g.pending_to_consumer.end();
    if (!in_pel && !force) continue;
    auto eit =
        std::find_if(entries.begin(), entries.end(), [&](const auto& e) { return e.first == id; });
    if (eit == entries.end()) {  // entry was deleted from the stream — drop the dangling PEL entry
      if (in_pel) {
        if (effects)
          effects->push_back({key, group, pit->second, id, g.last_delivered_id,
                              g.pending_delivery_ms[id], g.pending_delivery_count[id]});
        auto cit = g.pending_per_consumer.find(pit->second);
        if (cit != g.pending_per_consumer.end() && cit->second > 0 && --cit->second == 0)
          g.pending_per_consumer.erase(cit);
        g.pending_to_consumer.erase(pit);
        g.pending_delivery_ms.erase(id);
        g.pending_delivery_count.erase(id);
      }
      continue;
    }
    if (in_pel) {
      const auto dm = g.pending_delivery_ms.find(id);
      const std::int64_t idle = now - (dm != g.pending_delivery_ms.end() ? dm->second : 0);
      if (min_idle_ms > 0 && idle < min_idle_ms) continue;
      if (pit->second != consumer) {
        auto cit = g.pending_per_consumer.find(pit->second);
        if (cit != g.pending_per_consumer.end() && cit->second > 0 && --cit->second == 0)
          g.pending_per_consumer.erase(cit);
        pit->second = consumer;
        g.pending_per_consumer[consumer] += 1;
      }
    } else {
      g.pending_to_consumer[id] = consumer;
      g.pending_per_consumer[consumer] += 1;
      g.pending_delivery_count[id] = 0;
    }
    g.pending_delivery_ms[id] = delivery;
    if (options && options->retrycount >= 0)
      g.pending_delivery_count[id] = options->retrycount;
    else if (!justid)
      g.pending_delivery_count[id] =
          static_cast<std::int64_t>(static_cast<std::uint64_t>(g.pending_delivery_count[id]) + 1);
    if (effects)
      effects->push_back({key, group, consumer, id, g.last_delivered_id, g.pending_delivery_ms[id],
                          g.pending_delivery_count[id]});
    out.push_back(*eit);
    got_data = true;
  }
  if (got_data) g.consumers[consumer].active_time = now;
  return out;
}

bool DataStore::validate_xautoclaim_start(const std::string& start, std::string& err) {
  StreamID id;
  if (stream_id_parse_bound(start, true, id)) return true;
  const bool overflow = !start.empty() && start[0] == '(' &&
                        stream_id_parse_strict(start.substr(1), id) && id.ms == UINT64_MAX &&
                        id.seq == UINT64_MAX;
  err = overflow ? "invalid start ID for the interval"
                 : "Invalid stream ID specified as stream command argument";
  return false;
}

DataStore::AutoClaimResult DataStore::xautoclaim(const std::string& key, const std::string& group,
                                                 const std::string& consumer,
                                                 std::int64_t min_idle_ms, const std::string& start,
                                                 std::int64_t count, bool justid, bool& wrongtype,
                                                 std::string& err, bool* consumer_created,
                                                 std::vector<StreamClaimEffect>* effects) {
  wrongtype = false;
  err.clear();
  if (consumer_created) *consumer_created = false;
  AutoClaimResult res;
  res.next_cursor = "0-0";
  StreamID startk{0, 0};
  if (!validate_xautoclaim_start(start, err)) return res;
  stream_id_parse_bound(start, true, startk);
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) {
    err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'";
    return res;
  }
  if (it->second.type() != ValueType::Stream) {
    wrongtype = true;
    return res;
  }
  auto git = it->second.stream_groups().find(group);
  if (git == it->second.stream_groups().end()) {
    err = "NOGROUP No such key '" + key + "' or consumer group '" + group + "'";
    return res;
  }
  auto& g = git->second;
  const std::int64_t now = now_ms();
  if (count <= 0) count = 100;
  // Ensure the target consumer exists (XAUTOCLAIM creates it as a side effect).
  if (consumer_created && g.consumers.find(consumer) == g.consumers.end()) *consumer_created = true;
  {
    auto& c = g.consumers[consumer];
    c.seen_time = now;
  }
  // Iterate the group PEL (id-ordered) from the start cursor — this is what lets XAUTOCLAIM
  // surface entries that were XDELed from the stream but still linger in the PEL.
  std::vector<std::string> pel;
  for (const auto& [pid, _] : g.pending_to_consumer) {
    StreamID pk;
    if (stream_id_parse_strict(pid, pk) && stream_id_cmp(pk, startk) >= 0) pel.push_back(pid);
  }
  std::sort(pel.begin(), pel.end(), [](const std::string& a, const std::string& b) {
    StreamID ia, ib;
    stream_id_parse_strict(a, ia);
    stream_id_parse_strict(b, ib);
    return stream_id_cmp(ia, ib) < 0;
  });
  const auto& entries = it->second.stream_entries();
  auto exists = [&](const std::string& id) {
    return std::any_of(entries.begin(), entries.end(),
                       [&](const auto& e) { return e.first == id; });
  };
  auto remove_pel = [&](const std::string& id) {
    auto pit = g.pending_to_consumer.find(id);
    if (pit != g.pending_to_consumer.end()) {
      if (effects)
        effects->push_back({key, group, pit->second, id, g.last_delivered_id,
                            g.pending_delivery_ms[id], g.pending_delivery_count[id]});
      auto cit = g.pending_per_consumer.find(pit->second);
      if (cit != g.pending_per_consumer.end() && cit->second > 0 && --cit->second == 0)
        g.pending_per_consumer.erase(cit);
      g.pending_to_consumer.erase(pit);
    }
    g.pending_delivery_ms.erase(id);
    g.pending_delivery_count.erase(id);
  };
  std::int64_t attempts = count * 10;  // bound iterations like Redis (attempts_factor=10)
  std::int64_t remaining = count;
  std::size_t i = 0;
  bool got_data = false;
  for (; i < pel.size() && attempts > 0 && remaining > 0; --attempts) {
    const std::string id = pel[i];
    ++i;
    if (!exists(id)) {  // entry was XDELed/trimmed away → surface as deleted, drop NACK
      remove_pel(id);
      res.deleted.push_back(id);
      --remaining;
      continue;
    }
    auto dm = g.pending_delivery_ms.find(id);
    const std::int64_t idle = now - (dm != g.pending_delivery_ms.end() ? dm->second : 0);
    if (min_idle_ms > 0 && idle < min_idle_ms) continue;  // still owned; skip but don't claim
    auto pit = g.pending_to_consumer.find(id);
    if (pit != g.pending_to_consumer.end() && pit->second != consumer) {
      auto cit = g.pending_per_consumer.find(pit->second);
      if (cit != g.pending_per_consumer.end() && cit->second > 0 && --cit->second == 0)
        g.pending_per_consumer.erase(cit);
      pit->second = consumer;
      g.pending_per_consumer[consumer] += 1;
    }
    g.pending_delivery_ms[id] = now;
    if (!justid)
      g.pending_delivery_count[id] =
          static_cast<std::int64_t>(static_cast<std::uint64_t>(g.pending_delivery_count[id]) + 1);
    if (effects)
      effects->push_back({key, group, consumer, id, g.last_delivered_id, g.pending_delivery_ms[id],
                          g.pending_delivery_count[id]});
    auto eit =
        std::find_if(entries.begin(), entries.end(), [&](const auto& e) { return e.first == id; });
    res.claimed.push_back(*eit);
    got_data = true;
    --remaining;
  }
  if (got_data) g.consumers[consumer].active_time = now;
  // Cursor = next unprocessed PEL id, or 0-0 when the PEL was fully scanned.
  res.next_cursor = (i < pel.size()) ? pel[i] : "0-0";
  return res;
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
      if (!(iss >> std::quoted(value))) {
        err = "malformed string";
        current_db_ = prev_db;
        return false;
      }
      set(key, value, false, false, ttl, false);
    } else if (typ == "H") {
      std::size_t n = 0;
      if (!(iss >> n)) {
        err = "malformed hash";
        current_db_ = prev_db;
        return false;
      }
      std::vector<std::pair<std::string, std::string>> fields;
      for (std::size_t i = 0; i < n; ++i) {
        std::string f, v;
        if (!(iss >> std::quoted(f) >> std::quoted(v))) {
          err = "malformed hash field";
          current_db_ = prev_db;
          return false;
        }
        fields.push_back({f, v});
      }
      bool wrong = false;
      (void)hset(key, fields, wrong);
      if (ttl.has_value()) expire(key, *ttl);
    } else if (typ == "L") {
      std::size_t n = 0;
      if (!(iss >> n)) {
        err = "malformed list";
        current_db_ = prev_db;
        return false;
      }
      std::vector<std::string> values;
      for (std::size_t i = 0; i < n; ++i) {
        std::string v;
        if (!(iss >> std::quoted(v))) {
          err = "malformed list value";
          current_db_ = prev_db;
          return false;
        }
        values.push_back(v);
      }
      bool wrong = false;
      (void)rpush(key, values, wrong);
      if (ttl.has_value()) expire(key, *ttl);
    } else if (typ == "T") {
      std::size_t n = 0;
      if (!(iss >> n)) {
        err = "malformed set";
        current_db_ = prev_db;
        return false;
      }
      std::vector<std::string> members;
      for (std::size_t i = 0; i < n; ++i) {
        std::string m;
        if (!(iss >> std::quoted(m))) {
          err = "malformed set member";
          current_db_ = prev_db;
          return false;
        }
        members.push_back(m);
      }
      bool wrong = false;
      (void)sadd(key, members, wrong);
      if (ttl.has_value()) expire(key, *ttl);
    } else if (typ == "Z") {
      std::size_t n = 0;
      if (!(iss >> n)) {
        err = "malformed zset";
        current_db_ = prev_db;
        return false;
      }
      for (std::size_t i = 0; i < n; ++i) {
        std::string m;
        double sc = 0;
        if (!(iss >> std::quoted(m) >> sc)) {
          err = "malformed zset member";
          current_db_ = prev_db;
          return false;
        }
        (void)zadd_one(key, sc, m, false, false, false, false, false);
      }
      if (ttl.has_value()) expire(key, *ttl);
    } else if (typ == "X") {
      std::size_t n = 0;
      std::uint64_t lm = 0, ls = 0;
      if (!(iss >> n >> lm >> ls)) {
        err = "malformed stream";
        current_db_ = prev_db;
        return false;
      }
      for (std::size_t i = 0; i < n; ++i) {
        std::string id;
        std::size_t fn = 0;
        if (!(iss >> std::quoted(id) >> fn)) {
          err = "malformed stream item";
          current_db_ = prev_db;
          return false;
        }
        std::vector<std::pair<std::string, std::string>> fields;
        for (std::size_t j = 0; j < fn; ++j) {
          std::string f, v;
          if (!(iss >> std::quoted(f) >> std::quoted(v))) {
            err = "malformed stream field";
            current_db_ = prev_db;
            return false;
          }
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

std::uint64_t DataStore::mutation_epoch() const {
  return mutation_epoch_;
}
void DataStore::increment_mutation_epoch() {
  ++mutation_epoch_;
}

// ── Eviction (maxmemory-policy) ──

namespace {
// Redis's LRU clock (LRU_CLOCK(), LRU_BITS, LRU_CLOCK_RESOLUTION): seconds in 24 bits, so it wraps
// about every 194 days.
constexpr std::uint32_t kLruClockMax = (1u << 24) - 1;

std::uint32_t lru_clock() {
  return static_cast<std::uint32_t>((DataStore::now_ms() / 1000) & kLruClockMax);
}

// Redis's estimateObjectIdleTime(), in seconds: an access clock above the current one has wrapped.
std::int64_t lru_idle_seconds(std::uint32_t lru) {
  const std::uint32_t now = lru_clock();
  return now >= lru ? now - lru : static_cast<std::int64_t>(now) + (kLruClockMax - lru);
}
}  // namespace

void DataStore::touch_lru(Entry& e, bool lfu) {
  if (e.lru_fresh) {
    e.lru_fresh = false;
    if (lfu) {
      e.lru = 5;  // LFU_INIT_VAL
      return;
    }
  }
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
      const double p = 1.0 / (base * static_cast<double>(lfu_log_factor_) + 1.0);
      if (r < p) ++counter;
    }
    e.lru = counter;
  } else {
    e.lru = lru_clock();
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
  return lru_idle_seconds(it->second.lru);
}

std::optional<std::uint32_t> DataStore::object_lru_clock(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  return it->second.lru;  // LRU clock (or LFU counter), as DEBUG OBJECT's lru field shows it
}

std::optional<std::int64_t> DataStore::object_freq(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  return static_cast<std::int64_t>(it->second.lru_fresh ? 5 : (it->second.lru & 0xFF));
}

bool DataStore::set_object_idle(const std::string& key, std::int64_t idle_seconds) {
  auto& db = dbs_[current_db_];
  auto it = db.find(key);
  if (it == db.end()) return false;
  // Redis's objectSetLRUOrLFU(): the access clock `idle_seconds` ago, wrapped once into the clock's
  // range and then cut to its 24 bits (so an idle time past the range wraps, as in Redis).
  std::int64_t lru_abs = static_cast<std::int64_t>(lru_clock()) - idle_seconds;
  if (lru_abs < 0) lru_abs += kLruClockMax;
  it->second.lru = static_cast<std::uint32_t>(lru_abs) & kLruClockMax;
  it->second.lru_fresh = false;
  return true;
}

bool DataStore::set_object_freq(const std::string& key, std::int64_t freq) {
  auto& db = dbs_[current_db_];
  auto it = db.find(key);
  if (it == db.end()) return false;
  it->second.lru = static_cast<std::uint32_t>(freq & 0xFF);
  it->second.lru_fresh = false;
  return true;
}

std::optional<DataStore::EvictVictim> DataStore::evict_one(MaxmemoryPolicy policy, int samples) {
  if (policy == MaxmemoryPolicy::NoEviction) return std::nullopt;
  const bool volatile_only =
      policy == MaxmemoryPolicy::VolatileLru || policy == MaxmemoryPolicy::VolatileLfu ||
      policy == MaxmemoryPolicy::VolatileRandom || policy == MaxmemoryPolicy::VolatileTtl;
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
  struct Cand {
    int db;
    std::string key;
    std::uint32_t lru;
    std::int64_t expire;
  };
  std::optional<Cand> best;
  auto better = [&](const Cand& c) -> bool {
    if (!best) return true;
    if (ttl) return c.expire < best->expire;                                // soonest to expire
    if (lfu) return (c.lru & 0xFF) < (best->lru & 0xFF);                    // lowest frequency
    if (lru) return lru_idle_seconds(c.lru) > lru_idle_seconds(best->lru);  // longest idle
    return (rng() & 1) != 0;                                                // random: keep flipping
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
        if (logically_expired(e, now)) continue;
        if (volatile_only && !has_ttl) continue;
        // A key no command has touched yet counts as LFU_INIT_VAL under LFU.
        Cand c{static_cast<int>((db_start + di) % ndb), it->first, lfu && e.lru_fresh ? 5u : e.lru,
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
  vdb.erase(vit);
  record_evicted();
  return EvictVictim{best->db, best->key};
}

std::tuple<std::size_t, std::size_t, std::int64_t> DataStore::keyspace_stats() const {
  const auto& db = dbs_[current_db_];
  const auto now = now_ms();
  // Keys hidden while expiry is paused, or by a replica's command, are still in Redis's dict, so
  // they count (with a TTL of 0).
  std::size_t total = hidden_expired_[current_db_].size() + replica_hidden_[current_db_].size();
  std::size_t with_expiry = total;
  std::int64_t ttl_sum = 0;
  for (const auto& [k, e] : db) {
    ++total;
    if (e.expire_at_ms.has_value()) {
      ++with_expiry;
      const auto ttl = *e.expire_at_ms - now;
      if (ttl > 0) ttl_sum += ttl;
    }
  }
  const std::int64_t avg_ttl =
      with_expiry > 0 ? ttl_sum / static_cast<std::int64_t>(with_expiry) : 0;
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
          for (const auto& [f, v] : e.hash()) {
            cmd.push_back(f);
            cmd.push_back(v);
          }
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
          for (const auto& [m, s] : e.zset()) {
            cmd.push_back(fmt_double(s));
            cmd.push_back(m);
          }
          out.push_back(std::move(cmd));
          break;
        }
        case ValueType::Stream: {
          const auto& st = e.stream();
          if (!st.entries.empty()) {
            for (const auto& [id, fields] : st.entries) {
              std::vector<std::string> cmd{"XADD", key, id};
              for (const auto& [f, v] : fields) {
                cmd.push_back(f);
                cmd.push_back(v);
              }
              out.push_back(std::move(cmd));
            }
          } else {
            // Empty stream: streams are not deleted when they lose their last entry, so
            // recreate the key with the XADD MAXLEN 0 trick (Redis rewriteStreamObject).
            out.push_back({"XADD", key, "MAXLEN", "0", "0-1", "x", "y"});
          }
          // Restore last-id / entries-added / max-deleted-entry-id even after XDEL/trim.
          out.push_back({"XSETID", key, stream_id_str({st.last_ms, st.last_seq}), "ENTRIESADDED",
                         std::to_string(st.entries_added), "MAXDELETEDID",
                         st.max_deleted_id.empty() ? "0-0" : st.max_deleted_id});
          // Recreate consumer groups and their pending-entries lists. XGROUP CREATE
          // rebuilds the group (preserving its entries-read counter, including the -1
          // "invalid" sentinel); XGROUP CREATECONSUMER recreates every consumer — even
          // those with no PEL (e.g. created by NOACK reads) — and XCLAIM ... FORCE re-adds
          // each pending message to its owning consumer's PEL.
          for (const auto& [gname, grp] : st.groups) {
            out.push_back({"XGROUP", "CREATE", key, gname, grp.last_delivered_id, "ENTRIESREAD",
                           std::to_string(grp.entries_read)});
            for (const auto& [cname, _] : grp.consumers)
              out.push_back({"XGROUP", "CREATECONSUMER", key, gname, cname});
            for (const auto& [pid, consumer] : grp.pending_to_consumer) {
              auto ms_it = grp.pending_delivery_ms.find(pid);
              auto cnt_it = grp.pending_delivery_count.find(pid);
              const std::string dms =
                  ms_it != grp.pending_delivery_ms.end() ? std::to_string(ms_it->second) : "0";
              const std::string dcnt =
                  cnt_it != grp.pending_delivery_count.end() ? std::to_string(cnt_it->second) : "1";
              out.push_back({"XCLAIM", key, gname, consumer, "0", pid, "TIME", dms, "RETRYCOUNT",
                             dcnt, "JUSTID", "FORCE"});
            }
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
  return std::string(encoding_of(it->second));
}

const char* DataStore::encoding_of(
    Entry& e) {  // non-const: seeing a big encoding sets the sticky flag
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
    return e.encoding_big ? big_enc : small_enc;
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
        if (bytes > budget) {
          small = false;
          break;
        }
      }
    } else {
      // 0 is treated as 1 (a single element is still listpack).
      const std::int64_t entry_limit = lim.list_size == 0 ? 1 : lim.list_size;
      small = within(e.list().size(), entry_limit);
      if (small)
        for (const auto& v : e.list())
          if (v.size() > 64) {
            small = false;
            break;
          }
    }
    return small ? "listpack" : "quicklist";
  }
  if (e.type() == ValueType::Set) {
    // The member walk only for a set small enough to be an intset (SSCAN asks on every call).
    if (!e.encoding_big && !e.encoding_no_intset && within(e.set().size(), lim.set_intset)) {
      bool all_int = true;
      for (const auto& v : e.set())
        if (!is_int(v)) {
          all_int = false;
          break;
        }
      if (all_int) return "intset";
    }
    bool small = within(e.set().size(), lim.set_entries);
    if (small)
      for (const auto& v : e.set())
        if (v.size() > static_cast<std::size_t>(lim.set_value)) {
          small = false;
          break;
        }
    return sticky(small, "listpack", "hashtable");
  }
  if (e.type() == ValueType::Hash) {
    bool small = within(e.hash().size(), lim.hash_entries);
    if (small)
      for (const auto& [f, v] : e.hash()) {
        if (f.size() > static_cast<std::size_t>(lim.hash_value) ||
            v.size() > static_cast<std::size_t>(lim.hash_value)) {
          small = false;
          break;
        }
      }
    return sticky(small, "listpack", "hashtable");
  }
  if (e.type() == ValueType::ZSet) {
    bool small = within(e.zset().size(), lim.zset_entries);
    if (small)
      for (const auto& [m, _] : e.zset()) {
        if (m.size() > static_cast<std::size_t>(lim.zset_value)) {
          small = false;
          break;
        }
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
  if (e.expire_at_ms.has_value())
    oss << "ttl=" << *e.expire_at_ms << "|";
  else
    oss << "ttl=-1|";
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
    std::sort(items.begin(), items.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
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

namespace {
std::string entry_digest(const Entry& e) {
  std::ostringstream out;
  out << std::hex << std::hash<std::string>{}(entry_digest_payload(e));
  return out.str();
}
}  // namespace

std::optional<std::string> DataStore::debug_digest_value(const std::string& key) {
  auto& db = dbs_[current_db_];
  expire_if_needed(db, key);
  auto it = db.find(key);
  if (it == db.end()) return std::nullopt;
  return entry_digest(it->second);
}

std::optional<std::string> DataStore::watch_digest(const std::string& key) const {
  const auto& db = dbs_[current_db_];
  const auto it = db.find(key);
  if (it == db.end() || logically_expired(it->second)) return std::nullopt;
  return entry_digest(it->second);
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
      if (logically_expired(e, now)) continue;
      const auto payload = std::to_string(dbi) + "|" + key + "|" + entry_digest_payload(e);
      acc ^= std::hash<std::string>{}(payload);
    }
  }
  char buf[17];
  std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(acc));
  return std::string(24, '0') + buf;
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
      // Collect expired keys in this bucket, then erase by key (local_iterator can't be passed to
      // erase)
      std::vector<std::string> to_del;
      for (auto it = db.begin(bucket); it != db.end(bucket) && scanned < budget_per_db;
           ++it, ++scanned) {
        if (logically_expired(it->second, now)) to_del.push_back(it->first);
      }
      for (const auto& k : to_del) {
        remove_expired(db, db.find(k));
        expired.emplace_back(static_cast<int>(dbi), k);
      }
      bucket = (bucket + 1) % num_buckets;
    } while (scanned < budget_per_db && bucket != start_bucket);
  }
  return expired;
}

void DataStore::flushall() {
  for (auto& db : dbs_) db.clear();
  for (auto& hidden : hidden_expired_) hidden.clear();
  for (auto& hidden : replica_hidden_) hidden.clear();
}

void DataStore::flushdb() {
  dbs_[current_db_].clear();
  hidden_expired_[current_db_].clear();
  replica_hidden_[current_db_].clear();
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
      std::size_t keys = 0;
      for (const auto& db : batch) keys += db.size();
      batch.clear();
      lazyfree_pending_.fetch_sub(keys, std::memory_order_relaxed);
    }
  });
}

void DataStore::lazyfree_enqueue(DB&& db) {
  if (db.empty()) return;
  ensure_lazyfree_thread();
  lazyfree_pending_.fetch_add(db.size(), std::memory_order_relaxed);
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
    case ValueType::String:
      return false;
    case ValueType::Hash:
      return e.hash().size() > kBig;
    case ValueType::List:
      return e.list().size() > kBig;
    case ValueType::Set:
      return e.set().size() > kBig;
    case ValueType::ZSet:
      return e.zset().size() > kBig;
    case ValueType::Stream:
      return e.stream_entries().size() > kBig;
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
    tmp.insert(db.extract(it));  // O(1) node move out of the live map
    lazyfree_enqueue(std::move(tmp));
  } else {
    db.erase(it);
  }
  return true;
}

void DataStore::flushdb_async() {
  DB tmp;
  std::swap(tmp, dbs_[current_db_]);  // O(1): live db becomes empty immediately
  lazyfree_enqueue(std::move(tmp));
  hidden_expired_[current_db_].clear();
  replica_hidden_[current_db_].clear();
}

void DataStore::flushall_async() {
  for (auto& db : dbs_) {
    DB tmp;
    std::swap(tmp, db);
    lazyfree_enqueue(std::move(tmp));
  }
  for (auto& hidden : hidden_expired_) hidden.clear();
  for (auto& hidden : replica_hidden_) hidden.clear();
}

std::size_t DataStore::dbsize() {
  return dbs_[current_db_].size() + hidden_expired_[current_db_].size() +
         replica_hidden_[current_db_].size();
}

std::pair<std::uint64_t, std::vector<std::string>> DataStore::scan(
    std::uint64_t cursor, std::int64_t count, const std::optional<std::string>& pattern,
    const std::optional<std::string>& type) {
  auto& db = dbs_[current_db_];
  std::vector<std::string> keys;
  const std::uint64_t next = scan_dict(db, cursor, count, [&](const auto& kv) {
    if (scan_match(pattern, kv.first)) keys.push_back(kv.first);
  });
  // Step 3 of scanGenericCommand(), after the walk (an erase never resizes the table, so the cursor
  // stays good): with TYPE, lookupKeyReadWithFlags(LOOKUP_NOTOUCH|LOOKUP_NONOTIFY) of each key,
  // which expires it and counts a hit or a miss; without TYPE, expireIfNeeded().
  std::vector<std::string> out;
  out.reserve(keys.size());
  for (auto& key : keys) {
    if (type) {
      expire_if_needed(db, key);
      const auto it = db.find(key);
      if (it == db.end()) {
        record_miss();
        continue;
      }
      record_hit();
      if (*type != type_name(it->second.type())) continue;
    } else if (expire_if_needed(db, key)) {
      continue;
    }
    out.push_back(std::move(key));
  }
  return {next, out};
}

void DataStore::shrink_sparse_tables() {
  for (auto& db : dbs_) db.shrink_if_sparse();
}

DataStore& store() {
  static DataStore s;
  return s;
}

}  // namespace peadb
