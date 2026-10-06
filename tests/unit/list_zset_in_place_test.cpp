// In-place list and sorted-set edits (compat/delta.md item 31): LSET, LREM, LINSERT, LTRIM and
// ZREM change the collection without deleting the key, so the TTL stays, and they delete the key
// only when it becomes empty. LREM and LTRIM are checked against a simple model of Redis's rules
// on random lists.
#include "datastore.hpp"

#include <cstdint>
#include <deque>
#include <iostream>
#include <limits>
#include <random>
#include <string>
#include <vector>

using peadb::DataStore;

#define ASSERT(cond, msg)                                             \
  do {                                                                \
    if (!(cond)) {                                                    \
      std::cerr << "FAIL: " << msg << " (line " << __LINE__ << ")\n"; \
      return 1;                                                       \
    }                                                                 \
  } while (0)

namespace {

std::vector<std::string> all(DataStore& s, const std::string& key) {
  bool wt = false;
  return s.lrange(key, 0, -1, wt);
}

// Redis's lremCommand(): walk from the head (count >= 0) or the tail (count < 0) and remove up
// to |count| matches (0 = all).
std::vector<std::string> model_lrem(std::vector<std::string> l, std::int64_t count,
                                    const std::string& v, std::int64_t& removed) {
  removed = 0;
  // Worked out apart from the code under test: |INT64_MIN| is larger than any list, so it means
  // "every match" like 0 does.
  const bool every = count == 0 || count == std::numeric_limits<std::int64_t>::min();
  const std::uint64_t limit = every ? std::numeric_limits<std::uint64_t>::max()
                                    : static_cast<std::uint64_t>(count < 0 ? -count : count);
  std::vector<bool> drop(l.size(), false);
  if (count >= 0) {
    for (std::size_t i = 0; i < l.size() && static_cast<std::uint64_t>(removed) < limit; ++i)
      if (l[i] == v) drop[i] = true, ++removed;
  } else {
    for (std::size_t i = l.size(); i-- > 0 && static_cast<std::uint64_t>(removed) < limit;)
      if (l[i] == v) drop[i] = true, ++removed;
  }
  std::vector<std::string> out;
  for (std::size_t i = 0; i < l.size(); ++i)
    if (!drop[i]) out.push_back(l[i]);
  return out;
}

// Redis's ltrimCommand() index rules.
std::vector<std::string> model_ltrim(const std::vector<std::string>& l, std::int64_t start,
                                     std::int64_t stop) {
  const auto n = static_cast<std::int64_t>(l.size());
  if (start < 0) start += n;
  if (stop < 0) stop += n;
  if (start < 0) start = 0;
  if (start > stop || start >= n) return {};
  if (stop >= n) stop = n - 1;
  return std::vector<std::string>(l.begin() + start, l.begin() + stop + 1);
}

int test_random_lrem_ltrim() {
  DataStore s;
  std::mt19937_64 rng(31);
  const std::vector<std::int64_t> extremes = {std::numeric_limits<std::int64_t>::min(),
                                              std::numeric_limits<std::int64_t>::max(), -1, 0, 1};
  for (int round = 0; round < 3000; ++round) {
    std::vector<std::string> l(rng() % 12);
    for (auto& e : l) e = std::string(1, static_cast<char>('a' + rng() % 3));
    bool wt = false, removed_key = false;
    s.del("k");
    if (!l.empty()) s.rpush("k", l, wt);
    s.expire("k", DataStore::now_ms() + 100000);
    if (round % 2 == 0) {
      const std::int64_t count = round % 10 == 0 ? extremes[rng() % extremes.size()]
                                                 : static_cast<std::int64_t>(rng() % 9) - 4;
      const std::string v(1, static_cast<char>('a' + rng() % 3));
      std::int64_t want_removed = 0;
      const auto want = model_lrem(l, count, v, want_removed);
      const auto got_removed = s.lrem("k", count, v, wt, removed_key);
      ASSERT(!wt, "lrem wrongtype");
      ASSERT(got_removed == want_removed, "lrem count, round " << round);
      ASSERT(all(s, "k") == want, "lrem contents, round " << round);
      ASSERT(removed_key == (want.empty() && !l.empty()), "lrem key_removed, round " << round);
    } else {
      const std::int64_t start = round % 10 == 1 ? extremes[rng() % extremes.size()]
                                                 : static_cast<std::int64_t>(rng() % 17) - 8;
      const std::int64_t stop = round % 10 == 3 ? extremes[rng() % extremes.size()]
                                                : static_cast<std::int64_t>(rng() % 17) - 8;
      const auto want = model_ltrim(l, start, stop);
      const bool existed = s.ltrim("k", start, stop, wt, removed_key);
      ASSERT(!wt, "ltrim wrongtype");
      ASSERT(existed == !l.empty(), "ltrim existed, round " << round);
      ASSERT(all(s, "k") == want, "ltrim contents, round " << round);
      ASSERT(removed_key == (want.empty() && !l.empty()), "ltrim key_removed, round " << round);
    }
    if (s.type_of("k") != "none") ASSERT(s.pttl("k") > 90000, "TTL lost, round " << round);
  }
  return 0;
}

int test_lset_linsert() {
  DataStore s;
  bool wt = false;
  ASSERT(s.lset("k", 0, "x") == DataStore::LsetResult::NoKey, "lset on a missing key");
  ASSERT(s.linsert("k", true, "a", "x", wt) == 0 && !wt, "linsert on a missing key");
  s.rpush("k", {"a", "b", "c"}, wt);
  s.expire("k", DataStore::now_ms() + 100000);
  ASSERT(s.lset("k", -1, "z") == DataStore::LsetResult::Ok, "lset -1");
  ASSERT(s.lset("k", 3, "z") == DataStore::LsetResult::OutOfRange, "lset past the end");
  ASSERT(s.lset("k", -4, "z") == DataStore::LsetResult::OutOfRange, "lset before the start");
  ASSERT(s.lset("k", std::numeric_limits<std::int64_t>::min(), "z") ==
             DataStore::LsetResult::OutOfRange,
         "lset INT64_MIN");
  ASSERT(s.linsert("k", true, "a", "0", wt) == 4, "linsert before the head");
  ASSERT(s.linsert("k", false, "z", "9", wt) == 5, "linsert after the tail");
  ASSERT(s.linsert("k", false, "nope", "9", wt) == -1, "linsert without the pivot");
  ASSERT((all(s, "k") == std::vector<std::string>{"0", "a", "b", "z", "9"}), "list contents");
  ASSERT(s.pttl("k") > 90000, "TTL lost by lset/linsert");
  s.set("str", "v", false, false, std::nullopt, false);
  ASSERT(s.lset("str", 0, "x") == DataStore::LsetResult::WrongType, "lset wrongtype");
  s.linsert("str", true, "a", "x", wt);
  ASSERT(wt, "linsert wrongtype");
  return 0;
}

int test_zrem() {
  DataStore s;
  bool wt = false, removed_key = false;
  ASSERT(s.zrem("z", {"a"}, wt, removed_key) == 0 && !wt && !removed_key, "zrem missing key");
  for (int i = 0; i < 5; ++i)
    s.zadd_one("z", i, "m" + std::to_string(i), false, false, false, false, false);
  s.expire("z", DataStore::now_ms() + 100000);
  ASSERT(s.zrem("z", {"m1", "nope", "m1", "m3"}, wt, removed_key) == 2 && !removed_key, "zrem two");
  const auto left = s.zrange("z", 0, -1, true, wt);
  ASSERT(
      left.size() == 3 && left[0].first == "m0" && left[1].first == "m2" && left[2].first == "m4",
      "zrem keeps the order index in step");
  ASSERT(s.pttl("z") > 90000, "TTL lost by zrem");
  ASSERT(s.zrem("z", {"m0", "m2", "m4"}, wt, removed_key) == 3 && removed_key,
         "zrem empties the key");
  ASSERT(s.type_of("z") == "none", "empty zset deleted");
  // -0.0 and 0.0 are one score: the member is found and both indexes drop it.
  s.zadd_one("neg", -0.0, "a", false, false, false, false, false);
  s.zadd_one("neg", 0.0, "b", false, false, false, false, false);
  ASSERT(s.zrem("neg", {"a"}, wt, removed_key) == 1 && !removed_key, "zrem of a -0.0 score");
  const auto neg_left = s.zrange("neg", 0, -1, true, wt);
  ASSERT(neg_left.size() == 1 && neg_left[0].first == "b", "zrem -0.0 left the index in step");
  ASSERT(s.zrem("neg", {"b"}, wt, removed_key) == 1 && removed_key, "zrem of the 0.0 score");
  s.set("str", "v", false, false, std::nullopt, false);
  s.zrem("str", {"a"}, wt, removed_key);
  ASSERT(wt, "zrem wrongtype");
  return 0;
}

int test_incrbyfloat_keeps_ttl() {
  DataStore s;
  bool ok = false;
  std::string err;
  s.set("f", "1.5", false, false, DataStore::now_ms() + 100000, false);
  ASSERT(s.incrbyfloat("f", 1, ok, err) == "2.5" && ok, "incrbyfloat value");
  ASSERT(s.pttl("f") > 90000, "TTL lost by incrbyfloat");
  return 0;
}

}  // namespace

int main() {
  if (test_random_lrem_ltrim() || test_lset_linsert() || test_zrem() ||
      test_incrbyfloat_keeps_ttl())
    return 1;
  std::cout << "list_zset_in_place_test: OK\n";
  return 0;
}
