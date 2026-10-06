// peadb::Dict (include/dict.hpp) against std::unordered_map / std::unordered_set as a model, and the
// SCAN guarantee of its scan(): every element present for the whole scan is returned at least once,
// however the table grows or shrinks between calls (compat/delta.md item 32).

#include "dict.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <map>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace {

int failures = 0;

void check(bool ok, const std::string& what) {
  if (!ok) {
    std::cerr << "FAIL: " << what << "\n";
    ++failures;
  }
}

// The hash PeaDB's tables use (keyed SipHash-1-2).
using Map = peadb::DictMap<std::string, std::string, peadb::DictStringHash>;
using Set = peadb::DictSet<std::string, peadb::DictStringHash>;

// Every element is in the bucket its hash names, each bucket chain holds bucket_size() elements, and
// iteration visits each element once, matching the model.
void check_map(const Map& d, const std::unordered_map<std::string, std::string>& model, const std::string& what) {
  check(d.size() == model.size(), what + ": size " + std::to_string(d.size()) + " vs " + std::to_string(model.size()));
  check(d.empty() == model.empty(), what + ": empty");
  std::map<std::string, std::string> seen;
  std::size_t visits = 0;
  for (const auto& [k, v] : d) {
    ++visits;
    seen[k] = v;
  }
  check(visits == model.size(), what + ": iteration visits " + std::to_string(visits));
  check(seen == std::map<std::string, std::string>(model.begin(), model.end()), what + ": iteration contents");
  if (d.bucket_count() > 0) {
    const std::size_t nb = d.bucket_count();
    check((nb & (nb - 1)) == 0, what + ": power-of-two bucket count");
    std::size_t total = 0;
    for (std::size_t b = 0; b < nb; ++b) {
      total += d.bucket_size(b);
      for (auto it = d.begin(b); it != d.end(b); ++it) check(d.bucket(it->first) == b, what + ": element in its bucket");
    }
    check(total == d.size(), what + ": bucket sizes add up");
    check(d.size() <= nb, what + ": load factor at most 1");
  } else {
    check(d.empty(), what + ": no buckets only when empty");
  }
  for (const auto& [k, v] : model) {
    auto it = d.find(k);
    check(it != d.end() && it->second == v, what + ": find " + k);
    check(d.count(k) == 1 && d.contains(k), what + ": count " + k);
  }
}

void random_map_ops() {
  std::mt19937 rng(12345);
  Map d;
  std::unordered_map<std::string, std::string> model;
  auto key = [&](int range) { return "k" + std::to_string(rng() % range); };
  for (int step = 0; step < 200000; ++step) {
    const int range = (step / 20000) % 2 == 0 ? 5000 : 200;  // phases of growth and of mostly deletes
    const int op = rng() % 13;
    const std::string k = key(range);
    const std::string v = "v" + std::to_string(rng() % 1000);
    switch (op) {
      case 0: {
        auto r = d.insert({k, v});
        auto m = model.insert({k, v});
        check(r.second == m.second && r.first->second == m.first->second, "insert");
        break;
      }
      case 1: {
        auto r = d.emplace(k, v);
        auto m = model.emplace(k, v);
        check(r.second == m.second && r.first->first == k, "emplace");
        break;
      }
      case 2: {
        auto r = d.try_emplace(k, v);
        auto m = model.try_emplace(k, v);
        check(r.second == m.second && r.first->second == m.first->second, "try_emplace");
        break;
      }
      case 3:
        d[k] = v;
        model[k] = v;
        break;
      case 4:
      case 5:
        check(d.erase(k) == model.erase(k), "erase by key");
        break;
      case 6: {
        auto it = d.find(k);
        if (it != d.end()) {
          auto next = std::next(it);
          d.erase(it);
          model.erase(k);
          check(next == d.end() || model.count(next->first) == 1, "the next iterator stays good after an erase");
        }
        break;
      }
      case 7: {
        auto nh = d.extract(k);
        const bool had = model.count(k) == 1;
        check(nh.empty() != had, "extract");
        if (!nh.empty()) {
          check(nh.key() == k && nh.mapped() == model[k], "extracted node");
          Map other;
          auto r = other.insert(std::move(nh));
          check(r.inserted && nh.empty() && other.size() == 1 && other.find(k)->second == model[k], "insert node");
          auto back = other.extract(other.begin());
          auto r2 = d.insert(std::move(back));
          check(r2.inserted && r2.position->first == k, "reinsert node");
        }
        break;
      }
      case 8:
        if (rng() % 50 == 0) {
          const bool sparse = d.bucket_count() > 4 && d.size() * 100 / d.bucket_count() < 10;
          check(d.shrink_if_sparse() == sparse, "shrink_if_sparse resizes exactly when under 10% full");
          const std::size_t nb = d.bucket_count();
          check(nb <= 4 || d.size() * 100 / nb >= 10, "no shrink left to do");
        }
        break;
      case 9:
        if (rng() % 500 == 0) {
          Map copy(d);
          check_map(copy, model, "copy");
          Map moved(std::move(copy));
          check(copy.empty() && copy.bucket_count() == 0, "moved-from is empty");
          check_map(moved, model, "moved");
          Map assigned;
          assigned = moved;
          check_map(assigned, model, "copy-assigned");
          assigned = std::move(moved);
          check_map(assigned, model, "move-assigned");
          d.swap(assigned);
          check_map(d, model, "swapped");
        }
        break;
      case 10:
        if (rng() % 2000 == 0) {
          d.clear();
          model.clear();
          check(d.bucket_count() == 0, "clear frees the buckets");
        }
        break;
      case 11: {
        // Erase while iterating (the remove-if loop).
        if (rng() % 300 == 0) {
          for (auto it = d.begin(); it != d.end();) {
            const auto pos = it++;
            if (pos->second.back() == '7') {
              model.erase(pos->first);
              d.erase(pos);
            }
          }
        }
        break;
      }
      case 12:
        if (rng() % 1000 == 0) d.reserve(d.size() * 3);
        break;
    }
    if (step % 5000 == 0) check_map(d, model, "step " + std::to_string(step));
  }
  check_map(d, model, "end");
}

void random_set_ops() {
  std::mt19937 rng(777);
  Set d;
  std::unordered_set<std::string> model;
  for (int step = 0; step < 100000; ++step) {
    const std::string k = std::to_string(rng() % ((step / 10000) % 2 == 0 ? 3000 : 100));
    switch (rng() % 4) {
      case 0:
      case 1:
        check(d.insert(k).second == model.insert(k).second, "set insert");
        break;
      case 2:
        check(d.erase(k) == model.erase(k), "set erase");
        break;
      case 3:
        if (!d.empty()) {  // SPOP's pattern: take begin() until empty, now and then
          if (rng() % 2000 == 0) {
            while (!d.empty()) {
              model.erase(*d.begin());
              d.erase(d.begin());
            }
          }
        }
        break;
    }
    if (rng() % 100 == 0) d.shrink_if_sparse();
  }
  check(d.size() == model.size(), "set size");
  std::set<std::string> seen(d.begin(), d.end());
  check(seen == std::set<std::string>(model.begin(), model.end()), "set contents");
}

// The SCAN guarantee: elements present from the first call to the last are all returned, while
// between calls the table grows (inserts) or shrinks (deletes then shrink_if_sparse()).
void scan_guarantee() {
  std::mt19937 rng(99);
  for (int round = 0; round < 400; ++round) {
    Set d;
    const int stable = 1 + static_cast<int>(rng() % 3000);
    for (int i = 0; i < stable; ++i) d.insert("stable:" + std::to_string(i));
    std::vector<std::string> transient;
    int next_transient = 0;
    for (int i = 0; i < static_cast<int>(rng() % 5000); ++i) {
      transient.push_back("t:" + std::to_string(next_transient++));
      d.insert(transient.back());
    }
    if (rng() % 2) d.shrink_if_sparse();
    std::unordered_set<std::string> seen;
    std::uint64_t cursor = 0;
    int calls = 0;
    const int mode = static_cast<int>(rng() % 3);  // 0 grow, 1 shrink, 2 both
    // The table stops growing at `cap` transient elements, so the scan ends.
    const std::size_t cap = rng() % 40000;
    do {
      cursor = d.scan(cursor, [&](const std::string& k) { seen.insert(k); });
      ++calls;
      const bool grow = mode == 0 || (mode == 2 && rng() % 2);
      if (grow) {
        const int n = transient.size() < cap ? static_cast<int>(rng() % 200) : 0;
        for (int i = 0; i < n; ++i) {
          transient.push_back("t:" + std::to_string(next_transient++));
          d.insert(transient.back());
        }
      } else if (!transient.empty()) {
        const std::size_t n = std::min<std::size_t>(transient.size(), rng() % 400);
        for (std::size_t i = 0; i < n; ++i) {
          const std::size_t pick = rng() % transient.size();
          d.erase(transient[pick]);
          transient[pick] = transient.back();
          transient.pop_back();
        }
        d.shrink_if_sparse();
      }
    } while (cursor != 0 && calls < 10000000);
    int missed = 0;
    for (int i = 0; i < stable; ++i) missed += seen.count("stable:" + std::to_string(i)) ? 0 : 1;
    check(missed == 0, "scan round " + std::to_string(round) + " (mode " + std::to_string(mode) + ") missed " +
                           std::to_string(missed) + " of " + std::to_string(stable));
  }
}

// A static table: one scan visits every bucket exactly once, so every element exactly once.
void scan_static_exact() {
  for (int n : {1, 3, 4, 5, 100, 1024, 1025, 70000}) {
    Set d;
    for (int i = 0; i < n; ++i) d.insert(std::to_string(i));
    std::map<std::string, int> seen;
    std::uint64_t cursor = 0;
    std::size_t calls = 0;
    do {
      cursor = d.scan(cursor, [&](const std::string& k) { ++seen[k]; });
      ++calls;
    } while (cursor != 0);
    check(calls == d.bucket_count(), "static scan: one call per bucket for n=" + std::to_string(n));
    bool once = seen.size() == static_cast<std::size_t>(n);
    for (const auto& [k, c] : seen) once = once && c == 1;
    check(once, "static scan returns each element once for n=" + std::to_string(n));
  }
  Set empty;
  check(empty.scan(0, [](const std::string&) {}) == 0, "scan of an empty table ends at once");
  check(empty.scan(12345, [](const std::string&) {}) == 0, "scan of an empty table ignores the cursor");
}

// Every hash in one bucket (long chains), and hashes that differ only in high bits.
struct ConstantHash {
  std::size_t operator()(const std::string&) const { return 42; }
};
struct HighBitsHash {
  std::size_t operator()(const std::string& s) const { return std::hash<std::string>{}(s) << 40; }
};

template <class H>
void degenerate_hashes(const char* what) {
  peadb::DictSet<std::string, H> d;
  std::unordered_set<std::string> model;
  std::mt19937 rng(5);
  for (int i = 0; i < 3000; ++i) {
    const std::string k = std::to_string(rng() % 1500);
    if (rng() % 3) {
      d.insert(k);
      model.insert(k);
    } else {
      d.erase(k);
      model.erase(k);
    }
  }
  check(d.size() == model.size(), std::string(what) + ": size");
  std::set<std::string> seen;
  std::uint64_t cursor = 0;
  do {
    cursor = d.scan(cursor, [&](const std::string& k) { seen.insert(k); });
  } while (cursor != 0);
  check(seen == std::set<std::string>(model.begin(), model.end()), std::string(what) + ": scan contents");
  check(std::set<std::string>(d.begin(), d.end()) == seen, std::string(what) + ": iteration contents");
}

// References and pointers stay valid across rehashes; an iterator kept across an insert that
// rehashes can still be erased (DataStore::rename() does this).
void stability() {
  Map d;
  d["anchor"] = "value";
  std::string* p = &d.find("anchor")->second;
  auto it = d.find("anchor");
  for (int i = 0; i < 100000; ++i) d["k" + std::to_string(i)] = "v";
  check(p == &d.find("anchor")->second && *p == "value", "reference survives growth");
  d.erase(it);
  check(d.find("anchor") == d.end() && d.size() == 100000, "iterator kept across a rehash still erases");
  for (int i = 0; i < 99990; ++i) d.erase("k" + std::to_string(i));
  std::string* q = &d.find("k99995")->second;
  check(d.shrink_if_sparse() && d.bucket_count() == 16, "shrink to the smallest power of two (10 -> 16)");
  check(q == &d.find("k99995")->second, "reference survives a shrink");
  d.clear();
  check(d.shrink_if_sparse() == false && d.bucket_count() == 0, "an empty, cleared table has no buckets");
  d["x"] = "y";
  check(d.bucket_count() == 4, "first insert allocates 4 buckets");
  for (int i = 0; i < 3; ++i) d["y" + std::to_string(i)] = "z";
  check(d.bucket_count() == 4 && d.size() == 4, "4 elements fit 4 buckets");
  d["w"] = "z";
  check(d.bucket_count() == 8, "the fifth element doubles the table");
}

// A value whose copy can fail: a failed copy of the table frees what it built and leaves the
// source as it was.
struct Fragile {
  static int live;
  static int copies_left;
  int v = 0;
  Fragile() { ++live; }
  explicit Fragile(int x) : v(x) { ++live; }
  Fragile(const Fragile& o) : v(o.v) {
    if (copies_left-- == 0) throw std::runtime_error("copy failed");
    ++live;
  }
  ~Fragile() { --live; }
};
int Fragile::live = 0;
int Fragile::copies_left = -1;

void exception_safety() {
  {
    peadb::DictMap<std::string, Fragile> d;
    for (int i = 0; i < 1000; ++i) d.try_emplace(std::to_string(i), i);
    check(Fragile::live == 1000, "1000 live values");
    Fragile::copies_left = 500;
    bool threw = false;
    try {
      peadb::DictMap<std::string, Fragile> copy(d);
    } catch (const std::runtime_error&) {
      threw = true;
    }
    Fragile::copies_left = -1;
    check(threw && Fragile::live == 1000 && d.size() == 1000, "a failed copy frees its partial copy");
  }
  check(Fragile::live == 0, "every value freed with the table");
}

void map_misc() {
  Map d{{"a", "1"}, {"b", "2"}};
  check(d.size() == 2 && d.at("a") == "1", "initializer list and at()");
  bool threw = false;
  try {
    (void)d.at("missing");
  } catch (const std::out_of_range&) {
    threw = true;
  }
  check(threw, "at() of a missing key throws");
  auto r = d.insert_or_assign("a", "9");
  check(!r.second && d["a"] == "9", "insert_or_assign assigns");
  std::vector<std::pair<std::string, std::string>> items(d.begin(), d.end());
  check(items.size() == 2, "range construction of a vector");
  Map from_range(items.begin(), items.end());
  check(from_range.size() == 2 && from_range["b"] == "2", "range constructor");
  const Map& cd = d;
  Map::const_iterator ci = d.find("a");
  check(ci == cd.find("a") && ci != cd.end(), "iterator and const_iterator compare");
  check(d.erase(d.begin(), d.end()) == d.end() && d.empty(), "erase a range");
}


// An erase through an iterator does not look for the next element, so it stays O(1) when the table
// is sparse (a keyspace emptied inside one MULTI, or while a fork child stops the shrink).
void sparse_erase_is_constant_time() {
  Map d;
  for (int i = 0; i < 1000000; ++i) d["k" + std::to_string(i)] = "v";
  for (int i = 0; i < 1000000; ++i) d.erase(d.find("k" + std::to_string(i)));
  check(d.empty() && d.bucket_count() >= (1u << 20), "a sparse table of a million buckets");
  // A key in one of the first buckets, so a walk to the next element would cross the whole table.
  std::string probe;
  for (int i = 0; probe.empty(); ++i) {
    if (d.bucket("p" + std::to_string(i)) < 64) probe = "p" + std::to_string(i);
  }
  const auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < 20000; ++i) {
    d[probe] = "v";
    d.erase(d.find(probe));
  }
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
  // About 1 ms when O(1); walking the empty buckets for each erase takes seconds.
  check(ms < 500, "20,000 insert/erase pairs in a sparse table took " + std::to_string(ms) + " ms");
}

// SipHash-2-4 against the paper's test vectors (key 00..0f; messages of 0 and 15 bytes 00, 01, ...),
// which checks the code that SipHash-1-2 (DictStringHash) shares; and DictStringHash is stable within
// the process and spreads keys over the low bits.
void siphash_vectors() {
  const std::uint64_t k0 = 0x0706050403020100ULL;
  const std::uint64_t k1 = 0x0f0e0d0c0b0a0908ULL;
  unsigned char msg[15];
  for (int i = 0; i < 15; ++i) msg[i] = static_cast<unsigned char>(i);
  check(peadb::siphash<2, 4>(msg, 0, k0, k1) == 0x726fdb47dd0e0e31ULL, "SipHash-2-4 of the empty message");
  check(peadb::siphash<2, 4>(msg, 15, k0, k1) == 0xa129ca6149be45e5ULL, "SipHash-2-4 of 15 bytes");
  const peadb::DictStringHash h;
  check(h("abc") == h(std::string("abc")) && h("abc") != h("abd"), "DictStringHash is stable and differs");
  std::set<std::size_t> low;
  for (int i = 0; i < 4096; ++i) low.insert(h("key:" + std::to_string(i)) & 1023);
  check(low.size() > 900, "4096 keys use most of 1024 buckets (" + std::to_string(low.size()) + ")");
}
}  // namespace

int main() {
  random_map_ops();
  random_set_ops();
  scan_static_exact();
  scan_guarantee();
  degenerate_hashes<ConstantHash>("constant hash");
  degenerate_hashes<HighBitsHash>("high-bits hash");
  stability();
  exception_safety();
  map_misc();
  sparse_erase_is_constant_time();
  siphash_vectors();
  if (failures == 0) std::cout << "dict_test passed\n";
  return failures == 0 ? 0 : 1;
}
