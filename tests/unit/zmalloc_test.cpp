// The allocation counter behind used_memory and maxmemory (include/zmalloc.hpp, compat/delta.md
// item 38): every C++ allocation adds the bytes the allocator gave it and every free takes the same
// bytes away, so the count goes up by at least what was asked for and comes back to where it was
// once everything is freed, whatever container, operator form or thread did the work.

#include "dict.hpp"
#include "zmalloc.hpp"

#include <cstdint>
#include <deque>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <new>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {
int failures = 0;
void check(bool ok, const std::string& what) {
  if (!ok) {
    std::cerr << "FAIL: " << what << "\n";
    ++failures;
  }
}

std::int64_t used() { return static_cast<std::int64_t>(peadb::zmalloc_used_memory()); }

struct alignas(64) Aligned {
  char bytes[200];
};

// Builds and frees one of each kind of allocation; returns the growth seen while they were alive.
// `alone`: no other thread allocates meanwhile, so the count must come back exactly.
std::int64_t churn(bool alone) {
  const std::int64_t before = used();
  std::int64_t grown = 0;
  {
    std::vector<std::string> strings;
    for (int i = 0; i < 1000; ++i) strings.emplace_back(100 + i % 50, 'x');
    std::string grow;
    for (int i = 0; i < 5000; ++i) grow += "abcdefghij";  // reallocations inside the C++ library
    std::map<std::string, std::string> tree;
    std::unordered_map<std::string, int> hashed;
    std::deque<std::string> dq;
    std::list<int> lst;
    peadb::Dict<std::string, std::string> dict;
    for (int i = 0; i < 2000; ++i) {
      const std::string key = "key:" + std::to_string(i) + std::string(30, 'k');
      tree[key] = key;
      hashed[key] = i;
      dq.push_back(key);
      lst.push_back(i);
      dict.emplace(key, key);
    }
    for (int i = 0; i < 1000; ++i) dict.erase(dict.find("key:" + std::to_string(i) + std::string(30, 'k')));
    dict.shrink_if_sparse();
    auto* array = new int[1000];
    auto* aligned = new Aligned;
    auto* aligned_array = new Aligned[10];
    check(reinterpret_cast<std::uintptr_t>(aligned) % 64 == 0, "aligned new honours alignment");
    auto* nothrow = new (std::nothrow) char[4096];
    check(nothrow != nullptr, "nothrow new returns memory");
    auto unique = std::make_unique<std::vector<int>>(10000);
    auto shared = std::make_shared<std::string>(5000, 'y');
    grown = used() - before;
    delete[] array;
    delete aligned;
    delete[] aligned_array;
    delete[] nothrow;
  }
  if (alone) {
    check(used() == before, "the count is back where it started after a mixed churn (" +
                                std::to_string(used() - before) + " bytes off)");
  }
  return grown;
}

}  // namespace

int main() {
  // Growth is at least what was asked for.
  const std::int64_t grown = churn(true);
  check(grown > 1000 * 100 + 5000 * 10 + 2000 * 3 * 34 + 1000 * 4 + 4096 + 10000 * 4,
        "the count grows by at least the requested bytes, grew " + std::to_string(grown));

  // One large allocation: the count follows it closely (the allocator adds little to 10 MB).
  {
    const std::int64_t before = used();
    auto big = std::make_unique<std::string>(10 * 1024 * 1024, 'z');
    const std::int64_t delta = used() - before;
    check(delta >= 10 * 1024 * 1024 && delta <= 10 * 1024 * 1024 + 64 * 1024,
          "a 10 MB string counts about 10 MB, counted " + std::to_string(delta));
    big.reset();
    check(used() == before, "freeing the 10 MB string gives it back");
  }

  // zmalloc_string_bytes() is what freeing a string gives back, also for a buffer the allocator made
  // bigger than asked (macOS can hand back a larger freed region as a string doubles), and 0 for
  // text kept inside the object.
  {
    std::string small = "short";
    check(peadb::zmalloc_string_bytes(small) == 0, "a short string holds no heap buffer");
    for (std::size_t target : {100000ul, 3000000ul, 5000000ul, 9000000ul, 17000000ul}) {
      std::string s;
      while (s.size() < target) s.append(std::string(57, 'a'));
      const std::int64_t bytes = static_cast<std::int64_t>(peadb::zmalloc_string_bytes(s));
      check(bytes >= static_cast<std::int64_t>(s.capacity()), "a string holds at least its capacity");
      const std::int64_t before = used();
      std::string().swap(s);
      check(before - used() == bytes, "freeing a string of " + std::to_string(target) +
                                          " bytes gives back zmalloc_string_bytes() (" +
                                          std::to_string(before - used()) + " vs " +
                                          std::to_string(bytes) + ")");
    }
  }

  // Memory freed on another thread than the one that allocated it, both ways (the lazyfree thread
  // frees what the main thread built; std::thread itself hands its state across).
  {
    const std::int64_t before = used();
    auto data = std::make_unique<std::vector<std::string>>();
    for (int i = 0; i < 1000; ++i) data->emplace_back(200, 'a');
    std::thread freer([&data] { data.reset(); });
    freer.join();
    check(used() == before, "memory allocated on the main thread and freed on another");

    std::unique_ptr<std::vector<std::string>> made;
    std::thread maker([&made] {
      made = std::make_unique<std::vector<std::string>>();
      for (int i = 0; i < 1000; ++i) made->emplace_back(200, 'b');
    });
    maker.join();
    check(used() > before + 1000 * 200, "memory allocated on another thread is counted");
    made.reset();
    check(used() == before, "memory allocated on another thread and freed on the main thread");

    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
      threads.emplace_back([] {
        for (int round = 0; round < 200; ++round) churn(false);
      });
    }
    for (int round = 0; round < 200; ++round) churn(false);
    for (auto& th : threads) th.join();
    threads.clear();
    threads.shrink_to_fit();
    check(used() == before, "the count is back after five threads churned at once (" +
                                std::to_string(used() - before) + " bytes off)");
  }

  if (failures == 0) std::cout << "zmalloc_test: all checks passed\n";
  return failures == 0 ? 0 : 1;
}
