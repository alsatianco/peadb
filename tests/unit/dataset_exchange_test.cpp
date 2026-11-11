// This executable substitutes the allocation-counter object with an ordinary allocator,
// then fails exactly the Nth C++ allocation. Production binaries keep src/zmalloc.cpp.
#include "datastore.hpp"

#include "zmalloc.hpp"

#include <filesystem>
#include <fstream>
#include <unistd.h>

#include <cstdlib>
#include <iostream>
#include <new>
#include <string>
#include <thread>
#include <chrono>
#include <atomic>

namespace {
thread_local long fail_after = -1;
thread_local bool caller = false, forbid_free = false;
thread_local unsigned caller_frees = 0;
std::atomic<unsigned> worker_frees{0};
void release(void* p) noexcept {
  if (p) {
    if (forbid_free) ++caller_frees;
    if (!caller) worker_frees.fetch_add(1, std::memory_order_relaxed);
  }
  std::free(p);
}
void* allocate(std::size_t n) {
  if (fail_after >= 0 && fail_after-- == 0) {
    fail_after = -1;
    throw std::bad_alloc();
  }
  auto* p = std::malloc(n ? n : 1);
  if (!p) throw std::bad_alloc();
  return p;
}
int failures = 0;
void check(bool ok, const char* message) {
  if (!ok) {
    ++failures;
    std::cerr << "FAIL: " << message << '\n';
  }
}
}  // namespace
void* operator new(std::size_t n) {
  return allocate(n);
}
void* operator new[](std::size_t n) {
  return allocate(n);
}
void operator delete(void* p) noexcept {
  release(p);
}
void operator delete[](void* p) noexcept {
  release(p);
}
void operator delete(void* p, std::size_t) noexcept {
  release(p);
}
void operator delete[](void* p, std::size_t) noexcept {
  release(p);
}
void* operator new(std::size_t n, const std::nothrow_t&) noexcept {
  try {
    return allocate(n);
  } catch (...) {
    return nullptr;
  }
}
void* operator new[](std::size_t n, const std::nothrow_t&) noexcept {
  return ::operator new(n, std::nothrow);
}
void* operator new(std::size_t n, std::align_val_t alignment) {
  if (fail_after >= 0 && fail_after-- == 0) {
    fail_after = -1;
    throw std::bad_alloc();
  }
  void* p = nullptr;
  if (posix_memalign(&p, static_cast<std::size_t>(alignment), n ? n : 1)) throw std::bad_alloc();
  return p;
}
void* operator new[](std::size_t n, std::align_val_t a) {
  return ::operator new(n, a);
}
void* operator new(std::size_t n, std::align_val_t a, const std::nothrow_t&) noexcept {
  try {
    return ::operator new(n, a);
  } catch (...) {
    return nullptr;
  }
}
void* operator new[](std::size_t n, std::align_val_t a, const std::nothrow_t&) noexcept {
  return ::operator new(n, a, std::nothrow);
}
void operator delete(void* p, const std::nothrow_t&) noexcept {
  release(p);
}
void operator delete[](void* p, const std::nothrow_t&) noexcept {
  release(p);
}
void operator delete(void* p, std::align_val_t) noexcept {
  release(p);
}
void operator delete[](void* p, std::align_val_t) noexcept {
  release(p);
}
void operator delete(void* p, std::size_t, std::align_val_t) noexcept {
  release(p);
}
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept {
  release(p);
}
void operator delete(void* p, std::align_val_t, const std::nothrow_t&) noexcept {
  release(p);
}
void operator delete[](void* p, std::align_val_t, const std::nothrow_t&) noexcept {
  release(p);
}
namespace peadb {
std::size_t zmalloc_used_memory() {
  return 0;
}
std::size_t zmalloc_string_bytes(const std::string&) {
  return 0;
}
}  // namespace peadb

namespace {
using peadb::DataStore;
using Clock = std::chrono::steady_clock;
void drained(DataStore& owner) {
  const auto deadline = Clock::now() + std::chrono::seconds(15);
  while (owner.lazyfree_pending_objects() && Clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  check(owner.lazyfree_pending_objects() == 0, "owned worker drains pending keys");
}
void commit(DataStore& live, DataStore& staging,
            std::unique_ptr<DataStore::DatasetRetirement> ticket) {
  const auto before = caller_frees;
  fail_after = 0;
  forbid_free = true;
  live.exchange_dataset(staging);
  live.retire_dataset(staging, std::move(ticket));
  const bool no_allocations = fail_after == 0;
  forbid_free = false;
  fail_after = -1;
  check(no_allocations, "exchange and prepared retirement allocate nothing");
  check(caller_frees == before, "exchange and retirement free no caller-owned entries");
}
void prepare_failures() {
  unsigned failed = 0;
  bool done = false;
  // Each iteration owns a fresh, unstarted worker. Sweep ticket/vector AND thread-state
  // allocations. Retry the same owner after each failure: failed thread construction must
  // not leave lazyfree_started_ set with no worker to drain the ticket.
  for (long n = 0; n < 100 && !done; ++n) {
    DataStore live, staging;
    live.set("old", "keep", false, false, {}, false);
    staging.set("new", "ready", false, false, {}, false);
    auto epoch = live.mutation_epoch();
    std::unique_ptr<DataStore::DatasetRetirement> ticket;
    fail_after = n;
    try {
      ticket = live.prepare_dataset_retirement();
    } catch (const std::bad_alloc&) {
      ++failed;
    }
    fail_after = -1;
    check(live.get("old") == "keep" && staging.get("new") == "ready" &&
              live.mutation_epoch() == epoch,
          "preparation success/failure leaves both datasets untouched");
    done = bool(ticket);
    if (!ticket) ticket = live.prepare_dataset_retirement();
    commit(live, staging, std::move(ticket));
    check(live.get("new") == "ready" && !live.get("old"), "publish after retry");
    check(staging.dbsize() == 0, "retired staging remains usable");
    staging.set("reuse", "ok", false, false, {}, false);
    check(staging.get("reuse") == "ok", "detached database reuse");
    drained(live);
  }
  check(done && failed >= 5, "complete preparation allocation-failure sweep");
}
void payloads_and_runtime() {
  DataStore live, staging;
  auto* identity = &live;
  unsigned hooks = 0;
  live.set_lazy_expire_hook([&](int, const std::string&) { ++hooks; });
  live.set_replica_expiry(true);
  live.set_writable_replica(true);
  live.set_lfu_log_factor(73);
  staging.set_lfu_log_factor(19);
  live.encoding_limits().hash_entries = 7;
  live.record_hit();
  live.record_miss();
  live.record_evicted();
  live.record_expired();
  for (int db = 0; db < 16; ++db) {
    live.select_db(db);
    staging.select_db(db);
    live.set("old", "old", false, false, {}, false);
    bool wrong = false;
    staging.set("str", std::string("v\0x", 3), false, false, 9007199254740993LL, false);
    staging.hset("hash", {{"f", "v"}}, wrong);
    staging.rpush("list", {"a", "b"}, wrong);
    staging.sadd("set", {"a"}, wrong);
    staging.zadd_one("zset", 1.25, "m", false, false, false, false, false);
    DataStore::StreamDump stream;
    stream.entries = {{"1-0", {{"f", "v"}}}};
    stream.last_ms = 1;
    stream.entries_added = 1;
    staging.restore_stream("stream", stream);
  }
  live.select_db(3);
  staging.select_db(8);
  live.set("local-ttl", "mine", false, false, DataStore::now_ms() + 60000, false);
  check(live.replica_expires_tracked_keys() == 1, "old local TTL owned");
  const auto le = live.mutation_epoch(), se = staging.mutation_epoch();
  fail_after = 0;
  live.exchange_dataset(staging);
  const bool no_allocations = fail_after == 0;
  fail_after = -1;
  check(no_allocations && &live == identity, "stable allocation-free exchange");
  check(live.current_db() == 3 && staging.current_db() == 8, "selected DBs stay local");
  check(live.mutation_epoch() == le + 1 && staging.mutation_epoch() == se + 1,
        "exchange advances each local mutation epoch");
  check(live.replica_expiry() && !staging.replica_expiry() && live.lfu_log_factor() == 73 &&
            staging.lfu_log_factor() == 19 && live.encoding_limits().hash_entries == 7,
        "replica/encoding/eviction policy stays local");
  check(live.stat_keyspace_hits() == 1 && live.stat_keyspace_misses() == 1 &&
            live.stat_evicted_keys() == 1 && live.stat_expired_keys() == 1,
        "statistics stay local");
  check(live.replica_expires_tracked_keys() == 0 && staging.replica_expires_tracked_keys() == 1,
        "local TTL ownership follows its dataset");
  for (int db = 0; db < 16; ++db) {
    live.select_db(db);
    staging.select_db(db);
    bool wrong = false;
    check(live.get("str") == std::string("v\0x", 3) && live.pttl("str") > 1000000,
          "binary string and absolute TTL in every DB");
    check(live.hget("hash", "f", wrong) == "v" && !wrong, "hash payload");
    check(live.lrange("list", 0, -1, wrong) == std::vector<std::string>({"a", "b"}),
          "list payload");
    check(live.smembers("set", wrong) == std::vector<std::string>({"a"}), "set payload");
    check(live.zrange("zset", 0, -1, false, wrong) ==
              std::vector<std::pair<std::string, double>>({{"m", 1.25}}),
          "sorted-set payload");
    check(live.xlen("stream", wrong) == 1 && !wrong, "stream payload");
    check(staging.get("old") == "old" && !live.get("old"), "old dataset retained before retire");
  }
  live.select_db(0);
  live.set_replica_expiry(false);
  live.set("expired", "v", false, false, 1, false);
  live.get("expired");
  check(hooks == 1, "live expiry hook survives exchange");
  auto epoch = live.mutation_epoch();
  live.exchange_dataset(live);
  check(live.mutation_epoch() == epoch && live.dbsize() == 6, "self-exchange is a no-op");
}
void hidden_expiry() {
  DataStore live, staging;
  live.set_expire_paused(true);
  live.select_db(15);
  live.set("paused", "keep", false, false, 1, false);
  check(!live.get("paused") && live.dbsize() == 1, "paused key hidden");
  staging.select_db(1);
  staging.set_replica_expiry(true);
  staging.set("replica-hidden", "keep", false, false, 1, false);
  check(!staging.get("replica-hidden") && staging.dbsize() == 1, "replica key hidden");
  live.exchange_dataset(staging);
  live.select_db(1);
  staging.select_db(15);
  live.set_replica_expiry(true);
  staging.set_expire_paused(true);
  {
    DataStore::LazyExpireDisabled left(live), right(staging);
    check(live.get("replica-hidden") == "keep" && staging.get("paused") == "keep",
          "all hidden-map ownership follows data, restored under each object's guards");
  }
  auto ticket = live.prepare_dataset_retirement();
  live.retire_dataset(staging, std::move(ticket));
  check(staging.dbsize() == 0, "retirement clears hidden maps as well as physical maps");
  drained(live);
}
void aborted_staging() {
  DataStore live, staging;
  live.set("old", "still-live", false, false, {}, false);
  auto ticket = live.prepare_dataset_retirement();
  staging.set_replica_expiry(true);
  staging.set_writable_replica(true);
  staging.select_db(15);
  staging.set("partial", "unpublished", false, false, DataStore::now_ms() + 60000, false);
  const auto epoch = live.mutation_epoch();
  fail_after = 0;
  forbid_free = true;
  const auto frees = caller_frees;
  live.retire_dataset(staging, std::move(ticket));
  const bool no_allocations = fail_after == 0;
  fail_after = -1;
  forbid_free = false;
  check(no_allocations && caller_frees == frees, "aborted staging retirement is allocation-free");
  check(live.get("old") == "still-live" && live.mutation_epoch() == epoch,
        "aborting a partial stage preserves live data and epoch");
  check(staging.dbsize() == 0 && staging.replica_expires_tracked_keys() == 0,
        "aborting stage detaches keys and TTL ownership");
  drained(live);
  // An unused prepared ticket neither retires data nor joins/owns a second worker.
  auto unused = live.prepare_dataset_retirement();
  unused.reset();
  check(live.get("old") == "still-live", "unused ticket destruction preserves data");
}
void large_retirement_and_shutdown() {
  const auto before = worker_frees.load();
  auto live = std::make_unique<DataStore>();
  DataStore staging, second;
  // Preallocate both tickets before changing either dataset. Queue multiple retirements
  // alongside the ordinary FLUSH/UNLINK queue, then immediately destroy the owner to
  // exercise its real worker drain/join rather than relying on PID or fixed sleeps.
  auto one = live->prepare_dataset_retirement();
  auto two = live->prepare_dataset_retirement();
  for (int i = 0; i < 100000; ++i) {
    staging.select_db(i % 16);
    staging.set("key:" + std::to_string(i), std::string(64, 'v'), false, false, {}, false);
  }
  second.set_replica_expiry(true);
  second.set_writable_replica(true);
  second.set("local", "v", false, false, DataStore::now_ms() + 60000, false);
  live->set("ordinary", "flush", false, false, {}, false);
  live->flushall_async();
  const auto start = Clock::now();
  fail_after = 0;
  forbid_free = true;
  const auto frees = caller_frees;
  live->retire_dataset(staging, std::move(one));
  live->retire_dataset(second, std::move(two));
  const bool no_allocations = fail_after == 0;
  const bool no_frees = caller_frees == frees;
  fail_after = -1;
  forbid_free = false;
  const auto elapsed = Clock::now() - start;
  check(no_allocations && no_frees, "large retirements have no inline allocation/destruction");
  check(elapsed < std::chrono::seconds(1), "100k-key retirement submission is bounded");
  check(second.replica_expires_tracked_keys() == 0, "retire local TTL tracker without a key walk");
  for (int db = 0; db < 16; ++db) {
    staging.select_db(db);
    check(staging.dbsize() == 0, "all retired DBs remain reusable");
  }
  live.reset();
  check(worker_frees.load() > before + 100000,
        "owner shutdown joins worker after actual data frees");
}
}  // namespace
int main() {
  caller = true;
  prepare_failures();
  payloads_and_runtime();
  hidden_expiry();
  aborted_staging();
  large_retirement_and_shutdown();
  std::cout << "dataset exchange failures=" << failures << '\n';
  return failures ? 1 : 0;
}
