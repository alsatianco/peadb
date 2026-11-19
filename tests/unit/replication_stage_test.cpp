// This executable substitutes the allocation-counter object with an ordinary allocator,
// then fails exactly the Nth C++ allocation. Production binaries keep src/zmalloc.cpp.
#include "replication_stage.hpp"
#include "lua_engine.hpp"

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
#include <cstring>
#include <stdexcept>

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
using namespace peadb;
using Clock = std::chrono::steady_clock;
const ReplicationLoadIdentity identity{7, 3};
const std::string old_code =
    "#!lua name=old\nlocal n=0;redis.register_function('old',function() n=n+1;return n end)";
const std::string new_code =
    "#!lua name=new\nredis.register_function('new',function() return 99 end)";
void string(std::string& out, const std::string& value) {
  if (value.size() < 64)
    out.push_back(static_cast<char>(value.size()));
  else if (value.size() < 16384) {
    out.push_back(static_cast<char>(0x40 | (value.size() >> 8)));
    out.push_back(static_cast<char>(value.size()));
  } else {
    out.push_back(char(0x80));
    for (int shift = 24; shift >= 0; shift -= 8) out.push_back(char(value.size() >> shift));
  }
  out += value;
}
std::string snapshot(const std::string& code = new_code) {
  std::string out = "REDIS0011";
  out += char(0xfa);
  string(out, "repl-stream-db");
  string(out, "15");
  out += char(245);
  string(out, code);
  out += char(0);
  string(out, "key");
  string(out, "new-value");
  out += char(0xfe);
  out += char(15);
  out += char(0xfc);
  constexpr std::uint64_t expiry = 4102444800000ULL;
  for (unsigned i = 0; i < 8; ++i) out += char(expiry >> (8 * i));
  out += char(0);
  string(out, "ttl");
  string(out, "future");
  out += char(0xff);
  out.append(8, '\0');
  return out;
}
RdbReadFn reader(const std::string& bytes, std::size_t& pos, std::size_t fragment = 11) {
  return [&bytes, &pos, fragment](void* buffer, std::size_t size) -> ssize_t {
    const auto count = std::min({size, fragment, bytes.size() - pos});
    std::memcpy(buffer, bytes.data() + pos, count);
    pos += count;
    return static_cast<ssize_t>(count);
  };
}
std::string invoke(const FunctionRegistry& functions, const char* name) {
  SessionState session;
  return lua_engine_fcall(functions.libraries().at(name)->functions.front().callback_ref, {}, {},
                          session, true);
}
void seed(DataStore& live, FunctionRegistry& functions) {
  live.set_replica_expiry(true);
  live.set_writable_replica(true);
  live.select_db(15);
  live.set("old", "old-value", false, false, DataStore::now_ms() + 60000, false);
  live.select_db(0);
  std::string name, err;
  check(functions.load(old_code, false, name, err), "old captured callback installed");
}
void drain(DataStore& live, FunctionRetirementQueue& functions) {
  while (functions.pending()) check(functions.drain(7) <= 7, "owner-thread function work budget");
  const auto deadline = Clock::now() + std::chrono::seconds(15);
  while (live.lazyfree_pending_objects() && Clock::now() < deadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  check(live.lazyfree_pending_objects() == 0, "owned datastore worker drains keys");
}
bool publish(ReplicationLoadStage& stage, ReplicationLoadIdentity current) {
  const auto before = caller_frees;
  fail_after = 0;
  forbid_free = true;
  const bool result = stage.publish(current);
  const bool no_allocations = fail_after == 0;
  const bool no_frees = caller_frees == before;
  forbid_free = false;
  fail_after = -1;
  check(no_allocations && no_frees, "paired exchange/discard allocates and frees nothing");
  return result;
}
void preparation_failures() {
  DataStore live;
  FunctionRegistry functions;
  FunctionRetirementQueue queue;
  seed(live, functions);
  const auto closure = functions.libraries().at("old").get();
  const auto epoch = live.mutation_epoch();
  bool completed = false;
  int refused = 0;
  for (int allocation = 0; allocation < 128 && !completed; ++allocation) {
    try {
      fail_after = allocation;
      ReplicationLoadStage stage(live, functions, queue, identity);
      fail_after = -1;
      completed = true;
    } catch (const std::bad_alloc&) {
      fail_after = -1;
      ++refused;
    }
    check(functions.libraries().at("old").get() == closure && live.mutation_epoch() == epoch,
          "every preparation failure preserves paired live identity/epoch");
    check(live.current_db() == 0 && live.replica_expires_tracked_keys() == 1,
          "preparation preserves selection/local TTL ownership");
    drain(live, queue);
  }
  check(completed && refused >= 7, "sweep covers both retirement tickets and stage construction");
  check(invoke(functions, "old") == ":1\r\n", "preparation retains captured callback state");
}
void successful_pair(bool eof) {
  DataStore live;
  FunctionRegistry functions;
  FunctionRetirementQueue queue;
  seed(live, functions);
  std::weak_ptr<const FunctionLibrary> old = functions.libraries().at("old");
  auto bytes = snapshot();
  RdbStreamFrame frame;
  if (eof) {
    frame.kind = RdbStreamFrame::Kind::Eof;
    frame.eof_marker.assign(40, 'z');
  } else
    frame.length = bytes.size();
  const std::string commands = "*1\r\n$4\r\nPING\r\n";
  if (eof) bytes += frame.eof_marker + commands;
  std::size_t pos = 0;
  std::string err;
  ReplicationLoadStage stage(live, functions, queue, identity);
  check(!stage.take_result(), "commands cannot escape before decode");
  check(stage.load(reader(bytes, pos, eof ? 65536 : 11), frame, err), "framed snapshot validates");
  check(stage.stats().loaded == 2 && stage.replication().stream_db == 15,
        "stage owns complete counters and prospective AUX");
  check(!stage.take_result(), "validated command suffix stays private before paired exchange");
  live.select_db(15);
  check(live.get("old") == "old-value" && invoke(functions, "old") == ":1\r\n",
        "old data and callbacks remain live through validation");
  check(publish(stage, identity), "current validated stage exchanges both owners");
  check(live.current_db() == 15 && !live.get("old") && live.get("ttl") == "future",
        "all-database exchange preserves live selected database");
  check(live.pexpiretime("ttl") == 4102444800000LL && live.replica_expires_tracked_keys() == 0,
        "absolute TTL transfers without replica-local TTL ownership");
  live.select_db(0);
  check(live.get("key") == "new-value" && functions.libraries().size() == 1 &&
            invoke(functions, "new") == ":99\r\n",
        "keys and functions publish together");
  check(!old.expired() && queue.pending() == 1, "old callbacks remain queued on owner thread");
  auto result = stage.take_result();
  check(result && result->read_ahead == (eof ? commands : ""),
        "exact once-only command suffix release");
  check(!stage.take_result() && !publish(stage, identity), "single-use completion cannot replay");
  drain(live, queue);
  check(old.expired(), "old Lua callbacks retired before engine shutdown");
}
void rejected_loads() {
  // Each failure happens after at least one library has compiled where possible.
  for (int scenario = 0; scenario < 8; ++scenario) {
    DataStore live;
    FunctionRegistry functions;
    FunctionRetirementQueue queue;
    seed(live, functions);
    const auto closure = functions.libraries().at("old").get();
    const auto epoch = live.mutation_epoch();
    auto bytes = snapshot();
    RdbStreamFrame frame;
    frame.length = bytes.size();
    if (scenario == 0) bytes.back() = 1;  // nonzero invalid CRC
    if (scenario == 1) {
      bytes = snapshot(new_code + "\nthis is invalid lua");
      frame.length = bytes.size();
    }
    if (scenario == 2) bytes.resize(bytes.size() - 3);
    if (scenario == 3) {
      frame.kind = RdbStreamFrame::Kind::Eof;
      frame.eof_marker.assign(40, 'z');
      bytes.append(40, 'x');
    }
    std::size_t pos = 0;
    std::string err;
    {
      ReplicationLoadStage stage(live, functions, queue, identity);
      RdbLoadControl control;
      if (scenario == 4 || scenario == 5)
        control.checkpoint = [&](const RdbLoadProgress& p) {
          if (p.keys) {
            if (scenario == 5) throw std::runtime_error("owned checkpoint");
            return false;
          }
          return true;
        };
      bool loaded = false;
      try {
        loaded = stage.load(reader(bytes, pos), frame, err, &control);
      } catch (const std::runtime_error& error) {
        check(scenario == 5 && std::string(error.what()) == "owned checkpoint",
              "callback exception propagates");
      }
      check(loaded == (scenario >= 6), "only valid identity-rejection cases decode successfully");
      const auto current = scenario == 6   ? ReplicationLoadIdentity{8, 3}
                           : scenario == 7 ? ReplicationLoadIdentity{7, 4}
                                           : identity;
      check(!publish(stage, current), "failed or stale generation/attempt cannot publish");
      check(!stage.take_result(), "failed/stale commands remain unavailable");
    }
    check(functions.libraries().at("old").get() == closure && live.mutation_epoch() == epoch,
          "CRC/Lua/truncation/trailer/cancel/exception/stale abort preserves paired live state");
    live.select_db(15);
    check(live.get("old") == "old-value" && live.replica_expires_tracked_keys() == 1 &&
              invoke(functions, "old") == ":1\r\n",
          "abort retains old value/TTL/captured state");
    check(queue.pending() == 1, "discard owns failed stage callback retirement");
    drain(live, queue);
  }
}
void decode_allocation_failures() {
  DataStore live;
  FunctionRegistry functions;
  FunctionRetirementQueue queue;
  seed(live, functions);
  const auto closure = functions.libraries().at("old").get();
  const auto bytes = snapshot();
  RdbStreamFrame frame;
  frame.length = bytes.size();
  bool completed = false;
  int refused = 0;
  for (int allocation = 0; allocation < 512 && !completed; ++allocation) {
    std::size_t pos = 0;
    std::string err;
    {
      ReplicationLoadStage stage(live, functions, queue, identity);
      auto input = reader(bytes, pos);
      try {
        fail_after = allocation;
        completed = stage.load(input, frame, err);
        fail_after = -1;
        if (!completed) ++refused;
      } catch (const std::bad_alloc&) {
        fail_after = -1;
        ++refused;
      }
      // Abandon successful as well as partial stages, exercising destructor cleanup.
    }
    check(functions.libraries().at("old").get() == closure && live.current_db() == 0,
          "all decoder allocation failures leave live owners untouched");
    drain(live, queue);
  }
  check(completed && refused > 10, "decode allocation sweep reaches successful full frame");
}
void large_discard_and_shutdown() {
  DataStore live;
  FunctionRegistry functions;
  FunctionRetirementQueue queue;
  seed(live, functions);
  std::string code = "#!lua name=large\n";
  for (int i = 0; i < 2048; ++i)
    code += "redis.register_function('f" + std::to_string(i) + "',function() return 1 end)\n";
  auto bytes = snapshot(code);
  bytes.resize(bytes.size() - 9);  // replace EOF/checksum with more complete key records
  for (int i = 0; i < 20000; ++i) {
    bytes += char(0);
    string(bytes, "key:" + std::to_string(i));
    string(bytes, std::string(64, 'v'));
  }
  bytes += char(0xff);
  bytes.append(8, '\0');
  RdbStreamFrame frame;
  frame.length = bytes.size();
  std::size_t pos = 0;
  std::string err;
  {
    ReplicationLoadStage stage(live, functions, queue, identity);
    check(stage.load(reader(bytes, pos, 65536), frame, err), "large paired stage fully decoded");
    const auto before = caller_frees;
    fail_after = 0;
    forbid_free = true;
    stage.discard();
    stage.discard();
    const bool no_allocations = fail_after == 0, no_frees = caller_frees == before;
    fail_after = -1;
    forbid_free = false;
    check(no_allocations && no_frees,
          "large paired discard is allocation/free-free and idempotent");
    check(!stage.take_result() && !publish(stage, identity), "discarded stage never republishes");
  }
  check(queue.pending() == 1 && queue.drain(1) == 1 && queue.pending() == 1,
        "large registry is not synchronously destroyed by stage teardown");
  // Queue/worker destructors below own partially drained shutdown, before Lua shutdown.
  check(invoke(functions, "old") == ":1\r\n", "large abort keeps original closure state");
}
void exception_unwind() {
  DataStore live;
  FunctionRegistry functions;
  FunctionRetirementQueue queue;
  seed(live, functions);
  const auto bytes = snapshot();
  RdbStreamFrame frame;
  frame.length = bytes.size();
  std::size_t pos = 0;
  std::string err;
  try {
    ReplicationLoadStage stage(live, functions, queue, identity);
    check(stage.load(reader(bytes, pos), frame, err), "unwind stage fully loaded");
    throw std::runtime_error("abandon validated stage");
  } catch (const std::runtime_error&) {
  }
  check(queue.pending() == 1 && functions.libraries().count("old") == 1,
        "exception unwinding queues paired stage without replacing live callbacks");
  drain(live, queue);
}
}  // namespace
int main() {
  caller = true;
  peadb::lua_engine_init();
  preparation_failures();
  successful_pair(false);
  successful_pair(true);
  rejected_loads();
  decode_allocation_failures();
  exception_unwind();
  large_discard_and_shutdown();
  peadb::lua_engine_shutdown();
  std::cout << "replication stage failures=" << failures << '\n';
  return failures ? 1 : 0;
}
