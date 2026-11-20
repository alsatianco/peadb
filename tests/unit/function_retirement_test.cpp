// Fail C++ allocation and observe C++ destruction without changing production allocation.
#include "functions.hpp"
#include "lua_engine.hpp"

#include "zmalloc.hpp"

#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <exception>
#include <iostream>
#include <new>
#include <string>
#include <thread>

namespace {
thread_local long fail_after = -1;
thread_local bool forbid_free = false;
thread_local unsigned caller_frees = 0;
void release(void* p) noexcept {
  if (p) {
    if (forbid_free) ++caller_frees;
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
const std::string old_code =
    "#!lua name=old\nlocal n=0;redis.register_function{function_name='count',"
    "callback=function() n=n+1;return n end,flags={'no-writes'}}";
const std::string new_code =
    "#!lua name=new\nredis.register_function('fresh',function() return 99 end)";
std::string invoke(const peadb::FunctionRegistry& registry, const std::string& library) {
  peadb::SessionState session;
  return peadb::lua_engine_fcall(registry.libraries().at(library)->functions[0].callback_ref, {},
                                 {}, session, true);
}
void submit(peadb::FunctionRetirementQueue& queue, peadb::FunctionRegistry& registry,
            std::unique_ptr<peadb::FunctionRetirementQueue::Ticket> ticket) {
  const auto before = caller_frees;
  fail_after = 0;
  forbid_free = true;
  queue.retire(registry, std::move(ticket));
  const bool no_allocation = fail_after == 0;
  forbid_free = false;
  fail_after = -1;
  const auto frees = caller_frees - before;
  check(no_allocation && frees == 0, "submission allocates/frees nothing");
  check(registry.libraries().empty() && registry.index().empty(), "submission empties source");
}
std::size_t step(peadb::FunctionRetirementQueue& queue, std::size_t budget) {
  fail_after = 0;
  const auto used = queue.drain(budget);
  const bool no_allocation = fail_after == 0;
  fail_after = -1;
  check(no_allocation && used <= budget, "drain obeys budget without C++ allocation");
  return used;
}
}  // namespace

int main() {
  using namespace peadb;
  // Programming errors fail before reaching Lua or mutating any detached registry.
  for (int misuse = 0; misuse < 4; ++misuse) {
    const auto pid = fork();
    check(pid >= 0, "owned misuse child forked");
    if (pid == 0) {
      alarm(5);
      std::set_terminate([] { std::_Exit(73); });
      FunctionRetirementQueue queue, other;
      FunctionRegistry candidate;
      if (misuse == 0) queue.retire(candidate, other.prepare());
      if (misuse == 1) queue.retire(candidate, {});
      if (misuse == 2) {
        std::thread worker([&] { queue.drain(1); });
        worker.join();
      }
      if (misuse == 3) {
        std::thread worker([&] { queue.prepare(); });
        worker.join();
      }
      std::_Exit(74);
    }
    if (pid > 0) {
      int status = 0;
      check(waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 73,
            "wrong-owner/null/worker misuse terminates before mutation");
    }
  }
  lua_engine_init();
  {
    // Exercise the server service entry point, not only the queue primitive. A large
    // single library must span turns, and neither Lua nor an unguarded decoder may
    // retire callbacks while its temporary references are live.
    FunctionRetirementQueue service;
    FunctionRegistry detached;
    std::string code = "#!lua name=service\n", name, err;
    for (int i = 0; i < 257; ++i)
      code += "redis.register_function('s" + std::to_string(i) + "',function() return 1 end)\n";
    check(detached.load(code, false, name, err), "server service fixture installed");
    std::weak_ptr<const FunctionLibrary> retired = detached.libraries().at("service");
    submit(service, detached, service.prepare());
    lua_engine_set_dispatch([&](const auto&, SessionState&, bool&) {
      return ":" + std::to_string(service_replica_function_retirement(service)) + "\r\n";
    });
    SessionState session;
    check(lua_engine_eval("return redis.call('PING')", {}, {}, session) == ":0\r\n",
          "real Lua dispatch cannot retire callbacks");
    g_client_list_callback = [&] {
      check(service_replica_function_retirement(service) == 0,
            "nested command dispatch cannot retire callbacks");
      return std::string();
    };
    bool close = false;
    check(handle_command({"CLIENT", "LIST"}, session, close) == "$0\r\n\r\n",
          "nested command fixture dispatched");
    g_client_list_callback = nullptr;
    std::size_t units = 0;
    for (const auto mode : {LoadingMode::Blocking, LoadingMode::Async}) {
      const LoadingScope loading(mode);
      check(service_replica_function_retirement(service) == 0,
            "unguarded decoder context cannot drain");
      service_loading_events([&] { units += service_replica_function_retirement(service); });
      check(service.pending() == 1 && !retired.expired(), "loading pump retains partial work");
    }
    check(units == 128, "each guarded loading pump drains exactly 64 work units");
    unsigned turns = 0;
    while (service.pending()) {
      fail_after = 0;
      const auto used = service_replica_function_retirement(service);
      const bool no_allocation = fail_after == 0;
      fail_after = -1;
      check(no_allocation && used > 0 && used <= 64,
            "normal turn has bounded allocation-free work");
      units += used;
      ++turns;
    }
    check(turns == 7 && units == 516 && retired.expired(),
          "normal turns finish every index/callback/library/ticket exactly once");
    check(service_replica_function_retirement(service) == 0, "empty normal service is harmless");
    lua_engine_set_dispatch({});
  }
  {
    std::string name, err;
    FunctionRegistry live;
    check(live.load(old_code, false, name, err), "old closure installed");
    check(invoke(live, "old") == ":1\r\n", "closure initialized");
    const auto identity = live.libraries().at("old");
    FunctionRetirementQueue queue;
    check(step(queue, 0) == 0 && step(queue, 100) == 0, "empty drain does no work");

    {
      FunctionRegistry duplicate;
      check(!duplicate.load("#!lua name=duplicate\nredis.register_function('same',function() end);"
                            "redis.register_function('same',function() end)",
                            false, name, err) &&
                err.starts_with("Error registering functions: ") &&
                err.ends_with("Function already exists in the library"),
            "per-load name index preserves duplicate error");
      check(duplicate.libraries().empty() && duplicate.index().empty(),
            "duplicate registration publishes nothing");
      check(duplicate.load(new_code, false, name, err), "name index resets after duplicate");
    }
    // Fail preparation of both entries in one name index, including a vector insertion
    // after the index insertion succeeded. Every reference and temporary index must unwind.
    const std::string two_functions =
        "#!lua name=two\nredis.register_function('first',function() end);"
        "redis.register_function('second',function() end)";
    bool prepared = false;
    unsigned name_failures = 0;
    for (long n = 0; n < 2000; ++n) {
      FunctionRegistry candidate;
      bool loaded = false, threw = false;
      fail_after = n;
      try {
        loaded = candidate.load(two_functions, false, name, err);
      } catch (const std::bad_alloc&) {
        threw = true;
      }
      fail_after = -1;
      if (threw) {
        ++name_failures;
        check(candidate.libraries().empty() && candidate.index().empty(),
              "name-index allocation failure leaves private registry empty");
        check(live.libraries().at("old") == identity, "name-index OOM preserves live callbacks");
        check(candidate.load(new_code, false, name, err), "engine reusable after name-index OOM");
      } else {
        check(loaded, "name-index allocation sweep completes successfully");
        prepared = loaded;
        break;
      }
    }
    check(prepared && name_failures > 0, "every name-index allocation exercised");

    // Sweep preparation before publication. Failure neither touches callbacks nor leaves
    // queue ownership behind; retry succeeds on the exact same objects.
    unsigned caught = 0;
    bool complete = false;
    for (long n = 0; n < 20; ++n) {
      std::unique_ptr<FunctionRetirementQueue::Ticket> ticket;
      fail_after = n;
      try {
        ticket = queue.prepare();
      } catch (const std::bad_alloc&) {
        ++caught;
      }
      fail_after = -1;
      check(live.libraries().at("old") == identity && queue.pending() == 0,
            "preparation failure preserves registry and queue");
      if (ticket) {
        complete = true;
        break;  // unused preparation destroys an empty registry
      }
    }
    check(complete && caught > 0, "all preparation allocations swept through success");
    check(invoke(live, "old") == ":2\r\n", "preparation retains closure state");

    // A retired copy must not mutate a still-live library, even when names collide with
    // another private candidate. Two queued owners also release the same library once.
    {
      FunctionRegistry copy(live), copy2(live);
      submit(queue, copy, queue.prepare());
      submit(queue, copy2, queue.prepare());
      check(queue.pending() == 2 && step(queue, 0) == 0, "multiple pending tickets");
      check(step(queue, 100) == 6 && queue.pending() == 0,
            "shared library uses index/owner/ticket steps without callback mutation");
      check(identity->functions.size() == 1 && invoke(live, "old") == ":3\r\n",
            "shared callback identity and captured state survive retirement");
    }
    {
      FunctionRegistry failed;
      check(failed.load(new_code, false, name, err), "partial candidate loaded");
      check(!failed.load("#!lua name=bad\ninvalid Lua", false, name, err),
            "later invalid library aborts candidate");
      submit(queue, failed, queue.prepare());
      while (queue.pending()) check(step(queue, 1) == 1, "failed-stage retirement advances");
      check(invoke(live, "old") == ":4\r\n", "abort preserves live state");
    }
    {
      FunctionRegistry staging;
      check(staging.load(new_code, false, name, err), "successful candidate loaded");
      auto ticket = queue.prepare();
      const auto before = caller_frees;
      fail_after = 0;
      forbid_free = true;
      live.swap(staging);
      const bool no_allocation = fail_after == 0;
      forbid_free = false;
      fail_after = -1;
      const auto frees = caller_frees - before;
      check(no_allocation && frees == 0, "registry publication allocates/frees nothing");
      submit(queue, staging, std::move(ticket));
      check(invoke(live, "new") == ":99\r\n", "new registry visible immediately");
      while (queue.pending()) check(step(queue, 2) > 0, "old shared registry drains");
      check(identity->functions.size() == 1, "external strong owner retains whole old library");
    }

    // One large library is the important bound: draining one library per tick would
    // still unref/destroy thousands of callbacks in a single call.
    constexpr std::size_t functions = 8192;
    FunctionRegistry large;
    const std::string code = "#!lua name=large\nfor i=1," + std::to_string(functions) +
                             " do redis.register_function('f'..i,function() return i end) end";
    check(large.load(code, false, name, err), "large single library loaded");
    std::weak_ptr<const FunctionLibrary> large_owner = large.libraries().at("large");
    submit(queue, large, queue.prepare());
    check(step(queue, functions) == functions && !large_owner.expired(),
          "index nodes consume their own budget before callbacks");
    for (std::size_t i = 0; i < functions; ++i) {
      check(step(queue, 1) == 1 && !large_owner.expired(),
            "each callback/metadata release consumes one unit");
    }
    check(step(queue, 1) == 1 && large_owner.expired() && queue.pending() == 1,
          "library destruction is separate from callback and ticket release");
    check(step(queue, 1) == 1 && queue.pending() == 0, "empty ticket destruction consumes a unit");
    // Reuse the empty queue/tail and source registry after complete retirement.
    check(large.load(new_code, false, name, err), "detached registry reusable");
    submit(queue, large, queue.prepare());
    check(step(queue, 3) == 3 && queue.pending() == 1, "index/callback/library budget exact");
    check(step(queue, 9) == 1 && queue.pending() == 0, "drain returns actual work used");

    // Losing the last external strong owner between ticks must still select bounded
    // callback teardown, rather than erasing a now-unique multi-function library whole.
    FunctionRegistry retained;
    check(retained.load(code, false, name, err), "second large library loaded");
    auto external = retained.libraries().at("large");
    submit(queue, retained, queue.prepare());
    check(step(queue, functions) == functions, "second index retired");
    external.reset();
    check(step(queue, 1) == 1 && queue.pending() == 1, "newly unique owner remains incremental");
    check(step(queue, 2 * functions) == functions + 1 && queue.pending() == 0,
          "remaining callbacks/library/ticket retired exactly once");

    // Unrefs must actually happen, not merely clear names. Repeated closures capture
    // 64K-element tables; explicit GC keeps this memory probe independent of Lua GC timing.
    SessionState session;
    auto memory = [&] {
      const auto reply = lua_engine_eval(
          "collectgarbage('collect');return tostring(collectgarbage('count'))", {}, {}, session);
      return std::stod(reply.substr(reply.find("\r\n") + 2));
    };
    const std::string capture =
        "#!lua name=capture\nlocal t={};for i=1,65536 do t[i]=i end;"
        "redis.register_function('capture',function() return #t end)";
    auto retire_capture = [&] {
      FunctionRegistry candidate;
      check(candidate.load(capture, false, name, err), "captured table loaded");
      submit(queue, candidate, queue.prepare());
      while (queue.pending()) check(step(queue, 1) == 1, "captured closure retired");
    };
    for (int i = 0; i < 5; ++i) retire_capture();
    const auto before = memory();
    for (int i = 0; i < 100; ++i) retire_capture();
    const auto after = memory();
    check(after - before < 128, "retired callbacks are unregistered and collectible");
    std::cout << "Lua retained KiB after retirement: " << after - before << '\n';

    // Queue teardown owns draining, even partially drained callbacks and a long chain.
    std::weak_ptr<const FunctionLibrary> final_owner;
    {
      FunctionRetirementQueue shutdown;
      FunctionRegistry candidate;
      check(candidate.load(code, false, name, err), "shutdown library loaded");
      final_owner = candidate.libraries().at("large");
      submit(shutdown, candidate, shutdown.prepare());
      check(step(shutdown, functions + 1) == functions + 1, "shutdown starts with partial library");
      for (int i = 0; i < 50000; ++i) submit(shutdown, candidate, shutdown.prepare());
      check(shutdown.pending() == 50001, "shutdown owns long intrusive queue");
    }
    check(final_owner.expired(), "shutdown drained callbacks and queue without recursive chain");
    check(invoke(live, "new") == ":99\r\n", "engine still usable after all drains");
  }
  lua_engine_shutdown();
  return failures ? 1 : 0;
}
