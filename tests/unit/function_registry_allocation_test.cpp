// This executable substitutes the allocation-counter object with an ordinary allocator,
// then fails exactly the Nth C++ allocation. Production binaries keep src/zmalloc.cpp.
#include "functions.hpp"
#include "lua_engine.hpp"
#include "zmalloc.hpp"
#include "rdb.hpp"
#include <filesystem>
#include <fstream>
#include <unistd.h>

#include <cstdlib>
#include <iostream>
#include <new>
#include <string>
#include <thread>

namespace {
thread_local long fail_after = -1;
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
std::string counter(const peadb::FunctionRegistry& registry) {
  peadb::SessionState session;
  return peadb::lua_engine_fcall(registry.libraries().at("old")->functions[0].callback_ref, {}, {},
                                 session, true);
}
}  // namespace
void* operator new(std::size_t n) {
  return allocate(n);
}
void* operator new[](std::size_t n) {
  return allocate(n);
}
void operator delete(void* p) noexcept {
  std::free(p);
}
void operator delete[](void* p) noexcept {
  std::free(p);
}
void operator delete(void* p, std::size_t) noexcept {
  std::free(p);
}
void operator delete[](void* p, std::size_t) noexcept {
  std::free(p);
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
  std::free(p);
}
void operator delete[](void* p, const std::nothrow_t&) noexcept {
  std::free(p);
}
void operator delete(void* p, std::align_val_t) noexcept {
  std::free(p);
}
void operator delete[](void* p, std::align_val_t) noexcept {
  std::free(p);
}
void operator delete(void* p, std::size_t, std::align_val_t) noexcept {
  std::free(p);
}
void operator delete[](void* p, std::size_t, std::align_val_t) noexcept {
  std::free(p);
}
void operator delete(void* p, std::align_val_t, const std::nothrow_t&) noexcept {
  std::free(p);
}
void operator delete[](void* p, std::align_val_t, const std::nothrow_t&) noexcept {
  std::free(p);
}
namespace peadb {
std::size_t zmalloc_used_memory() {
  return 0;
}
std::size_t zmalloc_string_bytes(const std::string&) {
  return 0;
}
}  // namespace peadb
int main() {
  using namespace peadb;
  lua_engine_init();
  {
    FunctionRegistry registry;
    std::string name, err;
    const std::string old =
        "#!lua name=old\nlocal n=0;redis.register_function{function_name='count',"
        "description='retained callback with captured state',callback=function() n=n+1;return n "
        "end,flags={'no-writes'}}";
    const std::string replacement =
        "#!lua name=old\nredis.register_function{function_name='replacement',"
        "description='new function with allocation during export',callback=function() return 99 "
        "end,flags={'no-writes'}}";
    const std::string other =
        "#!lua name=other\nredis.register_function('other',function() return 9 end)";
    check(registry.load(old, false, name, err), "original library installed");
    check(counter(registry) == ":1\r\n", "closure starts once");
    long expected = 1;
    unsigned failures_seen = 0;
    // Exercise every allocation in compilation/registration, exported callbacks, map nodes,
    // source/name copies, and replacement. The first complete success terminates each sweep.
    for (int operation = 0; operation < 4; ++operation) {
      bool completed = false;
      for (long n = 0; n < 2000; ++n) {
        FunctionRegistry target(registry);
        const auto identity = target.libraries().at("old");
        const std::vector<std::string> sources = {replacement, other};
        const std::string policy = operation == 1 ? "FLUSH" : "REPLACE";
        bool succeeded = false, threw = false;
        fail_after = n;
        try {
          if (operation == 0)
            succeeded = target.load(replacement, true, name, err);
          else if (operation == 3)
            succeeded = target.load(other, false, name, err);
          else
            succeeded = target.restore(sources, policy, err);
        } catch (const std::bad_alloc&) {
          threw = true;
        }
        fail_after = -1;
        if (threw) {
          ++failures_seen;
          check(target.libraries().size() == 1 && target.libraries().at("old") == identity &&
                    target.index().size() == 1 && target.index().count("count"),
                "OOM preserves complete destination registry");
          ++expected;
          check(counter(target) == ":" + std::to_string(expected) + "\r\n",
                "OOM preserves closure identity and state");
          // A protected registration error/exception must not poison the Lua stack, hooks or
          // loading globals: the next independent compile succeeds on the same engine.
          FunctionRegistry recovery;
          check(recovery.load(other, false, name, err),
                "Lua engine reusable after allocation failure");
        } else {
          check(succeeded, "sweep ends only with successful operation");
          completed = succeeded;
          break;
        }
      }
      check(completed, "every allocation sweep completes");
      check(registry.libraries().size() == 1 && registry.index().count("count"),
            "private installation leaves original registry alone");
    }
    check(failures_seen > 0,
          "allocation sweep reached parsing/compilation/export/commit preparation");
    std::cout << "injected allocation failures: " << failures_seen << '\n';
    // Independent registries may use identical library/function names without a collision.
    FunctionRegistry independent;
    check(independent.load(old, false, name, err) && counter(independent) == ":1\r\n",
          "independent callback state");
    ++expected;
    check(counter(registry) == ":" + std::to_string(expected) + "\r\n",
          "original callback still advances");
    std::weak_ptr<const FunctionLibrary> weak;
    {
      FunctionRegistry temporary;
      check(temporary.load(other, false, name, err), "owned context compiled");
      weak = temporary.libraries().begin()->second;
      {
        FunctionRegistry copy(temporary);
        temporary.clear();
        check(!weak.expired(), "copy retains callback owner");
      }
    }
    check(weak.expired(), "last context destroys callback owner");
    // Compilation/registration/name collisions never remove existing callbacks.
    for (const auto& code :
         {"#!lua name=bad\ninvalid Lua",
          "#!lua name=bad\nredis.register_function('count',function() end)",
          "#!lua name=bad\nredis.register_function('x',function() end);error('abort')"}) {
      check(!registry.load(code, false, name, err), "invalid library refused");
      check(registry.libraries().size() == 1 && registry.index().size() == 1,
            "failed library leaves no index entries");
    }
    // Ref ownership is observable after Lua GC: discarded callbacks capture a large table
    // each. Leaking even a few refs exceeds the bound; the registry table may retain slots.
    SessionState session;
    auto memory = [&] {
      const auto reply = lua_engine_eval(
          "collectgarbage('collect'); return tostring(collectgarbage('count'))", {}, {}, session);
      const auto start = reply.find("\r\n");
      check(start != std::string::npos && reply[0] == '$', "Lua memory sample available");
      return std::stod(reply.substr(start + 2));
    };
    const std::string capture =
        "#!lua name=capture\nlocal data={};for i=1,65536 do data[i]=i end;"
        "redis.register_function('capture',function() return #data end)";
    for (int i = 0; i < 5; ++i) {
      FunctionRegistry temp;
      check(temp.load(capture, false, name, err), "GC warmup");
    }
    const double before = memory();
    for (int i = 0; i < 100; ++i) {
      FunctionRegistry temp;
      check(temp.load(capture, false, name, err), "temporary captured callback");
      check(!temp.load(capture + ";error('abort')", true, name, err),
            "failed captured callback load");
    }
    const double after = memory();
    check(after - before < 128, "discarded and failed callback refs are unregistered before GC");

    // Fail each allocation while decoding into a private datastore/function context.
    char scratch[] = "/tmp/peadb-rdb-alloc-XXXXXX";
    auto* directory = mkdtemp(scratch);
    check(directory != nullptr, "owned target fixture directory");
    if (directory) {
      const auto path = std::filesystem::path(directory) / "dump.rdb";
      auto string = [](const std::string& value) {
        return std::string{char(0x40 | (value.size() >> 8)), char(value.size() & 255)} + value;
      };
      std::string data = "REDIS0011";
      data += char(245);
      data += string(other);
      data += char(254);
      data += char(15);
      data += char(0);
      data += string("key");
      data += string(std::string(1000, 'x'));
      data += char(255);
      const auto crc = crc64(0, data.data(), data.size());
      for (int i = 0; i < 8; ++i) data += char(crc >> (8 * i));
      {
        std::ofstream file(path, std::ios::binary);
        file.write(data.data(), data.size());
      }
      bool completed = false;
      unsigned caught = 0;
      for (long n = 0; n < 2000; ++n) {
        DataStore datastore;
        datastore.select_db(13);
        FunctionRegistry functions;
        RdbLoadStats stats;
        RdbReplicationInfo aux;
        RdbLoadTarget target{datastore, functions, stats};
        target.replication = &aux;
        bool ok = false, threw = false;
        fail_after = n;
        try {
          ok = rdb_load_into(path, target, err);
        } catch (const std::bad_alloc&) {
          threw = true;
        }
        fail_after = -1;
        check(datastore.current_db() == 13, "target allocation failure restores database");
        if (threw) {
          ++caught;
          check(!aux.valid(), "target allocation failure cannot publish AUX");
        } else {
          check(ok, "target allocation sweep ends with complete decode");
          completed = ok;
          break;
        }
      }
      check(completed && caught > 0, "private decode allocation sweep completes");
      std::cout << "injected target allocation failures: " << caught << '\n';
      std::filesystem::remove_all(directory);
    }
    // Worker compilation is refused before accessing the interpreter.
    bool accepted = true;
    std::thread worker([&] {
      FunctionRegistry private_registry;
      std::string n, e;
      accepted = private_registry.load(other, false, n, e);
    });
    worker.join();
    check(!accepted, "worker cannot compile on main interpreter");
  }
  lua_engine_shutdown();
  return failures ? 1 : 0;
}
