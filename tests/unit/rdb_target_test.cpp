#include "command.hpp"
#include "functions.hpp"
#include "lua_engine.hpp"
#include "rdb.hpp"

#include <filesystem>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <unistd.h>

namespace {
int failures = 0;
void check(bool ok, const char* message) {
  if (!ok) {
    ++failures;
    std::cerr << "FAIL: " << message << '\n';
  }
}
std::string call(const peadb::FunctionRegistry& registry, const std::string& function) {
  const auto lib = registry.libraries().find(registry.index().at(function));
  for (const auto& f : lib->second->functions) {
    if (f.canonical_name == function) {
      peadb::SessionState session;
      return peadb::lua_engine_fcall(f.callback_ref, {}, {}, session, true);
    }
  }
  return "missing";
}
const std::string library =
    "#!lua name=snapshot\nredis.register_function{function_name='value', "
    "callback=function() return 42 end, flags={'no-writes'}}";
std::string framed(std::string body) {
  body = "REDIS0011" + body + char(255);
  auto crc = peadb::crc64(0, body.data(), body.size());
  for (int i = 0; i < 8; ++i) body += char(crc >> (8 * i));
  return body;
}
void write(const std::filesystem::path& path, const std::string& bytes) {
  std::ofstream file(path, std::ios::binary);
  file.write(bytes.data(), bytes.size());
}
void run(const std::filesystem::path& path) {
  using namespace peadb;
  auto& live = store();
  live.flushall();
  functions_register_rdb_hooks();
  std::string name, err;
  check(live_functions().load(library, false, name, err), "snapshot function compiles");
  const std::string binary("v\0x", 3);
  const std::string large(200000, 'L');
  DataStore::StreamDump stream;
  stream.entries = {{"1-0", {{"f", binary}}}, {"2-0", {{"f", "second"}}}};
  stream.last_ms = 3;
  stream.entries_added = 3;
  stream.max_deleted_id = "3-0";
  DataStore::StreamGroupDump group;
  group.name = "group";
  group.last_delivered_id = "2-0";
  group.entries_read = 2;
  group.pending = {{"1-0", "consumer", 123456, 7}};
  group.consumers["consumer"] = {123, 456};
  group.consumers["empty"] = {234, 567};
  stream.groups.push_back(group);
  {
    DataStore::LazyExpireDisabled guard(live);
    for (int db : {0, 1, 15}) {
      live.select_db(db);
      bool wrong = false;
      live.set("string", binary, false, false, 9007199254740993LL, false);
      live.set("large", large, false, false, {}, false);
      live.set("expired", "old", false, false, 1, false);
      live.rpush("list", {binary, "b"}, wrong);
      live.hset("hash", {{"f", binary}}, wrong);
      live.sadd("set", {binary, "b"}, wrong);
      live.zadd_one("zset", 1.25, binary, false, false, false, false, false);
      live.restore_stream("stream", stream);
    }
  }
  RdbReplicationInfo aux{15, std::string(40, 'a'), 12345};
  check(rdb_save(path, err, &aux), "save rich snapshot");
  live.flushall();
  live.select_db(7);
  live.set("live", "unchanged", false, false, {}, false);
  live.set_replica_expiry(true);
  live.set_writable_replica(true);
  live.set("local-ttl", "keep", false, false, DataStore::now_ms() + 60000, false);
  live_functions().clear();
  check(live_functions().load(
            "#!lua name=live\nlocal n=0;redis.register_function{function_name='counter',"
            "callback=function() n=n+1;return n end,flags={'no-writes'}}",
            false, name, err),
        "live closure compiles");
  check(call(live_functions(), "counter") == ":1\r\n", "live callback initialized");
  const auto live_record = live_functions().libraries().begin()->second;
  const auto global_stats = rdb_last_load_stats();
  unsigned live_hooks = 0;
  rdb_register_function_hooks({}, [&](const std::string&, std::string&) {
    ++live_hooks;
    throw std::runtime_error("live hook called");
    return false;
  });
  for (bool keep : {false, true}) {
    DataStore staging;
    // Deliberately oppose runtime role and explicit TTL policy.
    staging.set_replica_expiry(!keep);
    staging.set_writable_replica(true);
    staging.select_db(9);
    unsigned expiry_hooks = 0;
    staging.set_lazy_expire_hook([&](int, const std::string&) { ++expiry_hooks; });
    FunctionRegistry functions;
    RdbLoadStats stats{999, 999};
    RdbReplicationInfo result{0, "old", 1};
    RdbLoadTarget target{staging, functions, stats};
    target.expiry = keep ? RdbLoadTarget::Expiry::KeepExpired : RdbLoadTarget::Expiry::DropExpired;
    target.replication = &result;
    unsigned dropped = 0;
    target.expired = [&](int db, const std::string& key) {
      check((db == 0 || db == 1 || db == 15) && key == "expired", "expiry target callback");
      ++dropped;
    };
    check(rdb_load_into(path, target, err), "explicit rich load succeeds");
    check(staging.current_db() == 9 && staging.replica_expiry() == !keep,
          "target selection and role restored");
    check(stats.loaded == (keep ? 24 : 21) && stats.expired == (keep ? 0 : 3) &&
              dropped == (keep ? 0 : 3),
          "isolated load counters/expiry");
    check(
        result.valid(staging.database_count()) && result.stream_db == 15 && result.offset == 12345,
        "target AUX published");
    check(expiry_hooks == 0 && staging.replica_expires_tracked_keys() == 0,
          "loading never propagates/owns TTLs");
    check(call(functions, "value") == ":42\r\n" && functions.libraries().size() == 1,
          "private function callable");
    DataStore::LazyExpireDisabled guard(staging);
    for (int db : {0, 1, 15}) {
      staging.select_db(db);
      bool wrong = false;
      check(staging.get("string") == binary && staging.get("large") == large,
            "binary and large strings in target");
      check(staging.lrange("list", 0, -1, wrong) == std::vector<std::string>({binary, "b"}),
            "list in target");
      check(staging.hget("hash", "f", wrong) == binary && staging.scard("set", wrong) == 2,
            "hash and set in target");
      check(staging.zrange("zset", 0, -1, true, wrong) ==
                std::vector<std::pair<std::string, double>>({{binary, 1.25}}),
            "zset in target");
      DataStore::StreamDump actual;
      check(staging.dump_stream("stream", actual) && actual.entries == stream.entries &&
                actual.entries_added == 3 && actual.max_deleted_id == "3-0" &&
                actual.groups.size() == 1,
            "stream history in target");
      if (!actual.groups.empty()) {
        const auto& g = actual.groups[0];
        check(g.name == "group" && g.entries_read == 2 && g.last_delivered_id == "2-0" &&
                  g.consumers.size() == 2 && g.pending.size() == 1 &&
                  g.pending[0].consumer == "consumer" && g.pending[0].delivery_ms == 123456 &&
                  g.pending[0].delivery_count == 7,
              "groups/consumers/PEL in target");
      }
      check(staging.exists({"expired"}) == keep, "explicit role expiry policy");
      check(staging.pexpiretime("string") == 9007199254740993LL, "exact absolute TTL");
    }
  }
  check(live.current_db() == 7 && live.get("live") == "unchanged" &&
            live.replica_expires_tracked_keys() == 1,
        "live data/TTL ownership untouched");
  check(live_hooks == 0 && rdb_last_load_stats().loaded == global_stats.loaded &&
            rdb_last_load_stats().expired == global_stats.expired,
        "global hooks and counters untouched");
  check(live_functions().libraries().begin()->second == live_record &&
            call(live_functions(), "counter") == ":2\r\n",
        "live callback identity/state untouched");

  // A callback exception unwinds only the private target's selection/expiry guard.
  DataStore staging;
  FunctionRegistry functions;
  RdbLoadStats stats;
  RdbReplicationInfo result;
  staging.select_db(11);
  staging.set_replica_expiry(true);
  RdbLoadTarget target{staging, functions, stats};
  target.replication = &result;
  target.expired = [](int, const std::string&) {
    throw std::runtime_error("stop load");
  };
  bool threw = false;
  try {
    rdb_load_into(path, target, err);
  } catch (const std::runtime_error&) {
    threw = true;
  }
  check(threw && staging.current_db() == 11 && !result.valid(),
        "exception restores target and leaves AUX unpublished");
  staging.set("past", "expired", false, false, 1, false);
  check(!staging.get("past"), "lazy expiry restored after exception");

  // Structural failures never consult singleton hooks; counters/AUX are explicit outputs.
  target.expired = {};
  for (const auto& body : {std::string("\xfe\x10", 2), std::string("\0\x03key\x04xx", 9)}) {
    write(path, framed(body));
    result = aux;
    check(!rdb_load_into(path, target, err) && !result.valid() && staging.current_db() == 11,
          "DB-bound/truncated target refusal");
  }
  write(path, framed(std::string("\0\x03key\x03val", 9)));
  auto data = framed(std::string("\0\x03key\x03val", 9));
  data.back() ^= 1;
  write(path, data);
  staging.set("sentinel", "old", false, false, {}, false);
  check(!rdb_load_into(path, target, err) && staging.get("sentinel") == "old",
        "explicit checksum check precedes mutation");
  target.skip_checksum_validation = true;
  const bool ok = rdb_load_into(path, target, err);
  check(staging.current_db() == 11, "successful load restores target selection");
  staging.select_db(0);
  check(ok && staging.get("key") == "val", "explicit checksum override");
  check(call(live_functions(), "counter") == ":3\r\n" && live_hooks == 0,
        "failure preserves live callback state");
  // A later bad/colliding FUNCTION2 can leave a partial private context, never live state.
  auto encoded = [](const std::string& value) {
    return std::string{char(0x40 | (value.size() >> 8)), char(value.size() & 255)} + value;
  };
  for (const auto& bad : {"#!lua name=bad\ninvalid Lua",
                          "#!lua name=bad\nredis.register_function('value',function() end)"}) {
    std::string body(1, char(245));
    body += encoded(library);
    body += char(245);
    body += encoded(bad);
    write(path, framed(body));
    DataStore private_data;
    private_data.select_db(12);
    FunctionRegistry private_functions;
    RdbLoadStats private_stats;
    RdbReplicationInfo private_aux;
    RdbLoadTarget private_target{private_data, private_functions, private_stats};
    private_target.replication = &private_aux;
    check(!rdb_load_into(path, private_target, err) && !private_aux.valid() &&
              private_data.current_db() == 12,
          "bad target function refuses with AUX and DB restored");
    check(private_functions.libraries().size() == 1 && private_functions.index().size() == 1,
          "private failure retains only successfully compiled target library");
    check(live_functions().libraries().begin()->second == live_record && live.current_db() == 7 &&
              live_hooks == 0,
          "bad target function cannot publish keys or callbacks");
  }
  // Historical private type 200 still targets the supplied datastore for local recovery.
  auto small = [](const std::string& value) {
    return std::string(1, char(value.size())) + value;
  };
  std::string old_stream(1, char(200));
  old_stream += small("private");
  old_stream += char(1);
  old_stream += small("1-0");
  old_stream += char(1);
  old_stream += small("f");
  old_stream += small("v");
  old_stream += char(1);
  old_stream += char(0);
  old_stream += char(1);
  old_stream += small("group");
  old_stream += small("1-0");
  old_stream += char(1);
  old_stream += small("1-0");
  old_stream += small("owner");
  old_stream += char(5);
  old_stream += char(7);
  write(path, framed(old_stream));
  check(rdb_load_into(path, target, err), "private recovery stream uses explicit target");
  DataStore::StreamDump recovered;
  staging.select_db(0);
  check(staging.dump_stream("private", recovered) && recovered.entries.size() == 1 &&
            recovered.groups.size() == 1 && recovered.groups[0].pending.size() == 1 &&
            recovered.groups[0].pending[0].consumer == "owner",
        "private stream PEL retained");
  bool worker_loaded = true;
  std::thread worker([&] {
    DataStore private_data;
    FunctionRegistry private_functions;
    RdbLoadStats private_stats{88, 99};
    RdbLoadTarget private_target{private_data, private_functions, private_stats};
    std::string error;
    worker_loaded = rdb_load_into(path, private_target, error);
    check(private_stats.loaded == 88 && private_stats.expired == 99,
          "worker refuses before applying state");
  });
  worker.join();
  check(!worker_loaded, "target decode cannot run on worker interpreter");
  live.set_replica_expiry(false);
  live.set_writable_replica(false);
  functions_register_rdb_hooks();
}
void compare_fixture(const std::filesystem::path& path) {
  using namespace peadb;
  functions_register_rdb_hooks();
  auto& live = store();
  live.flushall();
  live.set_replica_expiry(true);
  live.set("live sentinel", "keep", false, false, {}, false);
  DataStore staging;
  FunctionRegistry functions;
  RdbLoadStats stats;
  RdbReplicationInfo aux;
  RdbLoadTarget target{staging, functions, stats};
  target.expiry = RdbLoadTarget::Expiry::KeepExpired;
  target.replication = &aux;
  std::string err;
  const bool loaded = rdb_load_into(path, target, err);
  check(loaded, "external snapshot loads into private target");
  if (!loaded) {
    std::cerr << err << '\n';
    return;
  }
  check(live.get("live sentinel") == "keep", "external target leaves live sentinel alone");
  check(rdb_load(path, err), "external snapshot loads through live wrapper");
  DataStore::LazyExpireDisabled no_live_expire(live), no_target_expire(staging);
  check(staging.debug_digest() == live.debug_digest(), "external target matches live data digest");
  for (int db = 0; db < 16; ++db) {
    staging.select_db(db);
    live.select_db(db);
    auto keys = staging.keys("*"), live_keys = live.keys("*");
    std::sort(keys.begin(), keys.end());
    std::sort(live_keys.begin(), live_keys.end());
    check(keys == live_keys, "external database key sets");
    for (const auto& key : keys) {
      check(staging.pexpiretime(key) == live.pexpiretime(key), "external TTL unchanged");
      DataStore::StreamDump actual, expected;
      if (staging.dump_stream(key, actual)) {
        check(live.dump_stream(key, expected) && actual.entries == expected.entries &&
                  actual.entries_added == expected.entries_added &&
                  actual.max_deleted_id == expected.max_deleted_id &&
                  actual.legacy_empty_first_id == expected.legacy_empty_first_id &&
                  actual.last_ms == expected.last_ms && actual.last_seq == expected.last_seq &&
                  actual.groups.size() == expected.groups.size(),
              "external stream values/history");
        for (std::size_t i = 0; i < actual.groups.size() && i < expected.groups.size(); ++i) {
          auto& a = actual.groups[i];
          auto& b = expected.groups[i];
          check(a.name == b.name && a.last_delivered_id == b.last_delivered_id &&
                    a.entries_read == b.entries_read && a.consumers.size() == b.consumers.size() &&
                    a.pending.size() == b.pending.size(),
                "external groups/PEL lengths");
          for (const auto& [name, consumer] : a.consumers) {
            const auto it = b.consumers.find(name);
            check(it != b.consumers.end() && it->second.seen_time == consumer.seen_time &&
                      it->second.active_time == consumer.active_time,
                  "external consumer times");
          }
          auto order = [](const auto& x, const auto& y) {
            return x.id < y.id;
          };
          std::sort(a.pending.begin(), a.pending.end(), order);
          std::sort(b.pending.begin(), b.pending.end(), order);
          for (std::size_t j = 0; j < a.pending.size() && j < b.pending.size(); ++j)
            check(a.pending[j].id == b.pending[j].id &&
                      a.pending[j].consumer == b.pending[j].consumer &&
                      a.pending[j].delivery_ms == b.pending[j].delivery_ms &&
                      a.pending[j].delivery_count == b.pending[j].delivery_count,
                  "external PEL metadata");
        }
      }
    }
  }
  check(functions.libraries().size() == live_functions().libraries().size(),
        "external library count");
  for (const auto& [name, lib] : functions.libraries()) {
    const auto other = live_functions().libraries().at(name);
    check(other->code == lib->code && other->functions.size() == lib->functions.size() &&
              other != lib,
          "external libraries compile independently");
    if (!lib->functions.empty()) {
      peadb::SessionState first, second;
      check(lua_engine_fcall(lib->functions[0].callback_ref, {}, {}, first, true) ==
                lua_engine_fcall(other->functions[0].callback_ref, {}, {}, second, true),
            "external callbacks execute equally");
    }
  }
  check(rdb_save(path.string() + ".peadb.rdb", err), "external target/wrapper roundtrip saved");
  std::cout << "external target comparison passed: " << path << '\n';
}

}  // namespace
int main(int argc, char** argv) {
  peadb::lua_engine_init();
  if (argc > 1) {
    for (int i = 1; i < argc; ++i) compare_fixture(argv[i]);
    peadb::lua_engine_shutdown();
    return failures ? 1 : 0;
  }
  char scratch[] = "/tmp/peadb-rdb-target-XXXXXX";
  auto* directory = mkdtemp(scratch);
  if (!directory) return 1;
  run(std::filesystem::path(directory) / "dump.rdb");
  std::filesystem::remove_all(directory);
  peadb::lua_engine_shutdown();
  return failures ? 1 : 0;
}
