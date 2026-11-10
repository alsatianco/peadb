#include "command.hpp"
#include "datastore.hpp"
#include "lua_engine.hpp"
#include "rdb.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

namespace {
int failures = 0;
constexpr const char* kLoading = "-LOADING Redis is loading the dataset in memory\r\n";
void check(bool ok, const char* label) {
  if (!ok) {
    ++failures;
    std::cerr << "FAIL: " << label << '\n';
  }
}
std::string run(peadb::SessionState& client, std::initializer_list<std::string> args) {
  bool close = false;
  return peadb::handle_command(std::vector<std::string>(args), client, close);
}
std::string resp(std::initializer_list<std::string> args) {
  std::string out = "*" + std::to_string(args.size()) + "\r\n";
  for (const auto& arg : args) out += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
  return out;
}
std::string read(const std::filesystem::path& path) {
  std::ifstream file(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}
void write(const std::filesystem::path& path, const std::string& bytes) {
  std::ofstream file(path, std::ios::binary | std::ios::trunc);
  file << bytes;
}
template <class F>
void rejected(F fn, const char* label) {
  bool threw = false;
  try {
    fn();
  } catch (const std::logic_error&) {
    threw = true;
  }
  check(threw, label);
}

void replay(const std::filesystem::path& dir) {
  using namespace peadb;
  const auto source = dir / "source.aof";
  const auto audit = dir / "audit.aof";
  const std::string bytes = resp({"SELECT", "3"}) + resp({"SET", "counter", "40"}) +
                            resp({"MULTI"}) + resp({"INCR", "counter"}) + resp({"SELECT", "5"}) +
                            resp({"SET", "transaction", "yes"}) + resp({"EXEC"}) +
                            resp({"SELECT", "3"}) + resp({"INCR", "counter"});
  write(source, bytes);
  check(configure_persistence("dump.rdb", true, audit.string(), "always").empty(), "open AOF");
  SessionState client;
  std::size_t calls = 0;
  std::string err;
  check(load_aof_file(
            source.string(), err,
            [&] {
              ++calls;
              check(loading_mode() == LoadingMode::Blocking,
                    "AOF establishes blocking loading mode");
              check(run(client, {"GET", "counter"}) ==
                        (calls == 1 ? "-NOAUTH Authentication required.\r\n" : kLoading),
                    "replay does not bypass client auth/loading");
              if (calls == 1) check(run(client, {"AUTH", "secret"}) == "+OK\r\n", "nested AUTH");
              check(run(client, {"SET", "injected", "bad"}) == kLoading,
                    "cannot mutate replay target");
              check(run(client, {"CONFIG", "SET", "appendonly", "no"}) == kLoading,
                    "replay does not bypass config restrictions");
              check(run(client, {"MULTI"}) == "+OK\r\n", "client transaction starts");
              check(run(client, {"GET", "counter"}) == kLoading,
                    "loading dirties client transaction");
              check(run(client, {"EXEC"}).starts_with("-EXECABORT"),
                    "client EXEC refuses dirty queue");
              check(run(client, {"SELECT", "11"}) == "+OK\r\n", "client SELECT permitted");
              check(run(client, {"ECHO", "alive"}) == "$5\r\nalive\r\n", "safe client command");
              rejected([&] { service_loading_events([] {}); }, "recursive event service refused");
              rejected([&] { load_aof_file(source.string(), err); }, "recursive AOF load refused");
              check(run(client, {"CONFIG", "SET", "timeout", "7"}) == "+OK\r\n",
                    "permitted config update survives service");
            }),
        "replay with clients succeeds");
  check(calls == 9, "checkpoint per complete AOF record, not inside EXEC");
  check(run(client, {"CONFIG", "GET", "timeout"}).find("7\r\n") != std::string::npos,
        "service preserves actual runtime config effects");
  check(loading_mode() == LoadingMode::None, "replay restores prior loading mode");
  store().select_db(3);
  check(store().get("counter") == "42" && !store().get("injected"), "DB3 replay not redirected");
  store().select_db(5);
  check(store().get("transaction") == "yes", "transaction DB and replay state preserved");
  check(read(audit).empty() && read(source) == bytes, "replay/client refusals append no AOF data");

  store().select_db(7);
  bool threw = false;
  try {
    load_aof_file(source.string(), err, [&] {
      store().select_db(12);
      throw std::runtime_error("service error");
    });
  } catch (const std::runtime_error& e) {
    threw = std::string(e.what()) == "service error";
  }
  check(threw && store().current_db() == 7 && loading_mode() == LoadingMode::None,
        "callback exception restores DB/loading/replay state");
  check(run(client, {"SET", "after-exception", "ok"}) == "+OK\r\n", "normal dispatch resumes");
  aof_cron(peadb::DataStore::now_ms());
  check(read(audit).find("after-exception") != std::string::npos,
        "exception cannot leave AOF propagation suppressed");
  configure_persistence("dump.rdb", false, audit.string(), "always");
}

void rdb(const std::filesystem::path& dir) {
  using namespace peadb;
  store().flushall();
  store().select_db(3);
  const std::string large(200000, 'v');
  store().set("large", large, false, false, {}, false);
  std::string err;
  const auto path = dir / "snapshot.rdb";
  check(rdb_save(path, err), "save RDB fixture");
  store().select_db(7);
  SessionState client;
  client.authenticated = true;
  unsigned calls = 0;
  RdbLoadControl control;
  control.checkpoint = [&](const RdbLoadProgress&) {
    const int db = store().current_db();
    service_loading_events([&] {
      ++calls;
      check(run(client, {"SELECT", "15"}) == "+OK\r\n", "RDB nested SELECT");
      check(run(client, {"GET", "large"}) == kLoading, "RDB target remains inaccessible");
    });
    check(store().current_db() == db, "RDB service restores target DB at every boundary");
    return true;
  };
  {
    const LoadingScope loading(LoadingMode::Blocking);
    check(rdb_load(path, err, {}, nullptr, {}, &control), "RDB with guarded nested commands");
  }
  check(calls > 5 && store().current_db() == 7, "large RDB yields with caller DB restored");
  store().select_db(3);
  check(store().get("large") == large, "RDB destination unaffected by client SELECT");
}

void execution(const std::filesystem::path& dir) {
  using namespace peadb;
  store().flushall();
  store().select_db(0);
  const auto audit = dir / "execution.aof";
  check(configure_persistence("dump.rdb", true, audit.string(), "always").empty(), "execution AOF");
  SessionState outer;
  outer.authenticated = true;
  pubsub_subscribe(123, "checkpoint");
  unsigned calls = 0;
  g_pubsub_send = [&](int, const std::string&) {
    ++calls;
    const int outer_db = store().current_db();
    const LoadingScope loading(LoadingMode::Async);  // infrastructure only; no production swapdb
    service_loading_events([&] {
      SessionState nested;
      nested.authenticated = true;
      check(run(nested, {"SELECT", "9"}) == "+OK\r\n", "nested async DB");
      check(run(nested, {"MULTI"}) == "+OK\r\n", "nested EXEC independence");
      check(run(nested, {"INCR", "nested"}) == "+QUEUED\r\n", "not inherited outer EXEC");
      check(run(nested, {"EXEC"}) == "*1\r\n:1\r\n", "nested transaction commits once");
      aof_cron(DataStore::now_ms());
      const auto now = read(audit);
      check(
          now.find("nested") != std::string::npos && now.find("outer-before") == std::string::npos,
          "nested propagation flushes independently of pending outer transaction");
    });
    check(store().current_db() == outer_db, "outer transaction DB restored");
    bool threw = false;
    try {
      service_loading_events([&] {
        SessionState other;
        other.authenticated = true;
        run(other, {"SELECT", "13"});
        run(other, {"ECHO", "exception"});
        throw std::runtime_error("during transaction");
      });
    } catch (const std::runtime_error&) {
      threw = true;
    }
    check(threw && store().current_db() == outer_db,
          "exception restores active transaction/DB/scratch/pending context");
    service_loading_events([] {});
    return true;
  };
  check(run(outer, {"MULTI"}) == "+OK\r\n", "outer MULTI");
  check(run(outer, {"INCR", "outer-before"}) == "+QUEUED\r\n", "outer queue before service");
  check(run(outer, {"PUBLISH", "checkpoint", "go"}) == "+QUEUED\r\n", "outer queue checkpoint");
  check(run(outer, {"INCR", "outer-after"}) == "+QUEUED\r\n", "outer queue after service");
  check(run(outer, {"EXEC"}) == "*3\r\n:1\r\n:1\r\n:1\r\n", "outer transaction completes");
  check(calls == 1, "single nested checkpoint");
  g_pubsub_send = {};
  pubsub_unsubscribe_all(123);
  aof_cron(DataStore::now_ms());
  const auto bytes = read(audit);
  check(bytes.find("nested") < bytes.find("outer-before") &&
            bytes.find("outer-before") < bytes.find("outer-after"),
        "separate ordered AOF units");
  check(store().get("outer-before") == "1" && store().get("outer-after") == "1",
        "outer writes remain in DB0");
  store().select_db(9);
  check(store().get("nested") == "1" && !store().get("outer-after"), "nested writes remain in DB9");
  configure_persistence("dump.rdb", false, audit.string(), "always");
  store().flushall();
  std::string err;
  check(load_aof_file(audit.string(), err), "combined isolated units replay successfully");
  store().select_db(0);
  check(store().get("outer-before") == "1" && store().get("outer-after") == "1",
        "outer pending propagation preserved exactly once");
  store().select_db(9);
  check(store().get("nested") == "1", "nested AOF mutation persisted exactly once");
}
}  // namespace

int main() {
  using namespace peadb;
  char scratch[] = "/tmp/peadb-loading-service-XXXXXX";
  const char* created = mkdtemp(scratch);
  if (!created) return 1;
  const auto dir = std::filesystem::path(created);
  lua_engine_init();
  lua_engine_set_dispatch([](const auto& args, auto& session, bool& close) {
    return handle_command(args, session, close);
  });
  functions_register_rdb_hooks();
  configure_security("secret", "", "", true, "no", {});
  configure_runtime_settings({{"enable-protected-configs", "yes"}}, "");
  rejected([] { service_loading_events([] {}); }, "service requires loading state");
  replay(dir);
  rdb(dir);
  execution(dir);
  {
    const LoadingScope loading(LoadingMode::Blocking);
    set_lua_script_context(true);
    rejected([] { service_loading_events([] {}); },
             "Lua service requires independent future design");
    set_lua_script_context(false);
    service_loading_events([] {});  // rejected invocation must leave guard reusable
  }
  lua_engine_shutdown();
  std::filesystem::remove_all(dir);
  std::cout << "Loading service failures: " << failures << '\n';
  return failures ? 1 : 0;
}
