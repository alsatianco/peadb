#include "command.hpp"
#include "datastore.hpp"
#include "lua_engine.hpp"

#include <fstream>
#include <stdexcept>
#include <unistd.h>

#include <iostream>
#include <string>
#include <vector>

namespace {
int failures = 0;
constexpr const char* kLoading = "-LOADING Redis is loading the dataset in memory\r\n";
std::string run(peadb::SessionState& session, std::initializer_list<std::string> args) {
  bool close = false;
  return peadb::handle_command(std::vector<std::string>(args), session, close);
}
void equal(const std::string& got, const std::string& expected, const char* label) {
  if (got != expected) {
    ++failures;
    std::cerr << label << ": expected " << expected << "got " << got << '\n';
  }
}
void contains(const std::string& got, const std::string& expected, const char* label) {
  if (got.find(expected) == std::string::npos) {
    ++failures;
    std::cerr << label << ": missing " << expected << " in " << got << '\n';
  }
}
}  // namespace

int main(int argc, char** argv) {
  // A fresh process is needed because rename-command is applied when the table first builds.
  if (argc == 2 && std::string(argv[1]) == "--renamed") {
    peadb::configure_security("", "", "", true, "no",
                              {{"GET", "FETCH", "get", 0, ""},
                               {"INFO", "STATUS", "info", 0, ""},
                               {"CONFIG", "SETTINGS", "config", 0, ""},
                               {"REPLICAOF", "FOLLOW", "replicaof", 0, ""},
                               {"SCRIPT", "SCRIPTS", "script", 0, ""}});
    peadb::SessionState client;
    peadb::set_loading(true);
    equal(run(client, {"FETCH", "k"}), kLoading, "renamed read");
    contains(run(client, {"STATUS", "persistence"}), "loading:1\r\n", "renamed INFO");
    equal(run(client, {"SETTINGS", "SET", "appendonly", "no"}), kLoading, "renamed CONFIG");
    equal(run(client, {"SCRIPTS", "LOAD"}),
          "-ERR wrong number of arguments for 'script|load' command\r\n",
          "renamed subcommand arity");
    peadb::set_loading_mode(peadb::LoadingMode::Async);
    equal(run(client, {"FOLLOW", "NO", "ONE"}), kLoading, "renamed async restriction");
    equal(run(client, {"MULTI"}), "+OK\r\n", "renamed async transaction");
    equal(run(client, {"FOLLOW", "NO", "ONE"}), kLoading, "renamed queue restriction");
    contains(run(client, {"EXEC"}), "-EXECABORT", "renamed queue abort");
    peadb::set_loading(false);
    std::cout << "Renamed loading admission failures: " << failures << '\n';
    return failures ? 1 : 0;
  }
  peadb::lua_engine_init();
  peadb::lua_engine_set_dispatch([](const auto& args, auto& session, bool& close) {
    return peadb::handle_command(args, session, close);
  });
  peadb::functions_register_rdb_hooks();
  peadb::configure_security("secret", "", "", true, "no", {});
  peadb::configure_runtime_settings({{"enable-protected-configs", "yes"}}, "");
  peadb::SessionState client;
  peadb::set_loading(true);
  equal(run(client, {"GET", "k"}), "-NOAUTH Authentication required.\r\n", "auth precedence");
  equal(run(client, {"SCRIPT", "LOAD"}),
        "-ERR wrong number of arguments for 'script|load' command\r\n",
        "subcommand arity before auth");
  equal(run(client, {"AUTH", "secret"}), "+OK\r\n", "AUTH while loading");
  equal(run(client, {"GET", "k"}), kLoading, "GET while loading");
  equal(run(client, {"PING"}), kLoading, "PING uses generated flags");
  equal(run(client, {"SET", "k", "v"}), kLoading, "SET while loading");
  equal(run(client, {"GET"}), "-ERR wrong number of arguments for 'get' command\r\n",
        "arity precedence");
  equal(run(client, {"SCRIPT", "LOAD"}),
        "-ERR wrong number of arguments for 'script|load' command\r\n",
        "subcommand arity before loading");
  equal(run(client, {"XINFO", "GROUPS"}),
        "-ERR wrong number of arguments for 'xinfo|groups' command\r\n", "stream subcommand arity");
  equal(run(client, {"OBJECT", "ENCODING"}),
        "-ERR wrong number of arguments for 'object|encoding' command\r\n",
        "object subcommand arity");
  equal(run(client, {"ECHO", "ok"}), "$2\r\nok\r\n", "loading-safe command");
  equal(run(client, {"SELECT", "2"}), "+OK\r\n", "SELECT while loading");
  contains(run(client, {"INFO", "persistence"}), "loading:1\r\n", "shared INFO mode");
  equal(run(client, {"CONFIG", "SET", "appendonly", "no"}), kLoading, "no-change config");
  equal(run(client, {"CONFIG", "SET", "repl-diskless-load", "disabled"}), kLoading, "fixed config");
  equal(run(client, {"CONFIG", "SET", "appendonly", "bad"}), kLoading, "config value precedence");
  equal(run(client, {"CONFIG", "SET", "appendonly", "no", "unknown-option", "x"}), kLoading,
        "first config error");
  contains(run(client, {"CONFIG", "SET", "unknown-option", "x", "appendonly", "no"}),
           "Unknown option", "earlier unknown config");
  equal(run(client, {"CONFIG", "SET", "timeout", "7", "appendonly", "no"}), kLoading,
        "atomic config batch");
  equal(run(client, {"CONFIG", "GET", "timeout"}), "*2\r\n$7\r\ntimeout\r\n$1\r\n0\r\n",
        "refused batch unchanged");
  equal(run(client, {"MULTI"}), "+OK\r\n", "MULTI while loading");
  equal(run(client, {"GET", "k"}), kLoading, "queue admission");
  contains(run(client, {"EXEC"}), "-EXECABORT", "rejected queue aborts");
  equal(run(client, {"MULTI"}), "+OK\r\n", "safe MULTI");
  equal(run(client, {"ECHO", "ok"}), "+QUEUED\r\n", "safe queue");
  equal(run(client, {"EXEC"}), "*1\r\n$2\r\nok\r\n", "safe EXEC");
  equal(run(client, {"CONFIG", "SET", "dir", "."}), kLoading,
        "protected allowed config still loading-gated");
  equal(run(client, {"MULTI"}), "+OK\r\n", "loading config transaction");
  equal(run(client, {"CONFIG", "SET", "appendonly", "no"}), "+QUEUED\r\n",
        "config checked at execution");
  equal(run(client, {"EXEC"}), std::string("*1\r\n") + kLoading, "config handler error in EXEC");
  equal(run(client, {"MULTI"}), "+OK\r\n", "invalid subcommand transaction");
  equal(run(client, {"SCRIPT", "LOAD"}),
        "-ERR wrong number of arguments for 'script|load' command\r\n",
        "queue arity before loading");
  contains(run(client, {"EXEC"}), "-EXECABORT", "invalid subcommand aborts transaction");
  peadb::set_loading(false);
  equal(run(client, {"MULTI"}), "+OK\r\n", "queue before load");
  equal(run(client, {"SET", "k", "must-not-run"}), "+QUEUED\r\n", "queue write");
  peadb::set_loading(true);
  equal(run(client, {"EXEC"}),
        "-EXECABORT Transaction discarded because of: LOADING Redis is loading the dataset in "
        "memory\r\n",
        "EXEC inverse loading flags");
  peadb::set_loading(false);
  equal(run(client, {"GET", "k"}), "$-1\r\n", "rejected write never applied");
  // Async is an admission mode only; the transport must not enable it before staged load.
  equal(run(client, {"SET", "old", "old-value"}), "+OK\r\n", "old dataset seed");
  equal(run(client,
            {"FUNCTION", "LOAD",
             "#!lua name=oldlib\nredis.register_function{function_name='old_f',callback=function() "
             "return redis.call('get','old') end,flags={'no-writes'}}"}),
        "$6\r\noldlib\r\n", "old function seed");
  {
    const peadb::LoadingScope async(peadb::LoadingMode::Async);
    contains(run(client, {"INFO", "persistence"}), "loading:0\r\nasync_loading:1\r\n",
             "async INFO");
    equal(run(client, {"GET", "old"}), "$9\r\nold-value\r\n", "async old GET");
    equal(run(client, {"FCALL_RO", "old_f", "0"}), "$9\r\nold-value\r\n", "async old function");
    equal(run(client, {"EVAL_RO", "return redis.call('get','old')", "0"}), "$9\r\nold-value\r\n",
          "async old script");
    equal(run(client, {"SCRIPT", "LOAD"}),
          "-ERR wrong number of arguments for 'script|load' command\r\n", "async subcommand arity");
    equal(run(client, {"REPLICAOF", "NO", "ONE"}), kLoading, "async REPLICAOF");
    equal(run(client, {"SLAVEOF", "NO", "ONE"}), kLoading, "async SLAVEOF");
    equal(run(client, {"BGSAVE"}), kLoading, "async persistence");
    equal(run(client, {"CONFIG", "SET", "appendonly", "no"}), kLoading, "async per-config gate");
    equal(run(client, {"CONFIG", "SET", "repl-diskless-load", "disabled"}), kLoading,
          "async fixed config gate");
    equal(run(client, {"CONFIG", "SET", "timeout", "7"}), "+OK\r\n", "async allowed config");
    // Ordinary replica READONLY/MASTERDOWN gates keep their precedence over loading.
    peadb::configure_replication(true, "", 0);
    equal(run(client, {"SET", "old", "bad"}),
          "-READONLY You can't write against a read only replica.\r\n",
          "async readonly precedence");
    equal(run(client, {"GET", "old"}), "$9\r\nold-value\r\n", "async replica reads");
    equal(run(client, {"CONFIG", "SET", "replica-serve-stale-data", "no"}), "+OK\r\n",
          "set stale gate");
    contains(run(client, {"GET", "old"}), "-MASTERDOWN", "async stale precedence");
    equal(run(client, {"CONFIG", "SET", "replica-serve-stale-data", "yes"}), "+OK\r\n",
          "restore stale gate");
    peadb::configure_replication(false, "", 0);
    equal(run(client, {"MULTI"}), "+OK\r\n", "async MULTI");
    equal(run(client, {"REPLICAOF", "NO", "ONE"}), kLoading, "async queue restriction");
    contains(run(client, {"EXEC"}), "-EXECABORT", "async dirty EXEC");
    equal(run(client, {"MULTI"}), "+OK\r\n", "async read transaction");
    equal(run(client, {"GET", "old"}), "+QUEUED\r\n", "async read queue");
    equal(run(client, {"EXEC"}), "*1\r\n$9\r\nold-value\r\n", "async read EXEC");
    // Exceptions/nesting must restore the previous mode, including after a failed load.
    try {
      const peadb::LoadingScope blocking(peadb::LoadingMode::Blocking);
      equal(run(client, {"FCALL_RO", "old_f", "0"}), kLoading, "blocking functions");
      equal(run(client, {"EVAL", "return 1", "0"}), kLoading, "blocking scripts");
      throw std::runtime_error("cancelled load");
    } catch (const std::runtime_error&) {
    }
    equal(run(client, {"GET", "old"}), "$9\r\nold-value\r\n", "scope restores async");
  }
  contains(run(client, {"INFO", "persistence"}), "loading:0\r\nasync_loading:0\r\n",
           "scope restores idle");
  equal(run(client, {"MULTI"}), "+OK\r\n", "pre-async queue");
  equal(run(client, {"REPLICAOF", "NO", "ONE"}), "+QUEUED\r\n", "pre-async restricted command");
  {
    const peadb::LoadingScope async(peadb::LoadingMode::Async);
    equal(run(client, {"EXEC"}),
          "-EXECABORT Transaction discarded because of: LOADING Redis is loading the dataset in "
          "memory\r\n",
          "EXEC aggregate async flags");
  }
  // AOF replay bypasses client admission, including its internal MULTI/EXEC. The public
  // caller owns the loading scope; a refused/corrupt load cannot leave replay privilege set.
  char path[] = "/tmp/peadb-loading-aof-XXXXXX";
  const int fd = mkstemp(path);
  if (fd < 0) return 1;
  close(fd);
  {
    std::ofstream file(path);
    file << "*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$8\r\nfrom-aof\r\n$2\r\nok\r\n*1\r\n$"
            "4\r\nEXEC\r\n";
  }
  std::string error;
  {
    const peadb::LoadingScope blocking(peadb::LoadingMode::Blocking);
    if (!peadb::load_aof_file(path, error)) {
      ++failures;
      std::cerr << error << '\n';
    }
    equal(run(client, {"GET", "from-aof"}), kLoading, "replay privilege restored");
  }
  unlink(path);
  equal(run(client, {"SELECT", "0"}), "+OK\r\n", "AOF database");
  equal(run(client, {"GET", "from-aof"}), "$2\r\nok\r\n", "AOF applied under load");
  // Eviction does not increment mutation_epoch: retain a direct value assertion as well.

  // Redis evict.c:isSafeToPerformEvictions() disables both eviction and OOM while
  // loading (also async). INFO must not evict partially loaded or old retained keys.
  peadb::configure_maxmemory("1");
  peadb::configure_maxmemory_policy("noeviction");
  {
    const peadb::LoadingScope blocking(peadb::LoadingMode::Blocking);
    equal(run(client, {"SET", "no-oom", "x"}), kLoading, "loading suppresses OOM");
    peadb::configure_maxmemory_policy("allkeys-lru");
    const auto epoch = peadb::store().mutation_epoch();
    run(client, {"INFO", "persistence"});
    if (peadb::store().mutation_epoch() != epoch) {
      ++failures;
      std::cerr << "INFO evicted during load\n";
    }
  }
  peadb::configure_maxmemory_policy("noeviction");
  {
    const peadb::LoadingScope async(peadb::LoadingMode::Async);
    equal(run(client, {"SET", "async-write", "ok"}), "+OK\r\n", "async OOM suppression");
    equal(run(client, {"GET", "from-aof"}), "$2\r\nok\r\n",
          "loading preserves keys against eviction");
  }
  peadb::configure_maxmemory("0");
  peadb::configure_maxmemory_policy("noeviction");
  equal(run(client, {"CONFIG", "RESETSTAT"}), "+OK\r\n", "reset error counters");
  {
    const peadb::LoadingScope blocking(peadb::LoadingMode::Blocking);
    equal(run(client, {"GET", "old"}), kLoading, "count rejection");
    equal(run(client, {"CONFIG", "SET", "appendonly", "no"}), kLoading, "count handler refusal");
    contains(run(client, {"INFO", "commandstats"}),
             "cmdstat_get:calls=0,usec=0,usec_per_call=0.00,rejected_calls=1,failed_calls=0",
             "rejection stats");
    contains(run(client, {"INFO", "errorstats"}), "errorstat_LOADING:count=2",
             "LOADING error count");
  }
  equal(run(client, {"FUNCTION", "FLUSH", "SYNC"}), "+OK\r\n", "function cleanup");
  peadb::lua_engine_shutdown();
  std::cout << "Loading admission failures: " << failures << '\n';
  return failures ? 1 : 0;
}
