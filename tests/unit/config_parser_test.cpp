// Config loader tests: line splitting, settings, value checks and errors as Redis 7.2.5's
// loadServerConfigFromString() does them (src/config.c), plus the command-line conversion of
// Redis's main(). The expected error strings are Redis's own.
#include "command.hpp"
#include "config.hpp"
#include "redis_config_table.generated.hpp"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

namespace {

int g_failures = 0;

void fail(const std::string& what) {
  std::cerr << "FAIL: " << what << "\n";
  ++g_failures;
}

void expect(bool ok, const std::string& what) {
  if (!ok) fail(what);
}

peadb::ServerConfig expect_ok(const std::string& text) {
  peadb::ServerConfig cfg;
  std::string err;
  if (!peadb::load_config_string(text, cfg, err)) fail("expected OK for [" + text + "], got: " + err);
  return cfg;
}

void expect_error(const std::string& text, const std::string& needle) {
  peadb::ServerConfig cfg;
  std::string err;
  if (peadb::load_config_string(text, cfg, err)) {
    fail("expected an error containing [" + needle + "] for [" + text + "]");
  } else if (err.find(needle) == std::string::npos) {
    fail("error for [" + text + "] lacks [" + needle + "]: " + err);
  }
}

std::vector<std::string> split(const std::string& line, bool* ok = nullptr) {
  std::vector<std::string> out;
  const bool r = peadb::split_config_args(line, out);
  if (ok != nullptr) *ok = r;
  return out;
}

void test_split_args() {
  expect(split("set \"a b\" 'c d'") == std::vector<std::string>{"set", "a b", "c d"}, "quotes");
  expect(split("  x\t y  ") == std::vector<std::string>{"x", "y"}, "spaces and tabs");
  expect(split("\"\\x41\\n\\t\\\\\\\"\"") == std::vector<std::string>{"A\n\t\\\""}, "escapes");
  expect(split("'it\\'s'") == std::vector<std::string>{"it's"}, "single-quote escape");
  expect(split("a \"\"") == std::vector<std::string>{"a", ""}, "empty argument");
  expect(split("a\"b c\"") == std::vector<std::string>{"ab c"}, "quote inside a word");
  expect(split("pass#word") == std::vector<std::string>{"pass#word"}, "# inside a word");
  bool ok = true;
  split("\"abc\"def", &ok);
  expect(!ok, "closing quote followed by a letter is an error");
  split("\"abc", &ok);
  expect(!ok, "unterminated double quote");
  split("'abc", &ok);
  expect(!ok, "unterminated single quote");
}

void test_basic_settings() {
  const auto cfg = expect_ok(
      "# comment\n"
      "   # indented comment\n"
      "\n"
      "PORT 6390\n"  // names are case-insensitive
      "maxclients 4096\n"
      "bind 0.0.0.0\n"
      "loglevel debug\n"
      "cluster-enabled yes\n"
      "cluster-announce-ip 10.9.8.7\n"
      "cluster-announce-port 7100\n"
      "cluster-announce-bus-port 17100\n"
      "requirepass Str0ng#Pass-2026\n"
      "masterauth \"with space#and hash\"\n"
      "maxmemory 1gb\n"
      "repl-backlog-size 2MB\n"
      "appendonly YES\n"
      "appendfsync Always\n"
      "unixsocketperm 700\n"
      "maxmemory-samples 5000\n");
  expect(cfg.port == 6390, "port");
  expect(cfg.maxclients == 4096, "maxclients");
  expect(cfg.bind == "0.0.0.0", "bind");
  expect(cfg.log_level == "debug", "loglevel");
  expect(cfg.cluster_enabled, "cluster-enabled");
  expect(cfg.cluster_announce_ip == "10.9.8.7", "cluster-announce-ip");
  expect(cfg.cluster_announce_port == "7100", "cluster-announce-port");
  expect(cfg.cluster_announce_bus_port == "17100", "cluster-announce-bus-port");
  expect(cfg.requirepass == "Str0ng#Pass-2026", "a # inside a value is kept");
  expect(cfg.masterauth == "with space#and hash", "quoted value");
  expect(cfg.maxmemory == "1073741824", "maxmemory unit");
  expect(cfg.repl_backlog_size == "2097152", "repl-backlog-size unit");
  expect(cfg.appendonly, "appendonly YES");
  expect(cfg.appendfsync == "always", "appendfsync is canonical lower case");
  expect(cfg.unixsocketperm == 0700, "unixsocketperm is octal");
  expect(cfg.maxmemory_samples == "5000", "maxmemory-samples up to INT_MAX");
  expect(cfg.warnings.empty(), "no warnings for applied settings");
}

void test_aliases_last_wins() {
  auto cfg = expect_ok("hash-max-ziplist-entries 10\nhash-max-listpack-entries 20\n");
  expect(cfg.raw["hash-max-listpack-entries"] == "20", "alias folded, last wins (listpack last)");
  cfg = expect_ok("hash-max-listpack-entries 20\nhash-max-ziplist-entries 10\n");
  expect(cfg.raw["hash-max-listpack-entries"] == "10", "alias folded, last wins (ziplist last)");
  cfg = expect_ok("slave-serve-stale-data no\nmin-slaves-to-write 2\nlua-time-limit 100\n");
  expect(cfg.raw["replica-serve-stale-data"] == "no", "slave-serve-stale-data alias");
  expect(cfg.raw["min-replicas-to-write"] == "2", "min-slaves-to-write alias");
  expect(cfg.raw["busy-reply-threshold"] == "100", "lua-time-limit alias");
  cfg = expect_ok("port 1\nport 2\n");
  expect(cfg.port == 2, "last port wins");
}

void test_line_errors() {
  expect_error("port 6379\nport 6380 # main port\n", "wrong number of arguments");
  expect_error("port 6379\nport 6380 # main port\n", "at line 2\n>>> 'port 6380 # main port'");
  expect_error("port 6379\nport 6380 # main port\n", "*** FATAL CONFIG FILE ERROR");
  expect_error("no-such-setting yes\n", "Bad directive or wrong number of arguments");
  expect_error("requirepass \"abc\n", "Unbalanced quotes in configuration line");
  expect_error("port\n", "wrong number of arguments");
  expect_error("rename-command DEBUG\n", "Bad directive or wrong number of arguments");
  expect_error("include\n", "Bad directive or wrong number of arguments");
}

void test_value_checks() {
  const auto replication = expect_ok("repl-timeout 7\nrepl-ping-slave-period 3\n");
  expect(replication.warnings.empty(), "replication liveness settings are applied, not warned");
  expect(replication.raw.at("repl-timeout") == "7" &&
             replication.raw.at("repl-ping-replica-period") == "3",
         "replication liveness config and alias values");
  expect_error("appendonly maybe\n", "argument must be 'yes' or 'no'");
  expect_error("port 70000\n", "argument must be between 0 and 65535 inclusive");
  expect_error("port 6379x\n", "argument couldn't be parsed into an integer");
  expect_error("port 07\n", "argument couldn't be parsed into an integer");
  expect_error("maxclients 0\n", "argument must be between 1 and 4294967295 inclusive");
  expect_error("maxmemory 1x\n", "argument must be a memory value");
  expect_error("maxmemory -1\n", "argument must be a memory value");
  expect_error("unixsocketperm 800\n", "argument couldn't be parsed as an octal number");
  expect_error("unixsocketperm 1000\n", "argument must be between 0 and 777 inclusive");
  expect_error("loglevel info\n",
               "argument(s) must be one of the following: debug, verbose, notice, warning, nothing");
  expect_error("maxmemory-policy lru\n", "argument(s) must be one of the following: volatile-lru");
  expect_error("dbfilename dir/dump.rdb\n", "dbfilename can't be a path, just a filename");
  expect_error("appendfilename \"\"\n", "appendfilename can't be empty");
  expect_error("notify-keyspace-events KEX\n", "Invalid event class character");
  expect_error("save 900\n", "Invalid save parameters");
  expect_error("save 0 1\n", "Invalid save parameters");
  expect_error("client-output-buffer-limit master 0 0 0\n", "Invalid client class specified");
  expect_error("client-output-buffer-limit normal 1x 0 0\n", "Error in hard, soft or soft_seconds");
  expect_error("shutdown-on-sigterm save nosave\n", "SAVE and NOSAVE can't be used simultaneously");
  expect_error("dir /no/such/dir/for/peadb\n", std::strerror(ENOENT));
  // PeaDB has no defragmentation, so it answers like a Redis built without HAVE_DEFRAG.
  expect_error("activedefrag yes\n", "Active defragmentation cannot be enabled");
  expect_error("maxmemory-clients 200%\n", "percentage argument must be less or equal to 100");
  expect_error("cluster-announce-hostname bad_host\n", "Hostnames may only contain");
  // Values of settings PeaDB does not apply are still checked like Redis.
  expect_error("hz abc\n", "argument couldn't be parsed into an integer");
  expect_error("daemonize maybe\n", "argument must be 'yes' or 'no'");
}

void test_save_and_buffer_limits() {
  auto cfg = expect_ok("save 900 1\nsave 300 10\n");
  expect(cfg.save == "900 1 300 10", "save lines add up: [" + cfg.save + "]");
  cfg = expect_ok("save 900 1\nsave \"\"\n");
  expect(cfg.save.empty(), "save \"\" clears");
  cfg = expect_ok("save \"\"\nsave 60 5\n");
  expect(cfg.save == "60 5", "save after clearing");
  cfg = expect_ok("save \"60 5 30 0\"\n");
  expect(cfg.save == "60 5 30 0", "one quoted save argument is split");
  cfg = expect_ok("client-output-buffer-limit normal 0 0 0\nclient-output-buffer-limit slave 256mb 64mb 60\n");
  expect(cfg.client_output_buffer_limit == "normal 0 0 0 replica 268435456 67108864 60",
         "client-output-buffer-limit lines add up: [" + cfg.client_output_buffer_limit + "]");
}

void test_rename_command() {
  auto cfg = expect_ok("rename-command DEBUG \"\"\nrename-command flushdb FLUSHDB_RENAMED\n");
  expect(cfg.rename_commands.size() == 2, "two renames");
  expect(cfg.rename_commands[0].from == "DEBUG" && cfg.rename_commands[0].to.empty(),
         "rename-command X \"\" removes the command");
  expect(cfg.rename_commands[1].to == "FLUSHDB_RENAMED", "rename target");
  expect(cfg.rename_commands[1].origin.find("at line 2") != std::string::npos, "rename origin");
}

void test_refused_and_ignored() {
  expect_error("user default on >secret ~* +@all\n", "PeaDB has no ACL users");
  expect_error("aclfile /etc/redis/users.acl\n", "cannot load an ACL file");
  expect_error("loadmodule /x.so\n", "PeaDB cannot load Redis modules");
  expect_error("mymodule.setting 1\n", "Module Configuration detected without loadmodule");
  expect_error("tls-port 6380\n", "Failed finding TLS support");
  expect_error("tls-replication yes\n", "Failed finding TLS support");
  expect_error("tls-cluster yes\n", "Failed finding TLS support");
  auto cfg = expect_ok("replicaof 10.0.0.1 6379\n");
  expect(cfg.master_host == "10.0.0.1" && cfg.master_port == 6379, "configured master");
  cfg = expect_ok("slaveof 10.0.0.2 6380\n");
  expect(cfg.master_host == "10.0.0.2" && cfg.master_port == 6380, "slaveof alias");
  cfg = expect_ok("replicaof \"\" 0\n");
  expect(cfg.replica && cfg.master_host.empty(), "an empty host is still a configured replica");
  expect_error("replicaof 10.0.0.1 nope\n", "Invalid master port");
  expect_error("masteruser repl\n", "named ACL user");
  expect_error("enable-debug-command no\n", "rename-command DEBUG");
  expect_error("sentinel monitor m 127.0.0.1 6379 2\n", "sentinel directive while not in sentinel mode");
  expect_error("bind 127.0.0.1 -::1\n", "exactly one bind address");

  // Redis settings whose value is what PeaDB always does: accepted with no warning.
  cfg = expect_ok(
      "daemonize no\ndatabases 16\nlogfile \"\"\nsupervised no\ntls-port 0\ntls-replication no\n"
      "aclfile \"\"\nreplicaof no one\nenable-debug-command yes\nrdbcompression no\n"
      "list-max-ziplist-entries 5\nsentinel\n");
  expect(cfg.warnings.empty(), "no warning when the value matches PeaDB");
  // Settings PeaDB applies: no warning either.
  cfg = expect_ok("shutdown-on-sigterm nosave now\nshutdown-on-sigint force\nrdb-key-save-delay 10\n");
  expect(cfg.warnings.empty(), "no warning for the shutdown-on-sig* and rdb-key-save-delay settings");

  // Any other Redis setting PeaDB does not apply: accepted with one warning per line.
  cfg = expect_ok("hz 10\ndaemonize yes\npidfile /var/run/redis.pid\ntls-cert-file /x.crt\n");
  expect(cfg.warnings.size() == 4, "one warning per ignored setting");
  expect(!cfg.warnings.empty() && cfg.warnings[0].find("config line 1: 'hz'") != std::string::npos,
         "warning names the line and the setting");
  expect(!cfg.warnings.empty() && cfg.warnings[3].find("/x.crt") == std::string::npos,
         "warnings never print values");
}

void test_include() {
  const auto dir = std::filesystem::temp_directory_path() /
                   ("peadb_config_test_" + std::to_string(::getpid()));
  std::filesystem::create_directories(dir / "conf.d");
  const auto inc = dir / "inc.conf";
  std::ofstream(inc) << "port 7001\nmaxclients 77\n";
  std::ofstream(dir / "conf.d" / "a.conf") << "timeout 30\n";

  auto cfg = expect_ok("port 6000\ninclude " + inc.string() + "\nmaxclients 88\n");
  expect(cfg.port == 7001 && cfg.maxclients == 88, "include is applied in place");
  cfg = expect_ok("include " + (dir / "conf.d" / "*.conf").string() + "\n");
  expect(cfg.timeout == "30", "include with a glob");
  cfg = expect_ok("include " + (dir / "empty.d" / "*.conf").string() + "\n");
  expect(cfg.warnings.empty(), "a glob that matches nothing is skipped");
  expect_error("include " + (dir / "missing.conf").string() + "\n",
               "Fatal error, can't open config file '" + (dir / "missing.conf").string() + "'");
  std::ofstream(dir / "bad.conf") << "port 1\nnot-a-setting 2\n";
  expect_error("include " + (dir / "bad.conf").string() + "\n", "at line 2\n>>> 'not-a-setting 2'");
  std::ofstream(dir / "loop.conf") << "include " << (dir / "loop.conf").string() << "\n";
  expect_error("include " + (dir / "loop.conf").string() + "\n", "include nesting is deeper");

  peadb::ServerConfig missing;
  std::string err;
  expect(!peadb::load_server_config((dir / "nope.conf").string(), false, "", missing, err) &&
             err.find("Fatal error, can't open config file") != std::string::npos,
         "a missing config file is an error: " + err);
  std::filesystem::remove_all(dir);
}

peadb::ServerConfig load_argv(const std::vector<std::string>& argv, std::string* error = nullptr) {
  bool from_stdin = false;
  const auto options = peadb::config_options_from_argv(argv, 1, from_stdin);
  peadb::ServerConfig cfg;
  std::string err;
  const bool ok = peadb::load_server_config("", false, options, cfg, err);
  if (error != nullptr) *error = ok ? "" : err;
  else if (!ok) fail("command line failed: " + err);
  return cfg;
}

void test_command_line() {
  bool from_stdin = false;
  expect(peadb::config_options_from_argv({"peadb-server", "--port", "6380", "--save", ""}, 1,
                                         from_stdin) == "port \"6380\" \nsave \"\" ",
         "options text");
  auto cfg = load_argv({"peadb-server", "--port", "6380", "--maxclients", "5", "--save"});
  expect(cfg.port == 6380 && cfg.maxclients == 5 && cfg.save.empty(), "--port --maxclients --save");
  cfg = load_argv({"peadb-server", "--save", "--port", "6381"});
  expect(cfg.port == 6381 && cfg.save.empty(), "--save followed by an option means --save \"\"");
  cfg = load_argv({"peadb-server", "--save", "900", "1", "300", "10"});
  expect(cfg.save == "900 1 300 10", "--save with several values: [" + cfg.save + "]");
  cfg = load_argv({"peadb-server", "--port 6390"});
  expect(cfg.port == 6390, "name and value in one argument");
  cfg = load_argv({"peadb-server", "--requirepass", "--starts-with-dashes"});
  expect(cfg.requirepass == "--starts-with-dashes", "a value may start with --");
  cfg = load_argv({"peadb-server", "--requirepass", "a \"quoted\" pass\\word"});
  expect(cfg.requirepass == "a \"quoted\" pass\\word", "values are quoted and unquoted exactly");
  cfg = load_argv({"peadb-server", "--rename-command", "DEBUG", ""});
  expect(cfg.rename_commands.size() == 1 && cfg.rename_commands[0].to.empty(),
         "--rename-command DEBUG \"\"");
  std::string err;
  load_argv({"peadb-server", "--tls-port", "6380"}, &err);
  expect(err.find("Failed finding TLS support") != std::string::npos, "--tls-port is refused");
  load_argv({"peadb-server", "--loglevel", "error"}, &err);
  expect(err.find("must be one of the following") != std::string::npos, "--loglevel error is refused");
  load_argv({"peadb-server", "--no-such-option", "1"}, &err);
  expect(err.find("Bad directive") != std::string::npos, "unknown option is refused");
  from_stdin = false;
  peadb::config_options_from_argv({"peadb-server", "-"}, 1, from_stdin);
  expect(from_stdin, "- reads the config from stdin");
}

// Review findings (2026-10-05): each of these differed from Redis before the fix.
void test_last_value_wins() {
  // A refusal or warning is decided on the final value: a later line or option can undo it.
  auto cfg = expect_ok("bind 127.0.0.1 -::1\nbind 0.0.0.0\n");
  expect(cfg.bind == "0.0.0.0", "a later single bind replaces a refused multi-address bind");
  cfg = expect_ok("replicaof 10.0.0.1 6379\nreplicaof no one\n");
  expect(!cfg.replica && cfg.master_host.empty() && cfg.master_port == 0,
         "no one clears configured role");
  expect_ok("tls-port 6380\ntls-port 0\n");
  expect_ok("enable-debug-command no\nenable-debug-command yes\n");
  cfg = expect_ok("replicaof no one\nreplicaof 10.0.0.1 6379\n");
  expect(cfg.master_host == "10.0.0.1" && cfg.master_port == 6379, "last replicaof wins");
  expect_error("replicaof 127.0.0.1 6379\ncluster-enabled yes\n",
               "replicaof directive not allowed in cluster mode");
  expect_error("cluster-enabled yes\nslaveof 127.0.0.1 6379\n",
               "replicaof directive not allowed in cluster mode");
  expect_ok("cluster-enabled yes\nreplicaof 127.0.0.1 6379\nreplicaof no one\n");
  cfg = expect_ok("daemonize yes\ndaemonize no\n");
  expect(cfg.warnings.empty(), "no warning when the final value is what PeaDB does");
  cfg = expect_ok("hz 10\nhz 20\n");
  expect(cfg.warnings.size() == 1 && cfg.warnings[0].find("config line 2") != std::string::npos,
         "one warning per setting, naming its last line");
  std::string err;
  peadb::ServerConfig c2;
  bool from_stdin = false;
  const auto opts = peadb::config_options_from_argv({"peadb-server", "--bind", "0.0.0.0"}, 1, from_stdin);
  expect(peadb::load_config_string("bind 127.0.0.1 -::1\n" + opts, c2, err) && c2.bind == "0.0.0.0",
         "stock redis.conf bind line + --bind 0.0.0.0 starts: " + err);
}

void test_review_save_and_renames() {
  auto cfg = expect_ok("save 3600 0\n");
  expect(cfg.save == "3600 0", "save with 0 changes is kept: [" + cfg.save + "]");
  cfg = expect_ok("save 900 4294967297\n");
  expect(cfg.save == "900 1", "the change count is an int, as in Redis: [" + cfg.save + "]");
  const auto dir = std::filesystem::temp_directory_path() /
                   ("peadb_config_inc_" + std::to_string(::getpid()));
  std::filesystem::create_directories(dir);
  std::ofstream(dir / "a.conf") << "port 7002\n";
  cfg = expect_ok("include " + (dir / "a.conf").string() + "\nsave 900 1\nsave 300 10\n");
  expect(cfg.save == "300 10", "after an include each save line replaces the list, as in Redis: [" +
                                   cfg.save + "]");
  std::filesystem::remove_all(dir);

  expect_error("rename-command FLUSHALL \"\"\nrename-command FLUSHALL \"\"\n",
               "at line 2\n>>> 'rename-command FLUSHALL \"\"'\nNo such command in rename-command");
  expect_error("rename-command CONFIG \"\"\nrename-command CONFIG x\n", "No such command in rename-command");
  expect_error("rename-command SENTINEL \"\"\n", "No such command in rename-command");
  expect_error("rename-command GET LOLWUT\n", "Target command name already exists");
  expect_ok("rename-command GET get\n");  // removed first, then added back: allowed, as in Redis
  cfg = expect_ok("rename-command GET GET\nrename-command FAILOVER secret-name\nrename-command secret-name \"\"\n");
  expect(cfg.rename_commands.size() == 3 && cfg.rename_commands[2].redis_name == "failover",
         "renames chain through Redis's table");
  // The first bad line is reported, whether it is a rename or any other line.
  expect_error("rename-command NOPE x\nno-such-setting 1\n", "at line 1");
}

void test_review_nul_and_post_checks() {
  const auto dir = std::filesystem::temp_directory_path() /
                   ("peadb_config_nul_" + std::to_string(::getpid()));
  std::filesystem::create_directories(dir);
  {
    // NUL padding at the end of the file (as after a power loss) drops only the padding, so
    // command-line options after it still apply.
    std::ofstream out(dir / "pad.conf", std::ios::binary);
    out << "port 7003\ntimeout 5\n" << std::string(4000, '\0');
  }
  {
    // A NUL inside a line drops the rest of that fgets() read, newline included, so the next
    // line is joined to it, as in Redis.
    std::ofstream out(dir / "mid.conf", std::ios::binary);
    out << "port 7003\n" << std::string("timeout 5\0garbage", 17) << "\nmaxclients 66\n";
  }
  peadb::ServerConfig cfg;
  std::string err;
  bool from_stdin = false;
  const auto opts = peadb::config_options_from_argv({"peadb-server", "--requirepass", "s3cret"}, 1, from_stdin);
  expect(peadb::load_server_config((dir / "pad.conf").string(), false, opts, cfg, err), err);
  expect(cfg.port == 7003 && cfg.timeout == "5" && cfg.requirepass == "s3cret",
         "options after NUL padding still apply");
  peadb::ServerConfig cfg2;
  expect(!peadb::load_server_config((dir / "mid.conf").string(), false, "", cfg2, err) &&
             err.find(">>> 'timeout 5maxclients 66'\nwrong number of arguments") != std::string::npos,
         "a NUL inside a line joins the next line, as in Redis: " + err);
  expect_error("logfile /no/such/dir/peadb.log\n", "Can't open the log file: ");
  expect_error("locale-collate no_such_locale_xyz\n", "Failed to configure LOCALE for invalid locale name.");
  std::filesystem::remove_all(dir);
}

// Ties the generated table to the loader: a Redis pin bump that adds a validator or a special
// setting must be handled (or listed here on purpose), not silently skipped.
void test_generated_table() {
  const std::vector<std::string> handled_validators = {
      "isValidDBfilename", "isValidAOFfilename", "isValidAOFdirname", "isValidAnnouncedHostname",
      "isValidAnnouncedNodename", "isValidShutdownOnSigFlags",
      "isValidActiveDefrag",
      // Not checked on purpose: PeaDB sets no process title, so the setting is accepted with
      // a warning whatever its value.
      "isValidProcTitleTemplate"};
  const auto cwd = std::filesystem::current_path();
  for (const auto& spec : peadb::kRedisConfigSpecs) {
    if (spec.is_valid != nullptr &&
        std::find(handled_validators.begin(), handled_validators.end(), spec.is_valid) ==
            handled_validators.end()) {
      fail(std::string("validator not handled: ") + spec.is_valid + " (" + spec.name + ")");
    }
    if (spec.type == peadb::RedisConfigType::Special) {
      peadb::ServerConfig cfg;
      std::string err;
      peadb::load_config_string(std::string(spec.name) + " x\n", cfg, err);
      if (err.find("unhandled special config") != std::string::npos) {
        fail(std::string("special setting not handled: ") + spec.name);
      }
    }
    // Every setting but dir (the working directory) has the default CONFIG GET prints.
    expect((spec.default_value == nullptr) == (std::string(spec.name) == "dir"),
           std::string("default value of ") + spec.name);
  }
  std::filesystem::current_path(cwd);  // in case `dir x` found a directory called x
  expect(peadb::kRedisConfigSpecCount == 183, "Redis 7.2.5 has 183 settings in a release build");
  auto flags = [](const char* name) { return peadb::find_redis_config(name)->flags; };
  expect((flags("daemonize") & peadb::kCfgImmutable) != 0, "daemonize is immutable");
  expect((flags("dir") & peadb::kCfgProtected) != 0, "dir is protected");
  expect((flags("dbfilename") & peadb::kCfgProtected) != 0, "dbfilename is protected");
  expect((flags("rdb-key-save-delay") & peadb::kCfgHidden) != 0, "rdb-key-save-delay is hidden");
  expect((flags("requirepass") & peadb::kCfgSensitive) != 0, "requirepass is sensitive");
  expect(flags("maxmemory") == 0, "maxmemory has no flag CONFIG acts on");
  expect(std::string(peadb::find_redis_config("save")->default_value) == "3600 1 300 100 60 10000",
         "save default");
  expect(std::string(peadb::find_redis_config("proto-max-bulk-len")->default_value) == "536870912",
         "numeric default is evaluated");
  expect(std::string(peadb::find_redis_config("loglevel")->default_value) == "notice",
         "enum default is the enum name");
}

// The checks and value forms CONFIG SET shares with the loader.
void test_config_set_values() {
  using peadb::config_set_args;
  const auto* save = peadb::find_redis_config("SAVE");
  expect(save != nullptr && std::string(save->name) == "save", "lookup ignores case");
  const auto* alias = peadb::find_redis_config("slave-serve-stale-data");
  expect(alias != nullptr && std::string(alias->name) == "replica-serve-stale-data",
         "lookup finds an alias");
  expect(peadb::find_redis_config("list-max-ziplist-entries") == nullptr,
         "a deprecated name is not a setting");
  expect(config_set_args(*save, "900 1") == std::vector<std::string>{"900", "1"},
         "split at spaces");
  expect(config_set_args(*save, "900  1") == std::vector<std::string>{"900", "", "1"},
         "every single space splits");
  expect(config_set_args(*save, "").empty(), "an empty value has no arguments");
  expect(config_set_args(*peadb::find_redis_config("requirepass"), "a b") ==
             std::vector<std::string>{"a b"},
         "a single-argument setting keeps its value whole");
  auto value = [](const char* name, const std::string& v) {
    const auto* spec = peadb::find_redis_config(name);
    std::string out, err;
    if (!peadb::check_redis_config_value(*spec, config_set_args(*spec, v), out, err)) {
      return "ERR " + err;
    }
    return out;
  };
  expect(value("unixsocketperm", "700") == "700", "octal is printed in base 8");
  expect(value("maxmemory", "1kb") == "1024", "memory is printed in bytes");
  expect(value("maxmemory-clients", "10%") == "10%", "percent is printed as N%");
  expect(value("appendonly", "YES") == "yes", "bool is lower case");
  expect(value("maxmemory-policy", "ALLKEYS-LRU") == "allkeys-lru", "enum is lower case");
  expect(value("shutdown-on-sigterm", "now save") == "save now", "flags print in enum order");
  expect(value("shutdown-on-sigterm", "default save") == "save", "default is only printed alone");
  // The checked value is what CONFIG SET hands to the live setting, and CONFIG GET prints it back.
  for (const char* v : {"default", "save", "nosave now", "save now force", "nosave now force"}) {
    expect(peadb::shutdown_flags_to_str(peadb::parse_shutdown_flags(value("shutdown-on-sigint", v))) ==
               v,
           std::string("shutdown flags round trip: ") + v);
  }
  expect(peadb::parse_shutdown_flags("nosave force") ==
             (peadb::kShutdownNosave | peadb::kShutdownForce),
         "shutdown flags are Redis's SHUTDOWN_* bits");
  expect(value("latency-tracking-info-percentiles", "50.0 99.90") == "50 99.9",
         "percentiles print like %f without trailing zeros");
  expect(value("save", "100  10").rfind("ERR Invalid save parameters", 0) == 0,
         "an empty middle argument is not a number");
  expect(value("activedefrag", "yes").rfind("ERR Active defragmentation", 0) == 0,
         "activedefrag yes is refused");
  const auto* fixed = peadb::peadb_fixed_setting("rdbcompression");
  expect(fixed != nullptr && std::string(fixed->value) == "no" && fixed->refuse == nullptr,
         "rdbcompression is fixed at no");
  expect(peadb::peadb_fixed_setting("hz") == nullptr, "hz has no fixed value");
}

// `dir` changes the working directory while the config is read, as Redis's chdir(2) does.
void test_dir_changes_working_directory() {
  const auto cwd = std::filesystem::current_path();
  const auto dir = std::filesystem::temp_directory_path() /
                   ("peadb-config-dir-" + std::to_string(::getpid()));
  std::filesystem::create_directories(dir);
  expect_ok("dir " + dir.string() + "\n");
  expect(std::filesystem::equivalent(std::filesystem::current_path(), dir),
         "dir is now the working directory");
  std::filesystem::current_path(cwd);
  std::filesystem::remove_all(dir);
}

}  // namespace

int main() {
  test_split_args();
  test_basic_settings();
  test_aliases_last_wins();
  test_line_errors();
  test_value_checks();
  test_save_and_buffer_limits();
  test_rename_command();
  test_refused_and_ignored();
  test_include();
  test_command_line();
  test_generated_table();
  test_last_value_wins();
  test_review_save_and_renames();
  test_review_nul_and_post_checks();
  test_config_set_values();
  test_dir_changes_working_directory();
  if (g_failures != 0) {
    std::cerr << g_failures << " config_parser_test check(s) failed\n";
    return 1;
  }
  std::cout << "config_parser_test passed\n";
  return 0;
}
