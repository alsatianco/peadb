#include "config.hpp"
#include "datastore.hpp"
#include "logger.hpp"
#include "command.hpp"
#include "lua_engine.hpp"
#include "peadb_version.hpp"
#include "rdb.hpp"
#include "server.hpp"

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <filesystem>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <execinfo.h>
#include <unistd.h>

// Fatal-signal crash report: dump signal, faulting address, and a backtrace to
// stderr so a production segfault leaves an actionable artifact (Redis does the
// same). SA_RESETHAND restores the default handler, so the trailing raise() then
// produces a core dump as usual.
static void crash_handler(int sig, siginfo_t* info, void*) {
  char hdr[160];
  const int n = std::snprintf(hdr, sizeof(hdr),
      "\n=== PeaDB crashed: signal %d (%s), fault addr %p ===\n",
      sig, strsignal(sig), info ? info->si_addr : nullptr);
  if (n > 0) { ssize_t w = ::write(STDERR_FILENO, hdr, static_cast<size_t>(n)); (void)w; }
  void* frames[64];
  const int nframes = backtrace(frames, 64);
  backtrace_symbols_fd(frames, nframes, STDERR_FILENO);
  ::raise(sig);  // re-raise with default disposition (SA_RESETHAND) for the core dump
}

static void install_crash_handlers() {
  struct sigaction sa{};
  sa.sa_sigaction = crash_handler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = SA_SIGINFO | SA_RESETHAND;
  for (int sig : {SIGSEGV, SIGABRT, SIGBUS, SIGFPE, SIGILL}) {
    sigaction(sig, &sa, nullptr);
  }
  std::set_terminate([]() {
    const char msg[] = "\n=== PeaDB std::terminate (unhandled exception) ===\n";
    ssize_t w = ::write(STDERR_FILENO, msg, sizeof(msg) - 1); (void)w;
    std::abort();  // → SIGABRT → crash_handler dumps the backtrace
  });
}

int main(int argc, char** argv) {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGHUP, SIG_IGN);  // as in Redis (initServer, server.c)
  {
    // SIGTERM and SIGINT shut down like the SHUTDOWN command, from the event loop (Redis's
    // setupSignalHandlers()). SA_RESTART, unlike Redis, so a signal does not make a blocking call
    // in another thread fail with EINTR; the event loop wakes at least every 100 ms anyway.
    struct sigaction sa{};
    sa.sa_handler = peadb::shutdown_signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT, &sa, nullptr);
  }
  install_crash_handlers();

  // Command line, as in Redis's main() (server.c): -v/--version or -h/--help as the first
  // argument; otherwise an optional config file as the first argument (when it does not start
  // with '-'), then `--name value...` options that are appended to the config text and parsed
  // with it, so `--port 6380` means the same as a `port 6380` line, the last value wins, and an
  // option Redis does not know stops startup. "-" as the first or last argument reads the config
  // from stdin. PeaDB adds one option of its own, `--config <path>`, another way to name the
  // config file (kept for existing scripts; Redis has no setting called "config").
  std::vector<std::string> args(argv, argv + argc);
  if (args.size() >= 2 && (args[1] == "-v" || args[1] == "--version")) {
    std::cout << "PeaDB server v=" PEADB_VERSION " (Redis 7.2.5 compatible)\n";
    return 0;
  }
  if (args.size() >= 2 && (args[1] == "-h" || args[1] == "--help")) {
    std::cout << "Usage: peadb-server [/path/to/redis.conf] [options] [-]\n"
                 "       peadb-server - (read config from stdin)\n"
                 "       peadb-server -v or --version\n"
                 "       peadb-server -h or --help\n"
                 "       peadb-server --config /path/to/redis.conf [options]\n\n"
                 "Options are Redis config settings with a leading --, for example:\n"
                 "       peadb-server --port 7777\n"
                 "       peadb-server /etc/redis/6379.conf --loglevel verbose\n"
                 "       peadb-server --save \"\" --appendonly yes --requirepass secret\n";
    return 0;
  }
  // Redis enters Sentinel mode for a --sentinel argument or a redis-sentinel program name
  // (checkForSentinelMode(), server.c); PeaDB has no Sentinel, so it must not start as a data
  // server instead.
  if (std::string(args[0]).find("redis-sentinel") != std::string::npos ||
      std::find(args.begin() + 1, args.end(), "--sentinel") != args.end()) {
    std::cerr << peadb::format_config_error(
        "", "PeaDB has no Sentinel mode (compat/delta.md item 8); run Redis Sentinel itself");
    return 1;
  }
  std::string config_path;
  for (std::size_t i = 1; i < args.size(); ++i) {
    if (args[i] == "--config") {
      if (i + 1 >= args.size()) {
        std::cerr << peadb::format_config_error("", "--config needs a file name");
        return 1;
      }
      config_path = args[i + 1];
      args.erase(args.begin() + static_cast<std::ptrdiff_t>(i),
                 args.begin() + static_cast<std::ptrdiff_t>(i) + 2);
      break;
    }
  }
  std::size_t first_option = 1;
  if (args.size() >= 2 && !args[1].empty() && args[1][0] != '-') {
    if (!config_path.empty()) {
      std::cerr << peadb::format_config_error(
          "", "two config files given: '" + args[1] + "' and --config '" + config_path + "'");
      return 1;
    }
    config_path = args[1];
    first_option = 2;
  }
  bool config_from_stdin = false;
  const std::string options =
      peadb::config_options_from_argv(args, first_option, config_from_stdin);

  peadb::ServerConfig config;
  std::string config_error;
  if (!peadb::load_server_config(config_path, config_from_stdin, options, config, config_error)) {
    std::cerr << config_error;
    if (config_error.empty() || config_error.back() != '\n') std::cerr << '\n';
    return 1;
  }

  peadb::configure_log_level(config.log_level);
  // Startup log contract (relied on by the upstream Redis test harness): a line
  // containing " PID: <pid>", then "Server initialized", then "Ready to accept"
  // once the listeners are up (logged in run_server).
  peadb::log(peadb::LogLevel::Info,
             "PeaDB " PEADB_VERSION " starting, version 7.2.5-compat, PID: " +
                 std::to_string(getpid()));
  for (const auto& w : config.warnings) peadb::log(peadb::LogLevel::Warn, w);
  peadb::g_server_start_time.store(peadb::DataStore::now_ms());
  peadb::configure_runtime_port(config.port);
  peadb::configure_max_clients(config.maxclients);
  {
    const std::string aof_error = peadb::configure_persistence(
        config.dbfilename, config.appendonly, config.appendfilename, config.appendfsync);
    if (!aof_error.empty()) {
      peadb::log(peadb::LogLevel::Error, "FATAL: " + aof_error);
      return 1;
    }
  }
  // Must run before the first command_table() build (AOF replay below builds it) so
  // rename-command directives take effect on the live table.
  peadb::configure_security(config.requirepass, config.masterauth, config.protected_mode,
                            config.enable_module_command, config.rename_commands);
  peadb::init_replication_id();   // unique per-process replid (PSYNC2 safety)
  peadb::configure_maxmemory(config.maxmemory);
  peadb::configure_maxmemory_policy(config.maxmemory_policy);
  peadb::configure_maxmemory_samples(config.maxmemory_samples);
  peadb::configure_timeout(config.timeout);
  peadb::configure_tcp_backlog(config.tcp_backlog);
  peadb::configure_encoding_limits(config.raw);
  peadb::configure_runtime_settings(config.raw, config_path);
  peadb::configure_client_output_buffer_limit(config.client_output_buffer_limit);
  peadb::configure_save(config.save);
  peadb::configure_repl_backlog_size(config.repl_backlog_size);
  peadb::configure_notify_keyspace_events(config.notify_keyspace_events);
  peadb::configure_cluster(config.cluster_enabled, config.bind, config.port, config.cluster_announce_ip,
                           config.cluster_announce_port, config.cluster_announce_bus_port);
  // Builds the command table, so every configure_* call that it depends on must come first.
  {
    std::vector<std::string> rename_warnings;
    const std::string rename_error = peadb::validate_command_renames(rename_warnings);
    if (!rename_error.empty()) {
      std::cerr << rename_error;
      return 1;
    }
    for (const auto& w : rename_warnings) peadb::log(peadb::LogLevel::Warn, w);
  }

  // Cluster addressing hazard: PeaDB falls back to the static `bind` address when no
  // cluster-announce-ip is set, which is a documented simplification of Redis's live-connection
  // auto-detection (docs/decisions/p5-cluster-addressing-and-version-provenance.md) — it does NOT
  // match Redis, which instead blanks its address to force auto-detection (cluster.c:898-916).
  // That fallback has a hole: a wildcard bind (0.0.0.0, ::, *, or empty — e.g. the shipped
  // Dockerfile's `--bind 0.0.0.0`) would announce an address no peer can dial. Refuse to start
  // rather than announce it, and never silently substitute loopback either.
  //
  // The guard checks the resolved announced host unconditionally — NOT gated on whether
  // cluster-announce-ip was left empty — because the same hazard exists when an operator
  // explicitly sets `--cluster-announce-ip 0.0.0.0`: cluster_announced_host() returns the
  // explicit value verbatim in that case, and an explicit wildcard is exactly as undialable by
  // a peer as an unset one that fell back to a wildcard bind. Gating on "was announce-ip left
  // empty" only caught the fallback path and let an explicit wildcard sail through.
  if (config.cluster_enabled) {
    const std::string announced_host = peadb::cluster_announced_host();
    if (peadb::is_wildcard_announce_host(announced_host)) {
      const std::string source = config.cluster_announce_ip.empty()
          ? "'bind' ('" + config.bind + "')"
          : "the explicit 'cluster-announce-ip' ('" + config.cluster_announce_ip + "')";
      peadb::log(peadb::LogLevel::Error,
          "FATAL: cluster-enabled is yes but " + source + " resolves to a wildcard address "
          "that no peer can dial — refusing to start. Set 'cluster-announce-ip' "
          "(--cluster-announce-ip) to a real, peer-reachable address.");
      return 1;
    }
  }

  // Initialize the Lua scripting engine and wire the command dispatcher.
  peadb::lua_engine_init();
  peadb::lua_engine_set_dispatch(
      [](const std::vector<std::string>& args, peadb::SessionState& session, bool& close) -> std::string {
        return peadb::handle_command(args, session, close);
      });
  // Wire RDB save/load to the function-library registry (must precede the RDB load below).
  peadb::functions_register_rdb_hooks();

  peadb::log(peadb::LogLevel::Info, "Server initialized");

  // The config loader has made `dir` the working directory, as Redis does.
  const std::filesystem::path snap = config.dbfilename;
  peadb::set_loading(true);
  // Like Redis's loadDataFromDisk(), load the AOF or the RDB, never both: with the AOF on it holds
  // every write, so loading the RDB under it would apply each write twice (compat/delta.md item
  // 28). The RDB is not read even when the AOF is empty or new, as in Redis.
  if (config.appendonly) {
    const std::filesystem::path aof = config.appendfilename;
    std::error_code ec;
    if (std::filesystem::file_size(aof, ec) == 0 && !ec && std::filesystem::exists(snap, ec)) {
      const std::string rdb = snap.string();
      peadb::log(
          peadb::LogLevel::Warn,
          "The AOF " + aof.string() +
              " is empty, and with appendonly yes only the AOF is loaded (as in Redis), "
              "so " +
              rdb + " is not loaded and the server starts with no data. To move the data in " +
              rdb + " to the AOF, start with appendonly no and run CONFIG SET appendonly yes.");
    }
    if (std::filesystem::exists(aof)) {
      std::string err;
      if (!peadb::load_aof_file(aof.string(), err)) {
        // A tolerable truncated tail returns success; reaching here means real
        // corruption. Refuse to start (Redis behaviour) rather than silently
        // serving partial data.
        peadb::log(peadb::LogLevel::Error,
                   "FATAL: AOF load failed: " + err +
                   " — refusing to start. Restore from backup or repair the AOF.");
        return 1;
      }
    }
  } else if (std::filesystem::exists(snap)) {
    std::string err;
    // A file with the "REDIS" magic is a native RDB: if it fails to load (e.g. CRC
    // mismatch) that is corruption — refuse to start rather than silently falling back to
    // the legacy plaintext snapshot and serving partial/empty data. Only non-native files
    // fall back to the legacy PEADB-SNAPSHOT format.
    bool is_native_rdb = false;
    {
      std::ifstream probe(snap, std::ios::binary);
      char magic[5] = {0};
      if (probe.read(magic, 5)) is_native_rdb = std::string(magic, 5) == "REDIS";
    }
    if (!peadb::rdb_load(snap, err)) {
      if (is_native_rdb) {
        peadb::log(peadb::LogLevel::Error,
                   "FATAL: RDB load failed: " + err +
                   " — refusing to start. Restore from backup or repair the dump.");
        return 1;
      }
      peadb::log(peadb::LogLevel::Info, "RDB load failed (" + err + "), trying legacy snapshot");
      if (!peadb::store().load_snapshot_file(snap, err)) {
        peadb::log(peadb::LogLevel::Warn, "failed to load snapshot: " + err);
      }
    }
  }

  peadb::set_loading(false);
  // A SIGTERM or SIGINT during the load: exit without saving, as Redis does from
  // whileBlockedCron(). Redis stops the load at once; PeaDB finishes it first, because it loads
  // before it serves anything (compat/delta.md item 70).
  if (peadb::shutdown_signal_pending()) {
    peadb::set_loading(true);  // prepare_for_shutdown() then never saves
    if (peadb::prepare_for_shutdown(peadb::kShutdownNosave)) return 0;
    peadb::set_loading(false);
    peadb::log(peadb::LogLevel::Warn,
               "SIGTERM received but errors trying to shut down the server, "
               "check the logs for more information");
  }

  int rc = peadb::run_server(config);
  peadb::lua_engine_shutdown();
  return rc;
}
