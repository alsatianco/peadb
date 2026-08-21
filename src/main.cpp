#include "config.hpp"
#include "datastore.hpp"
#include "logger.hpp"
#include "command.hpp"
#include "lua_engine.hpp"
#include "peadb_version.hpp"
#include "rdb.hpp"
#include "server.hpp"

#include <iostream>
#include <string>
#include <fstream>
#include <filesystem>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <execinfo.h>
#include <unistd.h>

static void signal_handler(int) {
  peadb::request_shutdown();
}

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
  std::signal(SIGTERM, signal_handler);
  std::signal(SIGINT, signal_handler);
  install_crash_handlers();

  // First pass: find the config file. Two spellings are accepted: redis-server style
  // (first positional argument, `peadb-server /path/peadb.conf ...`) — required by the
  // upstream Redis test harness, which spawns `$binary $config_file` — and the native
  // `--config <path>` flag. CLI flags override file values either way.
  std::string config_path;
  if (argc >= 2 && argv[1][0] != '-') config_path = argv[1];
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--config" && i + 1 < argc) {
      config_path = argv[i + 1];
      break;
    }
    if (std::string(argv[i]) == "--help" || std::string(argv[i]) == "-h") {
      std::cout << "Usage: peadb-server [<config-file>] [--port <port>] [--bind <ip>] [--loglevel <error|warn|info|debug>]\n"
                   "                    [--dir <path>] [--dbfilename <file>] [--config <path>]\n"
                   "                    [--requirepass <pass>] [--masterauth <pass>]\n"
                   "                    [--protected-mode <yes|no>] [--enable-module-command <yes|no|local>]\n"
                   "                    [--cluster-enabled <yes|no>] [--cluster-announce-ip <ip>]\n"
                   "                    [--cluster-announce-port <port>] [--cluster-announce-bus-port <port>]\n";
      return 0;
    }
  }

  // Start from file config (if any), then let CLI args override
  peadb::ServerConfig config = config_path.empty() ? peadb::ServerConfig{}
                                                    : peadb::load_config(config_path);

  for (int i = (argc >= 2 && argv[1][0] != '-') ? 2 : 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--config" && i + 1 < argc) { ++i; continue; } // already handled
    if (arg == "--port" && i + 1 < argc) { config.port = std::stoi(argv[++i]); continue; }
    if (arg == "--bind" && i + 1 < argc) { config.bind = argv[++i]; continue; }
    if (arg == "--loglevel" && i + 1 < argc) { config.log_level = argv[++i]; continue; }
    if (arg == "--dir" && i + 1 < argc) { config.dir = argv[++i]; continue; }
    if (arg == "--dbfilename" && i + 1 < argc) { config.dbfilename = argv[++i]; continue; }
    if (arg == "--requirepass" && i + 1 < argc) { config.requirepass = argv[++i]; continue; }
    if (arg == "--masterauth" && i + 1 < argc) { config.masterauth = argv[++i]; continue; }
    if (arg == "--protected-mode" && i + 1 < argc) { const std::string v = argv[++i]; config.protected_mode = (v == "yes" || v == "1" || v == "true"); continue; }
    if (arg == "--enable-module-command" && i + 1 < argc) { config.enable_module_command = argv[++i]; continue; }
    if (arg == "--maxmemory" && i + 1 < argc) { config.maxmemory = argv[++i]; continue; }
    if (arg == "--maxmemory-policy" && i + 1 < argc) { config.maxmemory_policy = argv[++i]; continue; }
    if (arg == "--maxmemory-samples" && i + 1 < argc) { config.maxmemory_samples = argv[++i]; continue; }
    if (arg == "--timeout" && i + 1 < argc) { config.timeout = argv[++i]; continue; }
    if (arg == "--tcp-backlog" && i + 1 < argc) { try { config.tcp_backlog = std::stoi(argv[++i]); } catch (...) {} continue; }
    if (arg == "--client-output-buffer-limit" && i + 1 < argc) { config.client_output_buffer_limit = argv[++i]; continue; }
    if (arg == "--save" && i + 1 < argc) { config.save = argv[++i]; continue; }
    if (arg == "--appendonly" && i + 1 < argc) { const std::string v = argv[++i]; config.appendonly = (v == "yes" || v == "1" || v == "true"); continue; }
    if (arg == "--appendfilename" && i + 1 < argc) { config.appendfilename = argv[++i]; continue; }
    if (arg == "--appendfsync" && i + 1 < argc) { config.appendfsync = argv[++i]; continue; }
    if (arg == "--repl-backlog-size" && i + 1 < argc) { config.repl_backlog_size = argv[++i]; continue; }
    if (arg == "--cluster-enabled" && i + 1 < argc) { const std::string v = argv[++i]; config.cluster_enabled = (v == "yes" || v == "1" || v == "true"); continue; }
    if (arg == "--cluster-announce-ip" && i + 1 < argc) { config.cluster_announce_ip = argv[++i]; continue; }
    if (arg == "--cluster-announce-port" && i + 1 < argc) { config.cluster_announce_port = argv[++i]; continue; }
    if (arg == "--cluster-announce-bus-port" && i + 1 < argc) { config.cluster_announce_bus_port = argv[++i]; continue; }
  }

  peadb::set_log_level(peadb::parse_log_level(config.log_level));
  // Startup log contract (relied on by the upstream Redis test harness): a line
  // containing " PID: <pid>", then "Server initialized", then "Ready to accept"
  // once the listeners are up (logged in run_server).
  peadb::log(peadb::LogLevel::Info, "PeaDB " PEADB_VERSION " starting, version 7.2.5-compat, PID: " +
                                        std::to_string(getpid()));
  peadb::g_server_start_time.store(peadb::DataStore::now_ms());
  peadb::configure_runtime_port(config.port);
  peadb::configure_max_clients(config.maxclients);
  peadb::configure_persistence(config.dir, config.dbfilename, config.appendonly, config.appendfilename, config.appendfsync);
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
  peadb::configure_client_output_buffer_limit(config.client_output_buffer_limit);
  peadb::configure_save(config.save);
  peadb::configure_repl_backlog_size(config.repl_backlog_size);
  peadb::configure_notify_keyspace_events(config.notify_keyspace_events);
  peadb::configure_cluster(config.cluster_enabled, config.bind, config.port, config.cluster_announce_ip,
                           config.cluster_announce_port, config.cluster_announce_bus_port);

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

  const auto snap = std::filesystem::path(config.dir) / config.dbfilename;
  if (std::filesystem::exists(snap)) {
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

  if (config.appendonly) {
    const auto aof = std::filesystem::path(config.dir) / config.appendfilename;
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
  }

  int rc = peadb::run_server(config);
  peadb::lua_engine_shutdown();
  return rc;
}
