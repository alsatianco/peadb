#include "config.hpp"
#include "datastore.hpp"
#include "logger.hpp"
#include "command.hpp"
#include "lua_engine.hpp"
#include "rdb.hpp"
#include "server.hpp"

#include <iostream>
#include <string>
#include <filesystem>
#include <csignal>

static void signal_handler(int) {
  peadb::request_shutdown();
}

int main(int argc, char** argv) {
  std::signal(SIGPIPE, SIG_IGN);
  std::signal(SIGTERM, signal_handler);
  std::signal(SIGINT, signal_handler);

  // First pass: find --config so we can load the file before applying CLI overrides
  std::string config_path;
  for (int i = 1; i < argc; ++i) {
    if (std::string(argv[i]) == "--config" && i + 1 < argc) {
      config_path = argv[i + 1];
      break;
    }
    if (std::string(argv[i]) == "--help" || std::string(argv[i]) == "-h") {
      std::cout << "Usage: peadb-server [--port <port>] [--bind <ip>] [--loglevel <error|warn|info|debug>] [--config <path>]\n";
      return 0;
    }
  }

  // Start from file config (if any), then let CLI args override
  peadb::ServerConfig config = config_path.empty() ? peadb::ServerConfig{}
                                                    : peadb::load_config(config_path);

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--config" && i + 1 < argc) { ++i; continue; } // already handled
    if (arg == "--port" && i + 1 < argc) { config.port = std::stoi(argv[++i]); continue; }
    if (arg == "--bind" && i + 1 < argc) { config.bind = argv[++i]; continue; }
    if (arg == "--loglevel" && i + 1 < argc) { config.log_level = argv[++i]; continue; }
    if (arg == "--dir" && i + 1 < argc) { config.dir = argv[++i]; continue; }
    if (arg == "--dbfilename" && i + 1 < argc) { config.dbfilename = argv[++i]; continue; }
  }

  peadb::set_log_level(peadb::parse_log_level(config.log_level));
  peadb::log(peadb::LogLevel::Info, "starting peadb-server");
  peadb::g_server_start_time.store(peadb::DataStore::now_ms());
  peadb::configure_runtime_port(config.port);
  peadb::configure_max_clients(config.maxclients);
  peadb::configure_persistence(config.dir, config.dbfilename, config.appendonly, config.appendfilename, config.appendfsync);

  // Initialize the Lua scripting engine and wire the command dispatcher.
  peadb::lua_engine_init();
  peadb::lua_engine_set_dispatch(
      [](const std::vector<std::string>& args, peadb::SessionState& session, bool& close) -> std::string {
        return peadb::handle_command(args, session, close);
      });

  const auto snap = std::filesystem::path(config.dir) / config.dbfilename;
  if (std::filesystem::exists(snap)) {
    std::string err;
    // Try native RDB format first, fall back to legacy PEADB-SNAPSHOT format
    if (!peadb::rdb_load(snap, err)) {
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
        peadb::log(peadb::LogLevel::Warn, "failed to load AOF: " + err);
      }
    }
  }

  int rc = peadb::run_server(config);
  peadb::lua_engine_shutdown();
  return rc;
}
