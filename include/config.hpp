#pragma once

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace peadb {

struct ServerConfig {
  int port = 6379;
  int maxclients = 10000;
  int tcp_backlog = 511;  // listen(2) backlog (Redis default 511)
  std::string bind = "127.0.0.1";
  std::string unixsocket;  // AF_UNIX listener path; empty = disabled
  std::string log_level = "info";
  std::string dir = ".";
  std::string dbfilename = "dump.rdb";
  bool appendonly = false;
  std::string appendfilename = "appendonly.aof";
  std::string appendfsync = "everysec";
  std::string maxmemory;                    // e.g. "100mb"; empty = unlimited
  std::string maxmemory_policy;             // e.g. "allkeys-lru"; empty = noeviction
  std::string maxmemory_samples;            // eviction sample size; empty = 5
  std::string timeout;                       // idle-client timeout (seconds); empty = 0 (off)
  std::string client_output_buffer_limit;    // accumulated "class hard soft sec" groups
  std::string save;                         // raw "sec changes [sec changes ...]"; empty = none
  std::string repl_backlog_size;            // e.g. "256mb"; empty = default 1mb
  std::string notify_keyspace_events;       // e.g. "KEA"; empty = disabled
  // ── Security ──
  std::string requirepass;                 // empty = no auth required (Redis default)
  std::string masterauth;                  // password the replica sends to its master
  bool protected_mode = true;              // refuse non-loopback clients when no auth set
  std::string enable_module_command = "no"; // "yes" | "no" | "local" — gates MODULE LOAD
  // rename-command FROM TO (TO empty disables FROM). Order-preserving, repeatable.
  std::vector<std::pair<std::string, std::string>> rename_commands;
  std::unordered_map<std::string, std::string> raw;
};

ServerConfig load_config(const std::string& path);

} // namespace peadb
