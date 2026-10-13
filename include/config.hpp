#pragma once

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

namespace peadb {

// One rename-command directive, already checked by the loader against Redis's command table
// the way Redis checks it. command.cpp applies it when it builds the command table.
struct RenameCommandDirective {
  std::string from;
  std::string to;          // empty = remove the command
  std::string redis_name;  // the Redis command this name stands for (after earlier renames)
  int line = 0;            // config line number, for warnings (which never print `to`)
  std::string origin;      // "Reading the configuration file, at line N\n>>> '<line>'"
};

struct ServerConfig {
  int port = 6379;
  int maxclients = 10000;
  int tcp_backlog = 511;  // listen(2) backlog (Redis default 511)
  std::string bind = "127.0.0.1";
  std::string unixsocket;  // AF_UNIX listener path; empty = disabled
  int unixsocketperm = 0;  // mode for the unix socket file; 0 = leave the mode bind(2) gave it
  std::string log_level = "notice";     // a Redis loglevel name
  std::string dbfilename = "dump.rdb";  // relative to `dir`, which the loader chdir()s into
  bool appendonly = false;
  std::string appendfilename = "appendonly.aof";
  std::string appendfsync = "everysec";
  std::string maxmemory;                   // bytes, decimal; empty = unlimited
  std::string maxmemory_policy;            // e.g. "allkeys-lru"; empty = noeviction
  std::string maxmemory_samples;           // eviction sample size; empty = 5
  std::string timeout;                     // idle-client timeout (seconds); empty = 0 (off)
  std::string client_output_buffer_limit;  // accumulated "class hard soft sec" groups
  std::string save;                        // "sec changes [sec changes ...]"; empty = none
  std::string repl_backlog_size;           // bytes, decimal; empty = default 1mb
  std::string notify_keyspace_events;      // e.g. "KEA"; empty = disabled
  // ── Security ──
  std::string requirepass;  // empty = no auth required (Redis default)
  std::string masterauth;   // password the replica sends to its master
  std::string masteruser;   // empty = AUTH password, otherwise AUTH username password
  bool replica = false;     // replicaof/slaveof; independent of the host text
  std::string master_host;
  int master_port = 0;
  bool protected_mode = true;                // refuse non-loopback clients when no auth set
  std::string enable_module_command = "no";  // "yes" | "no" | "local" — gates MODULE LOAD
  std::vector<RenameCommandDirective> rename_commands;  // in config order
  // ── Cluster ──
  bool cluster_enabled = false;  // startup-only, like Redis's IMMUTABLE_CONFIG
  std::string
      cluster_announce_ip;  // empty = use `bind` (a documented simplification —
                            // Redis instead auto-detects from a live connection;
                            // see docs/decisions/p5-cluster-addressing-and-version-provenance.md)
  std::string cluster_announce_port;      // empty = use `port`
  std::string cluster_announce_bus_port;  // empty = announced port + 10000
  // Every standard Redis setting the config named, by canonical name (aliases are folded into
  // it and the last value wins, as in Redis), holding the checked value: "yes"/"no", a decimal
  // number, a lower-case enum name, or the string. configure_encoding_limits() and
  // configure_runtime_settings() read the settings that have no field above.
  std::unordered_map<std::string, std::string> raw;
  // One line per Redis setting that PeaDB accepted but does not apply, for main.cpp to log.
  std::vector<std::string> warnings;
};

struct RedisConfigSpec;  // redis_config_table.generated.hpp

// The Redis 7.2.5 setting called `name` (its name or its alias, in any case), or nullptr.
const RedisConfigSpec* find_redis_config(const std::string& name);

// The arguments CONFIG SET hands to a setting (performInterfaceSet() in config.c): a MULTI_ARG
// setting's value is split at every single space, so an empty value gives no arguments; any
// other setting gets the value as its only argument.
std::vector<std::string> config_set_args(const RedisConfigSpec& spec, const std::string& value);

// Redis's checks of one setting's arguments, shared by the config loader and CONFIG SET. On
// success `value` is the checked value in the form CONFIG GET prints it (for
// client-output-buffer-limit: only the classes given, the slave class named "replica").
bool check_redis_config_value(const RedisConfigSpec& spec, const std::vector<std::string>& args,
                              std::string& value, std::string& err);

// A Redis setting that PeaDB does not apply but whose behaviour is one of its values: PeaDB
// always does what `value` means, CONFIG GET reports `value`, and CONFIG SET refuses any other
// value. In the config file, another value stops startup with `refuse` when that is set, and is
// accepted with a warning otherwise (compat/delta.md item 24).
struct FixedSetting {
  const char* value;   // in the form CONFIG GET prints it
  const char* refuse;  // nullptr when another value only gets a warning
};

// The fixed behaviour of the Redis setting `name` (its canonical name), or nullptr.
const FixedSetting* peadb_fixed_setting(const std::string& name);

// Redis's sdssplitargs (sds.c): splits one config line into arguments. "Double quotes" take the
// escapes \n \r \t \b \a \xHH and \<any char>; 'single quotes' take \'. A closing quote must be
// followed by a space or the end of the line. Returns false on unbalanced quotes.
bool split_config_args(const std::string& line, std::vector<std::string>& out);

// The config text Redis's main() builds from command-line options: argv[first..] where each
// `--name` starts a new line and every following argument is appended quoted, so
// `--port 6380 --save ""` becomes "port \"6380\" \nsave \"\" ". Sets from_stdin when a lone "-"
// is argv[1] or the last argument (read the config from stdin, as in Redis).
std::string config_options_from_argv(const std::vector<std::string>& argv, std::size_t first,
                                     bool& from_stdin);

// Parses config text like Redis's loadServerConfigFromString(). Returns false on the first bad
// line, with `error` set to the full report to print (see format_config_error()).
bool load_config_string(const std::string& text, ServerConfig& cfg, std::string& error);

// Redis's loadServerConfig(): reads `path` (unless empty; a name with * ? or [ is a glob), then
// stdin when from_stdin, then appends `options`, and parses the whole text. A config file that
// cannot be opened is an error, as in Redis.
bool load_server_config(const std::string& path, bool from_stdin, const std::string& options,
                        ServerConfig& cfg, std::string& error);

// The report Redis prints before it exits on a bad config line ("*** FATAL CONFIG FILE ERROR").
// `origin` is the "Reading the configuration file, at line N\n>>> '<line>'" part (may be empty).
std::string format_config_error(const std::string& origin, const std::string& reason);

}  // namespace peadb
