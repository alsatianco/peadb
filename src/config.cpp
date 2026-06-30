#include "config.hpp"

#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>

namespace peadb {
namespace {

std::string trim(const std::string& s) {
  const auto first = std::find_if_not(s.begin(), s.end(), [](unsigned char c) { return std::isspace(c); });
  if (first == s.end()) return "";
  const auto last = std::find_if_not(s.rbegin(), s.rend(), [](unsigned char c) { return std::isspace(c); }).base();
  return std::string(first, last);
}

}  // namespace

ServerConfig load_config(const std::string& path) {
  ServerConfig cfg;
  if (path.empty()) return cfg;

  std::ifstream in(path);
  if (!in.is_open()) return cfg;

  std::string line;
  while (std::getline(in, line)) {
    const auto no_comment = line.substr(0, line.find('#'));
    const auto cleaned = trim(no_comment);
    if (cleaned.empty()) continue;

    std::istringstream iss(cleaned);
    std::string key;
    if (!(iss >> key)) continue;

    std::string value;
    std::getline(iss, value);
    value = trim(value);

    // rename-command takes two tokens (FROM TO) and an empty TO is meaningful
    // (it disables the command), so it must be handled before the empty-value skip.
    if (key == "rename-command") {
      std::istringstream rargs(value);
      std::string from, to;
      rargs >> from;            // TO may be absent/empty (= disable)
      rargs >> to;
      if (!from.empty()) cfg.rename_commands.emplace_back(from, to);
      continue;
    }

    // client-output-buffer-limit is one class per line in the config file; accumulate
    // all lines into a single flat spec for configure_client_output_buffer_limit.
    if (key == "client-output-buffer-limit") {
      if (!value.empty()) {
        cfg.client_output_buffer_limit += (cfg.client_output_buffer_limit.empty() ? "" : " ");
        cfg.client_output_buffer_limit += value;
      }
      continue;
    }

    // save thresholds accumulate across lines; `save ""` clears them.
    if (key == "save") {
      if (value == "\"\"" || value.empty()) { cfg.save.clear(); }
      else { cfg.save += (cfg.save.empty() ? "" : " "); cfg.save += value; }
      continue;
    }

    if (value.empty()) continue;

    cfg.raw[key] = value;

    if (key == "port") {
      cfg.port = std::stoi(value);
    } else if (key == "maxclients") {
      cfg.maxclients = std::stoi(value);
    } else if (key == "bind") {
      cfg.bind = value;
    } else if (key == "loglevel") {
      cfg.log_level = value;
    } else if (key == "dir") {
      cfg.dir = value;
    } else if (key == "dbfilename") {
      cfg.dbfilename = value;
    } else if (key == "appendonly") {
      cfg.appendonly = (value == "yes" || value == "1" || value == "true");
    } else if (key == "appendfilename") {
      cfg.appendfilename = value;
    } else if (key == "appendfsync") {
      cfg.appendfsync = value;
    } else if (key == "requirepass") {
      cfg.requirepass = value;
    } else if (key == "masterauth") {
      cfg.masterauth = value;
    } else if (key == "protected-mode") {
      cfg.protected_mode = (value == "yes" || value == "1" || value == "true");
    } else if (key == "enable-module-command") {
      cfg.enable_module_command = value;
    } else if (key == "maxmemory") {
      cfg.maxmemory = value;
    } else if (key == "maxmemory-policy") {
      cfg.maxmemory_policy = value;
    } else if (key == "maxmemory-samples") {
      cfg.maxmemory_samples = value;
    } else if (key == "timeout") {
      cfg.timeout = value;
    } else if (key == "repl-backlog-size") {
      cfg.repl_backlog_size = value;
    } else if (key == "notify-keyspace-events") {
      cfg.notify_keyspace_events = value;
    }
  }

  return cfg;
}

}  // namespace peadb
