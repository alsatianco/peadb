#pragma once

#include <string>
#include <unordered_map>

namespace peadb {

struct ServerConfig {
  int port = 6379;
  int maxclients = 10000;
  std::string bind = "127.0.0.1";
  std::string log_level = "info";
  std::string dir = ".";
  std::string dbfilename = "dump.rdb";
  bool appendonly = false;
  std::string appendfilename = "appendonly.aof";
  std::unordered_map<std::string, std::string> raw;
};

ServerConfig load_config(const std::string& path);

} // namespace peadb
