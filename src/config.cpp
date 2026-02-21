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
    }
  }

  return cfg;
}

}  // namespace peadb
