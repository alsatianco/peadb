#include "config.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>

int main() {
  const std::filesystem::path path = std::filesystem::temp_directory_path() / "peadb_config_test.conf";
  {
    std::ofstream out(path);
    out << "# comment\n";
    out << "port 6390\n";
    out << "maxclients 4096\n";
    out << "bind 0.0.0.0\n";
    out << "loglevel debug\n";
    out << "unknown-directive some value\n";
  }

  const auto cfg = peadb::load_config(path.string());

  if (cfg.port != 6390) {
    std::cerr << "port parse failed\n";
    return 1;
  }
  if (cfg.maxclients != 4096) {
    std::cerr << "maxclients parse failed\n";
    return 1;
  }
  if (cfg.bind != "0.0.0.0") {
    std::cerr << "bind parse failed\n";
    return 1;
  }
  if (cfg.log_level != "debug") {
    std::cerr << "loglevel parse failed\n";
    return 1;
  }
  if (cfg.raw.find("unknown-directive") == cfg.raw.end()) {
    std::cerr << "unknown directive should be tolerated\n";
    return 1;
  }

  std::filesystem::remove(path);
  std::cout << "config_parser_test passed\n";
  return 0;
}
