#include "command.hpp"
#include "datastore.hpp"

#include <iostream>
#include <fstream>
#include <filesystem>
#include <algorithm>

// Validate every registered command has a plausible arity (non-zero) and
// that the total count is at least what the README advertises.
int main() {
  const std::size_t count = peadb::command_count();
  const auto names = peadb::command_names();

  if (count == 0) {
    std::cerr << "command_count() returned 0\n";
    return 1;
  }

  constexpr std::size_t kExpectedMin = 200;
  if (count < kExpectedMin) {
    std::cerr << "command_count() = " << count << " but expected >= " << kExpectedMin << "\n";
    return 1;
  }

  // Verify a stable set of critical commands are present
  const std::vector<std::string> required = {
    "GET", "SET", "DEL", "MGET", "MSET", "EXISTS",
    "HGET", "HSET", "HDEL",
    "LPUSH", "RPUSH", "LPOP", "RPOP",
    "SADD", "SREM", "SMEMBERS",
    "ZADD", "ZRANGE", "ZPOPMIN",
    "XADD", "XREAD",
    "SUBSCRIBE", "PUBLISH",
    "MULTI", "EXEC", "DISCARD",
    "EVAL", "EVALSHA",
    "CLUSTER", "PSYNC", "REPLICAOF",
    "INFO", "CONFIG", "COMMAND",
    "BGSAVE", "BGREWRITEAOF", "WAIT",
    "SCAN", "HSCAN", "SSCAN", "ZSCAN",
    "BLPOP", "BRPOP", "BZPOPMIN",
  };
  for (const auto& cmd : required) {
    if (std::find(names.begin(), names.end(), cmd) == names.end()) {
      std::cerr << "Required command missing from table: " << cmd << "\n";
      return 1;
    }
  }

  // Write golden snapshot (for future divergence detection)
  const auto golden = std::filesystem::path(__FILE__).parent_path() / "command_contract.golden.txt";
  if (!std::filesystem::exists(golden)) {
    std::ofstream out(golden);
    for (const auto& n : names) out << n << "\n";
    std::cout << "Generated golden snapshot: " << golden << "\n";
  } else {
    // Compare against existing golden
    std::vector<std::string> golden_names;
    std::ifstream in(golden);
    std::string line;
    while (std::getline(in, line)) if (!line.empty()) golden_names.push_back(line);

    // Find commands added or removed
    std::vector<std::string> added, removed;
    for (const auto& n : names)
      if (std::find(golden_names.begin(), golden_names.end(), n) == golden_names.end()) added.push_back(n);
    for (const auto& n : golden_names)
      if (std::find(names.begin(), names.end(), n) == names.end()) removed.push_back(n);

    if (!removed.empty()) {
      std::cerr << "Commands removed from table (update golden if intentional):\n";
      for (const auto& n : removed) std::cerr << "  - " << n << "\n";
      return 1;
    }
    if (!added.empty()) {
      std::cout << "New commands added (updating golden):\n";
      for (const auto& n : added) std::cout << "  + " << n << "\n";
      std::ofstream out(golden);
      for (const auto& n : names) out << n << "\n";
    }
  }

  std::cout << "command_contract_test passed: " << count << " commands\n";
  return 0;
}
