#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace peadb {

struct ParsedCommand {
  std::vector<std::string> args;
  std::size_t consumed = 0;
};

std::optional<ParsedCommand> parse_one_command(std::string_view buffer);

} // namespace peadb
